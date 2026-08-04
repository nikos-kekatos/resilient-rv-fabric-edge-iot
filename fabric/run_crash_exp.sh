#!/usr/bin/env bash
# Crash-injection & recovery experiment orchestrator (CRITIS review, concern #1).
# Runs the REAL gateway.DurableOutbox against the REAL NATS JetStream, crashing the
# relay at each of the 3 stages, then measures loss / dup-publications / dup-incidents.
set -u
NET=rvexp
IMG=rvnode:latest
NATS=nats://nats-exp:4222
K=50            # total verdicts in the stream
C=25            # crash occurs while relaying verdict #C (0-indexed)
DEV=node-1
DR="docker run --rm --network $NET -v $PWD:/exp -w /exp -e NATS_URL=$NATS $IMG python3"

mkdir -p expdata
echo "=== Crash-injection experiment: K=$K verdicts, crash at verdict #$C ==="
for MODE in before_persist after_persist after_publish; do
  WAL=/exp/expdata/outbox-$MODE.jsonl
  rm -f expdata/outbox-$MODE.jsonl expdata/outbox-$MODE.jsonl.cursor
  echo
  echo "--- MODE: $MODE ---"
  # Run 1: relay 0..C, hard-crash on verdict C at $MODE
  docker run --rm --network $NET -v "$PWD":/exp -w /exp \
      -e NATS_URL=$NATS -e CRASH_AT=$MODE -e CRASH_AFTER_N=$C \
      $IMG python3 exp_crash_relay.py --relay --gw "$MODE" --device "$DEV" \
      --wal "$WAL" --start 0 --n $((C+1)) ; echo "  (run1 exit=$?)"
  # Run 2: restart -> recover() replays un-acked suffix, then relay C+1..K-1 (no crash)
  $DR exp_crash_relay.py --relay --gw "$MODE" --device "$DEV" --wal "$WAL" \
      --start $((C+1)) --n $((K-C-1))
  # Measure what a deduping L3 consumer sees
  echo -n "  RESULT: " ; $DR exp_count_stream.py --subject "gw.$MODE.verdict"
done

echo
echo "=== Recovery-time vs backlog (worst case: cursor lost, replay whole WAL) ==="
for B in 100 1000 5000; do
  echo -n "  " ; $DR exp_crash_relay.py --recover-timing --gw rtiming --device "$DEV" \
      --wal /exp/expdata/rtiming.jsonl --backlog $B
done
