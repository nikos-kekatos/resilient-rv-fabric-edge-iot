#!/usr/bin/env python3
"""Crash-injection driver for the gateway->backend relay (CRITIS review, concern #1).

Exercises the REAL production outbox (gateway.DurableOutbox) against a REAL NATS
JetStream, injecting a hard crash (os._exit, i.e. SIGKILL-equivalent) at each of
the three relay stages the reviewer asked about:

    before_persist  -- between MQTT receipt and WAL fsync
    after_persist   -- after WAL fsync, before the JetStream publish ack
    after_publish   -- after the publish ack, before the ack-cursor advance

Two modes:
  --relay   : relay verdicts [START_SEQ, START_SEQ+N) for device DEVICE, honouring
              CRASH_AT / CRASH_AFTER_N (a crash hard-exits; a clean finish prints
              {"published": n}).
  --recover-timing : pre-populate a WAL of --backlog entries with cursor=0, then
              time DurableOutbox.recover() replaying the whole backlog (no crash).
              Prints {"replayed": n, "recovery_ms": t}.

The verdict schema matches what the gateway publishes and what backend_l3 dedups
by: a safe_tx alert carries a stable per-device event id ``eid = gw:device:seq``.
"""
import argparse, asyncio, json, os, time
import nats
from gateway import DurableOutbox

SUBJECT_STREAM = "RVFABRIC"


def make_verdict(gw, device, seq):
    # one safe_tx verdict; timestamp is the logical seq so ordering is well-defined.
    return {"timestamp": seq, "type": "safe_tx", "device": device,
            "gw": gw, "eid": f"{gw}:{device}:{seq}", "dev_ts": seq}


async def ensure_stream(js):
    try:
        await js.add_stream(name=SUBJECT_STREAM, subjects=["gw.*.verdict", "fleet.tick"])
    except Exception:
        pass


async def do_relay(args):
    nc = await nats.connect(args.nats)
    js = nc.jetstream()
    await ensure_stream(js)
    subject = f"gw.{args.gw}.verdict"
    ob = DurableOutbox(args.wal)
    replayed = await ob.recover(js, subject)     # replay any un-acked suffix first
    ob.open()
    if replayed:
        print(json.dumps({"phase": "recover", "replayed": replayed}), flush=True)
    published = 0
    for seq in range(args.start, args.start + args.n):
        await ob.append_and_publish(js, subject, make_verdict(args.gw, args.device, seq))
        published += 1
    await nc.drain()
    print(json.dumps({"phase": "relay", "published": published}), flush=True)


async def do_recover_timing(args):
    # Pre-populate a WAL of `backlog` entries with cursor=0 (worst case: cursor lost),
    # then time recover() replaying the whole backlog. Measures recovery cost vs size.
    if os.path.exists(args.wal):
        os.remove(args.wal)
    cur = args.wal + ".cursor"
    if os.path.exists(cur):
        os.remove(cur)
    with open(args.wal, "w") as f:
        for seq in range(args.backlog):
            f.write(json.dumps(make_verdict(args.gw, args.device, seq)) + "\n")
        f.flush(); os.fsync(f.fileno())
    nc = await nats.connect(args.nats)
    js = nc.jetstream()
    await ensure_stream(js)
    subject = f"gw.{args.gw}.verdict"
    ob = DurableOutbox(args.wal)
    t0 = time.monotonic()
    replayed = await ob.recover(js, subject)
    dt = (time.monotonic() - t0) * 1000.0
    await nc.drain()
    print(json.dumps({"phase": "recover-timing", "backlog": args.backlog,
                      "replayed": replayed, "recovery_ms": round(dt, 2)}), flush=True)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--relay", action="store_true")
    ap.add_argument("--recover-timing", action="store_true")
    ap.add_argument("--nats", default=os.environ.get("NATS_URL", "nats://localhost:4222"))
    ap.add_argument("--gw", default="gw1")
    ap.add_argument("--device", default="node-1")
    ap.add_argument("--wal", default=os.environ.get("OUTBOX", "/tmp/exp-outbox.jsonl"))
    ap.add_argument("--start", type=int, default=0)
    ap.add_argument("--n", type=int, default=1)
    ap.add_argument("--backlog", type=int, default=1000)
    a = ap.parse_args()
    if a.recover_timing:
        asyncio.run(do_recover_timing(a))
    else:
        asyncio.run(do_relay(a))
