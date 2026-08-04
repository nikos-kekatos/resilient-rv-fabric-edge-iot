#!/usr/bin/env python3
"""Count what a deduplicating L3 consumer would see on the verdict stream.

Reads EVERY message currently retained in the RVFABRIC stream on a subject and
applies the SAME event-id dedup rule as backend_l3._dup, reporting:

    received  -- total publications on the subject (incl. crash replays / dups)
    unique    -- distinct event ids  == what a deduping monitor actually processes
    duplicates= received - unique  (extra publications the dedup collapses)

So "duplicate incidents" seen by the correlator == received - unique that get
suppressed, i.e. the correlator processes exactly `unique` verdicts regardless of
how many times the outbox replayed them.
"""
import argparse, asyncio, json, os
import nats

STREAM = "RVFABRIC"


async def main(args):
    nc = await nats.connect(args.nats)
    js = nc.jetstream()
    info = await js.stream_info(STREAM)
    last = info.state.last_seq
    seen = set()
    received = 0
    for seq in range(1, last + 1):
        try:
            m = await js.get_msg(STREAM, seq)
        except Exception:
            continue  # gap (discarded); skip
        if m.subject != args.subject:
            continue
        try:
            v = json.loads(m.data)
        except Exception:
            continue
        eid = v.get("eid")
        if eid is None:
            continue
        received += 1
        seen.add(eid)
    await nc.drain()
    print(json.dumps({"subject": args.subject, "received": received,
                      "unique": len(seen), "duplicates": received - len(seen)}))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--nats", default=os.environ.get("NATS_URL", "nats://localhost:4222"))
    ap.add_argument("--subject", default="gw.gw1.verdict")
    asyncio.run(main(ap.parse_args()))
