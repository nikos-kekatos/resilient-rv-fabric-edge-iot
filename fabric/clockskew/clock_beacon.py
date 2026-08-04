#!/usr/bin/env python3
"""Clock beacon: periodically publish this process's (faketime-skewed) wall clock,
so a host collector can measure a gateway's real clock offset from a dense, uniform
sample stream rather than from sparse, bursty alert events.

Run this under the SAME faketime spec as the gateway it mirrors, so its time.time()
carries the identical injected offset/drift. It publishes {"id","clock"} to
clock.<id> every --interval seconds.
"""
import argparse, asyncio, json, os, time
import nats


async def main(a):
    # retry until the broker is up (droplet startup race)
    nc = None
    for _ in range(60):
        try:
            nc = await nats.connect(a.nats, connect_timeout=3)
            break
        except Exception:
            await asyncio.sleep(2)
    if nc is None:
        nc = await nats.connect(a.nats)   # final attempt, raise if still down
    subj = f"clock.{a.id}"
    while True:
        try:
            await nc.publish(subj, json.dumps({"id": a.id, "clock": time.time()}).encode())
        except Exception:
            pass
        await asyncio.sleep(a.interval)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--id", required=True)
    ap.add_argument("--nats", default=os.environ.get("NATS_URL", "nats://localhost:4222"))
    ap.add_argument("--interval", type=float, default=0.5)
    asyncio.run(main(ap.parse_args()))
