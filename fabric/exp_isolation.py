#!/usr/bin/env python3
"""G4 consumer-isolation experiment: a slow L3 consumer must not stall a fast one.

The fabric gives each L3 monitor its own durable JetStream cursor, so a lagging
consumer never gates the others; the shared-log prototype serialises all monitors
behind one file cursor, so the slowest sets the pace. We publish N verdicts, then
run a FAST consumer (acks immediately) and a SLOW consumer (d ms/verdict) on
independent durables and measure how long the fast one takes to drain everything
and how far behind the slow one is at that moment. The shared-log counterfactual is
N*d (head-of-line blocking behind the slow handler).
"""
import argparse, asyncio, json, os, time
import nats
from nats.js.api import StreamConfig

STREAM, SUBJ = "ISO", "iso.v"


async def main(a):
    nc = await nats.connect(a.nats)
    js = nc.jetstream()
    try:
        await js.delete_stream(STREAM)
    except Exception:
        pass
    await js.add_stream(StreamConfig(name=STREAM, subjects=[SUBJ]))
    payload = json.dumps({"type": "safe_tx", "eid": "x"}).encode()
    for _ in range(a.n):
        await js.publish(SUBJ, payload)

    state = {"fast_done_at": None, "slow_at_fast_done": 0}

    async def fast():
        sub = await js.pull_subscribe(SUBJ, durable="fast")
        t0 = time.monotonic(); c = 0
        while c < a.n:
            try:
                msgs = await sub.fetch(200, timeout=5)
            except Exception:
                break
            for m in msgs:
                await m.ack(); c += 1
        state["fast_time"] = time.monotonic() - t0
        state["fast_count"] = c
        state["fast_done_at"] = time.monotonic()

    async def slow():
        sub = await js.pull_subscribe(SUBJ, durable="slow")
        c = 0
        while c < a.n:
            try:
                msgs = await sub.fetch(10, timeout=5)
            except Exception:
                break
            for m in msgs:
                await asyncio.sleep(a.delay)      # slow per-verdict processing
                await m.ack(); c += 1
                if state["fast_done_at"] and not state["slow_at_fast_done"]:
                    state["slow_at_fast_done"] = c
            if state["fast_done_at"] and state["slow_at_fast_done"]:
                break     # snapshot taken; no need to drain the slow one fully
        state["slow_count_final"] = c

    await asyncio.gather(fast(), slow())
    await nc.drain()
    out = {
        "verdicts": a.n, "slow_delay_ms": a.delay * 1000,
        "fast_drain_time_s": round(state["fast_time"], 3),
        "fast_delivered": state["fast_count"],
        "slow_delivered_when_fast_done": state["slow_at_fast_done"],
        "shared_log_counterfactual_s": round(a.n * a.delay, 1),
        "isolation_speedup_x": round((a.n * a.delay) / state["fast_time"], 1) if state["fast_time"] else None,
    }
    print(json.dumps(out, indent=2))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--nats", default=os.environ.get("NATS_URL", "nats://localhost:4222"))
    ap.add_argument("--n", type=int, default=2000)
    ap.add_argument("--delay", type=float, default=0.02, help="slow consumer s/verdict")
    asyncio.run(main(ap.parse_args()))
