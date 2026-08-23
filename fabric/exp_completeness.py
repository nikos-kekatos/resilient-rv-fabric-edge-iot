#!/usr/bin/env python3
"""Evidence-completeness evaluation for the evidence-aware verdict algebra.

Drives four scenarios on a REAL NATS JetStream and checks the evidence-completeness
tag c in {sound > degraded > incomplete > unavailable} that RV-Fabric assigns from
state it already tracks: event-id continuity (gap), the retention watermark
(backlog/capacity), and tick-vs-evidence (liveness). The worst active condition wins.

  normal stream               -> sound
  retention >= watermark       -> degraded
  event-id sequence gap        -> incomplete
  tick alive, evidence silent   -> unavailable
"""
import argparse, asyncio, json, os, time
import nats
from nats.js.api import StreamConfig

STREAM, SUBJ, TICK = "COMPL", "compl.v", "compl.tick"


def classify(*, gap, backlog, capacity, ticks_recent, evidence_recent, watermark):
    # ordering sound > degraded > incomplete > unavailable: report the worst active.
    if ticks_recent and not evidence_recent:
        return "unavailable"
    if gap:
        return "incomplete"
    if capacity and backlog / capacity >= watermark:
        return "degraded"
    return "sound"


def mk(gw, dev, seq):
    return json.dumps({"eid": f"{gw}:{dev}:{seq}", "type": "safe_tx"}).encode()


async def reset(js, cap):
    try:
        await js.delete_stream(STREAM)
    except Exception:
        pass
    await js.add_stream(StreamConfig(name=STREAM, subjects=[SUBJ, TICK], max_msgs=cap))


async def backlog(js):
    info = await js.stream_info(STREAM)
    return info.state.messages


async def has_gap(js, watermark=None):
    """True if the consumer can prove evidence is missing.

    An interior hole is visible from the delivered ids alone. A *truncated suffix*
    is not: if the tail of a device's stream never arrives, the ids that did arrive
    are still contiguous, and a device whose every event was dropped leaves no trace
    at all. Both need the gateway's per-device high-water mark, which the gateway
    publishes on the tick subject; pass it as `watermark` ({(gw,dev): last_seq}).
    Without it this can only report interior holes.
    """
    sub = await js.pull_subscribe(SUBJ, durable="chk")
    seqs = {}
    for _ in range(200):
        try:
            msgs = await sub.fetch(100, timeout=1)
        except Exception:
            break
        for m in msgs:
            d = json.loads(m.data.decode())
            gw, dev, s = d["eid"].split(":")
            seqs.setdefault((gw, dev), set()).add(int(s))
            await m.ack()
    for v in seqs.values():
        if v and (max(v) - min(v) + 1) != len(v):
            return True                       # interior hole
    if watermark:
        for k, hi in watermark.items():
            v = seqs.get(k)
            if not v:
                return True                   # device never appeared downstream
            if max(v) < hi:
                return True                   # truncated suffix
    return False


async def main(a):
    nc = await nats.connect(a.nats)
    js = nc.jetstream()
    T, WM, CAP = a.window, a.watermark, a.capacity
    results = []

    # 1) normal -> sound
    await reset(js, CAP)
    for i in range(20):
        await js.publish(SUBJ, mk("gw1", "d1", i))
    results.append(("normal stream", "sound",
        classify(gap=await has_gap(js), backlog=await backlog(js), capacity=CAP,
                 ticks_recent=True, evidence_recent=True, watermark=WM)))

    # 2) retention >= watermark -> degraded (fill past the high-water mark, no gap)
    await reset(js, CAP)
    for i in range(int(CAP * (WM + 0.05))):
        await js.publish(SUBJ, mk("gw1", "d1", i))
    results.append((f"retention {int((WM+0.05)*100)}% of cap", "degraded",
        classify(gap=await has_gap(js), backlog=await backlog(js), capacity=CAP,
                 ticks_recent=True, evidence_recent=True, watermark=WM)))

    # 3) event-id sequence gap -> incomplete
    await reset(js, CAP)
    for i in list(range(10)) + list(range(11, 20)):     # skip seq 10
        await js.publish(SUBJ, mk("gw1", "d1", i))
    results.append(("event-id sequence gap", "incomplete",
        classify(gap=await has_gap(js), backlog=await backlog(js), capacity=CAP,
                 ticks_recent=True, evidence_recent=True, watermark=WM)))

    # 4) tick alive, evidence silent -> unavailable
    await reset(js, CAP)
    last_verdict = time.monotonic()
    for i in range(5):
        await js.publish(SUBJ, mk("gw1", "d1", i))
        last_verdict = time.monotonic()
    await asyncio.sleep(T + 0.5)                          # let evidence go stale
    last_tick = time.monotonic()
    await js.publish(TICK, json.dumps({"ts": last_tick}).encode())  # tick still alive
    now = time.monotonic()
    results.append(("tick alive, evidence silent", "unavailable",
        classify(gap=await has_gap(js), backlog=await backlog(js), capacity=CAP,
                 ticks_recent=(now - last_tick) < T,
                 evidence_recent=(now - last_verdict) < T, watermark=WM)))

    await js.delete_stream(STREAM)
    await nc.drain()
    ok = all(exp == obs for _, exp, obs in results)
    print(json.dumps({"window_s": T, "watermark": WM, "capacity": CAP,
                      "scenarios": [{"scenario": s, "expected": e, "observed": o,
                                     "match": e == o} for s, e, o in results],
                      "all_match": ok}, indent=2))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--nats", default=os.environ.get("NATS_URL", "nats://localhost:4222"))
    ap.add_argument("--window", type=float, default=2.0, help="liveness T (s)")
    ap.add_argument("--watermark", type=float, default=0.8)
    ap.add_argument("--capacity", type=int, default=100)
    asyncio.run(main(ap.parse_args()))
