#!/usr/bin/env python3
"""Retention-exhaustion / T_buffer experiment.

The paper's G5 (flow control) "defers loss under transient overload by letting
events queue"; this measures what happens when a backend link outage lasts
LONGER than the buffer can hold. We measure it directly.

Part A -- overflow behaviour. Fill a bounded JetStream stream (max_msgs = CAP) with
no consumer draining, under both JetStream discard policies:
  * discard=old : when full, the OLDEST verdicts are silently dropped -> monitoring
                  continuity is lost with NO signal to the producer.
  * discard=new : when full, new publishes are REJECTED with an error the producer
                  sees -> a health alert CAN be raised before continuity is lost.
Reports, for each: attempted publishes, publish errors surfaced, messages retained,
verdicts silently lost, and whether the loss was observable.

Part B -- buffer horizon. Measure the raw durable-publish rate R, then report the
outage the buffer absorbs, T_buffer = CAP / R_in, for a range of capacities and the
paper's 1000-device fleet rate.
"""
import argparse, asyncio, json, os, time
import nats
from nats.js.api import StreamConfig, DiscardPolicy, RetentionPolicy

STREAM = "RET"
SUBJ = "ret.x"


async def reset(js, cap, discard):
    try:
        await js.delete_stream(STREAM)
    except Exception:
        pass
    await js.add_stream(StreamConfig(
        name=STREAM, subjects=[SUBJ], max_msgs=cap,
        discard=discard, retention=RetentionPolicy.LIMITS))


async def part_a(js, cap):
    payload = json.dumps({"type": "safe_tx", "device": "d", "eid": "d:0"}).encode()
    attempts = cap * 2                      # push to 2x capacity
    out = {}
    for name, pol in (("discard=old", DiscardPolicy.OLD), ("discard=new", DiscardPolicy.NEW)):
        await reset(js, cap, pol)
        errors = 0
        for i in range(attempts):
            try:
                await js.publish(SUBJ, payload)
            except Exception:
                errors += 1
        info = await js.stream_info(STREAM)
        retained = info.state.messages
        # with discard=old, silently-lost = attempts - retained (producer saw no error);
        # with discard=new, rejected = errors (producer saw every drop).
        silently_lost = (attempts - retained) - errors
        out[name] = {"capacity": cap, "attempted": attempts, "publish_errors": errors,
                     "retained": retained, "silently_lost": silently_lost,
                     "loss_observable": errors > 0 and silently_lost == 0}
    return out


async def part_b(js, cap):
    # Measure raw durable-publish rate (no consumer): time N publishes.
    await reset(js, cap, DiscardPolicy.OLD)
    payload = json.dumps({"type": "safe_tx", "device": "d", "eid": "d:0"}).encode()
    N = 20000
    t0 = time.monotonic()
    for i in range(N):
        await js.publish(SUBJ, payload)
    r = N / (time.monotonic() - t0)
    horizons = []
    for capv in (10_000, 100_000, 1_000_000):
        for rin, label in ((1930, "1000-dev fleet (paper)"), (round(r), "measured max")):
            horizons.append({"capacity": capv, "R_in_msg_s": rin,
                             "source": label, "T_buffer_s": round(capv / rin, 1)})
    return {"measured_publish_rate_msg_s": round(r), "horizons": horizons}


async def main(args):
    nc = await nats.connect(args.nats)
    js = nc.jetstream()
    a = await part_a(js, args.cap)
    b = await part_b(js, args.cap)
    try:
        await js.delete_stream(STREAM)
    except Exception:
        pass
    await nc.drain()
    print(json.dumps({"part_a_overflow": a, "part_b_horizon": b}, indent=2))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--nats", default=os.environ.get("NATS_URL", "nats://localhost:4222"))
    ap.add_argument("--cap", type=int, default=1000)
    asyncio.run(main(ap.parse_args()))
