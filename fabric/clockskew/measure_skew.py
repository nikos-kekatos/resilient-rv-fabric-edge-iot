#!/usr/bin/env python3
"""Measure REAL cross-gateway clock skew as experienced by the fabric.

Unlike injecting offsets onto recorded timestamps after the fact, this reads the
`gw_ts_s` ordering timestamp that each gateway stamps with `time.time()`
(gateway.py:195). When a gateway process runs under libfaketime (or on a VM whose
clock is deliberately unsynchronised), that `time.time()` returns the faked/divergent
clock, so the skew flows through the sidecar exactly as a real deployment would see
it. We subscribe to the `gw.*.verdict` JetStream subjects, estimate each gateway's
realised clock offset (stamped time minus local arrival time), and report:

  * eps  -- the realised cross-gateway offset |off(gw_a) - off(gw_b)|, in seconds;
  * the P3.6 robustness margin: whether eps stays below the cross-gateway
    correlation window W (default 30 s), i.e. whether a genuinely skewed pair of
    gateways still lands both overflow episodes inside the same window.

This is the metric the paper currently demonstrates only with synthetic offsets;
run it against the libfaketime stack (docker-compose.skew.yml) or two VMs.
"""
import argparse, asyncio, json, os, statistics, time
import nats


async def main(a):
    nc = await nats.connect(a.nats)
    js = nc.jetstream()
    # per-gateway list of (stamped_gw_ts_s, local_arrival_wall_s)
    seen = {}
    order = []  # (arrival, gw, gw_ts) in arrival order, for cross-gw ordering check

    async def on(msg):
        now = time.time()
        gw = msg.subject.split(".")[1]           # gw.<id>.verdict
        try:
            ts = float(json.loads(msg.data.decode()).get("timestamp", 0))
        except Exception:
            return
        seen.setdefault(gw, []).append((ts, now))
        order.append((now, gw, ts))
        await msg.ack()

    sub = await js.subscribe("gw.*.verdict", durable=None, cb=on)
    print(f"listening {a.secs}s on gw.*.verdict ...")
    await asyncio.sleep(a.secs)
    await sub.unsubscribe()
    await nc.drain()

    if len(seen) < 2:
        print(json.dumps({"error": "need verdicts from >=2 gateways",
                          "gateways_seen": list(seen)}, indent=2))
        return
    # realised offset per gateway = median(stamped - arrival); a synchronised
    # gateway is ~0, a skewed one carries its injected offset/drift.
    off = {gw: statistics.median(ts - arr for ts, arr in v) for gw, v in seen.items()}
    gws = sorted(off)
    eps = max(off.values()) - min(off.values())
    # cross-gateway ordering inversions: consecutive verdicts (by arrival) from
    # DIFFERENT gateways whose stamped order disagrees with arrival order.
    inv = sum(1 for (a1, g1, t1), (a2, g2, t2) in zip(order, order[1:])
              if g1 != g2 and t2 < t1)
    out = {
        "gateways": gws,
        "realised_offset_s": {g: round(off[g], 3) for g in gws},
        "eps_cross_gateway_s": round(eps, 3),
        "p36_window_s": a.window,
        "p36_robust": eps < a.window,
        "p36_margin_s": round(a.window - eps, 3),
        "cross_gateway_ordering_inversions": inv,
        "verdicts_per_gateway": {g: len(seen[g]) for g in gws},
    }
    print(json.dumps(out, indent=2))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--nats", default=os.environ.get("NATS_URL", "nats://localhost:4222"))
    ap.add_argument("--secs", type=float, default=40, help="listen window")
    ap.add_argument("--window", type=float, default=30, help="P3.6 correlation window W")
    asyncio.run(main(ap.parse_args()))
