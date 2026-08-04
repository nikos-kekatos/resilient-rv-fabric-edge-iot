#!/usr/bin/env python3
"""Clean cross-gateway clock-skew (epsilon) probe using periodic clock beacons.

Each gateway mirror publishes its faketime-skewed clock on clock.<id>; here we record
the host receipt time for every beacon and estimate each mirror's offset as
median(beacon_clock - host_recv). Because both mirrors sit behind the same Docker-VM
base clock and are measured against the same host clock, that shared base cancels in
    eps = offset(gw2) - offset(gw1),
leaving the true injected inter-gateway skew. A zero-offset control therefore reads
eps ~ 0 (sub-100 ms), unlike the alert-event probe whose control read ~13 s.

We also report whether eps stays below the P3.6 correlation window W, i.e. whether a
genuinely skewed pair of gateways still lands both overflow episodes in one window.
"""
import argparse, asyncio, json, os, statistics, time
import nats


async def main(a):
    nc = await nats.connect(a.nats)
    samples = {}

    async def cb(m):
        recv = time.time()
        try:
            d = json.loads(m.data.decode())
        except Exception:
            return
        samples.setdefault(d["id"], []).append(d["clock"] - recv)

    sub = await nc.subscribe("clock.*", cb=cb)
    await asyncio.sleep(a.secs)
    await sub.unsubscribe()
    await nc.drain()

    ids = sorted(samples)
    if len(ids) < 2:
        print(json.dumps({"error": "need beacons from >=2 gateways", "seen": ids}, indent=2))
        return
    off = {g: statistics.median(samples[g]) for g in ids}
    # spread within each mirror (network jitter) -> the measurement's own noise floor
    jitter = {g: round((max(samples[g]) - min(samples[g])) * 1000, 1) for g in ids}
    eps = max(off.values()) - min(off.values())
    out = {
        "offsets_s": {g: round(off[g], 3) for g in ids},
        "eps_cross_gateway_s": round(eps, 3),
        "p36_window_s": a.window,
        "p36_robust": eps < a.window,
        "p36_margin_s": round(a.window - eps, 3),
        "samples_per_gateway": {g: len(samples[g]) for g in ids},
        "per_gateway_jitter_ms": jitter,
    }
    print(json.dumps(out, indent=2))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--nats", default=os.environ.get("NATS_URL", "nats://localhost:4222"))
    ap.add_argument("--secs", type=float, default=20)
    ap.add_argument("--window", type=float, default=30)
    asyncio.run(main(ap.parse_args()))
