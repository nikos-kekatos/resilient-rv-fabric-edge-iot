#!/usr/bin/env python3
"""Standalone P3.7 gateway-silence detector with a backend-resident tick (no MonPoly).

Publishes fleet.tick at 1 Hz (the backend clock, decoupled from any gateway) and
subscribes to gw.*.verdict. A gateway is flagged P3.7 (gateway-silence) when it has
produced no verdict for more than T seconds while the tick keeps pulsing, i.e. the
fabric is provably alive but that gateway's stream has vanished, exactly the condition
a real network partition produces. Re-arms when the gateway becomes active again.
Prints each firing and a final summary.
"""
import argparse, asyncio, json, os, time
import nats


async def main(a):
    nc = None
    for _ in range(60):
        try:
            nc = await nats.connect(a.nats, connect_timeout=3); break
        except Exception:
            await asyncio.sleep(2)
    if nc is None:
        nc = await nats.connect(a.nats)

    last_seen = {}          # gw -> monotonic time of its last verdict
    fired = set()
    events = []

    async def on(msg):
        gw = msg.subject.split(".")[1]
        last_seen[gw] = time.monotonic()
        fired.discard(gw)   # re-arm on activity

    sub = await nc.subscribe("gw.*.verdict", cb=on)

    async def tick():
        while True:
            try:
                await nc.publish("fleet.tick", json.dumps({"ts": time.monotonic()}).encode())
            except Exception:
                pass
            await asyncio.sleep(1.0)
    tt = asyncio.create_task(tick())

    t0 = time.monotonic()
    while time.monotonic() - t0 < a.secs:
        now = time.monotonic()
        for gw, ls in list(last_seen.items()):
            if now - ls > a.t and gw not in fired:
                fired.add(gw)
                ev = {"gw": gw, "silent_for_s": round(now - ls, 1), "at_s": round(now - t0, 1)}
                events.append(ev)
                print(json.dumps({"P3.7_gateway_silence": ev}), flush=True)
        await asyncio.sleep(1.0)

    tt.cancel()
    await sub.unsubscribe(); await nc.drain()
    print(json.dumps({"p37_firings": len(events), "threshold_T_s": a.t,
                      "gateways_seen": sorted(last_seen), "events": events}))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--nats", default=os.environ.get("NATS_URL", "nats://localhost:4222"))
    ap.add_argument("--t", type=float, default=6, help="silence threshold T (s)")
    ap.add_argument("--secs", type=float, default=45)
    asyncio.run(main(ap.parse_args()))
