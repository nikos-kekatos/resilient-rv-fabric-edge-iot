#!/usr/bin/env python3
"""Live cross-gateway P3.6 firing via the REAL MonPoly engine, on the two-host stream.

Subscribes to gw.*.verdict, and for every overflow alert emits an
  @t overflow_gw("<gateway>", <count>)
timepoint into a running MonPoly process evaluating p3_6_crossgw.mfotl
(CNT g; ONCE[0,30] overflow_gw(g,_)  AND  cnt >= 2). Timepoints are stamped with a
monotone backend-arrival second (the fabric's backend clock), so MonPoly sees a
well-ordered log; P3.6 fires when overflow from >=2 distinct gateways falls in one
30s window. Prints the number of P3.6 satisfactions after --secs.

This demonstrates the fleet property firing end-to-end through the formal engine on a
genuinely two-host stream (skew resilience of the SAME property is quantified
separately by measure_eps.py).
"""
import argparse, asyncio, json, os, subprocess, time
import nats


async def main(a):
    mon = subprocess.Popen(
        ["monpoly", "-sig", a.sig, "-formula", a.formula],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
        bufsize=1, text=True)
    fires = 0
    last_t = 0

    async def reader():
        nonlocal fires
        loop = asyncio.get_event_loop()
        while True:
            line = await loop.run_in_executor(None, mon.stdout.readline)
            if not line:
                break
            # MonPoly prints a line per satisfying timepoint, e.g. "@t (tp): (2)"
            if "(" in line and "time point" in line:
                fires += 1

    nc = None
    for _ in range(60):
        try:
            nc = await nats.connect(a.nats, connect_timeout=3); break
        except Exception:
            await asyncio.sleep(2)
    if nc is None:
        nc = await nats.connect(a.nats)

    async def on(msg):
        nonlocal last_t
        try:
            d = json.loads(msg.data.decode())
        except Exception:
            return
        if d.get("type") != "overflow":
            return
        gw = msg.subject.split(".")[1]
        val = int(d.get("metadata", {}).get("count", 1))
        t = max(last_t + 0, int(time.time()))
        if t < last_t:
            t = last_t
        last_t = t
        try:
            mon.stdin.write(f'@{t} overflow_gw("{gw}", {val})\n'); mon.stdin.flush()
        except Exception:
            pass

    sub = await nc.subscribe("gw.*.verdict", cb=on)
    rt = asyncio.create_task(reader())
    await asyncio.sleep(a.secs)
    await sub.unsubscribe(); await nc.drain()
    try:
        mon.stdin.close()
    except Exception:
        pass
    await asyncio.sleep(0.5)
    mon.terminate()
    print(json.dumps({"p36_firings": fires, "engine": "monpoly (real)",
                      "note": "cross-gateway overflow within 30s window, two-host stream"}))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--nats", default=os.environ.get("NATS_URL", "nats://localhost:4222"))
    ap.add_argument("--sig", default="crossgw_specs/crossgw.sig")
    ap.add_argument("--formula", default="crossgw_specs/p3_6_crossgw.mfotl")
    ap.add_argument("--secs", type=float, default=40)
    asyncio.run(main(ap.parse_args()))
