#!/usr/bin/env python3
"""Scaling sweep (Q3): sustained delivered verdict rate and end-to-end p95
latency at increasing offered load (concurrent producers) against one NATS
JetStream durable consumer, to show the backend's saturation/headroom.
Env: NATS_URL."""
import asyncio, json, os, statistics, time
import nats

NATS_URL = os.environ.get("NATS_URL", "nats://localhost:4222")
PER_PUB = 1500


async def run_load(pubs):
    nc = await nats.connect(NATS_URL)
    js = nc.jetstream()
    try:
        await js.add_stream(name=f"SC{pubs}", subjects=[f"sc{pubs}.n"])
    except Exception:
        pass
    got = {"n": 0, "first": None, "last": None}
    lat = []
    sub = await js.subscribe(f"sc{pubs}.n", durable=f"d{pubs}")

    async def consume():
        while True:
            try:
                m = await sub.next_msg(timeout=8)
            except Exception:
                return
            r = time.perf_counter()
            p = json.loads(m.data)
            got["n"] += 1
            got["first"] = got["first"] or r
            got["last"] = r
            lat.append((r - p["t"]) * 1e3)
            await m.ack()

    task = asyncio.create_task(consume())

    async def pub():
        for _ in range(PER_PUB):
            await js.publish(f"sc{pubs}.n", json.dumps({"t": time.perf_counter()}).encode())

    await asyncio.gather(*[pub() for _ in range(pubs)])
    while got["n"] < pubs * PER_PUB and (got["last"] is None or time.perf_counter() - got["last"] < 3):
        await asyncio.sleep(0.05)
    task.cancel()
    thru = got["n"] / (got["last"] - got["first"]) if got["last"] and got["last"] != got["first"] else 0
    p95 = sorted(lat)[min(len(lat) - 1, int(0.95 * len(lat)))] if lat else 0
    await nc.drain()
    return pubs, got["n"], thru, (statistics.mean(lat) if lat else 0), p95


async def main():
    print("pubs recv thru_msg_s lat_mean_ms lat_p95_ms")
    for pubs in [1, 2, 4, 8, 16, 32]:
        r = await run_load(pubs)
        print(f"{r[0]} {r[1]} {r[2]:.0f} {r[3]:.2f} {r[4]:.2f}", flush=True)


asyncio.run(main())
