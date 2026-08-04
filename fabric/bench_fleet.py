#!/usr/bin/env python3
"""Fleet load emulation (Q4 scaling): emulate D devices, each an independent
publisher at R msg/s for T seconds, feeding one durable JetStream consumer.
Reports delivered/sent (loss), latency mean/p95, sustained rate. This emulates a
1000-device fleet's aggregate verdict load through the real broker without
1000 containers. Env: NATS_URL."""
import asyncio, json, os, statistics, time
import nats

NATS_URL = os.environ.get("NATS_URL", "nats://localhost:4222")


async def fleet(D, R, T):
    nc = await nats.connect(NATS_URL)
    js = nc.jetstream()
    try:
        await js.add_stream(name=f"FL{D}", subjects=[f"fl{D}.n"])
    except Exception:
        pass
    got = {"n": 0, "first": None, "last": None}
    lat = []
    flag = {"stop": False}
    sub = await js.subscribe(f"fl{D}.n", durable=f"fd{D}")

    async def consume():
        while not flag["stop"]:
            try:
                m = await sub.next_msg(timeout=3)
            except Exception:
                continue
            r = time.perf_counter()
            p = json.loads(m.data)
            got["n"] += 1
            got["first"] = got["first"] or r
            got["last"] = r
            lat.append((r - p["t"]) * 1e3)
            await m.ack()

    task = asyncio.create_task(consume())
    sent = {"n": 0}

    async def dev():
        for _ in range(int(R * T)):
            await js.publish(f"fl{D}.n", json.dumps({"t": time.perf_counter()}).encode())
            sent["n"] += 1
            await asyncio.sleep(1.0 / R)

    await asyncio.gather(*[dev() for _ in range(D)])
    deadline = time.perf_counter() + 8
    while got["n"] < sent["n"] and time.perf_counter() < deadline:
        await asyncio.sleep(0.1)
    flag["stop"] = True
    task.cancel()
    thru = got["n"] / (got["last"] - got["first"]) if got["last"] and got["last"] != got["first"] else 0
    p95 = sorted(lat)[min(len(lat) - 1, int(0.95 * len(lat)))] if lat else 0
    await nc.drain()
    return D, sent["n"], got["n"], thru, (statistics.mean(lat) if lat else 0), p95


async def main():
    print("devices sent recv loss% thru_msg_s lat_mean_ms p95_ms")
    for D in [250, 500, 1000]:
        d, s, r, t, m, p = await fleet(D, R=2.0, T=10)
        print(f"{d} {s} {r} {100*(s-r)/s:.2f} {t:.0f} {m:.2f} {p:.2f}", flush=True)


asyncio.run(main())
