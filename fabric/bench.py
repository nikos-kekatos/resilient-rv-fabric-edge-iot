#!/usr/bin/env python3
"""Substrate micro-benchmark: per-message latency (Q2, paced so the consumer
keeps up) and sustained throughput (Q3, burst with a CONCURRENT consumer) on
the MQTT (Mosquitto) and NATS JetStream hops, colocated (loopback)."""
import asyncio, json, os, statistics, time
import paho.mqtt.client as mqtt
import nats

LAT_N = 500      # paced latency samples
THRU_N = 20000   # throughput burst
MQTT_HOST = os.environ.get("MQTT_HOST", "localhost")
NATS_URL = os.environ.get("NATS_URL", "nats://localhost:4222")


def pct(xs, q):
    xs = sorted(xs); return xs[min(len(xs) - 1, int(q * len(xs)))]


def bench_mqtt(host=MQTT_HOST):
    lat = []; recv = {"n": 0, "first": None, "last": None}
    def on_msg(c, u, m):
        r = time.perf_counter(); p = json.loads(m.payload)
        if p.get("k") == "lat": lat.append((r - p["t"]) * 1e6)
        else:
            recv["n"] += 1; recv["first"] = recv["first"] or r; recv["last"] = r
    sub = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="b-sub")
    sub.on_message = on_msg; sub.connect(host, 1883); sub.subscribe("bench/x", qos=1); sub.loop_start()
    pub = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="b-pub")
    pub.connect(host, 1883); pub.loop_start(); time.sleep(0.5)
    for i in range(LAT_N):                                   # paced latency
        pub.publish("bench/x", json.dumps({"k": "lat", "t": time.perf_counter()}), qos=1)
        time.sleep(0.004)
    time.sleep(1.0)
    t0 = time.perf_counter()                                 # throughput burst
    for i in range(THRU_N):
        pub.publish("bench/x", json.dumps({"k": "t", "i": i}), qos=1)
    dl = time.perf_counter() + 30
    while recv["n"] < THRU_N and time.perf_counter() < dl: time.sleep(0.02)
    sub.loop_stop(); pub.loop_stop()
    thru = recv["n"] / (recv["last"] - recv["first"]) if recv["last"] else 0
    return dict(recv=recv["n"], sent=THRU_N, thru=thru,
                lat_mean=statistics.mean(lat), lat_p95=pct(lat, 0.95))


async def bench_nats():
    nc = await nats.connect(NATS_URL); js = nc.jetstream()
    try: await js.add_stream(name="BENCH", subjects=["bench.n"])
    except Exception: pass
    lat = []; got = {"n": 0, "first": None, "last": None}
    sub = await js.subscribe("bench.n", durable="benchc")
    async def consume():
        while True:
            try: msg = await sub.next_msg(timeout=8)
            except Exception: return
            r = time.perf_counter(); p = json.loads(msg.data)
            if p.get("k") == "lat": lat.append((r - p["t"]) * 1e6)
            else:
                got["n"] += 1; got["first"] = got["first"] or r; got["last"] = r
            await msg.ack()
    task = asyncio.create_task(consume())
    for i in range(LAT_N):                                   # paced latency
        await js.publish("bench.n", json.dumps({"k": "lat", "t": time.perf_counter()}).encode())
        await asyncio.sleep(0.004)
    await asyncio.sleep(1.0)
    for i in range(THRU_N):                                  # throughput burst
        await js.publish("bench.n", json.dumps({"k": "t", "i": i}).encode())
    while got["n"] < THRU_N and (got["last"] is None or time.perf_counter() - got["last"] < 3):
        await asyncio.sleep(0.05)
    task.cancel()
    thru = got["n"] / (got["last"] - got["first"]) if got["last"] else 0
    await nc.drain()
    return dict(recv=got["n"], sent=THRU_N, thru=thru,
                lat_mean=statistics.mean(lat), lat_p95=pct(lat, 0.95))


if __name__ == "__main__":
    m = bench_mqtt(); n = asyncio.run(bench_nats())
    print(f"MQTT  (QoS1): recv {m['recv']}/{m['sent']}  thru {m['thru']:.0f} msg/s  "
          f"lat mean {m['lat_mean']:.0f}us p95 {m['lat_p95']:.0f}us")
    print(f"NATS  (JS):   recv {n['recv']}/{n['sent']}  thru {n['thru']:.0f} msg/s  "
          f"lat mean {n['lat_mean']:.0f}us p95 {n['lat_p95']:.0f}us")
