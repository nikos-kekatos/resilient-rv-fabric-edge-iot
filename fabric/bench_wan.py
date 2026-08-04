#!/usr/bin/env python3
"""Latency-only probe for the netem-WAN sweep (Q2). Measures end-to-end
publish->subscriber-receive latency on each hop: MQTT QoS1 (device->gateway) and
NATS JetStream durable (gateway->backend). Throughput is measured separately
(bench.py, colocated); under an emulated WAN the serial JetStream publish-ack
would make a 20k burst take minutes, so it is intentionally omitted here."""
import asyncio, json, os, statistics, time
import paho.mqtt.client as mqtt
import nats

LAT_N = int(os.environ.get("LAT_N", "800"))
PACE = float(os.environ.get("PACE", "0.004"))
MQTT_HOST = os.environ.get("MQTT_HOST", "localhost")
NATS_URL = os.environ.get("NATS_URL", "nats://localhost:4222")


def pct(xs, q):
    xs = sorted(xs); return xs[min(len(xs) - 1, int(q * len(xs)))]


def summ(lat):
    return dict(n=len(lat), mean=statistics.mean(lat), p50=pct(lat, 0.50),
                p95=pct(lat, 0.95), p99=pct(lat, 0.99))


def bench_mqtt(host=MQTT_HOST):
    lat = []
    def on_msg(c, u, m):
        r = time.perf_counter(); p = json.loads(m.payload)
        if p.get("k") == "lat": lat.append((r - p["t"]) * 1e3)   # ms
    sub = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="w-sub")
    sub.on_message = on_msg; sub.connect(host, 1883)
    sub.subscribe("bench/w", qos=1); sub.loop_start()
    pub = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="w-pub")
    pub.connect(host, 1883); pub.loop_start(); time.sleep(0.5)
    for i in range(LAT_N):
        pub.publish("bench/w", json.dumps({"k": "lat", "t": time.perf_counter()}), qos=1)
        time.sleep(PACE)
    time.sleep(1.5)
    sub.loop_stop(); pub.loop_stop()
    return summ(lat)


async def bench_nats():
    nc = await nats.connect(NATS_URL); js = nc.jetstream()
    try: await js.add_stream(name="BENCHW", subjects=["bench.w"])
    except Exception: pass
    lat = []
    sub = await js.subscribe("bench.w", durable="benchwc")
    async def consume():
        while True:
            try: msg = await sub.next_msg(timeout=8)
            except Exception: return
            r = time.perf_counter(); p = json.loads(msg.data)
            if p.get("k") == "lat": lat.append((r - p["t"]) * 1e3)   # ms
            await msg.ack()
    task = asyncio.create_task(consume())
    for i in range(LAT_N):
        await js.publish("bench.w", json.dumps({"k": "lat", "t": time.perf_counter()}).encode())
        await asyncio.sleep(PACE)
    await asyncio.sleep(1.5)
    task.cancel()
    await nc.drain()
    return summ(lat)


if __name__ == "__main__":
    m = bench_mqtt(); n = asyncio.run(bench_nats())
    print(f"MQTT_QoS1 n={m['n']} mean={m['mean']:.2f} p50={m['p50']:.2f} "
          f"p95={m['p95']:.2f} p99={m['p99']:.2f} ms")
    print(f"NATS_JS   n={n['n']} mean={n['mean']:.2f} p50={n['p50']:.2f} "
          f"p95={n['p95']:.2f} p99={n['p99']:.2f} ms")
