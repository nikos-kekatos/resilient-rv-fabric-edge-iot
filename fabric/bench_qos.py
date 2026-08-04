#!/usr/bin/env python3
"""Ablation: MQTT QoS 0 (no at-least-once) vs QoS 1 loss under a fast burst,
to quantify the 'ingest loss returns' row. Env: MQTT_HOST."""
import os, time
import paho.mqtt.client as mqtt

HOST = os.environ.get("MQTT_HOST", "localhost")
N = 50000


def run(qos):
    recv = {"n": 0}
    def on(c, u, m): recv["n"] += 1
    sub = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"s{qos}")
    sub.on_message = on
    sub.connect(HOST, 1883); sub.subscribe("q/x", qos=qos); sub.loop_start()
    pub = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"p{qos}")
    pub.connect(HOST, 1883); pub.loop_start(); time.sleep(0.5)
    t0 = time.perf_counter()
    for i in range(N):
        pub.publish("q/x", str(i), qos=qos)
    time.sleep(4)
    sub.loop_stop(); pub.loop_stop()
    return recv["n"]


for q in (1, 0):
    r = run(q)
    print(f"QoS{q}: recv {r}/{N}  loss {100*(N-r)/N:.2f}%", flush=True)
