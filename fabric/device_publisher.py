#!/usr/bin/env python3
"""Emulated IoT device: publishes tool_call events to MQTT dev/<id>/evt at QoS 1.

Sources events exactly as hierarchical-rv-rtlola/iot_app.py produces them: same
per-event schema (keys turn/actor/kind/tool/args) and the same profile-driven
generators (safe/overflow/timespoof/stealth/fuzz/spam/pulsing/mixed). Instead of
appending JSON lines to /shared_data/events.log, each event is published to the
MQTT topic dev/<id>/evt so the canonicaliser (L1) can consume it off the broker.

Either replays a JSONL trace (already in the iot_app schema) or generates events
live from --profile.

QoS 1 gives at-least-once delivery (paper guarantee #1); L1 is idempotent per
(device, window) so the duplicates at-least-once may introduce are harmless.
"""
import argparse
import json
import os
import random
import time

import paho.mqtt.client as mqtt

# --- true randomness, as in iot_app.py ---
random.seed(os.urandom(16))

# BUFFER_SIZE from iot_app.py / integration contract: buffer_limit is always 30.
BUFFER_SIZE = 30

# Profiles supported by iot_app.py (contract: PROFILE env, default "normal").
PROFILES = (
    "normal", "overflow", "timespoof", "spam",
    "stealth", "fuzzer", "pulsing", "mixed",
)


def make_raw_event(device_id, event_type, attempted_size, actual_sent, fake_time=None):
    """Build one raw device event, byte-for-byte matching iot_app.emit_raw_event.

    Contract event_schema: the time key is "turn" (NOT "timestamp"); for the
    timespoof profile it carries a spoofed epoch (fake_time). buffer_limit is
    always BUFFER_SIZE. Downstream L1 only reads turn/actor/tool/args.actual_sent/
    args.buffer_limit, but we emit the full object to stay schema-faithful.
    """
    current_time = fake_time if fake_time else int(time.time())
    return {
        "turn": current_time,
        "actor": device_id,
        "kind": "tool_call",
        "tool": event_type,
        "args": {
            "attempted_size": attempted_size,
            "actual_sent": actual_sent,
            "buffer_limit": BUFFER_SIZE,
        },
    }


# --- per-action generators, mirroring iot_app.py exactly ---
def gen_safe_tx(device_id):
    payload = random.randint(10, 30)
    return make_raw_event(device_id, "safe_send", payload, payload)


def gen_overflow_attack(device_id):
    payload = random.randint(35, 60)
    return make_raw_event(device_id, "vulnerable_send", payload, payload)


def gen_time_spoof_attack(device_id):
    payload = random.randint(10, 30)
    offset = random.choice([-600, -300, 300, 600])
    fake_time = int(time.time()) + offset
    return make_raw_event(device_id, "time_spoof_send", payload, payload, fake_time=fake_time)


def gen_stealth_overflow(device_id):
    payload = random.choice([31, 32])  # borderline overflow
    return make_raw_event(device_id, "vulnerable_send", payload, payload)


def gen_fuzz_attack(device_id):
    payload = random.choice([-15, 0, 9999])  # negative, zero, or huge sizes
    # iot_app: >BUFFER_SIZE stays vulnerable_send (classifies as overflow),
    # negative/zero become fuzz_send (classifies as logic_fuzz_anomaly).
    tool = "vulnerable_send" if payload > BUFFER_SIZE else "fuzz_send"
    return make_raw_event(device_id, tool, payload, payload)


def profile_step(profile, device_id, publish):
    """Run one iteration of iot_app.py's main loop for the given profile,
    publishing each generated event via `publish` and sleeping as iot_app does.
    """
    # iot_app.py: ~15% of iterations are an idle pause with no event emitted.
    if random.random() < 0.15:
        time.sleep(random.uniform(0.5, 1.5))
        return

    if profile == "normal":
        publish(gen_safe_tx(device_id))
        time.sleep(random.uniform(0.1, 4.0))

    elif profile == "overflow":
        if random.random() < 0.8:
            publish(gen_overflow_attack(device_id))
        else:
            publish(gen_safe_tx(device_id))
        time.sleep(random.uniform(4.0, 10.0))

    elif profile == "timespoof":
        publish(gen_time_spoof_attack(device_id))
        time.sleep(random.uniform(2.0, 6.0))

    elif profile == "spam":
        publish(gen_safe_tx(device_id))
        time.sleep(random.uniform(0.005, 0.05))

    elif profile == "stealth":
        if random.random() < 0.90:
            publish(gen_safe_tx(device_id))
        else:
            publish(gen_stealth_overflow(device_id))
        time.sleep(random.uniform(0.5, 3.0))

    elif profile == "fuzzer":
        if random.random() < 0.6:
            publish(gen_fuzz_attack(device_id))
        else:
            publish(gen_safe_tx(device_id))
        time.sleep(random.uniform(0.5, 2.0))

    elif profile == "pulsing":
        time.sleep(random.uniform(20.0, 30.0))
        for _ in range(20):
            publish(gen_safe_tx(device_id))
            time.sleep(0.05)

    elif profile == "mixed":
        action = random.choices(
            [gen_safe_tx, gen_overflow_attack, gen_time_spoof_attack],
            weights=[0.6, 0.2, 0.2],
        )[0]
        publish(action(device_id))
        time.sleep(random.uniform(0.2, 3.5))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--broker", default="localhost")
    ap.add_argument("--port", type=int, default=1883)
    ap.add_argument("--id", required=True, help="device id, e.g. node-1")
    ap.add_argument("--trace", help="JSONL trace to replay (iot_app schema); omit to generate live")
    ap.add_argument("--rate", type=float, default=0.5, help="seconds between events (trace replay only)")
    ap.add_argument("--qos", type=int, default=1, help="1=at-least-once (set 0 for the loss baseline, Q1)")
    ap.add_argument("--profile", default=os.environ.get("PROFILE", "normal"), choices=PROFILES,
                    help="live-generation profile, as in iot_app.py PROFILE env")
    ap.add_argument("--gw", default="gw1", help="gateway this device is homed to (topic partition)")
    args = ap.parse_args()

    c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"dev-{args.id}")
    c.connect(args.broker, args.port)
    c.loop_start()
    # Topic carries the home gateway so each gateway subscribes to only its own
    # devices (dev/<gw>/+/evt): disjoint fleets, no cross-gateway double-counting.
    topic = f"dev/{args.gw}/{args.id}/evt"

    def publish(evt):
        # Force actor to this device id so the published payload is authoritative.
        evt["actor"] = args.id
        # QoS 1 (guarantee #1): at-least-once delivery to the L1 canonicaliser.
        c.publish(topic, json.dumps(evt), qos=args.qos)

    try:
        if args.trace:
            # Replay a pre-recorded trace already in the iot_app event schema.
            for line in open(args.trace):
                line = line.strip()
                if line:
                    publish(json.loads(line))
                    time.sleep(args.rate)
        else:
            # Live generation matching iot_app.py's __main__ loop.
            startup_delay = random.uniform(1.0, 5.0)
            time.sleep(startup_delay)
            print(f"Node {args.id} started with profile: [{args.profile}] "
                  f"after {startup_delay:.2f}s delay", flush=True)
            while True:
                profile_step(args.profile, args.id, publish)
    finally:
        c.loop_stop()


if __name__ == "__main__":
    main()
