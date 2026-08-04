#!/usr/bin/env python3
"""Gateway: MQTT subscriber -> timestamping sidecar -> REAL L1 + L2 ->
NATS JetStream (gw.<id>.verdict) + 1 Hz fleet.tick.

Tier split (matches the paper: L1@edge, L2@gateway, L3@backend):
  L1 = canonicaliser.py   per-event classification into 4 mutually-exclusive classes
  L2 = cloud_monitor.py   per-device sliding-window aggregation -> JSON alert dicts

The MonPoly + RTLola fleet correlation (L3) runs at the BACKEND (backend_l3.py),
because cross-device correlation needs the whole-fleet view, not a single gateway's.
Each L2 alert is published to gw.<id>.verdict; the backend correlator consumes it.

L1/L2 logic is mirrored in-process from canonicaliser.py / cloud_monitor.py (both are
stdin->stdout Unix filters with no importable per-event API). Constants are kept in
sync with those modules; see the TODOs to switch to direct imports if they expose one.
"""
import argparse
import asyncio
import json
import os
import time
from collections import defaultdict, deque

import paho.mqtt.client as mqtt
import nats

# L1 thresholds (canonicaliser.py) and L2 windows (cloud_monitor.py), verbatim.
TS_SKEW_S = 15
OVERFLOW_WINDOW, OVERFLOW_THRESHOLD = 30, 3
SPAM_WINDOW, SPAM_THRESHOLD = 5, 30
PULSING_WINDOW, PULSING_THRESHOLD = 2, 15


def l1_classify(evt):
    """L1 (canonicaliser.py) -> (timestamp:int, class:str, value:int).

    Mirrors canonicaliser's priority if/elif chain and thresholds exactly, including
    the asymmetric time_anomaly line (wall-clock `curr` as timestamp, spoofed `turn`
    as value).  TODO: replace with `canonicaliser.classify(evt)` if it is exposed.
    """
    t_dev, tool, a = evt["turn"], evt["tool"], evt["args"]
    # SIDECAR: order on the gateway-anchored second timestamp (monotone across a
    # gateway's devices), NOT the device turn. Device time is used only to detect
    # spoofing and as the time_anomaly witness value.
    gw_t = int(evt.get("gw_ts_s", time.time()))
    is_overflow = a["actual_sent"] > a["buffer_limit"]
    is_fuzzed = a["actual_sent"] <= 0
    is_time_spoofed = abs(gw_t - t_dev) > TS_SKEW_S
    if is_overflow and tool == "vulnerable_send":
        return gw_t, "buffer_overflow", a["actual_sent"]
    elif is_time_spoofed:
        return gw_t, "time_anomaly", t_dev
    elif is_fuzzed:
        return gw_t, "logic_fuzz_anomaly", a["actual_sent"]
    else:
        return gw_t, "safe_tx", a["actual_sent"]


class L2Aggregator:
    """L2 (cloud_monitor.py): per-device sliding windows -> alert dicts.

    Replicates cloud_monitor.process_cloud_stream's windowing but *returns* the
    alerts (type/device/timestamp/metadata) instead of printing them, so the gateway
    can publish them.  TODO: port directly if cloud_monitor exposes a stream API.
    """
    def __init__(self):
        self.overflow = defaultdict(deque)
        self.spam = defaultdict(deque)
        self.pulsing = defaultdict(deque)

    def aggregate(self, ts, cls, device):
        out = []
        def alert(t, md=None):
            out.append({"timestamp": ts, "type": t, "device": device, "metadata": md or {}})
        if cls == "time_anomaly":
            alert("time_anomaly")
        elif cls == "logic_fuzz_anomaly":
            alert("fuzzing")
        elif cls == "buffer_overflow":
            h = self.overflow[device]; h.append(ts)
            while h and h[0] < ts - OVERFLOW_WINDOW: h.popleft()
            if len(h) >= OVERFLOW_THRESHOLD:
                alert("overflow", {"count": len(h), "window_sec": OVERFLOW_WINDOW}); h.clear()
        elif cls == "safe_tx":
            alert("safe_tx")   # P3.3 escalation baseline
            s = self.spam[device]; s.append(ts)
            while s and s[0] < ts - SPAM_WINDOW: s.popleft()
            if len(s) >= SPAM_THRESHOLD:
                alert("dos_spam", {"count": len(s), "window_sec": SPAM_WINDOW}); s.clear()
            p = self.pulsing[device]; p.append(ts)
            while p and p[0] < ts - PULSING_WINDOW: p.popleft()
            if len(p) >= PULSING_THRESHOLD:
                alert("dos_spam"); p.clear()
        return out


class DurableOutbox:
    """Transactional write-ahead outbox for the gateway->backend relay.

    Per verdict, in order: (1) append the verdict to an ``fsync``'d write-ahead
    log (WAL); (2) publish it to JetStream and await the durable ack; (3) advance
    an ``fsync``'d confirmation cursor recording how many WAL entries are known
    durable at the backend. Recovery on restart replays the WAL *suffix* beyond
    the cursor, so:

      * a crash between (1) and (2) replays an un-published verdict -> recovered,
        no duplicate (it never reached the backend before);
      * a crash between (2) and (3) replays an already-published verdict -> the
        backend's event-id dedup collapses it (exactly-once *effect*);
      * only a crash before (1) drops the in-flight verdict -- the residual
        window between the MQTT ack and the WAL persist, which the outbox cannot
        close (stated as residual risk in the paper).

    The cursor also bounds recovery cost: confirmed entries are never replayed,
    so the WAL does not force an unbounded replay after a long uptime.

    Fault injection (testing only; the three ``CRASH_AT`` points map to the three
    rows above) is driven by env vars and is inert when they are unset.
    """
    def __init__(self, wal_path, cursor_path=None):
        self.wal_path = wal_path
        self.cursor_path = cursor_path or (wal_path + ".cursor")
        self.confirmed = 0          # WAL entries known durable at the backend
        self.wal = None
        self._n = 0                 # verdicts relayed this process (for fault injection)
        self._crash_at = os.environ.get("CRASH_AT")            # before_persist|after_persist|after_publish
        self._crash_n = int(os.environ.get("CRASH_AFTER_N", "-1"))

    def _read_cursor(self):
        try:
            with open(self.cursor_path) as f:
                return int((f.read().strip() or "0"))
        except (FileNotFoundError, ValueError):
            return 0

    def _persist_cursor(self):
        # small file; open/fsync/close per advance keeps the on-disk cursor
        # crash-consistent without carrying a mutable handle.
        with open(self.cursor_path, "w") as c:
            c.write(str(self.confirmed))
            c.flush()
            os.fsync(c.fileno())

    def _maybe_crash(self, point):
        if self._crash_at == point and self._n == self._crash_n:
            print(f"[outbox] FAULT-INJECT hard-crash at '{point}' (verdict #{self._n})", flush=True)
            os._exit(137)   # hard exit: no flush of un-fsync'd buffers, like SIGKILL

    async def recover(self, js, subject):
        """Replay WAL entries beyond the confirmed cursor. Returns #replayed."""
        self.confirmed = self._read_cursor()
        replayed = 0
        if os.path.exists(self.wal_path):
            with open(self.wal_path) as f:
                lines = [ln.strip() for ln in f if ln.strip()]
            for ln in lines[self.confirmed:]:
                await js.publish(subject, ln.encode())
                replayed += 1
            self.confirmed = len(lines)   # everything is now (re)published & durable
            self._persist_cursor()
        return replayed

    def open(self):
        self.wal = open(self.wal_path, "a")

    async def append_and_publish(self, js, subject, alert):
        line = json.dumps(alert)
        self._maybe_crash("before_persist")
        self.wal.write(line + "\n")            # (1) WAL append + fsync
        self.wal.flush()
        os.fsync(self.wal.fileno())
        self._maybe_crash("after_persist")
        await js.publish(subject, line.encode())   # (2) durable JetStream publish + ack
        self._maybe_crash("after_publish")
        self.confirmed += 1                    # (3) advance fsync'd ack cursor
        self._persist_cursor()
        self._n += 1


class Gateway:
    def __init__(self, gw_id, mqtt_host, nats_url):
        self.gw_id = gw_id
        self.mqtt_host = mqtt_host
        self.nats_url = nats_url
        self.queue = None          # bound to the running loop in run()
        self.loop = None
        self.l2 = L2Aggregator()
        self.seq = defaultdict(int)   # per-device sequence -> stable event ids
        wal_path = os.environ.get("OUTBOX", f"/tmp/rvfabric-outbox-{gw_id}.jsonl")
        self.outbox = DurableOutbox(wal_path)   # WAL + fsync'd ack cursor

    # --- timestamping sidecar: gateway-anchored monotonic microseconds (guarantee #2) ---
    def stamp(self, evt):
        evt["gw_ts_us"] = time.monotonic_ns() // 1000   # monotone per gateway (diagnostics)
        evt["gw_ts_s"] = int(time.time())               # gateway-anchored ORDERING time (seconds)
        evt["dev_ts"] = evt.get("turn")                 # device time: spoof-check + diagnostics, never ordering
        return evt

    def on_message(self, client, userdata, msg):
        evt = self.stamp(json.loads(msg.payload))
        self.loop.call_soon_threadsafe(self.queue.put_nowait, evt)

    async def tick(self, js):
        # 1 Hz clock-tick so the backend's time-triggered (RTLola) monitor fires under
        # total device silence (guarantee #3); the gateway only forwards the pulse.
        while True:
            await js.publish("fleet.tick", json.dumps({"ts_us": time.monotonic_ns() // 1000}).encode())
            await asyncio.sleep(1.0)

    def _eid(self, alert):
        d = alert["device"]
        alert["eid"] = f"{self.gw_id}:{d}:{self.seq[d]}"   # stable per-device event id
        self.seq[d] += 1

    async def run(self):
        self.loop = asyncio.get_running_loop()   # the loop on_message must schedule onto
        self.queue = asyncio.Queue()
        mc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"gw-{self.gw_id}")
        mc.on_message = self.on_message
        mc.connect(self.mqtt_host, 1883)
        mc.subscribe(f"dev/{self.gw_id}/+/evt", qos=1)   # only this gateway's homed devices
        mc.loop_start()

        nc = await nats.connect(self.nats_url)
        js = nc.jetstream()
        try:
            await js.add_stream(name="RVFABRIC", subjects=["gw.*.verdict", "fleet.tick"])
        except Exception:
            pass  # backend may have created it
        print(f"[gw-{self.gw_id}] up: MQTT dev/{self.gw_id}/+/evt -> L1/L2 -> NATS gw.{self.gw_id}.verdict", flush=True)

        # --- outbox recovery: replay un-confirmed verdicts from a prior instance ---
        replayed = await self.outbox.recover(js, f"gw.{self.gw_id}.verdict")
        if replayed:
            print(f"[gw-{self.gw_id}] outbox: replayed {replayed} un-acked verdicts on restart", flush=True)
        self.outbox.open()

        # Tick source (guarantee #3): the PREFERRED source is the backend, so the
        # silence-liveness pulse survives loss of ALL gateways. Set
        # RVFABRIC_TICK_SOURCE=gateway to fall back to per-gateway ticks (the old
        # behaviour, which stops if every gateway dies).
        if os.environ.get("RVFABRIC_TICK_SOURCE", "backend") == "gateway":
            asyncio.create_task(self.tick(js))
        npub = 0
        while True:
            evt = await self.queue.get()
            ts, cls, _value = l1_classify(evt)                       # L1
            for alert in self.l2.aggregate(ts, cls, evt["actor"]):   # L2
                alert["gw"] = self.gw_id
                alert["gw_ts_us"] = evt["gw_ts_us"]
                alert["dev_ts"] = evt.get("dev_ts")                  # device time (for the monotonicity comparison)
                self._eid(alert)                                     # stable event id
                await self.outbox.append_and_publish(js, f"gw.{self.gw_id}.verdict", alert)
                npub += 1
                if alert["type"] != "safe_tx":   # log attack-class alerts (not the safe baseline)
                    print(f"[gw-{self.gw_id}] L2 alert #{npub}: {alert['type']} <- {alert['device']}", flush=True)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--id", required=True)
    ap.add_argument("--mqtt", default="localhost")
    ap.add_argument("--nats", default="nats://localhost:4222")
    a = ap.parse_args()
    asyncio.run(Gateway(a.id, a.mqtt, a.nats).run())
