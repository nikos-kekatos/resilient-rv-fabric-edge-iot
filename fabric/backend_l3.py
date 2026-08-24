#!/usr/bin/env python3
"""Backend L3: durable JetStream consumers feeding the REAL MonPoly + RTLola engines.

Each monitor holds an INDEPENDENT durable cursor (guarantee #4: per-consumer
offsets), so a slow MonPoly does not head-of-line-block RTLola. RTLola also
consumes fleet.tick to advance its clock under silence (guarantee #3).

LABEL NOTE (see integration contract): the task numbering calls the MonPoly+RTLola
tier "cloud_monitor", but in the actual hierarchical-rv-rtlola pipeline that logic
lives in ``correlator_monitor.py`` (``cloud_monitor.py`` is the middle tier that
produces the JSON alert dicts). We wire against real behaviour, so we reuse the
engine classes straight from ``correlator_monitor``:
  - OnlineMonPolyMonitor: persistent ``monpoly`` process per property P3.1-P3.4,
    fed the predicates overflow/time_anomaly/fuzzing/dos_spam/safe_tx.
  - OnlineRTLolaMonitor:  single persistent ``rtlola-cli`` process for the
    time-triggered silent-node property P3.5 (silent_node.lola), fed overflow/safe_tx.

WIRE SHAPE. The messages on ``gw.*.verdict`` are the cloud_monitor ALERT dicts
(``{"timestamp": int, "type": str, "device": str, "metadata": {...}}``) — the exact
object both engines' ``process_alert`` deserialise. ``fleet.tick`` carries
``{"ts_us": int}`` (no type/device); a tick is not an event but a clock pulse, used
to drain RTLola violations that fired at a @Local(0.2Hz) deadline with no new verdict.

SPEC PATHS. The engine classes reference container paths /app/monpoly_specs and
/app/rtlola_specs (per the integration contract, "all paths are container paths").
Deploy this backend with the spec dirs mounted there, exactly as
hierarchical-rv-rtlola/docker-compose.yml mounts ./monpoly_specs -> /app/monpoly_specs
and ./rtlola_specs -> /app/rtlola_specs. Read specs consulted for this wiring:
monpoly_specs/{p3_1_apt,p3_2_botnet,p3_3_escalation,p3_4_persistent}.mfotl,
monpoly_specs/signature.sig, rtlola_specs/silent_node.lola.
"""
import asyncio
import json
import os
import time
import re
import subprocess
import sys
import threading

import nats

# --- import the REAL L3 engines ----------------------------------------------
# correlator_monitor.py is vendored beside this file, so the backend is
# self-contained. It used to be imported from a sibling checkout of the
# hierarchical prototype, which existed in the development tree but not in this
# artefact -- set HIER_RV_DIR to prefer such a checkout if you have one.
_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)
HIER_RV_DIR = os.environ.get("HIER_RV_DIR")
if HIER_RV_DIR and HIER_RV_DIR not in sys.path:
    sys.path.insert(0, HIER_RV_DIR)

from correlator_monitor import (  # noqa: E402  (path injected above)
    OnlineMonPolyMonitor,
    OnlineRTLolaMonitor,
    emit_incident,
)


# Engine instances, created in main() (both spawn persistent subprocesses).
# MonPoly is a HARD dependency: its __init__ raises RuntimeError if no monpoly
# process starts, which aborts the backend by design. RTLola is best-effort.
monpoly_monitor: "OnlineMonPolyMonitor | None" = None
rtlola_monitor: "OnlineRTLolaMonitor | None" = None
crossgw_monitor = None  # P3.6, fabric-only (below)
gwsilence_monitor = None  # P3.7, fabric-only (below)

# P3.7 threshold: a gateway that has been active but then emits no verdict for
# more than this many fabric-seconds (ticks) is declared silent.
GW_SILENCE_T = 10


class CrossGatewayMonitor:
    """P3.6 cross-gateway campaign: a property the fabric ENABLES.

    The single-host shared-log prototype sees one host and cannot attribute a
    verdict to an originating gateway, so it cannot express "an overflow campaign
    spanning >=2 gateways".  The fabric tags every verdict with alert["gw"], so we
    can.  We run one persistent monpoly process over crossgw_specs and feed it
    ``@ts overflow_gw("<gw>", ts)`` for each overflow verdict; it fires when >=2
    distinct gateways show overflow within 30s (mirrors P3.2's aggregation, but
    over gateways instead of devices).  Firings are deduped per 30s window, as the
    reference engine does for coordinated_attack.
    """
    SIG = "/app/crossgw_specs/crossgw.sig"
    FORMULA = "/app/crossgw_specs/p3_6_crossgw.mfotl"

    def __init__(self):
        self.proc = None
        self.fired = 0
        self._seen = set()
        try:
            self.proc = subprocess.Popen(
                ["monpoly", "-sig", self.SIG, "-formula", self.FORMULA],
                stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                text=True, bufsize=1,
            )
            threading.Thread(target=self._read, daemon=True).start()
            print("[P3.6] cross-gateway monitor up", flush=True)
        except Exception as e:  # monpoly missing / spec error: P3.6 is best-effort
            print(f"[P3.6] disabled: {e}", flush=True)
            self.proc = None

    def _read(self):
        for line in self.proc.stdout:
            line = line.strip()
            if not line.startswith("@"):
                continue
            # A firing is a NON-EMPTY satisfying relation. MonPoly also emits empty
            # relations "@ts. (tp): ()" at unsatisfied points; require the trailing
            # tuple to carry cnt>=2 (>=2 distinct gateways), else skip.
            m = re.search(r"\(([^()]*)\)\s*$", line)
            if not m or not m.group(1).strip():
                continue
            try:
                cnt = int(m.group(1).split(",")[0])
            except Exception:
                continue
            if cnt < 2:
                continue
            try:
                # MonPoly prints "@2. (time point 1): (2)"; strip the trailing dot.
                ts = int(line[1:].split()[0].rstrip("."))
            except Exception:
                ts = 0
            key = ts // 30
            if key in self._seen:   # one incident per 30s window
                continue
            self._seen.add(key)
            self.fired += 1
            print(f"[P3.6] cross-gateway campaign #{self.fired} @ts={ts} (gws={cnt}): {line}", flush=True)

    def feed_overflow(self, gw, ts):
        if self.proc is None or self.proc.poll() is not None:
            return
        try:
            self.proc.stdin.write(f'@{int(ts)} overflow_gw("{gw}", {int(ts)})\n')
            self.proc.stdin.flush()
        except Exception:
            pass


class GatewaySilenceMonitor:
    """P3.7 gateway-silence: the cross-gateway analog of P3.5 (device silent-node).

    Fire ``gateway_silence`` when a gateway that HAS been active stops producing
    verdicts for more than GW_SILENCE_T seconds, WHILE fleet.tick keeps pulsing
    (proving the fabric itself is alive). The tick — not wall-clock — is the clock:
    we advance an internal counter on each fleet.tick and measure silence in ticks,
    exactly as the paper's liveness argument drives P3.5 (guarantee #3). Using
    fabric time means a stalled fabric (ticks also stop) does NOT falsely age
    gateways: silence is only declared when the fabric is demonstrably alive.

    WHY THIS NEEDS THE FABRIC (inexpressible in the single-gateway shared-log
    prototype). The prototype sees ONE shared log with no per-originator
    attribution, so "gateway X went dark" is indistinguishable from "gateway X
    had nothing to report", and there is no independent liveness signal to tell
    either apart from "the whole monitor stalled". The fabric supplies exactly the
    two ingredients that make the property expressible:
      (a) per-gateway verdict streams (gw.<id>.verdict) — a separate last_seen[gw]
          per origin, so we can single out the one site that stopped speaking; and
      (b) fleet.tick — a fabric-wide liveness pulse decoupled from any single
          gateway's traffic, so "this gateway is silent" can be asserted against a
          clock that is provably still running.
    Only with BOTH can the backend distinguish "a whole gateway/site went dark"
    (tick still pulsing, that gw's stream stopped) from "the fabric/backend itself
    stalled" (tick stopped too) or "quiet but healthy" (still within GW_SILENCE_T).

    Dedup per silence episode: a gateway fires at most once while continuously
    silent; it re-arms (may fire again) only after it becomes active again.
    """

    def __init__(self, t=None):
        self.t = GW_SILENCE_T if t is None else t
        self.now = 0            # fabric-time tick counter (advances on fleet.tick)
        self.last_seen = {}     # gw -> tick count at its most recent verdict
        self.armed = {}         # gw -> eligible to fire for the current episode?
        self.fired = 0
        print("[P3.7] gateway-silence monitor up", flush=True)

    def feed_verdict(self, gw):
        """Called on every gw.*.verdict: mark this gateway alive at fabric-now and
        re-arm it — a gateway that speaks again ends its current silence episode."""
        try:
            if not gw:
                return
            self.last_seen[gw] = self.now
            self.armed[gw] = True   # re-arm: a later silence episode may fire again
        except Exception:
            pass  # best-effort: never crash the backend on a malformed verdict

    def on_tick(self):
        """Called on every fleet.tick: advance fabric time and check liveness."""
        try:
            self.now += 1
            for gw, seen in list(self.last_seen.items()):
                if self.now - seen > self.t and self.armed.get(gw):
                    self.armed[gw] = False   # one incident per continuous-silence episode
                    self.fired += 1
                    print(f"[P3.7] gateway-silence #{self.fired}: gw={gw} silent >{self.t}s", flush=True)
        except Exception:
            pass  # best-effort: a tick must never take down the backend


async def drain(sub, feed):
    async for msg in sub.messages:
        feed(json.loads(msg.data))
        await msg.ack()


# --- deduplication: exactly-once EFFECT over the at-least-once outbox ----------
# The gateway outbox may replay a verdict after a crash; each consumer skips event
# ids it has already processed, so replays never double-count.
_seen = {}


def _dup(tag, v):
    eid = v.get("eid")
    if eid is None:
        return False
    s = _seen.setdefault(tag, set())
    if eid in s:
        return True
    s.add(eid)
    return False


# --- monotonicity instrumentation (Q1) ----------------------------------------
# On the SAME received verdict stream, count timestamp-order violations under
# device-time ordering (what the single-host shared log feeds MonPoly) vs
# gateway-time ordering (what the sidecar feeds). A violation = a timestamp
# strictly less than the previous one in stream order.
_mono = {"n": 0, "dev_viol": 0, "gw_viol": 0, "last_dev": None, "last_gw": None}


def _count_mono(v):
    _mono["n"] += 1
    dev, gw = v.get("dev_ts"), v.get("timestamp")
    if dev is not None and _mono["last_dev"] is not None and dev < _mono["last_dev"]:
        _mono["dev_viol"] += 1
    if gw is not None and _mono["last_gw"] is not None and gw < _mono["last_gw"]:
        _mono["gw_viol"] += 1
    if dev is not None:
        _mono["last_dev"] = dev
    if gw is not None:
        _mono["last_gw"] = gw
    if _mono["n"] % 25 == 0:
        print(f"[MONO] n={_mono['n']} device-order-viol={_mono['dev_viol']} "
              f"gateway-order-viol={_mono['gw_viol']}", flush=True)


# --- real MonPoly / RTLola feeds ----------------------------------------------
def to_monpoly(v):
    """Metric first-order cross-device correlation (P3.1-P3.4).

    ``v`` is a cloud_monitor alert dict. OnlineMonPolyMonitor.process_alert maps the
    alert ``type`` to a MonPoly predicate (unknown types return [] and are ignored),
    feeds one ``@ts predicate("device", ts)`` line to every live property process,
    then returns any incidents its reader threads have parsed.
    """
    if monpoly_monitor is None:
        return
    if not all(k in v for k in ("type", "device", "timestamp")):
        return  # not an alert (e.g. a stub/heartbeat) -> nothing to correlate
    if _dup("monpoly", v):
        return  # replayed verdict, already correlated
    _count_mono(v)   # Q1: device-order vs gateway-order monotonicity violations
    # P3.6 (fabric-only): tag overflow with its originating gateway.
    if crossgw_monitor is not None and v.get("type") == "overflow":
        crossgw_monitor.feed_overflow(v.get("gw", "?"), v["timestamp"])
    for incident in monpoly_monitor.process_alert(v):
        emit_incident(incident)


def to_rtlola(v):
    """Time-triggered silent-node property P3.5 (verdicts + ticks).

    Two message shapes arrive on this callback (verdicts and fleet.tick share it):
      - verdict/alert dict (has type+device): forward the verdict into the RTLola
        CSV stream via process_alert (any of the specification's declared input
        streams counts as "still reporting"), emitting any incidents it returns.
      - fleet.tick (``{"ts_us": ...}``, no type/device): a clock pulse. Under total
        silence no verdict arrives, so we drain the incidents that RTLola's own
        @Local(0.2Hz) deadline produced (get_pending_incidents). This is what makes
        the time-triggered property fire without new events (guarantee #3).
    """
    # P3.7 (fabric-only): the same two message shapes drive gateway-silence, and we
    # do it BEFORE the rtlola_monitor guard so P3.7 keeps working even when RTLola is
    # disabled (best-effort). A tick advances fabric time and checks liveness; a
    # verdict marks its originating gateway alive. The tick + verdict consumers share
    # one event loop, so on_tick/feed_verdict never interleave concurrently.
    if gwsilence_monitor is not None:
        if "type" not in v or "device" not in v:
            gwsilence_monitor.on_tick()          # fleet.tick: the fabric clock
        else:
            gwsilence_monitor.feed_verdict(v.get("gw"))
    if rtlola_monitor is None:
        return
    if "type" not in v or "device" not in v:
        for incident in rtlola_monitor.get_pending_incidents():
            emit_incident(incident)
        return
    if _dup("rtlola", v):
        return  # replayed verdict, already processed
    for incident in rtlola_monitor.process_alert(v):
        emit_incident(incident)


async def main():
    global monpoly_monitor, rtlola_monitor, crossgw_monitor, gwsilence_monitor

    # Start the engines before consuming (persistent monpoly + rtlola-cli processes).
    monpoly_monitor = OnlineMonPolyMonitor()          # raises RuntimeError if monpoly missing
    rtlola_monitor = OnlineRTLolaMonitor()            # silent_node.lola; best-effort
    crossgw_monitor = CrossGatewayMonitor()           # P3.6, fabric-only; best-effort
    gwsilence_monitor = GatewaySilenceMonitor()       # P3.7, fabric-only; best-effort

    nc = await nats.connect(os.environ.get("NATS_URL", "nats://localhost:4222"))
    js = nc.jetstream()

    # Ensure the stream exists before subscribing: a gateway may not have created
    # it yet (startup race). add_stream is idempotent; ignore "already exists".
    try:
        await js.add_stream(name="RVFABRIC", subjects=["gw.*.verdict", "fleet.tick"])
    except Exception:
        pass

    monpoly = await js.subscribe("gw.*.verdict", durable="monpoly")
    rtlola = await js.subscribe("gw.*.verdict", durable="rtlola")
    rtlola_tick = await js.subscribe("fleet.tick", durable="rtlola-tick")

    # Backend-resident tick (guarantee #3, preferred source): the 1 Hz liveness
    # pulse originates HERE, not at the gateways, so silence-liveness survives loss
    # of every gateway at once. Gateways no longer publish fleet.tick unless
    # RVFABRIC_TICK_SOURCE=gateway (legacy). The pulse round-trips through JetStream
    # so it reuses the exact same consumption path as before.
    async def backend_tick():
        while True:
            try:
                await js.publish("fleet.tick",
                                 json.dumps({"ts_us": time.monotonic_ns() // 1000}).encode())
            except Exception:
                pass  # best-effort: a missed pulse must never take down the backend
            await asyncio.sleep(1.0)

    tick_task = None
    if os.environ.get("RVFABRIC_TICK_SOURCE", "backend") != "gateway":
        tick_task = asyncio.create_task(backend_tick())

    try:
        await asyncio.gather(
            drain(monpoly, to_monpoly),
            drain(rtlola, to_rtlola),
            drain(rtlola_tick, to_rtlola),
        )
    finally:
        if tick_task:
            tick_task.cancel()
        rtlola_monitor.shutdown()
        monpoly_monitor.shutdown()
        await nc.drain()


if __name__ == "__main__":
    asyncio.run(main())
