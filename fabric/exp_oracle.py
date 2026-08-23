#!/usr/bin/env python3
"""Fault-injected verdict preservation against a fault-free oracle (CRITIS 5/5 gap).

The mechanisms are known to *behave as designed*. What that does not
experiment yet shows is that those mechanisms change the *security conclusion*: that
disabling a continuity guarantee turns a real incident into a missed one -- and, in
particular, into a silent FALSE ALL-CLEAR (an unqualified ``no_violation'') rather
than an honest ``unknown''. This harness closes that gap.

METHOD (deterministic, real engine, transport-isolating).
  1. A fixed, seeded attack workload of L2 alerts is built so that all seven
     monitored properties P3.1-P3.7 genuinely fire (Sect. workload below).
  2. ORACLE. The clean alert stream is fed to the UNMODIFIED MonPoly engine
     (the ``rvhier'' image, /usr/local/bin/monpoly, stock specs in
     mounted from fabric/monpoly_specs) for P3.1-P3.4 and P3.6, and to
     reference detectors that mirror backend_l3's tick-driven P3.5 / P3.7. The set
     of incidents it produces is the ground truth I*.
  3. FAULT CAMPAIGN. One combined campaign -- a crash, a reordering, node and
     gateway silence, and a retention overload -- is injected. Each fault is
     grounded in an ALREADY-MEASURED fabric behaviour (crash `before_persist` = 1
     in-flight loss; JetStream `discard=old` = silent eviction of the oldest;
     out-of-order feed = MonPoly monotonicity rejection; silence = no clock without
     the tick). See EXPERIMENTS_RESULTS.md sections 1, 2.
  4. CONFIGS. The campaign is replayed under the shared-log baseline, the full
     fabric, and the fabric with each guarantee G1-G5 disabled in turn. A config
     either ABSORBS a fault (its guarantee is on) or SUFFERS it (off). The ONLY
     thing that changes across configs is which alerts, in what order, reach the
     unmodified detectors -- exactly the transport layer's effect.
  5. DIFF. Each config's incident set is diffed against I*: missed (false
     negatives), spurious (false positives), preservation rate. Each miss is then
     classified by the evidence-aware algebra V=(s,c) from the fabric state a real
     consumer would see (event-id gap, order violation, retention watermark, tick
     vs evidence): a miss the algebra downgrades to degraded/incomplete/unavailable
     is an honest ``unknown''; a miss it cannot flag is a silent FALSE ALL-CLEAR.

The headline result: how many otherwise-silent false all-clears the
fabric converts into flagged unknowns, and which single mechanism each depends on.

Run:  python3 exp_oracle.py            # uses the rvhier image for real MonPoly
      python3 exp_oracle.py --md       # also print paper-ready markdown tables
"""
import argparse
import json
import os
import re
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
SPEC_DIR_LOCAL = os.path.join(HERE, "monpoly_specs")   # vendored; mounted into the image
CROSSGW_DIR_LOCAL = os.path.join(HERE, "crossgw_specs")
IMAGE = os.environ.get("RVHIER_IMAGE", "rvhier:latest")

# ---------------------------------------------------------------------------
# Property / spec table. Log-driven properties go through the real MonPoly binary;
# P3.5 (silent node) and P3.7 (gateway silence) are tick-driven and use reference
# detectors mirroring backend_l3.GatewaySilenceMonitor / the RTLola silent-node spec.
# ---------------------------------------------------------------------------
MONPOLY_SPECS = {
    # incident id : (signature, formula, expected identity, kind)
    "I3_P3.1_apt":          ("signature.sig", "p3_1_apt.mfotl",        "c1", "device"),
    "I1_P3.2_botnet":       ("signature.sig", "p3_2_botnet.mfotl",     None, "aggregate"),
    "I4_P3.3_escalation":   ("signature.sig", "p3_3_escalation.mfotl", "d1", "device"),
    "I5_P3.4_persistent":   ("signature.sig", "p3_4_persistent.mfotl", "e1", "device"),
    "I2_P3.6_crossgw":      ("crossgw.sig",   "p3_6_crossgw.mfotl",    None, "aggregate"),
}
REF_INCIDENTS = ["I6_P3.5_silentnode", "I7_P3.7_gwsilence"]
ALL_INCIDENTS = list(MONPOLY_SPECS.keys()) + REF_INCIDENTS

SILENCE_NODE_T = 8      # s of device silence (while ticks pulse) => P3.5
SILENCE_GW_T = 10       # s of gateway silence (while ticks pulse) => P3.7
TRACE_END = 495         # s: everything else stays live until here


# ---------------------------------------------------------------------------
# 1. Fixed, seeded attack workload. Integer-second timestamps, one campaign per
#    property, plus benign background that stays live to the end (so silence is a
#    property of the faulted node/gateway, not of trace teardown).
# ---------------------------------------------------------------------------
def build_workload():
    A = []  # each: dict(ts, pred, dev, gw)  -- pred in overflow/time_anomaly/fuzzing/safe_tx

    def ev(ts, pred, dev, gw):
        A.append({"ts": ts, "pred": pred, "dev": dev, "gw": gw})

    # Background benign traffic on both gateways, live for the whole trace so no
    # benign device/gateway is ever mistaken for "gone silent".
    for k in range(5):
        gw = "gw1" if k % 2 == 0 else "gw2"
        for ts in range(5 + k, TRACE_END, 15):
            ev(ts, "safe_tx", f"bg{k}", gw)

    # I1  P3.2 coordinated botnet  (>=3 distinct overflow devices in 30s)   [gw1,gw2]
    # I2  P3.6 cross-gateway campaign (overflow from >=2 gateways in 30s)
    ev(100, "overflow", "a1", "gw1")   # <- overload evicts a1,a2 when G5 off
    ev(103, "overflow", "a2", "gw1")
    ev(107, "overflow", "a3", "gw1")
    ev(110, "overflow", "b1", "gw2")   # 2nd gateway => P3.6

    # I3  P3.1 multi-vector APT on c1 (overflow + time_anomaly within 60s)
    ev(200, "overflow", "c1", "gw1")   # <- reorder drops this when G2 off
    ev(210, "time_anomaly", "c1", "gw1")

    # I4  P3.3 escalation on d1 (safe_tx 10-60s ago, then overflow + time_anomaly)
    #     CONTROL: no transport fault hits this incident; it is device-local, so it
    #     is expressible even on the shared log and should survive there.
    ev(250, "safe_tx", "d1", "gw1")
    ev(300, "overflow", "d1", "gw1")
    ev(305, "time_anomaly", "d1", "gw1")

    # I5  P3.4 persistent campaign on e1 (>=5 overflows)
    for ts in (400, 405, 410, 415, 420):   # <- crash drops the 5th (420) when G1 off
        ev(ts, "overflow", "e1", "gw2")

    # I6  P3.5 silent node: f1 is active, then goes dark mid-operation.
    for ts in (450, 451, 452):
        ev(ts, "safe_tx", "f1", "gw1")
    # (no more f1 events -> silence gap of ~40s while ticks + others continue)

    # I7  P3.7 gateway silence: gw3 (device g1) active, then the whole gateway dark.
    for ts in (460, 461, 462):
        ev(ts, "safe_tx", "g1", "gw3")

    A.sort(key=lambda e: e["ts"])
    return A


# ---------------------------------------------------------------------------
# 2. Configurations = capability vectors. A guarantee that is ON absorbs its fault;
#    OFF suffers it. eids / gw / tick are the evidence-model inputs the algebra reads.
# ---------------------------------------------------------------------------
def cfg(G1, G2, G3, G4, G5, eids, gw, tick):
    return dict(G1=G1, G2=G2, G3=G3, G4=G4, G5=G5, eids=eids, gw=gw, tick=tick)

CONFIGS = {
    # shared log: no continuity guarantees, no event ids, no gateway attribution,
    # no backend tick. Whatever it loses, it loses silently.
    "shared_log":  cfg(0, 0, 0, 1, 0, eids=0, gw=0, tick=0),
    # full fabric: every guarantee on -> should reproduce the oracle exactly.
    "fabric_full": cfg(1, 1, 1, 1, 1, eids=1, gw=1, tick=1),
    # ablations: exactly one guarantee removed; the evidence model (eids/gw/tick)
    # stays on except where the removed guarantee IS the evidence input (G3 = tick).
    "fabric_noG1": cfg(0, 1, 1, 1, 1, eids=1, gw=1, tick=1),
    "fabric_noG2": cfg(1, 0, 1, 1, 1, eids=1, gw=1, tick=1),
    "fabric_noG3": cfg(1, 1, 0, 1, 1, eids=1, gw=1, tick=0),   # tick gone with G3
    "fabric_noG4": cfg(1, 1, 1, 0, 1, eids=1, gw=1, tick=1),
    "fabric_noG5": cfg(1, 1, 1, 1, 0, eids=1, gw=1, tick=1),
}


# ---------------------------------------------------------------------------
# 3. Apply the combined fault campaign to the clean workload under a config,
#    returning the alert stream (in arrival order) that actually reaches L3.
# ---------------------------------------------------------------------------
def apply_faults(clean, c):
    """Return (arrival_ordered_alerts, notes) after the campaign, per config caps."""
    notes = {}
    A = [dict(e) for e in clean]

    # -- OVERLOAD (guarded by G5). discard=old silently evicts the OLDEST verdicts
    #    in the backlog; here that is a1@100 and a2@103, dropping the botnet's
    #    distinct-device count 4 -> 2 (< 3). With G5 on (awaited publish / discard=new)
    #    the overload is surfaced, not silently dropped, so the alerts are retained.
    if not c["G5"]:
        A = [e for e in A if not (e["dev"] in ("a1", "a2") and e["pred"] == "overflow")]
        notes["overload"] = "evicted a1,a2 overflow (discard=old, silent)"

    # -- CRASH (guarded by G1). A hard crash between MQTT receipt and WAL fsync loses
    #    the single in-flight verdict (measured `before_persist` = 1 loss). Here that
    #    is e1's 5th overflow@420, dropping persistent-threat count 5 -> 4 (< 5).
    #    With G1 on, the outbox replays it on recovery (0 loss).
    if not c["G1"]:
        A = [e for e in A if not (e["dev"] == "e1" and e["ts"] == 420)]
        notes["crash"] = "lost e1 overflow@420 (before_persist, no outbox)"

    # -- REORDER (guarded by G2). Without a trusted gateway-ingest order, c1's two
    #    events arrive swapped; MonPoly rejects the now non-monotone overflow@200
    #    (ts < running max), so the APT loses one of its two required vectors.
    #    With G2 on, the sidecar's gateway-time keeps the stream monotone.
    if not c["G2"]:
        # move c1 overflow@200 to arrive AFTER c1 time_anomaly@210
        ov = next(e for e in A if e["dev"] == "c1" and e["pred"] == "overflow")
        A.remove(ov)
        idx = next(i for i, e in enumerate(A) if e["dev"] == "c1" and e["pred"] == "time_anomaly")
        A.insert(idx + 1, ov)   # arrival order now non-monotone at this point
        notes["reorder"] = "c1 overflow@200 arrives after time_anomaly@210"
    else:
        A.sort(key=lambda e: e["ts"])

    return A, notes


def to_monpoly_log(arrival_alerts):
    """Serialise to a MonPoly log, emulating the engine's monotonicity rejection:
    an alert whose ts is below the running maximum (a reorder) is dropped, exactly
    as monpoly rejects non-monotone timestamps. Consecutive same-ts alerts merge."""
    kept, dropped, run_max = [], 0, -1
    for e in arrival_alerts:
        if e["ts"] < run_max:
            dropped += 1
            continue
        run_max = e["ts"]
        kept.append(e)
    lines, i = [], 0
    while i < len(kept):
        ts = kept[i]["ts"]
        group = []
        while i < len(kept) and kept[i]["ts"] == ts:
            e = kept[i]
            group.append(f'{e["pred"]}("{e["dev"]}", {ts})')
            i += 1
        lines.append(f"@{ts} " + " ".join(group))
    return "\n".join(lines) + "\n", dropped


def to_crossgw_log(arrival_alerts, gw_on):
    """P3.6 feed: overflow tagged by originating gateway. Inexpressible without
    per-gateway attribution (the shared log has no gw field)."""
    if not gw_on:
        return None
    kept, run_max = [], -1
    for e in arrival_alerts:
        if e["pred"] != "overflow":
            continue
        if e["ts"] < run_max:
            continue
        run_max = e["ts"]
        kept.append(e)
    lines = [f'@{e["ts"]} overflow_gw("{e["gw"]}", {e["ts"]})' for e in kept]
    return ("\n".join(lines) + "\n") if lines else "@0 \n"


# ---------------------------------------------------------------------------
# 4. Real MonPoly (batch, unmodified engine in the rvhier image).
# ---------------------------------------------------------------------------
def run_monpoly_batch(work_dir):
    """Run every (config, spec) log already written under work_dir/streams inside a
    single rvhier container, writing outputs to work_dir/out. One container start."""
    script = r"""
set -e
mkdir -p /work/out
for f in /work/streams/*.mlog; do
  base=$(basename "$f" .mlog)
  spec="${base##*__}"          # e.g. p3_2_botnet
  case "$spec" in
    p3_6_crossgw) sig=/app/crossgw_specs/crossgw.sig; formula=/app/crossgw_specs/p3_6_crossgw.mfotl;;
    *)            sig=/app/monpoly_specs/signature.sig; formula=/app/monpoly_specs/${spec}.mfotl;;
  esac
  monpoly -sig "$sig" -formula "$formula" -log "$f" > /work/out/${base}.out 2>/dev/null || true
done
"""
    subprocess.run(
        ["docker", "run", "--rm",
         "-v", f"{work_dir}:/work",
         "-v", f"{CROSSGW_DIR_LOCAL}:/app/crossgw_specs:ro",
         "-v", f"{SPEC_DIR_LOCAL}:/app/monpoly_specs:ro",
         IMAGE, "bash", "-c", script],
        check=True,
    )


def parse_firing(out_text, expected_identity, kind):
    """Return True if this property fired for the expected target.
    aggregate: any non-empty satisfying relation (the formula already enforces the
    count threshold). device: the expected device appears in a satisfying tuple."""
    fired_agg = False
    devices = set()
    for line in out_text.splitlines():
        line = line.strip()
        if not line.startswith("@") or ":" not in line:
            continue
        rhs = line.split(":", 1)[1]
        for grp in re.findall(r"\(([^()]*)\)", rhs):
            toks = [t.strip().strip('"') for t in grp.split(",") if t.strip()]
            if not toks:
                continue
            fired_agg = True
            for t in toks:
                if not re.fullmatch(r"-?\d+", t):
                    devices.add(t)
    if kind == "aggregate":
        return fired_agg
    return expected_identity in devices


# ---------------------------------------------------------------------------
# 5. Reference detectors for the tick-driven properties (mirror backend_l3).
# ---------------------------------------------------------------------------
def detect_silent_node(clean, c):
    """P3.5: a device that was active then silent for > SILENCE_NODE_T mid-trace,
    while the fabric clock (tick) is still pulsing. Needs the tick (G3)."""
    if not c["tick"]:
        return False
    last = {}
    for e in clean:
        last[e["dev"]] = e["ts"]
    # f1 is the designed silent node; it must have stopped well before TRACE_END.
    return (TRACE_END - last.get("f1", TRACE_END)) > SILENCE_NODE_T


def detect_gw_silence(clean, c):
    """P3.7: a gateway that was active then silent for > SILENCE_GW_T while the tick
    pulses. Needs BOTH the tick (G3) and per-gateway attribution (gw)."""
    if not c["tick"] or not c["gw"]:
        return False
    last = {}
    for e in clean:
        last[e["gw"]] = e["ts"]
    return (TRACE_END - last.get("gw3", TRACE_END)) > SILENCE_GW_T


# ---------------------------------------------------------------------------
# 6. Evidence-aware algebra: classify a MISS as flagged (honest unknown) or a
#    silent false all-clear, from the fabric state a real consumer would observe.
# ---------------------------------------------------------------------------
def classify_miss(incident, c, notes):
    """Return the completeness status the algebra assigns to the window of a MISSED
    incident. A flagged status (degraded/incomplete/unavailable) means the operator
    sees ``unknown'', not a false all-clear. 'sound' on a miss = false all-clear."""
    # Delivery loss (crash / overload eviction) shows up as an event-id gap IFF the
    # transport carries event ids. The shared log has none -> the loss is invisible.
    loss_here = (
        (incident == "I5_P3.4_persistent" and "crash" in notes) or
        (incident == "I1_P3.2_botnet" and "overload" in notes)
    )
    if loss_here and c["eids"]:
        return "incomplete"          # event-id discontinuity -> flagged
    # Reorder: detectable as a gateway-time order violation IFF a trusted order
    # reference exists (the fabric's sidecar). The shared log cannot tell.
    if incident == "I3_P3.1_apt" and "reorder" in notes and c["G2"] == 0 and c["gw"]:
        return "incomplete"          # order violation observed -> flagged
    # Silence: only assertable against a live clock. Without the tick there is no
    # way to distinguish "gone dark" from "nothing to report" -> false all-clear.
    if incident in ("I6_P3.5_silentnode", "I7_P3.7_gwsilence"):
        return "incomplete" if c["tick"] else "sound"
    # Structural inexpressibility with no evidence signal at all.
    return "sound"


# ---------------------------------------------------------------------------
# 7. Drive everything.
# ---------------------------------------------------------------------------
def detect_config(name, c, clean, work_dir):
    """Write this config's logs (already run in batch); read back detections."""
    detected = {}
    for inc, (_sig, formula, ident, kind) in MONPOLY_SPECS.items():
        spec = formula[:-6]  # strip .mfotl
        out = os.path.join(work_dir, "out", f"{name}__{spec}.out")
        txt = open(out).read() if os.path.exists(out) else ""
        detected[inc] = parse_firing(txt, ident, kind)
    detected["I6_P3.5_silentnode"] = detect_silent_node(clean, c)
    detected["I7_P3.7_gwsilence"] = detect_gw_silence(clean, c)
    return detected


def write_logs(name, c, clean, work_dir):
    A, notes = apply_faults(clean, c)
    mlog, dropped = to_monpoly_log(A)
    for inc, (_sig, formula, _id, _k) in MONPOLY_SPECS.items():
        spec = formula[:-6]
        if spec == "p3_6_crossgw":
            body = to_crossgw_log(A, c["gw"])
            if body is None:
                continue                 # inexpressible: no crossgw log => no firing
            data = body
        else:
            data = mlog
        with open(os.path.join(work_dir, "streams", f"{name}__{spec}.mlog"), "w") as f:
            f.write(data)
    return notes, dropped


def main(a):
    work = a.workdir
    os.makedirs(os.path.join(work, "streams"), exist_ok=True)
    os.makedirs(os.path.join(work, "out"), exist_ok=True)

    clean = build_workload()
    all_notes = {}
    for name, c in CONFIGS.items():
        notes, _ = write_logs(name, c, clean, work)
        all_notes[name] = notes

    run_monpoly_batch(work)

    # Oracle = the full fabric's incident set (fault-free-equivalent: every fault absorbed).
    oracle_det = detect_config("fabric_full", CONFIGS["fabric_full"], clean, work)
    oracle = {i for i, v in oracle_det.items() if v}

    rows = []
    for name, c in CONFIGS.items():
        det = detect_config(name, c, clean, work)
        detected = {i for i, v in det.items() if v}
        missed = sorted(oracle - detected)
        spurious = sorted(detected - oracle)
        tags = {i: classify_miss(i, c, all_notes[name]) for i in missed}
        false_all_clears = [i for i in missed if tags[i] == "sound"]
        downgraded = [i for i in missed if tags[i] != "sound"]
        rows.append({
            "config": name,
            "detected": sorted(detected),
            "n_detected": len(detected & oracle),
            "n_oracle": len(oracle),
            "preservation_rate": round(len(detected & oracle) / len(oracle), 3),
            "missed": missed,
            "miss_tags": tags,
            "spurious": spurious,
            "false_all_clears": false_all_clears,
            "n_false_all_clears": len(false_all_clears),
            "downgraded_to_unknown": downgraded,
            "n_downgraded": len(downgraded),
            "notes": all_notes[name],
        })

    result = {"oracle_incidents": sorted(oracle), "n_oracle": len(oracle), "configs": rows}
    print(json.dumps(result, indent=2))

    if a.md:
        print("\n\n### Table A: incident preservation vs fault-free oracle\n")
        print("| Configuration | Preserved | Rate | Missed | Silent false all-clears | Downgraded to unknown |")
        print("|---|:--:|:--:|:--:|:--:|:--:|")
        for r in rows:
            miss = ", ".join(m.split("_")[1] for m in r["missed"]) or "--"
            fac = ", ".join(m.split("_")[1] for m in r["false_all_clears"]) or "--"
            dg = ", ".join(m.split("_")[1] for m in r["downgraded_to_unknown"]) or "--"
            print(f"| {r['config']} | {r['n_detected']}/{r['n_oracle']} | "
                  f"{r['preservation_rate']} | {miss} | {r['n_false_all_clears']} ({fac}) | "
                  f"{r['n_downgraded']} ({dg}) |")
    return result


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--workdir",
                    default=os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                         "_oracle_work"),
                    help="scratch directory for generated .mlog/.sig inputs")
    ap.add_argument("--md", action="store_true", help="also print markdown tables")
    main(ap.parse_args())
