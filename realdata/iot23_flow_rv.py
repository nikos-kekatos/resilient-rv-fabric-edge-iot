#!/usr/bin/env python3
"""Flow-based RV monitors for real IoT malware (IoT-23 multi-device case study).

The iot_app-style predicates (payload overflow / zero-byte fuzz / time-spoof) do
not match network-flow malware, whose signal is *connection structure*. This adds
three flow-appropriate monitors, defined over Zeek conn records
r=(ts, src, dst, state) where a connection is FAILED if it never established
(state in S0/REJ/RSTO/RSTR/RSTOS0/SH/S1):

  L2 per-device, sliding window W:
    port_scan(d)   : |{ dst : failed(d,dst) in W }|            >= FANOUT
    ddos_flood(d)  : max_dst |{ conn(d,dst) in W_burst }|      >= FLOOD
    c2_beacon(d)   : max_dst repeat(d,dst)>=BEACON  AND  fanout(d) < FANOUT
                     (many repeats to few peers = command-and-control, not a scan)

  L3 fleet correlation (formalised as MFOTL, run over the per-device alerts):
    P3.2' coordinated_scan : >=3 devices raise port_scan            (botnet sweep)
    P3.6' cross_gw_scan    : port_scan on >=2 distinct gateways

Formal-spec form (as carried at L3, MonPoly MFOTL syntax):
    coordinated_scan  :=  (cnt <- CNT d; ONCE[0,W] port_scan(d)) AND cnt >= 3
    cross_gw_scan     :=  (cnt <- CNT g; ONCE[0,W] port_scan_gw(g)) AND cnt >= 2

Detection is scored per DEVICE (each capture's source IPs are prefixed with the
capture tag so captures count as distinct devices) against IoT-23 ground truth
(device is malicious if the majority of its connections are labelled Malicious).
"""
import argparse, glob, json, os, re, statistics
from collections import defaultdict, Counter

HERE = os.path.dirname(os.path.abspath(__file__))
FAILED = {"S0", "REJ", "RSTO", "RSTR", "RSTOS0", "SH", "S1"}
FANOUT = 20          # distinct failed destinations -> horizontal scan
FLOOD = 100          # connections to one dst within a burst window -> flood
FLOOD_W = 10         # seconds
BEACON = 50          # repeated connections to one dst -> C&C


def parse(path, tag, max_events):
    fields = None; idx = {}
    n = 0
    with open(path, errors="replace") as f:
        for line in f:
            if line.startswith("#fields"):
                fields = line.rstrip("\n").split("\t")[1:]; idx = {k: i for i, k in enumerate(fields)}; continue
            if line.startswith("#") or not line.strip() or fields is None:
                continue
            p = line.rstrip("\n").split("\t")
            try:
                ts = float(p[idx["ts"]]); src = p[idx["id.orig_h"]]
                dst = p[idx["id.resp_h"]]; state = p[idx["conn_state"]]
            except Exception:
                continue
            gt = "Malicious" if "Malicious" in line else ("Benign" if "Benign" in line else None)
            if gt is None:
                continue
            yield ts, f"{tag}:{src}", dst, state, gt
            n += 1
            if n >= max_events:
                break


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--caps", nargs="+", required=True)
    ap.add_argument("--max", type=int, default=20000)
    ap.add_argument("--gateways", type=int, default=2)
    ap.add_argument("--prefix", default="iot23_flow")
    a = ap.parse_args()

    files = []
    for c in a.caps:
        files.extend(sorted(glob.glob(c)))

    dev_gw = {}
    dev_gt = defaultdict(Counter)
    failed_dsts = defaultdict(set)               # device -> distinct failed dsts
    failed_dst_count = defaultdict(Counter)      # device -> failed dst -> count (C&C signal)
    dst_count = defaultdict(Counter)             # device -> dst -> count
    dst_times = defaultdict(lambda: defaultdict(list))  # device -> dst -> [ts]
    scan_ts = defaultdict(list)                  # device -> [ts of failed conns] (for MonPoly)
    for i, path in enumerate(files):
        tag = re.sub(r"\.conn\.log\.labeled$", "", os.path.basename(path))
        gw = f"gw{i % a.gateways + 1}"
        cnt = 0
        for ts, dev, dst, state, gt in parse(path, tag, a.max):
            dev_gw[dev] = gw; dev_gt[dev][gt] += 1
            if state in FAILED:
                failed_dsts[dev].add(dst); failed_dst_count[dev][dst] += 1; scan_ts[dev].append(ts)
            dst_count[dev][dst] += 1
            dst_times[dev][dst].append(ts)
            cnt += 1
        print(f"[{tag}] -> {gw}: {cnt} conns")

    # L2 alerts per device
    alerts = defaultdict(set)                     # device -> {alert types}
    for d in dev_gw:
        fanout = len(failed_dsts[d])
        max_repeat = max(failed_dst_count[d].values()) if failed_dst_count[d] else 0
        # flood: max connections to a single dst within any FLOOD_W-second window
        flood_hit = False
        for dst, times in dst_times[d].items():
            ts = sorted(times); j = 0
            for k in range(len(ts)):
                while ts[k] - ts[j] > FLOOD_W:
                    j += 1
                if k - j + 1 >= FLOOD:
                    flood_hit = True; break
            if flood_hit:
                break
        if fanout >= FANOUT:
            alerts[d].add("port_scan")
        if flood_hit:
            alerts[d].add("ddos_flood")
        # c2_beacon: many failed repeats to few peers AND regular interval (periodicity)
        if max_repeat >= BEACON and fanout < FANOUT and failed_dst_count[d]:
            top = max(failed_dst_count[d], key=failed_dst_count[d].get)
            tt = sorted(dst_times[d][top])
            if len(tt) >= 3:
                ia = [tt[i + 1] - tt[i] for i in range(len(tt) - 1)]; m = sum(ia) / len(ia)
                if m > 0 and (sum((x - m) ** 2 for x in ia) / len(ia)) ** 0.5 / m < 0.5:
                    alerts[d].add("c2_beacon")

    # device-level detection vs ground truth (majority label)
    def is_mal(d):
        c = dev_gt[d]; return c["Malicious"] >= c["Benign"]
    TP = FP = TN = FN = 0
    for d in dev_gw:
        mal, flag = is_mal(d), bool(alerts[d])
        TP += flag and mal; FP += flag and not mal
        FN += (not flag) and mal; TN += (not flag) and not mal
    prec = TP / (TP + FP) if TP + FP else 0
    rec = TP / (TP + FN) if TP + FN else 0
    f1 = 2 * prec * rec / (prec + rec) if prec + rec else 0

    # L3 coordination
    scan_devs = [d for d in alerts if "port_scan" in alerts[d]]
    scan_gws = {dev_gw[d] for d in scan_devs}

    # emit MonPoly logs for the REAL engine (P3.2 overflow / P3.6 overflow_gw),
    # one event per scanner per 5s bucket, rebased to start at 0
    ev = []
    for d in scan_devs:
        seen = set()
        for t in scan_ts[d]:
            b = t // 5
            if b not in seen:
                seen.add(b); ev.append((t, d))
    if ev:
        t0 = min(t for t, _ in ev); ev = sorted((t - t0, d) for t, d in ev)
        with open(os.path.join(HERE, f"{a.prefix}_p32.log"), "w") as fd, \
             open(os.path.join(HERE, f"{a.prefix}_p36.log"), "w") as fg:
            for t, d in ev:
                fd.write(f'@{t} overflow("{d}",{t})\n')
                fg.write(f'@{t} overflow_gw("{dev_gw[d]}",{t})\n')
    type_devs = defaultdict(list)
    for d, ts in alerts.items():
        for t in ts:
            type_devs[t].append(d)

    report = {
        "prefix": a.prefix, "captures": len(files),
        "thresholds": {"FANOUT": FANOUT, "FLOOD": FLOOD, "FLOOD_W": FLOOD_W, "BEACON": BEACON},
        "connections": sum(sum(c.values()) for c in dev_gt.values()),
        "unique_devices": len(dev_gw), "devices_per_gateway": dict(Counter(dev_gw.values())),
        "malicious_devices_gt": sum(is_mal(d) for d in dev_gw),
        "confusion_device_level": {"TP": TP, "FP": FP, "TN": TN, "FN": FN},
        "precision": round(prec, 3), "recall": round(rec, 3), "f1": round(f1, 3),
        "alerts_by_type": {t: len(v) for t, v in type_devs.items()},
        "flagged_devices": {d: sorted(alerts[d]) for d in sorted(alerts) if alerts[d]},
        "P3.2_coordinated_scan": {"devices": len(scan_devs), "fires": len(scan_devs) >= 3},
        "P3.6_cross_gateway_scan": {"gateways": sorted(scan_gws), "fires": len(scan_gws) >= 2},
    }
    with open(os.path.join(HERE, f"{a.prefix}_report.json"), "w") as f:
        json.dump(report, f, indent=2)
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
