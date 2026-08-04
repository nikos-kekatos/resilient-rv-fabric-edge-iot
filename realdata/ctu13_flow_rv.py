#!/usr/bin/env python3
"""Flow-based RV on CTU-13 (Stratosphere) — GENUINE coordinated multi-bot botnets.

CTU-13 is labeled Argus bidirectional NetFlow (.binetflow). Unlike TON_IoT's
attacker-vs-testbed setup, several scenarios run real botnets with MANY coordinated
bots (Neris ~10, Rbot several), so P3.2/P3.6 are exercised by true coordination.
Argus has no Zeek conn_state, so port_scan keys on destination fan-out (a bot
sweeping many hosts). device = SrcAddr; ground truth = a flow labelled 'Botnet'.

Usage: python3 ctu13_flow_rv.py --caps 'ctu13/CTU-13-Dataset/*/*.binetflow' --max 200000 --gateways 2
"""
import argparse, csv, glob, json, os, re
from collections import defaultdict, Counter

FANOUT, FLOOD, FLOOD_W, BEACON = 40, 200, 10, 80


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--caps", nargs="+", required=True)
    ap.add_argument("--max", type=int, default=200000)
    ap.add_argument("--gateways", type=int, default=2)
    a = ap.parse_args()
    files = []
    for c in a.caps:
        files.extend(sorted(glob.glob(c)))

    dev_gw, dev_gt = {}, defaultdict(Counter)
    dsts = defaultdict(set); dst_count = defaultdict(Counter); dst_times = defaultdict(lambda: defaultdict(list))
    for i, path in enumerate(files):
        tag = path.split("/")[-2]                     # scenario number
        gw = f"gw{i % a.gateways + 1}"
        n = 0
        with open(path, newline="") as f:
            r = csv.DictReader(f)
            for row in r:
                src = f"{tag}:{row['SrcAddr']}"; dst = row["DstAddr"]
                mal = "Botnet" in row["Label"]
                dev_gw[src] = gw; dev_gt[src]["M" if mal else "B"] += 1
                dsts[src].add(dst); dst_count[src][dst] += 1
                try:
                    # StartTime "2011/08/18 15:47:57.983212" -> seconds of day is enough for ordering
                    hms = row["StartTime"].split()[1]; h, m, s = hms.split(":")
                    dst_times[src][dst].append(int(float(h) * 3600 + float(m) * 60 + float(s)))
                except Exception:
                    pass
                n += 1
                if n >= a.max:
                    break
        print(f"[{tag}] -> {gw}: {n} flows")

    alerts = defaultdict(set)
    for d in dev_gw:
        fanout = len(dsts[d]); max_repeat = max(dst_count[d].values()) if dst_count[d] else 0
        flood_hit = False
        for dd, times in dst_times[d].items():
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
        if max_repeat >= BEACON and fanout < FANOUT:
            alerts[d].add("c2_beacon")

    def is_mal(d):
        return dev_gt[d]["M"] > 0
    TP = FP = TN = FN = 0
    for d in dev_gw:
        mal, flag = is_mal(d), bool(alerts[d])
        TP += flag and mal; FP += flag and not mal
        FN += (not flag) and mal; TN += (not flag) and not mal
    prec = TP / (TP + FP) if TP + FP else 0
    rec = TP / (TP + FN) if TP + FN else 0
    scan_devs = [d for d in alerts if "port_scan" in alerts[d]]
    scan_gws = {dev_gw[d] for d in scan_devs}
    bot_devs = [d for d in dev_gw if is_mal(d)]
    report = {
        "scenarios": len(files), "flows": sum(sum(c.values()) for c in dev_gt.values()),
        "unique_devices": len(dev_gw), "botnet_devices_gt": len(bot_devs),
        "confusion": {"TP": TP, "FP": FP, "TN": TN, "FN": FN},
        "precision": round(prec, 3), "recall": round(rec, 3),
        "alerts_by_type": {t: sum(t in v for v in alerts.values()) for t in ("port_scan", "ddos_flood", "c2_beacon")},
        "flagged_bots": [d for d in scan_devs if is_mal(d)][:12],
        "P3.2_coordinated_scan": {"devices": len(scan_devs), "fires": len(scan_devs) >= 3},
        "P3.6_cross_gateway_scan": {"gateways": sorted(scan_gws), "fires": len(scan_gws) >= 2},
    }
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
