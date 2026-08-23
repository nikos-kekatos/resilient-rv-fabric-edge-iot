#!/usr/bin/env python3
"""Flow-based RV case study on TON_IoT (UNSW Cyber Range) — real multi-device.

TON_IoT's Train_Test_Network.csv is Zeek-derived and KEEPS src_ip/dst_ip/conn_state
/ts + a `label` (0/1) and `type` (normal, scanning, ddos, dos, backdoor, injection,
password, xss, mitm, ransomware), so per-device identity survives and the flow
monitors (port_scan / ddos_flood / c2_beacon) + fleet correlation (P3.2/P3.6) run
directly — same monitor code as the IoT-23 study, only the front-end differs.

Usage: python3 ton_iot_rv.py --csv ton_iot/Train_Test_Network.csv --gateways 2 [--max 0]
"""
import argparse, csv as csvmod, json, os, re
import hashlib
from collections import defaultdict, Counter

HERE = os.path.dirname(os.path.abspath(__file__))
FAILED = {"S0", "REJ", "RSTO", "RSTR", "RSTOS0", "SH", "S1"}
FANOUT, FLOOD, FLOOD_W, BEACON = 20, 100, 10, 50
W_SCAN = 60   # port_scan sliding window (s): >=FANOUT distinct failed dsts within W_SCAN



def _stable_gw(key, gateways, seed=0):
    """Deterministic gateway assignment.

    Python's builtin hash() is salted per process (PYTHONHASHSEED), so using it
    here made the gateway partition -- and therefore every cross-gateway (P3.6)
    count -- irreproducible across runs and machines. md5 is stable everywhere.
    `seed` exists so the partition can be varied deliberately to check that a
    result is not an artefact of one particular split.
    """
    h = hashlib.md5(f"{seed}:{key}".encode()).hexdigest()
    return f"gw{(int(h, 16) % gateways) + 1}"

def scan_window(events, W, need):
    """True if any W-second window over the (ts,dst) failed events has >=need
    distinct destinations (online sliding-window horizontal-scan detection)."""
    if len(events) < need:
        return False
    ev = sorted(events)
    from collections import Counter as _C
    cnt = _C(); left = 0
    for right in range(len(ev)):
        cnt[ev[right][1]] += 1
        while ev[right][0] - ev[left][0] > W:
            cnt[ev[left][1]] -= 1
            if cnt[ev[left][1]] == 0:
                del cnt[ev[left][1]]
            left += 1
        if len(cnt) >= need:
            return True
    return False
BEACON_CV = 0.5   # max coeff. of variation of inter-arrival -> periodic (C&C), not bursty benign retries

# tolerant column lookup
COL = {"ts": ["ts", "timestamp"], "src": ["src_ip", "srcip", "source ip"],
       "dst": ["dst_ip", "dstip", "destination ip"],
       "state": ["conn_state", "state"], "label": ["label"],
       "type": ["type", "attack", "attack_cat", "category"]}


def pick(headers, keys):
    low = {h.strip().lower(): h for h in headers}
    for k in keys:
        if k in low:
            return low[k]
    return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", required=True)
    ap.add_argument("--gateways", type=int, default=2)
    ap.add_argument("--gw-seed", type=int, default=0,
                    help="vary the deterministic gateway partition")
    ap.add_argument("--max", type=int, default=0, help="0 = all rows")
    ap.add_argument("--prefix", default="ton_iot")
    a = ap.parse_args()

    dev_gw, dev_gt = {}, defaultdict(Counter)
    dev_type = defaultdict(Counter)
    failed_dsts = defaultdict(set)
    dst_count = defaultdict(Counter)
    failed_dst_count = defaultdict(Counter)      # repeats over FAILED conns (C&C signal)
    failed_events = defaultdict(list)            # (ts,dst) failed conns for windowed scan
    dst_times = defaultdict(lambda: defaultdict(list))

    def gw_of(ip):
        return _stable_gw(ip, a.gateways, a.gw_seed)

    with open(a.csv, newline="", encoding="utf-8-sig") as f:  # utf-8-sig strips BOM on 'ts'
        r = csvmod.DictReader(f)
        cols = {k: pick(r.fieldnames, v) for k, v in COL.items()}
        missing = [k for k in ("src", "dst", "state", "label") if not cols[k]]
        if missing:
            raise SystemExit(f"missing columns {missing}; headers={r.fieldnames}")
        print("columns:", cols)
        n = 0
        for row in r:
            src = row[cols["src"]].strip()
            dst = row[cols["dst"]].strip()
            state = (row[cols["state"]] or "").strip().upper()
            try:
                ts = float(row[cols["ts"]]) if cols["ts"] and row[cols["ts"]] not in ("", "-") else n
            except ValueError:
                ts = n
            lab = str(row[cols["label"]]).strip()
            typ = (row[cols["type"]].strip().lower() if cols["type"] else ("attack" if lab in ("1", "attack") else "normal"))
            mal = lab in ("1", "attack", "malicious") or typ not in ("normal", "benign", "0", "")
            dev = src
            dev_gw[dev] = gw_of(dev)
            dev_gt[dev]["Malicious" if mal else "Benign"] += 1
            if mal:
                dev_type[dev][typ] += 1
            if state in FAILED:
                failed_dsts[dev].add(dst)
                failed_dst_count[dev][dst] += 1
                failed_events[dev].append((ts, dst))
            dst_count[dev][dst] += 1
            dst_times[dev][dst].append(ts)
            n += 1
            if a.max and n >= a.max:
                break

    alerts = defaultdict(set)
    for d in dev_gw:
        fanout = len(failed_dsts[d])
        max_repeat = max(failed_dst_count[d].values()) if failed_dst_count[d] else 0
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
        if scan_window(failed_events[d], W_SCAN, FANOUT):
            alerts[d].add("port_scan")
        if flood_hit:
            alerts[d].add("ddos_flood")
        # c2_beacon: many failed repeats to few peers AND regular interval (periodic)
        if max_repeat >= BEACON and fanout < FANOUT and failed_dst_count[d]:
            top = max(failed_dst_count[d], key=failed_dst_count[d].get)
            times = sorted(dst_times[d][top])
            if len(times) >= 3:
                iats = [times[i + 1] - times[i] for i in range(len(times) - 1)]
                mean = sum(iats) / len(iats)
                if mean > 0:
                    cv = (sum((x - mean) ** 2 for x in iats) / len(iats)) ** 0.5 / mean
                    if cv < BEACON_CV:
                        alerts[d].add("c2_beacon")

    # GT: a device is an ATTACKER if it sources any attack flow (TON_IoT attackers
    # are dedicated hosts; benign devices source 0 attack flows).
    def is_mal(d):
        return dev_gt[d]["Malicious"] > 0
    # STRUCTURAL attackers = those these 3 monitors target (scan/flood/C&C);
    # app-layer attacks (injection/xss/password/ransomware/mitm) need content specs.
    STRUCT = {"scanning", "dos", "ddos", "backdoor"}
    struct_attackers = {d for d in dev_gw if any(t in STRUCT for t in dev_type[d])}
    TP = FP = TN = FN = 0
    for d in dev_gw:
        mal, flag = is_mal(d), bool(alerts[d])
        TP += flag and mal; FP += flag and not mal
        FN += (not flag) and mal; TN += (not flag) and not mal
    prec = TP / (TP + FP) if TP + FP else 0
    rec = TP / (TP + FN) if TP + FN else 0
    f1 = 2 * prec * rec / (prec + rec) if prec + rec else 0
    # recall restricted to the attack classes these monitors actually target
    flagged = {d for d in alerts if alerts[d]}
    rec_struct = len(flagged & struct_attackers) / len(struct_attackers) if struct_attackers else 0

    scan_devs = [d for d in alerts if "port_scan" in alerts[d]]
    scan_gws = {dev_gw[d] for d in scan_devs}
    type_devs = defaultdict(list)
    for d, ts in alerts.items():
        for t in ts:
            type_devs[t].append(d)
    # per-monitor precision (fraction of flagged devices that are real attackers)
    per_monitor = {}
    for t, ds in type_devs.items():
        hits = sum(is_mal(d) for d in ds)
        per_monitor[t] = {"flagged": len(ds), "attackers": hits,
                          "precision": round(hits / len(ds), 3) if ds else 0}

    report = {
        "prefix": a.prefix, "thresholds": {"FANOUT": FANOUT, "FLOOD": FLOOD, "BEACON": BEACON},
        "flows": sum(sum(c.values()) for c in dev_gt.values()),
        "unique_devices": len(dev_gw), "devices_per_gateway": dict(Counter(dev_gw.values())),
        "attacker_devices_gt": sum(is_mal(d) for d in dev_gw),
        "structural_attackers_gt (scan/dos/ddos/backdoor)": len(struct_attackers),
        "attack_types_seen": dict(sum((c for c in dev_type.values()), Counter()).most_common()),
        "confusion_device_level": {"TP": TP, "FP": FP, "TN": TN, "FN": FN},
        "precision": round(prec, 3), "recall_all_attackers": round(rec, 3),
        "recall_structural (scan/flood/C&C attackers)": round(rec_struct, 3),
        "f1": round(f1, 3),
        "alerts_by_type": {t: len(v) for t, v in type_devs.items()},
        "per_monitor_precision": per_monitor,
        "P3.2_coordinated_scan": {"devices": len(scan_devs), "fires": len(scan_devs) >= 3},
        "P3.6_cross_gateway_scan": {"gateways": sorted(scan_gws), "fires": len(scan_gws) >= 2},
    }
    with open(os.path.join(HERE, f"{a.prefix}_report.json"), "w") as f:
        json.dump(report, f, indent=2)
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
