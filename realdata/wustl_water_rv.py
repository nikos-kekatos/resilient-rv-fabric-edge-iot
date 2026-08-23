#!/usr/bin/env python3
"""Flow-based RV on WUSTL-IIoT-2021 — REAL water-supply SCADA (Modbus/port 502).

Makes the CRITIS water case study a real pilot: runs the same flow monitors
(port_scan / ddos_flood / c2_beacon) and the real MonPoly L3 (P3.2/P3.6) on an
openly-published water-SCADA Argus flow dataset. Argus has no Zeek conn_state, so
the failed-probe signal is DstPkts==0 (SYN/probe with no reply). device = SrcAddr;
ground truth = Target==1 / Traffic label (DoS, Reconn, CommInj, Backdoor).

Usage: python3 wustl_water_rv.py --csv wustl/wustl_iiot_2021.csv --gateways 2 [--max 0]
Emits <prefix>_p32.log / <prefix>_p36.log for the real monpoly engine.
"""
import argparse, csv, json, os, sys
import hashlib
from datetime import datetime
from collections import defaultdict, Counter

FANOUT, FLOOD, FLOOD_W, BEACON, BEACON_CV, W_SCAN = 20, 100, 10, 50, 0.5, 60



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
    """>=need distinct dsts within any W-s window over (ts,dst) failed events."""
    if len(events) < need:
        return False
    ev = sorted(events); from collections import Counter as _C
    c = _C(); left = 0
    for right in range(len(ev)):
        c[ev[right][1]] += 1
        while ev[right][0] - ev[left][0] > W:
            c[ev[left][1]] -= 1
            if c[ev[left][1]] == 0:
                del c[ev[left][1]]
            left += 1
        if len(c) >= need:
            return True
    return False
STRUCT = {"dos", "ddos", "reconn", "recon", "backdoor"}   # classes the monitors target
HERE = os.path.dirname(os.path.abspath(__file__))


def col(headers, *cands):
    low = {h.strip().lower(): h for h in headers}
    for c in cands:
        if c in low:
            return low[c]
    return None


def parse_ts(s, fallback):
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S", "%m/%d/%Y %H:%M:%S"):
        try:
            return int(datetime.strptime(s.strip(), fmt).timestamp())
        except (ValueError, AttributeError):
            pass
    try:
        return int(float(s))
    except (ValueError, TypeError):
        return fallback


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", required=True)
    ap.add_argument("--gateways", type=int, default=2)
    ap.add_argument("--gw-seed", type=int, default=0,
                    help="vary the deterministic gateway partition")
    ap.add_argument("--max", type=int, default=0)
    ap.add_argument("--prefix", default="wustl_water")
    a = ap.parse_args()

    dev_gt = defaultdict(Counter); dev_type = defaultdict(Counter); dev_gw = {}
    failed_dsts = defaultdict(set); failed_dst_count = defaultdict(Counter)
    failed_events = defaultdict(list)
    dst_times = defaultdict(lambda: defaultdict(list)); scan_ts = defaultdict(list)

    def gw_of(ip): return _stable_gw(ip, a.gateways, a.gw_seed)

    with open(a.csv, newline="", encoding="utf-8-sig", errors="replace") as f:
        r = csv.DictReader(f)
        C = {k: col(r.fieldnames, *v) for k, v in {
            "src": ("srcaddr", "src_ip", "saddr"), "dst": ("dstaddr", "dst_ip", "daddr"),
            "ts": ("starttime", "stime", "ts"), "dpkts": ("dstpkts", "dpkts", "dst_pkts"),
            "label": ("target", "label"), "type": ("traffic", "type", "attack_cat"),
        }.items()}
        miss = [k for k in ("src", "dst", "label") if not C[k]]
        if miss:
            sys.exit(f"missing columns {miss}; headers={r.fieldnames}")
        print("columns:", C)
        n = 0
        for row in r:
            src = row[C["src"]].strip(); dst = row[C["dst"]].strip()
            lab = str(row[C["label"]]).strip()
            typ = (row[C["type"]].strip().lower() if C["type"] else "")
            mal = lab in ("1", "attack", "malicious") or typ not in ("normal", "0", "", "benign")
            ts = parse_ts(row[C["ts"]], n) if C["ts"] else n
            try:
                dpkts = int(float(row[C["dpkts"]])) if C["dpkts"] and row[C["dpkts"]] not in ("", "-") else 1
            except ValueError:
                dpkts = 1
            dev_gw[src] = gw_of(src); dev_gt[src]["M" if mal else "B"] += 1
            if mal:
                dev_type[src][typ] += 1
            if dpkts == 0:                      # failed probe = Argus analogue of Zeek S0
                failed_dsts[src].add(dst); failed_dst_count[src][dst] += 1; scan_ts[src].append(ts)
                failed_events[src].append((ts, dst))
            dst_times[src][dst].append(ts)
            n += 1
            if a.max and n >= a.max:
                break
    print(f"parsed {n} flows")

    alerts = defaultdict(set)
    for d in dev_gw:
        fanout = len(failed_dsts[d]); mr = max(failed_dst_count[d].values()) if failed_dst_count[d] else 0
        flood = False
        for dd, tt in dst_times[d].items():
            ts = sorted(tt); j = 0
            for k in range(len(ts)):
                while ts[k] - ts[j] > FLOOD_W:
                    j += 1
                if k - j + 1 >= FLOOD:
                    flood = True; break
            if flood:
                break
        if scan_window(failed_events[d], W_SCAN, FANOUT):
            alerts[d].add("port_scan")
        if flood:
            alerts[d].add("ddos_flood")
        if mr >= BEACON and fanout < FANOUT and failed_dst_count[d]:
            top = max(failed_dst_count[d], key=failed_dst_count[d].get); tt = sorted(dst_times[d][top])
            if len(tt) >= 3:
                ia = [tt[i + 1] - tt[i] for i in range(len(tt) - 1)]; m = sum(ia) / len(ia)
                if m > 0 and (sum((x - m) ** 2 for x in ia) / len(ia)) ** 0.5 / m < BEACON_CV:
                    alerts[d].add("c2_beacon")

    def is_mal(d): return dev_gt[d]["M"] > 0
    attackers = {d for d in dev_gw if is_mal(d)}
    struct = {d for d in dev_gw if any(t in STRUCT for t in dev_type[d])}
    flagged = {d for d in alerts if alerts[d]}
    TP = len(flagged & attackers); FP = len(flagged - attackers); FN = len(attackers - flagged)
    prec = TP / len(flagged) if flagged else 0
    rec = TP / len(attackers) if attackers else 0
    rec_s = len(flagged & struct) / len(struct) if struct else 0

    # emit MonPoly logs: attack-source events (DoS/Reconn) -> overflow / overflow_gw
    ev = []
    # Feed L3 from what the monitors flagged, NOT from ground truth: intersecting
    # with `attackers` here would leak labels into the correlation stream, which a
    # deployed fabric does not have, and would make this dataset run a different
    # pipeline from TON_IoT (ton_iot_monpoly.py feeds `scanners` unfiltered).
    for d in flagged:
        seen = set()
        for t in scan_ts[d] or [tt for dd in dst_times[d] for tt in dst_times[d][dd]]:
            b = t // 5
            if b not in seen:
                seen.add(b); ev.append((t, d))
    p32 = p36 = None
    if ev:
        t0 = min(t for t, _ in ev); ev = sorted((t - t0, d) for t, d in ev)
        p32 = os.path.join(HERE, f"{a.prefix}_p32.log"); p36 = os.path.join(HERE, f"{a.prefix}_p36.log")
        with open(p32, "w") as fd, open(p36, "w") as fg:
            for t, d in ev:
                fd.write(f'@{t} overflow("{d}",{t})\n'); fg.write(f'@{t} overflow_gw("{dev_gw[d]}",{t})\n')

    report = {"prefix": a.prefix, "flows": n, "unique_source_identities": len(dev_gw),
              "attacker_identities": len(attackers), "structural_attackers": len(struct),
              "attack_types": dict(sum((c for c in dev_type.values()), Counter()).most_common()),
              "confusion": {"TP": TP, "FP": FP, "TN": len(dev_gw) - len(attackers) - FP, "FN": FN},
              "precision": round(prec, 3), "recall_all": round(rec, 3), "recall_struct": round(rec_s, 3),
              "alerts_by_type": {t: sum(t in v for v in alerts.values()) for t in ("port_scan", "ddos_flood", "c2_beacon")},
              "flagged_attackers": sorted(flagged & attackers)[:12],
              "gateways_of_flagged": sorted({dev_gw[d] for d in flagged & attackers}),
              "monpoly_logs": {"p32": p32, "p36": p36, "events": len(ev)}}
    with open(os.path.join(HERE, f"{a.prefix}_report.json"), "w") as f:
        json.dump(report, f, indent=2)
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
