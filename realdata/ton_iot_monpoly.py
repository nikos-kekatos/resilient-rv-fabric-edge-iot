#!/usr/bin/env python3
"""Feed real TON_IoT port-scan detections to the REAL MonPoly engine (L3).

Bridges the flow monitor (L1/L2) to the formal L3: for every device the port_scan
monitor flags (>=FANOUT distinct failed destinations), emit an
``overflow(device, ts)`` event at each failed-scan timestamp (subsampled to one per
5s bucket per device to bound log size while preserving temporal overlap), then run
the unmodified MonPoly p3_2_botnet formula (coordinated attack: >=3 distinct devices
within 30s). A parallel ``overflow_gw(gateway, ts)`` log drives the cross-gateway
p3_6 formula (>=2 distinct gateways). MonPoly output = the formal P3.2/P3.6 firings
on real data. Writes the two logs; the caller runs monpoly in the rvhier image.
"""
import argparse, csv, os
import hashlib
from collections import defaultdict, Counter

FAILED = {"S0", "REJ", "RSTO", "RSTR", "RSTOS0", "SH", "S1"}
FANOUT = 20
HERE = os.path.dirname(os.path.abspath(__file__))



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

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", required=True)
    ap.add_argument("--gateways", type=int, default=2)
    ap.add_argument("--gw-seed", type=int, default=0,
                    help="vary the deterministic gateway partition")
    ap.add_argument("--bucket", type=int, default=5, help="subsample: one event per device per N s")
    a = ap.parse_args()

    failed_ev = defaultdict(list)        # device -> [(ts,dst)] failed conns
    scan_ts = defaultdict(list)          # device -> [ts of failed conns]
    with open(a.csv, newline="", encoding="utf-8-sig") as f:
        r = csv.DictReader(f)
        for row in r:
            if (row.get("conn_state") or "").strip().upper() in FAILED:
                d = row["src_ip"].strip()
                try:
                    t = int(float(row["ts"]))
                except (ValueError, KeyError):
                    continue
                failed_ev[d].append((t, row["dst_ip"].strip())); scan_ts[d].append(t)

    def scan_window(ev, W, need):        # >=need distinct dsts within any W-s window
        if len(ev) < need:
            return False
        ev = sorted(ev); c = Counter(); left = 0
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

    scanners = {d for d in failed_ev if scan_window(failed_ev[d], 60, FANOUT)}
    gw = {d: _stable_gw(d, a.gateways, a.gw_seed) for d in scanners}

    # collect (ts, device) subsampled per bucket, rebased to start at 0
    events = []
    for d in scanners:
        seen = set()
        for t in scan_ts[d]:
            b = t // a.bucket
            if b not in seen:
                seen.add(b); events.append((t, d))
    if not events:
        print("no scanner events"); return
    t0 = min(t for t, _ in events)
    events = sorted((t - t0, d) for t, d in events)

    log_dev = os.path.join(HERE, "ton_p32.log")
    log_gw = os.path.join(HERE, "ton_p36.log")
    with open(log_dev, "w") as fd, open(log_gw, "w") as fg:
        for t, d in events:
            fd.write(f'@{t} overflow("{d}",{t})\n')
            fg.write(f'@{t} overflow_gw("{gw[d]}",{t})\n')
    print(f"scanner devices: {len(scanners)}  gateways: {sorted(set(gw.values()))}")
    print(f"P3.2 log: {log_dev} ({len(events)} events)")
    print(f"P3.6 log: {log_gw}")


if __name__ == "__main__":
    main()
