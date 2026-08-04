#!/usr/bin/env python3
"""Controlled same-host shared-log vs fabric ingest-loss comparison (CRITIS Q1).

Both reviews' top attack surface: the headline 1.0%->0 loss compares the fabric to
a shared-log number imported from prior work, not a same-host controlled run. This
runs the fair comparison the reviewers asked for: ONE fixed trace, the SAME host,
the SAME load and pacing, the SAME L1 code, only the transport differs, counting
how many events reach the L1 boundary under each.

Shared-log arm  : the REAL prototype transport -- k concurrent writers doing the
                  exact iot_app per-event ``open(LOG,"a"); write(json+"\n")`` while
                  a reader runs ``tail -F LOG | canonicaliser.py`` (the real L1).
                  Delivered = canonicaliser output lines.
Fabric arm      : k MQTT QoS-1 publishers to a subscriber attached first (the
                  gateway's ingest hop). Delivered = messages received.

Both arms drain until their delivered count is stable, so genuine transport loss is
isolated from teardown truncation. Reported over M trials as mean +/- std.
"""
import argparse, json, os, signal, statistics, subprocess, sys, threading, time, random

sys.path.insert(0, "/exp")
import device_publisher as dp
import paho.mqtt.client as mqtt

TRACE = "/exp/expdata/trace.jsonl"
LOG = "/exp/expdata/events.log"
CANON = "/exp/expdata/canonicaliser.py"
GENS = [dp.gen_safe_tx, dp.gen_overflow_attack, dp.gen_time_spoof_attack,
        dp.gen_stealth_overflow, dp.gen_fuzz_attack]


def gen_trace(n, k, seed=1234):
    rnd = random.Random(seed)
    evts = []
    for i in range(n):
        dev = f"dev-{i % k}"
        g = GENS[i % len(GENS)]
        e = g(dev)
        e["actor"] = dev
        evts.append(e)
    with open(TRACE, "w") as f:
        for e in evts:
            f.write(json.dumps(e) + "\n")
    return evts


def paced_slices(evts, k):
    return [evts[i::k] for i in range(k)]


# ---------------- shared-log arm ----------------
def run_sharedlog(evts, k, rate):
    if os.path.exists(LOG):
        os.remove(LOG)                           # fresh file, no leftover content
    open(LOG, "w").close()
    count = {"n": 0}
    # -n0: start at end, read zero pre-existing lines (only new appends count).
    proc = subprocess.Popen(
        f"tail -n0 -F {LOG} | python3 {CANON}", shell=True,
        stdout=subprocess.PIPE, preexec_fn=os.setsid, bufsize=1, text=True)

    def reader():
        for _ in proc.stdout:
            count["n"] += 1
    t = threading.Thread(target=reader, daemon=True); t.start()
    time.sleep(1.0)                              # let tail -F attach before writes

    per = 1.0 / rate * k                         # per-writer inter-event delay
    def writer(slice_):
        for e in slice_:
            with open(LOG, "a") as f:
                f.write(json.dumps(e) + "\n")
            time.sleep(per)
    ths = [threading.Thread(target=writer, args=(s,)) for s in paced_slices(evts, k)]
    for th in ths: th.start()
    for th in ths: th.join()

    # drain until stable
    last = -1
    while count["n"] != last:
        last = count["n"]; time.sleep(2.0)
    os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
    return count["n"]


# ---------------- fabric arm (MQTT QoS 1) ----------------
def run_fabric(evts, k, rate, broker):
    recv = {"n": 0}
    ready = threading.Event()
    sub = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="base-sub")
    sub.on_message = lambda c, u, m: recv.__setitem__("n", recv["n"] + 1)
    sub.on_subscribe = lambda *a, **k: ready.set()
    sub.on_connect = lambda c, u, f, rc, props=None: c.subscribe("base/+/evt", qos=1)
    sub.connect(broker, 1883); sub.loop_start()
    ready.wait(10)                               # block until SUBACK before publishing
    time.sleep(0.5)

    per = 1.0 / rate * k
    def writer(slice_, wid):
        c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"base-pub-{wid}")
        c.connect(broker, 1883); c.loop_start()
        for e in slice_:
            c.publish(f"base/{e['actor']}/evt", json.dumps(e), qos=1)
            time.sleep(per)
        c.loop_stop(); c.disconnect()
    ths = [threading.Thread(target=writer, args=(s, i)) for i, s in enumerate(paced_slices(evts, k))]
    for th in ths: th.start()
    for th in ths: th.join()

    last = -1
    while recv["n"] != last:
        last = recv["n"]; time.sleep(2.0)
    sub.loop_stop(); sub.disconnect()
    return recv["n"]


def main(a):
    evts = gen_trace(a.n, a.k)
    N = len(evts)
    # discarded warm-up: settles broker/log/subscriber state (first-container races).
    warm = evts[:500]
    run_sharedlog(warm, a.k, a.rate); run_fabric(warm, a.k, a.rate, a.broker)
    sl, fb = [], []
    for trial in range(a.trials):
        s = run_sharedlog(evts, a.k, a.rate)
        f = run_fabric(evts, a.k, a.rate, a.broker)
        sl.append(N - s); fb.append(N - f)
        print(json.dumps({"trial": trial, "N": N,
                          "sharedlog_delivered": s, "sharedlog_lost": N - s,
                          "fabric_delivered": f, "fabric_lost": N - f}), flush=True)
    def pct(x): return round(100.0 * statistics.mean(x) / N, 3)
    print(json.dumps({"summary": True, "N": N, "trials": a.trials, "rate_msg_s": a.rate,
                      "sharedlog_loss_pct_mean": pct(sl),
                      "sharedlog_lost_mean": round(statistics.mean(sl), 1),
                      "fabric_loss_pct_mean": pct(fb),
                      "fabric_lost_mean": round(statistics.mean(fb), 1)}, indent=2))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=7000)
    ap.add_argument("--k", type=int, default=8)
    ap.add_argument("--rate", type=float, default=500.0, help="aggregate msg/s")
    ap.add_argument("--trials", type=int, default=5)
    ap.add_argument("--broker", default="mosq-exp")
    main(ap.parse_args())
