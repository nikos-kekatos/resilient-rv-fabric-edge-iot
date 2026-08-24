# A Resilient Runtime-Verification Fabric for Security Monitoring of Critical Edge-IoT Infrastructure

**Authors.** Nikolaos Kekatos, Marinelio Chintri, Panagiotis Katsaros, Alexios Lekidis, Tom Nianios,
Ioannis Seitoglou, Anastasios Temperekidis, Stylianos Basagiannis. Accepted at **CRITIS 2026**.

`paper/paper.pdf` is the submitted manuscript (20 pages); `MANIFEST.md` maps every paper element
(figures, the five guarantees G1-G5, and questions Q1-Q8) to the script that produces it.

The fabric treats the transport beneath the monitors as part of the verification problem: its
delivery guarantees are stated as invariants, so loss, skew, silence and overload become observable
to the monitor instead of silently degrading it.

**Datasets are not bundled** (size and licensing). `realdata/DOWNLOAD.md` gives the download steps
for WUSTL-IIoT-2021 and the flow corpora used in the real-data validation; recorded expected output
is in `realdata/expected/` and `fabric/expected/`.

---

# Reproducibility Artifact — CRITIS 2026

**Paper:** *A Resilient Runtime-Verification Fabric for Security Monitoring of Critical
Edge-IoT Infrastructure* (RV-Fabric).

This artifact contains the code and specifications behind the paper. What it reproduces,
and what it does not, in plain terms:

**Reproduces exactly, on any host** — the headline oracle result (Table 3, Q8: shared log
1/7 vs fabric 7/7, and every ablation row), the crash-injection counts (Q6), the
retention/overload behaviour (Q7), the completeness tags, and all of the real-data
validation (§8: identity counts, precision/recall, and the P3.2/P3.6 episode counts). These
run against the real MonPoly and RTLola engines and, where brokers are needed, against real
Mosquitto and NATS JetStream.

**Present as code, but with no recorded output here** — the per-event latency sweep (Q2), the
two-host clock-skew sweep, and the "independent re-execution" figures. Q2 needs Linux `netem`
(`--cap-add NET_ADMIN`); the skew sweep creates and then destroys two cloud VMs, so the
infrastructure does not survive its own run (a single-host `libfaketime` variant,
`clockskew/run_skew_faketime.sh`, needs no cloud).

Two of these have since been recorded, on a *different* host than the paper:
`fabric/expected/q3_scale_fleet.md` (Q3 saturation and fleet sweep, needs only NATS) and
`fabric/expected/q4_fullstack_g2.md` (Q4 detection through the real engines plus the Table 2
G2 order-violation counter). Q4 is unseeded *by design* — `device_publisher.py` seeds from
`os.urandom(16)` — so that file is one sample to compare in shape, not a golden file. The scripts are included; the results in
the paper were measured on hosts and networks this repository cannot recreate, and are
host-dependent by nature.

**Modelled rather than executed** — `exp_oracle.py` applies its fault campaign at the
*alert* level, deriving each fault from the measured behaviour of Q6/Q7, rather than
crashing a live gateway. The engines and specifications it drives are the real, unmodified
ones; what is synthetic is the injection, and the paper says so.

---

## 0. Layout

```
artifact/
├── README.md                 ← this file (start here)
├── MANIFEST.md               ← every file and its role
├── fabric/                   ← controlled-testbed experiments (§7, Q1–Q8)
│   ├── docker-compose.yml         full fabric: mosquitto + nats + backend + gateway + devices
│   ├── docker-compose.attack.yml  coordinated-attack device overlay (Q4 detection)
│   ├── gateway.py                 L1/L2 + timestamping sidecar + durable outbox (G1,G2)
│   ├── backend_l3.py              L3: real MonPoly (P3.1–P3.4) + RTLola (P3.5); 1 Hz tick (G3)
│   ├── device_publisher.py        device emitter (replays workload profiles / traces)
│   ├── exp_oracle.py              Q8 — fault-free-oracle verdict preservation  → Table 3
│   ├── run_crash_exp.sh           Q6 — crash injection at 3 relay stages       → Q6 prose
│   ├── exp_retention.py           Q7 — overload / T_buffer horizon (G5)
│   ├── exp_isolation.py           Q5 — G4 consumer isolation                   → Table 2 (G4 row)
│   ├── exp_baseline.py            Q1 — controlled shared-log vs fabric baseline
│   ├── bench_wan.py               Q2 — per-hop latency under netem WAN delay
│   ├── bench_scale.py/bench_fleet.py  Q3 — throughput and fleet scale
│   ├── monpoly_specs/            MonPoly signature + P3.1–P3.4 formulas
│   ├── rtlola_specs/             RTLola silent-node (P3.5) specification
│   ├── crossgw_specs/            MonPoly spec for cross-gateway P3.6
│   ├── clockskew/                 §7 two-host clock-skew (P3.6) — see clockskew/DISTRIBUTED.md
│   └── expected/                  reference outputs (oracle_results.json, EXPERIMENTS_RESULTS.md)
└── realdata/                 ← real-data validation (§8)
    ├── DOWNLOAD.md                how to fetch WUSTL-IIoT-2021 and TON_IoT
    ├── wustl_water_rv.py          §8 WUSTL water-SCADA flow monitors + detection
    ├── ton_iot_rv.py              §8 TON_IoT flow monitors + detection
    ├── ton_iot_monpoly.py         §8 real MonPoly P3.2/P3.6 on TON_IoT
    └── expected/                  reference reports (*_report.json)
```

---

## 1. Prerequisites

- **Docker** (tested with 28.1) and **Docker Compose v2**.
- **Python ≥ 3.10** with `pip install -r fabric/requirements.txt` (`paho-mqtt`, `nats-py`).
- **The `rvhier:latest` base image** — provides the `monpoly` and `rtlola-cli` *binaries*
  (heavy: compiles MonPoly via opam and RTLola via cargo). Build it once:
  ```bash
  git clone https://github.com/nikos-kekatos/hybrid-hierarchical-rv-edge-iot.git
  docker build -t rvhier:latest -f fabric/Dockerfile.rvbase \
      hybrid-hierarchical-rv-edge-iot/code
  ```
  > This is the **one external dependency**, and it supplies binaries only. The monitor
  > specifications are vendored here in `fabric/monpoly_specs/` and `fabric/rtlola_specs/`
  > and are mounted into the container at run time, so the image does not need to carry
  > them. `backend_l3.py`, `exp_oracle.py` and `ton_iot_monpoly.py` shell out to the real
  > `monpoly` binary from this image.
- **`rtlola-cli`**, only if you want to run the P3.5 specification standalone. The `rvhier`
  image already carries it; a minimal standalone image is:
  ```bash
  docker build -t rtlola:cli - <<'DOCKER'
  FROM rust:1-slim
  RUN cargo install rtlola-cli --locked
  ENTRYPOINT []
  DOCKER
  ```
- **Datasets** for §8 are not bundled (size/licensing). See `realdata/DOWNLOAD.md`.

Hardware used in the paper: a single laptop (Apple M4 Pro, 12 cores, 24 GB RAM) for the
containerised testbed; two cloud VMs (Ubuntu 24.04, NTP disabled on one) for the two-host
clock-skew experiment. Absolute latency/throughput are host-dependent; the *behaviour*
(loss classes, preservation counts) reproduces on any host.

---

## 2. Quick start — the headline result (Table 3, Q8)

The paper's central result (`shared log 1/7 vs fabric 7/7`) is fully reproducible with one
command, using the real MonPoly engine:

```bash
# build rvhier:latest once (see Requirements above)
cd fabric
python3 exp_oracle.py --md
```

**Expected (Table 3 / `tab:oracle`):**

| Configuration        | Preserved | Missed | False all-clears | Downgraded |
|----------------------|:---------:|:------:|:----------------:|:----------:|
| Shared log + faults  | **1/7**   | 6      | **6**            | 0          |
| RV-Fabric (full)     | **7/7**   | 0      | 0                | 0          |
| −G1 / −G2 / −G5      | 6/7       | 1      | 0                | 1          |
| −G3 (tick)           | 5/7       | 2      | 2                | 0          |
| −G4 (isolation)      | 7/7       | 0      | 0                | 0          |

The run prints the JSON on stdout (there is no `--out` flag), so redirect it to compare:
`python3 exp_oracle.py > oracle_results.json` and diff against
`fabric/expected/oracle_results.json`.
The campaign is deterministic and seeded (each fault bound to one incident; P3.3 on `d1` is
the no-fault control) — see the `exp_oracle.py` header for the exact fault→incident bindings.

---

## 3. Controlled-testbed experiments (§7)

Bring up the brokers (or the full fabric) first:
```bash
cd fabric
docker compose up -d mosquitto nats           # brokers only (for exp_isolation/retention/baseline)
# or:  docker compose up --build               # full fabric (devices → gateway → backend)
```

| Paper element        | Command (in `fabric/`)                                  | Expected result |
|----------------------|----------------------------------------------------------|-----------------|
| **Q1** baseline (§7) | `docker compose up -d mosquitto` then `EXP_ROOT="$PWD" python3 exp_baseline.py --n 7000 --rate 500 --trials 5` (and `--rate 3000`). The script generates its own seeded trace into `expdata/`; from inside the compose network add `--broker mosquitto` | **0/7000 loss** on both shared-log and fabric at 500 & 3000 msg/s (the ~1% prior figure does not reproduce). |
| **Q2** latency (§7)  | `python3 bench_wan.py` + `tc qdisc add dev <if> root netem delay 5ms` | MQTT hop ≈ 3δ (17/32/60 ms at δ=5/10/20 ms); JetStream ≈ δ (7/13/23 ms); p95 < 75 ms. |
| **Q3** throughput (§7)| `docker compose up -d nats` then `python3 bench_scale.py` ; `python3 bench_fleet.py` | saturation ~10.7K→14K msg/s at 0 loss; fleets 250/500/1000 devices at 0 loss, ~60 ms p95. Reproduced on a second host (~10.9K plateau, 0.00% loss at all three fleet sizes) — see `expected/q3_scale_fleet.md`. |
| **Q5** isolation, G4 (Table 2) | `python3 exp_isolation.py --n 2000 --delay 0.02`| fast consumer **0.043 s ± 0.001** vs slow peer processed **3**; the 40 s shared-cursor figure is an *arithmetic counterfactual* (`n × delay`), not a measured arm, so ~940× is a ratio against it. The **invariant** is the shape (fast drain in well under a second while the slow peer clears a handful); the drain time itself is host-dependent, so the ratio moves with it (a slower host measured 0.083 s / ~480×). |
| **Q6** crash (inlined in the Q6 prose)| `bash run_crash_exp.sh`                                 | before-fsync loses **1**, after-persist/after-publish lose **0** (dup collapsed to 0 incidents); recovery 8.8/95/474 ms for 100/1000/5000 entries (host-dependent, and linear in the backlog -- that linearity is the claim). |
| **Q7** overload (§7) | `python3 exp_retention.py --cap 1000`                    | `discard=old` silently drops **1000/2000**; `discard=new` raises **1000** publish errors (loss observable). The two loss counts are exact and host-independent; the single-producer publish rate this script measures is *not* (10,658 msg/s recorded, 4,141 on another host) and only feeds the `T_buffer = capacity / R_in` horizons. |
| **Q8** oracle (Table 3)| `python3 exp_oracle.py --md`                           | see §2 above. |

> `run_crash_exp.sh` expects a Docker network `rvexp` with a `nats-exp` JetStream container
> and the `rvnode:latest` image (`docker build -t rvnode:latest -f Dockerfile.node .`); the
> script header documents the K=50 / crash-at-#25 setup.

### Two-host clock-skew of P3.6 (§7, Q4)
The cross-gateway P3.6 skew result is measured across **two separate cloud VMs** — independent
kernel clocks, a real network, NTP disabled on gw2 — by
`fabric/clockskew/run_do_experiment.sh` (two DigitalOcean droplets, destroyed on exit; needs an
authenticated `doctl`). Two cheaper harnesses reproduce the same behaviour without a cloud
account: `two_vm_skew.sh` (two local Lima VMs) and the single-host `libfaketime` variant
`run_skew_faketime.sh`. Walkthrough for all three: `fabric/clockskew/DISTRIBUTED.md` and
`fabric/clockskew/README.md`. Expected: ε tracks the injected offset; P3.6 stays sound while
ε < W = 30 s and flips at 35 s.

---

## 4. Real-data validation (§8)

Download the datasets first (`realdata/DOWNLOAD.md`) into `realdata/wustl/` and
`realdata/ton_iot/`. The flow monitors and MonPoly need the `rvhier:latest` binary for the
formal P3.2/P3.6 firings.

```bash
cd realdata
# WUSTL-IIoT-2021 water-SCADA (Argus/Modbus)
python3 wustl_water_rv.py --csv wustl/wustl_iiot_2021.csv --gateways 2
# TON_IoT (Zeek flows)
python3 ton_iot_rv.py --csv ton_iot/Train_Test_Network.csv --gateways 2
# Real MonPoly P3.2 (coordinated) + P3.6 (cross-gateway) on TON_IoT
#   (writes MonPoly .log inputs, then runs the engine from the rvhier image)
python3 ton_iot_monpoly.py --csv ton_iot/Train_Test_Network.csv --gateways 2
```

**Expected (as reported in §8):**
- **WUSTL:** 14 source identities (6 attackers); per-identity detection **precision 0.83 /
  recall 0.83** (recall_struct 1.0), detection driven by `ddos_flood`. Real MonPoly:
  **P3.2 fires (1499 firings, 17 episodes)**; **P3.6 is satisfied continuously**
  (732 firings collapsing to 1 sustained episode -- no gap exceeds 7 s over a 24.9 min span).
- **TON_IoT:** 11,536 identities (19 attackers); per-identity detection **0.64 / 0.47**
  (0.64 on structural classes). Real MonPoly: **P3.2 (27 firings, 11 episodes)**,
  **P3.6 (522 firings, 168 episodes)**.

Firing and episode counts, and the command that produces them, are in
`realdata/expected/EPISODES.md`.

Each script writes a `*_report.json`; diff against `realdata/expected/`. (Note: an *episode*
is consecutive MonPoly satisfaction timepoints collapsed into one incident.)

---

## 4b. Running the specifications directly

The monitor specifications are in `fabric/monpoly_specs/` (P3.1–P3.4),
`fabric/crossgw_specs/` (P3.6 cross-gateway, P3.7 gateway silence) and
`fabric/rtlola_specs/` (P3.5 silent node). They are mounted into the container at run time,
so the image supplies engines only and you can edit a specification without rebuilding.

```bash
# MonPoly: P3.7, on a trace where one gateway goes dark
docker run --rm -v "$PWD/fabric/crossgw_specs":/s -v /tmp:/w rvhier:latest bash -c \
  "monpoly -sig /s/gwsilence.sig -formula /s/p3_7_gwsilence.mfotl -log /w/gw.log"

# RTLola: P3.5. The trace is a CSV with a `time` column and one column per input
# stream (safe_tx,overflow,time_anomaly,fuzzing), each row naming the emitting device.
docker run --rm -v "$PWD/fabric/rtlola_specs":/s -v /tmp:/w rtlola:cli sh -c \
  "rtlola-cli monitor /s/silent_node.lola --offline relative --csv-in /w/trace.csv"

# Collapse MonPoly satisfactions into distinct episodes (see realdata/expected/EPISODES.md)
python3 realdata/episodes.py --out p36.out --window 30
```

## 5. Notes & honesty flags

- **Determinism / seeds:** the oracle campaign (Q8) and the shared-log/fabric baseline (Q1)
  are seeded and deterministic. Stochastic figures (detection, isolation, recovery) are means
  over 5–10 runs with 95% CIs in the paper.
- **Gateway partition (§8):** neither dataset records a gateway topology, so each source
  identity is mapped to one of two logical gateways by a *stable MD5* of its identifier
  (`_stable_gw()`), independent of label and attack class. It used to use Python's builtin
  `hash()`, which is salted per process and made every cross-gateway count irreproducible;
  `--gw-seed` now varies the split deliberately. P3.6 counts depend on the partition, P3.2
  counts do not — see `REPRODUCIBILITY.md`.
- **L3 feed:** both datasets feed the fleet properties from what the flow monitors flagged,
  with no reference to the ground-truth labels. An earlier version of `wustl_water_rv.py`
  intersected its feed with the label set, which leaked labels into the correlation stream.
- **Negative result:** CTU-13 was tried and did **not** fit the flow monitors (precision 0.013);
  it is not used in the paper (`realdata/ctu13_flow_rv.py`, EXPERIMENTS_RESULTS §3g).
- `fabric/expected/EXPERIMENTS_RESULTS.md` is the full experiment log; some real-data entries
  there predate the final sliding-window monitors — the paper and the `*_report.json` files
  carry the final numbers.
