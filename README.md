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

This artifact reproduces every quantitative claim in the paper: the controlled-testbed
experiments (§6, Q1–Q8) and the real-data validation (§7, WUSTL water-SCADA + TON_IoT).
All numbers were produced by the code here against real brokers (Mosquitto MQTT + NATS
JetStream) and the real MonPoly engine — nothing is simulated or hard-coded.

---

## 0. Layout

```
artifact/
├── README.md                 ← this file (start here)
├── MANIFEST.md               ← every file and its role
├── fabric/                   ← controlled-testbed experiments (§6, Q1–Q8)
│   ├── docker-compose.yml         full fabric: mosquitto + nats + backend + gateway + devices
│   ├── docker-compose.attack.yml  coordinated-attack device overlay (Q4 detection)
│   ├── gateway.py                 L1/L2 + timestamping sidecar + durable outbox (G1,G2)
│   ├── backend_l3.py              L3: real MonPoly (P3.1–P3.4) + RTLola (P3.5); 1 Hz tick (G3)
│   ├── device_publisher.py        device emitter (replays workload profiles / traces)
│   ├── exp_oracle.py              Q8 — fault-free-oracle verdict preservation  → Table 4
│   ├── run_crash_exp.sh           Q6 — crash injection at 3 relay stages       → Table 2
│   ├── exp_retention.py           Q7 — overload / T_buffer horizon (G5)
│   ├── exp_isolation.py           Q5 — G4 consumer isolation                   → Table 3 (G4 row)
│   ├── exp_baseline.py            Q1 — controlled shared-log vs fabric baseline
│   ├── bench_wan.py               Q2 — per-hop latency under netem WAN delay
│   ├── bench_scale.py/bench_fleet.py  Q3 — throughput and fleet scale
│   ├── monpoly_specs/            MonPoly signature + P3.1–P3.4 formulas
│   ├── rtlola_specs/             RTLola silent-node (P3.5) specification
│   ├── crossgw_specs/            MonPoly spec for cross-gateway P3.6
│   ├── clockskew/                 §6 two-host clock-skew (P3.6) — see clockskew/DISTRIBUTED.md
│   └── expected/                  reference outputs (oracle_results.json, EXPERIMENTS_RESULTS.md)
└── realdata/                 ← real-data validation (§7)
    ├── DOWNLOAD.md                how to fetch WUSTL-IIoT-2021 and TON_IoT
    ├── wustl_water_rv.py          §7 WUSTL water-SCADA flow monitors + detection
    ├── ton_iot_rv.py              §7 TON_IoT flow monitors + detection
    ├── ton_iot_monpoly.py         §7 real MonPoly P3.2/P3.6 on TON_IoT
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
- **Datasets** for §7 are not bundled (size/licensing). See `realdata/DOWNLOAD.md`.

Hardware used in the paper: a single laptop (Apple M4 Pro, 12 cores, 24 GB RAM) for the
containerised testbed; two cloud VMs (Ubuntu 24.04, NTP disabled on one) for the two-host
clock-skew experiment. Absolute latency/throughput are host-dependent; the *behaviour*
(loss classes, preservation counts) reproduces on any host.

---

## 2. Quick start — the headline result (Table 4, Q8)

The paper's central result (`shared log 1/7 vs fabric 7/7`) is fully reproducible with one
command, using the real MonPoly engine:

```bash
# build rvhier:latest once (see Requirements above)
cd fabric
python3 exp_oracle.py --md
```

**Expected (Table 4 / `tab:oracle`):**

| Configuration        | Preserved | Missed | False all-clears | Downgraded |
|----------------------|:---------:|:------:|:----------------:|:----------:|
| Shared log + faults  | **1/7**   | 6      | **6**            | 0          |
| RV-Fabric (full)     | **7/7**   | 0      | 0                | 0          |
| −G1 / −G2 / −G5      | 6/7       | 1      | 0                | 1          |
| −G3 (tick)           | 5/7       | 2      | 2                | 0          |
| −G4 (isolation)      | 7/7       | 0      | 0                | 0          |

The run also writes `oracle_results.json`; diff it against `fabric/expected/oracle_results.json`.
The campaign is deterministic and seeded (each fault bound to one incident; P3.3 on `d1` is
the no-fault control) — see the `exp_oracle.py` header for the exact fault→incident bindings.

---

## 3. Controlled-testbed experiments (§6)

Bring up the brokers (or the full fabric) first:
```bash
cd fabric
docker compose up -d mosquitto nats           # brokers only (for exp_isolation/retention/baseline)
# or:  docker compose up --build               # full fabric (devices → gateway → backend)
```

| Paper element        | Command (in `fabric/`)                                  | Expected result |
|----------------------|----------------------------------------------------------|-----------------|
| **Q1** baseline (§6) | `python3 exp_baseline.py --n 7000 --rate 500` (and `--rate 3000`) | **0/7000 loss** on both shared-log and fabric at 500 & 3000 msg/s (the ~1% prior figure does not reproduce). |
| **Q2** latency (§6)  | `python3 bench_wan.py` + `tc qdisc add dev <if> root netem delay 5ms` | MQTT hop ≈ 3δ (17/32/60 ms at δ=5/10/20 ms); JetStream ≈ δ (7/13/23 ms); p95 < 75 ms. |
| **Q3** throughput (§6)| `python3 bench_scale.py` ; `python3 bench_fleet.py`     | saturation ~10.7K→14K msg/s at 0 loss; fleets 250/500/1000 devices at 0 loss, ~60 ms p95. |
| **Q5** isolation, G4 (Table 3) | `python3 exp_isolation.py --n 2000 --delay 0.02`| fast consumer **0.043 s ± 0.001** vs slow peer processed **3**; shared-cursor counterfactual 40 s → **~940× isolation**. |
| **Q6** crash (Table 2)| `bash run_crash_exp.sh`                                 | before-fsync loses **1**, after-persist/after-publish lose **0** (dup collapsed to 0 incidents); recovery 8.8/95/474 ms for 100/1000/5000 entries. |
| **Q7** overload (§6) | `python3 exp_retention.py --cap 1000`                    | `discard=old` silently drops **1000/2000**; `discard=new` raises **1000** publish errors (loss observable); publish ceiling ~10.7K msg/s. |
| **Q8** oracle (Table 4)| `python3 exp_oracle.py --md`                           | see §2 above. |

> `run_crash_exp.sh` expects a Docker network `rvexp` with a `nats-exp` JetStream container
> and the `rvnode:latest` image (`docker build -t rvnode:latest -f Dockerfile.node .`); the
> script header documents the K=50 / crash-at-#25 setup.

### Two-host clock-skew of P3.6 (§6, Q4)
The cross-gateway P3.6 skew result is measured across **two independent hosts**. See
`fabric/clockskew/DISTRIBUTED.md` and `fabric/clockskew/two_vm_skew.sh`. A single-host
`libfaketime` variant (`run_skew_faketime.sh`) gives the same result. Expected: ε tracks the
injected offset; P3.6 stays sound while ε < W = 30 s and flips at 35 s.

---

## 4. Real-data validation (§7)

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
python3 ton_iot_monpoly.py
```

**Expected (as reported in §7):**
- **WUSTL:** 14 source identities (6 attackers); per-identity detection **precision 0.83 /
  recall 0.83** (recall_struct 1.0), detection driven by `ddos_flood`. Real MonPoly:
  **P3.2 fires (2 episodes), P3.6 fires (17 episodes)**.
- **TON_IoT:** 11,536 identities (19 attackers); per-identity detection **0.64 / 0.47**
  (0.64 on structural classes). Real MonPoly: **P3.2 (11 episodes), P3.6 (154 episodes)**.

Each script writes a `*_report.json`; diff against `realdata/expected/`. (Note: an *episode*
is consecutive MonPoly satisfaction timepoints collapsed into one incident.)

---

## 5. Notes & honesty flags

- **Determinism / seeds:** the oracle campaign (Q8) and the shared-log/fabric baseline (Q1)
  are seeded and deterministic. Stochastic figures (detection, isolation, recovery) are means
  over 5–10 runs with 95% CIs in the paper.
- **Gateway partition (§7):** neither dataset records a gateway topology; each source identity
  is mapped to one of two logical gateways by a hash of its identifier, independent of label
  and attack class (see `gw_of()` in `wustl_water_rv.py` / `ton_iot_rv.py`).
- **Negative result:** CTU-13 was tried and did **not** fit the flow monitors (precision 0.013);
  it is not used in the paper (`realdata/ctu13_flow_rv.py`, EXPERIMENTS_RESULTS §3g).
- `fabric/expected/EXPERIMENTS_RESULTS.md` is the full experiment log; some real-data entries
  there predate the final sliding-window monitors — the paper and the `*_report.json` files
  carry the final numbers.
