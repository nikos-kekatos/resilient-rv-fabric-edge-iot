# MANIFEST — file inventory and paper mapping

Every file in this artifact and what it corresponds to in the paper. See `README.md` for
run instructions.

## Paper element → script

| Paper element                       | File(s)                                   |
|-------------------------------------|-------------------------------------------|
| Fig. 2 architecture (the fabric)    | `fabric/gateway.py`, `fabric/backend_l3.py`, `fabric/device_publisher.py`, `fabric/docker-compose.yml` |
| §4 five guarantees G1–G5            | `fabric/gateway.py` (G1 outbox, G2 sidecar), `fabric/backend_l3.py` (G3 tick, G4 cursors), JetStream config (G5) |
| **Q1** baseline (§6)                | `fabric/exp_baseline.py`                   |
| **Q2** latency (§6)                 | `fabric/bench_wan.py`                      |
| **Q3** throughput / fleet (§6)      | `fabric/bench_scale.py`, `fabric/bench_fleet.py`, `fabric/bench_qos.py` |
| **Q4** detection (§6)               | `fabric/docker-compose.attack.yml` + `fabric/device_publisher.py` |
| **Q4** clock-skew P3.6 (two hosts)  | `fabric/clockskew/` (`two_vm_skew.sh`, `crossgw_monpoly.py`, `clock_beacon.py`, `docker-compose.skew.yml`, `DISTRIBUTED.md`) |
| **Q5** ablation, G4 isolation (Table 3) | `fabric/exp_isolation.py`             |
| **Q6** crash recovery (Table 2)     | `fabric/run_crash_exp.sh`, `fabric/exp_crash_relay.py`, `fabric/exp_count_stream.py` |
| **Q7** overload / T_buffer (§6)     | `fabric/exp_retention.py`                  |
| **Q8** oracle preservation (Table 4)| `fabric/exp_oracle.py` → `fabric/expected/oracle_results.json` |
| §4 evidence tags (4 scenarios)      | `fabric/exp_completeness.py`               |
| **§7** WUSTL water-SCADA            | `realdata/wustl_water_rv.py` → `realdata/expected/wustl_water_report.json` |
| **§7** TON_IoT detection            | `realdata/ton_iot_rv.py` → `realdata/expected/ton_iot_report.json` |
| **§7** TON_IoT real MonPoly P3.2/P3.6 | `realdata/ton_iot_monpoly.py`           |
| §7 negative result (not in paper)   | `realdata/ctu13_flow_rv.py`, `realdata/iot23_flow_rv.py` |

## Supporting files

| File                                | Role                                       |
|-------------------------------------|--------------------------------------------|
| `fabric/canonicaliser.py`           | L1 canonicalisation filter (used by baseline) |
| `fabric/monpoly_specs/{signature.sig,p3_1_apt,p3_2_botnet,p3_3_escalation,p3_4_persistent}.mfotl` | MonPoly signature + the fleet formulas P3.1–P3.4 |
| `fabric/rtlola_specs/silent_node.lola` | RTLola time-triggered silent-node specification (P3.5) |
| `fabric/crossgw_specs/{crossgw.sig,p3_6_crossgw.mfotl}` | MonPoly signature + formula for cross-gateway P3.6 |
| `fabric/mosquitto/mosquitto.conf`   | MQTT broker config                         |
| `fabric/Dockerfile.{backend,node,bench,rvbase}` | container images (backend `FROM rvhier:latest`) |
| `fabric/requirements.txt`           | Python deps (`paho-mqtt`, `nats-py`)       |
| `fabric/expected/EXPERIMENTS_RESULTS.md` | full experiment log / expected numbers |
| `realdata/DOWNLOAD.md`              | dataset download instructions              |

## External dependency (not bundled)

- **`rvhier:latest`** Docker image — provides the `monpoly` + `rtlola-cli` **binaries**
  only; build it from https://github.com/nikos-kekatos/hybrid-hierarchical-rv-edge-iot
  (see README). The specs themselves are vendored in this repo. Required by
  `backend_l3.py`, `exp_oracle.py`, `ton_iot_monpoly.py`.
- **Datasets** — WUSTL-IIoT-2021 and TON_IoT (see `realdata/DOWNLOAD.md`).
