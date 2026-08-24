# MANIFEST — file inventory and paper mapping

Every file in this artifact and what it corresponds to in the paper (generated outputs --
`expdata/`, `*_report.json`, `*.log`, `_oracle_work/` -- are not listed; see `.gitignore`).
See `README.md` for run instructions.

## Paper element → script

| Paper element                       | File(s)                                   |
|-------------------------------------|-------------------------------------------|
| Fig. 2 architecture (the fabric)    | `fabric/gateway.py`, `fabric/backend_l3.py`, `fabric/device_publisher.py`, `fabric/docker-compose.yml` |
| §4 five guarantees G1–G5            | `fabric/gateway.py` (G1 outbox, G2 sidecar), `fabric/backend_l3.py` (G3 tick, G4 cursors), JetStream config (G5) |
| **Q1** baseline (§7)                | `fabric/exp_baseline.py`                   |
| **Q2** latency (§7)                 | `fabric/bench_wan.py`                      |
| **Q3** throughput / fleet (§7)      | `fabric/bench_scale.py`, `fabric/bench_fleet.py`, `fabric/bench_qos.py` |
| **Q4** detection (§7)               | `fabric/docker-compose.attack.yml` + `fabric/device_publisher.py` |
| **Q4** clock-skew P3.6 (two hosts)  | `fabric/clockskew/` (`two_vm_skew.sh`, `crossgw_monpoly.py`, `clock_beacon.py`, `docker-compose.skew.yml`, `DISTRIBUTED.md`) |
| **Q5** ablation, G4 isolation (Table 2) | `fabric/exp_isolation.py`             |
| **Q6** crash recovery (inlined in the Q6 prose)     | `fabric/run_crash_exp.sh`, `fabric/exp_crash_relay.py`, `fabric/exp_count_stream.py` |
| **Q7** overload / T_buffer (§7)     | `fabric/exp_retention.py`                  |
| **Q8** oracle preservation (Table 3)| `fabric/exp_oracle.py` → `fabric/expected/oracle_results.json` |
| §4 evidence tags (4 scenarios)      | `fabric/exp_completeness.py`               |
| **§8** WUSTL water-SCADA            | `realdata/wustl_water_rv.py` → `realdata/expected/wustl_water_report.json` |
| **§8** TON_IoT detection            | `realdata/ton_iot_rv.py` → `realdata/expected/ton_iot_report.json` |
| **§8** TON_IoT real MonPoly P3.2/P3.6 | `realdata/ton_iot_monpoly.py`           |
| §8 negative result (not in paper)   | `realdata/ctu13_flow_rv.py`, `realdata/iot23_flow_rv.py` |

## Supporting files

| File                                | Role                                       |
|-------------------------------------|--------------------------------------------|
| `fabric/canonicaliser.py`           | L1 canonicalisation filter (used by baseline) |
| `fabric/monpoly_specs/{signature.sig,p3_1_apt,p3_2_botnet,p3_3_escalation,p3_4_persistent}.mfotl` | MonPoly signature + the fleet formulas P3.1–P3.4 |
| `fabric/rtlola_specs/silent_node.lola` | RTLola time-triggered silent-node specification (P3.5) |
| `fabric/crossgw_specs/{gwsilence.sig,p3_7_gwsilence.mfotl}` | MonPoly signature + formula for gateway silence (P3.7) |
| `fabric/correlator_monitor.py` | the L3 engine wrappers (MonPoly/RTLola subprocess drivers) `backend_l3.py` imports |
| `realdata/episodes.py` | collapses MonPoly satisfactions into distinct episodes |
| `fabric/crossgw_specs/{crossgw.sig,p3_6_crossgw.mfotl}` | MonPoly signature + formula for cross-gateway P3.6 |
| `fabric/mosquitto/mosquitto.conf`   | MQTT broker config                         |
| `fabric/Dockerfile.{backend,node,bench,rvbase}` | container images (backend `FROM rvhier:latest`) |
| `fabric/requirements.txt`           | Python deps (`paho-mqtt`, `nats-py`)       |
| `fabric/expected/EXPERIMENTS_RESULTS.md` | full experiment log / expected numbers |
| `fabric/expected/q3_scale_fleet.md` | Q3 saturation + fleet output recorded on a second host |
| `realdata/DOWNLOAD.md`              | dataset download instructions              |
| `realdata/expected/EPISODES.md`     | firing/episode counts for P3.2 & P3.6, and the command that reproduces them |
| `REPRODUCIBILITY.md`                | determinism notes: the stable gateway partition, what is seeded, what needs services |
| `fabric/bench.py`                   | shared MQTT/JetStream latency probe used by `bench_wan.py` inside `Dockerfile.bench` |
| `fabric/clockskew/{bootstrap_gw1.sh,bootstrap_gw2.sh,measure_skew.py,measure_eps.py,gw_silence.py,run_do_experiment.sh,run_do_clean.sh,run_skew_faketime.sh,Dockerfile.faketime}` | the rest of the two-host clock-skew harness: VM bootstrap, ε/skew measurement, the single-host `libfaketime` variant (see `clockskew/DISTRIBUTED.md`) |
| `paper/{paper.pdf,paper.tex,references.bib}` | the accepted manuscript and its sources |

## External dependency (not bundled)

- **`rvhier:latest`** Docker image — provides the `monpoly` + `rtlola-cli` **binaries**
  only; build it from https://github.com/nikos-kekatos/hybrid-hierarchical-rv-edge-iot
  (see README). The specs themselves are vendored in this repo. Required by
  `backend_l3.py`, `exp_oracle.py`, `ton_iot_monpoly.py`.
- **Datasets** — WUSTL-IIoT-2021 and TON_IoT (see `realdata/DOWNLOAD.md`).
