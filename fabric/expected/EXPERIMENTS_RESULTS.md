# RV-Fabric experiment results

Real measurements produced against a real NATS JetStream (container `nats:2 -js`)
using the production `gateway.DurableOutbox` and the production `backend_l3` eid
dedup rule. Reproduce with the scripts in this directory (all run inside
`rvnode:latest`, built from `Dockerfile.node`).

## 1. Crash-injection & recovery (`run_crash_exp.sh`)
K=50 verdicts on one gateway stream; a hard crash (`os._exit`, SIGKILL-equivalent)
is injected while relaying verdict #25, at each of the three relay stages. A
deduping consumer then reads the whole stream and counts.

| Failure point                         | Events lost | Duplicate publications | Duplicate incidents (after eid-dedup) |
|---------------------------------------|:-----------:|:----------------------:|:-------------------------------------:|
| before_persist (MQTT ack -> WAL fsync)|      1      |           0            |                  0                    |
| after_persist  (WAL -> publish ack)   |      0      |           0            |                  0                    |
| after_publish  (ack -> cursor advance)|      0      |           1            |                  0                    |

Recovery time (worst case: cursor lost, replay whole WAL):
100 -> 8.75 ms; 1000 -> 95.15 ms; 5000 -> 474.01 ms (~0.095 ms/entry, linear).

**Finding.** The outbox makes the WAL->JetStream window fully crash-tolerant
(0 loss; the after_publish replay is collapsed to 0 duplicate incidents by eid
dedup). The ONLY loss is at `before_persist`: the residual window between the MQTT
ack and the WAL fsync, where the in-flight verdict is still in memory. This bounds
and corrects the paper's earlier claim that the outbox closes the whole
"MQTT-receipt -> JetStream-durability" window.

## 2. Retention exhaustion / T_buffer (`exp_retention.py`, cap=1000)
Bounded stream (`max_msgs`), no consumer, pushed to 2x capacity.

| Discard policy | Attempted | Publish errors | Retained | Silently lost | Loss observable? |
|----------------|:---------:|:--------------:|:--------:|:-------------:|:----------------:|
| discard=old (JetStream default) |  2000 |    0   | 1000 | 1000 | **no** |
| discard=new                     |  2000 | 1000   | 1000 |    0 | **yes** |

Measured durable-publish rate: ~10 658 msg/s (cross-checks the paper's ~10K
one-producer Q3 figure).

Buffer horizon T_buffer = capacity / R_in:

| Capacity | @1930 msg/s (1000-dev fleet) | @10 658 msg/s (measured max) |
|----------|:----------------------------:|:----------------------------:|
| 10 000   | 5.2 s   | 0.9 s  |
| 100 000  | 51.8 s  | 9.4 s  |
| 1 000 000| 518.1 s | 93.8 s |

**Finding.** Under the default `discard=old`, sustained overload beyond retention
silently drops the OLDEST verdicts with no producer signal -> silent continuity
loss. Switching to `discard=new` converts overflow into an explicit publish error
the gateway can surface as a health alert BEFORE monitoring continuity is lost.
A 1M-message buffer absorbs an ~8.6 min backend outage at the 1000-device rate.

---

## 3b. Real multi-device case study on IoT-23 (`data/iot23_flow_rv.py`)
Real public malware traffic (Stratosphere IoT-23), 4 captures merged as one fleet
(each capture's source IPs prefixed by capture tag = distinct devices), split over
2 gateways. The iot_app payload predicates do NOT match flow malware, so three
flow-appropriate RV monitors were written: `port_scan` (>=20 distinct failed dsts),
`ddos_flood` (>=100 conns/dst in 10s), `c2_beacon` (>=50 repeats to few dsts).

- 53,612 connections, **748 devices across 2 gateways**, 3 GT-malicious devices.
- Device-level detection: TP=3 FP=1 TN=744 FN=0 -> **precision 0.75, recall 1.00, F1 0.857**
  (the 1 FP is Torii/cap20-1: it scans but is majority-benign in this slice).
- port_scan fired on Hide'n'Seek/Muhstik/Torii; c2_beacon on Hakai C&C.
- **P3.2 (coordinated scan, >=3 devices) FIRES; P3.6 (scan across >=2 gateways) FIRES**
  -> real cross-device + cross-gateway correlation on real data.

Note: the iot_app-style overflow/fuzz/time-spoof predicates are semantically
mismatched to flow malware (they gave recall~0 once the time-spoof replay artifact
was removed); these flow specs are the correct monitors for this data.

## 3c. Real multi-device case study on TON_IoT (`data/ton_iot_rv.py`)
TON_IoT (UNSW Cyber Range), `Train_Test_Network.csv`, 461,044 Zeek-derived flows,
device = src_ip, hashed across 2 gateways. Same flow monitors as 3b
(port_scan / ddos_flood / c2_beacon) run directly (CSV keeps src/dst IP + conn_state).

- 11,536 devices / 2 gateways; **19 attacker hosts** (scanning/dos/ddos/backdoor each
  have >=3 distinct sources -> P3.2 genuinely supported).
- Device-level: TP=14 FP=20 TN=11497 FN=5 -> precision 0.412,
  **recall 0.818 on the structural attack classes** these monitors target
  (scan/flood/C&C); app-layer attacks (injection/xss/password/ransomware/mitm)
  need content specs and are out of scope for these 3 monitors.
- Per-monitor precision: **port_scan 0.75, ddos_flood 0.70**, c2_beacon 0.32 (noisy
  on TON_IoT's benign repeated-failure traffic; it was clean on IoT-23 Hakai C&C).
- **P3.2 (coordinated scan, >=3 devices) FIRES; P3.6 (scan across >=2 gateways) FIRES.**

## 3d. Real MonPoly L3 on TON_IoT (`data/ton_iot_monpoly.py`)
Bridges the flow monitor to the REAL formal engine: each port_scan-flagged device's
failed-scan timestamps become `overflow(device,ts)` events (subsampled 1/5s), fed to
the UNMODIFIED `monpoly` binary (rvhier image) with the stock `p3_2_botnet.mfotl`
(coordinated attack: >=3 distinct devices in 30s) and `p3_6_crossgw.mfotl` (>=2
gateways). 8 scanner devices, 13,782 events.
- **P3.2 fires 1386 times**, up to **4 distinct devices** coordinated in a 30s window
  (e.g. `@21950 (tp 161): (4)`).
- **P3.6 fires** with **2 gateways** (e.g. `@3716 (tp 36): (2)`).
This is genuine formal-RV execution on real malware traffic, not a structural count.

## 3c-updated. TON_IoT beacon periodicity fix (`data/ton_iot_rv.py`)
Added a periodicity gate to c2_beacon (coeff. of variation of inter-arrival < 0.5 =
regular C&C, not bursty benign retries). Cut FP 20->5:
overall device-level **precision 0.41->0.69, F1 0.53->0.63**; per-monitor
port_scan 0.75, ddos_flood 0.70, **c2_beacon 0.32->0.50**; recall 0.73 on structural
classes; P3.2/P3.6 still fire.

## 3e. Cross-gateway P3.6 under injected clock skew (`ton_p36` + monpoly)
Addresses the single-host ε≈0 concern: re-ran the real MonPoly P3.6 on TON_IoT with
gw2's clock offset by ε. P3.6 (>=2 gateways, 30s window) firings vs ε:
0s->1159, 5s->1132, 15s->1122, 30s->1118, 60s->1111, 120s->1049. Cross-gateway
correlation is ROBUST to gateway clock skew for sustained campaigns (only ~10% drop
at ε=120s), because coordinated scanning is long-lived; the ε-aware window matters
only for brief coincidences.

## 3f. G4 consumer isolation (`exp_isolation.py`)
Fast L3 consumer drains 2000 verdicts in **0.043 s +/- 0.001** (5 runs) while a slow
consumer (20 ms/verdict) had processed only **3**; shared-log serialisation
counterfactual = 40 s -> **~938x isolation** from independent durable cursors (G4).

## 3g. CTU-13 (NEGATIVE, not used in paper)
Ran the flow monitors on CTU-13 Neris(10 bots)+Rbot. Result: precision 0.013,
recall 0.15 -- the monitors DON'T fit. CTU-13 bots are spam/click-fraud/C&C with
fanout of only 4-5 dsts, while high-fanout hosts are benign background. port_scan is
the wrong monitor for a spam botnet; detecting Neris needs spam/C&C-specific specs.
Honest negative; TON_IoT (scanning/DDoS/backdoor, which DO match) stays the case study.

## 3h. REAL water-SCADA pilot on WUSTL-IIoT-2021 (`data/wustl_water_rv.py`)
Openly-published water-supply SCADA Argus flow dataset (Modbus/port 502; WUSTL,
`curl -k .../wustl_iiot_2021.zip`, ~1M flows). Argus has no Zeek conn_state, so the
failed-probe signal is DstPkts==0. Same flow monitors + real MonPoly L3.
- 14 source identities, 6 attackers (5 structural); attack mix DoS 78305 / Reconn
  8240 / CommInj 259 / Backdoor 212 flows.
- Detection (per source identity): TP=5 FP=1 FN=1 -> precision 0.83, recall_all 0.83,
  **recall_struct 1.0**. ddos_flood fired on 5 sources (incl. 3 external DoS hosts
  across both gateways), c2_beacon on 1.
- **REAL MonPoly: P3.2 (coordinated attack, >=3 sources/30s) FIRES (3 sources, 6x);
  P3.6 (>=2 gateways/30s) FIRES 593x** -- genuine temporal co-occurrence on real
  water-SCADA data (unlike IoT-23, whose captures were temporally disjoint).
This makes the CRITIS water case study a REAL pilot, not just an illustrative mapping.

## 3. Controlled same-host shared-log vs fabric baseline (`exp_baseline.py`)
One fixed 7000-event trace replayed through the REAL shared-log transport
(`tail -n0 -F events.log | canonicaliser.py`, 8 concurrent per-event `open(append)`
writers) and the fabric (MQTT QoS1, subscriber attached first), same host, same
pacing, warm-up trial discarded, drained to stable. Delivered = events reaching L1.

| Rate (msg/s) | Trials | Shared-log lost | Fabric lost |
|--------------|:------:|:---------------:|:-----------:|
| 500          |   5    | 0 / 7000        | 0 / 7000    |
| 3000         |   5    | 0 / 7000        | 0 / 7000    |

**Finding.** In a fair, controlled steady-state comparison BOTH transports deliver
losslessly; the ~1% shared-log ingest loss imported from the prior prototype does
NOT reproduce. It is therefore prototype/condition-specific (startup truncation
race, higher concurrency, or teardown), not an intrinsic steady-state property. The
fabric's demonstrated advantage is resilience under FAULTS (crash exp. 1, overload
exp. 2, silence) plus the structural properties (ordering, silence-clock, consumer
isolation, flow control) the shared log lacks, not steady-state ingest loss.

---

## 4. Fault-injected verdict preservation vs a fault-free oracle (`exp_oracle.py`)
Shows the mechanisms change the
SECURITY CONCLUSION, not just message transport. A fixed seeded workload triggers
all seven properties P3.1-P3.7. The clean stream through the UNMODIFIED MonPoly
engine (rvhier image, stock specs) + reference detectors for the tick-driven P3.5/
P3.7 defines the oracle incident set I* (|I*|=7). One combined fault campaign
(crash=before_persist loss; reorder=monotonicity rejection; node+gateway silence;
retention overload=discard=old silent eviction -- each grounded in exp. 1-2) is
replayed under each config; the ONLY thing that changes is which alerts, in what
order, reach the detectors. Each miss is then classified by the algebra V=(s,c)
from the fabric state a consumer sees (eid gap, order violation, watermark, tick).

| Configuration | Preserved | Rate | Missed | Silent false all-clears | Downgraded to unknown |
|---------------|:---------:|:----:|--------|:-----------------------:|:---------------------:|
| shared-log    | 1/7 | 0.143 | P3.1 P3.2 P3.4 P3.5 P3.6 P3.7 | **6** | 0 |
| fabric (full) | 7/7 | 1.000 | --   | 0 | 0 |
| fabric -G1    | 6/7 | 0.857 | P3.4 (crash loss) | 0 | 1 (incomplete: eid gap) |
| fabric -G2    | 6/7 | 0.857 | P3.1 (reorder)    | 0 | 1 (incomplete: order violation) |
| fabric -G3    | 5/7 | 0.714 | P3.5 P3.7 (silence) | **2** | 0 |
| fabric -G4    | 7/7 | 1.000 | -- (timeliness only; see 3f) | 0 | 0 |
| fabric -G5    | 6/7 | 0.857 | P3.2 (overload)   | 0 | 1 (incomplete: eid gap) |

**Findings.**
1. The shared log loses SIX of the seven incidents and every one is a SILENT false
   all-clear (unqualified no_violation): it cannot express the gateway-scoped P3.6/
   P3.7, has no tick for P3.5, and drops the crash/reorder/overload-hit incidents
   with no signal. The full fabric reproduces the oracle exactly (7/7, 0 false
   all-clears).
2. Each disabled guarantee reintroduces exactly ONE class of miss -- direct causal
   evidence that the mechanism, not incidental engineering, protects the verdict.
3. THE KEY RESULT for the algebra: whenever the evidence model is intact (event ids
   for G1/G5 loss, the gateway-time order reference for G2), the resulting miss is
   DOWNGRADED to `incomplete` -- an honest "unknown" -- instead of a silent false
   all-clear. The fabric converts 4 of the shared log's 6 false all-clears into
   flagged unknowns.
4. HONEST LIMIT: G3 is load-bearing for the algebra ITSELF. Removing the tick
   removes the liveness clock the completeness status is computed against, so the
   two silence incidents (P3.5, P3.7) become false all-clears the algebra cannot
   rescue. Not every mechanism is algebra-recoverable; the liveness pulse is the
   one whose loss is unflaggable.
5. G4 (isolation) is a TIMELINESS guarantee: it preserves the incident SET (7/7
   here) but not delivery latency; its effect is quantified separately by the ~938x
   consumer-isolation result (3f).

Reproduce: `python3 exp_oracle.py --md` (needs the `rvhier:latest` image for the
real monpoly binary; deterministic, ~1 container start).
