# Q4 detection + G2 order violations (full-stack recorded run)

Produced by the full fabric with the coordinated-attack overlay, against the real
`rvhier:latest` base (MonPoly built via opam/dune, RTLola via cargo):

```sh
docker compose -f docker-compose.yml -f docker-compose.attack.yml up --build
docker logs <backend container>
```

**This is one sample, not a golden file.** `device_publisher.py` seeds from
`os.urandom(16)`, so the workload is randomised per run by design; the paper reports means
over five runs. Diff the *shape* — which properties fire, and that gateway-order violations
stay at zero — not the counts.

Recorded 2026-08-24, Apple M4 Pro / Docker Desktop, ~10 min of runtime.

## G2 — timestamp-order violations (Table 2, G2 row)

`backend_l3.py::_count_mono` counts, on the *same* received verdict stream, timestamps that
go backwards under device-time ordering (what a shared log feeds MonPoly) versus
gateway-time ordering (what the sidecar feeds).

```
[MONO] n=1200 device-order-viol=83 gateway-order-viol=0
```

Across two runs of this experiment: **78 and 83** device-order violations at n=1200, and
**0** gateway-order violations in both. The paper's Table 2 quotes `0 → 76 of 1200`; the
device figure moves with the randomised time-spoof workload, the **zero is the invariant**
and it is exact.

## Q4 — detection through the real engines

All seven fleet properties are exercised end-to-end. Incident counts from this run:

| Property | Incident | Count |
|---|---|---:|
| P3.1 | `APT_INDICATOR` | 31 |
| P3.2 | `COORDINATED_ATTACK` | 15 |
| P3.3 | `ESCALATION_PATTERN` | 31 |
| P3.4 | `PERSISTENT_THREAT` | 10 |
| P3.5 | `SILENT_NODE_ANOMALY` | 11802 |
| P3.6 | cross-gateway campaign | 14 |
| P3.7 | monitor up, no firing | 0 |

P3.1–P3.4 run through the real MonPoly engine, P3.5 through real RTLola, P3.6 through the
cross-gateway MonPoly correlator, P3.7 through the gateway-silence monitor.

Two things to read correctly:

- **P3.5's count is per tick, not per episode.** The specification triggers at
  `@Local(1Hz)` for as long as a node stays silent, so one silent device yields one incident
  per second. Collapse them the way `realdata/episodes.py` collapses MonPoly satisfactions
  before comparing against anything.
- **P3.7 correctly does not fire.** No gateway went dark in this run; both kept reporting.
  Its firing behaviour is exercised directly in `crossgw_specs/p3_7_gwsilence.mfotl`
  (a gateway dark at t=15 with `T_gw=10` fires from t=26).

## Note: P3.5 was broken in the live backend until this run

`silent_node.lola` was rewritten to declare four input streams
(`safe_tx`, `overflow`, `time_anomaly`, `fuzzing`) so that *any* verdict counts as "still
reporting", but `correlator_monitor.py` still wrote a two-column CSV header
(`overflow,safe_tx`). `rtlola-cli` rejects a spec whose declared inputs are not all named in
the header, so P3.5 failed at startup with:

```
⚠️ [RTLola] error: CSV header does not contain an entry for stream `time_anomaly`.
```

The header and row builder are now driven by one `RTLOLA_STREAMS` constant that must stay in
step with the specification's `input` declarations. Only the live backend was affected —
`exp_oracle.py` (Q8) drives P3.5 through its own reference detector, so Table 3 was never
touched by this.
