# Reproducibility notes

## Gateway partition (affects every cross-gateway / P3.6 result)

Neither TON_IoT nor WUSTL-IIoT-2021 records a gateway assignment, so the real-data
scripts partition source identities across two logical gateways themselves.

That partition used Python's builtin `hash()`, which is **salted per process**
(`PYTHONHASHSEED`). The split therefore differed on every run and on every machine, and
so did any count derived from it. This is fixed: the partition is now a stable MD5 of the
identity (`_stable_gw` in `realdata/`), identical everywhere, with `--gw-seed` available
to vary it deliberately.

**What this changed, measured on both datasets:** nothing that the paper reports. Detection
counts, precision, recall, the `port_scan`/`ddos_flood`/`c2_beacon` breakdown, and the
P3.2/P3.6 firing outcomes are unchanged; only the raw `devices_per_gateway` split moves
(TON_IoT `{gw1: 5809, gw2: 5727}` under the old unseeded hash, `{gw1: 5773, gw2: 5763}`
under the stable one). The `expected/*.json` files here are regenerated with the stable
partition.

**What remains partition-sensitive:** the MonPoly *episode* counts for the cross-gateway
property P3.6, since it asks whether an alert appears at two *distinct* gateways. P3.2,
quantifying over devices, does not depend on the partition. Both are counted by
`realdata/episodes.py`; the values for the shipped partition are recorded in
`realdata/expected/EPISODES.md`. P3.6 asks whether an alert appears at two distinct gateways inside one
window, so how many episodes it yields depends on which identities land where. Treat those
counts as specific to the shipped partition (`--gw-seed 0`), and vary `--gw-seed` to check
that a conclusion is not an artefact of one split.

## Unseeded by design

`fabric/device_publisher.py` seeds from `os.urandom(16)`, so the Q4 detection workload is
deliberately randomised per run; its figures are means over five runs, not a fixed trace.

## Needs services

`exp_baseline.py` and `run_crash_exp.sh` read from `/exp` and are container-only. The
throughput, latency, crash and retention experiments need Mosquitto and NATS JetStream;
`exp_oracle.py` needs the `rvhier` image for MonPoly.

## Running the RTLola specification

`rtlola_specs/silent_node.lola` (P3.5) needs `rtlola-cli`, which the `rvhier` image
provides. It can also be run standalone:

```sh
docker build -t rtlola:cli - <<'DOCKER'
FROM rust:1-slim
RUN cargo install rtlola-cli --locked
ENTRYPOINT []
DOCKER

rtlola-cli analyze silent_node.lola
rtlola-cli monitor silent_node.lola --offline relative --csv-in <trace.csv>
```

The trace is a CSV with a `time` column and one column per input stream
(`safe_tx,overflow,time_anomaly,fuzzing`), each row naming the device that emitted it.
On a trace where `d1` reports until t=10 and then stops while `d2` keeps reporting, the
specification first triggers for `d1` at t=18, i.e. after the full `8s` window, and never
triggers for `d2`.
