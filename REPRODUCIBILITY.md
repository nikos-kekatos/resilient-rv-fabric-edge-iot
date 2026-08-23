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
property P3.6. P3.6 asks whether an alert appears at two distinct gateways inside one
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
