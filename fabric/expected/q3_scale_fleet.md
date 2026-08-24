# Q3 — throughput and fleet scale (recorded output)

Absolute rates are host-dependent, so this is a *second* host rather than a golden file:
diff the **shape**, not the digits. Both scripts need only NATS JetStream
(`docker compose up -d nats`), no MonPoly/RTLola and no brokers beyond that.

Recorded 2026-08-24 on an Apple M4 Pro (24 GB) under Docker Desktop, `nats:2 -js`,
alongside the paper's own figures for comparison.

## `bench_scale.py` — saturation sweep

```
pubs recv thru_msg_s lat_mean_ms lat_p95_ms
1 1500 3602 0.31 0.40
2 3000 6560 0.35 0.45
4 6000 9745 0.54 0.67
8 12000 10874 222.46 363.05
16 24000 10976 808.44 1290.32
32 48000 10822 1941.57 3092.56
```

**Shape to check:** throughput climbs to a ceiling near **10.8–11.0K msg/s** and then stays
flat while latency grows without bound — the backend saturates rather than losing messages
(`recv` always equals the offered `pubs × 1500`). The paper's ~10.7K saturation figure sits
inside this plateau.

## `bench_fleet.py` — fleet emulation

```
devices sent recv loss% thru_msg_s lat_mean_ms p95_ms
250 5000 5000 0.00 513 20.94 29.73
500 10000 10000 0.00 1029 29.67 42.59
1000 20000 20000 0.00 2040 51.11 82.58
```

**Shape to check:** **0.00% loss at every fleet size**, throughput scaling linearly with
device count (~2 msg/s/device), p95 growing sub-linearly and staying well inside the 1 Hz
tick. Paper: 1997±20 msg/s and p95 121±6 ms at 1000 devices; this host was faster on p95
(82.6 ms) at the same zero loss.

## Note on the two "publish rate" numbers

`exp_retention.py` reports its own single-producer durable-publish rate, which is a
*different* measurement from the saturation sweep above and much more host-sensitive:
`EXPERIMENTS_RESULTS.md` §2 recorded ~10,658 msg/s, this host produced 4,141 msg/s. It only
feeds the `T_buffer = capacity / R_in` horizons, and the two Q7 loss counts (1000/2000
silently dropped vs 1000 surfaced errors) are exact and host-independent.
