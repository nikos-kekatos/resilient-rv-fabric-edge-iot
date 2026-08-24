# Real cross-gateway clock-skew validation

The paper's skew result is measured on **two separate cloud VMs** with independent kernel
clocks over a real network, NTP disabled on gw2 so its clock diverges genuinely — the
harness in `run_do_experiment.sh` (see `DISTRIBUTED.md`, Option B). In every harness here
the skew enters through the **running system's clocks**, so it propagates through the
gateway sidecar (`gateway.py:195`, `gw_ts_s = int(time.time())`) and into the P3.6
cross-gateway correlation exactly as a real deployment would see it.

The offset-injection sweep over *recorded* TON\_IoT timestamps
(`EXPERIMENTS_RESULTS.md` §3e) is a separate, supplementary check — not the source of the
paper's number.

## What is measured
**Preferred: `measure_eps.py` (dense clock beacons).** Each gateway is mirrored by a
`clock_beacon.py` process running under the *same* faketime spec, publishing its clock
on `clock.<id>` twice a second. The collector records host receipt time and estimates
`offset = median(beacon_clock - host_recv)` per mirror; `eps = offset(gw2)-offset(gw1)`
cancels the shared Docker-VM base clock, so a **zero-offset control reads eps ~ 0**
(measured $0.0$\,s, 3\,ms jitter) and injected offsets are recovered faithfully. Use
this for the quantitative result.

**Legacy: `measure_skew.py`** (subscribes to `gw.*.verdict`) estimates offsets from
sparse alert events; its control reads ~13\,s (confounded by alert timing), so it is
kept only for the ordering-inversion count, not for eps. It reports: 
- **`eps_cross_gateway_s`** — the realised offset between the two gateways' clocks;
- **`p36_robust` / `p36_margin_s`** — whether `eps` stays below the P3.6 window `W`
  (default 30 s), i.e. whether both overflow episodes still land in one window;
- **`cross_gateway_ordering_inversions`** — residual cross-gateway order flips (G2
  guarantees per-*device* monotonicity, not a global cross-gateway order, so this
  quantifies exactly what is and is not preserved under skew).

## Option 1 — libfaketime (cheap, one host, real syscall-level skew)
Each gateway runs under `faketime`, which intercepts `time.time()` so gateway2 sees a
genuinely offset/drifting clock. Needs Docker.
```
./run_skew_faketime.sh          # sweeps +0,+1,+2,+5,+10 s and a 5% drift; writes skew_results.csv
```
`FT_SPEC` syntax (libfaketime `-f`): `"+2 x1.0"` = +2 s fixed offset; `"+0 x1.05"` =
clock runs 5% fast (offset grows with runtime -> real drift).

## Option 2 — two local VMs with Lima (free, real separate kernels)
`two_vm_skew.sh` boots two Lima VMs, disables NTP on the second and steps its clock by
`SKEW_S` seconds, runs one gateway per VM against a broker on the host, and measures the
realised `eps`. Real independent kernels, but both on one machine.
```
brew install lima
nats-server -js -p 4222 &  mosquitto -p 1883 &
SKEW_S=7 ./two_vm_skew.sh
```

## Option 3 — two cloud VMs (what the paper reports)
`run_do_experiment.sh` provisions two DigitalOcean droplets, measures the *real*
cross-host `eps` with the beacon probe, sweeps the injected offset (`0 5 10 20 35` s) and,
with `FULL=1`, drives P3.6 through the real MonPoly engine on the two-host stream. It
**destroys both droplets and the temporary SSH key on exit**, so nothing is left billing —
and consequently nothing survives the run to be checked in here.
`run_do_clean.sh` is the same idea with per-host bootstrap scripts, and also exercises P3.7
under a real network partition.
```
doctl account get            # must already be authenticated
bash run_do_experiment.sh    # FULL=1 uses s-2vcpu-4gb: MonPoly's opam build OOMs a 1GB box
```
Independent kernels, a real network and genuinely divergent clocks: this is the faithful
setup for the headline P3.6/P3.7 claim. See `DISTRIBUTED.md` for the full walkthrough, and
`EXPECTED.md` for the reported ε sweep to diff your run against.

## Reporting
Put `eps` vs `FT_SPEC` (and the P3.6 margin) in a table/plot, and state that P3.6
tolerates skew up to `eps < W`; beyond that, either widen `W` or add per-gateway clock
disciplining. What makes this a measured resilience bound rather than a synthetic one is
that `eps` is observed from the running clocks, not imposed on recorded timestamps.
