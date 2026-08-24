# Expected output — two-host clock-skew of P3.6

The harnesses here provision hosts and then **destroy them on exit**, so no run artefact can
be checked in. This file records the reported result so a re-run has something to diff
against. Source of record: the paper, §7, *Clock-skew resilience of P3.6 (Q4, two independent
hosts)*.

## Setup that produced it

Two separate cloud VMs (1 vCPU, Ubuntu 24.04), independent kernels, a real network, **NTP
disabled on gw2** so its clock diverges genuinely. A per-gateway `clock_beacon.py` measures
the realised offset ε against a backend reference. This is the MINIMAL path of
`DISTRIBUTED.md` — ε and ordering only, no MonPoly on the box — which is why 1 vCPU suffices;
`FULL=1` in `run_do_experiment.sh` needs `s-2vcpu-4gb` because it builds MonPoly via opam.

## Result

| gw2 injected offset | measured ε | P3.6 (`≥2 gateways within W=30 s`) |
|---:|---:|:--|
| 0 s (control) | **0.001 s** | sound |
| 5 s | **4.75 s** | sound |
| 10 s | **9.71 s** | sound |
| 20 s | **19.64 s** | sound |
| 35 s | **33.76 s** | flips (ε > W) |

**What to check on a re-run.** Two things, neither of which is a digit:

1. **ε tracks the injected offset to within network jitter.** The absolute residuals depend
   on the network between your hosts, so expect your own numbers; what must hold is that ε
   follows the injection rather than sitting near zero (which would mean the skew never
   reached the sidecar) or drifting arbitrarily.
2. **P3.6 stays sound while ε < W and flips once ε exceeds it.** This is the claim. It is a
   property of a windowed operator, not of a particular host, so it should reproduce
   anywhere — including on the two cloud-free harnesses (`two_vm_skew.sh` on Lima,
   `run_skew_faketime.sh` on one host).

The zero-skew control matters: it reads ε ≈ 0 (0.001 s), which is what rules out the
measurement itself manufacturing an offset. `measure_skew.py`'s control instead reads ~13 s
because it infers offsets from sparse alert timings — use `measure_eps.py` for ε, and keep
`measure_skew.py` only for the ordering-inversion count.

## Not this

Do not confuse the above with `EXPERIMENTS_RESULTS.md` §3e, which sweeps ε over *recorded*
TON_IoT timestamps (firings 1159/1132/1122/1118/1111/1049 at ε = 0/5/15/30/60/120 s). That is
a supplementary robustness check on real data, not the two-host measurement the paper reports.
