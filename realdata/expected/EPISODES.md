# Episode counts (real MonPoly, shipped `--gw-seed 0` partition)

MonPoly reports a satisfaction at every timepoint the formula holds, so one campaign
yields a run of firings for as long as its events stay inside the property window.
`episodes.py` collapses a run into one episode when consecutive satisfactions are no
more than the property's own window (30 s) apart.

| Dataset | Property | Firings | Episodes | Note |
|---|---|---:|---:|---|
| WUSTL-IIoT-2021 | P3.2 coordinated | 1499 | **17** | max 4 coordinated sources |
| WUSTL-IIoT-2021 | P3.6 cross-gateway | 732 | **1** | *continuously* satisfied: 24.9 min span, no gap > 7 s |
| TON_IoT | P3.2 coordinated | 27 | **11** | max 3 coordinated sources |
| TON_IoT | P3.6 cross-gateway | 522 | **168** | |

WUSTL's P3.6 collapsing to a single episode is not a weak result: the condition holds
without a break for the whole trace, so it is one sustained episode rather than many
short ones.

Reproduce:

```sh
python3 wustl_water_rv.py --csv <wustl csv> --gateways 2     # or ton_iot_monpoly.py
docker run --rm -v "$PWD":/w \
  -v "$PWD/../fabric/monpoly_specs":/app/monpoly_specs:ro \
  -v "$PWD/../fabric/crossgw_specs":/app/crossgw_specs:ro \
  rvhier:latest bash -c \
  "monpoly -sig /app/crossgw_specs/crossgw.sig \
           -formula /app/crossgw_specs/p3_6_crossgw.mfotl -log /w/wustl_water_p36.log" > p36.out
python3 episodes.py --out p36.out --window 30
```

P3.2 counts are independent of the gateway partition; P3.6 counts are not, because the
property asks whether an alert appears at two *distinct* gateways.

**Note on the L3 feed.** Both datasets feed the fleet properties from what the flow
monitors flagged, with no reference to the ground-truth labels. An earlier version of
`wustl_water_rv.py` intersected the feed with the label set, which both leaked labels into
the correlation stream and made the two datasets run different pipelines; per-identity
detection metrics are unaffected by the change (precision/recall stay 0.833/0.833), but the
P3.2 episode count rises from 2 to 17 because more flagged sources reach L3.
