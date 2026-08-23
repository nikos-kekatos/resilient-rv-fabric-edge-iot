# Episode counts (real MonPoly, shipped `--gw-seed 0` partition)

MonPoly reports a satisfaction at every timepoint the formula holds, so one campaign
yields a run of firings for as long as its events stay inside the property window.
`episodes.py` collapses a run into a single episode when consecutive satisfactions are
no more than the property's own window (30 s) apart.

| Dataset | Property | Firings | Episodes |
|---|---|---:|---:|
| WUSTL-IIoT-2021 | P3.2 coordinated | 6 | **2** |
| WUSTL-IIoT-2021 | P3.6 cross-gateway | 582 | **1** |
| TON_IoT | P3.2 coordinated | 27 | **11** |
| TON_IoT | P3.6 cross-gateway | 522 | **168** |

Reproduce:

```sh
python3 ton_iot_monpoly.py --csv <ton_iot csv> --gateways 2
docker run --rm -v "$PWD":/w \
  -v "$PWD/../fabric/monpoly_specs":/app/monpoly_specs:ro \
  -v "$PWD/../fabric/crossgw_specs":/app/crossgw_specs:ro \
  rvhier:latest bash -c \
  "monpoly -sig /app/crossgw_specs/crossgw.sig \
           -formula /app/crossgw_specs/p3_6_crossgw.mfotl -log /w/ton_p36.log" > p36.out
python3 episodes.py --out p36.out --window 30
```

P3.2 counts are independent of the gateway partition; P3.6 counts are not, because the
property asks whether an alert appears at two *distinct* gateways.
