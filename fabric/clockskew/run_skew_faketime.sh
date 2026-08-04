#!/usr/bin/env bash
# Sweep a set of real cross-gateway clock offsets/drifts (libfaketime), and for each
# measure the realised eps and whether P3.6 stays robust. Produces skew_results.csv.
#
# Requires: Docker + docker compose. Run from this directory.
#   ./run_skew_faketime.sh
set -euo pipefail
cd "$(dirname "$0")"

OUT=skew_results.csv
echo "ft_spec,eps_s,p36_robust,p36_margin_s,cross_gw_inversions" > "$OUT"

# fixed offsets (seconds) and a drift case; extend as needed
SPECS=("+0 x1.0" "+1 x1.0" "+2 x1.0" "+5 x1.0" "+10 x1.0" "+0 x1.05")

for spec in "${SPECS[@]}"; do
  echo ">>> FT_SPEC=\"$spec\""
  FT_SPEC="$spec" docker compose -f docker-compose.skew.yml up -d --build
  sleep 8                                   # let gateways connect and drift accrue
  # measure from the host (needs nats-py); falls back to a one-off container if not.
  if python3 -c "import nats" 2>/dev/null; then
    res=$(NATS_URL=nats://localhost:4222 python3 measure_skew.py --secs 40 --window 30)
  else
    res=$(docker compose -f docker-compose.skew.yml run --rm --no-deps \
            -e NATS_URL=nats://nats:4222 gateway1 \
            python3 clockskew/measure_skew.py --secs 40 --window 30)
  fi
  echo "$res"
  eps=$(echo "$res"      | python3 -c "import sys,json;print(json.load(sys.stdin).get('eps_cross_gateway_s',''))")
  rob=$(echo "$res"      | python3 -c "import sys,json;print(json.load(sys.stdin).get('p36_robust',''))")
  mar=$(echo "$res"      | python3 -c "import sys,json;print(json.load(sys.stdin).get('p36_margin_s',''))")
  inv=$(echo "$res"      | python3 -c "import sys,json;print(json.load(sys.stdin).get('cross_gateway_ordering_inversions',''))")
  echo "\"$spec\",$eps,$rob,$mar,$inv" >> "$OUT"
  docker compose -f docker-compose.skew.yml down -v
done

echo "=== done -> $OUT ==="
cat "$OUT"
