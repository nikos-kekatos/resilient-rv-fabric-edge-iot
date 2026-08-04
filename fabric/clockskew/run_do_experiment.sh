#!/usr/bin/env bash
# One-shot DigitalOcean distributed clock-skew experiment for RV-Fabric.
#
# Creates two droplets (gw1 = brokers+reference gateway, gw2 = skewed gateway on a
# genuinely different kernel clock over a real network), measures the real
# cross-host epsilon with the beacon probe, sweeps the injected offset, and then
# DESTROYS both droplets and the temp SSH key (billing-safe via an EXIT trap).
#
# Run once from your authenticated session:
#     ! bash paper_cloudnet/rv-fabric-impl/clockskew/run_do_experiment.sh
#
# Requires: doctl already authenticated (doctl account get works in your shell).
set -uo pipefail

IMPL="$(cd "$(dirname "$0")/.." && pwd)"      # .../rv-fabric-impl
REGION="${REGION:-fra1}"
# FULL (#2) builds MonPoly (opam/dune), which OOMs a 1GB box -> use 4GB there.
if [ "${FULL:-1}" = "1" ]; then SIZE="${SIZE:-s-2vcpu-4gb}"; else SIZE="${SIZE:-s-1vcpu-1gb}"; fi
IMG="${IMG:-ubuntu-24-04-x64}"
NATS_VER="${NATS_VER:-v2.10.22}"
KEYFILE="/tmp/rvfab_do_key"
KEYNAME="rvfab-tmp-$$"
SKEWS="${SKEWS:-0 5 10 20 35}"                 # gw2 offset sweep (seconds)
GW1_ID=""; GW2_ID=""; FP=""

cleanup() {
  echo; echo "=== TEARDOWN (destroying droplets + key so billing stops) ==="
  # delete by NAME (robust even if ID capture failed on a partial create)
  for id in $(doctl compute droplet list --format ID,Name --no-header 2>/dev/null \
                | awk '$2=="rvfab-gw1"||$2=="rvfab-gw2"{print $1}'); do
    doctl compute droplet delete "$id" -f 2>/dev/null && echo "deleted droplet $id"
  done
  [ -n "$FP" ] && doctl compute ssh-key delete "$FP" -f 2>/dev/null && echo "deleted ssh key"
  rm -f "$KEYFILE" "$KEYFILE.pub"
  echo "teardown done."
}
trap cleanup EXIT

doctl account get >/dev/null || { echo "doctl not authenticated"; exit 1; }

echo "=== 1. temp SSH key ==="
rm -f "$KEYFILE" "$KEYFILE.pub"
ssh-keygen -t ed25519 -N "" -f "$KEYFILE" -q
FP=$(doctl compute ssh-key import "$KEYNAME" --public-key-file "$KEYFILE.pub" \
       --format FingerPrint --no-header)
echo "key fingerprint: $FP"

echo "=== 2. create two droplets ($SIZE, $REGION) ==="
VPC="${VPC:-$(doctl vpcs list --format Region,ID,Default --no-header 2>/dev/null | awk -v r="$REGION" '$1==r && $3=="true"{print $2}' | head -1)}"
[ -n "$VPC" ] && VPCARG="--vpc-uuid $VPC" || VPCARG=""
echo "using VPC: ${VPC:-<region default>}"
doctl compute droplet create rvfab-gw1 rvfab-gw2 \
  --region "$REGION" --size "$SIZE" --image "$IMG" --ssh-keys "$FP" $VPCARG --wait \
  --format ID,Name,PublicIPv4,PrivateIPv4 --no-header | tee /tmp/rvfab_dr.txt
GW1_ID=$(awk '/rvfab-gw1/{print $1}' /tmp/rvfab_dr.txt)
GW2_ID=$(awk '/rvfab-gw2/{print $1}' /tmp/rvfab_dr.txt)
GW1_PUB=$(awk '/rvfab-gw1/{print $3}' /tmp/rvfab_dr.txt)
GW1_PRIV=$(awk '/rvfab-gw1/{print $4}' /tmp/rvfab_dr.txt)
GW2_PUB=$(awk '/rvfab-gw2/{print $3}' /tmp/rvfab_dr.txt)
echo "gw1 pub=$GW1_PUB priv=$GW1_PRIV ; gw2 pub=$GW2_PUB"
if [ -z "$GW1_PUB" ] || [ -z "$GW2_PUB" ] || [ -z "$GW1_PRIV" ]; then
  echo "ERROR: droplet creation incomplete; aborting (teardown will run)"; exit 1
fi

SSH="ssh -i $KEYFILE -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=10"
SCP="scp -i $KEYFILE -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -q -r"

echo "=== 3. wait for SSH ==="
for H in "$GW1_PUB" "$GW2_PUB"; do
  for i in $(seq 60); do $SSH root@"$H" true 2>/dev/null && break; sleep 5; done
done

echo "=== 4. provision both ==="
for H in "$GW1_PUB" "$GW2_PUB"; do
  $SCP "$IMPL" root@"$H":/root/impl
  $SSH root@"$H" "export DEBIAN_FRONTEND=noninteractive; apt-get -o DPkg::Lock::Timeout=300 update -q >/dev/null 2>&1; \
    apt-get -o DPkg::Lock::Timeout=300 install -y -q python3-pip mosquitto curl >/dev/null 2>&1; \
    pip3 install -q --break-system-packages 'paho-mqtt>=2.0' 'nats-py>=2.6' >/dev/null 2>&1; \
    curl -sL https://github.com/nats-io/nats-server/releases/download/${NATS_VER}/nats-server-${NATS_VER}-linux-amd64.tar.gz | tar xz -C /tmp && mv /tmp/nats-server-*/nats-server /usr/local/bin/ 2>/dev/null; \
    echo provisioned \$(hostname)"
done

if [ "${FULL:-1}" = "1" ]; then
  echo "=== 4b. [FULL] kick off monpoly build on gw1 (overlaps with the eps phase) ==="
  $SSH root@"$GW1_PUB" "rm -f /tmp/monpoly_done; nohup bash -c '\
    export DEBIAN_FRONTEND=noninteractive; \
    apt-get -o DPkg::Lock::Timeout=300 install -y -q ocaml opam m4 libgmp-dev pkg-config git >/tmp/mpbuild.log 2>&1; \
    opam init --disable-sandboxing -y >>/tmp/mpbuild.log 2>&1; \
    eval \$(opam env); \
    opam install -y dune menhir zarith re dune-build-info >>/tmp/mpbuild.log 2>&1; \
    git clone https://bitbucket.org/monpoly/monpoly.git /tmp/monpoly >>/tmp/mpbuild.log 2>&1; \
    cd /tmp/monpoly && dune build >>/tmp/mpbuild.log 2>&1 && cp _build/default/src/main.exe /usr/local/bin/monpoly; \
    touch /tmp/monpoly_done' >/tmp/mpbuild_outer.log 2>&1 & echo monpoly-build-started"
fi

echo "=== 5. start brokers + reference gateway on gw1 ==="
# NOTE: brokers must be setsid+</dev/null-detached, else they get SIGHUP'd on ssh close.
$SSH root@"$GW1_PUB" "systemctl stop mosquitto 2>/dev/null; pkill -9 -f nats-server; pkill -9 -f 'mosquitto -p'; sleep 1; \
  setsid nats-server -js -p 4222 </dev/null >/tmp/nats.log 2>&1 & \
  setsid mosquitto -p 1883 </dev/null >/tmp/mosq.log 2>&1 & \
  for i in \$(seq 30); do python3 -c 'import socket;socket.create_connection((\"127.0.0.1\",4222),2)' 2>/dev/null && break; sleep 1; done; \
  cd /root/impl && NATS_URL=nats://127.0.0.1:4222 setsid python3 gateway.py --id gw1 --mqtt 127.0.0.1 --nats nats://127.0.0.1:4222 </dev/null >/tmp/gw.log 2>&1 & \
  setsid python3 clockskew/clock_beacon.py --id gw1 --nats nats://127.0.0.1:4222 </dev/null >/tmp/beacon.log 2>&1 & \
  setsid python3 device_publisher.py --id node-6-overflow --gw gw1 --broker 127.0.0.1 --profile overflow </dev/null >/tmp/dev.log 2>&1 & \
  sleep 2; echo gw1-up"

echo "=== 6. start skewed gateway + beacon on gw2 (dials gw1 private IP $GW1_PRIV) ==="
$SSH root@"$GW2_PUB" "timedatectl set-ntp false 2>/dev/null; \
  cd /root/impl && NATS_URL=nats://$GW1_PRIV:4222 setsid python3 gateway.py --id gw2 --mqtt $GW1_PRIV --nats nats://$GW1_PRIV:4222 </dev/null >/tmp/gw.log 2>&1 & \
  setsid python3 clockskew/clock_beacon.py --id gw2 --nats nats://$GW1_PRIV:4222 </dev/null >/tmp/beacon.log 2>&1 & \
  setsid python3 device_publisher.py --id node-16-overflow --gw gw2 --broker $GW1_PRIV --profile overflow </dev/null >/tmp/dev.log 2>&1 & \
  sleep 2; echo gw2-up"
sleep 10   # let both beacons connect and stream

echo "=== 7. sweep gw2 clock offset (delta steps, live beacon), measure REAL cross-host epsilon ==="
echo "offset_s measured_eps_s p36_robust margin_s" | tee /tmp/rvfab_eps.txt
PREV=0
for N in $SKEWS; do
  D=$((N - PREV)); PREV=$N
  # step gw2's real kernel clock by the delta; the running beacon reports it live (no restart)
  [ "$D" -ne 0 ] && $SSH root@"$GW2_PUB" "date -s \"@\$(( \$(date +%s) + $D ))\" >/dev/null" 2>/dev/null
  sleep 4
  R=$($SSH root@"$GW1_PUB" "cd /root/impl && NATS_URL=nats://127.0.0.1:4222 python3 clockskew/measure_eps.py --secs 12 --window 30 2>/dev/null")
  LINE=$(echo "$R" | python3 -c "import sys,json;d=json.load(sys.stdin);print($N, d['eps_cross_gateway_s'], d['p36_robust'], d['p36_margin_s'])" 2>/dev/null || echo "$N ERR ($(echo "$R" | tr -d '\n' | cut -c1-60))")
  echo "$LINE" | tee -a /tmp/rvfab_eps.txt
done
# reset gw2 clock back toward reference
$SSH root@"$GW2_PUB" "date -s \"@\$(( \$(date +%s) - $PREV ))\" >/dev/null" 2>/dev/null

echo; echo "=== RESULT #1 (real two-host clock skew, epsilon) ==="
cat /tmp/rvfab_eps.txt

if [ "${PARTITION:-1}" = "1" ]; then
  echo; echo "=== 7b. [PARTITION] P3.7 gateway-silence under a REAL network partition ==="
  # backend-resident tick + silence detector on gw1 (no MonPoly), 50s window
  $SSH root@"$GW1_PUB" "cd /root/impl && NATS_URL=nats://127.0.0.1:4222 setsid python3 clockskew/gw_silence.py --t 6 --secs 50 </dev/null >/tmp/p37.log 2>&1 & echo detector-up"
  sleep 10                                     # both gateways active, no silence yet
  echo "--- partitioning gw2 from gw1 (iptables DROP) at ~t=10s ---"
  $SSH root@"$GW2_PUB" "iptables -A OUTPUT -d $GW1_PRIV -j DROP; echo partitioned"
  sleep 18                                     # gw2 stream vanishes; P3.7 should fire
  echo "--- healing partition at ~t=28s ---"
  $SSH root@"$GW2_PUB" "iptables -D OUTPUT -d $GW1_PRIV -j DROP; echo healed"
  sleep 16                                     # gw2 reconnects, outbox replays, re-arm
  echo "--- P3.7 detector result ---"
  $SSH root@"$GW1_PUB" "tail -20 /tmp/p37.log" | tee /tmp/rvfab_p37.txt
fi

if [ "${FULL:-1}" = "1" ]; then
  echo; echo "=== 8. [FULL] P3.6 firing through the real MonPoly engine on the two-host stream ==="
  echo "waiting for monpoly build on gw1 (started during provisioning)..."
  $SSH root@"$GW1_PUB" "for i in \$(seq 180); do [ -f /tmp/monpoly_done ] && break; sleep 5; done; \
    if command -v monpoly >/dev/null 2>&1; then echo monpoly-ready; else echo monpoly-MISSING; tail -3 /tmp/mpbuild.log; fi"
  echo "--- running live cross-gateway collector (40s, both overflow devices active) ---"
  $SSH root@"$GW1_PUB" "cd /root/impl && export PATH=\$PATH:/usr/local/bin && NATS_URL=nats://127.0.0.1:4222 python3 clockskew/crossgw_monpoly.py --secs 40 2>/dev/null" | tee /tmp/rvfab_p36.txt
  echo "=== RESULT #2 (distributed P3.6 firing) ==="; cat /tmp/rvfab_p36.txt
fi

echo; echo "=== copy the RESULT block(s) above back into the chat ==="
