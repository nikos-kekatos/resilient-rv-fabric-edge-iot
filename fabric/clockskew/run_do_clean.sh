#!/usr/bin/env bash
# Clean two-host distributed run: fabric started via per-host bootstrap scripts
# (reliable local backgrounding), sweep/partition driven by simple single ssh calls.
# Measures real cross-host epsilon + P3.7 under a real partition, then DESTROYS all.
set -uo pipefail
IMPL="$(cd "$(dirname "$0")/.." && pwd)"
REGION="${REGION:-fra1}"; SIZE="${SIZE:-s-1vcpu-1gb}"; IMG="${IMG:-ubuntu-24-04-x64}"
NATS_VER="${NATS_VER:-v2.10.22}"; KEYFILE="/tmp/rvfab_do_key"; KEYNAME="rvfab-tmp-$$"
SKEWS="${SKEWS:-0 5 10 20 35}"; FP=""
SSHO="-i $KEYFILE -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=10"

cleanup() {
  echo; echo "=== TEARDOWN ==="
  for id in $(doctl compute droplet list --format ID,Name --no-header 2>/dev/null | awk '$2=="rvfab-gw1"||$2=="rvfab-gw2"{print $1}'); do
    doctl compute droplet delete "$id" -f 2>/dev/null && echo "deleted droplet $id"; done
  [ -n "$FP" ] && doctl compute ssh-key delete "$FP" -f 2>/dev/null && echo "deleted key"
  rm -f "$KEYFILE" "$KEYFILE.pub"; echo "teardown done."
}
trap cleanup EXIT
doctl account get >/dev/null || { echo "not authenticated"; exit 1; }

echo "=== 1. key + droplets ==="
rm -f "$KEYFILE" "$KEYFILE.pub"; ssh-keygen -t ed25519 -N "" -f "$KEYFILE" -q
FP=$(doctl compute ssh-key import "$KEYNAME" --public-key-file "$KEYFILE.pub" --format FingerPrint --no-header)
VPC=$(doctl vpcs list --format Region,ID,Default --no-header 2>/dev/null | awk -v r="$REGION" '$1==r&&$3=="true"{print $2}' | head -1)
doctl compute droplet create rvfab-gw1 rvfab-gw2 --region "$REGION" --size "$SIZE" --image "$IMG" \
  --ssh-keys "$FP" ${VPC:+--vpc-uuid $VPC} --wait --format Name,PublicIPv4,PrivateIPv4 --no-header | tee /tmp/rvfab_dr.txt
G1=$(awk '/rvfab-gw1/{print $2}' /tmp/rvfab_dr.txt); G1P=$(awk '/rvfab-gw1/{print $3}' /tmp/rvfab_dr.txt)
G2=$(awk '/rvfab-gw2/{print $2}' /tmp/rvfab_dr.txt)
echo "gw1=$G1 priv=$G1P gw2=$G2"
[ -z "$G1" ] || [ -z "$G2" ] || [ -z "$G1P" ] && { echo "create failed"; exit 1; }

echo "=== 2. wait ssh + provision ==="
for H in "$G1" "$G2"; do for i in $(seq 60); do ssh $SSHO root@"$H" true 2>/dev/null && break; sleep 5; done; done
for H in "$G1" "$G2"; do
  scp $SSHO -q -r "$IMPL" root@"$H":/root/impl
  ssh $SSHO root@"$H" "export DEBIAN_FRONTEND=noninteractive; apt-get -o DPkg::Lock::Timeout=300 update -q >/dev/null 2>&1; \
    apt-get -o DPkg::Lock::Timeout=300 install -y -q python3-pip mosquitto curl >/dev/null 2>&1; \
    pip3 install --break-system-packages -q 'paho-mqtt>=2.0' 'nats-py>=2.6' >/dev/null 2>&1; \
    curl -sL https://github.com/nats-io/nats-server/releases/download/${NATS_VER}/nats-server-${NATS_VER}-linux-amd64.tar.gz | tar xz -C /tmp && mv /tmp/nats-server-*/nats-server /usr/local/bin/ 2>/dev/null; \
    python3 -c 'import nats,paho.mqtt' && echo 'deps OK on '\$(hostname) || echo 'DEPS FAIL on '\$(hostname)"
done

echo "=== 3. launch fabric via bootstrap scripts (one nohup each) ==="
ssh $SSHO root@"$G1" "chmod +x /root/impl/clockskew/bootstrap_gw1.sh; nohup bash /root/impl/clockskew/bootstrap_gw1.sh >/root/boot.log 2>&1 & echo gw1-launched"
sleep 12   # gw1 brokers must be up before gw2 dials them
ssh $SSHO root@"$G2" "chmod +x /root/impl/clockskew/bootstrap_gw2.sh; nohup bash /root/impl/clockskew/bootstrap_gw2.sh $G1P >/root/boot.log 2>&1 & echo gw2-launched"
sleep 8
echo "gw1 boot_done: $(ssh $SSHO root@"$G1" 'cat /root/boot_done 2>/dev/null')"
echo "gw2 boot_done: $(ssh $SSHO root@"$G2" 'cat /root/boot_done 2>/dev/null')"

echo "=== 4. wait until measure_eps sees both gateways ==="
for i in $(seq 12); do
  n=$(ssh $SSHO root@"$G1" "cd /root/impl && NATS_URL=nats://127.0.0.1:4222 python3 clockskew/measure_eps.py --secs 5 --window 30 2>/dev/null" | python3 -c "import sys,json;print(len(json.load(sys.stdin).get('offsets_s',{})))" 2>/dev/null || echo 0)
  [ "$n" = "2" ] && { echo "both gateways streaming"; break; }
  echo "  not ready ($n/2), retry..."; sleep 4
done

echo "=== 5. RESULT #1: real cross-host epsilon sweep ==="
echo "offset_s eps_s p36_robust margin_s" | tee /tmp/rvfab_eps.txt
PREV=0
for N in $SKEWS; do
  D=$((N-PREV)); PREV=$N
  [ "$D" -ne 0 ] && ssh $SSHO root@"$G2" "date -s \"@\$(( \$(date +%s) + $D ))\" >/dev/null" 2>/dev/null
  sleep 4
  R=$(ssh $SSHO root@"$G1" "cd /root/impl && NATS_URL=nats://127.0.0.1:4222 python3 clockskew/measure_eps.py --secs 12 --window 30 2>/dev/null")
  echo "$R" | python3 -c "import sys,json;d=json.load(sys.stdin);print($N,d['eps_cross_gateway_s'],d['p36_robust'],d['p36_margin_s'])" 2>/dev/null | tee -a /tmp/rvfab_eps.txt \
    || echo "$N ERR" | tee -a /tmp/rvfab_eps.txt
done
ssh $SSHO root@"$G2" "date -s \"@\$(( \$(date +%s) - $PREV ))\" >/dev/null" 2>/dev/null
echo; echo "### EPS TABLE ###"; cat /tmp/rvfab_eps.txt

echo "=== 6. RESULT #2: P3.7 under a REAL network partition ==="
ssh $SSHO root@"$G1" "cd /root/impl && NATS_URL=nats://127.0.0.1:4222 nohup python3 clockskew/gw_silence.py --t 5 --secs 70 </dev/null >/tmp/p37.log 2>&1 & echo detector-up"
sleep 12
echo "  partitioning gw2 <-> gw1 (iptables DROP both directions) for 40s"
ssh $SSHO root@"$G2" "iptables -A OUTPUT -d $G1P -j DROP; iptables -A INPUT -s $G1P -j DROP; echo dropped; iptables -S | grep -c DROP"
# DIAGNOSTIC: during the partition, does gw1 still receive gw2 verdicts? (expect 0)
echo "  [diag] gw.gw2.verdict count at gw1 during partition (expect ~0):"
ssh $SSHO root@"$G1" "cd /root/impl && NATS_URL=nats://127.0.0.1:4222 timeout 12 python3 -c 'import asyncio,nats
async def m():
 nc=await nats.connect(\"nats://127.0.0.1:4222\"); c={\"gw1\":0,\"gw2\":0}
 async def cb(x):
  g=x.subject.split(\".\")[1]; c[g]=c.get(g,0)+1
 await nc.subscribe(\"gw.*.verdict\",cb=cb); await asyncio.sleep(10); await nc.drain(); print(\"verdicts during partition:\",c)
asyncio.run(m())' 2>&1 | tail -2"
sleep 22
echo "  healing"; ssh $SSHO root@"$G2" "iptables -D OUTPUT -d $G1P -j DROP; iptables -D INPUT -s $G1P -j DROP; echo healed"
sleep 18
echo "### P3.7 RESULT ###"; ssh $SSHO root@"$G1" "tail -10 /tmp/p37.log" | tee /tmp/rvfab_p37.txt
echo "  [diag] gw2 gateway log tail:"; ssh $SSHO root@"$G2" "tail -3 /tmp/gw.log 2>/dev/null"

echo; echo "=== DONE (copy the EPS TABLE and P3.7 RESULT above) ==="
