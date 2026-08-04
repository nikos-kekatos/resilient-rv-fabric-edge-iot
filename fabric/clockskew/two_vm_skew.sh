#!/usr/bin/env bash
# Strongest option: two REAL VMs with separate kernels => separate real clocks and a
# real network between them. We disable NTP on the second VM and set a deliberate
# offset, run one gateway per VM against a broker on the host, and measure the
# realised eps with measure_skew.py. Uses Lima (macOS/Linux); adapt to multipass or
# two cloud instances trivially.
#
# Prereq:  brew install lima   (and nats-server+mosquitto on the host, already used
#          by the native experiments)
set -euo pipefail
cd "$(dirname "$0")"
IMPL_DIR="$(cd .. && pwd)"
HOST_IP="${HOST_IP:-$(ipconfig getifaddr en0 2>/dev/null || hostname -I | awk '{print $1}')}"

echo "host broker IP = $HOST_IP  (gateways will connect here)"
echo "start brokers on the host first:"
echo "  nats-server -js -p 4222 &   mosquitto -p 1883 &"

for VM in gw1 gw2; do
  limactl start --name "rvfab-$VM" --cpus 1 --memory 1 template://ubuntu-lts 2>/dev/null || true
  limactl copy -r "$IMPL_DIR" "rvfab-$VM:/tmp/impl"
  limactl shell "rvfab-$VM" sudo apt-get update -q
  limactl shell "rvfab-$VM" sudo apt-get install -y -q python3-pip
  limactl shell "rvfab-$VM" pip3 install -q "paho-mqtt>=2.0" "nats-py>=2.6"
done

# gw1: leave clock synchronised (reference).
# gw2: disable NTP and step the clock by a real offset (e.g. +7 s), so its time.time()
#      genuinely differs from gw1 -- no injection, a real unsynchronised host.
SKEW_S="${SKEW_S:-7}"
limactl shell rvfab-gw2 sudo timedatectl set-ntp false
limactl shell rvfab-gw2 sudo date -s "@$(( $(date +%s) + SKEW_S ))"

# run one gateway per VM against the host broker
limactl shell rvfab-gw1 sh -c \
  "cd /tmp/impl && NATS_URL=nats://$HOST_IP:4222 python3 gateway.py --id gw1 --mqtt $HOST_IP --nats nats://$HOST_IP:4222 &"
limactl shell rvfab-gw2 sh -c \
  "cd /tmp/impl && NATS_URL=nats://$HOST_IP:4222 python3 gateway.py --id gw2 --mqtt $HOST_IP --nats nats://$HOST_IP:4222 &"

# drive one overflow device per gateway from the host, then measure realised eps
python3 "$IMPL_DIR/device_publisher.py" --id node-6-overflow  --gw gw1 --broker "$HOST_IP" --profile overflow &
python3 "$IMPL_DIR/device_publisher.py" --id node-16-overflow --gw gw2 --broker "$HOST_IP" --profile overflow &
sleep 3
NATS_URL=nats://localhost:4222 python3 measure_skew.py --secs 40 --window 30

echo "teardown: limactl stop rvfab-gw1 rvfab-gw2 && limactl delete rvfab-gw1 rvfab-gw2"
