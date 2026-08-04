#!/usr/bin/env bash
# Runs ON gw2 (launched once via: nohup bash bootstrap_gw2.sh <GW1_PRIV_IP> &).
# Skewed gateway: disables NTP so its kernel clock can be stepped independently,
# then starts gateway + beacon + device dialing gw1's private broker.
GW1P="$1"
cd /root/impl || exit 1
timedatectl set-ntp false 2>/dev/null
pkill -9 -f gateway.py; pkill -9 -f clock_beacon; pkill -9 -f device_publisher
sleep 1
NATS_URL=nats://$GW1P:4222 setsid python3 gateway.py --id gw2 --mqtt "$GW1P" --nats nats://$GW1P:4222 </dev/null >/tmp/gw.log 2>&1 &
setsid python3 clockskew/clock_beacon.py --id gw2 --nats nats://$GW1P:4222 </dev/null >/tmp/beacon.log 2>&1 &
setsid python3 device_publisher.py --id node-2-clean --gw gw2 --broker "$GW1P" --profile normal </dev/null >/tmp/dev.log 2>&1 &
sleep 3
echo "gw=$(pgrep -fc gateway.py) beacon=$(pgrep -fc clock_beacon) dev=$(pgrep -fc device_publisher)" > /root/boot_done
