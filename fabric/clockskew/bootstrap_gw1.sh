#!/usr/bin/env bash
# Runs ON gw1 (launched once via: nohup bash bootstrap_gw1.sh &). Because this is a
# real local shell on the droplet, setsid+& backgrounding is reliable (unlike inline
# ssh compound commands). Starts brokers + reference gateway + beacon + device.
cd /root/impl || exit 1
systemctl stop mosquitto 2>/dev/null
pkill -9 -f nats-server; pkill -9 -f "mosquitto -p"; pkill -9 -f gateway.py; pkill -9 -f clock_beacon; pkill -9 -f device_publisher
sleep 1
setsid nats-server -js -p 4222 </dev/null >/tmp/nats.log 2>&1 &
# mosquitto 2.x binds localhost-only without a config -> bind 0.0.0.0 so gw2 can reach it
printf 'listener 1883 0.0.0.0\nallow_anonymous true\n' > /tmp/mosq.conf
setsid mosquitto -c /tmp/mosq.conf </dev/null >/tmp/mosq.log 2>&1 &
for i in $(seq 30); do python3 -c 'import socket;socket.create_connection(("127.0.0.1",4222),2)' 2>/dev/null && break; sleep 1; done
NATS_URL=nats://127.0.0.1:4222 setsid python3 gateway.py --id gw1 --mqtt 127.0.0.1 --nats nats://127.0.0.1:4222 </dev/null >/tmp/gw.log 2>&1 &
setsid python3 clockskew/clock_beacon.py --id gw1 --nats nats://127.0.0.1:4222 </dev/null >/tmp/beacon.log 2>&1 &
setsid python3 device_publisher.py --id node-1-clean --gw gw1 --broker 127.0.0.1 --profile normal </dev/null >/tmp/dev.log 2>&1 &
sleep 3
echo "nats=$(pgrep -c nats-server) mosq=$(pgrep -fc 'mosquitto -p') gw=$(pgrep -fc gateway.py) beacon=$(pgrep -fc clock_beacon) dev=$(pgrep -fc device_publisher)" > /root/boot_done
