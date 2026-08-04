# Running RV-Fabric on two real hosts (the missing distributed experiment)

Goal: move the clock-skew / P3.6 / P3.7 results from "emulated on one host" to
"measured across two machines with real, independent clocks and a real network."

Layout (3 roles; the broker role can share a machine with gw1):

    ┌── VM-A (gateway 1, REFERENCE clock, NTP on) ──┐
    │   gateway.py --id gw1   +  clock_beacon --id gw1
    └───────────────────────────────────────────────┘
    ┌── VM-B (gateway 2, SKEWED clock, NTP OFF) ────┐
    │   gateway.py --id gw2   +  clock_beacon --id gw2
    └───────────────────────────────────────────────┘
    ┌── BROKER host (can be VM-A or a 3rd box) ─────┐
    │   nats-server -js   +   mosquitto              │
    │   measure_eps.py    (+ optional backend_l3)    │
    └───────────────────────────────────────────────┘

Two levels of result:
  * MINIMAL (no MonPoly): real cross-gateway skew ε + ordering. Needs only
    nats/mosquitto + gateways + beacons. This fills Table `tab:skew` with a
    *genuinely distributed* ε instead of a single-host libfaketime one.
  * FULL (P3.6/P3.7 fire end-to-end): additionally run backend_l3 in the
    `rvhier` Docker image (it has MonPoly+RTLola) on the broker host.

================================================================================
OPTION A — two local VMs with Lima (free, on your Mac)
================================================================================
    brew install lima
    # brokers on the Mac host (reachable from the VMs):
    nats-server -js -p 4222 &         # JetStream
    mosquitto -p 1883 &
    HOST=$(ipconfig getifaddr en0)    # your Mac's LAN IP; VMs dial this

    # --- boot two VMs ---
    limactl start --name gw1 --cpus 1 --memory 1 template://ubuntu-lts
    limactl start --name gw2 --cpus 1 --memory 1 template://ubuntu-lts
    for V in gw1 gw2; do
      limactl copy -r "$(cd .. && pwd)" "$V:/tmp/impl"
      limactl shell "$V" sudo apt-get update -q
      limactl shell "$V" sudo apt-get install -y -q python3-pip
      limactl shell "$V" pip3 install -q "paho-mqtt>=2.0" "nats-py>=2.6"
    done

    # --- gw2: make its clock genuinely diverge (real, not injected) ---
    SKEW_S=8
    limactl shell gw2 sudo timedatectl set-ntp false
    limactl shell gw2 sudo date -s "@$(( $(date +%s) + SKEW_S ))"

    # --- run a gateway + a clock beacon on each VM ---
    for V in gw1 gw2; do
      limactl shell "$V" sh -c \
        "cd /tmp/impl && NATS_URL=nats://$HOST:4222 nohup python3 gateway.py \
           --id $V --mqtt $HOST --nats nats://$HOST:4222 >/tmp/gw.log 2>&1 &"
      limactl shell "$V" sh -c \
        "cd /tmp/impl && nohup python3 clockskew/clock_beacon.py \
           --id $V --nats nats://$HOST:4222 >/tmp/beacon.log 2>&1 &"
    done

    # --- drive one overflow device per gateway from the Mac ---
    python3 ../device_publisher.py --id node-6-overflow  --gw gw1 --broker $HOST --profile overflow &
    python3 ../device_publisher.py --id node-16-overflow --gw gw2 --broker $HOST --profile overflow &

    # --- MEASURE the real cross-gateway skew ε (clean beacon probe) ---
    sleep 5
    NATS_URL=nats://localhost:4222 python3 measure_eps.py --secs 30 --window 30

    # teardown
    limactl stop gw1 gw2 && limactl delete gw1 gw2

Sweep the skew by re-stepping gw2's clock (`sudo date -s ...`) to a few offsets
(e.g. 0, 5, 10, 20, 35 s) and re-running measure_eps.py, exactly as the single-host
sweep did, but now the offset is a *real* divergence between two kernels.

================================================================================
OPTION B — two cloud instances (AWS / GCP / DigitalOcean)
================================================================================
Provision 2 small Linux VMs (e.g. AWS t3.small, GCP e2-small, or DO $6 droplet),
same region, in one VPC/security-group that allows TCP 4222 and 1883 between them.
Call them GW1 (also the broker host) and GW2.

On GW1 (broker + reference gateway):
    sudo apt-get update && sudo apt-get install -y python3-pip nats-server mosquitto
    pip3 install "paho-mqtt>=2.0" "nats-py>=2.6"
    nats-server -js -p 4222 &            #  ensure 0.0.0.0 bind (default)
    mosquitto -p 1883 &                  #  allow_anonymous true, listener 0.0.0.0
    scp -r ../ ubuntu@GW1:/tmp/impl      #  copy rv-fabric-impl (run from your Mac)
    cd /tmp/impl
    NATS_URL=nats://127.0.0.1:4222 python3 gateway.py --id gw1 --mqtt 127.0.0.1 --nats nats://127.0.0.1:4222 &
    python3 clockskew/clock_beacon.py --id gw1 --nats nats://127.0.0.1:4222 &

On GW2 (skewed gateway; GW1_IP = GW1's private IP):
    sudo apt-get update && sudo apt-get install -y python3-pip
    pip3 install "paho-mqtt>=2.0" "nats-py>=2.6"
    scp -r ../ ubuntu@GW2:/tmp/impl
    sudo timedatectl set-ntp false && sudo date -s "@$(( $(date +%s) + 8 ))"   # real +8s skew
    cd /tmp/impl
    NATS_URL=nats://GW1_IP:4222 python3 gateway.py --id gw2 --mqtt GW1_IP --nats nats://GW1_IP:4222 &
    python3 clockskew/clock_beacon.py --id gw2 --nats nats://GW1_IP:4222 &

Drive devices + measure (from GW1 or your Mac, pointing at GW1_IP):
    python3 device_publisher.py --id node-6-overflow  --gw gw1 --broker GW1_IP --profile overflow &
    python3 device_publisher.py --id node-16-overflow --gw gw2 --broker GW1_IP --profile overflow &
    NATS_URL=nats://GW1_IP:4222 python3 clockskew/measure_eps.py --secs 30 --window 30

For a REAL partition test, drop the link with a firewall rule on GW2:
    sudo iptables -A OUTPUT -d GW1_IP -j DROP      # partition
    sudo iptables -D OUTPUT -d GW1_IP -j DROP      # heal
and watch P3.7 (gateway silence) fire on the backend while the tick keeps pulsing.

================================================================================
FULL P3.6/P3.7 firing (adds MonPoly)
================================================================================
On the broker host, instead of just nats/mosquitto, run the backend in Docker:
    docker build -t rvhier:latest ../../../hierarchical-rv-rtlola   # once (heavy)
    docker run --rm --network host -e NATS_URL=nats://127.0.0.1:4222 \
      -v "$PWD/..":/exp -w /exp $(docker build -q -f Dockerfile.backend ..) python3 backend_l3.py
Then P3.6 (overflow on ≥2 gateways within 30s) and P3.7 (a gateway gone dark) fire
on the real two-host stream, under real clock skew and (optionally) a real partition.

================================================================================
What to report in the paper
================================================================================
Replace/augment Table `tab:skew`: the ε column is now the *measured* offset between
two real kernels (not libfaketime on one). Add a P3.7 row for the real-partition
run. State the host types (e.g. "2× AWS t3.small, same region, NTP disabled on gw2").
This closes the single-host limitation — the last load-bearing gap.
