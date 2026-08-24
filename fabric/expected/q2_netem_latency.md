# Q2 — per-hop latency under an emulated WAN (recorded output)

`bench_wan.py` measures publish→subscriber-receive latency on each hop: MQTT QoS 1
(device→gateway) and NATS JetStream durable (gateway→backend). Throughput is deliberately
not measured here — under emulated delay the serial JetStream publish-ack would make a 20k
burst take minutes.

Recorded 2026-08-24, Apple M4 Pro / Docker Desktop, `LAT_N=800`, `PACE=0.004`.

## Where the delay is applied — read this before comparing digits

`netem` shapes **egress only**, so the round trip depends on how many interfaces carry the
qdisc. Two setups give the same physics but different numbers for the same nominal δ:

- **README form** — `tc qdisc add dev <if> root netem delay δ` on one interface: one
  direction delayed, **RTT ≈ δ**. This is what the paper's figures use.
- **This run** — δ on both broker egress *and* client egress, i.e. every crossing delayed:
  **RTT ≈ 2δ**.

So this run's δ corresponds to the paper's 2δ, and the two agree closely:

| | this run δ=5 | paper δ=10 | this run δ=10 | paper δ=20 |
|---|---:|---:|---:|---:|
| MQTT QoS 1 mean | **32.08** ms | 32 ms | **62.36** ms | 60 ms |
| NATS JetStream mean | **13.97** ms | 13 ms | **24.69** ms | 23 ms |

## Full measurements

```
baseline (no netem)
  MQTT_QoS1 n=800 mean=0.47  p50=0.43  p95=0.91  p99=1.67  ms
  NATS_JS   n=800 mean=0.81  p50=0.75  p95=1.42  p99=2.31  ms

δ=5ms symmetric (RTT ≈ 10ms)
  MQTT_QoS1 n=800 mean=32.08 p50=31.89 p95=37.83 p99=39.50 ms
  NATS_JS   n=800 mean=13.97 p50=13.88 p95=15.53 p99=17.73 ms

δ=10ms symmetric (RTT ≈ 20ms)
  MQTT_QoS1 n=800 mean=62.36 p50=62.50 p95=74.70 p99=78.03 ms
  NATS_JS   n=800 mean=24.69 p50=24.70 p95=26.56 p99=27.88 ms

δ=20ms symmetric (RTT ≈ 40ms)   <-- offered load exceeds the link; see below
  MQTT_QoS1 n=800 mean=426.21 p50=422.62 p95=737.68 p99=786.34 ms
  NATS_JS   n=800 mean=45.75  p50=45.77  p95=48.63  p99=49.59  ms
```

## What reproduces

**The ratio, not the milliseconds.** Normalised per round trip:

- **MQTT QoS 1 ≈ 3.2 × RTT** — the acknowledged handshake crosses the link about three
  times (publish, PUBACK, then broker→subscriber delivery), which is the paper's point.
- **NATS JetStream ≈ 1.4 × RTT** — roughly a single crossing, because the durable publish
  costs one acknowledged round trip and delivery rides the existing consumer connection.

The paper's `p95 < 75 ms` bound also holds: at RTT ≈ 20 ms (this run's δ=10, the paper's
δ=20) p95 is 74.70 ms.

**The δ=20 MQTT row is a saturation artefact, not a latency measurement.** `PACE=0.004`
offers 250 msg/s; once RTT reaches 40 ms the QoS 1 in-flight window cannot retire that fast,
so the queue builds and the mean (426 ms) reflects queueing, not propagation. JetStream at the
same δ stays at 45.75 ms because its publish path is paced by its own acks. If you sweep past
RTT ≈ 20 ms, raise `PACE` accordingly or the MQTT arm measures your backlog.

## Reproducing

`Dockerfile.bench` provides `tc` (iproute2) and both probes, and needs `--cap-add NET_ADMIN`
to apply the qdisc. The broker images carry no `tc`, so the delay is applied to their network
namespaces from a sidecar:

```sh
docker network create wanexp
docker run -d --name wan-mqtt --network wanexp eclipse-mosquitto:2 \
  sh -c 'printf "listener 1883\nallow_anonymous true\n" > /m.conf; exec mosquitto -c /m.conf'
docker run -d --name wan-nats --network wanexp nats:2 -js
docker build -t wanbench -f Dockerfile.bench .

D=5
for c in wan-mqtt wan-nats; do
  docker run --rm --net "container:$c" --cap-add NET_ADMIN wanbench \
    "tc qdisc replace dev eth0 root netem delay ${D}ms"
done
docker run --rm --network wanexp --cap-add NET_ADMIN \
  -e MQTT_HOST=wan-mqtt -e NATS_URL=nats://wan-nats:4222 wanbench \
  "tc qdisc replace dev eth0 root netem delay ${D}ms; python3 /bench_wan.py"
```

For the paper's one-sided form, apply the qdisc only on the client and read δ as the RTT.
