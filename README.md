# cannes-mqtt: an MQTT broker PoC on cannes

A proof of concept: an MQTT 3.1.1 broker built on the [cannes](../README.md) thread-per-core
actor engine, with io_uring for the network, benchmarked against Mosquitto 2.0.18 on the same
machine and the same load.

**Result in one line:** with one core each, cannes-mqtt delivers about **12x (fan-in) to 16x
(fan-out) more messages per second** than Mosquitto before saturating. At loads both brokers can
handle, CPU use is similar or lower and tail latency is lower. Two things cost more: idle CPU
(0.03 core instead of 0.001) and memory per idle connection (1.3 KB instead of 0.8 KB).

## Architecture

Three tiers of actors on every core (see the design doc, *Broker MQTT sur cannes : architecture
actuelle et cible*):

| Tier | Actor | Placement | Reached by |
|------|-------|-----------|------------|
| 1. Connection | `Connections`: listener (SO_REUSEPORT), sockets, MQTT parsing, output buffers | every core | the kernel spreads accepted connections over the cores |
| 2. Routing | `Router`: subscription trie, a full replica on every core | every core | **synchronous call** (`ActorRef`) from tier 1 on the same core, no event and no hop; SUBSCRIBE/UNSUBSCRIBE are broadcast to the other replicas |
| 3. Context | `Contexts`: retained messages, client-id registry (session takeover) | partitioned by hash over the cores | events |

The path of a PUBLISH:

1. It is parsed on the core that received it and matched against the local index replica.
2. It is encoded once.
3. It is appended to the output buffers of local subscribers.
4. It is sent as one `Deliver` event to each other core that has subscribers.

At the end of each loop iteration, every connection with pending output gets one `send`, and all
of them are submitted to io_uring together. Under load, many messages therefore share each
syscall, and the CPU cost per message falls as throughput rises. Mosquitto works the other way:
it writes each message to each subscriber as it is routed.

### Scope of the PoC

- MQTT 3.1.1: CONNECT, PUBLISH (QoS 0 and 1 inbound), SUBSCRIBE/UNSUBSCRIBE with `+`/`#`
  wildcards, retained messages, PINGREQ, DISCONNECT, and client-id takeover.
- Subscriptions are granted QoS 0, which the protocol allows. Every delivery is QoS 0.
- QoS 1 publishes are acknowledged once routed.
- Not implemented: persistence, QoS 1/2 delivery, persistent sessions, keep-alive enforcement,
  will messages, authentication, TLS, MQTT 5.
- Interoperability is checked with `mosquitto_pub` / `mosquitto_sub`.
- `tests/broker.rs` covers six cases:
  - wildcards across cores;
  - one delivery per connection with overlapping filters;
  - retained messages set and cleared;
  - unsubscribe and disconnect;
  - takeover;
  - ordering of 20k messages.

```sh
cargo run --release --bin cannes-mqtt -- --port 1883 --cores 1,2 --mode adaptive
```

Mode values:

- `spin`: busy-poll.
- `sleep:<µs>`: wait in io_uring after each idle iteration. An I/O completion wakes the core.
- `adaptive`: spin while traffic flows; after 2000 idle loops, sleep 50 µs at a time.
- `adaptive:<loops>:<µs>`: the same, with the idle loops and sleep length set.

## Benchmark

### Method

- **Machine:** cloud VM, 4 vCPU Intel Xeon @ 2.80 GHz, Linux 6.18, loopback TCP.
- **Pinning:** the brokers run on core 1 (and core 2 for the two-core run). The load generator
  runs on cores 0 and 3.
- **Mosquitto:** 2.0.18 with `bench/mosquitto.conf` (TCP_NODELAY, no logging, unbounded queues).
- **Load generator:** `src/bin/mqtt-bench.rs`.
  - Publishers send open-loop with Poisson arrivals, so a slow broker cannot slow the load down
    (no coordinated omission).
  - Each message carries the time it was *scheduled* to be sent. Latency is measured from that
    time to the subscriber's receipt.
  - Payload is 64 bytes.
  - Each run has a 1 s warm-up and a 4 s measurement.
- **Broker CPU:** taken from `/proc/<pid>/task/*/schedstat`, summed over all broker threads.
- **Broker memory:** taken from `VmRSS`.
- **Reproduce:** `bench/compare.sh` runs the whole matrix. Raw output is in `bench/results.txt`.

When a broker cannot keep up, its backlog grows without bound and the measured latency hits the
histogram ceiling (100 ms, shown as ">100 ms"). The **saturation point** is therefore the highest
offered rate at which `delivered = offered`.

### Fan-in: 10 publishers → 1 subscriber on `bench/#`, one broker core

| Offered msg/s | Mosquitto delivered | CPU | p50 / p99 | cannes (sleep) delivered | CPU | p50 / p99 |
|---:|---:|---:|---|---:|---:|---|
| 20 000 | 20 029 | 0.44 | 51 µs / 1.5 ms | 20 029 | 0.48 | 56 µs / 1.4 ms |
| 100 000 | **77 147 (saturated)** | 0.98 | >100 ms | 100 162 | 0.88 | 61 µs / 2.0 ms |
| 200 000 | 83 367 | 0.95 | >100 ms | 200 277 | 0.96 | 145 µs / 1.8 ms |
| 500 000 | 74 202 | 0.98 | >100 ms | 500 254 | 0.97 | 225 µs / 5.3 ms |
| 1 000 000 | 75 824 | 0.98 | >100 ms | 1 000 552 | 0.96 | 215 µs / 58 ms |
| 2 000 000 | 71 357 | 0.99 | >100 ms | **1 844 757 (saturated)** | 0.97 | >100 ms |

Mosquitto saturates at about 80k msg/s, which is about 12 µs of CPU per message. cannes-mqtt
sustains 1M msg/s on the same core, at under 1 µs of CPU per message.

### Fan-out: 1 publisher on `bench/0` → 50 subscribers, one broker core

The deliveries column counts deliveries: 50 × the publish rate.

| Publish msg/s → deliveries/s | Mosquitto delivered | CPU | p50 / p99 | cannes (sleep) delivered | CPU | p50 / p99 |
|---:|---:|---:|---|---:|---:|---|
| 1 000 → 51 200 | 51 202 | 0.48 | 670 µs / 64 ms | 51 189 | **0.34** | 498 µs / 2.3 ms |
| 2 000 → 102 000 | **105 845 (saturated)** | 0.93 | 51 ms / >100 ms | 100 420 | 0.52 | 536 µs / 3.6 ms |
| 4 000 → 200 000 | 113 423 | 0.99 | >100 ms | 199 900 | 0.73 | 708 µs / 3.6 ms |
| 10 000 → 500 000 | 125 503 | 0.99 | >100 ms | 497 653 | 0.94 | 890 µs / 19 ms |
| 20 000 → 1 000 000 | 125 103 | 0.99 | >100 ms | 1 000 643 | 0.92 | 1.2 ms / >100 ms |
| 40 000 → 2 000 000 | 114 268 | 0.99 | >100 ms | 2 006 371 | 0.98 | 3.6 ms / >100 ms |

- Mosquitto peaks at about 125k deliveries/s.
- cannes-mqtt keeps up with 2M deliveries/s on one core, at 0.5 µs of CPU per delivery. At that
  rate the load generator's single reader thread, serving 50 sockets, is what adds the latency.
- At the lowest rate, both brokers keep up. cannes-mqtt uses 30 % less CPU and its p99 is 28x
  lower.

### Idle: 10 000 connections that send nothing

| | Mosquitto | cannes `sleep:1000` | cannes `spin` | cannes 2 cores `adaptive` |
|---|---:|---:|---:|---:|
| Memory per connection | **0.78 KB** | 1.29 KB | 1.29 KB | 1.31 KB |
| CPU while idle | **0.001 core** | 0.034 core | 0.99 core | 0.68 core |
| Connection rate | 1 918 /s | 2 405 /s | 2 910 /s | 2 104 /s |

Idle costs are where Mosquitto still wins:

- **Memory.** Each connection holds a 1 KB receive buffer. A shared buffer pool
  (io_uring provided buffers) would remove it.
- **CPU.** `sleep:1000` wakes the core every millisecond. A core with no traffic could instead
  block in io_uring until a completion arrives, but that needs a wake-up for events from other
  cores (an eventfd).

### Other configurations

- **`spin`** gives the lowest latency (fan-in: 28 µs p50 at 100k msg/s against 61 µs with
  `sleep`). It costs a full core even when idle.
- **Two cores, `adaptive`** reaches the same totals as one core in this setup (1.92M msg/s fan-in,
  2.0M deliveries/s fan-out). That is the limit of the load generator: two client cores with
  single-threaded sockets. This run does not measure how the broker scales across cores.
  Measuring that needs a separate client machine. Full rows are in `bench/results.txt`.

## What it means for cost

These rows are the ones that matter for a pay-per-use cloud service:

- A broker core running Mosquitto saturates at about 80k msg/s (fan-in) or 125k deliveries/s
  (fan-out).
- On this machine, the same core running cannes-mqtt handles 1M–2M.
- For the same traffic, that is roughly an order of magnitude fewer broker cores, with the same
  protocol and the same clients.

**Caveats:**

- This is a PoC. Persistence, QoS 1/2 delivery and TLS all add CPU per message and are not
  implemented. TLS in particular would narrow the gap.
- The results come from one VM over loopback.
- Idle-heavy IoT fleets (many connections, little traffic) are the case where cannes-mqtt does not
  win yet.
