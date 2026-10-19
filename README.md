# croisette

An MQTT 3.1.1 broker built on the [cannes](https://github.com/jc19000/cannes) thread-per-core
actor engine, with io_uring for the network. It is benchmarked against Mosquitto 2.0.18 on the
same machine and the same load.

**Result in one line:** with one core each, croisette sustains about **12x (fan-in), 14x
(fan-out) and 7x (QoS 1) the message rate** of Mosquitto before saturating, and 5x to 8x over
TLS. At loads both brokers
handle, croisette uses 20 to 50 % less CPU and has lower tail latency. When idle, both use about
0.001 core. Memory per idle connection is 0.93 KB, against 0.78 KB for Mosquitto.

Status: MQTT 3.1.1 with TLS and snapshots on disk (see [Scope](#scope)). It has not been run in
production.

```sh
cargo run --release --bin croisette -- --port 1883 --cores 1,2
```

## Architecture

Three tiers of actors run on every core (see the design doc, *Broker MQTT sur cannes :
architecture actuelle et cible*):

| Tier | Actor | Placement | Reached by |
|------|-------|-----------|------------|
| 1. Connection | `Connections`: listener (SO_REUSEPORT), sockets, MQTT parsing, output buffers, QoS windows, keep-alive | every core | the kernel spreads accepted connections over the cores |
| 2. Routing | `Router`: subscription trie with shared-subscription groups, a full replica on every core | every core | **synchronous call** (`ActorRef`) from tier 1 on the same core, with no event and no hop; SUBSCRIBE/UNSUBSCRIBE are broadcast to the other replicas |
| 3. Context | `Contexts`: retained messages, sessions (client-id registry, persistent sessions and their queues) | partitioned by hash over the cores | events |

### The path of a PUBLISH

1. It is parsed on the core that received it and matched against that core's index replica.
2. It is encoded once. QoS 1/2 copies only get their packet id (`codec::requalify`).
3. It is appended to the output buffers of local subscribers.
4. It is sent as one `Deliver` event to each other core that has subscribers.

At the end of each loop iteration, every connection with pending output gets one `send`, and all
of them are submitted to io_uring together. Under load, many messages share each syscall, so the
CPU cost per message falls as throughput rises. Mosquitto writes each message to each subscriber
as it is routed.

### Persistent sessions

A session lives on the context partition of its client id. When a persistent connection closes,
its subscriptions in every router replica are moved to *session targets*. The broker adds the
session target before it removes the connection's entry, and both changes come from the same
core, so no message falls in between.

While a session is offline, its QoS 1/2 messages are queued at the partition. On reconnection:

- the subscriptions come back;
- unacknowledged messages are redelivered with DUP and their original packet ids;
- the queue follows.

Takeover hands the session from the old connection to the new one. A stress test reconnects a
subscriber abruptly 20 times while messages are in flight, for QoS 1 and for QoS 2, and checks
that no message is lost. That test found a race, now fixed: messages routed to a connection just
before every replica saw it close are forwarded to its session.

## Scope

**MQTT 3.1.1, implemented:**

- **CONNECT:**
  - validated: protocol level, flags, client id;
  - credentials: password file in clear or salted SHA-256;
  - CONNACK return codes 1, 2, 4 and 5;
  - CONNECT timeout.
- **Keep-alive:** a client silent for 1.5 periods is disconnected. Will messages are published,
  and retained if asked, when a client goes away without DISCONNECT.
- **PUBLISH:**
  - QoS 0, 1 and 2 in both directions;
  - per-connection in-flight window (`max_inflight`, default 128);
  - a QoS 2 PUBLISH is routed once (duplicates before PUBREL are not routed again).
- **SUBSCRIBE / UNSUBSCRIBE:**
  - `+` and `#` wildcards, with the `$` topic rule;
  - granted QoS up to 2;
  - one delivery per connection when several of its filters match, at the highest QoS.
- **Retained messages:** they keep their QoS; a message with an empty payload clears the topic.
- **Persistent sessions** (`clean_session = 0`), as described above.
- **Shared subscriptions:** `$share/<group>/<filter>` delivers to one member of each group, in
  turn.
- **ACL** in Mosquitto's format, with `user`, `topic` and `pattern` lines and the `%c` / `%u`
  substitutions.
- **`$SYS/broker/...` statistics**, aggregated over the cores.
- **Limits and backpressure:**
  - maximum packet size;
  - `max_queued` bytes per connection: past that, QoS 0 messages to a slow subscriber are
    dropped and counted, so it neither grows the broker's memory nor slows the others;
  - `max_core_backlog` bytes between two cores: the same for a core slower than its senders.

**TLS:** a TLS listener with rustls (`--tls-cert`, `--tls-key`, `--tls-port`, default 8883),
alongside the TCP one.

**Persistence:** `--persist-dir DIR` saves the retained messages and the persistent sessions
(subscriptions and undelivered QoS 1/2 messages):

- every `--autosave` seconds (default 30), each core writes its partition with io_uring, to a new
  file renamed over the previous one;
- again on SIGTERM or SIGINT, which stop the broker cleanly;
- at start-up, the snapshots are read and their entries spread over the current cores, whose
  number may have changed.

Messages received after the last snapshot are lost on a crash, and nothing is fsynced. This is
Mosquitto's model (autosave_interval).

**Not implemented:** MQTT 5, bridges, WebSockets.

**Tests:**

- `tests/broker.rs`: 29 end-to-end tests with real TCP and TLS clients on two cores, including
  a restart from snapshots.
- Unit tests for the codec, the index and authentication.
- `tests/interop.sh`: 16 checks with `mosquitto_pub` / `mosquitto_sub`, TLS and restart included.

### Options

```
croisette [--port 1883] [--cores 1,2] [--mode MODE] [--max-queued BYTES] [--max-packet BYTES]
          [--max-inflight N] [--connect-timeout S] [--sys-interval S]
          [--passwd FILE] [--acl FILE] [--allow-anonymous true|false]
          [--tls-cert PEM --tls-key PEM [--tls-port 8883]] [--persist-dir DIR [--autosave S]]
croisette hash-password USER PASSWORD
```

Mode values:

- `sleep:<µs>`: wait in io_uring after each idle iteration; an I/O completion wakes the core.
  This is the default on one core, as `sleep:100000`.
- `adaptive`: spin while traffic flows, and sleep 1 ms at a time after 2000 idle loops. This is
  the default on several cores. I/O still wakes a sleeping core at once; events from other cores
  wait for the end of the sleep.
- `adaptive:<loops>:<µs>`: the same, with the idle loops and sleep length set.
- `spin`: busy-poll.

## Benchmark

### Method

- **Machine:** cloud VM, 4 vCPU Intel Xeon @ 2.80 GHz, Linux 6.18, loopback TCP.
- **Pinning:** the brokers run on core 1 (and core 2 for the two-core run). The load generator
  runs on cores 0 and 3.
- **Mosquitto:** 2.0.18 with `bench/mosquitto.conf` (TCP_NODELAY, no logging, unbounded queues,
  no in-flight limit).
- **croisette:** run with its default options.
- **Load generator:** `src/bin/croisette-bench.rs`.
  - Publishers send open-loop with Poisson arrivals, so a slow broker cannot slow the load down
    (no coordinated omission).
  - Each message carries the time it was *scheduled* to be sent. Latency is measured from that
    time to the subscriber's receipt.
  - Payload is 64 bytes.
  - Each run has a 1 s warm-up and a 4 s measurement.
- **Broker CPU:** taken from `/proc/<pid>/task/*/schedstat`, summed over all broker threads.
- **Broker memory:** taken from `VmRSS`.
- **Reproduce:**
  - `bench/compare.sh` runs the whole matrix; the raw output is in `bench/results.txt`.
  - `bench/quick.sh` is the 40 s regression bench run before every commit; its baseline is
    `bench/quick-baseline.txt`.

When a broker cannot keep up, its backlog grows and the measured latency hits the histogram
ceiling (100 ms, shown as ">100 ms"). The **saturation point** is therefore the highest offered
rate at which `delivered = offered`.

### Fan-in: 10 publishers → 1 subscriber on `bench/#`, QoS 0, one broker core

| Offered msg/s | Mosquitto delivered | CPU | p50 / p99 | croisette delivered | CPU | p50 / p99 |
|---:|---:|---:|---|---:|---:|---|
| 20 000 | 20 027 | 0.41 | 49 µs / 3.2 ms | 20 028 | **0.30** | 43 µs / 1.0 ms |
| 100 000 | **79 119 (saturated)** | 0.97 | >100 ms | 100 165 | 0.79 | 54 µs / 4.3 ms |
| 200 000 | 81 751 | 0.97 | >100 ms | 200 276 | 0.92 | 149 µs / 18 ms |
| 500 000 | 85 551 | 0.97 | >100 ms | 500 310 | 0.96 | 183 µs / 4.2 ms |
| 1 000 000 | 78 014 | 0.98 | >100 ms | 1 000 405 | 0.94 | 316 µs / 28 ms |
| 2 000 000 | 91 420 | 0.99 | >100 ms | **1 973 674 (saturated)** | 0.79 | >100 ms |

Mosquitto saturates at about 80k msg/s, which is about 12 µs of CPU per message. croisette
sustains 1M msg/s on the same core, at under 1 µs of CPU per message.

### Fan-out: 1 publisher on `bench/0` → 50 subscribers, QoS 0, one broker core

The publish rate is multiplied by 50: deliveries = 50 × publishes.

| Publishes/s → deliveries/s | Mosquitto delivered | CPU | p50 / p99 | croisette delivered | CPU | p50 / p99 |
|---:|---:|---:|---|---:|---:|---|
| 1 000 → 51 200 | 51 136 | 0.47 | 548 µs / 4.0 ms | 51 198 | **0.26** | 427 µs / 4.4 ms |
| 2 000 → 102 000 | 100 696 | 0.84 | 1.7 ms / 41 ms | 100 408 | 0.43 | 426 µs / 2.7 ms |
| 4 000 → 200 000 | **147 979 (saturated)** | 0.99 | >100 ms | 199 941 | 0.67 | 452 µs / 2.8 ms |
| 10 000 → 500 000 | 131 996 | 0.99 | >100 ms | 498 240 | 0.92 | 606 µs / 3.1 ms |
| 20 000 → 1 000 000 | 130 874 | 0.98 | >100 ms | 1 001 278 | 0.98 | 630 µs / 3.3 ms |
| 40 000 → 2 000 000 | 126 209 | 0.99 | >100 ms | 2 006 566 | 0.95 | 822 µs / 7.8 ms |

- Mosquitto peaks at about 130–150k deliveries/s.
- croisette delivers 2M/s on one core, at 0.5 µs of CPU per delivery.

### QoS 1 end to end: 10 publishers → 1 subscriber, every message acknowledged, one broker core

| Offered msg/s | Mosquitto delivered | CPU | p50 / p99 | croisette delivered | CPU | p50 / p99 |
|---:|---:|---:|---|---:|---:|---|
| 10 000 | 9 964 | 0.52 | 66 µs / 1.5 ms | 9 965 | **0.41** | 51 µs / 1.3 ms |
| 20 000 | 20 028 | 0.88 | 141 µs / 8.2 ms | 20 028 | **0.64** | 54 µs / 1.9 ms |
| 50 000 | **26 386 (saturated)** | 0.98 | >100 ms | 50 148 | 0.88 | 269 µs / 3.6 ms |
| 100 000 | 24 628 | 0.98 | >100 ms | 100 164 | 0.89 | 359 µs / 2.4 ms |
| 200 000 | 27 666 | 0.99 | >100 ms | 199 833 | 0.91 | 432 µs / 23 ms |

Mosquitto saturates at about 26k QoS 1 msg/s. croisette sustains 200k msg/s, and at 20k msg/s
it uses 27 % less CPU with a p99 four times lower.

### TLS: the same loads over TLS 1.3, QoS 0, one broker core

Mosquitto uses `bench/mosquitto-tls.conf`; croisette runs with `--tls-cert` / `--tls-key`. Both
use the test certificate in `tests/data`. The load generator speaks TLS with rustls.

| Load | Mosquitto delivered | CPU | p50 / p99 | croisette delivered | CPU | p50 / p99 |
|---|---:|---:|---|---:|---:|---|
| fan-in 20 000 msg/s | 20 026 | 0.52 | 62 µs / 2.5 ms | 20 029 | **0.33** | 46 µs / 1.4 ms |
| fan-in 50 000 msg/s | 50 147 | 0.94 | 298 µs / 28 ms | 50 149 | 0.58 | 45 µs / 2.8 ms |
| fan-in 100 000 msg/s | **59 987 (saturated)** | 0.98 | >100 ms | 100 173 | 0.83 | 64 µs / 1.4 ms |
| fan-in 200 000 msg/s | 55 653 | 0.98 | >100 ms | 200 283 | 0.97 | 175 µs / 2.7 ms |
| fan-in 500 000 msg/s | 61 440 | 0.97 | >100 ms | 500 295 | 0.95 | 200 µs / 7.6 ms |
| fan-out 1 000 → 51 200/s | 51 198 | 0.58 | 775 µs / 11 ms | 51 193 | **0.31** | 495 µs / 8.9 ms |
| fan-out 4 000 → 200 000/s | **99 008 (saturated)** | 0.98 | >100 ms | 199 867 | 0.77 | 682 µs / 3.3 ms |
| fan-out 10 000 → 500 000/s | 92 956 | 0.99 | >100 ms | 498 109 | 0.94 | 707 µs / 3.4 ms |

TLS costs both brokers. The gap stays at about 8x in fan-in and 5x in fan-out, because croisette
encrypts each connection's batch of messages as a few records per loop iteration, while Mosquitto
encrypts message by message. Raw rows are in `bench/results-tls.txt`.

### Idle: 10 000 connections that send nothing

| | Mosquitto | croisette (default) | croisette `spin` | croisette 2 cores (default `adaptive`) |
|---|---:|---:|---:|---:|
| Memory per connection | **0.78 KB** | 0.93 KB | 0.93 KB | 0.95 KB |
| CPU while idle | 0.001 core | 0.001 core | 0.99 core | 0.06 core |

- **Memory:** a receive buffer starts at 256 bytes. It doubles when a receive fills it, and
  halves back after the traffic drops. Each session also has a registry entry.
- **CPU:** the default one-core mode only waits in io_uring, so it uses no CPU when idle. On
  several cores, the default mode wakes every millisecond to see the other cores' events:
  0.06 core in total. It used to sleep 50 µs at a time, which cost 0.66 core for the same latency
  under load. An eventfd wake-up in cannes would remove the rest.

### Other configurations

- **`spin`:** lowest latency (fan-in at 100k msg/s: 30 µs p50, against 54 µs by default), at the
  cost of a full core even when idle.
- **Two cores, `adaptive`:** it reaches the same totals as one core in this setup, which is the
  limit of the load generator (two client cores). This run does not measure how the broker scales
  across cores; that needs a separate client machine. At 2M msg/s offered, the two-core run in
  `bench/results.txt` collapsed to 270k msg/s delivered with 470 MB of memory. The cause was
  events queued without bound for a core slower than its senders. Since then, cores exchange
  credits: past `max_core_backlog` (16 MiB) unacknowledged bytes toward a core, QoS 0 messages
  to it are dropped and counted. In the same overload, memory now stays at 61 MB and most runs
  deliver 1.2M msg/s.

Full rows are in `bench/results.txt`.

## What it means for cost

These rows matter for a pay-per-use cloud service. A broker core running Mosquitto saturates at
about:

- 80k msg/s in fan-in;
- 130–150k deliveries/s in fan-out;
- 26k msg/s with QoS 1.

On the same machine, the same core running croisette handles 1M–2M msg/s, or 200k msg/s with
QoS 1. For the same traffic, that is roughly an order of magnitude fewer broker cores, with the
same protocol and the same clients. When idle, croisette costs no more CPU than Mosquitto and
about 20 % more memory per connection.

With TLS, the ratio is about 8x in fan-in and 5x in fan-out.

**Caveat:** the results come from one VM over loopback.
