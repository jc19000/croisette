# croisette

An MQTT 5 and 3.1.1 broker built on the [cannes](https://github.com/jc19000/cannes) thread-per-core
actor engine, with io_uring for the network. It is benchmarked against Mosquitto 2.0.18 on the
same machine and the same load.

**Result in one line:** with one core each, croisette sustains about **12x (fan-in), 14x
(fan-out) and 6x (QoS 1) the message rate** of Mosquitto before saturating, and 5x to 8x over
TLS. At loads both brokers
handle, croisette uses 20 to 45 % less CPU and has lower tail latency. When idle, both use about
0.001 core. Memory per idle connection is 1.0 KB, against 0.78 KB for Mosquitto.

Status: MQTT 5 and 3.1.1, with TLS, WebSocket and durable storage (snapshots and a write-ahead log, see [Scope](#scope)). It has not been run in
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

**MQTT 3.1.1 (and the same in MQTT 5), implemented:**

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

**MQTT 5**, alongside MQTT 3.1.1 and 3.1 on the same listeners; messages flow between client
versions:

- **Properties:** user properties, content type, response topic, correlation data and payload
  format are forwarded.
- **Reason codes:**
  - CONNACK;
  - PUBACK, PUBREC: No matching subscribers, Not authorized;
  - SUBACK, UNSUBACK;
  - DISCONNECT from the broker: keep-alive timeout, session taken over, packet too large,
    malformed packet, topic alias invalid.
- **Sessions:** session expiry interval (also changed by DISCONNECT) and Clean Start; the broker
  assigns client identifiers when a client sends none.
- **Messages:** message expiry, whether the message is live, queued for an offline session, or
  retained.
- **Flow:**
  - topic aliases from clients (up to 64);
  - the client's Receive Maximum (in-flight window) and Maximum Packet Size are honoured;
  - the broker's maximum packet size is announced.
- **Subscription options:** No Local, Retain As Published, Retain Handling.
- **Will:**
  - will delay interval: the will waits at the client id's partition, is cancelled if the client
    comes back, and is sent at the end of the delay or of the session;
  - DISCONNECT with reason 0x04 sends the will.

Not implemented yet:

- subscription identifiers (announced as unavailable);
- enhanced authentication (AUTH; CONNACK Bad authentication method);
- topic aliases from the broker to clients;
- request/response information;
- server redirection.

**WebSocket:** MQTT over WebSocket (`--ws-port`), and over secure WebSocket (`--wss-port`, with
TLS). The upgrade accepts the `mqtt` subprotocol. A packet may span binary frames, and a frame
may hold several packets.

**Storage:** `--persist-dir DIR` keeps the retained messages and the persistent sessions
(subscriptions and undelivered QoS 1/2 messages) on disk. `--storage` picks how:

| `--storage` | What is on disk | Lost on a crash |
|---|---|---|
| `none` | nothing (`--persist-dir` ignored) | everything |
| `snapshot` (default with `--persist-dir`) | snapshots every `--autosave` seconds | what came after the last snapshot (Mosquitto's `autosave_interval` model) |
| `wal` | snapshots + a write-ahead log | nothing that was acknowledged |

Snapshots: every `--autosave` seconds (default 30), each core writes its partition with
io_uring, to a new file renamed over the previous one; again on SIGTERM or SIGINT, which stop
the broker cleanly. At start-up the snapshots are read and their entries spread over the current
cores, whose number may have changed.

Write-ahead log (`--storage wal`): each core appends to its own segment the QoS 1/2 messages it
receives, the changes of retained messages and of persistent sessions. The records of one loop
iteration go out as one io_uring write, and the PUBACK / PUBREC of the messages they hold are
sent when that write completes: one disk flush for many messages (group commit). Each snapshot
is a checkpoint: the cores start new segments and drop the ones it covers. At start-up the
segments are replayed on top of the snapshots (a message may then be delivered twice, never
lost). Records carry a CRC; a torn last record is ignored.

- `--fsync batch` (default): the log is opened O_DSYNC, so an acknowledged message survives a
  power cut.
- `--fsync interval:<ms>`: a thread flushes the log every period; an acknowledged message
  survives a broker crash, and a power cut loses up to one period.
- `--fsync never`: the kernel writes back when it wants; an acknowledged message survives a
  broker crash.
- `--durable-qos 2`: log only QoS 2 messages (default 1: QoS 1 and 2).

On the test VM a synchronous 4 KB write takes about 400 µs, yet QoS 1 at 200 000 msg/s (10
publishers, one core) runs as fast with `--storage wal --fsync batch` as with snapshots only
(4.7 CPU-seconds per million messages both), since each flush carries about 80 messages.

**Conflation:** `--conflate FILTER` (repeatable) bounds the queues of offline sessions for
state-like topics (telemetry, device shadows): a message on a matching topic replaces the one of
the same topic already queued, at its place. A device back after a day gets the last value of
each such topic, not 86 400 of them; commands on other topics keep their full history.

**Not implemented:** bridges, clustering across machines.

**Tests:**

- `tests/broker.rs`: 37 end-to-end tests with real TCP, TLS and WebSocket clients, MQTT 5 and
  3.1.1, on two cores, including a restart from snapshots and a kill -9 with the write-ahead log.
- Unit tests for the codec, the index and authentication.
- `tests/interop.sh`: 20 checks with `mosquitto_pub` / `mosquitto_sub`, MQTT 5, TLS and restart
  included.

### Options

```
croisette [--port 1883] [--cores 1,2] [--mode MODE] [--max-queued BYTES] [--max-packet BYTES]
          [--max-inflight N] [--connect-timeout S] [--sys-interval S]
          [--passwd FILE] [--acl FILE] [--allow-anonymous true|false]
          [--tls-cert PEM --tls-key PEM [--tls-port 8883] [--wss-port P]] [--ws-port P]
          [--persist-dir DIR [--autosave S] [--storage none|snapshot|wal]
           [--fsync batch|interval:<ms>|never] [--durable-qos 1|2]] [--conflate FILTER]...
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
| 20 000 | 20 029 | 0.42 | 48 µs / 1.5 ms | 20 027 | **0.29** | 42 µs / 514 µs |
| 100 000 | **80 966 (saturated)** | 0.98 | >100 ms | 100 168 | 0.73 | 39 µs / 718 µs |
| 200 000 | 95 972 | 0.97 | >100 ms | 200 290 | 0.93 | 83 µs / 1.3 ms |
| 500 000 | 77 677 | 0.98 | >100 ms | 500 305 | 0.94 | 189 µs / 8.2 ms |
| 1 000 000 | 78 126 | 0.98 | >100 ms | 1 000 477 | 0.98 | 224 µs / 5.2 ms |
| 2 000 000 | 85 148 | 1.00 | >100 ms | **1 829 783 (saturated)** | 0.77 | >100 ms |

Mosquitto saturates at 80–95k msg/s, about 12 µs of CPU per message. croisette sustains 1M
msg/s on the same core, at under 1 µs of CPU per message.

### Fan-out: 1 publisher on `bench/0` → 50 subscribers, QoS 0, one broker core

The publish rate is multiplied by 50: deliveries = 50 × publishes.

| Publishes/s → deliveries/s | Mosquitto delivered | CPU | p50 / p99 | croisette delivered | CPU | p50 / p99 |
|---:|---:|---:|---|---:|---:|---|
| 1 000 → 51 200 | 51 198 | 0.43 | 482 µs / 3.6 ms | 51 198 | **0.26** | 434 µs / 3.3 ms |
| 2 000 → 102 400 | 100 508 | 0.81 | 1.2 ms / 22 ms | 100 408 | 0.44 | 415 µs / 2.4 ms |
| 4 000 → 204 800 | **135 819 (saturated)** | 0.98 | >100 ms | 199 888 | 0.64 | 426 µs / 2.7 ms |
| 10 000 → 512 000 | 135 702 | 0.99 | >100 ms | 498 291 | 0.92 | 564 µs / 3.1 ms |
| 20 000 → 1 024 000 | 140 679 | 0.99 | >100 ms | 1 001 408 | 0.97 | 709 µs / 3.6 ms |
| 40 000 → 2 048 000 | 132 607 | 0.99 | >100 ms | 2 006 358 | 0.97 | 944 µs / 14 ms |

- Mosquitto peaks at about 135–140k deliveries/s.
- croisette delivers 2M/s on one core, at 0.5 µs of CPU per delivery.

### QoS 1 end to end: 10 publishers → 1 subscriber, every message acknowledged, one broker core

| Offered msg/s | Mosquitto delivered | CPU | p50 / p99 | croisette delivered | CPU | p50 / p99 |
|---:|---:|---:|---|---:|---:|---|
| 10 000 | 9 965 | 0.51 | 65 µs / 4.1 ms | 9 962 | **0.39** | 49 µs / 5.6 ms |
| 20 000 | 20 176 | 0.83 | 98 µs / 22 ms | 20 029 | **0.67** | 65 µs / 1.9 ms |
| 50 000 | **31 512 (saturated)** | 0.99 | >100 ms | 50 146 | 0.87 | 246 µs / 7.5 ms |
| 100 000 | 28 317 | 0.99 | >100 ms | 100 177 | 0.90 | 389 µs / 4.6 ms |
| 200 000 | 28 470 | 0.98 | >100 ms | 200 319 | 0.94 | 435 µs / 38 ms |

Mosquitto saturates at about 30k QoS 1 msg/s. croisette sustains 200k msg/s, and at 20k msg/s it
uses 20 % less CPU with a p99 ten times lower.

### TLS: the same loads over TLS 1.3, QoS 0, one broker core

Mosquitto uses `bench/mosquitto-tls.conf`; croisette runs with `--tls-cert` / `--tls-key`. Both
use the test certificate in `tests/data`. The load generator speaks TLS with rustls.

| Load | Mosquitto delivered | CPU | p50 / p99 | croisette delivered | CPU | p50 / p99 |
|---|---:|---:|---|---:|---:|---|
| fan-in 20 000 msg/s | 20 027 | 0.46 | 49 µs / 1.1 ms | 20 028 | **0.32** | 44 µs / 1.2 ms |
| fan-in 50 000 msg/s | 50 160 | 0.91 | 151 µs / 41 ms | 50 148 | 0.57 | 44 µs / 2 ms |
| fan-in 100 000 msg/s | **62 585 (saturated)** | 1.00 | >100 ms | 100 153 | 0.82 | 52 µs / 963 µs |
| fan-in 200 000 msg/s | 64 938 | 0.97 | >100 ms | 200 281 | 0.97 | 179 µs / 3 ms |
| fan-in 500 000 msg/s | 62 301 | 0.99 | >100 ms | 500 307 | 0.98 | 203 µs / 3 ms |
| fan-out 1 000 → 51 200/s | 51 198 | 0.52 | 576 µs / 3.4 ms | 51 194 | **0.29** | 456 µs / 3.2 ms |
| fan-out 4 000 → 204 800/s | 97 246 | 0.99 | >100 ms | 199 880 | 0.64 | 451 µs / 2 ms |
| fan-out 10 000 → 512 000/s | 102 484 | 0.99 | >100 ms | 498 248 | 0.95 | 687 µs / 2.2 ms |

TLS costs both brokers. The gap stays at about 8x in fan-in and 5x in fan-out, because croisette
encrypts each connection's batch of messages as a few records per loop iteration, while Mosquitto
encrypts message by message. Raw rows are in `bench/results-tls.txt`.

### Idle: 10 000 connections that send nothing

| | Mosquitto | croisette (default) | croisette `spin` | croisette 2 cores (default `adaptive`) |
|---|---:|---:|---:|---:|
| Memory per connection | **0.78 KB** | 1.00 KB | 1.01 KB | 1.03 KB |
| CPU while idle | 0.001 core | 0.001 core | 0.99 core | 0.06 core |

- **Memory:** a receive buffer starts at 256 bytes. It doubles when a receive fills it, and
  halves back after the traffic drops. Each session also has a registry entry.
- **CPU:** the default one-core mode only waits in io_uring, so it uses no CPU when idle. On
  several cores, the default mode wakes every millisecond to see the other cores' events:
  0.06 core in total. It used to sleep 50 µs at a time, which cost 0.66 core for the same latency
  under load. An eventfd wake-up in cannes would remove the rest.

### Other configurations

- **`spin`:** lowest latency (fan-in at 100k msg/s: 32 µs p50, against 39 µs by default), at the
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

- 80–95k msg/s in fan-in;
- 135–140k deliveries/s in fan-out;
- about 30k msg/s with QoS 1.

On the same machine, the same core running croisette handles 1M–2M msg/s, or 200k msg/s with
QoS 1. For the same traffic, that is roughly an order of magnitude fewer broker cores, with the
same protocol and the same clients. When idle, croisette costs no more CPU than Mosquitto and
about 30 % more memory per connection.

With TLS, the ratio is about 8x in fan-in and 5x in fan-out.

**Caveat:** the results come from one VM over loopback.
