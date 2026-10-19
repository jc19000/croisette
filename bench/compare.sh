#!/usr/bin/env bash
# Compares croisette with Mosquitto on the same load, same machine.
# Brokers are pinned to cores 1 (and 2), the load generator to cores 0 and 3.
# usage: bench/compare.sh [results-file]
set -u
cd "$(dirname "$0")/.."
cargo build --release -q
OUT=${1:-bench/results.txt}
BENCH=./target/release/croisette-bench
ONLY=${ONLY:-fanin,fanout,qos1,idle}
[ -n "${APPEND:-}" ] || : > "$OUT"

declare -A BROKERS=(
  [mosquitto]="taskset -c 1 mosquitto -c bench/mosquitto.conf"
  [croisette-1core]="./target/release/croisette --port 18832 --cores 1"
  [croisette-1core-spin]="./target/release/croisette --port 18832 --cores 1 --mode spin"
  [croisette-2cores-adaptive]="./target/release/croisette --port 18832 --cores 1,2 --mode adaptive"
  # TLS listeners on port 18833 (tls block).
  [mosquitto-tls]="taskset -c 1 mosquitto -c bench/mosquitto-tls.conf"
  [croisette-1core-tls]="./target/release/croisette --port 18832 --cores 1 --tls-cert tests/data/cert.pem --tls-key tests/data/key.pem --tls-port 18833"
)
ORDER=${ORDER:-mosquitto croisette-1core croisette-1core-spin croisette-2cores-adaptive}

run() { # broker-name, bench args...
  local name=$1; shift
  ${BROKERS[$name]} 2>/dev/null & local pid=$!
  sleep 0.5
  local line port=18832 tls=()
  [[ $name == *-tls ]] && port=18833 && tls=(--tls tests/data/ca.pem)
  line=$(timeout 40 taskset -c 0,3 $BENCH "$@" "${tls[@]}" --port $port --broker-pid "$pid" 2>&1 | tail -1)
  kill "$pid" 2>/dev/null; wait "$pid" 2>/dev/null
  echo "$name | $line" | tee -a "$OUT"
  sleep 0.5
}

for b in $ORDER; do
  # Fan-in: 10 publishers, 1 subscriber on bench/#. Total published rate in msg/s.
  [[ $ONLY == *fanin* ]] && for r in 20000 100000 200000 500000 1000000 2000000; do
    run "$b" flow --pubs 10 --subs 1 --rate $r --secs 4 --warmup 1
  done
  # Fan-out: 1 publisher on bench/0, 50 subscribers: deliveries = 50 x rate.
  [[ $ONLY == *fanout* ]] && for r in 1000 2000 4000 10000 20000 40000; do
    run "$b" flow --pubs 1 --subs 50 --filter bench/0 --rate $r --secs 4 --warmup 1 --threads 1
  done
  # QoS 1 end to end: 10 publishers, 1 subscriber, everything acknowledged.
  [[ $ONLY == *qos1* ]] && for r in 10000 20000 50000 100000 200000; do
    run "$b" flow --pubs 10 --subs 1 --rate $r --secs 4 --warmup 1 --qos 1
  done
  # TLS (QoS 0), for the -tls brokers: ORDER="mosquitto-tls croisette-1core-tls" ONLY=tls.
  [[ $ONLY == *tls* ]] && for r in 20000 50000 100000 200000 500000; do
    run "$b" flow --pubs 10 --subs 1 --rate $r --secs 4 --warmup 1
  done
  [[ $ONLY == *tls* ]] && for r in 1000 4000 10000; do
    run "$b" flow --pubs 1 --subs 50 --filter bench/0 --rate $r --secs 4 --warmup 1 --threads 1
  done
  # Idle connections: memory per connection, CPU while nothing happens.
  [[ $ONLY == *idle* ]] && run "$b" idle --conns 10000 --secs 5
done
