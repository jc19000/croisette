#!/usr/bin/env bash
# Compares cannes-mqtt with Mosquitto on the same load, same machine.
# Brokers are pinned to cores 1 (and 2), the load generator to cores 0 and 3.
# usage: bench/compare.sh [results-file]
set -u
cd "$(dirname "$0")/.."
cargo build --release -q
OUT=${1:-bench/results.txt}
BENCH=./target/release/mqtt-bench
: > "$OUT"

declare -A BROKERS=(
  [mosquitto]="taskset -c 1 mosquitto -c bench/mosquitto.conf"
  [cannes-1core-sleep]="./target/release/cannes-mqtt --port 18832 --cores 1 --mode sleep:1000"
  [cannes-1core-spin]="./target/release/cannes-mqtt --port 18832 --cores 1 --mode spin"
  [cannes-2cores-adaptive]="./target/release/cannes-mqtt --port 18832 --cores 1,2 --mode adaptive"
)
ORDER="mosquitto cannes-1core-sleep cannes-1core-spin cannes-2cores-adaptive"

run() { # broker-name, bench args...
  local name=$1; shift
  ${BROKERS[$name]} 2>/dev/null & local pid=$!
  sleep 0.5
  local line
  line=$(timeout 40 taskset -c 0,3 $BENCH "$@" --port 18832 --broker-pid "$pid" 2>&1 | tail -1)
  kill "$pid" 2>/dev/null; wait "$pid" 2>/dev/null
  echo "$name | $line" | tee -a "$OUT"
  sleep 0.5
}

for b in $ORDER; do
  # Fan-in: 10 publishers, 1 subscriber on bench/#. Total published rate in msg/s.
  for r in 20000 100000 200000 500000 1000000 2000000; do
    run "$b" flow --pubs 10 --subs 1 --rate $r --secs 4 --warmup 1
  done
  # Fan-out: 1 publisher on bench/0, 50 subscribers: deliveries = 50 x rate.
  for r in 1000 2000 4000 10000 20000 40000; do
    run "$b" flow --pubs 1 --subs 50 --filter bench/0 --rate $r --secs 4 --warmup 1 --threads 2
  done
  # Idle connections: memory per connection, CPU while nothing happens.
  run "$b" idle --conns 10000 --secs 5
done
