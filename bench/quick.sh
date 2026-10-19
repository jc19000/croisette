#!/usr/bin/env bash
# Quick regression bench (~40 s): the same load on croisette before and after a change.
# Prints one line per run; compare with bench/quick-baseline.txt.
# usage: bench/quick.sh [output-file]
set -u
cd "$(dirname "$0")/.."
cargo build --release -q
OUT=${1:-/dev/stdout}
BENCH=./target/release/croisette-bench

run() { # broker args | bench args...
  local broker=$1; shift
  ./target/release/croisette --port 18834 $broker 2>/dev/null & local pid=$!
  sleep 0.5
  local line
  line=$(timeout 40 taskset -c 0,3 $BENCH "$@" --port 18834 --broker-pid "$pid" 2>&1 | tail -1)
  kill "$pid" 2>/dev/null; wait "$pid" 2>/dev/null
  echo "[$broker] $line" | sed 's/ payload=64//; s/flow pubs=/pubs=/' >> "$OUT"
  sleep 0.3
}

S1="--cores 1 --mode sleep:1000"
SL="--cores 1 --mode sleep:100000"
run "$S1" flow --pubs 10 --subs 1 --rate 100000 --secs 3 --warmup 1
run "$S1" flow --pubs 10 --subs 1 --rate 1000000 --secs 3 --warmup 1
run "$S1" flow --pubs 1 --subs 50 --filter bench/0 --rate 10000 --secs 3 --warmup 1
run "--cores 1,2 --mode adaptive" flow --pubs 10 --subs 1 --rate 500000 --secs 3 --warmup 1
run "$S1" idle --conns 5000 --secs 3
run "$S1" flow --pubs 10 --subs 1 --rate 200000 --secs 3 --warmup 1 --qos 1
run "$SL" idle --conns 5000 --secs 3
