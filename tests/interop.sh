#!/usr/bin/env bash
# Interoperability with the Mosquitto command-line clients (mosquitto_pub, mosquitto_sub).
# usage: tests/interop.sh   (needs mosquitto-clients; builds croisette)
set -u
cd "$(dirname "$0")/.."
cargo build --release -q
PORT=18840
DIR=$(mktemp -d)
trap 'kill $BROKER 2>/dev/null; rm -rf "$DIR"' EXIT
fail=0
check() { # name, expected, actual
  if [ "$2" == "$3" ]; then echo "ok   $1"; else echo "FAIL $1: expected [$2] got [$3]"; fail=1; fi
}
start() {
  ./target/release/croisette --port $PORT --cores 1,2 --sys-interval 1 "$@" 2>/dev/null & BROKER=$!
  sleep 0.5
}
stop() { kill $BROKER; wait $BROKER 2>/dev/null; }
sub() { mosquitto_sub -p $PORT "$@" 2>&1; }
pub() { mosquitto_pub -p $PORT "$@"; }

start
for q in 0 1 2; do
  sub -t "q/$q" -q $q -C 3 -W 3 > "$DIR/q$q" & S=$!
  sleep 0.3
  for i in 1 2 3; do pub -t "q/$q" -q $q -m "m$i"; done
  wait $S
  check "QoS $q publish and subscribe" "m1 m2 m3" "$(tr '\n' ' ' < "$DIR/q$q" | sed 's/ $//')"
done

pub -t "r/door" -r -q 1 -m open
check "retained message" "open" "$(sub -t 'r/#' -C 1 -W 3)"
pub -t "r/door" -r -n
check "retained message cleared" "" "$(sub -t 'r/#' -C 1 -W 1 | grep -v Timed)"

sub -t "w/#" -C 1 -W 5 -v > "$DIR/will" & S=$!
sleep 0.3
mosquitto_sub -p $PORT -t nothing --will-topic w/dev --will-payload gone -i will-dev & D=$!
sleep 0.3
kill -9 $D
wait $S
check "will on abrupt disconnect" "w/dev gone" "$(cat "$DIR/will")"

sub -t "s/#" -q 1 -c -i persistent -C 1 -W 2 > /dev/null # creates the session, then leaves
pub -t s/a -q 1 -m queued
check "persistent session queue" "queued" "$(sub -t 's/#' -q 1 -c -i persistent -C 1 -W 3)"

# The next round's value counts this subscriber (and maybe clients of the steps above still
# closing): at least 1.
n=$(sub -t '$SYS/broker/clients/connected' -C 2 -W 4 | tail -1)
check "\$SYS clients" "yes" "$([ "${n:-0}" -ge 1 ] 2>/dev/null && echo yes || echo "no ($n)")"

sub -t "share/x" -q 1 -C 0 -W 1 > /dev/null 2>&1
mosquitto_sub -p $PORT -t '$share/g/share/#' -C 2 -W 3 > "$DIR/sh1" & A=$!
mosquitto_sub -p $PORT -t '$share/g/share/#' -C 2 -W 3 > "$DIR/sh2" & B=$!
sleep 0.3
for i in 1 2 3 4; do pub -t share/x -m "s$i"; done
wait $A $B
check "shared subscription" "4" "$(cat "$DIR/sh1" "$DIR/sh2" | sort -u | wc -l | tr -d ' ')"
stop

start
mosquitto_sub -p $PORT -V mqttv5 -t 'v5/#' -C 2 -W 3 -F '%t %p %P' > "$DIR/v5" & S=$!
sleep 0.3
mosquitto_pub -p $PORT -V mqttv5 -t v5/a -m hello -q 1 -D publish user-property color blue
mosquitto_pub -p $PORT -V mqttv311 -t v5/b -m old -q 2
wait $S
check "MQTT 5 user property forwarded" "v5/a hello color:blue" "$(head -1 "$DIR/v5")"
check "MQTT 3.1.1 publisher, MQTT 5 subscriber" "v5/b old" "$(sed -n 2p "$DIR/v5" | sed 's/ $//')"
mosquitto_sub -p $PORT -V mqttv5 -t v5s -q 1 -C 1 -W 1 -i v5sess -c -x 60 > /dev/null 2>&1
mosquitto_pub -p $PORT -V mqttv5 -t v5s -m queued -q 1
check "MQTT 5 session expiry interval" "queued" "$(sub -V mqttv5 -t v5s -q 1 -C 1 -W 2 -i v5sess -c -x 60)"
mosquitto_pub -p $PORT -V mqttv5 -t v5r -m kept -r -D publish message-expiry-interval 1
sleep 2.2
check "MQTT 5 message expiry of a retained message" "" "$(sub -V mqttv5 -t v5r -C 1 -W 1 | grep -v Timed)"
stop

start --tls-cert tests/data/cert.pem --tls-key tests/data/key.pem --tls-port $((PORT + 1))
mosquitto_sub -p $((PORT + 1)) --cafile tests/data/ca.pem -t tls/x -C 1 -W 3 > "$DIR/tls" & S=$!
sleep 0.3
pub -t tls/x -m "through TLS"
wait $S
check "TLS subscriber, TCP publisher" "through TLS" "$(cat "$DIR/tls")"
sub -t tls/y -C 1 -W 3 > "$DIR/tls2" & S=$!
sleep 0.3
mosquitto_pub -p $((PORT + 1)) --cafile tests/data/ca.pem -t tls/y -q 1 -m "from TLS"
wait $S
check "TLS publisher, TCP subscriber" "from TLS" "$(cat "$DIR/tls2")"
stop

start --persist-dir "$DIR/db"
pub -t keep/me -r -q 1 -m persisted
sub -t "later/#" -q 1 -c -i keeper -C 1 -W 1 > /dev/null # a persistent session, offline
pub -t later/x -q 1 -m "queued before restart"
stop # SIGTERM: snapshots written
start --persist-dir "$DIR/db"
check "retained message after restart" "persisted" "$(sub -t 'keep/#' -C 1 -W 3)"
check "session queue after restart" "queued before restart" "$(sub -t 'later/#' -q 1 -c -i keeper -C 1 -W 3)"
stop

printf 'alice:wonder\n' > "$DIR/passwd"
printf 'user alice\ntopic readwrite a/#\n' > "$DIR/acl"
start --passwd "$DIR/passwd" --acl "$DIR/acl" --allow-anonymous false
check "anonymous refused" "1" "$(pub -t a/x -m x 2>&1 | grep -ci 'not authori')"
check "bad password refused" "1" "$(pub -t a/x -m x -u alice -P nope 2>&1 | grep -ci 'bad user name or password\|not authori')"
sub -t "a/#" -u alice -P wonder -C 1 -W 3 > "$DIR/auth" & S=$!
sleep 0.3
pub -t a/x -m allowed -u alice -P wonder
pub -t b/x -m denied -u alice -P wonder
wait $S
check "authenticated publish" "allowed" "$(cat "$DIR/auth")"
stop

exit $fail
