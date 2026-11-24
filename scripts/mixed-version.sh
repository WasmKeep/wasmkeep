#!/usr/bin/env bash
# The mixed-version test (a release-candidate step, docs/releasing.md):
# three nodes on this machine, against moto as the tutorials and the S3
# suites run it, start on the published 0.7.0 binary, then move one node at
# a time to the binary built from this checkout (drain, stop, swap, start,
# wait for ready and `cluster status`), and finally move one node back to
# 0.7.0. After each step it checks, through every node: the durable counter
# keeps every acknowledged write, `actors.call` works between every pair of
# nodes (and so between versions, both ways), alarms armed before the step
# fire once, secrets and env files are readable, and WebSocket room clients
# receive every acknowledged publish, live or after catching up. It also
# probes what 0.8 changed: the signature start floor (#266), request head
# and call limits on hops (#268), the placement of a room and its pusher
# and its refusal header (#261, #265), the pusher's stored idle position
# read by a room built with the 0.7.0 SDK (#274), databases written by 0.7
# opened by 0.8 (#263) and the env file mode rule (#270).
#
# This is a test, not a supported procedure: WasmKeep supports nodes of one
# version per cluster (docs/cluster.md, "Upgrading WasmKeep").
#
# Needs gh (signed in), cargo with wasm32-wasip2, node (with a global
# WebSocket, 22 or later), curl, jq, and uv (for uvx) or moto_server. It
# downloads the 0.7.0 release once into target/mixed-version/cache/ and
# checks its SHA-256 against the release's SHA256SUMS and the value pinned
# below. Everything it writes stays under target/mixed-version/ (or
# MIXED_VERSION_DIR); the run's data is removed at the end unless
# MIXED_VERSION_KEEP=1. Nodes and moto are stopped by the PIDs this script
# started, never by name.
#
# Exit status: 0 when every upgrade check passed; 1 otherwise. What the
# downgrade step finds is reported (INFO lines) and never changes it.
#
# Usage: scripts/mixed-version.sh
#   MIXED_VERSION_PORT_BASE (default 18100): client ports BASE+1..3,
#   cluster ports BASE+11..13, metrics BASE+21..23, moto BASE+50, and
#   BASE+60 for the env file probe.
set -euo pipefail
cd "$(dirname "$0")/.."
root=$PWD

OLD_VERSION=v0.7.0
OLD_ASSET=wasmkeep-0.7.0-x86_64-unknown-linux-gnu.tar.gz
OLD_SHA256=3dda0dfebc23428fff34779515b83bae38327923ffb7755b7a3c97872e5b469b
MOTO_VERSION=5.2.5
APP=mv/test
BUCKET=s3://mixed-version
NODES=(a b c)
base=${MIXED_VERSION_PORT_BASE:-18100}
mv_dir=${MIXED_VERSION_DIR:-$root/target/mixed-version}
cache=$mv_dir/cache
run=$mv_dir/run
started_at=$SECONDS

[ "$(uname -sm)" = "Linux x86_64" ] || { echo "the 0.7.0 binary used here is for Linux x86_64" >&2; exit 1; }
for tool in gh cargo node curl jq; do
  command -v "$tool" >/dev/null || { echo "needs $tool" >&2; exit 1; }
done

declare -A port cport mport pid bin
i=1
for n in "${NODES[@]}"; do
  port[$n]=$((base + i)); cport[$n]=$((base + 10 + i)); mport[$n]=$((base + 20 + i)); i=$((i + 1))
done
moto_port=$((base + 50))
for p in "${port[@]}" "${cport[@]}" "${mport[@]}" "$moto_port" $((base + 60)); do
  if (exec 3<>"/dev/tcp/127.0.0.1/$p") 2>/dev/null; then
    echo "port $p is in use: set MIXED_VERSION_PORT_BASE" >&2; exit 1
  fi
done

# ---------------------------------------------------------------- report
report=$mv_dir/report.txt
fails=0
phase=upgrade
log() { printf '%s %s\n' "$(date -u +%H:%M:%S)" "$*" | tee -a "$report"; }
pass() { log "PASS [$step] $*"; }
note() { log "NOTE [$step] $*"; }
fail() {
  if [ "$phase" = downgrade ]; then log "INFO [$step] (downgrade) $*"
  else log "FAIL [$step] $*"; fails=$((fails + 1)); fi
}
step=setup

# ---------------------------------------------------------------- binaries
old_dir=$cache/$OLD_VERSION
old_bin=$old_dir/${OLD_ASSET%.tar.gz}/wasmkeep
if [ ! -x "$old_bin" ]; then
  mkdir -p "$old_dir"
  gh release download "$OLD_VERSION" --repo WasmKeep/wasmkeep --pattern "$OLD_ASSET" \
    --pattern SHA256SUMS --dir "$old_dir" --clobber
  (cd "$old_dir" && sha256sum --ignore-missing --strict -c SHA256SUMS)
  echo "$OLD_SHA256  $old_dir/$OLD_ASSET" | sha256sum --strict -c -
  tar -xzf "$old_dir/$OLD_ASSET" -C "$old_dir"
fi
cargo build --release --locked -p wasmkeep-cli
new_bin=$root/target/release/wasmkeep
old_label=$("$old_bin" --version | cut -d' ' -f2)
new_label=$("$new_bin" --version | cut -d' ' -f2)
[ "$old_label" != "$new_label" ] || { echo "both binaries report $old_label" >&2; exit 1; }
label() { if [ "$1" = "$old_bin" ]; then echo "0.7.0"; else echo "main"; fi; }

# ---------------------------------------------------------------- guests
# The app as built for 0.7.0: the room classes and the front from the
# v0.7.0 tree (the 0.7.0 SDK); test-actor from this checkout (the WIT is
# unchanged since 0.7.0, and its probes are newer). The pusher is later
# redeployed from this checkout's SDK alone, against the 0.7.0 room (#274).
# Outside this checkout: inside it, the old crates would inherit this
# workspace's version (version.workspace) instead of their own.
old_src=${MIXED_VERSION_OLD_SRC:-${XDG_CACHE_HOME:-$HOME/.cache}/wasmkeep-mixed-version/src-$OLD_VERSION}
if [ ! -f "$old_src/.complete" ]; then
  rm -rf "$old_src"; mkdir -p "$old_src"
  git archive "$OLD_VERSION" crates/wasmkeep-sdk crates/wasmkeep-wit \
    crates/wasmkeep-server/tests/guests/room-scaling \
    crates/wasmkeep-server/tests/guests/room-scaling-front rust-toolchain.toml \
    | tar -x -C "$old_src"
  # The old crates inherit their version and settings from the old
  # workspace root: keep it, with only the members extracted here.
  git show "$OLD_VERSION:Cargo.toml" | python3 -c '
import re, sys
s = sys.stdin.read()
s = re.sub(r"(?ms)^members\s*=\s*\[.*?\]", "members = [\"crates/wasmkeep-sdk\", \"crates/wasmkeep-wit\"]", s, count=1)
s = re.sub(r"(?ms)^default-members\s*=\s*\[.*?\]\n?", "", s)
sys.stdout.write(s)' > "$old_src/Cargo.toml"
  touch "$old_src/.complete"
fi
guest() { (cd "$1" && cargo build -j 4 --release --locked --target wasm32-wasip2 -q); }
guest "$old_src/crates/wasmkeep-server/tests/guests/room-scaling"
guest "$old_src/crates/wasmkeep-server/tests/guests/room-scaling-front"
guest crates/wasmkeep-server/tests/guests/room-scaling
guest crates/wasmkeep-server/tests/guests/test-actor
rel=target/wasm32-wasip2/release
old_room=$old_src/crates/wasmkeep-server/tests/guests/room-scaling/$rel/room_scaling_guest.wasm
old_front=$old_src/crates/wasmkeep-server/tests/guests/room-scaling-front/$rel/room_scaling_front.wasm
new_room=$root/crates/wasmkeep-server/tests/guests/room-scaling/$rel/room_scaling_guest.wasm
test_actor=$root/crates/wasmkeep-server/tests/guests/test-actor/$rel/test_actor.wasm

# ---------------------------------------------------------------- the run
rm -rf "$run"; mkdir -p "$run"; chmod 700 "$run"
: > "$report"
moto_pid=; clients_pid=; probe_pid=; bg_pid=
cleanup() {
  local n
  for p in $probe_pid $bg_pid $clients_pid; do kill "$p" 2>/dev/null || true; done
  for n in "${NODES[@]}"; do
    [ -z "${pid[$n]:-}" ] || kill -9 "${pid[$n]}" 2>/dev/null || true
  done
  # SIGTERM, which uvx passes on to moto (a SIGKILL would leave it running).
  [ -z "$moto_pid" ] || kill "$moto_pid" 2>/dev/null || true
  wait 2>/dev/null || true
  # Kept for a look when asked, or when an upgrade check failed.
  if [ "${MIXED_VERSION_KEEP:-0}" = 1 ] || [ "${fails:-0}" -gt 0 ]; then
    echo "kept $run" >&2
  else rm -rf "$run"; fi
}
trap cleanup EXIT
trap 'exit 143' TERM INT

if command -v moto_server >/dev/null; then moto=(moto_server)
elif command -v uvx >/dev/null; then moto=(uvx --quiet --from "moto[server]==$MOTO_VERSION" moto_server)
else echo "no S3 server: install uv (for uvx) or moto[server]==$MOTO_VERSION" >&2; exit 1
fi
"${moto[@]}" -p "$moto_port" >"$run/moto.log" 2>&1 &
moto_pid=$!
for k in $(seq 1 120); do
  curl -sf -o /dev/null "http://127.0.0.1:$moto_port/moto-api/" && break
  kill -0 "$moto_pid" 2>/dev/null && [ "$k" -lt 120 ] || { cat "$run/moto.log" >&2; exit 1; }
  sleep 1
done
curl -sf -X PUT -H 'Content-Length: 0' "http://127.0.0.1:$moto_port/mixed-version" >/dev/null
export AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_REGION=us-east-1 \
  AWS_ENDPOINT=http://127.0.0.1:$moto_port AWS_ALLOW_HTTP=true

# Env file and secrets key in the documented layout: mode 600, the
# service's own (docs/deploy/three-nodes.md, upgrading-to-0.8.md).
mkdir -m 700 "$run/etc"
# chmod as well as the umask: a default ACL on the directory wins over it.
(umask 077; printf 'MV_ENV=from-env-file\n' > "$run/etc/t.env"); chmod 600 "$run/etc/t.env"
"$old_bin" secrets new-key --out "$run/etc/secrets.key" >/dev/null

old() { "$old_bin" "$@"; }
new() { "$new_bin" "$@"; }

start_node() { # start_node ID BINARY
  local n=$1 b=$2
  mkdir -p "$run/$n/data" "$run/$n/cache"; chmod 700 "$run/$n" "$run/$n/data" "$run/$n/cache"
  echo "=== $(date -u +%T) start $(label "$b") ($("$b" --version))" >> "$run/$n.log"
  "$b" serve --app "$APP" --bucket "$BUCKET" --node-id "$n" \
    --data-dir "$run/$n/data" --cache-dir "$run/$n/cache" \
    --listen "127.0.0.1:${port[$n]}" --advertise "http://127.0.0.1:${port[$n]}" \
    --cluster-listen "127.0.0.1:${cport[$n]}" --cluster-advertise "https://127.0.0.1:${cport[$n]}" \
    --metrics-listen "127.0.0.1:${mport[$n]}" \
    --env "t=$run/etc/t.env" --secrets-key "$run/etc/secrets.key" \
    >> "$run/$n.log" 2>&1 &
  pid[$n]=$!; bin[$n]=$b
}
wait_ready() { # wait_ready ID SECS
  local n=$1 deadline=$((SECONDS + ${2:-90}))
  until curl -sf -o /dev/null "http://127.0.0.1:${port[$n]}/_wasmkeep/ready"; do
    if ! kill -0 "${pid[$n]}" 2>/dev/null; then
      fail "node $n ($(label "${bin[$n]}")) exited before ready: $(tail -n 5 "$run/$n.log" | tr '\n' ' ')"
      return 1
    fi
    [ $SECONDS -lt $deadline ] || { fail "node $n not ready after ${2:-90}s"; return 1; }
    sleep 0.5
  done
}
stop_node() { # SIGTERM, a clean stop; SIGKILL after 60 s, reported
  local n=$1 deadline=$((SECONDS + 60))
  kill -TERM "${pid[$n]}"
  while kill -0 "${pid[$n]}" 2>/dev/null; do
    if [ $SECONDS -ge $deadline ]; then
      fail "node $n did not stop within 60 s of SIGTERM; killed"
      kill -9 "${pid[$n]}"; break
    fi
    sleep 0.5
  done
  wait "${pid[$n]}" 2>/dev/null || true
  pid[$n]=
}
status_json() { new cluster status --bucket "$BUCKET" --json 2>/dev/null || true; }
# Wait until node N reports STATE (serving, drained) in `cluster status`.
wait_state() { # wait_state ID STATE SECS
  local n=$1 want=$2 deadline=$((SECONDS + ${3:-60})) got=
  while [ $SECONDS -lt $deadline ]; do
    got=$(new cluster status --bucket "$BUCKET" 2>/dev/null | awk -v n="$n" '$1 == n && $2 == "yes" { print $4 }')
    [ "$got" = "$want" ] && return 0
    sleep 1
  done
  fail "node $n is '${got:-not live}', not $want, after ${3:-60}s"
  return 1
}

# curl with the status first, then the x-wasmkeep-* headers, then the body.
req() { # req METHOD NODE PATH [curl args...]
  local m=$1 n=$2 p=$3; shift 3
  : > "$run/resp.head"; : > "$run/resp.body"
  curl -s -m 30 -X "$m" -D "$run/resp.head" -o "$run/resp.body" "$@" "http://127.0.0.1:${port[$n]}$p" >/dev/null || true
  RS=$(awk 'NR == 1 { print $2 }' "$run/resp.head"); RS=${RS:-000}
  RNODE=$(awk -F': ' 'tolower($1) == "x-wasmkeep-node" { gsub("\r", "", $2); print $2 }' "$run/resp.head")
  RB=$(cat "$run/resp.body")
}
# Retry a request on 503/000 (a hand-over, a node restarting), at most SECS.
req_retry() { # req_retry SECS METHOD NODE PATH [args...]
  local secs=$1; shift
  local deadline=$((SECONDS + secs))
  while :; do
    req "$@"
    case $RS in 503|000|502) ;; *) return 0 ;; esac
    # Every answer retried is kept: a refusal must not hide behind a retry.
    printf '%s %s via %s (%s): %s %s\n' "$(date -u +%T.%3N)" "$1 $3" "$2" \
      "$(label "${bin[$2]:-}")" "$RS" "$(head -c 100 "$run/resp.body" | tr '\n' ' ')" >> "$run/retries-$step.txt"
    [ $SECONDS -lt $deadline ] || return 0
    sleep 0.5
  done
}
retries() { # the answers retried in this step, counted by status
  [ -s "$run/retries-$step.txt" ] || return 0
  note "retried: $(awk '{ print $7 }' "$run/retries-$step.txt" | sort | uniq -c | tr -s ' ' | tr '\n' ' ')(first: $(head -1 "$run/retries-$step.txt" | cut -c1-160))"
}
holder() { # holder CLASS/NAME: the node owning it, or -
  new actor where --app "$APP" --bucket "$BUCKET" --json "$1" 2>/dev/null \
    | jq -r '.[0].owner // "-"' 2>/dev/null || echo -
}
# Have node N hold test-actor NAME: an actor move, checked through the
# answer header of a read.
pin_actor() { # pin_actor NAME ID
  local a=$1 n=$2 deadline=$((SECONDS + 40))
  new actor move --app "$APP" --bucket "$BUCKET" "t/$a" --to "$n" >/dev/null 2>&1 || true
  while [ $SECONDS -lt $deadline ]; do
    req GET "$n" "/actor/t/$a/fired"
    [ "$RNODE" = "$n" ] && return 0
    sleep 1
  done
  fail "t/$a is not on $n after a move (answered by '${RNODE}', $RS)"
  return 1
}
pin() { pin_actor "at-$1" "$1"; }

# ---------------------------------------------------------------- checks
ctr=0             # the shared durable counter's acknowledged value
declare -A fired  # alarms armed: actor -> fired count expected
published=0       # the room's highest acknowledged number
secret2=0         # 1 once main's CLI has sealed MV_SECRET2

check_status() {
  local out
  out=$(new cluster status --bucket "$BUCKET" 2>&1 || true)
  printf '%s\n' "$out" > "$run/status-$step.txt"
  old cluster status --bucket "$BUCKET" > "$run/status-old-cli-$step.txt" 2>&1 || true
  local live
  live=$(printf '%s\n' "$out" | grep -Eo '^[0-9]+ of [0-9]+ nodes live' || true)
  if [ "$live" = "3 of 3 nodes live" ]; then pass "cluster status: $live"
  else fail "cluster status: '${live:-no summary}'"; fi
  for n in "${NODES[@]}"; do
    local line v
    line=$(printf '%s\n' "$out" | awk -v n="$n" '$1 == n')
    # The VERSION column, by the header (live rows only: LIVE is one word).
    v=$(printf '%s\n' "$out" | awk -v n="$n" '
      $1 == "NODE" { for (i = 1; i <= NF; i++) if ($i == "VERSION") col = i }
      col && $1 == n && $2 == "yes" { print $col }')
    local want; if [ "${bin[$n]}" = "$old_bin" ]; then want=$old_label; else want=$new_label; fi
    if [ "$v" = "$want" ] && printf '%s\n' "$line" | grep -q ' serving '; then
      pass "status: $n serving $(label "${bin[$n]}") ($v)"
    else fail "status: $n: '$line' (want serving $want)"; fi
  done
  local warn; warn=$(printf '%s\n' "$out" | grep -i 'warn\|different' | tr '\n' ' ' || true)
  [ -z "$warn" ] || note "status warnings: $warn"
  if ! diff -q <(sed 's/[0-9.]*m\?s / /g' "$run/status-$step.txt" | awk '{print $1, $2}') \
       <(sed 's/[0-9.]*m\?s / /g' "$run/status-old-cli-$step.txt" | awk '{print $1, $2}') >/dev/null; then
    note "the 0.7.0 CLI's status differs: $(head -c 400 "$run/status-old-cli-$step.txt" | tr '\n' ' ')"
  fi
}

check_counter() {
  local n
  for n in "${NODES[@]}"; do
    req_retry 20 POST "$n" /actor/t/ctr/durable
    if [ "$RS" = 200 ] && [ "$RB" = $((ctr + 1)) ]; then
      ctr=$RB; pass "counter: POST via $n ($(label "${bin[$n]}")) -> $RB on ${RNODE}"
    elif [ "$RS" = 200 ]; then
      fail "counter: POST via $n answered $RB, expected $((ctr + 1)) (a lost durable write?)"
      ctr=$RB
    else fail "counter: POST via $n: $RS $RB"; fi
  done
  for n in "${NODES[@]}"; do
    req_retry 20 GET "$n" /actor/t/ctr/durable
    if [ "$RS" = 200 ] && [ "$RB" = "$ctr" ]; then pass "counter: GET via $n -> $RB"
    else fail "counter: GET via $n: $RS '$RB', expected $ctr"; fi
  done
}

check_calls() {
  local i j seen_old_new=0 seen_new_old=0
  for n in "${NODES[@]}"; do pin "$n" || true; done
  for i in "${NODES[@]}"; do
    for j in "${NODES[@]}"; do
      [ "$i" != "$j" ] || continue
      req_retry 20 POST "$i" "/actor/t/at-$i/call?to=t/at-$j&path=%2Fdurable"
      local dir; dir="$i($(label "${bin[$i]}")) -> $j($(label "${bin[$j]}"))"
      if [ "$RS" = 200 ] && [[ $RB == 200:* ]] && [ "$RNODE" = "$i" ]; then
        pass "actors.call $dir: $RB"
        [ "${bin[$i]}" = "$old_bin" ] && [ "${bin[$j]}" = "$new_bin" ] && seen_old_new=1
        [ "${bin[$i]}" = "$new_bin" ] && [ "${bin[$j]}" = "$old_bin" ] && seen_new_old=1
      else
        fail "actors.call $dir: $RS '$RB' (caller ran on '${RNODE}')"
      fi
    done
  done
  local olds=0 news=0
  for n in "${NODES[@]}"; do if [ "${bin[$n]}" = "$old_bin" ]; then olds=1; else news=1; fi; done
  if [ $olds = 1 ] && [ $news = 1 ]; then
    if [ $seen_old_new = 1 ]; then pass "actors.call 0.7.0 -> main exercised"; else fail "no successful 0.7.0 -> main call"; fi
    if [ $seen_new_old = 1 ]; then pass "actors.call main -> 0.7.0 exercised"; else fail "no successful main -> 0.7.0 call"; fi
  fi
}

check_secrets() {
  local n
  for n in "${NODES[@]}"; do
    req_retry 20 GET "$n" "/actor/t/at-$n/env?name=MV_SECRET"
    if [ "$RS" = 200 ] && [ "$RB" = "sealed-value wasi=unset" ]; then pass "secret via $n on ${RNODE}"
    else fail "secret via $n on '${RNODE}': $RS '$RB'"; fi
    req_retry 20 GET "$n" "/actor/t/at-$n/env?name=MV_ENV"
    if [ "$RS" = 200 ] && [ "$RB" = "from-env-file wasi=unset" ]; then pass "env file via $n on ${RNODE}"
    else fail "env file via $n on '${RNODE}': $RS '$RB'"; fi
  done
}

check_secret2() { # the secret main's CLI sealed, on every node
  local n
  for n in "${NODES[@]}"; do
    req_retry 20 GET "$n" "/actor/t/at-$n/env?name=MV_SECRET2"
    if [ "$RS" = 200 ] && [ "$RB" = "main-sealed wasi=unset" ]; then
      pass "secret sealed by main's CLI via $n ($(label "${bin[$n]}")) on ${RNODE}"
    else fail "secret sealed by main's CLI via $n ($(label "${bin[$n]}")) on '${RNODE}': $RS '$RB'"; fi
  done
}

# Alarms armed on actors held by each node (moved there, and armed through
# that node, which must answer), one to fire during the next hand-over and
# one after the restart; each must fire exactly once. Every alarm armed is
# read again at the end of the run, for a late second run.
declare -A every_alarm
arm_alarms() { # arm_alarms TAG DELAY_MS
  local n
  for n in "${NODES[@]}"; do
    local a="al-$1-$n"
    pin_actor "$a" "$n" || continue
    req_retry 20 POST "$n" "/actor/t/$a/alarm?in=$2"
    if [ "$RS" = 200 ] && [ "$RNODE" = "$n" ]; then fired[$a]=1; every_alarm[$a]=$n
    else fail "arming $a on $n: $RS $RB (ran on '${RNODE}')"; fi
  done
}
check_alarms() { # check_alarms [WAIT_SECS]
  local a
  for a in "${!fired[@]}"; do
    local deadline=$((SECONDS + ${1:-60})) f="" at=""
    while :; do
      req_retry 20 GET a "/actor/t/$a/fired"; f=$RB
      [ "$f" = "${fired[$a]}" ] && break
      [ $SECONDS -lt $deadline ] || break
      sleep 1
    done
    req_retry 20 GET a "/actor/t/$a/attempts"; at=$RB
    if [ "$f" = "${fired[$a]}" ] && [ "$at" = "${fired[$a]}" ]; then
      pass "alarm $a, armed on ${every_alarm[$a]}, fired once (read on ${RNODE})"
    else fail "alarm $a, armed on ${every_alarm[$a]}: fired '$f', attempts '$at', expected ${fired[$a]}"; fi
    unset "fired[$a]"
  done
}

client_state() { cat "$run/clients"/*.json 2>/dev/null | jq -s '.' 2>/dev/null; }
pubs=0            # publishes sent, for their ids (the room dedupes by id)
publish() { # publish K messages, through each node in turn
  local k
  for k in $(seq 1 "$1"); do
    local n=${NODES[$((k % 3))]}
    pubs=$((pubs + 1))
    req_retry 30 GET "$n" "/r6/publish?room=quiet&id=mv-$pubs"
    if [ "$RS" = 200 ] && [[ $RB =~ ^[0-9]+$ ]] && [ "$RB" -gt "$published" ]; then
      published=$RB
    else fail "room publish mv-$pubs via $n: $RS '$RB' (last acknowledged $published)"; fi
  done
}
# What placement did on the main nodes (#261, #265): apart and spread
# wake-ups, and placed wake-ups received (refused ones included).
check_placement() {
  local n
  for n in "${NODES[@]}"; do
    [ "${bin[$n]}" = "$new_bin" ] || continue
    note "node $n placement: $(curl -s "http://127.0.0.1:${mport[$n]}/metrics" \
      | grep -E '^wasmkeep_placement(s_received|_apart|_spread)_total' \
      | sed -E 's/\{tenant="[^"]*",app="[^"]*",?/{/; s/\{\}//' | tr '\n' ' ')"
  done
}
check_room() { # check_room WHAT: every client holds every acknowledged number
  # Missing: acknowledged numbers a client was never handed, live or by
  # the history.
  local deadline=$((SECONDS + 45)) missing
  while :; do
    missing=$(client_state | jq -r --argjson h "$published" \
      '[.[] | (([range(1; $h + 1)] - .have) as $m | select($m | length > 0) | "\(.id) lacks \($m | .[0:5] | map(tostring) | join(","))")] | join("; ")' \
      || echo "no client state")
    [ -z "$missing" ] && break
    [ $SECONDS -lt $deadline ] || break
    sleep 1
  done
  local count problems
  count=$(client_state | jq length 2>/dev/null || echo 0)
  problems=$(client_state | jq -r '[.[] | .problems[] ] | unique | join("; ")' || true)
  if [ -z "$missing" ] && [ "$count" -gt 0 ]; then pass "room ($1): all $count clients hold 1..$published"
  else fail "room ($1): of 1..$published: ${missing:-no clients}"; fi
  [ -z "$problems" ] || fail "room ($1): $problems"
  note "room clients: $(client_state | jq -c '{epochs: ([.[].epochs[]] | unique), live: ([.[].live] | add), fetched: ([.[].fetched] | add), repeats: ([.[].repeats] | add), fetches: ([.[].fetches] | add), historyRefused: ([.[].historyRefused] | add), historyStartRefused: ([.[].historyStartRefused] | add), reconnects: ([.[].reconnects] | add), closes: ([.[].closes | to_entries[]] | group_by(.key) | map({(.[0].key): (map(.value) | add)}) | add)}' || true)"
}
# Where the room, its pusher and its edges are (#261, #265).
check_apart() {
  local r p e where=""
  r=$(holder room/quiet); p=$(holder pusher/quiet)
  if [ "$r" != - ] && [ "$r" = "$p" ]; then note "room/quiet and pusher/quiet both on $r"
  else note "room/quiet on $r, pusher/quiet on $p"; fi
  req GET a "/r6/map?room=quiet"
  for e in ${RB//,/ }; do where="$where ${e#*/}@$(holder "$e")"; done
  note "room/quiet edges:${where:- none ($RS $RB)}"
}
check_all() {
  check_status
  check_counter
  check_calls
  check_secrets
  [ "$secret2" = 0 ] || check_secret2
  publish 3
  check_room "live"
  check_apart
  check_placement
}

# #266: right after a node starts, wake-ups through its peers (fresh actors,
# so placement may send them to the new, empty node) and calls into it.
start_probe() { # start_probe ID
  local n=$1 out=$run/probe-$step.txt
  (
    end=$((SECONDS + 30)); k=0
    while [ $SECONDS -lt $end ]; do
      for m in "${NODES[@]}"; do
        [ "$m" != "$n" ] || continue
        k=$((k + 1))
        code=$(curl -s -m 10 -o "$run/probe-b-$n" -D "$run/probe-h-$n" -w '%{http_code}' -X POST \
          "http://127.0.0.1:${port[$m]}/actor/t/probe-$step-$k/durable" || echo 000)
        if [ "$code" != 200 ]; then
          echo "$(date -u +%T.%3N) via $m: $code $(grep -i '^x-wasmkeep\|^retry' "$run/probe-h-$n" | tr -d '\r' | tr '\n' ' ') $(head -c 120 "$run/probe-b-$n")"
        else
          echo "$(date -u +%T.%3N) via $m: 200 on $(awk -F': ' 'tolower($1)=="x-wasmkeep-node"{print $2}' "$run/probe-h-$n" | tr -d '\r')"
        fi
      done
      sleep 0.2
    done
  ) > "$out" 2>&1 &
  probe_pid=$!
}
end_probe() {
  wait "$probe_pid" 2>/dev/null || true; probe_pid=
  local out=$run/probe-$step.txt bad
  bad=$(grep -v ': 200 on' "$out" || true)
  local total; total=$(wc -l < "$out")
  local placed; placed=$(grep -c ": 200 on $1\$" "$out" || true)
  if [ -z "$bad" ]; then pass "start probe: $total wake-ups through peers all 200 ($placed ran on $1)"
  else fail "start probe: $(printf '%s\n' "$bad" | wc -l) of $total not 200: $(printf '%s\n' "$bad" | head -5 | tr '\n' '|')"; fi
  local floor; floor=$(curl -s "http://127.0.0.1:${mport[$1]}/metrics" 2>/dev/null | grep '^wasmkeep_signatures_before_start_total' || true)
  [ -z "$floor" ] || note "node $1: $floor"
}

# Publishes through the other nodes, one a second, while node N is
# drained, stopped and started again: the clients connected through N are
# away for some of them and must catch up. Only acknowledged numbers count.
bg_pub_start() { # bg_pub_start ID
  local n=$1 out=$run/bg-$step.txt
  (
    k=0
    while :; do
      for m in "${NODES[@]}"; do
        [ "$m" != "$n" ] || continue
        k=$((k + 1))
        code=$(curl -s -m 10 -o "$run/bg-body" -w '%{http_code}' \
          "http://127.0.0.1:${port[$m]}/r6/publish?room=quiet&id=bg-$step-$k" || echo 000)
        echo "$code $(head -c 60 "$run/bg-body" | tr '\n' ' ')"
        sleep 0.5
      done
    done
  ) > "$out" 2>&1 &
  bg_pid=$!
}
bg_pub_stop() {
  kill "$bg_pid" 2>/dev/null; wait "$bg_pid" 2>/dev/null; bg_pid=
  local out=$run/bg-$step.txt acked max
  acked=$(awk '$1 == 200 && $2 ~ /^[0-9]+$/' "$out" | wc -l)
  max=$(awk '$1 == 200 && $2 ~ /^[0-9]+$/ { if ($2 > m) m = $2 } END { print m + 0 }' "$out")
  [ "$max" -le "$published" ] || published=$max
  note "publishes during the step: $acked acknowledged, others $(awk '$1 != 200 { print $1 }' "$out" | sort | uniq -c | tr -s ' ' | tr '\n' ' ')(highest $max)"
}

# Drain, stop, swap, start, ready, undrain, status.
swap() { # swap ID BINARY
  local n=$1 b=$2 from; from=$(label "${bin[$n]}")
  log "---- step $step: node $n $from -> $(label "$b")"
  arm_alarms "$step-after" 45000
  arm_alarms "$step-handover" 6000
  bg_pub_start "$n"
  if ! new cluster drain "$n" --bucket "$BUCKET" >/dev/null; then fail "drain $n refused"; fi
  wait_state "$n" drained 90 || true
  stop_node "$n"
  # Withdrawn before the start (--force: no live node has the id), so the
  # new process takes actors, and placed wake-ups, from its first heartbeat.
  new cluster undrain "$n" --bucket "$BUCKET" --force >/dev/null 2>&1 || fail "undrain $n refused"
  local t0=$SECONDS
  start_node "$n" "$b"
  until (exec 3<>"/dev/tcp/127.0.0.1/${port[$n]}") 2>/dev/null; do
    kill -0 "${pid[$n]}" 2>/dev/null || break; sleep 0.1
  done
  start_probe "$n"
  if wait_ready "$n" 90; then pass "node $n ($(label "$b")) ready $((SECONDS - t0))s after its start"
  else end_probe "$n"; bg_pub_stop; return 1; fi
  wait_state "$n" serving 60 || true
  end_probe "$n"
  bg_pub_stop
  check_all
  check_alarms
  retries
}

# ---------------------------------------------------------------- 0. all 0.7.0
# From here on a failed command is a check's to report, not a reason to stop.
set +e
step=deploy
# The 0640 env file a 0.7.0 install may have (owned by the service user,
# group-readable), the rule #270 added: main must refuse it, 0.7.0 starts.
(umask 027; printf 'MV_ENV=x\n' > "$run/etc/t-0640.env"); chmod 640 "$run/etc/t-0640.env"
mkdir -m 700 "$run/probe-env"
envprobe() { # envprobe BINARY: the exit status within 5 s (124: still running)
  local b=$1 p rc=124
  "$b" serve --actor "t=$test_actor" --env "t=$run/etc/t-0640.env" --data-dir "$run/probe-env/$(label "$b")" \
    --listen 127.0.0.1:$((base + 60)) > "$run/probe-env/$(label "$b").log" 2>&1 &
  p=$!
  for _ in $(seq 1 50); do kill -0 $p 2>/dev/null || break; sleep 0.1; done
  if kill -0 $p 2>/dev/null; then kill $p; wait $p 2>/dev/null || true
  else wait $p 2>/dev/null && rc=0 || rc=$?; fi
  echo $rc
}
rc_old=$(envprobe "$old_bin"); rc_new=$(envprobe "$new_bin")
if [ "$rc_old" = 124 ] && [ "$rc_new" != 124 ] && [ "$rc_new" != 0 ]; then
  pass "env file 0640 owner-readable: 0.7.0 starts, main refuses (exit $rc_new: $(grep -i 'env' "$run/probe-env/main.log" | head -1 | cut -c1-200))"
else
  fail "env file 0640: 0.7.0 exit '$rc_old', main exit '$rc_new' (want 124 then non-zero): $(tail -n 2 "$run/probe-env/main.log" | tr '\n' ' ')"
fi

old deploy --no-project --app "$APP" --bucket "$BUCKET" \
  --actor "t=$test_actor" \
  --actor "room=$old_room" --actor "pusher=$old_room" --actor "edge=$old_room" \
  --actor "directory=$old_room" --actor "relay=$old_room" --front "$old_front" \
  --access room=internal --access pusher=internal --access edge=internal \
  --access directory=internal --access relay=internal --writes pusher=batched \
  > "$run/deploy-1.txt" 2>&1 || { cat "$run/deploy-1.txt"; exit 1; }
printf 'sealed-value\n' | old secrets set --app "$APP" --bucket "$BUCKET" \
  --key "$run/etc/secrets.key" --class t MV_SECRET >/dev/null 2>&1 \
  || { echo "secrets set failed" >&2; exit 1; }

step=start-0.7.0
for n in "${NODES[@]}"; do start_node "$n" "$old_bin"; done
for n in "${NODES[@]}"; do wait_ready "$n" 90 || { tail -n 20 "$run/$n.log"; exit 1; }; done
wait_state a serving 30 || true

mkdir -p "$run/clients"
# Four clients through each node: twelve, past the test room's edge
# capacity of five, so it splits into edges (#265 spreads them).
node scripts/mixed-version/room-clients.mjs "$run/clients" quiet 4 "${port[a]}" "${port[b]}" "${port[c]}" \
  > "$run/clients.log" 2>&1 &
clients_pid=$!
sleep 8
check_all
# The call limits between two 0.7.0 nodes, to compare with the hop below.
for spec in "97 10" "120 10"; do
  read -r hn hv <<< "$spec"
  req_retry 10 POST b "/actor/t/at-b/call?to=t/at-c&path=%2Fdurable&hn=$hn&hv=$hv"
  note "actors.call 0.7.0 -> 0.7.0 with $hn headers of $hv bytes: $RS '$RB'"
done

# The first publish after a quiet spell (#274), all of it 0.7.0: the
# control for the same after main's pusher is deployed. Where the room and
# pusher are just before the publish ("-": hibernated and released).
quiet_publish() {
  note "quiet: 40 s with no publish, so the room, pusher and edges can hibernate"
  sleep 40
  note "before the publish: room/quiet on $(holder room/quiet), pusher/quiet on $(holder pusher/quiet)"
  local before after
  before=$(client_state | jq '[.[] | .fetches] | add' || echo 0)
  publish 1
  check_room "first publish after a quiet spell"
  after=$(client_state | jq '[.[] | .fetches] | add' || echo 0)
  note "history fetches for the first publish after a quiet spell, pusher $1: $((after - before))"
}
step=quiet-0.7.0
quiet_publish "0.7.0 SDK"

# A database written by 0.7.0 (#263), opened by main after the upgrade: a
# table under the prefix 0.8 reserves (0.7.0 may refuse it already), a
# plain one, and one big enough that sorting it is a long statement.
step=sqlite-0.7.0
big_sort="SELECT x FROM big ORDER BY x DESC LIMIT 1"
for sql in "CREATE TABLE _wasmkeep_legacy (x INTEGER)" \
           "CREATE TABLE plain_t (x INTEGER)" "INSERT INTO plain_t VALUES (1)" \
           "CREATE TABLE big (x BLOB)" \
           "WITH RECURSIVE c(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM c WHERE i < 300000) INSERT INTO big SELECT randomblob(16) FROM c" \
           "$big_sort"; do
  req_retry 20 POST a /actor/t/sqlx/sql --data-binary "$sql"
  note "0.7.0 sql '${sql:0:60}' on ${RNODE}: $RS $RB"
done

# ---------------------------------------------------------------- 1..3 upgrade
phase=upgrade
step=upgrade-a; swap a "$new_bin" || exit 1

# #274: the pusher from this checkout's SDK, every other room class from
# 0.7.0's, deployed with main's CLI; then a quiet room left to hibernate.
step=pusher-main-sdk
if new deploy --no-project --app "$APP" --bucket "$BUCKET" \
  --actor "t=$test_actor" \
  --actor "room=$old_room" --actor "pusher=$new_room" --actor "edge=$old_room" \
  --actor "directory=$old_room" --actor "relay=$old_room" \
  > "$run/deploy-2.txt" 2>&1; then
  pass "main CLI deploy on a mixed cluster: $(tail -n 1 "$run/deploy-2.txt")"
else fail "main CLI deploy: $(tr '\n' ' ' < "$run/deploy-2.txt")"; fi
# A secret sealed by main's CLI, read on every node, 0.7.0 ones included.
if printf 'main-sealed\n' | new secrets set --app "$APP" --bucket "$BUCKET" \
  --key "$run/etc/secrets.key" --class t MV_SECRET2 > "$run/secrets-2.txt" 2>&1; then secret2=1
else fail "main CLI secrets set: $(tr '\n' ' ' < "$run/secrets-2.txt")"; fi
sleep 8
check_secret2
publish 2
check_room "after the pusher redeploy"
quiet_publish "main SDK"

# #268: request heads and calls across a 0.7.0 -> main hop (t/at-a on a).
step=limits
pin a || true; pin b || true
hdrs() { local k; for k in $(seq 1 "$1"); do printf -- '-H\nx-mv-%03d: v\n' "$k"; done; }
for count in 90 95 99; do
  mapfile -t h < <(hdrs "$count")
  req GET a /actor/t/at-a/durable "${h[@]}"; direct="$RS"
  req GET b /actor/t/at-a/durable "${h[@]}"; hop="$RS on ${RNODE}"
  note "$count client headers to t/at-a (main): direct $direct, via b (0.7.0) $hop"
  [ "${hop%% *}" = "$direct" ] || fail "$count headers: direct $direct but via 0.7.0 $hop"
done
for spec in "97 10" "120 10" "60 1000"; do
  read -r hn hv <<< "$spec"
  req_retry 10 POST b "/actor/t/at-b/call?to=t/at-a&path=%2Fdurable&hn=$hn&hv=$hv"
  note "actors.call 0.7.0 -> main with $hn headers of $hv bytes: $RS '$RB'"
  req_retry 10 POST a "/actor/t/at-a/call?to=t/at-b&path=%2Fdurable&hn=$hn&hv=$hv"
  note "actors.call main -> 0.7.0 with $hn headers of $hv bytes: $RS '$RB'"
done

step=upgrade-b; swap b "$new_bin" || exit 1
step=upgrade-c; swap c "$new_bin" || exit 1

# #263: the 0.7.0 database, now on a main node.
step=sqlite
new actor move --app "$APP" --bucket "$BUCKET" t/sqlx --to b >/dev/null 2>&1 || true
sleep 3
for sql in "SELECT count(*) FROM plain_t" "INSERT INTO plain_t VALUES (2)" \
           "ALTER TABLE plain_t ADD COLUMN y INTEGER" "SELECT count(*) FROM big" "$big_sort"; do
  req_retry 20 POST b /actor/t/sqlx/sql --data-binary "$sql"
  note "main sql '${sql:0:60}' on ${RNODE}: $RS $RB"
done
req_retry 20 POST b /actor/t/sqlx/sql --data-binary "SELECT count(*) FROM plain_t"
if [ "$RS" = 200 ] && [[ $RB == ok:* ]]; then pass "0.7.0 database read and written on main"
else fail "0.7.0 database on main: $RS $RB"; fi

upgrade_secs=$((SECONDS - started_at))

# ---------------------------------------------------------------- downgrade
phase=downgrade
step=downgrade-c; swap c "$old_bin"

step=end
# Every alarm armed in the run, read again: still exactly one run each.
late=0
for a in "${!every_alarm[@]}"; do
  case $a in *downgrade*) phase=downgrade ;; *) phase=upgrade ;; esac
  req_retry 20 GET a "/actor/t/$a/attempts"
  if [ "$RS" != 200 ] || [ "$RB" != 1 ]; then
    fail "alarm $a (armed on ${every_alarm[$a]}) has run '$RB' times by the end"; late=$((late + 1))
  fi
done
[ "$late" -gt 0 ] || pass "every alarm armed (${#every_alarm[@]}) ran once by the end"
kill "$clients_pid" 2>/dev/null || true; wait "$clients_pid" 2>/dev/null || true; clients_pid=
# What the nodes warned about, by message, per node.
for n in "${NODES[@]}"; do
  sed -nE 's/^[0-9T:.Z-]+ +(WARN|ERROR) +([^ ]+): ([^=]{1,90}).*/\1 \2: \3/p' "$run/$n.log" \
    | sort | uniq -c | sort -rn | head -8 | while read -r line; do note "node $n log: $line"; done
done
log "upgrade checks: $fails failed; upgrade part ${upgrade_secs}s, whole run $((SECONDS - started_at))s"
log "report: $report; node logs: $run/<node>.log (kept with MIXED_VERSION_KEEP=1)"
[ "$fails" -eq 0 ]
