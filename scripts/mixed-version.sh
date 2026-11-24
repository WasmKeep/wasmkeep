#!/usr/bin/env bash
# The mixed-version check (a gate before tagging a release, docs/releasing.md):
# three nodes on this machine, against moto as the tutorials and the S3
# suites run it, start on a published release (OLD_VERSION, v0.8.0 by
# default), then move one node at a time to the binary built from this
# checkout (drain, stop, swap, start, wait for ready and `cluster status`),
# and finally move one node back to the old release. After each step it
# checks, through every node: the durable counter keeps every acknowledged
# write, `actors.call` works between every pair of nodes (and so between
# versions, both ways), alarms armed before the step fire once, secrets and
# env files are readable, WebSocket room clients receive every acknowledged
# publish, live or after catching up, and both versions' `cluster status`
# agree. While the cluster is mixed, both CLIs deploy and seal secrets.
#
# Four paths are checked as PASS or FAIL lines, each failing when its
# precondition was not reached:
# - the signature start floor: no client sees a failure from a node's
#   start; the refusals for their date are counted (observational when
#   none happen, see end_probe);
# - the pusher's stored idle position: after the room and its pusher have
#   both left memory, the first publish makes no client fetch the history,
#   with this checkout's pusher, on a room built with each version's SDK;
# - kept apart: a pusher woken on its room's node is sent on, and a placed
#   wake-up's apart refusal is read by a node of the other version, both
#   ways;
# - a node of this checkout holding the room is drained while clients are
#   connected, and every client keeps every acknowledged number.
# It also probes request head and call limits on hops, databases written by
# the old version and opened by the new one, and the env file mode rule.
#
# This is a test, not a supported procedure: whether a release can be
# installed one node at a time is said on its upgrading page
# (docs/cluster.md, "Upgrading WasmKeep").
#
# Needs gh (signed in), cargo with wasm32-wasip2, node (with a global
# WebSocket, 22 or later), curl, jq, and uv (for uvx) or moto_server. It
# downloads the old release once into target/mixed-version/cache/ and checks
# its SHA-256 against the release's SHA256SUMS and the value pinned below
# (or given). Everything it writes stays under target/mixed-version/ (or
# MIXED_VERSION_DIR); the run's data is removed at the end unless
# MIXED_VERSION_KEEP=1. Nodes and moto are stopped by the PIDs this script
# started, never by name.
#
# Exit status: 0 when every check passed, the move back included; 1 when
# one failed; 2 when the old version cannot be used (no pinned SHA-256).
#
# Usage: scripts/mixed-version.sh [OLD_VERSION]
#   OLD_VERSION (argument or variable, default v0.8.0): the release the
#   nodes start on. A version without a SHA-256 pinned below is refused
#   unless OLD_SHA256 gives the hash of its Linux x86_64 archive (from the
#   release's SHA256SUMS).
#   MIXED_VERSION_PORT_BASE (default 18100): client ports BASE+1..3,
#   cluster ports BASE+11..13, metrics BASE+21..23, moto BASE+50, and
#   BASE+60 for the env file probe.
#   MIXED_VERSION_IDLE_SECS (default 15): the nodes' --idle-timeout-secs,
#   short so that a room and its pusher leave memory within a minute.
set -euo pipefail
cd "$(dirname "$0")/.."
root=$PWD

# The SHA-256 of each known release's wasmkeep-<v>-x86_64-unknown-linux-gnu.tar.gz,
# as its SHA256SUMS asset lists it. Add the next release here.
declare -A PINNED_SHA256=(
  [v0.7.0]=3dda0dfebc23428fff34779515b83bae38327923ffb7755b7a3c97872e5b469b
  [v0.8.0]=966f254da08c68b0ffc5d41018b61aebe188b078d2819a764e6935abaa0a5c0e
)
OLD_VERSION=${1:-${OLD_VERSION:-v0.8.0}}
[[ $OLD_VERSION =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]] || { echo "OLD_VERSION '$OLD_VERSION' is not vX.Y.Z" >&2; exit 2; }
old_short=${OLD_VERSION#v}
OLD_ASSET=wasmkeep-$old_short-x86_64-unknown-linux-gnu.tar.gz
OLD_SHA256=${OLD_SHA256:-${PINNED_SHA256[$OLD_VERSION]:-}}
if [ -z "$OLD_SHA256" ]; then
  echo "no SHA-256 pinned for $OLD_VERSION: add it to PINNED_SHA256, or give" \
    "OLD_SHA256=<the hash of $OLD_ASSET from the release's SHA256SUMS>" >&2
  exit 2
fi
# Whether VERSION is older than 0.8.0, which first refused an env file
# its group can read.
before_080() { [ "$(printf '%s\n0.8.0\n' "$1" | sort -V | head -1)" != 0.8.0 ]; }
MOTO_VERSION=5.2.5
idle_secs=${MIXED_VERSION_IDLE_SECS:-15}
APP=mv/test
BUCKET=s3://mixed-version
NODES=(a b c)
base=${MIXED_VERSION_PORT_BASE:-18100}
mv_dir=${MIXED_VERSION_DIR:-$root/target/mixed-version}
cache=$mv_dir/cache
run=$mv_dir/run
started_at=$SECONDS

[ "$(uname -sm)" = "Linux x86_64" ] || { echo "the $old_short binary used here is for Linux x86_64" >&2; exit 1; }
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
# Per step, in order: its phase, checks passed and failed, and its time.
steps=()
declare -A step_phase step_pass step_fail step_t0 step_secs
log() { printf '%s %s\n' "$(date -u +%H:%M:%S)" "$*" | tee -a "$report"; }
pass() { log "PASS [$step] $*"; step_pass[$step]=$(( ${step_pass[$step]:-0} + 1 )); }
note() { log "NOTE [$step] $*"; }
# Every phase counts, the move back included.
fail() {
  log "FAIL [$step] $*"; fails=$((fails + 1))
  step_fail[$step]=$(( ${step_fail[$step]:-0} + 1 ))
}
begin() { # begin STEP: the steps that follow are counted under it
  [ -z "${step:-}" ] || step_secs[$step]=$(( SECONDS - ${step_t0[$step]:-$SECONDS} ))
  step=$1; steps+=("$1"); step_phase[$1]=$phase; step_t0[$1]=$SECONDS
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
# Checked on every run, a cached archive included: the pin is the gate's.
echo "$OLD_SHA256  $old_dir/$OLD_ASSET" | sha256sum --strict --quiet -c - \
  || { echo "$old_dir/$OLD_ASSET does not match the pinned SHA-256" >&2; exit 2; }
cargo build --release --locked -p wasmkeep-cli
new_bin=$root/target/release/wasmkeep
old_label=$("$old_bin" --version | cut -d' ' -f2)
new_label=$("$new_bin" --version | cut -d' ' -f2)
[ "$old_label" != "$new_label" ] || { echo "both binaries report $old_label" >&2; exit 1; }
label() { if [ "$1" = "$old_bin" ]; then echo "$old_short"; else echo "new"; fi; }

# ---------------------------------------------------------------- guests
# The app as built for the old release: the room classes and the front
# from its tree (its SDK); test-actor from this checkout (its WIT must not
# have changed since, and its probes are newer). The pusher is later
# redeployed from this checkout's SDK alone, against the old room, and then
# every room class from this checkout's SDK.
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
moto_pid=; clients_pid=; small_pid=; probe_pid=; bg_pid=
cleanup() {
  local n
  for p in $probe_pid $bg_pid $clients_pid $small_pid; do kill "$p" 2>/dev/null || true; done
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

# Node b has a smaller instance pool (180 resident actors, the rest the
# front's), so that a few dozen wake-ups put it past placement's floor (a
# fifth of its capacity): the apart refusal needs a sender that places.
declare -A extra=([a]="" [b]="--max-instances 200" [c]="")
start_node() { # start_node ID BINARY
  local n=$1 b=$2
  mkdir -p "$run/$n/data" "$run/$n/cache"; chmod 700 "$run/$n" "$run/$n/data" "$run/$n/cache"
  echo "=== $(date -u +%T) start $(label "$b") ($("$b" --version))" >> "$run/$n.log"
  # shellcheck disable=SC2086 # extra holds whole flags
  "$b" serve --app "$APP" --bucket "$BUCKET" --node-id "$n" \
    --data-dir "$run/$n/data" --cache-dir "$run/$n/cache" \
    --listen "127.0.0.1:${port[$n]}" --advertise "http://127.0.0.1:${port[$n]}" \
    --cluster-listen "127.0.0.1:${cport[$n]}" --cluster-advertise "https://127.0.0.1:${cport[$n]}" \
    --metrics-listen "127.0.0.1:${mport[$n]}" --idle-timeout-secs "$idle_secs" ${extra[$n]} \
    --env "t=$run/etc/t.env" --secrets-key "$run/etc/secrets.key" \
    >> "$run/$n.log" 2>&1 &
  pid[$n]=$!; bin[$n]=$b
}
# The sum of a node's metric NAME over the series whose labels contain
# MATCH (all of them without it).
metric() { # metric ID NAME [MATCH]
  curl -s -m 5 "http://127.0.0.1:${mport[$1]}/metrics" 2>/dev/null | awk -v n="$2" -v m="${3:-}" '
    { split($1, f, "{") } f[1] == n && index($1, m) { s += $NF } END { printf "%d\n", s + 0 }'
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
holder() { # holder CLASS/NAME: the node owning it, - for none, ? when unknown
  new actor where --app "$APP" --bucket "$BUCKET" --json "$1" 2>/dev/null \
    | jq -r '.[0].owner // "-"' 2>/dev/null || echo "?"
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
declare -A sealed # secrets sealed during the run: name -> "value|which CLI"

# `cluster status` from both CLIs: three live, each node serving on the
# version it runs.
check_status() {
  local cli out
  for cli in new old; do
    out=$("$cli" cluster status --bucket "$BUCKET" 2>&1 || true)
    printf '%s\n' "$out" > "$run/status-$cli-cli-$step.txt"
    local live who
    who="$( [ $cli = new ] && echo "new CLI" || echo "$old_short CLI")"
    live=$(printf '%s\n' "$out" | grep -Eo '^[0-9]+ of [0-9]+ nodes live' || true)
    if [ "$live" = "3 of 3 nodes live" ]; then pass "cluster status ($who): $live"
    else fail "cluster status ($who): '${live:-no summary}'"; fi
    for n in "${NODES[@]}"; do
      local line v
      line=$(printf '%s\n' "$out" | awk -v n="$n" '$1 == n')
      # The VERSION column, by the header (live rows only: LIVE is one word).
      v=$(printf '%s\n' "$out" | awk -v n="$n" '
        $1 == "NODE" { for (i = 1; i <= NF; i++) if ($i == "VERSION") col = i }
        col && $1 == n && $2 == "yes" { print $col }')
      local want; if [ "${bin[$n]}" = "$old_bin" ]; then want=$old_label; else want=$new_label; fi
      if [ "$v" = "$want" ] && printf '%s\n' "$line" | grep -q ' serving '; then
        pass "status ($who): $n serving $(label "${bin[$n]}") ($v)"
      else fail "status ($who): $n: '$line' (want serving $want)"; fi
    done
    local warn; warn=$(printf '%s\n' "$out" | grep -i 'warn\|different' | tr '\n' ' ' || true)
    [ -z "$warn" ] || note "status warnings ($who): $warn"
  done
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
    if [ $seen_old_new = 1 ]; then pass "actors.call $old_short -> new exercised"; else fail "no successful $old_short -> new call"; fi
    if [ $seen_new_old = 1 ]; then pass "actors.call new -> $old_short exercised"; else fail "no successful new -> $old_short call"; fi
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

check_sealed() { # every secret sealed during the run, on every node
  local n name
  for name in "${!sealed[@]}"; do
    local want=${sealed[$name]%%|*} who=${sealed[$name]#*|}
    for n in "${NODES[@]}"; do
      req_retry 20 GET "$n" "/actor/t/at-$n/env?name=$name"
      if [ "$RS" = 200 ] && [ "$RB" = "$want wasi=unset" ]; then
        pass "$name sealed by the $who CLI, via $n ($(label "${bin[$n]}")) on ${RNODE}"
      else fail "$name sealed by the $who CLI, via $n ($(label "${bin[$n]}")) on '${RNODE}': $RS '$RB'"; fi
    done
  done
}
# A secret for class t sealed by CLI (old or new); read on every node by
# every later check_all.
seal() { # seal CLI NAME VALUE
  local who; if [ "$1" = old ]; then who=$old_short; else who=new; fi
  if printf '%s\n' "$3" | "$1" secrets set --app "$APP" --bucket "$BUCKET" \
    --key "$run/etc/secrets.key" --class t "$2" > "$run/secrets-$2.txt" 2>&1; then
    pass "$who CLI secrets set $2 on a cluster of $(versions)"; sealed[$2]="$3|$who"
  else fail "$who CLI secrets set $2: $(tr '\n' ' ' < "$run/secrets-$2.txt")"; fi
}
# The versions the live nodes run, as "0.8.0, new, new".
versions() { local n vs=(); for n in "${NODES[@]}"; do vs+=("$(label "${bin[$n]}")"); done; local IFS=,; echo "${vs[*]}"; }

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

client_state() { cat "${1:-$run/clients}"/*.json 2>/dev/null | jq -s '.' 2>/dev/null; }
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
# What placement did on the new nodes: apart and spread
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
  check_sealed
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
  if [ -z "$bad" ]; then pass "start floor: no client failure: $total wake-ups through peers all 200 ($placed ran on $1)"
  else fail "start floor: $(printf '%s\n' "$bad" | wc -l) of $total wake-ups not 200: $(printf '%s\n' "$bad" | head -5 | tr '\n' '|')"; fi
  # Calls refused on the started node as signed before its start. The
  # floor is the start plus the clock skew nodes allow each other (1 s,
  # not a setting), and peers send a node calls only once its heartbeat
  # is live, many seconds after its start: on one machine's clock, no call
  # normally lands in that window. Counted when it happens (with no client
  # failure, above, it is the floor working); a note when it does not.
  local floor; floor=$(metric "$1" wasmkeep_signatures_before_start_total)
  if [ "$floor" -gt 0 ]; then
    if [ -z "$bad" ]; then pass "start floor: $floor calls refused on $1 for their date, none seen by a client"
    else fail "start floor: $floor calls refused on $1 for their date, and clients saw failures (above)"; fi
  else
    note "start floor (observational): 0 calls refused on $1 for their date; the window (its start plus 1 s of skew) closes before peers call it"
  fi
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

# ---------------------------------------------------------------- the four paths
# A check that is a gate when GATE is 1 (PASS or FAIL) and a control
# otherwise (a NOTE either way).
verdict() { # verdict GATE OK MESSAGE
  if [ "$1" = 1 ]; then if [ "$2" = 1 ]; then pass "$3"; else fail "$3"; fi
  else note "control ($( [ "$2" = 1 ] && echo met || echo not met )): $3"; fi
}
# Waits until neither ACTOR... is held (out of memory, its hold released):
# 0 when so within SECS, else 1, with the holders in HELD.
released() { # released SECS ACTOR...
  local deadline=$((SECONDS + $1)) a h; shift
  while :; do
    HELD=""
    for a in "$@"; do h=$(holder "$a"); [ "$h" = - ] || HELD="$HELD $a@$h"; done
    [ -z "$HELD" ] && return 0
    [ $SECONDS -lt $deadline ] || return 1
    sleep 2
  done
}

# The stored idle position. The room `small` keeps the pusher's default
# idle period: once a burst is delivered its pusher keeps one alarm 5 s
# later, then one a minute out while its group holds sockets. In between,
# with nothing published, the room and the pusher leave memory after the
# nodes' --idle-timeout-secs and release their holds. The first publish
# then wakes both cold, and the pusher hands its edges the position it
# stored: no client fetches the history. Three clients, one through each
# node, in $run/clients-small.
small_published=0
small_publish() { # small_publish ID
  pubs=$((pubs + 1))
  req_retry 30 GET "$1" "/r6/publish?room=small&id=sm-$pubs"
  if [ "$RS" = 200 ] && [[ $RB =~ ^[0-9]+$ ]] && [ "$RB" -gt "$small_published" ]; then small_published=$RB
  else fail "room small: publish via $1: $RS '$RB' (last acknowledged $small_published)"; fi
}
small_missing() { # what the clients of `small` lack of 1..N, after at most 30 s
  local deadline=$((SECONDS + 30)) missing
  while :; do
    missing=$(client_state "$run/clients-small" | jq -r --argjson h "$small_published" \
      '[.[] | (([range(1; $h + 1)] - .have) as $m | select($m | length > 0) | "\(.id) lacks \($m | .[0:5] | map(tostring) | join(","))")] | join("; ")' \
      || echo "no client state")
    [ -z "$missing" ] && break
    [ $SECONDS -lt $deadline ] || break
    sleep 1
  done
  printf '%s' "$missing"
}
small_fetches() { client_state "$run/clients-small" | jq '[.[] | .fetches] | add // 0' 2>/dev/null || echo 0; }
idle_check() { # idle_check GATE WHAT
  local gate=$1 what=$2 t0 missing before after count
  small_publish a; small_publish b; small_publish c
  missing=$(small_missing)
  if [ -n "$missing" ]; then verdict "$gate" 0 "idle position ($what): the burst before the quiet spell: $missing"; return; fi
  t0=$SECONDS
  if ! released 150 room/small pusher/small; then
    verdict "$gate" 0 "idle position ($what): still held after $((SECONDS - t0)) s with nothing published:$HELD"
    return
  fi
  note "idle position ($what): room/small and pusher/small out of memory $((SECONDS - t0)) s after the burst"
  sleep 2 # one more save of every client's state
  before=$(small_fetches)
  small_publish b
  missing=$(small_missing)
  sleep 2
  after=$(small_fetches)
  count=$(client_state "$run/clients-small" | jq length 2>/dev/null || echo 0)
  if [ -z "$missing" ] && [ "$count" -gt 0 ] && [ "$after" = "$before" ]; then
    verdict "$gate" 1 "idle position ($what): the first publish after both left memory ($small_published) reached all $count clients with 0 history fetches"
  else
    verdict "$gate" 0 "idle position ($what): the first publish after both left memory ($small_published): $((after - before)) history fetches; ${missing:-every client holds 1..$small_published}"
  fi
}

# Kept apart, sent on: room/big woken on R by a call from R's front, then
# its pusher woken the same way on R. R holds the room, so it sends the
# pusher's wake-up to a peer, which runs it.
apart_sent() { # apart_sent R
  local r=$1 before after h p rs rb
  if ! released 90 room/big pusher/big; then fail "apart, sent on: room/big or pusher/big still held:$HELD"; return; fi
  req_retry 20 GET "$r" "/r6/history?room=big&n=0"
  h=$(holder room/big)
  if [ "$h" != "$r" ]; then fail "apart, sent on: room/big woke on '$h', not $r ($RS '$RB')"; return; fi
  before=$(metric "$r" wasmkeep_placement_apart_total 'outcome="sent"')
  req_retry 20 GET "$r" "/r6/warm-pusher?room=big"; rs=$RS; rb=$RB
  after=$(metric "$r" wasmkeep_placement_apart_total 'outcome="sent"')
  p=$(holder pusher/big)
  if [ "$rs" = 200 ] && [ "$after" -gt "$before" ] && [ "$p" != - ] && [ "$p" != "$r" ]; then
    pass "apart, sent on: pusher/big woken on $r ($(label "${bin[$r]}")), which holds its room, ran on $p ($(label "${bin[$p]:-}")); sent $before -> $after"
  else
    fail "apart, sent on: warm-pusher via $r ($(label "${bin[$r]}")): $rs '$rb'; pusher/big on '$p'; $r's sent $before -> $after"
  fi
}

# Kept apart, the refusal read: pusher/legacy's wake-up, a request from
# outside through node b, past placement's floor, placed on R, which
# holds room/legacy: R refuses it with the apart reason, and b, of the
# other version, runs it here (the pusher's guest answers 403 to a caller
# from outside). b's fill comes from calls of t/at-b, which are never
# placed, so no peer is marked as having just taken one. Placement picks
# the lighter of b's peers: when that is not R, the attempt is noted and
# made again (the peer that took it is then left out for a few periods).
apart_refused() { # apart_refused R
  local r=$1 x=b attempt before after h p resident got=""
  if [ "$(label "${bin[$r]}")" = "$(label "${bin[$x]}")" ]; then fail "apart, refusal: $x and $r run one version"; return; fi
  pin b || return
  for attempt in 1 2 3; do
    if ! released 90 room/legacy pusher/legacy; then fail "apart, refusal: room/legacy or pusher/legacy still held:$HELD"; return; fi
    req_retry 20 GET "$r" "/r6/history?room=legacy&n=0"
    h=$(holder room/legacy)
    if [ "$h" != "$r" ]; then note "apart, refusal (attempt $attempt): room/legacy woke on '$h', not $r"; continue; fi
    seq 1 70 | xargs -P 8 -I{} curl -s -m 20 -o /dev/null -X POST \
      "http://127.0.0.1:${port[$x]}/actor/t/at-b/call?to=t/fill-$step-$attempt-{}&path=%2Ffired"
    resident=$(metric "$x" wasmkeep_actors_resident)
    before=$(metric "$r" wasmkeep_placements_received_total 'outcome="apart"')
    req GET "$x" /actor/pusher/legacy/outside
    after=$(metric "$r" wasmkeep_placements_received_total 'outcome="apart"')
    p=$(holder pusher/legacy)
    got="$RS from '${RNODE}', pusher/legacy on '$p', $x resident $resident, $r's apart refusals $before -> $after"
    if [ "$after" -gt "$before" ] && [ "$RNODE" = "$x" ] && [ "$RS" -lt 500 ] && [ "$p" = "$x" ]; then
      pass "apart, refusal: placed on $r ($(label "${bin[$r]}")), refused as apart, read and run by $x ($(label "${bin[$x]}")): $got"
      return
    fi
    note "apart, refusal (attempt $attempt): not refused by $r: $got"
  done
  fail "apart, refusal: no wake-up of pusher/legacy placed on $r in 3 attempts (last: $got)"
}

# A node of this checkout holding room/quiet drained, with every client
# connected and publishes through the other nodes; then undrained.
drain_room() { # drain_room ID
  local n=$1 h p deadline
  log "---- step $step: node $n ($(label "${bin[$n]}")) drained holding room/quiet"
  # Its pusher elsewhere first, so the room may stay with n.
  p=$(holder pusher/quiet)
  if [ "$p" = "$n" ]; then
    local other; for other in "${NODES[@]}"; do [ "$other" = "$n" ] || break; done
    new actor move --app "$APP" --bucket "$BUCKET" pusher/quiet --to "$other" >/dev/null 2>&1 || true
    sleep 3
  fi
  new actor move --app "$APP" --bucket "$BUCKET" room/quiet --to "$n" >/dev/null 2>&1 || true
  deadline=$((SECONDS + 40))
  while h=$(holder room/quiet); [ "$h" != "$n" ] && [ $SECONDS -lt $deadline ]; do
    publish 1 # an event, so that the room wakes where the move asks
    sleep 1
  done
  if [ "$h" != "$n" ]; then fail "drain with the room: room/quiet on '$h' after a move to $n"; return; fi
  pass "drain with the room: room/quiet on $n (pusher/quiet on $(holder pusher/quiet))"
  bg_pub_start "$n"
  if ! new cluster drain "$n" --bucket "$BUCKET" >/dev/null; then fail "drain $n refused"; fi
  wait_state "$n" drained 90 || true
  h=$(holder room/quiet)
  if [ "$h" != "$n" ] && [ "$h" != - ]; then pass "drain with the room: room/quiet handed to $h ($(label "${bin[$h]:-}"))"
  else fail "drain with the room: room/quiet on '$h' once $n is drained"; fi
  sleep 5 # publishes while drained
  new cluster undrain "$n" --bucket "$BUCKET" >/dev/null 2>&1 || fail "undrain $n refused"
  wait_state "$n" serving 60 || true
  bg_pub_stop
  publish 3
  check_room "after $n drained holding the room"
  retries
}

# ---------------------------------------------------------------- 0. all old
# From here on a failed command is a check's to report, not a reason to stop.
set +e
begin deploy
# The 0640 env file an install may have (owned by the service user,
# group-readable): 0.8.0 and later refuse it, older releases start.
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
refuses() { [ "$1" != 124 ] && [ "$1" != 0 ]; }
if before_080 "$old_short"; then old_ok() { [ "$rc_old" = 124 ]; }; old_want="starts"
else old_ok() { refuses "$rc_old"; }; old_want="refuses"; fi
if old_ok && refuses "$rc_new"; then
  pass "env file 0640 owner-readable: $old_short $old_want (exit $rc_old), new refuses (exit $rc_new: $(grep -i 'env' "$run/probe-env/new.log" | head -1 | cut -c1-200))"
else
  fail "env file 0640: $old_short exit '$rc_old' (want: $old_want), new exit '$rc_new' (want non-zero): $(tail -n 2 "$run/probe-env/new.log" | tr '\n' ' ')"
fi

# The pusher class is open to outside requests (its guest answers 403 to
# them): the apart refusal needs a placed wake-up of it, and only a
# request from outside is placed.
old deploy --no-project --app "$APP" --bucket "$BUCKET" \
  --actor "t=$test_actor" \
  --actor "room=$old_room" --actor "pusher=$old_room" --actor "edge=$old_room" \
  --actor "directory=$old_room" --actor "relay=$old_room" --front "$old_front" \
  --access room=internal --access pusher=open --access edge=internal \
  --access directory=internal --access relay=internal --writes pusher=batched \
  > "$run/deploy-1.txt" 2>&1 || { cat "$run/deploy-1.txt"; exit 1; }
printf 'sealed-value\n' | old secrets set --app "$APP" --bucket "$BUCKET" \
  --key "$run/etc/secrets.key" --class t MV_SECRET >/dev/null 2>&1 \
  || { echo "secrets set failed" >&2; exit 1; }

begin "start-$old_short"
for n in "${NODES[@]}"; do start_node "$n" "$old_bin"; done
for n in "${NODES[@]}"; do wait_ready "$n" 90 || { tail -n 20 "$run/$n.log"; exit 1; }; done
wait_state a serving 30 || true

mkdir -p "$run/clients" "$run/clients-small"
# Four clients through each node: twelve, past the test room's edge
# capacity of five, so it splits into edges, spread over the nodes.
node scripts/mixed-version/room-clients.mjs "$run/clients" quiet 4 "${port[a]}" "${port[b]}" "${port[c]}" \
  > "$run/clients.log" 2>&1 &
clients_pid=$!
# One through each node in the room the idle position is checked on.
node scripts/mixed-version/room-clients.mjs "$run/clients-small" small 1 "${port[a]}" "${port[b]}" "${port[c]}" \
  > "$run/clients-small.log" 2>&1 &
small_pid=$!
sleep 8
check_all
# The call limits between two old nodes, to compare with the hop below.
for spec in "97 10" "120 10"; do
  read -r hn hv <<< "$spec"
  req_retry 10 POST b "/actor/t/at-b/call?to=t/at-c&path=%2Fdurable&hn=$hn&hv=$hv"
  note "actors.call $old_short -> $old_short with $hn headers of $hv bytes: $RS '$RB'"
done

# The control: the old pusher and room on old nodes.
begin "idle-$old_short"
idle_check 0 "$old_short pusher, $old_short room, all $old_short"

# A database written by the old version, opened by the new one after the
# upgrade: a table under the reserved prefix (refused since 0.7.0), a
# plain one, and one big enough that sorting it is a long statement.
begin "sqlite-$old_short"
big_sort="SELECT x FROM big ORDER BY x DESC LIMIT 1"
for sql in "CREATE TABLE _wasmkeep_legacy (x INTEGER)" \
           "CREATE TABLE plain_t (x INTEGER)" "INSERT INTO plain_t VALUES (1)" \
           "CREATE TABLE big (x BLOB)" \
           "WITH RECURSIVE c(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM c WHERE i < 300000) INSERT INTO big SELECT randomblob(16) FROM c" \
           "$big_sort"; do
  req_retry 20 POST a /actor/t/sqlx/sql --data-binary "$sql"
  note "$old_short sql '${sql:0:60}' on ${RNODE}: $RS $RB"
done

# ---------------------------------------------------------------- 1..3 upgrade
phase=upgrade
begin upgrade-a; swap a "$new_bin" || exit 1

# Both CLIs on the mixed cluster (a new; b, c old). The new CLI deploys the
# pusher from this checkout's SDK, every other room class from the old
# one's; the old CLI then deploys the same with a configuration for t.
# Each seals a secret, read on every node by every later step.
begin cli-skew
if new deploy --no-project --app "$APP" --bucket "$BUCKET" \
  --actor "t=$test_actor" \
  --actor "room=$old_room" --actor "pusher=$new_room" --actor "edge=$old_room" \
  --actor "directory=$old_room" --actor "relay=$old_room" \
  > "$run/deploy-2.txt" 2>&1; then
  pass "new CLI deploy on a cluster of $(versions): $(tail -n 1 "$run/deploy-2.txt")"
else fail "new CLI deploy: $(tr '\n' ' ' < "$run/deploy-2.txt")"; fi
seal new MV_SECRET2 new-sealed
printf 'MV_CONFIG=from-old-cli\n' > "$run/etc/t.config"
if old deploy --no-project --app "$APP" --bucket "$BUCKET" \
  --actor "t=$test_actor" \
  --actor "room=$old_room" --actor "pusher=$new_room" --actor "edge=$old_room" \
  --actor "directory=$old_room" --actor "relay=$old_room" --config "t=$run/etc/t.config" \
  > "$run/deploy-3.txt" 2>&1; then
  pass "$old_short CLI deploy on a cluster of $(versions): $(tail -n 1 "$run/deploy-3.txt")"
else fail "$old_short CLI deploy: $(tr '\n' ' ' < "$run/deploy-3.txt")"; fi
seal old MV_SECRET3 old-sealed
sleep 8
for n in "${NODES[@]}"; do
  req_retry 20 GET "$n" "/actor/t/at-$n/env?name=MV_CONFIG"
  if [ "$RS" = 200 ] && [ "$RB" = "from-old-cli wasi=unset" ]; then
    pass "configuration deployed by the $old_short CLI, via $n ($(label "${bin[$n]}")) on ${RNODE}"
  else fail "configuration deployed by the $old_short CLI, via $n on '${RNODE}': $RS '$RB'"; fi
done
check_status
check_sealed
publish 2
check_room "after both CLIs deployed"

begin idle-new-pusher-old-room
idle_check 1 "new pusher, $old_short room, nodes $(versions)"

# Request heads and calls across an old -> new hop (t/at-a on a).
begin limits
pin a || true; pin b || true
hdrs() { local k; for k in $(seq 1 "$1"); do printf -- '-H\nx-mv-%03d: v\n' "$k"; done; }
for count in 90 95 99; do
  mapfile -t h < <(hdrs "$count")
  req GET a /actor/t/at-a/durable "${h[@]}"; direct="$RS"
  req GET b /actor/t/at-a/durable "${h[@]}"; hop="$RS on ${RNODE}"
  note "$count client headers to t/at-a (new): direct $direct, via b ($old_short) $hop"
  [ "${hop%% *}" = "$direct" ] || fail "$count headers: direct $direct but via $old_short $hop"
done
for spec in "97 10" "120 10" "60 1000"; do
  read -r hn hv <<< "$spec"
  req_retry 10 POST b "/actor/t/at-b/call?to=t/at-a&path=%2Fdurable&hn=$hn&hv=$hv"
  note "actors.call $old_short -> new with $hn headers of $hv bytes: $RS '$RB'"
  req_retry 10 POST a "/actor/t/at-a/call?to=t/at-b&path=%2Fdurable&hn=$hn&hv=$hv"
  note "actors.call new -> $old_short with $hn headers of $hv bytes: $RS '$RB'"
done

# Kept apart on the mixed cluster: sent on by the new node, and refused by
# the new node to an old sender.
begin apart-new-receives
apart_sent a
apart_refused a
check_placement

begin drain-new-with-room
drain_room a

begin upgrade-b; swap b "$new_bin" || exit 1

# Every room class from this checkout's SDK (the front stays the old one).
begin room-new-sdk
if new deploy --no-project --app "$APP" --bucket "$BUCKET" \
  --actor "t=$test_actor" \
  --actor "room=$new_room" --actor "pusher=$new_room" --actor "edge=$new_room" \
  --actor "directory=$new_room" --actor "relay=$new_room" \
  > "$run/deploy-4.txt" 2>&1; then
  pass "new CLI deploy of every room class on a cluster of $(versions): $(tail -n 1 "$run/deploy-4.txt")"
else fail "new CLI deploy: $(tr '\n' ' ' < "$run/deploy-4.txt")"; fi
sleep 8
publish 2
check_room "after the room classes' redeploy"
begin idle-new-pusher-new-room
idle_check 1 "new pusher, new room, nodes $(versions)"

# Kept apart the other way: sent on by the old node, and refused by the
# old node to a new sender (b).
begin apart-old-receives
apart_sent c
apart_refused c
check_placement

begin upgrade-c; swap c "$new_bin" || exit 1

# The old version's database, now on a new node.
begin sqlite
new actor move --app "$APP" --bucket "$BUCKET" t/sqlx --to b >/dev/null 2>&1 || true
sleep 3
for sql in "SELECT count(*) FROM plain_t" "INSERT INTO plain_t VALUES (2)" \
           "ALTER TABLE plain_t ADD COLUMN y INTEGER" "SELECT count(*) FROM big" "$big_sort"; do
  req_retry 20 POST b /actor/t/sqlx/sql --data-binary "$sql"
  note "new sql '${sql:0:60}' on ${RNODE}: $RS $RB"
done
req_retry 20 POST b /actor/t/sqlx/sql --data-binary "SELECT count(*) FROM plain_t"
if [ "$RS" = 200 ] && [[ $RB == ok:* ]]; then pass "$old_short database read and written on new"
else fail "$old_short database on new: $RS $RB"; fi

upgrade_secs=$((SECONDS - started_at))

# ---------------------------------------------------------------- downgrade
# Gating like the rest: one node back to the old release.
phase=downgrade
begin downgrade-c; swap c "$old_bin"

begin end
# Every alarm armed in the run, read again: still exactly one run each.
late=0
for a in "${!every_alarm[@]}"; do
  req_retry 20 GET a "/actor/t/$a/attempts"
  if [ "$RS" != 200 ] || [ "$RB" != 1 ]; then
    fail "alarm $a (armed on ${every_alarm[$a]}) has run '$RB' times by the end"; late=$((late + 1))
  fi
done
[ "$late" -gt 0 ] || pass "every alarm armed (${#every_alarm[@]}) ran once by the end"
for p in "$clients_pid" "$small_pid"; do kill "$p" 2>/dev/null || true; wait "$p" 2>/dev/null || true; done
clients_pid=; small_pid=
# What the nodes warned about, by message, per node.
for n in "${NODES[@]}"; do
  sed -nE 's/^[0-9T:.Z-]+ +(WARN|ERROR) +([^ ]+): ([^=]{1,90}).*/\1 \2: \3/p' "$run/$n.log" \
    | sort | uniq -c | sort -rn | head -8 | while read -r line; do note "node $n log: $line"; done
done
step_secs[end]=$((SECONDS - step_t0[end]))
log "---- by step (phase, step, passed, failed, seconds)"
for s in "${steps[@]}"; do
  log "$(printf '%-9s %-28s %4d %4d %5d' "${step_phase[$s]}" "$s" "${step_pass[$s]:-0}" "${step_fail[$s]:-0}" "${step_secs[$s]:-0}")"
done
log "$OLD_VERSION -> $new_label: $fails checks failed; upgrade part ${upgrade_secs}s, whole run $((SECONDS - started_at))s"
log "report: $report; node logs: $run/<node>.log (kept with MIXED_VERSION_KEEP=1)"
[ "$fails" -eq 0 ]
