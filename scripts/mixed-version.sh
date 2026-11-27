#!/usr/bin/env bash
# The mixed-version check (a gate before tagging a release, docs/releasing.md):
# three nodes on this machine, against moto as the tutorials and the S3
# suites run it, start on a published release (OLD_VERSION, v0.8.1 by
# default), then move one node at a time to the binary built from this
# checkout (drain, stop, swap, start, wait for ready and `cluster status`).
# After each step it checks, through every node: the durable counter keeps
# every acknowledged write, `actors.call` works between every pair of nodes
# (and so between versions, both ways), alarms armed before the step fire
# once, secrets and env files are readable, WebSocket room clients receive
# every acknowledged publish, live or after catching up, and both versions'
# `cluster status` agree. While the cluster is mixed, both CLIs deploy,
# seal secrets, drain a node and make a backup.
#
# From 0.9.0 it is the proof of ADR 0043 (section 8), and goes on:
# - back and forward: one node returns to the old release and goes up
#   again, with the same checks, before anything is finalized;
# - the gate: a deploy that needs the new format (a wasmkeep.toml with
#   `apart-from` and `spread`, deploy manifest 8) is refused while a node
#   of the old release is live, and `cluster upgrade finalize` is refused
#   naming that node; with every node new it is still refused, naming the
#   floor; after `cluster upgrade finalize` it is accepted, and placement
#   follows the keys;
# - the returning old node: after the finalize a node of the old release
#   started again stops itself on the upgrade floor, which it names; once
#   the gated format is written it stops on the manifest it cannot read.
#   Neither time does it answer a request, and the counter goes on
#   through the others;
# - tracing (ADR 0044): two new nodes export to a collector this script
#   runs, with --trace-sample 0. A client's `traceparent ...-01` forwarded
#   by a node of the old release is not followed; through a new node that
#   trusts it, it is, on both nodes;
# - edges return home (ADR 0040, section 14, from 0.10.0): after every
#   step each new node's rebalance pass is paused while a node of the
#   old release runs a minor version apart, and its moves and
#   the cold sockets its edges' homes took (old homes included) are noted;
# - the generation floor (ADR 0045, from a new side of 0.10): an actor
#   created by a new node before the finalize has a marker, byte for byte
#   0.9's, and an old node takes it over; one created by an old node is
#   taken over by a new one after the finalize above its marker; a node
#   that served through the finalize creates an actor with no marker, and
#   after `cluster markers clean` one above a removed marker's number,
#   with `cluster verify` counting no marker left;
# - CLI skew: the old CLI against the finalized cluster (`deploy`,
#   `secrets set`, `cluster status`, `cluster drain`, `backup create`):
#   each works and is read rightly, or is refused and changes nothing.
#
# Two modes, by the two binaries' minor versions:
# - the next minor version (0.9 beside v0.8.1): everything above;
# - the same minor version (a patch release: 0.9.1 beside v0.9.0): the
#   upgrade under load, the four paths below, one node back and up again,
#   and both CLIs on the mixed cluster. Nothing is gated between two
#   patches of one minor version, so the gate, the returning old node, the
#   tracing phase and the old CLI on a finalized cluster are left out, and
#   the nodes run without tracing.
#
# The new side's version. A gated format is written only by a `wasmkeep`
# of the next minor version, and a build of this checkout reports the
# workspace version, which the release's pull request bumps. Until then,
# MIXED_VERSION_NEW_VERSION=0.9.0 builds the new side with the test-only
# feature `test-version` of wasmkeep-cluster and WASMKEEP_TEST_VERSION
# (see its build.rs), into its own target directory: it reports
# `0.9.0+test.<commit>`. Without the variable the plain release build is
# used and nothing is overridden: the next-minor mode once the workspace
# version is ahead of OLD_VERSION, the same-minor mode otherwise.
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
# Exit status: 0 when every check passed, in every phase; 1 when
# one failed; 2 when the old version cannot be used (no pinned SHA-256).
#
# Usage: scripts/mixed-version.sh [OLD_VERSION]
#   OLD_VERSION (argument or variable, default v0.8.1): the release the
#   nodes start on. A version without a SHA-256 pinned below is refused
#   unless OLD_SHA256 gives the hash of its Linux x86_64 archive (from the
#   release's SHA256SUMS).
#   MIXED_VERSION_NEW_VERSION (unset): the version the new side is built
#   to report, X.Y.Z (the test-only override above). Unset: this
#   checkout's own version.
#   MIXED_VERSION_OLDER_VERSION (v0.8.0 when OLD_VERSION is v0.8.1, else
#   none): a release from before the upgrade floor was read, started once
#   after the gated deploy (next-minor mode): it must stop on the manifest.
#   It needs a pinned SHA-256.
#   MIXED_VERSION_PORT_BASE (default 18100): client ports BASE+1..3,
#   cluster ports BASE+11..13, metrics BASE+21..23, moto BASE+50,
#   BASE+60 for the env file probe and BASE+70 for the trace collector.
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
  [v0.8.1]=88f7a35dd5e96306f5ef51c0987d9cf7d3f55b7601fb5e8fa8b43a77e6bdf6ac
  [v0.9.0]=b2a5b38c82f5ef3d6a59c26bd6460610c4dc684305b0a9ec73be3db294311240
)
OLD_VERSION=${1:-${OLD_VERSION:-v0.8.1}}
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
sink_port=$((base + 70))
for p in "${port[@]}" "${cport[@]}" "${mport[@]}" "$moto_port" $((base + 60)) "$sink_port"; do
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
# Downloads a release's Linux x86_64 archive once, checks it against the
# release's SHA256SUMS, and on every run (a cached archive included: the
# pin is the gate's) against the pinned hash: the binary in RELEASE_BIN.
fetch_release() { # fetch_release VERSION SHA256
  local asset=wasmkeep-${1#v}-x86_64-unknown-linux-gnu.tar.gz dir=$cache/$1
  RELEASE_BIN=$dir/${asset%.tar.gz}/wasmkeep
  if [ ! -x "$RELEASE_BIN" ]; then
    mkdir -p "$dir"
    gh release download "$1" --repo WasmKeep/wasmkeep --pattern "$asset" \
      --pattern SHA256SUMS --dir "$dir" --clobber
    (cd "$dir" && sha256sum --ignore-missing --strict -c SHA256SUMS)
    echo "$2  $dir/$asset" | sha256sum --strict -c -
    tar -xzf "$dir/$asset" -C "$dir"
  fi
  echo "$2  $dir/$asset" | sha256sum --strict --quiet -c - \
    || { echo "$dir/$asset does not match the pinned SHA-256" >&2; exit 2; }
}
fetch_release "$OLD_VERSION" "$OLD_SHA256"
old_bin=$RELEASE_BIN
# The new side: this checkout's release build, or, when asked, the same
# build reporting another version (the test-only feature, in a target
# directory of its own so that target/release never holds a binary with
# another version's name).
minor_of() { local v=${1%%[-+]*}; echo "${v%.*}"; }
NEW_VERSION=${MIXED_VERSION_NEW_VERSION:-}
if [ -n "$NEW_VERSION" ]; then
  [[ $NEW_VERSION =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || {
    echo "MIXED_VERSION_NEW_VERSION '$NEW_VERSION' is not X.Y.Z" >&2; exit 2; }
  WASMKEEP_TEST_VERSION=$NEW_VERSION cargo build --release --locked -p wasmkeep-cli \
    --features wasmkeep-cluster/test-version --target-dir "$mv_dir/build"
  new_bin=$mv_dir/build/release/wasmkeep
else
  cargo build --release --locked -p wasmkeep-cli
  new_bin=$root/target/release/wasmkeep
fi
old_label=$("$old_bin" --version | cut -d' ' -f2)
new_label=$("$new_bin" --version | cut -d' ' -f2)
[ "$old_label" != "$new_label" ] || { echo "both binaries report $old_label" >&2; exit 1; }
new_minor=$(minor_of "$new_label")
old_minor=$(minor_of "$old_label")
if [ -n "$NEW_VERSION" ]; then
  case $new_label in "$NEW_VERSION"+test*) ;; *) echo "the new side reports $new_label, not $NEW_VERSION+test" >&2; exit 1 ;; esac
else
  case $new_label in *+test*) echo "the new side reports a test version, $new_label, and none was asked for" >&2; exit 1 ;; esac
fi
# The mode: whether anything is gated between the two binaries.
if [ "$new_minor" = "$old_minor" ]; then
  gated=0
  mode="same minor version ($old_minor): the upgrade under load and both CLIs on the mixed cluster; no gate, no returning old node, no tracing phase (MIXED_VERSION_NEW_VERSION builds the new side as another version)"
else
  gated=1
  mode="next minor version ($old_minor -> $new_minor): every phase"
fi
echo "mixed-version: $OLD_VERSION -> $new_label, $mode"
# ADR 0045's lines, from a new side of 0.10 on.
genfloor=0
if [ $gated = 1 ] && [ "$(printf '%s\n0.10\n' "$new_minor" | sort -V | head -1)" = 0.10 ]; then genfloor=1; fi
# A release from before the floor was read, for the returning node.
older_bin=""; older_short=""
OLDER_VERSION=${MIXED_VERSION_OLDER_VERSION-$( [ "$OLD_VERSION" != v0.8.1 ] || echo v0.8.0 )}
if [ $gated = 1 ] && [ -n "$OLDER_VERSION" ]; then
  [ -n "${PINNED_SHA256[$OLDER_VERSION]:-}" ] || {
    echo "no SHA-256 pinned for MIXED_VERSION_OLDER_VERSION $OLDER_VERSION" >&2; exit 2; }
  fetch_release "$OLDER_VERSION" "${PINNED_SHA256[$OLDER_VERSION]}"
  older_bin=$RELEASE_BIN; older_short=${OLDER_VERSION#v}
fi
label() {
  if [ "$1" = "$old_bin" ]; then echo "$old_short"
  elif [ -n "$older_bin" ] && [ "$1" = "$older_bin" ]; then echo "$older_short"
  else echo "new"; fi
}

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
moto_pid=; clients_pid=; small_pid=; probe_pid=; bg_pid=; sink_pid=
cleanup() {
  local n
  for p in $probe_pid $bg_pid $clients_pid $small_pid $sink_pid; do kill "$p" 2>/dev/null || true; done
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
# Where both CLIs' `backup create` write.
curl -sf -X PUT -H 'Content-Length: 0' "http://127.0.0.1:$moto_port/mixed-version-backup" >/dev/null
BACKUP=s3://mixed-version-backup

# The trace collector (ADR 0044): what each node exports, one file a node.
node scripts/mixed-version/otlp-sink.mjs "$run/otlp" "$sink_port" > "$run/otlp-sink.log" 2>&1 &
sink_pid=$!
for k in $(seq 1 50); do
  curl -sf -o /dev/null "http://127.0.0.1:$sink_port/" && break
  kill -0 "$sink_pid" 2>/dev/null && [ "$k" -lt 50 ] || { cat "$run/otlp-sink.log" >&2; exit 1; }
  sleep 0.2
done
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
# What a node of this checkout runs with besides: tracing on a and b, to
# the collector above, with no trace sampled by a draw (--trace-sample 0),
# so that a span exported is one whose flag was followed. Node a trusts the
# flag a client sends; b does not; c has tracing off.
declare -A extra_new=(
  [a]="--otlp-endpoint http://127.0.0.1:$sink_port/a --trace-sample 0 --trace-trust-incoming"
  [b]="--otlp-endpoint http://127.0.0.1:$sink_port/b --trace-sample 0"
  [c]="")
# Between two patches of one minor version the tracing phase is not run.
[ $gated = 1 ] || extra_new=([a]="" [b]="" [c]="")
start_node() { # start_node ID BINARY
  local n=$1 b=$2 more=""
  [ "$b" != "$new_bin" ] || more=${extra_new[$n]}
  mkdir -p "$run/$n/data" "$run/$n/cache"; chmod 700 "$run/$n" "$run/$n/data" "$run/$n/cache"
  echo "=== $(date -u +%T) start $(label "$b") ($("$b" --version))" >> "$run/$n.log"
  # shellcheck disable=SC2086 # extra and more hold whole flags
  "$b" serve --app "$APP" --bucket "$BUCKET" --node-id "$n" \
    --data-dir "$run/$n/data" --cache-dir "$run/$n/cache" \
    --listen "127.0.0.1:${port[$n]}" --advertise "http://127.0.0.1:${port[$n]}" \
    --cluster-listen "127.0.0.1:${cport[$n]}" --cluster-advertise "https://127.0.0.1:${cport[$n]}" \
    --metrics-listen "127.0.0.1:${mport[$n]}" --idle-timeout-secs "$idle_secs" ${extra[$n]} $more \
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
# A bucket object as moto holds it (unsigned, as the buckets are made).
s3obj() { curl -sf "http://127.0.0.1:$moto_port/mixed-version/$1" 2>/dev/null || true; }
# Test-actor NAME's per-actor marker, and its ownership record's generation.
marker_of() { s3obj "state/$APP/leases/t/$1"; }
owner_gen() { s3obj "state/$APP/owners/t/$1" | sed -n 3p; }
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

# Runs a command, keeping what it printed in $run/NAME.txt: its exit
# status in RC, its output on one line in OUT.
run_cli() { # run_cli NAME COMMAND... (its input: the file RUN_STDIN, or none)
  local name=$1; shift
  "$@" > "$run/$name.txt" 2>&1 < "${RUN_STDIN:-/dev/null}"; RC=$?
  OUT=$(tr '\n' ' ' < "$run/$name.txt" | tr -s ' ' | cut -c1-700)
  # A refusal is a message and an exit status, never a crash.
  if grep -q 'panicked at' "$run/$name.txt" || [ "$RC" -ge 126 ]; then
    fail "$name: the command crashed (exit $RC): $OUT"
  fi
}
# The app's current version and its manifest format, as "VERSION FORMAT".
current() {
  status_json | jq -r --arg a "$APP" '.apps[] | select(.app == $a) | "\(.version) \(.manifest_format)"' 2>/dev/null
}
# The app's upgrade, as `cluster upgrade status` reads it: "STATE|FLOOR IN FORCE|BLOCKS".
upgrade_state() {
  new cluster upgrade status --bucket "$BUCKET" --app "$APP" --json 2>/dev/null \
    | jq -r '.apps[0] | "\(.state)|\(.floor_in_force)|\(.blocks | join(","))"' 2>/dev/null
}
# The capability words a live node announces.
words() { status_json | jq -r --arg n "$1" '.nodes[] | select(.id == $n and .live) | .words | join(" ")' 2>/dev/null; }

# `cluster status` from both CLIs (the new one alone once the upgrade is
# finalized: the old CLI is then judged in its own step): three live, each
# node serving on the version it runs.
status_clis="new old"
check_status() {
  local cli out
  for cli in $status_clis; do
    out=$("$cli" cluster status --bucket "$BUCKET" 2>&1 || true)
    printf '%s\n' "$out" > "$run/status-$cli-cli-$step.txt"
    local live who
    who="$( [ "$cli" = new ] && echo "new CLI" || echo "$old_short CLI")"
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

check_counter() { # check_counter [ID...]: through these nodes, all without
  local n nodes=("$@")
  [ $# -gt 0 ] || nodes=("${NODES[@]}")
  for n in "${nodes[@]}"; do
    req_retry 20 POST "$n" /actor/t/ctr/durable
    if [ "$RS" = 200 ] && [ "$RB" = $((ctr + 1)) ]; then
      ctr=$RB; pass "counter: POST via $n ($(label "${bin[$n]}")) -> $RB on ${RNODE}"
    elif [ "$RS" = 200 ]; then
      fail "counter: POST via $n answered $RB, expected $((ctr + 1)) (a lost durable write?)"
      ctr=$RB
    else fail "counter: POST via $n: $RS $RB"; fi
  done
  for n in "${nodes[@]}"; do
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
# Spread actors return home (ADR 0040, section 14) on the new nodes: while
# a node of the old release runs beside them, a minor version apart, the
# pass is paused (for versions, or first for a drain or a node not
# serving), so no rebalance move starts. Noted with it: why it is paused,
# the moves so far, and the sockets for an edge nobody held that its home
# (old or new) took, or did not.
declare -A rb_moves # new node -> its rebalance moves when last read while mixed
check_rebalance() {
  local n page old=0 paused why moves total home
  for n in "${NODES[@]}"; do
    [ "${bin[$n]}" = "$new_bin" ] || old=1
  done
  for n in "${NODES[@]}"; do
    [ "${bin[$n]}" = "$new_bin" ] || continue
    page=$(curl -s "http://127.0.0.1:${mport[$n]}/metrics" || true)
    paused=$(printf '%s\n' "$page" | grep -E '^wasmkeep_rebalance_paused\{' \
      | awk '{s+=$2} END {print s+0}' || true)
    why=$(printf '%s\n' "$page" | grep -E '^wasmkeep_rebalance_paused\{.* 1$' \
      | grep -Eo 'reason="[^"]*"' | sed -E 's/reason="([^"]*)"/\1/' | tr '\n' ' ' || true)
    why=${why% }
    moves=$(printf '%s\n' "$page" | grep -E '^wasmkeep_rebalance_moves_total' \
      | sed -E 's/\{tenant="[^"]*",app="[^"]*",?/{/' | tr '\n' ' ' || true)
    total=$(printf '%s\n' "$page" | grep -E '^wasmkeep_rebalance_moves_total' \
      | awk '{s+=$2} END {print s+0}' || true)
    home=$(printf '%s\n' "$page" | grep -E '^wasmkeep_placement_spread_total\{.*outcome="home(_failed)?"' \
      | sed -E 's/\{tenant="[^"]*",app="[^"]*",?/{/' | tr '\n' ' ' || true)
    note "node $n rebalance: paused ${why:-no}; moves ${moves:-none}; cold sockets ${home:-none}"
    if [ "$old" = 1 ] && [ "$gated" = 1 ]; then
      # The version skew is the reason, unless a drain or a node not
      # serving is (those are reported first, and pause it too).
      case "${paused:-0}:$why" in
        0:*) fail "node $n: rebalance not paused while a node of $old_short runs a minor version apart" ;;
        *:versions) pass "node $n: rebalance paused for versions while a node of $old_short runs" ;;
        *:draining|*:not_serving) note "node $n: rebalance paused for $why (before versions)" ;;
        *) fail "node $n: rebalance paused for $why, not versions, while a node of $old_short runs" ;;
      esac
      # No move starts while the versions are mixed: its count stays
      # flat (a node restarted since starts again from 0).
      if [ -n "${rb_moves[$n]:-}" ] && [ "${total:-0}" -gt "${rb_moves[$n]}" ]; then
        fail "node $n: $((total - rb_moves[$n])) rebalance moves while a node of $old_short ran"
      fi
      rb_moves[$n]=${total:-0}
    else
      unset 'rb_moves[$n]'
    fi
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
  check_rebalance
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
# With THIRD, b's other peer is asked to drain first, and undrained after:
# a node asked to drain is no placement target, so R is b's only one. The
# lighter peer otherwise takes the wake-up every time (a new node beside
# the old R holding room/legacy). The actors THIRD held stay where the
# drain put them for the steps after (no rebalance while versions mix).
apart_refused() { # apart_refused R [THIRD]
  local r=$1 third=${2:-} x=b
  if [ "$(label "${bin[$r]}")" = "$(label "${bin[$x]}")" ]; then fail "apart, refusal: $x and $r run one version"; return; fi
  pin b || return
  if [ -n "$third" ]; then
    if ! new cluster drain "$third" --bucket "$BUCKET" >/dev/null; then fail "apart, refusal: drain $third refused"; return; fi
    if ! wait_state "$third" drained 90; then
      new cluster undrain "$third" --bucket "$BUCKET" >/dev/null 2>&1 || true
      wait_state "$third" serving 60 || true
      return
    fi
    note "apart, refusal: $third drained, so that b places on $r alone"
  fi
  apart_refused_attempts "$r" "$x"
  if [ -n "$third" ]; then
    new cluster undrain "$third" --bucket "$BUCKET" >/dev/null 2>&1 || fail "undrain $third refused"
    wait_state "$third" serving 60 || true
  fi
}
apart_refused_attempts() { # apart_refused_attempts R X
  local r=$1 x=$2 attempt before after h p resident got=""
  for attempt in 1 2 3 4 5 6; do
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
  fail "apart, refusal: no wake-up of pusher/legacy placed on $r in 6 attempts (last: $got)"
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

# ---------------------------------------------------------------- the gate
# The app as a project file, the whole of it as the last deploy with flags
# left it (the file is the whole truth: a class or a rule it does not name
# is removed), without the keys, or with them: `fan` kept apart from
# `feed`, and `leaf` spread. With the keys the version is manifest format
# 8, the first format behind the upgrade floor (ADR 0043, section 2).
project_file() { # project_file DIR KEYS (0 or 1)
  local apart="" spread=""
  [ "$2" = 0 ] || { apart='apart-from = "feed"'; spread='spread = true'; }
  mkdir -p "$1"
  cat > "$1/wasmkeep.toml" <<EOF
[app]
tenant = "${APP%/*}"
name = "${APP#*/}"

[front]
component = "$old_front"

[classes.t]
component = "$test_actor"
access = "open"
config = "$run/etc/t.config"

[classes.feed]
component = "$test_actor"
access = "open"

[classes.fan]
component = "$test_actor"
access = "open"
$apart

[classes.leaf]
component = "$test_actor"
access = "open"
$spread

[classes.room]
component = "$new_room"

[classes.pusher]
component = "$new_room"
access = "open"
writes = "batched"

[classes.edge]
component = "$new_room"

[classes.directory]
component = "$new_room"

[classes.relay]
component = "$new_room"
EOF
}
# A deploy of the file with the keys, which must be refused and change
# nothing: its message in OUT.
gated_refused() { # gated_refused NAME
  local before after
  before=$(current)
  run_cli "$1" new deploy --project "$run/project/keys" --bucket "$BUCKET" --yes
  after=$(current)
  if [ "$RC" != 0 ] && [ -n "$before" ] && [ "$after" = "$before" ]; then
    pass "a deploy with apart-from and spread is refused on a cluster of $(versions), nothing deployed: $OUT"
  else
    fail "a deploy with apart-from and spread on a cluster of $(versions): exit $RC, current '$before' -> '$after': $OUT"
  fi
}
# feed/NAME then fan/NAME, each woken by a request through node R: where
# each ran, in FEED_ON and FAN_ON.
pair_wake() { # pair_wake NAME R
  req_retry 20 GET "$2" "/actor/feed/$1/fired"; FEED_ON=$RNODE
  req_retry 20 GET "$2" "/actor/fan/$1/fired"; FAN_ON=$RNODE
}

# ---------------------------------------------------------------- tracing
# A W3C `traceparent` a client sends for a new trace: TP, its trace id in
# TID and its parent id in TSPAN.
traceparent() { # traceparent FLAGS (01: asks to be sampled)
  TID=$(head -c 16 /dev/urandom | od -An -tx1 | tr -d ' \n')
  TSPAN=$(head -c 8 /dev/urandom | od -An -tx1 | tr -d ' \n')
  TP="00-$TID-$TSPAN-$1"
}
# Whether node N exported a span of trace ID: a span carries its trace id
# as 16 raw bytes, and the collector keeps each export in hex.
exported() { grep -q "$2" "$run/otlp/$1.hex" 2>/dev/null; }
wait_exported() { # wait_exported N ID SECS
  local deadline=$((SECONDS + $3))
  until exported "$1" "$2"; do
    [ $SECONDS -lt $deadline ] || return 1
    sleep 0.5
  done
}
# The `traceparent` the guest of t/ACTOR received, asked through node N
# with the client's TP: in RB, the node that ran it in RNODE.
header_seen() { # header_seen N ACTOR
  req_retry 20 GET "$1" "/actor/t/$2/header?name=traceparent" -H "traceparent: $TP"
}

# ---------------------------------------------------------------- CLI skew
# One CLI drains a node and puts it back, the new CLI reading the state.
drain_by() { # drain_by CLI ID
  local who; if [ "$1" = old ]; then who=$old_short; else who=new; fi
  run_cli "drain-$1-$2-$step" "$1" cluster drain "$2" --bucket "$BUCKET"
  if [ "$RC" = 0 ] && wait_state "$2" drained 90; then
    pass "$who CLI cluster drain $2 ($(label "${bin[$2]}")) on a cluster of $(versions)"
  else fail "$who CLI cluster drain $2 ($(label "${bin[$2]}")): exit $RC: $OUT"; fi
  run_cli "undrain-$1-$2-$step" "$1" cluster undrain "$2" --bucket "$BUCKET"
  if [ "$RC" = 0 ] && wait_state "$2" serving 60; then
    pass "$who CLI cluster undrain $2 ($(label "${bin[$2]}"))"
  else fail "$who CLI cluster undrain $2 ($(label "${bin[$2]}")): exit $RC: $OUT"; fi
}
# `backup create` of the app by one CLI into an archive.
backup_by() { # backup_by CLI ARCHIVE NAME
  run_cli "$3" "$1" backup create --bucket "$BUCKET" --to "$BACKUP/$2" --app "$APP"
}
# The objects of an archive, one line each (its key, its ETag), sorted,
# into FILE (moto's listing, read without a signature as the bucket was
# made).
archive_list() { # archive_list ARCHIVE FILE
  local token="" page k
  : > "$2.raw"
  for k in $(seq 1 500); do
    page=$(curl -s -G "http://127.0.0.1:$moto_port/${BACKUP#s3://}" --data-urlencode list-type=2 \
      --data-urlencode "prefix=$1/" ${token:+--data-urlencode "continuation-token=$token"})
    printf '%s' "$page" | grep -o '<Key>[^<]*</Key>\|<ETag>[^<]*</ETag>' | paste - - >> "$2.raw"
    token=$(printf '%s' "$page" | sed -n 's:.*<NextContinuationToken>\([^<]*\)</NextContinuationToken>.*:\1:p')
    [ -n "$token" ] || break
  done
  sort "$2.raw" > "$2"; rm -f "$2.raw"
}
# What a refused `backup create` left in an archive, from its listings
# before and after: ADDED and GONE (objects new, and objects removed or
# rewritten), and STARTED, the new objects that are a run's `started`
# record. A run is in an archive once its `complete` record is; a
# `started` alone is a run that did not finish, as after any interruption.
archive_diff() { # archive_diff BEFORE AFTER
  ADDED=$(comm -13 "$1" "$2" | wc -l); GONE=$(comm -23 "$1" "$2" | wc -l)
  STARTED=$(comm -13 "$1" "$2" | grep -c '<Key>[^<]*runs/[^/<]*/started</Key>')
  COMPLETED=$(comm -13 "$1" "$2" | grep -c '<Key>[^<]*\(runs/[^/<]*/complete\|latest\)</Key>')
}
# A command of the old CLI on the finalized cluster that either works or
# is refused: 0 when it was refused. A refusal passes only with the
# message it is known by (PATTERN, an extended regular expression); what a
# command that worked wrote is checked by the caller.
# What a CLI that checks the floor says (0.9 and later); 0.8.1 has no such
# check, and is refused only by what it cannot read.
older_cli='older than the cluster'
old_cli_refused() { # old_cli_refused WHAT PATTERN (after run_cli)
  [ "$RC" != 0 ] || return 1
  if [[ $OUT =~ $2 ]]; then pass "$old_short CLI $1 on the finalized cluster: refused (exit $RC): $OUT"
  else fail "$old_short CLI $1 on the finalized cluster: exit $RC, with another message than /$2/: $OUT"; fi
  return 0
}

# ---------------------------------------------------------------- the returning old node
# Node c is stopped and started on an older release, which must stop
# itself, for the reason REASON names, without ever answering a request or
# writing a lease; the counter goes on through a and b, and c comes back on
# the new version.
# - floor: the current manifest is one it reads, so what stops it is the
#   app's upgrade floor, and its log says so;
# - manifest: the gated format is current. A node of 0.8.1 reads the
#   current manifest before the floor, and one of 0.8.0 reads no floor:
#   both stop on a manifest they cannot read.
old_node_returns() { # old_node_returns WHEN REASON (floor or manifest) [BINARY]
  local b=${3:-$old_bin} name its_label want log_from t0 answered="" rc="" secs said path code c_version up
  name=$(label "$b"); its_label=$("$b" --version | cut -d' ' -f2)
  case $2 in
    floor) want='older than the floor|upgrade was finalized at' ;;
    *) want='unreadable .*manifest' ;;
  esac
  log "---- step $step: node c new -> $name $1, which must not serve"
  if ! new cluster drain c --bucket "$BUCKET" >/dev/null; then fail "drain c refused"; fi
  wait_state c drained 90 || true
  stop_node c
  new cluster undrain c --bucket "$BUCKET" --force >/dev/null 2>&1 || fail "undrain c refused"
  log_from=$(( $(wc -l < "$run/c.log") + 1 ))
  t0=$SECONDS
  start_node c "$b"
  while [ $((SECONDS - t0)) -lt 60 ]; do
    for path in /_wasmkeep/ready /actor/t/ctr/durable; do
      code=$(curl -s -m 2 -o /dev/null -w '%{http_code}' "http://127.0.0.1:${port[c]}$path" || true)
      [ "$code" != 200 ] || answered="$answered $path@$((SECONDS - t0))s"
    done
    if ! kill -0 "${pid[c]}" 2>/dev/null; then wait "${pid[c]}" 2>/dev/null; rc=$?; break; fi
    sleep 0.5
  done
  secs=$((SECONDS - t0))
  said=$(tail -n +"$log_from" "$run/c.log" | grep -E "$want" | head -1 | cut -c1-400)
  if [ -z "$answered" ]; then pass "the $name node, started $1, was never ready and answered no request in $secs s"
  else fail "the $name node answered 200 $1:$answered"; fi
  if [ -z "$rc" ]; then
    fail "the $name node did not stop itself: still running after $secs s: $(tail -n 2 "$run/c.log" | tr '\n' ' ' | cut -c1-400)"
    stop_node c
  else
    pid[c]=
    if [ "$rc" != 0 ] && [ -n "$said" ]; then
      pass "the $name node stopped itself after $secs s (exit $rc), on the $2: $said"
    else
      fail "the $name node exited with $rc after $secs s without the reason /$want/: $(tail -n +"$log_from" "$run/c.log" | tail -n 3 | tr '\n' ' ' | cut -c1-400)"
    fi
  fi
  # The counter goes on through the others.
  check_counter a b
  # It wrote no lease: c's is still the new version's, released, and the
  # app's upgrade is as it was.
  c_version=$(status_json | jq -r '.nodes[] | select(.id == "c") | .version' 2>/dev/null)
  if [ "$c_version" = "$new_label" ]; then pass "the $name node wrote no lease: c's still names $new_label"
  else fail "c's lease names '$c_version' after the $name node's start (want $new_label, never $its_label)"; fi
  up=$(upgrade_state)
  if [[ $up == "one version|$new_minor|"* ]]; then pass "cluster upgrade status is still one version, floor $new_minor ($up)"
  else fail "cluster upgrade status after the $name node's start: '$up' (want one version, floor $new_minor)"; fi
  # Node c back on the new version.
  t0=$SECONDS
  start_node c "$new_bin"
  if wait_ready c 150; then pass "node c (new) ready $((SECONDS - t0))s after its start"; fi
  wait_state c serving 60 || true
}

# ---------------------------------------------------------------- 0. all old
# From here on a failed command is a check's to report, not a reason to stop.
set +e
begin deploy
note "mode: $mode"
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
# feed, fan and leaf are the gate's: a room, its pusher and an edge under
# the app's own names, which only `apart-from` and `spread` place.
own=(--actor "feed=$test_actor" --actor "fan=$test_actor" --actor "leaf=$test_actor")
old deploy --no-project --app "$APP" --bucket "$BUCKET" \
  --actor "t=$test_actor" "${own[@]}" \
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
  --actor "t=$test_actor" "${own[@]}" \
  --actor "room=$old_room" --actor "pusher=$new_room" --actor "edge=$old_room" \
  --actor "directory=$old_room" --actor "relay=$old_room" \
  > "$run/deploy-2.txt" 2>&1; then
  pass "new CLI deploy on a cluster of $(versions): $(tail -n 1 "$run/deploy-2.txt")"
else fail "new CLI deploy: $(tr '\n' ' ' < "$run/deploy-2.txt")"; fi
seal new MV_SECRET2 new-sealed
printf 'MV_CONFIG=from-old-cli\n' > "$run/etc/t.config"
if old deploy --no-project --app "$APP" --bucket "$BUCKET" \
  --actor "t=$test_actor" "${own[@]}" \
  --actor "room=$old_room" --actor "pusher=$new_room" --actor "edge=$old_room" \
  --actor "directory=$old_room" --actor "relay=$old_room" --config "t=$run/etc/t.config" \
  > "$run/deploy-3.txt" 2>&1; then
  pass "$old_short CLI deploy on a cluster of $(versions): $(tail -n 1 "$run/deploy-3.txt")"
else fail "$old_short CLI deploy: $(tr '\n' ' ' < "$run/deploy-3.txt")"; fi
seal old MV_SECRET3 old-sealed
# Each CLI drains a node of the other version, and puts it back.
drain_by old a
drain_by new c
# Each CLI makes a backup of the app, into an archive of its own.
backup_by new new backup-new-mixed
if [ "$RC" = 0 ]; then pass "new CLI backup create on a cluster of $(versions): $(tail -n 1 "$run/backup-new-mixed.txt")"
else fail "new CLI backup create on a cluster of $(versions): exit $RC: $OUT"; fi
backup_by old old backup-old-mixed
if [ "$RC" = 0 ]; then pass "$old_short CLI backup create on a cluster of $(versions): $(tail -n 1 "$run/backup-old-mixed.txt")"
else fail "$old_short CLI backup create on a cluster of $(versions): exit $RC: $OUT"; fi
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
# the new node to an old sender. c, b's other peer, is drained for the
# refusal, so that b places on a.
begin apart-new-receives
apart_sent a
apart_refused a c
check_placement

begin drain-new-with-room
drain_room a

begin upgrade-b; swap b "$new_bin" || exit 1

# Every room class from this checkout's SDK (the front stays the old one).
begin room-new-sdk
if new deploy --no-project --app "$APP" --bucket "$BUCKET" \
  --actor "t=$test_actor" "${own[@]}" \
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
# old node to a new sender (b). a, the lighter of b's peers, is drained
# for the refusal, so that b places on c.
begin apart-old-receives
apart_sent c
apart_refused c a
check_placement

if [ $gated = 1 ]; then
  # ---------------------------------------------------------------- the gate, one old node live
  # a and b new, c on the old release: nothing behind the floor is written.
  phase=gate
  begin "gate-beside-$old_short"
  project_file "$run/project/plain" 0
  project_file "$run/project/keys" 1
  # The file without the keys is the app as it is: accepted beside the old
  # node, in a format it reads.
  run_cli deploy-file-plain new deploy --project "$run/project/plain" --bucket "$BUCKET" --yes
  if [ "$RC" = 0 ]; then pass "new CLI deploy from the project file, without the keys, on a cluster of $(versions): $(tail -n 1 "$run/deploy-file-plain.txt")"
  else fail "new CLI deploy from the project file, without the keys: exit $RC: $OUT"; fi
  note "the plan of the project file's first deploy: $(grep -v '^ *$' "$run/deploy-file-plain.txt" | head -12 | tr '\n' '|' | cut -c1-600)"
  sleep 8
  check_counter
  gated_refused deploy-keys-mixed
  gate_names=""
  case $OUT in *"c ($old_label"*) gate_names="the deploy's refusal" ;; esac
  if [[ $OUT == *"needs every node at $new_minor"* ]] && [[ $OUT == *"upgrade floor is $old_minor"* ]]; then
    pass "the refusal is the floor's: the format needs every node at $new_minor, and the floor is $old_minor"
  else fail "the refusal is not the floor's (needs every node at $new_minor, upgrade floor is $old_minor): $OUT"; fi
  # What the refusal sends the operator to: refused too, naming the node.
  run_cli finalize-mixed new cluster upgrade finalize --bucket "$BUCKET" --app "$APP" --yes
  if [ "$RC" != 0 ] && [[ $OUT == *"c ($old_label"* ]]; then
    pass "cluster upgrade finalize beside a live $old_short node is refused, naming it (exit $RC): $OUT"
    gate_names=${gate_names:+$gate_names and }"the refusal of \`cluster upgrade finalize\`"
  else fail "cluster upgrade finalize beside a live $old_short node: exit $RC: $OUT"; fi
  if [ -n "$gate_names" ]; then pass "node c ($old_label) is named by $gate_names"
  else fail "nothing names node c ($old_label) as what blocks the gated deploy"; fi
  up=$(upgrade_state)
  if [[ $up == "mixed|$old_minor|"* ]]; then pass "cluster upgrade status: mixed, floor $old_minor ($up)"
  else fail "cluster upgrade status on a cluster of $(versions): '$up' (want mixed, floor $old_minor)"; fi

  if [ $genfloor = 1 ]; then
    # ADR 0045, section 5: before the finalize a new node writes what 0.9
    # does, and an old node takes the actor over; an actor the old node
    # creates is kept for the line after the finalize.
    begin genfloor-before-finalize
    if pin_actor gf-new a; then
      m=$(marker_of gf-new)
      if [[ $m == "wasmkeep-lease 2"$'\n'* ]]; then pass "t/gf-new, created by a ($new_label) before the finalize, has 0.9's marker ($(echo "$m" | tr '\n' ' '))"
      else fail "t/gf-new, created by a before the finalize, has no 0.9 marker: '$m'"; fi
      g=$(owner_gen gf-new)
      if pin_actor gf-new c && [ "$(owner_gen gf-new)" -gt "${g:-0}" ]; then pass "c ($old_label) takes t/gf-new over, at generation $(owner_gen gf-new)"
      else fail "c ($old_label) did not take t/gf-new over (generation ${g:-?} -> $(owner_gen gf-new))"; fi
    fi
    if pin_actor gf-old c && [ -n "$(marker_of gf-old)" ]; then pass "t/gf-old, created by c ($old_label), has a marker"
    else fail "t/gf-old, created by c ($old_label): no marker ($(marker_of gf-old))"; fi
    retries
  fi

  # ---------------------------------------------------------------- tracing, one old node live
  # ADR 0044, section 4 and "Not settled", 11: a node follows the sampled
  # flag of a hop only from a node that announces `traces`, which sets the
  # header itself. A node of the old release passes on what a client sent.
  # With --trace-sample 0 no node samples by a draw, so a span exported is a
  # flag followed.
  phase=traces
  begin traces
  traces_ready=1
  for n in a b; do
    w=$(words "$n")
    if [[ " $w " == *" traces "* ]]; then pass "node $n (new, tracing on) announces \`traces\`"
    else fail "node $n (new, started with --otlp-endpoint) does not announce \`traces\`: '$w'"; traces_ready=0; fi
  done
  w=$(words c)
  if [ "${bin[c]}" = "$old_bin" ] && [[ " $w " != *" traces "* ]]; then pass "node c ($old_short) does not announce \`traces\`"
  else fail "node c is not a live $old_short node without \`traces\`: $(label "${bin[c]}"), '$w'"; traces_ready=0; fi
  pin_actor tr-b b || traces_ready=0
  pin_actor tr-c c || traces_ready=0
  hex16='[0-9a-f]{16}'
  # 1. Asking to be sampled, through the old node, to an actor on b: c
  #    forwards it with the client's header. b continues the trace and does
  #    not follow the flag: its guest sees the flag off.
  traceparent 01; id_old=$TID
  header_seen c tr-b
  if [ "$RS" = 200 ] && [ "$RNODE" = b ] && [[ $RB =~ ^00-$id_old-$hex16-00$ ]]; then
    pass "a client's sampled traceparent forwarded by c ($old_short) is not followed on b: its guest received $RB"
  else fail "a client's sampled traceparent forwarded by c ($old_short) to b: $RS on '${RNODE}', the guest received '$RB' (want 00-$id_old-<span>-00)"; fi
  # 2. The same from outside, straight to b, which does not trust it.
  traceparent 01; id_out=$TID
  header_seen b tr-b
  if [ "$RS" = 200 ] && [ "$RNODE" = b ] && [[ $RB =~ ^00-$id_out-$hex16-00$ ]]; then
    pass "a client's sampled traceparent sent to b (no --trace-trust-incoming) is not followed: its guest received $RB"
  else fail "a client's sampled traceparent sent to b: $RS on '${RNODE}', the guest received '$RB' (want 00-$id_out-<span>-00)"; fi
  # 3. The same through a, which trusts it: a samples, and b follows a's hop.
  traceparent 01; id_new=$TID
  header_seen a tr-b
  if [ "$RS" = 200 ] && [ "$RNODE" = b ] && [[ $RB =~ ^00-$id_new-$hex16-01$ ]]; then
    pass "the same through a (new, --trace-trust-incoming) is followed on b: its guest received $RB"
  else fail "a client's sampled traceparent through a to b: $RS on '${RNODE}', the guest received '$RB' (want 00-$id_new-<span>-01)"; fi
  followed=1
  for n in a b; do
    if wait_exported "$n" "$id_new" 20; then pass "node $n exported spans of the trace that came through a ($id_new)"
    else fail "node $n exported no span of the trace that came through a ($id_new) within 20 s"; followed=0; fi
  done
  # b's exporter has delivered since: what it did not send by now, it never made.
  sleep 3
  leaked=""
  for n in a b; do
    for id in "$id_old" "$id_out"; do ! exported "$n" "$id" || leaked="$leaked $n:$id"; done
  done
  if [ $traces_ready = 1 ] && [ $followed = 1 ] && [ -z "$leaked" ]; then
    pass "no span exported for the trace forwarded by c ($old_short) nor for the one sent straight to b (the collector had b's spans of the followed trace by then)"
  elif [ -n "$leaked" ]; then fail "spans exported for a trace whose flag must not be followed:$leaked"
  else fail "not shown that the flag forwarded by c is not followed: b exported nothing at all, or the nodes were not as the check needs"; fi
  # 4. ADR 0043, section 8, 7: a hop from a tracing node to the old one
  #    passes, and the old node's guest receives the hop's header as any
  #    other: a's, in the client's trace, sampled.
  traceparent 01; id_wire=$TID
  header_seen a tr-c
  if [ "$RS" = 200 ] && [ "$RNODE" = c ] && [[ $RB =~ ^00-$id_wire-$hex16-01$ ]] && [ "$RB" != "$TP" ]; then
    pass "a forward from a (tracing) to c ($old_short) passes, and c's guest received a's traceparent as an ordinary header: $RB"
  else fail "a forward from a (tracing) to c ($old_short): $RS on '${RNODE}', the guest received '$RB' (the client sent $TP)"; fi
  note "the collector: $(cat "$run/otlp/a.hex" 2>/dev/null | wc -l) exports from a, $(cat "$run/otlp/b.hex" 2>/dev/null | wc -l) from b"
  check_calls

fi

phase=upgrade
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

# ---------------------------------------------------------------- back and forward
# Before anything is finalized, one node back to the old release and up
# again: gating like the rest.
phase=back
begin downgrade-c; swap c "$old_bin"
begin upgrade-c-again; swap c "$new_bin" || exit 1

if [ $gated = 1 ]; then
  # ---------------------------------------------------------------- the gate, every node new
  phase=gate
  begin gate-before-finalize
  up=$(upgrade_state)
  if [[ $up == "finalize pending|$old_minor|"* ]]; then pass "cluster upgrade status: finalize pending, floor $old_minor ($up)"
  else fail "cluster upgrade status with every node new: '$up' (want finalize pending, floor $old_minor)"; fi
  gated_refused deploy-keys-unfinalized
  if [[ $OUT == *"needs every node at $new_minor"* ]] && [[ $OUT == *"upgrade floor is $old_minor"* ]] \
    && [[ $OUT == *"cluster upgrade finalize"* ]]; then
    pass "with every node new the refusal names the floor ($old_minor) and \`cluster upgrade finalize\`"
  else fail "with every node new the refusal does not name the floor $old_minor and finalize: $OUT"; fi
  # Without the keys a fan wakes beside its feed: the control of what the
  # keys change.
  beside=""
  for k in 1 2 3 4; do
    pair_wake "plain-$k" a
    if [ "$FEED_ON" = a ] && [ "$FAN_ON" = a ]; then beside="plain-$k"; break; fi
    note "without the keys: feed/plain-$k woke on '$FEED_ON', fan/plain-$k on '$FAN_ON'"
  done
  if [ -n "$beside" ]; then pass "without the keys, fan/$beside woke on a, beside feed/$beside"
  else fail "without the keys, no fan woke on a beside its feed in 4 pairs"; fi

  begin finalize
  run_cli finalize-dry new cluster upgrade finalize --bucket "$BUCKET" --app "$APP"
  if [ "$RC" = 2 ] && [[ $(upgrade_state) == "finalize pending|$old_minor|"* ]]; then
    pass "cluster upgrade finalize without --yes writes nothing (exit 2): $OUT"
  else fail "cluster upgrade finalize without --yes: exit $RC, then '$(upgrade_state)': $OUT"; fi
  run_cli finalize new cluster upgrade finalize --bucket "$BUCKET" --app "$APP" --yes
  if [ "$RC" = 0 ]; then pass "cluster upgrade finalize (exit 0): $OUT"
  else fail "cluster upgrade finalize with every node new: exit $RC: $OUT"; fi
  finalized=0
  up=$(upgrade_state)
  if [[ $up == "one version|$new_minor|"* ]]; then pass "cluster upgrade status: one version, floor $new_minor ($up)"; finalized=1
  else fail "cluster upgrade status after the finalize: '$up' (want one version, floor $new_minor)"; fi
  # The old CLI is judged in its own step from here on.
  status_clis=new

  if [ $genfloor = 1 ] && [ $finalized = 1 ]; then
    # ADR 0045, sections 2 and 4, on nodes that served through the
    # finalize: no marker for a new actor; one created by 0.9 taken over
    # above its marker; after the cleanup, an actor whose marker was the
    # only carrier of its generation starts above it.
    begin genfloor-after-finalize
    old_marker=$(marker_of gf-old | sed -n 2p)
    if pin_actor gf-old a && [ "$(owner_gen gf-old)" -gt "${old_marker:-0}" ]; then pass "a takes t/gf-old (created by $old_short) over at generation $(owner_gen gf-old), above its marker's ${old_marker:-?}"
    else fail "t/gf-old after the finalize: generation $(owner_gen gf-old), its marker ${old_marker:-?}"; fi
    if pin_actor gf-after a && [ -z "$(marker_of gf-after)" ]; then pass "t/gf-after, created by a after the finalize, has no marker"
    else fail "t/gf-after, created after the finalize: marker '$(marker_of gf-after)'"; fi
    # A restore's marker, the only carrier of its actor's generation.
    curl -sf -X PUT --data-binary $'wasmkeep-lease 2\n41\n' "http://127.0.0.1:$moto_port/mixed-version/state/$APP/leases/t/gf-seeded" >/dev/null \
      || fail "writing a restore's marker for t/gf-seeded to moto failed"
    run_cli markers-clean new cluster markers clean --bucket "$BUCKET" --app "$APP" --yes
    if [ "$RC" = 0 ] && [ -z "$(marker_of gf-seeded)" ] && [ -z "$(marker_of gf-new)" ]; then pass "cluster markers clean (exit 0): $OUT"
    else fail "cluster markers clean: exit $RC, gf-seeded's marker '$(marker_of gf-seeded)': $OUT"; fi
    left=$( (new cluster verify --bucket "$BUCKET" --app "$APP" --json 2>/dev/null || true) | jq -r '.apps[0].lease_markers' 2>/dev/null || true)
    if [ "$left" = 0 ]; then pass "cluster verify counts no lease marker left"
    else fail "cluster verify after the cleanup: lease markers left '$left'"; fi
    if pin_actor gf-seeded a && [ "$(owner_gen gf-seeded)" -gt 41 ]; then pass "t/gf-seeded starts at generation $(owner_gen gf-seeded), above its removed marker's 41"
    else fail "t/gf-seeded after the cleanup: generation '$(owner_gen gf-seeded)', not above 41"; fi
    check_all
    retries
  fi

  # The floor is raised and nothing behind it is written yet: a node of the
  # old release reads every manifest, and must stop on the floor alone.
  phase=old-node
  begin "returning-$old_short-floor"
  old_node_returns "after the finalize, before the gated deploy" floor
  check_all
  retries

  phase=gate
  begin gated-deploy
  gated=0
  before=$(current)
  run_cli deploy-keys new deploy --project "$run/project/keys" --bucket "$BUCKET" --yes
  after=$(current)
  if [ $finalized = 1 ] && [ "$RC" = 0 ] && [ "${after#* }" = 8 ] && [ "$after" != "$before" ]; then
    pass "after the finalize the same deploy is accepted, in manifest format 8: $(tail -n 1 "$run/deploy-keys.txt")"; gated=1
  else fail "the deploy with apart-from and spread after the finalize: exit $RC, current '$before' -> '$after': $OUT"; fi
  note "the plan of the gated deploy: $(grep -v '^ *$' "$run/deploy-keys.txt" | head -12 | tr '\n' '|' | cut -c1-600)"
  sleep 8
  # Placement follows the keys: a fan no longer wakes beside its feed.
  sent_before=$(metric a wasmkeep_placement_apart_total 'outcome="sent"')
  apart=0; together=""
  for k in 1 2 3 4; do
    pair_wake "kept-$k" a
    if [ "$FEED_ON" != a ]; then note "with the keys: feed/kept-$k woke on '$FEED_ON', not a"; continue; fi
    if [ -n "$FAN_ON" ] && [ "$FAN_ON" != a ] && [ "$(holder "fan/kept-$k")" = "$FAN_ON" ]; then apart=$((apart + 1))
    else together="$together fan/kept-$k@'$FAN_ON'"; fi
  done
  sent_after=$(metric a wasmkeep_placement_apart_total 'outcome="sent"')
  if [ $gated = 1 ] && [ $apart -gt 0 ] && [ -z "$together" ] && [ "$sent_after" -gt "$sent_before" ]; then
    pass "placement follows the keys: $apart fans woken through a, which held their feeds, ran on another node (a's apart wake-ups sent on: $sent_before -> $sent_after)"
  else fail "placement after the gated deploy: $apart fans apart from their feed on a; beside it:${together:- none}; a's sent $sent_before -> $sent_after"; fi
  check_all
  retries

  # ---------------------------------------------------------------- the returning old node, again
  # The gated format is written: the same, on a manifest it cannot read.
  phase=old-node
  begin "returning-$old_short-format"
  old_node_returns "after the gated deploy" manifest
  check_all
  retries
  # ADR 0043, section 8, 4, second half: a release that reads no floor.
  if [ -n "$older_bin" ]; then
    begin "returning-$older_short-format"
    old_node_returns "after the gated deploy" manifest "$older_bin"
    check_all
    retries
  else
    note "no release from before the floor is started (MIXED_VERSION_OLDER_VERSION)"
  fi

  # ---------------------------------------------------------------- the old CLI, finalized cluster
  # ADR 0043, section 8, 8: each command works, and what it wrote is read
  # rightly by the new nodes, or it is refused with a message and changes
  # nothing. The deploy comes last: it may replace the gated version.
  phase="cli-skew"
  begin "cli-$old_short-finalized"
  run_cli old-status old cluster status --bucket "$BUCKET"
  if ! old_cli_refused "cluster status" "$older_cli"; then
    if grep -q '^3 of 3 nodes live' "$run/old-status.txt"; then pass "$old_short CLI cluster status on the finalized cluster: 3 of 3 nodes live"
    else fail "$old_short CLI cluster status on the finalized cluster: exit 0 without the three live nodes: $OUT"; fi
  fi
  printf 'old-finalized\n' > "$run/secret-4.in"
  RUN_STDIN="$run/secret-4.in" run_cli old-secrets old secrets set --app "$APP" --bucket "$BUCKET" \
    --key "$run/etc/secrets.key" --class t MV_SECRET4
  if old_cli_refused "secrets set" "$older_cli"; then
    sleep 8
    req_retry 20 GET a "/actor/t/at-a/env?name=MV_SECRET4"
    if [ "$RS" = 200 ] && [[ $RB != old-finalized* ]]; then pass "the refused secrets set sealed nothing (MV_SECRET4 reads '$RB')"
    else fail "after the refused secrets set, MV_SECRET4 reads: $RS '$RB'"; fi
  else
    pass "$old_short CLI secrets set MV_SECRET4 on the finalized cluster"; sealed[MV_SECRET4]="old-finalized|$old_short"
    sleep 8
  fi
  check_sealed
  run_cli old-drain old cluster drain b --bucket "$BUCKET"
  if old_cli_refused "cluster drain" "$older_cli"; then
    if wait_state b serving 10; then pass "the refused drain changed nothing: b is serving"; fi
  else
    if wait_state b drained 90; then pass "$old_short CLI cluster drain b on the finalized cluster"; fi
    run_cli old-undrain old cluster undrain b --bucket "$BUCKET"
    if [ "$RC" = 0 ] && wait_state b serving 60; then pass "$old_short CLI cluster undrain b"
    else
      fail "$old_short CLI cluster undrain b: exit $RC: $OUT"
      new cluster undrain b --bucket "$BUCKET" >/dev/null 2>&1; wait_state b serving 60 || true
    fi
  fi
  # A backup by the new CLI now carries the app's floor (run format 2). The
  # old CLI must refuse that archive and add no run to it: nothing there is
  # removed or rewritten, and what it leaves is at most the `started` record
  # of the run it gave up (0.8.1 writes it before it reads the last run's
  # format), which the new CLI reads past.
  backup_by new new backup-new-finalized
  if [ "$RC" = 0 ] && grep -q 'backup format 2' "$run/backup-new-finalized.txt"; then
    pass "new CLI backup create after the finalize: a run of backup format 2 in the archive of the mixed run"
    archive_list new "$run/archive-new-before.txt"
    backup_by old new backup-old-format2
    archive_list new "$run/archive-new-after.txt"
    archive_diff "$run/archive-new-before.txt" "$run/archive-new-after.txt"
    if [ "$RC" != 0 ] && [[ $OUT =~ unknown\ or\ inconsistent\ backup\ format|$older_cli ]]; then
      pass "$old_short CLI backup create into the archive holding a format 2 run is refused (exit $RC): $OUT"
    else fail "$old_short CLI backup create into the archive holding a format 2 run: exit $RC: $OUT"; fi
    if [ -s "$run/archive-new-before.txt" ] && [ "$GONE" = 0 ] && [ "$COMPLETED" = 0 ] && [ "$ADDED" = "$STARTED" ] && [ "$ADDED" -le 1 ]; then
      pass "the refused backup added no run to that archive: of $(wc -l < "$run/archive-new-before.txt") objects none removed or rewritten, $ADDED added (the \`started\` record of the run it gave up)"
    else
      fail "the archive holding a format 2 run after the refused backup: $GONE objects removed or rewritten, $ADDED added ($STARTED \`started\` records, $COMPLETED \`complete\` or \`latest\`): $(comm -13 "$run/archive-new-before.txt" "$run/archive-new-after.txt" | head -3 | tr '\n\t' ' |')"
    fi
    backup_by new new backup-new-after-refusal
    if [ "$RC" = 0 ]; then pass "new CLI backup create into that archive after the refusal: $(tail -n 1 "$run/backup-new-after-refusal.txt")"
    else fail "new CLI backup create into that archive after the $old_short CLI's refused run: exit $RC: $OUT"; fi
  else fail "new CLI backup create after the finalize: exit $RC, no run of backup format 2 to try the $old_short CLI on: $OUT"; fi
  # Its own archive, with runs of format 1 only: the app's current manifest
  # is what it cannot read, and it stops part-way through a run. That run
  # must not be completed nor anything before it touched, and the new CLI
  # must still read that archive and add a run to it.
  archive_list old "$run/archive-old-before.txt"
  backup_by old old backup-old-finalized
  archive_list old "$run/archive-old-after.txt"
  archive_diff "$run/archive-old-before.txt" "$run/archive-old-after.txt"
  if old_cli_refused "backup create (its own archive)" "invalid deploy manifest|$older_cli"; then
    if [ -s "$run/archive-old-before.txt" ] && [ "$GONE" = 0 ] && [ "$COMPLETED" = 0 ]; then
      pass "the refused backup completed no run in its own archive: of $(wc -l < "$run/archive-old-before.txt") objects none removed or rewritten, $ADDED added for a run without a \`complete\` record"
    else
      fail "the $old_short CLI's own archive after its refused backup: $GONE objects removed or rewritten, $ADDED added, $COMPLETED \`complete\` or \`latest\`"
    fi
  else
    pass "$old_short CLI backup create of the finalized app into its own archive: $(tail -n 1 "$run/backup-old-finalized.txt")"
  fi
  backup_by new old backup-new-into-old
  if [ "$RC" = 0 ]; then pass "new CLI backup create into the $old_short CLI's archive after that: $(tail -n 1 "$run/backup-new-into-old.txt")"
  else fail "new CLI backup create into the $old_short CLI's archive: exit $RC: $OUT"; fi
  before=$(current)
  run_cli old-deploy old deploy --no-project --app "$APP" --bucket "$BUCKET" \
    --actor "t=$test_actor" "${own[@]}" \
    --actor "room=$new_room" --actor "pusher=$new_room" --actor "edge=$new_room" \
    --actor "directory=$new_room" --actor "relay=$new_room"
  after=$(current)
  if old_cli_refused "deploy" "unreadable [0-9a-f]+/manifest|$older_cli"; then
    if [ -n "$before" ] && [ "$after" = "$before" ]; then pass "the refused deploy changed nothing (current $after)"
    else fail "the $old_short CLI's refused deploy changed the current version: '$before' -> '$after'"; fi
  else
    note "$old_short CLI deploy on the finalized cluster was accepted: current '$before' -> '$after'; the checks below read it"
    sleep 8
  fi
  check_status
  check_counter
  check_calls
  check_secrets
  publish 2
  check_room "after the $old_short CLI's commands"
  retries

fi

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
