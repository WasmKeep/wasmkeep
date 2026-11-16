#!/usr/bin/env bash
# One big WebSocket room (bench/room, driven by bench/roomload): how far one
# room actor goes. Writes bench/results/<date>-<commit>-room-<label>.csv in
# bench/run.sh's columns, only once the run has finished. Numbers in docs
# must come from these files. See docs/benchmarks.md, "One big room".
#
#   bench/room.sh [local]   one node and the load on this machine, the whole
#                           matrix (sizes x write modes x rates)
#   bench/room.sh node      on the node's machine: serve the room and sample
#                           the node until stopped (several machines)
#   bench/room.sh client    on each load machine: one run of the matrix's
#                           cells against ROOM_URL's node
#   bench/room.sh merge     on one machine: the clients' and the node's files
#                           (gathered into ROOM_DIR) into the results file
#
# Settings (environment):
#   ROOM_LABEL   names the setup in the file name (required for local, merge)
#   ROOM_SIZES   sockets in the room, over all clients ("10 100 1000")
#   ROOM_RATES   messages a second into the room, over all clients ("100 1000")
#   ROOM_MODES   how the room stores a message ("plain durable batch")
#   ROOM_SECS    seconds of sending per cell (10); ROOM_DRAIN after it (5)
#   ROOM_SENDERS sockets that send, over all clients (10)
#   ROOM_PROCS   load processes on this machine (1); ROOM_CLIENTS machines
#                running `client` at once (1): each takes its share
#   ROOM_CLIENT  this machine's number, from 0 (client)
#   ROOM_SOURCE_IPS  comma-separated source addresses for this machine's
#                sockets (each gives about 28,000 to one server address)
#   ROOM_HTTP_SENDERS=1  senders POST instead of using their socket
#   ROOM_URL     the node, ws://HOST:PORT (client); ROOM_LISTEN for node,
#                and ROOM_ADVERTISE, its address as others reach it
#                (http://HOST:PORT), when ROOM_LISTEN is 0.0.0.0
#   ROOM_DIR     where the per-process files go (a temporary directory)
#   ROOM_START_DELAY seconds from a client's start to its cells' first send,
#                so every client is connected (client; 60)
#
# A durable write needs a bucket: WASMKEEP_ROOM_BUCKET=s3://BUCKET[/PREFIX]
# and AWS_* for it, or, locally, moto through uvx as scripts/s3-suites.sh
# starts it. Every mode runs on the same one-node cluster on that bucket, so
# the modes differ only in how the room writes. ROOM_BUCKET=none runs the
# node without one (local mode): `exec-durable` is then an fsync, and
# nothing is replicated after each event.
set -euo pipefail
cd "$(dirname "$0")/.."

role=${1:-local}
sizes=${ROOM_SIZES:-10 100 1000}
rates=${ROOM_RATES:-100 1000}
modes=${ROOM_MODES:-plain durable batch}
secs=${ROOM_SECS:-10}
drain=${ROOM_DRAIN:-5}
senders=${ROOM_SENDERS:-10}
procs=${ROOM_PROCS:-1}
clients=${ROOM_CLIENTS:-1}
client=${ROOM_CLIENT:-0}
largest=0
for n in $sizes; do (( n > largest )) && largest=$n; done

commit=$(git rev-parse --short HEAD)
git diff --quiet HEAD -- . ':!bench/results' || commit="${commit}-dirty"

build() {
  cargo build --release --locked -p wasmkeep-cli -p wasmkeep-roomload
  (cd bench/room && cargo build --release --target wasm32-wasip2 --locked)
}

# The descriptors a node needs for `n` sockets: the server gives
# connections at most a quarter of what is not reserved (fds::budget).
node_fds() { echo $(( 4 * ($1 + 1000) + 4096 )); }

moto_pid=
node_pid=
partial=
sampler_pid=
cleanup() {
  for pid in $sampler_pid $node_pid $moto_pid; do kill "$pid" 2>/dev/null || true; done
  wait 2>/dev/null || true
  [ -z "$partial" ] || rm -f "$partial"
}
trap cleanup EXIT

# Sets prefix (not in a subshell: it exports AWS_* and keeps moto's pid).
bucket() {
  if [ "${ROOM_BUCKET:-}" = none ]; then
    prefix=
    return
  fi
  if [ -n "${WASMKEEP_ROOM_BUCKET:-}" ]; then
    prefix=$WASMKEEP_ROOM_BUCKET
    return
  fi
  command -v uvx >/dev/null || { echo "no bucket: set WASMKEEP_ROOM_BUCKET and AWS_*, or install uv for moto" >&2; exit 1; }
  local log port=5056
  log=$(mktemp -t wasmkeep-room-moto.XXXXXX)
  uvx --quiet --from "moto[server]==5.2.5" moto_server -p "$port" >"$log" 2>&1 &
  moto_pid=$!
  for _ in $(seq 1 60); do
    curl -sf -o /dev/null "http://127.0.0.1:$port/moto-api/" && break
    kill -0 "$moto_pid" 2>/dev/null || { cat "$log" >&2; exit 1; }
    sleep 1
  done
  curl -sf -X PUT -H 'Content-Length: 0' "http://127.0.0.1:$port/wasmkeep-room" >/dev/null
  export AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_REGION=us-east-1
  export AWS_ENDPOINT=http://127.0.0.1:$port AWS_ALLOW_HTTP=true
  prefix=s3://wasmkeep-room
}

free_port() { python3 -I -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1])'; }

# start_node LISTEN METRICS BUCKET_PREFIX DATA_DIR SOCKETS: sets node_pid.
start_node() {
  local listen=$1 metrics=$2 prefix=$3 dir=$4 n=$5
  mkdir -p "$dir"
  local cluster=()
  [ -z "$prefix" ] || cluster=(--bucket "$prefix" --node-id room --cluster-plaintext accept \
    --advertise "${ROOM_ADVERTISE:-http://$listen}")
  target/release/wasmkeep serve \
    --actor "room=bench/room/target/wasm32-wasip2/release/room.wasm" \
    --data-dir "$dir/data" --cache-dir "$dir/cache" \
    --listen "$listen" --metrics-listen "$metrics" \
    --max-sockets-per-actor "${ROOM_MAX_SOCKETS_PER_ACTOR:-$n}" \
    --max-sockets-per-node "${ROOM_MAX_SOCKETS_PER_NODE:-$n}" \
    --max-connections $(( n + 1000 )) "${cluster[@]}" >"$dir/node.log" 2>&1 &
  node_pid=$!
  # A new node first checks that no other process runs its node id (about
  # 15 s), then serves.
  for _ in $(seq 1 600); do
    curl -sf -o /dev/null "http://${listen/0.0.0.0/127.0.0.1}/actor/room/ready" && return
    kill -0 "$node_pid" 2>/dev/null || { tail -20 "$dir/node.log" >&2; exit 1; }
    sleep 0.1
  done
  echo "the node did not start" >&2; exit 1
}

# load URL N RATE OUTDIR START_AT_MS: this machine's share of one cell, as
# ROOM_PROCS processes; process ids are unique over every client.
load() {
  local url=$1 n=$2 rate=$3 dir=$4 start_at=$5
  local share_n=$(( n / (clients * procs) ))
  local share_s=$(( senders / (clients * procs) ))
  (( share_s > 0 )) || share_s=1
  local share_r
  share_r=$(python3 -I -c "print($rate / ($clients * $procs))")
  local pids=() i id stats
  for i in $(seq 0 $(( procs - 1 ))); do
    id=$(( client * procs + i ))
    stats=(); (( id == 0 )) && stats=(--room-stats)
    local extra=()
    [ -n "${ROOM_SOURCE_IPS:-}" ] && extra+=(--source-ip "$ROOM_SOURCE_IPS")
    [ "${ROOM_HTTP_SENDERS:-0}" = 1 ] && extra+=(--http-senders)
    target/release/roomload run --url "$url" --sockets "$share_n" --senders "$share_s" \
      --rate "$share_r" --duration "$secs" --drain "$drain" --payload 200 \
      --id "$id" --procs $(( clients * procs )) --start-at "$start_at" \
      "${stats[@]}" "${extra[@]}" --out "$dir/run-$id.txt" &
    pids+=($!)
  done
  local status=0
  for pid in "${pids[@]}"; do wait "$pid" || status=1; done
  return $status
}

# The cell's room: a fresh name, its write mode first.
room_name() { echo "$1-n$2-r$3-${ROOM_RUN:-$commit}"; }

header() {
  date=$(date -u +%Y-%m-%d)
  cpu=$( (lscpu 2>/dev/null | sed -n 's/^Model name: *//p' | head -1) || true)
  [ -n "$cpu" ] || cpu=unknown
  cores=$(nproc 2>/dev/null || echo unknown)
  rustc=$(rustc --version | cut -d' ' -f2)
  host=${BENCH_HOST:-unspecified}
  thp=$(sed -n 's/.*\[\(.*\)\].*/\1/p' /sys/kernel/mm/transparent_hugepage/enabled 2>/dev/null || true)
  [ -n "$thp" ] || thp=unknown
  echo "date,commit,host,cpu,cores,rustc,bench,median_ns,median_low_ns,median_high_ns,mean_ns,unit,thp"
}

# rows PREFIX SAMPLE NODE_DIED FILES...: the merged cell, in the CSV's columns.
rows() {
  local prefix=$1 sample=$2 died=$3; shift 3
  local flag=(); [ "$died" = 1 ] && flag=(--node-died)
  target/release/roomload merge --prefix "$prefix" --sample "$sample" "${flag[@]}" "$@" \
    | while IFS=, read -r bench value unit; do
        echo "\"$date\",\"$commit\",\"$host\",\"$cpu\",\"$cores\",\"$rustc\",\"$bench\",$value,,,,\"$unit\",\"$thp\""
      done
}

case "$role" in
local)
  : "${ROOM_LABEL:?label the setup, for example ROOM_LABEL=workstation}"
  bash bench/preflight.sh "$(( $(node_fds "$largest") + 4 * largest ))"
  build
  bucket
  work=${ROOM_DIR:-$(mktemp -d -t wasmkeep-room.XXXXXX)}
  mkdir -p bench/results
  out="bench/results/$(date -u +%Y-%m-%d)-${commit}-room-${ROOM_LABEL}.csv"
  partial="$out.partial"
  header > "$partial"
  for n in $sizes; do
    for mode in $modes; do
      for rate in $rates; do
        cell="n$n/$mode/r$rate"
        dir="$work/n$n-$mode-r$rate"
        mkdir -p "$dir"
        listen=127.0.0.1:$(free_port)
        metrics=127.0.0.1:$(free_port)
        cell_prefix=; [ -z "$prefix" ] || cell_prefix="$prefix/$(room_name "$mode" "$n" "$rate")"
        start_node "$listen" "$metrics" "$cell_prefix" "$dir/node" "$n"
        target/release/roomload sample --pid "$node_pid" --metrics "http://$metrics/metrics" \
          --interval-ms 250 --out "$dir/sample.txt" &
        sampler_pid=$!
        # Senders start once every socket is open: room for the ramp-up.
        start_at=$(( $(date +%s%3N) + 2000 + n * 1000 / 2000 ))
        load "ws://$listen/actor/room/$(room_name "$mode" "$n" "$rate")" "$n" "$rate" "$dir" "$start_at" || true
        died=0; kill -0 "$node_pid" 2>/dev/null || died=1
        kill "$sampler_pid" 2>/dev/null || true; wait "$sampler_pid" 2>/dev/null || true
        kill "$node_pid" 2>/dev/null || true; wait "$node_pid" 2>/dev/null || true
        sampler_pid=; node_pid=
        rows "room/$cell" "$dir/sample.txt" "$died" "$dir/"run-*.txt >> "$partial"
      done
    done
  done
  mv "$partial" "$out"
  partial=
  echo "wrote $out"
  ;;
node)
  : "${ROOM_LISTEN:?the address to serve on, for example ROOM_LISTEN=0.0.0.0:8080}"
  : "${WASMKEEP_ROOM_BUCKET:?name the bucket (and AWS_* for it)}"
  bash bench/preflight.sh "$(node_fds "$largest")"
  build
  work=${ROOM_DIR:-$(mktemp -d -t wasmkeep-room.XXXXXX)}
  metrics=127.0.0.1:${ROOM_METRICS_PORT:-9100}
  start_node "$ROOM_LISTEN" "$metrics" "$WASMKEEP_ROOM_BUCKET/room-${ROOM_RUN:-$commit}" "$work/node" "$largest"
  echo "node up on $ROOM_LISTEN (pid $node_pid); sampling to $work/sample.txt; stop with Ctrl-C" >&2
  target/release/roomload sample --pid "$node_pid" --metrics "http://$metrics/metrics" \
    --interval-ms 500 --duration 604800 --out "$work/sample.txt"
  ;;
client)
  : "${ROOM_URL:?the node, for example ROOM_URL=ws://NODE:8080}"
  : "${ROOM_RUN:?name the run, the same on every machine (it names the rooms)}"
  : "${ROOM_T0:?the start of the first cell, Unix seconds, the same on every machine}"
  build
  work=${ROOM_DIR:-$(mktemp -d -t wasmkeep-room.XXXXXX)}
  # Cells start on a shared schedule: ROOM_T0, then one cell every
  # ROOM_CELL_SECS (ramp-up, sending, drain and a margin).
  cell_secs=${ROOM_CELL_SECS:-$(( secs + drain + 30 + largest / 1000 ))}
  k=0
  for n in $sizes; do
    for mode in $modes; do
      for rate in $rates; do
        dir="$work/n$n-$mode-r$rate"
        mkdir -p "$dir"
        t=$(( ROOM_T0 + k * cell_secs ))
        ramp=$(( t - ${ROOM_START_DELAY:-$(( 20 + largest / 2000 ))} ))
        now=$(date +%s)
        (( ramp > now )) && sleep $(( ramp - now ))
        load "$ROOM_URL/actor/room/$(room_name "$mode" "$n" "$rate")" "$n" "$rate" "$dir" $(( t * 1000 )) || true
        k=$(( k + 1 ))
      done
    done
  done
  echo "client $client done: files in $work" >&2
  ;;
merge)
  : "${ROOM_LABEL:?label the setup, for example ROOM_LABEL=fleet-3}"
  : "${ROOM_DIR:?the directory holding the n*-*-r* directories of every client and sample.txt from the node}"
  cargo build --release --locked -p wasmkeep-roomload
  mkdir -p bench/results
  out="bench/results/$(date -u +%Y-%m-%d)-${commit}-room-${ROOM_LABEL}.csv"
  partial="$out.partial"
  header > "$partial"
  for n in $sizes; do
    for mode in $modes; do
      for rate in $rates; do
        dir="$ROOM_DIR/n$n-$mode-r$rate"
        ls "$dir/"run-*.txt >/dev/null 2>&1 || { echo "skipping $dir: no files" >&2; continue; }
        rows "room/n$n/$mode/r$rate" "$ROOM_DIR/sample.txt" 0 "$dir/"run-*.txt >> "$partial"
      done
    done
  done
  mv "$partial" "$out"
  partial=
  echo "wrote $out"
  ;;
*)
  echo "usage: $0 [local|node|client|merge]" >&2; exit 2 ;;
esac
