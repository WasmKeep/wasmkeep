#!/usr/bin/env bash
# The three-node comparison (bench/compare --cluster): WasmKeep and celld,
# three nodes each on one bucket, steady state then a takeover per node
# (cluster3/ rows); the takeovers again with the killed node's disk removed
# (cluster3-wipe/ rows); and a control that must lose writes, which shows
# the lost-write check finds them (cluster3-control/ rows). Writes
# bench/results/<date>-<commit>-cluster-<label>.csv in bench/run.sh's
# columns, only if every run finished and the control lost writes. Numbers
# in docs must come from these files.
#
# Needs WASMKEEP_COMPARE_BUCKET=s3://BUCKET[/PREFIX] and AWS_* for it, a
# label for the store (CLUSTER_LABEL=minio-tmpfs, CLUSTER_LABEL=r2), and
# bench/celld/fetch.sh run once. Nodes keep their data under
# target/compare-cluster/, on the repository's disk, or under
# CLUSTER_DATA_DIR if set (say so in the label). Extra arguments go to
# compare (--quick for a short run).
set -euo pipefail
cd "$(dirname "$0")/.."
: "${WASMKEEP_COMPARE_BUCKET:?name the bucket (and AWS_* for its endpoint and keys)}"
: "${CLUSTER_LABEL:?label the store, for example CLUSTER_LABEL=r2}"

commit=$(git rev-parse --short HEAD)
git diff --quiet HEAD -- . ':!bench/results' || commit="${commit}-dirty"
date=$(date -u +%Y-%m-%d)
cpu=$( (lscpu 2>/dev/null | sed -n 's/^Model name: *//p' | head -1) || true)
[ -n "$cpu" ] || cpu=$(sysctl -n machdep.cpu.brand_string 2>/dev/null || echo unknown)
cores=$(nproc 2>/dev/null || sysctl -n hw.ncpu)
rustc=$(rustc --version | cut -d' ' -f2)
host=${BENCH_HOST:-unspecified}
thp=$(sed -n 's/.*\[\(.*\)\].*/\1/p' /sys/kernel/mm/transparent_hugepage/enabled 2>/dev/null || true)
[ -n "$thp" ] || thp=unknown

cargo build --release -p wasmkeep-cli -p wasmkeep-compare
(cd examples/counter && cargo build --release --target wasm32-wasip2 --locked)
(cd examples/counter && cargo build --release --target wasm32-wasip2 --locked \
  --features durable --target-dir target/durable)
(cd bench/ledger && cargo build --release --target wasm32-wasip2 --locked)
(cd bench/ledger && cargo build --release --target wasm32-wasip2 --locked \
  --features durable --target-dir target/durable)
durable="wasmkeep-rust-durable celld-fleet celld-bucket"

mkdir -p bench/results
suffix=""
for arg in "$@"; do [ "$arg" = --quick ] && suffix=-quick; done
out="bench/results/${date}-${commit}-cluster-${CLUSTER_LABEL}${suffix}.csv"
# Rows go to a partial file, renamed only when every run has finished.
partial="$out.partial"
trap 'rm -f "$partial"' EXIT
echo "date,commit,host,cpu,cores,rustc,bench,median_ns,median_low_ns,median_high_ns,mean_ns,unit,thp" > "$partial"
rows() {
  while IFS=, read -r bench value unit; do
    echo "\"$date\",\"$commit\",\"$host\",\"$cpu\",\"$cores\",\"$rustc\",\"$bench\",$value,,,,\"$unit\",\"$thp\"" >> "$partial"
  done
}
# shellcheck disable=SC2086
target/release/compare --cluster "$@" $durable wasmkeep-rust | rows
# shellcheck disable=SC2086
target/release/compare --cluster --wipe "$@" $durable | rows
# Fails, and so writes no file, if the control loses nothing.
target/release/compare --cluster --control "$@" wasmkeep-rust | rows
mv "$partial" "$out"
trap - EXIT
echo "wrote $out"
if command -v column >/dev/null; then column -s, -t "$out" | cut -c1-200; else cat "$out"; fi
