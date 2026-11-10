#!/usr/bin/env bash
# The three-node comparison (bench/compare --cluster): WasmKeep and celld,
# three nodes each on one bucket, steady state then a takeover per node.
# Writes bench/results/<date>-<commit>-cluster-<label>.csv in bench/run.sh's
# columns. Numbers in docs must come from these files.
#
# Needs WASMKEEP_COMPARE_BUCKET=s3://BUCKET[/PREFIX] and AWS_* for it, a
# label for the store (CLUSTER_LABEL=minio-tmpfs, CLUSTER_LABEL=r2), and
# bench/celld/fetch.sh run once. Nodes keep their data under
# target/compare-cluster/, on the repository's disk. Extra arguments go to
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
targets="wasmkeep-rust-durable celld-fleet celld-bucket wasmkeep-rust"

mkdir -p bench/results
suffix=""
for arg in "$@"; do [ "$arg" = --quick ] && suffix=-quick; done
out="bench/results/${date}-${commit}-cluster-${CLUSTER_LABEL}${suffix}.csv"
echo "date,commit,host,cpu,cores,rustc,bench,median_ns,median_low_ns,median_high_ns,mean_ns,unit,thp" > "$out"
# shellcheck disable=SC2086
target/release/compare --cluster "$@" $targets | while IFS=, read -r bench value unit; do
  echo "\"$date\",\"$commit\",\"$host\",\"$cpu\",\"$cores\",\"$rustc\",\"$bench\",$value,,,,\"$unit\",\"$thp\"" >> "$out"
done
echo "wrote $out"
if command -v column >/dev/null; then column -s, -t "$out" | cut -c1-200; else cat "$out"; fi
