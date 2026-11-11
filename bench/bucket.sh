#!/usr/bin/env bash
# The one-node comparison on a bucket, alone (bench/compare --bucket, the
# compare-bucket/ rows that bench/run.sh also writes): WasmKeep's Rust
# counter, plain and durable, and celld, each one node on the same bucket.
# Writes bench/results/<date>-<commit>-bucket-<label>.csv in bench/run.sh's
# columns, only if the run finished. Numbers in docs must come from these
# files.
#
# Needs WASMKEEP_COMPARE_BUCKET=s3://BUCKET[/PREFIX] and AWS_* for it, a
# label for the store and for where the nodes keep their data
# (BUCKET_LABEL=minio-tmpfs-data-disk), and bench/celld/fetch.sh run once.
# Nodes keep their data in the temporary directory, or under
# BUCKET_DATA_DIR if set: name a directory on the disk to measure with a
# real fsync. Run it several times under different labels (-r1, -r2) to see
# what one run varies by. Extra arguments go to compare (--quick for a
# short run).
set -euo pipefail
cd "$(dirname "$0")/.."
: "${WASMKEEP_COMPARE_BUCKET:?name the bucket (and AWS_* for its endpoint and keys)}"
: "${BUCKET_LABEL:?label the store and the data directory, for example BUCKET_LABEL=r2-data-disk}"

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
targets="wasmkeep-rust wasmkeep-rust-durable"
if [ -x "${CELLD_BIN:-target/celld/v0.6.3/celld}" ]; then
  targets="$targets celld"
else
  echo "skipping celld: run bench/celld/fetch.sh (or set CELLD_BIN)" >&2
fi

if [ -n "${BUCKET_DATA_DIR:-}" ]; then
  mkdir -p "$BUCKET_DATA_DIR"
  TMPDIR=$(cd "$BUCKET_DATA_DIR" && pwd)
  export TMPDIR
fi

mkdir -p bench/results
suffix=""
for arg in "$@"; do [ "$arg" = --quick ] && suffix=-quick; done
out="bench/results/${date}-${commit}-bucket-${BUCKET_LABEL}${suffix}.csv"
# Rows go to a partial file, renamed only when the run has finished.
partial="$out.partial"
trap 'rm -f "$partial"' EXIT
echo "date,commit,host,cpu,cores,rustc,bench,median_ns,median_low_ns,median_high_ns,mean_ns,unit,thp" > "$partial"
# shellcheck disable=SC2086
target/release/compare --bucket "$@" $targets | while IFS=, read -r bench value unit; do
  echo "\"$date\",\"$commit\",\"$host\",\"$cpu\",\"$cores\",\"$rustc\",\"$bench\",$value,,,,\"$unit\",\"$thp\"" >> "$partial"
done
mv "$partial" "$out"
trap - EXIT
echo "wrote $out"
if command -v column >/dev/null; then column -s, -t "$out" | cut -c1-200; else cat "$out"; fi
