#!/usr/bin/env bash
# Runs the benchmarks and writes bench/results/<date>-<commit>.csv.
# Numbers in docs or posts must come from a file produced by this script.
set -euo pipefail
cd "$(dirname "$0")/.."
bash bench/preflight.sh 4096

commit=$(git rev-parse --short HEAD)
head_at_start=$(git rev-parse HEAD)
git diff --quiet HEAD -- . ':!bench/results' || commit="${commit}-dirty"
date=$(date -u +%Y-%m-%d)
cpu=$( (lscpu 2>/dev/null | sed -n 's/^Model name: *//p' | head -1) || true)
[ -n "$cpu" ] || cpu=$(sysctl -n machdep.cpu.brand_string 2>/dev/null || echo unknown)
cores=$(nproc 2>/dev/null || sysctl -n hw.ncpu)
rustc=$(rustc --version | cut -d' ' -f2)
host=${BENCH_HOST:-unspecified}
# The kernel's transparent huge page setting (the bracketed word), last
# column of every row: memory per actor depended on it before the server
# turned them off for itself (docs/benchmarks.md#reproduce).
thp=$(sed -n 's/.*\[\(.*\)\].*/\1/p' /sys/kernel/mm/transparent_hugepage/enabled 2>/dev/null || true)
[ -n "$thp" ] || thp=unknown

rm -rf target/criterion
cargo bench -p wasmkeep-runtime -p wasmkeep-server "$@"

mkdir -p bench/results
out="bench/results/${date}-${commit}.csv"
echo "date,commit,host,cpu,cores,rustc,bench,median_ns,median_low_ns,median_high_ns,mean_ns,unit" > "$out"
find target/criterion -path '*/new/estimates.json' | sort | while read -r est; do
  bench=${est#target/criterion/}
  bench=${bench%/new/estimates.json}
  jq -r --arg d "$date" --arg c "$commit" --arg h "$host" --arg cpu "$cpu" \
        --arg cores "$cores" --arg r "$rustc" --arg b "$bench" \
    '[$d, $c, $h, $cpu, $cores, $r, $b,
      (.median.point_estimate|floor),
      (.median.confidence_interval.lower_bound|floor),
      (.median.confidence_interval.upper_bound|floor),
      (.mean.point_estimate|floor), "ns"] | @csv' "$est"
done >> "$out"
# Failover: only with an S3 API to run against (see crates/wasmkeep-cli/tests/failover.rs).
if [ -n "${WASMKEEP_TEST_S3_BUCKET:-}" ]; then
  for run in 1 2 3; do
    # A failed run (no endpoint, a bug) records nothing rather than aborting.
    ms=$( (cargo nextest run -p wasmkeep-cli --test failover --no-capture 2>&1 || true) | sed -n 's/.*failover_ms=\([0-9]*\).*/\1/p' | head -1)
    if [ -n "$ms" ]; then
      ns=$((ms * 1000000))
      echo "\"$date\",\"$commit\",\"$host\",\"$cpu\",\"$cores\",\"$rustc\",\"failover/kill9-run$run\",$ns,$ns,$ns,$ns,\"ns\"" >> "$out"
    fi
  done
fi
# Comparison with other runtimes (bench/compare): each runs as its own
# server process, driven by the same client. Targets whose toolchain or
# build is missing are skipped.
cargo build --release -p wasmkeep-cli -p wasmkeep-compare
(cd examples/counter && cargo build --release --target wasm32-wasip2 --locked)
# The same counter, its POST durable before it returns (`exec-durable`).
(cd examples/counter && cargo build --release --target wasm32-wasip2 --locked \
  --features durable --target-dir target/durable)
targets="wasmkeep-rust wasmkeep-rust-durable"
# The same counter in the other guest languages, when their toolchain is
# here (see each example's build.sh for what it needs).
for lang in zig c go py; do
  if examples/$lang-counter/build.sh >/dev/null 2>&1; then
    targets="$targets wasmkeep-$lang"
  else
    echo "skipping wasmkeep-$lang: examples/$lang-counter/build.sh failed (toolchain missing?)" >&2
  fi
done
if command -v npm >/dev/null; then
  (cd sdks/typescript && npm ci --no-audit --no-fund) \
    && (cd examples/ts-counter && npm ci --no-audit --no-fund && npm run build) \
    && targets="$targets wasmkeep-ts" \
    && (cd examples/ts-counter && npm run build:starlingmonkey) \
    && targets="$targets wasmkeep-ts-starlingmonkey"
  (cd bench/workerd && npm ci --no-audit --no-fund) && targets="$targets workerd"
fi
target/release/compare $targets | while IFS=, read -r bench value unit; do
  echo "\"$date\",\"$commit\",\"$host\",\"$cpu\",\"$cores\",\"$rustc\",\"$bench\",$value,,,,\"$unit\"" >> "$out"
done
# The Rust counter again as a one-node cluster on a bucket (compare-bucket/
# rows), and celld on the same bucket when bench/celld/fetch.sh has
# downloaded it: only with WASMKEEP_COMPARE_BUCKET=s3://... and AWS_*
# pointing at it.
if [ -n "${WASMKEEP_COMPARE_BUCKET:-}" ]; then
  bucket_targets="wasmkeep-rust wasmkeep-rust-durable"
  if [ -x "${CELLD_BIN:-target/celld/v0.6.3/celld}" ]; then
    bucket_targets="$bucket_targets celld"
  else
    echo "skipping celld: run bench/celld/fetch.sh (or set CELLD_BIN)" >&2
  fi
  target/release/compare --bucket $bucket_targets | while IFS=, read -r bench value unit; do
    echo "\"$date\",\"$commit\",\"$host\",\"$cpu\",\"$cores\",\"$rustc\",\"$bench\",$value,,,,\"$unit\"" >> "$out"
  done
fi

# Every row gets the huge page setting as its last column.
sed -e '1s/$/,thp/' -e "2,\$s/\$/,\"$thp\"/" "$out" > "$out.tmp" && mv "$out.tmp" "$out"

# Cargo builds as it goes: a source change, or another commit checked out,
# during the run may be in the numbers. Say so in the file name.
if [ "$(git rev-parse HEAD)" != "$head_at_start" ] \
   || ! git diff --quiet HEAD -- . ':!bench/results'; then
  case "$commit" in
    *-dirty) ;;
    *) sed "s/,\"$commit\",/,\"$commit-dirty\",/" "$out" > "${out%.csv}-dirty.csv"
       rm "$out"; out="${out%.csv}-dirty.csv"
       echo "warning: the tree changed during the run; results marked dirty" >&2 ;;
  esac
fi
echo "wrote $out"
if command -v column >/dev/null; then column -s, -t "$out" | cut -c1-200; else cat "$out"; fi
