#!/usr/bin/env bash
# The three-node soak against a real bucket (crates/wasmkeep-cli/tests/soak.rs)
# and writes bench/results/<date>-<commit>-soak-<label>.csv, in bench/run.sh's
# columns. Numbers in docs must come from these files.
#
# Needs WASMKEEP_TEST_S3_BUCKET and AWS_* for the bucket (an env file:
# `set -a; . file; set +a`), and a label for the store: SOAK_LABEL=r2.
# WASMKEEP_SOAK_SECS (1800 by default) and WASMKEEP_SOAK_KILL_EVERY (180)
# set its length and how often a node is killed. SOAK_WRITES=batched runs
# the soak's class with batched plain writes (ADR 0034): the binary is
# built with the `batched-writes` feature and the nodes start with
# `--writes t=batched`; strict (the default) runs it as before. The mode is
# part of the results file's name and of each row's bench name.
set -euo pipefail
cd "$(dirname "$0")/.."
: "${WASMKEEP_TEST_S3_BUCKET:?name the bucket (and AWS_* for its endpoint and keys)}"
: "${SOAK_LABEL:?label the store, for example SOAK_LABEL=r2}"
export WASMKEEP_SOAK_SECS=${WASMKEEP_SOAK_SECS:-1800}
export WASMKEEP_SOAK_KILL_EVERY=${WASMKEEP_SOAK_KILL_EVERY:-180}
writes=${SOAK_WRITES:-strict}
case "$writes" in
  strict) features=() ;;
  batched) features=(--features batched-writes) ;;
  *) echo "SOAK_WRITES=$writes: strict or batched" >&2; exit 2 ;;
esac
export WASMKEEP_SOAK_WRITES=$writes
bash bench/preflight.sh 4096

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

log=$(mktemp)
trap 'rm -f "$log"' EXIT
# Interrupted: stop the harness and the nodes it started (its whole process
# group); remote nodes it killed are started again by the harness on its
# way out, and here once more in case it was killed first.
interrupted() {
  trap '' INT TERM
  kill -TERM -- -$$ 2>/dev/null || true
  if [ -n "${WASMKEEP_SOAK_NODES:-}" ] && [ -n "${WASMKEEP_SOAK_START:-}" ]; then
    for id in a b c; do
      sh -c "$(printf '%s' "$WASMKEEP_SOAK_START" | sed "s|{id}|$id|g")" || true
    done
  fi
  exit 130
}
trap interrupted INT TERM
status=0
cargo nextest run -p wasmkeep-cli "${features[@]}" --test soak --no-capture 2>&1 | tee "$log" || status=$?

mkdir -p bench/results
suffix=""
[ "$writes" = strict ] || suffix="-$writes"
out="bench/results/${date}-${commit}-soak-${SOAK_LABEL}${suffix}.csv"
echo "date,commit,host,cpu,cores,rustc,bench,median_ns,median_low_ns,median_high_ns,mean_ns,unit,thp" > "$out"
sed -n 's/^soak_metric \([^ ]*\) \([0-9]*\) \([a-z]*\)$/\1 \2 \3/p' "$log" | while read -r name value unit; do
  [ "$writes" = strict ] || name="${name}/${writes}"
  echo "\"$date\",\"$commit\",\"$host\",\"$cpu\",\"$cores\",\"$rustc\",\"$name\",$value,,,,\"$unit\",\"$thp\"" >> "$out"
done
echo "wrote $out"
exit "$status"
