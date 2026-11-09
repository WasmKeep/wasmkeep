#!/usr/bin/env bash
# Check the descriptor budget selected by the caller's workload. The full
# comparison keeps 1,002 SQLite actors open; --quick keeps 102. Each actor
# uses roughly three descriptors, with additional headroom for the process.
# This is a benchmark prerequisite, not runtime admission control.
set -euo pipefail

if [[ $# -ne 1 || ! "${1:-}" =~ ^[1-9][0-9]*$ ]]; then
  echo "usage: bash bench/preflight.sh MINIMUM_DESCRIPTORS (positive integer)" >&2
  exit 2
fi
minimum=$1

if ! soft=$(ulimit -Sn) || ! hard=$(ulimit -Hn); then
  echo "error: cannot read the benchmark's inherited file-descriptor limits" >&2
  exit 2
fi

for limit in "$soft" "$hard"; do
  if [[ "$limit" != unlimited && ! "$limit" =~ ^[0-9]+$ ]]; then
    echo "error: unrecognized file-descriptor limit (soft=$soft, hard=$hard); expected a number or unlimited" >&2
    exit 2
  fi
done
if [[ "$soft" == unlimited ]] || (( soft >= minimum )); then
  exit 0
fi

echo "error: benchmarks require a soft file-descriptor limit of at least $minimum (soft=$soft, hard=$hard)" >&2
if [[ "$hard" != unlimited ]] && (( hard < minimum )); then
  echo "The parent shell, service manager or container must allow a hard limit of at least $minimum before retrying." >&2
else
  echo "Adjust only the soft limit explicitly before retrying, for example: ulimit -Sn $minimum" >&2
fi
exit 2
