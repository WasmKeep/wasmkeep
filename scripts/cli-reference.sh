#!/usr/bin/env bash
# Regenerates docs/reference/cli.md from `wasmkeep --help` (every command,
# recursively). With --check, changes nothing and fails when the page is
# stale. Builds the CLI first (cargo's own cache makes that quick).
#   ./scripts/cli-reference.sh [--check]
set -euo pipefail
cd "$(dirname "$0")/.."
case "${1:-}" in
  ''|--check) ;;
  *) echo "usage: $0 [--check]" >&2; exit 2 ;;
esac
cargo build --quiet -p wasmkeep-cli
target_dir=$(cargo metadata --format-version 1 --no-deps | python3 -I -c 'import json,sys; print(json.load(sys.stdin)["target_directory"])')
exec python3 -I scripts/cli-reference.py "$target_dir/debug/wasmkeep" "$@"
