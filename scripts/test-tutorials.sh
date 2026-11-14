#!/usr/bin/env bash
# Run the tutorials in docs/tutorials/ end to end, with the commands as the
# pages write them, and check what they say you should see. Each tutorial
# with commands to run has a test, scripts/tutorials/<name>.sh, beside its
# page docs/tutorials/<name>.md (scripts/tutorials/lib.sh says how they
# work). To add one: mark the page's blocks with `<!-- test: ID -->` and
# write its test.
#
#   ./scripts/test-tutorials.sh                     every tutorial test
#   ./scripts/test-tutorials.sh one-node-to-three   one of them
#
# A test whose tools are missing (the local S3 store, a Rust target) is
# skipped and listed; with WASMKEEP_TUTORIALS_NO_SKIP=1 (the nightly CI) a
# skip fails the run. Each test runs at most TUTORIAL_TIMEOUT seconds
# (default 900). Not part of ./scripts/check.sh: it builds the server in
# release mode and runs a cluster for a few minutes.
set -euo pipefail
cd "$(dirname "$0")/.."
root=$PWD
export TMPDIR=${TMPDIR:-$root/target/tmp}
mkdir -p "$TMPDIR"

names=("$@")
if [ "${#names[@]}" -eq 0 ]; then
  for test in scripts/tutorials/*.sh; do
    name=$(basename "$test" .sh)
    [ "$name" = lib ] || names+=("$name")
  done
fi

passed=(); skipped=(); failed=()
for name in "${names[@]}"; do
  test=scripts/tutorials/$name.sh
  page=docs/tutorials/$name.md
  [ -f "$test" ] || { echo "no test $test" >&2; exit 2; }
  [ -f "$page" ] || { echo "no page $page for $test" >&2; exit 2; }
  printf '\n== tutorial %s\n' "$name"
  set +e
  TUTORIAL=$page timeout --signal=TERM --kill-after=30 "${TUTORIAL_TIMEOUT:-900}" \
    bash -c 'set -euo pipefail; source scripts/tutorials/lib.sh; source "$1"' bash "$test"
  status=$?
  set -e
  case $status in
    0) passed+=("$name") ;;
    77) skipped+=("$name") ;;
    124) echo "tutorial $name timed out" >&2; failed+=("$name") ;;
    *) failed+=("$name") ;;
  esac
done

printf '\n== tutorials\n'
[ "${#passed[@]}" -eq 0 ] || printf 'passed: %s\n' "${passed[*]}"
[ "${#skipped[@]}" -eq 0 ] || printf 'skipped: %s\n' "${skipped[*]}"
[ "${#failed[@]}" -eq 0 ] || printf 'failed: %s\n' "${failed[*]}"
if [ "${#failed[@]}" -gt 0 ]; then exit 1; fi
if [ "${#skipped[@]}" -gt 0 ] && [ "${WASMKEEP_TUTORIALS_NO_SKIP:-}" = 1 ]; then
  echo 'a tutorial was skipped and WASMKEEP_TUTORIALS_NO_SKIP=1' >&2; exit 1
fi
