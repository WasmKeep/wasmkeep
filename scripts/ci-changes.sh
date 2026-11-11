#!/usr/bin/env bash
# Says whether a CI run must run the code jobs: prints "code=true" or
# "code=false" (.github/workflows/ci.yml, job "changes").
#
#   scripts/ci-changes.sh <event>      in the checkout CI made
#
# code=false only for a pull request whose every changed path is on the
# list below: documentation and benchmark result files, which no code job
# reads. The "docs site" and "benchmark publication paths" jobs check those
# and always run. Everything else answers code=true: another event, a
# checkout that is not a pull request's merge commit, a diff that fails or
# is empty, and any path not on the list.
#
# A file read by a test or a build is code even under docs/: add it next to
# docs/reference/api.md.
set -uo pipefail

code=true
classify() {
  [ "${1:-}" = pull_request ] || { echo "event '${1:-}': everything runs" >&2; return; }
  # A pull request is checked out as its merge into the base: the first
  # parent is the base, the second the change.
  git rev-parse -q --verify 'HEAD^2' >/dev/null \
    || { echo 'HEAD is not a merge commit: everything runs' >&2; return; }
  local files f
  # --no-renames: a file moved onto the list still shows the path it left.
  # quotePath: a path git has to quote matches nothing below, so it is code.
  files=$(git -c core.quotePath=true diff --no-renames --name-only 'HEAD^1' HEAD) \
    || { echo 'git diff failed: everything runs' >&2; return; }
  [ -n "$files" ] || { echo 'no changed path: everything runs' >&2; return; }
  code=false
  while IFS= read -r f; do
    case "$f" in
      docs/reference/api.md) ;;   # compared with the WIT by a Rust test
      README.md|ROADMAP.md|CHANGELOG.md|CONTRIBUTING.md|CLAUDE.md) continue ;;
      docs/*.md|docs/book.toml) continue ;;
      bench/results/*) continue ;;
      deploy/*/README.md|sdks/*/README.md|examples/*/README.md) continue ;;
    esac
    echo "code: $f" >&2
    code=true
  done <<<"$files"
}
classify "$@"
echo "code=$code"
