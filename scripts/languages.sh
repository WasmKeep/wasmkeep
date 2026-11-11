#!/usr/bin/env bash
# What the CI's "examples" and "languages" jobs run, locally (part of
# `scripts/check.sh --full`):
#   1. every Rust example: fmt, clippy, a release build for wasm32-wasip2,
#      and its component checked against the world it claims;
#   2. the TypeScript SDK: its WIT and types in sync with the host, the type
#      check and its tests;
#   3. every example in another language built (TypeScript on both engines,
#      C, Zig, Python, Go on TinyGo and standard Go);
#   4. the counter contract, the chat suite, jobs and workflows against every
#      language, with WASMKEEP_REQUIRE_LANGS=1: a component that is missing
#      fails instead of being skipped.
# It installs no toolchain (see `scripts/doctor.sh --full`), except that it
# runs `npm ci` where a TypeScript package has no node_modules yet.
set -euo pipefail
cd "$(dirname "$0")/.."
# Languages whose tools need setup beyond installing a package are built
# only when that setup is there; otherwise they are skipped, listed in
# $LANGUAGES_SKIPPED (check.sh shows them as not run), and their tests
# are left out. CI builds them all.
skipped_list=${LANGUAGES_SKIPPED:-/dev/null}
tests='all()'
skip_lang() { # name, the tests to leave out
  echo "skipped: $1"
  echo "$1" >> "$skipped_list"
  tests="$tests - ($2)"
}
root=$(pwd)
wit="$root/crates/wasmkeep-wit/wit"

# Rust examples: directory, then the component's file name ("" means the
# directory name, dashes as underscores), then the world ("" for plain
# wasi:http).
rust_examples=(
  'hello||'
  'counter||actor'
  'rate-limiter||actor'
  'chat-room||actor'
  'watch/monitor|watch_monitor|actor'
  'watch/hub|watch_hub|actor'
  'agent||actor'
  'jobs/kv|jobs_kv|actor'
  'jobs/queue|jobs_queue|actor'
  'jobs/app|jobs_app|actor'
  'workflows/orders|workflows_orders|actor'
  'workflows/ledger|workflows_ledger|actor'
  'workflows/app|workflows_app|actor'
)
for entry in "${rust_examples[@]}"; do
  IFS='|' read -r dir name world <<<"$entry"
  [ -n "$name" ] || name=$(tr - _ <<<"$dir")
  echo "-- examples/$dir"
  (
    cd "examples/$dir"
    cargo fmt --check
    cargo clippy --release --target wasm32-wasip2 --locked --quiet -- -D warnings
    cargo build --release --target wasm32-wasip2 --locked --quiet
    wasm="target/wasm32-wasip2/release/$name.wasm"
    wasm-tools validate "$wasm"
    if [ -n "$world" ]; then wasm-tools component targets "$wit" --world "$world" "$wasm"; fi
  )
done

echo '-- sdks/typescript'
(
  cd sdks/typescript
  [ -d node_modules ] || npm ci --silent
  npm run --silent sync
  git diff --exit-code -- wit types
  npm run --silent check
  npm test --silent
)

for ex in examples/ts-*/; do
  echo "-- $ex"
  (cd "$ex" && { [ -d node_modules ] || npm ci --silent; } && npm run --silent build && npm run --silent build:starlingmonkey)
done
for ex in examples/c-*/ examples/py-*/ examples/go-*/; do
  [ "$ex" = examples/go-std-counter/ ] && continue
  echo "-- $ex"
  "$ex/build.sh"
done
# Standard Go: componentize-go on PATH (it downloads its own binary into
# the user's cache directory on first use) and Go 1.27.1+ ($GO_STD, or go).
if command -v "${COMPONENTIZE_GO:-componentize-go}" >/dev/null 2>&1; then
  echo '-- examples/go-std-counter/'
  examples/go-std-counter/build.sh
else
  skip_lang 'standard Go (componentize-go on PATH, Go 1.27.1+)' 'test(go_standard)'
fi
for ex in examples/zig-*/; do
  echo "-- $ex"
  ZIG="${ZIG:-zig}" "$ex/build.sh"
done

echo '-- every language against the same tests'
WASMKEEP_REQUIRE_LANGS=1 cargo nextest run -p wasmkeep-server --locked -E "$tests" \
  --test languages --test chat --test jobs_langs --test workflows_ts --test workflows_langs
