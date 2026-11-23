#!/usr/bin/env bash
# Checks a toolchain image (deploy/toolchain/Dockerfile) builds what it
# says, run inside it with a checkout of this repository mounted:
#
#   podman run --rm -v "$PWD:/src:ro" IMAGE deploy/toolchain/test.sh
#
# It copies the checkout to a directory of its own (without target/,
# node_modules/ or .git), so the mounted one is only read, then:
#   1. examples/todo-front as its wasmkeep.toml builds it: `wasmkeep dev`
#      runs both builds and serves the app, and a signed-in request adds an
#      item through the front to the internal actor;
#   2. a counter in every language the image is for (Rust, TypeScript,
#      Python, Go on TinyGo, C, Zig), each a valid component.
# npm, cargo and the Go module proxy download what the examples lock.
set -euo pipefail
src=$(cd "$(dirname "$0")/../.." && pwd)
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
tar -C "$src" --exclude=./.git --exclude='./target' --exclude='*/target' \
  --exclude='*/node_modules' --exclude='./.wasmkeep' -cf - . | tar -C "$work" -xf -
cd "$work"
wit=crates/wasmkeep-wit/wit

echo '-- examples/todo-front with wasmkeep dev'
# The tutorial's place for a copy (docs/tutorials/first-app.md): its crates
# find the SDK at the same depth.
mkdir -p .wasmkeep
cp -r examples/todo-front .wasmkeep/todo
token=$(od -An -N16 -tx1 /dev/urandom | tr -d ' \n')
echo "TOKENS=alice:$token" > .wasmkeep/todo/.env.local
chmod 600 .wasmkeep/todo/.env.local
(cd .wasmkeep/todo && exec wasmkeep dev --listen 127.0.0.1:18080 > dev.log 2>&1) &
dev=$!
up=false
for _ in $(seq 600); do
  # Any answer: the front answers 401 without a token.
  if curl -s -o /dev/null http://127.0.0.1:18080/; then up=true; break; fi
  kill -0 "$dev" 2>/dev/null || break
  sleep 1
done
$up || { cat .wasmkeep/todo/dev.log; echo 'wasmkeep dev did not serve the app' >&2; exit 1; }
status=$(curl -sS -o /dev/null -w '%{http_code}' -X POST --data milk \
  -H "Authorization: Bearer $token" http://127.0.0.1:18080/api/todos)
list=$(curl -fsS -H "Authorization: Bearer $token" http://127.0.0.1:18080/api/todos)
kill "$dev"; wait "$dev" 2>/dev/null || true
[ "$status" = 201 ] && [[ $list == *milk* ]] || {
  cat .wasmkeep/todo/dev.log; echo "POST answered $status, the list is $list" >&2; exit 1;
}
echo "ok: the front added an item (201) and listed it"

counter() { # a component, and whether to check it against the actor world
  wasm-tools validate "$1"
  # As CI's examples job: the Rust ones only (a QuickJS or CPython
  # component imports more of WASI than the world, which the host links).
  if [ "${2:-}" = world ]; then wasm-tools component targets "$wit" --world actor "$1"; fi
  echo "ok: $1 ($(wc -c < "$1") bytes)"
}
echo '-- Rust'
(cd examples/counter && cargo build --release --locked --target wasm32-wasip2 --quiet)
counter examples/counter/target/wasm32-wasip2/release/counter.wasm world
echo '-- TypeScript'
(cd sdks/typescript && npm ci --silent)
(cd examples/ts-counter && npm ci --silent && npm run --silent build)
counter examples/ts-counter/counter.wasm
echo '-- Python'
examples/py-counter/build.sh
counter examples/py-counter/counter.wasm
echo '-- Go (TinyGo)'
examples/go-counter/build.sh
counter examples/go-counter/counter.wasm
echo '-- C'
examples/c-counter/build.sh
counter examples/c-counter/counter.wasm
echo '-- Zig'
examples/zig-counter/build.sh
counter examples/zig-counter/counter.wasm
echo 'the toolchain image builds every example above'
