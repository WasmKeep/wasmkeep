#!/usr/bin/env bash
# Builds jobs.wasm: Go bindings with wit-bindgen-go, then TinyGo's
# wasip2 target, which writes a component directly (no adapter).
#   TinyGo 0.39 (with Go 1.24): https://tinygo.org/getting-started/install/
#   go install go.bytecodealliance.org/cmd/wit-bindgen-go@v0.7.0
set -euo pipefail
cd "$(dirname "$0")"
WIT=../../crates/wasmkeep-wit/wit
TINYGO=${TINYGO:-tinygo}
WIT_BINDGEN_GO=${WIT_BINDGEN_GO:-wit-bindgen-go}
rm -rf gen
mkdir -p gen
# TinyGo's runtime imports wasi:filesystem whether or not the program uses
# it, so the component is encoded against the actor world plus those
# imports (the host provides them inert: no directories). The bindings
# still come from the plain actor world.
cp -r "$WIT" gen/wit
cat > gen/wit/tinygo.wit <<'WIT'
package wasmkeep:actor@0.2.0;

world actor-tinygo {
    include actor;
    import wasi:filesystem/types@0.2.12;
    import wasi:filesystem/preopens@0.2.12;
}
WIT
"$WIT_BINDGEN_GO" generate --world actor --out gen "$WIT" >/dev/null
"$TINYGO" build -target=wasip2 -no-debug -opt=2 \
  --wit-package gen/wit --wit-world actor-tinygo -o jobs.wasm .
echo "wrote $(pwd)/jobs.wasm ($(wc -c < jobs.wasm) bytes)"
