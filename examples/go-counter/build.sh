#!/usr/bin/env bash
# Builds counter.wasm with TinyGo's wasip2 target, which writes a component
# directly (no adapter), against the WIT the Go SDK ships (sdks/go/wit).
#   TinyGo 0.39 (with Go 1.24): https://tinygo.org/getting-started/install/
set -euo pipefail
cd "$(dirname "$0")"
TINYGO=${TINYGO:-tinygo}
SDK=$(go list -m -f '{{.Dir}}' github.com/wasmkeep/wasmkeep/sdks/go)
"$TINYGO" build -target=wasip2 -no-debug -opt=2 \
  --wit-package "$SDK/wit" --wit-world actor-tinygo -o counter.wasm .
echo "wrote $(pwd)/counter.wasm ($(wc -c < counter.wasm) bytes)"
