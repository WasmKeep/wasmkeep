#!/usr/bin/env bash
# Builds counter.wasm with standard Go (not TinyGo) through componentize-go:
# bindings from the actor world (generated here on every build, ignored by
# git), then `go build` for wasip1 and the WASI 0.2 adapter it embeds.
#   Go 1.27.1 or newer (GO_STD names it when the go on PATH is older, as
#   for TinyGo's 1.24)
#   go install github.com/bytecodealliance/componentize-go@v0.5.1
# componentize-go downloads its own binary from its GitHub release into
# the user's cache directory on first use.
set -euo pipefail
cd "$(dirname "$0")"
WIT=../../crates/wasmkeep-wit/wit
GO=$(command -v "${GO_STD:-go}")
export PATH="$(dirname "$GO"):$PATH" GOTOOLCHAIN=local   # componentize-go runs `go list` too
COMPONENTIZE_GO=${COMPONENTIZE_GO:-componentize-go}
rm -rf wit_world wit_exports.go wasi_* wasmkeep_actor_*
# `bindings` rewrites go.mod from its own template; keep the committed one.
cp go.mod go.mod.keep
"$COMPONENTIZE_GO" -d "$WIT" -w actor bindings >/dev/null
mv go.mod.keep go.mod
GOFLAGS="${GOFLAGS:-} -buildvcs=false" "$COMPONENTIZE_GO" -d "$WIT" -w actor build --go "$GO" -o counter.wasm
echo "wrote $(pwd)/counter.wasm ($(wc -c < counter.wasm) bytes)"
