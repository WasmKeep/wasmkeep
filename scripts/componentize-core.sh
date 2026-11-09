#!/usr/bin/env bash
# Turns a core WebAssembly module built for WASI 0.1 (C, Zig, ... with
# wasi-libc, as a reactor) into a WASI 0.2 component with the official
# adapter, pinned to the Wasmtime version WasmKeep uses.
#
#   scripts/componentize-core.sh core.wasm out.wasm
#
# Needs wasm-tools and curl. The adapter is downloaded once from crates.io
# into target/wasi-adapter/.
set -euo pipefail
ADAPTER_VERSION=50.0.1   # = the wasmtime pin in Cargo.toml
root="$(cd "$(dirname "$0")/.." && pwd)"
dir="$root/target/wasi-adapter/$ADAPTER_VERSION"
adapter="$dir/wasi_snapshot_preview1.reactor.wasm"
if [ ! -f "$adapter" ]; then
  mkdir -p "$dir"
  crate="wasi-preview1-component-adapter-provider-$ADAPTER_VERSION"
  curl -sSfL "https://static.crates.io/crates/wasi-preview1-component-adapter-provider/$crate.crate" \
    | tar xz -C "$dir" "$crate/artefacts/wasi_snapshot_preview1.reactor.wasm"
  mv "$dir/$crate/artefacts/wasi_snapshot_preview1.reactor.wasm" "$adapter"
  rm -rf "${dir:?}/$crate"
fi
wasm-tools component new "$1" --adapt "wasi_snapshot_preview1=$adapter" -o "$2"
