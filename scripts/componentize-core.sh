#!/usr/bin/env bash
# Turns a core WebAssembly module built for WASI 0.1 (C, Zig, ... with
# wasi-libc, as a reactor) into a WASI 0.2 component with the official
# adapter, pinned to the Wasmtime version WasmKeep uses.
#
#   scripts/componentize-core.sh core.wasm out.wasm
#
# Needs wasm-tools and curl. The adapter is downloaded once from crates.io
# into target/wasi-adapter/ and checked against a pinned SHA-256 before it is
# unpacked. For another version, take the new `cksum` from
# https://index.crates.io/wa/si/wasi-preview1-component-adapter-provider.
set -euo pipefail
ADAPTER_VERSION=50.0.1   # the guest-side WASI 0.1 adapter; the host pin in Cargo.toml may be a later patch
ADAPTER_CRATE_SHA256=08af31e188aec735fabb6370000e3c5954eae92d434a63ad22f665b55b01dda4   # of the .crate, as crates.io's index lists it
root="$(cd "$(dirname "$0")/.." && pwd)"
dir="$root/target/wasi-adapter/$ADAPTER_VERSION"
adapter="$dir/wasi_snapshot_preview1.reactor.wasm"
if [ ! -f "$adapter" ]; then
  mkdir -p "$dir"
  crate="wasi-preview1-component-adapter-provider-$ADAPTER_VERSION"
  tmp=$(mktemp)
  trap 'rm -f "$tmp"' EXIT
  curl -sSfL -o "$tmp" "https://static.crates.io/crates/wasi-preview1-component-adapter-provider/$crate.crate"
  echo "$ADAPTER_CRATE_SHA256  $tmp" | sha256sum -c --quiet \
    || { echo "componentize-core.sh: $crate.crate does not match its pinned SHA-256" >&2; exit 1; }
  tar xzf "$tmp" -C "$dir" "$crate/artefacts/wasi_snapshot_preview1.reactor.wasm"
  mv "$dir/$crate/artefacts/wasi_snapshot_preview1.reactor.wasm" "$adapter"
  rm -rf "${dir:?}/$crate"
fi
wasm-tools component new "$1" --adapt "wasi_snapshot_preview1=$adapter" -o "$2"
