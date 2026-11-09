#!/usr/bin/env bash
# Builds jobs.wasm: bindings with wit-bindgen, the C SDK (sdks/c), clang + wasi-libc for
# wasm32-wasi (a reactor), then the WASI 0.2 adapter.
#   Ubuntu/Debian: apt install clang lld wasi-libc libclang-rt-18-dev-wasm32
#   Arch: pacman -S clang lld wasi-libc wasi-compiler-rt (sysroot /usr/share/wasi-sysroot)
#   cargo install wit-bindgen-cli --version 0.63.0 && cargo install wasm-tools
# With wasi-sdk instead, set CC=$WASI_SDK/bin/clang and SYSROOT=$WASI_SDK/share/wasi-sysroot.
set -euo pipefail
cd "$(dirname "$0")"
WIT=../../crates/wasmkeep-wit/wit
CC=${CC:-clang}
# Debian puts wasi-libc under /usr; Arch under /usr/share/wasi-sysroot.
if [ -z "${SYSROOT:-}" ]; then
  if [ -d /usr/share/wasi-sysroot ]; then SYSROOT=/usr/share/wasi-sysroot; else SYSROOT=/usr; fi
fi
mkdir -p gen
wit-bindgen c "$WIT" --world actor --out-dir gen >/dev/null
SDK=../../sdks/c
"$CC" --target=wasm32-wasi --sysroot="$SYSROOT" -O2 -mexec-model=reactor -Wall -Wextra -Igen -I"$SDK" \
  jobs.c "$SDK/wasmkeep.c" "$SDK/wasmkeep_kv.c" "$SDK/wasmkeep_wire.c" "$SDK/wasmkeep_workflow.c" gen/actor.c gen/actor_component_type.o -o gen/core.wasm
../../scripts/componentize-core.sh gen/core.wasm jobs.wasm
echo "wrote $(pwd)/jobs.wasm ($(wc -c < jobs.wasm) bytes)"
