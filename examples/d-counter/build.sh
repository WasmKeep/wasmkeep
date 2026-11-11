#!/usr/bin/env bash
# Builds counter.wasm: bindings with wit-bindgen d, the D SDK (sdks/d),
# LDC with -betterC for wasm32-wasi, linked by clang against wasi-libc (a
# reactor), then the WASI 0.2 adapter.
#   LDC 1.43: https://github.com/ldc-developers/ldc/releases (Arch: pacman -S ldc)
#   clang, lld, wasi-libc: as for examples/c-counter
#   cargo install wit-bindgen-cli --version 0.63.0 && cargo install wasm-tools
set -euo pipefail
cd "$(dirname "$0")"
WIT=../../crates/wasmkeep-wit/wit
LDC=${LDC:-ldc2}
CC=${CC:-clang}
# Debian puts wasi-libc under /usr; Arch under /usr/share/wasi-sysroot.
if [ -z "${SYSROOT:-}" ]; then
  if [ -d /usr/share/wasi-sysroot ]; then SYSROOT=/usr/share/wasi-sysroot; else SYSROOT=/usr; fi
fi
rm -rf gen
mkdir -p gen
wit-bindgen d "$WIT" --world actor --out-dir gen >/dev/null
SDK=../../sdks/d
"$LDC" -mtriple=wasm32-unknown-wasi -betterC -O2 -release -Igen -I"$SDK" -od=gen/obj -oq -c \
  counter.d "$SDK/wasmkeep.d" $(find gen/wit -name '*.d')
"$CC" --target=wasm32-wasi --sysroot="$SYSROOT" -mexec-model=reactor -nostartfiles \
  -Wl,--no-entry -Wl,--export-dynamic -Wl,--strip-debug gen/obj/*.o -o gen/core.wasm
../../scripts/componentize-core.sh gen/core.wasm counter.wasm
echo "wrote $(pwd)/counter.wasm ($(wc -c < counter.wasm) bytes)"
