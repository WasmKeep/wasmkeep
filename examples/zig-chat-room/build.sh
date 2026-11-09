#!/usr/bin/env bash
# Builds chat_room.wasm: C bindings with wit-bindgen, the Zig SDK (sdks/zig,
# over the C SDK in sdks/c), a wasm32-wasi reactor, then the WASI 0.2 adapter.
#   Zig 0.17 (or `pip install ziglang==0.17.0` and ZIG="python -m ziglang")
#   cargo install wit-bindgen-cli --version 0.63.0 && cargo install wasm-tools
set -euo pipefail
cd "$(dirname "$0")"
WIT=../../crates/wasmkeep-wit/wit
SDK=../../sdks
ZIG=${ZIG:-zig}
APP=chat_room
mkdir -p gen
wit-bindgen c "$WIT" --world actor --out-dir gen >/dev/null
$ZIG translate-c -target wasm32-wasi -lc -I gen -I "$SDK/c" "$SDK/c/wasmkeep.h" > gen/wasmkeep_c.zig
# Modules: the app imports "wasmkeep" (sdks/zig), which imports "wasmkeep_c".
# A 1 MiB stack, as Rust's: Zig's default (16 MiB) is reserved in every
# actor's memory, about 2.1 MB more per resident actor than C or Rust.
$ZIG build-exe -target wasm32-wasi -fno-entry -mexec-model=reactor -rdynamic -O ReleaseSmall --stack 1048576 \
  -lc -I gen -I "$SDK/c" "$SDK/c/wasmkeep.c" "$SDK/c/wasmkeep_kv.c" "$SDK/c/wasmkeep_wire.c" "$SDK/c/wasmkeep_workflow.c" gen/actor.c gen/actor_component_type.o \
  --dep wasmkeep -Mroot=$APP.zig \
  --dep wasmkeep_c -Mwasmkeep="$SDK/zig/wasmkeep.zig" \
  -Mwasmkeep_c=gen/wasmkeep_c.zig \
  -femit-bin=gen/core.wasm
../../scripts/componentize-core.sh gen/core.wasm $APP.wasm
echo "wrote $(pwd)/$APP.wasm ($(wc -c < $APP.wasm) bytes)"
