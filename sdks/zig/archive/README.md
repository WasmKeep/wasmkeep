# WasmKeep Zig SDK @VERSION@

Write [WasmKeep](https://github.com/WasmKeep/wasmkeep) actors as a Zig file
with a few public functions. This archive is the SDK of WasmKeep
@VERSION@, for servers of the same version, and targets the
`wasmkeep:actor@@WIT_VERSION@` WIT world. The Zig SDK wraps the C SDK,
which is in here too with the bindings `wit-bindgen c` generates, so
building an actor needs no wit-bindgen. Needs Zig 0.17.

In this directory:

- `zig/`: the SDK (`wasmkeep.zig`, with `kv.zig`, `queue.zig` and
  `workflow.zig`); `version` in `wasmkeep.zig` is `"@VERSION@"`.
- `c/`: the C SDK it wraps, and `c/gen/`, the generated bindings.
- `wit/`: the WIT they were generated from.
- `scripts/componentize-core.sh`: turns the core module Zig writes into a
  component, with the WASI 0.1 adapter of the Wasmtime version WasmKeep
  uses. It downloads the adapter from crates.io on its first run and keeps
  it in `target/wasi-adapter/` in this directory.
- `LICENSE`, `NOTICE`: Apache License 2.0, and what else is in here.

## Build

You need Zig 0.17 (or `pip install ziglang==0.17.1` and
`ZIG="python -m ziglang"`), wasm-tools, curl and tar. Tried on Linux, with
Zig 0.17.1 and wasm-tools 1.263.0.

Put your actor in `app.zig` (the
[counter](https://github.com/WasmKeep/wasmkeep/blob/@REF@/examples/zig-counter/counter.zig)
is a start) next to this extracted directory, and run from there:

```bash
SDK=wasmkeep-zig-sdk-@VERSION@   # this directory
ZIG=${ZIG:-zig}
mkdir -p gen
$ZIG translate-c -target wasm32-wasi -lc -I "$SDK/c/gen" -I "$SDK/c" "$SDK/c/wasmkeep.h" > gen/wasmkeep_c.zig
# A 1 MiB stack: Zig's default (16 MiB) is reserved in every actor's memory.
$ZIG build-exe -target wasm32-wasi -fno-entry -mexec-model=reactor -rdynamic -O ReleaseSmall --stack 1048576 \
  -lc -I "$SDK/c/gen" -I "$SDK/c" \
  "$SDK/c/wasmkeep.c" "$SDK/c/wasmkeep_kv.c" "$SDK/c/wasmkeep_wire.c" "$SDK/c/wasmkeep_workflow.c" \
  "$SDK/c/gen/actor.c" "$SDK/c/gen/actor_component_type.o" \
  --dep wasmkeep -Mroot=app.zig \
  --dep wasmkeep_c -Mwasmkeep="$SDK/zig/wasmkeep.zig" \
  -Mwasmkeep_c=gen/wasmkeep_c.zig \
  -femit-bin=gen/core.wasm
"$SDK/scripts/componentize-core.sh" gen/core.wasm actor.wasm
```

Then serve it with a `wasmkeep` binary of the same version:
`wasmkeep serve --actor hits=actor.wasm`.

## Next

- [The Zig SDK's guide](https://github.com/WasmKeep/wasmkeep/blob/@REF@/sdks/zig/README.md):
  the API, and the key-value, queue and workflow clients.
- Examples: [counter](https://github.com/WasmKeep/wasmkeep/tree/@REF@/examples/zig-counter),
  [chat room](https://github.com/WasmKeep/wasmkeep/tree/@REF@/examples/zig-chat-room),
  [jobs](https://github.com/WasmKeep/wasmkeep/tree/@REF@/examples/zig-jobs).
- [Languages](https://github.com/WasmKeep/wasmkeep/blob/@REF@/docs/languages.md)

These links go to the files of `@REF@`, the commit this archive was built
from: they describe this version.
