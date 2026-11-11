# WasmKeep C SDK @VERSION@

Write [WasmKeep](https://github.com/WasmKeep/wasmkeep) actors as a few
plain C functions; it works from C++ too. This archive is the SDK of
WasmKeep @VERSION@, for servers of the same version, and targets the
`wasmkeep:actor@@WIT_VERSION@` WIT world. It carries the bindings
`wit-bindgen c` generates for that world, so building an actor needs no
wit-bindgen.

In this directory:

- `wasmkeep.h` (includes `wasmkeep_kv.h` and `wasmkeep_workflow.h`) and
  `wasmkeep.c`, `wasmkeep_kv.c`, `wasmkeep_wire.c`, `wasmkeep_workflow.c`:
  the SDK. `WASMKEEP_SDK_VERSION` in `wasmkeep.h` is `"@VERSION@"`.
- `gen/`: the generated bindings (`actor.h`, `actor.c`,
  `actor_component_type.o`).
- `wit/`: the WIT they were generated from.
- `scripts/componentize-core.sh`: turns the core module clang writes into
  a component, with the WASI 0.1 adapter of the Wasmtime version WasmKeep
  uses. It downloads the adapter from crates.io on its first run and keeps
  it in `target/wasi-adapter/` in this directory.
- `LICENSE`, `NOTICE`: Apache License 2.0, and what else is in here.

## Build

You need clang with lld and wasi-libc (or wasi-sdk), wasm-tools, curl and
tar. Tried on Linux, with clang 18 and wasi-libc on Ubuntu 24.04 and
wasm-tools 1.263.0:

```text
Ubuntu/Debian: apt install clang lld wasi-libc libclang-rt-18-dev-wasm32
Arch:          pacman -S clang lld wasi-libc wasi-compiler-rt
wasm-tools:    cargo install wasm-tools, or a release binary
```

Put your actor in `app.c` (the
[counter](https://github.com/WasmKeep/wasmkeep/blob/@REF@/examples/c-counter/counter.c)
is a start) next to this extracted directory, and run from there:

```bash
SDK=wasmkeep-c-sdk-@VERSION@   # this directory
# wasi-libc: /usr on Debian, /usr/share/wasi-sysroot on Arch,
# $WASI_SDK/share/wasi-sysroot with wasi-sdk (also set CC=$WASI_SDK/bin/clang)
SYSROOT=${SYSROOT:-$([ -d /usr/share/wasi-sysroot ] && echo /usr/share/wasi-sysroot || echo /usr)}
${CC:-clang} --target=wasm32-wasi --sysroot="$SYSROOT" -O2 -mexec-model=reactor -I"$SDK" -I"$SDK/gen" \
  app.c "$SDK/wasmkeep.c" "$SDK/wasmkeep_kv.c" "$SDK/wasmkeep_wire.c" "$SDK/wasmkeep_workflow.c" \
  "$SDK/gen/actor.c" "$SDK/gen/actor_component_type.o" -o core.wasm
"$SDK/scripts/componentize-core.sh" core.wasm actor.wasm
```

Then serve it with a `wasmkeep` binary of the same version:
`wasmkeep serve --actor hits=actor.wasm`.

## Next

- [The C SDK's guide](https://github.com/WasmKeep/wasmkeep/blob/@REF@/sdks/c/README.md):
  the API, and the key-value, queue and workflow clients.
- Examples: [counter](https://github.com/WasmKeep/wasmkeep/tree/@REF@/examples/c-counter),
  [chat room](https://github.com/WasmKeep/wasmkeep/tree/@REF@/examples/c-chat-room),
  [jobs](https://github.com/WasmKeep/wasmkeep/tree/@REF@/examples/c-jobs).
- [Languages](https://github.com/WasmKeep/wasmkeep/blob/@REF@/docs/languages.md)

These links go to the files of `@REF@`, the commit this archive was built
from: they describe this version.
