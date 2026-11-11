# Counter, in C#

The [Rust counter](../counter) in C#, built with
[componentize-dotnet](https://github.com/bytecodealliance/componentize-dotnet)
(preview) and NativeAOT-LLVM, straight on the bindings it generates from
the actor world: there is no C# SDK yet. It passes the same black-box test
(`crates/wasmkeep-server/tests/languages.rs`). The component is about
929 KB once its debug sections are stripped. What works and what is
missing: [C#](../../docs/languages.md#c-spike).

```bash
(cd examples/csharp-counter && ./build.sh)   # writes examples/csharp-counter/counter.wasm (see build.sh for the tools)
cargo run -p wasmkeep-cli -- serve --actor counter=examples/csharp-counter/counter.wasm
```

The first build restores about 800 MB of preview packages from nuget.org
and the dotnet-experimental feed ([`nuget.config`](nuget.config)), so
`./scripts/check.sh --full` builds it only with `CHECK_CSHARP=1`; CI
always does.

In another terminal:

```bash
curl -X POST localhost:8080/actor/counter/a    # 1
```
