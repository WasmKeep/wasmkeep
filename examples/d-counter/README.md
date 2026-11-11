# Counter, in D

The [Rust counter](../counter) in D (LDC, `-betterC`), with the
[D SDK](../../sdks/d). It passes the same black-box test
(`crates/wasmkeep-server/tests/languages.rs`). The whole component is about
88 KB ([measured](../../docs/languages.md#component-sizes)).

```bash
(cd examples/d-counter && ./build.sh)   # writes examples/d-counter/counter.wasm (see build.sh for the tools)
cargo run -p wasmkeep-cli -- serve --actor counter=examples/d-counter/counter.wasm
```

In another terminal:

```bash
curl -X POST localhost:8080/actor/counter/a    # 1
```
