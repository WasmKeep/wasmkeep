# Counter, in C

The [Rust counter](../counter) in C, with the [C SDK](../../sdks/c). It
passes the same black-box test (`crates/wasmkeep-server/tests/languages.rs`).
The component is about 170 KB
([measured](../../docs/languages.md#component-sizes)).

```bash
(cd examples/c-counter && ./build.sh)   # writes examples/c-counter/counter.wasm (see build.sh for the tools)
cargo run -p wasmkeep-cli -- serve --actor counter=examples/c-counter/counter.wasm
```
