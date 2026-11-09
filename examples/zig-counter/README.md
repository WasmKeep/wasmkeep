# Counter, in Zig

The [Rust counter](../counter) in Zig, with the [Zig SDK](../../sdks/zig).
It passes the same black-box test (`crates/wasmkeep-server/tests/languages.rs`).
The whole component is about 30 KB.

```bash
(cd examples/zig-counter && ./build.sh)   # writes examples/zig-counter/counter.wasm (see build.sh for the tools)
cargo run -p wasmkeep-cli -- serve --actor counter=examples/zig-counter/counter.wasm
```
