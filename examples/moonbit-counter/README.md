# Counter, in MoonBit

The [Rust counter](../counter) in MoonBit, with the
[MoonBit SDK](../../sdks/moonbit). It passes the same black-box test
(`crates/wasmkeep-server/tests/languages.rs`). The whole component is about
49 KB ([measured](../../docs/languages.md#component-sizes)).

```bash
(cd examples/moonbit-counter && ./build.sh)   # writes examples/moonbit-counter/counter.wasm (see sdks/moonbit/build.sh for the tools)
cargo run -p wasmkeep-cli -- serve --actor counter=examples/moonbit-counter/counter.wasm
```

In another terminal:

```bash
curl -X POST localhost:8080/actor/counter/a    # 1
```
