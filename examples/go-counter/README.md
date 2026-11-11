# Counter, in Go

The [Rust counter](../counter) in Go, with the [Go SDK](../../sdks/go),
built with TinyGo. It passes the same black-box test
(`crates/wasmkeep-server/tests/languages.rs`). The whole component is about
162 KB ([measured](../../docs/languages.md#component-sizes)).

```bash
(cd examples/go-counter && ./build.sh)   # writes examples/go-counter/counter.wasm (see build.sh for the tools)
cargo run -p wasmkeep-cli -- serve --actor counter=examples/go-counter/counter.wasm
```

In another terminal:

```bash
curl -X POST localhost:8080/actor/counter/a    # 1
```
