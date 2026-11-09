# Counter, in TypeScript

The [Rust counter](../counter) in TypeScript, with [`@wasmkeep/sdk`](../../sdks/typescript).
It passes the same black-box test (`crates/wasmkeep-server/tests/languages.rs`).

From the repository root:

```bash
(cd sdks/typescript && npm ci)
(cd examples/ts-counter && npm ci && npm run build)  # writes examples/ts-counter/counter.wasm
(cd examples/ts-counter && npm run build:starlingmonkey)  # or on StarlingMonkey: writes counter-starlingmonkey.wasm
cargo run -p wasmkeep-cli -- serve --actor counter=examples/ts-counter/counter.wasm
```

In another terminal:

```bash
curl -X POST localhost:8080/actor/counter/alice   # 1
```
