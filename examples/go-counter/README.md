# Counter, in Go

The [Rust counter](../counter) in Go, built with TinyGo straight from the
bindings `wit-bindgen-go` generates (Go has no full SDK yet; the
[`sdks/go`](../../sdks/go) module has only the key-value, queue and workflow clients). It passes the
same black-box test (`crates/wasmkeep-server/tests/languages.rs`). The whole
component is about 151 KB
([measured](../../docs/languages.md#component-sizes)).

```bash
(cd examples/go-counter && ./build.sh)   # writes examples/go-counter/counter.wasm (see build.sh for the tools)
cargo run -p wasmkeep-cli -- serve --actor counter=examples/go-counter/counter.wasm
```

In another terminal:

```bash
curl -X POST localhost:8080/actor/counter/a    # 1
```

`build.sh` regenerates `gen/` (ignored by git) from
[`crates/wasmkeep-wit/wit`](../../crates/wasmkeep-wit/wit) on every build.
Most of `main.go` is `wasi:http` plumbing; storage is one call:
`storage.Query(sql, params)`.
