# Counter, in standard Go

The [Rust counter](../counter) in standard Go (not TinyGo), built with
[componentize-go](https://github.com/bytecodealliance/componentize-go),
straight on the bindings it generates from the actor world: the
[Go SDK](../../sdks/go) is for TinyGo's bindings. It passes the same
black-box test (`crates/wasmkeep-server/tests/languages.rs`). The
component is about 2.7 MB, most of it Go's runtime.

```bash
(cd examples/go-std-counter && ./build.sh)   # writes examples/go-std-counter/counter.wasm (see build.sh for the tools)
cargo run -p wasmkeep-cli -- serve --actor counter=examples/go-std-counter/counter.wasm
```

The app is the two packages componentize-go asks for:
[`export_wit_world`](export_wit_world) (the five hooks) and
[`export_wasi_http_incoming_handler`](export_wasi_http_incoming_handler)
(requests); `build.sh` generates the rest. What works and what is
missing: [standard Go](../../docs/languages.md#standard-go-through-componentize-go).
