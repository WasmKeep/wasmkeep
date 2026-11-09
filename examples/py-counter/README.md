# Counter, in Python

The [Rust counter](../counter) in Python, with
[componentize-py](https://github.com/bytecodealliance/componentize-py) and
the bindings it generates from the `wasmkeep:actor` world. It passes the same
black-box test (`crates/wasmkeep-server/tests/languages.rs`). The component
embeds CPython (about 19 MB).

```bash
pip install componentize-py==0.25.1
(cd examples/py-counter && ./build.sh)   # writes examples/py-counter/counter.wasm
cargo run -p wasmkeep-cli -- serve --actor counter=examples/py-counter/counter.wasm
```
