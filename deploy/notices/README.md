# Notices for the counters' runtimes

The release's `counter-<lang>.wasm` components embed their language's
runtime. `scripts/runtime-notices.sh` appends the license texts of what
each embeds to `THIRD_PARTY_NOTICES-components.txt`; this directory holds
the texts no installed tool ships. Each file's header names the tool
version it is for and where it was taken from, and the script fails when
the tool is at another version: move the pin and the text together.

| File | For | Taken from |
| --- | --- | --- |
| `quickjs-ng.txt` | componentize-qjs 0.5.0 (QuickJS-ng 0.16.2) | `quickjs/LICENSE` in the rquickjs-sys 0.14.0 crate |
| `componentize-qjs-crates.txt` | componentize-qjs 0.5.0 | generated, below |
| `cpython.txt` | componentize-py 0.25.1 (CPython 3.14.0) | the CPython tarball componentize-py's `build.rs` names: `LICENSE`, the incorporated-software part of `Doc/license.rst`, HACL*'s header |
| `componentize-py-crates.txt` | componentize-py 0.25.1 | generated, below |
| `wasm-tools.txt` | the two `wit-dylib-ffi` crates those runtimes link | `LICENSE-Apache-2.0_WITH_LLVM-exception` and `LICENSE-MIT` of wasm-tools at the commits named |
| `tinygo.txt` | TinyGo 0.39.0 | `LICENSE` at the v0.39.0 tag |
| `wasi-adapter.txt` | Wasmtime 50.0.1 (`scripts/componentize-core.sh`) | `LICENSE` at the v50.0.1 tag |

## The crates of the runtimes

Both lists come from `scripts/notices.sh --components` (cargo-about, with
`about.toml`: a crate outside its accepted licenses fails), run on the
runtime crate of the tool's source at the pinned tag, for wasm32-wasip2.
Build dependencies are included; a superset is the conservative side.

```bash
# componentize-qjs: crates/runtime at v0.5.0, plus the four crates the
# component's .dep-v0 record lists but that tag's Cargo.lock does not
# resolve for it.
git clone --depth 1 --branch v0.5.0 https://github.com/andreiltd/componentize-qjs qjs
git -C qjs rev-parse HEAD   # 66b3fafcfd7588e155edc0bec4839e5de7298072
cargo new --lib qjs-extra && cd qjs-extra
cargo add bumpalo@=3.20.3 unicode-width@=0.2.2 wast@=262.0.0 wat@=1.262.0
cargo update -p memchr --precise 2.8.2 && cd ..
scripts/notices.sh --components qjs.txt qjs/crates/runtime qjs-extra

# componentize-py: runtime/ at v0.25.1.
git clone --depth 1 --branch v0.25.1 https://github.com/bytecodealliance/componentize-py cpy
git -C cpy rev-parse HEAD   # 2a53e46a0083619f2c85ac5f20abe4132cda4b88
scripts/notices.sh --components cpy.txt cpy/runtime
```

Each output's text from `Licenses used:` on replaces the same part of the
committed file; the header above it says what it covers. For the
TypeScript counter, `scripts/runtime-notices.sh` reads the component's own
record of its crates (cargo auditable's `.dep-v0` section) and fails on a
crate the list leaves out: when a new componentize-qjs adds one, add it to
`qjs-extra`. The Python runtime carries no such record.
