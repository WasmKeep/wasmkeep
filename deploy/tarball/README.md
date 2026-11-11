# WasmKeep @VERSION@

The `wasmkeep` binary for Linux (glibc 2.28 or newer), from
<https://github.com/WasmKeep/wasmkeep>. WasmKeep is a self-hosted platform
for actors: WebAssembly components, each with its own SQLite database.
Pre-alpha: for trying it, side projects and staging, not production.

In this directory:

- `wasmkeep`: the server and its command line, one binary.
- `LICENSE`, `NOTICE`: Apache License 2.0.
- `THIRD_PARTY_NOTICES.txt`: the licenses of the libraries compiled into
  the binary.

## Install

```bash
sudo install -m 755 wasmkeep /usr/local/bin/wasmkeep
wasmkeep --version
```

Every file of a release is signed.
[Check the tarball](https://github.com/WasmKeep/wasmkeep/blob/@REF@/docs/deploy/single-node.md#1-install-the-binary)
you took this from before you trust it.

## First minute

A node serves components. The release this tarball came from
(<https://github.com/WasmKeep/wasmkeep/releases>) also carries
`counter.wasm`, the counter example already built, and
`THIRD_PARTY_NOTICES-components.txt`, the licenses of the libraries
compiled into it. Download it, then:

```bash
wasmkeep serve --actor counter=counter.wasm
```

In another terminal:

```bash
curl -X POST localhost:8080/actor/counter/alice   # 1
curl -X POST localhost:8080/actor/counter/alice   # 2
curl localhost:8080/actor/counter/bob             # 0, a separate actor
```

Each name is its own actor, with its own database under `.wasmkeep/data/`.

## Next

- [The README](https://github.com/WasmKeep/wasmkeep/blob/@REF@/README.md):
  what WasmKeep is, and what works today.
- [Your first actor in 10 minutes](https://github.com/WasmKeep/wasmkeep/blob/@REF@/docs/tutorials/first-actor.md)
- [Languages](https://github.com/WasmKeep/wasmkeep/blob/@REF@/docs/languages.md)
- [One server](https://github.com/WasmKeep/wasmkeep/blob/@REF@/docs/deploy/single-node.md),
  [a cluster](https://github.com/WasmKeep/wasmkeep/blob/@REF@/docs/cluster.md)
- [Report a security issue](https://github.com/WasmKeep/wasmkeep/blob/@REF@/SECURITY.md)

These links go to the files of `@REF@`, the commit this binary was built
from: they describe this version.
