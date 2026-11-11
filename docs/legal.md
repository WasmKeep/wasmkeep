# Licensing and compliance

What WasmKeep's license obligations are, what it depends on, and what users
inherit when they build actors. The dependency list is kept current by
`scripts/licenses.sh`.

## WasmKeep itself

- **License**: Apache-2.0 for all code written for this repository
  ([LICENSE](../LICENSE)), with its explicit patent grant from every
  contributor. See [NOTICE](../NOTICE).
- **Name**: the WasmKeep name (and any logo, once there is one) is reserved
  ([TRADEMARK.md](../TRADEMARK.md)).
- **Contributions**: under the Contributor License Agreement
  ([CLA.md](../CLA.md)). The CLA also lets the maintainer offer commercial
  licenses ([ADR 0004](adr/0004-cla-for-dual-licensing.md)); contributions
  stay available under Apache-2.0.
- **Authorship**: written by Julien Turbide with AI models (mostly Claude in
  Claude Code, with Codex and Kimi for parts and reviews) under his
  direction and review. The implementation is original; what comes from
  other projects is used as published, under its own licence: the WASI
  interfaces vendored in `crates/wasmkeep-wit/wit/deps/` (and copied to
  `sdks/typescript/wit/deps/`, `crates/wasmkeep-sdk/wit/deps/`,
  `sdks/go/wit/deps/` and `sdks/python/wasmkeep/wit/deps/`), published by
  the WebAssembly Community
  Group under the [W3C Community Contributor License
  Agreement](https://www.w3.org/community/about/agreements/cla/), bindings
  generated from WIT files, and the dependencies listed by
  `scripts/licenses.sh` (see the
  README's [Authors](../README.md#authors) and
  [History and use of AI](../README.md#history-and-use-of-ai)). Where
  WasmKeep interoperates with another project's format or protocol, it is an
  independent implementation of the published format:
  - **LTX** (the replication file format) matches `github.com/superfly/ltx`
    (Apache-2.0). The reference library is used only by the conformance tool
    in `conformance/ltx` (a development tool, never shipped), whose output
    files are the test fixtures in `crates/wasmkeep-storage/tests/ltx`.
  - **WIT, WASI, the component model**: open standards of the W3C WebAssembly
    Community Group and the Bytecode Alliance.

## Dependencies

All Rust dependencies (`Cargo.lock`, about 400 crates) are under permissive
licenses: MIT, MIT-0, Apache-2.0 (some WITH LLVM-exception), BSD-2/3-Clause,
0BSD, ISC, Zlib, Unicode-3.0, Unlicense, BSL-1.0, CC0-1.0 and
CDLA-Permissive-2.0 (the Mozilla CA list in `webpki-roots`). None is
copyleft. SQLite (bundled by `rusqlite`) is in the public domain.

`scripts/licenses.sh` lists every crate with its license and fails on one
outside that set. `cargo deny check` ([deny.toml](../deny.toml)) checks the
same set, refuses crates from other registries or git, and fails on known
vulnerabilities (RustSec); its one accepted advisory is `bincode` being
unmaintained, reached only by the simulation tool. Run both before a
release. The list is an inventory: it does not contain the notices
themselves. A binary distribution must also carry each dependency's
copyright lines, NOTICE contents and license texts (Apache-2.0 and MIT
require the notices to travel with the binary; CDLA-Permissive-2.0 requires
its text). `scripts/notices.sh` gathers them with `cargo about`
([about.toml](../about.toml)): every crate in the binary for the release
targets, grouped by license with each license's full text, the
CDLA-Permissive-2.0 text included, and the NOTICE files dependencies ship.
The release workflow puts that file in every tarball, next to the binary.
`scripts/sbom.sh` writes a CycloneDX SBOM of the `wasmkeep` binary for one
target.

Development-only tools, never part of WasmKeep: workerd (Apache-2.0, in
`bench/workerd`), the Go LTX conformance tool, wasm-tools and wit-bindgen
(Apache-2.0 WITH LLVM-exception), Zig (MIT), TinyGo (BSD-3-Clause),
componentize-py (Apache-2.0), mdBook (MPL-2.0, builds the docs site only).

## What users inherit when they build actors

A component bundles its language's runtime. Distributing a component (not
running it on your own servers) carries that runtime's license:

| Language | Runtime inside the component | License |
| --- | --- | --- |
| Rust | the Rust standard library | MIT OR Apache-2.0 |
| TypeScript (default) | QuickJS-ng, via componentize-qjs | MIT (QuickJS-ng); Apache-2.0 (componentize-qjs) |
| TypeScript (`--engine starlingmonkey`) | SpiderMonkey, via StarlingMonkey and ComponentizeJS | **MPL-2.0** (SpiderMonkey): distributing the component requires making SpiderMonkey's source available; Apache-2.0 (StarlingMonkey, ComponentizeJS) |
| Python | CPython, via componentize-py | PSF License (keep its notice); Apache-2.0 |
| C, Zig | wasi-libc | MIT / Apache-2.0 / CC0 |
| Go | the TinyGo runtime | BSD-3-Clause |
| All (except Rust and Go) | the WASI 0.1 adapter (Wasmtime) | Apache-2.0 WITH LLVM-exception |

Running components on your own servers (the normal use of WasmKeep) is not
distribution.

## Patents

Apache-2.0 gives users a patent license from every contributor for their
contributions. It says nothing about third parties' patents. WasmKeep's
design draws on long-published, widely implemented ideas (the actor model,
1973; virtual actors as in Microsoft Orleans, 2014; single-writer leases on
conditional writes; WAL shipping as in Litestream and LiteFS).

## Trademarks of others

WasmKeep's docs compare it with, or name, other products (Cloudflare Workers
and Durable Objects, workerd, celld (Deno), wasmCloud, Spin, Rivet, Microsoft Orleans,
Litestream, LiteFS, Fly.io, AWS S3, Cloudflare R2, OVHcloud Object
Storage, MinIO, Ceph, Hetzner, DigitalOcean, Scaleway, Kubernetes,
Firecracker). The names are used only to identify those products; they belong to
their owners, and WasmKeep is not affiliated with or endorsed by any of them.

## Privacy

WasmKeep collects nothing by default. The usage ping is opt-in
(`--usage-ping`), sends the seven fields documented in
[telemetry.md](telemetry.md), and has no built-in destination. Actor data
stays on the operator's machines and bucket.

## Cryptography

TLS (rustls with ring) for actors' outbound HTTPS and for mutual TLS 1.3
between nodes (certificates made in memory with rcgen), HMAC-SHA256
signatures on node calls, and ChaCha20-Poly1305 with HKDF-SHA256 (ring)
for secrets sealed in the bucket. The bucket client (`object_store`) and
the usage ping use rustls with AWS-LC (`aws-lc-rs`) for their HTTPS, and
`object_store` also uses AWS-LC for S3 request signing. All are standard
algorithms from published open-source libraries. Check the export rules of
your jurisdiction before distributing binaries.
