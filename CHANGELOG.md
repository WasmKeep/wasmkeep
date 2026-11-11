# Changelog

Changes to WasmKeep, newest first. Before 1.0, any minor version (0.x) may
change the `wasmkeep:actor` WIT world, the formats in the bucket or the
command line; its notes say what changed and how to move.

The text under a version's heading is its GitHub release text
(`.github/workflows/release.yml` copies it).

## Unreleased

- **Languages**: Go through TinyGo is supported, with a full
  [Go SDK](sdks/go) and its counter, chat room and jobs app; D and
  MoonBit are experimental, each with an SDK, a counter and a chat room;
  a C# counter (componentize-dotnet) and a standard-Go counter
  (componentize-go) pass the shared tests.
  [Tiers and numbers](docs/languages.md).
- **Compared with celld**: the same counter on one node and on three, a
  node killed under writes, with a check for lost writes that gives every
  write a token of its own, repeats the takeovers with the killed node's
  disk removed, and includes a run that must lose writes
  ([WasmKeep and celld](docs/compared-with-celld.md),
  [benchmarks](docs/benchmarks.md#three-nodes-wasmkeep-against-celld)).
- **Durable writes on a cluster node** are no longer fsynced locally as
  well as uploaded: `exec-durable` answers once the bucket holds the
  write, as documented, and a node trusts a local file again only after
  checking it against the bucket. The bucket is the only durable copy of
  such a write ([running a cluster](docs/cluster.md#how-it-works)). A
  node without a bucket still fsyncs.
- **Installing**: the README starts from a release, not a clone: download
  and check a binary, or run the image. The Rust, TypeScript and Python
  SDKs now build as packages on their own (a crate, an npm tarball, a
  wheel), and the Go module is ready for its tag; none is on a registry
  yet. The server and every SDK carry one version, checked in CI
  ([languages](docs/languages.md#versions)).
- **Prebuilt components**: a release carries `counter.wasm`, `kv.wasm`,
  `queue.wasm`, `workflows-orders.wasm` and `workflows-ledger.wasm`,
  built from the examples and signed like its other files, so the
  [quickstart](README.md#quickstart) and the callers of the
  [key-value, queue](docs/kv-and-queues.md) and
  [workflow](docs/workflows.md#the-workflow-class) classes in other
  languages need no Rust toolchain. The notices of the libraries
  compiled into them come with them, in
  `THIRD_PARTY_NOTICES-components.txt`. The tarball has a short README of
  its own.
- **C and Zig SDK archives**: a release carries
  `wasmkeep-c-sdk-<version>.tar.gz` and `wasmkeep-zig-sdk-<version>.tar.gz`,
  signed like its other files: each SDK with the C bindings already
  generated, the WIT and the componentize script, and a README whose build
  command works in the extracted directory, without a clone or
  wit-bindgen ([C](sdks/c/README.md), [Zig](sdks/zig/README.md)).
  `WASMKEEP_SDK_VERSION` (C) and `wk.version` (Zig) carry the version.
- **Releases**: a tag's release waits for CI to pass on the tagged
  commit, and a version's image tag is written once. Publishing the
  release publishes the SDKs to crates.io, npm and PyPI from its tag,
  without stored tokens, each after a maintainer's approval
  ([releasing](docs/releasing.md)).

## 0.1.0

The first release, published as a **pre-alpha pre-release**: for trying
WasmKeep, side projects and staging, not production. See the
[roadmap](ROADMAP.md) for what comes next.

### What is in it

- **Actors**: WebAssembly components (`wasmkeep:actor@0.2.0` WIT world),
  one instance per name, events one at a time, a SQLite database each,
  hibernation, alarms that fire at least once, calls between actors,
  WebSockets that stay open while an actor sleeps, outbound HTTP limited to
  public addresses, and hot reload with `wasmkeep dev`.
- **Clusters**: several nodes on one S3-compatible bucket with conditional
  writes, each actor on one node at a time, its database replicated to the
  bucket and restored on another node when its node dies (free 15 to about
  21 s after the node's last write, then the restore). One lease per node,
  checked in deterministic simulation. Mutual TLS between nodes, required
  by default.
- **Operating a cluster**: `wasmkeep deploy` (rolling, with rollbacks,
  access rules and configuration), `wasmkeep cluster status`, `drain`,
  `fence` and `forget`, `wasmkeep actor move`, `wasmkeep secrets` (sealed
  in the bucket), garbage collection of old history in the bucket,
  Prometheus metrics, health and readiness endpoints, and a Grafana
  dashboard.
- **Languages**: Rust (recommended); Zig and C/C++; TypeScript (QuickJS
  by default, StarlingMonkey as an option) and Python; Go through TinyGo
  (experimental). [Tiers and numbers](docs/languages.md).
- **SDK patterns on actors**: sharded key-value store, queues and durable
  workflows ([KV and queues](docs/kv-and-queues.md),
  [workflows](docs/workflows.md)).
- **Packaging**: binaries for x86_64 and aarch64 Linux (glibc 2.28 or
  newer), each with a CycloneDX SBOM and third-party notices; a
  distroless image for linux/amd64 and linux/arm64 at
  `ghcr.io/wasmkeep/wasmkeep:0.1.0`; all signed with keyless cosign. A
  three-node Compose example against MinIO in
  [`deploy/compose/`](deploy/compose/README.md).

### Known limits

- Not production-ready: soaked against Cloudflare R2 and OVHcloud Object
  Storage with nodes killed under load, nothing lost, but not yet carrying
  real users' traffic, and not run against AWS S3
  ([measured](docs/benchmarks.md#three-nodes-against-a-real-bucket)).
- Upgrade every node of a cluster together: nodes of different versions
  are not supported in one cluster
  ([upgrading](docs/cluster.md#upgrading-wasmkeep)).
- One tenant per process ([security](docs/security.md#known-gaps)).
- No independent third-party security audit yet
  ([security](docs/security.md#audits)).
- The performance targets on dedicated hardware are not measured yet;
  millions of actors per cluster are a goal, not a measurement.

### Install and verify

[Single node](docs/deploy/single-node.md#1-install-the-binary) checks the
binaries with cosign; [Compose](deploy/compose/README.md) checks the image.
