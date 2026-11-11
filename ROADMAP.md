# Roadmap

What is being worked on, what comes after, and what is already built.
Dates are not promised; the order is, and features come before speed.
For what each release contained, see the [changelog](CHANGELOG.md); for
the numbers behind every claim here, [benchmarks](docs/benchmarks.md).

WasmKeep is **pre-alpha**. Its first release, `0.1.0`, is a pre-release for
trying it, side projects and staging.

## Now

The next release, `0.2.0`: **a team can install WasmKeep and run a real
app on it.** Three pieces of work, in this order.

1. **Installable.** Today the SDKs are used from a clone of this
   repository. For `0.2.0`: the Rust, TypeScript, Python and Go SDKs from
   their registries (crates.io, npm, PyPI, a Go module tag), the C and Zig
   SDKs as archives in the release, prebuilt example components, and a
   getting-started path that needs no clone. Releases publish the SDKs.
2. **Our own app on it.** Watch, the uptime monitor in
   [`examples/watch`](examples/watch), running on a three-node cluster on a
   released build, behind HTTPS, with dashboards, alerts and a nightly copy
   of the bucket. What that shows is missing goes on this page.
3. **Reliable enough to leave running.** A snapshot upload gets a timeout
   and stops failing silently; a restore gets one deadline for the whole of
   it (one took 56 s in the soak,
   [measured](docs/benchmarks.md#three-nodes-against-a-real-bucket)); a
   forwarded request is sent again when its connection had just closed;
   the tests that depend on timing wait on events instead; and the release
   candidate passes a twelve-hour soak. Done so far: a durable write on a
   cluster node no longer syncs to the local disk as well as the bucket.

## Next

`0.3.0`: **operable by someone who did not write it.**

1. **Mixed versions during an upgrade.** Today every node is upgraded
   together ([how](docs/cluster.md#upgrading-wasmkeep)).
2. **The remaining operator commands**: inspect or export one actor,
   verify a bucket.
3. **Traces.**
4. Whatever running our own app shows is missing.

## After that

Not before the two releases above.

- **Faster, and compared.** Performance targets on dedicated hardware
  with a stated scale envelope; a plain write that does not hold its
  actor during the upload; the comparison with celld on a cloud bucket
  with a node per machine
  ([one machine is measured](docs/compared-with-celld.md)); a run against
  AWS S3.
- **Languages**: an SDK for standard Go; outbound HTTP and the key-value,
  queue and workflow clients for D and MoonBit
  ([tiers](docs/languages.md)). Java, Kotlin and PHP cannot produce an
  actor yet, and languages that need Wasm garbage collection wait on a
  decision of their own.
- **Several tenants per process**, with limits per tenant, when a
  multi-team user asks.
- Ideas, each needing a design first: encrypting actor state in the
  bucket, exporting many actors' data for analytics, composing components
  at deploy time.

## Later, on demand

Each waits for a user who needs it.

- **Multi-region**: a cluster and bucket per region, each actor with a home
  region. Until then, keep a cluster in one region, close to its bucket
  ([why](docs/deploy/three-nodes.md#one-region)).
- **Native key-value and queue interfaces**, if the SDK patterns fall
  short.
- **Cron triggers** (alarms cover them today) and optional built-ins: TLS
  termination, a Helm chart, static assets uploaded with `wasmkeep deploy`
  and served by every node without waking an actor. Until then, serve
  them from the [front door](docs/deploy/front-door.md#static-files).
- **M5, an escape hatch**: a microVM tier (Firecracker) for tenants that
  need raw Linux, behind the same routing and storage API.

## Not planned

A hosted cloud, a global anycast network, Cloudflare Workers API
compatibility, or our own JavaScript engine. WasmKeep is meant to be run by
you, for standard Wasm components.

## Built

By milestone, the names the [design decisions](docs/adr) use. In `0.1.0`
unless marked "since".

### M0: Hello component

A WebAssembly component served over HTTP with microsecond instantiation
([measured](docs/benchmarks.md#wasmkeep-on-its-own)).

### M1: Actors on one node

One instance per name, events one at a time, SQLite per actor,
hibernation, alarms, calls between actors, WebSockets that survive
hibernation, `on-init` per version, hot reload with `wasmkeep dev`, and a
Rust SDK ([concepts](docs/concepts.md)).

### M2: Cluster and durability

Several nodes on one S3-compatible bucket
([running a cluster](docs/cluster.md)).

- Replication of each actor's changes to the bucket as LTX files
  ([ADR 0005](docs/adr/0005-replication-format-ltx-v3.md)).
- Ownership: one lease per node since
  [ADR 0020](docs/adr/0020-one-lease-per-node.md), which replaced a lease
  per actor. A dead node's actors are free 15 to about 21 s after its last
  write, then restored on another node.
- Forwarding between nodes: HTTP, actor calls and WebSockets.
- `wasmkeep deploy` and rolling upgrades of application code
  ([ADR 0008](docs/adr/0008-deploy-and-rolling-upgrades.md)).
- Every change to these protocols tested in deterministic simulation,
  under thousands of seeded failures
  ([ADR 0006](docs/adr/0006-simulation-tool.md)).
- Soaked on real buckets, R2 and OVHcloud Object Storage: 36 kills under
  load in four runs and twelve hours with 35 more, no acknowledged write
  lost or duplicated
  ([measured](docs/benchmarks.md#three-nodes-against-a-real-bucket)).

### M3: First public release

Languages, documentation, hardening, and `0.1.0`
([changelog](CHANGELOG.md)). What is left of it is under [Now](#now).

- Languages ([tiers and numbers](docs/languages.md)). Recommended: Rust.
  Supported: Zig, C/C++, TypeScript (QuickJS by default,
  [ADR 0018](docs/adr/0018-quickjs-the-default-typescript-engine.md)),
  Python, and since `0.1.0` Go on TinyGo with a full [SDK](sdks/go).
  Experimental, since `0.1.0`: D and MoonBit, each with an SDK, a counter
  and a chat room; a C# counter and a standard-Go counter.
- Mutual TLS between nodes by default
  ([ADR 0011](docs/adr/0011-node-to-node-mtls.md)); alarms that survive
  their node ([ADR 0009](docs/adr/0009-alarms-in-a-cluster.md)); a clean
  shutdown hands a node's actors over at once.
- Who may create an actor, per class
  ([ADR 0010](docs/adr/0010-who-may-create-an-actor.md)); access rules and
  configuration carried by `wasmkeep deploy`
  ([ADR 0013](docs/adr/0013-access-rules-and-config-in-deploys.md));
  outbound HTTP limited to public addresses.
- Garbage collection of old history in the bucket
  ([ADR 0012](docs/adr/0012-bucket-garbage-collection.md)).
- `wasmkeep cluster status`, `fence` and `forget`; Prometheus metrics,
  health and readiness endpoints, a
  [Grafana dashboard](deploy/grafana/wasmkeep.json), and what actors print
  in the node's log ([monitoring](docs/monitoring.md)).
- A distroless image, a three-node Compose example
  ([`deploy/compose/`](deploy/compose/README.md)), binaries and image
  built and signed by CI, an SBOM, and an opt-in anonymous usage ping, off
  by default ([what is sent](docs/telemetry.md)).
- A documentation site with a generated API reference and deployment
  guides; an internal security review in ten parts
  ([security](docs/security.md)); the [legal basics](docs/legal.md); CI on
  every pull request, with a benchmark regression check.
- Watch, an uptime monitor built on WasmKeep
  ([what it taught me](docs/dogfood.md)); and since `0.1.0` a comparison
  with celld on one machine
  ([WasmKeep and celld](docs/compared-with-celld.md)).

### M4: Operating a cluster

Started alongside M3; what is left of it is in [Next](#next).

- Draining a node and moving one actor: `wasmkeep cluster drain` and
  `wasmkeep actor move`
  ([ADR 0014](docs/adr/0014-draining-a-node-and-moving-an-actor.md),
  [ADR 0017](docs/adr/0017-moving-one-actor.md)).
- Secrets sealed in the bucket, changed without a deploy or a restart
  ([ADR 0016](docs/adr/0016-secrets-in-the-bucket.md)).
- Key-value and queues as SDK patterns on actors
  ([guide](docs/kv-and-queues.md),
  [ADR 0015](docs/adr/0015-kv-and-queues-as-sdk-patterns.md)), in the
  Rust, TypeScript, Python, Go, C and Zig SDKs.
- Durable workflows as an SDK pattern ([guide](docs/workflows.md),
  [ADR 0019](docs/adr/0019-workflows-as-an-sdk-pattern.md)): runners in
  Rust and TypeScript, clients in the Go, Python, C and Zig SDKs.

## Stretch goal: the benchmark showdown

I would love WasmKeep to be put head to head with the alternatives, measured
by someone other than me, in the style of independent performance
benchmarkers such as [Anton Putra](https://www.youtube.com/@AntonPutra): same
hardware, real load, latency percentiles, throughput, CPU and memory on a
dashboard.

Today `bench/compare` compares one workload, a counter, on WasmKeep
(Rust, Zig, C, Go on TinyGo, TypeScript and Python), `workerd` and celld,
as plain processes on one machine (`bench/run.sh`), and three nodes of
WasmKeep and celld with a node killed under writes (`bench/cluster.sh`);
the results are in [benchmarks](docs/benchmarks.md). The plan is a
comparison suite anyone can run unchanged:

- The same workloads implemented on each platform: a stateless "hello", a
  counter actor (write per request), a chat room (WebSocket fan-out), and
  wake-from-idle for many actors.
- Contenders where the comparison is fair: Cloudflare `workerd` (self-hosted),
  celld, Rivet, and Spin for the stateless case.
- Containers, a load generator, Prometheus and a Grafana dashboard; one command
  to run everything; raw results committed.
- My own numbers published only when anyone can reproduce them.

This is an aspiration and an invitation, not an endorsement by anyone named
here.

---

Want something on this list sooner, or something new on it? Open an issue. The
design decisions behind the roadmap are in [docs/adr](docs/adr).
