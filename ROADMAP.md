# Roadmap

WasmKeep is built in milestones. Each one ends with something you can run, and
each starts once the previous one's acceptance tests pass, except runs that
need outside resources: M2's run against a real cloud bucket was done
during M3 (on R2 and OVHcloud Object Storage; AWS S3 not run).
Dates are not promised; the order is.

Status legend: ✅ done · 🚧 in progress · ⏳ planned · 💭 only if there is demand

## ✅ M0: Hello component

Serve a WebAssembly component over HTTP with microsecond instantiation.
[Benchmarks](docs/benchmarks.md#wasmkeep-on-its-own).

## ✅ M1: Actors on one node

The full actor model on a single server: one instance per name, events one at
a time, SQLite per actor, hibernation, alarms, calls between actors,
WebSockets that survive hibernation, `on-init` per version, hot reload with
`wasmkeep dev`, a Rust SDK and three examples.
[Concepts](docs/concepts.md) and [benchmarks](docs/benchmarks.md).

## ✅ M2: Cluster and durability

Several nodes, one S3-compatible bucket, and no committed write lost when a
node dies ([running a cluster](docs/cluster.md)). The real-bucket
run is done: R2 and OVHcloud Object Storage pass the suites
and a three-node soak
([measured](docs/benchmarks.md#three-nodes-against-a-real-bucket)); AWS S3
is not run.

- ✅ Deterministic simulation harness (madsim): every change to the cluster
  protocols is tested under thousands of seeded failure scenarios
  ([ADR 0006](docs/adr/0006-simulation-tool.md))
- ✅ Actor leases on the bucket: exactly one owner per actor, safe under clock
  skew, checked in simulation (since replaced by one lease per node,
  [ADR 0020](docs/adr/0020-one-lease-per-node.md), in M3)
- ✅ Replication: each actor's changes streamed to the bucket as LTX files,
  compatible with Litestream's tooling
  ([ADR 0005](docs/adr/0005-replication-format-ltx-v3.md))
- ✅ Node heartbeats, request forwarding between nodes: HTTP, actor calls and
  WebSockets
- ✅ Failover: a dead node's actors restored elsewhere within 30 s (stretch: 10 s);
  since ADR 0020 they are free 15 to about 21 s after the node's last write,
  then restored ([how it works](docs/cluster.md#how-it-works))
- ✅ `wasmkeep deploy` and rolling upgrades of application code across a
  cluster

## 🚧 M3: First public release

More languages, documentation, hardening, and a first release that a
team could run: `0.1.0`, a pre-alpha pre-release
([changelog](CHANGELOG.md)). Measured numbers are in
[benchmarks](docs/benchmarks.md).

Done:

- ✅ Languages beyond Rust, each measured with the same counter
  ([tiers and numbers](docs/languages.md)): Rust (recommended); Zig and
  C/C++ (supported, perform like Rust); TypeScript and Python (supported,
  they embed a runtime: 2.2× (TypeScript on QuickJS) to 2.5× (Python) the
  CPU per request on a workstation, about 4× on a small VM (TypeScript on
  StarlingMonkey and Python; that run had no QuickJS build), and more
  memory per actor); Go through TinyGo (experimental at the release; since
  supported, with an SDK)
- ✅ A documentation site: concepts, generated API reference, and
  deployment guides (single VPS, three nodes, where to run it, what to put
  in front)
- ✅ Alarms in a cluster ([ADR 0009](docs/adr/0009-alarms-in-a-cluster.md)),
  including a race where a late delete could remove a newer alarm, and an alarm a node brings from
  single-node use, both checked under simulation
- ✅ A clean shutdown (SIGTERM) hands its actors to another node at once
- ✅ Who may create an actor: per-class access modes
  ([ADR 0010](docs/adr/0010-who-may-create-an-actor.md))
- ✅ Actors know their own address; configuration and secrets from files
  on each node; outbound HTTP limited to public addresses and to the
  ports browsers allow, with private CAs for HTTPS
- ✅ An internal security review in ten parts ([security](docs/security.md)), and
  dependency checks (advisories, licenses, sources)
- ✅ Mutual TLS between nodes ([ADR 0011](docs/adr/0011-node-to-node-mtls.md)),
  each node's certificate pinned in its heartbeat, plain HTTP between
  nodes refused by default (`--cluster-plaintext accept`
  allows it), revocation tested end to end, about
  10 µs per forwarded request
  ([measured](docs/benchmarks.md#between-nodes-plain-http-against-mutual-tls))
- ✅ What actors print (stdout, stderr, `console`) in the node's log,
  tagged with the actor and rate-limited
  ([monitoring](docs/monitoring.md#what-actors-print))
- ✅ Monitoring: Prometheus metrics (bucket requests, failovers and more),
  health and readiness endpoints, a [Grafana dashboard](deploy/grafana/wasmkeep.json)
  ([monitoring](docs/monitoring.md))
- ✅ Packaging: a distroless image, a three-node Compose
  example against MinIO with mutual TLS and an optional Prometheus and
  Grafana overlay ([`deploy/compose/`](deploy/compose/README.md)), a
  CycloneDX SBOM
- ✅ Watch, an uptime monitor built on WasmKeep as my dogfood app
  ([examples/watch](examples/watch), [what it taught me](docs/dogfood.md))
- ✅ Opt-in anonymous usage ping, off by default ([what is sent](docs/telemetry.md))
- ✅ One lease per node instead of one per actor
  ([ADR 0020](docs/adr/0020-one-lease-per-node.md)): ownership records,
  the upgrade path, `wasmkeep cluster fence` and `cluster forget`, then a
  twelve-hour soak against a real bucket: nothing lost or duplicated, but
  one actor was writable again only 90 s after the kill (most of it a 56 s
  restore) and durable writes are slower in the tail than
  with per-actor leases
  ([measured](docs/benchmarks.md#three-nodes-against-a-real-bucket)).
  The fixes since (bounded segment uploads and restore reads) need a soak
  of their own, and the durable write's stages are not yet timed

Next, in this order:

1. ✅ Garbage collection in the bucket
   ([ADR 0012](docs/adr/0012-bucket-garbage-collection.md)): old history,
   snapshots, deploy artifacts and deleted actors' data. Actor history is
   collected after each periodic snapshot (kept an hour by default,
   `--history-retention-secs`), deleting an actor erases its history, and
   `wasmkeep deploy` removes old versions beyond what rollbacks need. At
   10,000 writes a restore takes 1.2 ms after collection instead of 6.1 ms
   ([measured](docs/benchmarks.md#restore-against-history-length))
2. ✅ Configuration and access rules carried by `wasmkeep deploy`
   ([ADR 0013](docs/adr/0013-access-rules-and-config-in-deploys.md)): a
   node started without `--access` no longer reopens what ADR 0010 closed.
   Secrets went into the bucket in M4 (below)
3. ✅ CI with its full matrix (other platforms, the S3 and
   failover job, every language build), on every pull request
4. ✅ A three-node soak against a real S3 or R2 bucket (M2's open item):
   on R2 and OVHcloud Object Storage, from a desktop and from three VPS
   next to the bucket, 27 kills under load (nine in each run), nothing lost
   ([measured](docs/benchmarks.md#three-nodes-against-a-real-bucket)).
   One failover in 267 took 32 s, a restore stalled by one slow GET;
   fixed, and the VPS soak with the fix passed: writable again
   in 15.5 s at the median, 24.6 s at worst (measured with per-actor leases,
   before ADR 0020; the node-lease runs are in the same section). AWS S3
   is not run
5. ✅ Scale evidence:
   restart and alarm recovery as the number of actors grows
   ([measured](docs/benchmarks.md#restart-and-alarm-recovery-against-actor-count)
   up to 100,000 actors on a node and 10,000 alarms on a dead one). Found and fixed: the surviving
   nodes swept the same alarms, so recovery ran at one node's rate; they
   now recover a dead node's alarms at their combined rate, and the sweep
   leaves out draining nodes and other tenants' nodes
6. ✅ `wasmkeep cluster status` (nodes, TLS and deploys read from the bucket
   alone), and upgrades of WasmKeep itself documented as "upgrade every node
   together" (mixed versions come after the release)
7. ⏳ The performance targets on dedicated hardware

The first release: ✅ the [legal basics](docs/legal.md)
(licensing, name, export control), ✅ binaries and the image built by CI
and signed, with a benchmark regression check on pull requests, ✅ the
first public release, `0.1.0`, ✅ a first comparison with celld on one
machine ([WasmKeep and celld](docs/compared-with-celld.md)), then ⏳ the
same on a cloud bucket with a node per machine.

Continuing alongside, without holding the release:

- 🚧 More languages, in order: ✅ TypeScript on QuickJS, now the default
  engine (4.6× less memory per actor than StarlingMonkey, which stays an
  option for Web APIs; [ADR 0018](docs/adr/0018-quickjs-the-default-typescript-engine.md)),
  ✅ Go on TinyGo, now supported (the [Go SDK](sdks/go): its counter,
  chat room and jobs app pass the shared tests), then standard Go through
  componentize-go, ✅ D (experimental: the [D SDK](sdks/d), its counter and
  chat room pass), MoonBit, then a C# spike. Java, Kotlin and PHP
  cannot produce an actor yet; languages that need Wasm garbage collection wait on a decision of
  their own
- 🚧 The benchmark showdown suite (below): the counter comparison runs today

After the release: mixed versions during an upgrade, the other operator
commands (inspect or export an actor, verify the bucket), and traces.

## 🚧 M4: Operating a cluster

What teams running WasmKeep need next.

- ✅ Draining a node ([ADR 0014](docs/adr/0014-draining-a-node-and-moving-an-actor.md),
  [how](docs/cluster.md#draining-a-node)): `wasmkeep cluster drain` hands a
  node's actors and their alarms to the others a few at a time, and
  `cluster status` shows when it is drained; and moving one actor to a
  named node (`wasmkeep actor move`, [ADR 0017](docs/adr/0017-moving-one-actor.md))
- ✅ Secrets in the bucket ([ADR 0016](docs/adr/0016-secrets-in-the-bucket.md),
  [how](docs/cluster.md#secrets-in-the-bucket)): sealed with a key the nodes
  hold, changed with `wasmkeep secrets` without a deploy or a restart; a
  node that cannot open them hands its actors over rather than run them,
  and a class can require values (`wasmkeep deploy --requires`) so it never
  runs without them
- ✅ Workflows as an SDK pattern, like key-value and queues: durable
  steps, sleeps and waits for events on actors and alarms
  ([ADR 0019](docs/adr/0019-workflows-as-an-sdk-pattern.md),
  [guide](docs/workflows.md)): runners in Rust and TypeScript
  (TypeScript with synchronous outbound HTTP, `http.fetch`), clients in
  every other SDK
- ⏳ Several tenants per process, with limits per tenant, when a multi-team
  user asks
- ✅ Key-value and queues in the SDKs first: one API (`kv`, `queue`, a read
  cache) backed by actors (sharded key-value actors, batching queue
  actors), so a platform primitive can replace the backend later without
  changing application code. Done in every SDK: Rust, TypeScript, Python,
  Go, C and Zig ([guide](docs/kv-and-queues.md),
  [ADR 0015](docs/adr/0015-kv-and-queues-as-sdk-patterns.md)); the shard and
  queue actors stay Rust, and the other SDKs have the client side, each
  tested against them with its own jobs app

Ideas, not yet scheduled, each needing a design first: encrypting actor
state in the bucket, exporting many actors' data for analytics, composing
components at deploy time.

## 💭 Later, on demand

Each waits for a user who needs it:

- Multi-region: a cluster and bucket per region, each actor with a home
  region. Until then, keep a cluster in one region, close to its bucket
  ([why](docs/deploy/three-nodes.md#one-region))
- Native key-value and queue interfaces, if the SDK patterns fall short
- Cron triggers (alarms cover them today), and optional built-ins (TLS
  termination, Helm chart, static assets: files uploaded with `wasmkeep
  deploy` and served by every node from local disk, with caching headers
  and range requests, without waking an actor; until then, serve them
  from the [front door](docs/deploy/front-door.md#static-files))

## 💭 M5: An escape hatch

A microVM tier (Firecracker) for tenants that need raw Linux, behind the same
routing and storage API.

## Stretch goal: the benchmark showdown

I would love WasmKeep to be put head to head with the alternatives, measured
by someone other than me, in the style of independent performance
benchmarkers such as [Anton Putra](https://www.youtube.com/@AntonPutra): same
hardware, real load, latency percentiles, throughput, CPU and memory on a
dashboard.

Today `bench/compare`, run by `bench/run.sh`, compares one workload, a
counter, on WasmKeep (in every guest language) and `workerd`, as plain
processes on one machine; the results are in [benchmarks](docs/benchmarks.md). The plan is a
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

## Not planned

A hosted cloud, a global anycast network, Cloudflare Workers API
compatibility, or our own JavaScript engine. WasmKeep is meant to be run by you,
for standard Wasm components.

---

Want something on this list sooner, or something new on it? Open an issue. The
design decisions behind the roadmap are in [docs/adr](docs/adr).
