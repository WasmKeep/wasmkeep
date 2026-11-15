# WasmKeep and Cloudflare Workers

WasmKeep's programming model resembles Cloudflare's Durable Objects: one
addressable instance per name, one event at a time, its own storage,
alarms and WebSockets. This page says where each is the better choice and
why. Numbers come from [Benchmarks](benchmarks.md); everything else
describes how the two are built. Two different things carry the Cloudflare
name here: **workerd**, the open-source runtime anyone can run, and
**Cloudflare Workers**, the hosted platform built on it. Much of what makes
Workers strong is the platform, not the runtime.

## Where WasmKeep is the better choice

- **Durable actors on your own servers, with failover.** Each actor's
  SQLite database is replicated to any S3-compatible bucket, and when a
  node dies another takes its actors over. With one lease per node
  ([ADR 0020](adr/0020-one-lease-per-node.md)), the 0.4.2 candidate
  (`563cb94`) ran for two hours on three VPS next to the bucket: writable
  again at 17.4 s median, 22.5 s p99 and 29.0 s maximum. Across 39 kills,
  no acknowledged durable writes were lost or duplicated; one plain write
  was lost ([result CSV](../bench/results/2026-11-14-563cb94-soak-ovh-bhs-3vps-2h-0.4.2-rc.csv)).
  The older twelve-hour run (18.2 s median, 24.4 s p99 and a 90 s
  recovery) predates the upload and
  restore-read bounds; it remains a historical measurement
  ([benchmarks](benchmarks.md#three-nodes-against-a-real-bucket)). workerd
  runs Durable Objects on one machine, without replication or failover
  between machines; that is the hosted platform's.
- **Languages with WASI 0.2 component toolchains.** Actors implement
  WasmKeep's component interfaces and run on Wasmtime: Rust, TypeScript,
  Python, C, Zig and Go (TinyGo). SDK capabilities and runtime tradeoffs
  vary ([languages](languages.md)). Workers are built around JavaScript;
  other languages run as WebAssembly inside a JavaScript isolate, with
  JavaScript glue.
- **Less CPU for compiled languages.** On the same counter, WasmKeep with
  Rust used 104 µs of server CPU per request against workerd's 128 µs on
  a 16-core workstation, and 39% of workerd's on a 4-vCPU VM; reads and
  writes were about 2.6 times faster at the median. Memory per resident
  actor is about the same (250 KB against 277 KB)
  ([measured](benchmarks.md#wasmkeep-against-workerd-the-same-counter)).
  Compiled WebAssembly runs close to native speed, with no JavaScript
  engine in between.
- **Your data, your bill.** The data is SQLite files and a documented
  replication format ([ADR 0007](adr/0007-replication-protocol.md)) in a
  bucket you own. There is no WasmKeep service fee; you pay your
  provider's compute, storage, request and network charges
  ([measured bucket requests](benchmarks.md#bucket-requests-at-rest)).
- **Durable workflows as a library**, on actors and alarms, in Rust and
  TypeScript with clients in the Go, Python, C and Zig SDKs ([workflows](workflows.md)).

## Where Workers is the better choice

- **JavaScript-heavy code.** V8 compiles JavaScript as it runs; WasmKeep's
  TypeScript runs on QuickJS, an interpreter inside each actor, and used
  about 1.8 times workerd's CPU per request on the same counter. Apps that
  do a lot of work in JavaScript belong on workerd.
- **Maturity.** Workers has years in production at Cloudflare's scale, a
  hardened V8 sandbox, web-standard APIs (`fetch`, streams, the cache
  API), npm compatibility and mature tooling. WasmKeep is pre-alpha.
- **Concurrency inside a request.** A Worker can await several things at
  once. A WasmKeep handler is synchronous: an outbound `http.fetch` blocks
  the actor until it answers, and the actor's other events wait. A batch
  of requests (`http.fetchAll`) or of actor calls (`actors.callMany`) is
  waited for together, but not alongside other work.
- **Hostile tenants side by side.** Cloudflare runs untrusted code from
  many customers on shared machines. WasmKeep's WebAssembly sandbox is
  strong, but a tier for tenants who do not trust each other (a process or
  microVM per tenant) is not built yet ([security model](security.md)).
- **The platform around it.** A global network that routes users to a
  nearby location, and managed key-value, queues, SQL and object storage
  next to the code. WasmKeep is self-hosted, in one region per cluster.

## In short

WasmKeep is the better fit for stateful, self-hosted apps, especially in
compiled languages, where the data should live in your own bucket and the
cost should be your infrastructure and provider charges. Workers is the
better fit for JavaScript-first apps that want a global edge and a mature ecosystem
today. The two programming models are close: an app designed around one
actor per room, user or document keeps its design on either, though its
code has to be ported to the other's APIs.

Cloudflare Workers, Durable Objects and workerd are Cloudflare's
products, named here only to identify them ([licensing](legal.md#trademarks-of-others)).
