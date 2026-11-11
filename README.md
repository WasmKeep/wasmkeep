# WasmKeep

**Run many tiny stateful actors, written in any language that compiles to
WebAssembly, on servers you own.**

WasmKeep is a self-hosted platform for *actors*: small programs that each own a
piece of state (a chat room, a user's session, a game lobby, a rate limit, a
device) and handle one event at a time. Each actor is a WebAssembly component
with its own SQLite database. Actors that are idle leave memory and come back in
about a millisecond when the next request arrives, so a single server can keep
far more of them than fit in memory.

Think **Cloudflare Durable Objects, but for standard Wasm components, on your
own machines.**

> **Status: pre-alpha.** What works today: actors with their own SQLite
> database, one-event-at-a-time execution, hibernation, alarms that fire at
> least once, calls between actors, WebSockets that stay open while an actor
> sleeps, hot reload, and **clusters**: several nodes on one S3-compatible
> bucket, each actor on one node at a time, its database replicated to the
> bucket and restored elsewhere when a node dies (free on another node 15
> to about 21 s after its last write, then the restore;
> [how it works](docs/cluster.md#how-it-works)), and `wasmkeep deploy` for new versions of
> an app. Soaked against Cloudflare R2 and OVHcloud Object Storage with
> nodes killed under load, nothing lost
> ([measured](docs/benchmarks.md#three-nodes-against-a-real-bucket)), but
> not yet carrying real users' traffic: not production-ready. See
> [Status](#status) and the [roadmap](ROADMAP.md).
>
> **Scale, as measured so far** (one workstation, data in RAM, in-memory
> bucket; [benchmarks](docs/benchmarks.md#restart-and-alarm-recovery-against-actor-count)):
> a node with 100,000 actors on its disk serves 4 ms after a restart and
> re-arms their alarms within 12 s; the 10,000 pending alarms of a node
> that dies fire on the others within about 4 minutes at default timings
> (23.2 s measured with timings at a tenth of the defaults; the 4 minutes
> is a ×10 estimate). Millions of actors are a goal, not
> yet measured.

## Why WasmKeep

- **No locks, no data races on an actor's state.** Each actor handles one
  event at a time, in arrival order. Two requests to the same chat room can
  never interleave. Different actors run in parallel, so what an actor
  learns from another actor or an outside system can still change between
  two of its events.
- **State next to the code.** Every actor gets a real SQLite database, private
  to it. No separate database to run, size or connect to.
- **Cheap when idle.** An idle actor's Wasm instance is released: it costs
  disk, plus a little host state for any open WebSocket. It wakes in about
  a millisecond, with its data, its pending alarm and even its open
  WebSockets.
- **Any language.** Actors are standard WASI 0.2 components. Rust,
  TypeScript, Python, C, Zig and Go (TinyGo) have SDKs and pass the same
  tests; standard Go, D and MoonBit are experimental, and a C# counter
  builds through componentize-dotnet; more are next ([languages](docs/languages.md)). An
  actor imports WasmKeep's own interfaces (`wasmkeep:actor`, for storage,
  alarms, calls and sockets) besides WASI, so it runs on a host that
  implements them: WasmKeep today.
- **Sandboxed.** Each actor runs in its own Wasm instance with no
  filesystem, raw sockets or environment access, outbound HTTP to public
  addresses only by default, and no data but its own database. A CPU
  budget per event, memory limits and SQL limits per actor: a guest that
  loops forever is stopped and its caller gets a 500, while other actors
  keep running. Every actor
  of a node shares one process, so this is not a boundary for hostile
  tenants yet
  ([security model](docs/security.md)).
- **Survives a dead server.** Run several nodes on one S3-compatible bucket
  and each actor's database is replicated to it; when a node dies, another
  takes its actors over (15 to about 21 s after the node's last write,
  then the restore). Writes made with `exec-durable` are
  acknowledged only once they are in the bucket, so they survive the node;
  ordinary writes are replicated right after the event and can be lost with
  the node in that moment ([the replication contract](docs/cluster.md)). No
  consensus cluster to operate: the bucket holds the leases, the ownership
  records and each actor's replicated database. A node cut off from it for
  8 s stops serving its actors until it reaches the bucket again.
- **Built to operate.** `wasmkeep cluster status` reads a cluster's state
  from the bucket alone; `wasmkeep cluster drain` empties a node before you
  replace it, moving its actors and their alarms, and `wasmkeep actor move`
  moves one; `wasmkeep secrets` keeps
  an app's secrets sealed in the bucket and changes them without a deploy;
  old history and deploys are garbage-collected
  ([running a cluster](docs/cluster.md)).
- **Yours to run.** One binary on a VPS is a complete deployment. Apache-2.0.
  No account, no vendor, no egress fees to someone else's network.

## Who it is for

- **Teams that like the Durable Objects programming model** but want to run
  it themselves, in Rust, TypeScript, Python, C, Zig or Go.
- **Companies that need on-premises or sovereign deployments**: everything
  runs on your hardware, and replication goes to any S3-compatible bucket
  (AWS S3, Cloudflare R2, MinIO, Ceph...).
- **SaaS products that run customer code**: plugins and automations, each
  with its own state and resource limits. Pre-alpha: run code you trust; the
  sandbox protects the host from buggy actors but is not yet a promise
  against a determined, hostile tenant ([security model](docs/security.md)).
- **Edge and IoT deployments**: a single small binary per site.

It is not a replacement for Postgres and stateless servers: it is for the
parts of an application where that stack gets awkward (state many requests
fight over, WebSockets that must meet, timers, per-customer isolation).
[Who it is for, and real-world use cases](docs/use-cases.md) compares the
two and walks through each pattern.

## How it compares

| | Language of your code | Stateful actors | Self-hosted, with failover | Wake / cold start |
| --- | --- | --- | --- | --- |
| **WasmKeep** | Any WASI 0.2 component (Rust, TypeScript, Python, C, Zig, Go today) | Yes, SQLite per actor | Yes: nodes on one S3 bucket, actors free on another node 15 to about 21 s after a node dies, then the restore | ~1 ms wake, ~20 µs instantiate |
| Cloudflare Workers + Durable Objects | JS/TS; Rust, Python and others via Wasm inside a JavaScript isolate | Yes | No (the managed platform); `workerd` runs locally, distributed DOs in progress | ms |
| [celld](https://daily.dev/posts/celld-self-hosted-distributed-durable-objects-sxggesxkd.md) | JavaScript | Yes, SQLite per cell | Yes (alpha), S3 as coordinator | ~4 ms wake |
| [Rivet](https://rivet.dev) | Mostly JS/TS | Yes | Yes | ms |
| [Spin](https://spinframework.dev) / wasmCloud | Wasm components | No (stateless functions) | Yes | µs |

Cloudflare can run Rust and other languages, compiled to Wasm and hosted inside
a V8 JavaScript isolate with JavaScript glue, against Cloudflare's own APIs.
WasmKeep runs standard components directly on Wasmtime, with no JavaScript in
between, and you run it. [WasmKeep and Cloudflare Workers](docs/compared-with-workers.md)
says where each is the better choice, and why;
[WasmKeep and celld](docs/compared-with-celld.md) does the same for
celld, measured on one node and on three.

Our numbers come from [`bench/`](bench/) and are reproducible with
`./bench/run.sh`: see [Benchmarks](docs/benchmarks.md), including a first
comparison with Cloudflare's workerd on the same counter. WasmKeep with Rust
used 81% of workerd's server CPU per request on a 16-core workstation and
39% on a 4-vCPU VM; its memory per resident actor was about workerd's on
both (250 KB against 277 KB on the workstation). WasmKeep with TypeScript (on
QuickJS, the default engine) used about twice Rust's CPU per request.
We want this comparison measured independently too: see the
[benchmark showdown goal](ROADMAP.md#stretch-goal-the-benchmark-showdown).

## Install

On Linux, x86_64 or aarch64 (glibc 2.28 or newer), download a release. Name
the version: releases are pre-releases for now, so there is no `latest`.

```bash
v=0.1.0
f=wasmkeep-$v-$(uname -m)-unknown-linux-gnu.tar.gz
curl -fsSLO "https://github.com/WasmKeep/wasmkeep/releases/download/v$v/$f"
tar xzf "$f" && sudo install -m 755 "${f%.tar.gz}/wasmkeep" /usr/local/bin/wasmkeep
wasmkeep --version
```

Every file of a release is signed:
[check a download](docs/deploy/single-node.md#1-install-the-binary) before
you trust it. Or use the container image, `ghcr.io/wasmkeep/wasmkeep:0.1.0`,
for linux/amd64 and linux/arm64
([running it](deploy/compose/README.md#the-image-on-its-own)). On macOS and Windows,
use the image or build from source (below).

## Quickstart

A node serves components: build the counter example, then serve it. You
need [rustup](https://rustup.rs); the pinned toolchain and the
`wasm32-wasip2` target install automatically.

```bash
git clone https://github.com/wasmkeep/wasmkeep && cd wasmkeep
(cd examples/counter && cargo build --release --target wasm32-wasip2)
wasmkeep serve --actor counter=examples/counter/target/wasm32-wasip2/release/counter.wasm
```

Starting with the release after 0.1.0, a release also carries that
counter already built, `counter.wasm`, signed like its other files, so the
first minute needs no toolchain (0.1.0 has no `counter.wasm`). The
release's `THIRD_PARTY_NOTICES-components.txt` holds the notices of the
libraries compiled into it and the other example components:

```bash
# $v as in Install, and later than 0.1.0
curl -fsSLO "https://github.com/WasmKeep/wasmkeep/releases/download/v$v/counter.wasm"
wasmkeep serve --actor counter=counter.wasm
```

Without the release binary, build the server from the same clone:
`cargo run -p wasmkeep-cli -- serve --actor counter=...`.

In another terminal:

```bash
curl -X POST localhost:8080/actor/counter/alice   # 1
curl -X POST localhost:8080/actor/counter/alice   # 2
curl localhost:8080/actor/counter/bob             # 0, a separate actor
```

Each name is its own actor with its own SQLite database under `.wasmkeep/data/`.
Restart the server and the counts are still there.

To develop with hot reload (rebuilds and swaps on every save, keeping data;
`dev` keeps it under `.wasmkeep/dev`, not the `.wasmkeep/data` of `serve`):

```bash
wasmkeep dev examples/counter   # from a clone
```

To run several nodes on one bucket, with replication, failover and
`wasmkeep deploy`, see [Running a cluster](docs/cluster.md).

## Learn more

- [What is WasmKeep?](docs/overview.md): the model, use cases, and when *not* to use it
- [Your first actor in 10 minutes](docs/tutorials/first-actor.md) (Rust)
- [Languages](docs/languages.md): which languages work today and how
- [Concepts](docs/concepts.md): addresses, events, life cycle, storage, alarms, calls, WebSockets
- [API reference](docs/reference/api.md): every host function and hook, with its name in the Rust, TypeScript, Python, C and Zig SDKs
- Deploying: [one server (VPS)](docs/deploy/single-node.md), [three nodes](docs/deploy/three-nodes.md)
- Examples: [counter](examples/counter), [chat room](examples/chat-room) (WebSockets),
  [rate limiter](examples/rate-limiter) (alarms), [AI agent](examples/agent) (a model over
  HTTP, a reminder tool on an alarm), [jobs](examples/jobs) (the SDK's
  [key-value and queue](docs/kv-and-queues.md) patterns, with its clients in every SDK), and
  the counter, chat room and jobs app in TypeScript, Python, C, Zig and Go, the counter
  and chat room in D and MoonBit, a counter in C# and in standard Go,
  [workflows](examples/workflows) and [Watch](examples/watch), an uptime monitor (`examples/`)
- [Running a cluster](docs/cluster.md): replication, failover, forwarding, `wasmkeep deploy`
- [Roadmap](ROADMAP.md): what is built, what comes next, and what is not planned
- Design decisions: [docs/adr](docs/adr); how the pieces fit: [Concepts](docs/concepts.md) and [Running a cluster](docs/cluster.md#how-it-works)
- [Security model](docs/security.md): what is protected, the limits, the known gaps; report vulnerabilities as in [SECURITY.md](SECURITY.md)

All of it also builds as a site: `./scripts/docs-site.sh` (needs
`cargo install mdbook --version 0.5.4`), then open `target/book/index.html`.

## Status

WasmKeep is young and **pre-alpha**: its first release, `0.1.0`, is a
pre-release ([changelog](CHANGELOG.md)).

- **Works, and is tested**: everything in the status note at the top,
  each feature with tests that run before every merge, the node lease,
  replication, the alarm index, draining and moving an actor checked in
  deterministic simulation on node leases, and an internal
  security review in ten parts, done during development with AI-assisted
  adversarial review; no independent third-party audit yet
  ([security model](docs/security.md#audits)).
- **Measured on real infrastructure**: three-node soaks against Cloudflare
  R2 and OVHcloud Object Storage, from a desktop and from three VPS next
  to the bucket, 36 nodes killed under load without losing or duplicating
  a write ([benchmarks](docs/benchmarks.md#three-nodes-against-a-real-bucket)).
- **On node leases** (one lease per node, which replaced one per actor:
  [ADR 0020](docs/adr/0020-one-lease-per-node.md)): a twelve-hour soak
  with nothing lost or duplicated over 35 kills, writable again in 18.2 s
  at the median, 24.4 s at p99. Not passed yet: one actor was writable
  again only 90 s after the kill (most of it a 56 s restore), and
  durable writes are slower in the tail than with per-actor leases (p99
  about 1.7 times, measured side by side). Those runs predate
  the fixes since (bounded segment uploads and restore reads), which
  still need a soak of their own
  ([benchmarks](docs/benchmarks.md#three-nodes-against-a-real-bucket)).
- **Not yet**: weeks of real traffic, a run against AWS S3. What is left is on the [roadmap](ROADMAP.md).

Do not trust it with data you care about yet.

## Contributing

Contributions are welcome: see [CONTRIBUTING.md](CONTRIBUTING.md). Your first
pull request asks you to sign the [CLA](CLA.md), with one comment.

## License

Copyright 2026 Julien Turbide. Licensed under the Apache License 2.0
([LICENSE](LICENSE), [NOTICE](NOTICE)). The name is covered by
[TRADEMARK.md](TRADEMARK.md).
Dependencies and what you inherit when you ship an actor:
[Licensing and compliance](docs/legal.md).

Cloudflare, Durable Objects, AWS, S3 and the other product names in these
docs belong to their owners. They are named only to describe and compare;
WasmKeep is not affiliated with or endorsed by any of them.

## Authors

- **Julien Turbide** (Nuagerie): the idea, the
  design and architecture, every decision on scope and priorities, review
  of every change, and running it.
- **Claude** (Anthropic), through Claude Code: most of the implementation,
  tests, simulations, benchmarks and documentation, and adversarial
  reviews of designs and code.
- **Codex** (OpenAI) and **Kimi** (Moonshot AI): implementation and
  reviews, as second opinions on designs and code.

## History and use of AI

I wanted a Durable Objects-style platform I could run myself, with actors
in any language and their data in a bucket I own, without a per-request
bill. WasmKeep is that,
and it is also an experiment: how far infrastructure software can go when
AI writes most of the code under a human's design and review.

It started from a design document I wrote: the architecture, the
guarantees, the milestones and the rules of the codebase. Its decisions are
recorded in the [ADRs](docs/adr). It is written with AI models, mostly Claude in Claude
Code, with Codex and Kimi for parts and reviews. I set the direction
and the priorities, take the decisions the ADRs record, and review and
merge every change; the models write most of the code, the tests and the
docs.

What keeps that honest is the same as for any codebase, applied
strictly:

- **Every change goes through a pull request.** Before a merge, the
  full check runs locally (`./scripts/check.sh --full`): formatting, lints, every test, 200-seed
  simulations, the oldest supported Rust, the guest SDKs, every example
  and language whose tools are installed, dependency audits and the docs
  build; hosted CI adds the other platforms, the S3 suites, the LTX
  conformance and the languages a machine lacks. Some tests are still sensitive to timing
  on shared runners, and CI on main has failed on them: since 0.1.0 a
  release is built only from a commit CI passed on (0.1.0 itself was
  tagged before its CI run had finished).
- **A bug fix starts with a failing test**; anything about ordering,
  leases or failover gets a simulation case.
- **Risky changes get adversarial reviews**: independent reviewers that
  try to break a design or a diff, each finding checked before it counts.
  The lease redesign in [ADR 0020](docs/adr/0020-one-lease-per-node.md)
  went through several rounds.
- **Numbers come from files** written by the benchmark scripts, never
  from memory, and the docs say what was not measured.
- **Decisions are written down** in the ADRs, including the ones revised
  after a mistake.

Branch commits and release tags are signed with my key; pull requests
are squash-merged, so commits on main carry GitHub's signature. They are
in my name: I answer for every one of them.

