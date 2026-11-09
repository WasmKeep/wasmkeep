# Who it is for, and real-world use cases

Most applications are well served by stateless servers in front of
Postgres, with Valkey (or Redis) for caching, queues and pub/sub. WasmKeep
does not replace that stack. It is for the parts of an application where
that stack gets awkward: state that many requests fight over, connections
that must meet in one place, timers that must fire exactly when they
should, and code or data that must stay isolated per customer.

The short version:

- **A good fit**: many small, independent pieces of state (a room, a user,
  a device, an API key, a document), each receiving a modest stream of
  events, often with WebSockets or timers attached.
- **Not a fit**: one big shared dataset you query across (reporting,
  search, analytics), a single object every request touches, or plain
  stateless APIs. Details in [the overview](overview.md#when-not-to-use-it).

## Next to Postgres, Valkey and stateless servers

| What you need | With stateless servers, Postgres and Valkey | With actors on WasmKeep |
| --- | --- | --- |
| A chat room or game lobby over WebSockets | Sticky sessions, or Valkey pub/sub to reach whichever server holds each socket; presence kept somewhere shared | The room is one actor: its sockets, its state and the order of its events in one place. Sockets stay open while it sleeps |
| Exact counters, quotas, rate limits under contention | Row locks (`SELECT ... FOR UPDATE`), retries on conflicts, or Lua scripts in Valkey | One actor per key handles one event at a time: no race to write code for |
| Timers: reminders, retries, expiries, "check every minute" | A cron job, a jobs table, a queue, and a lock so two workers do not both run it | An alarm on the actor, delivered at least once, also when its node dies |
| Per-customer isolation (their data, their plugin code) | Row-level security or a schema per tenant, plus a separate sandbox for their code | A Wasm sandbox with CPU and memory limits, and a database per actor |
| Lots of small, independent state | One large shared database every request contends on, sized for the peak | Many small SQLite databases, each next to its code; an idle actor costs disk, not memory |
| Running it | A database to operate, back up and fail over, plus the cache and the app servers | One binary per node and an S3-compatible bucket; no consensus cluster to operate |

**Use both.** Actors are islands: each one's data is fast to reach through
it, and slow to query across. Keep Postgres for what spans everything
(reporting, search, billing, analytics) and use actors for the hot,
coordinated, real-time parts. A typical split: your existing API and
database stay; chat, presence, rate limits, per-user workflows and
device state move to actors; actors call your API with outbound HTTP, or
push summaries to it, when the rest of the system needs to know.

## Use cases

Each pattern below names the actor, what it keeps, and the features it
leans on. Where the repository has a working example, it is linked.

### Real-time rooms: chat, presence, collaboration, game lobbies

- **One actor per** room, document or match.
- **Keeps**: members, recent history (SQLite), whatever the room needs.
- **Uses**: WebSockets attached to the actor (they survive hibernation),
  broadcasts to every socket, alarms for timeouts (an idle match closes,
  a turn times out).
- **Why actors**: every message for a room arrives at the same place, in
  order, without a pub/sub layer between servers.
- **Example**: [`examples/chat-room`](../examples/chat-room), also in
  [TypeScript](../examples/ts-chat-room), [Python](../examples/py-chat-room),
  [C](../examples/c-chat-room) and [Zig](../examples/zig-chat-room).

### Rate limits and quotas

- **One actor per** API key, IP, user or tenant.
- **Keeps**: the current window and its count.
- **Uses**: one event at a time (the count is exact), an alarm to close the
  window; an idle key deletes itself, so unused keys cost nothing.
- **Why actors**: no lock, no race between servers, no shared counter that
  every request contends on.
- **Example**: [`examples/rate-limiter`](../examples/rate-limiter).

### Scheduled and long-running work: monitors, reminders, workflows

- **One actor per** monitored URL, reminder, order or workflow run.
- **Keeps**: its state and history; the next step.
- **Uses**: alarms (persisted with the actor, retried with backoff when the
  handler fails, fired by another node if its owner dies), outbound HTTP,
  `exec-durable` for steps that must not be lost; for multi-step work, the
  durable [workflows](workflows.md) library (Rust and TypeScript, with
  clients in Go, Python, C and Zig), which runs each workflow on an
  ordinary actor.
- **Why actors**: the schedule lives with the state it acts on; there is
  no cron, job table or lock to keep consistent.
- **Example**: [Watch](../examples/watch), an uptime monitor with one actor
  per URL and a hub that delivers alerts to a webhook; what building it
  taught us is in [dogfood](dogfood.md). Workflows:
  [`examples/workflows`](../examples/workflows) and
  [`examples/ts-workflows`](../examples/ts-workflows).

### Per-user state: sessions, carts, settings

- **One actor per** user, cart or session.
- **Keeps**: the session, the cart, the user's settings.
- **Uses**: durable writes, alarms for expiry or reminders, outbound HTTP.
- **Why actors**: each user's state is local to its code and touched by
  one event at a time, so concurrent tabs or retries cannot corrupt it.

### AI agents

An agent is state plus a loop: a conversation's history, what it has
learned, the tools it called and what they returned, waiting for a model,
a tool or a person, then acting again. That is an actor.

- **One actor per** conversation, agent or task (a ticket, a document, a
  customer).
- **Keeps**: the conversation and its memory in SQLite (messages, facts,
  summaries, tool results), surviving restarts and node failures.
- **Uses**:
  - outbound HTTP to call a model and tools: hosted APIs by default
    (public addresses only); a model server on your network (Ollama,
    vLLM, an internal gateway) once `--allow-outbound` opens its range,
    and `--outbound-ca` trusts its private CA;
  - WebSockets to stream progress and answers to the user, attached to
    the agent's actor and kept while it sleeps;
  - alarms for anything that waits: a reminder, a follow-up, a retry of a
    failed tool call, a periodic check of an inbox or a feed. Delivered at
    least once, also when the node dies;
  - `actors.call` for agents that work together (a planner and workers),
    with call cycles refused instead of deadlocking;
  - a counter actor per user or key to cap model spend, exact under load.
- **Why actors**: two messages to the same conversation are handled in
  order, never interleaved; an agent waiting for a person or a slow tool
  costs disk, not memory; the history lives next to the code that uses it.
  Tool code you did not write (a plugin, or code a model produced) runs in
  the Wasm sandbox with CPU and memory limits, with the
  [pre-alpha caveat](security.md) given for customer code below.
- **Sovereignty**: model calls can stay on your network, and the history
  stays on your servers and in your bucket, for teams that cannot send
  conversations to a third party's runtime.
- **Example**: [`examples/agent`](../examples/agent), one actor per
  conversation calling an OpenAI-compatible endpoint (hosted, or Ollama
  through `--allow-outbound`), with the history in SQLite, a WebSocket
  feed, a reminder tool on an alarm and a cap on model calls per hour.

**Limits today**, and how to work with them:

- **30 s per event.** An event (a request, an alarm, a call) runs at most
  30 s of wall-clock time; programs embedding WasmKeep can change it
  (`ActorConfig::wall_per_event`), `wasmkeep serve` cannot yet. A long
  model call can reach it: stream the answer to the user over the
  WebSocket as it arrives, and split long work into steps (one tool call
  or model turn per event, the next one scheduled with an alarm or a
  call), recording progress in SQLite so a cut step resumes.
- **No model runs inside an actor.** Actors call models over HTTP; there
  is no GPU access from Wasm.
- **No vector index built in.** A conversation's own memory is small
  enough to search in SQLite (stored embeddings, compared in code). Search
  across a large corpus belongs in a dedicated store (pgvector, Qdrant)
  that agents call.
- **Language costs.** Python and TypeScript actors carry their runtime:
  more memory per resident agent than Rust, C or Zig; TypeScript on
  QuickJS, the default engine, uses about a fifth of StarlingMonkey's
  (741 KB against 3.4 MB)
  ([languages](languages.md), [benchmarks](benchmarks.md)).

### Device twins and the edge

- **One actor per** device or site.
- **Keeps**: the device's latest state and pending commands.
- **Uses**: alarms for missed heartbeats, WebSockets for live control.
- **Where**: on one small server per site, or a cluster with a local
  MinIO, without outbound network at run time.

### Customer code in a SaaS product

- **One actor per** tenant or plugin instance.
- **Keeps**: the plugin's own data, separate from every other tenant.
- **Uses**: the Wasm sandbox (no filesystem, sockets or environment unless
  granted; outbound HTTP limited to public addresses), a CPU budget per
  event, a memory cap per actor, access rules per class.
- **Why actors**: isolation of code and data per customer, from any
  language that compiles to a Wasm component.
- **Caveat**: today a process serves one tenant, and the sandbox protects
  the host from buggy code, not yet from a determined attacker
  ([security model](security.md)).

## Where it fits operationally

- **One VPS**: a single binary, data on local disk. A side project can run
  here for years.
- **A small cluster** (three nodes or more): any S3-compatible bucket that
  supports conditional writes (AWS S3, Cloudflare R2, OVHcloud Object
  Storage, MinIO; R2 and OVHcloud tested), which each node checks at start;
  a dead node's actors are restored on another, with no consensus cluster
  to run
  ([three nodes](deploy/three-nodes.md), or the
  [Compose example](../deploy/compose/README.md) to try it locally).
- **On-premises and sovereign deployments**: everything, the bucket
  included, can stay on your hardware and in your jurisdiction.
- **Hosting providers**: the same binary can be offered as a managed
  service; WasmKeep itself has no hosted cloud.

## Status

WasmKeep is pre-alpha. What exists and what is measured is in the
[ROADMAP](../ROADMAP.md) and [benchmarks](benchmarks.md). Clustering has
been soaked against Cloudflare R2 and OVHcloud Object Storage: three
nodes, one killed every 3 minutes under load, 36 kills over four
30-minute runs, nothing acknowledged lost
([measured](benchmarks.md#three-nodes-against-a-real-bucket)). It has not
yet carried real users' traffic.
