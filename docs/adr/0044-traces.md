# 0044. Traces

- Status: accepted (2026-11-25); built in two slices: the context, the
  exporter and the `request`, `front`, `event` and `bucket` spans on one
  node, then the `call` and `hop` spans, the context on a hop (section
  3) and sampling across nodes (section 4, amended while building: the
  heartbeat word `traces`). The exporter of section 5 is the choice: its
  size and dependency checks were measured on the building branch and
  the size accepted there; the load test is still to run, on the `0.9.0`
  fleet (no bench in the repository starts nodes with an exporter).
- Milestone: `0.9.0` (see [Now](../../ROADMAP.md#now) in ROADMAP.md)
- Follows: ADR 0011 (the cluster port), ADR 0023 (the front; the
  `x-wasmkeep-` range is the host's), ADR 0025 (several calls at once),
  ADR 0034 (batched writes), ADR 0038 (the sender and the proof on every
  hop), [ADR 0043](0043-rolling-upgrades.md) (the wire rule for hops,
  the node file and going back)
- Changes no interface: no WIT change, no bucket format, no signed
  header. One heartbeat word, `traces`, which a reader that predates it
  ignores (ADR 0043, the heartbeat's later lines)

## Context

A request can cross three nodes: a front on the node that received it,
an actor on a second, and an actor that one calls on a third, each
ending in a bucket write. Today an operator has, per node, metrics
(counts and histograms, with no link between a request and its parts)
and log lines. Finding where one slow request spent its time means
matching timestamps across three logs.

What the code does today, read at 0.8.0 and checked again at the
`v0.8.1` tag:

- **No span exists.** The `tracing` crate is used for log events only;
  no `span!`, `#[instrument]` or span field appears in the server.
- **A header outside the `x-wasmkeep-` range passes through.** A
  client's `traceparent` reaches the guest today, on the node that
  received it and across a forward, like any other header.
- **A guest can set it on `actors.call`.** The host drops only names in
  its own range from a call's headers (`actor/routing.rs`).
- **A forward's proof hashes the client's headers.** The admission
  proof (`x-wasmkeep-admitted`) covers every header that is not
  hop-by-hop, `host`, `expect` or in the host's range
  (`identity_headers`, `cluster/forwarding.rs`). A header added or
  changed after the proof is computed makes the proof fail.
- **A node's call signs a fixed list.** `x-wasmkeep-internal` covers the
  method, path, body hash and seven named headers. `traceparent` is not
  one of them, so adding it changes no signature.
- **The owner counts it.** Every header outside the range counts in the
  client's part of a request head (`client_head`, `http.rs`), on a hop
  too.
- **Every bucket request of a node passes one wrapper**
  (`counted_store.rs`), which already times each by operation and
  outcome.
- **An event and its request run on different tasks.** A request is
  handed to the actor's mailbox and runs on the actor's loop; an upload
  can outlive the event that caused it (ADR 0034).

## Decision

### 1. What a trace covers

A trace follows one request, or one event that has no request, through
the host. Six kinds of span, all made by the host:

| Span | Made where | Says |
|---|---|---|
| `request` | The node that receives an HTTP request, on the public or the cluster port (`http.rs`) | Method, status, what it was routed to (front, actor, asset, file link), duration |
| `front` | Around one run of the front (`front.rs`) | Wall time, CPU time, outcome |
| `event` | Around one handler run on an actor (the event path): a request, a call, an alarm, a socket open, message or close | Class, actor, kind, time waited in the mailbox, CPU time, whether the actor was woken, outcome |
| `call` | In the calling event or front, around one `actors.call`, local or to another node (`actor/routing.rs`); one per entry of a batch (ADR 0025) | Target class and actor, whether a request for it left the node (`remote`: true also for a placed call a peer refused and this node then ran), status, outcome (`answered`, `failed`, `timeout`) |
| `hop` | On the sending node, around one request to another node, from the moment it is prepared to the peer's answer (`cluster/forwarding.rs`, `cluster.rs`, `cluster/placing.rs`) | This node and the target node (`node`, `peer`), kind (`forward`, `upgrade`, `call`, `placed`, `hand_off`, `take`), status, outcome (`answered`, `failed`), whether it was sent again (`resent`), whether the header had room (`carried`) |
| `bucket` | In the counting wrapper, one per bucket request made inside a sampled span | Operation, outcome, key family (`wal`, `snapshot`, `owners`, `nodes`, ...), duration |

The request this record is tested by (a front on node a, an actor on b
that calls an actor on c) reads as:

```text
request (a)
└─ front (a)
   └─ call (a)
      └─ hop (a → b)
         └─ request (b)
            └─ event (b)
               ├─ bucket GET ×n      (the wake-up's restore, if cold)
               ├─ call (b)
               │  └─ hop (b → c)
               │     └─ request (c)
               │        └─ event (c)
               │           └─ bucket PUT   (the write that acknowledges)
               └─ bucket PUT
```

**Waking an actor** is part of its first event's span: the ownership
acquisition and the restore's bucket requests are its children.

**The write.** For a strict class the event is acknowledged after its
upload, so the PUT is a child of the event. For a batched class (ADR
0034) an upload can carry several events and end after them: it is a
child of the event that started the flight, and the other events carry
an attribute saying their write was batched. See "Not settled".

**An event without a request** (an alarm, a socket message, a close)
starts a trace of its own. A socket's open is an event of the upgrade's
request, on the owner, across a relay too.

**A hop outside any trace makes no span.** A drain's or a sweep's alarm
hand-off and a move's take belong to no request or event: they carry no
context, and the node that receives one draws for it as for any request.
An alarm handed off from inside its own event is that event's hop.

**Not traced:** heartbeats and lease renewals, collection, sweeps, the
idle pass, deploy following. They belong to no request; metrics cover
them. Guest code is not instrumented: what a handler does inside its
span is the app's to trace.

**Attributes are bounded.** Tenant, app, class, actor name, node id,
kind, status, durations, counts. Never a header, a body, a query
string, a path beyond its route kind, a bucket key, or a secret's name.
The actor name is the one high-cardinality value; it is what an
operator searches by.

### 2. `traceparent`, passed to guests as an ordinary header

The context is the W3C Trace Context header:
`traceparent: 00-<trace id, 32 hex>-<parent id, 16 hex>-<flags, 2 hex>`.
`tracestate` is passed on unchanged and never read.

- **To a guest.** A handler receives `traceparent` among its request's
  headers, naming the `event` (or `front`) span as the parent. It is an
  ordinary header: no WIT change, no new SDK function is needed to read
  it, and an app with its own tracing can use it as a parent.
- **From a guest, on `actors.call`.** The host sets `traceparent` on
  every call from the context of the event that makes it, replacing
  whatever the guest put there. The host knows which event is calling;
  a guest cannot move a call into another trace or turn sampling on.
- **From a guest, on outbound HTTP.** Nothing is added. A trace id sent
  to a third party is the app's decision; a guest that wants it copies
  the header it received.
- **On a response.** Nothing is added.
- **With tracing off** (the default), the host neither reads, writes
  nor replaces the header. A client's `traceparent` passes through
  exactly as it does in 0.8.

**A malformed header** (wrong lengths, an all-zero id, an unknown
version below `ff` with a short value) is treated as absent, and a new
trace starts. The value the guest then sees is the host's.

**Within the head limits.** The host adds or replaces a guest's header
only if the client's part of the head stays within the head limits with
it (the rule of section 3, condition 2); otherwise the guest receives the
request as the client sent it.

**Across nodes.** The header a call or a forward carries to another node
names the sending node's `hop` span (section 3), so the guest there sees
its own event's context, in the same trace.

### 3. On a hop

Nothing on a hop is signed differently, and no header is added to the
host's range. This is ADR 0043's wire rule: a `0.9` node sends hops as
`0.8.1` does, and a header outside the range may be added under three
conditions, each met here.

1. **Before the proof.** On a forward (a relayed upgrade and a placed
   request included), the sending node sets `traceparent` (parent: its
   `hop` span) before `sign_admission` computes the proof, since the
   proof hashes it. On a node's call the order does not matter; it is
   set before the call is signed all the same.
2. **Within the head limits.** A forward gains a header only when the
   client sent none: 70 bytes and one field. The node adds it only if
   the client's part of the head stays within the head limits with it,
   measured as the owner will count it (the forward's URI is the owner's
   address by then; the owner reads the client's `Host` instead, so the
   address is not counted too). Otherwise the request goes on without
   one and the owner starts a trace of its own (the `hop` span says
   `carried: false`). A value that takes no more room than the client's
   always replaces it: the head was within the limits when it arrived.
3. **Safe for a guest to see.** It is: that is section 2.

**What is signed, and why the header need not be.** The signed message
of a node's call is what `0.8.1` signs, byte for byte: `traceparent` is
in no line of it, so the MAC is the same with and without the header. A
forward's proof is computed as `0.8.1` computes it, same grammar, same
key, same hash of the client's headers; `traceparent` is one of those
headers, as it already is when a client sends one. Neither is checked to
authorize anything: who called, the chain, the app, the markers and the
client's identity are the signed fields, and none of them reads the
header. A forged or altered `traceparent` on a hop can attach a request
to another trace, start a new one, or change the sampled flag, and
nothing else. On a forward it cannot even do that without the proof
failing, which makes the request a client's.

**A node with tracing on sends its own header or none.** It announces
the heartbeat word `traces` (section 4), and its peers then follow the
flag on what it sends. So no hop of such a node carries a client's or a
guest's `traceparent`: inside a trace the header is replaced by the
hop's, and whenever the hop's is not set (no room for it, or outside any
trace) a header already there is removed. This is the one case where a
client's header does not go on as sent: a client's value that does not
parse, shorter than the host's, in a head with no room for the host's.

**The same bytes otherwise.** With tracing on, a call and a forward are
the requests `0.8.1` sends plus that one header line (a test compares
them with the bytes captured from the release before). With tracing
off, nothing is added, removed or read.

**Beside a node of the previous version.** Read in the code at the
`v0.8.1` tag: its `identity_headers` and its `signature` are the ones
above, so it verifies the hop as before; it removes only the
`x-wasmkeep-` range from what a guest receives, so it passes
`traceparent` to its guest as any header (the name appears nowhere in
its source); it counts the header in the client's part of the head, as
this version does when it decides whether to add it; and it ignores a
heartbeat word it does not know. It makes no span, and its own calls do
not carry the context on unless its guest copies the header, so a trace
through an old node shows a gap or splits in two. What it sends is not
followed (section 4). That is accepted for the length of an upgrade.

### 4. Sampling

The decision is made once, at the first span of a trace, and travels in
the header's sampled flag.

- **`--trace-sample <ratio>`**, 0 to 1, default `0.01`. Read only when
  an endpoint is set.
- **At the edge**, for a request from outside or an event without a
  request, the node draws at random with that ratio. It does not derive
  the decision from the trace id: a client chooses that id, and a
  decision computed from it would let a client choose the outcome.
- **An incoming `traceparent` from outside is continued**: the trace id
  is kept, so a caller's own tracing lines up with WasmKeep's spans.
  **Its sampled flag is ignored** unless the node runs with
  **`--trace-trust-incoming`**. Without the flag, no client can turn
  sampling on for its requests, and so no client can raise the node's
  export volume. With it, the incoming flag decides in both directions
  (a `01` is sampled, a `00` is not); it is for a node behind a proxy
  the operator controls that already samples.
- **On a hop that authenticates as a node's, from a node that announces
  `traces`**, the flag is followed, in both directions, with or without
  `--trace-trust-incoming`: the first node decided. Authenticates means
  a node's call whose signature checked, a forward whose proof checked,
  or, on the cluster port, the TLS connection of the node the hop names
  as its sender. A forward with no proof (plain HTTP, no secret loaded)
  is treated as a request from outside, as is a signed request on the
  public port in refuse mode.
- **The word `traces`** (heartbeat line 11) is announced by a node with
  tracing on. Added while building: authentication alone says who sent
  the hop, not who wrote the header. A node with tracing off, or of
  `0.8.1`, passes a client's `traceparent` through a forward whose proof
  still checks, and a guest's through a signed call. Following those
  flags would let a client or a guest turn sampling on at the next node.
  A node announcing the word sets the header itself or sends none
  (section 3), so its flag is its own decision. From any other node the
  flag is ignored and the receiver draws, as for a request from outside.
  A node that turns tracing on is followed once its peers have listed
  its next heartbeat; until then traces through it can be in pieces.
  The other way, a node restarted with tracing off, or put back on
  `0.8.1`, under the same id is still followed until its peers list its
  next heartbeat (one period, plus their listing): for that long a
  client's flag sent through it decides at the next node. Accepted: it
  is a restart's length, and what it costs is spans.
- **A hop with no header** (no room in the head, or sent outside any
  trace) starts a trace at the receiver, which draws.
- **An unsampled trace costs a context, not spans.** The node keeps the
  three values (trace id, a parent id, the flag) to pass on, creates no
  span object and exports nothing.

So a sampled trace is whole across the nodes it crosses, and an
unsampled one makes no span on any of them. Every node should run with
the same tracing settings; with tracing on at some nodes only, traces
stop or split at the others and nothing else follows.

**When the decision is made.** The `request` span starts before the
router runs, so the receiving node checks the hop first, and only when
there is a flag to follow: on the cluster port it reads the sender the
hop names against the connection's certificate; elsewhere it judges the
call's signature (once: the router then reads that verdict) or checks
the forward's proof (which spends nothing). The check comes before the
replay records are read. A replayed call whose signature still verifies
therefore gets its flag followed and is then refused: one `request`
span, in `accept` mode only.

A call's signature does not cover `traceparent`. On plain HTTP between
nodes (`accept` mode), someone on that network path could flip the flag
or the id of a call, or replay one within its minute. They can already
read every request there; what they gain is sampling. With TLS between
nodes the question does not arise.

**A trace id chosen by a client can be reused** across requests. With
the default ratio one in a hundred of them is sampled, and they share a
trace. A backend shows one large trace; nothing else follows. An
operator who minds runs a proxy that strips the header.

### 5. The exporter

**OTLP over HTTP with protobuf**, to one endpoint, **off by default**.

- `--otlp-endpoint <url>` (for example `http://collector:4318`), and
  the same key in the node file. No endpoint: no exporter is built, no
  span is made, no thread is started.
- Request headers for a collector that needs a credential come from the
  environment (`OTEL_EXPORTER_OTLP_HEADERS`), not from the node file,
  which is not a secrets file.
- Spans go through a bounded batch queue. A full queue drops spans and
  counts them (`wasmkeep_trace_spans_dropped_total`); export failures
  are counted too (`wasmkeep_trace_exports_total{outcome}`) and logged
  at most once a minute. **Exporting never delays or fails a request.**
- At a clean stop the queue is flushed with a bound of a few seconds.
- The resource names the service `wasmkeep`, with the version and the
  node id.
- The endpoint is the operator's: it is not subject to an app's egress
  policy, as the bucket is not.

**The node file and going back.** The node file rejects unknown keys
(ADR 0043). A `0.8.1` binary started on a file with the new key exits.
The upgrading page says to add the key after `finalize`, or to remove
it before going back; the command-line flag has the same effect and is
easier to remove.

#### What the crates cost: one measurement, and the check

One throwaway build, not the implementation: the tree at 0.8.0, Linux
x86-64, the pinned toolchain, the release profile (symbols stripped),
`cargo build --release -p wasmkeep-cli` before and after:

| | Before | After |
|---|---|---|
| `wasmkeep` binary | 57,175,088 bytes | 57,720,368 bytes |
| Packages in `Cargo.lock` | 448 | 461 |
| `cargo deny check` | advisories, bans, licenses, sources ok | the same, with no change to `deny.toml` |

**+545,280 bytes, 0.95%.**

Added to the CLI crate in that build, exact versions, default features
off (the implementation adds the first three to the server crate, and
not the fourth):

- `opentelemetry` 0.33.1 (`trace`)
- `opentelemetry_sdk` 0.33.1 (`trace`)
- `opentelemetry-otlp` 0.33.1 (`http-proto`, `reqwest-client`, `trace`)
- `tracing-opentelemetry` 0.34.0

The 13 new packages: those four, `opentelemetry-http`,
`opentelemetry-proto`, `prost`, `prost-derive`, `futures-executor`,
`portable-atomic`, and `rand` 0.9 with `rand_chacha` and `rand_core`.
All are Apache-2.0 or MIT, already on the allowed list.

What made it cheap: the exporter uses `reqwest` 0.13.5 with rustls,
which the tree already has for the bucket client. Nothing pulls gRPC
(`tonic`), a second TLS stack or a second HTTP client.

Limits of the measurement:

- **It is compile-only.** The build wired an exporter, a batch
  processor, a sampler and a layer at start, reachable at run time so
  the linker kept them. It contained no span call site and exported
  nothing. Call sites add little code; the run-time cost is unmeasured.
- The `otlp` crate turns its own `metrics` feature on through
  `http-proto`; it is in the number.
- `rand` 0.9 now sits beside the `rand` versions already in the tree. The bans check allows
  duplicate versions.
- Which `reqwest` client the batch processor needs at run time (async
  on the node's runtime, or blocking on the processor's own thread) was
  not exercised. The blocking one would add `reqwest`'s `blocking`
  feature.
- The attribution files (`NOTICE`, the licenses page) must be
  regenerated.

**The check, before the exporter is final.** That build says the cost is
likely small; it does not settle it. On the branch that builds traces,
against the release it starts from, these are measured and written into
this record:

- the size of the `wasmkeep` binary before and after, same profile and
  target;
- the packages added to `Cargo.lock`, by name;
- `cargo deny check` (advisories, bans, licenses, sources) with no
  exception added to `deny.toml` for them;
- the load test with tracing off and at the default ratio ("Tests",
  Load).

If the size or the dependency checks come out materially worse than the
table above, or the load test fails, the alternative kept in reserve
applies: spans as structured log lines with no new dependency
("Alternatives rejected"), unless the project accepts the measured cost
in this record. The span places and the context are the same either way.

#### The check on the building branch (2026-11-25)

On `feat/traces`, based on `a531d27d86d54112495955a0b79ee48bca38fd21`,
Linux x86-64, rustc 1.99.1, the same stripped release profile and
`cargo build --release -p wasmkeep-cli` before and after:

| | Before | After |
|---|---|---|
| `wasmkeep` binary | 57,450,544 bytes | 60,063,408 bytes |
| Packages in `Cargo.lock` | 448 | 460 |
| `cargo deny check` | Not rerun on the baseline | advisories, bans, licenses, sources ok |

**+2,612,864 bytes, 4.55%**, against the 0.95% of the throwaway build.

The 12 added packages are `opentelemetry`, `opentelemetry_sdk`,
`opentelemetry-otlp`, `opentelemetry-http`, `opentelemetry-proto`,
`prost`, `prost-derive`, `futures-executor`, `portable-atomic`,
`rand` 0.9, `rand_chacha` 0.9 and `rand_core` 0.9: the estimate's list
less `tracing-opentelemetry`, which is not used. No `deny.toml` exception
was added. Dependency notices and the license inventory were regenerated.

How each part of the check came out:

- **Dependencies: passed.** One package fewer than estimated, the same
  licenses, no exception.
- **Size: larger than estimated, and accepted.** The project accepts
  +2,612,864 bytes (4.55%) for `0.9.0` and keeps OTLP. The fallback
  would save that and give up what this record is for: structured log
  lines need a log pipeline the operator builds before one request can be
  followed across nodes. A cargo feature was considered and not taken: on
  by default it saves nothing in the released binary, and off by default
  the released binary has no traces. The OpenTelemetry crates are used in
  two files (`traces.rs`, `traces/exporter.rs`); the host's own code
  already carries the context, samples, queues and batches, so replacing
  the SDK with a hand-written OTLP encoder stays possible if the size
  matters later. That was not measured.
- **Load: open.** The comparison with tracing off and at the default
  ratio has not been run. The exporter is not final until it passes; the
  log-line fallback applies if it fails. Looked at again when hops were
  built: the benches in the repository (`bench/cluster.sh`,
  `bench/room.sh`) start their nodes themselves, with no way to pass an
  exporter's endpoint and no collector to send to, and the run this
  record names is the three-node room load on a real bucket. It is left
  to the `0.9.0` fleet run.

The exporter uses asynchronous `reqwest` on a thread and a
current-thread runtime of its own, with no `blocking` feature. Its
client follows no redirect and takes no proxy from the environment.
Compression is not built in: `OTEL_EXPORTER_OTLP_COMPRESSION` is ignored
with a warning.

### 6. How spans are made

**The host carries the context itself.** A small value (trace id, span
id, sampled) travels with the request into the mailbox job, into the
call, and into an upload's flight. Spans are created through the
OpenTelemetry API from that value, at the six places of section 1.

The `tracing` crate's spans are not used for this, for two reasons read
in the code:

- An event runs on the actor's task, not the request's, and an upload
  can run on a third. A task-local "current span" does not follow that
  path; the context has to be passed by hand whichever API makes the
  spans.
- The log output is built by `tracing`'s formatter, which prints the
  spans an event is inside. Spans added at a level the log shows would
  change every log line.

So `tracing-opentelemetry` is likely not needed, and the measured cost
is an upper bound. It stays in the measurement because the choice is
the implementer's to confirm.

**Logs.** A warning or an error logged inside a sampled span carries
`trace_id` as a field. Nothing else changes in the log.

**With tracing off** each of the places does one read that finds
nothing: whether the process has a tracer (set once at start, never when
no endpoint is given), or whether the running task carries a context
(none does). No random number is drawn, no header is read or written,
and nothing is measured for an attribute.

**Attributes that change.** A span's `outcome` starts as `not_run` and is
replaced by what happened; it is written once, when the span ends.

## Alternatives rejected

- **Spans as structured log lines only.** One JSON line per span, with
  its ids and duration, and no new dependency. Rejected as the default:
  no collector reads it without a log pipeline the operator builds and
  maintains, and joining lines from three nodes is the problem this
  record starts from. It comes back if the check of section 5 fails
  (the size or the dependency checks on the building branch, or the
  load test at the default ratio), or if a later OpenTelemetry release
  fails the dependency checks. The six span places and the
  context are the same, so the switch is the exporter alone.
- **OTLP over gRPC.** Pulls `tonic` and its stack for no gain here;
  every collector takes OTLP over HTTP.
- **A WIT function for the trace context.** A guest already receives
  headers. A function would be a new import, and a component using it
  would not link on a node without it (ADR 0043's gate table).
- **A header in the host's range** (`x-wasmkeep-trace`). A node of the
  previous version would pass an unknown header of that range to the
  guest on a signed call, so it would need a capability word, and no
  outside tool would understand it.
- **Signing `traceparent` into a node's call.** It would change the
  signed set, which ADR 0043 freezes for `0.9`, to protect a sampling
  flag.
- **Deciding the sample from the trace id**, the usual ratio sampler.
  It gives every node the same answer without passing a flag, but the
  id is the client's to choose.
- **Not continuing a client's trace id** unless trusted. Safer against
  reuse of an id, but a caller's own trace then never lines up with
  WasmKeep's spans. Ignoring only the flag gives the same protection of
  the export volume.
- **Tracing on by default.** An exporter with no endpoint has nowhere
  to send, and a default endpoint would be a request to the network
  nobody asked for.

## Consequences

- Three operator settings (`--otlp-endpoint`, `--trace-sample`,
  `--trace-trust-incoming`), each also a node-file key, and two metrics.
- One heartbeat word, `traces`, from a node with tracing on. It gates
  nothing: it says whose `traceparent` a hop carries.
- With tracing on, a client's `traceparent` no longer reaches the guest
  of a forwarded request as sent: the trace id is kept, the parent is
  the host's.
- A guest sees a `traceparent` it did not see before when tracing is on
  and the client sent none. An app that rejects unknown headers would
  notice; none of the SDKs do.
- With tracing on, the host replaces `traceparent` on `actors.call`. An
  app that carried its own context in that header between actors loses
  it there; it can use a header of its own.
- The binary grows by 2.6 MB (4.55%) and 12 packages, measured on the
  building branch (section 5); each package is one more to keep through
  the dependency checks.
- A trace through a node of the previous version has a gap.
- Bucket spans exist only inside sampled traces. The request counts and
  histograms stay the full picture of bucket use.

## Tests

- **Header.** Parsing and writing `traceparent`; each malformed form
  read as absent; `tracestate` unchanged.
- **Off.** With no endpoint, a client's `traceparent` reaches the guest
  byte for byte, on one node and across a forward.
- **Guest.** A handler sees the event's context; a guest-set
  `traceparent` on `actors.call` is replaced; outbound HTTP carries
  only what the guest set.
- **Sampling.** An outside `01` is not sampled without the trust flag
  beyond the ratio, and is with it; a `00` with the trust flag is not;
  a hop whose signature or proof checked, from a node announcing
  `traces`, is followed, and from any other node is not; a forward with
  no proof is treated as outside. At a ratio between 0 and 1, every
  trace through two or three nodes has all its spans or none.
- **Hop.** A forward with an added `traceparent` verifies at the owner,
  and one changed after the proof does not; a head at the limit is
  forwarded unchanged; a call and a forward with tracing on are the
  bytes captured from the release before plus one header line, the
  call's MAC over the same message; a hop to and from a node of the
  previous version passes (ADR 0043, the mixed-version check: its
  tracing phase is not written yet).
- **One trace, three nodes.** A cluster test with an exporter in
  memory, plain and over TLS: a request through a front on a to an
  actor on b that calls an actor on c gives one trace id, spans from
  three nodes, the parents of section 1 and one bucket write under the
  last event. A forward and a relayed upgrade likewise, on two nodes.
- **Back-pressure.** A collector that does not answer: requests are
  served at the same latency, the drop counter moves, the process stops
  within its bound.
- **Load.** The cluster run of ADR 0043, section 8 (a held room load on
  three nodes and a real bucket) is unchanged with tracing off, and
  passes its limits at the default ratio.

## Not settled

1. ~~The dependency cost on the real branch.~~ Settled (section 5, the
   check on the building branch): measured, the size accepted.
2. **Run-time cost.** Not measured at all. The load test at the default
   ratio is the test.
3. **The default ratio.** `0.01` is a starting value. Under the load
   test it is several hundred traces a second across the cluster, which
   the batch queue's size must be set for.
4. **Batched uploads.** Parenting a shared upload to the event that
   started the flight is the simplest rule; span links from the other
   events would be more exact and are more work. To decide when the
   batched path is instrumented.
5. **Socket messages.** A trace per message is right at a low ratio and
   says nothing about a connection as a whole. Whether a socket's
   events should share one trace was left open.
6. ~~`tracing-opentelemetry`.~~ Settled: not used.
7. ~~The `reqwest` client flavour.~~ Settled: the asynchronous client, on
   the exporter's own thread and runtime.
8. **Whether the actor name belongs in attributes** for every operator.
   A switch to leave it out is cheap and was not designed.
9. **A replayed call over plain HTTP** costs one `request` span each
   time: the flag is followed before the replay records are read
   (`admission_replayed` could be consulted first).
10. **A forward over plain HTTP is verified twice** when its flag is
    followed and the app has rate limits: once for the flag, once for
    admission.
11. **No end-to-end case of a sender without the word.** The unit tests
    cover it; a tracing phase in the mixed-version check is to cover it
    before `0.9.0`.
