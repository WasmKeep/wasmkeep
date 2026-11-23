# Security model

What WasmKeep protects, against whom, and how; what it does not do yet. The
limits below are enforced by the code (each has a test), not aspirations.

> **Pre-alpha.** Run code you trust, or code from people you trust. The
> Wasm sandbox protects the host from buggy actors; it is not yet a promise
> against a determined, hostile tenant (side channels such as Spectre are
> out of scope until a process- or VM-per-tenant tier exists).

## Who can do what

| Party | Can | Cannot |
| --- | --- | --- |
| Anyone who reaches the HTTP port | Send requests and open WebSockets to actors of `open` classes (creating actors by naming them), and to existing actors of `existing` classes | Reach `internal` classes; pass for another actor or node; create actors of `existing` classes; hold connections, bodies or sockets past the limits below |
| An actor (tenant code) | Use its own database, alarm, sockets, configuration; call actors of its tenant; make outbound HTTP to public addresses | Read files, open sockets, see the node's environment, reach private networks or cloud metadata, touch another actor's data, crash the host, run past its CPU, wall-clock, memory and SQL limits |
| A node of the cluster | Sign calls to other nodes; take leases; write the bucket | (Trusted: nodes and the bucket's owner control the cluster) |
| Whoever can write the bucket | Everything: data, deploys, the cluster secret | (Trusted) |

## Layers

**Front door** (`crates/wasmkeep-server/src/http.rs`)
- 30 s to send request headers, and idle limit between kept-alive requests.
- 10,000 connections per node (`--max-connections`); more wait in the
  kernel backlog. Accept errors back off instead of spinning.
- Request bodies: 16 MB (413 when declared larger; a failed read past it).
  What a handler leaves unread is read and discarded, within the same
  16 MB and for at most 30 s after the handler let go of the body
  (not the body of a request sent with `Expect: 100-continue` whose
  handler read none of it: it is never asked for, see
  [concepts](concepts.md)).
- WebSockets: 1 MiB per client message, 256 messages or 4 MiB queued per
  client, requested defaults of 1,000 sockets per actor and 50,000 sessions
  per node (including relays), configurable with
  [socket admission options](reference/socket-limits.md) and reduced by the
  descriptor budget; zero refuses every WebSocket at that scope. Per-app
  isolation requires `--max-sockets-per-app` (or `SocketLimits::per_app`)
  below the effective node cap; its default alone does not isolate apps.
  Over capacity: HTTP 503 with `Retry-After`. Opening and closing sessions
  retain capacity; idle clients are pinged then closed.
  A socket is invisible to the actor's other events until
  `on-websocket-open` accepts it. Close reasons never carry host errors.
- `x-wasmkeep-*` headers from clients are stripped or ignored: only the host
  sets who is calling. None reaches the front, none the front sets reaches
  the client, and none a guest sets on `actors.call` reaches the callee (but the
  SDKs' four key-value and queue headers, which the host gives no meaning).

**Who may create an actor** (ADR 0010): per class, `open` (default),
`existing` (outside requests reach only existing actors; nothing is created
for unknown names), `existing:NAME,...`, or `internal` (actors only).

**The guest sandbox**
- Capabilities: only what the `wasmkeep:actor` world imports. Filesystem and
  sockets interfaces that toolchains import anyway are inert (no
  directories, every socket and lookup refused).
- CPU: 50 ms of guest CPU time per event (the thread's own CPU time, so
  other threads sharing the core are not charged to the guest), and 2 s of SQL CPU per event
  (SQLite runs in the host, so it has its own budget, measured with the
  thread's CPU clock and enforced inside a statement); wall clock 30 s per
  event (also for a front request, response included). The front has
  its own CPU budget, `--front-cpu-budget-ms` (50 ms by default), and its
  own slots in the instance pool (`--max-front-requests`).
- Memory: 128 MiB per linear memory, 2 memories and 32 core instances per
  component; resident actors capped at the instance pool less the front's
  slots (90% by default), the least recently active idle one hibernating
  to make room. Outside its linear memory, a guest holds at most 4,096
  host resources at once (handles: requests, headers, bodies, streams,
  pollables), and a `wasi:http` `fields` it builds, or copies to change,
  at most 64 KiB (counting a few dozen bytes per entry on top of its names
  and values). Past either, the call traps: the event fails, and an
  actor's instance is replaced by a fresh one at its next event. A
  response to an outbound request fails past 64 KiB of head (status line
  and headers), and its body past 64 KiB of trailers. An `actors.call`
  carries at most 256 headers and 64 KiB of names and values (else
  `too-large`). A body a guest writes queues at most 64 KiB in the host
  until it is sent (writes of at most 32 KiB at a time). So a guest that
  fills its table with requests it never sends, each with full headers
  and a full queue, holds about 512 MiB per instance (4,096 × 128 KiB)
  in the worst case. A request it was sent carries the headers its client
  sent, bounded by the HTTP parser (100 headers in about 400 KiB); one
  that keeps those requests instead of dropping them holds up to that
  each.
- Outbound HTTP: public addresses only (the host resolves, checks and
  connects to the checked address; TLS verified against the name);
  `--allow-outbound` and `--outbound-ca` open specific internal services.
  The ports browsers block are refused too (the Fetch standard's list:
  SMTP 25, 465 and 587, FTP, SSH, IRC and other non-HTTP services), since
  a crafted HTTP request can drive some of those servers;
  `--allow-outbound-port` opens one. 16 requests in flight per actor, 256
  per node.
- What a guest prints reaches the node's log within a line rate per
  instance, per app and per process (`--guest-log-rate`; past it, lines are dropped
  and counted), as the last field of its line (`line="..."`, after the
  app, the actor and the stream), cut at 4 KB a line once escaped.
  Quotes and backslashes are escaped, and so are controls, format
  characters (bidi controls, zero-width ones) and line separators, in
  the text and in actor names (a name with a space or `=` is quoted): a
  guest cannot flood the log, start a line of its own, add fields to its
  line, reorder what a terminal shows or send terminal escapes. What it prints is its choice, secrets included, so the node's
  log is as sensitive as the actors make it.

**SQLite sandbox** (`crates/wasmkeep-storage/src/db.rs`)
- No `ATTACH`, `VACUUM`, extensions, `fts3_tokenizer`, or temporary
  tables, views, triggers or indexes (ordinary ones are allowed, but not on
  host tables); virtual tables limited to fts3/fts4/fts5/rtree;
  pragmas from an allow-list; defensive mode; `trusted_schema=OFF`. Any
  other authorizer action is refused.
- Host tables (`_wasmkeep_*`) invisible to guests: a guest can neither read
  nor write them, nor give a table, view, index or trigger such a name,
  rename onto one, or declare a foreign key to a host table. Their names and
  schema (not their rows) show in `sqlite_master`. Host statements run with
  triggers and foreign key enforcement off, so they run nothing a guest
  wrote.
- 1 GB per database; 16 MiB per value and per query result. The result
  budget counts text/blob payloads after UTF-8 replacement, column names
  once, and conservative result/row/cell, allocation and vector-capacity
  overhead, including empty values ([details](concepts.md#storage)). A statement
  is stopped at the event's wall clock and SQL CPU budget, checked between
  steps of SQLite's virtual machine and inside `instr`, `replace` and
  `trim`. A stop inside one of these functions fails the statement only;
  it does not roll back the guest's open transaction (a stop between steps
  of a statement that writes does). A single step over a large value is
  not interrupted, so a statement can overrun the budgets; `LIKE` and
  `GLOB` are the costliest, their time growing with the value's length
  times the pattern's (up to SQLite's 50,000-byte patterns). Host
  statements have the same wall-clock limit.
- At most 2,000,000 instructions per compiled statement (past it,
  `quota-exceeded`). SQLite's own limits on columns, function arguments,
  expression depth and compound `SELECT` terms are kept, set explicitly:
  SQLite also applies them when it loads a stored schema, so lower ones
  would make existing databases unreadable. SQLite's heap is capped at
  4 GiB for the whole process, temporary data (sorts, materialised
  subqueries) included, as it stays in memory: a statement that needs more
  fails with `quota-exceeded`. The cap is shared, so while one statement
  holds most of it, others can fail too.
- 256 MB of write-ahead log between two captures: past it, statements that
  write fail until the event ends (reads, commit and rollback still run),
  or, when another connection reading the database keeps the log from
  being emptied, until it stops.
  Checked before a statement that writes and while it runs; one stopped
  while it runs rolls back the event's open transaction, as at a deadline,
  and can have written past the cap by what it wrote since the last check.
  Capture reads the log frame by frame and holds only the newest version of
  each changed page, so rewriting the same rows costs no memory (a
  transaction touching many distinct pages still holds them all).
- [Files](files.md#limits) written by streamed writes and upload links
  are spooled to the node's disk within a budget: 1 GiB per app (at
  least one file of `--max-blob-bytes`, at most half the process's) and
  4 GiB per process (at least two such files). An upload link holds its
  whole size before it reads its body, one body per link and node at a
  time, and a body that sends nothing for 60 s or averages under
  16 KiB/s after its first 30 s is aborted. A streamed write holds each
  byte as it is written, an instance has at most 16 open at once, and
  one left open goes when its event ends. A whole-file `put` (16 MiB at
  most) is outside these budgets. Without a bucket, the node-link key
  file is written whole and linked into place, and refused unless it is
  32 bytes.

**Between nodes** (`crates/wasmkeep-server/src/cluster.rs`)
- Calls carry an HMAC-SHA256 signature (secret from the bucket, never sent)
  over the target node, method, path, the body's SHA-256, caller, chain,
  time and a nonce unique to each send (seeded by random bytes never
  published, so no call can be signed ahead of the node that sends it);
  valid for a minute, accepted once (the replay record expires in order
  and holds up to 500,000 signatures, several thousand calls a second into
  one node). That record does not survive a restart, so a signature dated
  before the receiving node (or its runtime) started, plus the allowed
  clock skew (`max_skew`, for a sender whose clock is ahead), is refused
  with a 503 and signed again, even within its minute. The start is read
  on the monotonic clock, so this node's clock stepping either way after
  it started moves the floor with it. Calls in the first `max_skew` after
  a start, and those of a peer whose clock is behind by d for d longer,
  are refused that way (`wasmkeep_signatures_before_start_total`). This
  assumes the previous process stopped taking calls before its successor
  started, as a restart does; a replacement started elsewhere while the
  old one still serves is not covered. The receiver checks the body
  against it before the actor runs (only a validly signed call is
  buffered). Heartbeats count only under their own key.
- Without a cluster listener, only with `--cluster-plaintext accept` (by
  default `serve` refuses to start a cluster node without one: nodes talk
  over mutual TLS by default): plain HTTP, not encrypted, and responses
  are not signed: keep node-to-node traffic on a private network. A node
  without one warns when it advertises a public address.
- With one (`--cluster-listen`, `--cluster-advertise`; ADR 0011): mutual
  TLS 1.3 to and from peers that also have one, each side's certificate
  pinned in its live heartbeat; no session resumption; the client's pin is
  checked again on every request. The port serves actor paths and the
  health and readiness checks only, and never forwards a request again.
- Revocation: `wasmkeep cluster fence <node>` (ADR 0020) writes a fence
  over the node's heartbeat (its node lease): the running process stops at
  its next heartbeat write, no peer trusts it from its next listing, and
  the actors it held are free 15 s after its last write. A fence revokes
  that process, not the machine: a service manager that restarts it starts
  a new process, which passes its start check once the fence is 15 s old
  and is trusted again. To revoke a machine, also stop its service and
  disable the restart, and rotate what it holds: the bucket credentials,
  and the cluster secret (`cluster/secret`) if it may have been read.
  Deleting the heartbeat does not revoke cleanly: the node stops too, but
  its actors stay held until a fence or a new process with its id. With no
  heartbeat object, the fence `cluster fence` creates is dated by the
  bucket's own time for that write, never earlier than the operator's
  clock (a clock running behind would free the actors too early). A peer stops being trusted
  once its heartbeat is fenced or released, deleted, names a new key, or
  expires (its ttl, three heartbeat periods, plus the
  allowed clock skew); a node that cannot list the heartbeats trusts each
  pin only until its heartbeat's expiry. From then on its handshakes are
  refused, its kept-alive requests get a 503, nothing is sent to it
  (pooled connections included), relayed WebSockets are cut on either
  side within 500 ms, and answers from it or to it (forwarded responses,
  `actors.call`) are not delivered, even if already streaming. A request
  it sent before that still runs to the end: only its answer is withheld.
- `--cluster-plaintext refuse` (the default; phase B of [ADR 0011](adr/0011-node-to-node-mtls.md)) closes the plaintext path: a
  node's call counts only on the cluster port. A signed call on the
  public port is treated as an outside client's (access rules apply, no
  caller, no chain, `internal` classes refused), and a valid signature
  there is spent in a record of its own (up to 50,000), which the
  cluster port's checks read, so a plaintext copy is not replayed onto
  the cluster port. That record never fills the cluster port's own
  records: signed calls on the public port, however many, cannot refuse
  a node's calls, takes or alarm hand-offs. Once it is full, new ones are
  dropped (`wasmkeep_signatures_public_dropped_total`), and the cluster
  port's mutual TLS alone keeps
  such a copy out; peers
  without a cluster listener are not used (their actors get a 503 from
  this node), and TLS peers are never reached in plaintext. The forwarded
  header gives no privilege on any port: it only stops a request being
  forwarded again.
- An app is two prefixes, `apps/<tenant>/<app>/` (deploys and secrets)
  and `state/<tenant>/<app>/` (its actors' state), so several apps of one
  tenant can share a bucket prefix without sharing an actor
  ([ADR 0022](adr/0022-app-owned-state-and-the-one-time-migration.md)).
  A node refuses a hop between nodes that names another app. `nodes/`,
  `cluster/` and `control/drain/` stay shared by the prefix: fencing and
  draining name a node, not an app.
- One bucket prefix is one trust domain: every node on it signs calls with
  the same cluster secret, whatever app or tenant it serves.

**The bucket** (`crates/wasmkeep-server/src/replica.rs`)
- Restore streams one object and one page at a time; chains that would
  build a database over twice the node's cap, objects larger than such a
  database, and files without checksums are refused; every file's chain
  checksums are verified before the result is put in place.
- Every read of a restore is given up after 5 s of silence and sent
  again, up to three times; then the restore fails and the next event
  restores from a fresh listing.
- A node proves at start, before it writes a heartbeat or takes a lease,
  that the bucket enforces conditional writes: on a throwaway object under
  `probe/`, a create over it and a replace with a stale version must be
  refused. A bucket that ignores them would let two nodes own one actor,
  so it is refused: the node stays not ready and `serve` exits
  (`wasmkeep_cluster::bucket_check`).

## Configuration and secrets

`--env CLASS=FILE` (`NAME=value` files, readable only by the server: on
Unix a file is refused at start if other users can open it, if its group
can write it, or if its group can read it while it is the server's user's
own. So `0600` the server's user's, or `0640` root's with the server's
group, which the service cannot rewrite. The same holds for `--front-env`
files and for `wasmkeep dev`'s local values, which are read with a
warning on a filesystem that keeps no file modes).
Values never reach the data directory, the bucket or the logs; parse errors
name the file and line, never the value.

`wasmkeep deploy --config CLASS=FILE` stores plain configuration in the
bucket, in the clear, as part of the version
([ADR 0013](adr/0013-access-rules-and-config-in-deploys.md)): only for what
anyone who can read the bucket may see (whoever can read it can already
read every actor's data). Comments are not uploaded; names that look like
secrets are warned about without the value; the manifest holds digests,
not values; `Debug` output of configuration shows names only. Node `--env`
values win by name.

`wasmkeep secrets` stores an app's secrets in the bucket, sealed
([ADR 0016](adr/0016-secrets-in-the-bucket.md)): ChaCha20-Poly1305 under a
key derived per app from a 32-byte secrets key, which the operator gives
to the nodes (`--secrets-key`, refused if its group or others can read
it) and to the machine running `wasmkeep secrets`. Each object opens only at its own bucket
prefix, tenant, app and sequence number. Values come from stdin or a file,
never from the command line, and are never printed or logged.

- **Protects against** anyone who can read the bucket, or its backups:
  they see neither names nor values, and cannot forge or move a sealed
  object.
- **Does not protect against** whoever can write the bucket: they can
  deploy code that reads `env.get` and sends the values out. Signed deploys
  (planned, not yet designed) would narrow that.
- **A node that cannot open the current secrets** (no key, another key, an
  altered object at once; 30 s without reaching the current set) refuses
  the app's events with 503, hands its actors to a node that can, takes
  none, and reports not ready. Past those windows it never runs an actor
  with an older or empty set; when no node can open them, the app's events
  are all 503.
- `cluster/secret` (node-call signing) is still stored in the clear; a
  bucket reader can sign node calls on a node started with
  `--cluster-plaintext accept`. With `refuse` (the default) node calls
  are honoured only on the mutual-TLS cluster port, whose peers are
  pinned in their heartbeats.

Access rules travel with the deployed version too: a node following an app
applies the version's rules, decided with the class each event takes, so
no event runs new code under an old rule or the reverse. `--access` is
refused with `--app` once rules are deployed. A rollback that reopens a
class needs `--allow-reopen`.

## Several apps in one process

A process serving several apps ([the node file](reference/node-file.md))
shares one cluster certificate between its apps' node ids, each of which
publishes its pin. Trust is per node id, not per certificate:

- every hop a node signs names its sender, bound to the hop's signature
  under the cluster secret; on the cluster port the sender's own live
  heartbeat must publish the pin the connection presented, and its answer
  or relayed socket stays trusted only while it does. A healthy app's
  heartbeat never keeps a lapsed sibling's hops or relays trusted;
- a process of several apps requires the sender on every hop and a signed
  proof on every forward, and refuses a refused hop with 503: it never
  runs as a client's request;
- the public port of such a process removes every `x-wasmkeep-*` header,
  and routes by hostname only;
- `--cluster-plaintext accept` is refused with several apps.

The apps share the process's crashes, engine and memory: per-app floors
and ceilings protect against a noisy neighbour, not a hostile one. Put
apps of different trust in different processes.

## Known gaps

- Hostile multi-tenancy: every actor of a node shares one process (see the note above).
  Isolation tiers per app (a process per app, or one started by an
  operator's launcher inside a microVM, Kata Containers or gVisor) are
  planned, not built ([roadmap](../ROADMAP.md#later-on-demand)).
- Node-to-node traffic goes over mutual TLS by default
  ([ADR 0011](adr/0011-node-to-node-mtls.md)): a node in a cluster needs
  `--cluster-listen` and `--cluster-advertise`, and refuses plain HTTP
  between nodes. A node started with `--cluster-plaintext accept` opens
  the plaintext path again: without a listener it reaches peers at their
  plain address, and with one it stays reachable, signed calls included,
  on its public port; there a response to a call is not signed, and
  someone on the path can read traffic and change responses. Refusing
  nodes still serve outside clients on the public port as usual, and the
  mode is not published: nothing stops a node from being started in
  `accept` mode later, so each node's flags are what count.
- Host work other than SQL (encoding sends and HTTP, WebSocket frames) is
  not charged to any CPU budget; it is small per call, and the wall clock
  bounds it.
- In a cluster, a deleted actor of an `existing` class can be woken from
  outside (as an empty actor).
- Limits are per node. A process serves one tenant (several tenants per
  process is planned but not started), so
  today that is also per tenant; within a tenant, one class or
  actor can use the node's whole share (instance pool, connections,
  sockets), bounded per actor only where noted above.

## Audits

Before the first public release the code had an internal security review
in ten parts (front door, guest boundary, cluster); each fix came with a
test that fails without it. Later hardening: plain HTTP between nodes
refused by default, outbound HTTP refusing the ports browsers block, and
limits on what actors print reaching the node's log (all described above).
No independent third-party audit has been done yet.

## Reporting a vulnerability

Report privately through the repository's security advisories, not in a
public issue.

## Ingress rate limits

An app's [project-file limits](project-file.md#ingress-limits) run before body
consumption, front instance allocation and guest actor work, including
public actor routes and WebSocket handshakes. Forged `x-wasmkeep-*` headers
do not exempt public requests. Cluster-port traffic is authenticated by
mutual TLS. Quotas are local to a node and reset when its process restarts.

Read-only owner hints may precede admission. The `remote_owner` lookup in
`actor/routing.rs` still performs one bucket GET per cold actor name before the
limit check. Over-limit clients cycling random names can therefore cost bucket
reads and owner-hint cache churn, without creating actor state.
If routing selects a remote owner,
that owner charges the request using a signed client identity. Otherwise the
receiving node admits before creating or waking an actor, acquiring ownership,
or reserving a mailbox slot, for both HTTP requests and WebSocket handshakes.
A later ownership race may still cause forwarding: a charged request carries
an authenticated charged bit in that identity and is not charged again by the
owner. Admission may spend quota even if acquisition or execution subsequently
fails; races may over-charge, but cannot bypass admission before local actor work.

When its cluster secret is already loaded, a forwarder replaces client-supplied
proofs with a domain-separated signed client identity: the normalized TCP IP key
(IPv6 by its 64-bit prefix, IPv4-mapped addresses equivalent to IPv4), bound to
the destination owner, method, path and query, app, timestamp, end-to-end header
values and charged bit. An uncharged identity makes the owner apply its own
policies using that IP or the policy's header key. Only successful admission
matching at least one local rule sets the charged bit. Replayed uncharged proofs
consume owner quota again. Charged proofs are single-use, with a unique send nonce
and a separate per-node replay store capped at 4,096 signatures, isolated from
actor-call and control replay stores. Entries remain until their signature window
expires; replays and new charged proofs when the store is full return 503, never
an unchecked exemption. Uncharged proofs do not consume replay-store capacity.
The proof grants no actor privileges and does not authenticate the body.
Missing, invalid or expired proofs on forwarded requests return 503 when the
owner has policies. Without policies, unsigned forwards retain their previous
behavior. Signing never fetches a cold secret. Clock skew outside the signature
window still invalidates a proof on a policy-enforcing owner.
Forwarded public traffic is charged on both plaintext and mutual-TLS transports;
authenticated internal actor calls remain exempt. Already-spent internal call
signatures return 403 before body reads in plaintext mode; fresh signatures still
undergo body and atomic replay checks before actor work.

IP keys use only the TCP peer, with IPv6 aggregated to 64-bit subnet prefixes; IPv4-mapped IPv6
uses the IPv4 key. Forwarded addresses are not trusted or parsed. Behind a proxy,
use its shared IP quota or an edge-overwritten header and restrict direct access
to the node. Header keys are not credentials; a client allowed to choose one can
rotate identities. Header values are hashed and absent from logs and metric labels.

Matching is case-sensitive, excludes the query, decodes unreserved percent
escapes, canonicalizes remaining escape hex to uppercase, and collapses repeated
slashes outside actor routing identity. Both the raw dot-segment form and the
resolved form of requests and rules are matched; a match in either charges once.
Dot segments include percent-encoded `.` and `..`. Under `/actor/`, class and name
are split exactly as the actor router splits them: dots never remove or alter
those segments, and resolution of the remaining path cannot climb above the
actor name. Actor names use the router's full percent and UTF-8 decoding, then
canonical percent encoding to preserve segment boundaries. Exact rules ignore
trailing slashes. A `GET` rule also covers `HEAD`; a `HEAD` rule covers only `HEAD`.
Other reserved escapes stay encoded: the paths in `https://example.test/a%2Fb`
and `https://example.test/a/b` differ. Prefix rules retain their final slash;
a prefix covers that slash and descendants, not the path without the slash. This normalization is only for policy matching; it does not rewrite
the URI delivered to routing or guest code.

Only app services with policies share the 65,536-bucket process budget, capped
at 4,096 buckets per service. Publishing or removing policies and dropping a
service rebalance shares. Under pressure, fully refilled buckets are evicted
first; only then are depleted buckets evicted in least-recently-used order.
Current request buckets are protected. A separate ordered refill-deadline index
selects eligible full buckets in O(log n) eviction work (with at most 32 protected
keys); depleted fallback uses the LRU index. Unrelated credits and clocks are
unchanged. Registry activation runs only during policy reconciliation, including
initial publication; request admission never locks the process-wide registry. Rejected quota attempts count as
uses. Evicted identities get a fresh burst if they return, so cycling more keys
than the actual policy-service share can still weaken enforcement; these are
bounded admission controls, not durable quotas.
If an admission needs more simultaneous rule buckets than its app share, it
returns 429 with `Retry-After: 1` and the capacity outcome.

If its current or pending version has policies, a node that cannot apply the
deployment refuses public intake with 503, including direct actor requests.
Undecodable manifests also fail closed in rate-limit-aware binaries.

Offline or later-joining older binaries are not protected by the live-reader
check: they may treat format 6 as corrupt and serve without limits. Before
enabling policies, stop and upgrade every node, including offline nodes;
prevent an older binary from rejoining. Fail-closed handling of undecodable
manifests is a guarantee of rate-limit-aware binaries only.

With neither current nor pending policies, ingress admission returns immediately:
it does not change response ordering, capture a front snapshot, or increment rate
counters. Bucket read errors do not invent pending policies; corrupt manifests do.
The metrics endpoint still registers `wasmkeep_rate_limit_requests_total` and
`wasmkeep_rate_limit_active_keys` even when no policy exists, plus the `ingress`
record label in `wasmkeep_signatures_refused_full_total`. These metrics are the
intentional exception to no-policy compatibility.

These limits control app admission. Use edge filtering and connection
limits for floods that can overwhelm the listener or the network before
app admission runs.
