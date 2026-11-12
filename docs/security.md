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
  client, 1,000 sockets per actor and 50,000 per node, idle clients pinged
  then closed.
  A socket is invisible to the actor's other events until
  `on-websocket-open` accepts it. Close reasons never carry host errors.
- `x-wasmkeep-*` headers from clients are stripped or ignored: only the host
  sets who is calling.

**Who may create an actor** (ADR 0010): per class, `open` (default),
`existing` (outside requests reach only existing actors; nothing is created
for unknown names), `existing:NAME,...`, or `internal` (actors only).

**The guest sandbox**
- Capabilities: only what the `wasmkeep:actor` world imports. Filesystem and
  sockets interfaces that toolchains import anyway are inert (no
  directories, every socket and lookup refused).
- CPU: 50 ms of guest time per event, and 2 s of SQL CPU per event
  (SQLite runs in the host, so it has its own budget, measured with the
  thread's CPU clock and enforced inside a statement); wall clock 30 s per
  event (also for a stateless component's request, response included).
- Memory: 128 MiB per linear memory, 2 memories and 32 core instances per
  component; resident actors capped at 90% of the instance pool, the least
  recently active idle one hibernating to make room.
- Outbound HTTP: public addresses only (the host resolves, checks and
  connects to the checked address; TLS verified against the name);
  `--allow-outbound` and `--outbound-ca` open specific internal services.
  The ports browsers block are refused too (the Fetch standard's list:
  SMTP 25, 465 and 587, FTP, SSH, IRC and other non-HTTP services), since
  a crafted HTTP request can drive some of those servers;
  `--allow-outbound-port` opens one. 16 requests in flight per actor, 256
  per node.
- What a guest prints reaches the node's log within a line rate per
  instance and per node (`--guest-log-rate`; past it, lines are dropped
  and counted), cut at 4 KB a line, with control characters escaped: a
  guest cannot flood the log, forge entries of its own or send terminal
  escapes. What it prints is its choice, secrets included, so the node's
  log is as sensitive as the actors make it.

**SQLite sandbox** (`crates/wasmkeep-storage/src/db.rs`)
- No `ATTACH`, `VACUUM`, extensions, `fts3_tokenizer`, or temporary
  tables, views, triggers or indexes (ordinary ones are allowed, but not on
  host tables); virtual tables limited to fts3/fts4/fts5/rtree;
  pragmas from an allow-list; defensive mode; `trusted_schema=OFF`.
- Host tables (`_wasmkeep_*`) invisible to guests.
- 1 GB per database; 16 MB per value and per query result; a statement
  stopped at the event's wall clock.
- 256 MB of write-ahead log between two captures: past it, statements that
  write fail until the event ends (reads, commit and rollback still run).
  Capture reads the log frame by frame and holds only the newest version of
  each changed page, so rewriting the same rows costs no memory (a
  transaction touching many distinct pages still holds them all).

**Between nodes** (`crates/wasmkeep-server/src/cluster.rs`)
- Calls carry an HMAC-SHA256 signature (secret from the bucket, never sent)
  over the target node, method, path, the body's SHA-256, caller, chain,
  time and a nonce unique to each send; valid for a minute, accepted once
  (the replay record expires in order and holds up to 500,000 signatures,
  several thousand calls a second into one node). The receiver checks the body
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
  there is spent, so a plaintext copy can never be replayed onto the
  cluster port; peers
  without a cluster listener are not used (their actors get a 503 from
  this node), and TLS peers are never reached in plaintext. The forwarded
  header gives no privilege on any port: it only stops a request being
  forwarded again.
- One app per bucket prefix: a node refuses to start next to another app of
  its tenant.

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

`--env CLASS=FILE` (`NAME=value` files, readable only by the server's user).
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

## Known gaps

- Hostile multi-tenancy: every actor of a node shares one process (see the note above).
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
