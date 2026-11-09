# 0011. Mutual TLS between nodes, with certificates pinned in heartbeats

- Status: accepted; all steps done, step 11 (refuse by default) on
  2026-11-06, before the first release (see "Plan"). Decided in order to close
  the known security gaps between nodes; the open questions below were answered with the
  recommended option and can be revisited (question 4 is still open,
  see below).
- Date: 2026-11-05
- Milestone: [M3](../../ROADMAP.md#-m3-first-public-release)

## Context

Nodes talk to each other over plain HTTP on the same port clients use:
forwarded requests, relayed WebSockets, and `actors.call` between actors on
different nodes. Calls carry an HMAC-SHA256 signature over their headers
and body (`x-wasmkeep-internal: v4 ...` since the call nonce; v3 when this
was written, with the cluster secret from the
bucket), so nobody can pass for a node or change a call without the
secret. But anyone on the network between nodes can read every request and
response, and change responses. The security model asks operators to keep
node traffic on a private network; that is the last open gap between nodes
(docs/security.md, "Known gaps").

The original design planned mTLS with certificates issued at
`wasmkeep cluster init`: a cluster CA. Three designs were drawn up independently and judged against
the code (a CA in the bucket, operator-provided PKI, and certificates pinned
in heartbeats).

## Decision

**Each node is its own trust anchor, published through the bucket.**

- At start, a node generates an ECDSA P-256 key and a self-signed
  certificate in memory (CN = node id). The key never touches disk or the
  bucket; a restart is a key rotation.
- Its heartbeat (`nodes/<id>`, already the only place a node speaks for
  itself) gains two optional lines: the `https://` address of its cluster
  listener, and the SHA-256 fingerprints of its certificate (one, or two
  during a change). Older nodes ignore lines after the app, so a mixed
  cluster keeps working.
- A peer is trusted if and only if its certificate's fingerprint is in a
  live heartbeat under its own key. The bucket stays the root of trust, as
  it is for the cluster secret: whoever can write the bucket controls the
  cluster. Unlike a CA key in the bucket, nothing at rest lets a reader of
  the bucket (or of a backup of it) mint a node identity.
- Revocation is the heartbeat expiring (or being deleted): a removed node's
  pin stops being trusted within the heartbeat horizon, and every request
  re-checks the peer's pin, so a kept-alive connection is refused too.

**A separate cluster listener.** `--cluster-listen ADDR` (with
`--cluster-advertise https://HOST:PORT`) serves TLS 1.3 only, ALPN
`http/1.1`, with a mandatory pinned client certificate; it serves actor
paths and the health and readiness checks, nothing else. The public
`--listen` port is unchanged. Metrics stay on `--metrics-listen`.

**TLS session resumption is off on both sides.** A resumed session carries
the client certificate from the original handshake without running the
verifier again, which would let a removed node, or one whose pin changed,
back in. Connections are pooled and kept alive, so full handshakes are
rare.

**Transport identity is not caller identity.** The HMAC signature (v3, now v4 with a nonce) stays
the only way a request counts as a node's call (who is calling, the chain).
TLS adds confidentiality and integrity in both directions and
authenticates the hop. Requests forwarded for outside clients stay
external, with the access check repeated on the owner. A request that
arrived on the cluster port is never forwarded again.

**Rollout without a flag day.**

- Phase A: nodes restarted with `--cluster-listen` publish their TLS
  address and use mTLS to peers that do the same; plain HTTP is still used
  to (and accepted from) peers without it.
- Phase B: `--cluster-plaintext refuse`: signatures arriving on the public
  port are ignored (such calls are treated as coming from outside), and
  peers without TLS are not used.
- Phase C (a later release): `refuse` becomes the default when
  `--cluster-listen` is set. (Done 2026-11-06 in step 11, before any
  release rather than in a later one; a node with `--node-id` and no
  `--cluster-listen` refuses to start unless given
  `--cluster-plaintext accept`.)
- No plaintext fallback after a TLS failure: a peer that publishes a TLS
  address is reached over TLS or not at all (503, the caller retries).

**rustls with the `ring` provider, named explicitly.** Both ring and
aws-lc-rs are compiled into the dependency tree, so every config is built
with `builder_with_provider`; `clippy.toml` bans every rustls entry point
that would fall back to the process default (which panics with two
providers compiled in).

## Consequences

- No `cluster init`, no CA ceremony, no certificate files: two flags.
- A new node's pin reaches its peers with the next heartbeat listing (a
  few seconds). Until then peers refuse its handshakes; it reports not
  ready (`/_wasmkeep/ready`) until its identity has been published for a
  heartbeat period, and a refused unknown pin makes the verifying node list
  heartbeats again sooner.
- New dependencies: `rcgen` (in-memory key and certificate generation) and
  `tower-service` (the connector trait hyper-util's client requires; already
  in the lock through hyper-util).
- `sim/` needs no change: it models leases and replication, not HTTP, and
  heartbeats stay advisory (they drive placement, never ownership).
  Cross-node behaviour is tested in `crates/wasmkeep-server/tests/cluster/`
  and a new `tests/cluster_tls.rs`.
- A guest allowed onto the node subnet with `--allow-outbound` can still
  reach the public port of other nodes, never the cluster port (it has no
  pinned certificate).

## Open questions, answered for now

1. Replace the original design's certificates issued at `wasmkeep cluster init`? Yes:
   pins need no CA and no init step.
2. Is "can read the bucket but not write it" a role worth protecting? Yes,
   which favours pins over a CA key at rest.
3. Operator-supplied PKI (`--cluster-cert`, `--cluster-key`,
   `--cluster-ca`)? Deferred; it would be a second trust root.
4. Should refuse mode stop publishing the plain address? Still open.
   Phase C (step 11) left the heartbeat unchanged, so the plain address
   is still published, and peers cannot see a node's mode (step 8).

## Plan

Each step lands on its own, with tests that fail before it; steps that
change who is trusted get an adversarial review.

1. This ADR.
2. Heartbeat lines for the cluster address and pins (wasmkeep-cluster), no
   behaviour change.
3. A connector for the three node clients, plain only, no behaviour change.
4. Identity and pin verifiers, unit-tested, not wired.
5. The cluster listener: one serve function owning both accept loops and
   one shutdown; handshake timeout and connection cap. (Done: not wired
   yet; `http::serve_with_cluster`, `ClusterPort`.)
6. Wiring in accept mode, with the whole cross-node suite running over TLS.
   (Done: `ClusterConfig::cluster_tls` and `serve_with_cluster_listener`,
   library only until step 7. Differences from the text above: a peer's
   pins are looked up by the `host:port` of the cluster address it
   published, at connect time; when two live heartbeats name the same
   address, the newer one is trusted there (a node restarted under another
   id), and neither on a tie. Every pin is trusted until its heartbeat's
   write time plus the liveness horizon (ttl and allowed skew), checked
   against the clock, so trust lapses on time even if the bucket can no
   longer be listed. The cluster port checks the client's pin on every
   request, and cuts an upgraded WebSocket once that pin stops being live
   (checked every 500 ms); a relaying node cuts its side the same way when
   the owner's pin stops being trusted at its address. A node's pooled
   connections are dropped (the clients rebuilt) whenever an address it
   knew changes pins or disappears, and nothing is sent to an address
   whose heartbeat expired, so a pooled connection is only reused to the
   same live identity. A certificate matching no pin (on either side)
   lists the heartbeats again sooner, at most once per heartbeat period; a
   refused or timed-out connection does not. So does a connection whose
   address's heartbeat, as last listed, expired: it waits up to 3 s for a
   listing that shows a newer one before it is refused (a node whose own
   listing is late under load would otherwise refuse a live peer; an
   expired claim is never used). Readiness waits a period and
   a half after the identity was published, counted again after a
   heartbeat outage. `serve_with` refuses a node configured for TLS, so a
   published cluster address is always served. The cross-node suite runs
   a second time, from `tests/cluster_over_tls.rs`, with every node
   publishing a plain address nobody listens on.)
7. CLI flags. (Done: `wasmkeep serve --cluster-listen ADDR
   --cluster-advertise https://HOST:PORT`, both or neither, only with
   `--node-id` and actors; both ports served with one shutdown. The
   cluster port's connection cap is counted in the open-files budget
   before the public port and the resident actors: 1,024, or an eighth of
   the usable descriptors when the limit is low (`fds::reserve_cluster_port`,
   `ServeLimits::cluster_connections`). Without a listener a node still
   warns about a public `--advertise`; with one it does not. Tested with
   two real processes whose plain addresses lead nowhere,
   `crates/wasmkeep-cli/tests/cluster_tls.rs`, run in CI's S3 job.)
8. Refuse mode. (Done: `wasmkeep serve --cluster-plaintext accept|refuse`,
   `ClusterConfig::cluster_plaintext` (`ClusterPlaintext::Accept` by
   default); `refuse` needs a cluster listener (the CLI and
   `ActorService::new` refuse it without one). In refuse mode a
   signature is honoured only on a request that arrived on the cluster
   port; on the public port a valid one is spent (so that copy can never
   be replayed onto the cluster port) and the request is an outside
   client's: access rules apply, the caller and chain headers are
   dropped, and an `internal` class answers 403. The forwarded header grants nothing on either port and never
   did: it only stops the request being forwarded again (a 503 if this
   node does not own the actor), so a plain node forwarding to a refusing
   node is served as a client would be. A peer without a cluster address
   is never used: requests and WebSocket relays to its actors get a 503
   and `actors.call` fails, and its id is logged at most once a minute;
   a plain base URL is refused at the last check before sending and by
   the connector itself. A relayed WebSocket upgrade carries the client's
   headers minus hop-by-hop ones and every header only a node or the host
   sets. Refused signatures are logged once a minute per reason. A TLS
   peer is still never reached in plaintext. The node logs at start that
   it refuses plain HTTP; the heartbeat is unchanged, so peers cannot see
   the mode (whether to stop publishing the plain address is open
   question 4). Refusing nodes still serve outside clients on the public
   port, including requests a plain node forwards there.)
9. Revocation tests. (Done: `crates/wasmkeep-server/tests/revocation.rs`,
   two real nodes over the shared bucket, everything between them over
   TLS. Each test warms up every kind of connection both ways, makes `a`
   stop trusting `b`, and times each refusal from the event: `b`'s
   heartbeat deleted, `b` unable to write its heartbeat, `b`'s heartbeat
   naming a new key while the old process keeps its connections, or `a`
   unable to list the heartbeats. In each, `a` stops sending to `b` (its
   pooled connection unused), `b`'s next request on its kept-alive
   connection to `a`'s cluster port gets a 503 and its new handshakes are
   refused, a WebSocket relayed either way is cut on `a`'s side, an
   `actors.call` in flight either way does not deliver its answer, and
   the next one fails. Disabling any one mechanism fails at least one
   test. Measured with a 100 ms heartbeat (horizon 400 ms: ttl of three
   periods plus 100 ms of skew): requests and handshakes are refused 30
   to 110 ms after a deletion or a new key (the next listing), 355 to 400
   ms after the last heartbeat write, and 255 to 370 ms after listings
   start failing (the horizon from the last listing); WebSockets are cut
   within the next 500 ms check after that, 355 to 400 ms after the
   event in these runs. Changes: an answer from a peer, or to a peer on
   the cluster port, is not delivered once that peer's pin stopped being
   live: checked when the answer is ready, before every frame of its
   body, and every 500 ms while the body waits (a forwarded request gets
   a 503, `actors.call` fails; on the cluster port a 503 that keeps the
   connection, whose next request is refused). A node no longer drops its
   pools when its own heartbeat lapses or its own pin changes (it never
   connects to its own address), so a node cut off from the bucket keeps
   its connections to its peers, which refuse them on their side. Still
   open: a request admitted before the pin lapsed runs to the end (only
   its answer is withheld), and its body still being uploaded is not cut;
   revocation of WebSockets and waiting bodies is polled (500 ms).)
10. Docs and benchmarks. (Done: `crates/wasmkeep-server/benches/cluster.rs`,
   two pairs of nodes over loopback, one plain and one behind cluster
   listeners whose plain addresses lead nowhere; results in
   `bench/results/2026-11-05-d8e9ab7.csv` (and an earlier run,
   `2026-11-05-b07a9d7.csv`) and
   [benchmarks](../benchmarks.md#between-nodes-plain-http-against-mutual-tls).
   On a pooled connection, TLS adds 9 to 12 µs to a forwarded request
   (106.3 to 115.1 µs in the later run; 63.2 µs sent to the owner
   directly) and 15 to 21 µs to an `actors.call` between nodes (133.7 to
   155.1 µs), one request at a time. A full handshake is not measured: connections are pooled, and
   rebuilt only when a pin changes or lapses (forwarding
   drops hop-by-hop headers, so a client's `Connection: close` no longer
   closes the connection between nodes). Operator docs were written with each step (`docs/cluster.md`,
   `docs/security.md`, `docs/deploy/three-nodes.md`, `docs/monitoring.md`);
   `docs/cluster.md` now links the cost and the measured revocation
   times.)
11. `refuse` by default. (Done 2026-11-06, before any release, so no
   running cluster changes behaviour on an upgrade: `wasmkeep serve`
   defaults to `--cluster-plaintext refuse`, and a node started with
   `--node-id` and without `--cluster-listen` refuses to start, naming
   both ways out (a cluster listener, or `--cluster-plaintext accept`).
   A one-node cluster gets a loopback listener
   ([single node](../deploy/single-node.md)). The library's
   `ClusterConfig` keeps `Accept` as its default, for embedders and the
   tests; the CLI is what operators run.)
