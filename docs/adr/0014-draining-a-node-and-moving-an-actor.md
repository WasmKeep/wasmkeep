# 0014. Draining a node and moving an actor

- Status: accepted and implemented (2026-11-05; `actor move` built as
  [ADR 0017](0017-moving-one-actor.md)). Revised
  the same day after an adversarial
  review: peers of another tenant, clusters without a cluster port, the
  five places a node claims, the hand-over racing its own release, alarms
  and sockets during a hand-over, and the cost of the control listing.
  The first draft's "warm-up" became an alarm hand-off.
- Amended by [ADR 0022](0022-app-owned-state-and-the-one-time-migration.md)
  (0.4.0): move keys are under the app's root, `state/<t>/<a>/moves/...`
  (below, `control/move/<tenant>/...` is the layout before 0.4.0); calls
  between nodes carry the caller's app; heartbeat line 6 always names an
  app, the implicit `default/default` for `--actor` nodes.
- Amended by [ADR 0020](0020-one-lease-per-node.md) (2026-11-07): ownership is a node lease plus an
  ownership record per actor; where this record says lease, read the
  actor's hold (`LeaseView`: the hold exists and the node lease is valid).
- Amended 2026-11-07, from the drain simulation on node leases: a peer
  that accepted an actor's alarm from a hand-off looks again after 1 s
  (for 30 s, or twice ttl plus grace if longer) while the actor's record
  names another node, whatever its
  `control/` listing says. A draining node whose hold a bucket outage
  ended hands the alarm off with its release still pending; a peer that
  had not listed the drain yet left the alarm to it, and nobody fired it
  until the sweep (`a_handed_alarm_waits_for_the_senders_release`).
- Amended by [ADR 0022](0022-app-owned-state-and-the-one-time-migration.md) (planned, 0.4.0): move keys move under the app's root, and node-to-node calls carry the app.
- Date: 2026-11-05
- Milestone: [M4](../../ROADMAP.md#m4-operating-a-cluster)

## Context

An operator who replaces a machine, resizes it or adds a node has one tool
today: stop the process. A clean stop hands actors over at once, but the
node is gone meanwhile. An idle actor's alarm then waits for the sweep:
about 25 s after it is due, with the defaults ([ADR 0009](0009-alarms-in-a-cluster.md)).
There is no notion of a node that is up but should not take actors.

What the code does today, and what this ADR has to work with:

- **Routing.** A node that has no mailbox for an actor asks for its
  owner: the owner hint (1 s), then the lease. A lease that is missing,
  released (`expires_at 0`) or expired past grace counts as free. A node
  claims in `ensure_owner`, which has five callers:
  - HTTP requests and `actors.call`;
  - the upgrade probe;
  - alarm delivery;
  - `ws_open`;
  - socket events.

  `acquire` also lets a node take back its own unexpired lease. A request
  is forwarded at most once (`x-wasmkeep-forwarded`, or arrival on the
  cluster port). A receiver that finds another owner answers 503 with
  `Retry-After: 1`.
- **Tenants.** Nodes of different tenants may share a bucket prefix. A
  node serves every `/actor/…` path as its own tenant, so it must never
  be sent another tenant's actor. Today forwarding only targets the
  holder of the actor's own lease key, so that never happens.
- **Release.** Dropping an actor's state drops its `LeaseHold`, which
  spawns the release PUT. Only shutdown waits for it, through a
  node-wide counter. A failed upload abandons the actor, which also
  releases. `Mailboxes::hibernate` returns nothing to wait on, and an
  actor with queued events stays resident. The mailbox registry forgets
  an actor *before* its state, and so its lease, is dropped.
- **Last write.** Each event ends in `finish`, which awaits the upload of
  its changes before the next event or hibernation. A release follows the
  last upload, except an upload cut short by the wall-clock limit, which
  may still land later as a late commit ([ADR 0007](0007-replication-protocol.md)).
- **Alarms.** A node arms an actor's alarm only in these cases:
  - when an event on it finishes;
  - when the alarm event runs;
  - on a retry or a sweep;
  - by scanning its local databases at startup.

  Acquiring and restoring arm nothing. After a release, the old owner's
  timer still holds the alarm. When it fires, the old owner skips it if
  another node owns the actor, and otherwise claims it. A refused alarm
  event is retried only on `Capacity` or `Full`; any other error
  completes it. The alarm path (`alarm(hint)`) acquires, restores, re-arms
  a future alarm, runs a due one, and clears stale index entries.
- **Signed calls.** The only signed node-to-node call is `actors.call`.
  It is honoured on either port in ADR 0011's phase A, and only on the
  cluster port in phase B. A cluster may have no cluster port at all.
- **Heartbeat.** `nodes/<id>`, header `wasmkeep-node 2`, six lines, then
  the cluster address and pins (lines 7 and 8) only with TLS. A reader
  takes at most 8 lines and drops a malformed or empty line 7 or 8.
- **Listing.** `Directory::list` lists the prefix, then reads every
  object's body in sequence. One failed read fails the whole listing.

## Decision

### Intents are object keys in the bucket

As for deploys ([ADR 0008](0008-deploy-and-rolling-upgrades.md)), the
operator writes the bucket and nodes poll it. No admin API. The intent
is in the key; bodies are empty and never read.

```text
control/drain/<node-id>
control/move/<tenant>/<class>/<name hex>/<target node-id>
```

Not under `nodes/<id>/`: on MinIO, an object and a "folder" of the same
name conflict. Each node lists `control/` once per heartbeat period (3 s),
reading keys and their last-modified time only.

- `wasmkeep cluster drain <node-id>` writes the drain marker.
  `wasmkeep cluster undrain <node-id>` deletes it.
- `wasmkeep actor move <tenant>/<class>/<name> --to <node-id>` writes a
  move key. A new target is a new key, so deleting one move never removes
  another.
- A move key whose last-modified time (the bucket's clock) is more than 5
  minutes old is ignored, and the node that lists it deletes it.
- **A failed listing changes nothing.** A node keeps the drain state it
  last read until a successful listing shows the marker gone. A failure
  therefore never undrains every node at once.

### Who may receive an actor

An **eligible peer**, for actor A on node D, is a live node that meets
all of these:

- serves the same app as D (heartbeat line 6 equal to D's), so the same
  tenant;
- writes heartbeat line 9 (below), which shows it knows this ADR;
- is reachable from D the way `actors.call` reaches it: through its
  cluster address in phase B, through either address in phase A;
- has no drain marker in D's last listing.

Same app means the same tenant. Line 6 is always `tenant/app`: since
[ADR 0022](0022-app-owned-state-and-the-one-time-migration.md), nodes
serving classes given on the command line (`--actor`) write their
implicit app, `default/default`, where they wrote `tenant/` before. For them it does not guarantee the
same classes; a peer without the class answers 404, as any node would.

A peer that fails to take an event sent to it is **suspect** for a
heartbeat horizon (10 s by default) and not chosen meanwhile. Failing
means a 5xx answer, a refused connection, a failed call, or a 503 from a
peer that is draining itself. By the time it is chosen again, a dead node
has left the peer list and a draining one shows it on line 9. The event
that failed gets its error; the next ones go to another peer, or are
claimed here. A chosen peer is never recorded as the actor's owner,
because it may not become one.

The **chosen peer** for A is the eligible peer with the highest
rendezvous hash of (A's address, peer id). Every draining node picks
the same peer for the same actor. The choice spreads actors evenly, and
it needs no load figure, so it cannot herd every hand-over onto a
newly added node.

### Heartbeat line 9

Lines 7 and 8 keep their meaning and are written empty without TLS, so
line 9 is line 9 on every node. Readers today drop an empty line 7 or 8
and read at most 8 lines, so older nodes see no change. The header stays
`wasmkeep-node 2`.

```text
line 9:  serving | draining | drained | stuck
```

Nodes learn about drains from `control/`, which is fresher; line 9 is
for eligibility and `cluster status`. `stuck` means draining with actors
left and no eligible peer.

### A draining node stops claiming

The check sits in `ensure_owner`, so it covers all five callers. On a
draining node, `ensure_owner` never takes a lease that is free, and
never takes back its own lease once that is no longer held in memory.
It returns a new outcome, `Elsewhere(peer)`, which each caller handles
as below:

| Caller | On `Elsewhere(peer)` |
| --- | --- |
| HTTP request, not yet forwarded | forward to the peer; the peer claims |
| HTTP request already forwarded, or arrived on the cluster port | 503 with `Retry-After: 1` (the sender's hint lasts at most 1 s) |
| `actors.call` from a local guest | call the peer instead |
| Upgrade probe | `forward_upgrade` to the peer, or 503 with `Retry-After: 1` if already forwarded |
| Alarm delivery | alarm hand-off to the peer (below); the timer retries if it fails |
| `ws_open`, socket events | none in practice: sockets are on the owner and close before a hand-over |

With no eligible peer, a draining node claims as today and reports
`stuck`. Draining must never make an actor unreachable.

Other nodes need no change. They forward to the draining node only for
actors it still owns. An older node may also forward an actor whose
owner hint has gone stale; it gets the 503 above, for at most 1 s.

### Handing over one actor

A draining node hands over its resident actors, up to 16 at a time
(a constant, not a flag), once per heartbeat period while it drains. For each actor:

1. **Close its sockets** with 1012 ("actor moving; reconnect") and wait,
   up to 5 s, until their `on-websocket-close` events have run, each with
   its upload. They run on this node, while it still holds the lease. A
   close event queued after the release would find no lease and never
   run. Upgrades arriving meanwhile go through `ensure_owner`, as on any
   node. The job checks again when it runs: a socket still open or
   closing (accepted since, or a client slow to answer the close) makes
   it keep the lease, close the socket and leave the actor for the next
   pass.
2. **Queue a hand-over job in the actor's own mailbox.** It runs after
   every event already queued, each of which finished with its upload.
   It checks again that the node still drains and has a peer (a job that
   runs late does nothing), reads the committed alarm, closes the
   instance and the database, and releases the lease, waiting until the
   release is written. `release` reports one of three outcomes:
   - `Released`;
   - `Superseded`: the lease was no longer this grant's (lost, taken
     over, or already released);
   - `Failed`: the bucket kept refusing, retried every 250 ms up to the
     lease's TTL. The lease then expires on its own (ttl + grace), and
     requests meanwhile get 503.

   A lease that had already lapsed in memory is dropped (released as on
   hibernation), with nothing to hand over.
3. **Drop the actor from memory.** Events queued behind the job, and any
   later ones, find no lease. `ensure_owner` sends them to the chosen
   peer (a request is forwarded, a call goes there, a socket closes),
   which claims. Nothing on this node can take the lease back: a
   draining node never acquires a lease it does not hold in memory.
4. **Hand off its alarm**, if it has one. The alarm goes to the chosen
   peer on a signed node call, the same call `actors.call` makes. It
   carries a marker header (`x-wasmkeep-alarm-handoff`), and the due time
   is in the signed body, so it cannot be altered in transit. It follows
   `actors.call`'s port rule (ADR 0011), signature and single use. The
   peer arms its timer with the time unless an earlier one is
   registered, and from there the existing alarm path does all the work:
   acquire, restore, run the alarm if still committed, clear stale index
   entries. The peer also forgets any owner it had learned for the
   actor. When the peer confirms, this node forgets the alarm, unless a
   newer time was registered meanwhile. If the hand-off fails, the alarm
   stays registered here and is retried each pass. If the release failed,
   the alarm stays here until the lease expires. An
   actor with no alarm needs no hand-off: its next request restores it
   wherever it lands.

If the events queued before the job have not finished within 30 s (a
long event, or a hot actor), the drain moves on and tries that actor
again on a later pass. The job, still queued, runs when its turn comes.

**At the peer, the alarm hand-off:**

- is honoured only on a verified signed call whose signature covers the
  marker, so it cannot be added or stripped in transit. The marker is
  stripped from every client request and every guest's call, so neither
  can arm another actor's alarm;
- answers 403 for another tenant (the body names it), and 404 for a
  class the node does not serve, like any call;
- answers 409 while the node is draining itself;
- answers 200 once the alarm is armed. It neither acquires the lease
  nor restores the database.

An older peer is never eligible, and treats the call as an ordinary one.

**When an alarm fires on a draining node.** It goes through
`ensure_owner`, which hands the alarm off to the chosen peer. On a
refusal or an error, the node suspects that peer and tries again in
500 ms, with another peer or, with none left, by claiming the actor and
running the alarm itself. As a last resort, the sweep still delivers the
alarm (ADR 0009).

**Why releasing first is safe:** release then acquire is what
hibernation and shutdown already do, so no lease or replication rule
changes. The hand-off only arms a timer: the peer acquires through the
alarm path, which runs every check it runs today.

The first version of this section described a *leaving* state in the
mailbox registry and a `/_wasmkeep/alarm` path. Both were replaced while
building it:
- **The job.** Running in the actor's mailbox, it orders itself after
  the queued events without a new registry state, and it removes the
  race between dropping the state and the release starting.
- **The marker.** Carried on the signed call, it reuses that call's
  signing, replay protection and port rule, instead of adding a new path
  to both listeners.

### The alarm sweep

A node that drains leaves the sweep's shard list, on every node that
reads its drain marker or its line 9. The others divide its share of
overdue entries between them. Its own sweep still delivers entries that
are anyone's (3x overdue), through the alarm hand-off. If no other node
is left, it keeps its share, so no entry is nobody's.

The shard list also leaves out live nodes of another app on the same
bucket prefix (another tenant). They never deliver this app's entries,
so their share used to wait until it was anyone's. An older node that
publishes no app line still counts.

### Moving one actor

Not built by this ADR. The owner can release an actor, but nothing then makes
the named target take it before the next request claims it wherever that
request lands. Moving to a target needs the target to acquire first (a
warm-up), which this ADR set aside. Under this ADR, `wasmkeep actor move`
only wrote its request, and nodes read it and did nothing with it. Built
as [ADR 0017](0017-moving-one-actor.md).

### When a drain ends

When a draining node holds no actors (routing stubs do not count), it
writes `drained` on line 9. It keeps running until the operator stops it.
It still answers requests by forwarding them, so a load balancer can be
updated afterwards. `undrain` returns it to `serving`.

## Alternatives

- **Drain by stopping the process** (today). It is simple, but it moves
  every actor at once, and idle actors' alarms wait for the sweep.
- **A warm-up that acquires and restores on the peer** (this ADR's first
  draft). It arms an alarm without running the alarm path, so a stale
  index entry is never cleared: the sweep re-sends it every 10 s. It also
  adds a lease protocol (200/409) for no gain in correctness. Rejected
  for the alarm hand-off.
- **Choosing the peer with the fewest resident actors.** Heartbeats are
  up to 3 s old, so every hand-over in that window goes to the same
  peer: a newly added node takes everything and hits `max_resident`.
  Rejected for rendezvous hashing.
- **An admin HTTP API on each node.** It needs its own authentication,
  and it reaches one node, whereas the bucket reaches all of them.
  Rejected for now, like ADR 0008's "push to nodes".
- **A hash-preferred home node per actor** (an earlier design sketch). Not
  needed: hibernation already spreads load. Revisit if
  `wasmkeep_forwards_total` shows forwarding dominates.
- **Pinning an actor to a node.** It would need lease semantics beyond
  "first to claim".

## Consequences

- No ownership rule changes. Drain and move reuse release-then-acquire.
  The risk is in routing and admission, so most of the tests below are
  deterministic server tests with injected faults.
- **New surface:**
  - a marker header on signed node calls (`x-wasmkeep-alarm-handoff`),
    covered by the signature, with the tenant and due time in the signed
    body. It is stripped from client requests and guests' calls;
  - an `ensure_owner` outcome, handled by its callers;
  - a hand-over job run in the actor's mailbox, and
    `Mailboxes::submit_if_resident`, which never creates a mailbox;
  - a release that can be awaited.
- Each node lists `control/` keys every 3 s: one more LIST per node per
  period, with no object reads.
- **Unavailability during a hand-over:**
  - requests queued behind the hand-over job wait for it (the release,
    milliseconds), then go to the peer;
  - the drain waits 30 s for the events queued before the job, then
    moves on and tries again on a later pass;
  - an actor with a socket that will not close stays until it does: a
    socket's close event must run with the lease;
  - a forward that times out still returns 504, as today.
- **Alarm timing:**
  - a peer that sees a draining node as an actor's owner looks again
    after 1 s rather than dropping the alarm, since the owner may be
    handing it over;
  - so does a peer that accepted the actor's alarm from a hand-off in
    the last 30 s (or twice ttl plus grace if longer), whoever the
    record names (amended 2026-11-07): only a
    draining node hands one off, and its record names it until its
    release lands;
  - a hand-off whose release failed keeps the alarm on the draining
    node until the release, retried in the background, lands (ADR 0020;
    before it, until the lease expired);
  - a node is not `drained` while an alarm it failed to hand off is
    still with it, nor while a release is pending (ADR 0020).
- **Mixed versions:**
  - older nodes are never eligible;
  - an older node's stale hint costs a 503 for at most 1 s;
  - a peer that started draining after this node's last `control/`
    listing answers one 503, then becomes suspect;
  - a cluster made only of older peers drains as `stuck`.

**Found while reviewing, left as is:**

- A client can send `x-wasmkeep-forwarded` itself on the public port. Only
  that client's own request is affected (it can get a 503).

## Tests

- **Server tests** (`crates/wasmkeep-server/tests/cluster/`, with fault
  points):
  - a drain empties a node; no committed write is lost; requests see only
    503 with `Retry-After` or success;
  - an alarm due inside a hand-over that is slowed by a fault point fires
    on another node within 1 s of its time;
  - the peer is down during the hand-off: the alarm fires from D's timer
    retry, not the sweep;
  - a cluster without a cluster port, and one in phase A, both drain;
  - a node of another tenant on the prefix is never chosen, and refuses a
    hand-off;
  - `actors.call` and an upgrade on a draining node go to the peer;
  - an event racing the state's drop never takes the lease back;
  - an older peer (no line 9) is never chosen;
  - a failed `control/` listing keeps a node draining;
  - sockets on a moved actor close with 1012, and their close events run;
  - `actor move` moves a resident actor to the named node;
  - a node alone in its cluster, drained, reports `stuck` and still
    serves its actors.
- **Unit tests:**
  - heartbeat lines 7 to 9 read by old and new parsers;
  - rendezvous choice and eligibility;
  - move keys expiring by last-modified time;
  - `release` outcomes.
- **`sim/` case** (the project rule: changes to leases and failover
  come with a simulation case). The lease and
  replication simulations already cover release then acquire by any
  node, so a hand-over adds nothing there. The new case goes in
  `madsim_alarm_sweep.rs`, with a timer model if it lacks one:
  - a draining owner hands its alarms off to a peer that may be down or
    refusing;
  - the invariant: every committed due alarm is delivered within the
    hand-off bound when an eligible peer is up, and within the sweep's
    bound otherwise;
  - variants that must be caught: completing a refused alarm instead of
    retrying it, and a draining node that keeps its sweep shard while not
    delivering it.

## Plan

| Slice | Size | State |
| --- | --- | --- |
| Heartbeat line 9; `cluster status` shows it | S | done |
| `control/` key listing; `cluster drain`/`undrain`, `actor move` CLI | S | done |
| Eligibility and rendezvous choice | S | done |
| `ensure_owner` returns `Elsewhere(peer)`; the five callers handle it | M | done |
| Awaited release with outcomes; hand-over job in the mailbox | M | done |
| Alarm hand-off on a signed call; paced hand-over; sockets first | M | done |
| Sim case, adversarial review of the code | M | done |
| Docs: `docs/cluster.md`, `docs/deploy/three-nodes.md` | S | done |
| Sweep shards skip draining nodes and other apps | S | done |
| `actor move` (needs the target to acquire first) | M | done in [ADR 0017](0017-moving-one-actor.md) |
