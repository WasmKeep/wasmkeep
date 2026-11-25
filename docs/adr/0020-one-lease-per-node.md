# 0020. One lease per node, not one per actor

- Status: accepted; implemented; measured by the soak after
  (Verification, item 4). Proposed 2026-11-07. Revised after several
  rounds of adversarial review, each narrower than the last; the final
  check of the changed parts, for safety and liveness, found a race
  between two acquisitions and the resolver (fixed by a lock per actor)
  and four ways a record could stay held. The simulation tested the
  protocol mechanically before any code. It lands before the first
  public release, since changing leases requires stopping every node at
  once (section 6).
- Amended by [ADR 0022](0022-app-owned-state-and-the-one-time-migration.md) (planned, 0.4.0): ownership records and lease markers move under the app's root; `nodes/` stays shared.
- Amended by [ADR 0024](0024-socket-home.md) (planned, 0.4.0): a hibernation with sockets open keeps the record (parked) instead of releasing it; every wake still acquires at a new generation.
- Amended 2026-11-14, from the socket home simulation (ADR 0024,
  Verification): the resolver of an unresolved acquire keeps the entry
  while the record is still the one the write was conditioned on, for a
  bounded wait (section 3). It ended the entry on its first read of an
  unchanged record, so a write landing after that read left the record
  naming this node with nothing here to release it: harmless while the
  node served the actor (its next event took it back), but a drain or a
  move of that actor waited until the node stopped.
- Amended 2026-11-14, from a revocation test that failed now and then:
  a hold its actor drops after the node's lease ran out, before
  the lease task saw the lapse, is released at once, and that release can
  land while the lease writes do not (section 1, "A release before the
  lapse is seen"). This was the behaviour since the first release; the
  text said every such hold went into the pending releases. It is safe
  by ordering, not by clocks, and is now counted
  (`wasmkeep_releases_after_lapse_total`: every release that landed
  while the node's lease was not valid, logged once per lapse).
- Amended 2026-11-14 (0.4.2): a node lists `nodes/` every ten heartbeat
  periods (30 s) and, in between, reads the object of each node it
  already knows, one GET each: the same bodies, so the fence watch and the
  failover timings are unchanged for known nodes. A node that starts says
  hello to its peers so they list at once ([ADR 0011](0011-node-to-node-mtls.md),
  amendment of the same day). See "Amendment of 2026-11-14: fewer
  listings of nodes/" at the end.
- Amended 2026-11-20: a slow bucket. A round reads the heartbeats at
  once, each within a read attempt; a read that fails leaves its node as
  last read, trusted until its own horizon; a peer whose trust would end
  before the next round lands is read early; the intents are read on a
  task of their own; a heartbeat dated ahead counts from its first read.
  The horizon (10 s) is unchanged. Listing `nodes/` moved to a task of its
  own, for discovery only; rounds no longer read stopped (released or
  fenced) nodes; a hello reads its sender by key; `cluster forget` may
  delete a released lease no record names. See "Amendment of 2026-11-20:
  a slow bucket" at the end.
- Amended 2026-11-23: the running twin check does not take a copy of the
  node lease a new process's first write replaced, read back after that
  write (by a sibling app of the process, or from a replica behind), for a
  twin; a hello goes to the peers of its own app only. See "Amendment of 2026-11-23: a predecessor's body read
  back" at the end.
- Amended 2026-11-24: lease reads. A node lease read to judge an
  ownership record free is never older than a body of that lease the
  process has already read; an older one is read again, and never frees
  the record. A fence an operator created over a deleted object is newer
  than every named body dated less than twice the skew after it, and a
  start over one writes no earlier than that; a read finding a lease
  absent restarts the unpin window. Also defines the app's
  upgrade floor record, read at start. See "Amendment of 2026-11-24: lease
  reads, and the upgrade floor" at the end.
- Amended 2026-11-24 (0.8.1): availability. The failover wait ignores a
  body kept after a failed read or older than one the process read; a
  round has at most 32 reads in flight; a node found gone leaves the live
  senders at once and is forgotten once no earlier read can land; a
  runtime joining a process that holds heartbeats reads them by key
  first; over TLS, the name on an unknown certificate is read by key; the
  hellos wait for a live peer of this app. No lease rule changes. See
  "Amendment of 2026-11-24: availability" at the end.
- Amended by [ADR 0034](0034-batched-plain-writes.md) (proposed 2026-11-15; if accepted): a hold of a batched actor ends only through a barrier, dropped at once (lapse, lost lease, failure) or flushed first (hibernation, park, drain, move, clean stop); a clean stop flushes within its limit and counts what it drops (this record's sections 3 and 4; that record's sections 6 and 10).
- Date: 2026-11-07
- Milestone: [M3](../../ROADMAP.md#m3-first-public-release)
- Amends: [ADR 0007](0007-replication-protocol.md),
  [0009](0009-alarms-in-a-cluster.md),
  [0012](0012-bucket-garbage-collection.md),
  [0014](0014-draining-a-node-and-moving-an-actor.md) and
  [0017](0017-moving-one-actor.md): ownership is a node lease plus an
  ownership record per actor.
- Amended by [ADR 0035](0035-a-recorded-head-on-hand-over.md) (2026-11-17): acknowledgement and ending share the hold cell's lock; ended holds register their frozen head locally before release, and a same-process re-acquire carries it; ownership records have a version 2 (`wasmkeep-owner 2`), gated per app by `formats/<tenant>/<app>/recorded-head` and the capability word `recorded-head`, read by every reader beside version 1.

## Context

Every actor in memory holds a lease, `leases/<tenant>/<class>/<name>`, and
renews it with a conditional PUT a third of the ttl (3.3 s) after its
previous renewal returned (`renew_loop`, `lease_hold.rs`, before this
record). In the
30-minute soak against R2
([benchmarks](../benchmarks.md#three-nodes-against-a-real-bucket)),
Cloudflare counted 47,083 PUTs for the bucket
([`…-0f2986f-soak-r2-requests.csv`](../../bench/results/2026-11-07-0f2986f-soak-r2-requests.csv)),
of which about 23,000 were renewals of 60 busy actors, more than the
21,260 writes those actors made
([`…-0f2986f-soak-r2.csv`](../../bench/results/2026-11-07-0f2986f-soak-r2.csv)).
The renewal count is derived (PUTs minus writes, snapshots, alarm entries
and heartbeats); a counter added for this record measures it
(Verification, item 1).

On R2 and S3 every PUT is billed, so a cluster's bill grows with how long
actors stay in memory, not with what they do. On any store, 1,000 busy
actors make about 300 renewals a second, which a provider may throttle,
and each renewal is one more chance of a stalled request on the path that
keeps an actor owned.

What the per-actor lease guarantees, and any replacement must keep:

- **One owner at a time** under clock skew up to `max_skew`
  (`wasmkeep_cluster::lease`, proven in `sim/tests/madsim_lease.rs`): an
  owner acts until `sent + ttl - safety` on its clock, another node takes
  over only after `sent + ttl + grace` on its own
  (`grace + safety >= 2 * max_skew`), validity comes only from writes the
  bucket acknowledged, and a delayed write cannot extend anything because
  every write is conditional.
- **A generation of its own for every hold**: every acquisition, including
  a node taking back its own lease, writes generation + 1
  (`Leases::acquire`, `lease/legacy.rs`). Segment keys (ADR 0007),
  alarm-index tags and the owner fence (ADR 0009) and garbage
  collection's "a segment this hold wrote" (ADR 0012) all rely on it.

Every node already writes a heartbeat, `nodes/<id>`, every 3 s, with the
time it was written on its own clock (`NodeInfo::written_at_ms`) and a
token random per process (`instance`). Today it is an unconditional
PUT, written after the node lists every heartbeat (`heartbeat_loop`,
`cluster/view.rs`, before this record).

## Decision

Validity moves from the actor to the node: a node acts as owner only while
its **node lease** is valid, and an actor's object becomes an **ownership
record** written only when the actor changes hands. Everything else about
holding an actor stays as it is today: a hold starts with an acquisition
(and a restore), ends with a release (hibernation, a clean stop, a drain or
a move, a failed upload), and every hold has a generation of its own.
Only renewals disappear.

### 1. The node lease

The node lease is the heartbeat object `nodes/<id>`, now written
conditionally, in a new format: header `wasmkeep-node 3`, with a fixed line
giving its **state** and the process's `instance`. The state is `live`,
`released` (a clean stop) or `fenced` (taken by another node). A
`released` or `fenced` object keeps the `written_at` of the last `live`
write it replaced, and every reader (membership, alarm shards, routing,
`cluster status`, the start check) treats it as not live, whatever its
age. Old decoders cannot read the new format, which is intended (section
6).

After the three lease lines comes the payload: id, address, version, app,
cluster address, pins, drain state, `secrets`, capability words, one per
line and always written (empty if need be). Payload line 10, added in
0.4.0, is the node's load, written once known:
`load actors=<n> sockets=<n> events_per_min=<n|-> front_per_min=<n|->`
(`-` until a minute of samples exists; unknown keys and words are
skipped, and `front_per_min`, front requests per minute, added after
0.4.0's first form of the line, reads as unknown when absent). Readers from before
it read 9 payload lines and stop, so they never see it; a missing or
malformed line 10 reads as no load. Unlike the other payload lines it
drives nothing (it is shown by `cluster status`), and the lease logic
reads only the three lease lines.

- **One cadence, its own task.** The lease write runs on a task of its
  own, every `min(heartbeat period, ttl / 3)` measured from the previous
  *send*, never waiting for the heartbeat listing, the cluster secret or
  `control/` (those move to a separate task at the heartbeat period). A
  configured heartbeat period above `ttl / 3` is refused. Each attempt has
  a timeout of a third of `ttl - safety` at most (2 s with the defaults)
  and is retried within the period.
- **Validity** is `written_at + ttl - safety` on the node's own clock,
  where `written_at` is the send time stored in the last write the bucket
  acknowledged. Readiness follows it: a node is ready only while its node
  lease is valid (with the existing margin), not while its last heartbeat
  is younger than three periods.
- **The first write** after the start check is `If-None-Match: *` if the
  check found no object, and otherwise `If-Match` on the version the
  check read last. If that write fails, the node goes back to the start
  check; it does not take a new instance. The start check waits ttl
  plus grace in every case, including when no object exists (today it
  returns at once then), so a process whose object was deleted while it
  still ran is always given time to show itself. A peer's `fenced` or
  `released` write naming the instance the check first read is not a twin:
  it proves that process lapsed once the object's kept `written_at + ttl +
  grace` has passed on this node's clock (the same bound as section 2,
  since a fence may be forced), and the check goes on then, without the
  rest of its wait. A first write whose reply was lost follows the re-read
  rule below before going back to the check.
- **Every later write** is `If-Match` on the version of the last
  acknowledged write.
- **A failed condition is not proof of a fence.** A write whose reply was
  lost may have landed, and its retry then fails the condition. On a
  conflict the node reads `nodes/<id>` once: if the object is `live` and
  names this process's instance, it is this node's own write; the node
  adopts its version (the read's token: R2's differs from the write's) and
  its stored `written_at`, and goes on. Otherwise (fenced, released,
  another instance, or absent) the node lease is lost: the node drops
  every hold at once, without releasing anything, as for a twin,
  and `wasmkeep serve` exits at once with a non-zero status: no connection
  drain, no releases, no `released` write (the fence, or the other
  instance, already frees the records). A supervisor restarts it (systemd
  `Restart=on-failure`, as the deployment guides set); the new process
  goes through the start check.
- **A lease that only ran out** (the bucket unreachable for longer than
  `ttl - safety`, no conflict seen) is not lost: the node drops every hold
  locally at once, putting each into the set of pending releases (section
  3) with its generation and acquire version, reports not ready, and keeps
  sending its lease write,
  `If-Match` on the last acknowledged version. Success proves `nodes/<id>`
  did not change, so no record naming this instance was freed meanwhile
  (section 2 frees only on a change of that object): the lease is valid
  again and the node acquires afresh, at new generations. A conflict then
  is handled as above. A bucket blip therefore never restarts a cluster.
- **A release before the lapse is seen** (amended 2026-11-14). The node
  learns that its lease ran out when a lease attempt ends after the
  validity it last published: the first attempt to end past it, whether
  it failed, timed out or landed, ends every hold into the pending
  releases above. Until then each hold is still in its actor's hands,
  invalid (it may not act: its lease ran out on its own clock). An event
  that reaches the actor then (a request, a call, an alarm, a drain's or
  a move's hand-over, a WebSocket closing) finds the hold no longer valid
  and drops it, and a dropped hold sends its release at once, as it does
  while the lease is valid; so does an idle actor's eviction. The lapse
  itself closes every WebSocket when the validity ends, without waiting
  for a lease attempt, and each close is such an event: every actor with
  sockets open is given up then. A parked record (ADR 0024) is released
  in the window only by a drain, or by a last socket's release whose
  check passed before the lapse: the sweeps and the last-socket release
  skip while the lease is invalid. If the bucket takes that write while
  it refuses or times out the lease's own (a slow or throttled bucket,
  an error on that one key; a whole outage refuses both, and the release
  waits as a pending one), the record names nobody and any node acquires
  it at once, without a fence.

  That is safe by ordering, with no clock compared between nodes.
  Dropping the hold empties its cell before the release is sent, and
  everything the owner does (acknowledging a write once its upload
  landed, alarm delivery, the alarm index rules, garbage collection,
  `exec-durable`) asks `LeaseView::is_valid`, which needs the cell; the
  node's own clock has passed the validity by then anyway, and either
  alone stops it. The next owner acquires only once the release landed,
  so after this node stopped acting, and restores what it lists then,
  which holds every acknowledged write. An unacknowledged upload of this
  node still in flight can land after that restore and before the new
  owner's first write, a late commit ADR 0007 allows: as likely as after
  any release while the lease is valid (a hibernation, an abandoned
  upload), and likelier than after a fence, which leaves at least the
  grace between the lapse and the takeover.

  With the defaults the window lasts from `ttl - safety` (8 s) after the
  send of the last acknowledged lease write until the next attempt ends
  past it: up to about 3.7 s (a write failing at once at 6 s, the next
  hanging for its 2.67 s from 9 s), less when the writes hang. Such an
  actor therefore moves 8 to about 11.7 s after that send (at 8 s for
  one with sockets open), where a fence frees it only after ttl plus
  grace (15 s) and the watch. The node that takes it sees a released
  record and counts no failover. The node that released it counts, in
  `wasmkeep_releases_after_lapse_total`, every release it sent while its
  lease was not valid, with a hold in hand, that landed: in the window
  above, and also once the lease task published the lapse and before it
  ended the holds (the lapse hook closing the WebSockets runs in
  between), and for a hold registered after that (an acquisition asked
  before the lapse whose write landed after: invalid from the start, its
  actor drops it, and its release is sent at once). It logs the first
  since its lease was last valid (`released an actor after this node's
  lease ran out`), and how many there were once the lease is valid
  again, not a line per actor. A release counts when it was answered:
  one that landed with its reply lost ends later as superseded,
  uncounted, so the count is a lower bound. Gating the first send on a
  valid lease, as the retries are, was weighed and not taken: it would
  slow these hand-overs to the fence's time and buy no safety. The
  cluster test
  `a_node_whose_lease_writes_fail_hands_its_actor_over_once_its_lease_ran_out`
  (the release's answer held for 250 ms, writes to the node refused
  meanwhile) and the simulations pin it: `madsim_early_release` (lease
  writes refused for a while, other writes landing, replies lost after
  their write landed, each release after a lapse answered late) and
  `madsim_replication` with lease cuts (real uploads, late ones
  included, checked by the final restore). They catch a hold that keeps
  acting until its release is answered on many seeds (about a fifth to a
  third of them in the first, a tenth in the second): on a hibernation's
  release, and on a release after a lapse with holds also acting on
  their cell alone, since the node's own clock already stops them there.
- **Twins** (two processes with one id) are caught by the condition
  itself: the second writer's condition fails and its re-read shows the
  other instance.

### 2. Fencing a node and taking its actors

Node B may fence node A only while B's own node lease is valid, and only
after B has seen A's object unchanged (the same instance and the same
`written_at`, which A changes on every write; never by comparing version
tokens) for a full lease period plus one attempt timeout, counted from B's
first acknowledged lease write after any lapse of its own (so after a
bucket outage every node first renews, and only then may judge the
others). B watches with the heartbeat listing's bodies, which carry no
version; only the final decision needs one. Then B makes one versioned GET of
`nodes/A`, and decides on that read alone (never on the listing, the
membership view or an earlier read): if it is `live`, and its
`written_at + ttl + grace` has passed on B's clock, B writes a fence over
it, `If-Match` on that same GET's version, state `fenced`, naming A's
instance, keeping A's `written_at`. If the GET shows a version other than
the instance and `written_at` B watched, A wrote: B starts watching
again. If A's next write landed in between, the fence
fails and B reads again (A is alive). If the fence lands, every write A
still has in flight fails its condition, so A's validity ends at the last
`written_at` B saw, plus `ttl - safety`: the same bound as today's
per-actor takeover. One fence covers every actor of that process. Two
nodes fencing A at once both condition on A's version; one lands, the
other re-reads a fence and goes on.

A record naming node A and instance *i* is **free** only on the evidence
of a read of `nodes/A` issued **after** the read of the record (never the
membership view, never an earlier read) that finds a `wasmkeep-node 3`
object in any state other than `live` naming *i*: `live` naming another
instance (free at once), or `fenced` or `released` naming any instance,
free once that object's kept `written_at + ttl + grace` has passed on the
reader's clock. The time bound makes freeing by fence as safe as today's
per-actor takeover whoever wrote the fence and whenever: the fenced
process's validity ended at that `written_at + ttl - safety` on its own
clock. Instances on `nodes/A` only
change through conditional writes, and a new instance writes only after
its start check waited ttl plus grace, so once *i*'s object was replaced
*i* can no longer write. Two cases free nothing, and the record stays held
until A's object shows one of those states: an absent `nodes/A`, and an
object in an old format (headers 1 and 2), since an old binary writes its
heartbeat unconditionally and may have overwritten *i*'s object while *i*
was still valid; new nodes log an old-format heartbeat as an old version
running (section 6).

`nodes/<id>` objects are lease state: nothing deletes one while a record
names that id (ADR 0012 never collects them). Revoking a node, which today
means deleting its heartbeat ([security](../security.md)), becomes
fencing it: `wasmkeep cluster fence <node>` writes a fence over the node's
current object (`If-Match` on the version it read, keeping its
`written_at`), without the watch a peer needs, since the time bound above
alone keeps it safe; the node exits at its next write, and its actors are
free ttl plus grace after its last write. When `nodes/<id>` is absent
(deleted by mistake, the machine gone), the command creates the fence with
`If-None-Match: *`, its `written_at` set to the operator's clock and no
instance: records naming that id are free ttl plus grace later, and a
process still running under it fails its next write and exits (its last
acknowledged write came before the object vanished, so before the fence's
`written_at`, within the skew bound). `wasmkeep cluster forget <node>`
deletes a retired old-format heartbeat once it is stale by the start
guard's test; no record can name it, since a new node using that id would
have replaced it. After an object was deleted
anyway, a process started with that `--node-id` frees its actors: it
passes the start check and its new instance frees the old one's records. Takeover stays lazy, as
today: the next request or alarm for one of the actors acquires it.

### 3. The ownership record

`owners/<tenant>/<class>/<name>` (a new prefix, never confused with old
`leases/` objects) holds a node id, an instance and a generation, or
names nobody (released), keeping the generation.

- **Acquire**, only while this node's lease is valid (its first `live`
  write acknowledged or adopted, and not run out), checked right before
  the record write. Read the record (version *r*), then, if it names nobody, is
  free (section 2, with `nodes/A` read after *r*), or names this node's own
  current instance (a hold of this process that ended, for example one
  whose release is still being retried), write it with `If-Match` *r* at
  **generation + 1**, naming this node and instance. With no record, see
  section 6 for the first generation; `If-None-Match: *`. No acquisition
  ever reuses or lowers a generation, so every hold keeps a generation of
  its own and ADR 0007, 0009 and 0012 hold unchanged. The new owner then
  restores, as today (it lists after acquiring).
- **Own.** A node owns the actor while it holds the hold it acquired (in
  memory) and its node lease is valid. `LeaseView::is_valid` becomes "this
  hold exists and the node lease is valid"; the acknowledgement rule of
  `Replica::put`, alarm delivery, the alarm index rules, garbage
  collection, `exec-durable` and the event cut on a lost lease all ask it,
  as today.
- **Release.** Hibernation, a clean stop, a drain, a move and an abandoned
  hold (a failed or interrupted upload) write the record naming nobody,
  `If-Match` on its version, generation kept. The node stops acting as
  owner before sending it, whatever the reply. If the release fails or its
  outcome is unknown, the node keeps it in a set of **pending releases**,
  each keyed by the actor, this instance, the hold's generation *g* and
  the version *v* of the hold's acquire (holds are not renewed, so that is
  the record's version while the hold lasts). Every retry is the same
  write, `If-Match` *v*, and never adopts a version read later (the
  re-read-and-adopt rule of section 1 is for `nodes/<id>` only). A pending
  release ends when it lands, or when a read shows the record no longer
  names this instance at generation *g* (nobody, another owner, a higher
  generation, or no record at all). A new acquisition of the same actor on this node
  does not cancel it: if the acquisition lands, its write moved the
  version past *v*, so the release can never apply, and the next read
  shows generation *g* + 1, which ends it. Retries back off, and pause
  while the node lease is invalid. (The first send of a dropped hold's
  release does not wait for a valid lease: section 1, "A release before
  the lapse is seen".)
- **An acquire that did not succeed** (no reply, a timeout, a cancelled
  write, and a conflict too, since the client retries internally and a
  first attempt may have landed) first reads the record back once, still
  under the actor's lock. Naming this instance at the generation tried,
  it is this write (only this node writes this instance, one acquisition
  of an actor at a time, and the generation is the attempt's): the
  acquisition holds it, at that read's version. Replaced since the read
  the write was conditioned on, by another holder whose node lease does
  not free it, the race is lost: that node owns the actor, and the
  request goes there. Otherwise (the record unchanged above all: the
  write may still be in flight, and land), or when a read fails, it is
  kept in the same set as an unresolved
  acquire (actor, this instance, the generation tried, the record the
  write was conditioned on, and a deadline). The background
  task reads the record until it can decide: if it names this instance at
  that generation and no hold for the actor exists here, it releases it
  (`If-Match` on that read's version: only this node could have written
  this instance at that generation); if that release fails or its outcome
  is unknown, it becomes an ordinary pending release keyed on that read's
  version. If a hold for it exists here, or the record names anything
  else, the entry ends, except a record still the one the write was
  conditioned on (or still absent, for a create): that write may still
  land, so the entry stays, read again, until the deadline. Past it, a
  read sent after the deadline that finds the record unchanged ends the
  entry. The record is compared by its content (holder and generation),
  never by version token: a record's content never comes back once it
  changed (every acquisition raises the generation, a release keeps it
  and names nobody). The deadline is set when the entry is made: 30 s
  later, `object_store`'s own request timeout (its default, which the
  server does not change), so a write the client gave up on has as long
  again to land. The deadline is on the node's wall clock, like every
  time in this record: a clock step moves it, which changes only how
  long a drain may wait. It bounds liveness only: a write
  landing later still is the case this rule narrows, harmless while the
  node serves the actor (its next event takes the record back as its
  own) and freed when the node stops or is fenced. Nothing in it rests on
  the node lease's validity: the entry acts only by releasing a record
  naming this instance with no hold here, and retries pause while the
  lease is invalid as for every entry; a deadline passed meanwhile ends
  the entry only on a fresh read. Two acquisitions of one actor that
  read the same record and both went unanswered keep one entry. A drain
  reports drained only once such an entry ends, so it may wait up to the
  deadline (plus a second: a pass whose entries only wait runs again a
  second later, and a waiting entry is not counted as a refusal by the
  pass's throttle); a clean stop does not
  wait for one, since its `released` write frees a record such a write
  left, landed or still to land.
  The drain and move outcomes that today say "the lease expires on its
  own" (`HandOver::Expiring`, `MoveOut::Expiring`, ADR 0014 and 0017)
  become "the release is pending": the actor is freed when it lands, or
  when the node stops or is fenced.
- **One lock per actor on each node** orders acquisitions and the
  resolution of unresolved acquires: an acquisition holds it from its read
  until its hold is registered (or its failure recorded), and the resolver
  holds it from its read until its release's reply. Without it, a resolver
  reading a record that a second, in-flight acquisition of the same actor
  just wrote at the same generation would release that new hold: two
  owners with no clock skew involved.
- **Who is the owner** (for routing): one GET of the record, within the
  250 ms the routing path allows today (`ADMISSION_WAIT`), and the named
  node's liveness from the membership view the node already keeps (a
  `fenced` or `released` object, or a node missing from the view, is not
  live). Routing never reads `nodes/<id>` per request; only an acquisition
  reads it fresh (section 2). A request reaching another node is forwarded
  to the named node, as today with leases; a record naming nobody, or a
  free one, is acquired where the request lands.

### 4. A clean stop and a drain

A clean stop, in this order: release every hold as today and wait for
the releases (bounded, as today); then make the node lease **locally
invalid** on the whole node, so every `LeaseView::is_valid` says no,
uploads still in flight return `Late`, and the lost-lease cut ends any
event still running (a straggler past the stop limit is cut, not left
acting); stop the lease task for good, without aborting an attempt in
flight (it finishes or times out, and an acknowledged one gives the
version to condition on; it extends nothing, since the node is already
invalid); and only then write the node lease `released`, `If-Match` on
that version, naming its instance, keeping its last `written_at`. On a
conflict, the section 1 re-read applies once: its own `live` object, adopt
and retry; a fence, done (it frees the records too). A write that
fails is tried again, for one attempt timeout and a second more, within
the stop's limit; past it, the node lease stays live:
peers free its actors only by fencing it, and a restart under its id
waits ttl plus grace. That write frees every record whose release had
not landed. A drain releases
each resident actor as today and reports drained only when its set of
pending releases is empty, so no record is left naming a live node that
no longer serves it.

### 5. Timing, unchanged

ttl 10 s, grace 5 s, safety 2 s, max skew 1 s, a lease write every third
of the ttl, and another node may take over 15 s after the last
acknowledged write, as today, plus whatever part of a lease period and an
attempt timeout the fencer had not yet watched.

The renewal arithmetic, as built: a write goes every
heartbeat period, at most a third of the ttl (3 s by default), from the
previous send; each is bounded by a third of `ttl - safety` (2.67 s); the
lease is valid `ttl - safety` (8 s) after the send of the last acknowledged
write. After a write acknowledged at `t`, the next go at `t + 3` and
`t + 6`: the first has its whole attempt, the second must be answered
within 2 s. So a node has two retries in practice before its lease runs
out; a third would need writes every `(ttl - safety) / 3`. A node whose lease writes keep failing loses
every actor at once instead of one by one;
`wasmkeep_node_lease_failures_total` replaced
`wasmkeep_lease_renew_failures_total` (removed with per-actor leases) as that node
signal, to alert on at its first increment.

### 6. Upgrading from per-actor leases

Old and new nodes must never run together, and an old binary must never
take an actor the new version owns, whether the new version found the
actor or created it.

- **Start order.** The bucket check first; then the start guard
  and the start check run together, so a new node serves about 15 s after
  it starts when the old nodes stopped at least 10 s before.
- **Start guard.** A new node waits (it does not exit) while any heartbeat
  in an old format (headers 1 and 2) was written less than its horizon
  plus ttl plus grace ago, re-reading every half period and logging how
  long is left. A heartbeat whose `written_at` is in the future lapses at
  the latest horizon plus ttl plus grace plus max skew after this node
  first read that same body (measured on this node's monotonic clock),
  with a warning naming the object, so a retired node's clock cannot hold
  the cluster forever (heartbeats and per-actor renewals are separate tasks: a
  stalled listing stops an old node's heartbeat while its renewals go
  on). An old heartbeat that keeps being rewritten means an old node is
  running: the new node exits with why, as the start check does for a
  twin. An old binary started after the switch cannot read new
  heartbeats; its own start check then reports its id as taken if a new
  node uses it, and every actor it reaches carries a marker (next item).
  After the switch, an old-format heartbeat that is fresh by the same test,
  or changes between listings, is an old version running: new nodes log it
  as an error and `cluster status` shows it. A stale one is a retired
  node's: `cluster status` shows it as retired, old format, without an
  error. Either way it frees no record (section 2).
- **First acquisition of an actor** reads `leases/<tenant>/<class>/<name>`.
  If it is still live by the old rules (`expires_at + grace` not passed),
  whatever node it names, this node's own id included (an old record has
  no instance to tell a predecessor from a past hold of this process): the
  actor is held, refuse as for a held lease today. If it is absent, create
  a `wasmkeep-lease 2` marker with `If-None-Match: *`; if it is present and
  not live, replace it with the marker, `If-Match` on its version. Old code
  reads the marker as corrupt and never takes it over (`LeaseRecord::decode`
  accepts only `wasmkeep-lease 1`). The
  marker carries the generation to seed: `max(the old generation, the
  highest generation in the actor's wal/ and snapshot/ keys)`. Then create
  `owners/` at that generation + 1. A crash between the two writes leaves
  the marker, from which the next acquisition seeds the same way.
- **The procedure** in [Upgrading WasmKeep](../cluster.md#upgrading-wasmkeep)
  changes with this version: start the new nodes once `cluster status`
  shows no live node, and expect them to serve about 15 s after they
  start. Its "going back is the same procedure" no longer holds after
  this upgrade: a per-actor-lease binary refuses every marked actor (by
  design). Reverting needs a tool that rewrites `leases/` from `owners/`,
  built only if asked.

## Cost, after

Per actor, once and for good: its first hold under the new version also
reads `leases/`, lists its keys to seed the generation and writes the
marker (a GET, a LIST and a PUT), and `leases/` markers and `owners/`
records are two small objects per actor that are never collected (an old
binary must keep being refused; ADR 0012 is amended to say so).

Per node, unchanged in count: a lease write every third of the ttl (the
heartbeat, now conditional) and the heartbeat listing, now on its own
task. Per hold: a record read and write to acquire, a write to release,
and the restore's listing, as today. Renewals disappear: in the soak,
about 23,000 of 47,083 PUTs, so a busy actor costs what it writes and what
its holds cost, not how long it stays in memory.

The listing (unchanged by this record, which made it
also what peers fence by): each node lists `nodes/` every heartbeat period
(3 s by default) and reads each node's object, one LIST and then one GET
per node, one after another. A cluster of N nodes makes N x (1 + N)
requests a period: 12 per 3 s at three nodes (about 260,000 GETs and
86,000 LISTs a day, about $0.50 a day at AWS S3 Standard list prices), 420
at twenty (about 12 million GETs and 580,000 LISTs a day, about $8). One
listing's duration grows with N too (`wasmkeep_listing_seconds`). A planned improvement reads a body only
when its listed ETag changed.

Bounds added after the first implementation, each to keep
one slow bucket from holding everything up:

| Bound | Value | Why |
| --- | --- | --- |
| Fences at once (`FENCE_CONCURRENCY`) | 8 | fences run on a task of their own, beside the listing, so a hung fence never delays the listing and the pins it refreshes |
| Pending entries at once (`PENDING_CONCURRENCY`) | 32 | after a lapse with thousands of holds, a pass one at a time took minutes; a drain and a shutdown wait on it |
| Refusals in a row before a pass stops | 32 | a refusing or throttling bucket gets no burst of thousands of writes beside the node lease's renewals |
| A foreground release (`RELEASE_TRIES`) | 1 s, then parked | hibernation and hand-overs do not wait on a slow bucket; the background retries keep the full attempt |
| A handed alarm looked at again (`handed_recheck_ms`) | 30 s, or twice ttl plus grace | the sender's release may be pending; its record frees on the release or a fence |

## Consequences

- One node lease covers every actor of a node: losing it (the bucket
  unreachable from that node for longer than `ttl - safety`) drops them
  all together. Today the same outage drops them one renewal at a time.
- A node restarted under its id takes its predecessor's actors back as
  they are next used, at new generations, after its start check.
- `wasmkeep cluster status` shows each node lease's state; the metrics gain
  successful lease writes and fences.
- The upgrade is one-way (section 6).

## Verification

1. **Measure first.** A counter of successful renewals
   (`wasmkeep_lease_renewals_total`) and the soak's request counts, so the
   before and after are measured, not derived.
2. **Simulation** (`sim/`): a node-lease model in the style of
   `madsim_lease.rs`, with lease writes delayed and applied late, lost
   replies and their retries, fences racing heartbeats, skew at the bound,
   restarts under the same id and releases with unknown outcomes. The
   invariant is the one checked today: never two owners acting for one
   actor, and never one generation for two holds. It must fail against a
   deliberately broken model (a fence that is not conditional; a wake that
   reuses a generation) before it passes against the design.
3. **`tests/cluster/`**: a fenced node drops its actors; a heartbeat that
   lands after a fence is refused; a lost reply is adopted, not taken for
   a fence; a restarted node takes its predecessor's actors back; a
   deleted heartbeat object frees nothing; after a bucket outage no node
   fences another before renewing; a clean stop cuts a straggler before
   writing `released`; a drain ends only when no release is pending; a
   fence decided on a stale read is refused by its condition; a pending
   release never applies to a later hold of the same node; an acquire with
   an unknown outcome is released if it landed; an old-format heartbeat
   frees nothing; `wasmkeep cluster fence` makes the fenced node exit; a
   lost acquire followed by a second one landing at the same generation is
   never released by the resolver while its hold lives (also a `sim/`
   case); an unresolved acquire whose write lands after the resolver's
   first read, the record unchanged then, is released (also a `sim/`
   case, `madsim_socket_home`'s `ResolverEndsUnchanged` variant: the
   resolver ending the entry on that first read is caught as a leak); the
   upgrade seeds generations above every existing segment, marks
   `leases/` for found and new actors, and refuses a live old record that
   names the starting node's own id.
4. **The soak, before and after** (`bench/soak.sh`, same bucket and
   machines): renewals gone from the PUT count, nothing lost or
   duplicated, failover no slower.
5. An adversarial review of this record, then of the code.

Recorded runs:

1. The renewal counter (`wasmkeep_lease_renewals_total`) measured the
   before: the per-actor soaks in
   [benchmarks](../benchmarks.md#three-nodes-against-a-real-bucket)
   (three 30-minute runs on R2 and OVHcloud, and the 12-hour run on three
   VPS: 5,024,032 writes, none lost). The after is item 4.
2. `madsim_node_lease`, 200 seeds: 2,776 acquisitions, never two owners,
   never one generation for two holds; the three broken models caught at
   seeds 62, 0 and 33. Since then the replication, alarm-index, drain and
   move simulations drive node leases too (each also run at 2,000 seeds),
   and all run at 200 seeds in `./scripts/check.sh --full`. The first
   `check.sh --deep` run (20,000 seeds each): `madsim_node_lease` 317,342
   acquisitions with
   a twin process, every broken model caught; every other simulation
   passed after one checker fix in `madsim_drain` (its deadline ignored a
   third node's bucket outage).
3. These cases run in `tests/cluster/` and `cluster/tests.rs` on every
   `check.sh --full`; the S3-gated suites pass against moto on the
   node-lease code, and the S3 tests against Cloudflare R2 and OVHcloud
   Object Storage. AWS S3 has not been run.
4. The 12-hour soak on the same three VPS, bucket and cadence as the
   per-actor run (build `82dab61`, the label the result file names carry
   for the build measured, before the bounded segment uploads; see
   [benchmarks](../benchmarks.md#three-nodes-against-a-real-bucket)):
   nothing lost or duplicated over 3,659,687 writes and 35 kills; every
   alarm fired. Renewals are gone from the request counts (PUTs other
   than segments 0.80 per node and second, against 3.1 to 7.4). Failover
   is not quite "no slower": writable again 18.2 s at the median against
   17.0 s, 24.4 s at p99; the worst, 89.6 s, was mostly one slow
   restore (56 s, 55 reads in turn, none cut), a restore cost both designs share (the
   per-actor run's worst was one too), not the lease. Durable writes ran
   slower during that run; the runs do not tell the bucket's speed from
   the design. Alternating 30-minute blocks run the same night
   ([benchmarks](../benchmarks.md#three-nodes-against-a-real-bucket)) do:
   node leases made 27 % fewer writes than per-actor leases (the tail:
   durable p99 about 1.7 times), with actors writable again 1 to 2 s later
   at the median. A cost of the design in the measured build `82dab61`,
   not of the bucket's speed; where in the durable write it lies is open.
5. Several adversarial review rounds of this record, then a review of
   each code change.

## Alternatives

- **Longer per-actor leases** (ttl 60 s): six times fewer renewals, and
  failovers six times slower. Rejected: failover time is a promise.
- **Keeping ownership across hibernation** (this record's first draft):
  saves a write on each hibernation and wake, but a woken hold then shares
  a generation with the previous one, which defeats the ADR 0009 owner
  fence and garbage collection's rule, and drains, moves and clean stops
  would also have to hand over actors that are not in memory. Rejected:
  renewals are the cost that matters.
- **One object listing every actor a node holds**, renewed as one PUT:
  one renewal per node, but a large object rewritten every 3 s, and
  acquisitions on one node contend on it.
- **Renewing only actors that ran recently**: fewer renewals for idle
  actors, but busy ones, the expensive case, still renew every 3.3 s.

## Implementation

- **Heartbeat format 3** (the node lease's lines, then the heartbeat's
  fields) is read by `wasmkeep_cluster::membership`; fences and releases
  keep those fields. The fence watch is `node_lease::FenceWatch`, and the
  routing read of a record `Owners::other_owner`. Ownership records stay
  in `node_lease::Owners` rather than behind the `Leases` API: the server
  calls them directly.
- **The node lease** is the heartbeat, written conditionally on a task of
  its own every heartbeat period (a period above `ttl / 3` is refused),
  each attempt bounded by `(ttl - safety) / 3`; a failed condition is
  re-read once, and a lost lease stops the node as a twin does; readiness
  follows the lease's validity; the start check reads with versions and
  waits ttl plus grace in every case; a clean stop writes `released` last.
- **Fencing.** Peers fence stale nodes (`FenceWatch` over the listing's
  bodies, the fence decided on one fresh read, only while this node's
  lease is valid); metrics count lease writes, their failures and fences;
  `cluster status` shows lease states. `cluster_tls` drops a fenced or
  released node's pins at the next listing, since it trusts only live
  nodes; a test says so.
- **Holds on ownership records** (`cluster/lease_hold.rs`): a hold is
  valid while it exists and the node lease is valid with no lapse since
  it was acquired; a lapse ends every hold at once into the pending
  releases, a lost lease or a twin drops them; pending releases and
  unresolved acquires are retried in the background while the lease is
  valid, under a lock per actor, an unresolved acquire kept while its
  record is unchanged until its deadline (`holds::retry`,
  `Owners::resolve`, 2026-11-14); drains report drained only with nothing
  pending; a clean stop releases, then makes the lease locally invalid,
  then writes `released`; routing reads the record and trusts a holder
  only if the view shows its named process live. The per-actor renewal
  metrics are gone (`wasmkeep_node_lease_failures_total` replaces their
  alert).
- **The upgrade path**: the marker (`mark_old_lease`: created or replacing
  an expired old lease, seeding the first record above its old lease's
  generation and every generation the actor's keys carry; a live old
  lease refuses, whatever node it names), the start guard (run with the
  start check; a rewritten old-format heartbeat refuses the start; one
  dated ahead lapses on this node's count), old versions logged while
  running and shown by `cluster status`, and the start check's shortcut
  on a fence or release of the process it first read (node leases need no
  age rule; old formats keep it). [Upgrading
  WasmKeep](../cluster.md#upgrading-wasmkeep) describes the procedure.
- **Operator commands**: `cluster fence` (`operator_fence`: over the
  current object, or created when absent; nothing over a fence or an old
  format) and `cluster forget` (a stale old-format heartbeat only). ADR
  0012 is amended (heartbeats, markers and records are never collected),
  and the revocation in [security](../security.md) is rewritten.

## Amendment of 2026-11-14: fewer listings of nodes/

Each node listed `nodes/` every heartbeat period and then read every
object the listing gave ("Cost, after"): 0.33 LIST/s per node at 3 s,
priced on R2 and S3 like a PUT. The listing is needed only to find nodes
this node does not know yet; the bodies of the nodes it knows are what
the fence watch, the failover wait, the pins and the peer list use, and a
GET of each gives the same body.

### The rule

`heartbeat_loop` (`cluster/view.rs`):

- A listing of `nodes/` at the node's first period, every
  `FULL_LISTING_EVERY` (10) periods (30 s), and when one was asked for:
  a hello from a peer that just started, an ownership record naming a
  node this node has never seen (`Cluster::address_of`), or with TLS an
  unknown pin knocking ([ADR 0011](0011-node-to-node-mtls.md), amendment
  of the same day). An asked-for listing is made at once, at most one
  early listing a period and none in a period that listed already; one
  asked for after that is made at the next period. A scheduled listing
  that fails is tried again the next period, and the nodes this node
  knows are read one by one in that period all the same (fix of
  2026-11-15: a failed scheduled listing was retried every period in
  place of those reads, so a store refusing LIST while answering GET
  froze the view, and the trust in every pin lapsed one horizon later).
  How fresh a peer's heartbeat is, and how long its pin is trusted,
  comes from the latest read of its object, by a listing or not: never
  from when this node last listed.
- In the other periods, one GET of each node the last refresh found,
  live or not, this node's own included (`membership::heartbeats_of`). An
  object that is gone (`cluster forget`) drops out; a read that fails
  fails the refresh, which keeps the last view, as a failed listing did.
- Everything after the read is the same code for both: the twin check,
  the notes about old versions and leases dated ahead, the peer list, the
  pins, `failover.observe` and the fence watch.

### What still holds

- **Fencing (section 2).** The watch sees the same bodies, every period,
  for every node it knows; the decision is still made on its own fresh
  versioned GET, conditional on it. A node is fenced exactly when it was
  before.
- **The free rule (section 3)** reads `nodes/A` after the record, as
  before; it never used the listing.
- **Failover timings for a node that was known:** a dead node's lease is
  watched from the same reads, so the time to fence it, and the time
  until its actors can be taken, are unchanged. A node fenced, released,
  restarted with a new instance, or forgotten is seen within a period, as
  before.
- **The twin check** still reads this node's own object every period.

### What got slower, worst cases at the defaults

| Event | Before | After |
| --- | --- | --- |
| A node of 0.4.2 starts: its peers see it | 3 s | 3 s (its hello makes them list at once; one that already listed early in that period lists at the next) |
| A node that says no hello starts (0.4.1 in a rolling upgrade, or every hello lost) | 3 s | 30 s, or within a period of its first handshake with an unknown pin over TLS |
| A node that starts, takes actors and dies before any peer saw it | fenced a lease period plus an attempt after the peers' first sight (3 s after its start) | the same, after the peers' first sight: within a period with its hello, up to 30 s without |

Until a peer sees a new node, it sends nothing to it, counts it in no
alarm shard and drains nothing to it; a request reaching that peer for an
actor the new node already holds gets a 503 to retry (the record names a
node the peer cannot reach), and makes the peer list at once, so for
about a period. Placement and availability only, never ownership.

### Cost after

Per node and period, at N nodes: N GETs, as before, and one LIST every
ten periods instead of every period. Three nodes at 3 s: 0.033 LIST/s
and 1.0 GET/s per node, from 0.33 LIST/s and 1.0 GET/s. Measured in
`tests/cluster/listings.rs`
(`an_idle_node_lists_and_reads_what_its_cadences_say`: 11 listings and
360 heartbeat reads in 120 periods, per node).

The planned improvement above (read a body only when its listed ETag
changed) saves nothing at idle: a live node rewrites `written_at` on
every renewal, so every live node's body changes every period. It would
save the reads of objects of nodes that stopped and were not forgotten.

### Rolling upgrades

A node of 0.4.1 lists every period, as before, and sees nodes of 0.4.2 at
once; it answers their hello 404 (a reserved path, never reaching a
component), which changes nothing on either side. A node of 0.4.2 finds
a node of 0.4.1 that starts within 30 s (above), or sooner over TLS.
Nothing in the bucket changes format.

### Simulation

`sim/tests/madsim_node_lease.rs` already watched peers by reading each
peer's object in turn (its fixed peer set; discovery is not modelled):
the model of the new periods. It passes at 200 seeds, its broken
variants caught as before.

### Tests

`tests/cluster/listings.rs`: `an_idle_node_lists_and_reads_what_its_cadences_say`
(the counted cadences of three idle nodes),
`a_node_that_joins_is_listed_by_its_peers_within_a_period` (fails without
the hello), `a_node_without_hellos_is_found_by_the_scheduled_listing`
(a node of 0.4.1, its heartbeat written by the test at an address that
answers 404, as 0.4.1 answers a hello).
`tests/revocation.rs`: the old process that "does not list the
heartbeats" (`Revoke::NewKey`, and the node cut off from listings) now
also cannot read them one by one; the guarantee checked is the same.

## Amendment of 2026-11-20: a slow bucket

A node trusts a peer (its pins, [ADR 0011](0011-node-to-node-mtls.md);
the sender of its hops; its place in the view) until that peer's
heartbeat's `written_at` plus the horizon, three heartbeat periods plus
the allowed skew: 10 s at the defaults. That rule stays. What changes is
how the node reads, so that its own late reads are not what makes the
trust run out. Before, the reads of a round went out one after the other,
none had a time limit, and the read of `control/changed` ran after them in
the same loop: with N nodes each read's latency counted N times, one read
that never answered stopped every later round, and forwarding to a
healthy peer was refused with "the heartbeat naming it expired". A
listing's reads after its LIST were sequential too.

### The rule

- **At once, each within an attempt.** A round reads the objects of every
  node it knows at once, each within one read attempt
  (`heartbeat_read_attempt`: the lease attempt, 2.67 s at the defaults, at
  most a heartbeat period but never under 1 s), up to 32 in flight
  (amendment of 2026-11-24, availability). A listing reads what it
  lists at once too (up to 16 reads in flight), the listing and its reads
  within two attempts. One slow read costs one read's time, not one more
  per node.
- **Per peer.** A read that answers is applied; one that finds no object
  drops that node, as a listing that leaves it out does; one that fails
  or times out leaves that node as last read. Such a node keeps the trust
  its last heartbeat gave, until that heartbeat's own horizon and never
  past it, and nothing about it changes: its pins stay, no connection
  pool is dropped, the failover wait keeps what it knew of it (since the
  amendment of 2026-11-24, availability: before, it judged the same body
  again and could take a node that still writes for a stale one), and the
  fence watch neither counts it as seen again nor forgets it (only
  heartbeats just read are watched). The round reports a failure (the hellos and the
  scheduled listing go on as before) and the next round reads it again.
- **Early reads.** After a round, a peer whose trust would end before the
  next round could land (before `next + lead`, `lead` twice the slowest
  read of the last round plus 50 ms, at most one attempt) is read again
  `lead` before its trust ends, once per heartbeat of that peer
  (`membership::early_reads`). On a healthy bucket no peer is ever due
  (its trust ends about 7 s after the next round starts), so the counted
  cadences of the 2026-11-14 amendment are unchanged. The early read is
  not cancelled by the end of the period; the round waits for it.
- **The intents apart.** `control/changed` and `control/` are read on a
  task of their own (`control_loop`), each bucket operation of a refresh
  within one read attempt (the marker's read, the four listings of the
  intents, the deletes of expired ones), not the refresh as a whole: a
  slow bucket that answers still has the intents applied, late, and one
  operation that does not answer fails the refresh, which keeps the last
  intents (ADR 0014). Loading the cluster secret at the top of a round is
  bounded by two attempts.
- **Order.** The heartbeats as last read are the process's, shared by
  its apps as the trust table is (ADR 0038), and each app applies its view
  while holding them, so two apps never publish their views out of order.
  A node's entry is replaced only by a read issued later than the one it
  holds, whichever lands last, and a listing issued before a read does not
  drop the node that read found. Issue order is not the bucket's order: a
  read issued later may have been served before a write another read
  already saw. So a `live` body never replaces a held `fenced` or
  `released` body of the same instance (that process can never write
  again, so any live body of it is older). Other stale bodies served that
  way are possible, for example an older `live` body of a node read after
  a newer one: the next read replaces it, and the trust it gives still
  ends at that older heartbeat's own horizon, so it is never extended.

### Freshness, and the clocks

A heartbeat is judged by when it was written, never by when this node's
read of it finished: the trust in it ends at `written_at + horizon` on
this node's clock, and no read result moves that end later except a newer
heartbeat. One exception makes it shorter, never longer: a heartbeat dated
ahead of this clock counts as written when the process first read that
heartbeat (`membership::FirstSeen`, kept with the heartbeats as last read
and shared by the process's apps), and reading it again does not move
that, nor does an app joining the process or a runtime restarted in place.
A restart of the whole process starts it again: a dead peer whose clock
is ahead is then trusted for one more horizon from the restarted
process's first read. Before, a peer whose clock ran an hour fast was trusted for an hour
after it died.

A peer's clock behind this one makes its trust end early by that much:
the horizon's skew allowance (1 s) covers the tolerated skew, as before.

### How long a dead or partitioned peer stays trusted

Let `T` be the true time of a peer's last heartbeat write that reached the
bucket, `d` how far its clock is ahead of this node's (negative if
behind), and `F` when this node first read that heartbeat. This node
stops trusting it at `min(T + d, F) + horizon`, whatever its own reads do
after: at most `T + 11 s` at the defaults while the clocks keep within the
allowed skew, and at most `F + 10 s` whatever the peer's clock says.
Without a read of that last heartbeat, the trust ends earlier, at the
previous one's horizon. So:

- a peer that dies, or is cut off from the bucket (it can no longer
  write), is distrusted within those bounds; its own lease ran out
  before, `ttl - safety` (8 s) after its last acknowledged write was sent,
  and its actors are free `ttl + grace` (15 s) after its last write, as
  before;
- a peer fenced, released, forgotten or with a new key is distrusted at
  the first read that shows it, within a period, as before;
- a peer cut off from this node only, still writing, stays trusted: its
  membership is confirmed; the connections to it fail and are handled as
  before (the suspect list, the failover marks).

### The horizon: unchanged

The trust in a peer that keeps writing must outlast the time until a
newer heartbeat of it is read. That heartbeat can be one write period
plus its write's latency old when read, plus the skew, and the read takes
its own latency. With rounds alone the next round's read lands up to a
period after the last, so the horizon must cover two periods, the write
latency, the read latency and the skew: at 10 s, the write and the read
together get 3 s. With early reads the second period is gone: a read
issued one attempt before the trust ends renews it if one period, the
write's latency, the read's attempt and the skew fit in the horizon:
3 s + 2.67 s (a write slower than the lease attempt fails anyway, and the
writer's lease is then at risk too) + 2.67 s + 1 s = 9.33 s, within 10 s.
So no widening: it would lengthen what it costs to revoke a node that
stops writing (ADR 0011), the time a dead peer is still chosen to place
or drain to, and the start guard's wait for an old-format heartbeat
(horizon plus 15 s), for no gain. The horizon must also stay under ttl
plus grace (15 s): past that, a dead node's actors may be taken over
while it is still in the view.

Past those latencies the node still fails closed: reads that keep taking
longer than the margin let the trust end at the newest heartbeat's
horizon, as before, and a request to a peer whose trust ended still waits
up to 3 s for a read that renews it (ADR 0011, step 6). A read slower than
the attempt is now given up rather than waited for: a bucket whose reads
all take a little longer than 2.67 s renews nothing, where the reads
before this amendment renewed late. That too fails closed, and the
horizon would not have covered such reads anyway (a period, the write and
the skew leave 6 s for one read, and with rounds alone 3 s for the write
and the read together). An early read planned on the last round's latency
can also land a little late if the bucket just got slower; the next one
is planned on the new latency.

### Metrics

`wasmkeep_heartbeat_read_milliseconds` (each read of a peer's heartbeat,
timed-out ones at the attempt), `wasmkeep_node_lease_write_milliseconds`
(each write of this node's own, from its send to its answer),
`wasmkeep_heartbeat_age_milliseconds` (how old each peer's heartbeat was
when applied: that peer's period and write latency, this node's read
latency and the clocks' difference), `wasmkeep_heartbeat_read_failures_total`
and `wasmkeep_heartbeat_early_reads_total`. Read lag against write lag:
compare a node's reads with its peers' writes; an age's 99th percentile
nearing the horizon (10 s), or early reads rising, says the margin is
thin ([monitoring](../monitoring.md)).

### Rolling upgrades

Nothing in the bucket changes, nor what a node writes or how often it
lists. A node of an older version still reads one object after another.

### Simulation and tests

The protocol under test is the reader's timing, which the simulations do
not model (they read each peer's object in turn with no latency); the
safety properties they check (fences, the free rule, ownership) do not
depend on it, and they pass unchanged. The cases are in the cluster
suite, plain and over TLS, with a bucket slow for one node:

- `tests/cluster/slow_bucket.rs`:
  `forwarding_stays_up_on_a_slow_bucket_while_a_dead_peer_expires` (every
  heartbeat read of one node 100 ms late; its trust in a peer that keeps
  writing never lapses over 2 s of samples every 10 ms, and every
  request it forwards there is answered, while a peer that dies is
  distrusted within the horizon of its last write; before, the trust
  lapsed in about one sample in seven) and
  `a_hung_control_read_does_not_hold_up_the_view` (a read of
  `control/changed` that never answers; before, the trust in the peer
  lapsed for good a horizon later).
- `cluster::tests`: `one_hung_heartbeat_read_neither_holds_up_the_view_nor_drops_the_pools`,
  `heartbeat_reads_go_out_at_once` (a listing and a round of five peers
  whose every read takes 300 ms end within a second; they took 1.5 s and 1.8 s),
  `a_heartbeat_dated_ahead_is_trusted_one_horizon_from_its_first_read`,
  `a_failed_read_carries_a_heartbeat_only_to_its_own_horizon` (the rule
  kept), `an_older_read_landing_late_never_undoes_a_newer_one`,
  `a_live_body_read_late_never_undoes_a_fence_of_its_instance`,
  `a_runtime_that_joins_late_does_not_trust_a_dead_fast_clock_peer_again`
  and `a_slow_bucket_still_applies_the_intents` (every intents operation
  200 ms, under the attempt, five of them past two attempts together).
- `membership::tests`: `a_trust_about_to_end_before_the_next_round_lands_is_read_early`
  and `a_heartbeat_dated_ahead_counts_from_its_first_read`.

### Second part (same amendment): listing off the read path

The first part left two costs on a slow bucket: every
tenth round still listed `nodes/` inline and waited for the LIST and its
reads (up to two attempts) before reading by key, and a listing that
failed left the next one due, so with a steadily slow LIST every round
listed again, back to back. And a prefix that holds many `released`
node leases left by earlier clusters had every round read them all by
key, beside the few live nodes.

- **Listing is discovery only, on a task of its own** (`listing_loop`):
  new node ids, nodes whose object is gone, and the objects of stopped
  nodes. The first period's round still lists (within two attempts), to
  find the peers before saying hello, unless the process already holds
  heartbeats (amendment of 2026-11-24, availability); from then on every
  round reads the known nodes by key. The listing task lists every ten periods and when
  asked (a hello, a record naming an unknown node, an unknown pin, a first
  listing that failed), at once but at most once a period; an ask that
  arrives while it lists is kept for its next listing (an ask is no
  longer lost when the period ends). Each listing gets the longest of two
  attempts, the scheduled interval and 30 s, since nothing waits for it;
  one that fails or runs out is tried again after one period, then two,
  four, at most the interval. Its results are applied like any read's,
  ordered by when they were issued, so a listing landing late changes
  nothing a later read saw.
- **Stopped nodes are not read every round.** A node whose heartbeat as
  last read is `released` or `fenced` for an instance cannot come back
  as that instance (instances change only through conditional writes, and
  a new process has a new one), so rounds leave it out; the listings
  still read it, so a new process under that id is found by the next
  listing, or sooner by its hello. Its object stays as last read for the
  free rule (section 3) and the failover wait, as before. With 90
  released leases and three live nodes a round makes four reads, not 94.
- **A join is not held up by a slow LIST.** A hello, a hop from a signed
  sender, or this node looking for a node (`address_of`, a record naming
  it) makes this node read that node's object by key at once, on a task of
  its own, at most once a period per node (`join_read_loop`), unless that
  node is in its view and not stopped. So a node restarted under its id
  after a clean stop (`released`, which the rounds no longer read) is
  found within about a period, whatever the listing does; the process's
  table of live senders, which keeps the stopped instance's last live
  heartbeat until its horizon, does not decide it. Over TLS the first hello of a
  new process is refused at the handshake (its pin is not known yet),
  which asks for a listing as before: such a join waits for that listing,
  which now lands however slow the LIST, where a listing given two
  attempts never did. Reading the certificate's name at the handshake
  would avoid that wait; done in the amendment of 2026-11-24,
  availability.
- **The dead-peer bound is unchanged**: rounds by key renew a peer's
  trust, so a slow LIST changes nothing about when a dead one lapses.

#### Long-released node leases: `cluster forget`

A `released` node lease can now be deleted by `wasmkeep cluster forget`,
by the operator, under three conditions: it is older than its horizon
plus a lease's ttl and grace (25 s at the defaults, the window already
used for old-format heartbeats); no ownership record of any app under the
prefix names that node, any instance (one listing per tenant and app of
`state/`, one read per record; a record that cannot be read counts as
naming it); and the object is unchanged when read again right before the
delete. Why that is safe: the released object is what frees a record
still naming the node (section 3; an absent object frees nothing), so it
must stay while any does; no new record can name it, since only its own
process writes records naming itself and that process released; and a
new process under the id replaces the object first, which the second
read sees. What remains: a store's DELETE takes no condition (the bucket adapter
has no conditional delete), so a process starting under that id whose
first write lands between the second read and the delete loses its lease
(its next write finds no object, `LostCause::Deleted`) and exits, to be
started again. That costs availability only: nothing was held under the
new instance yet, and no other node's state is touched. A `fenced` lease stays: its process did not stop cleanly, so
records likely still name it, and the fence is the evidence that frees
them and that ends that instance. A `live` one stays (fence it to revoke
the node).

The other option, nodes removing long-released leases on their own, was
not taken: every node would delete lease state, racing a process starting
under that id (its start check reads that object) and each other, to save
what is now nothing per period (rounds no longer read stopped nodes). An
operator command, run when wanted, with the record check, is the
conservative choice.

#### Tests (second part)

- `tests/cluster/slow_bucket.rs`:
  `a_slow_listing_holds_up_no_read_and_a_dead_peer_still_expires` (every
  LIST from one node 1.5 s late, three times what an inline listing had:
  its trust in a writing peer never lapses over 3 s of samples, every
  forwarded request is answered, and a peer that dies is distrusted
  within the horizon of its last write; before, the trust lapsed in 81 of
  271 samples) and
  `a_node_that_joins_while_listing_is_slow_is_trusted_from_its_hello`
  (LIST 3 s late: plain, the joining node is trusted within 2 s through
  its hello's read by key; over TLS within 10 s, through the listing its
  refused handshake asks for; before, never) and
  `a_node_restarted_under_its_id_is_read_by_key_whatever_the_listing` (a
  clean stop, then the same id again: trusted within 1 s with listings 3 s
  late or refused; before, only a listing found it).
- `cluster::tests::a_round_reads_only_the_nodes_that_may_still_write`
  (90 released leases and three live nodes: four reads a round, 94
  before).
- `status::tests::a_released_lease_no_record_names_may_be_forgotten`
  (wasmkeep-cli: removed when no record names it; kept when one does,
  when recent, and when fenced or live).
- `tests/cluster/listings.rs`: a listing's read of a node's own object is
  no longer one of its rounds, and a listing's reads are left out of the
  heartbeats read a period (three, as before).

## Amendment of 2026-11-23: a predecessor's body read back

A node restarted as a new process (same id, a new instance) stopped itself
once in 15 restarts of a three-node cluster during a rolling restart. Its
start check passed over its predecessor's `released` lease, its first write
replaced that lease, and it served; half a second later its view showed
the predecessor's `released` body again, and the running check took it for
another process ("this node's lease was released by another process").

Within one runtime, every read of `nodes/` waits for that first write. Within
one process it does not: the apps of a process share the heartbeats as last
read ([ADR 0038](0038-several-apps-per-process.md)), and those nodes ran two
apps each. A sibling app that became unique first listed `nodes/` before
this app's first write, which kept the predecessor's body for this node's
id; a read by key of a peer alone (a join read) then applied every node as
last read to this app's view, before this app read its own object again. A
bucket replica behind an acknowledged write would deliver the same body
the same way. The rule below judges the body by its writer, so it covers
both.

### The rule

- The start check returns, with the version the first write is conditioned
  on, the instance and `written_at` of the node lease (format 3) under that
  version, if it names one: the predecessor that write replaces.
- After the first write landed, a body of this node's own object that names
  that instance and is dated no later than that `written_at` is not a twin:
  it is a copy of the body replaced, or an earlier body of the same writer.
  The running check ignores it, and the next read shows this process's own
  write.
- Everything else is judged as before: another instance (live, `released`
  or `fenced`, whatever its date), a body of the predecessor's instance
  dated later than the one replaced, a body in the old format, or one with
  no instance. A first write that created the object (none existed) replaces
  no predecessor, and every other body is judged as before. A first write
  that fails its condition goes back to the start check, and the
  predecessor is the one that check reads.

### Why this hides no twin

- The predecessor can write nothing more under this id: every write of a
  node lease (renewal, release, a lost reply's retry) is conditional on that
  process's own last version, which this process's first write replaced. A
  write it made since would be dated later on its own clock, which is the
  same clock that dated the body replaced, so no clock skew between nodes
  enters the comparison.
- A twin is another instance: a process started at the same moment, whose
  body names its own instance, or one that writes `released` over this
  node's lease, is judged as before. Its writes also make this node's next
  renewal fail its condition, whose re-read stops the node, as before.
- A process of an older version writes without conditions: its body is never
  exempt.
- The rule relies on the conditional writes the bucket check proves at
  start, as every other part of this record does.

### A hello goes to the peers of its own app

The same restarts left one to three warnings on the peers, "bad, expired or
misaddressed signature". A node said hello to every node of its view,
whatever its app; a process serving several apps routes a hop by the app it
names, the sender's, to a runtime that checks the signature against its own
node id, not the one the hello was signed for. A node now says hello to the
peers of its own app only: the other app's runtime says hello to its own.
Such refusals are now counted, `wasmkeep_signatures_bad_total`.

### Tests

- `tests/cluster/node_leases.rs`:
  `a_predecessors_body_read_after_a_restart_is_not_a_twin` (a clean stop,
  a new process with the same id, then reads of its own object answer with
  the predecessor's `released` body for several rounds; again with its last
  `live` one, from a new process: the node keeps serving; before, it
  stopped on the first),
  `a_twin_is_found_after_a_restart_whatever_its_body` (the same reads
  answering with a `live` body of another instance dated before the
  restart, a `released` body of another instance, or a later body of the
  predecessor's instance: each still stops the node) and
  `after_a_failed_first_write_the_predecessor_is_the_one_read_again`
  (another stopped process's body lands between the check and the first
  write: a copy of it is not a twin, one of the first predecessor's is).
- `cluster::tests::a_siblings_read_of_this_nodes_predecessor_is_not_a_twin`
  (two apps in one process: the sibling's listing holds the predecessor's
  body, a read of a peer alone applies it; not a twin, while another
  instance applied the same way is) and
  `cluster::tests::only_the_replaced_writers_past_is_not_a_twin` (its
  `released` or `fenced` body, or an earlier one: not a twin; a later body
  of it, another instance, no instance or the old format: a twin).
- `tests/cluster/two_apps.rs`:
  `a_restart_says_hello_to_its_own_apps_peers_only` (two processes serving
  two apps each, one restarted: no call refused for its signature; before,
  18).

## Amendment of 2026-11-24: lease reads, and the upgrade floor

A record naming node A and instance *i* is free on a read of A's lease
made after the read of the record: `live` naming another instance, or
`fenced` or `released` once its `written_at` is ttl plus grace old. The
rule trusts that read. A read that returns an earlier body of A's lease (a
replica or a cache behind the bucket's latest write) can free a record the
current instance holds: A ran as *h*, stopped, and now runs as *i* and
holds the actor; a stale read of *h*'s `released` body, long past its ttl
and grace, or of *h*'s last `live` body (another instance than *i*), frees
the record while *i* acts as its owner.

Matching the body's instance against the record's is no remedy. A body
of another instance is the normal proof that a holder is gone: its
process exited, the node id started again, and that newer process may
itself have stopped since. The same body names another instance when it
is an earlier process's, read late. Neither the record nor the lease
carries anything that tells the two apart, and refusing every mismatch
would leave the actors of a restarted node unfreeable.

### The rule

- Every process keeps the newest body of each node's lease it has read,
  from any read: its heartbeat rounds and listings (every app of the
  process, as their heartbeats as last read), and the reads made to judge
  a record. It only moves forward. Of two bodies of the same instance, a
  `fenced` or `released` one is newer than any `live` one, whatever their
  dates: that instance never writes `live` after its end, and its clock
  may have stepped back before it (a fence or a release keeps the
  `written_at` of the last `live` write). A fence an operator created
  over a deleted object (`fenced`, no instance) is newer than a body of a
  named instance dated less than twice the skew after it, and older than
  one dated that much after it or more (see "A fence created over a
  deleted object" below). Otherwise the body dated earlier is older; two
  bodies of different instances dated the same are not ordered.
- A read made to judge a record (the acquisition's, and the read back after
  an acquisition's failed write) that returns a body older than the newest
  held for that node is read again, up to three reads in all. If every read
  is older, nothing is judged: the acquisition is refused for now as a
  stale read (nothing written; the request is answered as unavailable),
  and the read back stays unresolved, as for a failed read.
- The newest held is never kept on its date alone. Once judging reads
  have returned older bodies for ttl plus grace, with no read returning the
  held one (or a body not older) in that time, the next older body read
  replaces it, counted and logged. The window starts at the first such
  read, not at the last confirming one, so an outage with no reads at all
  does not open it early: the first read after an outage is still judged
  against the newest held. A read that finds the lease absent (a judging
  read, or a heartbeat round that finds the node gone or a body it cannot
  read) is a current answer too: the window starts again, so the old
  bodies a replica serves of a deleted lease never replace the newest
  while the bucket answers that it is gone. A clock that ran ahead, or one
  that stepped back across instances, so holds a node's records back for
  about that long on a process, and the rule is then the one before this
  amendment, never weaker.
- Counted: `wasmkeep_lease_reads_behind_total` (reads behind, read again),
  `wasmkeep_lease_reads_stale_total` (judgements refused because every read
  was behind) and `wasmkeep_lease_reads_unpinned_total` (entries replaced
  after the window), each logged at most every 10 s with the node and both
  dates.
- Nothing changes in the stored records or leases, so nodes of the
  previous patch run beside this one: each judges by its own reads.

### A fence created over a deleted object

`cluster fence` over an absent `nodes/<id>` creates a fence dated by the
operator's clock, naming no instance, then moves its date to the bucket's
time plus a second and the skew if that is later (`raise_created_fence`).
It is written after every body of the processes before it, yet until it is
raised (or if the raise fails, or with `operator_fence` alone) it may be
dated before the last of them: that process's clock may run ahead and the
operator's behind, each within the skew allowed, so by less than twice the
skew. Ordered by date alone, a process that read the predecessor's last
`live` body and then the fence takes the current fence for a stale read
(refused, not judged) and keeps the predecessor's body as its newest. When
the node id runs again as another instance and takes the actor, a stale
read of that predecessor's body is not behind, names another instance than
the record's, and frees it at once: two owners. The simulation found it
(`stale_lease_reads_never_free_a_held_record`, seed 5367 of 20,000).

- Ordering: a created fence is newer than every body of a named instance
  (`live`, `fenced` or `released`) dated less than twice the skew allowed
  after it, and older than one dated at least that much after it. A body
  written before the fence is dated less than twice the skew after it
  (its writer's clock at most the skew ahead, the operator's at most the
  skew behind), so every predecessor's body is older. Two created fences
  (one raised to a later date) are ordered by date.
- Starting over one: the start check writes the first `live` body no
  earlier than the created fence's `written_at` plus twice the skew on its
  own clock, whether the fence was there at its first read or appeared
  over no object during the check. The check compares the fence's date
  with the clock that dates the first write, right before that write, so
  the body is at least twice the skew after the fence whatever the skew,
  and a successor's bodies are always newer than the fence. With a fence
  read first, the check's own wait (ttl plus grace from that read) already
  goes past that when ttl plus grace is at least four times the skew (the
  defaults: 15 s and 4 s), unless the fence is dated ahead of this clock;
  a fence created late in the check is where it adds up to twice the skew.
  The node logs the longer wait, and `cluster fence` warns when this
  machine's clock is ahead of the bucket's, since that dates the fence
  ahead.
- `cluster fence` dates by the bucket's time every created fence it leaves
  behind: one it created, and one it found already there (`NotLive`), as
  after a create whose reply was lost and whose retry found its own fence.
- Why not ttl plus grace (this amendment's first form): every body written
  after a fence but dated less than the window after it makes a reader keep
  the fence as its newest, and the fence served again frees the record
  that body's process holds. Nodes of 0.8.0 do not wait for a fence's
  date, so with ttl plus grace a node of 0.8.0 starting anywhere in the
  fence's first ttl plus grace was exposed. Twice the skew is the
  smallest window that still orders every predecessor's body.

### Nodes of 0.8.0 beside this version

Under a bucket that never serves stale reads, none of this matters. Under
stale reads, in a cluster that mixes 0.8.0 and this version:

- A node of 0.8.0 starting over a created fence does not wait for the
  fence's date. Over a fence it read first, its own wait still dates its
  first body past the window at the defaults (a fence not dated ahead);
  over a fence created during its check, it starts once its own wait
  ends, and its first body may be dated inside the window when the fence
  landed within about twice the skew of that end. A reader of this
  version that then reads that first body, and no later one, takes it for
  a stale read and keeps the fence; served the fence again once its ttl
  plus grace has passed, it frees the record that node holds
  (`node_lease::tests::a_start_inside_the_fence_window_is_not_closed`).
  A read of a body dated past the window ends it: the node's next renewal
  (every third of the ttl), or, over a fence `cluster fence` dated by the
  bucket's time plus a second and the skew, the one after, since the
  node's second body can still fall inside the window then.
- Compared with the rule before this window (dates alone), that is a
  narrower exposure: dates alone also freed records through a
  predecessor's body dated after a created fence, in a cluster of any
  version, and made a reader refuse the current fence. Compared with
  0.8.0's own rule (every read judged as read), it is narrower still:
  there, any stale read of an earlier body frees a record.

### What it does not close

- Deleting a live node's lease out of band (a manual `s3 rm`, a lifecycle
  rule, a sync tool) is outside the guarantee. A created fence that lands
  late shows why. `operator_fence` dates the fence before it sends the
  create, and `If-None-Match: *` cannot tell one absence from another.
  object_store (0.14.2) retries a conditional create after a 5xx for up to
  180 s. So a create can land over a later absence: after the lease of a
  process that started over no object and took actors was deleted. Its old
  date then frees that process's records while its lease is still valid
  (`node_lease::tests::a_created_fence_landing_late_is_not_closed`: dated
  100 s, a node started at 108 s, its record taken at 115.5 s with its
  lease valid until 116 s). `cluster fence` moving any created fence it
  finds to the bucket's time narrows this to a create that lands after the
  command has exited. The complete fix is
  [#303](https://github.com/WasmKeep/wasmkeep/issues/303).
- The window tolerates exactly the configured skew. A predecessor whose
  clock ran ahead by more than the skew allowed can date a body past the
  window, and the rule is then the one before it (dates alone) for that
  body; as everywhere in this protocol, a clock beyond the skew allowed
  voids the time bound.

- A first read that is stale: a process that has never read a body of
  A's lease newer than the stale one cannot tell. Closing it needs the
  ownership record to carry the date (or version) of its holder's lease
  when it acquired, so a reader can refuse a body dated before it. That is
  a format change, gated as every format change is.
- Order across instances rests on their writers' clocks. A successor
  writes its first `live` body only after its start check has watched the
  object for ttl plus grace and, over a `fenced` or `released` body, until
  that body's `written_at` is ttl plus grace old on its own clock (section
  1), which is more than twice the skew allowed. So a successor's
  bodies are dated after its predecessor's. A fence created over a deleted
  object has no start check: it is ordered by the rule above, not by date
  alone. Twins (two processes started
  with one id by mistake) write in turn and may be dated within the skew
  of each other: their bodies can be ordered wrongly, and the rule then
  gives no more than the rule before. A body dated ahead holds the newest
  until older bodies have been read for ttl plus grace with none
  confirming it: a record may stay unfreed on that process that long,
  never freed wrongly.
- Reads that are all behind for ttl plus grace, with no current answer at
  all (no body, and no absent answer) in that time, replace the newest
  held: the rule before this amendment, as said above.

### The upgrade floor record

The app's upgrade floor is `formats/<tenant>/<app>/floor` under the bucket
prefix, beside the recorded-head setting
([ADR 0035](0035-a-recorded-head-on-hand-over.md), section 5), and outside
`state/` and `apps/`. Its body is exactly:

```
wasmkeep-floor 1
<major>.<minor>
```

each line ending in a newline, the version in decimal without leading
zeros (`0.9`). It names the lowest minor version allowed to serve the app.

- Read by every node at start since 0.8.1, after the bucket check and
  before its first lease write. A floor above the binary's own minor
  version stops the start; the message names both versions. A missing
  record starts as before. A body in any other form (another header, a
  third number, a trailing space, CRLF) refuses to start, as an unreadable
  format setting refuses service. A read with no answer is retried and
  neither starts nor refuses.
- Read a second time once the first lease write has landed, before the
  node serves. Until that write the node has no live lease, so whoever
  writes the floor after checking the live nodes cannot know a node is
  starting; the second read catches a floor written in between. Only an
  answer stops the node there (a floor above its version, or an
  unreadable body), and its lease is then written `released`: it held
  nothing, since acquisitions wait for the start's verdict. Without an
  answer the node stays checking, renews its lease every period as usual
  and reads the floor again after each renewal; a clean stop meanwhile
  releases the lease.
- The second read does not close every race with the writer. A writer
  that lists the live nodes before this node's first write lands, and
  writes the floor after this node's second read, finds no such node and
  is not seen by it. Only the writer can close that: once the floor is
  written, it lists the live nodes again and reports any below the floor.
  0.9.0's `finalize` must keep that second listing.
- Not read after that: a node already serving when a floor above it is
  written keeps serving until it stops.
- No command removes it: app deletion, `--recreate` and restores work
  under `state/` and `apps/`, never `formats/`, as for the format setting.
- Written from 0.9.0, by `wasmkeep cluster upgrade finalize`, once every
  live node runs the new minor version. Nothing in 0.8.1 writes it. A node
  of the previous minor version that was offline when the upgrade was
  finalized then cannot start into the cluster.
- Written also, from 0.9.0, for an app that never ran (no floor, no
  registered node, no deploy, no state): its first deploy,
  or its first node when it is served without a deploy, creates the floor
  at its own minor version
  ([ADR 0043](0043-rolling-upgrades.md), section 3). Both writers only
  create or raise it, by conditional writes.

### Tests

- `node_lease::tests`:
  - `which_body_of_a_lease_is_older`: an instance's end is newer than any of its `live` bodies, whatever the dates; a created fence is newer than every named body dated less than twice the skew after it, and older than one dated that much after it or more.
  - `a_created_fence_is_newer_than_the_bodies_before_it`: a predecessor's `live` body dated after an operator's created fence, served again once the node runs as another instance that holds the actor, is read again and frees nothing (judged by date alone it took the record).
  - `a_successor_soon_after_a_created_fence_keeps_its_record`: a successor starting 1 s (a node of 0.8.0) or 2 s after the fence, renewed, keeps its record against the current read and the fence served again (a window of ttl plus grace refused the first and freed on the second).
  - `a_start_inside_the_fence_window_is_not_closed`: the limit above, and the same reads leaving the record to a successor dated past the window.
  - `a_created_fence_landing_late_is_not_closed`: the out-of-band limit above, and the same fence moved to the bucket's time freeing nothing.
- `wasmkeep-cli` `cluster_cmd::fence_tests::a_created_fence_found_is_dated_by_the_bucket`: a created fence the command finds already there is dated by the bucket's time as one it created; a peer's fence keeps its date.
  - `an_absent_lease_keeps_the_newest_pinned`: absent answers between stale reads keep the window from opening; without them, it opens.
  - `the_newest_reads_only_move_forward`.
  - `a_body_dated_ahead_holds_the_newest_for_a_window_only`: an older body replaces the held one once older bodies have been read for ttl plus grace with no confirming read. A gap with no reads does not shorten the window, a confirming read ends it, and a clock stepping back counts it from the step.
  - `a_stale_read_of_a_predecessor_frees_nothing`: the bucket serves a predecessor's `released` body after the process read its successor's `live` one. The acquisition is refused as a stale read while every read is stale, and the record is held by the successor once a read is current. Judging the exhausted read anyway, or switching the table off, frees the record.
  - `a_fence_after_a_clock_step_back_frees_after_ttl_and_grace`.
  - `the_read_back_never_judges_a_stale_body`.
- `cluster::tests::heartbeat_reads_raise_the_newest_lease_bodies`: the server's heartbeat reads feed the table that its ownership records use, and a later-issued stale read does not lower it.
- `cluster::tests::a_start_over_a_created_fence_dates_its_body_after_it`: over a created fence dated ahead at the first read, or created over no object late in the check, the first `live` body is dated at least twice the skew after the fence (both fail on the start check before).
- `sim/tests/madsim_node_lease.rs`:
  - `stale_lease_reads_never_free_a_held_record`: the record clients' reads of node leases return the body served before the newest one served, one read in three. There are never two owners and never one generation for two holds, and reads behind the newest are met and read again.
  - In `each_broken_variant_is_caught`, `StaleReadsTrusted` (the same reads judged as read) and `StaleExhaustedJudged` (an exhausted read judged anyway) both give two owners.
  - `a_read_back_judging_as_read_is_caught`: the read back judging its rival's lease as read gives two owners, rarely, so the search runs to 1,000 seeds.
  - `a_first_stale_read_is_not_closed`: a stale read older than what the bucket held when the process last read, though never given to it, does give two owners. This is the limit above.
  - `a_killed_holder_is_taken_over_under_stale_reads_and_a_clock_step`: every clock steps back 2 s, and then the holder is killed. A survivor holds the actor within the bound the waiters' test uses, on every seed.
  - `stale_lease_reads_with_a_hot_operator_never_free_a_held_record`: an operator that fences four times as often, deletes the lease before every fence and runs the full skew behind, so its created fences are dated before the bodies they follow; never two owners. Its runs have no twin: the simulation's start check reads twice where the server's polls, and a twin's whole run can fall between those reads when leases are deleted that often.
  - `a_created_fence_judged_by_date_is_caught` and `an_absent_read_keeping_the_window_is_caught`: the same runs with dates alone for created fences, or with absent answers leaving the window running, give two owners (about once in 5,000 and in 1,000 seeds; a seed that showed each is checked first, and the correct rules pass it).
  - The start bound has no broken variant here: the simulation's start check waits ttl plus grace after reading a created fence, which at its timings (1.5 s, four times the skew 1.4 s) already dates past the window, and it starts its check again over a fence written during it. The server's test above covers the bound.
- `app_format::tests`: the floor's exact body, refused forms and key.
- `cluster::tests::a_floor_above_this_version_stops_the_start`:
  - a floor above the binary's minor version, or an unreadable one: refused, and no lease written;
  - a floor written during the start check: refused after the first write, with the lease left `released`;
  - the binary's own minor version, or no record: the node starts.
- `cluster::tests::the_floor_read_after_the_first_write_waits_for_an_answer`:
  - a 7 s blip of the floor's reads: the node stays checking with its lease renewed, then starts;
  - a first write whose answer was lost: still read, still refused;
  - a stop during the read: the lease is released.

## Amendment of 2026-11-24: availability

Follow-ups of the slow-bucket amendment (2026-11-20). Each changes what
a node reads or when, or what it does with a body it already has. None
changes a lease rule: what frees an ownership record, the fence, the
start check, the trust horizon and the newest-reads table stay as they
are.

### The rule

- **The failover wait judges only what was read just now.** The heartbeat
  rounds pass every node as last read to the failover wait
  (`failover::OwnerWatch`). A node that a round kept as last read (its read
  failed) or whose body is older than one this process already read
  (`node_lease::older_than` against the newest-reads table, without
  touching that table) is not news: its entry stays as the last real read
  left it, and this node's own fence of it still holds. A kept body that
  differs from the one the watch holds was read since by another app of
  the process (the heartbeats as last read are shared) and is taken as
  read, so a fence another app found reaches this app's wait at its next
  view, although its own rounds no longer read a stopped node. A nudge
  (this node's own fence) of a node the rounds no longer read lasts until
  a read shows that node again, at worst the next listing: it only allows
  tries the acquisition then decides, and only while the watch holds the
  node `live` (or does not know it): a `fenced` or `released` entry is
  judged by its date. A `live` body the watch never saw and nothing read
  just now (an app that joins the process while that node's reads fail)
  counts as in view: nothing this app read shows that node stopped
  writing. A `fenced` or `released` one takes its date. Before, a kept
  body was judged again with a later clock. A node that still wrote but
  could not be read for a horizon looked stale, and a request for its
  actors was kept until the deadline waiting for a fence that no read
  could bring. An earlier instance's body read late looked like a restart
  to a kept request. An earlier `live` body of a fenced node undid the
  fence. Now the request is answered as it would be without the wait.
  A peer whose clock stepped back writes bodies older than the newest held:
  the watch keeps its last entry, in view, until a body not older is read
  or a fence lands (the fence watch is unchanged, and sees every fresh read).
- **At most 32 reads in flight in a round** (`HEARTBEAT_READ_CONCURRENCY`),
  landing in any order: a new read is sent as soon as any read in flight
  lands, so reads that hang do not queue behind one another, and a round
  ends in about its slowest read (at most one attempt) unless more than
  32 reads hang at once. What answered is applied as it lands, in batches
  of whatever landed together, each stamped with its earliest read's send
  time (the order rule above compares that stamp, which is never later
  than the read's own). So a read that hangs delays no other node's
  renewal: the trust in the nodes that answered is renewed while the hung
  ones are still out, and a hung node keeps the trust its last heartbeat
  gave, to that heartbeat's horizon, as before. Only rounds and early
  reads set the latency the early reads are planned on; join reads (a
  hello, a hop, a certificate's name) do not, since anyone reaching the
  cluster port can cause one.
- **A node found gone leaves the live senders at once.** The process's
  table of live senders (the sender check of hops) drops a node that no
  heartbeat as last read names any more (its object is gone), as the pins
  already did, rather than at its last heartbeat's horizon. A stopped
  (`released` or `fenced`) node is left as before, until its horizon.
- **A node found gone is forgotten** once no read issued before could
  still land: twice the longest listing limit of the process's apps (60 s
  at the defaults). Before, the heartbeats as last read kept an entry for
  every node id the prefix ever had. A listing that leaves out a node
  already found gone no longer renews that mark. A read issued before the
  mark and landing after it was dropped would show the body it read until
  the next read: its trust still ends at that heartbeat's own horizon.
- **A runtime joining a process that already holds heartbeats** (a second
  app, or a runtime restarted in place) makes its first round by key: one
  read of each node held that may still write, and of its own object. The
  listing task lists at once. Before, that round listed inline, within two
  attempts, and a slow LIST held up its view and its early reads. A process
  that holds no heartbeat yet still lists first, to find its peers before
  saying hello.
- **Over TLS, the name on an unknown certificate is read by key.** When a
  client presents a certificate whose pin is not live here, the node reads
  the common name on it (the node id, or the process's name when it has
  one, [ADR 0038](0038-several-apps-per-process.md)) and, if it is a valid
  node id, reads `nodes/<name>` by key on the join-read task, as for a
  hello. That name was not verified: it only chooses what to read, and
  trust still comes from the heartbeat read. At most four such names are
  kept a period (taking them does not renew that budget, and past it a
  certificate is not even parsed), and the join-read task reads each name
  at most once a period, counted apart from the reads signed calls ask
  for, so that a certificate naming a real node never takes the read that
  node's own hello would make. A stranger reaching the cluster port thus
  makes the node read the bucket at most four times a period. A
  process with a name serving several apps finds nothing under its name
  and waits for the listing, as before.
- **The hellos wait for a live peer of this app.** The first round of
  hellos is sent once a round finds a live node of this app in the view,
  the nodes a hello goes to. Before, any other node id as last read
  counted, `released` and other apps' nodes included: the round was then
  spent on nobody, and no hello was sent when a peer showed up later.

### Tests

- `failover::tests`:
  `a_body_kept_as_last_read_neither_starts_nor_ends_a_wait`,
  `an_older_body_read_late_neither_restarts_nor_stales_a_node`,
  `a_writer_whose_clock_stepped_back_stays_in_view_until_its_fence`,
  `a_live_body_first_shown_kept_is_in_view_and_a_stopped_one_takes_its_date`
  and `this_nodes_own_fence_counts_only_while_the_node_looks_live`.
- `cluster::tests`:
  `a_peer_kept_as_last_read_is_not_stale_to_the_failover_wait` and
  `an_older_lease_body_read_later_misleads_no_kept_request` (before: `Stale`
  and `Restarted`), `a_fence_another_app_read_reaches_this_apps_failover_wait`
  (two apps in one process), `a_round_caps_its_reads_in_flight` (before: 80 at
  once), `a_gone_node_leaves_the_live_senders_at_once`,
  `gone_nodes_are_forgotten_once_no_earlier_read_can_land`,
  `a_runtime_joining_a_process_does_not_wait_for_a_slow_listing` (LIST 3 s
  late; before, the first round waited two attempts),
  `only_live_peers_of_this_app_count_as_found_for_the_hellos`,
  `hung_reads_past_the_cap_hold_up_neither_the_round_nor_the_answers`
  (three hung reads over 70 peers: answers applied within 300 ms, the round
  in about one attempt; before, nothing applied until the round ended,
  one attempt per hung read), `join_reads_do_not_plan_the_early_reads`,
  `an_app_joining_while_a_peers_reads_fail_keeps_no_request_for_it`,
  `another_apps_older_read_misleads_no_kept_request` and
  `a_late_read_does_not_bring_a_gone_node_back`.
- `cluster_tls::tests::the_name_on_an_unknown_certificate_is_kept_to_read_by_key`.
- `tests/cluster/slow_bucket.rs`:
  `a_joining_node_is_seen_from_its_first_call_while_listing_is_slow` (LIST
  3 s late, the joining node never looked for: in the view within 2 s,
  plain and over TLS; before, over TLS, only the listing showed it).
- `sim/tests/madsim_node_lease.rs`: the waiters' reads are per node, and a
  node whose read fails is passed to the watch as kept, as the server's
  rounds do. Before, any failed read left the whole round unlearnt.
