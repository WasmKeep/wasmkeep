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
- Date: 2026-11-07
- Milestone: [M3](../../ROADMAP.md#m3-first-public-release)
- Amends: [ADR 0007](0007-replication-protocol.md),
  [0009](0009-alarms-in-a-cluster.md),
  [0012](0012-bucket-garbage-collection.md),
  [0014](0014-draining-a-node-and-moving-an-actor.md) and
  [0017](0017-moving-one-actor.md): ownership is a node lease plus an
  ownership record per actor.

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
`load actors=<n> sockets=<n> events_per_min=<n|->` (`-` until a minute of
samples exists; unknown keys and words are skipped). Readers from before
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
  while the node lease is invalid.
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
