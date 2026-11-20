# 0009. Alarms in a cluster: an index in the bucket and a sweep

- Status: accepted (implemented 2026-11-03, revised after three rounds of adversarial review the same day; index keys tagged with the owner's generation and a nonce on 2026-11-05, closing a race with deletes that land late; R4, an alarm from before joining listed before the first upload, the same day). Mutation-checked tests cover the sweep delivering an orphaned alarm, sweeps getting past stuck entries, the cursor under an escaped prefix, committed retries, backoff for handlers that reschedule and fail, a fresh backoff per alarm, recording the listed entry, stale-entry cleanup, late deletes after a takeover and after an owner's own reschedule (the alarm still delivered by a node that never ran the actor), the owner's generation in new keys, and legacy entries next to tagged ones. Not covered by tests: listing from `exec-durable`, giving the actor up after a failed listing, the timer's backoff horizon, resuming after the delivery cap, and sharding when nodes disagree; R1 to R4 and the fence are also checked under deterministic simulation (`sim/tests/madsim_alarm_index.rs`): three owners in turns on node leases (`wasmkeep_sim::node`, ADR 0020: holds, fences, releases), state uploads following replication's late-commit rule, requests applied late, deletes timing out and landing later, crashes, with the invariant that a committed alarm's named entry exists checked every 50 ms. Some nodes join with an alarm in their own data. 200 seeds per push; untagged keys, no generation check, no fence, a first upload before its entry, and clearing while the bucket has nothing for the actor are each caught by some seed (the last two with a budget of 2,000 seeds; the empty-bucket case is reached by a fault aimed at a joining node's first upload, and a separate test shows the rules keep every entry under that fault). Cluster tests cover R4 (`an_alarm_from_single_node_data_is_indexed_before_its_first_upload`) and the entry left while the bucket has nothing (`an_entry_for_an_actor_the_bucket_never_had_stays_without_creating_it`). Not modelled there: `exec-durable`'s mid-event index update, deleting an actor, the PUT retry and legacy migration (cluster tests cover those); the node's timer and mailbox deliveries are modelled on the batched hold since 2026-11-19 (see that amendment). One known exception to the invariant as the simulation states it: if recording the new entry in the database fails after its PUT, the event is still replicated naming the previous entry (at another time), while the new, unnamed entry lists the alarm; nothing is lost (that entry is delivered, then removed as unnamed).
- Amended by [ADR 0020](0020-one-lease-per-node.md) (2026-11-07): ownership is a node lease plus an ownership record per actor; where this record says lease, read the actor's hold (`LeaseView`: the hold exists and the node lease is valid). `sim/tests/madsim_alarm_index.rs` runs on node leases since 2026-11-07 (`wasmkeep_sim::node`, holds acting by the server's own rule): over 200 seeds, 1,511 takeovers, 3,993 alarms and the invariant held throughout (2,000 seeds: 15,086 takeovers); every unsafe variant is caught, within 20 seeds. On node leases a takeover during a joining node's first upload needs that node cut off and fenced (its lease keeps renewing while the upload is only slow), so "clearing while the bucket has nothing" is reached by a fault aimed at that upload (delayed at the bucket, the node cut off), under which the rules also hold.
- Amended by [ADR 0022](0022-app-owned-state-and-the-one-time-migration.md) (planned, 0.4.0): the alarm index moves under the app's root, `state/<tenant>/<app>/`.
- Amended 2026-11-14 (0.4.2): a sweep with nothing due waits until the earliest entry it listed is the sweep's, at most six sweep periods (60 s), and a change in the view of the nodes wakes it. See "Amendment of 2026-11-14" at the end.
- Amended by [ADR 0034](0034-batched-plain-writes.md) (proposed 2026-11-15; if accepted): in a batched class an entry is named at each event but PUT once per segment, before its upload; entries are retired only after that segment's acknowledgement; the fence is judged on the acknowledged head.
- Amended 2026-11-19 (0.7.0): an event's end leaves a delivery in flight for the same time alone, a covered or dropped delivery is delivered again, a local redelivery of a time the hold already ran skips the stale-hint clear, and that clear's DELETEs run off the mailbox. See "Amendment of 2026-11-19" below.
- Date: 2026-11-03
- Milestone: [M3](../../ROADMAP.md#m3-first-public-release)

## Context

An actor's alarm lives in its own database (`_wasmkeep_alarm`), which
replication carries to the bucket. What makes it *fire* is each node's
in-memory timer, filled from events the node runs and, at startup, from a
scan of its local data directory.

In a cluster that left a gap: no node knew about alarms of actors it had
never run. If the node holding an idle actor with a pending alarm died for
good (or, since clean shutdowns hand actors over, was removed), the alarm
never fired until some request happened to reach the actor. Separately, a
node that was not the owner re-armed the alarm every lease period forever,
and a failing handler's retry schedule lived only in memory, so it reset on
hibernation.

## Decision

The actor's database stays the source of truth. The bucket gets an index of
pending alarms that any node can read cheaply, and every node sweeps it.

1. **Index entries.**
   `alarms/<tenant>/<due ms, 15 digits>/<class>.t/<name as hex>/<generation, 20 digits>-<nonce as hex>`,
   empty objects. Due time
   first and zero-padded, so a listing (S3 lists in key order) yields the
   earliest alarms first; the name as hex, so nothing in the key is escaped
   by the object store (some stores percent-encode or reject characters in keys). The last segment (the tag) is the lease
   generation of the owner that wrote the entry and a nonce that process
   never used before, so every write uses a new key. Entries written before
   tags existed are `alarms/<tenant>/<at>/<class>/<name as hex>`
   ("legacy"); readers accept both. Tagged entries sit in a sibling folder
   (`<class>.t`; class names have no `.`), never under a legacy key: no key
   is both an object and the folder of others, which MinIO cannot hold. The key
   format and the rules below are in `wasmkeep_cluster::alarm_index`, pure
   functions the simulation can share.
2. **The owner keeps the index equal to the committed alarm.** After an
   event, the owner lists the committed alarm *before* replicating the
   event, and removes the entry it listed before *after* replicating it, so
   a crash in between leaves an extra entry, never a missing one. The exact
   entry it listed is recorded in the database (`alarm_indexed` meta:
   `<at>/<tag>`, empty for none; a bare `<at>` is a legacy entry), so the
   next owner, on any node, knows which entry stands for the alarm. An
   unchanged alarm keeps its entry, whichever owner wrote it; a legacy one
   is replaced by a tagged one on the next event (migration). Only while
   its lease is valid and replication is healthy, so what the database
   names is in the bucket. If the index cannot be brought in line (the
   PUT fails, the recorded entry cannot be read or written, the lease
   lapsed), the node gives the actor up, as for a failed upload, rather
   than replicate an alarm no other node could find. `exec-durable`, which uploads in the middle of an event,
   lists a pending alarm first too, under the generation of the replica it
   uploads with. Three rules make a delete that lands late (an S3 retry, a
   slow network) harmless:
   - **R1. A fresh key for every PUT.** No entry is ever written again
     (except the immediate retry of the same PUT), so no delete sent
     earlier can remove a newer entry.
   - **R2. Delete only what the replicated state no longer names.** The
     entry an event replaced, once that event is replicated; or, when an
     alarm delivered for a time no longer matches the committed alarm, the
     actor's entries at that time other than the one the database names.
     Entries older owners wrote there are named by no state: the owner
     restored the latest, and keys are never reused. "The latest" holds only
     once the owner's own upload ends the history: until then an earlier
     owner's upload that lands late is on no chain anyone built on, so it
     becomes the state after all (a late commit, ADR 0007), and it may name
     an entry about to be removed. So an owner that has uploaded nothing yet
     uploads a one-line change (`owner_fence` meta) before it removes
     entries as stale (found in review): at most one
     extra upload per hold. An owner that found nothing in the bucket for
     the actor, and has uploaded nothing since, removes nothing: an earlier
     owner's first snapshot can name an entry (R4) and still land. It does
     not fence by uploading either: that would create the actor in the
     bucket, empty, and the joining node whose snapshot never landed would
     later restore that empty state over its own data (no sidecar matches,
     so the bucket's version is downloaded and its own file set aside as
     `.replaced`). It retries such an entry after the longest backoff
     (`retry_max`, 5 min) rather than on every sweep, and releases the
     actor's lease at once: held until the idle timeout, it would route
     client requests to this node, whose first would create the actor
     empty in the same way (as any request reaching another node first
     already can, see [cluster](../cluster.md): start the node with the
     data first).
   - **R3. Never delete an entry with a generation above one's own.** A
     newer owner wrote it (the owner's lease ran out between its check and
     the listing).
   - **R4 (not about deletes). An alarm from before joining is listed before the first
     upload.** A node that ran on its own and joins a cluster uploads each
     actor's database as its first snapshot when the bucket has nothing for
     it. If that database holds an alarm, the node lists it under a fresh
     key and records the entry in the database first, so the snapshot names
     an entry that exists; if it cannot, nothing is uploaded and the alarm
     stays on that node only. (Before 2026-11-05 the snapshot went first:
     a node cut off before the actor's first event ended left an alarm in
     the bucket that no entry listed.)
3. **Retries are committed.** When `on-alarm` fails, whatever alarm the
   handler left due before the retry time (the same one, or one it set for
   now) moves to the retry time (1 s, doubling to 5 min); one it set later,
   or a clear, stands. The failure count is in the database, read before the
   handler runs; setting or clearing an alarm outside a failed run resets
   it. So the backoff survives hibernation and moves, a handler that
   reschedules itself and fails still backs off, and the index lists the
   retry time: no sweep delivers it early.
4. **Sweep.** Every `alarm_sweep` (10 s; less often when nothing is due,
   amendment of 2026-11-14) each node reads its tenant's index
   up to "now minus a lease period plus grace", stopping at the first entry
   not that overdue. It reads at most 2048 entries and resumes after the
   last one next time, so entries nobody can act on (a class no node serves,
   an actor that cannot start) cannot hold the rest back. The live nodes
   (fresh heartbeats) share the entries by a hash of the address, so a dead
   node's alarms are recovered in parallel; an entry overdue three times
   longer is anyone's. Each node delivers at most 256 per sweep through its
   timer, where a time it already holds earlier, or within the longest
   backoff (a retry), wins. Its own entries go first; anyone's entries the
   walk met keep an eighth of the 256, taken from a random rotation, and
   take what own entries leave; entries the node already has in flight or
   retrying do not count (`plan_sweep`; an earlier walk, first come, first
   served, had every surviving node deliver the same anyone's entries). Delivery is the ordinary alarm event: take the
   actor (restoring it from the bucket), read the committed alarm, run
   `on-alarm` if due. Entries at a time that does not match the committed
   alarm are stale and are removed (R2, R3), after the committed alarm is
   indexed; that costs one LIST of the actor's tagged entries at that time
   (the legacy key is deleted by its exact name). This works without the
   instance too: after a trap, or once `on-alarm` deleted the actor, the
   entry spared is the one the replicated state names (none, after a
   delete). An entry delivered for an actor with no state (no committed
   alarm and no `on-init` recorded: deleted since, or never initialised)
   is removed without running the actor, which would only create it
   again.
5. **A node that cannot run an alarm** because another node owns the actor
   drops it: the owner has it, committed and indexed. If the bucket is
   unreachable it retries after a lease period. (A first version listed the
   alarm in the index instead; the review showed that a stale local copy of
   a deleted actor could then bring the actor back.)

## Consequences

- An alarm whose owner dies fires on another node a lease period plus grace
  after it was due, plus up to one sweep: about 25 s with the defaults
  (since the amendment of 2026-11-14, for an entry the survivors' sweeps
  had listed, or written before the owner left their view; see there for
  the one case that waits up to 75 s).
- One extra PUT per event that changes the alarm (plus a DELETE of the old
  entry); none for events that leave it alone, even under a new lease
  generation. A sweep reads only overdue entries.
- An entry left behind (a crash between the PUT and the upload, a PUT that
  landed after it was given up) costs one wake-up when it comes due, then
  is removed. For an actor deleted since, the wake-up restores its empty
  database only to read it; the actor is not run.
- An alarm set by a plain `exec` event is as durable as that event's writes.
- During retries `get-alarm` returns the next attempt's time.
- Single-node servers keep their behaviour, except that retries are now
  committed (and so survive restarts).

## What can still go wrong

- Alarms set before this change, on actors no event reaches, have no entry
  until an event moves them. (Pre-alpha: no such deployments.)
- During a rolling upgrade, nodes still on the old code change alarms
  without updating the index. Nodes from before tags (2026-11-05) skip
  tagged keys when sweeping (and read the whole index each sweep, since
  they never reach a key they parse as not yet due), read a tagged
  `alarm_indexed` as "nothing listed" and add a legacy entry, and keep the
  unfenced late-delete race for legacy keys: roll all nodes forward
  together. Rolling back leaves alarms of idle actors that only have
  tagged entries invisible to the old sweeps until an event reaches the
  actor. Old nodes also never remove tagged entries, and leave a tagged
  `alarm_indexed` in place when they clear an alarm, so each actor that
  had an alarm when the rollback started keeps one tagged entry until the
  cluster rolls forward. While the rollback lasts, old sweeps step over
  the past-due ones on every sweep. After rolling forward, each leftover
  costs one or two wake-ups (within the usual 256 per node per sweep) and
  is then removed, since its generation is lower.
- Entries for classes no node serves stay in the index (a few bytes each);
  sweeps step past them.
- An entry listed by a joining node that died between the PUT and its
  first upload (R4) stays while the bucket has nothing for the actor: a
  wake-up every `retry_max` on the node that delivers it. It resolves when
  that node comes back and uploads the actor (the alarm then fires as
  usual), though that node lists a new entry of its own, and the old one
  goes only on a sweep after the alarm fired; if the node never comes
  back, the entry stays. In the same corner, an alarm whose first snapshot
  lands late can fire up to `retry_max` late. Each failed attempt at a
  first upload (the PUT, recording it, or the snapshot) leaves one more
  such entry, all at the alarm's time, each cleared after it fires.
- Recovery speed: each live node delivers up to 256 alarms per sweep (10 s),
  so a dead node's backlog drains at about 25 alarms per second per live
  node.
- Bounded delays: an entry whose shard falls to a node that does not serve
  its class, or while nodes disagree about who is alive, waits until it is
  three margins overdue (about 45 s with the defaults); an orphaned alarm
  can wait behind a stale registration on the sharded node for up to the
  longest backoff (5 min).

## Alternatives

- **A conditional delete of the observed object version**: the
  object_store version we pin takes only a path for a delete (no
  precondition), and sends it to S3 as a DeleteObjects request.
- **Deleting only after a settle time** longer than any request can be in
  flight: it adds a clock assumption (the skew between any two nodes is
  up to twice the per-node bound), a new table hidden from guest SQL, and
  more code paths than tagged keys.
- **Every node scans every actor's database in the bucket**: one GET per
  actor per sweep; does not scale.
- **The due time in the object body, or last in the key**: a sweep would
  have to read every entry, due or not (the first version did this; the
  review caught it).
- **A per-node list of owned actors**: the dead node cannot hand it over.

## Amendment of 2026-11-14: the sweep waits when nothing is due

An idle node listed its alarm index every 10 s, 0.1 LIST/s per node,
whether the index held anything or not, and on R2 and S3 a LIST costs
what a PUT costs. What a sweep can deliver is known from its own listing:
it stops at the first entry not overdue, and that entry becomes the
sweep's at its due time plus a lease period plus grace.

### The rule

`alarm_index::next_sweep_wait_ms`, used by `sweep_alarms` and by
`sim/tests/madsim_alarm_sweep.rs`:

- After a sweep that listed entries due (delivered or not), stopped at
  its scan cap, resumed after an earlier page (whose skipped entries stay
  due), or failed: the next sweep a period later (`alarm_sweep`, 10 s), as
  before.
- After a sweep with nothing due: when the earliest entry it listed
  becomes the sweep's, but at least a period and at most
  `IDLE_SWEEP_FACTOR` (6) periods later, so 10 to 60 s; 60 s with an
  empty index.
- At once when the node's view of the nodes changes (a node joined,
  left, went stale, was fenced, released, or restarted as a new
  instance), at most one sweep a period: that is when a dead node's
  entries need the sweep. The view is refreshed every heartbeat period
  ([ADR 0020](0020-one-lease-per-node.md), amendment of the same day).

### What still holds

- The index, its keys and R1 to R4 are unchanged; only when a sweep runs
  changed. Sharding, the scan and delivery caps, and anyone's entries are
  unchanged.
- An entry the sweep had listed is delivered at the latest a period after
  it became the sweep's, as before: about 25 s after its due time with
  the defaults. So is an entry of a node that dies, written before the
  survivors saw it leave: its departure wakes their sweep, which lists the
  entry and waits for its turn.

### What got slower, worst case at the defaults

An entry written after a node's last sweep, due before the earliest
entry that sweep listed, whose owner stays alive and in view but never
fires it (a timer lost to a bug, the case the sweep backstops; the
common cases, an owner that died or a class nobody serves, are covered
above and by the backlog rule): found by the next idle sweep, up to 60 s
after the previous one, so up to 75 s after its due time instead of
25 s. An index with entries coming due keeps the sweeps at their due
times: an alarm actor firing every 15 s on a live node costs its peers
about one listing every 15 to 30 s, against one every 10 s before.

### Cost after

Per node: one LIST a minute on an empty index, 0.017 LIST/s from 0.1.
Measured in `tests/cluster/listings.rs`
(`an_idle_node_lists_and_reads_what_its_cadences_say`: 10 sweeps in 120
heartbeat periods with a 200 ms sweep period, the six-period wait).

### Rolling upgrades

A node of 0.4.1 sweeps every 10 s as before; the sweeps of the two
versions share the index as they did, by the same hash.

### Tests

- `sim/tests/madsim_alarm_sweep.rs` spaces its sweeps by the rule, a
  change in the lingering node's view waking them. Its liveness and rate
  checks hold at 200 seeds; a light run (no backlog, agreeing views,
  alarms coming due over 20 s) checks the latency the rule promises,
  each entry delivered within a period (and the drift and the delivery's
  own time) of becoming the sweep's
  (`an_idle_sweep_delivers_each_alarm_within_a_period_of_its_turn`), and
  an idle sweep that always waits its longest, ignoring the earliest
  entry, is caught (`an_idle_sweep_that_ignores_the_earliest_entry_is_caught`).
- `tests/cluster/listings.rs`, `a_view_change_wakes_the_alarm_sweep`: a
  peer's clean stop makes the survivor sweep within five heartbeat
  periods, where the idle wait is twelve; with the wake-up removed the
  test fails.
- `alarm_index::tests::an_idle_sweep_waits_for_the_earliest_entry_within_bounds`.
- The cluster tests of orphaned alarms (`tests/cluster/alarms.rs`) pass
  unchanged.

## Amendment of 2026-11-19: one delivery per alarm round

On a busy actor every round of its alarm was delivered several times. A
large room, its mailbox always busy, ran several stale-hint clears for each
alarm round, each a large share of a second inside the mailbox: most of
the mailbox's time went to them, and its publishes waited behind. The timer re-registered the
committed alarm at the end of every event, even when the delivery it had
already handed to the mailbox was for that time; each event that ended
while the round's delivery waited in the queue queued another. The first
ran the round and moved the alarm on; each of the others found its time no
longer committed and cleared it as a stale hint (section 4: on a batched
hold a flush to the barrier, the fence, a LIST and DELETEs).

### The rule

Pure decisions in `alarm_index`, used by the timer (`alarm.rs`) and by
`sim/tests/support/batched.rs`:

- **An event's end leaves a delivery in flight for the same time alone**
  (`delivery_covers`; the timer's `sync`). The mailbox runs events in
  order, so that delivery runs after the event and reads the alarm it
  committed. A different time (a move while the delivery waits, or a
  clear and a new time) registers as before, and the old delivery's late
  completion or retry changes nothing (its identity no longer matches).
  During an admission backoff nothing is in flight, so a sync registers
  anew as before. Other registrations (the startup scan, restores, the
  backoff after a failure, an alarm moved later by its own delivery) keep
  replacing whatever is there, as `a_new_committed_time_supersedes_pending_and_inflight_retries`
  tests.
- **A covered delivery whose run registered nothing is delivered again**
  (`deliver_again`). Its result and the event's sync reach the timer from
  different tasks, so the run may have ended before the commit, or without
  running `on-alarm` (another owner, a failure); nothing else would arm the
  committed alarm, and a single node has no sweep. A run that ran the
  round moves or clears the alarm itself, which replaces the registration,
  so its completion changes nothing.
- **A delivery the mailbox dropped is delivered again**, with the
  admission backoff (100 ms doubling to 5 s): the actor failed to start, or
  the event stopped before answering. Before, it was forgotten until the
  actor's next event (which, for the same time, it now covers) or a sweep.
  A delivery that ran, or any delivery while the node shuts down,
  completes as before. Counted in
  `wasmkeep_alarm_dropped_deliveries_total`.
- **A local delivery of a time this hold already ran `on-alarm` for skips
  the stale-hint clear** (`stale_clear_skippable`, the latest such time
  and the hold's lease generation). The entry its state named then is
  retired by its exact key once the move is replicated (R2; ADR 0034,
  section 9, on a batched hold), so the LIST would find nothing of this
  hold's. Such a delivery still happens after a legitimate move (the alarm
  cleared and set again while a delivery waits, or a stale delivery that
  ran the committed round while that round's own registration was
  queued). A delivery from the index (the sweep) always clears: the
  entries it lists at that time are named by no state (an earlier owner's,
  or one whose exact-key DELETE failed or was dropped with a failed
  segment), and only it removes them.
- **The stale-hint clear's DELETEs run off the mailbox**, as ADR 0034,
  section 9 allows: the LIST and the choice of entries stay serialized with
  the hold's next event; a DELETE landing late cannot remove a newer entry
  (R1).

### What still holds

- R1 to R4 and the fence are unchanged; clearing fewer stale hints never
  removes an entry a state names. A restorable committed alarm still has
  its entry: nothing here writes or deletes an entry the rules did not
  already choose.
- A batched hold judges R2 only on its acknowledged head: the belt skips a
  clear, it never judges one.
- An entry no state names at a time the actor keeps re-arming is cleared
  as before only by a delivery from the index, which the owner's own sweep
  defers while a registration of the actor is due within the longest
  backoff (section 4). That is unchanged: before, such entries were often
  cleared by the duplicate deliveries this amendment removes. They cost a
  line in each sweep's listing until the actor goes quiet.

### Measuring it

`wasmkeep_alarm_deliveries_total{tenant,app,class,outcome}` counts the
deliveries a mailbox ran: `ran`, `stale`, `duplicate` (a stale hint the
belt skipped) and `deferred`. Stale per ran was well above one on such a
room before; it should be near zero after ([monitoring](../monitoring.md#alarm-deliveries)).

### Tests

- `tests/alarms.rs`,
  `events_that_end_while_a_delivery_is_queued_do_not_queue_more`: an
  alarm comes due behind a busy mailbox and six fetch events; one
  delivery, no stale hint. Before the change it saw stale hints.
- `alarm.rs`: events ending with the in-flight time queue no second
  delivery; a move while a delivery is in flight registers the new time;
  a covered delivery that registered nothing is delivered again, in
  either order of its result and the sync; a dropped delivery is delivered
  again with backoff, not counted as an admission retry; only the sweep
  marks deliveries as from the index. The existing tests keep their
  assertions (`Retry` gained its drop flag).
- `sim/tests/madsim_alarm_index.rs`,
  `batched_deliveries_keep_one_per_round_and_the_index_invariants`: the
  timer and mailbox on ADR 0034's batched hold, with refused, dropped and
  deferred deliveries, results racing the next events' syncs, moves while
  a delivery waits, failed segments and entry PUTs, crashes and cuts, and
  at the end a delivery from the index for every entry left at a time no
  state names. At 200 seeds no second delivery of a round, no committed
  alarm left unarmed once every result is in, no orphan left after the
  final deliveries, and the model's index and replication invariants hold
  throughout. Its uploads fail (segments and entry PUTs) but are not
  stalled, and completions are not delayed across ownerships: the other
  workloads of the model judge those, and their loss bound is stated for
  their event pattern, not this mailbox's. Each rule removed is caught within 200 seeds:
  re-registering the in-flight time (a second delivery runs stale),
  forgetting a covered delivery, forgetting a dropped one (the committed
  alarm unarmed), and the belt skipping deliveries from the index (an
  orphan left).
