# 0009. Alarms in a cluster: an index in the bucket and a sweep

- Status: accepted (implemented 2026-11-03, revised after three rounds of adversarial review the same day; index keys tagged with the owner's generation and a nonce on 2026-11-05, closing a race with deletes that land late; R4, an alarm from before joining listed before the first upload, the same day). Mutation-checked tests cover the sweep delivering an orphaned alarm, sweeps getting past stuck entries, the cursor under an escaped prefix, committed retries, backoff for handlers that reschedule and fail, a fresh backoff per alarm, recording the listed entry, stale-entry cleanup, late deletes after a takeover and after an owner's own reschedule (the alarm still delivered by a node that never ran the actor), the owner's generation in new keys, and legacy entries next to tagged ones. Not covered by tests: listing from `exec-durable`, giving the actor up after a failed listing, the timer's backoff horizon, resuming after the delivery cap, and sharding when nodes disagree; R1 to R4 and the fence are also checked under deterministic simulation (`sim/tests/madsim_alarm_index.rs`): three owners in turns on node leases (`wasmkeep_sim::node`, ADR 0020: holds, fences, releases), state uploads following replication's late-commit rule, requests applied late, deletes timing out and landing later, crashes, with the invariant that a committed alarm's named entry exists checked every 50 ms. Some nodes join with an alarm in their own data. 200 seeds per push; untagged keys, no generation check, no fence, a first upload before its entry, and clearing while the bucket has nothing for the actor are each caught by some seed (the last two with a budget of 2,000 seeds; the empty-bucket case is reached by a fault aimed at a joining node's first upload, and a separate test shows the rules keep every entry under that fault). Cluster tests cover R4 (`an_alarm_from_single_node_data_is_indexed_before_its_first_upload`) and the entry left while the bucket has nothing (`an_entry_for_an_actor_the_bucket_never_had_stays_without_creating_it`). Not modelled there: `exec-durable`'s mid-event index update, deleting an actor, the PUT retry, legacy migration and timer delivery (cluster tests cover those). One known exception to the invariant as the simulation states it: if recording the new entry in the database fails after its PUT, the event is still replicated naming the previous entry (at another time), while the new, unnamed entry lists the alarm; nothing is lost (that entry is delivered, then removed as unnamed).
- Amended by [ADR 0020](0020-one-lease-per-node.md) (2026-11-07): ownership is a node lease plus an ownership record per actor; where this record says lease, read the actor's hold (`LeaseView`: the hold exists and the node lease is valid). `sim/tests/madsim_alarm_index.rs` runs on node leases since 2026-11-07 (`wasmkeep_sim::node`, holds acting by the server's own rule): over 200 seeds, 1,511 takeovers, 3,993 alarms and the invariant held throughout (2,000 seeds: 15,086 takeovers); every unsafe variant is caught, within 20 seeds. On node leases a takeover during a joining node's first upload needs that node cut off and fenced (its lease keeps renewing while the upload is only slow), so "clearing while the bucket has nothing" is reached by a fault aimed at that upload (delayed at the bucket, the node cut off), under which the rules also hold.
- Amended by [ADR 0022](0022-app-owned-state-and-the-one-time-migration.md) (planned, 0.4.0): the alarm index moves under the app's root, `state/<tenant>/<app>/`.
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
4. **Sweep.** Every `alarm_sweep` (10 s) each node reads its tenant's index
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
  after it was due, plus up to one sweep: about 25 s with the defaults.
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
