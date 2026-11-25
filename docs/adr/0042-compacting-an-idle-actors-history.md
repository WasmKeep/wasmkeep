# 0042. Compacting an idle actor's history

- Status: accepted (2026-11-24), shipped in 0.8.0. Slices 1 to 3 (this
  record, the rule `idle_collectable` and the simulation), slice 4 (the
  idle snapshot, "As built: the idle snapshot") and slice 5 (the idle
  pass, "As built: the idle pass") are in 0.8.0, with
  `--no-idle-compaction` to turn both off. From slice 6, the
  documentation in `docs/cluster.md` and `docs/actor-data-access.md` is
  in; the amended sentences of ADR 0012 and ADR 0033 and the change
  feed measured again after bursts are not, and follow. The pass has
  been tested, and not yet watched on a cluster left running longer than
  its retention window. In 0.8.1: `backup create --history all` no
  longer fails a run that races a pass, and the backup and
  recorded-head simulations are in ("As built: the backup retry and the
  last simulations"). Also in 0.8.1: a pass stopped at its deadline is
  continued by further passes until the history is compact ("As built:
  continuing a stopped pass"), after a cluster left at rest showed the
  two largest actors losing one batch each and stopping.
- Milestone: `0.8.0` (see [Built](../../ROADMAP.md#built) in ROADMAP.md)
- Follows: ADR 0007 (chained segments), ADR 0012 (collection), ADR 0020 (holds and generations), ADR 0028 (idle expiry's acquisition), ADR 0033 (offline readers, the retention window), ADR 0034 (barriers, snapshots only at the acknowledged head), ADR 0035 (recorded head), ADR 0039 (the change feed)
- Amends: ADR 0012, decision 3 on idle actors ("acceptable for now"); its sentence is still to be updated

## Context

An actor's history is `wal/<txid>-<generation>-<parent>.ltx` segments and `snapshot/<txid>-<generation>.ltx` snapshots (ADR 0007). The owner snapshots every 64 segments (`SNAPSHOT_EVERY`, `replica.rs`) and then collects what the snapshot makes unnecessary (`collectable` in `wasmkeep-cluster/src/replication.rs`, `Collector::collect` in `replica.rs`, `spawn_collection` in `actor/collection.rs`).

Collection follows writes. ADR 0012 decided that an actor that stops writing keeps its last interval (fewer than 64 segments) plus whatever was younger than the retention window at its last pass, until it writes again. For an actor that wrote a burst and then went idle, that is every object of the burst, for good.

The change feed lists every history object of an app at each poll (ADR 0039, section 9: `⌈K/1000⌉` LIST requests for `K` objects). Listing is parallel in 0.7.0, but `K` still counts the leftovers of every idle actor.

Constraints found in the code and records:

1. **The generation guard.** `collectable(listing, owner_generation, snapshot)` returns nothing unless the snapshot's generation is the owner's and the segment at `P` is listed. A hold that only read cannot collect. ADR 0012, condition 2, has the interleaving that makes this necessary.
2. **A snapshot key names the position's generation, not its uploader.** A restored replica starts `since_snapshot` at the plan's segment count, so a non-writer can upload a snapshot at a previous owner's head.
3. **Hibernation releases the hold.** `Drop for ActorState` (`actor/state.rs`) releases or parks it; a `Collector` stops when its `LeaseView` is no longer valid.
4. **The retention window is a promise.** ADR 0033, section 1, rests on it: the readers' retry bound (`2d + ε < W`), point-in-time reads "over roughly the last `--history-retention-secs`", the undo of an `actor restore`, and the delete-request bound. `docs/cluster.md` and `docs/actor-data-access.md` say the same.
5. **The feed compares the head.** ADR 0039, section 3, treats an actor as unchanged when the head's position and the head object's identity are the recorded ones. A moved head costs a full read and a re-emit, and `rebase` with reason `unproven` once the ancestry is gone.
6. **The step before a drop is cancelled by an arriving job.** In `wasmkeep-runtime/src/mailbox.rs` the step races `rx.recv()` and the job wins. `before_drop` (`actor/state.rs`) returns `Ready` at once for a strict hold.
7. **A key does not name one content for good.** After `app delete` and `deploy --recreate`, generations restart at 1 and keys recur (ADR 0033, Context).
8. **The ownership record's last-modified time is idle expiry's clock** (`wasmkeep-cluster/src/expiry.rs`). A version 2 record carries `idle-since` through a rewrite; a version 1 record does not.

## Decision

Compaction is **the existing snapshot plus the existing collection, run for an actor that stopped writing**. It replaces nothing in place, merges no segments, and adds no object format, key or record field.

### 1. What is replaced and what is kept

After compaction an idle actor's prefix holds two objects:

- `S`, a snapshot at the head `P = (g, t)`;
- the segment at `P`.

Everything else is deleted: every segment or snapshot with a txid below `t`, and every one of a generation below `g`. This is exactly `collectable`'s set, and the same shape a delete leaves (ADR 0012, "Deleted actors").

The segment at `P` is kept for ADR 0012's reason: the planner takes the head from segments first, so without it a late segment of an earlier generation becomes a head whose parent is gone.

**Invariant: compaction never changes the head position or the head object.** The feed therefore sees a compacted actor as unchanged and reads nothing.

### 2. The idle snapshot, at hibernation

When an actor leaves memory for `DropCause::Idle` or `DropCause::Evicted`, its owner uploads a snapshot at the head if all of these hold:

- this hold wrote the head (`replica.head_generation() == replica.generation()`);
- replication has not failed, and the hold is at a `Flushed` barrier (ADR 0034, section 6): nothing in flight, nothing uncaptured;
- no snapshot is at the head already;
- it is worth it: at least `IDLE_SNAPSHOT_EVERY` segments since the last snapshot (proposed 8, counted across holds as `since_snapshot` already is), or at least one segment and a database of at most 256 KiB.

The threshold bounds the extra PUTs for an actor that writes once per wake. With it, an actor at rest holds at most 2 + 7 objects; with a threshold of 1 it holds exactly 2. Both numbers are starting values to measure.

It is a new `SnapshotKind::Idle`. Like `AfterDelete`, it leaves the periodic schedule alone (no `snapshot_failed()` back-off). Like `Periodic`, it gets one bounded attempt (`periodic_snapshot_bound`).

It must run as an upload flight off the mailbox, as a batched hold's periodic snapshot does (`start_periodic_snapshot`, `PeriodicSnapshot::run`, `PeriodicFlights`), in both modes. Reason: the step is cancelled when a job arrives, and a cancelled inline PUT can still land. Recorded as a flight, the upload goes on, and the node's resets, deletes and barriers wait for it (ADR 0034, section 7), so a snapshot of erased data cannot land after the erasure on that node. If a job arrives, hibernation is abandoned as today and the outcome is taken in at the next apply (`settle_periodic`).

Not taken on a drain's hand-over, a move, or a clean stop: the actor is not idle there, or the deadline is the stop's. (Checked in slice 4; see "As built: the idle snapshot". A drain and a move do reach `before_drop` with `Evicted`, but only after their own job ended the hold, so there is no hold to take it.)

Nothing is deleted at hibernation.

### 3. The idle pass, one retention window later

The node that hibernated the actor remembers it: actor, generation `g`, and the version of the ownership record its release wrote. The entry is due at release time plus the retention window plus the skew bound.

When due, a job runs in the actor's mailbox, with no guest code, modelled on `expire_idle` (`actor/expiry.rs`):

1. Skip if the actor is resident, held or parked here, if the node drains, or if the app is deleted or restoring.
2. Acquire with `Owners::acquire_unchanged` on the remembered version. It takes the record only if it names nobody and is unchanged. A wake since, an app delete, a recreate or a whole-app restore makes it fail, and the entry is dropped.
3. Read the generation `g` of the released record it replaced. Nothing is restored, activated or uploaded.
4. List the prefix. Take `S`, the newest snapshot of generation `g` whose segment is also listed. If there is none, release and stop.
5. Delete `collectable(listing, g, S)`, honouring the retention window by `last_modified`, in batches, checking the new hold's `LeaseView` before each: `Collector::collect`, built with generation `g` and the new hold's view. Steps 4 and 5 are `idle_collectable(listing, g)` in `wasmkeep-cluster/src/replication.rs` (slice 2). As built, `Collector::collect` takes the snapshot as an argument and lists the prefix itself, so the pass cannot use it unchanged without listing twice: slice 5 gives it a variant that applies `idle_collectable` to its own listing (one listing, as costed in section 4).
6. Release, preserving the inherited head requirement and the idle origin (`acquire_unchanged_prepared` with `idle_origin_ms`).

**The rule, in one sentence.** A holder that acquired a *released* record of generation `g` unchanged, and uploads nothing, may delete `collectable(listing, g, S)` for a listed snapshot `S` of generation `g`.

**Its continuation (0.8.1).** The same holds for a holder that acquired unchanged a released record of generation `g + k` written by the release of a pass for `g` (as this rule, or this continuation, for `k - 1`), when the node knows that pass to be its own: every hold after `g` was then a pass, and no pass uploads. See "As built: continuing a stopped pass".

**Why it is safe.** ADR 0012's proof needs only that `S` and the segment at `P` were acknowledged by the generation that wrote `P`. It never uses who sends the deletes; it already covers deletes that land late.

- No hold existed after `g`: every acquisition writes a new generation, and the record still says `g`, released. (Checked in the code, see "Confirmed in the code" below.)
- So only `g` can have uploaded a snapshot keyed with generation `g`. No earlier hold could name that position, and no later hold exists. This closes constraint 2.
- `g` snapshots only at its acknowledged head (`refuse_unless_at_head`). So `(g, t)` was acknowledged by `g`.
- Every later generation lists after acquiring, sees `S` and `P`, and builds on them. Uploads of earlier generations lose to the segment at `P`. A late upload of `g` itself is above `t` and kept.

The memory entry is only a hint for when to try. Safety rests on the record and the listing the pass reads itself, so a stale or mistaken entry cannot delete wrongly. (A continuation's `g` comes from the node's memory of its own pass; what proves that no other hold came since is again the record it acquires unchanged.)

**What the hold is for.** Not for the chain's safety. It excludes a concurrent wake for the length of the pass, keeps `Collector` and its lease check as they are, and ties the pass to the app's current incarnation as today's passes are tied (constraint 7).

### 4. Triggers and limits

- Trigger: time since release (the retention window). The idle snapshot's trigger is segments or bytes since the last snapshot. Age alone triggers nothing.
- The queue is in memory, bounded (proposed 100,000 entries, the oldest due dropped first; as built, a full queue refuses a new actor instead, see "As built: the idle pass"), a few passes at once per node (proposed 4, as `expiry::PARALLEL`).
- A class with `expire-after-idle` in an app on version 1 ownership records is not compacted: the pass would restart its idle clock (constraint 8).
- A pass that fails is retried a few times inside its hold, then given up.
- A kill switch: `--no-idle-compaction`. This deletes history.
- Metrics: `wasmkeep_idle_snapshots_total{outcome}`, `wasmkeep_idle_compactions_total{outcome}` (`done`, `changed`, `nothing`, `refused`; as built also `busy`, and from 0.8.1 `continued` and `stopped`); deletes count in `wasmkeep_history_collected_total`.

Cost per compacted actor: one snapshot PUT (the database), two ownership PUTs, one LIST, one bulk DELETE per 1,000 objects.

## The open questions, answered

### 1. What compaction means here

A prefix of the chain is replaced by one snapshot at the head: section 1. Segments are not merged. A merged segment would span a txid range, which the key format (`wal/<txid>-<generation>-<parent>`) and `Listing::from_keys` cannot name; that is a new format for no gain over a snapshot.

### 2. Who compacts, and when

- **The owner** uploads the snapshot, at hibernation, while it holds the actor.
- **The same node, under a fresh hold**, deletes one retention window later.
- **Not a sweep, not the CLI.** Either would have to write a segment to pass the generation guard, which moves the head (constraint 5). The CLI holds bucket credentials only and cannot hold an actor.
- **Node lease (ADR 0020).** The pass holds the actor like any hold: a lapsed or fenced node lease ends it, and its deletes stop at the next batch. Deletes that land late are covered by ADR 0012's proof.
- **Batched writes (ADR 0034).** The snapshot is taken only at a `Flushed` barrier, so at the acknowledged head with nothing in flight. The pass uploads nothing.
- **A concurrent wake.** Before the pass: the record changed, the pass is dropped. During the pass: an event sent here queues behind the job in the mailbox; an event elsewhere is forwarded here, as for an expiry job. After: the next wake restores `S` and no segment.
- **A move or drain.** No idle snapshot on those paths. A draining node runs no pass (`ensure_owner_with` answers `Elsewhere`). The target's hold changes the record, so the source's entry is dropped.
- **An operator restore.** `actor restore` is a new write by the owner (ADR 0033, section 6). The next hibernation snapshots it; the pass a window later deletes the earlier history, which is what the documented "ordinary retention window" already says.
- **A whole-app restore, an app delete.** Both remove ownership records (ADR 0035, "As built"), so `acquire_unchanged` never takes one.

### 3. Crash safety and ordering

Order of writes:

1. The segments, acknowledged (already there).
2. The idle snapshot, one conditional create. It publishes itself: a restore uses it once it is whole, and until then plans from the previous snapshot.
3. The release.
4. A window later: the acquisition, then the deletes, then the release.

A crash at any point leaves a restorable chain:

- Before or during 2: no object or a whole one, at a position on the chain.
- Between 2 and 4: more objects than needed, nothing missing. The entry is lost with the process; the leftovers stay until the actor next writes and rests.
- During the deletes: every remaining set still plans to the same head, since nothing at or above `S` is ever in the set.
- After the acquisition and before the release: the record names a dead hold, as after any crash.

Readers:

- **Restore.** Plans `S` and zero segments. The recorded head of ADR 0035 is the exact position `P`, which is kept.
- **Backup, export, `actor read`.** ADR 0033's retry argument holds unchanged, because the pass deletes only objects older than the window. A reader that listed after `S` plans from `S`, and nothing in that plan is collectable.
- **The change feed.** Head position and head object are unchanged, so no read and no emit. Rule 5 of ADR 0039, section 4, proves descent as before; a feed stopped for longer than the window can get `unproven`, as today.

Retention and backups:

- The window is honoured. Point-in-time reads over the last window stay.
- An idle actor today keeps *more* than the window by accident ("more for an actor that stopped writing", ADR 0033, section 1). That surplus goes. The doc sentence changes; the promise ("roughly the last window") does not.
- `backup create --history all` copies what is listed. Runs spaced closer than the window still see every position. Backups already taken are untouched: they live in the destination.

### 4. Mixed versions

A 0.7 node reads a compacted history. The result is a snapshot and the segment at its position, which a delete and an ordinary collection already leave, and `plan_restore` is unchanged. The pass writes ownership records with the existing code in the app's existing format. No capability word and no gate are needed.

A 0.7 node in the cluster takes no idle snapshot and runs no pass; its actors stay as today.

### 5. Simulation and tests

See "Tests" below.

### 6. Slices

See "Slices" below.

### 7. Smaller alternative

The recommendation is the smaller alternative: the snapshot path plus collection. What it could not avoid is one new rule for who deletes after the release (section 3).

One step smaller exists: the idle snapshot plus an immediate pass under the writer's own hold, honouring the window. It needs no new rule. It does not meet the goal: a burst written inside the last window stays for good, which is the case the ROADMAP names.

## Alternatives rejected

- **Erase without the window at hibernation.** Smallest of all, and it breaks ADR 0033's reader bound, point-in-time reads and the restore's undo for every actor that hibernates.
- **Park or delay the release for a window.** The record keeps naming the node, which pins the actor against placement at wake-up (ADR 0041), the first two items of this release.
- **A sweep that acquires and writes a fence segment.** Passes ADR 0012 unchanged, but moves every compacted actor's head: one read and one `rebase` per actor per feed.
- **Deferred deletes without a hold.** Cheapest per actor. Nothing ties them to the app's incarnation an hour later (constraint 7).
- **Merged segments.** A new format and a mixed-version gate.
- **A head index the feed lists instead of the history.** Solves the listing, not the storage, and adds a write per hibernation and a second source of truth.

## Consequences

- An actor idle for a retention window after its last write holds 2 objects (up to 9 with the proposed threshold), and the feed's listing stops growing with history.
- A restore of a compacted actor reads one object.
- One more deletion rule to keep right. It is a pure function shared with the simulation.
- More bucket writes: a snapshot at qualifying hibernations, and two ownership PUTs per pass.
- The golden trace changes on purpose (see Tests).

Known limits, stated rather than fixed here:

- **An actor that writes a little at each wake** is collected at each hibernation by the hold that wrote it (slice 4, "As built"): down to the snapshot at its head, the segment under it, and what the retention window keeps. The pass is for what that window kept.
- **An actor woken within the window without writing keeps its leftovers.** The wake rewrites the record, so the pass is dropped, and a hold that only read cannot collect. It is compacted after it next writes and then rests for a window. A fix needs evidence that survives read-only holds; ADR 0035's released `complete` head is the candidate, but as built nothing reads completeness and the format is off by default. It deserves its own record.
- **A restart loses the queue.** Actors hibernated within the last window before a restart keep their leftovers until they next write and rest. A local file in the data directory, or a slow sweep over `owners/`, could recover them; neither is in this design.
- **An actor whose node crashed is not compacted** until a later owner writes.
- **A pass that fails after its acquisition is not retried later**: the record's generation moved.
- **A release that landed with its reply lost is not queued**: it comes back `Superseded`, not through `release_landed` (see "Confirmed in the code"). The actor keeps its leftovers until it next writes and rests.
- **The owner generation moves past the head.** The pass's hold raises the record's generation, as a wake that only read does. In version 2, `--require-owner-match` accepts that record (its recorded head is the head); in version 1 it refuses a compacted actor until it next writes (asserted in the restore-identical test). A history compacted over a chain of passes (0.8.1) leaves the record one generation past the head per pass, and `--require-owner-match` accepts only one (`vouches_for`), so in version 2 too such an actor is refused until it next writes, as after two wakes that only read.
- **A pass takes a resident slot** for its job's empty state while it runs, only while the node has room.
- **An actor whose name the record key shortened with a hash** (an encoded name over 200 bytes) is not compacted: the key cannot be told back to its address, as for idle expiry.
- **A pass found `busy`** (the actor in use on the node when due) is dropped; the actor is queued again only by its next release after a write.
- **A pass stopped** by a waiting event, a drain or its hold's end leaves what it did not delete until the actor next writes and rests (`stopped`). One stopped by its deadline is continued (0.8.1), up to 1,000 passes, and given up after 3 in a row that deleted nothing, a history whose listing alone takes the deadline for one; then it is left the same way.
- **A full queue** refuses new actors; they keep their leftovers until they next write and rest.
- **A class with an idle limit is not compacted**, in either record format (see "As built: the idle pass").

## Slices

In order, one PR each.

1. **This ADR.** ROADMAP and `docs/cluster.md` link it. Risk: none.
2. **The rule.** `idle_collectable(listing, released_generation)` in `wasmkeep-cluster/src/replication.rs`, returning the chosen snapshot and the keys, built on `collectable`. Unit tests: the kept set; no snapshot of that generation; a snapshot of that generation without its segment; a later generation's objects untouched; ADR 0012's condition-2 interleaving scripted, with the unsafe choice (any snapshot at the head) failing with `PlanError::Missing`. Risk: low.
3. **The simulation** (below), before any server code. Risk: low; it is what gives slice 5 its evidence.
4. **The idle snapshot.** `SnapshotKind::Idle` and the flight in `replica.rs` (generalising `start_periodic_snapshot` and `PeriodicUpload`, today under `self.batched`, to strict holds), the step in `before_drop` (`actor/state.rs`), `actor/uploads.rs`, metrics, the golden trace re-recorded. Tests: the replica tests listed below. **Own review**: it changes the hibernation step, which today returns at once for strict holds, and the 0.4.2 golden trace.
5. **The idle pass.** The queue (a new `actor/compaction.rs`), the hook where a release lands (`release_landed` in `cluster/lease_hold.rs`, which as built does not have the released record's version: `Owners::release` must return it first, see "Confirmed in the code"), the mailbox job using `ensure_owner_with(Some((version, idle_origin)), false)` without its restore, `Collector` built with the released generation, the flag, metrics. **Own adversarial review**: it deletes history under a hold that did not write it. One thing to settle there: `ensure_owner_with` restores and verifies for expiry (ADR 0035, section 6), and this job must not; the review decides whether that needs a separate entry point.
6. **Cluster tests, the feed test, docs** (`docs/cluster.md` "History in the bucket", `docs/actor-data-access.md`, ADR 0012's and ADR 0033's amended sentences), and the feed listing measured again at 100,000 actors after bursts. Risk: low.

## Tests

### Simulation (`sim/tests/madsim_replication.rs`)

A new dimension of `Rules` beside `Collect`: owners that release take an idle snapshot when they wrote the head, and a per-node task runs the pass after a delay through `acquire_unchanged`, the shared rule, and the delete client whose requests land late.

Oracles:

- every acknowledged write is restored (existing);
- the final re-plan never returns `Missing`, whatever landed late (existing);
- **new: head unchanged.** For every pass, the plan's head and the head object's key are the same before and after;
- **new: restore identical.** The positions the snapshot model holds after the pass equal those before it, and those of the listing without `S` (as built: before the pass alone, `S` is at the head on both sides);
- **new: no gap.** After a pass the plan is a snapshot with every segment after it listed.

Unsafe variants some seed must catch (or, if no seed reaches it within the budget, a scripted unit test as ADR 0012 did for its guard):

- the pass takes any snapshot at the head, without checking the released record's generation;
- the pass also deletes the segment at `P`;
- the pass takes a record that changed since the release (to show whether the version check carries weight; my argument says safety does not need it, and the sim should say so either way).

Non-vacuity: counts of idle snapshots taken, passes that deleted at least one object, passes dropped for a changed record, and pass deletes that landed after their hold ended. The test fails if any is zero.

Other sims:

- `madsim_backup.rs`: a reader racing passes finishes within two attempts with the window above ADR 0033's bound; a variant that ignores the window exhausts a reader's attempts. (Built in 0.8.1, see "As built: the backup retry and the last simulations".)
- `madsim_expiry.rs`: a pass and an expiry race for one record and one wins by the version; a pass keeps the idle origin on a version 2 record.
- `madsim_recorded_head.rs` or `madsim_floor_proof.rs`: after a pass, an acquirer proves the recorded head at the exact position. (Built in 0.8.1, in `madsim_floor_proof.rs`.)
- `madsim_batched.rs`: no idle snapshot is labelled with a segment in flight.

### Golden trace (`crates/wasmkeep-server/tests/cluster/golden_trace.rs`)

The script has a hibernation after writes. If the threshold is met there, one snapshot PUT appears at that hibernation. Re-record with `WASMKEEP_RECORD_TRACE=1` and review that this is the only difference: no other operation, key or order moves. The script wakes the actor well inside the window, so no pass appears. (As re-recorded in slice 4, the PUT is not the only difference, and cannot be: two operations follow from it. The move's restore on the other node reads the new snapshot instead of the periodic snapshot and the three segments after it, and the erasure after the `delete-self` deletes it too. Nothing else moved.)

Add a second, separate scripted trace for the pass (acquisition, listing, deletes, release, in that order), so the 0.4.2 trace stays a comparison with 0.4.2.

### Real code

- **Replica** (`replica.rs` tests): an idle snapshot is refused with a segment in flight and when the hold did not write the head; it leaves the periodic schedule as it was; a cancelled step leaves a flight that a following reset waits for; one already at the head is not uploaded again.
- **Restore identical** (cluster test): write a burst over two generations, hibernate, pass the window, run the pass. Assert the prefix is exactly `S` and the segment at `P`. Restore on another node and compare the database checksum and an `actor export` of the bytes with those taken before the pass; run `integrity_check`.
- **Readers**: `actor export --at` inside the window still works before the pass; `backup create` during a pass succeeds.
- **The feed** (`crates/wasmkeep-cli/src/feed/poll_tests.rs`, with the counting store): poll, compact, poll again; the second poll makes no GET but the four markers and commits no batch.
- **Wake, drain, lapse**: a wake before the pass leaves every object; a pass on a draining node does not start; a pass stops deleting when the node lease lapses.
- **Mixed versions**: `plan_restore` over a compacted listing, as a unit test, plus the existing rolling-upgrade cluster test run over a compacted actor if the harness can pin a 0.7 binary (not checked).

## Confirmed in the code (2026-11-23, while writing slices 1 to 3)

- **Every acquisition writes a new generation, including one whose restore fails.** In `wasmkeep-cluster/src/node_lease.rs` the only writes of an ownership record are the three acquisition writes and the release. `Owners::acquire_prepared` writes the record's generation + 1, or for an absent record the seed above every generation the actor's keys and old lease carry (`seed_generation` in `cluster/lease_hold.rs`, ADR 0020, section 6). `Owners::acquire_unchanged_prepared` writes generation + 1, and refuses an absent record (`Changed`). A `Hold` exists only after that write (or its read-back) succeeded. The restore runs after it (`Replica::restore` in `ensure_owner_with`, `actor/state.rs`), so a restore that fails has already moved the record, and its hold is released at its own generation (`release_hold`). `Owners::release` keeps the hold's generation and names nobody. The resolver (`Owners::resolve`) only releases an unresolved acquire at that acquire's own (raised) generation. A wake of a parked hold is an acquisition, never a re-adoption (ADR 0024, section 2; `Breaks::wake_reuses_parked` in the simulation is the broken variant). The other `Hold` constructions in `wasmkeep-cluster` (`hold_cell.rs`, `local_heads.rs`) are in tests. Outside `node_lease.rs`, a grep of `crates/` for the `owners/` prefix finds two more writers, neither of which leaves a released record at an old generation. The in-place whole-app restore (`wasmkeep-cli/src/backup/restore.rs`) clears the records and writes a generation floor above every generation the bucket carries (`leases/` markers, + 1). The one-time migration of ADR 0022 leaves tombstones at the old keys, which do not decode as records. Within these paths, nothing writes a lower or equal generation over a later one, so a record that still says `g`, released, proves no hold after `g`.
- **`release_landed` does not give the released record's version.** It receives the `Hold`, whose `version` is the record as the acquisition wrote it, and `Owners::release` drops the version its `replace` returned (`Ok(_) => Released::Released`). Slice 5 makes `Owners::release` return it (`Released::Released` carrying the version, or a second method). Two more things found there:
  - A release whose write landed with its reply lost comes back as a conflict, then `still_this_hold` reads a record naming nobody and answers `Superseded`, which never reaches `release_landed`. Such an actor is not queued. That costs leftovers, not safety: one more entry for the known limits.
  - The server compares versions by ETag (`expiry::etag`, the first line of the version `object_store.rs` encodes). On S3 the ETag of a plain PUT is a hash of the body, and a version 1 released record at generation `g` has the same bytes in every incarnation of the app. So the version check alone cannot tell a recreated app's record at the same generation from the remembered one (constraint 7). Safety does not rest on it: the pass applies the rule to the record and listing it reads itself. The "tied to the app's current incarnation" argument in section 3 therefore cannot rest on the version alone (see "Not confirmed").
- **The simulation's answer on the version check:** see "Simulation results" below.

## As built: the idle snapshot (slice 4)

Revised after its adversarial review: two must-fix findings, M1 and M2, and the should-fix ones.

- **The replica.** `SnapshotKind::Idle` and `Replica::start_idle_snapshot` (`replica.rs`). The conditions are split in two:
  - `Replica::idle_snapshot_possible`, the checks that need nothing of the database:
    - it first takes in an upload that ended, so a step cut short does not hold back every later one;
    - replication has not stopped;
    - the hold wrote the head;
    - no segment is in flight;
    - no snapshot is at the head (`since_snapshot` is 0 exactly then);
    - no periodic or idle snapshot of the actor is uploading;
    - no failed periodic snapshot is waiting out its back-off, since the bucket refuses snapshots then.
  - `Replica::idle_snapshot_wanted`, the size rule, read from the captured size without copying:
    - `IDLE_SNAPSHOT_EVERY` (8) segments since the last snapshot, or at least one for a database of at most `IDLE_SNAPSHOT_SMALL_BYTES` (256 KiB);
    - never above `IDLE_SNAPSHOT_MAX_BYTES` (64 MiB). Such an actor keeps its periodic snapshots and their collection.
- **The flight.** It is recorded in `PeriodicFlights` and `Uploads::periodic` in both modes, reusing `start_periodic_snapshot`'s machinery rather than generalising it. It does not call `snapshot_failed()`, so a failure changes nothing of the periodic schedule. `settle_periodic` takes the outcome in, and a success counts as a snapshot at its label.
- **Only after a guest event.** `ActorState::event_hold` is the generation of the hold under which a guest event last ran (set in `finish`). A hold a maintenance job took and that wrote only a fence takes none: a delete found carried out, a blob sweep.
- **At most 8 at once per node** (`IDLE_SNAPSHOTS_AT_ONCE`, a semaphore taken with `try_acquire`). A hibernation finding none free takes none, counted `busy`, so a burst of evictions is not held behind a queue of uploads. On by default. Eviction churn is to be measured in the benchmark pass.
- **The step.** `ActorState::idle_snapshot` (`actor/state.rs`), called from `before_drop` for `DropCause::Idle` and `DropCause::Evicted` only.
  - A strict hold: copied with `ActorDb::snapshot`.
  - A batched hold: at its `Flushed` barrier, with `snapshot_at_captured` before the capture ends.
  - Commits not shipped (`CaptureError::Pending`) stop replication and give the actor up, as `replicate` does.
  - The upload runs in a task of its own. The step waits for its one attempt, so in the common case the snapshot lands, and is acknowledged under the hold, before the release.
- **No upload outlives the hold that started it unwaited (M1).** A late PUT of a strict hold's idle snapshot could otherwise land a database after another node erased it.
  - A step cut short leaves the flight. The next hibernation's step waits for it first (`settle_flight`), and a strict hold's step waits for any flight before it is ready.
  - A strict hold's flush to its barrier (a drain's hand-over, a move) waits for it within its deadline, and is not at its barrier past it.
  - A strict hold's clean stop cancels it (no request not yet started is sent) and waits for it within the stop's deadline, then gives up.
  - Batched holds already waited at their barrier (ADR 0034, section 7).
- **Collection under the same hold (M2).** Once the idle snapshot is created, or found there at the head, the step runs ADR 0012's collection below it (`collectable` with the hold's own generation and that snapshot, honouring the retention window), within one upload budget, before the release. No new rule is needed: this hold wrote the snapshot.
  - Without it, history grew without bound. A landed idle snapshot zeroes the count a periodic snapshot waits for (64 segments), so an actor that writes a little at each wake uploaded a snapshot and a segment per wake, and nothing was ever collected.
  - The cluster test `snapshots::an_actor_that_writes_once_per_wake_keeps_a_bounded_history` writes once and hibernates 70 times with a window of zero. The prefix never holds more than 4 objects, and ends as the snapshot at the head and the segment under it.
  - With the default window, what is younger than an hour stays until a later pass. That is the window's promise, and the pass of slice 5 is for it.
  - Two alternatives were weighed and not taken:
    - counting the objects below the plan's base at a restore collects only at the next wake and puts work on the restore path;
    - a count of segments since the last collection cannot be rebuilt from a listing at the next hold, as `since_snapshot` is.
- **Metrics.** `wasmkeep_idle_snapshots_total{outcome}`, with outcomes `created`, `existed`, `failed`, `refused` and `busy`. A failure also counts in `wasmkeep_snapshot_upload_failures_total`.
- **The switch.** `serve --no-idle-compaction` and `[process] idle-compaction = false` in a node file (`ClusterConfig::idle_compaction`). It is one switch for the whole of this record: the pass will obey it too.
- **Which paths reach `before_drop` with `Evicted`.** `Mailboxes::hibernate` sends it for a capacity eviction, and also from a drain's hand-over, a move (both ends), an idle expiry, a delete request, a blob sweep, an upgrade or a call that found another owner, an alarm stub, and an activation that was cut.
  - Holds ended first, so no snapshot: a drain's hand-over and the move's source release their hold in their own job before `hibernate`. The stubs hold nothing, and a cut activation dropped its hold.
  - Holds kept until the drop, so the conditions decide:
    - an operator delete request and an idle expiry keep their hold until `hibernate`. After their reset and the snapshot at it, a snapshot is at the head, so none is taken.
    - a delete found carried out and a blob sweep write a fence, but ran no guest event under that hold, so none is taken.
  - A clean stop and an app delete are `DropCause::Stop`.
- **Parked holds.** A parked hold (a room with sockets open, ADR 0024) also takes it: `before_drop` runs before `Drop` decides between parking and releasing. The pass of slice 5 follows releases only, so for a parked room this snapshot only shortens the next restore.
- **Simulation.** `madsim_batched.rs` models it at a hibernation's barrier: `idle_snapshots_at_hibernation_keep_every_restore_whole`. One taken with a segment in flight (the database as it is, labelled at the acknowledged head) is caught as a restore that is not the owner's prefix: `an_idle_snapshot_labelled_with_a_segment_in_flight_is_caught`.

## As built: the idle pass (slice 5)

- **The release's version.** `Owners::release_written` returns the version its own write's answer gave (`Released::Released` and `Some(version)`; `None` for every other outcome); `Owners::release` wraps it, so its forty callers are unchanged. The three places a release lands in the server (`LeaseHold::release`, `send_release`, and the background retry of a pending release in `retry_pending`) go through `Cluster::release_landed`, which ends the frozen head's entry as before and calls `note_idle_release`.
- **What is queued.** A hold whose acknowledged head is a position of its own generation (`compaction::wrote`: only a segment it uploaded puts one there), whose release landed with its version, with idle compaction on and the app not refused. A hold that only read and the pass's own hold (it uploads nothing) are not queued, so a pass never queues another (until 0.8.1, where a pass stopped at its deadline queues its continuation, "As built: continuing a stopped pass"); a landed release of a hold that did not write, and any acquisition of the actor on this node, forget its entry (the version it remembered is gone). A hold of an expiry or a delete request whose reset wrote a segment is queued like any other; its pass compacts what the erasure left, or finds the record changed.
- **The queue** (`actor/compaction.rs`, `Queue`, on the `Cluster`, so per app and node, as `idle_snapshot_slots` is): keyed by record key, a later release replacing an earlier entry; at most 100,000 entries (`QUEUE_MAX`): a full queue refuses a new actor (the entries there are older and closer to running), counted in `wasmkeep_idle_passes_dropped_total`; an entry holds the key, the released generation, the version, the release time (the idle origin), the due time (release + `history_retention` + `max_skew_ms`) and its retries. A loop (`compaction_loop`) runs at most 4 passes at once (`PASSES_AT_ONCE`): it takes a due entry whenever a slot is free, and sleeps until an entry is added, a slot is given back (when the pass's job ends, not when the loop stops waiting for it, so no more than 4 run), or the next entry comes due, at most 5 s. (As first built it took at most 4 entries per tick of up to 5 s: 100,000 rested actors would have taken about 35 hours. Review finding M1.) The tombstone (`app_tombstoned`) and idle compaction off clear it.
- **The job** (`ActorState::idle_pass`), in the actor's mailbox, no guest code. Before submitting (`pass_one`): refused for a class with an idle limit or no longer deployed, a draining node, an app refusal; `busy` if the actor is resident, held, parked or has a socket here; `changed` if one read of the record's metadata (a HEAD) shows another version, so a stale entry costs no resident slot. A passing obstacle puts the entry back 5 s later, at most 3 times, then counts it `refused`: a node behind the deployed version (`deploy_behind`), an app gate not yet checked, a record or marker read the bucket did not answer, an acquisition `Unavailable`, and a node at its resident limit (the job is admitted only with room: it never evicts an actor). In the job: `busy` again if a hold or a live instance appeared, refused if the node drains or moves the actor, and refused before anything is written if the app's markers give another epoch or say deleted or restoring (the check of step 3, made early). Then:
  1. `LeaseHold::acquire_with(cluster, address, Some((version, released_ms)))`, the expiry's acquisition: the app gate, the format gate, the local evidence, `acquire_unchanged_prepared` with the remembered version (`same_version`: the `ETag`, and the version id too where both carry one, a versioned bucket; an expiry's listing gives the `ETag` alone) and the release time as idle origin. The hold is a local, never `self.held`, so no event runs under it; events queue behind the job. It is registered while it lasts (`idle_pass_keys`), so placement does not count it as the actor in use (`holds_for_events`).
  2. The released generation is the hold's less one, and must equal the entry's (an equal `ETag` at another generation is another record).
  3. The incarnation check (below).
  4. `Collector::idle` (built from the cluster: `history_key`, the released generation, the new hold's `LeaseView`) and `Collector::collect_idle`: one listing, `idle_collectable`, the deletes older than the window by `last_modified`, in batches of 100, the lease checked before each, and the pass's stop too: 5 s after the acquisition (`PASS_DEADLINE`), an event waiting in the mailbox behind the job (`Mailboxes::waiting`), or a drain begun, it deletes no more, and what is left waits for the actor's next write (from 0.8.1, a stop at the deadline is continued). `collect` and `collect_idle` share the listing and the deletes (`Collector::list`, `delete_older`). Up to 3 attempts, one second then two apart, while the hold is valid and the pass not stopped.
  5. `LeaseHold::release`: unverified, so it freezes the requirement it inherited, and in version 2 the idle origin it carried.
  Then the job's empty state is let go by a second job, behind any event that came during the pass, only if it still has no hold and no instance (as first built it was hibernated at once, which could evict an actor an event had just woken).
- **`ensure_owner_with` is not used.** Settled here, for the review: it drops a hold, restores and verifies, sends a draining node's wake-up to a peer or claims when none is eligible, and applies placement. None of that belongs to a pass. The job calls `LeaseHold::acquire_with` itself, with its own drain and move checks (a cluster test, with those checks removed, saw a drained node compact).
- **Outcomes**, `wasmkeep_idle_compactions_total{outcome}`: `done` (as built in 0.8.0 also a pass stopped part way; from 0.8.1 only one that left nothing the window lets go, with `continued` and `stopped` for the others), `changed` (the acquisition found the record changed, or held: `RECORD_CHANGED`, `NotOwner`; or the generation differs), `nothing` (`IdleCollected::NoSnapshot`), `refused`, and `busy`, which the ADR did not list: the actor was in use on this node when the entry came due. A busy entry is dropped, as section 3, step 1 says; its next release after a write queues it again. Deletes count in `wasmkeep_history_collected_total`, failures in `wasmkeep_history_collect_failures_total`.
- **Classes with an idle limit are not compacted, in either record format.** The record proposed only version 1 (constraint 8). In version 2 the pass does carry the idle origin through both rewrites (`a_pass_keeps_the_idle_origin_on_a_version_2_record` in `madsim_expiry.rs`), but the node that collects expiry reads a record's carried `idle-since` only for actors it refused or judged itself (`carried_idle_since`); for the others it judges by the last-modified time, which the pass moved. The collector is usually another node, so the pass would restart such an actor's idle clock in either format. A fix (reading `idle-since` for every recently modified record, or a cheaper sign) belongs to ADR 0035's expiry rules, not here. A class with an idle limit is expired after it anyway.
- **The switch.** `--no-idle-compaction` and `[process] idle-compaction = false` stop both the snapshot and the pass: nothing is queued, and the loop clears the queue.
- **`--require-owner-match`** (`history::export`, `actor export` and `actor read`): a released version 2 record one generation past the listed head whose recorded head is exactly that head is accepted (`vouches_for`); that is what a pass, or a wake that only read, leaves. A version 1 record carries no head: such an actor is refused until it next writes, as the flag's help says.
- **Test hooks**, `#[doc(hidden)]` on `ActorService`: `idle_passes_queued`, `pause_idle_passes`, `idle_passes_due_now`, and `idle_passes_take_current_versions` (the recreate test's stand-in for an `ETag` that is a hash of the body).

### The recreate case, settled

Constraint 7 and "Not confirmed" asked what ties a pass to the app's current incarnation once the app was deleted and recreated while the node that queued the entry kept running.

What the code does without this slice's check:

- A node that sees the tombstone stops serving the app (`stop_for_deleted_app`, which now also clears the queue) and `serve` exits: its memory goes with it.
- `app delete` removes data only once no live node of the app heartbeats, or after fencing it, and `--recreate` waits 40 s after the tombstone and the last heartbeat. So a node of the old incarnation that is still running through a delete and a recreate was paused or cut off. Its lease then lapses, a node following its app re-arms the tombstone check (`rearm_app_check`), and the check after the lapse compares the `recreated` marker and stops. No acquisition happens in between (`app_refusal`).
- That leaves nodes that do not follow their app, and any gap in the above. There the version check is all that is left, and on S3 the `ETag` of a version 1 released record at the same generation is the same in both incarnations (a hash of the same bytes).

The design chosen, as the blob sweep already does for files (ADR 0030, `epochs_pass`): **the pass compares the app's epoch, read before its acquisition and again under its hold, with the one this runtime started with**, and compacts nothing on a difference. The epoch is `blobs::epoch` of the `recreated` and `restored` markers (`ActorConfig::blob_epoch`, from `AppRef::prepare_local`); the pass reads those two markers again, and also `deleted` and `restoring` (refused while either exists). Cost: eight small GETs per pass (the four markers before the acquisition, so a deleted or recreated app's record is not rewritten, and again after it). Read after the acquisition, the check holds for the whole pass: while this node's hold, and so its lease and heartbeat, is live, `app delete` cannot remove anything, and the deletes stop at the next batch once the lease lapses. Dropping the queue at the tombstone was added too; it is not what the proof rests on.

The test, `idle_pass::a_pass_deletes_nothing_of_a_recreated_app` (cluster suite): node `a` writes an actor over two generations and hibernates (one entry queued, passes paused). The test then deletes the actor's history and record and writes the `recreated` marker, as `app delete` and `deploy --recreate` leave them, without a tombstone a test node would see. A new node `c` writes another history for the same actor up to the same generation and releases it. The entry is given the record's current version (`idle_passes_take_current_versions`, what an `ETag` of identical bytes gives), and the pass runs. The read before the acquisition already sees the new epoch, and the outcome is `refused` with every object of the new incarnation still there, the record not written. With both epoch reads removed, the same test fails: `a`'s pass deleted the recreated app's objects.

The read under the hold is what the deletes rest on, and has tests of its own (review finding M2: with the read before the acquisition refusing first, removing the one under the hold left the test above green). A store hook writes a marker during the pass's acquisition, between its two reads: `a_recreated_marker_written_during_the_pass_stops_it`, `a_restored_marker_written_during_the_pass_stops_it` (a whole-app restore in place), and `a_deleted_or_restoring_marker_written_during_the_pass_stops_it`. Each asserts `refused` and every object kept; with the read under the hold removed, all three fail.

### Tests, and whether each was seen failing

Each "fix" below was removed by hand and the test run again.

- **Restore identical** (`an_idle_pass_leaves_the_snapshot_and_its_segment_and_the_same_database`): two bursts on two generations, a window of 3 s, the pass. The prefix is exactly `S` and the segment at `P`; an export after the pass has the position and raw checksum of one taken before it, read at most two objects, and passed SQLite's `integrity_check` (`history::export` verifies); node `b` restores it and reads the same count, and an export after `b`'s restore has the same raw checksum. `actor export --at` between the bursts gives the first burst's count before the pass. Without the pass the prefix keeps every object (the assertion on two objects is what fails).
- **Wake** (`a_wake_before_the_pass_leaves_every_object`): a read-only wake on the node that remembers the pass forgets its entry; one on another node leaves it, and the pass finds the record changed with one read, before submitting its job; every object stays. (In a first build, with both the version check and the generation check removed, the objects still stayed, the read-only hold having uploaded no snapshot of its generation, and the test failed only on the outcome: the chain's safety does not rest on the version, as the simulation found.)
- **Drain** (`a_draining_node_runs_no_pass`): with the drain checks removed, the drained node compacted; with them, `refused` and nothing deleted. `a_drain_begun_during_a_pass_stops_its_deletes` (the listing held back 4 s, the drain begun meanwhile): nothing deleted; with the pass's stop removed (both its drain and waiting-event conditions), it deleted. `a_drain_begun_before_the_pass_job_starts_refuses_it` (the record's read held back, the drain begun meanwhile): refused, nothing acquired. Not seen failing: with the drain check at the start of the job removed it is still refused before acquiring, by something on the drain's side that I did not pin down (likely the drain's hand-over of residents reaching the job's state); the check stays.
- **A request during a slow pass** (`a_request_during_a_slow_pass_is_served_after_it`, the listing held back 2 s): a request sent meanwhile waits behind the job, the pass stops before deleting, and the request is served after it with the right count, under its own hold at the pass's generation + 1. With the waiting-event condition removed from the stop, the pass deleted and the test fails.
- **Throughput** (`fifty_due_passes_all_run_promptly`): 50 rested actors with a 60 s window (so a 5 s tick), their passes due together, all run within 8 s. With the tick-paced loop of the first build, 8 of 50 ran in 8 s.
- **Version 2** (`on_a_version_2_app_a_pass_keeps_the_recorded_head_and_the_owner_match`): a pass on an app with the recorded-head format; the record it leaves is one generation past the head, released, with its recorded head at the head (the inherited requirement survived the release), and `--require-owner-match` accepts the export.
- **Lapse**: `replica::tests::an_idle_collection_deletes_below_the_released_snapshot_and_stops_at_a_lapse`, a unit test with a store that lapses the lease at its first batch of deletes: 100 deleted, then nothing; with the check before each batch removed, it deletes past the lapse and fails. It also checks the kept set (the snapshot at the head, its segment, a later generation's segment), the window, and `NoSnapshot` for a generation that snapshotted nothing.
- **Readers**: the `--at` read above, and `an_export_racing_a_pass_publishes_the_same_database` (an export whose segment reads are held back 1.5 s while the pass deletes publishes the same database). There is no fix to remove; it checks the invariant. And `backup::tests::backup_create_during_an_idle_pass` (CLI crate, no node: the pass's `idle_collectable` deletes applied after the backup's listing, at its first read). `--history head` completes and verifies at the head. As first built, `--history all --require-continuous false` completed with the deleted position recorded as a gap, and `--history all` by default was refused as partial, as when an ordinary collection lands between its listing and its reads (ADR 0033); the next run completed. Since 0.8.1 both complete in the same run, the key recorded as collected (see "As built: the backup retry and the last simulations"). Neither reader test has a fix to remove; the existing `collector_between_listing_and_get_forces_a_fresh_plan` and `history_all_reports_listed_get_miss` cover the same shape for ordinary collection.
- **The feed** (`feed::poll_tests::a_poll_after_an_idle_pass_reads_nothing_but_the_markers`, a store counting GETs): poll, compact with `idle_collectable`, poll again. The second poll reads the four app markers and nothing else, and writes no file. With the segment at `P` also deleted, the second poll reads history and the test fails.
- **Recreate**: above.
- **The idle-limit gate** (`a_class_with_an_idle_limit_is_not_compacted`): with the gate removed, the pass compacted; with it, `refused`.
- **The switch** (`no_pass_is_queued_with_idle_compaction_off`).
- **Queue** (unit): one entry per actor, due first, removable; a full queue refuses a new actor, counted, and still replaces an actor already there.
- **`Owners::release_written`** (unit): the version written is the record's, an acquisition unchanged on it takes the record once, and the pass's own release changes it.
- **The golden trace of the pass** (`golden_trace::the_bucket_operations_of_an_idle_pass`, fixture `golden_trace_idle_pass.txt`), separate from the 0.4.2 trace, which is unchanged: the writes, the hibernation's snapshot, listing and release, then the pass's `PUT owner [update] holder=a generation=2`, one `LIST`, three `DELETE`s (the first snapshot and two segments below `S`), and `PUT owner [update] holder=- generation=2`.

### The simulations

`madsim_expiry.rs`, at 2,000 seeds:

- `an_idle_pass_and_an_expiry_race_and_the_version_decides`: nodes also queue, after each release of a hold that ran events, the version the release wrote (`LeaseNode::release_written`), and take it back 400 ms later with `acquire_unchanged`. New oracle: no version of one record is taken twice (by expiries, passes, or one of each). 5,327 expiries, 6,418 passes took their record, 48,885 found it changed, 3,324 versions were tried by both an expiry and a pass; no version taken twice, no active or alarmed actor expired.
- `an_idle_pass_that_acquires_as_an_event_does_is_caught`: caught at seed 0 taking a version an expiry already took.
- `a_pass_keeps_the_idle_origin_on_a_version_2_record`: scripted, 50 seeds each way. With the release's last-modified time as origin, the record the pass leaves carries it, and an expiry past the limit from the release chooses the actor although the pass rewrote the record; with no origin (the broken variant) it does not.
- The earlier cases are unchanged (5,862 expiries; the expiry's plain acquisition still caught as an active actor expired, at seed 10).

Not done: a recreate dimension in `madsim_replication.rs` (its model has no app incarnations, and the epoch check sits outside the chain rule the simulation checks; the cluster tests above are its evidence). The backup reader racing passes, the recorded-head proof after a pass and the per-actor retry inside `backup create --history all`, listed here when slice 5 shipped, came in 0.8.1 (next section).

## As built: the backup retry and the last simulations (0.8.1)

### The retry in `backup create --history all`

`Copier::actor` (`wasmkeep-cli/src/backup.rs`) already listed, planned and read an actor again, up to 8 attempts (`ATTEMPTS`), when a read of its plan missed. What failed the run was the accounting of the other keys: every key any attempt listed and did not copy was a gap, so a key a pass deleted after the listing failed a continuous run even after a fresh listing showed the pass's shape. Now, once an attempt has copied the plan and left gaps, the actor is listed again:

- a key gone from the new listing that `collectable` would delete below a listed snapshot whose segment is listed too (the shared rule, which `idle_collectable` and ADR 0012's collection both apply), and whose last-modified time, as first listed, is at least the retention window before the tool's clock (`--history-retention-secs`, new on `backup create`, default 3600 as on `serve`), is recorded in the actor's `collected` list, not as a gap (`collected_by_rule`, computed once per listing);
- if a gap is still listed (its read missed) or the listing changed, the actor is listed, planned and read again, within the attempts;
- what remains is a gap, as before: `--require-continuous` (the default with `--history all`) fails the actor, `--require-continuous false` records it. When the attempts run out the actor keeps its gaps rather than failing the run as a whole.

**What continuity now means.** ADR 0033 already made no claim for objects erased before the listing. A key gone between the listing and its read that collection could have deleted, by its shape and its age, is treated the same way: a listing a moment later would not have shown it, and the next run would not have either. Runs spaced closer than the window have already copied such a key (a run starts from the previous run's objects). The ranges still record what was archived (a collected segment can split them; `a_collected_historical_segment_splits_ranges_without_a_gap`). A key gone younger than the window (an actor delete's erase, which has no window, a collector that ignored it, any stray delete), or that no snapshot and its segment explain, is still a gap. What the tool cannot check: the ownership record (a collection by any generation is judged alike), and the window the cluster really runs with (the operator gives it; one set above the cluster's only counts more gaps). Its clock is compared with the bucket's last-modified times, so a tool clock ahead of the bucket's by the skew excuses keys younger than the window by as much.

Tests (`backup::tests`), each seen failing against a mutation of `collected_by_rule`: `backup_create_during_an_idle_pass` (window 0, the fixture's objects being a moment old: head, all, all continuous each complete in one run and verify, the deleted snapshot recorded as collected; fails with the rule excusing nothing), `history_all_reports_listed_get_miss` and `rereview_retry_observations_include_vanished_non_head` (every observed key is a gap or collected), `a_miss_no_collection_explains_stays_a_gap` (continuous refused and its verification too, `false` completes with the gap; fails with the shape check removed), `a_key_younger_than_the_window_gone_below_a_snapshot_stays_a_gap` (the default hour: the pass's shape, a key a moment old, refused, and the next run completes; fails with the age check removed). And `a_listed_key_that_keeps_missing_is_retried_then_a_gap`: a listed key whose reads always miss is read exactly 8 times, then kept as a gap with the actor; with the retry for a still-listed key removed it is read once, and the test fails.

### A backup reader racing passes (`madsim_backup.rs`)

Two models, both with the real planner and `idle_collectable`:

- **In the replication model.** `Idle::Windowed`, a new dimension of `Rules`: `Idle::Safe` with the pass deleting only what was last modified at least `IDLE_WINDOW_MS` (4 s) before it, on its node's clock, from a listing with times. No periodic collection and no deletes, so every delete is a pass's. A lease-free reader lists at once and reads the plan (snapshot, segment at its position, segments: what `--history head` copies), its reads spread over a deadline of 1.8 s and the oldest object read last, and starts again on a miss; it gives a backup up after 2 attempts. `2 × 1.8 s + 0.3 s` (the skew bound) is below the window, a compile-time assertion. Oracles: no reader gives up; every completed backup passes `oracle_history` (the owners' history at its head, with every write acknowledged before its listing); the model's own checks, per pass and final. At 2,000 seeds of 60 s: 57,170 backups, 34 on a second attempt after 34 reads found a pass's delete, none gave up, 988 passes deleted, the longest attempt 1,800 ms. The race is rare there: a plan a pass deletes from is usually read before the pass, or starts at a periodic snapshot younger than the window. A reader that skips an object it finds gone, instead of starting again, is caught by the content oracle (first at seed 17). Passes ignoring the window were not caught here within 2,000 seeds (no reader gave up), so that variant lives in the focused model.
- **A focused model**, where the race is common: one writer, no faults, 10 ms steps; each wake a new generation writing 1 to 6 segments, an idle snapshot at hibernation, a pass 0 to 3 s later unless the actor woke first, deletes landing up to 400 ms late, the pass's clock skewed by up to the bound, rests short or long. The pass's delay is not the window, more adversarial than the server, whose pass waits a window after the release: the bound rests on the objects' age alone. The reader is the same, and its restore is compared with every position written up to its head. At 2,000 seeds of 60 s: 64,737 backups, 1,307 on a second attempt (on 987 seeds), none gave up. Mutations: passes ignoring the window, caught on 157 of 2,000 seeds (first seed 29); a window equal to the read deadline, below the bound, caught on 32 (first 166; the test runs at least 400 seeds); a reader skipping a missing object, caught by the content oracle on 999 (first 0).

Running `Idle::Windowed` changes no other case: with it off, nothing is listed with times and no draw is added (the idle case's counts at 2,000 seeds are the same as without it).

### The recorded head after a pass (`madsim_floor_proof.rs`)

`after_an_idle_pass_an_acquirer_proves_the_recorded_head_exactly`: each seed writes a history over 1 to 4 generations with periodic snapshots. The last writer's hold acknowledges its head H through the hold cell (`hold_cell::verify`, `acknowledge`, `end`) and releases with `Requirement::stored(Requirement::frozen(..))`, encoded and decoded as a version 2 record: the body `Owners::release_written` writes, built here without a bucket. An idle snapshot is taken at H, or on some seeds none (the threshold not met), the pass then compacting from a periodic snapshot of H's generation below H. The pass, one generation up, inherits its requirement (`combine` of its node's evidence and the record), applies `idle_collectable`, deletes, and releases unverified, freezing what it inherited. On some seeds a late segment of an earlier generation lands after the pass. An acquirer on another node reads the record (`Requirement::of_record`), plans and judges with `prove`. Checked: the record still requires H, complete, stamped by H's writer; the plan's head is H; `prove` says proven, and by the exact branch (H on the chain with its checksum, nothing above the stamp). At 2,000 seeds: every pass proven exactly, 1,942 passes deleted, 361 from a snapshot below the head. `a_broken_idle_pass_fails_the_recorded_head_proof`, at 2,000 seeds: the pass also deleting the segment at `P`, caught on 463 seeds (a late segment becomes the head, the plan misses its parent); deleting `S`, on 1,942; deleting a segment between `S` and H, on 361; an idle snapshot holding another position's database, on 1,565 (`Conflict`); a release dropping what it inherited, on 2,000.

## As built: continuing a stopped pass (0.8.1)

**What was seen.** A cluster left at rest for 2 hours past a burst: 69 of 71 actors compacted to 2 objects or fewer. The two largest, a room with 8,437 objects and its pusher with 6,003, each lost exactly 100 objects and stopped; nothing queued another pass. Every pass counted `done`. Exports before and after were byte-identical.

**The cause, in the code.** `delete_older` asks the pass's stop before each batch of `COLLECT_BATCH` (100), and the stop includes `PASS_DEADLINE`, 5 s from just after the acquisition (and the second marker read), which also covers the listing. A listing of thousands of objects and one bulk delete took the deadline, so the second batch's check stopped it. `compact_under` ignored `IdleCollected::Collected { stopped, .. }` and counted `done`. The pass's own release queues nothing (it did not write), so the rest waited for the actor's next write. Which of the three stops fired on that cluster is inferred (the deadline: nothing waited, nothing drained), not logged; the stop's reason is logged now. The two `changed` there are not these actors' passes stopping: `changed` has three causes (one read showing another version than the release wrote; the acquisition finding the record changed or held; another generation), none of them a stop. 73 outcomes for 71 actors suggests two stale entries, which is inferred, not checked.

**What changed.**

- `IdleCollected::Collected` counts `left`: collectable, old enough, and not deleted. A pass with `left > 0` is not `done`.
- The pass's stop records why (`Stop`): an event waiting, a drain, the deadline; `left > 0` without a stop is the lease ending.
- A pass stopped at its deadline arms a continuation (`Queue::arm`) before its release. Its release, when it lands with its version, wrote nothing, so `note_idle_release` forgets the actor's entry as before and then queues the armed one (`Queue::continue_from`): the record key, the version this release wrote, this hold's generation (what the next acquisition must find), the writer's generation `g` (`snapshot_generation`, what it compacts for), the first release's time as idle origin, due 5 s later (`CONTINUE_AFTER`). This runs inside the job, so an event behind it acquires after the entry is there and forgets it as any acquisition here does. The outcome is `continued`.
- The continuation is a pass like the first: the same checks, `acquire_unchanged` on its version, the generation check (`hold - 1` equals the entry's), the incarnation check twice, `idle_collectable(listing, g)`, the window, batches of 100, the lease and the stop before each, 5 s, the release unverified.
- A bucket error at or past the deadline, the hold still valid, is a stop at the deadline too (`stopped`, then continued), not `refused`, which would end the chain.
- Bounds: one entry per actor, so a giant history takes at most one of the node's 4 slots, and its spacing puts it behind every entry due before it; up to 1,000 passes in a chain (`CONTINUATIONS_MAX`, about 100,000 objects at one batch a pass); given up after 3 in a row that deleted nothing (`FRUITLESS_MAX`). A pass stopped by an event, a drain or its lease, a chain at a bound, and a release that did not land with its version (or a full queue) count `stopped`; what is left waits for the actor's next write and rest.
- Metrics: `continued` and `stopped` in `wasmkeep_idle_compactions_total{outcome}`. No bucket format, key or record field changes.

**Why it is safe.** The rule's argument needs that only `g` can have uploaded a snapshot keyed with `g`, at a position `g` acknowledged. After `g`, every hold of the actor raised the record's generation (every acquisition does). The continuation acquired unchanged the record its predecessor's release wrote, so no hold came between that pass and this one; by induction back to the first pass, which acquired `g`'s own released record unchanged, every hold after `g` was one of this node's passes. A pass uploads nothing: no segment, no snapshot, no fence. So the listing still holds no object of a generation above `g` that a later hold wrote, `S` and the segment at `P` are `g`'s, and `collectable(listing, g, S)` is the same set the first pass would have deleted, less what it did. The other parts carry over:

- **Under the writer's hold, not.** As for the first pass, the hold only excludes a concurrent wake and ties the deletes to a valid lease and the app's incarnation (checked again by each pass); ADR 0012's proof covers deletes that land late.
- **A write between passes.** It needs an acquisition, which changes the record: on this node it forgets the entry; elsewhere the continuation's one read or its `acquire_unchanged` finds another version, `changed`, dropped, nothing deleted. The writer's own release queues its own pass, for its own generation.
- **The recorded head.** Each pass releases unverified, freezing what it inherited; the recorded head stays the writer's head through the chain (`madsim_floor_proof.rs`, below). The owner-match limit is in "Known limits".
- **Backups racing passes** ("As built: the backup retry and the last simulations"). A continuation deletes only what is older than the window by `last_modified`, judged again at its own time, so ADR 0033's two-attempt bound rests on the objects' age as before, and `backup create`'s rule (`collected_by_rule`) excuses the same shapes.

**Tests.** `idle_pass.rs`: `a_pass_stopped_at_its_deadline_is_continued_until_the_history_is_compact` (a test hook, `limit_idle_passes`, makes each pass delete one batch of 2 and stop as at its deadline: every pass but the last counts `continued` and queues its continuation at its hold's generation, the last `done`; the prefix ends as `S` and its segment, the export is the same database, the record is one generation past the head per pass, a restore on the other node reads it); `a_write_between_two_passes_stops_the_chain` (a write on the other node between two passes: the continuation counts `changed` and deletes nothing; a wake on the node that queued it forgets it). The other idle-pass tests and the pass's golden trace are unchanged.

**Simulations.**

- `madsim_replication.rs`: half the passes stop after 1 to 3 deletes and, once their release landed, queue a continuation on the record that release wrote (read back, as for the first pass), for the writer's generation, 0.1 to 1.5 s later, in the actor's mailbox, the per-pass oracles checked on each. Wakes come between some. New non-vacuity counts: continuations that deleted, and continuations dropped for a changed record. With the version check dropped (`Idle::ChangedRecord`), continuations take changed records too, and the chain still holds, as for the first pass.
- `madsim_backup.rs`: the replication model's passes stop and continue as above under the windowed reader; in the focused model half the passes stop after 1 to 3 deletes and continue 0.1 to 1.5 s later unless a wake came first.
- `madsim_floor_proof.rs`: on half the seeds a chain of 2 to 4 passes, each acquiring the previous pass's released record (one generation up), inheriting from it and deleting part of what is left for the writer's generation; the acquirer after the last proves the writer's head exactly. The mutations now act in the last pass, so on a chain in a continuation, and each is still caught.
- What no simulation checks: the server's own link from the release's answer to the queued continuation (`continue_from`); the cluster and unit tests above do. The sims read the released version back by a GET, as they do for the first pass.

SIM_RESULTS_PLACEHOLDER

## Simulation results

Slice 3, in `sim/tests/madsim_replication.rs` (the `Idle` dimension of `Rules`). The model:

- An owner that hibernates uploads an idle snapshot at its head if its hold wrote the head and none is there, in one bounded attempt. Then it releases.
- One wake in ten only reads: it restores, takes the periodic snapshot a restore of five or more segments makes, and hibernates.
- The node then runs the pass 0.3 to 3 s later, in the actor's mailbox: its own workload waits, and a pass is skipped while the actor is resident there.
- Nobody wakes the actor until after the pass four times in five. Otherwise a wake may come first.
- The pass reads the released record's version back. The server is to take it from the release, see "Confirmed in the code".
- Deletes go through the client whose requests time out and land late, under the faults the other cases use: held links, kills, slow requests, skewed clocks.

The per-pass oracles are checked on the pass's own listing, not on later live reads, where a legal late commit can move the head:

- head position and head object key unchanged;
- no gap: the plan after the pass is a snapshot with every segment after it;
- restore identical.

Restore identical is checked twice. Against the plan before the pass, the check is vacuous: with `S` at the head, both plans read `S`'s body. So the restore after the pass is also compared with the listing *without* `S`, the history the segments alone give, wherever that still plans.

At 2,000 seeds (30 simulated seconds each):

- **Safe**: 39,256 acknowledged writes, all restored. 2,945 idle snapshots and 3,054 passes: 640 deleted, 175 were dropped for a changed record, 589 found no snapshot of the released generation (read-only wakes), and 449 more were skipped with the actor resident. 100 deletes landed after their pass's hold ended. 881 restores were compared without the snapshot. Beside ADR 0012's periodic collection: 33,917 writes, all restored, 560 passes deleted.
- **Each oracle against a mutation** (first seed at 200 seeds):
  - head unchanged: deleting the segment at `P`, seed 2;
  - no gap: also deleting `S`, seed 2;
  - final re-plan never `Missing`: also deleting `S`, seed 3;
  - restore identical: an idle snapshot whose body misses its own head position, seed 2;
  - every acknowledged write restored: the same snapshot, seed 1.
- **Unsafe, also deleting the segment at `P`**: caught by the head oracle at seed 2. It also breaks a restore (a late segment of an earlier generation becomes a head whose parent is gone) at seed 120, and on 14 of 2,000 seeds.
- **Unsafe, any snapshot at the head**: not caught within 2,000 seeds. 170 passes compacted from another generation's snapshot, one a read-only wake uploaded at a previous owner's head, but none was followed by ADR 0012's condition-2 landing. As for ADR 0012, the interleaving is scripted instead: `an_idle_pass_from_any_snapshot_at_the_head_would_break_a_restore` in `replication.rs` fails with `Missing`.
- **The version check dropped** (a plain acquisition, the remembered generation): 39 passes took a record changed since the release and deleted, and every seed keeps every acknowledged write. **The sim says the version check carries no weight for the chain's safety**, as section 3 argues. The model has no app incarnations (constraint 7), which is what the check is for (see "Not confirmed"). The case stays in the suite as a guard of that claim.
- **Where the model departs from the design:**
  - no retention window: the pass deletes at once, which is more adversarial;
  - an idle-snapshot threshold of 1 segment: the database is always small;
  - the released version is read back by a GET, not returned by the release;
  - a pass is skipped while the actor is resident on its node: about 15% of the passes due, dropped as section 3, step 1 says.
- **Where the numbers come from:** the 14-of-2,000 and the 2,000-seed figures for the unsafe variants come from an exploratory run that is not in the suite. The suite's drop-`P` and mutation cases stop at their first catching seed.
- **The late-delete count** watches the bucket for the keys a pass sent and finds still there when its hold ended. A later pass deleting the same key would also be counted. In the main safe run only passes delete. Beside periodic collection, its deletes can be counted too.

## Not confirmed

- ~~What ties a pass to the app's current incarnation when the app was deleted and recreated while the node that queued the entry kept running.~~ Settled in slice 5: the app's epoch, read after the acquisition (see "The recreate case, settled").
- ~~How `ensure_owner_with` can acquire without restoring.~~ Settled in slice 5: it does not; the job calls `LeaseHold::acquire_with` itself (see "As built: the idle pass").
- The thresholds (8 segments, 256 KiB, 100,000 entries, 4 at once) are proposals with no measurement behind them.
- Whether a class with an idle limit can be compacted in version 2: not as built, since the expiry collector reads `idle-since` only for actors it refused or judged (see "As built: the idle pass").
