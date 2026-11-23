# 0042. Compacting an idle actor's history

- Status: proposed (2026-11-23). Slices 1 to 3 done: this record, the
  rule (`idle_collectable`) and the simulation; no server code yet.
- Milestone: `0.8.0`, item 4 under Now in [ROADMAP.md](../../ROADMAP.md)
- Follows: ADR 0007 (chained segments), ADR 0012 (collection), ADR 0020 (holds and generations), ADR 0028 (idle expiry's acquisition), ADR 0033 (offline readers, the retention window), ADR 0034 (barriers, snapshots only at the acknowledged head), ADR 0035 (recorded head), ADR 0039 (the change feed)
- Amends, once accepted: ADR 0012, decision 3 on idle actors ("acceptable for now")

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

Not taken on a drain's hand-over, a move, or a clean stop: the actor is not idle there, or the deadline is the stop's. (Inferred: I did not read `drain.rs` or `moving.rs`; the barrier table of ADR 0034, section 6, says they flush and release from their own jobs.)

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

**Why it is safe.** ADR 0012's proof needs only that `S` and the segment at `P` were acknowledged by the generation that wrote `P`. It never uses who sends the deletes; it already covers deletes that land late.

- No hold existed after `g`: every acquisition writes a new generation, and the record still says `g`, released. (Checked in the code, see "Confirmed in the code" below.)
- So only `g` can have uploaded a snapshot keyed with generation `g`. No earlier hold could name that position, and no later hold exists. This closes constraint 2.
- `g` snapshots only at its acknowledged head (`refuse_unless_at_head`). So `(g, t)` was acknowledged by `g`.
- Every later generation lists after acquiring, sees `S` and `P`, and builds on them. Uploads of earlier generations lose to the segment at `P`. A late upload of `g` itself is above `t` and kept.

The memory entry is only a hint for when to try. Safety rests on the record and the listing the pass reads itself, so a stale or mistaken entry cannot delete wrongly.

**What the hold is for.** Not for the chain's safety. It excludes a concurrent wake for the length of the pass, keeps `Collector` and its lease check as they are, and ties the pass to the app's current incarnation as today's passes are tied (constraint 7).

### 4. Triggers and limits

- Trigger: time since release (the retention window). The idle snapshot's trigger is segments or bytes since the last snapshot. Age alone triggers nothing.
- The queue is in memory, bounded (proposed 100,000 entries, the oldest due dropped first), a few passes at once per node (proposed 4, as `expiry::PARALLEL`).
- A class with `expire-after-idle` in an app on version 1 ownership records is not compacted: the pass would restart its idle clock (constraint 8).
- A pass that fails is retried a few times inside its hold, then given up.
- A kill switch: `--no-idle-compaction`. This deletes history.
- Metrics: `wasmkeep_idle_snapshots_total{outcome}`, `wasmkeep_idle_compactions_total{outcome}` (`done`, `changed`, `nothing`, `refused`); deletes count in `wasmkeep_history_collected_total`.

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

- **An actor woken within the window without writing keeps its leftovers.** The wake rewrites the record, so the pass is dropped, and a hold that only read cannot collect. It is compacted after it next writes and then rests for a window. A fix needs evidence that survives read-only holds; ADR 0035's released `complete` head is the candidate, but as built nothing reads completeness and the format is off by default. It deserves its own record.
- **A restart loses the queue.** Actors hibernated within the last window before a restart keep their leftovers until they next write and rest. A local file in the data directory, or a slow sweep over `owners/`, could recover them; neither is in this design.
- **An actor whose node crashed is not compacted** until a later owner writes.
- **A pass that fails after its acquisition is not retried later**: the record's generation moved.
- **A release that landed with its reply lost is not queued**: it comes back `Superseded`, not through `release_landed` (see "Confirmed in the code"). The actor keeps its leftovers until it next writes and rests.

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
- **new: restore identical.** The positions the snapshot model holds after the pass equal those before it;
- **new: no gap.** After a pass the plan is a snapshot with every segment after it listed.

Unsafe variants some seed must catch (or, if no seed reaches it within the budget, a scripted unit test as ADR 0012 did for its guard):

- the pass takes any snapshot at the head, without checking the released record's generation;
- the pass also deletes the segment at `P`;
- the pass takes a record that changed since the release (to show whether the version check carries weight; my argument says safety does not need it, and the sim should say so either way).

Non-vacuity: counts of idle snapshots taken, passes that deleted at least one object, passes dropped for a changed record, and pass deletes that landed after their hold ended. The test fails if any is zero.

Other sims:

- `madsim_backup.rs`: a reader racing passes finishes within two attempts with the window above ADR 0033's bound; a variant that ignores the window exhausts a reader's attempts.
- `madsim_expiry.rs`: a pass and an expiry race for one record and one wins by the version; a pass keeps the idle origin on a version 2 record.
- `madsim_recorded_head.rs` or `madsim_floor_proof.rs`: after a pass, an acquirer proves the recorded head at the exact position.
- `madsim_batched.rs`: no idle snapshot is labelled with a segment in flight.

### Golden trace (`crates/wasmkeep-server/tests/cluster/golden_trace.rs`)

The script has a hibernation after writes. If the threshold is met there, one snapshot PUT appears at that hibernation. Re-record with `WASMKEEP_RECORD_TRACE=1` and review that this is the only difference: no other operation, key or order moves. The script wakes the actor well inside the window, so no pass appears.

Add a second, separate scripted trace for the pass (acquisition, listing, deletes, release, in that order), so the 0.4.2 trace stays a comparison with 0.4.2.

### Real code

- **Replica** (`replica.rs` tests): an idle snapshot is refused with a segment in flight and when the hold did not write the head; it leaves the periodic schedule as it was; a cancelled step leaves a flight that a following reset waits for; one already at the head is not uploaded again.
- **Restore identical** (cluster test): write a burst over two generations, hibernate, pass the window, run the pass. Assert the prefix is exactly `S` and the segment at `P`. Restore on another node and compare the database checksum and an `actor export` of the bytes with those taken before the pass; run `integrity_check`.
- **Readers**: `actor export --at` inside the window still works before the pass; `backup create` during a pass succeeds.
- **The feed** (`crates/wasmkeep-cli/src/feed/poll_tests.rs`, with the counting store): poll, compact, poll again; the second poll makes no GET but the four markers and commits no batch.
- **Wake, drain, lapse**: a wake before the pass leaves every object; a pass on a draining node does not start; a pass stops deleting when the node lease lapses.
- **Mixed versions**: `plan_restore` over a compacted listing, as a unit test, plus the existing rolling-upgrade cluster test run over a compacted actor if the harness can pin a 0.7 binary (not checked).

## Confirmed in the code (2026-11-23, while writing slices 1 to 3)

- **Every acquisition writes a new generation, including one whose restore fails.** In `wasmkeep-cluster/src/node_lease.rs` the only writes of an ownership record are the three acquisition writes and the release. `Owners::acquire_prepared` writes the record's generation + 1, or for an absent record the seed above every generation the actor's keys and old lease carry (`seed_generation` in `cluster/lease_hold.rs`, ADR 0020, section 6). `Owners::acquire_unchanged_prepared` writes generation + 1, and refuses an absent record (`Changed`). A `Hold` exists only after that write (or its read-back) succeeded. The restore runs after it (`Replica::restore` in `ensure_owner_with`, `actor/state.rs`), so a restore that fails has already moved the record, and its hold is released at its own generation (`release_hold`). `Owners::release` keeps the hold's generation and names nobody. The resolver (`Owners::resolve`) only releases an unresolved acquire at that acquire's own (raised) generation. A wake of a parked hold is an acquisition, never a re-adoption (ADR 0024, section 2; `Breaks::wake_reuses_parked` in the simulation is the broken variant). Nothing writes a lower or equal generation over a later one, so a record that still says `g`, released, proves no hold after `g`.
- **`release_landed` does not give the released record's version.** It receives the `Hold`, whose `version` is the record as the acquisition wrote it, and `Owners::release` drops the version its `replace` returned (`Ok(_) => Released::Released`). Slice 5 makes `Owners::release` return it (`Released::Released` carrying the version, or a second method). Two more things found there:
  - A release whose write landed with its reply lost comes back as a conflict, then `still_this_hold` reads a record naming nobody and answers `Superseded`, which never reaches `release_landed`. Such an actor is not queued. That costs leftovers, not safety: one more entry for the known limits.
  - The server compares versions by ETag (`expiry::etag`, the first line of the version `object_store.rs` encodes). On S3 the ETag of a plain PUT is a hash of the body, and a version 1 released record at generation `g` has the same bytes in every incarnation of the app. So the version check alone cannot tell a recreated app's record at the same generation from the remembered one (constraint 7). Safety does not rest on it: the pass applies the rule to the record and listing it reads itself. But the "tied to the app's current incarnation" argument in section 3 needs the app's tombstone check (`app_refusal`), which every acquisition already makes, and not the version.
- **The simulation's answer on the version check:** see "Simulation results" below.

## Simulation results

Filled in by slice 3.

## Not confirmed

- That drains and moves never reach `before_drop` with `Idle` or `Evicted`.
- How `ensure_owner_with` can acquire without restoring.
- The thresholds (8 segments, 256 KiB, 100,000 entries, 4 at once) are proposals with no measurement behind them.
