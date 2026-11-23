# 0042. Compacting an idle actor's history

- Status: proposed (2026-11-23). Slices 1 to 3 done: this record, the
  rule (`idle_collectable`) and the simulation. Slice 4, the idle
  snapshot, built (see "As built: the idle snapshot"). Slice 5, the idle
  pass, built (see "As built: the idle pass"), with its own adversarial
  review to come; slice 6 (ADR 0012's and ADR 0033's sentences, the feed
  measured again) is next.
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

- **An actor that writes a little at each wake** is collected at each hibernation by the hold that wrote it (slice 4, "As built"): down to the snapshot at its head, the segment under it, and what the retention window keeps. The pass is for what that window kept.
- **An actor woken within the window without writing keeps its leftovers.** The wake rewrites the record, so the pass is dropped, and a hold that only read cannot collect. It is compacted after it next writes and then rests for a window. A fix needs evidence that survives read-only holds; ADR 0035's released `complete` head is the candidate, but as built nothing reads completeness and the format is off by default. It deserves its own record.
- **A restart loses the queue.** Actors hibernated within the last window before a restart keep their leftovers until they next write and rest. A local file in the data directory, or a slow sweep over `owners/`, could recover them; neither is in this design.
- **An actor whose node crashed is not compacted** until a later owner writes.
- **A pass that fails after its acquisition is not retried later**: the record's generation moved.
- **A release that landed with its reply lost is not queued**: it comes back `Superseded`, not through `release_landed` (see "Confirmed in the code"). The actor keeps its leftovers until it next writes and rests.
- **The owner generation moves past the head.** The pass's hold raises the record's generation, as a wake that only read does, so `actor export --require-owner-match` refuses a compacted actor until it next writes (asserted in the restore-identical test).
- **A pass takes a resident slot** for its job's empty state while it runs; on a node at its resident limit it can evict a live actor (whose own release may queue another pass).
- **An actor whose name the record key shortened with a hash** (an encoded name over 200 bytes) is not compacted: the key cannot be told back to its address, as for idle expiry.
- **A pass found `busy`** (the actor in use on the node when due) is dropped; the actor is queued again only by its next release after a write.
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

- `madsim_backup.rs`: a reader racing passes finishes within two attempts with the window above ADR 0033's bound; a variant that ignores the window exhausts a reader's attempts.
- `madsim_expiry.rs`: a pass and an expiry race for one record and one wins by the version; a pass keeps the idle origin on a version 2 record.
- `madsim_recorded_head.rs` or `madsim_floor_proof.rs`: after a pass, an acquirer proves the recorded head at the exact position.
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
- **What is queued.** A hold whose acknowledged head is a position of its own generation (`compaction::wrote`: only a segment it uploaded puts one there), whose release landed with its version, with idle compaction on and the app not refused. A hold that only read and the pass's own hold (it uploads nothing) are not queued, so a pass never queues another. A hold of an expiry or a delete request whose reset wrote a segment is queued like any other; its pass compacts what the erasure left, or finds the record changed.
- **The queue** (`actor/compaction.rs`, `Queue`, on the `Cluster`, so per app and node, as `idle_snapshot_slots` is): keyed by record key, a later release replacing an earlier entry; at most 100,000 entries (`QUEUE_MAX`), the one due first dropped first; an entry holds the key, the released generation, the version, the release time (the idle origin) and the due time, release + `history_retention` + `max_skew_ms`. A loop (`compaction_loop`) looks every tenth of the window (50 ms to 5 s), takes what is due, and runs at most 4 passes at once (`PASSES_AT_ONCE`). The tombstone (`app_tombstoned`) and idle compaction off clear it.
- **The job** (`ActorState::idle_pass`), in the actor's mailbox, no guest code. Before submitting (`pass_one`): refused for a class with an idle limit or no longer deployed, a node behind the deployed version (`deploy_behind`), a draining node, an app refusal; `busy` if the actor is resident, held, parked or has a socket here. In the job: `busy` again if a hold or a live instance appeared, refused if the node drains or moves the actor, and refused before anything is written if the app's markers give another epoch or say deleted or restoring (the check of step 3, made early). Then:
  1. `LeaseHold::acquire_with(cluster, address, Some((etag, released_ms)))`, the expiry's acquisition: the app gate, the format gate, the local evidence, `acquire_unchanged_prepared` with the `ETag` of the remembered version and the release time as idle origin. The hold is a local, never `self.held`, so no event runs under it; events queue behind the job.
  2. The released generation is the hold's less one, and must equal the entry's (an equal `ETag` at another generation is another record).
  3. The incarnation check (below).
  4. `Collector::idle` (built from the cluster: `history_key`, the released generation, the new hold's `LeaseView`) and `Collector::collect_idle`: one listing, `idle_collectable`, the deletes older than the window by `last_modified`, in batches of 100, the lease checked before each. `collect` and `collect_idle` now share the listing and the deletes (`Collector::list`, `delete_older`). Up to 3 attempts, one second then two apart, while the hold is valid.
  5. `LeaseHold::release`: unverified, so it freezes the requirement it inherited, and in version 2 the idle origin it carried.
  The loop then hibernates the job's empty state, as an expiry does.
- **`ensure_owner_with` is not used.** Settled here, for the review: it drops a hold, restores and verifies, sends a draining node's wake-up to a peer or claims when none is eligible, and applies placement. None of that belongs to a pass. The job calls `LeaseHold::acquire_with` itself, with its own drain and move checks (a cluster test, with those checks removed, saw a drained node compact).
- **Outcomes**, `wasmkeep_idle_compactions_total{outcome}`: `done`, `changed` (the acquisition found the record changed, or held: `RECORD_CHANGED`, `NotOwner`; or the generation differs), `nothing` (`IdleCollected::NoSnapshot`), `refused`, and `busy`, which the ADR did not list: the actor was in use on this node when the entry came due. A busy entry is dropped, as section 3, step 1 says; its next release after a write queues it again. Deletes count in `wasmkeep_history_collected_total`, failures in `wasmkeep_history_collect_failures_total`.
- **Classes with an idle limit are not compacted, in either record format.** The record proposed only version 1 (constraint 8). In version 2 the pass does carry the idle origin through both rewrites (`a_pass_keeps_the_idle_origin_on_a_version_2_record` in `madsim_expiry.rs`), but the node that collects expiry reads a record's carried `idle-since` only for actors it refused or judged itself (`carried_idle_since`); for the others it judges by the last-modified time, which the pass moved. The collector is usually another node, so the pass would restart such an actor's idle clock in either format. A fix (reading `idle-since` for every recently modified record, or a cheaper sign) belongs to ADR 0035's expiry rules, not here. A class with an idle limit is expired after it anyway.
- **The switch.** `--no-idle-compaction` and `[process] idle-compaction = false` stop both the snapshot and the pass: nothing is queued, and the loop clears the queue.
- **Test hooks**, `#[doc(hidden)]` on `ActorService`: `idle_passes_queued`, `pause_idle_passes`, `idle_passes_due_now`, and `idle_passes_take_current_versions` (the recreate test's stand-in for an `ETag` that is a hash of the body).

### The recreate case, settled

Constraint 7 and "Not confirmed" asked what ties a pass to the app's current incarnation once the app was deleted and recreated while the node that queued the entry kept running.

What the code does without this slice's check:

- A node that sees the tombstone stops serving the app (`stop_for_deleted_app`, which now also clears the queue) and `serve` exits: its memory goes with it.
- `app delete` removes data only once no live node of the app heartbeats, or after fencing it, and `--recreate` waits 40 s after the tombstone and the last heartbeat. So a node of the old incarnation that is still running through a delete and a recreate was paused or cut off. Its lease then lapses, a node following its app re-arms the tombstone check (`rearm_app_check`), and the check after the lapse compares the `recreated` marker and stops. No acquisition happens in between (`app_refusal`).
- That leaves nodes that do not follow their app, and any gap in the above. There the version check is all that is left, and on S3 the `ETag` of a version 1 released record at the same generation is the same in both incarnations (a hash of the same bytes).

The design chosen, as the blob sweep already does for files (ADR 0030, `epochs_pass`): **the pass compares the app's epoch read after its acquisition with the one this runtime started with**, and compacts nothing on a difference. The epoch is `blobs::epoch` of the `recreated` and `restored` markers (`ActorConfig::blob_epoch`, from `AppRef::prepare_local`); the pass reads those two markers again, and also `deleted` and `restoring` (refused while either exists). Cost: eight small GETs per pass (the four markers before the acquisition, so a deleted or recreated app's record is not rewritten, and again after it). Read after the acquisition, the check holds for the whole pass: while this node's hold, and so its lease and heartbeat, is live, `app delete` cannot remove anything, and the deletes stop at the next batch once the lease lapses. Dropping the queue at the tombstone was added too; it is not what the proof rests on.

The test, `idle_pass::a_pass_deletes_nothing_of_a_recreated_app` (cluster suite): node `a` writes an actor over two generations and hibernates (one entry queued, passes paused). The test then deletes the actor's history and record and writes the `recreated` marker, as `app delete` and `deploy --recreate` leave them, without a tombstone a test node would see. A new node `c` writes another history for the same actor up to the same generation and releases it. The entry is given the record's current version (`idle_passes_take_current_versions`, what an `ETag` of identical bytes gives), and the pass runs. The acquisition takes the record (version and generation match), the epoch differs, and the outcome is `refused` with every object of the new incarnation still there. With the epoch comparison removed (both reads; and, in a first build, the one after the acquisition alone), the same test fails: `a`'s pass deleted the recreated app's objects. A whole-app restore in place changes the `restored` marker and is refused the same way.

### Tests, and whether each was seen failing

Each "fix" below was removed by hand and the test run again.

- **Restore identical** (`an_idle_pass_leaves_the_snapshot_and_its_segment_and_the_same_database`): two bursts on two generations, a window of 3 s, the pass. The prefix is exactly `S` and the segment at `P`; an export after the pass has the position and raw checksum of one taken before it, read at most two objects, and passed SQLite's `integrity_check` (`history::export` verifies); node `b` restores it and reads the same count (compared by count, not by checksum; the export's checksum is the byte-level comparison). `actor export --at` between the bursts gives the first burst's count before the pass. Without the pass the prefix keeps every object (the assertion on two objects is what fails).
- **Wake** (`a_wake_before_the_pass_leaves_every_object`): a read-only wake before the pass; every object stays and the outcome is `changed`. With both the version check and the generation check removed, the objects still stay (the read-only hold uploaded no snapshot of its generation, so `nothing`), and the test fails on the outcome: the chain's safety does not rest on the version, as the simulation found.
- **Drain** (`a_draining_node_runs_no_pass`): with the drain checks removed, the drained node compacted; with them, `refused` and nothing deleted.
- **Lapse**: `replica::tests::an_idle_collection_deletes_below_the_released_snapshot_and_stops_at_a_lapse`, a unit test with a store that lapses the lease at its first batch of deletes: 100 deleted, then nothing; with the check before each batch removed, it deletes past the lapse and fails. It also checks the kept set (the snapshot at the head, its segment, a later generation's segment), the window, and `NoSnapshot` for a generation that snapshotted nothing.
- **Readers**: the `--at` read above, and `an_export_racing_a_pass_publishes_the_same_database` (an export whose segment reads are held back 1.5 s while the pass deletes publishes the same database). There is no fix to remove; it checks the invariant. And `backup::tests::backup_create_during_an_idle_pass` (CLI crate, no node: the pass's `idle_collectable` deletes applied after the backup's listing, at its first read). `--history head` completes and verifies at the head. `--history all --require-continuous false` completes with the deleted position recorded as a gap. `--history all` by default is refused as partial, as it is when an ordinary collection lands between its listing and its reads (ADR 0033), and the next run completes. So a continuous `--history all` run that races a pass fails once: the pass deletes positions older than the window, which runs spaced closer than the window have already copied. Neither reader test has a fix to remove; the existing `collector_between_listing_and_get_forces_a_fresh_plan` and `history_all_reports_listed_get_miss` cover the same shape for ordinary collection.
- **The feed** (`feed::poll_tests::a_poll_after_an_idle_pass_reads_nothing_but_the_markers`, a store counting GETs): poll, compact with `idle_collectable`, poll again. The second poll reads the four app markers and nothing else, and writes no file. With the segment at `P` also deleted, the second poll reads history and the test fails.
- **Recreate**: above.
- **The idle-limit gate** (`a_class_with_an_idle_limit_is_not_compacted`): with the gate removed, the pass compacted; with it, `refused`.
- **The switch** (`no_pass_is_queued_with_idle_compaction_off`).
- **Queue** (unit): one entry per actor, due first; a full queue drops the entry due first.
- **`Owners::release_written`** (unit): the version written is the record's, an acquisition unchanged on it takes the record once, and the pass's own release changes it.
- **The golden trace of the pass** (`golden_trace::the_bucket_operations_of_an_idle_pass`, fixture `golden_trace_idle_pass.txt`), separate from the 0.4.2 trace, which is unchanged: the writes, the hibernation's snapshot, listing and release, then the pass's `PUT owner [update] holder=a generation=2`, one `LIST`, three `DELETE`s (the first snapshot and two segments below `S`), and `PUT owner [update] holder=- generation=2`.

### The simulations

`madsim_expiry.rs`, at 2,000 seeds:

- `an_idle_pass_and_an_expiry_race_and_the_version_decides`: nodes also queue, after each release of a hold that ran events, the version the release wrote (`LeaseNode::release_written`), and take it back 400 ms later with `acquire_unchanged`. New oracle: no version of one record is taken twice (by expiries, passes, or one of each). 5,327 expiries, 6,418 passes took their record, 48,885 found it changed, 3,324 versions were tried by both an expiry and a pass; no version taken twice, no active or alarmed actor expired.
- `an_idle_pass_that_acquires_as_an_event_does_is_caught`: caught at seed 0 taking a version an expiry already took.
- `a_pass_keeps_the_idle_origin_on_a_version_2_record`: scripted, 50 seeds each way. With the release's last-modified time as origin, the record the pass leaves carries it, and an expiry past the limit from the release chooses the actor although the pass rewrote the record; with no origin (the broken variant) it does not.
- The earlier cases are unchanged (5,862 expiries; the expiry's plain acquisition still caught as an active actor expired, at seed 10).

Not done: a recreate dimension in `madsim_replication.rs`. Its model has no app incarnations, and the epoch check sits outside the chain rule the simulation checks; the cluster test above is its evidence.

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
