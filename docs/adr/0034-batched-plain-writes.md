# 0034. Batched plain writes: one segment in flight per actor

- Status: proposed (2026-11-15). Written after an independent review of
  a first sketch, which found it sound for durable writes and named what
  it lacked: barriers for snapshots and resets, a structural rule for
  ending a hold, and an opt-in rollout, since batching changes what the
  docs promise about plain writes. Revised the same day after an
  adversarial review of this text against the code (alarm cleanup for an
  actor without state judged on local state; captures cut by a wall;
  failure seen mid-event; what "a barrier" can enforce in Rust; panics
  and parking; the mailbox's drop step racing new jobs; the index's
  requests per segment; the capability word's bootstrap; a mode change
  on a resident actor; uncatchable verification variants; several
  figures). Nothing here is built.
- Date: 2026-11-15
- Milestone: [After that](../../ROADMAP.md#after-that), "Faster, and
  compared": a plain write that does not hold its actor during the upload
- Amends, if accepted: [ADR 0007](0007-replication-protocol.md) (a
  segment carries the commits of several events; snapshots and resets
  only at the acknowledged head; the checkpoint sentence, corrected
  there already), [ADR 0009](0009-alarms-in-a-cluster.md) (an index
  entry published once per segment, retired after the segment's
  acknowledgement, the fence judged on the acknowledged head),
  [ADR 0033](0033-data-access-export-and-online-backup.md) (positions
  are segments), [ADR 0020](0020-one-lease-per-node.md) (its
  sections 3 and 4, by this record's sections 6 and 10: two ways to end a
  hold, a clean stop that flushes),
  [ADR 0014](0014-draining-a-node-and-moving-an-actor.md) and
  [ADR 0017](0017-moving-one-actor.md) (a hand-over flushes before it
  releases), [ADR 0024](0024-socket-home.md) (a hold is parked only once
  flushed), [ADR 0028](0028-actors-that-end.md) (a reset only at the
  acknowledged head) and [ADR 0002](0002-wasmkeep-actor-wit-v0.1.md)
  (one function added, `storage.flush`). [ADR 0012](0012-bucket-garbage-collection.md)'s
  rule is not changed: collection still follows an acknowledged snapshot,
  which section 7 keeps at the acknowledged head.
- Follows: [ADR 0005](0005-replication-format-ltx-v3.md) (a segment is one
  LTX file), [ADR 0006](0006-simulation-tool.md) (the simulation checks
  it before code).

## Context

Every writing event waits for its own segment before the actor runs its
next event. `ActorState::finish` (`actor/state.rs`) calls `replicate`,
which captures what the event committed (`ActorDb::take_changes`) and
awaits `Replica::ship` (`replica.rs`): one conditional PUT, bounded by
`put_bounded`, and the acknowledgement rule after it. The next event in
the actor's mailbox starts only once that returns. A plain `exec`
answers its caller before the upload, but the actor itself waits, so:

- **A busy actor makes about 17 writing events a second against a real
  bucket** (derived: one event per round trip; the durable write's
  measured p50 next to the bucket is 47 to 62 ms in the
  [soaks](../benchmarks.md#three-nodes-against-a-real-bucket), 57 ms in
  the 12-hour node-lease run, which gives about 17). Its
  plain writes go no faster than its durable ones.
- **One PUT per writing event** (measured: 1.03 PUTs per durable write
  over the 12-hour node-lease soak). On R2 and S3
  every PUT is billed.
- **A room on a local bucket stops at a few hundred messages a second.**
  The room benchmark of
  [PR #134](https://github.com/WasmKeep/wasmkeep/pull/134) (workstation
  trial, not fleet numbers; moto on loopback, 10 s of sending at an
  offered 1,000 messages a second) acknowledged 4,039 to 4,228 messages
  in 10 s per room of 10 or 100 sockets in its `plain` mode, and 4,050 to
  4,163 in `durable`, with the node at 14 to 30 % of a core; rooms of
  1,000 and 10,000 sockets (1,000 accepted) acknowledged 3,189 to 3,266.
  Without a bucket the same room of 10 or 100 sockets took all 10,000 at
  0.09 to 0.26 ms p50. These figures are in that PR's
  `bench/results/2026-11-15-699ebec-room-workstation*.csv`, not yet on
  `main`; they are cited here from the PR and are to be cited from
  `bench/results` once it merges.

What the code already gives:

- **The capture coalesces commits.** `WalCapture::capture`
  (`wasmkeep-storage/src/wal.rs`) reads every frame appended since its
  last position and keeps the newest version of each page up to the last
  commit frame. Captured after several commits, it yields one segment
  whose pre-apply checksum is the first commit's and whose post-apply
  checksum is the last's. It is called once per event only because
  `replicate` is.
- **Durable writes rest on the acknowledgement rule alone**
  (`Replica::acknowledged`: the PUT returned while the hold was valid)
  and on generations (ADR 0007). Neither depends on how many commits a
  segment carries.
- **The fork detector** in `Replica::ship` refuses changes whose
  pre-apply checksum is not the replicated head's.
- The roadmap lists "a plain write that does not hold its actor during
  the upload" under "Faster, and compared".

What it does not give, and this record must add (the review's findings):
nothing today stops a snapshot or a reset from being labelled at a head
the database has moved past (`ActorDb::snapshot` refuses only uncaptured
frames, and `Replica::ship_snapshot` labels at `self.head` without
comparing checksums); every site that ends a hold (`self.replica = None;
self.lease = None` and its variants) assumes nothing is left to upload;
and the docs promise that a plain write is lost with its node only
between its commit and the end of its event.

## Decision

### 1. Two class modes, strict the default

A class's writes are **`strict`** (today's behaviour, unchanged, the
default) or **`batched`** (opt in, per class). A third mode, **`gated`**,
is reserved for later (section 12, and Alternatives).

```toml
[classes.rooms]
component = "rooms/target/wasm32-wasip2/release/rooms.wasm"
writes = "batched"
```

and `deploy --writes rooms=batched` beside flags. A class without
`writes` is `strict`, so every class deployed today keeps its behaviour,
and an unnamed class stays strict through the release that brings this.
The mode is part of the class's version, like `access`, but a hold
reads it once, when it is acquired, and keeps it: a deploy that changes
a class's mode reaches a resident actor only at its next hold, or at the
next `Flushed` barrier (section 6), where the node switches it. Switching
from batched to strict with a segment in flight would send the next
event through strict's inline capture from the captured position, which
the fork detector refuses. On a single node
(no bucket) the mode changes nothing: writes commit locally as today.

Strict stays 0.4.2's code path, inline upload included, so that a test
can say "a strict class behaves as 0.4.2" and mean it. Everything below
is about batched classes unless it says otherwise.

### 2. The uploader: one segment in flight

Each actor of a batched class has at most **one segment in flight**.
This is group commit at the WAL: commits made while a segment is in
flight wait in the WAL, uncaptured, and the next segment takes them all.

- **Event boundaries.** The node records a boundary each time the
  actor's mailbox is between two jobs (after `finish`, after a host job)
  and at each flush point (section 4): the WAL position just after the
  last commit frame, and the alarm index entry the database names there
  (section 9). A segment always ends at a boundary, never in the middle
  of an event: a restore therefore always gives the state after some
  event or flush point of one owner, as today (`exec-durable` already
  cut events at its call).
- **Capture when the uploader is idle.** At a boundary, if nothing is in
  flight and something is uncaptured, the node captures up to the last
  boundary within the byte cap, encodes the segment at the next txid on
  the acknowledged head, and hands it to an upload task of its own,
  which first PUTs the segment's alarm entry when it has one to publish
  (section 9) and then the segment. The event's `finish` does not wait
  for it: the actor takes its next job.
- **A capture is never cut half-way.** Capturing advances `WalCapture`
  and may checkpoint, so a capture whose `Changes` were then dropped
  would leave the next segment's pre-apply checksum wrong and fail the
  replica. Capture, encoding and the hand-off to the upload task are
  therefore one step on a blocking job that is not cancelled with the
  guest call: a wall cut, a lease cut or `Cut::Replica` during a flush
  ends the guest's wait, and the step still completes and hands its
  segment over (or, if it fails, fails the replica as a failed capture
  does today). The same step serves `finish`, the completion job and the
  flush.
- **Capture again when it lands.** The upload task checks the
  acknowledgement rule as soon as the PUT returns (as `put_bounded` does
  now) and records the outcome. Then whichever comes first applies it: a
  completion job posted to the actor's mailbox (when the actor is
  between events), the next boundary, or a flush waiting on it. Applying
  an acknowledged outcome moves the acknowledged head and its checksum,
  writes the sidecar, runs the segment's retire list (section 9) and
  wakes the flushes it satisfies, then captures again if anything is
  uncaptured. Applying a failure fails the replica (section 8).
- **The byte cap**, 1 MiB of encoded segment (estimated during the
  capture at each boundary from the pages it would carry, page count
  times page size plus LTX headers, a bound on the LZ4-compressed size
  plus framing), so that `put_bounded`'s "per started megabyte" charges
  one: 21 s. A capture stops at the last boundary within it. An event
  that alone writes more is one segment by itself, as it is today:
  batching never makes a segment larger than strict would. The cap is a
  correctness bound, not a tuning: an upload's first attempt is given 5
  s plus a second per started megabyte, later attempts twice as long
  each, all of them within 20 s plus the same (`UPLOAD_IDLE`,
  `UPLOAD_BUDGET_IDLES`), and the bucket client gives one request 30 s.
  Without a cap a stalled minute of a busy actor would become one
  segment no attempt can carry.
- **Captured is in flight.** The captured position is always the end of
  the segment in flight, or the acknowledged head when nothing is in
  flight. Nothing is kept in memory between them but the one encoded
  segment: there is no pending set to merge.
- **A node-wide budget**: the encoded segments in flight of every
  batched actor together hold at most 256 MiB, as one node's restores do
  (ADR 0007, restore step 6). A segment larger than the budget takes it
  whole. A capture that does not fit waits for room, its commits still
  uncaptured in the WAL, and the event-start rule (section 3) holds the
  actor if the wait grows.

Segment keys, the LTX format, chaining, `plan_restore` and collection
are unchanged: one segment in flight keeps the chain a chain.

### 3. When an event may start, and the loss bound

One operator knob, **`--max-unsent-age`** (default 500 ms, from 100 ms
to 5 s): how long a committed write of a batched actor may wait unsent
behind the segment in flight. It is the "maximum unreplicated age" the
review asked for, stated as what the node controls: a write's
unreplicated age is its unsent age plus its own upload, and only the
first is the node's to bound.

**The event-start rule.** A batched actor's mailbox takes its next job
only while all three hold:

1. its oldest uncaptured commit is younger than `--max-unsent-age`;
2. its uncaptured pages are below the byte cap (the next segment is not
   already full);
3. its capture is not waiting on the node budget for longer than
   `--max-unsent-age`.

Otherwise the event that crossed the bound waits at its end, after its
answer, until the segment in flight lands and the next capture starts,
and the rule is checked again. There is no timer: the knob is
backpressure only. With a bucket whose round trip is shorter than the
knob it never acts. With a slower one the actor runs `knob` worth of
events per round trip and waits for the rest; a stalled bucket stops
the actor's events after `knob`, as strict stops them at once.

**The loss bound.** When a node dies, or a batched actor's hold ends
without a flush (section 6), what is lost is the segment in flight and
the uncaptured commits. By the rule above, with `r` the bucket's round
trip at the time and `e` the longest event:

- with a healthy bucket, the writes of about the last one or two round
  trips (`r` to `2r`): at 1,000 events a second and `r` = 55 ms, about
  55 to 110 events (derived);
- at worst, under a bucket that stalls, the writes committed from
  about `r` before the stall began (the segment then in flight) to
  `--max-unsent-age + e` after it, since no event starts after that;
- in bytes, at most one segment in flight (the cap, or one event larger
  than it) plus the cap and one event uncaptured.

A segment that also publishes an alarm entry costs two PUTs in a row on
its task (section 9), so for an actor whose every event changes its
alarm, `r` above is two round trips.

Strict loses at most the one event whose upload was under way. A
batched class trades that for throughput, and says so (section 12).

### 4. Durable writes: `storage.flush` and `exec-durable`

One function is added to the `storage` interface, after
`delete-self-durable`:

```wit
/// Returns once every write this actor committed before the call is
/// durable: in the bucket, uploaded while this node held the actor, when
/// the node has one; fsynced locally when it does not. Refused with
/// `not-allowed` inside an open transaction. `internal` means what was
/// committed since the last durable point is not known to be durable
/// (an upload failed or is still under way): fail the request; the
/// actor's state is then whatever the bucket holds.
flush: func() -> result<_, sql-error>;
```

- **Meaning.** `flush` returns `ok` once the acknowledged head covers
  every commit made before the call, each segment up to it having
  landed while the hold was valid (the acknowledgement rule, per
  segment: one in flight means each segment is acknowledged only after
  the one below it). It records a boundary at the call, publishing the
  alarm index first (section 9), then waits for the segment in flight,
  applies it, captures up to its boundary, uploads, and waits again.
  The flushing event does the completions itself: the completion job of
  section 2 queues behind the running event and would never run in
  time.
- **`exec-durable` is `exec` plus `flush`.** Its contract is unchanged:
  inside a transaction it takes only `COMMIT` (or `END`), outside one it
  refuses `BEGIN` and `SAVEPOINT`, and `ok` means durable. In a strict
  class it is today's code; in a batched one, `exec_commit` and then the
  flush above. In both modes the WIT stays as it is.
- **Inside a transaction**: `not-allowed`, nothing changes (nothing in
  an open transaction can be made durable).
- **On a single node**: the WAL is fsynced (`sync_wal`), as
  `exec-durable` fsyncs its commit.
- **In a strict class**: it ships this event's writes so far if there are
  any (every earlier event was acknowledged before this one started),
  or returns `ok` at once.
- **Nothing pending**: `ok` at once.
- **During a drain or a move**: the hand-over job runs after queued
  events (ADR 0014), so a flush inside an event completes before it. A
  hold that lapses meanwhile makes the upload `Late`: `internal`.
- **After a failure**: `internal` at once, and the event is cut (section
  8).
- **The event's wall**: a flush waits until the event's wall-clock limit
  less one second at most, then answers `internal` ("not known to be
  durable: still uploading"). Its uploads go on: they run on their task,
  not in the guest call, and a capture under way completes as one step
  (section 2), so neither that deadline nor the wall's own cut
  interrupts them, and the replica stays healthy. Today's marker ("an
  upload was interrupted", set around an inline upload so a cut fails
  the replica) applies to strict classes only.
- **`internal` means undetermined**: everything since the last
  acknowledgement may or may not become durable. The app fails the
  request and does not retry it in the same event. A new error kind
  would change the WIT's enum (a breaking change, below); `internal`
  with a fixed message prefix (`not durable:`) says it.

**Durable latency in a batched class**: one segment upload when the
uploader is idle, two when a segment is in flight (its landing, then the
flush's own), three at most when the uncaptured part was already full
(the cap, then the flushing event's own segment). A segment upload is
one round trip, or two when it also publishes an alarm entry (section
9). Without alarm changes its p50 lies between one and two round trips
and its p99 is two (derived). A durable write of a strict class keeps
today's cost: one round trip, plus the entry's when the alarm changed.

### 5. Two positions, two tripwires

The replica keeps the **acknowledged head** (`head`, its checksum, what
the sidecar names) apart from the **captured position** (the end of the
segment in flight). Every rule that today asks "the head" asks the
acknowledged head: the sidecar (`record`), `local_is_at`, the fence
(section 9), snapshots and resets (section 7), the next segment's
parent. Both tripwires stay:

- the capture chains from the captured position (`WalCapture`'s running
  checksum), so a segment's post-apply checksum follows the one before;
- a segment's pre-apply checksum must equal the acknowledged checksum
  when its upload starts (the fork detector in `Replica::ship`). One in
  flight makes both the same check at the moment a segment starts; a
  bug that started one early fails it.

### 6. Barriers: two ways to end a hold

A hold of a batched actor ends in one of two ways, and only through a
**`Barrier`** value: `Flushed` (nothing in flight, nothing uncaptured,
the acknowledged head is the local database) or `GaveUp(reason)`.

- **Drop now**: the hold can no longer acknowledge anything, so waiting
  is pointless. Empty the hold's cell first (ADR 0020, section 1: a PUT
  that lands afterwards returns `Late`), count what is dropped, release,
  never wait. Yields `GaveUp`.
- **Flush, then release**: the node chose to let the actor go. Run a
  flush with the site's deadline; `Flushed` releases (or parks); on a
  failure, drop now instead; past the deadline, the site's own answer
  (below).

What is enforced, since Rust cannot make dropping a value a compile
error: today a hold ends by being dropped (`self.lease = None`, and
`LeaseHold`'s `Drop` releases it). For a batched actor, dropping a
`LeaseHold` empties its cell and puts it into the pending releases
without parking it; only `release(Barrier)` and `park(Barrier)` end a
hold otherwise, and parking requires `Flushed`. Every site in the table
below calls one of them. A check in `./scripts/check.sh` refuses `lease
= None` and `replica = None` outside the module that owns the two
functions. `Replica`'s `Drop` counts a drop with anything pending and no
`GaveUp` seen (`reason="unbarriered"`) and logs an error; in debug
builds it also asserts, unless the thread is already panicking. Safety
does not rest on any of this (the cell is emptied first either way):
what the barrier protects is plain writes on clean hand-overs, and the
snapshot, reset and alarm rules below.

| Site (today's code) | Kind | Deadline | Past the deadline / on `GaveUp` |
| --- | --- | --- | --- |
| `ActorState::abandon` (`state.rs`): a failed upload or capture | drop now | none | counted `failure` |
| `ensure_owner_with`, "whatever ran before may be stale" (`state.rs`): a hold that lapsed | drop now | none | counted `lapse` |
| The node lease lost or fenced (ADR 0020, section 1: every hold dropped) | drop now | none | counted `lost_lease` |
| `finish`, the database worker failed (`state.rs`) | drop now | none | counted `failure` |
| A panic in an event (the mailbox's `CatchUnwind` drops the state) | drop now, never parked | none | counted `panic` |
| `hand_over` and `move_out` finding the hold lapsed (`drain.rs`, `moving.rs`) | drop now | none | counted `lapse` |
| The app deleted (`stop_actors`, 1001) | drop now | none | counted `app_deleted` |
| A restore that failed, or the previous connection still open (`ensure_owner_with`) | nothing pending | none | (a fresh hold, no event ran) |
| `alarm` for an actor without state (`without_state`, `alarm_delivery.rs`), which also runs after a trapped event | flush, then clean up and release | one upload budget | on failure drop now, nothing cleared; past the deadline nothing cleared, the hold kept, tried again |
| `delete_recorded` and `finish`'s durable delete, after the reset | `Flushed` by section 7 | none | the reset failed: drop now, `failure` |
| Idle hibernation and capacity eviction (`run_actor`, `wasmkeep-runtime`) | flush, then release | one upload budget | stays resident; tried again at the next idle check, or another actor is evicted |
| Parking with sockets open (`Drop for ActorState`, ADR 0024) | flush, then park | as hibernation | as hibernation |
| A drain's hand-over (`hand_over`, `drain.rs`) | flush, then release | 20 s from the job's start | `HandOver::Busy`: the hold stays, the next pass tries again |
| A move out (`move_out`, `moving.rs`) | flush, then record the move and release | 20 s | `MoveOut::Busy`: the move is tried again |
| A clean stop (`stop_actors`, `actor.rs`) | flush, then release | the stop's limit | dropped and counted `stop`, then released |

The mailbox in `wasmkeep-runtime` drops a state synchronously today
(`run_actor` returns, and `Drop for ActorState` releases or parks). It
gains an asynchronous step before that drop, run by the state, which
returns whether it may be dropped, ordered so that no second state can
be built beside it:

- the step runs **before** `run_actor`'s check that nothing is queued
  (`rx.is_empty()` under the registry lock), with the actor still
  registered;
- a job that arrives during it wins: the hibernation is abandoned (the
  uploads go on on their task), the job runs, and the idle wait starts
  over; the check is made again after any step;
- an actor with nothing pending passes the step at once, so a request
  reaching an idle actor never waits for a flush that has nothing to do;
- during a clean stop the mailboxes refuse new jobs (`enqueue` once
  closed), so no completion job can be posted: the step applies the
  completions itself, as a flush inside an event does;
- `stop_actors` takes whether to flush (a clean stop) or drop (an app
  deleted), since both use it.

A panic drops the state from `CatchUnwind`, outside the step: `Drop for
ActorState` then never parks a hold with anything pending (it empties
the cell and counts `panic`), whatever sockets are open. A batched actor
idle for the idle timeout (30 s) has nothing pending unless its bucket
is stalling, since the completion job captures and ships within a round
trip of its last event.

A move records its target and deadline (`Move.until_ms`, 30 s) **after**
the flush, so the flush does not use the routing window. `HAND_OVER_WAIT`
and `MOVE_WAIT` (30 s) count the job's flush with the events before it;
a flush that outlasts them leaves the job queued, which finishes it or
answers `Busy` when its turn comes, as a long event does today.

### 7. Snapshots and resets only at the acknowledged head

A snapshot or a reset labelled at a head the database has moved past is
a broken history: a snapshot of `n + k` labelled `n` makes collection
delete what lies below it, then segment `n + 1` carries `n`'s pre-apply
checksum, the restore's checksum check fails, and the actor is degraded
for good. So:

- **`ship_snapshot` refuses** unless nothing is in flight and the
  snapshot's post-apply checksum equals the acknowledged checksum. The
  existing refusal of uncaptured frames (`ActorDb::snapshot`'s
  `CaptureError::Pending`) stays as the second guard. This applies to
  the periodic snapshot and the one after a delete, which are labelled
  at the head; the first snapshot (`SnapshotKind::First`) is at a new
  position on an empty bucket and keeps today's rule.
- **`snapshot_due` is asked only inside a barrier.** When a periodic
  snapshot is due (64 segments since the last, counted on acknowledged
  segments), the actor's next mailbox gap runs a flush and then the
  snapshot, under today's bounds (one short attempt, the retry waits of
  `PERIODIC_SNAPSHOT_IDLES`). Events queue behind it meanwhile, as they
  queue behind a periodic snapshot today.
- **`ship_reset` refuses** unless nothing is in flight, and chains on the
  acknowledged checksum (as it does now: `pre_apply: self.checksum`).
  Its users flush first: `delete-self` at the end of the event,
  `delete-self-durable` in its call (a flush that fails makes it answer
  `internal`, "calling this again is safe", as today), the operator
  delete's job, and ADR 0033's restore of one actor. The reset's txid is
  then the acknowledged head's plus one, which ADR 0033's
  `replaced = delete <txid>` label already assumes. Flushing data that
  the reset then erases costs one upload; only the in-flight segment
  must be resolved for correctness (its key would be the reset's), so a
  later refinement may skip the uncaptured part.
- **The first snapshot** (an actor new to the bucket, `start_replication`)
  is taken before any event, with nothing in flight, as today.

A restore replays every segment since the last snapshot, as now: 64
while periodic snapshots succeed, more while they back off after
failures (`PERIODIC_SNAPSHOT_IDLES`). Each segment now holds up to the
cap, or one event larger than it, so the 64 are up to 64 MiB plus such
events, where today they are 64 events.

### 8. A failed replica cuts the running event

In a strict class an upload fails between events. In a batched one it
fails while the next event runs, on a database ahead of anything that
can now be acknowledged. That event must not answer as if it had
succeeded, call other actors or send on sockets from that state:

- The upload task publishes a failure at once, on a flag the event can
  see (a watch beside the replica), separately from applying the outcome
  (section 2), which waits for a boundary.
- `limit` (`actor/host.rs`) gains a third arm beside the wall and
  `Cut::Lease`: **`Cut::Replica`**, which selects on that flag. The
  instance is discarded, as for any cut.
- Every call with an effect outside the actor checks the flag first and
  refuses once it is set, since the cut is asynchronous: every storage
  call (`internal`, "replication stopped", as `exec-durable` alone does
  now), `actors.call` and `calls.call-many`, and socket sends.
- No queued event starts before the actor is given up: the cut event's
  `finish` sees the failure and drops now (`abandon`). A failure applied
  while the actor is between events is handled by its completion job,
  which abandons.

### 9. Alarms (amends ADR 0009)

ADR 0009's index stays: fresh keys (R1), delete only what the replicated
state no longer names (R2), never an entry of a newer generation (R3),
an alarm from before joining listed before the first upload (R4). What
changes is when the PUT and the deletes run, since a segment now
carries several events and only its end can be restored:

- **Name at the event, publish at the segment.** At each event's end, as
  today, the node compares the committed alarm with the entry the
  database names (`needs_publish`); if they differ it chooses a fresh
  key now (this hold's generation, a new nonce) and writes it into the
  `alarm_indexed` meta row, a local commit before the event's boundary.
  It does not PUT it then. When a segment is prepared, the entry named at
  its end boundary is PUT before the segment's upload starts, if it
  differs from the entry the acknowledged head names. Entries named only
  inside a segment are never PUT: no restorable state names them. A
  failed PUT fails the replica (section 8), as ADR 0009 gives the actor
  up today when the index cannot be brought in line. So every state that
  can be restored, or land late, names an entry that exists.
- **Where the requests run.** The entry's PUT runs on the segment's
  upload task, before the segment's PUT and within the same upload
  budget (today's `put_new_alarm_entry` has no bound of its own beyond
  the client's 30 s per request; it gets the upload's). It is skipped
  when the entry equals the one the captured position names (the
  segment before it already published it). The retire list's DELETEs,
  and `clear_stale_hint`'s LIST, run on the task after the
  acknowledgement, off the actor's path.
- **Retire after acknowledgement.** Each segment carries a retire list:
  the entry the acknowledged head named (when the segment names another)
  and, after an alarm delivered for a time no longer committed, that
  time's stale entries (`clear_stale_hint`). The list runs only when the
  segment is acknowledged, with today's R2 and R3 checks; on a failure
  it is dropped (an extra entry is harmless: it is delivered and removed
  as unnamed).
- **Every R2 decision uses the acknowledged head's entry.** What "the
  replicated state names" is the entry the acknowledged head names, never
  the local database's: `clear_stale_hint`, `entries_to_clear` and the
  premise of `may_index` ("everything the actor's database records is in
  the bucket") hold only at a `Flushed` barrier. So `without_state`,
  which runs whenever there is no instance (also after a trapped event,
  with the hold and a segment possibly in flight), flushes first and
  judges then; if the flush fails it drops now and removes nothing.
  Otherwise an alarm set by a trapped `on-init` and cleared by its retry
  could have its entry removed while the acknowledged head still holds
  the alarm.
- **The fence on the acknowledged head.** `fence` answers "the history
  ends in my generation" from the acknowledged head only. When it must
  upload (`owner_fence`), it writes the row and flushes. A fence judged on
  the captured position would let R2 remove an entry that a late upload
  of the previous owner can still name.
- **`exec-durable` and `flush`** publish through the same path: the
  flush records a boundary, the entry named there is PUT when its
  segment is prepared.
- **The local timer** is armed from the committed local alarm at each
  event's end, as today. So an alarm set by an event whose segment never
  lands may fire on this node, and its `on-alarm` run, before its node
  dies and the setting is lost with it. This is listed in section 12.

### 10. Clean stop, rolling upgrades and the drop counter

A clean stop (ADR 0020, section 4) becomes: stop admitting events
(the mailboxes close, as now); let the queued events finish; flush every
resident batched actor **in parallel**, each as its last event ends;
release each hold as its barrier is reached; at the stop's limit,
whatever is not acknowledged is dropped, counted, and its hold
released; then the node lease's `released` write, as now. The limit
stays `SHUTDOWN_GRACE` (10 s) for events and flushes together. With a
healthy bucket a flush takes a round trip or two; a stop under a slow
bucket drops, and says how much.

**`wasmkeep_plain_events_dropped_total{reason}`** counts the events whose
writes were committed but not acknowledged when their hold ended without
a flush: `stop`, `failure`, `lapse`, `lost_lease`, `panic`,
`app_deleted`, `unbarriered`. A rolling upgrade with a healthy bucket
shows zero. A node killed outright counts nothing (it is dead); the soak
measures that loss from the clients.

### 11. The wall arithmetic

At the defaults: an event's wall 30 s; an upload's budget 4 × 5 s plus
a second per started MiB, 21 s for a segment at the 1 MiB cap.

| | Strict (today) | Batched |
| --- | --- | --- |
| Durable write, healthy bucket | 1 round trip | 1 to 2 round trips (3 at most) |
| Durable write, worst bounded case | 21 s (one upload), within the wall | 42 s (the segment in flight, then its own), 63 s with a full backlog: past the wall, so the flush answers `internal` at 29 s |
| An event larger than the cap | 20 s + 1 s/MiB, as today | the same, after the segment in flight |
| A drain's or move's hand-over | release only | a flush within 20 s, then `Busy` |
| A clean stop | events within 10 s | events and flushes within 10 s, the rest dropped and counted |

So under a slow but working bucket, a batched class's durable writes
answer `internal` sooner than a strict class's (one budget fits the wall,
two do not). With a healthy bucket the difference is one round trip at
p99. The cap could shrink to bring two budgets under 30 s only by going
below what a budget already covers (20 s is the base, before any byte),
so it does not try: the flush's own deadline is the answer.

### 12. What becomes observable in a batched class

The docs say today that a plain write can be lost if its node dies
between its commit and the end of its event
([cluster](../cluster.md#replication),
[failover](../failover-and-scaling.md)). In a batched class:

- **Later events see unacknowledged writes.** Their answers, their calls
  to other actors and their socket messages can rest on state the next
  owner never sees. A client can read a value, and read an older one
  after a failover (reads are not monotonic across a failover).
- **Another actor can make durable what this one loses.** Actor B runs
  `exec-durable` on data it read from A; A's write is then lost with its
  node.
- **Counters and ids can be handed out again**, up to the events lost
  (the soak's `plain_duplicates`).
- **Broadcasts of lost messages**: sockets saw a message whose write was
  then lost; after the failover the room numbers another message the
  same.
- **A bucket stall on a live node** gives up every busy batched actor's
  segment in flight and uncaptured commits at once, where strict gives
  up one event each.
- **An alarm can fire, on this node, from a setting never acknowledged**
  (section 9).
- **History has fewer positions**: one per segment, not per event
  (section 14).

Nothing changes for durable writes, for strict classes, or for clean
hand-overs with a healthy bucket, which lose nothing. A later mode,
**`gated`**, would hold an event's answers, socket sends and outgoing
calls until its segment is acknowledged, host-side, with no WIT change:
batched throughput, durable-like visibility, a round trip of latency.

### 13. Checkpoints (corrects ADR 0007)

ADR 0007 said the replicator checkpoints once a segment is uploaded and
the WAL holds only shipped frames. The code checkpoints after a capture,
before the upload: `ActorDb::take_changes` runs `wal_checkpoint(TRUNCATE)`
once the WAL file reaches 4 MiB (`CHECKPOINT_AFTER_BYTES`, or the WAL
cap if lower) with no transaction open, and `ActorDb::snapshot`
checkpoints before it reads every page. It is safe: a local file is
trusted again only when its sidecar names the chain's head and its
checksum is the sidecar's (`local_is_at`), so a file checkpointed ahead
of the bucket is downloaded again. ADR 0007's text is corrected in place
(2026-11-15).

In a batched class one more condition holds: a checkpoint runs only
after a capture that reached the WAL's last commit frame. A checkpoint
with uncaptured commits would move them into the main file, and no
capture would ever see them.

### 14. Transaction ids are segment counters (amends ADR 0033)

A key's `txid` has always counted segments, one per upload. In a batched
class a segment holds several events, so:

- ADR 0033's positions (`--txid N`, `actor history`, a backup's points,
  the change feed's units) are **segments**; a point between two events
  of one segment cannot be chosen.
- **A segment's stamp is its capture time** (the LTX `timestamp_ms` set
  when it is encoded, as now), which is at or after every commit it
  holds. So `--at T`, "the newest position whose segment and every one
  before it carry a stamp at or before `T`", never includes a commit
  after `T`, and may leave out the commits of up to one segment before
  it.
- Snapshot waits (`SnapshotWait.txid`) and "every 64 segments" count
  segments, as they do.

### 15. The WIT, versions and deploys

`storage.flush` is an addition: the package stays
`wasmkeep:actor@0.2.0`, and a component built before links as it did
([languages](../languages.md#versions), ADR 0002's "small now, additive
later"). No error kind is added (section 4).

TypeScript and Python components import the whole `storage` interface,
so every one built with the SDK that adds `flush` needs a node that has
it, even if it never calls it; Rust, Go, C and Zig components import
only what they call. 0.4.2 lived with this for `delete-self-durable`
by documentation alone. This record proposes that `deploy` checks it:

- A node whose binary has `storage.flush` and batched classes announces
  the capability word **`flush`** on its node lease's capability line,
  from the binary alone, whether or not it serves a batched class yet,
  as `ends` and `front` are (beside `take`, `front`, `handoff`, `ends`;
  readers ignore words they do not know).
- `deploy` reads each component's imports (wasmtime's
  `Component::component_type`, as the front's link check does) and
  refuses a version in which a component imports `storage.flush`, or a
  class has `writes = "batched"`, while a live node of the app does not
  announce `flush`, naming those nodes. The same check covers
  `delete-self-durable` with the word `ends`.
- A class with `writes` makes the manifest format 5, which older nodes do
  not read (the precedent is format 4, `expire-after-idle`).
- An older node that starts after the deploy cannot link such a
  component and refuses that class's events, as today. Upgrade every
  node, then deploy.

Mixed versions read each other's history: keys and LTX are unchanged, and
a segment of several commits is an ordinary LTX file to a node of 0.4.2.

### 16. Metrics

Beside the drop counter (section 10): `wasmkeep_unsent_age_seconds`, a
histogram of the oldest uncaptured commit's age at each capture;
`wasmkeep_segment_events` and `wasmkeep_segment_bytes`, histograms per
segment; `wasmkeep_batched_waits_total{reason="age|bytes|budget"}`, the
event-start rule acting; `wasmkeep_flush_seconds`; and the node budget's
bytes in use. The soak reads them.

## Before and after

Measured figures name their source; the rest are derived, and nothing
measures this design yet. The room's `batch-` mode in PR #134 batches in
the app (messages held in memory, one `exec-durable` 50 ms after the
first): it is an analogue of what a batched class does in the host, not
a measurement of it.

| | Today (strict) | Batched |
| --- | --- | --- |
| Durable write p50 next to the bucket | 47 to 62 ms, measured (soaks, VPS) | 1 to 2 round trips, about 55 to 110 ms; one more per segment that publishes an alarm entry; derived |
| Writing events a second, one busy actor, real bucket | about 17, derived from the durable p50 | bounded by the event's own cost, derived |
| PUTs per writing event | 1.03, measured (12-hour node-lease soak) | one per segment, two when its alarm entry changes: about one per round trip per busy actor, derived |
| Room of 10 or 100 sockets on moto, offered 1,000 msg/s | 404 to 423 acknowledged a second, measured (PR #134, workstation) | up to the 1,000 a second the same room took without a bucket, measured there; derived as the ceiling |
| Room of 1,000 sockets | 326 to 327 a second on moto, measured | the broadcast is the limit: without a bucket it fell behind at 1,000 a second (measured), so no better than that |
| App-level batching, 1,000 msg/s into 1,000 sockets | 36 ms p50, 66 ms p99, measured (`batch-` mode) | not needed for throughput; still for one frame per socket per batch |
| Plain write's answer | after the commit | unchanged |
| Lost at a node's death, per busy actor | the one event uploading | about the last one or two round trips of writes; at worst `--max-unsent-age` plus one event and one round trip of them (section 3), derived |
| Replayed after the last snapshot | 64 segments of one event each | 64 segments of up to 1 MiB each |
| A strict class | | unchanged |

On the VPS, at 1,000 events a second for one actor, PUTs would fall
from 1,000 a second (if the actor could keep up, which today it cannot)
to about 18 (derived), the segment holding about 55 events of a few
pages each, far under the cap. An actor whose every event changes its
alarm makes two PUTs per segment in a row, so about 9 segments and 18
PUTs a second, each segment holding about 110 events.

## Consequences

- A busy actor's write rate stops being its bucket's round trip, for the
  classes that opt in. Its PUTs, and on R2 and S3 its bill, follow round
  trips instead of events.
- A batched class loses more on a crash and shows more before it is
  durable (section 12). Its docs carry the loss bound in time.
- Durable writes keep their guarantee in both modes; in a batched class
  they cost up to one more round trip, and under a slow bucket answer
  `internal` sooner (section 11).
- Every end of a hold names its kind, in the type system.
- A clean stop and every hand-over flush; a stop under a slow bucket
  counts what it drops.
- Strict classes behave as 0.4.2, which a test checks; they still pass
  through the new plumbing (barriers at every release site, the
  mailbox's step before a drop, `Cut::Replica`, `storage.flush`, the
  deploy check), each a no-op for them.

## Alternatives

- **Several segments in flight**, with restore cutting at the first
  hole. Rejected. Segment `n + 1` landing without `n` is a missing link,
  which restore treats as a broken history (ADR 0007, restore step 4)
  and degrades the actor for good. Cutting at the first hole instead
  would make the planner unable to tell a hole left by a lost upload
  from one left by corruption or a collection bug, weakening the only
  tripwire the chain has; a durable acknowledgement would need every
  lower segment acknowledged too, so it gains nothing for durable
  writes; and every rule above (snapshot labels, resets, the fence,
  retire lists) would need "the highest contiguous acknowledged
  segment". The gain is small: one segment in flight already lets an
  actor run at its own speed until it writes more than the cap per round
  trip (about 18 MiB a second at 55 ms).
- **A pending set in memory** (the first sketch): events' changes kept
  and merged in memory while a segment is in flight. Rejected for the
  WAL: the merge must be right across commit sizes and a shrinking
  database (`WalCapture` already is), and the memory grows with the
  backlog. The WAL holds the backlog for free.
- **A time window** (flush every N ms). Rejected: it adds the window's
  latency to an idle actor's durable write, where group commit adds
  nothing; the knob of section 3 acts only under backpressure.
- **Batched by default.** Rejected for this release: it changes what the
  docs promise about plain writes for every app at once (section 12).
  Flipping the default needs soak data on the loss bound and docs that
  state it in time.
- **`gated` now.** Deferred: it needs every outgoing effect (an answer,
  a socket send, a call) held host-side per event, which is a design of
  its own.
- **Leave it to apps** (the room's `batch-` mode). It works today and
  keeps its own frames per batch, but every app must write it, and it
  holds unwritten messages in the instance's memory, where a trap loses
  them.

## Verification (to build)

### Simulation

`sim/tests/madsim_replication.rs` gains batched owners: events commit
locally, one uploader task, flushes waiting, crashes with a segment in
flight and commits uncaptured, lease cuts, late PUTs and collection.
`sim/tests/madsim_alarm_index.rs` gains the publish, retire and fence
rules of section 9.

Invariants, checked over 200 seeds per push:

- every `flush` and durable write acknowledged is in the final restore,
  with batching, crashes, lease cuts, late PUTs and collection;
- `plan_restore` never finds a link missing, and no checksum fails;
- the restored state is a prefix of one owner's sequence of events and
  flush points;
- the plain writes lost per ownership change stay within section 3's
  bound;
- no alarm in a restorable state without its index entry;
- a deleted actor never comes back;
- no plain write is lost on a clean hand-over (drain, move, hibernation,
  park, clean stop) while the bucket is healthy;
- no outward effect (an answer, a call to another actor that writes
  durably from what it was told) happens after the replica's failure is
  recorded.

Each of these variants must be caught by some seed:

- a durable write acknowledged when only its own segment landed (two in
  flight, the lower one lost);
- a flush acknowledged from the captured position;
- a release without a barrier;
- a snapshot taken with a segment in flight (section 7's guard removed);
- a reset chained from the captured position;
- an alarm entry retired before its segment's acknowledgement;
- the fence judged on the captured head;
- two segments in flight;
- an event started after the replica failed (caught by the outward
  effect invariant);
- a release without a barrier (caught by the clean hand-over invariant);
- a capture dropped half-way by a cut (the next segment's checksum);
- a mode switched with a segment in flight;
- `without_state` judging R2 on the local database with writes
  pending.

The test fails if late batched segments, or crashes with a segment in
flight and commits uncaptured, stop occurring: it cannot pass
vacuously.

### Storage

- A capture with a byte limit stops at a boundary, never inside an event,
  and takes an event over the limit alone.
- A property test: segments captured at any choice of boundaries,
  applied in order, give the database and checksum of capturing each
  event alone, across commit sizes and a database that shrinks
  (`WalCapture`'s page sums).
- A crash at each point (captured, checkpointed, PUT sent, PUT landed,
  sidecar written) leaves `local_is_at` right: the local file is used
  only when it is the acknowledged head.
- No checkpoint while a commit is uncaptured.

### Cluster tests

- Kill a node with segments in flight: every actor restores a consistent
  prefix.
- Stall the bucket on a live node: the event-start rule holds the actor,
  the failure cuts the running event, and a waiting durable write
  answers `internal`.
- A drain, a move, a hibernation, a park and wake, and a clean stop lose
  nothing with a healthy bucket.
- A clean stop with a slow bucket counts its drops.
- A durable write behind a segment of several MiB answers `internal` at
  the wall; the actor survives and its uploads land.
- An alarm set by every event: each entry retired only after its
  segment's acknowledgement.
- `delete-self`, `delete-self-durable` and ADR 0033's restore of one
  actor with a segment in flight.
- A periodic snapshot under load, then a takeover.
- A panic with sockets open and a segment in flight: the hold is not
  parked, the drop is counted.
- An idle actor hibernating while a request arrives during its step
  before the drop: one state, the request served.
- A wall cut during a flush's capture: the replica stays healthy and the
  next segment follows.
- A redeploy from batched to strict on a busy actor: the switch waits
  for a `Flushed` barrier, nothing is given up.
- A strict class behaves as 0.4.2 (the existing replication tests, run on
  a strict class).
- `flush` inside a transaction, on a single node, with nothing pending,
  and after a failure.
- A mixed cluster: a 0.4.2 node restores batched segments.
- `deploy` refuses a component importing `flush`, and a batched class,
  while a node without the word `flush` is live, and accepts the first
  batched deploy on nodes that all have it.

### Soak

`crates/wasmkeep-cli/tests/soak.rs` runs a batched class beside the
strict ones, with the same kills: `plain_lost` judged against the kills
times the per-actor bound of section 3, durable loss judged at zero in
both modes, durable p50 and p99 reported for both, and the metrics of
section 16 sampled at each kill.

## Docs to change when accepted

Not changed while this is proposed: [concepts](../concepts.md#storage)
(the `exec`, `exec-durable` and new `flush` bullets; class modes),
[cluster](../cluster.md#replication) (the bullet on `storage.exec`'s
upload), [failover](../failover-and-scaling.md) (what a plain `exec`
can lose), [the project file](../project-file.md) (`writes`),
[monitoring](../monitoring.md) (section 16),
[languages](../languages.md#versions) (the addition and the deploy check),
the API reference, and the WIT's own comments on `exec` and
`exec-durable`.

## Open questions

- The defaults: `--max-unsent-age` 500 ms and the 1 MiB cap, to be set
  from the soak.
- Whether 10 s is enough for a stop's flushes, or batched classes want a
  longer stop limit (systemd's default stop timeout is 90 s).
- When, and on what soak evidence, `batched` could become the default.
- `gated`'s design.
- Whether a reset should skip uploading the uncaptured commits it erases
  (section 7).
- Whether strict classes should move onto the uploader task once the
  "strict behaves as 0.4.2" tests exist, to keep one code path.
- Whether the knob should be per class rather than per node.
- The room figures, once PR #134 merges, cited from `bench/results`.

## Plan

1. The simulation of the uploader, the barriers and the alarm rules,
   with every variant above caught, before any server code.
2. `WalCapture` with boundaries and the byte cap; its storage tests.
3. The uploader task, the two positions and `Cut::Replica`, behind the
   class mode.
4. `Barrier` and the release sites, the mailbox's step before a drop.
5. Snapshot and reset guards; the alarm rules.
6. `storage.flush` in the WIT and the SDKs (Rust and TypeScript first,
   as for `delete-self-durable`), `exec-durable` as exec plus flush; the
   capability word and the deploy check.
7. The cluster tests, the metrics, the soak with both modes, then the
   docs above.
