# 0012. Garbage collection of actor history in the bucket

- Status: accepted and implemented (2026-11-05: all five steps of the plan below;
  the open questions are decided below). Revised after an
  adversarial review of the design and one of each code step.
- Amended by [ADR 0020](0020-one-lease-per-node.md) (2026-11-07): node
  leases, ownership records and the `leases/` markers are lease state,
  never collected (see "Not collected here").
- Amended by [ADR 0022](0022-app-owned-state-and-the-one-time-migration.md) (planned, 0.4.0): history collection walks the app's root, `state/<tenant>/<app>/`.
- Date: 2026-11-05
- Milestone: [M3](../../ROADMAP.md#m3-first-public-release)

## Context

Every write an actor commits in a cluster becomes one object in the bucket
(a segment), and every 64 segments the owner adds a snapshot
([ADR 0007](0007-replication-protocol.md)). Nothing is ever deleted: not
old segments, not snapshots no restore will start from, not stale uploads
that landed after their writer's lease ran out, and not a deleted actor's
history (a delete only adds a segment resetting the database).

Periodic snapshots bound what a restore *replays* (fewer than 64 segments),
but not what it *lists*: restore lists the whole prefix, twice when there
are segments to download (once since 2026-11-07, ADR 0007). Measured in
memory ([benchmarks](../benchmarks.md#restore-against-history-length)),
a restore at 10,000 writes takes 6.1 ms against 2.6 ms at 1,000
while replaying fewer segments; against S3 or R2, which page listings by
1,000 keys, each listing of 10,158 objects is 11 requests. Storage grows
the same way, and "deleting an actor" does not erase its data.

ADR 0007 already states the intent: garbage collection deletes segments and
snapshots not needed to restore the current head, after a retention
window, never on the write path. This ADR decides how, so that it stays
safe under the faults ADR 0007 tolerates: uploads that land after their
writer's lease ran out, a new owner that has not uploaded yet, and deletes
that land late.

Two facts of the current code shape the rule:

- **The planner picks the head from segments first.** The head is the
  newest segment of the highest generation; a snapshot is the head only
  when the prefix has no segment at all (`plan_restore`). Walking back from
  the head, restore stops at the first snapshot on the way.
- **A periodic snapshot sits at the head's position.** It is taken after an
  event, every 64 segments (the code counts segments only; ADR 0007's "or
  64 MB" is not implemented), of the database at the current head
  `P = (generation, txid)`, and stored as `snapshot/<txid>-<generation>`:
  the same position as the segment that is the head (at 64 segments since
  the last snapshot the head is always a segment). Its generation is the
  owner's only if the owner has uploaded a segment since restoring: a
  restored replica starts its count at the number of segments after the
  snapshot its plan started from (`since_snapshot = plan.segments.len()`),
  so after restoring 64 or more such segments the next event takes a
  snapshot even if it wrote nothing, at the previous owner's head.

## Decision

### The rule

An owner with lease generation `h` collects an actor's history only:

1. after a periodic snapshot `S` at position `P = (h, t)` that this owner
   uploaded and that was **created and acknowledged** by this call: the
   `put` returned success while the lease was valid (ADR 0007's rule), not
   `Exists`, not `Late`;
2. with `S`'s generation equal to `h` (the snapshot is at a segment this
   owner wrote, never at the previous owner's head);
3. while its lease is valid, checked again before each batch of deletes.

Condition 2 is what makes the rule safe; condition 1 keeps it simple.
Without condition 2 a restore can break (found by the review): `m`'s
upload of `(m, x)` is held; `j` (with `m < j < h`) restores to
`(m, x-1)`, uploads `(j, x)` on top of it (also held), and its lease runs
out; `(m, x)` lands; `h` restores to `(m, x)` and, having restored 64
segments, snapshots there without writing anything. Collecting from that
snapshot would delete `(m, x-1)`, and when `j`'s upload lands it is the
head (its generation is higher than `m`'s), a legal late commit, whose
parent is gone. With condition 2, `S` is at a segment `h` wrote, after
which only uploads built on `h`'s chain can become the head. Condition 1
is not needed for data: a snapshot that landed late is still the right
database at `P`, and `P` itself was acknowledged; at worst an owner that
listed before it landed fails a download and retries. It is kept because
`Exists` and `Late` are rare and refusing them costs nothing.

It then deletes, under the actor's prefix, every object that is:

- a segment or snapshot with txid below `t`, of any generation; or
- a segment or snapshot of a generation below `h`, at any txid;

except `S` itself and **the segment at `P`**. Everything else is kept:
`S`, the segment at `P`, and this owner's segments after `t`.

The rule is a pure function of key names and the owner's generation and
snapshot (`wasmkeep_cluster::replication::collectable`, next to
`plan_restore`), so the simulation runs the same code as the server.

### Why it is safe

The invariant: after any collection, and whatever lands late, the restore
plan of the prefix still exists (`plan_restore` never returns `Missing`)
and still contains every acknowledged write (ADR 0007).

- **The chain a later owner restores is never touched.** `S` and the
  segment at `P` were acknowledged, so they were in the bucket before any
  later generation `k` could acquire, and `k` lists after acquiring. Its
  head is `h`'s newest segment (or its own, built on it); walking back it
  reaches `P`, where `S` is, and stops. Every object on that walk has
  generation `h` and txid `t` or above (or is `k`'s own): none is
  collectable. So a delete of `h`'s that lands late, after `k` restored,
  removes nothing `k` built on.
- **Late uploads of earlier generations cannot become the head.** A
  segment of generation `m < h` landing after the collection loses to the
  segment at `P` (and anything after it), which has the higher generation.
  This is why `P` is kept: with only `S` left, the prefix would have no
  segment of generation `h`, and a late segment of `m` would become the
  head, with its parent collected (`Missing`). A late snapshot of `m` is
  never a head while any segment exists, and is off the chain.
- **No fence is needed.** ADR 0009's fence exists because a new owner that
  has uploaded nothing cannot tell whether an earlier owner's late upload
  will become the state. Here collection only follows this owner's own
  acknowledged upload (`S`, at a segment it wrote), after which no upload
  of a lower generation can be the state.
- **Late uploads of this owner's own** (an upload cut by the event's wall
  clock, landing later) are at txid above `t`, generation `h`: kept.

### Deleted actors

A delete ships a reset segment at `(h, r)` and nothing else, so it would
never trigger a collection. After an acknowledged reset, the owner uploads
a snapshot at `(h, r)` (the empty database) and collects **without the
retention window**: erasing the actor's data is the point of the delete,
and an actor that was deleted gets no later pass. What remains is the
reset segment and that snapshot, two small objects, which keep the actor
known to exist (`exists_in_bucket`, access mode `existing`) and restore to
the empty database. The actor's lease object stays too (a few bytes; not
collected by this ADR). (The doc comment of `exists_in_bucket` says a
delete leaves an empty snapshot; it leaves a reset segment, corrected with
the server step.)

### When it runs

- One collection pass per actor, in the background, after a periodic or
  delete snapshot that qualifies under the rule: never inside an event,
  never delaying an acknowledgement. At most one pass per actor at a time:
  a pass requested while one runs (a periodic snapshot, then the delete in
  the same event) runs after it, never skipped. A pass in flight when the
  actor is given up stops before its next batch.
- A pass lists the prefix once, computes the collectable keys (a pure
  function of key names, `collectable`), and deletes them in batches
  (`delete_stream`, a bulk request on S3), checking the lease before each
  batch. Objects younger than the retention window (their `last_modified`
  in the listing) are kept for a later pass (except after a delete).
- Cost: one listing per 64 writes, and one delete per object ever written
  (once). A listing covers the whole prefix, about one retention window of
  writes for a busy actor, so passes do not queue up: while one runs, a
  single request waits, replaced by any newer one (its snapshot is newer,
  so it collects at least as much). After a delete, a failed pass is
  retried a few times while the node holds the actor. (From the review of
  step 3.) Failures are logged and counted; the next qualifying snapshot
  tries again.

### Deploy artifacts (if in scope, see open questions)

`apps/<tenant>/<app>/<version>/` holds each deployed version's components
and manifest, linked by `previous`. Keep the current version and the
versions its manifest chain reaches within `N` steps (rollback walks that
chain), delete older versions after the retention window. Nodes load only
the version `current` names, so a node that has not caught up yet is not
affected. Run by `wasmkeep deploy` after it replaced `current`, not by
nodes.

### Not collected here

Heartbeats of retired nodes (`nodes/<id>`), leases of deleted actors, and
alarm index entries (ADR 0009 removes those itself) are small and bounded
by the number of nodes and actors; they are left for later.

Amended by [ADR 0020](0020-one-lease-per-node.md) (2026-11-07): node
leases (`nodes/<id>`, format 3), ownership records (`owners/...`) and the
markers that replace per-actor leases (`leases/...`, `wasmkeep-lease 2`)
are lease state, never collected. A record may name a node long after it
stopped, and only that node's object, fenced or released, frees it; a
marker is what keeps a binary from before ADR 0020 from taking the actor.
`wasmkeep cluster forget` removes only a retired node's heartbeat in the
format from before node leases.

## Plan

One PR each, with tests that fail before it:

1. This ADR; ROADMAP and `docs/cluster.md` ("Not yet") link it. (Done.)
2. (Done: `collectable` with seven unit tests, including the review's
   interleaving for condition 2, scripted. The simulation models snapshot
   contents and snapshots right after a restore, and owners collect by the
   rule with deletes that land late: over 200 seeds, about 4,500
   acknowledged writes, all restored, and 5,800 objects collected.
   Deleting the segment at `P` is caught at seed 28. Dropping the
   generation guard is not reached by the simulation within 5,000 seeds,
   even with periodic snapshots made to fail and owners pausing after a
   restore: it needs two held uploads and a snapshot right after a
   restore in one narrow window. The unit test scripts it instead,
   restore failing with `Missing` without the guard.) Planned as:
   `collectable(listing, h, S)` in `wasmkeep-cluster` with unit tests (the
   kept set, earlier generations at any txid, a late segment of an earlier
   generation after collection, the guards, a delete's reset). The
   replication simulation (`sim/tests/madsim_replication.rs`) gains owners
   that collect by the rule through a delete client whose requests can
   land late. Before that, the simulation must model what it currently
   does not: snapshot contents (each snapshot records the positions it
   holds, so an acknowledged write below a snapshot counts as restored
   when it is in the plan's snapshot), and a final check that re-plans
   with the snapshots instead of walking every chain back to the empty
   database; and snapshots taken right after a restore, at the restored
   head, as the server does once 64 segments were restored. Unsafe
   variants some seed must catch: keeping only `S` (not the segment at
   `P`), and collecting without the generation guard. (Collecting after a
   late snapshot is not unsafe, see condition 1, so it is not a variant.)
3. (Done: `Collector` in `replica.rs`, triggered from the periodic
   snapshot and after a delete; `--history-retention-secs`, default 3600;
   `wasmkeep_history_collected_total` and `_collect_failures_total`; three
   cluster tests, plain and over TLS, the retention one failing when the
   window is ignored. The flag is named `--history-retention-secs`, like
   the CLI's other durations. Not covered by a dedicated test: a pass
   stopping when the lease is lost.) Planned as: the server pass:
   `ship_snapshot` reports whether it created the
   snapshot; collection after a qualifying snapshot, the retention flag
   (`--history-retention`, 1 h), a metric for objects collected and passes
   failed, delete snapshots collected without the window, passes queued
   per actor. Cluster tests: the prefix shrinks to `S`, the segment at `P`
   and what follows; a restore on another node after a collection; a
   deleted actor leaves two objects; nothing is collected while the lease
   is lost.
4. Deploy artifacts. (Done: `AppRef::collect_versions` in
   `crates/wasmkeep-server/src/apps/collect.rs`, run by `wasmkeep deploy`
   after it changed `current`, with `--version-retention-secs` (default
   3600). The current version and `KEEP_PREVIOUS` (5) before it are kept;
   only the current version's lineage is a candidate, so a directory a
   concurrent deploy is still writing, or a failed deploy left, is never
   touched, nor the versions a rollback stepped back from (rollback
   re-points `current` at an existing version): those leak, a few small
   objects each. Manifest first, candidates oldest first (a pass cut short
   strands nothing reachable), `current` re-read before each version, the
   window counted from when a version stopped being current (its
   successor's manifest), only plain ids followed, and an unreadable
   manifest never deleted (from the review of step 4). Unit tests and a
   cluster test with a node following the app.)
5. `benches/restore.rs` rerun at 10,000 writes after collection; the benchmarks
   page updated. (Done: `restore/collected/N` and `list/collected/N`,
   recorded in `bench/results/2026-11-05-4c8b21a.csv`: at 10,000 writes a
   restore takes 1.23 ms after collection against 6.13 ms before, and the
   listing 1.7 µs against 1.72 ms; the collected prefix holds at most 65
   objects. [Benchmarks](../benchmarks.md#restore-against-history-length).)

## Decisions on the open questions (2026-11-05)

1. **Retention window: 1 hour**, configurable (`--history-retention-secs`,
   0 allowed), except after a delete (see "Deleted actors").
2. **Deploy artifacts: in this ADR** (step 4 of the plan).
3. **Idle actors: acceptable for now.** Collection follows writes, so an
   actor that stops writing keeps its last interval of history (fewer than
   64 segments) plus whatever was younger than the window at its last
   pass, until it writes again.

## What can still go wrong

- An actor whose periodic snapshots keep failing is never collected: its
  prefix grows as today (the failure is logged; the history-length benchmark
  measured that case without snapshots).
- Collection deletes history: a bug in the rule loses data. Hence a pure
  rule shared with the simulation, the retention window, and a metric.
- Clock skew beyond the bound breaks the acknowledgement rule, and with it
  this one, exactly as it breaks leases (ADR 0007).
- Not this ADR's to fix, but met on the way: because the planner prefers
  segments, a prefix whose only object of the highest generation is a
  snapshot can have its head displaced by a late segment of a lower
  generation. Collection avoids it by keeping the segment at `P`. An
  actor's first snapshot (uploaded when the bucket had nothing for it) has
  no segment under it, but the only earlier uploads that can still land
  there are other first snapshots (an owner uploads segments only after
  its first snapshot was acknowledged), and a snapshot of a higher
  generation outranks them.
- Restore lists the prefix once (ADR 0007; twice before 2026-11-07);
  collection bounds the size of that listing, not its existence.
