# 0033. Data access, export and online backup

- Status: partially implemented (2026-11-15): actor export and read-only
  actor SQL and history listing are built. Online backup create/list/verify
  have an initial implementation; the [backup guide](../deploy/backup-and-restore.md)
  records its cache and concurrency limits. Step 8 (`backup prune`) is implemented
  with the retention and coordination refinement below. Whole-app
  backup restore (plan step 6) is implemented; the guide records its recovery
  journal, `restore` heartbeat capability, partial-undo force warning and
  phase-specific abort/undo recovery. Single-actor
  live restore (plan step 7) has an initial implementation: staged requests,
  owner-fenced replacement, replay markers, alarm ordering and batched flush
  barriers. See the [operator guide](../actor-data-access.md#restore-one-actor-while-its-cluster-runs). The [first-slice guide](../actor-data-access.md) records its
  positional app syntax, limits and deferred options. Revised after an
  adversarial review (a destination keyed by content, since generations restart after an
  app is recreated; prune excluded from a running backup; restore
  requests in the intents' fallback listing and ended as deletes are;
  resets labelled in the database instead of guessed from headers;
  restores refused once superseded by a later delete; the alarm index
  rebuilt from the committed alarm; a restore of a whole app fenced
  from node starts)
- Amended by [ADR 0034](0034-batched-plain-writes.md) (proposed 2026-11-15; if accepted): in a batched class a position is a segment of several events, stamped with its capture time; `--at` and `--txid` choose between segments.
- Amended by [ADR 0030](0030-files-and-static-assets.md) (2026-11-17, proposed; with its slices B5 and B6): a backup copies an app's files as listed and the assets of the versions it keeps, and `backup restore` brings them back; `actor restore` rewinds the database only and its replacement keeps the actor's current `blob_incarnation`. See that record's section 5.
- Date: 2026-11-14
- Milestone: [Now](../../ROADMAP.md#now), `0.5.0`: actor export,
  read-only SQL, verified online backup and single-actor point-in-time
  restore, in that order. Whole-app point-in-time restore is
  [later, on demand](../../ROADMAP.md#later-on-demand).
- Follows: [ADR 0005](0005-replication-format-ltx-v3.md) (segments are
  LTX files), [ADR 0007](0007-replication-protocol.md) (the restore
  plan), [ADR 0012](0012-bucket-garbage-collection.md) (what collection
  deletes, and when), [ADR 0020](0020-one-lease-per-node.md) (holds,
  generations, the first generation of an actor),
  [ADR 0022](0022-app-owned-state-and-the-one-time-migration.md) (an
  app is two prefixes) and [ADR 0028](0028-actors-that-end.md) (resets,
  erasing, operator requests).
- Amends, if accepted: [ADR 0022](0022-app-owned-state-and-the-one-time-migration.md)
  (a family under an app's root, `restores/`),
  [ADR 0028](0028-actors-that-end.md) (a third kind of operator request
  beside moves and deletes; one more host meta key carried over by every
  delete), [ADR 0009](0009-alarms-in-a-cluster.md) (an index entry
  written by a job and by a restore command) and the
  [backup guide](../deploy/backup-and-restore.md) (the rule that a copy
  is restorable only with the cluster stopped). ADR 0012's rule is not
  changed: nothing here holds or slows collection.

## Context

A team evaluating WasmKeep asks two things: can I see and query my data,
and can I back it up and restore it while it runs? Today the answers are
no and no:

- [Back up and restore](../deploy/backup-and-restore.md) says a bucket
  copy is restorable only with the cluster stopped; with the cluster
  running it is "not established", and the reasons are in the code: a
  collection pass can delete a snapshot between a copy's listing and its
  GET, ownership records and the alarm index are copied at other moments
  than the history, and nothing checks the result.
- No command opens one actor's database from outside. The only way to
  look at an actor's rows is code in the actor.
- No actor can be restored alone to an earlier point, and an app cannot
  be restored while the cluster's other apps run.

What the code gives us to build on:

- **An actor's history is a tree of immutable objects** (ADR 0007):
  `R actors/<class>/<name>/wal/<txid>-<generation>-<parent>.ltx`, one
  per committed transaction, and `snapshot/<txid>-<generation>.ltx`, the
  whole database at one position, every 64 segments (`R` is the app's
  root, `state/<tenant>/<app>/`, ADR 0022). Every key is created once,
  conditionally, and never rewritten while it exists. A key does **not**
  name one content for good: `app delete` removes the app's ownership
  records and lease markers with its history (ADR 0028, section 5), so
  after `deploy --recreate` generations start again at 1
  (`seed_generation`) and the same key can come back with other bytes.
- **The restore plan is a pure function of key names**
  (`wasmkeep_cluster::replication::plan_restore`): the head is the
  newest segment of the highest generation, and the plan walks back by
  parent links to the first snapshot on the way. Replay is in
  `wasmkeep-storage` (`wal::Restorer`), which checks each segment's
  pre-apply checksum against the running database checksum, and each
  LTX file's own CRC-64 trailer. Neither imports the cluster crate, the
  server, or anything that needs a hold. A replay from the bucket needs
  bucket read access and nothing else.
- **Collection** (ADR 0012, `Collector::collect` in
  `crates/wasmkeep-server/src/replica.rs`) deletes only after the
  owner's own created and acknowledged periodic snapshot `S` at
  `P = (h, t)`, and then only objects below `t` or of a generation below
  `h`, never `S` or the segment at `P`, and only those whose listed
  `last_modified` (the bucket's clock) is older than
  `--history-retention-secs` (an hour by default; 0 is allowed) on the
  collecting node's clock. After a delete it erases without the window.
  After collection a prefix at rest holds at most about 65 objects; a
  busy one also holds everything younger than the window.
- **Each segment carries its commit time.** The LTX header's
  `timestamp_ms` is the node's wall clock when the segment was encoded
  (`crate::alarm::now_ms_i64()`, `replica.rs`), right after the commit
  and before the upload. It travels with the object through any copy.
  The listing's `last_modified` is when the upload completed, on the
  bucket's clock. A copy resets it, and it is late for an upload that
  landed late.
- **Operator requests** (ADR 0028, section 3): the command line holds
  bucket credentials only, so it writes a request object. The actor's
  owner, or any serving node if nobody holds it, runs a job in the
  actor's mailbox without guest code, and writes an outcome beside the
  request. Only a holder writes history, and `ship_reset` already
  replaces a whole database with one segment on the current head.
- **An actor's first generation** comes from its lease marker
  (`R leases/<class>/<name>`, `wasmkeep-lease 2`) and from the highest
  generation its history keys carry, whichever is higher
  (`mark_old_lease`, called by `seed_generation`, ADR 0020, section 6).
  A prefix with history but no ownership record is safe to start on:
  without a marker the seed comes from the keys. A marker matters only to
  lift the seed above generations the keys no longer show.
- **The alarm index is derivable.** Each database records the exact
  entry it is indexed under (`alarm_indexed` in `_wasmkeep_meta`,
  ADR 0009), and its alarm in `_wasmkeep_alarm`.
- **Requests at rest cost money** ([benchmarks](../benchmarks.md#bucket-requests-at-rest),
  measured on 0.4.1, before ADR 0014's amendment of 2026-11-14 cut the
  intents' listings; not measured since): on R2 and S3 a LIST is billed
  like a PUT, and listings page by 1,000 keys. Whatever a tool lists per actor, per run, multiplies by the
  number of actors.

**One premise of this work needs correcting.** It is tempting to say
that history always ends in the current owner's generation, so that the
newest segment is settled. It is not so. An owner uploads in its own
generation only when it writes (ADR 0009's `owner_fence` and ADR 0028's
fence are one-off uploads for two specific cases). A holder that only
reads leaves no trace. Until a later holder uploads, the newest segment
may be an upload that landed after its writer's hold ended. That upload
may become the history as a late commit, or lose to an even later
upload of a generation in between (ADR 0012, condition 2, has the
interleaving). Only a holder's own acknowledged upload settles what came
before it, and a reader without a hold cannot make one. Everything
below that reads the bucket without a hold says what it promises in
those terms.

## Decision

### 1. Reading an actor without its owner

Every tool in this record reads an actor the way a node restores it, but
without a hold: one listing of the actor's prefix, `plan_restore`, then
the snapshot and the segments of the plan, replayed with `wal::Restorer`
into a file of the tool's own. It writes nothing to the bucket, sends
nothing to a node, and does not wake the actor. The owner, if any, sees
nothing but a few more GETs on its prefix.

**What a replay of the head promises.** The database at the plan's head
`H`, read from a listing that started at time `L`:

- holds every write acknowledged before `L`. ADR 0007's argument
  applies as it stands: an acknowledged upload completed while its hold
  was valid, so it is in any listing made after; and the newest segment
  of the highest generation is on a chain that contains every
  acknowledged write of every earlier generation;
- is the database exactly after one committed transaction, never part of
  one: a segment is a whole commit;
- may also hold writes that were never acknowledged (uploads in flight,
  or landed after their hold ended). Such a tip is usually a late commit
  that later history builds on. In the narrow interleaving of ADR 0012,
  condition 2, it is overtaken instead, and no later restore holds it.
  The tool cannot tell these apart, and says so in its output when the
  head's generation is not the ownership record's (one GET; a hint, not
  a proof).

**Safety by verification, liveness by the window.** A plan is good only
if it is complete: every object it names exists, the LTX checksums
match, and each segment's pre-apply checksum is the running checksum.
The tool trusts nothing else, in particular no timing. On a missing
object (404), a missing link (`PlanError::Missing`), or a listing that
went stale part-way, it lists the actor's prefix again, plans again, and
keeps what it already read only if the new plan names it. After 8
attempts it gives up on that actor with the reason. A checksum mismatch
is not retried: it is reported as corruption.

Why the retries end. Write `W` for the retention window and `ε` for the
skew bound between the nodes' clocks and the bucket's.

- An object of the plan made from listing `L1` can only be deleted by a
  pass triggered by a snapshot `S*` that is not on that plan's chain at
  or above its starting snapshot. If `S*` had been on it, the plan would
  have started from `S*` or a newer snapshot, and nothing at or above it
  is collectable. So `S*` was created after `L1`. A late delete of a
  pass from an earlier holder is covered by the same argument, since
  ADR 0012 proves those delete nothing on the current chain above `S*`.
- The next listing `L2`, after the miss, sees `S*` (acknowledged before
  its pass deleted anything), so its plan starts at `S*` or newer. The
  objects of that plan were created after `S*`: the owner uploads its
  segments in order, and a snapshot before the next segment. So they
  were all created after `L1`.
- A pass deletes an object only once its `last_modified` is older than
  `W` on the pass's clock. With each attempt taking at most `d`, the
  second attempt's reads end before `L1 + 2d`. If `2d + ε < W`, no
  object of the second plan can be collected before they end.
- An erase after a delete has no window, but its plan (the reset
  segment and the snapshot at it) is never collected except by the
  next delete. So each delete that happens while an actor is being read
  costs at most one more attempt.

`d` counts from the read of the listing the plan came from to the last
read of the plan, so a tool must not let plans wait in a queue: it plans
an actor only when it can start reading it (section 4 applies
back-pressure to its listing). The reads themselves are bounded as a
restore's are (ADR 0007, step 7: 30 s, plus a second per started
megabyte and a second per eight objects), which is seconds for a small
actor and about 17 minutes for a database of 1 GiB. A plan can hold more
than 65 objects when periodic snapshots keep failing (ADR 0012, "What
can still go wrong"), and the same deadline grows with it. With `W = 0`, a busy actor can keep a reader
retrying until the 8 attempts are spent. The tool then fails that actor
and names the cause ("collected faster than read: raise
`--history-retention-secs`"). It never returns a wrong database. The
docs ask for `W` above twice the read deadline of the largest actor,
plus the skew bound, on a cluster that is backed up or exported: 35
minutes for actors at the 1 GiB cap, which the default of an hour
covers. (`last_modified` is taken here as the time the upload
completed. If a provider reports when a multipart upload started, the
bound shifts by at most one upload's time. This changes liveness only,
not safety.)

**Which past positions remain.** A position `X` on the current chain
can be replayed if the listing holds a snapshot at or below `X` on the
chain and every segment from it to `X`. After collection that is:

- from the newest snapshot that collection kept (`S`, at `P`) to the
  head, always;
- further back, as far as snapshots and segments are still younger than
  the window: about `W` of history, less up to one snapshot interval,
  because a position whose base snapshot is older than `W` has lost it;
- more for an actor that stopped writing: collection follows writes
  (ADR 0012, open question 3), so an idle actor keeps its last
  interval and what was younger than the window at its last pass;
- nothing before the newest delete in the chain: a delete erases at
  once (ADR 0012, "Deleted actors"). A restore of section 6 keeps the
  window, and with `W = 0` keeps nothing before it either.

So the live bucket offers point-in-time reads over roughly the last
`--history-retention-secs`. A longer window keeps more, at the cost of
storage and of every restore's listing (one page per 1,000 objects,
about write rate times `W` objects for a busy actor). A backup with
`--history all` (section 4) keeps every position since its first run.

**What "at a time" means.** `--at T` names the newest position on the
current chain such that its segment, and every segment before it on
the chain, carries an LTX `timestamp_ms` at or before `T`. That is commit
time on the writing node's clock, which survives a copy and is not
delayed by a late landing. Along one chain the stamps rise except for
skew between successive owners (up to `ε`), hence "and every segment
before it". A snapshot's own stamp is when it was taken, not when its
position was committed, so a snapshot at `P` is dated by the segment at
`P`, which collection keeps beside it. A relative time (`--at -15m`) is
counted on the operator's clock, which the nodes' skew bound does not
cover; the output prints the resolved position and its stamp. Positions on abandoned branches are never chosen: `--at`
and `--txid` both walk the current chain only. To find the position the
tool narrows with the listing's `last_modified` (live bucket) or the
run manifest's recorded stamps (backup), then reads the headers of the
segments it replays anyway. A `T` before the oldest position that can
be replayed is an error that prints the range.

**Deletes and replacements are labelled, not guessed.** A reset
(`ship_reset`, ADR 0028) and the replacement of section 6 are both one
segment holding a whole database, but neither the key nor the LTX
header tells them from an ordinary commit that rewrote every page (a
small database), and a database past 1 GiB never holds every page
anyway (the lock page is left out). So every delete routine and every
restore writes a host meta key in the database it uploads, `replaced =
delete <txid> <time> <generation>` or `replaced = restore <txid> <T> <id>`, naming
the txid of the segment that carries it (the holder knows it: the head's
txid plus one). The row stays in the database until the next delete or
restore replaces it, so one read of the replayed database names the
newest replacement at or below that position. A database without the key (a delete by a node from
before this record) is labelled only when it is empty of app tables,
and the output says "probably deleted". Labels need a replay, so `actor
history` shows them only with `--deep`. A replay of a deleted actor's
head gives the empty database (with its host meta), and says so. A
position before the newest delete is not replayable (its objects are
erased), and the error says so: "erased by a delete at txid r; restore
it from a backup".

### 2. `wasmkeep actor history` and `wasmkeep actor export`

```bash
wasmkeep actor history --app acme/shop cart/u-1842 --bucket s3://b/p
wasmkeep actor export  --app acme/shop cart/u-1842 --bucket s3://b/p \
    [--at 2026-11-14T09:30:00Z | --at -15m | --txid 4123] -o cart.db
```

The actor's address and `--app` are written as for `actor delete` and
`actor move` (ADR 0028, ADR 0017). `--bucket` may name a backup
destination of section 4, with `--run ID` (default: its latest run), in
which case the run's manifest replaces the listing: what the run copied
for that actor is exactly what is read.

**`actor history`** lists the prefix once and prints, without reading
an object: the head (generation, txid, landed at), the oldest position
that can be replayed, the snapshots on the chain, the objects and bytes
under the prefix, and how many objects are off the chain (late uploads,
not yet collected). `--deep` replays the head and adds the newest delete
or restore and the commit times from the LTX headers. `--json` for
scripts. It is what an operator runs before `--at` or `actor
restore`.

**`actor export`** writes a plain SQLite file:

1. Plan as in section 1 (head, `--txid`, or `--at`), with its retries.
2. Replay into `<out>.partial` beside the output (never into the output
   itself), bounded by `--max-bytes` (default: the server's
   `--max-db-bytes` default, twice, as a restore bounds it).
3. **Verify**, all of which must pass or the partial file is removed and
   the command exits non-zero:
   - every object's LTX trailer checksum, and each segment's pre-apply
     checksum against the running database checksum (as a restore does);
   - the final database checksum equals the post-apply checksum of the
     last object applied;
   - `PRAGMA integrity_check` returns `ok`. `PRAGMA foreign_key_check`
     is run and its count reported, not failed: foreign keys are the
     app's business.
4. **Make it plain.** The replayed file is in WAL mode (header bytes 18
   and 19). The tool switches it to a rollback journal (`journal_mode =
   DELETE`), so it opens in any SQLite tool with no `-wal` file beside
   it, then fsyncs it and renames it to the output, refusing to replace
   an existing file without `--force`. `--raw` keeps the bytes exactly as
   replayed, which `--expect-checksum` can then compare.
5. Print what it wrote: the position, its commit time, whether it is the
   head and whether its generation is the ownership record's (above),
   objects read, size, tables, the checks.

**Host tables.** The file holds `_wasmkeep_meta` and `_wasmkeep_alarm`
as the actor's database does; guests never see them (the reserved
prefix, `wasmkeep-storage`), and the docs say they are internal and may
change between releases. `--app-tables-only` writes a second copy
without them, after the checks, with `VACUUM INTO`, so the verified
bytes are never edited in place.

**Who may run it.** Read access to `R actors/` of that app is enough, so
an operator can hand out a credential scoped to one app's state, read
only. The file holds the actor's data in the clear: the docs say to
protect it like the bucket.

### 3. `wasmkeep actor sql`

```bash
wasmkeep actor sql --app acme/shop cart/u-1842 --bucket s3://b/p \
    [--at T | --txid N] [--format table|csv|tsv|json|jsonl] \
    [--cache DIR] [--host-tables] [--max-rows N] \
    "SELECT sku, qty FROM items ORDER BY sku"
```

Read-only SQL on one replayed snapshot, run on the operator's machine:

- **The database** is the export of section 2 at the position asked,
  into a temporary file removed on exit, or into `--cache DIR`, which
  keeps one file per actor, with the raw bytes as replayed (WAL mode, so
  that later segments' pre-apply checksums still follow it) and a
  sidecar naming its position and checksum (as a node's `.replica`
  sidecar does). It is opened with `immutable=1`, which needs no `-shm`
  file and no write access. With a cache, a second
  query lists the prefix and replays only the segments since the cached
  position when they follow it on the chain; otherwise it builds the
  file again.
- **No guest code runs**, no component is loaded, no host function
  exists: this is the SQLite library `wasmkeep-storage` bundles, the
  same version as the nodes, opened `SQLITE_OPEN_READONLY` with
  `PRAGMA query_only = 1`, extension loading off, and an authorizer that
  refuses anything but reads, `ATTACH` and `PRAGMA`s that write
  included.
- **One statement**, which SQLite must report read-only
  (`sqlite3_stmt_readonly`); trailing statements are refused, not
  ignored. `_wasmkeep_*` tables are hidden as from a guest unless
  `--host-tables` is given.
- **Output formats**:
  - `table` (the default on a terminal): aligned columns, with cells
    over 80 characters cut unless `--wide`;
  - `csv` (RFC 4180, a header row; the default when not on a terminal);
  - `tsv`, with tabs and newlines in values escaped as `\t` and `\n`;
  - `json`, one array of objects keyed by column name (a repeated name
    gets `_2`, `_3`);
  - `jsonl`, one object per line.

  Types: `INTEGER` is a number (one beyond 2^53 is a string in JSON,
  since most readers lose its digits), `REAL` a number (infinities as
  `null`), `TEXT` a string, `BLOB` base64 in JSON and `X'..'` hex in
  the others, `NULL` as `null` or an empty field.
- `--max-rows` (default 10,000 on a terminal, unlimited otherwise) stops
  and says so. The exit code is non-zero on any SQL error, with SQLite's
  message.

Queries across actors are the change feed's (section 7), not this
command's. A loop over `actor sql` is the stopgap, at the cost of
section 8.

### 4. Online backup: `wasmkeep backup`

```bash
wasmkeep backup create --bucket s3://acme-wasmkeep/prod \
    --to s3://acme-wasmkeep-backup/prod [--app acme/shop | --all] \
    [--history head|all] [--cache DIR]
wasmkeep backup verify --from s3://acme-wasmkeep-backup/prod [--run ID] [--sample N]
wasmkeep backup list   --from s3://acme-wasmkeep-backup/prod
wasmkeep backup prune  --from s3://acme-wasmkeep-backup/prod --keep-runs N
```

A backup copies one app's state (or every app's, `--all`) while the
cluster runs, so that **every actor in the copy is consistent on its
own**: a state its own history passed through, verified by replay,
holding every write acknowledged before that actor was listed. **No
consistency across actors is promised.** Each actor is copied at its
own moment within the run, so two actors that changed together (a
transfer between two accounts, a workflow and the actor it calls) may
come back one before and one after. That is the actor model's rule
already: no transaction spans two actors. An app that needs a
cross-actor invariant back after a restore keeps it in one actor, or
repairs it from its own records (ADR 0015 and 0019 patterns are
idempotent by design).

**The source is only read.** `backup create` needs LIST and GET on the
source and nothing else, never a node, never a write: a credential
that can read but not write or delete the bucket is enough. The
destination needs PUT: objects are created with `If-None-Match: *`,
and only `latest`, `runs/<id>/verified` and the conditional publication lock
are replaced. Only `prune`
needs DELETE there, and is meant to run with another credential. The
destination's credentials come from `--to-env FILE` (the variables
`object_store` reads, in an env file), so that the nodes' credentials
never reach the copy and the copy's never reach the nodes, as the
backup guide already asks.

**The destination's layout**:

| Keys | What |
| --- | --- |
| `data/<sha-256>` | Copies of source objects, keyed by the SHA-256 of their bytes, so a key names one content for good whatever happened at the source (an app recreated, two source prefixes backed up into one destination). Each is created once and never rewritten. |
| `runs/<run id>/started` | Written first, with the source bucket and prefix (no credential) and the app's `recreated` marker as read at the start. |
| `runs/<run id>/actors/<chunk>` | The run's manifest, bounded by serialized bytes below 64 MiB, with ordered fragments for oversized actors: for each object, its source key and its digest. |
| `runs/<run id>/app/<tenant>/<app>` | The deploy version and the secrets version the run copied for that app, with their keys. |
| `runs/<run id>/complete` | Written last: the run's summary and the SHA-256 of every chunk. A run is **restorable** only if this object exists and its chunks match it. |
| `latest` | The id and completion record SHA-256 of the newest complete run (a checked hint; `backup list` lists `runs/`). |
| `prunes/<id>` | Permanent marker written by `prune` before scanning or deleting (below). |
| `prune-done/<id>` | Closes the corresponding marker after pending requests drain. |
| `prune-lock` | Conditional lock shared by pruning and backup publication; retained as an unlocked record after release. |
| `before-restore/<nonce>`, `restoring/<run>` | Permanent undo labels and restore pins containing the protected run ID. |

A run id is the start time (UTC, to the second) and 8 random hex
digits, so two runs never share one. The destination is an archive,
not a bucket to start nodes on: a restore (section 5) copies one run's
objects into a prefix of its own. Nothing ever reads the destination by
planning over a listing of `data/`, which holds objects of many runs.

**What is copied for each actor, and in what order.** From the actor's
plan (section 1), in this order:

1. the plan's snapshot `S`, at position `P`;
2. **the segment at `P`**, if the listing has it, although the plan does
   not apply it. ADR 0012 keeps it for a reason that holds in any copy:
   the planner takes the head from segments first, so a set of objects
   with `S` but without the segment at `P`, beside older segments of the
   same generation, plans to an older head. Every deleted actor is in
   that shape (its reset segment and the snapshot at it), so leaving the
   segment out would bring a deleted actor's data back from any copy
   that also holds its older objects;
3. the plan's segments, ascending;
4. with `--history all`, every other history object of the listing that
   is still there. A GET miss records a gap and may split the recoverable
   ranges; it is never silently treated as continuous history.

With `--history all`, `--require-continuous` defaults to true: a gap in
history observed at listing makes create report the gap and exit nonzero.
An explicit false permits the ranges that actually survived. (Amended in
0.8.1: after a miss, create lists the actor again. A key gone from below
a listed snapshot whose segment is listed too, the shape collection
leaves by `collectable`, and last modified at least the retention window
before (`--history-retention-secs`, default 3600), is recorded as
`collected`, not as a gap: a listing made after that collection would
not have shown it either. A key still listed whose read missed, or a gap
while the listing changed, reads the actor again, within the 8 attempts.
Any other key gone, a delete's erase among them, stays a gap. The tool
does not see the ownership record; see ADR 0042, "As built: the backup
retry and the last simulations".) Verification
replays every position inside each recorded range, including historical
snapshot boundaries and segment chains. Runs spaced closer than retention
alone do not ensure continuity: reads can cross expiry, and deletes erase
without a retention window. No claim covers objects erased before listing.

Each object is read from the source, checked (its size against the
listing, its LTX trailer checksum), applied to the actor's replay, and
written to the destination under its digest with `If-None-Match: *`.
A conflict there means those very bytes are already there, since the
key is their digest. The manifest maps the source key to the digest, and
a restore writes each object back under its source key. The tool always streams
objects through itself rather than asking the provider to copy them on
the server side: it needs their bytes for the replay, and source and
destination are meant to be in different accounts.

The actor is done when its replay passed every check of section 2. The
run then records, in the manifest: the actor's address, the plan
(snapshot id, segment at `P`, segment ids), each object's size and LTX
stamp, the head and its checksum, and the alarm the replayed database
holds with the entry its `alarm_indexed` names (section 5 rebuilds the
index from it). A miss or a missing link starts the actor again per
section 1.

**One listing for the app, not one per actor.** The run lists
`R actors/` recursively, by pages of 1,000 keys. Keys of one actor are
contiguous (its `snapshot/` keys, then its `wal/` keys), and each actor
is planned when its last key has been listed and a copy slot is free,
then copied while the listing goes on (16 actors at a time, the restore
read limits of ADR 0007 per actor). The listing waits for a slot rather
than queueing plans: a plan's time `d` (section 1) counts from its
listing, so a queue would spend the window. Such a listing is not taken at one moment: an
actor whose keys span two pages may be planned from keys read at two
times. That can only make the plan miss objects (a snapshot created
after its page, segments collected before theirs), never contain a
wrong one, since keys are never rewritten. A missing link or a 404 then
sends that actor back for a listing of its own prefix. Correctness rests
on the replay, not on the listing being a snapshot.

**Why collection cannot make a backup wrong**, and when it delays one.
Safety: an actor counts as copied only after the replay of what the run
read passed, and what was written to the destination is those very
objects. No timing assumption enters, so `W = 0`, an erase after a
delete, and a pass whose deletes land late cannot mark a broken actor as
copied; at worst they make the run retry or fail that actor. Liveness:
section 1's bound applies per actor, with `d` the time to copy one
actor, not the whole run. A run of 100,000 actors that takes hours is
fine with the default hour, because one actor's copy is bounded by its
read deadline (seconds for a small one). This is what the backup guide's "two passes within the window"
lacked: the window must cover one actor's copy, not the copy of the
bucket.

**Holding collection with a marker, weighed and not chosen.** A
`R backup-hold` object read by every collection pass would make
`W = 0` safe for liveness too, and would let a run copy old history
without racing collection. Against it:

- every pass reads it (one more GET per 64 writes per actor, or a cached
  value that needs its own staleness rule);
- a run that crashes leaves it behind, and storage grows until it
  expires, so it needs an expiry and a renewal, which is a lease, with
  everything ADR 0020 had to prove about leases;
- nodes of a version without it ignore it, and nothing tells the run;
- the verified retry already gives safety without it, and the default
  window already gives liveness.

It stays an option if `W = 0` clusters ask for online backups.

**The other keys of an app, and their order.** After every actor of the
app is done:

- **Deploys.** Read `apps/<t>/<a>/current`, copy the version it names
  (the manifest and components under `apps/<t>/<a>/<version>/`) and the
  versions its `previous` chain reaches within 5 steps (rollback's
  reach), then read `current` again; if it changed, copy the new one
  too and record the newest. Reading it *after* the history means the
  recorded code is at least as new as the code that wrote any copied
  state, as long as deploys move forward. A rollback during the run is
  recorded as what it is, the current version. Deploy collection
  (ADR 0012, step 4) keeps the current version and the 5 before it, for
  an hour after each stopped being current, so a version read through
  `current` is still there when copied, unless six deploys and an hour
  pass in between, which the second read of `current` then shows.
- **Secrets.** The same pattern for `apps/<t>/<a>/secrets/`: the sealed
  version its own `current` names. The secrets key (`--secrets-key`)
  is not in the bucket and is not copied.
- **A tombstone** (`apps/<t>/<a>/deleted`, ADR 0028) found at the start
  or the end, or an `apps/<t>/<a>/recreated` marker that differs from
  the one `started` recorded (a delete and a `--recreate` within the
  run): the app's run fails, since its actors may have been copied from
  two incarnations of the app.

**Not copied, on purpose**:

| Keys | Why not |
| --- | --- |
| `R owners/` | Lease state of the running cluster. A record names nodes that are not in a restored one, and its generation is recomputed (section 5). |
| `R leases/` | Written anew by a restore, with the right generation (section 5). |
| `nodes/`, `cluster/secret` | The running cluster's leases and the secret its nodes sign calls with. A restored cluster writes its own (the secret is created when missing). |
| `R alarms/` | Copied at another moment than the history, it can miss an entry a copied database names (an alarm lost) or name one it does not. Rebuilt from the databases instead (section 5). |
| `R moves/`, `R deletes/`, `R restores/`, `control/` | Operator intents of that moment. A restore must not drain, move or delete again. |
| `probe/`, `diagnose/`, `cluster/migration` and the migration marks | Scratch and one-time state. |
| `R blob-sweeps/`, `R blob-usage` (ADR 0030, amendment of 2026-11-17) | Sweeps of that moment and a count. A `swept` record written back beside a restored database of that incarnation would remove a live actor's files; the count is taken again. |

Files of [ADR 0030](0030-files-and-static-assets.md) (proposed, not built) live under `R blobs/` and
can be overwritten under conditions. A run copies them after the
history, by listing, with no consistency between a row and the file it
names; `R blob-sweeps/` and `R blob-usage` are in the table above. That
record's section 5 ("Backup and restore of a whole app", amendment of
2026-11-17) decides it.

**Incremental runs.** Every run is a full run in what it promises, and
incremental in what it moves:

- The previous complete run's manifest (or `--cache DIR`, which also
  keeps each actor's replayed database at its last head, raw) says which
  objects the destination already holds. An actor whose head has not
  moved costs no GET and no PUT: its manifest entry is carried over, but
  only if this run's listing shows each of its plan's keys with the
  same size and `ETag` as the run that read it recorded. Anything else
  (a key gone, or back with other bytes after the app was recreated) is
  copied again.
- An actor that moved needs only the objects it does not have. With a
  cache whose head is on the new chain, the new segments are applied to
  the cached database. A new snapshot `S` at `P` is then read and copied,
  and accepted only if its trailer's post-apply checksum equals the
  replay's checksum at `P`; the manifest names it as the plan's start, so
  plans do not grow across runs. Without a cache, or when its head is not
  on the chain, the actor is replayed from its new plan's snapshot.
- Objects are never deleted from the destination by `create`.

**`--history head` (the default) or `all`.** `head` copies what each
actor's head needs: a busy actor costs one snapshot and up to 64
segments per run, however much it wrote since the last one. `all`
copies every history object the source still has, and the manifest of
each run names, for each actor, every object any earlier kept run named
for it as well as its own (from the previous manifest, carried over).
That set is one tree of positions within one incarnation of the app,
since a recreated app fails the run, as above, and it includes the
segment at each snapshot's position. So planning a past position over it
is planning over a superset of a listing the source once had, which is
what section 1 reads. A restore writes back only the objects the plan
for its chosen position needs. If the time between
two runs' listings of an actor stays under `W - ε`, every object the
actor ever had is in the destination (each lives at least `W - ε` in
the source), and the backup offers every position since its first run
to `actor export --at` and `actor restore --at`. History before a delete
is kept only as far as the last run before it, since the erase does not
wait. `all` costs a GET and a PUT per write (section 8).

**Marking a run restorable.** The run writes its manifest chunks as
actors complete, then the deploy and secrets records, then `complete`
(`If-None-Match: *`) listing the counts, the actors that failed with
why, and every chunk's digest, then `latest`. A run with failed actors
still completes, and says it is partial: `backup list` shows
`complete (3 actors failed)`, `restore` refuses it unless
`--allow-partial`, and the exit code is non-zero. A run that stops
before `complete` is not restorable. What it copied still saves a later
run the work.

**`backup verify`** reads a run from the destination alone. For each
actor (or a random `--sample N`), it reads the manifest's objects and
replays them with every check of section 2. It also checks that the
deploy version, the secrets version and every chunk are there and match
the digests. It writes `runs/<id>/verified` with the date and what was
checked. `create` has already replayed what it read from the source;
`verify` proves what the destination holds, which is what a restore
reads.

**`backup prune --keep-runs N`** deletes runs beyond the newest `N`
complete ones (ordered by run ID), except `latest`, complete runs started within
24 hours, and permanent undo/restore pins. `N = 0` still retains these exceptions.
`before-restore/<nonce>` labels and archive `restoring/<run>` pins contain the run
ID and never expire automatically. Whole-app and backup-backed actor restore
publish a destination pin before reading a run or changing a target; undo backup
pins its result before publication. Finish older CLI restores before enabling
prune: their live-bucket fences cannot be discovered from the destination alone.

Prune validates every kept manifest and computes their union of content hashes
across actors and apps before deleting anything. It deletes `complete` and
`verified` first, then the other run metadata, with `started` last. It collects
only unreferenced `data/` objects older than 24 hours. It refuses while a run has
`started` without `complete` and is at most 24 hours old; older incomplete runs
are abandoned. Create refuses publication after 24 hours. Ages use the bucket's
modification time against the CLI clock, which must be synchronized.

**Coordination refinement (step 8).** Prune writes a permanent `prunes/<nonce>`
marker before scanning. `prune-done/<nonce>` records its completion after pending
requests drain, even when the pass refused. Create checks for active markers
before starting and compares the permanent marker set before publishing. Any
intervening prune refuses completion rather than attempting the originally
proposed HEAD-and-recopy recovery. Keeping completed markers closes the race
where an entire prune fits between a create's two listings. A conditional
`prune-lock` serializes prune deletion and create's `complete`/`root`/`latest`
publication. No new dependency is required.

Retries recompute reachability; no stale deletion plan is replayed. A killed
process leaves the lock held: stop it, wait 40 seconds for pending requests,
clear only `prune-lock`, and retry. This follows whole-app restore's recovery
convention and fails closed rather than expiring a live lock. Ordinary returned
errors attempt to unlock. Markers and pins must not be removed as routine cleanup.
The [operator guide](../deploy/backup-and-restore.md#prune-an-online-archive)
records these recovery and upgrade requirements.

Pruning is how a delete reaches the backups: deleted actors' data remains in
kept runs. `delete-self` and idle expiry erase the live bucket, not the backups.

**What this changes in the rule of the backup guide.** "Restorable only
with the cluster stopped" becomes:

- **A backup** made by `backup create` is restorable with the cluster
  running, per actor, as above, once `backup verify` passed (and a
  restore drill, below, is still the proof of the procedure).
- **A copy made by an S3 tool** stays what the guide says: restorable
  with the cluster stopped, "not established" with it running.
- **Restoring a whole app** (section 5) needs that app's nodes stopped,
  and only those: the cluster's other apps keep running.
- **Restoring one actor** (section 6) needs nothing stopped.

### 5. Restoring a whole app from a backup

```bash
wasmkeep backup restore --from s3://acme-wasmkeep-backup/prod [--run ID] \
    --app acme/shop --bucket s3://acme-wasmkeep/prod [--in-place] [--deploy backup|keep]
```

**Into an empty app root** (the default): `state/<t>/<a>/` under the
target prefix must hold nothing, no live node may serve the app there
(the command reads `nodes/` as `app delete` does), and the app must
have no tombstone. Then, in this order:

0. **Fence node starts first.** Write `apps/<t>/<a>/restoring`, holding
   the run id, and `apps/<t>/<a>/restored` with a body no writer used
   before. A node refuses to start serving the app while `restoring`
   exists, as it refuses a tombstone, and stops if it sees it at its
   next deploy check. A node whose own copy of `restored` differs
   removes its local files of the app (`<data_dir>/state/<t>/<a>/`)
   before it serves, as it does for `recreated` after `app delete
   --recreate` (ADR 0028, section 5). Without this, an actor created
   after the backup and absent from it would come back from a node's
   disk as its first snapshot, as the backup guide warns today. A node
   from before this record ignores both markers, so the command refuses
   while `nodes/` shows any node of the app, live or not, of an earlier
   build without `restore` on its heartbeat capability line, unless
   `--force`. The heartbeat version line remains unchanged.
1. **History.** For each actor of the run, copy the manifest's objects
   from `data/<digest>` to its source key, `If-None-Match: *`: the
   snapshot, the segment at `P`, the segments (for `--history all`, the
   plan of the position chosen with `--at`, and the segment at its
   snapshot). Every actor's prefix then plans to the
   head the manifest recorded, and to nothing else.
2. **Generations.** Write each actor's lease marker,
   `R leases/<class>/<name>`, `wasmkeep-lease 2`, with the highest
   generation its copied keys carry. With no ownership record, the
   first node to acquire the actor seeds from the marker and the keys,
   whichever is higher, and acquires at that + 1 (`mark_old_lease`): no
   generation already in the history is ever used again.
3. **Alarms**, from the committed alarm, not only from the entry the
   database names. For each actor whose database holds an alarm at `a`:
   create the entry its `alarm_indexed` names, exactly that key (a
   tagged key with its generation and nonce, or a legacy key as a
   legacy key); and if that entry's time is not `a`, or there is none,
   also create a fresh tagged entry at `a`, generation 0, new nonce.
   ADR 0009 documents states whose named entry is at another time than
   the committed alarm (a new entry written but not yet named when the
   event was replicated): rebuilding the named entry alone would leave
   that alarm unlisted, never fired until something woke the actor.
   Writing an entry the history once named, a key an earlier request may
   have deleted, would break R1 in a live root. Here it is safe: the root
   is empty or cleared, and no writer of that root is live (step 0, and
   `LATE_WRITES` in place). Both entries' generations are at or below the
   marker's seed, so any holder to come may retire them (R3). An entry
   whose alarm has moved since is delivered to an actor without that
   alarm, which runs nothing (ADR 0028's `er04`).
4. **Deploy and secrets**, with `--deploy backup` (the default here):
   the run's version directories, then `apps/<t>/<a>/current`; the
   sealed secrets version, then its `current`. A sealed secret opens
   only under the prefix, tenant and app it was sealed for (ADR 0016):
   under another prefix the command says to run `wasmkeep secrets
   reset`.
5. **Open the app.** List and plan every restored actor, replay its head,
   and compare its position and database checksum with the manifest. Refuse
   unexpected actors or heads. Delete `apps/<t>/<a>/restoring`, last. A
   restore cut short leaves it, so no node serves a half-restored app;
   running the command again resumes (every write is a create, and an
   existing object with the same bytes is not an error).
6. Print the command that starts the nodes, and what the restored app is
   (below).

**In place** (`--in-place`): the app's state is replaced under the same
prefix, while the cluster's other apps run.

1. The app's nodes must be stopped: no node lease of the app `live`
   (`cluster fence`, or `--force` after it; the command fences an expired
   `live` lease itself). Since 0.7.0 it then waits until `LATE_WRITES`
   (40 s), plus a second for whole-second stamps, after the bucket stored
   the newest write of the app's node lease objects (a renewal, release or
   fence), measured on the bucket's `Last-Modified` clock, not a node's or
   the operator's. It lists them again after the wait and waits again if one
   changed, refusing after three rounds. With no node lease of the app under
   the prefix, it does not wait at all. When the listing proves nothing (an
   unreadable heartbeat, one naming no app, a node without a node lease or
   the `restore` capability, or `--force`), it also waits that long after
   the fence was stored. An upload held by a dead node is assumed to land
   within that window or never; `docs/deploy/backup-and-restore.md` says
   where the bucket client does not enforce it.
2. **The state being replaced is backed up first**, by an ordinary
   `backup create` run into the same destination, marked
   `before-restore`. The command goes on only if that run completed with
   no failed actor (`--force` accepts a partial one). A wrong restore
   can then be undone by restoring that run.
3. The command reads, for **every actor seen under the old root** (its
   history, its ownership record or its marker), whether the run holds it
   or not, the generation of its ownership record, its marker's seed,
   and the highest generation its history keys carry.
4. It deletes every key under `state/<t>/<a>/` (as `app delete`,
   step 2, without the tombstone), and lists until a listing is empty.
5. It runs the steps above, except that markers are written for every
   actor of step 3 too, not only the run's, each carrying the highest of
   the run's keys, the old record, the old marker and the old keys,
   plus one. An actor absent from the run then starts empty, at a
   generation above everything its old history used. No generation the prefix ever used comes back: the local files'
   sidecars, alarm entries in older backups and the logs all keep
   meaning one hold each. `--deploy keep` (the default in place) leaves
   the app's current deploy and secrets as they are, since the code may
   be newer than the backup on purpose. `--deploy backup` restores the
   run's.

**What a restored app is.**

- **Each actor at its own moment** of the run (section 4), every write
  acknowledged before its listing included, and nothing written after
  it. Writes since are gone, durable or not.
- **No ownership records, free to take**: the first request for an actor
  acquires it at a fresh generation. Idle expiry (ADR 0028, section 4)
  finds candidates by their records, so an actor restored and never
  woken does not expire until it has been woken and has hibernated once.
- **Alarms** due since the backup fire soon after the start (the sweep
  finds their entries), at least once, as the backup guide says today.
  What actors did outside between the backup and the restore (a webhook,
  an email) may happen again.
- **Sockets**: there are none. The app's nodes were stopped (closing
  their sockets with 1001 or 1012), or the prefix is new. Clients
  reconnect when the nodes are up.
- **No intents**: no drain, move, delete or restore request applies
  again.

### 6. Restoring one actor while the cluster runs: `wasmkeep actor restore`

```bash
wasmkeep actor restore --app acme/shop cart/u-1842 --bucket s3://b/p \
    (--txid N | --at T | --from s3://acme-wasmkeep-backup/prod [--run ID] [--at T]) \
    [--if-head GEN:TXID] [--wait SECS | --no-wait]
```

**History is never rewound.** Deleting segments after `N` would delete
acknowledged writes, and a lower head can be displaced by a late upload
of any earlier holder. Txids would also go backward under the sidecars,
alarm tags and collection's rule. Instead a restore is **a new write by
the actor's holder whose content is the old state**: one replacement
segment at the head's txid + 1, under the holder's generation, built
like `ship_reset` but with every page of the restored database instead
of an empty one. It is fenced by what fences every write. Only a
holder writes (ADR 0007, 0020). The job runs in the holder's mailbox,
after the events queued before it. A previous holder lost its hold
before this one acquired, and its late uploads are on no chain this one
builds. A restore is therefore also undoable, for as long as the history before
it is kept: the retention window, like any other history (nothing with
`W = 0`), or for good in a backup taken before. `actor restore --txid
<the head before>` brings it back.

**The command** (bucket credentials only, as `actor delete`):

1. Builds the target database with `actor export`'s replay and checks
   (section 2), from the live bucket at `--txid` or `--at`, or from a
   backup run. The live bucket's range is the one of section 1; older
   points need a backup.
2. Refuses if the app has a tombstone, if the tenant's layout is not
   0.4.0's, or if the actor has no history in the bucket ("no such
   actor": restoring an address the bucket never knew, or knows no
   more, is not decided here).
3. **Stages** the database, the raw replay in WAL mode (never the
   rollback-journal copy `actor export` hands out), as one LTX snapshot
   file at
   `R restores/<class>/<name hex>/<id>.ltx` (multipart for a large one),
   `<id>` being 16 random hex digits, then writes the **request**
   `R restores/<class>/<name hex>/<id>` with
   `wasmkeep-restore 1`, where the state came from (the live bucket and
   position, or the backup run and position; never a credential), the
   staged object's size, page size and post-apply checksum, and
   `--if-head` when given. Under `R restores/<class>/<name hex>/`, a key
   of 16 hex digits is a request, `<id>.ltx` its staged database, and
   `<id>.done`, `.conflict`, `.refused`, `.absent` and `.superseded` its
   outcomes; anything else is ignored. Then it writes `control/changed` (ADR 0014's marker, amendment
   of 2026-11-14), so the app's nodes list `restores/` within a heartbeat
   period. A request whose marker write failed is taken back and the
   command fails.
4. Waits for the outcome, like `actor delete` (default 60 s, at most
   3,000).

**Who acts**: exactly as for an operator delete (ADR 0028, section 3,
"Who acts" and the table "By the actor's state", with `restores/` for
`deletes/`). The owner queues the job; nobody's actor is acquired by
one serving node, conditionally; a moving actor's job runs on the
target; a parked actor with sockets is woken by its parking node (ADR
0024). Nodes list `restores/` with the other intents (ADR 0014,
amendment of 2026-11-14): when `control/changed` changes, at their first
period, and in the 60 s fallback listing, which is what retries a
refused request and expires an old one. One prefix more in that
listing is one LIST per node per 60 s at rest. Requests expire after 50
minutes (`RESTORE_TTL_MS`) by the bucket's clock, and the node that lists an expired one
deletes it with its staged object. A node that ends a request (its
outcome written) deletes the request and its staged object, rewrites
`control/changed`, and keeps the request out of its own copy until it
would expire, exactly as `end_delete_request` does for deletes, so that
other nodes stop acting on their copies within a period.

**Amendment (2026-11-17):** Steps 3–5 validate before disruption and preserve
exact `--if-head` comparison through socket draining and retries.

**The restore job**, in the actor's mailbox, no guest code:

1. Forget any learned owner. `ensure_owner`: acquire and restore, or
   nothing if held. Refused: the request stays for a later pass. If the
   bucket holds nothing for the actor (the restore found an empty head,
   `needs_snapshot`: the app recreated or the actor gone since the
   command checked), the outcome is `<id>.absent` and nothing is
   written: a first upload must be a snapshot, which this job does not
   make.
2. **Replays and supersession.** The host meta `operator_restore` holds
   `<T> <id>` of the last restore request carried out. If it is this
   request, the job **fences** (ADR 0028, section 3, step 3: what a
   restore read is the state only once this hold's upload is in) and
   reports done. If it is a later one, or if the newest delete in the
   database (`replaced = delete <txid> <time> <generation>`, section 1) is later than
   this request's `T`, the job fences and writes `<id>.superseded`:
   a restore asked for before a delete that has since been reported
   never brings the data back, which keeps ADR 0028's promise that a
   "Deleted" is not followed by the data returning. The delete's time
   used for this comparison is the reset object's bucket last-modified
   timestamp, located by its generation and txid. The label's node time is
   diagnostic only. Equal bucket timestamps supersede conservatively. If
   listing fails or the label cannot identify the reset, the job refuses.
   If a successful listing finds no reset, ordinary collection proves it
   predates every live request when history retention is at least
   `RESTORE_TTL_MS + 2 * max_skew_ms + 1000 ms`: restore requests live at most 50
   minutes, and collection removes only objects past retention. The margin
   covers the collector and request reader's clock comparisons plus one
   second for bucket last-modified precision (3 s with the default 1 s
   skew; the retention bound is then 3003 s). Such a collected delete does
   not supersede the request, even though its label survives in the actor's
   next life. With shorter retention, a missing reset refuses with an error
   naming the configured retention and required TTL-plus-margin bound.
   The unchanged default 3600 s retention clears that bound. Delete requests
   keep their independent one-hour lifetime (`DELETE_TTL_MS`). This inference assumes
   ordinary collection under that retention policy, not external object
   deletion or a previously shorter retention. An operator who means to undo
   a delete asks again after it.
3. **Validate before disruption.** Read the request and compare `--if-head`
   with the current head, if supplied. A mismatch ends as `<id>.conflict`
   with the current head. Read the staged object (GET), check its LTX trailer,
   post-apply checksum, size against `--max-db-bytes`, WAL mode and page size
   against the current database. Invalid input ends as `<id>.refused` with
   why. Check the app and ownership fences before closing admission or
   sockets. Preflight refusals preserve sockets and upgrade admission.
4. **Drain only a validated replacement.** The node now answers upgrades
   with 503 and `Retry-After: 1`, including registrations that raced the
   early gate. Sockets already open or closing receive **1012** ("service
   restart"). Their close events run on the old state. Retain the validated
   image and the head checked in step 3 while the job answers `Busy` and
   waits 5 s between passes, up to three waits; sockets still present then
   end as `<id>.refused`. Before this drain begins, normal handshake
   validation applies (an incomplete upgrade can receive 400).
5. **Keep `--if-head` exact.** In every later pass, including the drained
   pass, compare the current head with the head checked in step 3 before
   replacement. Retain that checked head across retryable failures too.
   Any movement, including HTTP writes, alarms, actor calls, another restore,
   ownership recovery or socket close callbacks, ends as `<id>.conflict`
   with the current head and no replacement. Sockets may already have been
   closed. Without `--if-head`, replace whatever is there; history retains
   the intervening writes for its normal retention window.
6. **Build the replacement**, in scratch space beside the database (as
   `delete-self-durable` builds its empty database). The staged database, with its host meta
   set for this actor now, not then:
   - `operator_delete`: the current database's value, carried over, so
     a delete request left behind never deletes the restored actor
     (ADR 0028, section 3, step 5);
   - `operator_restore = <T> <id>`, and `replaced = restore <txid> <T>
     <id>` with the txid this upload will have;
   - the alarm: if the restored database has one at `a`, first **create
     a fresh index entry** for `a` (this hold's generation, a new nonce,
     R1) and set `alarm_indexed` to it; if not, clear `alarm_indexed`.
     The entry comes before the upload that names it, as ADR 0009
     requires. An entry left by a failed upload names no state and is
     cleaned as such;
   - `alarm_failures` cleared: the restored alarm starts a fresh backoff.
7. **Upload** it as one segment on the current head (`ship_reset`'s
   path, which a database of another number of pages replays correctly:
   the header's page count resizes the file), acknowledged only if it
   completed while the hold was valid.
   Not acknowledged: the actor is given up as after a failed
   `exec-durable`, and the request stays. The next attempt restores
   whatever the bucket holds and finds out, by the replay marker,
   whether it happened.
8. **After the acknowledgement**, in this order: write `<id>.done`
   with the new head (the restore is done once the replacement is
   recorded, as a delete is done once its reset is); retire the index
   entry the replaced state named (R2: the durable state no longer names
   it); cancel the node's timer for the replaced alarm and arm it with
   `a` (an alarm already overdue fires soon, and one that fired before
   the restored point fires again: at least once); end the request as
   above; then give the actor up as `delete-self` does when it cannot
   remove its files: the local database is not the replacement, so the
   next event restores from the bucket, which sets the old file aside
   (its sidecar no longer matches). In the background, upload a snapshot at the
   replacement's position (a new kind, `AfterRestore`, collected with the
   window like a periodic one, never erased); its failure only delays
   collection, as a periodic snapshot's does. `on-init` does not run
   again unless the restored state never ran it.

**Every delete carries a non-empty `operator_restore` over**, as it carries
`operator_delete` (`delete-self`, `delete-self-durable`, the operator
delete and idle expiry): a restore request left behind must not bring
back an actor deleted after it was carried out. The CLI and owner require every recorded node of the app, including
offline nodes, to announce `actor-restore` before executing a request.
Older nodes must be upgraded first; an unreadable membership listing refuses
execution. This capability is separate from whole-app `restore` fences.

Carrying this marker does not create an empty row: an absent or empty
`operator_restore` means no completed restore request to recognize, and
both take the same replay path in step 2. Only a non-empty marker is
written into a carried snapshot. A restore itself always writes its new,
non-empty marker. The `replaced` delete label remains independent: it also
supersedes a pending restore that has never run. A failed read of only the
restore marker does not fail a strict delete: the fresh reset position still
provides bucket-clock supersession evidence. Unreadable ordering evidence
makes a subsequent restore refuse; a collected reset uses the retention bound
in step 2, so losing that marker cannot resurrect data from a live old request.

The replacement also records `restore_stale_alarms`, allowing a retry after
an outcome-write failure to retire the old index entry under its fence. Before
another replacement, prior cleanup debt is retired. Build and app-gate failures
remove their fresh entries; an ambiguous upload keeps its entry until the next
owner restores and fences the actual head, then removes entries at the staged
alarm time not named by that head. Failed requests retain a per-request retry
count across heartbeat passes. Backoff starts at the larger of 3 s and the
heartbeat period, doubles to a 60 s cap (or the heartbeat period if larger),
and keeps the request active while reopening socket admission. Finished or
expired requests discard the count; an owner restart starts it again.
Transaction-id exhaustion is checked before allocating scratch state.
An AfterRestore upload failure does not change periodic-snapshot backoff.

**Ordering with other requests.** Deletes and restores of one actor
run in the order their jobs reach the mailbox, and each is checked
against the markers (step 2, and ADR 0028's for deletes). A delete
after a restore deletes. A restore asked for after a delete brings the
data back: from the live bucket only if the position is after the
delete (section 1), otherwise from a backup. A restore asked for before
a delete that ran first is superseded. A move moves the restored actor.

**Files of [ADR 0030](0030-files-and-static-assets.md) (proposed).** A restored database names the files
of its own time, and this command restores no file. The replacement
keeps the actor's current `blob_incarnation`, carried over in step 6
like `operator_delete`, never the staged database's: a restore to a
point before a delete brings back rows whose files were swept, and they
stay gone (that record, section 5, amendment of 2026-11-17).

**The command's answers**: `.done` prints "Restored to txid N of
generation G, now txid M" and removes the outcome; `.conflict` prints
the head found, non-zero; `.refused` prints why, non-zero; `.absent`
and `.superseded` say so, non-zero; no outcome
and no request (expired): "dropped", non-zero; still there: "requested;
the cluster keeps trying until T + 1 h". As for deletes, only the
outcome acknowledges: it is written after the replacement was recorded.

### 7. Later: the change feed

A reader that follows the bucket and applies every actor's committed
changes to an external target, so that SQL can run across all actors:

```bash
wasmkeep feed --bucket s3://b/p --app acme/shop \
    --to duckdb --out shop.duckdb | --to parquet --out shop-parquet \
    | --to postgres --dsn-env SHOP_PG \
    [--state DIR] [--every 60s] [--classes carts,orders]
```

This record fixes its interface and guarantees, not its design.

- **What it reads**: what section 1 reads, nothing more: listings and
  objects of the app's history, with no hold, no node, no write to the
  bucket. Its progress (each actor's last applied position, and its
  replayed database if it keeps one) is in `--state DIR` or in the
  target, never in the bucket.
- **The unit** is an actor's state at a position: tenant, app, class,
  name, generation, txid and commit time (the LTX stamp). It is applied
  as a replacement of that actor's rows in the target (the actor's
  partition), in one target transaction where the target has them.
  Row-level changes (diffs between two positions, which pages alone do
  not give) are a later refinement with the same guarantees.
- **Keyed** by `(tenant, app, class, name)`. Target tables are per
  class and table, with `_wk_tenant`, `_wk_app`, `_wk_name`, `_wk_txid`,
  `_wk_generation` and `_wk_committed_at` columns beside the actor's.
- **At least once.** A state may be applied again after a crash of the
  feed. Applying is idempotent when the target ignores a position at or
  below the one it holds for that actor, which the feed's writes check.
- **Ordered per actor** along the chain, never across actors. A feed may
  skip positions (several commits coalesced into one state); it never
  emits a position that is not on the chain it read.
- **Late tips** (section 1): when an actor's head stops descending from
  the last position emitted, the feed emits the new head as a full
  state marked `rebase`, which replaces the partition like any other.
- **Deletes**: a deleted actor's partition is emptied (a state marked
  `deleted`). Erasure in the target is then the feed's to honour.
- **Lag**: at least one poll interval, plus a read per changed actor.

Not decided: how the feed finds changed actors without listing the
whole app at each poll (bucket event notifications, or a change index
the nodes would write, which is a write path change), schema drift
between actors of one class, and how many actors one feed follows.

### 8. Cost in bucket requests

The [automated fixture drill](../deploy/backup-and-restore.md#automated-fixture-drill)
now records timings and the restored node's ObjectStore operation counters
in `bench/results/*-restore-drill-*.csv`. These counters exclude backup CLI
requests, S3 pagination/retries and cleanup; the table below remains derived,
not an end-to-end measured request bill. A moto run proves the drill path,
not real-bucket cost or fleet recovery latency.

Counts per operation, from the code's request patterns, with `n` the
objects under an actor's prefix (at most about 65 at rest after
collection, plus write rate times `W` for a busy actor), `s` the
segments replayed (at most 64 while periodic snapshots succeed: the
snapshot follows the 64th segment), `A` an
app's actors and `K` its history objects. A listing is one LIST per
1,000 keys. C is the number of serialized manifest chunks (at least one per actor,
more for oversized records). These are derived from the code, not measured. The
measured floor they add to is three idle nodes of 0.4.1 making 8.6
million LISTs and 13 million GETs a month
([at rest](../benchmarks.md#bucket-requests-at-rest)). 0.4.2 lists the
intents far less (ADR 0014, amendment of 2026-11-14) and has not been
measured at rest, so the shares of that floor given below are lower
bounds: against 0.4.2's floor these tools weigh more.

| Operation | LIST | GET | PUT | DELETE |
| --- | --- | --- | --- | --- |
| `actor history` | ⌈n/1000⌉ | 0 (1 with the record hint) | 0 | 0 |
| `actor export` / first `actor sql` | ⌈n/1000⌉ | 1 + s, +1 for the record hint | 0 | 0 |
| `actor sql` with a warm cache | ⌈n/1000⌉ | new segments only | 0 | 0 |
| A retry after collection (section 1) | +⌈n/1000⌉ | up to +64 | 0 | 0 |
| `backup create`, first run, `head` | ⌈K/1000⌉ + 1 (`prunes/`) | Σ (2 + s) over actors, + deploy and secrets objects | the same, in the destination, + C + 4 | 0 |
| `backup create`, later run, `head` | ⌈K/1000⌉ + 1 | per moved actor: new objects of its plan (publication refused if a prune ran meanwhile) | the same, + C + 4 | 0 |
| `backup create`, `all` | ⌈K/1000⌉ | one per object written since the last run | the same | 0 |
| `backup verify` | 0 (manifest) | Σ (2 + s) in the destination, or for the sample | 1 | 0 |
| `backup restore` | 2 (nodes, empty check); in place, + a backup run's and ⌈old keys/1000⌉ per listing until empty | Σ (2 + s) from the destination; in place, + one record read per old actor | Σ (2 + s) + one marker per actor + one or two per alarm + deploy + `restoring` and `restored` | 1 (`restoring`); in place, + every old key |
| `actor restore`, the command | export's | export's + one per second waited | staged (1, or its parts) + request + `control/changed` | 1 (outcome) |
| `actor restore`, the nodes | 1 per node per `control/changed` change, and 1 per node per 60 s at rest for the prefix; the owner's restore listing | acquire and restore as usual, + staged object | entry, replacement, outcome, `control/changed`, release, snapshot | old entry, request, staged |
| change feed, per poll | ⌈K/1000⌉ | one per new segment | 0 in the bucket | 0 |

What follows from it:

- **Reading one actor costs what waking it on a new node costs** (a
  restore), and no node does anything.
- **A backup's listing is per app, not per actor**: an app of 10,000
  actors at 20 objects each is 200 LISTs a run. Hourly, that is about
  146,000 a month, under 2 % of the idle floor above. Listing each
  actor's prefix instead would be 10,000 LISTs a run, fifty times more.
- **An idle actor costs a later run nothing** but its share of the
  listing.
- **`--history all` adds a GET and a PUT per write**: each write, one
  PUT at the source, adds a GET at the source and a PUT at the
  destination, with the manifest growing by one entry per write kept. `head` costs at most one snapshot and 64 segments per
  actor per run.
- **Pruning** reads every kept manifest (C chunk GETs per kept run)
  and lists `data/`, ⌈objects/1000⌉ LISTs.
- **The change feed listing a whole app every minute** is
  ⌈K/1000⌉ × 43,200 LISTs a month: 8.6 million for an app of 200,000
  objects, as much again as the idle floor. That is why finding changed
  actors is the feed's open question.
- On a bucket that does not bill requests these are only load, which
  the read limits of ADR 0007 already bound per tool.

## Consequences

- An operator can read, query, export and restore one actor, and back
  up and restore an app, without stopping the cluster or touching a
  node. A team evaluating WasmKeep gets a yes to both questions, with
  the cross-actor limit stated.
- The read-only tools are the restore path without the hold. They share
  `plan_restore`, `wal::Restorer` and the checksums as they stand. The
  bounded reads (`download`, `bounded_get`, `list_sized`) are private to
  `wasmkeep-server` and move to a crate the command line can use, or are
  written again for it. The command line gains a dependency on `wasmkeep-storage`'s
  restorer and SQLite (already built for the server).
- The backup guide changes its central rule (section 4), and a
  `backup create` copy becomes the recommended backup. `rclone` stays as
  the stopped-cluster method.
- Server changes: on the write path, the restore job and its
  replacement segment, the `restores/` family, and the host meta keys
  `operator_restore` written by restores and carried by deletes only when
  non-empty, and `replaced` written by every delete and restore;
  off it, the `restoring` and `restored` markers a node obeys.
  Per CLAUDE.md it needs its simulation cases, cluster tests and an
  adversarial review before it merges.
- Backups keep deleted actors' data until pruned: "a delete erases" is
  true of the live bucket only, as with bucket versioning today.
- Collection is not changed and not slowed. A cluster at
  `--history-retention-secs 0` cannot be backed up online reliably, and
  is told so.

## Alternatives

- **Copy the bucket with an S3 tool, twice** (the backup guide's
  reasoning): no verification, no per-actor proof, and the ownership
  records and alarm index of different moments. It also needs the
  window to cover the whole copy, not one actor. Kept as the
  stopped-cluster method only.
- **Hold collection with a marker**: weighed in section 4. Not needed
  for safety, and a lease of its own for liveness.
- **Bucket versioning**: a complement, not a replacement. It needs a
  tool to rebuild a moment from versions (the guide), and it keeps
  every version of every key, at a cost per write that `--history all`
  pays only in the destination.
- **Export through the owner** (a node call that dumps the database):
  wakes the actor, loads its node, needs node credentials or a new API
  on the cluster port, and gives the same answer the bucket gives.
- **The command line writes the replacement itself**: it holds no node
  lease, so it cannot hold the actor (ADR 0020). Writing history without
  a hold is what every rule here forbids.
- **Restore by rewinding history**: deletes acknowledged writes, makes
  a lower head that late uploads displace, and moves txids backward
  under sidecars, alarm tags and collection. Rejected in section 6.
- **Restore by stopping the actor** (a move to nowhere, or a delete then
  a write): the delete machinery already provides the fence (a holder,
  a mailbox, a marker, an outcome). A restore is the same request with
  other content, not a second mechanism.
- **A consistent cut across actors** (all actors at one moment): needs
  coordination every write would pay for, or every actor paused. The
  actor model promises nothing across actors, and backups do not either.
- **Copy objects on the provider's side** (`CopyObject`): fewer bytes
  through the tool, but no replay, so no verification and no alarm
  index, and the source and destination would have to share an account.
  Possible later as an option for a destination in the same account,
  with `backup verify` run after it.

## Not decided here

1. **The minimum node version** for `restoring` and `restored`, and
   whether a node of an earlier version should be detected by its
   heartbeat's capability words rather than its version line.
2. **Restoring an address the bucket does not know** (never created, or
   its app recreated): the first upload would have to be a snapshot of
   the staged database at a fresh position. Refused until asked for.
3. **Importing a database not from history** (`actor restore --file
   fixed.db`): the job of section 6 accepts any staged database, so
   the mechanism exists. Whether operators may write state no actor
   wrote, and how its schema is checked, is a product question.
4. **Two credentials in one command** (`--to-env FILE`), or a profile
   file shared with the other commands.
5. **Files of [ADR 0030](0030-files-and-static-assets.md)**: decided there (section 5, amendment of
   2026-11-17): copied after the history, as listed, and a run records
   the incarnation each database names.
6. **`wasmkeep bucket verify`** for the live bucket (the roadmap's
   "verify a bucket"): section 1's replay over every actor, writing
   nothing. It falls out of `backup create` without the destination.
   Its cost is a backup's GETs.
7. **A node with no bucket** (`serve` with a data directory only): its
   databases are local files, and SQLite's online backup API would
   export them. It is not part of this record.
8. **The change feed's discovery, schema drift and scale** (section 7).
9. Resolved by step 8: shared objects are retained by the union of kept
   manifests; `before-restore` pins have no automatic expiry.

## Tests (to build)

**Unit tests** (`wasmkeep-cluster`, pure functions beside
`plan_restore`):

- the copy set of an actor includes the segment at the plan's snapshot
  position. Without it, a set holding older segments of the same
  generation plans to an older head (scripted: a deleted actor whose
  pre-delete segments are present plans to the pre-delete head);
- `--at` resolution walks the current chain only, uses the LTX stamps,
  and handles a stamp lower than its parent's within the skew;
- deletes and restores are found by the `replaced` meta, and an
  unlabelled delete only as "probably";
- a position before the newest delete reports as erased, not missing;
- the destination's carry-over refuses a plan key whose size or `ETag`
  changed (a recreated app's keys come back with other bytes).

**Prune unit tests** (`wasmkeep-cli`, in-memory stores): retention and age
boundaries, `latest`, shared and unshared content, undo labels, a real interrupted
restore fence, malformed kept manifests, archive-prefix isolation, both
create/prune start orders, concurrent pruners and pin admission, and a fault
before/after each mutation (including lost responses) followed by recovery.

**Simulation** (`sim/tests/madsim_backup.rs`, new). Backup ordering
against collection is the kind of interleaving CLAUDE.md asks a
simulation for. It runs the replication simulation's owners (node
leases, held and late uploads, kills, skewed clocks), collecting by
`collectable` with deletes that land late, and deleting actors (reset, then erase). Beside them, a
backup reader lists by pages with delays between pages and between a
listing and its GETs, copies, retries by section 1, and runs
incrementally. Invariants checked at the end of each seed:

- every actor the run marks copied replays, from the destination's
  objects named by the manifest, to the recorded head, with every
  checksum;
- that head holds every write acknowledged before the actor's listing;
- a deleted actor's copy replays to the empty database;
- per-run manifests remain preserved and every recorded recoverable range
  replays from destination bodies; a listing/GET collection miss is reported
  as a retry or history gap, never proof of continuous coverage.

Unsafe variants some seed must catch: copying the plan without the
segment at `P` (with incremental runs and deletes); marking an actor
copied from the listing without the replay; no re-listing after a 404 or
a missing link; reading `wal/` and `snapshot/` from different listings
and trusting the plan; keying the destination by source key (with an
app deleted and recreated between runs, so generations restart); a
prune that ignores a running create. The test also fails if
no seed makes a reader retry (it cannot pass vacuously), and records collection misses rather than manufacturing retries from random
booleans. The model bodies are abstract positions/reset markers; CLI tests
exercise real LTX and SQLite checksums.

**The restore job** joins two existing simulations. In
`madsim_replication`, an owner writes a replacement: every acknowledged
write before it is replaced, none after it is lost, and a restore never
shows a pre-restore state after its acknowledgement. In
`madsim_alarm_index`, the job's entry is created before the upload and
the old one retired after, under late deletes and takeovers. The unsafe
variant is the entry written after the upload.

**Cluster tests** (`crates/wasmkeep-server/tests/cluster/`, new files
`export.rs`, `backup.rs`, `restore.rs`):

- **Exports during writes**: a counter written continuously, exported
  in a loop. Every export passes its checks, and its value is at least
  the count acknowledged before the export started, and at most the
  count sent.
- **Exports during kills**: the owner killed mid-write and taken over;
  exports before, during and after. Each is a state on the final chain
  or a tip the test logs as overtaken, never torn, never failed checks.
- **Exports racing collection**: retention 1 s, bursts of 64 writes
  during a slowed export (an injected delay between listing and GETs).
  The retry path runs and is counted, and the result is right.
- **Deleted actors**: export gives the empty database, `--txid` before
  the reset is refused, and `history` shows the reset.
- **`actor sql`**: each format against a fixture, writes refused
  (`INSERT`, `ATTACH`, a second statement), host tables hidden and
  shown, and a cache replaying only new segments.
- **Backup during writes and kills**: three nodes, writers on 200
  actors, a node killed every 20 s, two runs, `head` and `all`. Then
  restore into an empty prefix, start a node, and check that every actor
  restores to at least its acknowledged count at its listing. Alarms set
  in the databases fire from the rebuilt index, and the second run read
  only moved actors (by `wasmkeep_bucket_requests_total`).
- **Backup racing deploys**: a deploy during the run is recorded as the
  newer version.
- **In-place restore**: the app's nodes stopped, another app's node
  serving throughout. Old generations are not reused, local files of
  the app are removed by the `restored` marker, and the `before-restore`
  run restores the replaced state.
- **Backup across a recreated app**: `app delete`, `deploy --recreate`
  and new writes between two runs; the second run copies the new bytes
  and a restore of either run gives that run's data. A delete and a
  recreate within one run fail it.
- **Single-actor restore**: while writes go on, with an open socket
  (closed 1012, its close event run before the replacement, new upgrades
  answered 503 meanwhile), with a pending alarm (fires after the
  restore, old entry gone, old timer cancelled), the owner killed
  mid-job (the request completes once, by the marker), a `--if-head`
  conflict after a close event wrote, a delete request left behind (not
  re-run, by the carried marker), a restore asked for before a delete
  that ran first (superseded), the app recreated before the job
  (absent), a staged database of another page size (refused), a refused
  request retried by the fallback listing and an expired one removed
  with its staged object, a moving actor, and an undo by `--txid`.

**A restore drill**, scripted (`scripts/drills/backup-restore.sh`),
run by `check.sh --s3` against moto or a real bucket, and once by hand
in the soak (`bench/soak.sh`):

1. a backup run during the soak's load and kills;
2. `backup verify`;
3. restore into a scratch prefix, one node with outbound traffic
   blocked except to the bucket, as the guide's drill says;
4. a sample of actors compared with the soak's acknowledged log;
5. the run's time, the restore's time and the request counts recorded
   in `bench/results/`, so that section 8's figures become
   measurements.

## Plan

One PR each, with tests that fail before it:

1. This record; ROADMAP and the backup guide link it.
2. `actor history` and `actor export`, in `wasmkeep-cli` on
   `plan_restore` and `wal::Restorer`, with the unit and cluster tests
   above.
3. `actor sql`.
4. `madsim_backup` with the reader model and its unsafe variants,
   before any backup code.
5. `backup create`, `list` and `verify`, the manifest and the cache.
   The backup guide is rewritten around them.
6. `backup restore`, into an empty root first, then `--in-place` with
   the `restored` marker (a node change, with its cluster test).
7. `actor restore`: the `restores/` family, the job, `AfterRestore`,
   `operator_restore` carried by every delete, and the two simulation
   additions; an adversarial review of the job.
8. `backup prune` (implemented; destination-only retention, permanent pins,
   marker handshake and conditional publication/deletion lock).
9. The drill in `check.sh --s3` and opt-in in the soak is implemented
   for the known test fixture. Section 8 records the measurement boundary;
   full request cost and a fleet run remain outstanding. Arbitrary guest
   alarm isolation still requires an operator firewall.
10. The change feed, after its open questions are answered in a record
    of its own.

## Restore operation recovery

Actor restore uses its request ID; whole-app restore uses the journal nonce.
Both print the ID before waiting and accept `--resume`. A resume never files
a new request. Actor outcomes remain at the existing adjacent keys without
being consumed and expire after the request TTL; after expiry the ID is
unknown and nothing is filed.

Whole-app restore matches an active journal's nonce and follows its existing
exclusive-lock recovery rules. With no journal, it answers done only if
`apps/<tenant>/<app>/restored` holds that nonce. There are no durable completion
receipts in this release; those move to 0.6.0.
See [operation recovery](../operations.md).
