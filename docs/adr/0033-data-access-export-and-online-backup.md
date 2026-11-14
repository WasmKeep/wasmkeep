# 0033. Data access, export and online backup

- Status: proposed (2026-11-14)
- Date: 2026-11-14
- Milestone: [After that](../../ROADMAP.md#after-that), "operating
  without the author": inspect or export one actor, verify a bucket,
  back a cluster's bucket up to another and restore it
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
  conditionally, and never rewritten: a key names one content for good.
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
  (`mark_old_lease`, ADR 0020, section 6). A prefix with history but no
  ownership record is safe to start on, as long as its markers are right.
- **The alarm index is derivable.** Each database records the exact
  entry it is indexed under (`alarm_indexed` in `_wasmkeep_meta`,
  ADR 0009), and its alarm in `_wasmkeep_alarm`.
- **Requests at rest cost money** ([benchmarks](../benchmarks.md#bucket-requests-at-rest)):
  on R2 and S3 a LIST is billed like a PUT, and listings page by 1,000
  keys. Whatever a tool lists per actor, per run, multiplies by the
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

`d` is a few seconds (at most 64 objects of an actor, read 8 at a time,
ADR 0007's read rules). With `W = 0`, a busy actor can keep a reader
retrying until the 8 attempts are spent. The tool then fails that actor
and names the cause ("collected faster than read: raise
`--history-retention-secs`"). It never returns a wrong database. The
docs ask for `W` of at least five minutes on a cluster that is backed up
or exported, which the default of an hour is.

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
- nothing before the newest reset in the chain: a delete erases at once
  (ADR 0012, "Deleted actors"), and a restore of section 6 keeps the
  window.

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
before it". Positions on abandoned branches are never chosen: `--at`
and `--txid` both walk the current chain only. To find the position the
tool narrows with the listing's `last_modified` (live bucket) or the
run manifest's recorded stamps (backup), then reads the headers of the
segments it replays anyway. A `T` before the oldest position that can
be replayed is an error that prints the range.

**Resets.** A reset (`ship_reset`, ADR 0028) and the replacement of
section 6 are both one segment that rewrites the whole database: its
LTX header's `commit` equals the number of pages it holds, starting at
page 1. The tool labels such a position "replaced", and "deleted" when
the database after it has no table outside `_wasmkeep_*`. A replay of a
deleted actor's head gives the empty database (with its host meta),
and says so. `--txid` below the newest reset is refused with "erased
by a delete at txid r; restore it from a backup".

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
that can be replayed, the snapshots on the chain, the replaced and
deleted positions, the objects and bytes under the prefix, and how many
objects are off the chain (late uploads, not yet collected). `--json`
for scripts. It is what an operator runs before `--at` or `actor
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
  keeps one file per actor with a sidecar naming its position and
  checksum (as a node's `.replica` sidecar does). With a cache, a second
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
destination needs PUT (create only, `If-None-Match: *`). Only `prune`
needs DELETE there, and is meant to run with another credential. The
destination's credentials come from `--to-env FILE` (the variables
`object_store` reads, in an env file), so that the nodes' credentials
never reach the copy and the copy's never reach the nodes, as the
backup guide already asks.

**The destination's layout**:

| Keys | What |
| --- | --- |
| `data/<source key>` | Copies of source objects, under the key they have in the source (`data/state/acme/shop/actors/...`, `data/apps/acme/shop/<version>/...`). Each is created once and never rewritten, as at the source. |
| `runs/<run id>/actors/<chunk>` | The run's manifest, 1,000 actors per chunk (below). |
| `runs/<run id>/app/<tenant>/<app>` | The deploy version and the secrets version the run copied for that app, with their keys. |
| `runs/<run id>/complete` | Written last: the run's summary and the SHA-256 of every chunk. A run is **restorable** only if this object exists and its chunks match it. |
| `latest` | The id of the newest complete run (the one mutable key; a hint, `backup list` lists `runs/`). |

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
   is still there (a 404 there is skipped, not retried).

Each object is read from the source, checked (its size against the
listing, its LTX trailer checksum), applied to the actor's replay, and
written to the destination with `If-None-Match: *`. A conflict there
means the object was already copied (keys name one content), and the
run reads it back only if `verify` is asked. The tool always streams
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
is planned when its last key has been listed, then copied while the
listing goes on (bounded: 16 actors at a time, the restore read limits
of ADR 0007 per actor). Such a listing is not taken at one moment: an
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
fine with the default hour, because no actor's copy takes more than
seconds. This is what the backup guide's "two passes within the window"
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
  or the end: the app's run fails, since its state is being removed
  under it.

**Not copied, on purpose**:

| Keys | Why not |
| --- | --- |
| `R owners/` | Lease state of the running cluster. A record names nodes that are not in a restored one, and its generation is recomputed (section 5). |
| `R leases/` | Written anew by a restore, with the right generation (section 5). |
| `nodes/`, `cluster/secret` | The running cluster's leases and the secret its nodes sign calls with. A restored cluster writes its own (the secret is created when missing). |
| `R alarms/` | Copied at another moment than the history, it can miss an entry a copied database names (an alarm lost) or name one it does not. Rebuilt from the databases instead (section 5). |
| `R moves/`, `R deletes/`, `R restores/`, `control/` | Operator intents of that moment. A restore must not drain, move or delete again. |
| `probe/`, `diagnose/`, `cluster/migration` and the migration marks | Scratch and one-time state. |

Files of ADR 0030 (proposed), if it lands, live under `R blobs/` and
can be overwritten under conditions. A run would copy them after the
history, by listing, with no consistency between a row and the file it
names. That is left to that record (Not decided here).

**Incremental runs.** Every run is a full run in what it promises, and
incremental in what it moves:

- The previous complete run's manifest (or `--cache DIR`, which also
  keeps each actor's replayed database at its last head) says which
  objects the destination already holds. An actor whose head has not
  moved costs no GET and no PUT: its manifest entry is carried over,
  after its keys appeared in this run's listing.
- An actor that moved needs only the objects it does not have: with a
  cache, the new segments are applied to the cached database (they
  follow its head on the chain), so even a new snapshot needs no replay
  from it. Without a cache, a new snapshot is read and replayed from.
- Objects are never deleted from the destination by `create`.

**`--history head` (the default) or `all`.** `head` copies what each
actor's head needs: a busy actor costs one snapshot and up to 64
segments per run, however much it wrote since the last one. `all`
copies every history object the source still has. If the time between
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
complete ones (manifests first, then `complete` last, so a pruned run
is never half-listed as restorable). It then deletes the `data/` objects
that no kept run names. That listing of every kept manifest is the
costly part (section 8). Pruning is also how a delete reaches the
backups: a deleted actor's data stays in the runs from before its
delete until they are pruned. The docs say so beside `actor delete`.
`delete-self` and idle expiry erase the live bucket, not the backups.

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

1. **History.** For each actor of the run, copy the manifest's objects
   from `data/` to the target key, `If-None-Match: *`: the snapshot, the
   segment at `P`, the segments. Every actor's prefix then plans to the
   head the manifest recorded, and to nothing else.
2. **Generations.** Write each actor's lease marker,
   `R leases/<class>/<name>`, `wasmkeep-lease 2`, with the highest
   generation its copied keys carry. With no ownership record, the
   first node to acquire the actor seeds from the marker and the keys,
   whichever is higher, and acquires at that + 1 (`mark_old_lease`): no
   generation already in the history is ever used again.
3. **Alarms.** For each actor whose manifest records an alarm, create
   the entry its database names (`alarm_indexed`), exactly that key,
   with its generation and nonce. ADR 0009's R1 to R3 hold as written:
   the entry is the one the durable state names, and its generation is
   below any holder's to come, so the first holder may retire it when it
   moves the alarm. An actor whose database has an alarm but no
   `alarm_indexed` (written before tags) gets a fresh entry at its time
   with generation 0, which any holder may retire.
4. **Deploy and secrets**, with `--deploy backup` (the default here):
   the run's version directories, then `apps/<t>/<a>/current`; the
   sealed secrets version, then its `current`. A sealed secret opens
   only under the prefix, tenant and app it was sealed for (ADR 0016):
   under another prefix the command says to run `wasmkeep secrets
   reset`.
5. **Local files.** Write `apps/<t>/<a>/restored` with a body no
   writer used before. A node whose own copy of that marker differs
   removes its local files of the app (`<data_dir>/state/<t>/<a>/`)
   before it serves, as it does for `recreated` after `app delete
   --recreate` (ADR 0028, section 5). Without this, an actor created
   after the backup and absent from it would come back from a node's
   disk as its first snapshot, as the backup guide warns today.
6. Print the command that starts the nodes, and what the restored app is
   (below).

**In place** (`--in-place`): the app's state is replaced under the same
prefix, while the cluster's other apps run.

1. The app's nodes must be stopped: no node lease of the app `live`, and
   `LATE_WRITES` (40 s) passed since the last one was written or fenced,
   exactly the conditions `app delete` waits for and the same fencing
   (`cluster fence`, or `--force` after it). An upload held by a dead
   node lands within that window or never.
2. **The state being replaced is backed up first**, by an ordinary
   `backup create` run into the same destination, marked
   `before-restore`. A wrong restore can then be undone by restoring
   that run.
3. The command reads, for each actor, the generation of its ownership
   record and the highest generation its current history keys carry.
4. It deletes every key under `state/<t>/<a>/` (as `app delete`,
   step 2, without the tombstone), and lists until a listing is empty.
5. It runs the steps above, except that each marker carries the
   highest of the run's keys, the old record and the old keys, plus
   one. No generation the prefix ever used comes back: the local files'
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
builds. A restore is therefore also undoable: the history before it is
kept for the retention window like any other, and `actor restore --txid
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
3. **Stages** the database as one LTX snapshot file at
   `R restores/<class>/<name hex>/<id>.ltx` (multipart for a large one),
   `<id>` being 16 random hex digits, then writes the **request**
   `R restores/<class>/<name hex>/<id>` with
   `wasmkeep-restore 1`, where the state came from (the live bucket and
   position, or the backup run and position; never a credential), the
   staged object's size and post-apply checksum, and `--if-head` when
   given. Then it writes `control/changed` (ADR 0014's marker, amendment
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
0024). Nodes list `restores/` only when `control/changed` changes and
when they start, never at rest (a restore always writes the marker; no
older command writes restore requests). Requests expire after an hour
by the bucket's clock, their staged object with them. A node that ends
a request deletes both.

**The restore job**, in the actor's mailbox, no guest code:

1. Forget any learned owner. `ensure_owner`: acquire and restore, or
   nothing if held. Refused: the request stays for a later pass.
2. **Replays.** The host meta `operator_restore` holds `<T> <id>` of the
   last restore request carried out. If it is this request, or a later
   one, the job **fences** (ADR 0028, section 3, step 3: what a restore
   read is the state only once this hold's upload is in) and reports
   done.
3. **`--if-head`**: if the current head is not the one given, the
   outcome is `<id>.conflict` with the current head, and nothing
   changes. Without it, the restore replaces whatever was written
   since the operator looked, which the history keeps (undo, above).
4. **Read the staged object** (GET), check its LTX trailer and its
   post-apply checksum against the request, and its size against the
   node's `--max-db-bytes`. A refusal writes `<id>.refused` with why.
5. **Sockets**: any open or closing are closed with **1012** ("service
   restart"): the actor goes on existing, rewound, and clients should
   reconnect, unlike a delete's 1000. Their close events run in the
   mailbox first, on the state being replaced: their writes are in the
   history before the replacement, not after. The job answers `Busy`
   and is queued again, waiting up to 5 s, as a delete's is.
6. **Build the replacement.** The staged database, with its host meta
   set for this actor now, not then:
   - `operator_delete`: the current database's value, carried over, so
     a delete request left behind never deletes the restored actor
     (ADR 0028, section 3, step 5);
   - `operator_restore = <T> <id>`;
   - the alarm: if the restored database has one at `a`, first **create
     a fresh index entry** for `a` (this hold's generation, a new nonce,
     R1) and set `alarm_indexed` to it; if not, clear `alarm_indexed`.
     The entry comes before the upload that names it, as ADR 0009
     requires. An entry left by a failed upload names no state and is
     cleaned as such;
   - `alarm_failures` cleared: the restored alarm starts a fresh backoff.
7. **Upload** it as one segment on the current head (`ship_reset`'s
   path), acknowledged only if it completed while the hold was valid.
   Not acknowledged: the actor is given up as after a failed
   `exec-durable`, and the request stays. The next attempt restores
   whatever the bucket holds and finds out, by the replay marker,
   whether it happened.
8. **After the acknowledgement**: retire the index entry the replaced
   state named (R2: the durable state no longer names it); arm the
   node's timer with `a` (an alarm already overdue fires soon, and one
   that fired before the restored point fires again: at least once);
   upload a snapshot at the replacement's position (a new kind,
   `AfterRestore`, collected with the window like a periodic one, never
   erased); write `<id>.done` with the new head; delete the request and
   the staged object; drop the instance and hibernate the stub, so the
   next event opens the restored database (the local file is at the
   head, by its sidecar). `on-init` does not run again unless the
   restored state never ran it.

**Every delete carries `operator_restore` over**, as it carries
`operator_delete` (`delete-self`, `delete-self-durable`, the operator
delete and idle expiry): a restore request left behind must not bring
back an actor deleted after it was carried out. A node from before this
record does not carry it. During a rolling upgrade, a restore request
whose key could not be deleted can then run again on an actor such a
node deleted. The same caveat is ADR 0028's for deletes.

**Ordering with other requests.** Deletes and restores of one actor
run in the order their jobs reach the mailbox. Each is checked against
its own marker, so a delete after a restore deletes, and a restore after
a delete brings the data back, from the live bucket only if the
position is after the reset (section 1), otherwise from a backup. A move
moves the restored actor.

**Files of ADR 0030 (proposed).** A restored database names the files
of its own time. A restore to a point before a delete brings back an
incarnation whose files were swept. That record already says such a
restore takes the files from the same backup. Restoring files is not in
this one.

**The command's answers**: `.done` prints "Restored to txid N of
generation G, now txid M" and removes the outcome; `.conflict` prints
the head found, non-zero; `.refused` prints why, non-zero; no outcome
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

Counts per operation, from the code's request patterns, with `n` the
objects under an actor's prefix (at most about 65 at rest after
collection, plus write rate times `W` for a busy actor), `s` the
segments replayed (at most 63 while periodic snapshots succeed), `A` an
app's actors and `K` its history objects. A listing is one LIST per
1,000 keys. These are derived from the code, not measured. The
measured floor they add to is three idle nodes making 8.6 million LISTs
and 13 million GETs a month
([at rest](../benchmarks.md#bucket-requests-at-rest)).

| Operation | LIST | GET | PUT | DELETE |
| --- | --- | --- | --- | --- |
| `actor history` | ⌈n/1000⌉ | 0 (1 with the record hint) | 0 | 0 |
| `actor export` / first `actor sql` | ⌈n/1000⌉ | 1 + s, +1 for the record hint | 0 | 0 |
| `actor sql` with a warm cache | ⌈n/1000⌉ | new segments only | 0 | 0 |
| A retry after collection (section 1) | +⌈n/1000⌉ | up to +64 | 0 | 0 |
| `backup create`, first run, `head` | ⌈K/1000⌉ | Σ (2 + s) over actors, + deploy and secrets objects | the same, in the destination, + ⌈A/1000⌉ + 3 | 0 |
| `backup create`, later run, `head` | ⌈K/1000⌉ | per moved actor: new objects of its plan | the same, + ⌈A/1000⌉ + 3 | 0 |
| `backup create`, `all` | ⌈K/1000⌉ | one per object written since the last run | the same | 0 |
| `backup verify` | 0 (manifest) | Σ (2 + s) in the destination, or for the sample | 1 | 0 |
| `backup restore` | 2 (nodes, empty check) | Σ (2 + s) from the destination | Σ (2 + s) + A markers + one per alarm + deploy | in place: K + the old keys |
| `actor restore`, the command | export's | export's + one per second waited | staged (1, or its parts) + request + `control/changed` | 1 (outcome) |
| `actor restore`, the nodes | 1 per node per `control/changed` change; the owner's restore listing | acquire and restore as usual, + staged object | entry, replacement, snapshot, outcome, release | old entry, request, staged |
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
- **`--history all` doubles a busy app's requests**: each write, one
  PUT at the source, adds a GET at the source and a PUT at the
  destination. `head` costs at most one snapshot and 64 segments per
  actor per run.
- **Pruning** reads every kept manifest (⌈A/1000⌉ GETs per kept run)
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
- The read-only tools are the restore path without the hold, so they
  share its code and its tests: `plan_restore`, `wal::Restorer`, the
  checksums. The command line gains a dependency on `wasmkeep-storage`'s
  restorer and SQLite (already built for the server).
- The backup guide changes its central rule (section 4), and a
  `backup create` copy becomes the recommended backup. `rclone` stays as
  the stopped-cluster method.
- One server change on the write path: the restore job and its
  replacement segment, the `restores/` family and two host meta keys.
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

1. **Restoring an address the bucket does not know** (never created, or
   its app recreated): the first upload would have to be a snapshot of
   the staged database at a fresh position. Refused until asked for.
2. **Importing a database not from history** (`actor restore --file
   fixed.db`): the job of section 6 accepts any staged database, so
   the mechanism exists. Whether operators may write state no actor
   wrote, and how its schema is checked, is a product question.
3. **Two credentials in one command** (`--to-env FILE`), or a profile
   file shared with the other commands.
4. **Files of ADR 0030**, if it lands: copy order, and whether a run
   records which incarnations its databases name.
5. **`wasmkeep bucket verify`** for the live bucket (the roadmap's
   "verify a bucket"): section 1's replay over every actor, writing
   nothing. It falls out of `backup create` without the destination.
   Its cost is a backup's GETs.
6. **A node with no bucket** (`serve` with a data directory only): its
   databases are local files, and SQLite's online backup API would
   export them. It is not part of this record.
7. **The change feed's discovery, schema drift and scale** (section 7).
8. **Pruning runs that share objects across apps** with `--all`, and a
   retention rule for `before-restore` runs.

## Tests (to build)

**Unit tests** (`wasmkeep-cluster`, pure functions beside
`plan_restore`):

- the copy set of an actor includes the segment at the plan's snapshot
  position. Without it, a set holding older segments of the same
  generation plans to an older head (scripted: a deleted actor whose
  pre-delete segments are present plans to the pre-delete head);
- `--at` resolution walks the current chain only, uses the LTX stamps,
  and handles a stamp lower than its parent's within the skew;
- resets and replacements are recognised by their headers;
- `--txid` below the newest reset is refused.

**Simulation** (`sim/tests/madsim_backup.rs`, new). Backup ordering
against collection is the kind of interleaving CLAUDE.md asks a
simulation for. It runs the replication simulation's owners (node
leases, held and late uploads, kills, skewed clocks), collecting by
`collectable` with deletes that land late, with retention windows of 0,
short and long, and deleting actors (reset, then erase). Beside them, a
backup reader lists by pages with delays between pages and between a
listing and its GETs, copies, retries by section 1, and runs
incrementally. Invariants checked at the end of each seed:

- every actor the run marks copied replays, from the destination's
  objects named by the manifest, to the recorded head, with every
  checksum;
- that head holds every write acknowledged before the actor's listing;
- a deleted actor's copy replays to the empty database;
- with `--history all` and runs closer than the window, every
  acknowledged position replays from the destination.

Unsafe variants some seed must catch: copying the plan without the
segment at `P` (with incremental runs and deletes); marking an actor
copied from the listing without the replay; no re-listing after a 404 or
a missing link; reading `wal/` and `snapshot/` from different listings
and trusting the plan; copying segments before their snapshot with a run
stopped half-way and restored from the union. The test also fails if
no seed makes a reader retry (it cannot pass vacuously), and with
retention 0 it counts starved actors rather than failing.

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
- **Single-actor restore**: while writes go on, with an open socket
  (closed 1012, its close event run before the replacement), with a
  pending alarm (fires after the restore, old entry gone), the owner
  killed mid-job (the request completes once, by the marker), a
  `--if-head` conflict, a delete request left behind (not re-run, by the
  carried marker), a moving actor, and an undo by `--txid`.

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
8. `backup prune`.
9. The drill in `check.sh --s3` and in the soak; section 8 measured.
10. The change feed, after its open questions are answered in a record
    of its own.
