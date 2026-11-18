# 0039. The change feed

- Status: accepted, as built (2026-11-17), slices F1 to F5 and F7:
  `wasmkeep feed` and `wasmkeep feed compact`
  ([the change feed](../change-feed.md)). Slice F6, the listing measured
  at 100,000 actors, ran on 2026-11-18: 51.4 s a poll at rest with 2
  history objects per actor
  ([benchmarks](../benchmarks.md#the-change-feed-at-100000-actors)); a
  large history lists for minutes, and parallel listing is the follow-up.
- Date: 2026-11-17
- Milestone: [Next](../../ROADMAP.md#next), `0.6.0`, fourth item
- Follows: [ADR 0033](0033-data-access-export-and-online-backup.md)
  (reading an actor without its owner, section 1; the feed's interface
  and guarantees, section 7), [ADR 0034](0034-batched-plain-writes.md)
  (a position is a segment; what a batched class leaves unacknowledged),
  [ADR 0035](0035-a-recorded-head-on-hand-over.md) (offline readers gain
  no current-head guarantee and keep their LIST assumptions),
  [ADR 0012](0012-bucket-garbage-collection.md) (what collection and a
  delete leave under an actor's prefix),
  [ADR 0028](0028-actors-that-end.md) (deletes, deleting and recreating
  an app) and [ADR 0020](0020-one-lease-per-node.md) (generations).
- Amends, once accepted: ADR 0033, section 7 (the first target is
  Parquet alone; two more `_wk_` columns; its three undecided points are
  decided here) and section 8 (the feed's row of the request table).
  ADR 0033's text is left as it is until then.

## Context

An actor's data is one SQLite database, and an app is many of them. A
question across actors ("how many carts hold this article", "which
rooms were active this week") has no place to run: `wasmkeep actor sql`
reads one actor, and a backup is a copy of history, not a table.

ADR 0033, section 7 fixed what a feed promises and left its design
open: how it finds changed actors, what it does when actors of one
class do not have the same columns, and how many actors it can follow.
The code it can stand on exists:

- `history::export_with_options` (`wasmkeep-server`, `replica/history.rs`,
  called by `actor_read.rs`) plans an actor from a listing with
  `Listing::from_keys` and `plan_restore`, reads the plan with the
  retries of ADR 0033 section 1, replays it and verifies it (LTX
  trailers, the checksum chain, the final checksum, `integrity_check`).
  Its result names the position, the listed head, the capture stamp and
  `readonly::Verification`: the app tables and the `replaced` label.
- `history::ancestry` walks a listing from a head to the oldest parent
  the listing still holds.
- `backup create` lists an app's history once and cuts the stream into
  one group of keys per actor (`actor_groups` in the CLI's `backup.rs`),
  planning each actor from its group. It reads the app's markers
  (`restoring`, `deleted`, `recreated`, `restored`) to know which
  incarnation of the app it copies, because a recreated app can reuse a
  history key with other bytes.
- `history::list_objects` keeps each key's `ObjectMeta` from the same
  listing, so an object has an identity beyond its name.

What is missing is the loop around them, a place to keep progress, and
a format that a query engine reads.

## Decision

`wasmkeep feed` polls one app's history and appends, for every actor
whose head changed, the actor's **whole state at that head** to Parquet
files in a local directory. It only reads the bucket. Its progress is a
second local directory. A view over the files gives each actor's latest
state, which is the table to query.

```bash
wasmkeep feed acme/shop --bucket s3://b/p \
    --to parquet --out shop-feed --state shop-feed-state \
    [--every 60s] [--classes carts,orders] [--max-bytes N]
```

The app is positional, as for `actor export`. Without `--every` the
command runs one poll and exits (0 when every changed actor was
written, 2 when some were left for the next poll), which suits a timer.
With `--every` it polls again that long after the previous poll
*ended*. `--to parquet` is the only target of this record; `duckdb` and
`postgres` of ADR 0033 stay later targets with the same unit and
guarantees.

### 1. What the feed never does

- **No write to the bucket.** No PUT, DELETE or copy, under any prefix:
  no marker, no progress object, no lock. A credential that can only
  read the app's history (`actors/`) and the app's four markers is
  enough, and the feed asks for nothing else. It does not read
  ownership records, intents, deploys or secrets.
- **No node.** It calls no node API, takes no hold, wakes no actor and
  changes nothing a node watches. A node sees a listing and some GETs.
- **No ordering across actors**, and no instant at which the whole
  output is the app: two actors are read at different moments.
- **No row-level change.** A state replaces the actor's rows. A diff
  between two positions is not produced (ADR 0033: pages alone do not
  give it).
- **No backup.** The output leaves out the host tables
  (`_wasmkeep_meta`, `_wasmkeep_alarm`) and cannot be restored into a
  cluster. It can always be thrown away and built again from the bucket.

### 2. The unit and the position

The unit is ADR 0033's: one actor's state at one position. A
**position** is `(generation, txid)`, the `SegmentId` of the newest
object of the plan (`Plan::head`): the segment counter and the ownership
generation that wrote it. Its time is the segment's LTX `timestamp_ms`.

The feed emits heads only. Between two polls an actor may have written
many segments: they are coalesced into the one state at the head the
listing shows. It never emits a position that is not the head of a
complete, verified plan.

**What a head may contain.** ADR 0033 section 1 applies unchanged: the
state holds every write acknowledged before the listing started, is the
database exactly after a whole commit, and may also hold a segment that
landed without being acknowledged. Such a tip is usually built on
later. When it is overtaken instead, the next poll sees a head that
does not descend from it and emits a `rebase` (section 4).

**Batched classes** (ADR 0034). The feed reads the bucket, so it sees a
write only once its segment is uploaded:

- commits a node has captured or not yet captured, but not uploaded, do
  not exist for the feed. If the node dies they never will. The feed is
  therefore never ahead of what a restore would see; it can be behind
  what clients were told, by up to the class's loss bound;
- a position is a segment of several events. No state between two
  events of one segment can appear;
- the stamp is the segment's capture time, at or after every commit it
  holds (ADR 0034, section 14);
- a segment in flight that lands after its owner lost its hold is the
  late tip above, in a batched class as in a strict one.

The feed does not read the ownership record, so it does not print
ADR 0033's owner hint. The `rebase` is what tells a consumer that an
earlier state was not kept.

### 3. One poll

1. Take the lock of the state directory (section 6). Read the app's
   markers. While the app is `restoring` or `deleted` the poll writes
   nothing and says why.
2. **List the app's history once**, `state/<tenant>/<app>/actors/`, as
   one stream cut into a group of keys per actor, the way `backup
   create` does (the grouping moves to code both share). Keys arrive in
   order, and the state directory holds its actors in the same order,
   so the two are merged in one pass: no map of the whole app is kept
   in memory, and an actor the state knows but the listing lacks is
   seen where it would have been.
3. For each group: plan it (`Listing::from_keys`, `plan_restore`). If
   the head's position and the head object's identity (its `ObjectMeta`:
   entity tag, else size and modification time) are the ones recorded,
   and the app's incarnation is the one recorded, the actor is
   **unchanged**: no object is read. The identity is compared because a
   recreated app can hold other bytes under the same key.
4. A changed actor is read and verified as `actor export` reads it,
   from the group's plan, into a scratch file under the state directory.
   At most 16 actors are read at once (the limit `backup create` uses),
   and the listing is not advanced while all 16 are busy: an actor is
   planned only when its read can start, which ADR 0033's retry bound
   needs.
5. Its kind is decided (section 4), its app tables are appended to the
   batch being written, and the scratch file is removed.
6. The batch is committed (section 7). A poll is one batch unless it
   exceeds 1 GiB of row data or 10,000 actors, in which case it is cut
   and each part is committed on its own.

**The one exception to "one listing".** When a planned object is gone
(collected between the listing and the read) or a link is missing, the
actor's own prefix is listed again and planned again, up to 8 attempts,
exactly as ADR 0033 section 1 prescribes. An actor that still fails, or
is larger than `--max-bytes`, is reported, keeps its recorded head and
is tried again at the next poll. It never stops the other actors. A
checksum mismatch is reported as corruption and not retried in the same
poll.

**Resuming.** A stopped feed keeps no cursor into the listing. The next
poll lists the app again and compares every head with the recorded one,
so whatever changed while it was stopped, or half-way through a poll
that died, is found. That is why the unit is a whole state: a feed of
row changes would have to find every segment it missed, and collection
may have removed them.

### 4. `state`, `rebase`, `deleted`

Every emitted state has a kind. All three replace the actor's rows; the
kind says whether the states emitted before are this one's past.

Write `P` for the position last emitted for the actor, `H` for the new
head, and "the label" for the `replaced` host meta key of the replayed
database (ADR 0033 section 1), which the state directory also records.
The first rule that applies decides:

1. **The app's incarnation changed** (its `recreated` or `restored`
   marker differs from the recorded one): every listed actor is emitted
   as `rebase`, reason `incarnation`, whatever its position, and every
   recorded actor that is not listed as `deleted`, reason `absent`.
2. **The label changed and names a delete at `H`'s txid**: `deleted`,
   reason `delete`. The label stays in the database until the next
   delete or restore, so a label alone proves nothing: only a delete
   whose segment *is* the head means the actor is deleted now. A delete
   leaves the reset segment and the snapshot at it (ADR 0012), so a
   deleted actor is still listed and is found by its head moving, like
   any change.
3. **The label changed otherwise**: `rebase`, with reason `restore` for
   a single-actor restore (its replacement descends from the live head
   in the chain, but its content is a past state) and reason `delete`
   for an actor deleted and written again between two polls.
4. **No earlier state**: `state`.
5. **Descent is proven**: `state`. Either `P` is on
   `history::ancestry(listing, H)`, or that ancestry holds a position
   of `P`'s generation with a txid at or above `P`'s. The second form
   is needed because collection removes old segments. It is sound
   because a generation is one hold, a hold has one replica, and a
   replica writes its positions one after the other, each on the one
   before: two positions of one generation are on one chain. Every
   write path was checked against that
   ([decision 1](#decisions-on-the-open-questions)). A position of
   generation 0 never satisfies the second form: 0 is not a hold's
   generation.
6. **Otherwise**: `rebase`. Reason `fork` when the ancestry holds
   another position at `P`'s txid, or `H`'s txid is below `P`'s; reason
   `unproven` when collection has removed the ancestry before the
   comparison could be made.

Three cases that have no rule of their own:

- **An actor missing from a complete listing** while the incarnation is
  unchanged should not happen: a delete keeps two objects and
  collection keeps the head. The feed lists that actor's prefix once
  more, and if it is still empty emits `deleted`, reason `absent`, with
  a warning. An actor already recorded as `deleted` and still not listed
  is skipped: no listing, nothing emitted.
- **A database with no app tables and no label** (written by a node
  from before ADR 0033, or an actor that never created a table) is
  emitted as `state` with no rows. The projection is the same as for a
  delete, and the feed does not guess.
- **`unproven` is the careful answer, not an error.** A consumer that
  acted on earlier states as facts must be told when the feed cannot
  show they are still history. Polling more often than the retention
  window (`--history-retention-secs`, an hour by default) keeps the
  ancestry listed and the case rare.

The feed relies on the bucket contract the other offline readers rely
on: a strongly consistent LIST (ADR 0035). A stale listing can show an
old head, which the feed would emit as a `rebase` and correct at the
next poll.

### 5. The output directory

```text
OUT/
  _wk_feed.json                         format version, tenant, app, feed id
  _wk_states/<seq>.parquet              one row per actor emitted in batch <seq>
  tables/<class>/<table>/<seq>.parquet  that batch's rows of one class and table
  views.duckdb.sql                      the views of section 8
```

`<seq>` is the batch's **sequence number**: a counter kept in the state
directory, 20 digits, never reused. An actor appears at most once in a
batch. Positions cannot order states (a `rebase` may have a lower txid,
and generations restart when an app is recreated), so the sequence
number is the order: an actor's latest state is its row of
`_wk_states` with the highest `_wk_seq`. Table names are encoded as
actor names are on disk (`encode_segment`), so any SQLite identifier is
one safe path segment.

`_wk_states` columns: `_wk_tenant`, `_wk_app`, `_wk_class`, `_wk_name`,
`_wk_seq`, `_wk_kind` (`state`, `rebase`, `deleted`), `_wk_reason`,
`_wk_generation`, `_wk_txid`, `_wk_committed_at` (the stamp of
section 2), `_wk_tables` (the app tables of that state, so a table
that became empty or was dropped is known) and `_wk_emitted_at` (the
feed's clock).

Each data file holds the table's own columns and, beside them,
`_wk_tenant`, `_wk_app`, `_wk_name`, `_wk_seq`, `_wk_generation`,
`_wk_txid` and `_wk_committed_at`: ADR 0033's columns plus `_wk_seq`.
An app column whose name begins with `_wk_` is written as `_app` plus
its name (`_app_wk_name`), with a warning; actors with app columns beginning with `_app_wk_` (case-insensitively) are refused before batching, reported on stderr with a non-zero poll exit, and left unadvanced while unrelated actors proceed.

**Only ordinary app tables are written**: no host table, no view, no
virtual table and no shadow table of a virtual table. The feed asks
SQLite which is which (`PRAGMA table_list` on the replayed database,
type `table` only) and does not guess from names. `_wk_tables` lists
the same tables, so it is not `Verification.tables` as `actor export`
prints it: that list is every `type='table'` row of the schema, which
includes virtual tables and their shadows. What this leaves out is in
[decision 4](#decisions-on-the-open-questions).

**A committed file never changes.** Once `_wk_states/<seq>.parquet`
exists under its final name, neither it nor the data files of that
number are rewritten or removed by a poll. A consumer that loads files
as they appear should watch `_wk_states`, and load a batch's data files
when its `_wk_states` file is there.

### 6. The state directory

```text
STATE/
  lock        held (an exclusive file lock) while a feed runs
  state.db    a SQLite database
  scratch/    replayed databases being read, plaintext, removed after use
```

`state.db` is opened with `synchronous = FULL`. It holds:

- `feed`: the format version, the feed id (also in `OUT/_wk_feed.json`),
  the bucket and app it follows, the recorded incarnation, the highest
  sequence number reserved and the highest committed;
- `heads`, one row per actor, keyed by class and encoded name: the
  position, the head object's identity, the label, the kind and the
  sequence number last emitted. A row is never removed: an actor
  emitted as `deleted` keeps its row, with that kind, so that it is not
  emitted again at every poll and so that compaction knows it;
- `columns`, one row per class, table and column: the type last written
  (section 8).

SQLite is already a dependency of every crate involved, gives the
ordered scan of step 2 and makes "advance the state" one transaction.
The feed refuses to start when the bucket, the app or the feed id do
not match: a state directory belongs to one output directory. Both
directories hold actor data in the clear (the scratch files and the
Parquet files) and are to be protected like the bucket.

### 7. Crash safety: files first, then the state

A batch is committed in this order:

1. **Reserve** the next sequence number in `state.db` (one transaction).
2. **Write the data files**: each as `<seq>.parquet.partial`, fsynced,
   renamed, its directory fsynced.
3. **Write `_wk_states/<seq>.parquet`** the same way. This rename is
   the commit for readers: the views join every data row to its
   actor's latest `_wk_states` row, so data files without it are not
   seen.
4. **Advance** `state.db`: the batch's heads, its column types and the
   committed sequence number, in one transaction.

At start, and before each poll, the feed looks at every number above
the committed one:

- a number with no `_wk_states` file was never visible: its data files
  and any `.partial` file are removed;
- a number with a `_wk_states` file was visible and is true (its data
  files were complete before it appeared). It is left exactly as it is.
  Recovery restores its actor bookkeeping and widening types from the visible files with unknown identities forcing current-bucket verification, including an absent-state emission for actors removed before recovery.
  The state did not advance, so the same actors are found changed again
  and emitted under a new number. That is the "at least once" of
  ADR 0033: a state can appear twice, never half.

A crash can therefore lose work, never a state: every head the state
directory records has its rows in committed files, and every head it
does not record is read again. Losing the state directory is not
repaired in place: a new state directory with an existing output
directory is refused, and the remedy is a new output directory, built
again from the bucket
([decision 5](#decisions-on-the-open-questions)).

**Guarantees, as ADR 0033 states them**: at least once; ordered per
actor by `_wk_seq`, never across actors; positions may be skipped; no
position off a verified chain is emitted.

### 8. Schema drift and the views

Actors of one class need not have the same tables or columns (an old
actor that has not run its migration yet), and SQLite types values, not
columns.

- **Within one file**, columns are united by name, compared without
  case as SQLite compares them; the first spelling seen is kept. An
  actor without a column has NULL there.
- **A column's type in a file** is that of its values: integer, real,
  text or blob. A column with no value at all is written with its
  recorded type, or left out of the file when none is recorded.
- **Integers and reals together are a real** (Parquet `DOUBLE`), with
  no warning, when every integer of the column in that file is between
  -2^53 and 2^53, so that none changes value. This is the ordinary case,
  not an oddity: the TypeScript SDK binds `2` as an integer and `2.5` as
  a real, so a price column without a `REAL` affinity holds both.
- **Any other mix is text**: a column holding more than one type in one
  file, or integers and reals with an integer outside that range, is
  written as text, rendered as `actor sql` renders CSV, with a warning
  naming the class, table and column.
- **Across files**, the type last written is recorded in `state.db`,
  and a type only widens: integer, then real, then text. Once a column
  has been written as real, a later file of integers alone is written
  as real too (or as text, if one is outside the range); once it has
  been written as text it stays text. Any other change (a blob column
  that becomes integers) gives the column its new type from then on.
  Older files keep theirs, since committed files never change.
- **The views absorb the difference.** After a batch that changed a
  recorded type or added a column, the feed rewrites
  `OUT/views.duckdb.sql`: for each class and table, a view named
  `<class>_<table>` that lists every recorded column, casts a column
  whose files disagree to `DOUBLE` when they hold integers and reals
  only and to text otherwise, and keeps the rows whose `_wk_seq` is
  their actor's latest in `_wk_states` and whose kind is not `deleted`;
  and a view `<class>_<table>_history` without that filter. The file is
  plain SQL: the feed depends on no DuckDB code, and another engine can
  read the Parquet files with a view of its own.

A dropped table simply stops receiving rows: its old rows are not the
latest of any actor, and the view returns none of them.

### 9. Cost per poll

Derived from the request pattern, not measured (the measurement is
slice F6). With `K` the app's history objects, `c` the actors whose
head changed, and `s` the segments of a plan (at most 64 while periodic
snapshots succeed):

| | LIST | GET | PUT | DELETE |
| --- | --- | --- | --- | --- |
| one poll | ⌈K/1000⌉, + the actor's own listing per retry | 4 (markers) + Σ (1 + s) over the `c` changed actors | 0 | 0 |

- **An unchanged actor costs its share of the listing and no GET.** A
  poll of an app at rest is the listing and four GETs.
- **A changed actor costs a whole restore** (its snapshot and its
  segments), and its whole state is written again, however small the
  change. The feed keeps no replayed copy between polls: the state
  directory stays small, at the price of `1 + s` GETs where ADR 0033's
  table said "one per new segment". A later snapshot exactly at the head
  needs one extra segment GET to preserve ADR 0033's commit timestamp:
  six GETs for one changed actor with no replay segments, not five.
  A busy large actor is the worst
  case: an actor of 1 GiB that changes at every poll is read and
  written whole at every poll. `--max-bytes` and `--classes` are the
  tools for it.
- **The listing bound.** Pages of one prefix are sequential: each page
  is asked for with the last key of the one before. An app of `N`
  actors at 20 objects each is `N/50` pages one after the other: 2,000
  for 100,000 actors. A poll cannot be faster than that many page round
  trips, and polled every minute it is about 86 million LISTs a month.
  The interval should grow with the app; the guide will give the
  arithmetic.
- **Memory** is one actor's keys, the batch being written and the
  SQLite cache: it does not grow with `N`.

**Change my mind on the listing**: a poll of an app of 100,000 actors
at rest taking more than about a minute. The first answer stays
read-only: list several key ranges of the prefix at once (one stream
per class, or per range with a start key). A change index written by
the nodes is a write path change and would need its own record; bucket
event notifications tie the feed to one provider.

### 10. The dependency

One new dependency, in `wasmkeep-cli` only: the `parquet` crate of the
Apache `arrow-rs` project, Apache-2.0. The justification line for the
change that adds it: *writes Parquet, the file format DuckDB and every
warehouse read; no smaller maintained writer exists*.

- **Minimal features**: `default-features = false`, the column writer
  without the `arrow` feature (no Arrow array crates), and at most one
  compression codec. The exact feature list is fixed in slice F1 and
  written in [docs/legal.md](../legal.md).
- **The licence check is the gate, not this text**: `cargo deny check`
  against `deny.toml`'s list, `scripts/licenses.sh`,
  `scripts/notices.sh` and the SBOM must pass with the tree the chosen
  features bring. If a feature brings a licence outside the list, the
  feature goes; the list is not widened for the feed.
- The runtime, storage, cluster and server crates gain nothing. The
  `wasmkeep` binary, which is also the node, grows by the writer: the
  feed is always built, behind no cargo feature
  ([decision 2](#decisions-on-the-open-questions)).

## Consequences

- SQL across an app's actors, with DuckDB or anything that reads
  Parquet, from a directory a timer keeps current.
- The output grows with every change: a state is appended, never
  edited, so its size follows how often actors change times how large
  they are, not how much changed. Slice F7 (compaction) bounds it.
- A deleted actor's earlier rows stay in older files until compaction.
  The views hide them; the files still hold them. Until F7 the way to
  honour an erasure in the output is to remove both directories and
  build again. After compaction its rows are gone and its name stays,
  in one `deleted` row of `_wk_states`
  ([decision 3](#decisions-on-the-open-questions)). For a deleted actor
  the bucket's keys hold that name too; for an actor of an earlier
  incarnation of the app (reason `absent`) the output is then the only
  place that does, until a new output directory is built.
- A full-text table that holds its own text (FTS5 without an external
  content table) is not in the feed: neither the virtual table nor its
  shadows are written.
- Lag is at least the poll interval, plus the listing, plus a restore
  per changed actor.
- One more reader of the bucket, with a request bill the operator
  chooses through `--every`.
- With [several apps per process](0038-several-apps-per-process.md)
  nothing changes: a feed follows one app, and each app has its own.

## Alternatives

- **Row-level changes from LTX pages.** Pages do not name rows, and a
  feed that stopped longer than the retention window could not resume.
- **A change index written by the nodes.** Removes the listing, but it
  is a write on the owner's path and a new object family to collect.
  Kept as the second answer to the "change my mind" condition.
- **Progress kept in the bucket.** Breaks the read-only rule, and a
  read-only credential could no longer run the feed.
- **A cached replay per actor**, applying only new segments: fewer GETs,
  but the state directory becomes a second copy of the app, and a
  `rebase` still needs a full read.
- **Writing DuckDB files directly**: a large native dependency, one
  writer per file, and one engine. Parquet files are read by all of
  them.
- **Rewriting an actor's rows in place**: Parquet files are immutable;
  it would be compaction at every poll.

## Tests (to build)

Against the in-memory object store with the counting wrapper the server
already has (`counted_store.rs`), so request counts are asserted, and
under `./scripts/check.sh --s3` for the bucket-gated run.

- **Projection equals a known workload**: a scripted workload over
  several classes, with its expected final rows computed apart; after
  the feed catches up, each view returns exactly those rows. A second
  poll with no change makes no GET but the four markers and commits no
  batch.
- **Stop and resume mid-poll**: the feed is killed at each step of
  section 7 (after a data file, after every data file, after the
  `_wk_states` rename, before the state transaction) and half-way
  through the listing. After the next poll the projection is the
  expected one, every file committed before the kill is byte-identical,
  and no `.partial` or orphan file is left.
- **A delete**: the actor's rows leave the view, `_wk_states` holds
  `deleted`; the same actor written again appears as `rebase`, reason
  `delete`, when both happened between two polls, and a database that
  still carries an old delete label is not reported deleted. The same
  in a batched class, with commits pending when the delete is asked:
  the label's txid must be the reset's. A poll after the delete emits
  nothing more for that actor.
- **A restore**: `actor restore` to a past position appears as `rebase`,
  reason `restore`, and the view holds the past rows.
- **A late tip overtaken**: a listing built with a tip of an earlier
  generation, then with the chain that replaced it: `rebase`, reason
  `fork`. With the ancestry collected: `unproven`. With the same owner
  continuing past a collected snapshot: `state`.
- **A new incarnation**: an app deleted and recreated with the same
  keys and other bytes, and an app restored in place: every actor is a
  `rebase`, missing ones are `deleted`; a poll during `restoring`
  writes nothing.
- **A schema change**: a column added by some actors only, a column
  whose values change type within one poll and between two polls, a
  dropped table, a column named `_wk_name`; integers and reals in one
  column (a `DOUBLE`, no warning), then with an integer above 2^53
  (text, a warning), then a file of integers alone after a real was
  written (still a `DOUBLE`). An actor with an FTS5 table and an R-tree:
  neither they nor their shadow tables are written or listed in
  `_wk_tables`, and its ordinary tables are. The generated views load in
  DuckDB and return the expected rows; this test runs when the DuckDB
  command-line tool is installed and is reported as not run otherwise.
- **A batched class**: with a segment in flight, the feed shows the
  acknowledged state; after the node is killed, it never shows the lost
  commits.
- **Collected while read**: an object of the plan removed between the
  listing and the read costs one listing of that actor and succeeds;
  an actor that keeps failing keeps its old head and the poll exits 2.
- **Read-only**: every test runs through a store wrapper that fails on
  any write or delete.

## Docs to change when accepted

A change-feed guide with the DuckDB steps and the interval arithmetic,
linked from `docs/SUMMARY.md`; the actor data access guide; the CLI
reference; `docs/legal.md` and the notices; ADR 0033's sections 7 and 8;
`ROADMAP.md`.

## Decisions on the open questions

The six questions this record was proposed with, decided on 2026-11-17
against the code at `0465468`.

### 1. Same-generation descent holds on every write path

Rule 5's second form stays. What it needs: all the positions one
generation ever wrote are on one chain. What the code gives:

- **A generation is one hold.** Every acquisition writes the ownership
  record at generation + 1, a node taking its own actor back included
  (ADR 0020), and the first record of an actor is seeded above every
  generation its keys carry. Every give-up (`ActorState::abandon`, a
  failed or interrupted upload) drops the hold, and `Replica::restore`
  runs once per hold: a replica never restores twice in one generation.
- **A replica writes one line.** Each write is at `(its generation,
  head txid + 1)` with the head as parent, the key is created, never
  overwritten (`PutMode::Create`: other bytes at the key are a conflict
  that stops the replica), and the head advances only on an
  acknowledgement. An upload that fails or is cut leaves `failed` set,
  and nothing more is written under that generation. So the last
  position of a generation may be an unacknowledged tip, and nothing
  of the same generation is beside it.

| Path | What it writes | On the generation's line |
| --- | --- | --- |
| Plain and durable writes (`Replica::ship`) | a segment at head + 1 | yes |
| Batched writes (ADR 0034, the ticket of `replica/batched.rs`) | a segment at head + 1, one in flight at a time; a ticket dropped before its capture uploaded nothing, and its txid is used once | yes |
| Delete: `delete-self`, the durable one, the operator's (`ship_reset`) | a reset segment at head + 1 | yes; the label decides the kind first (rules 2 and 3) |
| Single-actor restore (`ship_reset` of the replacement) | a segment at head + 1, by the holder | yes; rule 3 answers `rebase` before descent is asked |
| First snapshot (empty history) | a snapshot at head + 1, no segment | yes |
| Periodic snapshot, and the one after a delete or a restore | a snapshot **at the head**, keyed by the head's position, which may be of an earlier generation | no new position: the same database under a second name |
| Recorded head on hand-over (ADR 0035) | nothing under `actors/`: a field of the ownership record, and a floor a restore must see | not a write path |
| `cluster migrate` (ADR 0022: offline, once, the layout from before 0.4.0) | copies of history objects under the app's root, bytes and key tails unchanged | no new position |
| The ownership format setting (`app_format.rs`) | nothing under `actors/` | not a write path |

So "one writer per generation" is true of positions, not of objects: a
snapshot named after an earlier generation's position can be written by
a later holder. The feed compares positions, and that snapshot is only
ever the database at the position it names
(`Replica::refuse_unless_at_head`).

**Where it does not hold, and what the feed does:**

- **A new incarnation of the app** (`app delete` then a recreation,
  `backup restore` into an empty root or in place). Generations start
  again or history keys come back with other bytes, so a position can
  name two databases. Both write a marker (`recreated`, `restored`),
  and rule 1 answers before any position is compared: every actor is a
  `rebase`. Descent is never asked across incarnations.
- **Generation 0.** `ActorState` builds its replica with
  `hold.generation().unwrap_or(0)`: a hold lost between the acquire and
  the restore gives a replica of generation 0. F4 checked the complete
  path: `Replica::restore` calls `LeaseView::verify` before returning,
  and `hold_cell::verify` refuses an empty hold cell. Restore itself
  does not upload. This replica therefore cannot reach a first upload;
  the feed sees no object from it. The upload's first attempt does run
  before acknowledgement checks validity, but only for a replica that
  already passed restore. The conservative feed rule stays: generation
  0 cannot establish same-generation descent. F4's node-produced LTX
  fixtures under zero-generation keys verify `unproven` after collection.
- **A delete whose label is behind.** F4 found no path for a batched
  acknowledgement to advance the head between label construction and
  reset. `delete_recorded` first flushes to `Barrier::Flushed` (no
  pending replica work or database boundaries), then ends capture,
  before calling `replicate_delete`. The durable host call also flushes
  first and holds the replica lock across label construction and
  `ship_reset`. F4 tests plain, durable and operator deletes with a
  batched upload pending: the label's txid equals the reset head, and
  the feed emits `deleted`. Rule 2 therefore keeps its exact-txid
  comparison; no server change or relaxed delete inference is needed.

**Revisit if** a write path is added that writes under `actors/` without
holding the actor (a repair tool, a node-written change index), or the
ownership record stops raising the generation at every acquisition.

### 2. Always built, no cargo feature

The feed is part of every `wasmkeep` binary.

- The CLI has no cargo feature today, and `scripts/check.sh`,
  `cargo deny`, the licence and notice scripts, `scripts/sbom.sh` and
  the release workflow all describe one tree. A feature makes two: two
  SBOMs and notice files, or one that does not match the binary an
  operator built without it.
- A released binary that may or may not have `feed` is a support
  question at every report.
- What F1 added is small: `parquet` with no optional feature (no Arrow,
  no codec), seven crates in the lock file, all inside the accepted
  licences, in a release binary of about 42 MB that Wasmtime dominates.

The size is to be measured once, when the command is exposed: the
release binary before and after, from release artifacts or one local
build each, written in `docs/legal.md`.

**Revisit if** that measure is above 2 MB, or a later target (`duckdb`,
`postgres`) needs a native library or a large tree: that target would
be a feature, the Parquet writer still would not.

### 3. Compaction keeps a deleted actor's `deleted` row, and no data row

The compacted batch holds **one `_wk_states` row for every actor in
`heads`**, and data rows for the live ones only. For a deleted actor
that is its `deleted` row: name, position, kind, reason.

- For reason `delete` it is what building again from the bucket would
  give: a deleted actor is still listed (its reset and the snapshot at
  it stay for good, ADR 0028), and a new feed emits it as `deleted`.
- For reason `absent` (an actor of an earlier incarnation, gone from
  the bucket) a new build would not know the actor at all. Its row is
  kept all the same: one rule, and the consumer below needs it most
  there, since a whole app's actors went at once.
- A consumer that loads files as they appear (section 5) and was behind
  when compaction ran still learns of the delete. With the row dropped
  it would keep the actor's rows for ever, which breaks "at least once".
- No time to configure, nothing to expire.

What stays is the actor's name. The bucket's keys hold it anyway for
reason `delete`; for reason `absent` the output alone does, and the way
to remove it is a new output directory, which an operator who deleted
an app to erase it should build in any case. F7's proof becomes "no
*data* row of a deleted actor remains".

**Revisit if** names of deleted actors must leave the output without a
new build: that needs a retention time for `deleted` rows, and
consumers that load within it.

### 4. Virtual tables and their shadow tables are left out

Neither is written, and neither is listed in `_wk_tables`.

- **Today's readers do not separate them.** `readonly::verify` lists
  every `type='table'` row of the schema that is not a host or `sqlite_`
  table: a virtual table and each of its shadows are in
  `Verification.tables`, `actor export` keeps them in the file, and
  `actor sql` can read both. Guests may create FTS3, FTS4, FTS5 and
  R-tree tables (`ALLOWED_MODULES`), so they exist in real actors.
- **Shadow tables are an index's private pages**: blobs in a layout
  SQLite does not promise, often larger than the data, rewritten at
  every change. As Parquet they cost and answer nothing.
- **By type, not by name**: `PRAGMA table_list` gives `table`,
  `virtual` or `shadow` (checked on the bundled SQLite with FTS5 and
  R-tree). A name pattern would drop an app's ordinary table that
  happens to be called `notes_data`.

The cost: text held only by a full-text table (FTS5 without an external
content table) is not in the feed. The usual layout, an ordinary table
indexed by an external-content FTS table, loses nothing.

**Revisit if** someone needs that text: write the virtual table's own
rows (`SELECT *` through the module), never its shadows.

### 5. A lost state directory is not recovered from `_wk_states`

The refusal of section 7 stays: a new state directory with an existing
output directory does not start, and the remedy is a new output
directory.

- `_wk_states` does not hold what `heads` holds: the head object's
  identity, the label and the app's incarnation are not in the output,
  and the recorded column types would have to be read back from every
  file.
- Without them no actor can be called unchanged and no descent can be
  claimed. A recovered feed would read every actor again and emit each
  as `rebase`, reason `unproven`: the bucket cost of a new build, with
  an output directory twice the size.
- The output is disposable by design (section 1).

**Revisit if** F6 shows a build of a large app is too long to repeat.
The cheap form is then an explicit `--adopt`: a state seeded with the
next sequence number alone, every actor emitted once as `unproven`.

### 6. Integers and reals in one column are a `DOUBLE`

As section 8 now says: a `DOUBLE` when every integer is within ±2^53,
text otherwise, and real is kept across files as text is.

- Text was the wrong default for the common case. From TypeScript a
  whole number is bound as an integer and any other as a real, so any
  amount, average or coordinate in a column without `REAL` affinity is
  mixed. As text, `sum()` needs a cast, the column never becomes
  numeric again, and every poll warns.
- Nothing is lost in the files: the range check keeps every integer
  exact, and a column that fails it falls to text, which is lossless.
- One place rounds: the view casts older integer-only files of a column
  that later became real, and an integer above 2^53 in such a file is
  rounded **by the view**, not in the file. `<class>_<table>_history`
  read file by file keeps the integers.

**Revisit if** an app needs exact 64-bit integers beside reals in one
column: Parquet `DECIMAL`, decided then.

### What these change in the slices

- **F1 (built, `d75d2a1`)**: its writer turns any mixed column into
  text. The integer-and-real rule and "real is kept" are a change to
  `feed/writer.rs` and its tests, to make with F2 or right after it.
- **F2**: choose the tables with `PRAGMA table_list`, not with
  `Verification.tables`; `_wk_tables` is that choice. Nothing else.
- **F3**: unchanged.
- **F4**: `heads` keeps deleted actors, and one still absent is
  skipped without a listing; generation 0 proves no descent; the
  batched delete test of the label's txid.
- **F5**: the views cast integer and real files to `DOUBLE`.
- **F7**: one `_wk_states` row per actor in `heads`.

## Plan

Each slice is one change; each says what it proves.

- **F1. The writer and the dependency.** `parquet` with the minimal
  features, the type rules of section 8 within one file, the
  write-fsync-rename of a file. Proves that files read back equal what
  was written, and that `cargo deny check`, the licence and notice
  scripts and the SBOM pass. Built before decision 6: integers and
  reals as a `DOUBLE` is a follow-up to its writer.
- **F2. One poll.** The shared actor grouping, `state.db`, unchanged
  heads skipped, `state` rows, `--classes`, `--max-bytes`, the lock,
  the tables chosen with `PRAGMA table_list` (decision 4). Proves the
  projection of a known workload, the request counts of section 9 and
  the read-only wrapper. The binary's size before and after is
  recorded when the command is exposed (decision 2).
- **F3. Crash safety.** Sequence numbers, the commit order, recovery at
  start, batches. Proves stop and resume at every step.
- **F4. Kinds.** The label, descent (never from generation 0),
  incarnations, the absent actor, a `heads` row kept for a deleted
  actor. Proves the delete (in a batched class too), restore, late tip,
  incarnation and batched-class cases. **One adversarial review after
  F4** covers F2 to F4: the feed writes nothing to a cluster, but its
  claims about history are claims about replication.
- **F5. Drift across files and the views.** Recorded types, the
  `DOUBLE` cast of integer and real files, `views.duckdb.sql`,
  `--every`, the guide. Proves the schema-change case in DuckDB.
- **F6. The listing measured.** A synthetic app of 100,000 actors, a
  poll at rest and with 1 % changed, written to `bench/results/`.
  Decides the "change my mind" condition with a number.
- **F7. Compaction.** `wasmkeep feed compact`: under the feed's lock,
  one new batch holding one `_wk_states` row per actor in `heads` and
  each live actor's latest rows copied from the existing files (no
  bucket read), then the older files removed. Proves the views return
  the same rows before and after, that no data row of a deleted actor
  remains, and that its `deleted` row does (decision 3).

  Implemented on the development branch as `wasmkeep feed compact --out OUT
  --state STATE`. Reservation also records a compaction journal in `state.db`.
  The new `_wk_states` rename is the switch, after durable data files, as in
  section 7. Recovery under the lock rolls back an unpublished batch, or
  advances the compacted heads, regenerates views using the new batch and
  completes removal of older files. The journal is cleared only after durable
  removal. Sequence numbers are never reused. Empty historical table schemas
  survive without data rows, so the latest views remain queryable; their
  `_wk_seq` changes to the compacted batch, and `_history` loses old states.
