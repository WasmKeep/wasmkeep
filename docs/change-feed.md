# Querying the change feed with DuckDB

The change feed reads one app's bucket history and writes whole actor states
as local Parquet files. It contacts no node and writes nothing to the bucket.
This guide describes the implementation on the ADR 0039 development branch;
continuous polling (`--every`) is not implemented yet.

## Run a poll

```sh
wasmkeep feed acme/shop --bucket s3://my-bucket/prefix \
  --to parquet --out shop-feed --state shop-feed-state
```

Keep both directories between invocations. They must be separate, and a state
directory belongs to exactly one output directory and bucket/app. Protect both
like the bucket: they contain plaintext actor data. If progress is lost, build
into two new directories; an existing output cannot be adopted.

The command exits 0 when all changed actors were written, or 2 when some were
left for another poll (also while the app is restoring or deleted). Errors are
reported on stderr. `--classes carts,orders` limits classes; `--max-bytes N`
limits the size of an actor read. Repeat the command with a scheduler, without
overlapping invocations: the state directory is locked during a poll.

The shared backup/feed history grouper allows **32 MiB (33,554,432 bytes) per
actor** of retained listing allocations: full key, ETag and version string
capacities and metadata vector slots (including spare capacity). Object bodies
are separate. Tests cover 15,000 objects and 73,126 objects: 20 writes/s for the
default one-hour retention, plus snapshots every 64 segments and an initial
snapshot. Long keys or provider metadata reach the byte bound sooner; it is not
a fixed object-count limit. Grouping accumulates one actor plus one lookahead
object; conversion to the consumer's ordered map adds tail keys and tree
allocations, and the feed can retain up to 16 yielded groups during concurrent
processing, so this is not a process RSS cap. An oversized actor is reported
with its prefix, total object count, limit and recovery instructions, left
unadvanced, and other actors can proceed. Reduce its write rate or configured
history retention, let collection run, then retry. Shorter retention reduces the
recovery window. `actor export` and `actor history` use a separate listing path;
the inspector retains its separate 10,000-object listing cap.

The interval is a cost and lag choice. Each poll lists the app's history once:
with K objects and 1,000 objects per page, that is approximately ceil(K/1,000)
LIST requests, plus retries. For 100,000 actors with 20 objects each, that is
2,000 pages per poll, about 86.4 million LISTs in 30 days at one poll per minute.
Changed actors also cost a full restore; unchanged actors need no history GETs.
There are four marker GETs per poll. Lag includes the scheduling interval,
listing and restore time. Measured at 100,000 actors with 2 history objects
each, a poll at rest takes about 51 s, and the listing grows with history: at 20
objects per actor, expect minutes per poll
([benchmarks](benchmarks.md#the-change-feed-at-100000-actors)), so poll a large
app less often.

## Load the generated views

Install DuckDB separately; WasmKeep does not include it. After a successful poll:

```sh
duckdb shop.duckdb
```

In DuckDB:

```sql
.read shop-feed/views.duckdb.sql
SHOW TABLES;
SELECT _wk_name, * EXCLUDE (_wk_name) FROM carts_items;
SELECT _wk_name, count(*) AS item_count
FROM carts_items GROUP BY _wk_name ORDER BY _wk_name;
```

Replace `carts_items` with your class and table (`<class>_<table>`). Quote
identifiers containing spaces or punctuation with double quotes. Ambiguous
view names (for example `a_b`/`c` and `a`/`b_c`, or a table named `items_history`
beside `items`) are reported rather than silently overwriting another view.

Reload `.read shop-feed/views.duckdb.sql` after **each poll or compaction**, including one
that recovers interrupted work. The script uses absolute file paths and an
atomic transaction to replace the views. It enumerates committed files; an
already loaded view does not discover new table files. Moving the directories
requires a new feed, since progress binds the original output path.

Each latest-state view has the app columns and `_wk_tenant`, `_wk_app`,
`_wk_name`, `_wk_seq`, `_wk_generation`, `_wk_txid`, `_wk_committed_at`.
Columns are united by SQLite's case-insensitive naming rules; missing columns
are NULL. Columns with only NULL values and no recorded type are omitted until
a type is known. Integer/real files are cast to DOUBLE; other conflicting types
are cast to VARCHAR. Blob-to-text conversion uses `X'hex'`, as the writer does.
The writer keeps real and text types sticky. Older files remain unchanged.
Older integers outside ±2^53 can round in a DOUBLE view; inspect their original
Parquet files individually when exact historical integers matter.

App columns beginning with `_wk_` become `_app_wk_` columns, with a warning.
An actor containing any `_app_wk_` column (case-insensitively) is refused and
left unadvanced; other actors can still proceed. Host tables, views, virtual
tables and their shadow tables are excluded, including self-contained FTS text.

## Latest state, history and replacement

```sql
SELECT * FROM carts_items_history ORDER BY _wk_name, _wk_seq;
SELECT _wk_class, _wk_name, _wk_seq, _wk_kind, _wk_reason
FROM read_parquet('shop-feed/_wk_states/*.parquet', union_by_name = true)
ORDER BY _wk_class, _wk_name, _wk_seq;
```

`carts_items` selects rows matching each actor's greatest `_wk_seq` in
`_wk_states`, excluding `deleted`. Empty or dropped tables therefore lose their
old rows. Sequence numbers order emitted states per actor; generation/txid and
timestamps are not substitutes. There is no consistent instant across actors.

Every emission replaces the actor's **whole state**. `state` continues known
history; `rebase` replaces it when a restore, incarnation change, fork or
unproven ancestry means earlier emissions cannot be treated as its past.
A lower txid after rebase can still be the latest state. `deleted` removes the
actor's rows from latest-state queries. Neither rebase nor deletion subtracts
individual rows from a historical aggregate automatically.

Delivery is at least once: recovery may emit the same state under another
sequence. The latest view selects one emission, while `_history` includes all
committed emissions, including duplicates and pre-rebase/pre-delete rows.
Do not sum `_history` as though it were a stream of row changes. A custom
consumer should watch `_wk_states` as the commit record, then replace each
actor's rows, including when its new state has no data rows.

## Compact the local files

```sh
wasmkeep feed compact --out shop-feed --state shop-feed-state
```

Compaction reads only the existing local files, without bucket credentials or
requests. It takes the same exclusive lock as a poll and fails if another feed
operation holds it. It writes one new batch containing each recorded actor's
latest state, then removes superseded Parquet files. Deleted actors retain one
`deleted` state row (including their name, position and reason), and no data
rows. Empty and dropped table schemas remain available as empty views.

The new batch uses a fresh, monotonically increasing sequence; positions,
capture times and kinds are retained. Latest-state query results are unchanged
apart from `_wk_seq`; `_history` now contains only the retained states. Reload
`views.duckdb.sql` after compaction, and coordinate readers using old file paths
with it: those files are removed. A file-loading consumer must treat the new
batch as whole-state replacements, just as for a poll.

New files are fsynced before the `_wk_states` rename makes the batch visible.
A durable local journal distinguishes compaction from a poll. If interrupted,
the next poll or compaction, under the lock, removes unpublished work or
finishes the published switch and cleanup. Reserved numbers are never reused;
committed files are never rewritten in place. Keep enough free space for both
the old files and the new batch until publication completes.

Old data remains on disk after deletion until compaction. Compaction removes
that data from the feed's files; it retains deleted actor names as described
above. Removing names requires building into new directories.
The feed is an analytical projection, not a backup or a complete event log:
heads between polls may be skipped, and batched writes appear only after upload.
