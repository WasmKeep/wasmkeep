# Querying the change feed with DuckDB

The change feed reads one app's bucket history and writes whole actor states
as local Parquet files. It contacts no node and writes nothing to the bucket.
This guide describes the implementation on the ADR 0039 development branch;
continuous polling and compaction are not implemented yet.

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

The interval is a cost and lag choice. Each poll lists the app's history once:
with K objects and 1,000 objects per page, that is approximately ceil(K/1,000)
LIST requests, plus retries. For 100,000 actors with 20 objects each, that is
2,000 pages per poll, about 86.4 million LISTs in 30 days at one poll per minute.
Changed actors also cost a full restore; unchanged actors need no history GETs.
There are four marker GETs per poll. Lag includes the scheduling interval,
listing and restore time. These are estimates, not F6 measurements.

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

Reload `.read shop-feed/views.duckdb.sql` after **each poll**, including one
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

Old data remains on disk after deletion. Until compaction is implemented,
erasure from feed files requires removing both directories and building again.
The feed is an analytical projection, not a backup or a complete event log:
heads between polls may be skipped, and batched writes appear only after upload.
