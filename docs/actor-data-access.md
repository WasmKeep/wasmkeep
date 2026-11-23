# Exporting and querying an actor

`wasmkeep actor export` and `wasmkeep actor sql` read one actor's history
straight from the bucket. They acquire no hold, contact no node, write no
bucket objects and run no guest code. Nodes can continue serving the actor.

```bash
wasmkeep actor export acme/shop cart/u-1842 --bucket s3://my-bucket/prefix -o cart.db
wasmkeep actor export acme/shop cart/u-1842 --bucket s3://my-bucket/prefix --at -15m -o earlier.db
wasmkeep actor export acme/shop cart/u-1842 --bucket s3://my-bucket/prefix --txid 4123 -o tx4123.db
wasmkeep actor sql acme/shop cart/u-1842 'SELECT sku, qty FROM items ORDER BY sku' \
  --bucket s3://my-bucket/prefix --format json
```

Bucket endpoint, region and credentials use the same `AWS_*` environment
variables as other bucket commands. Grant LIST/GET on the app's
`state/TENANT/APP/actors/` prefix. GET on its `owners/` prefix is optional
and supplies the ownership-generation hint. Credentials are never printed.
The output file contains plaintext application data and internal
`_wasmkeep_*` tables, whose layout may change between releases. Protect it
like the bucket. On Unix it is created with permissions `0600`.

## Which state is read

The head export contains every **acknowledged durable write** from before
its listing, subject to subsequent application updates or deletes. Ordinary
write responses are not durability acknowledgements. The exported state is
one complete segment boundary; it is not a locked, linearizable snapshot.
A segment may contain several events under batching.

The newest segment may also contain **unacknowledged writes**. A late upload
can be overtaken by later history. The commands report the selected
generation/txid, LTX capture time, listed head, and ownership-generation
hint. A different or unavailable owner generation prints a warning; a
matching generation still does not prove acknowledgement. Add
`--require-owner-match` to either command to refuse a mismatched or unavailable
owner generation with exit code **3**, before publishing an export or emitting
SQL results. Other runtime failures exit 1; argument errors exit 2. This checks
the listed head even for historical selections; it does not prove durability.

`--at` takes an RFC3339 timestamp with timezone or a relative duration such
as `-15m`, `-2h`, `-1d` (also seconds). Relative durations use the operator's
clock. Absolute and relative times select the longest retained prefix of
the **current ancestry** whose LTX capture stamps are all at or before that
time. A clock moving backwards never allows skipping an earlier segment.
Periodic snapshots are dated by their matching segment, not by the snapshot
creation time or the bucket's upload time. The first upload is a snapshot
at txid 1 with no segment; its own stamp dates that initial position.
A retained snapshot without its matching segment (except txid 1) cannot date
that position: `--at` skips it and considers the following dated segments.
If timestamp selection exceeds its read deadline, use `--txid` to avoid the
capture-time scan. `--txid` selects an exact boundary on that same ancestry. The flags conflict.

A point older than retained history is refused with the available txid range.
Collection limits how far back you can read; a delete erases prior history.
An actor that stopped writing no longer keeps more than that: one retention
window after its last write and hibernation, the node that hibernated it
compacts its history to the snapshot at its head and the segment there
([ADR 0042](adr/0042-compacting-an-idle-actors-history.md), see "History in
the bucket" in [the cluster guide](cluster.md)). Points within the last
window stay readable; older ones of an idle actor go, as they do for an
actor that keeps writing.
Exporting a deleted head gives an empty application database, with host
metadata. Older writers do not label deletes, so an empty database is
reported as **probably deleted**. A reset followed by new writes contains
only that new incarnation's application state. Erased points require a backup.

## Verification and files

Every replay verifies LTX trailers, the pre/post-apply checksum chain and
an independent checksum of the final raw file. SQLite's `integrity_check`
must pass; foreign-key violation counts are reported without failing the
export. The private copy is then converted to `journal_mode=DELETE`, fsynced
and published without replacing an existing file. It needs no WAL sidecar.
The reported checksum describes the **raw replay**, before journal conversion.
Scratch files are removed on errors, Ctrl-C and SIGTERM (exit 1). SIGKILL,
process crashes or power loss can leave scratch files for manual removal.
Existing destinations are always refused. Publication normally uses a hard
link; filesystems that reject links use exclusive create, copy and fsync with
Unix mode `0600`. This fallback exposes an incomplete destination during the
copy; ordinary copy errors remove it, but a crash can leave a partial file.

Both commands accept `--tmpdir DIR` for scratch placement. Export defaults to
the output parent; SQL defaults to the system temporary directory. Scratch
copies are **plaintext**, including internal tables, even if the bucket is
encrypted. Use a trusted directory or encrypted filesystem. Budget for the
largest replayed database plus SQLite journal/sidecar space; copying across
filesystems or using the fallback also needs a full database at the destination
(and two copies on a shared filesystem). Free-space checks use decoded LTX
sizes before applying batches and the final size before fallback copying where
filesystem statistics are available. These are advisory: quotas and concurrent
writers can still cause disk-full errors.

`--max-bytes` bounds the replayed database (default 2 GiB, twice the server's
default cap). Reads are bounded and use at most eight objects / 32 MiB of
read-ahead, with one larger object permitted by itself. Missing objects or
links and stale listings cause up to eight fresh attempts. Corruption fails
immediately. Keep `--history-retention-secs` above twice the expected read
deadline plus clock skew; the default hour covers the default server cap.
A very short window can cause repeated collection races and a clear failure.

Each invocation lists the actor's prefix, downloads its snapshot and replay
segments, and reads capture-time evidence. Historical time selection may read
older retained segments too, and read selected segments again during replay.
One optional GET reads the ownership hint. There is no SQL cache in this slice.

## Read-only SQL and formats

SQL first makes a verified temporary export, then opens it read-only and
immutable with `query_only`. Its authorizer denies writes, ATTACH, extension
loading, host tables, schema tables, and PRAGMAs (including table-valued
PRAGMAs). Exactly one statement returning rows is allowed; trailing
statements are errors. No guest component is loaded. The temporary directory
is removed after output, including on query/output errors. Hidden-table names
may still be visible through eponymous virtual tables; hiding is a convenience,
**not a security boundary**.

Queries are limited to 10,000 rows, 16 MiB of retained results, 1 MiB per
value/SQL statement and 256 columns. Execution stops after five seconds or
ten million SQLite VM instructions. Exceeding a limit fails with a nonzero
exit code; add `LIMIT` or narrow the query. Values over 1 MiB fail with an error;
they are not truncated. SQL errors also exit nonzero.

`--format` accepts:

- `table`: aligned columns, escaped controls and cells limited to 80 characters;
- `csv`: RFC 4180 quoting, header row and CRLF line endings;
- `tsv`: header row; backslashes, tabs, newlines and carriage returns escaped;
- `json`: an array of objects;
- `jsonl`: one object per line.

The default is `table` on a terminal and `csv` otherwise. Duplicate column
names receive unique `_2`, `_3` suffixes. NULL is `null` in JSON and an empty
field otherwise. BLOBs are base64 in JSON and `X'hex'` otherwise. JSON
integers beyond 2^53 are strings to preserve their digits; non-finite REALs
become `null`. Status and verification messages go to stderr, leaving SQL
stdout suitable for piping.

This implements the export/query slice of [ADR 0033](adr/0033-data-access-export-and-online-backup.md).
History listing, online backup, and live actor restore are also available.
Caching and application-only/raw export options remain future work.

## Restore one actor while its cluster runs

`wasmkeep actor restore --app acme/shop cart/u-1842 --bucket s3://live/prod
--txid 4123` stages verified historical state and asks the current owner to
replace the actor in its mailbox. Use `--at -15m` instead of `--txid`, or
`--from s3://archive/prod --run RUN` for a backup run. Backup credentials use
the same AWS environment as the live bucket. `--if-head GEN:TXID` refuses a
changed head; `--no-wait` returns the request id immediately, and `--wait`
defaults to 60 seconds (maximum 3000).

The replacement is a **new write**, at the owner's current generation and a
new transaction id. It does not rewind or edit history. A batched owner flushes
first. Earlier history stays subject to the ordinary retention window, so an
undo needs a retained position or a backup. An app restore fence or tombstone
blocks the job. A request predating a later delete cannot resurrect that data;
an intentional undo of a delete needs a new request, usually from a backup.

Validation, the initial `--if-head` comparison and restore fences run before
any socket disruption. Only once a validated replacement starts draining do
sockets close with 1012 and upgrades receive 503 (`Retry-After: 1`). Before
that, normal handshake validation applies, including 400 for an incomplete
upgrade. `--if-head` is checked again after draining and across retries: any
head movement, including close-callback writes, is a conflict and preserves
the current data, even though sockets may already be closed. Without
`--if-head`, the replacement still replaces whatever is there.
Restored alarms get fresh index entries before the upload
and can fire again. Effects outside the actor's database are not undone.
The staged database must have the current database's page size and fit the
node's database limit. Requests expire after 50 minutes by the bucket's clock. After a delete reset
has been collected, restore requires history retention of at least 3003 seconds
with the default clock skew; the default 3600 seconds clears this bound.
Shorter retention refuses restoration when the reset is missing.
Only the `.done` outcome acknowledges restoration; the request disappearing
alone does not. Right after `.done`, the actor can answer 503 with
`Retry-After: 1` for a moment, while the owner ships the replacement's
snapshot and ends its previous hold of the actor; clients retry it like any
other 503. During a rolling upgrade, older nodes do not process restore
requests or preserve their markers through deletes: finish upgrading before
using this command.

**Rows, not files.** An actor's [files](files.md) are not in its
database: `actor restore` leaves them as they are now. A restored row
may name a file deleted since (check it with `head` before trusting
it), and a restore to a point before a delete brings the old rows back
in a new, empty file namespace. The files of a past moment come from a
backup of the whole app, restored into a prefix of its own
([files and assets in backups](deploy/backup-and-restore.md#files-and-assets)).

Restore requests print an operation ID before waiting. Use `--resume ID`
instead of selecting a source again after interruption; see
[operation recovery](operations.md).
