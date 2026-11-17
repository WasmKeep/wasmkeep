# Back up and restore a cluster's bucket

The bucket is the cluster's only durable copy of everything: a node's
disk is a cache of it ([replication](../cluster.md#replication)). This
page says what is in the bucket, how to copy it so that the copy can be
restored, how to restore it, and how to check that it works.

## Online archives

Use `backup create` to copy a running app, then `backup verify` to check
what the destination actually holds:

```bash
wasmkeep backup create --bucket s3://live/prod --app acme/shop \
  --to s3://archive/prod --to-env backup.env --history head --cache backup-cache
wasmkeep backup list --from s3://archive/prod
# Run with the archive account's AWS_* environment:
wasmkeep backup verify --from s3://archive/prod
```

`backup.env` contains destination `AWS_*` settings as `NAME=value`, one per
line. Bucket overrides (`AWS_BUCKET`, `AWS_BUCKET_NAME`, `bucket`, and
`bucket_name`) are rejected in both source and destination settings. The CLI
bucket is applied after configuration; overlapping prefixes with the same
bucket name are refused unless both endpoints identify distinct, well-known AWS
partitions (AWS and AWS China). Source credentials need only LIST and GET; no lease, owner request,
source write or running-node connection is made. The destination needs
LIST, GET and PUT. Use a separate bucket and account. Keep credentials,
archives and the local plaintext cache private. `--all` discovers deployed
apps instead of requiring `--app`. Legacy app layouts must be migrated first.

Each copied actor is replayed with LTX trailer and chain checksums, an
independent final database checksum, and SQLite `integrity_check`.
Foreign-key violations are reported rather than treated as storage corruption.
A copied head includes writes acknowledged before that actor's listing;
it may include unacknowledged tips. **There is no consistent point across
actors.** Reconcile cross-actor business invariants after restoring.

Online create refuses all virtual-hosted-style settings (including an explicit
false value) and bucket overrides. Explicit endpoint URLs must be origins
without paths, queries, fragments or credentials. Path-style custom DNS
endpoints are supported, including Cloudflare R2
(`https://<account>.r2.cloudflarestorage.com`), OVHcloud
(`https://s3.<region>.io.cloud.ovh.net`) and MinIO behind a hostname.
Bucket-hosted AWS URLs and hosts beginning with the configured bucket name
are refused. A destination host beginning with the source bucket's DNS label
(or full bucket name for dotted names) is also refused. These checks apply
before any backup requests. With the same bucket name, equal or overlapping source/destination prefixes
are refused for any custom endpoint, even when the hostnames differ: aliases
can reach the same store. Choose a different bucket name or a non-overlapping
prefix. AWS regional service endpoints identify the same AWS partition; only
distinct known partitions (AWS and AWS China) prove separation.
Configure a path-style service endpoint; do not use a bucket-hosted URL.

`--history head` keeps the snapshot, its boundary segment and the segments
to the head. `--history all` also retains the available historical objects
and the previous compatible run's objects. Deletes erase live history
immediately, but **older backups still contain the deleted data**. An app
recreated between runs starts a new history set; a tombstone or recreation
during a run refuses completion.

Each actor records the recoverable transaction ranges actually archived.
A GET miss in retained history can split a range, even when the head is intact.
Create reports these gaps and exits nonzero with `--require-continuous true`,
the default for `--history all`. Use `--require-continuous false` to explicitly
accept a partial historical range; full verify replays every position within
each recorded range and reports those ranges. Runs closer than the retention
window alone do not guarantee continuous history: a read can cross expiry.
Every retry adds its observed keys to gap accounting. A retained snapshot
restores its own position even if its boundary segment has disappeared.
Objects already erased before listing cannot be promised by this archive.

The archive uses immutable `data/<sha256>` objects and checksummed actor
manifest chunks bounded by serialized bytes below the 64 MiB loader cap;
actor records span ordered 1 MiB fragments. The loader streams fragments to a
private temporary file, without concatenating them in memory, and parses one
bounded actor at a time. Limits are 8 MiB serialized metadata per actor,
64 MiB per metadata object and accounted resident index, and 1 GiB of actor
metadata spool per run (incarnation markers are capped at 64 KiB each). Listing indexes and create use the same bounded policy;
exceeding a budget is an explicit error, not a truncated backup. These are
metadata budgets, not an RSS limit: JSON trees and allocator overhead add
memory, and LTX bodies/database replay have the separate 2 GiB database cap.
Temporary manifest files are removed on success or error. Split large runs by
app if a run budget is exceeded; oversized individual histories need a future
streaming history format.

`started` binds the run ID, source and expected app incarnations. Each app
record references that run and the started digest and binds actor membership
and status. `complete` references these records and the started digest;
`runs/<id>/root`, written after completion, references its completion digest.
Every load, including `verify --run ID`, checks that run's own root and
reconciles app membership with the incarnations. `latest` is only a convenience
pointer. Archives predating the per-run root must be recreated.
Never start nodes against this layout. `backup list` distinguishes incomplete
runs from complete runs and reports failed actors. A partial run exits nonzero,
and verification refuses it. A missing object, invalid chunk, corrupted LTX
or mismatched head/checksum also causes verification to fail. `--run ID`
selects an older run; `--sample N` checks a random subset and explicitly
records partial verification. Use full verification before a restore drill.

Threat model: verify detects corruption and inconsistent edits. Protected
chunk and app records bind expected actor membership and copied/failed status.
There is no key and no signature: an attacker who can rewrite the whole archive,
including all digests and per-run roots, can forge a consistent archive.
These hashes do not authenticate the archive or provide an externally trusted root.

Deploys (current plus five predecessors) and sealed secrets are copied after
actor history, with current pointers checked again. Owners, leases, node
heartbeats, cluster secrets, alarms and operator intents are excluded.
The manifest records each database's pending alarm and indexed tag for the
restore command. Keep the secrets decryption key separately.

Collection continues normally. A missing link/object causes a fresh actor
listing, up to eight attempts; checksum failures are terminal. Set
`--history-retention-secs` above twice the largest actor's read deadline plus
clock skew: about 35 minutes at the 1 GiB cap; the default hour covers this.
A busy zero-retention actor can starve the reader and fail the run safely.

Copying runs up to 16 actors concurrently with listing backpressure.
Unchanged head plans reuse their manifest only when
all listed sizes and ETags match. Moved actors fetch only new source objects,
and `--cache` keeps content, raw databases and checksum sidecars. When the cached
head is on the new replay plan, only its new segments are applied after checking
the cached database checksum. A new snapshot beyond the cached head starts a
fresh replay. Destination reads therefore exceed ADR 0033's optimized estimates.
Concurrent prune markers refuse completion conservatively; completed markers
permit later creates. The stopped-cluster copy and drill below remain available.

## Prune an online archive

```bash
wasmkeep backup prune --from s3://archive/prod --keep-runs 7
```

This command opens only the archive destination, using the current bucket
credentials (LIST, GET, PUT and DELETE). It never opens or changes a live bucket.
The retention rule keeps the newest N complete runs by run ID (UTC start time,
with the random suffix breaking ties). It also always keeps `latest`, complete
runs whose `started` object is at most 24 hours old, and runs pinned by
`before-restore/<nonce>` undo labels or `restoring/<run>` archive pins.
`--keep-runs 0` is allowed; these protections still apply. Age uses object-store
modification timestamps against the CLI clock; keep that clock synchronized.

Prune refuses while an incomplete run's `started` object is at most 24 hours
old. Older incomplete runs are abandoned; create refuses to publish after
24 hours. Before deleting, prune validates all kept complete manifests and
builds their combined content references, including deploys, secrets and all
apps. It removes discarded runs' `complete` and `verified` first, their other
metadata next, and `started` last. Only unreferenced `data/` objects older than
24 hours are collected. Shared content survives for every kept run.

Prune writes a permanent `prunes/<nonce>` marker before scanning. After requests
have drained, `prune-done/<nonce>` closes the marker, including a refused pass.
Create checks these markers before starting and before publishing; an intervening
prune makes it refuse completion. Create publication and prune deletion also
share a conditional `prune-lock`, protecting the `complete` to `latest` gap.
Never delete prune markers or completion records: they prevent a create spanning
an entire prune from missing it. Existing older CLIs refuse all such markers;
use this version for subsequent creates.

An error is safe to retry: prune recomputes references rather than replaying an
old deletion plan. A killed process or an uncertain lock response can leave
`prune-lock` held. Stop the old CLI, wait at least 40 seconds for pending
requests, remove **only** `prune-lock`, then rerun prune. A competing CLI must
never clear a live lock. Ordinary returned errors attempt to release it.

Whole-app restore and backup-backed actor restore now write an archive pin
before reading a run or changing the target. This requires archive PUT access.
Undo backups are pinned before publication, closing the gap before the undo
label is written. Pins and undo labels have no automatic expiry, including after
success, abort or failure; this deliberately favors recovery over reclamation.
Do not remove them while any restore or its `restoring` target fence can still
refer to the run. Before the first prune after upgrading, finish any restores
started by an older CLI (which did not write archive pins). A destination-only
pruner cannot discover fences in arbitrary live buckets.

## Restore an online archive

Stop the selected app's nodes first; other apps can keep running. Restore
always verifies the entire selected run, even if a sampled verification
previously passed. Partial or corrupt runs and tombstoned apps are refused.

```bash
wasmkeep backup restore --from s3://archive/prod --run RUN_ID \
  --app acme/shop --bucket s3://live/recovery
# Replace existing state, keeping the current code and secrets:
wasmkeep backup restore --from s3://archive/prod --run RUN_ID \
  --app acme/shop --bucket s3://live/prod --in-place
```

Without `--in-place`, the app's state root must be empty. Deploy and sealed
secrets default to `--deploy backup`; in place they default to `--deploy keep`.
Either choice can be explicit. Keeping a deploy requires an existing current
version. Secrets sealed for a different prefix need `wasmkeep secrets reset`.
The implicit `default/default` app is refused because its nodes do not follow
deploy checks. The archive and target must have disjoint bucket/prefix locations;
this first restore CLI uses the same AWS environment for both.

All app nodes, including offline ones still listed under `nodes/`, must
advertise the `restore` word on their heartbeat capability line. The version
line remains the binary version, unchanged. Older builds without that
capability must be stopped and upgraded before restarting. `--force` bypasses
this capability check and permits a partial undo backup, with an explicit
warning naming every actor the undo cannot recover. It never permits a live
app node, a tombstone, or a partial selected run (except the pinned partial
undo run, again requiring `--force`).

The command creates `apps/<tenant>/<app>/restoring` before changing state.
Nodes refuse it on start and stop at their next deploy check. A unique
`restored` marker invalidates their old local files, including actors absent
from the backup. Expired node leases are fenced; the command waits 40 seconds
after the restore fence and the latest heartbeat or lease fence before writing
history. It rebuilds generation floors and the alarm index, then installs
versions and current pointers, and removes `restoring` last.

In place, a verified backup of the old state is made first; `--force` may
accept failed actors only in this undo backup. All successfully copied actors
and the deploy metadata are still verified. Scheduled `backup create` refuses
while `restoring` exists and detects changes to `restored` across its copy.
Only the internal undo creator may read through its own exact restore fence.
The archive's `before-restore/<operation-id>` object names its run; the
restore journal also records `before_restore`. Restore that run to undo.
Before opening the fence, restore lists each target actor, plans its actual
head and replays it to compare its position and database checksum with the
manifest. Extra actors and unexpected higher generations retain the fence.
Generation floors for old actors are journaled before deletion, including
actors absent from the selected run.

After an error, rerun with the same options; the journal pins the run even
if `latest` moved. A process killed while holding
`apps/<tenant>/<app>/restore-lock` requires an operator to confirm that the
old restore process is stopped, wait at least 40 seconds for its outstanding
requests, delete **only that lock**, and rerun. An unlocked lock record is
reused conditionally. Ordinary exits settle pending requests for 40 seconds
before releasing the lock, so a late deletion cannot remove a later restore
fence. If `restoring` is already absent, the restore finished; clear an orphaned
lock without repeating the restore.
Do not remove `restoring` to expose incomplete state. The journal remains
through interrupted clearing and copying. Serialize restore, app deletion,
deploy, secrets updates and archive maintenance; concurrent operator mutations
are not a supported workflow. Archive write access is needed to create and
label the undo run; resume and undo after prepare verify the archive with read-only access. The standalone
`backup verify` command still writes its verification report.

### Recovery by phase

Read `apps/<tenant>/<app>/restoring` to identify `phase`, `run` and
`before_restore`. Keep nodes stopped. Resolve an orphaned lock as described
above before any recovery command.

| Phase | Supported recovery |
| --- | --- |
| No journal | No restore is pending, or it finished. Inspect state before starting a new operation. Clear only an orphaned lock after stopping its holder and waiting 40 seconds. |
| `prepare` | Original state has not been cleared. Retry the original options after fixing archive permissions/corrupt actors, optionally `--force` for explicitly accepted missing undo coverage. Or run `backup restore --abort --from ARCHIVE --bucket TARGET --app TENANT/APP`; it needs no archive reads or writes and keeps original bucket state. For an empty-root restore it instead cleans partial state and restored deploy/secrets, as in later phases. |
| `clear` | For an empty-root restore, run `backup restore --abort --from ARCHIVE --bucket TARGET --app TENANT/APP` to return to an empty state root. For an in-place restore, state may be partly deleted. Retry the original options, or restore the pinned `before_restore` run with `--in-place --deploy backup`. The fence stays closed. |
| `copy` | State or deploy may be partial, including conflicting stray keys. An empty-root restore can always use `--abort`, even if its archive run is lost; it has no undo run. For an in-place restore, abort is refused. Retry after repairing a transient error, or select the pinned undo run as below. Undo returns the journal to `clear`, removes failed-copy state and deploy payloads, preserves generation floors, and copies the undo under the same fence. |
| `abort` | Empty-root cleanup was interrupted. Rerun `--abort`; normal resume is refused. Cleanup is idempotent and the fence remains until it completes. |
| Undo `clear` / `copy` | Rerun the exact undo options. The journal now pins the undo run and resumes that operation. Partial undo again requires `--force`; actors not covered remain absent. |

```bash
wasmkeep backup restore --from s3://archive/prod --bucket s3://live/prod \
  --app acme/shop --run BEFORE_RESTORE_RUN_ID --in-place --deploy backup
```

Only the journal's pinned undo run may replace an unfinished restore; an
unrelated run is refused. An empty-root restore has no undo backup: repair
its archive/conflicting objects to resume, or use `--abort` before the fence
opens. Abort uses the mode recorded in the journal, regardless of the supplied
`--in-place` flag. Under the exclusive restore lock it waits 40 seconds and
checks for live nodes again before deleting the selected `state/<tenant>/<app>/`
contents and, for `--deploy backup`, its deploy and secrets. `--deploy keep`
preserves the existing deploy and secrets. The `restored` cache invalidation
marker and app lifecycle markers remain. It removes `restoring` last and holds
the lock for another 40 seconds before release. No archive access is needed.
After the fence opens, abort is no longer available. Never expose
partly copied state by deleting `restoring`. If an in-place undo archive is
unavailable after clearing, restore its access or recover the missing archive
objects first; neither `--abort` nor `--force` can reconstruct missing data.

Each actor returns to its own backed-up moment. Due alarms may fire again;
external effects can repeat, sockets reconnect, and old operator intents and
ownership records are not restored. A restore drill is still required to
validate your infrastructure and operational procedure.

## Inspect one actor

```bash
wasmkeep actor history acme/shop counter/alice --bucket s3://live/prod --json
wasmkeep actor history acme/shop counter/alice --bucket s3://live/prod --deep
wasmkeep actor export acme/shop counter/alice --bucket s3://live/prod -o alice.db
```

Shallow history uses only LIST. Deep history replays the head and reports its
capture time and replacement label (or “probably deleted” for an empty database
without a label). The app argument follows the existing positional export/SQL
syntax. See [actor data access](../actor-data-access.md).

## Stopped-cluster copies

The remaining sections describe copies made by an S3 tool, not the online
archive format. A running-bucket `rclone` copy is still not established as
restorable. A restore drill remains the proof of your operational procedure.

## What is in the bucket

Everything under the prefix given to `--bucket s3://BUCKET/PREFIX`. Each
app's actor state is under its own root, `state/<tenant>/<app>/` (written
`R` below; `default/default` for nodes started with `--actor`), from
0.4.0 ([ADR 0022](../adr/0022-app-owned-state-and-the-one-time-migration.md)):

| Keys | What | Written |
| --- | --- | --- |
| `R actors/<class>/<name>/wal/` | One object for every write an actor committed (a segment) | Created once, never changed |
| `R actors/<class>/<name>/snapshot/` | The whole database, every 64 writes | Created once, never changed |
| `R owners/<class>/<name>` | Which node holds the actor, with a generation that grows by one at every hold | Replaced when the actor changes hands |
| `nodes/<id>` | Each node's heartbeat, which is its lease | Replaced every 3 s |
| `R alarms/...` | The index of pending alarms, by due time | Created and deleted as alarms change |
| `R moves/...` | Requests to move one actor (`actor move`) | By the operator; deleted when done or 5 minutes old |
| `R deletes/...` | Requests to delete one actor (`actor delete`) | By the operator; deleted once the delete is recorded, or an hour old |
| `apps/<tenant>/<app>/<version>/` and `apps/<tenant>/<app>/current` | Deployed versions (components, access rules, configuration) and the pointer to the current one | Versions created once; `current` replaced by a deploy |
| `apps/<tenant>/<app>/secrets/` | The app's secrets, sealed, and their own `current` | As above |
| `cluster/secret` | The secret nodes sign their calls to each other with | Created once |
| `control/drain/` | Drain requests | By the operator's commands |
| `control/changed` | Rewritten after every drain, undrain or move request, so nodes list the requests when they change | By the operator's commands, and by a node that completes a move |
| `cluster/migration`, `cluster/migrated/<tenant>`, `cluster/removed/<tenant>` | The lock of a running `cluster migrate`, each migrated tenant's commit, and the mark `--remove-old` writes before it deletes the tenant's old keys ([upgrading to 0.4.0](upgrading-to-0.4.md)) | By `cluster migrate` |
| `R leases/<class>/<name>` | One small marker per actor, written by the node that first takes it. It carries the generation the actor's first ownership record starts from, and keeps a binary from before node leases from taking the actor ([ADR 0020](../adr/0020-one-lease-per-node.md)) | Created once, never collected |
| `probe/` | The object a node's start check writes to test conditional writes | Deleted right after the check |
| `diagnose/` | Scratch objects of `wasmkeep cluster diagnose` ([buckets](buckets.md)) | Deleted when the run ends |

History objects are deleted only by garbage collection, by the node that
holds the actor, after a newer snapshot of its own was acknowledged, and
only once they are older than `--history-retention-secs` (one hour by
default). A deleted actor's history is erased at once
([History in the bucket](../cluster.md#history-in-the-bucket),
[ADR 0012](../adr/0012-bucket-garbage-collection.md)).

Not in the bucket, to keep with the backup:

- the secrets key (`--secrets-key`). Without it the sealed secrets in a
  copy cannot be opened;
- each node's `--env` files, the stateless component if you serve one, and
  the `serve` command lines (the systemd units);
- with `--actor` instead of `--app`, the components themselves.

A copy holds every actor's data and the cluster secret in the clear.
Protect it like the bucket ([security model](../security.md)).

## What a restorable copy needs

To restore an actor, a node lists the actor's prefix, takes the newest
segment of the highest generation as the head, and walks back from it,
segment by segment, to the first snapshot on the way
([ADR 0007](../adr/0007-replication-protocol.md)). So a copy restores an
actor if it holds that snapshot and every segment after it up to the
head. Objects besides those do no harm. A segment missing in the middle
is an error, not a rollback: the actor answers `500` until the history is
repaired, and the other actors are not affected.

### With the cluster stopped: restorable

Stop every node cleanly and wait until `wasmkeep cluster status` exits
with 1 (no live node). A clean stop gives each actor up to 10 s to finish
its queued events and their uploads before the node gives it up. A node
that logs `shutdown: some actors did not finish in time` cut some events
short: a plain `exec` among them may not be in the bucket, and is then
not in the copy either. Once every process has exited nothing writes or
deletes any more, as long as nobody runs `wasmkeep deploy` or
`wasmkeep secrets` meanwhile, and the copy is the bucket at one moment.
This is the one way the code shows to be restorable. Its cost is the
cluster being down while the copy runs.

### With the cluster running: not established

A copy made object by object reads a listing first and the objects
afterwards, while nodes keep writing and collecting. From the code:

- Segments and snapshots are never changed after they are created, so an
  object that was copied is a good object.
- Garbage collection can delete an object between the listing and its
  copy. Take an actor whose last snapshot is older than the retention
  window. It reaches 64 writes during the copy, uploads a new snapshot
  and deletes the old one with the segments after it that are older than
  the window. The copy then has the newer segments the listing named,
  without the snapshot they start from and without the new one, which
  the listing did not name. That actor does not restore from that copy.
- A second copy into the same destination, started right after the first
  and without deleting anything there, brings the new snapshot and what
  follows it. Everything created since the first listing is younger than
  the retention window, so collection cannot delete it while the second
  pass runs, if both passes end within the window (an actor deleted
  meanwhile is erased at once, whatever the window). Extra objects in the
  destination do no harm to a restore. By that reasoning two passes into
  an empty destination close the gap for history. That is reasoning, not
  a test: nothing in the repository restores a cluster from such a copy.
- The other objects are copied at other moments than the history. An
  ownership record copied before a takeover, next to segments copied
  after it, carries a lower generation than those segments, and the next
  hold would use a generation that is already in the history. An alarm
  index copied before or after its actor's database can list an alarm
  the database does not have, or miss one it has. What a cluster
  started on such a copy does in each of these cases is not established.

So: a copy of a running cluster is better than none, and most actors in
it restore. It is not a backup the code shows to be restorable. If the
cluster cannot stop, bucket versioning is the other way, with unknowns
of its own.

### Bucket versioning

With versioning turned on in the bucket (AWS S3, and the S3-compatible
stores that implement it), a delete or a replace keeps the earlier
version of the object. Collection then removes nothing for good, and the
state of every key at one past moment can be rebuilt from the versions,
without stopping anything. The state at one moment is what a cluster
whose nodes all died at that moment would leave: nodes that die give
nothing up, and the next ones take their actors from the bucket, which is
what failover does.

What it costs and what is not checked:

- Rebuilding "every key as it was at time T" into another prefix is a job
  for the provider's tools or a script over the version listing. WasmKeep
  has no command for it, and nothing in the repository runs a cluster on
  a versioned bucket or restores one from versions: the paragraph above
  is what versioning promises, not something measured here.
- Nothing is deleted any more, so the bucket grows until a lifecycle rule
  expires old versions. That rule's age is how far back you can restore.
- A deleted actor's data stays in the old versions until that rule
  removes it. `delete-self` erases nothing for good before then.

## An example copy

An example with [rclone](https://rclone.org), one S3 tool among several.
It is an example, not a tested procedure: nothing in the repository runs
it, against any provider. Check how your tool lists, retries and verifies
on your store, then prove the copy with a restore drill.

```bash
# The cluster is stopped: this must print no live node and exit with 1.
wasmkeep cluster status --bucket s3://acme-wasmkeep/prod

# Copy the prefix to another bucket, into a new folder per day.
rclone copy src:acme-wasmkeep/prod dst:acme-wasmkeep-backup/2026-11-12/prod

# Compare sizes and checksums of both sides.
rclone check src:acme-wasmkeep/prod dst:acme-wasmkeep-backup/2026-11-12/prod
```

Keep the copy in another account or at another provider than the bucket,
with credentials the nodes do not have: whoever can write the bucket can
also delete it. Start the nodes again when the check has passed.

## Restoring

1. **Stop every node** and wait until `wasmkeep cluster status` exits
   with 1. Never restore under running nodes: they hold a newer deploy
   and newer secrets in memory than the copy names. They keep running
   the newer deploy, and they refuse the older secrets: 503 for the app
   after 30 s, until they are restarted
   ([Secrets in the bucket](../cluster.md#secrets-in-the-bucket)).
2. **Copy the backup into an empty prefix**, not over the damaged one.
   Objects written after the backup carry higher positions and
   generations than the copy's, and a restore takes the highest as the
   head: left in place, they win over what you copied back. Keeping the
   damaged prefix also keeps a way back.

   ```bash
   rclone copy dst:acme-wasmkeep-backup/2026-11-12/prod src:acme-wasmkeep-restored/prod
   ```

   Keep the prefix's name (`prod` here) and change the bucket if the app
   has secrets in the bucket: a sealed secret opens only under the
   prefix, tenant and app it was sealed for. Under another prefix,
   write the secrets again with `wasmkeep secrets reset`.
3. **Move each node's data directory aside** (`--data-dir`), or decide to
   keep it knowingly. A node replaces a local database that is not at the
   bucket's head, keeping the old file as `<name>.db.replaced` (one
   generation only: a second replacement overwrites it). But for
   an actor the bucket knows nothing about, the local database becomes
   its first snapshot: with the old directories in place, actors created
   after the backup come back from disk, on whichever node has their
   file.
4. **Start the nodes** with `--bucket` naming the restored prefix, and
   with the same node ids as before.
5. **Check**: `wasmkeep cluster status` lists every node live, and the
   deployed version and `seq` of the backup. Request a few actors you
   know.

What a restored cluster is:

- **The state of the backup.** Every write since is gone, durable or not.
- **The deploy and the secrets of the backup.** `apps/.../current` names
  the version that was current then. Deploy again for a newer one, and
  set again any secret changed since.
- **Free to take.** In a copy made with the cluster stopped, the node
  leases say `released`, and a record that still names one of those
  nodes is free: the first request for an actor starts it. In a copy made
  while nodes ran, the leases say `live` and records name the processes
  of that time. A node started under the same id replaces its lease after
  the start check (about 15 s when the copy's lease says `live`), which frees
  the records naming its old process. An id nobody starts again is fenced
  by the other nodes, or with `wasmkeep cluster fence <node>`.
- **Alarms that came due since the backup fire** soon after the start,
  and anything the actors did outside between the backup and the restore
  (a webhook sent, an email, a call to your API) is unknown to the
  restored state and may happen again. Alarms are at least once; a
  restore makes that visible.
- **Drain requests of the backup apply again**: a node that was draining
  when the copy was made drains again. `wasmkeep cluster undrain <node>`
  withdraws it. A move request in the copy may apply again too, for up
  to 5 minutes after the restore copy: a node counts a move request's age
  from the object's date in the bucket, which a copy normally sets anew
  (not tested). It expires by itself.

One actor cannot be restored alone today: there is no command for it,
and copying one actor's old objects under a running cluster changes
nothing, since its current history has the higher positions.

## A restore drill

A backup that has never been restored is a hope. Do this before you need
it, and again after every WasmKeep upgrade:

1. Copy last night's backup into an empty prefix of a scratch bucket,
   with the same prefix name.
2. Start one node on it, on a machine that is not part of the cluster,
   with an empty data directory, the same app and the secrets key:
   `wasmkeep serve --app acme/shop --bucket s3://scratch/prod --node-id drill ...`.
   Restored actors fire their overdue alarms and call out, and outbound
   HTTP reaches public addresses by default: if the actors send webhooks
   or call an API, first block that machine's outbound traffic except to
   the scratch bucket's endpoint (a firewall rule that allows only the
   bucket), and give it no `--allow-outbound`. The node itself must
   still reach the bucket: without it, it never starts serving.
3. Request a sample of actors, including the largest databases and some
   that had not been written for a long time. A `500` is an actor that
   did not restore: look at the node's log for the missing object.
4. Compare what they answer with what you expect at the time of the
   backup.
5. Write down how long the copy and the first requests took. That is
   your restore time.

The live-node checks cover the actors requested. For online archives, run
full `backup verify` as well: replay verification and serving restored
actors test different parts of recovery.

## Migrating to 0.4.0

0.4.0 keys each app's actor state under its own root,
`state/<tenant>/<app>/`
([ADR 0022](../adr/0022-app-owned-state-and-the-one-time-migration.md)).
A bucket prefix that holds actor state is migrated once, offline, by
`wasmkeep cluster migrate`, and the old keys removed later by
`--remove-old`: the whole procedure, with its backup and rollback, is in
[Upgrade a cluster to 0.4.0](upgrading-to-0.4.md). For backups:

- Keep one backup from before the migration until `--remove-old --apply`
  has run.
- A backup from before 0.4.0 restores into an empty prefix, then is
  migrated. A backup synced without deleting over a migrated prefix
  leaves both layouts; restore into an empty prefix.
- Lifecycle rules, access policies and backup scripts written for
  `actors/<tenant>/` must move to `state/<tenant>/<app>/`.
- On a versioned bucket, the old keys keep their noncurrent versions until
  the lifecycle rules expire them.

## Automated fixture drill

`./scripts/check.sh --s3` runs `scripts/drills/backup-restore.sh` against
its moto server, or the configured `WASMKEEP_TEST_S3_BUCKET`. It creates
a two-node fixture cluster and four counters, acknowledges durable writes,
and continues modest writes during online backup, full verification and
restore. The restore uses a fresh prefix, data directory and loopback ports.
Every sampled counter must answer with at least the durable count captured
before backup began. This is a per-actor lower bound, not an app-wide snapshot;
acknowledgements after backup started are outside that bound.

Prerequisites are the normal Rust/wasm32-wasip2 toolchains, Python 3 and
AWS CLI. **Additional tool justification:** AWS CLI supplies authenticated,
paginated scratch-object enumeration and deletion without adding a Rust or
Python package dependency. It uses the same credentials and `AWS_ENDPOINT`
as the node. No backup-prune command is required.

The drill creates a random top-level scratch prefix and refuses overlap
with an externally supplied source. Only the standalone fixture setup writes
to its source; backup of a supplied soak source is read-only. Cleanup stops
all owned nodes before deleting the exact scratch prefix and the temporary
data directory, on success and ordinary failure/interrupt. Cleanup errors
fail the drill. SIGKILL or machine loss cannot run cleanup; operators must
remove that run's `restore-drill-<random>/` prefix afterwards. Use dedicated
test credentials and a test bucket; a versioned bucket may retain old object
versions after deletion.

For a soak, set `SOAK_RESTORE_DRILL=1 ./bench/soak.sh`. It runs one drill
alongside the existing writers and kills, snapshots their maximum acknowledged
durable counts into `drill-acks.json`, and makes a drill failure fail the soak.
The local soak deploys its fixture before starting nodes when this option is
set. A remote soak additionally needs `WASMKEEP_SOAK_BUCKET=s3://BUCKET/PREFIX`
and an already deployed `restore-drill/fixture` app served with `--app` with class `t`, built from the
same test-actor source. The hook never deploys to the remote source. To invoke
the hook manually while that source is running:

```sh
./scripts/drills/backup-restore.sh --source s3://test/soak-run --acks drill-acks.json
```

The acknowledgement file is a nonempty JSON object mapping routes such as
`/actor/t/s123-0/durable` to positive integer counts. Capture it before the
command starts. The automated soak hook captures it from client responses,
not by reading the source after backup.

**Alarm isolation and remaining gap:** there is no start-without-alarms
option. This drill accepts only archived components byte-identical to the
local test actor, refuses other classes and fronts, and starts the local
fixture with `--app restore-drill/fixture` after replacing the scratch
deploy with an exact local fixture project (no archived configuration or front). Its alarm handler only
changes its own database and alarm; it cannot send a webhook. Archived
configuration and fronts are not executed. Listeners are loopback-only and
there are no external client requests. This is not a general-purpose drill
for arbitrary applications: those still need the outbound firewall isolation
in the manual procedure above. A future start-without-alarms mode remains
unimplemented. The nodes follow restore fences; this drill does not validate secrets.

Each successful run writes a unique CSV in `bench/results/` (or `--output`),
after cleanup succeeds. It records create, verify, restore and startup times,
checked actors, acknowledged floors, and the restored node's
`wasmkeep_bucket_requests_total` counters at the end of actor checks. Those
are ObjectStore operation counts, including startup/background work: one
listing counts once even if it spans pages, and a multipart upload counts
once when it starts. They exclude the backup CLI, source nodes, fixture
validation downloads and cleanup. They are **not** total billed S3 requests
or a measurement of the entire backup cost. Fleet timing and full request
cost still require a real-bucket run.
