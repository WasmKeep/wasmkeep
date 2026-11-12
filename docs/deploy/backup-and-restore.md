# Back up and restore a cluster's bucket

The bucket is the cluster's only durable copy of everything: a node's
disk is a cache of it ([replication](../cluster.md#replication)). This
page says what is in the bucket, how to copy it so that the copy can be
restored, how to restore it, and how to check that it works.

> **Pre-alpha, and a guide without a tool.** WasmKeep has no command to
> back up a bucket, to verify one or a copy of one, to restore one, or to
> export or restore a single actor. Verifying a bucket and exporting an
> actor are on the [roadmap](../../ROADMAP.md). Until then a backup is a
> copy made with an S3 tool, and the only proof that it restores is a
> restore drill ([below](#a-restore-drill)).

## What is in the bucket

Everything under the prefix given to `--bucket s3://BUCKET/PREFIX`:

| Keys | What | Written |
| --- | --- | --- |
| `actors/<tenant>/<class>/<name>/wal/` | One object for every write an actor committed (a segment) | Created once, never changed |
| `actors/<tenant>/<class>/<name>/snapshot/` | The whole database, every 64 writes | Created once, never changed |
| `owners/<tenant>/<class>/<name>` | Which node holds the actor, with a generation that grows by one at every hold | Replaced when the actor changes hands |
| `nodes/<id>` | Each node's heartbeat, which is its lease | Replaced every 3 s |
| `alarms/<tenant>/...` | The index of pending alarms, by due time | Created and deleted as alarms change |
| `apps/<tenant>/<app>/<version>/` and `apps/<tenant>/<app>/current` | Deployed versions (components, access rules, configuration) and the pointer to the current one | Versions created once; `current` replaced by a deploy |
| `apps/<tenant>/<app>/secrets/` | The app's secrets, sealed, and their own `current` | As above |
| `cluster/secret` | The secret nodes sign their calls to each other with | Created once |
| `control/` | Drain and move requests | By the operator's commands |
| `leases/`, `probe/` | Markers left by earlier versions, and the object the start check writes | Small |

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
with 1 (no live node). A clean stop lets each actor finish its events and
their uploads before the node gives it up. After it nothing writes or
deletes any more, as long as nobody runs `wasmkeep deploy` or
`wasmkeep secrets` meanwhile, and the copy is the bucket at one moment.
This is the one way I can show to be restorable from the code. Its cost
is the cluster being down while the copy runs.

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
  the database does not have, or miss one it has. I do not know what a
  cluster started on such a copy does in each of these cases.

So: a copy of a running cluster is better than none, and most actors in
it restore. It is not a backup I can vouch for. If the cluster cannot
stop, bucket versioning is the other way, with unknowns of its own.

### Bucket versioning

With versioning turned on in the bucket (AWS S3, and the S3-compatible
stores that implement it), a delete or a replace keeps the earlier
version of the object. Collection then removes nothing for good, and the
state of every key at one past moment can be rebuilt from the versions,
without stopping anything. The state at one moment is what a cluster
whose nodes all died at that moment would leave: nodes that die give
nothing up, and the next ones take their actors from the bucket, which is
what failover does.

What it costs and what I have not checked:

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
   bucket's head, keeping the old file as `<name>.db.replaced`. But for
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
  the start check (up to 15 s), which frees the records naming its old
  process. An id nobody starts again is fenced by the other nodes, or
  with `wasmkeep cluster fence <node>`.
- **Alarms that came due since the backup fire** soon after the start,
  and anything the actors did outside between the backup and the restore
  (a webhook sent, an email, a call to your API) is unknown to the
  restored state and may happen again. Alarms are at least once; a
  restore makes that visible.
- **Drain requests of the backup apply again**: a node that was draining
  when the copy was made drains again. `wasmkeep cluster undrain <node>`
  withdraws it.

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
   or call an API, cut that machine's route to the internet first, and
   give it no `--allow-outbound`.
3. Request a sample of actors, including the largest databases and some
   that had not been written for a long time. A `500` is an actor that
   did not restore: look at the node's log for the missing object.
4. Compare what they answer with what you expect at the time of the
   backup.
5. Write down how long the copy and the first requests took. That is
   your restore time.

The drill reads every object only for the actors it requests. Until a
verify command exists, the sample is the measure of the copy.
