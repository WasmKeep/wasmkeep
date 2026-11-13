# Upgrade a cluster to 0.4.0

0.4.0 keys each app's actor state under its own root,
`state/<tenant>/<app>/`, where 0.2 and 0.3 keyed it by tenant alone
(`actors/<tenant>/`, `owners/<tenant>/`, `leases/<tenant>/`,
`alarms/<tenant>/` and `control/move/<tenant>/`)
([ADR 0022](../adr/0022-app-owned-state-and-the-one-time-migration.md)).
A bucket prefix that holds actor state is migrated once, offline, by
`wasmkeep cluster migrate`. A node, `deploy` or `actor move` of 0.4.0
refuses a tenant that was not migrated, and prints the command to run.

This page is the whole procedure, from the backup to removing the old
keys. A bucket prefix with no actor state yet (no `actors/`, `owners/`,
`leases/` or `alarms/` folder) needs none of it: upgrade the nodes.

> **Pre-alpha.** The migration has been run against the test suites and
> simulations only. No measured duration is published yet: the dry run
> prints an estimate from its own listing latency. Plan a maintenance
> window, and keep the backup until the end.

## Before you start

- **One version for every node.** Nodes of 0.3 and 0.4 cannot run side
  by side: a node of one refuses the other's calls. Upgrades stop every
  node anyway.
- **Credentials that write the bucket.** The migration reads, copies,
  writes and (in the last step) deletes under the prefix.
- **The commands below**, with `s3://BUCKET/PREFIX` the value of the
  nodes' `--bucket`, and the AWS_* variables set as for the nodes.
- **Lifecycle rules, access policies and backup scripts** written for
  `actors/<tenant>/` must move to `state/<tenant>/<app>/` after the
  upgrade. An app is now two prefixes, `apps/<tenant>/<app>/` and
  `state/<tenant>/<app>/`.

## The steps

1. **Back up the bucket** with the cluster stopped
   ([how](backup-and-restore.md#with-the-cluster-stopped-restorable)), or
   right after step 3. Keep that copy until step 11 is done.

2. **Freeze deploys and secret changes**, CI included. A deploy by an
   older `wasmkeep` during the migration stops it before it commits.

3. **Stop every node**: the service units, not just the processes. A
   process its service manager restarts writes its lease again, and the
   migration refuses, or aborts before committing. `wasmkeep cluster
   status --bucket s3://BUCKET/PREFIX` exits with 1 once no node is live.

4. **Install 0.4.0** on every machine, without starting it.

5. **Dry run**:

   ```sh
   wasmkeep cluster migrate --bucket s3://BUCKET/PREFIX
   ```

   It writes nothing, and prints:

   - the app each class goes to: the one app whose current version
     deploys it; else the tenant's only app (`default/default` for the
     tenant `default` with no app, or only an app named `default`). A
     class two apps deploy, or one in no app's current version when the
     tenant has several apps, needs `--heir TENANT/APP`, with no default;
     the other apps start that class empty. So does such a class of the
     tenant `default` when it has another app: it may be an `--actor`
     node's, served from `default/default`. `--heir` must name an app of
     the tenant, or `default/default`;
   - the node leases it sees, and whether each may still run (a node
     stopped cleanly is quiet at once; a crashed or fenced one once its
     lease is 16 s old, with the default timings);
   - the objects per family, and an estimate of the time;
   - what refuses the run, if anything.

   It exits with 0 when `--apply` would run, 1 otherwise.

6. **Apply**: the same command with `--apply` (and the same `--heir`):

   ```sh
   wasmkeep cluster migrate --bucket s3://BUCKET/PREFIX --apply
   ```

   It takes the lock (`cluster/migration`), waits one node-lease window
   (16 s with the default timings: a node the dry run judged stopped,
   because its clock and the migrator's disagree, renews its lease in
   that time and the run aborts), copies inside the bucket (no download),
   checks every copy, writes the barriers that stop older binaries (token
   `3` in every app's `current`, and a tombstone over every old ownership
   record, or where an actor has history and no record), proves that no
   node ran meanwhile, and commits each tenant (`cluster/migrated/<tenant>`).
   Move requests (`control/move/`) are not copied: request them again
   after the upgrade.

   Run one `--apply` or `--rollback` at a time: a second run takes the
   lock over, and the first one stops. After a crash or an abort, run it
   again: it resumes. Exit codes: 0 done, 1 refused or aborted with
   nothing new committed, 2 some tenants committed and others not (run it
   again).

7. **Verify**: run the dry run again. Every tenant reads `migrated`, and
   nothing is refused. `wasmkeep cluster status` still shows no live node.

8. **Start** every node of 0.4.0. At its first start a node puts its old
   local databases aside (`<data_dir>/actors.pre-0.4/`), and restores
   each actor from the bucket on its first wake; a node with no bucket
   (`dev`, `serve` alone) moves them into its app's folder instead.
   **From the first start, the migration is one-way.**

9. **Check the app**: `wasmkeep cluster status` shows every node live,
   and requests to a few known actors return their data. Then unfreeze
   deploys and secret changes.

10. **Remove the old keys, dry run**, once the app answers (the same day
    is the intent):

    ```sh
    wasmkeep cluster migrate --bucket s3://BUCKET/PREFIX --remove-old
    ```

    It deletes nothing, and prints, for every migrated tenant, the
    old-layout keys it would delete, and what it keeps:

    - the tombstones over the old ownership records, for good: without
      them a 0.2 binary started by mistake would create those actors
      anew, empty;
    - history younger than `--history-retention-secs` (3600 by default;
      give the nodes' value): run it again later for those;
    - move requests younger than 5 minutes, which is when they expire;
      older ones are deleted. 0.4.0 never reads them.

    A tenant with old-layout keys and no commit is listed as not migrated
    and never touched. Nothing under `state/`, `apps/` or `nodes/` is ever
    deleted.

    It refuses, deleting nothing, while:

    - a run of `--apply` or `--rollback` is unfinished (the lock exists);
    - the rollback would still work: no node has started since the
      migration and no app was deployed. Start the nodes first (step 8),
      or pass `--forgo-rollback` to give the rollback up;
    - an old-layout key was written after the tenant's commit, or an old
      ownership record is not a tombstone, or history or a marker has no
      tombstone: a binary of a version before 0.4.0 ran on the migrated
      prefix. Its writes are not in what 0.4.0 serves. The refusal names
      each key: copy them aside if they matter, remove them from the
      prefix, and run it again.

11. **Remove the old keys**: the same command with `--apply`:

    ```sh
    wasmkeep cluster migrate --bucket s3://BUCKET/PREFIX --remove-old --apply
    ```

    Before its first delete it writes `cluster/removed/<tenant>` for each
    tenant; from then on `--rollback` refuses for good. If a run of
    `--apply` or `--rollback` takes the lock in between, it stops before
    deleting. After a crash, run it again: it goes on. Exit codes: 0
    done, 1 refused or stopped. Storage is back to one copy of the state
    (on a versioned bucket, once the noncurrent versions expire). The
    backup from step 1 can go.

## Rolling back

**Before the first start** (step 8), and while no app was deployed:

```sh
wasmkeep cluster migrate --bucket s3://BUCKET/PREFIX --rollback
```

puts the old ownership records and the `current` tokens back and deletes
the copies, for the older binaries. Then reinstall the older version and
start it. A rollback that has started finishes when run again, whatever
ran since. If a 0.4.0 node had already put its local databases aside
(`<data_dir>/actors.pre-0.4/`) before it failed to start, move them back
to `<data_dir>/actors/` and remove the file left at that path before
starting the older binary.

**After the first start**, the rollback refuses: a node of any version
ran, an app was deployed, or a key under a migrated root is newer than
the tenant's commit. The state stays where 0.4.0 reads it. Going back to
the older version then means restoring the backup from step 1 into an
empty prefix, which loses every write since the migration. After
`--remove-old --apply` has started, the rollback refuses whatever else
is true.

## Notes

- Copies get a new last-modified time, so copied history is kept one more
  `--history-retention-secs` before collection.
- A backup from before 0.4.0 restores into an empty prefix, then is
  migrated. A backup synced without deleting over a migrated prefix leaves
  both layouts: 0.4.0 reads only `state/` for a migrated tenant and
  ignores the old keys; for a tenant not migrated yet, `cluster migrate`
  refuses one that has both with no migration in progress. Restore into
  an empty prefix.
- A node of 0.2 started on a migrated prefix with `--actor` answers 503
  for every migrated actor (its old record is a tombstone); an actor it
  creates under the old keys is not seen by 0.4.0, and `--remove-old`
  refuses until those keys are moved out. Do not run it there. Binaries
  of 0.3 refuse a migrated prefix, and 0.2 `serve --app` and `deploy`
  stop on token `3`.
