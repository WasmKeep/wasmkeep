# 0022. App-owned state and the one-time migration

- Status: accepted; built for 0.4.0, in no release yet: the layout
  (`crates/wasmkeep-cluster/src/state_root.rs`),
  `cluster migrate` with `--remove-old` (`crates/wasmkeep-cluster/src/migrate.rs`,
  `crates/wasmkeep-cli/src/migrate_cmd.rs`; `sim/tests/madsim_migrate.rs`,
  `crates/wasmkeep-server/tests/cluster/migrate.rs`), the start checks and
  local moves (`crates/wasmkeep-server/src/local_layout.rs`), the implicit
  app and the app between nodes (`crates/wasmkeep-server/src/cluster/forwarding.rs`,
  `wrong_app`), and two apps of one tenant on one prefix
  (`sim/tests/madsim_two_apps.rs`). Built in 0.3.0: the guard only
  (below, "Old binaries").
- Date: 2026-11-12
- Release: [0.4.0](../../ROADMAP.md#now)
- Amends: [ADR 0007](0007-replication-protocol.md),
  [0009](0009-alarms-in-a-cluster.md),
  [0012](0012-bucket-garbage-collection.md),
  [0014](0014-draining-a-node-and-moving-an-actor.md),
  [0017](0017-moving-one-actor.md) and
  [0020](0020-one-lease-per-node.md): actor state is keyed under its
  app's root. [ADR 0016](0016-secrets-in-the-bucket.md) only for the
  root: secrets do not move.
- Follows: [ADR 0021](0021-the-app-model.md) ("An app owns its actors").

## Context

Actor state is keyed by tenant, class and name: `actors/<tenant>/...`,
`owners/<tenant>/...`, `leases/<tenant>/...`, `alarms/<tenant>/...` and
`control/move/<tenant>/...`. Two apps of one tenant on one bucket prefix
share every actor whose class name they share. ADR 0021 decided that an
app owns its actors, and that existing data moves once into one layout
rather than keeping two layouts for good. This record fixes that layout
and the migration.

## Decision

### One layout

Each app's actor state lives under its own root,
`R = <prefix>state/<tenant>/<app>/`, beside `apps/`:

| Family | Before 0.4.0 | From 0.4.0 |
|---|---|---|
| history (ADR 0007) | `actors/<t>/<c>/<n>/{wal,snapshot}/...` | `R actors/<c>/<n>/{wal,snapshot}/...` |
| ownership record (ADR 0020) | `owners/<t>/<c>/<n>` | `R owners/<c>/<n>` |
| old lease marker, the generation seed | `leases/<t>/<c>/<n>` | `R leases/<c>/<n>` |
| alarm index (ADR 0009) | `alarms/<t>/<at>/<c>.t/<n>/<gen>-<nonce>` | `R alarms/<at>/<c>.t/<n>/<gen>-<nonce>` |
| moves (ADR 0017) | `control/move/<t>/<c>/<n>/<target>` | `R moves/<c>/<n>/<target>` |
| deploys, secrets | `apps/<t>/<a>/...` | unchanged |
| shared by the prefix | `nodes/`, `control/drain/`, `cluster/secret`, `probe/` | unchanged |
| migration (below) | | `cluster/migration`, `cluster/migrated/<t>`, `cluster/removed/<t>` |

- **Key tails are byte-identical.** Everything after the family keeps
  today's encoding, so the migration only changes the front of a key
  and never decodes a name (the segment encoding is lossy for long
  names).
- **The tenant and app are written raw**, as under `apps/`; the 0.3.0 name
  grammar keeps them safe.
- **`state/` sits beside `apps/`, not inside it**, so the deploy garbage
  collector, which treats every child of `apps/<t>/<a>/` as a version,
  can never reach it. An app is two prefixes, `apps/<t>/<a>/` and
  `state/<t>/<a>/`, for deleting, exporting and access policies alike.
- **On disk** the database moves to
  `<data_dir>/state/<t>/<a>/actors/<c>/<n>.db` (each segment encoded),
  with its sidecars beside it.
- **The address an actor sees stays `tenant/class/name`.** It is
  replicated and guest-visible; the app comes from where the actor lives.
- **Secrets do not move.** Sealing binds the format, prefix, tenant and
  app (ADR 0016), not the object key.

### The implicit app

`serve --actor` and `dev` run as the app `default/default`. Nothing
changes for their users. `deploy --app default/default` is allowed: it is
how an `--actor` cluster moves to deploys and keeps its state.

### The app between nodes

Every node-to-node call carries the caller's app in a signed header,
`x-wasmkeep-app: tenant/app` (ADR 0011's signed list). A node that serves
another app refuses the call before reading its body: 503,
`Retry-After: 1`, "not this app's node". The tenant checks on drain and
move bodies pass between two apps of one tenant; the header does not.
Heartbeats always name the app, `default/default` for `--actor` nodes.
`actor move` gains `--app tenant/app`.

### The migration: offline, by a command, once

```
wasmkeep cluster migrate --bucket s3://B/P                 # dry run, read-only
wasmkeep cluster migrate --bucket s3://B/P --apply [--heir T/A]...
wasmkeep cluster migrate --bucket s3://B/P --rollback      # until a 0.4.0 node starts
wasmkeep cluster migrate --bucket s3://B/P --remove-old [--apply] [--forgo-rollback]
                                                           # once the app answers on 0.4.0
```

It runs while every node is stopped, which an upgrade across a lease
change already requires (ADR 0020, section 6). The dry run writes
nothing: it prints the plan, the live nodes it sees, object counts and
an estimate of the time. Each class goes to the one app whose manifest
deploys it; a class in no manifest goes to the tenant's only app (or to
`default/default`); a class two apps deploy, a tenant with several
apps, or the tenant `default` with an app other than `default` (such a
class may be an `--actor` node's), needs `--heir`, with no default.
`--heir` names an app of the tenant, or `default/default`.

`--apply`, in outline, resumable from any step, with one commit per
tenant:

1. **Plan and refuse**: an unresolved heir, a live node lease (the ADR
   0020 rule; migrate fences nothing), an object too large for one
   server-side copy, a different earlier plan.
2. **Lock**: create `cluster/migration` holding the plan, each app's
   current `current` token and the full `nodes/` set. Every later write
   that another run would make wrong (deletes in R, barriers, commits)
   first rewrites the lock conditionally, so a run whose lock was taken
   over stops. Then wait one node-lease window: a node misjudged as
   stopped (clocks apart) renews its lease in it, which step 6 sees, or
   has lost it and acknowledges nothing after.
3. **Copy** server-side into R. History objects, which are create-only,
   are skipped when present; mutable objects (records, markers, alarms)
   are copied every time. Move requests are not copied: a copy would
   renew their lifetime, measured from the last write.
4. **Verify**: list the source again until it is unchanged, compare
   mutable objects byte for byte, check that a restore plan on R ends at
   the same head as on the source, and delete keys in R the source no
   longer has.
5. **Barriers**: mark every app's `current` with a new token, `3`, that
   older binaries cannot read, and overwrite each bare ownership record,
   conditionally on what was copied, with a tombstone naming the new
   root; an actor with history and no record gets a tombstone created
   where its record would be. A conflict means an old node took the
   actor: abort.
6. **Final proof**: list `nodes/` again and read each key; any new or
   changed key means a node ran, and the run aborts with nothing
   committed. Every `current` of the tenant must carry token 3.
7. **Commit**: create `cluster/migrated/<tenant>`. From here, R is the
   truth for that tenant. Then unlock.

A node acknowledges a write only while its node lease is valid (ADR
0020), and holding one means rewriting `nodes/<id>`. Refusing live nodes
first and proving after the barriers that none ran is what keeps every
acknowledged write.

`--rollback` restores the ownership records and the `current` tokens and
deletes R, and only while `nodes/` is unchanged since the commit, that
is, while no node of any version has run, no app of the tenant was
deployed, and no key in R is newer than the commit. **After the first 0.4.0 node
starts, the migration is one-way.** The bare keys stay until
`--remove-old`, meant for the same day, once the app answers on 0.4.0:

- **What it deletes**: for each tenant with a commit, the bare history,
  markers and alarm entries, and the bare move requests older than their
  5-minute lifetime (moves are never copied; an older `actor move` may
  still write one, which no 0.4.0 node reads, so a live one is left for
  a later run rather than refused). It keeps the tombstones for good:
  without them a 0.2 `--actor` binary would recreate those actors empty
  at generation 1. It keeps bare history younger than the history
  retention (`--history-retention-secs`, ADR 0012) for a later run. It
  never derives a key under `state/`, `apps/` or `nodes/`, and never
  touches a tenant with no commit.
- **When it refuses**, deleting nothing: while the lock exists; while
  `--rollback` would still work (the rollback's own one-way test: no
  `nodes/` change, no deploy, no newer key in R), unless given
  `--forgo-rollback`; and on any bare key an older binary wrote since: a
  history, marker or alarm key last modified at or after the commit (both
  are the bucket's times; equal counts, since a store may keep whole
  seconds and nothing of those families is written in the run's last
  node-lease window), a key of a class the commit names no heir for, a
  bare ownership record that is not a tombstone, or history or a marker
  of an actor with no tombstone (a new actor created in the window between
  the final proof and the commit). The refusal names each key.
- **One-way for good**: before its first delete it creates
  `cluster/removed/<tenant>` for each tenant it deletes from (none for a
  tenant with nothing to delete), which `--rollback` counts as one-way,
  then reads the lock again and stops if a run took it meanwhile, then
  reads each such tenant's commit again and stops if it is gone or is not
  the one it listed (a rollback that took the lock and let it go in
  between). A rollback checks under its lock, and deletes the commit
  under it: so a rollback that ends before the lock read leaves no
  commit, one that starts after the marks refuses, and a rollback never
  restores records over deleted history. It takes the lock first as not
  checked (`mode rollback-unchecked`, keeping the lock it replaced), and
  marks it checked only once that check passes, before its first other
  write: a rollback that failed or crashed in the check, run again,
  checks again rather than resuming, and a refusal puts back the lock
  it replaced (none, or an unfinished `--apply`'s). Only a checked
  rollback lock is finished whatever ran since; an older build refuses
  an unchecked one. A removal that finds the lock after its marks
  leaves them: the rollback is given up for those tenants (unless a
  checked rollback holds the lock, which removes them as it finishes),
  so a later `--remove-old` asks no `--forgo-rollback` for a marked
  tenant. An `--apply` that lost its lock right after writing a commit
  (a run with nothing to do released it) keeps that commit when the
  tenant is marked: a removal may have deleted on the strength of it,
  and without it a later `--apply` would migrate the tenant again from
  its tombstones over the only copy. A rollback that finishes
  deletes the mark with the commit, and `--apply`, under its lock,
  deletes the mark of a tenant it is about to commit that has no commit:
  only a removal that stopped before deleting can have left it.
- It is a dry run without `--apply`, and resumes after a crash: it
  recomputes everything from listings, and deletes are idempotent. It
  takes no lock: nodes run while it does, and a held lock would refuse a
  node that restarts.

A 0.4.0 node or CLI checks before any write: it refuses while the lock
exists, and refuses a tenant with bare keys and no commit, printing the
command to run. A node renames its local data directory at start: aside
in cluster mode (the bucket is the truth), into the new layout when the
local copy is the only one, leaving a file at the old path so that an
old binary fails instead of starting empty.

### Old binaries

- **0.3.0** refuses `serve`, `deploy` and `dev` on a bucket that has
  `cluster/migration` or any `cluster/migrated/<tenant>`. Both names are
  reserved from 0.3.0.
- **0.2 `serve --app` and `deploy`** cannot read token `3`, and stop.
- **0.2 `serve --actor`** gets the tombstone on an existing actor and
  answers 503. A brand-new actor it creates under the bare keys is found
  by `--remove-old`; running 0.2 on a migrated bucket is unsupported.
- **Mixed 0.3 and 0.4 nodes** are not supported: their calls fail on the
  signed header.

### What this amends

- **ADR 0007**: history keys move under R; the protocol is unchanged.
- **ADR 0009**: the alarm index moves under R; each node sweeps its own
  app's index only.
- **ADR 0012**: history collection walks R. Copied objects get a new
  last-modified time and are kept one more retention period.
- **ADR 0014 and 0017**: move keys become `R moves/...`; a drain still
  names a node, and calls carry the app.
- **ADR 0020**: ownership records and lease markers move under R; node
  leases (`nodes/`) stay shared by the prefix, and fencing stays
  app-blind.
- **ADR 0016**: only that the app's state root is a second prefix beside
  `apps/<t>/<a>/`; secrets and sealing are unchanged.
- **ADR 0021**: the one-app-per-tenant-per-prefix refusal is removed.

## Consequences

- Two apps never share an actor, whatever prefix they use.
- Every upgrade to 0.4.0 of a cluster with state runs one command while
  stopped, adding about the time of one server-side copy of the state
  (tens of seconds for a thousand actors; an estimate until measured).
  Storage doubles until `--remove-old`.
- Lifecycle rules, credentials and backup scripts written for
  `actors/<t>/` must move to `state/<t>/<a>/`. A pre-0.4 backup is
  restored into an empty prefix, then migrated. Deploys from CI and
  secret changes are frozen during the run.
- A class two pre-0.3 apps shared goes to one heir; the other app starts
  it empty.

## Alternatives

- **Two layouts for good, or a zero-copy root** that keeps pre-0.4 apps
  on the bare keys. No migration, but every later feature that touches
  keys (collection, export, deleting an app) is written and simulated
  twice, and old binaries keep working on bare data. Rejected, as in
  ADR 0021.
- **Online migration while nodes run.** It needs reads from both layouts,
  ownership valid in two places under one fence, and sweeps over both:
  the two-layout cost again. Upgrades already stop every node.
- **Automatic migration at node start.** A rewrite of the whole bucket
  should show its plan first, and some heirs need a person to choose.
- **Reverse migration after 0.4.0 has written.** It has its own crash
  windows and doubles the work; `--rollback` before the first start and
  the bare data kept until `--remove-old` cover the realistic mistakes.
- **Fencing live nodes from migrate.** A fenced node under a service
  manager restarts, and a wrong `--bucket` would fence production. It
  refuses live nodes instead.
