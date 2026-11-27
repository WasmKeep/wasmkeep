# Upgrade a cluster to 0.9.0

0.9.0 is the first minor release a cluster can move to one node at a
time, without stopping as a whole, with the checks for it in the binary.
The supported pair is **0.8.1 to 0.9.0**. Either way, the upgrade ends
with one command, `wasmkeep cluster upgrade finalize`: **until you run
it, any node may go back to 0.8.1, and what 0.9.0 adds to the bucket
stays off.** After it there is no way back.

**Recommended for 0.9.0: stop every node, then start them all on 0.9.0**
([with every node stopped](#with-every-node-stopped-recommended)). An
upgrade one node at a time is supported and safe for data, but it is not
yet proven to meet the latency goal under load: while each node
restarts, its clients see slow and failed deliveries, and afterwards a
big room's edges stay on the nodes upgraded first until 0.10.0
rebalances them ([measured](#measured)).

The rules are in [ADR 0043](../adr/0043-rolling-upgrades.md); the commands
are described in [Upgrading WasmKeep](../cluster-upgrades.md). This page
is the rule for this release.

## Before you start

- **Every node runs 0.8.1.** A cluster on 0.8.0 installs 0.8.1 first,
  every node stopped, as
  [from 0.8.0 to 0.8.1](upgrading-to-0.8.md#from-080-to-081) says. A 0.8.0
  node does not read the upgrade floor, and the pair 0.8.0 beside 0.9.0 is
  not supported. Nothing refuses it for you: a node compares major and
  minor versions only, so it cannot tell 0.8.0 from 0.8.1
  ([Limits](#limits-and-what-is-not-built)).
  `wasmkeep cluster status` shows each node's version.
- **Older clusters go one minor version at a time**, by each release's
  page. A 0.9.0 node refuses to start beside a live node older than 0.8.
  It also refuses with every node stopped, when the node leases they
  left say the app last ran 0.7 or older: it names one of them and says
  to install the last 0.8 release on every node and start them first.
- **A bucket from before 0.4.0 is migrated with 0.8.1 first.**
  `wasmkeep cluster migrate` is removed in 0.9.0
  ([below](#removed-in-090)).
- **Use the 0.9.0 `wasmkeep` for the commands on this page.** It runs
  beside 0.8.1 and 0.9.0 nodes, and `finalize` has to be run with it.
- **A single node is not a rolling upgrade.** Its restart is downtime:
  stop it, install 0.9.0, start it, then finalize.
- **Upgrade backup jobs with the cluster**, before the first backup after
  the finalize ([Backups](#backups)).

## With every node stopped (recommended)

The cluster serves nothing between the last stop and the first start,
so plan a short window.

1. **Stop every node** cleanly (`systemctl stop wasmkeep`, `SIGTERM`),
   and wait until `wasmkeep cluster status` exits with 1 (no live node).
2. **Install 0.9.0** on every node.
3. **Start every node**, and wait until each is `serving`:
   `/_wasmkeep/ready` answers 200 on it, and `cluster status` lists it
   live, on 0.9.0.
4. **Finalize** ([below](#finalize)). It comes after the start: until a
   node has run 0.9, every lease the nodes left names 0.8.1. With the
   0.9.0 `wasmkeep` it needs a live node; from 0.9.1 it also works once
   those nodes are stopped again, their leases naming 0.9
   ([with no live node](#finalize)).

The general steps are in
[Upgrading WasmKeep](../cluster-upgrades.md#with-every-node-stopped).

## One node at a time

Supported and safe for data, at a cost under load: each node's clients
reconnect while it restarts, with slow and failed deliveries for those
minutes, and the node upgraded last ends with few of a big room's edges,
which nothing moves back on its own yet ([measured](#measured)). For
each node in turn:

1. **Drain it**, and wait until `cluster status` shows it `drained`:

   ```bash
   wasmkeep cluster drain node-b --bucket s3://wasmkeep-demo
   wasmkeep cluster status --bucket s3://wasmkeep-demo
   ```

   It hands its actors to the other nodes and keeps forwarding
   ([Draining a node](../cluster-operations.md#draining-a-node)).
2. **Stop it** cleanly (`systemctl stop wasmkeep`, `SIGTERM`). `cluster
   status` then lists it as `no (released)`.
3. **Install 0.9.0.**
4. **Withdraw the drain request**, so that the new process takes actors
   from its first heartbeat. `undrain` withdraws a request even when no
   live node has the id:

   ```bash
   wasmkeep cluster undrain node-b --bucket s3://wasmkeep-demo
   ```

   Or start the node first and run `undrain` then; the drain request
   stays in the bucket until it is withdrawn.
5. **Start it.**
6. **Wait for `serving`**: `/_wasmkeep/ready` answers 200 on the node, and
   `cluster status` lists it live, on 0.9.0, with `STATE` `serving`. Then
   move to the next node.

While nodes of both versions are live, `cluster status` warns that live
nodes run different versions, and `wasmkeep cluster upgrade status` says
`mixed` and exits with 3. Both are expected.

During the upgrade, clients see what a rolling restart of one version
shows: retries for a draining node's actors, and reconnects for its
WebSockets.

Do not add the node-file keys or flags that are new in 0.9.0 yet
([Going back](#going-back)), and keep the mixed state short: it is meant
to last the length of an upgrade, not days.

## Finalize

Once every node runs 0.9.0:

```bash
wasmkeep cluster upgrade status --bucket s3://wasmkeep-demo
wasmkeep cluster upgrade finalize --bucket s3://wasmkeep-demo        # what it would do
wasmkeep cluster upgrade finalize --bucket s3://wasmkeep-demo --yes
```

`status` reads the bucket alone and writes nothing. Per app it prints the
upgrade floor, one state, the nodes that block a finalize, and each
registered node with its version and whether it is live. After the last
node it says `finalize pending`: every live node runs 0.9 and the floor
is still 0.8. `--app TENANT/APP` narrows both commands to one app;
`status --json` is for scripts.

| State | Meaning | Exit code |
| --- | --- | --- |
| `one version` | Every live node runs the floor's version: nothing to do | 0 |
| `mixed` | Live nodes on two minor versions next to each other | 3 |
| `finalize pending` | Every live node on one version, the floor below it | 3 |
| `no live node` | Nothing to compare | 3 |
| `node below the floor` | A live node runs a version below the floor: stop it | 1 |
| `refused` | Live nodes more than one minor version apart, a version that does not parse, or an unreadable floor record | 1 |

`finalize` works per app, over every app with a registered node under
the prefix. Without `--yes` it prints what it would do and exits with 2.
Without `--app`, it raises the floor of every app it can, names each
refused app with its reason, and exits with 1 if any was refused, even
after raising the others. With `--yes`:

1. It is **refused, naming them, while a live node of the app is older**
   than 0.9. It is refused too while an older node is not live but its
   lease is neither `released` nor `fenced`: a stale lease is not proof
   of a stop (the machine may be cut off and still act). Run `wasmkeep
   cluster fence <id>` for such a node first; a fence counts 15 s after
   the node's last write. **With no live node of the app**, the 0.9.0
   `wasmkeep` refuses it. From 0.9.1 it is accepted when every lease
   of the app is `released` or `fenced` and names 0.9 or later, and no
   object under `nodes/` fails to decode; a lease that still says 0.8.1
   refuses it: start the app once on 0.9.0, or remove that lease with
   `wasmkeep cluster forget`
   ([the cases](../cluster-upgrades.md#versions-the-upgrade-floor-and-finalize)).
2. It **raises the app's upgrade floor** to 0.9, by a conditional write.
   An app already at 0.9 is left as it is: the command can be run again.
3. It **waits one start check (about 25 s) and lists the nodes again.**
   An older node that is live then started while the floor was being
   written. It is reported as an error with exit code 1, and must be
   stopped. The floor stays.
4. It names the stopped nodes whose lease still says 0.8.1. That is not
   an error: such a node stops at its next start. Install 0.9.0 there, or
   remove its lease with `wasmkeep cluster forget`.

There is no `--force`.

### What finalizing changes

- **The floor.** Each app has an upgrade floor,
  `formats/<tenant>/<app>/floor` in the bucket: the lowest minor version
  allowed to serve it. Without a record it is 0.8. `finalize` writes 0.9.
  From then on a 0.8.1 node that starts into the app reads the floor,
  writes no lease and stops, reporting not ready with `the app's upgrade
  floor is above this node's version, or unreadable`. Once a version with
  `apart-from` or `spread` is deployed, a 0.8.1 node stops earlier still,
  on the manifest: `unreadable <version>/manifest`. It serves nothing
  either way; that message means the same thing, install 0.9.
- **Formats that need every node at 0.9 may be written.** Before the
  finalize, nodes and commands of 0.9.0 write only what 0.8.1 reads.
  After it:
  - a deploy with `apart-from` or `spread` on a class is accepted. Such a
    version is manifest format 8
    ([project file](../project-file.md#front-and-classes)). Before the
    finalize the deploy is refused and names the floor; after it, it is
    still refused while a live node is older or does not announce the
    word `apart`. A version without those keys keeps the format 0.8.1
    reads, before and after;
  - `wasmkeep cluster forget` removes a released node lease through a
    mark, so that it can no longer delete the lease of a node that just
    started under the same id
    ([how it works](../cluster.md#how-it-works)). Before the finalize it
    deletes as 0.8.1 does, and says so.

A new app is the exception to "off until you finalize". The first deploy
of an app that never ran (no floor, no deploy, no state, no registered
node, no tombstone), or its first node, creates its floor at once: at the
lower of its own version and the lowest version live under the bucket
prefix, and nothing while that is 0.8. So an app created after every node runs 0.9.0 is finalized from its
first deploy, and one created while a 0.8.1 node is live under the prefix
is not.

### Going back

**Until the finalize, any node may return to 0.8.1**, by the same steps:
drain, stop, install 0.8.1, start. First remove what 0.8.1 does not
know, since it refuses to start on an unknown node-file key or flag:

- the `[process]` keys `otlp-endpoint`, `trace-sample` and
  `trace-trust-incoming` of the [node file](../reference/node-file.md);
- the flags `--otlp-endpoint`, `--trace-sample` and
  `--trace-trust-incoming`.

**After the finalize, going back is not supported**, and a 0.8.1 node
that tries stops at its first read of the floor. Nothing lowers or
deletes the floor. `app delete` and `deploy --recreate` keep it too: an
app recreated after a finalize stays finalized.

A whole-app restore from a backup that carries a higher floor than the
target's raises the target's floor, with no node of the app live. For
that app it ends the way back as a finalize does ([Backups](#backups)).

An app created by 0.9.0 with no 0.8.1 node live under the prefix has its
floor at 0.9 from the start (above): there is no going back for that app.

### Finalize before the next upgrade

Finalize while 0.9 nodes are live. At the next upgrade, that means
before you stop the last node of the previous version, 0.9:

- `finalize` needs a live node of the app. With every node stopped it is
  refused: `no node of this app is live: there is nothing to finalize`.
- A node refuses to start two minor versions above an app's floor when
  the app has registered nodes, even alone. Restarted in place from 0.9
  on 0.10 without a finalize at 0.9, it does not start: put 0.9 back,
  finalize, then upgrade.

Skipping the finalize costs nothing else until then, except that what
0.9.0 adds stays refused.

## Removed in 0.9.0

**0.8.1 is the last version that carries each of these.**

- **`wasmkeep cluster migrate`**, with `--rollback` and `--remove-old`:
  the one-time migration to the layout of 0.4.0
  ([ADR 0022](../adr/0022-app-owned-state-and-the-one-time-migration.md)).
  - A bucket older than 0.4.0 is migrated with the 0.8.1 binary, brought
    to 0.8.1, then upgraded
    ([upgrading to 0.4.0](upgrading-to-0.4.md)).
  - A bucket migrated earlier keeps working: nodes and commands still
    read its commits and tombstones.
  - If its old keys were never removed, run `cluster migrate --remove-old
    --apply` with the 0.8.1 binary while the cluster is still on 0.8.1.
    0.9.0 cannot remove them.
  - Nodes and commands of 0.9.0 still refuse a tenant left in the old
    layout, or a prefix a 0.8.x migration holds, and say to use 0.8.1.
- **Three forms only nodes older than 0.6.0 sent** are no longer
  accepted ([ADR 0038, amendment](../adr/0038-several-apps-per-process.md#amendment-2026-11-25-the-older-forms-are-no-longer-accepted)):
  - a signed call, or a forward's proof, that names no sender is refused
    with 503;
  - the older form of the client's address in a forward's proof is not
    read;
  - a 404 in answer to a node's hello counts as a failed hello and is
    sent again.

  Nodes of 0.8.1 send none of these, so the upgrade from 0.8.1 is not
  affected. What that rests on is under
  [Limits](#limits-and-what-is-not-built).

Not removed, although older: a forward with no proof at all (0.8.1 still
sends one in some states), the heartbeat formats from before node leases
(`wasmkeep-node 1` and `2`) with their start guard, and the per-actor
lease marker.

## Backups

**Upgrade backup jobs to 0.9 before the first 0.9 backup of a finalized
app.**

From 0.9.0 `backup create` stores the upgrade floor of an app that has
one, and a whole-app restore leaves the higher of the target's floor and
the backup's. A run that carries a floor is backup format 2; a run in
which no app has a floor stays format 1. So the first backup made by
0.9.0 after the finalize, or of an app created finalized, is the first
format 2 run of its archive, and `create` says so: `this archive now
needs wasmkeep 0.9 or later`.

A `wasmkeep` of 0.8.1 refuses a format 2 run, with `unknown or
inconsistent backup format`. On an archive that holds one, with 0.8.1:

- `backup verify`, `backup restore` and an actor restore from that run
  are refused. A refused `restore` has already written its archive pin,
  `restoring/<run>`, which keeps that run from being pruned: remove that
  one object if the run must be prunable again.
- `backup create` is refused when the archive's latest run is format 2,
  after it wrote `runs/<id>/started` for its own run. That object makes
  `backup prune`, of any version, refuse for 24 hours. To prune sooner,
  delete the `started` object of each refused create, and nothing else.
- `backup prune` is refused when it keeps a format 2 run.

A 0.9.0 `wasmkeep` reads both formats. A whole-app restore is also
checked like a deploy, and can itself raise a floor; the rules are in
[backup and restore](backup-and-restore.md#the-upgrade-floor-and-the-deploys-format).

## CLI versions

Use a `wasmkeep` within one minor version of every live node of the app,
and not older than the app's floor.

- **Commands that write are refused outside that**: `deploy` and a
  rollback, the `secrets` writes, `app delete` (without `--force`),
  `app format --enable` and a whole-app `backup restore`. The refusal names the node and its version, or
  says that this `wasmkeep` is older than the cluster. A deploy is
  refused too while the app's floor record is unreadable.
- **Read-only commands are never refused.** `cluster upgrade status`
  prints a warning when the `wasmkeep` that runs it is outside that
  range, or older than the floor.
- **These run no such check**: `cluster drain` and `undrain`, `cluster
  fence`, `actor delete`, `actor move`, `backup create` and `secrets
  rotate-key`. `cluster forget` asks only whether it may write its mark.
  They write what 0.8.1 reads, and stay usable during the upgrade.
- **A 0.8.1 `wasmkeep` has no such check.** Before the finalize the
  bucket holds only what 0.8.1 reads. Do not use it against a finalized
  app: it does not read the floor, and its refusals do not name a
  version. In the mixed-version check, its `cluster status`, `secrets
  set` and `cluster drain` still worked there and were read rightly by
  the 0.9 nodes. Once a version with `apart-from` or `spread` is current,
  its `deploy` fails with `unreadable <version>/manifest` and its `backup
  create` with `invalid deploy manifest`; into an archive that already
  holds a run made by 0.9 after the finalize, `backup create` fails with
  `unknown or inconsistent backup format`. Each of those means: use a
  `wasmkeep` of 0.9.

## Limits, and what is not built

- **0.8.0 to 0.9.0 is not refused.** Versions compare by major and minor
  only, so a 0.8.0 node runs beside 0.9.0 and a cluster stopped on 0.8.0
  starts on 0.9.0. Until the finalize it reads everything 0.9.0 writes.
  It does not read the floor: started after the finalize it is not
  stopped, and meets formats it does not know. Install 0.8.1 first.
- **A skipped minor version is refused from the leases left, so only
  while they are there.** An app whose node leases were all removed with
  `wasmkeep cluster forget` starts on any version: nothing says what ran
  it. Once one node of an app has run 0.8, a new node id starts on 0.9.0
  even if the app's other nodes stopped on 0.7 and never ran 0.8; the
  node logs a warning that names their leases. A node stopped cleanly on
  0.7 and restarted under its own id is refused until it has run 0.8:
  install 0.8.1 on it and start it once, or start it under a new node
  id. If that id belonged to a node retired before the app ran 0.8,
  remove its lease with `wasmkeep cluster forget <id>`. A fenced lease
  (a node that crashed) is removed by no command and does not refuse a
  replacement under its id. Leases from before an app was deleted and
  created again (`deploy --recreate`) are not counted.
- **No finalize without a live node.** After an upgrade done with every
  node stopped, start the nodes, then finalize.
- **A serving node does not read the floor again.** A node already
  serving when a floor above it is written keeps serving. `cluster
  upgrade status` shows `node below the floor`, `cluster status` warns,
  and formats that need the floor stay refused while it is live; stop it
  yourself. A node draining and stopping on its own below the floor, and
  the log line of a node that sees a live node outside its range, are
  not built.
- **A node never stops because another node started.** The newcomer
  outside the range is the one that refuses.
- **An unreadable floor record has no repair command.** Every node of
  the app refuses to start and every writer refuses to write until the
  object is rewritten by hand with the exact body (`wasmkeep-floor 1`, a
  newline, `0.9`, a newline), or deleted, which reads as 0.8.
- **An object under `nodes/` that does not decode** is removed by no
  command. It delays every start by a lease's ttl plus grace and keeps
  the prefix's new apps from getting a floor at their first deploy,
  until it is deleted from the bucket by hand.
- **Edges are not rebalanced after a rolling upgrade.** A big room's
  edges stay on the nodes upgraded first, and the latency goal after the
  upgrade was not met ([measured](#measured)). Stopping every node is the
  recommended way for 0.9.0; rebalancing is planned for 0.10.0.
- **`cluster status` is slow with many released node leases**: 7 to 47 s
  on a prefix with about 140 of them, measured on 0.8.1. `cluster
  forget` removes them.
- **The removals above** were made on a reading of the 0.8.1 senders and
  unit tests of each refusal; the mixed-version check runs 0.9.0 beside
  0.8.1 nodes, which no longer send the removed forms.
- **0.x to 1.0** is not covered by these rules.

## Measured

The proof planned in
[ADR 0043, section 8](../adr/0043-rolling-upgrades.md#8-how-the-mixed-version-check-becomes-the-proof):

- **The mixed-version check**, `scripts/mixed-version.sh`
  ([releasing](../releasing.md#the-mixed-version-check)), passed 501 of
  501 checks from 0.8.1 to 0.9.0.

On a three-node cluster on 4-vCPU machines with a remote S3-compatible
bucket:

- **A rolling upgrade under load**, 0.8.1 to 0.9.0 one node at a time,
  under 4,500 clients at 10 messages/s:
  - nothing was lost: every publish was acknowledged in the end, and no
    message was delivered twice or skipped;
  - each node was `serving` again 16 to 25 s after its stop;
  - while a node restarts, its clients reconnect: for those minutes the
    delivery p99 is seconds to tens of seconds (3.6 to 23 s measured),
    and 1 to 2 % of publishes fail and must be retried by the client;
  - afterwards the node upgraded last held 4 of the 62 edges: nothing
    rebalances them on its own yet. Moved back by hand with `wasmkeep
    actor move`, to 17, 22 and 23 edges, the 300 s window after that had
    a delivery p99 of 0.90 to 1.05 s, against a goal of 1 s: not met.
- **Two hours in the mixed state**, two nodes on 0.9.0 and one on
  0.8.1: no acknowledged write lost, 6,694 writes in order.

So a rolling upgrade is safe for data but not yet proven to meet the
latency goal under load, and stopping every node is the recommended way
for 0.9.0. Rebalancing edges after a node rejoins is planned for 0.10.0.

## What else is new

The [changelog](../../CHANGELOG.md) lists it: `wasmkeep cluster verify`
([an upgrade, a verify and a trace](operating.md) walks through one),
optional [traces](../monitoring.md#traces), and `apart-from` and `spread`
for your own class names.
