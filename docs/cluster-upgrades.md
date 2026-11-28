# Upgrading WasmKeep

Upgrading the `wasmkeep` binary itself (not your application: that is
[Deploying](cluster-deploying.md#deploying), which rolls from node to node
safely). Which way a cluster moves between two versions is what the
upgrading page of the release you move to says
([0.4.0](deploy/upgrading-to-0.4.md), [0.5.0](deploy/upgrading-to-0.5.md),
[0.6.0](deploy/upgrading-to-0.6.md), [0.7.0](deploy/upgrading-to-0.7.md),
[0.8.0](deploy/upgrading-to-0.8.md), [0.9.0](deploy/upgrading-to-0.9.md)):
it is the rule for that release, and this page does not override it.

- **From 0.8.1 to 0.9.0: stop every node, then start them all on
  0.9.0** (recommended), or one node at a time, which is supported and
  safe for data; then `wasmkeep cluster upgrade finalize`. Stopping is
  recommended because a rolling upgrade under load is not yet proven to
  meet the latency goal and leaves a big room's edges unbalanced until
  0.10.0 ([measured](deploy/upgrading-to-0.9.md#measured)). The steps
  are in [upgrading to 0.9.0](deploy/upgrading-to-0.9.md); the commands
  and what they check are [below](#versions-the-upgrade-floor-and-finalize).
- **Every other pair: stop every node, upgrade them all, start them
  all**, unless that release's page names the pair as one that can be
  upgraded one node at a time
  ([the steps](#with-every-node-stopped)). Without such a statement,
  WasmKeep does not support nodes of different versions in one cluster,
  even for the length of a rolling restart. The cluster then serves
  nothing between the last stop and the first start, so plan a short
  window.

## Versions, the upgrade floor and `finalize`

From 0.9.0 the tools for an upgrade one node at a time are in the binary
([ADR 0043](adr/0043-rolling-upgrades.md)). Whether a given pair of
versions may be upgraded that way is still what the release's upgrading
page says; these are the checks and the commands it uses.

- **A node refuses to start beside a version it cannot run with.** A node
  of minor version N starts only while every live node of its app runs
  N-1, N or N+1. Otherwise it writes no lease, logs the node and its
  version, and reports not ready. Upgrade one minor version at a time.
  With every node stopped it reads the node leases they left: an app
  with no floor whose leases all name a version below N-1 is refused
  too (a cluster stopped on 0.7 does not start on 0.9), and so is a node
  stopped cleanly and restarted under its own id over a minor version.
- **Each app has an upgrade floor**, `formats/<tenant>/<app>/floor` in
  the bucket: the lowest minor version allowed to serve it. A node below
  the floor does not start (0.8.1 reads it too). Without a record the
  floor is 0.8. A new app gets its floor at the version that creates it:
  the first deploy of an app that never ran (no deploy, no state, no
  registered node), or its first node. A deploy never sets the floor
  above the lowest version live under the bucket prefix, so the nodes
  that run the other apps can start into the new one. A backup carries
  the floor, and a whole-app restore leaves the higher of the target's
  and the backup's
  ([backup and restore](deploy/backup-and-restore.md#the-upgrade-floor-and-the-deploys-format)).
- **`wasmkeep cluster upgrade status`** shows, per app, each node's
  version, the floor, and one state: `one version`, `mixed`, `finalize
  pending` (every live node runs the new version, the floor is still the
  old one), `no live node`, `node below the floor`, or `refused`. It names
  the nodes that block a finalize. Exit code 0 for `one version`, 3 while
  an upgrade is under way, 1 when something needs attention.
  `wasmkeep cluster status` prints the same floor and state in one line
  per app.
- **`wasmkeep cluster upgrade finalize --yes`** ends the upgrade: it
  raises the floor to the version of the `wasmkeep` that runs it. It is
  refused, naming them, while a live node of the app is older. After the
  write it waits about 25 s and lists the nodes again; an older node live
  then is reported as an error and must be stopped. Without `--yes` it
  prints what it would do, with the same checks. Without `--app` it
  raises the floor of every app it can, names each app it refused with
  the reason and what to do, and exits with 1 if any was refused, even
  after raising the others.

```sh
wasmkeep cluster upgrade status --bucket s3://wasmkeep-demo
wasmkeep cluster upgrade finalize --bucket s3://wasmkeep-demo --yes
```

Until you finalize, any node may go back to the previous version, and
nothing the previous version cannot read is written. After it, no node of
an older version starts, and going back is not supported. A format a new
version introduces is refused until the app is finalized, and then still
while a live node does not announce its capability word. Commands that
write (`deploy`, `secrets`, `app delete`, `app format --enable`) are
refused from a `wasmkeep` older than the app's floor, or while a live
node of the app is more than one minor version away from it. The way
out is in the message: use a `wasmkeep` within one minor version of
every live node, and not older than the floor. Commands that only read
(`cluster status`, `cluster upgrade status`, `actor where`) warn in that
case and go on.

`finalize` counts an older node as stopped only when its lease is
`released` or `fenced`. A lease that is merely stale (the machine
vanished, or is cut off) is refused: run `wasmkeep cluster fence <id>`
first; a fence counts 15 s after the node's last write.

`finalize` also works with every node of the app stopped (from 0.9.2;
0.9.0 refuses an app with no live node). It is then accepted only when
every lease the app's nodes left is `released` or `fenced` and names the
version of the `wasmkeep` that finalizes or a later one: those leases
are the only record of which version last served the app. It is refused
when:

- a lease names an older version (a node that was not upgraded, or was
  stopped before the upgrade): start the app once on the new version,
  then finalize; or, for a released lease of a node that will not serve
  the app again, remove it with `wasmkeep cluster forget <id>` (a fenced
  lease cannot be forgotten: start a node of the app on the new version
  and finalize while it is live); or delete the app;
- a lease is stale but neither released nor fenced: run `wasmkeep
  cluster fence <id>`, wait 15 s, finalize;
- an object under `nodes/` does not decode (it may be a node of a
  version this one cannot read): start a node of the app and finalize,
  or delete the object from the bucket by hand once you know no running
  node writes it;
- the app has no lease at all: start a node of the app, then finalize.

A fence counts as a stop 15 s after the node's last write, the default
lease timings: the lease does not record a node's own. After the write,
the second listing also reports any object under `nodes/` that does not
decode, with exit code 1; the floor stays.

Finalize before you stop the last node of the previous version for the
next upgrade. A node refuses to start two minor versions above an app's
floor, even alone: restarted in place from 0.9 on 0.10 without a
finalize at 0.9, it does not start. Put 0.9 back, finalize, then
upgrade.

`node below the floor` means a node of an older version is live under a
floor above it. Either it started while the floor was being written, or
its read of the floor got no answer after its first lease write and it
keeps checking without serving; its `/_wasmkeep/ready` answer says which.
Stop it.

## Exceptions before 0.9.0

**One exception: from 0.4.0 to 0.4.1, one node at a time.** 0.4.1
changes nothing 0.4.0 reads or writes in the bucket (layout, manifests,
ownership records, alarm index), signs calls between nodes as 0.4.0
does, and adds one heartbeat word, `handoff`, which 0.4.0 ignores. Stop
one node cleanly, install 0.4.1, start it, and wait until `cluster
status` lists it live before the next. Until the last node is upgraded,
`cluster status` warns that live nodes run different versions: expected
during this upgrade. Sockets through the front
([ADR 0027](adr/0027-sockets-through-the-front.md)) need 0.4.1 on both
ends: a hand-off to a room owned by a 0.4.0 node gets a 503 with
`Retry-After: 1`, and a front on a 0.4.0 node sees an upgrade as the
plain `GET` it saw before. Deploy an app that hands sockets off once
every node runs 0.4.1.

**A second exception: from 0.4.1 to 0.4.2, one node at a time**, the
same steps. 0.4.2 changes no format 0.4.1 reads in the bucket (layout,
ownership records, history, alarm index, intents); what it adds, 0.4.1
never reads: the delete requests under `state/<t>/<a>/deletes/`,
`control/changed`, an app's tombstone and `recreated` marker, and
manifest format 4. It signs calls between nodes as 0.4.1 does, and adds
one heartbeat word, `ends`, which 0.4.1 ignores. A 0.4.2 node that
starts says hello to its peers (`/_wasmkeep/hello`); a 0.4.1 node
answers 404 and lists `nodes/` every period anyway. Until the last node
is upgraded:

- A deploy with an idle limit (`expire-after-idle`, manifest format 4)
  and [`app delete`](cluster-operations.md#deleting-an-app) are refused while a live node of
  the app runs 0.4.1. [`actor delete`](cluster-operations.md#deleting-one-actor) requests wait
  until a 0.4.2 node holds the actor, or finds it free.
- A component that imports `storage.delete-self-durable` cannot be
  loaded by a 0.4.1 node, and the 0.4.2 deploy does not check for it:
  that node stays not ready on the version ("cannot apply the deployed
  version"). From 0.5.0, deploy refuses it
  ([below](#rolling-upgrades-and-new-storage-functions)).
  Every TypeScript and Python component built with the 0.4.2 SDK imports
  it, called or not, as do Rust, Go, C and Zig components that call it.
  Deploy them once every node runs 0.4.2.
- Use the 0.4.2 CLI. A drain, an undrain or a move from the 0.4.1 CLI is
  read within a minute instead of 3 s; the 0.4.1 CLI cannot read a
  version with an idle limit (it refuses to deploy over one), and knows
  nothing of tombstones.
- A 0.4.1 node that starts is seen by 0.4.2 peers within 30 s (sooner
  over TLS), instead of a period.

Going back to 0.4.1 is the same, one node at a time, as long as no
version with an idle limit is current and no component that needs 0.4.2
is deployed; a 0.4.1 node cannot serve either. Every other pair of
versions before 0.8.1 follows the rule [above](#upgrading-wasmkeep).

## With every node stopped

1. `wasmkeep cluster status --bucket ...`: note the nodes it lists as live.
2. Stop every node cleanly (`systemctl stop wasmkeep`, `SIGTERM`). Each
   finishes its requests and releases its actors; a clean stop takes up to
   about 29 s.
3. Wait until `cluster status` exits with 1 (no live node), about 10 s after
   the last stop. A node still listed as live is still running the old
   binary: stop it first. This check does not depend on version strings.
4. Install the same new binary on every node.
5. Start every node, then run `cluster status` again: every node live, one
   version in the summary, no warning.

Going back to the previous binary is the same procedure, except across
the 0.4.0 layout change: `cluster migrate --rollback` (0.8.x and earlier;
removed in 0.9.0) works only until the first node starts after the
migration, and never after `--remove-old --apply`
([upgrading to 0.4.0](deploy/upgrading-to-0.4.md)). Builds made from
a git checkout report the release they follow and their commit
(`0.9.2+4f1c2a9e7b3d`), a release's tag the release alone, so the version
column tells two builds of `main` apart. A build without git (a source
tarball, a container image built locally) reports the release alone:
step 3 is what proves no old node is left.

**What goes wrong if old and new nodes run together** (each is a reason
for the rule; there may be others nobody has listed):

- **Every hop, from 0.3 to a later version**: nodes after 0.3.0 name their
  app on every hop to another node, and refuse a hop that names none: a
  request forwarded, an upgrade relayed or a call made by a 0.3 node gets
  503, "refused: sent by a node of an older version". The other way, the
  0.3 node fails the newer node's signatures: calls to `internal` classes,
  moves and alarm hand-offs fail, and other calls run there as requests
  from outside, with no calling actor.
- **Starting next to the other version**: a node started without `--app`
  names its app `default/default` after 0.3.0 and `default/`
  before. A newer node refuses to start while a 0.3 one is live on the bucket prefix,
  asking to stop every node and upgrade them together; a 0.3 node started
  next to a newer one refuses too, saying another app of the tenant runs
  there.
- **The layout of actor state** (0.4.0, [ADR 0022](adr/0022-app-owned-state-and-the-one-time-migration.md)):
  0.4.0 keys every app's actors under `state/<tenant>/<app>/`, and a
  bucket written by an earlier version is migrated once, with every node
  stopped, by `wasmkeep cluster migrate` (removed in 0.9.0: migrate with
  0.8.1 first). A 0.3 binary refuses a bucket
  being migrated or migrated; 0.2's `serve --app` and `deploy` cannot read
  the `current` token `3` and stop; a 0.2 `serve --actor` node finds a
  tombstone over each migrated actor's old record and answers 503 for it
  (an actor it creates under the old keys is not seen by 0.4.0: running
  0.2 on a migrated bucket is unsupported). Going back after the first
  0.4.0 start is not supported ([Upgrade a cluster to 0.4.0](deploy/upgrading-to-0.4.md)).
- **Calls between nodes**: the format of signed calls can change between
  versions.
  Old and new nodes refuse each other's calls (logged as "bad, expired or
  misaddressed signature (... a node of an older version?)"), so calls to
  `internal` classes on the other kind fail, and a call cycle across them
  ends in a timeout instead of `reentrant`.
- **Formats in the bucket**: a version can change how heartbeats,
  ownership records or alarm index entries are written. A node of another
  version may skip what it cannot read, so it may not see its peers'
  ownership or alarms: actors refused, alarms late or missed.
- **No other guard before 0.9.0**: the deploy `seq`
  ([ADR 0008](adr/0008-deploy-and-rolling-upgrades.md)) keeps an actor
  from running older *application* code, not from an older WasmKeep.
  Before 0.9.0 nothing compares WasmKeep versions between nodes; the
  version in each heartbeat is only reported, by `cluster status`. From
  0.9.0 a starting node refuses a live node more than one minor version
  away ([above](#versions-the-upgrade-floor-and-finalize)); a node of
  0.8.1 or before still checks nothing about the other nodes' versions.

## Rolling upgrades and new storage functions

From 0.5.0, every node announces the heartbeat word
`flush`: it has `storage.flush`
([ADR 0034](adr/0034-batched-plain-writes.md)), which a node of 0.4.2
or before cannot link. Before it writes anything, `wasmkeep deploy`
reads the imports of each component of the version it would make
current (those it is given, and those it keeps or a `--rollback` brings
back, read from the bucket when a live node lacks a word), and refuses,
naming the nodes and the classes, while a live node of the app lacks
what a component imports:

- a component importing `storage.flush` needs every live node to
  announce `flush`;
- a component importing `storage.delete-self-durable` needs every live
  node to announce `ends` (0.4.2 and later). 0.4.2 accepted that deploy,
  and a 0.4.1 node stayed not ready on it; this refusal is new, for every
  class.

There is no `--force`: such a node would stay not ready on the version.
TypeScript and Python components import the whole `storage` interface,
so every one built with the SDK of that release imports `flush`, called
or not; Rust, Go, C and Zig components import what they call.
**Upgrade every node of the app, then deploy.**

What the check does not cover, during a rolling upgrade:

- It runs in `deploy`, from the heartbeats in the bucket, not on the
  nodes. A node started without `--node-id` writes no heartbeat, so the
  check cannot see it, nor a node that starts after the deploy.
- A node that misses it cannot link the component and stays not ready on
  the version ("cannot apply the deployed version"), but it still takes
  actors: their events are refused until it is upgraded. The data is
  safe (it uploads nothing it could not run); availability is not.

A class's `writes` (`strict`, the default, or `batched`) is part of its
version: `strict`, written or not, makes the same version and manifest
as before. `batched` is accepted by `deploy`: from 0.5.0 on, a
version with a batched class is written as manifest format 5, which a
deploy refuses while a live node lacks `flush` (and, as for formats 4
and 3, `ends` and `front`).

## The recorded-head ownership format

A successful upload does not mean every S3-compatible bucket lists it at
once. When an actor changes hands, a listing that lags by a few seconds
can show a complete but older history: the next node would restore it
and write on, leaving acknowledged writes on an abandoned branch. The
recorded-head format ([ADR 0035](adr/0035-a-recorded-head-on-hand-over.md))
closes that gap for complete hand-overs (hibernation, drain, move, a
clean stop): the releasing node writes the position it acknowledged into
the actor's ownership record (`wasmkeep-owner 2`), and the next node
activates only once its restore provably reaches it. While the listing
does not show it yet, the node lists again (at most ten times within
15 s), then answers **503 with `Retry-After`**, keeps the requirement in
the record and waits 1, 2, 4, 8, 16, then 30 s before that actor's next
try. Nothing is served from the older history.

It is off until you enable it, one app at a time:

```bash
wasmkeep app format --app acme/shop --bucket s3://my-bucket/prefix           # show
wasmkeep app format --app acme/shop --bucket s3://my-bucket/prefix --enable  # switch
```

Before `--enable`, upgrade every node of the app to 0.5.0 or later (each
announces `recorded-head`; `cluster status` warns while an app's nodes
differ), then **stop and drain the app**. The command refuses, writing
nothing, while a registered node of the app does not announce
`recorded-head` (upgrade it, or `cluster forget` it once stopped), while
one still runs the app, or within 40 s of the last one stopping. It then
writes `formats/<tenant>/<app>/recorded-head`, outside the app's own
prefixes: deploys, rollbacks, `app delete --recreate` and `backup
restore` keep it. Start the nodes again: their records are converted on
first use, starting with no recorded position.

**There is no way back.** An enabled app cannot return to version 1
records, and binaries older than 0.5.0 cannot read version 2 ones (they
treat them as unreadable, never as free). Rolling back the binary means a
release that still reads and enforces the format. An enabled app refuses
service on any node while a live node of it lacks `recorded-head`, so a
mistakenly started old binary stops the app rather than corrupting it:
stop that node.

What it does not cover: a node that crashes after writing (it records no
head), where the next owner relies on the bucket's strongly consistent
listing, as before. Nor does a clean stop always count as complete: a
node gives each actor 3 s for its release, after which it writes its own
lease `released` and its remaining actors take the crash path (their
last recorded floor still holds). `cluster diagnose` checks the bucket's
semantics, not this setting.
