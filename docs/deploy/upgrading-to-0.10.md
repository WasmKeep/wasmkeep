# Upgrade a cluster to 0.10.0

0.10.0 is the second minor release a cluster can move to one node at a
time, with the checks for it in the binary. The supported pair is **0.9.x
to 0.10.0**. The upgrade ends with one command, `wasmkeep cluster upgrade
finalize`: **until you run it, any node may go back to 0.9, and what 0.10.0
adds to the bucket stays off.** After it there is no way back.

What 0.10.0 changes for an operator, in one list:

- A big room's edges **return to their home node after a node rejoins**,
  so a rolling upgrade no longer leaves the node upgraded last with few
  of them ([edge rebalancing](#edge-rebalancing)).
- Creating an actor writes **no per-actor lease marker** once the app is
  finalized at 0.10, and a restore writes one generation floor per app
  ([the generation floor](#the-generation-floor-and-cluster-markers-clean)).
- **Released node leases are removed by the live nodes**, a day after
  their release ([lease removal](#released-node-leases-are-removed)).
- A node **renews its lease every 2 s** instead of every 3 s
  ([the renewal cadence](#the-lease-renewal-cadence)).

The rules are in [ADR 0043](../adr/0043-rolling-upgrades.md); the commands
are described in [Upgrading WasmKeep](../cluster-upgrades.md). This page
is the rule for this release. It assumes you have read
[the page for 0.9.0](upgrading-to-0.9.md), whose finalize it repeats.

## Before you start

- **Every node runs 0.9.x, and the app is finalized at 0.9.** Check with
  `wasmkeep cluster upgrade status`: the state is `one version` and the
  floor is 0.9. A cluster on 0.8.1 goes to 0.9 first, by
  [its page](upgrading-to-0.9.md), and finalizes there. A 0.10 node
  refuses to start two minor versions above an app's floor, even alone:
  started over a 0.8 floor, it does not start. Put 0.9 back, finalize,
  then upgrade.
- **Versions may differ by one minor.** A node of minor version N starts
  only while every live node of its app runs N-1, N or N+1; 0.10 beside
  0.9 is allowed, 0.10 beside 0.8 is not. A cluster stopped on 0.8 does
  not start on 0.10 either.
- **Use the 0.10.0 `wasmkeep` for the commands on this page.** It runs
  beside 0.9 and 0.10 nodes, and `finalize` has to be run with it.
  [Commands that write](upgrading-to-0.9.md#cli-versions) are refused
  from a `wasmkeep` outside one minor version of every live node, or
  older than the app's floor.
- **Do not add the new node-file keys or flags yet**: `[process]
  rebalance`, `--no-rebalance`, `[process] lease-removal` and
  `--no-lease-removal`. A 0.9 `serve` refuses them, so they would stop
  you from going back ([Going back](#going-back)).
- **A single node is not a rolling upgrade.** Its restart is downtime:
  stop it, install 0.10.0, start it, then finalize.

## With every node stopped

The cluster serves nothing between the last stop and the first start,
so plan a short window. This is the way if your clients cannot take a
reconnect, since each node's restart in the rolling way closes its
sockets.

1. **Stop every node** cleanly (`systemctl stop wasmkeep`, `SIGTERM`),
   and wait until `wasmkeep cluster status` exits with 1 (no live node).
2. **Install 0.10.0** on every node.
3. **Start every node**, and wait until each is `serving`.
4. **Finalize** ([below](#finalize)). It needs a live node, so it comes
   after the start.

The general steps are in
[Upgrading WasmKeep](../cluster-upgrades.md#with-every-node-stopped).

## One node at a time

Supported and safe for data. While each node restarts, its clients
reconnect to the others. This page states no latency figure for a rolling
upgrade under load: if the delivery times of a big room matter during the
window, stop every node instead. For each node in turn:

1. **Drain it**, and wait until `cluster status` shows it `drained`:

   ```bash
   wasmkeep cluster drain node-b --bucket s3://wasmkeep-demo
   wasmkeep cluster status --bucket s3://wasmkeep-demo
   ```

   It hands its actors to the other nodes and keeps forwarding
   ([Draining a node](../cluster-operations.md#draining-a-node)).
2. **Stop it** cleanly. `cluster status` then lists it as `no (released)`.
3. **Install 0.10.0.**
4. **Withdraw the drain request** so the new process takes actors from its
   first heartbeat (`wasmkeep cluster undrain node-b`), or start the node
   and run `undrain` then.
5. **Start it**, and wait for `serving`: `/_wasmkeep/ready` answers 200,
   and `cluster status` lists it live, on 0.10.0.

While nodes of both versions are live, `cluster status` warns that live
nodes run different versions, and `wasmkeep cluster upgrade status` says
`mixed` and exits with 3. Both are expected. Keep the mixed state short:
it is meant to last the length of an upgrade, not days.

**Edges stay where they are while two minor versions are live.** The
rebalance is paused for the whole mixed state (`cluster status` shows the
app's `REBALANCE` as `paused (mixed versions)`), and no node moves an edge
before the last one runs 0.10. Do not wait for `balanced` between the
nodes of the upgrade itself. Once the last node serves, the rebalance
starts by itself ([below](#edge-rebalancing)).

## Finalize

Once every node runs 0.10.0:

```bash
wasmkeep cluster upgrade status --bucket s3://wasmkeep-demo
wasmkeep cluster upgrade finalize --bucket s3://wasmkeep-demo        # what it would do
wasmkeep cluster upgrade finalize --bucket s3://wasmkeep-demo --yes
```

The command, its states and its exit codes are those of
[0.9.0's finalize](upgrading-to-0.9.md#finalize), with 0.10 where it says
0.9: it is refused, naming them, while a live node of the app is older (a
stale lease needs `wasmkeep cluster fence <id>` first); it raises the
app's floor, `formats/<tenant>/<app>/floor`, to 0.10; it waits about 25 s
and lists the nodes again; and it needs a live node. There is no
`--force`. Finalize while 0.10 nodes are live, and before you stop the
last node of 0.9 for the next upgrade.

### What finalizing changes

- **The floor** is 0.10: a 0.9 node that starts into the app reads it,
  writes no lease and stops.
- **Nodes stop writing per-actor lease markers** and a restore writes a
  generation floor
  ([next section](#the-generation-floor-and-cluster-markers-clean)). A node
  that served through the finalize changes path without a restart.
- **Nothing else needs the finalize.** Edge rebalancing starts as soon as
  every live node runs 0.10, and the removal of released leases works
  once the app is finalized at 0.9.

### Going back

**Until the finalize, any node may return to 0.9**, by the same steps:
drain, stop, install 0.9, start. First remove what 0.9 does not know,
since it refuses to start on an unknown node-file key or flag:

- the `[process]` keys `rebalance` and `lease-removal` of the
  [node file](../reference/node-file.md);
- the flags `--no-rebalance` and `--no-lease-removal`.

**After the finalize, going back is not supported.** Nothing lowers or
deletes the floor.

## The generation floor, and `cluster markers clean`

Up to 0.9, creating an actor wrote one small object beside its ownership
record, `state/<tenant>/<app>/leases/<class>/<name>` (the per-actor lease
marker), and a whole-app restore wrote one per actor to keep each actor's
next generation above its deleted history. From 0.10, **once the app's
floor is 0.10**, a node writes none, and a restore writes one object per
app, `state/<tenant>/<app>/generation-floor`
([ADR 0045](../adr/0045-the-generation-floor-of-a-restored-app.md)). Before
the finalize, a 0.10 node writes exactly what 0.9 does, and reads the
upgrade floor once more for each actor it creates.

The markers written before stay, and nodes still read them when an actor
is first acquired. **Run `wasmkeep cluster markers clean` once the app is
finalized at 0.10**, to remove them and let nodes stop reading:

```bash
wasmkeep cluster upgrade status --bucket s3://wasmkeep-demo
wasmkeep cluster markers clean --bucket s3://wasmkeep-demo --app acme/shop         # counts, writes nothing
wasmkeep cluster markers clean --bucket s3://wasmkeep-demo --app acme/shop --yes
wasmkeep cluster verify --bucket s3://wasmkeep-demo --app acme/shop                # lease markers left: 0
```

- It is refused while the app's floor is below 0.10, and while a live node
  of the app does not announce `genfloor` (a node older than 0.10).
- A marker whose generation the actor's ownership record carries is
  deleted. One that is the only carrier of a restore's generation is
  first folded into the app's generation floor, then deleted.
- It may run with nodes live and be interrupted and run again. Without
  `--yes` it prints the counts and exits with 2. A marker written
  meanwhile by a node that had not yet read the finalized floor is
  reported: run the command again.
- **`markers none`** is what the generation floor says when no marker is
  left: nodes may ignore any marker, and stop reading them. A node keeps
  that in memory, so after a restart its first created actor reads the
  marker's key once more. `cluster verify` prints the floor and whether
  markers are `none` or kept, and counts markers left under `markers none`
  as redundant, not as a problem.

The cleanup is not needed for the cluster to work: it removes the
leftover objects and the read. See
[removing the per-actor lease markers](../cluster-operations.md#removing-the-per-actor-lease-markers).

## Released node leases are removed

A node that stops cleanly leaves its lease, marked `released`. Up to 0.9
the lease stayed until `wasmkeep cluster forget` removed it, so a cluster
whose nodes start under new ids piled them up under `nodes/`.

From 0.10 the live nodes remove them
([ADR 0020, amendment of 2026-11-27](../adr/0020-one-lease-per-node.md#amendment-of-2026-11-27-released-leases-removed-by-the-live-nodes)):

- **On by default.** The lowest-id live node of each app removes its
  app's `released` leases **a day after their release**, with the steps
  `cluster forget` takes: the mark, written only over the lease it read,
  then its delete, and never a lease an ownership record still names.
- **After the app is finalized at 0.9**, and never beside a live node
  older than 0.9. The leases of the nodes you stopped for the upgrade
  therefore go a day after it, without a command.
  `wasmkeep_node_lease_removals_total{outcome="blocked"}` rises on the
  node that would remove them while the app is not finalized.
- **To turn it off**, run `serve --no-lease-removal` or set `[process]
  lease-removal = false` in the node file, alike on every node (a node
  with it off is still the one chosen if its id is the lowest). Remove the
  setting before going back to 0.9.
- `cluster forget <node>` is still the way for an app with no live node,
  or with the removal off. A fenced lease (a node that crashed) is
  removed by no command.

## Edge rebalancing

A big room's edges (the built-in `edge`, or a class with `spread = true`)
each have a home: the node the rendezvous choice names among the nodes
that serve. From 0.10 a node holding one away from its home moves it
there, with the move of [ADR 0017](../adr/0017-moving-one-actor.md), once
no node drains, every live node serves and runs one minor version, and
the home has served for three heartbeat periods and has room
([ADR 0040, section 14](../adr/0040-directory-broadcast-groups-and-the-reconnecting-room.md#amendment-2026-11-26-edges-return-home)).

- **Paused while two minor versions are live**, while any node drains
  or does not serve, and for ten minutes after a node of the app stopped
  or went quiet, so nothing moves while a node is down between the steps
  above, drain withdrawn or not
  ([amendment of 2026-11-28](../adr/0040-directory-broadcast-groups-and-the-reconnecting-room.md#amendment-2026-11-28-no-move-while-a-node-may-come-back)).
  A node removed for good with `wasmkeep cluster forget` stops counting
  at once. `cluster status` shows each node's `AWAY` count and each app's
  `REBALANCE`: `balanced`, `rebalancing`, or `paused` with why.
- **Paced**: two moves in flight and 500 sockets closed per heartbeat
  period, per app on each node. The clients of a moved edge reconnect
  once more, and an upgrade for an edge being moved is answered `503` with
  `Retry-After: 1`.
- **`wasmkeep actor move` of an edge off its home is undone** a minute
  after its take. To place edges by hand, start the nodes with
  `--no-rebalance` (or `[process] rebalance = false`).
- **A rolling restart** of 0.10 nodes waits for `balanced` between nodes,
  for at most two minutes
  ([draining a node](../cluster-operations.md#draining-a-node)).

## The lease renewal cadence

A node renews its lease **2 s after its last acknowledged write**,
instead of one heartbeat period (3 s) after the last send, each attempt
allowed 2 s and a failed one tried again at once
([ADR 0020, second part of the amendment](../adr/0020-one-lease-per-node.md#second-part-same-amendment-the-renewals-budget)).
This closes a way for one slow request to run a node's lease out (the
"this node's lease ran out" warning). The cost is **more writes to
`nodes/`**: one per node every 2 s instead of every 3 s, half as many
again. On a bucket that bills per request, or limits requests per second,
that is the line to watch: 0.5 writes a second for each node.

## What the last 0.9.x carries

Until the finalize, nodes of the last 0.9 release stay valid members of
the cluster, and are what you return to. Beside 0.10 they:

- write per-actor lease markers, and do not announce `genfloor`. The
  marker cleanup is refused while such a node is live;
- never rebalance edges, and do not publish the `AWAY` count, so a mixed
  cluster shows `paused (mixed versions)`;
- never remove released node leases (a 0.10 node does, once the app is
  finalized at 0.9);
- renew their lease every 3 s;
- refuse the node-file keys and flags that are new in 0.10 (above).

0.10.0 removes nothing from the formats of 0.9: markers are still read,
because 0.9 writes them.

## What else is new

The [changelog](../../CHANGELOG.md) lists it: `wasmkeep actor move` no
longer waits for a 30 s timeout, `cluster drain --process` works for a
process that names none, and `cluster status` reads the bucket
concurrently.
