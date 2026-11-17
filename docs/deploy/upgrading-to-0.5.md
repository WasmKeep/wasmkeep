# Upgrade a cluster to 0.5.0

0.5.0 adds four things that change what a node or the CLI writes to the
bucket, and each is off until you ask for it: a batched class, a rate
limit, the recorded head, and the restore commands' markers. An app that
uses none of them keeps its manifests and ownership records byte for byte,
so upgrading the binaries alone changes nothing a 0.4.2 node reads.

The rule for everything below is the one in
[running a cluster](../cluster.md#rolling-upgrades-and-new-storage-functions):
**upgrade every node first, then deploy.** Each new feature announces a
word in the node's heartbeat, and the command that would depend on it
checks the words of the app's nodes.

## The words a 0.5.0 node announces

| Word | What it means | What needs it |
| --- | --- | --- |
| `flush` | links `storage.flush`, reads manifest format 5 | a component that imports `storage.flush`; a version with a batched class |
| `rate_limits` | reads manifest format 6 and enforces its limits | a version with a `limit` rule |
| `restore` | honours the fences of a whole-app restore | `backup restore --in-place` |
| `actor-restore` | supports live single-actor restores and delete markers | `actor restore` |
| `recorded-head` | reads, keeps and enforces ownership records of version 2 | `app format --enable` |

`wasmkeep cluster status` shows each node's words (the `WORDS` column,
`words` in `--json`) and warns when the live nodes of one app differ on
`recorded-head`.

## Manifest formats 5 and 6

- **Format 5** is written only for a version with a class whose `writes`
  is `"batched"`. A deploy of it is refused while a live node of the app
  lacks `flush`.
- **Format 6** is written only for a version with a rate limit (a
  `[limits.<name>]` table in `wasmkeep.toml`,
  [project file](../project-file.md#ingress-limits)). A deploy of it is refused while a live node of
  the app lacks `rate_limits`.
- **A strict class with no rate limit** is written as before: a version
  with neither keeps the manifest bytes, and so the version id, it had
  under 0.4.2. `"strict"` written in `wasmkeep.toml` makes the same
  version as leaving it out.
- A 0.4.2 node does not read formats 5 and 6. The deploy checks above
  cannot see a node started without `--node-id` or one that starts after
  the deploy, so do not start a 0.4.2 node on an app whose current
  version is in format 5 or 6.

## Components built with the new SDKs

A component built with the TypeScript or Python SDK of this release
imports `storage.flush` whether or not it calls it. It links only on a
node that announces `flush`, so deploy it after every node runs 0.5.0;
`deploy` refuses it before writing anything while a live node lacks the
word. Components built before, and Rust components that do not call
`storage::flush`, link as they did.

## The restore commands

- `wasmkeep backup restore --in-place` needs `restore` on every node
  registered for the app, offline ones included, and the app stopped. A
  node that lacks the word, or one you cannot upgrade, needs `--force`
  after it is stopped.
- `wasmkeep actor restore` needs `actor-restore` on every node registered
  for the app, offline ones included. The CLI checks it before it stages
  anything, and has no `--force`. The owning node checks it again before
  it runs the request, and fails closed: it does not run the restore when
  a registered node lacks the word, or when it cannot read the nodes'
  heartbeats within 10 seconds.

Upgrade every node and the CLI before you rely on either. A node that was
removed for good is dropped from the check with `wasmkeep cluster forget`.
See [back up and restore](backup-and-restore.md) and
[actor data access](../actor-data-access.md).

## The recorded head, and owner record version 2

`wasmkeep app format --enable --app TENANT/APP --bucket s3://BUCKET/PREFIX`
switches one app to ownership
records of version 2 (`wasmkeep-owner 2`), which carry the recorded head
([ADR 0035](../adr/0035-a-recorded-head-on-hand-over.md),
[the operator guide](../cluster.md#the-recorded-head-ownership-format)).
Until you run it, every record is version 1, as under 0.4.2.

- **Per app, and one way.** There is no command to turn it off.
- **Every node and the CLI on 0.5.0 first.** The command refuses while a
  node registered for the app does not announce `recorded-head`.
- **The app stopped and drained.** It refuses while any node of the app is
  live, and until the last of them stopped 40 seconds ago (the window for
  late writes).
- **A 0.4.x node reads version 2 as corrupt.** It will not take the
  record. Start only nodes of 0.5.0 or later on an enabled app, including
  any node outside the bucket's registrations.

## Descriptors and the default socket cap

At startup a node sizes its WebSocket capacity from its soft open-file
limit ([socket options](../reference/socket-limits.md)). The default of
50,000 sockets per node is only requested: at a soft limit of 65,536 the
effective cap is about 25,778, and the node logs a warning. To get more,
raise the limit (`LimitNOFILE=` in the service unit; the hard limit
bounds it) or set `--max-sockets-per-node` to what you need and give the
unit a limit that covers it. The startup log and the metrics show the
requested and the effective ceilings.

## Rolling back to 0.4.2

Going back to 0.4.2 is possible only while no version in format 5 or 6 is
the app's current one and `recorded-head` is not enabled for it.

To check it:

- `wasmkeep cluster status` prints the manifest format of each app's
  current version (the `FORMAT` column, `manifest_format` in `--json`).
  5 means a batched class, 6 a rate limit; deploy a version without them
  and wait until it is current before you stop a 0.5.0 node.
- `wasmkeep app format --app TENANT/APP --bucket s3://BUCKET/PREFIX`
  says whether the recorded head is enabled. Once it is, there is no way
  back to a binary before 0.5.0.

What was tested on the fleet: 0.4.2 to 0.5.0 node by node with writes
flowing and actors moved between old and new nodes; a deploy of a
batched class refused while a 0.4.2 node was live; `app format --enable`
refused while nodes ran the app; and, with a strict version current and
the recorded head not enabled, one node rolled back to 0.4.2 and forward
again, including actors written in batched mode. Not tested: rolling
back with in-place restore markers or `restores/` present, and with
format 6 (a rate limit) in the app's history.

## The order

1. Upgrade the binaries, one node at a time, and the CLI everywhere it runs.
2. Check `wasmkeep cluster status`: every node of an app announces the
   words in the table above.
3. Deploy, with batched classes, rate limits and 0.5.0 SDK components as
   you want them.
4. Only then, per app, enable the recorded head, if you want it.
