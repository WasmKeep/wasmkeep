# Upgrade a cluster to 0.6.0

0.6.0 lets one process serve several apps behind its three ports, with
the [node file](../reference/node-file.md). Nothing changes for a node
that keeps serving one app: `serve --app` and its flags, its ports,
routes, exit codes and metrics stay as they were. Other changes of
0.6.0 that every node sees are under [Other changes](#other-changes).

Use the 0.6.0 CLI during the upgrade. Each feature below waits for a
capability word that 0.6.0 nodes announce in their heartbeat; a live node
without it blocks the feature, and nothing is forced:

| Word | Needed for |
| --- | --- |
| `multi-app` | a process of several apps (stage 2 below) |
| `assets` | a version with [static assets](../static-assets.md) |
| `blobs` | a version that imports `wasmkeep:actor/blobs` ([files](../files.md)) |
| `place` | being a target of [placement at wake-up](#placement-at-wake-up) |

The package stays `wasmkeep:actor@0.2.0`: `blobs` is an added interface,
and a component that does not import it links on any node.

## Two stages

1. **Upgrade with one app per process.** Upgrade every node, one at a
   time, as for any release, still serving one app (`serve --app`, or a
   one-entry node file). From this stage on every node announces
   `multi-app` in its heartbeat, names its sender in every hop it signs,
   and signs every forward it can.
2. **Enable a second app.** Once every node of every app (offline ones
   included) runs 0.6.0 and uses the cluster port, add a second `[[app]]`
   to one node's file and restart that process. Its first app keeps its
   node id; the second app's node id joins that app as a new node, or as
   an existing node id moved from another process after
   `wasmkeep cluster drain` there, its data directory named in the entry.

A process of several apps refuses to start while a live node lacks
`multi-app`, naming it. If an older node starts later, every runtime of
the process reports not ready (`/_wasmkeep/ready` answers 503, the
metrics page names the reason), logs which node, and keeps serving its
actors: take the older node out, or upgrade it.

`wasmkeep cluster status` shows the node ids of each process (the
PROCESS table); `wasmkeep cluster drain --process ID` drains every node id
a process runs.

## Load-balancer health checks

Before enabling the second app, update the load balancer's probes.
Health checks on the public port must send one of the app's configured
hostnames, for example `Host: shop.example.com`, with
`GET /_wasmkeep/ready`. This checks that app, so probe each app separately
if the balancer tracks their readiness independently. An unknown host
returns 421; a missing Host returns 421 on HTTP/1.0 or 400 on HTTP/1.1.
The metrics port (`[listen].metrics` / `--metrics-listen`) serves
`/metrics` only: `/_wasmkeep/ready` there returns 404, not readiness.
The public port's `/_wasmkeep/health` works without an app hostname but
checks process liveness only; it is not a readiness substitute.

See the [front-door examples](front-door.md).

## Rolling back

Drain the shared process's other apps (`wasmkeep cluster drain` of their
node ids), restart it with one app, then downgrade. A binary older than
0.6.0 must never run while a process of several apps is live in its
cluster: it cannot check the sender of a hop, and reads several node ids
at one address as it always has.


## Placement at wake-up

0.6.0 nodes publish two more fields on their heartbeat's load line
(`actor_room`, `socket_room`), which older readers skip, and announce
`place` ([ADR 0041](../adr/0041-placement-at-wake-up.md)). A node places
a wake-up only on a node that announces `place`, and counts only those
nodes in its budget: during a rolling upgrade older nodes keep every
actor they wake and receive nothing placed, while upgraded nodes place
among themselves. Nothing refuses to start. Placement is on by default;
`serve --no-placement`, or `placement = false` under `[process]` in the
node file, turns it off for a process before or after the upgrade. A
downgrade needs nothing: an older node ignores the fields and the word.

## Static assets

Upgrade every app node before deploying [static assets](../static-assets.md).
Nodes announce `assets` and read manifest formats 1 through 7. Only
versions with assets use format 7; versions without them retain their
previous format and identity. Deploy and rollback refuse a version with
assets while any live app node lacks `assets`; there is no force option.
Older CLIs cannot read format 7 and stop without deploying or collecting.

If an older node starts later, it cannot load format 7 and reports not
ready (503) until upgraded. Before downgrading binaries, deploy without
assets (`--no-assets`, or remove `[assets]` from the project), wait for
every node to apply it, and only then downgrade. Do not roll back to a
retained format 7 version on older nodes. Existing `_assets` objects may
remain; older version collectors do not treat `_assets` as a version.

## Files

Upgrade every node of an app before deploying a version whose actors or
front use [files](../files.md). Nodes of 0.6.0 announce `blobs`; a
deploy (or a rollback) of a version that imports
`wasmkeep:actor/blobs` is refused while a live node of the app lacks
it, with no force option. A TypeScript or Python component that does
not use files imports nothing new and deploys at any time.

**Do not downgrade a node below 0.6.0** once an app it serves has had a
version that uses files. An older node deletes an actor without the
sweep record and the new file namespace a delete now writes: 0.6.0
nodes notice it (the next start of that actor draws a fresh namespace,
and the sweep removes nothing it cannot prove deleted), but files of
the deleted actor would stay until the bucket's lifecycle rule takes
them. Nothing can make an older binary refuse; this is the rule to
follow.

Also new on the public port: `/_wasmkeep/blobs/v1/` answers node links.
Uploads are node links in 0.6.0, 16 MiB at most on a bucket
([why](../files.md#links)). Back up apps that use files with the 0.6.0
CLI ([files and assets](backup-and-restore.md#files-and-assets)).

## Other changes

- **Logs go to stderr.** Every command, `wasmkeep serve` included,
  writes its logs to stderr; stdout carries only a command's own output.
  A service that collected a node's logs by redirecting stdout alone
  needs stderr too (systemd's journal takes both).
- **`x-wasmkeep-outcome: unknown`.** A host answer for a request that
  may have run (a forward whose answer was lost, an answer withheld
  during a rolling upgrade) carries this header, no `Retry-After`, and a
  body ending `the request may have run`. Clients and proxies that retry
  on a 503 should not retry such an answer blindly unless the request is
  idempotent.
- **Ready waits for the tombstone check.** `/_wasmkeep/ready` answers 503
  until the node has checked whether its app was deleted (the reason joins
  `wasmkeep_not_ready`), so a probe may fail for a moment longer after a
  start. Allow for it in the load balancer's grace period.
- **Hibernation fix.** An actor's state is dropped before the actor
  leaves the registry; nothing to do.
