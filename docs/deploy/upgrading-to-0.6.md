# Upgrade a cluster to 0.6.0

0.6.0 lets one process serve several apps behind its three ports, with
the [node file](../reference/node-file.md). Nothing changes for a node
that keeps serving one app: `serve --app` and its flags, its ports,
routes, exit codes and metrics stay as they were.

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

## Rolling back

Drain the shared process's other apps (`wasmkeep cluster drain` of their
node ids), restart it with one app, then downgrade. A binary older than
0.6.0 must never run while a process of several apps is live in its
cluster: it cannot check the sender of a hop, and reads several node ids
at one address as it always has.
