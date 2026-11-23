# Glossary

The words the docs use, in a line or two each, with the page that explains them. Terms are in
alphabetical order.

**actor**: one named instance of a class, with its own SQLite database and its own queue of events,
run by one node at a time. The first event to a name creates it. [Concepts](concepts.md#addresses).

**app**: a deployed set of classes, with an optional front and its static assets, named
`TENANT/APP` and served by `serve --app`. [ADR 0021](adr/0021-the-app-model.md).

**apart**: how placement keeps a room and its pusher on different nodes, so the pair does not share
one node's CPU. Counted by `wasmkeep_placement_apart_total`.
[ADR 0041](adr/0041-placement-at-wake-up.md#amendment-2026-11-22-keeping-a-room-and-its-pusher-apart).

**barrier**: the point where a hold has nothing in flight and nothing uncaptured, so everything the
actor committed is in the bucket. A hand-over or a snapshot waits for it.
[ADR 0034](adr/0034-batched-plain-writes.md#6-barriers-two-ways-to-end-a-hold).

**batched writes**: a class mode (`writes = "batched"`) where plain writes are uploaded behind the
events instead of before the next one. Faster, with a bounded window of unacknowledged writes.
[What saved means](what-saved-means.md#opt-in-to-batched-plain-writes).

**change feed**: a read-only export of changed actor states from the bucket's history to local
Parquet files, with `wasmkeep feed`. It contacts no node. [Querying the change feed](change-feed.md).

**class**: one compiled component, given a name when it is served or deployed. Each actor belongs to
one class. [Concepts](concepts.md#addresses).

**drain**: asking a node to stop taking actors and hand its own to other nodes, before a restart or
an upgrade. [Draining a node](cluster.md#draining-a-node).

**edge**: in a broadcast room, the class that holds clients' sockets and sends them messages, up to
a cap per edge. [ADR 0040](adr/0040-directory-broadcast-groups-and-the-reconnecting-room.md#5-the-edge).

**front**: the stateless component of an app that answers every path outside `/actor/`, one
instance per request, and calls actors. [The front](concepts.md#the-front).

**generation**: the number an actor's ownership record carries for each hold; history written under
a newer generation wins over an older one. [How it works](cluster.md#how-it-works).

**head**: the last state the bucket acknowledged for an actor. Snapshots and restores are taken at
the head. [ADR 0035](adr/0035-a-recorded-head-on-hand-over.md).

**hibernate**: an idle actor leaves memory (after `--idle-timeout-secs`, 30 s by default). The next
event wakes it, here or on another node. [Life cycle](concepts.md#life-cycle).

**history**: the segments and snapshots of an actor in the bucket, from which any node restores it.
[History in the bucket](cluster.md#history-in-the-bucket).

**hold**: one node's ownership of one actor, from the moment it takes it until it gives it up or
loses it. The ownership record names the holder and a generation.
[How it works](cluster.md#how-it-works), [ADR 0020](adr/0020-one-lease-per-node.md).

**lease**: a node's right to run actors, renewed by writing its heartbeat to the bucket. A node
without a valid lease is not ready and takes no actor.
[ADR 0020](adr/0020-one-lease-per-node.md), [How it works](cluster.md#how-it-works).

**node**: one `wasmkeep serve` process in a cluster, with a node id, a lease and a share of the
actors. [Running a cluster](cluster.md).

**park**: a room that hibernates with sockets open keeps its ownership record naming its node, so
its clients stay connected and the next event wakes it there. [ADR 0024](adr/0024-socket-home.md).

**placement**: the choice of which node wakes an actor nobody holds: a node keeps it while it is
lightly loaded, and otherwise sends the event to a lighter node.
[Placement at wake-up](failover-and-scaling.md#placement-at-wake-up),
[ADR 0041](adr/0041-placement-at-wake-up.md).

**pusher**: in a broadcast room, the class that pulls from the room and drives the fan-out, so
delivery does not queue behind publishes in the room's mailbox.
[ADR 0040](adr/0040-directory-broadcast-groups-and-the-reconnecting-room.md#13-the-pusher-amended-2026-11-20).

**relay**: in a broadcast room, a stateless class between the room and many edges: one call from the
room reaches the edges through it. [ADR 0040](adr/0040-directory-broadcast-groups-and-the-reconnecting-room.md#8-the-relay-layer).
Also the node that joins a client's socket to the owner on another node
([Concepts](concepts.md#websockets)).

**retention window**: how long old history stays in the bucket before garbage collection may delete
it (`--history-retention-secs`, one hour by default), so a bug can be noticed before history is gone.
[History in the bucket](cluster.md#history-in-the-bucket), [ADR 0012](adr/0012-bucket-garbage-collection.md).

**room**: an actor that keeps a numbered log of messages and the sockets of its clients, from the
broadcast SDK; also any actor holding sockets for a group.
[ADR 0040](adr/0040-directory-broadcast-groups-and-the-reconnecting-room.md).

**segment**: one object of an actor's history in the bucket: the writes of one event, or of a batch
of events in a batched class. [Replication](cluster.md#replication),
[ADR 0007](adr/0007-replication-protocol.md).

**snapshot**: a whole copy of an actor's database at the head, taken every 64 segments and when an
actor hibernates, so a restore reads one object. [History in the bucket](cluster.md#history-in-the-bucket).

**spread**: waking a big room's edges over the live nodes, so one node does not run every edge's
send loop. Counted by `wasmkeep_placement_spread_total`.
[ADR 0040](adr/0040-directory-broadcast-groups-and-the-reconnecting-room.md#14-edges-spread-over-the-nodes-amended-2026-11-22).

**tenant**: the owner of apps: a team, or a client of the operator, not a customer of your app (that
is an actor name). [Concepts](concepts.md#addresses).

**tombstone**: the record written first when an app is deleted. From then nodes refuse to take its
actors and stop serving it. [Deleting an app](cluster.md#deleting-an-app),
[ADR 0028](adr/0028-actors-that-end.md).
