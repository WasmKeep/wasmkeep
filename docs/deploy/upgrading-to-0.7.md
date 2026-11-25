# Upgrade a cluster to 0.7.0

0.7.0 moves a room's delivery to a fifth class, the pusher, and adds
metrics, `wasmkeep actor where` and `wasmkeep cluster forget` for released
node leases. A node that serves no big room needs nothing beyond the
binary: its ports, routes, exit codes and the package
`wasmkeep:actor@0.2.0` stay as they were. What every node sees is under
[Other changes](#other-changes).

## The binary

Upgrade the binary as [Upgrading WasmKeep](../cluster-upgrades.md#upgrading-wasmkeep)
says: stop every node, upgrade them all, start them all. 0.7.0 documents no
exception for one node at a time. No bucket or node-to-node format changes
between 0.6.0 and 0.7.0 (the one new stored item, an actor's
`alarm_touched` meta row, is ignored by 0.6.0), but a cluster of 0.6.0
and 0.7.0 nodes is untested. Use the 0.7.0 CLI afterwards: `actor
where`, `cluster forget` of a released node lease and the shorter restore
are its own.

## Big rooms

A room deployed with 0.6.0 ([ADR 0040](../adr/0040-directory-broadcast-groups-and-the-reconnecting-room.md#13-the-pusher-amended-2026-11-20),
section 13) changes shape: the room keeps publish, numbers and history, and
a new class, `pusher/<room name>`, runs the delivery rounds, the children,
the relays and scaling.

1. **Deploy the five classes together, with the front**, from one SDK
   version: the directory, the room, the edge, the relay and the new
   `pusher` class (`internal`, like the others). A 0.6.0 front refuses the publish answer's new field: the
   publish is numbered but the front answers an error, and a retry without
   an `id` publishes it twice.
2. **Deploy the pusher batched**: `wasmkeep deploy --writes pusher=batched`,
   or `writes = "batched"` under `[classes.pusher]` in `wasmkeep.toml`.
   (`serve --writes pusher=batched` applies only to a class given with
   `--actor`; `serve` refuses `--writes` together with `--app`, because a
   deployed app's version names each class's mode.) What it must keep across a crash is
   committed with `exec-durable`.
3. **Deploy the front with the room** for `history-json`. A new TypeScript
   front against an older room still works, at the old cost; a Rust front's
   `Client::history_json` has no such fallback (an older room answers 400).

The room keeps its history and numbers. On its first alarm the pusher
adopts the room's scale rows, then has the room hand over the directory map
and every edge, 16 edges per alarm; a pending split or merge resumes in
the pusher. Clients stay connected and receive the backlog once the move is
done; nothing is renumbered. A room's leftover 0.6.0 alarm fires once and
does nothing. Rooms with fixed edges (`edges`, a test configuration) are not
moved: recreate their edges.

**Browser clients.** An edge woken from hibernation now sends the sockets it
kept the messages of its first push, and answers every ping. `RoomClient`
of 0.7.0 handles the new `ready` and the `pong` without a head. An older
client fetches history once after such a wake, and reconnects on a `pong`
without a head: in a quiet room, somewhat more often than before. Ship the
0.7.0 client with the room.

**Where the room and its pusher run.** Placement does not keep them apart.
For a large room, move the pusher to another node and check with `actor
where`: [Place a large room's pusher on another node](../designing-for-hot-actors.md#place-a-large-rooms-pusher-on-another-node).

## Alarms

Inside `on-alarm`, `get-alarm` returns none until the handler sets or
clears the alarm, then what it set; it used to return the alarm being
delivered. Whatever the handler sets stands, even the delivered time
again. A handler that saved `get-alarm` there and put it back now restores
"no alarm": check handlers that do so ([ADR 0009](../adr/0009-alarms-in-a-cluster.md),
amendment of 2026-11-20).

## Other changes

- **Metrics.** `wasmkeep_event_duration_seconds` gains a `kind` label
  (`fetch`, `alarm`, `websocket`, `other`): a query without it sums them
  as before. New: mailbox wait, start-rule wait, host work, alarm
  deliveries, event and front fuel, front CPU time, segment uploads,
  bucket requests, heartbeat reads and the runtime's schedule lag
  ([monitoring](../monitoring.md)).
- **Memory controls (Linux/glibc).** `serve --idle-memory-trim-secs`
  (on by default; 0 turns it off) returns unused allocator pages once
  activity drops, and `--malloc-arena-max` caps glibc's arenas before
  worker threads start (0 keeps glibc's own policy). Elsewhere both do
  nothing ([socket limits](../reference/socket-limits.md#room-memory-after-clients-leave)).
- **Restores** no longer wait two fixed periods; they wait on the
  bucket's clock for what may still land
  ([backup and restore](backup-and-restore.md)). `--abort` keeps its
  fixed waits.
- **The change feed** lists an app's history in parallel ranges after
  the first poll (`wasmkeep feed --list-concurrency`, [guide](../change-feed.md)).
- **Peers on a slow bucket.** Heartbeat reads go out at once, each with
  a time limit, and listing `nodes/` runs on its own task: a slow bucket
  no longer makes nodes distrust healthy peers. Nothing to do.
- **Released node leases.** `wasmkeep cluster forget <node>` now removes
  a `released` node lease that no ownership record names
  ([cluster](../cluster.md)). Leases left by earlier clusters on a reused
  prefix stay until removed this way.
