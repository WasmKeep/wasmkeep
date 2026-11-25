# Designing for hot actors

A hot actor receives work faster than it can finish it. Adding nodes
helps independent actors run in parallel; it does not make the same
actor run events concurrently. Start by deciding which state must share
an order, then split everything else. This guide covers tools available
today and labels the planned scaling patterns separately.

## Identify the limit

These are runtime limits, not measured throughput targets:

| Resource | Today | Design consequence |
| --- | --- | --- |
| Waiting events per actor | 1,024; HTTP requests finding the queue full get `429` | Bound admission before the queue fills |
| Guest CPU per event | 50 ms by default | Keep computation and serialization small |
| Wall time per event | 30 s by default, including storage, calls and outbound HTTP | A slow dependency holds the actor's other events up |
| SQL CPU per event | 2 s by default, shared by all its statements | Index queries and bound batches; splitting one query into many does not reset the budget |
| Open WebSockets | 1,000 per actor; 50,000 per node | Split a larger logical room; upgrades past the caps get `503` |

Guest CPU excludes host calls, and on Linux and macOS the time the OS
gives other threads. A
CPU or wall limit stops the event. SQL exhaustion returns
`quota-exceeded` and refuses later statements in that event; a
transaction still open at event end is rolled back. Previously committed
changes are not automatically undone. The CPU settings are
`--cpu-budget-ms` and `--sql-cpu-budget-ms`; raising a budget allows
longer events, not parallel execution within an actor.

Socket caps are configurable with `--max-sockets-per-actor` and
`--max-sockets-per-node`. The
[room benchmark](benchmarks.md#one-big-room) documents the caps and the
upgrade refusals. A socket cap is not a tested capacity promise:
broadcast size, event rate and slow clients can become limiting sooner.

Watch tail latency, refusals, event duration, CPU, SQL errors and socket
backpressure together ([Monitoring](monitoring.md)). Low CPU with growing
latency can mean an actor is waiting for storage. The room benchmark notes that actor queue depth
is not exported; a WebSocket reader waits for its message's event
before reading the next frame, so WebSocket overload can appear as
latency and unacknowledged messages instead of HTTP `429`.

## Budget for the bucket round trip

Today, committed changes from a writing event are uploaded before that
actor starts its next event. For the ordinary path with a single
replication point, that is **one segment PUT per writing event**, not
per SQL statement. Plain `exec` can answer before upload but does not
remove this wait. Multiple durable calls in an event can introduce more
uploads; snapshots, ownership and alarms add bucket work too.

The historical node-lease soak on three OVH VPS next to their bucket
measured a durable-write p50 of **57 ms**, p99 of **674 ms**, and
**1.03 PUTs per durable write**. It ran for twelve hours at build
`82dab617`, before the later bounded-upload and restore changes; it is
evidence of bucket cost, not a measurement of this checkout. Sources:
[real-bucket benchmarks](benchmarks.md#three-nodes-against-a-real-bucket)
and the [recorded run](../bench/results/2026-11-08-82dab617-soak-ovh-bhs-3vps-12h-nodelease.csv).

**Derived sizing estimate:** a serial actor spending roughly 57 ms on
each writing event would do about `1 / 0.057`, or **17 writing events
per second**, before its own work and other waits. The reciprocal of a
median is not measured sustained throughput: tail latency, retries,
snapshots and message size change the answer. Do not use loopback or
no-bucket results as a real-bucket capacity promise. Measure with the
deployment's bucket and workload.

Choose durability based on what the application promises, not as a
throughput switch. [What saved means](what-saved-means.md) explains
when an answer may safely say "saved".

## Place a large room's pusher on another node

For a large room, keep the room actor and its pusher
(`pusher/<room name>`) on different nodes. Under heavy load, a room and
its pusher on one node delivered worse in our testing: that node's
segment uploads to the bucket developed a long tail, and the room's
events waited on uploads older than `--max-unsent-age` (the event-start
rule, [ADR 0034 section 3](adr/0034-batched-plain-writes.md#3-when-an-event-may-start-and-the-loss-bound)).
With the pusher on another node, on three 4-vCPU nodes and a real
bucket, 4,500 clients at 10 messages/s got every message with delivery
p99 under 1 s, none lost or repeated; 6,000 is the measured edge, where
p99 passed 1 s ([measured](../bench/results/2026-11-22-43119bd-room-group-ovh-bhs-3vps-r07-room-apart.json)). Treat a room as large when its node shows upload
tails near `--max-unsent-age`.

**It happens by default.** A node keeps the classes `room` and `pusher`
apart at wake-up, the broadcast SDKs' default class names. A wake-up of
`pusher/lobby`, which nobody holds, on the node that holds `room/lobby`
goes to another node, and the other way round. This applies to a
request, a call, an alarm or a drain
([ADR 0041, amendment](adr/0041-placement-at-wake-up.md#amendment-2026-11-22-keeping-a-room-and-its-pusher-apart)).
Nothing to configure, and `--no-placement` turns it off.

**With your own class names**, say which class to keep apart in
`wasmkeep.toml`, on either class of the pair:

```toml
[classes.feed]
component = "feed.wasm"

[classes.fan]
component = "fan.wasm"
apart-from = "feed"   # fan/<name> and feed/<name> wake on different nodes
```

The rule is the same one. A class is in one pair at most, and
`apart-from = false` on a class named `room` or `pusher` turns the
default off. The key needs every node on 0.9 and the upgrade finalized
([the project file](project-file.md#front-and-classes)).

With one node alive, or every other node draining, both wake where they
land: the rule never refuses a wake-up. It does not move a running
actor, and a busy pusher never hibernates, so in these cases the pair
stays together until moved:

- two wake-ups of the pair at the same moment on one node;
- a takeover after a node died;
- a node of an older version;
- a cluster of two nodes after a rolling restart (while one restarts,
  the other holds both).

**Move the pusher as a fallback.** If `actor where` (below) shows them
together and the room is busy, move the pusher alone
([ADR 0017](adr/0017-moving-one-actor.md)):

```bash
wasmkeep actor move --app acme/chat pusher/lobby --to node-b --bucket s3://acme-wasmkeep/prod
```

The target must be a live node of the same app that is not draining
([Moving one actor](cluster.md#moving-one-actor)). A move does not pin:
the pusher's next cold wake-up is placed again, by the rule above. A move
can also land elsewhere: when an event reaches a third node between the
owner's release and the target's take, that node acquires the actor, and
the move counts as done (ADR 0017, "another node claimed first").

**Check where each actor runs** with `wasmkeep actor where`, which only
reads ([Seeing where an actor runs](cluster.md#seeing-where-an-actor-runs)):

```bash
wasmkeep actor where --app acme/chat room/lobby pusher/lobby --bucket s3://acme-wasmkeep/prod
```

The two `OWNER` columns should name different nodes, both live. `wasmkeep
cluster status` confirms the target is live and serving, and shows the
load beside it ([Checking a cluster](cluster.md#checking-a-cluster)).

**What to watch** ([Monitoring](monitoring.md)), on the room's node and on
the pusher's node:

- `wasmkeep_segment_upload_seconds`: the time of each segment upload to
  the bucket, with `wasmkeep_bucket_request_seconds{operation="put"}` for
  every PUT the node sends.
- The start-rule waits: `wasmkeep_event_start_rule_wait_seconds` and
  `wasmkeep_batched_waits_total{reason="age"}`. Growth in either means the
  room's events are waiting at the boundary on uploads.
- `wasmkeep_node_lease_write_milliseconds`: how long the node's own
  heartbeat write takes. A rising tail there shows the node's bucket
  writes are slow, and can accompany slow segment uploads.

**A room's edges spread over the nodes, by default.** The pusher's calls
wake a room's edges (`edge/<room name>.e<k>`), and a call would wake each
one on the pusher's node. In a version with the classes `room`, `pusher`
and `edge`, each edge nobody holds wakes instead on the rendezvous choice
for its own name over the live nodes, the pusher's included, one hop at
most
([ADR 0040, section 14](adr/0040-directory-broadcast-groups-and-the-reconnecting-room.md#14-edges-spread-over-the-nodes-amended-2026-11-22)).
On three nodes, a room's edges end on two or three of them; about a third
share the room's node. A running edge never moves for it, so clients
reconnect no more than before. A drain releases its node's edges; each
wakes where its first event after takes it: a reconnecting client's
socket wakes it where that client landed, the pusher's next push on its
spread choice. With one node alive the edges wake where they are called;
`--no-placement` turns it off. For a class with another name, write
`spread = true` in its `[classes.<name>]` table (`false` turns the
default off for a class named `edge`); it needs every node on 0.9, as
`apart-from` does. List the edges with the same command:

```bash
wasmkeep actor where --app acme/chat edge/lobby.e1 edge/lobby.e2 edge/lobby.e3 --bucket s3://acme-wasmkeep/prod
```

`wasmkeep_placement_spread_total{outcome}` counts the wake-ups of an edge
`sent` to its node or woken `here`.

## Split independent state

Choose actor names around the smallest useful owner of state:

| A bottleneck | A possible split | What the application now owns |
| --- | --- | --- |
| One actor for every user's session | One actor per user or session | Routing and authorization for the name |
| One database actor for all customers | One database actor per customer, or a stable key partition | Cross-customer queries and migration between partitions |
| One queue for all jobs | Queues by work key or job category | Per-key ordering, distribution and deduplication |
| One room broadcasting to every client | Several delivery actors for one logical room | Membership, ordering, replay and partial delivery |

Preserve a single actor where atomic decisions need one database.
Splitting an order and its inventory, for example, requires an explicit
cross-actor protocol; there is no transaction across them. A global
coordinator touched by every request can become the next hot actor.

The [database pattern](database.md) remains a single writer per named
database. The [KV pattern](kv-and-queues.md#key-value) already hashes
keys to shard actors, but all callers must agree on the shard count;
changing it requires migrating the data. Neither gives automatic
resharding today.

More actors can spread over nodes. Existing resident actors do not move
just because a node joins, and a room with open sockets stays with its
socket home through hibernation. A move or drain can change its node,
but cannot split its workload. See [How it scales](failover-and-scaling.md#how-it-scales)
and [Moving one actor](cluster.md#moving-one-actor).

## Batch application work today

Put several operations in a bounded request, handle them in a
transaction, and finish with a durable commit before acknowledging the
batch. That amortizes the upload across operations. Stop growing the
batch before its SQL, CPU, body or response limits become the next
failure; include per-operation keys so an unknown outcome can be
retried safely.

Existing SDK helpers do this for their patterns:

- A database client uses `db::Client::batch` in Rust or
  `db.Client.batch` in TypeScript for a transaction on its database
  actor.
- A queue client uses `queue::Client::send_batch` or
  `queue.Client.sendBatch` to enqueue a batch in one write, all or none.

You can also collect messages in actor memory across events, then
commit and broadcast them together. Until committed durably they are
pending, not saved: a trap, hibernation, deploy or node loss can discard
the buffer. Bound its size, arrange a scheduled drain, and give clients
a final acknowledgement only after the durable commit. A preliminary
"accepted" response must leave clients able to retry messages for which
no saved acknowledgement arrives. Schedule the drain when the buffer
first becomes nonempty; resetting an alarm on every arrival also writes
state. Do not hold a SQL transaction open across events.

The [room trial](benchmarks.md#a-workstation-trial) demonstrates this
pattern, not host batching: its `batch-` room collects in memory,
commits with `exec-durable` after a **50 ms** window and broadcasts a
combined frame per socket. At **1,000 offered messages/s into 1,000
sockets**, the recorded cell has **36 ms p50** and **66 ms p99** to the
last recipient, rounded from the
[CSV](../bench/results/2026-11-15-699ebec-room-workstation.csv).
This was a **10 s** workstation trial at build `699ebec`, with moto on
loopback, node data on tmpfs and the load on the same machine. It is
not a real-bucket or fleet result. Combining socket frames also saves
fan-out work; batching SQL alone does not provide that saving.

## Refuse excess work in the front

Authenticate and choose the actor name in the app's front before
calling it. Apply a bounded admission policy per user, API key or room,
and answer excess demand with a retryable refusal and an appropriate
`Retry-After`. Clients should back off with jitter and a retry bound,
using the same operation key when an outcome is unknown.

The front gets a fresh instance for each request, so an in-memory token
bucket there cannot keep a quota across requests. For a shared quota,
call a limiter actor keyed by the quota's owner, then call the work
actor only if admitted. Avoid a single global limiter for all traffic;
it has the same serialization and storage costs as any actor. A limiter
decision and a call to another actor are not one transaction: define
what happens to the allowance if the work call fails.

Make protected classes `internal` so clients cannot bypass front
checks through `/actor/`. For WebSockets, front admission covers the
upgrade only; after hand-off, messages go directly to the actor. Bound
message rates there too. Keep flood protection and broad ingress rate
limits at the proxy or CDN. Declarative per-node rate limits in
`wasmkeep.toml` are **planned**, not a current front setting.

## Use the reconnecting room for one ordered stream

Use the SDK broadcast room when one logical stream needs more sockets than
one actor can hold, with ordered delivery and reconnect recovery. The room
is still one sequencer; independent streams should use independent rooms.
Rust exposes `wasmkeep_sdk::directory` and `wasmkeep_sdk::broadcast`;
TypeScript exposes `directory`, `broadcast` and the browser-only
`@wasmkeep/sdk/room-client`. See the runnable
[TypeScript room](../examples/ts-big-room/README.md) and
[ADR 0040](adr/0040-directory-broadcast-groups-and-the-reconnecting-room.md).

Deploy an internal directory, room, pusher, edge and relay class. The front
admits clients, looks up their edge through the directory and hands off the
socket. Publish over HTTP through the front; the socket accepts only
keepalive pings. The room numbers messages and keeps the history; the pusher
(`pusher/<room name>`) pulls from it, batches pushes, retries failed children
and scales the edges, so a round waiting on a slow edge never holds a
publish. The room asks the front to poke the pusher only when the pusher said
it went idle. Edges hold sockets; above 16 edges, one relay layer distributes
the pushes. Calls go one way: the room never calls the pusher, an edge never
calls anyone, and a relay only calls its edges. Calling back into an actor
waiting for you would stall the chain.

The defaults and ceilings are **protocol limits, not measured capacity**:

| Setting | Bound |
| --- | --- |
| Sockets per edge | 500 by default, no higher than the host's per-actor cap |
| Fan-out per pusher or relay | 16 children |
| One relay layer | 256 edges maximum; 128,000 sockets is only the arithmetic ceiling at 500 each |
| Room history | Last 2,000 messages or 8 MiB, whichever is less |
| Edge buffer | Last 512 messages or 1 MiB |
| Edge fan-out per event | 16 distinct cursor frames or 1 MiB of frame bytes; caught-up group first |
| Host send queue per socket | 256 frames or 4 MiB |

The example deliberately uses **150 sockets per edge** to demonstrate
splits sooner. An edge splits at 80% capacity; small neighbours can merge
with hysteresis. Movement closes affected sockets with **4801**. If a slow
client falls behind the edge buffer, it fetches history; if the edge cannot
even queue the behind notification, it closes that socket with **4802**.
A room's edges wake spread over the live nodes
([above](#place-a-large-rooms-pusher-on-another-node)); the relays wake
beside the pusher. Spreading moves only fan-out CPU: the room remains the
sequencer. Running edges are not rebalanced when a node joins; they move
only through a drain, a split or merge, or a cold wake.

The per-event bound limits frame construction; copying a large replay to
hundreds of sockets still consumes CPU in each host send. The 500-socket
50 ms gate covers a full retained buffer with caught-up sockets at 2 msg/s,
not simultaneous full-buffer replay to every socket.

Edges keep serialized items and share each frame across sockets at the same
cursor. Deferred groups retain their cursor and count as lagging; only an
`ok` send advances a cursor. Catch-up retries stay at 20 ms. When only stalled
sockets lag, unchanged answers double that interval up to 45 seconds. New
publishes and healthy catch-up work keep their normal cadence.

Delivery is **at least once, in order**, within the retained history window.
Each message has an epoch and number `(e, n)`; repeats are discarded by that
identity. A new epoch can reuse numbers lost with a batched room's node:
clients must invalidate the announced void range, not mistake the replacement
for a duplicate. With `confirm: false` (default), a broadcast can precede
upload; the loss bound is about the last one or two bucket round trips.
With `confirm: true`, the room flushes before pushing, so it never broadcasts
a number that is later lost, at the cost of waiting for upload. This is not
an acknowledgement that every client has received or stored the message.
Choose batched writes in the room's deployment configuration explicitly;
see [What saved means](what-saved-means.md#opt-in-to-batched-plain-writes).

Use `RoomClient` or implement all of the ADR's client rules:

- Retain `(e, n)`, discard repeats and handle void ranges through `onLost`.
- Hold live frames while fetching history pages after a gap, `behind`, or
  inconsistent `ready`/`pong`; an edge woken from hibernation by a header-only push sends open
  sockets a `ready` with `b`, a gap only when its position is past yours,
  and a `pong` with `n: null` is a keepalive. Handle `reset` by reloading
  application state
  when history or the last 64 epochs no longer cover the position.
- Reconnect through the front, with the last position and exponential full
  jitter (up to 30 seconds); handle 4801, 4802, 1012 and failed opens.
- Ping after 45 seconds without a frame; reconnect after another 10 seconds
  without a response. Honour HTTP 429/503 and `Retry-After`.
- Retry an uncertain HTTP publish with the same id while it remains in the
  history window. Authorization, presence and application state remain the
  application's responsibility.

The [reconnecting-room harness](benchmarks.md#reconnecting-room-harness)
records delivery and reconnect latency, recovery and topology on a deployed
cluster. Its small smoke check is correctness evidence only. The real-bucket
growth and slow-client benchmark at 3,000 to 10,000 clients is
[measured on 0.6.0](benchmarks-archive.md#a-big-room-on-three-nodes-preview), where
the pattern was a preview: delivery p99 of 5 to 15 s, held by the room's
publish path. 0.7.0 moves delivery off that path, to the pusher: with the
pusher on another node, 4,500 clients got delivery p99 under 1 s
([above](#place-a-large-rooms-pusher-on-another-node),
[measured](../bench/results/2026-11-22-43119bd-room-group-ovh-bhs-3vps-r07-room-apart.json)). Node loss is not measured. The older single-actor room trial above is a
separate workload, not evidence for this pattern's capacity.
