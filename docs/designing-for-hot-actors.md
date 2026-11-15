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

Socket caps are fixed today; configurable caps are planned on the
[roadmap](../ROADMAP.md#now). The
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

## Build fan-out explicitly today

For a logical room too large for one actor, keep a durable message log
and route clients to delivery actors. On each message, relay its stable
id and sequence to the delivery actors, which send it to their own
sockets. Assigning every sequence at one actor retains a single order,
but that sequencer remains a throughput limit; batch there as well or
relax ordering to independent streams.

Use `actors::call` / `actors.call` for individual relays, or
`calls::call_many` / `actors.callMany` for parallel calls (at most 16
per batch).
Check each returned error and HTTP status. The batch is not an atomic
broadcast: some recipients can succeed while others fail. A recipient
must not call back into an actor waiting on it; that is re-entry.

Keep a membership map, deterministic assignment or an application
directory of your own. Decide how clients find their delivery actor,
how membership expires, and how failed relays retry without duplicating
messages. Persist replayable messages before broadcasting when clients
must recover them. On reconnect, clients report their last sequence
and fetch gaps. A successful socket send queues a frame; it does not
prove the client received or stored it. Detect slow clients and dropped
sends, then let them catch up or reconnect.

The TypeScript `sockets.broadcast` helper loops over the current
actor's connections. It does not discover or broadcast to other actors.
Trees of relay actors can reduce direct fan-out at the root, but today
the application builds and maintains that tree itself.

## Planned for 0.6.0: a shared directory and broadcast groups

The [roadmap](../ROADMAP.md#next) plans a common directory for scaling
patterns, starting with broadcast groups and fan-out trees whose layers
grow and shrink within configured limits. It calls for measurements
against the big-room baseline, including slow clients, reconnects and
failed nodes. Aggregation and sharding with resharding would reuse that
directory; the rest of those patterns remains later, on demand.

There is no directory or broadcast-group SDK API to use today. The
planned feature would distribute a logical room among actors; each
actor would still execute on one node at a time. The separate planned
`0.5.0` [batched plain-write mode](what-saved-means.md#planned-opt-in-to-batched-plain-writes)
addresses storage waits, not socket distribution. Size today's design
from measured workloads, and keep its routing and retry contracts
explicit enough to migrate later.
