# When a node dies, and how it scales

What a cluster does when a node stops or dies, what callers see meanwhile,
and what adding nodes does and does not do. [Running a cluster](cluster.md)
has the mechanism and the commands; this page is the short answer to "what
happens to my application". It describes the code as it is today; what is
planned is on the [roadmap](../ROADMAP.md).

## One node per actor

An actor runs on one node at a time. Its state is one SQLite database with
one writer: the node that holds the actor applies its events one at a
time, and uploads what each event committed to the bucket. Two nodes
running the same actor would be two writers of one history, so a node
takes an actor only when the bucket says nobody else holds it
([how it works](cluster.md#how-it-works)).

A node holds an actor while the actor is in memory. When the actor
hibernates (30 s without an event, by default) the node gives it up, and
its next event starts it on whichever node that event reaches. A request
that reaches another node while the actor is held is forwarded to the
holder, once.

## A node that stops cleanly

On `SIGTERM` or Ctrl-C a node:

1. stops accepting connections, so a load balancer's check of
   `/_wasmkeep/ready` fails from then on;
2. lets requests under way finish, for up to 10 s;
3. closes its WebSockets with code 1012;
4. lets each actor finish its queued events, for up to 10 s, and gives
   each actor up, waiting up to 3 s for those releases to reach the
   bucket;
5. writes its own lease `released`, within about 6 s.

Each wait ends as soon as its work is done. Together they bound the
whole stop at about 29 s. An actor is free as soon as it is
given up: its next request starts it on another node at once, after that
node has rebuilt the database from the bucket (unless its local copy is
already current). Clients whose socket closed with 1012 open it again and
reach the new holder.

To empty a node before stopping it, [drain it](cluster.md#draining-a-node):
it hands its actors over while it still forwards requests, and each
actor's pending alarm moves to the new holder's timer.

## A node that dies

A node that is killed, loses power or is cut off from the bucket gives
nothing up. The actors it held in memory stay recorded as its own until
another node fences it: a peer that has seen its heartbeat unchanged for
6 s writes the fence once 15 s have passed since the dead node's last
heartbeat (the 10 s lease plus a 5 s grace period). The actors are then
free, **15 to about 21 s after the dead node's last heartbeat** with the
default timings. The wait is what keeps one writer per actor: a node that
is only slow, or cut off for a moment, must have stopped acting on its own
before anyone else starts
([ADR 0020](adr/0020-one-lease-per-node.md)).

Only actors that were in memory on the dead node wait. An actor that had
hibernated there was already given up, and its next request starts it on
whichever node it reaches. Two things of a hibernated actor still go with
the node: the WebSockets it held for the actor, and the actor's pending
alarm, which sat in that node's timer and now fires late
([below](#alarms)).

### What callers get meanwhile

For an actor the dead node held, until the fence:

| Caller | Gets |
| --- | --- |
| An HTTP request that reached the dead node | A broken connection, or none: the load balancer's check of `/_wasmkeep/ready` takes the node out |
| An HTTP request that reached another node | `503` with `Retry-After: 1`. While the dead node's heartbeat still counts as live (10 s) the request is forwarded and the body says `the actor's node did not answer; retry`: at once if the connection is refused, after up to 5 s if nothing answers the connection attempt. After that it is not forwarded: `the actor's node is not reachable; retry`, at once |
| `actors.call` from another actor | The error `failed`, with a text naming the node (`node b is not reachable`, or the connection error), or `timeout` when the caller's event reaches its wall-clock limit first |
| A WebSocket upgrade | The same `503` with `Retry-After: 1` |
| An open WebSocket | No close frame. A socket to the dead node itself breaks when the dead machine's end of the connection is closed (a killed process), and otherwise ends only when the client's own keepalive or TCP notices. One relayed through a live node over mutual TLS is cut by that node when it stops trusting the dead one, about 10 s after its last heartbeat ([cluster](cluster.md)). One relayed over plain HTTP (`--cluster-plaintext accept`) is not cut by the relaying node: it ends when the connection to the dead node breaks, or when the client's own keepalive notices |

One case is slower and is not a 503. A request forwarded on a connection
the dead node had already accepted, and then never answered, waits 30 s
and gets `504` with the body `the actor's node did not answer in time` and
no `Retry-After`.

A `503` does not say whether the request ran: one that was written to the
dead node may have run there. Retry what is safe to repeat, and make the
rest idempotent. What the actor had committed with `exec-durable` and
acknowledged is in the bucket and survives. A plain `exec` whose upload
had not finished when the node died can be lost
([replication](cluster.md#replication)).

After the fence, the first request for the actor makes the node it
reaches take it: that node rebuilds the database from the bucket, then
runs the request. Clients that retry on `503`, with a bound, get through
without doing anything else.

Requests are refused during the window, not held. Holding them until the
takeover is on the [roadmap](../ROADMAP.md).

### Alarms

Pending alarms are listed in the bucket, and every node reads the overdue
part of that list every 10 s. An alarm whose node died is fired by another
node once it is overdue by a lease period plus grace: with the defaults,
24 s late at the median and 36 s at worst with nodes next to their bucket
([measured](benchmarks.md#three-nodes-against-a-real-bucket),
[ADR 0009](adr/0009-alarms-in-a-cluster.md)). Alarms fire at least once,
so a handler that had started on the dead node may run again.

### What is not interrupted

- **Actors on the other nodes.** They keep their holder, their sockets and
  their alarms. Nothing is moved or restarted because a peer died.
- **Requests for hibernated actors**, wherever they last ran. Their
  sockets and alarms on the dead node are another matter (above).
- **Stateless components.** Every node runs its own copy and needs no
  other node to answer ([below](#stateless-components)).
- **The cluster itself.** There is no leader and no quorum: the remaining
  nodes keep working with the bucket, down to one node.

## How it scales

Throughput comes from many actors, not from one busy actor. Each actor
handles one event at a time; different actors run in parallel, on every
node.

- **Where an actor runs.** The node that receives the first request for an
  actor nobody holds takes it. A load balancer that
  [hashes the actor's path](deploy/front-door.md#route-by-actor-recommended)
  sends every request for one actor to the same node, so actors spread
  over the nodes as their names hash, and little is forwarded.
- **Adding a node.** Start it on the same bucket and prefix, with its own
  node id, and add it to the load balancer. With a consistent hash, a
  share of the actor paths now reach the new node. An actor still in
  memory elsewhere stays there (the new node forwards to it); once it
  hibernates, its next request starts it on the new node.
- **Removing a node.** Drain it, then stop it.

### What does not happen today

- **No rebalancing by load.** Nothing looks at how busy a node is. An
  actor in memory stays on its node until it hibernates, until it is moved
  with [`wasmkeep actor move`](cluster.md#moving-one-actor), until its
  node is drained with [`wasmkeep cluster drain`](cluster.md#draining-a-node),
  or until its node stops or dies. A move is not a pin: once the actor
  hibernates, its next event starts it wherever that event lands.
- **No splitting of one actor.** An actor that receives more events than
  it can run one at a time fills its queue (1,024 events, then `429`). No
  setting changes that, and a second node does not help. The application
  uses more, smaller actors: one per room, user, document or device, not
  one per application.
- **No placement by region or by rule.** Any node of the app may take any
  of its actors.

Load per node in the metrics, and placement, are on the
[roadmap](../ROADMAP.md).

## Stateless components

The component given to `wasmkeep serve` as its positional argument answers
every path outside `/actor/` and `/_wasmkeep/` (the node answers that one
itself). It has no database, no lease and nothing in the bucket: each
request gets a new instance on the node that received it,
so any node answers and the nodes do not coordinate. It scales with the
nodes behind the load balancer, within each node's connection and instance
limits and a 30 s wall clock per request.

It is a file each node is started with, not part of a deploy:
`wasmkeep deploy` does not change it. Updating it is a restart of each
node in turn, with two versions answering side by side until the last one
has restarted
([how](cluster.md#updating-the-stateless-component)).

## Actors or a database?

Actors are strong where state has an owner:

- **State per user, room, document or device.** Each actor's data is next
  to its code, and nothing else writes it.
- **Real time.** The sockets of a room meet in one actor, in one order.
- **Coordination.** One event at a time means a counter, a quota or a
  lock needs no transaction to be exact.
- **Scheduled work per entity.** An alarm on the actor, delivered at
  least once, also when its node dies.

They are weak at questions that span every entity: "all orders over 100
this month", search, reporting, anything a single SQL query over one big
table answers. Actors do not see each other's data, there is no way to
list or query across them, and asking each one in turn is a call per
actor.

Keep a database for those questions. A component reaches it over outbound
HTTP, the only way out of the sandbox: a database with an HTTP API, or
your own service in front of one. There are no raw sockets, so a
database's own wire protocol is not reachable from a component. Outbound
HTTP reaches public addresses only unless the operator opens a range with
`--allow-outbound`, refuses the ports browsers block, and allows 16
requests in flight per actor and 256 per node
([security model](security.md)). A common split: actors keep the hot,
per-entity state and push a summary to the database when something
changes, and the queries that span everything run there
([use cases](use-cases.md#next-to-postgres-valkey-and-stateless-servers)).

Cloudflare Durable Objects, celld and Rivet are built on the same model,
stateful actors ([how they compare](../README.md#how-it-compares)).
