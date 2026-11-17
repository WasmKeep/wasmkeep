# 0040. Directory, broadcast groups and the reconnecting room

- Status: partially implemented (2026-11-17). R1–R4 provide the directory,
  room core, edges, browser client and direct-edge scaling. R5–R8 remain
  planned; relays and the large-room example are not available yet.
- Date: 2026-11-17
- Milestone: [Next](../../ROADMAP.md#next), `0.6.0`, "One directory for
  the scaling patterns"
- Follows: [ADR 0015](0015-kv-and-queues-as-sdk-patterns.md) and
  [ADR 0029](0029-a-database-client-as-an-sdk-pattern.md) (a pattern is SDK
  code an app's own class runs), [ADR 0025](0025-calling-several-actors-at-once.md)
  (`call-many`, 16 entries), [ADR 0027](0027-sockets-through-the-front.md)
  (the front hands a socket to an actor), [ADR 0024](0024-socket-home.md)
  (sockets stay with their node), [ADR 0028](0028-actors-that-end.md)
  (an actor that ends), [ADR 0034](0034-batched-plain-writes.md) (batched
  plain writes and their loss bound) and
  [ADR 0037](0037-socket-session-admission.md) (socket caps).
- Amends: nothing. No WIT change, no host change (one possible exception,
  section 12).

## Context

One actor holds at most 1,000 sockets by default (ADR 0037) and runs one
event at a time. A room larger than that is today the application's own
work: [Designing for hot actors](../designing-for-hot-actors.md) says to
keep one sequencer, relay to delivery actors, and build membership,
replay, slow-client handling and reconnects by hand. Every app would
invent a different protocol, and the next scaling patterns (aggregation
trees, sharding with resharding) would each invent their own table of
"which actor has this key".

What the platform gives, and what this record must work within:

- `calls.call-many` starts at most 16 calls and returns when all have
  answered or the event's call deadline passed (ADR 0025). The requests
  of one batch total at most 16 MiB.
- A chain of calls must not come back: an actor waiting in a chain is
  `reentrant`, and two chains waiting on each other end in `timeout`
  (ADR 0025). A room pushing to an edge while that edge calls the room
  would stall both for the call deadline.
- A socket's send queue holds 256 messages or 4 MiB; past it `send`
  answers `backpressure` and the message is dropped. There is no
  "writable" event.
- An actor's memory lasts until it hibernates (30 s idle by default), is
  moved, or its node dies. Its sockets outlive hibernation (ADR 0024);
  a drain or a move closes them with 1012; a dead node closes nothing.
- A plain write of a batched class can be lost with its node, and later
  events, calls and socket messages may already rest on it: "the room
  numbers another message the same" (ADR 0034, section 12).
- An alarm is the only timer. Changing it is a write, and an index PUT
  in a cluster (ADR 0009; one per segment in a batched class).
- A socket event never creates an actor; the front's hand-off does
  (ADR 0027).

## Decision

Two SDK modules, in Rust (`wasmkeep_sdk::directory`,
`wasmkeep_sdk::broadcast`) and TypeScript (`directory`, `broadcast` in
`@wasmkeep/sdk`), plus a browser client (`@wasmkeep/sdk/room-client`,
no host import). The app deploys four classes that export the SDK's
code, all `internal` (ADR 0010): a **directory**, a **room**, an
**edge** and a **relay**. The app's front signs clients in and uses
three helpers: `join`, `publish`, `history`.

```text
publishers ──HTTP──▶ front ──call──▶ room ──call-many──▶ [relays] ──call-many──▶ edges ──▶ sockets
clients ──upgrade──▶ front ──call-many──▶ directory (lookup) + room (poke)
                       └── hand-off ──▶ edge
```

### 1. Calls go one way

Room to directory, room to relays, relays to edges, room to edges. The
front calls the directory and the room. **An edge and a relay call
nobody; the directory calls nobody.** Everything an edge has to tell the
room travels in its answer to a push. Everything a client has to ask the
room (a publish, a gap) goes over HTTP through the front. This rule is
what keeps a batch from meeting itself (Context); the SDK's edge and
relay code has no call to make, and the docs tell an app not to add one.

Consequence: in this version a client does not publish on its socket.
It sends an HTTP request to the front, which calls the room.

### 2. The directory

A directory maps points of a 64-bit space to actor addresses. It is
general: the broadcast group is its first user, sharding and aggregation
are to reuse it.

**One map per directory actor.** The actor's name is the map's name (a
group's map is `bg:<room name>`). An app-wide map would be one more hot
actor.

**Data model**, in the directory actor's SQLite:

```sql
CREATE TABLE map (id INTEGER PRIMARY KEY CHECK (id = 1),
                  owner TEXT NOT NULL, version INTEGER NOT NULL,
                  last_op TEXT, last_result BLOB);
CREATE TABLE entries (lo BLOB PRIMARY KEY,      -- 8 bytes, big-endian
                      target TEXT NOT NULL,     -- class/name
                      version INTEGER NOT NULL, -- map version at last change
                      meta BLOB NOT NULL);      -- the owner's, at most 1 KiB
```

Entries are ordered by `lo`. Entry *i* covers `[lo_i, lo_{i+1})`, the
last one up to 2^64. Invariant: an entry with `lo = 0` exists, so every
point has exactly one entry. At most 4,096 entries.

**The point of a key** is `fmix64(fnv1a64(key))`: FNV-1a over the key's
bytes (as `kv`, ADR 0015), then the 64-bit finalizer `h ^= h >> 33; h *=
0xff51afd7ed558ccd; h ^= h >> 33; h *= 0xc4ceb9fe1a85ec53; h ^= h >> 33`.
FNV-1a alone leaves keys that differ in their last byte within 2^48 of
each other, which a range split never separates. Test vectors are shared
by both SDKs.

**Who owns an entry.** The map has one owner: the actor that created
it, read from `x-wasmkeep-caller`. Only the owner changes the map; any
inside caller may read it. The owner is the authority on what should be
(it keeps its own table of targets and one pending operation); the
directory is the authority on what a lookup answers.

**Operations**, as requests under the path prefix
`wasmkeep-directory/v1`, framed with the SDK's `wire` module like `kv`:

| Operation | Who | Effect |
|---|---|---|
| `create(target, meta)` | anyone; becomes owner | One entry `[0, 2^64)`, version 1. Repeated by the owner: the current map. By another: `409`. |
| `lookup(point)` | inside callers | `lo`, `hi`, `target`, `meta`, the entry's version, the map's version. No write. `404` without a map. |
| `entries()` | inside callers | The whole map. |
| `split(lo, at, target, meta, expect, op)` | owner | The entry at `lo` becomes `[lo, at)`; a new entry `[at, hi)` names `target`. |
| `merge(lo, keep, expect, op)` | owner | The entry at `lo` and the one before it become one, naming the left or the right target (`keep`). |
| `delete(expect)` | owner | Ends the map (`delete-self-durable`). |

**A change is atomic because it is one event of one actor.** `split`
and `merge` run one transaction: check `expect` against the map's
version (else `409` with the current version), change the rows, set
both touched entries' `version` and the map's to `version + 1`, record
`op` and the result, and commit with `exec-durable`. An answer `ok` is
in the bucket. A repeated `op` returns the recorded result without
changing anything, so the owner may retry after a timeout or a crash.

**What a lookup during a change sees**: the map before it or the map
after it, never half. The directory changes nothing but its rows; making
the targets agree is the owner's protocol (section 7), built so that
both maps are safe to act on.

### 3. The room: numbers, epochs and the history window

The room is the group's only sequencer. Its state is in SQLite:

```sql
CREATE TABLE history (n INTEGER PRIMARY KEY, e INTEGER NOT NULL,
                      at INTEGER NOT NULL, id TEXT UNIQUE, body TEXT NOT NULL);
CREATE TABLE epochs (e INTEGER PRIMARY KEY, base INTEGER NOT NULL);
CREATE TABLE children (name TEXT PRIMARY KEY, kind TEXT NOT NULL,   -- edge | relay
                       parent TEXT, state TEXT NOT NULL, lo BLOB, hi BLOB);
CREATE TABLE ops (id TEXT PRIMARY KEY, kind TEXT NOT NULL, step INTEGER NOT NULL, args BLOB NOT NULL);
-- plus a meta row: next child number, map version, last split, last merge
```

**Publish.** `publish(messages)` takes up to 64 messages, 256 KiB
together, each a UTF-8 text of at most 64 KiB with an optional `id` (at
most 64 bytes). For each, the room takes the next number `n`, inserts
the row with a plain `exec`, and answers `(e, n)`. An `id` still in the
window answers the number it already has: a publisher retries an unknown
outcome with the same `id`. If no push is pending the event sets the
alarm to `now + push_delay` (20 ms). It does not fan out itself, so
publishes arriving during a push queue up and leave together.

**Window.** The history keeps the last 2,000 messages or 8 MiB,
whichever is less; older rows are deleted in the publishing event.

**Epochs.** A message's identity is `(e, n)`. `n` rises by one per
message. `e` is the room's epoch: at the first publish after a cold
start (a wake from hibernation, a move, a failover), the room inserts
`epochs(e + 1, base)` with `exec-durable`, `base` being the highest `n`
then in `history`, and continues at `base + 1`. The room cannot tell a
clean wake from a crash that lost writes, so it always bumps; the cost
is one durable write per wake of an idle room.

- After a clean stop nothing was lost: `base` is the last number anyone
  saw, and clients continue without noticing more than the new `e`.
- After a crash of a batched room, numbers above `base` were broadcast
  and lost. The new epoch reuses those numbers with other messages;
  `(e, n)` still never names two messages. Every frame carries `e` and
  the epoch's `base`, so whoever holds a number above `base` from an
  older epoch knows it is void (section 6).

`epochs` keeps the rows of the last 64 epochs.

**How much can be lost.** With `confirm = false` (the default) a push
goes out before its messages are in the bucket: a crash loses what
ADR 0034 section 3 states, the publishes of about the last one or two
bucket round trips (the room's alarm changes with most segments, so
count two PUTs per round trip there). With `confirm = true` the push
event calls `storage.flush` first and pushes only on `ok`
(`not durable: still uploading` leaves the alarm for a retry 50 ms
later): no number is ever broadcast and then lost, and delivery waits
for one upload. In a strict class the room works unchanged and loses at
most the publish under way, but runs one publishing event per bucket
round trip; a room class should be `writes = "batched"`.

**What a position the room no longer has gets.** `history(after, limit)`
answers from the client's `(e, n)`:

| The position | Answer |
|---|---|
| on the line, inside the window | the messages after it, at most 256 KiB, `more`, and the head |
| above the `base` of a later epoch `e'` (lost) | `lost: {e: e', base}`, then the messages from `base + 1` |
| older than the window, or older than the 64 epochs kept | `reset: {e, n}` of the first message kept, then the messages from there |
| absent (a new client) | the head only; `last = k` asks for the last `k` messages instead |

### 4. The push

On its alarm the room runs one **round**: a `call-many` to its children
(edges, or relays once there are more than 16 edges).

- The room keeps, in memory, each edge's acknowledged position. A push
  carries the messages after it, at most 512 messages or 512 KiB, with
  the header `(e, base, first n, head n)`. After a cold start the
  positions are unknown: the first push carries the header alone, and
  each edge's answer says what it has. If an edge's position has left
  the window, the push starts at the window's first message and the edge
  drops its buffer (section 5).
- An edge answers `(e, n)` it now has, its socket count, lagging count,
  and counters since its last answer (opens, refusals by reason, slow
  closes). A missed or failed push is thus resent by the next round;
  an edge drops numbers it already has in the same epoch.
- **A newer epoch.** On a header whose `e` is newer than its own, an
  edge first cuts its buffer above `base` and pulls every cursor above
  `base` back to it, then applies the push: the reused numbers are new
  messages. The room counts an answer `(e_old, n)` from an edge as the
  position `min(n, base)`, with the lowest `base` of the epochs after
  `e_old`.
- **A child that fails** (`failed`, `timeout`, a non-200 status) is
  `suspect`: skipped for 2 s, doubling to 16 s, so a dead node costs one
  slow round, not every round. The first success clears it. A round with
  messages left, or with a suspect child due, sets the next alarm.
- With `first n` past its head plus one an edge has a hole: it drops its
  buffer and tells its sockets they are behind.

One request to a child stays under 1 MiB, so 16 of them fit the batch's
16 MiB.

### 5. The edge

An edge holds sockets, at most `edge_cap` (500, which must not exceed
`--max-sockets-per-actor`). It writes only its configuration row
(the room's name, its range `[lo, hi)`, its state), durably, when it is
created, its range changes or it drains. Everything else is memory:

- a **buffer** of the last 512 messages or 1 MiB, with its `floor` (the
  number below which it has nothing) and its head;
- per socket, the client's point and a **cursor**, the last number
  queued for it.

**The queue bound and the overflow.** There is no second queue: the
bound is the host's, 256 frames or 4 MiB per socket. On a push the edge
sends each socket whose cursor is at or above the floor one frame with
the messages after its cursor (64 KiB at most per frame) and moves the
cursor. On `backpressure` the cursor stays: the socket is **lagging**
and is tried again at each push and at each frame from that client.
When a lagging cursor falls below the floor, the edge sends `behind`
and sets the cursor to the head: the client fetches the gap over HTTP
(section 6) while live frames resume. If `behind` itself meets
`backpressure`, the edge closes the socket with 4802. A slow client
thus costs the edge a cursor, never a copy of the stream.

**After a hibernation** the buffer, the cursors and the points are gone
while the sockets remain. The edge then knows no head until the next
push or call from the room; it treats every socket's cursor as that
head (it was idle for 30 s, and a client that missed a tail learns it
from `pong`), and at a range change closes every socket whose point it
no longer knows.

**Open.** The front's hand-off path is the root with the query
`?c=<client id>&p=<point, 16 hex digits>&e=<epoch>&n=<number>`, the
position absent for a new client; an edge trusts it because its class is
`internal` (ADR 0027). `on-websocket-open` refuses, with these reasons:
`unknown` (no configuration row: an edge that ended and that a hand-off
created again; it then calls `delete-self`), `moved` (the point is
outside its range, or it is draining), `full` (at `edge_cap`). Otherwise
it answers in the same event:

- same epoch and `n` at or above the floor: `ready` with the client's
  own `n`, then the buffered messages after it;
- no position: `ready` with the head; the stream starts there;
- anything else, or no head known yet: `ready` with `n: null`, cursor
  at the head: the client checks its position over HTTP.

### 6. The client protocol

Text frames, JSON. Server to client:

| Frame | Meaning |
|---|---|
| `{"t":"ready","e":4,"n":120}` | Connected; the stream resumes after `n`. `null`: check over HTTP. |
| `{"t":"m","e":4,"b":100,"n":121,"m":[{"a":1763400000000,"i":"k1","d":"hello"}]}` | Messages `n`, `n+1`, ... of epoch `e`, whose base is `b`. |
| `{"t":"behind","e":4,"n":180}` | Fetch what you miss up to here over HTTP; live frames follow. |
| `{"t":"pong","e":4,"n":180}` | Answer to `ping`, with the head. |

Client to server: `{"t":"ping"}` only. Close codes: 4801 `moved` (the
range moved or the edge drains), 4802 `slow`, 1012 (the edge's node
drains or the edge moved).

**The client's rules** (`room-client`, and any other client):

1. Keep `last = (e, n)`. A message numbered at or below `last.n` in the
   same epoch is a repeat: drop it.
2. A frame of the next epoch (`last.e + 1`) with `b < last.n`: the
   messages above `b` are void; report them (`onLost(b + 1, last.n)`),
   set `last = (e, b)`. The app decides what a void message means
   (remove it, mark it). A frame more than one epoch ahead is a gap:
   `history` answers with the lowest base in between.
3. A frame whose first new number is past `last.n + 1`, a `behind`, a
   `ready` whose `n` is `null` or differs from `last.n` (a new client
   takes it as its start), a `pong` whose head differs from `last`:
   there is a gap.
   Hold live frames, `GET <front>/history?after=<e>.<n>` until `more` is
   false, apply `lost` and `reset` as answered (`onReset`: the app
   reloads what it shows by its own means), then apply the held frames
   by rule 1.
4. **Keepalive**: after 45 s without a frame send `ping`; without a
   frame 10 s later, close and reconnect.
5. **Reconnect**: through the front's join URL with `last`, never to an
   edge's address. Delay before attempt *k* (from 0): a random time in
   `[0, min(30 s, 500 ms * 2^k))` (full jitter). After 4801 or 1012 the
   first delay is random in `[0, 2 s)`. `k` returns to 0 once a
   connection has stayed `ready` for 10 s. A failed open (a browser sees
   only 1006) is one more attempt.
6. HTTP answers `429` and `503` are retried after `Retry-After` plus
   the same jitter.

Delivery is therefore **at least once, in order**: every message that
stays in the history reaches every connected client, once or repeated;
a message lost with a node is announced void; a client away for longer
than the window is told `reset`.

### 7. Joining, and how edges are created, found and drained

**Join.** The front's `join(room, client id, position)` makes one
`call-many` of two entries: `lookup(point)` on the group's directory and
`poke` on the room. It then hands the socket to the edge the lookup
named. A `404` (no group yet) is retried once after the poke, which
creates the group. The poke's own failure is ignored.

**Poke** is the room's only trigger besides publishes, and it answers
at once: a join never waits for a round. A poke on a room without state
creates the group (the map, one edge for the whole space). Otherwise,
if the last round is more than 1 s old and no alarm is pending, it sets
the alarm to now. No alarm stays set in an idle room: one is set by a
publish, a poke, or unfinished work. The round (header only when
nothing is unsent) runs in `on-alarm`, refreshes the counts, and ends
with a **scale pass**:

- **Grow.** An edge at or above 80% of `edge_cap` is split: its range
  is cut at its midpoint and a new edge takes the upper half.
- **Shrink.** An edge below 30% is merged with the neighbour holding
  fewer sockets, if together they hold at most 60%: the neighbour takes
  the range, the edge drains.
- **Never both within a minute**: no merge within 60 s of a split, no
  split within 60 s of a merge, group-wide. An edge that fills during
  that minute refuses with `full` and its clients retry.
- One operation at a time, at most four per event; the alarm continues
  the rest. `range` and `init` answer the edge's socket count, and an
  edge touched by an operation is not judged again before its next
  push answer.

A range is ownership, not a hint: after a split the old edge closes the
sockets whose point left its range (4801), and they reconnect to the new
edge with their position. Both edges then sit near 40%. An edge is
between 30% and 80% full in steady state, so 10,000 clients need about
25 to 50 edges (derived), not 20.

**An operation is a durable row and idempotent steps.** The room writes
the `ops` row with `exec-durable`, runs the steps, and deletes the row
durably. After a crash, or a failed step, the alarm runs the row again
from its recorded step with the same `op` id.

| Step | Split of A into A and B | Merge of D into K |
|---|---|---|
| 1 | `init` B with `[mid, hi)` and the head; B joins the push list | `range` K to the union: K accepts D's points |
| 2 | directory `split` | directory `merge(keep = K)` |
| 3 | `range` A to `[lo, mid)`: A closes the sockets that left | `drain` D: it refuses opens and closes its sockets with 4801 |
| 4 | | when D reports no socket, or after 30 s: `end` D (`delete-self`), D leaves the push list |

At every point both the old and the new map lead a client to an edge
that receives pushes; at worst an edge refuses with `moved` and the
client's next lookup is right. A new edge is created by the room's
`init` call before the directory names it, so a hand-off never meets an
edge without its configuration (an ended edge excepted, section 5).

Edges are named `<room name>.e<k>`, relays `<room name>.r<k>`; `k`
comes from the room's durable counter and is never reused.

### 8. The relay layer

A relay is stateless. Its push carries the messages once and a list of
`(edge, position)`; it runs one `call-many` to those edges, each with
its own slice, and answers the edges' answers.

- Up to 16 edges, the room pushes to them directly. With a 17th, the
  room creates relays and gives every edge a parent; the depth is the
  same for all edges. A new edge goes to the relay with the fewest
  children; a relay is created when all have 16 and ended when it has
  none. The layer disappears when the edges are back to 12 or fewer.
- One relay layer is the ceiling: 16 relays of 16 edges, 256 edges,
  128,000 sockets at `edge_cap` 500 and about 50,000 to 100,000 at
  steady fill (derived). Past it a split is refused and counted.
- A relay answers after its edges, so a round takes the slowest edge. A
  relay's own failure marks the relay suspect; an edge's failure comes
  back in the relay's answer and marks that edge.

### 9. Failures

| What happens | What follows |
|---|---|
| The room hibernates, is moved, or its node is drained | No socket is touched. The next publish or poke wakes it; a new epoch with nothing lost. |
| The room's node dies | Publishes and pokes fail until the takeover (`failed` or `503`; [failover](../failover-and-scaling.md)); clients stay connected and receive nothing. Then a new epoch; in a batched room without `confirm`, numbers above `base` are announced void. A pending operation resumes. |
| An edge's node dies | Its sockets die without a close frame; clients notice at once or by keepalive (55 s at most) and rejoin with backoff. The lookup still names the edge; the hand-off waits for the takeover, then creates it on a live node, empty, with its durable range. The room marks it suspect meanwhile and resends from its position after. The range is not given to other edges. |
| An edge is moved or its node drained | Sockets close with 1012; clients rejoin within 2 s and resume from the buffer or over HTTP. |
| A relay's node dies | Its edges miss rounds until the takeover; their clients stay connected and catch up from the next pushes. |
| The directory moves or its node dies | Lookups fail meanwhile, so joins and rejoins back off; open sockets and pushes are unaffected; scale operations wait in their row. No change answered `ok` is lost. |
| A push is cut by the call deadline | The entries that answered keep their answer; the others are suspect and get the same numbers again. |
| A client stops reading | Section 5: lagging, then `behind`, then 4802. |

### 10. Stats

No host change, so no new Prometheus series. The room answers
`stats()`: epoch, head, the window's first number, publishes, rounds,
the last and the longest round, edges, relays, sockets, lagging
sockets, slow closes, refusals by reason, suspects, splits, merges,
epochs with lost numbers, and one line per edge. The front may publish
it. The node's own series still count sockets, calls and refusals
([Monitoring](../monitoring.md)).

### 11. The example

`examples/ts-big-room`: the four classes, a front (sign-in by a chosen
name, `join`, `publish`, `history`, `stats`) and one static page served
by the front, using `room-client` with no framework. The page shows the
messages, a send box, the connection's state and counters (received,
repeats dropped, reconnects, gaps fetched, void, resets), and a button
that drops the connection. Icons, if any, are inline SVG. Not in scope:
accounts, presence, typing notices, private rooms, message edits.

### 12. No WIT change

The SDKs gain `sockets.trySend` in TypeScript (`"ok"`, `"gone"` or
`"backpressure"`; `send` returns one boolean for both failures) and
nothing in Rust, whose `websockets::send` already tells them apart. The
one host change this record allows is a `send to all my sockets` call,
and only if the 10,000-client benchmark shows the edge's send loop,
not the calls or the bucket, to be what limits a round.

## Consequences

- A room of tens of thousands of clients is four small classes and a
  page of front code; the protocol is the same in every app.
- The room stays one sequencer: publish rate is one actor's, whatever
  the number of clients.
- A split disconnects about half of one edge's clients for a moment;
  they resume by number. Growth is paid in short reconnects.
- A join costs four events (front, directory, room poke, edge open);
  the poke is a short event that does not push.
- Deep catch-up reads go to the room through the front, at the worst
  moment (after an edge dies). They are bounded reads, jittered by the
  client and refusable by the front.
- An idle room with clients sets no alarm and may keep more edges than
  it needs until the next publish or join.
- Lost messages are visible and announced in a batched room unless
  `confirm` is set.

## Alternatives

- **Edges call the room** (publish on the socket, fetch gaps, report
  counts). Two chains then wait on each other until the deadline
  (ADR 0025). An upward path through push answers is possible later.
- **A heartbeat alarm** to watch occupancy. A write and an index PUT
  every few seconds for every room with a client, forever.
- **The room assigns edges itself, least loaded first.** Balanced, but
  every join waits on the hot actor, and the directory would be a list
  nobody else can reuse.
- **Splitting a range without moving sockets.** The full edge stays
  full, keeps half its inflow and splits again and again into slivers.
- **Epoch from the clock, or no epoch and gaps in the numbers.** A clock
  is not ordered across nodes; gaps make "is anything missing" depend on
  a chain of previous numbers in every frame.
- **Always flush before a push.** Offered as `confirm`; as the only
  mode it adds a bucket round trip to every delivery.
- **Edges persist their buffer.** A write per push per edge, to protect
  what the room's history already holds.

## Tests (to build)

- SDK unit tests, both languages, shared vectors: points; split and
  merge (coverage of the space, versions, `expect`, a repeated `op`,
  the limits); the room's decisions for `history` (on the line, lost,
  reset); the scale pass as a pure function (thresholds, the minute
  rule, the pair rule, relay parents); the client's rules with an
  injected clock and random source.
- Each operation of section 7 interrupted after each step, then resumed.
- A cluster test: 300 clients, `edge_cap` 150 so that three edges or
  more appear by splits; the node of one edge killed; one client that
  stops reading. Every client still connected ends with every number in
  order, repeats only after a reconnect, the slow one closed with 4802
  and caught up after it returns.
- A crash of a batched room between a push and its upload: clients are
  told the void numbers and agree with `history`; an edge holding the
  lost numbers delivers the new messages that reuse them.

## Docs to change when accepted

[Designing for hot actors](../designing-for-hot-actors.md) (the planned
section becomes the pattern, with measured figures from `bench/results`),
[languages](../languages.md), [benchmarks](../benchmarks.md) (the
group benchmark), the API reference, the ROADMAP item.

## Open questions

1. Where new edges land. A call creates the edge on a node the cluster
   chooses; nothing here spreads a room's edges over nodes. Placement at
   wake-up is its own roadmap item.
2. Publishing on the socket, by an outbox the room collects in push
   answers.
3. Giving a dead edge's range to its neighbours during the takeover
   wait, instead of making its clients wait.
4. A second relay layer, and `push_delay`, the window and the buffer as
   measured defaults instead of chosen ones.
5. A front-side cache of lookups is impossible (a fresh instance per
   request); whether the directory needs a read path that is not an
   event is left to measurement.

## Plan

Each slice lands in Rust and TypeScript with its tests.

- **R1. Directory.** The module, the class, the client. Proof: the unit
  tests for points, split and merge.
- **R2. Room core.** Numbers, epochs, window, `publish`, `history`,
  `confirm`. Proof: the decision tests, and a history cut short as a
  crash would leave it.
- **R3. Edge and client.** Push, buffer, cursors, the client protocol,
  `sockets.trySend`, `room-client`, a fixed set of edges. Proof: client
  rule tests; a one-node test with a slow socket.
- **R4. Scaling.** Join and poke, the scale pass, split, merge, drain,
  end. Proof: the interrupted-operation tests.
- **R5. Relays.** Proof: 17 edges with a small `edge_cap`, one node.
- **R6. Example and cluster test.** `examples/ts-big-room` and the
  300-client test above.
- **R7. Benchmark harness and docs.** `bench/room*` speaks the protocol
  and records deliveries, repeats, gaps, void numbers and reconnect
  times; the hot-actors page.
- **R8. The 10,000-client benchmark**, last, on a multi-node cluster
  with a real bucket: growth from 0 to 10,000 and back to 2,000, an
  edge's node killed, 1% slow clients. It passes when no connected
  client misses a number, void numbers are only those announced, and
  the results file states delivery latency and reconnect time. It also
  decides section 12's host call.

### R3 implementation notes

R3 supplies Rust `EdgeActor` and `Round`, TypeScript `defineEdge()` and
`define({ edges })`, and the browser-only `room-client` entry. Fixed edge
addresses are application configuration, recreated at each wake; each edge
currently owns the full range. Range changes, join/poke and scaling remain
R4. Neither edge implementation makes outbound calls or persists delivery
state. The edge cap is 500.

The internal JSON push has a current epoch header and optional `e`/`b` on
its message items. Rounds include those fields to preserve R2's original
message identities when retained history spans epochs. The edge removes
these item fields from browser frames and emits a separate frame at each
epoch boundary. The push header's rollback base is the lowest retained
base since the child's acknowledged epoch; each message frame carries its
own epoch's base. An old acknowledgement above the current head is valid
following loss; only a current-epoch acknowledgement above the head is
invalid. If a retained row's epoch boundary has expired from the 64-epoch
window, the internal push uses `recover: true` with no messages: the edge
drops its buffer, advances to the room head and sends `behind` to lagging
cursors. HTTP history then supplies the authoritative `reset`; the round
never invents an epoch base.

R2's binary protocol retains its integer range. The JSON browser protocol
accepts nonnegative safe integers (at most 2^53 - 1); rounds reject values
outside that range rather than round them. A TypeScript front can return
`JSON.stringify(broadcast.historyJSON(history))`, importing `broadcast`
from `@wasmkeep/sdk/front`, after authorizing and calling R2 history.
The helper converts bigint positions, timestamps and epoch boundaries and
rejects unrepresentable numbers. JSON escaping can expand the R2 binary
history response; a single text that cannot fit the 64 KiB live frame goes
through `behind` and HTTP history instead.

R3 verification includes shared Rust/TypeScript push vectors, epoch and
cursor tests, handler validation, browser transport with an injected clock,
HTTP retry/cancellation, and a three-client real-node test. The latter uses
paced traffic and a bounded ping sequence to fill both byte and frame
permits: byte backpressure alone can leave room for `behind`. It verifies
4802, reconnect, paged HTTP catch-up (including window reset), and resumed
live delivery. The host close-slot fix is provided by main (#180); R3
changes no WIT or server implementation.


### R4 implementation notes

TypeScript rooms opt in with `define({ scaling: { directory:
"directory/bg:lobby", edgeClass: "edge", capacity: 500 } })`. Rust rooms
keep a `Round` and call `handler::scaling(&mut scaling, &mut round)` from
`on_alarm`, using `Scaling::new(ScaleHost { directory, room, capacity,
head }, "edge/lobby", capacity)`. `room` is the full caller address;
`head` is refreshed from the core on every alarm. Fixed R3 configurations
remain supported. Capacity may be lowered for tests. Direct scaling stops
at 16 children until R5 adds relays.

After authorization, fronts use TypeScript `broadcast.join(roomAddress,
directoryAddress, clientId, position)` or Rust `Client::join(directory,
clientId, position)`. These return the hand-off response. The directory
address names the room's own `bg:<room name>` map. Lookup and poke share
one batch, followed by one lookup retry for 404; poke failure is ignored.
Poke only schedules an alarm, including on a new room. Thus the first join
may still answer unavailable until the alarm initializes the first edge
and map; the reconnecting client retries. No join runs a delivery round.

Metadata and the single pending operation row are committed together with
`exec-durable`. Before executing any operation the room arms a retry alarm;
each external step can be repeated after an unknown outcome, including
initial creation and ending an already-ended edge. A successful checkpoint
never depends on an in-memory operation object. After a range change,
counts must be refreshed by a push before another scaling decision.
The one-minute split/merge timestamps survive cold starts. A merge keeps
both children in the push list until the drained edge can end (zero host
sockets, or the 30-second timeout). Range changes close unknown points
following hibernation as well as points that moved.

R4 tests cover thresholds, adjacent merges, durable checkpoint failures,
repeated steps, suspect backoff, and 4801/resume behavior. The real-node
fixture uses four clients and capacity five, directory-based hand-offs,
HTTP recovery during a split, the real one-minute hysteresis interval,
and a drain that reconnects a remaining client after merging.
