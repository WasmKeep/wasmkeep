# 0040. Directory, broadcast groups and the reconnecting room

- Status: accepted, as built (2026-11-17), slices R1 to R7: the directory,
  room core, edges, browser client, scaling, one relay layer, the large-room
  example, the 300-client cluster proof and the remote benchmark harness.
  R8, the 10,000-client benchmark, ran on 2026-11-19 at 3,000 to 10,000
  clients ([results](../benchmarks.md#a-big-room-on-three-nodes-preview)):
  the room holds its sockets without wedging, but its publish path keeps
  delivery p99 at 5 to 15 s, so the room ships as a preview in 0.6.0. The
  publish path comes before the optional host "send to all my sockets"
  call, which stays deferred.
- Amended 2026-11-20 (0.7.0, stage 1 of the publish path): the room's
  rounds, children, relays and scaling move to a fifth class, the
  **pusher**, which pulls from the room ([section 13](#13-the-pusher-amended-2026-11-20)).
  Sections 3, 4, 7, 9 and 10 still describe what is done; section 13 says
  which class now does it.
- Amended 2026-11-20, after load runs of 0.7.0: the
  room renders the browser's history page itself (`history-json`,
  [section 3](#3-the-room-numbers-epochs-and-the-history-window)), the
  client keeps its socket on a 5xx history answer and resets its
  reconnect attempts only once caught up ([section 6](#6-the-client-protocol)),
  and the pusher is deployed with batched writes ([section 13](#13-the-pusher-amended-2026-11-20)).
- Amended 2026-11-20: a pusher round's work is bounded in counts, not
  only in bytes ("A round's work", [section 13](#13-the-pusher-amended-2026-11-20)).
- Amended 2026-11-22 (0.8.0, item 2): a big room's edges wake spread
  over the live nodes by default, not on the node that wakes them
  ([section 14](#14-edges-spread-over-the-nodes-amended-2026-11-22)); the
  placement limit of open question 1 is lifted.
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
front calls the directory and the room. **An edge and the directory call nobody; a relay calls only its edges.** Everything an edge has to tell the
room travels in its answer to a push. Everything a client has to ask the
room (a publish, a gap) goes over HTTP through the front. This rule is
what keeps a batch from meeting itself (Context); the SDK's edge has no call to make and its relay calls only downward, and the docs tell an app not to add one.

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
A retained row validates its exact `(epoch, number)` cursor even after that epoch leaves the 64-entry ledger, allowing reset recovery to paginate without changing the wire format. The browser bounds pending live work to 64 frames and 1 MiB of conservatively accounted string storage, cancels obsolete recovery on overflow, and reconnects from the last applied position; each history request has a 30-second deadline (amended 2026-11-20: the waits between retries no longer count, see section 6).

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
| older than the window, or older than the 64 epochs kept without an exact retained row | `reset: {e, n}` of the first message kept, then the messages from there |
| absent (a new client) | the head only; `last = k` asks for the last `k` messages instead |

**The browser's page (amended 2026-11-20).** The binary `history` above
stays for the pusher and other callers. A front serving a browser calls
`history-json(after, limit)` instead: the same decisions, answered as the
browser's JSON (`{"head", "lost"?, "reset"?, "more", "messages"}`, `id`
omitted when absent), byte for byte what `JSON.stringify` of the
converted binary page makes, rendered by one SQLite statement
(`json_quote` escapes as `JSON.stringify` does; shared vectors check it).
The front returns the bytes unread. Its page holds at most 256 messages
and 64 KiB of bodies, at least one message, `more` when cut. A room of an
earlier release answers the operation 400: the TypeScript client then
converts the binary page itself, but the Rust `Client::history_json` has
no fallback, so a Rust front that uses it deploys with its room. Before, a
front decoded each page into objects and encoded it again in the guest,
so a reader far behind got full pages over the front's 50 ms budget (a
500), which the client then answered by reconnecting, again and again. Fuel per front history request on a full
window, the most over every page (deterministic, measured by
`typescript_big_room_history_page_fuel` in
`crates/wasmkeep-server/tests/big_room.rs`;
[raw measurements](../../bench/results/2026-11-20-2e614d8-front-history-fuel.csv)):

| Front | 256 messages of 200 B | 61 (now 15) messages of 4 KiB |
|---|---|---|
| QuickJS, before | 245 M | 937 M |
| QuickJS, `history-json` | 1.1 M | 1.1 M |
| StarlingMonkey, before | 58 M | 47 M |
| StarlingMonkey, `history-json` | 4.9 M | 4.9 M |

A JSON number above 2^53 - 1 is refused (`invalid browser integer`)
rather than rounded, as the front's converter does.

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
  and an optional `stalled` count (lagging sockets whose send hit backpressure),
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

**Per-event bound (amended 2026-11-18).** No room event's work grows with
the room's state. Publish reads the head and an `id` by index and trims the
window with one statement SQLite runs; history reads the page it answers
in 32-row pages; a round reads the messages after each distinct child
position once and builds at most 1 MiB for all its children together, at
least one slice. Each row read is charged its JSON plus 1 KiB, an empty
one too, so a round reads about 1,000 rows at most whatever the bodies
(since the amendment of 2026-11-20 to section 13, at most 314: a round's
work is counted too);
the other lagging children keep their positions and are pushed by the
alarm 20 ms later, the next round starting at the first child deferred,
so under sustained publishing every child takes its turn. A child left
below the window gets a push past its position: its edge announces
`behind`. Sizes come from SQLite (`length(CAST(body AS BLOB))`) or
from the JSON text, never from re-encoding text in the guest. On QuickJS,
which has no native UTF-8 codec, the room, edge and relay classes convert
bulk text through SQLite (`CAST`), which runs natively; the SDK's
`TextEncoder`/`TextDecoder` have native fast paths for the rest. Before
this, publish, history and each round read the whole window, and a push
was re-serialized after each message it took: a room that had once filled
its window could exceed the default 50 ms budget on every event, each
trap discarding the instance and bumping the epoch.

When every lagging socket in an edge's answer is stalled, repeated unchanged
answers back off the retry alarm from 20 ms by doubling, capped at the
45-second keepalive interval. New positions, changing stalled counts, or
catch-up work reset it to 20 ms. New publishes still schedule their normal
push immediately; healthy sockets do not wait for a stalled reader's retry.
Older edges without `stalled` retain the 20 ms retry behavior.

### 5. The edge

**Per-event fan-out bound.** Both SDKs index retained messages by number and
keep each item's JSON from insertion. A push groups sockets by cursor and
builds one frame per distinct cursor, with the caught-up group first. It
builds at most 16 frames or 1 MiB of frame bytes per event (each frame remains
at most 64 KiB); per socket it only sends the shared frame. Deferred groups
keep their cursor and count as lagging. A cursor advances only on `ok`;
below the buffer floor it still receives `behind`, then closes with 4802 if
that notification is backpressured. No WIT change is required.

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
sends each scheduled cursor group one shared frame with the messages after
its cursor (64 KiB at most per frame), within the per-event bound above.
Each socket cursor moves only when its send succeeds. On `backpressure` the cursor stays: the socket is **lagging**
and is tried again at each push and at each frame from that client.
When a lagging cursor falls below the floor, the edge sends `behind`
and sets the cursor to the head: the client fetches the gap over HTTP
(section 6) while live frames resume. If `behind` itself meets
`backpressure`, the edge closes the socket with 4802. A slow client
thus costs the edge a cursor, never a copy of the stream.

**After a hibernation** the buffer, the cursors and the points are gone
while the sockets remain. The edge then knows no head until the next
push or call from the room, and at a range change closes every socket
whose point it no longer knows.

(Amended 2026-11-20.) On the first push after such a wake, the edge
places every socket that has no cursor at `first - 1`, so the messages
the push carries go out; their frames carry each message's own epoch,
base and number, and rules 2 and 3 apply, one frame per socket. A
header-only first push has nothing to send: the edge then sends each of
those sockets `{"t":"ready","e":E,"b":B,"n":first - 1}` (the push's
epoch and base), so the client learns where the stream starts. A client
whose own position is lower fetches the gap over HTTP; one that missed
nothing makes no request. If that `ready` meets `backpressure`, the
socket closes with 4802, as for `behind`. (The announcement is not sent
with messages: at 500 sockets a second frame each doubles the host sends
of a cold event, a trap there repeats the wake, and the push's epoch can
be newer than its first messages'.) A `ping` is always answered:
before the first push, with `{"t":"pong","e":0,"n":null}`. Before this,
the edge placed those sockets at the push's `head` before applying it:
the messages of that push never reached them, nothing told them so, and
a ping got no answer until a push came. A client that had received 373
when a header-only push said 375 stayed at 373 until the next publish.
A socket that joined the woken edge before that push already had its
`ready` with `n: null` and gets no second one.

(Amended 2026-11-23.) "First push" means the first since the instance
started, whatever head it already has:

- A client that joined the woken edge before that push was told
  `n: null` and checks its position over HTTP. The push no longer sends
  it `behind`, which made it fetch a second time: it starts, with no
  frame, where the push starts, and the push's messages follow it live.
- A head the edge took from a stored configuration (`init`, `range` or
  `drain` before any push, with nothing buffered) no longer places the
  sockets it kept at that head in silence. The first push places them as
  above: at `first - 1` when it carries messages (the edge starts there,
  below the stored head if need be, since it has nothing buffered), and,
  header-only, at the edge's head, announced by a `ready` with `b`.
  Clients that joined after the stored head resumed from it and keep
  their cursor.
- A first push with the `recover` flag starts the edge at its head, and
  announces it the same way. A socket found gone when told is forgotten.

A wake stays at one frame per socket. Shared vectors
(`wake-vectors.txt`) cover each case in both SDKs, and
`room_edge.rs`'s `wake_500_*` tests wake an edge holding 500 sockets,
header-only and with a message: under 5 M fuel in Rust, 100 M on QuickJS and 250 M on
StarlingMonkey (whose test runs in `--full`), under the 500 M the 50 ms
budget is calibrated for.

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
| `{"t":"ready","e":4,"b":100,"n":120}` | On an open socket, from an edge woken from hibernation by a header-only push (section 5): the stream resumes after `n`; `b` is the epoch's base. |
| `{"t":"m","e":4,"b":100,"n":121,"m":[{"a":1763400000000,"i":"k1","d":"hello"}]}` | Messages `n`, `n+1`, ... of epoch `e`, whose base is `b`. |
| `{"t":"behind","e":4,"n":180}` | Fetch what you miss up to here over HTTP; live frames follow. |
| `{"t":"pong","e":4,"n":180}` | Answer to `ping`, with the head. `"e":0,"n":null`: the edge knows no head yet. |

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
   there is a gap. A `ready` with `b` (amended 2026-11-20) first applies
   `b` as rule 2 does when its epoch is `last.e + 1`; it is a gap only
   if its epoch then differs from `last.e` or its `n` is above `last.n`.
   It does not change the connection's state. An older client ignores
   `b` and takes the `ready` as a gap whenever its `e` or `n` differs from
   `last`: since the room bumps its epoch when it wakes, that is one
   request per older client after each header-only wake. A `pong` with
   `n: null` is a keepalive only. An older client rejects it as invalid
   (`onError`) and reconnects; in a quiet room, whose edge hibernates
   between its pings, it thus cycles about every 45 s, where before the
   edge's silence made it reconnect about every 55 s.
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
   connection has stayed `ready` for 10 s after its history caught up
   (amended 2026-11-20: not while still behind, so a client that cannot
   catch up does not reconnect at the fastest rate forever). A failed
   open (a browser sees only 1006) is one more attempt.
6. HTTP answers `429` and any `5xx` are retried, the socket kept, after
   `Retry-After` plus the same jitter; after a `5xx` other than `503`
   (a room unavailable, not a page too big) the next request asks for
   half the page (`limit`, which the front clamps to 1..256), and each
   answer doubles it back. Only a `4xx` is an error (amended
   2026-11-20: a `500`, the front's CPU budget cut by a full page, used
   to close the socket, and the client rejoined behind as before). Each
   request has a 30 s deadline; the waits between them do not count.

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
- One operation at a time, and one durable step of it per event (amended
  2026-11-18; it was four whole operations): the alarm, set to now, runs
  the next step, so publishes, pokes and history wait behind at most one
  step's calls and durable save, never a scale pass of several operations. `range` and `init` answer the edge's socket count, and an
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
| A client stops reading and the node's write to it waits past the write-idle limit (60 s, [security](../security.md)) | The node closes the connection before section 5 runs its course: the edge sees the socket end with 1006, not 4802, and the client rejoins as after any abnormal close. |

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

### 13. The pusher (amended 2026-11-20)

**Why.** A round waits for its fan-out through relays to edges
(section 8: it takes the slowest edge), so its length grows with the
clients and the tree, and publishes, pokes and history queue behind it
in the room's one mailbox: the sequencer spends its time waiting on
delivery, and the room's own rounds, not its publishes, set the
publish latency of a big room.

**Five classes.** The app deploys a **pusher** beside the directory, the
room, the edge and the relay, `internal` like them. It is addressed from
the room's name: the room `room/lobby` has the pusher `pusher/lobby`
(the class names are options in both SDKs; `room` and `pusher` by
default). The pusher takes everything that delivers:

- the round alarm (section 4), with the children's positions, suspects,
  lag retries and the resume point;
- the relay layer (section 8);
- scaling (section 7) and its durable state: the scale row, the
  operation row, the child counter, the minute rule's times, the refusal
  count; the directory map's owner is the pusher;
- `poke`, and the topology part of `stats` (section 10).

The room keeps publish (numbers, epochs, the window), `history`, and a new
read-only `pull`. It sets no alarm and holds no per-child state, in memory
or in SQLite.

```text
publishers ──HTTP──▶ front ──call──▶ room                (publish: numbers)
                       └──call──▶ pusher                 (poke, only when the room asks)
pusher ──call──▶ room                                    (pull: messages after a position)
pusher ──call-many──▶ [relays] ──call-many──▶ edges ──▶ sockets
pusher ──call──▶ directory                               (split, merge)
```

**Calls still go one way** (section 1): front to room, front to pusher,
pusher to room, pusher to relays, edges and directory. The room never
calls the pusher, so a pusher waiting on a pull never meets itself; the
pusher's call to the room is a short read the room answers from SQLite.
No WIT change, no host change.

**The pull contract.** `pull(after, budget, idle)` on the room's path
prefix, framed with the `wire` module like `history`, accepted from the
room's pusher only (`403` otherwise: an idle claim from anyone else
would silence the wake):

| Field | Meaning |
|---|---|
| `after` | optional `(e, n)`: a child's acknowledged position. Absent: the header alone, for a child whose position is unknown. |
| `budget` | what is left of the round's 1 MiB, at most 1 MiB, and since 2026-11-20 no more than the rows the round's work bound leaves (below): it buys rows at 1 KiB each (at least one); `0` asks for the header alone. |
| `idle` | the pusher is about to stop clocking itself (below). |

The answer is the push header a child at `after` needs, then the messages:
`(e, b, first, head)`, a `recover` flag, and up to 513 messages, each
`(n, e, b, at, id?, body)` with its own epoch and that epoch's base. `e`
and `head` are the room's; `b` is the lowest base of the epochs after
`after.e` (the current epoch's base without `after`); `first` is the first
number carried, or `head + 1` when none is. A position above a later
epoch's base starts at that base (the numbers are void); a position below
the window starts at the window's first message, so `first > after.n + 1`
and the edge announces `behind` (section 5); a row whose epoch boundary
left the 64 kept gives `recover` and no messages. The bound is the round's
(section 4): each row is charged its body and id bytes plus 1 KiB,
at most 512 KiB of bodies and 513 rows, at least one row when `budget` is
not zero, whatever the window holds; the room reads only those rows.
(The budget counts rows, not bytes: the 512 KiB cap bounds the bytes,
and the pusher charges what it builds against the round's 1 MiB.)
Body bytes come from SQLite (`CAST`), never re-encoded in the room.

`pull` is read-only but for three things, each as `history` already does
or the room did before the split: the first operation after a cold start
seals an epoch durably (section 3); with `confirm`, the pull flushes first
and answers `503 not durable: still uploading` until the upload is in,
so the pusher retries 50 ms later and no number is ever pushed and then
lost; and the idle flag below, which lives in memory.

**A round over pulls.** The round is section 4's, unchanged in its cuts,
budget and resume point. It pulls once per distinct child position
(children at one position share the pull, as they shared a slice), plus
one header-only pull when a child has no position, each pull taking what
is left of the round's 1 MiB. Pulls in one round can see different heads
or epochs (a publish, or a room restart, between them): each child's push
is that child's pull's header, and its acknowledgment is checked against
that header. A pull that fails defers the children that needed it, as the
budget does, 20 ms later. A round pulls at most 16 distinct positions;
children at further positions are deferred to the next round, so many
laggards never make one round a long chain of synchronous calls to the
room.

**A round's work (amended 2026-11-20).** The 1 MiB bounds bytes, not
work: with small bodies a round over children lagging at distinct
positions read about 1,000 rows and pushed as many, about 455 M fuel on
QuickJS and StarlingMonkey, close to the 500 M the 50 ms budget is
calibrated for. A guest cannot read its fuel, so a round also counts its
work, the same way in both SDKs: 8 for each pull, 3 for each request to
a child (a push or an init), 1 for each row read and 1 for each row
encoded into a push. Pushed straight to edges, a row counts once per
child it goes to; through relays, once per slice, since the relays'
packs are the only encoding (the round no longer encodes each child's
push there). A round does at most `ROUND_WORK` = 640, besides the 1 MiB
and the 16 pulls:

- A pull is sized for the children at its position this round: the work
  left after its own 8 and their 3 each buys rows (`budget` above), each
  counted once read and once pushed (once per child when direct), at
  least one. One child: at most 314 rows; 127 relayed children at one
  position: 125 rows, to every one of them.
- A push is cut at the rows left, as at 512 messages; a child whose push
  would carry none of the messages it needs is deferred. The JSON bytes
  count against the 1 MiB the same way, once per slice when relayed.
- The children the bound leaves out keep their positions and are pushed
  20 ms later, the next round starting at the first of them, as the byte
  budget defers: delivery stays eventual and in order. A child already at
  the head this round read needs no messages: it goes first next round
  and sets no alarm, so a group at its head still goes idle, unless its
  last answer asked for a retry (its sockets lagging, or a stalled
  reader's backoff), which it keeps. A child left out keeps the socket
  count it last answered, which scaling reads.
- The first child the round reaches is always pushed, at least one row.

The weights come from measurements on QuickJS, where a row read and
pushed cost about 0.4 M fuel and a push request about 0.7 M (0.75 M
relayed). Pusher alarms in `tests/big_room.rs`, against 455 M before:

| Children | Each round | Fuel |
|---|---|---|
| 256 at distinct positions | 1 pull, 314 rows to 1 child | about 166 M |
| 127 at one position (8 relays) | 125 rows to all 127 | about 210 M |
| 256 at one position (16 relays) | 1 row to 210 children | about 157 M |
| 256 after a restart | headers to 210 children | 215 M |

The requests are the fixed cost: 8 + 3 × 127 = 389 of the 640 at 127
edges, which leaves 125 rows a round; at 256 edges the requests alone
fill a round, so a slice moves one row a round, to 210 edges, and the
rest follow 20 ms later. On StarlingMonkey the rounds take about 160 M,
but the first alarm after a restart still takes about 430 M (453 M
before), most of it not the counted work; its distinct-positions test
stays `--full` only.

**Waking the pusher.**

- While some child's acknowledged position is below the head, or a child
  is lagging, the pusher sets its alarm 20 ms later, as rounds do today.
- When its next alarm would be later than 20 ms (none, a stalled reader's
  backoff, a suspect's wait), the pusher ends the round with an **idle
  pull**: `after` at the head it delivered, `budget` 0, `idle` set. The
  room records the flag only if that position is its head; the answer's
  head tells the pusher whether a publish came meanwhile, and then it
  clocks itself 20 ms later instead.
- Any other pull clears the flag. A cold room starts with the flag set,
  and a flag older than 60 s without a pull (longer than an event's wall,
  30 s by default) counts as set, so a pusher that lost its alarm is woken
  by the next publish.
- The publish answer carries one more field, `poke`: the flag. The front
  (`Client.publish` in both SDKs) pokes the pusher only then, with a
  publish poke, which brings the pusher's alarm to 20 ms from now when it
  is later or unset. An idle pusher's mailbox is empty, so the poke
  returns at once. A failed poke is tried up to three times, then
  ignored; the flag stays set until the pusher's next pull, so the next
  publish pokes again.
- **The delivery guarantee.** A poke can be lost for good: the pusher's
  node dies after its idle pull, a publish then pokes an address that
  answers nothing until the takeover, and a quiet room sends no later
  publish. In 0.6.0 the room's own durable alarm, set in the publish
  event, guaranteed the push. So, while its group holds sockets (or a
  child did not say how many), an idle pusher **always keeps one durable
  alarm**, at most 60 s out, which the sweep delivers on a live node
  after a takeover: an acknowledged publish reaches the edges within that
  time even if nothing else ever comes. After delivering something new it
  keeps it 5 s out instead, so a recent burst recovers sooner. An empty
  group keeps none. This is the heartbeat the Alternatives below set
  aside, bounded to rooms with clients: one alarm write and one
  header-only round a minute per idle room.
- A scale step that fails (a directory or edge unreachable, the edge
  ceiling) is retried 2 s later as before, but no longer fails the
  pusher's event: the round's result and the idle pull stand, so
  publishes still poke it and delivery does not wait on the step.
- A join's poke (section 7) now goes to the pusher, keeps its old rule
  (an alarm now if none is set and the last round is more than 1 s old)
  and still answers at once.

**Stage 1 still blocks.** A round still waits for its slowest child,
`call-many` being synchronous, and the pusher's other events (pokes) wait
behind it. Publishes, history pages and pulls do not: they are the room's,
and the room's events are now its own. A slow edge delays delivery, never
an acknowledgment. Not in this stage: a round that does not wait, and
several pushers per room.

**Authority.** The pusher is now the controller every edge and the map
know. A new edge's configuration names the pusher in `room` (the field
keeps its name: it is the address whose `init`, `range`, `drain`, `end`
and direct pushes the edge accepts), and the pusher creates the directory
map, so it is the map's owner. Two operations make that transferable,
each accepted from the current controller only and idempotent:

- edge `handover {"room": <new controller>}`: rewrites the configuration's
  `room` durably; already naming it, or no configuration, answers `200`;
- directory `transfer(owner)`: one durable transaction, the map's version
  unchanged; already owned by `owner` answers `200`.

**Moving a room deployed under 0.6.0.** Its room holds the scale row, the
operation row (if one was under way), the directory map's ownership and
every edge's `room`. Nothing moves until the new pusher asks; deploy all
five classes together (the 0.7.0 front reads the publish answer's new
field and the 0.6.0 front would refuse it: the publish is numbered, but
the 0.6.0 front answers an error, and a retry without an `id` numbers it
again).

1. The pusher's first alarm finds no scale state of its own and calls the
   room's `adopt(read)`: the room's legacy rows, as they are (each SDK
   reads its own format). With none, the pusher starts a new group, as a
   0.6.0 room did. With some, it saves them durably as its own state with
   a migration row, operation included.
2. Each following alarm runs one `adopt(handover)` step: the room, the
   one that edges and the map still accept, calls the directory's
   `transfer` to the pusher, then `handover` on up to 16 of the adopted
   children per step (16 steps at most for 256 edges). This is the only
   time a room calls anyone, and only on its pusher's request: a call
   down from the pusher through the room to edges and the directory,
   which call nobody, so no chain comes back.
3. `adopt(retire)`: the room deletes its legacy rows durably, then the
   pusher deletes its migration row. A pending operation then resumes in
   the pusher from its recorded step (section 7).

Until the last step the pusher runs no round and no scale pass; clients
stay connected and receive the backlog once it is done, a few alarms
later. Every step can be repeated after an unknown outcome. A 0.6.0 room
may still have an alarm set: it fires once and does nothing. The room
also still answers a 0.6.0 front's poke, with nothing. A fixed-edge room
(R3's `edges`, a test configuration) keeps no list the room could hand
over: its edges' rows name the room, and are recreated.

**Wire details.** The pusher's one path is `poke` under the prefix
`wasmkeep-pusher/v1`
(an empty body for a join, `u64` 1 for a publish). `adopt` takes a kind
(0 read, 1 hand over: the directory's address or empty, then up to 16
edges, 2 retire). A step that continues at once sets the alarm 1 ms
after its event's time, not at it: a host before 0.7.0 clears, after
`on-alarm`, an alarm still at the delivered time, so a punctual delivery
would stall the operation (this was already so for the room's scale
steps). On such a host `get-alarm` inside `on-alarm` also answers the
alarm being delivered: the pusher's idle decision reads the round's own
next alarm instead. Since 0.7.0 the host keeps what the handler set, the
delivered time included, and `get-alarm` there answers none until the
handler touches the alarm ([ADR 0009](0009-alarms-in-a-cluster.md),
amendment of 2026-11-20); the SDKs keep both precautions, which are right
on either host, as long as they support hosts before it (a component
cannot tell which host it runs on).

**What a pusher cold start rebuilds.** Its scale state is durable, so a
pusher that wakes, moves or loses its node keeps its children, relays
and pending operation; the directory and the edges were changed only
by its own recorded steps. What it loses is memory: positions,
suspects and lag retries. Its first round is header-only, and the
answers rebuild the positions, as a 0.6.0 room's cold start did.

(Amended 2026-11-23.) It keeps one position. When it goes idle with
every child initialized and acknowledged at the head its idle pull
confirmed, the pusher writes that head to its own SQLite
(`wasmkeep_pusher_idle`, a plain write), and it deletes it when a round
leaves it busy: at most one write each way per burst of publishes, none
while it stays idle. A cold start reads it and gives it to every child
that has no position, so its first round pulls from there. In a quiet
room whose room, pusher and edges all hibernated, the publish that wakes
them is then pushed, not announced: each edge's sockets get it in an `m`
frame of the new epoch whose base is the clients' position (rule 2:
nothing void; rule 3: no gap), and no client fetches history. Before,
the first round was header-only, each woken edge told its sockets the
new head with a `ready`, and every client of the room fetched that one
message over HTTP at once.

- A child that is elsewhere answers its own position, as after any push:
  one above drops the repeats; one below (a lost delete after a crash
  mid-burst) gets a hole and sends `behind`, as a child below the window
  does.
- A position the room refuses (`invalid position`) is dropped and those
  children start with none, the header-only round. So does a missing
  table, a failed read or a lost write.
- Not the room's base: a cold room's new epoch starts at the last number
  published, not the last delivered, and a pusher that was busy when the
  room restarted would push past an edge's position (a `behind` where
  the old round caught it up). Nor a jittered or conditional fetch:
  either still makes every client of the room ask for the same message.
- No wire or pull-contract change: a pull at a position is 0.7's, and a
  0.7 pusher simply has no position to read.

`room_edge_full_hibernation_pushes_one_publish_with_no_history_fetch`
(Rust) and `room-round.test.ts` (TypeScript) test it; `room-model.test.ts`
restarts its pusher from the kept position among its faults.

**Write mode (amended 2026-11-20).** Deploy the pusher with batched
writes ([ADR 0034](0034-batched-plain-writes.md)). Its per-round write is
its alarm; everything that must survive a crash is committed durably;
child positions are never stored, but for the idle position above, a
plain write a crash may lose. Strict, a busy pusher spent most of its
time on the replication of its own alarm moves: each one, and its alarm
index entry, a bucket round trip before its next round. Batched loses
nothing that matters:

- the scale state, the operation row and the migration step are each
  committed with `exec-durable`, which waits for the acknowledged head
  in a batched class too (the other migration writes land in those same
  transactions);
- positions are rebuilt from the edges' answers (above); a lost idle
  position leaves the header-only first round;
- an edge drops numbers it already has, and the client's rule 1 drops
  repeats, so a round repeated after a lost alarm move delivers nothing
  twice to the application;
- an alarm move lost with the node leaves the last acknowledged alarm,
  which the sweep delivers on a live node: the idle alarm (at most 60 s
  out), or a scale step's 2 s retry; and a room whose pusher stops
  pulling counts it idle after 60 s, so its publishes poke it again.

`room_cluster_batched_pusher_crash_keeps_its_operation_and_delivers`
crashes a batched pusher with its alarm moves unacknowledged and a scale
operation under way.

**Stats.** The pusher answers the topology (the example's `stats` route);
the room's head and window come from `history`.

### 14. Edges spread over the nodes (amended 2026-11-22)

Since section 13, edges are woken by the pusher's calls (`init`, then
its rounds), and a call wakes its callee on the caller's node. With the
room and its pusher kept apart
([ADR 0041, amendment](0041-placement-at-wake-up.md#amendment-2026-11-22-keeping-a-room-and-its-pusher-apart)),
the whole tree therefore woke on the pusher's node, one node doing every
edge's send loop, until that node passed placement's floor. Spreading
moves fan-out CPU only: the room stays the one sequencer.

**The rule.** In a version with classes named `room`, `pusher` and
`edge` (the broadcast SDKs' default names), a wake-up of an `edge` actor
goes to the **rendezvous choice for the edge's own address over the live
nodes, the waking node included**. Edges are named `<room>.e<k>`, so a
room's edges each have their own choice and spread about evenly. When the
choice is the waking node, the edge wakes there; otherwise the event is
sent there as a drain sends it (a call or request forwarded, an alarm
handed off), and that node acquires it. It applies under the conditions of the room
and pusher rule, through the same path:

- the wake-up is this node's own: a call from an actor or the front
  here, an alarm no node handed here, or a request from outside that this
  node runs from its copy in memory (after the load rule's placed hop was
  refused, or a hop sent apart was not run). A request whose body streams,
  the ordinary case, is never spread; the SDK's edge is `internal`, so in
  practice no request from outside reaches it. A socket for an edge
  nobody holds is the one exception (amendment of 2026-11-26 below): its
  home takes the edge first;
- the route read answered "nobody holds it": a record naming a node, or
  any "try here" (a takeover, a stub, a failed read), is the ordinary path;
- the edge is not parked here (ADR 0024), and placement is on
  (`--no-placement` turns it off with the other rules).

The live nodes are this node and ADR 0014's eligible peers: same app,
live, `serving`, not asked to drain, not suspected, reachable, and not in
placement's backoff (ADR 0041, section 6), so a placed hop that a peer
refused is not sent straight back to it. So:

- **One hop.** The receiver acquires what it is sent and never judges it
  again. A peer that answers that it did not run the event (a host `503`
  before the actor) is suspected and the event runs here, once. **A
  wake-up is never refused**: with no eligible peer, the edge wakes where
  it lands.
- **A stale hint is not a failed peer.** A node keeps the owner it last
  read for an actor for one second (`HINT_MS`). A receiver whose hint
  still names the node that released the edge a moment ago would answer
  "ownership is changing", and its sender would suspect it, leaving every
  other edge woken in that window beside the pusher. So a receiver of
  another node's event that finds another owner forgets its hint and
  reads the record once more: nobody holds it, and it acquires. If the
  record names another owner, it answers `503` with `x-wasmkeep-owner-seen:
  <owner>`, and the sender does not suspect it: it runs the event by the
  ordinary path, which goes to that owner. A receiver that is draining
  answers without the mark and is suspected, as before.
- **The same choice from every node.** The set does not depend on which
  node wakes the edge (the pusher's, a relay's, the sweep's), so two
  nodes waking one edge agree, and a test can say in advance where each
  edge goes.
- **The room's node is not avoided.** About a third of a room's edges on
  three nodes share the room's node. Avoiding it would take a read of the
  room's record on each cold wake (the waking node, the pusher's, does
  not hold the room), and on three nodes avoiding both the room's and the
  pusher's node would leave one node for every edge.
- **The load rule does not apply to an edge** (amendment of 2026-11-26).
  Until then, past the floor, a call that placement's load rule (ADR
  0041, sections 1 to 5) sent to a lighter node was a placed hop, not
  spread again by its receiver. With a home, the rebalance would move
  that edge back at its next quiet pass: two rules cannot both be one
  actor's fixed point, and the name rule is the one every node agrees on.
  The receiver's own limits stay: a node with no resident slot answers
  `503` before the actor, and the edge wakes where it was called. With
  rebalancing off, the load rule applies first, as before.

**What does not move** (rewritten 2026-11-26). The rule gives each edge a
**home**: the choice above, over this node and the peers that serve its
app. An edge on its home never moves while the live set is the same, and
an edge that hibernates with sockets keeps its socket home (ADR 0024):
its record still names its node, so the next push goes to it. An edge
away from its home, because it woke while the live set was smaller (a
drain, a dead node) or a socket woke it on a node that could not reach
its home, is moved there once the cluster is quiet (below). So clients
reconnect for what section 7 already says (splits, merges, drains, a dead
node), and once more for an edge that comes home.

**A rolling restart** (rewritten 2026-11-26). A draining node closes its
edges' sockets (1012), releases each edge's record and hibernates it: it
hands no edge to a peer (only alarms are handed off). The first event
for a released edge wakes it on its home over the nodes still live:
the pusher's next push by the rule above, and a reconnecting client's
socket because its home takes the edge before the socket is relayed
(below). When the drained node serves again, the edges whose home it is
(about one in N on N nodes) are moved back to it by the nodes holding
them, once it has served for the settle delay. Rendezvous gives that
property: when a node leaves the set, only the edges whose home it was
change home, and when it returns, exactly those get it back. With two
nodes, every edge is on the remaining node while the other restarts, and
half return.

**Measuring it.** The release's fleet check: three nodes with default
placement, clients connected through every node (as a balancer spreads
them), a 4,500-client hold. `wasmkeep actor where` for the room, its
pusher and its edges, 20 s into the hold, should name at least two nodes
for the edges; record `wasmkeep_placement_spread_total{outcome}` from
each node beside it. Then restart one node at a time: after a node
reports `serving` again, wait at least one heartbeat refresh (so the
others see it serving) before draining the next, and repeat `actor where`
and the metric after the last. Since 2026-11-26, the step waits for the
app's `cluster status` to say `balanced` (or `paused (mixed versions)`
during an upgrade between minor versions) instead of one refresh.

Not spread: the room, the pusher, the directory and relays (relays exist
only past 16 edges, and wake beside the pusher). For other class names,
the per-class key `spread` asks for the same rule (`0.9.0`,
[ADR 0041, amendment of 2026-11-25](0041-placement-at-wake-up.md#amendment-2026-11-25-apart-from-and-spread-for-an-apps-own-class-names)).

**Mixed versions.** Nothing new in the bucket or on the heartbeat: a 0.7
node keeps the edges it wakes and acquires those sent to it. It does not
send `x-wasmkeep-owner-seen`, so a sender suspects it for a stale hint as
before, and it ignores the mark.

**Metrics.** `wasmkeep_placement_spread_total{outcome}`: wake-ups of a
spread edge, `sent` to its node or woken `here` (this node is the choice,
or no peer can take it), and, since 2026-11-26, a socket's edge taken by
its `home` or not (`home_failed`).

**Tests.** `placement::spreads` and `placement::spread`, and the peer
answer classifiers with the mark (unit tests); `tests/cluster/spread.rs`,
three nodes with default placement:

- one `call-many` of the pusher wakes twelve edges, each on its predicted
  node, on every node, one hop each, none placed by the load rule;
- none moves on later calls from any node;
- a drain releases a node's edges and the pusher's next push wakes each
  on the choice over the two others, the others staying, though a node
  still holds a hint naming the drained one;
- a receiver whose hint names the node that just released the edge
  acquires it, and is not suspected;
- a chosen node with no resident slot answers `503` before the actor: the
  edge runs on the pusher's node, once;
- one node, and placement off, wake every edge where it is called.

Not tested: a placed hop refused and then spread (the backoff rule), and
a client's socket racing the push through a drain.

#### Amendment 2026-11-26: edges return home

A rolling restart left the edges where they woke during it: the
restarted nodes came back with few edges or none, and the others kept
the rest, so the tree's send loops ended unevenly spread. Nothing looked at a held edge again; the only tools were a drain and
`wasmkeep actor move`. The same happens on one version; an upgrade is
only where it shows most.

**Home.** For an actor of a class that spreads (the built-in `edge`, or
a class with `spread = true`), its home is the choice of the rule above,
computed over this node and the peers of its app that serve on heartbeat
line 9, are not asked to drain and can be reached. Peers in placement's
backoff, and suspected ones, stay in the set: those say a hop failed a
moment ago, not that the peer should lose its edges.

**The pass.** Once per heartbeat period, in the loop that acts on move
requests, and again each time one of its moves ends, a node that is not
draining, whose lease is valid and that can open the app's secrets moves
the spread actors it holds (resident, or parked with sockets) whose home
is another node, in the order of their addresses. It starts nothing
unless:

1. placement and rebalancing are on;
2. the cluster is quiet in this node's view: no node of the app is asked
   to drain, every live one shows `serving` (a drain shrinks the set, so
   homes change for its length), and none of the app stopped or went quiet
   less than ten minutes ago (amendment of 2026-11-28, below);
3. every live node runs this node's minor version (patches of one minor
   do not pause it): during an upgrade a move toward a node about to be
   restarted is a wasted reconnect.

Then, per actor, its home must have been seen `serving` by this node
without a break for three heartbeat periods on its own clock (a new
process starts the count again), answer take calls, and have room by ADR
0041, section 4's candidate conditions (a fresh load with both room
fields, resident room above 0, socket fill below 800); it must not be
backed off after a failed take (below); and an actor this node acquired
through a take call while it is not the actor's home in its own view (an
operator's move off home, or a move or cold socket whose sender saw
another home) stays a minute before it is moved again. A take at the
actor's home in the receiver's view arms no stay: nothing would move it
from there while the views agree. An actor acquired any other way (a
wake-up, a takeover, a drain's release) has no wait of its own: the edges
a restarted node should get back were acquired by the others during its
drain, less than a minute before it serves. An actor that a move is
under way for, that an operator's move request names, that a restore
holds, or that is backed off after a move that did not finish here
(below) is left alone.

A move is the owner's sequence of ADR 0017, started from memory: close
the actor's sockets with 1012, flush, record the move, release, send the
target a signed take call. It writes no move key and reads none. From
before it closes the sockets until the release (for a parked room,
until its take), the actor is marked
closing on its node: an upgrade for it there is answered `503` with
`Retry-After: 1` (counted as `moving` in
`wasmkeep_admission_refused_total`). After the release, the move is
recorded and upgrades there are relayed to the target. Without the mark, a client that reconnects at once is
relayed back by a node that still names this one as the owner, reopens a
socket, and the move finds it busy, every time. An operator's move (ADR
0017) and a drain's hand-over (ADR 0014) close sockets the same way and
take the same mark. A socket's registration checks the mark again under
its lock, so an upgrade that passed the first check just before the mark
is either refused, or registered before the move closes the sockets. A
socket registered then may still be pending (not yet accepted), so the
first close does not list it; the move keeps waiting while any socket of
the actor is reserved, and closes each one as it is accepted. A move
ends:

- `moved`, or `taken_elsewhere` (another node took it in the gap): done;
- `busy`, for a cause of this node's (a socket still closing, the
  release pending, the actor no longer held here): that actor is backed
  off, 10 s doubling up to 160 s; the target is not blamed;
- `failed`, the target refused the take or did not answer: the target is
  suspected and backed off, a heartbeat horizon doubling on each failure
  in a row up to 160 s, for moves and cold sockets alike. A take answered
  `503` is not a failure (amendment of 2026-11-28): it is sent again for
  2 s, and one answered `503` throughout backs the target off the same way
  without suspecting it (counted `busy`).

What a move closed is charged to the period's budget, not what it was
expected to close.

**Why it cannot cycle.** A move goes from A to the node B of highest
weight in A's set, which includes A, so the weight of B is above A's for
that actor. Every move raises the weight of the actor's node; a return
would need the opposite. Views that differ cannot change that: weights
depend on the actor and the node only. The minute after a take bounds a
node that flaps in and out of the set to one move per actor a minute.

**Pacing.** Per app on each node: at most two rebalance moves in flight
(operators' moves have sixteen), the next starting when one ends; at most 500
sockets closed per heartbeat period per node (one full edge), a move
that goes over putting the next period in deficit. On N nodes: at most
2(N - 1) moves in flight and 500(N - 1) sockets closed per period in the
cluster.

**A socket no longer decides an edge's node.** When a WebSocket upgrade
finds that nobody holds a spread actor and its home is another node, the
node asks the home to take it (the take call of ADR 0017), then relays
the socket to the owner that leaves, as it relays one to any remote
owner. A take refused, failed or not answered within its 10 s wakes the
actor where the socket landed, as before: **a wake-up is never refused**.
A relayed upgrade is never asked again. One take runs per actor at a
time: the sockets reconnecting to one cold edge at once wait for its
answer. A home without room (ADR 0041, section 4), or backed off after a
failed take, is not asked; a take that fails suspects and backs it off. ADR 0041's reasons against a take
at wake-up do not hold here: an edge exists before any socket reaches it
(the room's `init` creates it before the directory names it), and a
socket for an edge held elsewhere is relayed already. Without it, during
a drain a reconnecting client that wins the race against the push would
put an edge on the balancer's node, and a client reconnecting through a
third node in the gap between a move's release and its take would wake
the edge there.

**Drains and deaths.** A draining node runs no pass, and any node
draining pauses every node's. A node that leaves for good leaves nothing
to move: its edges woke at their homes over the smaller set. A dead
node's edges are taken over where a request lands (a record naming a
dead node is "try here", not "nobody"), and those not at home are moved
once the cluster is quiet: their clients reconnect a second time. A node
stuck not serving pauses the pass for as long as it shows; a node stopped,
or dead, pauses it for ten minutes after its last write (amendment of
2026-11-28).

**Upgrades.** Nothing is gated, because nothing new is written: the take
call and its body are frozen since 0.9 (ADR 0043, section 2), no header
is added, no bucket key. The load line gains `misplaced=<n>` (older
readers skip unknown load keys). The pass waits for one minor version
among the live nodes, not for `finalize`: after the last node of an
upgrade serves, it runs before `finalize`, and a node of the older
version that comes back pauses it again. While two minor versions run,
nothing moves, so the cluster is as uneven between steps as 0.9 left it.
A node of the new version asks a home of the older one to take a cold
edge: the call is the frozen one, so it works both ways.

**Switch.** On by default, off with placement (`--no-placement`), and off
alone with `serve --no-rebalance` or `rebalance = false` under
`[process]` in a node file. An operator who places edges by hand turns it
off; to keep edges off a node, drain it.

**What an operator sees.** `misplaced=<n>` on the heartbeat's load line,
counted whether or not the pass may run, and left out by a node with
rebalancing off; `cluster status` shows it per
node (`AWAY`) and one word per app: `balanced` when every live node
publishes 0, `rebalancing`, or `paused` with the node that drains, is not
serving or stopped within the ten minutes, or `mixed versions`. It is read from heartbeats, so it lags
a node that just started serving by up to two periods, and a drain
intent by one. Metrics, by tenant and
app: `wasmkeep_rebalance_misplaced`, `wasmkeep_rebalance_moves_total{outcome}`,
`wasmkeep_rebalance_sockets_closed_total` and
`wasmkeep_rebalance_paused{reason}` (`off`, `draining`, `not_serving`,
`versions`, `lease`).

**Tests.** `rebalance::decide` and `home` (unit tests, each rule at its
edge); `sim/tests/madsim_rebalance.rs` runs the real rule over modelled
nodes that drain, die, restart (sometimes as the next minor version) and
flap out of reach, with views that lag and sockets that reconnect at
random, and checks convergence once quiet, monotone moves with the
minute's stay, pacing, no move while a node drains or two minor versions
run, spread classes only, and that a cold socket's edge is taken by its
home or by the waking node, never a third, each with a mutation that
fails it; `tests/cluster/spread.rs`, plain and over TLS: a node with
clients on every edge drains, is stopped and starts again, nothing moves
during the drain, every edge is back home after and each client was
closed at most twice; a socket for an edge nobody holds wakes it at its
home (where it lands with `--no-rebalance`); an edge woken away from home
moves there, and stays with `--no-rebalance` or `--no-placement`; an
operator's move off home is undone after the stay; an edge with 30
clients that reconnect after a random wait below 2 s moves home without
being held up by reopened sockets (and is not moved home without the
closing mark); a parked edge with a socket is moved as a parked room; a
drain, and an operator's move, of two edges with 30 clients each that
reconnect after 1 ms or after a random wait below 2 s end within 15 s,
each client closed at most twice (without the mark the drain stays
`draining` and the move never lands at 1 ms, and clients are closed three
times at the random wait). `madsim_node_lease` has the rebalance move and
the cold socket's take among its events, and one holder at a time holds
through them. Not tested here: a peer a minor version apart in a cluster
test (the unit tests and the mixed-version check cover it), and what a
move costs an edge at its socket cap.

#### Amendment 2026-11-28: no move while a node may come back

A rolling upgrade of three nodes under a big room, run as the upgrading
page says (drain, stop, install, withdraw the drain, start, one node at a
time), showed two faults once the last node served again: delivery was
slow for minutes, though nothing was lost and the edges did end home.

**1. Moves while a node was stopped.** Rule 2 read only the live peers. A
stopped node's lease is `released`, so it is not live, and once the page's
step had withdrawn its drain request nothing paused the pass: the other
nodes moved edges to their homes over the two of them, and moved most
again when the stopped node served. Now a node of the app that is listed
under `nodes/` but not live (`released`, `fenced`, or a heartbeat past
the horizon) pauses every pass (reason `not_serving`) until its last
write of the node lease is **ten minutes** old (`rebalance::AWAY`). The
bound:

- It is longer than a restart's stop, install and start check, so a
  node that is coming back is waited for. A node starting again writes a
  `serving` heartbeat and is a live peer from then: the settle delay
  applies to it as before.
- It ends, so a node that is gone for good does not stall the pass: its
  edges woke at their homes over the nodes left, and those taken over
  elsewhere move once the ten minutes have passed. `cluster forget`, and
  the removal of released leases (ADR 0020), delete the key, which then
  no longer counts at all.
- It is judged by the date of that write on the writer's clock (within
  the allowed skew), not by when this node saw it stop, so a node that
  restarts meanwhile reaches the same answer.

`cluster status` shows the app `paused (<id> stopped)` meanwhile.

**2. Edges raced once the node served.** Two causes, both fixed:

- *A take answered `503` blamed the home.* A node answers a take `503`
  when it cannot take the actor at that moment and asks for a retry: it
  is draining, it cannot open the secrets, the acquisition or the
  restore cannot finish now, or the call was signed before it started.
  The mover and the cold socket's sender counted that as a failed target,
  suspected it and backed it off, so the first take refused just after
  the home served made both peers leave it out for a heartbeat horizon
  or more. A take answered `503` is now signed and sent again every
  250 ms for 2 s (every try within the take's 10 s from the first, none
  sent with less than a second left), and one answered `503` throughout,
  or whose try after a `503` fails another way, is `unavailable`: the
  target is backed off as before, so the next edges do not wait on it,
  but it is not suspected, so requests still go to it; the move backs the
  actor off too (counted `busy`), and a cold socket wakes the actor where
  it landed. A moment's refusal passes within the 2 s; a node that refuses
  for longer (draining before the sender sees it, its secrets blocked)
  costs each caller 2 s once, then its backoff. The decision reads the
  status alone, so a receiver of any version is retried the same way. The receiver logs why it refused, at `info`. The reason of
  the refusal in that run was not recorded (it was logged at `debug`);
  a cluster test with a slow bucket did not reproduce it, so this
  amendment does not name it.
- *A stub woke the edge on a third node.* An upgrade for an actor whose
  mailbox is on the node it lands on was routed "try here" without a
  read: a mailbox means the actor is resident. But the first socket's
  admission check leaves a mailbox while it asks the home to take the
  actor, and a request waiting leaves one too. Every later socket for the
  edge on that node then acquired it there, in the gap between a move's
  release and its take on the home, and that node moved it again: the
  logs' "another node took the actor first". An upgrade for a spread
  actor whose mailbox here holds no lease, is not parked and is not being
  moved from this node now reads the record, as with no mailbox: nobody
  holds it, so its home takes it first (the take of the move and the cold
  socket's both reach the home, which holds it after the first). A move
  from this node keeps the mailbox's answer, the move's target, so a
  socket follows an operator's move off home. A read that fails still
  means "try here". Only WebSocket upgrades read it: a plain request that
  lands on such a mailbox is unchanged.

A wake-up is still never refused: a socket waits for its home's answer
(at most the take's 10 s, its retries after a `503` at most 2 s), then
wakes the actor where it landed.

**Tests.** `rebalance::decide` (a node stopped within the bound pauses
every node; at the bound it no longer counts);
`sim/tests/madsim_rebalance.rs` gives the pass the nodes that are down,
with how long, adds to property 4 that no move starts while one went down
less than the bound ago (mutation: the pass blind to them), and now and
then stops a node for good when the faults end, so convergence over the
nodes left, within a bound that includes this one, shows the pause ends;
`cluster/wire_tests.rs`: a take answered `503` twice is signed and sent
again until it is held, one answered `503` throughout is `unavailable`
within its retries, one answered `503` then `500` is `unavailable`, and a
`500` alone is `failed` and not sent again; an operator's move of an edge
to a node that is not its home, with clients through the moving node,
ends on its target (a guard: it passes without the move check too, as
the gap is short on an in-memory bucket); `tests/cluster/spread.rs`:
the upgrading page's step with the drain withdrawn while the node is
down moves nothing until it serves, then moves the edge once, straight
home; a node stopped for good pauses the pass for the bound only (shortened);
and a rejoin under clients that reconnect at once through both other
nodes, the rejoined node on a slow bucket, ends with every edge home, no move
or cold take failed, none ended `taken_elsewhere`, and each
client closed at most twice (without the read of the record, moves ended
`taken_elsewhere` and edges went back and forth between the other two
nodes). `cluster status` shows the pause.

## Consequences

- A room of tens of thousands of clients is four small classes and a
  page of front code; the protocol is the same in every app.
- The room stays one sequencer: publish rate is one actor's, whatever
  the number of clients.
- Node memory grows with the sockets a node holds, its own and those it
  relays for another node: each costs its connection's buffers and up to
  4 MiB of queued frames (256 frames), and there is no node-wide cap on
  queued bytes, so a reconnect storm of slow clients can raise a node's
  peak far above its steady use.
- A front on QuickJS turned a full 256 KiB history page from the room's
  binary answer into JSON and bytes in the guest, without native UTF-8:
  the tightest per-event cost left on a room's path. Since 2026-11-20 the
  room renders the page's JSON (`history-json`, section 3) and the front
  forwards it.
- `call-many` has no deadline of its own: a round waits for its slowest
  child up to the event's call deadline, and the room's other events wait
  behind it. Suspects bound that to one slow round per failing child.
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

1. **Resolved: placement limit.** Until 0.8.0, a room's tree of edges ran
   on the node that woke it until that node passed placement's floor.
   Since the amendment of 2026-11-22
   ([section 14](#14-edges-spread-over-the-nodes-amended-2026-11-22)), its
   edges wake spread over the live nodes by default, with no class key;
   the room remains the sequencer.
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
from `@wasmkeep/sdk/front`, after authorizing and calling R2 history
(since 2026-11-20 it returns `room.historyJSON(after, limit)`, the same
bytes rendered by the room, section 3).
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


### R5 implementation notes

Rust exports `RelayActor`; TypeScript exports `broadcast.defineRelay()`.
Deploy that class internally as `relay` (TypeScript also accepts
`scaling.relayClass`). The durable scale operation includes the next relay
assignments. At the 17th edge it initializes relays and refreshes existing
R4 edge configurations before activating the layer. Each edge accepts pushes
from the room and that room's relay name prefix; only the room can change
its range or end it. At 12 edges the operation ends the relays and returns
to direct pushes. Empty relays are ended even above that threshold.

A relay request shares one bounded history slice among up to 16 edge
headers. Children with later positions receive their portion; remaining
history follows in subsequent rounds. A relay failure backs off the relay
without marking every edge failed; a returned edge failure backs off only
that edge. Neither makes an upward call. The room retries the original
assignments after backoff, as section 9 specifies.

The 256-edge ceiling returns `room edge ceiling: 256` and durably increments
`ScaleState.refused`, without allocating a child or starting an operation.
The default split/merge hysteresis remains 60,000 ms. Tests can override it
with Rust `Scaling::with_hysteresis(ms)` or TypeScript
`scaling.hysteresisMs`. The real-node fixture uses one second, 34 paced
clients and capacity two, proves contiguous delivery with HTTP gap recovery
across splits, then removes clients until relays retire at 12 edges.


### R6 implementation notes

[`examples/ts-big-room`](../../examples/ts-big-room) embeds a plain browser page
and the SDK client in its front, with SDK room, edge, relay and directory
classes. Both JS engines run an end-to-end example test in the language suite.
The page exposes connection state, delivery counters and a manual socket drop.
Names are public labels, not accounts; the stats route reports the head and
persisted topology, not the full runtime statistics proposed in section 10.
`onRepeat` on the browser receiver supplies the discarded-item counter,
including overlaps with HTTP catch-up. The example also exposed and tests the
QuickJS `null` representation of an unset WIT alarm: poke accepts both engines'
representations.

`crates/wasmkeep-server/tests/room_cluster.rs` runs 30 and 300 paced clients on
three real in-process nodes sharing an in-memory object store. Each node owns
its own Tokio runtime; killing one stops its tasks and sockets without a drain
or lease release. Edge names are prewarmed on three nodes (placement remains
outside this ADR), then the SDK creates their ranges through normal splits.
The capacity is 15/150 and the fixture's hysteresis is one second. The test
asserts sockets on all three nodes, 4801 moves, reconnects after node death,
contiguous numbered delivery, a stalled client closed with 4802, paged HTTP
catch-up with no reset or loss, and resumed live delivery for every client.
HTTP recovery is concurrency-bounded and publishes use stable retry ids.
The 300-client proof is enabled by default; it is paced rather than a benchmark.
R7/R8 still own benchmark publication and real-bucket scale claims.


### R7 implementation notes

`bench/room-group.mjs` drives the TypeScript SDK client against a deployed
example-compatible front, with paced HTTP publishers, configurable clients
and duration, reconnects, latency histograms, recovery/repeat/void counters,
close reasons and sampled topology/optional node metrics. Results require an
explicit output file; no measurements are published by this slice.
`scripts/check-room-bench.py` checks 24 clients against a temporary local node,
including forced reconnects and one dropped frame recovered over HTTP, within
30 seconds. See the [harness contract](../benchmarks.md#reconnecting-room-harness)
and [room design guide](../designing-for-hot-actors.md#use-the-reconnecting-room-for-one-ordered-stream).
R8 still owns real-bucket load, grow/shrink, slow-client and node-loss evidence.
