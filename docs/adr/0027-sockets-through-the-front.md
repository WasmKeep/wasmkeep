# 0027. Sockets through the front

- Status: accepted (2026-11-14), built in 0.4.1; revised after an
  adversarial review (a socket event never creates the actor; a signed
  request with the mark is never a call; a hand-off not sent says nothing
  of the peer)
- Date: 2026-11-14
- Release: [0.4.1](../../ROADMAP.md#m4-operating-a-cluster)
- Follows: [ADR 0023](0023-the-front.md) ("The WebSocket hand-off: not
  now": the reserved range and the plain GET),
  [ADR 0024](0024-socket-home.md) (where a room with sockets wakes) and
  [ADR 0003](0003-websockets-wit-v0.2.md) (sockets owned by the host).
- Amends: [ADR 0010](0010-who-may-create-an-actor.md) (a socket can come
  from an inside caller), [ADR 0013](0013-access-rules-and-config-in-deploys.md)
  (a class made `internal` closes only its outside sockets).

## Context

Since 0.4.0 the front receives every request outside `/actor/` and calls
the app's actors as an inside caller, so sign-in is written once and
classes can be `internal`. WebSockets do not follow. A socket is opened
only by an upgrade at `/actor/<class>/<name>/...` (`actor/upgrade.rs`),
admitted as outside (`admit(true)` there and in `ws_prepare`,
`actor/sockets.rs`). A room class must therefore be `existing` or `open`,
its address public, and every room checks the client's token itself. An
upgrade at one of the front's own paths reaches the front as a plain GET,
which cannot answer it.

0.4.0 reserved what a hand-off needs (ADR 0023): the `x-wasmkeep-` range
is stripped from requests to the front and from its answers, so the host
can use it both ways without a client or the front forging it.

What must not change: actors (`actor`, `actor-core`), built fronts, and
the limits per socket. One socket carrying several actors is 0.5.0.

## Decision

### The host marks an upgrade to the front

When a request outside `/actor/` is a WebSocket upgrade (a `GET` with
`Upgrade: websocket`, `Sec-WebSocket-Version: 13` and a
`Sec-WebSocket-Key`, the test `/actor/` upgrades use), the host:

- keeps the connection's upgrade aside before the front sees the
  request, with the client's headers;
- strips the `x-wasmkeep-` range as for every request to the front, then
  sets **`x-wasmkeep-upgrade: websocket`**.

The front still receives a `GET` (wasmtime's `wasi:http` drops
`Upgrade` and `Connection`, as before). A client cannot send the mark:
the range is stripped first. A request that is not an upgrade never
carries it. SDK helper: `is_websocket(&request)` / `isWebSocket(req)`.

### The front answers with three headers

To hand the socket to an actor, the front answers with:

| Header | Value |
|---|---|
| `x-wasmkeep-handoff-class` | the class, as written (`room`) |
| `x-wasmkeep-handoff-name` | the name, percent-encoded as in `x-wasmkeep-caller` (every byte but ASCII letters, digits and `-._~`) |
| `x-wasmkeep-handoff-path` | the path and query the actor's `on-websocket-open` sees, starting with `/` |

Three fields, each encoded on its own: the host never splits one string
into parts, and a name can hold any character an address allows. Each
header must appear exactly once; the status and body of that answer are
not read. SDK helpers build it: Rust `front::hand_off(class, name, path)`
and TypeScript `handOff(class, name, path)` return an ordinary response
(`426`, `websocket upgrade required`, plus the three headers).

A response header was chosen over a host function: it works in every
language that can write a front, with no change to the `front` world, and
the front's answer is the decision, made once, when the front has checked
what it needed (a token, a room name).

The host then:

- **on an upgrade it marked**, with all three headers valid: stops the
  front's instance and frees its slot (the answer's body is not read),
  counts `wasmkeep_front_requests_total{outcome="handoff"}`, and opens the
  socket on that actor itself (below);
- **on an upgrade it marked**, with the headers missing in part, repeated,
  or not decodable (a name that is not UTF-8 after decoding, a path that
  does not start with `/` or is not a valid path and query): answers
  `500`, counted `guest_error`, as a front's broken answer;
- **on any other request**: strips the range from the answer as since
  0.4.0, so the client gets the `426` the helper built. A hand-off is
  only ever honoured on a request the host marked.

### The host opens the socket as an inside caller

The actor is in the front's own app: the class must be one of the
version's classes, as for `actors.call`. The open follows the path of an
`/actor/` upgrade (`ActorService::upgrade`), with one difference: the
opener is **the front, an inside caller** (ADR 0010).

- An `internal` class is not refused (`early_rule` is not checked), and
  admission is `admit(false)`: required values are still checked, and the
  actor is created if it does not exist, as a front's call creates it. The
  front validates names before handing off, as before calling.
- **The mark is the socket's.** The host records, in the socket's own
  entry in its registry (`websocket.rs`), that the front opened it. Every
  event of that socket (`on-websocket-open`, each message, its close)
  admits as inside; every event of a socket opened at `/actor/` admits as
  outside, as before. The registry entry is the host's, not the
  instance's, so the mark survives the actor hibernating, waking, and
  being parked (ADR 0024): `ws_prepare` reads it instead of passing
  `admit(true)`.
- **A socket event never creates the actor**, the front's sockets
  included: their messages and closes admit as inside (an `internal` class
  is not refused, required values are checked) and, under any class but
  `open`, the actor must still exist. Only the open itself, the front's
  decision, may create it.
- A class a deploy makes `internal` closes its outside sockets at their
  next event (1008, ADR 0013); sockets the front opened stay. A deleted
  actor of an `internal` or `existing` class closes every socket, as
  today (`delete-self`), and since socket events never create the actor,
neither their close events nor a later message create it again.
  - Note (2026-11-14): in a cluster the existence check alone did not
    hold this: a deleted actor keeps its lease, and the bucket its prefix
    (ADR 0010, Consequences), so the close events of the sockets the
    delete closed ran on a new, empty instance. The host now marks those
    sockets in its registry when it closes them for the delete (closing
    ones included), and their close events are not delivered, whoever
    opened them (`tests/cluster/front_sockets.rs`,
    `a_deleted_rooms_sockets_do_not_bring_it_back`).

### What `on-websocket-open` sees

`on-websocket-open(conn, path)` is unchanged: `path` is the one the front
named in `x-wasmkeep-handoff-path`, query included. The event has no
request, so there is no `x-wasmkeep-caller` to read, and the WIT does not
gain a caller argument (that would change `actor-core` for every built
actor). An actor knows a socket came through the front **because its
class is `internal`**: then every socket it has was opened by the front,
and the path, with what the front put in it (the user it signed in, say,
in the query), is the front's word. A class that is also reachable
from outside (`open`, `existing`) cannot tell the two apart, and must not
trust its path.

### Errors reach the client as HTTP

The client gets an HTTP error and never a socket that opens and then
closes:

| Cause | Answer |
|---|---|
| hand-off headers broken | `500` |
| the class is not one of the app's (or the node serves no actors) | `404 no such actor class` |
| the name is not a valid name | `400` with the reason |
| `on-websocket-open` returned an error | `403` with the actor's error as the body |
| a required value not set, the actor unavailable | `503` with `Retry-After` |
| too many sockets on the actor or the node | `503` with `Retry-After: 5` |
| the owner node unreachable, or of an older version | `503` with `Retry-After: 1` |

For the refusal, the host waits for `on-websocket-open`'s answer before
it sends 101 (it already does: the socket is opened before the 101) and,
on a refusal, removes the socket from its registry and answers `403`
instead of 101 followed by close 1008. The actor's own reason is the
body, as it was the close reason: no host detail reaches the client.
Upgrades at `/actor/` keep their behaviour (101, then 1008).

### Limits

- **The front's slot** is held only until its answer's head: the
  instance is stopped once the host reads a hand-off, before the socket
  opens. The open waits like an `/actor/` upgrade (the admission wait, a
  dead node's takeover up to `--failover-wait-secs`, the relay's 30 s,
  `on-websocket-open`'s wall), on the client's connection only, which
  `--max-connections` bounds as for any upgrade. A slow open therefore
  never holds a front slot, and a slow front holds one only up to its
  wall, as any front request.
- **Per socket and per actor**: unchanged (1,000 sockets per actor,
  50,000 per node, 1 MiB per message, 4 MiB queued).

### Between nodes

When the room is owned by another node (resident there, or parked there
with its sockets, ADR 0024), the node that ran the front relays the
upgrade to the owner, as it relays an `/actor/` upgrade, but **signed**:

- `GET /actor/<class>/<percent-encoded name><path>`, with the client's
  WebSocket headers (`upgrade_headers` strips the range first), and
  `x-wasmkeep-caller: <tenant>/_front/main`, `x-wasmkeep-upgrade:
  websocket`, the forwarded mark and `x-wasmkeep-app`;
- signed as an `actors.call` (`x-wasmkeep-internal`, v4): method, path,
  the SHA-256 of the empty body, caller, app, and **`x-wasmkeep-upgrade`,
  added to the signed names**. A call without it signs as before, so no
  existing signature changes. Each relay is signed afresh (the second send
  after a refused handshake included) and spent once by the owner (the
  calls' record of used signatures).

The owner believes an upgrade is the front's only when the signature is
valid, its body hash is the empty body's, it is spent for the first time,
the app is its own, and the signed caller is exactly
`<tenant>/_front/main` with the mark `websocket`. A valid signature
without the caller or the mark is refused (`403`); an invalid one, or none,
is an outside upgrade as today, so a forged mark or caller from a client
changes nothing (and `internal` still answers `403`). The mark is in the
signed names so that a signed call seen on the wire cannot be replayed as
an upgrade: an `actors.call` never carries it, and a node refuses (`403`)
any signed request that carries the mark without being an upgrade (a
relay whose `Upgrade` header was removed on the way). On the public port in
`--cluster-plaintext refuse` mode, a signed relay is outside, as every
signed call there; relays go to the cluster port then.

The owner runs the open with the same mark as a local hand-off, and the
socket lives there. The relaying node only copies bytes, as for any relay.
**Socket home** is unchanged: the room's sockets are on its owner, and a
room that hibernates with them is parked there and woken there by every
node's front.

A relay is never sent on again (the forwarded mark), and a draining owner
answers it as any relayed upgrade (the client reconnects).

### Compatibility

- **Actors** are unchanged: same worlds, same events. A socket's events
  run as inside events when the front opened it, which only changes what
  an `internal` or `existing` class admits.
- **Built fronts** are unchanged: they never set the range (it was
  stripped), so none hands off by accident. They see one more request
  header, `x-wasmkeep-upgrade`, on upgrades.
- **Older nodes.** A 0.4.0 node treats a signed upgrade as an outside
  one (it reads no signature on upgrades), so it would refuse a hand-off
  to an `internal` room with `403`. The heartbeat gains the capability
  word **`handoff`** on line 11, beside `take` and `front`; a node relays
  a hand-off only to an owner that announces it, and otherwise answers
  `503` with `Retry-After: 1`, without counting that peer as failing
  (it was never sent anything). Unknown words are ignored, so 0.4.0 nodes
  read the new heartbeat.
- **Behaviour changes**, for the 0.4.1 notes:
  - the front sees `x-wasmkeep-upgrade: websocket` on WebSocket upgrades;
  - a front's answer to an upgrade with the three hand-off headers opens a
    socket instead of reaching the client;
  - outbound HTTP answers (actors' and fronts') lose the `x-wasmkeep-`
    range, headers and trailers (added 2026-11-14).

## Consequences

- A room class can be `internal`: its address is not public, the token is
  checked once, in the front, and the front puts in the path what the room
  needs to know. The chat example becomes a front plus an `internal` room.
- A front that copies another server's response headers onto its answer
  to an upgrade lets that server name an actor of the app. Since 0.4.0,
  `actors.call` answers lose the range, so an actor cannot; outbound HTTP
  answers keep their headers, and the SDK docs say to copy only names the
  front knows.
  - Note (2026-11-14): outbound HTTP answers lose the range too,
    headers and trailers, in the host's sender (`egress.rs`) for actors
    and fronts alike, so no upstream can hand a socket off through a front
    that passes its answer on (`tests/front.rs`,
    `an_upstreams_hand_off_headers_do_not_hand_off`). The SDK docs say so
    instead of warning against copying.
- An actor of a class reachable both ways cannot tell a handed-off socket
  from an outside one; the docs say to make it `internal`.
- A hand-off costs one front instance (until its answer's head) per
  socket opened, and, when the room is elsewhere, one signed relay.

## Alternatives

- **A host function `hand-off(class, name, path)` in the `front` world.**
  A new import in every front language, and a call that would have to say
  what happens to the front's own answer. The response headers are one
  decision, in the front's answer.
- **One header (`x-wasmkeep-handoff: room/lobby/ws`).** A string to split,
  where a name may hold `/`. Three fields are never ambiguous.
- **The mark in the actor's state.** Lost when it hibernates; and one mark
  per actor would admit outside sockets of the same room as inside.
- **101 then close 1008 for a refusal, as at `/actor/`.** A client sees a
  socket open and close. The hand-off waits for the answer anyway.
- **An unsigned relay with the caller header.** Any client could forge it;
  the signed hop is what `actors.call` already trusts.
- **A caller argument for `on-websocket-open`.** A change of `actor-core`
  for every built actor, for what `internal` already says.

## Not decided here

- One socket for several actors, each a channel (0.5.0, its own ADR).
- A caller per socket readable by the actor.
- Stripping the range from a front's outbound HTTP answers: since
  decided (2026-11-14), stripped for every guest (see Consequences).
