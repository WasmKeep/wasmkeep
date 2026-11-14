# 0024. Socket home

- Status: accepted (2026-11-14), built in 0.4.0; revised after an
  adversarial review (the resolver's generation, parking in one step,
  drains on every pass, the safeguard re-read)
- Date: 2026-11-14
- Release: [0.4.0](../../ROADMAP.md#m4-operating-a-cluster)
- Amends: [ADR 0020](0020-one-lease-per-node.md) (section 3, "Release":
  hibernation with open sockets keeps the ownership record) and
  [ADR 0023](0023-the-front.md) ("Socket home: a gate for clustered use":
  the gate is lifted).
- Follows: [ADR 0003](0003-websockets-wit-v0.2.md) (sockets owned by the
  host), [ADR 0014](0014-draining-a-node-and-moving-an-actor.md) and
  [ADR 0017](0017-moving-one-actor.md) (drains and moves close sockets
  with 1012).

## Context

A WebSocket belongs to the node that accepted it, not to the actor's
instance (ADR 0003): it survives the actor hibernating. The actor's
ownership record does not. Hibernation releases it (ADR 0020, section 3),
so the next event for the room is acquired wherever it lands. When it
lands on another node, that node takes the room, and the first node closes
the room's sockets with 1012: at their next message (`ws_prepare`), or at
the owner sweep, once a minute (`actor/socket_owners.rs`). Every client
reconnects, and a room keeping a list of who is in it loses its close
events.

On one node nothing changes. On a cluster reached at
`/actor/<class>/<name>` through a balancer that hashes the path, events for
a room mostly land on one node, so this is rare. With a front
(ADR 0023), any node's front calls the room with `actors.call`, so a room
that hibernated is woken wherever the caller's request landed, and its
clients are disconnected on most wakes. ADR 0023 therefore did not
recommend the front for socket classes on a cluster until this record.

The rule wanted: **an actor with open sockets wakes on the node that holds
them; other nodes relay to it.**

ADR 0020 rejected keeping ownership across hibernation (its first draft)
for three reasons:

1. a woken hold would share a generation with the previous one, which
   defeats the ADR 0009 owner fence and garbage collection's "a segment
   this hold wrote" (ADR 0012);
2. drains, moves and clean stops would have to hand over actors that are
   not in memory;
3. it saved only a write per hibernation and wake, and renewals were the
   cost that mattered.

The third reason does not apply here: the point is where the actor wakes,
not a saved write. The first two must still be answered.

## Decision

### 1. A hibernation with open sockets keeps the record

When an actor hibernates (its mailbox drops it after the idle timeout, or
evicts it to make room) while it has at least one socket on this node that
is open and not closing, and this node's hold on it is valid, the node
**parks** the hold instead of releasing it: it stops acting as owner at
once, exactly as before a release (the hold leaves the set of live holds,
so `LeaseView::is_valid` says no), and keeps the hold's key, generation
and version in a set of **parked records**. Nothing is written.
The hold leaves the live set and enters the parked set in one step, under
the lock of the live set, and only if it may still act in its epoch
(otherwise it is released as before): at no moment is the key in neither
set, so a lapse ending every hold (which empties both sets under the same
lock) cannot miss it, and the resolver (section 3) cannot see the key
unheld in between. The record
still names this node and its instance, at the hold's generation.

A hold is parked only from that idle drop, and only when the node is not
draining, not moving the actor, not shut out of the app's secrets
(ADR 0016) and not shutting down. Every other end of a hold releases it as
before: a restore that failed, a lapsed hold, an abandoned upload, a
hand-over, a move, a clean stop. A parked record is never authority to
act: it is a release deferred, nothing else.

### 2. A wake is an acquisition

The parked record names this node's own instance, so routing on any other
node finds it (`Owners::other_owner`: one GET of the record, the named
process live in the membership view) and forwards the request, relays the
WebSocket upgrade, or sends the `actors.call` to this node, as for an
actor resident there.

Here, the event finds no hold and acquires, as every event does
(`Owners::acquire`). The record names this node's current instance, which
ADR 0020 already treats as acquirable ("a hold of this process that
ended"): it writes the record at **generation + 1**, `If-Match` on the
version it read. The woken hold has a generation of its own, so the first
reason ADR 0020 gave does not arise: the owner fence, alarm-index tags and
garbage collection see a new hold, as after any wake. The wake runs under
the lock per actor (ADR 0020, section 3) and drops the parked entry once
the record is known to have moved: when it succeeds (the new hold
replaces it) and when another holder is found. A wake that wrote nothing
(the bucket did not answer the read, the lease is not valid) keeps it, and
so does one whose outcome is unknown: its write may not have landed, and
the record then still names the parked hold, which only the parked entry
can release. If the write did land, the resolver releases it (section 3),
and the parked entry's own release then finds a moved version and ends
(ADR 0020's pending release rule).

A wake never re-adopts the parked hold to save that write: the one write
per wake is what keeps every hold's generation its own, and what ends any
late write of the parked hold (the conditional write moves the record's
version).

Safety is ADR 0020's, unchanged: parking writes nothing and grants
nothing, and acting still needs a live hold plus a valid node lease. Two
nodes running the actor at once would need one of them to acquire a record
that names another live process, which `Owners::acquire` refuses exactly
as for a resident actor.

### 3. When a parked record is released

A parked record is released (`If-Match` on the parked hold's version,
generation kept, the ADR 0020 release; a release that fails or has no
answer joins the pending releases, retried in the background) when:

- **the last socket goes**: the actor has no socket left on this node that
  is open (closing ones do not count) and no live hold here. Checked when
  a socket's slot is freed, and again by the owner sweep once a minute,
  whatever the reason the sockets went (a client's close, a close the
  server started, a close event that could not run);
- **the node drains** (ADR 0014): every parked record is released at the
  start of each drain pass, before the resident actors are handed over,
  and an event for a parked room reaching the draining node releases its
  record before it is sent to the peer; the sockets of non-resident rooms
  close with 1012 as today. A drain reports `drained` only with no parked
  record and no pending release, so no record is left naming a node that
  no longer serves the actor;
- **the actor is moved** (ADR 0017): the owner side acts on a parked actor
  too (before, it acted only on resident ones, and the target refuses an
  actor whose record names a live node, so a parked actor would never
  move): it records the move (every event for the room, its sockets'
  close events included, then goes to the target), releases the record,
  closes the sockets with 1012 and sends the target its take call. Only
  a release that landed lets it go on. One left pending, or one that
  finds the record naming this node at a later generation (a wake whose
  write landed unanswered, which its resolver releases; read again to
  tell it from another holder, and an unread record counts as this
  node's), keeps the sockets and the request, since the target would
  find the record naming a live node. The parked entry is gone by then,
  so the room is no longer this node's to move: on each move pass, for
  up to two minutes, the owner reads its record while the room has
  sockets here and is neither resident, parked nor held here, and closes
  them with 1012 once another live node holds it. The target deletes the
  request as it takes the room, so this look does not depend on the
  request still being listed; past the two minutes, the owner sweep
  closes them;
- **the node lease runs out** (no write acknowledged for ttl minus safety):
  every socket closes with 1012 at that moment (as today), and every
  parked record joins the pending releases, as every live hold does;
- **a clean stop**: every socket closes with 1012 (as today) and every
  parked record is released before the node lease is written `released`;
  that write frees any that did not land;
- **the app's secrets cannot be opened** on this node: it drains
  (ADR 0016), as above. A node draining with no peer to hand to (`stuck`)
  keeps serving its actors, parked rooms included, as ADR 0014 says.

A lost node lease (a fence, a twin) drops parked records with the holds,
without releasing anything: the fence, or the other instance, frees them,
as for live holds.

The release of a parked record takes the lock per actor, as a wake does:
the two never interleave. Either the wake goes first (the release then
finds no parked entry) or the release does (the wake then finds a record
naming nobody and acquires it as any free record). Safety never rests on
that lock: every write is conditional on a version, and a release on the
parked version can no longer apply once any acquisition moved it (ADR
0020's pending release rule then ends it).

The resolver of an unresolved acquire counts a parked record as held
here only at the **same generation**, as it counts a live hold: an
unresolved acquire at the parked generation (a lost acquire followed by a
second that landed at the same generation, then parked) is never released
by it. An unresolved acquire at a later generation (a wake whose reply
was lost) is resolved as before: released if it landed, while the parked
entry stays for the case where it did not. Without the generation, the
resolver would skip a wake that landed, the parked release would then
find a moved version and end, and the record would name this node with
nothing here to release it: harmless while the node serves, a drain or a
move that never ends once it does not.

A drain releases the whole parked set on every pass, not once (a park
that checked "not draining" just before the drain began may land after
the first pass), and the set of pending releases counts parked records:
a drain is `drained`, and a clean stop's wait for releases ends, only with
no parked record left.

Releasing loses the room's close events when no instance runs them. A
drain closes a parked room's sockets without running its close events, as
it does today for every hibernated room (the draining node takes no
actor), and so does a move of a parked room (its events go to the target,
which has no socket to close). A node shut out of the secrets runs no
event anyway.

### 4. When the socket node dies

Nothing new: the parked record names a process whose node lease stops
being renewed. A peer fences it after the usual watch, and the record is
free ttl plus grace after the dead node's last acknowledged write, plus
whatever part of a lease period and an attempt timeout the fencer had not
yet watched (ADR 0020, sections 2 and 5); the next event acquires it wherever it lands, and
the sockets are gone with their node. A request for it meanwhile waits as
for any actor of a dead node (`--failover-wait-secs`, docs/cluster.md
"When a node dies").

This is the cost of the rule: today a hibernated room's record names
nobody, so after its node dies the room is free at once; a parked one is
free after the same 15 to 20 s as a resident actor. It applies only to
rooms that hibernated with sockets open, whose clients are disconnected
by the node's death anyway; they reconnect, and wait
(`--failover-wait-secs`) or get a 503 until the room is free, so clients
should back off on reconnect, as docs/cluster.md already asks.

A partition is the other case: a node that still reaches the bucket but
not its peers keeps its lease, so its parked rooms stay its own, and
events for them from the other side fail (a forward or relay error, then
a 503) until it is reachable again or stopped. This is today's behaviour
for every resident actor; parking extends it to rooms that hibernated
with sockets open. A node its peers cannot reach should be stopped or
fenced (`wasmkeep cluster fence`), which frees all of its records.

A fenced node keeps no record: the fence makes its next lease write fail,
and the process exits (ADR 0020, section 1) with every hold and parked
record dropped; peers free them by the fence.

### 5. A node alive with idle sockets

A room whose clients stay connected for days without a message stays
parked for days, at no cost: holds are not renewed (ADR 0020), so a parked
record costs no request while it lasts. Its sockets are pinged every 25 s
by the host, locally. The owner sweep reads no record for a parked room
on most passes (it is this node's own), so a room parked for days also
stops costing the one GET a minute the sweep makes today for each
hibernated room with sockets. As a safeguard against a parked entry that
no longer matches the record (a fault this record did not foresee), one
pass in ten re-reads parked rooms' records, at most 5,000 a pass (the
sweep's own bound), going on where the last re-read stopped: every parked
record is read within ten passes per 5,000 parked (100 passes at the
50,000 bound), and a re-read cut short by a slow bucket starts the next
at the first record it did not read. One that no longer names
this instance at the parked generation is dropped from the set, and its
sockets are then treated as today (closed if another live node holds the
room). A record that cannot be decoded proves nothing: it is kept,
counted (`wasmkeep_websocket_sweep_unreadable_total`), logged once and
passed over, in the re-read and in the owner sweep alike, so one bad
record never holds back the rooms after it; only a bucket that does not
answer cuts a pass short.

The room stays on its node while it has sockets: placement at wake-up
(ROADMAP) does not move it. Moving it is a move or a drain, both of which
close its sockets as they do today.

### 6. Limits

None new. A parked record exists only for an actor with an open socket on
this node, so parked records are bounded by the sockets a node accepts
(`MAX_SOCKETS_PER_NODE`, 50,000) and hold a few hundred bytes each. A
parked actor uses no resident slot (`--max-resident`): it is not in
memory.

### 7. Other versions

No format changes: the record, the acquire rule, the routing read and the
release are ADR 0020's. A node that does not park (any node before this
record) releases on hibernation as before, so its own sockets see the 1012
behaviour of today, and nothing else changes for the cluster: it routes to
a parked record's node like to any owner, and never takes a record naming
a live process. A parking node beside it needs nothing from it. So no
capability word is needed, and a rolling upgrade (the 1.0 goal) works with
both kinds of node at once. Today every 0.4 node is upgraded together
anyway (ADR 0022: a 0.3 node cannot run on a 0.4 bucket).

### 8. Metrics

- `wasmkeep_parked_records` (gauge): records this node keeps for actors
  that hibernated with sockets open.
- `wasmkeep_parked_wakes_total` (counter): wakes of a parked actor on its
  node (each one acquisition at a new generation).
- `wasmkeep_parked_releases_total{reason}` (counter): parked records
  released, by `reason`: `last_socket`, `drain`, `move`, `lapse` or
  `stop`.

`wasmkeep_websocket_owner_closes_total{reason="sweep"}` should stay near
zero on a cluster where every node parks; a rise means rooms are being
woken elsewhere.

### 9. ADR 0023's gate

Socket classes may be reached through the front on a cluster: a call from
any node's front wakes the room on the node that holds its sockets.
`/actor/<class>/<name>` with a hashing balancer remains a good choice (it
spares a relay for most socket upgrades), no longer a requirement.

## Consequences

- A room with sockets open wakes on its sockets' node, wherever its events
  arrive; its clients stay connected across hibernations.
- A hibernated room with sockets is free about 15 s after its node dies,
  not at once (section 4).
- A parked room stays on its node until its sockets close, a move, or a
  drain.
- Each hibernation with sockets saves a release write; each wake still
  costs the record's read and write, as today.
- Drains, moves, clean stops and lapses each release parked records, so
  ADR 0020's "no record left naming a live node that no longer serves it"
  holds. ADR 0020's second reason is answered by releasing, not by
  handing over: a drain or a move closes a parked room's sockets without
  its close events, as a drain does today for every hibernated room.
- Across a partition between nodes, a parked room is as unreachable from
  the far side as a resident actor (section 4).

## Verification

1. **Simulation** (`sim/tests/madsim_socket_home.rs`), on the shared node
   (`wasmkeep_sim::node`) with the parked set from `wasmkeep_cluster::holds`:
   three nodes, one room, seeded faults (cut links, slow writes that land
   late, crashes and restarts, clean stops, drains), events landing on
   random nodes and relayed to the running node a record names, sockets
   opened and closed on the room's node, and every socket closed when its
   node's lease lapses. Checked: never two owners acting, never one
   generation for two holds, never a room taken by another node while a
   node keeps it parked with a valid lease, never a room taken while a
   live node with a valid lease, not draining, holds its sockets (unless
   that node's own wake had an unknown outcome or found another holder,
   counted apart as exposed), and never a record naming a node that
   neither holds, parks, nor has anything pending for it while its lease
   is valid (a leak). Broken variants that must be caught, and are: a wake
   that re-adopts the parked hold without writing (one generation, two
   holds), a lapse that forgets parked records (a leak), releasing on
   hibernation (sockets lost), a resolver that counts a parked hold at any
   generation (a leak), and a wake with an unknown outcome that drops the
   parked hold (a leak). The scenario case
   (`a_room_wakes_where_its_sockets_are`): a room with a socket on n1,
   called through n2 and n3, wakes on n1 each time; n1 is killed and
   another node takes the room within the failover bound; the new node
   parks it with a socket, and the last socket closing leaves a record
   naming nobody.
2. **`tests/cluster/`** (`websockets.rs`): a room with a real WebSocket on
   node a hibernates and is called through node b twice, served on a each
   time, at a new generation each time, its socket open and answered; the
   record is released once the socket closes; when the last socket goes
   and its close event cannot run (its wake's record write is refused),
   the freed slot releases the record, pending until the bucket accepts
   it; a drain of a releases the parked record and closes the socket with
   1012; a move of a parked room waits while its release is pending, or
   while the record names that node at a later generation, its socket
   open, and closes the socket with 1012 once the target takes the room
   (without the owner sweep). The
   tests of sockets closed by a room taken elsewhere now change the record
   behind the node's back first.
3. An adversarial review of this record, then of the code.

Recorded runs: `madsim_socket_home` at 20,000 seeds in release mode
(189,717 acquisitions, 71,892 parks, 28,962 wakes at home, 82,577 relays,
30,511 releases at the last socket, 2,525 drains of a parked room, 12
exposed takeovers), every broken variant caught (at seeds 0, 9, 0, 296
and 434). The review of the code found two delays (a drain released
parked records only after its hand-overs; any parked record slowed the
pending-release retries to every 4 s), a park that could land after the
last socket's nudge, a move of a parked room that could repeat, and two
test gaps (the last-socket release was not exercised; the simulation
could not catch the resolver's generation rule). All are fixed, the gaps
by the test and the two resolver variants above.

The simulation found one gap of ADR 0020 that this record does not
change: an acquisition whose write has no answer, and lands only after the
resolver read the record (unchanged then) and ended the entry, leaves a
record naming its node with nothing there to release it (487 times in
the 20,000 seeds). The node takes it back at the actor's next event, as its
own; a drain or a move of that actor waits until the node stops. The
simulation counted it apart; fixing it (the resolver keeping the entry
while the record is unchanged, for a bounded time) belongs to ADR 0020.
Fixed there on 2026-11-14 (ADR 0020, section 3, amended): the
simulation now counts such a record as a leak, and a broken variant
(`ResolverEndsUnchanged`, the resolver ending the entry on its first read
of an unchanged record) is caught, first at seed 34.

## Alternatives

- **Close the sockets when the room wakes elsewhere** (today): simple, and
  failover of a hibernated room is immediate, but every wake through
  another node disconnects every client, which makes the front unusable
  for rooms on a cluster.
- **A separate hint, "wake where the sockets are"**: the socket node writes
  a small object (or a heartbeat entry) naming the rooms it has sockets
  for, and routing reads it before acquiring. It keeps release on
  hibernation, but it is advisory: two requests can still race to acquire
  on two nodes, the hint must be written and cleaned up on every socket
  change, and a stale hint sends events to the wrong node. The ownership
  record already says where to send an event; keeping it is the hint, with
  ADR 0020's guarantees.
- **Keeping the hold across hibernation**, as ADR 0020's first draft did,
  re-adopted on wake without a write: rejected for ADR 0020's reasons (one
  generation for two holds). This record keeps only the record, and every
  wake writes a new generation.
- **Relaying socket frames to wherever the actor runs**: the room could
  wake anywhere and the socket node would forward each message. It doubles
  every message's path, needs a protocol between nodes for socket events,
  and the room's broadcasts would have to come back the same way.

## Implementation

- `wasmkeep_cluster::holds::Parked`: the parked set (park, drop on a wake,
  take for a release, end all on a lapse or a lost lease), shared by the
  server and the simulations; the resolver's "held here" counts it at its
  own generation, and an unresolved wake that the resolver released makes
  the node re-read and forget the parked entry
  (`Cluster::forget_parked_behind`). `Owners::still_names_this` is the
  re-read.
- `crates/wasmkeep-server/src/cluster/lease_hold.rs`: parking a hold,
  releasing a parked record under the actor's lock, the lapse and stop
  paths.
- `crates/wasmkeep-server/src/actor/state.rs`: an `ActorState` dropped by
  its mailbox parks its hold when the rules of section 1 allow.
- `actor/socket_owners.rs`: the sweep skips parked rooms and releases
  parked records with no open socket; a freed socket slot nudges the same
  check. `actor/drain.rs`, `actor/moving.rs` and `ActorService::shutdown`:
  section 3.
