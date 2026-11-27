# 0041. Placement at wake-up

- Status: accepted (2026-11-17), as built: revised the same day from the
  simulation (sections 3 to 6 and 8, and the properties), then wired
  (P1, P4) and documented (P5). On by default; the release's benchmark
  pass with a hashing balancer confirms the default, the floor and the
  band (open questions below).
- Amended 2026-11-22 (0.8.0, item 1): a room and its pusher kept apart
  at wake-up, by default
  ([amendment](#amendment-2026-11-22-keeping-a-room-and-its-pusher-apart)).
  The same path spreads a big room's edges over the live nodes
  ([ADR 0040, section 14](0040-directory-broadcast-groups-and-the-reconnecting-room.md#14-edges-spread-over-the-nodes-amended-2026-11-22)).
- Amended 2026-11-25 (0.9.0, item 4): the per-class keys `apart-from`
  and `spread` ask for both rules for classes with the app's own names
  ([amendment](#amendment-2026-11-25-apart-from-and-spread-for-an-apps-own-class-names)).
- Date: 2026-11-17
- Milestone: [Next](../../ROADMAP.md#next), `0.6.0`, item 6
- Follows: [ADR 0014](0014-draining-a-node-and-moving-an-actor.md) (a
  draining node sends an event to a chosen peer, which claims),
  [ADR 0017](0017-moving-one-actor.md) (the take call),
  [ADR 0020](0020-one-lease-per-node.md) (ownership records under a node
  lease), [ADR 0024](0024-socket-home.md) (parked holds),
  [ADR 0035](0035-a-recorded-head-on-hand-over.md) (the recorded head),
  [ADR 0036](0036-per-node-ingress-rate-limits.md) and
  [ADR 0037](0037-socket-session-admission.md) (admission), and
  [ADR 0038](0038-several-apps-per-process.md) (the sender and the proof
  on every hop, budgets shared between apps, the load line per app).
- Amends, once accepted: nothing in ADR 0020 or ADR 0035. The heartbeat's
  load line gains two fields and stops being display only.

## Context

An actor that nobody holds starts on the node that receives its next
event. A load balancer that hashes the actor's path spreads requests
well, but three kinds of wake-up ignore it: a call from another actor
wakes the callee on the caller's node, a call from the front wakes it
where the front request landed, and a balancer that does not hash sends
the first request anywhere. One node can then hold most of the residents
while another has room, and a node that joins or comes back gets only
what the balancer sends it.

Nothing reads node load today. Each node publishes it on payload line 10
of its node lease (`load actors=... sockets=...`), for `wasmkeep cluster
status` only.

This record decides the first automatic use of it, and the smallest one:
**where an actor that nobody holds starts**. The load rule never moves a
running actor; since the amendment of 2026-11-26, a spread actor is moved
to its home (ADR 0040, section 14), and the load rule does not apply to
it.

## Decision

A node that is about to wake an actor nobody holds keeps it, unless the
node is clearly fuller than another node of the app. Then it sends the
event there, once, with a signed mark that carries its own fill, and
that node acquires the actor as it would for any request, unless its own
count says it is not the lighter one. The rest of this section says
exactly when, where and how.

### 1. What is placed, and what never is

The decision is taken in routing (`actor/routing.rs`), where a cold
address is routed before it takes a local actor slot. Today that step
answers "another node holds it" or "try here" (`remote_owner`). It gains
a third answer, "nobody holds it", from the same single read of the
ownership record: the record is absent, or it names nobody (a released
record). Only that answer can lead to a placement. No read is added.

An event is **placeable** when all of these hold:

- it is a request from outside whose body is absent or declared
  (`content-length`) at 64 KiB or less, or an `actors.call` from an actor
  or from the front (one entry of a `call-many` counts as one call);
- it did not arrive from another node: it has no forwarded mark and did
  not arrive on the cluster port;
- the node has no mailbox for the actor, no parked hold for it
  (ADR 0024), and no move request names it (ADR 0017);
- the node is not draining (the drain's own rule decides then, ADR 0014),
  its node lease is valid, and it can open the app's secrets;
- the record read answered "nobody holds it". A read that failed or
  timed out means "try here", as today.

Never placed:

- **A WebSocket upgrade**, from a client or handed over by the front. A
  socket relayed to another node holds a session on both nodes for its
  whole life (ADR 0037), so placing it would add socket load to save
  resident load. With a hashing balancer, every socket of a room would be
  relayed.
- **An alarm.** The sweep already gives each live node its own share of
  the overdue alarms (`plan_sweep`), and the node that delivers one must clear its index entry
  (ADR 0009). That path stays as it is.
- **A request with a larger or chunked body.** It is streamed, so it
  could not run here after a refusal (section 6).
- **A take call, an alarm hand-off, a drain's hand-over**, and every
  operator pass (deletes, restores, expiry).
- **A record that names a node**, whatever its state: live, lapsed,
  fenced, or this node's own ended or parked hold. Takeovers after a
  failure and wakes of parked holds are unchanged.

### 2. The load measure

Each node id publishes two more fields on its load line, for its own app:

```
load actors=<n> sockets=<n> events_per_min=<n|-> front_per_min=<n|-> actor_room=<n> socket_room=<n>
```

- `actor_room`: how many more residents this app could have on this node
  now. With `cap` the app's `max-resident` (the process limit if unset),
  `floor` its `min-resident`, and the process's residents counted over
  all apps:
  `min(cap - actors, max(floor - actors, limit - process_residents))`,
  each difference stopping at 0. The `max` is ADR 0038's floor rule: an
  app below its floor can always evict another app's idle resident, so a
  full process is not full for it.
- `socket_room`: `min(app ceiling - app sockets, node ceiling - process
  sockets)`, stopping at 0, with ADR 0037's effective ceilings.

A node's **resident fill**, in thousandths, is
`actors * 1000 / (actors + actor_room)`, and its **socket fill** is
`sockets * 1000 / (sockets + socket_room)`, in integer arithmetic,
rounded down; a zero denominator is 1000 (no room at all).

The rule compares resident fills, because what a placement moves is one
resident. The socket fill only rules a candidate out (section 4): a node
short of socket sessions is a poor home for an actor that may open
sockets, but a sender full of sockets gains nothing by sending a
resident away.

So load counts **per app, bounded by the process**. Per app alone, a
process filled by another app would look empty. Per process alone, one
busy app would push a quiet app's actors away from a node where that
quiet app is within its floor.

Rates (`events_per_min`, `front_per_min`) are not used: they lag by a
minute. Readers of older versions skip the new fields, as the line's
parser already skips unknown keys.

A node reads **its own** fill from memory at the moment of the decision,
never from its heartbeat.

### 3. Freshness

A node's view (`cluster/view.rs`) keeps, for each peer, its last load,
the moment the view first saw that heartbeat on this node's **monotonic**
clock, and a credit (section 5). The heartbeat's own time is not used:
clocks differ.

A peer's load is **fresh** for three heartbeat periods after it was first
seen (9 s with the default 3 s period). A stale load makes the peer no
candidate. If the bucket stops answering, the view is kept but nothing
new is seen, so every load goes stale within three periods and every
node keeps what it wakes. **Stale means keep it.**

Fresh says how old the observation is, not that its numbers are current.
A heartbeat first seen a millisecond ago can predate placements this
node sent, placements other nodes sent, and wake-ups that reached the
peer directly. The credit covers the first of these only, and only since
the heartbeat was first seen. So a fresh load does two things and no
more: it makes the peer a candidate, and it sizes the budget. Whether
the peer takes the actor is decided by the peer, on its own count read
from memory when the hop arrives (section 5, "On arrival"). **A fresh
number can cost one refused hop. It cannot place an actor.**

### 4. Candidates

The candidate set for an actor is the peers that:

- serve the same app, are live, report `serving` on heartbeat line 9, are
  not asked to drain and are not suspected (ADR 0014's eligible peers);
- can be reached the way node calls reach them;
- announce the capability word `place` (section 9);
- have a fresh load with both room fields, and a socket fill below 800;
- are not in this node's placement backoff (section 6);
- have had no placed event from them accepted here in the last three
  heartbeat periods (section 5, "No return").

Every exclusion applies before the two are taken. The node itself is
never in the set. The two candidates are the two
highest rendezvous weights for the actor's encoded address over that set
(`rendezvous`, the hash ADR 0014 uses). With one peer there is one
candidate; with none, the node keeps the actor.

Choice: rendezvous, not a random pair. Two nodes that wake the same actor
at the same moment pick the same target, so they do not race each other
for the record, and a test can say in advance where an actor goes.

### 5. The rule

It has three parts: what the sender decides, what the receiver checks on
arrival, and no return.

**The sender.** With `s` this node's resident fill, and for a candidate
`p` its published `actors` and `actor_room`, its credit `k` (placements
this node sent it since that heartbeat was first seen, whatever the
answer) and `N` the number of nodes of the app that announce `place`,
this node included:

1. **Floor.** If `s < 200`, keep it. A node under a fifth of its capacity
   never sends an actor away, so an idle cluster forwards nothing.
2. **The lighter candidate.** For each candidate, the estimated fill is
   `(actors + k) * 1000 / (actors + actor_room)`. Take the lower one; on
   a tie, the higher rendezvous weight.
3. **The 25% rule.** Keep it unless `s * 4 > c * 5`, with `c` that
   estimated fill: this node must be more than 25% above the candidate.
4. **The budget.** Let `t = s * 4 * (actors + actor_room) / 5000`,
   rounded down: the highest resident count at which `p` stands at 80%
   of this node's fill or less. This node may send `p` at most
   `(t - actors) / (N - 1)` events per heartbeat of `p` it sees, rounded
   down, `t - actors` stopping at 0. If `k` has reached it, keep it. A
   share of 0 keeps it: **there is no minimum of one.**

Otherwise the event is placed on `p` and `k` grows by one. A new
heartbeat from `p` sets `k` back to 0.

Rule 4 is the stricter one at the integer edges: `k` below the share
means `actors + k < t`, which means rule 3 passes. A sender at 501 with
a peer at 400 of 1000 passes rule 3 (2004 > 2000), but `t` is 400 and
the share is 0, so it keeps the actor, for the reason `budget`: one more
resident would put the peer at 401, above 80% of 501. Rule 3 stays as
the reason `band`.

**On arrival.** The hop carries `s` in the placed mark (section 8). The
receiver reads its own `actors` and `actor_room` from memory and
computes the same `t = s * 4 * (actors + actor_room) / 5000`. It takes
the event only if `actors + 1 <= t`. Otherwise it refuses (section 6)
and the event runs where it woke. So an actor is placed only when the
receiver, counting that actor, stands at 80% of the sender's fill or
less **on the receiver's own current count**, whatever any heartbeat
said.

**No return.** Each node keeps, per peer and on its own monotonic clock,
when it last accepted a placed event from that peer and when it last
sent that peer one (any answer). For three heartbeat periods (9 s) after
accepting one from `X`, `X` is no candidate (section 4). For three
heartbeat periods after sending one to `X`, a placed event from `X` is
refused. At three periods exactly, both are allowed again. The second
half covers two hops that cross in flight.

What each part is for:

- The arrival check is what makes a stale or outrun number safe. A
  sender cannot know what reached the receiver since its heartbeat:
  other senders that hold another heartbeat, or wake-ups that arrived
  directly. The receiver can.
- The budget keeps the hops few. Up to `N - 1` nodes read the same
  heartbeat of a light node; each sends at most its share of its own
  gap, so together they send at most the gap of the fullest of them,
  instead of each sending the whole gap and all but the first being
  refused.
- No return makes flapping impossible for a pair, not just unlikely:
  fills can cross within seconds when residents hibernate on one node
  and wake on the other.

Two consequences of rounding down. A receiver whose gap is under
`N - 1` actors gets nothing from a sender: placement stops up to
`N - 2` actors short of `t`. And a capacity so small that 80% of the
sender's fill is less than one resident never receives a placement:
with two nodes, a sender at 1 of 5 (fill 200) and a receiver at 0 of 1,
`t` is 0 and the actor is kept.

The numbers (floor 200, the 5/4 ratio, three periods for freshness and
for no return) are constants in 0.6.0, not settings.

### 6. The hop, and what happens when it is not taken

A placed event travels as the drain's hand-over does: one forward to the
chosen node, which acquires. It is **not** the take call (section 7).

Before sending, a placed request's body is read into memory (64 KiB at
most); a call's already is. The hop carries the forwarded mark, the app,
the sender and its proof (ADR 0038, section 5), and the placed mark
(section 8). The sender then acts on the answer:

| What comes back | What the sender does |
| --- | --- |
| Any answer without the refusal mark | Returns it. If it carries `x-wasmkeep-node` naming the chosen node (the actor ran there), remembers that node as the actor's owner (the owner hint), so the next requests skip the record read. An access answer (`403`, `404`) sets no hint. |
| `503` with `x-wasmkeep-placed: refused` | The event did not run there. Runs it here, with placement off for this event. Backs the node off. |
| The hop was never read (no address, the target not trusted, a refused handshake, a request given back unsent: `never_read_by_peer`) | The same: runs it here, backs the node off. |
| Anything else (the connection failed after the write, no answer in time) | Answers as a failed forward does today: `503` with `Retry-After` for a request, `failed` for a call. The event may have run, so it is not run again. Backs the node off. |

"Runs it here" is the ordinary path: this node reads the record again
and acquires, or forwards to an owner that appeared meanwhile.

**Backoff** is placement's own table: the node is no candidate for one
heartbeat horizon (10 s by default). It is not ADR 0014's suspect list,
which also decides drains and who collects expiries; a full node is not a
suspect. A refusal for the arrival check backs the node off like any
other, so a sender whose number is behind sends that node at most one
refused hop per backoff.

**The receiver** treats the hop as a forwarded request, so it never
forwards it and never places it. It answers `503` with
`x-wasmkeep-placed: refused` whenever it answers without handing the
event to the guest for one of these reasons: it is draining, cannot open
the secrets, is moving the actor away, has no resident capacity, the hop's
proof or sender did not check (ADR 0038 refuses such a hop; the mark
tells the sender it did not run), it found another holder, its own
count fails the arrival check (section 5), it sent the sender a placed
event in the last three heartbeat periods (no return), or the mark's
value is not a whole number from 200 to 1000. An access answer (`403`,
`404`) is the client's answer and carries no mark: it would be the same
here. The host sets the response mark; it is removed from a guest's
response.

**Never a loop.** An event is placed at most once. The receiver cannot
send it on. After a refusal the sender uses the ordinary path, which
forwards at most once to a real owner.

### 7. Why not the take call

The move's take call (`x-wasmkeep-take`, ADR 0017) could ask the chosen
node to acquire first, and the request would then follow as an ordinary
forward. It would make every refusal clean. It is not used, because:

- a take answers "no such actor" for an actor that does not exist yet,
  and whether a request may create one is the request's own admission
  (ADR 0010), judged by the node that will hold it;
- it costs a second round trip on every placement, and wakes an actor
  for a request that may never arrive;
- the forward that acquires on arrival is already the drain's path.

The buffered body and the refusal mark give the same clean refusals for
everything placeable.

### 8. The placed mark

`x-wasmkeep-placed: <s>` on the hop, `s` being the sender's resident
fill in thousandths at the decision (200 to 1000). It is added to the
headers the hop's signature covers (`signature`'s list, as `x-wasmkeep-take` was), in the
forward's proof (`x-wasmkeep-admitted`) and in a call's signature
(`x-wasmkeep-internal`). The signature already binds the destination
node, the method, the actor's path, the app, a timestamp and, through
`x-wasmkeep-sender`, the sending node id; a call's also binds the body
and is accepted once. A hop with the signed mark is sent only to nodes
announcing `place`, since an older node would compute another signature.

A node that cannot sign the hop keeps the actor: the cluster secret is
not loaded, or the request has no peer address to prove.

What the mark grants: **nothing**. Any node acquires an actor nobody
holds when a client's request for it arrives. The mark only takes
rights away: the receiver's right to place, and, through its value, the
right to run here when the receiver is not lighter. The arrival check,
no return and the count of placements received apply only to a mark
under a signature that checked. So:

- a replay of a placed forward does what the same client request sent
  to that node's public port does, and needs no replay store of its
  own. The existing ones still apply: a call's signature and a charged
  rate-limit proof are accepted once;
- a mark a client forged reaches a node only where a client's forwarded
  mark does today: the public port of a single-app process that still
  accepts plaintext hops. There the request is handled as that
  forwarded request is today; the mark adds only "do not place", which
  is today's behaviour, and it is not counted as a placement received.
  On the public port of a process with several apps the mark is
  stripped with every other `x-wasmkeep-*` header;
- a hop whose proof is required and does not check is refused as
  ADR 0038 refuses it, with the refusal mark (section 6).

### 9. The `place` capability word, and the switch

A node announces `place` on heartbeat line 11 when it publishes the room
fields, reads the mark and has placement on. A node without the word is
never a candidate and is not counted in `N`. A node places only when it
announces the word itself.

During a mixed-version upgrade, older nodes keep everything they wake
and receive nothing placed; newer nodes place among themselves. Nothing
refuses to start.

Placement is on by default. `serve --no-placement`, or `placement =
false` under `[process]` in the node file, turns it off for a process:
its node ids neither place nor announce the word. It applies to every
app of the process.

### 10. Leases, drains, moves, failover

- **Leases.** Unchanged. The receiver acquires through the ordinary
  path: the record's conditional write under its node lease (ADR 0020)
  and the recorded-head gate (ADR 0035). Placement never writes a
  record and never asks a holder to release. If two nodes place or
  acquire the same actor at once, one write wins and the other node
  finds a holder.
- **Drains.** A draining node does not place; its drain sends events to
  its chosen peer. A node asked to drain, or not `serving`, is no
  candidate. A node that starts draining after it was chosen refuses,
  and the sender runs the event.
- **Moves.** An actor named by a move request is not placed, on any
  node. A moved actor is not pinned: once it hibernates, its next wake
  is placed like any other.
- **Failover.** A record naming a dead node is taken over by the node
  the request reaches, after the usual wait. Requests kept during that
  wait are not placed.
- **A load balancer with a consistent hash.** Placement sends an actor
  away from the node the balancer picked, so every request for it then
  costs one forward while it is resident. The owner hint removes the
  record read, not the hop. That price is why the floor and the 25%
  band exist, and why upgrades are not placed. With an even hash, nodes
  stay within the band and nothing is placed.
- **Rate limits and budgets.** A request is charged once, at the node it
  reached (ADR 0036); a refused placement does not charge it again. A
  buffered body counts in no new budget: it is bounded by 64 KiB for the
  time of one hop.

### 11. Metrics

All with `tenant` and `app`:

- `wasmkeep_placements_total{kind,outcome}`: decisions that sent an
  event. `kind`: `request`, `call`. `outcome`: `placed`, `refused`,
  `unsent`, `failed` (the four rows of section 6).
- `wasmkeep_placements_received_total{outcome}`: `acquired`, `fuller`
  (the arrival check), `returned` (no return), `refused` (any other
  reason of section 6).
- `wasmkeep_placement_kept_total{reason}`: placeable wake-ups kept here:
  `floor`, `band`, `budget`, `no_candidate`, `unsigned`.
- `wasmkeep_placement_fill{kind}`: this node id's fill, in thousandths
  (`kind`: `residents`, `sockets`).

The hop also counts in `wasmkeep_forwards_total`. `wasmkeep cluster
status` shows each node's fill and room.

## How a joining node fills

A new node publishes a resident fill of 0. Each other node above the
floor sends it its share of wake-ups, among the actors for which the new
node is one of the two candidates, until the new node's next heartbeat
shows where it stands. That repeats each period until the new node is
within `N - 2` actors of `t` for the lightest sender. Actors already
resident elsewhere stay; they reach the new node only after they
hibernate and wake.

So it fills at the rate at which actors wake cold, and no faster. A
cluster whose actors never hibernate does not fill a new node at all.

Placement narrows a gap to the band. It does not equalise.

The simulation (`sim/tests/madsim_placement.rs`) runs the real decision
function and the real arrival check over simulated nodes, heartbeats
that are late, lost or read at different times, wake-ups that reach a
node directly, and actors that wake and hibernate. It has a node that
joins three loaded ones, and one that returns after its actors were
taken over. A placement is **accepted** when the receiver took the
event; the hop was **sent** whatever the answer. For every seed:

1. **One hop.** No event is placed twice, and no placed event is
   forwarded again.
2. **One holder.** The node-lease simulation's ownership invariant
   holds with placement on.
3. **Nothing resident moves.** An actor changes node only across a
   hibernation, a refusal before it ran, or a node's failure. Restated
   by the amendment of 2026-11-26 for the classes that do not spread: a
   spread actor is also moved to its home, which
   `sim/tests/madsim_rebalance.rs` checks.
4. **Direction.** Every hop sent leaves a node whose resident fill was
   at least 200 and goes to a node whose last fill seen by the sender
   was below 80% of it.
5. **No overshoot, no herd.**
   (a) When a placement is accepted, the receiver's residents, counting
   that actor, are at most `s * 4 * capacity / 5000`, rounded down, with
   `s` the fill the hop carried and `capacity` the receiver's own
   `actors + actor_room` on arrival. No allowance is added: not `N - 2`,
   not the wake-ups that reached it directly.
   (b) Under one heartbeat of a node showing `a` residents, the nodes
   that hold it send that node at most `t_max - a` events together,
   stopping at 0, whatever it answers. `t_max` is rule 4's `t` for the
   highest fill any of them had while holding it, and they count the
   same `N`. Three senders at 600, capacity 100, a heartbeat showing 0:
   16 each, 48.
6. **No flapping.** For any arrivals, on each node's own clock: a node
   that accepted a placement from a peer sends that peer no placed
   event for the next 9000 ms, and a node that sent a peer a placed
   event accepts none from it for the next 9000 ms. At 9000 ms exactly
   both are allowed. So two nodes never have placements accepted in
   both directions within three heartbeat periods.
7. **It fills at the rate of the wake-ups, then it is quiet.** A
   wake-up is **offered** to the joining node `j` when it is placeable,
   wakes on another node at fill 200 or more that holds a fresh
   heartbeat of `j`, and `j` is its lighter candidate. The **target** is
   `t - (N - 2)` residents, `t` being rule 4's for the lightest other
   node and `j`'s capacity.
   (a) *Rate.* In the directed scenario (the other nodes' fills fixed,
   every heartbeat of `j` seen by every other node within one period,
   none of `j`'s residents hibernating), with each other node offered at
   least `w >= 1` wake-ups per heartbeat of `j`: until `j` reaches the
   target, every offered wake-up is accepted by `j` or kept for the
   reason `budget`, none is refused, and `j` reaches the target within
   `ceil(t / ((N - 1) * w)) + 2` heartbeat periods of its first
   heartbeat. With capacity 1000 and three others at 600 (`N` = 4, `t` =
   480, target 478): at one offered wake-up per second, one per node per
   period, 162 periods (486 s), and after 42 s `j` holds exactly the 42
   actors offered; at 160 or more per node per period, 3 periods (9 s).
   `j` joins empty, so `t` is also its gap.
   (b) *Quiet.* For every seed, no placement is accepted while the
   sender's fill at sending and the receiver's fill on arrival are
   within the band (`higher * 4 <= lower * 5`), whatever any heartbeat
   says. With actual fills 500 and 400 of capacity 100 and a fresh
   heartbeat that still shows 0, the hop is sent and refused (41 > 40).
   Such a hop is sent at most once per sender and receiver per backoff
   (10 s).
8. **An idle cluster is silent.** With every node under the floor, no
   event is placed.

Each rule has a mutation that must fail its property:

- **The budget removed** fails 5(b): the three senders above send 180,
  not 48.
- **The minimum of one restored** fails 5(b): with two nodes, a sender
  at 1 of 5 and a heartbeat showing 0 of 1, one hop is sent and
  `t_max - a` is 0.
- **The arrival check removed** fails 5(a) and 7(b). For 5(a): three
  nodes, capacity 100, two senders at 600, so `t` = 48 and a share of
  24. Sender 1 sends 24 under the first heartbeat. The next shows 24
  and only sender 1 sees it: it sends 12 more. Sender 2 still holds the
  first, fresh, and sends 24: 60 residents, above 48. For 7(b): the 500
  and 400 case above.
- **No return removed** fails 6: two nodes, capacity 100, A at 60 and B
  at 0. A places 48 on B. Within 9 s, 40 of A's residents hibernate and
  A's heartbeat shows 20. B, at fill 480, computes `t` = 38 for A and
  places on it; A, at 21 counting the actor, accepts.
- **The floor removed** fails 8, and **the heartbeat's own clock under
  skew** fails 4.

What the simulation disproved in the first version of this record, and
what changed:

- *A minimum budget of one* placed an actor on a receiver that it put
  above the sender (sender 1 of 5, receiver 0 of 1), and the receiver
  placed its next wake-up back 1 ms later. The minimum is gone, the
  receiver checks its own count, and no return forbids the way back.
- *An overshoot bound stated on heartbeats* (80% of the fullest sender
  plus `N - 2`) does not hold when senders hold different heartbeats or
  wake-ups arrive directly. The bound is now enforced where the count
  is known, and has no allowance.
- *A fill deadline in seconds* (two idle timeouts plus ten periods)
  cannot hold: at one cold wake-up per second, 42 actors can arrive in
  42 s where 480 were required. The property is now a rate.
- *Quiet judged on heartbeats* fails when a fresh heartbeat still shows
  an empty node that has filled since. Quiet is now judged on actual
  fills, at the receiver.

## Consequences

- A node that joins or returns fills from the others' wake-ups without
  an operator.
- A placed actor costs a forward per request until it hibernates.
- A node fills only as fast as actors wake cold, and stops up to
  `N - 2` actors short of the band's edge.
- A placement sent on a number that is behind costs one hop and a
  refusal; the event then runs where it woke.
- A request with a body above 64 KiB, an upgrade or an alarm always
  wakes its actor where it lands.
- One more thing can go wrong at wake-up: a chosen node that dies
  between its heartbeat and the hop. A request then gets one `503`
  unless the hop was never read, and the node is backed off.
- The load line is now read by nodes. A node that published wrong room
  figures would attract or repel wake-ups, never break ownership.

## Alternatives

- **The take call first.** Section 7.
- **A random pair, or the least loaded node of all.** A random pair
  splits two concurrent wakes of one actor; the least loaded node makes
  every sender pick the same target, the herd the budget exists to
  bound.
- **No mark, the forwarded mark alone.** It would work for routing. The
  mark lets the receiver refuse without running, lets it count what it
  received, and keeps "placed" out of what a plain forward means.
- **A minimum resident count before the floor applies.** It would stop
  the smallest sender, not the cause: a receiver whose capacity is too
  small for the sender's 80%. Rounding the budget down and the arrival
  check cover every capacity.
- **A longer memory for the credit** (kept across heartbeats, or a
  heartbeat sequence returned in the hop's answer). It would correct a
  sender's view of its own placements, never of other senders' or of
  direct wake-ups. The receiver's count needs none of it.
- **Moving running actors.** On the roadmap, only if this is not enough.

## Tests (to build)

- Load: room arithmetic with a cap, a floor and a full process; the line
  written and read; a frozen older reader skipping the fields.
- Decision: the floor, the band at its integer edges, the budget
  rounded down and a share of 0, a stale load, one candidate, none, the
  same pair chosen on every node, a peer excluded for no return before
  the two are taken; the arrival check at `actors + 1 = t` and
  `t + 1`.
- Routing: each "never placed" case of section 1; each row of section
  6's table; the owner hint after a placement; the backoff; a refusal
  for the arrival check and for no return, each run where it woke; a
  call's chain and re-entrancy kept across a placed call.
- Signing: the mark and its value inside both signatures, a stripped,
  added or altered mark refused, a guest unable to set either header,
  a node without `place` never chosen, a process with placement off
  never chosen.
- Cluster: three nodes, one fuller; a fourth joins and fills; two apps
  in one process, one full, the other placed by its own room.
- The simulation above.

## Docs to change when accepted

`failover-and-scaling.md` ("What does not happen today"), `cluster.md`,
`monitoring.md`, `deploy/front-door.md`, `reference/node-file.md`,
`ROADMAP.md`.

## Open questions

- The floor (200) and the band (25%) are first values. The simulation
  and the release's benchmark pass may change them before this record
  is accepted.
- On by default: confirmed only if the benchmark shows no loss with a
  hashing balancer.
- Whether a refused or unsent placement should also be offered to the
  second candidate. Not in 0.6.0: one attempt, then here.

## Plan

Each slice is one change and says what it proves.

- **P1. Load and view.** The room fields, written and read
  (`wasmkeep-cluster`'s `membership.rs`: `Load`, `NodeInfo`; the
  server's `actor/load.rs`); the view keeps each peer's load,
  first-seen time and credit (`Peer` in `cluster.rs`,
  `cluster/view.rs`); `place` parsed, not announced; `cluster status`
  shows fill. Proves the arithmetic and that older readers are
  unaffected.
- **P2. The decision.** Pure functions in `wasmkeep-cluster`'s
  `placement.rs`: the sender's decision (candidates, the rule, the
  budget, freshness, no return) and the receiver's arrival check.
  Proves section 5 by unit tests.
- **P3. The simulation.** `sim/tests/madsim_placement.rs`, before any
  wiring, so the constants can still change. Proves the eight
  properties.
- **P4. The hop.** The three-way route (`Owners::other_owner` in
  `node_lease.rs`, `remote_owner` and `call` in `actor/routing.rs`,
  `dispatch` in `actor/requests.rs`), the mark in both signatures
  (`cluster/signing.rs`, `cluster/forwarding.rs`, `cluster.rs`), the
  buffered body, the mark's value and the arrival check, the two
  no-return times, the refusal mark, the fallback, the hint, the
  backoff, the switch, the word announced, the metrics. Proves the routing,
  signing and cluster tests. One adversarial review follows it: it
  touches the path to ownership.
- **P5. Docs and status.** The pages above; this record becomes
  "accepted, as built".

## As built

- The route's three answers come from one read (`Owners::route`); an
  outside request reads it once in `handle`, and `dispatch` reuses that
  answer, so placement adds no read.
- The receiver counts placed events it accepted and has not registered
  yet into `actors` (taking them from the room) under the same lock as
  the arrival check, so two hops arriving at once cannot both pass it on
  the same count.
- The mark is believed only with a sender bound to the signature and
  checked live in the app (`x-wasmkeep-sender`): no-return needs one. A
  forward's proof is checked on its original path, before routing cuts
  `/actor/<class>/<name>` off it.
- Besides the reasons of section 6, a receiver refuses with the mark a
  placed hop for a class it does not serve yet (a deploy adding it on
  its way: the sender serves it), and marks the `503`s it gives before
  the event reached the guest: a forward proof or a call's sender that
  does not check, a hop for another app, and an event its closed
  mailboxes never ran (a stop without a drain). A sender treats only a
  `503` with the mark as a refusal; any other answer (a `403` for a
  call's body that does not match its signature, say) is returned as it
  is.
- A request asks first, charging nothing, and reads its declared body
  only if it would be placed; it then decides again and charges. A body
  that is slow or never comes costs no credit and no no-return time, and
  one whose numbers moved meanwhile runs here.
- A placed hop that gets no answer in time answers `503` with
  `Retry-After`, as section 6 says, where an ordinary forward answers
  `504`.
- Amended 2026-11-18 (0.6.0 testing pass): the ambiguous row of section
  6 (the hop failed after the write, no answer in time, an answer from a
  node no longer trusted) answers `503` with `x-wasmkeep-outcome:
  unknown` and no `Retry-After`, as every host answer for a request that
  may have run now does ([what "saved" means](../what-saved-means.md)).
  The event is still never run again here.
- The cluster tests' nodes run with placement on, the default. The
  golden trace runs both ways (with it on, b has one resident slot like
  a, so a full node's wake-up of another actor is evaluated and kept, a
  share of 0, and the trace is the same), and a second trace records a
  placed life: one read of the record on the sender, then the receiver's
  ordinary acquisition, the same operations a first event makes there.

## Amendment (2026-11-22): keeping a room and its pusher apart

A large room delivers well only with its pusher on another node
([ADR 0040, section 13](0040-directory-broadcast-groups-and-the-reconnecting-room.md#13-the-pusher-amended-2026-11-20)):
on three 4-vCPU nodes, 4,500 clients at 10 messages/s held delivery p99
under 1 s only once the pusher was moved off the room's node by hand.
The load rule above never separates them. Below the floor it keeps
everything, and the pusher's pull and the front's poke are calls, which
wake the callee on the caller's node. So, besides load, a wake-up now
looks at one more thing it already knows: which actors this node holds.

**The pair.** In a version with classes named `room` and `pusher` (the
broadcast SDKs' default class names), `room/x` and `pusher/x` are kept
apart, for every name `x`; this matches the SDKs' addressing
(`room/lobby` has the pusher `pusher/lobby`). Each node works it out
from the version's classes. Nothing is written at deploy: the manifest,
its format and the version id are unchanged, and an upgraded node
applies the rule without a redeploy. There was no setting in `0.8`. A
key naming other classes (renamed SDK classes, other pairs) needs a
manifest line that older nodes cannot read, and so a format and a deploy
gate of its own: it came in `0.9.0`
([amendment of 2026-11-25](#amendment-2026-11-25-apart-from-and-spread-for-an-apps-own-class-names)).

**The rule.** An actor of the pair is kept apart on a wake-up when all
of these hold:

- the wake-up is this node's own: a request that reached it unforwarded
  (with a body that can be read into memory, as section 1's placeable
  requests), a call from an actor or the front here, or an alarm due
  here that no node handed to it;
- the route read answered "nobody holds it" (section 1). "Another node
  holds it" sends it there as ever. "Try here" (a record naming a node
  not live, or this node with a release pending, a stub mailbox here, or
  a read that failed) is the ordinary path, takeover included;
- this node holds the companion now (read from memory, no bucket read);
- placement is on (`--no-placement` turns this rule off with the load
  rule: an operator's switch during an upgrade, or for an app whose
  classes happen to be named `room` and `pusher`).

Then:

1. If an eligible peer exists (ADR 0014's: live, same app, `serving`,
   not asked to drain, not suspected, reachable), the event goes to the
   rendezvous choice among them, as a drain sends it. A request or call
   is forwarded and an alarm handed off (`x-wasmkeep-alarm-handoff`);
   the peer acquires it, as any event another node sent, and never sends
   it on. A handed alarm is remembered on its receiver until after it is
   due, and the delivery there acquires the actor whatever it finds: one
   hop at most, also when the companion moved to the receiver in flight.
2. Otherwise (one node alive, every peer draining or suspected), it is
   acquired here, as without the rule. **The rule never refuses a
   wake-up.**

A draining node choosing the peer for an actor of the pair (section 10,
and ADR 0014's hand-over and alarm hand-off) reads the companion's
record, one read with a 1 s bound, and chooses among the peers that do
not hold it; if every peer holds it, the plain choice.

A placed hop (section 6) is refused, with the mark, by a receiver that
holds the actor's companion, and with `x-wasmkeep-placed-reason: apart`
beside it (`wasmkeep_placements_received_total{outcome="apart"}`). Its
sender runs the event, or sends it apart by the rule, and does not back
that receiver off: the refusal says nothing of its load. A sender of an
older version backs it off, as for any refusal.

**When the peer does not run it.** A peer's `503` that its actor did not
answer (no `x-wasmkeep-node`) and whose outcome is not unknown means the
event did not run there. Examples: the peer started draining, had no
slot, or found ownership changing. The sender then suspects the peer for
a heartbeat horizon and runs the event here, once, by the ordinary path.
An answer of unknown outcome (the hop failed after the write, no answer
in time) is returned as a failed forward is, and the peer is suspected.
A `5xx` the actor answered, or an actor error (`500`: a trap, a restore
that failed), suspects nobody. The same holds for a drain's forwards: an
actor's error says nothing of its node.

**Never under the rule:** an event another node sent, a handed alarm, a
take (an operator's move wins; the next cold wake-up applies the rule),
a WebSocket upgrade (section 1), a request with a streamed body, an
actor parked here (ADR 0024), and the operator passes.

**What it is not.** It is not a pin, and it moves nothing running. A
pair that is together stays together until one of them hibernates, and
a busy pusher never hibernates. So a pair that lands together stays
together until an operator moves one (ADR 0017). That happens when:

- two wake-ups of the pair reach one node at the same moment: both find
  the other not yet held, and both acquire there;
- the waking node had no eligible peer;
- the pusher's record named a node that died, and the node taking it
  over holds the room (a takeover is "try here", not "nobody");
- a node of an older version wakes one of them.

**Two nodes and a rolling restart.** With two nodes, restarting one
always leaves the other alone: while one drains, every wake-up and
hand-over goes to the other, which holds both, `alone`. When the first
comes back, nothing moves them: the pair stays on one node until one
hibernates or is moved. Three nodes or more keep a peer for each step
of a rolling restart.

A hop under this rule is a drain's forward, not a placed hop: it carries
no placed mark, needs no capability on the receiver, and the properties
of the load rule (direction, the budget, no overshoot, quiet, an idle
cluster silent) do not count it.

**Metrics.** `wasmkeep_placement_apart_total{outcome}`: wake-ups of an
actor whose companion this node held, `sent` to a peer or woken here
`alone`.

**Mixed versions.** Nothing is new in the bucket or on the heartbeat.
A 0.7 node keeps what it wakes. A forward from a 0.8 node is a plain
forward, which it acquires. A placed refusal's reason is a header that
it ignores.

**Tests.**

- The pure rule: `placement::companion`, `apart` and `peer_apart`.
- How a peer's answer is read (not run, failed).
- `tests/cluster/apart.rs`, plain and over TLS, on three nodes with
  default placement:
  - a room and its pusher woken through one node, by a request and by a
    call, in both orders, land on different nodes;
  - a draining pusher's node hands its alarm and its next wake-up to the
    node without the room, for names whose plain drain choice is the
    room's node;
  - an alarm due beside its room on a node that is not draining is
    handed once and fired on a peer;
  - a pusher whose record names a dead node is taken over where the
    request landed, not sent apart, and suspects no peer;
  - a placed hop refused by the room's node runs on its sender and backs
    nothing off;
  - one node where both wake;
  - placement off.
- `tests/room_cluster.rs`: the SDKs' own room and pusher, clients
  joining and publishing through one node, end on different nodes. The
  other room cluster tests lay out their actors themselves and run with
  placement off.
- The simulation `sim/tests/madsim_apart.rs` models the decision
  around the real pure rule, over drains, deaths, hibernations and
  alarms whose companion moves in flight. A handed alarm hops once, and
  a pair shares a node only where the rule cannot help. Each property
  has a mutation that fails it: a receiver that judges a handed alarm
  again, and routing blind to the rule. It does not model simultaneous
  wake-ups or leases.

## Amendment (2026-11-25): `apart-from` and `spread` for an app's own class names

The two rules above apply by class name: `room` and `pusher` are kept
apart, and `edge` is spread beside them. An app that names its classes
otherwise got neither. Two per-class keys in `wasmkeep.toml` ask for
them. Nothing else changes: the wake-up path, what it sends and when,
the fallbacks and the metrics are those of the amendment of 2026-11-22
and of ADR 0040, section 14.

**The keys.**

- `apart-from = "<class>"`: the actor of this class and the actor of
  the same name in the named class are kept apart, both ways. One entry
  is enough for a pair; saying it on both classes is the same pair.
  `apart-from = false`: this class is kept apart from no class.
- `spread = true`: this class's actors wake spread over the live nodes.
  `spread = false`: they do not.

The spread key is named `spread`: the word the rule already has in ADR
0040, in the glossary and in `wasmkeep_placement_spread_total`. It is a
yes or no because the rule takes no parameter. `apart-from` names a
class because that rule needs the other one.

**With the built-in rules.** A class the file says nothing about keeps
the built-in rule, so a version without the keys behaves as before.

- An `apart-from` entry replaces the built-in pair for both of its
  classes when it is on `room` or `pusher`, or names one of them.
  `false` on either turns the pair off.
- A `spread` entry decides for its class, with or without `room` and
  `pusher` in the version.

**One pair for a class.** A wake-up looks for one companion, held here
or not. So a class is in one pair at most: the project file, the deploy
and the manifest's decoder all refuse a second pair for a class, and
`false` on a class that another class names. Several companions for one
class would need the wake-up and the drain to look for each; that is not
built.

**One rule for a class.** A class is not both spread and kept apart: the
spread choice could be the node that holds the companion. `spread =
true` on a class of a pair (by its own entry, by one that names it, or
by the built-in pair) is refused in the same three places. A class named
`edge` that an entry pairs is kept apart and not spread.

**In the version.** `apart <class> <other>` (`-` for none) and
`spread <class> on|off`, one line per class that says the key, in
manifest format 8. A version without such a line keeps the format it had
(1 to 7), its bytes and its id, so nodes of `0.8.1` keep reading it. A
deploy from the project file writes what the file says and removes what
it does not; a deploy with flags keeps the version's entries for the
classes that stay.

**The gate.** Format 8 is the first format behind the upgrade floor
([ADR 0043, section 2](0043-rolling-upgrades.md#2-the-gate-table)): a
deploy that needs it is refused while the app's floor is below `0.9`,
while a live node of the app runs an older version, and while a live
node does not announce the capability word `apart`. Every node of `0.9`
announces it, with placement on or off: the word says the node reads the
format. A node with `--no-placement` reads the lines and applies
neither rule, as it applies no built-in one.

**Not included.** A pair of more than two classes; a companion with
another name than the actor's; a parameter for `spread` (a subset of
nodes, a count). An outside request still wakes a spread actor where it
lands, as it does an `edge`: the rule is for wake-ups by a call or an
alarm.

**Tests.**

- The pure rule with entries: `placement::Asked::companion`, `spreads`,
  `check` and `kept`; nothing asked gives the built-in rules.
- The project file: both keys, each refusal.
- The manifest: format 8 round trip, each refused line, and the older
  format, bytes and id without the keys.
- The gate: `a_gate_needs_the_floor_and_every_live_word` (the row of
  format 8) and `format_8_needs_the_floor_and_apart_on_every_live_node`.
- `tests/cluster/apart_from.rs`, plain and over TLS, with three nodes of
  `0.9` that follow the app and classes named `feed`, `fan` and `leaf`:
  without the keys they wake where they land; the deploy with the keys
  is refused under a floor of `0.8`, beside a live node of `0.8.1` and
  beside a node of `0.9` without `apart`, each time leaving `current` as
  it was; accepted once every node announces the word; then `fan` and
  `feed` of one name end on different nodes, in both orders, and each
  `leaf` called from a `feed` wakes on its own choice. A fresh app's
  first deploy with the keys is accepted and creates its floor.

## Amendment (2026-11-26): spread actors go home, and the load rule leaves them

ADR 0040, section 14 gives each spread actor a home, the rendezvous
choice of the amendment of 2026-11-22 over the nodes that serve, and
moves a spread actor that a node holds away from its home there once the
cluster is quiet (that section says when, how fast and what pauses it).
Two lines of this record change with it:

- **"A running actor never moves because of it"** becomes: the load rule
  never moves a running actor; a spread actor is moved to its home.
  Simulation property 3 above holds for the classes that do not spread.
- **The load rule does not apply to a spread class** while rebalancing
  is on. A placed hop could send a spread actor to a lighter node past
  the floor, and the rebalance would move it back at its next quiet pass:
  two rules cannot both be the fixed point of one actor, and the name
  rule is the one every node agrees on. A call or request for a spread
  actor nobody holds is therefore never placed; it wakes by the spread
  rule, and the receiver's own limits still apply (a node with no
  resident slot answers `503` before the actor, and the actor wakes where
  it was called). With `--no-rebalance` the load rule applies first, as
  before.

Section 1's "an upgrade wakes it where it arrives" keeps holding for
every class but a spread one: a WebSocket upgrade for a spread actor
nobody holds has its home take it first, and wakes it where it arrives
only when that take is refused or fails. The two reasons section 1 gives
against a take at wake-up do not hold for it: a spread actor exists
before any socket reaches it (a room's `init` creates its edges), and a
socket for an actor held elsewhere is relayed already.

Nothing new is sent or written for placement: no mark, no header, no
bucket key. The heartbeat's load line gains `misplaced=<n>`, which older
readers skip as section 2 says of unknown load keys.

**Tests.** The exemption: `tests/cluster/spread.rs`'s
`an_edge_away_from_home_moves_there_unless_rebalancing_is_off` and the
existing `a_rooms_edges_wake_spread_and_stay_put` (no edge placed by the
load rule); the rest is ADR 0040's.
