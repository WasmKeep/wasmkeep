# 0017. Moving one actor to a named node

- Status: accepted and implemented (2026-11-05). Revised the same day after a review of
  the first draft. That draft let the owner claim the actor back during
  the move, and sent a take call to nodes that
  would have run it as a guest request. A move is now a drain of one
  actor, plus a take call sent only to nodes that advertise it.
- Amended by [ADR 0020](0020-one-lease-per-node.md) (2026-11-07): ownership is a node lease plus an
  ownership record per actor; where this record says lease, read the
  actor's hold (`LeaseView`: the hold exists and the node lease is valid).
  `sim/tests/madsim_drain.rs` models the node leases of
  [ADR 0020](0020-one-lease-per-node.md).
- Date: 2026-11-05
- Milestone: [M4](../../ROADMAP.md#m4-operating-a-cluster) (the part [ADR 0014](0014-draining-a-node-and-moving-an-actor.md) left open)

## Context

`wasmkeep actor move TENANT/CLASS/NAME --to NODE` writes
`control/move/<tenant>/<class>/<name hex>/<target>` (ADR 0014). Every node
lists those keys once per heartbeat period (3 s by default), ignores
those older than 5 minutes (deleting them), and does nothing else with
them. An owner can release an actor, but the next event claims it on
whichever node it lands, often the old owner itself: a node that is not
draining takes back a free lease on its next event for the actor.

What the code offers:

- **The drain** (ADR 0014). While a node drains, `ensure_owner` returns
  `Elsewhere { peer }` for any actor it does not hold in memory, so
  requests are forwarded, calls go to the peer, upgrades are relayed,
  and alarms are handed off (`hand_off_alarm`). Its hand-over job runs in
  the actor's mailbox after the queued events (each already uploaded):
  - it keeps the lease while a socket is open or closing;
  - it reads the committed alarm;
  - it closes the instance and database;
  - it releases the lease, retrying for up to one lease TTL
    (`LeaseReturn::Failed` past that, when the lease expires on its own).
- **A forwarded request makes the receiver claim.** A node that is not
  draining acquires a free lease on the first event it gets.
- **Signed node calls with a marker header.** The alarm hand-off is one:
  the signature covers the marker. A node from before a marker existed
  computes another signature, refuses the call as signed, and treats it
  as an outside request: it runs the guest's handler.
- **Heartbeats.** Line 9 is the drain state and line 10 says the node
  reads secrets (ADR 0016). Readers ignore lines past those they know.
- **Hints.** A node keeps an actor's owner for at most 1 s, in the
  service and in the actor's own state.

## Decision

### A move is a drain of one actor

Each node keeps a map of moves under way (actor to target and a deadline
30 s ahead), shared by the service and every actor's state. While an
actor has an entry, `ensure_owner` behaves as if the node were draining,
for that actor only, with the move's target as the peer: it returns
`Elsewhere { peer: target }` instead of claiming, including after the
lease was released. The entry lives outside the actor's state on purpose:
the paths that forward an `Elsewhere` event hibernate the state, and an
evicted or rebuilt state must still forward rather than claim the actor
back. Every path that already handles
`Elsewhere` then does the right thing with no new code:

- requests queued behind the hand-over, and new ones, are forwarded to
  the target, which claims on the first one;
- `actors.call` goes to the target;
- upgrades are relayed there;
- an alarm firing during the move is handed off there.

The entry is removed when the move ends: after a successful take (the
lease then names the target, so this node forwards to it), or after a
failed one (the next event then claims the actor as usual).

### The take call

Nothing above makes the target the owner if no event arrives: an idle
actor would be claimed again by the next event wherever it lands. So the
old owner asks the target to take it:

- **Who receives it.** Only nodes that advertise it. Heartbeat line 11
  holds capability words; a node that implements this ADR writes `take`.
  Older nodes, which would run a take as a guest request (see Context),
  are never sent one and are not valid move targets.
- **Shape.** A signed node call to the actor's path, with the marker
  header `x-wasmkeep-take`, covered by the signature. Its signed body holds
  `wasmkeep-take 1` and the tenant. The marker is stripped from client
  requests and guests' calls, and honoured only on a verified, single-use
  call. Time limit: 10 s (a restore can take a few).
- **At the target, a job in the actor's mailbox:**
  1. Refuse, without acquiring, while it is draining or cannot open the
     app's secrets (503), for another tenant (403), or for a class it
     does not serve (404).
  2. Forget any owner it had learned for the actor, in the service and in
     the actor's state: a hint left from forwarding to the old owner must
     not answer for the bucket.
  3. Check that the actor exists (in the bucket, or locally), the way
     `existing` access rules do; if not, 404. A mistyped name never
     creates an actor.
  4. `ensure_owner`: acquire and restore, no guest code.
  5. Open the restored database (without instantiating the component),
     read the committed alarm, and arm it unless an earlier time is
     registered. If it cannot be read, answer 503: the old owner keeps its
     registration and asks again.
  6. Answer 200. If the acquire lost, answer 409 with header
     `x-wasmkeep-holder: <node id>`; if the bucket did not answer, 503.

### Moving a resident actor (the owner acts)

Once per heartbeat period, a node that is not draining looks at the move
keys for actors it holds (resident, with a valid lease). A draining node
does not act on move keys: its drain already moves every actor. If
several keys name the same actor, the newest (by last-modified time)
counts and the others are deleted. For one key, to target T:

1. **Check T.** It must be another node, and an eligible peer (live, same
   app, `serving`, not asked to drain, not suspect, reachable), and it
   must advertise `take`. Otherwise the key stays and is tried again on
   later passes until it expires: a target may be starting, or briefly
   suspect.
2. **Close the actor's sockets** with 1012 and wait (5 s), as a drain
   does. While a move is pending, upgrades for the actor are relayed to T,
   so clients that reconnect do not land back here.
3. **Queue the move job** in the actor's mailbox. It runs after the
   queued events:
   - it does nothing if this attempt was abandoned meanwhile (each
     attempt carries a token; a job whose token is no longer current
     returns at once);
   - otherwise it runs the drain's hand-over steps: keep the lease if a
     socket is open or the alarm cannot be read (`Busy`), read the alarm,
     record the move, close, release.
4. **By outcome:**
   - **Released.** Send the take call to T:
     - **200, or 409 naming T:** the move is done. Forget the alarm here
       (only that registration, `clear_if`), delete the key, hibernate
       the stub.
     - **409 naming another node:** another node claimed first, which is
       correct. Delete the key, log it, hibernate the stub.
     - **Anything else** (refused, 503, no answer): clear `moving_to`,
       keep the key, and try again on a later pass. The actor is free
       meanwhile; the next event claims it, possibly here.
   - **`Busy`** (a socket), or the queued events not done within 30 s:
     clear `moving_to`, keep the key, try again later.
   - **The release failed** (`Failed`): the lease expires on its own
     (ttl + grace). Clear `moving_to`, keep the alarm and the key, and try
     again after that.
5. **The deadline.** If 30 s pass with `moving_to` set and no outcome,
   it is cleared and the stub hibernated.

At most 16 moves run at a time per node, each in its own task, so a slow
move never holds up the drain loop. A key is tried on every pass until it
is done, dropped, or expires (5 minutes). A node that does not serve the
actor's class leaves the key to the nodes that do.

### Moving an actor nobody holds (the target acts)

A node that lists a move key naming itself, for an actor no node holds
(the lease missing, released or expired), runs the take job locally.
On 200 (or "already holds it") it deletes the key. On 404 (no such
actor) it logs and deletes the key. Otherwise it keeps the key for a
later pass. If another node holds the actor, the target does nothing;
that owner acts.

### What a move does not promise

- **It does not stick.** A moved actor that goes idle hibernates on the
  target after the idle timeout, and its next event claims it wherever
  that event lands. A move puts an actor somewhere now; it does not pin
  it (ADR 0014 rejects pinning).
- **It can evict.** A take on a full target hibernates another idle actor
  there, as any new actor would.

## Alternatives

- **The first draft:** a service-level routing pin and a take call to any
  eligible node. Rejected: events queued in the actor's mailbox never
  consulted the pin, so the owner claimed the actor back. And nodes of
  today's release, which publish `serving`, would have run the take as a
  guest request.
- **A dedicated `/_wasmkeep/node/take` route** instead of a marker on the
  actor's path. Older nodes would answer 404 rather than run a guest.
  With the capability gate they are never sent a take, so the marker,
  which reuses the call's signing and port rule, is kept.
- **Pinning an actor to a node.** Rejected in ADR 0014.
- **Relying on the next request** (no take). An idle actor would not move.

## Consequences

- No lease rule changes. The take is an ordinary conditional acquire,
  and two nodes racing to claim end with one owner.
- **New surface:**
  - `moving_to` in an actor's state, honoured by `ensure_owner`;
  - a second signed marker (`x-wasmkeep-take`);
  - heartbeat line 11 (capabilities, `take`);
  - the take job;
  - move keys acted on by owners and targets.
- **During a move:** the actor's events at the old owner are forwarded to
  the target (one hop); those queued behind the hand-over wait for it
  (milliseconds, or up to the 30 s wait for a busy actor).

## Tests

- **Cluster tests** (`crates/wasmkeep-server/tests/cluster/`, plain and
  TLS):
  - a resident actor moves under concurrent requests: the target ends up
    holding it, with its data, no request fails beyond 503 with
    `Retry-After`, and requests sent to the old owner reach the target;
  - an alarm due during the move fires on the target on time, and one due
    after the move fires there once the old owner is stopped (sweep
    slowed);
  - an actor nobody holds is taken by the target; a mistyped name is not
    created;
  - a target without `take` (no line 11), draining, of another app, or
    down: the key is dropped or retried, and the actor stays reachable;
  - a target with a fresh owner hint for the old owner still takes;
  - a socket on the moving actor closes with 1012 and its close event runs
    first; clients that reconnect reach the target;
  - a release that fails keeps the key and the alarm;
  - moving to the current owner deletes the key;
  - a take marker from a client, or in a guest's call, is an ordinary
    request.
- **Unit:** the take body; the marker covered by the signature; line 11
  read by older and newer readers.
- **`sim/` case** (ordering and leases need one, see
  [CONTRIBUTING.md](../../CONTRIBUTING.md)): extend
  `sim/tests/madsim_drain.rs` with a move under concurrent events and an
  alarm due during it:
  - invariants: one owner, no alarm lost, the target holding the actor
    when the move reports done;
  - a variant that hibernates the stub right after the release (so the
    owner claims back) must be caught.

## Plan

| Slice | Size |
| --- | --- |
| Heartbeat line 11 (`take`); `moving_to` in `ensure_owner` | S |
| The take call and job (refusals, hints, existence, acquire, alarm) | M |
| Owners act on move keys (check, sockets, move job, outcomes, deadline) | M |
| Targets take actors nobody holds | S |
| Sim case, adversarial review of the code | M |
| Docs: `docs/cluster.md`, ADR 0014's note | S |
