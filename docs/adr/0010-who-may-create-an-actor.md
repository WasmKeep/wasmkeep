# 0010. Who may create an actor

- Status: accepted (implemented 2026-11-04, revised after an adversarial review the same day). Tests, mutation-checked:
  - `tests/access.rs`, single node:
    - unknown names answer 404 and leave no database;
    - an actor creates one, after which it is reachable;
    - deleted, it is unknown again;
    - a deleted actor's sockets are closed and cannot bring it back;
    - `internal` answers actors only;
    - sockets are refused the same way.
  - `tests/cluster/`, `existing_access_checks_the_bucket_and_leaves_nothing_for_unknown_names`, under a bucket prefix and with an escaped name:
    - no lease or snapshot for an unknown name, over HTTP and WebSocket;
    - an actor created on one node is reachable through another;
    - an `internal` actor answers a call from another node, and refuses outside requests even with forged node and caller headers.
- Date: 2026-11-04
- Milestone: [M3](../../ROADMAP.md#-m3-first-public-release)

## Context

There is no "create" step: the first event to an address creates the actor
and its database. For rooms and counters that is the point. For classes
whose actors another actor manages, it is a hole, found building Watch
([dogfood](../dogfood.md), frictions 8 and 9):

- `GET /actor/monitor/<anything>` created an empty monitor: a database on
  disk and, in a cluster, a lease and a snapshot in the bucket. Leases are
  never deleted (their generation fences replication), so every name
  requested left an object in the bucket for good. Anyone who could reach
  the server could fill the disk and the bucket, one name per request.
- Every such class re-implemented the same check in code, and forgetting
  it was silent: the monitor did.

## Decision

A mode per class, `--access CLASS=MODE` on `serve` and `dev`:

- **`open`** (the default): as before.
- **`existing`**: outside requests reach only actors that exist. A new name
  answers 404, and nothing is created: no database, no lease, nothing in
  the bucket. Other actors create them, with `actors.call`.
- **`internal`**: outside requests are refused with 403. Only actors (calls)
  and alarms reach the class.

"Outside" is a request arriving at a node's front door. Calls between
actors are inside, including calls between nodes, which carry the
cluster secret. A client cannot pose as one: the caller header is stripped
from outside requests and the secret is checked as before. Requests forwarded to the owner do not carry the secret, so the
owner checks them again; the actor is resident there, so that costs
nothing. WebSockets always come from outside: the upgrade is checked, and
under `existing` and `internal` so is every socket event (in the same event
as the work, so a delete in between counts), and a delete closes the
actor's sockets. A socket event never recreates a deleted actor of such a
class. Open classes keep their sockets on delete, as before.

**Existence** is decided without creating anything, before the lease. The
actor exists if it is in memory on this node, has a database file here, or,
in a cluster, has anything under its prefix in the bucket (one listing,
read no further than the first key).

`--access` naming a class that is not served is an error: a typo would
leave the class open. With `--app`, the deployed classes count as served;
a class a later deploy adds cannot be named until the node restarts. Every
node of a cluster must get the same rules.

## Consequences

- Watch runs monitors as `existing`: the hub creates them, and dashboards
  read them and stream from them as before.
- **In a cluster, a deleted actor still counts as existing.** A delete
  leaves an empty snapshot in the bucket (so no node restores the old
  data), and the check sees the prefix. An outside request can then wake
  it as an empty actor, running `on-init`, and a request that writes takes
  a new lease and ships new segments for it. This is limited to names that
  once existed (the fill-the-bucket attack stays closed), but in a cluster
  a delete is not a revocation. On a single node a deleted actor is unknown
  again. A delete marker would make the two agree; it changes replication,
  so it waits for a case in `sim/`.
- **A miss in a cluster costs one bucket LIST.** An attacker can drive that
  with random names (S3 bills LISTs). It is bounded by the request rate
  and creates nothing; a short negative cache per node would cut it, if
  the bill shows it matters.
- **A refused name holds a mailbox in memory** until the idle timeout (no
  database, no instance: a few hundred bytes).
- **Singletons.** A class with one well-known actor (`hub/main`) could not
  create it under `existing`, since nobody calls it. `existing:NAME,...`
  (added the same day) lists names outside requests may create; the rest
  of the class stays `existing`.
- Deployed apps (`--app`) take the rules from the command line too; carrying
  them in the deploy manifest, so they travel with the version, is done:
  see [ADR 0013](0013-access-rules-and-config-in-deploys.md). `--access`
  with `--app` is refused once rules are deployed.
- If a node cannot read the cluster secret, calls from other nodes look
  like outside requests: an `internal` class answers them 403. This fails
  closed.
