# 0037. Socket session admission

- Status: accepted, as built (0.5.0)
- Date: 2026-11-16

## Context

An HTTP upgrade outlives HTTP admission. A relay holds two transports without
registering a socket with a local actor. Registry counts alone therefore
cannot bound a node's concurrent socket load.

## Decision

Reserve a process session after failover holds and admission probes, before
forwarding an upgrade or sending 101. Transfer the reservation
to the upgraded task, retaining it through transport and close cleanup. Each
relay consumes a session on the ingress process and on the owner. Local front
hand-offs reserve once. Actor registration remains atomic on the owner;
application reservations sit beneath a shared process allowance.

Expose actor, app and node ceilings, preserving requested actor/node defaults
of 1,000 and 50,000. The app ceiling defaults to the node ceiling for
single-app compatibility; per-app isolation requires an explicit app ceiling
below the effective node ceiling (or `SocketLimits::per_app` for embedders).
Zero refuses every WebSocket at that scope. Capacity refusals use 503 and
`Retry-After`.
Report requested and effective socket limits in startup logs and metrics.

Allocate descriptor capacity for public connections, cluster connections,
resident SQLite databases and socket sessions together. Charge two descriptors
per session to cover relays. Keep 256 descriptors for listeners and other host
activity. Allocate cluster and public shares first, then reserve a minimum socket share of
`min(requested sessions, remaining descriptors / 4)`. Give actors their request
where it fits alongside that share, and sockets any unused actor capacity. An explicit node socket request instead
shares that capacity between actors and sockets, reusing unrequested shares.
At low limits retain one requested public/cluster connection and resident
actor for degraded operation, warning when reserved headroom cannot be met.
Sockets receive no minimum floor when reserved capacity is exhausted. Relay
EOF propagates a write-half shutdown while the reverse stream keeps draining.
The configured WebSocket close wait bounds the entire wind-down, including
shutdown, before transport teardown releases the ingress permit.

Amendment (0.5.1): with the default request (50,000 sessions per node), a node
whose soft limit is 65,536 admits about 25,800 sessions (25,778 with the
cluster port, 26,290 without). That is the budget working as designed, not a
misconfiguration, so startup no longer warns. When only the default request is
cut, it logs one INFO line stating the effective number (and does not emit the
generic "descriptor admission estimate reduced" warning for it). The warning
stays for an explicit `--max-sockets-per-node` that does not fit, and for any
cut of actors or connections. The default stays 50,000 rather than a number
that fits 65,536, so nodes with higher limits keep their capacity.

## Consequences

Relays now consume the node ceiling, tightening the previous behavior. No wire
format, ownership or replication changes follow. The reserved allowance is an
operating estimate: bucket clients, outbound guest HTTP and transient database
work do not have a shared hard descriptor semaphore. Operators must monitor
actual descriptors and memory. The room harness records the new ceilings;
benchmark results require an actual measured run.
