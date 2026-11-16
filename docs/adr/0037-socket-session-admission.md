# 0037. Socket session admission

- Status: proposed
- Date: 2026-11-16

## Context

An HTTP upgrade outlives HTTP admission. A relay holds two transports without
registering a socket with a local actor. Registry counts alone therefore
cannot bound a node's concurrent socket load.

## Decision

Reserve a process session before routing an upgrade. Transfer the reservation
to the upgraded task, retaining it through transport and close cleanup. Each
relay consumes a session on the ingress process and on the owner. Local front
hand-offs reserve once. Actor registration remains atomic on the owner;
application reservations sit beneath a shared process allowance.

Expose actor and node ceilings, preserving requested defaults of 1,000 and
50,000. Zero disables admission. Capacity refusals use 503 and `Retry-After`.
Report requested and effective socket limits in startup logs and metrics.

Allocate descriptor capacity for public connections, cluster connections,
resident SQLite databases and socket sessions together. Charge two descriptors
per session to cover relays. Keep 256 descriptors for listeners and other host
activity. Allocate cluster and public shares first, then divide remaining
capacity between actor storage and sockets, reusing unrequested shares.
Reject startup when required services cannot fit instead of inventing capacity.

## Consequences

Relays now consume the node ceiling, tightening the previous behavior. No wire
format, ownership or replication changes follow. The reserved allowance is an
operating estimate: bucket clients, outbound guest HTTP and transient database
work do not have a shared hard descriptor semaphore. Operators must monitor
actual descriptors and memory. The room harness records the new ceilings;
benchmark results require an actual measured run.
