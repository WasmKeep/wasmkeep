# Socket admission options

`wasmkeep serve` accepts these options:

| Option | Requested default | Meaning |
| --- | --- | --- |
| `--max-sockets-per-actor N` | 1,000 | Opening, active and closing sockets belonging to one actor on its owner. |
| `--max-sockets-per-app N` | Node cap | Sessions belonging to this app; set below the node cap for per-app isolation. |
| `--max-sockets-per-node N` | 50,000 | Sessions on this process, including sessions it relays to another owner. |

Zero refuses every WebSocket at that scope. These are concurrent session limits,
not request rates. An upgrade over capacity receives HTTP 503 with
`Retry-After`; a slot is reserved before 101 or an outbound relay connection.
Closing sockets retain their slot through transport and close-event cleanup.
Failover-held upgrades reserve only after the hold ends and admission probes
complete. Relays allow 250 ms for the reverse stream after either direction
finishes, then release both transports and the ingress slot.
A local front hand-off uses one session, not two. A relay occupies one session
on each process involved. No protocol or bucket format changes are needed.

At startup the soft `RLIMIT_NOFILE` is raised toward the hard limit, up to
1,048,576. The resulting soft limit bounds an admission estimate:
256 reserved descriptors, one per public or cluster connection, three per
resident actor and two per socket session. Cluster connections receive at
most an eighth of the unreserved capacity (and at most 1,024); public
connections receive at most a quarter of the remainder. By default, actors
retain capacity alongside a minimum socket share of the smaller of the
requested sessions and a quarter of the remaining descriptors (two descriptors
per session). Actors keep their full request when it fits alongside that share;
sockets receive unused actor capacity. An explicit `--max-sockets-per-node`
shares the remainder between actors and sockets, reallocating unused shares.
Both HTTP and socket slots
are held during upgrade handover, so that transition is counted conservatively.
At very low limits, startup retains at least one requested public connection,
resident actor and cluster connection, with no socket capacity if none fits.
It logs a degraded-start warning when reserved headroom cannot be met;
raise the OS limit to restore capacity.

With the defaults, a soft limit of 65,536 admits about 25,800 sessions per
node (25,778 on a cluster node, 26,290 on a single node), not 50,000. The node
logs one INFO line with the effective number and no warning, since nothing was
asked for beyond the default. An explicit `--max-sockets-per-node` that does
not fit is a warning ("socket session capacity reduced by descriptor budget").
To admit more, raise `LimitNOFILE` (about two descriptors per session).

The startup log and [metrics](../monitoring.md#open-files) expose requested
and effective socket ceilings. The effective actor ceiling cannot exceed
the app or process ceiling. Outbound clients and bucket activity are not all
bounded by this calculation: it is an admission estimate, not a guarantee
against descriptor exhaustion. If the OS limit cannot be read, requested
limits remain in effect and startup logs a warning.

The current CLI serves one app. Embedders hosting several `ActorService`s
must share one `SocketBudget` across their configurations; `SocketLimits`
adds an app ceiling beneath that shared process ceiling. Per-app isolation
requires `--max-sockets-per-app` (or `SocketLimits::per_app` for embedders)
set below the effective node cap. The default remains the requested node cap
(50,000 for embedders) for compatibility with single-app deployments; it
does not isolate apps from one another. Choose app ceilings so one app cannot
consume the whole node allowance.

## Tuning examples

For a node intended to hold 10,000 sessions, with at most 2,000 on one actor:

```sh
# Also set LimitNOFILE=65536 in the service unit when using systemd.
ulimit -n 65536
wasmkeep serve --actor chat=chat.wasm \
  --max-sockets-per-actor 2000 --max-sockets-per-node 10000
```

To disable WebSocket admission while retaining HTTP actor requests:

```sh
wasmkeep serve --actor chat=chat.wasm --max-sockets-per-node 0
```

Raise caps only with memory and descriptor observations under the intended
workload. `bench/room.sh` sets the caps to its requested room size plus 1,000
slots for reconnect and close-cleanup overlap; override
with `ROOM_MAX_SOCKETS_PER_ACTOR` and `ROOM_MAX_SOCKETS_PER_NODE` to measure
clamping and refusal. `roomload sample` records requested/effective limits
and session usage alongside its existing FD and resident-memory samples.
No new room-size performance result is implied by these options.
