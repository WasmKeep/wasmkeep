# Socket admission options

`wasmkeep serve` accepts these options:

| Option | Requested default | Meaning |
| --- | --- | --- |
| `--max-sockets-per-actor N` | 1,000 | Opening, active and closing sockets belonging to one actor on its owner. |
| `--max-sockets-per-node N` | 50,000 | Sessions on this process, including sessions it relays to another owner. |

Zero disables admission at that scope. These are concurrent session limits,
not request rates. An upgrade over capacity receives HTTP 503 with
`Retry-After`; a slot is reserved before 101 or an outbound relay connection.
Closing sockets retain their slot through transport and close-event cleanup.
A local front hand-off uses one session, not two. A relay occupies one session
on each process involved. No protocol or bucket format changes are needed.

At startup the soft `RLIMIT_NOFILE` is raised toward the hard limit, up to
1,048,576. The resulting soft limit bounds an admission estimate:
256 reserved descriptors, one per public or cluster connection, three per
resident actor and two per socket session. Cluster connections receive at
most an eighth of the unreserved capacity (and at most 1,024); public
connections receive at most a quarter of the remainder. Actors and sockets
share what remains, reallocating unused shares. Both HTTP and socket slots
are held during upgrade handover, so that transition is counted conservatively.
Startup fails if the estimate cannot fit a public connection, an actor when
serving actors, or a cluster connection when serving a cluster listener.

The startup log and [metrics](../monitoring.md#open-files) expose requested
and effective socket ceilings. The effective actor ceiling cannot exceed
the app or process ceiling. Outbound clients and bucket activity are not all
bounded by this calculation: it is an admission estimate, not a guarantee
against descriptor exhaustion. If the OS limit cannot be read, requested
limits remain in effect and startup logs a warning.

The current CLI serves one app. Embedders hosting several `ActorService`s
must share one `SocketBudget` across their configurations; `SocketLimits`
adds an app ceiling beneath that shared process ceiling.

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
workload. `bench/room.sh` sets the caps to its requested room size; override
with `ROOM_MAX_SOCKETS_PER_ACTOR` and `ROOM_MAX_SOCKETS_PER_NODE` to measure
clamping and refusal. `roomload sample` records requested/effective limits
and session usage alongside its existing FD and resident-memory samples.
No new room-size performance result is implied by these options.
