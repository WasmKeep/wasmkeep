# Socket admission options

`wasmkeep serve` accepts these options:

| Option | Requested default | Meaning |
| --- | --- | --- |
| `--max-sockets-per-actor N` | 1,000 | Opening, active and closing sockets belonging to one actor on its owner. |
| `--max-sockets-per-app N` | Node cap | Sessions belonging to this app; set below the node cap for per-app isolation. |
| `--max-sockets-per-node N` | 50,000 | Sessions on this process, including sessions it relays to another owner. |

Zero refuses every WebSocket at that scope. These are concurrent session limits,
not request rates. An upgrade over capacity receives HTTP 503 with
`Retry-After: 5`; a slot is reserved before 101 or an outbound relay connection.
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

A process can serve several apps. Embedders hosting several `ActorService`s
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

## Room memory after clients leave

Socket caps also bound how many per-connection allocations can be live at
once. They are not RSS limits. The release CLI uses Rust's system allocator
(glibc on the GNU/Linux release target), with no mimalloc feature or global
allocator override. Freed allocations can remain in its arenas and be
reused without their pages leaving RSS. `[heap]` alone misses secondary
arenas, which are anonymous `mmap` regions.

The current socket configuration eagerly allocates a 128 KiB read buffer
per transport: 125 MiB of capacity at 1,000 owned sockets, before tasks,
registrations and frames. The send queue is limited to 256 messages and
4 MiB per socket; the protocol write-buffer limit is 8 MiB and the incoming
message/frame limit is 1 MiB. These are separate limits, not an observed
per-socket footprint or an exact bound on allocation capacity. Relayed
sessions have transports on both nodes. Closing sessions retain admission
until the reader, writer and close callback finish; their buffers and
permits are then dropped. The socket registry can retain its bounded
high-water capacity.

An edge's replay buffer is bounded to 512 messages or 1 MiB of encoded
content, with additional JavaScript object overhead. The room history is
bounded to 2,000 rows or 8 MiB of bodies in SQLite. JavaScript collection
does not shrink Wasm linear memory. The instance must hibernate before its
pages can be released; socket closes do not directly evict it, and further
publishes, alarms or other events can keep it resident.

With Wasmtime 50.0.2's current pool defaults, dropping a store decommits
linear memories and tables (`keep_resident = 0`, decommit batch size 1).
Async stack pages are retained for reuse, and bounded by the instance pool.
The enormous reserved virtual address space is not committed RSS.

Use these controls together:

- `--max-sockets-per-node` and, for isolation, `--max-sockets-per-app` cap
  concurrent sessions. Leave room for reconnect/close overlap; the actor
  cap alone cannot bound a room distributed across many edges.
- `--max-instances` caps live front and actor instances, and the async
  stack pool. `--max-front-requests` reserves a part of those slots for
  the front; the remaining slots bound resident actors. For example,
  64 slots with eight reserved for the front allow 56 resident actors.
  Each component is limited to two linear memories of at most 128 MiB
  each by the current runtime configuration; these are ceilings, not
  expected consumption. Embedders can lower `RuntimeConfig::max_memory_bytes`.
- Keep `--idle-timeout-secs` finite (30 by default). On Linux/glibc,
  `--idle-memory-trim-secs` samples resident actors plus sockets in use
  (60 seconds by default; 0 disables it). It trims when the sum falls to
  at most half its sampled peak since the last trim, or is zero, with a
  process-wide 60-second minimum cooldown and serialization. The peak
  resets after every actual trim. A busy app does not prevent reclamation
  after a large enough drop, but steady nonzero activity never triggers it.
  This cannot release live allocations, thread caches, partly used pages
  or live pool instances, and has no RSS target.
- `--malloc-arena-max N` sets glibc's arena cap at CLI startup, before worker
  threads start. The default is available parallelism clamped to **2..8**
  (2 if detection fails). `0` leaves glibc's default/environment policy
  untouched; explicit positive values override it. This is a no-op outside
  Linux/glibc. Fewer arenas can reduce retention but increase allocation
  contention; fleet measurement is pending. For a service unit, use
  `Environment=MALLOC_ARENA_MAX=8` together with `--malloc-arena-max 0` to
  let the environment choose the cap; see [operations](../operations.md#allocator-policy).

Choose socket and instance caps from the intended payload, history and
slow-client workload, then measure at those caps. Do not extrapolate the
small-actor memory estimate or multiply one room measurement into a hard
RSS guarantee. A service/container memory limit is the final process
ceiling, with an OOM/restart consequence; admission caps should leave
headroom below it.

The [local diagnosis](../benchmarks.md#local-room-memory-recovery) attributes
retention to free allocator pages. Patched workstation runs paged; fleet
measurement is pending.
