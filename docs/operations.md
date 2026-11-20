# Resuming destructive operations

`actor delete`, `app delete`, `actor restore` and `backup restore` print
`Operation ID: <id>` before waiting. Save that ID. If the terminal disconnects
or a wait ends, repeat the command with `--resume <id>` and the same bucket,
app and actor. A resume reads the original operation; it never submits another
actor request or selects a new app operation.

```sh
wasmkeep actor delete cart/alice --app acme/shop --bucket s3://live/prod --resume ID
wasmkeep app delete --app acme/shop --bucket s3://live/prod --resume ID
wasmkeep actor restore cart/alice --app acme/shop --bucket s3://live/prod --resume ID
wasmkeep backup restore --app acme/shop --bucket s3://live/prod --from s3://archive --resume ID
```

For actor restore, omit the original source selection (`--txid`, `--at`,
`--from`) when resuming. For an unfinished backup restore, retain the original
source, run, deployment choice and in-place option. Its existing journal and
exclusive-lock recovery rules still apply: stop the previous CLI and allow
its outstanding bucket requests to expire before clearing an abandoned lock.

Actor outcomes remain beside their request keys for the request TTL (one hour),
then the node expires them. Reading an outcome does not consume it. After
expiry, an ID is unknown and nothing is filed. Unknown IDs never imply success
and never authorize a new delete or restore.

A pending actor delete ends `superseded` if a later operator restore or a
self-delete, expiry or other delete at or after the request's bucket time has
replaced the actor. The node checks the restored markers and the delete reset's
bucket time before changing data. Requests still have empty bodies.

App delete resumes must match the existing tombstone ID before continuing
cleanup. After recreation removes that tombstone, the old ID is unknown.
Backup restore resumes must match the active journal nonce. Once the journal
is gone, `apps/TENANT/APP/restored` answers done only if it holds that nonce;
a missing or different marker makes the ID unknown. These app markers follow
their existing lifecycle, rather than the actor request TTL.

A 0.5.0 CLI reports a superseded delete as dropped and fails closed. A 0.5.0
node neither recognises nor expires the `.superseded` key; an upgraded node
is needed to expire it. Request bindings, capability negotiation and durable
completion receipts are deferred to 0.6.0.

## Allocator policy

On Linux/glibc, the CLI applies `--malloc-arena-max N` before starting worker
threads. It defaults to available parallelism clamped to 2..8 (2 if detection
fails). `0` skips `mallopt`, preserving glibc's default or environment policy.
Explicit positive values override `MALLOC_ARENA_MAX`; other platforms ignore
this setting. To manage the cap through a systemd service unit instead:

```ini
[Service]
Environment=MALLOC_ARENA_MAX=8
```

Add `--malloc-arena-max 0` to the unit's existing `wasmkeep` command. The
environment must be present when the process starts. A smaller cap can
reduce allocator retention at the cost of contention.

`serve --idle-memory-trim-secs 60` samples resident actors plus socket
sessions and trims at half the peak since the previous trim, or at zero.
Calls are serialized, run on a blocking worker, and are separated by at
least 60 seconds after completion. `0` disables trimming. Steady busy
activity does not trigger trimming on a timer. See [socket limits](reference/socket-limits.md#room-memory-after-clients-leave)
and [allocator metrics](monitoring.md). These controls have no RSS target;
patched workstation runs paged and fleet measurement remains pending.
