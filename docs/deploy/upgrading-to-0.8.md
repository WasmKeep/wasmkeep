# Upgrade a cluster to 0.8.0

0.8.0 hardens what a node accepts and writes: private env files, data and
cache directories, bounded request heads, upload budgets, tighter SQLite
limits, and a metrics listener that checks its `Host`. Ports, routes, exit
codes, the bucket format and the package `wasmkeep:actor@0.2.0` stay as
they were. Most of it needs nothing; the items under
[What to do](#what-to-do) each change something an operator, an actor or a
client can see.

## The binary

Upgrade the binary as [Upgrading WasmKeep](../cluster-upgrades.md#upgrading-wasmkeep)
says: stop every node, upgrade them all, start them all. 0.8.0 documents no
exception for one node at a time. Do the file and directory steps below
while the nodes are stopped, before the first start of the new binary: a
node refuses an env file, a secrets key or a cache it should not trust.

## What to do

1. **Env files** (`--env`, `--front-env`, and `wasmkeep dev`'s
   `.env.local`). On Unix a file is refused at start if other users can open
   it, its group can write it, or its group can read it while the server's
   user owns it. Use `chmod 600 FILE` for a file the service owns, or
   `chown root:wasmkeep FILE && chmod 640 FILE`. `--config` files are not
   affected.
2. **Data and cache directories.** `chmod 700` the `--data-dir` and
   `--cache-dir` of an existing install, owned by the service's user, and set
   `UMask=0077` in the unit. The cache directory is used only while it and its
   files belong to the server's user and no one else can write them;
   otherwise components compile again and the fixing command is logged at
   start. [Single node: upgrading an existing install](single-node.md#upgrading-an-existing-install-to-08)
   has the commands.
3. **Secrets key.** Unchanged rule, worth checking: the `--secrets-key` file
   is `0600` and the service's own (the root `0640` form of an env file is
   not accepted for it). After any key change, run
   `wasmkeep secrets rotate-key --finish` once no node holds the old key: it
   deletes the objects the old key sealed and fails while any are left. A key
   change does not change the values; rotate values the old key could open
   at their source ([Secrets in the bucket](../cluster-deploying.md#secrets-in-the-bucket)).
4. **Log filters.** What guests print is now the last field of the line,
   `line="..."`, after a fixed `guest output` message; before it was the
   event message. Update filters and parsers that match it. Output is limited
   to 1,000 lines a second per app and 5,000 per process, beyond each
   instance's `--guest-log-rate`.
5. **Metrics scrapers.** The metrics listener answers `421` unless the
   `Host` is an IP address, `localhost` or a name under it, a name given with
   `--metrics-host NAME` (repeatable; `[listen] metrics-hosts`), or, off
   loopback, a single-label name. A scraper that reaches a node by a dotted
   host name needs `--metrics-host` with that name
   ([monitoring](../monitoring.md)).
6. **Request heads.** A head over 64 KiB or over 100 header fields gets
   `431`. Check a proxy that adds large cookies or many headers; node headers
   are not counted.
7. **`actors.call`.** At most 96 headers and 62 KiB of path and headers (it
   was 256 and 64 KiB), and a chain of at most 16 callers. A call past them
   fails with `too-large` or `failed`: check actors that forward all of a
   request's headers, or call each other in deep chains.
8. **Stalled clients.** A response write that waits 60 s with no byte taken
   is closed; a WebSocket closed this way ends with `1006`. Nothing to
   configure; make sure clients reconnect on `1006`.
9. **File uploads.** Spooled upload bytes are budgeted: 1 GiB per app and
   4 GiB per process. An upload link answers `503` past a budget, `409` for
   the same link while its first body is arriving, and `408` for a body that
   sends nothing for 60 s or averages under 16 KiB/s after its first 30 s; a
   streamed write fails with `unavailable`. An instance has at most 16
   streamed writes open. Make uploaders and callers retry `503` and keep a
   steady rate; size `--max-blob-bytes` against the budgets
   ([files](../files.md#limits)).
10. **SQLite.** A statement compiles to at most 2,000,000 instructions,
    SQLite's heap is capped at 4 GiB per process with temporary data kept in
    memory, and a guest schema change may not name the `_wasmkeep_` prefix.
    An actor that sorts or materialises very large results can now fail with
    `quota-exceeded`: page the query or index it.
11. **StarlingMonkey actors: rebuild them.** An actor built with an earlier
    SDK leaks host handles, and now fails one request about every 2,000, as
    a guest may hold at most 4,096 at once. Rebuild it with this version's
    SDK (the TypeScript SDK releases a request's handles once it is answered)
    and deploy it. Actors in other languages need a rebuild only to pick up
    the release of a broken stream's error.
12. **Releases and CI** (for whoever cuts releases). The release workflow,
    run by hand, starts only on a `vX.Y.Z` tag or on `main`. Build scripts
    check the SHA-256 of what they download, and Python tools install from
    hash-locked requirements ([releasing](../releasing.md)).

## Other changes

- **Cluster signatures.** Signed calls on the public port are recorded
  apart from the cluster port's records, and a node refuses signed calls
  dated before it started plus the allowed clock skew, answering `503` so
  the sender signs again. New metrics
  `wasmkeep_signatures_public_dropped_total` and
  `wasmkeep_signatures_before_start_total`. Nothing to do.
- **Deploy and restore markers.** A node that cannot read its local markers
  retries, and is not ready until it can, instead of dropping the app.
  A rollback racing `wasmkeep app delete` is refused. Nothing to do.
- **Placement.** A room and its pusher wake on different nodes by default,
  and a big room's edges spread over the nodes; see the 0.8.0 notes in
  the [changelog](../../CHANGELOG.md). Nothing to deploy.

## From 0.8.0 to 0.8.1

0.8.1 is a patch: a security fix, fixes and follow-ups, with no new
feature (the [changelog](../../CHANGELOG.md) lists them). **Upgrade every
node of 0.7.0 or 0.8.0**: on those versions a `LIKE` or `GLOB` pattern
full of wildcards, in a statement an actor runs, can stop the node.

What stays as it was:

- **The bucket.** No format changes: layout, manifests, ownership
  records, node leases and heartbeats (no new heartbeat word), history,
  the alarm index and intents are read and written as by 0.8.0. A backup
  archive's actor entries can gain a `collected` list, which 0.8.0
  ignores: it verifies and restores an archive made by 0.8.1.
- **The WIT.** The package stays `wasmkeep:actor@0.2.0`, and only its
  comments changed. Actors built with the 0.8.0 SDKs run as they are,
  with one exception: rebuild with the 0.8.1 SDK, and deploy, a Rust, Go
  or TypeScript actor that passes more than 16 requests to the batch
  helper (`fetch_all`, `FetchAll`, `fetchAll`), which could trap on an
  empty poll.
- **Calls between nodes.** What a node signs and sends is unchanged byte
  for byte, so 0.8.0 and 0.8.1 nodes verify each other's calls.
- **Ports, routes, exit codes and existing flags.**

What is new on the command line:

- `serve --sqlite-heap-bytes BYTES` (`[limits] sqlite-heap-bytes` in a
  [node file](../reference/node-file.md)): SQLite's heap cap for the
  process. The default is 4 GiB, as 0.8.0 had it fixed, and the least is
  64 MiB. Nothing to set unless you want another cap.
- `backup create --history-retention-secs SECS` (default 3600): give it
  the value the cluster's nodes run with as `serve
  --history-retention-secs`, if that is not the default
  ([backup and restore](backup-and-restore.md)).

What to check after the upgrade:

- **Queries with `LIKE` or `GLOB` on an indexed column.** `GLOB` on a
  column with the default (`BINARY`) collation and `LIKE` on a `COLLATE
  NOCASE` column no longer use the index for a prefix: write a range
  instead (`path >= 'a/' AND path < 'a0'` for `path GLOB 'a/*'`,
  `name >= 'a' AND name < 'b'` for `name LIKE 'a%'`).
- **Dashboards on `wasmkeep_signatures_bad_total`.** It now counts a bad
  signature once per request, and on the cluster port too.
- **Alerts on `wasmkeep_plain_events_dropped_total`.** A write dropped
  after the node's lease was replaced counts as `lost_lease`, where it
  counted as `lapse` or `stop`.
- **A systemd unit copied from the single-node guide.** Optional: add the
  new hardening lines ([single node](single-node.md)).

### The binary

Upgrade the binary as [Upgrading WasmKeep](../cluster-upgrades.md#upgrading-wasmkeep)
says: **stop every node, upgrade them all, start them all**. 0.8.1
documents no exception for one node at a time, so plan a short window in
which the cluster serves nothing.

1. **Stop every node** cleanly (`systemctl stop wasmkeep`, or `SIGTERM`).
2. **Install** the 0.8.1 binary on each.
3. **Start every node.**
4. **Wait until each is `serving`**: `/_wasmkeep/ready` answers 200 on
   every node, and `cluster status` lists every node live, `serving`, with
   version 0.8.1.

   ```bash
   wasmkeep cluster status --bucket s3://wasmkeep-demo
   ```

### The upgrade floor

At start, a 0.8.1 node reads the app's upgrade floor,
`formats/<tenant>/<app>/floor`. Nothing writes that record yet: 0.9.0's
`cluster upgrade finalize` will, and without it a node starts as before.
If a 0.8.1 binary is started into an app finalized at a later minor
version, it does not serve: it stops, its
log names the floor and its own version, and until then it is not ready
with the reason `the app's upgrade floor is above this node's version, or
unreadable`. Install that version or later
([troubleshooting](../troubleshooting.md),
[ADR 0020](../adr/0020-one-lease-per-node.md#the-upgrade-floor-record)).
A 0.8.0 binary does not read the floor.
