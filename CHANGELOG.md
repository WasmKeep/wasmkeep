# Changelog

Changes to WasmKeep, newest first. Before 1.0, any minor version (0.x) may
change the `wasmkeep:actor` WIT world, the formats in the bucket or the
command line; its notes say what changed and how to move.

The text under a version's heading is its GitHub release text
(`.github/workflows/release.yml` copies it).

## Unreleased

### Fixed

- **`wasmkeep actor move` returns in well under a second.** It made its
  last check on a second runtime while the first still held the bucket's
  pooled connection, and that request waited for object_store's 30 s
  timeout: a move took 30 s or more against a bucket that keeps its
  connections open. `actor move` and `actor delete` now make every
  request on one runtime.
- **Parallel moves no longer warn "telling the nodes failed
  (precondition failed)".** The rewrite of `control/changed` has no
  condition, but a bucket may answer 409 to one of two writes of the same
  key at once. The CLI now writes it again after a short pause, up to
  three times, before it warns; the request itself was always written.
- **`wasmkeep cluster drain --process ID` works on a node started
  without a node file.** Such a node names no process in its heartbeat,
  so the drain was refused. When no live node names the process,
  `--process` now takes the live node whose id is `ID`; the same holds
  for `undrain`.

## 0.9.0

**0.9 can be upgraded one node at a time: a cluster moves from 0.8.1 to
0.9.0 without stopping as a whole, and one command ends the upgrade.
Stopping every node is still the recommended way for 0.9.0 (see Known
limits).** A node
refuses to start beside a version it cannot run with, each app has an
upgrade floor, and `wasmkeep cluster upgrade finalize` raises it once
every node runs the new version. Until then any node may go back to 0.8.1
and nothing 0.8.1 cannot read is written; after it there is no way back.
What 0.9.0 adds to the bucket waits for the finalize: a deploy with
`apart-from` or `spread` for your own class names, and the mark that
keeps `cluster forget` from deleting the lease of a node that just
started. Also new are `wasmkeep
cluster verify`, which checks that a bucket is whole and never writes,
and optional traces over OTLP. `wasmkeep cluster migrate` is removed, with
three forms only nodes older than 0.6.0 sent: 0.8.1 is the last version
that carries them. A backup of a finalized app needs 0.9 to be read, so
upgrade backup jobs with the cluster. The steps, and what is not built
yet, are in [upgrading to 0.9.0](docs/deploy/upgrading-to-0.9.md). Still
a **pre-alpha pre-release**.

### Added

- **`wasmkeep cluster upgrade status` and `finalize`** (ADR 0043).
  `status` shows each node's version, each app's upgrade floor and the
  state of its upgrade. `finalize --yes` ends an upgrade: it raises the
  app's floor to the new version once every live node runs it, and is
  refused, naming them, while a live node is older, or while an older
  node's lease is stale without a release or a fence (run `wasmkeep
  cluster fence` first). After it, no node of an older version starts.
  `cluster status` shows the floor and `finalize pending` in one line
  per app
  ([upgrading WasmKeep](docs/cluster-upgrades.md#versions-the-upgrade-floor-and-finalize)).
- **A node refuses to start beside a version it cannot run with.** A
  node of minor version N starts only while every live node of its app
  runs N-1, N or N+1, and names the node that stops it. It also refuses
  an app whose previous upgrade was never finalized, and, with every
  node stopped, an app whose node leases say it last ran a version
  below N-1 (a cluster stopped on 0.7 does not start on 0.9: install
  the last 0.8 release first).
- **A new app is born finalized.** The first deploy of an app that
  never ran, or the first node of an app served without a deploy,
  creates its upgrade floor at its own version. An app that already has
  a deploy, state or a registered node keeps the floor it has until
  `finalize`. A deploy never sets the floor above the lowest version
  live under the bucket prefix, and a node restarted in place on a
  newer version creates none.
- **`apart-from` and `spread` for your own class names.** Placement
  keeps a room and its pusher on different nodes and spreads a room's
  edges, for classes named `room`, `pusher` and `edge`. A class in
  `wasmkeep.toml` can now ask for the same: `apart-from = "<class>"`
  keeps its actor and that class's actor of the same name apart, and
  `spread = true` spreads its actors over the nodes. `false` turns the
  default off. A version with either key is manifest format 8: a deploy
  is refused until the app's upgrade to 0.9 is finalized, and while a
  live node is older or does not announce `apart`. Without the keys a
  version keeps the format nodes of 0.8.1 read
  ([project file](docs/project-file.md#front-and-classes),
  [ADR 0041, amendment](docs/adr/0041-placement-at-wake-up.md#amendment-2026-11-25-apart-from-and-spread-for-an-apps-own-class-names)).
- **A backup carries the app's upgrade floor, and a restore keeps it.**
  `backup create` stores the floor of an app that has one. A whole-app
  restore leaves the higher of the target's floor and the backup's,
  written before any state or deploy, so a node of an older version
  started on the restored app stops at the floor
  ([backup and restore](docs/deploy/backup-and-restore.md#the-upgrade-floor-and-the-deploys-format)).
- **`wasmkeep cluster verify`** checks that a bucket is whole. It only
  reads: no write is ever sent. Per app it checks `current`, the
  manifest and each component's digest. Per actor it checks that a
  restore plan exists from a snapshot through every segment to the
  head, that the ownership record decodes, and, in an app that records
  heads, that the recorded head is reached. It also checks alarm index
  entries, file records, node leases and the upgrade floor, and counts
  the per-actor lease markers left. `--deep` reads every history object
  and checks its checksums. `--app`, `--actor` and `--sample N` narrow
  it; `--json` is for scripts. It prints objects read, requests and
  time, and exits with 1 naming the key of each problem. What the
  bucket refused to give is listed apart and exits with 2: no verdict.
  It is not `backup verify` (a backup's own contents) nor `cluster
  diagnose` (what the bucket's API does)
  ([verifying a bucket](docs/cluster-operations.md#verifying-a-bucket)).
- **Traces** (ADR 0044): optional OTLP/HTTP export (`--otlp-endpoint`)
  of spans for requests, fronts, actor events, `actors.call`, hops
  between nodes and bucket requests. A request that crosses nodes is one
  trace: the first node decides whether it is sampled and the others
  follow it, when they have tracing on too. Run every node with the same
  tracing settings. Off by default; with it off nothing changes on the
  wire. Hops are signed as in 0.8.1
  ([monitoring](docs/monitoring.md#traces)).

### Changed

- **Commands that write are refused from a `wasmkeep` older than the
  app's upgrade floor**, and while a live node of the app is more than
  one minor version away: `deploy`, `secrets`, `app delete` (without
  `--force`) and `app format --enable`. A deploy is also refused while
  the app's floor record is unreadable. Each refusal says which
  `wasmkeep` may write: one within one minor version of every live
  node, and not older than the floor. One check now answers "does every
  live node announce this word" for every writer, the CLI included; the
  messages are unchanged.
- **Read-only commands warn about a live node outside their range and
  go on**: `cluster status` and `actor where` now print the line that
  `cluster upgrade status` prints (`actor where` on standard error).
- **A whole-app restore is checked like a deploy** (ADR 0043). Every
  version it installs goes through the deploy's gate, on the floor the
  restore leaves, before anything is written. It is refused from a
  `wasmkeep` older than the floor, and, when it has to raise the floor,
  while an older node of the app is registered and not stopped for
  sure (fence it, or forget a heartbeat from before node leases). It
  warns, naming them, when live nodes of other apps under the prefix
  are older than the floor: they cannot serve the restored app.
- **A backup run that carries a floor is backup format 2. Upgrade
  backup jobs to 0.9 before the first 0.9 backup of a finalized app.**
  A `wasmkeep` of 0.8 refuses such a run (`unknown or inconsistent
  backup format`) instead of restoring the app without its floor: for
  `backup verify` and `restore`, and for `create` and `prune` on an
  archive whose latest or kept run is one. A refused 0.8 `create`
  leaves a `runs/<id>/started` object that blocks `prune` for 24 hours;
  [backup and restore](docs/deploy/backup-and-restore.md#online-archives)
  says how to clear it. `create` says when an archive starts to need
  0.9. A run in which no app has a floor stays format 1.
- **`wasmkeep cluster forget` no longer deletes the lease of a node
  that just started under the same id**, once the app's upgrade to 0.9
  is finalized. It replaces the released lease with a mark, by a write
  that fails if the lease changed, then deletes the mark. A node that
  reads the mark writes nothing over it and waits for it to be gone. If
  the command stopped between its two steps, the node does not start
  after 60 s and says to run the command again; `cluster status` names
  such a mark, and so does `cluster verify`, as a `note:` line (`notes`
  in `--json`) that changes neither its verdict nor its exit code. A
  node whose start check was watching a released lease that is removed
  now checks its node id again and starts, where it stopped with `this
  node's lease object was deleted`. Before the finalize the command
  deletes as it did, and says so
  ([cluster](docs/cluster.md#how-it-works),
  [ADR 0043](docs/adr/0043-rolling-upgrades.md#built-the-removals-and-the-mark-of-cluster-forget)).
- **`wasmkeep_front_requests_total` no longer has `outcome="guest_error"`:
  it is `trap` or `no_response`.** `trap` is a front that trapped, or
  that did not handle a host call's error. `no_response` is one that
  ended without an answer the node could send: it returned with none,
  or answered an error, a `1xx` status or a broken hand-off. Change
  dashboards and alerts that select `guest_error`; the `reason` of the
  node's `front request failed` log lines changes the same way
  ([monitoring](docs/monitoring.md)).
- **A front that keeps failing no longer fills the node's log.** The
  WARN line of a failed front request (`front request failed`, and `no
  instance free`) is logged for the first 10 requests per reason in 60
  s; one line then says how many more were suppressed. The error on a
  line is cut at 2 KiB. `wasmkeep_front_requests_total` still counts
  every request.

### Removed

- **`wasmkeep cluster migrate` is removed** (ADR 0022), with
  `--rollback` and `--remove-old`. A cluster older than 0.4.0 upgrades
  through 0.8.x first: pin the 0.8.1 binary, migrate with it, then
  upgrade ([upgrading to 0.4.0](docs/deploy/upgrading-to-0.4.md)). A
  bucket migrated earlier keeps working: nodes and commands still read
  its commits and tombstones. If its old keys were never removed, run
  `cluster migrate --remove-old --apply` with the 0.8.1 binary while the
  cluster is still on 0.8.1, before upgrading: 0.9.0 cannot remove them.
  Nodes and commands still refuse a tenant left in the old layout, or a
  prefix a 0.8.x migration holds, and now say to use 0.8.1.
- **Nodes no longer accept what only versions before 0.6.0 sent.** A
  signed call, or a forward's proof, that names no sender is refused
  with 503. The forward proof's older form of the client address is not
  read. A hello answered 404 counts as failed and is sent again. Nodes
  of 0.8.1 send none of these, so an upgrade from 0.8.1 is not affected
  ([ADR 0038, amendment](docs/adr/0038-several-apps-per-process.md#amendment-2026-11-25-the-older-forms-are-no-longer-accepted)).

### Fixed

- **An idle compaction chain survives a slow release.** A pass stopped
  at its deadline whose release did not land within a second ended the
  chain: the release was retried in the background, but nothing queued
  the next pass, and a large actor kept part of its history until it
  next wrote and rested. The continuation now stays armed while the
  release is retried, and the retry queues it when it lands, on the
  record that release wrote. A retry that finds the record taken since, a
  lapse of the node's lease, or any acquisition of the actor on the node
  clears it. Such a pass is counted once its release ends, `continued`
  or `stopped`; the outcomes of `wasmkeep_idle_compactions_total` are
  unchanged. A release that landed with its answer lost (a try cut off
  at its one-second bound after its write went through) comes back
  superseded; the record is then read back once, and if it names nobody
  at the pass's generation, which only that release writes, the chain
  continues from it
  ([ADR 0042](docs/adr/0042-compacting-an-idle-actors-history.md), "As
  built: a release retried in the background").
- **A restore's read of a history object, and an upload's read-back,
  are bounded.** They keep no more of a body than the size its answer
  declared, and read again one that goes past it or ends short of it (a
  read-back takes a longer one for other bytes at once). A long body
  was read without a bound, and a short one was taken as the object.

### Known limits

- **Edges are not rebalanced after a rolling upgrade.** After an upgrade
  one node at a time under load, a big room's edges stay on the nodes
  upgraded first, and the latency goal after the upgrade was not met.
  Stopping every node is recommended for 0.9.0; rebalancing is planned
  for 0.10.0
  ([measured](docs/deploy/upgrading-to-0.9.md#measured)).

## 0.8.1

**0.8 settled: a security fix, the follow-ups 0.8.0 left open, and no
format change.** A `LIKE` or `GLOB` pattern full of wildcards could stop a
node of 0.7.0 or 0.8.0; that is fixed, and every cluster on those versions
should upgrade. Lease reads no longer trust an older copy of a node's
lease, a slow or failing bucket read keeps fewer requests waiting, a backup
with `--history all` completes when it races a collection, and SQLite's
memory cap is a setting. Nothing in the bucket, the `wasmkeep:actor@0.2.0`
package or the signatures between nodes changes, and actors built with
the 0.8.0 SDKs keep running (one SDK fix needs a rebuild, below). A node
also reads, at start, the upgrade floor that 0.9.0 will write. Upgrade as
for any release, every node stopped: the steps are in
[upgrading from 0.8.0 to 0.8.1](docs/deploy/upgrading-to-0.8.md#from-080-to-081).
Still a **pre-alpha pre-release**.

### Security

- **A `LIKE` or `GLOB` pattern could stop a node (0.7.0 and 0.8.0).**
  SQLite matches these patterns by recursion, one level per wildcard. A
  wildcard pattern of tens of kilobytes (`%a%a...`; the test uses 50,000
  bytes), in a statement an actor runs, overflowed the thread's stack, and
  the node's process aborted, with every app it serves. Any actor's code
  could do it, and so could a client of an app that passes client input as
  a `LIKE` or `GLOB` pattern. The other nodes take the actors over as
  after any crash. 0.8.1
  matches both without recursion, in the host, as it already did `instr`,
  `replace` and `trim`: SQLite's answers, stopped at the statement's time
  and CPU budgets. The offline connections that verify backups, exports and
  restores do the same, where a guest's index on such a pattern aborted the
  run. As a second barrier, `serve` and `dev` run on 8 MiB thread stacks.
  Upgrade to 0.8.1 ([security](docs/security.md#layers)).

### Changes

- **`LIKE` and `GLOB` no longer narrow an indexed search to a prefix.**
  This is the cost of the fix above, as for any replaced `like()`. It
  changes two cases, which now scan the table: `GLOB` on a `BINARY` column
  (write `path >= 'a/' AND path < 'a0'` for `path GLOB 'a/*'`) and `LIKE`
  on a `COLLATE NOCASE` column (write `name >= 'a' AND name < 'b'` for
  `name LIKE 'a%'`). `LIKE` on any other column never used an index.
  Check the queries of actors with large tables.
- **SQLite's memory cap is a setting.** `serve --sqlite-heap-bytes BYTES`,
  or `sqlite-heap-bytes` in a node file's `[limits]`, sets SQLite's
  process-wide heap cap: 4 GiB by default, as in 0.8.0, and at least
  64 MiB. Like the other flags a node file describes, the flag is refused
  beside `--node-file`
  ([node file](docs/reference/node-file.md)).
- **Guest objects named `_wasmkeep_` from before 0.8 are kept.** 0.8.0
  refuses a guest schema change that names the reserved prefix, but a
  database could already hold such objects. At open they are now logged
  once and left as they are, closed to the guest's schema changes; the
  actor opens and nothing is dropped or renamed. Offline verification no
  longer refuses them, only an object holding a host table's own name, so
  backups and exports of such actors verify.
- **Streamed file reads are capped and closed.** An instance holds at most
  16 streamed file reads with bytes left at once (past it, `not-allowed`;
  a stream dropped or read to its end no longer counts), and each closes
  its file or response when its event ends, where a resident actor kept
  the descriptor open. A wait for a read that the guest gives up no longer
  loses bytes. The WIT documents these caps, and `actors.call`'s header,
  size and chain limits; only its comments changed
  ([files](docs/files.md#limits)).
- **SDKs: a batch of more than 16 fetches no longer traps.** `fetch_all`
  (Rust), `FetchAll` (Go) and `fetchAll` (TypeScript) start what fits in
  the host's 16 open requests and queue the rest. When every open request
  finished while others were still queued, the helper polled an empty
  list and the actor trapped (`empty poll list`; the caller got a 500).
  It now starts the queued requests instead. The fix is in the SDKs:
  rebuild with the 0.8.1 SDK, and deploy, any actor that passes more than
  16 requests to the batch helper.
- **Lease reads never trust an older copy.** A node lease read to judge an
  actor's ownership record free is never older than a body of that lease
  the process has already read. An older one (a stale replica or a cache)
  is read again, up to three reads in all, and never frees the record;
  if all three are older, the request is answered as unavailable, to be
  retried. Counted by
  `wasmkeep_lease_reads_behind_total`, `wasmkeep_lease_reads_stale_total`
  and `wasmkeep_lease_reads_unpinned_total`. A fence `cluster fence`
  created over a deleted node lease counts as newer than every body of
  that lease dated less than twice the allowed clock skew (2 s) after it,
  and a node starting over one writes its first body no earlier than
  that, saying so in its log. `cluster fence` dates by the bucket's time
  any created fence it leaves behind, also one it found already there,
  and warns when this machine's clock is ahead of the bucket's. A read
  that finds a node lease absent restarts the window after which an older
  body replaces the newest held. Under stale reads, a node of 0.8.0 that
  starts right as such a fence lands may still have the actor it holds
  taken by a node of 0.8.1
  ([what remains](docs/adr/0020-one-lease-per-node.md#nodes-of-080-beside-this-version)).
  No format changes
  ([ADR 0020](docs/adr/0020-one-lease-per-node.md#amendment-of-2026-11-24-lease-reads-and-the-upgrade-floor)).
- **A node reads the app's upgrade floor at start.** The record,
  `formats/<tenant>/<app>/floor`, will be written from 0.9.0 by `cluster
  upgrade finalize`; nothing writes it yet, and a node starts as before
  without it. A node reads it before its first lease write and again after
  it: a floor above its minor version, or an unreadable one, stops the
  start with a message that says so, and a read with no answer is retried.
  So a 0.8.1 binary started into an app finalized at 0.9 stops instead of
  serving
  ([ADR 0020](docs/adr/0020-one-lease-per-node.md#the-upgrade-floor-record),
  [troubleshooting](docs/troubleshooting.md)).
- **Cluster availability after a slow bucket.** A request kept for a dead
  node's actors is no longer kept for a peer that only could not be read,
  or for a body older than one already read. A round has at most 32
  heartbeat reads in flight, so a hung read delays no other node's. A
  node whose object is gone leaves the live senders at once and is
  forgotten a minute later. An app joining a process reads the known nodes
  by key instead of waiting for a slow listing; over TLS, a joining node
  is read by the name on its certificate, so it no longer waits for a
  listing either; and the start hellos are not spent before a live peer of
  the app is found. No lease rule or format changes
  ([ADR 0020](docs/adr/0020-one-lease-per-node.md#amendment-of-2026-11-24-availability)).
- **Calls between nodes: one signature check per request.** A node call's
  signature (`x-wasmkeep-internal`) is checked once on arrival, by the
  runtime that serves the request, and every later check of that request
  reads the result (a hop refused there stays refused). One code path
  signs every hop. What is signed and sent is unchanged byte for byte
  (pinned by tests), so 0.8.0 and 0.8.1 nodes verify each other.
  `wasmkeep_signatures_bad_total` counts a bad signature once per request:
  on the cluster port now too, and, when plain HTTP between nodes is
  accepted, junk in that header on the public port; expect it to count
  differently on a dashboard. A request to a front path carrying the
  header is now judged on arrival like any other: in refuse mode a valid
  signature there is spent on the public port (never honoured on the
  cluster port), and in accept mode a bad one is counted and logged
  ([monitoring](docs/monitoring.md)).
- **Backups: `backup create --history all` completes when it races a
  collection.** A continuous run that raced a history collection or an
  idle pass failed as partial, and the next run completed. Now an actor is
  listed again after a read misses. A key gone from below a retained
  snapshot and its segment, and older than the retention window, is
  recorded under the actor's new `collected` list and is no longer a gap;
  a key still listed whose read missed is read again, up to 8 attempts;
  any other key gone is a gap, as before. Give the window the cluster
  runs with as `--history-retention-secs` (default 3600, as on `serve`).
  Nothing else in the archive changes, and 0.8.0 reads an archive with the
  new list. New simulations cover a backup reader racing idle passes and
  an acquirer proving the recorded head after a pass
  ([backup and restore](docs/deploy/backup-and-restore.md),
  [ADR 0042](docs/adr/0042-compacting-an-idle-actors-history.md#as-built-the-backup-retry-and-the-last-simulations-081)).
- **Idle compaction finishes large histories.** An idle pass stopped at
  its 5 s deadline is now continued by further passes, 5 s apart, until
  the actor's history is compact (an actor with thousands of objects
  used to lose one batch of 100 and keep the rest until it next wrote,
  and the pass counted `done`). `wasmkeep_idle_compactions_total{outcome}`
  gains `continued` and `stopped`, and `done` now means nothing old
  enough was left. Each pass raises the record's generation, so on a
  version 2 app an actor compacted over several passes is refused by
  `--require-owner-match` until it next writes, as after two wakes that
  only read. A chain also ends (`stopped`) when a pass's release does not
  land within a second: the release is retried in the background, but the
  continuation is not queued again, and what is left stays until the
  actor next writes and rests for a window
  ([ADR 0042](docs/adr/0042-compacting-an-idle-actors-history.md), "As
  built: continuing a stopped pass").
- **`lost_lease` is counted.** A batched write dropped because another
  writer replaced the node's lease (a fence, a twin), or by the stop that
  follows, now counts as `lost_lease` in
  `wasmkeep_plain_events_dropped_total`, where it counted as `lapse` or
  `stop`; the label set is unchanged. An actor that traps on its full
  resource table logs a line of its own, as the front does
  ([ADR 0034](docs/adr/0034-batched-plain-writes.md)).
- **Hardening of the documented deployments.** The single-node guide's
  systemd unit drops every capability, filters system calls to
  `@system-service`, limits address families and writes only the data and
  cache directories: add the new lines to an existing unit
  ([single node](docs/deploy/single-node.md)). The compose demo's bucket
  key and Grafana password come from `WASMKEEP_DEMO_*` variables (a `.env`
  file), and their public defaults are marked insecure.
- **Releases** (for whoever cuts them). A release tag must be a signed tag
  GitHub verifies. The mixed-version check, three nodes moved one at a
  time from the previous release to this one and back, is a gate before a
  patch tag: it takes the old version as a parameter (0.8.0 by default),
  and its checks and the move back count in its exit status
  ([releasing](docs/releasing.md#the-mixed-version-check)).

## 0.8.0

**A big room that needs no hand placement, idle histories compacted, and
a hardened node.** A room and its pusher wake on different nodes by
default, and a big room's edges wake spread over the nodes, so the
operator no longer moves the pusher by hand. An actor that stops writing
has its history compacted to a snapshot and the write at its head, a
retention window after it rests, so the change feed's listing stops
growing with it. A quiet room's first publish after a full hibernation is
pushed instead of fetched by every client. An internal audit of the guest
boundary, the cluster's signatures, uploads and local files led to
tighter limits and stricter checks, several of which you must act on when
you upgrade (below). Still a **pre-alpha pre-release**.

**Big rooms with default placement.** On three 4-vCPU nodes and a real
bucket, with default placement and no `actor move`, 4,500 clients at 10
messages/s got delivery p99 under 1 s (894-978 ms across the generators),
every message acknowledged and none lost or repeated, and the layout
(the room and its pusher apart, edges on all three nodes) was kept
through a rolling restart of the nodes. 6,000 clients missed narrowly
(p99 0.99-1.04 s)
([measured](docs/benchmarks.md#a-big-room-on-three-nodes-080),
[results](bench/results/2026-11-23-28f83b9-room-group-ovh-bhs-3vps-r08-default-placement.json)).
The margin at 4,500 is smaller than with 0.7.0's hand layout (see the
known limitations).

### Act on these when you upgrade

These change what a node accepts or does at start, or what an actor or a
client sees. The steps are in
[upgrading to 0.8.0](docs/deploy/upgrading-to-0.8.md).

- **`--env` files must be private.** On Unix, `--env` and `--front-env`
  files, and `wasmkeep dev`'s `.env.local`, are refused at start if other
  users can open them, if their group can write them, or if their group can
  read them while the server's user owns them (before, a world-readable file
  only logged a warning). Use `chmod 600 FILE` for a file the service owns,
  or `chown root:wasmkeep FILE && chmod 640 FILE`. `--config` files are not
  affected.
- **Guest output is logged in a new format.** What guests print is the last
  field of its line, `line="..."`, quoted, after a fixed `guest output`
  message (before, it was the event message). Update log filters that match
  it. Escapes follow logfmt and now cover format characters and line
  separators; actor names, event errors and traps are quoted the same way.
  The 4 KB limit applies after escaping, and output is limited to 1,000
  lines a second per app and 5,000 per process, beyond each instance's
  `--guest-log-rate`.
- **`actors.call` carries less.** At most 96 headers and 62 KiB of path and
  headers (was 256 headers and 64 KiB), and a call chain of at most 16
  actors; past them a call fails (`too-large`, `failed`). Check actors that
  forward many headers or call deeply.
- **A request head over 64 KiB or 100 header fields gets `431`.** Node
  headers are not counted; both ports leave 24 KiB and 32 fields for them.
  Check a proxy that adds large cookies or many headers.
- **The metrics listener checks `Host`.** It answers `421` unless the host is
  an IP address, `localhost` or a name under it, a name given with the new
  `--metrics-host` (`[listen] metrics-hosts`), or, when not bound to
  loopback, a single-label name. A scraper that names a node by a dotted
  host name needs `--metrics-host`. A stalled scraper is let go after 10 s.
- **A stalled client's socket is closed.** A response write that waits 60 s
  with no byte taken by the client closes the connection; a WebSocket closed
  this way ends with `1006`. Kept-alive connections and idle WebSockets are
  not affected. Clients that stop reading must reconnect.
- **The component cache directory is checked.** `--cache-dir` loads an
  artefact only if the directory and the file belong to the server's user,
  are not group- or world-writable, and are not reached through a symlink;
  otherwise the component is compiled again and the command that fixes the
  directory is logged at start. New cache directories are created 0700 and
  artefacts 0600; existing artefacts others can read are made 0600 when
  loaded. Run `chmod 700` on the cache and data directories of an existing
  install.
- **Data directories are owner-only.** `serve` and `dev` create a missing
  data directory 0700 and warn when an existing one is readable by others;
  CLI scratch directories that hold actor data (backup, restore, actor read
  and sql, inspect, the feed) are created 0700. The single-node guide sets
  `UMask=0077`.
- **Changing the secrets key has a second step.** After a key change,
  `wasmkeep secrets rotate-key --finish` deletes the sealed objects another
  key sealed (keeping the one before `current` for 30 s after it moved, and
  objects above `current` for the one-hour retention window), and fails
  while any are left. Changing the key does not change the values: rotate
  values the old key could open at their source.
- **File uploads are budgeted.** Files being written by streamed writes and
  upload links are spooled within 1 GiB per app (at least one file of
  `--max-blob-bytes`, at most half the process's) and 4 GiB per process (at
  least two such files). An upload link holds its whole size before it reads
  its body and answers `503` past a budget; a streamed write fails with
  `unavailable`. An upload body that sends nothing for 60 s, or averages
  under 16 KiB/s after its first 30 s, is aborted with `408`; the same link
  sent again while its first body is arriving answers `409`. An instance has
  at most 16 streamed writes open, and one left open goes when its event
  ends. Clients must retry `503`, and uploaders must keep a steady rate.
- **Tighter SQLite limits.** A statement may compile to at most 2,000,000
  instructions (`quota-exceeded` past it), SQLite's heap is capped at 4 GiB
  per process (temporary data included, as it now stays in memory), and
  `instr`, `replace` and `trim` stop at the statement's budgets. Guest schema
  changes may not name the reserved `_wasmkeep_` prefix. Check actors with
  very large sorts or temporary data.
- **Rebuild StarlingMonkey actors.** A guest now holds at most 4,096 host
  resources (handles) at once; past that its event fails and the actor's
  instance is replaced. The TypeScript SDK now releases each request's handles
  once it is answered, and the TypeScript, Go, C, Zig and MoonBit SDKs release
  a broken stream's error. A StarlingMonkey actor built with an earlier SDK
  leaks handles and fails one request about every 2,000: rebuild it with
  this version's SDK and deploy it.
- **Manual releases are restricted.** The release workflow run by hand starts
  only on a `vX.Y.Z` tag or on `main`; any other ref is refused. Tag pushes
  and pull requests are unchanged. This concerns whoever cuts releases, not
  operators of a node.

### What is new

**A snapshot at the head when an actor hibernates.** In a cluster, an
actor that leaves memory because it was idle or evicted first uploads a
snapshot at its last write, if its node wrote that write and at least 8
writes were made since the last snapshot (one is enough for a database
of 256 KiB or less, and never above 64 MiB). Its next restore reads one
object, and the node then collects the history below it, as after a
periodic snapshot, within the retention window. It is one short attempt
that never fails anything, counted in
`wasmkeep_idle_snapshots_total{outcome}`: the first step of compacting an
idle actor's history (ADR 0042). `serve --no-idle-compaction`, or
`[process] idle-compaction = false` in a node file, turns it off.

**An idle actor's history is compacted a retention window after it
rests.** The node that released an actor after a write takes its record
again one retention window later, only if nothing changed it since, and
deletes what the window kept below the snapshot at the head, without
running or restoring the actor. An actor at rest then holds two objects,
and the change feed still sees it unchanged. A wake, a move, an expiry, a
drain, a deleted, restoring or recreated app, or a class with
`expire-after-idle` makes the pass compact nothing. Counted in
`wasmkeep_idle_compactions_total{outcome}`; the same switch turns it off.

**A room and its pusher wake on different nodes, by default.** In a
version with classes named `room` and `pusher` (the broadcast SDKs'
default names), a wake-up of `pusher/<name>`, which nobody holds, on the
node that holds `room/<name>` goes to another node, and the other way
round. This applies to a request, a call, an alarm and a drain's
hand-over. Nothing to configure, and nothing changes in the bucket.
`--no-placement` turns it off with the load rule.

With one node alive, or every other node draining, both wake where they
land: a wake-up is never refused. A wake-up sent apart that the peer did
not run (a `503` before the actor) runs here.

It moves nothing running, so a pair that lands together stays together
until one hibernates or is moved. With two nodes, a rolling restart
always leaves them together. Moving the pusher by hand is now the
fallback
([designing for hot actors](docs/designing-for-hot-actors.md#place-a-large-rooms-pusher-on-another-node),
[ADR 0041, amendment](docs/adr/0041-placement-at-wake-up.md#amendment-2026-11-22-keeping-a-room-and-its-pusher-apart)).

New metric `wasmkeep_placement_apart_total{outcome}`. A placed wake-up
refused by a node holding the actor's companion counts as
`outcome="apart"` in `wasmkeep_placements_received_total`, and does not
back that node off. An event sent on by a drain that the actor itself
answers with a `5xx`, or that traps, no longer marks the peer suspect.

Upgrading: nothing to deploy. Nodes of 0.7.0 keep what they wake and
acquire what is sent to them.

**A big room's edges wake spread over the nodes, by default.** In a
version with classes named `room`, `pusher` and `edge`, an edge nobody
holds, woken by a call or an alarm, wakes on the rendezvous choice for
its address over the live nodes (the waking node included) instead of
beside the pusher that calls it: one hop at most, never refused, and
with one node alive it wakes where it is called. A running edge never
moves for it, so clients reconnect no more than before. A drain releases
its node's edges, and each wakes where its first event takes it: a
reconnecting client's socket where that client landed, the pusher's next
push on its spread choice. A node that receives an event from another
node and finds another owner by a hint under a second old reads the
record again before answering "ownership is changing", and that answer
now names the owner it read (`x-wasmkeep-owner-seen`) so its sender does
not suspect it. `--no-placement` turns the spread off
([ADR 0040, section 14](docs/adr/0040-directory-broadcast-groups-and-the-reconnecting-room.md#14-edges-spread-over-the-nodes-amended-2026-11-22)).
New metric `wasmkeep_placement_spread_total{outcome}`. Nothing to
deploy or configure.

**A quiet room's first publish after a full hibernation is pushed, not
fetched by every client.** A pusher that woke with no positions pushed a
header alone, every woken edge told its sockets the new head with a
`ready`, and every client of the room fetched that one message over HTTP
at once. The pusher now keeps where every child stood when it last went
idle with all of them at the head (a plain write to its own SQLite, at
most one per burst, deleted while busy), and a cold start pulls from
there: the edges' sockets get the message in an `m` frame and no client
fetches. A position the room refuses, or none kept, gives the header
alone, as before
([ADR 0040, section 13](docs/adr/0040-directory-broadcast-groups-and-the-reconnecting-room.md#13-the-pusher-amended-2026-11-20)).
In both SDKs' edges
([section 5](docs/adr/0040-directory-broadcast-groups-and-the-reconnecting-room.md#5-the-edge)),
a client that joined a woken edge before its first push no longer gets a
`behind` after its `ready` with `n: null`, so it fetches history once,
not twice; and a head stored by `init`, `range` or `drain` before the
first push no longer places the sockets the edge kept at that head
without telling them: the first push places and announces them as it
does on a headless edge. Shared vectors (`wake-vectors.txt`) cover these
cases, `recover` as the first push and a socket gone when told. A wake of
an edge holding 500 sockets stays under the 50 ms CPU budget in Rust and
QuickJS (`wake_500_*`, StarlingMonkey in `--full`).

Upgrading: nothing changes on the wire, for clients, edges or the
room's `pull`; deploy the classes together as usual. A 0.7 pusher keeps
no position, so its first round stays header-only.

### Security and hardening

- **Wasm and wasi-http field limits.** A `wasi:http` `fields` a guest builds
  is at most 64 KiB. An outbound response fails past 64 KiB of head or of
  trailers (trailers were held to 16 KiB before). Writes to an outgoing body
  are at most 32 KiB at a time, so an unsent body holds at most 64 KiB in
  the host.
- **A tighter SQL sandbox for actor databases.** Host statements no longer
  run guest triggers or foreign key cascades, and have the statement
  deadline. The authorizer refuses actions it does not name, and the
  write-ahead log cap is also checked while a statement runs
  ([security](docs/security.md)).
- **Cluster signatures.** Signed calls arriving on the public port (refuse
  mode) are recorded apart from the cluster port's records, so they can no
  longer get the cluster port's calls, takes or alarm hand-offs refused; once
  that record is full, new ones are dropped and counted
  (`wasmkeep_signatures_public_dropped_total`). A node refuses signed calls
  dated before it started plus the allowed clock skew, so a call accepted
  before a restart cannot be accepted again; such calls, and calls past a full
  signature record, are answered `503` so the sender signs them again
  (`wasmkeep_signatures_before_start_total`). Call nonces no longer derive
  from the instance published in heartbeats.
- **Local files and links.** The history export creates its database 0600 at
  open, and presigned-link probe errors no longer include the signed URL.
  Without a bucket, the node-link key file is written
  whole and linked into place, so two starts on one data directory agree on
  one key, and a key file that is not 32 bytes is always refused.
- **Config `Debug` output** shows names only, never values.
- **Build and release supply chain.** Build scripts and workflows check the
  SHA-256 of what they download (rustup-init, the WASI adapter, TinyGo, LDC),
  install Python tools from hash-locked requirements, and pin Dockerfile
  syntax by digest. `cargo deny` also runs on every example's lockfile. The
  release's `sign` job attests build provenance for everything `SHA256SUMS`
  lists ([releasing](docs/releasing.md)).

### Fixed

- **A node no longer drops an app when it cannot read its local markers.** A
  local read error (too many open files under socket load, an I/O or
  permission error) was taken as a deleted app: the node stopped it, closed
  its sockets, stopped its actors without flushing and removed its local
  files; at start the same error removed the app's local files. Now only a
  missing file or other bytes count as a mismatch: any other error is retried
  while serving, and fails the start without removing anything. A node that
  keeps failing to read them is not ready until a check reads them again.
- **A rollback racing `wasmkeep app delete` is refused**, as a deploy already
  was, instead of reporting success for a deleted app. A rollback that cannot
  check for a delete made meanwhile says it switched `current` (to which
  version) and not to roll back again, and an assets deploy whose marker
  renewal fails says whether `current` was switched.
- **A blocked checkpoint no longer drops captured changes.** When the
  checkpoint after a capture could not run (a reader still on the write-ahead
  log), the changes already captured were lost and the event failed. They are
  now returned, later checkpoints do not wait for the reader at every event,
  and capture carries on across the log's restart.
- **A runtime restarted in place no longer stops itself** as a twin of its own
  predecessor.
- **A node restarted as a new process no longer stops itself** when its view,
  after its first write, shows the node lease that write replaced (its
  stopped predecessor's): kept by a sibling app's listing in a process serving
  several apps, or read from a bucket replica behind. That copy, or an earlier
  body of the same instance, is not a twin; any other instance, or a later body
  of that one, still stops the node.
- **A restart no longer leaves "bad, expired or misaddressed signature"
  warnings on its peers** when processes serve several apps: a node says hello
  to the peers of its own app only. Such refusals are now counted,
  `wasmkeep_signatures_bad_total`.
- CI: CodeQL also runs on push to `main`, and a speed-sensitive StarlingMonkey
  test moves to the full suite.

### Known limitations

- **Two nodes always co-locate the room and the pusher after a rolling
  restart.** Moving the pusher by hand is still the fallback
  ([designing for hot actors](docs/designing-for-hot-actors.md#place-a-large-rooms-pusher-on-another-node)).
- **The idle pass's queue is lost on restart.** Actors that rested within
  the last retention window before a restart keep their leftover history
  until they next write and rest.
- **A `--history all` backup that races an idle pass is refused once.**
  Run it again.
- **The margin at 4,500 clients is smaller than 0.7.0's hand layout.**
  Delivery p99 was 894-978 ms, against 644-763 ms with the pusher placed by
  hand, because the room's node now also runs about a third of the edges.
  6,000 clients is still the edge, just over the 1 s target.
- **Delivery is disturbed while nodes restart.** In the 4,500-client
  rolling-restart run, delivery p99 rose to 18.8-33.5 s during the
  restarts and 271 publishes failed; no client saw a gap or a repeat, and
  all 4,500 were back before the leave.
- **Not measured:** a killed node, and the apart and spread rules on two
  nodes.

## 0.7.0

**Big rooms, a cluster that keeps its peers on a slow bucket, and the
metrics to see why.** A room's delivery moves to a fifth class, the
pusher, so publishes and history no longer wait on delivery rounds; a
woken edge delivers to the sockets it kept, a pusher round is bounded in
work, and the room renders the browser's history page itself. A slow
bucket no longer makes nodes distrust healthy peers. New metrics show
where an actor's time goes, what an event and a front request cost, and
how long segment uploads and bucket requests take; the CLI gains
`wasmkeep actor where`, and `cluster forget` removes released node
leases. Change-feed listing runs in parallel, a restore waits on the
bucket's clock instead of two fixed periods, and a node returns unused
allocator pages once activity drops. Still a **pre-alpha pre-release**.

**Big rooms are no longer a preview.** In 0.6.0 they were, with no
supported client count: the room's publish path held delivery back. 0.7.0
takes delivery off that path, to the pusher. On three 4-vCPU nodes, 4,500
clients at 10 messages/s got delivery p99 under 1 s, with no message lost
or repeated, the room and its pusher on different nodes; 6,000 is the
measured edge, where p99 passed 1 s
([measured](bench/results/2026-11-22-43119bd-room-group-ovh-bhs-3vps-r07-room-apart.json)). Keeping them apart is the operator's for now (below).

What changes in behaviour: inside `on-alarm`, `get-alarm` returns none
until the handler sets or clears the alarm, and whatever it sets stands,
even the delivered time; `wasmkeep_event_duration_seconds` gains a `kind`
label; `serve` trims idle allocator memory by default and caps glibc's
arenas. The package stays `wasmkeep:actor@0.2.0`.

Upgrade the binary on every node together. A room deployed with 0.6.0
needs its five classes deployed together with the front, the pusher
batched; see [upgrading to 0.7.0](docs/deploy/upgrading-to-0.7.md).

Known limitations:

- **A large room's delivery tail depends on segment-upload latency from
  the room's node.** Uploads slower than `--max-unsent-age` hold the room
  and its pusher at the event-start rule, and delivery waits with them.
  `wasmkeep_segment_upload_seconds` shows it.
- **Placement does not keep a room and its pusher apart.** For a large
  room, keep them on different nodes with `wasmkeep actor move`, and
  check with `wasmkeep actor where`
  ([how](docs/designing-for-hot-actors.md#place-a-large-rooms-pusher-on-another-node)).
  A move does not pin: once idle, the pusher's next event may place it
  anywhere, so check again after a restart or a deploy.
- **With the default placement the tail is longer.** Where the room and
  its pusher share a node, delivery waits on that node's uploads for
  both; the figures above are with them apart.
- **At high client counts the pusher becomes the busiest actor**: it
  runs every delivery round of the room's group.
- **Released node leases accumulate on a reused prefix** until removed
  with `wasmkeep cluster forget <node>`.
- **`cluster forget` has no conditional delete.** A store's DELETE takes
  no condition, so the command reads the lease twice and deletes it only
  if it did not change; a process starting under that id whose first
  write lands between the second read and the delete loses its lease,
  exits and is restarted by its service manager: availability only,
  nothing is taken from another node ([cluster](docs/cluster.md)).

- **Segment upload metrics** tell a slow bucket from a slow node:
  `wasmkeep_segment_upload_seconds` (first PUT to acknowledgement),
  `wasmkeep_segment_upload_retries_total` and
  `wasmkeep_segment_upload_failures_total`, next to the existing
  `wasmkeep_segment_bytes` and the event-start rule's wait. No actor
  labels. Two more tell the two apart across a node:
  `wasmkeep_bucket_request_seconds{operation,status}` times every bucket
  request, and `wasmkeep_runtime_schedule_lag_seconds` records how late the
  async runtime wakes a task that sleeps 100 ms.
- **`wasmkeep actor where`: which node an actor runs on.** It reads the
  ownership record of each actor it names and the heartbeats, and prints
  the owning node, whether that node is live and since when it has owned
  the actor (`not placed` for an idle actor with no owner), in columns or
  with `--json`. Read-only: nothing is written, claimed or woken.
- **Docs: where to place a big room's pusher.** For a large room, keep
  the room actor and its pusher on different nodes: together they delivered
  worse under heavy load, with a long tail of segment uploads on their node.
  [Designing for hot actors](docs/designing-for-hot-actors.md#place-a-large-rooms-pusher-on-another-node)
  gives the `actor move` command, says a move does not pin, and lists what
  to watch; placement does not yet keep them apart.

- **A slow bucket no longer makes nodes distrust healthy peers** (ADR 0020
  and ADR 0011, amendments of 2026-11-20). A node read its peers'
  heartbeats one after the other, with no time limit, and read
  `control/changed` after them in the same loop: on a slow bucket the
  newer heartbeats came too late, the trust in a peer ran out although it
  kept writing, and forwarding, calls and sockets to it failed with "the
  heartbeat naming it expired". Now the reads go out at once, each within
  a read attempt (2.67 s at the defaults); a read that fails or times out
  keeps that peer as last read, trusted until its own horizon and no
  longer, without dropping any pooled connection; a peer whose trust would
  end before the next round lands is read again early; and the intents
  are read on a task of their own. A heartbeat is judged by when it was
  written, as before; one dated ahead of the node's clock now counts from
  when the node first read it, so a dead peer whose clock ran fast is no
  longer trusted for that much longer. The 10 s horizon is unchanged. New
  metrics: `wasmkeep_heartbeat_read_milliseconds`,
  `wasmkeep_node_lease_write_milliseconds`,
  `wasmkeep_heartbeat_age_milliseconds`,
  `wasmkeep_heartbeat_read_failures_total` and
  `wasmkeep_heartbeat_early_reads_total`.
- **Listing `nodes/` no longer holds up the heartbeat reads** (ADR 0020,
  amendment of 2026-11-20). A node listed `nodes/` inline every ten
  periods, and after a failed listing every period, so a slow LIST
  stalled its reads. Listing now runs on a task of its own, for
  discovery only, with a longer limit and a backoff; every period reads
  the known nodes by key. Nodes whose lease is `released` or `fenced` are
  read only when listing, not every period: a prefix holding many leases
  left by earlier clusters costs nothing per period any more. A node that
  says hello, or sends a signed hop, before it was listed is read by key
  at once, so a join does not wait for a slow LIST (over TLS it still
  waits for the listing its refused handshake asks for).
- **`wasmkeep cluster forget` removes a released node lease** that no
  ownership record names and that is older than 25 s. It still refuses
  a live or fenced node lease, and a released one a record still names.
- **An edge woken from hibernation delivers its first push to the sockets
  it kept, and answers every ping** (ADR 0040, sections 5 and 6). The edge
  used to place those sockets at the push's `head` before applying it, so
  the messages of that push never reached them and nothing told them so:
  in a quiet room a client stayed stale until the next publish, and a
  ping got no `pong` until a push came. Both SDKs' edges now place them at
  `first - 1`, so the push's messages go out in frames that carry their
  own position, one frame per socket. A header-only first push, which has
  nothing to send, instead sends each a `ready` with the push's base,
  `{"t":"ready","e":E,"b":B,"n":first - 1}`; a `ping` before any push gets
  `{"t":"pong","e":0,"n":null}`. `RoomClient` fetches history after such a
  `ready` only when its position is past the client's own, applying `b`
  as for a frame of the next epoch, and takes a `pong` with no head as a
  keepalive; neither changes its state from `ready`. An older client
  fetches whenever the announced `e` or `n` differs from its own, which
  after a header-only wake (the room bumps the epoch when it wakes) is one
  request each. It also rejects a headless `pong` (`onError`) and
  reconnects: in a quiet room, whose edge hibernates between pings, about
  every 45 s, where it reconnected about every 55 s before (no `pong`).
  Shared Rust/TypeScript vectors (`wake-vectors.txt`) cover the wake.

- The QuickJS distinct-positions room budget test runs in `check.sh --full`
  again rather than in CI: its fuel is bounded, but its real 50 ms CPU budget
  still trapped once on a slow CI runner. The big-room fuel tests get the
  big-room group's four-minute limit.

- **`describeError` is exported from `@wasmkeep/sdk/front`**, and the big-room
  example's front logs with it: a WIT error now logs its payload instead of
  `Error: [object Object] (see error.payload)`.

- **A pusher round is bounded in work, not only in bytes.** A round over
  many children lagging at distinct positions read and pushed about 1,000
  rows (the 1 MiB budget at 1 KiB a row), which took about 455 M fuel on
  QuickJS and StarlingMonkey, close to the 500 M the 50 ms budget is
  calibrated for, and could trap at the CPU budget. A round now also
  counts its work, in counts a guest can keep where it cannot read its
  fuel: 8 a pull, 3 a request to a child, 1 a row read and 1 a row encoded
  into a push, at most 640 (`ROUND_WORK`, in both SDKs). Through relays a
  row counts once per slice, since the relays' packs are the only
  encoding: the round no longer encodes each child's push there. A pull
  is sized for the children at its position, so one child reads at most
  314 rows and 127 relayed edges at one position each get 125 rows a
  round (before, one or two of them got 314). The children the rest would
  need keep their positions for the next round 20 ms later, which starts
  at the first of them, so delivery stays in order and every child takes
  its turn. Children already at the head the round read are left for the
  next round without setting an alarm, so a group at its head still goes
  idle, unless one's last answer asked for a retry (lagging sockets),
  which it keeps; they keep their last socket count. On QuickJS the
  pusher's alarms now take at most about 215 M fuel: the
  distinct-positions test asserts them at 250 M and runs in every run and
  CI again, with two new tests of one position fanned out to 127 and 256
  edges. Its StarlingMonkey variant stays ignored: its rounds take about
  160 M, but the first alarm after the restart still takes about 430 M.
  At 256 edges the requests alone fill a round: a slice moves one row a
  round, to 210 of them. See ADR 0040, section 13.
- **Wasmtime 50.0.3**, which fixes RUSTSEC-2026-0587: a WASI filesystem
  sandbox escape on Windows through a drive-prefixed path. WasmKeep ships
  no Windows build; the patch release of the pinned version is taken anyway.
- **The CPU budget no longer charges a guest for other threads' time.**
  A short stretch of guest code between two host calls was timed with the
  wall clock, capped at two epoch ticks (2 ms). An event that makes many
  host calls, such as a batch of fetches, runs as many such stretches, and
  on an oversubscribed core each one the OS preempted was charged up to
  2 ms of other threads' time: an event that used a few milliseconds of
  CPU could be charged past its budget and answer 500 as a CPU-budget
  trap. Once one of an event's wall-clock stretches runs longer than a
  quarter of a tick, the rest of the event is now measured with the
  thread CPU clock. Later stretches of at most 50 µs use their wall
  time instead, saving the ending CPU-clock syscall while conservatively
  charging the guest. Every later stretch still reads CPU time at its
  start, so a long, preempted stretch is charged its actual CPU time;
  the meter stays switched until the event resets. An unloaded event
  with short stretches still reads only the wall clock. A guest that burns
  CPU still traps at its budget, and a
  tight loop is still interrupted at the next epoch tick.

- **The room renders the browser's history page.** A front answering
  `GET /history` decoded the room's binary page into objects and encoded
  it again in the guest, so a full page cost the front most of its CPU
  budget, or more: on a full window, a page of 256 messages of 200 B took
  245 M fuel on QuickJS and one of 61 messages of 4 KiB 937 M (500 M is
  what the 50 ms budget is calibrated for), and the request answered 500.
  The room has a new operation, `history-json`: the same page, rendered
  as the browser's JSON by one SQLite statement, byte for byte what
  `JSON.stringify(broadcast.historyJSON(history))` made (shared vectors
  and the unicode tests check every escape). Its pages hold at most 256
  messages and 64 KiB of bodies, at least one message, `more` when cut.
  A TypeScript front returns `room.historyJSON(after, limit)` unread
  (Rust: `Client::history_json`); its fuel per page no longer grows with
  the page: 1.1 M on QuickJS and 4.9 M on StarlingMonkey
  ([raw measurements](bench/results/2026-11-20-2e614d8-front-history-fuel.csv)).
  Against a room
  of an earlier release the TypeScript client converts the binary page
  as before. The binary `history` is unchanged. The example front
  accepts `limit` (clamped to 1..256). See ADR 0040, section 3.
- **The browser client keeps its socket on a 5xx history answer.**
  `RoomClient` threw on any history answer but a 429 or 503, which closed
  the socket; the client rejoined behind, asked for the same page, met
  the same 500, and since `ready` reset its reconnect attempts after
  10 s it did so at the fastest rate, forever. Now any 5xx is retried
  like a 503, the socket kept, after `Retry-After` plus full jitter; a
  5xx other than 503 also halves the page (`limit`, doubled back after
  each answer). Only a 4xx is an error. The 30 s deadline covers each request, not the
  waits between them. The attempt counter returns to 0 only once the
  connection has stayed ready for 10 s after its history caught up. See
  ADR 0040, section 6.
- **Deploy the pusher with batched writes.** Its per-round write is its
  alarm, and a strict pusher replicated every alarm move, and its alarm
  index entry, before its next round: a busy pusher spent most of its
  time there. Everything it must keep across a crash (its scale state,
  operation row and migration step) is committed with `exec-durable`,
  which waits for the upload in a batched class too, and child positions
  are never stored. The ts-big-room example and the room benchmark smoke
  now serve it batched; `room_cluster` crashes a batched pusher with its
  alarm moves unacknowledged and a scale operation under way. See
  ADR 0040, section 13.

  **Upgrading:** deploy the pusher class batched:
  `wasmkeep deploy --writes pusher=batched` (part of the version), or
  `writes = "batched"` under `[classes.pusher]` in `wasmkeep.toml`, or
  `serve --writes pusher=batched`. Deploy the front and the room
  together for `history-json`. A new TypeScript front against an older
  room still works, at the old cost; a Rust front's
  `Client::history_json` has no such fallback (an older room answers
  400), so a Rust front that uses it must be deployed with its room.

- **`on-alarm` keeps the alarm it sets, even for the delivered time.**
  After a successful `on-alarm` the host cleared an alarm still at the
  time it had delivered, so a handler that re-armed for exactly that time
  (a punctual "continue now") lost its alarm and stalled. Now whatever the
  handler sets or clears is its new alarm; only an alarm it left alone is
  cleared. Inside `on-alarm`, `get-alarm` now returns none until the
  handler sets or clears the alarm, then what it set; it used to return
  the alarm being delivered. A guest that saved `get-alarm` there and put
  it back now restores "no alarm" instead of the delivered time. A
  `set-alarm` rolled back with its transaction still counts for nothing.
  The broadcast SDKs keep their 1 ms re-arm and do not read `get-alarm`
  in `on-alarm`, since a component may still run on an older host. See
  ADR 0009, amendment of 2026-11-20, and the `alarms` and `on-alarm` docs
  in the WIT.
- **StarlingMonkey big-room OOM on one core fixed by the room/pusher
  split.** Both full-window variants now run in ordinary CI, retaining
  their fuel ceilings and the serialized `big-room` group, after five
  passes each on one core beside a CPU burner at the default 50 ms event
  budget and 128 MiB memory limit. Both distinct-positions variants remain
  `--full`-only: QuickJS passed the contention runs but hit a CPU-budget
  trap in the four-thread affected suite; StarlingMonkey timed out in its
  first contention run. The two TypeScript `room_edge` fanout tests also
  remain `--full`-only because their in-test Node build is load-sensitive.
- **Periodic snapshots of batched classes leave the mailbox.** A batched
  hold took its periodic snapshot inside the mailbox gap after an event:
  a flush to the barrier, the copy, then the upload, with every event
  queued behind two bucket round trips (and the upload's whole attempt,
  up to 10 s, on a slow bucket). Now the database is copied at the first
  gap with nothing in flight, at the acknowledged head, leaving later
  commits in the WAL (no flush), and the encoding, upload and collection
  run on a task of their own. A restore still sees either the previous
  snapshot and every segment after it, or the new one; collection waits
  for the new one to be acknowledged; barriers, resets and deletes wait
  for an upload under way, including one a hold given up on the same
  node left behind. Strict classes are unchanged. The mailbox's
  host work for it is `wasmkeep_mailbox_host_work_seconds{work="snapshot_copy"}`,
  the copy alone (it was `snapshot_in_gap`, the whole snapshot, earlier
  in this release). See ADR 0034, section 7, and
  [monitoring](docs/monitoring.md).
- **Front request work.** `wasmkeep_front_cpu_seconds{tenant,app}` and
  `wasmkeep_front_fuel{tenant,app}` report the guest CPU time each front
  request was charged against `--front-cpu-budget-ms`, and its Wasmtime
  fuel, from instantiation to the instance's end, trapped requests
  included. Equal fuel with more CPU time on one node points at the machine;
  more fuel, at bigger requests. Runtime embedders get the meter of a front
  store from `Runtime::new_metered_async_store_with_budget`.
- **The room's delivery moves to a pusher** (stage 1,
  [ADR 0040](docs/adr/0040-directory-broadcast-groups-and-the-reconnecting-room.md#13-the-pusher-amended-2026-11-20),
  section 13). A busy room spent most of its mailbox time waiting on its
  own delivery rounds, and publishes queued behind them. A fifth class,
  the **pusher** (`pusher/<room name>`), now runs the rounds, the
  children's positions and suspects, the relay layer, scaling and its
  durable state; it pulls from the room with a new read-only `pull`
  (the push header and up to 513 messages, bounded as a round was).
  The room keeps numbers, epochs, the window and history, sets no alarm
  and never calls the pusher. The pusher clocks itself every 20 ms while
  a child is behind; when it goes idle it tells the room in its last
  pull, and the publish answer then asks the front to poke it
  (`Client.publish` does, trying a failed poke up to three times). While
  its group holds sockets, an idle pusher always keeps one durable alarm
  at most 60 s out, so an acknowledged publish whose poke was lost (its
  node died, the call failed) is still delivered; an empty group keeps
  none. A round pulls at most 16 distinct positions and defers the rest.
  A join pokes the pusher, not the room. A round still waits for its slowest child, but only the
  pusher waits: publishes and history are answered meanwhile. Edges gain
  `handover`, directories `transfer`; the wire `Writer` of the
  TypeScript SDK writes into one buffer. TypeScript: `broadcast.define()`
  takes only `confirm` (and `pusher`, its class), and
  `broadcast.definePusher({ scaling | edges, room })` is the new class.
  Rust: `broadcast::pusher::handler::{round, scaling, handle}` replace
  `handler::{round, scaling, poke}` of the room. Both SDKs: `Core` takes
  no alarm (TypeScript `new Core(store, confirm)`; Rust's `Store` loses
  `alarm`), `beforePush`/`before_push` give way to `pull`, `Round.run`
  takes a `Source` (the pusher's `Pulled`, or a `Core`), and TypeScript
  exports `broadcast.serve` (the room's operations, for tests). A scale step continues
  1 ms after its event rather than at its own time, which a punctual
  alarm delivery could clear and stall.

  **Upgrading a room deployed with 0.6.0.** Deploy the five classes
  together (the `pusher` class `internal`, from the same SDK version as
  the room and the front: a 0.6.0 front refuses the publish answer's new
  field: the publish is numbered but the 0.6.0 front answers an error, and
  a retry without an `id` publishes it twice). The room keeps its history and numbers. On its first alarm the
  pusher reads the room's scale rows (`adopt`), saves them as its own,
  then has the room hand the directory map and every edge over to it
  (16 edges per alarm) and retire its rows; a pending split or merge
  resumes in the pusher. Clients stay connected and receive the backlog
  once the move is done, a few alarms later; nothing is renumbered. A
  room's leftover 0.6.0 alarm fires once and does nothing. Fixed-edge
  rooms (`edges`, a test configuration) are not moved: recreate their
  edges.

- **Guest event work.** `wasmkeep_event_fuel{tenant,app,class,kind}` reports
  Wasmtime work units per actor event, independently of CPU speed, alongside
  event duration. Fuel is a meter with an effectively unlimited allowance,
  reset per event; CPU-budget enforcement is unchanged. Runtime embedders
  can control instrumentation with `RuntimeConfig::measure_fuel` and read
  event work through `CpuMeter::fuel_used`. Counts exclude host work and can
  undercount trapped events; see [monitoring](docs/monitoring.md#event-work-fuel).
  Room tests now assert calibrated per-event fuel ceilings too; their
  speed-sensitive ignores remain while they retain the 50 ms CPU limit.
- **Allocator retention controls (Linux/glibc).** `serve --idle-memory-trim-secs`
  defaults to 60 seconds (0 disables it). It samples resident actors plus
  sockets in use and returns unused allocator pages when activity drops to
  at most half the peak since the last trim, or reaches zero. Trims run off
  the executor, are serialized across apps, and retain a minimum 60-second
  cooldown; steady busy activity does not trigger them. The global CLI flag
  `--malloc-arena-max N` caps arenas before worker threads start, defaulting
  to available parallelism clamped to 2..8 (fallback 2); 0 preserves glibc's
  default/environment policy. Both controls are no-ops elsewhere.
  `wasmkeep_allocator_trim_seconds` and
  `wasmkeep_allocator_trims_total{released="true|false"}` expose actual calls.
  Memory after a big room is not yet measured with them on several nodes.

- **One delivery per alarm round.** An event that ended while its actor's
  alarm delivery waited in the mailbox queued another delivery of the same
  round; every one after the first found the alarm moved and cleared it as
  a stale hint, in a cluster with a flush, a fence, a LIST and DELETEs
  inside the mailbox. A busy room spent most of its mailbox time there and
  its publishes waited behind it. Now an event's end leaves a delivery for
  the same time alone, a delivery the mailbox dropped (the actor failed to
  start, or the event stopped) is delivered again with backoff, up to six
  times in a row, instead of waiting for the next event or a sweep, a local redelivery of a time the
  actor's hold already ran skips the stale-hint clear, and that clear's
  DELETEs of tagged keys run off the mailbox.
  `wasmkeep_alarm_deliveries_total{tenant,app,class,outcome}` (`ran`,
  `stale`, `duplicate`, `deferred`) and
  `wasmkeep_alarm_dropped_deliveries_total` show it: stale over ran should
  be near zero ([monitoring](docs/monitoring.md#alarm-deliveries),
  [ADR 0009](docs/adr/0009-alarms-in-a-cluster.md), amendment of
  2026-11-19).

- **Where an actor's time goes.** Three histograms per app and class:
  `wasmkeep_mailbox_wait_seconds` (queued to started),
  `wasmkeep_event_start_rule_wait_seconds` (a batched actor's event
  waiting on the previous event's upload, ADR 0034) and
  `wasmkeep_event_duration_seconds`, which gains a `kind` label (`fetch`,
  `alarm`, `websocket`, `other`): a query on it without `kind` sums them
  as before. [Monitoring](docs/monitoring.md) says how to read a slow
  answer from them.
  `wasmkeep_mailbox_host_work_seconds{tenant,app,class,work}` now accounts
  for host work and jobs without guest events; `wasmkeep_alarm_stale_hints_total`
  counts stale alarm hint clear attempts per app and class.

- **Parallel change-feed listing.** `wasmkeep feed --list-concurrency N`
  (default 16) lists ordered ranges concurrently after the first poll,
  using saved heads to balance the ranges and temporary disk to bound
  memory. Actor/object order and the per-actor listing budget are unchanged.
  LIST costs remain approximately the same; the first poll stays sequential.
  See [the feed guide](docs/change-feed.md).
- **Shorter restores.** `backup restore` no longer waits two fixed 40 s
  periods. The quiet wait before it writes state is measured on the
  bucket's clock (`Last-Modified`), never a node's or the operator's. It
  waits 41 s after the bucket stored the newest write of the app's node
  lease objects (a renewal, release or fence), lists them again, and waits
  again if one changed, refusing after three rounds. It does not wait at all
  when the app has no node lease under the prefix. When the nodes listing
  proves nothing (an unreadable heartbeat, one naming no app, an old node,
  `--force`), it also waits 41 s after the fence was stored. The restore lock
  is released at once when every write to the target and archive got an
  answer. It is held 40 s after one that did not, counting the S3 client's
  own retries. `--abort` keeps its fixed waits.
  `docs/deploy/backup-and-restore.md` says what each wait guards against,
  and where the 40 s bound is an assumption the bucket client does not
  enforce.

## 0.6.0

**Several apps in one process, files, static assets and larger rooms.**
One process serves several apps behind its three ports (a node file);
actors and the front keep files; an app deploys static assets; an
operator streams an app's actors to Parquet with the change feed, and
inspects a bucket from a local page; a room scales past one actor with a
directory, a broadcast group and edge actors (a preview); a node places a wake-up on
a lighter node. Rust gets typed clients, and a React quickstart ties a
TypeScript actor to a Vite page. Still a **pre-alpha pre-release**.

What changes in behaviour: every command logs to stderr; a host answer
for a request that may have run carries `x-wasmkeep-outcome: unknown`;
`/_wasmkeep/ready` waits for the app's tombstone check; a hibernating
actor's state is dropped before it leaves the registry, so an event in
that window no longer builds a second live state. The package stays
`wasmkeep:actor@0.2.0`: `blobs` is an added interface, and a component
built without files imports nothing new and links as before. **Do not
downgrade a node below 0.6.0 once an app imports `blobs`.**

Upgrade node by node, with the 0.6.0 CLI: nodes announce `multi-app`,
`assets`, `blobs` and `place` in their heartbeat, and each feature waits
for every live node to have its word. Mixed versions and the order of
the two stages are in [upgrading to 0.6.0](docs/deploy/upgrading-to-0.6.md).

Measured on three OVH nodes and a real bucket
([benchmarks](docs/benchmarks.md)): no durable write lost in two 2-hour
soaks with node kills (464,219 strict, 470,272 batched); a returning node
takes its share in 9.3 s with placement on against 18.4 s off; no
benchmark moved more than 9% against 0.5.1.

Wasmtime moves to 50.0.2, which fixes eight advisories published on
2026-11-19 (RUSTSEC-2026-0566 to 0573), among them a guest able to abort
the host process through a stale GC heap pointer, and unbounded host
memory from empty writes to an output or HTTP/2 body stream.

Known limits:

- **Big rooms are a preview, with no supported client count.** On three
  nodes, 3,000 to 10,000 clients stay connected and the room no longer
  wedges, but the room's publish path holds delivery p99 at 5 to 15 s and
  acknowledges 2.4 to 4.3 of 10 messages/s
  ([measured](docs/benchmarks.md#a-big-room-on-three-nodes-preview)). The
  room's node keeps the memory it took, and its edges stay on one node.
  The preview at the default 50 ms budget assumes a reasonably fast CPU:
  on a slow shared runner the room's front went over its budget and
  answered 500.
- **On StarlingMonkey, a big room can run out of guest memory on a
  starved CPU.** With one core for the node, the room actor's heap grows
  until SpiderMonkey's garbage collector cannot allocate, and the room's
  instance is discarded; with two cores, or on QuickJS, the same test
  passes. Build big rooms for QuickJS (the default engine) for now.
- **Batched plain writes lose more than the healthy bound.** 14.3 per
  node kill in the soak, inside the stalled-bucket bound (about 31) but
  above the healthy one (about 8, ADR 0034). Use durable writes for data
  that must survive a kill.
- **The change feed lists the whole history each poll.** 51 s at rest
  for 100,000 actors with 2 objects each; minutes with long histories.
  Parallel listing and compacting idle histories come later.
- **A restore takes about 90 s**, most of it two fixed 40 s waits that
  make sure no old holder still writes.
- **Idle nodes send more GETs than modeled**: about 3.2 a second per
  node instead of 2.3.

- **`app delete` removes actors whose names are escaped.** An actor name
  with `:`, a space, `%`, `/`, an uppercase letter or `~` (big rooms'
  directory and broadcast-group actors) is stored escaped twice; the
  delete escaped each listed key once more, deleted nothing and gave up
  with "objects keep appearing". It now deletes exactly what it lists,
  so `--resume` clears what an earlier delete left. `wasmkeep feed`
  exported none of those actors and now does.

- **`app delete` writes the tombstone at once.** To refuse an app that
  does not exist, it listed every key of the app before writing the
  tombstone: on a real bucket, an app with about 950,000 history objects
  kept being served for 7 minutes after the command started, and the
  tombstone's `at` (taken before that listing) read 7 minutes earlier
  than its write. It now reads one listing page, and `at` is taken just
  before the write. The nodes themselves stopped within 3 s of the write.

- **A deleted app stops being served within seconds even while its node
  backs off.** A node checked for the app's tombstone only at its deploy
  checks, and after a failed deploy check (a version it cannot read, a
  bucket error) those back off, doubling up to 5 minutes: an `app delete`
  made then took effect only at the end of that wait. The tombstone is
  now checked every period (3 s) during the backoff too, with one app per
  process or several.

- **`actor restore` no longer reports a done restore as dropped.** The
  command read the outcomes, then the request: a node ending the request
  between the two (outcome written, then request deleted) made it exit 1
  with "restore request dropped" although the actor was restored. It now
  reads the request first. A request that is really gone with no outcome
  says so: it expired or was removed, nothing says the actor was
  restored, and a new request is needed. The backup-restore drill
  retries a 503 after an actor restore, as a client must.

- **A room never wedges at the default CPU budget.** Every per-event path
  of the room is bounded whatever the room went through: publish and
  history no longer load the whole 2,000-message window, a round reads
  each distinct child position once and builds at most 1 MiB per round
  (continuing 20 ms later), and a scale pass runs one durable step per
  event instead of up to four whole operations. In both SDKs. On QuickJS
  the room, edge and relay convert bulk UTF-8 through SQLite, and the
  SDK's `TextEncoder`/`TextDecoder` gain native fast paths. A
  response whose caller left mid-body no longer traps the actor (which
  discarded its instance), and host errors print their fields instead of
  `[object Object]` in the log. A TypeScript actor's uncaught error now
  answers a generic `500 actor error` body; its details go to the log
  only. Rust's `broadcast::Store::query` takes parameters.

- **Bounded big-room fan-out.** Rust and TypeScript edges share serialized
  frames by cursor, serving caught-up sockets first and building at most
  16 frames / 1 MiB per event. Stalled-only retry alarms back off to the
  keepalive interval without delaying new publishes. The room benchmark
  counts publish failures by status, retries the same id for up to 30 seconds,
  and separately reports unacknowledged publishes. ADR 0040 states the
  current single-node placement limit for a room's tree.

- **Busy actors fit backup/feed listings.** History grouping now accounts for
  retained metadata and string capacities with a 32 MiB per-actor budget,
  replacing the 8 MiB budget's fixed 1,024-byte surcharge per object. Tests
  cover 15,000 and 73,126 objects. Refusals name the actor, total object count,
  limit and recovery steps; the inspector's separate limit is unchanged.

- **History copied inside the bucket reads again.** Some S3-compatible
  providers report another ETag in a listing than on a read of the same
  object made by a server-side copy (such as the copy `cluster migrate`
  makes), and refuse a read conditional on the listed one. `actor
  export`, `actor history --deep`, `actor sql`, `feed` and `backup create`
  refused every such object with "history listing went stale". A refused
  read is now made once more without the condition and accepted only when
  the object is proven to be the listed one: same size, same
  last-modified second, a valid LTX file with its checksum, and the same
  listed ETag in a new listing taken after the read. A replacement
  between the listing and the read is still refused. Nodes were not
  affected: their restore does not compare ETags.

- **An answer for a request that may have run says so.** A host answer
  whose request was handed to the owner or the guest and whose own answer
  was lost or withheld now carries `x-wasmkeep-outcome: unknown` and no
  `Retry-After`, and its body ends `the request may have run`: a forward
  written to the owner whose connection then failed, an answer from a
  node no longer trusted, an answer withheld because the forwarding
  node's certificate stopped being live while the request ran (seen in a
  rolling upgrade: a 503 whose durable write had landed), a placed hop
  that failed, both `504`s, and a `500 actor error` for an event that
  ended with no response. Forwards that provably never reached the owner
  keep the plain `503` with `Retry-After: 1`.
- **`/_wasmkeep/ready` waits for the app's tombstone check.** A node
  following its app answered ready as soon as its first lease write
  landed, before its check that the app was not deleted (ADR 0028), and
  refused every actor until then: on a real bucket the first request
  after ready could get `503 this node cannot run the actor now`. The
  reasons `this node has not yet checked whether its app was deleted` and
  `the app was deleted` join `wasmkeep_not_ready`.

- Every command now writes its logs to stderr, `wasmkeep serve` included;
  stdout carries only a command's own output, so `--json` stays JSON when the
  bucket client retries. A service that collected a node's logs by
  redirecting stdout alone now needs stderr too (systemd's journal takes
  both).

- **Placement at wake-up** ([ADR 0041](docs/adr/0041-placement-at-wake-up.md)):
  a node about to wake an actor nobody holds sends that one event to a
  clearly lighter node of the app (a resident fill of at least 20%, more
  than 25% above the lighter of two rendezvous candidates, within a
  budget per heartbeat), which acquires it unless its own count, read on
  arrival, says it is not the lighter one; then the event runs where it
  woke. Outside requests with no body or one of 64 KiB or less, and
  `actors.call`, are placed; WebSocket upgrades, alarms and streamed
  bodies never are, and a running actor never moves. The heartbeat's load
  line gains `actor_room` and `socket_room` per app (older readers skip
  them), nodes announce `place`, the hop carries a signed
  `x-wasmkeep-placed` mark, and `wasmkeep cluster status` shows each
  node's fill. On by default; `serve --no-placement` or `[process]
  placement = false` turns it off. New metrics:
  `wasmkeep_placements_total`, `wasmkeep_placements_received_total`,
  `wasmkeep_placement_kept_total`, `wasmkeep_placement_fill`.

- `backup restore` warns when the selected app version imports files but the backup predates file manifests.

- **Actor SQL/export verification:** apply SQLite authorization, schema checks,
  and execution deadlines before verification queries; reject hostile host
  metadata views and run verification off the async executor.

- **Local inspector:** `wasmkeep inspect --app TENANT/APP` serves an embedded,
  authenticated loopback page for bucket status, class and actor listings,
  retained history and constrained read-only SQL. No node endpoint or bucket
  writes. See the [inspector guide](docs/inspector.md).

- **A React quickstart:** `examples/react-todos` is a TypeScript actor
  and a React + Vite page over one typed contract, with Vite proxying to
  `wasmkeep dev` and the built page deployed as static assets
  (`not-found = "spa"`); the tutorial is
  [docs/build/react.md](docs/build/react.md), run as written by
  `scripts/tutorials/react.sh`. `wasmkeep-ts build` now resolves
  `@wasmkeep/sdk/contracts` and `@wasmkeep/sdk/room-client`, as it did
  `@wasmkeep/sdk/front`: before, an actor could not import a contract file
  from the contracts entry.

- **Typed Rust clients:** the `client` feature of `wasmkeep-sdk` adds
  `contract!` (a plain `macro_rules!` macro) and `record!`: declare an actor
  class's operations with typed inputs, answers and declared errors, and get a
  client whose methods return values and the same nine-kind failure as the
  TypeScript clients, with `batch` over `call_many`. Requests, limits and the
  error envelope match TypeScript (the shared problem vectors run through the
  client). No dependency, no WIT or server change. See [typed
  clients](docs/typed-clients.md#typed-clients-in-rust).

- **Static assets (ADR 0030, section 9):** deploy project directories with
  app-scoped indexes, hostname isolation, verified per-app caches, browser
  cache rules, compressed variants and ranges. Deploy/collection markers
  protect retained versions through concurrent collection and rollback;
  unchanged deploys repair missing files. `wasmkeep dev` serves files live
  with the same routing and headers. Format 7 requires the `assets`
  capability. See [static assets](docs/static-assets.md).

- **Big rooms (ADR 0040):** the Rust and TypeScript SDKs gain a
  directory (key ranges to actors, split and merge as durable
  transactions) and a broadcast group: a room actor that numbers messages
  as (epoch, number) and keeps a history window, edge actors of at most
  500 sockets that it feeds by call-many, and a relay layer past 16 edges
  (up to 256). Edges split at 80% full and drain below 30%. Delivery is at
  least once and ordered; the browser client drops repeats, recovers gaps
  over HTTP and reconnects from its last position. Numbers lost with
  batched writes are announced void; `confirm` flushes before each round.
  Example: `examples/ts-big-room`. See the [designing for hot actors](docs/designing-for-hot-actors.md)
  page.

- **The change feed (ADR 0039):** `wasmkeep feed --to parquet --out DIR
  --state DIR` reads an app's actors from the bucket, read-only, and
  writes one append-only Parquet file per class and table for the actors
  that changed since the last poll, with `_wk_states` saying which state
  each row is (`state`, `deleted`, `rebase` after a restore or a recreated
  app). A stopped or crashed feed resumes without gaps; delivery is at
  least once, ordered per actor. Each poll regenerates
  `views.duckdb.sql`, the latest state of every actor per table, with
  columns united across schema changes. `wasmkeep feed compact` keeps
  only each actor's latest state. See [the change
  feed](docs/change-feed.md).

- Actor-requested WebSocket closes preserve their code and reason even when the
  data queue is full; queued frames are discarded and their byte budget released.

- **Files for actors and the front (ADR 0030, `blobs`):** the interface
  `wasmkeep:actor/blobs` (head, get, put, delete, list, ranged reads,
  streamed writes, download and upload links), in each caller's own
  namespace on the app's bucket or the node's disk. `wasmkeep_sdk::blobs`
  in Rust, `@wasmkeep/sdk/blobs` in TypeScript (buffered calls and links).
  Whole-file calls and bucket uploads are 16 MiB at most; streams are
  bounded by `--max-blob-bytes`. Nodes announce `blobs`; a version that
  imports it is refused while a live node lacks it. See [files](docs/files.md).

- **A hibernating actor never has two live states.** The state was
  dropped after the actor left the registry, so an event in that window
  spawned a second actor and state while the first was still dropping.
  It is now dropped with the actor still registered; an event meanwhile
  queues and runs on a fresh state, an eviction mark raised during the
  drop is kept, a panicking drop is contained, and an event waits (up to
  the event's wall budget) for the earlier state's resources.

- **Several apps per process (ADR 0038):** `wasmkeep serve --node-file
  wasmkeep-node.toml` serves several apps behind one public, one cluster
  and one metrics port. The public port routes by hostname (421 for an
  unknown host, every `x-wasmkeep-*` header removed); each app keeps its
  node id, lease, state and restarts, and a verdict on one app's node id
  restarts that app alone, in place. Residents, connections, front
  reservations, uploads and restore reads are shared between apps with a
  floor per app. Every node now names its sender in each hop it signs and
  announces `multi-app` (and `process=<id>`) in its heartbeat; a process
  of several apps refuses to start while a live node lacks it. `cluster
  status` groups node ids by process, `cluster drain --process ID` drains
  them all. `serve --app` is unchanged. See [the node
  file](docs/reference/node-file.md) and [upgrading to
  0.6.0](docs/deploy/upgrading-to-0.6.md).

## 0.5.1

**Fixes and safer operations on 0.5.0.** Activations run under the same
deadline as events; the guest SDKs keep HTTP headers lossless; destructive
commands print an operation ID and can be resumed with `--resume`; a
refused actor restore no longer disturbs the actor's sockets; the
recorded-head format gets its follow-ups; `serve --no-alarms` runs a node
that never fires alarms. No WIT or manifest format change: a 0.5.0
cluster upgrades node by node, and actors built with the 0.5.0 SDKs keep
running. During the upgrade, use the 0.5.1 CLI, and expect a superseded
actor delete to show as dropped to a 0.5.0 CLI (below). The TypeScript
SDK's `http.fetch(...).headers` changes behaviour (below). Still a
**pre-alpha pre-release**.

- Refused actor restores preserve existing WebSockets and upgrade admission.
  Snapshot validation and restore fences now precede the 1012 close; accepted
  restores retain their checked snapshot while the old sockets drain.
  Conditional restores keep `--if-head` exact through draining and retries:
  intervening writes produce a conflict and survive. Late upgrade refusals
  report "actor restore pending" with `Retry-After: 1`, not capacity exhaustion.

- Actor delete, app delete, actor restore and backup restore print operation IDs
  before waiting and accept `--resume ID`, which never files a new request.
  Actor outcomes are read without consuming them and last for the request TTL;
  after expiry the ID is unknown and nothing is filed. App delete matches the
  tombstone ID; backup restore matches the journal nonce or completed `restored`
  marker. Pending actor deletes superseded by a later restore or a delete at
  or after their bucket time leave the replacement intact. A 0.5.0 CLI reports
  a superseded delete as dropped (fails closed); a 0.5.0 node neither recognises
  nor expires `.superseded`. Bindings, capability negotiation and durable
  receipts move to 0.6.0. See [operation recovery](docs/operations.md).

- **Activation deadlines:** an actor's activation (instantiating
  its component and running its start function) runs under the same wall,
  lease and replication guard as an event; cut, it waits for its pending
  database work, gives up its hold and answers 503 with `Retry-After`. A
  front's validation runs under the front's wall budget. No new setting.

- **Lossless HTTP headers in the guest SDKs:** the TypeScript,
  Python and Rust SDKs keep headers as ordered (name, value) pairs, compare
  names without case, and never join `Set-Cookie`. `get(name)` is the first
  value, `getAll(name)` (`get_all`) every value in order, `append` adds and
  `set` replaces all: `HeaderList` (TypeScript: `req.fields`,
  `actors.call(...).fields`, `http.fetch(...).fields`), `Headers` (Python:
  `req.fields`, `CallResponse.fields`, `req.header_list`) and
  `wasmkeep_sdk::headers::Headers` (Rust). A response sets repeats with
  `headerList` (TypeScript) or `header_list=` (Python); `actors.call` and
  `http.fetch` accept a list value in a record, pairs or the new type;
  `respond_headers` and `request_headers` are new in Rust. No WIT change.
  Behaviour that changes: TypeScript's `http.fetch(...).headers` no longer
  joins `Set-Cookie` values with `, ` (it holds the first; read all through
  `fields`); a TypeScript response whose headers repeat `Content-Type` in
  another case no longer gets a second default one; a TypeScript lookup in a
  request without a header list is now case-insensitive. In Rust, `Fetched`
  gains a public `headers` field (a struct literal of it needs the field).
  The older `headers` dicts and records still hold one value per name.

- **Recorded-head follow-ups ([ADR 0035](docs/adr/0035-a-recorded-head-on-hand-over.md)),
  for apps that enabled the format:** an idle expiry refused for
  visibility keeps the actor's idle origin (`idle-since`), so it still
  expires once its history is visible; a local database that claims the
  recorded head with another checksum is set aside and the history
  downloaded, instead of a 500; a link of the history still missing after
  the bounded re-listing is reported as a corruption (500) instead of a
  503 for ever.

- `wasmkeep serve --no-alarms` starts a node that serves requests but never
  fires an actor's alarms (no delivery, no sweep of the bucket's alarm index);
  alarms stay recorded and pending for a node without the flag. The restore
  drill uses it for its throwaway restored node, so a restored copy does not
  run the app's side effects twice.
- A node at a 65,536 open-file limit with the default socket request (50,000)
  no longer warns that socket capacity was reduced: it admits about 25,800
  sockets and says so in one INFO line stating the number. A warning remains
  when an explicit `--max-sockets-per-node` is cut, or when anything else is.

## 0.5.0

**Batched writes, data access and per-node limits.** A class can batch
its plain writes into one upload (`writes = "batched"`); an operator can
export an actor as SQLite, query it read-only, back up an app while the
cluster runs, prune old backups, restore a whole app or one actor, and
check all of it with a backup and restore drill. A node enforces
declarative rate limits from `wasmkeep.toml`, configurable socket caps
and a file-descriptor budget. An app can opt in to a recorded head on
hand-over, so that the next node serves only the state the last one
acknowledged. TypeScript actor clients are typed. Still a **pre-alpha
pre-release**.

**Upgrade:** [upgrading to 0.5.0](docs/deploy/upgrading-to-0.5.md): the
new capability words, the manifest formats a batched class and a rate
limit write, the one-way recorded-head format, and what a rollback to
0.4.2 needs.

**Soak** of `57b76c2` on three OVH VPS, two hours and 39 kills in each
mode ([strict](bench/results/2026-11-16-57b76c2-soak-ovh-bhs-3vps-2h-strict-57b76c2.csv),
[batched](bench/results/2026-11-16-57b76c2-soak-ovh-bhs-3vps-2h-batched-57b76c2-batched.csv),
[numbers](docs/benchmarks.md#two-hours-strict-and-batched)). Durable
writes: 386,488 acknowledged in strict and 404,324 in batched, none lost
or handed out twice in either; durable write p50 78.8 ms and 76.8 ms, p99
999 ms and 920 ms; writable again after a kill p50 18.2 s and 16.8 s, p99
22.5 s and 21.9 s, worst 24.9 s and 24.5 s; 312 alarms in each, none
missed. Plain writes acknowledged and lost with their node: 2 of 208,101
in strict and 785 of 1,237,008 in batched (about 20 per kill).

**Rolling upgrade** from 0.4.2 to the release candidate `a391642`, on the
fleet: node by node with writes flowing and actors moved between old and
new nodes; a batched deploy refused while a 0.4.2 node was live;
`app format --enable` refused while nodes ran the app; with strict
current and the recorded head not enabled, one node rolled back to 0.4.2
and forward again, actors written in batched mode included. Not tested:
in-place restore markers, `restores/` and format 6 in a rollback
([upgrading](docs/deploy/upgrading-to-0.5.md#rolling-back-to-042)).

Date: 2026-11-17

- **Per-node rate limits (0.5.0, [ADR 0036](docs/adr/0036-per-node-ingress-rate-limits.md)):**
  `[limits.<name>]` rules in `wasmkeep.toml` by route, method and key (the
  app, the client, or a header). A node checks them before it reads a
  body or does any actor work, and answers 429 with `Retry-After`; a
  refused direct request writes nothing to the bucket. Clients are told by
  their TCP address only (an IPv6 one by its 64-bit prefix), never by a
  forwarded header. A forwarded request is charged once, by the owner,
  from a signed identity. An app with no rule skips all of it. A version
  with a rule is manifest format 6: a deploy is refused while a live node
  lacks `rate_limits`, and a node of 0.4.x reads it as corrupt and limits
  nothing, so upgrade every node first
  ([project file](docs/project-file.md#ingress-limits)).
- **Socket caps and a descriptor budget (0.5.0, [ADR 0037](docs/adr/0037-socket-session-admission.md)):**
  an upgrade holds a permit for its whole life, on the local path and on
  relays, and over a cap it gets 503 with `Retry-After`. Sockets draw from
  a budget derived from the soft open-file limit, with room kept for
  SQLite files, the bucket client and listeners; at a limit of 1,024 that
  is 144 sockets and 96 resident actors (192 before). A very low limit
  starts degraded with a warning. A relay whose one direction ended keeps
  delivering the other until the close wait, so the last frames and the
  Close arrive.
- **Online backup** ([ADR 0033](docs/adr/0033-data-access-export-and-online-backup.md),
  [guide](docs/deploy/backup-and-restore.md)): `wasmkeep backup create`,
  `list` and `verify`, and `actor history`. A backup reads the live bucket
  with LIST and GET only: no lease, no ownership record, no write. Each
  actor is replayed and verified before its record is written; runs are
  content-addressed and end in a checksummed `complete`; `verify` works
  from the destination alone. It detects corruption, not an attacker who
  can rewrite the whole archive.
- **`backup restore`:** restores a whole app from a verified run, into an
  empty root or `--in-place` (a verified backup of the current state is
  taken first and pinned as the undo). A fence is written before any state:
  nodes refuse to serve the app while it stands, and `backup create`,
  `app delete` and `deploy --recreate` refuse. Every phase is journaled, so
  a rerun continues, and `--abort` cancels before the fence opens. Nodes
  announce the word `restore`; `--force` waives old nodes only.
- **`actor restore`:** restores one actor to a time, a transaction or a
  backup while the cluster runs, as a new write at the next position,
  never a rewrite. The owner runs it, closes the actor's sockets (1012),
  and a later delete always wins over an older request. Needs
  `actor-restore` on every node registered for the app, checked by the CLI
  and by the owner. The strict golden trace is re-recorded: every delete
  now labels its snapshot, with no change to a bucket request's key or
  order.
- **`backup prune --keep-runs N`:** keeps the newest N complete runs, the
  latest, every complete run under 24 hours old and pinned runs (an
  in-place restore's undo, a restore in progress), and deletes only what
  no kept run references. It never touches the live bucket; a create
  overlapping a prune fails rather than publishing a run with missing data,
  and an interrupted prune is finished by a rerun.
- **A backup and restore drill, and a `backup create` fix:**
  `scripts/drills/backup-restore.sh` backs up a fixture app under write
  load, verifies the run, restores it into an empty prefix, starts a
  throwaway node there and checks that every acknowledged write is
  present; `check.sh --s3` runs it against moto. It found that `backup
  create` polled an ended actor listing again, which panics on a real
  paginated S3 listing; the listing is fused now.
- **Typed TypeScript clients and a structured error envelope:**
  `defineOperation` and `defineContract` with codecs (a Zod-style
  `{parse}` validator works), a client that returns values and a nine-kind
  failure instead of throwing, `batch()` over `callMany` and `bind`
  ([guide](docs/typed-clients.md)). Application errors may use an opt-in
  envelope, `{"v":1,"code","message","details?"}` as
  `application/vnd.wasmkeep.error+json`, written and read the same way by
  the TypeScript and Rust SDKs (shared test vectors), with a hostile body
  refused rather than trusted. `CallError` gains a closed `CallErrorKind`.
  Additive; no WIT change.

- **Recorded head on hand-over (0.5.0, [ADR 0035](docs/adr/0035-a-recorded-head-on-hand-over.md)):**
  an app enabled with `wasmkeep app format --enable` writes ownership
  records as `wasmkeep-owner 2`, carrying the position each releasing
  node acknowledged. The next node activates only once its restore
  proves it (the exact position, or a later generation on the selected
  chain); a bucket listing that lags answers 503 with `Retry-After`
  after at most ten listings in 15 s, with a per-actor cooldown, instead
  of serving older state. Off by default; one way; every node of the
  app must run 0.5.0 or later (heartbeat word `recorded-head`) and the
  app must be stopped to enable it. All readers accept both record
  versions; binaries before 0.5.0 cannot read version 2. `actor export`
  shows a record's recorded head; `cluster status` warns when an app's
  nodes differ on the capability. See
  [the operator guide](docs/cluster.md#the-recorded-head-ownership-format).

- **Configurable socket admission (0.5.0):** `serve` accepts
  `--max-sockets-per-actor` (1,000 requested) and `--max-sockets-per-node`
  (50,000 requested), plus `--max-sockets-per-app` (defaults to the node cap;
  set lower for isolation). Zero refuses every WebSocket at that scope.
  The node ceiling now includes relays, tightening the previous owner-only accounting. Socket sessions join
  the descriptor admission estimate and hold reservations through transport
  cleanup. Over-capacity upgrades receive 503 with `Retry-After`. Startup
  logs and metrics report requested/effective socket ceilings. See
  [socket admission options](docs/reference/socket-limits.md).

- **Actor export and read-only SQL** ([guide](docs/actor-data-access.md),
  ADR 0033): `actor export TENANT/APP CLASS/NAME -o FILE` and `actor sql`
  replay verified bucket history without contacting nodes or acquiring a
  hold. Current-head, capture-time and transaction selection follow the
  current ancestry; output reports the unacknowledged-tip caveat. Exports
  are portable SQLite files; SQL has an authorizer, bounded results and
  table/CSV/TSV/JSON/JSONL output. Both accept `--tmpdir` with advisory disk
  checks and clean scratch copies on Ctrl-C/SIGTERM; `--require-owner-match`
  refuses unknown/mismatched owner hints (exit 3). Publication falls back to
  private no-clobber copying when links are unavailable. Time selection skips
  undated snapshot-only bases, and short object bodies are retried.

- **SQL result limits now account for retained memory.** The 16 MiB
  budget includes row and cell overhead, vector spare capacity, allocation
  allowances and column names once, as well as text/blob payloads. Queries
  with many empty values or large aliases now fail with `quota-exceeded`
  when they exceed it; names and payloads are checked before copying.
- **`storage.flush`: make the writes so far durable without a
  statement** ([ADR 0034](docs/adr/0034-batched-plain-writes.md),
  section 4). Added to `wasmkeep:actor@0.2.0` (components built before
  link as they did) and to the Rust (`storage::flush`) and TypeScript
  (`storage.flush()`) SDKs. It returns once every write the actor
  committed before the call is durable, plain `exec` included: in a
  cluster this event's writes so far go to the bucket in one upload (an
  alarm set meanwhile listed first, as `exec-durable` does), or nothing is
  sent when nothing is pending; on a single node the WAL is fsynced. It
  is refused with `not-allowed` inside a transaction. An `internal`
  starting `replication stopped` means the writes are not known to be
  durable and the actor is restored from the bucket, as after a failed
  `exec-durable`, and every later `flush` of the event answers it at once
  ([concepts](docs/concepts.md#storage)). `exec-durable` is unchanged.
  TypeScript and Python components built with this SDK import it whether
  or not they call it, so they need a node of this release.
- **`deploy` refuses a component a live node could not link**
  ([cluster](docs/cluster.md#rolling-upgrades-and-new-storage-functions)).
  Nodes now announce the heartbeat word `flush`. `deploy` (and
  `--rollback`) reads the imports of each component of the version it
  would make current and refuses, before writing anything and without
  `--force`, one that imports `storage.flush` while
  a live node of the app lacks `flush`, or one that imports
  `storage.delete-self-durable` while a live node lacks `ends` (a node of
  0.4.1). The second is new for every class: 0.4.2 accepted that deploy
  and the 0.4.1 node stayed not ready on it. The classes a deploy keeps and
  the version a rollback brings back are checked too. The check cannot
  see a node started without `--node-id`, or a node that starts after
  the deploy. Upgrade every node, then deploy.
- **A class's `writes` in `wasmkeep.toml`** (ADR 0034, section 1):
  `"strict"`, the default and every release's behaviour, or `"batched"`,
  which every build of this release deploys and runs (strict stays the
  default). `"strict"` written or not makes the same version, byte for
  byte. A version with a batched class is written as manifest format 5,
  which a deploy holds back while a live node lacks `flush`; each hold
  keeps the mode it was acquired with until its writes are flushed
  ([project file](docs/project-file.md)). `deploy --writes CLASS=MODE`
  and `serve --writes` set it from the command line, and `--max-unsent-age`
  bounds how long a committed write waits unsent. In a batched class, what a node's death can lose is the segment in flight plus the commits not yet captured: with a healthy bucket about two upload round trips plus one event (2r + e), and with a stalled bucket up to about `--max-unsent-age` (500 ms by default) plus one event after the stall began. Durable writes (`exec-durable`, `flush`) are never lost. Measured on three OVH VPS, 2 h and 39 kills per mode ([strict](bench/results/2026-11-16-57b76c2-soak-ovh-bhs-3vps-2h-strict-57b76c2.csv) and [batched](bench/results/2026-11-16-57b76c2-soak-ovh-bhs-3vps-2h-batched-57b76c2-batched.csv)): durable writes lost 0 in both; plain writes lost 2 of 208,101 acknowledged in strict and 785 of 1,237,008 in batched (about 20 per kill, at about 172 plain writes a second across the cluster); durable write p50 and p99 78.8 ms and 999 ms in strict, 76.8 ms and 920 ms in batched; failover p99 22.5 s and 21.9 s; no alarm missed. As aggregate counts over all actors and kills, they are evidence, not a proof of the per-actor bound.
- **`cluster diagnose` requires listings that are strongly consistent
  after a write.** A node taking an actor over plans its restore from one
  listing of the actor's history, and after a voluntary hand-over it may
  list at once: a segment that listing omits would make it restore an
  older head and fork the history. 0.4.2 passed a listing that showed a
  write up to a second late, and only warned up to ten seconds. Now the
  first listing that starts after a write was acknowledged must show it
  (`list-new-key`, now five fresh keys, `large-object`, `last-modified`,
  `list-paging`), or the check fails and the bucket is reported not safe;
  a listing that fails or does not answer is still an `error` (no
  verdict). The verdict for exit 0 now reads `required behaviour observed
  by these checks (not a proof of safety)`: the checks sample a bucket,
  they do not prove it safe. The `--json` field `verdict` keeps `safe`
  for exit 0 ([buckets](docs/deploy/buckets.md)).
- **The TypeScript SDK no longer reads a broken request body as a whole
  one.** The incoming body treated any error of its stream as the body's
  end, so a body cut short (a client that went away, a close short of
  its Content-Length, a streamed body over the host's 16 MiB cap) reached
  a handler as a shorter, complete-looking body. Now only the stream's
  `closed` is its end, and only once the trailers say it ended well;
  anything else makes `bytes()`, `text()` and `json()` throw `the request
  body is incomplete: ...`, on QuickJS and StarlingMonkey alike. The
  Rust and Python SDKs already told the two apart, and Go's
  `ReadBody()` does (its `Body()` gives what arrived); the C (and Zig,
  which uses it), D and MoonBit SDKs still read a broken body as a
  shorter one.
- **A failed listing of `nodes/` no longer stops the reads of known
  nodes.** In 0.4.2 a node whose scheduled listing failed tried the
  listing again every heartbeat instead of reading the nodes it knows, so
  a bucket that refuses or throttles LIST while it answers reads would
  freeze its view, and its trust in every peer would lapse one horizon
  later. Now a heartbeat whose listing fails still reads the known nodes,
  and the listing is tried again the next one
  ([ADR 0020](docs/adr/0020-one-lease-per-node.md), amendment of 2026-11-14).

## 0.4.2

**Actors that end, and a database kept by one actor.** An operator
deletes one actor (`wasmkeep actor delete`) or a whole app (`wasmkeep
app delete`); a class can expire actors left idle for a stated time
(`expire-after-idle`); and `storage.delete-self-durable` returns only
once the delete is in the bucket. The Rust and TypeScript SDKs make one
actor the owner of an app's shared tables, which the app's other actors
and its front query with bound SQL. `wasmkeep new` writes a Rust or
TypeScript project that builds and runs the first time, and a toolchain
image, `ghcr.io/wasmkeep/toolchain`, builds any Recommended or Supported
SDK's components with only Podman or Docker installed. The release
carries the counter prebuilt in each of those languages. An idle node
lists the bucket about 13 times less (on R2 and S3 a LIST costs what a
PUT costs). Still a **pre-alpha pre-release**. The interface between a
component and the host stays `wasmkeep:actor@0.2.0`, with one function
added: components built for 0.4.1 run as they are.

**Upgrade: rolling**, from 0.4.1, one node at a time
([upgrading WasmKeep](docs/cluster.md#upgrading-wasmkeep)). 0.4.2 changes
no format 0.4.1 reads in the bucket (layout, ownership records, history,
alarm index, intents), and signs calls between nodes as 0.4.1 does. What
it adds, 0.4.1 never reads: delete requests (`state/<t>/<a>/deletes/`),
`control/changed`, an app's tombstone and its `recreated` marker, and
manifest format 4, written only for a version with an idle limit. Its
heartbeat adds the word `ends`, which 0.4.1 ignores; its hello to peers
gets a 404 from 0.4.1, which lists every period anyway. While both
versions run, `cluster status` warns of mixed versions, and:

- A deploy with `expire-after-idle` and `app delete` are refused while a
  live node of the app runs 0.4.1; `actor delete` requests wait until a
  0.4.2 node holds the actor or finds it free.
- A 0.4.1 node cannot load a component that imports
  `storage.delete-self-durable`, and the deploy does not check for it:
  that node stays not ready on the version. Every TypeScript and Python
  component built with the 0.4.2 SDK imports it, called or not (the
  release's `counter-typescript.wasm` and `counter-python.wasm` too), as
  do Rust, Go, C and Zig components that call it. Deploy them once every
  node runs 0.4.2.
- Upgrade the CLI with the nodes: a drain, an undrain or a move from the
  0.4.1 CLI is read within a minute instead of 3 s, and the 0.4.1 CLI
  knows nothing of idle limits or tombstones.
- A 0.4.1 node that starts is seen by 0.4.2 peers within 30 s (sooner
  over TLS).

Going back to 0.4.1 rolls the same way while no version with an idle
limit is current and no component that needs 0.4.2 is deployed. From
0.4.0, upgrade to 0.4.1 first.

The rolling upgrade was tried on the same three nodes: one node put back
on 0.4.1 beside two on this release, one counter written through each in
turn (1, 2, 3, one owner); the 0.4.2 owner killed (`kill -9`), writable
again through the 0.4.1 node after 17 s with the count kept (4); that
0.4.1 owner killed, writable through a 0.4.2 node after 15 s (5); the
0.4.1 node rolled forward, and 6, 7, 8 through the three nodes.

Soaked on three nodes against a real bucket, a node killed every three
minutes, for two hours on this release's code (commit `563cb94`; later
commits change documentation, these notes and versions only):

39 kills, 378,472 acknowledged durable writes, none lost or applied
twice; writable again after a kill (1,264 takeovers) at 17.4 s at the
median, 22.5 s at p99 and 29.0 s at worst; 312 alarms fired, none
missed; no stale read after a takeover; a restarted node ready within
0.2 s. Of 202,535 plain writes (acknowledged before the bucket holds
them), one was lost to a kill and its number handed out again, as plain
writes allow
([`2026-11-14-563cb94-soak-ovh-bhs-3vps-2h-0.4.2-rc.csv`](bench/results/2026-11-14-563cb94-soak-ovh-bhs-3vps-2h-0.4.2-rc.csv)).

### What changed

- **The counter in every Recommended and Supported language, prebuilt**:
  besides `counter.wasm` (Rust), the release carries
  `counter-typescript.wasm`, `counter-python.wasm`, `counter-go.wasm`
  (TinyGo), `counter-c.wasm` and `counter-zig.wasm`, built with the tool
  versions CI tests, each in `SHA256SUMS` with its own Sigstore bundle
  (36 files in the release, from 26). The license texts of the runtimes
  they embed (QuickJS-ng, CPython and the C libraries it links, the Rust
  crates of the componentize-qjs and componentize-py runtimes, TinyGo and
  the Go standard library, wasi-libc, Zig's library, the WASI adapter) are
  added to `THIRD_PARTY_NOTICES-components.txt`. The Experimental languages
  (standard Go, D, MoonBit, C#) are not released.
  [Verifying a download](docs/deploy/single-node.md#1-install-the-binary).

- **`wasmkeep new DIR --lang rust|typescript [--front] [--app TENANT/APP]`**
  writes a project that builds and runs the first time: a class `counter`,
  optionally a front over it (the class then `internal`), its
  `wasmkeep.toml`, a README with the commands (`wasmkeep dev`, then
  `wasmkeep deploy`), a `.gitignore` and an `.env.local.example`. The
  templates are in the binary (no network), the SDK is pinned to the CLI's
  own version, and a directory that is not empty is refused. Every variant
  is built and served by `wasmkeep dev` in the tests. Other languages
  later. [Starting a project](docs/tutorials/first-actor.md#starting-a-project-with-wasmkeep-new).
- **A database kept by one actor**, in the Rust and TypeScript SDKs
  ([guide](docs/database.md),
  [ADR 0029](docs/adr/0029-a-database-client-as-an-sdk-pattern.md)): a
  class mounts the SDK's handler (`db::handle`, `db.define`) and its
  ordered migrations (`db::migrate`); the app's other actors and its front
  send it SQL with `db::Client` / `db.Client` (`query`, `exec`, `batch`,
  `read`) over `actors.call`. Parameters are bound, each request is one
  transaction (a batch is all or none, a read is always rolled back, a
  write commits durably), statements that control the transaction are
  refused, requests and answers are capped, and only inside callers are
  answered. The SQL comes from the app's code, never from its clients.
  [`examples/db`](examples/db) and [`examples/ts-db`](examples/ts-db);
  every client is tested against every handler. The other SDKs come later.
- **Actors that end** ([ADR 0028](docs/adr/0028-actors-that-end.md)):
  - `wasmkeep actor delete --app TENANT/APP CLASS/NAME --bucket ...`
    deletes an actor: the node that holds it, or any node serving its
    class if nobody does (its node died, say), closes its WebSockets with
    1000, removes its data and alarm and records the delete in the bucket.
    The command waits until the delete is recorded (`--wait`, 60 s;
    `--no-wait`), refuses a name with nothing in the bucket, and a request
    read twice is carried out once. Nodes of an earlier version ignore the
    requests ([cluster](docs/cluster.md#deleting-one-actor)).
  - `storage.delete-self-durable`, added to `wasmkeep:actor@0.2.0` (a
    component built before links as it did): deletes the actor at once and
    returns only once the delete is in the bucket (fsynced on one node),
    or `internal` when it is not known to be. Later storage calls in that
    event are refused with `not-allowed`. Rust
    `storage::delete_self_durable`, TypeScript `storage.deleteSelfDurable`;
    the other SDKs later.
  - Metrics `wasmkeep_actor_deletes_total{how}` and
    `wasmkeep_actor_delete_failures_total{how}` (`self`, `self_durable`,
    `operator`).
  - **Idle expiry**: a class's `expire-after-idle` (`"7d"`, at least an
    hour) in `wasmkeep.toml`, or `deploy --expire-after-idle CLASS=7d`
    (`CLASS=` removes it), part of the version. In a cluster, the lowest
    live node of the app lists the class's ownership records every 10
    minutes and deletes actors idle that long, never one with a pending
    alarm or an open WebSocket; an event arriving meanwhile wins, or
    finds a new actor ([cluster](docs/cluster.md#idle-expiry)). A version
    with a limit is manifest format 4: deploys are refused while a live
    node of the app does not announce the new heartbeat word `ends`.
  - **`wasmkeep app delete --app TENANT/APP --bucket ...`**: writes the
    app's tombstone (`apps/<t>/<a>/deleted`), from which deploys, secrets,
    moves and deletes refuse the app and its nodes stop serving it
    (WebSockets closed with 1001, node lease released, local files
    removed, `serve` exits); once none is live, removes everything under
    `state/<t>/<a>/` and `apps/<t>/<a>/` but the tombstone. Refused while
    a live node of the app lacks `ends`. `deploy --recreate` uses the name
    again ([cluster](docs/cluster.md#deleting-an-app)).
  - Metrics: `how="expired"` on the delete counters,
    `wasmkeep_expiry_passes_total`, `wasmkeep_expiry_candidates_total`,
    `wasmkeep_expiry_kept_total{reason}`.
  - Behaviour changes: a node following an app (`serve --app`) acquires
    no actor until it has checked for the app's tombstone since its lease
    was valid (a moment after it starts); the heartbeat adds the word
    `ends`, which earlier versions ignore.

- **Fewer listings of the bucket.** An idle node lists the bucket about
  0.1 times a second instead of 1.43 (1.1 in 0.4.1; on R2 and S3 a LIST
  costs what a PUT costs): `nodes/` every 30 s, the nodes it knows read one by one every
  heartbeat in between, and a node that starts says hello so its peers list
  at once; the drain, move and delete requests when `control/changed` says
  they changed, or every minute; the alarm index when an entry is due for
  the sweep, at most a minute apart, and at once when a node joins or
  leaves. Fencing, failover timings for known nodes and pin revocation are
  unchanged ([ADR 0009, 0011, 0014, 0017 and 0020](docs/adr/0020-one-lease-per-node.md),
  amendments of 2026-11-14). New metric: `wasmkeep_node_hellos_total`.
  These listings change no format in the bucket. Upgrade the CLI with
  the nodes: `cluster drain`, `undrain` and
  `actor move` of 0.4.1 do not write `control/changed`, so nodes of 0.4.2
  read their requests within a minute instead of 3 s. While both versions
  run, 0.4.1 nodes answer the hello 404 and keep listing every period, and
  0.4.2 nodes see a 0.4.1 node that starts within 30 s (sooner over TLS).
- `wasmkeep_bucket_requests_total` counts the node's read of the app's
  secrets every heartbeat period (`serve --app`), a GET it missed
  before: an idle node's counted GETs go up by one a period, with no new
  request ([three nodes at rest](docs/benchmarks.md)).

- **The toolchain image**, `ghcr.io/wasmkeep/toolchain:<version>` for
  linux/amd64 and linux/arm64: `wasmkeep` and the build tools of the Rust,
  TypeScript, Python, Go (TinyGo), C and Zig SDKs at the versions CI uses
  (`scripts/check-toolchain.py` keeps them equal), so a project builds with
  only Podman or Docker installed. The release workflow builds it, tests the
  pushed image on both platforms (`deploy/toolchain/test.sh`), and signs it
  like the server image; the nightly workflow builds and tests it too. About
  880 MB to download. [The toolchain image](docs/deploy/toolchain.md).
- `wasmkeep_releases_after_lapse_total` counts every release that landed
  while the node's lease was not valid, not only those before its lease
  task saw the lapse: also one sent between the lease task's seeing the
  lapse and its ending the holds (the WebSockets the lapse closes), and
  one of a hold whose acquisition landed after the lapse. Its warning is
  logged once per lapse, for the first such release, with the count
  logged once the lease is valid again, instead of one line per actor.

### Known limits

The limits listed for 0.4.1 still hold. Also:

- The database pattern and `delete-self-durable` are in the Rust and
  TypeScript SDKs only, and `wasmkeep new` writes Rust and TypeScript
  projects only; the other languages come later.
- Idle expiry needs a cluster (a single node keeps no ownership
  records, so nothing expires there), and never expires an actor whose
  encoded name was shortened with a hash.
- A component that imports `delete-self-durable` (every TypeScript and
  Python component built with this SDK) needs a 0.4.2 node: during an
  upgrade from 0.4.1, deploy it once every node runs 0.4.2.
- An `actor delete` or an `app delete` cannot be undone, except from a
  bucket that keeps old versions
  ([backup and restore](docs/deploy/backup-and-restore.md)).
- The toolchain image is about 880 MB to download; the Experimental
  languages (standard Go, D, MoonBit, C#) are neither in it nor
  prebuilt in the release.

### Install and verify

[Install](README.md#install) downloads a binary or runs the image.
[Single node](docs/deploy/single-node.md#1-install-the-binary) shows the
cosign check, the same for every file of the release: 36 assets, 17
files and `SHA256SUMS`, each with its Sigstore bundle. The prebuilt
counters `counter-typescript.wasm`, `counter-python.wasm`,
`counter-go.wasm`, `counter-c.wasm` and `counter-zig.wasm` are checked
the same way, for example:

```bash
f=counter-typescript.wasm
for file in "$f" "$f.sigstore.json"; do
  curl -fsSLO "https://github.com/WasmKeep/wasmkeep/releases/download/v0.4.2/$file"
done
cosign verify-blob --bundle "$f.sigstore.json" \
  --certificate-identity https://github.com/WasmKeep/wasmkeep/.github/workflows/release.yml@refs/tags/v0.4.2 \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com "$f"
wasmkeep serve --actor counter="$f"
```

[Compose](deploy/compose/README.md) checks the server image, and
[the toolchain image](docs/deploy/toolchain.md) its own.

## 0.4.1

**Sockets through the front.** The front hands a client's WebSocket to
an actor of the app, so a room class can be `internal`, with no public
`/actor/` address and its token checked once, in the front. Handlers
send several outbound HTTP requests at once in Rust, TypeScript and Go.
The host strips its reserved `x-wasmkeep-` headers from outbound HTTP
answers, so no upstream can hand a socket off through a front. A room
deleted under its sockets stays deleted: their close events no longer
bring it back. A node whose lease ran out before it noticed can release
an actor early; that was always so and safe by ordering, and is now
documented, counted (`wasmkeep_releases_after_lapse_total`) and pinned
by tests. Still a **pre-alpha pre-release**. The interface between a
component and the host stays `wasmkeep:actor@0.2.0`, unchanged:
components built for 0.4.0 run as they are.

**Upgrade: rolling**, from 0.4.0, one node at a time
([upgrading WasmKeep](docs/cluster.md#upgrading-wasmkeep)). 0.4.1 changes
no format in the bucket (layout, manifests, ownership records, alarm
index) and signs calls between nodes as 0.4.0 does; its heartbeat adds
the word `handoff`, which 0.4.0 ignores, and a 0.4.1 node relays a
hand-off only to a node that announces it. While both versions run,
`cluster status` warns of mixed versions, and a hand-off to a room on a
0.4.0 node gets a 503 with `Retry-After: 1`: deploy an app that hands
sockets off once every node runs 0.4.1. From 0.3.x, upgrade to 0.4.0
first ([upgrading to 0.4.0](docs/deploy/upgrading-to-0.4.md)).

Soaked on three nodes against a real bucket, a node killed every three
minutes, for two hours on this release's code (commit `0bf0547`; later
commits change these notes and versions only): 39 kills, 377,095
acknowledged durable writes, none lost or duplicated; failover p50
17.6 s, p99 23.1 s, max 27.1 s; no alarm missed; no stale read after a
takeover; a restarted node ready again within 0.3 s
(`bench/results/2026-11-14-0bf0547-soak-ovh-bhs-3vps-2h-0.4.1-rc.csv`).
The rolling upgrade was tried on the same three nodes: one node on 0.4.0
beside two on 0.4.1 kept one owner per actor, a node of either version
killed was taken over by a node of the other with every write kept, and
the last node was then rolled forward.

### What changed

- **Sockets through the front**
  ([ADR 0027](docs/adr/0027-sockets-through-the-front.md)). The front
  accepts a WebSocket at its own paths and hands it to an actor of the
  app, so a room class can be `internal` and needs no public `/actor/`
  address. The host marks an upgrade to the front with
  `x-wasmkeep-upgrade: websocket`; the front answers with three
  `x-wasmkeep-handoff-*` headers (class, name, path), built by
  `front::hand_off(class, name, path)` in Rust and `handOff(...)` in
  TypeScript, with `is_websocket` / `isWebSocket` to tell an upgrade. The
  host stops the front's instance, frees its slot, and opens the socket on
  that actor as an inside caller; each socket keeps that mark through
  hibernation. A refused hand-off is an HTTP error (404, 400, 403 with the
  actor's reason, 503), never a socket that opens and closes. Between
  nodes the hand-off is relayed signed, with the front's caller and the
  upgrade mark signed, and only to nodes that announce the new heartbeat
  word `handoff`. `examples/todo-front` gains `/api/live`, a socket
  handed to the signed-in user's `internal` list.
- Behaviour changes: the front sees `x-wasmkeep-upgrade: websocket` on
  WebSocket upgrades; a front's answer to an upgrade with the three
  hand-off headers opens a socket instead of reaching the client; a
  WebSocket upgrade refused because its actor is unavailable (or a value
  it requires is not set) now carries `Retry-After: 1`; outbound HTTP
  answers, to actors and fronts alike, lose the host's `x-wasmkeep-`
  range (headers and trailers), as `actors.call` answers have since
  0.4.0, so no upstream can hand a socket off through a front that passes
  its answer on. Actors and built fronts are unchanged.
- In a cluster, the close events of the sockets a `delete-self` closed
  (under an `existing` or `internal` class) no longer run on a new, empty
  instance of the deleted actor.
- **Several outbound HTTP requests at once** from the Rust, TypeScript and
  Go SDKs: `http::fetch_all`, `http.fetchAll` (QuickJS and StarlingMonkey)
  and `FetchAll` send a batch together and wait for all of it, one result
  per request in input order, at most 16 open at once (the host's limit per
  actor; the rest start as earlier ones finish). `fetch`, `http.fetch` and
  `Fetch` are now a batch of one, with the same answers.
  [Languages](docs/languages.md#several-outbound-requests-at-once) says what
  each SDK does, Python, C, Zig, D and MoonBit included (no outbound HTTP
  helper yet).
- **A node whose lease writes fail while its other writes land** hands an
  actor over as soon as an event reaches it after its lease ran out (8 s
  after the send of its last acknowledged lease write, with the
  defaults; the lapse closes its WebSockets, so every actor with sockets
  open goes then), before
  the node sees the lapse: the release of the hold its actor dropped can
  land, and another node takes the actor at once, without a fence. That
  was already so, and is safe by ordering (the node stops acting before
  it sends the release, and the next owner acquires only after the
  release landed), but ADR 0020 said otherwise and nothing showed it. It
  is now documented ([ADR 0020](docs/adr/0020-one-lease-per-node.md),
  section 1), counted on the releasing node in
  `wasmkeep_releases_after_lapse_total` (a lower bound: a release whose
  reply was lost is not counted) with a warning in its log
  ([monitoring](docs/monitoring.md#failovers)), and pinned by a cluster
  test and a simulation (`madsim_early_release`).

### Known limits

The limits listed for 0.4.0 still hold, except the first: the front now
accepts a WebSocket at its own paths. Also:

- Fronts, `call-many` and the hand-off helpers are in the Rust and
  TypeScript SDKs only; several outbound requests at once are in Rust,
  TypeScript and Go (Python, C, Zig, D and MoonBit have no outbound HTTP
  helper yet).
- A hand-off needs 0.4.1 on the front's node and on the room's: during
  an upgrade from 0.4.0 a hand-off to a room on a 0.4.0 node gets a 503.

### Install and verify

[Install](README.md#install) downloads a binary or runs the image.
[Single node](docs/deploy/single-node.md#1-install-the-binary) shows the
cosign check, the same for every file of the release;
[Compose](deploy/compose/README.md) checks the image.

## 0.4.0

**An app is one thing.** An app's classes and its front are deployed,
versioned and rolled back together, and the app owns its actors' state:
each app keys its actors under its own root in the bucket, so two apps
never share an actor, and a one-time migration moves a bucket prefix
written by 0.2 or 0.3 there. The front, a stateless component deployed
with the app, receives every request outside `/actor/` and calls the
app's actors, several at once with `call-many`. A project file,
`wasmkeep.toml`, says what the app is, for `deploy` and `dev`, with
classes `internal` unless it says otherwise. Load you can see: actors,
sockets and events per node and per app in `cluster status` and the
metrics. A WebSocket room that hibernates wakes where its sockets are,
so a call through another node keeps its clients connected. `wasmkeep
cluster diagnose` checks that a bucket does what WasmKeep relies on.
Still a **pre-alpha pre-release**: for trying WasmKeep, side projects
and staging, not production. The interface between a component and the
host stays `wasmkeep:actor@0.2.0`, with the worlds `front` and
`front-core` and the interface `calls` added: a component built for an
earlier release runs as it is. Fronts and `call-many` are in the Rust
and TypeScript SDKs first. See the [roadmap](ROADMAP.md) for what comes
next.

**Upgrade: stop-all.** Every node is stopped for a one-time migration of
the bucket layout ([upgrading to 0.4.0](docs/deploy/upgrading-to-0.4.md));
no rolling upgrade from 0.3.x.

Soaked on three nodes against a real bucket, on a bucket prefix migrated
to the new layout, a node killed every three minutes, for two hours on
this release's code (commit `e55c152`; later commits change tests and
these notes only): 39 kills, 443,092 acknowledged durable writes, none
lost or duplicated; failover p50 17.1 s, p99 21.0 s, max 22.7 s; no
alarm missed; no stale read after a takeover; a restarted node ready
again within 0.3 s
(`bench/results/2026-11-14-e55c152-soak-ovh-bhs-3vps-2h-0.4.0-rc.csv`).
An earlier two-hour soak on the code of app-owned state and the
migration (commit `7e03c05`) gave the same picture
(`bench/results/2026-11-14-7e03c05-soak-ovh-bhs-3vps-2h-0.4.csv`).

### What changed

- **`wasmkeep.toml`: the app is what the file says**
  ([ADR 0026](docs/adr/0026-the-project-file.md),
  [reference](docs/project-file.md)). A project file names the app, its
  front, its classes with their rule (`internal` unless it says
  otherwise), configuration file and required names, and a build command
  for each part. `wasmkeep deploy --bucket ...` in its directory (or with
  `--project PATH`) runs the builds, prints every change before it
  switches, and deploys exactly the file: a class or front the file no
  longer has is removed, and configuration or required names it does not
  give are removed. Every class's rule is written in the version, `open`
  included (manifest format 3, now also without a front). Unknown keys
  are refused with their line and column. `--rollback` runs no build.
  `wasmkeep dev` with no crate directory runs the file: the front and the
  classes with their rules, local values from `.env.local`, rebuilt from
  each part's `watch` paths, for any language with a build command.
  **Behaviour change:** in a directory with a `wasmkeep.toml`, `deploy`
  refuses `--actor`, `--access`, `--config`, `--requires` and the front
  flags, and `dev` refuses crate directories, `--access` and `--env`;
  pass `--no-project` to use them. Deploys with flags elsewhere are
  unchanged, their default of `open` included. A build runs without
  `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` and `AWS_SESSION_TOKEN`:
  the deploy's bucket credentials are not the build's, and a build that
  needs credentials of its own takes them from a profile or a file.
- **A WebSocket room wakes where its sockets are**
  ([ADR 0024](docs/adr/0024-socket-home.md)). A room that hibernates with
  sockets open keeps its ownership record, so an event reaching it through
  another node (a front's `actors.call`, a request, an upgrade) is relayed
  to the node holding its sockets and wakes it there, at a new generation:
  its clients stay connected. Before, the room woke wherever the event
  landed and its sockets closed with 1012. The record is released when the
  room's last socket goes, and by a drain, a move, a lapse of the node
  lease or a clean stop. Socket classes may now be reached through the
  front on a cluster. A room parked this way is free about 15 to 20 s
  after its node dies, as a resident actor, instead of at once. New
  metrics: `wasmkeep_parked_records`, `wasmkeep_parked_wakes_total` and
  `wasmkeep_parked_releases_total{reason}`; the owner sweep no longer
  reads the record of a parked room each minute (one pass in ten, as a
  safeguard). No format changes: a node of an earlier version beside one
  of this version keeps closing its own sockets as before.
- **`wasmkeep cluster diagnose` checks a bucket.** It runs, through the
  nodes' own bucket code, what WasmKeep relies on: conditional creates
  and replaces (one winner among simultaneous creates, and among
  simultaneous replaces of one version, a stale, deleted or overwritten
  version refused, a read's version accepted), reads right after
  writes, overwrites and deletes, listings that show a new key and drop
  a deleted one (the lag is measured) and return more than one page,
  deletes of absent keys, a 16 MiB round trip, last-modified order and
  precision, and this machine's clock against the bucket's. One line per
  check, then `safe for WasmKeep` (exit 0), `not safe: ...` (1) or no
  verdict (2, also for a safe run whose cleanup failed); `--json` for
  scripts. It writes only under a scratch prefix of its own
  (`run-<time>-<pid>/` in `diagnose/`, or in `--prefix-for-test`) that
  must be empty, and deletes it afterwards even when a check fails. The
  bucket requirements, how to run it and the providers checked so far
  (moto and OVHcloud Object Storage) are in
  [Buckets](docs/deploy/buckets.md).
- **Fronts in Rust and TypeScript.** The Rust SDK has a `front` module
  (from world `front-core`): `actors` and `env` as in an actor,
  `export_front!`, and `http::respond_with`. A front crate depends on
  `wasmkeep-sdk` with `default-features = false`: the new default feature
  `actor` holds the actor world (storage, alarms, WebSockets, `Actor`,
  `export!`, `kv`, `queue`, `workflow`), which a front must not import.
  Actors keep the defaults and change nothing. The TypeScript SDK has
  `@wasmkeep/sdk/front` (`defineFront({ fetch })`) and `wasmkeep-ts build
  --world front`. Both have the helpers `caller()` (`Front`, `Actor`, or
  `Outside`; only the exact `<tenant>/_front/main` is the front), `host()`,
  `scheme()` (`https` when the first `X-Forwarded-Proto` value is) and
  `unavailable(err)` (503 with `Retry-After: 1` for `failed` and
  `timeout`, 500 otherwise, with a generic body), exported for actors too.
  TypeScript requests gain `authority`, `headerList`, and bounded body
  reads: `req.bytes(limit)` and `req.text(limit)` return `undefined` for
  a body longer than `limit` bytes, having read at most `limit + 1` of it,
  chunked or not. Example:
  [`examples/todo-front`](examples/todo-front) (Rust front and `internal`
  class) and [`examples/ts-todo-front`](examples/ts-todo-front). Python,
  Go, C, Zig, D and MoonBit fronts come later
  ([ADR 0023](docs/adr/0023-the-front.md)).
- **Calling several actors at once.** A new interface,
  `wasmkeep:actor/calls`, imported by worlds `actor-core` and
  `front-core`: `call-many(list<(address, request)>)` starts up to 16
  calls together and returns one `result<response, call-error>` per
  entry, in input order, once all have answered or the batch's one
  deadline passed (an actor's call deadline, a front's call deadline). A
  front gathering four slow actors pays about one round trip, not four.
  Each entry is an `actors.call` in every other respect: caller value,
  chain (an entry naming the caller is `reentrant`), routing and signed
  hops between nodes, and from a front, its count against the per-actor
  bound and in `wasmkeep_front_calls_total`. The calls a node's fronts
  have waiting at once are now at most their slots (`front busy: too
  many front calls waiting on this node`), so batches hold no more actor
  instances than single calls could. A batch of more than 16
  starts nothing and fails every entry with `failed("too many calls:
  ...")`; its requests together, and its responses together, are capped
  at 16 MiB (`too-large`). Two callees of one batch that call each other
  wait until the deadline (`timeout`). Rust: `calls::call_many` (also
  `wasmkeep_sdk::front::calls`); TypeScript: `actors.callMany`, which
  returns a `CallError` per failed entry instead of throwing. Other SDKs
  later. The package stays `@0.2.0`: built components do not import the
  interface and run unchanged; one that imports it needs this release.
  The todo-front examples gain `GET /api/team`, every user's count from
  one batch ([ADR 0025](docs/adr/0025-calling-several-actors-at-once.md)).
- **An actor's `actors.call` gives up before its own event is cut.** A
  call from an actor now fails with `timeout` while 5 s of the caller's
  event wall remain (a fifth of a wall shorter than 25 s), as a front's
  call does; before, it waited the whole wall from
  when it started, so the caller's event was always cut first and its
  client got the failed event's answer instead of what the guest made of
  the `timeout`. A batch's entries share that deadline, so the ones that
  answered keep their answers.
- **`wasmkeep cluster status` shows each node's load**: ACTORS, SOCKETS
  and EV/MIN (events per minute over the last minute or more) per live
  node, and ACTORS per app, summed over its live nodes; `actors`,
  `sockets` and `events_per_min` in `--json`, `null` when unknown. Nodes
  say it on a new last line of their heartbeat, which nodes of 0.3.0
  never read; a node that is not live, or of an earlier version, shows
  `-`.
- **Every hop between nodes names its app, and a node refuses another
  app's.** A forwarded request, a relayed WebSocket upgrade and a call
  between nodes carry `x-wasmkeep-app: tenant/app`, the sending node's
  app, covered by the signature on signed calls. A node answers a hop for
  an app it does not serve with 503, `Retry-After: 1`, "not this app's
  node", before reading its body, and the actor does not run. A client's
  header is never believed or passed on: it is removed from what clients
  send to actors and from what guests send, and never reaches an actor. `serve` without `--app` (classes given on
  the command line) runs as the implicit app `default/default`, and says
  so in its heartbeat (it said `default/` before); its keys are unchanged
  ([ADR 0022](docs/adr/0022-app-owned-state-and-the-one-time-migration.md)).
  Nodes of earlier versions send no app: see Upgrading below.
- **`actor move` names the actor's app**: `wasmkeep actor move --app
  acme/shop room/lobby --to node-c`. Without `--app`, the actor is written
  `default/CLASS/NAME`, of the implicit app; another tenant in front
  (`acme/room/lobby`) is refused with a message asking for `--app`. A
  target serving another app gets a warning
  ([moving one actor](docs/cluster.md#moving-one-actor)).
- **Metrics name the node's app, and `wasmkeep_info` its version.**
  Every series with a `class` label (`wasmkeep_events_total`,
  `wasmkeep_event_duration_seconds`) also carries `tenant` and `app`: the
  app the node serves, or `default/default` without `--app`. This changes
  those series' identity: selectors that match every label exactly, and
  recording rules or dashboards that list all of them, need `tenant` and
  `app` added; `sum by (class)` and selectors on some labels keep
  working. The new gauge `wasmkeep_info{version,tenant,app,node}`, always
  1, shows which version each node runs (`node` only in a cluster); other
  series are unchanged ([metrics](docs/monitoring.md#metrics)).
- **Series from zero, and rules for failover waits and refusals.**
  Counters whose labels take a fixed set of values are on the metrics
  page from the node's start, every series at 0:
  `wasmkeep_failover_waits_total`, `wasmkeep_bucket_requests_total`
  (`status="error"` included), `wasmkeep_event_cuts_total`,
  `wasmkeep_forwards_total`, `wasmkeep_signatures_refused_full_total`,
  `wasmkeep_websocket_owner_closes_total`,
  `wasmkeep_websocket_sweep_unreadable_total`,
  `wasmkeep_parked_releases_total` and the front's two counters, so
  `increase()` counts the first event. `wasmkeep_not_ready` has one series
  per reason, 1 for the reason the node is not ready and 0 for the others,
  where it had a single series only while not ready: count not-ready
  nodes with `count(wasmkeep_not_ready == 1)`, as the dashboard now does.
  `deploy/prometheus/alerts.yml` adds `WasmKeepFailoverWaitsFailed`
  (requests kept for a takeover that got a 503: `expired`, `over_cap`),
  `WasmKeepAdmissionRefused` (5 minutes of `capacity`, `overloaded` or
  `wrong_app` refusals) and `WasmKeepAdmissionUnavailable` (15 minutes of
  `unavailable`, which ticks briefly while nodes catch up with a deploy or
  a secrets change); the Grafana dashboard charts both in a new row
  ([alerts](docs/monitoring.md#alerts-worth-having)).
- **A build names its commit.** A build from a git checkout reports
  the release and its commit, `0.4.0+<commit>` (`wasmkeep --version`, its
  heartbeat, so the VERSION column of `cluster status`, and
  `wasmkeep_info{version}`); a build of a release's tag, or one without
  git (a source tarball, a container image built locally), reports the
  release alone. Before, every build reported the release alone. The usage report still sends the release version alone.
- **Clock skew, one figure.** The pages that gave the lease timings'
  tolerance as "a few seconds" now say what the code allows, as
  `cluster diagnose` does: 1 s between nodes, and between a node and the
  bucket.
- **Clearer command and start messages.**
  - The first `wasmkeep deploy` of a new app no longer prints that access
    rules now come with deploys; only an app deployed before they did is
    told, once.
  - Nodes log their access rules as one INFO line per start and per
    deploy (`access rules (deployed): classes open to outside requests:
    counter; existing actors only: hub`), instead of a WARN that every
    class is open and a line per restricted class. A node following an
    app that never deployed rules still warns.
  - `cluster status` shows `-` in STATE for a node that is not live
    (fenced, released or stale), as it already did for its load; LIVE
    says why. `state` is `null` in `--json` for such a node.
  - A node's load is read as each heartbeat is written, with its drain
    state of the same moment: ACTORS lags by at most one heartbeat (an
    actor just taken over shows at the next one), and a `drained` node no
    longer shows the actors it just handed over.
  - `cluster drain` says the node reports `draining`, then `drained` once
    its actors are handed over, and that a small drain may go straight
    to `drained`.
  - A cluster node logs `checking that no other process runs node id a;
    ready in about 15 s` when its start check begins, and `no other
    process runs node id a: ready to take actors` when it ends.
  - Log lines name an actor as its app does, with the app as a field of
    its own: `app=demo/shop actor=counter/alice`, instead of
    `actor=demo/counter/alice` (tenant, class and name), in the guest log
    and every other line about an actor. Log queries on `actor=` need the
    new form.
  - A command whose output goes to a closed pipe (`wasmkeep cluster
    status --json | head`) exits quietly with status 141 instead of
    panicking with "failed printing to stdout: Broken pipe".
  - `serve --app` started before the app's first deploy waits for it
    (`waiting for the first deploy of demo/shop (wasmkeep deploy --app
    demo/shop --bucket s3://demo ...): this node starts serving when one
    lands`), reading `current` every 3 s as for new versions, instead of
    exiting with "nothing is deployed for demo/shop yet". It neither
    listens nor writes a heartbeat until then; nodes and the first deploy
    may now start in any order.
  - A deploy that changes nothing says `demo/shop: nothing changed:
    version V is already current (deploy N)` (it wrote nothing and `N` is
    unchanged), and a rollback says `rolled back demo/shop: FROM -> TO
    (deploy N)`, instead of the line of a new deploy. From a project
    file, the no-op line replaces `nothing changed: the deployed version
    is the file's`. Scripts that read `version V is current` after a
    rollback need the new line.
  - The warning when a deploy or rollback removes the front of an app
    with secrets gives the two commands to run in full, with the bucket
    and app filled in: `wasmkeep secrets list --bucket B --app T/A --key
    KEY_FILE` and `wasmkeep secrets unset --bucket B --app T/A --key
    KEY_FILE --front NAME...`.
  - `cluster status` shows front requests per minute per node
    (FRONT/MIN; `front_per_min` in `--json`, and a new
    `front_per_min=<n|->` key on the heartbeat's load line, which
    earlier readers skip), and whether each app's deployed version has a
    front (FRONT, `yes` or `no`; `front` in `--json`). EV/MIN still
    counts actor events only.
  - `deploy --rollback` prints `rolling back demo/shop to V:` above the
    changes it lists, and its note that a project file may now describe
    another version goes to stdout with them, before the `rolled back`
    line, instead of to stderr.
  - `wasmkeep serve` and `dev` write their log without colour codes when
    stdout is not a terminal (a file, a pipe, journald), and honour
    `NO_COLOR`; on a terminal they keep their colours.
  - The log line for unset required values names both ways to stop
    requiring them: `requires` in `wasmkeep.toml`, or `--requires
    CLASS=` / `--front-requires` without a project file (deploy refuses
    those flags beside one), and gives `secrets set` with `--class CLASS
    NAME` or `--front NAME`.
- **A move of a room parked with open sockets waits for its record's
  release**: one whose release did not land was reported done, its
  request deleted, while the room stayed. Once it lands and the target
  takes the room, the sockets left on the old node close with 1012 at
  once, not at the next owner sweep. An ownership record that cannot be
  decoded no longer stops every owner sweep pass at that room: it is
  passed over, logged once and counted in the new
  `wasmkeep_websocket_sweep_unreadable_total{read}`. A lapse or a clean stop no
  longer writes a second release for each parked room whose sockets it
  closed ([ADR 0024](docs/adr/0024-socket-home.md)).
- **A drain or a move no longer waits until the node stops for an actor
  whose ownership write went unanswered and landed late**: the node now
  watches such a record for 30 s and releases it if the write lands
  ([ADR 0020](docs/adr/0020-one-lease-per-node.md#3-the-ownership-record),
  amended 2026-11-14).

### The front, host side

The stateless component given to `serve` becomes the app's front
([ADR 0023](docs/adr/0023-the-front.md)). It ships with the app's deploy:
see the next section.

- **The front calls actors.** The component is linked with `actors` and
  `env` besides WASI and `wasi:http` (the new WIT worlds `front-core` and
  `front`; the package stays `0.2.0` and world `actor` is unchanged, so
  built actors and built stateless components run as they are). Its
  `actors.call` reaches the `--actor` classes as an inside caller:
  `internal` classes included, and the callee sees `x-wasmkeep-caller:
  default/_front/main`. Without `--actor`, its calls fail with
  `invalid-address`. Calls to an actor on another node are signed hops, as
  an actor's.
- **`serve --front-env FILE`** gives the front its values (`env.get`), as
  `--env CLASS=FILE` does for a class; `--env _front=FILE` is refused.
- **Its own limits.** `--max-front-requests N` front requests at once (by
  default a tenth of `--max-instances`, rounded up, beside actors, and the
  whole pool on a node that serves only a component): past it, 503 with
  `Retry-After: 1`, without running the front; resident actors get the
  rest of the pool. A quarter of those slots may wait on one actor at
  once: past it the call fails with `failed("front busy: ...")`. A call
  from the front gives up with `timeout` while 5 s of its 30 s wall remain.
  The front's CPU budget is its own flag, `--front-cpu-budget-ms` (50 ms by
  default); `--cpu-budget-ms` is the actors' only.
- **Metrics**: `wasmkeep_front_requests_total{tenant,app,outcome}`,
  `wasmkeep_front_request_seconds`, `wasmkeep_front_slots_in_use` and
  `wasmkeep_front_calls_total{tenant,app,outcome}`
  ([metrics](docs/monitoring.md#metrics)); a client that leaves before
  the end of its answer counts as `outcome="client_gone"`.

Behaviour changes:

- With `--app`, a component on the `serve` command line is refused at
  start; such a node answers 404 outside `/actor/` until its deploy brings
  a front.
- Without `--app`, the component gets `actors` and `env`.
- A client's `x-wasmkeep-*` headers and trailers no longer reach the
  stateless component, and its own `x-wasmkeep-*` response headers and
  trailers no longer reach the client. The host removes them silently:
  the component may set them without an error.
- A client's `x-wasmkeep-*` headers no longer reach an actor at
  `/actor/...`, the SDKs' key-value and queue names included (before,
  only the names the host reads were removed); the forwarded mark still
  does.
- An answer to `actors.call` no longer carries any `x-wasmkeep-*` header
  (such as `x-wasmkeep-node` when another node answered), nor the hop's
  framing headers (`transfer-encoding`, `connection` and the like; the
  body is whole, and `content-length` stays), for actors too.
- A client that leaves before the end of its answer stops the stateless
  component's instance and frees its slot; a whole answer (its declared
  length sent, its trailers, or no body: `HEAD`, 204, 304) lets it run
  on, up to its wall.
- No free instance for the stateless component answers 503 with
  `Retry-After: 1`, no longer 500.
- `serve --app --max-instances 1` is refused at start: an `--app` node
  keeps slots for the front its deploy brings, beside an actor.
- The stateless component's CPU budget is `--front-cpu-budget-ms`, no
  longer `--cpu-budget-ms`.
- A guest's own `x-wasmkeep-*` headers on `actors.call` no longer reach
  the callee, for actors too (before, a local call passed a guest-set
  `x-wasmkeep-forwarded` or `x-wasmkeep-node` on unchanged), except the
  four the SDKs' key-value and queue clients send
  (`x-wasmkeep-kv-expect`, `x-wasmkeep-kv-ttl-ms`, `x-wasmkeep-kv-durable`,
  `x-wasmkeep-queue-delay-ms`).
- `/actor/...` no longer reaches a stateless component on a node without
  actors: it answers 404.
- A `1xx` status from the stateless component becomes a 500.
- The guest log names the stateless component `actor=_front` instead of
  `actor=stateless`.

### The front in deploys

An app's front ships in its version, with the classes
([ADR 0023](docs/adr/0023-the-front.md),
[deploying the front](docs/cluster.md#deploying-the-front)).

- **`wasmkeep deploy --front PATH`** sets the front, `--no-front` removes
  it, and without either the current one is kept (copied inside the
  bucket). `--front-config FILE` and `--front-requires NAMES` are its
  `--config` and `--requires`, kept by later deploys, removed with an empty
  value. `--rollback` conflicts with the four, and brings back the
  previous version's front with its classes. A first deploy may give a
  front and no class.
- **Checked before anything is written**: the front must link against the
  front world, export the incoming handler and instantiate once (without
  values, required names or not); one whose start function traps or that
  needs more than the limits, one that imports `storage`, `alarms` or
  `websockets`, or one that exports an actor's `on-*` functions, is
  refused. `--config _front=...`, `--requires _front=...` and `--access
  _front=...` are refused too, naming the front's own flags.
- **The front's secrets stay when it goes**: `--no-front`, or a rollback
  to a version without a front, warns while the app has secrets that its
  `_front` entries remain (`wasmkeep secrets unset --front NAME...`
  removes them): a node older than the front cannot open a set with them,
  and a later front would read them.
- **`deploy` prints** `front: added`, `changed`, `removed` or `unchanged`,
  the front's values by name (`config _front: NAME added`), and each open
  class as `reachable at /actor/<class>/<name>, past the front`.
- **Nodes following the app (`serve --app`)** load the version's
  `_front.wasm` (digest checked), link it before taking any lock, and swap
  it in with the classes; a front that does not link is a version the node
  cannot apply (it keeps the previous one and reports not ready). Without
  a front, paths outside `/actor/` answer 404.
- **Its values** come from the same three layers as a class's: the
  deployed `--front-config`, the app's secrets in the new scope `_front`
  (`wasmkeep secrets set --front NAME`, `unset --front NAME`, `_front
  <name> <length>` entries in `set --file`, shown by `secrets list`), and
  the node's `--front-env`.
- **Required values**: while a name in `--front-requires` is not set, or
  the node cannot open the app's secrets, the front is not run and the
  node answers a generic 503 with `Retry-After: 1`; the missing names go
  to the log once and to the `unconfigured` outcome of
  `wasmkeep_front_requests_total`, never to the client.
- **Bucket**: a version with a front is written as manifest format 3
  (`front main <sha256>`, `config _front`, `requires _front`; it also
  accepts an explicit `access <class> open`), with `_front.wasm` and
  `_front.config` beside the classes, collected with their version.
  Versions without a front keep format 1 or 2, the same bytes and ids.
- **Heartbeats** announce the capability word `front` beside `take`. A
  deploy or rollback making a format 3 version current, and a secrets
  `set` or `reset` that gives the set its first `_front` entry, are
  refused while a live node of the app lacks it (for secrets, one that
  reads them), with no `--force`: such a node could not read the version,
  or any of the app's secrets.

### App-owned state

These change the layout in the bucket and on disk; they supersede the
second-app refusal and the migration guard listed under 0.3.0. See
"Upgrading to 0.4.0" before starting a node of this version
on a bucket that holds actor state.

- **Each app's actor state lives under its own root,
  `state/<tenant>/<app>/`**, beside `apps/<tenant>/<app>/`: history,
  ownership records, old-lease markers, the alarm index and move requests
  (`state/<t>/<a>/{actors,owners,leases,alarms,moves}/`). Each key keeps
  its tail, byte for byte; only its front changes. On disk the databases
  move to `<data_dir>/state/<t>/<a>/actors/`. Nodes given classes on the
  command line (`serve --actor`, `dev`) keep theirs under the implicit app
  `default/default`
  ([ADR 0022](docs/adr/0022-app-owned-state-and-the-one-time-migration.md)).
- **Two apps of one tenant share a bucket prefix safely.** The same class
  and actor name in two apps are two actors. `deploy` no longer refuses a
  second app of a tenant, `deploy` and `serve` no longer warn about apps
  sharing a prefix, and a node no longer refuses to start next to a node
  of another app. A node still refuses to start next to a live node of
  the version before (its heartbeat names `tenant/`).
- **`wasmkeep cluster migrate`** moves a bucket prefix written by 0.2 or
  0.3 into the new layout, once, while every node is stopped. Without
  `--apply` it is a dry run that writes nothing: it prints the app each
  class goes to, the nodes it sees and whether they may still run, object
  counts and an estimate. A class goes to the one app whose current
  version deploys it; a class in no manifest goes to the tenant's only
  app, or to `default/default` for the tenant `default` with no app (or
  only an app named `default`); a class two apps deploy, a tenant with
  several apps, or the tenant `default` with another app needs `--heir
  TENANT/APP`, which must name an app of the tenant (or
  `default/default`); the other apps start a shared class empty.
  `--apply` waits one node-lease window (16 s with the default timings,
  so that a node whose clock disagrees with the migrator's shows itself),
  copies server-side, verifies, then puts barriers in front of older
  binaries (token `3` in every app's `current`, a tombstone over every old
  ownership record, and one where an actor has history but no record),
  proves that no node ran, and commits each tenant
  (`cluster/migrated/<tenant>`). Move requests are not copied: a copy
  would renew them. It refuses while a node may still run and fences
  nothing; after a crash, run it again. Two runs at once stop each other
  before either commits a state the other undid. `--rollback` undoes it
  while no node of any version has started and no app was deployed
  since; after that, it is one-way. Nothing of the old layout is deleted
  by it. Exit codes: 0 done, 1 refused or aborted, 2
  partly committed ([upgrading to 0.4.0](docs/deploy/upgrading-to-0.4.md)).
- **`wasmkeep cluster migrate --remove-old`** deletes what a migration left
  in the layout before 0.4.0, for every migrated tenant: a dry run that
  lists it, then `--remove-old --apply`. It keeps the tombstones over the
  old ownership records for good, history younger than
  `--history-retention-secs` (3600 by default) and move requests not yet
  expired (5 minutes) for a later run, and never touches `state/`,
  `apps/`, `nodes/` or a tenant with no commit. It refuses while a run of
  migrate is unfinished, while the rollback would still work (no node
  started since; `--forgo-rollback` gives it up), and on any old-layout
  key written after the commit, an old record that is not a tombstone or
  history with no tombstone (a binary before 0.4.0 ran on the prefix),
  naming each key. Before its first delete it writes
  `cluster/removed/<tenant>`, after which `--rollback` refuses for good;
  a tenant with nothing to delete gets none. It stops, deleting nothing,
  if a `--rollback` or `--apply` ran meanwhile. It can be run again after
  a crash.
- **`actor move` runs the start check `serve` runs**: it refuses a tenant
  whose state is still in the layout before 0.4.0, printing the migrate
  command, instead of writing a request no node reads.
- **`wasmkeep_admission_refused_total` counts wrong-app hops** under their
  own reason, `wrong_app`: a hop from another node or a forwarded request
  naming another app (503 "not this app's node"), or no app (a node of an
  older version). The page now has five series.
- **`serve` and `deploy` of this version check the bucket before they
  write there**: they refuse while `cluster/migration` exists, and refuse
  a tenant whose state is still in the layout before 0.4.0, printing the
  command to run. Other commands that write to the bucket (`secrets`,
  `actor move`, `cluster drain`, `fence`, `forget`) refuse while
  `cluster/migration` exists, or when they cannot tell. `cluster status`
  reads on.
- **Token `3` in `current`.** Every deploy and rollback of this version
  writes it, as does `cluster migrate`; it means the app is marked (ADR
  0013: rules come with deploys, nodes refuse `--access` with `--app`)
  and its state is rooted. `serve --app` refuses a `current` without it:
  run `cluster migrate --apply`, or deploy once with this version.
  Versions before 0.4.0 cannot read it: their `serve --app` and `deploy`
  stop.
- **A node moves its local databases once, at start.** In a cluster the
  bucket is the truth: `<data_dir>/actors/<tenant>` goes aside to
  `<data_dir>/actors.pre-0.4/<tenant>`, and each actor restores from its
  new root on its first wake. Alone (`dev`, `serve` without a cluster) the
  local copy is the only one: it becomes the app's, if no app of the
  tenant has local state yet; otherwise the start is refused. One rename,
  never a copy; a file is left at the old path so an older binary fails
  instead of starting empty. Both folders present is refused, never
  merged.
- **Moves** are listed under the node's own root; a move written by an
  older `actor move` (`control/move/...`) is not read, and not migrated:
  request it again after the upgrade.

### Upgrading to 0.4.0

From 0.3.0, on a bucket prefix that holds actor state (a prefix last run
by 0.2.0 has the same layout and takes the same steps; read the notes
under 0.3.0 for the rest of that upgrade):

1. Freeze deploys, secret changes, `actor move` and drains (CI
   included).
2. Stop every node: the service units, not just the processes. Then back
   up the bucket.
3. Install 0.4.0 on every node, without starting it.
4. `wasmkeep cluster migrate --bucket s3://BUCKET/PREFIX`: read the plan;
   give `--heir` where it asks.
5. Run it again with `--apply` (and the same `--heir`).
6. Start every node, and check the app.
7. The same day: `cluster migrate --remove-old`, then `--remove-old
   --apply`.

The whole procedure is in [upgrading to
0.4.0](docs/deploy/upgrading-to-0.4.md). **One-way after the first
start.** Until a node of any version starts, `cluster migrate
--rollback` puts the old layout back for the older binaries. Once one
has started, the migration is one-way; the old keys stay in the bucket
until `--remove-old --apply`. Keep a backup from before the migration
until then. Lifecycle rules, access policies and backup
scripts written for `actors/<tenant>/` must move to `state/<t>/<a>/`.
Nodes that followed an app with `--access` and no deployed rules must
deploy those rules (`deploy --access`) and start without `--access`.

With or without actor state:

- Stop every node of a cluster, upgrade them all, start them all
  ([upgrading](docs/cluster.md#upgrading-wasmkeep)): nodes of this
  version refuse every hop from a node of an earlier one (a forwarded
  request, a relayed upgrade or a call, which carries no app) with a 503
  saying it came from a node of an older version. The other way, a node
  of an earlier version fails the signature of this version's calls:
  calls to `internal` classes, moves and alarm hand-offs fail, and other
  calls run there as requests from outside, with no calling actor.
- A node of this version started while a 0.3 node given classes on the
  command line is still live on the bucket prefix (its heartbeat names
  `default/`) refuses to start, and asks to stop every node and upgrade
  them together; so does a 0.3 node started next to a live node of this
  version, though its message speaks of another app of the tenant.
- `wasmkeep actor move TENANT/CLASS/NAME` becomes `wasmkeep actor move
  --app TENANT/APP CLASS/NAME` for an actor of a deployed app; scripts
  that move actors of `default/default` keep working.
- Components built for 0.1.0 to 0.3.0 need no rebuild. The SDKs are at
  `0.4.0` on their registries, with the server. Actors built with the
  Rust SDK keep its default features; a Rust front depends on
  `wasmkeep-sdk` with `default-features = false`.
- Dashboards, recording rules and exact selectors on the series with a
  `class` label add `tenant` and `app`; log queries on `actor=` use
  `app=TENANT/APP actor=CLASS/NAME` (both under What changed).

### Known limits

The limits listed for 0.3.0 still hold. Also:

- The front cannot yet accept a WebSocket at its own path and hand it to
  an actor: a room is still reached at `/actor/<class>/<name>` (planned
  for 0.4.1, [roadmap](ROADMAP.md#now)).
- Fronts and `call-many` are in the Rust and TypeScript SDKs only; the
  other SDKs build actors as before.

### Install and verify

[Install](README.md#install) downloads a binary or runs the image.
[Single node](docs/deploy/single-node.md#1-install-the-binary) shows the
cosign check, the same for every file of the release;
[Compose](deploy/compose/README.md) checks the image.

## 0.3.0

**Reliable enough to leave running.** This release fixes the known ways
a cluster stalled or lost an answer when a node or the bucket misbehaved,
keeps a request for a dead node's actor until the takeover instead of
refusing it, and adds the guides that running an application on a cluster
showed missing. Still a **pre-alpha pre-release**: for trying WasmKeep,
side projects and staging, not production. The interface between an actor
and the host is unchanged (`wasmkeep:actor@0.2.0`), and so are the SDKs'
APIs: a component built for 0.1.0 or 0.2.0 runs as it is. See the
[roadmap](ROADMAP.md) for what comes next.

Soaked on three nodes against a real bucket, a node killed every three
minutes: twelve hours on a release candidate (239 kills, failover p99
22.43 s, no acknowledged write lost or duplicated, no alarm missed;
`bench/results/2026-11-12-b849bfe-soak-ovh-bhs-3vps-12h.csv`), then three
hours on this release's code (59 kills, failover p99 20.9 s, the same
result; `bench/results/2026-11-13-648c2d1-soak-ovh-bhs-3vps-3h.csv`).

### What changed

- **A request waits for a takeover.** When an actor's node dies, a GET or
  HEAD without a body, or a WebSocket upgrade, for one of its actors is
  kept by the node that received it until the actor can be taken over (15
  to about 21 s after the dead node's last write, unchanged), then served,
  instead of being answered 503 meanwhile. A request with a body, an
  actor-to-actor call and a forwarded request still get the 503 at once,
  which still means it did not run. A client that leaves while its
  request is kept is dropped, and its request never runs. At most a
  quarter of the node's connection limit, and 64 per actor, are kept;
  beyond that, the 503 at once. `--failover-wait-secs` sets the wait
  (about 25 s by default, from the lease timing); 0 turns it off. Set the
  load balancer's server timeout above it.
  `wasmkeep_failover_waits_total` counts how each wait ended
  ([what a caller sees](docs/cluster.md#when-a-node-dies-what-a-caller-sees),
  [front door](docs/deploy/front-door.md)).
- **A snapshot upload is bounded, tried again after a failure, and
  counted.** The snapshot an actor's owner adds every 64 writes holds the
  actor while it uploads. The upload had no bound of its own, so a bucket
  that left it unanswered held the actor until the bucket client ended the
  request (30 s by default). It now gets one attempt, given up after 5 s
  plus a second per megabyte, 10 s at most. A snapshot that fails costs no
  write and fails no event, and is tried again once the actor has made one
  more write and 30 s have passed, then 8 writes later, then every 64; a
  failure used to put the next snapshot 64 more writes away. The node
  keeps these waits while an idle actor is out of its memory.
  `wasmkeep_snapshot_upload_failures_total` counts the snapshot uploads
  that were not acknowledged
  ([replication](docs/cluster.md#replication),
  [monitoring](docs/monitoring.md)).
- **A restore's reads have one deadline, a refused read is sent again,
  and failed restores are counted.** Restoring an actor from the bucket
  bounded each read but not the reads together, so reads that each stayed
  under their limit added up with the length of the history while every
  caller of the actor waited. The reads of one restore now end within
  30 s, plus a second per megabyte of what it reads and a second per
  eight objects. The clock runs only while the restore waits for the
  bucket to give it the object it applies next: the listing, work on the
  local disk and the wait for a turn among the node's other restores are
  not counted. Neither are the seconds that other restores' reads on the
  node add to the limit of a read (a second per megabyte in flight, 256 at
  most), so restores that read side by side after a takeover are not
  ended for each other's bytes; a restore alone on the node has no more
  than its own time. Running out of time is answered like a bucket that
  does not answer, a 503 with `Retry-After`, and leaves the local file as
  it was. A read the bucket refuses with an error a resend can get past
  (a 5xx or a broken connection, after the bucket client's own retries)
  used to fail the whole restore; it is now sent again after 0.5 s, then
  1 s, three tries per object. `wasmkeep_restore_failures_total` counts
  the restores that did not complete
  ([replication](docs/cluster.md#replication),
  [monitoring](docs/monitoring.md)).
- **A request body the handler left unread no longer costs the upload or
  the answer.** A handler may answer without reading the body. The server
  now reads and discards what is left, up to the body cap and for at most
  30 s, so a large upload is no longer cut off and, when the request came
  through another node, its answer is no longer lost. A body held back
  with `Expect: 100-continue` is not asked for, and a node that forwards a
  request does not pass `Expect` on ([concepts](docs/concepts.md)).
- **Between nodes, a WebSocket upgrade the owner never read is sent once
  more, and a forwarded request that was written is never sent twice.**
  An upgrade relayed to the actor's node that the node provably never read
  (it refused the TLS handshake, or the upgrade came back unsent on a new
  TLS connection) is sent once more while the client waits; it used to
  get a 503 asking the client to retry. Between nodes, the only other
  thing sent a second time is an `actors.call` the peer provably never
  read in the same way, once, as 0.2.0 already did after a refused
  handshake. A forwarded request is never sent again, whether its
  connection could not be opened or it was written into a connection that
  then failed: one that was written may have run on the owner, so the
  client gets a 503, which does not say whether it ran
  ([running a cluster](docs/cluster.md)).
- **A client that only listens no longer misses a room's messages
  without a close.** A room that hibernates keeps its WebSockets open on
  its node and releases its ownership record. When a request then woke it
  on another node, its sockets on the first node closed only one by one,
  each when its client next sent a message: a client that only listens
  stayed connected and silently missed everything the room said from then
  on. Now every socket of the room on that node closes with 1012 as soon
  as one of them sends, and an owner sweep, once a minute, closes the
  sockets of rooms another node took even if none of them sends. The
  sweep reads at most 5,000 records a pass, counted in
  `wasmkeep_websocket_sweep_reads_total`. A node's sockets also close with
  1012 when its lease runs out (8 s without a write reaching the bucket,
  with the defaults), before any other node can take its actors; nothing
  closed them before. A drain closes the sockets of hibernated rooms, and
  reports `drained` only once the node holds no socket. Each such close is
  counted in `wasmkeep_websocket_owner_closes_total`
  ([running a cluster](docs/cluster.md), [monitoring](docs/monitoring.md)).
- **A WebSocket the server closed stops at the close.** Its client used to
  keep it open by not answering the close, and its messages still ran
  events; a node draining waited on it forever. Now nothing it sends after
  the close is delivered, and it is dropped if its client has not answered
  within 5 s.
- **An idle WebSocket is pinged after 25 s** of silence from its client,
  instead of 60 s, so a load balancer's usual idle timeout of 60 s no
  longer cuts it first. The front door needs an idle timeout above 30 s
  ([front door](docs/deploy/front-door.md)).
- **A relayed WebSocket upgrade carries no `x-wasmkeep-*` header from the
  client.** The relay to the actor's node removed a fixed list of node
  headers; it now removes every header in that range, so a client cannot
  pass a marker only nodes may set.
- **Take and alarm hand-off calls between nodes keep their own record of
  used signatures**, so a flood of other node calls that fills the shared
  record (500,000 signatures at once) can no longer refuse the calls a
  drain and a move depend on. The take and hand-off record holds 50,000.
  Refusals because a record is full are counted in
  `wasmkeep_signatures_refused_full_total` and logged as a full record
  rather than as a replay.
- **A database set aside as `<name>.db.replaced` no longer has an older
  WAL beside it.** When a node replaces a local database with the bucket's
  copy and keeps the old file, a replacement without a WAL left the
  previous replacement's WAL next to the newer copy, where SQLite would
  apply it if the copy were opened for recovery. The previous copy's WAL
  and shared memory are now removed first.
- **`wasmkeep_admission_refused_total{reason}`** counts the requests,
  WebSocket upgrades and actor calls a node refused before their event
  ran: `access`, `capacity`, `overloaded` or `unavailable`
  ([monitoring](docs/monitoring.md#metrics)).
- **`deploy` refuses a second app of a tenant on a bucket prefix.**
  Actors are keyed by tenant, class and name, so a second app would share
  the first app's actors without a word; until now only a node refused to
  start, and only while a node of the other app was live. The first deploy
  of an app now lists `apps/<tenant>/` and refuses when another app there
  has a `current`, naming it and saying what to do: another prefix,
  another tenant when another owner runs the app, or, for an abandoned
  app, the keys to delete. Two apps of one tenant already deployed on one
  prefix are left as they were (a node of one still refuses to start while
  a node of the other is live), and `deploy` and `serve --app` warn about
  them by name; a third is refused. This lasts until apps own their state
  (planned for 0.4.0, [deploying](docs/cluster.md#deploying)).
- **A new app's tenant and app names follow the class grammar**,
  `[a-z][a-z0-9-]{0,63}`: the first `wasmkeep deploy` of an app refuses
  others and writes nothing. Actor addresses already required it of the
  tenant; the app name had no rule. An app deployed before keeps its name,
  and every later deploy and rollback of it works as before.
- **A bucket a later version migrated is refused.** Every command that
  opens a bucket first checks for the markers a later version writes while
  it moves actor state to a new layout (`cluster/migration`,
  `cluster/migrated/`), and stops with a message naming the marker. One
  read and one listing per command, bounded to 10 s.
- **The app model, in two decisions.**
  [ADR 0021](docs/adr/0021-the-app-model.md): an app is `tenant/app`, the
  unit of deploy, rollback, secrets and (from 0.4.0) state; the tenant is
  the owner of apps; 0.4.0 plans a front deployed with the app, app-owned
  state and classes `internal` by default with a project file, and later
  shared nodes for trusted tenants.
  [ADR 0022](docs/adr/0022-app-owned-state-and-the-one-time-migration.md):
  the layout of app-owned state and the one-time migration to it, planned
  for 0.4.0.
- **Guides for running a cluster**: what happens when a node dies and how
  a cluster scales ([failover and scaling](docs/failover-and-scaling.md));
  backing up and restoring a cluster's bucket
  ([backup and restore](docs/deploy/backup-and-restore.md)); the alerts
  [monitoring](docs/monitoring.md) recommends as a Prometheus rules file,
  [`deploy/prometheus/alerts.yml`](deploy/prometheus/alerts.yml); and an
  outbound tunnel in front of the nodes
  ([front door](docs/deploy/front-door.md#an-outbound-tunnel)).
- **Releases**: the npm job of the release workflow passed the tarball as
  a bare path, which npm read as a GitHub repository; it now publishes the
  file. See the correction under 0.2.0.

### Upgrading from 0.2.0

- Upgrade every node of a cluster together, as before
  ([upgrading](docs/cluster.md#upgrading-wasmkeep)). The formats in the
  bucket are unchanged.
- Components built for 0.1.0 or 0.2.0 need no rebuild. The SDKs are at
  `0.3.0` on their registries, with the server; their APIs are those of
  0.2.0.
- The front door: a server timeout above the failover wait (about 25 s by
  default) and an idle timeout above 30 s
  ([front door](docs/deploy/front-door.md)). `--failover-wait-secs 0`
  answers 503 at once, as 0.2.0 did.
- The first deploy of a new app refuses a tenant or app name outside
  `[a-z][a-z0-9-]{0,63}`, and a second app of the same tenant on the same
  bucket prefix. Apps already deployed keep deploying and rolling back.

### Known limits

The limits listed for 0.2.0 still hold:

- Not production-ready: soaked against Cloudflare R2 and OVHcloud Object
  Storage with nodes killed under load, nothing lost, but not yet carrying
  real users' traffic, and not run against AWS S3
  ([measured](docs/benchmarks.md#three-nodes-against-a-real-bucket)).
- Nodes of different versions are not supported in one cluster; one
  tenant per process ([security](docs/security.md#known-gaps)); no
  independent third-party security audit yet
  ([security](docs/security.md#audits)).
- Binaries are for Linux; on macOS and Windows, use the image or build
  from source. The Zig SDK is an archive, not yet a package `zig fetch`
  resolves; D and MoonBit are used from the source.

### Install and verify

[Install](README.md#install) downloads a binary or runs the image.
[Single node](docs/deploy/single-node.md#1-install-the-binary) shows the
cosign check, the same for every file of the release;
[Compose](deploy/compose/README.md) checks the image.

## 0.2.0

**The installable release.** With 0.1.0 the SDKs were used from a clone of
the repository; from this release each comes from where its language
expects it, and trying WasmKeep needs no toolchain. Still a **pre-alpha
pre-release**: for trying WasmKeep, side projects and staging, not
production. The interface between an actor and the host is unchanged
(`wasmkeep:actor@0.2.0`, a version of its own), so a component built for
0.1.0 runs as it is. See the [roadmap](ROADMAP.md) for what comes next.

### What is new

- **The SDKs install from their registries**, each at the server's
  version, `0.2.0`:

  | Language | Install | Then |
  | --- | --- | --- |
  | Rust | `cargo add wasmkeep-sdk` | [crate](crates/wasmkeep-sdk/README.md), [tutorial](docs/tutorials/first-actor.md) |
  | TypeScript | `npm install @wasmkeep/sdk` | `npx wasmkeep-ts build` ([package](sdks/typescript/README.md)) |
  | Python | `pip install wasmkeep` | `python -m wasmkeep build`, new in this release ([package](sdks/python/README.md)) |
  | Go (TinyGo) | `go get github.com/wasmkeep/wasmkeep/sdks/go@v0.2.0` | [module](sdks/go/README.md) |
  | C, Zig | `wasmkeep-c-sdk-0.2.0.tar.gz` and `wasmkeep-zig-sdk-0.2.0.tar.gz`, attached to this release | [C](sdks/c/README.md), [Zig](sdks/zig/README.md) |

  The crate, the npm package and the wheel are published from this
  release's tag once a maintainer approves each, so a registry can be a
  little behind this page. The server and every SDK carry one version;
  use an SDK with the server of the same version
  ([versions](docs/languages.md#versions)).
- **Prebuilt components**: the release carries `counter.wasm`, `kv.wasm`,
  `queue.wasm`, `workflows-orders.wasm` and `workflows-ledger.wasm`, built
  from the examples and signed like its other files. The
  [quickstart](README.md#quickstart) is now a download and one command,
  and the callers of the [key-value, queue](docs/kv-and-queues.md) and
  [workflow](docs/workflows.md#the-workflow-class) classes in other
  languages need no Rust toolchain. The notices of the libraries compiled
  into them are in `THIRD_PARTY_NOTICES-components.txt`. The binary's
  tarball has a short README of its own.
- **The C and Zig SDKs as archives**, signed like the other files: each
  SDK with the C bindings already generated, the WIT and the componentize
  script, and a README whose build command works in the extracted
  directory, without a clone or wit-bindgen. `WASMKEEP_SDK_VERSION` (C)
  and `wk.version` (Zig) carry the version.
- **A durable write on a cluster node is no longer synced to the local
  disk as well as uploaded.** `exec-durable` answers once the bucket holds
  the write, as it was documented to: one disk sync less on every durable
  write. The bucket is the only durable copy of such a write, and a node
  trusts a local file again only after checking it against the bucket. For
  whoever runs a cluster: start a node again on the same bucket and
  prefix, since after a crash or a power loss its local files may lack
  writes the bucket has ([replication](docs/cluster.md#replication)). A
  node without a bucket still syncs to its disk.
- **Languages**: Go through TinyGo moves from experimental to supported,
  with a full [Go SDK](sdks/go) and its counter, chat room and jobs app. D
  and MoonBit are experimental, each with an SDK, a counter and a chat
  room; a C# counter (componentize-dotnet) and a standard-Go counter
  (componentize-go) pass the shared tests.
  [Tiers and numbers](docs/languages.md).
- **Compared with celld**: the same counter on one node and on three, a
  node killed under writes, with a check for lost writes that gives every
  write a token of its own, repeats the takeovers with the killed node's
  disk removed, and includes a run that must lose writes
  ([WasmKeep and celld](docs/compared-with-celld.md),
  [benchmarks](docs/benchmarks.md#three-nodes-wasmkeep-against-celld)).
- **How releases are made**: a tag's release waits for CI to pass on the
  tagged commit (0.1.0 was tagged before its run had finished), and a
  version's image tag is written once. Publishing the release publishes
  the SDKs to crates.io, npm and PyPI from its tag, without stored tokens,
  each after a maintainer's approval ([releasing](docs/releasing.md)).

### Upgrading from 0.1.0

- Upgrade every node of a cluster together, as before
  ([upgrading](docs/cluster.md#upgrading-wasmkeep)). The formats in the
  bucket and the command line are unchanged.
- Components built for 0.1.0 need no rebuild. To move an actor's source
  to a published SDK, replace the path into a clone with the registry's
  package at `0.2.0`.
- The Go module's `v0.1.0` has only the key-value, queue and workflow
  clients; the `wasmkeep` package (hooks, storage, alarms) starts at
  `v0.2.0`.

### Known limits

The limits listed for 0.1.0 still hold:

- Not production-ready: soaked against Cloudflare R2 and OVHcloud Object
  Storage with nodes killed under load, nothing lost, but not yet carrying
  real users' traffic, and not run against AWS S3
  ([measured](docs/benchmarks.md#three-nodes-against-a-real-bucket)).
- Nodes of different versions are not supported in one cluster; one
  tenant per process ([security](docs/security.md#known-gaps)); no
  independent third-party security audit yet
  ([security](docs/security.md#audits)).
- Binaries are for Linux; on macOS and Windows, use the image or build
  from source. The Zig SDK is an archive, not yet a package `zig fetch`
  resolves; D and MoonBit are used from the source.

### Correction

Added after the release; the text above is as it was published.

- The npm package `@wasmkeep/sdk@0.2.0` was published without a
  provenance statement. The text above says the SDKs are published from
  the release's tag by the release workflow; for npm that did not happen:
  the workflow's npm job failed before it reached the registry, and the
  package was published from a maintainer's account instead. Later
  versions will be published by the workflow, with a provenance
  statement.

### Install and verify

[Install](README.md#install) downloads a binary or runs the image.
[Single node](docs/deploy/single-node.md#1-install-the-binary) shows the
cosign check, the same for every file of the release;
[Compose](deploy/compose/README.md) checks the image.

## 0.1.0

The first release, published as a **pre-alpha pre-release**: for trying
WasmKeep, side projects and staging, not production. See the
[roadmap](ROADMAP.md) for what comes next.

### What is in it

- **Actors**: WebAssembly components (`wasmkeep:actor@0.2.0` WIT world),
  one instance per name, events one at a time, a SQLite database each,
  hibernation, alarms that fire at least once, calls between actors,
  WebSockets that stay open while an actor sleeps, outbound HTTP limited to
  public addresses, and hot reload with `wasmkeep dev`.
- **Clusters**: several nodes on one S3-compatible bucket with conditional
  writes, each actor on one node at a time, its database replicated to the
  bucket and restored on another node when its node dies (free 15 to about
  21 s after the node's last write, then the restore). One lease per node,
  checked in deterministic simulation. Mutual TLS between nodes, required
  by default.
- **Operating a cluster**: `wasmkeep deploy` (rolling, with rollbacks,
  access rules and configuration), `wasmkeep cluster status`, `drain`,
  `fence` and `forget`, `wasmkeep actor move`, `wasmkeep secrets` (sealed
  in the bucket), garbage collection of old history in the bucket,
  Prometheus metrics, health and readiness endpoints, and a Grafana
  dashboard.
- **Languages**: Rust (recommended); Zig and C/C++; TypeScript (QuickJS
  by default, StarlingMonkey as an option) and Python; Go through TinyGo
  (experimental). [Tiers and numbers](docs/languages.md).
- **SDK patterns on actors**: sharded key-value store, queues and durable
  workflows ([KV and queues](docs/kv-and-queues.md),
  [workflows](docs/workflows.md)).
- **Packaging**: binaries for x86_64 and aarch64 Linux (glibc 2.28 or
  newer), each with a CycloneDX SBOM and third-party notices; a
  distroless image for linux/amd64 and linux/arm64 at
  `ghcr.io/wasmkeep/wasmkeep:0.1.0`; all signed with keyless cosign. A
  three-node Compose example against MinIO in
  [`deploy/compose/`](deploy/compose/README.md).

### Known limits

- Not production-ready: soaked against Cloudflare R2 and OVHcloud Object
  Storage with nodes killed under load, nothing lost, but not yet carrying
  real users' traffic, and not run against AWS S3
  ([measured](docs/benchmarks.md#three-nodes-against-a-real-bucket)).
- Upgrade every node of a cluster together: nodes of different versions
  are not supported in one cluster
  ([upgrading](docs/cluster.md#upgrading-wasmkeep)).
- One tenant per process ([security](docs/security.md#known-gaps)).
- No independent third-party security audit yet
  ([security](docs/security.md#audits)).
- The performance targets on dedicated hardware are not measured yet;
  millions of actors per cluster are a goal, not a measurement.

### Install and verify

[Single node](docs/deploy/single-node.md#1-install-the-binary) checks the
binaries with cosign; [Compose](deploy/compose/README.md) checks the image.
