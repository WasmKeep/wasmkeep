# 0013. Access rules and plain configuration travel with deploys

- Status: accepted and implemented (2026-11-05; steps 2 to 4 of the plan, tests in
  `apps.rs`, `tests/deploys.rs`, `tests/cluster/`, the CLI's unit tests
  and `crates/wasmkeep-cli/tests/deploy_config.rs`, the S3 one run against
  MinIO). Revised after an adversarial review the
  same day (old deploy tools over new versions, reused version ids,
  rollback across the migration, the class an event takes, sockets,
  instances kept with old values).
- Date: 2026-11-05
- Milestone: [M3](../../ROADMAP.md#-m3-first-public-release)

## Context

Two things a deployed app needs are still given to each node on its
command line, not by `wasmkeep deploy`
([ADR 0008](0008-deploy-and-rolling-upgrades.md)):

- **Access rules**, `--access CLASS=MODE`
  ([ADR 0010](0010-who-may-create-an-actor.md)). ADR 0010 says "every node
  of a cluster must get the same rules", and nothing checks it. A node
  started without `--access monitor=existing` serves `monitor` as `open`,
  and every request reaching it can create an actor, a lease and a snapshot
  in the bucket: the hole ADR 0010 closed, reopened by one forgotten flag.
- **Plain configuration**, `--env CLASS=FILE`: `NAME=value` files the
  actors read with `env.get`. The same files also hold secrets.

An earlier design sketch left one question open: is configuration part
of the deploy version? It proposed a separate pointer for configuration,
"so changing a URL neither reruns `on-init` nor rolls back with code".

The first half of that reason does not hold. `on-init` runs once per
**component**, not per deploy: `ActorClass::version` is the SHA-256 of the
component's bytes, `prepare` compares the actor's recorded init version
with it, and `apply_deploy` keeps the resident instances of a class whose
bytes did not change. A deploy that changes only rules or configuration
does not rerun `on-init`, wherever they are stored.

Secrets are out of scope: storing them in the bucket needs encryption and
key handling (since added, see
[ADR 0016](0016-secrets-in-the-bucket.md)). They stay in per-node `--env`
files, and no secret value may reach the bucket.

What the code did before this ADR that matters here:

- The front door decided access before the event took its class
  (`refresh_class`, inside `prepare`), with `ensure_owner` (bucket I/O) in
  between. A deploy landing in that window ran the new class under the
  old decision.
- An actor's mode was captured once, when its mailbox was created, and its
  values once, when its instance was created. A resident instance kept
  across a deploy kept both.
- A version id was the hash of the class table, and its directory, with
  `previous` in its manifest, was reused when the same table was deployed
  again later, so `previous` could point at an older version than the one
  actually replaced.

## Decision

### Rules and configuration are part of the version

A version is its classes, their access rules and their plain
configuration. One pointer (`current`), one sequence, one follower, one
atomic swap, as in ADR 0008.

- **Rules must change atomically with the code they protect**, in both
  directions. Code written for `open` may check names itself; code written
  for `existing` may rely on the platform to. Running either under the
  other's rule is a hole.
- **Configuration usually changes with code too** (new code reads a new
  `URL`), and the sequence check of ADR 0008 then gives "no old
  configuration on newer data" for actors that already ran the new deploy:
  a node still on deploy 6 does not run an actor that ran deploy 7, with
  its code or its values.
- A separate pointer would need a second sequence, a second follower and
  a rule for which of the two an event records. Its only remaining benefit
  is that a rollback of code keeps the newer configuration; see
  [Rollback](#rollback) for why rolling it back is the safer default.

The cost is that a configuration-only deploy is a new version, whose
directory needs the components. `wasmkeep deploy` copies them inside the
bucket from the current version (`ObjectStore::copy`, a server-side copy on
S3 and R2), so nothing is uploaded again. The copy is unconditional, which
is safe because the destination key is content-addressed: an overwrite
writes the same bytes. Nodes check every component's digest when they load
a version, so a corrupt source is refused there, not run.

### Bucket layout

```text
apps/<tenant>/<app>/current                  "<version> <seq> [2]"; If-Match only
apps/<tenant>/<app>/<version>/manifest       immutable
apps/<tenant>/<app>/<version>/<class>.wasm   immutable (unchanged)
apps/<tenant>/<app>/<version>/<class>.config immutable, new: one per class with configuration
```

No new pointer and no new prefix. A version directory holds everything a
node needs to run that version, and nothing another version refers to.

### Version ids

The id is the first 16 hex digits of the SHA-256 of the manifest's
**identity**, followed by its `previous` line:

- the identity is the `class` lines, then the `access` lines, then the
  `config` lines, each group sorted by class;
- `previous <version>\n` is appended when there is one.

Including `previous` means a directory is never reused under a different
lineage: deploying the same classes again after something else gets a new
directory, whose `previous` is the version it actually replaced, so
rollback and garbage collection follow the real history. A deploy is a
no-op when the identity equals the current manifest's (compared on the
text, not the id), so redeploying what is current still changes nothing.
Directories written before this ADR keep their ids; only new ones are
named this way. (ADR 0008's "the version hashes the class table" is
replaced.)

**For bucket garbage collection** ([ADR 0012](0012-bucket-garbage-collection.md),
step 4 of its plan): a version is deleted by deleting everything under `<version>/`,
not a list of known names, and the manifest first, so a half-deleted
directory has no manifest and no node loads it. No version's objects are
shared with another. A directory is written again only by a deploy of the
same identity onto the same `previous`; such a deploy recreates the
objects that are missing (create-only: existing ones are not rewritten)
and reads the manifest back before it swaps `current`. Collection
therefore re-reads `current` before each version it deletes, and stops
if it changed.

### Manifest

A manifest without rules or configuration is written exactly as before
(`wasmkeep-manifest 1`). One with either is `wasmkeep-manifest 2`:

```text
wasmkeep-manifest 2
previous <version>
class <class> <sha256 of the component>
access <class> <spec>
config <class> <sha256 of the .config object>
```

- `access` lines: one per class whose mode is not `open`. `<spec>` is the
  rest of the line after the class and one space: `existing`, `internal`,
  or `existing:NAME,NAME` (names may contain spaces, `room 1`; class names
  cannot). Names are sorted and deduplicated; a line not in that canonical
  form is refused. A rule for a class not in the version is refused, by
  `deploy` and by the parser.
- `config` lines: one per class with configuration. The `.config` object
  holds the parsed values, not the file: one `NAME='value'` line per name,
  sorted (`env::parse` reads it back unchanged, since nothing inside the
  quotes is interpreted). Comments and commented-out lines are never
  uploaded, and reformatting a file does not make a new version. At most
  `env::MAX_FILE` bytes.
- The parser refuses any line it does not know, as before. A node reading
  a manifest it cannot parse keeps its version, whole (below).

Values never appear in the manifest, only digests. Parse errors name the
object and the line, never a value (the `env.rs` convention).

### The format marker in `current`

Once a version with rules or configuration has been current, every later
write of `current` carries a third field, `2` (`"<version> <seq> 2"`),
rollbacks included. Before this ADR, `current` was read as exactly two
fields, so tools and nodes from before refuse it as unreadable:

- an old `wasmkeep deploy` (a CI image not upgraded) fails instead of
  writing a format-1 version on top, which would drop every rule;
- an old node keeps its version (or, starting, does not start).

The marker also tells a new node whether rules have ever been deployed
for the app ([Node flags](#node-flags)).

### `wasmkeep deploy`

```text
wasmkeep deploy --app T/A --bucket B [--actor CLASS=PATH]... \
               [--access CLASS=MODE]... [--config CLASS=FILE]...
```

- **A deploy changes what it names and keeps the rest.** Starting from the
  current version: `--actor` given replaces the class table, as before
  (classes left out are removed, with their rules and configuration);
  `--actor` absent keeps the current classes. `--access` sets a class's
  rule (`CLASS=open` removes it). `--config CLASS=FILE` replaces a class's
  configuration (`CLASS=` removes it). A class that is not named keeps its
  rule and configuration.

  A purely declarative deploy would move the hole from node flags to deploy
  flags: a CI job running `wasmkeep deploy --actor ...` without the
  `--access` it never knew about would reopen every class. Keeping what is
  not named makes the safe outcome the default, and the manifest still
  records the whole version. The base is the manifest of the `current`
  read whose ETag the swap is conditioned on, so a concurrent deploy fails
  with a conflict rather than losing a rule.
- `--access` and `--config` for a class not in the new version are
  refused (a typo would otherwise leave the class open or unconfigured).
  Names with control characters are refused.
- A deploy that removes a class with a rule other than `open` says so; if
  it also adds a class (a rename), it warns that the new class is open
  unless given a rule.
- **`--config` is stored in the clear.** The flag is not called `--env` so
  that a node's secrets file is not passed by habit. A value that looks
  like a secret is warned about, naming the class and the variable, never
  the value: a name containing `SECRET`, `TOKEN`, `PASSWORD`, `PASS`,
  `KEY`, `CREDENTIAL`, `AUTH` or `DSN` (any case), or a URL with a
  password in it (a user and password before an `@`). The deploy goes on: the warning is a
  nudge, the documentation the rule.
- The deploy prints what changes against the current version: classes,
  rules (one line per class whose mode changes) and the names of
  configuration values added, changed or removed.
- Upload and switch as in ADR 0008: components (or server-side copies),
  `.config` objects and the manifest with create-only writes, then
  `current` with `If-Match`.

### Rollback

`wasmkeep deploy --rollback` re-points `current` to the current manifest's
`previous`, as before, with the rules and configuration of that version,
and prints the same diff.

A rollback that would make any class more open (`existing` to `open`,
`internal` to anything else, an `existing:` list that grows) is refused
unless `--allow-reopen` is given. The cases it catches: a deploy that only
hardened a rule, then a rollback that would undo it; and a rollback past
the migration point, to a version from before rules were deployed, which
would open every restricted class. Keeping the newer rules on rolled-back
code instead is not safer in general (code written for `existing` rolled
back under `open` has nothing checking names), so the version's rules win,
but only on purpose. An explicit `--access CLASS=open` in a deploy is
already on purpose and needs no flag.

### Nodes

A node following an app reads the version, its rules and configuration
(digests checked), compiles the classes, and swaps everything in at once
with the sequence (`apply_version`). A version the node cannot read or
build (an unknown format, a digest mismatch, a malformed `.config`) is
refused whole: the node keeps the previous version, with its rules and
values, logs the error and backs off, as in ADR 0008. While it is behind
for that reason (not for a bucket outage, which every node shares), it
reports not ready, so a load balancer stops sending it requests it would
decide under rules the app no longer has. (ADR 0008 also says the node
reports the error in its heartbeat; that is not implemented, and belongs
with `wasmkeep cluster status`.)

**Rules and values belong to the class an event takes, and an event takes
one.** `ActorClass` carries its version's rules and its merged values.
Each event (a request, a call, an alarm, a socket open or message):

1. takes the class once, at its start (`admit`, or `refresh_class` for an
   alarm); nothing later in the event takes another, so `prepare` runs on
   it even if a deploy lands while the event waits for the bucket or a
   lease;
2. decides access with that class's rules: an outside request to an
   `internal` class is refused (403); to an `existing` class it reaches
   only an actor that exists (404 otherwise);
3. runs on that class, with `env.get` reading its values.

The front door keeps its check only as an early refusal (it saves a
forward and a mailbox): it may refuse with the rules it sees, but
admitting is decided in the event. A refusal is never a hole; an early
admission under a rule the event's class does not have would be.

- **Sockets.** A WebSocket message or close is an outside event like any
  other. Under `existing` the actor must still exist (as before); under a
  class a deploy made `internal`, the socket is closed (1008) at its next
  event.
- **Values on resident instances.** A deploy that changes a class's values
  but not its bytes drops the class's resident instances at their next
  event: the database is kept and `on-init` does not rerun (it is keyed on
  the component), but a guest that cached a value in memory or derived
  state from it reads the new values, the same on every actor of the
  class. A deploy that changes neither (rules only, or the same bytes
  redeployed) keeps them, as before.
- **Logs.** Each applied version logs every class's effective mode, as
  `--access` did at start. `ActorEnv`, `Version` and the deployed
  configuration in `ActorConfig` print names and counts in `Debug`, never
  values.

### Node flags

- **`--access` with `--app`** is accepted only while no version with rules
  has ever been current (no marker in `current`), and only so that a
  cluster can be upgraded without changing its command lines first (the
  steps below). It is then ignored as soon as a version with rules is
  applied, with a warning to remove it, and it is refused at start once
  the marker is there. The error says what to run instead.

  Letting the stricter rule win (the earlier sketch's proposal) would keep per-node rules,
  and with them answers that depend on the node: a request is checked
  where it arrives and checked again on the actor's owner (ADR 0010), so
  the outcome would depend on which two nodes are involved, which is
  indistinguishable from a bug. It also needs intersecting `existing:`
  name lists. And a node flag is a poor break-glass: a rules-only deploy
  reaches every node in about one heartbeat (3 s), restarting nodes takes
  longer. Without `--app` (`--actor` classes on one node, or `wasmkeep
  dev`) `--access` works as before: there is no deploy to carry it.
- A node with `--app`, no `--access` and no marker logs a warning at start
  that the app has never deployed access rules, so every class is open:
  the case of an operator who dropped the flags without deploying the
  rules.
- **`--env` stays, and wins by name.** Secrets lived only there until
  [ADR 0016](0016-secrets-in-the-bucket.md) put them in the bucket, sealed.
  Each class's values are the deployed configuration with the node's
  `--env` values over it. A name set by both is logged once per version
  applied, as a warning naming the class and the variable, never a value.
  Refusing the conflict instead would block a later deploy on that node.
- **`--env` for a class not served** is refused at start without `--app`,
  as before. With `--app` it is a warning: a deploy that removed the class
  must not keep nodes from restarting, and the values reach the class if a
  later deploy brings it (back).

### Migration and mixed versions

A deploy with neither rules nor configuration stays format 1 with no
marker, and nodes from before this ADR take it as before. Everything else
is refused whole by old nodes and old tools: they cannot parse
`wasmkeep-manifest 2` or a `current` with the marker. A running old node
keeps its version and refuses (503) actors that already ran the new
deploy on an upgraded node, as ADR 0008 does for any version a node cannot
build. An old node that restarts cannot start. Both fail closed. Hence the
order, for a cluster started with `--access`:

1. Upgrade every node to the new binary, command lines unchanged. The
   deployed version has no rules yet, so each node keeps applying its
   `--access`, as before.
2. Deploy the rules (and configuration, if wanted) once, without
   `--actor`: `wasmkeep deploy --app T/A --bucket B --access monitor=existing`.
   Every node switches to the deployed rules at the same sequence, and
   warns that its `--access` is now ignored.
3. Remove `--access` from the command lines (a node restarted with it is
   refused from now on).

The marker is set by the first version with rules *or* configuration, and
from then on nodes ignore `--access`. A first deploy that carries only
configuration would therefore drop the flags' rules. The deploy that sets
the marker says so, and lists the rules nodes will apply from then on
(every other class open); step 2 deploys the rules with it.

Upgrading WasmKeep itself is "every node together" in M3
([ROADMAP](../../ROADMAP.md));
step 1 is that. Between steps 1 and 2, as before this ADR, a node started
without `--access` serves every class open, and says so in its log.

## Alternatives

- **Configuration under its own pointer** (the earlier sketch's proposal). Rejected above:
  `on-init` is per component, so its main reason is gone, and two
  sequences would need their own ordering rule.
- **A declarative deploy** (the flags given are the whole version).
  Rejected: forgetting a flag in CI reopens a class.
- **Stricter rule wins between flag and manifest.** Rejected: per-node
  answers, merging name lists, slower than a rules-only deploy.
- **Lineage outside the version directory** (`previous` in `current`, or a
  history object per sequence) instead of `previous` in the id. A history
  object written before the swap can be orphaned by a crash and then block
  the next deploy at that sequence; `previous` in `current` alone cannot
  walk back more than one step. Hashing `previous` keeps the manifest as
  the only record and costs a new directory per change, which happens
  anyway when the content changes.
- **Content-addressed components** (`apps/<t>/<a>/blobs/<sha256>.wasm`,
  shared by versions) instead of copies. It saves storage, but versions
  would share objects, and garbage collection would need reference
  counting. Server-side copies keep each version self-contained.

## Consequences

- One forgotten flag no longer reopens a class: a node with no `--access`
  serves the deployed rules.
- Rules and configuration change on every node in about one heartbeat,
  with no restart, and never apart from the code they were deployed with.
- **The window is not zero.** Until a node applies a version that hardens
  a rule, it decides under the rule it has: a new name of a class just made
  `existing` can still be created through it, and then exists. For
  following nodes that is about one heartbeat; a node that cannot apply
  the version reports not ready instead of deciding for minutes. To close
  a class at once with no window, there is no substitute for stopping
  traffic to it upstream.
- A configuration-only deploy is a version: it gets a sequence, a line in
  the `previous` chain, and a server-side copy of each component. Rollback
  goes back one change, whatever kind it was.
- `serve --app` with `--access` is refused once rules are deployed: the
  one breaking change, pre-release, with the replacement in the error.
- Configuration is readable by whoever reads the bucket (already trusted
  with everything: [security](../security.md)). Secrets stay on nodes.

## Tests

Each fails before its change; the ones marked mutation-checked are run
again with the guarding line removed.

- Manifest (unit, `apps.rs`): formats 1 and 2 round trip; a manifest
  without rules or configuration is written byte for byte as before; names
  with spaces round trip, and non-canonical lines, unknown lines, rules
  for classes not in the version and format-1 manifests with rules are
  refused; `previous` is part of the id; `.config` objects are canonical
  (comments dropped, quotes and spaces kept, reformatting changes nothing);
  `current` with and without the marker.
- Lineage (`tests/cluster/`): deploy X, then Y, then X again: the third
  is a new directory whose `previous` is Y, and a rollback goes to Y.
- The class an event takes (`tests/deploys.rs` and `tests/cluster/`,
  mutation-checked):
  - request B is admitted at the front door under `open` and queues behind
    A (held at the `open` fault point); a deploy making the class
    `internal` lands; B is refused 403 (mutation: let the front door
    decide);
  - request B to an existing actor of an `existing` class waits in its
    existence check (a bucket whose listing is held); a deploy changing
    the code and making the class `internal` lands; B runs on the code it
    took (mutation: take the class again in `prepare`).
- Sockets: an open socket of a class a deploy makes `internal` is closed
  with 1008 at its next message.
- Configuration (`tests/deploys.rs`): a configuration-only deploy changes
  what `env.get` returns, without rerunning `on-init`, and drops the
  instance (mutation: keep it); a node `--env` value wins by name.
- Cluster (`tests/cluster/`): two nodes started on a version with rules
  and no flags: an unknown name of an `existing` class answers 404 through
  either node and leaves nothing in the bucket; an actor created by a call
  is reachable through both; configuration reaches `env.get` on both; a
  rules-and-values deploy reaches both, after which outside requests are
  refused and calls still reach the actor.
- Readiness: a node that read a version it cannot apply reports not ready;
  a bucket outage does not.
- CLI (`crates/wasmkeep-cli/tests/`): `serve --app` starting on a format-2
  version; `serve --app --access` accepted without the marker (and the
  flag ignored after a version with rules), refused with it; `deploy
  --access` and `--config` for a class not in the version refused; a
  deploy without `--access` keeps the current rules; a configuration-only
  deploy copies the components; `--rollback` that reopens refused without
  `--allow-reopen`, and prints `existing -> open`; two concurrent deploys,
  one with `--access`, one with `--actor`, end with one conflict and no
  lost rule; a secret-looking value is warned about, and no value
  appears in the output or the logs.

## Plan

1. This ADR.
2. Manifest format 2, canonical `.config` objects, `previous` in the id,
   the marker in `current`, `AppRef` reading and writing them (S).
3. Rules and values carried by `ActorClass`; one take per event;
   `apply_version`; sockets; dropping instances on new values; readiness;
   the race, configuration and cluster tests (M).
4. The CLI: `deploy --access/--config`, keeping what is not named,
   server-side copies, diffs and warnings, `--rollback --allow-reopen`,
   `serve --app` with or without `--access` and the marker; docs
   (`docs/cluster.md`, `docs/security.md`, ADR 0010's cross-reference) (S).
