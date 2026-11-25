# Deploying to a cluster

How an app's versions, its front, its access rules and its secrets reach
the nodes of a cluster. Part of [Running a cluster](cluster.md).

## Deploying

Instead of giving every node `--actor CLASS=PATH`, deploy once to the bucket
and let nodes follow. With a [project file](project-file.md)
(`wasmkeep.toml`, [ADR 0026](adr/0026-the-project-file.md)) in the app's
directory, the deploy is the file, built and printed before it switches:

```bash
wasmkeep deploy --bucket s3://wasmkeep-demo      # reads ./wasmkeep.toml
```

- The version is exactly the file: a class or front the file no longer
  has is removed, every class's rule is written (`internal` unless the
  file says otherwise), and configuration or required names the file does
  not give are removed. The output lists each change, removals first
  named, before anything is uploaded.
- Beside a file, `--actor`, `--access`, `--config`, `--requires` and the
  front flags are refused; `--no-project` deploys with flags only, as
  below (classes added that way are `open` unless given `--access`).
- `--rollback` reads only `[app]` from the file and runs no build.

With flags only:

```bash
wasmkeep deploy --app acme/shop --bucket s3://wasmkeep-demo \
  --actor counter=counter.wasm --actor chat=chat_room.wasm
wasmkeep serve --app acme/shop --bucket s3://wasmkeep-demo --node-id a ...
wasmkeep deploy --app acme/shop --bucket s3://wasmkeep-demo --rollback
```

- **Each app owns its actors** ([ADR 0022](adr/0022-app-owned-state-and-the-one-time-migration.md)):
  their state lives under `state/<tenant>/<app>/`, beside
  `apps/<tenant>/<app>/`, so several apps of one tenant can share a bucket
  prefix, and the same class and actor name in two apps are two actors.
  An app is those two prefixes, for deleting, exporting and access
  policies alike. Nodes started with `--actor` run as the implicit app
  `default/default`; `deploy --app default/default` moves such a cluster
  to deploys and keeps its actors. `wasmkeep app delete` removes an app
  ([Deleting an app](cluster-operations.md#deleting-an-app)).

- A bucket prefix that holds actor state written by a version before
  0.4.0 is migrated once with `wasmkeep cluster migrate` while every node
  is stopped ([Upgrade a cluster to 0.4.0](deploy/upgrading-to-0.4.md)).
  The command was removed in 0.9.0: a cluster older than 0.4.0 upgrades
  through 0.8.x first (pin the 0.8.1 binary to migrate). Until then,
  nodes and `deploy` of this version refuse that tenant and say to
  migrate with 0.8.1.
- The tenant and app names of a new app match `[a-z][a-z0-9-]{0,63}`; an
  app deployed earlier keeps its name.
- `deploy` compiles and checks every component before uploading anything;
  a broken one is refused and the current version keeps running.
- Nodes check for a new version every 3 s and switch each actor at its next
  event. No actor is dropped; `on-init` runs once per version.
- A node may start before the app's first deploy: `serve --app` then logs
  `waiting for the first deploy of TENANT/APP` and reads `current` every
  3 s, as for new versions. Until a deploy lands it does not listen,
  writes no heartbeat (`cluster status` does not list it yet) and opens
  no secrets; then it starts as if it had been started after the deploy.
- `deploy` ends with the version now current and its `seq`:
  `acme/shop: version V is current (deploy N)`. A deploy that changes
  nothing (the same classes, rules, configuration and front) writes
  nothing and says `acme/shop: nothing changed: version V is already
  current (deploy N)`, with `N` as it was; a rollback lists its changes
  under `rolling back acme/shop to TO:` and says
  `rolled back acme/shop: FROM -> TO (deploy N)`.
- An actor never runs older code than it already ran: a node that has not
  caught up yet refuses it (the client retries) and updates at once.
- Rollback makes the previous version current again (as a new deploy),
  with its access rules and configuration.
- Old versions are removed by `wasmkeep deploy` itself, after every deploy
  and rollback ([ADR 0012](adr/0012-bucket-garbage-collection.md)): it
  keeps the current version and the five before it (as many rollbacks
  as you can make in a row), and deletes older ones once they stopped
  being current more than `--version-retention-secs` ago (default 3600;
  0 at once; measured with the bucket's timestamps against the clock of
  the machine running the command). Only versions in the current one's
  line of `previous` links are candidates, oldest first, and the pass
  stops if `current` changes while it runs. Never collected: a version a
  concurrent deploy is still writing, one a failed deploy left, the
  versions a rollback stepped back from, and a version whose manifest
  this WasmKeep cannot read. Those stay in the bucket (they are small).

### Deploying static assets

Use `[assets] dir`, `build`, `watch`, `not-found`, `front-first` and
`immutable` in the [project file](project-file.md#assets-optional).
Without a project file, `--assets DIR` replaces files and `--no-assets`
removes them; with neither flag, the current index is preserved. The optional
`--assets-not-found`, `--assets-front-first` and `--assets-immutable`
flags require `--assets` and conflict with project-file deployment and
rollback. `--assets-verify` reads objects back and repairs digest failures.
An unchanged asset deploy still repairs missing objects.

`wasmkeep assets collect --app acme/shop --bucket s3://my-bucket
--version-retention-secs 3600` runs version and asset collection separately.
An asset pass uses the bucket's clock for both, and markers coordinate
it with deploys. Every retained manifest protects its files. See
[static assets](static-assets.md) for the full serving and storage rules.

### Deploying the front

An app's front ([ADR 0023](adr/0023-the-front.md)) is the stateless
`wasi:http` component that answers paths not served by [static assets](static-assets.md), outside `/actor/` and
`/_wasmkeep/`, and calls the app's actors with `actors.call`. It ships
in the app's version, with the classes:

```bash
wasmkeep deploy --app acme/shop --bucket s3://wasmkeep-demo --front site.wasm
wasmkeep deploy --app acme/shop --bucket s3://wasmkeep-demo \
  --front-config site.env --front-requires SESSION_KEY
wasmkeep deploy --app acme/shop --bucket s3://wasmkeep-demo --no-front
```

- **`--front PATH` sets it, `--no-front` removes it** (with its
  configuration and required names); with neither, the current front is
  kept, copied inside the bucket as classes are without `--actor`. A first
  deploy may give a front and no class: the app then serves its front,
  and its calls fail with `invalid-address`.
- **The front's secrets stay when the front goes.** `--no-front`, or a
  `--rollback` to a version without a front, leaves the app's `_front`
  secrets in the set: a node older than the front cannot open a set with
  them (it refuses every event and drains), and a later front reads the
  values left. Such a deploy prints a warning while the app has secrets,
  with the two commands to run, complete but for the key file (`--key`)
  and the names: `wasmkeep secrets list --bucket ... --app ... --key
  KEY_FILE` shows the names under `_front`, and `wasmkeep secrets unset
  --bucket ... --app ... --key KEY_FILE --front NAME...` removes them.
- **The front's scope is not a class.** `--config _front=...`,
  `--requires _front=...` and `--access _front=...` are refused: give
  the front's values with `--front-config` and `--front-requires`; the
  front has no access rule.
- **`--front-config FILE` and `--front-requires NAMES`** are the front's
  `--config` and `--requires`, with the same rules: kept by later deploys,
  `--front-config ''` and `--front-requires ''` remove them. While a
  required name is not set, the front is not run: every request outside
  `/actor/` gets a generic `503` with `Retry-After: 1` ("this node cannot
  serve the app now; retry"); the missing names go to the node's log, once,
  and to the `unconfigured` outcome of `wasmkeep_front_requests_total`,
  never to the client. The same 503 answers while the node cannot open the
  app's secrets.
- **Checked before anything is written.** The front must link against
  the front world (WASI, `wasi:http`, `actors`, `env`), export the
  incoming handler, and instantiate once: a start function that traps,
  or memory past the limits of the machine running `deploy` (its default
  limits), is refused, as is a component that imports `storage`,
  `alarms` or `websockets`, or exports an actor's `on-*` functions (an
  actor given as a front by mistake). The test instantiation runs
  without values, required names or not. A node starting with a
  deployed front instantiates it once too, unless its required values
  are not set (or the secrets not open) yet; a failure there is logged,
  and the node starts.
- **What `deploy` prints:** `front: added`, `changed`, `removed` or
  `unchanged`, the front's configuration and required names by name
  (`config _front: NAME added`), and every class whose rule is `open`
  (`class cart: open, reachable at /actor/cart/<name>, past the front`):
  a front does not close the `/actor/` paths, access rules do.
- **Nodes switch with the version.** A node loads `_front.wasm` (its
  digest checked), links it, and swaps it in with the classes, the rules
  and the values: a request under way finishes on the front it started
  with, the next one takes the new one. A front that does not link is a
  version the node cannot apply: it keeps the previous version whole and
  reports not ready, as for a class. `--rollback` brings back the previous
  version's front with its classes; `--no-front` makes the front's paths
  answer `404 not found`.
- **Older nodes.** A version with a front is written as manifest format 3,
  which a node from before 0.4.0's front cannot read (it would stay not
  ready). `deploy` and `--rollback` refuse to make such a version current
  while a live node of the app does not announce the capability word
  `front` on its heartbeat; there is no `--force`. Upgrade every node
  first. Versions without a front are written as before, with the same
  ids. A node started without `--node-id` writes no heartbeat, so the
  check cannot see it.
- **A front that passes the deploy's checks and still fails** (it traps
  while handling a request rather than at instantiation, say) answers
  500 for its requests. A node that starts with a deployed front that
  does not instantiate starts anyway, with its actors, logging the
  error. Deploy a fixed front, or roll back.
- **A node's own values:** `serve --front-env FILE` gives the front
  values on that node, over the deployed configuration and the secrets
  (`--env` takes classes only).

With `--app`, a component on the `serve` command line is refused at
start: the front comes from the deploy only. Without `--app`, the
positional component of `serve` is the front of the implicit app
`default/default`, a file on each node read once at start:

```bash
wasmkeep serve site.wasm --actor cart=cart.wasm --bucket s3://wasmkeep-demo --node-id a ...
```

Updating that file means copying the new one to every node and restarting
them one at a time, waiting for each to answer `/_wasmkeep/ready`; until
the last one restarts, two versions answer side by side. Deploying the app
(`--app`) avoids that.

The front calls the app's classes with `actors.call`, as an inside
caller: the callee sees `x-wasmkeep-caller: <tenant>/_front/main`, and a
call to an actor on another node is a signed hop, as an actor's ([the
front](concepts.md#the-front)). It runs on the node that received the
request and is never forwarded. Its outbound HTTP follows the same rules
as actors' (public addresses only, unless the node allows a range with
`--allow-outbound`).

### Access rules and configuration

Access rules ([ADR 0010](adr/0010-who-may-create-an-actor.md)) and plain
configuration are part of the deployed version
([ADR 0013](adr/0013-access-rules-and-config-in-deploys.md)): every node
applies the same ones, at the same moment as the code they go with, and
a node started without flags no longer serves a restricted class open.

```bash
wasmkeep deploy --app acme/shop --bucket s3://wasmkeep-demo \
  --access monitor=existing --access hub=existing:main \
  --config hub=hub-config.env
```

- **A deploy changes what it names and keeps the rest.** Without
  `--actor`, the current classes are kept (copied inside the bucket, not
  uploaded again). Classes not named keep their rule and configuration, so
  a CI job deploying new code does not drop rules it never knew about.
  `--access CLASS=open` removes a rule, `--config CLASS=` a class's
  configuration. `deploy` prints what changed (class code, rules, the
  names of configuration values) and warns when a class removed with a
  rule leaves an added one open (a rename).
- **`--config` is stored in the clear**, as parsed (no comments). Keep
  secrets in the bucket with `wasmkeep secrets set`
  ([Secrets in the bucket](#secrets-in-the-bucket)) or in `serve --env`
  files on each node: both win over the deployed configuration by name
  (each overlap is logged by name, never value). `deploy` warns about names that look like secrets (`TOKEN`,
  `KEY`, `PASSWORD`...) and URLs with a password, without printing them.
- **Rollback** brings back the previous version's rules too. One that
  would make a class more open (`existing` to `open`, say) is refused
  unless `--allow-reopen` is given.
- New values drop the class's resident instances at their next event (the
  database is kept, `on-init` does not rerun); rules alone do not. A class
  made `internal` closes its open sockets (1008) at their next message.
- A node that reads a version it cannot apply keeps the previous one and
  reports not ready (`/_wasmkeep/ready`) until it can.
- **`--access` with `--app`** is refused: rules come with deploys. Every
  deploy of 0.4.0 marks the app (its `current` carries token `3`, [ADR
  0022](adr/0022-app-owned-state-and-the-one-time-migration.md)), and a
  node refuses to follow a `current` from an earlier version. A cluster
  that still passes `--access` to its nodes deploys those rules once
  (`wasmkeep deploy --app acme/shop --bucket ... --access
  monitor=existing`), then removes `--access` from their command lines;
  the start-up error gives the `deploy` command. A node following an app
  that never deployed rules logs that every class is open.

### Secrets in the bucket

Secrets can live in the bucket, sealed with a key the nodes hold, and
change without a deploy or a restart
([ADR 0016](adr/0016-secrets-in-the-bucket.md)):

```bash
wasmkeep secrets new-key --out secrets.key        # mode 0600; keep it with the bucket credentials
wasmkeep serve ... --app acme/shop --secrets-key secrets.key   # every node
wasmkeep secrets set --bucket s3://acme-wasmkeep/prod --app acme/shop --key secrets.key \
  --class hub ADMIN_TOKEN < token.txt            # one trailing newline removed
wasmkeep secrets list --bucket s3://acme-wasmkeep/prod --app acme/shop --key secrets.key   # names only
```

- **Reaching the nodes.** Each node reads the app's secrets at start and
  checks for a change every 3 s. A class whose values changed drops its
  resident instances at their next event (the database is kept, `on-init`
  does not rerun); an event under way keeps the values it started with.
- **Which value wins**, by name: the deployed configuration, then the
  secrets, then the node's `--env` (logged at warn, never the value).
  `--env` stays a node-local break-glass.
- **Several at once:** `set --file FILE` (entries
  `<class> <name> <length>\n<value>\n`, any value, newlines included) and
  `unset --class CLASS NAME...` make one change each, so nodes never serve
  half of it.
- **The front's secrets** are the scope `_front`: `secrets set --front
  NAME`, `secrets unset --front NAME...`, and `_front <name> <length>`
  entries in `set --file`. `secrets list` shows the scope as `_front (the
  front)`. They reach the front's `env.get` over its deployed
  configuration and under the node's `--front-env`. A `set` (or `reset`)
  that gives the set its first `_front` entry is refused while a live
  node of the app that reads secrets does not announce the capability
  word `front`, with no `--force`: such a node could open none of the
  app's secrets, and would refuse every event. `unset` and `rotate-key`
  add no entry and are never refused. A node started without `--node-id`
  writes no heartbeat, so the check cannot see it: upgrade such nodes
  before using the scope.
- **A node that cannot open the current secrets** refuses the app's
  events with 503 and `Retry-After`, hands its actors to a node that can,
  takes none, and reports not ready. It blocks at once on a set it cannot
  open (no key, another key, an altered object), and after 30 s on one it
  cannot reach (the object missing or unreadable, `current` unreadable).
  Past those windows (one poll, 3 s, or the 30 s) it never runs an actor
  with an older or empty set. `cluster status` then shows it draining or
  drained without a drain marker; `undrain` does not apply. When no node
  can open them, every event for the app is 503 until one can. Socket close
  events on a blocked node do not run. Without `--secrets-key` (and
  without `--no-secrets`), a node refuses to start once the app has
  secrets.
- **Restoring the bucket from a backup** can put `current` below what the
  running nodes opened; they refuse it (and block after 30 s) until
  restarted: restart the nodes after a restore
  ([Back up and restore a cluster's bucket](deploy/backup-and-restore.md)
  has the whole procedure). Deleting `current` by hand
  leaves running nodes with the set they have and restarted ones with
  none.
- **Older nodes.** `set`, `unset` and `reset` refuse, without `--force`,
  while a live node of the app does not read secrets (an older version, or
  one run with `--no-secrets`): it would serve them as unset. Nodes say so
  on heartbeat line 10; the check counts a heartbeat as live with the
  default timings, as `cluster status` does.
- **Changing the key.** Give every node the new key as well (a second
  `--secrets-key`, one node restarted at a time), run `wasmkeep secrets
  rotate-key --key old.key --new-key new.key --app acme/shop
  --nodes-have-new-key` (or `--all`), then remove the old key from the
  nodes. The flag is required: a node without the new key cannot open the
  re-sealed set and answers 503 for the app until it gets the key, and no
  heartbeat says which keys a node holds. `set` and `unset` given two
  keys seal with the first, and take the same flag when that one is not
  the key the current set is sealed with.
- **Finishing a key change.** `rotate-key` keeps the object before
  `current`, sealed with the old key, for a node that read `current` just
  before it moved. Once the old key is removed from every node, run
  `wasmkeep secrets rotate-key --new-key new.key --app acme/shop --finish`
  (or `--all`). It never deletes `current`, refuses an app whose current
  set does not open with `--new-key`, and deletes the objects another key
  sealed:
  - below `current`, all of them, except the one just before it during
    the first 30 s after `current` moved (a node starting, a backup or a
    command may still read it);
  - above `current` (written by a `secrets` command still running, or one
    that stopped before moving `current`), once they are an hour old.

  Anything it leaves (those, and objects it cannot parse or that are too
  large) is counted: it warns and exits with an error, and a later run
  finishes. With `--all`, an app refused or left unfinished does not stop
  the others; the run fails at the end. On a bucket with object
  versioning, the deleted objects stay as noncurrent versions until a
  lifecycle rule removes them.
- **Rotating the key does not change the values.** The new key seals the
  same values. A value the old key could open (a leaked key, an old
  backup of the bucket, a noncurrent version) is exposed for good: after a
  key compromise, rotate the values themselves at their source (new API
  keys, new tokens), then `set` them.
- **Losing the key.** Give every node the new key, then `wasmkeep secrets
  reset --new-key new.key --from FILE --nodes-have-new-key` writes a whole
  set without opening the current one.
- **Moving from `--env`.** Upgrade every node, give them the key, `set`
  each name, then remove the names from the `--env` files and restart the
  nodes one at a time.
- **Required values.** `wasmkeep deploy --requires hub=ADMIN_TOKEN,WEBHOOK_URL`
  makes those names part of the version: while one is not set (by the
  configuration, the secrets or a node's `--env`), the class refuses its
  requests with 503 (an alarm waits, a socket closes with 1011) instead of
  running without it, and the node logs the class and the missing names
  once (again if they go missing again), never a value. A value set to the
  empty string counts as set. Once the value is set, requests run at once;
  a waiting alarm runs within the alarm retry ceiling (5 minutes by
  default); an idle socket closes at its next message. Kept by later
  deploys; `--requires hub=` removes them, and a rollback brings back the
  previous version's. Required names alone do not end ADR 0013's
  migration (only rules or configuration do).
