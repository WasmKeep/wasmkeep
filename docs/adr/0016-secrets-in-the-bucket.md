# 0016. Secrets in the bucket

- Status: accepted and implemented (2026-11-05; all five slices). Revised the same day after an
  adversarial review. Changes:
  - a crash between create and swap no longer deadlocks the CLI;
  - nodes that cannot open the secrets fail closed;
  - a plaintext format for any value;
  - associated data bound to the bucket prefix;
  - a narrower threat model;
  - no node-local floor;
  - three value layers, and a follower of its own;
  - recovery from a lost key.
- Date: 2026-11-05
- Milestone: [M4](../../ROADMAP.md#-m4-operating-a-cluster)

## Context

Secrets live in files on each node (`wasmkeep serve --env CLASS=FILE`). In
a cluster the operator copies the same file to every node and restarts
them to change a key. A deploy cannot bring the secrets a new version
needs: `wasmkeep deploy --config` carries plain configuration, stored in
the clear and versioned with the code ([ADR 0013](0013-access-rules-and-config-in-deploys.md)).

What the code does today, and what this ADR builds on:

- **Values.** `class_env` merges a version's configuration with the
  node's `--env` values over it, by name, when a version is applied or
  the service starts (`actor.rs`). The service keeps no separate layer
  per source. An event takes its class once, at its start. An instance
  reads its values when it is created, so a change of values drops
  resident instances at their next event (`refresh_class`); the database
  is kept and `on-init` does not rerun (ADR 0013).
- **Following.** `serve --app` reads the deployed version before the
  service starts, then a follower polls `apps/<tenant>/<app>/current`
  every 3 s and backs off up to 300 s on errors. A version the node cannot
  apply makes it report not ready (one flag, `deploy_behind`), but
  readiness only feeds `/_wasmkeep/ready` and metrics: a node that is not
  ready still holds leases, runs calls and alarms, and takes over actors.
- **`env.get`** (`crates/wasmkeep-wit`) and `env.rs` document that secrets
  come only from the node's files, and that values change only between
  events. `env::parse` reads single-line `NAME=value` with no escaping.
- **Cryptography in the tree.** `ring` 0.17 (through rustls and rcgen,
  [ADR 0011](0011-node-to-node-mtls.md)) provides ChaCha20-Poly1305, HKDF
  and a system RNG.
- **Trust.** Whoever can write the bucket can do everything
  ([security](../security.md#who-can-do-what)), including deploying code
  that reads `env.get` and sends the values out. A bucket *reader*, or a
  backup of the bucket, sees actor data and configuration. It also sees
  `cluster/secret` in the clear, which lets it sign node calls in ADR
  0011's default mode; this ADR does not change that, see Consequences.
- Names: tenants and classes are `[a-z][a-z0-9-]*`; app names are
  checked only for `/`. One app per bucket prefix, one tenant per
  process (several tenants per process is planned).

## Decision

### Secrets have their own lifecycle

Secrets are not part of a deploy version. Rolling code back must never
bring back a revoked key, and changing a key must not need a deploy. An
app's secrets are one sealed object per change, with their own sequence
number:

```text
apps/<tenant>/<app>/secrets/<seq:016x>   # sealed, immutable, conditional create
apps/<tenant>/<app>/secrets/current      # "<seq:016x>\n"
```

Object names are zero-padded hex so that a listing is in `seq` order;
`current` holds the same string.

**Writing a change** (the CLI):

1. List `secrets/` and read `current`. The next `seq` is one more than
   the highest of the listed objects and `current`. An object left by a
   command that died before moving `current` is skipped over, never
   reused.
2. Open the set `current` names (unless `reset`, below), apply the
   change, seal it as the next `seq`, and create the object with a
   conditional create.
3. Move `current` with If-Match on the version read in step 1, or
   create it with If-None-Match the first time.
4. If step 2 or 3 loses a race, start again at step 1. The change is
   applied again to the newer set, so nothing another operator wrote is
   lost.

### Sealing

Each object is sealed with ChaCha20-Poly1305 (`ring::aead`) under a key
derived for this one app: HKDF-SHA256 (`ring::hkdf`) from the **secrets
key**, with the salt `"wasmkeep-secrets"` and, as info, `"wasmkeep-secrets
1"`, the bucket prefix (without slashes around it), tenant and app, each
prefixed by its length as 4 big-endian bytes. A known object in
`crates/wasmkeep-server/src/secrets_known_object.txt` pins the format; an
independent implementation in Python's `cryptography`, written from this
section, produces the same bytes. One secrets key can serve several
apps and prefixes without sharing a nonce budget, and encrypting actor
state later can derive its own keys from it.

```text
wasmkeep-secrets 1\n
<key id>\n
<nonce, 24 hex>\n
<ciphertext and tag, hex>\n
```

- **Key id:** the first 8 bytes of SHA-256(`"wasmkeep-secrets-key-id"` ||
  secrets key), in hex. A node with the wrong key, or none, says so by
  id, never by value.
- **Nonce:** 96 random bits per object, from `ring::rand::SystemRandom`.
  Per derived key that is a handful of objects, far below the 2^32 bound
  for random nonces.
- **Associated data:** each field length-prefixed: `wasmkeep-secrets 1`,
  the bucket prefix, tenant, app, `seq`, key id. An object opens only at
  its own place: copying one from another app, another prefix (staging to
  production with the same key) or another `seq` fails.
- **Size:** the plaintext is at most `env::MAX_FILE`; a node reads at
  most twice that plus the header (the ciphertext is hex) and refuses a
  larger object unread.
- **Plaintext:** entries of `<class> <name> <length>\n<value bytes>\n`.
  A value is any UTF-8 (`env.get` returns a string), newlines included,
  so a PEM key fits. Names follow `env.rs`'s rules (`MAX_NAME`,
  `[A-Za-z_][A-Za-z0-9_]*`), classes follow class names.

A bucket reader without the key sees an app's secrets' size and when
they changed, not their names or values.

### The secrets key

- **Generating one.** `wasmkeep secrets new-key --out FILE` writes 64 hex
  characters from the system RNG to a new file. It is created exclusively,
  with mode 0600, and refuses an existing file. Redirecting a command's
  output would create the file under the shell's umask, so the command
  does not print the key.
- **Using it.** `wasmkeep serve --secrets-key FILE`: 64 hex characters
  and an optional trailing newline, nothing else, in a regular file. A
  file readable by its group or others (`mode & 0o077`) is refused, not
  warned about, and its content is never logged. Symlinks are followed
  and the target's mode counts: a Kubernetes secret must be mounted with
  `defaultMode: 0400` (its default, 0644, is refused). On systems without
  Unix modes, nothing is checked.
- **Two keys during a change.** `--secrets-key` may be given twice, and
  nodes open an object sealed under either key. To change the key:
  1. give every node the new key as well, restarting them one at a time;
  2. run `wasmkeep secrets rotate-key` for every app sealed under the old
     key (`--all` does every app under the prefix), with
     `--nodes-have-new-key` to say step 1 is done: no heartbeat says which
     keys a node holds, and a node without the new key answers 503 for the
     app;
  3. remove the old key from every node.

  Rotating the key does not protect values the old key could open: after
  a key compromise, change the values too.
- **Losing the key.** `wasmkeep secrets reset --new-key FILE --from
  SECRETS_FILE` writes a whole set, from a file in the plaintext format
  above, as the next `seq`, without opening the current one. Nodes need
  the new key to open it, so it takes `--nodes-have-new-key` too.
- One key per process. With several tenants per process (planned), one key per
  tenant, so a team holding one key cannot read another's secrets.

### The CLI

The CLI holds the key, because it seals: the machine running it is as
sensitive as a node. Values come from stdin or a file, never from the
command line, where they would reach shell history and `ps`.

```bash
wasmkeep secrets set   --bucket s3://b/prod --app acme/shop --key k --class api STRIPE_KEY < value
wasmkeep secrets set   --bucket s3://b/prod --app acme/shop --key k --file changes   # several at once
wasmkeep secrets unset --bucket s3://b/prod --app acme/shop --key k --class api STRIPE_KEY OLD_KEY
wasmkeep secrets list  --bucket s3://b/prod --app acme/shop --key k     # names, classes, seq; no values
```

- **Several changes in one `seq`.** `set --file` (the plaintext format)
  sets several names, and `unset` takes several names, each as one
  `seq`, so nodes never serve half of a related change.
- **Trailing newline.** A value from stdin loses exactly one trailing
  `\n` or `\r\n`; one from `--file` is taken as written.
- **Warnings.** `set --class` warns for a class the current version does
  not have, as `--env` files do.
- **Older nodes.** `set` will read the heartbeats and, while a live node
  does not read secrets from the bucket, refuse without `--force`: that
  node would serve `env.get` as none. Version strings cannot tell (every
  development build says `0.0.0`), so nodes say it on heartbeat line 10
  (`secrets`), except one run with `--no-secrets`. `reset` checks too.
- **Deleting old objects.** After moving `current`, the CLI deletes
  sealed objects that stopped being current more than an hour ago,
  timed by their successor's last-modified time (as ADR 0012 times deploy
  versions), always keeping the one before `current`. No copy of a
  removed secret stays in the bucket, sealed or not.
- **ADR 0012's deploy collection must not reach `secrets/`.** Its
  `is_version_id` accepts any lowercase id, so a manifest naming
  `secrets` as its previous version could send it there. `secrets` and
  `current` are refused as version ids (first slice).

### Nodes

- **Three layers.** The service keeps each class's deployed
  configuration, the app's secrets and the node's `--env` values apart,
  and builds a class's values from all three, by name, last wins:
  configuration, then secrets, then `--env`. A deploy and a secrets
  change each rebuild from all three, so neither drops the other's
  values. A secrets change reuses each class's compiled component; it
  only swaps values. A class whose values changed drops its resident
  instances at their next event (ADR 0013).
- **At start.** `serve --app` opens the current secrets before the
  service starts, as it reads the version, so no event runs without them.
- **A follower of its own.** It reads `secrets/current` every 3 s, with
  its own backoff and its own flag. A version this node cannot build does
  not delay a secrets change, and a bad secrets object does not delay a
  deploy.
- **A named object that is missing** (deleted after a newer `current`
  was written), or unreadable while `current` answers, means reading
  `current` again on the next poll; if it lasts 30 s the node fails closed
  (below). A corrupt `current` counts the same.
- **Never backwards, in memory.** A node keeps the highest `seq` it
  opened since it started and refuses a lower one, or an object whose
  authenticated `seq` is not the one `current` names. A restarted node
  accepts what `current` names, so a bucket restored from a backup works
  once the nodes restart (running ones refuse the lower `seq`, then fail
  closed). A bucket writer, who can read the secrets anyway (below), gains
  nothing from rolling a node back.
- **Failing closed.** A node that knows a set it cannot open (no key, a
  key id it does not hold, a tampered or oversized object), or that has
  not opened the set `current` names for 30 s:
  - refuses events for the app's classes with 503 and `Retry-After`,
    logging why;
  - stops claiming actors, as a draining node does ([ADR 0014](0014-draining-a-node-and-moving-an-actor.md)),
    so other nodes take them;
  - reports not ready.

  It never runs an actor with an older or empty set. During a bucket
  outage every node keeps the set it has and stays as it is: the outage
  already stops leases.
- **No key, by choice.** `serve --app` with no `--secrets-key` refuses to
  start once `secrets/current` exists, unless `--no-secrets` says this
  node runs without them, with `--env` only. A node started before any
  secrets existed and keyless fails closed when they appear.
- **Required values:** `wasmkeep deploy --requires CLASS=NAME,NAME...`
  writes `requires <class> <NAME>` lines in the manifest (part of the
  version; kept by later deploys; `CLASS=` removes them). A class missing a
  required name refuses its events: a request or call gets 503, an alarm
  waits (tried again at the longest backoff), a socket closes with 1011.
  The node logs the class and the missing names once (again after a
  secrets change), never a value; an empty value counts as set. Required
  names make the manifest format 2 but do not mark the app (ADR 0013): only
  rules or configuration do. Older nodes refuse a manifest with a
  line they do not know (`Manifest::parse` is strict), so such a version
  is refused, not misread. A rollback to a version without the lines
  drops the requirement with them.
- `wasmkeep dev` and `serve` without `--app` use `--env` only, as today.

### `env.get`

The interface does not change. Its doc comment, and `env.rs`'s module
doc, do: values also come from the app's secrets in the bucket, and
change between events when a deploy or a secrets change lands, never
during one. The SDKs' docs say the same.

### Consistency

Eventually consistent across nodes: for up to one poll period, two nodes
serve different secret sets. An event sees one set throughout. A change
that must be atomic across the cluster (revoke an old key while a new
one starts working) is the application's: accept both for a while.

### Moving secrets from `--env` to the bucket

1. Upgrade every node.
2. Give every node the key (`--secrets-key`), restarting them one at a
   time.
3. `wasmkeep secrets set` each name.
4. Remove the names from the `--env` files and restart the nodes one at a
   time. Until then `--env` wins by name. The override is logged at warn
   level, naming the class and variable, so a stale value left in a file
   is visible.

## What sealing protects, and what it does not

- **Protects against bucket readers and backups.** They cannot read a
  secret's name or value, forge one, or move one between apps, prefixes
  or `seq`s.
- **Does not protect against bucket writers.** A writer can deploy code
  that calls `env.get` and returns or sends the values (deploys are not
  signed yet). A writer can also delete the secrets, making nodes fail
  closed. Signed deploys, a planned change, are what would narrow this.

## Alternatives

- **Secrets as part of the deploy version** (like ADR 0013's config).
  Rejected: a rollback would bring back revoked keys, and a key change
  would need a deploy.
- **Sealing to a public key** (X25519 agreement, with nodes holding the
  private key). It lets CI write secrets without being able to read them,
  but every change would replace the whole set, or the format would need
  per-entry sealing and merge rules. Possible later, beside this.
- **A KMS (AWS KMS, Vault) from the start.** Later, as another source of
  the secrets key. Not in the first cut: it brings a cloud dependency and
  IAM to self-hosted clusters that may have neither.
- **An envelope per object** (a random data key wrapped by the key). It
  pays off for large data or frequent re-wrapping; an app's secrets are
  small, and a key change re-seals them anyway.
- **A new crypto crate** (`chacha20poly1305`, `age`). `ring` is already
  in the tree, widely used and maintained; a direct dependency on it adds
  no crate.
- **Pushing secrets to nodes** over the cluster port. Rejected like ADR
  0008's push to nodes: the bucket reaches every node, a push reaches the
  ones up at the time.
- **A floor persisted on each node** (this ADR's first draft). Rejected:
  it breaks bucket restores and wiped data directories, and protects
  against a party who can read the secrets anyway.

## Consequences

- `wasmkeep-server` takes a direct dependency on `ring` (already in the
  tree through rustls and rcgen; no new crate), for the AEAD,
  HKDF and RNG that sealing secrets needs.
- One more object read per node per poll (`secrets/current`), and a list
  and a few objects per change.
- A node that cannot open the secrets stops taking actors: the cluster
  needs a node with the key to serve the app.
- `cluster/secret` stays in the bucket in the clear. Sealing it under the
  same key would make a bucket reader unable to sign node calls; that is
  a separate change, left for signed deploys.

## Tests

- **Unit:**
  - seal and open; the wrong key;
  - each associated-data field changed (prefix, tenant, app, `seq`, key
    id), and a tenant/app split moved (`ab`/`c` against `a`/`bc`);
  - a flipped byte; a truncated object; an oversized one;
  - two keys during a change;
  - the plaintext format with newlines and every allowed byte;
  - key files: 64 hex characters, junk refused, group- or world-readable
    refused, `new-key --out` creating 0600 and refusing an existing file;
  - `is_version_id` refusing `secrets` and `current`.
- **Cluster** (`crates/wasmkeep-server/tests/`):
  - `secrets set` reaches every node's `env.get` within one poll, without
    a restart or a deploy;
  - a resident actor reads the new value at its next event, and the same
    value throughout an event;
  - a deploy after `secrets set` keeps the secrets;
  - the first event after a start sees them;
  - rolling code back leaves the secrets as they are;
  - a node with another key, or a keyless node once secrets appear,
    refuses the app's events with 503, takes no actor over (another node
    does), and reports not ready;
  - `serve --app` with no key refuses to start once secrets exist,
    unless `--no-secrets`;
  - secrets change while a version the node cannot build is current;
  - `current` naming a deleted object, and a lower `seq` after a newer
    one;
  - `--env` wins by name, logged at warn without the value;
  - no object in the bucket holds a value or a name in the clear: the test
    lists every object and searches each.
- **CLI:**
  - values from stdin (one trailing newline stripped) and `--file`,
    never argv;
  - a command that dies after the create and before moving `current`,
    then another `set` that succeeds;
  - two concurrent `set`s both land;
  - `rotate-key`, `reset`;
  - old objects deleted by their successor's time, never the one before
    `current`;
  - `set` refused while an older node is live.
- No `sim/` case: no lease, failover or ordering is involved beyond a
  monotonic counter in memory, which the cluster tests cover (a
  `tests/cluster/` case is enough for that).

## Plan

| Slice | Size |
| --- | --- |
| Sealed-object format, HKDF, key file and key id; `is_version_id` tightened; unit tests | S |
| `wasmkeep secrets` CLI (`new-key`, `set`, `unset`, `list`, `rotate-key`, `reset`, deleting old objects, older-node check) | M |
| Nodes: three layers, open at start, own follower, fail closed and stop claiming, `--no-secrets` | M |
| `env.get` and `env.rs` docs, SDK docs, `docs/concepts.md`, `docs/security.md`, `docs/cluster.md` | S |
| `requires` in the manifest, fail closed per name | S |

The feature is complete when all five are in, `requires` included.

## Open questions

- **Reloading the key without a restart** (on SIGHUP). It would save the
  two rolling restarts of a key change. Proposed: later, if key changes
  turn out to be frequent.
