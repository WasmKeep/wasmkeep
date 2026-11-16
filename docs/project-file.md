# The project file

`wasmkeep.toml` describes an app: its front, its classes, who may reach
each class, their configuration files and required names, and how each
part is built. `wasmkeep deploy` deploys exactly what it says, and
`wasmkeep dev` runs it locally the way a node runs the deployed version.
The decision and its reasons are in
[ADR 0026](adr/0026-the-project-file.md).

```toml
[app]
tenant = "acme"
name = "todo"

[front]
component = "front/target/wasm32-wasip2/release/todo_front.wasm"
build = "cd front && cargo build --release --target wasm32-wasip2"
requires = ["TOKENS"]
watch = ["front/src", "front/Cargo.toml"]

[classes.todos]
component = "todos/target/wasm32-wasip2/release/todo_todos.wasm"
build = "cd todos && cargo build --release --target wasm32-wasip2"
watch = ["todos/src"]

[dev]
port = 8080
```

```bash
wasmkeep dev                                   # build, serve, rebuild on change
wasmkeep deploy --bucket s3://my-bucket/prod   # build, print the plan, switch
```

[`examples/todo-front`](https://github.com/wasmkeep/wasmkeep/tree/main/examples/todo-front)
and [`examples/ts-todo-front`](https://github.com/wasmkeep/wasmkeep/tree/main/examples/ts-todo-front)
each have one.

## Where it is read from

`./wasmkeep.toml`, in the current directory only (parent directories are
not searched), or the file `--project PATH` names (a file, or a directory
holding `wasmkeep.toml`). `--no-project` ignores `./wasmkeep.toml`.

Relative paths in the file are relative to the file's directory, and
builds run there, wherever the command is run from. Paths may leave that
directory (`../shared/...`). They are read on the machine that runs the
command; what reaches the bucket is named by class, never by path.

## Keys

Every key below is the whole list: an unknown key or table, or a value
of the wrong type, is an error naming the file, line, column and key.

### `[app]` (required)

| Key | | |
| --- | --- | --- |
| `tenant` | required | `[a-z][a-z0-9-]{0,63}` |
| `name` | required | `[a-z][a-z0-9-]{0,63}` |

### `[front]` and `[classes.<name>]`

`[front]` is the app's front ([ADR 0023](adr/0023-the-front.md)); there
is at most one, and a file without `[front]` has no front. Each
`[classes.<name>]` is one class, named by its key (`[a-z][a-z0-9-]{0,63}`;
`_front` is the front's scope, not a class). A file needs a front or at
least one class.

| Key | | |
| --- | --- | --- |
| `component` | required | the `.wasm` file |
| `access` | classes only | `internal` (the default: the app's actors, alarms and front only), `existing`, `existing:NAME,...` or `open`, as `deploy --access` |
| `config` | optional | a file of `NAME=value` lines, plain configuration (stored in the bucket in the clear: never secrets) |
| `requires` | optional | names that must be set (by the configuration, the app's secrets or a node's `--env`) before the class or front runs |
| `expire-after-idle` | classes only | how long an actor of the class may stay idle before it is deleted: a whole number and one unit, `s`, `m`, `h` or `d` (`"7d"`, `"36h"`), at least `"1h"`; none by default (actors are kept). In a cluster only ([idle expiry](cluster.md#idle-expiry)) |
| `writes` | classes only | how the class's plain writes reach the bucket: `"strict"` (the default, every release's behaviour: an event's writes are uploaded before the next event starts) or `"batched"` (uploaded behind the events, several to a segment, [ADR 0034](adr/0034-batched-plain-writes.md)). `"batched"` needs a `wasmkeep` built with the `batched-writes` feature, whose `deploy` accepts it and whose nodes run it; any other refuses it ("not yet available"). The same as `deploy --writes CLASS=MODE`. `"strict"` written or not makes the same version, byte for byte |
| `build` | optional | a command for the system shell (`sh -c`, `cmd.exe` on Windows), run in the file's directory before `component` is read |
| `watch` | optional | files or directories of sources; `wasmkeep dev` runs `build` again when one changes (name sources, not what the build writes) |

<a id="ingress-limits"></a>

### `[limits.<name>]`

Optional host admission rules, applied by both `dev` and deployed nodes.
There may be 32 rules, named like classes. All matching rules must allow a
request; a refusal spends no tokens in any rule.

```toml
[limits.signin]
match = "POST /signin"
key = "ip"
rate = "4/s"
burst = 8

[limits.api]
match = "/api/*"
key = "header"
header = "x-client-id"
rate = "90/m"
burst = 15
```

| Key | Meaning |
| --- | --- |
| `match` | Absolute encoded path, optionally preceded by an uppercase standard HTTP method and one space. Only a final `/*` wildcard is allowed; `/api/*` includes `/api/` but not `/api`. Queries do not participate. Maximum 1024 bytes; no whitespace, fragments or regex. |
| `key` | `ip` (TCP peer) or `header`. Each app and named rule has separate client buckets on each node. |
| `header` | Required only for `header` keys. Lowercase HTTP field name, at most 128 bytes. Host-internal, forwarding and hop-by-hop fields are refused. |
| `rate` | Positive decimal integer followed by `/s`, `/m` or `/h`, at most 1,000,000 tokens per period. |
| `burst` | Integer from 1 to 1,000,000: initial and maximum tokens. |

Matching is case-sensitive, excludes the query, decodes unreserved percent
escapes, canonicalizes remaining escape hex to uppercase, and collapses repeated
slashes. Exact rules ignore trailing slashes. A `GET` rule also covers `HEAD`;
a `HEAD` rule covers only `HEAD`. Actor names use the router's full percent
and UTF-8 decoding, then canonical percent encoding to preserve segment boundaries.
Other reserved escapes stay encoded: the paths in `https://example.test/a%2Fb`
and `https://example.test/a/b` differ. Prefix rules retain their final slash;
a prefix covers that slash and descendants, not the path without the slash. This normalization is only for policy matching; it does not rewrite
the URI delivered to routing or guest code.

A missing method matches any method. A header
key must occur once and contain 1–256 non-whitespace printable ASCII
bytes; missing, duplicate or invalid values return 400. Unknown fields
and duplicate definitions fail with a source position.

Budgets are **per node**, reset on process restart, and use monotonic
elapsed time. A 429 response includes an integer `Retry-After` in seconds;
clients should wait at least that long. Direct `/actor/` requests and
WebSocket handshakes are covered; frames are not charged. Internal
forwarding is authenticated and does not consume another allowance.

IP keys use the TCP peer only, with IPv6 aggregated to 64-bit subnet prefixes and IPv4-mapped
IPv6 using the IPv4 key. Forwarding headers are never used as IP
identity. There is no trusted-proxy IP configuration in this release: a
proxy shares one peer quota. A header key supplied by an edge must be
removed from incoming requests and overwritten at that edge, with direct
access to the node restricted. Client-supplied identifiers are not auth.

Full project deploys replace the policy set (omission removes it). Legacy
partial flag deploys preserve policies. Plan output lists added, changed
and removed rules. Unchanged rules retain their budgets; edits to a named
rule clamp retained available tokens to the new burst. Renaming or
removing a rule discards its old budget. Nonempty policies require manifest
format 6 and `rate_limits` on every live app reader; deploy and rollback
refuse older readers. See [ADR 0036](adr/0036-per-node-ingress-rate-limits.md).

Each live app service receives an equal reserved share of the 65,536-bucket
process budget, capped at 4,096 buckets per service (including services without
policies in the share count). Creating or dropping a service rebalances shares;
shrinking a share evicts its least recently used buckets. Under request pressure,
an ordered LRU index evicts even partially depleted buckets from that app only,
without scanning or refilling unrelated buckets. Rejected quota attempts count as
uses. Evicted identities get a fresh burst if they return, so rotating keys can
weaken enforcement; these are bounded admission controls, not durable quotas.
If an admission needs more simultaneous rule buckets than its app share, it
returns 429 with `Retry-After: 1` and the capacity outcome.

Every forwarded public request or WebSocket handshake carries a domain-separated,
single-use signed receipt, even when the forwarder has no policy. The owner trusts
the forwarder's admission decision, including absence of a policy during deploy
skew. Receipts have a separate 500,000-entry replay store, independent of actor-call
and control signatures. A receipt never grants actor privileges. A failed signing
attempt, invalid/expired/replayed receipt, full receipt store, or unsigned forward
claim with an app header returns 503 on a policy-enforcing public ingress path,
instead of charging the forwarder's IP. Client-supplied receipts are stripped
before forwarding and replaced with the node's proof. Mutual-TLS cluster-port
traffic bypasses public admission. Already-spent internal call signatures return
403 before body reads in plaintext mode; fresh signatures still undergo body and
atomic replay checks before actor work. Concurrent copies arriving before that
spend are stopped by the later atomic check, not by the early read-only check.

Offline or later-joining older binaries are not protected by the live-reader
check: they may treat format 6 as corrupt and serve without limits. Before
enabling policies, stop and upgrade every node, including offline nodes;
prevent an older binary from rejoining. Fail-closed handling of undecodable
manifests is a guarantee of rate-limit-aware binaries only.

With neither current nor pending policies, ingress admission returns immediately:
it does not change response ordering, capture a front snapshot, or increment rate
counters. Bucket read errors do not invent pending policies; corrupt manifests do.
The metrics endpoint still registers `wasmkeep_rate_limit_requests_total` and
`wasmkeep_rate_limit_active_keys` even when no policy exists, plus the `ingress`
record label in `wasmkeep_signatures_refused_full_total`. These metrics are the
intentional exception to no-policy compatibility.

### `[dev]` (optional, `wasmkeep dev` only)

| Key | Default | |
| --- | --- | --- |
| `port` | `8080` | on `127.0.0.1`; `--listen` overrides it |
| `data-dir` | `.wasmkeep/dev` beside the file | actor databases; `--data-dir` overrides it |
| `env` | `.env.local` (read if it exists) | local values, `NAME=value` lines, for the front and every class |

There is no key for a value, a secret or a bucket. Configuration goes in
the files `config` names; secrets go in the bucket with `wasmkeep secrets
set` ([Secrets in the bucket](cluster.md#secrets-in-the-bucket)); local
values go in `.env.local`, which belongs in `.gitignore` (a `config` that
names the `[dev] env` file, by any path or link, is refused). The bucket is
`deploy --bucket`, so one file deploys to staging and production alike.

## `wasmkeep deploy`

```bash
wasmkeep deploy --bucket s3://my-bucket/prod
```

1. Runs every `build` (the front's first, then the classes' in name
   order). A failed build stops the deploy before the bucket is opened.
   `--no-build` skips them. `--bucket`, and that every `config` file is
   there, are checked first. A build's output goes to stderr (stdout is
   the plan), and it runs without `AWS_ACCESS_KEY_ID`,
   `AWS_SECRET_ACCESS_KEY` and `AWS_SESSION_TOKEN`: the deploy's bucket
   credentials are not the build's. A build that needs credentials of
   its own (an S3 build cache, say) takes them from a profile or a file.
2. Reads every component and configuration file, and warns about
   configuration names that look like secrets.
3. Prints what changes against the deployed version, before anything is
   uploaded, then switches:

```
acme/todo from wasmkeep.toml, before switching:
  front: changed
  class rooms: removed (it was internal)
  access todos: open -> internal
  config todos: MODE removed
  class rooms is not in wasmkeep.toml: it is removed; its actors' state stays in the bucket
acme/todo: version 3c2f0a9e41d5b7a0 is current (deploy 7)
```

**The file is the whole truth.** A class or front not in the file is
removed. A class without `access` is `internal`. A class or front without
`config`, `requires` or `expire-after-idle` has none, even if a deploy
with flags set some.
Deploying the same file again changes nothing. A removed class's actors
keep their state in the bucket; a later deploy that brings the class back
finds it.

**Every rule is written** in the version, `open` included, so a class's
rule never depends on how the app was deployed. Such a version uses
manifest format 3, read by every 0.4.0 node; a class with
`expire-after-idle` makes it format 4, which only nodes of this release
read (the deploy is refused while an older node of the app is live). The
first deploy from the
file of an app deployed with flags may change only that; the plan then
says `access c: open, now written in the version`.

**With flags.** Beside a file, `--actor`, `--access`, `--config`,
`--requires`, `--expire-after-idle`, `--front`, `--no-front`,
`--front-config` and `--front-requires` are refused. `--app` may be given if it names the
file's app. To deploy with flags only, pass `--no-project` (or run from
another directory): such a deploy keeps every class's rule, and a class
it adds without `--access` is `open`, the default of flags. The next
deploy from the file makes the version the file's again.

**Rollback.** `wasmkeep deploy --rollback` reads only `[app]` (an
unknown key or wrong value elsewhere in the file does not block it) and
runs no build: it restores the previous version as the bucket holds it.
The file must still be TOML: if it does not parse, fix it, or roll back
without it with `wasmkeep deploy --no-project --app TENANT/APP
--rollback`. The file
may then describe another version; the next deploy from it deploys the
file. A rollback that would make a class more open (from a file's
`internal` back to a flag deploy's `open`, say) is refused without
`--allow-reopen`.

**Builds run commands from the file**, as yourself, as `make` or `npm
run` would: run `wasmkeep deploy` or `dev` only on a `wasmkeep.toml` you
would run as a script.

## `wasmkeep dev`

```bash
wasmkeep dev
```

Runs every `build`, then serves the app on `127.0.0.1:8080` as a node
serves a deployed version: the front for every path outside `/actor/`,
each class with its rule from the file (`/actor/todos/alice` answers 403
for an `internal` class), its configuration and required names, under
the file's tenant and app. Values come from the configuration files and
from `.env.local`, whose values reach the front and every class.

While it runs, it polls each item's `watch` paths, components and
configuration files:

- a change under `watch` runs that item's `build`; a change saved while
  it runs runs it again;
- a changed component or configuration file (after a build, or one run
  in another terminal) reloads the whole app, front and classes together;
- a failed build, or a component that does not link, keeps the previous
  version serving; after a failed build, nothing reloads until that item
  builds again, so a mix of new and old parts is never served.

Changes to `wasmkeep.toml` or `.env.local` need a restart. Beside a file,
crate directories, `--access` and `--env` are refused; `wasmkeep dev
--no-project CRATE...` runs crates as before.
