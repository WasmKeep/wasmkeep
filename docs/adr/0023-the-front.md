# 0023. The front

- Status: accepted (2026-11-14), planned for 0.4.0
- Date: 2026-11-14
- Release: [0.4.0](../../ROADMAP.md#now)
- Follows: [ADR 0021](0021-the-app-model.md) ("Front") and
  [ADR 0022](0022-app-owned-state-and-the-one-time-migration.md) (the app
  between nodes).
- Amends: [ADR 0008](0008-deploy-and-rolling-upgrades.md) and
  [0013](0013-access-rules-and-config-in-deploys.md) (a version carries
  a front), [ADR 0010](0010-who-may-create-an-actor.md) (one more inside
  caller), [ADR 0016](0016-secrets-in-the-bucket.md) (a secrets scope for
  the front).
- Amended by: [ADR 0024](0024-socket-home.md) (the socket home gate is
  lifted).

## Context

ADR 0021 named the front: an app's optional stateless `wasi:http`
component that receives every request outside `/actor/` and
`/_wasmkeep/`, holds no state, calls the app's actors with `actors.call`,
and ships in the app's version. Today the stateless component is far from
that:

- **It cannot call actors or read configuration.** `HttpComponent::new`
  (`crates/wasmkeep-server/src/http.rs`) links WASI 0.2 and `wasi:http`
  only. `actors` and `env` are imported by the `actor` world alone
  (`crates/wasmkeep-wit/wit/actor.wit`).
- **It is outside the deploy.** It is the positional `component` of
  `serve`, loaded once at start. Changing it means replacing a file and
  restarting every node; a rollback does not reach it.
- **It sees whatever the client sent.** `App::route` hands it the request
  as received. A client can send `x-wasmkeep-caller` or `x-wasmkeep-actor`
  and the component cannot tell them from the host's.
- **It shares the actors' limits.** It runs under the node's one
  `--cpu-budget-ms` (`Runtime::store`), with a 30 s wall
  (`ServeLimits::stateless_wall`) equal to the 30 s an `actors.call`
  waits (`wall_per_event`). It has no metrics, and no bound of its own
  in the instance pool, beyond the tenth of the pool that
  `fds::resident_pool_limit` keeps from actors.
- **Routing today.** `/_wasmkeep/` is the server's; `/actor/<class>/<name>`
  goes to actors; everything else goes to the stateless component, or
  `404 not found` when there is none. The Host header is not consulted.

What the guest receives is already useful: wasmtime-wasi-http 50.0.1
gives the guest the client's Host as the request's authority, and drops
`host` and `upgrade` from the header list (`DEFAULT_FORBIDDEN_HEADERS`).
The scheme is fixed by the host to `http` (`Scheme::Http` in
`try_handle`).

## Decision

### The `front` world

Two new worlds in `wasmkeep:actor`. World `actor` and `actor-core` do not
change, so every built actor keeps working.

```wit
/// What the host links for a front, besides WASI and wasi:http.
world front-core {
    import actors;
    import env;
}

/// The world front components target.
world front {
    include front-core;
    include wasi:http/proxy@0.2.12;
    import wasi:cli/environment@0.2.12;
    import wasi:cli/exit@0.2.12;
    import wasi:cli/terminal-input@0.2.12;
    import wasi:cli/terminal-output@0.2.12;
    import wasi:cli/terminal-stdin@0.2.12;
    import wasi:cli/terminal-stdout@0.2.12;
    import wasi:cli/terminal-stderr@0.2.12;
    import wasi:random/insecure-seed@0.2.12;
}
```

- The `wasi:cli` and `wasi:random` imports are the same as world
  `actor`'s, for the same reason: standard libraries and ComponentizeJS
  import them.
- The front exports only `wasi:http/incoming-handler`. It has no
  `storage`, `alarms` or `websockets`, and no `on-*` export.
- The host links a front as it links actors: all of WASI 0.2, inert (no
  preopens, sockets and name lookups refused), `wasi:http` with the
  node's egress policy, then `actors` and `env`. So every plain
  `wasi:http/proxy` component, today's stateless components included, is
  a valid front.
- The package version stays `@0.2.0`: nothing is removed or changed, and
  a world is added.
- All five WIT copies carry the new worlds (the check script diffs them).
  No interface function is added, so the SDK name table
  (`crates/wasmkeep-wit/tests/api_reference.rs`) gains no row.
- **SDKs**: Rust and TypeScript get a front target in 0.4.0: a `front`
  module in the Rust SDK generated from `front-core`, and
  `wasmkeep-ts --world front`. Each has the helpers below and one
  example. Python, Go, C, Zig, D and MoonBit keep building actors
  unchanged; their front targets come later. The project rule that a WIT
  change updates every SDK in the same change applies to changes of an
  existing world; an added world is adopted SDK by SDK.

### Routing

`App::route` keeps its order:

1. `/_wasmkeep/...`: the server's, as today.
2. `/actor/<class>/<name>...`: the actors, as today, with each class's
   access rule.
3. Every other path: the front of the version the node runs. Without a
   front: `404 not found`, as today without a stateless component.

`/actor/` never reaches the front. Today, on a node with no actor service
(`serve COMPONENT` alone), `App::route` hands `/actor/...` to the
stateless component; from 0.4.0 it answers `404 not found` there.

The front is served on the public port only. It is never forwarded: a
front request runs on the node that received it. The cluster port keeps
serving `/actor/` and the health paths only.

An upgrade request outside `/actor/` reaches the front as it reaches the
stateless component today: a GET without `upgrade`. The host enforces
that its answer is never an upgrade: wasmtime-wasi-http lets a guest set
any status, `101` included, so a `1xx` status from the front is replaced
by `500` and counted as `guest_error`. 0.4.0 has no WebSocket at the
front's paths (see "The WebSocket hand-off").

### The front calls actors as an inside caller

- `actors.call("class/name", request)` from a front works as from an
  actor: buffered, 16 MiB each way, one call at a time per front
  instance. The callee is in the front's own app.
- The call is **inside** (ADR 0010): the front reaches `internal`
  classes, and creates actors of any class by calling them, as actors
  do. Access rules are checked for outside requests only (`admit`,
  `actor/state.rs`), and a required value that is not set refuses the
  call, inside or out.
- The front cannot be called. `_front` does not match the class grammar
  (`is_identifier`: a lowercase letter first), so `actors.call("_front/main")`
  fails with `invalid-address`, and `/actor/_front/...` answers 404.
- **Its caller value is `<tenant>/_front/main`.** The callee reads it in
  `x-wasmkeep-caller`, as it reads an actor's `<tenant>/<class>/<name>`.
  - Three parts, so code that splits the header keeps working.
  - It cannot be an actor: `_front` is never a class.
  - The app is implicit. ADR 0022 makes every address and caller relative
    to the app, and every hop between nodes carries the app in the signed
    `x-wasmkeep-app`; a node refuses a hop of another app. So the fronts
    of `acme/shop` and `acme/admin` have the same caller value and can
    never reach each other's actors.
  - `main` names the one front an app has, so a second one could be
    added later without a new format.
- **Signed as today.** The signature (`cluster/signing.rs`) covers
  `x-wasmkeep-caller` as a string, and the receiving node passes it to
  the guest without parsing it. No change to the signed list.
- **The calling node's types change.** `CallContext.me` and
  `build_request(from, ...)` are typed `ActorAddress`, which `_front`
  cannot be. The call path gets a caller type with two cases, an actor
  or the front. The front is never put in the chain
  (`x-wasmkeep-chain`): the chain lists actors waiting on a call, so
  that a call back to one fails `reentrant`, and a front cannot be called
  back. A front's call starts an empty chain.
- **`self-address`** in a front returns
  `{ tenant: <tenant>, class: "_front", name: "main" }`.
- **`env.get`** reads the front's values (see "Deploy").
- **SDK helper `caller()`** (Rust and TypeScript first): `Front`,
  `Actor(address)`, or `Outside` when the header is absent. Actors use it
  where it matters: `if caller() != Caller::Front { return 403 }`.

### Header hygiene

- **Requests to the front**: before the front sees a request, the host
  removes every header whose name starts with `x-wasmkeep-`. That covers
  the caller, actor and app headers, the signature (`x-wasmkeep-internal`),
  the chain, the forwarded mark, the node-call markers, and any name the
  host adds to that range later. `upgrade_headers`
  (`cluster/forwarding.rs`) already strips this range from relayed
  upgrades.
- **Requests from any guest**: `build_request` (`actor/routing.rs`),
  shared by actors and the front, drops every `x-wasmkeep-` header the
  guest set, then sets `x-wasmkeep-actor` and `x-wasmkeep-caller` itself.
  Today it drops only hop-by-hop headers, `x-wasmkeep-alarm-handoff`,
  `x-wasmkeep-take` and `x-wasmkeep-app`, and a local call reaches the
  callee without the stripping `requests.rs` does for requests from the
  port, so a guest-set `x-wasmkeep-forwarded` or `x-wasmkeep-node`
  reaches the callee unchanged. One rule for every caller.
  - Note (2026-11-14, while building it): four names in the range are the
    SDKs' own protocol between actors, sent by every SDK's key-value and
    queue clients since 0.1.0: `x-wasmkeep-kv-expect`,
    `x-wasmkeep-kv-ttl-ms`, `x-wasmkeep-kv-durable` and
    `x-wasmkeep-queue-delay-ms`. Dropping them would break those clients,
    built ones included, against "Built actors run unmodified" below. So
    `build_request` keeps exactly these four names (the host gives them
    no meaning) and drops every other name in the range. Moving them out
    of the range is an SDK protocol change for a later release, after
    which the exception goes. The front's own edges strip the whole range.
- **Responses from the front**: the host removes every `x-wasmkeep-`
  header from the front's response before the client gets it. This
  reserves the range for the host's own answers, including the 0.4.1
  hand-off's control header, from the first release with a front.
- **Trailers** follow the same rule both ways, and the removal is
  silent: the front may set any name, an `x-wasmkeep-` one included
  (wasmtime's `fields` calls never refuse it), and the host drops it on
  the way out. A front that copies the headers of an answer it received
  therefore never fails on a name in the range.
  - Note (2026-11-14, review): the first build forbade the range at the
    guest boundary instead, so `fields.append`, `set` and `from-list`
    returned `forbidden`. A call answered by another node carries the
    host-set `x-wasmkeep-node`, so a front copying its callee's headers
    trapped on a cluster only. The host now removes the range at the
    front's edges, and from what `actors.call` returns to any guest (with
    the hop's framing headers, as the body is whole): no SDK reads a name
    of the range from an answer. The front's own outbound HTTP is the
    guest's, as an actor's.
- **Requests to actors from outside**: an unsigned request to
  `/actor/...` (a client's, or one a node forwarded) loses the whole
  range too, but the forwarded mark the host reads to forward a request
  only once; the host then sets `x-wasmkeep-actor` itself. Before, only
  the names the host reads were removed, so a client could send the SDKs'
  key-value and queue names to an open actor that relays its headers.

### Host and scheme

- `host()`: the request's authority, which wasmtime-wasi-http fills from
  the URI or, for the usual origin-form request, from `Host`. Behind a
  proxy it is the client's Host only if the proxy passes it;
  [the front door](../deploy/front-door.md) nginx example already sets
  `proxy_set_header Host $host` and `X-Forwarded-Proto $scheme`.
- `scheme()`: `https` when the first `X-Forwarded-Proto` value is
  `https`, else `http`. The host keeps passing `http` as the request's
  scheme. A client can send `X-Forwarded-Proto`; `scheme()` is only as
  good as the proxy that replaces it, and the docs say so.
- Both are SDK helpers in Rust and TypeScript, not host functions.
- An actor reached by a call does not see the client's Host. The front
  passes what the actor needs (the customer, the hostname) explicitly.

### Deploy

**CLI.**

```
wasmkeep deploy --app acme/shop --bucket s3://b/p --front front.wasm
wasmkeep deploy --app acme/shop --bucket s3://b/p --front-config front.env --front-requires SESSION_KEY
wasmkeep deploy --app acme/shop --bucket s3://b/p --no-front
```

- `--front PATH` sets the front; `--no-front` removes it. With neither,
  the current front is kept, copied inside the bucket as classes are
  when `--actor` is not given.
- `--front-config FILE` and `--front-requires NAMES` are the front's
  `--config` and `--requires`, with the same rules (`FILE` empty, or
  `NAMES` empty, removes them). They have their own spelling so that
  `--config`, `--requires` and `--access` keep taking classes only.
- `--rollback` conflicts with these four flags, as it does with
  `--actor`, `--access`, `--config` and `--requires`. A rollback restores
  the previous version's front with its classes.
- The deploy validates the front before writing anything: it must
  instantiate against the front linker and export the incoming handler.
  A component that imports `storage`, `alarms` or `websockets`, or
  exports any of `actor-core`'s `on-*` functions (an actor given as a
  front by mistake), is refused.
- A first deploy may give a front and no class (`--front` alone): the
  app then serves its front, and its calls fail with `invalid-address`.
  Today `write` refuses a first deploy without `--actor`; it accepts one
  with `--front`.
- `current` does not change: on 0.4.0 every app's `current` carries
  token `3` (ADR 0022), which implies ADR 0013's marker, so `serve --app`
  already takes rules from the deploys.
- The output says `front: added`, `changed`, `removed` or `unchanged`,
  and lists each class whose rule is `open` with
  `reachable at /actor/<class>/<name>, past the front`.

**Access rules do not change.** Per ADR 0021, a class's rule is never
derived from whether the app has a front. A version deployed with flags
keeps today's default, `open`, front or no front; `--access` sets
another. In every manifest format, an absent `access` line means `open`.
Format 3 also accepts an explicit `access <class> open` (formats 1 and 2
refuse it, `Manifest::parse`), so that the project file, which writes
every class's rule (ADR 0021), needs no further format. Deploys with
flags keep writing `open` as absence.

**Bucket.** In the version directory, beside the classes:

```
apps/<tenant>/<app>/<version>/_front.wasm     as uploaded, immutable
apps/<tenant>/<app>/<version>/_front.config   plain configuration, immutable
```

`_front` cannot collide with a class's objects, which start with a
letter. Deploy collection (ADR 0012) removes them with their version.

**Manifest format 3, written only with a front.**

```
wasmkeep-manifest 3
previous 1a2b3c4d5e6f7081
class cart 9f86d0...
class room 60303a...
front main 2c26b4...
access room existing
config _front fcde2b...
requires _front SESSION_KEY
```

- `front main <sha256>`: at most one, the name `main` only.
- `config _front <digest>` and `requires _front <NAME>` take the front's
  scope. `access _front ...` is refused: the front has no rule.
- The `front` line is part of the version's identity, after the class
  table, so a deploy that changes only the front gets a new version id.
- A version without a front is written exactly as today (format 1 or 2,
  same bytes), so existing version ids do not change.
- Parsing format 3: the check that every `access`, `config` and
  `requires` key is a class of the version accepts `_front` for `config`
  and `requires` only, and only when a `front` line is present. Where a
  deploy keeps the current values for the classes still there (the
  `retain` on access and configuration, the filter that carries
  requires over), it keeps the `_front` entries while the version keeps
  a front, and a deploy that removes the front removes them.
- A node or CLI that cannot parse a manifest treats the version as
  corrupt, as today (`Manifest::parse` returns `None`): such a node stays
  not ready. Hence the capability word below.

**Nodes.** A node loading a version reads `_front.wasm` (digest
checked), links it before taking any lock, then swaps it with the classes
and the deploy sequence under the classes' write lock
(`apply_version_with`). A version whose front does not link is handled
like a version with a class that does not link. The front's values are
built under the same lock, from the same three layers as a class
(deployed configuration, the app's secrets, the node's values), and
`apply_secrets` rebuilds them too. A request in flight finishes on the
front it started with; the next request takes the new one.

**Required values.** While a name in `--front-requires` is not set, the
front is not instantiated: the host answers a generic `503` with
`Retry-After: 1` ("this node cannot serve the app now; retry"), as an
actor's outside request gets when its event is refused
(`actor/requests.rs`). The missing name goes to the log, once (ADR
0016), and to the `unconfigured` outcome, never to the client: the port
faces the public. While the node cannot open the app's secrets, the
front answers the same 503.

### Secrets

- The front's secrets are the scope `_front`:
  `wasmkeep secrets set --front NAME`, `secrets unset --front NAME`, and
  `_front <name> <length>` entries in `set --file`. `secrets list` shows
  the scope.
- `check_entry` (`secrets.rs`) accepts `_front` besides the class grammar.
  Sealing does not change (ADR 0016 binds format, prefix, tenant and app).
- **Refused while a live node lacks `front`.** One entry a node cannot
  check makes `SecretSet::decode` fail for the whole set, so a node of an
  older version would stop opening every secret of the app, refuse events
  and drain. So a secrets write whose resulting set has any `_front`
  entry is refused while a live node of the app does not announce the
  capability word `front`. This is the check `set` makes for the word
  `secrets` today (`nodes_without_secrets`), with one difference: no
  `--force`. Forcing the `secrets` check only makes older nodes serve
  new values as unset; here every such node would stop opening all of
  the app's secrets, refuse every event and drain.
- **The first deploy with a front** is refused the same way, for format
  3: every deploy that writes a format 3 manifest checks the heartbeats
  first. It has no `--force`: such a node could not read the version at
  all.
  - Note (while building it): `--rollback` makes a version current
    without writing it, so a rollback to a version with a front is
    checked the same way.
  - Note (while building it, after review): for secrets, the refusal is
    of a write that gives the set its first `_front` entry, counting only
    nodes that read secrets from the bucket. When the set already has
    one, such a node opens nothing already: refusing a class's change or
    a key rotation then protects no one and blocks a fix. `unset` and
    `rotate-key` add no entry, so they are never refused; `reset`, which
    cannot open the current set, is judged on the set it writes.
  - The check sees the nodes that write heartbeats: a `serve --app`
    without `--node-id` writes none, as for the word `secrets`. It counts
    a heartbeat as live with the default timings against the clock of the
    machine running the command, as `cluster status` does.

### `serve`

- **With `--app`, a positional component is refused at start.** The
  front comes from the deploy only. The error names the command:
  `wasmkeep deploy --app T/A --front PATH`. This is stricter than the
  rule `--access` follows (`serve_app_rules` refuses it once the app's
  rules come with its deploys): the refusal does not depend on what is
  deployed.
- **Without `--app`** (`serve COMPONENT [--actor ...]`, the implicit app
  `default/default`), the component is linked as a front: its calls reach
  the `--actor` classes and its caller value is `default/_front/main`.
  With no `--actor`, its calls fail with `invalid-address`.
- **`--front-env FILE`**, with or without `--app`, gives the front's node
  values, as `--env CLASS=FILE` does for a class (the third layer).
  `--env _front=FILE` is refused: `--env` takes classes.

### Limits

- **Wall**: 30 s per front request, as today (`stateless_wall`),
  including streaming the response.
- **Call deadline**: an `actors.call` from a front fails with `timeout`
  when 5 s of the request's wall remain, or after the actors' own call
  limit (`wall_per_event`), whichever comes first. A call started with
  less than 5 s left fails at once. The front always has time to answer
  its own timeout.
- **Slots**: `--max-front-requests N` bounds the front requests running
  at once on a node. The default is the part of the instance pool kept
  from actors today, `max_instances.div_ceil(10)` (100 for the default
  pool of 1,000) on a node that serves actors; a node that serves only a
  front gives it the whole pool, as before it had slots of its own
  (amended 2026-11-14). Past it the host answers `503` with `Retry-After: 1`,
  without instantiating. The resident actor limit becomes
  `max_instances - max_front_requests` (at least 1) instead of
  `resident_pool_limit(max_instances)`, so the slots are always there in
  the pool: a larger `--max-front-requests` takes room from actors, never
  from itself. `serve` refuses a value that leaves actors no room. The
  front is the only short-lived instance left once the stateless
  component becomes the front.
- **Per actor**: at most a quarter of the slots (at least 1) may wait at
  once on calls to one actor address. Past it the call fails at once with
  `failed("front busy: too many front requests waiting on this actor")`.
  This keeps one hot or stuck actor from taking every slot. The bound is
  per address: the calls waiting on the many actors of one unreachable
  node each count against their own actor, so together they can still
  fill the slots, each until its call deadline (see "Not decided here").
- **CPU**: `--front-cpu-budget-ms`, default 50 ms, separate from
  `--cpu-budget-ms` (actors). The runtime gains a store with its own
  budget. Past it the request answers 500, as today.
- **Errors**: `call-error` keeps its cases; adding one would break every
  built actor. Limits reach the front as `failed(...)` with a stable
  prefix (`front busy:`, `callee overloaded`, and the capacity 503 of the
  callee's node as a response). The SDK helper `unavailable(err)` maps
  `failed` and `timeout` to `503` with `Retry-After: 1`.
- Unchanged: the 16 MiB request body, 128 MiB memory, the outbound HTTP
  limits and the guest log rate.

### Metrics

- `wasmkeep_front_requests_total{outcome}`: `ok`, `busy` (no slot),
  `unconfigured`, `cpu_budget`, `wall`, `guest_error`.
- `wasmkeep_front_request_seconds`: a histogram, to the response's last
  byte.
- `wasmkeep_front_slots_in_use`: a gauge.
- The counters and the histogram carry `tenant` and `app`, like the other
  series of the app's work.
- `wasmkeep_front_calls_total{outcome}`: `ok`, `busy` (the per-actor
  bound), `timeout`, `failed`.
- A front's calls to another node are already counted by
  `wasmkeep_forwards_total{kind="call"}`. The front's log lines carry
  `actor=_front`.

### Placement with a front

Pretty paths cannot be hashed to an actor by the load balancer. A
hibernated actor has no owner (ADR 0020), so a front's call claims it on
the front's node; actors settle where front traffic lands. A call from
another node pays one signed hop, after an owner read that is cached for
one second (`HINT_MS`). This is a stated cost, watched with
`wasmkeep_forwards_total` and the bucket request counters before
anything is tuned. With a round-robin balancer a new node fills by itself.

### Socket home: a gate for clustered use

> Lifted by [ADR 0024](0024-socket-home.md) (0.4.0): a room with open
> sockets keeps its ownership record while it hibernates, so a call from
> any node's front wakes it on the node that holds its sockets. Socket
> classes can use the front on a cluster. The text below is the gate as
> decided.

Sockets stay on their node when the actor hibernates and its ownership
is released. When another node then takes the actor, its sockets are
closed with 1012 (`actor/socket_owners.rs`, `ws_prepare`). With a front,
any node's front may wake a hibernated room, and every client of that
room reconnects. On one node nothing changes.

So the front is not recommended for classes with sockets on a cluster
until a separate ADR, planned for the same release, decides where an
actor with open sockets wakes, with its simulation case and review. Until then the docs say: reach
socket classes at `/actor/<class>/<name>` with a hashing balancer. The
mechanism is not chosen here; keeping ownership across hibernation is
the alternative ADR 0020 rejected.

### The WebSocket hand-off: not now

A WebSocket at one of the front's own paths needs the host to take
the upgrade and open the socket on an actor the front names. It is
planned for 0.4.1, in its own ADR. 0.4.0 reserves what it needs:

- the `x-wasmkeep-` range on requests to the front, where the host will
  mark an upgrade, and on responses from the front, where the front will
  name the actor; both are stripped from 0.4.0 on;
- the rule that an upgrade outside `/actor/` reaches the front as a plain
  GET.

Until then a socket class is `existing` or `open`, reached at
`/actor/<class>/<name>`; outside upgrades and socket events are admitted
as outside (`admit(true)`, `actor/upgrade.rs`, `actor/sockets.rs`). An
`existing` room must be created first, for example by the front calling
it.

### Compatibility

- **Built actors** run unmodified: `actor` and `actor-core` do not
  change. A new caller value, `<tenant>/_front/main`, can appear. The
  SDKs' key-value, queue and workflow handlers admit any request with
  `x-wasmkeep-caller`, so they now admit the app's front, which is
  intended. Their `allow(caller)` callbacks may receive
  `<tenant>/_front/main`, which is not an actor address; their docs say
  so, as does the queue's delivery handler, which documents the caller as
  the sending queue's address. The doc comment of `actors.call` in the
  WIT (all five copies and the generated Go bindings) gains the front's
  value. An older build's code cannot tell the front from an actor without
  the new `caller()` helper.
- **Built stateless components** are valid fronts as they are.
- **Old binaries.** Format 3 and the `_front` scope ship with the
  heartbeat capability word `front` and the CLI refusals above. The word
  goes on the capability line beside `take`: the last line of the
  payload nodes write (`NodeInfo::payload`), line 11 of the older
  encoding. `payload()` writes the words as a list rather than the fixed
  `take`, `NodeInfo` gains a field beside `can_take`, and unknown words
  stay ignored (`NodeInfo::words`). With every node of a
  cluster upgraded together, no 0.4.0 node lacks it; the word makes the
  rule checked rather than assumed, as the roadmap asks of new formats
  for 1.x.
  A 0.3.0 node cannot run on a 0.4.0 bucket anyway (ADR 0022).
- **Behaviour changes**, for the 0.4.0 notes:
  - with `--app`, a component on the `serve` command line is refused;
  - without `--app`, that component gets `actors` and `env`;
  - a client's `x-wasmkeep-*` headers and trailers no longer reach the
    stateless component, and its own `x-wasmkeep-*` response headers and
    trailers no longer reach the client;
  - a client's `x-wasmkeep-*` headers no longer reach an actor (but the
    forwarded mark), and `actors.call` no longer returns any, nor the
    hop's framing headers;
  - a client leaving before the end of its answer stops the front's
    instance (a whole answer is not a client gone);
  - no free instance for the front answers 503 with `Retry-After: 1`
    instead of 500;
  - `--app` with `--max-instances 1` is refused at start: an `--app`
    node keeps front slots for the deployed front;
  - the front's CPU budget is its own flag;
  - a guest's own `x-wasmkeep-*` headers on `actors.call` no longer
    reach the callee, for actors too;
  - `/actor/...` no longer reaches a stateless component on a node
    without actors;
  - the guest log names the front `actor=_front` instead of
    `actor=stateless`.

## Consequences

- An app is one deploy: front, classes, rules and values move and roll
  back together, without restarting a node.
- Sign-in is written once, in the front, and reaches `internal` classes.
  A deploy or rollback to a weaker front opens them: `--allow-reopen`
  compares rules only. The deploy prints `front: changed`, and sensitive
  actors should still check `caller()`.
- The front creates actors from the names it passes. A name taken from a
  URL creates a database per guessed name; the docs tell the front to
  validate names before calling.
- Nodes switch to a new version a few seconds apart. A front on a switched
  node can call an actor on a node not yet switched; versions must stay
  compatible with the one before, as for classes today.
- With a front, actors wander to where front traffic lands, and calls
  between nodes add a hop. The cost is measured, not yet tuned.
- On a cluster, classes with sockets keep their `/actor/` URL until the
  socket-home ADR.

## Alternatives

- **New host functions for the front** (a typed actor stub, a
  streaming or concurrent call). More SDK work in every language for a
  first version. The front uses `actors.call`, which every SDK has.
- **The front as an actor class** (`existing:main`). One instance on one
  node, one event at a time: the single object every request touches.
- **Two-part caller value** (`_front/main`). Code that splits the header
  into three parts breaks. The three-part form costs nothing, since the
  app travels in `x-wasmkeep-app`.
- **Deriving `internal` from the front's presence.** Rejected by ADR 0021.
- **Keeping the positional component with `--app`.** Two sources for one
  thing, and a rollback that does not reach it.
- **Stripping named headers only.** A header added to the range later
  would leak. The prefix is the rule `upgrade_headers` already follows.
- **Front config in `--config _front=FILE`.** One flag would take
  classes and a reserved scope; a typo in `_front` would fail late.

## Not decided here

- The project file (`wasmkeep.toml`), its `[front]` section, its default
  of `internal`, and how `deploy` and `dev` read it.
- `wasmkeep dev` with a front.
- Where an actor with open sockets wakes (its own ADR, a gate for
  clustered use).
- The WebSocket hand-off through the front (0.4.1, its own ADR), and one
  client socket for several actors (0.5.0, its own ADR).
- Front targets for Python, Go, C, Zig, D and MoonBit.
- A call that does not create the actor, streaming or concurrent calls,
  and several fronts per app.
- A bound per target node on the front's calls waiting at once (the
  actors of one unreachable node can together hold every slot under the
  per-actor bound), to be measured before a number is chosen.
- The front's cost per request in interpreted languages: a fresh
  instance per request starts the engine each time. To be measured before
  a number is promised.
