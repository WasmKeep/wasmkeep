# 0021. The app model: tenant, app, class, actor, front

- Status: accepted (2026-11-12), partly planned. Built in 0.3.0: the
  first `deploy` of an app refuses a second app of its tenant on a bucket
  prefix, and new apps' names follow the class grammar
  (`crates/wasmkeep-server/src/apps.rs`,
  `crates/wasmkeep-server/tests/cluster/deploys.rs`,
  `crates/wasmkeep-cli/tests/deploy_config.rs`). Planned for 0.4.0, each
  with its own ADR: the front, app-owned actor state, the project file.
  Later: several apps on the same nodes.
- Date: 2026-11-12
- Release: 0.3.0 and [0.4.0](../../ROADMAP.md#next)

## Context

`wasmkeep deploy --app acme/shop` ships actor classes, their access rules
and configuration as one version, and `serve --app acme/shop` follows it
([ADR 0008](0008-deploy-and-rolling-upgrades.md),
[ADR 0013](0013-access-rules-and-config-in-deploys.md)). Secrets are kept
per app ([ADR 0016](0016-secrets-in-the-bucket.md)). But the rest of an
app is not one thing yet:

- Actors are keyed by tenant, class and name (`actors/<tenant>/...`,
  `owners/<tenant>/...`), not by app. Two apps of one tenant on one bucket
  prefix share every actor whose class name they share. Only a node
  refused to start, and only while a node of the other app was live;
  `deploy` checked nothing.
- The stateless component that serves everything outside `/actor/` is a
  file on each node's command line, outside the deployed version: it is
  updated by hand on every node, and a rollback does not reach it.
- "Tenant" was used for two things: the owner of apps, and a customer of
  an app.
- The app name had no grammar; the tenant's came from actor addresses.

## Decision

### Words

- **Tenant**: the owner of apps: a team, or a client of the operator.
  It groups apps. A customer of your SaaS is not a tenant: it is an actor
  name (`account/customer-42`), or data inside actors.
- **App**: `tenant/app`, for example `acme/shop`. The unit of deploy,
  version, rollback and secrets, and, from 0.4.0, of actor state. Written
  `tenant/app` wherever a user types it. Staging is another app
  (`acme/shop-staging`), with its own state.
- **Class**: a kind of actor in one app: a component, its access rule and
  its configuration. `user` in `acme/shop` and `user` in `acme/admin` are
  unrelated once apps own their state.
- **Actor**: one instance of a class, by name, with its own database.
  Inside an app it is addressed `class/name`. The address a guest sees
  stays `tenant/class/name` (planned, 0.4.0: read relative to its own
  app), so no WIT change and no rebuild.
- **Front** (planned, 0.4.0): the app's optional stateless `wasi:http`
  component. It receives every request outside `/actor/` and
  `/_wasmkeep/`, holds no state, calls the app's actors with the same
  `actors.call` actors use, and ships in the app's version, so a deploy
  and a rollback move front and actors together. Routing, sign-in and
  hostnames are handled there, in the app's own code.

Tenant and app names of a new app match `[a-z][a-z0-9-]{0,63}`, the
grammar of classes. An app that already exists keeps its name: the rule
is checked by the first deploy, while the app has no `current`.

### An app owns its actors (planned, 0.4.0)

Two apps never share actors, whatever bucket prefix they use: each app's
actor state is kept apart from other apps', in the bucket and on disk
(the layout is fixed by its own ADR). Existing
data moves there once, by a migration, rather than keeping today's layout
beside the new one for good. The layout and the migration are fixed by
their own ADR.

Until then, **one app per tenant per bucket prefix**, enforced by
`deploy`: the first deploy of an app lists `apps/<tenant>/` and refuses
when another app there has a `current`, naming it and saying what to do
(another prefix, or another tenant when another owner runs the app; for
an abandoned app, the keys to delete). A listing that fails refuses too. Two apps of one tenant already
deployed together on one prefix are left as they were, and `deploy` and
`serve --app` warn about them by name; a further one is refused. The check adds
no object to the bucket. The node's start check against a live node of
another app of its tenant stays.

### Who may reach a class (planned, 0.4.0)

With a project file, `wasmkeep.toml`, a class is `internal` (reachable
from its app's actors, alarms and front only) unless it says otherwise.
Each class's rule is written in the version, `open` included: it is never
derived from whether the app has a front, so adding or removing a front,
or rolling back across one, does not change who reaches an actor. Apps
deployed with flags only keep today's default, `open`
([ADR 0010](0010-who-may-create-an-actor.md)).

### Several apps on the same nodes (later)

Until then a node serves one app; a machine can run several nodes.
Shared nodes come later, for one owner's apps and for tenants the
operator trusts. Their per-app limits protect against a noisy neighbour;
they are not isolation from a hostile one
([security](../security.md)).

## Consequences

- A first deploy with a 0.3.0 CLI no longer creates a second app of a
  tenant on a prefix without a word. It does not catch pairs deployed
  before the check (they keep sharing actors, with a warning), deploys by
  older CLIs, two first deploys at the same moment, or a new app that
  finds the actors an abandoned app left.
- A tenant is blocked on a prefix by an app nobody runs any more, until
  someone deletes its keys under `apps/<tenant>/<app>/`. There is no
  `app delete`; the error and [the deploy docs](../cluster.md#deploying)
  say which keys to delete. The abandoned app's actors stay under the
  tenant, and the next app finds those whose class name it shares.
- Two concurrent first deploys of two apps of one tenant can both pass
  the listing. The check prevents mistakes, not races; app-owned state
  removes the need for it.
- Apps split onto separate prefixes to satisfy the check stay where they
  are when 0.4.0 lifts it: moving an app to another prefix breaks its
  sealed secrets (ADR 0016).
- Metric labels, `cluster status` and errors are to use the same words
  (tenant, app, class, actor) as they gain app-level detail (planned).

## Alternatives

- **Keep apps on the bare tenant keys for good, and add an app root only
  for new apps.** Two layouts forever: every later feature that touches
  keys (garbage collection, export, deleting an app) would be written and
  simulated twice. Rejected for a one-time migration.
- **A marker object per tenant that claims the prefix for one app.** A
  new object and format for a rule that 0.4.0 removes. The listing needs
  neither.
- **Derive the access default from the front's presence.** Rejected
  above: the same manifest line would mean two things.
