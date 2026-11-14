# 0026. The project file

- Status: accepted (2026-11-14), built in 0.4.0
- Date: 2026-11-14
- Release: [0.4.0](../../ROADMAP.md#m4-operating-a-cluster)
- Follows: [ADR 0021](0021-the-app-model.md) ("Who may reach a class":
  with a project file a class is `internal` unless it says otherwise) and
  [ADR 0023](0023-the-front.md) ("Not decided here": the project file,
  its `[front]` section, its default of `internal`, how `deploy` and
  `dev` read it, and `wasmkeep dev` with a front).
- Amends: [ADR 0013](0013-access-rules-and-config-in-deploys.md) (a
  deploy may write every class's rule, `open` included) and
  [ADR 0023](0023-the-front.md) (format 3 is also written without a
  front, for an explicit `open`).

## Context

An app is a front, classes, each class's rule, configuration and
required names (ADR 0021, 0023). Today it exists only as a `wasmkeep
deploy` command line:

```
wasmkeep deploy --app acme/todo --bucket s3://b/p \
  --front front.wasm --front-config front.env --front-requires TOKENS \
  --actor todos=todos.wasm --access todos=internal
```

- **It is not written anywhere.** Each deploy changes what it names and
  keeps the rest (ADR 0013): the version is the sum of every deploy
  since the first. A class removed from a script stays deployed; a
  `--config` dropped from it keeps the old values.
- **The default is open.** A class nobody gave `--access` is reachable
  from outside, creating actors for any name (ADR 0010). ADR 0021 makes
  a class `internal` by default with a project file, and keeps `open`
  for flag deploys, so that no existing script changes meaning.
- **`wasmkeep dev` is not the app.** It builds Rust crates given on the
  command line, serves them without a front, with `--access` and
  `--env` flags of its own, and its rules are not the deployed ones.
- **Building is outside.** Each example's README lists the `cargo build`
  (or `npm run build`) of each part before the deploy.

What must not change: deploys with flags only, their default of `open`,
the manifest formats nodes read, secrets (ADR 0016), and rollbacks.

## Decision

### The file and where it is found

`wasmkeep.toml`, TOML 1.0, in the app's repository, committed. `wasmkeep
deploy` and `wasmkeep dev` read `./wasmkeep.toml` (the current directory
only, no search upwards: a file in a parent directory is someone else's
app as often as it is yours), or the file `--project PATH` names (a file,
or a directory holding `wasmkeep.toml`). `--no-project` ignores a
`wasmkeep.toml` in the current directory, for a deploy with flags only.

Relative paths in the file resolve against the file's directory, never
the current directory, so `wasmkeep deploy --project app/` and `cd app &&
wasmkeep deploy` do the same thing.

### Schema

```toml
[app]
tenant = "acme"                 # required
name = "todo"                   # required

[front]                         # optional: no [front], no front
component = "front/target/wasm32-wasip2/release/todo_front.wasm"
build = "cd front && cargo build --release --target wasm32-wasip2"
config = "front.config"         # plain configuration, NAME=value lines
requires = ["TOKENS"]           # names that must be set (ADR 0016)
watch = ["front/src", "front/Cargo.toml"]   # for `wasmkeep dev`

[classes.todos]                 # one table per class; the key is its name
component = "todos/target/wasm32-wasip2/release/todo_todos.wasm"
access = "internal"             # the default; or "existing",
                                # "existing:NAME,...", "open"
build = "cd todos && cargo build --release --target wasm32-wasip2"
config = "todos.config"
requires = ["API_KEY"]
watch = ["todos/src"]

[dev]                           # `wasmkeep dev` only
port = 8080                     # on 127.0.0.1; --listen overrides
data-dir = ".wasmkeep/dev"      # --data-dir overrides
env = ".env.local"              # local values, never committed
```

- `[app]`, `tenant` and `name` are required, with the grammar of new apps
  (ADR 0021, `[a-z][a-z0-9-]{0,63}`).
- `component` is required in `[front]` and in each class. Every other key
  is optional.
- **Classes are `[classes.<name>]` tables**, not an array of tables: the
  name is the key, so a class given twice is a TOML error (a duplicate
  table), and the name is checked where it is written. A class name has
  the grammar of classes; `_front`, the front's scope, is not one. A file
  may have a front and no class, or classes and no front, but not
  neither.
- `access` takes what `--access` takes (`open`, `existing`,
  `existing:NAME,...`, `internal`), default `internal`. The front has no
  `access` (it receives every request outside `/actor/`, ADR 0023).
- `requires` is a list of names, as `--requires`; a name given twice is
  refused. `config` names a file of `NAME=value` lines, as `--config`.
- **Unknown keys and tables are refused**, with the file, line, column
  and key, and the keys that section takes: `acess = "open"` fails the
  deploy instead of leaving the class `internal` while its author thinks
  it open, and `[clases.todos]` fails instead of deploying no class.
  Wrong types (`requires = "A"`, `port = "80"`) are refused the same way.
- **No values in the file.** There is no key that holds a configuration
  value or a secret: `config` and `env` name files. Configuration is
  stored in the bucket in the clear, as with `--config` (the deploy warns
  about names that look secret, as it does); secrets stay `wasmkeep
  secrets set` (ADR 0016). `[dev] env` is read by `dev` only, never by
  `deploy`, and its default, `.env.local`, is meant for `.gitignore`. A
  `config` naming the `[dev] env` file is refused: it would upload local
  values in the clear. It is the same file however it is reached: the
  check compares the files themselves (device and inode, or canonical
  paths) where the configuration is read, and paths with `.` and `..`
  resolved when the env file does not exist.
- **No bucket in the file.** The bucket is where an app is deployed
  (staging, production), not part of the app: `--bucket` stays a flag. A
  deploy names it every time, so the same file deploys anywhere.

### The file is the whole truth

A deploy from the file deploys exactly the file, not changes to the
current version (ADR 0013's "classes not named keep their rule" does not
apply):

- the classes are the file's: one the file no longer has is removed;
- every class's rule is the file's, `internal` where it says nothing;
- a class or front without `config` has none, and one without
  `requires` requires none: values a flag deploy set are removed;
- the front is the file's: no `[front]` removes the current front.

The deploy prints what changes **before it switches** (before anything
is uploaded): each class added, changed or removed (with its rule),
each rule that changes, configuration names added, changed or removed
(never a value), required names, and the front. A removed class's actors
are not deleted: their state stays in the bucket, and a later deploy
that brings the class back finds it. Deploying the same file twice
changes nothing the second time (same version), as today.

There is no confirmation prompt: deploys run from scripts and CI, and
the file is reviewed where it is committed. What a deploy would do is
the first thing it prints.

### Every rule is written

A version deployed from the file carries an `access` line for every
class, `open` included (ADR 0021): a class's rule is never derived from
an absent line, whether the app has a front, or how it was deployed.
ADR 0023 made format 3 accept `access <class> open`; a version with such
a line is written in format 3, with or without a front, and the deploy
is held back by nodes that cannot read format 3 as a front is
(`nodes_without_front`). A version whose classes are all restricted
needs no `open` line and keeps format 2.

An explicit `open` and an absent rule mean the same to a node (`open`).
They differ in the version's identity, so the first deploy from the file
of an app deployed with flags makes a new version even when nothing else
changes; its plan says so (`access c: open, now written in the
version`), and a flag deploy dropping the line says `no longer written`:
a new version never comes with an empty plan.

**A deploy with flags after one from the file** (`--no-project`, or from
another directory) keeps each class's rule, as flag deploys do; an
explicit `open` it carries over is written as absence, as flag deploys
write it (the same rule). A class it adds without `--access` is `open`,
the default of flags (ADR 0021). The next deploy from the file writes
every rule again. A rollback restores the previous version as it was
written, explicit `open` lines included, and refuses to reopen a class
unless `--allow-reopen` (ADR 0013): rolling back from a file deploy to
the flag deploy before it can make a class that the file made `internal`
`open` again, and is refused for that.

### Flags with the file

With a project file, the flags that say what the version is (`--actor`,
`--access`, `--config`, `--requires`, `--front`, `--no-front`,
`--front-config`, `--front-requires`) are refused, naming the file and
`--no-project`. Two sources for one version would disagree sooner or
later, and "the flag wins" makes the file a lie the next time someone
reads it. `--app` may be given if it names the file's app, and is
refused otherwise. `--bucket`, `--version-retention-secs` and
`--rollback` are not part of the version and are taken as before.

`dev` likewise refuses crate directories, `--access` and `--env` with a
file; `--listen`, `--data-dir`, `--poll-ms` and the outbound and log
flags are the machine's, and override `[dev]` where both exist.

### Builds

Each `build` is a command line run by the system shell (`sh -c` on Unix,
`cmd.exe` on Windows) in the file's directory, with its output on the
terminal, before its `component` is read: the front's first, then the
classes' in name order. `deploy` and `dev` run them, so neither is tied
to Rust: `npm run build`, `make`, `tinygo build` are commands like
`cargo build`.

- A build that fails stops the deploy before the bucket is opened:
  nothing is written. `dev` stops at start, and keeps the previous
  version serving after a failed rebuild.
- `--no-build` skips them (a CI that built the components in an earlier
  step); without a project file it is refused. `--bucket` is checked
  before any build, so a mistyped one costs no build.
- **`--rollback` runs no build** and reads only `[app]` from the file: a
  rollback restores what the bucket holds, and the file may already
  describe a version not deployed yet, typos included, which must not
  block the way back. Only the rest's keys and values go unchecked: the
  file must still parse as TOML (finding `[app]` in a file that does not
  parse is a guess, and a wrong guess rolls back another app), and the
  error then says `--no-project --app TENANT/APP --rollback`.
- Builds run without the deploy's bucket credentials
  (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`):
  a command from the file needs no bucket, and one that does (a build
  cache) has its own. Their output goes to stderr, so stdout is the
  plan.
- Builds are commands from the file, which the person deploying runs as
  themselves, as they would run the repository's `Makefile` or `npm`
  scripts: the file is code of the app's repository, not input from
  elsewhere. Run nothing from a `wasmkeep.toml` you would not run as a
  script.

Paths may leave the file's directory (`../todo-front/todos/...`): an app
can take a class from a sibling directory, as `examples/ts-todo-front`
does. Paths name files read on the deploying machine only. What reaches
the bucket is named by class (validated as above) and `_front`, never by
a path.

### `wasmkeep dev` with the file

`wasmkeep dev` with a project file (and no crate directory) runs the
app as a deployed version runs on a node (ADR 0023): its front, its
classes, each class's rule from the file (`internal` by default), each
one's configuration and required names, under the file's tenant and app.
Values come from the configuration files and from `[dev] env`
(`.env.local` by default, read only if it exists when not named), whose
values reach the front and every class, as `--env` and `--front-env`
would: secrets on a laptop are not scoped (in a deploy, `secrets set
--class` and `--front` scope them).

It runs every `build` at start, then polls each item's `watch` paths
(files or directories, recursively), its `component` and its `config`:

- a change under `watch` runs the item's `build` again; what `watch`
  names is compared with what it was when the build started, so an edit
  saved during a build runs another (a build must not write under its
  own `watch` paths);
- a change to a component or configuration file (after a build, or from a
  build run elsewhere) reloads the whole app, front and classes swapped
  together as a deploy swaps them;
- a failed build, or a component that does not link, keeps the previous
  version serving, and says why; after a failed build nothing reloads
  until that item builds, so parts of two versions never load together.

A change to `wasmkeep.toml` itself needs a restart of `dev`, which says
so. `wasmkeep dev CRATE...` without a file is unchanged.

### Compatibility

- **Flag deploys** are unchanged, default `open` included, unless a
  `wasmkeep.toml` is in the current directory: then they are refused
  until `--no-project` is given. This is the one behaviour change for
  existing scripts, and it fails loudly.
- **Nodes** need no change beyond 0.4.0: format 3 and explicit `open` are
  ADR 0023's. The CLI writes format 3 without a front for the first time;
  0.4.0 nodes read it, and older ones hold it back as they hold a front
  back.
- **`dev CRATE...`** is unchanged.

## Consequences

- An app can be deployed from a checkout with `wasmkeep deploy --bucket
  ...` alone, and what it deploys is what the file in that commit says.
- The first deploy from the file of an app deployed with flags may make
  its classes `internal`, remove its open classes' reach from outside,
  and remove configuration its scripts set long ago. The deploy says so
  before switching; reading that output is the migration step.
- A class removed from the file stops being served at the next deploy:
  its actors' events fail with `invalid-address` until a deploy brings it
  back. Their state stays.
- `dev` and production read one description, so the rules a developer
  tests are the deployed ones.
- A repository with a `wasmkeep.toml` and deploy scripts using flags must
  add `--no-project` to them, or move to the file.

## Alternatives

- **The file as defaults, flags overriding.** Rejected: the deployed app
  would no longer be what the file says, and nothing records the
  override.
- **Keep what the file does not name, as flag deploys do.** Rejected:
  removing a class or a configuration file from the repository would not
  remove it from the app, the drift the file is meant to end.
- **`[[class]]` with a `name` key.** Two entries of the same name would
  pass TOML and need a check of their own; a table per class gets that
  from the format.
- **The bucket in the file (`[deploy] bucket`).** One file then deploys
  to one place, or carries every environment; buckets are the
  operator's, the app is the developer's.
- **Values or secrets in the file.** The file is committed: a value in it
  is a value in every clone. Configuration files and `wasmkeep secrets`
  already keep values where they belong.
- **Searching parent directories for the file**, as cargo does. A deploy
  run from a subdirectory of another app's repository would deploy that
  app. The current directory or `--project` is explicit.
- **Refusing paths outside the file's directory.** It protects nothing:
  the file already runs commands as the user, and paths are read only on
  the deploying machine. It would break apps that share a class between
  directories.
- **Builds as argument arrays** (`build = ["cargo", "build"]`). No shell
  means no `cd`, `&&` or environment variable, which every build in the
  examples needs; a string run by the shell is what `npm` scripts and
  `Makefile`s do.
- **A confirmation prompt before removing a class.** Deploys run
  unattended; the printed plan and the review of the file are the check.

## Not decided here

- Several environments in one file (`[env.staging]`), and a bucket from
  the environment (`WASMKEEP_BUCKET`).
- `wasmkeep deploy --dry-run` (the plan without the switch).
- Reloading `wasmkeep.toml` itself in `dev`.
- Templates (`wasmkeep new`) that write a `wasmkeep.toml`.
- Deleting a removed class's actors (`app delete`, a class's data).
