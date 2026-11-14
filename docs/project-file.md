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
| `build` | optional | a command for the system shell (`sh -c`, `cmd.exe` on Windows), run in the file's directory before `component` is read |
| `watch` | optional | files or directories; `wasmkeep dev` runs `build` again when one changes |

### `[dev]` (optional, `wasmkeep dev` only)

| Key | Default | |
| --- | --- | --- |
| `port` | `8080` | on `127.0.0.1`; `--listen` overrides it |
| `data-dir` | `.wasmkeep/dev` beside the file | actor databases; `--data-dir` overrides it |
| `env` | `.env.local` (read if it exists) | local values, `NAME=value` lines, for the front and every class |

There is no key for a value, a secret or a bucket. Configuration goes in
the files `config` names; secrets go in the bucket with `wasmkeep secrets
set` ([Secrets in the bucket](cluster.md#secrets-in-the-bucket)); local
values go in `.env.local`, which belongs in `.gitignore`. The bucket is
`deploy --bucket`, so one file deploys to staging and production alike.

## `wasmkeep deploy`

```bash
wasmkeep deploy --bucket s3://my-bucket/prod
```

1. Runs every `build` (the front's first, then the classes' in name
   order). A failed build stops the deploy before the bucket is opened.
   `--no-build` skips them.
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
`config` or `requires` has none, even if a deploy with flags set some.
Deploying the same file again changes nothing. A removed class's actors
keep their state in the bucket; a later deploy that brings the class back
finds it.

**Every rule is written** in the version, `open` included, so a class's
rule never depends on how the app was deployed. Such a version uses
manifest format 3, read by every 0.4.0 node.

**With flags.** Beside a file, `--actor`, `--access`, `--config`,
`--requires`, `--front`, `--no-front`, `--front-config` and
`--front-requires` are refused. `--app` may be given if it names the
file's app. To deploy with flags only, pass `--no-project` (or run from
another directory): such a deploy keeps every class's rule, and a class
it adds without `--access` is `open`, the default of flags. The next
deploy from the file makes the version the file's again.

**Rollback.** `wasmkeep deploy --rollback` reads only `[app]` and runs no
build: it restores the previous version as the bucket holds it. The file
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

- a change under `watch` runs that item's `build`;
- a changed component or configuration file (after a build, or one run
  in another terminal) reloads the whole app, front and classes together;
- a failed build, or a component that does not link, keeps the previous
  version serving.

Changes to `wasmkeep.toml` or `.env.local` need a restart. Beside a file,
crate directories, `--access` and `--env` are refused; `wasmkeep dev
--no-project CRATE...` runs crates as before.
