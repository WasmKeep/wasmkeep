# {{app}}

A WasmKeep app in {{language}}, written by `wasmkeep new` (WasmKeep
{{version}}).

| Path | What it is |
| --- | --- |
{{layout}}| `wasmkeep.toml` | the project file: the app, its parts, how each is built |
| `.env.local.example` | local values for `wasmkeep dev`, to copy to `.env.local` |

## Run it

You need {{needs}}. Then:

```bash
{{install}}
wasmkeep dev
```

`wasmkeep dev` builds every part, serves the app on
http://127.0.0.1:8080, and builds and reloads a part when you save one of
its files. The actors' databases stay under `.wasmkeep/dev`. In another
terminal:

```bash
{{try}}
```

## Deploy it

Choose `tenant` and `name` under `[app]` in `wasmkeep.toml` first: nodes
find the app by them. Then, with nodes serving the bucket
(https://docs.wasmkeep.dev/cluster.html):

```bash
wasmkeep deploy --bucket s3://my-bucket/prod
```

It runs every build, prints what changes, then switches every node to the
new version. `wasmkeep deploy --rollback` brings the previous one back.

## Versions

The SDK is pinned to {{version}}, the version of the `wasmkeep` that wrote
this project. Upgrade the two together.

The project file: https://docs.wasmkeep.dev/project-file.html
