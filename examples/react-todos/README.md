# React todos

A todo app in two parts over one contract: a TypeScript actor class
(`todos`, one shared list per name, in SQLite) and a React page built with
Vite, which calls the actor from the browser with a typed client. In
development Vite proxies `/actor/` to `wasmkeep dev`; deployed, the built
page is served as [static assets](../../docs/static-assets.md)
(`not-found = "spa"`) by the same node that runs the actor.

The step-by-step tutorial, from an empty directory to a deployed app, is
[docs/build/react.md](../../docs/build/react.md); `scripts/tutorials/react.sh`
runs its commands.

| Path | What it is |
| --- | --- |
| `src/contract.ts` | the contract: each operation's path, input, answer and errors, imported by both sides |
| `src/todos.ts` | class `todos`, the actor |
| `web/` | the page: `src/api.ts` (the typed client over `fetch`), `src/App.tsx`, `vite.config.ts` |
| `test/`, `web/test/` | tests of the contract and of the client, against fake servers |
| `wasmkeep.toml` | the project file: the class and the assets |

From this directory, after `npm ci` in `sdks/typescript`:

```bash
npm ci
wasmkeep dev          # builds the actor and the page, serves both on :8080
npm run dev           # in another terminal: Vite on :5173, proxying /actor/ to :8080
npm test              # type check, then the tests
```

Open <http://localhost:5173/> (hot reload) or <http://localhost:8080/> (the
built page, as a node serves it). `/l/<name>` is the list `<name>`.
`wasmkeep deploy --bucket s3://my-bucket` deploys the actor and the built
page together.

The class is `open`: anyone who knows a list's name can read and change it.
For sign-in, put a front between the page and the actors, as in
[todo-front](../todo-front).
