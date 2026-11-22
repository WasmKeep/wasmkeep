# A React app

You will build a todo app with a React page and one actor, from an empty
directory to a deployed app. The actor is TypeScript, `todos`: one shared
list per name, in SQLite. The page is React, built with Vite. Both use one
**contract**, a TypeScript file that says what each call takes and
answers, so the page cannot call the actor wrongly without the compiler
or a run-time check saying so. You will run it with `wasmkeep dev` and
Vite's dev server side by side, then deploy the built page as static
assets, and the actor, to a local bucket and a node.

Applies to: WasmKeep `0.6.0` and later (static assets are new in `0.6.0`;
`wasmkeep-ts` builds an actor that imports `@wasmkeep/sdk/contracts` from
`0.6.0` on). Commands assume Linux and one terminal: what you start runs in
its background, and the variables you set stay set. The finished app is
[`examples/react-todos`](https://github.com/wasmkeep/wasmkeep/tree/main/examples/react-todos),
and `scripts/tutorials/react.sh` runs this page's commands as written.
Until `0.7.0` is on npm, install the SDK from a clone in step 2 instead
(`npm install --save-exact <clone>/sdks/typescript`, after `npm ci` in the
clone's `sdks/typescript`), as in
[starting a project](../tutorials/first-actor.md#starting-a-project-with-wasmkeep-new).

## 1. Before you start

You need `wasmkeep` (see [Install](../../README.md#install)), Node.js 22.12
or later with npm, `curl`, and [uv](https://docs.astral.sh/uv/) or `pip` for
the local bucket. The actor is TypeScript, so there is no Rust to install.

<!-- test: tools -->
```bash
wasmkeep --version
node --version
```

You should see `wasmkeep` and its version, then Node's.

## 2. Create the project

The app is one directory with one `package.json`: the actor, the contract
and the page share it. The scripts say how each part is built:

<!-- test: mkdir -->
```bash
mkdir react-todos && cd react-todos
mkdir -p src web/src
```

<!-- test: package-json -->
```bash
cat > package.json <<'EOF'
{
  "name": "react-todos",
  "private": true,
  "type": "module",
  "scripts": {
    "dev": "vite web",
    "typecheck": "tsc -p tsconfig.json && tsc -p web/tsconfig.json",
    "build:actor": "wasmkeep-ts build src/todos.ts -o todos.wasm",
    "build:web": "tsc -p web/tsconfig.json && vite build web",
    "build": "npm run build:actor && npm run build:web"
  }
}
EOF
```

Install the dependencies, pinned to exact versions. The SDK builds the
actor (`wasmkeep-ts`) and has the contract and client types. React and
Vite make the page. `@types/react` and `@types/react-dom` are the
types TypeScript needs for React; there is no UI library:

<!-- test: install -->
```bash
npm install --save-exact @wasmkeep/sdk@0.7.0 react@19.3.1 react-dom@19.3.1
npm install --save-dev --save-exact typescript@5.9.3 vite@8.3.8 \
  @vitejs/plugin-react@6.3.0 @types/react@19.3.0 @types/react-dom@19.3.0
```

`npm` writes `package-lock.json`: commit it with the project, so a build
next year installs what you tested today.

Keep the build output and local state out of git:

<!-- test: gitignore -->
```bash
cat > .gitignore <<'EOF'
node_modules/
todos.wasm
web/dist/
.wasmkeep/
.env.local
secrets.key
EOF
```

Two TypeScript configurations: one for the actor and the contract (no
browser types), one for the page (browser types, JSX):

<!-- test: tsconfig -->
```bash
cat > tsconfig.json <<'EOF'
{
  "compilerOptions": {
    "target": "ES2022",
    "module": "ES2022",
    "moduleResolution": "Bundler",
    "lib": ["ES2022", "ESNext.Disposable"],
    "strict": true,
    "noEmit": true,
    "skipLibCheck": true,
    "types": []
  },
  "include": ["src", "test"]
}
EOF
```

<!-- test: web-tsconfig -->
```bash
cat > web/tsconfig.json <<'EOF'
{
  "compilerOptions": {
    "target": "ES2022",
    "module": "ES2022",
    "moduleResolution": "Bundler",
    "lib": ["ES2022", "ESNext.Disposable", "DOM", "DOM.Iterable"],
    "jsx": "react-jsx",
    "strict": true,
    "noEmit": true,
    "skipLibCheck": true,
    "types": []
  },
  "include": ["src", "test", "../src/contract.ts", "../test/node.d.ts"]
}
EOF
```

## 3. Write the contract

A contract is a file of values, one per operation: its name, method,
path, input, answer and the errors it may return. It imports only
`@wasmkeep/sdk/contracts`, which loads no host module, so the actor and
the page can both import it ([Typed actor clients](../typed-clients.md)):

<!-- test: contract -->
```bash
cat > src/contract.ts <<'EOF'
// The contract of the `todos` class: what a call looks like, as values the
// actor and the browser both import. It imports only
// `@wasmkeep/sdk/contracts`, which loads no host module, so it works in the
// actor (src/todos.ts), in the page (web/src/api.ts) and in a test.
import { codec, defineContract, defineOperation, urlPath } from "@wasmkeep/sdk/contracts";

/** The longest item, in characters. */
export const MAX_TEXT = 200;

const todo = codec.object({ id: codec.integer(), text: codec.string(), done: codec.boolean() });
const notFound = { "todos.not_found": codec.none() };

export type Todo = ReturnType<typeof todo.parse>;

export const list = defineOperation({
  name: "todos.list",
  path: "/",
  output: codec.object({ todos: codec.array(todo) }),
});

export const add = defineOperation({
  name: "todos.add",
  method: "POST",
  path: "/",
  input: codec.object({ text: codec.string() }),
  output: todo,
  errors: { "todos.invalid_text": codec.object({ max: codec.integer() }) },
});

export const toggle = defineOperation({
  name: "todos.toggle",
  method: "POST",
  path: (input) => urlPath`/${input.id}/toggle`,
  input: codec.object({ id: codec.integer() }),
  output: todo,
  errors: notFound,
});

export const remove = defineOperation({
  name: "todos.remove",
  method: "DELETE",
  path: (input) => urlPath`/${input.id}`,
  input: codec.object({ id: codec.integer() }),
  errors: notFound,
});

export const todos = defineContract({ list, add, toggle, remove });
EOF
```

The types come from the codecs (`codec.object`, `codec.integer`, ...), so
they cannot drift from what is checked: an input is checked before it is
sent, and an answer before it is returned. The `errors` of an operation
are the failures the actor chooses to describe; they travel as the
[error envelope](../typed-clients.md#the-error-envelope).

## 4. Write the actor

Class `todos`: every actor of it (`todos/main`, `todos/groceries`, ...) is
a list, in its own SQLite database. The handler follows the contract's
paths, and answers a failure with `problem()`, which writes the envelope:

<!-- test: actor -->
```bash
cat > src/todos.ts <<'EOF'
// Class `todos`: each actor (`todos/main`, `todos/groceries`, ...) is one
// shared list, in its own SQLite database. The routes and the answers are
// the contract in ./contract.ts; failures use the error envelope, so the
// page can tell "no such item" from "the server is down".
import { defineActor, json, storage } from "@wasmkeep/sdk";
import { problem } from "@wasmkeep/sdk/contracts";
import { MAX_TEXT } from "./contract";

type Row = ReturnType<typeof storage.query>[number];

const item = (row: Row) => ({ id: Number(row.id), text: String(row.text), done: Number(row.done) !== 0 });
const notFound = () => problem(404, "todos.not_found", "No such item");

export default defineActor({
  init() {
    storage.exec(
      "CREATE TABLE IF NOT EXISTS todos (id INTEGER PRIMARY KEY AUTOINCREMENT, text TEXT NOT NULL, done INTEGER NOT NULL DEFAULT 0)",
    );
  },

  fetch(req) {
    const path = req.path.split("?")[0];
    const method = req.method;
    if (path === "/" && method === "GET") {
      const rows = storage.query("SELECT id, text, done FROM todos ORDER BY id");
      return json({ todos: rows.map(item) });
    }
    if (path === "/" && method === "POST") {
      let text: unknown;
      try {
        text = (req.json<{ text?: unknown }>() ?? {}).text;
      } catch {
        return problem(400, "todos.invalid_text", "The body must be JSON", { max: MAX_TEXT });
      }
      const trimmed = typeof text === "string" ? text.trim() : "";
      if (trimmed === "" || [...trimmed].length > MAX_TEXT) {
        return problem(400, "todos.invalid_text", `Write 1 to ${MAX_TEXT} characters`, { max: MAX_TEXT });
      }
      const [row] = storage.query("INSERT INTO todos (text) VALUES (?1) RETURNING id, text, done", trimmed);
      return json(item(row), 201);
    }
    const toggle = /^\/([0-9]{1,15})\/toggle$/.exec(path);
    if (toggle && method === "POST") {
      const [row] = storage.query("UPDATE todos SET done = 1 - done WHERE id = ?1 RETURNING id, text, done", Number(toggle[1]));
      return row ? json(item(row)) : notFound();
    }
    const one = /^\/([0-9]{1,15})$/.exec(path);
    if (one && method === "DELETE") {
      const rows = storage.query("DELETE FROM todos WHERE id = ?1 RETURNING id", Number(one[1]));
      return rows.length > 0 ? { status: 204 } : notFound();
    }
    return { status: 404, body: "not found\n" };
  },
});
EOF
```

Build it:

<!-- test: build-actor -->
```bash
npm run build:actor
```

`wasmkeep-ts` bundles `src/todos.ts` into `todos.wasm`, a WebAssembly
component, as the Rust build of the [first actor](../tutorials/first-actor.md)
does.

## 5. Write the page

`web/` is the page, a Vite project. Start with its HTML and its Vite
configuration. The configuration sends every `/actor/...` request of
the dev server to `wasmkeep dev`, so the page and the API have one
origin, as they will when a node serves both:

<!-- test: html -->
```bash
cat > web/index.html <<'EOF'
<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Todos</title>
  </head>
  <body>
    <div id="root"></div>
    <script type="module" src="/src/main.tsx"></script>
  </body>
</html>
EOF
```

<!-- test: vite-config -->
```bash
cat > web/vite.config.ts <<'EOF'
// `npm run dev` serves the page with hot reload and sends every request to
// an actor (`/actor/...`) to `wasmkeep dev`, so the page and the API share
// one origin, as they do when the node serves the built files.
import react from "@vitejs/plugin-react";
import { realpathSync } from "node:fs";
import { defineConfig } from "vite";

// The SDK where npm put it; a link to a clone of WasmKeep resolves outside
// the project, and the dev server serves only the places it is allowed.
const sdk = realpathSync(new URL("../node_modules/@wasmkeep/sdk", import.meta.url));

export default defineConfig({
  plugins: [react()],
  server: {
    // `..` is the project: the contract is in ../src.
    fs: { allow: ["..", sdk] },
    proxy: { "/actor": "http://127.0.0.1:8080" },
  },
});
EOF
```

The configuration reads where npm put the SDK: when it is a link to a
clone of WasmKeep, as in this repository's test of this page, it lives
outside the project, and the dev server has to be allowed to serve it.

The browser's typed client. The SDK's `Client` is synchronous, as an
actor's calls are, and `fetch` is not: so this file sends the operation's
request with `fetch`, then gives the reply to a `Client` whose transport
returns it. The reply is checked and sorted into a value or a failure by
the code an actor uses. Nothing the server or the network does is thrown:

<!-- test: api -->
```bash
cat > web/src/api.ts <<'EOF'
// The typed client for a browser. `Client` in the SDK is synchronous, as an
// actor's calls are; a page has `fetch`, which is not. So the page sends the
// operation's request itself, then hands the answer to a `Client` whose
// transport returns it: the answer is checked and sorted into a value or a
// failure by the same code an actor uses.
import { CallError, Client, type ErrorMap, type InputArgs, type Operation, type Result, type WireRequest } from "@wasmkeep/sdk/contracts";
import { add, list, remove, toggle } from "../../src/contract";

/** The reply of the server, or why there is none. */
type Reply = { status: number; headers: Record<string, string>; body: Uint8Array } | CallError;

async function send(address: string, request: WireRequest): Promise<Reply> {
  try {
    const response = await fetch(`/actor/${address}${request.path}`, {
      method: request.method,
      headers: request.headers,
      body: request.body ? new TextDecoder().decode(request.body) : null,
    });
    const headers: Record<string, string> = {};
    response.headers.forEach((value, name) => (headers[name] = value));
    return { status: response.status, headers, body: new Uint8Array(await response.arrayBuffer()) };
  } catch (e) {
    return new CallError("failed", e instanceof Error ? e.message : "the request failed");
  }
}

/** Calls `operation` on the actor at `address` (`class/name`). Nothing the
 * server or the network does is thrown: the result says what happened. */
export async function call<I, O, E extends ErrorMap>(
  address: string,
  operation: Operation<I, O, E>,
  ...input: InputArgs<I>
): Promise<Result<O, E>> {
  let reply: Reply;
  try {
    reply = await send(address, operation.request(input[0] as I));
  } catch {
    // The input is not valid: the client reports it, and nothing is sent.
    reply = new CallError("failed", "not sent");
  }
  return new Client({
    call() {
      if (reply instanceof CallError) throw reply;
      return reply;
    },
    callMany() {
      throw new Error("batches are not used here");
    },
  }).call(address, operation, ...input);
}

/** The typed methods of one list. */
export function todoList(name: string) {
  const address = `todos/${name}`;
  return {
    list: () => call(address, list),
    add: (text: string) => call(address, add, { text }),
    toggle: (id: number) => call(address, toggle, { id }),
    remove: (id: number) => call(address, remove, { id }),
  };
}

/** A failure as one line for the person using the page. */
export function describe(failure: { kind: string; message?: string; status?: number }): string {
  if (failure.kind === "declared" && failure.message) return failure.message;
  if (failure.kind === "transport") return "The server cannot be reached. Try again.";
  if (failure.kind === "invalid-request") return "That cannot be sent.";
  return `The server answered something unexpected${failure.status ? ` (${failure.status})` : ""}.`;
}
EOF
```

The page: the list is named by the path, `/l/groceries`, and `/` is the
list `main`. Every button is a typed call; a failure is shown as the
message the actor gave:

<!-- test: app -->
```bash
cat > web/src/App.tsx <<'EOF'
import { useCallback, useEffect, useState, type FormEvent } from "react";
import { MAX_TEXT, type Todo } from "../../src/contract";
import { describe, todoList } from "./api";

/** The list is the last part of `/l/<name>`; the page is served at any such
 * path (`not-found = "spa"`), and `/` is the list `main`. */
function listName(path: string): string {
  const match = /^\/l\/([a-z0-9-]{1,32})\/?$/.exec(path);
  return match ? match[1] : "main";
}

export function App() {
  const name = listName(window.location.pathname);
  const [todos, setTodos] = useState<Todo[] | undefined>();
  const [text, setText] = useState("");
  const [error, setError] = useState<string | undefined>();
  const api = todoList(name);

  const load = useCallback(async () => {
    const r = await todoList(name).list();
    if (r.ok) {
      setTodos(r.value.todos);
      setError(undefined);
    } else setError(describe(r.failure));
  }, [name]);

  useEffect(() => {
    void load();
  }, [load]);

  async function run(done: () => Promise<{ ok: boolean; failure?: Parameters<typeof describe>[0] }>) {
    const r = await done();
    if (r.ok) setError(undefined);
    else if (r.failure) setError(describe(r.failure));
    await load();
    return r.ok;
  }

  async function onSubmit(event: FormEvent) {
    event.preventDefault();
    if (await run(() => api.add(text))) setText("");
  }

  const left = todos?.filter((t) => !t.done).length ?? 0;
  return (
    <main>
      <header>
        <h1>
          <svg viewBox="0 0 24 24" width="28" height="28" aria-hidden="true" focusable="false">
            <path d="M4 12.5l5 5L20 6.5" fill="none" stroke="currentColor" strokeWidth="3" strokeLinecap="round" strokeLinejoin="round" />
          </svg>
          Todos
        </h1>
        <p className="list-name">List: {name}</p>
      </header>
      <form onSubmit={onSubmit}>
        <label htmlFor="new-todo">New item</label>
        <input id="new-todo" value={text} maxLength={MAX_TEXT} onChange={(e) => setText(e.target.value)} autoFocus />
        <button type="submit">Add</button>
      </form>
      {error && <p role="alert" className="error">{error}</p>}
      {todos === undefined ? (
        !error && <p>Loading</p>
      ) : todos.length === 0 ? (
        <p>Nothing to do.</p>
      ) : (
        <ul>
          {todos.map((t) => (
            <li key={t.id} className={t.done ? "done" : undefined}>
              <label>
                <input type="checkbox" checked={t.done} onChange={() => void run(() => api.toggle(t.id))} />
                <span>{t.text}</span>
              </label>
              <button type="button" onClick={() => void run(() => api.remove(t.id))} aria-label={`Remove ${t.text}`}>
                Remove
              </button>
            </li>
          ))}
        </ul>
      )}
      {todos && todos.length > 0 && <p className="count">{left} left</p>}
    </main>
  );
}
EOF
```

<!-- test: main -->
```bash
cat > web/src/main.tsx <<'EOF'
import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { App } from "./App";
import "./styles.css";

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <App />
  </StrictMode>,
);
EOF
```

<!-- test: styles -->
```bash
cat > web/src/styles.css <<'EOF'
:root {
  color-scheme: light dark;
  --bg: #fafaf7;
  --fg: #1d1d1b;
  --muted: #5f5f5a;
  --line: #d9d9d2;
  --accent: #0b6b57;
  --danger: #a3261f;
  font-family: system-ui, sans-serif;
}
@media (prefers-color-scheme: dark) {
  :root {
    --bg: #161614;
    --fg: #ecece6;
    --muted: #a2a29a;
    --line: #3a3a36;
    --accent: #4fc3a8;
    --danger: #ff8a80;
  }
}
body {
  margin: 0;
  background: var(--bg);
  color: var(--fg);
}
main {
  max-width: 34rem;
  margin: 0 auto;
  padding: 2rem 1rem;
}
h1 {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  margin: 0;
  color: var(--accent);
}
.list-name,
.count {
  color: var(--muted);
}
form {
  display: grid;
  grid-template-columns: 1fr auto;
  gap: 0.5rem;
  margin: 1rem 0;
}
form label {
  grid-column: 1 / -1;
  font-size: 0.875rem;
  color: var(--muted);
}
input[type="text"],
input:not([type]) {
  padding: 0.5rem;
  border: 1px solid var(--line);
  border-radius: 0.25rem;
  background: transparent;
  color: inherit;
  font: inherit;
}
button {
  padding: 0.5rem 0.9rem;
  border: 1px solid var(--line);
  border-radius: 0.25rem;
  background: transparent;
  color: inherit;
  font: inherit;
  cursor: pointer;
}
button:focus-visible,
input:focus-visible {
  outline: 2px solid var(--accent);
  outline-offset: 2px;
}
ul {
  margin: 0;
  padding: 0;
  list-style: none;
}
li {
  display: flex;
  justify-content: space-between;
  align-items: center;
  gap: 1rem;
  padding: 0.5rem 0;
  border-bottom: 1px solid var(--line);
}
li label {
  display: flex;
  align-items: center;
  gap: 0.6rem;
}
li.done span {
  color: var(--muted);
  text-decoration: line-through;
}
.error {
  color: var(--danger);
}
EOF
```

<!-- test: sdk-types -->
```bash
cat > web/src/sdk-types.d.ts <<'EOF'
// The SDK's contracts entry refers to its host bindings in types only (they
// are not bundled into the page). This brings their declarations into the
// type check.
import type {} from "@wasmkeep/sdk";
EOF
```

The last file only brings the SDK's type declarations into the page's type
check; nothing of it reaches the browser.

<!-- test: typecheck -->
```bash
npm run typecheck
```

## 6. Write the project file

`wasmkeep.toml` describes the app: the class and the assets, and how each
is built ([The project file](../project-file.md), [Static assets](../static-assets.md)):

<!-- test: project-file -->
```bash
cat > wasmkeep.toml <<'EOF'
[app]
tenant = "acme"
name = "react-todos"

[assets]
dir = "web/dist"
build = "npm run build:web"
watch = ["web/src", "web/index.html", "src/contract.ts"]
not-found = "spa"
immutable = ["assets/"]

[classes.todos]
component = "todos.wasm"
access = "open"
build = "npm run build:actor"
watch = ["src"]

[dev]
port = 8080
EOF
```

- **`[assets]`**: the built page. `dir` is what Vite writes, `build` makes it,
  and `watch` lists the sources whose change builds it again under `dev`.
- **`not-found = "spa"`**: a request for a path that is no file, such as
  `/l/groceries`, from a browser navigating, is answered with `index.html`, and
  React reads the path. A missing script or style is still a 404.
- **`immutable = ["assets/"]`**: Vite names its files by content
  (`assets/index-B9NGg959.js`), so a browser may keep them for a year.
  `index.html` is never kept without asking.
- **`[classes.todos]`** is `access = "open"`: a browser calls the actor
  directly, at `/actor/todos/<name>/`, as it would any URL. A class of a
  project file is `internal` without it. There is no front: the node serves
  the assets, and the actors answer the API.

An open class has no sign-in. Anyone who knows a list's name can read and
change it, and a new name is a new actor. That is fine to try, and not
for private data. [Your first app](../tutorials/first-app.md) puts a front
in between, which signs users in and chooses the actor, and
[ingress limits](../project-file.md#ingress-limits) bound how fast a client
may call.

## 7. Run it

Two processes, each with its job. `wasmkeep dev` builds the actor and the
page, and serves the app on port 8080. Vite's dev server serves the page
on port 5173 with hot reload, and proxies the API to port 8080:

<!-- test: dev -->
```bash
wasmkeep dev > dev.log 2>&1 &
echo $! > dev.pid
until curl -sf -o /dev/null localhost:8080/; do sleep 1; done
./node_modules/.bin/vite web --host 127.0.0.1 --strictPort > vite.log 2>&1 &
echo $! > vite.pid
until curl -sf -o /dev/null 127.0.0.1:5173/; do sleep 1; done
```

(`npm run dev` starts the same Vite server; the page runs it directly so that
`vite.pid` is its own process, and `--host 127.0.0.1` keeps it to the
address `wasmkeep dev` uses.) `dev.log` says what is served:

```text
... running the project file app=acme/react-todos file=wasmkeep.toml
... serving todos at http://127.0.0.1:8080/actor/todos/<name>/ (access open)
```

Open <http://127.0.0.1:5173/> in a browser, add an item, tick it. Change
`App.tsx` and the page updates without a reload. The same page, as built,
is at <http://localhost:8080/>, served by `wasmkeep dev` the way a node will
serve it. Ask both for the page, and the actor through Vite's proxy:

<!-- test: pages -->
```bash
curl -s 127.0.0.1:5173/ | grep -c '<div id="root">'
curl -s localhost:8080/ | grep -c '<div id="root">'
curl -s -H 'Accept: text/html' localhost:8080/l/groceries | grep -c '<div id="root">'
curl -s -w '\n%{http_code}\n' -H 'Content-Type: application/json' -d '{"text":"buy milk"}' 127.0.0.1:5173/actor/todos/main/
curl -s 127.0.0.1:5173/actor/todos/main/
```

```text
1
1
1
{"id":1,"text":"buy milk","done":false}
201
{"todos":[{"id":1,"text":"buy milk","done":false}]}
```

`/l/groceries` is not a file, and the request says it wants HTML, so the node
answers `index.html` (the `spa` mode). Anything else missing answers 404:

<!-- test: spa -->
```bash
curl -s -o /dev/null -w '%{http_code}\n' localhost:8080/assets/missing.js
```

```text
404
```

### Errors are typed

The actor refuses an empty item. The answer is the error envelope, which
the page's client turns into `declared` failure with the message the
actor wrote:

<!-- test: error -->
```bash
curl -s -w '\n%{http_code}\n' -H 'Content-Type: application/json' -d '{"text":""}' localhost:8080/actor/todos/main/
```

```text
{"v":1,"code":"todos.invalid_text","message":"Write 1 to 200 characters","details":{"max":200}}
400
```

Try it in the page: an empty submit shows that message, in red, and
nothing else changes. Stop both processes (the node in step 9 uses the
same port):

<!-- test: stop-dev -->
```bash
kill $(cat vite.pid) $(cat dev.pid)
wait
```

## 8. Start a bucket

As in [From one node to three](../tutorials/one-node-to-three.md#3-start-a-bucket),
[moto](https://github.com/getmoto/moto) runs an S3 API in memory. With
uv:

<!-- test: moto-uvx -->
```bash
uvx --from 'moto[server]==5.2.5' moto_server -p 9000 > moto.log 2>&1 &
echo $! > moto.pid
```

Or with pip, install it once (`pip install 'moto[server]==5.2.5'`), then:

<!-- test: moto-pip -->
```bash
moto_server -p 9000 > moto.log 2>&1 &
echo $! > moto.pid
```

Wait for it, create a bucket named `demo`, and point WasmKeep at it:

<!-- test: bucket -->
```bash
until curl -sf -o /dev/null localhost:9000/moto-api/; do sleep 1; done
curl -X PUT -H 'Content-Length: 0' localhost:9000/demo
export AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_REGION=us-east-1 \
  AWS_ENDPOINT=http://127.0.0.1:9000 AWS_ALLOW_HTTP=true
```

## 9. Deploy it

`deploy` builds the actor and the page, lists what it will deploy and
asks before it switches. `--yes` answers for you, as a script must:

<!-- test: deploy -->
```bash
wasmkeep deploy --bucket s3://demo --yes
```

```text
acme/react-todos from wasmkeep.toml, before switching:
  class todos: added (open)
  asset assets/index-B9NGg959.js: 233627 bytes, text/javascript;charset=utf-8
  asset assets/index-CRwcsRHU.css: 1413 bytes, text/css;charset=utf-8
  asset index.html: 388 bytes, text/html;charset=utf-8
  assets: added
acme/react-todos: version 2ac339b0a5fdb0d1 is current (deploy 1)
```

(The file names and the version differ with the code.) The version holds the
class and the assets together, so they deploy and roll back together.
Without a project file, the same assets deploy with
`wasmkeep deploy --no-project --app acme/react-todos --bucket s3://demo
--assets web/dist --assets-not-found spa --assets-immutable assets/`
([static assets](../static-assets.md)).

Start a node. Given `--app`, it serves what was last deployed for that app:

<!-- test: serve -->
```bash
wasmkeep serve --app acme/react-todos --bucket s3://demo --data-dir node > node.log 2>&1 &
echo $! > node.pid
until curl -sf -o /dev/null localhost:8080/_wasmkeep/ready; do sleep 1; done
```

The node serves the page and the API, with no Node.js and no Vite: the files
come from the bucket, and a node holds them on disk. Look at what a browser
gets for the page, and for a hashed file:

<!-- test: node-page -->
```bash
curl -s -H 'Accept: text/html' localhost:8080/l/groceries | grep -c '<div id="root">'
curl -sI localhost:8080/ | grep -i '^cache-control'
asset=$(curl -s localhost:8080/ | grep -o '/assets/[^"]*\.js')
curl -sI "localhost:8080$asset" | grep -i '^cache-control'
curl -s -w '\n%{http_code}\n' -H 'Content-Type: application/json' -d '{"text":"deployed"}' localhost:8080/actor/todos/main/
```

```text
1
cache-control: public, max-age=0, must-revalidate
cache-control: public, max-age=31536000, immutable
{"id":1,"text":"deployed","done":false}
201
```

The page is revalidated each time, so a deploy shows at the next load; the
script it names is kept for a year, and a deploy that changes the page
names a new one. The id is `1` again: the node's actors are not those of `dev`.
Open <http://localhost:8080/> to use it.

## 10. Clean up

Stop the node and the bucket, wait for them to exit, and remove the project:

<!-- test: cleanup -->
```bash
kill $(cat node.pid) $(cat moto.pid)
wait
cd ..
rm -rf react-todos
```

moto kept the bucket in memory, so it is gone too.

## What just happened

- **One contract, both sides.** `src/contract.ts` is imported by the actor
  and by the page. The actor's answers and the page's reads are checked
  against the same codecs, and a failure the actor describes arrives as
  a typed value, not as a status code to guess at.
- **The browser calls the actor.** An `open` class is a URL,
  `/actor/todos/<name>/`. There is no server code between the page and the
  actor, and no sign-in either: add a front when you need one.
- **Two servers in development, none in production.** Vite serves the page
  and proxies `/actor/` to `wasmkeep dev`; deployed, the node serves the
  built files and the actors from one origin, so the page needs no CORS and no
  configuration for where the API is.
- **A single page, served as one.** `not-found = "spa"` answers a
  navigation to `/l/groceries` with `index.html`, and `immutable`
  lets browsers keep Vite's hashed files.
- **A version is the whole app.** The assets and the class deploy and roll
  back together (`wasmkeep deploy --rollback`); the actors' state stays.

## Next

- [Static assets](../static-assets.md): routes, caching, compression,
  ranges, and what `front-first` is for when a front shares the app.
- [Typed actor clients and structured errors](../typed-clients.md):
  batches, every kind of failure, and how a contract changes over time.
- [Your first app](../tutorials/first-app.md): a front that signs users in
  and keeps the actors internal.
- [Going to production](../tutorials/going-to-production.md): nodes, a real
  bucket and the checks before a deploy.
