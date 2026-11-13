# WasmKeep actors in Python

Write [WasmKeep](https://github.com/WasmKeep/wasmkeep) actors as plain Python
classes. WasmKeep is a self-hosted platform for actors: WebAssembly
components, each with its own SQLite database, handling one event at a time.
This package is the SDK and the command that builds your module into such a
component, with
[componentize-py](https://github.com/bytecodealliance/componentize-py)
(CPython embedded). WasmKeep is pre-alpha.

## Install

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install wasmkeep
```

That is all a build needs: the package carries the WIT files of the
`wasmkeep:actor` world, and componentize-py comes with it at the version
the SDK is written for. To run the result you need the `wasmkeep` server:
[install a release](https://github.com/WasmKeep/wasmkeep#install) of the
same version as the package.

## A first actor

`app.py`:

```python
from wasmkeep import Actor, storage


class Hits(Actor):
    # Once per deployed version, before any other event: create tables here.
    def init(self):
        storage.exec("CREATE TABLE IF NOT EXISTS hits (at INTEGER NOT NULL)")

    # One HTTP request at a time, in arrival order: no locks needed.
    def fetch(self, req):
        storage.exec("INSERT INTO hits (at) VALUES (unixepoch())")
        [row] = storage.query("SELECT count(*) AS n FROM hits")
        return {"hits": row["n"], "path": req.path}   # a dict is sent as JSON


WitWorld, IncomingHandler = Hits.exports()
```

Build it into a component and serve it:

```bash
python -m wasmkeep build app -o actor.wasm
wasmkeep serve --actor hits=actor.wasm
```

In another terminal:

```bash
curl localhost:8080/actor/hits/alice   # {"hits": 1, "path": "/"}
curl localhost:8080/actor/hits/alice   # {"hits": 2, "path": "/"}
curl localhost:8080/actor/hits/bob     # {"hits": 1, "path": "/"}: another actor, its own database
```

`build` takes the module's name (`app` for `app.py`), found in the current
directory; `-p DIR` (repeatable) names other directories to find it and
what it imports in. Pure-Python packages installed in the same environment
are bundled too. The last line of `app.py` is what the component exports:
keep it at module level.

The other hooks, all optional:

```python
from wasmkeep import Actor, sockets


class Room(Actor):
    # The alarm set with alarms.set(...) fired (at least once).
    def alarm(self):
        pass

    def websocket_open(self, conn, path):
        sockets.send(conn, "welcome")       # return False to refuse

    def websocket_message(self, conn, data):
        sockets.broadcast(data)

    def websocket_close(self, conn, code, reason):
        pass
```

## Versions

| | |
| --- | --- |
| The package | The version of the WasmKeep server it is released with; use the two at the same version |
| componentize-py | 0.25.1 exactly, installed with the package: the SDK imports the bindings that version generates |
| Python, to run the build | 3.10 or later (CI builds with 3.13) |
| Python, in the component | CPython 3.14, the one componentize-py 0.25.1 embeds, whatever Python ran the build |
| The interface | `wasmkeep:actor@0.2.0`, versioned on its own: `wasmkeep.WIT_VERSION`, the WIT files in the package's `wit/` |

`python -m wasmkeep --version` prints the package's version and the
interface's.

## Outside a component

The host's functions (`storage`, `alarms`, `actors`, `sockets`, `env`, and
`exports()`) exist only inside a built component, running on a WasmKeep
node. Under plain CPython the package imports, so an editor resolves it and
the build command runs, but calling one of them raises `RuntimeError`: an
actor is tried by building it and serving it (`wasmkeep serve` runs on a
laptop). Keep logic you want to unit-test in functions that take and return
plain values.

## API

| | |
| --- | --- |
| `storage.exec(sql, *params)` | Runs a statement; returns the rows changed |
| `storage.exec_durable(sql, *params)` | The same, returning once the write is durable (inside a transaction, only `COMMIT`) |
| `storage.query(sql, *params)` | Rows as dicts, by column name |
| `storage.delete_self()` | Deletes this actor when the event ends |
| `alarms.set(at_ms)`, `alarms.get()`, `alarms.clear()` | The actor's single alarm, in ms since the Unix epoch |
| `env.get(name, default=None)` | The class's configuration value or secret (`deploy --config`, `secrets set`, `serve --env`; last wins; see [`env`](https://github.com/WasmKeep/wasmkeep/blob/v0.3.0/docs/reference/api.md#interface-env)), or `default` |
| `actors.self_address()` | This actor's `ActorAddress` (`tenant`, `class_name`, `name`) |
| `actors.call("class/name", method=, path=, headers=, body=)` | Calls another actor; returns a `CallResponse` (`status`, `headers`, `body`, `text()`, `json()`) |
| `sockets.send(conn, data)`, `broadcast(data, except_conn=)`, `close(conn, code, reason)`, `connections()` | The actor's WebSockets (str is text, bytes binary) |
| `Response(body, status, headers)`, `json(value, status)` | Full responses; a handler may also return str, bytes, a dict or a list |

SQL values are `None`, `int`, `float`, `str` and `bytes` (`bool` binds as 0
or 1). Failures raise `SqlError` (with `.kind`: syntax, constraint, bind, ...)
and `CallError` (`.kind`: invalid-address, reentrant, too-large, timeout,
failed). An exception in `fetch` becomes a 500; in `init` or `alarm` it fails
the event (an alarm is retried).

One instance of your class handles events until the actor hibernates; keep
anything that must survive in `storage`, not on `self`.

## Key-value and queues

`wasmkeep.kv` and `wasmkeep.queue` are clients for key-value shards and
queues built from actors ([ADR 0015](https://github.com/WasmKeep/wasmkeep/blob/v0.3.0/docs/adr/0015-kv-and-queues-as-sdk-patterns.md)).
The shard and queue classes are the Rust SDK's: deploy the components of
[`examples/jobs`](https://github.com/WasmKeep/wasmkeep/tree/v0.3.0/examples/jobs)
as classes `kv` and `queue` (a release carries them already built, `kv.wasm`
and `queue.wasm`). These clients speak their protocol over `actors.call`, byte for byte like the
Rust and TypeScript clients, so all of them share the same data.

```python
from wasmkeep import Actor, kv, queue

store = kv.Client("kv", 8).with_read_cache(60_000)   # 8 shards; get may serve 60 s old reads
jobs = queue.Client("queue/jobs")


class App(Actor):
    def fetch(self, req):
        if queue.is_delivery(req):                    # a push from a queue
            return queue.handle_delivery(req, lambda _queue, m: "ack")
        version = store.put("user:42", "Ada")
        store.compare_and_set("user:42", version, "Ada L.")   # KvError "conflict" if it moved
        jobs.send("resize 42")
        return store.get("user:42").value


WitWorld, IncomingHandler = App.exports()
```

| | |
| --- | --- |
| `kv.Client(class_name, shards)` | Keys over `shards` actors `class_name/shard-<i>`; every caller uses the same count |
| `get(key)`, `get_fresh(key)` | An `Entry` (`value`, `version`, `expires_at`) or `None`; `get_fresh` skips the read cache |
| `put(key, value)`, `put_with(key, value, expect=, ttl_ms=, durable=)` | The new version; `expect` is `"any"`, `"absent"` or a version |
| `compare_and_set(key, expected, value)` | `put` only at version `expected` (`None`: absent) |
| `delete(key)`, `delete_if(key, version)` | `True` if the key was there |
| `list(prefix, after=None, limit=100)` | Keys (`bytes`) from every shard, in byte order |
| `queue.Client(address)` | One queue actor, `class/name` |
| `send(body)`, `send_delayed(body, ms)`, `send_batch(bodies)` | Ids, once durable; a batch (up to 1,000) is all or none |
| `lease(max, visibility_ms)`, `ack(m)`, `retry(m, delay_ms=None)`, `settle(items)` | Pull consumers; `ack` and `retry` are `False` for a stale lease |
| `stats()`, `dead_letters(limit)` | `Stats` (`ready`, `delayed`, `in_flight`, `dead`) and `DeadLetter`s |
| `queue.is_delivery(req)`, `queue.handle_delivery(req, handle)` | Push consumers: `handle(queue, message)` returns `"ack"`, `"retry"` or `queue.RetryAfter(ms)` |

Failures raise `kv.KvError` (`.kind`: conflict, key-too-long, storage, call,
refused with `.status`) and `queue.QueueError` (storage, call, refused);
numbers outside Rust's `u32`/`u64` raise `ValueError`. The
[guide](https://github.com/WasmKeep/wasmkeep/blob/v0.3.0/docs/kv-and-queues.md#python) covers the semantics and
limits; [`examples/py-jobs`](https://github.com/WasmKeep/wasmkeep/tree/v0.3.0/examples/py-jobs) is a working app.

## Workflows

`wasmkeep.workflow` starts and follows durable workflows
([ADR 0019](https://github.com/WasmKeep/wasmkeep/blob/v0.3.0/docs/adr/0019-workflows-as-an-sdk-pattern.md)): the
workflow class is written with the Rust SDK's runner (as
[`examples/workflows/orders`](https://github.com/WasmKeep/wasmkeep/tree/v0.3.0/examples/workflows/orders),
which a release carries already built, `workflows-orders.wasm`), one actor
per instance, and this client speaks its protocol over `actors.call`, like
the Rust `workflow::Client`. Only the client side is in Python.

```python
from wasmkeep import workflow

orders = workflow.Client("orders")              # instance `id` is actor `orders/<id>`
orders.start("order-42", "item=book")           # a Status; idempotent while kept
orders.send_event("order-42", "approved", b"")
status = orders.status("order-42")              # status.state, .output, .error, ...
orders.cancel("order-42")
```

| | |
| --- | --- |
| `workflow.Client(cls)` | Instances of the workflow class `cls` |
| `start(id, input=b"")` | Records the start (the instance runs in its own event, right after); returns its `Status` |
| `status(id)`, `cancel(id)` | The instance's `Status`; `cancel` stops it (a finished one is unchanged) |
| `send_event(id, name, payload=b"")` | Delivers an event, recorded durably |
| `Status` | `state` (`"running"`, `"waiting"`, `"completed"`, `"failed"`, `"cancelled"`), `wake_at`, `waiting_for`, `output`, `error`, `positions`, `is_finished` |

Failures raise `workflow.WorkflowError` (`.kind`: unknown, finished, call,
refused with `.status`). The [guide](https://github.com/WasmKeep/wasmkeep/blob/v0.3.0/docs/workflows.md#python) covers
the semantics.

## From a clone

The [examples](https://github.com/WasmKeep/wasmkeep/tree/v0.3.0/examples) build
against the SDK in the repository, with the same command
(`examples/py-counter/build.sh`): [counter](https://github.com/WasmKeep/wasmkeep/tree/v0.3.0/examples/py-counter),
[chat room](https://github.com/WasmKeep/wasmkeep/tree/v0.3.0/examples/py-chat-room),
[jobs](https://github.com/WasmKeep/wasmkeep/tree/v0.3.0/examples/py-jobs)
(key-value, queues and workflow orders). The protocol code
(`wasmkeep/wire.py`) imports no host bindings, so the SDK's unit tests run
under plain CPython, without componentize-py:

```bash
sdks/python/test/run.sh          # python3 -m unittest discover -s test
```

Python components are large (about 19 MB, CPython included) and slower than
Rust: see [languages](https://github.com/WasmKeep/wasmkeep/blob/v0.3.0/docs/languages.md).

## License

Apache-2.0; see `LICENSE` and `NOTICE` in the package.
