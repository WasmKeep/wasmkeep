"""The app class of the jobs example, in Python: an HTTP API over the SDK's
kv, queue and workflow clients, and the consumer the `jobs` queue pushes to.
It talks to the Rust shard and queue classes of examples/jobs (`kv` and
`queue`) and the `orders` workflow class of examples/workflows, and must be
deployed as class `app`: `queue/jobs` delivers to `app/worker`. Each route
needs only the classes it calls.

Key-value (keys spread over 4 shards of class `kv`):

- `GET /kv/<key>`: the value, with its version in `etag`; 404 if absent.
  `?cached=1` reads through this actor's 60 s read cache.
- `PUT /kv/<key>`: stores the body and answers the new version.
  `if-match: <version>` or `if-none-match: *` make it a compare-and-set
  (409 when it does not hold); `?ttl_ms=N` expires it.
- `DELETE /kv/<key>`: 200 if it was there, 404 otherwise; with
  `if-match: <version>`, only if it holds that version (409 otherwise).
- `GET /kv?prefix=P`: the keys starting with `P`, one per line.

Queues (actors of class `queue`):

- `POST /jobs`: queues the body on `queue/jobs`, which pushes it to
  `app/worker`. The worker stores the body in upper case at key
  `job:<id>`; a body starting with `fail` asks for a retry every time, so it
  ends in the dead-letter table after three attempts.
- `GET /jobs/stats`, `GET /jobs/dead`: counts and dead letters.
- `POST /inbox`: queues the body on `queue/inbox`, a pull queue.
- `POST /inbox/batch`: queues each line of the body, in one write; answers
  their ids, one per line.
- `POST /inbox/lease?visibility_ms=N&max=M`: leases up to `M` (default one)
  messages, answered as `<id> <attempt> <body>` lines.
- `POST /inbox/ack?id=I&attempt=A`: 200 if that lease was current, 409 if it
  ran out (the message was leased again) or was settled.

Workflows (instances of the Rust `orders` class of examples/workflows,
through the SDK's workflow client; the routes of examples/workflows/app):

- `POST /orders/<id>`: starts order `<id>` with the body as its input;
  answers its status. Starting an id again answers the existing order's.
- `GET /orders/<id>`: its status, one `key: value` per line; 404 if there is
  none.
- `POST /orders/<id>/approve`: delivers the `approved` event, then answers
  the status (409 if the order has finished).
- `POST /orders/<id>/cancel`: cancels it.
"""

from wasmkeep import Actor, Response, kv, queue, workflow

SHARDS = 4

# Keys over 4 shards, with a 60 s read cache: `get` may use it, writes
# through this client update it, `get_fresh` skips it.
store = kv.Client("kv", SHARDS).with_read_cache(60_000)
jobs = queue.Client("queue/jobs")
inbox = queue.Client("queue/inbox")
orders = workflow.Client("orders")


def param(path, name):
    if "?" not in path:
        return None
    for pair in path.split("?")[1].split("&"):
        parts = pair.split("=")
        if parts[0] == name:
            return parts[1] if len(parts) > 1 else ""
    return None


def number(value):
    """A non-negative integer parameter, or None."""
    return int(value) if value is not None and value.isascii() and value.isdigit() else None


def text(status, body):
    return Response(body, status)


def failure(e):
    """The error's text, with the kv and queue errors' status mapping."""
    if isinstance(e, kv.KvError) and e.kind == "conflict":
        return text(409, "the key changed\n")
    if isinstance(e, (kv.KvError, queue.QueueError)):
        return text(502, f"{e}\n")
    raise e


def show(status):
    """A workflow status as `key: value` lines, as the Rust app shows it."""
    out = f"state: {status.state}\npositions: {status.positions}\n"
    if status.wake_at is not None:
        out += f"wake_at: {status.wake_at}\n"
    if status.waiting_for:
        out += f"waiting_for: {','.join(status.waiting_for)}\n"
    if status.output is not None:
        out += f"output: {status.output.decode(errors='replace')}\n"
    if status.error is not None:
        out += f"error: {status.error}\n"
    return out


def order_route(req, parts):
    """`/orders/<id>` and `/orders/<id>/<action>`, with the Rust app's
    statuses: 404 for an unknown order, 409 for a finished one."""
    id_ = parts[1]
    try:
        if len(parts) == 2 and req.method == "POST":
            return show(orders.start(id_, req.bytes()))
        if len(parts) == 2 and req.method == "GET":
            return show(orders.status(id_))
        if parts[2:] == ["approve"] and req.method == "POST":
            orders.send_event(id_, "approved", b"")
            return show(orders.status(id_))
        if parts[2:] == ["cancel"] and req.method == "POST":
            return show(orders.cancel(id_))
    except workflow.WorkflowError as e:
        if e.kind == "unknown":
            return text(404, "no such order\n")
        if e.kind == "finished":
            return text(409, "the order has finished\n")
        return text(502, f"{e}\n")
    return text(404, "see the README for routes\n")


def kv_route(req, key):
    headers = {k.lower(): v for k, v in req.headers.items()}
    match = headers.get("if-match")
    version = None if match is None else number(match.replace('"', ""))
    if match is not None and version is None:
        return text(400, "if-match is not a version\n")
    try:
        if req.method == "GET":
            entry = store.get(key) if param(req.path, "cached") == "1" else store.get_fresh(key)
            if entry is None:
                return text(404, "no such key\n")
            return Response(entry.value, 200, {"etag": str(entry.version)})
        if req.method == "PUT":
            value = req.bytes()
            ttl_ms = number(param(req.path, "ttl_ms"))
            absent = headers.get("if-none-match") == "*"
            if ttl_ms is not None:
                expect = version if version is not None else "absent" if absent else "any"
                v = store.put_with(key, value, expect=expect, ttl_ms=ttl_ms)
            elif version is not None or absent:
                v = store.compare_and_set(key, version, value)
            else:
                v = store.put(key, value)
            return Response(f"{v}\n", 200, {"etag": str(v)})
        if req.method == "DELETE":
            removed = store.delete(key) if version is None else store.delete_if(key, version)
            return text(200, "deleted\n") if removed else text(404, "no such key\n")
        return text(405, "use GET, PUT or DELETE\n")
    except Exception as e:  # noqa: BLE001 - failure() re-raises the others
        return failure(e)


def attempt(f):
    try:
        return f()
    except Exception as e:  # noqa: BLE001 - failure() re-raises the others
        return failure(e)


def listing(path):
    keys = store.list(param(path, "prefix") or "", None, 1000)
    return "".join(f"{k.decode(errors='replace')}\n" for k in keys)


def lease(path):
    max_ = number(param(path, "max"))
    visibility = number(param(path, "visibility_ms"))
    messages = inbox.lease(1 if max_ is None else max_, 30_000 if visibility is None else visibility)
    return "".join(f"{m.id} {m.attempt} {m.body.decode(errors='replace')}\n" for m in messages)


def ack(path):
    id_ = number(param(path, "id"))
    attempt_ = number(param(path, "attempt"))
    if id_ is None or attempt_ is None:
        return text(400, "give id and attempt\n")
    # ack takes the leased message; only its id and attempt are sent.
    if inbox.ack(queue.Message(id_, attempt_, 0, b"")):
        return text(200, "acked\n")
    return text(409, "that lease is no longer current\n")


def stats(q):
    s = q.stats()
    return f"ready={s.ready} delayed={s.delayed} in_flight={s.in_flight} dead={s.dead}\n"


def dead(q):
    return "".join(f"{d.message.id} {d.message.attempt} {d.reason}\n" for d in q.dead_letters(100))


def work(message):
    """The worker: one outcome per pushed job. Writing `job:<id>` is
    idempotent, so a duplicate delivery does no harm."""
    body = message.body.decode(errors="replace")
    if body.startswith("fail"):
        return "retry"
    try:
        store.put(f"job:{message.id}", body.upper())
        return "ack"
    except Exception:  # noqa: BLE001 - any failure: deliver it again
        return "retry"


class App(Actor):
    def fetch(self, req):
        if queue.is_delivery(req):
            return queue.handle_delivery(req, lambda _queue, m: work(m))
        route = req.path.split("?")[0]
        get = req.method == "GET"
        post = req.method == "POST"
        if route == "/kv" and get:
            return attempt(lambda: listing(req.path))
        if route.startswith("/kv/") and len(route) > 4:
            return kv_route(req, route[4:])
        if route == "/jobs" and post:
            return attempt(lambda: f"{jobs.send(req.bytes())}\n")
        if route == "/jobs/stats" and get:
            return attempt(lambda: stats(jobs))
        if route == "/jobs/dead" and get:
            return attempt(lambda: dead(jobs))
        if route == "/inbox" and post:
            return attempt(lambda: f"{inbox.send(req.bytes())}\n")
        if route == "/inbox/batch" and post:
            lines = [line for line in req.bytes().decode(errors="replace").split("\n") if line]
            return attempt(lambda: "".join(f"{i}\n" for i in inbox.send_batch(lines)))
        if route == "/inbox/stats" and get:
            return attempt(lambda: stats(inbox))
        if route == "/inbox/lease" and post:
            return attempt(lambda: lease(req.path))
        if route == "/inbox/ack" and post:
            return attempt(lambda: ack(req.path))
        parts = route.strip("/").split("/")
        if parts[0] == "orders" and len(parts) in (2, 3) and parts[1]:
            return order_route(req, parts)
        return text(404, "see the README for routes\n")


WitWorld, IncomingHandler = App.exports()
