"""The WasmKeep Python SDK: write actors as plain Python classes.

```python
from wasmkeep import Actor, storage

class Hits(Actor):
    def init(self):
        storage.exec("CREATE TABLE IF NOT EXISTS hits (at INTEGER NOT NULL)")

    def fetch(self, req):
        storage.exec("INSERT INTO hits (at) VALUES (unixepoch())")
        [row] = storage.query("SELECT count(*) AS n FROM hits")
        return {"hits": row["n"], "path": req.path}

WitWorld, IncomingHandler = Hits.exports()
```

Build `app.py` into a component (componentize-py, with the WIT world this
package carries):

    python -m wasmkeep build app -o actor.wasm

Every hook runs one event at a time per actor: no locks needed. One
instance of the class handles events until the actor hibernates; keep state
that must survive in `storage`, not on `self`.
"""

import json as _json
from typing import Any, Dict, List, Optional, Tuple, Union

# componentize-py generates these two modules into the component: they exist
# there and nowhere else. Under plain CPython (`python -m wasmkeep build`, an
# editor, a test runner) the package still imports, and whatever needs the
# host says so when it is used.
try:
    from componentize_py_types import Err, Ok
    from wit_world import exports as _exports
    from wit_world.imports import actors as _actors
    from wit_world.imports import alarms as _alarms
    from wit_world.imports import env as _env
    from wit_world.imports import storage as _storage
    from wit_world.imports import websockets as _ws
    from wit_world.imports.streams import StreamError_Closed
    from wit_world.imports.wasi_http_types import (
        Fields,
        Method_Other,
        OutgoingBody,
        OutgoingResponse,
        ResponseOutparam,
    )
    from wit_world.imports.wasmkeep_actor_types import (
        Value_Blob,
        Value_Integer,
        Value_Null,
        Value_Real,
        Value_Text,
    )
except ModuleNotFoundError as _missing:
    if _missing.name not in ("componentize_py_types", "wit_world"):
        raise

    class _Outside:
        def __call__(self, *args, **kwargs):
            raise RuntimeError(
                "wasmkeep: the host's functions exist only inside a component; "
                "build one with `python -m wasmkeep build` and serve it with wasmkeep"
            )

        def __getattr__(self, name):
            if name.startswith("__"):
                raise AttributeError(name)
            self()

    class Err(Exception):  # type: ignore[no-redef]
        """Never raised out here: only the host fails a call."""

    Ok = _exports = _actors = _alarms = _env = _storage = _ws = _Outside()
    StreamError_Closed = Fields = Method_Other = OutgoingBody = _Outside()
    OutgoingResponse = ResponseOutparam = _Outside()
    Value_Blob = Value_Integer = Value_Null = Value_Real = Value_Text = _Outside()

__all__ = [
    "Actor",
    "ActorAddress",
    "CallError",
    "CallResponse",
    "Request",
    "Response",
    "SqlError",
    "actors",
    "alarms",
    "env",
    "json",
    "kv",
    "queue",
    "sockets",
    "storage",
    "workflow",
]

# The package's version, as in pyproject.toml (test/test_package.py checks it).
__version__ = "0.3.0"
# The version of the `wasmkeep:actor` WIT package in wasmkeep/wit: the
# interface the SDK is written against, which changes less often.
WIT_VERSION = "0.2.0"

SqlValue = Union[None, int, float, str, bytes, bool]

# ---------------------------------------------------------------- storage


class SqlError(Exception):
    """A failed SQL statement. `kind` is one of: syntax, constraint, bind,
    multiple-statements, quota-exceeded, not-allowed, internal."""

    def __init__(self, kind: str, message: str):
        super().__init__(message)
        self.kind = kind
        self.message = message


def _to_value(v: SqlValue):
    if v is None:
        return Value_Null()
    if isinstance(v, bool):
        return Value_Integer(1 if v else 0)
    if isinstance(v, int):
        return Value_Integer(v)
    if isinstance(v, float):
        return Value_Real(v)
    if isinstance(v, str):
        return Value_Text(v)
    if isinstance(v, (bytes, bytearray, memoryview)):
        return Value_Blob(bytes(v))
    raise TypeError(f"cannot bind {type(v).__name__} to SQL")


def _from_value(v) -> SqlValue:
    return None if isinstance(v, Value_Null) else v.value


def _sql(f, *args):
    try:
        return f(*args)
    except Err as e:
        err = e.value
        raise SqlError(err.kind.name.lower().replace("_", "-"), err.message) from None


class _Storage:
    """The actor's private SQLite database."""

    def exec(self, sql: str, *params: SqlValue) -> int:
        """Runs a statement that returns no rows; returns the rows changed."""
        return _sql(_storage.exec, sql, [_to_value(p) for p in params])

    def exec_durable(self, sql: str, *params: SqlValue) -> int:
        """Like `exec`, but returns only once the write is durable
        (replicated to the cluster's bucket, or fsynced on a single node).
        Inside a transaction it takes only `COMMIT` (or `END`), which makes
        the whole transaction durable."""
        return _sql(_storage.exec_durable, sql, [_to_value(p) for p in params])

    def query(self, sql: str, *params: SqlValue) -> List[Dict[str, SqlValue]]:
        """Runs a statement and returns its rows as dicts."""
        rows = _sql(_storage.query, sql, [_to_value(p) for p in params])
        return [
            {c: _from_value(v) for c, v in zip(rows.columns, values)}
            for values in rows.values
        ]

    def delete_self(self) -> None:
        """Deletes this actor once the current event ends."""
        _storage.delete_self()


storage = _Storage()

# ----------------------------------------------------------------- alarms


class _Alarms:
    """The actor's single alarm. It fires `alarm()` at least once, also after
    a crash or a move to another node."""

    def set(self, at_ms: int) -> None:
        """Sets (replaces) the alarm: a time in ms since the Unix epoch."""
        _alarms.set_alarm(int(at_ms))

    def get(self) -> Optional[int]:
        """The pending alarm (ms since the Unix epoch), if any."""
        return _alarms.get_alarm()

    def clear(self) -> None:
        """Cancels the alarm."""
        _alarms.clear_alarm()


alarms = _Alarms()

# ----------------------------------------------------------- actor calls


class CallError(Exception):
    """A call that did not get an answer. `kind`: invalid-address,
    reentrant, too-large, timeout, failed."""

    def __init__(self, kind: str, message: str, detail: str = ""):
        super().__init__(message)
        self.kind = kind
        # The host's text for invalid-address and failed ("" otherwise).
        self.detail = detail


class CallResponse:
    """Another actor's answer."""

    def __init__(self, status: int, headers: Dict[str, str], body: bytes):
        self.status = status
        self.headers = headers
        self.body = body

    def text(self) -> str:
        return self.body.decode()

    def json(self) -> Any:
        return _json.loads(self.body)


def _kebab(name: str) -> str:
    # "CallError_TooLarge" -> "too-large"
    tag = name.split("_", 1)[1]
    return "".join("-" + c.lower() if c.isupper() else c for c in tag).lstrip("-")


class ActorAddress:
    """An actor's address. Within a tenant, `f"{class_name}/{name}"` is what
    `actors.call` takes."""

    def __init__(self, tenant: str, class_name: str, name: str):
        self.tenant = tenant
        self.class_name = class_name
        self.name = name

    def __repr__(self) -> str:
        return f"ActorAddress({self.tenant!r}, {self.class_name!r}, {self.name!r})"


class _Actors:
    def self_address(self) -> ActorAddress:
        """This actor's own address (requests arrive with
        `/actor/<class>/<name>` removed, so this is how an actor learns who
        it is)."""
        a = _actors.self_address()
        return ActorAddress(a.tenant, a.class_, a.name)

    def call(
        self,
        address: str,
        method: str = "GET",
        path: str = "/",
        headers: Optional[Dict[str, str]] = None,
        body: Union[str, bytes] = b"",
    ) -> CallResponse:
        """Calls actor `class/name` (same tenant) and waits for its
        response. Other events to this actor wait meanwhile."""
        if isinstance(body, str):
            body = body.encode()
        request = _actors.Request(method, path, list((headers or {}).items()), body)
        try:
            r = _actors.call(address, request)
        except Err as e:
            kind = _kebab(type(e.value).__name__)
            detail = getattr(e.value, "value", None) or ""
            raise CallError(kind, detail or kind, detail) from None
        return CallResponse(r.status, dict(r.headers), bytes(r.body))


actors = _Actors()

# ---------------------------------------------------------------- env


class _Env:
    """Configuration and secrets the operator gives this class
    (``wasmkeep serve --env CLASS=FILE``)."""

    def get(self, name: str, default: Optional[str] = None) -> Optional[str]:
        """The value of `name`, or `default` if it is not set."""
        value = _env.get(name)
        return default if value is None else value


env = _Env()

# ------------------------------------------------------------- websockets


class _Sockets:
    def send(self, conn: int, data: Union[str, bytes]) -> bool:
        """Sends a text or binary message. Returns False if the connection
        is gone or its buffer is full."""
        msg = _ws.Message_Text(data) if isinstance(data, str) else _ws.Message_Binary(bytes(data))
        try:
            _ws.send(conn, msg)
            return True
        except Err:
            return False

    def broadcast(self, data: Union[str, bytes], except_conn: Optional[int] = None) -> None:
        """Sends to every open connection of this actor, except `except_conn`."""
        for conn in _ws.connections():
            if conn != except_conn:
                self.send(conn, data)

    def close(self, conn: int, code: int = 1000, reason: str = "") -> None:
        """Closes a connection."""
        _ws.close(conn, code, reason)

    def connections(self) -> List[int]:
        """This actor's open connections."""
        return list(_ws.connections())


sockets = _Sockets()

# ------------------------------------------------------------------- HTTP


class Request:
    """An incoming request. `path` includes the query, relative to the
    actor (`/actor/<class>/<name>` removed)."""

    def __init__(self, incoming):
        self._incoming = incoming
        self._body: Optional[bytes] = None
        method = incoming.method()
        self.method: str = (
            method.value if isinstance(method, Method_Other) else type(method).__name__.split("_", 1)[1].upper()
        )
        self.path: str = incoming.path_with_query() or "/"
        self.headers: Dict[str, str] = {
            k: v.decode(errors="replace") for k, v in incoming.headers().entries()
        }

    def bytes(self) -> bytes:
        """The body, read on first use."""
        if self._body is None:
            self._body = _read_all(self._incoming)
        return self._body

    def text(self) -> str:
        return self.bytes().decode()

    def json(self) -> Any:
        return _json.loads(self.bytes())


class Response:
    """A full response; a handler may also return just a body (str, bytes),
    or a dict or list (sent as JSON)."""

    def __init__(
        self,
        body: Union[str, bytes] = b"",
        status: int = 200,
        headers: Optional[Dict[str, str]] = None,
    ):
        self.body = body
        self.status = status
        self.headers = headers or {}


def json(value: Any, status: int = 200) -> Response:
    """A JSON response."""
    return Response(_json.dumps(value), status, {"content-type": "application/json"})


def _read_all(incoming) -> bytes:
    body = incoming.consume()
    stream = body.stream()
    chunks = []
    while True:
        try:
            chunks.append(stream.blocking_read(65536))
        except Err as e:
            if isinstance(e.value, StreamError_Closed):
                break
            raise
    stream.__exit__(None, None, None)
    return b"".join(chunks)


def _respond(out, response: Any) -> None:
    if isinstance(response, Response):
        r = response
    elif isinstance(response, (dict, list)):
        r = json(response)
    elif response is None:
        r = Response(b"", 204)
    else:
        r = Response(response)
    body = r.body.encode() if isinstance(r.body, str) else bytes(r.body)
    headers = {"content-type": "text/plain; charset=utf-8"}
    headers.update({k.lower(): v for k, v in r.headers.items()})
    outgoing = OutgoingResponse(Fields.from_list([(k, v.encode()) for k, v in headers.items()]))
    outgoing.set_status_code(r.status)
    out_body = outgoing.body()
    ResponseOutparam.set(out, Ok(outgoing))
    stream = out_body.write()
    for at in range(0, len(body), 4096):
        stream.blocking_write_and_flush(body[at : at + 4096])
    stream.__exit__(None, None, None)
    OutgoingBody.finish(out_body, None)


# ------------------------------------------------------------------ actor


class Actor:
    """Subclass and override any hook; all are optional."""

    def init(self) -> None:
        """Runs once per component version, before any other event
        (create or migrate tables here)."""

    def fetch(self, req: Request) -> Any:
        """Handles an HTTP request. Return a str or bytes (status 200), a
        dict or list (JSON), or a `Response`."""
        return Response("no fetch handler\n", 404)

    def alarm(self) -> None:
        """The alarm fired. Raising retries it with backoff."""

    def websocket_open(self, conn: int, path: str) -> Optional[bool]:
        """A client connected. Return False (or raise) to refuse it."""
        return True

    def websocket_message(self, conn: int, data: Union[str, bytes]) -> None:
        """A message arrived on one of this actor's connections."""

    def websocket_close(self, conn: int, code: int, reason: str) -> None:
        """A connection closed."""

    @classmethod
    def exports(cls) -> Tuple[type, type]:
        """The component exports for this actor:
        `WitWorld, IncomingHandler = MyActor.exports()` at module level."""
        holder: Dict[str, Actor] = {}

        def instance() -> Actor:
            if "actor" not in holder:
                holder["actor"] = cls()
            return holder["actor"]

        def guarded(f, *args):
            # An exception becomes the WIT `result` error (shown in the
            # host's logs).
            try:
                return f(*args)
            except Err:
                raise
            except Exception as e:  # noqa: BLE001 - reported to the host
                raise Err(f"{type(e).__name__}: {e}") from None

        class WitWorld:
            def on_init(self) -> None:
                guarded(instance().init)

            def on_alarm(self) -> None:
                guarded(instance().alarm)

            def on_websocket_open(self, conn: int, path: str) -> None:
                if guarded(instance().websocket_open, conn, path) is False:
                    raise Err("refused")

            def on_websocket_message(self, conn: int, msg) -> None:
                instance().websocket_message(conn, msg.value)

            def on_websocket_close(self, conn: int, code: int, reason: str) -> None:
                instance().websocket_close(conn, code, reason)

        class IncomingHandler(_exports.IncomingHandler):
            def handle(self, request, response_out) -> None:
                try:
                    response = instance().fetch(Request(request))
                except Exception as e:  # noqa: BLE001 - becomes a 500
                    response = Response(f"{type(e).__name__}: {e}\n", 500)
                _respond(response_out, response)

        return WitWorld, IncomingHandler


# The kv, queue and workflow clients (ADR 0015, ADR 0019) use `actors`,
# `CallError` and `Response` above, so they are imported last.
from . import kv, queue, workflow  # noqa: E402
