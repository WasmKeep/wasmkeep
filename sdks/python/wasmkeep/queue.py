"""Queues with at-least-once delivery, built from one actor (ADR 0015): the
producer and consumer side.

A queue is an actor whose SQLite table holds its messages: the Rust SDK's
queue, deployed from `examples/jobs/queue` (its settings, such as the push
consumer, are code in that class). This module speaks its protocol
(`/wasmkeep-queue/v1`) over `actors.call`:

- producers `send` (returns once the message is durable);
- pull consumers `lease` a batch, then `ack` or `retry` each message;
- push consumers answer deliveries with `handle_delivery`.

A message may be delivered more than once (a lost ack, a lease that ran
out, a failover), with the same `id`: record the ids you handled when a
duplicate would matter.

```python
from wasmkeep import Actor, queue

jobs = queue.Client("queue/jobs")

class App(Actor):
    def fetch(self, req):
        if queue.is_delivery(req):
            return queue.handle_delivery(req, lambda _queue, m: "ack")
        jobs.send("resize 42")
        return "ok\\n"
```
"""

from typing import Callable, Dict, List, Optional, Sequence

from . import Response
from ._call import CallFailed, call
from .wire import (
    MAX_BATCH,
    Data,
    DeadLetter,
    Message,
    Outcome,
    RetryAfter,
    Settlement,
    Stats,
    check_u32,
    check_u64,
    decode_counts,
    decode_dead,
    decode_messages,
    encode_bodies,
    encode_settlements,
    header,
    lossy,
    to_bytes,
)

__all__ = [
    "DELIVER_PATH",
    "MAX_BATCH",
    "Client",
    "DeadLetter",
    "Message",
    "Outcome",
    "QueueError",
    "RetryAfter",
    "Settlement",
    "Stats",
    "handle_delivery",
    "is_delivery",
]

_PREFIX = "/wasmkeep-queue/v1"
DELIVER_PATH = "/wasmkeep-queue/v1/deliver"
"""The path a push delivery arrives on, at the consumer."""
_DELAY = "x-wasmkeep-queue-delay-ms"


class QueueError(Exception):
    """Why a queue operation failed, as `kind`:

    - "storage": the queue's database refused the operation;
    - "call": the call to the queue failed. For a send, the message may or
      may not be queued;
    - "refused": the queue answered with an unexpected `status`.
    """

    def __init__(self, kind: str, detail: str = "", status: Optional[int] = None):
        if kind == "storage":
            message = f"storage: {detail}"
        elif kind == "call":
            message = detail
        else:
            message = f"queue answered {status}: {detail}"
        super().__init__(message)
        self.kind = kind
        self.detail = detail
        """The detail: the call's error, or the queue's answer."""
        self.status = status
        """The queue's status, for "refused"."""


def _bad_reply() -> QueueError:
    return QueueError("refused", "malformed reply from queue", 200)


class Client:
    """Reaches one queue actor."""

    def __init__(self, address: str):
        """The queue at `address` (`class/name`)."""
        self.address = address

    def _call(self, method: str, path: str, headers: Dict[str, str], body: bytes) -> bytes:
        try:
            status, reply = call(self.address, method, path, headers, body)
        except CallFailed as e:
            raise QueueError("call", str(e)) from None
        if status != 200:
            raise QueueError("refused", lossy(reply), status)
        return reply

    def send(self, body: Data) -> int:
        """Queues `body`; returns its id once it is durable."""
        return self.send_delayed(body, 0)

    def send_delayed(self, body: Data, delay_ms: int) -> int:
        """Queues `body`, visible after `delay_ms`."""
        ids = self._send([body], delay_ms)
        if not ids:
            raise _bad_reply()
        return ids[0]

    def send_batch(self, bodies: Sequence[Data]) -> List[int]:
        """Queues several messages (at most `MAX_BATCH`) in one durable
        write; returns their ids in order. All or none are queued."""
        return self._send(bodies, 0)

    def _send(self, bodies: Sequence[Data], delay_ms: int) -> List[int]:
        headers = {_DELAY: str(check_u64(delay_ms, "delay_ms"))}
        payload = encode_bodies([to_bytes(b) for b in bodies])
        ids = decode_counts(self._call("POST", f"{_PREFIX}/send", headers, payload))
        if ids is None or len(ids) != len(bodies):
            raise _bad_reply()
        return ids

    def lease(self, max: int, visibility_ms: int) -> List[Message]:  # noqa: A002 - Rust's name
        """Leases up to `max` visible messages for `visibility_ms`."""
        path = (
            f"{_PREFIX}/lease?max={check_u32(max, 'max')}"
            f"&visibility_ms={check_u64(visibility_ms, 'visibility_ms')}"
        )
        messages = decode_messages(self._call("POST", path, {}, b""))
        if messages is None:
            raise _bad_reply()
        return messages

    def ack(self, message: Message) -> bool:
        """Deletes a leased message. False if this lease is no longer
        current (it ran out and the message was leased again, or was
        settled). Any object with `id` and `attempt` will do."""
        return self.settle([Settlement(message.id, message.attempt, "ack")])[:1] == [True]

    def retry(self, message: Message, delay_ms: Optional[int] = None) -> bool:
        """Makes a leased message visible again after `delay_ms` (None: the
        queue's backoff). False if this lease is no longer current."""
        outcome: Outcome = "retry" if delay_ms is None else RetryAfter(delay_ms)
        return self.settle([Settlement(message.id, message.attempt, outcome)])[:1] == [True]

    def settle(self, items: Sequence[Settlement]) -> List[bool]:
        """Settles several leased messages in one call; one flag per item,
        True where the lease was current."""
        flags = decode_counts(self._call("POST", f"{_PREFIX}/settle", {}, encode_settlements(items)))
        if flags is None:
            raise _bad_reply()
        return [f == 1 for f in flags]

    def stats(self) -> Stats:
        """How many messages are ready, delayed, in flight and dead."""
        counts = decode_counts(self._call("GET", f"{_PREFIX}/stats", {}, b""))
        if counts is None or len(counts) != 4:
            raise _bad_reply()
        return Stats(*counts)

    def dead_letters(self, limit: int) -> List[DeadLetter]:
        """The oldest `limit` dead-lettered messages."""
        path = f"{_PREFIX}/dead?limit={check_u32(limit, 'limit')}"
        items = decode_dead(self._call("GET", path, {}, b""))
        if items is None:
            raise _bad_reply()
        return items


def is_delivery(req) -> bool:
    """Whether `req` (a `wasmkeep.Request`) is a push delivery from a queue
    (answer it with `handle_delivery`)."""
    return req.method == "POST" and req.path == DELIVER_PATH


def handle_delivery(req, handle: Callable[[str, Message], Outcome]) -> Response:
    """Answers a push delivery: `handle` gets the sending queue's address
    (`tenant/class/name`) and each message, and returns its outcome ("ack",
    "retry" or `RetryAfter(ms)`). Return the result from `fetch`.
    Deliveries from outside clients (without `x-wasmkeep-caller`) get 403. If
    `handle` raises, the exception propagates (`fetch` then answers 500) and
    the queue retries the whole batch with its backoff. Record handled ids
    in this actor's database, in the same event, when a duplicate delivery
    would matter."""
    queue = header(req.headers, "x-wasmkeep-caller")
    if queue is None:
        return Response("deliveries come from queue actors\n", 403, {"content-type": "text/plain"})
    messages = decode_messages(req.bytes())
    if messages is None:
        return Response("malformed delivery\n", 400, {"content-type": "text/plain"})
    outcomes = [Settlement(m.id, m.attempt, handle(queue, m)) for m in messages]
    return Response(encode_settlements(outcomes), 200, {"content-type": "application/octet-stream"})
