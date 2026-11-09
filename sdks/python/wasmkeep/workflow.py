"""Durable workflows (ADR 0019): the client side.

A workflow class is served by the Rust SDK's runner (or the TypeScript
one), one actor per instance, as `examples/workflows/orders` is. This module
starts and follows instances from any Python actor, speaking the runner's
protocol (`/wasmkeep-workflow/v1`) over `actors.call`, byte for byte like
the Rust `workflow::Client`:

```python
from wasmkeep import Actor, workflow

orders = workflow.Client("orders")

class App(Actor):
    def fetch(self, req):
        orders.start("order-42", "item=book")      # idempotent while kept
        orders.send_event("order-42", "approved", b"")
        return orders.status("order-42").state     # "waiting", "completed", ...
```

Errors raise `WorkflowError`: "unknown" (no such instance), "finished" (an
event to a finished instance), "call" or "refused".
"""

from typing import Optional

from ._call import CallFailed, call
from .wire import STATES, Data, Status, decode_status, hex, lossy, to_bytes

__all__ = ["STATES", "Client", "Status", "WorkflowError"]

_PREFIX = "/wasmkeep-workflow/v1"


class WorkflowError(Exception):
    """Why a workflow operation failed, as `kind`:

    - "unknown": no instance with that id (never started, or deleted after
      its retention);
    - "finished": the instance has finished, so it takes no more events;
    - "call": the call failed (the instance may or may not have recorded
      it);
    - "refused": the instance answered with an unexpected `status`.
    """

    def __init__(self, kind: str, detail: str = "", status: Optional[int] = None):
        if kind == "unknown":
            message = "no such workflow instance"
        elif kind == "finished":
            message = "the workflow instance has finished"
        elif kind == "call":
            message = detail
        else:
            message = f"refused ({status}): {detail}"
        super().__init__(message)
        self.kind = kind
        self.detail = detail
        """The detail: the call's error, or the instance's answer."""
        self.status = status
        """The instance's status, for "refused"."""


class Client:
    """Starts and queries instances of one workflow class."""

    def __init__(self, cls: str):
        """Instances of `cls` (the workflow class's name): instance `id` is
        actor `cls/id`."""
        self.cls = cls

    def _call(self, id: str, method: str, path: str, body: bytes) -> bytes:  # noqa: A002 - Rust's name
        try:
            status, reply = call(f"{self.cls}/{id}", method, f"{_PREFIX}{path}", {}, body)
        except CallFailed as e:
            raise WorkflowError("call", str(e)) from None
        if status == 200:
            return reply
        if status == 404:
            raise WorkflowError("unknown")
        if status == 409:
            raise WorkflowError("finished")
        raise WorkflowError("refused", lossy(reply), status)

    @staticmethod
    def _status(body: bytes) -> Status:
        status = decode_status(body)
        if status is None:
            raise WorkflowError("refused", "malformed status", 200)
        return status

    def start(self, id: str, input: Data = b"") -> Status:  # noqa: A002 - Rust's names
        """Starts instance `id` with `input`, or returns its status if it
        exists (a start is idempotent while the instance is kept). It runs
        in its own event, soon after: this call only records it."""
        return self._status(self._call(id, "POST", "/start", to_bytes(input)))

    def status(self, id: str) -> Status:  # noqa: A002 - Rust's name
        """The instance's status; "unknown" if there is none."""
        return self._status(self._call(id, "GET", "/status", b""))

    def send_event(self, id: str, name: str, payload: Data = b"") -> None:  # noqa: A002 - Rust's name
        """Delivers event `name` with `payload`: recorded durably, then the
        instance runs. "unknown" if it was never started, "finished" if it
        has finished."""
        self._call(id, "POST", f"/event?name={hex(name.encode())}", to_bytes(payload))

    def cancel(self, id: str) -> Status:  # noqa: A002 - Rust's name
        """Cancels the instance: it runs no further step (one running now
        finishes first). Returns its status; a finished one is unchanged."""
        return self._status(self._call(id, "POST", "/cancel", b""))
