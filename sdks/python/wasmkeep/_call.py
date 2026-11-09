"""One request to another actor for the `kv` and `queue` clients (the Rust
SDK's `wire::call`). A failed call becomes a message, worded as in Rust: the
call failing, not the callee's answer (that is in the status)."""

from typing import Dict, Tuple

from . import CallError, actors


class CallFailed(Exception):
    """The call got no answer; the text says why, as in Rust."""


def call(address: str, method: str, path: str, headers: Dict[str, str], body: bytes) -> Tuple[int, bytes]:
    """`(status, body)` once the callee answered; `CallFailed` if the call
    failed."""
    try:
        r = actors.call(address, method=method, path=path, headers=headers, body=body)
    except CallError as e:
        if e.kind == "invalid-address":
            text = f"invalid address {address}: {e.detail}"
        elif e.kind == "reentrant":
            text = f"call to {address} would re-enter an actor already in this call chain"
        elif e.kind == "too-large":
            text = f"call to {address}: body too large"
        elif e.kind == "timeout":
            text = f"call to {address} timed out"
        elif e.kind == "failed":
            text = f"call to {address} failed: {e.detail}"
        else:
            raise
        raise CallFailed(text) from None
    return r.status, bytes(r.body)
