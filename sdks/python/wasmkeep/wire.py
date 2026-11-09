"""The small binary framing the `kv`, `queue` and `workflow` clients speak
with the actors that hold the data (the Rust SDK's `wire.rs`, byte for byte):
unsigned integers as 8 bytes big-endian, byte strings as a 4-byte
big-endian length then the bytes. Also the codecs of each message, the
FNV-1a shard hash and the KV read cache.

Pure: it imports no host bindings (nothing from `wasmkeep` itself), so the
unit tests run it under plain CPython. Internal to the SDK; the shapes
follow the protocols in ADR 0015 and ADR 0019.
"""

import re
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Sequence, Tuple, TypeVar, Union

U64_MAX = 0xFFFF_FFFF_FFFF_FFFF
U32_MAX = 0xFFFF_FFFF

Data = Union[str, bytes, bytearray, memoryview]

# ------------------------------------------------------------------ framing


def check_u64(v: int, what: str = "value") -> int:
    """`v` checked as a u64: a ValueError where Rust's types rule it out."""
    if isinstance(v, bool) or not isinstance(v, int):
        raise ValueError(f"{what} must be an integer, got {v!r}")
    if v < 0 or v > U64_MAX:
        raise ValueError(f"{what} must be from 0 to {U64_MAX}, got {v}")
    return v


def check_u32(v: int, what: str = "value") -> int:
    """`v` checked as a u32 (a count or a limit)."""
    if isinstance(v, bool) or not isinstance(v, int):
        raise ValueError(f"{what} must be an integer, got {v!r}")
    if v < 0 or v > U32_MAX:
        raise ValueError(f"{what} must be an integer from 0 to {U32_MAX}, got {v}")
    return v


class Writer:
    """Appends to a buffer."""

    def __init__(self) -> None:
        self._buf = bytearray()

    def u64(self, v: int) -> "Writer":
        """An unsigned 64-bit integer."""
        self._buf += check_u64(v).to_bytes(8, "big")
        return self

    def bytes(self, v: bytes) -> "Writer":
        """A byte string: its length (4 bytes), then the bytes."""
        # Bodies are capped at 16 MB by `actors.call`, so a length always fits.
        self._buf += min(len(v), U32_MAX).to_bytes(4, "big")
        self._buf += v
        return self

    def finish(self) -> bytes:
        return bytes(self._buf)


class Reader:
    """Reads what `Writer` wrote. Every read fails (None) past the end."""

    def __init__(self, buf: bytes) -> None:
        self._buf = bytes(buf)
        self._at = 0

    def u64(self) -> Optional[int]:
        if len(self._buf) - self._at < 8:
            return None
        v = int.from_bytes(self._buf[self._at : self._at + 8], "big")
        self._at += 8
        return v

    def bytes(self) -> Optional[bytes]:
        if len(self._buf) - self._at < 4:
            return None
        n = int.from_bytes(self._buf[self._at : self._at + 4], "big")
        if len(self._buf) - self._at - 4 < n:
            return None
        self._at += 4
        v = self._buf[self._at : self._at + n]
        self._at += n
        return v

    def is_empty(self) -> bool:
        return self._at >= len(self._buf)


def _u32(v: Optional[int]) -> Optional[int]:
    """A u64 that must fit a u32 (an attempt), as Rust's `u32::try_from`."""
    return v if v is not None and v <= U32_MAX else None


def hex(data: bytes) -> str:  # noqa: A001 - the name the other SDKs use
    """Lowercase hex, so any key fits in a path segment."""
    return bytes(data).hex()


def to_bytes(v: Data) -> bytes:
    """A key, value or body as bytes: str is UTF-8 (as Rust's `&str`)."""
    if isinstance(v, str):
        return v.encode()
    if isinstance(v, (bytes, bytearray, memoryview)):
        return bytes(v)
    raise TypeError(f"expected str or bytes, got {type(v).__name__}")


def lossy(body: bytes) -> str:
    """A refusal's text: the body as lossy UTF-8, trimmed."""
    return body.decode(errors="replace").strip()


def header(headers: Dict[str, str], name: str) -> Optional[str]:
    """A header's value (first occurrence, case-insensitive)."""
    lower = name.lower()
    for k, v in headers.items():
        if k.lower() == lower:
            return v
    return None


# ----------------------------------------------------------------------- kv

MAX_KEY_LEN = 1024
"""Keys are at most this long (bytes)."""


@dataclass(frozen=True)
class Entry:
    """A stored value."""

    value: bytes
    """The bytes stored."""
    version: int
    """Changes on every write of the key, and is never reused within a shard
    (not even after a delete), so a compare-and-set on an old version always
    loses."""
    expires_at: Optional[int] = None
    """When the key expires (ms since the Unix epoch), if it does."""


Expect = Union[str, int]
"""What a conditional write expects to find: "any", "absent" (no key, or an
expired one), or a version (an int)."""


def check_expect(expect: Expect) -> Expect:
    if expect in ("any", "absent") and isinstance(expect, str):
        return expect
    if isinstance(expect, int) and not isinstance(expect, bool):
        return check_u64(expect, "version")
    raise ValueError(f'expect must be "any", "absent" or a version, got {expect!r}')


def allows(expect: Expect, current: Optional[int]) -> bool:
    """Whether a key currently at `current` (None: absent) meets `expect`."""
    if expect == "any":
        return True
    if expect == "absent":
        return current is None
    return current == expect


def encode_expect(expect: Expect) -> str:
    return str(check_expect(expect))


_DIGITS = re.compile(r"\+?[0-9]+")


def decode_expect(s: str) -> Optional[Expect]:
    if s in ("any", "absent"):
        return s
    # Rust's `u64::from_str`: ASCII digits, an optional leading `+`.
    if len(s) > 21 or not _DIGITS.fullmatch(s):
        return None
    v = int(s)
    return v if v <= U64_MAX else None


_FNV_OFFSET = 0xCBF2_9CE4_8422_2325
_FNV_PRIME = 0x0000_0100_0000_01B3


def shard_for(key: Data, shards: int) -> int:
    """The shard (0-based) that holds `key` when there are `shards` of
    them: FNV-1a (64 bits) of the key, modulo the count (zero counts as
    one). Part of the data layout: it never changes, and matches the Rust
    SDK."""
    h = _FNV_OFFSET
    for b in to_bytes(key):
        h = ((h ^ b) * _FNV_PRIME) & U64_MAX
    return h % max(1, check_u32(shards, "shards"))


def shard_name(index: int) -> str:
    """The actor name of shard `index`: `shard-<index>`."""
    return f"shard-{index}"


def encode_entry(entry: Entry) -> bytes:
    return (
        Writer()
        .u64(entry.version)
        .u64(entry.expires_at if entry.expires_at is not None else 0)
        .bytes(entry.value)
        .finish()
    )


def decode_entry(buf: bytes) -> Optional[Entry]:
    r = Reader(buf)
    version = r.u64()
    expires = r.u64()
    value = r.bytes()
    if version is None or expires is None or value is None or not r.is_empty():
        return None
    return Entry(value, version, expires if expires != 0 else None)


def encode_keys(keys: Sequence[bytes]) -> bytes:
    w = Writer().u64(len(keys))
    for k in keys:
        w.bytes(k)
    return w.finish()


def decode_keys(buf: bytes) -> Optional[List[bytes]]:
    return _list(buf, lambda r: r.bytes())


def merge_pages(pages: Sequence[Sequence[bytes]], limit: int) -> List[bytes]:
    """Merges sorted pages from every shard into one sorted page of `limit`
    (byte order, as Rust sorts `Vec<u8>`)."""
    return sorted(k for page in pages for k in page)[:limit]


class ReadCache:
    """A bounded map of recent reads. When full, expired entries go first,
    then everything: simple, and a full cache is refilled by the next
    reads."""

    def __init__(self, capacity: int) -> None:
        self.capacity = capacity
        self.entries: Dict[bytes, Tuple[int, Optional[Entry]]] = {}

    def get(self, key: bytes, now: int, max_age: int) -> Tuple[bool, Optional[Entry]]:
        """`(True, entry)` if read less than `max_age` ago (an entry that
        expired since reads as absent, `(True, None)`); `(False, None)` on a
        miss. A miss differs from a cached absence."""
        hit = self.entries.get(key)
        if hit is None or max(0, now - hit[0]) >= max_age:
            return (False, None)
        e = hit[1]
        if e is None or (e.expires_at is not None and e.expires_at <= now):
            return (True, None)
        return (True, e)

    def insert(self, key: bytes, now: int, entry: Optional[Entry]) -> None:
        if len(self.entries) >= self.capacity and key not in self.entries:
            for k in [
                k
                for k, (_, e) in self.entries.items()
                if e is not None and e.expires_at is not None and e.expires_at <= now
            ]:
                del self.entries[k]
            if len(self.entries) >= self.capacity:
                self.entries.clear()
        self.entries[key] = (now, entry)

    def remove(self, key: bytes) -> None:
        self.entries.pop(key, None)


# -------------------------------------------------------------------- queue

MAX_BATCH = 1000
"""Messages one send may carry (the queue answers 413 past it)."""


@dataclass(frozen=True)
class Message:
    """A leased or delivered message."""

    id: int
    """Unique within the queue, never reused, the same on every delivery."""
    attempt: int
    """Which delivery this is, from 1. Acks and retries name it, so one for
    an earlier lease of a message leased again is ignored."""
    enqueued_at: int
    """When it was sent (ms since the Unix epoch)."""
    body: bytes
    """What was sent."""


@dataclass(frozen=True)
class RetryAfter:
    """An outcome: deliver the message again after `ms` milliseconds."""

    ms: int


Outcome = Union[str, RetryAfter]
"""What a consumer did with a message: "ack" (handled: delete it), "retry"
(deliver it again after the queue's backoff), or `RetryAfter(ms)`; a retry
of a message that had all its attempts dead-letters it."""


@dataclass(frozen=True)
class Settlement:
    """One settlement: a lease (`id`, `attempt`) and its outcome."""

    id: int
    attempt: int
    outcome: Outcome


@dataclass(frozen=True)
class Stats:
    """How many messages are where."""

    ready: int
    """Visible now: the next lease or delivery takes them."""
    delayed: int
    """Sent with a delay that has not passed."""
    in_flight: int
    """Leased and not yet acked, or waiting for a retry."""
    dead: int
    """In the dead-letter table."""


@dataclass(frozen=True)
class DeadLetter:
    """A dead-lettered message."""

    message: Message
    """The message as it was last delivered."""
    failed_at: int
    """When it was dead-lettered (ms since the Unix epoch)."""
    reason: str
    """Why: attempts exhausted, or the last delivery error."""


T = TypeVar("T")


def _list(buf: bytes, item: Callable[[Reader], Optional[T]]) -> Optional[List[T]]:
    """Reads a count, then `n` items with `item`; None if any is malformed
    or bytes are left over."""
    r = Reader(buf)
    n = r.u64()
    if n is None:
        return None
    out: List[T] = []
    for _ in range(n):
        v = item(r)
        if v is None:
            return None
        out.append(v)
    return out if r.is_empty() else None


def _read_message(r: Reader) -> Optional[Message]:
    id_ = r.u64()
    attempt = _u32(r.u64())
    enqueued_at = r.u64()
    body = r.bytes()
    if id_ is None or attempt is None or enqueued_at is None or body is None:
        return None
    return Message(id_, attempt, enqueued_at, body)


def _write_message(w: Writer, m: Message) -> Writer:
    return w.u64(m.id).u64(check_u32(m.attempt, "attempt")).u64(m.enqueued_at).bytes(m.body)


def encode_messages(messages: Sequence[Message]) -> bytes:
    w = Writer().u64(len(messages))
    for m in messages:
        _write_message(w, m)
    return w.finish()


def decode_messages(buf: bytes) -> Optional[List[Message]]:
    return _list(buf, _read_message)


def encode_settlements(items: Sequence[Settlement]) -> bytes:
    """`u64::MAX` in the delay slot means "the queue's backoff"."""
    w = Writer().u64(len(items))
    for s in items:
        if s.outcome == "ack":
            kind, delay = 0, 0
        elif s.outcome == "retry":
            kind, delay = 1, U64_MAX
        elif isinstance(s.outcome, RetryAfter):
            kind, delay = 1, check_u64(s.outcome.ms, "retry delay")
        else:
            raise ValueError(f'an outcome is "ack", "retry" or RetryAfter(ms), got {s.outcome!r}')
        w.u64(check_u64(s.id, "id")).u64(check_u32(s.attempt, "attempt")).u64(kind).u64(delay)
    return w.finish()


def _read_settlement(r: Reader) -> Optional[Settlement]:
    id_ = r.u64()
    attempt = _u32(r.u64())
    kind = r.u64()
    delay = r.u64()
    if id_ is None or attempt is None or kind is None or delay is None:
        return None
    outcome: Outcome
    if kind == 0:
        outcome = "ack"
    elif kind == 1 and delay == U64_MAX:
        outcome = "retry"
    elif kind == 1:
        outcome = RetryAfter(delay)
    else:
        return None
    return Settlement(id_, attempt, outcome)


def decode_settlements(buf: bytes) -> Optional[List[Settlement]]:
    return _list(buf, _read_settlement)


def encode_counts(counts: Sequence[int]) -> bytes:
    w = Writer().u64(len(counts))
    for c in counts:
        w.u64(c)
    return w.finish()


def decode_counts(buf: bytes) -> Optional[List[int]]:
    return _list(buf, lambda r: r.u64())


def encode_bodies(bodies: Sequence[bytes]) -> bytes:
    w = Writer().u64(len(bodies))
    for b in bodies:
        w.bytes(b)
    return w.finish()


def decode_bodies(buf: bytes) -> Optional[List[bytes]]:
    return _list(buf, lambda r: r.bytes())


def encode_dead(items: Sequence[DeadLetter]) -> bytes:
    w = Writer().u64(len(items))
    for d in items:
        _write_message(w, d.message).u64(d.failed_at).bytes(d.reason.encode())
    return w.finish()


def _read_dead(r: Reader) -> Optional[DeadLetter]:
    message = _read_message(r)
    failed_at = r.u64()
    reason = r.bytes()
    if message is None or failed_at is None or reason is None:
        return None
    try:
        # Rust's `String::from_utf8`: invalid UTF-8 is malformed.
        return DeadLetter(message, failed_at, reason.decode("utf-8"))
    except UnicodeDecodeError:
        return None


def decode_dead(buf: bytes) -> Optional[List[DeadLetter]]:
    return _list(buf, _read_dead)


# ----------------------------------------------------------------- workflow

STATES = ("running", "waiting", "completed", "failed", "cancelled")
"""A workflow instance's states, as the runner names them."""


@dataclass(frozen=True)
class Status:
    """Where a workflow instance stands (the Rust SDK's `workflow::Status`)."""

    state: str
    """"running", "waiting", "completed", "failed" or "cancelled"."""
    wake_at: Optional[int] = None
    """When waiting: until when (ms since the Unix epoch), at the latest."""
    waiting_for: List[str] = field(default_factory=list)
    """When waiting for events: their names."""
    output: Optional[bytes] = None
    """When completed: the workflow's output."""
    error: Optional[str] = None
    """When failed: why."""
    positions: int = 0
    """Positions recorded so far (steps, sleeps and waits)."""

    @property
    def is_finished(self) -> bool:
        """Whether the instance has finished (completed, failed or
        cancelled)."""
        return self.state in ("completed", "failed", "cancelled")


def encode_status(s: Status) -> bytes:
    if s.state not in STATES:
        raise ValueError(f"a state is one of {', '.join(STATES)}, got {s.state!r}")
    return (
        Writer()
        .bytes(s.state.encode())
        .u64(check_u64(s.wake_at if s.wake_at is not None else 0, "wake_at"))
        .bytes("\n".join(s.waiting_for).encode())
        .u64(1 if s.output is not None else 0)
        .bytes(s.output if s.output is not None else b"")
        .bytes((s.error or "").encode())
        .u64(check_u64(s.positions, "positions"))
        .finish()
    )


def decode_status(buf: bytes) -> Optional[Status]:
    """As Rust's `decode_status`: text must be UTF-8 and the state one of
    `STATES`; bytes after the last field are ignored, as there."""
    r = Reader(buf)
    state = r.bytes()
    wake_at = r.u64()
    names = r.bytes()
    has_output = r.u64()
    output = r.bytes()
    error = r.bytes()
    positions = r.u64()
    if (
        state is None
        or wake_at is None
        or names is None
        or has_output is None
        or output is None
        or error is None
        or positions is None
    ):
        return None
    try:
        state_s = state.decode("utf-8")
        names_s = names.decode("utf-8")
        error_s = error.decode("utf-8")
    except UnicodeDecodeError:
        return None
    if state_s not in STATES:
        return None
    return Status(
        state_s,
        wake_at if wake_at != 0 else None,
        [n for n in names_s.split("\n") if n],
        output if has_output == 1 else None,
        error_s or None,
        positions,
    )
