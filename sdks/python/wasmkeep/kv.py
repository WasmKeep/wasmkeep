"""Key-value storage built from actors (ADR 0015): the client side.

Keys are spread over a fixed number of shard actors of one class
(`<class>/shard-0` .. `shard-<n-1>`), chosen by FNV-1a 64 of the key modulo
`n`. Each shard keeps its keys in its own SQLite database, so every
operation on a key is strongly consistent. The shard class is the Rust SDK's
ready-made shard, deployed from `examples/jobs/kv`; this client speaks its
protocol (`/wasmkeep-kv/v1`) over `actors.call`.

```python
from wasmkeep import kv

store = kv.Client("kv", 8)
version = store.put("user:42", "Ada")
entry = store.get("user:42")                    # Entry(value, version, expires_at) or None
store.compare_and_set("user:42", version, "Ada L.")  # KvError kind "conflict" if it moved
```
"""

import time
from typing import Dict, List, Optional

from ._call import CallFailed, call
from .wire import (
    MAX_KEY_LEN,
    U64_MAX,
    Data,
    Entry,
    Expect,
    ReadCache,
    Reader,
    allows,
    check_u32,
    check_u64,
    decode_entry,
    decode_keys,
    encode_expect,
    hex,
    lossy,
    merge_pages,
    shard_for,
    shard_name,
    to_bytes,
)

__all__ = [
    "MAX_KEY_LEN",
    "Client",
    "Entry",
    "Expect",
    "KvError",
    "allows",
    "shard_for",
    "shard_name",
]

_PREFIX = "/wasmkeep-kv/v1"
_EXPECT = "x-wasmkeep-kv-expect"
_TTL = "x-wasmkeep-kv-ttl-ms"
_DURABLE = "x-wasmkeep-kv-durable"


class KvError(Exception):
    """Why a KV operation failed, as `kind`:

    - "conflict": a conditional write found another version (or a key where
      it expected none);
    - "key-too-long": the key is longer than `MAX_KEY_LEN` bytes;
    - "storage": the shard's database refused the operation;
    - "call": the call to the shard failed. The outcome of a write is then
      not known (it may have applied): re-read before retrying a conditional
      write;
    - "refused": the shard answered with an unexpected `status`.
    """

    def __init__(self, kind: str, detail: str = "", status: Optional[int] = None):
        if kind == "conflict":
            message = "conflict: the key changed"
        elif kind == "key-too-long":
            message = f"key longer than {MAX_KEY_LEN} bytes"
        elif kind == "storage":
            message = f"storage: {detail}"
        elif kind == "call":
            message = detail
        else:
            message = f"shard answered {status}: {detail}"
        super().__init__(message)
        self.kind = kind
        self.detail = detail
        """The detail: the call's error, or the shard's answer."""
        self.status = status
        """The shard's status, for "refused"."""


def _bad_reply() -> KvError:
    return KvError("refused", "malformed reply from shard", 200)


def _refused(status: int, body: bytes) -> KvError:
    return KvError("refused", lossy(body), status)


def _check_key(key: bytes) -> None:
    if len(key) > MAX_KEY_LEN:
        raise KvError("key-too-long")


def _now_ms() -> int:
    return int(time.time() * 1000)


# One per instance: lost with it (hibernation, a failed event, a deploy).
_CACHE = ReadCache(4096)


class Client:
    """Reaches the shards of one class. Cheap to create; keep one per
    class."""

    def __init__(self, class_name: str, shards: int):
        """Keys spread over `shards` actors of `class_name` (at least one).
        Every caller must use the same count."""
        self.class_name = class_name
        self.shards = max(1, check_u32(shards, "shards"))
        self._cache_ms = 0

    def with_read_cache(self, max_age_ms: int) -> "Client":
        """Serve `get` from an in-memory cache in this actor's instance when
        the entry was read less than `max_age_ms` ago. This actor's own
        writes through the client update the cache, so it reads its own
        writes; other actors' writes show up within `max_age_ms`. The cache
        is lost with the instance (hibernation, a failed event, a deploy).
        Returns this client."""
        self._cache_ms = check_u64(max_age_ms, "max_age_ms")
        return self

    def _address(self, key: bytes) -> str:
        return f"{self.class_name}/{shard_name(shard_for(key, self.shards))}"

    def _cache_key(self, key: bytes) -> bytes:
        return self.class_name.encode() + b"\0" + key

    def _remember(self, key: bytes, entry: Optional[Entry]) -> None:
        if self._cache_ms > 0:
            _CACHE.insert(self._cache_key(key), _now_ms(), entry)

    def _forget(self, key: bytes) -> None:
        if self._cache_ms > 0:
            _CACHE.remove(self._cache_key(key))

    def _call(self, key: bytes, method: str, headers: Dict[str, str], body: bytes):
        try:
            return call(self._address(key), method, f"{_PREFIX}/k/{hex(key)}", headers, body)
        except CallFailed as e:
            if method != "GET":
                self._forget(key)
            raise KvError("call", str(e)) from None

    def get(self, key: Data) -> Optional[Entry]:
        """The entry at `key` (None if absent or expired), from the read
        cache if one is set and fresh."""
        k = to_bytes(key)
        if self._cache_ms > 0:
            hit, entry = _CACHE.get(self._cache_key(k), _now_ms(), self._cache_ms)
            if hit:
                return entry
        return self.get_fresh(k)

    def get_fresh(self, key: Data) -> Optional[Entry]:
        """The entry at `key`, always asking the shard."""
        k = to_bytes(key)
        _check_key(k)
        status, body = self._call(k, "GET", {}, b"")
        entry: Optional[Entry] = None
        if status == 200:
            entry = decode_entry(body)
            if entry is None:
                raise _bad_reply()
        elif status != 404:
            raise _refused(status, body)
        self._remember(k, entry)
        return entry

    def put(self, key: Data, value: Data) -> int:
        """Stores `value` at `key` (durably); returns the new version."""
        return self.put_with(key, value)

    def compare_and_set(self, key: Data, expected: Optional[int], value: Data) -> int:
        """Stores `value` only if `key` is at version `expected` (None:
        absent); `KvError` "conflict" otherwise."""
        return self.put_with(key, value, expect="absent" if expected is None else expected)

    def put_with(
        self,
        key: Data,
        value: Data,
        expect: Expect = "any",
        ttl_ms: Optional[int] = None,
        durable: bool = True,
    ) -> int:
        """Stores `value` at `key`; returns the new version.

        - `expect`: the condition the write needs: "any" (none, the
          default), "absent", or a version;
        - `ttl_ms`: expire the key this many milliseconds after the write;
        - `durable`: return only once the write is durable (in the bucket in
          a cluster, fsynced on one node). False acknowledges once committed
          on the shard's node: faster, but a node lost in that window loses
          it.
        """
        k = to_bytes(key)
        v = to_bytes(value)
        _check_key(k)
        headers = {_EXPECT: encode_expect(expect), _DURABLE: "1" if durable else "0"}
        if ttl_ms is not None:
            headers[_TTL] = str(check_u64(ttl_ms, "ttl_ms"))
        sent = _now_ms()
        status, body = self._call(k, "PUT", headers, v)
        if status == 200:
            version = Reader(body).u64()
            if version is None:
                raise _bad_reply()
            expires_at = None if ttl_ms is None else min(sent + ttl_ms, U64_MAX)
            self._remember(k, Entry(v, version, expires_at))
            return version
        self._forget(k)
        if status == 409:
            raise KvError("conflict")
        raise _refused(status, body)

    def delete(self, key: Data) -> bool:
        """Removes `key`; True if it was there."""
        return self._delete_with(key, "any")

    def delete_if(self, key: Data, version: int) -> bool:
        """Removes `key` only if it holds `version`; `KvError` "conflict"
        otherwise."""
        return self._delete_with(key, check_u64(version, "version"))

    def _delete_with(self, key: Data, expect: Expect) -> bool:
        k = to_bytes(key)
        _check_key(k)
        status, body = self._call(k, "DELETE", {_EXPECT: encode_expect(expect)}, b"")
        if status == 200:
            removed = Reader(body).u64()
            if removed is None:
                raise _bad_reply()
            self._remember(k, None)
            return removed == 1
        self._forget(k)
        if status == 409:
            raise KvError("conflict")
        raise _refused(status, body)

    def list(self, prefix: Data = b"", after: Optional[Data] = None, limit: int = 100) -> List[bytes]:
        """Up to `limit` keys starting with `prefix`, in byte order, after
        `after` (exclusive) if given: pass the last key of a page to get the
        next one. Asks every shard (one call each), so it costs more than a
        read; keep it for listings, not hot paths."""
        check_u32(limit, "limit")
        path = f"{_PREFIX}/list?prefix={hex(to_bytes(prefix))}&limit={limit}"
        if after is not None:
            path += f"&after={hex(to_bytes(after))}"
        pages = []
        for index in range(self.shards):
            address = f"{self.class_name}/{shard_name(index)}"
            try:
                status, body = call(address, "GET", path, {}, b"")
            except CallFailed as e:
                raise KvError("call", str(e)) from None
            if status != 200:
                raise _refused(status, body)
            keys = decode_keys(body)
            if keys is None:
                raise _bad_reply()
            pages.append(keys)
        return merge_pages(pages, limit)
