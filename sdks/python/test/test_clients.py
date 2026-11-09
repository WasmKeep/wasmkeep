"""The kv and queue clients against fake actors: the exact requests they
send (address, method, path, headers, body) and how they read answers,
statuses and failed calls, as the Rust clients do."""

import time
import unittest

import host_stub  # noqa: F401 - before wasmkeep
from host_stub import Response, fail

from wasmkeep import kv, queue, workflow
from wasmkeep.wire import (
    DeadLetter,
    Entry,
    Message,
    RetryAfter,
    Settlement,
    Stats,
    Status as WfStatus,
    Writer,
    decode_settlements,
    encode_counts,
    encode_dead,
    encode_entry,
    encode_keys,
    encode_messages,
    encode_status,
)


def host(answer):
    """Records every call and answers with `answer(address, request)`."""
    calls = []

    def handle(address, request):
        calls.append((address, request.method, request.path, list(request.headers), bytes(request.body)))
        return answer(address, request)

    host_stub.install(handle)
    return calls


def ok(body):
    return Response(200, body)


def status(s, text=""):
    return Response(s, text.encode())


def raises(exc):
    def answer(_address, _request):
        raise exc

    return answer


def u64(v):
    return Writer().u64(v).finish()


class KvClient(unittest.TestCase):
    def assertKvError(self, f, kind, message=None, status=None):
        with self.assertRaises(kv.KvError) as cm:
            f()
        self.assertEqual(cm.exception.kind, kind)
        if message is not None:
            self.assertEqual(str(cm.exception), message)
        if status is not None:
            self.assertEqual(cm.exception.status, status)

    def test_put_put_with_and_compare_and_set_send_what_rust_sends(self):
        calls = host(lambda a, r: ok(u64(7)))
        store = kv.Client("kv", 8)
        self.assertEqual(store.put("user:42", "Ada"), 7)
        self.assertEqual(
            calls[0],
            (
                "kv/shard-2",  # shard_for("user:42", 8) in Rust
                "PUT",
                "/wasmkeep-kv/v1/k/" + b"user:42".hex(),
                [("x-wasmkeep-kv-expect", "any"), ("x-wasmkeep-kv-durable", "1")],
                b"Ada",
            ),
        )
        store.put_with("user:42", bytes([0, 1]), expect=5, ttl_ms=60_000, durable=False)
        self.assertEqual(
            calls[1][3],
            [("x-wasmkeep-kv-expect", "5"), ("x-wasmkeep-kv-durable", "0"), ("x-wasmkeep-kv-ttl-ms", "60000")],
        )
        self.assertEqual(calls[1][4], bytes([0, 1]))
        store.compare_and_set("user:42", 6, "x")
        store.compare_and_set("user:42", None, "x")
        store.put_with("user:42", "x", expect="absent")
        self.assertEqual(calls[2][3][0], ("x-wasmkeep-kv-expect", "6"))
        self.assertEqual(calls[3][3][0], ("x-wasmkeep-kv-expect", "absent"))
        self.assertEqual(calls[4][3][0], ("x-wasmkeep-kv-expect", "absent"))
        # Out-of-range numbers fail before any call.
        for bad in (dict(ttl_ms=-1), dict(expect=-1), dict(expect="maybe"), dict(expect=True)):
            with self.assertRaises(ValueError, msg=repr(bad)):
                store.put_with("k", "v", **bad)
        with self.assertRaises(ValueError):
            store.delete_if("k", 2**64)
        with self.assertRaises(ValueError):
            kv.Client("kv", 2**32)
        self.assertEqual(len(calls), 5)

    def test_answers_conflict_refusal_malformed_failed_call_long_key(self):
        store = kv.Client("kv", 4)
        host(lambda a, r: status(409, "conflict\n"))
        self.assertKvError(lambda: store.put("k", "v"), "conflict", "conflict: the key changed")
        host(lambda a, r: status(500, "storage: disk full\n"))
        self.assertKvError(lambda: store.get("k"), "refused", "shard answered 500: storage: disk full", 500)
        host(lambda a, r: ok(bytes(3)))
        self.assertKvError(lambda: store.get("k"), "refused", "shard answered 200: malformed reply from shard", 200)
        host(lambda a, r: ok(b""))
        self.assertKvError(lambda: store.put("k", "v"), "refused", status=200)
        host(lambda a, r: ok(b""))
        self.assertKvError(lambda: store.delete("k"), "refused", status=200)

        address = f"kv/shard-{kv.shard_for('k', 4)}"
        host(raises(fail("timeout")))
        self.assertKvError(lambda: store.put("k", "v"), "call", f"call to {address} timed out")
        host(raises(fail("failed", "trapped")))
        self.assertKvError(lambda: store.get("k"), "call", f"call to {address} failed: trapped")
        host(raises(fail("reentrant")))
        self.assertKvError(
            lambda: store.delete("k"),
            "call",
            f"call to {address} would re-enter an actor already in this call chain",
        )
        host(raises(fail("invalid-address", "no class kv")))
        self.assertKvError(lambda: store.get("k"), "call", f"invalid address {address}: no class kv")
        host(raises(fail("too-large")))
        self.assertKvError(lambda: store.put("k", "v"), "call", f"call to {address}: body too large")

        calls = host(lambda a, r: ok(u64(1)))
        self.assertKvError(lambda: store.put("k" * 1025, "v"), "key-too-long", "key longer than 1024 bytes")
        self.assertEqual(store.put("k" * 1024, "v"), 1, "1024 bytes is allowed")
        # The limit is on UTF-8 bytes: 513 two-byte characters are too long.
        self.assertKvError(lambda: store.get("é" * 513), "key-too-long")
        self.assertEqual(len(calls), 1)

    def test_get_decodes_an_entry_404_is_absent(self):
        calls = host(lambda a, r: ok(encode_entry(Entry(b"Ada", 3, 99))))
        store = kv.Client("kv", 1)
        self.assertEqual(store.get("user:1"), Entry(b"Ada", 3, 99))
        self.assertEqual(calls[0], ("kv/shard-0", "GET", "/wasmkeep-kv/v1/k/" + b"user:1".hex(), [], b""))
        host(lambda a, r: status(404, "no such key\n"))
        self.assertIsNone(store.get_fresh("user:1"))
        self.assertIsNone(store.get(b"user:1"))

    def test_delete_and_delete_if(self):
        calls = host(lambda a, r: ok(u64(1)))
        store = kv.Client("kv", 1)
        self.assertTrue(store.delete("k"))
        self.assertEqual(calls[0][1:], ("DELETE", "/wasmkeep-kv/v1/k/6b", [("x-wasmkeep-kv-expect", "any")], b""))
        host(lambda a, r: ok(u64(0)))
        self.assertFalse(store.delete("k"))
        cond = host(lambda a, r: status(409))
        self.assertKvError(lambda: store.delete_if("k", 4), "conflict")
        self.assertEqual(cond[0][3], [("x-wasmkeep-kv-expect", "4")])

    def test_list_asks_every_shard_and_merges_the_pages(self):
        pages = {
            "kv/shard-0": [b"user:3"],
            "kv/shard-1": [b"user:1", b"user:4"],
            "kv/shard-2": [b"user:2"],
        }
        calls = host(lambda a, r: ok(encode_keys(pages[a])))
        store = kv.Client("kv", 3)
        self.assertEqual(store.list("user:", "user:0", 3), [b"user:1", b"user:2", b"user:3"])
        path = f"/wasmkeep-kv/v1/list?prefix={b'user:'.hex()}&limit=3&after={b'user:0'.hex()}"
        self.assertEqual(
            [c[:3] for c in calls],
            [(a, "GET", path) for a in ("kv/shard-0", "kv/shard-1", "kv/shard-2")],
        )
        calls = host(lambda a, r: ok(encode_keys([])))
        self.assertEqual(store.list(), [])
        self.assertEqual(calls[0][2], "/wasmkeep-kv/v1/list?prefix=&limit=100")
        host(lambda a, r: status(409, "conflict"))
        self.assertKvError(lambda: store.list("", None, 10), "refused", status=409)  # not a conflict
        host(raises(fail("timeout")))
        self.assertKvError(lambda: store.list(), "call", "call to kv/shard-0 timed out")
        host(lambda a, r: ok(b"\0"))
        self.assertKvError(lambda: store.list(), "refused", status=200)
        with self.assertRaises(ValueError):
            store.list("", None, -1)

    def test_read_cache_serves_get_and_follows_this_clients_writes(self):
        state = {"value": b"one", "version": 1}

        def answer(_a, r):
            if r.method == "GET":
                return ok(encode_entry(Entry(state["value"], state["version"])))
            if r.method == "PUT":
                state["version"] += 1
                return ok(u64(state["version"]))
            return ok(u64(1))

        calls = host(answer)
        # A class name of its own: the cache is per instance, shared by clients.
        cached = kv.Client("cache-test", 2).with_read_cache(60_000)
        fresh = kv.Client("cache-test", 2)

        def value(e):
            return e and e.value

        self.assertEqual(value(cached.get("flag")), b"one")
        state["value"] = b"two"  # another actor's write
        self.assertEqual(value(cached.get("flag")), b"one", "served from the cache")
        self.assertEqual(value(fresh.get("flag")), b"two", "a client without a cache asks")
        self.assertEqual(value(cached.get_fresh("flag")), b"two")
        state["value"] = b"three"
        self.assertEqual(value(cached.get("flag")), b"two", "get_fresh refreshed the cache")
        self.assertEqual(len(calls), 3)

        # Its own writes go through the cache: a put, then a delete.
        cached.put("flag", "mine")
        self.assertEqual(cached.get("flag"), Entry(b"mine", state["version"]))
        cached.delete("flag")
        self.assertIsNone(cached.get("flag"), "a delete caches the absence")
        # A conflict forgets the key: the next get asks the shard.
        host(lambda a, r: status(409))
        self.assertKvError(lambda: cached.compare_and_set("flag", 1, "x"), "conflict")
        after = host(lambda a, r: ok(encode_entry(Entry(b"theirs", 9))))
        self.assertEqual(value(cached.get("flag")), b"theirs")
        self.assertEqual(len(after), 1)
        # A failed write forgets it too: it may have applied.
        host(raises(fail("timeout")))
        self.assertKvError(lambda: cached.put("flag", "lost?"), "call")
        after = host(lambda a, r: ok(encode_entry(Entry(b"applied", 10))))
        self.assertEqual(value(cached.get("flag")), b"applied")
        self.assertEqual(len(after), 1)
        # A ttl write caches its expiry.
        host(lambda a, r: ok(u64(11)))
        now = int(time.time() * 1000)
        cached.put_with("session", "s", ttl_ms=5_000)
        hit = cached.get("session")
        self.assertIsNotNone(hit.expires_at)
        self.assertTrue(now + 5_000 <= hit.expires_at <= int(time.time() * 1000) + 5_000)


class QueueClient(unittest.TestCase):
    def assertQueueError(self, f, kind, message=None, status=None):
        with self.assertRaises(queue.QueueError) as cm:
            f()
        self.assertEqual(cm.exception.kind, kind)
        if message is not None:
            self.assertEqual(str(cm.exception), message)
        if status is not None:
            self.assertEqual(cm.exception.status, status)

    def test_send_send_delayed_and_send_batch(self):
        def answer(_a, r):
            n = int.from_bytes(bytes(r.body)[:8], "big")
            return ok(encode_counts([10 + i for i in range(n)]))

        calls = host(answer)
        jobs = queue.Client("queue/jobs")
        self.assertEqual(jobs.send("resize 42"), 10)
        self.assertEqual(
            calls[0],
            (
                "queue/jobs",
                "POST",
                "/wasmkeep-queue/v1/send",
                [("x-wasmkeep-queue-delay-ms", "0")],
                Writer().u64(1).bytes(b"resize 42").finish(),
            ),
        )
        self.assertEqual(jobs.send_delayed("later", 3_600_000), 10)
        self.assertEqual(calls[1][3], [("x-wasmkeep-queue-delay-ms", "3600000")])
        self.assertEqual(jobs.send(b""), 10, "an empty body is a message")
        self.assertEqual(jobs.send_batch(["a", bytes([1]), ""]), [10, 11, 12])
        self.assertEqual(calls[3][4], Writer().u64(3).bytes(b"a").bytes(bytes([1])).bytes(b"").finish())
        with self.assertRaises(ValueError):
            jobs.send_delayed("x", -1)

        host(lambda a, r: ok(encode_counts([1])))
        self.assertQueueError(
            lambda: jobs.send_batch(["a", "b"]), "refused", "queue answered 200: malformed reply from queue"
        )
        host(lambda a, r: status(413, "more than 1000 messages\n"))
        self.assertQueueError(lambda: jobs.send("x"), "refused", "queue answered 413: more than 1000 messages", 413)
        host(raises(fail("timeout")))
        self.assertQueueError(lambda: jobs.send("x"), "call", "call to queue/jobs timed out")

    def test_lease_ack_retry_and_settle(self):
        leased = [Message(4, 1, 1000, b"hello")]

        def answer(_a, r):
            if "/lease" in r.path:
                return ok(encode_messages(leased))
            items = decode_settlements(bytes(r.body)) or []
            return ok(encode_counts([1 if s.attempt == 1 else 0 for s in items]))

        calls = host(answer)
        inbox = queue.Client("queue/inbox")
        self.assertEqual(inbox.lease(10, 30_000), leased)
        self.assertEqual(calls[0][1:], ("POST", "/wasmkeep-queue/v1/lease?max=10&visibility_ms=30000", [], b""))
        self.assertTrue(inbox.ack(leased[0]))
        self.assertEqual(calls[1][2], "/wasmkeep-queue/v1/settle")
        self.assertEqual(calls[1][4].hex(), "0000000000000001" + "0000000000000004" + "0000000000000001" + "00" * 16)
        self.assertFalse(inbox.ack(Message(4, 2, 1000, b"hello")), "a stale lease")
        self.assertTrue(inbox.retry(leased[0]))
        self.assertEqual(decode_settlements(calls[3][4]), [Settlement(4, 1, "retry")])
        inbox.retry(leased[0], 250)
        self.assertEqual(decode_settlements(calls[4][4]), [Settlement(4, 1, RetryAfter(250))])
        self.assertEqual(inbox.settle([Settlement(1, 1, "ack"), Settlement(2, 2, "retry")]), [True, False])
        with self.assertRaises(ValueError):
            inbox.lease(2**32, 1)
        host(lambda a, r: ok(encode_counts([])))
        self.assertFalse(inbox.ack(leased[0]), "no flag back")

    def test_stats_and_dead_letters(self):
        dead = [DeadLetter(Message(2, 3, 5, b"fail"), 9, "attempts exhausted")]
        calls = host(lambda a, r: ok(encode_counts([1, 2, 3, 4])) if r.path.endswith("/stats") else ok(encode_dead(dead)))
        q = queue.Client("queue/jobs")
        self.assertEqual(q.stats(), Stats(ready=1, delayed=2, in_flight=3, dead=4))
        self.assertEqual(q.dead_letters(100), dead)
        self.assertEqual(
            [c[1:3] for c in calls],
            [("GET", "/wasmkeep-queue/v1/stats"), ("GET", "/wasmkeep-queue/v1/dead?limit=100")],
        )
        host(lambda a, r: ok(encode_counts([1, 2, 3])))
        self.assertQueueError(q.stats, "refused", status=200)


class FakeRequest:
    """What `wasmkeep.Request` offers: method, path (with the query),
    headers and the body."""

    def __init__(self, method, path, headers, body):
        self.method = method
        self.path = path
        self.headers = headers
        self._body = body

    def bytes(self):
        return self._body


class Delivery(unittest.TestCase):
    def test_handle_delivery_answers_one_outcome_per_message(self):
        batch = [Message(1, 1, 10, b"resize 42"), Message(2, 3, 11, b"fail")]
        deliver = FakeRequest(
            "POST", queue.DELIVER_PATH, {"x-wasmkeep-caller": "default/queue/jobs"}, encode_messages(batch)
        )
        self.assertTrue(queue.is_delivery(deliver))
        self.assertFalse(queue.is_delivery(FakeRequest("GET", queue.DELIVER_PATH, {}, b"")))
        self.assertFalse(queue.is_delivery(FakeRequest("POST", queue.DELIVER_PATH + "?x=1", {}, b"")))
        seen = []

        def handle(source, m):
            seen.append(f"{source} {m.id}")
            return "retry" if m.body.startswith(b"fail") else "ack"

        r = queue.handle_delivery(deliver, handle)
        self.assertEqual(seen, ["default/queue/jobs 1", "default/queue/jobs 2"])
        self.assertEqual(r.status, 200)
        self.assertEqual(r.headers, {"content-type": "application/octet-stream"})
        self.assertEqual(decode_settlements(r.body), [Settlement(1, 1, "ack"), Settlement(2, 3, "retry")])
        delayed = queue.handle_delivery(deliver, lambda _q, m: queue.RetryAfter(500))
        self.assertEqual(decode_settlements(delayed.body)[0], Settlement(1, 1, RetryAfter(500)))

        # The caller header is looked up case-insensitively.
        mixed = FakeRequest("POST", queue.DELIVER_PATH, {"X-WasmKeep-Caller": "default/queue/q"}, encode_messages([]))
        self.assertEqual(queue.handle_delivery(mixed, lambda q, m: "ack").status, 200)
        outside = queue.handle_delivery(FakeRequest("POST", queue.DELIVER_PATH, {}, encode_messages(batch)), handle)
        self.assertEqual(
            (outside.status, outside.headers, outside.body),
            (403, {"content-type": "text/plain"}, "deliveries come from queue actors\n"),
        )
        garbage = FakeRequest("POST", queue.DELIVER_PATH, {"x-wasmkeep-caller": "q"}, bytes([1, 2, 3]))
        bad = queue.handle_delivery(garbage, handle)
        self.assertEqual(
            (bad.status, bad.headers, bad.body),
            (400, {"content-type": "text/plain"}, "malformed delivery\n"),
        )
        # A raising handler propagates (fetch answers 500).
        with self.assertRaises(RuntimeError):
            queue.handle_delivery(deliver, lambda q, m: (_ for _ in ()).throw(RuntimeError("boom")))
        # An outcome that is not one fails the whole answer.
        with self.assertRaises(ValueError):
            queue.handle_delivery(deliver, lambda q, m: "nack")


class WorkflowClient(unittest.TestCase):
    WAITING = WfStatus("waiting", 42, ["approved"], None, None, 3)

    def assertWorkflowError(self, f, kind, message=None, status=None):
        with self.assertRaises(workflow.WorkflowError) as cm:
            f()
        self.assertEqual(cm.exception.kind, kind)
        if message is not None:
            self.assertEqual(str(cm.exception), message)
        if status is not None:
            self.assertEqual(cm.exception.status, status)

    def test_each_operation_sends_what_rust_sends(self):
        calls = host(lambda a, r: ok(encode_status(self.WAITING)))
        orders = workflow.Client("orders")
        self.assertEqual(orders.start("o1", "item=book"), self.WAITING)
        self.assertEqual(orders.status("o1"), self.WAITING)
        self.assertEqual(orders.cancel("o1"), self.WAITING)
        orders.start("o2", bytes([0, 1]))
        host_calls = calls[:]
        calls = host(lambda a, r: ok(b""))
        self.assertIsNone(orders.send_event("o1", "approved", b"yes"))
        orders.send_event("o1", "été", "")
        self.assertEqual(
            host_calls,
            [
                ("orders/o1", "POST", "/wasmkeep-workflow/v1/start", [], b"item=book"),
                ("orders/o1", "GET", "/wasmkeep-workflow/v1/status", [], b""),
                ("orders/o1", "POST", "/wasmkeep-workflow/v1/cancel", [], b""),
                ("orders/o2", "POST", "/wasmkeep-workflow/v1/start", [], bytes([0, 1])),
            ],
        )
        self.assertEqual(
            calls,
            [
                # The name as lowercase hex of its UTF-8 bytes.
                ("orders/o1", "POST", "/wasmkeep-workflow/v1/event?name=617070726f766564", [], b"yes"),
                ("orders/o1", "POST", "/wasmkeep-workflow/v1/event?name=c3a974c3a9", [], b""),
            ],
        )

    def test_unknown_finished_refused_malformed_and_failed_calls(self):
        orders = workflow.Client("orders")
        host(lambda a, r: status(404, "no such workflow instance\n"))
        for op in (lambda: orders.status("x"), lambda: orders.cancel("x"), lambda: orders.send_event("x", "e")):
            self.assertWorkflowError(op, "unknown", "no such workflow instance")
        host(lambda a, r: status(409, "finished\n"))
        self.assertWorkflowError(
            lambda: orders.send_event("o1", "approved"), "finished", "the workflow instance has finished"
        )
        host(lambda a, r: status(403, "workflows are called by actors\n"))
        self.assertWorkflowError(
            lambda: orders.start("o1"), "refused", "refused (403): workflows are called by actors", 403
        )
        host(lambda a, r: ok(b"\0\0"))
        self.assertWorkflowError(lambda: orders.status("o1"), "refused", "refused (200): malformed status", 200)
        # A send_event answer's body is not read.
        host(lambda a, r: ok(b"\0\0"))
        orders.send_event("o1", "approved")

        host(raises(fail("timeout")))
        self.assertWorkflowError(lambda: orders.start("o1"), "call", "call to orders/o1 timed out")
        host(raises(fail("failed", "trapped")))
        self.assertWorkflowError(lambda: orders.send_event("o1", "e"), "call", "call to orders/o1 failed: trapped")
        host(raises(fail("invalid-address", "no class orders")))
        self.assertWorkflowError(lambda: orders.status("o1"), "call", "invalid address orders/o1: no class orders")


if __name__ == "__main__":
    unittest.main()
