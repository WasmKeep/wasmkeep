"""The framing, codecs, shard hash and read cache: the Rust SDK's unit tests
(wire.rs, kv.rs, queue.rs), plus golden bytes and shard choices computed
from the Rust code (the same vectors as the TypeScript SDK's wire tests).

`wasmkeep/wire.py` is loaded on its own, without the package or any stub: it
must not need the host bindings."""

import importlib.util
import os
import sys
import unittest

_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "wasmkeep", "wire.py")
_spec = importlib.util.spec_from_file_location("wasmkeep_wire_alone", _PATH)
wire = importlib.util.module_from_spec(_spec)
sys.modules["wasmkeep_wire_alone"] = wire  # dataclasses look the module up
_spec.loader.exec_module(wire)

Entry = wire.Entry
Message = wire.Message
Settlement = wire.Settlement
DeadLetter = wire.DeadLetter
RetryAfter = wire.RetryAfter
Writer = wire.Writer
Reader = wire.Reader
U64_MAX = wire.U64_MAX


class Framing(unittest.TestCase):
    def test_round_trips_and_rejects_truncation(self):
        buf = Writer().u64(7).bytes(b"abc").bytes(b"").u64(U64_MAX).finish()
        r = Reader(buf)
        self.assertEqual(r.u64(), 7)
        self.assertEqual(r.bytes(), b"abc")
        self.assertEqual(r.bytes(), b"")
        self.assertEqual(r.u64(), U64_MAX)
        self.assertTrue(r.is_empty())
        self.assertIsNone(r.u64())
        # A length that runs past the end is refused, not truncated.
        self.assertIsNone(Reader(buf[8:12]).bytes())

    def test_big_endian_as_rust_writes_it(self):
        buf = Writer().u64(7).bytes(b"abc").u64(0x0102030405060708).finish()
        self.assertEqual(buf.hex(), "0000000000000007" + "00000003616263" + "0102030405060708")
        # Out-of-range integers are refused rather than wrapped.
        for bad in (-1, U64_MAX + 1, True, 1.0, "1"):
            with self.assertRaises(ValueError, msg=repr(bad)):
                Writer().u64(bad)
        # Python ints are exact: u64::MAX round-trips (no 2^53 limit).
        self.assertEqual(Reader(Writer().u64(2**53 + 1).finish()).u64(), 2**53 + 1)

    def test_hex_is_lowercase_two_digits_per_byte(self):
        every = bytes(range(256))
        self.assertEqual(len(wire.hex(every)), 512)
        self.assertEqual(bytes.fromhex(wire.hex(every)), every)
        self.assertEqual(wire.hex(bytes([0, 255, 0x41])), "00ff41")
        self.assertEqual(wire.hex(b""), "")

    def test_ranges(self):
        self.assertEqual(wire.check_u32(0xFFFF_FFFF), 0xFFFF_FFFF)
        for bad in (-1, 0x1_0000_0000, False, 2.0):
            with self.assertRaises(ValueError):
                wire.check_u32(bad)
        with self.assertRaises(TypeError):
            wire.to_bytes(42)


class ShardHash(unittest.TestCase):
    def test_fnv1a_64_matches_the_rust_sdk(self):
        # Known FNV-1a 64 values: "" and "a" (kv.rs).
        self.assertEqual(wire.shard_for("", 0xFFFF_FFFF), 0xCBF2_9CE4_8422_2325 % 0xFFFF_FFFF)
        self.assertEqual(wire.shard_for("a", 1 << 16), 0xAF63_DC4C_8601_EC8C % (1 << 16))
        self.assertEqual(wire.shard_for("anything", 1), 0)
        self.assertEqual(wire.shard_for("anything", 0), 0, "zero shards means one")
        # Computed with the Rust `shard_for`: [key, n=4, n=8, n=1000, n=u32::MAX].
        vectors = [
            ("", 1, 5, 37, 1343537162),
            ("a", 0, 4, 996, 895863001),
            ("user:42", 2, 2, 410, 1223114855),
            ("user:0", 0, 0, 736, 1829090531),
            ("user:1", 3, 3, 947, 1829091222),
            ("user:2", 2, 6, 158, 1829091913),
            ("job:1", 1, 1, 481, 310798266),
            ("greeting", 2, 6, 198, 2089897798),
            ("clé é", 2, 6, 550, 819157025),
            (bytes([0, 255, 128]), 0, 0, 392, 1085081017),
        ]
        for key, n4, n8, n1000, top in vectors:
            self.assertEqual(
                [wire.shard_for(key, n) for n in (4, 8, 1000, 0xFFFF_FFFF)],
                [n4, n8, n1000, top],
                f"key {key!r}",
            )
        # A str key hashes as its UTF-8 bytes.
        self.assertEqual(wire.shard_for("clé é", 1000), wire.shard_for("clé é".encode(), 1000))
        # Spread: 1,000 keys over 8 shards, the exact counts Rust gets.
        seen = [0] * 8
        for i in range(1000):
            seen[wire.shard_for(f"user:{i}", 8)] += 1
        self.assertEqual(seen, [126, 125, 125, 126, 124, 125, 125, 124])
        with self.assertRaises(ValueError):
            wire.shard_for("k", -1)
        self.assertEqual(wire.shard_name(3), "shard-3")


class Kv(unittest.TestCase):
    def test_expectations(self):
        self.assertTrue(wire.allows("any", None))
        self.assertTrue(wire.allows("any", 3))
        self.assertTrue(wire.allows("absent", None))
        self.assertFalse(wire.allows("absent", 3))
        self.assertTrue(wire.allows(3, 3))
        self.assertFalse(wire.allows(3, 4))
        self.assertFalse(wire.allows(3, None))
        for e in ("any", "absent", 0, U64_MAX):
            self.assertEqual(wire.decode_expect(wire.encode_expect(e)), e)
        self.assertEqual(wire.encode_expect(42), "42")
        for bad in ("nope", "-1", " 1", "1_0", "١", str(U64_MAX + 1), ""):
            self.assertIsNone(wire.decode_expect(bad), repr(bad))
        self.assertEqual(wire.decode_expect("+7"), 7)
        for bad in (-1, U64_MAX + 1, True, "maybe", None):
            with self.assertRaises(ValueError):
                wire.encode_expect(bad)

    def test_entries_and_key_lists_round_trip(self):
        e = Entry(b"v", 9, 1234)
        self.assertEqual(wire.decode_entry(wire.encode_entry(e)), e)
        self.assertEqual(wire.encode_entry(e).hex(), "0000000000000009" + "00000000000004d2" + "0000000176")
        forever = Entry(b"v", 9)
        self.assertEqual(wire.decode_entry(wire.encode_entry(forever)), forever)
        # Trailing bytes are refused.
        self.assertIsNone(wire.decode_entry(wire.encode_entry(e) + b"\0"))
        keys = [b"a", b"", bytes([0, 255])]
        self.assertEqual(wire.decode_keys(wire.encode_keys(keys)), keys)
        self.assertIsNone(wire.decode_keys(bytes([0, 0, 0, 0, 0, 0, 0, 1])))
        # The largest version is exact.
        big = Entry(b"v", U64_MAX, U64_MAX)
        self.assertEqual(wire.decode_entry(wire.encode_entry(big)), big)

    def test_pages_merge_in_byte_order(self):
        pages = [[b"a", b"d"], [b"b", b"c", b"e"], []]
        self.assertEqual(wire.merge_pages(pages, 4), [b"a", b"b", b"c", b"d"])
        # Byte order: "é" (c3 a9) sorts after "z" (7a), a prefix before its
        # extensions.
        self.assertEqual(
            wire.merge_pages([["é".encode()], [b"z", b"za"], [b""]], 10),
            [b"", b"z", b"za", "é".encode()],
        )

    def test_read_cache_ages_expires_and_stays_bounded(self):
        c = wire.ReadCache(2)
        c.insert(b"k", 100, Entry(b"", 1))
        self.assertEqual(c.get(b"k", 150, 100), (True, Entry(b"", 1)))
        self.assertEqual(c.get(b"k", 200, 100), (False, None), "too old")
        c.insert(b"gone", 100, None)
        self.assertEqual(c.get(b"gone", 120, 100), (True, None), "absence is cached")
        self.assertEqual(c.get(b"other", 120, 100), (False, None), "a miss is not an absence")

        # An entry that expired while cached reads as absent.
        c.insert(b"k", 100, Entry(b"", 2, 130))
        self.assertEqual(c.get(b"k", 129, 100), (True, Entry(b"", 2, 130)))
        self.assertEqual(c.get(b"k", 130, 100), (True, None))

        # Full: expired entries are dropped first, then everything.
        c.insert(b"third", 140, Entry(b"", 3))
        self.assertLessEqual(len(c.entries), 2)
        self.assertIn(b"gone", c.entries, "only the expired entry went")
        self.assertEqual(c.get(b"third", 141, 100), (True, Entry(b"", 3)))
        c.insert(b"fourth", 142, Entry(b"", 4))
        self.assertEqual(list(c.entries), [b"fourth"], "nothing expired: everything goes")
        c.remove(b"fourth")
        self.assertEqual(c.get(b"fourth", 143, 100), (False, None))
        c.remove(b"never there")
        # A clock that went backwards still counts as fresh.
        c.insert(b"k", 500, Entry(b"", 5))
        self.assertEqual(c.get(b"k", 400, 100), (True, Entry(b"", 5)))


class Queue(unittest.TestCase):
    def test_frames_round_trip(self):
        messages = [Message(1, 2, 3, b"x"), Message(U64_MAX, 0xFFFF_FFFF, 0, b"")]
        self.assertEqual(wire.decode_messages(wire.encode_messages(messages)), messages)
        items = [Settlement(1, 1, "ack"), Settlement(2, 3, "retry"), Settlement(3, 1, RetryAfter(50))]
        self.assertEqual(wire.decode_settlements(wire.encode_settlements(items)), items)
        self.assertEqual(wire.decode_counts(wire.encode_counts([4, 5])), [4, 5])
        self.assertEqual(wire.decode_bodies(wire.encode_bodies([b"a", b""])), [b"a", b""])
        dead = [DeadLetter(messages[0], 9, "attempts exhausted")]
        self.assertEqual(wire.decode_dead(wire.encode_dead(dead)), dead)

    def test_settlements_encode_as_rust_does(self):
        def one(outcome):
            return wire.encode_settlements([Settlement(5, 2, outcome)]).hex()

        head = "0000000000000001" + "0000000000000005" + "0000000000000002"
        self.assertEqual(one("ack"), head + "0000000000000000" + "0000000000000000")
        # u64::MAX in the delay slot means the queue's backoff.
        self.assertEqual(one("retry"), head + "0000000000000001" + "ffffffffffffffff")
        self.assertEqual(one(RetryAfter(50)), head + "0000000000000001" + "0000000000000032")
        for bad in ("nack", None, 50):
            with self.assertRaises(ValueError):
                one(bad)
        with self.assertRaises(ValueError):
            wire.encode_settlements([Settlement(1, 0x1_0000_0000, "ack")])
        with self.assertRaises(ValueError):
            one(RetryAfter(-1))

    def test_malformed_frames_are_refused(self):
        # A count promising more than the buffer holds.
        self.assertIsNone(wire.decode_messages(b""))
        one = bytearray(wire.encode_counts([7]))
        one[7] = 2  # says two counts, holds one
        self.assertIsNone(wire.decode_counts(bytes(one)))
        # Trailing bytes are refused too.
        self.assertIsNone(wire.decode_counts(wire.encode_counts([1]) + b"\0"))
        # An unknown outcome kind.
        self.assertIsNone(wire.decode_settlements(Writer().u64(1).u64(1).u64(1).u64(9).u64(0).finish()))
        # An attempt past u32::MAX (Rust: `u32::try_from` fails).
        attempt = Writer().u64(1).u64(1).u64(0x1_0000_0000).u64(0).bytes(b"x").finish()
        self.assertIsNone(wire.decode_messages(attempt))
        # A dead letter's reason must be UTF-8 (Rust: `String::from_utf8`).
        reason = Writer().u64(1).u64(1).u64(1).u64(0).bytes(b"x").u64(9).bytes(b"\xff").finish()
        self.assertIsNone(wire.decode_dead(reason))


Status = wire.Status

GOLDEN_STATUS = (
    "0000000777616974696e67000000000000002a0000000d617070726f7665640a70616964"
    "000000000000000000000000000000000000000000000003"
)
"""{waiting, wake_at 42, waiting_for [approved, paid], no output, no error,
positions 3}: the same bytes as the Rust and TypeScript workflow tests."""


class WorkflowStatus(unittest.TestCase):
    def test_golden_bytes(self):
        waiting = Status("waiting", 42, ["approved", "paid"], None, None, 3)
        self.assertEqual(wire.encode_status(waiting).hex(), GOLDEN_STATUS)
        self.assertEqual(wire.decode_status(bytes.fromhex(GOLDEN_STATUS)), waiting)
        self.assertFalse(waiting.is_finished)

    def test_round_trips_every_state(self):
        for status in (
            Status("running"),
            Status("waiting", None, ["approved"], None, None, 1),
            Status("completed", None, [], b"shipped \xff", None, 6),
            Status("completed", None, [], b"", None, 2),  # an empty output is an output
            Status("failed", None, [], None, "not approved in time", 3),
            Status("cancelled", None, [], None, None, 4),
        ):
            self.assertEqual(wire.decode_status(wire.encode_status(status)), status, status)
            self.assertEqual(status.is_finished, status.state in ("completed", "failed", "cancelled"))
        # No output reads as None, not b"".
        self.assertIsNone(wire.decode_status(wire.encode_status(Status("running"))).output)

    def test_malformed_status_is_refused(self):
        golden = bytes.fromhex(GOLDEN_STATUS)
        for n in range(len(golden)):
            self.assertIsNone(wire.decode_status(golden[:n]), f"truncated at {n}")
        # Bytes after the last field are ignored, as in Rust.
        self.assertIsNotNone(wire.decode_status(golden + b"\0"))

        def frame(state=b"running", names=b"", has_output=0, error=b""):
            return (
                Writer().bytes(state).u64(0).bytes(names).u64(has_output).bytes(b"").bytes(error).u64(0).finish()
            )

        self.assertIsNone(wire.decode_status(frame(state=b"paused")))
        self.assertIsNone(wire.decode_status(frame(state=b"\xff")))
        self.assertIsNone(wire.decode_status(frame(names=b"\xff")))
        self.assertIsNone(wire.decode_status(frame(error=b"\xff")))
        # Only 1 means an output (Rust: `== 1`).
        self.assertIsNone(wire.decode_status(frame(has_output=2)).output)
        with self.assertRaises(ValueError):
            wire.encode_status(Status("paused"))


if __name__ == "__main__":
    unittest.main()
