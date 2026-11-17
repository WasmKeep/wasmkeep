"""Headers are ordered (name, value) pairs: names compare without case,
`get` is the first value, `get_all` every value in order, `append` adds,
`set` replaces all, and Set-Cookie is never joined with a comma."""

import unittest

import host_stub  # noqa: F401 - before wasmkeep
from host_stub import Response, install

import wasmkeep
from wasmkeep import actors
from wasmkeep.headers import Headers, response_pairs

COOKIE_A = "a=1; Expires=Wed, 21 Oct 2026 07:28:00 GMT"
COOKIES = [("set-cookie", COOKIE_A), ("x-other", "o"), ("Set-Cookie", "b=2")]


class FakeMethod:
    pass


FakeMethod.__name__ = "Method_Get"


class FakeFields:
    def __init__(self, entries):
        self._entries = entries

    def entries(self):
        return self._entries


class FakeIncoming:
    def __init__(self, entries):
        self._fields = FakeFields([(k, v.encode()) for k, v in entries])

    def method(self):
        return FakeMethod()

    def path_with_query(self):
        return "/x"

    def headers(self):
        return self._fields


class HeaderList(unittest.TestCase):
    def test_get_is_first_and_get_all_every_value_in_order_whatever_the_case(self):
        h = Headers(COOKIES)
        self.assertEqual(h.get("SET-COOKIE"), COOKIE_A)
        self.assertEqual(h.get_all("set-Cookie"), [COOKIE_A, "b=2"])
        self.assertIsNone(h.get("missing"))
        self.assertEqual(h.get_all("missing"), [])
        self.assertTrue(h.has("X-OTHER"))
        self.assertEqual(h.items(), COOKIES)
        self.assertEqual(h.getAll("set-cookie"), [COOKIE_A, "b=2"])

    def test_append_adds_set_replaces_all_delete_removes_all(self):
        h = Headers([("a", "1")])
        h.append("A", "2")
        h.append("b", "x")
        self.assertEqual(h.get_all("a"), ["1", "2"])
        h.set("a", "3")
        self.assertEqual(h.items(), [("a", "3"), ("b", "x")])
        h.set("c", "y")
        self.assertEqual(h.get_all("c"), ["y"])
        h.delete("B")
        self.assertFalse(h.has("b"))

    def test_a_dict_with_list_values_is_one_pair_per_value(self):
        self.assertEqual(
            Headers({"set-cookie": ["a=1", "b=2"], "x": "y"}).items(),
            [("set-cookie", "a=1"), ("set-cookie", "b=2"), ("x", "y")],
        )

    def test_a_response_keeps_repeats_and_adds_no_second_content_type(self):
        self.assertEqual(
            response_pairs({"Content-Type": "application/json", "Set-Cookie": ["a=1", "b=2"]}),
            [("content-type", "application/json"), ("set-cookie", "a=1"), ("set-cookie", "b=2")],
        )
        self.assertEqual(response_pairs(None), [("content-type", "text/plain; charset=utf-8")])
        self.assertEqual(
            response_pairs([("Set-Cookie", "a=1"), ("set-cookie", "b=2")]),
            [("content-type", "text/plain; charset=utf-8"), ("set-cookie", "a=1"), ("set-cookie", "b=2")],
        )


class Hosted(unittest.TestCase):
    def test_a_request_keeps_every_header_in_order(self):
        r = wasmkeep.Request(FakeIncoming(COOKIES + [("Cookie", "x=1"), ("cookie", "y=2")]))
        self.assertEqual(r.header_list, COOKIES + [("Cookie", "x=1"), ("cookie", "y=2")])
        self.assertEqual(r.fields.get_all("COOKIE"), ["x=1", "y=2"])
        self.assertEqual(r.fields.get_all("set-cookie"), [COOKIE_A, "b=2"])
        # The dict is as it was: the last value.
        self.assertEqual(r.headers["Set-Cookie"], "b=2")

    def test_a_call_sends_every_header_and_its_response_keeps_every_header(self):
        sent = []

        def answer(address, request):
            sent.append(request.headers)
            return Response(200, b"", COOKIES)

        install(answer)
        r = actors.call("t/x", headers={"Set-Cookie": ["a=1", "b=2"], "X-One": "1"})
        self.assertEqual(sent[-1], [("Set-Cookie", "a=1"), ("Set-Cookie", "b=2"), ("X-One", "1")])
        self.assertEqual(r.fields.get_all("set-cookie"), [COOKIE_A, "b=2"])
        self.assertEqual(r.header_list, COOKIES)
        self.assertEqual(r.headers["Set-Cookie"], "b=2")
        actors.call("t/x", headers=[("a", "1"), ("a", "2")])
        self.assertEqual(sent[-1], [("a", "1"), ("a", "2")])

    def test_a_response_writes_every_header(self):
        r = wasmkeep.Response("ok", headers={"Content-Type": "a/b"}, header_list=[("Set-Cookie", "a=1"), ("set-cookie", "b=2")])
        self.assertEqual(
            wasmkeep._response_fields(r),
            [("content-type", b"a/b"), ("set-cookie", b"a=1"), ("set-cookie", b"b=2")],
        )


if __name__ == "__main__":
    unittest.main()
