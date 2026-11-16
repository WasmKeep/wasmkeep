// The typed client bound to `actors.call` / `actors.callMany` (the fake
// host): the real adapter keeps the host's addressing, chain errors and
// batch behaviour.

import { test } from "node:test";
import assert from "node:assert/strict";
import { install } from "./host-stub";
import * as codec from "../src/codec";
import { defineOperation, urlPath } from "../src/contracts";
import { actorClient } from "../src/transport";

const enc = new TextEncoder();
const hello = defineOperation({
  name: "hello",
  path: (i: { who: string }) => urlPath`/hello/${i.who}`,
  input: codec.object({ who: codec.string() }),
  output: codec.object({ greeting: codec.string() }),
});

test("call and batch go through the actor transport, in order, with host errors as values", () => {
  install((address, req) => {
    if (address === "t/re") throw { payload: { tag: "reentrant" } };
    return { status: 200, headers: [], body: enc.encode(JSON.stringify({ greeting: `hi ${req.path}@${address}` })) };
  });
  const c = actorClient();
  const one = c.call("t/a", hello, { who: "x y" });
  assert.deepEqual(one, { ok: true, status: 200, value: { greeting: "hi /hello/x%20y@t/a" } });
  const r = c.batch([
    ["t/a", hello, { who: "1" }],
    ["t/re", hello, { who: "2" }],
    ["t/b", hello, { who: "3" }],
  ] as const);
  assert(r[0].ok && r[0].value.greeting === "hi /hello/1@t/a");
  assert(!r[1].ok && r[1].failure.kind === "transport" && r[1].failure.error.known === "reentrant");
  assert(r[2].ok && r[2].value.greeting === "hi /hello/3@t/b");
  const single = c.call("t/re", hello, { who: "z" });
  assert(!single.ok && single.failure.kind === "transport" && single.failure.error.kind === "reentrant");
});
