// The browser client against a fake `fetch`: the URL and body it sends, the
// value it returns, and a server that cannot be reached.
import assert from "node:assert/strict";
import { test } from "node:test";
import { problemResponse } from "@wasmkeep/sdk/contracts";
import { describe, todoList } from "../src/api";

type Fetch = (url: string, init: { method: string; headers: Record<string, string>; body: string | null }) => Promise<Response>;
const calls: Array<{ url: string; method: string; body: string | null }> = [];

function serve(handler: () => Response | Promise<Response>) {
  calls.length = 0;
  const fetch: Fetch = async (url, init) => {
    calls.push({ url, method: init.method, body: init.body });
    return handler();
  };
  (globalThis as unknown as { fetch: Fetch }).fetch = fetch;
}

test("add posts to the actor's path and returns the item", async () => {
  serve(() => new Response('{"id":3,"text":"milk","done":false}', { status: 201, headers: { "content-type": "application/json" } }));
  const r = await todoList("groceries").add("milk");
  assert(r.ok);
  assert.equal(r.value.id, 3);
  assert.deepEqual(calls, [{ url: "/actor/todos/groceries/", method: "POST", body: '{"text":"milk"}' }]);
});

test("a declared error is described by its message", async () => {
  const p = problemResponse(404, { code: "todos.not_found", message: "No such item" });
  serve(() => new Response(p.body as string, { status: 404, headers: p.headers as Record<string, string> }));
  const r = await todoList("main").remove(9);
  assert(!r.ok);
  assert.equal(describe(r.failure), "No such item");
});

test("a server that cannot be reached is a transport failure", async () => {
  serve(() => Promise.reject(new TypeError("fetch failed")));
  const r = await todoList("main").list();
  assert(!r.ok);
  assert.equal(r.failure.kind, "transport");
  assert.equal(describe(r.failure), "The server cannot be reached. Try again.");
});

test("an invalid input sends nothing", async () => {
  serve(() => new Response("{}"));
  const r = await todoList("main").toggle(1.5);
  assert(!r.ok);
  assert.equal(r.failure.kind, "invalid-request");
  assert.equal(calls.length, 0);
});
