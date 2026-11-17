// The contract against a fake server: what each operation sends, and how an
// answer, a declared error and a malformed answer come back.
import assert from "node:assert/strict";
import { test } from "node:test";
import { Client, problemResponse, type Transport } from "@wasmkeep/sdk/contracts";
import { add, list, remove, toggle } from "../src/contract";

type Sent = { address: string; method: string; path: string; body?: string };

function fake(answer: { status: number; headers?: Record<string, string>; body?: string }) {
  const sent: Sent[] = [];
  const transport: Transport = {
    call(address, request) {
      sent.push({ address, method: request.method, path: request.path, body: request.body && new TextDecoder().decode(request.body) });
      return { status: answer.status, headers: answer.headers ?? {}, body: new TextEncoder().encode(answer.body ?? "") };
    },
    callMany() {
      throw new Error("not used");
    },
  };
  return { client: new Client(transport), sent };
}

const json = { "content-type": "application/json" };

test("list sends GET / and decodes the items", () => {
  const { client, sent } = fake({ status: 200, headers: json, body: '{"todos":[{"id":1,"text":"milk","done":false,"extra":1}]}' });
  const r = client.call("todos/main", list);
  assert(r.ok);
  assert.deepEqual(r.value, { todos: [{ id: 1, text: "milk", done: false }] });
  assert.deepEqual(sent, [{ address: "todos/main", method: "GET", path: "/", body: undefined }]);
});

test("add sends the text as JSON", () => {
  const { client, sent } = fake({ status: 201, headers: json, body: '{"id":2,"text":"eggs","done":false}' });
  const r = client.call("todos/main", add, { text: "eggs" });
  assert(r.ok);
  assert.equal(r.value.id, 2);
  assert.deepEqual(sent[0], { address: "todos/main", method: "POST", path: "/", body: '{"text":"eggs"}' });
});

test("toggle and remove put the id in the path", () => {
  const a = fake({ status: 200, headers: json, body: '{"id":7,"text":"x","done":true}' });
  assert(a.client.call("todos/main", toggle, { id: 7 }).ok);
  assert.equal(a.sent[0].path, "/7/toggle");
  const b = fake({ status: 204 });
  assert(b.client.call("todos/main", remove, { id: 7 }).ok);
  assert.deepEqual(b.sent[0], { address: "todos/main", method: "DELETE", path: "/7", body: undefined });
});

test("a declared error comes back typed", () => {
  const p = problemResponse(400, { code: "todos.invalid_text", message: "Write 1 to 200 characters", details: { max: 200 } });
  const { client } = fake({ status: 400, headers: p.headers as Record<string, string>, body: p.body as string });
  const r = client.call("todos/main", add, { text: "" });
  assert(!r.ok);
  assert(r.failure.kind === "declared");
  assert.equal(r.failure.code, "todos.invalid_text");
  assert.equal(r.failure.details.max, 200);
});

test("an answer of the wrong shape is not believed", () => {
  const { client } = fake({ status: 200, headers: json, body: '{"todos":[{"id":"1"}]}' });
  const r = client.call("todos/main", list);
  assert(!r.ok);
  assert.equal(r.failure.kind, "invalid-response");
});

test("an input of the wrong shape is not sent", () => {
  const { client, sent } = fake({ status: 200 });
  // @ts-expect-error text must be a string
  const r = client.call("todos/main", add, { text: 1 });
  assert(!r.ok);
  assert.equal(r.failure.kind, "invalid-request");
  assert.equal(sent.length, 0);
});
