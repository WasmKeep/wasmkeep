// The request helpers (ADR 0023): `caller` never takes a value the host
// does not send for the front, `scheme` reads the first X-Forwarded-Proto
// value only, and `unavailable` keeps a call's detail from the client.

import { test } from "node:test";
import assert from "node:assert/strict";
import { CallError } from "../src/errors";
import { caller, host, parseCaller, scheme, unavailable, type ActorRequest } from "../src/request";

function request(headerList: [string, string][], authority?: string): ActorRequest {
  return {
    method: "GET",
    path: "/",
    headers: Object.fromEntries(headerList),
    headerList,
    authority,
    bytes: () => new Uint8Array(),
    text: () => "",
    json: <T>() => ({}) as T,
  };
}

test("the front is exactly tenant/_front/main", () => {
  assert.deepEqual(parseCaller("default/_front/main"), { kind: "front" });
  assert.deepEqual(parseCaller("acme/_front/main"), { kind: "front" });
  for (const odd of [
    "default/_front/main/x",
    "default/_front/other",
    "default/_front/",
    "default/_front",
    "/_front/main",
    "default/_Front/main",
    "default/_front/main%20",
    "_front/main",
  ]) {
    assert(parseCaller(odd).kind !== "front", odd);
  }
});

test("an actor is its address with the name decoded", () => {
  assert.deepEqual(parseCaller("default/room/lobby"), {
    kind: "actor",
    address: { tenant: "default", class: "room", name: "lobby" },
  });
  assert.deepEqual(parseCaller("acme/kv/a%2Fb%20c"), {
    kind: "actor",
    address: { tenant: "acme", class: "kv", name: "a/b c" },
  });
  assert.deepEqual(parseCaller("acme/kv/a/b"), {
    kind: "actor",
    address: { tenant: "acme", class: "kv", name: "a/b" },
  });
});

test("absent or malformed is outside", () => {
  assert.deepEqual(parseCaller(undefined), { kind: "outside" });
  for (const odd of [
    "",
    "default",
    "default/room",
    "default/room/",
    "/room/lobby",
    "default//lobby",
    "default/Room/lobby",
    "default/9room/lobby",
    "default/room/bad%zz",
    "default/room/bad%2",
    "default/room/%ff",
  ]) {
    assert.deepEqual(parseCaller(odd), { kind: "outside" }, odd);
  }
});

test("caller reads the first caller header", () => {
  assert.deepEqual(caller(request([])), { kind: "outside" });
  assert.deepEqual(caller(request([["x-wasmkeep-caller", "default/_front/main"]])), { kind: "front" });
  assert.deepEqual(
    caller(
      request([
        ["x-wasmkeep-caller", "default/room/a"],
        ["x-wasmkeep-caller", "default/_front/main"],
      ]),
    ).kind,
    "actor",
  );
});

test("scheme reads the first X-Forwarded-Proto value", () => {
  const s = (...values: string[]) => scheme(request(values.map((v) => ["x-forwarded-proto", v])));
  assert.equal(s(), "http");
  assert.equal(s("https"), "https");
  assert.equal(s("HTTPS"), "https");
  assert.equal(s(" https , http"), "https");
  assert.equal(s("http, https"), "http");
  assert.equal(s("http", "https"), "http");
  assert.equal(s("https", "http"), "https");
  assert.equal(s(""), "http");
  assert.equal(s("httpsx"), "http");
});

test("host is the request's authority", () => {
  assert.equal(host(request([], "shop.example")), "shop.example");
  assert.equal(host(request([["host", "spoofed"]])), undefined);
});

test("unavailable is 503 for failed and timeout, without the detail", () => {
  for (const e of [new CallError("failed", "front busy: too many"), new CallError("timeout", "timeout")]) {
    const r = unavailable(e) as { status: number; headers: Record<string, string>; body: string };
    assert.equal(r.status, 503);
    assert.equal(r.headers["retry-after"], "1");
    assert(!r.body.includes("busy"));
  }
  for (const e of [new CallError("invalid-address", "secret"), new CallError("reentrant", "x"), new Error("secret")]) {
    const r = unavailable(e) as { status: number; headers?: Record<string, string>; body: string };
    assert.equal(r.status, 500);
    assert.equal(r.headers?.["retry-after"], undefined);
    assert(!r.body.includes("secret"));
  }
});
