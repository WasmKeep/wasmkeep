// The database pattern's wire (ADR 0029) and client against fake actors:
// the same request and answer bytes as the Rust SDK's `db.rs` tests, the
// transaction-control check case by case, the limits, and how the client
// reads answers, errors and failed calls.

import { test } from "node:test";
import assert from "node:assert/strict";
import { install, type Request } from "./host-stub";
import { Client, objects } from "../src/db-client";
import {
  DEFAULT_LIMITS,
  DbError,
  type Statement,
  controlsTransaction,
  decodeReply,
  decodeRequest,
  encodeRequest,
  errorBody,
  limitsOf,
  rowsBody,
  valueFrom,
  wireValue,
} from "../src/db-wire";

const enc = new TextEncoder();
const dec = new TextDecoder();

// Byte for byte the Rust SDK's `db::tests::REQUEST` and `RESPONSE`.
const REQUEST =
  String.raw`{"statements":[{"sql":"INSERT INTO t (a, b, c, d) VALUES (?1, ?2, ?3, ?4) RETURNING id",` +
  String.raw`"params":[{"integer":"-9223372036854775808"},{"real":"1.5e+21"},"tab\there \"é\" 😀",{"blob":"00ff10"}],"rows":true},` +
  String.raw`{"sql":"DELETE FROM t WHERE a = ?1","params":[null],"rows":false}]}`;
const RESPONSE =
  String.raw`{"results":[{"columns":["id","x"],"rows":[[{"integer":"9223372036854775807"},{"real":"2"}],` +
  String.raw`[{"integer":"9007199254740993"},{"real":"-0"}],[null,"\u0001"]]},{"changes":3}]}`;

const STATEMENTS: Statement[] = [
  {
    sql: "INSERT INTO t (a, b, c, d) VALUES (?1, ?2, ?3, ?4) RETURNING id",
    params: [-(2n ** 63n), 1.5e21, 'tab\there "é" 😀', new Uint8Array([0, 255, 16])],
    rows: true,
  },
  { sql: "DELETE FROM t WHERE a = ?1", params: [null] },
];

test("requests are written byte for byte as the Rust client writes them", () => {
  assert.equal(encodeRequest(STATEMENTS), REQUEST);
  const decoded = decodeRequest(enc.encode(REQUEST), DEFAULT_LIMITS);
  if (!Array.isArray(decoded)) throw new Error(decoded.message);
  assert.deepEqual(decoded[0].params, [
    { tag: "integer", val: -(2n ** 63n) },
    { tag: "real", val: 1.5e21 },
    { tag: "text", val: 'tab\there "é" 😀' },
    { tag: "blob", val: new Uint8Array([0, 255, 16]) },
  ]);
  assert.equal(decoded[0].rows, true);
  assert.deepEqual(decoded[1], { sql: "DELETE FROM t WHERE a = ?1", params: [{ tag: "null" }], rows: false });
});

test("bytes of any typed array bind as a blob", () => {
  const view = new Uint8Array([9, 0, 255, 16, 9]).subarray(1, 4);
  const other = new DataView(view.buffer, 1, 3);
  assert.equal(
    encodeRequest([{ sql: "x", params: [view, other as unknown as Uint8Array] }]),
    '{"statements":[{"sql":"x","params":[{"blob":"00ff10"},{"blob":"00ff10"}],"rows":false}]}',
  );
});

test("a safe integer binds as an integer unless tagged real; booleans as 1 or 0", () => {
  const body = encodeRequest([{ sql: "x", params: [2, { tag: "real", val: 2 }, true, 2.5, -0] }]);
  assert.equal(
    body,
    '{"statements":[{"sql":"x","params":[{"integer":"2"},{"real":"2"},{"integer":"1"},{"real":"2.5"},{"integer":"0"}],"rows":false}]}',
  );
  assert.throws(() => encodeRequest([{ sql: "x", params: [2n ** 63n] }]), RangeError);
  assert.equal(
    encodeRequest([{ sql: "x", params: [1e300, 2 ** 53] }]),
    '{"statements":[{"sql":"x","params":[{"real":"1e+300"},{"real":"9007199254740992"}],"rows":false}]}',
    "past 2^53 - 1, a number is a real: it may have been rounded already",
  );
  assert.throws(() => encodeRequest([{ sql: "x", params: ["\ud800"] }]), RangeError);
  assert.throws(() => encodeRequest([{ sql: "\udc00" }]), RangeError);
});

test("answers are read and written as the Rust SDK does", () => {
  const outcomes = decodeReply(200, enc.encode(RESPONSE), STATEMENTS);
  assert.deepEqual(outcomes, [
    {
      columns: ["id", "x"],
      rows: [
        [9223372036854775807n, 2],
        [9007199254740993n, -0],
        [null, "\u0001"],
      ],
    },
    { changes: 3 },
  ]);
  assert.equal(Object.is((outcomes[0] as { rows: unknown[][] }).rows[1][1], -0), true);
  const rows = rowsBody(
    ["id", "x"],
    [
      [
        { tag: "integer", val: 9223372036854775807n },
        { tag: "real", val: 2 },
      ],
      [
        { tag: "integer", val: 9007199254740993n },
        { tag: "real", val: -0 },
      ],
      [{ tag: "null" }, { tag: "text", val: "\u0001" }],
    ],
    0,
    Infinity,
  );
  assert.equal(`{"results":[${rows},{"changes":3}]}`, RESPONSE);
  assert.equal(rowsBody(["a"], [[{ tag: "text", val: "x".repeat(100) }]], 0, 50), undefined, "over the cap");
  // An answer whose least size is over the cap is refused before any of it
  // is encoded: no byte of these blobs is read.
  const unread = new Proxy(new Uint8Array(20_000), {
    get(target, key) {
      if (key === "length") return target.length;
      throw new Error(`blob read: ${String(key)}`);
    },
  });
  const blobs = [0, 1, 2].map(() => [{ tag: "blob", val: unread } as const]);
  assert.equal(rowsBody(["b"], blobs, 0, 64 * 1024), undefined, "refused unread");
  // The least size never refuses what fits: exactly at the cap is kept.
  const small = [[{ tag: "blob", val: new Uint8Array([0, 255]) } as const]];
  const fits = rowsBody(["b"], small, 0, Infinity)!;
  assert.equal(fits, '{"columns":["b"],"rows":[[{"blob":"00ff"}]]}');
  const counted = fits.length - "]}".length; // the caller counts the closing
  assert.equal(rowsBody(["b"], small, 0, counted), fits, "at the cap");
  assert.equal(rowsBody(["b"], small, 0, counted - 1), undefined, "a byte over");

  const malformed = (body: string, statements: Statement[] = STATEMENTS) => {
    assert.throws(
      () => decodeReply(200, enc.encode(body), statements),
      (e: unknown) => e instanceof DbError && e.kind === "refused" && e.status === 200,
      body,
    );
  };
  malformed(RESPONSE, STATEMENTS.slice(0, 1));
  malformed(RESPONSE, [STATEMENTS[1], STATEMENTS[0]]);
  malformed('{"results":[],"x":1}', []);
  malformed('{"results":[{"changes":1.5}]}', [{ sql: "x" }]);
  malformed('{"results":[{"columns":["a"],"rows":[[1]]}]}', [{ sql: "x", rows: true }]);
});

test("values are exact, and anything else is refused", () => {
  for (const bad of [
    { integer: "01" },
    { integer: "-0" },
    { integer: "+1" },
    { integer: " 1" },
    { integer: "9223372036854775808" },
    { integer: 1 },
    { real: "inf" },
    { real: "0x10" },
    { real: "" },
    { real: " 1" },
    { real: 1.5 },
    { blob: "abc" },
    { blob: "zz" },
    { text: "a" },
    { integer: "1", real: "1" },
    1,
    true,
    [],
    "\ud800",
  ]) {
    assert.equal(valueFrom(bad), undefined, JSON.stringify(bad));
  }
  assert.deepEqual(wireValue({ tag: "real", val: NaN }), { real: "NaN" });
  assert.deepEqual(valueFrom({ real: "-Infinity" }), { tag: "real", val: -Infinity });
  assert.deepEqual(valueFrom({ real: "1.e5" }), { tag: "real", val: 1e5 });
  assert.deepEqual(valueFrom({ blob: "0F" }), { tag: "blob", val: new Uint8Array([15]) });
});

test("errors are written and read as the Rust SDK does", () => {
  const sql = errorBody("sql", "UNIQUE constraint failed: t.a", "constraint", 1);
  assert.equal(sql, '{"error":"sql","kind":"constraint","statement":1,"message":"UNIQUE constraint failed: t.a"}');
  assert.throws(
    () => decodeReply(400, enc.encode(sql), []),
    (e: unknown) =>
      e instanceof DbError &&
      e.kind === "sql" &&
      e.sqlKind === "constraint" &&
      e.statement === 1 &&
      e.message === "statement 1: constraint error: UNIQUE constraint failed: t.a",
  );
  const commit = errorBody("sql", "FOREIGN KEY constraint failed", "constraint");
  assert.throws(
    () => decodeReply(400, enc.encode(commit), []),
    (e: unknown) => e instanceof DbError && e.statement === undefined && e.message.startsWith("transaction: "),
  );
  assert.throws(
    () => decodeReply(400, enc.encode(errorBody("not-allowed", "no", undefined, 0)), []),
    (e: unknown) => e instanceof DbError && e.kind === "not-allowed" && e.statement === 0,
  );
  assert.throws(
    () => decodeReply(500, enc.encode(errorBody("unknown-outcome", "may be kept")), []),
    (e: unknown) => e instanceof DbError && e.kind === "call" && e.message === "may be kept",
  );
  assert.throws(
    () => decodeReply(413, enc.encode(errorBody("too-large", "big")), []),
    (e: unknown) => e instanceof DbError && e.kind === "too-large" && e.message === "too large: big",
  );
  assert.throws(
    () => decodeReply(403, enc.encode(errorBody("forbidden", "outside")), []),
    (e: unknown) => e instanceof DbError && e.kind === "refused" && e.status === 403 && e.detail === "outside",
  );
  assert.throws(
    () => decodeReply(502, enc.encode(" plain text\n"), []),
    (e: unknown) => e instanceof DbError && e.kind === "refused" && e.status === 502 && e.detail === "plain text",
  );
});

test("requests are checked against the limits", () => {
  const limits = { ...DEFAULT_LIMITS, maxStatements: 1, maxSqlBytes: 10, maxParams: 1 };
  const req = (s: Statement[]) => enc.encode(encodeRequest(s));
  const code = (r: ReturnType<typeof decodeRequest>) => (Array.isArray(r) ? "ok" : `${r.status} ${r.code}`);
  assert.equal(code(decodeRequest(req([{ sql: "SELECT 1" }]), limits)), "ok");
  assert.equal(code(decodeRequest(req([{ sql: "SELECT 1" }, { sql: "SELECT 1" }]), limits)), "413 too-large");
  assert.equal(code(decodeRequest(req([{ sql: "SELECT 12345" }]), limits)), "413 too-large");
  assert.equal(code(decodeRequest(req([{ sql: "SELECT 'éé'" }]), limits)), "413 too-large", "bytes, not chars");
  assert.equal(code(decodeRequest(req([{ sql: "SELECT ?", params: [null, null] }]), limits)), "413 too-large");
  for (const bad of [
    new Uint8Array([0xff]),
    enc.encode(""),
    enc.encode("[]"),
    enc.encode('{"statements":[]}'),
    enc.encode('{"statements":{}}'),
    enc.encode('{"statements":[{"sql":1}]}'),
    enc.encode('{"statements":[{"sql":"x","rows":1}]}'),
    enc.encode('{"statements":[{"sql":"x","params":[1]}]}'),
    enc.encode('{"statements":[{"sql":"x","extra":1}]}'),
    enc.encode('{"statements":[{"sql":"x"}],"mode":"write"}'),
    enc.encode('{"statements":[{"sql":"\\ud800"}]}'),
  ]) {
    assert.equal(code(decodeRequest(bad, DEFAULT_LIMITS)), "400 bad-request", dec.decode(bad));
  }
  const bare = decodeRequest(enc.encode('{"statements":[{"sql":"x"}]}'), DEFAULT_LIMITS);
  assert.deepEqual(bare, [{ sql: "x", params: [], rows: false }], "params and rows may be left out");
});

test("a limit left undefined keeps its default; a bad one throws", () => {
  assert.deepEqual(limitsOf(undefined), DEFAULT_LIMITS);
  assert.deepEqual(limitsOf({ maxStatements: undefined, maxParams: 3 }), { ...DEFAULT_LIMITS, maxParams: 3 });
  for (const bad of [NaN, -1, "5" as unknown as number]) {
    assert.throws(() => limitsOf({ maxRequestBytes: bad }), RangeError);
  }
});

test("transaction control is spotted however it is written", () => {
  for (const sql of [
    "COMMIT",
    "commit",
    "  End transaction",
    "BEGIN IMMEDIATE",
    "ROLLBACK TO sp",
    "savepoint sp",
    "RELEASE sp",
    "/**/commit",
    "/* a */ -- b\n COMMIT",
    ";COMMIT",
    " ; ;\n\tcommit;",
    "COMMIT/* x */",
    "\fBEGIN",
  ]) {
    assert.equal(controlsTransaction(sql), true, JSON.stringify(sql));
  }
  for (const sql of [
    "SELECT 1",
    "INSERT INTO begin_log VALUES (1)",
    "commits",
    "COMMITé",
    '"COMMIT"',
    "-- COMMIT",
    "/* COMMIT */ SELECT 1",
    "/* unterminated COMMIT",
    "",
    "EXPLAIN COMMIT",
    "WITH x AS (SELECT 1) SELECT * FROM x",
    "COMMİT",
  ]) {
    assert.equal(controlsTransaction(sql), false, JSON.stringify(sql));
  }
});

test("the client sends POSTs to the database's address and reads the answers", () => {
  const calls: { address: string; request: Request }[] = [];
  install((address, request) => {
    calls.push({ address, request: { ...request, body: request.body.slice() } });
    const body = JSON.parse(dec.decode(request.body));
    const answer = body.statements.map((s: { rows: boolean }) =>
      s.rows ? { columns: ["n", "n"], rows: [[{ integer: "1" }, "a"]] } : { changes: 2 },
    );
    return { status: 200, headers: [], body: enc.encode(JSON.stringify({ results: answer })) };
  });
  const main = new Client("db/main");
  const rows = main.query("SELECT ?1 AS n", [1]);
  assert.deepEqual(rows, { columns: ["n", "n"], rows: [[1, "a"]] });
  assert.deepEqual(objects(rows), [{ n: "a" }], "a repeated column keeps its last value");
  assert.equal(main.exec("DELETE FROM t"), 2);
  assert.deepEqual(main.batch([{ sql: "a" }, { sql: "b", rows: true }]), [
    { changes: 2 },
    { columns: ["n", "n"], rows: [[1, "a"]] },
  ]);
  assert.deepEqual(main.read([{ sql: "c", rows: true }]), [{ columns: ["n", "n"], rows: [[1, "a"]] }]);
  assert.deepEqual(
    calls.map((c) => [c.address, c.request.method, c.request.path, c.request.headers]),
    [
      ["db/main", "POST", "/wasmkeep-db/v1/read", [["content-type", "application/json"]]],
      ["db/main", "POST", "/wasmkeep-db/v1/write", [["content-type", "application/json"]]],
      ["db/main", "POST", "/wasmkeep-db/v1/write", [["content-type", "application/json"]]],
      ["db/main", "POST", "/wasmkeep-db/v1/read", [["content-type", "application/json"]]],
    ],
  );
  assert.equal(
    dec.decode(calls[0].request.body),
    '{"statements":[{"sql":"SELECT ?1 AS n","params":[{"integer":"1"}],"rows":true}]}',
  );

  install(() => {
    throw { payload: { tag: "timeout" } };
  });
  assert.throws(
    () => main.exec("x"),
    (e: unknown) => e instanceof DbError && e.kind === "call" && e.message === "call to db/main timed out",
  );
});
