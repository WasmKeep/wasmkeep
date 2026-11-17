// `@wasmkeep/sdk/blobs` over a stand-in host (test/blobs-stub.ts).

import { test } from "node:test";
import assert from "node:assert/strict";
import * as blobs from "../src/blobs";
import { pages, reset, seen } from "./blobs-stub";

test("put, get, head and delete, with sizes as numbers", () => {
  reset();
  assert.equal(blobs.head("a.txt"), undefined);
  assert.equal(blobs.get("a.txt"), undefined);
  const info = blobs.put("a.txt", "héllo", { contentType: "text/plain" });
  assert.deepEqual([info.key, info.size, info.contentType, typeof info.modifiedMs], ["a.txt", 6, "text/plain", "number"]);
  const file = blobs.get("a.txt")!;
  assert.equal(file.text(), "héllo");
  assert.deepEqual(file.info, info);
  assert.deepEqual(blobs.head("a.txt"), info);
  assert.equal(blobs.put("b", new Uint8Array([1, 2])).contentType, "application/octet-stream");
  blobs.delete("a.txt");
  assert.equal(blobs.head("a.txt"), undefined);
});

test("conditions, and host errors as BlobError", () => {
  reset();
  const first = blobs.put("k", "1", { ifAbsent: true });
  const conflict = (f: () => unknown) =>
    assert.throws(f, (e: unknown) => e instanceof blobs.BlobError && e.kind === "conflict" && e.known === "conflict");
  conflict(() => blobs.put("k", "2", { ifAbsent: true }));
  conflict(() => blobs.put("k", "2", { ifMatch: '"nope"' }));
  blobs.put("k", "2", { ifMatch: first.etag });
  assert.throws(() => blobs.put("k", "3", { ifAbsent: true, ifMatch: first.etag }), TypeError);
  assert.throws(
    () => blobs.head("a b"),
    (e: unknown) => e instanceof blobs.BlobError && e.kind === "invalid-key" && e.detail === "bad key a b",
  );
  assert.throws(() => blobs.list({ limit: 0 }), (e: unknown) => e instanceof blobs.BlobError && e.kind === "not-allowed");
});

test("listAll follows next across short and empty pages", () => {
  reset();
  const keys = ["a", "up", "upload", "uploads/a", "uploads/b", "z"];
  for (const k of keys) blobs.put(k, k);
  pages.max = 2;
  const page = blobs.list({ prefix: "up", limit: 10 });
  assert.deepEqual(page.entries.map((e) => e.key), ["up", "upload"]);
  assert.equal(page.next, "upload");
  assert.deepEqual([...blobs.listAll("up")].map((e) => e.key), ["up", "upload", "uploads/a", "uploads/b"]);
  pages.emptyPageOnce = true;
  assert.deepEqual([...blobs.listAll("", 3)].map((e) => e.key), keys);
  assert.deepEqual([...blobs.listAll("nothing")], []);
});

test("links pass their options to the host and its refusal back", () => {
  const refused = (e: unknown) => e instanceof blobs.BlobError && e.kind === "not-allowed";
  assert.throws(() => blobs.linkUpload("u/1", { size: 10, contentType: "image/png" }), refused);
  assert.deepEqual(seen.upload, { size: 10n, contentType: "image/png", expiresInSecs: undefined });
  assert.throws(() => blobs.linkDownload("u/1", { filename: "photo.png" }), refused);
  assert.deepEqual(seen.download, { expiresInSecs: undefined, disposition: { tag: "attachment", val: "photo.png" } });
  assert.throws(() => blobs.linkDownload("u/1", { inline: true, expiresInSecs: 60 }), refused);
  assert.deepEqual(seen.download, { expiresInSecs: 60, disposition: { tag: "inline" } });
});
