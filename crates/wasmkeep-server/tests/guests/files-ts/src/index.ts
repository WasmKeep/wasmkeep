// An actor that stores and lists files with the TypeScript SDK
// (`@wasmkeep/sdk/blobs`, ADR 0030): the routes of the Rust one in
// ../files-rust but its streams (crates/wasmkeep-server/tests/cluster/blobs.rs
// runs both).

import { defineActor } from "@wasmkeep/sdk";
import * as blobs from "@wasmkeep/sdk/blobs";

const STATUS: Record<string, number> = {
  "invalid-key": 400,
  "not-allowed": 403,
  conflict: 409,
  "too-large": 413,
  "quota-exceeded": 507,
};

function query(path: string, name: string): string | undefined {
  const q = path.split("?")[1] ?? "";
  for (const pair of q.split("&")) {
    const at = pair.indexOf("=");
    if (at > 0 && pair.slice(0, at) === name) return pair.slice(at + 1);
  }
  return undefined;
}

function answer(f: () => string) {
  try {
    return f();
  } catch (e) {
    if (e instanceof NotFound) return { status: 404, body: "none" };
    if (e instanceof blobs.BlobError) {
      return { status: STATUS[e.kind] ?? 503, body: `err:${e.kind}` };
    }
    throw e;
  }
}

export default defineActor({
  fetch(req) {
    const path = req.path.split("?")[0];
    if (path.startsWith("/files/")) {
      const key = path.slice("/files/".length);
      if (req.method === "PUT") {
        return answer(() => {
          const i = blobs.put(key, req.bytes(), {
            contentType: query(req.path, "type"),
            ifAbsent: req.headers["if-none-match"] === "*",
            ifMatch: req.headers["if-match"],
          });
          return `ok ${i.etag} ${i.size} ${i.contentType}`;
        });
      }
      if (req.method === "DELETE") {
        return answer(() => {
          blobs.delete(key);
          return "ok";
        });
      }
      return answer(() => {
        const f = blobs.get(key);
        if (f === undefined) throw new NotFound();
        return f.text();
      });
    }
    if (path.startsWith("/info/")) {
      return answer(() => {
        const i = blobs.head(path.slice("/info/".length));
        if (i === undefined) throw new NotFound();
        return `${i.size} ${i.contentType}`;
      });
    }
    if (path === "/all") {
      const page = Number(query(req.path, "page") ?? "1000");
      return answer(() => [...blobs.listAll(query(req.path, "prefix") ?? "", page)].map((i) => i.key).join("\n"));
    }
    if (path === "/page") {
      return answer(() => {
        const l = blobs.list({
          prefix: query(req.path, "prefix"),
          after: query(req.path, "after"),
          limit: Number(query(req.path, "limit") ?? "1000"),
        });
        return [...l.entries.map((e) => e.key), `next=${l.next ?? "-"}`].join("\n");
      });
    }
    if (path.startsWith("/link/")) {
      return answer(() => blobs.linkDownload(path.slice("/link/".length)));
    }
    if (path.startsWith("/upload-link/")) {
      return answer(() => blobs.linkUpload(path.slice("/upload-link/".length), { size: 10, contentType: "image/png" }).url);
    }
    return { status: 404, body: "no route" };
  },
});

/** A missing file: answered 404 `none`. */
class NotFound extends Error {}
