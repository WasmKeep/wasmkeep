// A test guest for crates/wasmkeep-server/tests/fetch_langs.rs: the
// TypeScript SDK's `http.fetch` and `http.fetchAll` behind the same routes
// as the Rust test-actor's, on QuickJS and on StarlingMonkey.
//
// GET /fetch?url=U&send=N fetches U (percent-encoded) with a 5 s timeout
// and a 4096-byte limit: a GET, or a POST of N bytes of `x` when N > 0.
// It answers `STATUS:BODY`, or `error: ...`.
//
// GET /fetch-all?u=I&u=... (each I percent-encoded `N URL`) sends those
// requests with `http.fetchAll`, and answers one line each, as /fetch
// words it; with seq=1, one `http.fetch` after the other instead.
//
// GET /count answers how many times it ran in this instance; GET /large
// answers 16 MiB, for a caller that leaves after the head. GET /throw
// throws an object with a secret field, which no answer may show.

import { defineActor, http } from "@wasmkeep/sdk";

/** The query's values of `name`, decoded. */
function params(path: string, name: string): string[] {
  const query = path.split("?")[1] ?? "";
  return query
    .split("&")
    .filter((p) => p.startsWith(`${name}=`))
    .map((p) => decodeURIComponent(p.slice(name.length + 1).replace(/\+/g, " ")));
}

function request(send: number, url: string): http.FetchRequest {
  const r: http.FetchRequest = { url, timeoutMs: 5000, limit: 4096 };
  if (send > 0) {
    r.method = "POST";
    r.body = "x".repeat(send);
  }
  return r;
}

function line(r: http.FetchResult): string {
  return r.ok ? `${r.response.status}:${r.response.text()}` : `error: ${r.error.message}`;
}

let count = 0;
export default defineActor({
  fetch(req) {
    const path = req.path.split("?")[0];
    if (req.method !== "GET") return { status: 404, body: "GET only\n" };
    if (path === "/count") return String(++count);
    if (path === "/large") return new Uint8Array(16 * 1024 * 1024);
    if (path === "/throw") throw { message: "refused", token: "secret-token-1234" };
    if (path === "/fetch") {
      const send = Number(params(req.path, "send")[0] ?? "0");
      const r = request(send, params(req.path, "url")[0] ?? "");
      try {
        const got = http.fetch(r.url, r);
        return `${got.status}:${got.text()}`;
      } catch (e) {
        return `error: ${(e as Error).message}`;
      }
    }
    if (path === "/fetch-all") {
      const batch = params(req.path, "u").map((item) => {
        const at = item.indexOf(" ");
        return request(Number(item.slice(0, at)), item.slice(at + 1));
      });
      let results: http.FetchResult[];
      if (params(req.path, "seq").length > 0) {
        results = batch.map((r) => {
          try {
            return { ok: true, response: http.fetch(r.url, r) } as const;
          } catch (e) {
            return { ok: false, error: e as http.FetchError } as const;
          }
        });
      } else {
        results = http.fetchAll(batch);
      }
      return results.map((r) => `${line(r)}\n`).join("");
    }
    return { status: 404, body: "no route\n" };
  },
});
