// The todo-front example's front in TypeScript: the JSON API of
// examples/todo-front/front, over the same `todos` actor class (built from
// examples/todo-front/todos), which it reaches as an inside caller.
//
// - `GET /api`: where the API is, built from `scheme()` and `host()`
// - `GET /api/todos`, `POST /api/todos`, `DELETE /api/todos/<id>`: the
//   signed-in user's list, with `Authorization: Bearer <token>`
//
// Sign-in is a token per user, from the front's secret `TOKENS`
// (`alice:<token>,bob:<token>`, each token at least 32 characters). With
// `TOKENS` unset, empty or malformed, nobody is signed in. Tokens are
// compared in constant time, and the user name, which becomes the actor's
// name, is checked before any call.

import { actors, defineFront, env, host, scheme, unavailable, type ActorRequest } from "@wasmkeep/sdk/front";

/** The class the front calls; deploy it `--access todos=internal`. */
const CLASS = "todos";
const MIN_TOKEN = 32;
const MAX_BODY = 1000;

/** Equal, in time that depends on the lengths only. */
function same(a: string, b: string): boolean {
  if (a.length !== b.length) return false;
  let diff = 0;
  for (let i = 0; i < a.length; i++) diff |= a.charCodeAt(i) ^ b.charCodeAt(i);
  return diff === 0;
}

/** The user whose token the request carries, if any. Every entry is
 * compared, whatever matches. */
function signedIn(req: ActorRequest): string | undefined {
  // The first Authorization header, as the Rust front reads it.
  const auth = req.headerList?.find(([n]) => n === "authorization")?.[1] ?? req.headers["authorization"] ?? "";
  if (!auth.startsWith("Bearer ")) return undefined;
  const token = auth.slice("Bearer ".length).trim();
  const tokens = env.get("TOKENS");
  if (tokens === undefined || token.length < MIN_TOKEN) return undefined;
  let found: string | undefined;
  for (const entry of tokens.split(/[,\n]/)) {
    const at = entry.indexOf(":");
    if (at < 0) continue;
    const user = entry.slice(0, at).trim();
    const expected = entry.slice(at + 1).trim();
    if (/^[a-z0-9-]{1,32}$/.test(user) && expected.length >= MIN_TOKEN && same(expected, token)) found = user;
  }
  return found;
}

export default defineFront({
  fetch(req) {
    const path = req.path.split("?")[0];
    if (req.method === "GET" && path === "/") {
      return "todo-front in TypeScript: the API is at /api\n";
    }
    if (req.method === "GET" && path === "/api") {
      // The client chooses Host: JSON.stringify escapes it like any input.
      return { headers: { "content-type": "application/json" }, body: JSON.stringify({ todos: `${scheme(req)}://${host(req) ?? "localhost"}/api/todos` }) + "\n" };
    }
    if (path !== "/api/todos" && !path.startsWith("/api/todos/")) return { status: 404, body: "not found\n" };
    const user = signedIn(req);
    if (user === undefined) {
      return { status: 401, headers: { "www-authenticate": "Bearer" }, body: "sign in with Authorization: Bearer <token>\n" };
    }
    const rest = path.slice("/api/todos".length);
    let call: { method: string; path: string; body?: Uint8Array };
    if (req.method === "GET" && rest === "") call = { method: "GET", path: "/" };
    else if (req.method === "POST" && rest === "") {
      // `bytes()` reads the whole body (up to the host's 16 MiB limit):
      // refuse a declared length past ours before reading.
      if (Number(req.headers["content-length"] ?? 0) > MAX_BODY) return { status: 413, body: "at most 1000 bytes\n" };
      const body = req.bytes();
      if (body.length > MAX_BODY) return { status: 413, body: "at most 1000 bytes\n" };
      call = { method: "POST", path: "/", body };
    } else if (req.method === "DELETE" && /^\/[0-9]{1,19}$/.test(rest)) call = { method: "DELETE", path: rest };
    else return { status: 404, body: "not found\n" };
    try {
      const r = actors.call(`${CLASS}/${user}`, { ...call, headers: { "content-type": "text/plain; charset=utf-8" } });
      const type = r.headers["content-type"];
      return { status: r.status, headers: type ? { "content-type": type } : {}, body: r.body };
    } catch (e) {
      console.error(`call to ${CLASS}/${user}: ${e instanceof Error ? e.message : String(e)}`);
      return unavailable(e);
    }
  },
});
