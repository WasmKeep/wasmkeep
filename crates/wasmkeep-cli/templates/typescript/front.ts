// The app's front: every request outside `/actor/` comes here. It holds no
// state; it calls class `counter`, which it reaches as an inside caller, so
// the class stays `internal` (no client reaches it directly).
//
// - `GET /`: a page with the count of actor `counter/main`, and a button
// - `POST /`: adds one to `counter/main`, then back to the page
// - `GET /api/counter/<name>`, `POST /api/counter/<name>`: any counter, by
//   name (1 to 32 of `a-z`, `0-9`, `-`)
//
// Anyone may count: there is no sign-in. examples/ts-todo-front in the
// WasmKeep repository has one, with a token per user from a secret.
import { actors, defineFront, unavailable, type ActorResponse } from "@wasmkeep/sdk/front";

/** Calls actor `counter/<name>`: its value, or the answer to give instead. */
function count(name: string, method: "GET" | "POST"): string | ActorResponse {
  try {
    const r = actors.call(`counter/${name}`, { method, path: "/" });
    if (r.status === 200) return r.text().trim();
    console.error(`counter/${name}: status ${r.status}`);
    return { status: 502, body: "the counter did not answer\n" };
  } catch (e) {
    console.error(`counter/${name}: ${e instanceof Error ? e.message : String(e)}`);
    return unavailable(e);
  }
}

function page(value: string): ActorResponse {
  // `value` is the actor's answer, digits only, but escape it anyway.
  const digits = value.replace(/[^0-9]/g, "");
  return {
    headers: {
      "content-type": "text/html; charset=utf-8",
      "x-content-type-options": "nosniff",
      "content-security-policy": "default-src 'none'; form-action 'self'; frame-ancestors 'none'",
    },
    body:
      `<!doctype html>\n<meta charset="utf-8">\n<title>{{app}}</title>\n<h1>{{app}}</h1>\n` +
      `<p>Count: <strong>${digits}</strong></p>\n` +
      `<form method="post" action="/"><button>Add one</button></form>\n`,
  };
}

export default defineFront({
  fetch(req) {
    const path = req.path.split("?")[0];
    if (path === "/" && req.method === "GET") {
      const value = count("main", "GET");
      return typeof value === "string" ? page(value) : value;
    }
    if (path === "/" && req.method === "POST") {
      const value = count("main", "POST");
      return typeof value === "string" ? { status: 303, headers: { location: "/" } } : value;
    }
    if (path.startsWith("/api/counter/") && (req.method === "GET" || req.method === "POST")) {
      const name = path.slice("/api/counter/".length);
      // Checked before any call: a name taken from a request would create
      // one actor, and one database, per guess.
      if (!/^[a-z0-9-]{1,32}$/.test(name)) return { status: 404, body: "not found\n" };
      const value = count(name, req.method);
      return typeof value === "string" ? `${value}\n` : value;
    }
    return { status: 404, body: "not found\n" };
  },
});
