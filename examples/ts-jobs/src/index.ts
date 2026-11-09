// The app class of the jobs example, in TypeScript: an HTTP API over the
// SDK's kv and queue clients, and the consumer the `jobs` queue pushes to.
// It talks to the Rust shard and queue classes of examples/jobs (`kv` and
// `queue`) and must be deployed as class `app`: `queue/jobs` delivers to
// `app/worker`.
//
// Key-value (keys spread over 4 shards of class `kv`):
//
// - `GET /kv/<key>`: the value, with its version in `etag`; 404 if absent.
//   `?cached=1` reads through this actor's 60 s read cache.
// - `PUT /kv/<key>`: stores the body and answers the new version.
//   `if-match: <version>` or `if-none-match: *` make it a compare-and-set
//   (409 when it does not hold); `?ttl_ms=N` expires it.
// - `DELETE /kv/<key>`: 200 if it was there, 404 otherwise; with
//   `if-match: <version>`, only if it holds that version (409 otherwise).
// - `GET /kv?prefix=P`: the keys starting with `P`, one per line.
//
// Queues (actors of class `queue`):
//
// - `POST /jobs`: queues the body on `queue/jobs`, which pushes it to
//   `app/worker`. The worker stores the body in upper case at key
//   `job:<id>`; a body starting with `fail` asks for a retry every time, so
//   it ends in the dead-letter table after three attempts.
// - `GET /jobs/stats`, `GET /jobs/dead`: counts and dead letters.
// - `POST /inbox`: queues the body on `queue/inbox`, a pull queue.
// - `POST /inbox/batch`: queues each line of the body, in one write;
//   answers their ids, one per line.
// - `POST /inbox/lease?visibility_ms=N&max=M`: leases up to `M` (default
//   one) messages, answered as `<id> <attempt> <body>` lines.
// - `POST /inbox/ack?id=I&attempt=A`: 200 if that lease was current, 409
//   if it ran out (the message was leased again) or was settled.
import { defineActor, kv, queue, type ActorRequest, type ActorResponse } from "@wasmkeep/sdk";

const SHARDS = 4;
const decoder = new TextDecoder();

/** Keys over 4 shards, with a 60 s read cache: `get` may use it, writes
 * through this client update it, `getFresh` skips it. */
const store = new kv.Client("kv", SHARDS).withReadCache(60_000);
const jobs = new queue.Client("queue/jobs");
const inbox = new queue.Client("queue/inbox");

function param(path: string, name: string): string | undefined {
  const query = path.split("?")[1];
  if (query === undefined) return undefined;
  for (const pair of query.split("&")) {
    const [k, v] = pair.split("=");
    if (k === name) return v ?? "";
  }
  return undefined;
}

/** A non-negative integer parameter, or `undefined`. */
function number(value: string | undefined): number | undefined {
  return value !== undefined && /^[0-9]+$/.test(value) ? Number(value) : undefined;
}

function text(status: number, body: string): ActorResponse {
  return { status, body };
}

/** The error's text, with the kv and queue errors' status mapping. */
function failure(e: unknown): ActorResponse {
  if (e instanceof kv.KvError && e.kind === "conflict") return text(409, "the key changed\n");
  if (e instanceof kv.KvError || e instanceof queue.QueueError) return text(502, `${e.message}\n`);
  throw e;
}

function kvRoute(req: ActorRequest, key: string): ActorResponse {
  const match = req.headers["if-match"];
  const version = match === undefined ? undefined : number(match.replace(/"/g, ""));
  if (match !== undefined && version === undefined) return text(400, "if-match is not a version\n");
  try {
    switch (req.method) {
      case "GET": {
        const entry = param(req.path, "cached") === "1" ? store.get(key) : store.getFresh(key);
        if (entry === undefined) return text(404, "no such key\n");
        return { headers: { etag: String(entry.version) }, body: entry.value };
      }
      case "PUT": {
        const value = req.bytes();
        const ttlMs = number(param(req.path, "ttl_ms"));
        const absent = req.headers["if-none-match"] === "*";
        let v: number;
        if (ttlMs !== undefined) {
          const expect: kv.Expect = version !== undefined ? { version } : absent ? "absent" : "any";
          v = store.putWith(key, value, { expect, ttlMs });
        } else if (version !== undefined || absent) {
          v = store.compareAndSet(key, version, value);
        } else {
          v = store.put(key, value);
        }
        return { headers: { etag: String(v) }, body: `${v}\n` };
      }
      case "DELETE": {
        const removed = version === undefined ? store.delete(key) : store.deleteIf(key, version);
        return removed ? text(200, "deleted\n") : text(404, "no such key\n");
      }
      default:
        return text(405, "use GET, PUT or DELETE\n");
    }
  } catch (e) {
    return failure(e);
  }
}

function list(path: string): ActorResponse {
  try {
    const keys = store.list(param(path, "prefix") ?? "", undefined, 1000);
    return keys.map((k) => `${decoder.decode(k)}\n`).join("");
  } catch (e) {
    return failure(e);
  }
}

function lease(path: string): ActorResponse {
  const max = number(param(path, "max")) ?? 1;
  const visibility = number(param(path, "visibility_ms")) ?? 30_000;
  try {
    return inbox
      .lease(max, visibility)
      .map((m) => `${m.id} ${m.attempt} ${decoder.decode(m.body)}\n`)
      .join("");
  } catch (e) {
    return failure(e);
  }
}

function ack(path: string): ActorResponse {
  const id = number(param(path, "id"));
  const attempt = number(param(path, "attempt"));
  if (id === undefined || attempt === undefined) return text(400, "give id and attempt\n");
  try {
    return inbox.ack({ id, attempt }) ? text(200, "acked\n") : text(409, "that lease is no longer current\n");
  } catch (e) {
    return failure(e);
  }
}

function stats(q: queue.Client): ActorResponse {
  try {
    const s = q.stats();
    return `ready=${s.ready} delayed=${s.delayed} in_flight=${s.inFlight} dead=${s.dead}\n`;
  } catch (e) {
    return failure(e);
  }
}

function dead(q: queue.Client): ActorResponse {
  try {
    return q
      .deadLetters(100)
      .map((d) => `${d.message.id} ${d.message.attempt} ${d.reason}\n`)
      .join("");
  } catch (e) {
    return failure(e);
  }
}

function attempt(f: () => string): ActorResponse {
  try {
    return f();
  } catch (e) {
    return failure(e);
  }
}

/** The worker: one outcome per pushed job. Writing `job:<id>` is
 * idempotent, so a duplicate delivery does no harm. */
function work(message: queue.Message): queue.Outcome {
  const body = decoder.decode(message.body);
  if (body.startsWith("fail")) {
    console.warn(`job ${message.id}: asked to fail, attempt ${message.attempt}`);
    return "retry";
  }
  try {
    store.put(`job:${message.id}`, body.toUpperCase());
    return "ack";
  } catch {
    return "retry";
  }
}

export default defineActor({
  fetch(req) {
    if (queue.isDelivery(req)) return queue.handleDelivery(req, (_queue, m) => work(m));
    const route = req.path.split("?")[0];
    const get = req.method === "GET";
    const post = req.method === "POST";
    if (route === "/kv" && get) return list(req.path);
    if (route.startsWith("/kv/") && route.length > 4) return kvRoute(req, route.slice(4));
    if (route === "/jobs" && post) return attempt(() => `${jobs.send(req.bytes())}\n`);
    if (route === "/jobs/stats" && get) return stats(jobs);
    if (route === "/jobs/dead" && get) return dead(jobs);
    if (route === "/inbox" && post) return attempt(() => `${inbox.send(req.bytes())}\n`);
    if (route === "/inbox/batch" && post) {
      const lines = req.text().split("\n").filter((l) => l !== "");
      return attempt(() => inbox.sendBatch(lines).map((id) => `${id}\n`).join(""));
    }
    if (route === "/inbox/stats" && get) return stats(inbox);
    if (route === "/inbox/lease" && post) return lease(req.path);
    if (route === "/inbox/ack" && post) return ack(req.path);
    return text(404, "see the README for routes\n");
  },
});
