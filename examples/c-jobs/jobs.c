/* The app class of the jobs example, in C: an HTTP API over the C SDK's kv
 * and queue clients, and the consumer the `jobs` queue pushes to. It talks
 * to the Rust shard and queue classes of examples/jobs (`kv` and `queue`)
 * and must be deployed as class `app`: `queue/jobs` delivers to
 * `app/worker`. The routes and bodies are examples/ts-jobs's.
 *
 * Key-value (keys spread over 4 shards of class `kv`):
 *
 * - `GET /kv/<key>`: the value, with its version in `etag`; 404 if absent.
 *   `?cached=1` reads through this actor's 60 s read cache.
 * - `PUT /kv/<key>`: stores the body and answers the new version.
 *   `if-match: <version>` or `if-none-match: *` make it a compare-and-set
 *   (409 when it does not hold); `?ttl_ms=N` expires it.
 * - `DELETE /kv/<key>`: 200 if it was there, 404 otherwise; with
 *   `if-match: <version>`, only if it holds that version (409 otherwise).
 * - `GET /kv?prefix=P`: the keys starting with `P`, one per line.
 *
 * Queues (actors of class `queue`):
 *
 * - `POST /jobs`: queues the body on `queue/jobs`, which pushes it to
 *   `app/worker`. The worker stores the body in upper case at key
 *   `job:<id>`; a body starting with `fail` asks for a retry every time, so
 *   it ends in the dead-letter table after three attempts.
 * - `GET /jobs/stats`, `GET /jobs/dead`: counts and dead letters.
 * - `POST /inbox`: queues the body on `queue/inbox`, a pull queue.
 * - `POST /inbox/batch`: queues each line of the body, in one write;
 *   answers their ids, one per line.
 * - `POST /inbox/lease?visibility_ms=N&max=M`: leases up to `M` (default
 *   one) messages, answered as `<id> <attempt> <body>` lines.
 * - `POST /inbox/ack?id=I&attempt=A`: 200 if that lease was current, 409
 *   if it ran out (the message was leased again) or was settled.
 *
 * Workflows (instances of the Rust `orders` workflow class of
 * examples/workflows, which calls its `ledger` class), as
 * examples/workflows/app:
 *
 * - `POST /orders/<id>`: starts order `<id>` with the body as its input;
 *   answers its status. Starting an id again answers the existing order's.
 * - `GET /orders/<id>`: its status, one `key: value` per line; 404 if there
 *   is none.
 * - `POST /orders/<id>/approve`: delivers the `approved` event, then
 *   answers the status (409 if the order has finished).
 * - `POST /orders/<id>/cancel`: cancels it; answers its status. */
#include <ctype.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>

#include "wasmkeep.h"

#define SHARDS 4

/* Keys over 4 shards, with a 60 s read cache: wk_kv_get may use it, writes
 * through this client update it, wk_kv_get_fresh skips it. */
static const wk_kv store = {.class_name = "kv", .shards = SHARDS, .cache_ms = 60000};
static const wk_queue jobs = {.address = "queue/jobs"};
static const wk_queue inbox = {.address = "queue/inbox"};
static const wk_workflow orders = {.class_name = "orders"};

/* ------------------------------------------------------------- helpers */

/* A growing text buffer for multi-line bodies. */
typedef struct text {
  char *ptr;
  size_t len, cap;
} text;

static void append(text *t, const char *p, size_t n) {
  if (t->cap - t->len < n + 1) {
    size_t cap = t->cap ? t->cap : 256;
    while (cap - t->len < n + 1) cap *= 2;
    char *grown = realloc(t->ptr, cap);
    if (!grown) abort();
    t->ptr = grown;
    t->cap = cap;
  }
  memcpy(t->ptr + t->len, p, n);
  t->len += n;
  t->ptr[t->len] = 0;
}

static void appendf(text *t, const char *fmt, ...) __attribute__((format(printf, 2, 3)));
static void appendf(text *t, const char *fmt, ...) {
  char buf[64];
  va_list args;
  va_start(args, fmt);
  int n = vsnprintf(buf, sizeof buf, fmt, args);
  va_end(args);
  if (n > 0) append(t, buf, (size_t)n < sizeof buf ? (size_t)n : sizeof buf - 1);
}

static wk_response reply_text(text *t) {
  wk_response r = wk_reply_copy(200, NULL, t->ptr ? t->ptr : "", t->len);
  free(t->ptr);
  return r;
}

/* The value of `name` in the path's query (`?a=1&b=2`), or false. */
static bool param(const char *path, const char *name, wk_bytes *out) {
  const char *q = strchr(path, '?');
  if (!q) return false;
  size_t name_len = strlen(name);
  for (const char *pair = q + 1; *pair;) {
    size_t len = strcspn(pair, "&");
    size_t key_len = strcspn(pair, "=");
    if (key_len > len) key_len = len;
    if (key_len == name_len && memcmp(pair, name, name_len) == 0) {
      const char *v = key_len < len ? pair + key_len + 1 : pair + len;
      /* As the TypeScript app's split("="): up to the next '='. */
      size_t v_len = key_len < len ? strcspn(v, "=&") : 0;
      *out = wk_bytes_of(v, v_len);
      return true;
    }
    pair += len;
    if (*pair == '&') pair++;
  }
  return false;
}

/* A non-negative integer, or false. */
static bool number(wk_bytes s, uint64_t *out) {
  if (s.len == 0) return false;
  uint64_t v = 0;
  for (size_t i = 0; i < s.len; i++) {
    if (s.ptr[i] < '0' || s.ptr[i] > '9') return false;
    uint64_t d = (uint64_t)(s.ptr[i] - '0');
    if (v > (UINT64_MAX - d) / 10) return false;
    v = v * 10 + d;
  }
  *out = v;
  return true;
}

static bool number_param(const char *path, const char *name, uint64_t *out) {
  wk_bytes v;
  return param(path, name, &v) && number(v, out);
}

static wk_response kv_failure(const wk_kv_error *err) {
  if (err->kind == WK_KV_CONFLICT) return wk_reply(409, "the key changed\n");
  return wk_reply(502, "%s\n", err->message);
}

static wk_response queue_failure(const wk_queue_error *err) { return wk_reply(502, "%s\n", err->message); }

static wk_response with_etag(wk_response r, uint64_t version) {
  char etag[21];
  snprintf(etag, sizeof etag, "%" PRIu64, version);
  wk_response_header(&r, "etag", etag);
  return r;
}

/* ------------------------------------------------------------------ kv */

static wk_response kv_route(wk_request *req, wk_bytes key) {
  /* Read if-match first: each wk_request_header call reuses the buffer. */
  const char *match = wk_request_header(req, "if-match");
  bool has_version = false;
  uint64_t version = 0;
  if (match) {
    char digits[32];
    size_t n = 0;
    for (const char *p = match; *p && n < sizeof digits - 1; p++)
      if (*p != '"') digits[n++] = *p;
    digits[n] = 0;
    if (!number(wk_bytes_of(digits, n), &version)) return wk_reply(400, "if-match is not a version\n");
    has_version = true;
  }
  wk_kv_error err;
  if (strcmp(req->method, "GET") == 0) {
    wk_bytes cached;
    bool use_cache = param(req->path, "cached", &cached) && cached.len == 1 && cached.ptr[0] == '1';
    wk_kv_entry entry;
    int found = use_cache ? wk_kv_get(&store, key, &entry, &err) : wk_kv_get_fresh(&store, key, &entry, &err);
    if (found < 0) return kv_failure(&err);
    if (found == 0) return wk_reply(404, "no such key\n");
    wk_response r = wk_reply_copy(200, "application/octet-stream", entry.value.ptr, entry.value.len);
    r = with_etag(r, entry.version);
    wk_kv_entry_free(&entry);
    return r;
  }
  if (strcmp(req->method, "PUT") == 0) {
    const char *none_match = wk_request_header(req, "if-none-match");
    bool absent = none_match && strcmp(none_match, "*") == 0;
    wk_kv_put_options options = {0};
    options.expect = has_version ? wk_kv_expect_version(version) : absent ? wk_kv_expect_absent() : wk_kv_expect_any();
    options.has_ttl = number_param(req->path, "ttl_ms", &options.ttl_ms);
    wk_bytes value = wk_request_body(req);
    uint64_t v;
    if (!wk_kv_put_with(&store, key, value, &options, &v, &err)) return kv_failure(&err);
    return with_etag(wk_reply(200, "%" PRIu64 "\n", v), v);
  }
  if (strcmp(req->method, "DELETE") == 0) {
    bool removed;
    bool ok = has_version ? wk_kv_delete_if(&store, key, version, &removed, &err)
                          : wk_kv_delete(&store, key, &removed, &err);
    if (!ok) return kv_failure(&err);
    return removed ? wk_reply(200, "deleted\n") : wk_reply(404, "no such key\n");
  }
  return wk_reply(405, "use GET, PUT or DELETE\n");
}

static wk_response list(const char *path) {
  wk_bytes prefix = wk_str("");
  param(path, "prefix", &prefix);
  wk_kv_keys keys;
  wk_kv_error err;
  if (!wk_kv_list(&store, prefix, NULL, 1000, &keys, &err)) return kv_failure(&err);
  text t = {0};
  for (size_t i = 0; i < keys.count; i++) {
    append(&t, keys.keys[i].ptr, keys.keys[i].len);
    append(&t, "\n", 1);
  }
  wk_kv_keys_free(&keys);
  return reply_text(&t);
}

/* --------------------------------------------------------------- queue */

static wk_response send_one(const wk_queue *q, wk_request *req) {
  uint64_t id;
  wk_queue_error err;
  if (!wk_queue_send(q, wk_request_body(req), &id, &err)) return queue_failure(&err);
  return wk_reply(200, "%" PRIu64 "\n", id);
}

static wk_response send_batch(wk_request *req) {
  wk_bytes body = wk_request_body(req);
  wk_bytes *lines = calloc(body.len / 2 + 1, sizeof *lines);
  if (!lines) abort();
  size_t count = 0;
  for (size_t at = 0; at < body.len;) {
    const char *nl = memchr(body.ptr + at, '\n', body.len - at);
    size_t len = nl ? (size_t)(nl - (body.ptr + at)) : body.len - at;
    if (len) lines[count++] = wk_bytes_of(body.ptr + at, len);
    at += len + 1;
  }
  uint64_t *ids = calloc(count ? count : 1, sizeof *ids);
  if (!ids) abort();
  wk_queue_error err;
  wk_response r;
  if (!wk_queue_send_batch(&inbox, lines, count, ids, &err)) {
    r = queue_failure(&err);
  } else {
    text t = {0};
    for (size_t i = 0; i < count; i++) appendf(&t, "%" PRIu64 "\n", ids[i]);
    r = reply_text(&t);
  }
  free(ids);
  free(lines);
  return r;
}

static wk_response lease(const char *path) {
  uint64_t max = 1, visibility = 30000;
  number_param(path, "max", &max);
  number_param(path, "visibility_ms", &visibility);
  wk_queue_messages messages;
  wk_queue_error err;
  if (!wk_queue_lease(&inbox, max > UINT32_MAX ? UINT32_MAX : (uint32_t)max, visibility, &messages, &err))
    return queue_failure(&err);
  text t = {0};
  for (size_t i = 0; i < messages.count; i++) {
    const wk_queue_message *m = &messages.items[i];
    appendf(&t, "%" PRIu64 " %" PRIu32 " ", m->id, m->attempt);
    append(&t, m->body.ptr, m->body.len);
    append(&t, "\n", 1);
  }
  wk_queue_messages_free(&messages);
  return reply_text(&t);
}

static wk_response ack(const char *path) {
  uint64_t id, attempt;
  if (!number_param(path, "id", &id) || !number_param(path, "attempt", &attempt) || attempt > UINT32_MAX)
    return wk_reply(400, "give id and attempt\n");
  bool current;
  wk_queue_error err;
  if (!wk_queue_ack(&inbox, id, (uint32_t)attempt, &current, &err)) return queue_failure(&err);
  return current ? wk_reply(200, "acked\n") : wk_reply(409, "that lease is no longer current\n");
}

static wk_response stats(const wk_queue *q) {
  wk_queue_stats s;
  wk_queue_error err;
  if (!wk_queue_get_stats(q, &s, &err)) return queue_failure(&err);
  return wk_reply(200, "ready=%" PRIu64 " delayed=%" PRIu64 " in_flight=%" PRIu64 " dead=%" PRIu64 "\n", s.ready,
                  s.delayed, s.in_flight, s.dead);
}

static wk_response dead(const wk_queue *q) {
  wk_queue_dead_letters items;
  wk_queue_error err;
  if (!wk_queue_get_dead_letters(q, 100, &items, &err)) return queue_failure(&err);
  text t = {0};
  for (size_t i = 0; i < items.count; i++) {
    const wk_queue_dead_letter *d = &items.items[i];
    appendf(&t, "%" PRIu64 " %" PRIu32 " ", d->message.id, d->message.attempt);
    append(&t, d->reason.ptr, d->reason.len);
    append(&t, "\n", 1);
  }
  wk_queue_dead_letters_free(&items);
  return reply_text(&t);
}

/* The worker: one outcome per pushed job. Writing `job:<id>` is
 * idempotent, so a duplicate delivery does no harm. (Upper case is ASCII
 * here; the TypeScript app's toUpperCase covers all of Unicode.) */
static bool work(const char *queue, const wk_queue_message *m, wk_queue_outcome *outcome, void *ctx) {
  (void)queue;
  (void)ctx;
  if (m->body.len >= 4 && memcmp(m->body.ptr, "fail", 4) == 0) {
    *outcome = wk_queue_retry_outcome();
    return true;
  }
  char key[32];
  snprintf(key, sizeof key, "job:%" PRIu64, m->id);
  char *upper = malloc(m->body.len + 1);
  if (!upper) abort();
  for (size_t i = 0; i < m->body.len; i++) upper[i] = (char)toupper((unsigned char)m->body.ptr[i]);
  bool ok = wk_kv_put(&store, wk_str(key), wk_bytes_of(upper, m->body.len), NULL, NULL);
  free(upper);
  *outcome = ok ? wk_queue_ack_outcome() : wk_queue_retry_outcome();
  return true;
}

/* ----------------------------------------------------------- workflows */

/* A status as examples/workflows/app shows it: `key: value` lines. */
static wk_response show(const wk_workflow_status *s) {
  text t = {0};
  appendf(&t, "state: %s\npositions: %" PRIu64 "\n", wk_workflow_state_name(s->state), s->positions);
  if (s->wake_at) appendf(&t, "wake_at: %" PRIu64 "\n", s->wake_at);
  if (s->waiting_count) {
    append(&t, "waiting_for: ", 13);
    for (size_t i = 0; i < s->waiting_count; i++) {
      if (i) append(&t, ",", 1);
      append(&t, s->waiting_for[i].ptr, s->waiting_for[i].len);
    }
    append(&t, "\n", 1);
  }
  if (s->has_output) {
    append(&t, "output: ", 8);
    append(&t, s->output.ptr, s->output.len);
    append(&t, "\n", 1);
  }
  if (s->error.len) {
    append(&t, "error: ", 7);
    append(&t, s->error.ptr, s->error.len);
    append(&t, "\n", 1);
  }
  return reply_text(&t);
}

static wk_response order_failure(const wk_workflow_error *err) {
  if (err->kind == WK_WORKFLOW_UNKNOWN) return wk_reply(404, "no such order\n");
  if (err->kind == WK_WORKFLOW_FINISHED) return wk_reply(409, "the order has finished\n");
  return wk_reply(502, "%s\n", err->message);
}

/* `rest` is what follows `/orders/`: `<id>` or `<id>/<action>`. */
static wk_response order_route(wk_request *req, const char *rest) {
  const char *slash = strchr(rest, '/');
  wk_bytes id = wk_bytes_of(rest, slash ? (size_t)(slash - rest) : strlen(rest));
  const char *action = slash ? slash + 1 : NULL;
  if (id.len == 0) return wk_reply(404, "see the README for routes\n");
  bool get = strcmp(req->method, "GET") == 0;
  bool post = strcmp(req->method, "POST") == 0;
  wk_workflow_status s = {0};
  wk_workflow_error err;
  bool ok;
  if (!action && post) {
    ok = wk_workflow_start(&orders, id, wk_request_body(req), &s, &err);
  } else if (!action && get) {
    ok = wk_workflow_get_status(&orders, id, &s, &err);
  } else if (action && strcmp(action, "approve") == 0 && post) {
    ok = wk_workflow_send_event(&orders, id, wk_str("approved"), wk_str(""), &err) &&
         wk_workflow_get_status(&orders, id, &s, &err);
  } else if (action && strcmp(action, "cancel") == 0 && post) {
    ok = wk_workflow_cancel(&orders, id, &s, &err);
  } else {
    return wk_reply(404, "see the README for routes\n");
  }
  if (!ok) return order_failure(&err);
  wk_response r = show(&s);
  wk_workflow_status_free(&s);
  return r;
}

/* -------------------------------------------------------------- routes */

wk_response wk_fetch(wk_request *req) {
  if (wk_queue_is_delivery(req)) return wk_queue_handle_delivery(req, work, NULL);
  size_t route_len = strcspn(req->path, "?");
  char *route = malloc(route_len + 1);
  if (!route) abort();
  memcpy(route, req->path, route_len);
  route[route_len] = 0;
  bool get = strcmp(req->method, "GET") == 0;
  bool post = strcmp(req->method, "POST") == 0;
  wk_response r;
  if (strcmp(route, "/kv") == 0 && get) r = list(req->path);
  else if (strncmp(route, "/kv/", 4) == 0 && route_len > 4) r = kv_route(req, wk_bytes_of(route + 4, route_len - 4));
  else if (strcmp(route, "/jobs") == 0 && post) r = send_one(&jobs, req);
  else if (strcmp(route, "/jobs/stats") == 0 && get) r = stats(&jobs);
  else if (strcmp(route, "/jobs/dead") == 0 && get) r = dead(&jobs);
  else if (strcmp(route, "/inbox") == 0 && post) r = send_one(&inbox, req);
  else if (strcmp(route, "/inbox/batch") == 0 && post) r = send_batch(req);
  else if (strcmp(route, "/inbox/stats") == 0 && get) r = stats(&inbox);
  else if (strcmp(route, "/inbox/lease") == 0 && post) r = lease(req->path);
  else if (strcmp(route, "/inbox/ack") == 0 && post) r = ack(req->path);
  else if (strncmp(route, "/orders/", 8) == 0) r = order_route(req, route + 8);
  else r = wk_reply(404, "see the README for routes\n");
  free(route);
  return r;
}
