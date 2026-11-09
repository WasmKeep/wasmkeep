/* The key-value and queue clients and the push-delivery handler (ADR 0015):
 * the Rust SDK's kv::Client, queue::Client and queue::handle_delivery, over
 * wk_call_with_headers. See wasmkeep_kv.h; the framing is in wasmkeep_wire.c. */
#include "wasmkeep_wire.h"

#include <inttypes.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>

#define KV_PREFIX "/wasmkeep-kv/v1"
#define KV_EXPECT "x-wasmkeep-kv-expect"
#define KV_TTL "x-wasmkeep-kv-ttl-ms"
#define KV_DURABLE "x-wasmkeep-kv-durable"
#define QUEUE_PREFIX "/wasmkeep-queue/v1"
#define QUEUE_DELAY "x-wasmkeep-queue-delay-ms"
#define CALLER "x-wasmkeep-caller"

/* A malloc'd, printf-formatted string. */
static char *format(const char *fmt, ...) __attribute__((format(printf, 1, 2)));
static char *format(const char *fmt, ...) {
  va_list args, copy;
  va_start(args, fmt);
  va_copy(copy, args);
  int n = vsnprintf(NULL, 0, fmt, copy);
  va_end(copy);
  char *out = malloc(n < 0 ? 1 : (size_t)n + 1);
  if (!out) abort();
  if (n < 0) out[0] = 0;
  else vsnprintf(out, (size_t)n + 1, fmt, args);
  va_end(args);
  return out;
}

/* Why the last call to `address` failed, worded as the Rust SDK's
 * wire::call: the call failing, not the callee's answer. */
void wk_call_failure(char *out, size_t size, const char *address) {
  const char *kind = wk_error_kind();
  if (strcmp(kind, "invalid-address") == 0) snprintf(out, size, "invalid address %s: %s", address, wk_error());
  else if (strcmp(kind, "reentrant") == 0)
    snprintf(out, size, "call to %s would re-enter an actor already in this call chain", address);
  else if (strcmp(kind, "too-large") == 0) snprintf(out, size, "call to %s: body too large", address);
  else if (strcmp(kind, "timeout") == 0) snprintf(out, size, "call to %s timed out", address);
  else snprintf(out, size, "call to %s failed: %s", address, wk_error());
}

/* A refusal's text: `who answered <status>: <body, trimmed>`. */
static void refusal(char *out, size_t size, const char *who, uint16_t status, wk_bytes body) {
  const char *p = body.ptr;
  size_t n = body.len;
  while (n && (*p == ' ' || *p == '\t' || *p == '\n' || *p == '\r' || *p == '\f' || *p == '\v')) p++, n--;
  while (n && (p[n - 1] == ' ' || p[n - 1] == '\t' || p[n - 1] == '\n' || p[n - 1] == '\r' || p[n - 1] == '\f' ||
               p[n - 1] == '\v'))
    n--;
  snprintf(out, size, "%s answered %u: %.*s", who, (unsigned)status, (int)(n > 400 ? 400 : n), p);
}

static const char MALFORMED_SHARD[] = "malformed reply from shard";
static const char MALFORMED_QUEUE[] = "malformed reply from queue";

/* ------------------------------------------------------------------ kv */

static void kv_fail(wk_kv_error *err, wk_kv_error_kind kind, uint16_t status, const char *message) {
  if (!err) return;
  err->kind = kind;
  err->status = status;
  snprintf(err->message, sizeof err->message, "%s", message);
}

static void kv_conflict(wk_kv_error *err) { kv_fail(err, WK_KV_CONFLICT, 0, "conflict: the key changed"); }

static void kv_bad_reply(wk_kv_error *err) {
  if (!err) return;
  kv_fail(err, WK_KV_REFUSED, 200, "");
  snprintf(err->message, sizeof err->message, "shard answered 200: %s", MALFORMED_SHARD);
}

static void kv_refused(wk_kv_error *err, const wk_call_response *r) {
  if (!err) return;
  kv_fail(err, WK_KV_REFUSED, r->status, "");
  refusal(err->message, sizeof err->message, "shard", r->status, r->body);
}

static void kv_call_failed(wk_kv_error *err, const char *address) {
  if (!err) return;
  kv_fail(err, WK_KV_CALL, 0, "");
  wk_call_failure(err->message, sizeof err->message, address);
}

static bool check_key(wk_bytes key, wk_kv_error *err) {
  if (key.len <= WK_KV_MAX_KEY_LEN) return true;
  kv_fail(err, WK_KV_KEY_TOO_LONG, 0, "key longer than 1024 bytes");
  return false;
}

/* One per instance: lost with it (hibernation, a failed event, a deploy). */
static wk_kv_cache *cache(void) {
  static wk_kv_cache *c;
  if (!c) c = wk_kv_cache_new(4096);
  return c;
}

/* The cache's key: the class, a NUL, then the key. */
static wk_bytes cache_key(const wk_kv *kv, wk_bytes key) {
  size_t class_len = strlen(kv->class_name);
  char *k = malloc(class_len + 1 + key.len + 1);
  if (!k) abort();
  memcpy(k, kv->class_name, class_len + 1);
  if (key.len) memcpy(k + class_len + 1, key.ptr, key.len);
  wk_bytes out = {k, class_len + 1 + key.len};
  return out;
}

static void remember(const wk_kv *kv, wk_bytes key, const wk_kv_entry *entry) {
  if (!kv->cache_ms) return;
  wk_bytes k = cache_key(kv, key);
  wk_kv_cache_insert(cache(), k.ptr, k.len, (uint64_t)wk_now_ms(), entry);
  free((void *)k.ptr);
}

static void forget(const wk_kv *kv, wk_bytes key) {
  if (!kv->cache_ms) return;
  wk_bytes k = cache_key(kv, key);
  wk_kv_cache_remove(cache(), k.ptr, k.len);
  free((void *)k.ptr);
}

wk_kv wk_kv_client(const char *class_name, uint32_t shards) {
  wk_kv kv = {class_name, shards ? shards : 1, 0};
  return kv;
}

wk_kv wk_kv_with_read_cache(wk_kv kv, uint64_t max_age_ms) {
  kv.cache_ms = max_age_ms;
  return kv;
}

static char *shard_address(const wk_kv *kv, uint32_t index) {
  return format("%s/shard-%" PRIu32, kv->class_name, index);
}

/* The path of a key: KV_PREFIX/k/<hex>. The key is checked first, so it
 * fits. */
static void key_path(char out[sizeof KV_PREFIX "/k/" + 2 * WK_KV_MAX_KEY_LEN], wk_bytes key) {
  memcpy(out, KV_PREFIX "/k/", sizeof KV_PREFIX "/k/" - 1);
  wk_hex(out + sizeof KV_PREFIX "/k/" - 1, key.ptr, key.len);
}

int wk_kv_get(const wk_kv *kv, wk_bytes key, wk_kv_entry *entry, wk_kv_error *err) {
  if (kv->cache_ms) {
    wk_bytes k = cache_key(kv, key);
    int hit = wk_kv_cache_get(cache(), k.ptr, k.len, (uint64_t)wk_now_ms(), kv->cache_ms, entry);
    free((void *)k.ptr);
    if (hit >= 0) return hit;
  }
  return wk_kv_get_fresh(kv, key, entry, err);
}

int wk_kv_get_fresh(const wk_kv *kv, wk_bytes key, wk_kv_entry *entry, wk_kv_error *err) {
  if (!check_key(key, err)) return -1;
  char path[sizeof KV_PREFIX "/k/" + 2 * WK_KV_MAX_KEY_LEN];
  key_path(path, key);
  char *address = shard_address(kv, wk_kv_shard_for(key, kv->shards));
  wk_call_response r;
  int found = -1;
  if (!wk_call_with_headers(address, "GET", path, NULL, 0, NULL, 0, &r)) {
    kv_call_failed(err, address);
    free(address);
    return -1;
  }
  if (r.status == 200) {
    wk_kv_entry e;
    if (wk_kv_decode_entry(r.body.ptr, r.body.len, &e)) {
      remember(kv, key, &e);
      *entry = e;
      found = 1;
    } else {
      kv_bad_reply(err);
    }
  } else if (r.status == 404) {
    remember(kv, key, NULL);
    found = 0;
  } else {
    kv_refused(err, &r);
  }
  wk_call_response_free(&r);
  free(address);
  return found;
}

bool wk_kv_put(const wk_kv *kv, wk_bytes key, wk_bytes value, uint64_t *version, wk_kv_error *err) {
  return wk_kv_put_with(kv, key, value, NULL, version, err);
}

bool wk_kv_compare_and_set(const wk_kv *kv, wk_bytes key, const uint64_t *expected, wk_bytes value,
                           uint64_t *version, wk_kv_error *err) {
  wk_kv_put_options options = {0};
  options.expect = expected ? wk_kv_expect_version(*expected) : wk_kv_expect_absent();
  return wk_kv_put_with(kv, key, value, &options, version, err);
}

bool wk_kv_put_with(const wk_kv *kv, wk_bytes key, wk_bytes value, const wk_kv_put_options *options,
                    uint64_t *version, wk_kv_error *err) {
  static const wk_kv_put_options defaults = {0};
  if (!options) options = &defaults;
  if (!check_key(key, err)) return false;
  char expect[21], ttl[21];
  wk_header headers[3] = {
      {KV_EXPECT, wk_kv_expect_encode(options->expect, expect)},
      {KV_DURABLE, options->local ? "0" : "1"},
      {KV_TTL, ttl},
  };
  snprintf(ttl, sizeof ttl, "%" PRIu64, options->ttl_ms);
  char path[sizeof KV_PREFIX "/k/" + 2 * WK_KV_MAX_KEY_LEN];
  key_path(path, key);
  char *address = shard_address(kv, wk_kv_shard_for(key, kv->shards));
  uint64_t sent = (uint64_t)wk_now_ms();
  wk_call_response r;
  bool ok = false;
  if (!wk_call_with_headers(address, "PUT", path, headers, options->has_ttl ? 3 : 2, value.ptr, value.len, &r)) {
    forget(kv, key);
    kv_call_failed(err, address);
    free(address);
    return false;
  }
  if (r.status == 200) {
    wk_reader reader = wk_reader_of(r.body.ptr, r.body.len);
    uint64_t v;
    if (wk_read_u64(&reader, &v)) {
      uint64_t expires = 0;
      if (options->has_ttl) expires = sent > UINT64_MAX - options->ttl_ms ? UINT64_MAX : sent + options->ttl_ms;
      wk_kv_entry e = {value, v, expires};
      remember(kv, key, &e);
      if (version) *version = v;
      ok = true;
    } else {
      kv_bad_reply(err);
    }
  } else {
    forget(kv, key);
    if (r.status == 409) kv_conflict(err);
    else kv_refused(err, &r);
  }
  wk_call_response_free(&r);
  free(address);
  return ok;
}

static bool delete_with(const wk_kv *kv, wk_bytes key, wk_kv_expect expect, bool *removed, wk_kv_error *err) {
  if (!check_key(key, err)) return false;
  char value[21];
  wk_header headers[1] = {{KV_EXPECT, wk_kv_expect_encode(expect, value)}};
  char path[sizeof KV_PREFIX "/k/" + 2 * WK_KV_MAX_KEY_LEN];
  key_path(path, key);
  char *address = shard_address(kv, wk_kv_shard_for(key, kv->shards));
  wk_call_response r;
  bool ok = false;
  if (!wk_call_with_headers(address, "DELETE", path, headers, 1, NULL, 0, &r)) {
    forget(kv, key);
    kv_call_failed(err, address);
    free(address);
    return false;
  }
  if (r.status == 200) {
    wk_reader reader = wk_reader_of(r.body.ptr, r.body.len);
    uint64_t n;
    if (wk_read_u64(&reader, &n)) {
      remember(kv, key, NULL);
      if (removed) *removed = n == 1;
      ok = true;
    } else {
      kv_bad_reply(err);
    }
  } else {
    forget(kv, key);
    if (r.status == 409) kv_conflict(err);
    else kv_refused(err, &r);
  }
  wk_call_response_free(&r);
  free(address);
  return ok;
}

bool wk_kv_delete(const wk_kv *kv, wk_bytes key, bool *removed, wk_kv_error *err) {
  return delete_with(kv, key, wk_kv_expect_any(), removed, err);
}

bool wk_kv_delete_if(const wk_kv *kv, wk_bytes key, uint64_t version, bool *removed, wk_kv_error *err) {
  return delete_with(kv, key, wk_kv_expect_version(version), removed, err);
}

bool wk_kv_list(const wk_kv *kv, wk_bytes prefix, const wk_bytes *after, uint32_t limit, wk_kv_keys *out,
                wk_kv_error *err) {
  out->keys = NULL;
  out->count = 0;
  char *prefix_hex = malloc(2 * prefix.len + 1);
  char *after_hex = after ? malloc(2 * after->len + 1) : NULL;
  if (!prefix_hex || (after && !after_hex)) abort();
  wk_hex(prefix_hex, prefix.ptr, prefix.len);
  char *path = after ? format(KV_PREFIX "/list?prefix=%s&limit=%" PRIu32 "&after=%s", prefix_hex, limit,
                              wk_hex(after_hex, after->ptr, after->len))
                     : format(KV_PREFIX "/list?prefix=%s&limit=%" PRIu32, prefix_hex, limit);
  free(prefix_hex);
  free(after_hex);
  bool ok = true;
  for (uint32_t index = 0; ok && index < kv->shards; index++) {
    char *address = shard_address(kv, index);
    wk_call_response r;
    if (!wk_call_with_headers(address, "GET", path, NULL, 0, NULL, 0, &r)) {
      kv_call_failed(err, address);
      ok = false;
    } else {
      if (r.status != 200) {
        kv_refused(err, &r);
        ok = false;
      } else if (!wk_kv_decode_keys(r.body.ptr, r.body.len, out)) {
        kv_bad_reply(err);
        ok = false;
      }
      wk_call_response_free(&r);
    }
    free(address);
  }
  free(path);
  if (!ok) {
    wk_kv_keys_free(out);
    return false;
  }
  wk_kv_merge_keys(out, limit);
  return true;
}

/* --------------------------------------------------------------- queue */

static void queue_fail(wk_queue_error *err, wk_queue_error_kind kind, uint16_t status) {
  if (!err) return;
  err->kind = kind;
  err->status = status;
  err->message[0] = 0;
}

static void queue_bad_reply(wk_queue_error *err) {
  if (!err) return;
  queue_fail(err, WK_QUEUE_REFUSED, 200);
  snprintf(err->message, sizeof err->message, "queue answered 200: %s", MALFORMED_QUEUE);
}

wk_queue wk_queue_client(const char *address) {
  wk_queue q = {address};
  return q;
}

/* One call to the queue; true with *r (free it) when it answered 200. */
static bool queue_call(const wk_queue *q, const char *method, const char *path, const wk_header *headers,
                       size_t count, const void *body, size_t body_len, wk_call_response *r, wk_queue_error *err) {
  if (!wk_call_with_headers(q->address, method, path, headers, count, body, body_len, r)) {
    if (err) {
      queue_fail(err, WK_QUEUE_CALL, 0);
      wk_call_failure(err->message, sizeof err->message, q->address);
    }
    return false;
  }
  if (r->status != 200) {
    if (err) {
      queue_fail(err, WK_QUEUE_REFUSED, r->status);
      refusal(err->message, sizeof err->message, "queue", r->status, r->body);
    }
    wk_call_response_free(r);
    return false;
  }
  return true;
}

static bool send_batch_delayed(const wk_queue *q, const wk_bytes *bodies, size_t count, uint64_t delay_ms,
                               uint64_t *ids, wk_queue_error *err) {
  char delay[21];
  snprintf(delay, sizeof delay, "%" PRIu64, delay_ms);
  wk_header headers[1] = {{QUEUE_DELAY, delay}};
  wk_writer w = {0};
  wk_queue_encode_bodies(&w, bodies, count);
  wk_call_response r;
  bool ok = queue_call(q, "POST", QUEUE_PREFIX "/send", headers, 1, w.ptr, w.len, &r, err);
  free(w.ptr);
  if (!ok) return false;
  uint64_t *got = NULL;
  size_t n = 0;
  ok = wk_queue_decode_counts(r.body.ptr, r.body.len, &got, &n) && n == count;
  wk_call_response_free(&r);
  if (ok && ids && n) memcpy(ids, got, n * sizeof *ids);
  free(got);
  if (!ok) queue_bad_reply(err);
  return ok;
}

bool wk_queue_send(const wk_queue *q, wk_bytes body, uint64_t *id, wk_queue_error *err) {
  return wk_queue_send_delayed(q, body, 0, id, err);
}

bool wk_queue_send_delayed(const wk_queue *q, wk_bytes body, uint64_t delay_ms, uint64_t *id,
                           wk_queue_error *err) {
  uint64_t got;
  if (!send_batch_delayed(q, &body, 1, delay_ms, &got, err)) return false;
  if (id) *id = got;
  return true;
}

bool wk_queue_send_batch(const wk_queue *q, const wk_bytes *bodies, size_t count, uint64_t *ids,
                         wk_queue_error *err) {
  return send_batch_delayed(q, bodies, count, 0, ids, err);
}

bool wk_queue_lease(const wk_queue *q, uint32_t max, uint64_t visibility_ms, wk_queue_messages *out,
                    wk_queue_error *err) {
  out->items = NULL;
  out->count = 0;
  char *path = format(QUEUE_PREFIX "/lease?max=%" PRIu32 "&visibility_ms=%" PRIu64, max, visibility_ms);
  wk_call_response r;
  bool ok = queue_call(q, "POST", path, NULL, 0, NULL, 0, &r, err);
  free(path);
  if (!ok) return false;
  ok = wk_queue_decode_messages(r.body.ptr, r.body.len, out);
  wk_call_response_free(&r);
  if (!ok) queue_bad_reply(err);
  return ok;
}

bool wk_queue_settle(const wk_queue *q, const wk_queue_settlement *items, size_t count, bool *current,
                     wk_queue_error *err) {
  wk_writer w = {0};
  wk_queue_encode_settlements(&w, items, count);
  wk_call_response r;
  bool ok = queue_call(q, "POST", QUEUE_PREFIX "/settle", NULL, 0, w.ptr, w.len, &r, err);
  free(w.ptr);
  if (!ok) return false;
  uint64_t *flags = NULL;
  size_t n = 0;
  ok = wk_queue_decode_counts(r.body.ptr, r.body.len, &flags, &n);
  wk_call_response_free(&r);
  if (ok && current)
    for (size_t i = 0; i < count; i++) current[i] = i < n && flags[i] == 1;
  free(flags);
  if (!ok) queue_bad_reply(err);
  return ok;
}

bool wk_queue_ack(const wk_queue *q, uint64_t id, uint32_t attempt, bool *current, wk_queue_error *err) {
  wk_queue_settlement s = {id, attempt, wk_queue_ack_outcome()};
  return wk_queue_settle(q, &s, 1, current, err);
}

bool wk_queue_retry(const wk_queue *q, uint64_t id, uint32_t attempt, const uint64_t *delay_ms, bool *current,
                    wk_queue_error *err) {
  wk_queue_settlement s = {id, attempt, delay_ms ? wk_queue_retry_after(*delay_ms) : wk_queue_retry_outcome()};
  return wk_queue_settle(q, &s, 1, current, err);
}

bool wk_queue_get_stats(const wk_queue *q, wk_queue_stats *out, wk_queue_error *err) {
  wk_call_response r;
  if (!queue_call(q, "GET", QUEUE_PREFIX "/stats", NULL, 0, NULL, 0, &r, err)) return false;
  uint64_t *counts = NULL;
  size_t n = 0;
  bool ok = wk_queue_decode_counts(r.body.ptr, r.body.len, &counts, &n) && n == 4;
  wk_call_response_free(&r);
  if (ok) {
    out->ready = counts[0];
    out->delayed = counts[1];
    out->in_flight = counts[2];
    out->dead = counts[3];
  } else {
    queue_bad_reply(err);
  }
  free(counts);
  return ok;
}

bool wk_queue_get_dead_letters(const wk_queue *q, uint32_t limit, wk_queue_dead_letters *out,
                               wk_queue_error *err) {
  out->items = NULL;
  out->count = 0;
  char *path = format(QUEUE_PREFIX "/dead?limit=%" PRIu32, limit);
  wk_call_response r;
  bool ok = queue_call(q, "GET", path, NULL, 0, NULL, 0, &r, err);
  free(path);
  if (!ok) return false;
  ok = wk_queue_decode_dead(r.body.ptr, r.body.len, out);
  wk_call_response_free(&r);
  if (!ok) queue_bad_reply(err);
  return ok;
}

/* ------------------------------------------------------------ delivery */

bool wk_queue_is_delivery(const wk_request *req) {
  return strcmp(req->method, "POST") == 0 && strcmp(req->path, WK_QUEUE_DELIVER_PATH) == 0;
}

wk_response wk_queue_handle_delivery(wk_request *req, wk_queue_handler handle, void *ctx) {
  static const char forbidden[] = "deliveries come from queue actors\n";
  static const char malformed[] = "malformed delivery\n";
  static const char failed[] = "the delivery handler failed\n";
  const char *caller = wk_request_header(req, CALLER);
  if (!caller) return wk_reply_static(403, "text/plain", forbidden, sizeof forbidden - 1);
  /* A copy: the handler may read other headers, which reuses the buffer. */
  wk_bytes queue = wk_bytes_dup(caller, strlen(caller));
  wk_bytes body = wk_request_body(req);
  wk_queue_messages messages;
  if (!wk_queue_decode_messages(body.ptr, body.len, &messages)) {
    free((void *)queue.ptr);
    return wk_reply_static(400, "text/plain", malformed, sizeof malformed - 1);
  }
  wk_queue_settlement *outcomes = calloc(messages.count ? messages.count : 1, sizeof *outcomes);
  if (!outcomes) abort();
  bool ok = true;
  for (size_t i = 0; ok && i < messages.count; i++) {
    const wk_queue_message *m = &messages.items[i];
    outcomes[i].id = m->id;
    outcomes[i].attempt = m->attempt;
    outcomes[i].outcome = wk_queue_ack_outcome();
    ok = handle(queue.ptr, m, &outcomes[i].outcome, ctx);
  }
  wk_response response;
  if (ok) {
    wk_writer w = {0};
    wk_queue_encode_settlements(&w, outcomes, messages.count);
    response = wk_reply_copy(200, "application/octet-stream", w.ptr, w.len);
    free(w.ptr);
  } else {
    /* As a Rust or TypeScript handler that panics or throws: the queue
     * retries the whole batch with its backoff. */
    response = wk_reply_static(500, "text/plain", failed, sizeof failed - 1);
  }
  free(outcomes);
  wk_queue_messages_free(&messages);
  free((void *)queue.ptr);
  return response;
}
