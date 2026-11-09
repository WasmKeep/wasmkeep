/* The framing, codecs, shard hash and read cache of the kv and queue
 * clients, and the workflow status codec: pure, libc only. See wasmkeep_wire.h. */
#include "wasmkeep_wire.h"

#include <stdlib.h>

static void *xrealloc(void *p, size_t n) {
  void *out = realloc(p, n ? n : 1);
  if (!out) abort();
  return out;
}

/* ------------------------------------------------------------- framing */

static void reserve(wk_writer *w, size_t more) {
  if (w->cap - w->len >= more) return;
  size_t cap = w->cap ? w->cap : 64;
  while (cap - w->len < more) cap *= 2;
  w->ptr = xrealloc(w->ptr, cap);
  w->cap = cap;
}

void wk_write_u64(wk_writer *w, uint64_t v) {
  reserve(w, 8);
  for (int i = 7; i >= 0; i--) w->ptr[w->len++] = (uint8_t)(v >> (8 * i));
}

void wk_write_bytes(wk_writer *w, const void *p, size_t len) {
  /* Bodies are capped at 16 MB by actors.call, so a length always fits. */
  uint32_t n = len > UINT32_MAX ? UINT32_MAX : (uint32_t)len;
  reserve(w, 4 + (size_t)n);
  for (int i = 3; i >= 0; i--) w->ptr[w->len++] = (uint8_t)(n >> (8 * i));
  if (n) memcpy(w->ptr + w->len, p, n);
  w->len += n;
}

bool wk_read_u64(wk_reader *r, uint64_t *v) {
  if (r->len < 8) return false;
  uint64_t out = 0;
  for (int i = 0; i < 8; i++) out = out << 8 | r->ptr[i];
  r->ptr += 8;
  r->len -= 8;
  *v = out;
  return true;
}

bool wk_read_bytes(wk_reader *r, const uint8_t **p, size_t *len) {
  if (r->len < 4) return false;
  uint32_t n = (uint32_t)r->ptr[0] << 24 | (uint32_t)r->ptr[1] << 16 | (uint32_t)r->ptr[2] << 8 | r->ptr[3];
  /* A length that runs past the end is refused, not truncated. */
  if (r->len - 4 < n) return false;
  *p = r->ptr + 4;
  *len = n;
  r->ptr += 4 + (size_t)n;
  r->len -= 4 + (size_t)n;
  return true;
}

char *wk_hex(char *out, const void *p, size_t len) {
  static const char digits[] = "0123456789abcdef";
  const uint8_t *b = p;
  for (size_t i = 0; i < len; i++) {
    out[2 * i] = digits[b[i] >> 4];
    out[2 * i + 1] = digits[b[i] & 0x0f];
  }
  out[2 * len] = 0;
  return out;
}

wk_bytes wk_bytes_dup(const void *p, size_t len) {
  char *out = xrealloc(NULL, len + 1);
  if (len) memcpy(out, p, len);
  out[len] = 0;
  wk_bytes b = {out, len};
  return b;
}

int wk_bytes_compare(wk_bytes a, wk_bytes b) {
  size_t n = a.len < b.len ? a.len : b.len;
  int c = n ? memcmp(a.ptr, b.ptr, n) : 0;
  if (c) return c;
  return a.len < b.len ? -1 : a.len > b.len ? 1 : 0;
}

bool wk_utf8_valid(const void *p, size_t len) {
  const uint8_t *s = p;
  size_t i = 0;
  while (i < len) {
    uint8_t c = s[i];
    size_t n;
    uint32_t cp;
    if (c < 0x80) {
      i++;
      continue;
    } else if ((c & 0xe0) == 0xc0) {
      n = 1;
      cp = c & 0x1f;
    } else if ((c & 0xf0) == 0xe0) {
      n = 2;
      cp = c & 0x0f;
    } else if ((c & 0xf8) == 0xf0) {
      n = 3;
      cp = c & 0x07;
    } else {
      return false;
    }
    if (len - i - 1 < n) return false;
    for (size_t k = 1; k <= n; k++) {
      if ((s[i + k] & 0xc0) != 0x80) return false;
      cp = cp << 6 | (s[i + k] & 0x3f);
    }
    /* Overlong forms, surrogates and code points past U+10FFFF. */
    if ((n == 1 && cp < 0x80) || (n == 2 && cp < 0x800) || (n == 3 && cp < 0x10000)) return false;
    if ((cp >= 0xd800 && cp <= 0xdfff) || cp > 0x10ffff) return false;
    i += n + 1;
  }
  return true;
}

/* ------------------------------------------------------------------ kv */

uint32_t wk_kv_shard_for(wk_bytes key, uint32_t shards) {
  uint64_t hash = 0xcbf29ce484222325ULL;
  const uint8_t *b = (const uint8_t *)key.ptr;
  for (size_t i = 0; i < key.len; i++) {
    hash ^= b[i];
    hash *= 0x00000100000001b3ULL;
  }
  /* The remainder is below `shards`, so it fits. */
  return (uint32_t)(hash % (shards ? shards : 1));
}

bool wk_kv_expect_allows(wk_kv_expect expect, bool present, uint64_t current) {
  switch (expect.kind) {
    case WK_KV_EXPECT_ANY: return true;
    case WK_KV_EXPECT_ABSENT: return !present;
    case WK_KV_EXPECT_VERSION: return present && current == expect.version;
  }
  return false;
}

const char *wk_kv_expect_encode(wk_kv_expect expect, char out[21]) {
  switch (expect.kind) {
    case WK_KV_EXPECT_ABSENT: memcpy(out, "absent", 7); return out;
    case WK_KV_EXPECT_VERSION: {
      char digits[21];
      size_t n = 0;
      uint64_t v = expect.version;
      do {
        digits[n++] = (char)('0' + v % 10);
        v /= 10;
      } while (v);
      for (size_t i = 0; i < n; i++) out[i] = digits[n - 1 - i];
      out[n] = 0;
      return out;
    }
    case WK_KV_EXPECT_ANY: break;
  }
  memcpy(out, "any", 4);
  return out;
}

bool wk_kv_expect_decode(const char *s, wk_kv_expect *out) {
  if (strcmp(s, "any") == 0) {
    *out = wk_kv_expect_any();
    return true;
  }
  if (strcmp(s, "absent") == 0) {
    *out = wk_kv_expect_absent();
    return true;
  }
  if (*s == '+') s++;
  if (!*s) return false;
  uint64_t v = 0;
  for (; *s; s++) {
    if (*s < '0' || *s > '9') return false;
    uint64_t d = (uint64_t)(*s - '0');
    if (v > (UINT64_MAX - d) / 10) return false;
    v = v * 10 + d;
  }
  *out = wk_kv_expect_version(v);
  return true;
}

void wk_kv_entry_free(wk_kv_entry *entry) {
  if (!entry) return;
  free((void *)entry->value.ptr);
  memset(entry, 0, sizeof *entry);
}

void wk_kv_encode_entry(wk_writer *w, const wk_kv_entry *entry) {
  wk_write_u64(w, entry->version);
  wk_write_u64(w, entry->expires_at);
  wk_write_bytes(w, entry->value.ptr, entry->value.len);
}

bool wk_kv_decode_entry(const void *p, size_t len, wk_kv_entry *out) {
  wk_reader r = wk_reader_of(p, len);
  uint64_t version, expires;
  const uint8_t *value;
  size_t value_len;
  if (!wk_read_u64(&r, &version) || !wk_read_u64(&r, &expires) || !wk_read_bytes(&r, &value, &value_len) ||
      !wk_reader_empty(&r))
    return false;
  out->version = version;
  out->expires_at = expires;
  out->value = wk_bytes_dup(value, value_len);
  return true;
}

void wk_kv_keys_free(wk_kv_keys *keys) {
  if (!keys) return;
  for (size_t i = 0; i < keys->count; i++) free((void *)keys->keys[i].ptr);
  free(keys->keys);
  keys->keys = NULL;
  keys->count = 0;
}

static void keys_push(wk_kv_keys *keys, size_t *cap, wk_bytes b) {
  if (keys->count == *cap) {
    *cap = *cap ? *cap * 2 : 8;
    keys->keys = xrealloc(keys->keys, *cap * sizeof *keys->keys);
  }
  keys->keys[keys->count++] = b;
}

void wk_kv_encode_keys(wk_writer *w, const wk_bytes *keys, size_t count) {
  wk_write_u64(w, count);
  for (size_t i = 0; i < count; i++) wk_write_bytes(w, keys[i].ptr, keys[i].len);
}

/* A count, then that many byte strings, into a fresh list. Grows as it
 * reads, so a count promising more than the buffer holds fails at the end
 * of the buffer instead of allocating it. */
static bool decode_byte_list(const void *p, size_t len, wk_kv_keys *out) {
  wk_reader r = wk_reader_of(p, len);
  wk_kv_keys keys = {NULL, 0};
  size_t cap = 0;
  uint64_t n;
  if (!wk_read_u64(&r, &n)) return false;
  for (uint64_t i = 0; i < n; i++) {
    const uint8_t *b;
    size_t b_len;
    if (!wk_read_bytes(&r, &b, &b_len)) {
      wk_kv_keys_free(&keys);
      return false;
    }
    keys_push(&keys, &cap, wk_bytes_dup(b, b_len));
  }
  if (!wk_reader_empty(&r)) {
    wk_kv_keys_free(&keys);
    return false;
  }
  *out = keys;
  return true;
}

bool wk_kv_decode_keys(const void *p, size_t len, wk_kv_keys *out) {
  wk_kv_keys page;
  if (!decode_byte_list(p, len, &page)) return false;
  if (page.count) {
    out->keys = xrealloc(out->keys, (out->count + page.count) * sizeof *out->keys);
    memcpy(out->keys + out->count, page.keys, page.count * sizeof *page.keys);
    out->count += page.count;
  }
  free(page.keys);
  return true;
}

static int compare_keys(const void *a, const void *b) {
  return wk_bytes_compare(*(const wk_bytes *)a, *(const wk_bytes *)b);
}

void wk_kv_merge_keys(wk_kv_keys *keys, uint32_t limit) {
  if (keys->count > 1) qsort(keys->keys, keys->count, sizeof *keys->keys, compare_keys);
  while (keys->count > limit) free((void *)keys->keys[--keys->count].ptr);
}

/* --------------------------------------------------------- read cache */

#define CACHE_BUCKETS 1024

typedef struct cache_node {
  struct cache_node *next;
  uint8_t *key;
  size_t len;
  uint64_t at;
  bool present;
  wk_kv_entry entry;
} cache_node;

struct wk_kv_cache {
  cache_node *buckets[CACHE_BUCKETS];
  size_t size;
  size_t capacity;
};

static size_t bucket(const void *key, size_t len) {
  return wk_kv_shard_for(wk_bytes_of(key, len), CACHE_BUCKETS);
}

static cache_node **find(wk_kv_cache *c, const void *key, size_t len) {
  cache_node **at = &c->buckets[bucket(key, len)];
  for (; *at; at = &(*at)->next)
    if ((*at)->len == len && (len == 0 || memcmp((*at)->key, key, len) == 0)) return at;
  return at;
}

static void drop(wk_kv_cache *c, cache_node **at) {
  cache_node *n = *at;
  *at = n->next;
  free(n->key);
  wk_kv_entry_free(&n->entry);
  free(n);
  c->size--;
}

wk_kv_cache *wk_kv_cache_new(size_t capacity) {
  wk_kv_cache *c = calloc(1, sizeof *c);
  if (!c) abort();
  c->capacity = capacity;
  return c;
}

void wk_kv_cache_free(wk_kv_cache *c) {
  if (!c) return;
  for (size_t i = 0; i < CACHE_BUCKETS; i++)
    while (c->buckets[i]) drop(c, &c->buckets[i]);
  free(c);
}

size_t wk_kv_cache_size(const wk_kv_cache *c) { return c->size; }

static bool expired(const cache_node *n, uint64_t now) {
  return n->present && n->entry.expires_at != 0 && n->entry.expires_at <= now;
}

int wk_kv_cache_get(const wk_kv_cache *c, const void *key, size_t len, uint64_t now, uint64_t max_age,
                    wk_kv_entry *out) {
  cache_node *n = *find((wk_kv_cache *)c, key, len);
  if (!n) return -1;
  uint64_t age = now > n->at ? now - n->at : 0;
  if (age >= max_age) return -1;
  if (!n->present || expired(n, now)) return 0;
  out->version = n->entry.version;
  out->expires_at = n->entry.expires_at;
  out->value = wk_bytes_dup(n->entry.value.ptr, n->entry.value.len);
  return 1;
}

void wk_kv_cache_insert(wk_kv_cache *c, const void *key, size_t len, uint64_t now, const wk_kv_entry *entry) {
  cache_node **at = find(c, key, len);
  if (!*at && c->size >= c->capacity) {
    for (size_t i = 0; i < CACHE_BUCKETS; i++) {
      cache_node **p = &c->buckets[i];
      while (*p) {
        if (expired(*p, now)) drop(c, p);
        else p = &(*p)->next;
      }
    }
    if (c->size >= c->capacity) {
      for (size_t i = 0; i < CACHE_BUCKETS; i++)
        while (c->buckets[i]) drop(c, &c->buckets[i]);
    }
    at = find(c, key, len);
  }
  cache_node *n = *at;
  if (n) {
    wk_kv_entry_free(&n->entry);
  } else {
    n = calloc(1, sizeof *n);
    if (!n) abort();
    n->key = xrealloc(NULL, len);
    if (len) memcpy(n->key, key, len);
    n->len = len;
    *at = n;
    c->size++;
  }
  n->at = now;
  n->present = entry != NULL;
  if (entry) {
    n->entry.version = entry->version;
    n->entry.expires_at = entry->expires_at;
    n->entry.value = wk_bytes_dup(entry->value.ptr, entry->value.len);
  }
}

void wk_kv_cache_remove(wk_kv_cache *c, const void *key, size_t len) {
  cache_node **at = find(c, key, len);
  if (*at) drop(c, at);
}

/* --------------------------------------------------------------- queue */

void wk_queue_messages_free(wk_queue_messages *m) {
  if (!m) return;
  for (size_t i = 0; i < m->count; i++) free((void *)m->items[i].body.ptr);
  free(m->items);
  m->items = NULL;
  m->count = 0;
}

void wk_queue_dead_letters_free(wk_queue_dead_letters *d) {
  if (!d) return;
  for (size_t i = 0; i < d->count; i++) {
    free((void *)d->items[i].message.body.ptr);
    free((void *)d->items[i].reason.ptr);
  }
  free(d->items);
  d->items = NULL;
  d->count = 0;
}

static void write_message(wk_writer *w, const wk_queue_message *m) {
  wk_write_u64(w, m->id);
  wk_write_u64(w, m->attempt);
  wk_write_u64(w, m->enqueued_at);
  wk_write_bytes(w, m->body.ptr, m->body.len);
}

/* A message; the body points into the reader's buffer. An attempt past
 * UINT32_MAX is malformed (Rust: u32::try_from). */
static bool read_message(wk_reader *r, wk_queue_message *m) {
  uint64_t attempt;
  const uint8_t *body;
  size_t body_len;
  if (!wk_read_u64(r, &m->id) || !wk_read_u64(r, &attempt) || attempt > UINT32_MAX ||
      !wk_read_u64(r, &m->enqueued_at) || !wk_read_bytes(r, &body, &body_len))
    return false;
  m->attempt = (uint32_t)attempt;
  m->body.ptr = (const char *)body;
  m->body.len = body_len;
  return true;
}

void wk_queue_encode_messages(wk_writer *w, const wk_queue_message *items, size_t count) {
  wk_write_u64(w, count);
  for (size_t i = 0; i < count; i++) write_message(w, &items[i]);
}

bool wk_queue_decode_messages(const void *p, size_t len, wk_queue_messages *out) {
  wk_reader r = wk_reader_of(p, len);
  wk_queue_messages list = {NULL, 0};
  size_t cap = 0;
  uint64_t n;
  if (!wk_read_u64(&r, &n)) return false;
  for (uint64_t i = 0; i < n; i++) {
    wk_queue_message m;
    if (!read_message(&r, &m)) {
      wk_queue_messages_free(&list);
      return false;
    }
    if (list.count == cap) {
      cap = cap ? cap * 2 : 8;
      list.items = xrealloc(list.items, cap * sizeof *list.items);
    }
    m.body = wk_bytes_dup(m.body.ptr, m.body.len);
    list.items[list.count++] = m;
  }
  if (!wk_reader_empty(&r)) {
    wk_queue_messages_free(&list);
    return false;
  }
  *out = list;
  return true;
}

void wk_queue_encode_settlements(wk_writer *w, const wk_queue_settlement *items, size_t count) {
  wk_write_u64(w, count);
  for (size_t i = 0; i < count; i++) {
    const wk_queue_outcome *o = &items[i].outcome;
    uint64_t kind = o->kind == WK_QUEUE_ACK ? 0 : 1;
    uint64_t delay = o->kind == WK_QUEUE_ACK ? 0 : o->kind == WK_QUEUE_RETRY ? UINT64_MAX : o->delay_ms;
    wk_write_u64(w, items[i].id);
    wk_write_u64(w, items[i].attempt);
    wk_write_u64(w, kind);
    wk_write_u64(w, delay);
  }
}

bool wk_queue_decode_settlements(const void *p, size_t len, wk_queue_settlement **items, size_t *count) {
  wk_reader r = wk_reader_of(p, len);
  wk_queue_settlement *list = NULL;
  size_t n_items = 0, cap = 0;
  uint64_t n;
  if (!wk_read_u64(&r, &n)) return false;
  for (uint64_t i = 0; i < n; i++) {
    uint64_t id, attempt, kind, delay;
    if (!wk_read_u64(&r, &id) || !wk_read_u64(&r, &attempt) || attempt > UINT32_MAX || !wk_read_u64(&r, &kind) ||
        !wk_read_u64(&r, &delay) || kind > 1) {
      free(list);
      return false;
    }
    if (n_items == cap) {
      cap = cap ? cap * 2 : 8;
      list = xrealloc(list, cap * sizeof *list);
    }
    wk_queue_settlement s = {id, (uint32_t)attempt, {WK_QUEUE_ACK, 0}};
    if (kind == 1) s.outcome = delay == UINT64_MAX ? wk_queue_retry_outcome() : wk_queue_retry_after(delay);
    list[n_items++] = s;
  }
  if (!wk_reader_empty(&r)) {
    free(list);
    return false;
  }
  *items = list;
  *count = n_items;
  return true;
}

void wk_queue_encode_counts(wk_writer *w, const uint64_t *counts, size_t count) {
  wk_write_u64(w, count);
  for (size_t i = 0; i < count; i++) wk_write_u64(w, counts[i]);
}

bool wk_queue_decode_counts(const void *p, size_t len, uint64_t **counts, size_t *count) {
  wk_reader r = wk_reader_of(p, len);
  uint64_t n;
  if (!wk_read_u64(&r, &n)) return false;
  /* Exactly n counts must follow: checked before allocating. */
  if (n > r.len / 8 || r.len != n * 8) return false;
  uint64_t *out = xrealloc(NULL, (size_t)n * sizeof *out);
  for (uint64_t i = 0; i < n; i++) wk_read_u64(&r, &out[i]);
  *counts = out;
  *count = (size_t)n;
  return true;
}

void wk_queue_encode_bodies(wk_writer *w, const wk_bytes *bodies, size_t count) {
  wk_kv_encode_keys(w, bodies, count);
}

bool wk_queue_decode_bodies(const void *p, size_t len, wk_kv_keys *out) { return decode_byte_list(p, len, out); }

void wk_queue_encode_dead(wk_writer *w, const wk_queue_dead_letter *items, size_t count) {
  wk_write_u64(w, count);
  for (size_t i = 0; i < count; i++) {
    write_message(w, &items[i].message);
    wk_write_u64(w, items[i].failed_at);
    wk_write_bytes(w, items[i].reason.ptr, items[i].reason.len);
  }
}

bool wk_queue_decode_dead(const void *p, size_t len, wk_queue_dead_letters *out) {
  wk_reader r = wk_reader_of(p, len);
  wk_queue_dead_letters list = {NULL, 0};
  size_t cap = 0;
  uint64_t n;
  if (!wk_read_u64(&r, &n)) return false;
  for (uint64_t i = 0; i < n; i++) {
    wk_queue_dead_letter d;
    const uint8_t *reason;
    size_t reason_len;
    /* The reason must be UTF-8 (Rust: String::from_utf8). */
    if (!read_message(&r, &d.message) || !wk_read_u64(&r, &d.failed_at) || !wk_read_bytes(&r, &reason, &reason_len) ||
        !wk_utf8_valid(reason, reason_len)) {
      wk_queue_dead_letters_free(&list);
      return false;
    }
    if (list.count == cap) {
      cap = cap ? cap * 2 : 8;
      list.items = xrealloc(list.items, cap * sizeof *list.items);
    }
    d.message.body = wk_bytes_dup(d.message.body.ptr, d.message.body.len);
    d.reason = wk_bytes_dup(reason, reason_len);
    list.items[list.count++] = d;
  }
  if (!wk_reader_empty(&r)) {
    wk_queue_dead_letters_free(&list);
    return false;
  }
  *out = list;
  return true;
}

/* ------------------------------------------------------------ workflow */

static const char *const STATE_NAMES[] = {"running", "waiting", "completed", "failed", "cancelled"};
#define STATE_COUNT (sizeof STATE_NAMES / sizeof *STATE_NAMES)

const char *wk_workflow_state_name(wk_workflow_state state) {
  return (unsigned)state < STATE_COUNT ? STATE_NAMES[state] : "unknown";
}

bool wk_workflow_state_is_finished(wk_workflow_state state) {
  return state == WK_WORKFLOW_COMPLETED || state == WK_WORKFLOW_FAILED || state == WK_WORKFLOW_CANCELLED;
}

void wk_workflow_status_free(wk_workflow_status *status) {
  if (!status) return;
  for (size_t i = 0; i < status->waiting_count; i++) free((void *)status->waiting_for[i].ptr);
  free(status->waiting_for);
  free((void *)status->output.ptr);
  free((void *)status->error.ptr);
  memset(status, 0, sizeof *status);
}

void wk_workflow_encode_status(wk_writer *w, const wk_workflow_status *s) {
  const char *state = wk_workflow_state_name(s->state);
  wk_write_bytes(w, state, strlen(state));
  wk_write_u64(w, s->wake_at);
  /* The names joined with '\n'. */
  wk_writer names = {0};
  for (size_t i = 0; i < s->waiting_count; i++) {
    size_t n = s->waiting_for[i].len;
    reserve(&names, n + 1);
    if (i) names.ptr[names.len++] = '\n';
    if (n) memcpy(names.ptr + names.len, s->waiting_for[i].ptr, n);
    names.len += n;
  }
  wk_write_bytes(w, names.ptr, names.len);
  free(names.ptr);
  wk_write_u64(w, s->has_output ? 1 : 0);
  wk_write_bytes(w, s->output.ptr, s->has_output ? s->output.len : 0);
  wk_write_bytes(w, s->error.ptr, s->error.len);
  wk_write_u64(w, s->positions);
}

bool wk_workflow_decode_status(const void *p, size_t len, wk_workflow_status *out) {
  wk_reader r = wk_reader_of(p, len);
  const uint8_t *state, *names, *output, *error;
  size_t state_len, names_len, output_len, error_len;
  uint64_t wake_at, has_output, positions;
  if (!wk_read_bytes(&r, &state, &state_len) || !wk_read_u64(&r, &wake_at) ||
      !wk_read_bytes(&r, &names, &names_len) || !wk_read_u64(&r, &has_output) ||
      !wk_read_bytes(&r, &output, &output_len) || !wk_read_bytes(&r, &error, &error_len) ||
      !wk_read_u64(&r, &positions))
    return false;
  /* Text must be UTF-8 (Rust: str::from_utf8). Bytes left over are
   * ignored, as Rust's decode_status does. */
  if (!wk_utf8_valid(names, names_len) || !wk_utf8_valid(error, error_len)) return false;
  size_t s = 0;
  while (s < STATE_COUNT && !(strlen(STATE_NAMES[s]) == state_len && memcmp(STATE_NAMES[s], state, state_len) == 0))
    s++;
  if (s == STATE_COUNT) return false;
  wk_workflow_status st;
  memset(&st, 0, sizeof st);
  st.state = (wk_workflow_state)s;
  st.wake_at = wake_at;
  /* Split on '\n', dropping empty names (Rust filters them out). */
  size_t cap = 0;
  for (size_t at = 0; at < names_len;) {
    const uint8_t *nl = memchr(names + at, '\n', names_len - at);
    size_t n = nl ? (size_t)(nl - (names + at)) : names_len - at;
    if (n) {
      if (st.waiting_count == cap) {
        cap = cap ? cap * 2 : 4;
        st.waiting_for = xrealloc(st.waiting_for, cap * sizeof *st.waiting_for);
      }
      st.waiting_for[st.waiting_count++] = wk_bytes_dup(names + at, n);
    }
    at += n + 1;
  }
  /* Exactly 1 means an output (Rust: `== 1`). */
  st.has_output = has_output == 1;
  st.output = st.has_output ? wk_bytes_dup(output, output_len) : wk_bytes_dup("", 0);
  st.error = wk_bytes_dup(error, error_len);
  st.positions = positions;
  *out = st;
  return true;
}
