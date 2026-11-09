/* Key-value storage and queues built from actors (ADR 0015): the client
 * side, for C. wasmkeep.h includes this header.
 *
 *   static wk_kv store;
 *   static wk_queue jobs;
 *
 *   void wk_init(void) {
 *     store = wk_kv_client("kv", 8);          // 8 shards: kv/shard-0 .. kv/shard-7
 *     jobs = wk_queue_client("queue/jobs");
 *   }
 *
 *   uint64_t v;
 *   wk_kv_error err;
 *   if (!wk_kv_put(&store, wk_str("user:42"), wk_str("Ada"), &v, &err))
 *     return wk_reply(502, "%s\n", err.message);
 *   wk_kv_entry e;
 *   if (wk_kv_get(&store, wk_str("user:42"), &e, &err) == 1) {
 *     ... e.value, e.version ...
 *     wk_kv_entry_free(&e);
 *   }
 *
 * The shard and queue classes are the Rust SDK's (deploy the components in
 * examples/jobs: `kv` and `queue`); these clients speak their protocol
 * (`/wasmkeep-kv/v1`, `/wasmkeep-queue/v1`) over actors.call, byte for byte
 * as the Rust and TypeScript clients do, so they share the same data.
 *
 * Conventions:
 * - Keys, values and bodies are wk_bytes (wk_str for a C string): bytes,
 *   not text. Clients borrow the strings they are made from (class name,
 *   address): keep them alive (a literal is fine).
 * - Every call takes an optional error (NULL to ignore it). On failure it
 *   holds the kind, the status for WK_KV_REFUSED / WK_QUEUE_REFUSED, and a
 *   message worded as in the Rust SDK.
 * - What a call returns that holds memory (an entry, keys, messages, dead
 *   letters) is the caller's: free it with its _free function. Returned
 *   bytes are also NUL-terminated (the NUL is not counted in len).
 * - Every operation is one actors.call (a list is one per shard), which
 *   holds this actor's event while it runs. */
#ifndef WASMKEEP_KV_H
#define WASMKEEP_KV_H

#include "wasmkeep.h"

#ifdef __cplusplus
extern "C" {
#endif

/* A C string as bytes (without its NUL). */
static inline wk_bytes wk_str(const char *s) {
  wk_bytes b = {s ? s : "", s ? strlen(s) : 0};
  return b;
}
/* `len` bytes at `p` as wk_bytes. */
static inline wk_bytes wk_bytes_of(const void *p, size_t len) {
  wk_bytes b = {p ? (const char *)p : "", p ? len : 0};
  return b;
}

/* ------------------------------------------------------------------ kv */

/* Keys are at most this long (bytes). */
#define WK_KV_MAX_KEY_LEN 1024

typedef enum wk_kv_error_kind {
  WK_KV_OK = 0,
  /* A conditional write found another version (or a key where it expected
   * none). */
  WK_KV_CONFLICT,
  /* The key is longer than WK_KV_MAX_KEY_LEN. */
  WK_KV_KEY_TOO_LONG,
  /* The shard's database refused the operation. */
  WK_KV_STORAGE,
  /* The call to the shard failed: the outcome of a write is then not known
   * (it may have applied). Re-read before retrying a conditional write. */
  WK_KV_CALL,
  /* The shard answered with an unexpected status (`status`); a malformed
   * reply counts as status 200. */
  WK_KV_REFUSED,
} wk_kv_error_kind;

typedef struct wk_kv_error {
  wk_kv_error_kind kind;
  uint16_t status;   /* for WK_KV_REFUSED */
  char message[512]; /* NUL-terminated, as Rust's Display */
} wk_kv_error;

/* A stored value. Free with wk_kv_entry_free. */
typedef struct wk_kv_entry {
  wk_bytes value;
  /* Changes on every write of the key, and is never reused within a shard
   * (not even after a delete), so a compare-and-set on an old version
   * always loses. */
  uint64_t version;
  /* When the key expires (ms since the Unix epoch); 0 if it does not. */
  uint64_t expires_at;
} wk_kv_entry;
void wk_kv_entry_free(wk_kv_entry *entry);

typedef enum wk_kv_expect_kind {
  WK_KV_EXPECT_ANY = 0, /* write whatever is there */
  WK_KV_EXPECT_ABSENT,  /* only if the key is absent (or expired) */
  WK_KV_EXPECT_VERSION, /* only if the key holds `version` */
} wk_kv_expect_kind;

/* What a conditional write expects to find. */
typedef struct wk_kv_expect {
  wk_kv_expect_kind kind;
  uint64_t version;
} wk_kv_expect;

static inline wk_kv_expect wk_kv_expect_any(void) { wk_kv_expect e = {WK_KV_EXPECT_ANY, 0}; return e; }
static inline wk_kv_expect wk_kv_expect_absent(void) { wk_kv_expect e = {WK_KV_EXPECT_ABSENT, 0}; return e; }
static inline wk_kv_expect wk_kv_expect_version(uint64_t v) { wk_kv_expect e = {WK_KV_EXPECT_VERSION, v}; return e; }
/* Whether a key at `current` (absent when `present` is false) meets it. */
bool wk_kv_expect_allows(wk_kv_expect expect, bool present, uint64_t current);

/* Options for wk_kv_put_with. Zeroed, they are the defaults: no condition,
 * no expiry, durable. */
typedef struct wk_kv_put_options {
  wk_kv_expect expect;
  /* Expire the key `ttl_ms` milliseconds after the write. */
  bool has_ttl;
  uint64_t ttl_ms;
  /* Acknowledge once committed on the shard's node, before the write is
   * durable (in the bucket in a cluster, fsynced on one node): faster, but
   * a node lost in that window loses it. */
  bool local;
} wk_kv_put_options;

/* The shard (0-based) that holds `key` when there are `shards` of them:
 * FNV-1a (64 bits) of the key, modulo the count (zero counts as one). Part
 * of the data layout: it never changes, and matches the Rust SDK. */
uint32_t wk_kv_shard_for(wk_bytes key, uint32_t shards);

/* Reaches the shards of one class. A plain value: copy it freely. */
typedef struct wk_kv {
  const char *class_name; /* borrowed */
  uint32_t shards;
  uint64_t cache_ms; /* 0: no read cache */
} wk_kv;

/* Keys spread over `shards` actors of `class_name` (at least one). Every
 * caller must use the same count: changing it moves keys. */
wk_kv wk_kv_client(const char *class_name, uint32_t shards);

/* Serve wk_kv_get from an in-memory cache in this actor's instance when the
 * entry was read less than `max_age_ms` ago. This actor's own writes
 * through a client with a cache update it, so it reads its own writes;
 * other actors' writes show up within `max_age_ms`. The cache is lost with
 * the instance (hibernation, a failed event, a deploy). */
wk_kv wk_kv_with_read_cache(wk_kv kv, uint64_t max_age_ms);

/* The entry at `key`: 1 and *entry filled (free it), 0 if absent or
 * expired, -1 on failure. From the read cache if one is set and fresh. */
int wk_kv_get(const wk_kv *kv, wk_bytes key, wk_kv_entry *entry, wk_kv_error *err);
/* Like wk_kv_get, always asking the shard. */
int wk_kv_get_fresh(const wk_kv *kv, wk_bytes key, wk_kv_entry *entry, wk_kv_error *err);

/* Stores `value` at `key` (durably); *version (may be NULL) gets the new
 * version. */
bool wk_kv_put(const wk_kv *kv, wk_bytes key, wk_bytes value, uint64_t *version, wk_kv_error *err);
/* Stores `value` with `options` (NULL: the defaults). */
bool wk_kv_put_with(const wk_kv *kv, wk_bytes key, wk_bytes value, const wk_kv_put_options *options,
                    uint64_t *version, wk_kv_error *err);
/* Stores `value` only if `key` is at *expected (NULL: absent);
 * WK_KV_CONFLICT otherwise. */
bool wk_kv_compare_and_set(const wk_kv *kv, wk_bytes key, const uint64_t *expected, wk_bytes value,
                           uint64_t *version, wk_kv_error *err);

/* Removes `key`; *removed (may be NULL) tells whether it was there. */
bool wk_kv_delete(const wk_kv *kv, wk_bytes key, bool *removed, wk_kv_error *err);
/* Removes `key` only if it holds `version`; WK_KV_CONFLICT otherwise. */
bool wk_kv_delete_if(const wk_kv *kv, wk_bytes key, uint64_t version, bool *removed, wk_kv_error *err);

/* Keys from wk_kv_list. Free with wk_kv_keys_free. */
typedef struct wk_kv_keys {
  wk_bytes *keys;
  size_t count;
} wk_kv_keys;
void wk_kv_keys_free(wk_kv_keys *keys);

/* Up to `limit` keys starting with `prefix`, in byte order, after *after
 * (exclusive; NULL: from the start): pass the last key of a page to get the
 * next one. Asks every shard (one call each), so keep it for listings, not
 * hot paths. */
bool wk_kv_list(const wk_kv *kv, wk_bytes prefix, const wk_bytes *after, uint32_t limit, wk_kv_keys *out,
                wk_kv_error *err);

/* --------------------------------------------------------------- queue */

/* Messages one send may carry. */
#define WK_QUEUE_MAX_BATCH 1000
/* The path a push delivery arrives on, at the consumer. */
#define WK_QUEUE_DELIVER_PATH "/wasmkeep-queue/v1/deliver"

typedef enum wk_queue_error_kind {
  WK_QUEUE_OK = 0,
  /* The queue's database refused the operation. */
  WK_QUEUE_STORAGE,
  /* The call to the queue failed. For a send, the message may or may not
   * be queued. */
  WK_QUEUE_CALL,
  /* The queue answered with an unexpected status (`status`); a malformed
   * reply counts as status 200. */
  WK_QUEUE_REFUSED,
} wk_queue_error_kind;

typedef struct wk_queue_error {
  wk_queue_error_kind kind;
  uint16_t status;
  char message[512];
} wk_queue_error;

/* A leased or delivered message. */
typedef struct wk_queue_message {
  /* Unique within the queue, never reused, the same on every delivery. */
  uint64_t id;
  /* Which delivery this is, from 1. Acks and retries name it, so one for
   * an earlier lease of a message leased again is ignored. */
  uint32_t attempt;
  /* When it was sent (ms since the Unix epoch). */
  uint64_t enqueued_at;
  wk_bytes body;
} wk_queue_message;

/* Messages from wk_queue_lease. Free with wk_queue_messages_free. */
typedef struct wk_queue_messages {
  wk_queue_message *items;
  size_t count;
} wk_queue_messages;
void wk_queue_messages_free(wk_queue_messages *messages);

typedef enum wk_queue_outcome_kind {
  WK_QUEUE_ACK = 0,      /* handled: delete it */
  WK_QUEUE_RETRY,        /* deliver it again after the queue's backoff */
  WK_QUEUE_RETRY_AFTER,  /* deliver it again after `delay_ms` */
} wk_queue_outcome_kind;

/* What a consumer did with a message. A retry of a message that had all
 * its attempts dead-letters it. */
typedef struct wk_queue_outcome {
  wk_queue_outcome_kind kind;
  uint64_t delay_ms; /* for WK_QUEUE_RETRY_AFTER */
} wk_queue_outcome;

static inline wk_queue_outcome wk_queue_ack_outcome(void) { wk_queue_outcome o = {WK_QUEUE_ACK, 0}; return o; }
static inline wk_queue_outcome wk_queue_retry_outcome(void) { wk_queue_outcome o = {WK_QUEUE_RETRY, 0}; return o; }
static inline wk_queue_outcome wk_queue_retry_after(uint64_t delay_ms) {
  wk_queue_outcome o = {WK_QUEUE_RETRY_AFTER, delay_ms};
  return o;
}

/* One settlement: a lease (`id`, `attempt`) and its outcome. */
typedef struct wk_queue_settlement {
  uint64_t id;
  uint32_t attempt;
  wk_queue_outcome outcome;
} wk_queue_settlement;

/* How many messages are where. */
typedef struct wk_queue_stats {
  uint64_t ready;     /* visible now */
  uint64_t delayed;   /* sent with a delay that has not passed */
  uint64_t in_flight; /* leased and not yet acked, or waiting for a retry */
  uint64_t dead;      /* in the dead-letter table */
} wk_queue_stats;

/* A dead-lettered message. */
typedef struct wk_queue_dead_letter {
  wk_queue_message message; /* as it was last delivered */
  uint64_t failed_at;       /* ms since the Unix epoch */
  wk_bytes reason;          /* UTF-8: attempts exhausted, or the last error */
} wk_queue_dead_letter;

/* Dead letters from wk_queue_dead_letters. Free with
 * wk_queue_dead_letters_free. */
typedef struct wk_queue_dead_letters {
  wk_queue_dead_letter *items;
  size_t count;
} wk_queue_dead_letters;
void wk_queue_dead_letters_free(wk_queue_dead_letters *dead);

/* Reaches one queue actor. A plain value: copy it freely. */
typedef struct wk_queue {
  const char *address; /* "class/name", borrowed */
} wk_queue;

/* The queue at `address` ("class/name"). */
wk_queue wk_queue_client(const char *address);

/* Queues `body`; *id (may be NULL) gets its id once it is durable. */
bool wk_queue_send(const wk_queue *q, wk_bytes body, uint64_t *id, wk_queue_error *err);
/* Queues `body`, visible after `delay_ms`. */
bool wk_queue_send_delayed(const wk_queue *q, wk_bytes body, uint64_t delay_ms, uint64_t *id,
                           wk_queue_error *err);
/* Queues `count` messages (at most WK_QUEUE_MAX_BATCH; the queue refuses
 * more) in one durable write; ids (may be NULL, else room for `count`)
 * gets their ids in order. All or none are queued. */
bool wk_queue_send_batch(const wk_queue *q, const wk_bytes *bodies, size_t count, uint64_t *ids,
                         wk_queue_error *err);

/* Leases up to `max` visible messages for `visibility_ms`. Free *out. */
bool wk_queue_lease(const wk_queue *q, uint32_t max, uint64_t visibility_ms, wk_queue_messages *out,
                    wk_queue_error *err);
/* Deletes a leased message. *current (may be NULL) is false if this lease
 * is no longer current (it ran out and the message was leased again, or
 * was settled). */
bool wk_queue_ack(const wk_queue *q, uint64_t id, uint32_t attempt, bool *current, wk_queue_error *err);
/* Makes a leased message visible again after *delay_ms (NULL: the queue's
 * backoff). */
bool wk_queue_retry(const wk_queue *q, uint64_t id, uint32_t attempt, const uint64_t *delay_ms, bool *current,
                    wk_queue_error *err);
/* Settles `count` leased messages in one call; current (may be NULL, else
 * room for `count`) gets one flag per item, true where the lease was
 * current (false for items the queue's reply does not mention). */
bool wk_queue_settle(const wk_queue *q, const wk_queue_settlement *items, size_t count, bool *current,
                     wk_queue_error *err);
/* How many messages are ready, delayed, in flight and dead. */
bool wk_queue_get_stats(const wk_queue *q, wk_queue_stats *out, wk_queue_error *err);
/* The oldest `limit` dead-lettered messages. Free *out. */
bool wk_queue_get_dead_letters(const wk_queue *q, uint32_t limit, wk_queue_dead_letters *out,
                               wk_queue_error *err);

/* Whether `req` is a push delivery from a queue (answer it with
 * wk_queue_handle_delivery). */
bool wk_queue_is_delivery(const wk_request *req);

/* A push consumer's handler: `queue` is the sending queue's address
 * ("tenant/class/name"), `message` one delivered message (valid during the
 * call). Set *outcome and return true; return false to fail the whole
 * delivery (it answers 500 and the queue retries the batch with its
 * backoff). */
typedef bool (*wk_queue_handler)(const char *queue, const wk_queue_message *message, wk_queue_outcome *outcome,
                                 void *ctx);

/* Answers a push delivery: calls `handle` once per message and replies with
 * their outcomes. Return the response from wk_fetch. Deliveries from
 * outside clients (without x-wasmkeep-caller) get 403, a malformed body 400.
 * Record handled ids in this actor's database, in the same event, when a
 * duplicate delivery would matter. */
wk_response wk_queue_handle_delivery(wk_request *req, wk_queue_handler handle, void *ctx);

#ifdef __cplusplus
}
#endif

#endif
