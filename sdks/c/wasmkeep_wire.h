/* Internal to the C SDK: the small binary framing the kv, queue and
 * workflow clients speak with the actors that hold the data (the Rust SDK's wire.rs, byte
 * for byte): unsigned integers as 8 bytes big-endian, byte strings as a
 * 4-byte big-endian length then the bytes. Also the codecs of each message,
 * the FNV-1a shard hash, the KV read cache and the workflow status codec.
 *
 * Pure: libc only, no host imports, so sdks/c/test/run.sh compiles it with
 * the host's compiler. The shapes follow the protocols in ADR 0015 and ADR
 * 0019. Out of
 * memory aborts, as elsewhere in the SDK. */
#ifndef WASMKEEP_WIRE_H
#define WASMKEEP_WIRE_H

#include "wasmkeep_kv.h"

#ifdef __cplusplus
extern "C" {
#endif

/* ------------------------------------------------------------- framing */

/* Appends to a growing buffer; free `ptr` when done. */
typedef struct wk_writer {
  uint8_t *ptr;
  size_t len;
  size_t cap;
} wk_writer;

void wk_write_u64(wk_writer *w, uint64_t v);
void wk_write_bytes(wk_writer *w, const void *p, size_t len);

/* Reads what wk_writer wrote. Every read fails (false) past the end. */
typedef struct wk_reader {
  const uint8_t *ptr;
  size_t len;
} wk_reader;

static inline wk_reader wk_reader_of(const void *p, size_t len) {
  wk_reader r = {(const uint8_t *)p, len};
  return r;
}
bool wk_read_u64(wk_reader *r, uint64_t *v);
/* The bytes point into the reader's buffer. */
bool wk_read_bytes(wk_reader *r, const uint8_t **p, size_t *len);
static inline bool wk_reader_empty(const wk_reader *r) { return r->len == 0; }

/* Lowercase hex of `len` bytes into `out` (room for 2 * len + 1); returns
 * out, NUL-terminated. */
char *wk_hex(char *out, const void *p, size_t len);

/* A malloc'd copy of `len` bytes, NUL-terminated (not counted). */
wk_bytes wk_bytes_dup(const void *p, size_t len);

/* Byte-wise order, as Rust sorts Vec<u8>. */
int wk_bytes_compare(wk_bytes a, wk_bytes b);

/* Whether `len` bytes are valid UTF-8 (Rust's String::from_utf8). */
bool wk_utf8_valid(const void *p, size_t len);

/* ------------------------------------------------------------------ kv */

/* "any", "absent" or the version in decimal, into `out` (room for 21). */
const char *wk_kv_expect_encode(wk_kv_expect expect, char out[21]);
/* The inverse (Rust's u64::from_str for a version: digits, an optional
 * leading '+'); false if malformed. */
bool wk_kv_expect_decode(const char *s, wk_kv_expect *out);

void wk_kv_encode_entry(wk_writer *w, const wk_kv_entry *entry);
/* False if malformed or bytes are left over; *out is then untouched. */
bool wk_kv_decode_entry(const void *p, size_t len, wk_kv_entry *out);

void wk_kv_encode_keys(wk_writer *w, const wk_bytes *keys, size_t count);
/* Appends the decoded keys to *out (copies); false if malformed. */
bool wk_kv_decode_keys(const void *p, size_t len, wk_kv_keys *out);

/* Sorts keys in byte order and keeps the first `limit` (freeing the
 * rest): the merge of every shard's page. */
void wk_kv_merge_keys(wk_kv_keys *keys, uint32_t limit);

/* A bounded map of recent reads, keyed by bytes. When full, expired
 * entries go first, then everything: simple, and a full cache is refilled
 * by the next reads. */
typedef struct wk_kv_cache wk_kv_cache;
wk_kv_cache *wk_kv_cache_new(size_t capacity);
void wk_kv_cache_free(wk_kv_cache *cache);
size_t wk_kv_cache_size(const wk_kv_cache *cache);
/* -1 on a miss (absent or read `max_age` ago or more), 0 for a cached
 * absence (or an entry that expired since), 1 with a copy in *out. */
int wk_kv_cache_get(const wk_kv_cache *cache, const void *key, size_t len, uint64_t now, uint64_t max_age,
                    wk_kv_entry *out);
/* Remembers `entry` (NULL: the key is absent) as read at `now`. */
void wk_kv_cache_insert(wk_kv_cache *cache, const void *key, size_t len, uint64_t now, const wk_kv_entry *entry);
void wk_kv_cache_remove(wk_kv_cache *cache, const void *key, size_t len);

/* --------------------------------------------------------------- queue */

void wk_queue_encode_messages(wk_writer *w, const wk_queue_message *items, size_t count);
bool wk_queue_decode_messages(const void *p, size_t len, wk_queue_messages *out);

/* UINT64_MAX in the delay slot means "the queue's backoff". */
void wk_queue_encode_settlements(wk_writer *w, const wk_queue_settlement *items, size_t count);
/* Decoded settlements: free `*items`. */
bool wk_queue_decode_settlements(const void *p, size_t len, wk_queue_settlement **items, size_t *count);

void wk_queue_encode_counts(wk_writer *w, const uint64_t *counts, size_t count);
/* Decoded counts: free `*counts`. */
bool wk_queue_decode_counts(const void *p, size_t len, uint64_t **counts, size_t *count);

void wk_queue_encode_bodies(wk_writer *w, const wk_bytes *bodies, size_t count);
/* Decoded bodies: free with wk_kv_keys_free. */
bool wk_queue_decode_bodies(const void *p, size_t len, wk_kv_keys *out);

void wk_queue_encode_dead(wk_writer *w, const wk_queue_dead_letter *items, size_t count);
bool wk_queue_decode_dead(const void *p, size_t len, wk_queue_dead_letters *out);

/* ------------------------------------------------------------ workflow */

/* An instance's status, as the workflow class answers it (ADR 0019): the
 * state's name, wake_at, the waiting event names joined with '\n', 1 or 0
 * for an output, the output, the error (empty: none), the positions. */
void wk_workflow_encode_status(wk_writer *w, const wk_workflow_status *status);
/* False if malformed (*out is then untouched); free *out otherwise. As in
 * Rust, bytes after the positions are ignored. */
bool wk_workflow_decode_status(const void *p, size_t len, wk_workflow_status *out);

/* ------------------------------------------- shared by the clients */

/* Why the last call to `address` failed, into `out`, worded as the Rust
 * SDK's wire::call. Defined in wasmkeep_kv.c: it reads the host's last
 * error, so it is not part of the pure code above. */
void wk_call_failure(char *out, size_t size, const char *address);

#ifdef __cplusplus
}
#endif

#endif
