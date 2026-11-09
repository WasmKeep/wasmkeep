/* Native unit tests of the C SDK's kv, queue and workflow clients: the
 * framing, codecs, shard hash and read cache (the Rust SDK's unit tests,
 * with the golden bytes and shard choices of
 * sdks/typescript/test/wire.test.ts, computed from the Rust code), the
 * workflow status codec (golden bytes from the Rust encode_status), then
 * the clients and the delivery handler against a fake actors.call. Run
 * with sdks/c/test/run.sh. */
#include <stdio.h>
#include <stdlib.h>

#include "wasmkeep_wire.h"

static int failures, checks;

#define CHECK(cond)                                                   \
  do {                                                                \
    checks++;                                                         \
    if (!(cond)) {                                                    \
      failures++;                                                     \
      fprintf(stderr, "%s:%d: check failed: %s\n", __FILE__, __LINE__, #cond); \
    }                                                                 \
  } while (0)

#define CHECK_STR(got, want)                                                              \
  do {                                                                                    \
    checks++;                                                                             \
    const char *g_ = (got), *w_ = (want);                                                 \
    if (strcmp(g_, w_) != 0) {                                                            \
      failures++;                                                                         \
      fprintf(stderr, "%s:%d: got \"%s\"\n%*swant \"%s\"\n", __FILE__, __LINE__, g_,      \
              (int)strlen(__FILE__) + 7, "", w_);                                         \
    }                                                                                     \
  } while (0)

static const char *hex_of(const wk_writer *w) {
  static char buf[4096];
  return wk_hex(buf, w->ptr, w->len);
}

static wk_bytes B(const char *s) { return wk_str(s); }

static bool same(wk_bytes a, wk_bytes b) { return wk_bytes_compare(a, b) == 0; }

/* ------------------------------------------------------------- framing */

static void test_framing(void) {
  wk_writer w = {0};
  wk_write_u64(&w, 7);
  wk_write_bytes(&w, "abc", 3);
  wk_write_bytes(&w, "", 0);
  wk_write_u64(&w, UINT64_MAX);
  wk_reader r = wk_reader_of(w.ptr, w.len);
  uint64_t v;
  const uint8_t *p;
  size_t n;
  CHECK(wk_read_u64(&r, &v) && v == 7);
  CHECK(wk_read_bytes(&r, &p, &n) && n == 3 && memcmp(p, "abc", 3) == 0);
  CHECK(wk_read_bytes(&r, &p, &n) && n == 0);
  CHECK(wk_read_u64(&r, &v) && v == UINT64_MAX);
  CHECK(wk_reader_empty(&r));
  CHECK(!wk_read_u64(&r, &v));
  /* A length that runs past the end is refused, not truncated. */
  wk_reader cut = wk_reader_of(w.ptr + 8, 4);
  CHECK(!wk_read_bytes(&cut, &p, &n));
  free(w.ptr);

  /* Big-endian, as Rust writes them. */
  wk_writer be = {0};
  wk_write_u64(&be, 7);
  wk_write_bytes(&be, "abc", 3);
  wk_write_u64(&be, 0x0102030405060708ULL);
  CHECK_STR(hex_of(&be), "0000000000000007" "00000003616263" "0102030405060708");
  free(be.ptr);
}

static void test_hex(void) {
  uint8_t all[256];
  for (int i = 0; i < 256; i++) all[i] = (uint8_t)i;
  char out[513];
  wk_hex(out, all, 256);
  CHECK(strlen(out) == 512);
  CHECK(strncmp(out, "000102", 6) == 0 && strcmp(out + 506, "fdfeff") == 0);
  CHECK_STR(wk_hex(out, "\x00\xff\x41", 3), "00ff41");
  CHECK_STR(wk_hex(out, "", 0), "");
}

/* ------------------------------------------------------------------ kv */

static void test_shard_hash(void) {
  /* Known FNV-1a 64 values: "" and "a" (kv.rs). */
  CHECK(wk_kv_shard_for(B(""), UINT32_MAX) == (uint32_t)(0xcbf29ce484222325ULL % UINT32_MAX));
  CHECK(wk_kv_shard_for(B("a"), 1u << 16) == (uint32_t)(0xaf63dc4c8601ec8cULL % (1u << 16)));
  CHECK(wk_kv_shard_for(B("anything"), 1) == 0);
  CHECK(wk_kv_shard_for(B("anything"), 0) == 0); /* zero shards means one */
  /* Computed with the Rust shard_for: key, n=4, n=8, n=1000, n=u32::MAX. */
  static const struct {
    const char *key;
    size_t len;
    uint32_t n4, n8, n1000, max;
  } vectors[] = {
      {"", 0, 1, 5, 37, 1343537162},
      {"a", 1, 0, 4, 996, 895863001},
      {"user:42", 7, 2, 2, 410, 1223114855},
      {"user:0", 6, 0, 0, 736, 1829090531},
      {"user:1", 6, 3, 3, 947, 1829091222},
      {"user:2", 6, 2, 6, 158, 1829091913},
      {"job:1", 5, 1, 1, 481, 310798266},
      {"greeting", 8, 2, 6, 198, 2089897798},
      {"cl\xc3\xa9 \xc3\xa9", 7, 2, 6, 550, 819157025}, /* "clé é" as UTF-8 */
      {"\x00\xff\x80", 3, 0, 0, 392, 1085081017},
  };
  for (size_t i = 0; i < sizeof vectors / sizeof *vectors; i++) {
    wk_bytes k = wk_bytes_of(vectors[i].key, vectors[i].len);
    CHECK(wk_kv_shard_for(k, 4) == vectors[i].n4);
    CHECK(wk_kv_shard_for(k, 8) == vectors[i].n8);
    CHECK(wk_kv_shard_for(k, 1000) == vectors[i].n1000);
    CHECK(wk_kv_shard_for(k, UINT32_MAX) == vectors[i].max);
  }
  /* Spread: 1,000 keys over 8 shards, the exact counts Rust gets. */
  int seen[8] = {0};
  for (int i = 0; i < 1000; i++) {
    char key[24];
    snprintf(key, sizeof key, "user:%d", i);
    seen[wk_kv_shard_for(B(key), 8)]++;
  }
  static const int want[8] = {126, 125, 125, 126, 124, 125, 125, 124};
  CHECK(memcmp(seen, want, sizeof want) == 0);
}

static void test_expectations(void) {
  CHECK(wk_kv_expect_allows(wk_kv_expect_any(), false, 0));
  CHECK(wk_kv_expect_allows(wk_kv_expect_any(), true, 3));
  CHECK(wk_kv_expect_allows(wk_kv_expect_absent(), false, 0));
  CHECK(!wk_kv_expect_allows(wk_kv_expect_absent(), true, 3));
  CHECK(wk_kv_expect_allows(wk_kv_expect_version(3), true, 3));
  CHECK(!wk_kv_expect_allows(wk_kv_expect_version(3), true, 4));
  CHECK(!wk_kv_expect_allows(wk_kv_expect_version(3), false, 0));
  wk_kv_expect all[] = {wk_kv_expect_any(), wk_kv_expect_absent(), wk_kv_expect_version(0),
                        wk_kv_expect_version(UINT64_MAX)};
  for (size_t i = 0; i < 4; i++) {
    char buf[21];
    wk_kv_expect back;
    CHECK(wk_kv_expect_decode(wk_kv_expect_encode(all[i], buf), &back));
    CHECK(back.kind == all[i].kind && back.version == all[i].version);
  }
  char buf[21];
  CHECK_STR(wk_kv_expect_encode(wk_kv_expect_version(42), buf), "42");
  CHECK_STR(wk_kv_expect_encode(wk_kv_expect_version(UINT64_MAX), buf), "18446744073709551615");
  wk_kv_expect e;
  CHECK(!wk_kv_expect_decode("nope", &e));
  CHECK(!wk_kv_expect_decode("-1", &e));
  CHECK(!wk_kv_expect_decode("18446744073709551616", &e));
  CHECK(wk_kv_expect_decode("+7", &e) && e.version == 7);
}

static void test_entries_and_keys(void) {
  wk_kv_entry e = {B("v"), 9, 1234};
  wk_writer w = {0};
  wk_kv_encode_entry(&w, &e);
  CHECK_STR(hex_of(&w), "0000000000000009" "00000000000004d2" "0000000176");
  wk_kv_entry back;
  CHECK(wk_kv_decode_entry(w.ptr, w.len, &back));
  CHECK(back.version == 9 && back.expires_at == 1234 && same(back.value, B("v")));
  CHECK(back.value.ptr[back.value.len] == 0);
  wk_kv_entry_free(&back);
  /* Trailing bytes are refused. */
  wk_write_bytes(&w, "", 0);
  CHECK(!wk_kv_decode_entry(w.ptr, w.len - 3, &back));
  free(w.ptr);

  wk_bytes keys[] = {B("a"), B(""), wk_bytes_of("\x00\xff", 2)};
  wk_writer kw = {0};
  wk_kv_encode_keys(&kw, keys, 3);
  wk_kv_keys got = {0};
  CHECK(wk_kv_decode_keys(kw.ptr, kw.len, &got));
  CHECK(got.count == 3 && same(got.keys[0], keys[0]) && same(got.keys[1], keys[1]) && same(got.keys[2], keys[2]));
  /* Decoding appends: the pages of every shard go into one list. */
  CHECK(wk_kv_decode_keys(kw.ptr, kw.len, &got) && got.count == 6);
  wk_kv_keys_free(&got);
  free(kw.ptr);
  static const uint8_t promise[] = {0, 0, 0, 0, 0, 0, 0, 1};
  wk_kv_keys none = {0};
  CHECK(!wk_kv_decode_keys(promise, sizeof promise, &none) && none.count == 0);
  /* A count far past the buffer fails without allocating it. */
  static const uint8_t huge[] = {0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff};
  CHECK(!wk_kv_decode_keys(huge, sizeof huge, &none));
}

static void test_merge(void) {
  wk_kv_keys keys = {0};
  const char *page[] = {"a", "d", "b", "c", "e"};
  for (int i = 0; i < 5; i++) {
    wk_writer w = {0};
    wk_bytes k = B(page[i]);
    wk_kv_encode_keys(&w, &k, 1);
    wk_kv_decode_keys(w.ptr, w.len, &keys);
    free(w.ptr);
  }
  wk_kv_merge_keys(&keys, 4);
  CHECK(keys.count == 4 && same(keys.keys[0], B("a")) && same(keys.keys[1], B("b")) &&
        same(keys.keys[2], B("c")) && same(keys.keys[3], B("d")));
  wk_kv_keys_free(&keys);
  /* Byte order: "é" (c3 a9) sorts after "z" (7a), a prefix before its
   * extensions. */
  const char *mixed[] = {"\xc3\xa9", "z", "za", ""};
  for (int i = 0; i < 4; i++) {
    wk_writer w = {0};
    wk_bytes k = B(mixed[i]);
    wk_kv_encode_keys(&w, &k, 1);
    wk_kv_decode_keys(w.ptr, w.len, &keys);
    free(w.ptr);
  }
  wk_kv_merge_keys(&keys, 10);
  CHECK(keys.count == 4 && same(keys.keys[0], B("")) && same(keys.keys[1], B("z")) &&
        same(keys.keys[2], B("za")) && same(keys.keys[3], B("\xc3\xa9")));
  wk_kv_keys_free(&keys);
}

static void test_cache(void) {
  wk_kv_cache *c = wk_kv_cache_new(2);
  wk_kv_entry e1 = {B(""), 1, 0}, got;
  wk_kv_cache_insert(c, "k", 1, 100, &e1);
  CHECK(wk_kv_cache_get(c, "k", 1, 150, 100, &got) == 1 && got.version == 1);
  wk_kv_entry_free(&got);
  CHECK(wk_kv_cache_get(c, "k", 1, 200, 100, &got) == -1); /* too old */
  wk_kv_cache_insert(c, "gone", 4, 100, NULL);
  CHECK(wk_kv_cache_get(c, "gone", 4, 120, 100, &got) == 0);  /* absence is cached */
  CHECK(wk_kv_cache_get(c, "other", 5, 120, 100, &got) == -1); /* a miss is not an absence */

  /* An entry that expired while cached reads as absent. */
  wk_kv_entry e2 = {B("x"), 2, 130};
  wk_kv_cache_insert(c, "k", 1, 100, &e2);
  CHECK(wk_kv_cache_get(c, "k", 1, 129, 100, &got) == 1 && got.version == 2 && got.expires_at == 130 &&
        same(got.value, B("x")));
  wk_kv_entry_free(&got);
  CHECK(wk_kv_cache_get(c, "k", 1, 130, 100, &got) == 0);

  /* Full: expired entries are dropped first, then everything. */
  wk_kv_entry e3 = {B(""), 3, 0};
  wk_kv_cache_insert(c, "third", 5, 140, &e3);
  CHECK(wk_kv_cache_size(c) <= 2);
  CHECK(wk_kv_cache_get(c, "third", 5, 141, 100, &got) == 1 && got.version == 3);
  wk_kv_entry_free(&got);
  wk_kv_cache_remove(c, "third", 5);
  CHECK(wk_kv_cache_get(c, "third", 5, 141, 100, &got) == -1);
  /* A clock that went back counts as no age (Rust: saturating_sub). */
  wk_kv_cache_insert(c, "k", 1, 500, &e1);
  CHECK(wk_kv_cache_get(c, "k", 1, 400, 100, &got) == 1);
  wk_kv_entry_free(&got);
  wk_kv_cache_free(c);
}

/* --------------------------------------------------------------- queue */

static void test_queue_frames(void) {
  wk_queue_message messages[] = {{1, 2, 3, B("x")}, {UINT64_MAX, UINT32_MAX, 0, B("")}};
  wk_writer w = {0};
  wk_queue_encode_messages(&w, messages, 2);
  wk_queue_messages got;
  CHECK(wk_queue_decode_messages(w.ptr, w.len, &got));
  CHECK(got.count == 2 && got.items[0].id == 1 && got.items[0].attempt == 2 && got.items[0].enqueued_at == 3 &&
        same(got.items[0].body, B("x")) && got.items[1].id == UINT64_MAX && got.items[1].attempt == UINT32_MAX &&
        got.items[1].body.len == 0);
  wk_queue_messages_free(&got);
  free(w.ptr);

  wk_queue_settlement items[] = {
      {1, 1, wk_queue_ack_outcome()}, {2, 3, wk_queue_retry_outcome()}, {3, 1, wk_queue_retry_after(50)}};
  wk_writer sw = {0};
  wk_queue_encode_settlements(&sw, items, 3);
  wk_queue_settlement *back;
  size_t n;
  CHECK(wk_queue_decode_settlements(sw.ptr, sw.len, &back, &n) && n == 3);
  CHECK(back[0].id == 1 && back[0].outcome.kind == WK_QUEUE_ACK);
  CHECK(back[1].id == 2 && back[1].attempt == 3 && back[1].outcome.kind == WK_QUEUE_RETRY);
  CHECK(back[2].outcome.kind == WK_QUEUE_RETRY_AFTER && back[2].outcome.delay_ms == 50);
  free(back);
  free(sw.ptr);

  uint64_t counts[] = {4, 5};
  wk_writer cw = {0};
  wk_queue_encode_counts(&cw, counts, 2);
  uint64_t *cs;
  CHECK(wk_queue_decode_counts(cw.ptr, cw.len, &cs, &n) && n == 2 && cs[0] == 4 && cs[1] == 5);
  free(cs);
  free(cw.ptr);

  wk_bytes bodies[] = {B("a"), B("")};
  wk_writer bw = {0};
  wk_queue_encode_bodies(&bw, bodies, 2);
  wk_kv_keys bs;
  CHECK(wk_queue_decode_bodies(bw.ptr, bw.len, &bs) && bs.count == 2 && same(bs.keys[0], B("a")) &&
        bs.keys[1].len == 0);
  wk_kv_keys_free(&bs);
  free(bw.ptr);

  wk_queue_dead_letter dead[] = {{messages[0], 9, B("attempts exhausted")}};
  wk_writer dw = {0};
  wk_queue_encode_dead(&dw, dead, 1);
  wk_queue_dead_letters ds;
  CHECK(wk_queue_decode_dead(dw.ptr, dw.len, &ds) && ds.count == 1 && ds.items[0].message.id == 1 &&
        ds.items[0].failed_at == 9 && same(ds.items[0].reason, B("attempts exhausted")) &&
        same(ds.items[0].message.body, B("x")));
  wk_queue_dead_letters_free(&ds);
  free(dw.ptr);
}

static void test_settlement_bytes(void) {
  const char *head = "0000000000000001" "0000000000000005" "0000000000000002";
  wk_queue_outcome outcomes[] = {wk_queue_ack_outcome(), wk_queue_retry_outcome(), wk_queue_retry_after(50)};
  const char *tails[] = {"0000000000000000" "0000000000000000", "0000000000000001" "ffffffffffffffff",
                         "0000000000000001" "0000000000000032"};
  for (int i = 0; i < 3; i++) {
    wk_queue_settlement s = {5, 2, outcomes[i]};
    wk_writer w = {0};
    wk_queue_encode_settlements(&w, &s, 1);
    char want[256];
    snprintf(want, sizeof want, "%s%s", head, tails[i]);
    CHECK_STR(hex_of(&w), want);
    free(w.ptr);
  }
}

static void test_malformed(void) {
  wk_queue_messages m;
  CHECK(!wk_queue_decode_messages("", 0, &m));
  /* A count promising more than the buffer holds. */
  uint64_t seven = 7;
  wk_writer w = {0};
  wk_queue_encode_counts(&w, &seven, 1);
  w.ptr[7] = 2;
  uint64_t *cs;
  size_t n;
  CHECK(!wk_queue_decode_counts(w.ptr, w.len, &cs, &n));
  w.ptr[7] = 1;
  /* Trailing bytes are refused too. */
  wk_write_bytes(&w, "", 0);
  CHECK(!wk_queue_decode_counts(w.ptr, w.len - 3, &cs, &n));
  free(w.ptr);
  /* An unknown outcome kind. */
  wk_writer k = {0};
  wk_write_u64(&k, 1);
  wk_write_u64(&k, 1);
  wk_write_u64(&k, 1);
  wk_write_u64(&k, 9);
  wk_write_u64(&k, 0);
  wk_queue_settlement *s;
  CHECK(!wk_queue_decode_settlements(k.ptr, k.len, &s, &n));
  free(k.ptr);
  /* An attempt past u32::MAX (Rust: u32::try_from fails). */
  wk_writer a = {0};
  wk_write_u64(&a, 1);
  wk_write_u64(&a, 1);
  wk_write_u64(&a, 0x100000000ULL);
  wk_write_u64(&a, 0);
  wk_write_bytes(&a, "x", 1);
  CHECK(!wk_queue_decode_messages(a.ptr, a.len, &m));
  free(a.ptr);
  /* A dead letter's reason must be UTF-8 (Rust: String::from_utf8). */
  wk_writer d = {0};
  wk_write_u64(&d, 1);
  wk_write_u64(&d, 1);
  wk_write_u64(&d, 1);
  wk_write_u64(&d, 0);
  wk_write_bytes(&d, "x", 1);
  wk_write_u64(&d, 9);
  wk_write_bytes(&d, "\xff", 1);
  wk_queue_dead_letters dl;
  CHECK(!wk_queue_decode_dead(d.ptr, d.len, &dl));
  free(d.ptr);
  /* A huge message count fails at the end of the buffer. */
  static const uint8_t huge[] = {0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff};
  CHECK(!wk_queue_decode_messages(huge, sizeof huge, &m));
  CHECK(!wk_queue_decode_counts(huge, sizeof huge, &cs, &n));
}

static void test_utf8(void) {
  CHECK(wk_utf8_valid("", 0));
  CHECK(wk_utf8_valid("cl\xc3\xa9 \xe2\x82\xac \xf0\x9f\x98\x80", 13));
  CHECK(!wk_utf8_valid("\xc3", 1));           /* truncated */
  CHECK(!wk_utf8_valid("\xc0\xaf", 2));       /* overlong */
  CHECK(!wk_utf8_valid("\xed\xa0\x80", 3));   /* a surrogate */
  CHECK(!wk_utf8_valid("\xf4\x90\x80\x80", 4)); /* past U+10FFFF */
  CHECK(!wk_utf8_valid("\x80", 1));
}

/* ------------------------------------------------ clients, fake transport */

/* What the last call sent, and what the next one answers. */
static struct {
  char address[128], method[16], path[4096], headers[512];
  wk_bytes body;
  int calls;
  bool fail;
  const char *fail_kind, *fail_message;
  uint16_t status;
  wk_writer reply;
  /* For a list: one reply per shard, by the address's shard number. */
  wk_writer pages[4];
  bool paged;
} fake;

static void fake_reset(uint16_t status) {
  free(fake.reply.ptr);
  free((void *)fake.body.ptr);
  for (int i = 0; i < 4; i++) free(fake.pages[i].ptr);
  memset(&fake, 0, sizeof fake);
  fake.status = status;
}

static void fake_reply_text(const char *text) {
  wk_bytes b = wk_bytes_dup(text, strlen(text));
  free(fake.reply.ptr);
  fake.reply.ptr = (uint8_t *)b.ptr;
  fake.reply.len = fake.reply.cap = b.len;
}

static const char *error_message = "", *error_kind = "";
const char *wk_error(void) { return error_message; }
const char *wk_error_kind(void) { return error_kind; }

static int64_t clock_ms = 1000;
int64_t wk_now_ms(void) { return clock_ms; }

bool wk_call_with_headers(const char *address, const char *method, const char *path, const wk_header *headers,
                          size_t count, const void *body, size_t body_len, wk_call_response *out) {
  fake.calls++;
  snprintf(fake.address, sizeof fake.address, "%s", address);
  snprintf(fake.method, sizeof fake.method, "%s", method);
  snprintf(fake.path, sizeof fake.path, "%s", path);
  fake.headers[0] = 0;
  for (size_t i = 0; i < count; i++) {
    size_t at = strlen(fake.headers);
    snprintf(fake.headers + at, sizeof fake.headers - at, "%s: %s\n", headers[i].name, headers[i].value);
  }
  free((void *)fake.body.ptr);
  fake.body = wk_bytes_dup(body ? body : "", body ? body_len : 0);
  memset(out, 0, sizeof *out);
  if (fake.fail) {
    error_kind = fake.fail_kind;
    error_message = fake.fail_message;
    return false;
  }
  const wk_writer *reply = &fake.reply;
  if (fake.paged) {
    const char *dash = strrchr(address, '-');
    reply = &fake.pages[dash ? atoi(dash + 1) % 4 : 0];
  }
  out->status = fake.status;
  out->body = wk_bytes_dup(reply->ptr ? (const char *)reply->ptr : "", reply->len);
  out->raw_ = (void *)out->body.ptr;
  return true;
}

void wk_call_response_free(wk_call_response *r) {
  free(r->raw_);
  memset(r, 0, sizeof *r);
}

static void test_kv_client(void) {
  wk_kv kv = wk_kv_client("kv", 4);
  wk_kv_error err;
  uint64_t v = 0;

  /* put: the shard Rust picks, the key in hex, durable, no condition. */
  fake_reset(200);
  wk_write_u64(&fake.reply, 7);
  CHECK(wk_kv_put(&kv, B("user:1"), B("Ada"), &v, &err) && v == 7);
  CHECK_STR(fake.address, "kv/shard-3");
  CHECK_STR(fake.method, "PUT");
  CHECK_STR(fake.path, "/wasmkeep-kv/v1/k/757365723a31");
  CHECK_STR(fake.headers, "x-wasmkeep-kv-expect: any\nx-wasmkeep-kv-durable: 1\n");
  CHECK(same(fake.body, B("Ada")));

  /* put_with: every option, in the Rust order. */
  wk_kv_put_options o = {wk_kv_expect_version(9), true, 1500, true};
  CHECK(wk_kv_put_with(&kv, B("user:1"), B("x"), &o, NULL, &err));
  CHECK_STR(fake.headers, "x-wasmkeep-kv-expect: 9\nx-wasmkeep-kv-durable: 0\nx-wasmkeep-kv-ttl-ms: 1500\n");
  uint64_t expected = 3;
  CHECK(wk_kv_compare_and_set(&kv, B("k"), &expected, B("x"), NULL, &err));
  CHECK(strstr(fake.headers, "x-wasmkeep-kv-expect: 3\n") != NULL);
  CHECK(wk_kv_compare_and_set(&kv, B("k"), NULL, B("x"), NULL, &err));
  CHECK(strstr(fake.headers, "x-wasmkeep-kv-expect: absent\n") != NULL);

  /* Statuses: 409 is a conflict, others are refused with the body. */
  fake_reset(409);
  CHECK(!wk_kv_put(&kv, B("k"), B("x"), &v, &err) && err.kind == WK_KV_CONFLICT);
  CHECK_STR(err.message, "conflict: the key changed");
  fake_reset(500);
  fake_reply_text("  storage: disk full\n");
  CHECK(!wk_kv_put(&kv, B("k"), B("x"), &v, &err) && err.kind == WK_KV_REFUSED && err.status == 500);
  CHECK_STR(err.message, "shard answered 500: storage: disk full");
  fake_reset(200); /* an empty 200 is malformed */
  CHECK(!wk_kv_put(&kv, B("k"), B("x"), &v, &err) && err.kind == WK_KV_REFUSED && err.status == 200);
  CHECK_STR(err.message, "shard answered 200: malformed reply from shard");

  /* A failed call, worded as Rust's wire::call. */
  fake_reset(200);
  fake.fail = true;
  fake.fail_kind = "timeout";
  fake.fail_message = "timeout";
  CHECK(!wk_kv_put(&kv, B("user:1"), B("x"), &v, &err) && err.kind == WK_KV_CALL);
  CHECK_STR(err.message, "call to kv/shard-3 timed out");
  fake.fail_kind = "failed";
  fake.fail_message = "no route";
  CHECK(wk_kv_get(&kv, B("user:1"), &(wk_kv_entry){0}, &err) == -1);
  CHECK_STR(err.message, "call to kv/shard-3 failed: no route");
  fake.fail_kind = "reentrant";
  CHECK(!wk_kv_delete(&kv, B("user:1"), NULL, &err));
  CHECK_STR(err.message, "call to kv/shard-3 would re-enter an actor already in this call chain");

  /* Keys past 1 KiB are refused before any call. */
  char long_key[WK_KV_MAX_KEY_LEN + 1];
  memset(long_key, 'k', sizeof long_key);
  fake_reset(200);
  CHECK(!wk_kv_put(&kv, wk_bytes_of(long_key, sizeof long_key), B("x"), &v, &err) &&
        err.kind == WK_KV_KEY_TOO_LONG && fake.calls == 0);
  CHECK_STR(err.message, "key longer than 1024 bytes");
  wk_write_u64(&fake.reply, 1);
  CHECK(wk_kv_put(&kv, wk_bytes_of(long_key, WK_KV_MAX_KEY_LEN), B("x"), &v, &err));

  /* get: 200 decodes, 404 is absent. */
  fake_reset(200);
  wk_kv_entry e = {B("Ada"), 5, 0};
  wk_kv_encode_entry(&fake.reply, &e);
  wk_kv_entry got;
  CHECK(wk_kv_get(&kv, B("user:1"), &got, &err) == 1 && got.version == 5 && same(got.value, B("Ada")));
  CHECK_STR(fake.method, "GET");
  CHECK_STR(fake.headers, "");
  wk_kv_entry_free(&got);
  fake.status = 404;
  CHECK(wk_kv_get(&kv, B("user:1"), &got, &err) == 0);

  /* delete and delete_if. */
  fake_reset(200);
  wk_write_u64(&fake.reply, 1);
  bool removed = false;
  CHECK(wk_kv_delete(&kv, B("k"), &removed, &err) && removed);
  CHECK_STR(fake.method, "DELETE");
  CHECK_STR(fake.headers, "x-wasmkeep-kv-expect: any\n");
  CHECK(wk_kv_delete_if(&kv, B("k"), 12, &removed, &err));
  CHECK_STR(fake.headers, "x-wasmkeep-kv-expect: 12\n");

  /* list: every shard, the same path, merged in byte order. */
  fake_reset(200);
  fake.paged = true;
  const char *pages[4][2] = {{"item:03", NULL}, {"item:00", "item:04"}, {NULL, NULL}, {"item:01", "item:02"}};
  for (int s = 0; s < 4; s++) {
    wk_bytes ks[2];
    size_t n = 0;
    for (int i = 0; i < 2; i++)
      if (pages[s][i]) ks[n++] = B(pages[s][i]);
    wk_kv_encode_keys(&fake.pages[s], ks, n);
  }
  wk_kv_keys keys;
  wk_bytes after = B("item:00");
  CHECK(wk_kv_list(&kv, B("item:"), &after, 3, &keys, &err) && fake.calls == 4);
  CHECK_STR(fake.address, "kv/shard-3");
  CHECK_STR(fake.path, "/wasmkeep-kv/v1/list?prefix=6974656d3a&limit=3&after=6974656d3a3030");
  CHECK(keys.count == 3 && same(keys.keys[0], B("item:00")) && same(keys.keys[2], B("item:02")));
  wk_kv_keys_free(&keys);
  CHECK(wk_kv_list(&kv, B(""), NULL, 1000, &keys, &err) && keys.count == 5);
  CHECK_STR(fake.path, "/wasmkeep-kv/v1/list?prefix=&limit=1000");
  wk_kv_keys_free(&keys);
}

static void test_read_cache(void) {
  wk_kv kv = wk_kv_with_read_cache(wk_kv_client("cached", 2), 1000);
  wk_kv_error err;
  wk_kv_entry got;
  fake_reset(200);
  wk_kv_entry e = {B("off"), 1, 0};
  wk_kv_encode_entry(&fake.reply, &e);
  CHECK(wk_kv_get(&kv, B("flag"), &got, &err) == 1 && fake.calls == 1);
  wk_kv_entry_free(&got);
  /* Served from the cache until it is max_age old. */
  CHECK(wk_kv_get(&kv, B("flag"), &got, &err) == 1 && fake.calls == 1 && same(got.value, B("off")));
  wk_kv_entry_free(&got);
  CHECK(wk_kv_get_fresh(&kv, B("flag"), &got, &err) == 1 && fake.calls == 2);
  wk_kv_entry_free(&got);
  clock_ms += 1000;
  CHECK(wk_kv_get(&kv, B("flag"), &got, &err) == 1 && fake.calls == 3);
  wk_kv_entry_free(&got);
  /* Its own writes update it, with the expiry the write asked for. */
  fake_reset(200);
  wk_write_u64(&fake.reply, 2);
  wk_kv_put_options o = {wk_kv_expect_any(), true, 50, false};
  CHECK(wk_kv_put_with(&kv, B("flag"), B("on"), &o, NULL, &err));
  CHECK(wk_kv_get(&kv, B("flag"), &got, &err) == 1 && fake.calls == 1 && got.version == 2 &&
        got.expires_at == (uint64_t)clock_ms + 50 && same(got.value, B("on")));
  wk_kv_entry_free(&got);
  clock_ms += 50; /* expired while cached: absent, without a call */
  CHECK(wk_kv_get(&kv, B("flag"), &got, &err) == 0 && fake.calls == 1);
  /* A delete caches the absence; a conflict forgets the key. */
  fake_reset(200);
  wk_write_u64(&fake.reply, 1);
  CHECK(wk_kv_delete(&kv, B("flag"), NULL, &err));
  CHECK(wk_kv_get(&kv, B("flag"), &got, &err) == 0 && fake.calls == 1);
  fake_reset(409);
  CHECK(!wk_kv_put(&kv, B("flag"), B("x"), NULL, &err));
  fake.status = 404;
  CHECK(wk_kv_get(&kv, B("flag"), &got, &err) == 0 && fake.calls == 2);
  /* A client without a cache never uses it. */
  wk_kv plain = wk_kv_client("cached", 2);
  CHECK(wk_kv_get(&plain, B("flag"), &got, &err) == 0 && fake.calls == 3);
}

static void test_queue_client(void) {
  wk_queue q = wk_queue_client("queue/inbox");
  wk_queue_error err;
  uint64_t id;

  fake_reset(200);
  uint64_t one = 41;
  wk_queue_encode_counts(&fake.reply, &one, 1);
  CHECK(wk_queue_send(&q, B("hello"), &id, &err) && id == 41);
  CHECK_STR(fake.address, "queue/inbox");
  CHECK_STR(fake.method, "POST");
  CHECK_STR(fake.path, "/wasmkeep-queue/v1/send");
  CHECK_STR(fake.headers, "x-wasmkeep-queue-delay-ms: 0\n");
  wk_writer body = {0};
  wk_bytes hello = B("hello");
  wk_queue_encode_bodies(&body, &hello, 1);
  CHECK(fake.body.len == body.len && memcmp(fake.body.ptr, body.ptr, body.len) == 0);
  free(body.ptr);
  CHECK(wk_queue_send_delayed(&q, B("later"), 3600000, &id, &err));
  CHECK_STR(fake.headers, "x-wasmkeep-queue-delay-ms: 3600000\n");
  /* A reply with another number of ids than bodies is malformed. */
  wk_bytes two[] = {B("a"), B("b")};
  uint64_t ids[2];
  CHECK(!wk_queue_send_batch(&q, two, 2, ids, &err) && err.kind == WK_QUEUE_REFUSED && err.status == 200);
  CHECK_STR(err.message, "queue answered 200: malformed reply from queue");

  fake_reset(400);
  fake_reply_text("at most 1000 messages per send\n");
  CHECK(!wk_queue_send(&q, B("x"), &id, &err) && err.kind == WK_QUEUE_REFUSED && err.status == 400);
  CHECK_STR(err.message, "queue answered 400: at most 1000 messages per send");

  fake_reset(200);
  wk_queue_message m = {7, 1, 5, B("hello")};
  wk_queue_encode_messages(&fake.reply, &m, 1);
  wk_queue_messages leased;
  CHECK(wk_queue_lease(&q, 10, 30000, &leased, &err) && leased.count == 1 && leased.items[0].id == 7 &&
        same(leased.items[0].body, B("hello")));
  CHECK_STR(fake.path, "/wasmkeep-queue/v1/lease?max=10&visibility_ms=30000");
  CHECK_STR(fake.headers, "");
  wk_queue_messages_free(&leased);

  fake_reset(200);
  uint64_t flag = 1;
  wk_queue_encode_counts(&fake.reply, &flag, 1);
  bool current = false;
  CHECK(wk_queue_ack(&q, 7, 1, &current, &err) && current);
  CHECK_STR(fake.path, "/wasmkeep-queue/v1/settle");
  wk_writer settle = {0};
  wk_queue_settlement s = {7, 1, wk_queue_ack_outcome()};
  wk_queue_encode_settlements(&settle, &s, 1);
  CHECK(fake.body.len == settle.len && memcmp(fake.body.ptr, settle.ptr, settle.len) == 0);
  free(settle.ptr);
  uint64_t delay = 500;
  CHECK(wk_queue_retry(&q, 7, 1, &delay, &current, &err));
  CHECK_STR(hex_of(&(wk_writer){(uint8_t *)fake.body.ptr, fake.body.len, 0}),
            "0000000000000001" "0000000000000007" "0000000000000001" "0000000000000001" "00000000000001f4");
  /* Items the reply does not mention were not current. */
  wk_queue_settlement both[] = {{7, 1, wk_queue_ack_outcome()}, {8, 1, wk_queue_retry_outcome()}};
  bool flags[2] = {false, true};
  CHECK(wk_queue_settle(&q, both, 2, flags, &err) && flags[0] && !flags[1]);

  fake_reset(200);
  uint64_t counts[] = {1, 2, 3, 4};
  wk_queue_encode_counts(&fake.reply, counts, 4);
  wk_queue_stats st;
  CHECK(wk_queue_get_stats(&q, &st, &err) && st.ready == 1 && st.delayed == 2 && st.in_flight == 3 && st.dead == 4);
  CHECK_STR(fake.method, "GET");
  CHECK_STR(fake.path, "/wasmkeep-queue/v1/stats");

  fake_reset(200);
  wk_queue_dead_letter d = {m, 9, B("attempts exhausted")};
  wk_queue_encode_dead(&fake.reply, &d, 1);
  wk_queue_dead_letters dead;
  CHECK(wk_queue_get_dead_letters(&q, 100, &dead, &err) && dead.count == 1 &&
        same(dead.items[0].reason, B("attempts exhausted")));
  CHECK_STR(fake.path, "/wasmkeep-queue/v1/dead?limit=100");
  wk_queue_dead_letters_free(&dead);

  fake_reset(200);
  fake.fail = true;
  fake.fail_kind = "invalid-address";
  fake.fail_message = "no class";
  CHECK(!wk_queue_send(&q, B("x"), &id, &err) && err.kind == WK_QUEUE_CALL);
  CHECK_STR(err.message, "invalid address queue/inbox: no class");
  fake.fail_kind = "too-large";
  CHECK(!wk_queue_send(&q, B("x"), &id, &err));
  CHECK_STR(err.message, "call to queue/inbox: body too large");
  fake_reset(200);
}

/* ------------------------------------------------------- delivery handler */

static const char *caller;
static wk_bytes request_body;

const char *wk_request_header(wk_request *req, const char *name) {
  (void)req;
  return strcmp(name, "x-wasmkeep-caller") == 0 ? caller : NULL;
}

wk_bytes wk_request_body(wk_request *req) {
  (void)req;
  return request_body;
}

wk_response wk_reply_static(uint16_t status, const char *content_type, const void *body, size_t len) {
  wk_response r = {status, content_type, {(const char *)body, len}, false, NULL};
  return r;
}

wk_response wk_reply_copy(uint16_t status, const char *content_type, const void *body, size_t len) {
  wk_response r = {status, content_type, wk_bytes_dup(body, len), true, NULL};
  return r;
}

static int handled;

static bool worker(const char *queue, const wk_queue_message *m, wk_queue_outcome *outcome, void *ctx) {
  CHECK_STR(queue, "default/queue/jobs");
  CHECK(ctx == &handled);
  handled++;
  if (m->body.len >= 4 && memcmp(m->body.ptr, "fail", 4) == 0) *outcome = wk_queue_retry_outcome();
  else if (m->body.len >= 5 && memcmp(m->body.ptr, "later", 5) == 0) *outcome = wk_queue_retry_after(50);
  else if (m->body.len >= 5 && memcmp(m->body.ptr, "crash", 5) == 0) return false;
  return true;
}

static void test_delivery(void) {
  wk_request req = {"POST", WK_QUEUE_DELIVER_PATH, NULL};
  CHECK(wk_queue_is_delivery(&req));
  wk_request get = {"GET", WK_QUEUE_DELIVER_PATH, NULL};
  wk_request query = {"POST", WK_QUEUE_DELIVER_PATH "?x=1", NULL};
  CHECK(!wk_queue_is_delivery(&get) && !wk_queue_is_delivery(&query));

  /* Outside clients: 403. */
  caller = NULL;
  wk_response r = wk_queue_handle_delivery(&req, worker, &handled);
  CHECK(r.status == 403 && same(r.body, B("deliveries come from queue actors\n")));
  CHECK_STR(r.content_type, "text/plain");

  /* A malformed body: 400. */
  caller = "default/queue/jobs";
  request_body = B("nope");
  r = wk_queue_handle_delivery(&req, worker, &handled);
  CHECK(r.status == 400 && same(r.body, B("malformed delivery\n")) && handled == 0);

  /* One outcome per message, in order. */
  wk_queue_message ms[] = {{1, 1, 0, B("resize 42")}, {2, 3, 0, B("fail always")}, {3, 1, 0, B("later")}};
  wk_writer w = {0};
  wk_queue_encode_messages(&w, ms, 3);
  request_body = wk_bytes_of(w.ptr, w.len);
  r = wk_queue_handle_delivery(&req, worker, &handled);
  CHECK(r.status == 200 && handled == 3);
  CHECK_STR(r.content_type, "application/octet-stream");
  wk_queue_settlement want[] = {
      {1, 1, wk_queue_ack_outcome()}, {2, 3, wk_queue_retry_outcome()}, {3, 1, wk_queue_retry_after(50)}};
  wk_writer expected = {0};
  wk_queue_encode_settlements(&expected, want, 3);
  CHECK(r.body.len == expected.len && memcmp(r.body.ptr, expected.ptr, expected.len) == 0);
  free((void *)r.body.ptr);
  free(expected.ptr);
  free(w.ptr);

  /* A handler that fails: 500, and the queue retries the batch. */
  wk_queue_message crash = {4, 1, 0, B("crash")};
  wk_writer c = {0};
  wk_queue_encode_messages(&c, &crash, 1);
  request_body = wk_bytes_of(c.ptr, c.len);
  r = wk_queue_handle_delivery(&req, worker, &handled);
  CHECK(r.status == 500);
  free(c.ptr);

  /* An empty batch answers an empty list. */
  wk_writer empty = {0};
  wk_queue_encode_messages(&empty, NULL, 0);
  request_body = wk_bytes_of(empty.ptr, empty.len);
  r = wk_queue_handle_delivery(&req, worker, &handled);
  CHECK(r.status == 200 && r.body.len == 8);
  free((void *)r.body.ptr);
  free(empty.ptr);
}

/* ------------------------------------------------------------ workflow */

/* `hex` (lowercase, even length) as bytes into `out`; returns the count. */
static size_t unhex(uint8_t *out, const char *hex) {
  size_t n = strlen(hex) / 2;
  for (size_t i = 0; i < n; i++) {
    unsigned v = 0;
    sscanf(hex + 2 * i, "%2x", &v);
    out[i] = (uint8_t)v;
  }
  return n;
}

/* {waiting, wake_at 42, waiting_for [approved, paid], no output, no error,
 * positions 3}, as the Rust encode_status writes it. */
static const char GOLDEN_STATUS[] = "0000000777616974696e67" "000000000000002a"
                                    "0000000d617070726f7665640a70616964" "0000000000000000" "00000000" "00000000"
                                    "0000000000000003";

static void test_workflow_status(void) {
  uint8_t buf[256];
  size_t n = unhex(buf, GOLDEN_STATUS);
  wk_workflow_status s;
  CHECK(wk_workflow_decode_status(buf, n, &s));
  CHECK(s.state == WK_WORKFLOW_WAITING && s.wake_at == 42 && s.positions == 3);
  CHECK(s.waiting_count == 2 && same(s.waiting_for[0], B("approved")) && same(s.waiting_for[1], B("paid")));
  CHECK_STR(s.waiting_for[1].ptr, "paid"); /* NUL-terminated */
  CHECK(!s.has_output && s.output.len == 0 && s.error.len == 0);
  CHECK(!wk_workflow_state_is_finished(s.state));
  /* And back, byte for byte. */
  wk_writer w = {0};
  wk_workflow_encode_status(&w, &s);
  CHECK_STR(hex_of(&w), GOLDEN_STATUS);
  free(w.ptr);
  wk_workflow_status_free(&s);
  CHECK(s.waiting_for == NULL && s.waiting_count == 0);

  /* Every state, with an output (bytes, a NUL inside) and an error. */
  wk_bytes names[1] = {B("x")};
  for (int st = WK_WORKFLOW_RUNNING; st <= WK_WORKFLOW_CANCELLED; st++) {
    wk_workflow_status in = {(wk_workflow_state)st, 0, names, 1, true, wk_bytes_of("shipped \0 book", 14),
                             B("boom"), 9};
    wk_writer e = {0};
    wk_workflow_encode_status(&e, &in);
    wk_workflow_status out;
    CHECK(wk_workflow_decode_status(e.ptr, e.len, &out));
    CHECK(out.state == (wk_workflow_state)st && out.has_output && same(out.output, in.output));
    CHECK(same(out.error, B("boom")) && out.positions == 9 && out.waiting_count == 1);
    wk_workflow_status_free(&out);
    free(e.ptr);
  }
  CHECK_STR(wk_workflow_state_name(WK_WORKFLOW_CANCELLED), "cancelled");
  CHECK(wk_workflow_state_is_finished(WK_WORKFLOW_COMPLETED) && wk_workflow_state_is_finished(WK_WORKFLOW_FAILED) &&
        wk_workflow_state_is_finished(WK_WORKFLOW_CANCELLED) && !wk_workflow_state_is_finished(WK_WORKFLOW_RUNNING));

  /* Empty names are dropped; has_output other than 1 is no output (Rust:
   * `== 1`); bytes after the positions are ignored (as Rust). */
  wk_writer odd = {0};
  wk_write_bytes(&odd, "running", 7);
  wk_write_u64(&odd, 0);
  wk_write_bytes(&odd, "\na\n\nb\n", 6);
  wk_write_u64(&odd, 2);
  wk_write_bytes(&odd, "ignored", 7);
  wk_write_bytes(&odd, "", 0);
  wk_write_u64(&odd, 1);
  wk_write_u64(&odd, 99); /* trailing */
  CHECK(wk_workflow_decode_status(odd.ptr, odd.len, &s));
  CHECK(s.state == WK_WORKFLOW_RUNNING && s.waiting_count == 2 && same(s.waiting_for[0], B("a")) &&
        same(s.waiting_for[1], B("b")) && !s.has_output && s.output.len == 0 && s.positions == 1);
  wk_workflow_status_free(&s);
  free(odd.ptr);

  /* Malformed: cut anywhere before the positions end, an unknown state,
   * invalid UTF-8 in the state, the names or the error. *out untouched. */
  wk_workflow_status untouched = {WK_WORKFLOW_FAILED, 7, NULL, 0, false, {"", 0}, {"", 0}, 7};
  s = untouched;
  for (size_t cut = 0; cut < n; cut++) CHECK(!wk_workflow_decode_status(buf, cut, &s));
  CHECK(s.state == WK_WORKFLOW_FAILED && s.wake_at == 7 && s.positions == 7);
  static const struct {
    const char *state, *names, *error;
    size_t state_len, names_len, error_len;
  } bad[] = {
      {"paused", "", "", 6, 0, 0},
      {"Waiting", "", "", 7, 0, 0},
      {"waiting\0", "", "", 8, 0, 0},
      {"", "", "", 0, 0, 0},
      {"wait\xc3", "", "", 5, 0, 0},
      {"waiting", "ok\n\xc3(", "", 7, 5, 0},
      {"failed", "", "\xed\xa0\x80", 6, 0, 3},
  };
  for (size_t i = 0; i < sizeof bad / sizeof *bad; i++) {
    wk_writer b = {0};
    wk_write_bytes(&b, bad[i].state, bad[i].state_len);
    wk_write_u64(&b, 0);
    wk_write_bytes(&b, bad[i].names, bad[i].names_len);
    wk_write_u64(&b, 0);
    wk_write_bytes(&b, "", 0);
    wk_write_bytes(&b, bad[i].error, bad[i].error_len);
    wk_write_u64(&b, 0);
    CHECK(!wk_workflow_decode_status(b.ptr, b.len, &s));
    free(b.ptr);
  }
  CHECK(s.state == WK_WORKFLOW_FAILED && s.wake_at == 7);
}

static void test_workflow_client(void) {
  wk_workflow orders = wk_workflow_client("orders");
  wk_workflow_error err;
  wk_workflow_status s;
  uint8_t golden[256];
  size_t golden_len = unhex(golden, GOLDEN_STATUS);

  /* start: POST to <class>/<id>, /start, the input as the body. */
  fake_reset(200);
  free(fake.reply.ptr);
  fake.reply.ptr = (uint8_t *)wk_bytes_dup(golden, golden_len).ptr;
  fake.reply.len = fake.reply.cap = golden_len;
  CHECK(wk_workflow_start(&orders, B("order-1"), B("item=book"), &s, &err));
  CHECK_STR(fake.address, "orders/order-1");
  CHECK_STR(fake.method, "POST");
  CHECK_STR(fake.path, "/wasmkeep-workflow/v1/start");
  CHECK_STR(fake.headers, "");
  CHECK(same(fake.body, B("item=book")));
  CHECK(s.state == WK_WORKFLOW_WAITING && s.waiting_count == 2 && s.wake_at == 42);
  wk_workflow_status_free(&s);
  CHECK(wk_workflow_start(&orders, B("order-1"), B(""), NULL, &err)); /* status not wanted */

  /* status: GET, no body. */
  CHECK(wk_workflow_get_status(&orders, B("order-1"), &s, &err) && s.positions == 3);
  CHECK_STR(fake.method, "GET");
  CHECK_STR(fake.path, "/wasmkeep-workflow/v1/status");
  CHECK(fake.body.len == 0);
  wk_workflow_status_free(&s);

  /* cancel: POST, no body, answers the status. */
  CHECK(wk_workflow_cancel(&orders, B("order-1"), &s, &err) && s.state == WK_WORKFLOW_WAITING);
  CHECK_STR(fake.method, "POST");
  CHECK_STR(fake.path, "/wasmkeep-workflow/v1/cancel");
  CHECK(fake.body.len == 0 && fake.calls == 4);
  wk_workflow_status_free(&s);

  /* send_event: the name in lowercase hex of its UTF-8, the payload as the
   * body; the 200's body is not read. */
  CHECK(wk_workflow_send_event(&orders, B("order-1"), B("approved"), B("by ada"), &err));
  CHECK_STR(fake.path, "/wasmkeep-workflow/v1/event?name=617070726f766564");
  CHECK(same(fake.body, B("by ada")));
  CHECK(wk_workflow_send_event(&orders, B("o"), B("cl\xc3\xa9"), B(""), NULL));
  CHECK_STR(fake.address, "orders/o");
  CHECK_STR(fake.path, "/wasmkeep-workflow/v1/event?name=636cc3a9");
  CHECK(fake.calls == 6);

  /* 404 unknown, 409 finished, others refused with the trimmed body. */
  fake_reset(404);
  fake_reply_text("no such workflow instance\n");
  CHECK(!wk_workflow_get_status(&orders, B("nope"), &s, &err) && err.kind == WK_WORKFLOW_UNKNOWN);
  CHECK_STR(err.message, "no such workflow instance");
  CHECK(!wk_workflow_send_event(&orders, B("nope"), B("approved"), B(""), &err) && err.kind == WK_WORKFLOW_UNKNOWN);
  CHECK(!wk_workflow_cancel(&orders, B("nope"), &s, &err) && err.kind == WK_WORKFLOW_UNKNOWN);
  fake_reset(409);
  CHECK(!wk_workflow_send_event(&orders, B("done"), B("approved"), B(""), &err) && err.kind == WK_WORKFLOW_FINISHED);
  CHECK_STR(err.message, "the workflow instance has finished");
  fake_reset(403);
  fake_reply_text("  only actors may call this\n");
  CHECK(!wk_workflow_start(&orders, B("x"), B(""), &s, &err) && err.kind == WK_WORKFLOW_REFUSED && err.status == 403);
  CHECK_STR(err.message, "refused (403): only actors may call this");
  CHECK(!wk_workflow_cancel(&orders, B("x"), &s, NULL)); /* no error wanted */

  /* A malformed (or empty) 200 is refused with status 200. */
  fake_reset(200);
  CHECK(!wk_workflow_start(&orders, B("x"), B(""), &s, &err) && err.kind == WK_WORKFLOW_REFUSED && err.status == 200);
  CHECK_STR(err.message, "refused (200): malformed status");
  fake_reply_text("garbage");
  CHECK(!wk_workflow_get_status(&orders, B("x"), &s, &err) && err.status == 200);
  /* ... but not for an event, whose answer is not read. */
  CHECK(wk_workflow_send_event(&orders, B("x"), B("e"), B(""), &err));

  /* A failed call, worded as Rust's wire::call. */
  fake_reset(200);
  fake.fail = true;
  fake.fail_kind = "timeout";
  fake.fail_message = "timeout";
  CHECK(!wk_workflow_start(&orders, B("order-1"), B(""), &s, &err) && err.kind == WK_WORKFLOW_CALL);
  CHECK_STR(err.message, "call to orders/order-1 timed out");
  fake.fail_kind = "invalid-address";
  fake.fail_message = "bad name";
  CHECK(!wk_workflow_send_event(&orders, B("a/b"), B("e"), B(""), &err) && err.kind == WK_WORKFLOW_CALL);
  CHECK_STR(err.message, "invalid address orders/a/b: bad name");
}

int main(void) {
  test_framing();
  test_hex();
  test_shard_hash();
  test_expectations();
  test_entries_and_keys();
  test_merge();
  test_cache();
  test_queue_frames();
  test_settlement_bytes();
  test_malformed();
  test_utf8();
  test_kv_client();
  test_read_cache();
  test_queue_client();
  test_delivery();
  test_workflow_status();
  test_workflow_client();
  fake_reset(0);
  if (failures) {
    fprintf(stderr, "%d of %d checks failed\n", failures, checks);
    return 1;
  }
  printf("ok: %d checks passed\n", checks);
  return 0;
}
