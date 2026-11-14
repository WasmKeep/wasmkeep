/* The WasmKeep C SDK: write actors as a few plain C functions.
 *
 *   #include "wasmkeep.h"
 *
 *   void wk_init(void) {
 *     wk_exec("CREATE TABLE IF NOT EXISTS hits (at INTEGER NOT NULL)");
 *   }
 *
 *   wk_response wk_fetch(wk_request *req) {
 *     wk_exec("INSERT INTO hits (at) VALUES (?)", wk_int(wk_now_ms()));
 *     return wk_reply(200, "%lld hits on %s\n", (long long)wk_query_int("SELECT count(*) FROM hits"), req->path);
 *   }
 *
 * Define only the hooks you need; the others have defaults (fetch answers
 * 404, a WebSocket is accepted and ignored). Every hook runs one event at a
 * time per actor: no locks needed.
 *
 * Build: compile wasmkeep.c, wasmkeep_kv.c, wasmkeep_wire.c and
 * wasmkeep_workflow.c and your file together with the bindings `wit-bindgen c`
 * generates for the `actor` world (see examples/c-counter). The key-value
 * and queue clients are declared in wasmkeep_kv.h, the workflow client in
 * wasmkeep_workflow.h; this header includes both.
 *
 * Strings passed in are NUL-terminated. Text and blobs read back are
 * `wk_bytes` (pointer and length, not NUL-terminated: print them with
 * "%.*s", (int)b.len, b.ptr), valid until the rows or response holding them
 * are freed. */
#ifndef WASMKEEP_H
#define WASMKEEP_H

/* This SDK's version: the WasmKeep release it comes with, the server's
 * version (scripts/check-versions.py keeps them equal). */
#define WASMKEEP_SDK_VERSION "0.4.0"

#include <stdarg.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ------------------------------------------------------------- values */

typedef struct wk_bytes {
  const char *ptr;
  size_t len;
} wk_bytes;

typedef enum wk_type { WK_NULL, WK_INTEGER, WK_REAL, WK_TEXT, WK_BLOB } wk_type;

/* A value bound to or read from SQL. */
typedef struct wk_value {
  wk_type type;
  union {
    int64_t integer;
    double real;
    wk_bytes bytes; /* text or blob */
  } as;
} wk_value;

static inline wk_value wk_null(void) { wk_value v = {WK_NULL, {0}}; return v; }
static inline wk_value wk_int(int64_t i) { wk_value v = {WK_INTEGER, {0}}; v.as.integer = i; return v; }
static inline wk_value wk_real(double r) { wk_value v = {WK_REAL, {0}}; v.as.real = r; return v; }
static inline wk_value wk_text_n(const char *s, size_t n) { wk_value v = {WK_TEXT, {0}}; v.as.bytes.ptr = s; v.as.bytes.len = n; return v; }
static inline wk_value wk_text(const char *s) { return wk_text_n(s, strlen(s)); }
static inline wk_value wk_blob(const void *p, size_t n) { wk_value v = {WK_BLOB, {0}}; v.as.bytes.ptr = (const char *)p; v.as.bytes.len = n; return v; }

/* (params, count) from a variable list of wk_value: wk_exec(sql, wk_int(1), wk_text("x")). */
#define WK_PARAMS(...) \
  ((const wk_value[]){wk_null(), ##__VA_ARGS__} + 1), \
  (sizeof((const wk_value[]){wk_null(), ##__VA_ARGS__}) / sizeof(wk_value) - 1)

/* ------------------------------------------------------------ storage */

/* The last failure (SQL, call), NUL-terminated; "" if none. */
const char *wk_error(void);
/* SQL failures: syntax, constraint, bind, multiple-statements,
 * quota-exceeded, not-allowed, internal. Calls: invalid-address, reentrant,
 * too-large, timeout, failed. */
const char *wk_error_kind(void);

/* Runs a statement that returns no rows. Returns the rows changed, or -1
 * (see wk_error). */
int64_t wk_exec_v(const char *sql, const wk_value *params, size_t n);
#define wk_exec(sql, ...) wk_exec_v((sql), WK_PARAMS(__VA_ARGS__))

/* Like wk_exec, but returns only once the write is durable (replicated to
 * the cluster's bucket, or fsynced on a single node). Not inside a
 * transaction. */
int64_t wk_exec_durable_v(const char *sql, const wk_value *params, size_t n);
#define wk_exec_durable(sql, ...) wk_exec_durable_v((sql), WK_PARAMS(__VA_ARGS__))

/* Rows from a query. Free with wk_rows_free. */
typedef struct wk_rows {
  size_t count;   /* rows */
  size_t columns;
  void *raw_;     /* private */
} wk_rows;

/* Runs a query. Returns false on failure (see wk_error), with *rows empty. */
bool wk_query_v(wk_rows *rows, const char *sql, const wk_value *params, size_t n);
#define wk_query(rows, sql, ...) wk_query_v((rows), (sql), WK_PARAMS(__VA_ARGS__))
void wk_rows_free(wk_rows *rows);

/* A cell; WK_NULL outside the result. */
wk_value wk_get(const wk_rows *rows, size_t row, size_t column);
/* A column's index by name, or -1. */
long wk_column(const wk_rows *rows, const char *name);
static inline int64_t wk_get_int(const wk_rows *r, size_t row, size_t col) {
  wk_value v = wk_get(r, row, col);
  return v.type == WK_INTEGER ? v.as.integer : v.type == WK_REAL ? (int64_t)v.as.real : 0;
}
static inline wk_bytes wk_get_text(const wk_rows *r, size_t row, size_t col) {
  wk_value v = wk_get(r, row, col);
  wk_bytes empty = {"", 0};
  return v.type == WK_TEXT || v.type == WK_BLOB ? v.as.bytes : empty;
}

/* The first column of the first row as an integer; 0 if there is none or on
 * failure. */
int64_t wk_query_int_v(const char *sql, const wk_value *params, size_t n);
#define wk_query_int(sql, ...) wk_query_int_v((sql), WK_PARAMS(__VA_ARGS__))

/* Deletes this actor once the current event ends. */
void wk_delete_self(void);

/* This actor's own address. The strings are NUL-terminated and live as long
 * as the instance; "class_name/name" is what wk_call takes (the name is not
 * encoded). */
typedef struct wk_address {
  const char *tenant;
  const char *class_name;
  const char *name;
} wk_address;
wk_address wk_self_address(void);

/* Configuration and secrets the operator gives this class (wasmkeep serve
 * --env CLASS=FILE): the value of `name`, or NULL if it is not set. The
 * string lives as long as the instance. */
const char *wk_env(const char *name);

/* Wall-clock time, in ms since the Unix epoch. */
int64_t wk_now_ms(void);

/* ------------------------------------------------------------- alarms */

/* The actor's single alarm (ms since the Unix epoch). It fires wk_alarm()
 * at least once, also after a crash or a move to another node. */
void wk_alarm_set(int64_t at_ms);
bool wk_alarm_get(int64_t *at_ms);
void wk_alarm_clear(void);

/* --------------------------------------------------------- WebSockets */

/* Sends a message. Returns false if the connection is gone or its buffer is
 * full. */
bool wk_send(uint64_t conn, const char *text);
bool wk_send_n(uint64_t conn, const char *text, size_t len);
bool wk_send_binary(uint64_t conn, const void *data, size_t len);
/* Sends text to every open connection of this actor. */
void wk_broadcast(const char *text);
void wk_broadcast_n(const char *text, size_t len);
void wk_close(uint64_t conn, uint16_t code, const char *reason);
/* This actor's open connections: writes up to `max` ids, returns how many
 * there are. wk_connections(NULL, 0) counts them. */
size_t wk_connections(uint64_t *out, size_t max);

/* -------------------------------------------------------- actor calls */

typedef struct wk_call_response {
  uint16_t status;
  wk_bytes body;
  void *raw_; /* private */
} wk_call_response;

/* Calls actor "class/name" (same tenant) and waits for its response. Other
 * events to this actor wait meanwhile. Returns false if no answer came (see
 * wk_error_kind). Free the response with wk_call_response_free. */
bool wk_call(const char *address, const char *method, const char *path,
             const void *body, size_t body_len, wk_call_response *out);

/* A request header for wk_call_with_headers (NUL-terminated strings). */
typedef struct wk_header {
  const char *name;
  const char *value;
} wk_header;

/* wk_call with `count` request headers. The callee reads them as any
 * request's headers, except `x-wasmkeep-caller`, which the host sets. */
bool wk_call_with_headers(const char *address, const char *method, const char *path,
                          const wk_header *headers, size_t count,
                          const void *body, size_t body_len, wk_call_response *out);
void wk_call_response_free(wk_call_response *response);

/* --------------------------------------------------------------- HTTP */

typedef struct wk_request {
  const char *method; /* "GET", "POST", ... */
  const char *path;   /* with the query, relative to the actor */
  void *raw_;         /* private */
} wk_request;

/* The request body, read on first use. */
wk_bytes wk_request_body(wk_request *req);
/* A header's value (NUL-terminated), or NULL. Names are lowercase. */
const char *wk_request_header(wk_request *req, const char *name);

typedef struct wk_response {
  uint16_t status;
  const char *content_type; /* NULL: text/plain; charset=utf-8 */
  wk_bytes body;
  bool owned_;              /* private: body was allocated by wk_reply */
  void *headers_;           /* private: wk_response_header's list */
} wk_response;

/* A response whose body is printf-formatted. */
wk_response wk_reply(uint16_t status, const char *fmt, ...) __attribute__((format(printf, 2, 3)));
wk_response wk_vreply(uint16_t status, const char *fmt, va_list args);
/* A response with a body that stays valid after the handler returns (static
 * data), or one that is copied (wk_reply_copy). */
wk_response wk_reply_static(uint16_t status, const char *content_type, const void *body, size_t len);
wk_response wk_reply_copy(uint16_t status, const char *content_type, const void *body, size_t len);

/* Adds a header to a response (`etag`, `cache-control`, ...). Name and
 * value are copied, so they may be temporary; the SDK frees them once the
 * response is sent. */
void wk_response_header_n(wk_response *r, const char *name, const char *value, size_t value_len);
static inline void wk_response_header(wk_response *r, const char *name, const char *value) {
  wk_response_header_n(r, name, value, strlen(value));
}

/* Fails the current init or alarm event (it is retried); the message shows
 * in the host's logs. */
void wk_fail(const char *message);

/* -------------------------------------------------------------- hooks */

/* Define the ones you need; each has a default. */
void wk_init(void);
wk_response wk_fetch(wk_request *req);
void wk_alarm(void);
/* Return false to refuse the connection. */
bool wk_ws_open(uint64_t conn, const char *path);
void wk_ws_message(uint64_t conn, wk_bytes data, bool binary);
void wk_ws_close(uint64_t conn, uint16_t code, const char *reason);

#ifdef __cplusplus
}
#endif

/* Key-value and queue clients (ADR 0015): wasmkeep_kv.c. */
#include "wasmkeep_kv.h"
/* The workflow client (ADR 0019): wasmkeep_workflow.c. */
#include "wasmkeep_workflow.h"

#endif
