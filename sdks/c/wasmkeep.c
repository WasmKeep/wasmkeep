/* The WasmKeep C SDK, on top of the bindings `wit-bindgen c` generates for
 * the `actor` world (actor.h). See wasmkeep.h. */
#include "wasmkeep.h"

#include <stdio.h>
#include <stdlib.h>
#include <time.h>

#include "actor.h"

#define WK_WEAK __attribute__((weak))

/* -------------------------------------------------------------- errors */

static char error_message[512];
static const char *error_kind = "";

static void set_error(const char *kind, const uint8_t *msg, size_t len) {
  error_kind = kind;
  if (len >= sizeof error_message) len = sizeof error_message - 1;
  memcpy(error_message, msg, len);
  error_message[len] = 0;
}

const char *wk_error(void) { return error_message; }
const char *wk_error_kind(void) { return error_kind; }

/* ------------------------------------------------------------- storage */

static actor_string_t str(const char *s) {
  actor_string_t out = {(uint8_t *)s, strlen(s)};
  return out;
}

static wasmkeep_actor_storage_list_value_t to_params(const wk_value *params, size_t n) {
  wasmkeep_actor_storage_list_value_t list = {NULL, n};
  if (n == 0) return list;
  list.ptr = calloc(n, sizeof *list.ptr);
  for (size_t i = 0; i < n; i++) {
    wasmkeep_actor_types_value_t *v = &list.ptr[i];
    switch (params[i].type) {
      case WK_NULL: v->tag = WASMKEEP_ACTOR_TYPES_VALUE_NULL; break;
      case WK_INTEGER: v->tag = WASMKEEP_ACTOR_TYPES_VALUE_INTEGER; v->val.integer = params[i].as.integer; break;
      case WK_REAL: v->tag = WASMKEEP_ACTOR_TYPES_VALUE_REAL; v->val.real = params[i].as.real; break;
      case WK_TEXT:
        v->tag = WASMKEEP_ACTOR_TYPES_VALUE_TEXT;
        v->val.text.ptr = (uint8_t *)params[i].as.bytes.ptr;
        v->val.text.len = params[i].as.bytes.len;
        break;
      case WK_BLOB:
        v->tag = WASMKEEP_ACTOR_TYPES_VALUE_BLOB;
        v->val.blob.ptr = (uint8_t *)params[i].as.bytes.ptr;
        v->val.blob.len = params[i].as.bytes.len;
        break;
    }
  }
  return list;
}

static const char *sql_kind(wasmkeep_actor_types_sql_error_kind_t kind) {
  static const char *const names[] = {"syntax", "constraint", "bind", "multiple-statements",
                                      "quota-exceeded", "not-allowed", "internal"};
  return kind < sizeof names / sizeof *names ? names[kind] : "internal";
}

static void sql_failed(wasmkeep_actor_storage_sql_error_t *err) {
  set_error(sql_kind(err->kind), err->message.ptr, err->message.len);
  wasmkeep_actor_storage_sql_error_free(err);
}

typedef bool (*exec_fn)(actor_string_t *, wasmkeep_actor_storage_list_value_t *, uint64_t *,
                        wasmkeep_actor_storage_sql_error_t *);

static int64_t run_exec(exec_fn f, const char *sql, const wk_value *params, size_t n) {
  actor_string_t s = str(sql);
  wasmkeep_actor_storage_list_value_t list = to_params(params, n);
  uint64_t changed = 0;
  wasmkeep_actor_storage_sql_error_t err;
  bool ok = f(&s, &list, &changed, &err);
  free(list.ptr); /* the values themselves are borrowed */
  if (!ok) {
    sql_failed(&err);
    return -1;
  }
  return (int64_t)changed;
}

int64_t wk_exec_v(const char *sql, const wk_value *params, size_t n) {
  return run_exec(wasmkeep_actor_storage_exec, sql, params, n);
}

int64_t wk_exec_durable_v(const char *sql, const wk_value *params, size_t n) {
  return run_exec(wasmkeep_actor_storage_exec_durable, sql, params, n);
}

bool wk_query_v(wk_rows *rows, const char *sql, const wk_value *params, size_t n) {
  memset(rows, 0, sizeof *rows);
  actor_string_t s = str(sql);
  wasmkeep_actor_storage_list_value_t list = to_params(params, n);
  wasmkeep_actor_storage_rows_t *raw = calloc(1, sizeof *raw);
  wasmkeep_actor_storage_sql_error_t err;
  bool ok = wasmkeep_actor_storage_query(&s, &list, raw, &err);
  free(list.ptr);
  if (!ok) {
    free(raw);
    sql_failed(&err);
    return false;
  }
  rows->count = raw->values.len;
  rows->columns = raw->columns.len;
  rows->raw_ = raw;
  return true;
}

void wk_rows_free(wk_rows *rows) {
  if (rows->raw_) {
    wasmkeep_actor_storage_rows_free(rows->raw_);
    free(rows->raw_);
  }
  memset(rows, 0, sizeof *rows);
}

wk_value wk_get(const wk_rows *rows, size_t row, size_t column) {
  const wasmkeep_actor_storage_rows_t *raw = rows->raw_;
  if (!raw || row >= raw->values.len || column >= raw->values.ptr[row].len) return wk_null();
  const wasmkeep_actor_types_value_t *v = &raw->values.ptr[row].ptr[column];
  switch (v->tag) {
    case WASMKEEP_ACTOR_TYPES_VALUE_INTEGER: return wk_int(v->val.integer);
    case WASMKEEP_ACTOR_TYPES_VALUE_REAL: return wk_real(v->val.real);
    case WASMKEEP_ACTOR_TYPES_VALUE_TEXT: return wk_text_n((const char *)v->val.text.ptr, v->val.text.len);
    case WASMKEEP_ACTOR_TYPES_VALUE_BLOB: return wk_blob(v->val.blob.ptr, v->val.blob.len);
    default: return wk_null();
  }
}

long wk_column(const wk_rows *rows, const char *name) {
  const wasmkeep_actor_storage_rows_t *raw = rows->raw_;
  size_t len = strlen(name);
  for (size_t i = 0; raw && i < raw->columns.len; i++) {
    if (raw->columns.ptr[i].len == len && memcmp(raw->columns.ptr[i].ptr, name, len) == 0) return (long)i;
  }
  return -1;
}

int64_t wk_query_int_v(const char *sql, const wk_value *params, size_t n) {
  wk_rows rows;
  if (!wk_query_v(&rows, sql, params, n)) return 0;
  int64_t value = wk_get_int(&rows, 0, 0);
  wk_rows_free(&rows);
  return value;
}

void wk_delete_self(void) { wasmkeep_actor_storage_delete_self(); }

static char *dup_str(const actor_string_t *s) {
  char *out = malloc(s->len + 1);
  if (!out) abort();
  memcpy(out, s->ptr, s->len);
  out[s->len] = 0;
  return out;
}

/* Values never change during an instance's life, so each name is fetched
 * once and kept (set or not) in a short list. */
typedef struct env_entry {
  struct env_entry *next;
  char *name;
  char *value; /* NULL: not set */
} env_entry;

const char *wk_env(const char *name) {
  static env_entry *cache;
  for (env_entry *e = cache; e; e = e->next)
    if (strcmp(e->name, name) == 0) return e->value;
  env_entry *e = malloc(sizeof *e);
  if (!e) abort();
  actor_string_t key, raw;
  actor_string_set(&key, name);
  e->name = dup_str(&key);
  e->value = wasmkeep_actor_env_get(&key, &raw) ? dup_str(&raw) : NULL;
  if (e->value) actor_string_free(&raw);
  e->next = cache;
  cache = e;
  return e->value;
}

wk_address wk_self_address(void) {
  /* An instance serves one actor, so the address never changes: fetch once. */
  static wk_address cached;
  if (!cached.tenant) {
    wasmkeep_actor_actors_actor_address_t raw;
    wasmkeep_actor_actors_self_address(&raw);
    cached.class_name = dup_str(&raw.class_);
    cached.name = dup_str(&raw.name);
    cached.tenant = dup_str(&raw.tenant);
    wasmkeep_actor_actors_actor_address_free(&raw);
  }
  return cached;
}

int64_t wk_now_ms(void) {
  struct timespec ts;
  clock_gettime(CLOCK_REALTIME, &ts);
  return (int64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

/* -------------------------------------------------------------- alarms */

void wk_alarm_set(int64_t at_ms) { wasmkeep_actor_alarms_set_alarm(at_ms < 0 ? 0 : (uint64_t)at_ms); }

bool wk_alarm_get(int64_t *at_ms) {
  uint64_t at;
  if (!wasmkeep_actor_alarms_get_alarm(&at)) return false;
  if (at_ms) *at_ms = (int64_t)at;
  return true;
}

void wk_alarm_clear(void) { wasmkeep_actor_alarms_clear_alarm(); }

/* ---------------------------------------------------------- WebSockets */

static bool send_message(uint64_t conn, wasmkeep_actor_websockets_message_t *msg) {
  wasmkeep_actor_websockets_send_error_t err;
  return wasmkeep_actor_websockets_send(conn, msg, &err);
}

bool wk_send_n(uint64_t conn, const char *text, size_t len) {
  wasmkeep_actor_websockets_message_t msg = {.tag = WASMKEEP_ACTOR_WEBSOCKETS_MESSAGE_TEXT};
  msg.val.text.ptr = (uint8_t *)text;
  msg.val.text.len = len;
  return send_message(conn, &msg);
}

bool wk_send(uint64_t conn, const char *text) { return wk_send_n(conn, text, strlen(text)); }

bool wk_send_binary(uint64_t conn, const void *data, size_t len) {
  wasmkeep_actor_websockets_message_t msg = {.tag = WASMKEEP_ACTOR_WEBSOCKETS_MESSAGE_BINARY};
  msg.val.binary.ptr = (uint8_t *)data;
  msg.val.binary.len = len;
  return send_message(conn, &msg);
}

void wk_broadcast_n(const char *text, size_t len) {
  actor_list_u64_t conns;
  wasmkeep_actor_websockets_connections(&conns);
  /* A slow reader drops this message; the others still get it. */
  for (size_t i = 0; i < conns.len; i++) wk_send_n(conns.ptr[i], text, len);
  actor_list_u64_free(&conns);
}

void wk_broadcast(const char *text) { wk_broadcast_n(text, strlen(text)); }

void wk_close(uint64_t conn, uint16_t code, const char *reason) {
  actor_string_t r = str(reason ? reason : "");
  wasmkeep_actor_websockets_close(conn, code, &r);
}

size_t wk_connections(uint64_t *out, size_t max) {
  actor_list_u64_t conns;
  wasmkeep_actor_websockets_connections(&conns);
  size_t n = conns.len;
  for (size_t i = 0; out && i < n && i < max; i++) out[i] = conns.ptr[i];
  actor_list_u64_free(&conns);
  return n;
}

/* --------------------------------------------------------- actor calls */

bool wk_call(const char *address, const char *method, const char *path,
             const void *body, size_t body_len, wk_call_response *out) {
  return wk_call_with_headers(address, method, path, NULL, 0, body, body_len, out);
}

bool wk_call_with_headers(const char *address, const char *method, const char *path,
                          const wk_header *headers, size_t count,
                          const void *body, size_t body_len, wk_call_response *out) {
  memset(out, 0, sizeof *out);
  actor_string_t a = str(address);
  wasmkeep_actor_actors_request_t req = {0};
  req.method = str(method ? method : "GET");
  req.path = str(path ? path : "/");
  req.body.ptr = (uint8_t *)body;
  req.body.len = body ? body_len : 0;
  /* The request is borrowed by the call: the strings stay the caller's. */
  actor_tuple2_string_string_t *pairs = NULL;
  if (headers && count) {
    pairs = calloc(count, sizeof *pairs);
    if (!pairs) abort();
    for (size_t i = 0; i < count; i++) {
      pairs[i].f0 = str(headers[i].name);
      pairs[i].f1 = str(headers[i].value ? headers[i].value : "");
    }
    req.headers.ptr = pairs;
    req.headers.len = count;
  }
  wasmkeep_actor_actors_response_t *raw = calloc(1, sizeof *raw);
  if (!raw) abort();
  wasmkeep_actor_actors_call_error_t err;
  bool ok = wasmkeep_actor_actors_call(&a, &req, raw, &err);
  free(pairs);
  if (!ok) {
    free(raw);
    static const char *const kinds[] = {"invalid-address", "reentrant", "too-large", "timeout", "failed"};
    const char *kind = err.tag < 5 ? kinds[err.tag] : "failed";
    if (err.tag == WASMKEEP_ACTOR_ACTORS_CALL_ERROR_INVALID_ADDRESS)
      set_error(kind, err.val.invalid_address.ptr, err.val.invalid_address.len);
    else if (err.tag == WASMKEEP_ACTOR_ACTORS_CALL_ERROR_FAILED)
      set_error(kind, err.val.failed.ptr, err.val.failed.len);
    else
      set_error(kind, (const uint8_t *)kind, strlen(kind));
    wasmkeep_actor_actors_call_error_free(&err);
    return false;
  }
  out->status = raw->status;
  out->body.ptr = (const char *)raw->body.ptr;
  out->body.len = raw->body.len;
  out->raw_ = raw;
  return true;
}

void wk_call_response_free(wk_call_response *response) {
  if (response->raw_) {
    wasmkeep_actor_actors_response_free(response->raw_);
    free(response->raw_);
  }
  memset(response, 0, sizeof *response);
}

/* ---------------------------------------------------------------- HTTP */

typedef struct request_state {
  wasi_http_types_own_incoming_request_t handle;
  bool body_read;
  char *body;
  size_t body_len;
  wasi_http_types_list_tuple2_field_name_field_value_t headers;
  char *header_value; /* the last value wk_request_header returned */
} request_state;

wk_bytes wk_request_body(wk_request *req) {
  request_state *st = req->raw_;
  if (!st->body_read) {
    st->body_read = true;
    wasi_http_types_own_incoming_body_t body;
    if (wasi_http_types_method_incoming_request_consume(wasi_http_types_borrow_incoming_request(st->handle), &body)) {
      wasi_http_types_own_input_stream_t stream;
      if (wasi_http_types_method_incoming_body_stream(wasi_http_types_borrow_incoming_body(body), &stream)) {
        for (;;) {
          actor_list_u8_t chunk;
          wasi_io_streams_stream_error_t err;
          if (!wasi_io_streams_method_input_stream_blocking_read(wasi_io_streams_borrow_input_stream(stream), 65536,
                                                                 &chunk, &err)) {
            wasi_io_streams_stream_error_free(&err);
            break; /* closed: the body has ended */
          }
          char *grown = realloc(st->body, st->body_len + chunk.len + 1);
          if (grown) {
            st->body = grown;
            memcpy(st->body + st->body_len, chunk.ptr, chunk.len);
            st->body_len += chunk.len;
          }
          actor_list_u8_free(&chunk);
        }
        wasi_io_streams_input_stream_drop_own(stream);
      }
      wasi_http_types_incoming_body_drop_own(body);
    }
    if (st->body) st->body[st->body_len] = 0;
  }
  wk_bytes out = {st->body ? st->body : "", st->body_len};
  return out;
}

const char *wk_request_header(wk_request *req, const char *name) {
  request_state *st = req->raw_;
  size_t len = strlen(name);
  for (size_t i = 0; i < st->headers.len; i++) {
    wasi_http_types_tuple2_field_name_field_value_t *h = &st->headers.ptr[i];
    if (h->f0.len == len && memcmp(h->f0.ptr, name, len) == 0) {
      free(st->header_value);
      st->header_value = malloc(h->f1.len + 1);
      if (!st->header_value) return NULL;
      memcpy(st->header_value, h->f1.ptr, h->f1.len);
      st->header_value[h->f1.len] = 0;
      return st->header_value;
    }
  }
  return NULL;
}

/* wk_response_header's list, owned by the response. */
typedef struct response_header {
  struct response_header *next;
  char *name;
  char *value;
  size_t value_len;
} response_header;

void wk_response_header_n(wk_response *r, const char *name, const char *value, size_t value_len) {
  response_header *h = malloc(sizeof *h);
  if (!h) abort();
  h->name = dup_str(&(actor_string_t){(uint8_t *)name, strlen(name)});
  h->value = dup_str(&(actor_string_t){(uint8_t *)value, value_len});
  h->value_len = value_len;
  h->next = NULL;
  /* Appended, so headers go out in the order they were added. */
  response_header **at = (response_header **)&r->headers_;
  while (*at) at = &(*at)->next;
  *at = h;
}

static void free_headers(wk_response *r) {
  for (response_header *h = r->headers_, *next; h; h = next) {
    next = h->next;
    free(h->name);
    free(h->value);
    free(h);
  }
  r->headers_ = NULL;
}

wk_response wk_vreply(uint16_t status, const char *fmt, va_list args) {
  wk_response r = {status, NULL, {"", 0}, false, NULL};
  va_list copy;
  va_copy(copy, args);
  int n = vsnprintf(NULL, 0, fmt, copy);
  va_end(copy);
  if (n < 0) return r;
  char *buf = malloc((size_t)n + 1);
  if (!buf) return r;
  vsnprintf(buf, (size_t)n + 1, fmt, args);
  r.body.ptr = buf;
  r.body.len = (size_t)n;
  r.owned_ = true;
  return r;
}

wk_response wk_reply(uint16_t status, const char *fmt, ...) {
  va_list args;
  va_start(args, fmt);
  wk_response r = wk_vreply(status, fmt, args);
  va_end(args);
  return r;
}

wk_response wk_reply_static(uint16_t status, const char *content_type, const void *body, size_t len) {
  wk_response r = {status, content_type, {(const char *)body, len}, false, NULL};
  return r;
}

wk_response wk_reply_copy(uint16_t status, const char *content_type, const void *body, size_t len) {
  wk_response r = {status, content_type, {"", 0}, false, NULL};
  char *buf = malloc(len ? len : 1);
  if (!buf) return r;
  memcpy(buf, body, len);
  r.body.ptr = buf;
  r.body.len = len;
  r.owned_ = true;
  return r;
}

static void send_response(wasi_http_types_own_response_outparam_t out, wk_response *r) {
  const char *type = r->content_type ? r->content_type : "text/plain; charset=utf-8";
  size_t count = 1;
  for (response_header *h = r->headers_; h; h = h->next) count++;
  wasi_http_types_tuple2_field_name_field_value_t *headers = calloc(count, sizeof *headers);
  if (!headers) abort();
  headers[0].f0.ptr = (uint8_t *)"content-type";
  headers[0].f0.len = strlen("content-type");
  headers[0].f1.ptr = (uint8_t *)type;
  headers[0].f1.len = strlen(type);
  size_t i = 1;
  for (response_header *h = r->headers_; h; h = h->next, i++) {
    headers[i].f0.ptr = (uint8_t *)h->name;
    headers[i].f0.len = strlen(h->name);
    headers[i].f1.ptr = (uint8_t *)h->value;
    headers[i].f1.len = h->value_len;
  }
  wasi_http_types_list_tuple2_field_name_field_value_t list = {headers, count};
  wasi_http_types_own_fields_t fields;
  wasi_http_types_header_error_t herr;
  if (!wasi_http_types_static_fields_from_list(&list, &fields, &herr)) fields = wasi_http_types_constructor_fields();
  free(headers);

  wasi_http_types_own_outgoing_response_t resp = wasi_http_types_constructor_outgoing_response(fields);
  wasi_http_types_method_outgoing_response_set_status_code(wasi_http_types_borrow_outgoing_response(resp), r->status);
  wasi_http_types_own_outgoing_body_t body;
  wasi_http_types_method_outgoing_response_body(wasi_http_types_borrow_outgoing_response(resp), &body);
  wasi_http_types_result_own_outgoing_response_error_code_t result = {.is_err = false, .val = {.ok = resp}};
  wasi_http_types_static_response_outparam_set(out, &result);

  wasi_http_types_own_output_stream_t stream;
  if (wasi_http_types_method_outgoing_body_write(wasi_http_types_borrow_outgoing_body(body), &stream)) {
    for (size_t at = 0; at < r->body.len; at += 4096) {
      size_t n = r->body.len - at < 4096 ? r->body.len - at : 4096;
      actor_list_u8_t bytes = {(uint8_t *)r->body.ptr + at, n};
      wasi_io_streams_stream_error_t serr;
      if (!wasi_io_streams_method_output_stream_blocking_write_and_flush(
              wasi_io_streams_borrow_output_stream(stream), &bytes, &serr)) {
        wasi_io_streams_stream_error_free(&serr);
        break;
      }
    }
    wasi_io_streams_output_stream_drop_own(stream);
  }
  wasi_http_types_error_code_t ferr;
  wasi_http_types_static_outgoing_body_finish(body, NULL, &ferr);
}

static const char *const method_names[] = {"GET", "HEAD", "POST", "PUT", "DELETE",
                                           "CONNECT", "OPTIONS", "TRACE", "PATCH"};

static char *dup_n(const uint8_t *p, size_t n) {
  char *s = malloc(n + 1);
  if (!s) return NULL;
  memcpy(s, p, n);
  s[n] = 0;
  return s;
}

void exports_wasi_http_incoming_handler_handle(exports_wasi_http_incoming_handler_own_incoming_request_t request,
                                               exports_wasi_http_incoming_handler_own_response_outparam_t out) {
  wasi_http_types_borrow_incoming_request_t borrowed = wasi_http_types_borrow_incoming_request(request);
  request_state st = {.handle = request};

  wasi_http_types_method_t method;
  wasi_http_types_method_incoming_request_method(borrowed, &method);
  char *method_name = method.tag == WASI_HTTP_TYPES_METHOD_OTHER
                          ? dup_n(method.val.other.ptr, method.val.other.len)
                          : dup_n((const uint8_t *)method_names[method.tag], strlen(method_names[method.tag]));
  wasi_http_types_method_free(&method);

  actor_string_t path_raw;
  char *path = wasi_http_types_method_incoming_request_path_with_query(borrowed, &path_raw)
                   ? dup_n(path_raw.ptr, path_raw.len)
                   : dup_n((const uint8_t *)"/", 1);
  if (path_raw.ptr && path) actor_string_free(&path_raw);

  wasi_http_types_own_headers_t headers = wasi_http_types_method_incoming_request_headers(borrowed);
  wasi_http_types_method_fields_entries(wasi_http_types_borrow_fields(headers), &st.headers);
  wasi_http_types_fields_drop_own(headers);

  wk_request req = {method_name ? method_name : "GET", path ? path : "/", &st};
  wk_response r = wk_fetch(&req);
  send_response(out, &r);

  if (r.owned_) free((void *)r.body.ptr);
  free_headers(&r);
  wasi_http_types_list_tuple2_field_name_field_value_free(&st.headers);
  free(st.header_value);
  free(st.body);
  free(method_name);
  free(path);
  wasi_http_types_incoming_request_drop_own(request);
}

/* --------------------------------------------------------------- hooks */

static const char *failure;

void wk_fail(const char *message) { failure = message ? message : "failed"; }

static bool finish(actor_string_t *err) {
  if (!failure) return true;
  actor_string_dup(err, failure);
  failure = NULL;
  return false;
}

WK_WEAK void wk_init(void) {}
WK_WEAK void wk_alarm(void) {}
WK_WEAK wk_response wk_fetch(wk_request *req) {
  (void)req;
  return wk_reply_static(404, NULL, "no fetch handler\n", strlen("no fetch handler\n"));
}
WK_WEAK bool wk_ws_open(uint64_t conn, const char *path) {
  (void)conn;
  (void)path;
  return true;
}
WK_WEAK void wk_ws_message(uint64_t conn, wk_bytes data, bool binary) {
  (void)conn;
  (void)data;
  (void)binary;
}
WK_WEAK void wk_ws_close(uint64_t conn, uint16_t code, const char *reason) {
  (void)conn;
  (void)code;
  (void)reason;
}

bool exports_actor_on_init(actor_string_t *err) {
  failure = NULL;
  wk_init();
  return finish(err);
}

bool exports_actor_on_alarm(actor_string_t *err) {
  failure = NULL;
  wk_alarm();
  return finish(err);
}

bool exports_actor_on_websocket_open(uint64_t conn, actor_string_t *path, actor_string_t *err) {
  char *p = dup_n(path->ptr, path->len);
  bool accepted = wk_ws_open(conn, p ? p : "");
  free(p);
  actor_string_free(path);
  if (!accepted) actor_string_dup(err, "refused");
  return accepted;
}

void exports_actor_on_websocket_message(uint64_t conn, actor_message_t *msg) {
  bool binary = msg->tag == WASMKEEP_ACTOR_WEBSOCKETS_MESSAGE_BINARY;
  wk_bytes data = binary ? (wk_bytes){(const char *)msg->val.binary.ptr, msg->val.binary.len}
                         : (wk_bytes){(const char *)msg->val.text.ptr, msg->val.text.len};
  wk_ws_message(conn, data, binary);
  actor_message_free(msg);
}

void exports_actor_on_websocket_close(uint64_t conn, uint16_t code, actor_string_t *reason) {
  char *r = dup_n(reason->ptr, reason->len);
  wk_ws_close(conn, code, r ? r : "");
  free(r);
  actor_string_free(reason);
}
