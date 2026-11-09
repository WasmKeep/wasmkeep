/* A chat room in C, with the WasmKeep C SDK (sdks/c). Each actor name is one
 * room: the Rust examples/chat-room, line for line in behaviour.
 *
 * - Connect a WebSocket to /actor/<class>/<room>: you receive the last 20
 *   messages, then everything said in the room. Text you send is broadcast to
 *   everyone in the room, you included. If the operator set MOTD (wasmkeep
 *   serve --env chat=FILE), it comes first, as "* <MOTD>".
 * - POST / with a text body says something over plain HTTP.
 * - GET / lists the recent messages.
 *
 * Messages are kept in the room's SQLite database (the last 100). The host
 * owns the sockets, so the room can hibernate while people stay connected:
 * the next message wakes it. */
#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "wasmkeep.h"

#define HISTORY 20
#define KEEP 100

static const char RECENT[] =
    "SELECT text FROM (SELECT id, text FROM messages ORDER BY id DESC LIMIT ?) ORDER BY id";

static void say(const char *text, size_t len) {
  wk_exec("INSERT INTO messages (at, text) VALUES (?, ?)", wk_int(wk_now_ms()), wk_text_n(text, len));
  wk_exec("DELETE FROM messages WHERE id <= (SELECT max(id) FROM messages) - ?", wk_int(KEEP));
  wk_broadcast_n(text, len);
}

/* "* someone joined #lobby (2 online)": the room is this actor's name. */
static void announce(const char *what) {
  char line[256]; /* a long room name is cut short */
  snprintf(line, sizeof line, "* someone %s #%s (%zu online)", what, wk_self_address().name,
           wk_connections(NULL, 0));
  wk_broadcast(line);
}

/* Whether `b` is valid UTF-8 (no overlong forms, surrogates or code points
 * past U+10FFFF). */
static bool utf8(wk_bytes b) {
  const unsigned char *s = (const unsigned char *)b.ptr;
  for (size_t i = 0; i < b.len;) {
    unsigned char c = s[i];
    size_t n;
    unsigned char lo = 0x80, hi = 0xBF; /* bounds of the second byte */
    if (c < 0x80) n = 0;
    else if (c >= 0xC2 && c <= 0xDF) n = 1;
    else if (c >= 0xE0 && c <= 0xEF) n = 2, lo = c == 0xE0 ? 0xA0 : 0x80, hi = c == 0xED ? 0x9F : 0xBF;
    else if (c >= 0xF0 && c <= 0xF4) n = 3, lo = c == 0xF0 ? 0x90 : 0x80, hi = c == 0xF4 ? 0x8F : 0xBF;
    else return false;
    if (n > b.len - i - 1) return false;
    if (n && (s[i + 1] < lo || s[i + 1] > hi)) return false;
    for (size_t k = 2; k <= n; k++)
      if ((s[i + k] & 0xC0) != 0x80) return false;
    i += n + 1;
  }
  return true;
}

void wk_init(void) {
  wk_exec("CREATE TABLE IF NOT EXISTS messages"
          " (id INTEGER PRIMARY KEY, at INTEGER NOT NULL, text TEXT NOT NULL)");
}

bool wk_ws_open(uint64_t conn, const char *path) {
  (void)path;
  const char *motd = wk_env("MOTD");
  if (motd) {
    char line[512]; /* a long MOTD is cut short */
    snprintf(line, sizeof line, "* %s", motd);
    wk_send(conn, line);
  }
  wk_rows rows;
  if (wk_query(&rows, RECENT, wk_int(HISTORY))) {
    for (size_t i = 0; i < rows.count; i++) {
      wk_bytes t = wk_get_text(&rows, i, 0);
      wk_send_n(conn, t.ptr, t.len);
    }
    wk_rows_free(&rows);
  }
  announce("joined");
  return true;
}

void wk_ws_message(uint64_t conn, wk_bytes data, bool binary) {
  (void)conn;
  if (!binary) say(data.ptr, data.len);
}

void wk_ws_close(uint64_t conn, uint16_t code, const char *reason) {
  (void)conn;
  (void)code;
  (void)reason;
  announce("left");
}

wk_response wk_fetch(wk_request *req) {
  if (strcmp(req->method, "POST") == 0) {
    wk_bytes body = wk_request_body(req);
    if (body.len > 4096 || !utf8(body)) body.len = 0;
    /* Trim whitespace at both ends. */
    while (body.len && isspace((unsigned char)body.ptr[0])) body.ptr++, body.len--;
    while (body.len && isspace((unsigned char)body.ptr[body.len - 1])) body.len--;
    if (body.len == 0) return wk_reply(400, "send a UTF-8 text body\n");
    say(body.ptr, body.len);
    return wk_reply(200, "sent\n");
  }
  if (strcmp(req->method, "GET") == 0) {
    wk_rows rows;
    if (!wk_query(&rows, RECENT, wk_int(HISTORY))) return wk_reply(500, "%s\n", wk_error());
    size_t total = 1;
    for (size_t i = 0; i < rows.count; i++) total += wk_get_text(&rows, i, 0).len + 1;
    char *out = malloc(total);
    size_t at = 0;
    for (size_t i = 0; out && i < rows.count; i++) {
      wk_bytes t = wk_get_text(&rows, i, 0);
      memcpy(out + at, t.ptr, t.len);
      at += t.len;
      if (i + 1 < rows.count) out[at++] = '\n';
    }
    if (out) out[at++] = '\n';
    wk_response r = wk_reply_copy(200, NULL, out ? out : "\n", out ? at : 1);
    free(out);
    wk_rows_free(&rows);
    return r;
  }
  return wk_reply(405, "use GET or POST\n");
}
