/* A counter actor in C, with the WasmKeep C SDK (sdks/c). GET returns the
 * value, POST increments it; each actor name is its own counter, kept in its
 * SQLite database. */
#include <string.h>

#include "wasmkeep.h"

void wk_init(void) {
  wk_exec("CREATE TABLE IF NOT EXISTS counter"
          " (id INTEGER PRIMARY KEY CHECK (id = 1), value INTEGER NOT NULL)");
  wk_exec("INSERT OR IGNORE INTO counter (id, value) VALUES (1, 0)");
}

wk_response wk_fetch(wk_request *req) {
  int64_t value;
  if (strcmp(req->method, "GET") == 0)
    value = wk_query_int("SELECT value FROM counter WHERE id = 1");
  else if (strcmp(req->method, "POST") == 0)
    value = wk_query_int("UPDATE counter SET value = value + 1 WHERE id = 1 RETURNING value");
  else
    return wk_reply(405, "use GET or POST\n");
  return wk_reply(200, "%lld\n", (long long)value);
}
