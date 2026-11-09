/* The workflow client (ADR 0019): the Rust SDK's workflow::Client, over
 * wk_call_with_headers. See wasmkeep_workflow.h; the status codec is in
 * wasmkeep_wire.c. */
#include "wasmkeep_wire.h"

#include <stdio.h>
#include <stdlib.h>

static void fail(wk_workflow_error *err, wk_workflow_error_kind kind, uint16_t status, const char *message) {
  if (!err) return;
  err->kind = kind;
  err->status = status;
  snprintf(err->message, sizeof err->message, "%s", message);
}

/* Rust's Error::Refused Display: `refused (<status>): <body, trimmed>`. */
static void refused(wk_workflow_error *err, uint16_t status, wk_bytes body) {
  if (!err) return;
  const char *p = body.ptr;
  size_t n = body.len;
  while (n && (*p == ' ' || *p == '\t' || *p == '\n' || *p == '\r' || *p == '\f' || *p == '\v')) p++, n--;
  while (n && (p[n - 1] == ' ' || p[n - 1] == '\t' || p[n - 1] == '\n' || p[n - 1] == '\r' || p[n - 1] == '\f' ||
               p[n - 1] == '\v'))
    n--;
  err->kind = WK_WORKFLOW_REFUSED;
  err->status = status;
  snprintf(err->message, sizeof err->message, "refused (%u): %.*s", (unsigned)status, (int)(n > 400 ? 400 : n), p);
}

wk_workflow wk_workflow_client(const char *class_name) {
  wk_workflow wf = {class_name};
  return wf;
}

/* One call to `<class>/<id>`: true with a 200 answer in *r (free it). */
static bool call(const wk_workflow *wf, wk_bytes id, const char *method, const char *path, wk_bytes body,
                 wk_call_response *r, wk_workflow_error *err) {
  size_t class_len = strlen(wf->class_name);
  char *address = malloc(class_len + 1 + id.len + 1);
  if (!address) abort();
  memcpy(address, wf->class_name, class_len);
  address[class_len] = '/';
  if (id.len) memcpy(address + class_len + 1, id.ptr, id.len);
  address[class_len + 1 + id.len] = 0;
  bool ok = false;
  if (!wk_call_with_headers(address, method, path, NULL, 0, body.ptr, body.len, r)) {
    if (err) {
      fail(err, WK_WORKFLOW_CALL, 0, "");
      wk_call_failure(err->message, sizeof err->message, address);
    }
  } else if (r->status == 200) {
    ok = true;
  } else {
    if (r->status == 404) fail(err, WK_WORKFLOW_UNKNOWN, 0, "no such workflow instance");
    else if (r->status == 409) fail(err, WK_WORKFLOW_FINISHED, 0, "the workflow instance has finished");
    else refused(err, r->status, r->body);
    wk_call_response_free(r);
  }
  free(address);
  return ok;
}

/* A call answering a status, decoded into *out (may be NULL). */
static bool status_call(const wk_workflow *wf, wk_bytes id, const char *method, const char *path, wk_bytes body,
                        wk_workflow_status *out, wk_workflow_error *err) {
  wk_call_response r;
  if (!call(wf, id, method, path, body, &r, err)) return false;
  wk_workflow_status s;
  bool ok = wk_workflow_decode_status(r.body.ptr, r.body.len, &s);
  wk_call_response_free(&r);
  if (!ok) {
    fail(err, WK_WORKFLOW_REFUSED, 200, "refused (200): malformed status");
    return false;
  }
  if (out) *out = s;
  else wk_workflow_status_free(&s);
  return true;
}

bool wk_workflow_start(const wk_workflow *wf, wk_bytes id, wk_bytes input, wk_workflow_status *out,
                       wk_workflow_error *err) {
  return status_call(wf, id, "POST", WK_WORKFLOW_PREFIX "/start", input, out, err);
}

bool wk_workflow_get_status(const wk_workflow *wf, wk_bytes id, wk_workflow_status *out, wk_workflow_error *err) {
  return status_call(wf, id, "GET", WK_WORKFLOW_PREFIX "/status", wk_bytes_of(NULL, 0), out, err);
}

bool wk_workflow_send_event(const wk_workflow *wf, wk_bytes id, wk_bytes name, wk_bytes payload,
                            wk_workflow_error *err) {
  static const char prefix[] = WK_WORKFLOW_PREFIX "/event?name=";
  char *path = malloc(sizeof prefix + 2 * name.len);
  if (!path) abort();
  memcpy(path, prefix, sizeof prefix - 1);
  wk_hex(path + sizeof prefix - 1, name.ptr, name.len);
  wk_call_response r;
  bool ok = call(wf, id, "POST", path, payload, &r, err);
  free(path);
  /* The body of a 200 is ignored. */
  if (ok) wk_call_response_free(&r);
  return ok;
}

bool wk_workflow_cancel(const wk_workflow *wf, wk_bytes id, wk_workflow_status *out, wk_workflow_error *err) {
  return status_call(wf, id, "POST", WK_WORKFLOW_PREFIX "/cancel", wk_bytes_of(NULL, 0), out, err);
}
