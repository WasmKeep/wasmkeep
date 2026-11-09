/* Durable workflows built from actors (ADR 0019): the client side, for C.
 * wasmkeep.h includes this header.
 *
 *   static const wk_workflow orders = {.class_name = "orders"};  // or wk_workflow_client("orders")
 *
 *   wk_workflow_status s;
 *   wk_workflow_error err;
 *   if (!wk_workflow_start(&orders, wk_str("order-42"), wk_str("item=book"), &s, &err))
 *     return wk_reply(502, "%s\n", err.message);
 *   ... s.state, s.waiting_for, s.output ...
 *   wk_workflow_status_free(&s);
 *   wk_workflow_send_event(&orders, wk_str("order-42"), wk_str("approved"), wk_str(""), &err);
 *
 * The workflow class itself runs on the Rust (or TypeScript) SDK's runner
 * (see examples/workflows: `orders`); this client speaks its protocol
 * (`/wasmkeep-workflow/v1`) over actors.call, byte for byte as the Rust
 * `wasmkeep_sdk::workflow::Client`. Every operation is one actors.call to
 * `<class>/<id>`, which holds this actor's event while it runs; the
 * instance itself runs later, in its own event.
 *
 * Conventions are the kv client's (wasmkeep_kv.h): ids, event names and
 * payloads are wk_bytes (wk_str for a C string), every call takes an
 * optional error (NULL to ignore it; its message is worded as Rust's
 * workflow::Error and keeps at most 400 bytes of a refusal's answer), and
 * a returned status is the caller's: free it with
 * wk_workflow_status_free. */
#ifndef WASMKEEP_WORKFLOW_H
#define WASMKEEP_WORKFLOW_H

#include "wasmkeep.h"

#ifdef __cplusplus
extern "C" {
#endif

/* The path prefix of the protocol. */
#define WK_WORKFLOW_PREFIX "/wasmkeep-workflow/v1"

typedef enum wk_workflow_error_kind {
  WK_WORKFLOW_OK = 0,
  /* No instance with that id (never started, or deleted after its
   * retention). */
  WK_WORKFLOW_UNKNOWN,
  /* The instance has finished, so it takes no more events. */
  WK_WORKFLOW_FINISHED,
  /* The call failed: the instance may or may not have recorded it. */
  WK_WORKFLOW_CALL,
  /* The instance answered with an unexpected status (`status`); a
   * malformed status counts as 200. */
  WK_WORKFLOW_REFUSED,
} wk_workflow_error_kind;

typedef struct wk_workflow_error {
  wk_workflow_error_kind kind;
  uint16_t status;   /* for WK_WORKFLOW_REFUSED */
  char message[512]; /* NUL-terminated, as Rust's Display */
} wk_workflow_error;

/* Where an instance stands. */
typedef enum wk_workflow_state {
  WK_WORKFLOW_RUNNING = 0, /* started, or continuing: its alarm runs it next */
  WK_WORKFLOW_WAITING,     /* waiting for a time or an event */
  WK_WORKFLOW_COMPLETED,   /* returned `output` */
  WK_WORKFLOW_FAILED,      /* failed with `error` */
  WK_WORKFLOW_CANCELLED,   /* cancelled with wk_workflow_cancel */
} wk_workflow_state;

/* "running", "waiting", "completed", "failed" or "cancelled". */
const char *wk_workflow_state_name(wk_workflow_state state);
/* Whether the instance has finished (completed, failed or cancelled). */
bool wk_workflow_state_is_finished(wk_workflow_state state);

/* An instance's status. Free with wk_workflow_status_free. Every wk_bytes
 * is a NUL-terminated copy (the NUL is not counted in len). */
typedef struct wk_workflow_status {
  wk_workflow_state state;
  /* When waiting: until when (ms since the Unix epoch), at the latest; 0
   * if not waiting for a time. */
  uint64_t wake_at;
  /* When waiting for events: their names (UTF-8). */
  wk_bytes *waiting_for;
  size_t waiting_count;
  /* When completed: the workflow's output. */
  bool has_output;
  wk_bytes output;
  /* When failed: why (UTF-8); empty (len 0) if there is no error. */
  wk_bytes error;
  /* Positions recorded so far (steps, sleeps and waits). */
  uint64_t positions;
} wk_workflow_status;
void wk_workflow_status_free(wk_workflow_status *status);

/* Starts and queries instances of one workflow class. A plain value: copy
 * it freely. */
typedef struct wk_workflow {
  const char *class_name; /* borrowed */
} wk_workflow;

/* Instances of `class_name` (the workflow class's name). */
wk_workflow wk_workflow_client(const char *class_name);

/* Starts instance `id` with `input`, or reports its status if it exists (a
 * start is idempotent while the instance is kept). It runs in its own
 * event, soon after: this call only records it. *out (may be NULL) gets
 * its status. */
bool wk_workflow_start(const wk_workflow *wf, wk_bytes id, wk_bytes input, wk_workflow_status *out,
                       wk_workflow_error *err);

/* The instance's status, into *out; WK_WORKFLOW_UNKNOWN if there is none.
 * (Rust's `status`: wk_workflow_status is the type's name.) */
bool wk_workflow_get_status(const wk_workflow *wf, wk_bytes id, wk_workflow_status *out, wk_workflow_error *err);

/* Delivers event `name` (UTF-8) with `payload`: recorded durably, then the
 * instance runs. WK_WORKFLOW_UNKNOWN if it was never started,
 * WK_WORKFLOW_FINISHED if it has finished. */
bool wk_workflow_send_event(const wk_workflow *wf, wk_bytes id, wk_bytes name, wk_bytes payload,
                            wk_workflow_error *err);

/* Cancels the instance: it runs no further step (one running now finishes
 * first). *out (may be NULL) gets its status; a finished one is
 * unchanged. */
bool wk_workflow_cancel(const wk_workflow *wf, wk_bytes id, wk_workflow_status *out, wk_workflow_error *err);

#ifdef __cplusplus
}
#endif

#endif
