// The order workflow of examples/workflows, in TypeScript: a class
// (deployed as `orders`) whose actors are orders, run by the SDK's workflow
// runner (ADR 0019). Every side effect is an HTTP request, with
// `http.fetch`, to the ledger service at LEDGER_URL (its configuration,
// `--env orders=FILE`), carrying the step's idempotency key:
//
//   POST <LEDGER_URL>/record?what=<what>&key=<key>   -> 200 "<what>#<n>"
//
// The reservation also sends the order's input as the request's body.
//
// The input is `key=value` pairs separated by `;`: `item`, `cooloff_ms`
// (default 1000) and `approve_within_ms` (default one day). Timings come
// from the configuration too: RETENTION_MS, BACKOFF_MS, ATTEMPTS,
// RUN_BUDGET_MS. Any workflow client starts and follows it, the Rust
// `app` class of examples/workflows included.

import { env, http, workflow } from "@wasmkeep/sdk";

function number(name: string, fallback: number): number {
  const v = Number(env.get(name));
  return Number.isFinite(v) && env.get(name) !== undefined ? v : fallback;
}

function field(input: string, name: string): string | undefined {
  for (const pair of input.split(";")) {
    const [k, v] = pair.trim().split("=");
    if (k === name) return v;
  }
  return undefined;
}

/** Records `what` at the ledger; a non-200 answer throws (and is retried). */
function ledger(what: string, key: string, body?: string): string {
  const base = env.get("LEDGER_URL");
  if (!base) throw new Error("LEDGER_URL is not configured");
  const r = http.fetch(`${base}/record?what=${encodeURIComponent(what)}&key=${encodeURIComponent(key)}`, {
    method: "POST",
    body,
    timeoutMs: 5_000,
  });
  if (r.status !== 200) throw new Error(`ledger answered ${r.status}: ${r.text().trim()}`);
  return r.text().trim();
}

export default workflow.define(
  (wf, bytes) => {
    const input = new TextDecoder().decode(bytes);
    const item = field(input, "item") ?? "something";
    const cooloff = Number(field(input, "cooloff_ms") ?? 1_000);
    const approveWithin = Number(field(input, "approve_within_ms") ?? 86_400_000);

    const reservation = wf.step("reserve", (key) => ledger(`reserve:${item}`, key, input));
    wf.sleep("cool-off", cooloff);
    if (wf.waitForEvent("approved", approveWithin) === undefined) {
      wf.step("release", (key) => ledger(`release:${item}`, key));
      throw workflow.fail(`not approved in time: ${reservation} was released`);
    }
    wf.step("charge", (key) => ledger(`charge:${item}`, key));
    // The same step name in a loop: each call is its own position.
    for (const who of ["buyer", "warehouse"]) {
      wf.step("notify", (key) => ledger(`notify:${who}`, key));
    }
    return `shipped ${item}`;
  },
  // Read on each event: at build time the configuration is not there yet.
  () => ({
    retentionMs: number("RETENTION_MS", 7 * 86_400_000),
    backoffBaseMs: number("BACKOFF_MS", 1_000),
    backoffMaxMs: number("BACKOFF_MS", 1_000) * 30,
    maxAttempts: number("ATTEMPTS", 5),
    runBudgetMs: number("RUN_BUDGET_MS", 20_000),
  }),
);
