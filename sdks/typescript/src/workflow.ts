/**
 * Durable workflows built from one actor per instance (ADR 0019): the
 * TypeScript runner and client, the same tables and protocol as the Rust
 * SDK's `workflow` module, so a client in either language follows a
 * workflow in the other.
 *
 * A workflow is a function the runner calls from the top every time the
 * instance moves forward. `wf.step` runs a piece of work once and records
 * its result (as JSON); `wf.sleep` waits; `wf.waitForEvent` waits for
 * `Client.sendEvent`. A recorded step returns its result without running
 * again, so after a sleep, an event or a failover the function replays to
 * where it stopped and goes on. A run that must wait stops by throwing a
 * `Stop`: let it through (rethrow it if you catch everything).
 *
 * ```ts
 * import { http, workflow } from "@wasmkeep/sdk";
 *
 * export default workflow.define((wf, input) => {
 *   const order = JSON.parse(new TextDecoder().decode(input));
 *   const reservation = wf.step("reserve", (key) =>
 *     http.fetch("https://stock.example/reserve", { method: "POST", headers: { "idempotency-key": key } }).text());
 *   wf.sleep("cool-off", 60_000);
 *   if (wf.waitForEvent("approved", 86_400_000) === undefined) throw workflow.fail("not approved in time");
 *   return `shipped ${order.item}`;
 * });
 * ```
 *
 * Two rules: between steps the function must make the same decisions on
 * every run (put time, randomness, reads and HTTP inside steps), and a
 * step may run more than once (use the key it is given as an idempotency
 * key).
 *
 * @module
 */

import { getRandomBytes } from "wasi:random/random@0.2.12";
import { call } from "./call";
import { type ActorDefinition, type ActorRequest, type ActorResponse, alarms, actors, storage } from "./index";
import { hex, header, toBytes } from "./wire";
import {
  type Config,
  type Settings,
  type State,
  type Status,
  decodeStatus,
  delay,
  encodeStatus,
  isFinished,
  settings,
} from "./workflow-wire";

export type { Config, State, Status } from "./workflow-wire";

const PREFIX = "/wasmkeep-workflow/v1";
const encoder = new TextEncoder();
const decoder = new TextDecoder();

/** Why a run stops before the workflow returns: thrown by the runner's
 * calls. `kind` is `wait` (a sleep, an event, a retry, or continuing in a
 * new event), `failed` (the workflow fails with `message`) or `retry`
 * (the runner's own database refused; internal). */
export class Stop extends Error {
  constructor(
    readonly kind: "wait" | "failed" | "retry",
    message: string,
  ) {
    super(message);
    this.name = "Stop";
  }
}

/** Fails the workflow with `reason`: `throw workflow.fail("...")`. */
export function fail(reason: string): Stop {
  return new Stop("failed", reason);
}

/** Whether `e` is the runner's `Stop` (rethrow it from a catch-all). */
export function isStop(e: unknown): e is Stop {
  return e instanceof Stop;
}

interface Recorded {
  name: string;
  kind: string;
  state: string;
  result: Uint8Array | null;
  error: string | null;
  attempts: number;
  nextAttemptAt: number | null;
  wakeAt: number | null;
}

function now(): number {
  return Date.now();
}

/** Runs `f`, turning a SQL error into the runner's own retry. */
function db<T>(f: () => T): T {
  try {
    return f();
  } catch (e) {
    if (e instanceof Stop) throw e;
    throw new Stop("retry", `the workflow's own database: ${e instanceof Error ? e.message : String(e)}`);
  }
}

function asNumber(v: unknown): number | null {
  return typeof v === "number" ? v : typeof v === "bigint" ? Number(v) : null;
}

/** A blob read back from SQL as bytes. On StarlingMonkey it is a
 * Uint8Array of another realm, not `instanceof Uint8Array` here, so any
 * typed-array view (or array of numbers) is taken. */
function asBytes(v: unknown): Uint8Array | null {
  if (v instanceof Uint8Array) return v;
  if (ArrayBuffer.isView(v)) return new Uint8Array(v.buffer, v.byteOffset, v.byteLength);
  if (v instanceof ArrayBuffer) return new Uint8Array(v);
  if (Array.isArray(v)) return Uint8Array.from(v as number[]);
  if (typeof v === "string") return encoder.encode(v);
  return null;
}

/** One run of an instance: what the workflow function uses. */
export class Run {
  private pos = 0;
  private stopped = false;
  /** The first `Stop` this run threw: what the run ended with, even if
   * the workflow caught it and returned. */
  firstStop: Stop | undefined;
  private ranStep = false;
  /** Earliest time the runner must look again, when it stops waiting. */
  wake: number | undefined;

  constructor(
    /** The instance's id (the actor's name). */
    readonly id: string,
    private readonly runId: string,
    private readonly config: Settings,
    private readonly startedAt: number,
    private readonly recorded: Map<number, Recorded>,
  ) {}

  /** A key unique to this run and to the next position: an idempotency
   * key for the service the next step calls. `step` passes it to its
   * function too. */
  stepKey(): string {
    return `${this.runId}-${this.pos}`;
  }

  /** Runs `f` once and records its result (JSON); returns the recorded
   * result on every later run. A throw is retried with backoff; when the
   * attempts run out the workflow fails (see `tryStep`). */
  step<T>(name: string, f: (key: string) => T): T {
    const r = this.tryStep(name, f);
    if ("error" in r) {
      this.stopped = true;
      const failed = fail(`step ${JSON.stringify(name)} failed after ${r.attempts} attempts: ${r.error}`);
      this.firstStop ??= failed;
      throw failed;
    }
    return r.ok;
  }

  /** Like `step`, but when the attempts run out it returns the last error
   * (every later run returns the same). */
  tryStep<T>(name: string, f: (key: string) => T): { ok: T } | { error: string; attempts: number } {
    return this.halting(() => this.attempt(name, f));
  }

  /** Waits `ms` from the first time this position is reached. */
  sleep(name: string, ms: number): void {
    this.halting(() => this.sleepAt(name, (t) => t + ms));
  }

  /** Waits until `at` (ms since the Unix epoch, or a Date). */
  sleepUntil(name: string, at: number | Date): void {
    const when = typeof at === "number" ? at : at.getTime();
    this.halting(() => this.sleepAt(name, () => when));
  }

  /** Waits for an event named `name` (`Client.sendEvent`) and returns its
   * payload, or `undefined` once `timeoutMs` has passed since this
   * position was first reached. Each wait takes the oldest event of that
   * name no earlier wait took. */
  waitForEvent(name: string, timeoutMs: number): Uint8Array | undefined {
    return this.halting(() => this.waitInner(name, timeoutMs));
  }

  private halting<T>(f: () => T): T {
    try {
      return f();
    } catch (e) {
      if (e instanceof Stop) {
        this.stopped = true;
        this.firstStop ??= e;
      }
      throw e;
    }
  }

  private waitUntil(at: number): Stop {
    this.wake = this.wake === undefined ? at : Math.min(this.wake, at);
    this.stopped = true;
    const stop = new Stop("wait", "waiting");
    this.firstStop ??= stop;
    return stop;
  }

  private next(name: string, kind: string): number {
    if (this.stopped) throw new Stop("wait", "waiting");
    const pos = this.pos++;
    const r = this.recorded.get(pos);
    if (r && (r.name !== name || r.kind !== kind)) {
      throw fail(
        `the code changed the order of its steps: position ${pos} was ${r.kind} ${JSON.stringify(r.name)}, now ${kind} ${JSON.stringify(name)}`,
      );
    }
    return pos;
  }

  private attempt<T>(name: string, f: (key: string) => T): { ok: T } | { error: string; attempts: number } {
    const key = this.stepKey();
    const pos = this.next(name, "step");
    const t = now();
    let attempts = 0;
    const r = this.recorded.get(pos);
    if (r) {
      if (r.state === "done") {
        const text = r.result ? decoder.decode(r.result) : "";
        try {
          return { ok: (text === "" ? undefined : JSON.parse(text)) as T };
        } catch {
          throw fail(`step ${JSON.stringify(name)}: its recorded result is not JSON (did a deploy change it?)`);
        }
      }
      if (r.state === "failed") return { error: r.error ?? "", attempts: r.attempts };
      if (r.nextAttemptAt !== null && r.nextAttemptAt > t) throw this.waitUntil(r.nextAttemptAt);
      attempts = r.attempts;
    }
    if (attempts >= this.config.maxAttempts) {
      const error = "the last attempt did not finish (trapped or cut)";
      db(() => storage.execDurable("UPDATE wasmkeep_wf_step SET state = 'failed', error = ? WHERE pos = ?", error, pos));
      return { error, attempts };
    }
    if (this.ranStep && t - this.startedAt >= this.config.runBudgetMs) throw this.waitUntil(t);
    this.ranStep = true;
    const attempt = attempts + 1;
    // Counted before it runs (a trap inside `f` counts too), with the
    // earliest retry should it never say how it ended.
    const retryAt = t + delay(this.config, attempt);
    db(() =>
      storage.exec(
        "INSERT INTO wasmkeep_wf_step (pos, name, kind, state, attempts, error, next_attempt_at) " +
          "VALUES (?, ?, 'step', 'pending', ?, 'did not finish (trapped or cut)', ?) " +
          "ON CONFLICT (pos) DO UPDATE SET attempts = excluded.attempts, error = excluded.error, " +
          "next_attempt_at = excluded.next_attempt_at",
        pos,
        name,
        attempt,
        retryAt,
      ),
    );
    // The watchdog: if this event ends without a word, the next attempt
    // still comes.
    alarms.set(retryAt);
    let encoded: Uint8Array;
    try {
      const value = f(key);
      // What replay will return: the same JSON round trip now, so the first
      // run and later ones see the same value (and a value JSON cannot hold
      // is the step's error, before anything is recorded).
      encoded = value === undefined ? new Uint8Array() : encoder.encode(JSON.stringify(value));
    } catch (e) {
      if (e instanceof Stop) throw e;
      const error = e instanceof Error ? e.message : String(e);
      if (attempt >= this.config.maxAttempts) {
        db(() => storage.execDurable("UPDATE wasmkeep_wf_step SET state = 'failed', error = ? WHERE pos = ?", error, pos));
        return { error, attempts: attempt };
      }
      const at = now() + delay(this.config, attempt);
      db(() => storage.exec("UPDATE wasmkeep_wf_step SET error = ?, next_attempt_at = ? WHERE pos = ?", error, at, pos));
      throw this.waitUntil(at);
    }
    db(() =>
      storage.execDurable("UPDATE wasmkeep_wf_step SET state = 'done', result = ?, error = NULL WHERE pos = ?", encoded, pos),
    );
    const text = decoder.decode(encoded);
    return { ok: (text === "" ? undefined : JSON.parse(text)) as T };
  }

  private sleepAt(name: string, at: (t: number) => number): void {
    const pos = this.next(name, "sleep");
    const t = now();
    const r = this.recorded.get(pos);
    let wake: number;
    if (r) {
      wake = r.wakeAt ?? 0;
    } else {
      wake = at(t);
      db(() =>
        storage.execDurable(
          "INSERT INTO wasmkeep_wf_step (pos, name, kind, state, wake_at) VALUES (?, ?, 'sleep', 'pending', ?)",
          pos,
          name,
          wake,
        ),
      );
    }
    if (t < wake) throw this.waitUntil(wake);
  }

  private waitInner(name: string, timeoutMs: number): Uint8Array | undefined {
    const pos = this.next(name, "event");
    const t = now();
    const r = this.recorded.get(pos);
    let deadline: number;
    // A wait that timed out says so in `error`, so an empty payload and a
    // timeout never read the same; otherwise its result is the payload.
    // (A delivered payload is never NULL: the column is NOT NULL and an
    // empty one reads back empty; rows of the first runner left NULL for a
    // timeout without the marker.)
    if (r && r.state === "done") return r.error === "timed out" || r.result === null ? undefined : r.result;
    if (r) {
      deadline = r.wakeAt ?? 0;
    } else {
      deadline = t + timeoutMs;
      db(() =>
        storage.execDurable(
          "INSERT INTO wasmkeep_wf_step (pos, name, kind, state, wake_at) VALUES (?, ?, 'event', 'pending', ?)",
          pos,
          name,
          deadline,
        ),
      );
    }
    const found = db(() =>
      storage.query(
        "SELECT seq, payload FROM wasmkeep_wf_event WHERE name = ? AND (consumed_at_pos IS NULL OR consumed_at_pos = ?) " +
          "ORDER BY seq LIMIT 1",
        name,
        pos,
      ),
    );
    if (found.length > 0) {
      const payload = asBytes(found[0].payload) ?? new Uint8Array();
      db(() => storage.exec("UPDATE wasmkeep_wf_event SET consumed_at_pos = ? WHERE seq = ?", pos, found[0].seq));
      db(() => storage.execDurable("UPDATE wasmkeep_wf_step SET state = 'done', result = ? WHERE pos = ?", payload, pos));
      return payload;
    }
    if (t >= deadline) {
      db(() =>
        storage.execDurable("UPDATE wasmkeep_wf_step SET state = 'done', result = NULL, error = 'timed out' WHERE pos = ?", pos),
      );
      return undefined;
    }
    throw this.waitUntil(deadline);
  }
}

// ------------------------------------------------------------------ store

const TABLES = [
  "CREATE TABLE IF NOT EXISTS wasmkeep_wf (id INTEGER PRIMARY KEY CHECK (id = 1), " +
    "status TEXT NOT NULL, run_id TEXT NOT NULL, input BLOB NOT NULL, output BLOB, " +
    "error TEXT, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL, " +
    "finished_at INTEGER, unfinished_runs INTEGER NOT NULL DEFAULT 0)",
  "CREATE TABLE IF NOT EXISTS wasmkeep_wf_step (pos INTEGER PRIMARY KEY, " +
    "name TEXT NOT NULL, kind TEXT NOT NULL, state TEXT NOT NULL, result BLOB, " +
    "error TEXT, attempts INTEGER NOT NULL DEFAULT 0, next_attempt_at INTEGER, wake_at INTEGER)",
  "CREATE TABLE IF NOT EXISTS wasmkeep_wf_event (seq INTEGER PRIMARY KEY AUTOINCREMENT, " +
    "name TEXT NOT NULL, payload BLOB NOT NULL, received_at INTEGER NOT NULL, consumed_at_pos INTEGER)",
];

interface Instance {
  status: State;
  runId: string;
  input: Uint8Array;
  output: Uint8Array | null;
  error: string | null;
  finishedAt: number | null;
  unfinishedRuns: number;
}

function load(): Instance | undefined {
  const rows = storage.query(
    "SELECT status, run_id, input, output, error, finished_at, unfinished_runs FROM wasmkeep_wf WHERE id = 1",
  );
  if (rows.length === 0) return undefined;
  const r = rows[0];
  return {
    status: (r.status as State) ?? "running",
    runId: String(r.run_id ?? ""),
    input: asBytes(r.input) ?? new Uint8Array(),
    output: asBytes(r.output),
    error: typeof r.error === "string" ? r.error : null,
    finishedAt: asNumber(r.finished_at),
    unfinishedRuns: asNumber(r.unfinished_runs) ?? 0,
  };
}

function recorded(): Map<number, Recorded> {
  const map = new Map<number, Recorded>();
  for (const r of storage.query(
    "SELECT pos, name, kind, state, result, error, attempts, next_attempt_at, wake_at FROM wasmkeep_wf_step ORDER BY pos",
  )) {
    map.set(asNumber(r.pos) ?? 0, {
      name: String(r.name ?? ""),
      kind: String(r.kind ?? ""),
      state: String(r.state ?? ""),
      result: asBytes(r.result),
      error: typeof r.error === "string" ? r.error : null,
      attempts: asNumber(r.attempts) ?? 0,
      nextAttemptAt: asNumber(r.next_attempt_at),
      wakeAt: asNumber(r.wake_at),
    });
  }
  return map;
}

function statusOf(instance: Instance): Status {
  const steps = recorded();
  let wakeAt: number | undefined;
  const waitingFor: string[] = [];
  if (instance.status === "waiting") {
    for (const r of steps.values()) {
      if (r.state !== "pending") continue;
      const at = r.nextAttemptAt ?? r.wakeAt;
      if (at !== null) wakeAt = wakeAt === undefined ? at : Math.min(wakeAt, at);
      if (r.kind === "event") waitingFor.push(r.name);
    }
  }
  return {
    state: instance.status,
    wakeAt,
    waitingFor,
    output: instance.output ?? undefined,
    error: instance.error ?? undefined,
    positions: steps.size,
  };
}

function setStatus(state: State, output: Uint8Array | null, error: string | null): void {
  const t = now();
  storage.execDurable(
    "UPDATE wasmkeep_wf SET status = ?, output = ?, error = ?, updated_at = ?, " +
      "finished_at = COALESCE(finished_at, ?) WHERE id = 1",
    state,
    output,
    error,
    t,
    isFinished(state) ? t : null,
  );
}

type Workflow = (wf: Run, input: Uint8Array) => string | Uint8Array | void;

function runOnce(config: Settings, workflow: Workflow): void {
  const t = now();
  const instance = load();
  if (!instance) {
    alarms.clear();
    storage.deleteSelf();
    return;
  }
  if (isFinished(instance.status)) {
    const until = (instance.finishedAt ?? t) + config.retentionMs;
    if (t >= until) {
      alarms.clear();
      storage.deleteSelf();
    } else {
      alarms.set(until);
    }
    return;
  }
  const limit = Math.max(config.maxAttempts * 2, 3);
  if (instance.unfinishedRuns >= limit) {
    setStatus(
      "failed",
      null,
      `the workflow's runs ended ${instance.unfinishedRuns} times without it returning ` +
        "(a trap or a cut outside any step, or a replay over its CPU budget)",
    );
    alarms.set(now() + config.retentionMs);
    return;
  }
  storage.exec("UPDATE wasmkeep_wf SET unfinished_runs = unfinished_runs + 1 WHERE id = 1");
  alarms.set(t + delay(config, instance.unfinishedRuns + 1));
  const run = new Run(actors.self().name, instance.runId, config, t, recorded());
  let output: Uint8Array | undefined;
  let stop: Stop | undefined;
  try {
    const result = workflow(run, instance.input);
    if (result === undefined) output = new Uint8Array();
    else if (typeof result === "string" || ArrayBuffer.isView(result)) output = toBytes(result as string | Uint8Array);
    else stop = fail(`the workflow returned a ${typeof result}: return a string or bytes`);
  } catch (e) {
    // Any other exception outside a step: like a trap in Rust, the run did
    // not finish. It is retried with backoff, and the unfinished-run limit
    // ends the instance if it keeps happening.
    if (!(e instanceof Stop)) throw e;
    stop = e;
  }
  // A `Stop` the workflow caught and then returned past still decides.
  if (run.firstStop !== undefined) stop = run.firstStop;
  if (stop?.kind === "retry") throw stop;
  storage.exec("UPDATE wasmkeep_wf SET unfinished_runs = 0 WHERE id = 1");
  if (stop === undefined) {
    setStatus("completed", output ?? new Uint8Array(), null);
    alarms.set(now() + config.retentionMs);
  } else if (stop.kind === "failed") {
    setStatus("failed", null, stop.message);
    alarms.set(now() + config.retentionMs);
  } else {
    const at = run.wake ?? now() + config.backoffMaxMs;
    const state: State = at <= now() ? "running" : "waiting";
    if (instance.status !== state) setStatus(state, null, null);
    alarms.set(at);
  }
}

/** Runs the instance: call it from `alarm`. Never throws: retries are its
 * own. */
export function onAlarm(config: Config, workflow: Workflow): void {
  const s = settings(config);
  try {
    runOnce(s, workflow);
  } catch {
    // The database refused, or the workflow threw outside a step: the
    // watchdog set before the run brings the next one (backed off by the
    // run counter); set one if there is none.
    if (alarms.get() === undefined) alarms.set(now() + s.backoffBaseMs);
  }
}

function serve(config: Settings, req: ActorRequest): ActorResponse {
  const path = req.path;
  if (!path.startsWith(PREFIX)) return { status: 404, body: "not a workflow path\n" };
  const rest = path.slice(PREFIX.length);
  const route = rest.split("?")[0];
  const t = now();
  const ok = (body: Uint8Array): ActorResponse => ({
    status: 200,
    headers: { "content-type": "application/octet-stream" },
    body,
  });
  if (req.method === "POST" && route === "/start") {
    const existing = load();
    if (!existing) {
      // The alarm first: it is a row of this database, so the durable
      // write below carries it too.
      alarms.set(t);
      const runId = hex(crypto_random(16));
      storage.execDurable(
        "INSERT INTO wasmkeep_wf (id, status, run_id, input, created_at, updated_at) VALUES (1, 'running', ?, ?, ?, ?)",
        runId,
        req.bytes(),
        t,
        t,
      );
    } else if (!isFinished(existing.status) && alarms.get() === undefined) {
      alarms.set(t);
    }
    return ok(encodeStatus(statusOf(load() as Instance)));
  }
  if (req.method === "GET" && route === "/status") {
    const i = load();
    return i ? ok(encodeStatus(statusOf(i))) : { status: 404, body: "no such workflow instance\n" };
  }
  if (req.method === "POST" && route === "/event") {
    const nameHex = new URLSearchParamsLite(rest).get("name");
    const name = nameHex === undefined ? undefined : unhexText(nameHex);
    if (name === undefined) return { status: 400, body: "event name missing\n" };
    const i = load();
    if (!i) return { status: 404, body: "no such workflow instance\n" };
    if (isFinished(i.status)) return { status: 409, body: "the workflow instance has finished\n" };
    alarms.set(t);
    storage.execDurable(
      "INSERT INTO wasmkeep_wf_event (name, payload, received_at) VALUES (?, ?, ?)",
      name,
      req.bytes(),
      t,
    );
    return ok(new Uint8Array());
  }
  if (req.method === "POST" && route === "/cancel") {
    const i = load();
    if (!i) return { status: 404, body: "no such workflow instance\n" };
    if (!isFinished(i.status)) {
      setStatus("cancelled", null, null);
      alarms.set(t + config.retentionMs);
    }
    return ok(encodeStatus(statusOf(load() ?? i)));
  }
  return { status: 404, body: "not a workflow path\n" };
}

/** Serves the client's protocol: call it from `fetch` for paths under
 * `/wasmkeep-workflow/v1`. Requests from outside clients (without
 * `x-wasmkeep-caller`) get `403`. */
export function handle(config: Config, req: ActorRequest): ActorResponse {
  let response: ActorResponse;
  if (header(req.headers, "x-wasmkeep-caller") === undefined) {
    response = { status: 403, body: "only other actors may use this workflow\n" };
  } else {
    try {
      response = serve(settings(config), req);
    } catch (e) {
      response = { status: 500, body: `${e instanceof Error ? e.message : String(e)}\n` };
    }
  }
  // A request that did not start an instance leaves nothing behind.
  const status = typeof response === "object" && !ArrayBuffer.isView(response) ? (response.status ?? 200) : 200;
  if (status !== 200) {
    try {
      if (load() === undefined) storage.deleteSelf();
    } catch {
      // The database refused: keep everything (it may hold an instance).
    }
  }
  return response;
}

/** Creates the tables: call it from `init`. */
export function init(): void {
  for (const sql of TABLES) storage.exec(sql);
}

/** A workflow class: the whole actor definition, from the workflow
 * function. `config` may be a function, called on each event: module code
 * runs at build time (pre-initialization), when `env.get` has no answer
 * yet, so settings read from the class's configuration go in one. */
export function define(workflow: Workflow, config: Config | (() => Config) = {}): ActorDefinition {
  const resolve = (): Config => (typeof config === "function" ? config() : config);
  return {
    init,
    alarm: () => onAlarm(resolve(), workflow),
    fetch: (req) => handle(resolve(), req),
  };
}

// --------------------------------------------------------------- client

/** What can go wrong talking to an instance. `kind`: `unknown` (never
 * started, or deleted after its retention), `finished` (no more events),
 * `call` (the call failed: it may or may not have been recorded),
 * `refused` (any other answer). */
export class WorkflowError extends Error {
  constructor(
    readonly kind: "unknown" | "finished" | "call" | "refused",
    message: string,
  ) {
    super(message);
    this.name = "WorkflowError";
  }
}

/** Starts and queries instances of one workflow class. */
export class Client {
  constructor(private readonly cls: string) {}

  private request(id: string, method: string, path: string, body: Uint8Array): Uint8Array {
    const r = call(`${this.cls}/${id}`, method, path, [], body);
    if ("error" in r) throw new WorkflowError("call", r.error);
    switch (r.reply.status) {
      case 200:
        return r.reply.body;
      case 404:
        throw new WorkflowError("unknown", "no such workflow instance");
      case 409:
        throw new WorkflowError("finished", "the workflow instance has finished");
      default:
        throw new WorkflowError("refused", `refused (${r.reply.status}): ${decoder.decode(r.reply.body).trim()}`);
    }
  }

  private status_(body: Uint8Array): Status {
    const s = decodeStatus(body);
    if (!s) throw new WorkflowError("refused", "malformed status");
    return s;
  }

  /** Starts instance `id` with `input`, or returns its status if it
   * exists. It runs in its own event, soon after. */
  start(id: string, input: string | Uint8Array = new Uint8Array()): Status {
    return this.status_(this.request(id, "POST", `${PREFIX}/start`, toBytes(input)));
  }

  /** The instance's status; `WorkflowError("unknown")` if there is none. */
  status(id: string): Status {
    return this.status_(this.request(id, "GET", `${PREFIX}/status`, new Uint8Array()));
  }

  /** Delivers event `name` with `payload`. */
  sendEvent(id: string, name: string, payload: string | Uint8Array = new Uint8Array()): void {
    this.request(id, "POST", `${PREFIX}/event?name=${hex(encoder.encode(name))}`, toBytes(payload));
  }

  /** Cancels the instance; returns its status. */
  cancel(id: string): Status {
    return this.status_(this.request(id, "POST", `${PREFIX}/cancel`, new Uint8Array()));
  }
}

// --------------------------------------------------------------- helpers

function crypto_random(n: number): Uint8Array {
  return getRandomBytes(BigInt(n));
}

/** `?a=1&b=2` lookups without `URLSearchParams` (QuickJS has none). */
class URLSearchParamsLite {
  constructor(private readonly query: string) {}
  get(name: string): string | undefined {
    const q = this.query.split("?")[1] ?? "";
    for (const pair of q.split("&")) {
      const [k, v] = pair.split("=");
      if (k === name) return v ?? "";
    }
    return undefined;
  }
}

function unhexText(s: string): string | undefined {
  if (s.length % 2 !== 0 || !/^[0-9a-fA-F]*$/.test(s)) return undefined;
  const bytes = new Uint8Array(s.length / 2);
  for (let i = 0; i < bytes.length; i++) bytes[i] = parseInt(s.slice(i * 2, i * 2 + 2), 16);
  try {
    return new TextDecoder("utf-8", { fatal: true }).decode(bytes);
  } catch {
    return undefined;
  }
}
