// The pure parts of workflows (ADR 0019): the status the runner reports,
// encoded as the Rust SDK's `workflow.rs` encodes it (byte for byte, so a
// client in either language follows a workflow in the other), the backoff,
// and the settings. No host imports: the unit tests run it under node.

import { Reader, Writer } from "./wire";

/** Where an instance stands. */
export type State = "running" | "waiting" | "completed" | "failed" | "cancelled";

const STATES: readonly State[] = ["running", "waiting", "completed", "failed", "cancelled"];

/** Whether the instance has finished. */
export function isFinished(state: State): boolean {
  return state === "completed" || state === "failed" || state === "cancelled";
}

/** An instance's status, as `Client.status` reports it. */
export interface Status {
  state: State;
  /** When waiting: until when (ms since the Unix epoch), at the latest. */
  wakeAt?: number;
  /** When waiting for events: their names. */
  waitingFor: string[];
  /** When completed: the workflow's output. */
  output?: Uint8Array;
  /** When failed: why. */
  error?: string;
  /** Positions recorded so far (steps, sleeps and waits). */
  positions: number;
}

const encoder = new TextEncoder();
const decoder = new TextDecoder();

export function encodeStatus(s: Status): Uint8Array {
  return new Writer()
    .bytes(encoder.encode(s.state))
    .u64(s.wakeAt ?? 0)
    .bytes(encoder.encode(s.waitingFor.join("\n")))
    .u64(s.output === undefined ? 0 : 1)
    .bytes(s.output ?? new Uint8Array())
    .bytes(encoder.encode(s.error ?? ""))
    .u64(s.positions)
    .finish();
}

export function decodeStatus(buf: Uint8Array): Status | undefined {
  const r = new Reader(buf);
  const state = r.bytes();
  const wake = r.u64();
  const names = r.bytes();
  const hasOutput = r.u64();
  const output = r.bytes();
  const error = r.bytes();
  const positions = r.u64();
  if (
    state === undefined ||
    wake === undefined ||
    names === undefined ||
    hasOutput === undefined ||
    output === undefined ||
    error === undefined ||
    positions === undefined
  ) {
    return undefined;
  }
  const s = decoder.decode(state) as State;
  if (!STATES.includes(s)) return undefined;
  const errorText = decoder.decode(error);
  return {
    state: s,
    wakeAt: wake === 0n ? undefined : Number(wake),
    waitingFor: decoder.decode(names).split("\n").filter((n) => n !== ""),
    output: hasOutput === 1n ? output : undefined,
    error: errorText === "" ? undefined : errorText,
    positions: Number(positions),
  };
}

/** A workflow class's settings; every field has a default. */
export interface Config {
  /** Attempts a step gets before it fails (5). */
  maxAttempts?: number;
  /** Backoff between attempts: `backoffBaseMs` doubling up to
   * `backoffMaxMs` (1 s to 5 min). */
  backoffBaseMs?: number;
  backoffMaxMs?: number;
  /** How long a finished instance is kept (7 days). */
  retentionMs?: number;
  /** Wall-clock time a run spends on new steps before it continues in a
   * new event (20 s); every run runs at least one. */
  runBudgetMs?: number;
}

export type Settings = Required<Config>;

/** `v` if it is a finite number, else `fallback` (a NaN from a bad
 * configuration value would disable the limits it sets). */
function finite(v: number | undefined, fallback: number): number {
  return v !== undefined && Number.isFinite(v) ? v : fallback;
}

export function settings(config: Config = {}): Settings {
  const base = Math.max(1, finite(config.backoffBaseMs, 1_000));
  return {
    maxAttempts: Math.max(1, Math.floor(finite(config.maxAttempts, 5))),
    backoffBaseMs: base,
    backoffMaxMs: Math.max(base, finite(config.backoffMaxMs, 300_000)),
    retentionMs: Math.max(0, finite(config.retentionMs, 7 * 86_400_000)),
    runBudgetMs: Math.max(0, finite(config.runBudgetMs, 20_000)),
  };
}

/** The wait before attempt `attempt + 1` after `attempt` failed. */
export function delay(s: Settings, attempt: number): number {
  const doublings = Math.min(Math.max(attempt - 1, 0), 32);
  return Math.min(s.backoffBaseMs * 2 ** doublings, s.backoffMaxMs);
}
