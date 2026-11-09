// `console` for engines that lack it.
//
// StarlingMonkey (`--engine starlingmonkey`) has `console`, which writes to
// the component's stdout and stderr. QuickJS (the default engine) has none,
// so code or an npm package calling `console.log` would throw. This module
// installs one that writes the same way: `log`, `info`, `debug`, `trace`,
// `dir` and `table` to stdout, `warn`, `error` and a failed `assert` to
// stderr, one line per call. WasmKeep logs what actors write there, tagged
// with the actor's address and within a line rate (`--guest-log-rate`).

import { getStderr } from "wasi:cli/stderr@0.2.12";
import { getStdout } from "wasi:cli/stdout@0.2.12";

/** One argument as `console` shows it. */
function show(value: unknown): string {
  if (typeof value === "string") return value;
  if (value instanceof Error) return value.stack ?? `${value.name}: ${value.message}`;
  if (typeof value === "object" && value !== null) {
    try {
      return JSON.stringify(value, (_k, v: unknown) => (typeof v === "bigint" ? `${v}n` : v));
    } catch {
      return String(value);
    }
  }
  return typeof value === "bigint" ? `${value}n` : String(value);
}

function writer(stream: () => { blockingWriteAndFlush(bytes: Uint8Array): void }) {
  return (...data: unknown[]): void => {
    const bytes = new TextEncoder().encode(`${data.map(show).join(" ")}\n`);
    try {
      const out = stream();
      // A blocking write takes at most 4096 bytes.
      for (let at = 0; at < bytes.length; at += 4096) {
        out.blockingWriteAndFlush(bytes.subarray(at, at + 4096));
      }
      (out as unknown as Disposable)[Symbol.dispose]?.();
    } catch {
      // Logging never fails the caller.
    }
  };
}

const g = globalThis as Record<string, unknown>;
if (typeof g.console !== "object" || g.console === null) {
  const out = writer(getStdout);
  const err = writer(getStderr);
  const noop = (): void => {};
  g.console = {
    log: out,
    info: out,
    debug: out,
    trace: out,
    dir: out,
    table: out,
    warn: err,
    error: err,
    assert: (condition?: unknown, ...data: unknown[]): void => {
      if (!condition) err("Assertion failed", ...data);
    },
    clear: noop,
    count: noop,
    countReset: noop,
    dirxml: noop,
    group: noop,
    groupCollapsed: noop,
    groupEnd: noop,
    time: noop,
    timeEnd: noop,
    timeLog: noop,
  };
}
