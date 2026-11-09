// What the two JavaScript engines differ on, for clear errors.
//
// QuickJS (the default engine) lacks Web APIs that StarlingMonkey
// (`--engine starlingmonkey`) has. Using one throws `ReferenceError: X is
// not defined`: at build time when module code runs it (pre-initialization),
// or in the handler that calls it. The SDK (for a handler's error) and
// `wasmkeep-ts build` (for a failed build) add which engine has it. The
// names are not stubbed: libraries test `typeof fetch` to pick a fallback.

/** Globals StarlingMonkey has and QuickJS does not (docs/languages.md). */
export const QUICKJS_MISSING: readonly string[] = [
  "AbortController", "AbortSignal", "Blob", "crypto", "CryptoKey", "Event",
  "EventTarget", "fetch", "File", "FormData", "Headers", "ReadableStream",
  "Request", "Response", "SubtleCrypto", "TransformStream", "URL",
  "URLSearchParams", "WritableStream", "clearInterval", "clearTimeout",
  "setInterval", "setTimeout", "structuredClone",
];

/** The global a `ReferenceError` message names, if QuickJS lacks it. */
export function missingOnQuickjs(message: string): string | undefined {
  const name = /\b([A-Za-z_$][\w$]*) is not defined\b/.exec(message)?.[1];
  return name !== undefined && QUICKJS_MISSING.includes(name) ? name : undefined;
}

/** What to do about `name`, one of {@link QUICKJS_MISSING}. */
export function engineHint(name: string): string {
  return (
    `QuickJS, the default engine, has no ${name}; ` +
    "StarlingMonkey does: build with `wasmkeep-ts build --engine starlingmonkey`"
  );
}

/** `message` (an error's), with the engine to use when it names one of
 * {@link QUICKJS_MISSING}; unchanged otherwise. */
export function explainMissing(message: string): string {
  const name = missingOnQuickjs(message);
  return name === undefined ? message : `${message} (${engineHint(name)})`;
}
