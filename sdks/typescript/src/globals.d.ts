// Web APIs actors can use on either engine: StarlingMonkey provides them,
// src/text.ts installs UTF-8 versions where QuickJS lacks them, and
// src/console.ts a `console` writing to stdout and stderr (WasmKeep logs
// both, see --guest-log-rate).
declare class TextEncoder {
  encode(input?: string): Uint8Array;
}
declare class TextDecoder {
  constructor(label?: string, options?: { fatal?: boolean; ignoreBOM?: boolean });
  decode(input?: Uint8Array): string;
}
declare const console: {
  assert(condition?: boolean, ...data: unknown[]): void;
  debug(...data: unknown[]): void;
  error(...data: unknown[]): void;
  info(...data: unknown[]): void;
  log(...data: unknown[]): void;
  trace(...data: unknown[]): void;
  warn(...data: unknown[]): void;
};
