/// <reference path="./interfaces/wasi-cli-environment.d.ts" />
/// <reference path="./interfaces/wasi-cli-exit.d.ts" />
/// <reference path="./interfaces/wasi-cli-stderr.d.ts" />
/// <reference path="./interfaces/wasi-cli-stdin.d.ts" />
/// <reference path="./interfaces/wasi-cli-stdout.d.ts" />
/// <reference path="./interfaces/wasi-cli-terminal-input.d.ts" />
/// <reference path="./interfaces/wasi-cli-terminal-output.d.ts" />
/// <reference path="./interfaces/wasi-cli-terminal-stderr.d.ts" />
/// <reference path="./interfaces/wasi-cli-terminal-stdin.d.ts" />
/// <reference path="./interfaces/wasi-cli-terminal-stdout.d.ts" />
/// <reference path="./interfaces/wasi-clocks-monotonic-clock.d.ts" />
/// <reference path="./interfaces/wasi-clocks-wall-clock.d.ts" />
/// <reference path="./interfaces/wasi-http-incoming-handler.d.ts" />
/// <reference path="./interfaces/wasi-http-outgoing-handler.d.ts" />
/// <reference path="./interfaces/wasi-http-types.d.ts" />
/// <reference path="./interfaces/wasi-io-error.d.ts" />
/// <reference path="./interfaces/wasi-io-poll.d.ts" />
/// <reference path="./interfaces/wasi-io-streams.d.ts" />
/// <reference path="./interfaces/wasi-random-insecure-seed.d.ts" />
/// <reference path="./interfaces/wasi-random-random.d.ts" />
/// <reference path="./interfaces/wasmkeep-actor-actors.d.ts" />
/// <reference path="./interfaces/wasmkeep-actor-alarms.d.ts" />
/// <reference path="./interfaces/wasmkeep-actor-calls.d.ts" />
/// <reference path="./interfaces/wasmkeep-actor-env.d.ts" />
/// <reference path="./interfaces/wasmkeep-actor-storage.d.ts" />
/// <reference path="./interfaces/wasmkeep-actor-types.d.ts" />
/// <reference path="./interfaces/wasmkeep-actor-websockets.d.ts" />
declare module 'wasmkeep:actor/actor@0.2.0' {
  export type Message = import('wasmkeep:actor/websockets@0.2.0').Message;
  export type * as WasiCliEnvironment0212 from 'wasi:cli/environment@0.2.12'; // import wasi:cli/environment@0.2.12
  export type * as WasiCliExit0212 from 'wasi:cli/exit@0.2.12'; // import wasi:cli/exit@0.2.12
  export type * as WasiCliStderr0212 from 'wasi:cli/stderr@0.2.12'; // import wasi:cli/stderr@0.2.12
  export type * as WasiCliStdin0212 from 'wasi:cli/stdin@0.2.12'; // import wasi:cli/stdin@0.2.12
  export type * as WasiCliStdout0212 from 'wasi:cli/stdout@0.2.12'; // import wasi:cli/stdout@0.2.12
  export type * as WasiCliTerminalInput0212 from 'wasi:cli/terminal-input@0.2.12'; // import wasi:cli/terminal-input@0.2.12
  export type * as WasiCliTerminalOutput0212 from 'wasi:cli/terminal-output@0.2.12'; // import wasi:cli/terminal-output@0.2.12
  export type * as WasiCliTerminalStderr0212 from 'wasi:cli/terminal-stderr@0.2.12'; // import wasi:cli/terminal-stderr@0.2.12
  export type * as WasiCliTerminalStdin0212 from 'wasi:cli/terminal-stdin@0.2.12'; // import wasi:cli/terminal-stdin@0.2.12
  export type * as WasiCliTerminalStdout0212 from 'wasi:cli/terminal-stdout@0.2.12'; // import wasi:cli/terminal-stdout@0.2.12
  export type * as WasiClocksMonotonicClock0212 from 'wasi:clocks/monotonic-clock@0.2.12'; // import wasi:clocks/monotonic-clock@0.2.12
  export type * as WasiClocksWallClock0212 from 'wasi:clocks/wall-clock@0.2.12'; // import wasi:clocks/wall-clock@0.2.12
  export type * as WasiHttpOutgoingHandler0212 from 'wasi:http/outgoing-handler@0.2.12'; // import wasi:http/outgoing-handler@0.2.12
  export type * as WasiHttpTypes0212 from 'wasi:http/types@0.2.12'; // import wasi:http/types@0.2.12
  export type * as WasiIoError0212 from 'wasi:io/error@0.2.12'; // import wasi:io/error@0.2.12
  export type * as WasiIoPoll0212 from 'wasi:io/poll@0.2.12'; // import wasi:io/poll@0.2.12
  export type * as WasiIoStreams0212 from 'wasi:io/streams@0.2.12'; // import wasi:io/streams@0.2.12
  export type * as WasiRandomInsecureSeed0212 from 'wasi:random/insecure-seed@0.2.12'; // import wasi:random/insecure-seed@0.2.12
  export type * as WasiRandomRandom0212 from 'wasi:random/random@0.2.12'; // import wasi:random/random@0.2.12
  export type * as WasmkeepActorActors020 from 'wasmkeep:actor/actors@0.2.0'; // import wasmkeep:actor/actors@0.2.0
  export type * as WasmkeepActorAlarms020 from 'wasmkeep:actor/alarms@0.2.0'; // import wasmkeep:actor/alarms@0.2.0
  export type * as WasmkeepActorCalls020 from 'wasmkeep:actor/calls@0.2.0'; // import wasmkeep:actor/calls@0.2.0
  export type * as WasmkeepActorEnv020 from 'wasmkeep:actor/env@0.2.0'; // import wasmkeep:actor/env@0.2.0
  export type * as WasmkeepActorStorage020 from 'wasmkeep:actor/storage@0.2.0'; // import wasmkeep:actor/storage@0.2.0
  export type * as WasmkeepActorTypes020 from 'wasmkeep:actor/types@0.2.0'; // import wasmkeep:actor/types@0.2.0
  export type * as WasmkeepActorWebsockets020 from 'wasmkeep:actor/websockets@0.2.0'; // import wasmkeep:actor/websockets@0.2.0
  export * as incomingHandler from 'wasi:http/incoming-handler@0.2.12'; // export wasi:http/incoming-handler@0.2.12
  /**
  * Runs before the first event an instance handles with this component
  * version: once for a new actor, and again after each deploy. Use it to
  * create or migrate tables. An error fails the event; it is retried with
  * the next event.
  */
  export function onInit(): void;
  /**
  * Runs when the pending alarm is due. An error or trap schedules a retry.
  */
  export function onAlarm(): void;
  /**
  * A client upgraded a GET to `path` (the part after the actor's name,
  * with its query). Return an error to refuse it: the connection is then
  * closed with code 1008 and the error as reason. It runs before the host
  * answers the upgrade (101), so the client is connected only once the
  * actor has accepted it. What the actor sends meanwhile waits in the
  * socket's queue, with its usual cap (`backpressure` past 4 MiB). A
  * client that leaves before the 101 (or a node relaying for it, which
  * gives up after 30 s) still gets this event if it was already queued,
  * followed by `on-websocket-close` with code 1006.
  */
  export function onWebsocketOpen(conn: bigint, path: string): void;
  /**
  * A message arrived on one of this actor's connections.
  */
  export function onWebsocketMessage(conn: bigint, msg: Message): void;
  /**
  * A connection closed (by the client, by `close`, or by the host).
  */
  export function onWebsocketClose(conn: bigint, code: number, reason: string): void;
  export type Result<T, E> = { tag: 'ok', val: T } | { tag: 'err', val: E };
}
