# 0018. QuickJS, the default TypeScript engine

- Status: accepted and implemented (2026-11-06)
- Date: 2026-11-06
- Milestone: [M3](../../ROADMAP.md#m3-first-public-release) (languages), before the first public release

## Context

TypeScript actors embed a JavaScript engine in every component. The
original TypeScript guest strategy (decided 2026-11-03) chose
ComponentizeJS, which embeds StarlingMonkey (SpiderMonkey with Web
APIs). `wasmkeep-ts build --engine quickjs` added QuickJS-ng through
componentize-qjs, which the pinned jco already ships, with StarlingMonkey
kept as the default "until the numbers decide".

The numbers are in (the run that decided, with transparent huge pages on:
[`2026-11-06-dec1a82.csv`](../../bench/results/2026-11-06-dec1a82.csv);
[benchmarks](../benchmarks.md#typescript-on-quickjs-against-starlingmonkey) shows the current run):
on the counter, QuickJS takes about 3.5× less memory per resident actor
(1.06 MB against 3.7 MB; with transparent huge pages off, which
`wasmkeep serve` now does by default, 741 KB against 3.4 MB, [`2026-11-06-351ea57.csv`](../../bench/results/2026-11-06-351ea57.csv)), less CPU per request (231 µs against 367 µs),
components 8× smaller (1.6 MB against 13.4 MB) and a faster first start
(152 ms against 706 ms). On
plain JavaScript work (`js_work`: JSON, sorting, strings, a regex, a hash)
it is slower: 6.19 ms against 3.84 ms per request at the median, 1.6× ([`2026-11-06-dec1a82.csv`](../../bench/results/2026-11-06-dec1a82.csv)).

What QuickJS lacks is the Web APIs StarlingMonkey brings: `URL`,
`crypto`, `fetch`, `Headers`, timers, streams, `structuredClone` and
`console`. Neither engine has `Intl`.

## Decision

QuickJS is the default engine; StarlingMonkey stays an option,
`wasmkeep-ts build --engine starlingmonkey`. The examples' `npm run build`
builds on QuickJS and `npm run build:starlingmonkey` on StarlingMonkey,
and every TypeScript test runs on both.

The SDK closes the gaps that cost nothing:

- **`console`**: installed on QuickJS. As on StarlingMonkey, it writes to
  the component's stdout and stderr, which the node logs.
- **Clear errors**: a `ReferenceError` naming a global QuickJS lacks gets
  the engine to use added, from a handler (the 500 body, a hook's error)
  and from a build whose module code used it.

The missing globals are not stubbed: libraries test `typeof fetch` to
pick a fallback, and a stub that throws would break them.

## Why

- **Memory per actor decides how many actors a node holds**, and holding
  many is what WasmKeep is for. About 1 MB per resident TypeScript actor
  instead of 3.7 MB (741 KB against 3.4 MB with transparent huge pages
  off, the default for `wasmkeep serve`).
- **Actor handlers are synchronous.** A handler cannot await a `fetch` or
  a timer before it answers, so those matter less in a WasmKeep actor than
  in a Worker; calls between actors, storage, alarms and sockets go
  through the SDK on both engines.
- **The slowdown on JavaScript-heavy code is bounded** (1.6×, not the several times that would have kept StarlingMonkey the default),
  and an actor that needs speed or Web APIs can switch engines with one
  flag.
- **Licensing**: QuickJS-ng is MIT and componentize-qjs Apache-2.0;
  SpiderMonkey's MPL-2.0 requires making its source available when a
  component is distributed ([legal](../legal.md)).
- **It was decided before the first public release**, so no one
  depended on the old default.

## Consequences

- npm packages that use Web APIs need `--engine starlingmonkey`. The error
  says so.
- componentize-qjs is younger (0.5.0) than ComponentizeJS. Both stay
  tested; StarlingMonkey remains the fallback if QuickJS regresses.
- Benchmark rows: from 2026-11-06, `wasmkeep-ts` is QuickJS and
  `wasmkeep-ts-starlingmonkey` is StarlingMonkey. Before, `wasmkeep-ts` was
  StarlingMonkey and `wasmkeep-ts-quickjs` QuickJS.
- Guest output (stdout, stderr, `console`) reaches the node's log for
  every language since 2026-11-06 ([monitoring](../monitoring.md#what-actors-print)).
