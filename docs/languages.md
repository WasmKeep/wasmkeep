# Languages

An actor is a **WebAssembly component** implementing the
[`wasmkeep:actor` world](../crates/wasmkeep-wit/wit/actor.wit): an ordinary
`wasi:http` request handler, plus `on-init`, `on-alarm` and three WebSocket
hooks, with access to its SQLite database, alarms, actor calls and WebSockets.
Any language whose toolchain can produce a WASI 0.2 component can write
actors. WasmKeep runs them directly on Wasmtime, with no JavaScript or other
runtime in between unless the language brings its own.

## Which language to use

WasmKeep runs **WASI 0.2 components** on a server. A language works if its
toolchain can produce a component that implements the `wasmkeep:actor` world.
Most "WebAssembly support" you read about targets the browser instead
(wasm-bindgen, Emscripten, Blazor, Pyodide, Kotlin/Wasm for JS): that is a
different target, and does not make a language usable here.

| Tier | Meaning |
| --- | --- |
| **Recommended** | SDK, tutorial, examples passing the shared tests, and benchmark rows close to the best measured; a small component with no embedded runtime. |
| **Supported** | SDK; its counter and chat room pass the shared tests in CI with pinned tools; trade-offs stated with numbers from the bench files. |
| **Experimental** | A counter passes the shared tests, but no SDK or chat room yet, or upstream tooling is preview. No compatibility promise. |
| **Not yet** | Cannot produce a working actor today; the entry says what blocks it. |

### Today

| Language | Tier | How it becomes a component | Why this tier |
| --- | --- | --- | --- |
| **Rust** | Recommended | `cargo build --target wasm32-wasip2`, [`wasmkeep-sdk`](../crates/wasmkeep-sdk) | SDK, examples, [tutorial](tutorials/first-actor.md); 81 KB; 104 µs of CPU per request, 250 KB per resident actor ([measured](benchmarks.md#the-same-counter-in-every-language)); no adapter (WASI 0.2 directly) |
| **Zig** | Supported | [Zig SDK](../sdks/zig) over the C SDK; the WASI 0.1 adapter | Counter, [chat room](../examples/zig-chat-room) and [jobs app](../examples/zig-jobs) (KV and queue clients) pass; 30 KB, the smallest. Performs like Rust: 107 µs of CPU per request, 242 KB per actor (with `--stack 1048576`; Zig's 16 MiB default stack takes megabytes more) ([measured](benchmarks.md#the-same-counter-in-every-language)). Recommended once it has a guide |
| **C / C++** | Supported | [C SDK](../sdks/c) over `wit-bindgen c`; clang + wasi-libc, the WASI 0.1 adapter | Counter, [chat room](../examples/c-chat-room) and [jobs app](../examples/c-jobs) (KV and queue clients) pass; 170 KB. Performs like Rust: 110 µs of CPU per request, 244 KB per actor ([measured](benchmarks.md#the-same-counter-in-every-language)). Recommended once it has a guide |
| **TypeScript / JavaScript** | Supported (embedded runtime) | [`@wasmkeep/sdk`](../sdks/typescript): esbuild, then componentize-qjs (QuickJS embedded, pre-initialized); or [ComponentizeJS](https://github.com/bytecodealliance/ComponentizeJS) (StarlingMonkey) with `--engine starlingmonkey` ([below](#typescript-on-quickjs)) | Counter, chat room and [jobs app](../examples/ts-jobs) (KV and queue clients) pass on both engines. QuickJS, the default: 1.7 MB; 230 µs of CPU per request (2.2× Rust) and 741 KB per actor (3×) ([measured](benchmarks.md#the-same-counter-in-every-language)), few Web APIs. StarlingMonkey: Web APIs and 1.5× faster plain JavaScript, for 4.6× the memory per actor ([measured](benchmarks.md#typescript-on-quickjs-against-starlingmonkey)) |
| **Python** | Supported (embedded runtime) | [Python SDK](../sdks/python), [componentize-py](https://github.com/bytecodealliance/componentize-py) (CPython embedded) | Counter, chat room and [jobs app](../examples/py-jobs) (KV and queue clients) pass. 18.7 MB, the largest; 259 µs of CPU per request (2.5× Rust) and 7.8 MB per actor (31×) ([measured](benchmarks.md#the-same-counter-in-every-language)) |
| **Go (TinyGo)** | Experimental | TinyGo (0.39 in CI, 0.43 tested) `-target=wasip2` over `wit-bindgen-go` bindings | The [counter](../examples/go-counter) and the [jobs app](../examples/go-jobs) (the [`sdks/go`](../sdks/go) KV and queue clients) pass; 248 µs of CPU per request (2.4× Rust) with a p99 up to 40× its median (the garbage collector), 390 KB per actor ([measured](benchmarks.md#the-same-counter-in-every-language)); no full SDK, chat room or guide yet (see [Go](#go)) |
| **C# / .NET** | Experimental (spike) | [componentize-dotnet](https://github.com/bytecodealliance/componentize-dotnet) 0.8.0-preview with NativeAOT-LLVM, straight on its bindings | The [counter](../examples/csharp-counter) passes; 929 KB stripped. No SDK, chat room or numbers from `bench/run.sh`; every tool is preview (see [C#](#c-spike)) |

### Next, in order

Each needs a counter passing the shared tests, and numbers from
`bench/run.sh`, before it gets a tier.

1. **Go:** finish TinyGo (a `wasmkeep` package, the chat room, a guide), then
   try standard Go through [componentize-go](https://github.com/bytecodealliance/componentize-go),
   which allocates through Go's runtime before Go starts: the clash that
   blocked us (below).
2. **D** (LDC with `-betterC` and `wit-bindgen d`): no GC, no runtime; the
   generator handles our world.
3. **MoonBit** (`wit-bindgen moonbit`): small components; the language is
   pre-1.0 and its string layout changed in 2026, so tool versions must be
   pinned together.
**Bring your own** through the C SDK, the way Zig does: Odin and Nim should
work (C-sized components); Swift and Haskell are possible with more effort
(no bindings generator, larger runtimes). Recipes, not SDKs.

### Not yet

| Language | What blocks it | What would unblock it |
| --- | --- | --- |
| Kotlin | Kotlin/Wasm's WASI target is still WASI 0.1; component support is an early JetBrains prototype on a forked bindings generator; it compiles to garbage-collected Wasm | JetBrains shipping WASI 0.2 and component-model support (on their roadmap), and WasmKeep enabling Wasm GC |
| Java | No maintained path to a component: TeaVM removed its WASI backend, `wit-bindgen` removed its Java generator, GraalVM's Wasm output targets browsers and Node | A maintained compiler and bindings generator |
| Scala, OCaml | Compile to garbage-collected Wasm; WasmKeep's Wasmtime is built without Wasm GC, and the component model cannot pass GC data yet | Enabling Wasm GC in WasmKeep (its own decision: an ADR, benchmarks, pool sizing) |
| Swift | Stable WASI 0.1 SDK, but no WIT bindings generator for imports | A Swift `wit-bindgen` backend |
| AssemblyScript | Its maintainers reject the component model | — |
| Ruby | No bindings generator; very large runtime | A `componentize-rb` |
| PHP | The interpreter runs on WASI 0.1 as a CGI-style module (one script per request), not as a component; no bindings generator, so PHP cannot call storage or export the actor's hooks | A `componentize-php` (the interpreter as a library, `wit-dylib` glue, pre-initialization), the route `componentize-py` takes for Python |
| Grain, Crystal | No component support | Upstream work |
| Lua (via `wit-dylib`) | Cannot export interface functions yet (our world needs `incoming-handler`), and needs Wasm exceptions | Upstream `wit-dylib` progress, Wasm GC/exceptions in WasmKeep |
| JavaScript via Javy | Produces core modules, not components | Use [TypeScript on QuickJS](#typescript-on-quickjs) instead |

## Component sizes

The same counter, as built by each example's `build.sh` (sizes on disk; see
[benchmarks](benchmarks.md) for speed and memory):

| Language | Component |
| --- | --- |
| Zig | 30 KB |
| Rust | 81 KB |
| Go (TinyGo) | 151 KB |
| C | 170 KB |
| C# | 929 KB (NativeAOT, debug sections stripped) |
| TypeScript | 1.7 MB (QuickJS engine, the default) |
| TypeScript on StarlingMonkey | 13.4 MB (StarlingMonkey JS engine) |
| Python | 18.7 MB (CPython) |

## TypeScript on QuickJS

QuickJS is the default engine
([ADR 0018](adr/0018-quickjs-the-default-typescript-engine.md)):
`wasmkeep-ts build` (`npm run build` in the TypeScript examples) uses
[componentize-qjs](https://github.com/andreiltd/componentize-qjs) 0.5.0,
which the pinned jco 1.41.0 already ships: the
[QuickJS-ng](https://github.com/quickjs-ng/quickjs) engine, pre-initialized
with your bundle, built without component-model async (WasmKeep's hooks are
synchronous). `wasmkeep-ts build --engine starlingmonkey`
(`npm run build:starlingmonkey`) builds the same source with ComponentizeJS
and StarlingMonkey instead.

The counter, the chat room and the jobs app pass the same tests on both
engines (`crates/wasmkeep-server/tests/languages.rs`, `tests/chat.rs`,
`tests/jobs_langs.rs`). Measured on one machine
([benchmarks](benchmarks.md#typescript-on-quickjs-against-starlingmonkey)),
QuickJS against StarlingMonkey: 1.7 MB instead of 13.4 MB, 741 KB of
memory per resident actor instead of 3.4 MB, and 230 µs of CPU per request
instead of 368 µs on the counter; but 5.8 ms instead of 3.8 ms on plain
JavaScript work.

Choose StarlingMonkey when the code (or an npm package it uses) needs the
Web APIs below, or spends milliseconds computing in JavaScript per event.

What does not work, or works differently, on QuickJS:

- **Fewer Web APIs.** QuickJS has no `URL`, `URLSearchParams`, `crypto`
  (`getRandomValues`, `randomUUID`), `fetch`, `Headers`, `Request`,
  `Response`, streams, `Blob`, `AbortController`, `setTimeout` or
  `structuredClone`; StarlingMonkey has all of these. Neither has `Intl`.
  Code or npm packages that use them fail at build time (during
  pre-initialization) or when called, with an error that names
  `--engine starlingmonkey`. They are not stubbed, so libraries that test
  `typeof fetch` still pick their fallback. QuickJS does have `atob`,
  `btoa`, `queueMicrotask`, `performance` and `WeakRef`. For outbound
  HTTP on either engine, the SDK has `http.fetch`: synchronous, as
  handlers are (StarlingMonkey's own `fetch` returns a promise a handler
  cannot await).
- **`console` comes from the SDK**, writing one line per call to stdout
  (`log`, `info`, `debug`) or stderr (`warn`, `error`), as StarlingMonkey's
  does; objects are shown as JSON. WasmKeep logs both
  ([what actors print](monitoring.md#what-actors-print)).
- **`TextEncoder` and `TextDecoder` come from the SDK**, UTF-8 only and
  without streaming `decode`. The SDK installs them when the engine lacks
  them, so the same code runs on both engines.
- **A WIT `none` is `null`, not `undefined`.** The SDK's `env.get` and
  `alarms.get` return `undefined` on both engines; code that imports
  `wasmkeep:*` or `wasi:*` interfaces directly sees `null`.
- **Slower JavaScript.** Neither engine has a JIT, and QuickJS's
  interpreter is slower: 5.8 ms against 3.8 ms for the same plain
  JavaScript work ([measured](benchmarks.md#typescript-on-quickjs-against-starlingmonkey)).
  The counter spends little time in JavaScript, so there QuickJS wins on
  instantiation and the host.
- **Not covered by the shared tests:** `execDurable` from TypeScript on
  either engine (`actors.call` is, through the jobs app's key-value and
  queue clients). Alarms were checked by hand on QuickJS.

## Go

TinyGo's `wasip2` target writes a component directly, against a WIT world
it is given. The [counter](../examples/go-counter) uses the bindings
`wit-bindgen-go` generates from the actor world, with no SDK in between:
about 100 lines, most of them HTTP plumbing an SDK would hide. TinyGo's
runtime imports `wasi:filesystem` whether or not the program uses it, so
`build.sh` encodes the component against the actor world plus those
imports; the host provides them inert (no directories).

The [`sdks/go`](../sdks/go) module has the key-value and queue clients
([guide](kv-and-queues.md#go)), and [`examples/go-jobs`](../examples/go-jobs)
uses them. It imports no generated bindings: the app passes its own
`actors.Call` binding in. What is missing before Go counts as supported:
the rest of a `wasmkeep` package (the role the C and Zig SDKs play: rows, a
request and a response type, `Reply`), the chat room, and a guide.

Standard Go (not TinyGo) remains blocked. Go 1.24 can export functions to
Wasm, and we got as far as a
component that instantiates (generated bindings adapted to standard Go's
`go:wasmimport` rules, a Go `cabi_realloc`, lazy runtime start). It fails
because Go's runtime assumes it owns all linear memory past its data, while
the WASI 0.1 adapter needs memory before Go starts: their allocations
overlap. A small WASI shim that needs no memory of its own would fix it;
that is future work.

## C# (spike)

C# can produce a WasmKeep actor today. The
[counter](../examples/csharp-counter) is built with componentize-dotnet
0.8.0-preview00011 and NativeAOT-LLVM 10.0.0-rc.1 on the .NET 10 SDK, and
passes the same counter contract as Rust; CI builds it.

What works:

- **The whole world.** componentize-dotnet runs `wit-bindgen c-sharp` on
  [`crates/wasmkeep-wit/wit`](../crates/wasmkeep-wit/wit) at every build;
  the app implements two generated interfaces (`IActorWorldExports` for
  the five hooks, `IIncomingHandlerExports` for requests) and calls the
  imports as static methods (`IStorageImports.Exec`, ...).
- **One build command.** `dotnet build` restores the compiler and the
  tools it needs and writes a WASI 0.2 component directly; no adapter.
- **Errors as exceptions.** A WIT error result is thrown as a
  `WitException`; a hook fails its event by throwing
  `WitException<string>`.

What is missing:

- **Everything is preview.** The compiler comes from Microsoft's
  dotnet-experimental feed (a release candidate), the packages are
  previews, and the first restore is about 800 MB.
- **No macOS.** At the version componentize-dotnet uses, the
  NativeAOT-LLVM compiler is published for Linux (x64 and arm64) and
  Windows x64; the macOS packages on the feed are years older, and
  `Counter.csproj` names the Linux x64 one.
- **An older filesystem import.** The component imports
  `wasi:filesystem@0.2.6` where the world has 0.2.12, so
  `wasm-tools component targets` rejects it; WasmKeep links the imports
  semver-compatibly and runs it.
- **No SDK.** The code names generated types in full
  (`ActorWorld.wit.Imports.wasi.http.v0_2_12.ITypesImports.OutgoingResponse`)
  and does its own HTTP plumbing; an SDK on the bindings, the chat room
  and numbers from `bench/run.sh` come before a tier above experimental.
- **Size.** 2.5 MB as built, 929 KB with its debug sections stripped
  (`wasm-tools strip --all`, which `build.sh` runs): NativeAOT compiles
  the runtime and the garbage collector in.

## What the tiers require

- **Experimental:** a counter passes the **same black-box test** as the Rust
  one (create, increment, persist across hibernation and restart).
- **Supported:** an SDK; the counter and the chat room pass the shared tests
  (`--test languages`, `--test chat`); it builds in CI from a clean checkout
  with pinned tool versions; this page states its trade-offs with numbers
  from `bench/results`.
- **Recommended:** all of that, a guide (how to start an actor and call the
  storage, alarm, call and WebSocket APIs; today only Rust has one), and
  benchmark rows close to the best measured.

Languages that embed an interpreter or a garbage-collected runtime
(JavaScript, Python, C#) produce larger components and wake more slowly than
Rust, C or Zig. Their numbers are published from `bench/run.sh` like the
others, so the trade-off is visible before you choose.

## Bring your own

If your language can produce a component that imports and exports what the
world declares, it works: there is nothing WasmKeep-specific in the binary
format. Generate bindings from
[`crates/wasmkeep-wit/wit`](../crates/wasmkeep-wit/wit) with your toolchain's
`wit-bindgen` (or equivalent), and check the result with
`wasm-tools component wit your.wasm`.
