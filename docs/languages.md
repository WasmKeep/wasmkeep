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
| **Experimental** | A counter passes the shared tests, but something Supported requires is missing: an SDK, a chat room, numbers from the bench files, or stable upstream tooling. No compatibility promise. |
| **Not yet** | Cannot produce a working actor today; the entry says what blocks it. |

### Today

| Language | Tier | How it becomes a component | Why this tier |
| --- | --- | --- | --- |
| **Rust** | Recommended | `cargo build --target wasm32-wasip2`, [`wasmkeep-sdk`](../crates/wasmkeep-sdk) | SDK, examples, [tutorial](tutorials/first-actor.md); 81 KB; 104 µs of CPU per request, 250 KB per resident actor ([measured](benchmarks.md#the-same-counter-in-every-language)); no adapter (WASI 0.2 directly) |
| **Zig** | Supported | [Zig SDK](../sdks/zig) over the C SDK; the WASI 0.1 adapter | Counter, [chat room](../examples/zig-chat-room) and [jobs app](../examples/zig-jobs) (KV and queue clients) pass; 30 KB, the smallest. Performs like Rust: 107 µs of CPU per request, 242 KB per actor (with `--stack 1048576`; Zig's 16 MiB default stack takes megabytes more) ([measured](benchmarks.md#the-same-counter-in-every-language)). Recommended once it has a guide |
| **C / C++** | Supported | [C SDK](../sdks/c) over `wit-bindgen c`; clang + wasi-libc, the WASI 0.1 adapter | Counter, [chat room](../examples/c-chat-room) and [jobs app](../examples/c-jobs) (KV and queue clients) pass; 170 KB. Performs like Rust: 110 µs of CPU per request, 244 KB per actor ([measured](benchmarks.md#the-same-counter-in-every-language)). Recommended once it has a guide |
| **TypeScript / JavaScript** | Supported (embedded runtime) | [`@wasmkeep/sdk`](../sdks/typescript): esbuild, then componentize-qjs (QuickJS embedded, pre-initialized); or [ComponentizeJS](https://github.com/bytecodealliance/ComponentizeJS) (StarlingMonkey) with `--engine starlingmonkey` ([below](#typescript-on-quickjs)) | Counter, chat room and [jobs app](../examples/ts-jobs) (KV and queue clients) pass on both engines. QuickJS, the default: 1.7 MB; 230 µs of CPU per request (2.2× Rust) and 741 KB per actor (3×) ([measured](benchmarks.md#the-same-counter-in-every-language)), few Web APIs. StarlingMonkey: Web APIs and 1.5× faster plain JavaScript, for 4.6× the memory per actor ([measured](benchmarks.md#typescript-on-quickjs-against-starlingmonkey)) |
| **Python** | Supported (embedded runtime) | [Python SDK](../sdks/python), [componentize-py](https://github.com/bytecodealliance/componentize-py) (CPython embedded) | Counter, chat room and [jobs app](../examples/py-jobs) (KV and queue clients) pass. 18.7 MB, the largest; 259 µs of CPU per request (2.5× Rust) and 7.8 MB per actor (31×) ([measured](benchmarks.md#the-same-counter-in-every-language)) |
| **Go (TinyGo)** | Supported | [Go SDK](../sdks/go); TinyGo (0.39 in CI, 0.43 tried by hand) `-target=wasip2`, which writes a component directly | Counter, [chat room](../examples/go-chat-room) and [jobs app](../examples/go-jobs) (KV, queue and workflow clients) pass; 162 KB. 248 µs of CPU per request (2.4× Rust) with a p99 up to 40× its median (the garbage collector), 390 KB per actor ([measured](benchmarks.md#the-same-counter-in-every-language), on the counter before the SDK: the same host calls without the wrapper); see [Go](#go) |
| **Go (standard)** | Experimental | [componentize-go](https://github.com/bytecodealliance/componentize-go) 0.5.1 with Go 1.27, straight on its bindings | The [counter](../examples/go-std-counter) passes; 2.7 MB. No SDK yet (the Go SDK is for TinyGo's bindings), no numbers from `bench/run.sh` (see [standard Go](#standard-go-through-componentize-go)) |
| **D** | Experimental | [D SDK](../sdks/d) over `wit-bindgen d`; LDC (1.43 in CI) with `-betterC`, clang + wasi-libc, the WASI 0.1 adapter | Counter and [chat room](../examples/d-chat-room) pass; 88 KB. No numbers from `bench/run.sh` yet, no outbound HTTP or key-value and queue clients (see [D](#d)) |
| **MoonBit** | Experimental | [MoonBit SDK](../sdks/moonbit) over `wit-bindgen moonbit`; `moon build --target wasm` (MoonBit 0.10.20 in CI), then `wasm-tools component embed --encoding utf16` and `component new` | Counter and [chat room](../examples/moonbit-chat-room) pass; 49 KB. The language is pre-1.0; no numbers from `bench/run.sh` yet, no outbound HTTP or key-value and queue clients (see [MoonBit](#moonbit)) |
| **C# / .NET** | Experimental (spike) | [componentize-dotnet](https://github.com/bytecodealliance/componentize-dotnet) 0.8.0-preview with NativeAOT-LLVM, straight on its bindings | The [counter](../examples/csharp-counter) passes; 929 KB stripped. No SDK, chat room or numbers from `bench/run.sh`; every tool is preview (see [C#](#c-spike)) |

### Getting an SDK

Each SDK has the version of the server it is released with
([versions](#versions)); here for `0.4.0`.

| Language | Get it | Build an actor |
| --- | --- | --- |
| Rust | `cargo add wasmkeep-sdk` (crates.io) | `cargo build --release --target wasm32-wasip2` ([tutorial](tutorials/first-actor.md)) |
| TypeScript / JavaScript | `npm install @wasmkeep/sdk` (npm) | `npx wasmkeep-ts build src/index.ts -o actor.wasm` ([README](../sdks/typescript/README.md)) |
| Python | `pip install wasmkeep` (PyPI) | `python -m wasmkeep build app -o actor.wasm` ([README](../sdks/python/README.md)) |
| Go (TinyGo) | `go get github.com/wasmkeep/wasmkeep/sdks/go@v0.4.0` (a module of this repository) | `tinygo build -target=wasip2`, with the WIT the module ships ([README](../sdks/go/README.md#install)) |
| C / C++ | `wasmkeep-c-sdk-0.4.0.tar.gz`, a file of the [release](https://github.com/WasmKeep/wasmkeep/releases) | The build command in the archive's README ([C SDK](../sdks/c/README.md)) |
| Zig | `wasmkeep-zig-sdk-0.4.0.tar.gz`, a file of the release | The build command in the archive's README ([Zig SDK](../sdks/zig/README.md)) |
| D, MoonBit, C#, standard Go | The repository's source at the release's tag | Each example's `build.sh` |

**Fronts** (an app's stateless `wasi:http` component, the
`wasmkeep:actor/front` world, [ADR 0023](adr/0023-the-front.md)): Rust
(the `front` module of `wasmkeep-sdk`, without its default `actor`
feature) and TypeScript (`@wasmkeep/sdk/front`, `wasmkeep-ts build
--world front`), with the `caller`, `host`, `scheme` and `unavailable`
helpers; see the [Rust](../crates/wasmkeep-sdk/README.md#writing-a-front)
and [TypeScript](../sdks/typescript/README.md#writing-a-front) READMEs and
[`examples/todo-front`](../examples/todo-front). Python, Go, C, Zig, D and
MoonBit build actors only; their front targets come later. Any plain
`wasi:http` component is a front too, without the SDK.

**Several calls at once** (`calls.call-many`,
[ADR 0025](adr/0025-calling-several-actors-at-once.md)): Rust
(`calls::call_many`) and TypeScript (`actors.callMany`), from actors and
fronts. Python, Go, C, Zig, D and MoonBit get it later; until then they
call one actor at a time with `actors.call`, and keep building unchanged:
the interface is an import of its own, which the host provides whether a
component imports it or not.

The release's files are signed:
[check a download](deploy/single-node.md#1-install-the-binary).

### Next, in order

Each is experimental once a counter passes the shared tests; numbers
from `bench/run.sh` come before it is supported.

1. **An SDK for standard Go**, which now builds actors through
   componentize-go ([below](#standard-go-through-componentize-go)).

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
| MoonBit | 49 KB |
| Rust | 81 KB |
| D | 88 KB |
| Go (TinyGo) | 162 KB |
| C | 170 KB |
| C# | 929 KB (NativeAOT, debug sections stripped) |
| TypeScript | 1.7 MB (QuickJS engine, the default) |
| Go (standard) | 2.7 MB (componentize-go; Go's runtime) |
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
it is given. The [Go SDK](../sdks/go) is a `wasmkeep` package over the
bindings `wit-bindgen-go` generates (committed in the module, so an app
needs only TinyGo): an `Actor` of optional hooks, `Exec`, `Query` and
`Rows`, alarms, `Call`, WebSockets, `Env`, `Self` and a synchronous
`Fetch`, plus the [key-value, queue](kv-and-queues.md#go) and
[workflow](workflows.md#go) clients. TinyGo's runtime imports
`wasi:filesystem` whether or not the program uses it, so the module's
`wit/` adds those imports to the actor world (`actor-tinygo`); the host
provides them inert (no directories).

An app adds the module with `go get
github.com/wasmkeep/wasmkeep/sdks/go@v0.4.0`: the module carries the
server's version number. The `wasmkeep` package starts at `v0.2.0`
(`v0.1.0` has only the clients); see the SDK's
[Install](../sdks/go/README.md#install).

The [counter](../examples/go-counter), [chat room](../examples/go-chat-room)
and [jobs app](../examples/go-jobs) pass the shared tests, and
`fetch_langs` checks `Fetch` against the Rust SDK's `http::fetch`.
Trade-offs:

- **A garbage collector.** TinyGo's runs inside the request: the
  counter's p99 is 10× (reads) to 40× (writes) its median, against under
  1.5× for Rust ([measured](benchmarks.md#the-same-counter-in-every-language)).
  Allocating less per event shortens it.
- **TinyGo, not Go.** TinyGo compiles most of the standard library, not
  all of it (its documentation lists which packages build); the SDK keeps
  to packages it handles well (no `fmt` or reflection).
- **Size.** 162 KB for the counter, about twice Rust's; `strconv`'s number
  parsing alone adds about 50 KB, so the SDK reads integers from SQLite
  without it.
- **Not covered by the shared tests:** `SetAlarm` and the `Alarm` hook,
  `ExecDurable` and `DeleteSelf` from Go (`Call` is, through the jobs
  app's clients).

### Standard Go, through componentize-go

Standard Go (not TinyGo) builds actors through
[componentize-go](https://github.com/bytecodealliance/componentize-go)
0.5.1 with Go 1.27: the [counter](../examples/go-std-counter) passes the
same counter contract as Rust, and CI builds it. What blocked our own
attempt before (Go's runtime assumes it owns all linear memory past its
data, while the WASI 0.1 adapter needs memory before Go starts) does not
show: the component instantiates, survives hibernation and restarts.

What works:

- **The whole world.** `componentize-go bindings` generates Go packages
  for every interface of [`crates/wasmkeep-wit/wit`](../crates/wasmkeep-wit/wit),
  and the app implements two packages it names: `export_wit_world` (the
  five hooks, returning `witTypes.Result[witTypes.Unit, string]`) and
  `export_wasi_http_incoming_handler` (`Handle`). The component imports
  exactly the world's 0.2.12 interfaces.
- **Go's own compiler and runtime**, not TinyGo's subset of them: what
  builds with `go build` for `wasip1` builds here.

What is missing:

- **An SDK.** componentize-go's bindings (`go.bytecodealliance.org/pkg`)
  are not the `wit-bindgen-go` ones the [Go SDK](../sdks/go) wraps for
  TinyGo, so the SDK does not build on standard Go yet. Its `kv`,
  `queue` and `workflow` clients import no bindings and would carry over.
- **Size.** 2.7 MB, most of it Go's runtime (stripping custom sections
  saves under 3%), against 162 KB on TinyGo; no numbers from
  `bench/run.sh` for CPU or memory per actor yet.
- **Rough edges.** The module must be named `wit_component` (the
  bindings import it by that path), `bindings` rewrites `go.mod` from its
  own template on every run (the example's `build.sh` keeps the committed
  one), the build needs `-buildvcs=false` inside a git checkout, and the
  tool downloads its own binary from its GitHub release into the user's
  cache directory on first use.

## D

LDC compiles D with `-betterC` (no garbage collector, no D runtime, no
exceptions) to objects for `wasm32-unknown-wasi`; clang links them
against wasi-libc as a reactor, and the WASI 0.1 adapter makes the
component, as for C. `wit-bindgen d` generates the bindings, and the
[D SDK](../sdks/d) wraps them: a struct of static hooks exported with
`mixin Actor!S`, `exec`, `query` and `Rows` with parameters as plain D
arguments, alarms, `call`, WebSockets, `env` and `self`. Without a
garbage collector, everything the SDK returns lives until the event ends
and is freed then.

The [counter](../examples/d-counter) and [chat room](../examples/d-chat-room)
pass the shared tests. D stays experimental until `bench/run.sh` measures
it; the SDK has no outbound HTTP, key-value, queue or workflow clients
yet. `-betterC` also means no D standard library parts that need the
runtime (most of Phobos): text is built with `reply`'s printf formats or
arena memory.

## MoonBit

`wit-bindgen moonbit` generates the bindings as a MoonBit module whose
world package declares the hooks (`declare pub fn on_init`, ...); the
[MoonBit SDK](../sdks/moonbit)'s `build.sh` adds the SDK package, the app
package and a small glue file that implements those declarations by
calling the app's `actor` (an `@wasmkeep.Actor` of optional hooks). `moon
build --target wasm` makes a core module with no WASI imports of its own,
and `wasm-tools` makes the component, with UTF-16 strings (MoonBit's).
The SDK wraps storage (`exec`, `query`, `Rows`), alarms, `call`,
WebSockets, `env` and `self_address`; failures are MoonBit errors
(`Failure`), raised from the SDK and caught at the hooks.

The [counter](../examples/moonbit-counter) and
[chat room](../examples/moonbit-chat-room) pass the shared tests.
MoonBit stays experimental: the language is pre-1.0 (its string layout
changed in 2026, so the compiler, core library and `wit-bindgen` are
pinned together), `bench/run.sh` does not measure it yet, and the SDK has
no outbound HTTP, key-value, queue or workflow clients. MoonBit's servers
publish versions by their compiler version (`0.10.20+65efd5b93`, as
`moon version --all` prints it), not by `moon`'s dated version; CI pins
that one, checked by SHA-256.

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

## Versions

The SDKs carry the server's version: one number for the `wasmkeep` binary,
the Rust crate and every other SDK manifest, changed together. Use the SDK
with the same version as the server you run: other pairings are not
tested.

| SDK | Server | WIT package |
| --- | --- | --- |
| 0.4.0 | 0.4.0 | `wasmkeep:actor@0.2.0` |
| 0.3.0 | 0.3.0 | `wasmkeep:actor@0.2.0` |
| 0.2.0 | 0.2.0 | `wasmkeep:actor@0.2.0` |
| 0.1.0 | 0.1.0 | `wasmkeep:actor@0.2.0` |

`0.2.0` is the first version on the registries (crates.io, npm, PyPI);
`0.1.0` was used from a clone of the repository. That the SDK and the WIT
package both read `0.2.0` in the `0.2.0` row is a coincidence.

The WIT package version is another number: it is the version of the
interface between a component and the host
([`actor.wit`](../crates/wasmkeep-wit/wit/actor.wit)), and changes only when
what it already has changes. An addition keeps it: `0.4.0` added the
worlds `front` and `front-core` and the interface `calls` under
`wasmkeep:actor@0.2.0`, and a component built before links as it did
([ADR 0023](adr/0023-the-front.md),
[ADR 0025](adr/0025-calling-several-actors-at-once.md)).

`scripts/check-versions.py` (run by `./scripts/check.sh` and CI) fails when a
manifest it lists has another version than the workspace's in `Cargo.toml`;
an SDK that gains a manifest with a version is added to that list. The C
SDK's is `WASMKEEP_SDK_VERSION` in `wasmkeep.h`, the Zig SDK's `version` in
`wasmkeep.zig`; a release carries both SDKs as archives of that version
(`wasmkeep-c-sdk-<version>.tar.gz`, `wasmkeep-zig-sdk-<version>.tar.gz`).

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
(JavaScript, Python, Go, C#) produce larger components and wake more slowly than
Rust, C or Zig. The supported ones' numbers are published from `bench/run.sh` like the
others, so the trade-off is visible before you choose.

## Bring your own

If your language can produce a component that imports and exports what the
world declares, it works: there is nothing WasmKeep-specific in the binary
format. Generate bindings from
[`crates/wasmkeep-wit/wit`](../crates/wasmkeep-wit/wit) with your toolchain's
`wit-bindgen` (or equivalent), and check the result with
`wasm-tools component wit your.wasm`.
