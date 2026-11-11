# WasmKeep actors in D

Write [WasmKeep](https://github.com/WasmKeep/wasmkeep/blob/main/README.md) actors in D with LDC and `-betterC`: no
garbage collector, no D runtime. The SDK is one module,
[`wasmkeep.d`](https://github.com/WasmKeep/wasmkeep/blob/main/sdks/d/wasmkeep.d), over the bindings `wit-bindgen d` generates.
D is **experimental** ([languages](https://github.com/WasmKeep/wasmkeep/blob/main/docs/languages.md#d)),
and Linux only for now.

The SDK is not published on its own: use it from the source of the
WasmKeep release your server runs (a clone at its tag, or the release's
source archive), whose version it has. It targets the
`wasmkeep:actor@0.2.0` WIT world.

```d
import wasmkeep;

struct Hits {
    // Once per deployed version, before any other event: create tables here.
    static void init() {
        if (exec("CREATE TABLE IF NOT EXISTS hits (at INTEGER NOT NULL)") < 0)
            fail(eventCopy(lastError()));
    }

    // One request at a time, in arrival order: no locks needed.
    static Response fetch(ref Request req) {
        exec("INSERT INTO hits (at) VALUES (?)", nowMs());
        return reply(200, "%lld hits on %.*s\n", queryInt("SELECT count(*) FROM hits"),
            cast(int) req.path.length, req.path.ptr);
    }

    // WebSockets: define only the hooks you need.
    static void message(ulong conn, const(ubyte)[] data, bool binary) {
        if (!binary) broadcast(cast(const(char)[]) data);
    }
}

mixin Actor!Hits;
```

Build it and serve it; see
[`examples/d-counter/build.sh`](https://github.com/WasmKeep/wasmkeep/blob/main/examples/d-counter/build.sh),
which this follows. LDC compiles to objects for `wasm32-unknown-wasi`,
clang links them against wasi-libc as a reactor, and the WASI 0.1 adapter
makes the component. It needs:

- LDC 1.43 ([releases](https://github.com/ldc-developers/ldc/releases);
  Arch: `pacman -S ldc`);
- clang, lld, wasi-libc and compiler-rt for wasm32 (Ubuntu/Debian:
  `apt install clang lld wasi-libc libclang-rt-18-dev-wasm32`; Arch:
  `pacman -S clang lld wasi-libc wasi-compiler-rt`);
- wit-bindgen 0.63.0 and wasm-tools
  (`cargo install wit-bindgen-cli --version 0.63.0 && cargo install wasm-tools`);
- curl and tar: `scripts/componentize-core.sh` downloads the adapter from
  crates.io on its first run.

From the root of that source tree, with your actor in `app.d`:

```bash
wit-bindgen d crates/wasmkeep-wit/wit --world actor --out-dir gen
ldc2 -mtriple=wasm32-unknown-wasi -betterC -O2 -release -Igen -Isdks/d -od=gen/obj -oq -c \
  app.d sdks/d/wasmkeep.d $(find gen/wit -name '*.d')
# wasi-libc: /usr on Debian, /usr/share/wasi-sysroot on Arch
clang --target=wasm32-wasi --sysroot=/usr/share/wasi-sysroot -mexec-model=reactor -nostartfiles \
  -Wl,--no-entry -Wl,--export-dynamic -Wl,--strip-debug gen/obj/*.o -o core.wasm
scripts/componentize-core.sh core.wasm actor.wasm
wasmkeep serve --actor hits=actor.wasm
```

`gen/` is the build's own directory: start it empty.

CI builds with LDC 1.43.0, clang and wasi-libc from Ubuntu 24.04, and
`wit-bindgen` 0.63.0.

## API

| | |
| --- | --- |
| Hooks | A struct of static functions, each optional, exported with `mixin Actor!S`: `init()`, `fetch(ref Request) -> Response`, `alarm()`, `open(ulong conn, const(char)[] path) -> bool` (false refuses), `message(ulong conn, const(ubyte)[] data, bool binary)`, `close(ulong conn, ushort code, const(char)[] reason)`. Without `open`, connections are accepted when `message` is defined |
| `fail(msg)` | Fails the current `init` or `alarm` event (an alarm is retried) |
| `exec(sql, args...)`, `execDurable(sql, args...)` | Rows changed, or -1 (then `lastError()`, `lastErrorKind()`). Arguments are `null`, `bool`, integers, floats, `const(char)[]` (text), `const(ubyte)[]` (blob) |
| `query(sql, args...)` | `Rows`: `ok`, `count`, `width`, `columnName(c)`, `column(name)`, `getInt`, `getReal`, `getText`, `getBlob`, `isNull` |
| `queryInt(sql, args...)` | First column of the first row (0 when there is none) |
| `setAlarm(ms)`, `getAlarm(out ms)`, `clearAlarm()` | The actor's single alarm, ms since the Unix epoch |
| `call(address, method, path, body, headers)` | Calls another actor (`class/name`): a `CallResponse` (`ok`, `status`, `body`, `headers`) |
| `send`, `sendBinary`, `broadcast(text, except)`, `close`, `connections` | WebSockets |
| `Request` | `method`, `path` (with the query), `headers`; `header(name)`, `body()`, `text()` |
| `reply(status, fmt, ...)`, `replyText(status, text, type)`, `withHeader(r, name, value)` | Responses (`Response`: `status`, `contentType`, `body`, `headers`) |
| `env(name)` | The class's configuration value or secret, or `null` |
| `self()` | This actor's `Address` (`tenant`, `class_`, `name`) |
| `deleteSelf()`, `nowMs()`, `eventAlloc(n)`, `eventCopy(s)` | |

**Memory.** Everything the SDK returns (rows, text, bodies, the bodies of
`reply`), and the arguments hooks receive (a WebSocket path, message or
close reason), live until the current event ends, when the SDK frees them:
nothing needs freeing by hand, and nothing may be kept past the event
without copying it (with `malloc`). `eventAlloc` and `eventCopy` give the
app memory with the same lifetime. `replyText` does not copy its text: pass
a literal or arena memory.

Not in the D SDK yet: outbound HTTP (`wasi:http/outgoing-handler`, usable
through the generated bindings directly) and the key-value, queue and
workflow clients.

## Examples

- [`examples/d-counter`](https://github.com/WasmKeep/wasmkeep/tree/main/examples/d-counter): the counter.
- [`examples/d-chat-room`](https://github.com/WasmKeep/wasmkeep/tree/main/examples/d-chat-room): the chat room.

They pass the same black-box tests as the Rust examples
(`crates/wasmkeep-server/tests/languages.rs` and `chat.rs`). The SDK has no
unit tests of its own.

## License

Apache-2.0; see `LICENSE` and `NOTICE`. WasmKeep is pre-alpha.
