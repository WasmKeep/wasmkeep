# WasmKeep actors in MoonBit

Write [WasmKeep](../../README.md) actors in MoonBit. The SDK is one
package, [`wasmkeep`](wasmkeep/wasmkeep.mbt), over the bindings
`wit-bindgen moonbit` generates, and [`build.sh`](build.sh) turns an app
package into a component. MoonBit is **experimental**
([languages](../../docs/languages.md#moonbit)): the language is pre-1.0,
and its tool versions must be pinned together.

An app is a MoonBit package (its `.mbt` files and a `moon.pkg`) that
defines its hooks as `actor`:

```moonbit
///|
pub let actor : @wasmkeep.Actor = @wasmkeep.Actor::new(init=migrate, fetch~)

///|
fn migrate() -> Unit raise {
  @wasmkeep.exec("CREATE TABLE IF NOT EXISTS hits (at INTEGER NOT NULL)") |> ignore
}

///|
fn fetch(req : @wasmkeep.Request) -> @wasmkeep.Response raise {
  @wasmkeep.exec("INSERT INTO hits (at) VALUES (?)", params=[@wasmkeep.int(@wasmkeep.now_ms())]) |> ignore
  let n = @wasmkeep.query_int("SELECT count(*) FROM hits")
  @wasmkeep.reply(200, "\{n} hits on \{req.path}\n")
}
```

```text
import {
  "wasmkeep/actor/wasmkeep" @wasmkeep,
}
```

(`init` is MoonBit's package initializer, so the hook takes another name.)

Build it and serve it:

```bash
sdks/moonbit/build.sh path/to/app actor.wasm
wasmkeep serve --actor hits=actor.wasm
```

`build.sh` generates the bindings into `<app>/gen` (a MoonBit module named
`wasmkeep/actor`), copies the SDK in as package `wasmkeep` and the app as
package `app`, adds the glue that exports the app's hooks
([`glue/`](glue)), builds the core module with `moon build --target wasm`,
and makes the component with `wasm-tools component embed --encoding utf16`
(MoonBit strings are UTF-16) and `component new`. It needs `moon`,
`wit-bindgen` 0.63.0 and `wasm-tools`.

CI pins MoonBit `0.10.20+65efd5b93` (the toolchain of `moon
0.1.20261104`), from `https://cli.moonbitlang.com/binaries/<version>/`
and `.../cores/core-<version>.tar.gz`, checked by SHA-256. A MoonBit
installation needs its core library bundled for the `wasm` target in
`$MOON_HOME/lib/core` (`moon bundle --target wasm --release` there); the
official installer does this, some distribution packages do not.

## API

| | |
| --- | --- |
| Hooks | `Actor::new(init?, fetch?, alarm?, open?, message?, close?)`, each optional: `init : () -> Unit raise`, `fetch : (Request) -> Response raise`, `alarm : () -> Unit raise`, `open : (UInt64, String) -> Bool raise` (false refuses), `message : (UInt64, Message) -> Unit raise`, `close : (UInt64, Int, String) -> Unit raise`. Without `open`, connections are accepted when `message` is given. An error raised by `init` or `alarm` fails the event (an alarm is retried); one raised by `fetch` answers 500 |
| `exec(sql, params?)`, `exec_durable(sql, params?)` | Rows changed; a failed statement raises `Failure("<kind>: <message>")` |
| `query(sql, params?)` | `Rows`: `columns`, `values`, `count()`, `column(name)`, `get`, `get_int`, `get_real`, `get_text`, `get_blob`, `is_null` |
| `query_int(sql, params?)` | First column of the first row |
| `null()`, `int(n)`, `real(d)`, `text(s)`, `blob(b)` | Parameters (`Value`) |
| `set_alarm(ms)`, `get_alarm()`, `clear_alarm()` | The actor's single alarm, ms since the Unix epoch |
| `call(address, verb~, path~, headers~, body~)` | Calls another actor (`class/name`): a `CallResponse` (`status`, `headers`, `body`); no answer raises `Failure("<case>: <detail>")` |
| `send`, `send_binary`, `broadcast(text, except?)`, `close(conn, code~, reason~)`, `connections()` | WebSockets |
| `Request` | `method_`, `path` (with the query), `headers`; `header(name)`, `body()`, `text()` |
| `reply(status, text)`, `reply_bytes(status, type, bytes)`, `Response::with_header` | Responses |
| `env(name)` | The class's configuration value or secret, if set |
| `self_address()` | This actor's `Address` (`tenant`, `class`, `name`) |
| `delete_self()`, `now_ms()` | |

Not in the MoonBit SDK yet: outbound HTTP and the key-value, queue and
workflow clients.

## Examples

- [`examples/moonbit-counter`](../../examples/moonbit-counter): the counter.
- [`examples/moonbit-chat-room`](../../examples/moonbit-chat-room): the chat room.

They pass the same black-box tests as the Rust examples
(`crates/wasmkeep-server/tests/languages.rs` and `chat.rs`). The SDK has no
unit tests of its own.
