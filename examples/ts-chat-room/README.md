# Chat room, in TypeScript

The [Rust chat room](../chat-room) in TypeScript, with [`@wasmkeep/sdk`](../../sdks/typescript):
WebSockets that stay open while the room hibernates, history in SQLite. It
passes the same end-to-end tests as the Rust room
(`crates/wasmkeep-server/tests/chat.rs`).

From the repository root:

```bash
(cd sdks/typescript && npm ci)
(cd examples/ts-chat-room && npm ci && npm run build)  # writes examples/ts-chat-room/chat_room.wasm
(cd examples/ts-chat-room && npm run build:starlingmonkey)  # or on StarlingMonkey: writes chat_room-starlingmonkey.wasm
cargo run -p wasmkeep-cli -- serve --actor chat=examples/ts-chat-room/chat_room.wasm
```

In another terminal, connect two WebSocket clients to
`ws://localhost:8080/actor/chat/lobby`.
