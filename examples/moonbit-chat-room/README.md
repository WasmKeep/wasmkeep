# Chat room, in MoonBit

The [Rust chat room](../chat-room) in MoonBit, with the
[MoonBit SDK](../../sdks/moonbit): WebSockets that stay open while the room
hibernates, history in SQLite. It passes the same end-to-end tests as the
Rust room (`crates/wasmkeep-server/tests/chat.rs`).

```bash
(cd examples/moonbit-chat-room && ./build.sh)   # writes examples/moonbit-chat-room/chat_room.wasm
cargo run -p wasmkeep-cli -- serve --actor chat=examples/moonbit-chat-room/chat_room.wasm
```

In another terminal, connect two WebSocket clients to
`ws://localhost:8080/actor/chat/lobby`.
