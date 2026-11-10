# Chat room, in Go

The [Rust chat room](../chat-room) in Go, with the [Go SDK](../../sdks/go),
built with TinyGo: WebSockets that stay open while the room hibernates,
history in SQLite. It passes the same end-to-end tests as the Rust room
(`crates/wasmkeep-server/tests/chat.rs`).

```bash
(cd examples/go-chat-room && ./build.sh)   # writes examples/go-chat-room/chat_room.wasm (see build.sh for the tools)
cargo run -p wasmkeep-cli -- serve --actor chat=examples/go-chat-room/chat_room.wasm
```

In another terminal, connect two WebSocket clients to
`ws://localhost:8080/actor/chat/lobby`.
