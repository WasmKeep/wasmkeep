# Chat room, in Python

The [Rust chat room](../chat-room) in Python, with the [Python SDK](../../sdks/python):
WebSockets that stay open while the room hibernates, history in SQLite. It
passes the same end-to-end tests as the Rust and TypeScript rooms
(`crates/wasmkeep-server/tests/chat.rs`).

```bash
pip install componentize-py==0.25.1
(cd examples/py-chat-room && ./build.sh)   # writes examples/py-chat-room/chat_room.wasm
cargo run -p wasmkeep-cli -- serve --actor chat=examples/py-chat-room/chat_room.wasm
```

In another terminal, connect two WebSocket clients to
`ws://localhost:8080/actor/chat/lobby`.
