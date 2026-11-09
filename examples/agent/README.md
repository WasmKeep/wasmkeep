# Agent: an AI agent on WasmKeep

One actor per conversation: the history lives in the conversation's SQLite
database, the model is called over outbound HTTP, replies and progress go
out over a WebSocket, and a "remind me in N minutes" tool waits on an
alarm, with the actor hibernated meanwhile. The design is the one in
[use cases: AI agents](../../docs/use-cases.md#ai-agents).

| Route | What it does |
| --- | --- |
| `POST /actor/agent/<conversation>/message` | A text body: stored, sent to the model with the recent history, the reply stored and returned as text |
| `GET /actor/agent/<conversation>/history` | The conversation as a JSON array |
| WebSocket on `/actor/agent/<conversation>` | Every new entry as one JSON object per message; text sent on it is handled like `POST /message` |

Each entry is an OpenAI chat message plus `id`, `at` (ms since the epoch)
and `kind`: `user`, `assistant` (a reply, or a request to call a tool),
`tool` (the tool's result) or `reminder`.

## What it shows

- **Two messages to one conversation are handled in order.** The second
  waits for the first turn to finish, and the model sees the whole first
  exchange. No locks in the code: an actor handles one event at a time.
- **The history survives hibernation, restarts and moves between nodes**:
  it is the actor's database, replicated to the bucket in a cluster.
- **Waiting costs disk, not memory.** "Remind me in 20 minutes" stores a
  reminder and sets the actor's alarm; the actor hibernates, and the alarm
  wakes it to post the reminder to the history and the open sockets, with
  the sockets kept open by the host meanwhile. Alarms are delivered at
  least once, also when the node dies; a retry after the reminder was
  recorded does not post it twice.
- **Tools in the standard format.** `remind_me` is declared as an
  OpenAI-style function tool; the model's `tool_calls` and the results are
  stored in the history as the API expects them.
- **A cap on model calls**: `MAX_CALLS_PER_HOUR` per conversation (default
  60). Over it, `POST /message` answers 429 and the message is not kept.

## Configuration

From the class configuration, read at each event:

| Name | |
| --- | --- |
| `MODEL_URL` | The chat-completions endpoint, or its base (`.../v1`); `/chat/completions` is added if missing |
| `MODEL` | The model name sent with each request |
| `API_KEY` | Optional, a secret: sent as `Authorization: Bearer ...` and nowhere else |
| `SYSTEM_PROMPT` | Optional, sent first in every request |
| `MAX_CALLS_PER_HOUR` | Optional, default 60 |

The key is never written to the history, the database or the server's
logs (the host does not log guest requests' headers, and the agent prints
nothing: what an actor prints to stdout or stderr does reach the node's
log, see [what actors print](../../docs/monitoring.md#what-actors-print)),
and it is removed from error text before that text is returned
(some endpoints repeat a wrong key in their error). Keep it in a
`serve --env` file readable only by the server's user, not in a
`wasmkeep deploy --config` file: deployed configuration is stored in the
bucket in the clear.

## Run it with Ollama

The model must support tool calls (for example `llama3.2` or `qwen2.5`).
Ollama listens on loopback, which actors cannot reach unless
`--allow-outbound` opens it. From the repository root:

```bash
ollama pull llama3.2
(cd examples/agent && cargo build --release --target wasm32-wasip2)
printf 'MODEL_URL=http://127.0.0.1:11434/v1\nMODEL=llama3.2\n' > agent.env
cargo run -p wasmkeep-cli -- serve --env agent=agent.env \
  --allow-outbound 127.0.0.1 \
  --actor agent=examples/agent/target/wasm32-wasip2/release/agent.wasm
```

In another terminal:

```bash
A=localhost:8080/actor/agent/demo
curl -X POST $A/message -d 'Hi! Remind me to drink some water in 2 minutes.'
curl $A/history
websocat ws://localhost:8080/actor/agent/demo   # live entries, and send text
```

A model on another machine of your network (vLLM, a gateway) works the
same with its range: `--allow-outbound 10.0.0.0/8`; add `--outbound-ca`
if it is served over HTTPS with a private CA.

## Run it with a hosted API

Public addresses are reachable without any flag. From the repository root:

```bash
(cd examples/agent && cargo build --release --target wasm32-wasip2)
printf 'MODEL_URL=https://api.openai.com/v1\nMODEL=gpt-4o-mini\nAPI_KEY=%s\n' \
  "$OPENAI_API_KEY" > agent.env && chmod 600 agent.env
cargo run -p wasmkeep-cli -- serve --env agent=agent.env \
  --actor agent=examples/agent/target/wasm32-wasip2/release/agent.wasm
```

Any OpenAI-compatible endpoint works the same way (a provider's
compatibility endpoint, OpenRouter, a LiteLLM gateway).

## On the Compose cluster

[`deploy/compose`](../../deploy/compose) runs three nodes serving the app
`demo/counter`. To run the agent there, deploy it in that app next to the
counter, with the non-secret settings as deployed configuration:

```bash
(cd examples/counter && cargo build --release --target wasm32-wasip2)
(cd examples/agent && cargo build --release --target wasm32-wasip2)
printf 'MODEL_URL=https://api.openai.com/v1\nMODEL=gpt-4o-mini\n' > model.conf
AWS_ACCESS_KEY_ID=wasmkeep-demo AWS_SECRET_ACCESS_KEY=wasmkeep-demo-secret \
AWS_REGION=us-east-1 AWS_ENDPOINT=http://127.0.0.1:9000 AWS_ALLOW_HTTP=true \
cargo run -p wasmkeep-cli -- deploy --app demo/counter --bucket s3://wasmkeep/demo \
  --actor counter=examples/counter/target/wasm32-wasip2/release/counter.wasm \
  --actor agent=examples/agent/target/wasm32-wasip2/release/agent.wasm \
  --config agent=model.conf
```

The key does not go there: give each node an `--env agent=FILE` with
`API_KEY` (a Compose override adding the flag to each node's `command`
and mounting the file). For a model on the host, each node also needs
`--allow-outbound` for the host's address on the container network. A
conversation then moves to another node with its history if its node
stops. These cluster steps are not covered by the tests.

## The 30 s limit

An event (a request, an alarm, a socket message) runs at most 30 s of
wall-clock time (`ActorConfig::wall_per_event`; `wasmkeep serve` has no
flag for it yet). A turn therefore makes at most two model calls (one, and
one more after a tool round) within a 25 s budget, each with what is left
as its timeout. Every step is stored as it happens: the user's message
before the first call, the tool call and its results together before the
second. A turn cut short (a slow model, the limit) leaves a valid history,
and the next message continues from it; a turn that runs out of calls
after a tool round answers 504 and says so.

Replies come whole, not streamed: the SDK's `http::fetch` returns the
complete body. A large local model on a CPU can take longer than 25 s for
one reply; use a smaller model or a GPU, or a hosted API.

Tested end to end in
[`crates/wasmkeep-server/tests/agent.rs`](../../crates/wasmkeep-server/tests/agent.rs),
against a mock endpoint.
