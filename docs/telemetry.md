# Usage ping

WasmKeep can send an anonymous usage report, so the project learns how many
installations exist and what they look like. It is **off by default**: a
node sends nothing unless it is started with `--usage-ping`.

```bash
wasmkeep serve --usage-ping --actor counter=counter.wasm
```

## What is sent

One small JSON document, a minute after the node starts and then once a
day, as an HTTP POST:

```json
{"schema":1,"version":"0.2.0","os":"linux","arch":"x86_64","cluster":true,"classes":"1-9","resident_actors":"10-99"}
```

| Field | Meaning |
| --- | --- |
| `schema` | Format version of this report |
| `version` | WasmKeep's version |
| `os`, `arch` | Operating system and CPU architecture (`linux`, `x86_64`, ...) |
| `cluster` | Whether the node runs in a cluster (`--node-id`) |
| `classes` | How many actor classes it serves, as a bucket: `0`, `1-9`, `10-99`, `100-999`, `1000-9999`, `10000+` |
| `resident_actors` | How many actors are in memory at that moment, same buckets |

That is all. No identifier for the node or the installation, no host
names, addresses, tenant, class or actor names, no request data, no
configuration. The receiving end sees the connection's IP address, as any
HTTP server does; how the collector treats it will be stated here when one
exists.

Each report is written to the node's log (`sending the usage ping`)
before it goes out, so you can see exactly what was sent. Failing to send
is logged at debug level and never affects serving.

## Where it goes

Nowhere yet: the project does not run a collector, so in current builds
`--usage-ping` logs that it has no endpoint and sends nothing. When a
collector exists, its address will be built into the release and listed
here.

`WASMKEEP_USAGE_PING_URL` sends the report to another URL instead, for
example your own collector.

The code is short: [`crates/wasmkeep-cli/src/usage.rs`](../crates/wasmkeep-cli/src/usage.rs).
