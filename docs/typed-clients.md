# Typed actor clients and structured errors

A call to another actor is, at the bottom, a request with a path and some
bytes, and an answer with a status and some bytes. The TypeScript SDK can
put types on that. You describe each operation once, as a value (its name,
method, path, input, answer and the errors it may return), and both sides
import it: the caller gets a typed method, and every input and every answer
is checked at run time, not only at compile time.

This is additive. `actors.call`, `actors.callMany` and the `kv`, `queue`,
`db` and `workflow` clients are unchanged, and nothing here needs a newer
host: it is built on the calls an actor already makes. The Rust SDK does not
generate typed clients; it can write and read the error envelope below, so a
Rust actor and a TypeScript caller (or the reverse) understand each other's
failures.

## Defining a contract

A contract file imports only `@wasmkeep/sdk/contracts`. That entry loads no
host module, so the same file works in an actor, a front, a test and an
ordinary Node program.

```ts
// cart-contract.ts
import { codec, defineContract, defineOperation, withQuery } from "@wasmkeep/sdk/contracts";

export const addItem = defineOperation({
  name: "cart.addItem",
  method: "POST",
  path: (input) => withQuery("/", { sku: input.sku }),
  input: codec.object({ sku: codec.string(), qty: codec.integer() }),
  output: codec.object({ count: codec.integer() }),
  errors: {
    "cart.out_of_stock": codec.object({ left: codec.integer() }),
    "cart.closed": codec.none(),
  },
});

export const getCart = defineOperation({
  name: "cart.get",
  path: "/",
  output: codec.object({ count: codec.integer(), note: codec.optional(codec.string()) }),
});

export const cart = defineContract({ addItem, getCart });
```

The types are inferred from the codecs, so they cannot drift from what is
validated. A method that takes `POST`, `PUT` or `PATCH` sends its input as a
JSON body; `GET`, `HEAD` and `DELETE` send none, and the input goes into the
path (or a `body` function, if you want one).

### Codecs

A codec is any object with `parse(input: unknown): T` that throws when the
input is not a `T`. The built-in set (`string`, `number`, `integer`,
`boolean`, `null`, `literal`, `oneOf`, `array`, `record`, `object`,
`optional`, `nullable`, `union`, `unknown`, `none`) has no dependency, and a
validator you already use can stand in wherever one is expected if it has
that shape.

- `optional(c)` is for an object key that may be absent. `nullable(c)` is for
  a value that may be `null`. They are different: an optional key rejects
  `null`, and a nullable key must be present.
- An object codec keeps the keys it declares and drops the others, so a peer
  that adds a field does not break an older reader.
- `none()` stands for no value: an empty body, or an error without details.

### Paths and queries

Build paths with the `urlPath` tag and queries with `withQuery`. Each value
is percent-encoded as one segment, so a `/` inside a value cannot add a
segment, and `.` or `..` cannot climb. A value that is not text (a lone
surrogate) or not a scalar is refused before anything is sent.

```ts
path: (i) => withQuery("/", { limit: i.limit, tag: i.tags })
```

When a value belongs in the path itself, interpolate it into a template
tagged with `urlPath`, and it is encoded the same way.

## Calling an actor

```ts
import { actorClient } from "@wasmkeep/sdk"; // or "@wasmkeep/sdk/front"
import { addItem, cart } from "./cart-contract";

const client = actorClient();

const r = client.call("cart/alice", addItem, { sku: "A1", qty: 2 });
if (r.ok) {
  console.log(r.value.count);
}

// Or bind a whole contract to one actor:
const alice = client.bind("cart/alice", cart);
const same = alice.addItem({ sku: "A1", qty: 2 });
```

The client uses the actor transport as it is: address handling, the call
chain (a `reentrant` error is still reentrant), body limits and the front's
deadline are the host's. Calls are synchronous. For tests, or another
transport, `new Client(transport)` takes any object with `call` and
`callMany`.

A front can call a contract the same way; from a front the call is an
inside call, as with `actors.call`. A portable client for a browser would be
a separate thing pointed at a public URL: nothing here gives a browser the
privileges of an inside caller.

## Batching

`client.batch` sends several operations at once over `actors.callMany` and
returns one result per entry, in input order, typed per entry:

```ts
const [added, current] = client.batch([
  ["cart/alice", addItem, { sku: "A1", qty: 1 }],
  ["cart/bob", getCart],
] as const);
```

- Every entry is its own result, so one failure does not hide the others.
  Nothing collapses them into a thrown error.
- At most 16 entries (`MAX_BATCH`). A longer batch sends nothing and every
  entry is a `transport` failure, as the host does.
- An entry whose input is not valid is an `invalid-request` and is left out
  of what is sent; the others still go.
- It is `callMany`, not concurrency in your code: the calls start together,
  and the handler waits once.

## Results and errors

Nothing the callee or the network does is thrown. A call returns
`{ ok: true, status, value }` or `{ ok: false, failure }`, and `failure.kind`
is one of:

| kind | meaning |
| --- | --- |
| `transport` | no answer came (a `CallError`): the callee may or may not have run |
| `declared` | an error the operation lists; `code`, `message` and decoded `details` are typed |
| `problem` | a valid envelope whose code the operation does not list (kept, not dropped) |
| `invalid-details` | a listed code whose details do not match their codec |
| `unsupported-envelope` | an envelope of a version this SDK does not read |
| `malformed-envelope` | claims to be an envelope but is not valid |
| `http` | a failure without an envelope: a legacy actor, plain text, a redirect |
| `invalid-response` | a success status whose body is too large, not JSON, or the wrong shape |
| `invalid-request` | the input is not valid; nothing was sent |

An input is checked before it is sent and an answer before it is returned,
so an HTTP 200 with the wrong shape is an `invalid-response`, not a value
you believe.

`CallError.kind` is a string so a kind from a newer host passes through;
`error.known` narrows it to the five kinds this SDK knows (`invalid-address`,
`reentrant`, `too-large`, `timeout`, `failed`) and is `undefined` for others.

## The error envelope

An actor chooses to answer a failure with a structured body. It is a response
convention, nothing more: it does not change the transport, and it is not a
storage failure. A `CallError` means no answer; a `SqlError` is a database
statement failing; the envelope is an answer your own code wrote.

An answer is an envelope when its status is 400 or more and its content type
is `application/vnd.wasmkeep.error+json`. The body is UTF-8 JSON of at most
16 KiB:

```json
{"v":1,"code":"cart.out_of_stock","message":"Out of stock","details":{"left":0}}
```

- `code`: stable, for programs. A letter, then letters, digits and `_ . : -`,
  at most 64 characters.
- `message`: public text, at most 512 characters.
- `details`: optional JSON, nested at most 8 deep, objects of at most 64
  members, numbers finite.
- Fields a reader does not know are ignored. A version it does not know is
  reported as unsupported, not guessed at.

Produce one in TypeScript:

```ts
import { problem } from "@wasmkeep/sdk/contracts";

fetch(req) {
  return problem(409, "cart.out_of_stock", "Out of stock", { left: 0 });
}
```

and in Rust:

```rust
use wasmkeep_sdk::problem::{self, Problem};

let p = Problem::new("cart.out_of_stock", "Out of stock")?.with_details(r#"{"left":0}"#)?;
let response = problem::response(409, &p)?;
```

Writing one refuses a code, message or details outside these limits (a
`RangeError` in TypeScript, a `ProblemError` in Rust), so a mistake shows when
the handler is written. Treat message and details as public: leave out stack traces,
queries, storage locations and credentials. An unhandled
exception is still a plain 500, and `unavailable()` still answers a failed
call with its generic text; the envelope is only for errors you decide to
describe.

To read one in Rust, `problem::classify` (or `classify_response` for an
`actors::call` response) returns a `Failure`: `Problem`, `UnsupportedVersion`,
`Malformed` or `Plain`. The same cases exist in TypeScript as
`classifyFailure`.

## Actors that do not use the envelope

An actor that does not use the envelope still works with a typed client. Its
failures arrive as `http` failures with the status and the start of the body
as text, and its successes are decoded by the operation's codec like any
other. Adopt the envelope one operation at a time: declare the errors in the
contract once the actor returns them. A reader that predates the envelope
sees a normal 4xx or 5xx with a JSON body.

## What a call does not promise

A typed call changes nothing about durability. Whether a write is durable is
decided in the handler: use `storage.execDurable` or `storage.flush()` before
returning success, and return an error if they throw. A handler that cannot
tell whether its writes are durable (`flush` reports `not durable: still
uploading`) should say so in its error and not claim success; those writes
stay committed, and repeating the mutation is not the answer.

The client never retries, and no result says a call is safe to retry. After a
`transport` failure, a timeout, or a 503, a mutation may already have
committed. Make retryable business operations idempotent (an idempotency key
in the input that the actor stores with the write) and let the caller decide.

## Changing a contract over time

The types guarantee that caller and callee were written against the same
contract file, not that the deployed actor is on the same version. The
runtime checks are what protect a rolling deploy.

- Adding an output field is safe: readers drop what they do not declare.
- Adding an optional input field is safe for old callers; a new required one
  is not.
- Make a field `optional` before an actor stops sending it.
- Adding a declared error code is safe: an older client sees it as a
  `problem`, with the code and message preserved.
- Changing the type of a field, or removing one that is required, breaks
  readers: add a new operation (`cart.addItem2`, or a new path) and retire
  the old one when no caller is left.
- Raise `v` of the envelope only for a change a version-1 reader cannot
  understand. Such a reader reports `unsupported-envelope` with the version.

## Limits

- A success body is decoded up to 1 MiB by default (`maxResponseBytes` on an
  operation changes it); the host's own limits on bodies still apply.
- There is no typed Rust client yet; Rust writes and reads the envelope and
  calls actors with `actors::call` as before.
