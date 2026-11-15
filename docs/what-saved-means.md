# What saved means

Decide what your caller may rely on before sending "saved". A successful
SQL statement, a durable write and a response received by the client are
different points. This guide describes today's behavior; `flush` and
batched plain writes below are **planned**, under
[ADR 0034](adr/0034-batched-plain-writes.md).

## Choose the acknowledgement

| Your task | Use today | What success means |
| --- | --- | --- |
| Keep replaceable state, such as a cursor or a regenerable draft | Plain `exec` | Committed locally; it may be lost on failure |
| Tell a caller an order or edit is saved | `exec-durable` before the response | Durable in this actor's storage |
| Save several related changes together | `exec("BEGIN")`, statements with `exec`, then `exec-durable("COMMIT")` | The whole transaction is durable |
| Make earlier committed writes durable without another SQL statement | No dedicated call today; `storage.flush` is planned | The proposed call waits for all this actor's commits before it |

An event is not an implicit transaction. Outside `BEGIN` ... `COMMIT`,
each statement commits separately. A trap rolls back a transaction still
open, not statements already committed. A successful durable call covers
the writes through that call, not writes made later in the event.

## Know where the durable copy lives

**A standalone node, without a bucket:** plain `exec` commits to local
SQLite without waiting for an fsync. `exec-durable` waits for local
fsync. Losing the machine's storage still loses that copy; there is no
other node to restore it from.

**A node with a bucket:** this is cluster storage even if there is only
one node. `exec-durable` returns success after the write reaches the
bucket while the node still holds the actor. The bucket is the durable
copy; the node does not also fsync the write locally. Recovery checks
local state against the bucket and restores it when needed. Restart on
the same bucket and prefix. See [Replication](cluster.md#replication).

A cluster's plain `exec` returns after the local commit. At the end of
the event, the host uploads its committed changes and waits before
starting the actor's next event. An HTTP response or a socket message
can leave before that upload. Neither a successful plain call nor a
response based on it is a durable acknowledgement. Switching to plain
writes does not remove today's wait between writing events.

## Save before answering

These are helper functions called inside an actor event, after schema
initialization. The example uses a saved document and a replaceable
draft. Run each schema statement in its own storage call:

```sql
CREATE TABLE documents (id TEXT PRIMARY KEY, body TEXT NOT NULL);
CREATE TABLE drafts (id TEXT PRIMARY KEY, body TEXT NOT NULL);
```

Rust uses `wasmkeep_sdk::storage`, `exec_durable` and a parameter slice:

```rust
use wasmkeep_sdk::{SqlError, params, storage};

// Losing this draft is acceptable to this application.
fn keep_draft(id: &str, body: &str) -> Result<u64, SqlError> {
    storage::exec(
        "INSERT INTO drafts (id, body) VALUES (?1, ?2) \
         ON CONFLICT(id) DO UPDATE SET body = excluded.body",
        &params![id, body],
    )
}

// Return "saved" to the client only after this returns Ok.
fn save_document(id: &str, body: &str) -> Result<(), SqlError> {
    storage::exec("BEGIN", &[])?;
    let result = (|| -> Result<(), SqlError> {
        storage::exec(
            "INSERT INTO documents (id, body) VALUES (?1, ?2) \
             ON CONFLICT(id) DO UPDATE SET body = excluded.body",
            &params![id, body],
        )?;
        storage::exec("DELETE FROM drafts WHERE id = ?1", &params![id])?;
        storage::exec_durable("COMMIT", &[])?;
        Ok(())
    })();
    if result.is_err() {
        // Cleanup only: a failed durable commit may already have committed.
        let _ = storage::exec("ROLLBACK", &[]);
    }
    result
}
```

TypeScript uses `storage.execDurable` and separate parameter arguments.
These SDK calls are synchronous; there is no `await`:

```ts
import { storage } from "@wasmkeep/sdk";

function keepDraft(id: string, body: string): number {
  return storage.exec(
    "INSERT INTO drafts (id, body) VALUES (?1, ?2) " +
      "ON CONFLICT(id) DO UPDATE SET body = excluded.body",
    id, body,
  );
}

// Return "saved" only if this returns without throwing.
function saveDocument(id: string, body: string): void {
  storage.exec("BEGIN");
  try {
    storage.exec(
      "INSERT INTO documents (id, body) VALUES (?1, ?2) " +
        "ON CONFLICT(id) DO UPDATE SET body = excluded.body",
      id, body,
    );
    storage.exec("DELETE FROM drafts WHERE id = ?1", id);
    storage.execDurable("COMMIT");
  } catch (error) {
    // Cleanup cannot undo a commit whose upload or fsync failed.
    try { storage.exec("ROLLBACK"); } catch { /* keep the original error */ }
    throw error;
  }
}
```

For a single statement, use `storage::exec_durable(sql, &params![...])`
or `storage.execDurable(sql, ...params)` directly. Inside an open
transaction, the durable call accepts only `COMMIT` or `END`; it refuses
other statements before running them. Keep the entire transaction in
the same event. Validate and authorize inputs before these helpers.

## Locate the failure

| Failure point | What the caller can conclude |
| --- | --- |
| Before a transaction commits | Its uncommitted changes are rolled back; earlier commits or outside effects are separate |
| After plain commit, before fsync or cluster upload | Local state changed, but recovery may lose it, even if the client got a response |
| During a durable call, with an error or a lost connection | The outcome may be unknown: commit, upload or fsync may have happened |
| After durable success, before the client receives the answer | The write is durable, but the client does not know it |
| After the client receives a response sent after durable success | The actor's write is durable; this says nothing about later writes or another service's work |

In a cluster, an upload failure makes the node give up the actor; a later
event restores what the bucket holds. A failed upload is not proof that
the write is absent. Do not catch a storage `internal` error and answer
"saved", or retry the operation inside that same event.

An HTTP timeout, broken connection, `503` or `504`, or a failed
`actors.call`, is not a general "nothing ran" receipt. A forwarded
request may have run before its answer was lost. Reconnect, read the
operation's recorded outcome, and retry only with the same operation
identity. [Cluster failure handling](cluster.md#when-a-node-dies-what-a-caller-sees)
describes which requests wait and which are refused.

## Give retries an identity

An idempotency key is an application protocol, not something WasmKeep
adds to arbitrary writes. Have the client make a key before its first
attempt and keep it for retries:

1. Route every attempt to the same actor. Scope the key to the caller
   and operation so unrelated requests cannot collide.
2. In a transaction, look up that key. If it exists, verify that the
   request payload matches and return the recorded result; reject reuse
   for different work.
3. Otherwise apply the changes and store the key, a payload fingerprint
   and the result in that same transaction. Make its commit durable
   before answering or sending effects based on it.
4. Keep the record for the application's retry lifetime. Deleting it
   allows an old request to run again.

The save helpers above show atomic storage, not a complete retry
protocol: an old save retried after a newer edit could overwrite it.
Add the operation record, and an expected document version when the
application needs to reject stale edits.

A local deduplication record does not make outbound HTTP atomic with
SQLite. For an external effect, durably record pending work, send it
with a stable key the receiver deduplicates, then record completion.
If the receiver cannot deduplicate or report the outcome, a lost answer
still needs reconciliation. Actor-to-actor calls have the same boundary.

## Make alarms safe to repeat

Alarms fire **at least once**. A handler may run again after a crash,
failure or failover; treat its scheduled work as an operation with a
stable identity. Commit local work and its completion marker together,
durably, and return without repeating it when the marker exists. Use
the same receiver-side idempotency rule for outside effects.

An alarm setting is stored with the actor's state; setting an alarm is
not itself a durable acknowledgement. If accepting a job promises that
its alarm survives, include the setting before the durable commit that
accepts the job. A later alarm setting needs its own durable point. See
[Alarms](concepts.md#alarms).

The SDK [queue pattern](kv-and-queues.md#consistency-and-failure) also
delivers at least once. Deduplicate deliveries by queue and message id;
a producer retry after an unknown send can create another message id,
so retain a business operation key in the payload too. The
[database pattern](database.md#consistency-and-failure) makes writes and
batches durable when answered, but a lost answer is still ambiguous.
Neither pattern supplies a transaction with the caller's own database.

## Planned: flush earlier commits

[ADR 0034](adr/0034-batched-plain-writes.md) proposes `storage.flush`:
success would mean every write this actor committed before the call is
durable, in the bucket while the hold was valid, or fsynced locally
without a bucket. It would be refused inside an open transaction; use
a durable `COMMIT` there. It would not make another actor's state or an
external service's work part of the same transaction.

**It is not in the current SDKs.** The Rust and TypeScript examples
above use today's durable commit instead. Do not add a `flush` call or
deploy a component expecting that import yet.

## Planned: opt in to batched plain writes

The same ADR proposes per-class `writes = "batched"` for `0.5.0`, with
today's strict behavior remaining the default. This is not an available
configuration yet. A batched actor would run later events while an
upload is in flight, then combine their commits into the next segment.
Without a bucket, this mode would change nothing.

That trades a wider loss window for throughput: several completed
events could be lost, and later events could read and broadcast state
that a new owner never sees. Reads could go backwards after failover,
and locally allocated sequence numbers could be handed out again.
The proposed unsent-age control is backpressure, not a promise to save
within that time; upload time and event duration matter too.

Successful durable calls would retain their guarantee. The proposed
batched `flush` or `exec-durable` could instead report `internal` with
`not durable: still uploading`: its wait ended, but its writes stayed
local and could become durable later. That remains an unknown outcome,
not permission to answer success or retry blindly.

Use plain writes for state whose loss the application can repair, and
durable operations for promises it must keep. For throughput changes
available now, see [Designing for hot actors](designing-for-hot-actors.md).
