/// <reference path="./wasmkeep-actor-types.d.ts" />
declare module 'wasmkeep:actor/storage@0.2.0' {
  /**
   * Runs a statement that returns no rows; returns the number of rows
   * changed. Returns once the write is committed to the local database.
   * Its upload follows: when the event ends in a class with strict writes
   * (the default), behind later events in one with batched writes.
   */
  export function exec(sql: string, params: Array<Value>): bigint;
  /**
   * Like `exec`, but returns only once the write is durable: replicated to
   * the bucket when the node has one, fsynced locally when it does not.
   * Inside a transaction it takes only `COMMIT` (or `END`), and makes the
   * whole transaction durable; any other statement there is refused with
   * `not-allowed` before it runs. Outside one it does not take a
   * statement that opens or controls a transaction (`BEGIN`, `SAVEPOINT`):
   * run those with `exec`. `internal` means the write is committed here
   * but not known to be durable: fail the request. In a class with strict
   * writes replication then stopped, and the actor is restored from the
   * bucket by its next event. In one with batched writes it is `exec`
   * then `flush`, and its `internal` means what `flush`'s does.
   */
  export function execDurable(sql: string, params: Array<Value>): bigint;
  /**
   * Runs a statement and returns its rows.
   */
  export function query(sql: string, params: Array<Value>): Rows;
  /**
   * Deletes this actor: its database is removed once the current event
   * ends, and its pending alarm is cleared. The next event to this address
   * starts a fresh actor.
   */
  export function deleteSelf(): void;
  /**
   * Deletes this actor now, and returns only once the delete is
   * durable: recorded in the bucket when the node has one (the next
   * event, wherever it runs, starts a fresh actor), the files removed
   * and fsynced when it does not. Its pending alarm is cleared. Later
   * storage calls in the same event are refused with `not-allowed`;
   * `get-alarm` then returns none, and `set-alarm` and `clear-alarm` do
   * nothing. Refused with `not-allowed` inside an open transaction,
   * changing nothing. `internal` means the delete is not known to be
   * durable (it may still land): the actor is restored from the bucket
   * by its next event, and calling this again is safe.
   */
  export function deleteSelfDurable(): void;
  /**
   * Returns once every write this actor committed before the call is
   * durable: in the bucket, uploaded while this node held the actor, when
   * the node has one; fsynced locally when it does not. Refused with
   * `not-allowed` inside an open transaction. `internal` means what was
   * committed since the last durable point is not known to be durable:
   * fail the request. A message starting `not durable: still uploading`
   * means the wait ran out while the upload went on: the writes stay
   * committed here, later events see them, and they usually become
   * durable soon after. Any other `internal` means replication stopped:
   * the actor's state is then whatever the bucket holds.
   */
  export function flush(): void;
  export type Value = import('wasmkeep:actor/types@0.2.0').Value;
  export type Rows = import('wasmkeep:actor/types@0.2.0').Rows;
  export type SqlError = import('wasmkeep:actor/types@0.2.0').SqlError;
}
