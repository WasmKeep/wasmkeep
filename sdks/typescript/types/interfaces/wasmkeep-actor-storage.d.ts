/// <reference path="./wasmkeep-actor-types.d.ts" />
declare module 'wasmkeep:actor/storage@0.2.0' {
  /**
   * Runs a statement that returns no rows; returns the number of rows
   * changed. Returns once the write is committed to the local database.
   */
  export function exec(sql: string, params: Array<Value>): bigint;
  /**
   * Like `exec`, but returns only once the write is durable: replicated to
   * the bucket when the node has one, fsynced locally when it does not.
   * Inside a transaction it takes only `COMMIT` (or `END`), and makes the
   * whole transaction durable; any other statement there is refused with
   * `not-allowed` before it runs. Outside one it does not take a
   * statement that opens or controls a transaction (`BEGIN`, `SAVEPOINT`):
   * run those with `exec`.
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
  export type Value = import('wasmkeep:actor/types@0.2.0').Value;
  export type Rows = import('wasmkeep:actor/types@0.2.0').Rows;
  export type SqlError = import('wasmkeep:actor/types@0.2.0').SqlError;
}
