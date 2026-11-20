declare module 'wasmkeep:actor/alarms@0.2.0' {
  /**
   * Schedules `on-alarm` at `at` (milliseconds since the Unix epoch),
   * replacing any pending alarm. A time in the past fires as soon as
   * possible.
   */
  export function setAlarm(at: bigint): void;
  /**
   * The pending alarm time, if any. Inside `on-alarm` the alarm being
   * delivered is consumed: none until the handler calls `set-alarm` or
   * `clear-alarm`, then what it set (none after a clear).
   */
  export function getAlarm(): bigint | undefined;
  /**
   * Cancels the pending alarm, if any.
   */
  export function clearAlarm(): void;
}
