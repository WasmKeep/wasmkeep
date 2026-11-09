declare module 'wasmkeep:actor/env@0.2.0' {
  /**
   * The value of `name`, or none if it is not set.
   */
  export function get(name: string): string | undefined;
}
