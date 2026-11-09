// The parts of node's test runner the tests use (no @types/node needed).
declare module "node:test" {
  export function test(name: string, fn: () => void): void;
}
declare module "node:assert/strict" {
  const assert: {
    (value: unknown, message?: string): asserts value;
    equal(actual: unknown, expected: unknown, message?: string): void;
    deepEqual(actual: unknown, expected: unknown, message?: string): void;
    throws(fn: () => unknown, expected?: object | ((e: unknown) => boolean), message?: string): void;
  };
  export default assert;
}
