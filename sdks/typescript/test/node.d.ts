// The parts of node's test runner the tests use (no @types/node needed).
declare module "node:test" {
  export function test(name: string, fn: () => void): void;
}
declare module "node:assert/strict" {
  const assert: {
    (value: unknown, message?: string): asserts value;
    equal(actual: unknown, expected: unknown, message?: string): void;
    deepEqual(actual: unknown, expected: unknown, message?: string): void;
    doesNotThrow(fn: () => unknown, message?: string): void;
    throws(fn: () => unknown, expected?: object | ((e: unknown) => boolean), message?: string): void;
  };
  export default assert;
}

declare module "node:fs" { export function readFileSync(path:string,encoding:"utf8"):string; }
declare module "node:sqlite" {
 export class DatabaseSync {
  constructor(path:string);
  exec(sql:string):void;
  close():void;
  prepare(sql:string):{run(...params:unknown[]):{changes:number|bigint};all(...params:unknown[]):Record<string,unknown>[];setReadBigInts(enabled:boolean):void;columns():{name:string}[]};
 }
}
declare module "node:fs" {
 export function mkdtempSync(prefix:string):string;
 export function rmSync(path:string,options:{recursive:boolean;force:boolean}):void;
}
declare module "node:os" {export function tmpdir():string;}
declare module "node:child_process" {
  export function execFileSync(
    file: string,
    args: string[],
    options: { stdio?: unknown; encoding?: "utf8" },
  ): string;
}
declare module "node:path" {
  export function join(...parts: string[]): string;
}
declare const process: { cwd(): string; execPath: string };
