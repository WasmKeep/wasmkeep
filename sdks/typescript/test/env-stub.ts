// Stands in for `wasmkeep:actor/env` under node (scripts/test.mjs maps the
// import here), for modules that import it beside the calls: nothing set.

export function get(_name: string): string | undefined {
  return undefined;
}
