// What `wasmkeep-ts build` imports (ADR 0030, decision 9): a component
// that uses no files imports no `wasmkeep:actor/blobs`, so it links on a
// node of any release; one that imports `@wasmkeep/sdk/blobs` does. Builds
// real components with both engines (a few seconds each).

import { test } from "node:test";
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

// The bundle runs from a temporary directory: find the SDK from the cwd
// `npm test` runs in.
const sdk = process.cwd();
const fixtures = join(sdk, "test", "fixtures", "build");
const jco = join(sdk, "node_modules", "@bytecodealliance", "jco", "dist", "jco.js");

function imports(fixture: string, world: string, engine: string): string[] {
  const dir = mkdtempSync(join(tmpdir(), "wasmkeep-build-"));
  try {
    const out = join(dir, "out.wasm");
    execFileSync(
      process.execPath,
      [join(sdk, "bin", "wasmkeep-ts.mjs"), "build", join(fixtures, fixture), "-o", out, "--world", world, "--engine", engine],
      { stdio: ["ignore", "ignore", "inherit"] },
    );
    const wit = execFileSync(process.execPath, [jco, "wit", out], { encoding: "utf8" });
    return wit
      .split("\n")
      .map((l: string) => l.trim())
      .filter((l: string) => l.startsWith("import "))
      .map((l: string) => l.slice("import ".length).replace(/;$/, ""));
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
}

const BLOBS = "wasmkeep:actor/blobs@0.2.0";

for (const engine of ["quickjs", "starlingmonkey"]) {
  for (const [fixture, world] of [
    ["plain-actor.js", "actor"],
    ["plain-front.js", "front"],
  ]) {
    test(`${fixture} on ${engine} imports no blobs`, () => {
      const got = imports(fixture, world, engine);
      assert(got.some((i) => i.startsWith("wasi:http/")), got.join(", "));
      assert(!got.includes(BLOBS), got.join(", "));
    });
  }
}

for (const [fixture, world] of [
  ["files-actor.js", "actor"],
  ["files-front.js", "front"],
]) {
  test(`${fixture} imports blobs`, () => {
    assert(imports(fixture, world, "quickjs").includes(BLOBS));
  });
}
