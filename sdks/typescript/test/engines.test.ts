// The engine hints added to errors, and the console installed on engines
// without one (QuickJS).

import { test } from "node:test";
import assert from "node:assert/strict";
import { engineHint, explainMissing, missingOnQuickjs } from "../src/engines";

test("a ReferenceError for a global QuickJS lacks names the engine that has it", () => {
  assert.equal(missingOnQuickjs("URL is not defined"), "URL");
  assert.equal(missingOnQuickjs("Error: crypto is not defined\n    at <anonymous>"), "crypto");
  assert.equal(
    explainMissing("fetch is not defined"),
    `fetch is not defined (${engineHint("fetch")})`,
  );
  assert(engineHint("URL").includes("--engine starlingmonkey"));
});

test("other errors are left alone", () => {
  for (const message of ["myHelper is not defined", "Intl is not defined", "boom", ""]) {
    assert.equal(missingOnQuickjs(message), undefined);
    assert.equal(explainMissing(message), message);
  }
});
