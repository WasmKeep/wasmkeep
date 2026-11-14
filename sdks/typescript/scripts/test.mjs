#!/usr/bin/env node
// npm test: bundles each test/*.test.ts with esbuild (the SDK's own build
// dependency) and runs it with node's test runner. The host's
// `wasmkeep:actor/actors` import is replaced by test/host-stub.ts (and
// `wasmkeep:actor/calls` by test/calls-stub.ts, over it, and
// `wasmkeep:actor/env` by test/env-stub.ts, empty), so the
// clients run against fake actors; nothing else from the host is loaded.

import { build } from "esbuild";
import { spawnSync } from "node:child_process";
import { mkdtemp, readdir, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const stub = join(root, "test", "host-stub.ts");
const callsStub = join(root, "test", "calls-stub.ts");
const envStub = join(root, "test", "env-stub.ts");
const tests = (await readdir(join(root, "test"))).filter((f) => f.endsWith(".test.ts"));
const out = await mkdtemp(join(tmpdir(), "wasmkeep-sdk-test-"));
try {
  await build({
    entryPoints: tests.map((f) => join(root, "test", f)),
    outdir: out,
    outExtension: { ".js": ".mjs" },
    bundle: true,
    format: "esm",
    platform: "node",
    target: "node22",
    logLevel: "warning",
    plugins: [
      {
        name: "wasmkeep-host-stub",
        setup(b) {
          b.onResolve({ filter: /^wasmkeep:actor\/actors@/ }, () => ({ path: stub }));
          b.onResolve({ filter: /^wasmkeep:actor\/calls@/ }, () => ({ path: callsStub }));
          b.onResolve({ filter: /^wasmkeep:actor\/env@/ }, () => ({ path: envStub }));
          b.onResolve({ filter: /^(wasmkeep|wasi):/ }, (a) => ({
            errors: [{ text: `${a.path} is not available under node: test a module that does not import it` }],
          }));
        },
      },
    ],
  });
  const files = tests.map((f) => join(out, f.replace(/\.ts$/, ".mjs")));
  const run = spawnSync(process.execPath, ["--test", ...files], { stdio: "inherit" });
  process.exitCode = run.status ?? 1;
} finally {
  await rm(out, { recursive: true, force: true });
}
