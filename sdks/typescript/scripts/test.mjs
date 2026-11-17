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
// The pure entry (`@wasmkeep/sdk/contracts`) must load no host module: bundle
// it with no stubs, so any `wasmkeep:` or `wasi:` import fails to resolve,
// and check the package maps it.
{
  const { readFile } = await import("node:fs/promises");
  const pkg = JSON.parse(await readFile(join(root, "package.json"), "utf8"));
  if (pkg.exports?.["./contracts"] !== "./src/pure.ts") {
    console.error('package.json must export "./contracts" as ./src/pure.ts');
    process.exit(1);
  }
  const r = await build({
    entryPoints: [join(root, "src", "pure.ts")],
    bundle: true,
    write: false,
    metafile: true,
    format: "esm",
    platform: "neutral",
    logLevel: "silent",
  }).catch((e) => {
    console.error(`src/pure.ts imports a host module:\n${e.message}`);
    process.exit(1);
  });
  const hostFiles = Object.keys(r.metafile.inputs).filter((f) => /(^|\/)(actors|call|index|front|db|kv|queue|workflow|http|handler|transport)\.ts$/.test(f));
  if (hostFiles.length > 0) {
    console.error(`src/pure.ts pulls in host-facing modules: ${hostFiles.join(", ")}`);
    process.exit(1);
  }
}
// The compile-time fixtures (test/typed-fixtures.ts) only count if `tsc`
// runs: `npm test` runs it first.
{
  const tsc = spawnSync(process.execPath, [join(root, "node_modules", "typescript", "bin", "tsc"), "--noEmit", "-p", root], { stdio: "inherit" });
  if (tsc.status !== 0) process.exit(tsc.status ?? 1);
}
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
          b.onResolve({ filter: /^wasmkeep:actor\/alarms@/ }, () => ({ path: join(root, "test", "room-alarms-stub.ts") }));
          b.onResolve({ filter: /^wasmkeep:actor\/websockets@/ }, () => ({ path: join(root, "test", "sockets-stub.ts") }));
          b.onResolve({ filter: /^wasmkeep:actor\/storage@/ }, () => ({ path: join(root, "test", "directory-storage-stub.ts") }));
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
