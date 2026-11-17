#!/usr/bin/env node
// npm test: type-checks, then bundles each test with Vite (which the example
// already has) and runs them with node's test runner.
import { build } from "vite";
import { spawnSync } from "node:child_process";
import { mkdtemp, readdir, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const typecheck = spawnSync("npm", ["run", "--silent", "typecheck"], { cwd: root, stdio: "inherit" });
if (typecheck.status !== 0) process.exit(typecheck.status ?? 1);

const entries = [];
for (const dir of ["test", "web/test"]) {
  for (const file of await readdir(join(root, dir))) {
    if (file.endsWith(".test.ts")) entries.push(join(root, dir, file));
  }
}
const out = await mkdtemp(join(tmpdir(), "react-todos-test-"));
try {
  await build({
    configFile: false,
    root,
    logLevel: "warn",
    // The SDK is TypeScript source: bundle it, do not leave it to node.
    ssr: { noExternal: ["@wasmkeep/sdk"] },
    build: {
      ssr: true,
      outDir: out,
      emptyOutDir: true,
      minify: false,
      rollupOptions: {
        input: Object.fromEntries(entries.map((e) => [e.slice(root.length + 1).replace(/\.ts$/, ""), e])),
        output: { format: "esm", entryFileNames: "[name].mjs" },
      },
    },
  });
  const files = entries.map((e) => join(out, e.slice(root.length + 1).replace(/\.ts$/, ".mjs")));
  const run = spawnSync(process.execPath, ["--test", ...files], { stdio: "inherit" });
  process.exitCode = run.status ?? 1;
} finally {
  await rm(out, { recursive: true, force: true });
}
