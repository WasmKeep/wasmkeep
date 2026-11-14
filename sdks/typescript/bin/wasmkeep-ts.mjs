#!/usr/bin/env node
// wasmkeep-ts build <entry.ts> [-o out.wasm] [--engine quickjs|starlingmonkey] [--world actor|front]
// wasmkeep-ts --help | --version
//
// Bundles a TypeScript (or JavaScript) actor whose default export is
// `defineActor({...})`, and turns it into a WebAssembly component
// implementing the `wasmkeep:actor/actor` world; with `--world front`, a
// front whose default export is `defineFront({...})` (from
// `@wasmkeep/sdk/front`), implementing `wasmkeep:actor/front`. By default with
// componentize-qjs (the QuickJS engine: small, and light per actor), or
// with ComponentizeJS (StarlingMonkey, with Web APIs such as fetch, URL
// and crypto) under `--engine starlingmonkey`. Either way the engine is
// pre-initialized with your code.

import { build } from "esbuild";
import { execFileSync } from "node:child_process";
import { createRequire } from "node:module";
import { readFile, rm, writeFile } from "node:fs/promises";
import { dirname, resolve, join, basename } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const sdkRoot = resolve(here, "..");
const require = createRequire(import.meta.url);

const usageLine =
  "usage: wasmkeep-ts build <entry.ts> [-o out.wasm] [--engine quickjs|starlingmonkey] [--world actor|front]";

function usage(msg) {
  if (msg) console.error(`error: ${msg}`);
  console.error(usageLine);
  process.exit(2);
}

const args = process.argv.slice(2);
if (args[0] === "--help" || args[0] === "-h") {
  console.log(usageLine);
  console.log("       wasmkeep-ts --help | --version");
  console.log("\nBuilds a TypeScript or JavaScript actor into a WebAssembly component,");
  console.log("with the QuickJS engine unless --engine says otherwise. With --world front,");
  console.log("builds a front (defineFront, from @wasmkeep/sdk/front) instead.");
  process.exit(0);
}
if (args[0] === "--version" || args[0] === "-V") {
  console.log(require("../package.json").version);
  process.exit(0);
}
if (args[0] !== "build") usage(args[0] ? `unknown command ${args[0]}` : undefined);
const entry = args[1] ? resolve(args[1]) : usage("no entry file");
const oi = args.indexOf("-o");
const out = resolve(oi > 0 ? args[oi + 1] : `${basename(entry).replace(/\.[cm]?[jt]s$/, "")}.wasm`);
const ei = args.indexOf("--engine");
const engine = ei > 0 ? args[ei + 1] : "quickjs";
if (engine !== "starlingmonkey" && engine !== "quickjs") usage(`unknown engine ${engine}`);
const wi = args.indexOf("--world");
const world = wi > 0 ? args[wi + 1] : "actor";
if (world !== "actor" && world !== "front") usage(`unknown world ${world}`);

// The generated entry module: the component's exports, from the actor's
// default export. The SDK comes first, so what it installs on engines that
// lack it (TextEncoder, TextDecoder and console on QuickJS) is there before any of the
// actor's modules run.
const wrapper = world === "front" ? `
import { frontExportsFor } from "@wasmkeep/sdk/front";
import definition from ${JSON.stringify(entry)};
export const incomingHandler = frontExportsFor(definition).incomingHandler;
` : `
import { exportsFor } from "@wasmkeep/sdk";
import definition from ${JSON.stringify(entry)};
const e = exportsFor(definition);
export const onInit = e.onInit;
export const onAlarm = e.onAlarm;
export const onWebsocketOpen = e.onWebsocketOpen;
export const onWebsocketMessage = e.onWebsocketMessage;
export const onWebsocketClose = e.onWebsocketClose;
export const incomingHandler = e.incomingHandler;
`;

const started = Date.now();
const bundle = await build({
  stdin: { contents: wrapper, resolveDir: dirname(entry), loader: "js", sourcefile: "wasmkeep-entry.js" },
  bundle: true,
  write: false,
  format: "esm",
  platform: "neutral",
  target: "es2022",
  external: ["wasi:*", "wasmkeep:*"],
  alias: {
    "@wasmkeep/sdk/front": join(sdkRoot, "src", "front.ts"),
    "@wasmkeep/sdk": join(sdkRoot, "src", "index.ts"),
  },
  logLevel: "warning",
});
const source = bundle.outputFiles[0].text;

// Componentize with jco. QuickJS is built without component-model async
// (`--backend-qjs-disable-async`): the host runs synchronous exports.
const sourcePath = join(dirname(out), ".wasmkeep-actor.js");
await writeFile(sourcePath, source);
try {
  // The CLI script next to the package's main module (dist/api.js).
  const jco = join(dirname(require.resolve("@bytecodealliance/jco")), "jco.js");
  execFileSync(
    process.execPath,
    [
      jco,
      "componentize",
      sourcePath,
      "--wit",
      join(sdkRoot, "wit"),
      "--world-name",
      world,
      ...(engine === "quickjs" ? ["--backend", "quickjs", "--backend-qjs-disable-async"] : []),
      "-o",
      out,
    ],
    { stdio: ["ignore", "ignore", "pipe"] },
  );
} catch (e) {
  // jco's own error (pre-initialization ran the actor's module code), then
  // which engine has the global it names, if that is why.
  const output = String(e.stderr ?? "");
  process.stderr.write(output);
  // A front that imports the actor world (`@wasmkeep/sdk` rather than
  // `@wasmkeep/sdk/front`, or storage, alarms or WebSockets directly).
  if (world === "front" && /wasmkeep:actor\/(storage|alarms|websockets|types)/.test(output + source)) {
    console.error(
      "\nerror: a front imports only actors and env: import from @wasmkeep/sdk/front, not @wasmkeep/sdk",
    );
    process.exit(1);
  }
  const engines = await enginesModule();
  const name = engines.missingOnQuickjs(output);
  if (engine === "quickjs" && name !== undefined) console.error(`\nerror: ${engines.engineHint(name)}`);
  process.exit(1);
} finally {
  await rm(sourcePath, { force: true });
}
const component = await readFile(out);

/** The SDK's src/engines.ts, compiled on the fly. */
async function enginesModule() {
  const { transform } = await import("esbuild");
  const ts = await readFile(join(sdkRoot, "src", "engines.ts"), "utf8");
  const { code } = await transform(ts, { loader: "ts", format: "esm" });
  const mod = await import(`data:text/javascript,${encodeURIComponent(code)}`);
  return mod;
}
console.log(`wrote ${out} (${(component.length / 1e6).toFixed(1)} MB) in ${((Date.now() - started) / 1000).toFixed(1)} s`);
