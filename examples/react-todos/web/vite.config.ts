// `npm run dev` serves the page with hot reload and sends every request to
// an actor (`/actor/...`) to `wasmkeep dev`, so the page and the API share
// one origin, as they do when the node serves the built files.
import react from "@vitejs/plugin-react";
import { realpathSync } from "node:fs";
import { defineConfig } from "vite";

// The SDK where npm put it; a link to a clone of WasmKeep resolves outside
// the project, and the dev server serves only the places it is allowed.
const sdk = realpathSync(new URL("../node_modules/@wasmkeep/sdk", import.meta.url));

export default defineConfig({
  plugins: [react()],
  server: {
    // `..` is the project: the contract is in ../src.
    fs: { allow: ["..", sdk] },
    proxy: { "/actor": "http://127.0.0.1:8080" },
  },
});
