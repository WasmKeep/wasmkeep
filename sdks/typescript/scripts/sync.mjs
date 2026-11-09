// Copies the WIT world from crates/wasmkeep-wit and regenerates the type
// declarations for its imports. CI runs this and fails on any diff, so the
// SDK always matches the host it targets.
import { cp, rm } from "node:fs/promises";
import { execFileSync } from "node:child_process";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const wit = resolve(root, "../../crates/wasmkeep-wit/wit");
await rm(join(root, "wit"), { recursive: true, force: true });
await cp(wit, join(root, "wit"), { recursive: true });
await rm(join(root, "types"), { recursive: true, force: true });
execFileSync("npx", ["jco", "guest-types", join(root, "wit"), "--world-name", "actor", "-o", join(root, "types")], {
  stdio: "inherit",
});
