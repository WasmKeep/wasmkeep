// An actor that uses no files: its component must import no `blobs`.
import { defineActor } from "@wasmkeep/sdk";

export default defineActor({
  fetch() {
    return "plain";
  },
});
