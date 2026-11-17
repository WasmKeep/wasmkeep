// An actor that uses files: its component imports `blobs`.
import { defineActor } from "@wasmkeep/sdk";
import * as blobs from "@wasmkeep/sdk/blobs";

export default defineActor({
  fetch() {
    return String(blobs.head("a") === undefined);
  },
});
