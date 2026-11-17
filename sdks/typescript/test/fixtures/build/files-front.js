// A front that uses files.
import { defineFront } from "@wasmkeep/sdk/front";
import * as blobs from "@wasmkeep/sdk/blobs";

export default defineFront({
  fetch() {
    return String(blobs.head("a") === undefined);
  },
});
