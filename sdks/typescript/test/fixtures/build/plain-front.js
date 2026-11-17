// A front that uses no files.
import { defineFront } from "@wasmkeep/sdk/front";

export default defineFront({
  fetch() {
    return "plain";
  },
});
