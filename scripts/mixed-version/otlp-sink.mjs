// A test collector for scripts/mixed-version.sh: records what nodes export
// over OTLP/HTTP, and answers 200. It decodes nothing: a span carries its
// trace id as 16 raw bytes, so the script looks for a trace by the hex of
// its id in what was recorded.
//
// Each node is given its own endpoint, http://127.0.0.1:PORT/<node>, and
// posts to <endpoint>/v1/traces. Every POST is appended to DIR/<node>.hex
// as one line: the path, then the body in hex (gunzipped first when the
// request says it is gzip). GET / answers "ok" once it listens.
//
// Usage: node otlp-sink.mjs DIR PORT
import http from "node:http";
import fs from "node:fs";
import path from "node:path";
import zlib from "node:zlib";

const [dir, port] = process.argv.slice(2);
if (!dir || !port) {
  console.error("usage: otlp-sink.mjs DIR PORT");
  process.exit(2);
}
fs.mkdirSync(dir, { recursive: true });

http
  .createServer((req, res) => {
    if (req.method !== "POST") {
      res.writeHead(200, { "content-type": "text/plain" }).end("ok\n");
      return;
    }
    const chunks = [];
    req.on("data", (c) => chunks.push(c));
    req.on("end", () => {
      let body = Buffer.concat(chunks);
      if ((req.headers["content-encoding"] || "") === "gzip") {
        try {
          body = zlib.gunzipSync(body);
        } catch (e) {
          console.error(`${req.url}: a gzip body that does not decode: ${e.message}`);
        }
      }
      const node = (req.url.split("/")[1] || "none").replace(/[^a-z0-9-]/g, "_");
      fs.appendFileSync(path.join(dir, `${node}.hex`), `${req.url} ${body.toString("hex")}\n`);
      console.log(`${new Date().toISOString()} POST ${req.url} ${body.length} bytes`);
      // An empty ExportTraceServiceResponse.
      res.writeHead(200, { "content-type": "application/x-protobuf" }).end();
    });
  })
  .listen(Number(port), "127.0.0.1");
