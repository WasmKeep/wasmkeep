// The app class of the db example, in TypeScript: the routes of the Rust
// app in examples/db/app (listed there), over the SDK's database client,
// with the same answers byte for byte. Every SQL statement is written
// here, with its values bound as parameters: nothing from a request
// becomes SQL text.
import { db, defineActor, type ActorResponse, type SqlValue } from "@wasmkeep/sdk";

const main = new db.Client("db/main");
const encoder = new TextEncoder();

function hex(bytes: Uint8Array): string {
  return Array.from(bytes, (b) => b.toString(16).padStart(2, "0")).join("");
}

/** A value as the Rust app prints it: text and blobs in hex. */
function show(v: SqlValue): string {
  if (v === null) return "null";
  if (typeof v === "string") return hex(encoder.encode(v));
  if (v instanceof Uint8Array) return hex(v);
  return String(v);
}

function describe(e: unknown): string {
  if (!(e instanceof db.DbError)) throw e;
  switch (e.kind) {
    case "sql":
      return `sql ${e.sqlKind} ${e.statement ?? "commit"}\n`;
    case "not-allowed":
      return `not-allowed ${e.statement}\n`;
    case "too-large":
      return "too-large\n";
    case "refused":
      return `refused ${e.status}\n`;
    case "call":
      return "call\n";
  }
}

function route(method: string, path: string, text: string): string | undefined {
  switch (`${method} ${path}`) {
    case "POST /notes": {
      const [r] = main.batch([{ sql: "INSERT INTO notes (body) VALUES (?1) RETURNING id", params: [text], rows: true }]);
      return "columns" in r ? `${show(r.rows[0][0])}\n` : undefined;
    }
    case "GET /notes":
      return main
        .query("SELECT id, body FROM notes ORDER BY id")
        .rows.map((r) => `${show(r[0])} ${r[1]}\n`)
        .join("");
    case "GET /count":
      return `${show(main.query("SELECT COUNT(*) AS n FROM notes").rows[0][0])}\n`;
    case "POST /notes/batch": {
      const lines = text.split("\n");
      if (lines[lines.length - 1] === "") lines.pop();
      const outcomes = main.batch(lines.map((line) => ({ sql: "INSERT INTO notes (body) VALUES (?1)", params: [line] })));
      return `${outcomes.reduce((n, o) => n + ("changes" in o ? o.changes : 0), 0)}\n`;
    }
    case "POST /notes/delete-in-read":
      return `${main.query("DELETE FROM notes RETURNING id").rows.length}\n`;
    case "POST /sneaky":
      main.batch([
        { sql: "INSERT INTO notes (body) VALUES ('sneaky')" },
        { sql: "/* done */ commit" },
        { sql: "INSERT INTO notes (body) VALUES ('after')" },
      ]);
      return "kept\n";
    case "POST /orphan":
      main.batch([
        { sql: "INSERT INTO notes (body) VALUES ('tagged')" },
        { sql: "INSERT INTO tags (note, tag) VALUES (999, 'x')" },
      ]);
      return "kept\n";
    case "GET /tags":
      return `${show(main.query("SELECT COUNT(*) AS n FROM tags").rows[0][0])}\n`;
    case "GET /types": {
      const put = "INSERT OR REPLACE INTO vals (k, v) VALUES (?1, ?2)";
      main.batch([
        { sql: put, params: ["a-max", 2n ** 63n - 1n] },
        { sql: put, params: ["b-min", -(2n ** 63n)] },
        { sql: put, params: ["c-real", { tag: "real", val: 2 }] },
        { sql: put, params: ["d-tenth", 0.1] },
        { sql: put, params: ["e-text", 'tab\t"é" \u{1f600}\u{1}'] },
        { sql: put, params: ["f-blob", new Uint8Array([0, 255, 16])] },
        { sql: put, params: ["g-null", null] },
        { sql: put, params: ["h-empty", new Uint8Array()] },
      ]);
      return main
        .query("SELECT k, typeof(v) AS t, v FROM vals ORDER BY k")
        .rows.map((r) => `${r[0]} ${r[1]} ${show(r[2])}\n`)
        .join("");
    }
    case "GET /error":
      main.query("SELEC 1");
      return "parsed\n";
    case "GET /big":
      main.query(
        `WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 3)
                 SELECT zeroblob(20000) AS b FROM n`,
      );
      return "small\n";
    case "POST /many":
      main.batch(Array.from({ length: 11 }, () => ({ sql: "SELECT 1", rows: true })));
      return "ran\n";
    default:
      return undefined;
  }
}

export default defineActor({
  fetch(req): ActorResponse {
    try {
      const out = route(req.method, req.path, req.text(64 * 1024) ?? "");
      return out === undefined ? { status: 404, body: "no such route\n" } : out;
    } catch (e) {
      return { status: 400, body: describe(e) };
    }
  },
});
