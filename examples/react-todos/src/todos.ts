// Class `todos`: each actor (`todos/main`, `todos/groceries`, ...) is one
// shared list, in its own SQLite database. The routes and the answers are
// the contract in ./contract.ts; failures use the error envelope, so the
// page can tell "no such item" from "the server is down".
import { defineActor, json, storage } from "@wasmkeep/sdk";
import { problem } from "@wasmkeep/sdk/contracts";
import { MAX_TEXT } from "./contract";

type Row = ReturnType<typeof storage.query>[number];

const item = (row: Row) => ({ id: Number(row.id), text: String(row.text), done: Number(row.done) !== 0 });
const notFound = () => problem(404, "todos.not_found", "No such item");

export default defineActor({
  init() {
    storage.exec(
      "CREATE TABLE IF NOT EXISTS todos (id INTEGER PRIMARY KEY AUTOINCREMENT, text TEXT NOT NULL, done INTEGER NOT NULL DEFAULT 0)",
    );
  },

  fetch(req) {
    const path = req.path.split("?")[0];
    const method = req.method;
    if (path === "/" && method === "GET") {
      const rows = storage.query("SELECT id, text, done FROM todos ORDER BY id");
      return json({ todos: rows.map(item) });
    }
    if (path === "/" && method === "POST") {
      let text: unknown;
      try {
        text = (req.json<{ text?: unknown }>() ?? {}).text;
      } catch {
        return problem(400, "todos.invalid_text", "The body must be JSON", { max: MAX_TEXT });
      }
      const trimmed = typeof text === "string" ? text.trim() : "";
      if (trimmed === "" || [...trimmed].length > MAX_TEXT) {
        return problem(400, "todos.invalid_text", `Write 1 to ${MAX_TEXT} characters`, { max: MAX_TEXT });
      }
      const [row] = storage.query("INSERT INTO todos (text) VALUES (?1) RETURNING id, text, done", trimmed);
      return json(item(row), 201);
    }
    const toggle = /^\/([0-9]{1,15})\/toggle$/.exec(path);
    if (toggle && method === "POST") {
      const [row] = storage.query("UPDATE todos SET done = 1 - done WHERE id = ?1 RETURNING id, text, done", Number(toggle[1]));
      return row ? json(item(row)) : notFound();
    }
    const one = /^\/([0-9]{1,15})$/.exec(path);
    if (one && method === "DELETE") {
      const rows = storage.query("DELETE FROM todos WHERE id = ?1 RETURNING id", Number(one[1]));
      return rows.length > 0 ? { status: 204 } : notFound();
    }
    return { status: 404, body: "not found\n" };
  },
});
