// The database class of the db example, in TypeScript: one actor per
// database (`db/main`), its schema applied by `db.migrate` on start,
// serving the SDK's protocol to the app's other actors only (an outside
// client gets 403). The same schema and limits as the Rust class in
// examples/db/database, so either answers either app.
import { db } from "@wasmkeep/sdk";

export default db.define({
  // The schema, in order. Append to it; never edit an applied entry.
  migrations: [
    "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT NOT NULL UNIQUE)",
    "CREATE TABLE vals (k TEXT PRIMARY KEY, v)",
    "CREATE TABLE tags (note INTEGER NOT NULL REFERENCES notes (id) DEFERRABLE INITIALLY DEFERRED, tag TEXT NOT NULL)",
  ],
  limits: {
    maxRequestBytes: 64 * 1024,
    maxStatements: 10,
    maxSqlBytes: 4096,
    maxParams: 8,
    maxResponseBytes: 64 * 1024,
  },
});
