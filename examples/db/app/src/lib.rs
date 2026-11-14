//! The app class of the db example: an HTTP API over the SDK's database
//! client, reaching the database actor `db/main` with `actors.call`. Every
//! SQL statement is written here, with its values bound as parameters:
//! nothing from a request becomes SQL text.
//!
//! - `POST /notes`: stores the body as a note (one statement with
//!   `RETURNING`, committed durably); answers its id.
//! - `GET /notes`: the notes, `<id> <body>` per line, oldest first.
//! - `GET /count`: how many notes there are.
//! - `POST /notes/batch`: stores each line of the body as a note, in one
//!   transaction; answers the rows changed. A repeated line breaks the
//!   `UNIQUE` constraint, and then none of the lines is kept.
//! - `POST /notes/delete-in-read`: runs `DELETE ... RETURNING` as a read;
//!   answers how many rows it returned. Reads are rolled back, so the
//!   notes stay.
//! - `POST /sneaky`: a batch whose second statement is a `COMMIT`: refused
//!   before anything runs.
//! - `POST /orphan`: tags a note that does not exist. The foreign key is
//!   deferred, so the statements run and the commit fails: nothing kept.
//! - `GET /tags`: how many tags there are.
//! - `GET /types`: writes one value of each SQL type, reads them back, and
//!   answers `<key> <type> <value>` per line (text and blobs in hex), so
//!   every SDK's answer can be compared byte for byte.
//! - `GET /error`: a statement that does not parse.
//! - `GET /big`: a result over the database's 64 KiB answer limit.
//! - `POST /many`: eleven statements, over its limit of ten.
//!
//! A failed request answers `400` with `<kind> ...` (`sql <sql kind>
//! <statement>`, `not-allowed <statement>`, `too-large`, `refused
//! <status>` or `call`).

use wasmkeep_sdk::db::{self, Client, Error, Outcome, Statement};
use wasmkeep_sdk::http::{self, IncomingRequest, Method, ResponseOutparam};
use wasmkeep_sdk::{Actor, Rows, Value, params};

struct App;

impl Actor for App {}

fn main_db() -> Client {
    Client::new("db/main")
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn describe(e: &Error) -> String {
    let kind = |k: &wasmkeep_sdk::SqlErrorKind| db::kind_name(*k);
    match e {
        Error::Sql {
            kind: k,
            statement: Some(i),
            ..
        } => format!("sql {} {i}\n", kind(k)),
        Error::Sql { kind: k, .. } => format!("sql {} commit\n", kind(k)),
        Error::NotAllowed { statement, .. } => format!("not-allowed {statement}\n"),
        Error::TooLarge(_) => "too-large\n".into(),
        Error::Refused(status, _) => format!("refused {status}\n"),
        Error::Call(_) => "call\n".into(),
    }
}

fn value(v: &Value) -> String {
    match v {
        Value::Null => "null".into(),
        Value::Integer(i) => i.to_string(),
        Value::Real(r) => r.to_string(),
        Value::Text(t) => hex(t.as_bytes()),
        Value::Blob(b) => hex(b),
    }
}

fn notes(rows: &Rows) -> String {
    rows.values
        .iter()
        .map(|r| format!("{} {}\n", value(&r[0]), r[1].as_str().unwrap_or_default()))
        .collect()
}

fn route(method: &Method, path: &str, body: &[u8]) -> Result<String, Error> {
    let db = main_db();
    let text = String::from_utf8_lossy(body).into_owned();
    Ok(match (method, path) {
        (Method::Post, "/notes") => {
            let insert = Statement::query(
                "INSERT INTO notes (body) VALUES (?1) RETURNING id",
                &params![text],
            );
            match db.batch(&[insert])?.pop() {
                Some(Outcome::Rows(rows)) => format!("{}\n", value(&rows.values[0][0])),
                _ => return Err(Error::Refused(200, "no id".into())),
            }
        }
        (Method::Get, "/notes") => notes(&db.query("SELECT id, body FROM notes ORDER BY id", &[])?),
        (Method::Get, "/count") => {
            let rows = db.query("SELECT COUNT(*) AS n FROM notes", &[])?;
            format!("{}\n", value(&rows.values[0][0]))
        }
        (Method::Post, "/notes/batch") => {
            let inserts: Vec<Statement> = text
                .lines()
                .map(|line| Statement::exec("INSERT INTO notes (body) VALUES (?1)", &params![line]))
                .collect();
            let changed: u64 = db
                .batch(&inserts)?
                .iter()
                .map(|o| match o {
                    Outcome::Changes(n) => *n,
                    Outcome::Rows(_) => 0,
                })
                .sum();
            format!("{changed}\n")
        }
        (Method::Post, "/notes/delete-in-read") => {
            let rows = db.query("DELETE FROM notes RETURNING id", &[])?;
            format!("{}\n", rows.len())
        }
        (Method::Post, "/sneaky") => {
            db.batch(&[
                Statement::exec("INSERT INTO notes (body) VALUES ('sneaky')", &[]),
                Statement::exec("/* done */ commit", &[]),
                Statement::exec("INSERT INTO notes (body) VALUES ('after')", &[]),
            ])?;
            "kept\n".into()
        }
        (Method::Post, "/orphan") => {
            db.batch(&[
                Statement::exec("INSERT INTO notes (body) VALUES ('tagged')", &[]),
                Statement::exec("INSERT INTO tags (note, tag) VALUES (999, 'x')", &[]),
            ])?;
            "kept\n".into()
        }
        (Method::Get, "/tags") => {
            let rows = db.query("SELECT COUNT(*) AS n FROM tags", &[])?;
            format!("{}\n", value(&rows.values[0][0]))
        }
        (Method::Get, "/types") => {
            let put = "INSERT OR REPLACE INTO vals (k, v) VALUES (?1, ?2)";
            db.batch(&[
                Statement::exec(put, &params!["a-max", i64::MAX]),
                Statement::exec(put, &params!["b-min", i64::MIN]),
                Statement::exec(put, &params!["c-real", 2.0]),
                Statement::exec(put, &params!["d-tenth", 0.1]),
                Statement::exec(put, &params!["e-text", "tab\t\"é\" \u{1f600}\u{1}"]),
                Statement::exec(put, &params!["f-blob", vec![0_u8, 255, 16]]),
                Statement::exec(put, &params!["g-null", None::<i64>]),
                Statement::exec(put, &params!["h-empty", Vec::<u8>::new()]),
            ])?;
            let rows = db.query("SELECT k, typeof(v) AS t, v FROM vals ORDER BY k", &[])?;
            rows.values
                .iter()
                .map(|r| {
                    let k = r[0].as_str().unwrap_or_default();
                    let t = r[1].as_str().unwrap_or_default();
                    format!("{k} {t} {}\n", value(&r[2]))
                })
                .collect()
        }
        (Method::Get, "/error") => {
            db.query("SELEC 1", &[])?;
            "parsed\n".into()
        }
        (Method::Get, "/big") => {
            db.query(
                "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 3)
                 SELECT zeroblob(20000) AS b FROM n",
                &[],
            )?;
            "small\n".into()
        }
        (Method::Post, "/many") => {
            let selects: Vec<Statement> =
                (0..11).map(|_| Statement::query("SELECT 1", &[])).collect();
            db.batch(&selects)?;
            "ran\n".into()
        }
        _ => return Err(Error::Refused(404, "no such route".into())),
    })
}

impl http::Guest for App {
    fn handle(request: IncomingRequest, out: ResponseOutparam) {
        let path = request.path_with_query().unwrap_or_default();
        let method = request.method();
        let body = http::read_body(&request, 64 * 1024).unwrap_or_default();
        match route(&method, &path, &body) {
            Ok(text) => http::respond(out, 200, "text/plain", text.as_bytes()),
            Err(Error::Refused(404, _)) => {
                http::respond(out, 404, "text/plain", b"no such route\n")
            }
            Err(e) => http::respond(out, 400, "text/plain", describe(&e).as_bytes()),
        }
    }
}

wasmkeep_sdk::export!(App);
