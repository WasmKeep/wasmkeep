//! A database kept by one actor ([ADR 0029]): the SQL of a whole app's
//! table, run by the actor that owns it, for every other actor of the app.
//!
//! A **database** is an actor whose SQLite database holds the data, like
//! a Cloudflare D1 database: one writer, so every statement sees the
//! latest write. Its class mounts [`handle`] (and applies its schema with
//! [`migrate`]); the app's other actors and its front reach it with a
//! [`Client`] over `actors.call`.
//!
//! ```ignore
//! // The database class (deployed as class `db`, `--access db=internal`):
//! use wasmkeep_sdk::{db, http};
//! struct Db;
//! const MIGRATIONS: &[&str] = &[
//!     "CREATE TABLE todos (id INTEGER PRIMARY KEY, text TEXT NOT NULL, done INTEGER NOT NULL DEFAULT 0)",
//! ];
//! impl wasmkeep_sdk::Actor for Db {
//!     fn on_init() -> Result<(), String> {
//!         db::migrate(MIGRATIONS).map(drop).map_err(|e| e.to_string())
//!     }
//! }
//! impl http::Guest for Db {
//!     fn handle(r: http::IncomingRequest, o: http::ResponseOutparam) { db::handle(r, o) }
//! }
//! wasmkeep_sdk::export!(Db);
//!
//! // Any other actor of the app:
//! let main = db::Client::new("db/main");
//! main.exec("INSERT INTO todos (text) VALUES (?1)", &params!["buy milk"])?;
//! let rows = main.query("SELECT id, text FROM todos WHERE done = ?1", &params![0])?;
//! ```
//!
//! **SQL comes from your code, never from a client.** The handler runs
//! whatever SQL its callers send, and only actors of the app (and its
//! front) can be callers: it refuses (`403`) a request without the
//! `x-wasmkeep-caller` header the host vouches for. Keep it that way:
//! never pass SQL text from an HTTP request to [`Client`], and bind every
//! value as a parameter (`?1`), never by formatting it into the SQL.
//!
//! Limits, stated plainly: one actor runs one request at a time, so the
//! database's throughput is about one durable write per bucket round trip;
//! a request and its answer are capped ([`Limits`]); and it is one
//! database, not a shared multi-writer one: split data into several
//! databases (`db/eu`, `db/tenant-42`) when one is not enough.
//!
//! [ADR 0029]: https://github.com/wasmkeep/wasmkeep/blob/main/docs/adr/0029-a-database-client-as-an-sdk-pattern.md

use std::fmt;

use crate::http::{self, IncomingRequest, Method, ResponseOutparam};
use crate::json::{self, Json};
use crate::{Rows, SqlError, SqlErrorKind, Value, now_ms, storage, wire};

const PREFIX: &str = "/wasmkeep-db/v1";
const READ: &str = "/wasmkeep-db/v1/read";
const WRITE: &str = "/wasmkeep-db/v1/write";

/// One statement of a request: its SQL, its parameters, and whether it
/// returns rows ([`Statement::query`]) or a count of changed rows
/// ([`Statement::exec`]).
#[derive(Debug, Clone, PartialEq)]
pub struct Statement {
    /// One SQL statement, with `?`, `?N` or `:name` placeholders.
    pub sql: String,
    /// The values bound to the placeholders, in order.
    pub params: Vec<Value>,
    /// `true`: run as a query, answering its rows (a `SELECT`, or a write
    /// with `RETURNING`). `false`: run as a write, answering the number of
    /// rows it changed.
    pub rows: bool,
}

impl Statement {
    /// A statement answering its rows: a `SELECT`, or a write with
    /// `RETURNING` (in a write request).
    pub fn query(sql: &str, params: &[Value]) -> Self {
        Self {
            sql: sql.to_owned(),
            params: params.to_vec(),
            rows: true,
        }
    }

    /// A statement answering the number of rows it changed.
    pub fn exec(sql: &str, params: &[Value]) -> Self {
        Self {
            sql: sql.to_owned(),
            params: params.to_vec(),
            rows: false,
        }
    }
}

/// What one statement of a request answered.
#[derive(Debug, Clone, PartialEq)]
pub enum Outcome {
    /// The rows of a [`Statement::query`].
    Rows(Rows),
    /// The rows changed by a [`Statement::exec`].
    Changes(u64),
}

/// Why a database request failed.
#[derive(Debug, Clone, PartialEq)]
pub enum Error {
    /// A statement failed in the database (statement `statement`, from 0;
    /// `None` when the transaction around them failed to begin or commit).
    /// Nothing of the request was kept.
    Sql {
        /// The kind of failure, as the host reports it.
        kind: SqlErrorKind,
        /// Which statement of the request, from 0.
        statement: Option<usize>,
        /// SQLite's message.
        message: String,
    },
    /// A statement tried to control the transaction (`BEGIN`, `COMMIT`,
    /// `END`, `ROLLBACK`, `SAVEPOINT`, `RELEASE`): the handler runs each
    /// request as one transaction of its own, so it refuses them before
    /// running anything.
    NotAllowed {
        /// Which statement of the request, from 0.
        statement: usize,
        /// Why.
        message: String,
    },
    /// The request or its answer is over a [`Limits`] value.
    TooLarge(String),
    /// The call to the database failed, or its commit was kept locally but
    /// could not be made durable: the outcome of a write is then not known
    /// (it may have been committed).
    Call(String),
    /// The database answered something else: another status (`403` for a
    /// refused caller), or a malformed answer (status `200`).
    Refused(u16, String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Sql {
                kind,
                statement: Some(i),
                message,
            } => write!(f, "statement {i}: {} error: {message}", kind_name(*kind)),
            Error::Sql { kind, message, .. } => {
                write!(f, "transaction: {} error: {message}", kind_name(*kind))
            }
            Error::NotAllowed { statement, message } => {
                write!(f, "statement {statement}: {message}")
            }
            Error::TooLarge(m) => write!(f, "too large: {m}"),
            Error::Call(m) => f.write_str(m),
            Error::Refused(s, m) => write!(f, "database answered {s}: {m}"),
        }
    }
}

impl std::error::Error for Error {}

/// The caps a database applies to each request. The host's own limits
/// apply as well: the event's CPU and wall-clock time, each statement's
/// time and result size, and the database's size.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    /// The request body (default 4 MiB). Blobs travel as hex, two bytes
    /// each.
    pub max_request_bytes: usize,
    /// Statements in one request (default 100).
    pub max_statements: usize,
    /// The SQL text of one statement (default 100,000 bytes).
    pub max_sql_bytes: usize,
    /// Parameters of one statement (default 100).
    pub max_params: usize,
    /// The encoded answer (default 8 MiB, under the 16 MB a call carries).
    pub max_response_bytes: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_request_bytes: 4 * 1024 * 1024,
            max_statements: 100,
            max_sql_bytes: 100_000,
            max_params: 100,
            max_response_bytes: 8 * 1024 * 1024,
        }
    }
}

/// Reaches one database actor. Cheap to create.
#[derive(Debug, Clone)]
pub struct Client {
    address: String,
}

impl Client {
    /// The database at `address` (`class/name`, such as `db/main`). If the
    /// name comes from a request (one database per tenant), check it
    /// against the names you serve first.
    pub fn new(address: &str) -> Self {
        Self {
            address: address.to_owned(),
        }
    }

    /// The database's address.
    pub fn address(&self) -> &str {
        &self.address
    }

    /// Runs one read; returns its rows. A read never changes the database:
    /// it runs in a transaction that is rolled back, so a write in it is
    /// undone.
    pub fn query(&self, sql: &str, params: &[Value]) -> Result<Rows, Error> {
        match self.read(&[Statement::query(sql, params)])?.pop() {
            Some(Outcome::Rows(rows)) => Ok(rows),
            _ => Err(bad_reply()),
        }
    }

    /// Runs one write, durably; returns the number of rows it changed.
    pub fn exec(&self, sql: &str, params: &[Value]) -> Result<u64, Error> {
        match self.batch(&[Statement::exec(sql, params)])?.pop() {
            Some(Outcome::Changes(n)) => Ok(n),
            _ => Err(bad_reply()),
        }
    }

    /// Runs `statements` in order in one transaction, committed durably
    /// once all succeed: all of them are kept, or none (the first failure
    /// rolls everything back). One answer per statement.
    pub fn batch(&self, statements: &[Statement]) -> Result<Vec<Outcome>, Error> {
        self.send(WRITE, statements)
    }

    /// Runs reads in order in one transaction, so they see the same data;
    /// it is rolled back, so nothing they do is kept.
    pub fn read(&self, statements: &[Statement]) -> Result<Vec<Outcome>, Error> {
        self.send(READ, statements)
    }

    fn send(&self, path: &str, statements: &[Statement]) -> Result<Vec<Outcome>, Error> {
        let body = encode_request(statements).into_bytes();
        let headers = vec![("content-type".to_owned(), "application/json".to_owned())];
        let response = wire::call(&self.address, "POST", path.to_owned(), headers, body)
            .map_err(Error::Call)?;
        decode_reply(response.status, &response.body, statements)
    }
}

fn bad_reply() -> Error {
    Error::Refused(200, "malformed reply from database".into())
}

/// Reads a database's answer to `statements`.
fn decode_reply(status: u16, body: &[u8], statements: &[Statement]) -> Result<Vec<Outcome>, Error> {
    let parsed = std::str::from_utf8(body).ok().and_then(json::parse);
    if status != 200 {
        let lossy = || String::from_utf8_lossy(body).trim().to_owned();
        return Err(parsed
            .as_ref()
            .and_then(|j| decode_error(status, j))
            .unwrap_or_else(|| Error::Refused(status, lossy())));
    }
    let results = parsed
        .as_ref()
        .filter(|j| j.has_only(&["results"]))
        .and_then(|j| j.get("results")?.as_array())
        .ok_or_else(bad_reply)?;
    if results.len() != statements.len() {
        return Err(bad_reply());
    }
    results
        .iter()
        .zip(statements)
        .map(|(r, s)| decode_outcome(r, s.rows).ok_or_else(bad_reply))
        .collect()
}

fn decode_error(status: u16, j: &Json) -> Option<Error> {
    if !j.has_only(&["error", "kind", "statement", "message"]) {
        return None;
    }
    let message = j.get("message")?.as_str()?.to_owned();
    let statement = match j.get("statement") {
        None => None,
        Some(n) => Some(usize::try_from(n.as_count()?).ok()?),
    };
    Some(match j.get("error")?.as_str()? {
        "sql" => Error::Sql {
            kind: kind_from(j.get("kind")?.as_str()?)?,
            statement,
            message,
        },
        "not-allowed" => Error::NotAllowed {
            statement: statement?,
            message,
        },
        "too-large" => Error::TooLarge(message),
        "unknown-outcome" => Error::Call(message),
        _ => Error::Refused(status, message),
    })
}

fn decode_outcome(j: &Json, rows: bool) -> Option<Outcome> {
    if !rows {
        if !j.has_only(&["changes"]) {
            return None;
        }
        return Some(Outcome::Changes(j.get("changes")?.as_count()?));
    }
    if !j.has_only(&["columns", "rows"]) {
        return None;
    }
    let columns: Vec<String> = j
        .get("columns")?
        .as_array()?
        .iter()
        .map(|c| c.as_str().map(str::to_owned))
        .collect::<Option<_>>()?;
    let values = j
        .get("rows")?
        .as_array()?
        .iter()
        .map(|row| {
            let row = row.as_array()?;
            if row.len() != columns.len() {
                return None;
            }
            row.iter().map(decode_value).collect::<Option<Vec<_>>>()
        })
        .collect::<Option<Vec<_>>>()?;
    Some(Outcome::Rows(Rows { columns, values }))
}

// ----------------------------------------------------------------- the wire

/// A kind's name on the wire (`constraint`, `multiple-statements`, ...).
pub fn kind_name(kind: SqlErrorKind) -> &'static str {
    match kind {
        SqlErrorKind::Syntax => "syntax",
        SqlErrorKind::Constraint => "constraint",
        SqlErrorKind::Bind => "bind",
        SqlErrorKind::MultipleStatements => "multiple-statements",
        SqlErrorKind::QuotaExceeded => "quota-exceeded",
        SqlErrorKind::NotAllowed => "not-allowed",
        SqlErrorKind::Internal => "internal",
    }
}

fn kind_from(s: &str) -> Option<SqlErrorKind> {
    Some(match s {
        "syntax" => SqlErrorKind::Syntax,
        "constraint" => SqlErrorKind::Constraint,
        "bind" => SqlErrorKind::Bind,
        "multiple-statements" => SqlErrorKind::MultipleStatements,
        "quota-exceeded" => SqlErrorKind::QuotaExceeded,
        "not-allowed" => SqlErrorKind::NotAllowed,
        "internal" => SqlErrorKind::Internal,
        _ => return None,
    })
}

/// A value on the wire: `null`, a JSON string for text, and
/// `{"integer":"<decimal>"}`, `{"real":"<number>"}` (as JavaScript writes
/// it) or `{"blob":"<lowercase hex>"}` for the others, so a 64-bit integer,
/// a real that happens to be whole, and bytes all arrive exactly.
fn write_value(out: &mut String, v: &Value) {
    match v {
        Value::Null => out.push_str("null"),
        Value::Text(s) => json::write_str(out, s),
        Value::Integer(i) => {
            out.push_str("{\"integer\":\"");
            out.push_str(&i.to_string());
            out.push_str("\"}");
        }
        Value::Real(r) => {
            out.push_str("{\"real\":\"");
            out.push_str(&json::number_to_string(*r));
            out.push_str("\"}");
        }
        Value::Blob(b) => {
            out.push_str("{\"blob\":\"");
            out.push_str(&wire::hex(b));
            out.push_str("\"}");
        }
    }
}

/// An integer as the wire spells it: `-?(0|[1-9][0-9]*)`, in range.
fn integer_from(s: &str) -> Option<i64> {
    let digits = s.strip_prefix('-').unwrap_or(s);
    let canonical = !digits.is_empty()
        && digits.bytes().all(|b| b.is_ascii_digit())
        && (digits == "0" || !digits.starts_with('0'))
        && s != "-0";
    if canonical { s.parse().ok() } else { None }
}

fn decode_value(j: &Json) -> Option<Value> {
    match j {
        Json::Null => Some(Value::Null),
        Json::String(s) => Some(Value::Text(s.clone())),
        Json::Object(members) if members.len() == 1 => {
            let (tag, v) = &members[0];
            let v = v.as_str()?;
            match tag.as_str() {
                "integer" => integer_from(v).map(Value::Integer),
                "real" => json::number_from_string(v).map(Value::Real),
                "blob" => wire::unhex(v).map(Value::Blob),
                _ => None,
            }
        }
        _ => None,
    }
}

fn encode_request(statements: &[Statement]) -> String {
    let mut out = String::from("{\"statements\":[");
    for (i, s) in statements.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push_str("{\"sql\":");
        json::write_str(&mut out, &s.sql);
        out.push_str(",\"params\":[");
        for (j, p) in s.params.iter().enumerate() {
            if j > 0 {
                out.push(',');
            }
            write_value(&mut out, p);
        }
        out.push_str("],\"rows\":");
        out.push_str(if s.rows { "true" } else { "false" });
        out.push('}');
    }
    out.push_str("]}");
    out
}

/// A refusal of the request as a whole: status, code, message.
#[derive(Debug, PartialEq)]
struct Refusal(u16, &'static str, String);

fn decode_request(body: &[u8], limits: &Limits) -> Result<Vec<Statement>, Refusal> {
    let bad = |m: &str| Refusal(400, "bad-request", m.to_owned());
    let text = std::str::from_utf8(body).map_err(|_| bad("the body is not UTF-8"))?;
    let j = json::parse(text).ok_or_else(|| bad("the body is not JSON"))?;
    if !j.has_only(&["statements"]) {
        return Err(bad("expected {\"statements\": [...]} and nothing else"));
    }
    let items = j
        .get("statements")
        .and_then(Json::as_array)
        .ok_or_else(|| bad("statements must be an array"))?;
    if items.is_empty() {
        return Err(bad("no statements"));
    }
    if items.len() > limits.max_statements {
        return Err(Refusal(
            413,
            "too-large",
            format!(
                "{} statements, more than {}",
                items.len(),
                limits.max_statements
            ),
        ));
    }
    items
        .iter()
        .enumerate()
        .map(|(i, item)| {
            let bad = |m: &str| bad(&format!("statement {i}: {m}"));
            if !item.has_only(&["sql", "params", "rows"]) {
                return Err(bad("expected sql, params and rows only"));
            }
            let sql = item
                .get("sql")
                .and_then(Json::as_str)
                .ok_or_else(|| bad("sql must be a string"))?;
            let rows = match item.get("rows") {
                None | Some(Json::Bool(false)) => false,
                Some(Json::Bool(true)) => true,
                Some(_) => return Err(bad("rows must be true or false")),
            };
            let params = match item.get("params") {
                None => &[][..],
                Some(p) => p.as_array().ok_or_else(|| bad("params must be an array"))?,
            };
            if sql.len() > limits.max_sql_bytes {
                return Err(Refusal(
                    413,
                    "too-large",
                    format!(
                        "statement {i}: SQL longer than {} bytes",
                        limits.max_sql_bytes
                    ),
                ));
            }
            if params.len() > limits.max_params {
                return Err(Refusal(
                    413,
                    "too-large",
                    format!("statement {i}: more than {} parameters", limits.max_params),
                ));
            }
            let params = params
                .iter()
                .map(decode_value)
                .collect::<Option<Vec<_>>>()
                .ok_or_else(|| bad("a parameter is not a wire value"))?;
            Ok(Statement {
                sql: sql.to_owned(),
                params,
                rows,
            })
        })
        .collect()
}

/// Whether `sql`'s first keyword controls a transaction (`BEGIN`,
/// `COMMIT`, `END`, `ROLLBACK`, `SAVEPOINT`, `RELEASE`), skipping
/// whitespace, comments and empty statements (`;`) as SQLite does. The
/// handler refuses those: a `COMMIT` in the middle of a batch would keep
/// the first half of it.
pub fn controls_transaction(sql: &str) -> bool {
    let s = sql.as_bytes();
    let mut at = 0;
    loop {
        match s.get(at) {
            Some(b' ' | b'\t' | b'\n' | b'\x0c' | b'\r' | b';') => at += 1,
            Some(b'-') if s.get(at + 1) == Some(&b'-') => {
                while s.get(at).is_some_and(|b| *b != b'\n') {
                    at += 1;
                }
            }
            Some(b'/') if s.get(at + 1) == Some(&b'*') => {
                at += 2;
                while at < s.len() && !(s[at] == b'*' && s.get(at + 1) == Some(&b'/')) {
                    at += 1;
                }
                at += 2;
            }
            _ => break,
        }
    }
    let start = at.min(s.len());
    let mut end = start;
    // SQLite's identifier characters: letters, digits, `_`, `$`, and any
    // byte from 0x80 (so `COMMITé` is a name, not `COMMIT`).
    while s
        .get(end)
        .is_some_and(|b| b.is_ascii_alphanumeric() || *b == b'_' || *b == b'$' || *b >= 0x80)
    {
        end += 1;
    }
    let word = &s[start..end];
    ["begin", "commit", "end", "rollback", "savepoint", "release"]
        .iter()
        .any(|k| word.eq_ignore_ascii_case(k.as_bytes()))
}

// ---------------------------------------------------------------- the actor

fn error_body(
    code: &str,
    kind: Option<SqlErrorKind>,
    statement: Option<usize>,
    message: &str,
) -> String {
    let mut out = String::from("{\"error\":");
    json::write_str(&mut out, code);
    if let Some(kind) = kind {
        out.push_str(",\"kind\":");
        json::write_str(&mut out, kind_name(kind));
    }
    if let Some(i) = statement {
        out.push_str(",\"statement\":");
        out.push_str(&i.to_string());
    }
    out.push_str(",\"message\":");
    json::write_str(&mut out, message);
    out.push('}');
    out
}

fn refuse(r: &Refusal) -> (u16, String) {
    (r.0, error_body(r.1, None, None, &r.2))
}

fn sql_error(e: &SqlError, statement: Option<usize>) -> (u16, String) {
    let status = if e.kind == SqlErrorKind::Internal {
        500
    } else {
        400
    };
    (
        status,
        error_body("sql", Some(e.kind), statement, &e.message),
    )
}

/// Serves [`Client`]'s protocol with the default [`Limits`]. Requests from
/// outside clients (without `x-wasmkeep-caller`) get `403`: only the app's
/// actors and its front may use the database.
pub fn handle(request: IncomingRequest, out: ResponseOutparam) {
    handle_with(&Limits::default(), request, out, |_| true);
}

/// Like [`handle`], also refusing (`403`) callers for which `allow`
/// returns `false`; it gets the caller's `tenant/class/name` as the host
/// sends it, the name percent-encoded (`room%201`; the front is
/// `tenant/_front/main`).
pub fn handle_allowing(
    request: IncomingRequest,
    out: ResponseOutparam,
    allow: impl Fn(&str) -> bool,
) {
    handle_with(&Limits::default(), request, out, allow);
}

/// Like [`handle_allowing`], with other [`Limits`].
pub fn handle_with(
    limits: &Limits,
    request: IncomingRequest,
    out: ResponseOutparam,
    allow: impl Fn(&str) -> bool,
) {
    let (status, body) = match wire::caller(&request) {
        Some(caller) if allow(&caller) => serve(limits, &request),
        _ => (
            403,
            error_body(
                "forbidden",
                None,
                None,
                "only the app's actors and its front may use this database",
            ),
        ),
    };
    http::respond(out, status, "application/json", body.as_bytes());
}

fn serve(limits: &Limits, request: &IncomingRequest) -> (u16, String) {
    let path = request.path_with_query().unwrap_or_default();
    let write = match path.as_str() {
        READ => false,
        WRITE => true,
        p if p.starts_with(PREFIX) => {
            return refuse(&Refusal(404, "not-found", format!("no such path {p}")));
        }
        _ => return refuse(&Refusal(404, "not-found", "not a database path".into())),
    };
    if !matches!(request.method(), Method::Post) {
        return refuse(&Refusal(405, "method", "use POST".into()));
    }
    let Some(body) = http::read_body(request, limits.max_request_bytes) else {
        return refuse(&Refusal(
            413,
            "too-large",
            format!("request body over {} bytes", limits.max_request_bytes),
        ));
    };
    match decode_request(&body, limits) {
        Ok(statements) => run(limits, write, &statements),
        Err(r) => refuse(&r),
    }
}

/// Runs a request as one transaction: committed durably (a write) or
/// rolled back (a read), and rolled back at the first failure.
fn run(limits: &Limits, write: bool, statements: &[Statement]) -> (u16, String) {
    if let Some(i) = statements.iter().position(|s| controls_transaction(&s.sql)) {
        let message = "transaction control (BEGIN, COMMIT, END, ROLLBACK, SAVEPOINT, RELEASE) is \
                       not allowed: each request is one transaction already";
        return (400, error_body("not-allowed", None, Some(i), message));
    }
    if let Err(e) = storage::exec("BEGIN", &[]) {
        return sql_error(&e, None);
    }
    let rollback = |answer: (u16, String)| {
        // A failure that already ended the transaction makes this a no-op.
        let _ = storage::exec("ROLLBACK", &[]);
        answer
    };
    let mut out = String::from("{\"results\":[");
    for (i, s) in statements.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        if s.rows {
            match storage::query(&s.sql, &s.params) {
                Ok(rows) => {
                    if !write_rows(&mut out, &rows, limits.max_response_bytes) {
                        return rollback(too_large_answer(limits));
                    }
                }
                Err(e) => return rollback(sql_error(&e, Some(i))),
            }
        } else {
            match storage::exec(&s.sql, &s.params) {
                Ok(n) => {
                    out.push_str("{\"changes\":");
                    out.push_str(&n.to_string());
                    out.push('}');
                }
                Err(e) => return rollback(sql_error(&e, Some(i))),
            }
        }
        if out.len() > limits.max_response_bytes {
            return rollback(too_large_answer(limits));
        }
    }
    out.push_str("]}");
    if write {
        if let Err(e) = storage::exec_durable("COMMIT", &[]) {
            // A transaction still open was not committed: undo it. If none
            // is, the commit happened locally and only its durability
            // failed: the write may be kept, so say that, not "nothing".
            return match storage::exec("ROLLBACK", &[]) {
                Ok(_) => sql_error(&e, None),
                Err(_) => (
                    500,
                    error_body(
                        "unknown-outcome",
                        None,
                        None,
                        &format!("the commit may have been kept: {}", e.message),
                    ),
                ),
            };
        }
    } else if let Err(e) = storage::exec("ROLLBACK", &[]) {
        return sql_error(&e, None);
    }
    (200, out)
}

fn too_large_answer(limits: &Limits) -> (u16, String) {
    refuse(&Refusal(
        413,
        "too-large",
        format!("answer over {} bytes", limits.max_response_bytes),
    ))
}

/// Appends `rows` as `{"columns":[...],"rows":[[...]]}`; `false` once
/// `out` passes `max` (the answer would be refused anyway).
fn write_rows(out: &mut String, rows: &Rows, max: usize) -> bool {
    out.push_str("{\"columns\":[");
    for (i, c) in rows.columns.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        json::write_str(out, c);
    }
    out.push_str("],\"rows\":[");
    for (i, row) in rows.values.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push('[');
        for (j, v) in row.iter().enumerate() {
            if j > 0 {
                out.push(',');
            }
            write_value(out, v);
        }
        out.push(']');
        if out.len() > max {
            return false;
        }
    }
    out.push_str("]}");
    true
}

// --------------------------------------------------------------- migrations

/// Applies the schema: `migrations` are statements, in order, each applied
/// once (in its own durable transaction) and recorded in the table
/// `wasmkeep_db_migrations`. Call it from `on_init`; returns how many it
/// applied. Only ever append to the list: a recorded migration whose text
/// changed, or more recorded migrations than listed (an older component),
/// is an error, and nothing is applied.
pub fn migrate(migrations: &[&str]) -> Result<usize, Error> {
    let fail = |statement: Option<usize>| {
        move |e: SqlError| Error::Sql {
            kind: e.kind,
            statement,
            message: e.message,
        }
    };
    storage::exec(
        "CREATE TABLE IF NOT EXISTS wasmkeep_db_migrations (
            version INTEGER PRIMARY KEY,
            sql TEXT NOT NULL,
            applied_at INTEGER NOT NULL
        )",
        &[],
    )
    .map_err(fail(None))?;
    let applied = storage::query(
        "SELECT sql FROM wasmkeep_db_migrations ORDER BY version",
        &[],
    )
    .map_err(fail(None))?;
    let mismatch = |message: String| Error::Sql {
        kind: SqlErrorKind::Constraint,
        statement: None,
        message,
    };
    if applied.len() > migrations.len() {
        return Err(mismatch(format!(
            "the database has {} migrations applied, this component lists {}",
            applied.len(),
            migrations.len()
        )));
    }
    for (i, row) in applied.values.iter().enumerate() {
        if row.first().and_then(Value::as_str) != Some(migrations[i]) {
            return Err(mismatch(format!(
                "migration {i} differs from the one applied: append new migrations instead of editing old ones"
            )));
        }
    }
    let now = i64::try_from(now_ms()).unwrap_or(i64::MAX);
    for (i, sql) in migrations.iter().enumerate().skip(applied.len()) {
        if controls_transaction(sql) {
            return Err(Error::NotAllowed {
                statement: i,
                message: "a migration may not control the transaction".into(),
            });
        }
        storage::exec("BEGIN", &[]).map_err(fail(Some(i)))?;
        let version = i64::try_from(i).unwrap_or(i64::MAX);
        let step = storage::exec(sql, &[])
            .and_then(|_| {
                storage::exec(
                    "INSERT INTO wasmkeep_db_migrations (version, sql, applied_at) VALUES (?1, ?2, ?3)",
                    &[Value::Integer(version), Value::Text((*sql).to_owned()), Value::Integer(now)],
                )
            })
            .and_then(|_| storage::exec_durable("COMMIT", &[]));
        if let Err(e) = step {
            let _ = storage::exec("ROLLBACK", &[]);
            return Err(fail(Some(i))(e));
        }
    }
    Ok(migrations.len() - applied.len())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The same request, byte for byte, as `sdks/typescript/test/db.test.ts`
    /// expects from the TypeScript client.
    pub(super) const REQUEST: &str = concat!(
        r#"{"statements":[{"sql":"INSERT INTO t (a, b, c, d) VALUES (?1, ?2, ?3, ?4) RETURNING id","#,
        r#""params":[{"integer":"-9223372036854775808"},{"real":"1.5e+21"},"tab\there \"é\" 😀",{"blob":"00ff10"}],"rows":true},"#,
        r#"{"sql":"DELETE FROM t WHERE a = ?1","params":[null],"rows":false}]}"#
    );

    /// A database's answer to [`REQUEST`], as both SDKs write it.
    pub(super) const RESPONSE: &str = concat!(
        r#"{"results":[{"columns":["id","x"],"rows":[[{"integer":"9223372036854775807"},{"real":"2"}],"#,
        r#"[{"integer":"9007199254740993"},{"real":"-0"}],[null,"\u0001"]]},{"changes":3}]}"#
    );

    fn statements() -> Vec<Statement> {
        vec![
            Statement::query(
                "INSERT INTO t (a, b, c, d) VALUES (?1, ?2, ?3, ?4) RETURNING id",
                &[
                    Value::Integer(i64::MIN),
                    Value::Real(1.5e21),
                    Value::Text("tab\there \"é\" 😀".into()),
                    Value::Blob(vec![0, 255, 16]),
                ],
            ),
            Statement::exec("DELETE FROM t WHERE a = ?1", &[Value::Null]),
        ]
    }

    #[test]
    fn requests_are_written_byte_for_byte_and_read_back() {
        assert_eq!(encode_request(&statements()), REQUEST);
        assert_eq!(
            decode_request(REQUEST.as_bytes(), &Limits::default()),
            Ok(statements())
        );
    }

    #[test]
    fn answers_are_read() {
        let outcomes = decode_reply(200, RESPONSE.as_bytes(), &statements()).unwrap();
        assert_eq!(
            outcomes,
            vec![
                Outcome::Rows(Rows {
                    columns: vec!["id".into(), "x".into()],
                    values: vec![
                        vec![Value::Integer(i64::MAX), Value::Real(2.0)],
                        vec![Value::Integer(9_007_199_254_740_993), Value::Real(-0.0)],
                        vec![Value::Null, Value::Text("\u{1}".into())],
                    ],
                }),
                Outcome::Changes(3),
            ]
        );
        let Outcome::Rows(rows) = &outcomes[0] else {
            unreachable!()
        };
        let mut written = String::from("{\"results\":[");
        assert!(write_rows(&mut written, rows, usize::MAX));
        written.push_str(",{\"changes\":3}]}");
        assert_eq!(written, RESPONSE, "and written back the same");

        // An answer that does not match the request is malformed.
        assert_eq!(
            decode_reply(200, RESPONSE.as_bytes(), &statements()[..1]),
            Err(bad_reply())
        );
        let swapped = [statements()[1].clone(), statements()[0].clone()];
        assert_eq!(
            decode_reply(200, RESPONSE.as_bytes(), &swapped),
            Err(bad_reply())
        );
        assert_eq!(
            decode_reply(200, b"{\"results\":[], \"x\":1}", &[]),
            Err(bad_reply())
        );
    }

    #[test]
    fn errors_are_read() {
        let sql = error_body(
            "sql",
            Some(SqlErrorKind::Constraint),
            Some(1),
            "UNIQUE constraint failed: t.a",
        );
        assert_eq!(
            sql,
            r#"{"error":"sql","kind":"constraint","statement":1,"message":"UNIQUE constraint failed: t.a"}"#
        );
        assert_eq!(
            decode_reply(400, sql.as_bytes(), &[]),
            Err(Error::Sql {
                kind: SqlErrorKind::Constraint,
                statement: Some(1),
                message: "UNIQUE constraint failed: t.a".into()
            })
        );
        let commit = error_body(
            "sql",
            Some(SqlErrorKind::Constraint),
            None,
            "FOREIGN KEY constraint failed",
        );
        assert!(matches!(
            decode_reply(400, commit.as_bytes(), &[]),
            Err(Error::Sql {
                statement: None,
                ..
            })
        ));
        let no = error_body("not-allowed", None, Some(0), "no");
        assert_eq!(
            decode_reply(400, no.as_bytes(), &[]),
            Err(Error::NotAllowed {
                statement: 0,
                message: "no".into()
            })
        );
        let unknown = error_body("unknown-outcome", None, None, "may be kept");
        assert_eq!(
            decode_reply(500, unknown.as_bytes(), &[]),
            Err(Error::Call("may be kept".into()))
        );
        let big = error_body("too-large", None, None, "big");
        assert_eq!(
            decode_reply(413, big.as_bytes(), &[]),
            Err(Error::TooLarge("big".into()))
        );
        let forbidden = error_body("forbidden", None, None, "outside");
        assert_eq!(
            decode_reply(403, forbidden.as_bytes(), &[]),
            Err(Error::Refused(403, "outside".into()))
        );
        assert_eq!(
            decode_reply(502, b" plain text\n", &[]),
            Err(Error::Refused(502, "plain text".into()))
        );
    }

    #[test]
    fn values_are_exact() {
        for bad in [
            r#"{"integer":"01"}"#,
            r#"{"integer":"-0"}"#,
            r#"{"integer":"+1"}"#,
            r#"{"integer":" 1"}"#,
            r#"{"integer":"9223372036854775808"}"#,
            r#"{"integer":1}"#,
            r#"{"real":"inf"}"#,
            r#"{"real":1.5}"#,
            r#"{"blob":"abc"}"#,
            r#"{"blob":"zz"}"#,
            r#"{"text":"a"}"#,
            r#"{"integer":"1","real":"1"}"#,
            "1",
            "true",
            "[]",
        ] {
            assert_eq!(decode_value(&json::parse(bad).unwrap()), None, "{bad}");
        }
        let mut s = String::new();
        write_value(&mut s, &Value::Real(f64::NAN));
        assert_eq!(s, r#"{"real":"NaN"}"#);
    }

    #[test]
    fn requests_are_checked_against_the_limits() {
        let limits = Limits {
            max_statements: 1,
            max_sql_bytes: 10,
            max_params: 1,
            ..Limits::default()
        };
        let req = |s: &[Statement]| encode_request(s).into_bytes();
        let one = Statement::exec("SELECT 1", &[]);
        assert!(decode_request(&req(std::slice::from_ref(&one)), &limits).is_ok());
        let code = |r: Result<Vec<Statement>, Refusal>| r.map_err(|Refusal(s, c, _)| (s, c));
        assert_eq!(
            code(decode_request(&req(&[one.clone(), one.clone()]), &limits)),
            Err((413, "too-large"))
        );
        let long = Statement::exec("SELECT 12345", &[]);
        assert_eq!(
            code(decode_request(&req(&[long]), &limits)),
            Err((413, "too-large"))
        );
        let two = Statement::exec("SELECT ?", &[Value::Null, Value::Null]);
        assert_eq!(
            code(decode_request(&req(&[two]), &limits)),
            Err((413, "too-large"))
        );
        for bad in [
            &b"\xff"[..],
            b"",
            b"[]",
            b"{\"statements\":[]}",
            b"{\"statements\":{}}",
            b"{\"statements\":[{\"sql\":1}]}",
            b"{\"statements\":[{\"sql\":\"x\",\"rows\":1}]}",
            b"{\"statements\":[{\"sql\":\"x\",\"params\":[1]}]}",
            b"{\"statements\":[{\"sql\":\"x\",\"extra\":1}]}",
            b"{\"statements\":[{\"sql\":\"x\"}],\"mode\":\"write\"}",
        ] {
            assert_eq!(
                code(decode_request(bad, &Limits::default())),
                Err((400, "bad-request")),
                "{bad:?}"
            );
        }
        // `params` and `rows` may be left out.
        assert_eq!(
            decode_request(b"{\"statements\":[{\"sql\":\"x\"}]}", &Limits::default()),
            Ok(vec![Statement::exec("x", &[])])
        );
    }

    #[test]
    fn transaction_control_is_spotted_however_it_is_written() {
        for sql in [
            "COMMIT",
            "commit",
            "  End transaction",
            "BEGIN IMMEDIATE",
            "ROLLBACK TO sp",
            "savepoint sp",
            "RELEASE sp",
            "/**/commit",
            "/* a */ -- b\n COMMIT",
            ";COMMIT",
            " ; ;\n\tcommit;",
            "COMMIT/* x */",
            "\x0cBEGIN",
        ] {
            assert!(controls_transaction(sql), "{sql:?}");
        }
        for sql in [
            "SELECT 1",
            "INSERT INTO begin_log VALUES (1)",
            "commits",
            "COMMITé",
            "\"COMMIT\"",
            "-- COMMIT",
            "/* COMMIT */ SELECT 1",
            "/* unterminated COMMIT",
            "",
            "EXPLAIN COMMIT",
            "WITH x AS (SELECT 1) SELECT * FROM x",
            "COMM\u{130}T",
        ] {
            assert!(!controls_transaction(sql), "{sql:?}");
        }
    }
}
