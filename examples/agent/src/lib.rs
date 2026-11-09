//! An AI agent: one actor per conversation (`/actor/agent/<conversation>`).
//!
//! - `POST /message` with a text body: the message joins the history, the
//!   model answers (calling a tool first if it wants), and the reply is the
//!   response body.
//! - `GET /history`: the conversation, as a JSON array.
//! - A WebSocket on the actor receives every new entry (the user's
//!   messages, tool calls and results, replies, reminders) as one JSON object
//!   per message. Text sent on the socket is handled like `POST /message`.
//!
//! The model is any OpenAI-compatible chat-completions endpoint (a hosted
//! API, or Ollama, vLLM or a gateway on your network), configured per class:
//! `MODEL_URL`, `MODEL`, and `API_KEY` if the endpoint needs one. The key is
//! sent to the endpoint and nowhere else: it is never stored in the history,
//! and is removed from any error text before that text is returned.
//!
//! The model has one tool, `remind_me`: "remind me in N minutes". It is an
//! alarm. The actor can hibernate meanwhile; when the alarm fires, the
//! reminder joins the history and reaches the open sockets.
//!
//! An event has 30 s of wall-clock time. A turn is at most two model calls
//! (one with a tool round), within a 25 s budget, and every step is stored
//! before the next call, so a cut turn leaves a history the next message
//! continues from.

use std::time::Duration;

use serde_json::{Value as Json, json};
use wasmkeep_sdk::http::{self, IncomingRequest, Method, ResponseOutparam};
use wasmkeep_sdk::{Actor, Message, Value, alarms, env, now_ms, params, storage, websockets};

/// Wall-clock budget for one turn, under the host's 30 s per event.
const TURN_BUDGET_MS: i64 = 25_000;
/// A model call is not started with less time than this left.
const MIN_CALL_MS: i64 = 2_000;
/// Model calls per turn: one, plus one after a tool round.
const MAX_CALLS_PER_TURN: usize = 2;
/// Messages sent to the model (the most recent ones).
const CONTEXT: i64 = 40;
/// Entries returned by `GET /history`.
const HISTORY: i64 = 500;
/// Largest model response read.
const RESPONSE_LIMIT: usize = 1024 * 1024;
/// Largest user message accepted.
const MESSAGE_LIMIT: usize = 16 * 1024;
/// Default for `MAX_CALLS_PER_HOUR`.
const DEFAULT_CALLS_PER_HOUR: i64 = 60;
/// Longest reminder: a week.
const MAX_REMINDER_MINUTES: f64 = 7.0 * 24.0 * 60.0;

const HOUR_MS: i64 = 3_600_000;

struct Agent;

/// The class configuration, read at each event (it may change between
/// events).
struct Config {
    url: String,
    model: String,
    key: Option<String>,
    system: Option<String>,
    calls_per_hour: i64,
}

impl Config {
    fn load() -> Result<Self, Failure> {
        let url = env::get("MODEL_URL").filter(|v| !v.is_empty());
        let model = env::get("MODEL").filter(|v| !v.is_empty());
        let (Some(url), Some(model)) = (url, model) else {
            return Err(Failure::new(
                503,
                "the agent is not configured: set MODEL_URL and MODEL",
            ));
        };
        // A base URL (".../v1") or the full endpoint.
        let url = if url.trim_end_matches('/').ends_with("/chat/completions") {
            url
        } else {
            format!("{}/chat/completions", url.trim_end_matches('/'))
        };
        Ok(Self {
            url,
            model,
            key: env::get("API_KEY").filter(|v| !v.is_empty()),
            system: env::get("SYSTEM_PROMPT").filter(|v| !v.is_empty()),
            calls_per_hour: env::get("MAX_CALLS_PER_HOUR")
                .and_then(|v| v.trim().parse().ok())
                .unwrap_or(DEFAULT_CALLS_PER_HOUR),
        })
    }

    /// `text` with the API key replaced: for anything derived from a
    /// response (an endpoint may echo the key in an error).
    fn redact(&self, text: &str) -> String {
        match &self.key {
            Some(key) => text.replace(key.as_str(), "[API_KEY]"),
            None => text.to_owned(),
        }
    }
}

/// Why a turn ended without a reply: an HTTP status and a message.
struct Failure {
    status: u16,
    message: String,
}

impl Failure {
    fn new(status: u16, message: impl Into<String>) -> Self {
        Self {
            status,
            message: message.into(),
        }
    }
}

impl From<storage::SqlError> for Failure {
    fn from(e: storage::SqlError) -> Self {
        Self::new(500, format!("storage: {}", e.message))
    }
}

fn now() -> i64 {
    i64::try_from(now_ms()).unwrap_or(i64::MAX)
}

// History -------------------------------------------------------------------

/// Stores one message (an OpenAI chat message object) and returns its entry
/// for `/history` and the sockets. `kind` tells clients what it is: `user`,
/// `assistant`, `tool`, or `reminder`.
fn append(kind: &str, message: &Json) -> Result<Json, Failure> {
    let at = now();
    let text = message.to_string();
    storage::exec(
        "INSERT INTO messages (at, kind, json) VALUES (?1, ?2, ?3)",
        &params![at, kind, text.as_str()],
    )?;
    let rows = storage::query("SELECT last_insert_rowid() AS id", &[])?;
    let id = rows.get(0, "id").and_then(Value::as_i64).unwrap_or(0);
    Ok(entry(id, at, kind, message.clone()))
}

fn entry(id: i64, at: i64, kind: &str, message: Json) -> Json {
    let mut entry = json!({ "id": id, "at": at, "kind": kind });
    if let (Some(e), Json::Object(m)) = (entry.as_object_mut(), message) {
        e.extend(m);
    }
    entry
}

/// The most recent `limit` entries, oldest first: `(kind, message)` with
/// the entry's id and time.
fn recent(limit: i64) -> Vec<(i64, i64, String, Json)> {
    let Ok(rows) = storage::query(
        "SELECT id, at, kind, json FROM
           (SELECT id, at, kind, json FROM messages ORDER BY id DESC LIMIT ?1)
         ORDER BY id",
        &params![limit],
    ) else {
        return Vec::new();
    };
    (0..rows.len())
        .filter_map(|i| {
            let id = rows.get(i, "id").and_then(Value::as_i64)?;
            let at = rows.get(i, "at").and_then(Value::as_i64)?;
            let kind = rows.get(i, "kind").and_then(Value::as_str)?.to_owned();
            let message = serde_json::from_str(rows.get(i, "json")?.as_str()?).ok()?;
            Some((id, at, kind, message))
        })
        .collect()
}

fn broadcast(entry: &Json) {
    let line = entry.to_string();
    for conn in websockets::connections() {
        // A slow reader drops this entry; it is still in the history.
        let _ = websockets::send(conn, &Message::Text(line.clone()));
    }
}

/// What the model sees: the system prompt, then the recent history. A cut
/// at the window's start must not begin with tool results whose call fell
/// outside it, which endpoints refuse.
fn context(cfg: &Config) -> Vec<Json> {
    let mut messages: Vec<Json> = cfg
        .system
        .iter()
        .map(|s| json!({ "role": "system", "content": s }))
        .collect();
    let history = recent(CONTEXT);
    let start = history
        .iter()
        .position(|(_, _, _, m)| m["role"] != "tool")
        .unwrap_or(history.len());
    messages.extend(history.into_iter().skip(start).map(|(_, _, _, m)| m));
    messages
}

// The model -----------------------------------------------------------------

fn tools() -> Json {
    json!([{
        "type": "function",
        "function": {
            "name": "remind_me",
            "description": "Schedule a reminder for the user. When it is due, the text is posted in this conversation.",
            "parameters": {
                "type": "object",
                "properties": {
                    "minutes": {
                        "type": "number",
                        "description": "Minutes from now (fractions allowed), at most one week."
                    },
                    "text": {
                        "type": "string",
                        "description": "What to remind the user of."
                    }
                },
                "required": ["minutes", "text"]
            }
        }
    }])
}

/// One chat-completions call: the reply message, as an assistant message
/// ready to store (tool-call arguments as JSON strings, every call with an
/// id).
fn call_model(cfg: &Config, timeout_ms: i64) -> Result<Json, Failure> {
    let body = json!({
        "model": cfg.model,
        "messages": context(cfg),
        "tools": tools(),
        "stream": false,
    })
    .to_string();
    let bearer = cfg.key.as_ref().map(|k| format!("Bearer {k}"));
    let mut headers = vec![("content-type", "application/json")];
    if let Some(b) = &bearer {
        headers.push(("authorization", b.as_str()));
    }
    let timeout = Duration::from_millis(u64::try_from(timeout_ms).unwrap_or(0));
    let response = http::fetch(
        "POST",
        &cfg.url,
        &headers,
        body.as_bytes(),
        timeout,
        RESPONSE_LIMIT,
    )
    .map_err(|e| Failure::new(502, cfg.redact(&format!("calling the model: {e}"))))?;
    let text = String::from_utf8_lossy(&response.body);
    if !(200..300).contains(&response.status) {
        let detail: String = text.chars().take(300).collect();
        return Err(Failure::new(
            502,
            cfg.redact(&format!(
                "the model endpoint answered {}: {detail}",
                response.status
            )),
        ));
    }
    let parsed: Json = serde_json::from_str(&text)
        .map_err(|e| Failure::new(502, format!("the model's answer is not JSON: {e}")))?;
    let message = &parsed["choices"][0]["message"];
    if !message.is_object() {
        return Err(Failure::new(502, "the model's answer has no message"));
    }
    let content = message["content"]
        .as_str()
        .map(|c| Json::String(cfg.redact(c)))
        .unwrap_or(Json::Null);
    let mut reply = json!({ "role": "assistant", "content": content });
    let calls: Vec<Json> = message["tool_calls"]
        .as_array()
        .into_iter()
        .flatten()
        .enumerate()
        .map(|(i, call)| {
            let id = call["id"]
                .as_str()
                .map_or_else(|| format!("call_{}_{i}", now()), str::to_owned);
            // Arguments are a JSON string in the API; some servers send an
            // object.
            let arguments = match &call["function"]["arguments"] {
                Json::String(s) => s.clone(),
                Json::Null => "{}".to_owned(),
                other => other.to_string(),
            };
            json!({
                "id": id,
                "type": "function",
                "function": { "name": call["function"]["name"], "arguments": arguments },
            })
        })
        .collect();
    if !calls.is_empty() {
        reply["tool_calls"] = Json::Array(calls);
    }
    Ok(reply)
}

/// Counts a model call against the hourly cap, or refuses it.
fn count_call(cfg: &Config) -> Result<(), Failure> {
    let at = now();
    storage::exec("DELETE FROM calls WHERE at <= ?1", &params![at - HOUR_MS])?;
    let rows = storage::query("SELECT count(*) AS n FROM calls", &[])?;
    let made = rows.get(0, "n").and_then(Value::as_i64).unwrap_or(0);
    if made >= cfg.calls_per_hour {
        return Err(Failure::new(
            429,
            format!(
                "this conversation reached its {} model calls this hour; try again later",
                cfg.calls_per_hour
            ),
        ));
    }
    storage::exec("INSERT INTO calls (at) VALUES (?1)", &params![at])?;
    Ok(())
}

// Tools ---------------------------------------------------------------------

/// Runs one tool call; the result is text for the model.
fn run_tool(name: &str, arguments: &str) -> Result<String, Failure> {
    let args: Json = serde_json::from_str(arguments).unwrap_or(Json::Null);
    match name {
        "remind_me" => {
            let minutes = match &args["minutes"] {
                Json::Number(n) => n.as_f64(),
                Json::String(s) => s.trim().parse().ok(),
                _ => None,
            };
            let text = args["text"].as_str().map(str::trim).unwrap_or_default();
            let Some(minutes) =
                minutes.filter(|m| m.is_finite() && *m >= 0.0 && *m <= MAX_REMINDER_MINUTES)
            else {
                return Ok("error: minutes must be a number from 0 to 10080".into());
            };
            if text.is_empty() {
                return Ok("error: text is required".into());
            }
            #[allow(clippy::cast_possible_truncation, reason = "bounded to a week")]
            let due = now() + (minutes * 60_000.0).round() as i64;
            storage::exec(
                "INSERT INTO reminders (due, text) VALUES (?1, ?2)",
                &params![due, text],
            )?;
            Ok(format!("reminder set for {minutes} minutes from now"))
        }
        other => Ok(format!("error: no tool named {other}")),
    }
}

/// Points the alarm at the next undelivered reminder, or clears it.
fn arm() -> Result<(), Failure> {
    let rows = storage::query(
        "SELECT min(due) AS due FROM reminders WHERE delivered = 0",
        &[],
    )?;
    match rows
        .get(0, "due")
        .and_then(Value::as_i64)
        .and_then(|d| u64::try_from(d).ok())
    {
        Some(due) => alarms::set_alarm(due),
        None => alarms::clear_alarm(),
    }
    Ok(())
}

/// The assistant message with tool calls, then each call's result, stored
/// together: a call without its result would make the history invalid for
/// the next model call.
fn tool_round(reply: &Json) -> Result<Vec<Json>, Failure> {
    storage::exec("BEGIN", &[])?;
    let result = (|| {
        let mut entries = vec![append("assistant", reply)?];
        for call in reply["tool_calls"].as_array().into_iter().flatten() {
            let output = run_tool(
                call["function"]["name"].as_str().unwrap_or_default(),
                call["function"]["arguments"].as_str().unwrap_or("{}"),
            )?;
            let message = json!({
                "role": "tool",
                "tool_call_id": call["id"],
                "content": output,
            });
            entries.push(append("tool", &message)?);
        }
        Ok::<_, Failure>(entries)
    })();
    match result {
        Ok(entries) => {
            storage::exec("COMMIT", &[])?;
            arm()?;
            Ok(entries)
        }
        Err(e) => {
            let _ = storage::exec("ROLLBACK", &[]);
            Err(e)
        }
    }
}

// A turn --------------------------------------------------------------------

/// A user message in, the model's reply out. Everything is stored and
/// broadcast as it happens.
fn turn(text: &str) -> Result<String, Failure> {
    let started = now();
    let cfg = Config::load()?;
    // Over the cap, the message is refused before it joins the history.
    count_call(&cfg)?;
    broadcast(&append(
        "user",
        &json!({ "role": "user", "content": text }),
    )?);
    for call in 0..MAX_CALLS_PER_TURN {
        let left = TURN_BUDGET_MS - (now() - started);
        if left < MIN_CALL_MS {
            break;
        }
        if call > 0 {
            count_call(&cfg)?;
        }
        let reply = call_model(&cfg, left)?;
        if reply.get("tool_calls").is_none() {
            let entry = append("assistant", &reply)?;
            broadcast(&entry);
            return Ok(reply["content"].as_str().unwrap_or_default().to_owned());
        }
        for entry in tool_round(&reply)? {
            broadcast(&entry);
        }
    }
    // Out of calls or time after a tool round: the results are stored, and
    // the next message continues from them.
    Err(Failure::new(
        504,
        "the model did not finish within this turn; the history is kept, send another message to continue",
    ))
}

impl Actor for Agent {
    fn on_init() -> Result<(), String> {
        for sql in [
            "CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY,
                at INTEGER NOT NULL,
                kind TEXT NOT NULL,
                json TEXT NOT NULL
            )",
            "CREATE TABLE IF NOT EXISTS reminders (
                id INTEGER PRIMARY KEY,
                due INTEGER NOT NULL,
                text TEXT NOT NULL,
                delivered INTEGER NOT NULL DEFAULT 0
            )",
            "CREATE INDEX IF NOT EXISTS reminders_due ON reminders (delivered, due)",
            "CREATE TABLE IF NOT EXISTS calls (at INTEGER NOT NULL)",
        ] {
            storage::exec(sql, &[]).map_err(|e| e.message)?;
        }
        Ok(())
    }

    /// Delivers every due reminder. Delivered at least once: a retry after
    /// the commit finds nothing left to deliver.
    fn on_alarm() -> Result<(), String> {
        let deliver = || -> Result<Vec<Json>, Failure> {
            storage::exec("BEGIN", &[])?;
            let due = storage::query(
                "SELECT id, text FROM reminders WHERE delivered = 0 AND due <= ?1 ORDER BY due, id",
                &params![now()],
            )?;
            let mut entries = Vec::new();
            for i in 0..due.len() {
                let id = due.get(i, "id").and_then(Value::as_i64).unwrap_or(0);
                let text = due.get(i, "text").and_then(Value::as_str).unwrap_or("");
                let message =
                    json!({ "role": "assistant", "content": format!("Reminder: {text}") });
                entries.push(append("reminder", &message)?);
                storage::exec(
                    "UPDATE reminders SET delivered = 1 WHERE id = ?1",
                    &params![id],
                )?;
            }
            storage::exec("COMMIT", &[])?;
            Ok(entries)
        };
        let entries = deliver().map_err(|e| e.message)?;
        for entry in &entries {
            broadcast(entry);
        }
        arm().map_err(|e| e.message)
    }

    fn on_websocket_open(_conn: u64, _path: String) -> Result<(), String> {
        Ok(())
    }

    fn on_websocket_message(conn: u64, msg: Message) {
        let Message::Text(text) = msg else {
            return;
        };
        let text = text.trim();
        if text.is_empty() || text.len() > MESSAGE_LIMIT {
            return;
        }
        // The reply reaches every socket through the broadcast; a failure
        // goes to the sender only.
        if let Err(e) = turn(text) {
            let error = json!({ "kind": "error", "status": e.status, "error": e.message });
            let _ = websockets::send(conn, &Message::Text(error.to_string()));
        }
    }
}

impl http::Guest for Agent {
    fn handle(request: IncomingRequest, out: ResponseOutparam) {
        let path = request.path_with_query().unwrap_or_default();
        let path = path.split('?').next().unwrap_or("/").to_owned();
        match (request.method(), path.as_str()) {
            (Method::Post, "/message") => {
                let text = http::read_body(&request, MESSAGE_LIMIT)
                    .and_then(|b| String::from_utf8(b).ok())
                    .map(|t| t.trim().to_owned())
                    .filter(|t| !t.is_empty());
                let Some(text) = text else {
                    return http::respond(
                        out,
                        400,
                        "text/plain",
                        b"send a UTF-8 text body (at most 16 KB)\n",
                    );
                };
                match turn(&text) {
                    Ok(reply) => {
                        http::respond(out, 200, "text/plain", format!("{reply}\n").as_bytes());
                    }
                    Err(e) => http::respond(
                        out,
                        e.status,
                        "text/plain",
                        format!("{}\n", e.message).as_bytes(),
                    ),
                }
            }
            (Method::Get, "/history") => {
                let entries: Vec<Json> = recent(HISTORY)
                    .into_iter()
                    .map(|(id, at, kind, m)| entry(id, at, &kind, m))
                    .collect();
                http::respond(
                    out,
                    200,
                    "application/json",
                    Json::Array(entries).to_string().as_bytes(),
                );
            }
            _ => http::respond(
                out,
                404,
                "text/plain",
                b"POST /message (text), GET /history, or a WebSocket\n",
            ),
        }
    }
}

wasmkeep_sdk::export!(Agent);
