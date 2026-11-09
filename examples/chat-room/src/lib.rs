//! A chat room. Each actor name is one room.
//!
//! - Connect a WebSocket to `/actor/<class>/<room>`: you receive the last 20
//!   messages, then everything said in the room. Text you send is broadcast
//!   to everyone in the room, you included. If the operator set `MOTD`
//!   (`wasmkeep serve --env chat=FILE`), it comes first, as `* <MOTD>`.
//! - `POST /` with a text body says something over plain HTTP.
//! - `GET /` lists the recent messages.
//!
//! Messages are kept in the room's SQLite database (the last 100). The host
//! owns the sockets, so the room can hibernate while people stay connected:
//! the next message wakes it.

use wasmkeep_sdk::http::{self, IncomingRequest, Method, ResponseOutparam};
use wasmkeep_sdk::{Actor, Message, Value, actors, env, now_ms, params, storage, websockets};

const HISTORY: i64 = 20;
const KEEP: i64 = 100;

struct ChatRoom;

fn recent(limit: i64) -> Vec<String> {
    storage::query(
        "SELECT text FROM (SELECT id, text FROM messages ORDER BY id DESC LIMIT ?1) ORDER BY id",
        &params![limit],
    )
    .map(|rows| {
        rows.values
            .iter()
            .filter_map(|row| row.first().and_then(Value::as_str).map(str::to_owned))
            .collect()
    })
    .unwrap_or_default()
}

fn broadcast(text: &str) {
    for conn in websockets::connections() {
        // A slow reader drops this message; the others still get it.
        let _ = websockets::send(conn, &Message::Text(text.to_owned()));
    }
}

fn say(text: &str) {
    let at = i64::try_from(now_ms()).unwrap_or(i64::MAX);
    let _ = storage::exec(
        "INSERT INTO messages (at, text) VALUES (?1, ?2)",
        &params![at, text],
    );
    let _ = storage::exec(
        "DELETE FROM messages WHERE id <= (SELECT max(id) FROM messages) - ?1",
        &params![KEEP],
    );
    broadcast(text);
}

/// "* someone joined #lobby (2 online)": the room is this actor's name.
fn announce(what: &str) {
    let room = actors::self_address().name;
    let online = websockets::connections().len();
    broadcast(&format!("* someone {what} #{room} ({online} online)"));
}

impl Actor for ChatRoom {
    fn on_init() -> Result<(), String> {
        storage::exec(
            "CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY,
                at INTEGER NOT NULL,
                text TEXT NOT NULL
            )",
            &[],
        )
        .map(drop)
        .map_err(|e| e.to_string())
    }

    fn on_websocket_open(conn: u64, _path: String) -> Result<(), String> {
        if let Some(motd) = env::get("MOTD") {
            let _ = websockets::send(conn, &Message::Text(format!("* {motd}")));
        }
        for text in recent(HISTORY) {
            let _ = websockets::send(conn, &Message::Text(text));
        }
        announce("joined");
        Ok(())
    }

    fn on_websocket_message(_conn: u64, msg: Message) {
        if let Message::Text(text) = msg {
            say(&text);
        }
    }

    fn on_websocket_close(_conn: u64, _code: u16, _reason: String) {
        announce("left");
    }
}

impl http::Guest for ChatRoom {
    fn handle(request: IncomingRequest, out: ResponseOutparam) {
        match request.method() {
            Method::Post => {
                match http::read_body(&request, 4096).and_then(|b| String::from_utf8(b).ok()) {
                    Some(text) if !text.trim().is_empty() => {
                        say(text.trim());
                        http::respond(out, 200, "text/plain", b"sent\n");
                    }
                    _ => http::respond(out, 400, "text/plain", b"send a UTF-8 text body\n"),
                }
            }
            Method::Get => {
                let mut body = recent(HISTORY).join("\n");
                body.push('\n');
                http::respond(out, 200, "text/plain", body.as_bytes());
            }
            _ => http::respond(out, 405, "text/plain", b"use GET or POST\n"),
        }
    }
}

wasmkeep_sdk::export!(ChatRoom);
