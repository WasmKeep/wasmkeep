//! One big room, for `bench/room.sh` only. Each actor name is one room, and
//! its first word, up to a `-`, is how the room stores what it is told:
//!
//! - `plain-...`: each message is inserted with `exec` (committed locally,
//!   replicated after the event), then broadcast;
//! - `durable-...`: each message is inserted with `exec-durable` (in the
//!   bucket, or fsynced without one, before it is broadcast);
//! - `batch-...`: messages wait in memory; an alarm 50 ms after the first
//!   commits them in one transaction with `exec-durable`, then broadcasts
//!   them in one frame per socket, a line each.
//!
//! The room numbers every message (`seq`, from 1), and broadcasts it to every
//! socket as `<seq> <text>`. It sends nothing else: no history and no join
//! notices, which would make a ramp-up of N sockets cost N² sends.
//!
//! - Connect a WebSocket to `/actor/room/<name>`; text you send is broadcast.
//! - `POST /` with a text body says the same over HTTP (`200` and the
//!   `seq`, once the message is stored as the mode says; `202` in a batch
//!   room, where it is stored with the next batch).
//! - `GET /` answers the room's counters, one `key value` per line: the last
//!   `seq` given, the messages stored and the highest stored `seq` (so a
//!   reader can check that nothing acknowledged is missing), sends queued,
//!   dropped for `backpressure` and for an unknown connection, and the
//!   sockets open.

use std::cell::RefCell;

use wasmkeep_sdk::http::{self, IncomingRequest, Method, ResponseOutparam};
use wasmkeep_sdk::websockets::{self, SendError};
use wasmkeep_sdk::{Actor, Message, Value, actors, alarms, now_ms, params, storage};

/// How long a batch room collects messages before it commits them.
const BATCH_MS: u64 = 50;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Mode {
    Plain,
    Durable,
    Batch,
}

fn mode() -> Mode {
    let name = actors::self_address().name;
    match name.split('-').next() {
        Some("durable") => Mode::Durable,
        Some("batch") => Mode::Batch,
        _ => Mode::Plain,
    }
}

/// What the room keeps between events while it is resident. A room that
/// hibernates loses the counters (not the messages: `seq` is read back from
/// the database), and a batch room has an alarm pending whenever `pending`
/// is not empty, so it does not hibernate with messages waiting.
#[derive(Default)]
struct State {
    /// The last `seq` given, `None` until read from the database.
    seq: Option<i64>,
    /// A batch room's messages not yet committed, with their `seq`.
    pending: Vec<(i64, String)>,
    sent: u64,
    backpressure: u64,
    unknown: u64,
}

thread_local! {
    static STATE: RefCell<State> = RefCell::new(State::default());
}

fn next_seq() -> i64 {
    STATE.with(|s| {
        let mut s = s.borrow_mut();
        let last = match s.seq {
            Some(seq) => seq,
            None => stored().1,
        };
        s.seq = Some(last + 1);
        last + 1
    })
}

/// Messages stored and the highest stored `seq`.
fn stored() -> (i64, i64) {
    let rows = storage::query("SELECT count(*), coalesce(max(seq), 0) FROM messages", &[]);
    let first = rows.ok().and_then(|r| r.values.into_iter().next());
    let get = |i: usize| {
        first
            .as_ref()
            .and_then(|row| row.get(i))
            .and_then(Value::as_i64)
            .unwrap_or(0)
    };
    (get(0), get(1))
}

fn broadcast(frame: &str) {
    let (mut sent, mut backpressure, mut unknown) = (0, 0, 0);
    for conn in websockets::connections() {
        match websockets::send(conn, &Message::Text(frame.to_owned())) {
            Ok(()) => sent += 1,
            Err(SendError::Backpressure) => backpressure += 1,
            Err(SendError::UnknownConnection) => unknown += 1,
        }
    }
    STATE.with(|s| {
        let mut s = s.borrow_mut();
        s.sent += sent;
        s.backpressure += backpressure;
        s.unknown += unknown;
    });
}

const INSERT: &str = "INSERT INTO messages (seq, body) VALUES (?1, ?2)";

/// Takes one message: stores it as the mode says and broadcasts it, or, in
/// a batch room, queues it for the next batch. Returns its `seq`, and
/// whether it is stored yet.
fn say(text: String) -> Result<(i64, bool), String> {
    let seq = next_seq();
    match mode() {
        Mode::Plain => storage::exec(INSERT, &params![seq, text.as_str()]),
        Mode::Durable => storage::exec_durable(INSERT, &params![seq, text.as_str()]),
        Mode::Batch => {
            let first = STATE.with(|s| {
                let mut s = s.borrow_mut();
                s.pending.push((seq, text));
                s.pending.len() == 1
            });
            if first {
                alarms::set_alarm(now_ms() + BATCH_MS);
            }
            return Ok((seq, false));
        }
    }
    .map_err(|e| {
        // Not stored: give the number back, so `seq` stays without holes.
        STATE.with(|s| s.borrow_mut().seq = Some(seq - 1));
        e.to_string()
    })?;
    broadcast(&format!("{seq} {text}"));
    Ok((seq, true))
}

/// Commits a batch room's waiting messages, then broadcasts them. An alarm
/// is delivered at least once: a batch already committed is not committed
/// again, since `pending` is emptied only once it is.
fn flush() -> Result<(), String> {
    let batch = STATE.with(|s| s.borrow().pending.clone());
    if batch.is_empty() {
        return Ok(());
    }
    storage::exec("BEGIN", &[]).map_err(|e| e.to_string())?;
    for (seq, text) in &batch {
        // `OR IGNORE`: a retried batch may find its first rows there.
        if let Err(e) = storage::exec(
            "INSERT OR IGNORE INTO messages (seq, body) VALUES (?1, ?2)",
            &params![*seq, text.as_str()],
        ) {
            let _ = storage::exec("ROLLBACK", &[]);
            return Err(e.to_string());
        }
    }
    storage::exec_durable("COMMIT", &[]).map_err(|e| e.to_string())?;
    STATE.with(|s| s.borrow_mut().pending.drain(..batch.len()).for_each(drop));
    let mut frame = String::new();
    for (seq, text) in &batch {
        if !frame.is_empty() {
            frame.push('\n');
        }
        frame.push_str(&seq.to_string());
        frame.push(' ');
        frame.push_str(text);
    }
    broadcast(&frame);
    // Messages that arrived while this ran wait for their own batch.
    if STATE.with(|s| !s.borrow().pending.is_empty()) {
        alarms::set_alarm(now_ms() + BATCH_MS);
    }
    Ok(())
}

fn counters() -> String {
    let (count, max) = stored();
    let sockets = websockets::connections().len();
    STATE.with(|s| {
        let s = s.borrow();
        format!(
            "seq {}\nstored {count}\nstored_max_seq {max}\npending {}\nsent {}\nbackpressure {}\nunknown {}\nsockets {sockets}\n",
            s.seq.unwrap_or(max),
            s.pending.len(),
            s.sent,
            s.backpressure,
            s.unknown,
        )
    })
}

struct Room;

impl Actor for Room {
    fn on_init() -> Result<(), String> {
        storage::exec(
            "CREATE TABLE IF NOT EXISTS messages (seq INTEGER PRIMARY KEY, body TEXT NOT NULL)",
            &[],
        )
        .map(drop)
        .map_err(|e| e.to_string())
    }

    fn on_alarm() -> Result<(), String> {
        flush()
    }

    fn on_websocket_open(_conn: u64, _path: String) -> Result<(), String> {
        Ok(())
    }

    fn on_websocket_message(_conn: u64, msg: Message) {
        if let Message::Text(text) = msg {
            // A message that could not be stored is not broadcast: its
            // sender sees no echo, which the load generator counts.
            let _ = say(text);
        }
    }
}

impl http::Guest for Room {
    fn handle(request: IncomingRequest, out: ResponseOutparam) {
        match request.method() {
            Method::Get => http::respond(out, 200, "text/plain", counters().as_bytes()),
            Method::Post => {
                match http::read_body(&request, 64 * 1024).and_then(|b| String::from_utf8(b).ok()) {
                    Some(text) if !text.is_empty() && !text.contains('\n') => match say(text) {
                        Ok((seq, true)) => {
                            http::respond(out, 200, "text/plain", format!("{seq}\n").as_bytes());
                        }
                        Ok((seq, false)) => {
                            http::respond(out, 202, "text/plain", format!("{seq}\n").as_bytes());
                        }
                        Err(e) => http::respond(out, 500, "text/plain", format!("{e}\n").as_bytes()),
                    },
                    _ => http::respond(out, 400, "text/plain", b"send one line of UTF-8 text\n"),
                }
            }
            _ => http::respond(out, 405, "text/plain", b"use GET or POST\n"),
        }
    }
}

wasmkeep_sdk::export!(Room);
