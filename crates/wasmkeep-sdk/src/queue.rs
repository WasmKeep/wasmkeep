//! Queues with at-least-once delivery, built from one actor ([ADR 0015]).
//!
//! A queue is an actor whose SQLite table holds its messages. A producer
//! [`send`](Client::send)s to it with `actors.call`; the send returns once
//! the message is durable. Messages leave the queue in one of two ways:
//!
//! - **Pull**: a consumer [`lease`](Client::lease)s a batch for a
//!   visibility timeout, then [`ack`](Client::ack)s each message (deleted)
//!   or [`retry`](Client::retry)s it. A message neither acked nor retried
//!   becomes visible again when its lease runs out.
//! - **Push**: given a [`Config::consumer`], the queue's alarm leases a
//!   batch and delivers it to that actor in the body of one call; the
//!   consumer answers one [`Outcome`] per message ([`handle_delivery`]).
//!   Failed or unanswered deliveries are retried with backoff, again by the
//!   alarm, so a quiet queue costs nothing between messages.
//!
//! Either way a message is delivered at least once and may be delivered
//! again (a lost ack, a lease that ran out, a failover), with the same
//! [`Message::id`]: consumers record the ids they handled when a duplicate
//! would matter. A message leased [`Config::max_attempts`] times without
//! an ack moves to the dead-letter table ([`Client::dead_letters`]).
//!
//! ```ignore
//! // The queue class (its own component, deployed as class `queue`):
//! use wasmkeep_sdk::queue::{self, Config};
//! struct Jobs;
//! fn config() -> Config { Config::new().consumer("app/worker") }
//! impl wasmkeep_sdk::Actor for Jobs {
//!     fn on_init() -> Result<(), String> { queue::store::init().map_err(|e| e.to_string()) }
//!     fn on_alarm() -> Result<(), String> { queue::store::on_alarm(&config()).map_err(|e| e.to_string()) }
//! }
//! impl wasmkeep_sdk::http::Guest for Jobs {
//!     fn handle(r: wasmkeep_sdk::http::IncomingRequest, o: wasmkeep_sdk::http::ResponseOutparam) {
//!         queue::store::handle(&config(), r, o)
//!     }
//! }
//! wasmkeep_sdk::export!(Jobs);
//!
//! // A producer:
//! wasmkeep_sdk::queue::Client::new("queue/jobs").send(b"resize 42")?;
//! ```
//!
//! Limits: a queue is one actor, so it handles one send, lease or delivery
//! at a time, and each send waits for a durable write (a bucket round trip
//! in a cluster): roughly 4 to 10 sends per second per queue, estimated
//! from the bucket latency targets, not measured.
//! [`Client::send_batch`] shares one write; several queues (by key hash)
//! multiply it. A push delivery holds the queue's event while the consumer
//! runs. Ordering is by visibility time then id, so FIFO only with one
//! consumer, batches of one and no retries.
//!
//! [ADR 0015]: https://github.com/wasmkeep/wasmkeep/blob/main/docs/adr/0015-kv-and-queues-as-sdk-patterns.md

use std::fmt;

use crate::http::{self, IncomingRequest, Method, ResponseOutparam};
use crate::wire::{self, Reader, Writer};
use crate::{SqlError, Value, alarms, now_ms, params, storage};

const PREFIX: &str = "/wasmkeep-queue/v1";
/// The path a push delivery arrives on, at the consumer.
pub const DELIVER_PATH: &str = "/wasmkeep-queue/v1/deliver";
const DELAY: &str = "x-wasmkeep-queue-delay-ms";
const MAX_BODY: usize = 16 * 1024 * 1024;
/// Messages one send may carry.
pub const MAX_BATCH: usize = 1000;

/// A queue's settings. Every field has a default; set what differs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config {
    /// The actor (`class/name`) push deliveries go to; `None` (the
    /// default) for a pull-only queue.
    pub consumer: Option<String>,
    /// Messages per push delivery (default 10).
    pub batch: u32,
    /// How long a leased message stays invisible before it is delivered
    /// again (default 30 s).
    pub visibility_ms: u64,
    /// Leases before a message is dead-lettered (default 5).
    pub max_attempts: u32,
    /// First retry delay after a failed delivery, doubling per attempt
    /// (default 1 s) ...
    pub retry_base_ms: u64,
    /// ... up to this (default 5 min).
    pub retry_max_ms: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            consumer: None,
            batch: 10,
            visibility_ms: 30_000,
            max_attempts: 5,
            retry_base_ms: 1_000,
            retry_max_ms: 300_000,
        }
    }
}

impl Config {
    /// The defaults: pull only.
    pub fn new() -> Self {
        Self::default()
    }

    /// Push deliveries to `address` (`class/name`).
    pub fn consumer(mut self, address: &str) -> Self {
        self.consumer = Some(address.to_owned());
        self
    }

    /// Messages per push delivery (at least one).
    pub fn batch(mut self, n: u32) -> Self {
        self.batch = n.max(1);
        self
    }

    /// The visibility timeout.
    pub fn visibility_ms(mut self, ms: u64) -> Self {
        self.visibility_ms = ms;
        self
    }

    /// Leases before dead-lettering (at least one).
    pub fn max_attempts(mut self, n: u32) -> Self {
        self.max_attempts = n.max(1);
        self
    }

    /// Retry backoff: `base_ms` doubling up to `max_ms`.
    pub fn retry_backoff(mut self, base_ms: u64, max_ms: u64) -> Self {
        self.retry_base_ms = base_ms;
        self.retry_max_ms = max_ms.max(base_ms);
        self
    }

    /// The delay before attempt `attempt + 1`, after attempt `attempt`
    /// (1-based) failed.
    pub fn backoff(&self, attempt: u32) -> u64 {
        let doublings = attempt.saturating_sub(1).min(63);
        self.retry_base_ms
            .saturating_mul(1_u64 << doublings)
            .min(self.retry_max_ms)
    }
}

/// A leased or delivered message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Message {
    /// Unique within the queue, never reused, the same on every delivery.
    pub id: u64,
    /// Which delivery this is, from 1. Acks and retries name it, so one for
    /// an earlier lease of a message leased again is ignored.
    pub attempt: u32,
    /// When it was sent (milliseconds since the Unix epoch).
    pub enqueued_at: u64,
    /// What was sent.
    pub body: Vec<u8>,
}

/// What a consumer did with a message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// Handled: delete it.
    Ack,
    /// Deliver it again after this many milliseconds (`None`: the queue's
    /// backoff), unless it has had all its attempts.
    Retry(Option<u64>),
}

/// How many messages are where.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Stats {
    /// Visible now: the next lease or delivery takes them.
    pub ready: u64,
    /// Sent with a delay that has not passed.
    pub delayed: u64,
    /// Leased and not yet acked, or waiting for a retry.
    pub in_flight: u64,
    /// In the dead-letter table.
    pub dead: u64,
}

/// A dead-lettered message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeadLetter {
    /// The message as it was last delivered.
    pub message: Message,
    /// When it was dead-lettered.
    pub failed_at: u64,
    /// Why: attempts exhausted, or the last delivery error.
    pub reason: String,
}

/// Why a queue operation failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    /// The queue's database refused the operation.
    Storage(String),
    /// The call to the queue failed. For a send, the message may or may
    /// not be queued.
    Call(String),
    /// The queue answered with an unexpected status.
    Refused(u16, String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Storage(m) => write!(f, "storage: {m}"),
            Error::Call(m) => f.write_str(m),
            Error::Refused(s, m) => write!(f, "queue answered {s}: {m}"),
        }
    }
}

impl std::error::Error for Error {}

impl From<SqlError> for Error {
    fn from(e: SqlError) -> Self {
        Error::Storage(e.to_string())
    }
}

fn encode_messages(messages: &[Message]) -> Vec<u8> {
    let mut w = Writer::default();
    w.u64(messages.len() as u64);
    for m in messages {
        w.u64(m.id)
            .u64(u64::from(m.attempt))
            .u64(m.enqueued_at)
            .bytes(&m.body);
    }
    w.finish()
}

fn decode_messages(buf: &[u8]) -> Option<Vec<Message>> {
    let mut r = Reader::new(buf);
    let n = r.u64()?;
    let mut out = Vec::new();
    for _ in 0..n {
        out.push(Message {
            id: r.u64()?,
            attempt: u32::try_from(r.u64()?).ok()?,
            enqueued_at: r.u64()?,
            body: r.bytes()?.to_vec(),
        });
    }
    r.is_empty().then_some(out)
}

/// One settlement: (id, attempt, outcome).
type Settlement = (u64, u32, Outcome);

/// `u64::MAX` in the delay slot means "the queue's backoff".
fn encode_settlements(items: &[Settlement]) -> Vec<u8> {
    let mut w = Writer::default();
    w.u64(items.len() as u64);
    for (id, attempt, outcome) in items {
        let (kind, delay) = match outcome {
            Outcome::Ack => (0, 0),
            Outcome::Retry(d) => (1, d.unwrap_or(u64::MAX)),
        };
        w.u64(*id).u64(u64::from(*attempt)).u64(kind).u64(delay);
    }
    w.finish()
}

fn decode_settlements(buf: &[u8]) -> Option<Vec<Settlement>> {
    let mut r = Reader::new(buf);
    let n = r.u64()?;
    let mut out = Vec::new();
    for _ in 0..n {
        let id = r.u64()?;
        let attempt = u32::try_from(r.u64()?).ok()?;
        let outcome = match (r.u64()?, r.u64()?) {
            (0, _) => Outcome::Ack,
            (1, u64::MAX) => Outcome::Retry(None),
            (1, d) => Outcome::Retry(Some(d)),
            _ => return None,
        };
        out.push((id, attempt, outcome));
    }
    r.is_empty().then_some(out)
}

fn encode_counts(counts: &[u64]) -> Vec<u8> {
    let mut w = Writer::default();
    w.u64(counts.len() as u64);
    for c in counts {
        w.u64(*c);
    }
    w.finish()
}

fn decode_counts(buf: &[u8]) -> Option<Vec<u64>> {
    let mut r = Reader::new(buf);
    let n = r.u64()?;
    let out: Option<Vec<u64>> = (0..n).map(|_| r.u64()).collect();
    out.filter(|_| r.is_empty())
}

fn encode_bodies(bodies: &[&[u8]]) -> Vec<u8> {
    let mut w = Writer::default();
    w.u64(bodies.len() as u64);
    for b in bodies {
        w.bytes(b);
    }
    w.finish()
}

fn decode_bodies(buf: &[u8]) -> Option<Vec<Vec<u8>>> {
    let mut r = Reader::new(buf);
    let n = r.u64()?;
    let out: Option<Vec<Vec<u8>>> = (0..n).map(|_| r.bytes().map(<[u8]>::to_vec)).collect();
    out.filter(|_| r.is_empty())
}

fn refused(status: u16, body: &[u8]) -> Error {
    Error::Refused(status, String::from_utf8_lossy(body).trim().to_owned())
}

fn bad_reply() -> Error {
    Error::Refused(200, "malformed reply from queue".into())
}

/// Reaches one queue actor.
#[derive(Debug, Clone)]
pub struct Client {
    address: String,
}

impl Client {
    /// The queue at `address` (`class/name`).
    pub fn new(address: &str) -> Self {
        Self {
            address: address.to_owned(),
        }
    }

    fn call(
        &self,
        method: &str,
        path: String,
        headers: Vec<(String, String)>,
        body: Vec<u8>,
    ) -> Result<Vec<u8>, Error> {
        let response =
            wire::call(&self.address, method, path, headers, body).map_err(Error::Call)?;
        if response.status != 200 {
            return Err(refused(response.status, &response.body));
        }
        Ok(response.body)
    }

    /// Queues `body`; returns its id once it is durable.
    pub fn send(&self, body: impl AsRef<[u8]>) -> Result<u64, Error> {
        self.send_delayed(body, 0)
    }

    /// Queues `body`, visible after `delay_ms`.
    pub fn send_delayed(&self, body: impl AsRef<[u8]>, delay_ms: u64) -> Result<u64, Error> {
        let ids = self.send_batch_delayed(&[body.as_ref()], delay_ms)?;
        ids.first().copied().ok_or_else(bad_reply)
    }

    /// Queues several messages in one durable write; returns their ids in
    /// order. All or none are queued.
    pub fn send_batch(&self, bodies: &[&[u8]]) -> Result<Vec<u64>, Error> {
        self.send_batch_delayed(bodies, 0)
    }

    fn send_batch_delayed(&self, bodies: &[&[u8]], delay_ms: u64) -> Result<Vec<u64>, Error> {
        let headers = vec![(DELAY.to_owned(), delay_ms.to_string())];
        let reply = self.call(
            "POST",
            format!("{PREFIX}/send"),
            headers,
            encode_bodies(bodies),
        )?;
        let ids = decode_counts(&reply).ok_or_else(bad_reply)?;
        if ids.len() != bodies.len() {
            return Err(bad_reply());
        }
        Ok(ids)
    }

    /// Leases up to `max` visible messages for `visibility_ms`.
    pub fn lease(&self, max: u32, visibility_ms: u64) -> Result<Vec<Message>, Error> {
        let path = format!("{PREFIX}/lease?max={max}&visibility_ms={visibility_ms}");
        let reply = self.call("POST", path, Vec::new(), Vec::new())?;
        decode_messages(&reply).ok_or_else(bad_reply)
    }

    /// Deletes a leased message. `false` if this lease is no longer current
    /// (it ran out and the message was leased again, or was settled).
    pub fn ack(&self, message: &Message) -> Result<bool, Error> {
        self.settle(&[(message.id, message.attempt, Outcome::Ack)])
            .map(|v| v.first() == Some(&true))
    }

    /// Makes a leased message visible again after `delay_ms` (`None`: the
    /// queue's backoff). `false` if this lease is no longer current.
    pub fn retry(&self, message: &Message, delay_ms: Option<u64>) -> Result<bool, Error> {
        self.settle(&[(message.id, message.attempt, Outcome::Retry(delay_ms))])
            .map(|v| v.first() == Some(&true))
    }

    /// Settles several leased messages in one call; one flag per item,
    /// `true` where the lease was current.
    pub fn settle(&self, items: &[(u64, u32, Outcome)]) -> Result<Vec<bool>, Error> {
        let reply = self.call(
            "POST",
            format!("{PREFIX}/settle"),
            Vec::new(),
            encode_settlements(items),
        )?;
        let flags = decode_counts(&reply).ok_or_else(bad_reply)?;
        Ok(flags.into_iter().map(|f| f == 1).collect())
    }

    /// How many messages are ready, delayed, in flight and dead.
    pub fn stats(&self) -> Result<Stats, Error> {
        let reply = self.call("GET", format!("{PREFIX}/stats"), Vec::new(), Vec::new())?;
        match decode_counts(&reply).as_deref() {
            Some(&[ready, delayed, in_flight, dead]) => Ok(Stats {
                ready,
                delayed,
                in_flight,
                dead,
            }),
            _ => Err(bad_reply()),
        }
    }

    /// The oldest `limit` dead-lettered messages.
    pub fn dead_letters(&self, limit: u32) -> Result<Vec<DeadLetter>, Error> {
        let reply = self.call(
            "GET",
            format!("{PREFIX}/dead?limit={limit}"),
            Vec::new(),
            Vec::new(),
        )?;
        decode_dead(&reply).ok_or_else(bad_reply)
    }
}

fn encode_dead(items: &[DeadLetter]) -> Vec<u8> {
    let mut w = Writer::default();
    w.u64(items.len() as u64);
    for d in items {
        let m = &d.message;
        w.u64(m.id)
            .u64(u64::from(m.attempt))
            .u64(m.enqueued_at)
            .bytes(&m.body)
            .u64(d.failed_at)
            .bytes(d.reason.as_bytes());
    }
    w.finish()
}

fn decode_dead(buf: &[u8]) -> Option<Vec<DeadLetter>> {
    let mut r = Reader::new(buf);
    let n = r.u64()?;
    let mut out = Vec::new();
    for _ in 0..n {
        let message = Message {
            id: r.u64()?,
            attempt: u32::try_from(r.u64()?).ok()?,
            enqueued_at: r.u64()?,
            body: r.bytes()?.to_vec(),
        };
        let failed_at = r.u64()?;
        let reason = String::from_utf8(r.bytes()?.to_vec()).ok()?;
        out.push(DeadLetter {
            message,
            failed_at,
            reason,
        });
    }
    r.is_empty().then_some(out)
}

/// Whether `request` is a push delivery from a queue (call
/// [`handle_delivery`] for it).
pub fn is_delivery(request: &IncomingRequest) -> bool {
    matches!(request.method(), Method::Post)
        && request.path_with_query().as_deref() == Some(DELIVER_PATH)
}

/// Answers a push delivery: `handle` gets the sending queue's address
/// (`tenant/class/name`) and each message, and returns its outcome.
/// Deliveries from outside clients (without `x-wasmkeep-caller`) get `403`.
/// Record handled ids in this actor's database, in the same event, when a
/// duplicate delivery would matter.
pub fn handle_delivery(
    request: IncomingRequest,
    out: ResponseOutparam,
    mut handle: impl FnMut(&str, &Message) -> Outcome,
) {
    let Some(queue) = wire::caller(&request) else {
        http::respond(
            out,
            403,
            "text/plain",
            b"deliveries come from queue actors\n",
        );
        return;
    };
    let Some(messages) = http::read_body(&request, MAX_BODY)
        .as_deref()
        .and_then(decode_messages)
    else {
        http::respond(out, 400, "text/plain", b"malformed delivery\n");
        return;
    };
    let outcomes: Vec<Settlement> = messages
        .iter()
        .map(|m| (m.id, m.attempt, handle(&queue, m)))
        .collect();
    http::respond(
        out,
        200,
        "application/octet-stream",
        &encode_settlements(&outcomes),
    );
}

/// The queue actor's side: its tables, the operations on them, push
/// delivery from the alarm, and the protocol [`Client`] speaks.
pub mod store {
    use super::*;

    fn now() -> u64 {
        now_ms()
    }

    fn int(v: u64) -> i64 {
        i64::try_from(v).unwrap_or(i64::MAX)
    }

    fn uint(v: Option<&Value>) -> u64 {
        v.and_then(Value::as_i64)
            .and_then(|v| u64::try_from(v).ok())
            .unwrap_or(0)
    }

    /// Creates the tables (idempotent). Call from `on_init`.
    pub fn init() -> Result<(), SqlError> {
        // AUTOINCREMENT: ids are never reused, so a late ack cannot settle a
        // newer message that took a deleted one's id.
        storage::exec(
            "CREATE TABLE IF NOT EXISTS wasmkeep_queue (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                body BLOB NOT NULL,
                enqueued_at INTEGER NOT NULL,
                visible_at INTEGER NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0,
                last_error TEXT
            )",
            &[],
        )?;
        storage::exec(
            "CREATE INDEX IF NOT EXISTS wasmkeep_queue_visible ON wasmkeep_queue (visible_at, id)",
            &[],
        )?;
        storage::exec(
            "CREATE TABLE IF NOT EXISTS wasmkeep_queue_dead (
                id INTEGER PRIMARY KEY,
                body BLOB NOT NULL,
                enqueued_at INTEGER NOT NULL,
                attempts INTEGER NOT NULL,
                failed_at INTEGER NOT NULL,
                reason TEXT NOT NULL
            )",
            &[],
        )?;
        Ok(())
    }

    /// Runs `f` in a (local, not durable) transaction: committed when it
    /// returns `Ok`, rolled back otherwise.
    fn in_transaction<T>(f: impl FnOnce() -> Result<T, Error>) -> Result<T, Error> {
        storage::exec("BEGIN", &[])?;
        match f() {
            Ok(v) => {
                storage::exec("COMMIT", &[])?;
                Ok(v)
            }
            Err(e) => {
                let _ = storage::exec("ROLLBACK", &[]);
                Err(e)
            }
        }
    }

    /// Queues `bodies` (at most [`MAX_BATCH`]) in one durable statement,
    /// visible after `delay_ms`; returns their ids. With a push consumer,
    /// sets the alarm.
    pub fn send(config: &Config, bodies: &[Vec<u8>], delay_ms: u64) -> Result<Vec<u64>, Error> {
        if bodies.is_empty() {
            return Ok(Vec::new());
        }
        if bodies.len() > MAX_BATCH {
            return Err(Error::Refused(
                413,
                format!("more than {MAX_BATCH} messages"),
            ));
        }
        let now = now();
        let visible = now.saturating_add(delay_ms);
        // One multi-row INSERT: all or none, and one durable write.
        let mut sql =
            String::from("INSERT INTO wasmkeep_queue (body, enqueued_at, visible_at) VALUES ");
        let mut args = Vec::with_capacity(bodies.len() * 3);
        for (i, body) in bodies.iter().enumerate() {
            sql.push_str(if i == 0 { "(?, ?, ?)" } else { ", (?, ?, ?)" });
            args.extend(params![body.as_slice(), int(now), int(visible)]);
        }
        storage::exec_durable(&sql, &args)?;
        // AUTOINCREMENT with one writer: one statement's rows take
        // consecutive ids, ending at the last one inserted.
        let rows = storage::query("SELECT last_insert_rowid() AS id", &[])?;
        let last = uint(rows.get(0, "id"));
        let first = last + 1 - bodies.len() as u64;
        if config.consumer.is_some() {
            wake_by(visible);
        }
        Ok((first..=last).collect())
    }

    /// Sets the alarm to `at` unless one is already due by then.
    fn wake_by(at: u64) {
        if alarms::get_alarm().is_none_or(|pending| pending > at) {
            alarms::set_alarm(at);
        }
    }

    /// Moves visible messages that had all their attempts to the
    /// dead-letter table.
    fn bury_exhausted(config: &Config, now: u64) -> Result<(), Error> {
        let args = params![int(now), i64::from(config.max_attempts)];
        storage::exec(
            "INSERT OR REPLACE INTO wasmkeep_queue_dead
                 (id, body, enqueued_at, attempts, failed_at, reason)
             SELECT id, body, enqueued_at, attempts, ?1,
                    'attempts exhausted' || COALESCE(': ' || last_error, '')
             FROM wasmkeep_queue WHERE visible_at <= ?1 AND attempts >= ?2",
            &args,
        )?;
        storage::exec(
            "DELETE FROM wasmkeep_queue WHERE visible_at <= ?1 AND attempts >= ?2",
            &args,
        )?;
        Ok(())
    }

    /// Leases up to `max` visible messages until `visibility_ms` from now;
    /// dead-letters exhausted ones first.
    pub fn lease(config: &Config, max: u32, visibility_ms: u64) -> Result<Vec<Message>, Error> {
        let now = now();
        let until = now.saturating_add(visibility_ms);
        in_transaction(|| {
            bury_exhausted(config, now)?;
            let rows = storage::query(
                "SELECT id, body, enqueued_at, attempts FROM wasmkeep_queue
                 WHERE visible_at <= ?1 ORDER BY visible_at, id LIMIT ?2",
                &params![int(now), i64::from(max)],
            )?;
            let mut messages = Vec::with_capacity(rows.len());
            for i in 0..rows.len() {
                let id = uint(rows.get(i, "id"));
                let attempt = u32::try_from(uint(rows.get(i, "attempts")) + 1).unwrap_or(u32::MAX);
                storage::exec(
                    "UPDATE wasmkeep_queue SET attempts = ?2, visible_at = ?3 WHERE id = ?1",
                    &params![int(id), i64::from(attempt), int(until)],
                )?;
                messages.push(Message {
                    id,
                    attempt,
                    enqueued_at: uint(rows.get(i, "enqueued_at")),
                    body: rows
                        .get(i, "body")
                        .and_then(Value::as_bytes)
                        .unwrap_or_default()
                        .to_vec(),
                });
            }
            Ok(messages)
        })
    }

    /// Applies outcomes to leased messages; one flag per item, `true` where
    /// `(id, attempt)` was the current lease.
    pub fn settle(config: &Config, items: &[(u64, u32, Outcome)]) -> Result<Vec<bool>, Error> {
        let now = now();
        let mut earliest = None::<u64>;
        let flags = in_transaction(|| {
            let mut flags = Vec::with_capacity(items.len());
            for &(id, attempt, outcome) in items {
                let changed = match outcome {
                    Outcome::Ack => storage::exec(
                        "DELETE FROM wasmkeep_queue WHERE id = ?1 AND attempts = ?2",
                        &params![int(id), i64::from(attempt)],
                    )?,
                    Outcome::Retry(delay) => {
                        // Out of attempts: visible at once, so the next lease
                        // or delivery dead-letters it without waiting.
                        let delay = if attempt >= config.max_attempts {
                            0
                        } else {
                            delay.unwrap_or_else(|| config.backoff(attempt))
                        };
                        let at = now.saturating_add(delay);
                        earliest = Some(earliest.map_or(at, |e| e.min(at)));
                        storage::exec(
                            "UPDATE wasmkeep_queue SET visible_at = ?3 WHERE id = ?1 AND attempts = ?2",
                            &params![int(id), i64::from(attempt), int(at)],
                        )?
                    }
                };
                flags.push(changed == 1);
            }
            Ok(flags)
        })?;
        if let (Some(at), Some(_)) = (earliest, &config.consumer) {
            wake_by(at);
        }
        Ok(flags)
    }

    /// Message counts.
    pub fn stats() -> Result<Stats, Error> {
        let rows = storage::query(
            "SELECT
                (SELECT COUNT(*) FROM wasmkeep_queue WHERE visible_at <= ?1) AS ready,
                (SELECT COUNT(*) FROM wasmkeep_queue WHERE visible_at > ?1 AND attempts = 0) AS delayed,
                (SELECT COUNT(*) FROM wasmkeep_queue WHERE visible_at > ?1 AND attempts > 0) AS in_flight,
                (SELECT COUNT(*) FROM wasmkeep_queue_dead) AS dead",
            &params![int(now())],
        )?;
        Ok(Stats {
            ready: uint(rows.get(0, "ready")),
            delayed: uint(rows.get(0, "delayed")),
            in_flight: uint(rows.get(0, "in_flight")),
            dead: uint(rows.get(0, "dead")),
        })
    }

    /// The oldest `limit` dead letters.
    pub fn dead_letters(limit: u32) -> Result<Vec<DeadLetter>, Error> {
        let rows = storage::query(
            "SELECT id, body, enqueued_at, attempts, failed_at, reason FROM wasmkeep_queue_dead
             ORDER BY failed_at, id LIMIT ?1",
            &params![i64::from(limit)],
        )?;
        Ok((0..rows.len())
            .map(|i| DeadLetter {
                message: Message {
                    id: uint(rows.get(i, "id")),
                    attempt: u32::try_from(uint(rows.get(i, "attempts"))).unwrap_or(u32::MAX),
                    enqueued_at: uint(rows.get(i, "enqueued_at")),
                    body: rows
                        .get(i, "body")
                        .and_then(Value::as_bytes)
                        .unwrap_or_default()
                        .to_vec(),
                },
                failed_at: uint(rows.get(i, "failed_at")),
                reason: rows
                    .get(i, "reason")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned(),
            })
            .collect())
    }

    /// Push delivery, run as the queue's alarm: leases one batch, calls the
    /// consumer with it and applies the outcomes (a failed call retries the
    /// whole batch with backoff), then sets the alarm for what is next. One
    /// batch per alarm, so sends get their turn between batches. Does
    /// nothing for a pull-only queue.
    pub fn on_alarm(config: &Config) -> Result<(), Error> {
        let Some(consumer) = &config.consumer else {
            return Ok(());
        };
        let batch = lease(config, config.batch, config.visibility_ms)?;
        if !batch.is_empty() {
            let delivered = wire::call(
                consumer,
                "POST",
                DELIVER_PATH.to_owned(),
                Vec::new(),
                encode_messages(&batch),
            );
            let outcomes = match delivered {
                // Only outcomes for this batch's leases count; a message the
                // consumer did not answer for waits out its visibility timeout.
                Ok(r) if r.status == 200 => decode_settlements(&r.body)
                    .map(|items| {
                        items
                            .into_iter()
                            .filter(|(id, attempt, _)| {
                                batch.iter().any(|m| m.id == *id && m.attempt == *attempt)
                            })
                            .collect::<Vec<_>>()
                    })
                    .ok_or_else(|| "malformed reply from consumer".to_owned()),
                Ok(r) => Err(format!(
                    "consumer answered {}: {}",
                    r.status,
                    String::from_utf8_lossy(&r.body).trim()
                )),
                Err(e) => Err(e),
            };
            let items: Vec<Settlement> = match outcomes {
                Ok(items) => {
                    let retried: Vec<Message> = batch
                        .iter()
                        .filter(|m| {
                            items
                                .iter()
                                .any(|(id, _, o)| *id == m.id && *o != Outcome::Ack)
                        })
                        .cloned()
                        .collect();
                    record_failure(&retried, "the consumer asked for a retry")?;
                    items
                }
                Err(reason) => {
                    record_failure(&batch, &reason)?;
                    batch
                        .iter()
                        .map(|m| (m.id, m.attempt, Outcome::Retry(None)))
                        .collect()
                }
            };
            settle(config, &items)?;
        }
        // The next alarm: the earliest message to become visible. Messages
        // still ready make that now, so the next batch goes at once.
        let rows = storage::query("SELECT MIN(visible_at) AS next FROM wasmkeep_queue", &[])?;
        match rows.get(0, "next").and_then(Value::as_i64) {
            Some(next) => alarms::set_alarm(u64::try_from(next).unwrap_or(0)),
            None => alarms::clear_alarm(),
        }
        Ok(())
    }

    /// Keeps the delivery error with each message about to be retried, so
    /// a dead letter says why.
    fn record_failure(batch: &[Message], reason: &str) -> Result<(), Error> {
        for m in batch {
            storage::exec(
                "UPDATE wasmkeep_queue SET last_error = ?2 WHERE id = ?1",
                &params![int(m.id), reason],
            )?;
        }
        Ok(())
    }

    /// Serves [`Client`]'s protocol. Requests from outside clients (without
    /// `x-wasmkeep-caller`) get `403`: only other actors may use the queue.
    pub fn handle(config: &Config, request: IncomingRequest, out: ResponseOutparam) {
        handle_allowing(config, request, out, |_| true);
    }

    /// Like [`handle`], also refusing (`403`) actors for which `allow`
    /// returns `false`; it gets the caller's `tenant/class/name`.
    pub fn handle_allowing(
        config: &Config,
        request: IncomingRequest,
        out: ResponseOutparam,
        allow: impl Fn(&str) -> bool,
    ) {
        let (status, body) = match wire::caller(&request) {
            Some(caller) if allow(&caller) => serve(config, &request),
            _ => (403, b"only other actors may use this queue\n".to_vec()),
        };
        let content_type = if status == 200 {
            "application/octet-stream"
        } else {
            "text/plain"
        };
        http::respond(out, status, content_type, &body);
    }

    fn serve(config: &Config, request: &IncomingRequest) -> (u16, Vec<u8>) {
        let path = request.path_with_query().unwrap_or_default();
        let Some(rest) = path.strip_prefix(PREFIX) else {
            return (404, b"not a queue path\n".to_vec());
        };
        let route = rest.split('?').next().unwrap_or_default();
        let number = |name: &str| wire::query_param(rest, name).and_then(|v| v.parse::<u64>().ok());
        let result = match (request.method(), route) {
            (Method::Post, "/send") => {
                let headers = wire::request_headers(request);
                let delay = wire::header(&headers, DELAY)
                    .and_then(|d| d.parse().ok())
                    .unwrap_or(0);
                let Some(bodies) = http::read_body(request, MAX_BODY)
                    .as_deref()
                    .and_then(decode_bodies)
                else {
                    return (400, b"malformed send\n".to_vec());
                };
                send(config, &bodies, delay).map(|ids| encode_counts(&ids))
            }
            (Method::Post, "/lease") => {
                let max = u32::try_from(number("max").unwrap_or(1)).unwrap_or(u32::MAX);
                let visibility = number("visibility_ms").unwrap_or(config.visibility_ms);
                lease(config, max, visibility).map(|m| encode_messages(&m))
            }
            (Method::Post, "/settle") => {
                let Some(items) = http::read_body(request, MAX_BODY)
                    .as_deref()
                    .and_then(decode_settlements)
                else {
                    return (400, b"malformed settle\n".to_vec());
                };
                settle(config, &items).map(|flags| {
                    encode_counts(&flags.into_iter().map(u64::from).collect::<Vec<_>>())
                })
            }
            (Method::Get, "/stats") => {
                stats().map(|s| encode_counts(&[s.ready, s.delayed, s.in_flight, s.dead]))
            }
            (Method::Get, "/dead") => {
                let limit = u32::try_from(number("limit").unwrap_or(100)).unwrap_or(u32::MAX);
                dead_letters(limit).map(|d| encode_dead(&d))
            }
            _ => return (404, b"not a queue operation\n".to_vec()),
        };
        match result {
            Ok(body) => (200, body),
            Err(Error::Refused(status, m)) => (status, format!("{m}\n").into_bytes()),
            Err(e) => (500, format!("{e}\n").into_bytes()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_doubles_and_caps() {
        let c = Config::new().retry_backoff(100, 1_000);
        assert_eq!(
            (1..=6).map(|a| c.backoff(a)).collect::<Vec<_>>(),
            vec![100, 200, 400, 800, 1_000, 1_000]
        );
        assert_eq!(c.backoff(0), 100);
        assert_eq!(c.backoff(u32::MAX), 1_000, "no overflow");
        let d = Config::default();
        assert_eq!((d.backoff(1), d.backoff(40)), (1_000, 300_000));
        assert_eq!(Config::new().max_attempts(0).max_attempts, 1);
        assert_eq!(Config::new().batch(0).batch, 1);
    }

    #[test]
    fn frames_round_trip() {
        let messages = vec![
            Message {
                id: 1,
                attempt: 2,
                enqueued_at: 3,
                body: b"x".to_vec(),
            },
            Message {
                id: u64::MAX,
                attempt: u32::MAX,
                enqueued_at: 0,
                body: Vec::new(),
            },
        ];
        assert_eq!(
            decode_messages(&encode_messages(&messages)),
            Some(messages.clone())
        );
        let items = vec![
            (1, 1, Outcome::Ack),
            (2, 3, Outcome::Retry(None)),
            (3, 1, Outcome::Retry(Some(50))),
        ];
        assert_eq!(decode_settlements(&encode_settlements(&items)), Some(items));
        assert_eq!(decode_counts(&encode_counts(&[4, 5])), Some(vec![4, 5]));
        let bodies: [&[u8]; 2] = [b"a", b""];
        assert_eq!(
            decode_bodies(&encode_bodies(&bodies)),
            Some(vec![b"a".to_vec(), Vec::new()])
        );
        let dead = vec![DeadLetter {
            message: messages[0].clone(),
            failed_at: 9,
            reason: "attempts exhausted".into(),
        }];
        assert_eq!(decode_dead(&encode_dead(&dead)), Some(dead));
    }

    #[test]
    fn malformed_frames_are_refused() {
        // A count promising more than the buffer holds.
        assert_eq!(decode_messages(&encode_counts(&[])[..0]), None);
        let mut one = encode_counts(&[7]);
        one[7] = 2; // says two counts, holds one
        assert_eq!(decode_counts(&one), None);
        // Trailing bytes are refused too.
        let mut extra = encode_counts(&[1]);
        extra.push(0);
        assert_eq!(decode_counts(&extra), None);
        // An unknown outcome kind.
        let mut w = Writer::default();
        w.u64(1).u64(1).u64(1).u64(9).u64(0);
        assert_eq!(decode_settlements(&w.finish()), None);
    }
}
