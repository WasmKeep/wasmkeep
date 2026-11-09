//! Durable workflows built from one actor per instance ([ADR 0019]).
//!
//! A workflow is a function the runner calls from the top on every event
//! that moves it forward. Inside it, [`Run::step`] runs a piece of work once
//! and records its result, [`Run::sleep`] waits, and
//! [`Run::wait_for_event`] waits for a [`Client::send_event`]. A recorded
//! step returns its result without running again, so after a sleep, an
//! event or a failover the function replays to where it stopped and goes
//! on:
//!
//! ```ignore
//! use wasmkeep_sdk::workflow::{self, Config, Run, Stop};
//!
//! fn order(wf: &mut Run, input: &[u8]) -> Result<Vec<u8>, Stop> {
//!     let reserved: String = wf.step("reserve stock", || stock::reserve(input))?;
//!     wf.sleep("cool-off", 60_000)?;
//!     if wf.wait_for_event("approved", 86_400_000)?.is_none() {
//!         return Err(Stop::fail("not approved in time"));
//!     }
//!     let key = wf.step_key(); // the next step's idempotency key
//!     let _: () = wf.step("charge", || payments::charge(&reserved, &key))?;
//!     Ok(b"done".to_vec())
//! }
//!
//! // The workflow class (its own component, deployed as class `orders`):
//! struct Orders;
//! impl wasmkeep_sdk::Actor for Orders {
//!     fn on_init() -> Result<(), String> { workflow::store::init().map_err(|e| e.to_string()) }
//!     fn on_alarm() -> Result<(), String> { workflow::store::on_alarm(&Config::new(), order); Ok(()) }
//! }
//! impl wasmkeep_sdk::http::Guest for Orders {
//!     fn handle(r: wasmkeep_sdk::http::IncomingRequest, o: wasmkeep_sdk::http::ResponseOutparam) {
//!         workflow::store::handle(&Config::new(), r, o)
//!     }
//! }
//! wasmkeep_sdk::export!(Orders);
//!
//! // Anywhere else:
//! workflow::Client::new("orders").start("order-42", b"...")?;
//! ```
//!
//! What it asks of the code:
//!
//! - **Determinism between steps.** Given the same input and recorded
//!   results, the function must reach the same steps in the same order.
//!   Anything that varies (the time, randomness, reads of data, HTTP) goes
//!   inside a step. Steps are identified by their position; a run that
//!   reaches a different name at a recorded position fails the workflow
//!   instead of mixing two histories.
//! - **Steps may run more than once**: a result is recorded after the
//!   step's work returns, so a node failure or a cut in between runs it
//!   again. Make steps idempotent, or pass [`Run::step_key`] to the service
//!   they call.
//!
//! Runs happen only in the instance's own `on-alarm`: [`Client::start`]
//! and [`Client::send_event`] record and set the alarm, then answer. While
//! an instance is not finished its alarm stays set, so a trap or a cut
//! never leaves it stuck. A finished instance keeps its status for
//! [`Config::retention_ms`], then deletes itself.
//!
//! [ADR 0019]: https://github.com/wasmkeep/wasmkeep/blob/main/docs/adr/0019-workflows-as-an-sdk-pattern.md

use std::fmt;

use crate::http::{self, IncomingRequest, Method, ResponseOutparam};
use crate::wire::{self, Reader, Writer};
use crate::{SqlError, Value, alarms, now_ms, params, storage};

const PREFIX: &str = "/wasmkeep-workflow/v1";
const MAX_BODY: usize = 16 * 1024 * 1024;

/// A workflow class's settings. Every field has a default; set what
/// differs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config {
    /// Attempts a step gets before it fails (default 5).
    pub max_attempts: u32,
    /// Backoff between a step's attempts: `base_ms` doubling up to `max_ms`
    /// (default 1 s to 5 min).
    pub backoff_base_ms: u64,
    /// See [`Config::backoff_base_ms`].
    pub backoff_max_ms: u64,
    /// How long a finished instance keeps its status and output before it
    /// deletes itself (default 7 days).
    pub retention_ms: u64,
    /// How much of an event's wall-clock time (30 s) a run uses for new
    /// steps before it continues in a new event (default 20 s).
    pub run_budget_ms: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            max_attempts: 5,
            backoff_base_ms: 1_000,
            backoff_max_ms: 300_000,
            retention_ms: 7 * 86_400_000,
            run_budget_ms: 20_000,
        }
    }
}

impl Config {
    /// The defaults.
    pub fn new() -> Self {
        Self::default()
    }

    /// Attempts a step gets before it fails.
    #[must_use]
    pub fn max_attempts(mut self, n: u32) -> Self {
        self.max_attempts = n.max(1);
        self
    }

    /// Backoff between a step's attempts.
    #[must_use]
    pub fn backoff(mut self, base_ms: u64, max_ms: u64) -> Self {
        self.backoff_base_ms = base_ms.max(1);
        self.backoff_max_ms = max_ms.max(self.backoff_base_ms);
        self
    }

    /// How long a finished instance is kept.
    #[must_use]
    pub fn retention_ms(mut self, ms: u64) -> Self {
        self.retention_ms = ms;
        self
    }

    /// Wall-clock time a run spends on new steps before it continues in a
    /// new event.
    #[must_use]
    pub fn run_budget_ms(mut self, ms: u64) -> Self {
        self.run_budget_ms = ms;
        self
    }

    /// The wait before attempt `attempt + 1` after `attempt` failed.
    pub fn delay(&self, attempt: u32) -> u64 {
        let doublings = attempt.saturating_sub(1).min(32);
        self.backoff_base_ms
            .saturating_mul(1u64 << doublings)
            .min(self.backoff_max_ms)
    }
}

/// Why a run stops before the workflow returns. Propagate it with `?`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Stop {
    /// Waiting: for a sleep, an event, a step's next attempt, or a new
    /// event to continue in. The runner sets the alarm.
    Wait,
    /// The workflow fails with this error.
    Failed(String),
    /// The runner's own database refused: not the workflow's fault, so the
    /// run is tried again later instead of failing it. Internal.
    #[doc(hidden)]
    Retry(String),
}

impl Stop {
    /// Fails the workflow with `reason`.
    pub fn fail(reason: impl Into<String>) -> Self {
        Self::Failed(reason.into())
    }
}

impl fmt::Display for Stop {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Wait => f.write_str("waiting"),
            Self::Failed(e) => write!(f, "failed: {e}"),
            Self::Retry(e) => write!(f, "to retry: {e}"),
        }
    }
}

impl std::error::Error for Stop {}

/// A value a step can return, recorded as bytes.
pub trait Stored: Sized {
    /// The bytes recorded.
    fn encode(&self) -> Vec<u8>;
    /// The value back, or `None` if the bytes are not one (a deploy changed
    /// the step's type).
    fn decode(bytes: &[u8]) -> Option<Self>;
}

impl Stored for Vec<u8> {
    fn encode(&self) -> Vec<u8> {
        self.clone()
    }
    fn decode(bytes: &[u8]) -> Option<Self> {
        Some(bytes.to_vec())
    }
}

impl Stored for String {
    fn encode(&self) -> Vec<u8> {
        self.as_bytes().to_vec()
    }
    fn decode(bytes: &[u8]) -> Option<Self> {
        String::from_utf8(bytes.to_vec()).ok()
    }
}

impl Stored for () {
    fn encode(&self) -> Vec<u8> {
        Vec::new()
    }
    fn decode(bytes: &[u8]) -> Option<Self> {
        bytes.is_empty().then_some(())
    }
}

impl Stored for u64 {
    fn encode(&self) -> Vec<u8> {
        self.to_be_bytes().to_vec()
    }
    fn decode(bytes: &[u8]) -> Option<Self> {
        Some(u64::from_be_bytes(bytes.try_into().ok()?))
    }
}

impl Stored for i64 {
    fn encode(&self) -> Vec<u8> {
        self.to_be_bytes().to_vec()
    }
    fn decode(bytes: &[u8]) -> Option<Self> {
        Some(i64::from_be_bytes(bytes.try_into().ok()?))
    }
}

impl Stored for bool {
    fn encode(&self) -> Vec<u8> {
        vec![u8::from(*self)]
    }
    fn decode(bytes: &[u8]) -> Option<Self> {
        match bytes {
            [0] => Some(false),
            [1] => Some(true),
            _ => None,
        }
    }
}

/// One recorded position.
#[derive(Debug, Clone)]
struct Recorded {
    name: String,
    kind: String,
    state: String,
    result: Option<Vec<u8>>,
    error: Option<String>,
    attempts: u32,
    next_attempt_at: Option<u64>,
    wake_at: Option<u64>,
}

/// One run of a workflow instance: what the workflow function uses.
#[derive(Debug)]
pub struct Run {
    id: String,
    run_id: String,
    config: Config,
    started_ms: u64,
    pos: u64,
    recorded: std::collections::BTreeMap<u64, Recorded>,
    /// Earliest time the runner must look again, when it stops waiting.
    wake: Option<u64>,
    /// Event names the run waits for.
    waiting_for: Vec<String>,
    /// A call already stopped the run: every later call stops too, so a
    /// `Stop` the workflow swallowed cannot shift the positions after it.
    stopped: bool,
    /// The first `Stop` a call returned: what the run ends with, even if
    /// the workflow dropped it and returned.
    first_stop: Option<Stop>,
    /// Attempts the last step that ran out of them had.
    last_attempts: u32,
    /// This run already ran a new step: the run budget applies from the
    /// second one, so every event makes progress.
    ran_step: bool,
}

impl Run {
    /// The instance's id (the actor's name).
    pub fn id(&self) -> &str {
        &self.id
    }

    /// A key unique to this run and to the next position: call it just
    /// before [`Run::step`] and pass it to the service the step calls, as
    /// an idempotency key, so a step that runs twice is applied once. A
    /// later run under the same id (after retention) gets other keys.
    pub fn step_key(&self) -> String {
        format!("{}-{}", self.run_id, self.pos)
    }

    /// Runs `f` once and records its result; returns the recorded result on
    /// every later run. A failure is retried with backoff; when the
    /// attempts run out the workflow fails with the last error (see
    /// [`Run::try_step`] to handle it instead).
    pub fn step<T: Stored>(
        &mut self,
        name: &str,
        f: impl FnOnce() -> Result<T, String>,
    ) -> Result<T, Stop> {
        match self.try_step(name, f)? {
            Ok(value) => Ok(value),
            Err(e) => {
                let failed = Err(Stop::Failed(format!(
                    "step {name:?} failed after {} attempts: {e}",
                    self.last_attempts
                )));
                self.halt(failed)
            }
        }
    }

    /// [`Run::step`] with its idempotency key ([`Run::step_key`]) given to
    /// `f`: the key for this very step, whatever came before it.
    pub fn step_keyed<T: Stored>(
        &mut self,
        name: &str,
        f: impl FnOnce(&str) -> Result<T, String>,
    ) -> Result<T, Stop> {
        let key = self.step_key();
        self.step(name, || f(&key))
    }

    /// Like [`Run::step`], but when the attempts run out it returns the last
    /// error to the workflow (every later run returns the same error).
    pub fn try_step<T: Stored>(
        &mut self,
        name: &str,
        f: impl FnOnce() -> Result<T, String>,
    ) -> Result<Result<T, String>, Stop> {
        let result = self.attempt(name, f);
        self.halt(result)
    }

    fn attempt<T: Stored>(
        &mut self,
        name: &str,
        f: impl FnOnce() -> Result<T, String>,
    ) -> Result<Result<T, String>, Stop> {
        let pos = self.next(name, "step")?;
        let now = now_ms();
        let mut attempts = 0;
        if let Some(r) = self.recorded.get(&pos).cloned() {
            match r.state.as_str() {
                "done" => {
                    let bytes = r.result.unwrap_or_default();
                    return T::decode(&bytes).map(Ok).ok_or_else(|| {
                        Stop::Failed(format!(
                            "step {name:?}: its recorded result does not decode as this type \
                             (did a deploy change it?)"
                        ))
                    });
                }
                "failed" => {
                    self.last_attempts = r.attempts;
                    return Ok(Err(r.error.unwrap_or_default()));
                }
                _ => {}
            }
            if let Some(at) = r.next_attempt_at
                && at > now
            {
                return Err(self.wait_until(at));
            }
            attempts = r.attempts;
        }
        if attempts >= self.config.max_attempts {
            self.last_attempts = attempts;
            // Attempts used up, the last one cut before it said how it ended.
            let error = "the last attempt did not finish (trapped or cut)".to_owned();
            exec_durable(
                "UPDATE wasmkeep_wf_step SET state = 'failed', error = ? WHERE pos = ?",
                params![error.clone(), pos_param(pos)],
            )?;
            return Ok(Err(error));
        }
        if self.ran_step && now.saturating_sub(self.started_ms) >= self.config.run_budget_ms {
            // Continue in a new event, with this event's time left alone.
            return Err(self.wait_until(now));
        }
        self.ran_step = true;
        // Count the attempt before running it: a trap inside `f` counts too.
        let attempt = attempts + 1;
        // If this attempt never says how it ended (a trap, a cut), the next
        // one waits its backoff all the same: an event delivered meanwhile
        // does not bring it early.
        let retry_at = now.saturating_add(self.config.delay(attempt));
        exec(
            "INSERT INTO wasmkeep_wf_step (pos, name, kind, state, attempts, error, next_attempt_at) \
             VALUES (?, ?, 'step', 'pending', ?, 'did not finish (trapped or cut)', ?) \
             ON CONFLICT (pos) DO UPDATE SET attempts = excluded.attempts, error = excluded.error, \
             next_attempt_at = excluded.next_attempt_at",
            params![pos_param(pos), name, i64::from(attempt), as_i64(retry_at)],
        )?;
        // The watchdog: if this event ends without a word, the next attempt
        // still comes.
        alarms::set_alarm(retry_at);
        match f() {
            Ok(value) => {
                exec_durable(
                    "UPDATE wasmkeep_wf_step SET state = 'done', result = ?, error = NULL WHERE pos = ?",
                    params![value.encode(), pos_param(pos)],
                )?;
                Ok(Ok(value))
            }
            Err(e) if attempt >= self.config.max_attempts => {
                self.last_attempts = attempt;
                exec_durable(
                    "UPDATE wasmkeep_wf_step SET state = 'failed', error = ? WHERE pos = ?",
                    params![e.clone(), pos_param(pos)],
                )?;
                Ok(Err(e))
            }
            Err(e) => {
                let at = now_ms().saturating_add(self.config.delay(attempt));
                exec(
                    "UPDATE wasmkeep_wf_step SET error = ?, next_attempt_at = ? WHERE pos = ?",
                    params![e, as_i64(at), pos_param(pos)],
                )?;
                Err(self.wait_until(at))
            }
        }
    }

    /// Waits `ms` milliseconds from the first time this position is
    /// reached.
    pub fn sleep(&mut self, name: &str, ms: u64) -> Result<(), Stop> {
        self.sleep_at(name, |now| now.saturating_add(ms))
    }

    /// Waits until `at` (milliseconds since the Unix epoch).
    pub fn sleep_until(&mut self, name: &str, at: u64) -> Result<(), Stop> {
        self.sleep_at(name, |_| at)
    }

    fn sleep_at(&mut self, name: &str, at: impl FnOnce(u64) -> u64) -> Result<(), Stop> {
        let result = self.sleep_inner(name, at);
        self.halt(result)
    }

    fn sleep_inner(&mut self, name: &str, at: impl FnOnce(u64) -> u64) -> Result<(), Stop> {
        let pos = self.next(name, "sleep")?;
        let now = now_ms();
        let wake = match self.recorded.get(&pos) {
            Some(r) => r.wake_at.unwrap_or(0),
            None => {
                let wake = at(now);
                exec_durable(
                    "INSERT INTO wasmkeep_wf_step (pos, name, kind, state, wake_at) \
                     VALUES (?, ?, 'sleep', 'pending', ?)",
                    params![pos_param(pos), name, as_i64(wake)],
                )?;
                wake
            }
        };
        if now >= wake {
            Ok(())
        } else {
            Err(self.wait_until(wake))
        }
    }

    /// Waits for an event named `name` ([`Client::send_event`]) and returns
    /// its payload, or `None` once `timeout_ms` has passed since this
    /// position was first reached. Each wait takes the oldest event of
    /// that name no earlier wait took.
    pub fn wait_for_event(&mut self, name: &str, timeout_ms: u64) -> Result<Option<Vec<u8>>, Stop> {
        let result = self.wait_inner(name, timeout_ms);
        self.halt(result)
    }

    fn wait_inner(&mut self, name: &str, timeout_ms: u64) -> Result<Option<Vec<u8>>, Stop> {
        let pos = self.next(name, "event")?;
        let now = now_ms();
        let deadline = match self.recorded.get(&pos) {
            // A wait that timed out says so in `error` (or, from the first
            // runner, left a NULL result; a delivered payload never is:
            // the column is NOT NULL). Otherwise the result is the payload.
            Some(r) if r.state == "done" => {
                return Ok(if r.error.as_deref() == Some("timed out") {
                    None
                } else {
                    r.result.clone()
                });
            }
            Some(r) => r.wake_at.unwrap_or(0),
            None => {
                let deadline = now.saturating_add(timeout_ms);
                exec_durable(
                    "INSERT INTO wasmkeep_wf_step (pos, name, kind, state, wake_at) \
                     VALUES (?, ?, 'event', 'pending', ?)",
                    params![pos_param(pos), name, as_i64(deadline)],
                )?;
                deadline
            }
        };
        let found = storage::query(
            "SELECT seq, payload FROM wasmkeep_wf_event \
             WHERE name = ? AND (consumed_at_pos IS NULL OR consumed_at_pos = ?) \
             ORDER BY seq LIMIT 1",
            &params![name, pos_param(pos)],
        )
        .map_err(sql_stop)?;
        if let Some(row) = found.values.first() {
            let seq = int(&row[0]);
            let payload = blob(&row[1]);
            exec(
                "UPDATE wasmkeep_wf_event SET consumed_at_pos = ? WHERE seq = ?",
                params![pos_param(pos), seq],
            )?;
            exec_durable(
                "UPDATE wasmkeep_wf_step SET state = 'done', result = ? WHERE pos = ?",
                params![payload.clone(), pos_param(pos)],
            )?;
            return Ok(Some(payload));
        }
        if now >= deadline {
            exec_durable(
                "UPDATE wasmkeep_wf_step SET state = 'done', result = NULL, error = 'timed out' \
                 WHERE pos = ?",
                params![pos_param(pos)],
            )?;
            return Ok(None);
        }
        self.waiting_for.push(name.to_owned());
        Err(self.wait_until(deadline))
    }

    /// Takes the next position for `name` of `kind`, checking it against
    /// what was recorded there.
    fn next(&mut self, name: &str, kind: &str) -> Result<u64, Stop> {
        if self.stopped {
            return Err(Stop::Wait);
        }
        let pos = self.pos;
        self.pos += 1;
        if let Some(r) = self.recorded.get(&pos)
            && (r.name != name || r.kind != kind)
        {
            return Err(Stop::Failed(format!(
                "the code changed the order of its steps: position {pos} was {} {:?}, now {kind} {name:?}",
                r.kind, r.name
            )));
        }
        Ok(pos)
    }

    /// Notes that the run stops here (see `stopped`).
    fn halt<T>(&mut self, result: Result<T, Stop>) -> Result<T, Stop> {
        if let Err(stop) = &result {
            self.stopped = true;
            if self.first_stop.is_none() {
                self.first_stop = Some(stop.clone());
            }
        }
        result
    }

    fn wait_until(&mut self, at: u64) -> Stop {
        self.wake = Some(self.wake.map_or(at, |w| w.min(at)));
        self.stopped = true;
        Stop::Wait
    }
}

fn pos_param(pos: u64) -> i64 {
    i64::try_from(pos).unwrap_or(i64::MAX)
}

fn as_i64(v: u64) -> i64 {
    i64::try_from(v).unwrap_or(i64::MAX)
}

fn int(v: &Value) -> i64 {
    match v {
        Value::Integer(i) => *i,
        _ => 0,
    }
}

fn opt_int(v: &Value) -> Option<u64> {
    match v {
        Value::Integer(i) => u64::try_from(*i).ok(),
        _ => None,
    }
}

fn blob(v: &Value) -> Vec<u8> {
    match v {
        Value::Blob(b) => b.clone(),
        Value::Text(t) => t.as_bytes().to_vec(),
        _ => Vec::new(),
    }
}

fn opt_blob(v: &Value) -> Option<Vec<u8>> {
    match v {
        Value::Null => None,
        other => Some(blob(other)),
    }
}

fn text(v: &Value) -> Option<String> {
    match v {
        Value::Text(t) => Some(t.clone()),
        _ => None,
    }
}

fn sql_stop(e: SqlError) -> Stop {
    Stop::Retry(format!("the workflow's own database: {}", e.message))
}

fn exec(sql: &str, args: Vec<Value>) -> Result<(), Stop> {
    storage::exec(sql, &args).map(drop).map_err(sql_stop)
}

fn exec_durable(sql: &str, args: Vec<Value>) -> Result<(), Stop> {
    storage::exec_durable(sql, &args)
        .map(drop)
        .map_err(sql_stop)
}

/// Where an instance stands.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum State {
    /// Started, or continuing: its alarm runs it next.
    Running,
    /// Waiting for a time or an event.
    Waiting,
    /// Returned this output.
    Completed,
    /// Failed with [`Status::error`].
    Failed,
    /// Cancelled with [`Client::cancel`].
    Cancelled,
}

impl State {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Waiting => "waiting",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }

    fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "running" => Self::Running,
            "waiting" => Self::Waiting,
            "completed" => Self::Completed,
            "failed" => Self::Failed,
            "cancelled" => Self::Cancelled,
            _ => return None,
        })
    }

    /// Whether the instance has finished (completed, failed or cancelled).
    pub fn is_finished(&self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Cancelled)
    }
}

/// An instance's status, as [`Client::status`] reports it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Status {
    /// Where it stands.
    pub state: State,
    /// When waiting: until when (ms since the Unix epoch), at the latest.
    pub wake_at: Option<u64>,
    /// When waiting for events: their names.
    pub waiting_for: Vec<String>,
    /// When completed: the workflow's output.
    pub output: Option<Vec<u8>>,
    /// When failed: why.
    pub error: Option<String>,
    /// Positions recorded so far (steps, sleeps and waits).
    pub positions: u64,
}

fn encode_status(s: &Status) -> Vec<u8> {
    let mut w = Writer::default();
    w.bytes(s.state.as_str().as_bytes())
        .u64(s.wake_at.unwrap_or(0))
        .bytes(s.waiting_for.join("\n").as_bytes())
        .u64(u64::from(s.output.is_some()))
        .bytes(s.output.as_deref().unwrap_or_default())
        .bytes(s.error.as_deref().unwrap_or_default().as_bytes())
        .u64(s.positions);
    w.finish()
}

fn decode_status(buf: &[u8]) -> Option<Status> {
    let mut r = Reader::new(buf);
    let state = State::parse(std::str::from_utf8(r.bytes()?).ok()?)?;
    let wake_at = Some(r.u64()?).filter(|w| *w != 0);
    let names = std::str::from_utf8(r.bytes()?).ok()?;
    let has_output = r.u64()? == 1;
    let output = r.bytes()?.to_vec();
    let error = std::str::from_utf8(r.bytes()?).ok()?.to_owned();
    let positions = r.u64()?;
    Some(Status {
        state,
        wake_at,
        waiting_for: names
            .split('\n')
            .filter(|n| !n.is_empty())
            .map(str::to_owned)
            .collect(),
        output: has_output.then_some(output),
        error: (!error.is_empty()).then_some(error),
        positions,
    })
}

/// What can go wrong talking to an instance.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    /// No instance with that id (never started, or deleted after its
    /// retention).
    Unknown,
    /// The instance has finished, so it takes no more events.
    Finished,
    /// The call failed (the instance may or may not have recorded it).
    Call(String),
    /// The instance refused the request.
    Refused(u16, String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unknown => f.write_str("no such workflow instance"),
            Self::Finished => f.write_str("the workflow instance has finished"),
            Self::Call(e) => write!(f, "{e}"),
            Self::Refused(status, body) => write!(f, "refused ({status}): {body}"),
        }
    }
}

impl std::error::Error for Error {}

/// Starts and queries instances of one workflow class.
#[derive(Debug, Clone)]
pub struct Client {
    class: String,
}

impl Client {
    /// Instances of `class` (the workflow class's name).
    pub fn new(class: &str) -> Self {
        Self {
            class: class.to_owned(),
        }
    }

    fn call(&self, id: &str, method: &str, path: String, body: Vec<u8>) -> Result<Vec<u8>, Error> {
        let address = format!("{}/{id}", self.class);
        let response = wire::call(&address, method, path, Vec::new(), body).map_err(Error::Call)?;
        match response.status {
            200 => Ok(response.body),
            404 => Err(Error::Unknown),
            409 => Err(Error::Finished),
            status => Err(Error::Refused(
                status,
                String::from_utf8_lossy(&response.body).trim().to_owned(),
            )),
        }
    }

    fn status_of(body: &[u8]) -> Result<Status, Error> {
        decode_status(body).ok_or_else(|| Error::Refused(200, "malformed status".into()))
    }

    /// Starts instance `id` with `input`, or returns its status if it
    /// exists (a start is idempotent while the instance is kept). It runs
    /// in its own event, soon after: this call only records it.
    pub fn start(&self, id: &str, input: impl AsRef<[u8]>) -> Result<Status, Error> {
        let body = self.call(
            id,
            "POST",
            format!("{PREFIX}/start"),
            input.as_ref().to_vec(),
        )?;
        Self::status_of(&body)
    }

    /// The instance's status; [`Error::Unknown`] if there is none.
    pub fn status(&self, id: &str) -> Result<Status, Error> {
        let body = self.call(id, "GET", format!("{PREFIX}/status"), Vec::new())?;
        Self::status_of(&body)
    }

    /// Delivers event `name` with `payload`: recorded durably, then the
    /// instance runs. [`Error::Unknown`] if it was never started,
    /// [`Error::Finished`] if it has finished.
    pub fn send_event(&self, id: &str, name: &str, payload: impl AsRef<[u8]>) -> Result<(), Error> {
        self.call(
            id,
            "POST",
            format!("{PREFIX}/event?name={}", wire::hex(name.as_bytes())),
            payload.as_ref().to_vec(),
        )
        .map(drop)
    }

    /// Cancels the instance: it runs no further step (one running now
    /// finishes first). Returns its status; a finished one is unchanged.
    pub fn cancel(&self, id: &str) -> Result<Status, Error> {
        let body = self.call(id, "POST", format!("{PREFIX}/cancel"), Vec::new())?;
        Self::status_of(&body)
    }
}

/// The workflow class's side: tables, runs and the client's protocol.
pub mod store {
    use super::*;

    /// Creates the tables. Call it from `on_init`.
    pub fn init() -> Result<(), SqlError> {
        for sql in [
            "CREATE TABLE IF NOT EXISTS wasmkeep_wf (id INTEGER PRIMARY KEY CHECK (id = 1), \
             status TEXT NOT NULL, run_id TEXT NOT NULL, input BLOB NOT NULL, output BLOB, \
             error TEXT, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL, \
             finished_at INTEGER, unfinished_runs INTEGER NOT NULL DEFAULT 0)",
            "CREATE TABLE IF NOT EXISTS wasmkeep_wf_step (pos INTEGER PRIMARY KEY, \
             name TEXT NOT NULL, kind TEXT NOT NULL, state TEXT NOT NULL, result BLOB, \
             error TEXT, attempts INTEGER NOT NULL DEFAULT 0, next_attempt_at INTEGER, \
             wake_at INTEGER)",
            "CREATE TABLE IF NOT EXISTS wasmkeep_wf_event (seq INTEGER PRIMARY KEY AUTOINCREMENT, \
             name TEXT NOT NULL, payload BLOB NOT NULL, received_at INTEGER NOT NULL, \
             consumed_at_pos INTEGER)",
        ] {
            storage::exec(sql, &[])?;
        }
        Ok(())
    }

    struct Instance {
        status: State,
        unfinished_runs: u32,
        run_id: String,
        input: Vec<u8>,
        output: Option<Vec<u8>>,
        error: Option<String>,
        finished_at: Option<u64>,
    }

    fn load() -> Result<Option<Instance>, SqlError> {
        let rows = storage::query(
            "SELECT status, run_id, input, output, error, finished_at, unfinished_runs \
             FROM wasmkeep_wf WHERE id = 1",
            &[],
        )?;
        Ok(rows.values.first().map(|r| Instance {
            status: text(&r[0])
                .and_then(|s| State::parse(&s))
                .unwrap_or(State::Running),
            run_id: text(&r[1]).unwrap_or_default(),
            input: blob(&r[2]),
            output: opt_blob(&r[3]),
            error: text(&r[4]),
            finished_at: opt_int(&r[5]),
            unfinished_runs: u32::try_from(int(&r[6])).unwrap_or(u32::MAX),
        }))
    }

    fn recorded() -> Result<std::collections::BTreeMap<u64, Recorded>, SqlError> {
        let rows = storage::query(
            "SELECT pos, name, kind, state, result, error, attempts, next_attempt_at, wake_at \
             FROM wasmkeep_wf_step ORDER BY pos",
            &[],
        )?;
        Ok(rows
            .values
            .iter()
            .map(|r| {
                let pos = opt_int(&r[0]).unwrap_or(0);
                let recorded = Recorded {
                    name: text(&r[1]).unwrap_or_default(),
                    kind: text(&r[2]).unwrap_or_default(),
                    state: text(&r[3]).unwrap_or_default(),
                    result: opt_blob(&r[4]),
                    error: text(&r[5]),
                    attempts: u32::try_from(int(&r[6])).unwrap_or(0),
                    next_attempt_at: opt_int(&r[7]),
                    wake_at: opt_int(&r[8]),
                };
                (pos, recorded)
            })
            .collect())
    }

    /// Status as reported: waits are derived from the pending positions.
    fn status(instance: &Instance) -> Result<Status, SqlError> {
        let steps = recorded()?;
        let mut wake_at: Option<u64> = None;
        let mut waiting_for = Vec::new();
        if instance.status == State::Waiting {
            for r in steps.values().filter(|r| r.state == "pending") {
                if let Some(at) = r.next_attempt_at.or(r.wake_at) {
                    wake_at = Some(wake_at.map_or(at, |w| w.min(at)));
                }
                if r.kind == "event" {
                    waiting_for.push(r.name.clone());
                }
            }
        }
        Ok(Status {
            state: instance.status.clone(),
            wake_at,
            waiting_for,
            output: instance.output.clone(),
            error: instance.error.clone(),
            positions: steps.len() as u64,
        })
    }

    fn set_status(
        state: &State,
        output: Option<Vec<u8>>,
        error: Option<String>,
    ) -> Result<(), SqlError> {
        let now = as_i64(now_ms());
        let finished = state.is_finished().then_some(now);
        storage::exec_durable(
            "UPDATE wasmkeep_wf SET status = ?, output = ?, error = ?, updated_at = ?, \
             finished_at = COALESCE(finished_at, ?) WHERE id = 1",
            &params![state.as_str(), output, error, now, finished],
        )
        .map(drop)
    }

    /// Runs the instance: call it from `on_alarm`. It runs `workflow` as far
    /// as it can, records where it stands, and sets the alarm for what
    /// comes next; a finished instance past its retention deletes itself.
    /// It never returns an error: retries are its own.
    pub fn on_alarm(config: &Config, workflow: impl Fn(&mut Run, &[u8]) -> Result<Vec<u8>, Stop>) {
        if run(config, workflow).is_err() && alarms::get_alarm().is_none() {
            // The database refused: the watchdog set before the run brings
            // the next one (backed off by the run counter); set one if there
            // is none.
            alarms::set_alarm(now_ms().saturating_add(config.backoff_base_ms));
        }
    }

    fn run(
        config: &Config,
        workflow: impl Fn(&mut Run, &[u8]) -> Result<Vec<u8>, Stop>,
    ) -> Result<(), String> {
        run_inner(config, workflow).map_err(|e| e.message)?
    }

    /// Runs that ended without the workflow function returning (a trap or
    /// a cut outside any step, the replay over its CPU budget) before the
    /// instance fails.
    fn max_unfinished_runs(config: &Config) -> u32 {
        config.max_attempts.saturating_mul(2).max(3)
    }

    #[allow(clippy::too_many_lines)]
    fn run_inner(
        config: &Config,
        workflow: impl Fn(&mut Run, &[u8]) -> Result<Vec<u8>, Stop>,
    ) -> Result<Result<(), String>, SqlError> {
        let now = now_ms();
        let Some(instance) = load()? else {
            // An empty actor (a call for an id never started): nothing to
            // keep.
            alarms::clear_alarm();
            storage::delete_self();
            return Ok(Ok(()));
        };
        if instance.status.is_finished() {
            let until = instance
                .finished_at
                .unwrap_or(now)
                .saturating_add(config.retention_ms);
            if now >= until {
                alarms::clear_alarm();
                storage::delete_self();
            } else {
                alarms::set_alarm(until);
            }
            return Ok(Ok(()));
        }
        let unfinished = instance.unfinished_runs;
        if unfinished >= max_unfinished_runs(config) {
            set_status(
                &State::Failed,
                None,
                Some(format!(
                    "the workflow's runs ended {unfinished} times without it returning \
                     (a trap or a cut outside any step, or a replay over its CPU budget)"
                )),
            )?;
            alarms::set_alarm(now_ms().saturating_add(config.retention_ms));
            return Ok(Ok(()));
        }
        // Counted now, cleared when the function returns: runs that keep
        // dying back off and end the instance instead of retrying forever.
        storage::exec(
            "UPDATE wasmkeep_wf SET unfinished_runs = unfinished_runs + 1 WHERE id = 1",
            &[],
        )?;
        // The watchdog: from here on, an alarm is always set.
        alarms::set_alarm(now.saturating_add(config.delay(unfinished + 1)));
        let mut run = Run {
            id: crate::actors::self_address().name,
            run_id: instance.run_id.clone(),
            config: config.clone(),
            started_ms: now,
            pos: 0,
            recorded: recorded()?,
            wake: None,
            waiting_for: Vec::new(),
            stopped: false,
            first_stop: None,
            last_attempts: 0,
            ran_step: false,
        };
        let mut outcome = workflow(&mut run, &instance.input);
        // A `Stop` the workflow dropped and returned past still decides.
        if let Some(stop) = run.first_stop.clone() {
            outcome = Err(stop);
        }
        if let Err(Stop::Retry(e)) = &outcome {
            // The database, not the workflow: leave the counter, try later.
            return Ok(Err(e.clone()));
        }
        storage::exec(
            "UPDATE wasmkeep_wf SET unfinished_runs = 0 WHERE id = 1",
            &[],
        )?;
        match outcome {
            Ok(output) => {
                set_status(&State::Completed, Some(output), None)?;
                alarms::set_alarm(now_ms().saturating_add(config.retention_ms));
            }
            Err(Stop::Failed(e)) => {
                set_status(&State::Failed, None, Some(e))?;
                alarms::set_alarm(now_ms().saturating_add(config.retention_ms));
            }
            Err(Stop::Wait) => {
                // A wait with no time (the workflow returned `Stop::Wait`
                // itself) looks again after the longest backoff.
                let at = run
                    .wake
                    .unwrap_or_else(|| now_ms().saturating_add(config.backoff_max_ms));
                // Continuing at once (the run's time used up) is running;
                // anything later is waiting.
                let state = if at <= now_ms() {
                    State::Running
                } else {
                    State::Waiting
                };
                if instance.status != state {
                    set_status(&state, None, None)?;
                }
                alarms::set_alarm(at);
            }
            Err(Stop::Retry(_)) => {}
        }
        Ok(Ok(()))
    }

    /// Serves [`Client`]'s protocol. Requests from outside clients (without
    /// `x-wasmkeep-caller`) get `403`: only other actors start or query
    /// workflows.
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
            Some(caller) if allow(&caller) => match serve(config, &request) {
                Ok(answer) => answer,
                Err(e) => (500, format!("{}\n", e.message).into_bytes()),
            },
            _ => (403, b"only other actors may use this workflow\n".to_vec()),
        };
        // A request that did not start an instance leaves nothing behind
        // (the request created this actor if it did not exist).
        if status != 200 && matches!(load(), Ok(None)) {
            storage::delete_self();
        }
        let content_type = if status == 200 {
            "application/octet-stream"
        } else {
            "text/plain"
        };
        http::respond(out, status, content_type, &body);
    }

    fn unknown() -> (u16, Vec<u8>) {
        // `handle_allowing` deletes the empty actor this call created.
        (404, b"no such workflow instance\n".to_vec())
    }

    fn serve(config: &Config, request: &IncomingRequest) -> Result<(u16, Vec<u8>), SqlError> {
        let path = request.path_with_query().unwrap_or_default();
        let Some(rest) = path.strip_prefix(PREFIX) else {
            return Ok((404, b"not a workflow path\n".to_vec()));
        };
        let route = rest.split('?').next().unwrap_or_default();
        let now = now_ms();
        Ok(match (request.method(), route) {
            (Method::Post, "/start") => {
                let Some(input) = http::read_body(request, MAX_BODY) else {
                    return Ok((400, b"unreadable input\n".to_vec()));
                };
                match load()? {
                    None => {
                        // The alarm first: it is a row of this database, so
                        // the durable write below carries it too.
                        alarms::set_alarm(now);
                        let run_id = wire::hex(&crate::wasi::random::random::get_random_bytes(16));
                        storage::exec_durable(
                            "INSERT INTO wasmkeep_wf (id, status, run_id, input, created_at, updated_at) \
                             VALUES (1, 'running', ?, ?, ?, ?)",
                            &params![run_id, input, as_i64(now), as_i64(now)],
                        )?;
                    }
                    // A start retried after a lost answer finds the
                    // instance; if the alarm did not survive, arm it again.
                    Some(i) if !i.status.is_finished() && alarms::get_alarm().is_none() => {
                        alarms::set_alarm(now);
                    }
                    Some(_) => {}
                }
                let instance = load()?.map(|i| status(&i)).transpose()?;
                (200, instance.map(|s| encode_status(&s)).unwrap_or_default())
            }
            (Method::Get, "/status") => match load()? {
                Some(instance) => (200, encode_status(&status(&instance)?)),
                None => unknown(),
            },
            (Method::Post, "/event") => {
                let Some(name) = wire::query_param(rest, "name")
                    .and_then(wire::unhex)
                    .and_then(|n| String::from_utf8(n).ok())
                else {
                    return Ok((400, b"event name missing\n".to_vec()));
                };
                let Some(payload) = http::read_body(request, MAX_BODY) else {
                    return Ok((400, b"unreadable payload\n".to_vec()));
                };
                match load()? {
                    None => unknown(),
                    Some(i) if i.status.is_finished() => {
                        (409, b"the workflow instance has finished\n".to_vec())
                    }
                    Some(_) => {
                        // The alarm first, so the durable write carries it.
                        alarms::set_alarm(now);
                        storage::exec_durable(
                            "INSERT INTO wasmkeep_wf_event (name, payload, received_at) VALUES (?, ?, ?)",
                            &params![name, payload, as_i64(now)],
                        )?;
                        (200, Vec::new())
                    }
                }
            }
            (Method::Post, "/cancel") => match load()? {
                None => unknown(),
                Some(instance) => {
                    if !instance.status.is_finished() {
                        set_status(&State::Cancelled, None, None)?;
                        alarms::set_alarm(now.saturating_add(config.retention_ms));
                    }
                    let instance = load()?.unwrap_or(instance);
                    (200, encode_status(&status(&instance)?))
                }
            },
            _ => (404, b"not a workflow path\n".to_vec()),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_doubles_up_to_its_cap() {
        let c = Config::new().backoff(100, 1_000);
        assert_eq!(
            (1..=6).map(|a| c.delay(a)).collect::<Vec<_>>(),
            [100, 200, 400, 800, 1_000, 1_000]
        );
        assert_eq!(Config::new().delay(u32::MAX), 300_000);
    }

    #[test]
    fn stored_values_round_trip_and_refuse_other_shapes() {
        assert_eq!(String::decode(&"hé".to_owned().encode()), Some("hé".into()));
        assert_eq!(u64::decode(&7u64.encode()), Some(7));
        assert_eq!(i64::decode(&(-7i64).encode()), Some(-7));
        assert_eq!(bool::decode(&true.encode()), Some(true));
        assert_eq!(<()>::decode(&().encode()), Some(()));
        assert_eq!(u64::decode(b"abc"), None);
        assert_eq!(bool::decode(&[2]), None);
        assert_eq!(<()>::decode(b"x"), None);
    }

    #[test]
    fn status_round_trips() {
        let s = Status {
            state: State::Waiting,
            wake_at: Some(42),
            waiting_for: vec!["approved".into(), "paid".into()],
            output: None,
            error: None,
            positions: 3,
        };
        assert_eq!(decode_status(&encode_status(&s)), Some(s.clone()));
        // The bytes the TypeScript SDK's test checks too (workflow.test.ts).
        assert_eq!(
            wire::hex(&encode_status(&s)),
            "0000000777616974696e67000000000000002a0000000d617070726f7665640a70616964000000000000000000000000000000000000000000000003"
        );
        let done = Status {
            state: State::Completed,
            wake_at: None,
            waiting_for: vec![],
            output: Some(Vec::new()),
            error: None,
            positions: 5,
        };
        assert_eq!(decode_status(&encode_status(&done)), Some(done));
        assert_eq!(decode_status(b"junk"), None);
    }
}
