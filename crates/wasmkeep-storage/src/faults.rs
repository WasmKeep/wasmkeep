//! Failure injection for tests (feature `fault-injection`, never in a
//! release build): makes a named host operation fail once, to test what
//! callers do with an I/O error between two writes. Also counts the local
//! syncs of durable statements, for tests of which callers ask for one.
//!
//! Points are process-wide, so a test arming one must run in its own
//! process: `cargo nextest` (the project's runner) does that. Under plain
//! `cargo test`, another test in the same binary may consume the fault.

use std::sync::Mutex;

static ARMED: Mutex<Vec<String>> = Mutex::new(Vec::new());
static DELAYS: Mutex<Vec<(String, std::time::Duration)>> = Mutex::new(Vec::new());
static HELD: Mutex<Vec<String>> = Mutex::new(Vec::new());

/// Whether an operation at `point` is being held by [`delay_next`] right
/// now: a test waits for this before changing things around it.
pub fn held(point: &str) -> bool {
    HELD.lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .iter()
        .any(|p| p == point)
}

/// Makes the next operation at `point` fail, once. Points:
/// `set_meta:<key>` (writing a host metadata value), `meta:<key>` (reading
/// one), `delete_meta:<key>` (removing one), `delete` (removing an actor's database files), `open_busy`
/// (opening an actor's database answers "busy"; arm it several times for
/// several attempts).
pub fn fail_next(point: &str) {
    ARMED
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .push(point.to_owned());
}

/// Makes the next operation at `point` wait `delay` first (on its blocking
/// thread), once: to hold an event at a known step while a test changes
/// something around it. Points: `open` (opening an actor's database),
/// `alarm_scan` (the startup scan for pending alarms, with a database open).
pub fn delay_next(point: &str, delay: std::time::Duration) {
    DELAYS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .push((point.to_owned(), delay));
}

/// Gates armed by [`hold_next`], and the points [`release`] opened.
static GATES: Mutex<Gates> = Mutex::new(Gates {
    armed: Vec::new(),
    released: Vec::new(),
});
static GATE_OPENED: std::sync::Condvar = std::sync::Condvar::new();

struct Gates {
    armed: Vec<String>,
    released: Vec<String>,
}

/// How long a gate holds at most, so a test that fails before releasing
/// it does not hang its process.
const GATE_LIMIT: std::time::Duration = std::time::Duration::from_secs(30);

/// Makes the next operation at `point` wait (on its blocking thread) until
/// [`release`] opens it, once: like [`delay_next`], but the test decides
/// when it goes on, not a timer. [`held`] tells when it is waiting. Points:
/// those of [`delay_next`].
pub fn hold_next(point: &str) {
    GATES
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .armed
        .push(point.to_owned());
}

/// Lets the operation [`hold_next`] holds at `point` go on (or the next
/// one to reach it, if none is held yet).
pub fn release(point: &str) {
    GATES
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .released
        .push(point.to_owned());
    GATE_OPENED.notify_all();
}

fn hold(point: &str) {
    let mut gates = GATES
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let Some(i) = gates.armed.iter().position(|p| p == point) else {
        return;
    };
    gates.armed.swap_remove(i);
    HELD.lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .push(point.to_owned());
    let (mut gates, _) = GATE_OPENED
        .wait_timeout_while(gates, GATE_LIMIT, |g| {
            !g.released.iter().any(|p| p == point)
        })
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if let Some(i) = gates.released.iter().position(|p| p == point) {
        gates.released.swap_remove(i);
    }
    drop(gates);
    let mut h = HELD
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if let Some(i) = h.iter().position(|p| p == point) {
        h.swap_remove(i);
    }
}

/// Waits, if a delay or a gate is armed at `point` (consuming it).
pub(crate) fn pause(point: &str) {
    hold(point);
    let delay = {
        let mut delays = DELAYS
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        delays
            .iter()
            .position(|(p, _)| p == point)
            .map(|i| delays.swap_remove(i).1)
    };
    if let Some(delay) = delay {
        let held = || {
            HELD.lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
        };
        held().push(point.to_owned());
        std::thread::sleep(delay);
        let mut h = held();
        if let Some(i) = h.iter().position(|p| p == point) {
            h.swap_remove(i);
        }
    }
}

/// Whether the operation at `point` must fail now (consuming the arm).
pub(crate) fn take(point: &str) -> bool {
    let mut armed = ARMED
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    match armed.iter().position(|p| p == point) {
        Some(i) => {
            armed.swap_remove(i);
            true
        }
        None => false,
    }
}

static LOCAL_SYNCS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// How many durable statements asked for a local fsync of their commit in
/// this process so far (`exec_durable`, not `exec_commit`).
pub fn local_syncs() -> u64 {
    LOCAL_SYNCS.load(std::sync::atomic::Ordering::SeqCst)
}

pub(crate) fn note_local_sync() {
    LOCAL_SYNCS.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
}
