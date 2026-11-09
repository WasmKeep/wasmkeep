//! Failure injection for tests (feature `fault-injection`, never in a
//! release build): makes a named host operation fail once, to test what
//! callers do with an I/O error between two writes.
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
/// one), `delete` (removing an actor's database files), `open_busy`
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
/// something around it. Points: `open` (opening an actor's database).
pub fn delay_next(point: &str, delay: std::time::Duration) {
    DELAYS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .push((point.to_owned(), delay));
}

/// Waits, if a delay is armed at `point` (consuming it).
pub(crate) fn pause(point: &str) {
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
