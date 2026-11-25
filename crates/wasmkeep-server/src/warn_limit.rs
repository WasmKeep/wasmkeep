//! A bound on WARN lines a client can cause at will: the front logs one
//! per failed request, and a route that traps would otherwise fill the
//! node's log as fast as it is asked for. Per reason, the first
//! [`BURST`] lines of an [`INTERVAL`] are logged; the rest are counted,
//! and one line says how many when the interval ends. The metrics count
//! every request either way.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Lines logged per reason in one [`INTERVAL`].
pub(crate) const BURST: u32 = 10;
/// The window [`BURST`] is counted over.
pub(crate) const INTERVAL: Duration = Duration::from_secs(60);
/// Longest error text on one line, in bytes: a wasm backtrace is cut.
pub(crate) const MAX_ERROR: usize = 2_048;
/// What ends an error text that was cut.
const CUT: &str = "... (cut)";

/// One reason's current window.
#[derive(Debug)]
struct Window {
    start: Instant,
    logged: u32,
    suppressed: u64,
}

/// What [`Windows::admit`] says of one line.
#[derive(Debug, PartialEq, Eq)]
enum Admit {
    /// Log it, after a summary of the `earlier` lines an ended interval
    /// suppressed (0: none, or already told).
    Log { earlier: u64 },
    /// Count it only. `first`: the first of the interval begun at
    /// `start`, whose summary is not scheduled yet.
    Suppress { first: bool, start: Instant },
}

/// The windows, by reason. The clock is the caller's, so a test has its own.
#[derive(Debug, Default)]
struct Windows(HashMap<&'static str, Window>);

impl Windows {
    fn admit(&mut self, reason: &'static str, now: Instant) -> Admit {
        let w = self.0.entry(reason).or_insert(Window {
            start: now,
            logged: 0,
            suppressed: 0,
        });
        let mut earlier = 0;
        if now.duration_since(w.start) >= INTERVAL {
            earlier = std::mem::take(&mut w.suppressed);
            w.start = now;
            w.logged = 0;
        }
        if w.logged < BURST {
            w.logged += 1;
            Admit::Log { earlier }
        } else {
            w.suppressed += 1;
            Admit::Suppress {
                first: w.suppressed == 1,
                start: w.start,
            }
        }
    }

    /// The lines suppressed in the interval begun at `start`, once: 0 when
    /// a later line already told them.
    fn take(&mut self, reason: &'static str, start: Instant) -> u64 {
        match self.0.get_mut(reason) {
            Some(w) if w.start == start => std::mem::take(&mut w.suppressed),
            _ => 0,
        }
    }
}

/// The limit of one front's WARN lines. `what` starts the summary line
/// (`front request failed`).
#[derive(Debug)]
pub(crate) struct WarnLimit {
    what: &'static str,
    windows: Mutex<Windows>,
}

impl WarnLimit {
    pub(crate) fn new(what: &'static str) -> Arc<Self> {
        Arc::new(Self {
            what,
            windows: Mutex::default(),
        })
    }

    fn locked(&self) -> std::sync::MutexGuard<'_, Windows> {
        self.windows.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// Whether to log one more line for `reason`. Logs the summary of an
    /// ended interval first, or has it logged when this one ends. `app`
    /// is the app's metric labels, for the summary.
    pub(crate) fn allow(self: &Arc<Self>, app: &Arc<str>, reason: &'static str) -> bool {
        // tokio's clock, as the timer's: a test pauses both.
        let now = tokio::time::Instant::now().into_std();
        let admit = self.locked().admit(reason, now);
        match admit {
            Admit::Log { earlier } => {
                self.summary(app, reason, earlier);
                true
            }
            Admit::Suppress { first, start } => {
                if first && let Ok(runtime) = tokio::runtime::Handle::try_current() {
                    let (limit, app) = (Arc::clone(self), Arc::clone(app));
                    runtime.spawn(async move {
                        tokio::time::sleep_until((start + INTERVAL).into()).await;
                        let suppressed = limit.locked().take(reason, start);
                        limit.summary(&app, reason, suppressed);
                    });
                }
                false
            }
        }
    }

    fn summary(&self, app: &str, reason: &'static str, suppressed: u64) {
        if suppressed > 0 {
            tracing::warn!(
                app,
                "{}: suppressed {suppressed} more (reason={reason}) in the last {} s",
                self.what,
                INTERVAL.as_secs()
            );
        }
    }
}

/// `e` with its causes, at most [`MAX_ERROR`] bytes of it.
pub(crate) fn error_chain(e: &wasmtime::Error) -> String {
    let mut text = format!("{e:#}");
    if text.len() > MAX_ERROR {
        let mut end = MAX_ERROR;
        while !text.is_char_boundary(end) {
            end -= 1;
        }
        text.truncate(end);
        text.push_str(CUT);
    }
    text
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_burst_is_logged_then_counted_then_summed_up() {
        let mut windows = Windows::default();
        let t0 = Instant::now();
        for i in 0..BURST {
            assert_eq!(
                windows.admit("trap", t0 + Duration::from_millis(u64::from(i))),
                Admit::Log { earlier: 0 }
            );
        }
        // The rest of the interval is counted; only the first asks for
        // the summary.
        let late = t0 + INTERVAL - Duration::from_millis(1);
        assert_eq!(
            windows.admit("trap", late),
            Admit::Suppress {
                first: true,
                start: t0
            }
        );
        for _ in 0..4 {
            assert_eq!(
                windows.admit("trap", late),
                Admit::Suppress {
                    first: false,
                    start: t0
                }
            );
        }
        // Another reason has its own lines.
        assert_eq!(windows.admit("wall", late), Admit::Log { earlier: 0 });

        // The interval over: the next line is logged, after the count.
        let t1 = t0 + INTERVAL;
        assert_eq!(windows.admit("trap", t1), Admit::Log { earlier: 5 });
        // Told once: the timer of the ended interval finds nothing.
        assert_eq!(windows.take("trap", t0), 0);
        for _ in 1..BURST {
            assert_eq!(windows.admit("trap", t1), Admit::Log { earlier: 0 });
        }
        assert_eq!(
            windows.admit("trap", t1),
            Admit::Suppress {
                first: true,
                start: t1
            }
        );
    }

    #[test]
    fn the_timer_takes_the_count_when_no_line_follows() {
        let mut windows = Windows::default();
        let t0 = Instant::now();
        for _ in 0..BURST + 3 {
            windows.admit("trap", t0);
        }
        assert_eq!(windows.take("trap", t0), 3);
        assert_eq!(windows.take("trap", t0), 0);
        // The next interval starts clean, with nothing to tell.
        assert_eq!(
            windows.admit("trap", t0 + INTERVAL),
            Admit::Log { earlier: 0 }
        );
    }

    /// The whole path, on a paused clock: ten lines allowed, then one
    /// summary when the interval ends, then lines again.
    #[tokio::test(start_paused = true)]
    async fn the_summary_is_logged_when_the_interval_ends() {
        #[derive(Clone, Default)]
        struct Logs(Arc<Mutex<Vec<u8>>>);
        impl std::io::Write for Logs {
            fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                self.0.lock().unwrap().extend_from_slice(buf);
                Ok(buf.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let logs = Logs::default();
        let writer = logs.clone();
        let subscriber = tracing_subscriber::fmt()
            .with_ansi(false)
            .with_writer(move || writer.clone())
            .finish();
        let _guard = tracing::subscriber::set_default(subscriber);
        let limit = WarnLimit::new("front request failed");
        let app: Arc<str> = Arc::from("tenant=\"default\",app=\"a\"");
        let allowed = (0..BURST + 7).filter(|_| limit.allow(&app, "trap")).count();
        assert_eq!(allowed, BURST as usize);
        assert!(logs.0.lock().unwrap().is_empty());
        tokio::time::sleep(INTERVAL + Duration::from_secs(1)).await;
        let text = String::from_utf8(logs.0.lock().unwrap().clone()).unwrap();
        assert_eq!(text.matches("suppressed").count(), 1, "{text}");
        assert!(
            text.contains("front request failed: suppressed 7 more (reason=trap) in the last 60 s"),
            "{text}"
        );
        assert!(text.contains("WARN"), "{text}");
        assert!(limit.allow(&app, "trap"));
    }

    #[test]
    fn a_long_error_is_cut_on_a_character() {
        let short = wasmtime::format_err!("inner").context("outer");
        assert_eq!(error_chain(&short), "outer: inner");
        // Two bytes a character, from an odd offset: the cap falls inside one.
        let long = wasmtime::format_err!("x{}", "é".repeat(MAX_ERROR));
        let text = error_chain(&long);
        assert_eq!(text.len(), MAX_ERROR - 1 + CUT.len());
        assert!(text.ends_with("é... (cut)"));
    }
}
