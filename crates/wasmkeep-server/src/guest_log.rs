//! What guests write to stdout and stderr, into the node's log.
//!
//! Each instance gets its own stdout and stderr. Complete lines go to the
//! node's log as events of target `wasmkeep::guest`, with the actor's
//! address (or `stateless`) and the stream; a line without its newline yet
//! waits for the rest, and is logged when the instance ends. Writes never
//! block or fail the guest: past the limits, lines are dropped and counted,
//! and the next line logged says how many were.
//!
//! Limits: lines per second per instance (the operator's rate, with a burst
//! of twice that), [`APP_RATE`] for each app of the process, [`NODE_RATE`]
//! for the whole process, and [`MAX_LINE`] bytes a line, escapes included.
//!
//! The text is the line's last field, `line="..."`, after a fixed message
//! and the real fields; quotes and backslashes in it are escaped, and so
//! are controls, format characters (bidi, zero-width) and line separators
//! ([`crate::logging::push_escaped`]). So a guest cannot start a line of
//! its own in the node's log, add fields to its own line, reorder what a
//! terminal shows or colour it.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, Mutex, Weak};
use std::task::{Context, Poll};
use std::time::Instant;

use tokio::io::AsyncWrite;
use wasmtime_wasi::cli::{IsTerminal, StdoutStream};

/// Lines a second per instance unless the operator says otherwise.
pub const DEFAULT_RATE: u32 = 50;
/// Lines a second for every guest of one app together: each app of the
/// process has its own, so a chatty app cannot use up another's.
pub const APP_RATE: u32 = 1_000;
/// Lines a second for every guest of the process together, over the
/// apps' own budgets.
pub const NODE_RATE: u32 = 5 * APP_RATE;
/// Longest line logged, in bytes, quoted and escaped; the rest of a longer
/// one is cut.
pub const MAX_LINE: usize = 4_096;

type Shared = Arc<Mutex<Bucket>>;

/// How much of what guests print reaches the node's log. Cheap to clone:
/// clones share the budgets.
#[derive(Debug, Clone)]
pub struct GuestLog {
    rate: u32,
    budgets: Arc<Budgets>,
    /// The budget of the app this clone was made for, `(app, budget)`.
    held: Option<Arc<(String, Shared)>>,
}

/// What every clone shares.
#[derive(Debug)]
struct Budgets {
    node: Shared,
    /// Each app's budget (`tenant/app`). The app's runtime holds it
    /// ([`GuestLog::for_app`]); it goes when that runtime does.
    apps: Mutex<HashMap<String, Weak<Mutex<Bucket>>>>,
}

impl GuestLog {
    /// At most `rate` lines a second per instance (bursts of twice that),
    /// [`APP_RATE`] for each app and [`NODE_RATE`] for the process; 0
    /// discards everything, as WasmKeep did before guest output was logged.
    pub fn new(rate: u32) -> Self {
        Self {
            rate,
            budgets: Arc::new(Budgets {
                node: Arc::new(Mutex::new(Bucket::new(NODE_RATE))),
                apps: Mutex::default(),
            }),
            held: None,
        }
    }

    /// This log for the runtime of `app` (`tenant/app`): the clone holds
    /// the app's budget, so its instances share one budget for as long as
    /// the runtime lives, however short-lived they are.
    pub(crate) fn for_app(&self, app: &str) -> Self {
        let bucket = self.app_bucket(app);
        Self {
            held: Some(Arc::new((app.to_owned(), bucket))),
            ..self.clone()
        }
    }

    /// The budgets an instance of `app` takes from.
    fn budget(&self, app: &str) -> Budget {
        Budget {
            app: self.app_bucket(app),
            node: Arc::clone(&self.budgets.node),
        }
    }

    /// The budget every instance of `app` shares, while something holds it.
    fn app_bucket(&self, app: &str) -> Shared {
        if let Some(held) = &self.held
            && held.0 == app
        {
            return Arc::clone(&held.1);
        }
        let mut apps = self
            .budgets
            .apps
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(bucket) = apps.get(app).and_then(Weak::upgrade) {
            return bucket;
        }
        // The apps that went with their runtimes.
        apps.retain(|_, bucket| bucket.strong_count() > 0);
        let bucket = Arc::new(Mutex::new(Bucket::new(APP_RATE)));
        apps.insert(app.to_owned(), Arc::downgrade(&bucket));
        bucket
    }

    /// How many apps have a budget (for tests).
    #[cfg(test)]
    fn apps_known(&self) -> usize {
        let mut apps = self.budgets.apps.lock().unwrap();
        apps.retain(|_, bucket| bucket.strong_count() > 0);
        apps.len()
    }

    /// The WASI context for one instance of `who` (an actor as its app
    /// names it, `class/name` as [`crate::logging::actor`] shows it, or
    /// `_front`) of `app` (`tenant/app`): [`crate::egress::guest_wasi`]'s,
    /// with its stdout and stderr logged.
    pub(crate) fn wasi(&self, app: String, who: String) -> wasmtime_wasi::WasiCtx {
        let mut builder = crate::egress::guest_wasi_builder();
        if self.rate > 0 {
            let shared = self.budget(&app);
            let output = Arc::new(Mutex::new(Output {
                app,
                who,
                pending: [Vec::new(), Vec::new()],
                bucket: Bucket::new(self.rate),
                shared,
                dropped: 0,
            }));
            builder
                .stdout(Stream {
                    output: Arc::clone(&output),
                    which: STDOUT,
                })
                .stderr(Stream {
                    output,
                    which: STDERR,
                });
        }
        builder.build()
    }
}

impl Default for GuestLog {
    fn default() -> Self {
        Self::new(DEFAULT_RATE)
    }
}

const STDOUT: usize = 0;
const STDERR: usize = 1;
const NAMES: [&str; 2] = ["stdout", "stderr"];

/// A token bucket: `rate` a second, up to twice that saved up.
#[derive(Debug)]
struct Bucket {
    rate: f64,
    tokens: f64,
    at: Instant,
}

impl Bucket {
    fn new(rate: u32) -> Self {
        let rate = f64::from(rate.max(1));
        Self {
            rate,
            tokens: rate * 2.0,
            at: now(),
        }
    }

    fn take(&mut self) -> bool {
        let now = now();
        let refill = now.duration_since(self.at).as_secs_f64() * self.rate;
        self.tokens = (self.tokens + refill).min(self.rate * 2.0);
        self.at = now;
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}

/// The buckets' clock.
#[cfg(not(test))]
fn now() -> Instant {
    Instant::now()
}

#[cfg(test)]
thread_local! {
    /// A test's stopped clock ([`tests::stop_clock`]): the buckets refill
    /// only when the test moves it, however long its loops take. Unset,
    /// the real clock.
    static CLOCK: std::cell::Cell<Option<Instant>> = const { std::cell::Cell::new(None) };
}

/// The buckets' clock: in tests, the stopped one if the test set it.
#[cfg(test)]
fn now() -> Instant {
    CLOCK
        .with(std::cell::Cell::get)
        .unwrap_or_else(Instant::now)
}

/// One instance's output: what is waiting for a newline, on each stream.
struct Output {
    app: String,
    who: String,
    pending: [Vec<u8>; 2],
    bucket: Bucket,
    shared: Budget,
    dropped: u64,
}

/// The budgets an instance shares with others: its app's, then the
/// process's. In that order, so an app over its own budget never takes
/// from the others' share of the process's.
struct Budget {
    app: Shared,
    node: Shared,
}

impl Budget {
    fn take(&self) -> bool {
        let take = |bucket: &Shared| {
            bucket
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take()
        };
        take(&self.app) && take(&self.node)
    }
}

impl Output {
    fn write(&mut self, which: usize, bytes: &[u8]) {
        self.pending[which].extend_from_slice(bytes);
        while let Some(end) = self.pending[which].iter().position(|&b| b == b'\n') {
            let line: Vec<u8> = self.pending[which].drain(..=end).collect();
            self.emit(which, &line[..end]);
        }
        // A line that never ends is logged in pieces.
        while self.pending[which].len() > MAX_LINE {
            let piece: Vec<u8> = self.pending[which].drain(..MAX_LINE).collect();
            self.emit(which, &piece);
        }
    }

    fn emit(&mut self, which: usize, line: &[u8]) {
        let allowed = self.bucket.take() && self.shared.take();
        if !allowed {
            self.dropped += 1;
            crate::metrics::inc(&crate::metrics::METRICS.guest_log_dropped);
            return;
        }
        if self.dropped > 0 {
            tracing::warn!(
                target: "wasmkeep::guest",
                app = %self.app,
                actor = %self.who,
                dropped = self.dropped,
                "guest output dropped: over the line rate"
            );
            self.dropped = 0;
        }
        crate::metrics::inc(&crate::metrics::METRICS.guest_log_lines);
        let text = clean(line);
        tracing::info!(
            target: "wasmkeep::guest",
            app = %self.app,
            actor = %self.who,
            stream = NAMES[which],
            line = %text,
            "guest output"
        );
    }
}

impl Drop for Output {
    /// The instance ended: what waits for a newline is logged as it is.
    fn drop(&mut self) {
        for which in [STDOUT, STDERR] {
            let rest = std::mem::take(&mut self.pending[which]);
            if !rest.is_empty() {
                self.emit(which, &rest);
            }
        }
        if self.dropped > 0 {
            tracing::warn!(
                target: "wasmkeep::guest",
                app = %self.app,
                actor = %self.who,
                dropped = self.dropped,
                "guest output dropped: over the line rate"
            );
        }
    }
}

/// `line` as the log's quoted value: invalid UTF-8 replaced, a trailing
/// `\r` dropped, escaped ([`crate::logging::push_escaped`]: `\u{1b}`,
/// `\"`), and cut so that the whole value, quotes included, is at most
/// [`MAX_LINE`] bytes; the cut falls between characters, never inside an
/// escape.
fn clean(line: &[u8]) -> String {
    let line = line.strip_suffix(b"\r").unwrap_or(line);
    // Escaping never shortens text: nothing past this is shown.
    let line = &line[..line.len().min(MAX_LINE)];
    let room = MAX_LINE - 1; // the closing quote
    let mut out = String::with_capacity(line.len() + 2);
    out.push('"');
    let mut one = String::new();
    for c in String::from_utf8_lossy(line).chars() {
        one.clear();
        crate::logging::push_escaped(&mut one, c);
        if out.len() + one.len() > room {
            break;
        }
        out.push_str(&one);
    }
    out.push('"');
    out
}

/// One of an instance's two streams.
struct Stream {
    output: Arc<Mutex<Output>>,
    which: usize,
}

impl IsTerminal for Stream {
    fn is_terminal(&self) -> bool {
        false
    }
}

impl StdoutStream for Stream {
    fn async_stream(&self) -> Box<dyn AsyncWrite + Send + Sync> {
        Box::new(Writer {
            output: Arc::clone(&self.output),
            which: self.which,
        })
    }
}

/// A handle the guest writes through; every handle of a stream shares its
/// pending line.
struct Writer {
    output: Arc<Mutex<Output>>,
    which: usize,
}

impl AsyncWrite for Writer {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        self.output
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .write(self.which, buf);
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    /// Stops this thread's bucket clock: a slow runner's loop no longer
    /// refills a bucket while the test counts what it gives.
    fn stop_clock() {
        CLOCK.with(|c| c.set(Some(Instant::now())));
    }

    /// Moves the stopped clock on by `by`.
    fn advance(by: Duration) {
        CLOCK.with(|c| c.set(c.get().map(|at| at + by)));
    }

    #[test]
    fn control_characters_are_escaped_and_lines_cut() {
        assert_eq!(clean(b"plain\ttab"), "\"plain\\ttab\"");
        assert_eq!(clean(b"line\r"), "\"line\"");
        assert_eq!(clean(b"a\rb\x1b[31mc"), "\"a\\rb\\u001b[31mc\"");
        assert_eq!(clean(&[b'x'; MAX_LINE + 10]).len(), MAX_LINE);
        assert_eq!(clean(b"bad \xff byte"), "\"bad \u{fffd} byte\"");
    }

    #[test]
    fn format_characters_and_line_separators_are_escaped() {
        let text = clean("a\u{202e}b\u{2066}c\u{2028}d\u{2029}e\u{200b}f\u{ad}g".as_bytes());
        for c in [
            '\u{202e}', '\u{2066}', '\u{2028}', '\u{2029}', '\u{200b}', '\u{ad}',
        ] {
            assert!(!text.contains(c), "{c:?} passed through: {text:?}");
        }
        assert!(
            text.contains("\\u202e") && text.contains("\\u2028"),
            "{text}"
        );
    }

    #[test]
    fn the_escaped_line_fits_the_limit() {
        // Each byte of these escapes to several: the limit is on the output.
        let escapes = clean(&[0x1b; MAX_LINE]);
        assert!(escapes.len() <= MAX_LINE, "{}", escapes.len());
        let bidi = clean("\u{202e}".repeat(MAX_LINE).as_bytes());
        assert!(bidi.len() <= MAX_LINE, "{}", bidi.len());
        // Cut between characters and escapes, never inside one.
        assert!(escapes.ends_with("\\u001b\""), "{escapes}");
        assert!(bidi.ends_with("\\u202e\""), "{bidi}");
        let wide = clean("\u{e9}".repeat(MAX_LINE).as_bytes());
        assert!(wide.len() <= MAX_LINE, "{}", wide.len());
        // Above U+FFFF, a surrogate pair, never half of one.
        let tag = clean("\u{e0001}".repeat(MAX_LINE).as_bytes());
        assert!(tag.len() <= MAX_LINE, "{}", tag.len());
        assert!(tag.ends_with("\\udb40\\udc01\""), "{tag}");
    }

    #[test]
    fn the_text_cannot_close_its_quotes() {
        assert_eq!(
            clean(br#"hi" level=error x=\"#),
            r#""hi\" level=error x=\\""#
        );
    }

    #[test]
    fn each_app_has_its_own_budget() {
        stop_clock();
        let log = GuestLog::new(1);
        let (a, b) = (log.for_app("t/a"), log.for_app("t/b"));
        let first = a.budget("t/a");
        let taken = (0..10_000).filter(|_| first.take()).count();
        assert_eq!(taken, APP_RATE as usize * 2);
        // A later instance of the app, or its front, shares that budget.
        assert!(!a.budget("t/a").take());
        assert!(!log.budget("t/a").take());
        let other = b.budget("t/b");
        let taken = (0..10_000).filter(|_| other.take()).count();
        assert_eq!(taken, APP_RATE as usize * 2);
        // A second later, the app has a second's worth again.
        advance(Duration::from_secs(1));
        let again = a.budget("t/a");
        let taken = (0..10_000).filter(|_| again.take()).count();
        assert_eq!(taken, APP_RATE as usize);
    }

    #[test]
    fn the_node_caps_every_app_together() {
        stop_clock();
        let log = GuestLog::new(1);
        let apps: Vec<_> = (0..10).map(|i| log.for_app(&format!("t/a{i}"))).collect();
        let taken: usize = apps
            .iter()
            .enumerate()
            .map(|(i, app)| {
                let budget = app.budget(&format!("t/a{i}"));
                (0..10_000).filter(|_| budget.take()).count()
            })
            .sum();
        // The node's burst: far under the ten apps' own budgets together.
        assert_eq!(taken, NODE_RATE as usize * 2);
    }

    #[test]
    fn an_apps_budget_goes_with_its_runtime() {
        let log = GuestLog::new(1);
        let app = log.for_app("t/gone");
        let instance = app.budget("t/gone");
        assert_eq!(log.apps_known(), 1);
        drop((app, instance));
        assert_eq!(log.apps_known(), 0);
    }

    #[test]
    fn the_bucket_allows_a_burst_then_the_rate() {
        stop_clock();
        let mut bucket = Bucket::new(5);
        let burst = (0..100).filter(|_| bucket.take()).count();
        assert_eq!(burst, 10);
        advance(Duration::from_secs(1));
        let after = (0..100).filter(|_| bucket.take()).count();
        assert_eq!(after, 5);
    }
}
