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
//! of twice that), [`APP_RATE`] for each app of the process, and
//! [`MAX_LINE`] bytes a line, escapes included.
//!
//! The text is the line's last field, `line="..."`, after a fixed message
//! and the real fields; quotes and backslashes in it are escaped, and so
//! are controls, format characters (bidi, zero-width) and line separators
//! ([`crate::logging::push_escaped`]). So a guest cannot start a line of
//! its own in the node's log, add fields to its own line, reorder what a
//! terminal shows or colour it.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Instant;

use tokio::io::AsyncWrite;
use wasmtime_wasi::cli::{IsTerminal, StdoutStream};

/// Lines a second per instance unless the operator says otherwise.
pub const DEFAULT_RATE: u32 = 50;
/// Lines a second for every guest of one app together: each app of the
/// process has its own, so a chatty app cannot use up another's.
pub const APP_RATE: u32 = 1_000;
/// Longest line logged, in bytes, quoted and escaped; the rest of a longer
/// one is cut.
pub const MAX_LINE: usize = 4_096;

/// How much of what guests print reaches the node's log. Cheap to clone:
/// clones share the apps' budgets.
#[derive(Debug, Clone)]
pub struct GuestLog {
    rate: u32,
    /// Each app's budget (`tenant/app`), made on its first instance.
    apps: Arc<Mutex<HashMap<String, Arc<Mutex<Bucket>>>>>,
}

impl GuestLog {
    /// At most `rate` lines a second per instance (bursts of twice that),
    /// and [`APP_RATE`] for each app; 0 discards everything, as WasmKeep
    /// did before guest output was logged.
    pub fn new(rate: u32) -> Self {
        Self {
            rate,
            apps: Arc::default(),
        }
    }

    /// The budget every instance of `app` shares.
    fn app_bucket(&self, app: &str) -> Arc<Mutex<Bucket>> {
        let mut apps = self
            .apps
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(bucket) = apps.get(app) {
            return Arc::clone(bucket);
        }
        let bucket = Arc::new(Mutex::new(Bucket::new(APP_RATE)));
        apps.insert(app.to_owned(), Arc::clone(&bucket));
        bucket
    }

    /// The WASI context for one instance of `who` (an actor as its app
    /// names it, `class/name` as [`crate::logging::actor`] shows it, or
    /// `_front`) of `app` (`tenant/app`): [`crate::egress::guest_wasi`]'s,
    /// with its stdout and stderr logged.
    pub(crate) fn wasi(&self, app: String, who: String) -> wasmtime_wasi::WasiCtx {
        let mut builder = crate::egress::guest_wasi_builder();
        if self.rate > 0 {
            let node = self.app_bucket(&app);
            let output = Arc::new(Mutex::new(Output {
                app,
                who,
                pending: [Vec::new(), Vec::new()],
                bucket: Bucket::new(self.rate),
                node,
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
            at: Instant::now(),
        }
    }

    fn take(&mut self) -> bool {
        let now = Instant::now();
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

/// One instance's output: what is waiting for a newline, on each stream.
struct Output {
    app: String,
    who: String,
    pending: [Vec<u8>; 2],
    bucket: Bucket,
    node: Arc<Mutex<Bucket>>,
    dropped: u64,
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
        let allowed = self.bucket.take()
            && self
                .node
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take();
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

    #[test]
    fn control_characters_are_escaped_and_lines_cut() {
        assert_eq!(clean(b"plain\ttab"), "\"plain\ttab\"");
        assert_eq!(clean(b"line\r"), "\"line\"");
        assert_eq!(clean(b"a\rb\x1b[31mc"), "\"a\\rb\\u{1b}[31mc\"");
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
            text.contains("\\u{202e}") && text.contains("\\u{2028}"),
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
        assert!(escapes.ends_with("\\u{1b}\""), "{escapes}");
        assert!(bidi.ends_with("\\u{202e}\""), "{bidi}");
        let wide = clean("\u{e9}".repeat(MAX_LINE).as_bytes());
        assert!(wide.len() <= MAX_LINE, "{}", wide.len());
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
        let log = GuestLog::new(1);
        let a = log.app_bucket("t/a");
        let taken = (0..10_000).filter(|_| a.lock().unwrap().take()).count();
        assert_eq!(taken, APP_RATE as usize * 2);
        assert!(!log.app_bucket("t/a").lock().unwrap().take());
        assert!(log.app_bucket("t/b").lock().unwrap().take());
    }

    #[test]
    fn the_bucket_allows_a_burst_then_the_rate() {
        let mut bucket = Bucket::new(5);
        let burst = (0..100).filter(|_| bucket.take()).count();
        assert_eq!(burst, 10);
        bucket.at -= std::time::Duration::from_secs(1);
        let after = (0..100).filter(|_| bucket.take()).count();
        assert_eq!(after, 5);
    }
}
