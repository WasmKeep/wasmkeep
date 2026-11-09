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
//! of twice that), [`NODE_RATE`] for the whole node, and [`MAX_LINE`] bytes
//! a line. Control characters are escaped, so a guest cannot start a line
//! of its own in the node's log or colour a terminal.

use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Instant;

use tokio::io::AsyncWrite;
use wasmtime_wasi::cli::{IsTerminal, StdoutStream};

/// Lines a second per instance unless the operator says otherwise.
pub const DEFAULT_RATE: u32 = 50;
/// Lines a second for every guest on the node together.
pub const NODE_RATE: u32 = 1_000;
/// Longest line logged, in bytes; the rest of a longer one is cut.
pub const MAX_LINE: usize = 4_096;

/// How much of what guests print reaches the node's log. Cheap to clone:
/// clones share the node-wide budget.
#[derive(Debug, Clone)]
pub struct GuestLog {
    rate: u32,
    node: Arc<Mutex<Bucket>>,
}

impl GuestLog {
    /// At most `rate` lines a second per instance (bursts of twice that),
    /// and [`NODE_RATE`] for the node; 0 discards everything, as WasmKeep
    /// did before guest output was logged.
    pub fn new(rate: u32) -> Self {
        Self {
            rate,
            node: Arc::new(Mutex::new(Bucket::new(NODE_RATE))),
        }
    }

    /// The WASI context for one instance of `who` (an actor's address, or
    /// `stateless`): [`crate::egress::guest_wasi`]'s, with its stdout and
    /// stderr logged.
    pub(crate) fn wasi(&self, who: String) -> wasmtime_wasi::WasiCtx {
        let mut builder = crate::egress::guest_wasi_builder();
        if self.rate > 0 {
            let output = Arc::new(Mutex::new(Output {
                who,
                pending: [Vec::new(), Vec::new()],
                bucket: Bucket::new(self.rate),
                node: Arc::clone(&self.node),
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
                actor = %self.who,
                dropped = self.dropped,
                "guest output dropped: over the line rate"
            );
            self.dropped = 0;
        }
        crate::metrics::inc(&crate::metrics::METRICS.guest_log_lines);
        let text = clean(line);
        tracing::info!(target: "wasmkeep::guest", actor = %self.who, stream = NAMES[which], "{text}");
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
                actor = %self.who,
                dropped = self.dropped,
                "guest output dropped: over the line rate"
            );
        }
    }
}

/// `line` as text for the log: invalid UTF-8 replaced, a trailing `\r`
/// dropped, control characters other than tab escaped (`\u{1b}`), and cut
/// at [`MAX_LINE`] bytes.
fn clean(line: &[u8]) -> String {
    let line = line.strip_suffix(b"\r").unwrap_or(line);
    let line = &line[..line.len().min(MAX_LINE)];
    let mut out = String::with_capacity(line.len());
    for c in String::from_utf8_lossy(line).chars() {
        if c.is_control() && c != '\t' {
            out.extend(c.escape_default());
        } else {
            out.push(c);
        }
    }
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
        assert_eq!(clean(b"plain\ttab"), "plain\ttab");
        assert_eq!(clean(b"line\r"), "line");
        assert_eq!(clean(b"a\rb\x1b[31mc"), "a\\rb\\u{1b}[31mc");
        assert_eq!(clean(&[b'x'; MAX_LINE + 10]).len(), MAX_LINE);
        assert_eq!(clean(b"bad \xff byte"), "bad \u{fffd} byte");
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
