//! The `wasi:io` streams of `open-read` and `open-write`.
//!
//! A read streams from the bucket's response or the file on disk. A write
//! is spooled to a file on the node's disk, bounded by `--max-blob-bytes`,
//! and becomes the file only at `finish` (ADR 0030, section 3). Both stop
//! when their event ends: the host flips their [`Alive`] token at the next
//! event, and an upload still open is abandoned then.
//!
//! Every spooled byte is charged to the app's and the node's spool
//! [`Budget`] before it reaches the disk, and given back when the spool
//! file goes (abandoned, failed, or committed).

use std::io::{Read as _, Seek as _, Write as _};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use futures_util::StreamExt as _;
use futures_util::stream::BoxStream;
use wasmkeep_wit::wasmkeep::actor::blobs::BlobError;
use wasmtime_wasi::p2::{InputStream, OutputStream, Pollable, StreamError, StreamResult};

use super::unavailable;

/// Whether the event a stream or upload was opened in is still running.
pub(crate) type Alive = Arc<AtomicBool>;

/// How much a read hands the guest at once, and how much a write takes
/// before the guest must wait for it to reach the spool file.
const CHUNK: usize = 64 * 1024;
const WRITE_BUFFER: usize = 1024 * 1024;

/// Bytes spool files may hold at once: an app's share, or the node's.
#[derive(Debug)]
pub(crate) struct Budget {
    limit: AtomicU64,
    used: AtomicU64,
}

impl Budget {
    pub(crate) const fn new(limit: u64) -> Self {
        Self {
            limit: AtomicU64::new(limit),
            used: AtomicU64::new(0),
        }
    }

    /// Lets the budget hold at least `limit`: one file of a runtime's
    /// largest always fits when nothing else is spooling.
    pub(crate) fn raise_to(&self, limit: u64) {
        self.limit.fetch_max(limit, Ordering::AcqRel);
    }

    /// Bytes charged now.
    #[cfg(test)]
    pub(crate) fn used(&self) -> u64 {
        self.used.load(Ordering::Acquire)
    }

    fn take(&self, n: u64) -> bool {
        let limit = self.limit.load(Ordering::Acquire);
        self.used
            .try_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                used.checked_add(n).filter(|&total| total <= limit)
            })
            .is_ok()
    }

    fn give(&self, n: u64) {
        let _ = self
            .used
            .try_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                Some(used.saturating_sub(n))
            });
    }
}

/// What one spool holds of its app's and the node's budgets, given back
/// when dropped.
#[derive(Debug)]
pub(crate) struct Held {
    app: Arc<Budget>,
    node: Arc<Budget>,
    bytes: u64,
}

impl Held {
    /// Nothing held yet, of `app` and `node`.
    pub(crate) fn none(app: Arc<Budget>, node: Arc<Budget>) -> Self {
        Self {
            app,
            node,
            bytes: 0,
        }
    }

    /// Holds `total` bytes in all, taking what is missing from both
    /// budgets; `false` (and nothing taken) when either is out.
    pub(crate) fn hold(&mut self, total: u64) -> bool {
        let Some(more) = total.checked_sub(self.bytes).filter(|&n| n > 0) else {
            return true;
        };
        if !self.node.take(more) {
            return false;
        }
        if !self.app.take(more) {
            self.node.give(more);
            return false;
        }
        self.bytes = total;
        true
    }
}

impl Drop for Held {
    fn drop(&mut self) {
        self.app.give(self.bytes);
        self.node.give(self.bytes);
    }
}

/// Where a read's bytes come from.
pub(crate) enum Source {
    Bucket(BoxStream<'static, object_store::Result<Bytes>>),
    Disk { file: std::fs::File, left: u64 },
    Empty,
}

impl std::fmt::Debug for Source {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Bucket(_) => "Source::Bucket",
            Self::Disk { .. } => "Source::Disk",
            Self::Empty => "Source::Empty",
        })
    }
}

impl Source {
    /// The bytes `[start, start + length)` of an open file on disk.
    pub(crate) fn disk(mut file: std::fs::File, start: u64, length: u64) -> std::io::Result<Self> {
        if length == 0 {
            return Ok(Self::Empty);
        }
        file.seek(std::io::SeekFrom::Start(start))?;
        Ok(Self::Disk { file, left: length })
    }
}

/// A file's bytes as an `input-stream`.
pub(crate) struct ReadStream {
    source: Option<Source>,
    buffer: Bytes,
    failed: Option<String>,
    alive: Alive,
}

impl ReadStream {
    pub(crate) fn new(source: Source, alive: Alive) -> Self {
        Self {
            source: Some(source),
            buffer: Bytes::new(),
            failed: None,
            alive,
        }
    }

    async fn fill(&mut self) {
        match self.source.take() {
            None | Some(Source::Empty) => {}
            Some(Source::Bucket(mut s)) => match s.next().await {
                Some(Ok(b)) => {
                    self.buffer = b;
                    self.source = Some(Source::Bucket(s));
                }
                Some(Err(e)) => self.failed = Some(e.to_string()),
                None => {}
            },
            Some(Source::Disk { mut file, left }) => {
                let want = (left as usize).min(CHUNK);
                let read = tokio::task::spawn_blocking(move || {
                    let mut buf = vec![0u8; want];
                    let n = file.read(&mut buf)?;
                    buf.truncate(n);
                    Ok::<_, std::io::Error>((file, buf))
                })
                .await;
                match read {
                    Ok(Ok((_, buf))) if buf.is_empty() => {
                        self.failed = Some("the file ended early".into());
                    }
                    Ok(Ok((file, buf))) => {
                        let left = left - buf.len() as u64;
                        self.buffer = Bytes::from(buf);
                        if left > 0 {
                            self.source = Some(Source::Disk { file, left });
                        }
                    }
                    Ok(Err(e)) => self.failed = Some(e.to_string()),
                    Err(e) => self.failed = Some(e.to_string()),
                }
            }
        }
    }
}

#[wasmtime_wasi::async_trait]
impl Pollable for ReadStream {
    async fn ready(&mut self) {
        if self.buffer.is_empty() && self.failed.is_none() && self.alive.load(Ordering::Acquire) {
            self.fill().await;
        }
    }
}

#[wasmtime_wasi::async_trait]
impl InputStream for ReadStream {
    fn read(&mut self, size: usize) -> StreamResult<Bytes> {
        if !self.alive.load(Ordering::Acquire) {
            return Err(StreamError::Closed);
        }
        if !self.buffer.is_empty() {
            let n = size.min(self.buffer.len());
            return Ok(self.buffer.split_to(n));
        }
        if let Some(e) = &self.failed {
            return Err(StreamError::LastOperationFailed(wasmtime::format_err!(
                "reading the file failed: {e}"
            )));
        }
        match self.source {
            None => Err(StreamError::Closed),
            Some(_) => Ok(Bytes::new()),
        }
    }
}

/// Why a spooled write cannot become a file.
#[derive(Debug, Clone)]
enum Fail {
    TooLarge,
    /// The app's or the node's spool budget is out.
    Budget,
    Io(String),
}

const NO_SPACE: &str = "the node has no spool space for this write now; retry later";

#[derive(Debug)]
struct SpoolState {
    path: PathBuf,
    file: Option<std::fs::File>,
    pending: Vec<u8>,
    /// Bytes in the file, not counting `pending`.
    written: u64,
    max: u64,
    failure: Option<Fail>,
    /// Finished or abandoned: takes nothing more.
    closed: bool,
    /// The budget this spool's bytes are charged to; moved to the
    /// [`Spooled`] at `finish`, dropped with the file otherwise.
    held: Option<Held>,
}

impl SpoolState {
    /// Charges `total` bytes in all, or records why not.
    fn charge(&mut self, total: u64) -> bool {
        let ok = self.held.as_mut().is_some_and(|h| h.hold(total));
        if !ok {
            self.failure = Some(Fail::Budget);
        }
        ok
    }
}

impl Drop for SpoolState {
    fn drop(&mut self) {
        if !self.closed || self.file.is_some() {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

/// A streamed write being spooled, shared by its upload and its stream.
#[derive(Debug, Clone)]
pub(crate) struct Spool {
    state: Arc<Mutex<SpoolState>>,
}

/// A spooled body, complete and fsynced, removed when dropped unless
/// [`Self::persist`] moved it.
#[derive(Debug)]
pub(crate) struct Spooled {
    pub(crate) path: PathBuf,
    pub(crate) size: u64,
    kept: bool,
    /// Given back once the file is committed or removed.
    _held: Option<Held>,
}

impl Spooled {
    /// The file is now someone else's to keep (renamed into place).
    pub(crate) fn persist(mut self) {
        self.kept = true;
    }
}

impl Drop for Spooled {
    fn drop(&mut self) {
        if !self.kept {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

impl Spool {
    /// A new, empty spool file at `path`, taking at most `max` bytes,
    /// each charged to `held` before it is written.
    pub(crate) async fn create(path: PathBuf, max: u64, held: Held) -> Result<Self, BlobError> {
        // Dropped before the file is ours (the request went away): the
        // file goes, whether it was made before or after.
        struct Unmade {
            path: PathBuf,
            gone: Arc<AtomicBool>,
            made: bool,
        }
        impl Drop for Unmade {
            fn drop(&mut self) {
                if !self.made {
                    self.gone.store(true, Ordering::SeqCst);
                    let _ = std::fs::remove_file(&self.path);
                }
            }
        }
        let mut unmade = Unmade {
            path: path.clone(),
            gone: Arc::new(AtomicBool::new(false)),
            made: false,
        };
        let (p, gone) = (path.clone(), Arc::clone(&unmade.gone));
        let file = tokio::task::spawn_blocking(move || {
            if let Some(dir) = p.parent() {
                std::fs::create_dir_all(dir)?;
            }
            let file = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&p)?;
            if gone.load(Ordering::SeqCst) {
                let _ = std::fs::remove_file(&p);
            }
            Ok::<_, std::io::Error>(file)
        })
        .await
        .map_err(unavailable)?
        .map_err(unavailable)?;
        unmade.made = true;
        Ok(Self {
            state: Arc::new(Mutex::new(SpoolState {
                path,
                file: Some(file),
                pending: Vec::new(),
                written: 0,
                max,
                failure: None,
                closed: false,
                held: Some(held),
            })),
        })
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, SpoolState> {
        self.state.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// The `output-stream` the guest writes the body to.
    pub(crate) fn stream(&self, alive: Alive) -> SpoolStream {
        SpoolStream {
            spool: self.clone(),
            alive,
        }
    }

    /// Whether the upload is still open.
    pub(crate) fn is_open(&self) -> bool {
        !self.lock().closed
    }

    /// Abandons the write: its file goes, and its stream takes nothing.
    pub(crate) fn abandon(&self) {
        let mut s = self.lock();
        s.closed = true;
        s.pending = Vec::new();
        if s.file.take().is_some() {
            let _ = std::fs::remove_file(&s.path);
            s.held = None;
        }
    }

    /// Appends `data` to the spool file directly (a node-link upload's body,
    /// not a guest's stream), within the limit.
    pub(crate) async fn append(&self, data: bytes::Bytes) -> Result<(), BlobError> {
        let file = {
            let mut s = self.lock();
            if s.closed {
                return Err(BlobError::NotAllowed("the upload is closed".into()));
            }
            let total = s.written + data.len() as u64;
            if total > s.max {
                s.failure = Some(Fail::TooLarge);
                return Err(BlobError::TooLarge);
            }
            if !s.charge(total) {
                return Err(BlobError::Unavailable(NO_SPACE.into()));
            }
            s.file
                .take()
                .ok_or_else(|| BlobError::Unavailable("the spool file is busy".into()))?
        };
        let n = data.len() as u64;
        let wrote = tokio::task::spawn_blocking(move || {
            let mut file = file;
            file.write_all(&data).map(|()| file)
        })
        .await;
        let mut s = self.lock();
        match wrote {
            Ok(Ok(file)) => {
                s.written += n;
                s.file = Some(file);
                Ok(())
            }
            Ok(Err(e)) => Err(unavailable(e)),
            Err(e) => Err(unavailable(e)),
        }
    }

    /// Ends the body: what was written, fsynced, or why it cannot be a file.
    pub(crate) async fn finish(&self) -> Result<Spooled, BlobError> {
        let (file, pending, path, size, held) = {
            let mut s = self.lock();
            if s.closed {
                return Err(BlobError::NotAllowed("the upload is closed".into()));
            }
            s.closed = true;
            let failed = match s.failure.clone() {
                Some(Fail::TooLarge) => Some(BlobError::TooLarge),
                Some(Fail::Budget) => Some(BlobError::Unavailable(NO_SPACE.into())),
                Some(Fail::Io(e)) => Some(BlobError::Unavailable(e)),
                None => None,
            };
            if let Some(e) = failed {
                // Nothing becomes of it: the file and its budget go now,
                // not when the guest drops its stream.
                s.pending = Vec::new();
                if s.file.take().is_some() {
                    let _ = std::fs::remove_file(&s.path);
                    s.held = None;
                }
                return Err(e);
            }
            let Some(file) = s.file.take() else {
                return Err(BlobError::Unavailable("the spool file is gone".into()));
            };
            let pending = std::mem::take(&mut s.pending);
            let size = s.written + pending.len() as u64;
            (file, pending, s.path.clone(), size, s.held.take())
        };
        let spooled = Spooled {
            path,
            size,
            kept: false,
            _held: held,
        };
        tokio::task::spawn_blocking(move || {
            let mut file = file;
            file.write_all(&pending)?;
            file.sync_all()
        })
        .await
        .map_err(unavailable)?
        .map_err(unavailable)?;
        Ok(spooled)
    }
}

/// A spool's `output-stream`.
pub(crate) struct SpoolStream {
    spool: Spool,
    alive: Alive,
}

impl SpoolStream {
    fn check(&self, s: &SpoolState) -> StreamResult<()> {
        if !self.alive.load(Ordering::Acquire) || s.closed {
            return Err(StreamError::Closed);
        }
        match &s.failure {
            Some(Fail::TooLarge) => Err(StreamError::LastOperationFailed(wasmtime::format_err!(
                "the file is past the node's limit of {} bytes",
                s.max
            ))),
            Some(Fail::Budget) => Err(StreamError::LastOperationFailed(wasmtime::format_err!(
                "{NO_SPACE}"
            ))),
            Some(Fail::Io(e)) => Err(StreamError::LastOperationFailed(wasmtime::format_err!(
                "spooling the file failed: {e}"
            ))),
            None => Ok(()),
        }
    }
}

#[wasmtime_wasi::async_trait]
impl Pollable for SpoolStream {
    async fn ready(&mut self) {
        let (file, pending) = {
            let mut s = self.spool.lock();
            if s.pending.is_empty() || s.closed || s.failure.is_some() {
                return;
            }
            let Some(file) = s.file.take() else {
                return;
            };
            (file, std::mem::take(&mut s.pending))
        };
        let n = pending.len() as u64;
        let wrote = tokio::task::spawn_blocking(move || {
            let mut file = file;
            file.write_all(&pending).map(|()| file)
        })
        .await;
        let mut s = self.spool.lock();
        match wrote {
            Ok(Ok(file)) => {
                s.written += n;
                if s.closed {
                    // Abandoned while writing: the file goes now.
                    let _ = std::fs::remove_file(&s.path);
                    s.held = None;
                } else {
                    s.file = Some(file);
                }
            }
            Ok(Err(e)) => s.failure = Some(Fail::Io(e.to_string())),
            Err(e) => s.failure = Some(Fail::Io(e.to_string())),
        }
    }
}

#[wasmtime_wasi::async_trait]
impl OutputStream for SpoolStream {
    fn write(&mut self, bytes: Bytes) -> StreamResult<()> {
        let spool = self.spool.clone();
        let mut s = spool.lock();
        self.check(&s)?;
        if s.pending.len() + bytes.len() > WRITE_BUFFER {
            return Err(StreamError::Trap(wasmtime::format_err!(
                "write past what check-write allowed"
            )));
        }
        let total = s.written + (s.pending.len() + bytes.len()) as u64;
        if total > s.max {
            s.failure = Some(Fail::TooLarge);
            drop(s);
            return Err(StreamError::LastOperationFailed(wasmtime::format_err!(
                "the file is past the node's limit"
            )));
        }
        if !s.charge(total) {
            drop(s);
            return Err(StreamError::LastOperationFailed(wasmtime::format_err!(
                "{NO_SPACE}"
            )));
        }
        s.pending.extend_from_slice(&bytes);
        Ok(())
    }

    fn flush(&mut self) -> StreamResult<()> {
        let s = self.spool.lock();
        self.check(&s)
    }

    fn check_write(&mut self) -> StreamResult<usize> {
        let s = self.spool.lock();
        self.check(&s)?;
        if s.file.is_none() {
            // A write to the file is under way.
            return Ok(0);
        }
        Ok(WRITE_BUFFER - s.pending.len())
    }
}
