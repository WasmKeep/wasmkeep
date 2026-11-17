//! The `wasi:io` streams of `open-read` and `open-write`.
//!
//! A read streams from the bucket's response or the file on disk. A write
//! is spooled to a file on the node's disk, bounded by `--max-blob-bytes`,
//! and becomes the file only at `finish` (ADR 0030, section 3). Both stop
//! when their event ends: the host flips their [`Alive`] token at the next
//! event, and an upload still open is abandoned then.

use std::io::{Read as _, Seek as _, Write as _};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
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
    Io(String),
}

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
    /// A new, empty spool file at `path`, taking at most `max` bytes.
    pub(crate) async fn create(path: PathBuf, max: u64) -> Result<Self, BlobError> {
        let p = path.clone();
        let file = tokio::task::spawn_blocking(move || {
            if let Some(dir) = p.parent() {
                std::fs::create_dir_all(dir)?;
            }
            std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&p)
        })
        .await
        .map_err(unavailable)?
        .map_err(unavailable)?;
        Ok(Self {
            state: Arc::new(Mutex::new(SpoolState {
                path,
                file: Some(file),
                pending: Vec::new(),
                written: 0,
                max,
                failure: None,
                closed: false,
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
            if s.written + data.len() as u64 > s.max {
                s.failure = Some(Fail::TooLarge);
                return Err(BlobError::TooLarge);
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
        let (file, pending, path, size) = {
            let mut s = self.lock();
            if s.closed {
                return Err(BlobError::NotAllowed("the upload is closed".into()));
            }
            s.closed = true;
            match s.failure.clone() {
                Some(Fail::TooLarge) => return Err(BlobError::TooLarge),
                Some(Fail::Io(e)) => return Err(BlobError::Unavailable(e)),
                None => {}
            }
            let Some(file) = s.file.take() else {
                return Err(BlobError::Unavailable("the spool file is gone".into()));
            };
            let pending = std::mem::take(&mut s.pending);
            let size = s.written + pending.len() as u64;
            (file, pending, s.path.clone(), size)
        };
        let spooled = Spooled {
            path,
            size,
            kept: false,
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
        if s.written + (s.pending.len() + bytes.len()) as u64 > s.max {
            s.failure = Some(Fail::TooLarge);
            drop(s);
            return Err(StreamError::LastOperationFailed(wasmtime::format_err!(
                "the file is past the node's limit"
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
