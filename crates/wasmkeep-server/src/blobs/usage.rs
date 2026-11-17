//! The app's stored files, counted once an hour, and its limit (ADR 0030,
//! section 7). On a bucket the sweep node lists `blobs/<epoch>/` and
//! writes the count to `R blob-usage`, which every node of the app reads
//! once a minute; on a node without a bucket the same count walks the
//! directory. With a limit, writes and upload links are refused with
//! `quota-exceeded` while the last count is over it: the limit lags by up
//! to an hour of writes plus the upload links outstanding.

use super::*;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

/// The count's key, under the app's root.
pub(crate) const USAGE_KEY: &str = "blob-usage";
const HEADER: &str = "wasmkeep-blob-usage 1";

/// One count of the app's current epoch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Count {
    pub(crate) epoch: String,
    pub(crate) bytes: u64,
    pub(crate) files: u64,
}

impl Count {
    fn encode(&self) -> String {
        format!(
            "{HEADER}\n{}\n{}\n{}\n{}\n",
            self.epoch,
            self.bytes,
            self.files,
            crate::alarm::now_ms()
        )
    }

    fn decode(body: &[u8]) -> Option<Self> {
        let mut lines = std::str::from_utf8(body).ok()?.lines();
        if lines.next()? != HEADER {
            return None;
        }
        let epoch = lines.next()?.to_owned();
        if !wasmkeep_cluster::blob_sweep::is_epoch(&epoch) {
            return None;
        }
        Some(Self {
            epoch,
            bytes: lines.next()?.parse().ok()?,
            files: lines.next()?.parse().ok()?,
        })
    }
}

/// The last count this runtime knows, and the limit.
#[derive(Debug, Default)]
pub(crate) struct Usage {
    limit: Option<u64>,
    known: AtomicBool,
    bytes: AtomicU64,
    files: AtomicU64,
}

impl Blobs {
    /// The same files, refused for writes once a count is over `limit`
    /// (`--max-app-blob-bytes`; `None`: no limit).
    pub(crate) fn with_quota(mut self, limit: Option<u64>) -> Self {
        self.usage.limit = limit;
        self
    }

    /// Whether the last count is over the limit.
    pub(crate) fn over_quota(&self) -> bool {
        self.usage.limit.is_some_and(|limit| {
            self.usage.known.load(Ordering::Acquire)
                && self.usage.bytes.load(Ordering::Acquire) > limit
        })
    }

    pub(super) fn quota(&self) -> Result<(), BlobError> {
        if self.over_quota() {
            Err(BlobError::QuotaExceeded)
        } else {
            Ok(())
        }
    }

    /// Takes `count` as the last, if it is of this runtime's epoch.
    pub(crate) fn note_usage(&self, count: &Count) {
        if Some(count.epoch.as_str()) != self.own_epoch().as_deref() {
            return;
        }
        self.usage.bytes.store(count.bytes, Ordering::Release);
        self.usage.files.store(count.files, Ordering::Release);
        self.usage.known.store(true, Ordering::Release);
        crate::metrics::blob_stored(&self.labels, count.bytes, count.files);
    }

    fn own_epoch(&self) -> Option<String> {
        match &self.backend {
            Backend::Bucket(b) => Some(b.epoch.clone()),
            Backend::Disk(d) => d
                .base()
                .file_name()
                .and_then(|n| n.to_str())
                .map(str::to_owned),
            Backend::Off(_) => None,
        }
    }

    /// Counts the files of this runtime's epoch: one listing on a bucket,
    /// a walk of the directory on disk.
    pub(crate) async fn count(&self) -> Result<Count, String> {
        let epoch = self.own_epoch().ok_or("files are not kept here")?;
        let (bytes, files) = match &self.backend {
            Backend::Bucket(b) => {
                let prefix = object_store::path::Path::parse(
                    format!("{}blobs/{}", b.root, b.epoch).as_str(),
                )
                .map_err(|e| e.to_string())?;
                let mut listing = b.store.list(Some(&prefix));
                let (mut bytes, mut files) = (0u64, 0u64);
                while let Some(meta) = futures_util::StreamExt::next(&mut listing).await {
                    let meta = meta.map_err(|e| format!("listing the files: {e}"))?;
                    bytes = bytes.saturating_add(meta.size);
                    files += 1;
                }
                (bytes, files)
            }
            Backend::Disk(d) => {
                let base = d.base().to_path_buf();
                tokio::task::spawn_blocking(move || walk(&base))
                    .await
                    .map_err(|e| e.to_string())?
                    .map_err(|e| format!("counting the files: {e}"))?
            }
            Backend::Off(why) => return Err(why.clone()),
        };
        Ok(Count {
            epoch,
            bytes,
            files,
        })
    }

    /// Writes `count` for the other nodes of the app.
    pub(crate) async fn publish_usage(&self, count: &Count) -> Result<(), String> {
        let b = self.bucket().ok_or("no bucket")?;
        let path = object_store::path::Path::parse(format!("{}{USAGE_KEY}", b.root).as_str())
            .map_err(|e| e.to_string())?;
        b.store
            .put_opts(
                &path,
                object_store::PutPayload::from(count.encode().into_bytes()),
                Default::default(),
            )
            .await
            .map(|_| ())
            .map_err(|e| format!("writing {USAGE_KEY}: {e}"))
    }

    /// The last count any node wrote; `None` before the first.
    pub(crate) async fn read_usage(&self) -> Result<Option<Count>, String> {
        let b = self.bucket().ok_or("no bucket")?;
        let path = object_store::path::Path::parse(format!("{}{USAGE_KEY}", b.root).as_str())
            .map_err(|e| e.to_string())?;
        let got = match b.store.get_opts(&path, Default::default()).await {
            Ok(got) => got,
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(e) => return Err(format!("reading {USAGE_KEY}: {e}")),
        };
        if got.meta.size > 4096 {
            return Ok(None);
        }
        let body = got
            .bytes()
            .await
            .map_err(|e| format!("reading {USAGE_KEY}: {e}"))?;
        Ok(Count::decode(&body))
    }
}

/// `<base>/<class>/<name id>/<incarnation>/f/*`: bytes and files.
fn walk(base: &std::path::Path) -> std::io::Result<(u64, u64)> {
    fn dirs(p: &std::path::Path) -> std::io::Result<Vec<std::path::PathBuf>> {
        match std::fs::read_dir(p) {
            Ok(entries) => {
                let mut out = Vec::new();
                for e in entries {
                    let e = e?;
                    if e.file_type()?.is_dir() {
                        out.push(e.path());
                    }
                }
                Ok(out)
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
            Err(e) => Err(e),
        }
    }
    let (mut bytes, mut files) = (0u64, 0u64);
    for class in dirs(base)? {
        for name in dirs(&class)? {
            for incarnation in dirs(&name)? {
                let f = incarnation.join("f");
                let entries = match std::fs::read_dir(&f) {
                    Ok(e) => e,
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                    Err(e) => return Err(e),
                };
                for e in entries {
                    let e = e?;
                    let meta = e.metadata()?;
                    if meta.is_file() {
                        bytes = bytes.saturating_add(meta.len());
                        files += 1;
                    }
                }
            }
        }
    }
    Ok((bytes, files))
}
