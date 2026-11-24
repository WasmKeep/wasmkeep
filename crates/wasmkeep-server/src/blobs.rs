//! Files of actors and of the front (ADR 0030, sections 1 to 8): the
//! `wasmkeep:actor/blobs` interface's host side, on the bucket or, on a
//! node without one, on the data directory's disk.
//!
//! Every object key and every path is built here, from the app's root,
//! its epoch, the caller's namespace and a checked key: a guest names a
//! key within its own namespace and nothing else.
//!
//! ```text
//! <prefix>state/<t>/<a>/blobs/<epoch>/<class>/<name id>/<incarnation>/<key>   on a bucket
//! <prefix>state/<t>/<a>/blobs/<epoch>/_front/main/0/<key>                     the front's
//! <data_dir>/state/<t>/<a>/blobs/<epoch>/<namespace>/{index.db,f/,tmp/}      on disk
//! ```

mod bucket;
mod disk;
pub(crate) mod host;
pub(crate) mod keys;
pub mod links;
pub(crate) mod serve;
mod streams;
pub(crate) mod sweep;
pub(crate) mod token;
mod usage;

use std::path::PathBuf;
use std::sync::Arc;

use object_store::ObjectStore;
use wasmkeep_wit::wasmkeep::actor::blobs::{BlobError, BlobInfo, Condition};

pub(crate) use keys::Namespace;
pub use keys::epoch;

/// A whole file through `get` or `put`, as a call's body: 16 MiB.
pub(crate) const MAX_BUFFERED: u64 = 16 * 1024 * 1024;
/// The default for one file (`--max-blob-bytes`): 1 GiB.
pub const DEFAULT_MAX_FILE: u64 = 1024 * 1024 * 1024;
/// The ceiling for one file: one presigned PUT on S3, 5 GiB.
pub const MAX_FILE_CEILING: u64 = 5 * 1024 * 1024 * 1024;
/// A listing page's longest.
pub(crate) const MAX_PAGE: u32 = 1_000;
/// Bytes all spool files of the process may hold at once (streamed writes
/// and upload links before they become files): 4 GiB, or two files of the
/// largest `--max-blob-bytes` if that is more.
pub(crate) const NODE_SPOOL: u64 = 4 * 1024 * 1024 * 1024;
/// Bytes one app's spool files may hold at once: 1 GiB, or one file of
/// its `--max-blob-bytes` if that is more; at most half the process's.
pub(crate) const APP_SPOOL: u64 = 1024 * 1024 * 1024;
/// Streamed writes one instance may have open at once.
pub(crate) const MAX_OPEN_WRITES: usize = 16;
/// Read streams one instance may have open at once: a stream counts until
/// the guest drops it or reads it to its end, and the event's end closes
/// every one (on disk, its descriptor; on a bucket, its response).
pub(crate) const MAX_OPEN_READS: usize = 16;

/// The process's spool budget, shared by every app runtime.
fn node_spool() -> Arc<streams::Budget> {
    static NODE: std::sync::OnceLock<Arc<streams::Budget>> = std::sync::OnceLock::new();
    Arc::clone(NODE.get_or_init(|| Arc::new(streams::Budget::new(NODE_SPOOL))))
}

/// An app's spool budget, of `node`'s: [`APP_SPOOL`] or one file of
/// `max_file`, whichever is more, and never more than half the node's, so
/// one app's uploads always leave room for the others. The node's is
/// raised to two such files if it is less.
fn app_share(max_file: u64, node: &streams::Budget) -> Arc<streams::Budget> {
    node.raise_to(max_file.saturating_mul(2));
    Arc::new(streams::Budget::new(
        APP_SPOOL.max(max_file).min(node.limit() / 2),
    ))
}

/// A bucket prefix longer than this leaves no room for the longest key
/// under S3's 1,024 bytes (ADR 0030, section 2).
pub(crate) const MAX_BUCKET_PREFIX: usize = 200;

/// An app runtime's files: where they are, and the limits on them.
#[derive(Debug)]
pub(crate) struct Blobs {
    backend: Backend,
    /// One file's largest size, streamed (`--max-blob-bytes`).
    max_file: u64,
    /// How links are made and checked; `None`: no links.
    links: Option<links::Links>,
    /// The last count of the app's files, and its limit.
    usage: usage::Usage,
    /// The app's metric labels ([`crate::metrics::app_labels`]).
    labels: Arc<str>,
    /// What the app's spool files may hold at once, and the node's.
    spool_app: Arc<streams::Budget>,
    spool_node: Arc<streams::Budget>,
    /// Upload links being received on this node, by namespace and key.
    uploading: std::sync::Mutex<std::collections::HashSet<(String, String)>>,
}

/// One upload link being received, its key released when dropped.
pub(crate) struct Receiving<'a> {
    blobs: &'a Blobs,
    claim: (String, String),
}

impl Drop for Receiving<'_> {
    fn drop(&mut self) {
        self.blobs
            .uploading
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(&self.claim);
    }
}

#[derive(Debug)]
enum Backend {
    Bucket(bucket::Bucket),
    Disk(disk::Disk),
    /// Files cannot be kept here: every call is `unavailable` with this.
    Off(String),
}

/// What a write stores, checked.
#[derive(Debug, Clone)]
pub(crate) struct Write {
    pub(crate) content_type: String,
    pub(crate) condition: Condition,
}

impl Blobs {
    /// Files in the bucket under `prefix` (ending in `/` or empty) and the
    /// app's root `bucket_root` (`state/<t>/<a>/`), spooling streamed
    /// writes in `spool` (emptied now).
    pub(crate) fn on_bucket(
        store: Arc<dyn ObjectStore>,
        prefix: &str,
        bucket_root: &str,
        epoch: &str,
        spool: PathBuf,
        max_file: u64,
    ) -> Self {
        let backend = if prefix.len() > MAX_BUCKET_PREFIX {
            Backend::Off(format!(
                "files need a bucket prefix of at most {MAX_BUCKET_PREFIX} bytes; this one is {}",
                prefix.len()
            ))
        } else {
            match bucket::Bucket::new(store, &format!("{prefix}{bucket_root}"), epoch, spool) {
                Ok(b) => Backend::Bucket(b),
                Err(e) => Backend::Off(e),
            }
        };
        Self::new(backend, max_file)
    }

    fn new(backend: Backend, max_file: u64) -> Self {
        let max_file = max_file.min(MAX_FILE_CEILING);
        let spool_node = node_spool();
        Self {
            backend,
            max_file,
            links: None,
            usage: usage::Usage::default(),
            labels: Arc::from(""),
            spool_app: app_share(max_file, &spool_node),
            spool_node,
            uploading: std::sync::Mutex::default(),
        }
    }

    /// The same files, their share taken of `node` rather than of the
    /// process's budget.
    #[cfg(test)]
    pub(crate) fn with_node_spool(mut self, node: Arc<streams::Budget>) -> Self {
        self.spool_app = app_share(self.max_file, &node);
        self.spool_node = node;
        self
    }

    /// Files on disk, under `app_dir` (`<data_dir>/state/<t>/<a>`).
    pub(crate) fn on_disk(app_dir: &std::path::Path, epoch: &str, max_file: u64) -> Self {
        Self::new(
            Backend::Disk(disk::Disk::new(app_dir.join("blobs").join(epoch))),
            max_file,
        )
    }

    /// The same files, spooling under `app` and `node` rather than the
    /// app's own share and the process's.
    #[cfg(test)]
    pub(crate) fn with_spool_budgets(
        mut self,
        app: Arc<streams::Budget>,
        node: Arc<streams::Budget>,
    ) -> Self {
        self.spool_app = app;
        self.spool_node = node;
        self
    }

    /// Claims `ns`'s `key` for one upload link's body; `None` while
    /// another is being received on this node.
    pub(crate) fn receive(&self, ns: &Namespace, key: &str) -> Option<Receiving<'_>> {
        let claim = (ns.rel().to_owned(), key.to_owned());
        let mut uploading = self.uploading.lock().unwrap_or_else(|p| p.into_inner());
        if !uploading.insert(claim.clone()) {
            return None;
        }
        Some(Receiving { blobs: self, claim })
    }

    /// The same files, with links made and checked by `links`.
    pub(crate) fn with_links(mut self, links: links::Links) -> Self {
        self.links = Some(links);
        self
    }

    /// The same files, their work counted under `labels`.
    pub(crate) fn with_labels(mut self, labels: Arc<str>) -> Self {
        self.labels = labels;
        self
    }

    /// The app's metric labels.
    pub(crate) fn labels(&self) -> &str {
        &self.labels
    }

    pub(crate) fn links(&self) -> Option<&links::Links> {
        self.links.as_ref()
    }

    /// The largest file a node-link upload may create: a create-if-absent
    /// write, which on a bucket is one PUT of at most 16 MiB (a multipart
    /// upload takes no condition here).
    pub(crate) fn max_upload(&self) -> u64 {
        match &self.backend {
            Backend::Bucket(_) => MAX_BUFFERED.min(self.max_file),
            _ => self.max_file,
        }
    }

    /// A download link of `key`, attachment or inline (section 4).
    pub(crate) async fn link_download(
        &self,
        ns: &Namespace,
        key: &str,
        inline: bool,
        filename: Option<&str>,
        secs: u32,
    ) -> Result<String, BlobError> {
        let r = self.make_download(ns, key, inline, filename, secs).await;
        if r.is_ok() {
            crate::metrics::blob_link(&self.labels, "download");
        }
        r
    }

    async fn make_download(
        &self,
        ns: &Namespace,
        key: &str,
        inline: bool,
        filename: Option<&str>,
        secs: u32,
    ) -> Result<String, BlobError> {
        let links = self
            .links
            .as_ref()
            .ok_or_else(|| BlobError::Unavailable("this node makes no links".into()))?;
        // Inline only for a type a browser cannot run: the stored one.
        let content_type = if inline {
            let Some(info) = self.head(ns, key).await? else {
                return Err(BlobError::NotAllowed(
                    "an inline link needs the file: no file has this key".into(),
                ));
            };
            if !links::inline_safe(&info.content_type) {
                return Err(BlobError::NotAllowed(format!(
                    "{} is not shown inline: only images, audio, video and plain text",
                    info.content_type
                )));
            }
            info.content_type
        } else {
            keys::DEFAULT_CONTENT_TYPE.to_owned()
        };
        let name = links::filename(filename, key);
        if let (Some(signer), Backend::Bucket(b)) = (&links.settings.presigner, &self.backend) {
            let disposition = links::disposition(inline, &name);
            return links
                .presigned_download(signer, &b.path(ns, key)?, secs, &disposition, &content_type)
                .await;
        }
        let grant = token::Grant::Download {
            inline,
            filename: (!inline).then_some(name),
        };
        links.node_link(ns, key, secs, grant).map(|(url, _)| url)
    }

    /// An upload link creating `key`: exactly `size` bytes of
    /// `content_type` (checked), always a node link.
    pub(crate) fn link_upload(
        &self,
        ns: &Namespace,
        key: &str,
        size: u64,
        content_type: &str,
        secs: u32,
    ) -> Result<(String, u64), BlobError> {
        let links = self
            .links
            .as_ref()
            .ok_or_else(|| BlobError::Unavailable("this node makes no links".into()))?;
        let content_type = keys::content_type(Some(content_type)).map_err(BlobError::NotAllowed)?;
        if size > self.max_upload() {
            return Err(BlobError::TooLarge);
        }
        self.quota()?;
        let grant = token::Grant::Upload { size, content_type };
        let r = links.node_link(ns, key, secs, grant);
        if r.is_ok() {
            crate::metrics::blob_link(&self.labels, "upload");
        }
        r
    }

    pub(crate) async fn head(
        &self,
        ns: &Namespace,
        key: &str,
    ) -> Result<Option<BlobInfo>, BlobError> {
        let r = match &self.backend {
            Backend::Bucket(b) => b.head(ns, key).await,
            Backend::Disk(d) => d.head(ns, key).await,
            Backend::Off(why) => Err(BlobError::Unavailable(why.clone())),
        };
        self.count_op("head", found(&r));
        r
    }

    pub(crate) async fn get(
        &self,
        ns: &Namespace,
        key: &str,
    ) -> Result<Option<(BlobInfo, Vec<u8>)>, BlobError> {
        let r = match &self.backend {
            Backend::Bucket(b) => b.get(ns, key).await,
            Backend::Disk(d) => d.get(ns, key).await,
            Backend::Off(why) => Err(BlobError::Unavailable(why.clone())),
        };
        self.count_op("get", found(&r));
        if let Ok(Some((_, body))) = &r {
            crate::metrics::blob_bytes(&self.labels, "out", body.len() as u64);
        }
        r
    }

    pub(crate) async fn put(
        &self,
        ns: &Namespace,
        key: &str,
        body: Vec<u8>,
        write: Write,
    ) -> Result<BlobInfo, BlobError> {
        let n = body.len() as u64;
        let r = if n > MAX_BUFFERED {
            Err(BlobError::TooLarge)
        } else if let Err(e) = self.quota() {
            Err(e)
        } else {
            match &self.backend {
                Backend::Bucket(b) => b.put(ns, key, body.into(), &write).await,
                Backend::Disk(d) => d.put(ns, key, body, write).await,
                Backend::Off(why) => Err(BlobError::Unavailable(why.clone())),
            }
        };
        self.count_op("put", outcome(&r));
        if r.is_ok() {
            crate::metrics::blob_bytes(&self.labels, "in", n);
        }
        r
    }

    pub(crate) async fn delete(&self, ns: &Namespace, key: &str) -> Result<(), BlobError> {
        let r = match &self.backend {
            Backend::Bucket(b) => b.delete(ns, key).await,
            Backend::Disk(d) => d.delete(ns, key).await,
            Backend::Off(why) => Err(BlobError::Unavailable(why.clone())),
        };
        self.count_op("delete", outcome(&r));
        r
    }

    /// At most `limit` files whose key starts with `prefix`, after `after`,
    /// in key order, and the key to continue after when there are more.
    pub(crate) async fn list(
        &self,
        ns: &Namespace,
        prefix: &str,
        after: Option<&str>,
        limit: u32,
    ) -> Result<(Vec<BlobInfo>, Option<String>), BlobError> {
        let r = match &self.backend {
            Backend::Bucket(b) => b.list(ns, prefix, after, limit).await,
            Backend::Disk(d) => d.list(ns, prefix, after, limit).await,
            Backend::Off(why) => Err(BlobError::Unavailable(why.clone())),
        };
        self.count_op("list", outcome(&r));
        r
    }

    /// The file's information and a stream of its bytes `[start, start +
    /// length)`, or all of them.
    pub(crate) async fn open_read(
        &self,
        ns: &Namespace,
        key: &str,
        range: Option<(u64, u64)>,
    ) -> Result<Option<(BlobInfo, streams::Source)>, BlobError> {
        let r = match &self.backend {
            Backend::Bucket(b) => b.open_read(ns, key, range).await,
            Backend::Disk(d) => d.open_read(ns, key, range).await,
            Backend::Off(why) => Err(BlobError::Unavailable(why.clone())),
        };
        self.count_op("read", found(&r));
        // The bytes a read opens, counted when it opens: a stream dropped
        // early still counts its range.
        if let Ok(Some((info, _))) = &r {
            let out = range.map_or(info.size, |(start, length)| {
                length.min(info.size.saturating_sub(start))
            });
            crate::metrics::blob_bytes(&self.labels, "out", out);
        }
        r
    }

    /// A file to spool a streamed write to, until [`Self::commit`]: with
    /// `reserve`, that many bytes of the spool budgets are held first (an
    /// upload link's size), else each byte is charged as it is written.
    pub(crate) async fn spool(
        &self,
        ns: &Namespace,
        reserve: Option<u64>,
    ) -> Result<streams::Spool, BlobError> {
        if let Err(e) = self.quota() {
            self.count_op("write", "quota");
            return Err(e);
        }
        let mut held =
            streams::Held::none(Arc::clone(&self.spool_app), Arc::clone(&self.spool_node));
        if let Some(n) = reserve
            && !held.hold(n)
        {
            self.count_op("write", "unavailable");
            return Err(BlobError::Unavailable(
                "the node has no spool space for this write now; retry later".into(),
            ));
        }
        let path = match &self.backend {
            Backend::Bucket(b) => b.spool_path(),
            Backend::Disk(d) => d.spool_path(ns).await?,
            Backend::Off(why) => return Err(BlobError::Unavailable(why.clone())),
        };
        streams::Spool::create(path, self.max_file, held).await
    }

    /// Makes a spooled write the file at `key`, with its condition.
    pub(crate) async fn commit(
        &self,
        ns: &Namespace,
        key: &str,
        spooled: streams::Spooled,
        write: Write,
    ) -> Result<BlobInfo, BlobError> {
        let r = if let Err(e) = self.quota() {
            Err(e)
        } else {
            match &self.backend {
                Backend::Bucket(b) => b.commit(ns, key, spooled, &write).await,
                Backend::Disk(d) => d.commit(ns, key, spooled, write).await,
                Backend::Off(why) => Err(BlobError::Unavailable(why.clone())),
            }
        };
        self.count_op("write", outcome(&r));
        if let Ok(info) = &r {
            crate::metrics::blob_bytes(&self.labels, "in", info.size);
        }
        r
    }

    fn count_op(&self, op: &str, outcome: &str) {
        crate::metrics::blob_op(&self.labels, op, outcome);
    }
}

/// A call's `outcome` label.
fn outcome<T>(r: &Result<T, BlobError>) -> &'static str {
    match r {
        Ok(_) => "ok",
        Err(BlobError::Conflict) => "conflict",
        Err(BlobError::TooLarge) => "too_large",
        Err(BlobError::QuotaExceeded) => "quota",
        Err(BlobError::InvalidKey(_) | BlobError::NotAllowed(_)) => "not_allowed",
        Err(BlobError::Unavailable(_)) => "unavailable",
    }
}

/// `not_found` for an absent file, else [`outcome`].
fn found<T>(r: &Result<Option<T>, BlobError>) -> &'static str {
    match r {
        Ok(None) => "not_found",
        r => outcome(r),
    }
}

/// Milliseconds since the epoch, by this node's clock.
fn now_ms() -> u64 {
    crate::alarm::now_ms()
}

/// 16 random lowercase hex digits.
pub(crate) fn random_id() -> Result<String, BlobError> {
    use ring::rand::SecureRandom as _;
    let mut b = [0u8; 8];
    ring::rand::SystemRandom::new()
        .fill(&mut b)
        .map_err(|_| BlobError::Unavailable("no randomness".into()))?;
    Ok(b.iter().map(|b| format!("{b:02x}")).collect())
}

fn unavailable(e: impl std::fmt::Display) -> BlobError {
    BlobError::Unavailable(e.to_string())
}

#[cfg(test)]
mod tests;
