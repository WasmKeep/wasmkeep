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
mod streams;

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
/// A bucket prefix longer than this leaves no room for the longest key
/// under S3's 1,024 bytes (ADR 0030, section 2).
pub(crate) const MAX_BUCKET_PREFIX: usize = 200;

/// An app runtime's files: where they are, and the limits on them.
#[derive(Debug)]
pub(crate) struct Blobs {
    backend: Backend,
    /// One file's largest size, streamed (`--max-blob-bytes`).
    max_file: u64,
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
            match bucket::Bucket::new(store, format!("{prefix}{bucket_root}blobs/{epoch}/"), spool)
            {
                Ok(b) => Backend::Bucket(b),
                Err(e) => Backend::Off(e),
            }
        };
        Self {
            backend,
            max_file: max_file.min(MAX_FILE_CEILING),
        }
    }

    /// Files on disk, under `app_dir` (`<data_dir>/state/<t>/<a>`).
    pub(crate) fn on_disk(app_dir: &std::path::Path, epoch: &str, max_file: u64) -> Self {
        Self {
            backend: Backend::Disk(disk::Disk::new(app_dir.join("blobs").join(epoch))),
            max_file: max_file.min(MAX_FILE_CEILING),
        }
    }

    pub(crate) async fn head(
        &self,
        ns: &Namespace,
        key: &str,
    ) -> Result<Option<BlobInfo>, BlobError> {
        match &self.backend {
            Backend::Bucket(b) => b.head(ns, key).await,
            Backend::Disk(d) => d.head(ns, key).await,
            Backend::Off(why) => Err(BlobError::Unavailable(why.clone())),
        }
    }

    pub(crate) async fn get(
        &self,
        ns: &Namespace,
        key: &str,
    ) -> Result<Option<(BlobInfo, Vec<u8>)>, BlobError> {
        match &self.backend {
            Backend::Bucket(b) => b.get(ns, key).await,
            Backend::Disk(d) => d.get(ns, key).await,
            Backend::Off(why) => Err(BlobError::Unavailable(why.clone())),
        }
    }

    pub(crate) async fn put(
        &self,
        ns: &Namespace,
        key: &str,
        body: Vec<u8>,
        write: Write,
    ) -> Result<BlobInfo, BlobError> {
        if body.len() as u64 > MAX_BUFFERED {
            return Err(BlobError::TooLarge);
        }
        match &self.backend {
            Backend::Bucket(b) => b.put(ns, key, body.into(), &write).await,
            Backend::Disk(d) => d.put(ns, key, body, write).await,
            Backend::Off(why) => Err(BlobError::Unavailable(why.clone())),
        }
    }

    pub(crate) async fn delete(&self, ns: &Namespace, key: &str) -> Result<(), BlobError> {
        match &self.backend {
            Backend::Bucket(b) => b.delete(ns, key).await,
            Backend::Disk(d) => d.delete(ns, key).await,
            Backend::Off(why) => Err(BlobError::Unavailable(why.clone())),
        }
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
        match &self.backend {
            Backend::Bucket(b) => b.list(ns, prefix, after, limit).await,
            Backend::Disk(d) => d.list(ns, prefix, after, limit).await,
            Backend::Off(why) => Err(BlobError::Unavailable(why.clone())),
        }
    }

    /// The file's information and a stream of its bytes `[start, start +
    /// length)`, or all of them.
    pub(crate) async fn open_read(
        &self,
        ns: &Namespace,
        key: &str,
        range: Option<(u64, u64)>,
    ) -> Result<Option<(BlobInfo, streams::Source)>, BlobError> {
        match &self.backend {
            Backend::Bucket(b) => b.open_read(ns, key, range).await,
            Backend::Disk(d) => d.open_read(ns, key, range).await,
            Backend::Off(why) => Err(BlobError::Unavailable(why.clone())),
        }
    }

    /// A file to spool a streamed write to, until [`Self::commit`].
    pub(crate) async fn spool(&self, ns: &Namespace) -> Result<streams::Spool, BlobError> {
        let path = match &self.backend {
            Backend::Bucket(b) => b.spool_path(),
            Backend::Disk(d) => d.spool_path(ns).await?,
            Backend::Off(why) => return Err(BlobError::Unavailable(why.clone())),
        };
        streams::Spool::create(path, self.max_file).await
    }

    /// Makes a spooled write the file at `key`, with its condition.
    pub(crate) async fn commit(
        &self,
        ns: &Namespace,
        key: &str,
        spooled: streams::Spooled,
        write: Write,
    ) -> Result<BlobInfo, BlobError> {
        match &self.backend {
            Backend::Bucket(b) => b.commit(ns, key, spooled, &write).await,
            Backend::Disk(d) => d.commit(ns, key, spooled, write).await,
            Backend::Off(why) => Err(BlobError::Unavailable(why.clone())),
        }
    }
}

/// Milliseconds since the epoch, by this node's clock.
fn now_ms() -> u64 {
    crate::alarm::now_ms()
}

/// 16 random lowercase hex digits.
fn random_id() -> Result<String, BlobError> {
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
