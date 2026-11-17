//! Files in the bucket (ADR 0030, sections 2 and 3): one object per file,
//! its content type stored with it, conditions as the bucket's
//! `If-None-Match: *` and `If-Match`.

use std::path::PathBuf;
use std::sync::Arc;

use bytes::Bytes;
use futures_util::StreamExt as _;
use object_store::ObjectStoreExt as _;
use object_store::path::Path;
use object_store::{
    Attribute, Attributes, GetOptions, GetRange, ObjectMeta, ObjectStore, PutMode,
    PutMultipartOptions, PutOptions, PutPayload, UpdateVersion,
};
use wasmkeep_wit::wasmkeep::actor::blobs::{BlobError, BlobInfo, Condition};

use super::keys::{DEFAULT_CONTENT_TYPE, check_key};
use super::streams::{Source, Spooled};
use super::{MAX_BUFFERED, Namespace, Write, now_ms, random_id, unavailable};

/// Heads a listing runs at once to read its entries' types.
const LIST_HEADS: usize = 16;
/// A multipart upload's part size.
const PART: usize = 8 * 1024 * 1024;

pub(super) struct Bucket {
    pub(super) store: Arc<dyn ObjectStore>,
    /// `<prefix>state/<t>/<a>/`, spelled as the rest of the state spells it.
    pub(super) root: String,
    /// The epoch this runtime computed at its start.
    pub(super) epoch: String,
    /// `<root>blobs/<epoch>/`.
    base: String,
    /// Where streamed writes are spooled before they are uploaded.
    spool_dir: PathBuf,
}

impl std::fmt::Debug for Bucket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Bucket").field("base", &self.base).finish()
    }
}

fn bucket_error(e: object_store::Error) -> BlobError {
    unavailable(format!("the bucket: {e}"))
}

/// `[start, start + length)` within a file of `size` bytes: the whole file
/// without a range, a range cut at the end, refused past it.
pub(super) fn clamp(range: Option<(u64, u64)>, size: u64) -> Result<(u64, u64), BlobError> {
    match range {
        None => Ok((0, size)),
        Some((start, _)) if start > size => Err(BlobError::NotAllowed(format!(
            "the range starts at {start}, past the end of a file of {size} bytes"
        ))),
        Some((start, length)) => Ok((start, length.min(size - start))),
    }
}

/// A page of at most `limit` entries out of `limit + 1` read, and the key
/// to continue after if there were more.
pub(super) fn page(mut rows: Vec<BlobInfo>, limit: u32) -> (Vec<BlobInfo>, Option<String>) {
    if rows.len() > limit as usize {
        rows.truncate(limit as usize);
        let next = rows.last().map(|r| r.key.clone());
        (rows, next)
    } else {
        (rows, None)
    }
}

fn info(key: &str, meta: &ObjectMeta, attributes: Option<&Attributes>) -> BlobInfo {
    let content_type = attributes
        .and_then(|a| a.get(&Attribute::ContentType))
        .map_or(DEFAULT_CONTENT_TYPE, |v| v.as_ref())
        .to_owned();
    BlobInfo {
        key: key.to_owned(),
        size: meta.size,
        content_type,
        etag: meta.e_tag.clone().unwrap_or_default(),
        modified_ms: u64::try_from(meta.last_modified.timestamp_millis()).unwrap_or(0),
    }
}

impl Bucket {
    /// Files under `root` (`<prefix>state/<t>/<a>/`) and `epoch`.
    pub(super) fn new(
        store: Arc<dyn ObjectStore>,
        root: &str,
        epoch: &str,
        spool_dir: PathBuf,
    ) -> Result<Self, String> {
        // What an earlier run of this process spooled is abandoned.
        let _ = std::fs::remove_dir_all(&spool_dir);
        std::fs::create_dir_all(&spool_dir)
            .map_err(|e| format!("creating {}: {e}", spool_dir.display()))?;
        // The root is spelled as every other key of the app's state spells
        // it (`Path::from`, which escapes bytes such as `~` in a prefix), so
        // files sit under the prefix that `app delete` and backups list;
        // only the part after it, the namespace and the key, is kept as is.
        let root = format!("{}/", Path::from(root).as_ref());
        let base = format!("{root}blobs/{epoch}/");
        Ok(Self {
            store,
            root,
            epoch: epoch.to_owned(),
            base,
            spool_dir,
        })
    }

    fn ns_base(&self, ns: &Namespace) -> String {
        format!("{}{}/", self.base, ns.rel())
    }

    pub(super) fn path(&self, ns: &Namespace, key: &str) -> Result<Path, BlobError> {
        // `parse`, not `from`: every byte of the key alphabet is kept as it
        // is, so a key has one spelling in the bucket (`from` escapes `~`).
        Path::parse(format!("{}{key}", self.ns_base(ns))).map_err(unavailable)
    }

    pub(super) async fn head(
        &self,
        ns: &Namespace,
        key: &str,
    ) -> Result<Option<BlobInfo>, BlobError> {
        let path = self.path(ns, key)?;
        let options = GetOptions {
            head: true,
            ..GetOptions::default()
        };
        match self.store.get_opts(&path, options).await {
            Ok(r) => Ok(Some(info(key, &r.meta, Some(&r.attributes)))),
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(bucket_error(e)),
        }
    }

    pub(super) async fn get(
        &self,
        ns: &Namespace,
        key: &str,
    ) -> Result<Option<(BlobInfo, Vec<u8>)>, BlobError> {
        let path = self.path(ns, key)?;
        let r = match self.store.get_opts(&path, GetOptions::default()).await {
            Ok(r) => r,
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(e) => return Err(bucket_error(e)),
        };
        // Checked before the body is read.
        if r.meta.size > MAX_BUFFERED {
            return Err(BlobError::TooLarge);
        }
        let i = info(key, &r.meta, Some(&r.attributes));
        let body = r.bytes().await.map_err(bucket_error)?;
        Ok(Some((i, body.to_vec())))
    }

    pub(super) async fn open_read(
        &self,
        ns: &Namespace,
        key: &str,
        range: Option<(u64, u64)>,
    ) -> Result<Option<(BlobInfo, Source)>, BlobError> {
        let path = self.path(ns, key)?;
        let Some(range) = range else {
            return match self.store.get_opts(&path, GetOptions::default()).await {
                Ok(r) => {
                    let i = info(key, &r.meta, Some(&r.attributes));
                    Ok(Some((i, Source::Bucket(r.into_stream()))))
                }
                Err(object_store::Error::NotFound { .. }) => Ok(None),
                Err(e) => Err(bucket_error(e)),
            };
        };
        // A range is checked against the file's size first, then read from
        // that very version of the file.
        let Some(i) = self.head(ns, key).await? else {
            return Ok(None);
        };
        let (start, length) = clamp(Some(range), i.size)?;
        if length == 0 {
            return Ok(Some((i, Source::Empty)));
        }
        let options = GetOptions {
            range: Some(GetRange::Bounded(start..start + length)),
            if_match: (!i.etag.is_empty()).then(|| i.etag.clone()),
            ..GetOptions::default()
        };
        match self.store.get_opts(&path, options).await {
            Ok(r) => Ok(Some((i, Source::Bucket(r.into_stream())))),
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(object_store::Error::Precondition { .. }) => {
                Err(unavailable("the file changed while it was opened"))
            }
            Err(e) => Err(bucket_error(e)),
        }
    }

    pub(super) async fn put(
        &self,
        ns: &Namespace,
        key: &str,
        body: Bytes,
        write: &Write,
    ) -> Result<BlobInfo, BlobError> {
        let path = self.path(ns, key)?;
        let size = body.len() as u64;
        let mode = match &write.condition {
            Condition::Any => PutMode::Overwrite,
            Condition::Absent => PutMode::Create,
            Condition::Etag(e) => PutMode::Update(UpdateVersion {
                e_tag: Some(e.clone()),
                version: None,
            }),
        };
        let options = PutOptions {
            mode,
            attributes: attributes(write),
            ..PutOptions::default()
        };
        match self
            .store
            .put_opts(&path, PutPayload::from(body), options)
            .await
        {
            Ok(r) => Ok(written(key, size, write, r)),
            Err(
                object_store::Error::AlreadyExists { .. }
                | object_store::Error::Precondition { .. },
            ) => Err(BlobError::Conflict),
            // `If-Match` on a key with no file.
            Err(object_store::Error::NotFound { .. })
                if matches!(write.condition, Condition::Etag(_)) =>
            {
                Err(BlobError::Conflict)
            }
            Err(e) => Err(bucket_error(e)),
        }
    }

    pub(super) fn spool_path(&self) -> PathBuf {
        let id = random_id().unwrap_or_else(|_| format!("{:016x}", now_ms()));
        self.spool_dir.join(id)
    }

    /// Uploads a spooled body: one conditional PUT up to 16 MiB; past it a
    /// multipart upload, which takes no condition (the bucket's multipart
    /// completion is not conditional here), so a larger body with one is
    /// `too-large` rather than written without it.
    pub(super) async fn commit(
        &self,
        ns: &Namespace,
        key: &str,
        spooled: Spooled,
        write: &Write,
    ) -> Result<BlobInfo, BlobError> {
        if spooled.size <= MAX_BUFFERED {
            let path = spooled.path.clone();
            let body = tokio::task::spawn_blocking(move || std::fs::read(path))
                .await
                .map_err(unavailable)?
                .map_err(unavailable)?;
            drop(spooled);
            return self.put(ns, key, Bytes::from(body), write).await;
        }
        if !matches!(write.condition, Condition::Any) {
            return Err(BlobError::TooLarge);
        }
        let path = self.path(ns, key)?;
        let options = PutMultipartOptions {
            attributes: attributes(write),
            ..PutMultipartOptions::default()
        };
        let mut upload = self
            .store
            .put_multipart_opts(&path, options)
            .await
            .map_err(bucket_error)?;
        // One part at a time, in order: a part is read from the spool file
        // only once the one before it was acknowledged.
        let sent = async {
            let mut file = Some(std::fs::File::open(&spooled.path).map_err(unavailable)?);
            let mut left = spooled.size;
            while left > 0 {
                let f = file
                    .take()
                    .ok_or_else(|| unavailable("the spool file is gone"))?;
                let want = (left as usize).min(PART);
                let (f, chunk) = tokio::task::spawn_blocking(move || {
                    use std::io::Read as _;
                    let mut f = f;
                    let mut buf = vec![0u8; want];
                    f.read_exact(&mut buf).map(|()| (f, buf))
                })
                .await
                .map_err(unavailable)?
                .map_err(unavailable)?;
                file = Some(f);
                left -= chunk.len() as u64;
                upload
                    .put_part(PutPayload::from(chunk))
                    .await
                    .map_err(bucket_error)?;
            }
            upload.complete().await.map_err(bucket_error)
        }
        .await;
        let r = match sent {
            Ok(r) => r,
            Err(e) => {
                let _ = upload.abort().await;
                return Err(e);
            }
        };
        Ok(written(key, spooled.size, write, r))
    }

    pub(super) async fn delete(&self, ns: &Namespace, key: &str) -> Result<(), BlobError> {
        let path = self.path(ns, key)?;
        match self.store.delete(&path).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(e) => Err(bucket_error(e)),
        }
    }

    /// The bucket lists whole segments: list under the prefix's last full
    /// segment, from the latest key that cannot be in the page, and keep
    /// what starts with the whole prefix.
    pub(super) async fn list(
        &self,
        ns: &Namespace,
        prefix: &str,
        after: Option<&str>,
        limit: u32,
    ) -> Result<(Vec<BlobInfo>, Option<String>), BlobError> {
        let ns_base = self.ns_base(ns);
        let (whole, partial) = match prefix.rfind('/') {
            Some(i) => (&prefix[..i], &prefix[i + 1..]),
            None => ("", prefix),
        };
        let dir = if whole.is_empty() {
            Path::parse(ns_base.trim_end_matches('/'))
        } else {
            Path::parse(format!("{ns_base}{whole}"))
        }
        .map_err(unavailable)?;
        // Every key starting with `prefix` comes after `prefix` less its
        // last byte (`uploads/up` lists from `uploads/u`); with a last part
        // of one byte, from the start of the folder.
        let floor = (partial.len() >= 2).then(|| &prefix[..prefix.len() - 1]);
        let offset = match (after, floor) {
            (Some(a), Some(f)) => Some(a.max(f)),
            (a, f) => a.or(f),
        };
        let mut stream = match offset {
            Some(o) => {
                let o = Path::parse(format!("{ns_base}{o}")).map_err(unavailable)?;
                self.store.list_with_offset(Some(&dir), &o)
            }
            None => self.store.list(Some(&dir)),
        };
        let want = limit as usize + 1;
        let mut found: Vec<(String, ObjectMeta)> = Vec::new();
        while found.len() < want {
            let Some(meta) = stream.next().await else {
                break;
            };
            let meta = meta.map_err(bucket_error)?;
            let Some(key) = meta.location.as_ref().strip_prefix(ns_base.as_str()) else {
                continue;
            };
            if !key.starts_with(prefix) {
                if key > prefix {
                    // Listings are in key order: nothing after this one
                    // starts with the prefix.
                    break;
                }
                continue;
            }
            if after.is_some_and(|a| key <= a) || check_key(key).is_err() {
                continue;
            }
            found.push((key.to_owned(), meta));
        }
        drop(stream);
        // The page's end is decided before the heads, which may drop a file
        // deleted meanwhile.
        let next = (found.len() > limit as usize).then(|| {
            found.truncate(limit as usize);
            found.last().map(|(k, _)| k.clone())
        });
        // A listing has no types: each entry's comes from its own head. A
        // file deleted since it was listed is left out.
        let entries: Vec<Option<BlobInfo>> = futures_util::stream::iter(found)
            .map(|(key, meta)| async move {
                let options = GetOptions {
                    head: true,
                    ..GetOptions::default()
                };
                let path = meta.location.clone();
                match self.store.get_opts(&path, options).await {
                    Ok(r) => Ok(Some(info(&key, &meta, Some(&r.attributes)))),
                    Err(object_store::Error::NotFound { .. }) => Ok(None),
                    Err(e) => Err(bucket_error(e)),
                }
            })
            .buffered(LIST_HEADS)
            .collect::<Vec<Result<_, _>>>()
            .await
            .into_iter()
            .collect::<Result<_, _>>()?;
        Ok((entries.into_iter().flatten().collect(), next.flatten()))
    }
}

fn attributes(write: &Write) -> Attributes {
    let mut a = Attributes::new();
    a.insert(Attribute::ContentType, write.content_type.clone().into());
    a
}

fn written(key: &str, size: u64, write: &Write, r: object_store::PutResult) -> BlobInfo {
    BlobInfo {
        key: key.to_owned(),
        size,
        content_type: write.content_type.clone(),
        etag: r.e_tag.unwrap_or_default(),
        // The write's own answer carries no time: the node's clock.
        modified_ms: now_ms(),
    }
}
