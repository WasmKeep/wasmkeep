//! Production bucket adapter on the `object_store` crate: S3, R2, MinIO, GCS,
//! Azure, local files or memory, with S3-style conditional writes.

use std::sync::Arc;

use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload, UpdateVersion};

use crate::control::Keys;
use crate::lease::{Bucket, BucketError, Version};
use crate::membership::Directory;

/// A bucket (optionally under a key prefix) for leases and heartbeats.
#[derive(Debug, Clone)]
pub struct ObjectStoreBucket {
    store: Arc<dyn ObjectStore>,
    prefix: String,
}

impl ObjectStoreBucket {
    /// Uses `store`, with every key under `prefix` (may be empty).
    pub fn new(store: Arc<dyn ObjectStore>, prefix: impl Into<String>) -> Self {
        let mut prefix = prefix.into();
        if !prefix.is_empty() && !prefix.ends_with('/') {
            prefix.push('/');
        }
        Self { store, prefix }
    }

    fn path(&self, key: &str) -> Path {
        Path::from(format!("{}{key}", self.prefix))
    }

    /// When the object at `key` was last written, by the bucket's own clock
    /// (ms since the epoch; S3 gives whole seconds, rounded down); `None` if
    /// there is none. `wasmkeep cluster fence` checks the operator's clock
    /// against it.
    pub async fn modified_ms(&self, key: &str) -> Result<Option<u64>, BucketError> {
        match self.store.head(&self.path(key)).await {
            Ok(meta) => Ok(Some(
                u64::try_from(meta.last_modified.timestamp_millis()).unwrap_or(0),
            )),
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(BucketError::Unavailable(e.to_string())),
        }
    }

    async fn put_mode(
        &self,
        key: &str,
        body: Vec<u8>,
        mode: PutMode,
    ) -> Result<Version, BucketError> {
        let update = matches!(mode, PutMode::Update(_));
        let opts = PutOptions {
            mode,
            ..PutOptions::default()
        };
        self.store
            .put_opts(&self.path(key), PutPayload::from(body), opts)
            .await
            .map(|r| encode_version(r.e_tag, r.version))
            .map_err(|e| put_error(update, e))
    }
}

/// A write's failure as a bucket error. A write conditioned on a version
/// (`update`) whose object is gone failed its condition: some stores answer
/// 404 rather than 412 (S3), and a lease or record deleted underneath its
/// holder must read as replaced, not as the bucket being unreachable.
fn put_error(update: bool, e: object_store::Error) -> BucketError {
    match e {
        object_store::Error::AlreadyExists { .. } | object_store::Error::Precondition { .. } => {
            BucketError::Conflict
        }
        object_store::Error::NotFound { .. } if update => BucketError::Conflict,
        e => BucketError::Unavailable(e.to_string()),
    }
}

/// Our opaque version carries both the ETag and the store's version id, as
/// object_store recommends preserving both for conditional updates.
fn encode_version(e_tag: Option<String>, version: Option<String>) -> Version {
    Version(format!(
        "{}\n{}",
        e_tag.unwrap_or_default(),
        version.unwrap_or_default()
    ))
}

fn decode_version(v: &Version) -> UpdateVersion {
    let (e_tag, version) = v.0.split_once('\n').unwrap_or((v.0.as_str(), ""));
    let some = |s: &str| (!s.is_empty()).then(|| s.to_owned());
    UpdateVersion {
        e_tag: some(e_tag),
        version: some(version),
    }
}

impl Bucket for ObjectStoreBucket {
    async fn get(&self, key: &str) -> Result<Option<(Vec<u8>, Version)>, BucketError> {
        match self.store.get(&self.path(key)).await {
            Ok(result) => {
                let version =
                    encode_version(result.meta.e_tag.clone(), result.meta.version.clone());
                let bytes = result
                    .bytes()
                    .await
                    .map_err(|e| BucketError::Unavailable(e.to_string()))?;
                Ok(Some((bytes.to_vec(), version)))
            }
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(BucketError::Unavailable(e.to_string())),
        }
    }

    async fn create(&self, key: &str, body: Vec<u8>) -> Result<Version, BucketError> {
        self.put_mode(key, body, PutMode::Create).await
    }

    async fn replace(
        &self,
        key: &str,
        body: Vec<u8>,
        expected: &Version,
    ) -> Result<Version, BucketError> {
        self.put_mode(key, body, PutMode::Update(decode_version(expected)))
            .await
    }
}

impl Directory for ObjectStoreBucket {
    #[cfg(test)]
    async fn put(&self, key: &str, body: Vec<u8>) -> Result<(), BucketError> {
        self.put_mode(key, body, PutMode::Overwrite).await.map(drop)
    }

    async fn list(&self, prefix: &str) -> Result<Vec<(String, Vec<u8>)>, BucketError> {
        use futures_util::TryStreamExt;
        let base = self.path(prefix);
        let metas: Vec<_> = self
            .store
            .list(Some(&base))
            .try_collect()
            .await
            .map_err(|e: object_store::Error| BucketError::Unavailable(e.to_string()))?;
        // Listed locations are in the store's encoding (a `%` in a segment is
        // escaped again): strip the prefix in that same encoding.
        let encoded_prefix = if self.prefix.is_empty() {
            String::new()
        } else {
            format!(
                "{}/",
                Path::from(self.prefix.trim_end_matches('/')).as_ref()
            )
        };
        let mut out = Vec::with_capacity(metas.len());
        for meta in metas {
            // Not under the prefix in the store's encoding: never expected;
            // skip it rather than hand back a key that is not relative.
            let Some(key) = meta.location.as_ref().strip_prefix(encoded_prefix.as_str()) else {
                continue;
            };
            let key = key.to_owned();
            match self.store.get(&meta.location).await {
                Ok(r) => out.push((
                    key,
                    r.bytes()
                        .await
                        .map_err(|e| BucketError::Unavailable(e.to_string()))?
                        .to_vec(),
                )),
                // Deleted between list and get: skip it.
                Err(object_store::Error::NotFound { .. }) => {}
                Err(e) => return Err(BucketError::Unavailable(e.to_string())),
            }
        }
        Ok(out)
    }
}

impl Keys for ObjectStoreBucket {
    async fn list_keys(&self, prefix: &str) -> Result<Vec<(String, u64)>, BucketError> {
        use futures_util::TryStreamExt;
        let metas: Vec<_> = self
            .store
            .list(Some(&self.path(prefix)))
            .try_collect()
            .await
            .map_err(|e: object_store::Error| BucketError::Unavailable(e.to_string()))?;
        let encoded_prefix = if self.prefix.is_empty() {
            String::new()
        } else {
            format!(
                "{}/",
                Path::from(self.prefix.trim_end_matches('/')).as_ref()
            )
        };
        Ok(metas
            .into_iter()
            .filter_map(|meta| {
                let key = meta
                    .location
                    .as_ref()
                    .strip_prefix(encoded_prefix.as_str())?;
                let modified = u64::try_from(meta.last_modified.timestamp_millis()).unwrap_or(0);
                Some((key.to_owned(), modified))
            })
            .collect())
    }

    async fn put_empty(&self, key: &str) -> Result<(), BucketError> {
        self.put_mode(key, Vec::new(), PutMode::Overwrite)
            .await
            .map(drop)
    }

    async fn delete(&self, key: &str) -> Result<(), BucketError> {
        match self.store.delete(&self.path(key)).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(e) => Err(BucketError::Unavailable(e.to_string())),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use object_store::memory::InMemory;

    use super::*;
    use crate::lease::{Clock, LeaseConfig, LeaseError, Leases};
    use crate::membership::Membership;

    struct At(Arc<std::sync::atomic::AtomicU64>);
    impl Clock for At {
        fn now_ms(&self) -> u64 {
            self.0.load(std::sync::atomic::Ordering::Relaxed)
        }
    }

    fn clock(t: u64) -> (At, Arc<std::sync::atomic::AtomicU64>) {
        let cell = Arc::new(std::sync::atomic::AtomicU64::new(t));
        (At(Arc::clone(&cell)), cell)
    }

    #[tokio::test]
    async fn listing_strips_a_prefix_that_needs_escaping() {
        // object_store escapes `%` (and others) in path segments again; keys
        // must still come back relative to the prefix.
        for prefix in ["cluster-a", "team%20a/prod", "a b"] {
            let bucket = ObjectStoreBucket::new(Arc::new(InMemory::new()), prefix);
            bucket.put("nodes/n1", b"x".to_vec()).await.unwrap();
            let listed = bucket.list("nodes/").await.unwrap();
            let keys: Vec<_> = listed.iter().map(|(k, _)| k.as_str()).collect();
            assert_eq!(keys, ["nodes/n1"], "prefix {prefix:?}");
        }
    }

    #[tokio::test]
    async fn conditional_writes_map_to_conflicts() {
        let bucket = ObjectStoreBucket::new(Arc::new(InMemory::new()), "cluster-a");
        let v1 = bucket.create("k", b"one".to_vec()).await.unwrap();
        assert_eq!(
            bucket.create("k", b"two".to_vec()).await,
            Err(BucketError::Conflict)
        );
        let v2 = bucket.replace("k", b"two".to_vec(), &v1).await.unwrap();
        assert_eq!(
            bucket.replace("k", b"three".to_vec(), &v1).await,
            Err(BucketError::Conflict)
        );
        let (body, v) = bucket.get("k").await.unwrap().unwrap();
        assert_eq!((body.as_slice(), &v), (&b"two"[..], &v2));
        assert_eq!(bucket.get("missing").await.unwrap(), None);
    }

    #[tokio::test]
    async fn a_conditional_replace_of_a_deleted_object_is_a_conflict() {
        // In memory answers 412; S3 answers 404 to `If-Match` on a missing
        // key. Both are a failed condition.
        let bucket = ObjectStoreBucket::new(Arc::new(InMemory::new()), "x");
        let v = bucket.create("k", b"one".to_vec()).await.unwrap();
        bucket.store.delete(&bucket.path("k")).await.unwrap();
        assert_eq!(
            bucket.replace("k", b"two".to_vec(), &v).await,
            Err(BucketError::Conflict)
        );
        let not_found = || object_store::Error::NotFound {
            path: "k".into(),
            source: "NoSuchKey".into(),
        };
        assert_eq!(put_error(true, not_found()), BucketError::Conflict);
        assert!(matches!(
            put_error(false, not_found()),
            BucketError::Unavailable(_)
        ));
    }

    #[tokio::test]
    async fn leases_work_on_object_store() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let config = LeaseConfig::new(1_000, 500, 200, 100).unwrap();
        let (ca, ta) = clock(0);
        let (cb, tb) = clock(0);
        let a = Leases::new(
            ObjectStoreBucket::new(Arc::clone(&store), "x"),
            ca,
            "a",
            config,
        );
        let b = Leases::new(
            ObjectStoreBucket::new(Arc::clone(&store), "x"),
            cb,
            "b",
            config,
        );

        let ga = a.acquire("leases/t/c/n").await.unwrap();
        assert!(matches!(
            b.acquire("leases/t/c/n").await,
            Err(LeaseError::HeldBy { .. })
        ));
        tb.store(1_501, std::sync::atomic::Ordering::Relaxed);
        let gb = b.acquire("leases/t/c/n").await.unwrap();
        assert_eq!(gb.generation, 2);
        ta.store(1_600, std::sync::atomic::Ordering::Relaxed);
        assert_eq!(a.renew(&ga).await, Err(LeaseError::Lost));
        assert!(b.renew(&gb).await.is_ok());
    }

    #[tokio::test]
    async fn heartbeats_list_only_this_prefix() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let (c1, _) = clock(10);
        let (c2, _) = clock(10);
        let (c3, _) = clock(10);
        let m1 = Membership::new(
            ObjectStoreBucket::new(Arc::clone(&store), "one"),
            c1,
            "n1",
            "a:1",
            3_000,
            0,
        )
        .unwrap();
        let m2 = Membership::new(
            ObjectStoreBucket::new(Arc::clone(&store), "one"),
            c2,
            "n2",
            "a:2",
            3_000,
            0,
        )
        .unwrap();
        let other = Membership::new(
            ObjectStoreBucket::new(Arc::clone(&store), "two"),
            c3,
            "z",
            "b:1",
            3_000,
            0,
        )
        .unwrap();
        m1.heartbeat().await.unwrap();
        m2.heartbeat().await.unwrap();
        other.heartbeat().await.unwrap();
        let ids: Vec<_> = m1
            .live_nodes()
            .await
            .unwrap()
            .into_iter()
            .map(|n| n.id)
            .collect();
        assert_eq!(ids, ["n1", "n2"]);
    }
}
