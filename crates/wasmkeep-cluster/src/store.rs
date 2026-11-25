//! What some commands and checks need from a bucket beyond conditional
//! writes ([`Bucket`]): listings, server-side copies and plain writes and
//! deletes. Implemented over `object_store` in
//! `crate::object_store::ObjectStoreBucket`.

use crate::lease::{Bucket, BucketError};

/// One listed object.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ObjectMeta {
    /// Its key relative to the bucket prefix, as written (not as the
    /// store escapes it).
    pub key: String,
    /// Its size in bytes.
    pub size: u64,
    /// When it was last written, by the bucket's clock (ms).
    pub modified_ms: u64,
    /// Its entity tag, if the store gives one.
    pub etag: Option<String>,
}

/// A bucket that lists, copies, writes and deletes, beyond conditional
/// writes.
#[allow(async_fn_in_trait)]
pub trait Store: Bucket {
    /// Every object under `prefix`, in key order.
    async fn list(&self, prefix: &str) -> Result<Vec<ObjectMeta>, BucketError>;
    /// The names of the folders directly under `prefix` (which ends in
    /// `/`), as written.
    async fn list_dirs(&self, prefix: &str) -> Result<Vec<String>, BucketError>;
    /// Copies `from` to `to` inside the bucket, overwriting it.
    async fn copy(&self, from: &str, to: &str) -> Result<(), BucketError>;
    /// Writes `key`, whatever is there.
    async fn put(&self, key: &str, body: Vec<u8>) -> Result<(), BucketError>;
    /// Deletes `key` (absent is fine).
    async fn delete(&self, key: &str) -> Result<(), BucketError>;
}
