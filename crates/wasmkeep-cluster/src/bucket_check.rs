//! Whether a bucket enforces the conditional writes WasmKeep relies on.
//!
//! Leases, replication and the alarm index are safe only if the bucket
//! refuses a create over an existing object (`If-None-Match: *`) and a
//! replace with a stale version (`If-Match`). An S3-compatible store that
//! accepts those requests but ignores the conditions would let two nodes
//! own one actor, with nothing to show for it until data is lost. A node
//! therefore proves both on a throwaway key before it takes anything.

use crate::lease::{Bucket, BucketError};

/// Why the check did not pass.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CheckError {
    /// The bucket answered, and broke a guarantee: never safe to run on.
    Unsafe(String),
    /// The bucket did not answer as a bucket should (unreachable, refused
    /// the credentials, an unexpected error): try again later.
    Unavailable(String),
}

/// Proves, on `key` (which must not exist; use a fresh one), that the
/// bucket refuses a create over an existing object and a replace with a
/// stale version, and accepts a replace with the current one. Leaves the
/// object behind: the caller deletes it.
pub async fn check_conditional_writes<B: Bucket>(bucket: &B, key: &str) -> Result<(), CheckError> {
    let unavailable = |step: &str, e: BucketError| CheckError::Unavailable(format!("{step}: {e}"));
    let first = match bucket.create(key, b"1".to_vec()).await {
        Ok(v) => v,
        // A fresh key cannot exist: an earlier attempt landed, or the
        // bucket answers creates with a conflict. Either way, not proven.
        Err(BucketError::Conflict) => {
            return Err(CheckError::Unavailable(
                "creating a fresh probe object was refused as a conflict".into(),
            ));
        }
        Err(e) => return Err(unavailable("creating a probe object", e)),
    };
    match bucket.create(key, b"2".to_vec()).await {
        Err(BucketError::Conflict) => {}
        Ok(_) => {
            return Err(CheckError::Unsafe(
                "the bucket let a create replace an existing object \
                 (it ignores `If-None-Match: *`)"
                    .into(),
            ));
        }
        Err(e) => return Err(unavailable("creating over the probe object", e)),
    }
    bucket
        .replace(key, b"2".to_vec(), &first)
        .await
        .map_err(|e| unavailable("replacing the probe object with its current version", e))?;
    match bucket.replace(key, b"3".to_vec(), &first).await {
        Err(BucketError::Conflict) => {}
        Ok(_) => {
            return Err(CheckError::Unsafe(
                "the bucket let a replace with a stale version through \
                 (it ignores `If-Match`)"
                    .into(),
            ));
        }
        Err(e) => {
            return Err(unavailable(
                "replacing the probe object with a stale version",
                e,
            ));
        }
    }
    // Version tokens are only ever handed back to the bucket: a read's may
    // differ from a write's (R2), so none is compared here.
    match bucket.get(key).await {
        Ok(Some((body, _))) if body == b"2" => Ok(()),
        Ok(Some((body, _))) => Err(CheckError::Unsafe(format!(
            "the probe object holds {:?} after the refused writes, not \"2\"",
            String::from_utf8_lossy(&body)
        ))),
        Ok(None) => Err(CheckError::Unavailable("the probe object vanished".into())),
        Err(e) => Err(unavailable("reading the probe object", e)),
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)] // test code

    use std::cell::RefCell;
    use std::collections::HashMap;

    use super::*;
    use crate::lease::Version;

    /// A bucket; with `lax`, one that accepts every write and ignores the
    /// conditions, as a store that does not implement them would.
    #[derive(Default)]
    struct Mem {
        objects: RefCell<HashMap<String, (Vec<u8>, u64)>>,
        next: RefCell<u64>,
        lax_create: bool,
        lax_replace: bool,
    }

    impl Mem {
        fn put(&self, key: &str, body: Vec<u8>) -> Version {
            *self.next.borrow_mut() += 1;
            let n = *self.next.borrow();
            self.objects.borrow_mut().insert(key.into(), (body, n));
            Version(n.to_string())
        }
    }

    impl Bucket for Mem {
        async fn get(&self, key: &str) -> Result<Option<(Vec<u8>, Version)>, BucketError> {
            Ok(self
                .objects
                .borrow()
                .get(key)
                .map(|(b, n)| (b.clone(), Version(n.to_string()))))
        }
        async fn create(&self, key: &str, body: Vec<u8>) -> Result<Version, BucketError> {
            if !self.lax_create && self.objects.borrow().contains_key(key) {
                return Err(BucketError::Conflict);
            }
            Ok(self.put(key, body))
        }
        async fn replace(
            &self,
            key: &str,
            body: Vec<u8>,
            expected: &Version,
        ) -> Result<Version, BucketError> {
            let current = self.objects.borrow().get(key).map(|(_, n)| n.to_string());
            if !self.lax_replace && current.as_deref() != Some(expected.0.as_str()) {
                return Err(BucketError::Conflict);
            }
            Ok(self.put(key, body))
        }
    }

    #[tokio::test]
    async fn a_bucket_that_enforces_both_conditions_passes() {
        assert_eq!(
            check_conditional_writes(&Mem::default(), "probe/a").await,
            Ok(())
        );
    }

    #[tokio::test]
    async fn a_bucket_that_ignores_if_none_match_is_unsafe() {
        let bucket = Mem {
            lax_create: true,
            ..Mem::default()
        };
        let got = check_conditional_writes(&bucket, "probe/a").await;
        assert!(
            matches!(&got, Err(CheckError::Unsafe(why)) if why.contains("If-None-Match")),
            "{got:?}"
        );
    }

    #[tokio::test]
    async fn a_bucket_that_ignores_if_match_is_unsafe() {
        let bucket = Mem {
            lax_replace: true,
            ..Mem::default()
        };
        let got = check_conditional_writes(&bucket, "probe/a").await;
        assert!(
            matches!(&got, Err(CheckError::Unsafe(why)) if why.contains("If-Match")),
            "{got:?}"
        );
    }

    #[tokio::test]
    async fn an_existing_probe_key_is_not_a_verdict() {
        let bucket = Mem::default();
        bucket.put("probe/a", b"x".to_vec());
        assert!(matches!(
            check_conditional_writes(&bucket, "probe/a").await,
            Err(CheckError::Unavailable(_))
        ));
    }
}
