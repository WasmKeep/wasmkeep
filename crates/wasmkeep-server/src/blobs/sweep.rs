//! The bucket side of ending files (ADR 0030, sections 5 and 7): sweep
//! records, removing a deleted incarnation's namespace, and removing the
//! files of other epochs. The decisions are `wasmkeep_cluster::blob_sweep`'s.
//!
//! Every prefix removed here is built from checked parts with a trailing
//! `/` ([`blob_sweep::namespace_folder`]), so it names one namespace, or
//! one epoch other than this runtime's own.

use futures_util::StreamExt as _;
use object_store::path::Path;
use object_store::{ObjectStoreExt as _, PutPayload};
use wasmkeep_cluster::blob_sweep::{self, Record, State};

use super::bucket::Bucket;
use super::{Backend, Blobs, keys};

/// A listed sweep record: its key under `blob-sweeps/` and age.
#[derive(Debug, Clone)]
pub(crate) struct Listed {
    pub(crate) class: String,
    pub(crate) name_id: String,
    pub(crate) incarnation: String,
    pub(crate) modified_ms: u64,
    /// The record's version as listed.
    pub(crate) etag: Option<String>,
}

impl Listed {
    /// The record's key under `blob-sweeps/`.
    pub(crate) fn key(&self) -> String {
        format!("{}/{}/{}", self.class, self.name_id, self.incarnation)
    }
}

fn err(e: impl std::fmt::Display) -> String {
    e.to_string()
}

impl Bucket {
    fn records(&self) -> String {
        format!("{}blob-sweeps/", self.root)
    }

    fn record_path(&self, class: &str, name_id: &str, inc: &str) -> Result<Path, String> {
        blob_sweep::parse_key(&format!("{class}/{name_id}/{inc}"))
            .ok_or_else(|| "not a sweep record's key".to_owned())?;
        Path::parse(format!("{}{class}/{name_id}/{inc}", self.records())).map_err(err)
    }

    /// Removes everything under `folder` (relative to `blobs/`, ending in
    /// `/`); how many objects.
    async fn remove_under(&self, folder: &str) -> Result<usize, String> {
        debug_assert!(folder.ends_with('/'));
        let prefix = Path::parse(format!(
            "{}blobs/{}",
            self.root,
            folder.trim_end_matches('/')
        ))
        .map_err(err)?;
        let want = format!("{}/", prefix.as_ref());
        // A listing that fails part way is an error, never a complete one:
        // a record dropped on it would leave files behind.
        let mut listed: Vec<Path> = Vec::new();
        let mut listing = self.store.list(Some(&prefix));
        while let Some(m) = listing.next().await {
            let m = m.map_err(err)?;
            if m.location.as_ref().starts_with(&want) {
                listed.push(m.location);
            }
        }
        let n = listed.len();
        if n == 0 {
            return Ok(0);
        }
        let stream = futures_util::stream::iter(listed.into_iter().map(Ok)).boxed();
        let mut deleted = self.store.delete_stream(stream);
        while let Some(r) = deleted.next().await {
            match r {
                Ok(_) | Err(object_store::Error::NotFound { .. }) => {}
                Err(e) => return Err(err(e)),
            }
        }
        Ok(n)
    }
}

impl Blobs {
    fn bucket(&self) -> Option<&Bucket> {
        match &self.backend {
            Backend::Bucket(b) => Some(b),
            _ => None,
        }
    }

    /// Whether files are in a bucket (sweep records, passes) rather than
    /// on this node's disk.
    pub(crate) fn on_a_bucket(&self) -> bool {
        self.bucket().is_some()
    }

    /// On a node without a bucket: removes the actor's files of every
    /// incarnation but `keep` (`None`: all).
    pub(crate) async fn remove_local(&self, class: &str, name: &str, keep: Option<&str>) {
        if let Backend::Disk(d) = &self.backend {
            d.remove_actor(class, name, keep.map(str::to_owned)).await;
        }
    }

    /// The epoch this runtime's files are under.
    pub(crate) fn epoch(&self) -> Option<&str> {
        self.bucket().map(|b| b.epoch.as_str())
    }

    /// Writes the sweep record of `class/name` at `incarnation`.
    pub(crate) async fn write_record(
        &self,
        class: &str,
        name: &str,
        incarnation: &str,
        state: State,
    ) -> Result<(), String> {
        let b = self.bucket().ok_or("no bucket")?;
        let path = b.record_path(class, &keys::name_id(name), incarnation)?;
        let body = Record {
            state,
            epoch: b.epoch.clone(),
            name: name.to_owned(),
        }
        .encode();
        b.store
            .put(&path, PutPayload::from(body.into_bytes()))
            .await
            .map(|_| ())
            .map_err(err)
    }

    /// Drops a sweep record only if it is still the version listed: a
    /// delete that rewrote it since keeps it. `Ok(false)`: it changed.
    pub(crate) async fn drop_record_if_listed(&self, l: &Listed) -> Result<bool, String> {
        let b = self.bucket().ok_or("no bucket")?;
        let path = b.record_path(&l.class, &l.name_id, &l.incarnation)?;
        match b.store.head(&path).await {
            Ok(meta) if meta.e_tag == l.etag => {}
            Ok(_) => return Ok(false),
            Err(object_store::Error::NotFound { .. }) => return Ok(true),
            Err(e) => return Err(err(e)),
        }
        self.drop_record(l).await.map(|()| true)
    }

    /// Drops a sweep record.
    pub(crate) async fn drop_record(&self, l: &Listed) -> Result<(), String> {
        let b = self.bucket().ok_or("no bucket")?;
        match b
            .store
            .delete(&b.record_path(&l.class, &l.name_id, &l.incarnation)?)
            .await
        {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(e) => Err(err(e)),
        }
    }

    /// Every sweep record, with its age's origin; keys this version does
    /// not know are left out (and alone).
    pub(crate) async fn records(&self) -> Result<Vec<Listed>, String> {
        let b = self.bucket().ok_or("no bucket")?;
        let base = b.records();
        let prefix = Path::parse(base.trim_end_matches('/')).map_err(err)?;
        let mut out = Vec::new();
        let mut listing = b.store.list(Some(&prefix));
        while let Some(m) = listing.next().await {
            let m = m.map_err(err)?;
            let Some(tail) = m.location.as_ref().strip_prefix(base.as_str()) else {
                continue;
            };
            let Some((class, name_id, incarnation)) = blob_sweep::parse_key(tail) else {
                continue;
            };
            out.push(Listed {
                class: class.to_owned(),
                name_id: name_id.to_owned(),
                incarnation: incarnation.to_owned(),
                modified_ms: u64::try_from(m.last_modified.timestamp_millis()).unwrap_or(0),
                etag: m.e_tag.clone(),
            });
        }
        Ok(out)
    }

    /// A record's body, or `None` if it is gone or not one this version
    /// knows.
    pub(crate) async fn read_record(&self, l: &Listed) -> Result<Option<Record>, String> {
        let b = self.bucket().ok_or("no bucket")?;
        match b
            .store
            .get(&b.record_path(&l.class, &l.name_id, &l.incarnation)?)
            .await
        {
            Ok(r) => Ok(Record::decode(&r.bytes().await.map_err(err)?)),
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(err(e)),
        }
    }

    /// Removes the files of one incarnation, under this runtime's epoch.
    pub(crate) async fn remove_incarnation(
        &self,
        class: &str,
        name_id: &str,
        incarnation: &str,
    ) -> Result<usize, String> {
        let b = self.bucket().ok_or("no bucket")?;
        let folder = blob_sweep::namespace_folder(&b.epoch, class, name_id, incarnation)
            .ok_or("not a namespace")?;
        b.remove_under(&folder).await
    }

    /// The epochs other than this runtime's that files are under.
    pub(crate) async fn other_epochs(&self) -> Result<Vec<String>, String> {
        let b = self.bucket().ok_or("no bucket")?;
        let base = format!("{}blobs/", b.root);
        let prefix = Path::parse(base.trim_end_matches('/')).map_err(err)?;
        let listed = b
            .store
            .list_with_delimiter(Some(&prefix))
            .await
            .map_err(err)?;
        Ok(listed
            .common_prefixes
            .iter()
            .filter_map(|p| p.as_ref().strip_prefix(base.as_str()).map(str::to_owned))
            .filter(|e| blob_sweep::is_epoch(e) && *e != b.epoch)
            .collect())
    }

    /// The epoch the app's markers give now (read again from the bucket).
    pub(crate) async fn fresh_epoch(
        &self,
        prefix: &str,
        tenant: &str,
        app: &str,
    ) -> Result<String, String> {
        let b = self.bucket().ok_or("no bucket")?;
        let mut bodies = Vec::new();
        for marker in ["recreated", "restored"] {
            let path = Path::from(format!("{prefix}apps/{tenant}/{app}/{marker}"));
            match b.store.get(&path).await {
                Ok(r) => bodies.push(Some(r.bytes().await.map_err(err)?.to_vec())),
                Err(object_store::Error::NotFound { .. }) => bodies.push(None),
                Err(e) => return Err(err(e)),
            }
        }
        Ok(super::epoch(bodies[0].as_deref(), bodies[1].as_deref()))
    }

    /// Removes the files of `epoch`, one other than this runtime's.
    pub(crate) async fn remove_epoch(&self, epoch: &str) -> Result<usize, String> {
        let b = self.bucket().ok_or("no bucket")?;
        if !blob_sweep::is_epoch(epoch) || epoch == b.epoch {
            return Err("not another epoch".into());
        }
        b.remove_under(&format!("{epoch}/")).await
    }
}

/// After a delete the bucket acknowledged under a valid hold: the record
/// `swept`, then the old incarnation's files removed, in the background. A
/// record that cannot be rewritten stays `pending`, for the pass to judge.
pub(crate) fn sweep_after_delete(
    blobs: std::sync::Arc<Blobs>,
    class: String,
    name: String,
    incarnation: String,
) {
    if !blobs.on_a_bucket() {
        return;
    }
    tokio::spawn(async move {
        if let Err(e) = blobs
            .write_record(&class, &name, &incarnation, State::Swept)
            .await
        {
            tracing::warn!(%class, error = %e, "marking a deleted actor's files swept failed; the sweep pass judges it");
            return;
        }
        if let Err(e) = blobs
            .remove_incarnation(&class, &keys::name_id(&name), &incarnation)
            .await
        {
            tracing::warn!(%class, error = %e, "removing a deleted actor's files failed; the sweep pass lists them again");
        }
    });
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)] // test code
    use std::sync::Arc;

    use object_store::ObjectStore;
    use object_store::memory::InMemory;

    use super::*;

    #[tokio::test]
    async fn removing_an_incarnation_touches_no_other_namespace() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let tmp = tempfile::tempdir().unwrap();
        let blobs = Blobs::on_bucket(
            Arc::clone(&store),
            "p/",
            "state/a/b/",
            "00000000000000e1",
            tmp.path().join("s"),
            super::super::DEFAULT_MAX_FILE,
        );
        let id = keys::name_id("n");
        let other = keys::name_id("m");
        let keep = [
            format!("p/state/a/b/blobs/00000000000000e1/c/{id}/0123456789abcdef/k"),
            format!("p/state/a/b/blobs/00000000000000e1/c/{other}/0/k"),
            format!("p/state/a/b/blobs/00000000000000e1/cc/{id}/0/k"),
            format!("p/state/a/b/blobs/00000000000000e1/c/{id}/00/k"),
        ];
        let gone = [
            format!("p/state/a/b/blobs/00000000000000e1/c/{id}/0/k"),
            format!("p/state/a/b/blobs/00000000000000e1/c/{id}/0/d/k"),
        ];
        for k in keep.iter().chain(&gone) {
            store
                .put(&Path::parse(k).unwrap(), PutPayload::from_static(b"x"))
                .await
                .unwrap();
        }
        assert_eq!(blobs.remove_incarnation("c", &id, "0").await.unwrap(), 2);
        for k in &keep {
            assert!(store.head(&Path::parse(k).unwrap()).await.is_ok(), "{k}");
        }
        for k in &gone {
            assert!(store.head(&Path::parse(k).unwrap()).await.is_err(), "{k}");
        }
        assert!(blobs.remove_incarnation("c", "short", "0").await.is_err());
        assert!(blobs.remove_incarnation("c", &id, "").await.is_err());
        // Other epochs: listed and removed, never this one.
        store
            .put(
                &Path::parse("p/state/a/b/blobs/00000000000000e0/c/x").unwrap(),
                PutPayload::from_static(b"x"),
            )
            .await
            .unwrap();
        store
            .put(
                &Path::parse("p/state/a/b/blobs/stray/x").unwrap(),
                PutPayload::from_static(b"x"),
            )
            .await
            .unwrap();
        assert_eq!(blobs.other_epochs().await.unwrap(), ["00000000000000e0"]);
        assert!(blobs.remove_epoch("00000000000000e1").await.is_err());
        assert_eq!(blobs.remove_epoch("00000000000000e0").await.unwrap(), 1);
        assert!(store.head(&Path::parse(&keep[0]).unwrap()).await.is_ok());
    }

    #[tokio::test]
    async fn records_are_written_listed_and_dropped() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let tmp = tempfile::tempdir().unwrap();
        let blobs = Blobs::on_bucket(
            Arc::clone(&store),
            "",
            "state/a/b/",
            "00000000000000e1",
            tmp.path().join("s"),
            super::super::DEFAULT_MAX_FILE,
        );
        blobs
            .write_record("c", "n m", "0", State::Pending)
            .await
            .unwrap();
        store
            .put(
                &Path::from("state/a/b/blob-sweeps/c/odd"),
                PutPayload::from_static(b"x"),
            )
            .await
            .unwrap();
        let listed = blobs.records().await.unwrap();
        assert_eq!(listed.len(), 1);
        let r = blobs.read_record(&listed[0]).await.unwrap().unwrap();
        assert_eq!(
            (r.state, r.name.as_str(), r.epoch.as_str()),
            (State::Pending, "n m", "00000000000000e1")
        );
        // Rewritten since it was listed (a new delete): kept.
        blobs
            .write_record("c", "n m", "0", State::Swept)
            .await
            .unwrap();
        assert!(!blobs.drop_record_if_listed(&listed[0]).await.unwrap());
        assert_eq!(blobs.records().await.unwrap().len(), 1);
        let again = blobs.records().await.unwrap();
        assert!(blobs.drop_record_if_listed(&again[0]).await.unwrap());
        assert!(blobs.records().await.unwrap().is_empty());
    }
}
