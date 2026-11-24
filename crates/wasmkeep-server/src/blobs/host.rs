//! What an actor's or the front's instance keeps to answer `blobs` calls:
//! its namespace, the app's files, and, for an actor, whether this node
//! still holds it (ADR 0030, section 3: writes need a valid hold).

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use wasmkeep_wit::wasmkeep::actor::blobs::{
    BlobError, BlobInfo, Disposition, DownloadOptions, Listing, Upload, UploadLink, UploadOptions,
    WriteOptions,
};
use wasmtime::component::{Resource, ResourceTable};
use wasmtime_wasi::p2::{DynInputStream, DynOutputStream};

use super::keys::{check_key, check_prefix, content_type};
use super::streams::{Alive, ReadStreams, Spool};
use super::{Blobs, MAX_PAGE, Namespace, Write};
use crate::cluster::LeaseView;

/// Whether the caller may write now.
#[derive(Debug, Clone)]
pub(crate) enum Gate {
    /// No hold to check: the front, or a node without a cluster.
    Free,
    /// An actor held under this lease.
    Held(LeaseView),
    /// An actor this node does not hold.
    Unheld,
}

impl Gate {
    fn valid(&self) -> bool {
        match self {
            Self::Free => true,
            Self::Held(view) => view.is_valid(),
            Self::Unheld => false,
        }
    }
}

/// An open streamed write, in the instance's resource table.
pub(crate) struct UploadEntry {
    key: String,
    write: Write,
    spool: Spool,
    body_taken: bool,
}

/// One instance's `blobs` state.
pub(crate) struct Caller {
    blobs: Option<Arc<Blobs>>,
    ns: Namespace,
    gate: Gate,
    alive: Alive,
    /// The streamed writes opened in this event, abandoned at the next.
    open: Vec<Spool>,
    /// The read streams opened in this event, closed at its end.
    reads: ReadStreams,
    /// Links made in this event (at most 1,000).
    links: u32,
    /// The actor was deleted in this event: nothing more is written.
    deleted: bool,
}

impl std::fmt::Debug for Caller {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Caller").field("ns", &self.ns).finish()
    }
}

fn not_held() -> BlobError {
    BlobError::NotAllowed("this node does not hold the actor: the write was not started".into())
}

fn lapsed() -> BlobError {
    BlobError::Unavailable(
        "this node's hold on the actor lapsed before the write was acknowledged: it may or may \
         not have landed"
            .into(),
    )
}

fn invalid(e: String) -> BlobError {
    BlobError::InvalidKey(e)
}

impl Caller {
    /// `ns`'s caller over `blobs` (`None`: no files on this node).
    pub(crate) fn new(blobs: Option<Arc<Blobs>>, ns: Namespace, gate: Gate) -> Self {
        Self {
            blobs,
            ns,
            gate,
            alive: Arc::new(AtomicBool::new(true)),
            open: Vec::new(),
            reads: ReadStreams::default(),
            links: 0,
            deleted: false,
        }
    }

    /// The actor was deleted durably in this event: its files are being
    /// swept, and every later write is refused.
    pub(crate) fn deleted(&mut self) {
        self.deleted = true;
        self.end_event();
    }

    /// The app's files, for the delete routine.
    pub(crate) fn handle(&self) -> Option<Arc<Blobs>> {
        self.blobs.clone()
    }

    /// Whether the hold is valid now.
    pub(crate) fn hold_valid(&self) -> bool {
        self.gate.valid()
    }

    /// An event starts: what the last one left open is closed and
    /// abandoned, and writes are checked against `gate` from now on.
    pub(crate) fn new_event(&mut self, gate: Gate) {
        self.end_event();
        self.alive = Arc::new(AtomicBool::new(true));
        self.gate = gate;
        self.links = 0;
    }

    /// The event ended: its streams read and write nothing more, its read
    /// streams close (their files and responses go, though the guest may
    /// keep the handles), and its open uploads are abandoned.
    pub(crate) fn end_event(&mut self) {
        self.alive.store(false, Ordering::Release);
        self.reads.close_all();
        for spool in self.open.drain(..) {
            spool.abandon();
        }
    }

    fn blobs(&self) -> Result<Arc<Blobs>, BlobError> {
        self.blobs.clone().ok_or_else(|| {
            BlobError::Unavailable("this node keeps no files for this component".into())
        })
    }

    fn may_write(&self) -> Result<(), BlobError> {
        if self.deleted {
            return Err(BlobError::NotAllowed(
                "the actor was deleted in this event".into(),
            ));
        }
        if self.gate.valid() {
            Ok(())
        } else {
            Err(not_held())
        }
    }

    /// A write's answer, unless the hold lapsed while it was under way.
    fn acknowledged<T>(&self, r: Result<T, BlobError>) -> Result<T, BlobError> {
        match r {
            Ok(_) if !self.gate.valid() => Err(lapsed()),
            r => r,
        }
    }

    pub(crate) async fn head(&self, key: String) -> Result<Option<BlobInfo>, BlobError> {
        check_key(&key).map_err(invalid)?;
        self.blobs()?.head(&self.ns, &key).await
    }

    pub(crate) async fn get(&self, key: String) -> Result<Option<(BlobInfo, Vec<u8>)>, BlobError> {
        check_key(&key).map_err(invalid)?;
        self.blobs()?.get(&self.ns, &key).await
    }

    pub(crate) async fn put(
        &self,
        key: String,
        body: Vec<u8>,
        options: WriteOptions,
    ) -> Result<BlobInfo, BlobError> {
        check_key(&key).map_err(invalid)?;
        let write = write(options)?;
        let blobs = self.blobs()?;
        self.may_write()?;
        let r = blobs.put(&self.ns, &key, body, write).await;
        self.acknowledged(r)
    }

    pub(crate) async fn delete(&self, key: String) -> Result<(), BlobError> {
        check_key(&key).map_err(invalid)?;
        let blobs = self.blobs()?;
        self.may_write()?;
        let r = blobs.delete(&self.ns, &key).await;
        self.acknowledged(r)
    }

    pub(crate) async fn list(
        &self,
        prefix: String,
        after: Option<String>,
        limit: u32,
    ) -> Result<Listing, BlobError> {
        check_prefix(&prefix).map_err(invalid)?;
        if let Some(a) = &after {
            check_key(a).map_err(invalid)?;
        }
        if limit == 0 || limit > MAX_PAGE {
            return Err(BlobError::NotAllowed(format!(
                "a listing's limit is 1 to {MAX_PAGE}, not {limit}"
            )));
        }
        let (entries, next) = self
            .blobs()?
            .list(&self.ns, &prefix, after.as_deref(), limit)
            .await?;
        Ok(Listing { entries, next })
    }

    pub(crate) async fn open_read(
        &mut self,
        table: &mut ResourceTable,
        key: String,
        range: Option<(u64, u64)>,
    ) -> wasmtime::Result<Result<Option<(BlobInfo, Resource<DynInputStream>)>, BlobError>> {
        if let Err(e) = check_key(&key) {
            return Ok(Err(invalid(e)));
        }
        let blobs = match self.blobs() {
            Ok(b) => b,
            Err(e) => return Ok(Err(e)),
        };
        if self.reads.open() >= super::MAX_OPEN_READS {
            return Ok(Err(BlobError::NotAllowed(format!(
                "at most {} read streams open at once: read one to its end or drop it first",
                super::MAX_OPEN_READS
            ))));
        }
        let (info, source) = match blobs.open_read(&self.ns, &key, range).await {
            Ok(Some(found)) => found,
            Ok(None) => return Ok(Ok(None)),
            Err(e) => return Ok(Err(e)),
        };
        let stream: DynInputStream = Box::new(self.reads.stream(source, Arc::clone(&self.alive)));
        Ok(Ok(Some((info, table.push(stream)?))))
    }

    pub(crate) async fn open_write(
        &mut self,
        table: &mut ResourceTable,
        key: String,
        options: WriteOptions,
    ) -> wasmtime::Result<Result<Resource<Upload>, BlobError>> {
        let opened = async {
            check_key(&key).map_err(invalid)?;
            let write = write(options)?;
            let blobs = self.blobs()?;
            self.may_write()?;
            self.open.retain(Spool::is_open);
            if self.open.len() >= super::MAX_OPEN_WRITES {
                return Err(BlobError::NotAllowed(format!(
                    "at most {} streamed writes open at once: finish or drop one first",
                    super::MAX_OPEN_WRITES
                )));
            }
            let spool = blobs.spool(&self.ns, None).await?;
            Ok::<_, BlobError>((write, spool))
        }
        .await;
        let (write, spool) = match opened {
            Ok(v) => v,
            Err(e) => return Ok(Err(e)),
        };
        self.open.push(spool.clone());
        let entry = table.push(UploadEntry {
            key,
            write,
            spool,
            body_taken: false,
        })?;
        Ok(Ok(Resource::new_own(entry.rep())))
    }

    pub(crate) fn body(
        &self,
        table: &mut ResourceTable,
        upload: &Resource<Upload>,
    ) -> wasmtime::Result<Result<Resource<DynOutputStream>, BlobError>> {
        let entry: Resource<UploadEntry> = Resource::new_borrow(upload.rep());
        let entry = table.get_mut(&entry)?;
        if entry.body_taken {
            return Ok(Err(BlobError::NotAllowed(
                "the upload's body was taken already".into(),
            )));
        }
        if !entry.spool.is_open() || !self.alive.load(Ordering::Acquire) {
            return Ok(Err(BlobError::NotAllowed("the upload is closed".into())));
        }
        entry.body_taken = true;
        let stream: DynOutputStream = Box::new(entry.spool.stream(Arc::clone(&self.alive)));
        Ok(Ok(table.push(stream)?))
    }

    pub(crate) async fn finish(
        &self,
        table: &mut ResourceTable,
        upload: Resource<Upload>,
    ) -> wasmtime::Result<Result<BlobInfo, BlobError>> {
        let entry: Resource<UploadEntry> = Resource::new_own(upload.rep());
        let entry = table.delete(entry)?;
        let finished = async {
            if !self.alive.load(Ordering::Acquire) {
                entry.spool.abandon();
                return Err(BlobError::NotAllowed("the upload is closed".into()));
            }
            let blobs = self.blobs()?;
            if let Err(e) = self.may_write() {
                entry.spool.abandon();
                return Err(e);
            }
            let spooled = entry.spool.finish().await?;
            let r = blobs
                .commit(&self.ns, &entry.key, spooled, entry.write)
                .await;
            self.acknowledged(r)
        };
        Ok(finished.await)
    }

    pub(crate) fn drop_upload(
        &self,
        table: &mut ResourceTable,
        upload: Resource<Upload>,
    ) -> wasmtime::Result<()> {
        let entry: Resource<UploadEntry> = Resource::new_own(upload.rep());
        table.delete(entry)?.spool.abandon();
        Ok(())
    }
}

impl Caller {
    fn count_link(&mut self) -> Result<(), BlobError> {
        if self.links >= super::links::PER_EVENT {
            return Err(BlobError::NotAllowed(format!(
                "at most {} links an event",
                super::links::PER_EVENT
            )));
        }
        self.links += 1;
        Ok(())
    }

    /// A link a browser downloads the file from (ADR 0030, section 4).
    pub(crate) async fn link_download(
        &mut self,
        key: String,
        options: DownloadOptions,
    ) -> Result<String, BlobError> {
        check_key(&key).map_err(invalid)?;
        let secs = super::links::life(options.expires_in_secs, super::links::DOWNLOAD_SECS)?;
        let (inline, filename) = match options.disposition {
            Disposition::Inline => (true, None),
            Disposition::Attachment(name) => (false, name),
        };
        let blobs = self.blobs()?;
        self.count_link()?;
        blobs
            .link_download(&self.ns, &key, inline, filename.as_deref(), secs)
            .await
    }

    /// A link a browser uploads one new file to: a write, so it needs the
    /// hold as `put` does.
    pub(crate) fn link_upload(
        &mut self,
        key: String,
        options: UploadOptions,
    ) -> Result<UploadLink, BlobError> {
        check_key(&key).map_err(invalid)?;
        let secs = super::links::life(options.expires_in_secs, super::links::UPLOAD_SECS)?;
        let blobs = self.blobs()?;
        self.may_write()?;
        self.count_link()?;
        let (url, expires_at_ms) =
            blobs.link_upload(&self.ns, &key, options.size, &options.content_type, secs)?;
        Ok(UploadLink {
            url,
            method: "PUT".into(),
            headers: vec![("content-type".into(), options.content_type)],
            expires_at_ms,
        })
    }
}

fn write(options: WriteOptions) -> Result<Write, BlobError> {
    Ok(Write {
        content_type: content_type(options.content_type.as_deref())
            .map_err(BlobError::NotAllowed)?,
        condition: options.condition,
    })
}

/// Implements `wasmkeep:actor/blobs` for a host type with a `blob` field
/// (a [`Caller`]) and a `table` field.
macro_rules! impl_blobs_host {
    ($host:ty) => {
        impl wasmkeep_wit::wasmkeep::actor::blobs::HostUpload for $host {
            async fn body(
                &mut self,
                upload: wasmtime::component::Resource<wasmkeep_wit::wasmkeep::actor::blobs::Upload>,
            ) -> wasmtime::Result<
                Result<
                    wasmtime::component::Resource<wasmtime_wasi::p2::DynOutputStream>,
                    wasmkeep_wit::wasmkeep::actor::blobs::BlobError,
                >,
            > {
                self.blob.body(&mut self.table, &upload)
            }

            async fn finish(
                &mut self,
                upload: wasmtime::component::Resource<wasmkeep_wit::wasmkeep::actor::blobs::Upload>,
            ) -> wasmtime::Result<
                Result<
                    wasmkeep_wit::wasmkeep::actor::blobs::BlobInfo,
                    wasmkeep_wit::wasmkeep::actor::blobs::BlobError,
                >,
            > {
                self.blob.finish(&mut self.table, upload).await
            }

            async fn drop(
                &mut self,
                upload: wasmtime::component::Resource<wasmkeep_wit::wasmkeep::actor::blobs::Upload>,
            ) -> wasmtime::Result<()> {
                self.blob.drop_upload(&mut self.table, upload)
            }
        }

        impl wasmkeep_wit::wasmkeep::actor::blobs::Host for $host {
            async fn head(
                &mut self,
                key: String,
            ) -> wasmtime::Result<
                Result<
                    Option<wasmkeep_wit::wasmkeep::actor::blobs::BlobInfo>,
                    wasmkeep_wit::wasmkeep::actor::blobs::BlobError,
                >,
            > {
                Ok(self.blob.head(key).await)
            }

            async fn get(
                &mut self,
                key: String,
            ) -> wasmtime::Result<
                Result<
                    Option<(wasmkeep_wit::wasmkeep::actor::blobs::BlobInfo, Vec<u8>)>,
                    wasmkeep_wit::wasmkeep::actor::blobs::BlobError,
                >,
            > {
                Ok(self.blob.get(key).await)
            }

            async fn put(
                &mut self,
                key: String,
                body: Vec<u8>,
                options: wasmkeep_wit::wasmkeep::actor::blobs::WriteOptions,
            ) -> wasmtime::Result<
                Result<
                    wasmkeep_wit::wasmkeep::actor::blobs::BlobInfo,
                    wasmkeep_wit::wasmkeep::actor::blobs::BlobError,
                >,
            > {
                Ok(self.blob.put(key, body, options).await)
            }

            async fn open_read(
                &mut self,
                key: String,
                range: Option<(u64, u64)>,
            ) -> wasmtime::Result<
                Result<
                    Option<(
                        wasmkeep_wit::wasmkeep::actor::blobs::BlobInfo,
                        wasmtime::component::Resource<wasmtime_wasi::p2::DynInputStream>,
                    )>,
                    wasmkeep_wit::wasmkeep::actor::blobs::BlobError,
                >,
            > {
                self.blob.open_read(&mut self.table, key, range).await
            }

            async fn open_write(
                &mut self,
                key: String,
                options: wasmkeep_wit::wasmkeep::actor::blobs::WriteOptions,
            ) -> wasmtime::Result<
                Result<
                    wasmtime::component::Resource<wasmkeep_wit::wasmkeep::actor::blobs::Upload>,
                    wasmkeep_wit::wasmkeep::actor::blobs::BlobError,
                >,
            > {
                self.blob.open_write(&mut self.table, key, options).await
            }

            async fn delete(
                &mut self,
                key: String,
            ) -> wasmtime::Result<Result<(), wasmkeep_wit::wasmkeep::actor::blobs::BlobError>> {
                Ok(self.blob.delete(key).await)
            }

            async fn list(
                &mut self,
                prefix: String,
                after: Option<String>,
                limit: u32,
            ) -> wasmtime::Result<
                Result<
                    wasmkeep_wit::wasmkeep::actor::blobs::Listing,
                    wasmkeep_wit::wasmkeep::actor::blobs::BlobError,
                >,
            > {
                Ok(self.blob.list(prefix, after, limit).await)
            }

            async fn link_download(
                &mut self,
                key: String,
                options: wasmkeep_wit::wasmkeep::actor::blobs::DownloadOptions,
            ) -> wasmtime::Result<Result<String, wasmkeep_wit::wasmkeep::actor::blobs::BlobError>>
            {
                Ok(self.blob.link_download(key, options).await)
            }

            async fn link_upload(
                &mut self,
                key: String,
                options: wasmkeep_wit::wasmkeep::actor::blobs::UploadOptions,
            ) -> wasmtime::Result<
                Result<
                    wasmkeep_wit::wasmkeep::actor::blobs::UploadLink,
                    wasmkeep_wit::wasmkeep::actor::blobs::BlobError,
                >,
            > {
                Ok(self.blob.link_upload(key, options))
            }
        }
    };
}

pub(crate) use impl_blobs_host;

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)] // test code
    use super::*;
    use wasmkeep_wit::wasmkeep::actor::blobs::Condition;

    fn opts() -> WriteOptions {
        WriteOptions {
            content_type: Some("text/plain".into()),
            condition: Condition::Any,
        }
    }

    fn on_disk() -> (tempfile::TempDir, Arc<Blobs>) {
        let tmp = tempfile::tempdir().unwrap();
        let blobs = Arc::new(Blobs::on_disk(
            tmp.path(),
            "e",
            super::super::DEFAULT_MAX_FILE,
        ));
        (tmp, blobs)
    }

    #[tokio::test]
    async fn a_write_without_a_hold_is_refused_and_starts_nothing() {
        let (_tmp, blobs) = on_disk();
        let ns = Namespace::actor("c", "n", "0");
        let mut caller = Caller::new(Some(Arc::clone(&blobs)), ns.clone(), Gate::Unheld);
        let mut table = ResourceTable::new();
        for refused in [
            caller.put("a".into(), b"x".to_vec(), opts()).await.err(),
            caller.delete("a".into()).await.err(),
            caller
                .open_write(&mut table, "a".into(), opts())
                .await
                .unwrap()
                .err(),
        ] {
            assert!(
                matches!(refused, Some(BlobError::NotAllowed(_))),
                "{refused:?}"
            );
        }
        // Reads need no hold.
        assert!(caller.head("a".into()).await.unwrap().is_none());
        // A valid hold writes.
        let (view, validity) = LeaseView::lapsable(1);
        caller.new_event(Gate::Held(view));
        caller.put("a".into(), b"x".to_vec(), opts()).await.unwrap();
        // Lapsed: refused again.
        validity.invalidate();
        assert!(matches!(
            caller.put("b".into(), b"x".to_vec(), opts()).await,
            Err(BlobError::NotAllowed(_))
        ));
        assert!(blobs.head(&ns, "b").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn keys_are_checked_before_anything_else() {
        let caller = Caller::new(None, Namespace::front(), Gate::Free);
        for key in ["", "../x", "a//b", "a b"] {
            assert!(matches!(
                caller.head(key.into()).await,
                Err(BlobError::InvalidKey(_))
            ));
            assert!(matches!(
                caller.put(key.into(), Vec::new(), opts()).await,
                Err(BlobError::InvalidKey(_))
            ));
        }
        assert!(matches!(
            caller.list("../".into(), None, 10).await,
            Err(BlobError::InvalidKey(_))
        ));
        assert!(matches!(
            caller.list(String::new(), Some("a/".into()), 10).await,
            Err(BlobError::InvalidKey(_))
        ));
        // A valid key on a node without files is `unavailable`, never
        // `invalid-key`.
        assert!(matches!(
            caller.head("a".into()).await,
            Err(BlobError::Unavailable(_))
        ));
    }

    #[tokio::test]
    async fn an_upload_left_open_is_abandoned_at_the_next_event() {
        let (tmp, blobs) = on_disk();
        let mut caller = Caller::new(Some(blobs), Namespace::front(), Gate::Free);
        let mut table = ResourceTable::new();
        let upload = caller
            .open_write(&mut table, "a".into(), opts())
            .await
            .unwrap()
            .unwrap();
        let body = caller.body(&mut table, &upload).unwrap().unwrap();
        let spools = || {
            std::fs::read_dir(tmp.path().join("blobs/e/_front/main/0/tmp"))
                .unwrap()
                .count()
        };
        assert_eq!(spools(), 1);
        caller.new_event(Gate::Free);
        assert_eq!(spools(), 0, "the spool file goes with the event");
        let stream = table.get_mut(&body).unwrap();
        assert!(stream.check_write().is_err());
        assert!(matches!(
            caller.finish(&mut table, upload).await.unwrap(),
            Err(BlobError::NotAllowed(_))
        ));
        assert!(caller.head("a".into()).await.unwrap().is_none());
    }
}
