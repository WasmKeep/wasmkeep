#![allow(clippy::unwrap_used)] // test code

mod batched;
mod one_reader;

use std::collections::BTreeMap;

use object_store::memory::InMemory;

use super::*;

#[test]
fn set_aside_never_leaves_an_older_wal_beside_the_newer_copy() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("a.db");
    let at = |suffix: &str| dir.path().join(format!("a.db{suffix}"));
    // The first replacement sets aside a database with its WAL.
    std::fs::write(&db, "first").unwrap();
    std::fs::write(at("-wal"), "first wal").unwrap();
    set_aside(&db).unwrap();
    assert_eq!(
        std::fs::read_to_string(at(".replaced-wal")).unwrap(),
        "first wal"
    );
    // The second sets aside one without a WAL: the first one's WAL must
    // not stay next to it, or SQLite would apply it on open.
    std::fs::write(&db, "second").unwrap();
    std::fs::write(at(".replaced-shm"), "stale").unwrap();
    set_aside(&db).unwrap();
    assert_eq!(std::fs::read_to_string(at(".replaced")).unwrap(), "second");
    assert!(!at(".replaced-wal").exists());
    assert!(!at(".replaced-shm").exists());
}

async fn replica(dir: &Path) -> Replica {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    Replica {
        store,
        prefix: "actors/t/c/a/".into(),
        generation: 1,
        lease: LeaseView::always_valid(1),
        head: SegmentId {
            generation: 1,
            txid: 1,
        },
        since_snapshot: 0,
        address: ActorAddress::new("t", "c", "a").unwrap(),
        app: Arc::from("t/default"),
        snapshot_waits: Arc::default(),
        periodic_flights: Arc::default(),
        snapshot_failures: 0,
        snapshot_retry_at: 0,
        snapshot_retry_after: tokio::time::Instant::now(),
        snapshot_retry_min: SNAPSHOT_RETRY_MIN,
        db_path: dir.join("a.db"),
        needs_snapshot: false,
        checksum: DbChecksum::from_value(7),
        max_db_bytes: 1 << 30,
        failed: None,
        in_flight: None,
        batched: super::batched::Uploads::default(),
        upload_idle: UPLOAD_IDLE,
    }
}

fn changes(pre_apply: DbChecksum) -> Changes {
    Changes {
        commit: 1,
        page_size: 512,
        pages: BTreeMap::from([(1, vec![1u8; 512])]),
        pre_apply,
        post_apply: DbChecksum::from_value(9),
    }
}

/// A snapshot of the database at `r`'s head: all of it (as from
/// `ActorDb::snapshot`), its post-apply checksum the head's.
fn at_head(r: &Replica) -> Changes {
    Changes {
        post_apply: r.checksum,
        ..changes(DbChecksum::from_value(0))
    }
}

#[tokio::test]
async fn changes_that_do_not_follow_the_head_are_refused_for_good() {
    let dir = tempfile::tempdir().unwrap();
    let mut r = replica(dir.path()).await;
    // Something committed locally was never shipped: the next capture's
    // pre-apply checksum is not the head's.
    assert!(r.ship(&changes(DbChecksum::from_value(8))).await.is_err());
    assert!(r.failed().is_some());
    // Not even a change that does follow the head goes out now.
    assert!(r.ship(&changes(DbChecksum::from_value(7))).await.is_err());
    assert!(
        list_with(&r.store, &r.prefix, None)
            .await
            .unwrap()
            .is_empty(),
        "nothing uploaded"
    );
}

/// A store whose writes land after this node's lease lapsed: each makes
/// it lapse while the upload is in flight.
struct LapseOnWrite {
    inner: InMemory,
    validity: Arc<crate::cluster::NodeValidity>,
    /// Lapse at the first batch of deletes instead (an idle pass).
    on_delete: bool,
}

impl std::fmt::Debug for LapseOnWrite {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("LapseOnWrite")
    }
}

impl std::fmt::Display for LapseOnWrite {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("LapseOnWrite")
    }
}

#[async_trait::async_trait]
impl ObjectStore for LapseOnWrite {
    async fn put_opts(
        &self,
        location: &StorePath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        if !self.on_delete {
            self.validity.invalidate();
        }
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &StorePath,
        opts: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &StorePath,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: futures_util::stream::BoxStream<'static, object_store::Result<StorePath>>,
    ) -> futures_util::stream::BoxStream<'static, object_store::Result<StorePath>> {
        if self.on_delete {
            self.validity.invalidate();
        }
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&StorePath>,
    ) -> futures_util::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
    {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&StorePath>,
    ) -> object_store::Result<object_store::ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &StorePath,
        to: &StorePath,
        options: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

/// A store whose next segment uploads (`/wal/` keys) misbehave: hang
/// without being applied, be applied and then hang (the reply lost),
/// or be refused as existing without being applied (a bucket's 409 to
/// a create racing an attempt still in flight).
struct PutFaults {
    inner: InMemory,
    hang_next: std::sync::atomic::AtomicU32,
    land_then_hang_next: std::sync::atomic::AtomicU32,
    refuse_next: std::sync::atomic::AtomicU32,
    /// The next segment uploads fail at once, not applied (a reset
    /// connection, a 500 after the client's own retries).
    fail_next: std::sync::atomic::AtomicU32,
    /// The next segment reads (a read-back's HEAD or GET) fail.
    fail_get_next: std::sync::atomic::AtomicU32,
    /// The next segment reads are answered, then their body breaks
    /// half-way; `cut_bodies` is held by each such body while it
    /// lives.
    cut_get_next: std::sync::atomic::AtomicU32,
    cut_bodies: Arc<()>,
    /// Runs `on_put` right before the segment upload of this number
    /// (1 is the first; 0: never): the lease lapsing as a resend goes.
    call_at_put: std::sync::atomic::AtomicU32,
    puts: std::sync::atomic::AtomicU32,
    on_put: std::sync::Mutex<Option<Box<dyn Fn() + Send + Sync>>>,
    /// The next segment uploads wait for a permit of `gate` (added by
    /// [`PutFaults::open_gate`]), then are applied: a PUT held in
    /// flight as long as a test wants, landing when it says.
    gate_next: std::sync::atomic::AtomicU32,
    gate: tokio::sync::Semaphore,
}

impl PutFaults {
    /// Lets one gated segment upload go on and land.
    fn open_gate(&self) {
        self.gate.add_permits(1);
    }
}

impl std::fmt::Debug for PutFaults {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PutFaults")
    }
}

impl std::fmt::Display for PutFaults {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PutFaults")
    }
}

fn take(n: &std::sync::atomic::AtomicU32) -> bool {
    use std::sync::atomic::Ordering::SeqCst;
    n.try_update(SeqCst, SeqCst, |v| v.checked_sub(1)).is_ok()
}

#[async_trait::async_trait]
impl ObjectStore for PutFaults {
    async fn put_opts(
        &self,
        location: &StorePath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        if location.as_ref().contains("/wal/") {
            let n = self.puts.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
            if n == self.call_at_put.load(std::sync::atomic::Ordering::SeqCst)
                && let Some(f) = self.on_put.lock().unwrap().as_ref()
            {
                f();
            }
            if take(&self.gate_next) {
                self.gate.acquire().await.unwrap().forget();
            }
        }
        if location.as_ref().contains("/wal/") || location.as_ref().contains("/snapshot/") {
            if take(&self.hang_next) {
                std::future::pending::<()>().await;
            }
            if take(&self.land_then_hang_next) {
                self.inner.put_opts(location, payload, opts).await?;
                return std::future::pending().await;
            }
            if take(&self.refuse_next) {
                return Err(object_store::Error::AlreadyExists {
                    path: location.to_string(),
                    source: "409 ConditionalRequestConflict".into(),
                });
            }
            if take(&self.fail_next) {
                return Err(object_store::Error::Generic {
                    store: "PutFaults",
                    source: "connection reset".into(),
                });
            }
        }
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &StorePath,
        opts: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &StorePath,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        if location.as_ref().contains("/wal/") && take(&self.fail_get_next) {
            return Err(object_store::Error::Generic {
                store: "PutFaults",
                source: "connection reset".into(),
            });
        }
        if location.as_ref().contains("/wal/") && take(&self.cut_get_next) {
            use futures_util::StreamExt;
            let got = self.inner.get_opts(location, options).await?;
            let (meta, range, attributes) =
                (got.meta.clone(), got.range.clone(), got.attributes.clone());
            let extensions = got.extensions.clone();
            let half = usize::try_from(meta.size / 2).unwrap();
            let half = got.bytes().await?.slice(..half);
            let alive = Arc::clone(&self.cut_bodies);
            let pieces = [
                Ok(half),
                Err(object_store::Error::Generic {
                    store: "PutFaults",
                    source: "connection reset".into(),
                }),
            ];
            let stream = futures_util::stream::iter(pieces).inspect(move |_| {
                let _ = &alive;
            });
            return Ok(object_store::GetResult {
                payload: object_store::GetResultPayload::Stream(Box::pin(stream)),
                meta,
                range,
                attributes,
                extensions,
            });
        }
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: futures_util::stream::BoxStream<'static, object_store::Result<StorePath>>,
    ) -> futures_util::stream::BoxStream<'static, object_store::Result<StorePath>> {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&StorePath>,
    ) -> futures_util::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
    {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&StorePath>,
    ) -> object_store::Result<object_store::ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &StorePath,
        to: &StorePath,
        options: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

/// A replica over a [`PutFaults`] store.
async fn faulty(dir: &Path, hang: u32, land_then_hang: u32) -> (Replica, Arc<PutFaults>) {
    faulty_with(dir, hang, land_then_hang, 0).await
}

async fn faulty_with(
    dir: &Path,
    hang: u32,
    land_then_hang: u32,
    refuse: u32,
) -> (Replica, Arc<PutFaults>) {
    let store = Arc::new(PutFaults {
        inner: InMemory::new(),
        hang_next: std::sync::atomic::AtomicU32::new(hang),
        land_then_hang_next: std::sync::atomic::AtomicU32::new(land_then_hang),
        refuse_next: std::sync::atomic::AtomicU32::new(refuse),
        fail_next: std::sync::atomic::AtomicU32::new(0),
        fail_get_next: std::sync::atomic::AtomicU32::new(0),
        cut_get_next: std::sync::atomic::AtomicU32::new(0),
        cut_bodies: Arc::default(),
        call_at_put: std::sync::atomic::AtomicU32::new(0),
        puts: std::sync::atomic::AtomicU32::new(0),
        on_put: std::sync::Mutex::new(None),
        gate_next: std::sync::atomic::AtomicU32::new(0),
        gate: tokio::sync::Semaphore::new(0),
    });
    let mut r = replica(dir).await;
    r.store = Arc::clone(&store) as Arc<dyn ObjectStore>;
    r.upload_idle = std::time::Duration::from_millis(200);
    (r, store)
}

/// A segment upload that fails at once (a reset connection; the
/// client's own retries spent) is sent again within the upload's
/// deadline, not given up.
#[tokio::test]
async fn a_segment_upload_that_fails_at_once_is_sent_again() {
    let dir = tempfile::tempdir().unwrap();
    let (mut r, store) = faulty(dir.path(), 0, 0).await;
    store
        .fail_next
        .store(2, std::sync::atomic::Ordering::SeqCst);
    r.ship(&changes(DbChecksum::from_value(7))).await.unwrap();
    assert_eq!(r.head.txid, 2);
    assert_eq!(store.fail_next.load(std::sync::atomic::Ordering::SeqCst), 0);
}

/// A read-back that fails (its HEAD or GET) after an attempt landed
/// unanswered is tried again within the deadline: the object may be
/// this upload's own.
#[tokio::test]
async fn a_read_back_that_fails_is_tried_again() {
    let dir = tempfile::tempdir().unwrap();
    let (mut r, store) = faulty(dir.path(), 0, 1).await;
    store
        .fail_get_next
        .store(1, std::sync::atomic::Ordering::SeqCst);
    tokio::time::timeout(
        std::time::Duration::from_secs(25),
        r.ship(&changes(DbChecksum::from_value(7))),
    )
    .await
    .expect("the upload waited past the event's limit")
    .unwrap();
    assert_eq!(r.head.txid, 2);
    assert_eq!(
        store
            .fail_get_next
            .load(std::sync::atomic::Ordering::SeqCst),
        0
    );
}

/// A refusal no resend can change (permission, authentication) still
/// ends the upload at once.
#[tokio::test]
async fn a_definite_refusal_ends_the_upload() {
    let err = object_store::Error::PermissionDenied {
        path: "x".into(),
        source: "403".into(),
    };
    assert!(definite(&err));
    let err = object_store::Error::Generic {
        store: "s",
        source: "reset".into(),
    };
    assert!(!definite(&err));
}

/// A segment upload the bucket leaves hanging is given up and sent
/// again, well within an event's 30 s, as a restore's reads are: it
/// used to wait for the client's 30 s timeout (seen in a smoke run
/// against a real bucket: one actor stalled 30 s, its event cut, the
/// actor given up).
#[tokio::test]
async fn a_hung_segment_upload_is_sent_again() {
    let dir = tempfile::tempdir().unwrap();
    let (mut r, _store) = faulty(dir.path(), 1, 0).await;
    let started = tokio::time::Instant::now();
    tokio::time::timeout(
        std::time::Duration::from_secs(25),
        r.ship(&changes(DbChecksum::from_value(7))),
    )
    .await
    .expect("the upload waited past the event's limit")
    .unwrap();
    assert!(started.elapsed() < std::time::Duration::from_secs(5));
    assert_eq!(r.head.txid, 2);
    assert_eq!(list_with(&r.store, &r.prefix, None).await.unwrap().len(), 1);
}

/// An upload that landed with its reply lost: the resend's conditional
/// create finds the object; read back, it is this upload's own bytes,
/// so the write is acknowledged (not failed, the actor not given up).
#[tokio::test]
async fn an_upload_that_landed_unanswered_is_acknowledged() {
    let dir = tempfile::tempdir().unwrap();
    let (mut r, _store) = faulty(dir.path(), 0, 1).await;
    tokio::time::timeout(
        std::time::Duration::from_secs(25),
        r.ship(&changes(DbChecksum::from_value(7))),
    )
    .await
    .expect("the upload waited past the event's limit")
    .unwrap();
    assert_eq!(r.head.txid, 2);
    assert_eq!(list_with(&r.store, &r.prefix, None).await.unwrap().len(), 1);
}

/// A create refused as existing with nothing there yet (AWS answers a
/// conditional PUT racing one still in flight with 409, which the
/// client reports as existing) is sent again, not taken for a conflict.
#[tokio::test]
async fn a_create_refused_with_nothing_there_is_sent_again() {
    let dir = tempfile::tempdir().unwrap();
    // Three refusals in a row: more than the attempts a stall may use,
    // still resent (they do not count against them).
    let (mut r, store) = faulty_with(dir.path(), 0, 0, 3).await;
    tokio::time::timeout(
        std::time::Duration::from_secs(25),
        r.ship(&changes(DbChecksum::from_value(7))),
    )
    .await
    .expect("the upload waited past the event's limit")
    .unwrap();
    assert_eq!(r.head.txid, 2);
    assert!(r.failed().is_none());
    assert_eq!(
        store.refuse_next.load(std::sync::atomic::Ordering::SeqCst),
        0
    );
    assert_eq!(
        list_with(&r.store, &r.prefix, None).await.unwrap().len(),
        1,
        "sent again, and landed"
    );
}

/// A read-back that finds this upload's own bytes after the lease
/// lapsed is still under the acknowledgement rule: `Late`, not durable.
/// An idle pass's collection (ADR 0042, section 3) deletes what
/// `idle_collectable` gives for the released generation, below the
/// newest snapshot of that generation with its segment, and keeps
/// that snapshot, its segment and every later generation's object.
/// The lease is checked before each batch: one that lapses during the
/// first stops the deletes there. With no snapshot of the generation,
/// nothing is deleted.
#[tokio::test]
async fn an_idle_collection_deletes_below_the_released_snapshot_and_stops_at_a_lapse() {
    use object_store::ObjectStoreExt as _;
    let (view, validity) = LeaseView::lapsable(3);
    let store = Arc::new(LapseOnWrite {
        inner: InMemory::new(),
        validity,
        on_delete: true,
    });
    let prefix = "actors/t/c/a/";
    let put = async |key: String| {
        store
            .inner
            .put(&StorePath::from(format!("{prefix}{key}")), vec![1].into())
            .await
            .unwrap();
    };
    // Generation 2 wrote 1..=250 and snapshotted at 250; generation 3
    // has a later segment (a late one, say).
    let seg = |txid, generation, parent| Segment {
        id: SegmentId { generation, txid },
        parent_generation: parent,
    };
    for txid in 1..=250 {
        put(segment_key(&seg(txid, 2, 2))).await;
    }
    put(snapshot_key(SegmentId {
        generation: 2,
        txid: 100,
    }))
    .await;
    put(snapshot_key(SegmentId {
        generation: 2,
        txid: 250,
    }))
    .await;
    put(segment_key(&seg(251, 3, 2))).await;
    let count = async || {
        use futures_util::TryStreamExt;
        store
            .inner
            .list(Some(&StorePath::from(prefix.trim_end_matches('/'))))
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .len()
    };
    let collector = Collector {
        store: Arc::clone(&store) as Arc<dyn ObjectStore>,
        prefix: prefix.into(),
        generation: 2,
        lease: view.clone(),
    };
    // Generation 3 released nothing it snapshotted: nothing to do.
    let other = Collector {
        generation: 3,
        ..collector.clone()
    };
    assert_eq!(
        other
            .collect_idle(std::time::Duration::ZERO, COLLECT_BATCH, &|| false)
            .await
            .unwrap(),
        IdleCollected::NoSnapshot
    );
    assert_eq!(count().await, 253);
    // A stop that says so before the first batch: nothing deleted, and
    // said.
    assert!(matches!(
        collector
            .collect_idle(std::time::Duration::ZERO, COLLECT_BATCH, &|| true)
            .await
            .unwrap(),
        IdleCollected::Collected {
            deleted: 0,
            stopped: true,
            ..
        }
    ));
    assert_eq!(count().await, 253);
    // 249 segments and one snapshot below 250: three batches, of
    // which the first runs and lapses the lease.
    let done = collector
        .collect_idle(std::time::Duration::ZERO, COLLECT_BATCH, &|| false)
        .await
        .unwrap();
    assert_eq!(
        done,
        IdleCollected::Collected {
            snapshot: SegmentId {
                generation: 2,
                txid: 250
            },
            deleted: COLLECT_BATCH,
            kept: 0,
            left: 250 - COLLECT_BATCH,
            stopped: false,
        }
    );
    assert_eq!(count().await, 253 - COLLECT_BATCH);
    assert!(matches!(
        collector
            .collect_idle(std::time::Duration::ZERO, COLLECT_BATCH, &|| false)
            .await,
        Err(ReplicaError::Late)
    ));
    // Under a valid hold, the rest goes; the snapshot at 250, its
    // segment and generation 3's segment stay. A window longer than
    // the objects' age deletes nothing.
    let valid = Collector {
        lease: LeaseView::always_valid(3),
        ..collector.clone()
    };
    assert_eq!(
        valid
            .collect_idle(std::time::Duration::from_secs(3600), COLLECT_BATCH, &|| {
                false
            })
            .await
            .unwrap(),
        IdleCollected::Collected {
            snapshot: SegmentId {
                generation: 2,
                txid: 250
            },
            deleted: 0,
            kept: 250 - COLLECT_BATCH,
            left: 0,
            stopped: false,
        }
    );
    valid
        .collect_idle(std::time::Duration::ZERO, COLLECT_BATCH, &|| false)
        .await
        .unwrap();
    let mut left: Vec<String> = {
        use futures_util::TryStreamExt;
        store
            .inner
            .list(Some(&StorePath::from(prefix.trim_end_matches('/'))))
            .map_ok(|m| m.location.to_string())
            .try_collect()
            .await
            .unwrap()
    };
    left.sort();
    let mut want = vec![
        format!(
            "{prefix}{}",
            snapshot_key(SegmentId {
                generation: 2,
                txid: 250
            })
        ),
        format!("{prefix}{}", segment_key(&seg(250, 2, 2))),
        format!("{prefix}{}", segment_key(&seg(251, 3, 2))),
    ];
    want.sort();
    assert_eq!(left, want);
}

/// The first attempt lands with its reply lost; the lease lapses as
/// the resend goes out, which meets the object and reads it back.
#[tokio::test]
async fn a_read_back_after_a_lapse_is_late() {
    let dir = tempfile::tempdir().unwrap();
    let (mut r, store) = faulty(dir.path(), 0, 1).await;
    let (view, validity) = LeaseView::lapsable(1);
    r.lease = view;
    *store.on_put.lock().unwrap() = Some(Box::new(move || validity.invalidate()));
    store
        .call_at_put
        .store(2, std::sync::atomic::Ordering::SeqCst);
    let head = r.head;
    assert!(matches!(
        r.ship(&changes(DbChecksum::from_value(7))).await,
        Err(ReplicaError::Late)
    ));
    assert_eq!(
        store.puts.load(std::sync::atomic::Ordering::SeqCst),
        2,
        "resent once"
    );
    assert_eq!(r.head, head, "the head did not move");
    assert!(r.failed().is_some());
}

/// ADR 0035, section 2: the hold cell is the home of the acknowledged
/// head. A segment's acknowledgement advances it, with the database
/// checksum after it; a periodic snapshot's only checks validity; once
/// the lease lapsed, nothing advances it, and the hold ended carries
/// the last acknowledged head.
#[tokio::test]
async fn acknowledgements_advance_the_hold_cell_head() {
    use wasmkeep_cluster::recorded_head::Position;
    let dir = tempfile::tempdir().unwrap();
    let mut r = replica(dir.path()).await;
    let (view, validity) = LeaseView::lapsable(1);
    r.lease = view.clone();
    let head = |v: &LeaseView| v.acknowledged_head();
    assert_eq!(head(&view), Some(Position::Empty), "verified at the start");
    r.ship(&changes(DbChecksum::from_value(7))).await.unwrap();
    let after = Position::Known {
        generation: 1,
        txid: 2,
        checksum: DbChecksum::from_value(9).value(),
    };
    assert_eq!(head(&view), Some(after));
    // A snapshot at the head is a check, never an advance.
    let all = at_head(&r);
    assert!(r.ship_snapshot(&all, SnapshotKind::Periodic).await.unwrap());
    assert_eq!(head(&view), Some(after));
    validity.invalidate();
    let mut next = changes(DbChecksum::from_value(9));
    next.post_apply = DbChecksum::from_value(11);
    assert!(r.ship(&next).await.is_err());
    assert_eq!(head(&view), Some(after), "nothing advances after a lapse");
}

/// After the lease lapsed, nothing is acknowledged and nothing more is
/// sent: the first attempt lands with its reply lost, and no resend
/// follows (it would only read back a write the rule refuses, `Late`).
#[tokio::test]
async fn an_upload_after_a_lapse_is_not_sent_again() {
    let dir = tempfile::tempdir().unwrap();
    let (mut r, _store) = faulty(dir.path(), 0, 1).await;
    let (view, validity) = LeaseView::lapsable(1);
    r.lease = view;
    // Lapsed already: `ship` does not look before uploading, so the
    // first attempt lands and hangs; the upload then stops.
    validity.invalidate();
    let head = r.head;
    let Err(ReplicaError::Bucket(why)) = r.ship(&changes(DbChecksum::from_value(7))).await else {
        panic!("acknowledged, or another error");
    };
    assert!(why.contains("the lease ran out"), "{why}");
    assert_eq!(
        list_with(&r.store, &r.prefix, None).await.unwrap().len(),
        1,
        "it landed"
    );
    assert_eq!(r.head, head, "the head did not move");
    assert!(r.failed().is_some());
}

/// The process's count of snapshot uploads not acknowledged. Other
/// tests of this binary may add to it meanwhile: compare, never match.
fn snapshot_failures_counted() -> u64 {
    crate::metrics::METRICS
        .snapshot_upload_failures
        .load(std::sync::atomic::Ordering::Relaxed)
}

/// A periodic snapshot gets one attempt, cut short: when the bucket
/// leaves it hanging it is given up within its bound and not sent
/// again (it used to get a segment's three attempts and whole
/// deadline), replication goes on, the count of segments since the
/// last snapshot is kept, and no next event tries at once.
#[tokio::test]
async fn a_hung_periodic_snapshot_is_given_up_after_one_short_attempt() {
    let dir = tempfile::tempdir().unwrap();
    let (mut r, store) = faulty(dir.path(), u32::MAX, 0).await;
    r.since_snapshot = SNAPSHOT_EVERY;
    let before = snapshot_failures_counted();
    let started = std::time::Instant::now();
    let shipped = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        r.ship_snapshot(&at_head(&r), SnapshotKind::Periodic),
    )
    .await
    .expect("a snapshot upload is bounded");
    let took = started.elapsed();
    assert!(
        matches!(shipped, Err(ReplicaError::Bucket(_))),
        "{shipped:?}"
    );
    // Twice the 200 ms the test's replica allows an idle bucket, not
    // the 1.8 s of a segment's whole upload.
    assert!(took >= std::time::Duration::from_millis(390), "{took:?}");
    assert!(took < std::time::Duration::from_millis(1_200), "{took:?}");
    assert_eq!(
        store.hang_next.load(std::sync::atomic::Ordering::SeqCst),
        u32::MAX - 1,
        "one attempt, not sent again"
    );
    assert_eq!(r.since_snapshot, SNAPSHOT_EVERY, "not reset by a failure");
    assert!(!r.snapshot_due(), "not tried again by the next event");
    assert!(r.failed().is_none(), "replication goes on");
    assert!(snapshot_failures_counted() > before, "counted");
    // Segments still ship; once one has and the wait has passed, the
    // next snapshot is due, and lands.
    store
        .hang_next
        .store(0, std::sync::atomic::Ordering::SeqCst);
    r.ship(&changes(DbChecksum::from_value(7))).await.unwrap();
    r.snapshot_retry_after = tokio::time::Instant::now();
    assert!(r.snapshot_due());
    let all = changes(r.checksum);
    assert!(r.ship_snapshot(&all, SnapshotKind::Periodic).await.unwrap());
    assert_eq!(r.since_snapshot, 0);
    assert!(!r.snapshot_due());
}

/// An actor's first snapshot is bounded too: one hung attempt is given
/// up and sent again, and the snapshot becomes the head.
#[tokio::test]
async fn a_hung_first_snapshot_upload_is_sent_again() {
    let dir = tempfile::tempdir().unwrap();
    let (mut r, _store) = faulty(dir.path(), 1, 0).await;
    r.needs_snapshot = true;
    let shipped = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        r.ship_snapshot(&changes(DbChecksum::from_value(7)), SnapshotKind::First),
    )
    .await
    .expect("a snapshot upload is bounded");
    assert!(shipped.unwrap(), "created and acknowledged");
    assert_eq!(r.head.txid, 2);
    assert!(!r.needs_snapshot());
    assert!(r.failed().is_none());
}

/// A periodic snapshot that keeps failing is tried again only after
/// one more segment, then eight, then every [`SNAPSHOT_EVERY`]: three
/// tries at most in any [`SNAPSHOT_EVERY`] segments.
#[tokio::test]
async fn a_snapshot_that_keeps_failing_is_tried_less_and_less_often() {
    let dir = tempfile::tempdir().unwrap();
    let (mut r, store) = faulty(dir.path(), 0, 0).await;
    r.snapshot_retry_min = std::time::Duration::ZERO;
    store
        .fail_next
        .store(u32::MAX, std::sync::atomic::Ordering::SeqCst);
    let all = at_head(&r);
    r.since_snapshot = SNAPSHOT_EVERY;
    // How many segments pass before each next attempt.
    let mut waits = Vec::new();
    for _ in 0..6 {
        assert!(r.snapshot_due());
        assert!(r.ship_snapshot(&all, SnapshotKind::Periodic).await.is_err());
        let mut waited = 0;
        while !r.snapshot_due() {
            r.since_snapshot += 1; // as a shipped segment does
            waited += 1;
        }
        waits.push(waited);
    }
    assert_eq!(waits, [1, 8, 64, 64, 64, 64]);
    // A success starts over: the count, and the waits.
    store
        .fail_next
        .store(0, std::sync::atomic::Ordering::SeqCst);
    assert!(r.ship_snapshot(&all, SnapshotKind::Periodic).await.unwrap());
    assert_eq!(r.since_snapshot, 0);
    r.since_snapshot = SNAPSHOT_EVERY;
    store
        .fail_next
        .store(u32::MAX, std::sync::atomic::Ordering::SeqCst);
    r.head.txid += 1;
    assert!(r.ship_snapshot(&all, SnapshotKind::Periodic).await.is_err());
    assert!(!r.snapshot_due());
    r.since_snapshot += 1;
    assert!(r.snapshot_due(), "one segment later");
}

/// A batched hold's snapshot due by bytes keeps both gates of a
/// failed snapshot's backoff, as one due by segments does: the
/// segments to wait for and the time (ADR 0034, section 7).
#[tokio::test]
async fn a_byte_triggered_snapshot_waits_out_the_backoff() {
    let dir = tempfile::tempdir().unwrap();
    let mut r = replica(dir.path()).await;
    r.set_snapshot_bytes(1_000);
    r.batched.bytes_since_snapshot = 5_000;
    r.since_snapshot = 3;
    assert!(r.batched_snapshot_due());
    r.snapshot_failed();
    r.since_snapshot += 1;
    assert!(!r.batched_snapshot_due(), "before the time");
    r.snapshot_retry_after = tokio::time::Instant::now();
    assert!(r.batched_snapshot_due(), "after the time, a segment on");
    r.since_snapshot -= 1;
    assert!(!r.batched_snapshot_due(), "not before its segment");
}

/// The bytes a restore counts toward a batched snapshot are the chain's
/// segments' since its snapshot, from the listing's sizes: 40 MiB, then
/// 40 more, make it due across a hibernation, and a byte-triggered
/// wait left on this node is taken up by a batched hold below 64
/// segments, never by a strict one (ADR 0034, section 7).
#[tokio::test]
async fn a_reacquired_hold_counts_its_restored_bytes_and_keeps_its_wait() {
    use wasmkeep_cluster::replication::{Segment, segment_key, snapshot_key};
    let id = |txid| SegmentId {
        generation: 1,
        txid,
    };
    let seg = |txid| {
        segment_key(&Segment {
            id: id(txid),
            parent_generation: 1,
        })
    };
    let keys = vec![snapshot_key(id(1)), seg(2), seg(3)];
    let sizes = std::collections::HashMap::from([
        (keys[0].clone(), 900 << 20),
        (keys[1].clone(), 40 << 20),
        (keys[2].clone(), 40 << 20),
    ]);
    let plan = plan_restore(&Listing::from_keys(keys.iter().map(String::as_str))).unwrap();
    assert_eq!(
        history_bytes(&plan, &keys, &sizes),
        80 << 20,
        "not the snapshot"
    );

    let dir = tempfile::tempdir().unwrap();
    let mut r = replica(dir.path()).await;
    r.since_snapshot = 1;
    r.batched.bytes_since_snapshot = 40 << 20;
    assert!(!r.batched_snapshot_due());
    r.batched.bytes_since_snapshot = 80 << 20;
    assert!(r.batched_snapshot_due());
    r.snapshot_failed();
    let waits = Arc::clone(&r.snapshot_waits);
    let reacquired = async || {
        let mut next = replica(dir.path()).await;
        next.snapshot_waits = Arc::clone(&waits);
        next.since_snapshot = 2;
        next.batched.bytes_since_snapshot = 80 << 20;
        next.resume_snapshot_wait();
        next
    };
    let mut batched = reacquired().await;
    batched.resume_batched_snapshot_wait();
    assert_eq!(batched.snapshot_failures, 1, "the wait is taken up");
    assert!(!batched.batched_snapshot_due(), "before its time");
    batched.snapshot_retry_after = tokio::time::Instant::now();
    assert!(!batched.batched_snapshot_due(), "not before its segment");
    batched.since_snapshot += 1;
    assert!(batched.batched_snapshot_due(), "after both");
    // A strict hold restored the same way forgets it, as before.
    r.snapshot_failed();
    let strict = reacquired().await;
    assert_eq!(strict.snapshot_failures, 0, "a strict hold is as before");
    assert!(strict.snapshot_waits.get(&strict.address).is_none());
}

/// The wait after a failed periodic snapshot is the node's, not the
/// `Replica`'s: the actor's next one, restored after a hibernation
/// with every segment counted again, takes it up (it used to start
/// with a snapshot due at once), in time and in segments.
#[tokio::test]
async fn a_restored_replica_takes_up_the_wait_of_the_one_before() {
    let dir = tempfile::tempdir().unwrap();
    let (mut r, store) = faulty(dir.path(), 0, 0).await;
    store
        .fail_next
        .store(u32::MAX, std::sync::atomic::Ordering::SeqCst);
    let all = at_head(&r);
    r.since_snapshot = SNAPSHOT_EVERY;
    r.head.txid = 100;
    // Two failures in a row: the next try waits for 8 segments.
    assert!(r.ship_snapshot(&all, SnapshotKind::Periodic).await.is_err());
    r.since_snapshot += 1;
    r.head.txid += 1;
    r.snapshot_retry_after = tokio::time::Instant::now();
    assert!(r.snapshot_due());
    assert!(r.ship_snapshot(&all, SnapshotKind::Periodic).await.is_err());
    let after = r.snapshot_retry_after;
    // As a restore builds it: three segments further, all counted.
    let waits = Arc::clone(&r.snapshot_waits);
    let restored = async |txid: u64, since_snapshot: u64| {
        let (mut next, _) = faulty(dir.path(), 0, 0).await;
        next.snapshot_waits = Arc::clone(&waits);
        next.head.txid = txid;
        next.since_snapshot = since_snapshot;
        next.resume_snapshot_wait();
        next
    };
    let mut next = restored(104, SNAPSHOT_EVERY + 4).await;
    assert_eq!(next.snapshot_failures, 2);
    assert_eq!(next.snapshot_retry_after, after, "the time is kept");
    next.snapshot_retry_after = tokio::time::Instant::now();
    assert!(!next.snapshot_due(), "3 of the 8 segments");
    next.since_snapshot += 4;
    assert!(!next.snapshot_due(), "7 of the 8 segments");
    next.since_snapshot += 1;
    assert!(next.snapshot_due(), "8 segments after the failure");
    // Every segment was there already, but not the time.
    let next = restored(200, SNAPSHOT_EVERY + 99).await;
    assert!(!next.snapshot_due(), "before the minimum time");
    // A snapshot landed meanwhile: nothing left to wait for.
    let next = restored(200, 3).await;
    assert_eq!(next.snapshot_failures, 0);
    assert!(r.snapshot_waits.get(&r.address).is_none());
}

/// A snapshot taken removes the node's record of the wait, and the
/// record holds a bounded number of actors: one more pushes out the
/// one whose time comes first.
#[tokio::test]
async fn the_nodes_record_of_waits_is_cleared_and_bounded() {
    let dir = tempfile::tempdir().unwrap();
    let (mut r, store) = faulty(dir.path(), 0, 0).await;
    let all = at_head(&r);
    r.since_snapshot = SNAPSHOT_EVERY;
    store
        .fail_next
        .store(1, std::sync::atomic::Ordering::SeqCst);
    assert!(r.ship_snapshot(&all, SnapshotKind::Periodic).await.is_err());
    assert_eq!(
        r.snapshot_waits.get(&r.address).map(|w| w.failures),
        Some(1)
    );
    r.since_snapshot += 1;
    r.snapshot_retry_after = tokio::time::Instant::now();
    assert!(r.ship_snapshot(&all, SnapshotKind::Periodic).await.unwrap());
    assert!(r.snapshot_waits.get(&r.address).is_none(), "cleared");

    let waits = SnapshotWaits::default();
    let now = tokio::time::Instant::now();
    let actor = |i: usize| ActorAddress::new("t", "c", format!("a{i}")).unwrap();
    let wait = |i: usize| SnapshotWait {
        failures: 1,
        txid: 1,
        after: now + std::time::Duration::from_secs(i as u64),
    };
    for i in 0..SNAPSHOT_WAITS_MAX {
        waits.set(&actor(i), wait(i + 1));
    }
    // One already there is replaced; nothing is pushed out.
    waits.set(&actor(5), wait(6));
    assert_eq!(waits.entries().len(), SNAPSHOT_WAITS_MAX);
    assert!(waits.get(&actor(0)).is_some());
    waits.set(&actor(SNAPSHOT_WAITS_MAX), wait(SNAPSHOT_WAITS_MAX + 1));
    assert_eq!(waits.entries().len(), SNAPSHOT_WAITS_MAX);
    assert!(waits.get(&actor(0)).is_none(), "the earliest went");
    assert!(waits.get(&actor(1)).is_some());
    assert!(waits.get(&actor(SNAPSHOT_WAITS_MAX)).is_some());
    waits.forget(&actor(1));
    assert!(waits.get(&actor(1)).is_none());
}

/// A failed periodic snapshot also waits a minimum time, however many
/// segments the actor ships meanwhile.
#[tokio::test]
async fn a_failed_periodic_snapshot_waits_a_minimum_time_too() {
    let dir = tempfile::tempdir().unwrap();
    let (mut r, store) = faulty(dir.path(), 0, 0).await;
    // An hour: no machine is slow enough for it to pass between the
    // try and the check after it. The test does not wait for it: it
    // moves the end of the wait instead.
    let min = std::time::Duration::from_secs(3600);
    let soon = std::time::Duration::from_millis(50);
    r.snapshot_retry_min = min;
    store
        .fail_next
        .store(1, std::sync::atomic::Ordering::SeqCst);
    let all = at_head(&r);
    r.since_snapshot = SNAPSHOT_EVERY;
    let before = tokio::time::Instant::now();
    assert!(r.ship_snapshot(&all, SnapshotKind::Periodic).await.is_err());
    r.since_snapshot += 10; // a busy actor
    assert!(!r.snapshot_due(), "the segments alone are not enough");
    assert!(
        r.snapshot_retry_after >= before + min,
        "the whole minimum, from the try"
    );
    // Due once the clock is past the end of the wait.
    r.snapshot_retry_after = tokio::time::Instant::now() + soon;
    tokio::time::sleep(soon).await;
    assert!(r.snapshot_due());
    // And the time alone is not enough either.
    store
        .fail_next
        .store(1, std::sync::atomic::Ordering::SeqCst);
    assert!(r.ship_snapshot(&all, SnapshotKind::Periodic).await.is_err());
    r.snapshot_retry_after = tokio::time::Instant::now();
    assert!(!r.snapshot_due(), "no segment since: an idle actor waits");
}

/// The snapshot after a delete is not a periodic one: when it fails
/// (every attempt, as a segment's) it is counted, and the periodic
/// snapshot's schedule is as it was.
#[tokio::test]
async fn a_failed_snapshot_after_a_delete_leaves_the_periodic_schedule_alone() {
    let dir = tempfile::tempdir().unwrap();
    let (mut r, store) = faulty(dir.path(), 0, 0).await;
    store
        .fail_next
        .store(u32::MAX, std::sync::atomic::Ordering::SeqCst);
    let all = at_head(&r);
    r.since_snapshot = 10;
    let schedule = |r: &Replica| {
        (
            r.since_snapshot,
            r.snapshot_failures,
            r.snapshot_retry_at,
            r.snapshot_retry_after,
        )
    };
    let was = schedule(&r);
    let before = snapshot_failures_counted();
    let shipped = r.ship_snapshot(&all, SnapshotKind::AfterDelete).await;
    assert!(
        matches!(shipped, Err(ReplicaError::Bucket(_))),
        "{shipped:?}"
    );
    assert_eq!(
        store.fail_next.load(std::sync::atomic::Ordering::SeqCst),
        u32::MAX - UPLOAD_ATTEMPTS,
        "sent again, as a segment is"
    );
    assert!(snapshot_failures_counted() > before, "counted");
    assert_eq!(schedule(&r), was);
    assert!(r.failed().is_none(), "replication goes on");
    // One that lands is a snapshot at the head: the count starts again.
    store
        .fail_next
        .store(0, std::sync::atomic::Ordering::SeqCst);
    r.head.txid += 1;
    assert!(
        r.ship_snapshot(&all, SnapshotKind::AfterDelete)
            .await
            .unwrap()
    );
    assert_eq!(r.since_snapshot, 0);
}

#[tokio::test]
async fn actor_restore_delete_order_uses_bucket_time_despite_node_skew() {
    let dir = tempfile::tempdir().unwrap();
    let (r, store) = faulty(dir.path(), 0, 0).await;
    let segment = Segment {
        id: r.head,
        parent_generation: r.head.generation,
    };
    let path = StorePath::from(format!("{}{}", r.prefix, segment_key(&segment)));
    store.put(&path, vec![1].into()).await.unwrap();
    let bucket_time = store
        .head(&path)
        .await
        .unwrap()
        .last_modified
        .timestamp_millis() as u64;
    let label = format!("delete {} 1 {}", r.head.txid, r.head.generation);
    assert_eq!(
        r.delete_time(
            &label,
            std::time::Duration::ZERO,
            std::time::Duration::from_secs(3603)
        )
        .await
        .unwrap(),
        Some(bucket_time)
    );
}

#[tokio::test]
async fn actor_restore_snapshot_failure_preserves_periodic_schedule() {
    let dir = tempfile::tempdir().unwrap();
    let (mut r, store) = faulty(dir.path(), 0, 0).await;
    store
        .fail_next
        .store(u32::MAX, std::sync::atomic::Ordering::SeqCst);
    let all = at_head(&r);
    let head = r.head;
    assert!(
        r.ship_snapshot(&all, SnapshotKind::AfterRestore)
            .await
            .is_err()
    );
    assert_eq!(
        store.fail_next.load(std::sync::atomic::Ordering::SeqCst),
        u32::MAX - 1
    );
    assert_eq!(r.snapshot_failures, 0);
    assert!(r.failed().is_none());
    assert_eq!(r.head, head);
    store
        .fail_next
        .store(0, std::sync::atomic::Ordering::SeqCst);
    assert!(
        r.ship_snapshot(&all, SnapshotKind::AfterRestore)
            .await
            .unwrap()
    );
    assert_eq!(r.snapshot_failures, 0);
    assert_eq!(r.head, head);
}

/// A periodic snapshot's create refused as existing with nothing
/// there is a failure, tried again later: "already there" is only
/// said of an object the bucket shows.
#[tokio::test]
async fn a_periodic_snapshot_refused_with_nothing_there_is_not_taken() {
    let dir = tempfile::tempdir().unwrap();
    let (mut r, _store) = faulty_with(dir.path(), 0, 0, 1).await;
    let all = at_head(&r);
    r.since_snapshot = SNAPSHOT_EVERY;
    let shipped = r.ship_snapshot(&all, SnapshotKind::Periodic).await;
    assert!(
        matches!(shipped, Err(ReplicaError::Bucket(_))),
        "{shipped:?}"
    );
    assert_eq!(r.since_snapshot, SNAPSHOT_EVERY);
    assert_eq!(r.snapshot_failures, 1);
    // One that is there: nothing created, and the count starts again.
    r.snapshot_retry_after = tokio::time::Instant::now();
    r.since_snapshot += 1;
    assert!(r.ship_snapshot(&all, SnapshotKind::Periodic).await.unwrap());
    r.since_snapshot = SNAPSHOT_EVERY;
    assert!(!r.ship_snapshot(&all, SnapshotKind::Periodic).await.unwrap());
    assert_eq!(r.since_snapshot, 0);
}

/// A periodic snapshot's one attempt is allowed a second per megabyte
/// only up to twice the idle bound: a database too large to upload
/// within that costs one short attempt per try, never the client's
/// own timeout.
#[test]
fn a_periodic_snapshots_attempt_is_short_whatever_the_database() {
    use std::time::Duration;
    let idle = UPLOAD_IDLE;
    assert_eq!(
        periodic_snapshot_bound(idle, 100 << 10),
        Duration::from_secs(6)
    );
    assert_eq!(
        periodic_snapshot_bound(idle, 3 << 20),
        Duration::from_secs(8)
    );
    assert_eq!(
        periodic_snapshot_bound(idle, 5 << 20),
        Duration::from_secs(10)
    );
    assert_eq!(
        periodic_snapshot_bound(idle, 100 << 20),
        Duration::from_secs(10)
    );
    assert_eq!(
        periodic_snapshot_bound(idle, usize::MAX),
        Duration::from_secs(10)
    );
}

/// Reads one object of 3 MiB whose first try fails as `fail_get_next`
/// and `cut_get_next` say, and looks at what the read holds half-way
/// through the pause before its resend.
async fn pauses_without_its_place(fail_get_next: u32, cut_get_next: u32) {
    let store = Arc::new(PutFaults {
        inner: InMemory::new(),
        hang_next: 0.into(),
        land_then_hang_next: 0.into(),
        refuse_next: 0.into(),
        fail_next: 0.into(),
        fail_get_next: fail_get_next.into(),
        cut_get_next: cut_get_next.into(),
        cut_bodies: Arc::default(),
        call_at_put: 0.into(),
        puts: 0.into(),
        on_put: std::sync::Mutex::new(None),
        gate_next: 0.into(),
        gate: tokio::sync::Semaphore::new(0),
    });
    let location = StorePath::from("a/wal/object");
    let body = vec![7u8; 3 << 20];
    store
        .inner
        .put(&location, body.clone().into())
        .await
        .unwrap();
    let size = body.len() as u64;
    let places = || {
        (
            RESTORE_READ_SLOTS.available_permits(),
            RESTORE_READ_BYTES.available_permits(),
            *RESTORE_READ_IN_FLIGHT.lock().unwrap(),
        )
    };
    let free = (RESTORE_READS, RESTORE_READ_MIB as usize, 0);
    assert_eq!(places(), free);
    let own = OwnInFlight::default();
    let (has, has_turn) = tokio::sync::watch::channel(false);
    let mut turn = Turn::new(size, Arc::clone(&own), has, Arc::from("t/default"));
    turn.take().await.unwrap();
    assert_eq!(places(), (RESTORE_READS - 1, 256 - 3, size));
    let read = {
        let store = Arc::clone(&store) as Arc<dyn ObjectStore>;
        tokio::spawn(async move {
            let time = ReadTime::new(size, Some(own));
            bounded_get(
                &store,
                &location,
                size,
                size,
                Some(size),
                &time,
                Some(&mut turn),
            )
            .await
        })
    };
    // The failure comes at once; the pause lasts half a second.
    tokio::time::sleep(READ_PAUSE / 2).await;
    let left = |n: &std::sync::atomic::AtomicU32| n.load(std::sync::atomic::Ordering::SeqCst);
    assert_eq!(
        (left(&store.fail_get_next), left(&store.cut_get_next)),
        (0, 0),
        "the first try failed"
    );
    assert_eq!(places(), free, "nothing held during the pause");
    assert_eq!(
        Arc::strong_count(&store.cut_bodies),
        1,
        "the answer that broke is dropped before the pause"
    );
    assert!(*has_turn.borrow(), "the pause is not a wait for a place");
    assert_eq!(read.await.unwrap().unwrap().len(), body.len());
    assert_eq!(places(), free, "and gave it back when the read ended");
}

/// A read the bucket refused pauses before it is sent again without
/// its place under the node's limits on reads: no slot, no megabytes,
/// no bytes counted in flight. It held all three through the pause,
/// keeping other restores' reads waiting for a read that only waited.
#[tokio::test]
async fn a_read_that_pauses_before_a_resend_gives_up_its_place() {
    pauses_without_its_place(1, 0).await;
}

/// So does a read whose body broke half-way, and it keeps nothing of
/// that answer through the pause, neither its stream nor the part
/// read: without its megabytes, nothing counts that memory, and the
/// wait for a place after the pause can be long.
#[tokio::test]
async fn a_read_whose_body_broke_keeps_nothing_of_it_while_it_pauses() {
    pauses_without_its_place(0, 1).await;
}

/// A read's limit counts every byte in flight on the node; its lift is
/// the part of it other restores' bytes account for, exactly zero for
/// a restore alone, and kept for each try that ended.
#[test]
fn a_reads_lift_is_what_other_restores_bytes_add_to_its_limit() {
    let s = std::time::Duration::from_secs;
    let (mine, theirs) = (OwnInFlight::default(), OwnInFlight::default());
    let _own = InFlightBytes::new(3 << 20, &mine);
    let time = ReadTime::new(3 << 20, Some(Arc::clone(&mine)));
    assert_eq!(time.look(), (s(8), s(0)), "alone on the node");
    {
        let _other = InFlightBytes::new(10 << 20, &theirs);
        assert_eq!(time.look(), (s(18), s(10)));
    }
    // The most seen stays, as for the limit itself.
    assert_eq!(time.look(), (s(18), s(10)));
    assert_eq!(time.lifted(), s(10));
    time.next_try();
    assert_eq!(time.lifted(), s(20), "a second try has its own");
    // Its own restore's later reads lift the limit, never the lift.
    let _more = InFlightBytes::new(20 << 20, &mine);
    assert_eq!(time.look(), (s(28), s(0)));
    // A read outside a restore counts what it is given, alone.
    assert_eq!(ReadTime::new(1, None).look(), (s(6), s(0)));
}

/// A restore's deadline is sized from what it reads: never under the
/// budget, a second more per started megabyte and per eight objects.
#[test]
fn a_restores_deadline_grows_with_what_it_reads() {
    let s = std::time::Duration::from_secs;
    assert_eq!(restore_time(RESTORE_BUDGET, 1, 1), s(32));
    // A snapshot and 63 segments, a megabyte in all.
    assert_eq!(restore_time(RESTORE_BUDGET, 64, 1 << 20), s(39));
    // A database of 1 GiB in one snapshot.
    assert_eq!(restore_time(RESTORE_BUDGET, 1, 1 << 30), s(30 + 1024 + 1));
    assert_eq!(
        restore_time(RESTORE_BUDGET, usize::MAX, u64::MAX),
        RESTORE_BUDGET
            .saturating_add(RESTORE_PER_MIB.saturating_mul(u32::MAX))
            .saturating_add(RESTORE_PER_AHEAD.saturating_mul(u32::MAX))
    );
}

/// When every attempt hangs, the upload fails within its deadline
/// (4 idles plus a second per started megabyte), replication stops,
/// and nothing more is shipped.
#[tokio::test]
async fn an_upload_that_keeps_hanging_fails_within_its_deadline() {
    let dir = tempfile::tempdir().unwrap();
    let (mut r, _store) = faulty(dir.path(), u32::MAX, 0).await;
    let started = tokio::time::Instant::now();
    assert!(matches!(
        r.ship(&changes(DbChecksum::from_value(7))).await,
        Err(ReplicaError::Bucket(_))
    ));
    let took = started.elapsed();
    // 4 × 200 ms + 1 s for the one started megabyte.
    assert!(took >= std::time::Duration::from_millis(1_700), "{took:?}");
    assert!(took < std::time::Duration::from_secs(3), "{took:?}");
    assert!(r.failed().is_some());
    assert!(matches!(
        r.ship(&changes(DbChecksum::from_value(7))).await,
        Err(ReplicaError::Local(_))
    ));
    assert!(
        list_with(&r.store, &r.prefix, None)
            .await
            .unwrap()
            .is_empty()
    );
}

/// Other bytes already at the segment's key are never taken for this
/// upload's own: still a conflict, replication stops.
#[tokio::test]
async fn other_bytes_at_the_segment_key_stay_a_conflict() {
    let dir = tempfile::tempdir().unwrap();
    let (mut r, store) = faulty(dir.path(), 0, 0).await;
    let key = segment_key(&Segment {
        id: SegmentId {
            generation: 1,
            txid: 2,
        },
        parent_generation: 1,
    });
    store
        .inner
        .put(
            &StorePath::from(format!("{}{key}", r.prefix)),
            PutPayload::from(b"someone else's".to_vec()),
        )
        .await
        .unwrap();
    assert!(matches!(
        r.ship(&changes(DbChecksum::from_value(7))).await,
        Err(ReplicaError::Exists)
    ));
    assert!(r.failed().is_some());

    // The same length, other bytes (another timestamp in the LTX
    // header): read back and compared, still a conflict.
    let dir = tempfile::tempdir().unwrap();
    let (mut r, store) = faulty(dir.path(), 0, 0).await;
    let same_length = changes(DbChecksum::from_value(7)).to_ltx(2, 2, 1).unwrap();
    store
        .inner
        .put(
            &StorePath::from(format!("{}{key}", r.prefix)),
            PutPayload::from(same_length),
        )
        .await
        .unwrap();
    assert!(matches!(
        r.ship(&changes(DbChecksum::from_value(7))).await,
        Err(ReplicaError::Exists)
    ));

    // More bytes than this upload sends: a conflict too, not a
    // restore's size error.
    let dir = tempfile::tempdir().unwrap();
    let (mut r, store) = faulty(dir.path(), 0, 0).await;
    store
        .inner
        .put(
            &StorePath::from(format!("{}{key}", r.prefix)),
            PutPayload::from(vec![0u8; 1 << 16]),
        )
        .await
        .unwrap();
    assert!(matches!(
        r.ship(&changes(DbChecksum::from_value(7))).await,
        Err(ReplicaError::Exists)
    ));
}

/// ADR 0007's acknowledgement rule under node leases: an upload that
/// completes after this node's lease lapsed (its epoch moved, so the
/// hold may no longer act) is not durable. It lands in the bucket, but `ship` answers `Late`, the
/// head does not move, and replication stops until the actor is
/// restored by its next owner.
#[tokio::test]
async fn an_upload_that_lands_after_a_lapse_is_late() {
    let dir = tempfile::tempdir().unwrap();
    let (view, validity) = LeaseView::lapsable(1);
    let mut r = replica(dir.path()).await;
    r.store = Arc::new(LapseOnWrite {
        inner: InMemory::new(),
        validity,
        on_delete: false,
    });
    r.lease = view;
    let head = r.head;
    assert!(matches!(
        r.ship(&changes(DbChecksum::from_value(7))).await,
        Err(ReplicaError::Late)
    ));
    assert_eq!(r.head, head, "the head did not move");
    assert_eq!(
        r.checksum,
        DbChecksum::from_value(7),
        "nothing recorded as replicated"
    );
    assert!(
        r.failed()
            .is_some_and(|why| why.contains("after the lease ran out")),
        "replication stopped, saying why: {:?}",
        r.failed()
    );
    assert_eq!(
        list_with(&r.store, &r.prefix, None).await.unwrap().len(),
        1,
        "the upload landed, unacknowledged"
    );
    // Nothing after it goes out.
    assert!(matches!(
        r.ship(&changes(DbChecksum::from_value(7))).await,
        Err(ReplicaError::Local(m)) if m.contains("replication stopped")
    ));
}

#[tokio::test]
async fn a_change_that_follows_the_head_is_shipped_and_becomes_the_head() {
    let dir = tempfile::tempdir().unwrap();
    let mut r = replica(dir.path()).await;
    r.ship(&changes(DbChecksum::from_value(7))).await.unwrap();
    assert_eq!(r.head.txid, 2);
    assert_eq!(r.checksum, DbChecksum::from_value(9));
    assert_eq!(list_with(&r.store, &r.prefix, None).await.unwrap().len(), 1);
}

/// What the guards of ADR 0034, section 7 must leave as it was when
/// they refuse: the head, the periodic snapshot's schedule and the
/// node's record of its wait.
fn untouched(r: &Replica) -> impl PartialEq + std::fmt::Debug + use<> {
    (
        r.head,
        r.checksum,
        r.since_snapshot,
        r.snapshot_failures,
        r.snapshot_retry_at,
        r.snapshot_retry_after,
        r.snapshot_waits.get(&r.address),
    )
}

async fn nothing_uploaded(r: &Replica) {
    assert!(
        list_with(&r.store, &r.prefix, None)
            .await
            .unwrap()
            .is_empty(),
        "nothing uploaded"
    );
}

/// A periodic snapshot, or the one after a delete, of a database that
/// is not the head's is refused before anything is uploaded or
/// counted: the periodic schedule stays as it was, and replication
/// stops (the actor is given up).
#[tokio::test]
async fn a_snapshot_labelled_at_a_head_it_does_not_hold_is_refused() {
    for kind in [
        SnapshotKind::Periodic,
        SnapshotKind::AfterDelete,
        SnapshotKind::AfterRestore,
    ] {
        let dir = tempfile::tempdir().unwrap();
        let mut r = replica(dir.path()).await;
        r.since_snapshot = SNAPSHOT_EVERY;
        let was = untouched(&r);
        let failures = snapshot_failures_counted();
        // The database moved past the head: `changes` always ends at
        // 9, and the test replica's head is at 7.
        let moved = changes(DbChecksum::from_value(7));
        let shipped = r.ship_snapshot(&moved, kind).await;
        assert!(
            matches!(&shipped, Err(ReplicaError::Refused(m)) if m.contains("labelled at head")),
            "{kind:?}: {shipped:?}"
        );
        assert!(
            r.failed().is_some_and(|f| f.starts_with("refused: ")),
            "{kind:?}: {:?}",
            r.failed()
        );
        assert_eq!(untouched(&r), was, "{kind:?}");
        assert_eq!(
            snapshot_failures_counted(),
            failures,
            "{kind:?}: not counted"
        );
        nothing_uploaded(&r).await;
        // Nothing more goes out: not the snapshot at the head either.
        assert!(r.ship_snapshot(&at_head(&r), kind).await.is_err());
        assert!(r.ship(&changes(DbChecksum::from_value(7))).await.is_err());
        nothing_uploaded(&r).await;
    }
}

/// The first snapshot is at a new position on an empty bucket: it is
/// not compared with a head, as before.
#[tokio::test]
async fn the_first_snapshot_is_not_compared_with_a_head() {
    let dir = tempfile::tempdir().unwrap();
    let mut r = replica(dir.path()).await;
    r.head = EMPTY;
    r.checksum = DbChecksum::default();
    r.needs_snapshot = true;
    let all = changes(DbChecksum::from_value(7));
    assert!(
        r.ship_snapshot(&all, SnapshotKind::First).await.unwrap(),
        "taken"
    );
    assert_eq!(r.checksum, all.post_apply);
    assert!(!r.needs_snapshot());
}

/// No snapshot of any kind is taken while a segment is in flight, even
/// one holding the database at the head.
#[tokio::test]
async fn a_snapshot_while_a_segment_is_in_flight_is_refused() {
    for kind in [
        SnapshotKind::First,
        SnapshotKind::Periodic,
        SnapshotKind::AfterDelete,
    ] {
        let dir = tempfile::tempdir().unwrap();
        let mut r = replica(dir.path()).await;
        r.since_snapshot = SNAPSHOT_EVERY;
        let segment = SegmentId {
            generation: 1,
            txid: 2,
        };
        r.in_flight = Some(segment);
        let was = untouched(&r);
        let shipped = r.ship_snapshot(&at_head(&r), kind).await;
        assert!(
            matches!(&shipped, Err(ReplicaError::Refused(m)) if m.contains("in flight")),
            "{kind:?}: {shipped:?}"
        );
        assert!(r.failed().is_some(), "{kind:?}");
        assert_eq!(untouched(&r), was, "{kind:?}");
        nothing_uploaded(&r).await;
    }
}

/// A reset while a segment is in flight is refused (its key would be
/// that segment's), and replication stops; with nothing in flight it
/// chains on the head, whatever database it replaces.
#[tokio::test]
async fn a_reset_while_a_segment_is_in_flight_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let mut r = replica(dir.path()).await;
    r.in_flight = Some(SegmentId {
        generation: 1,
        txid: 2,
    });
    let was = untouched(&r);
    let reset = r.ship_reset(changes(DbChecksum::from_value(3))).await;
    assert!(
        matches!(&reset, Err(ReplicaError::Refused(m)) if m.contains("in flight")),
        "{reset:?}"
    );
    assert!(r.failed().is_some());
    assert_eq!(untouched(&r), was);
    nothing_uploaded(&r).await;

    let dir = tempfile::tempdir().unwrap();
    let mut r = replica(dir.path()).await;
    r.ship_reset(changes(DbChecksum::from_value(3)))
        .await
        .unwrap();
    assert_eq!(r.head.txid, 2);
    assert_eq!(r.checksum, DbChecksum::from_value(9));
}

/// A segment is in flight while its upload runs, and no longer once it
/// ends; one whose `ship` was cancelled half-way (the event cut) stays
/// in flight, and replication is stopped too, so what follows is
/// refused as after any interrupted upload.
#[tokio::test]
async fn a_segment_is_in_flight_until_its_upload_ends() {
    let dir = tempfile::tempdir().unwrap();
    let (mut r, _store) = faulty(dir.path(), 1, 0).await;
    let cut = tokio::time::timeout(
        std::time::Duration::from_millis(50),
        r.ship(&changes(DbChecksum::from_value(7))),
    )
    .await;
    assert!(cut.is_err(), "the hung upload was cut");
    assert_eq!(
        r.in_flight,
        Some(SegmentId {
            generation: 1,
            txid: 2
        })
    );
    assert!(matches!(
        r.ship_snapshot(&at_head(&r), SnapshotKind::Periodic).await,
        Err(ReplicaError::Local(m)) if m.contains("replication stopped")
    ));

    let dir = tempfile::tempdir().unwrap();
    let mut r = replica(dir.path()).await;
    r.ship(&changes(DbChecksum::from_value(7))).await.unwrap();
    assert_eq!(r.in_flight, None);
    // A failed upload ends too.
    let dir = tempfile::tempdir().unwrap();
    let (mut r, store) = faulty(dir.path(), 0, 0).await;
    store
        .fail_next
        .store(u32::MAX, std::sync::atomic::Ordering::SeqCst);
    assert!(r.ship(&changes(DbChecksum::from_value(7))).await.is_err());
    assert_eq!(r.in_flight, None);
}

/// One app's restore reads fill its share of the node's read slots;
/// the other app's read takes its place at once, and every place comes
/// back.
#[tokio::test]
async fn a_saturating_apps_restore_reads_leave_the_others_share() {
    let _shares = [
        RESTORE_SLOT_SHARES.join("a/a"),
        RESTORE_SLOT_SHARES.join("b/b"),
        RESTORE_BYTE_SHARES.join("a/a"),
        RESTORE_BYTE_SHARES.join("b/b"),
    ];
    let turn = |app: &str| {
        let (has, _) = tokio::sync::watch::channel(false);
        Turn::new(1 << 20, OwnInFlight::default(), has, Arc::from(app))
    };
    let wait = std::time::Duration::from_millis(50);
    let mut held = Vec::new();
    loop {
        let mut t = turn("a/a");
        match tokio::time::timeout(wait, t.take()).await {
            Ok(taken) => {
                taken.unwrap();
                held.push(t);
            }
            Err(_) => break,
        }
    }
    assert_eq!(
        held.len(),
        RESTORE_READS * 3 / 4,
        "a stops at b's floor, half of b's part"
    );
    let mut b = turn("b/b");
    tokio::time::timeout(std::time::Duration::from_secs(5), b.take())
        .await
        .expect("b within its share is not kept waiting")
        .unwrap();
    drop(held);
    drop(b);
    assert_eq!(RESTORE_READ_SLOTS.available_permits(), RESTORE_READS);
    assert_eq!(
        RESTORE_READ_BYTES.available_permits(),
        RESTORE_READ_MIB as usize
    );
    assert_eq!(RESTORE_SLOT_SHARES.used("a/a"), 0);
    assert_eq!(RESTORE_BYTE_SHARES.used("b/b"), 0);
}

fn idle_snapshots_counted(outcome: &str) -> u64 {
    crate::metrics::METRICS
        .idle_snapshots
        .get(&[("outcome", outcome)])
        .load(std::sync::atomic::Ordering::Relaxed)
}

/// The database at `r`'s head, `pages` pages of 512 bytes.
fn at_head_of(r: &Replica, pages: u32) -> Changes {
    Changes {
        commit: pages,
        ..at_head(r)
    }
}

/// ADR 0042, section 2: an idle snapshot only from the hold that wrote
/// the head, with nothing in flight, none at the head yet, and enough
/// written since the last one. Each refusal uploads nothing, counts
/// nothing, leaves the schedule as it was and stops nothing.
#[tokio::test]
async fn an_idle_snapshot_needs_the_heads_writer_with_nothing_in_flight() {
    let skipped = async |r: &mut Replica, all: Changes| {
        let was = untouched(r);
        let started = r.start_idle_snapshot(all).await;
        let skip = started.err().expect("no idle snapshot");
        assert!(r.failed().is_none(), "{skip:?}: replication goes on");
        assert_eq!(untouched(r), was, "{skip:?}");
        assert!(r.batched.periodic.is_none(), "{skip:?}");
        assert!(!r.periodic_flights.uploading(&r.address), "{skip:?}");
        nothing_uploaded(r).await;
        skip
    };
    let dir = tempfile::tempdir().unwrap();
    let mut r = replica(dir.path()).await;
    r.since_snapshot = IDLE_SNAPSHOT_EVERY;
    // A segment in flight: not at a flushed barrier.
    r.in_flight = Some(SegmentId {
        generation: 1,
        txid: 2,
    });
    let all = at_head(&r);
    assert_eq!(skipped(&mut r, all).await, IdleSkip::InFlight);
    r.in_flight = None;
    // A hold that restored and wrote nothing: the head is the previous
    // generation's (ADR 0012, condition 2).
    r.generation = 2;
    let all = at_head(&r);
    assert_eq!(skipped(&mut r, all).await, IdleSkip::NotWriter);
    r.generation = 1;
    // A snapshot at the head already.
    r.since_snapshot = 0;
    let all = at_head(&r);
    assert_eq!(skipped(&mut r, all).await, IdleSkip::AtHead);
    // Too few segments for a database above 256 KiB; enough for one
    // at most that size.
    r.since_snapshot = IDLE_SNAPSHOT_EVERY - 1;
    let large = at_head_of(&r, 513);
    assert_eq!(skipped(&mut r, large).await, IdleSkip::BelowThreshold);
    r.since_snapshot = 1;
    assert!(r.idle_snapshot_wanted(512 * 512).is_ok());
    r.since_snapshot = IDLE_SNAPSHOT_EVERY;
    assert!(r.idle_snapshot_wanted(IDLE_SNAPSHOT_MAX_BYTES).is_ok());
    // Above the cap: never, however much was written.
    assert_eq!(
        r.idle_snapshot_wanted(IDLE_SNAPSHOT_MAX_BYTES + 1),
        Err(IdleSkip::TooLarge)
    );
    // While a failed periodic snapshot's wait runs: the bucket refuses
    // snapshots; by segments, then by time.
    r.snapshot_failures = 1;
    r.snapshot_retry_at = IDLE_SNAPSHOT_EVERY + 1;
    assert_eq!(r.idle_snapshot_wanted(512), Err(IdleSkip::BackingOff));
    r.snapshot_retry_at = 0;
    r.snapshot_retry_after = tokio::time::Instant::now() + std::time::Duration::from_secs(60);
    assert_eq!(r.idle_snapshot_wanted(512), Err(IdleSkip::BackingOff));
    r.snapshot_retry_after = tokio::time::Instant::now();
    assert!(r.idle_snapshot_wanted(512).is_ok());
    r.snapshot_failures = 0;
    // An upload a cut step left, ended since: taken in by the check,
    // not counted as uploading for good.
    let (outcome, settled) = watch::channel(None);
    r.batched.periodic = Some(PeriodicUpload {
        segments: 1,
        bytes: 0,
        settled,
    });
    assert_eq!(r.idle_snapshot_wanted(512), Err(IdleSkip::Uploading));
    outcome.send_replace(Some(PeriodicOutcome::Failed));
    assert!(r.idle_snapshot_wanted(512).is_ok());
    assert!(r.batched.periodic.is_none());
    // A database that is not the head's: refused, and replication
    // stops, as for any snapshot labelled at the head.
    let mut wrong = at_head(&r);
    wrong.post_apply = DbChecksum::from_value(99);
    let refused = r.start_idle_snapshot(wrong).await;
    assert!(
        matches!(&refused, Err(IdleSkip::Refused(_))),
        "{:?}",
        refused.err()
    );
    assert!(r.failed().is_some());
    nothing_uploaded(&r).await;
}

/// An idle snapshot leaves the periodic schedule as it was: starting it
/// counts no failed try, and one that fails changes nothing; one that
/// lands is a snapshot at the head, and the count starts again from it.
#[tokio::test]
async fn an_idle_snapshot_leaves_the_periodic_schedule_as_it_was() {
    let dir = tempfile::tempdir().unwrap();
    let (mut r, store) = faulty(dir.path(), 0, 0).await;
    r.since_snapshot = 40;
    // A failed periodic try whose wait is over: a schedule to keep.
    r.snapshot_failures = 1;
    r.snapshot_retry_at = 39;
    let was = untouched(&r);
    store
        .fail_next
        .store(u32::MAX, std::sync::atomic::Ordering::SeqCst);
    let failed = idle_snapshots_counted("failed");
    let job = r.start_idle_snapshot(at_head(&r)).await.unwrap();
    assert_eq!(job.position(), r.head());
    assert_eq!(untouched(&r), was, "starting is no failed try");
    assert_eq!(job.run().await, PeriodicOutcome::Failed);
    assert_eq!(
        store.fail_next.load(std::sync::atomic::Ordering::SeqCst),
        u32::MAX - 1,
        "one attempt"
    );
    r.settle_periodic();
    assert_eq!(untouched(&r), was, "a failure is no failed try either");
    assert!(r.failed().is_none(), "replication goes on");
    assert!(idle_snapshots_counted("failed") > failed);
    // Landed: the count starts again, the wait is over.
    store
        .fail_next
        .store(0, std::sync::atomic::Ordering::SeqCst);
    let created = idle_snapshots_counted("created");
    let job = r.start_idle_snapshot(at_head(&r)).await.unwrap();
    assert_eq!(job.run().await, PeriodicOutcome::Created);
    r.settle_periodic();
    assert_eq!(
        (r.since_snapshot, r.snapshot_failures, r.snapshot_retry_at),
        (0, 0, 0)
    );
    assert!(idle_snapshots_counted("created") > created);
    let keys = list_with(&r.store, &r.prefix, None).await.unwrap();
    assert_eq!(keys.len(), 1, "{keys:?}");
}

/// A snapshot already at the head is not uploaded again: once one
/// landed the count says so, and one already in the bucket (an earlier
/// upload given up that landed) is found there, not written over.
#[tokio::test]
async fn an_idle_snapshot_already_at_the_head_is_not_uploaded_again() {
    let dir = tempfile::tempdir().unwrap();
    let mut r = replica(dir.path()).await;
    r.since_snapshot = 3;
    let job = r.start_idle_snapshot(at_head(&r)).await.unwrap();
    assert_eq!(job.run().await, PeriodicOutcome::Created);
    r.settle_periodic();
    assert_eq!(
        r.start_idle_snapshot(at_head(&r)).await.err(),
        Some(IdleSkip::AtHead)
    );
    // The count says otherwise (it landed after its hold gave it up):
    // the conditional create finds it, and it counts as there.
    r.since_snapshot = 3;
    let existed = idle_snapshots_counted("existed");
    let job = r.start_idle_snapshot(at_head(&r)).await.unwrap();
    assert_eq!(job.run().await, PeriodicOutcome::Existed);
    assert!(idle_snapshots_counted("existed") > existed);
    r.settle_periodic();
    assert_eq!(r.since_snapshot, 0);
    let keys = list_with(&r.store, &r.prefix, None).await.unwrap();
    assert_eq!(keys.len(), 1, "{keys:?}");
}

/// The step before a drop that starts an idle snapshot is cancelled
/// when a job arrives, and the upload goes on, recorded as a flight:
/// a reset that follows (a delete, which an erasure follows) waits for
/// it, so the snapshot of the data cannot land after the reset. Here
/// the upload hangs; the reset starts only once its one attempt ends.
#[tokio::test]
async fn a_cancelled_idle_step_leaves_a_flight_a_following_reset_waits_for() {
    let dir = tempfile::tempdir().unwrap();
    let (mut r, store) = faulty(dir.path(), 1, 0).await;
    r.since_snapshot = 2;
    let job = r.start_idle_snapshot(at_head(&r)).await.unwrap();
    let settled = r.periodic_upload_settled().expect("a flight");
    // The step is dropped (a job arrived); the upload's own task goes on.
    let upload = tokio::spawn(job.run());
    drop(settled);
    assert!(r.periodic_flights.uploading(&r.address));
    let started = std::time::Instant::now();
    r.ship_reset(changes(r.checksum)).await.unwrap();
    let waited = started.elapsed();
    assert_eq!(upload.await.unwrap(), PeriodicOutcome::Failed);
    // Its one attempt: twice the 200 ms this replica allows an idle
    // bucket.
    assert!(
        waited >= std::time::Duration::from_millis(390),
        "{waited:?}"
    );
    assert_eq!(store.hang_next.load(std::sync::atomic::Ordering::SeqCst), 0);
    assert!(!r.periodic_flights.uploading(&r.address));
    assert!(r.batched.periodic.is_none(), "taken in by the reset");
    let keys = list_with(&r.store, &r.prefix, None).await.unwrap();
    assert!(
        keys.iter().all(|k| !k.contains("snapshot/")),
        "the hung snapshot never landed: {keys:?}"
    );
}
