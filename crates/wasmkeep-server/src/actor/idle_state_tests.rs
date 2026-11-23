//! A real `ActorState` taking its idle snapshot at hibernation (ADR 0042,
//! section 2): no hold ends while a snapshot of it uploads off the
//! mailbox, a refused or unshipped copy gives the actor up, a hold that ran
//! no guest event takes none, and a batched hold takes it at its barrier.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code

use std::sync::Arc;
use std::time::Duration;

use futures_util::StreamExt as _;
use object_store::path::Path as StorePath;
use object_store::{ObjectStore, PutOptions, PutPayload};
use wasmkeep_cluster::state_root::StateRoot;
use wasmkeep_runtime::DropCause;
use wasmkeep_storage::{Storage, StorageConfig};

use super::batched_state_tests::{node, restored, state};
use super::*;

/// A bucket whose snapshot uploads wait while the gate is shut.
#[derive(Debug)]
struct Gated {
    inner: object_store::memory::InMemory,
    open: tokio::sync::watch::Sender<bool>,
}

impl std::fmt::Display for Gated {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Gated")
    }
}

#[async_trait::async_trait]
impl ObjectStore for Gated {
    async fn put_opts(
        &self,
        location: &StorePath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        if location.as_ref().contains("/snapshot/") {
            let mut open = self.open.subscribe();
            let _ = open.wait_for(|open| *open).await;
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

struct Fixture {
    ctl: Ctl,
    mailboxes: Mailboxes<ActorState>,
    _dir: tempfile::TempDir,
}

/// What a test's mailbox job needs of the fixture: the bucket's gate and
/// this node's cluster.
#[derive(Clone)]
struct Ctl {
    gated: Arc<Gated>,
    cluster: Arc<Cluster>,
}

async fn fixture() -> Fixture {
    let gated = Arc::new(Gated {
        inner: object_store::memory::InMemory::new(),
        open: tokio::sync::watch::Sender::new(true),
    });
    let store: Arc<dyn ObjectStore> = Arc::clone(&gated) as Arc<dyn ObjectStore>;
    let cluster = node(&store).await;
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::new(
        StorageConfig::new(dir.path()),
        crate::state_dir(&StateRoot::new("", "t", "default")),
    );
    let (factory_storage, factory_cluster) = (storage, Arc::clone(&cluster));
    let mailboxes = Mailboxes::new_with_residency(
        wasmkeep_runtime::MailboxConfig::default(),
        move |address: ActorAddress, residency| {
            let (storage, cluster) = (factory_storage.clone(), Arc::clone(&factory_cluster));
            async move {
                Ok::<_, wasmkeep_runtime::BoxError>(state(address, residency, storage, cluster))
            }
        },
    );
    Fixture {
        ctl: Ctl { gated, cluster },
        mailboxes,
        _dir: dir,
    }
}

impl Ctl {
    fn shut(&self) {
        self.gated.open.send_replace(false);
    }

    /// Opens the gate `after` from now, in the background; returns when.
    fn open_after(&self, after: Duration) -> tokio::time::Instant {
        let gated = Arc::clone(&self.gated);
        tokio::spawn(async move {
            tokio::time::sleep(after).await;
            gated.open.send_replace(true);
        });
        tokio::time::Instant::now() + after
    }

    /// The transaction ids of `name`'s snapshots in the bucket.
    async fn snapshots(&self, name: &str) -> Vec<u64> {
        let mut found: Vec<u64> = self
            .gated
            .inner
            .list(None)
            .filter_map(|m| async move { m.ok() })
            .filter_map(|m| async move {
                let key = m.location.to_string();
                let rest = key.split(&format!("/{name}/snapshot/")).nth(1)?.to_owned();
                u64::from_str_radix(rest.get(..16)?, 16).ok()
            })
            .collect()
            .await;
        found.sort_unstable();
        found
    }
}

/// Restores `s` under a strict hold of this node, its first snapshot
/// uploaded, a guest event marked as run under it (unless `event` is false:
/// a maintenance job's hold).
async fn restored_strict(s: &mut ActorState, event: bool) {
    let cluster = s.cluster.clone().unwrap();
    let hold = LeaseHold::acquire(&cluster, &s.address).await.unwrap();
    let (view, generation) = (hold.view(), hold.generation().unwrap());
    let path = s.storage.db_path(s.address.class(), s.address.name());
    let mut replica = Replica::restore(&cluster, &s.address, view, generation, path, 1 << 30)
        .await
        .unwrap();
    let mut db = s
        .residency
        .retain(s.storage.open(s.address.class(), s.address.name()).unwrap());
    db.exec("CREATE TABLE t(v)", &[]).unwrap();
    db.start_capture().unwrap();
    let all = db.snapshot().unwrap();
    assert!(
        replica
            .ship_snapshot(&all, crate::replica::SnapshotKind::First)
            .await
            .unwrap()
    );
    let mut held = Held::new(hold, crate::apps::Writes::Strict);
    held.restored(Arc::new(tokio::sync::Mutex::new(replica)));
    s.held = Some(held);
    s.db = Some(db);
    if event {
        s.event_hold = s.hold_generation();
    }
}

/// One write of a strict hold, shipped as an event's is.
async fn write(s: &mut ActorState) {
    let db = s.db.as_mut().unwrap();
    db.exec("INSERT INTO t VALUES (randomblob(100))", &[])
        .unwrap();
    let changes = db.take_changes().unwrap().unwrap();
    s.replica()
        .unwrap()
        .lock()
        .await
        .ship(&changes)
        .await
        .unwrap();
}

/// A strict hold's idle snapshot whose step was cut (a job arrived) while
/// the bucket held its upload: the flight is left uploading.
async fn cut_step(f: &Ctl, s: &mut ActorState) {
    write(s).await;
    f.shut();
    let cut =
        tokio::time::timeout(Duration::from_millis(200), s.before_drop(DropCause::Idle)).await;
    assert!(cut.is_err(), "the step ended with its upload held");
    assert!(f.cluster.periodic_flights.uploading(&s.address));
}

fn idle_snapshots_counted(outcome: &str) -> u64 {
    crate::metrics::METRICS
        .idle_snapshots
        .get(&[("outcome", outcome)])
        .load(std::sync::atomic::Ordering::Relaxed)
}

/// M1: a second hibernation after a step cut short waits for the upload
/// the first one left, so the hold (released once the step is ready) never
/// ends while it may still land.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_second_hibernation_waits_for_the_upload_a_cut_step_left() {
    let f = fixture().await;
    let address = ActorAddress::new("t", "c", "again").unwrap();
    let c = f.ctl.clone();
    let (opened, returned, outcome, uploading) = f
        .mailboxes
        .submit(&address, move |s: &mut ActorState| {
            Box::pin(async move {
                let f = &c;
                restored_strict(s, true).await;
                cut_step(f, s).await;
                let opened = f.open_after(Duration::from_millis(300));
                let outcome = s.before_drop(DropCause::Idle).await;
                let returned = tokio::time::Instant::now();
                (
                    opened,
                    returned,
                    outcome,
                    f.cluster.periodic_flights.uploading(&s.address),
                )
            })
        })
        .await
        .unwrap();
    assert_eq!(outcome, StepOutcome::Ready);
    assert!(returned >= opened, "ready before the upload could end");
    assert!(!uploading, "ready with the upload under way");
    assert_eq!(
        f.ctl.snapshots("again").await,
        vec![1, 2],
        "the first step's snapshot landed"
    );
}

/// M1: a strict hold's flush to its barrier (a move's, a drain's
/// hand-over) waits for a snapshot uploading off the mailbox, within its
/// deadline; past it the hold is not at its barrier.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_strict_hand_over_waits_for_an_idle_snapshot_uploading() {
    let f = fixture().await;
    let address = ActorAddress::new("t", "c", "moved").unwrap();
    let c = f.ctl.clone();
    let (short, opened, returned, barrier, uploading) = f
        .mailboxes
        .submit(&address, move |s: &mut ActorState| {
            Box::pin(async move {
                let f = &c;
                restored_strict(s, true).await;
                cut_step(f, s).await;
                let short = s
                    .flush_to_barrier(Some(
                        tokio::time::Instant::now() + Duration::from_millis(100),
                    ))
                    .await;
                let opened = f.open_after(Duration::from_millis(300));
                let barrier = s
                    .flush_to_barrier(Some(tokio::time::Instant::now() + Duration::from_secs(10)))
                    .await;
                let returned = tokio::time::Instant::now();
                (
                    short,
                    opened,
                    returned,
                    barrier,
                    f.cluster.periodic_flights.uploading(&s.address),
                )
            })
        })
        .await
        .unwrap();
    assert_eq!(short, None, "at its barrier with the upload under way");
    assert_eq!(barrier, Some(Barrier::Flushed));
    assert!(returned >= opened && !uploading);
    f.mailboxes.hibernate(&address);
}

/// M1: a clean stop of a strict hold waits for its snapshot uploading,
/// within the stop's deadline (the upload sends no request it has not
/// started); past the deadline the hold gives up.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_strict_stop_waits_for_an_idle_snapshot_uploading_by_its_deadline() {
    let f = fixture().await;
    for (name, open, expected) in [
        ("stopped", true, StepOutcome::Ready),
        ("cut", false, StepOutcome::GaveUp),
    ] {
        let address = ActorAddress::new("t", "c", name).unwrap();
        let c = f.ctl.clone();
        let (opened, returned, outcome, uploading, held) = f
            .mailboxes
            .submit(&address, move |s: &mut ActorState| {
                Box::pin(async move {
                    let f = &c;
                    restored_strict(s, true).await;
                    cut_step(f, s).await;
                    let opened = open.then(|| f.open_after(Duration::from_millis(300)));
                    let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
                    let outcome = s
                        .before_drop(DropCause::Stop {
                            deadline: Some(deadline),
                            flush: true,
                        })
                        .await;
                    let returned = tokio::time::Instant::now();
                    (
                        opened,
                        returned,
                        outcome,
                        f.cluster.periodic_flights.uploading(&s.address),
                        s.held.is_some(),
                    )
                })
            })
            .await
            .unwrap();
        assert_eq!(outcome, expected, "{name}");
        if let Some(opened) = opened {
            assert!(returned >= opened && !uploading, "{name}");
            assert!(held, "{name}: released by the drop, after the step");
        } else {
            assert!(!held, "{name}: given up at the deadline");
        }
        f.ctl.gated.open.send_replace(true);
        f.mailboxes.hibernate(&address);
    }
}

/// S2: a hold under which no guest event ran (a delete's fence, a blob
/// sweep) takes no idle snapshot, though it wrote the head; one that ran
/// an event does.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn only_a_hold_that_ran_an_event_takes_an_idle_snapshot() {
    let f = fixture().await;
    for (name, event, expected) in [("job", false, vec![1]), ("event", true, vec![1, 2])] {
        let address = ActorAddress::new("t", "c", name).unwrap();
        f.mailboxes
            .submit(&address, move |s: &mut ActorState| {
                Box::pin(async move {
                    restored_strict(s, event).await;
                    write(s).await;
                    assert_eq!(s.before_drop(DropCause::Evicted).await, StepOutcome::Ready);
                })
            })
            .await
            .unwrap();
        assert_eq!(f.ctl.snapshots(name).await, expected, "{name}");
        f.mailboxes.hibernate(&address);
    }
}

/// S6, S5: a copy that is not the head's database is refused (counted
/// `refused`), and one with commits not shipped stops replication; either
/// way the actor is given up and nothing is uploaded.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_refused_or_unshipped_idle_snapshot_gives_the_actor_up() {
    let f = fixture().await;
    for name in ["refused", "unshipped"] {
        let address = ActorAddress::new("t", "c", name).unwrap();
        let refused = idle_snapshots_counted("refused");
        let held = f
            .mailboxes
            .submit(&address, move |s: &mut ActorState| {
                Box::pin(async move {
                    restored_strict(s, true).await;
                    write(s).await;
                    if name == "refused" {
                        s.replica().unwrap().lock().await.set_checksum_for_tests(5);
                    } else {
                        s.db.as_mut()
                            .unwrap()
                            .exec("INSERT INTO t VALUES (1)", &[])
                            .unwrap();
                    }
                    assert_eq!(s.before_drop(DropCause::Idle).await, StepOutcome::Ready);
                    s.held.is_some()
                })
            })
            .await
            .unwrap();
        assert!(!held, "{name}: the actor was not given up");
        assert_eq!(f.ctl.snapshots(name).await, vec![1], "{name}");
        if name == "refused" {
            assert!(idle_snapshots_counted("refused") > refused);
        }
        f.mailboxes.hibernate(&address);
    }
}

/// S6: a batched hold flushes to its barrier, then takes its idle snapshot
/// at the acknowledged head before its capture ends.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_batched_hold_takes_its_idle_snapshot_at_its_barrier() {
    let f = fixture().await;
    let address = ActorAddress::new("t", "c", "batched").unwrap();
    let created = idle_snapshots_counted("created");
    let head = f
        .mailboxes
        .submit(&address, move |s: &mut ActorState| {
            Box::pin(async move {
                restored(s).await;
                s.event_hold = s.hold_generation();
                let db = s.db.as_mut().unwrap();
                db.exec("INSERT INTO t VALUES (1)", &[]).unwrap();
                db.mark_boundary().unwrap().unwrap();
                assert_eq!(s.before_drop(DropCause::Idle).await, StepOutcome::Ready);
                s.replica().unwrap().lock().await.head().txid
            })
        })
        .await
        .unwrap();
    assert!(head > 1, "the write was not shipped");
    assert_eq!(f.ctl.snapshots("batched").await, vec![1, head]);
    assert!(idle_snapshots_counted("created") > created);
    f.mailboxes.hibernate(&address);
}
