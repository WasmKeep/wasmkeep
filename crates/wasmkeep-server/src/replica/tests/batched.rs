//! The batched uploader (ADR 0034, S8) on the fault store, with a real
//! actor database: real WAL captures, real checksums, a real sidecar.
//!
//! Each test names the rule of the split model (`sim/tests/support/batched.rs`)
//! it checks on the real code: the `Switch` that breaks the rule there, and
//! the `Violation` it causes. The model leaves out bytes, the byte cap and
//! the node budget, the sidecar file and checksums; the tests that check
//! those say so.

use std::path::Path;
use std::sync::atomic::Ordering::SeqCst;
use std::time::Duration;

use wasmkeep_storage::wal::{Restorer, WalBoundary};
use wasmkeep_storage::{ActorDb, CaptureHold, StateDir, Storage, StorageConfig};

use super::{PutFaults, faulty};
use crate::cluster::LeaseView;
use crate::replica::{
    Applied, EMPTY, Listing, Replica, ReplicaError, SegmentId, SnapshotKind, UploadBudget,
    UploadFailure, list_with, plan_restore,
};
use std::sync::Arc;
use wasmkeep_cluster::alarm_index::Entry;
use wasmkeep_storage::ltx::DbChecksum;

/// Long enough for any upload of these tests (an upload's budget at the
/// tests' 200 ms idle is under 2 s), short of hanging a run.
const PATIENCE: Duration = Duration::from_secs(25);

/// A batched actor: a replica over the fault store whose first snapshot
/// (txid 1) is uploaded, and its database, capturing in batched mode.
/// `checkpoint` is the WAL size from which a capture checkpoints.
struct Actor {
    r: Replica,
    store: Arc<PutFaults>,
    db: ActorDb,
    /// Dropped before `db` in these tests, as a hold that gives up drops
    /// it: commits may be left uncaptured.
    hold: CaptureHold,
}

async fn actor(dir: &Path, checkpoint: u64) -> Actor {
    let (mut r, store) = faulty(dir, 0, 0).await;
    let mut config = StorageConfig::new(dir.join("data"));
    config.max_wal_bytes = checkpoint;
    let storage = Storage::new(
        config,
        StateDir {
            dir: "actors".into(),
            scratch: "scratch".into(),
            tenant: "t".into(),
        },
    );
    let mut db = storage.open("c", "a").unwrap();
    db.exec("CREATE TABLE t(v TEXT)", &[]).unwrap();
    db.start_capture().unwrap();
    r.db_path = db.path().to_owned();
    r.head = EMPTY;
    r.checksum = DbChecksum::default();
    r.needs_snapshot = true;
    let all = db.snapshot().unwrap();
    assert!(r.ship_snapshot(&all, SnapshotKind::First).await.unwrap());
    let hold = CaptureHold::default();
    db.enable_batched_capture(&hold).unwrap();
    Actor { r, store, db, hold }
}

/// One event: a committed row, then its boundary.
fn write(db: &mut ActorDb, v: &str) -> WalBoundary {
    db.exec(&format!("INSERT INTO t VALUES ('{v}')"), &[])
        .unwrap();
    db.mark_boundary().unwrap().unwrap()
}

/// Reserves, prepares and hands off the next segment.
async fn start(r: &mut Replica, db: &mut ActorDb, budget: &UploadBudget) -> SegmentId {
    let plan = db.next_bounded_capture().unwrap();
    let permit = budget.reserve(plan.boundary.estimated_bytes).await.unwrap();
    r.prepare(plan, permit).await.unwrap().hand_off(db).unwrap()
}

/// Waits for the segment in flight to land, then applies it.
async fn land(r: &mut Replica) -> Result<Applied, ReplicaError> {
    tokio::time::timeout(PATIENCE, r.landed())
        .await
        .expect("the upload outlasted its budget");
    r.apply().await
}

/// What the sidecar names: generation, txid, checksum.
fn sidecar_of(r: &Replica) -> (u64, u64, u64) {
    let text = std::fs::read_to_string(super::sidecar(&r.db_path)).unwrap();
    let v: Vec<u64> = text
        .split_whitespace()
        .map(|p| p.parse().unwrap())
        .collect();
    (v[0], v[1], v[2])
}

/// The sidecar naming `r`'s acknowledged head.
fn names_head(r: &Replica) -> (u64, u64, u64) {
    let (head, sum) = r.acknowledged_position();
    (head.generation, head.txid, sum.value())
}

/// The head a restore would find in the bucket now.
async fn bucket_head(r: &Replica) -> SegmentId {
    let keys = list_with(&r.store, &r.prefix, None).await.unwrap();
    plan_restore(&Listing::from_keys(keys.iter().map(String::as_str)))
        .unwrap()
        .head()
}

/// Restores the bucket's chain into a new file: its checksum. The restorer
/// checks every segment's pre-apply checksum against the one before.
async fn restored(r: &Replica, dir: &Path) -> DbChecksum {
    use object_store::ObjectStoreExt;
    let keys = list_with(&r.store, &r.prefix, None).await.unwrap();
    let plan = plan_restore(&Listing::from_keys(keys.iter().map(String::as_str))).unwrap();
    let mut objects = vec![super::snapshot_key(plan.snapshot.unwrap())];
    for id in &plan.segments {
        let key = keys
            .iter()
            .find(|k| {
                Listing::from_keys([k.as_str()])
                    .segments
                    .first()
                    .is_some_and(|s| s.id == *id)
            })
            .unwrap();
        objects.push(key.clone());
    }
    let mut out = Restorer::create(&dir.join("restored.db"), 1 << 30).unwrap();
    for key in objects {
        let at = object_store::path::Path::from(format!("{}{key}", r.prefix));
        let bytes = r.store.get(&at).await.unwrap().bytes().await.unwrap();
        out.apply(&bytes).unwrap();
    }
    out.finish().unwrap()
}

/// Whether `fut` is still pending after a short while.
async fn pending<F: std::future::Future + Unpin>(fut: &mut F) -> bool {
    tokio::time::timeout(Duration::from_millis(50), fut)
        .await
        .is_err()
}

/// Segments are acknowledged one at a time, in order, each chaining on the
/// one before; the acknowledged head only moves forward; a waiter wakes at
/// the acknowledgement of its boundary, never at its capture.
///
/// Model: `Switch::HeadBackwards` (`Violation::HeadBackwards`),
/// `Switch::FlushCaptured` (`Violation::DurableLost`), and the chain
/// `plan_restore` must find whole (`Violation::MissingLink`). A second
/// segment started while one is in flight (`Switch::TwoInFlight`) is
/// refused, and replication stops.
#[tokio::test]
async fn segments_are_acknowledged_in_order_and_chain() {
    let dir = tempfile::tempdir().unwrap();
    let Actor {
        mut r,
        store: _store,
        mut db,
        hold,
    } = actor(dir.path(), 1 << 30).await;
    let budget = UploadBudget::node();
    let mut last = r.head();
    for i in 0..3 {
        // Two events in the first segment: batched, one segment for both.
        if i == 0 {
            write(&mut db, "first");
        }
        let b = write(&mut db, &format!("row {i}"));
        let mut waiter = Box::pin(r.wait_acknowledged(b));
        let id = start(&mut r, &mut db, &budget).await;
        assert_eq!(id.txid, last.txid + 1);
        assert_eq!(r.captured().0, id, "captured position: the segment's end");
        assert_eq!(r.acknowledged_position().0, last, "acknowledged: before it");
        assert!(!r.acknowledged_covers(b));
        // Landed or not, the waiter waits for the acknowledgement.
        tokio::time::timeout(PATIENCE, r.landed()).await.unwrap();
        assert!(pending(&mut waiter).await, "woken at capture");
        assert_eq!(r.apply().await.unwrap(), Applied::Acknowledged(id));
        assert!(r.head() > last, "the head moved forward");
        last = r.head();
        assert_eq!(r.head(), id);
        assert_eq!(r.captured(), r.acknowledged_position());
        assert!(r.acknowledged_covers(b));
        waiter.await.unwrap();
        assert_eq!(bucket_head(&r).await, id);
        assert!(r.uploader_idle());
        // Applying again changes nothing.
        assert_eq!(r.apply().await.unwrap(), Applied::Nothing);
        assert_eq!(r.head(), id);
    }
    assert_eq!(restored(&r, dir.path()).await, r.acknowledged_position().1);

    // A second segment while one is in flight: refused, replication stops.
    write(&mut db, "in flight");
    start(&mut r, &mut db, &budget).await;
    write(&mut db, "second");
    let plan = db.next_bounded_capture().unwrap();
    let permit = budget.reserve(plan.boundary.estimated_bytes).await.unwrap();
    assert!(matches!(
        r.prepare(plan, permit).await,
        Err(ReplicaError::Refused(m)) if m.contains("in flight")
    ));
    assert!(r.failed().is_some());
    assert!(db.has_pending_boundaries(), "the second was not captured");
    drop(hold);
    drop(db);
}

/// The hand-off is one step that a cut never interrupts: with the guest's
/// wait for it cancelled (as a wall or a lease cut cancels it), the step
/// still captures, encodes and hands the segment to its task, and the next
/// segment chains on it (ADR 0034, section 2).
///
/// Model: capture and its hand-off are one scheduled step there; a capture
/// whose changes were dropped would break the chain (`Violation::MissingLink`).
/// The step being synchronous is what makes this hold; the test checks it
/// holds the way `with_db` runs it.
#[tokio::test]
async fn a_cut_never_interrupts_the_hand_off() {
    let dir = tempfile::tempdir().unwrap();
    let Actor {
        mut r,
        store: _store,
        db,
        hold,
    } = actor(dir.path(), 1 << 30).await;
    let budget = UploadBudget::node();
    let mut db = db;
    write(&mut db, "cut");
    let plan = db.next_bounded_capture().unwrap();
    let permit = budget.reserve(plan.boundary.estimated_bytes).await.unwrap();
    let ticket = r.prepare(plan, permit).await.unwrap();
    // As `ActorHost::with_db`: the step runs on the database's worker, and
    // the event's wait for it is what a cut cancels.
    let mut step = tokio::task::spawn_blocking(move || {
        std::thread::sleep(Duration::from_millis(50));
        let id = ticket.hand_off(&mut db);
        (db, id)
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(1), &mut step)
            .await
            .is_err(),
        "the wait was cut"
    );
    let (mut db, id) = step.await.unwrap();
    let id = id.unwrap();
    assert_eq!(land(&mut r).await.unwrap(), Applied::Acknowledged(id));
    write(&mut db, "next");
    let next = start(&mut r, &mut db, &budget).await;
    assert_eq!(land(&mut r).await.unwrap(), Applied::Acknowledged(next));
    assert_eq!(restored(&r, dir.path()).await, r.acknowledged_position().1);
    drop(hold);
    drop(db);
}

/// A PUT that lands after the hold was cut is `Late`: not acknowledged, the
/// head and the sidecar stay, replication stops, the failure flag is up and
/// every waiter fails.
///
/// Model: `Switch::AckLate` (`Violation::DurableLost`): an acknowledgement
/// is judged when the PUT returns, on the hold that sent it.
#[tokio::test]
async fn a_put_landing_after_a_cut_is_late() {
    let dir = tempfile::tempdir().unwrap();
    let Actor {
        mut r,
        store,
        mut db,
        hold,
    } = actor(dir.path(), 1 << 30).await;
    let (view, validity) = LeaseView::lapsable(1);
    r.lease = view;
    let budget = UploadBudget::node();
    let head = r.head();
    let sidecar = sidecar_of(&r);
    store.gate_next.store(1, SeqCst);
    let b = write(&mut db, "late");
    let waiter = r.wait_acknowledged(b);
    let mut flag = r.failure_flag();
    start(&mut r, &mut db, &budget).await;
    // The hold is cut while the PUT is in flight; then the PUT lands.
    validity.invalidate();
    store.open_gate();
    flag.wait_for(Option::is_some).await.unwrap();
    assert_eq!(
        *flag.borrow(),
        Some(crate::replica::batched::Stop {
            reason: UploadFailure::Late,
            batched: true
        })
    );
    assert!(waiter.await.is_err(), "a waiter fails with the upload");
    assert!(matches!(land(&mut r).await, Err(ReplicaError::Late)));
    assert_eq!(r.head(), head, "not acknowledged");
    assert_eq!(sidecar_of(&r), sidecar, "the sidecar stays");
    assert!(r.failed().is_some());
    assert_eq!(
        bucket_head(&r).await.txid,
        head.txid + 1,
        "the PUT landed, unacknowledged"
    );
    assert_eq!(budget.available(), budget.total());
    drop(hold);
    drop(db);
}

/// A failed upload raises the failure flag at once, before anyone applies
/// it: waiters fail and no capture starts again, so the commits behind the
/// segment stay uncaptured (dropped with the hold). Applied, it stops
/// replication for good.
///
/// Model: `Switch::CaptureAfterFailure` (`Violation::MissingLink`):
/// a failed hold captures nothing more and is dropped.
#[tokio::test]
async fn an_upload_failure_drops_what_is_pending() {
    let dir = tempfile::tempdir().unwrap();
    let Actor {
        mut r,
        store,
        mut db,
        hold,
    } = actor(dir.path(), 1 << 30).await;
    let budget = UploadBudget::node();
    let head = r.head();
    let sidecar = sidecar_of(&r);
    store.fail_next.store(u32::MAX, SeqCst);
    let a = write(&mut db, "a");
    let wa = r.wait_acknowledged(a);
    start(&mut r, &mut db, &budget).await;
    let b = write(&mut db, "b");
    let wb = r.wait_acknowledged(b);
    let mut flag = r.failure_flag();
    tokio::time::timeout(PATIENCE, flag.wait_for(Option::is_some))
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        *flag.borrow(),
        Some(crate::replica::batched::Stop {
            reason: UploadFailure::Bucket(_),
            batched: true
        })
    ));
    assert!(wa.await.is_err());
    assert!(wb.await.is_err());
    // Not applied yet, and still no capture.
    let plan = db.next_bounded_capture().unwrap();
    let permit = budget.reserve(plan.boundary.estimated_bytes).await.unwrap();
    assert!(matches!(
        r.prepare(plan, permit).await,
        Err(ReplicaError::Local(m)) if m.contains("replication stopped")
    ));
    assert!(db.has_pending_boundaries(), "nothing captured");
    assert!(matches!(land(&mut r).await, Err(ReplicaError::Bucket(_))));
    assert!(r.failed().is_some());
    assert!(!r.uploader_idle());
    let permit = budget.reserve(plan.boundary.estimated_bytes).await.unwrap();
    assert!(r.prepare(plan, permit).await.is_err());
    assert_eq!(r.head(), head);
    assert_eq!(sidecar_of(&r), sidecar);
    assert_eq!(budget.available(), budget.total(), "its bytes are freed");
    drop(hold);
    drop(db);
}

/// A completion that arrives after its hold was abandoned and the actor
/// acquired again does not apply to the new hold: the outcome is in the
/// old replica's cell, judged on the old hold (`Late`). The new hold's
/// head, sidecar and failure flag stay as they were; a waiter of the old
/// hold fails when the hold ends.
///
/// Model: `Switch::CompletionOtherHold` (`Violation::HeadBackwards`).
/// Here it holds by construction (each replica has its own cell); the test
/// pins that down, and that the old PUT is judged on the old hold.
#[tokio::test]
async fn a_completion_after_a_reacquire_does_not_apply() {
    let dir = tempfile::tempdir().unwrap();
    let Actor {
        r: mut old,
        store,
        mut db,
        hold,
    } = actor(dir.path(), 1 << 30).await;
    let (view, validity) = LeaseView::lapsable(1);
    old.lease = view;
    let budget = UploadBudget::node();
    let (head, sum) = old.acknowledged_position();
    store.gate_next.store(1, SeqCst);
    let b = write(&mut db, "old hold");
    let waiter = old.wait_acknowledged(b);
    let mut old_flag = old.failure_flag();
    start(&mut old, &mut db, &budget).await;
    // Abandoned: the old hold's lease view lapses, its replica dropped.
    validity.invalidate();
    drop(old);
    assert!(
        matches!(waiter.await, Err(ReplicaError::Local(m)) if m.contains("the hold ended")),
        "a waiter of the dropped hold fails"
    );
    // Acquired again, at generation 2, restored at the bucket's head.
    let mut new = super::replica(dir.path()).await;
    new.store = Arc::clone(&store) as Arc<dyn object_store::ObjectStore>;
    new.generation = 2;
    new.lease = LeaseView::always_valid(2);
    new.head = head;
    new.checksum = sum;
    new.db_path = db.path().to_owned();
    new.record(sum).await.unwrap();
    let sidecar = sidecar_of(&new);
    // The old PUT lands now.
    store.open_gate();
    tokio::time::timeout(PATIENCE, old_flag.wait_for(Option::is_some))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        *old_flag.borrow(),
        Some(crate::replica::batched::Stop {
            reason: UploadFailure::Late,
            batched: true
        })
    );
    assert_eq!(new.apply().await.unwrap(), Applied::Nothing);
    assert_eq!(new.acknowledged_position(), (head, sum));
    assert_eq!(sidecar_of(&new), sidecar);
    assert!(new.uploader_idle());
    assert!(new.failure_flag().borrow().is_none());
    assert!(!new.acknowledged_covers(b));
    drop(hold);
    drop(db);
}

/// The node budget is FIFO: a small reservation does not pass a larger one
/// that asked first, even with room for itself; a cancelled reservation
/// takes nothing. A segment charges its estimate, at most the cap.
///
/// Not in the model: its header leaves bytes and the node budget out.
#[tokio::test]
async fn the_node_budget_is_fifo_and_charges_at_most_the_cap() {
    let budget = Arc::new(UploadBudget::new(4, 4));
    let held = budget.reserve(3).await.unwrap();
    assert_eq!(budget.available(), 1);
    let first = tokio::spawn({
        let budget = Arc::clone(&budget);
        async move { budget.reserve(2).await.unwrap() }
    });
    tokio::time::sleep(Duration::from_millis(20)).await;
    // A reservation cancelled while it waits takes nothing, and leaves
    // its place.
    let mut cancelled = Box::pin(budget.reserve(1));
    assert!(pending(&mut cancelled).await);
    drop(cancelled);
    let mut second = Box::pin({
        let budget = Arc::clone(&budget);
        async move { budget.reserve(1).await.unwrap() }
    });
    assert!(
        pending(&mut second).await,
        "passed the reservation before it"
    );
    drop(held);
    let first = first.await.unwrap();
    assert_eq!(first.charged(), 2);
    let second = second.await;
    assert_eq!(budget.available(), 1);
    drop((first, second));
    assert_eq!(budget.available(), 4);

    let budget = UploadBudget::new(10 << 20, 1 << 20);
    assert_eq!(budget.charge(64 << 20), 1 << 20, "at most the cap");
    assert_eq!(budget.charge(10), 10);
    assert_eq!(budget.charge(0), 1);
    assert_eq!(UploadBudget::node().total(), 256 << 20);
}

/// An oversize event is one segment by itself, flagged so its actor waits
/// for it, and charges the budget the cap only. Its permit is held by the
/// upload task until the PUT ends, even after its hold gave up and its
/// replica is gone.
///
/// Not in the model (bytes and the budget are out of its scope); the wait
/// at an oversize event's end is S9's.
#[tokio::test]
async fn an_oversize_segment_charges_the_cap_until_its_put_ends() {
    let dir = tempfile::tempdir().unwrap();
    let Actor {
        mut r,
        store,
        mut db,
        hold,
    } = actor(dir.path(), 1 << 30).await;
    db.set_segment_byte_cap(1);
    let budget = UploadBudget::new(1 << 20, 1024);
    write(&mut db, "big");
    write(&mut db, "next");
    let plan = db.next_bounded_capture().unwrap();
    assert!(plan.oversize);
    assert_eq!(plan.boundaries, 1, "alone");
    assert!(plan.boundary.estimated_bytes > 1024);
    store.gate_next.store(1, SeqCst);
    let permit = budget.reserve(plan.boundary.estimated_bytes).await.unwrap();
    assert_eq!(permit.charged(), 1024, "the cap, not its size");
    r.prepare(plan, permit)
        .await
        .unwrap()
        .hand_off(&mut db)
        .unwrap();
    assert!(r.in_flight_oversize());
    assert_eq!(r.in_flight_events(), 1);
    assert_eq!(budget.available(), budget.total() - 1024);
    // The hold gives up; the PUT goes on, and its bytes stay held.
    drop(r);
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(budget.available(), budget.total() - 1024);
    store.open_gate();
    tokio::time::timeout(PATIENCE, async {
        while budget.available() != budget.total() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("freed when the PUT ended");
    drop(hold);
    drop(db);
}

/// The sidecar never names a position ahead of the acknowledged head: not
/// at capture, not while the PUT is out, not once it has landed and waits
/// to be applied; only after the apply.
///
/// Model: `Switch::SidecarBeforeAck` (`Violation::SidecarAhead`), here on
/// the real file.
#[tokio::test]
async fn the_sidecar_is_never_ahead_of_the_acknowledged_head() {
    let dir = tempfile::tempdir().unwrap();
    let Actor {
        mut r,
        store,
        mut db,
        hold,
    } = actor(dir.path(), 1 << 30).await;
    let budget = UploadBudget::node();
    assert_eq!(sidecar_of(&r), names_head(&r));
    let before = sidecar_of(&r);
    store.gate_next.store(1, SeqCst);
    write(&mut db, "row");
    let id = start(&mut r, &mut db, &budget).await;
    assert_eq!(sidecar_of(&r), before, "captured");
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(sidecar_of(&r), before, "PUT out");
    store.open_gate();
    tokio::time::timeout(PATIENCE, r.landed()).await.unwrap();
    assert_eq!(bucket_head(&r).await, id, "landed");
    assert_eq!(sidecar_of(&r), before, "landed, not applied");
    assert_eq!(r.apply().await.unwrap(), Applied::Acknowledged(id));
    assert_eq!(sidecar_of(&r), names_head(&r));
    assert_eq!(
        (sidecar_of(&r).0, sidecar_of(&r).1),
        (id.generation, id.txid)
    );
    drop(hold);
    drop(db);
}

/// A snapshot or a reset is refused while a segment is in flight (S1's
/// guards), and so is a strict upload beside it; once the segment is
/// applied, a snapshot at the acknowledged head is taken.
///
/// Model: `Switch::SnapshotInFlight` (`Violation::NotOwnerPrefix`),
/// `Switch::ResetInFlight` (`Violation::DeletedReturned`) and, for the
/// strict upload, `Switch::ModeWithoutBarrier` (`Violation::StrictAhead`).
#[tokio::test]
async fn snapshots_and_resets_wait_for_the_acknowledged_head() {
    for what in ["snapshot", "reset", "strict"] {
        let dir = tempfile::tempdir().unwrap();
        let Actor {
            mut r,
            store,
            mut db,
            hold,
        } = actor(dir.path(), 1 << 30).await;
        let budget = UploadBudget::node();
        store.gate_next.store(1, SeqCst);
        write(&mut db, "row");
        let id = start(&mut r, &mut db, &budget).await;
        assert!(!r.uploader_idle());
        // The database at the captured position, ahead of the bucket.
        let all = db.snapshot().unwrap();
        let refused = match what {
            "snapshot" => r.ship_snapshot(&all, SnapshotKind::Periodic).await.err(),
            "reset" => r.ship_reset(all).await.err(),
            _ => r.ship(&all).await.err(),
        };
        assert!(
            matches!(&refused, Some(ReplicaError::Refused(m)) if m.contains("in flight")),
            "{what}: {refused:?}"
        );
        assert!(r.failed().is_some(), "{what}");
        assert_eq!(r.head().txid, id.txid - 1, "{what}");
        store.open_gate();
        drop(hold);
        drop(db);
    }

    let dir = tempfile::tempdir().unwrap();
    let Actor {
        mut r,
        store: _store,
        mut db,
        hold,
    } = actor(dir.path(), 1 << 30).await;
    let budget = UploadBudget::node();
    write(&mut db, "row");
    start(&mut r, &mut db, &budget).await;
    land(&mut r).await.unwrap();
    assert!(r.uploader_idle());
    let all = db.snapshot().unwrap();
    assert!(
        r.ship_snapshot(&all, SnapshotKind::Periodic).await.unwrap(),
        "taken at the acknowledged head"
    );
    drop(hold);
    drop(db);
}

/// A crash at each point of a segment's life, captured with and without a
/// checkpoint: the local file is trusted again (`local_is_at`, on copies of
/// the files as they are at the crash) only when it is the acknowledged
/// head and the bucket's head; otherwise the next owner restores from the
/// bucket. ADR 0034, Verification ("a crash at each point").
///
/// Model: its local cache stands for `local_is_at` (`Violation::SidecarAhead`
/// and the restored prefix, `Violation::NotOwnerPrefix`); here the files.
#[tokio::test]
async fn a_crash_at_each_point_leaves_local_is_at_right() {
    for checkpointed in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let Actor {
            mut r,
            store,
            mut db,
            hold,
        } = actor(dir.path(), if checkpointed { 1 } else { 1 << 30 }).await;
        let budget = UploadBudget::node();
        let crash = |r: &Replica, point: &str| {
            let at = dir.path().join(format!("crash-{point}"));
            std::fs::create_dir_all(&at).unwrap();
            let copy = at.join("a.db");
            std::fs::copy(&r.db_path, &copy).unwrap();
            for suffix in ["-wal", ".replica"] {
                let from = format!("{}{suffix}", r.db_path.display());
                if Path::new(&from).exists() {
                    std::fs::copy(&from, format!("{}{suffix}", copy.display())).unwrap();
                }
            }
            copy
        };
        let judge = async |r: &Replica, point: &str, trusted: bool| {
            let copy = crash(r, point);
            let head = bucket_head(r).await;
            let found = wasmkeep_storage::local_is_at(&copy, head.generation, head.txid);
            assert_eq!(
                found.is_some(),
                trusted,
                "checkpointed {checkpointed}, {point}: {found:?}"
            );
            if let Some(sum) = found {
                assert_eq!((head, sum), r.acknowledged_position(), "{point}");
            }
        };
        judge(&r, "at the head", true).await;
        store.gate_next.store(1, SeqCst);
        write(&mut db, "row");
        judge(&r, "committed", false).await;
        let id = start(&mut r, &mut db, &budget).await;
        judge(&r, "captured, PUT sent", false).await;
        store.open_gate();
        tokio::time::timeout(PATIENCE, r.landed()).await.unwrap();
        assert_eq!(bucket_head(&r).await, id);
        judge(&r, "PUT landed", false).await;
        r.apply().await.unwrap();
        judge(&r, "sidecar written", checkpointed).await;
        db.finish_batched_capture().unwrap();
        judge(&r, "flushed", true).await;
        drop(hold);
        drop(db);
    }
}

/// A ticket dropped before its capture (the event cut before the database
/// worker took the step) consumes nothing: the segment leaves flight, the
/// guards no longer see it, and the next capture takes the commits.
///
/// Model: no switch; the WAL keeps what was not captured.
#[tokio::test]
async fn a_ticket_dropped_before_its_capture_takes_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let Actor {
        mut r,
        store: _store,
        mut db,
        hold,
    } = actor(dir.path(), 1 << 30).await;
    let budget = UploadBudget::node();
    let head = r.head();
    let b = write(&mut db, "row");
    let plan = db.next_bounded_capture().unwrap();
    let permit = budget.reserve(plan.boundary.estimated_bytes).await.unwrap();
    drop(r.prepare(plan, permit).await.unwrap());
    assert_eq!(budget.available(), budget.total());
    assert_eq!(r.apply().await.unwrap(), Applied::Abandoned);
    assert!(r.uploader_idle());
    assert_eq!(r.head(), head);
    assert!(db.has_pending_boundaries(), "nothing consumed");
    // Dropped again, then a prepare without an apply in between.
    let permit = budget.reserve(plan.boundary.estimated_bytes).await.unwrap();
    drop(r.prepare(plan, permit).await.unwrap());
    let id = start(&mut r, &mut db, &budget).await;
    assert_eq!(id.txid, head.txid + 1);
    assert_eq!(land(&mut r).await.unwrap(), Applied::Acknowledged(id));
    assert!(r.acknowledged_covers(b));
    assert_eq!(restored(&r, dir.path()).await, r.acknowledged_position().1);
    drop(hold);
    drop(db);
}

/// The hand-off's tripwires: a capture that does not chain from the
/// captured position (something captured outside the uploader) is refused
/// before any upload, and so is a plan no longer the WAL's next prefix;
/// either fails the replica and raises the failure flag.
///
/// Model: `Switch::TwoInFlight` and `Switch::ResetCaptured` break the same
/// chain there (`Violation::MissingLink`); here the checksum catches it.
#[tokio::test]
async fn a_capture_that_does_not_chain_is_refused() {
    for stale in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let Actor {
            mut r,
            store: _store,
            mut db,
            hold,
        } = actor(dir.path(), 1 << 30).await;
        let budget = UploadBudget::node();
        write(&mut db, "a");
        let early = db.next_bounded_capture().unwrap();
        // Captured outside the uploader: the WAL moved past the head.
        db.take_bounded_changes(early).unwrap();
        let plan = if stale {
            early
        } else {
            write(&mut db, "b");
            db.next_bounded_capture().unwrap()
        };
        let permit = budget.reserve(plan.boundary.estimated_bytes).await.unwrap();
        let flag = r.failure_flag();
        let handed = r.prepare(plan, permit).await.unwrap().hand_off(&mut db);
        let why = match handed {
            Err(ReplicaError::Local(m)) => m,
            other => panic!("stale {stale}: {other:?}"),
        };
        assert!(
            why.contains(if stale { "capture:" } else { "does not chain" }),
            "{why}"
        );
        assert!(flag.borrow().is_some(), "stale {stale}");
        assert!(r.apply().await.is_err());
        assert!(r.failed().is_some());
        assert_eq!(bucket_head(&r).await.txid, 1, "nothing uploaded");
        assert_eq!(budget.available(), budget.total());
        drop(hold);
        drop(db);
    }
}

/// A PUT that lands with its reply lost is sent again, meets its own
/// object and is acknowledged once, by the uploader as by `ship`. ADR 0034,
/// Verification: "hung then landed".
#[tokio::test]
async fn a_segment_that_landed_unanswered_is_acknowledged_once() {
    let dir = tempfile::tempdir().unwrap();
    let Actor {
        mut r,
        store,
        mut db,
        hold,
    } = actor(dir.path(), 1 << 30).await;
    let budget = UploadBudget::node();
    store.land_then_hang_next.store(1, SeqCst);
    let b = write(&mut db, "row");
    let id = start(&mut r, &mut db, &budget).await;
    assert_eq!(land(&mut r).await.unwrap(), Applied::Acknowledged(id));
    assert_eq!(store.puts.load(SeqCst), 2, "sent again once");
    assert_eq!(r.apply().await.unwrap(), Applied::Nothing, "applied once");
    assert!(r.acknowledged_covers(b));
    assert_eq!(sidecar_of(&r), names_head(&r));
    drop(hold);
    drop(db);
}

/// An apply cancelled while it writes the sidecar leaves the uploader not
/// idle and its waiters waiting; the next apply finishes the write (one at
/// a time) and wakes them. The sidecar is never ahead meanwhile.
///
/// Model: `Switch::SidecarBeforeAck` (`Violation::SidecarAhead`).
#[test]
fn an_apply_cut_while_writing_the_sidecar_is_finished_by_the_next() {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()
        .unwrap()
        .block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let Actor {
                mut r,
                store: _store,
                mut db,
                hold,
            } = actor(dir.path(), 1 << 30).await;
            let budget = UploadBudget::node();
            let before = sidecar_of(&r);
            let b = write(&mut db, "row");
            let waiter = r.wait_acknowledged(b);
            let id = start(&mut r, &mut db, &budget).await;
            tokio::time::timeout(PATIENCE, r.landed()).await.unwrap();
            cut_apply(&mut r).await;
            assert_eq!(r.head(), id);
            assert_eq!(r.captured_sequence(), Some(b));
            assert!(!r.uploader_idle());
            assert!(!r.acknowledged_covers(b));
            let side = sidecar_of(&r);
            assert!(side == before || side == names_head(&r));
            assert_eq!(r.apply().await.unwrap(), Applied::Nothing);
            assert!(r.uploader_idle());
            assert!(r.acknowledged_covers(b));
            assert_eq!(sidecar_of(&r), names_head(&r));
            waiter.await.unwrap();
            drop(hold);
            drop(db);
        });
}

/// A waiter fails when replication stops for any reason, not only a failed
/// upload: here a refused snapshot, with nothing in flight.
#[tokio::test]
async fn a_waiter_fails_when_a_guard_stops_replication() {
    let dir = tempfile::tempdir().unwrap();
    let Actor {
        mut r,
        store: _store,
        mut db,
        hold,
    } = actor(dir.path(), 1 << 30).await;
    let b = write(&mut db, "row");
    let waiter = r.wait_acknowledged(b);
    let flag = r.failure_flag();
    let moved = super::changes(DbChecksum::from_value(1));
    assert!(matches!(
        r.ship_snapshot(&moved, SnapshotKind::Periodic).await,
        Err(ReplicaError::Refused(_))
    ));
    assert!(
        tokio::time::timeout(PATIENCE, waiter)
            .await
            .unwrap()
            .is_err()
    );
    assert!(flag.borrow().as_ref().unwrap().batched);
    drop(hold);
    drop(db);
}

/// Storage stamps each restarted capture with a new session. Neither an
/// unseen multi-boundary restart nor two restarts can confuse old waiters.
///
/// Model: `Switch::FlushCaptured`'s rule, that a flush resolves only on
/// its own acknowledgement (`Violation::DurableLost`).
#[tokio::test]
async fn a_capture_restarted_counts_boundaries_again() {
    let dir = tempfile::tempdir().unwrap();
    let Actor {
        mut r,
        mut db,
        hold,
        ..
    } = actor(dir.path(), 1 << 30).await;
    let budget = UploadBudget::node();
    let a = write(&mut db, "a");
    let old = r.wait_acknowledged(a);
    let never = r.wait_acknowledged(WalBoundary {
        sequence: a.sequence + 1,
        ..a
    });
    start(&mut r, &mut db, &budget).await;
    land(&mut r).await.unwrap();
    let mut hold = hold;
    for n in 0..2 {
        db.finish_batched_capture().unwrap();
        drop(hold);
        db.start_capture().unwrap();
        hold = CaptureHold::default();
        db.enable_batched_capture(&hold).unwrap();
        let b = write(&mut db, "b");
        write(&mut db, "c");
        assert_ne!(a.session, b.session);
        assert!(!r.acknowledged_covers(b));
        let mut waiter = Box::pin(r.wait_acknowledged(b));
        assert!(pending(&mut waiter).await, "restart {n}");
        assert_eq!(db.next_bounded_capture().unwrap().boundaries, 2);
        start(&mut r, &mut db, &budget).await;
        land(&mut r).await.unwrap();
        waiter.await.unwrap();
    }
    old.await.unwrap();
    assert!(never.await.is_err());
    drop(hold);
    drop(db);
}

#[tokio::test]
async fn hand_off_after_stop_or_lapse_consumes_nothing() {
    for lapse in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let Actor {
            mut r,
            mut db,
            hold,
            store,
        } = actor(dir.path(), 1 << 30).await;
        let (view, validity) = LeaseView::lapsable(1);
        r.lease = view;
        write(&mut db, "row");
        let plan = db.next_bounded_capture().unwrap();
        let budget = UploadBudget::node();
        let ticket = r
            .prepare(
                plan,
                budget.reserve(plan.boundary.estimated_bytes).await.unwrap(),
            )
            .await
            .unwrap();
        let puts = store.puts.load(SeqCst);
        if lapse {
            validity.invalidate();
        } else {
            r.fail("stopped before capture");
        }
        assert!(ticket.hand_off(&mut db).is_err());
        assert_eq!(db.next_bounded_capture(), Some(plan));
        assert_eq!(r.apply().await.unwrap(), Applied::Abandoned);
        assert_eq!(store.puts.load(SeqCst), puts);
        assert_eq!(budget.available(), budget.total());
        drop(hold);
        drop(db);
    }
}

#[tokio::test]
async fn permit_is_freed_when_landed_without_apply() {
    let dir = tempfile::tempdir().unwrap();
    let Actor {
        mut r,
        mut db,
        hold,
        ..
    } = actor(dir.path(), 1 << 30).await;
    let budget = UploadBudget::node();
    write(&mut db, "row");
    start(&mut r, &mut db, &budget).await;
    r.landed().await;
    assert_eq!(budget.available(), budget.total());
    assert!(!r.uploader_idle());
    drop(hold);
    drop(db);
}

#[tokio::test]
async fn prepare_checks_snapshot_and_permit_size() {
    let dir = tempfile::tempdir().unwrap();
    let Actor {
        mut r,
        mut db,
        hold,
        ..
    } = actor(dir.path(), 1 << 30).await;
    let budget = UploadBudget::node();
    write(&mut db, "row");
    let plan = db.next_bounded_capture().unwrap();
    r.needs_snapshot = true;
    assert!(
        r.prepare(
            plan,
            budget.reserve(plan.boundary.estimated_bytes).await.unwrap()
        )
        .await
        .is_err()
    );
    r.needs_snapshot = false;
    assert!(
        r.prepare(plan, budget.reserve(1).await.unwrap())
            .await
            .is_err()
    );
    assert_eq!(db.next_bounded_capture(), Some(plan));
    assert!(r.uploader_idle());
    drop(hold);
    drop(db);
}

#[tokio::test]
async fn apply_after_stop_does_not_acknowledge() {
    let dir = tempfile::tempdir().unwrap();
    let Actor {
        mut r,
        mut db,
        hold,
        ..
    } = actor(dir.path(), 1 << 30).await;
    let budget = UploadBudget::node();
    let b = write(&mut db, "row");
    let before = sidecar_of(&r);
    start(&mut r, &mut db, &budget).await;
    r.landed().await;
    r.fail("stopped before apply");
    assert!(r.apply().await.is_err());
    assert_eq!(sidecar_of(&r), before);
    assert!(!r.acknowledged_covers(b));
    assert!(r.wait_acknowledged(b).await.is_err());
    drop(hold);
    drop(db);
}

#[test]
fn every_guard_finishes_a_cut_sidecar() {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()
        .unwrap()
        .block_on(async {
            for guard in ["prepare", "snapshot", "reset", "strict", "restart"] {
                let dir = tempfile::tempdir().unwrap();
                let Actor {
                    mut r,
                    mut db,
                    hold,
                    ..
                } = actor(dir.path(), 1 << 30).await;
                let budget = UploadBudget::node();
                let b = write(&mut db, "row");
                start(&mut r, &mut db, &budget).await;
                r.landed().await;
                cut_apply(&mut r).await;
                assert_eq!(r.captured_sequence(), Some(b));
                let all = db.snapshot().unwrap();
                match guard {
                    "prepare" => {
                        write(&mut db, "next");
                        let plan = db.next_bounded_capture().unwrap();
                        drop(
                            r.prepare(
                                plan,
                                budget.reserve(plan.boundary.estimated_bytes).await.unwrap(),
                            )
                            .await
                            .unwrap(),
                        );
                    }
                    "snapshot" => {
                        r.ship_snapshot(&all, SnapshotKind::Periodic).await.unwrap();
                    }
                    "reset" => {
                        r.ship_reset(all).await.unwrap();
                    }
                    "strict" => {
                        let changes = wasmkeep_storage::wal::Changes {
                            pre_apply: r.checksum,
                            ..all
                        };
                        r.ship(&changes).await.unwrap();
                    }
                    _ => {
                        r.capture_restarted().await.unwrap();
                    }
                }
                assert!(r.acknowledged_covers(b), "{guard}");
                r.settle_abandoned();
                assert!(r.uploader_idle());
                assert_eq!(sidecar_of(&r), names_head(&r));
                assert!(r.failed().is_none());
                drop(hold);
                drop(db);
            }
        });
}

#[tokio::test]
async fn panic_inside_capture_fails_instead_of_abandoning() {
    let dir = tempfile::tempdir().unwrap();
    let Actor {
        mut r,
        mut db,
        hold,
        ..
    } = actor(dir.path(), 1 << 30).await;
    let budget = UploadBudget::node();
    write(&mut db, "row");
    let plan = db.next_bounded_capture().unwrap();
    let ticket = r
        .prepare(
            plan,
            budget.reserve(plan.boundary.estimated_bytes).await.unwrap(),
        )
        .await
        .unwrap();
    let flag = r.failure_flag();
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        ticket.hand_off_with(|| {
            db.take_bounded_changes(plan).unwrap();
            panic!("capture consumed WAL, then panicked");
        })
    }));
    assert!(panic.is_err());
    assert!(flag.borrow().is_some());
    assert!(r.apply().await.is_err());
    assert_eq!(budget.available(), budget.total());
    drop(hold);
    drop(db);
}

#[tokio::test]
async fn fork_tripwire_refuses_a_capture_ahead_of_acknowledged_head() {
    let dir = tempfile::tempdir().unwrap();
    let Actor {
        mut r,
        mut db,
        hold,
        ..
    } = actor(dir.path(), 1 << 30).await;
    let budget = UploadBudget::node();
    write(&mut db, "row");
    let plan = db.next_bounded_capture().unwrap();
    let mut ticket = r
        .prepare(
            plan,
            budget.reserve(plan.boundary.estimated_bytes).await.unwrap(),
        )
        .await
        .unwrap();
    // A second capture started against the captured position before that
    // position was acknowledged: the chain matches, but the fork guard must fail.
    ticket.acknowledged = DbChecksum::from_value(r.checksum.value() ^ 1);
    assert!(
        matches!(ticket.hand_off(&mut db), Err(ReplicaError::Local(m)) if m.contains("acknowledged head"))
    );
    assert!(r.failure_flag().borrow().is_some());
    assert_eq!(bucket_head(&r).await, r.head());
    drop(hold);
    drop(db);
}

#[tokio::test]
async fn an_acknowledged_plan_cannot_be_prepared_again() {
    let dir = tempfile::tempdir().unwrap();
    let Actor {
        mut r,
        mut db,
        hold,
        ..
    } = actor(dir.path(), 1 << 30).await;
    let budget = UploadBudget::node();
    write(&mut db, "row");
    let plan = db.next_bounded_capture().unwrap();
    start(&mut r, &mut db, &budget).await;
    land(&mut r).await.unwrap();
    assert!(
        matches!(r.prepare(plan, budget.reserve(plan.boundary.estimated_bytes).await.unwrap()).await,
        Err(ReplicaError::Refused(m)) if m.contains("already acknowledged"))
    );
    drop(hold);
    drop(db);
}

/// Queue the sidecar behind a blocked worker so a single poll always cuts
/// apply at the join, even when the filesystem completes writes immediately.
async fn cut_apply(r: &mut Replica) {
    let (release, wait) = std::sync::mpsc::channel();
    let (started, ready) = tokio::sync::oneshot::channel();
    let blocker = tokio::task::spawn_blocking(move || {
        started.send(()).unwrap();
        wait.recv().unwrap();
    });
    ready.await.unwrap();
    let mut apply = Box::pin(r.apply());
    let pending =
        std::future::poll_fn(|cx| std::task::Poll::Ready(apply.as_mut().poll(cx).is_pending()))
            .await;
    drop(apply);
    release.send(()).unwrap();
    blocker.await.unwrap();
    assert!(pending, "apply was cut at the sidecar write");
}

/// A ticket dropped before its capture leaves its boundaries uncaptured,
/// and the replica says so: a flush after it targets them, not the last
/// acknowledged boundary, and their age still counts for the event-start
/// rule (ADR 0034, sections 3 and 4).
#[tokio::test]
async fn an_abandoned_ticket_leaves_its_boundaries_marked() {
    let dir = tempfile::tempdir().unwrap();
    let Actor {
        mut r,
        store: _store,
        mut db,
        hold,
    } = actor(dir.path(), 1 << 30).await;
    let budget = UploadBudget::node();
    let first = write(&mut db, "one");
    r.note_boundary(first, std::time::Instant::now());
    start(&mut r, &mut db, &budget).await;
    land(&mut r).await.unwrap();
    assert!(r.acknowledged_covers(first));
    let second = write(&mut db, "two");
    r.note_boundary(second, std::time::Instant::now());
    let plan = db.next_bounded_capture().unwrap();
    let permit = budget.reserve(plan.boundary.estimated_bytes).await.unwrap();
    drop(r.prepare(plan, permit).await.unwrap());
    assert_eq!(r.last_marked(), Some(second), "still to flush");
    assert!(r.oldest_unsent().is_some(), "still unsent");
    assert_eq!(r.unacknowledged_events(), 1);
    assert_eq!(r.apply().await.unwrap(), Applied::Abandoned);
    assert_eq!(r.last_marked(), Some(second));
    let id = start(&mut r, &mut db, &budget).await;
    assert_eq!(r.last_marked(), Some(second), "in flight, not acknowledged");
    assert!(r.oldest_unsent().is_none(), "captured");
    assert_eq!(land(&mut r).await.unwrap(), Applied::Acknowledged(id));
    assert!(r.acknowledged_covers(second));
    assert_eq!(r.unacknowledged_events(), 0);
    drop(hold);
    drop(db);
}

/// An alarm index that records its requests, its PUTs held at a gate
/// while `gated` and failed while `refuse`.
struct Index {
    puts: std::sync::Mutex<Vec<Entry>>,
    deletes: std::sync::Mutex<Vec<Entry>>,
    gated: std::sync::atomic::AtomicBool,
    gate: tokio::sync::Semaphore,
    refuse: std::sync::atomic::AtomicBool,
}

impl Default for Index {
    fn default() -> Self {
        Self {
            puts: std::sync::Mutex::default(),
            deletes: std::sync::Mutex::default(),
            gated: std::sync::atomic::AtomicBool::default(),
            gate: tokio::sync::Semaphore::new(0),
            refuse: std::sync::atomic::AtomicBool::default(),
        }
    }
}

impl Index {
    fn handle(self: &Arc<Self>) -> crate::replica::AlarmIndex {
        let (put, delete) = (Arc::clone(self), Arc::clone(self));
        crate::replica::AlarmIndex::new(
            move |entry| {
                let index = Arc::clone(&put);
                async move {
                    if index.gated.load(SeqCst) {
                        index.gate.acquire().await.unwrap().forget();
                    }
                    if index.refuse.load(SeqCst) {
                        return Err("refused".to_owned());
                    }
                    index.puts.lock().unwrap().push(entry);
                    Ok(())
                }
            },
            move |entry| {
                let index = Arc::clone(&delete);
                async move {
                    index.deletes.lock().unwrap().push(entry);
                    Ok(())
                }
            },
        )
    }
    fn puts(&self) -> Vec<Entry> {
        self.puts.lock().unwrap().clone()
    }
    fn deletes(&self) -> Vec<Entry> {
        self.deletes.lock().unwrap().clone()
    }
}

fn entry(at: u64, generation: u64, nonce: &str) -> Entry {
    Entry::owned(at, generation, nonce).unwrap()
}

/// One event naming `named`: a committed row, its boundary, recorded.
fn write_naming(r: &mut Replica, db: &mut ActorDb, v: &str, named: Option<&Entry>) -> WalBoundary {
    let b = write(db, v);
    r.note_named_boundary(b, std::time::Instant::now(), named.cloned());
    b
}

/// The entry a segment's end names goes up before the segment, and the
/// one the acknowledged head named is retired only once the segment is
/// acknowledged; the acknowledged entry then follows (ADR 0034, section 9).
///
/// Model: `Switch::SegmentBeforeEntry`, `Switch::RetireBeforeAck`
/// (`Violation::AlarmMissing`).
#[tokio::test]
async fn a_segments_entry_goes_up_first_and_the_old_one_retires_after_acknowledgement() {
    let dir = tempfile::tempdir().unwrap();
    let Actor {
        mut r,
        store: _store,
        mut db,
        hold,
    } = actor(dir.path(), 1 << 30).await;
    let budget = UploadBudget::node();
    let generation = r.acknowledged_position().0.generation;
    let (old, new) = (entry(1000, generation, "a"), entry(2000, generation, "b"));
    let index = Arc::new(Index::default());
    index.gated.store(true, SeqCst);
    r.set_alarm_index(index.handle(), Some(old.clone()));
    let head = r.head();
    write_naming(&mut r, &mut db, "one", Some(&new));
    start(&mut r, &mut db, &budget).await;
    let mut landed = Box::pin(r.landed());
    assert!(
        pending(&mut landed).await,
        "the segment waits for its entry"
    );
    assert_eq!(
        bucket_head(&r).await,
        head,
        "nothing uploaded before the entry"
    );
    assert!(index.deletes().is_empty());
    index.gate.add_permits(1);
    landed.await;
    let id = match r.apply().await.unwrap() {
        Applied::Acknowledged(id) => id,
        other => panic!("{other:?}"),
    };
    assert_eq!(bucket_head(&r).await, id);
    assert_eq!(index.puts(), vec![new.clone()]);
    let started = std::time::Instant::now();
    while index.deletes().is_empty() {
        assert!(started.elapsed() < PATIENCE, "never retired");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(index.deletes(), vec![old]);
    assert_eq!(r.acknowledged_entry(), Some(Some(new.clone())));
    // A segment naming the acknowledged entry again sends no request.
    write_naming(&mut r, &mut db, "two", Some(&new));
    start(&mut r, &mut db, &budget).await;
    land(&mut r).await.unwrap();
    assert_eq!((index.puts().len(), index.deletes().len()), (1, 1));
    drop(hold);
    drop(db);
}

/// Only the entry a segment's end names is listed, never one named only
/// inside it; a segment that fails retires nothing (ADR 0034, section 9).
///
/// Model: `Switch::IntermediateEntry`, `Switch::RetireOnFailure`
/// (`Violation::AlarmMissing`).
#[tokio::test]
async fn only_the_end_entry_goes_up_and_a_failed_segment_retires_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let Actor {
        mut r,
        store,
        mut db,
        hold,
    } = actor(dir.path(), 1 << 30).await;
    let budget = UploadBudget::node();
    let generation = r.acknowledged_position().0.generation;
    let old = entry(1000, generation, "a");
    let (inside, end) = (entry(2000, generation, "b"), entry(3000, generation, "c"));
    let index = Arc::new(Index::default());
    r.set_alarm_index(index.handle(), Some(old));
    write_naming(&mut r, &mut db, "one", Some(&inside));
    write_naming(&mut r, &mut db, "two", Some(&end));
    store.fail_next.store(1_000, SeqCst);
    start(&mut r, &mut db, &budget).await;
    assert!(land(&mut r).await.is_err());
    assert_eq!(index.puts(), vec![end]);
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        index.deletes().is_empty(),
        "a failed segment retired its entry"
    );
    drop(hold);
    drop(db);
}

/// An entry PUT that fails fails the segment before its upload: the
/// replica stops, and no state naming the entry reaches the bucket (ADR
/// 0034, sections 8 and 9).
#[tokio::test]
async fn a_failed_entry_put_fails_the_segment_before_its_upload() {
    let dir = tempfile::tempdir().unwrap();
    let Actor {
        mut r,
        store: _store,
        mut db,
        hold,
    } = actor(dir.path(), 1 << 30).await;
    let budget = UploadBudget::node();
    let generation = r.acknowledged_position().0.generation;
    let index = Arc::new(Index::default());
    index.refuse.store(true, SeqCst);
    r.set_alarm_index(index.handle(), None);
    let head = r.head();
    write_naming(&mut r, &mut db, "one", Some(&entry(2000, generation, "b")));
    start(&mut r, &mut db, &budget).await;
    assert!(land(&mut r).await.is_err());
    assert!(r.failed().is_some());
    assert_eq!(bucket_head(&r).await, head, "the segment never went up");
    drop(hold);
    drop(db);
}

/// A periodic snapshot of a batched hold is due by segments, as a strict
/// one's, or by the acknowledged segments' bytes, whichever comes first,
/// so that a restore stays bounded when segments are full (ADR 0034,
/// section 7). Here the bytes bound is small: one large segment makes it
/// due; a snapshot taken starts both counts again.
#[tokio::test]
async fn a_batched_snapshot_is_due_by_acknowledged_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let Actor {
        mut r,
        store: _store,
        mut db,
        hold,
    } = actor(dir.path(), 1 << 30).await;
    let budget = UploadBudget::node();
    r.set_snapshot_bytes(16 << 10);
    db.exec("INSERT INTO t VALUES ('small')", &[]).unwrap();
    db.mark_boundary().unwrap().unwrap();
    start(&mut r, &mut db, &budget).await;
    land(&mut r).await.unwrap();
    assert!(!r.batched_snapshot_due(), "one small segment");
    db.exec("INSERT INTO t VALUES (randomblob(40000))", &[])
        .unwrap();
    db.mark_boundary().unwrap().unwrap();
    start(&mut r, &mut db, &budget).await;
    land(&mut r).await.unwrap();
    assert!(r.batched_snapshot_due(), "past the bytes bound");
    let all = db.snapshot().unwrap();
    assert!(r.ship_snapshot(&all, SnapshotKind::Periodic).await.unwrap());
    assert!(!r.batched_snapshot_due(), "counted again from the snapshot");
    drop(hold);
    drop(db);
}
