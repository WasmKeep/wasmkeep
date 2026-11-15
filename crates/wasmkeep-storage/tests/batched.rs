//! ADR 0034 S7: real SQLite WALs, no server/uploader implementation assumed.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use rusqlite::Connection;
use std::path::Path;
use wasmkeep_storage::wal::{
    CaptureError, Changes, DEFAULT_SEGMENT_BYTES, Restorer, WalCapture, estimated_segment_bytes,
};
use wasmkeep_storage::{ActorDb, CaptureHold, SqlValue, StateDir, Storage, StorageConfig};

fn raw(path: &Path) -> (Connection, WalCapture) {
    let conn = Connection::open(path).unwrap();
    conn.execute_batch("PRAGMA page_size=4096; PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0; CREATE TABLE t(id INTEGER PRIMARY KEY, v BLOB); PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    let capture = WalCapture::start(path, 4096).unwrap();
    (conn, capture)
}

fn actor(dir: &Path, checkpoint_threshold: u64) -> (ActorDb, CaptureHold) {
    let mut config = StorageConfig::new(dir);
    config.max_wal_bytes = checkpoint_threshold;
    let storage = Storage::new(
        config,
        StateDir {
            dir: "actors".into(),
            scratch: "scratch".into(),
            tenant: "t".into(),
        },
    );
    let mut db = storage.open("c", "a").unwrap();
    db.exec("CREATE TABLE t(v BLOB)", &[]).unwrap();
    db.start_capture().unwrap();
    let hold = CaptureHold::default();
    db.enable_batched_capture(&hold).unwrap();
    (db, hold)
}

fn apply(r: &mut Restorer, c: &Changes, txid: &mut u64) {
    *txid += 1;
    r.apply(&c.to_ltx(*txid, *txid, 0).unwrap()).unwrap();
}

// A reproducible generator, not SQLite randomblob: failures name their seed.
fn next(seed: &mut u64) -> u64 {
    *seed ^= *seed << 13;
    *seed ^= *seed >> 7;
    *seed ^= *seed << 17;
    *seed
}

#[test]
fn property_boundary_partitions_equal_whole_wal_and_each_event_including_vacuum() {
    for initial_seed in 1..=48 {
        let mut seed = initial_seed;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("source.db");
        let (conn, mut pieces) = raw(&path);
        conn.execute_batch("PRAGMA cache_size=5").unwrap();
        let mut whole = WalCapture::start(&path, 4096).unwrap();
        let mut each = WalCapture::start(&path, 4096).unwrap();
        let snapshot = pieces.snapshot().unwrap().to_ltx(1, 1, 0).unwrap();
        let piece_path = dir.path().join("pieces.db");
        let each_path = dir.path().join("each.db");
        let mut piece_restore = Restorer::create(&piece_path, 1 << 30).unwrap();
        let mut each_restore = Restorer::create(&each_path, 1 << 30).unwrap();
        piece_restore.apply(&snapshot).unwrap();
        each_restore.apply(&snapshot).unwrap();
        let (mut piece_txid, mut each_txid) = (1, 1);
        let mut shrank = false;
        let mut previous_pages = pieces.pages();
        let mut prefixes = std::collections::BTreeMap::new();
        for event in 0..24 {
            // Every seed grows then shrinks; VACUUM is a real WAL commit.
            if event % 6 == 5 {
                conn.execute_batch("DELETE FROM t; VACUUM").unwrap();
            } else {
                // Multiple commits inside one event, plus rewrites of pages.
                for _ in 0..1 + next(&mut seed) % 4 {
                    conn.execute_batch("BEGIN").unwrap();
                    let id = (next(&mut seed) % 12) as i64;
                    let len = 100 + (next(&mut seed) % 24000) as usize;
                    let data: Vec<u8> = (0..len).map(|_| next(&mut seed) as u8).collect();
                    conn.execute(
                        "INSERT OR REPLACE INTO t VALUES (?1, ?2)",
                        rusqlite::params![id, data],
                    )
                    .unwrap();
                    // Several statements rewrite more pages than fit in cache.
                    for _ in 0..2 {
                        let data: Vec<u8> = (0..24000).map(|_| next(&mut seed) as u8).collect();
                        conn.execute("UPDATE t SET v=?1", [data]).unwrap();
                    }
                    conn.execute_batch(
                        "SAVEPOINT s; UPDATE t SET v=zeroblob(40000); ROLLBACK TO s; RELEASE s",
                    )
                    .unwrap();
                    conn.execute_batch(if next(&mut seed).is_multiple_of(3) {
                        "ROLLBACK"
                    } else {
                        "COMMIT"
                    })
                    .unwrap();
                }
                // Guarantee a committed event even if all generated txns rolled back.
                conn.execute("INSERT OR REPLACE INTO t VALUES (99, 'committed')", [])
                    .unwrap();
                conn.execute_batch("BEGIN; UPDATE t SET v=zeroblob(40000); ROLLBACK")
                    .unwrap();
            }
            let before = (pieces.wal_bytes(), pieces.checksum());
            let boundary = pieces.mark_boundary().unwrap().unwrap();
            assert_eq!((pieces.wal_bytes(), pieces.checksum()), before);
            assert_eq!(
                pieces.mark_boundary().unwrap(),
                Some(boundary),
                "read-only mark"
            );
            let c = each.capture().unwrap().unwrap();
            shrank |= c.commit < previous_pages;
            previous_pages = c.commit;
            apply(&mut each_restore, &c, &mut each_txid);
            prefixes.insert(
                boundary.offset,
                (c.post_apply, std::fs::read(&each_path).unwrap()),
            );
            // Arbitrary capture timing and caps, including all pending events.
            if next(&mut seed).is_multiple_of(3) || event == 23 {
                while pieces.has_pending_boundaries() {
                    let cap = match next(&mut seed) % 4 {
                        0 => 0,
                        1 => estimated_segment_bytes(4096, 4),
                        2 => estimated_segment_bytes(4096, 12),
                        _ => DEFAULT_SEGMENT_BYTES,
                    };
                    let known: Vec<_> = pieces.boundaries().collect();
                    let c = pieces.capture_bounded(cap).unwrap().unwrap();
                    assert_eq!(c.boundary, known[c.boundaries - 1]);
                    assert_eq!(c.changes.pages.len(), c.boundary.distinct_pages);
                    let bytes = c.changes.to_ltx(piece_txid + 1, piece_txid + 1, 0).unwrap();
                    assert!(bytes.len() as u64 <= c.boundary.estimated_bytes);
                    assert!(c.boundary.estimated_bytes <= cap || c.oversize && c.boundaries == 1);
                    apply(&mut piece_restore, &c.changes, &mut piece_txid);
                    let (expected_sum, expected_bytes) = &prefixes[&c.boundary.offset];
                    assert_eq!(
                        c.changes.post_apply, *expected_sum,
                        "prefix, seed {initial_seed}"
                    );
                    assert_eq!(
                        &std::fs::read(&piece_path).unwrap(),
                        expected_bytes,
                        "prefix, seed {initial_seed}"
                    );
                }
            }
        }
        assert!(shrank, "seed {initial_seed} must exercise truncation");
        let whole_path = dir.path().join("whole.db");
        let mut whole_restore = Restorer::create(&whole_path, 1 << 30).unwrap();
        whole_restore.apply(&snapshot).unwrap();
        apply(
            &mut whole_restore,
            &whole.capture().unwrap().unwrap(),
            &mut 1,
        );
        let sum = whole_restore.finish().unwrap();
        assert_eq!(piece_restore.finish().unwrap(), sum, "seed {initial_seed}");
        assert_eq!(each_restore.finish().unwrap(), sum, "seed {initial_seed}");
        let expected = std::fs::read(&whole_path).unwrap();
        assert_eq!(
            std::fs::read(&piece_path).unwrap(),
            expected,
            "seed {initial_seed}"
        );
        assert_eq!(
            std::fs::read(&each_path).unwrap(),
            expected,
            "seed {initial_seed}"
        );
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
            .unwrap();
        assert_eq!(
            std::fs::read(path).unwrap(),
            expected,
            "seed {initial_seed}"
        );
    }
}

#[test]
fn rewritten_pages_count_once_and_counts_rebase_after_partial_capture() {
    let dir = tempfile::tempdir().unwrap();
    let (conn, mut c) = raw(&dir.path().join("a.db"));
    conn.execute("INSERT INTO t VALUES (1, 'a')", []).unwrap();
    let first = c.mark_boundary().unwrap().unwrap();
    for i in 0..100 {
        conn.execute("UPDATE t SET v=?1", [i]).unwrap();
        let b = c.mark_boundary().unwrap().unwrap();
        assert_eq!(b.distinct_pages, first.distinct_pages);
        assert_eq!(b.estimated_bytes, first.estimated_bytes);
    }
    assert!(c.wal_len() > first.estimated_bytes * 50);
    assert_eq!(c.wal_bytes(), 0);
    let first_capture = c.capture_bounded(0).unwrap().unwrap();
    assert_eq!(first_capture.boundaries, 1);
    assert_eq!(c.boundaries().last().unwrap().distinct_pages, 1);
    let rest = c.capture_bounded(first.estimated_bytes).unwrap().unwrap();
    assert_eq!(rest.boundaries, 100);
    assert_eq!(rest.changes.pages.len(), 1);
}

#[test]
fn cap_ends_at_commit_boundaries_and_oversize_events_are_alone() {
    let dir = tempfile::tempdir().unwrap();
    let (conn, mut c) = raw(&dir.path().join("a.db"));
    conn.execute("INSERT INTO t VALUES (1, 'small')", [])
        .unwrap();
    let first = c.mark_boundary().unwrap().unwrap();
    // An event contains two commits; neither can split into another segment.
    conn.execute("INSERT INTO t VALUES (2, randomblob(50000))", [])
        .unwrap();
    conn.execute("INSERT INTO t VALUES (3, randomblob(50000))", [])
        .unwrap();
    let large = c.mark_boundary().unwrap().unwrap();
    conn.execute("INSERT INTO t VALUES (4, 'tail')", [])
        .unwrap();
    let tail = c.mark_boundary().unwrap().unwrap();
    let cap = estimated_segment_bytes(4096, 3);
    let a = c.capture_bounded(cap).unwrap().unwrap();
    assert_eq!(a.boundary, first);
    assert!(!a.oversize);
    let b = c.capture_bounded(cap).unwrap().unwrap();
    assert_eq!(b.boundary.offset, large.offset);
    assert!(b.oversize);
    assert_eq!(b.boundaries, 1);
    let d = c.capture_bounded(cap).unwrap().unwrap();
    assert_eq!(d.boundary.offset, tail.offset);
    assert!(!d.oversize);
    for boundary in [first, large, tail] {
        let bytes = std::fs::read(c.wal_path()).unwrap();
        let frame = boundary.offset as usize - (4096 + 24);
        assert_eq!(
            u32::from_be_bytes(bytes[frame + 4..frame + 8].try_into().unwrap()),
            boundary.commit
        );
    }
}

#[test]
fn unmarked_commits_and_rolled_back_spills_are_not_captured() {
    let dir = tempfile::tempdir().unwrap();
    let (conn, mut c) = raw(&dir.path().join("a.db"));
    conn.execute("INSERT INTO t VALUES (1, 'marked')", [])
        .unwrap();
    let marked = c.mark_boundary().unwrap().unwrap();
    conn.execute("INSERT INTO t VALUES (2, 'unmarked')", [])
        .unwrap();
    let got = c.capture_bounded(u64::MAX).unwrap().unwrap();
    assert_eq!(got.boundary, marked);
    assert!(c.has_uncaptured_commits().unwrap());
    assert!(c.capture_bounded(u64::MAX).unwrap().is_none());
    c.mark_boundary().unwrap();
    c.capture_bounded(u64::MAX).unwrap();
    let position = c.wal_bytes();
    conn.execute_batch("PRAGMA cache_size=5; BEGIN; INSERT INTO t VALUES (3, zeroblob(1000000))")
        .unwrap();
    assert!(c.wal_len() > position);
    assert!(c.mark_boundary().unwrap().is_none());
    conn.execute_batch("ROLLBACK").unwrap();
    assert!(!c.has_uncaptured_commits().unwrap());
    conn.execute("INSERT INTO t VALUES (4, 'after rollback')", [])
        .unwrap();
    assert!(c.mark_boundary().unwrap().is_some());
    assert!(c.capture_bounded(u64::MAX).unwrap().is_some());
    assert!(!c.has_uncaptured_commits().unwrap());
}

#[test]
fn checkpoint_paths_refuse_pending_and_unmarked_commits() {
    let dir = tempfile::tempdir().unwrap();
    let (mut db, hold) = actor(dir.path(), 100_000);
    db.exec("INSERT INTO t VALUES (zeroblob(1000))", &[])
        .unwrap();
    db.mark_boundary().unwrap();
    db.exec("INSERT INTO t VALUES (zeroblob(200000))", &[])
        .unwrap();
    // Second event deliberately unmarked, above the checkpoint threshold.
    let wal_path = format!("{}-wal", db.path().display());
    let wal_before = std::fs::read(&wal_path).unwrap();
    let main_before = std::fs::read(db.path()).unwrap();
    assert!(matches!(
        db.take_changes(),
        Err(CaptureError::BoundariesPending)
    ));
    assert!(matches!(
        db.snapshot(),
        Err(CaptureError::BoundariesPending)
    ));
    assert!(db.start_capture().is_err());
    db.take_bounded_changes(db.next_bounded_capture().unwrap())
        .unwrap();
    assert!(!db.has_pending_boundaries());
    assert!(matches!(
        db.snapshot(),
        Err(CaptureError::BoundariesPending)
    ));
    assert_eq!(std::fs::read(&wal_path).unwrap(), wal_before);
    assert_eq!(std::fs::read(db.path()).unwrap(), main_before);
    db.mark_boundary().unwrap();
    db.take_bounded_changes(db.next_bounded_capture().unwrap())
        .unwrap();
    assert_eq!(std::fs::metadata(&wal_path).unwrap().len(), 0);
    assert_eq!(
        db.snapshot().unwrap().post_apply,
        db.captured_checksum().unwrap()
    );
    assert!(
        db.start_capture().is_err(),
        "live hold may have a segment in flight"
    );
    drop(hold); // Flushed barrier: the server would first await acknowledgement.
    db.start_capture().unwrap();
}

#[test]
fn partial_capture_does_not_checkpoint_pending_boundaries() {
    let dir = tempfile::tempdir().unwrap();
    let (mut db, hold) = actor(dir.path(), 100_000);
    db.exec("INSERT INTO t VALUES (zeroblob(1000))", &[])
        .unwrap();
    db.mark_boundary().unwrap();
    db.exec("INSERT INTO t VALUES (zeroblob(200000))", &[])
        .unwrap();
    db.mark_boundary().unwrap();
    db.set_segment_byte_cap(0);
    let main = std::fs::read(db.path()).unwrap();
    db.take_bounded_changes(db.next_bounded_capture().unwrap())
        .unwrap();
    assert!(db.has_pending_boundaries());
    assert_eq!(std::fs::read(db.path()).unwrap(), main);
    assert!(db.snapshot().is_err());
    db.take_bounded_changes(db.next_bounded_capture().unwrap())
        .unwrap();
    assert!(!db.has_pending_boundaries());
    assert_ne!(std::fs::read(db.path()).unwrap(), main);
    drop(hold);
}

#[test]
fn closing_after_give_up_preserves_uncaptured_wal_without_checkpoint() {
    let dir = tempfile::tempdir().unwrap();
    let (mut db, hold) = actor(dir.path(), 1 << 30);
    let path = db.path().to_owned();
    let before = std::fs::read(&path).unwrap();
    db.exec("INSERT INTO t VALUES ('pending')", &[]).unwrap();
    db.mark_boundary().unwrap();
    let wal_path = format!("{}-wal", path.display());
    let wal = std::fs::read(&wal_path).unwrap();
    drop(hold); // GaveUp: server must restore before this file is reused.
    drop(db);
    assert_eq!(std::fs::read(path).unwrap(), before);
    assert_eq!(std::fs::read(wal_path).unwrap(), wal);
}

#[test]
#[cfg(debug_assertions)]
fn closing_with_pending_boundaries_and_live_hold_asserts_and_preserves_wal() {
    let dir = tempfile::tempdir().unwrap();
    let (mut db, hold) = actor(dir.path(), 1 << 30);
    let path = db.path().to_owned();
    let before = std::fs::read(&path).unwrap();
    db.exec("INSERT INTO t VALUES ('pending')", &[]).unwrap();
    db.mark_boundary().unwrap();
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(db)));
    assert!(panic.is_err());
    assert_eq!(std::fs::read(&path).unwrap(), before);
    assert!(
        std::fs::metadata(format!("{}-wal", path.display()))
            .unwrap()
            .len()
            > 0
    );
    drop(hold);
}

#[test]
fn flushed_connection_can_close_while_hold_lives() {
    let dir = tempfile::tempdir().unwrap();
    let (mut db, hold) = actor(dir.path(), 1 << 30);
    db.exec("INSERT INTO t VALUES ('captured')", &[]).unwrap();
    db.mark_boundary().unwrap();
    db.take_bounded_changes(db.next_bounded_capture().unwrap())
        .unwrap();
    assert!(!db.has_pending_boundaries());
    drop(db); // Storage is drained; server still owes the acknowledgement barrier.
    drop(hold);
}

#[test]
fn discarding_a_capture_breaks_the_next_segments_checksum_chain() {
    let dir = tempfile::tempdir().unwrap();
    let (conn, mut c) = raw(&dir.path().join("a.db"));
    let mut r = Restorer::create(&dir.path().join("restore.db"), 1 << 30).unwrap();
    r.apply(&c.snapshot().unwrap().to_ltx(1, 1, 0).unwrap())
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'lost')", [])
        .unwrap();
    c.mark_boundary().unwrap();
    drop(c.capture_bounded(u64::MAX).unwrap().unwrap());
    conn.execute("INSERT INTO t VALUES (2, 'next')", [])
        .unwrap();
    c.mark_boundary().unwrap();
    let next = c.capture_bounded(u64::MAX).unwrap().unwrap();
    let err = r.apply(&next.changes.to_ltx(3, 3, 0).unwrap()).unwrap_err();
    assert!(err.to_string().contains("pre-apply checksum"));
}

#[test]
fn storage_crash_images_at_capture_checkpoint_put_and_sidecar_cuts() {
    // PUT sent has two possible durable outcomes, unlike checkpointed (no
    // request sent). Sidecar cuts use actual files and production's predicate.
    for (cut, checkpointed, put_sent, landed, sidecar_written) in [
        ("captured", false, false, false, false),
        ("checkpointed", true, false, false, false),
        ("put sent, lost", true, true, false, false),
        ("put sent, landed without ack", true, true, true, false),
        ("put landed", true, true, true, false),
        ("sidecar written", true, true, true, true),
        (
            "sidecar written, WAL not checkpointed",
            false,
            true,
            true,
            true,
        ),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let (mut db, hold) = actor(dir.path(), if checkpointed { 1 } else { 1 << 30 });
        let initial = db.snapshot().unwrap();
        db.exec("INSERT INTO t VALUES ('committed')", &[]).unwrap();
        db.mark_boundary().unwrap();
        let c = db
            .take_bounded_changes(db.next_bounded_capture().unwrap())
            .unwrap();
        let segment = c.changes.to_ltx(2, 2, 0).unwrap();
        // Model the object store receiving a request, with visibility allowed
        // before acknowledgement. Before PUT, there is no segment to land.
        let submitted = put_sent.then_some(segment);
        let bucket_segment = landed.then(|| submitted.as_ref().unwrap());
        let bucket_head = if bucket_segment.is_some() { 2 } else { 1 };
        let (sidecar_head, sidecar_sum) = if sidecar_written {
            (2, c.changes.post_apply)
        } else {
            (1, initial.post_apply)
        };
        let local = dir.path().join("crash.db");
        std::fs::copy(db.path(), &local).unwrap();
        let crash_wal = format!("{}-wal", local.display());
        std::fs::copy(format!("{}-wal", db.path().display()), &crash_wal).unwrap();
        assert_eq!(
            std::fs::metadata(&crash_wal).unwrap().len() == 0,
            checkpointed
        );
        std::fs::write(
            format!("{}.replica", local.display()),
            format!("1 {sidecar_head} {}", sidecar_sum.value()),
        )
        .unwrap();
        let reusable = wasmkeep_storage::local_is_at(&local, 1, bucket_head);
        assert_eq!(
            reusable.is_some(),
            checkpointed && sidecar_written,
            "cut {cut}"
        );
        let restored = dir.path().join("restored.db");
        let mut r = Restorer::create(&restored, 1 << 30).unwrap();
        r.apply(&initial.to_ltx(1, 1, 0).unwrap()).unwrap();
        if let Some(segment) = bucket_segment {
            r.apply(segment).unwrap();
        }
        let expected = if landed {
            c.changes.post_apply
        } else {
            initial.post_apply
        };
        assert_eq!(r.finish().unwrap(), expected, "cut {cut}");
        drop(hold); // process loss; never a clean handover
    }
}

#[test]
fn default_cap_bounds_incompressible_segments_and_plans_do_not_consume() {
    let dir = tempfile::tempdir().unwrap();
    let (mut db, hold) = actor(dir.path(), 1 << 30);
    let mut seed = 9182;
    for _ in 0..10 {
        let data = (0..150_000).map(|_| next(&mut seed) as u8).collect();
        db.exec("INSERT INTO t VALUES (?1)", &[SqlValue::Blob(data)])
            .unwrap();
        db.mark_boundary().unwrap();
    }
    let mut captures = 0;
    while db.has_pending_boundaries() {
        let checksum = db.captured_checksum();
        let plan = db.next_bounded_capture().unwrap();
        assert_eq!(db.next_bounded_capture(), Some(plan));
        assert_eq!(db.captured_checksum(), checksum);
        let got = db
            .take_bounded_changes(db.next_bounded_capture().unwrap())
            .unwrap();
        assert_eq!(got.boundary, plan.boundary);
        assert_eq!(got.boundaries, plan.boundaries);
        assert!(!got.oversize);
        assert!(got.boundary.estimated_bytes <= DEFAULT_SEGMENT_BYTES);
        assert!(got.changes.to_ltx(2, 2, 0).unwrap().len() as u64 <= DEFAULT_SEGMENT_BYTES);
        captures += 1;
    }
    assert!(captures > 1);
    drop(hold);
}

#[test]
fn bound_covers_encoding_for_all_sqlite_page_sizes() {
    for page_size in [512, 1024, 2048, 4096, 8192, 16384, 32768, 65536] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("a.db");
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch(&format!("PRAGMA page_size={page_size}; PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0; CREATE TABLE t(v BLOB); PRAGMA wal_checkpoint(TRUNCATE)")).unwrap();
        let mut c = WalCapture::start(&path, page_size).unwrap();
        conn.execute("INSERT INTO t VALUES (randomblob(150000))", [])
            .unwrap();
        let b = c.mark_boundary().unwrap().unwrap();
        let got = c.capture_bounded(b.estimated_bytes).unwrap().unwrap();
        assert!(!got.oversize);
        assert_eq!(got.changes.pages.len(), b.distinct_pages);
        assert!(got.changes.to_ltx(2, 2, 0).unwrap().len() as u64 <= b.estimated_bytes);
    }
}

#[test]
fn batched_capture_continues_after_checkpoint_with_new_wal_salts() {
    let dir = tempfile::tempdir().unwrap();
    let (mut db, hold) = actor(dir.path(), 1);
    for i in 0..6 {
        db.exec("INSERT INTO t VALUES (?1)", &[SqlValue::Integer(i)])
            .unwrap();
        db.mark_boundary().unwrap();
        let old = db.captured_checksum().unwrap();
        let got = db
            .take_bounded_changes(db.next_bounded_capture().unwrap())
            .unwrap();
        assert_eq!(got.changes.pre_apply, old);
        assert_eq!(db.snapshot().unwrap().post_apply, got.changes.post_apply);
    }
    drop(hold);
}

#[test]
fn transaction_marks_and_operations_after_give_up_are_refused() {
    let dir = tempfile::tempdir().unwrap();
    let (mut db, hold) = actor(dir.path(), 1 << 30);
    db.exec("BEGIN", &[]).unwrap();
    db.exec("INSERT INTO t VALUES ('uncommitted')", &[])
        .unwrap();
    assert!(db.mark_boundary().is_err());
    db.end_event().unwrap();
    assert!(db.mark_boundary().unwrap().is_none());
    db.exec("INSERT INTO t VALUES ('pending')", &[]).unwrap();
    db.mark_boundary().unwrap();
    drop(hold);
    assert!(db.mark_boundary().is_err());
    assert!(
        db.take_bounded_changes(db.next_bounded_capture().unwrap())
            .is_err()
    );
    assert!(db.start_capture().is_err());
}

#[test]
fn reset_wal_is_refused_without_consuming_a_boundary() {
    let dir = tempfile::tempdir().unwrap();
    let (conn, mut c) = raw(&dir.path().join("a.db"));
    conn.execute("INSERT INTO t VALUES (1, 'old')", []).unwrap();
    let b = c.mark_boundary().unwrap().unwrap();
    let before = c.checksum();
    // External checkpoint: our ActorDb paths prohibit this.
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (2, 'new')", []).unwrap();
    assert!(matches!(
        c.capture_bounded(u64::MAX),
        Err(CaptureError::Lost(_))
    ));
    assert_eq!(c.boundaries().collect::<Vec<_>>(), vec![b]);
    assert_eq!(c.checksum(), before);
    assert_eq!(c.wal_bytes(), 0);
}

#[test]
fn p8_commit_then_rolled_back_tail_then_short_commit() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("a.db");
    let (conn, mut c) = raw(&path);
    let mut single_scan = WalCapture::start(&path, 4096).unwrap();
    let mut strict = WalCapture::start(&path, 4096).unwrap();
    let snap = c.snapshot().unwrap().to_ltx(1, 1, 0).unwrap();
    let rp = dir.path().join("r.db");
    let mut r = Restorer::create(&rp, 1 << 30).unwrap();
    r.apply(&snap).unwrap();
    conn.execute_batch("PRAGMA cache_size=5; BEGIN; INSERT INTO t VALUES (1, randomblob(3000)); INSERT INTO t VALUES (2, randomblob(3000)); INSERT INTO t VALUES (5, randomblob(3000)); INSERT INTO t VALUES (6, randomblob(3000)); INSERT INTO t VALUES (7, randomblob(3000)); INSERT INTO t VALUES (8, randomblob(3000)); INSERT INTO t VALUES (9, randomblob(3000)); INSERT INTO t VALUES (10, randomblob(3000)); INSERT INTO t VALUES (11, randomblob(3000)); INSERT INTO t VALUES (12, randomblob(3000)); COMMIT; BEGIN; UPDATE t SET v=randomblob(3000);").unwrap();
    conn.execute_batch("ROLLBACK").unwrap();
    let b = c.mark_boundary().unwrap().unwrap();
    let s1 = strict.capture().unwrap().unwrap();
    let wal = std::fs::read(c.wal_path()).unwrap();
    let tail: Vec<u32> = (b.offset as usize..wal.len())
        .step_by(4120)
        .map(|o| u32::from_be_bytes(wal[o..o + 4].try_into().unwrap()))
        .collect();
    assert!(tail.iter().any(|p| *p <= b.commit));
    assert!(
        wal.len() as u64 > b.offset,
        "a rolled-back tail follows the boundary"
    );
    let frame = b.offset as usize - (4096 + 24);
    assert_eq!(
        u32::from_be_bytes(wal[frame + 4..frame + 8].try_into().unwrap()),
        b.commit
    );
    conn.execute("INSERT INTO t VALUES (3, 'after')", [])
        .unwrap();
    c.mark_boundary().unwrap().unwrap();
    let s2 = strict.capture().unwrap().unwrap();
    let got = c.capture_bounded(0).unwrap().unwrap();
    assert_eq!(got.changes, s1);
    r.apply(&got.changes.to_ltx(2, 2, 0).unwrap()).unwrap();
    let got = c.capture_bounded(0).unwrap().unwrap();
    assert_eq!(got.changes, s2);
    r.apply(&got.changes.to_ltx(3, 3, 0).unwrap()).unwrap();
    r.finish().unwrap();
    single_scan.mark_boundary().unwrap();
    let combined = single_scan.capture_bounded(u64::MAX).unwrap().unwrap();
    let combined_path = dir.path().join("combined.db");
    let mut combined_restore = Restorer::create(&combined_path, 1 << 30).unwrap();
    combined_restore.apply(&snap).unwrap();
    combined_restore
        .apply(&combined.changes.to_ltx(2, 2, 0).unwrap())
        .unwrap();
    combined_restore.finish().unwrap();
    assert_eq!(
        std::fs::read(&combined_path).unwrap(),
        std::fs::read(&rp).unwrap()
    );
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
        .unwrap();
    assert_eq!(std::fs::read(&rp).unwrap(), std::fs::read(&path).unwrap());
}

#[test]
fn p10_committed_spill_with_rewrites_and_savepoint_rollback() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("a.db");
    let (conn, mut c) = raw(&path);
    let mut strict = WalCapture::start(&path, 4096).unwrap();
    let snap = c.snapshot().unwrap().to_ltx(1, 1, 0).unwrap();
    let rp = dir.path().join("r.db");
    let mut r = Restorer::create(&rp, 1 << 30).unwrap();
    r.apply(&snap).unwrap();
    conn.execute_batch("PRAGMA cache_size=5;").unwrap();
    let mut txid = 1;
    for round in 0..6 {
        conn.execute_batch("BEGIN").unwrap();
        for id in 1..=12 {
            conn.execute(
                "INSERT OR REPLACE INTO t VALUES (?1, randomblob(3000))",
                [id],
            )
            .unwrap();
        }
        // rewrite the same pages several times past the cache: spilled frames get rewritten
        for _ in 0..3 {
            conn.execute_batch("UPDATE t SET v = randomblob(3000)")
                .unwrap();
        }
        conn.execute_batch("SAVEPOINT s; UPDATE t SET v = randomblob(3100); INSERT INTO t VALUES (100, randomblob(200000)); ROLLBACK TO s; RELEASE s;").unwrap();
        conn.execute_batch("UPDATE t SET v = randomblob(2900) WHERE id % 2 = 0; COMMIT")
            .unwrap();
        if round % 2 == 1 {
            // and a rolled-back spill tail after the commit
            conn.execute_batch("BEGIN; UPDATE t SET v = randomblob(3000); ROLLBACK")
                .unwrap();
        }
        let wal = std::fs::read(c.wal_path()).unwrap();
        let b = c.mark_boundary().unwrap().unwrap();
        let frames: Vec<u32> = (32..b.offset as usize)
            .step_by(4120)
            .map(|o| u32::from_be_bytes(wal[o..o + 4].try_into().unwrap()))
            .collect();
        let mut d = frames.clone();
        d.sort();
        d.dedup();
        assert!(frames.len() > d.len(), "must exercise repeated page frames");
        let s = strict.capture().unwrap().unwrap();
        let got = c.capture_bounded(u64::MAX).unwrap().unwrap();
        assert_eq!(got.changes, s, "round {round}");
        txid += 1;
        r.apply(&got.changes.to_ltx(txid, txid, 0).unwrap())
            .unwrap();
    }
    r.finish().unwrap();
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
        .unwrap();
    assert_eq!(std::fs::read(&rp).unwrap(), std::fs::read(&path).unwrap());
}

#[test]
fn strict_guards_preserve_checksum_and_position() {
    let dir = tempfile::tempdir().unwrap();
    let (mut db, hold) = actor(dir.path(), 1 << 30);
    db.exec("INSERT INTO t VALUES ('only unmarked')", &[])
        .unwrap();
    let before = (db.captured_checksum(), db.captured_wal_bytes());
    assert!(matches!(
        db.take_changes(),
        Err(CaptureError::BoundariesPending)
    ));
    assert_eq!((db.captured_checksum(), db.captured_wal_bytes()), before);
    drop(hold);

    let (conn, mut c) = raw(&dir.path().join("raw.db"));
    conn.execute("INSERT INTO t VALUES (1, 'marked')", [])
        .unwrap();
    c.mark_boundary().unwrap();
    let before = (c.checksum(), c.wal_bytes());
    assert!(matches!(c.capture(), Err(CaptureError::BoundariesPending)));
    assert_eq!((c.checksum(), c.wal_bytes()), before);
}

#[test]
fn reserved_plan_captures_exact_prefix_and_stale_plan_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let (mut db, hold) = actor(dir.path(), 1 << 30);
    db.exec("INSERT INTO t VALUES (zeroblob(20000))", &[])
        .unwrap();
    db.mark_boundary().unwrap();
    let plan = db.next_bounded_capture().unwrap();
    db.exec("INSERT INTO t VALUES (zeroblob(400000))", &[])
        .unwrap();
    db.mark_boundary().unwrap();
    // Both a new mark and a changed cap must leave the reservation intact.
    db.set_segment_byte_cap(0);
    let got = db.take_bounded_changes(plan).unwrap();
    assert_eq!(got.boundary, plan.boundary);
    assert_eq!(got.boundaries, plan.boundaries);
    assert_eq!(got.oversize, plan.oversize);
    let before = (
        db.captured_checksum(),
        db.captured_wal_bytes(),
        db.boundaries().collect::<Vec<_>>(),
    );
    assert!(matches!(
        db.take_bounded_changes(plan),
        Err(CaptureError::Lost(_))
    ));
    assert_eq!(
        (
            db.captured_checksum(),
            db.captured_wal_bytes(),
            db.boundaries().collect::<Vec<_>>()
        ),
        before
    );
    drop(hold);
}

#[test]
fn sustained_marks_signal_four_mib_pressure_and_drain_resets_it() {
    let dir = tempfile::tempdir().unwrap();
    let (mut db, hold) = actor(dir.path(), 8 << 20);
    for _ in 0..2 {
        db.exec("INSERT INTO t VALUES (zeroblob(100000))", &[])
            .unwrap();
    }
    db.mark_boundary().unwrap();
    db.take_bounded_changes(db.next_bounded_capture().unwrap())
        .unwrap();
    let cap = estimated_segment_bytes(4096, 40);
    db.set_segment_byte_cap(cap);
    db.exec("UPDATE t SET v=randomblob(100000) WHERE rowid=1", &[])
        .unwrap();
    db.mark_boundary().unwrap();
    let mut fired = false;
    for i in 0..100 {
        let before = std::fs::metadata(format!("{}-wal", db.path().display()))
            .unwrap()
            .len();
        assert!(before < 4 << 20);
        assert!(!db.wal_wants_checkpoint());
        db.exec(
            "UPDATE t SET v=randomblob(100000) WHERE rowid=?1",
            &[SqlValue::Integer(1 + (i + 1) % 2)],
        )
        .unwrap();
        db.mark_boundary().unwrap();
        db.take_bounded_changes(db.next_bounded_capture().unwrap())
            .unwrap();
        assert!(db.has_pending_boundaries(), "always one event behind");
        assert!(db.boundaries().last().unwrap().estimated_bytes < cap);
        let len = std::fs::metadata(format!("{}-wal", db.path().display()))
            .unwrap()
            .len();
        assert!(len > before, "capturing without draining cannot checkpoint");
        assert_eq!(db.wal_wants_checkpoint(), len >= 4 << 20);
        if db.wal_wants_checkpoint() {
            assert!(len < 8 << 20, "signal must precede the quota");
            fired = true;
            break;
        }
    }
    assert!(fired);
    // The S8 event-start rule must hold the actor here, allowing a full drain.
    while let Some(plan) = db.next_bounded_capture() {
        db.take_bounded_changes(plan).unwrap();
    }
    assert!(!db.wal_wants_checkpoint());
    assert_eq!(
        std::fs::metadata(format!("{}-wal", db.path().display()))
            .unwrap()
            .len(),
        0
    );
    drop(hold);
}

#[test]
fn blocked_checkpoint_returns_changes_and_can_retry_without_another_event() {
    let dir = tempfile::tempdir().unwrap();
    let (mut db, hold) = actor(dir.path(), 6000);
    let mut r = Restorer::create(&dir.path().join("restored.db"), 1 << 30).unwrap();
    r.apply(&db.snapshot().unwrap().to_ltx(1, 1, 0).unwrap())
        .unwrap();
    db.exec("INSERT INTO t VALUES ('a')", &[]).unwrap();
    db.mark_boundary().unwrap();
    let reader = Connection::open(db.path()).unwrap();
    reader.execute_batch("BEGIN").unwrap();
    let _: i64 = reader
        .query_row("SELECT count(*) FROM t", [], |r| r.get(0))
        .unwrap();
    db.exec("INSERT INTO t VALUES ('b')", &[]).unwrap();
    db.mark_boundary().unwrap();
    let got = db
        .take_bounded_changes(db.next_bounded_capture().unwrap())
        .unwrap();
    assert!(db.wal_wants_checkpoint());
    assert!(!db.has_pending_boundaries());
    assert!(db.finish_batched_capture().is_err());
    r.apply(&got.changes.to_ltx(2, 2, 0).unwrap()).unwrap();
    assert_eq!(r.finish().unwrap(), got.changes.post_apply);
    drop(reader);
    db.checkpoint_batched_capture().unwrap();
    assert!(!db.wal_wants_checkpoint());
    assert_eq!(
        WalCapture::start(db.path(), 4096).unwrap().checksum(),
        got.changes.post_apply
    );
    db.finish_batched_capture().unwrap();
    drop(hold);
}

#[test]
fn clean_batched_finish_refuses_uncaptured_then_makes_local_file_reusable() {
    let dir = tempfile::tempdir().unwrap();
    let (mut db, hold) = actor(dir.path(), 1 << 30);
    let path = db.path().to_owned();
    db.exec("INSERT INTO t VALUES ('committed')", &[]).unwrap();
    let main = std::fs::read(&path).unwrap();
    assert!(matches!(
        db.finish_batched_capture(),
        Err(CaptureError::BoundariesPending)
    ));
    db.mark_boundary().unwrap();
    assert!(matches!(
        db.finish_batched_capture(),
        Err(CaptureError::BoundariesPending)
    ));
    assert_eq!(std::fs::read(&path).unwrap(), main);
    let got = db
        .take_bounded_changes(db.next_bounded_capture().unwrap())
        .unwrap();
    std::fs::write(
        format!("{}.replica", path.display()),
        format!("1 2 {}", got.changes.post_apply.value()),
    )
    .unwrap();
    assert!(wasmkeep_storage::local_is_at(&path, 1, 2).is_none());
    db.finish_batched_capture().unwrap();
    assert!(db.take_changes().unwrap().is_none(), "batched flag cleared");
    assert!(db.mark_boundary().is_err());
    assert_eq!(
        wasmkeep_storage::local_is_at(&path, 1, 2),
        Some(got.changes.post_apply)
    );
    drop(db);
    assert_eq!(
        wasmkeep_storage::local_is_at(&path, 1, 2),
        Some(got.changes.post_apply)
    );
    drop(hold);
}

#[test]
fn boundary_sequences_do_not_repeat_across_checkpoints() {
    let dir = tempfile::tempdir().unwrap();
    let (mut db, hold) = actor(dir.path(), 1);
    let mut previous = None;
    let mut old_plan = None;
    for i in 0..3 {
        db.exec("INSERT INTO t VALUES (?1)", &[SqlValue::Integer(i)])
            .unwrap();
        let boundary = db.mark_boundary().unwrap().unwrap();
        if let Some((offset, sequence)) = previous {
            assert_eq!(boundary.offset, offset, "reproduce reused offsets");
            assert!(boundary.sequence > sequence);
            assert!(matches!(
                db.take_bounded_changes(old_plan.unwrap()),
                Err(CaptureError::Lost(_))
            ));
        }
        let plan = db.next_bounded_capture().unwrap();
        db.take_bounded_changes(plan).unwrap();
        previous = Some((boundary.offset, boundary.sequence));
        old_plan = Some(plan);
    }
    drop(hold);
}

#[test]
#[cfg(debug_assertions)]
fn owner_field_order_checks_live_hold_for_unmarked_commits_even_after_capture() {
    // Rust drops fields in declaration order. The actor must precede its hold.
    struct Owner {
        db: ActorDb,
        _hold: CaptureHold,
    }
    for captured in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let (db, hold) = actor(dir.path(), 1 << 30);
        let mut owner = Owner { db, _hold: hold };
        let path = owner.db.path().to_owned();
        let main = std::fs::read(&path).unwrap();
        if captured {
            owner
                .db
                .exec("INSERT INTO t VALUES ('in flight')", &[])
                .unwrap();
            owner.db.mark_boundary().unwrap();
            owner
                .db
                .take_bounded_changes(owner.db.next_bounded_capture().unwrap())
                .unwrap();
        }
        owner
            .db
            .exec("INSERT INTO t VALUES ('unmarked')", &[])
            .unwrap();
        let wal = std::fs::read(format!("{}-wal", path.display())).unwrap();
        let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(owner)));
        assert!(panic.is_err());
        assert_eq!(std::fs::read(&path).unwrap(), main);
        assert_eq!(
            std::fs::read(format!("{}-wal", path.display())).unwrap(),
            wal
        );
    }
}

#[test]
fn externally_truncated_wal_reports_lost_without_advancing() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("a.db");
    let (conn, mut c) = raw(&path);
    conn.execute("INSERT INTO t VALUES (1, 'old')", []).unwrap();
    let b = c.mark_boundary().unwrap().unwrap();
    let before = (c.checksum(), c.wal_bytes());
    let other = Connection::open(&path).unwrap();
    other
        .execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
        .unwrap();
    assert!(matches!(
        c.capture_bounded(u64::MAX),
        Err(CaptureError::Lost(_))
    ));
    assert_eq!(c.boundaries().collect::<Vec<_>>(), vec![b]);
    assert_eq!((c.checksum(), c.wal_bytes()), before);
}

#[test]
fn estimate_reserves_lz4_expansion_and_worst_case_index_separately() {
    use wasmkeep_storage::ltx::{HEADER_SIZE, PAGE_HEADER_SIZE, TRAILER_SIZE};
    for page_size in [512, 4096, 65536] {
        let fixed = (HEADER_SIZE + PAGE_HEADER_SIZE + 1 + 8 + TRAILER_SIZE) as u64;
        assert_eq!(estimated_segment_bytes(page_size, 0), fixed);
        for pages in [1, 127, 1024] {
            let lz4 = lz4_flex::block::get_maximum_output_size(page_size as usize) as u64;
            let payload_and_headers = (lz4 + PAGE_HEADER_SIZE as u64 + 4) * pages;
            let estimate = estimated_segment_bytes(page_size, pages as usize);
            assert!(
                estimate >= fixed + payload_and_headers + 30 * pages,
                "reserve expansion AND three ten-byte index varints per page"
            );
        }
    }
}

#[test]
fn pressure_starts_exactly_at_threshold_and_batched_quota_names_drain() {
    for threshold in [6000, 4 << 20, 8 << 20] {
        let dir = tempfile::tempdir().unwrap();
        let (mut db, hold) = actor(dir.path(), threshold);
        let threshold = threshold.min(4 << 20);
        let wal_path = format!("{}-wal", db.path().display());
        // A rolled-back spill can leave arbitrary physical tail length.
        let wal = std::fs::OpenOptions::new()
            .write(true)
            .open(wal_path)
            .unwrap();
        wal.set_len(threshold - 1).unwrap();
        assert!(!db.wal_wants_checkpoint());
        wal.set_len(threshold).unwrap();
        assert!(db.wal_wants_checkpoint());
        wal.set_len(9 << 20).unwrap();
        let err = db
            .exec("INSERT INTO t VALUES ('refused')", &[])
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("until batched capture drains and checkpoints")
        );
        // Artificial file was never a valid WAL; do not ask capture to scan it.
        wal.set_len(0).unwrap();
        drop(hold);
    }
}

#[test]
fn capture_session_survives_checkpoint_but_not_restart() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("source.db");
    let (conn, mut capture) = raw(&path);
    conn.execute_batch("INSERT INTO t VALUES (1, X'01')")
        .unwrap();
    let first = capture.mark_boundary().unwrap().unwrap();
    let plan = capture.next_capture(DEFAULT_SEGMENT_BYTES).unwrap();
    capture.capture_plan(plan).unwrap();
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
        .unwrap();
    capture.wal_truncated();
    conn.execute_batch("INSERT INTO t VALUES (2, X'02')")
        .unwrap();
    let second = capture.mark_boundary().unwrap().unwrap();
    assert_eq!(first.session, second.session);
    assert!(second.sequence > first.sequence);
    let plan = capture.next_capture(DEFAULT_SEGMENT_BYTES).unwrap();
    capture.capture_plan(plan).unwrap();
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
        .unwrap();
    let mut restarted = WalCapture::start(&path, 4096).unwrap();
    conn.execute_batch("INSERT INTO t VALUES (3, X'03')")
        .unwrap();
    let third = restarted.mark_boundary().unwrap().unwrap();
    assert_ne!(third.session, first.session);
    assert_eq!(third.sequence, first.sequence);
    assert!(restarted.capture_plan(plan).is_err());
    assert!(restarted.has_pending_boundaries());
}
