//! Capture → LTX → restore round trips on real SQLite databases: what an
//! actor committed is exactly what a restore rebuilds, checksums included.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code

use std::path::Path;

use wasmkeep_storage::ltx::{self, DbChecksum};
use wasmkeep_storage::wal::{CaptureError, Changes, restore};
use wasmkeep_storage::{ActorDb, SqlValue, StateDir, Storage, StorageConfig};

/// The LTX files an owner would upload: a snapshot, then one segment per
/// capture.
struct Shipped {
    files: Vec<Vec<u8>>,
    txid: u64,
}

impl Shipped {
    fn start(db: &mut ActorDb) -> Self {
        let snap = db.snapshot().unwrap();
        Self {
            files: vec![snap.to_ltx(1, 1, 0).unwrap()],
            txid: 1,
        }
    }

    fn ship(&mut self, changes: &Changes) {
        self.txid += 1;
        self.files
            .push(changes.to_ltx(self.txid, self.txid, 0).unwrap());
    }

    /// Captures and ships; returns whether anything was committed.
    fn capture(&mut self, db: &mut ActorDb) -> bool {
        match db.take_changes().unwrap() {
            Some(c) => {
                self.ship(&c);
                true
            }
            None => false,
        }
    }

    fn restore_to(&self, path: &Path) -> DbChecksum {
        let decoded: Vec<ltx::File> = self.files.iter().map(|b| ltx::decode(b).unwrap()).collect();
        restore(path, Some(&decoded[0]), &decoded[1..]).unwrap()
    }
}

fn exec(db: &mut ActorDb, sql: &str, params: &[SqlValue]) {
    db.exec(sql, params)
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
}

fn dump(path: &Path) -> Vec<String> {
    let conn =
        rusqlite::Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
            .unwrap();
    let ok: String = conn
        .query_row("PRAGMA integrity_check", [], |r| r.get(0))
        .unwrap();
    assert_eq!(ok, "ok", "restored database is corrupt");
    let mut out = Vec::new();
    for table in ["t", "_wasmkeep_meta", "_wasmkeep_alarm"] {
        let mut stmt = conn
            .prepare(&format!("SELECT * FROM {table} ORDER BY 1"))
            .unwrap();
        let width = stmt.column_count();
        let rows = stmt
            .query_map([], |r| {
                let mut row = Vec::new();
                for i in 0..width {
                    row.push(format!("{:?}", r.get_ref(i)?));
                }
                Ok(row.join("|"))
            })
            .unwrap();
        for row in rows {
            out.push(format!("{table}: {}", row.unwrap()));
        }
    }
    out
}

fn open(dir: &Path) -> ActorDb {
    let storage = rooted(StorageConfig::new(dir));
    let mut db = storage.open("c", "a").unwrap();
    db.start_capture().unwrap();
    db
}

#[test]
fn what_was_committed_is_what_is_restored() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = open(dir.path());
    let mut shipped = Shipped::start(&mut db);
    assert!(!shipped.capture(&mut db), "nothing committed yet");

    exec(
        &mut db,
        "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT, b BLOB)",
        &[],
    );
    exec(&mut db, "INSERT INTO t (v) VALUES ('one')", &[]);
    assert!(shipped.capture(&mut db));

    // Several commits in one event become one segment.
    for i in 0..20 {
        exec(
            &mut db,
            "INSERT INTO t (v) VALUES (?1)",
            &[SqlValue::Text(format!("row {i}"))],
        );
    }
    db.set_alarm(42).unwrap();
    assert!(shipped.capture(&mut db));

    // A rolled-back transaction ships nothing.
    exec(&mut db, "BEGIN", &[]);
    exec(&mut db, "INSERT INTO t (v) VALUES ('never')", &[]);
    db.end_event().unwrap();
    assert!(!shipped.capture(&mut db), "a rollback is not a change");

    // Enough data to pass the checkpoint threshold: the WAL is truncated
    // and restarts with new salts, and capture carries on.
    for _ in 0..6 {
        exec(&mut db, "INSERT INTO t (b) VALUES (zeroblob(1000000))", &[]);
        assert!(shipped.capture(&mut db));
    }
    exec(&mut db, "UPDATE t SET v = 'changed' WHERE id = 1", &[]);
    exec(&mut db, "DELETE FROM t WHERE id BETWEEN 3 AND 10", &[]);
    assert!(shipped.capture(&mut db));

    let source_sum = db.captured_checksum().unwrap();
    let source = db.path().to_owned();
    // Snapshot again: it must agree with what capture tracked.
    let snap = db.snapshot().unwrap();
    assert_eq!(snap.post_apply, source_sum);
    drop(db);

    let restored = dir.path().join("restored.db");
    let sum = shipped.restore_to(&restored);
    assert_eq!(sum, source_sum, "checksums agree");
    assert_eq!(dump(&restored), dump(&source));
    let conn = rusqlite::Connection::open(&restored).unwrap();
    let v: String = conn
        .query_row("SELECT v FROM t WHERE id = 1", [], |r| r.get(0))
        .unwrap();
    let rows: i64 = conn
        .query_row("SELECT count(*) FROM t", [], |r| r.get(0))
        .unwrap();
    assert_eq!((v.as_str(), rows), ("changed", 21 + 6 - 8));
}

#[test]
fn a_snapshot_with_changes_pending_hands_them_back() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = open(dir.path());
    let mut shipped = Shipped::start(&mut db);
    exec(
        &mut db,
        "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT, b BLOB)",
        &[],
    );
    match db.snapshot() {
        Err(CaptureError::Pending(changes)) => shipped.ship(&changes),
        other => panic!("expected the pending changes back, got {other:?}"),
    }
    exec(&mut db, "INSERT INTO t (v) VALUES ('after')", &[]);
    assert!(shipped.capture(&mut db));
    let source = db.path().to_owned();
    drop(db);
    let restored = dir.path().join("restored.db");
    shipped.restore_to(&restored);
    assert_eq!(dump(&restored), dump(&source));
}

#[test]
fn a_rolled_back_transaction_that_spilled_to_the_wal_is_not_shipped() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = open(dir.path());
    let mut shipped = Shipped::start(&mut db);
    exec(
        &mut db,
        "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT, b BLOB)",
        &[],
    );
    assert!(shipped.capture(&mut db));

    // More than the page cache holds, so SQLite writes these uncommitted
    // pages into the WAL before the rollback.
    exec(&mut db, "BEGIN", &[]);
    for _ in 0..12 {
        exec(
            &mut db,
            "INSERT INTO t (b) VALUES (randomblob(1000000))",
            &[],
        );
    }
    db.end_event().unwrap();
    let wal = dir.path().join("actors/t/c/a.db-wal");
    assert!(
        std::fs::metadata(&wal).unwrap().len() > 4 << 20,
        "the transaction did not spill; the test proves nothing"
    );
    assert!(!shipped.capture(&mut db), "uncommitted frames were shipped");

    // The next commit overwrites the stale frames; only it is shipped.
    exec(&mut db, "INSERT INTO t (v) VALUES ('kept')", &[]);
    assert!(shipped.capture(&mut db));
    let source = db.path().to_owned();
    drop(db);
    let restored = dir.path().join("restored.db");
    shipped.restore_to(&restored);
    assert_eq!(dump(&restored), dump(&source));
    let conn = rusqlite::Connection::open(&restored).unwrap();
    let rows: i64 = conn
        .query_row("SELECT count(*) FROM t", [], |r| r.get(0))
        .unwrap();
    assert_eq!(rows, 1);
}

#[test]
fn a_segment_out_of_order_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = open(dir.path());
    let mut shipped = Shipped::start(&mut db);
    exec(
        &mut db,
        "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT, b BLOB)",
        &[],
    );
    shipped.capture(&mut db);
    exec(&mut db, "INSERT INTO t (v) VALUES ('x')", &[]);
    shipped.capture(&mut db);
    let mut files: Vec<ltx::File> = shipped
        .files
        .iter()
        .map(|b| ltx::decode(b).unwrap())
        .collect();
    files.swap(1, 2);
    let err = restore(&dir.path().join("r.db"), Some(&files[0]), &files[1..]);
    assert!(err.is_err(), "applying segments out of order must fail");
}

// ---- Restore hardening (security review): a corrupt or hostile bucket
// object must be refused before it costs memory or disk.

/// A snapshot and two segments of a small database.
fn small_chain(dir: &tempfile::TempDir) -> Shipped {
    let storage = rooted(StorageConfig::new(dir.path()));
    let mut db = storage.open("c", "a").unwrap();
    db.start_capture().unwrap();
    let mut shipped = Shipped::start(&mut db);
    exec(&mut db, "CREATE TABLE t (x)", &[]);
    assert!(shipped.capture(&mut db));
    exec(&mut db, "INSERT INTO t VALUES (zeroblob(100000))", &[]);
    assert!(shipped.capture(&mut db));
    shipped
}

#[test]
fn streaming_restore_matches_the_whole_chain_restore() {
    let dir = tempfile::tempdir().unwrap();
    let shipped = small_chain(&dir);
    let whole = shipped.restore_to(&dir.path().join("whole.db"));
    let path = dir.path().join("streamed.db");
    let mut r = wasmkeep_storage::wal::Restorer::create(&path, 1 << 30).unwrap();
    for f in &shipped.files {
        r.apply(f).unwrap();
    }
    assert_eq!(r.finish().unwrap(), whole);
    assert_eq!(dump(&path), dump(&dir.path().join("whole.db")));
}

#[test]
fn a_file_claiming_a_huge_database_is_refused_before_anything_is_allocated() {
    let dir = tempfile::tempdir().unwrap();
    let shipped = small_chain(&dir);
    // The header's page count (bytes 12..16) set to u32::MAX: before, a
    // restore allocated zero pages up to it (and the process aborted).
    let mut forged = shipped.files[1].clone();
    forged[12..16].copy_from_slice(&u32::MAX.to_be_bytes());
    let path = dir.path().join("r.db");
    let mut r = wasmkeep_storage::wal::Restorer::create(&path, 64 << 20).unwrap();
    r.apply(&shipped.files[0]).unwrap();
    let err = r.apply(&forged).unwrap_err();
    assert!(
        matches!(err, wasmkeep_storage::wal::RestoreError::TooLarge(_)),
        "{err}"
    );
    assert!(std::fs::metadata(&path).unwrap().len() < 1 << 20);

    // The size cap applies to honest files too.
    let path = dir.path().join("small-cap.db");
    let mut r = wasmkeep_storage::wal::Restorer::create(&path, 4096).unwrap();
    let err = r.apply(&shipped.files[0]).unwrap_err();
    assert!(
        matches!(err, wasmkeep_storage::wal::RestoreError::TooLarge(4096)),
        "{err}"
    );
}

#[test]
fn a_file_without_checksums_is_refused() {
    // The no-checksum flag (bytes 4..8) would skip every chain check; our
    // encoder never sets it, so a file carrying it is not ours.
    let dir = tempfile::tempdir().unwrap();
    let shipped = small_chain(&dir);
    let mut forged = shipped.files[1].clone();
    forged[4..8].copy_from_slice(&ltx::HEADER_FLAG_NO_CHECKSUM.to_be_bytes());
    // ... and no pre-apply checksum, as such a file would have (bytes 40..48).
    forged[40..48].copy_from_slice(&0u64.to_be_bytes());
    let path = dir.path().join("r.db");
    let mut r = wasmkeep_storage::wal::Restorer::create(&path, 1 << 30).unwrap();
    r.apply(&shipped.files[0]).unwrap();
    let err = r.apply(&forged).unwrap_err();
    assert!(err.to_string().contains("without checksums"), "{err}");
}

#[test]
fn a_chain_that_shrinks_the_database_restores_exactly() {
    // A snapshot of 3 pages, then a segment that rewrites page 1 and cuts
    // the database to 2 pages: the streaming restore must take page 3 out
    // of the checksum and the file.
    let ps = 4096u32;
    let page = |b: u8| vec![b; ps as usize];
    let header = |commit, min, max, pre| ltx::Header {
        page_size: ps,
        commit,
        min_txid: min,
        max_txid: max,
        pre_apply_checksum: pre,
        ..ltx::Header::default()
    };
    let mut sum = DbChecksum::default();
    let mut snap = ltx::Encoder::new(header(3, 1, 1, 0)).unwrap();
    for (p, b) in [(1, 1u8), (2, 2), (3, 3)] {
        snap.add_page(p, &page(b)).unwrap();
        sum.toggle(p, &page(b), ps);
    }
    let snap = snap.finish(sum).unwrap();
    let pre = sum.value();
    let mut after = DbChecksum::default();
    after.toggle(1, &page(9), ps);
    after.toggle(2, &page(2), ps);
    let mut seg = ltx::Encoder::new(header(2, 2, 2, pre)).unwrap();
    seg.add_page(1, &page(9)).unwrap();
    let seg = seg.finish(after).unwrap();

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("r.db");
    let mut r = wasmkeep_storage::wal::Restorer::create(&path, 1 << 30).unwrap();
    r.apply(&snap).unwrap();
    r.apply(&seg).unwrap();
    assert_eq!(r.finish().unwrap(), after);
    let bytes = std::fs::read(&path).unwrap();
    assert_eq!(bytes.len(), 2 * ps as usize);
    assert!(bytes[..ps as usize].iter().all(|&b| b == 9));
    assert!(bytes[ps as usize..].iter().all(|&b| b == 2));

    // A failed apply poisons the restorer: nothing more is applied.
    let path = dir.path().join("p.db");
    let mut r = wasmkeep_storage::wal::Restorer::create(&path, 1 << 30).unwrap();
    assert!(r.apply(&seg).is_err(), "a segment with nothing before it");
    assert!(r.apply(&snap).is_err(), "applied after a failure");
}

/// A storage of tenant `t`, its databases at `<data_dir>/actors/t/` (where
/// the server's state root puts them; this crate does not know it).
fn rooted(config: StorageConfig) -> Storage {
    let state = StateDir {
        dir: "actors/t".into(),
        scratch: ".deleting".into(),
        tenant: "t".into(),
    };
    Storage::new(config, state)
}
