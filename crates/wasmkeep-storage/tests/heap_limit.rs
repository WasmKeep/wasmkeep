//! SQLite's heap is capped for the whole process. A test of its own binary:
//! it lowers the process-wide cap, which no other test may see.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code

use wasmkeep_storage::{SqlErrorKind, SqlValue as V, StateDir, Storage, StorageConfig};

#[test]
fn a_statement_needing_more_than_the_heap_cap_fails_cleanly() {
    let dir = tempfile::tempdir().unwrap();
    let state = StateDir {
        dir: "actors/t".into(),
        scratch: ".deleting".into(),
        tenant: "t".into(),
    };
    let mut db = Storage::new(StorageConfig::new(dir.path()), state)
        .open("c", "a")
        .unwrap();
    // Opening a database sets the cap; the pragma reads it back.
    let probe = rusqlite::Connection::open_in_memory().unwrap();
    let cap = |c: &rusqlite::Connection| -> i64 {
        c.query_row("PRAGMA hard_heap_limit", [], |r| r.get(0))
            .unwrap()
    };
    assert_eq!(cap(&probe), 4 << 30);
    // Lower it to 48 MiB for this process (the pragma only lowers it).
    probe
        .execute_batch(&format!("PRAGMA hard_heap_limit = {}", 48 << 20))
        .unwrap();
    assert_eq!(cap(&probe), 48 << 20);

    // Four 12 MB values held at once need more than that.
    let err = db
        .query(
            "SELECT printf('%.*c', 12000000, 'a'), printf('%.*c', 12000000, 'b'), \
             printf('%.*c', 12000000, 'c'), printf('%.*c', 12000000, 'd')",
            &[],
        )
        .unwrap_err();
    assert_eq!(err.kind, SqlErrorKind::QuotaExceeded, "{err:?}");
    assert!(err.message.contains("memory"), "{err:?}");
    // The connection and the process are fine; smaller statements run.
    let ok = db
        .query("SELECT length(printf('%.*c', 4000000, 'a'))", &[])
        .unwrap();
    assert_eq!(ok.values, vec![vec![V::Integer(4_000_000)]]);
    db.exec("CREATE TABLE t (x)", &[]).unwrap();
    db.set_meta("k", "v").unwrap();
}
