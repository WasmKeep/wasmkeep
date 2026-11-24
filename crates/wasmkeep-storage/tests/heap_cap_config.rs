//! The process-wide SQLite heap cap comes from the storage settings. A test
//! of its own binary: the cap is the process's.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code

use wasmkeep_storage::{DEFAULT_SQLITE_HEAP_BYTES, StateDir, Storage, StorageConfig};

fn cap() -> i64 {
    rusqlite::Connection::open_in_memory()
        .unwrap()
        .query_row("PRAGMA hard_heap_limit", [], |r| r.get(0))
        .unwrap()
}

#[test]
fn the_heap_cap_is_the_configured_one_and_the_lowest_set_wins() {
    assert_eq!(DEFAULT_SQLITE_HEAP_BYTES, 4 << 30);
    assert_eq!(
        StorageConfig::new("d").sqlite_heap_bytes,
        DEFAULT_SQLITE_HEAP_BYTES
    );
    let dir = tempfile::tempdir().unwrap();
    let open = |bytes: u64, name: &str| {
        let state = StateDir {
            dir: "actors/t".into(),
            scratch: ".deleting".into(),
            tenant: "t".into(),
        };
        let config = StorageConfig {
            sqlite_heap_bytes: bytes,
            ..StorageConfig::new(dir.path())
        };
        Storage::new(config, state).open("c", name).unwrap()
    };
    // Above the default: the first open sets it.
    let a = open(6 << 30, "a");
    assert_eq!(cap(), 6 << 30);
    // Lower: lowered.
    let b = open(1 << 30, "b");
    assert_eq!(cap(), 1 << 30);
    // Higher again: an open never raises it.
    let c = open(8 << 30, "c");
    assert_eq!(cap(), 1 << 30);
    drop((a, b, c));
}
