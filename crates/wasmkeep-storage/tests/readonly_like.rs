//! The offline connections (`readonly::verify`, `readonly::query`) run the
//! host's `LIKE` and `GLOB`, as guest connections do: an index a guest
//! built on a deep pattern is recomputed by `integrity_check` without
//! SQLite's recursion, and answers as on the guest's connection.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code

use wasmkeep_storage::{SqlValue as V, StateDir, Storage, StorageConfig, readonly};

/// A database the host made, then shaped with a raw connection, its WAL
/// folded into the main file (the offline connections read it immutable).
fn database(dir: &tempfile::TempDir, ddl: &str) -> std::path::PathBuf {
    let state = StateDir {
        dir: "actors/t".into(),
        scratch: ".deleting".into(),
        tenant: "t".into(),
    };
    drop(
        Storage::new(StorageConfig::new(dir.path()), state)
            .open("c", "a")
            .unwrap(),
    );
    let path = dir.path().join("actors/t/c/a.db");
    // SQLite's own LIKE builds the index here: a large stack for its
    // recursion.
    let (p, ddl) = (path.clone(), ddl.to_owned());
    std::thread::Builder::new()
        .stack_size(256 << 20)
        .spawn(move || {
            let raw = rusqlite::Connection::open(&p).unwrap();
            raw.execute_batch(&ddl).unwrap();
            raw.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
                .unwrap();
        })
        .unwrap()
        .join()
        .unwrap();
    path
}

/// Runs `f` on a thread with a 2 MiB stack, a blocking pool's default.
fn on_small_stack<T: Send + 'static>(f: impl FnOnce() -> T + Send + 'static) -> T {
    std::thread::Builder::new()
        .stack_size(2 << 20)
        .spawn(f)
        .unwrap()
        .join()
        .unwrap()
}

#[test]
fn verify_recomputes_a_deep_like_index_without_recursion() {
    let pattern = "%a".repeat(25_000);
    let value = format!("{}b", "a".repeat(30_000));
    for index in [
        format!("CREATE INDEX deep ON t ((x LIKE '{pattern}'))"),
        format!("CREATE INDEX deep ON t (x) WHERE x LIKE '{pattern}'"),
        format!(
            "CREATE INDEX deep ON t ((x GLOB '{}'))",
            "*a".repeat(25_000)
        ),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let ddl = format!(
            "CREATE TABLE t (x); INSERT INTO t VALUES ('{value}'), ('{}'); {index};",
            "a".repeat(30_000)
        );
        let path = database(&dir, &ddl);
        let p = path.clone();
        let verified = on_small_stack(move || readonly::verify(&p).map(|v| v.tables));
        assert_eq!(verified.unwrap(), ["t"], "{index}");
        // And the answers are SQLite's: no match for the value ending in
        // `b`, a match for the one of `a` alone.
        let p = path.clone();
        let sql = format!("SELECT x LIKE '{pattern}' FROM t ORDER BY length(x)");
        let rows = on_small_stack(move || readonly::query(&p, &sql)).unwrap();
        assert_eq!(rows.values, vec![vec![V::Integer(1)], vec![V::Integer(0)]]);
    }
}

#[test]
fn verify_accepts_a_guest_object_with_the_reserved_prefix_but_not_a_host_name() {
    let dir = tempfile::tempdir().unwrap();
    let path = database(
        &dir,
        "CREATE TABLE t (x); CREATE VIEW _wasmkeep_v AS SELECT x FROM t;
         CREATE INDEX _wasmkeep_i ON t (x);
         CREATE TRIGGER _wasmkeep_tr AFTER INSERT ON t BEGIN SELECT 1; END;",
    );
    assert_eq!(readonly::verify(&path).unwrap().tables, ["t"]);
    readonly::backup_address(&path).unwrap();
    // A host table's name held by something else still fails.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("x.db");
    let raw = rusqlite::Connection::open(&path).unwrap();
    raw.execute_batch(
        "CREATE TABLE t (x); CREATE VIEW _wasmkeep_meta AS SELECT 'k' AS key, 'v' AS value;",
    )
    .unwrap();
    drop(raw);
    readonly::verify(&path).unwrap_err();
}
