//! Objects with the host's reserved `_wasmkeep_` prefix that a guest
//! could make before 0.8 (views, indexes, triggers, and tables renamed onto
//! the prefix), or that name a host table (a foreign key to one), found in
//! a database at open.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code

use wasmkeep_storage::{
    ReservedObject, ReservedUse, SqlErrorKind, SqlValue as V, StateDir, Storage, StorageConfig,
};

fn storage(dir: &tempfile::TempDir) -> Storage {
    let state = StateDir {
        dir: "actors/t".into(),
        scratch: ".deleting".into(),
        tenant: "t".into(),
    };
    Storage::new(StorageConfig::new(dir.path()), state)
}

/// A database a guest shaped before 0.8, written with a raw connection:
/// at open it is found, warned about and left as it is, nothing of it
/// dropped; the guest keeps its own tables and can change none of the
/// reserved objects; the host's tables work.
#[test]
fn reserved_objects_from_before_0_8_are_found_kept_and_closed_to_ddl() {
    let dir = tempfile::tempdir().unwrap();
    let store = storage(&dir);
    {
        let mut db = store.open("c", "a").unwrap();
        assert!(db.reserved_objects().is_empty());
        db.set_meta("k", "v").unwrap();
    }
    let path = dir.path().join("actors/t/c/a.db");
    let raw = rusqlite::Connection::open(&path).unwrap();
    raw.execute_batch(
        "CREATE TABLE t (x);
         CREATE TABLE log (x);
         CREATE TABLE kept (x);
         INSERT INTO kept VALUES (1), (2), (3);
         ALTER TABLE kept RENAME TO _wasmkeep_kept;
         CREATE VIEW _wasmkeep_v AS SELECT x FROM t;
         CREATE INDEX _WASMKEEP_I ON t (x);
         CREATE TRIGGER _wasmkeep_tr AFTER INSERT ON t BEGIN INSERT INTO log VALUES (new.x); END;
         CREATE TABLE fk (k REFERENCES _wasmkeep_meta (key));",
    )
    .unwrap();
    let before = raw
        .query_row("SELECT count(*) FROM sqlite_schema", [], |r| {
            r.get::<_, i64>(0)
        })
        .unwrap();

    let mut db = store.open("c", "a").unwrap();
    let mut found: Vec<ReservedObject> = db.reserved_objects().to_vec();
    found.sort_by(|a, b| a.name.cmp(&b.name));
    let named = |kind: &str, name: &str| ReservedObject {
        kind: kind.into(),
        name: name.into(),
        uses: ReservedUse::Named,
    };
    assert_eq!(
        found,
        vec![
            named("index", "_WASMKEEP_I"),
            named("table", "_wasmkeep_kept"),
            named("trigger", "_wasmkeep_tr"),
            named("view", "_wasmkeep_v"),
            ReservedObject {
                kind: "table".into(),
                name: "fk".into(),
                uses: ReservedUse::References,
            },
        ]
    );

    // Closed to the guest's schema changes.
    for sql in [
        "DROP VIEW _wasmkeep_v",
        "DROP INDEX _WASMKEEP_I",
        "DROP TRIGGER _wasmkeep_tr",
        "DROP TABLE _wasmkeep_kept",
        "ALTER TABLE _wasmkeep_kept RENAME TO kept",
        "ALTER TABLE _wasmkeep_kept ADD COLUMN y",
        "REINDEX _WASMKEEP_I",
        "CREATE VIEW again AS SELECT * FROM _wasmkeep_kept",
    ] {
        let err = db.exec(sql, &[]).unwrap_err();
        assert_eq!(err.kind, SqlErrorKind::NotAllowed, "{sql}: {err:?}");
    }
    // The renamed table's rows stay, but the guest cannot reach them, nor
    // read through the view (SQLite names the view to the authorizer).
    for sql in [
        "SELECT count(*) FROM _wasmkeep_kept",
        "SELECT x FROM _wasmkeep_v",
    ] {
        let err = db.query(sql, &[]).unwrap_err();
        assert_eq!(err.kind, SqlErrorKind::NotAllowed, "{sql}: {err:?}");
    }
    // Its own tables work, the old trigger with them.
    db.exec("INSERT INTO t VALUES (7)", &[]).unwrap();
    let rows = db.query("SELECT x FROM log", &[]).unwrap();
    assert_eq!(rows.values, vec![vec![V::Integer(7)]]);
    // And so do the host's.
    assert_eq!(db.meta("k").unwrap().as_deref(), Some("v"));
    db.set_alarm(1_000).unwrap();
    assert_eq!(db.alarm().unwrap(), Some(1_000));
    db.end_event().unwrap();
    drop(db);

    // Nothing was dropped or changed: the same schema, the same rows.
    let after = raw
        .query_row("SELECT count(*) FROM sqlite_schema", [], |r| {
            r.get::<_, i64>(0)
        })
        .unwrap();
    assert_eq!(after, before);
    let kept: i64 = raw
        .query_row("SELECT sum(x) FROM _wasmkeep_kept", [], |r| r.get(0))
        .unwrap();
    assert_eq!(kept, 6);
}

/// A guest table that carries a trigger with a reserved name cannot be
/// dropped (SQLite asks to drop the trigger); one that carries only an
/// index with a reserved name can (SQLite drops the index with it, without
/// asking). Either way the rows stay the guest's to read and write.
#[test]
fn a_guest_table_with_a_reserved_trigger_is_kept() {
    let dir = tempfile::tempdir().unwrap();
    let store = storage(&dir);
    drop(store.open("c", "a").unwrap());
    let raw = rusqlite::Connection::open(dir.path().join("actors/t/c/a.db")).unwrap();
    raw.execute_batch(
        "CREATE TABLE t (x);
         CREATE TABLE u (x);
         CREATE INDEX _wasmkeep_i ON t (x);
         CREATE TRIGGER _wasmkeep_tr AFTER INSERT ON u BEGIN SELECT 1; END;",
    )
    .unwrap();
    let mut db = store.open("c", "a").unwrap();
    assert_eq!(db.reserved_objects().len(), 2);
    db.exec("INSERT INTO t VALUES (1)", &[]).unwrap();
    db.exec("INSERT INTO u VALUES (1)", &[]).unwrap();
    db.exec("DELETE FROM u", &[]).unwrap();
    let err = db.exec("DROP TABLE u", &[]).unwrap_err();
    assert_eq!(err.kind, SqlErrorKind::NotAllowed, "{err:?}");
    db.exec("DROP TABLE t", &[]).unwrap();
    drop(db);
    let db = store.open("c", "a").unwrap();
    assert_eq!(
        db.reserved_objects(),
        [ReservedObject {
            kind: "trigger".into(),
            name: "_wasmkeep_tr".into(),
            uses: ReservedUse::Named,
        }]
    );
}

/// At most 64 are kept, however many the schema holds.
#[test]
fn at_most_64_reserved_objects_are_kept() {
    let dir = tempfile::tempdir().unwrap();
    let store = storage(&dir);
    drop(store.open("c", "a").unwrap());
    let raw = rusqlite::Connection::open(dir.path().join("actors/t/c/a.db")).unwrap();
    let views: String = (0..100)
        .map(|i| format!("CREATE VIEW _wasmkeep_v{i} AS SELECT {i};"))
        .collect();
    raw.execute_batch(&views).unwrap();
    let db = store.open("c", "a").unwrap();
    assert_eq!(db.reserved_objects().len(), 64);
}
