//! Per-actor SQLite: round trips, persistence, error kinds, sandbox, quota.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code

use wasmkeep_storage::{SqlErrorKind, SqlValue as V, StateDir, Storage, StorageConfig};

fn storage(dir: &tempfile::TempDir) -> Storage {
    rooted(StorageConfig::new(dir.path()))
}

#[test]
fn values_round_trip_through_params_and_rows() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = storage(&dir).open("c", "a").unwrap();
    db.exec("CREATE TABLE t (i INTEGER, r REAL, s TEXT, b BLOB, n)", &[])
        .unwrap();
    let row = [
        V::Integer(-7),
        V::Real(1.5),
        V::Text("héllo".into()),
        V::Blob(vec![0, 255]),
        V::Null,
    ];
    assert_eq!(
        db.exec("INSERT INTO t VALUES (?1, ?2, ?3, ?4, ?5)", &row)
            .unwrap(),
        1
    );
    let rows = db.query("SELECT i, r, s, b, n FROM t", &[]).unwrap();
    assert_eq!(rows.columns, ["i", "r", "s", "b", "n"]);
    assert_eq!(rows.values, vec![row.to_vec()]);
}

#[test]
fn data_survives_closing_and_reopening() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut db = storage(&dir).open("c", "a").unwrap();
        db.exec("CREATE TABLE kv (k TEXT PRIMARY KEY, v INTEGER)", &[])
            .unwrap();
        db.exec("INSERT INTO kv VALUES ('x', 41)", &[]).unwrap();
        db.exec("UPDATE kv SET v = v + 1", &[]).unwrap();
    } // dropped, as on a process restart
    let mut db = storage(&dir).open("c", "a").unwrap();
    let rows = db.query("SELECT v FROM kv WHERE k = 'x'", &[]).unwrap();
    assert_eq!(rows.values, vec![vec![V::Integer(42)]]);
}

#[test]
fn each_address_has_its_own_database() {
    let dir = tempfile::tempdir().unwrap();
    let s = storage(&dir);
    let mut a = s.open("c", "Room").unwrap();
    let mut b = s.open("c", "room").unwrap();
    a.exec("CREATE TABLE only_in_a (x)", &[]).unwrap();
    let err = b.query("SELECT * FROM only_in_a", &[]).unwrap_err();
    assert_eq!(err.kind, SqlErrorKind::Syntax);
    assert_ne!(a.path(), b.path());
    assert!(
        a.path()
            .starts_with(dir.path().join("actors").join("t").join("c"))
    );
}

#[test]
fn statement_errors_have_the_right_kind() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = storage(&dir).open("c", "a").unwrap();
    db.exec(
        "CREATE TABLE u (id INTEGER PRIMARY KEY, v TEXT NOT NULL UNIQUE)",
        &[],
    )
    .unwrap();
    db.exec("INSERT INTO u (v) VALUES ('a')", &[]).unwrap();

    let kind = |r: Result<u64, wasmkeep_storage::SqlError>| r.unwrap_err().kind;
    assert_eq!(kind(db.exec("SELEC 1", &[])), SqlErrorKind::Syntax);
    assert_eq!(
        kind(db.exec("INSERT INTO missing VALUES (1)", &[])),
        SqlErrorKind::Syntax
    );
    assert_eq!(
        kind(db.exec("INSERT INTO u (v) VALUES ('a')", &[])),
        SqlErrorKind::Constraint
    );
    assert_eq!(
        kind(db.exec("INSERT INTO u (v) VALUES (NULL)", &[])),
        SqlErrorKind::Constraint
    );
    assert_eq!(
        kind(db.exec("INSERT INTO u (v) VALUES (?1, ?2)", &[V::Null])),
        SqlErrorKind::Syntax
    );
    assert_eq!(
        kind(db.exec("INSERT INTO u (v) VALUES (?1)", &[])),
        SqlErrorKind::Bind
    );
    assert_eq!(
        kind(db.exec("INSERT INTO u (v) VALUES ('b'); DROP TABLE u", &[])),
        SqlErrorKind::MultipleStatements
    );
    // exec on a statement that returns rows points the guest at query.
    let err = db.exec("SELECT 1", &[]).unwrap_err();
    assert_eq!(err.kind, SqlErrorKind::Syntax);
    assert!(err.message.contains("query"), "{}", err.message);
    // The rejected multi-statement call did not run its first half.
    let rows = db.query("SELECT count(*) FROM u", &[]).unwrap();
    assert_eq!(rows.values, vec![vec![V::Integer(1)]]);
}

#[test]
fn sandbox_blocks_attach_pragmas_and_file_escapes() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = storage(&dir).open("c", "a").unwrap();
    let outside = dir.path().join("escape.db");
    let outside_sql = outside.display().to_string().replace('\'', "''");

    let denied = [
        format!("ATTACH DATABASE '{outside_sql}' AS x"),
        format!("VACUUM INTO '{outside_sql}'"),
        "PRAGMA journal_mode = DELETE".to_owned(),
        "PRAGMA synchronous = OFF".to_owned(),
        "PRAGMA writable_schema = ON".to_owned(),
        "PRAGMA max_page_count = 999999999".to_owned(),
        "PRAGMA hard_heap_limit = 0".to_owned(),
        "PRAGMA soft_heap_limit = 1".to_owned(),
        "PRAGMA temp_store = FILE".to_owned(),
        "SELECT * FROM pragma_hard_heap_limit".to_owned(),
        "SELECT load_extension('x')".to_owned(),
    ];
    for sql in &denied {
        let err = db.query(sql, &[]).unwrap_err();
        assert!(
            matches!(err.kind, SqlErrorKind::NotAllowed | SqlErrorKind::Syntax),
            "{sql}: {err:?}"
        );
    }
    assert!(
        !outside.exists(),
        "a statement wrote outside the actor database"
    );

    // Allowed: migrations and introspection.
    db.exec("CREATE TABLE t (x)", &[]).unwrap();
    db.query("PRAGMA user_version = 3", &[]).unwrap();
    let v = db.query("PRAGMA user_version", &[]).unwrap();
    assert_eq!(v.values, vec![vec![V::Integer(3)]]);
    assert_eq!(
        db.query("PRAGMA table_info(t)", &[]).unwrap().values.len(),
        1
    );
}

#[test]
fn quota_stops_writes_but_not_reads() {
    let dir = tempfile::tempdir().unwrap();
    let s = rooted(StorageConfig {
        max_db_bytes: 256 * 1024,
        ..StorageConfig::new(dir.path())
    });
    let mut db = s.open("c", "a").unwrap();
    db.exec("CREATE TABLE big (b BLOB)", &[]).unwrap();
    let blob = V::Blob(vec![7; 16 * 1024]);
    let mut inserted = 0;
    let err = loop {
        match db.exec("INSERT INTO big VALUES (?1)", std::slice::from_ref(&blob)) {
            Ok(_) => inserted += 1,
            Err(e) => break e,
        }
        assert!(inserted < 100, "quota never hit");
    };
    assert_eq!(err.kind, SqlErrorKind::QuotaExceeded, "{err:?}");
    let rows = db.query("SELECT count(*) FROM big", &[]).unwrap();
    assert_eq!(rows.values, vec![vec![V::Integer(inserted)]]);
}

#[test]
fn an_open_transaction_is_rolled_back_at_the_end_of_the_event() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = storage(&dir).open("c", "a").unwrap();
    db.exec("CREATE TABLE t (x)", &[]).unwrap();

    db.exec("BEGIN", &[]).unwrap();
    db.exec("INSERT INTO t VALUES (1)", &[]).unwrap();
    db.exec("COMMIT", &[]).unwrap();
    assert!(!db.end_event().unwrap());

    db.exec("BEGIN", &[]).unwrap();
    db.exec("INSERT INTO t VALUES (2)", &[]).unwrap();
    assert!(db.end_event().unwrap(), "expected a rollback");

    let rows = db.query("SELECT x FROM t", &[]).unwrap();
    assert_eq!(rows.values, vec![vec![V::Integer(1)]]);
}

/// `exec-durable` never opens or controls a transaction: `BEGIN` and
/// `SAVEPOINT` are refused before they run (they used to run, then report
/// an error for a statement that had succeeded, with the connection left
/// at `synchronous = FULL`). A durable write still works afterwards.
#[test]
fn exec_durable_refuses_transaction_control() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = storage(&dir).open("c", "a").unwrap();
    db.exec("CREATE TABLE t (x)", &[]).unwrap();
    for sql in [
        "BEGIN",
        "BEGIN IMMEDIATE",
        "SAVEPOINT s",
        "RELEASE s",
        "ROLLBACK",
        "COMMIT",
        "END",
    ] {
        let err = db.exec_durable(sql, &[]).unwrap_err();
        assert_eq!(err.kind, SqlErrorKind::NotAllowed, "{sql}: {err:?}");
        assert!(!db.in_transaction(), "{sql} opened a transaction");
    }
    assert_eq!(db.exec_durable("INSERT INTO t VALUES (1)", &[]).unwrap(), 1);
    assert!(!db.end_event().unwrap());
}

/// A write interrupted inside a transaction (its deadline passed) makes
/// SQLite roll the transaction back: the event's later statements are
/// refused with a clear `not-allowed` instead of each committing alone,
/// until the event ends; the next event starts clean.
#[test]
fn an_interrupted_write_aborts_the_events_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = StorageConfig::new(dir.path());
    config.max_statement_time = std::time::Duration::from_millis(300);
    let mut db = rooted(config).open("c", "a").unwrap();
    db.exec("CREATE TABLE t (x)", &[]).unwrap();
    db.exec("BEGIN", &[]).unwrap();
    db.exec("INSERT INTO t VALUES (1)", &[]).unwrap();
    let err = db
        .exec(
            "INSERT INTO t WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c) \
             SELECT x FROM c",
            &[],
        )
        .unwrap_err();
    assert_eq!(err.kind, SqlErrorKind::QuotaExceeded, "{err:?}");
    for sql in ["INSERT INTO t VALUES (2)", "COMMIT"] {
        let err = db.exec(sql, &[]).unwrap_err();
        assert_eq!(err.kind, SqlErrorKind::NotAllowed, "{sql}: {err:?}");
        assert!(err.message.contains("rolled back"), "{sql}: {err:?}");
    }
    // Through exec-durable too, in or out of a transaction.
    for sql in ["COMMIT", "INSERT INTO t VALUES (2)"] {
        let err = db.exec_durable(sql, &[]).unwrap_err();
        assert_eq!(err.kind, SqlErrorKind::NotAllowed, "{sql}: {err:?}");
    }
    // And `storage.flush` (ADR 0034): the guest believes its transaction
    // open, so nothing is made durable under it.
    for _ in 0..2 {
        let err = db.sync_committed().unwrap_err();
        assert_eq!(err.kind, SqlErrorKind::NotAllowed, "{err:?}");
        assert!(err.message.contains("rolled back"), "{err:?}");
        assert!(db.flushable().is_err());
    }
    // The guest's own ROLLBACK (an error handler's) is what happened: Ok.
    assert_eq!(db.exec("ROLLBACK", &[]).unwrap(), 0);
    let err = db.query("SELECT count(*) FROM t", &[]).unwrap_err();
    assert_eq!(err.kind, SqlErrorKind::NotAllowed, "{err:?}");
    // The event ends; the next one starts clean, with nothing of the
    // rolled-back transaction kept.
    db.end_event().unwrap();
    assert_eq!(
        db.query("SELECT count(*) FROM t", &[]).unwrap().values,
        vec![vec![V::Integer(0)]]
    );
    assert_eq!(db.exec("INSERT INTO t VALUES (3)", &[]).unwrap(), 1);
}

#[test]
fn durable_writes_commit_and_persist() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut db = storage(&dir).open("c", "a").unwrap();
        db.exec("CREATE TABLE t (x)", &[]).unwrap();
        assert_eq!(db.exec_durable("INSERT INTO t VALUES (1)", &[]).unwrap(), 1);
        // Still sandboxed after the internal pragma toggle.
        assert_eq!(
            db.query("PRAGMA synchronous = OFF", &[]).unwrap_err().kind,
            SqlErrorKind::NotAllowed
        );
    }
    let mut db = storage(&dir).open("c", "a").unwrap();
    assert_eq!(db.query("SELECT x FROM t", &[]).unwrap().values.len(), 1);
}

#[test]
fn a_transaction_can_end_in_a_durable_commit() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut db = storage(&dir).open("c", "a").unwrap();
        db.exec("CREATE TABLE t (x)", &[]).unwrap();
        db.exec("BEGIN", &[]).unwrap();
        db.exec("INSERT INTO t VALUES (1)", &[]).unwrap();
        db.exec("INSERT INTO t VALUES (2)", &[]).unwrap();
        db.exec_durable("COMMIT", &[]).unwrap();
        assert!(!db.in_transaction());
        assert!(!db.end_event().unwrap(), "nothing left to roll back");
        // `END` is COMMIT too.
        db.exec("BEGIN", &[]).unwrap();
        db.exec("INSERT INTO t VALUES (3)", &[]).unwrap();
        db.exec_durable("END TRANSACTION", &[]).unwrap();
        assert_eq!(
            db.query("PRAGMA synchronous = OFF", &[]).unwrap_err().kind,
            SqlErrorKind::NotAllowed
        );
    }
    let mut db = storage(&dir).open("c", "a").unwrap();
    assert_eq!(db.query("SELECT x FROM t", &[]).unwrap().values.len(), 3);
}

#[cfg(unix)]
#[test]
fn a_durable_commit_fsyncs_the_wal() {
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir().unwrap();
    let mut db = storage(&dir).open("c", "a").unwrap();
    db.exec("CREATE TABLE t (x)", &[]).unwrap();
    db.exec("BEGIN", &[]).unwrap();
    db.exec("INSERT INTO t VALUES (1)", &[]).unwrap();
    // SQLite keeps its own handle; the fsync after the commit opens the
    // WAL anew, so it fails, and must say so rather than claim durability.
    let mut wal = db.path().as_os_str().to_owned();
    wal.push("-wal");
    let perms = |mode| std::fs::set_permissions(&wal, std::fs::Permissions::from_mode(mode));
    perms(0o000).unwrap();
    let r = db.exec_durable("COMMIT", &[]);
    perms(0o644).unwrap();
    let e = r.unwrap_err();
    assert_eq!(e.kind, SqlErrorKind::Internal, "{e:?}");
    assert!(e.message.contains("not fsynced"), "{e:?}");
    assert!(
        !db.in_transaction(),
        "committed, only not confirmed on disk"
    );
}

/// `storage.flush` on a single node (ADR 0034, section 4): plain commits
/// fsynced without a statement; refused, changing nothing, inside a
/// transaction; a failed fsync says so.
#[cfg(unix)]
#[test]
fn a_flush_fsyncs_plain_commits_and_not_inside_a_transaction() {
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir().unwrap();
    {
        let mut db = storage(&dir).open("c", "a").unwrap();
        db.exec("CREATE TABLE t (x)", &[]).unwrap();
        db.exec("INSERT INTO t VALUES (1)", &[]).unwrap();
        db.sync_committed().unwrap();
        db.exec("BEGIN", &[]).unwrap();
        db.exec("INSERT INTO t VALUES (2)", &[]).unwrap();
        let e = db.sync_committed().unwrap_err();
        assert_eq!(e.kind, SqlErrorKind::NotAllowed, "{e:?}");
        assert!(db.in_transaction(), "the transaction is still open");
        db.exec("ROLLBACK", &[]).unwrap();
        let mut wal = db.path().as_os_str().to_owned();
        wal.push("-wal");
        let perms = |mode| std::fs::set_permissions(&wal, std::fs::Permissions::from_mode(mode));
        perms(0o000).unwrap();
        let r = db.sync_committed();
        perms(0o644).unwrap();
        let e = r.unwrap_err();
        assert_eq!(e.kind, SqlErrorKind::Internal, "{e:?}");
        assert!(e.message.contains("not fsynced"), "{e:?}");
    }
    let mut db = storage(&dir).open("c", "a").unwrap();
    assert_eq!(db.query("SELECT x FROM t", &[]).unwrap().values.len(), 1);
}

#[test]
fn inside_a_transaction_only_a_commit_can_be_durable() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = storage(&dir).open("c", "a").unwrap();
    db.exec("CREATE TABLE t (x)", &[]).unwrap();
    db.exec("BEGIN", &[]).unwrap();
    // Refused before it runs: nothing in a transaction is durable before
    // its COMMIT.
    let e = db
        .exec_durable("INSERT INTO t VALUES (1)", &[])
        .unwrap_err();
    assert_eq!(e.kind, SqlErrorKind::NotAllowed, "{e:?}");
    assert!(db.in_transaction(), "the transaction is still open");
    let e = db.exec_durable("SAVEPOINT s", &[]).unwrap_err();
    assert_eq!(e.kind, SqlErrorKind::NotAllowed, "{e:?}");
    db.exec("COMMIT", &[]).unwrap();
    assert!(db.query("SELECT x FROM t", &[]).unwrap().values.is_empty());
}

#[test]
fn delete_removes_the_database() {
    let dir = tempfile::tempdir().unwrap();
    let s = storage(&dir);
    let path = {
        let mut db = s.open("c", "a").unwrap();
        db.exec("CREATE TABLE t (x)", &[]).unwrap();
        db.path().to_owned()
    };
    assert!(path.exists());
    s.delete("c", "a").unwrap();
    assert!(!path.exists());
    s.delete("c", "a").unwrap(); // idempotent
    let mut db = s.open("c", "a").unwrap();
    assert_eq!(
        db.query("SELECT * FROM t", &[]).unwrap_err().kind,
        SqlErrorKind::Syntax
    );
}

#[test]
fn a_durable_delete_removes_the_wal_and_the_database() {
    let dir = tempfile::tempdir().unwrap();
    let s = storage(&dir);
    let path = {
        let mut db = s.open("c", "a").unwrap();
        db.exec("CREATE TABLE t (x)", &[]).unwrap();
        db.exec("INSERT INTO t VALUES (1)", &[]).unwrap();
        db.path().to_owned()
    };
    s.delete_durably("c", "a").unwrap();
    for suffix in ["", "-wal", "-shm"] {
        assert!(!std::path::PathBuf::from(format!("{}{suffix}", path.display())).exists());
    }
    s.delete_durably("c", "a").unwrap(); // idempotent
    s.delete_durably("never", "made").unwrap(); // no directory
    let mut db = s.open("c", "a").unwrap();
    assert_eq!(
        db.query("SELECT * FROM t", &[]).unwrap_err().kind,
        SqlErrorKind::Syntax
    );
}

#[test]
fn host_tables_are_hidden_from_guests() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = storage(&dir).open("c", "a").unwrap();
    db.set_alarm(1_000).unwrap();
    db.set_meta("component", "abc").unwrap();

    let attempts = [
        "SELECT * FROM _wasmkeep_alarm",
        "SELECT * FROM _WASMKEEP_META",
        "UPDATE _wasmkeep_alarm SET at = 0",
        "DELETE FROM _wasmkeep_meta",
        "INSERT INTO _wasmkeep_meta VALUES ('k', 'v')",
        "DROP TABLE _wasmkeep_alarm",
        "ALTER TABLE _wasmkeep_meta RENAME TO x",
        "CREATE INDEX i ON _wasmkeep_meta (value)",
        "CREATE TABLE _wasmkeep_mine (x)",
    ];
    for sql in attempts {
        let err = db.query(sql, &[]).unwrap_err();
        assert_eq!(err.kind, SqlErrorKind::NotAllowed, "{sql}: {err:?}");
    }
    // Through a guest trigger, too.
    db.exec("CREATE TABLE t (x)", &[]).unwrap();
    let err = db
        .exec(
            "CREATE TRIGGER sneaky AFTER INSERT ON t BEGIN DELETE FROM _wasmkeep_alarm; END",
            &[],
        )
        .and_then(|_| db.exec("INSERT INTO t VALUES (1)", &[]))
        .unwrap_err();
    assert_eq!(err.kind, SqlErrorKind::NotAllowed, "{err:?}");

    assert_eq!(db.alarm().unwrap(), Some(1_000));
    assert_eq!(db.meta("component").unwrap().as_deref(), Some("abc"));
}

#[test]
fn alarms_and_meta_persist_and_follow_guest_transactions() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut db = storage(&dir).open("c", "a").unwrap();
        assert_eq!(db.alarm().unwrap(), None);
        db.set_alarm(5).unwrap();
        db.set_alarm(7).unwrap(); // replaces
        db.set_meta("component", "v1").unwrap();
        db.set_meta("component", "v2").unwrap();

        // Inside a guest transaction that is rolled back, the alarm change
        // is rolled back too.
        db.exec("BEGIN", &[]).unwrap();
        db.clear_alarm().unwrap();
        assert_eq!(db.alarm().unwrap(), None);
        assert!(db.end_event().unwrap());
        assert_eq!(db.alarm().unwrap(), Some(7));
    }
    let mut db = storage(&dir).open("c", "a").unwrap();
    assert_eq!(db.alarm().unwrap(), Some(7));
    assert_eq!(db.meta("component").unwrap().as_deref(), Some("v2"));
    db.clear_alarm().unwrap();
    assert_eq!(db.alarm().unwrap(), None);
}

#[test]
fn pending_alarms_are_found_by_scanning_the_data_dir() {
    let dir = tempfile::tempdir().unwrap();
    let s = storage(&dir);
    let long = "n".repeat(250); // hashed file name; address comes from meta
    s.open("c", "quiet").unwrap();
    s.open("c", "soon").unwrap().set_alarm(10).unwrap();
    s.open("other", &long).unwrap().set_alarm(20).unwrap();
    let mut cleared = s.open("c", "cleared").unwrap();
    cleared.set_alarm(30).unwrap();
    cleared.clear_alarm().unwrap();

    let scan = s.pending_alarms().unwrap();
    assert!(scan.skipped.is_empty(), "{:?}", scan.skipped);
    let mut found = scan.found;
    found.sort();
    assert_eq!(
        found,
        vec![
            ("t".into(), "c".into(), "soon".into(), 10),
            ("t".into(), "other".into(), long, 20),
        ]
    );
    assert!(
        rooted(StorageConfig::new(dir.path().join("empty")))
            .pending_alarms()
            .unwrap()
            .found
            .is_empty()
    );
}

/// The scan reads this root's databases only: another app's, in a folder
/// beside it, are its own node's to arm (ADR 0022).
#[test]
fn the_alarm_scan_reads_only_this_roots_databases() {
    let dir = tempfile::tempdir().unwrap();
    let s = storage(&dir);
    s.open("c", "mine").unwrap().set_alarm(10).unwrap();
    let other = Storage::new(
        StorageConfig::new(dir.path()),
        StateDir {
            dir: "actors/u".into(),
            scratch: ".deleting".into(),
            tenant: "u".into(),
        },
    );
    other.open("c", "theirs").unwrap().set_alarm(20).unwrap();
    assert_eq!(
        s.pending_alarms().unwrap().found,
        vec![("t".into(), "c".into(), "mine".into(), 10)]
    );
}

#[test]
fn one_unreadable_database_does_not_hide_the_other_alarms() {
    let dir = tempfile::tempdir().unwrap();
    let s = storage(&dir);
    s.open("c", "soon").unwrap().set_alarm(10).unwrap();
    // A file that is not a database, where a database would be.
    let bad = dir.path().join("actors/t/c/broken.db");
    std::fs::write(&bad, b"this is not a SQLite database, not even close").unwrap();
    let scan = s.pending_alarms().unwrap();
    assert_eq!(
        scan.found,
        vec![("t".into(), "c".into(), "soon".into(), 10)]
    );
    assert_eq!(scan.skipped.len(), 1, "{:?}", scan.skipped);
    // The error names the file and says why.
    let why = scan.skipped[0].to_string();
    assert!(why.contains("broken.db"), "{why}");
    assert!(why.contains(": "), "the cause is in the message: {why}");
}

// ---- Hardening from the security review. Each case is an
// attack a guest could make with nothing but `exec` and `query`.

#[test]
fn fts3_tokenizer_cannot_leak_or_install_a_function_pointer() {
    // With bound parameters SQLite allows fts3_tokenizer: one argument
    // returns a host pointer, two install one, and a virtual table using
    // it then calls the guest-chosen address (a segfault, or worse).
    let dir = tempfile::tempdir().unwrap();
    let mut db = storage(&dir).open("c", "a").unwrap();
    let leak = db.query("SELECT fts3_tokenizer(?1)", &[V::Text("simple".into())]);
    let leak = leak.unwrap_err();
    assert_eq!(leak.kind, SqlErrorKind::NotAllowed, "{leak:?}");
    let install = db.query(
        "SELECT fts3_tokenizer(?1, ?2)",
        &[V::Text("evil".into()), V::Blob(vec![0x41; 8])],
    );
    assert_eq!(install.unwrap_err().kind, SqlErrorKind::NotAllowed);
    // Built-in full-text search and r-trees still work.
    db.exec("CREATE VIRTUAL TABLE docs USING fts5(body)", &[])
        .unwrap();
    db.exec("INSERT INTO docs VALUES ('hello world')", &[])
        .unwrap();
    let hits = db
        .query("SELECT count(*) FROM docs WHERE docs MATCH 'hello'", &[])
        .unwrap();
    assert_eq!(hits.values, vec![vec![V::Integer(1)]]);
    db.exec("CREATE VIRTUAL TABLE old USING fts4(body)", &[])
        .unwrap();
    db.exec("CREATE VIRTUAL TABLE boxes USING rtree(id, x0, x1)", &[])
        .unwrap();
    // Other modules are refused.
    let err = db
        .exec("CREATE VIRTUAL TABLE s USING fts5vocab(docs, row)", &[])
        .unwrap_err();
    assert_eq!(err.kind, SqlErrorKind::NotAllowed, "{err:?}");
}

#[test]
fn a_temp_table_cannot_shadow_or_reach_host_tables() {
    // The review's sequence: a temp table renamed onto a host table's name
    // shadows it (temp is searched first), and its trigger then runs host
    // writes with the sandbox off. Temp objects are refused outright.
    let dir = tempfile::tempdir().unwrap();
    let mut db = storage(&dir).open("c", "a").unwrap();
    db.set_meta("deploy_seq", "3").unwrap();
    for sql in [
        "CREATE TEMP TABLE x(key TEXT PRIMARY KEY, value TEXT)",
        "CREATE TABLE temp.x(key TEXT PRIMARY KEY, value TEXT)",
        "CREATE TEMP VIEW v AS SELECT 1",
    ] {
        let err = db.exec(sql, &[]).unwrap_err();
        assert_eq!(err.kind, SqlErrorKind::NotAllowed, "{sql}: {err:?}");
    }
    db.exec("CREATE TABLE y(key TEXT PRIMARY KEY, value TEXT)", &[])
        .unwrap();
    let err = db
        .exec(
            "CREATE TEMP TRIGGER tr AFTER INSERT ON y BEGIN SELECT 1; END",
            &[],
        )
        .unwrap_err();
    assert_eq!(err.kind, SqlErrorKind::NotAllowed, "{err:?}");
    // The host still reads its own value.
    assert_eq!(db.meta("deploy_seq").unwrap().as_deref(), Some("3"));
}

#[test]
fn a_runaway_statement_is_stopped_at_its_deadline() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = StorageConfig::new(dir.path());
    config.max_statement_time = std::time::Duration::from_millis(300);
    let mut db = rooted(config).open("c", "a").unwrap();
    let started = std::time::Instant::now();
    let err = db
        .query(
            "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c) \
             SELECT count(*) FROM c",
            &[],
        )
        .unwrap_err();
    let took = started.elapsed();
    assert_eq!(err.kind, SqlErrorKind::QuotaExceeded, "{err:?}");
    assert!(
        took < std::time::Duration::from_secs(3),
        "stopped after {took:?}"
    );
    // The connection is usable afterwards, and host statements have no
    // deadline left over.
    assert_eq!(
        db.query("SELECT 1", &[]).unwrap().values,
        vec![vec![V::Integer(1)]]
    );
    db.set_meta("k", "v").unwrap();
}

#[test]
fn values_and_results_are_capped() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = StorageConfig::new(dir.path());
    config.max_value_bytes = 1 << 20;
    config.max_result_bytes = 4 << 20;
    let mut db = rooted(config).open("c", "a").unwrap();
    // One value over the cap.
    let err = db.query("SELECT zeroblob(2000000)", &[]).unwrap_err();
    assert_eq!(err.kind, SqlErrorKind::QuotaExceeded, "{err:?}");
    // Many rows under it, together over the result cap: refused while
    // reading, not after holding them all.
    let err = db
        .query(
            "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c) \
             SELECT zeroblob(500000) FROM c",
            &[],
        )
        .unwrap_err();
    assert_eq!(err.kind, SqlErrorKind::QuotaExceeded, "{err:?}");
    // Under both caps: fine.
    let ok = db.query("SELECT length(zeroblob(500000))", &[]).unwrap();
    assert_eq!(ok.values, vec![vec![V::Integer(500_000)]]);
}

#[test]
fn result_cap_counts_empty_text_rows() {
    assert_empty_rows_hit_result_cap("''");
}

#[test]
fn result_cap_counts_empty_blob_rows() {
    assert_empty_rows_hit_result_cap("x''");
}

fn assert_empty_rows_hit_result_cap(value: &str) {
    let dir = tempfile::tempdir().unwrap();
    let mut config = StorageConfig::new(dir.path());
    config.max_result_bytes = 1024;
    let mut db = rooted(config).open("c", "a").unwrap();
    let err = db
        .query(
            &format!(
                "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL \
                 SELECT x + 1 FROM c WHERE x < 100) SELECT {value} AS v FROM c"
            ),
            &[],
        )
        .unwrap_err();
    assert_eq!(err.kind, SqlErrorKind::QuotaExceeded, "{err:?}");
    assert_eq!(
        err.message,
        "result larger than 1024 bytes; select less or page through it"
    );
    // Quota rejection leaves the connection usable for ordinary small results.
    assert_eq!(
        db.query("SELECT 1 AS n, 'ok' AS t, x'01' AS b, NULL AS z", &[])
            .unwrap()
            .values,
        vec![vec![
            V::Integer(1),
            V::Text("ok".into()),
            V::Blob(vec![1]),
            V::Null,
        ]]
    );
}

#[test]
fn result_cap_counts_empty_cells_in_a_wide_row() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = StorageConfig::new(dir.path());
    config.max_result_bytes = 4096;
    let mut db = rooted(config).open("c", "a").unwrap();
    // The column names fit; their cells must also be charged.
    let cells = ["'' AS v"; 64].join(", ");
    assert!(
        db.query(&format!("SELECT {cells} WHERE 0"), &[])
            .unwrap()
            .values
            .is_empty()
    );
    let err = db.query(&format!("SELECT {cells}"), &[]).unwrap_err();
    assert_eq!(err.kind, SqlErrorKind::QuotaExceeded, "{err:?}");
    assert!(err.message.starts_with("result larger than 4096 bytes"));
}

#[test]
fn result_cap_counts_column_names_even_without_rows() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = StorageConfig::new(dir.path());
    config.max_result_bytes = 1024;
    let mut db = rooted(config).open("c", "a").unwrap();
    let alias = "a".repeat(2048);
    for condition in ["", " WHERE 0"] {
        let err = db
            .query(&format!("SELECT '' AS \"{alias}\"{condition}"), &[])
            .unwrap_err();
        assert_eq!(err.kind, SqlErrorKind::QuotaExceeded, "{err:?}");
        assert_eq!(
            err.message,
            "result larger than 1024 bytes; select less or page through it"
        );
    }
}

#[test]
fn result_cap_charges_column_names_only_once() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = StorageConfig::new(dir.path());
    config.max_result_bytes = 4096;
    let mut db = rooted(config).open("c", "a").unwrap();
    let alias = "a".repeat(1024);
    let rows = db
        .query(
            &format!(
                "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL \
                 SELECT x + 1 FROM c WHERE x < 10) SELECT '' AS \"{alias}\" FROM c"
            ),
            &[],
        )
        .unwrap();
    assert_eq!(rows.columns, [alias]);
    assert_eq!(rows.values, vec![vec![V::Text(String::new())]; 10]);
}

#[test]
fn result_cap_counts_lossy_text_expansion() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = StorageConfig::new(dir.path());
    config.max_result_bytes = 2048;
    let mut db = rooted(config).open("c", "a").unwrap();
    let sql = "SELECT CAST(?1 AS TEXT) AS v";
    // Preserve SQLite text conversion, including mixed valid/invalid UTF-8.
    let bytes = b"ok\xc3\xa9\xff\xf0\x90\x80";
    assert_eq!(
        db.query(sql, &[V::Blob(bytes.to_vec())]).unwrap().values,
        vec![vec![V::Text(String::from_utf8_lossy(bytes).into_owned())]]
    );
    assert_eq!(
        db.query(sql, &[V::Blob(vec![b'a'; 1000])]).unwrap().values,
        vec![vec![V::Text("a".repeat(1000))]]
    );
    // Raw bytes fit, but each invalid byte becomes a three-byte replacement.
    let err = db.query(sql, &[V::Blob(vec![0xff; 1000])]).unwrap_err();
    assert_eq!(err.kind, SqlErrorKind::QuotaExceeded, "{err:?}");
    assert!(err.message.starts_with("result larger than 2048 bytes"));
}

#[test]
fn writes_stop_when_one_event_fills_the_wal() {
    // Replication keeps committed pages in the WAL until the host captures
    // them between events; one event rewriting the same rows must not grow it
    // without bound.
    let dir = tempfile::tempdir().unwrap();
    let s = rooted(StorageConfig {
        max_wal_bytes: 1024 * 1024,
        ..StorageConfig::new(dir.path())
    });
    let mut db = s.open("c", "a").unwrap();
    db.start_capture().unwrap();
    db.exec("CREATE TABLE t (id INTEGER PRIMARY KEY, b BLOB)", &[])
        .unwrap();
    db.exec("INSERT INTO t VALUES (1, NULL)", &[]).unwrap();
    // Different bytes each time: SQLite skips rewriting identical content.
    let blob = |n: u8| V::Blob(vec![n; 64 * 1024]);
    let mut writes = 0u32;
    let err = loop {
        match db.exec("UPDATE t SET b = ?1 WHERE id = 1", &[blob(writes as u8)]) {
            Ok(_) => writes += 1,
            Err(e) => break e,
        }
        assert!(writes < 1000, "the WAL grew without bound");
    };
    assert_eq!(err.kind, SqlErrorKind::QuotaExceeded, "{err:?}");
    assert!(err.message.contains("one event"), "{err:?}");
    // Reads and transaction control still work.
    db.query("SELECT count(*) FROM t", &[]).unwrap();
    db.exec("BEGIN", &[]).unwrap();
    db.exec("ROLLBACK", &[]).unwrap();
    // Once the host has captured the changes, writes work again.
    assert!(db.take_changes().unwrap().is_some());
    db.exec("UPDATE t SET b = ?1 WHERE id = 1", &[blob(1)])
        .unwrap();
}

#[test]
fn a_rolled_back_transaction_that_filled_the_wal_does_not_wedge_the_actor() {
    // A transaction larger than the page cache spills to the WAL; rolled
    // back, its frames stay in the file. The next capture must still empty
    // the WAL, or every later write would be refused.
    let dir = tempfile::tempdir().unwrap();
    let s = rooted(StorageConfig {
        max_wal_bytes: 1024 * 1024,
        ..StorageConfig::new(dir.path())
    });
    let mut db = s.open("c", "a").unwrap();
    db.start_capture().unwrap();
    db.exec("CREATE TABLE t (b BLOB)", &[]).unwrap();
    db.take_changes().unwrap();
    db.exec("BEGIN", &[]).unwrap();
    let mut n = 0;
    let err = loop {
        match db.exec("INSERT INTO t VALUES (randomblob(500000))", &[]) {
            Ok(_) => n += 1,
            Err(e) => break e,
        }
        assert!(n < 100, "the WAL grew without bound");
    };
    assert_eq!(err.kind, SqlErrorKind::QuotaExceeded, "{err:?}");
    assert!(db.end_event().unwrap(), "the transaction was rolled back");
    assert!(db.take_changes().unwrap().is_none());
    db.exec("INSERT INTO t VALUES (1)", &[]).unwrap();
    // The checkpoint lost nothing: what was captured is what is on disk.
    assert!(db.take_changes().unwrap().is_some());
    db.snapshot().unwrap();
}

#[test]
fn sql_cpu_per_event_is_bounded() {
    // The guest's own CPU budget does not see time spent in SQLite; one
    // event's statements share their own CPU budget.
    let dir = tempfile::tempdir().unwrap();
    let s = rooted(StorageConfig {
        max_sql_cpu_per_event: std::time::Duration::from_millis(50),
        ..StorageConfig::new(dir.path())
    });
    let mut db = s.open("c", "a").unwrap();
    // One long statement is stopped at the budget, well before its own
    // (wall-clock) deadline.
    let started = std::time::Instant::now();
    let err = db
        .query(
            "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c \
             WHERE x < 1000000000) SELECT count(*) FROM c",
            &[],
        )
        .unwrap_err();
    assert_eq!(err.kind, SqlErrorKind::QuotaExceeded, "{err:?}");
    assert!(err.message.contains("CPU"), "{err:?}");
    assert!(started.elapsed() < std::time::Duration::from_secs(5));
    // The event's budget is spent: even a cheap statement is refused.
    let err = db.query("SELECT 1", &[]).unwrap_err();
    assert_eq!(err.kind, SqlErrorKind::QuotaExceeded, "{err:?}");
    // The next event starts with the full budget.
    db.end_event().unwrap();
    db.query("SELECT 1", &[]).unwrap();
    // Many short statements add up too. Each is small enough that several
    // fit in the budget even on a slow, shared machine.
    let mut n = 0;
    let err = loop {
        let r = db.query(
            "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c \
             WHERE x < 2000) SELECT count(*) FROM c",
            &[],
        );
        match r {
            Ok(_) => n += 1,
            Err(e) => break e,
        }
        assert!(n < 100_000, "never stopped");
    };
    assert_eq!(err.kind, SqlErrorKind::QuotaExceeded, "{err:?}");
    assert!(n > 1, "a few short statements fit");
}

/// Opening an actor's database waits for a lock another connection holds
/// for a moment (the previous instance's connection closing after
/// hibernation checkpoints under a lock) instead of failing with "database
/// is locked": under load that failure surfaced as WebSockets closed with
/// "unavailable" in the chat and agent tests.
#[test]
fn opening_waits_for_a_lock_held_for_a_moment() {
    let dir = tempfile::tempdir().unwrap();
    let store = storage(&dir);
    drop(store.open("c", "a").unwrap()); // created, in WAL mode
    let path = dir.path().join("actors/t/c/a.db");
    let other = rusqlite::Connection::open(&path).unwrap();
    other.execute_batch("BEGIN EXCLUSIVE").unwrap();
    let holder = std::thread::spawn(move || {
        std::thread::sleep(std::time::Duration::from_millis(300));
        other.execute_batch("COMMIT").unwrap();
    });
    let opened = store.open("c", "a");
    holder.join().unwrap();
    let mut db = opened.unwrap();
    db.exec("CREATE TABLE ok (x)", &[]).unwrap();
}

/// Two connections of one actor overlapping, as an instance closing after
/// hibernation overlaps the next one opening: every open and write must
/// succeed (no "database is locked").
#[test]
fn overlapping_opens_and_closes_of_one_database_all_succeed() {
    let dir = tempfile::tempdir().unwrap();
    let store = std::sync::Arc::new(storage(&dir));
    drop(store.open("c", "a").unwrap());
    let workers: Vec<_> = (0..4)
        .map(|w| {
            let store = std::sync::Arc::clone(&store);
            std::thread::spawn(move || {
                for i in 0..200 {
                    let mut db = store
                        .open("c", "a")
                        .unwrap_or_else(|e| panic!("worker {w}, round {i}: {e}"));
                    db.exec("CREATE TABLE IF NOT EXISTS n (x)", &[])
                        .unwrap_or_else(|e| panic!("worker {w}, round {i}: {e:?}"));
                    db.exec("INSERT INTO n VALUES (1)", &[])
                        .unwrap_or_else(|e| panic!("worker {w}, round {i}: {e:?}"));
                }
            })
        })
        .collect();
    for w in workers {
        w.join().unwrap();
    }
}

/// A "busy" answer while opening is tried again (it can come without
/// SQLite's busy handler in WAL mode), and given up after five attempts,
/// with SQLite's code in the message.
#[cfg(feature = "fault-injection")]
#[test]
fn a_busy_open_is_tried_again_then_given_up() {
    let dir = tempfile::tempdir().unwrap();
    let store = storage(&dir);
    for _ in 0..4 {
        wasmkeep_storage::faults::fail_next("open_busy");
    }
    let mut db = store.open("c", "a").expect("opened at the fifth attempt");
    db.exec("CREATE TABLE ok (x)", &[]).unwrap();
    drop(db);
    for _ in 0..5 {
        wasmkeep_storage::faults::fail_next("open_busy");
    }
    let err = store.open("c", "b").unwrap_err().to_string();
    assert!(err.contains("database is locked"), "{err}");
    assert!(err.contains("(SQLite code 5)"), "{err}");
}

/// `exec_commit` is `exec_durable` for a caller that makes the commit
/// durable itself: the same rules about transactions, and no fsync of its
/// own. With the WAL unreadable to a new handle, the fsync `exec_durable`
/// does after a transaction's `COMMIT` fails; `exec_commit` does not try.
#[cfg(unix)]
#[test]
fn a_commit_without_a_local_sync_keeps_the_rules_and_skips_the_fsync() {
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir().unwrap();
    {
        let mut db = storage(&dir).open("c", "a").unwrap();
        db.exec("CREATE TABLE t (x)", &[]).unwrap();
        for sql in ["BEGIN", "SAVEPOINT s", "RELEASE s", "ROLLBACK", "COMMIT"] {
            let err = db.exec_commit(sql, &[]).unwrap_err();
            assert_eq!(err.kind, SqlErrorKind::NotAllowed, "{sql}: {err:?}");
            assert!(!db.in_transaction(), "{sql} opened a transaction");
        }
        assert_eq!(db.exec_commit("INSERT INTO t VALUES (1)", &[]).unwrap(), 1);
        // Inside a transaction only its COMMIT runs.
        db.exec("BEGIN", &[]).unwrap();
        db.exec("INSERT INTO t VALUES (2)", &[]).unwrap();
        let err = db.exec_commit("INSERT INTO t VALUES (3)", &[]).unwrap_err();
        assert_eq!(err.kind, SqlErrorKind::NotAllowed, "{err:?}");
        let mut wal = db.path().as_os_str().to_owned();
        wal.push("-wal");
        let perms = |mode| std::fs::set_permissions(&wal, std::fs::Permissions::from_mode(mode));
        perms(0o000).unwrap();
        let r = db.exec_commit("COMMIT", &[]);
        perms(0o644).unwrap();
        r.unwrap();
        assert!(!db.in_transaction());
        assert!(!db.end_event().unwrap(), "nothing left to roll back");
    }
    let mut db = storage(&dir).open("c", "a").unwrap();
    assert_eq!(db.query("SELECT x FROM t", &[]).unwrap().values.len(), 2);
}

/// The guest's sequence: a foreign key from a guest table to a host table,
/// with `ON DELETE CASCADE`, and a trigger on the guest table that copies
/// host metadata out. The host's own `DELETE` (clearing the alarm) must not
/// run the cascade or the trigger with the host's privileges.
#[test]
fn a_host_delete_does_not_cascade_into_guest_triggers() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = storage(&dir).open("c", "a").unwrap();
    db.set_meta("secret", "s3cr3t").unwrap();
    let steps = [
        "PRAGMA foreign_keys=OFF",
        "CREATE TABLE g (a INTEGER REFERENCES _wasmkeep_alarm(id) ON DELETE CASCADE)",
        "CREATE TABLE leak(k,v)",
        "INSERT INTO g VALUES(1)",
        "PRAGMA foreign_keys=ON",
        "CREATE TRIGGER tr AFTER DELETE ON g BEGIN \
         INSERT INTO leak SELECT key,value FROM _wasmkeep_meta; END",
    ];
    let results: Vec<_> = steps.iter().map(|sql| db.exec(sql, &[])).collect();
    // A foreign key to a host table is refused outright.
    let err = results[1].as_ref().unwrap_err();
    assert_eq!(err.kind, SqlErrorKind::NotAllowed, "{err:?}");
    db.set_alarm(5_000).unwrap();
    db.clear_alarm().unwrap();
    assert_eq!(db.alarm().unwrap(), None);
    let leaked = db.query("SELECT count(*) FROM leak", &[]).unwrap();
    assert_eq!(leaked.values, vec![vec![V::Integer(0)]]);
}

/// The same, with the foreign key and trigger already in the file (written
/// by an older version, or by hand): host statements still run none of the
/// guest's triggers or cascades, in or out of a guest transaction, and the
/// guest's own triggers and foreign keys keep working.
#[test]
fn host_statements_run_no_guest_trigger_or_cascade() {
    let dir = tempfile::tempdir().unwrap();
    let store = storage(&dir);
    drop(store.open("c", "a").unwrap());
    let raw = rusqlite::Connection::open(dir.path().join("actors/t/c/a.db")).unwrap();
    raw.execute_batch(
        "CREATE TABLE g (a INTEGER REFERENCES _wasmkeep_alarm(id) ON DELETE CASCADE);
         CREATE TABLE leak(k, v);
         CREATE TRIGGER tr AFTER DELETE ON g BEGIN
             INSERT INTO leak SELECT key, value FROM _wasmkeep_meta;
         END;
         CREATE TRIGGER ta AFTER INSERT ON _wasmkeep_meta BEGIN
             INSERT INTO leak VALUES ('meta', new.value);
         END;",
    )
    .unwrap();
    let mut db = store.open("c", "a").unwrap();
    db.set_meta("secret", "s3cr3t").unwrap();
    for in_transaction in [false, true] {
        db.set_alarm(5_000).unwrap();
        // The guest cannot write a row that references a host table (the
        // foreign key check reads it); this file has one.
        raw.execute("INSERT INTO g VALUES (1)", []).unwrap();
        if in_transaction {
            db.exec("BEGIN", &[]).unwrap();
        }
        db.clear_alarm().unwrap();
        db.set_meta("other", "x").unwrap();
        if in_transaction {
            db.exec("COMMIT", &[]).unwrap();
        }
        assert_eq!(db.alarm().unwrap(), None);
        let leaked = db.query("SELECT count(*) FROM leak", &[]).unwrap();
        assert_eq!(leaked.values, vec![vec![V::Integer(0)]], "{in_transaction}");
        db.exec("DELETE FROM g", &[]).unwrap_err();
        db.end_event().unwrap();
    }
    // The guest's own trigger and foreign keys still work.
    db.exec("CREATE TABLE p (id INTEGER PRIMARY KEY)", &[])
        .unwrap();
    db.exec("CREATE TABLE ch (p REFERENCES p(id))", &[])
        .unwrap();
    db.exec("CREATE TABLE seen (x)", &[]).unwrap();
    db.exec(
        "CREATE TRIGGER mine AFTER INSERT ON p BEGIN INSERT INTO seen VALUES (new.id); END",
        &[],
    )
    .unwrap();
    db.exec("INSERT INTO p VALUES (7)", &[]).unwrap();
    let seen = db.query("SELECT x FROM seen", &[]).unwrap();
    assert_eq!(seen.values, vec![vec![V::Integer(7)]]);
    let err = db.exec("INSERT INTO ch VALUES (8)", &[]).unwrap_err();
    assert_eq!(err.kind, SqlErrorKind::Constraint, "{err:?}");
}

#[test]
fn reserved_names_are_refused_for_every_guest_object() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = storage(&dir).open("c", "a").unwrap();
    db.exec("CREATE TABLE t (x)", &[]).unwrap();
    for sql in [
        "CREATE VIEW _wasmkeep_v AS SELECT 1",
        "CREATE VIEW \"_WASMKEEP_META\" AS SELECT 1",
        "CREATE INDEX _wasmkeep_i ON t (x)",
        "CREATE TRIGGER _wasmkeep_tr AFTER INSERT ON t BEGIN SELECT 1; END",
        "ALTER TABLE t RENAME TO _wasmkeep_x",
        "ALTER TABLE t RENAME TO [_Wasmkeep_x]",
        "CREATE TABLE g (a REFERENCES \"_WASMKEEP_ALARM\")",
        "CREATE TABLE g (a, FOREIGN KEY (a) REFERENCES `_wasmkeep_meta` (key))",
        "ALTER TABLE t ADD COLUMN y REFERENCES _wasmkeep_alarm (id)",
    ] {
        let err = db.exec(sql, &[]).unwrap_err();
        assert_eq!(err.kind, SqlErrorKind::NotAllowed, "{sql}: {err:?}");
    }
    let schema = db
        .query(
            "SELECT count(*) FROM sqlite_master \
             WHERE name <> 't' AND substr(name, 1, 7) <> 'sqlite_'",
            &[],
        )
        .unwrap();
    assert_eq!(
        schema.values,
        vec![vec![V::Integer(2)]],
        "only the host tables"
    );
    // A name that merely contains the prefix is the guest's own.
    db.exec("CREATE TABLE app_wasmkeep_state (x)", &[]).unwrap();
    db.exec("CREATE INDEX my_wasmkeep_i ON t (x)", &[]).unwrap();
}

#[test]
fn guest_connections_have_tight_sqlite_limits() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = storage(&dir).open("c", "a").unwrap();
    let columns = |n: usize| (0..n).map(|i| format!("{i}")).collect::<Vec<_>>().join(",");
    db.query(&format!("SELECT {}", columns(500)), &[]).unwrap();
    db.query(&format!("SELECT {}", columns(501)), &[])
        .unwrap_err();
    let args = |n: usize| (0..n).map(|_| "1").collect::<Vec<_>>().join(",");
    db.query(&format!("SELECT max({})", args(64)), &[]).unwrap();
    db.query(&format!("SELECT max({})", args(65)), &[])
        .unwrap_err();
    let union = |n: usize| {
        (0..n)
            .map(|_| "SELECT 1")
            .collect::<Vec<_>>()
            .join(" UNION ALL ")
    };
    db.query(&union(100), &[]).unwrap();
    db.query(&union(101), &[]).unwrap_err();
    let depth = |n: usize| format!("SELECT {}", vec!["1"; n].join(" + "));
    db.query(&depth(200), &[]).unwrap();
    db.query(&depth(300), &[]).unwrap_err();
    let pattern = |n: usize| V::Text("a".repeat(n));
    db.query("SELECT 'a' LIKE ?1", &[pattern(100)]).unwrap();
    let err = db.query("SELECT 'a' LIKE ?1", &[pattern(101)]).unwrap_err();
    assert!(err.message.contains("too complex"), "{err:?}");
    let err = db.query("SELECT 'a' GLOB ?1", &[pattern(101)]).unwrap_err();
    assert!(err.message.contains("too complex"), "{err:?}");
}

/// `instr`, `replace` and the two-argument `trim`, `ltrim` and `rtrim` are
/// the host's, so they stop at the event's budgets; they answer exactly as
/// SQLite's own (compared with a plain connection), also for blobs, numbers,
/// NULLs, empty arguments and invalid UTF-8.
#[test]
fn string_functions_answer_as_sqlites_own() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = storage(&dir).open("c", "a").unwrap();
    let plain = rusqlite::Connection::open_in_memory().unwrap();
    let values = [
        V::Null,
        V::Integer(0),
        V::Integer(-120),
        V::Integer(12_012),
        V::Real(1.5),
        V::Real(100.0),
        V::Real(-2.5e-7),
        V::Real(1e20),
        V::Text(String::new()),
        V::Text("a".into()),
        V::Text("ab".into()),
        V::Text("banana".into()),
        V::Text("ana".into()),
        V::Text("héllo wörld".into()),
        V::Text("ö".into()),
        V::Text("  xx  ".into()),
        V::Text(" x".into()),
        V::Text("12".into()),
        V::Text(".5".into()),
        V::Blob(Vec::new()),
        V::Blob(vec![0]),
        V::Blob(vec![b'a', 0, b'n', b'a']),
        V::Blob(vec![0, b'a']),
        V::Blob(b"banana".to_vec()),
        V::Blob(vec![0xc3]),
        V::Blob(vec![0xa9, b'a', 0xc3, 0xa9, 0x80, 0x80, b'a']),
        V::Blob(vec![0xc3, 0xa9, 0x80]),
        V::Blob(vec![0x80, 0xc3]),
    ];
    // Invalid UTF-8 as text, too: CAST(blob AS TEXT) keeps the bytes. The
    // answer is compared as its type and bytes (hex).
    let plain_eval = |sql: &str, args: &[&V]| -> Option<(String, String)> {
        let params: Vec<rusqlite::types::Value> = args
            .iter()
            .map(|v| rusqlite::types::Value::from(*v))
            .collect();
        plain
            .query_row(sql, rusqlite::params_from_iter(params), |r| {
                Ok((r.get(0)?, r.get(1)?))
            })
            .ok()
    };
    let short = |v: &V| match v {
        V::Text(t) => t.len() <= 2,
        V::Blob(b) => b.len() <= 2,
        _ => true,
    };
    let mut checked = 0;
    for (name, arity) in [
        ("instr", 2),
        ("replace", 3),
        ("trim", 2),
        ("ltrim", 2),
        ("rtrim", 2),
    ] {
        let mut combos: Vec<Vec<&V>> = vec![vec![]];
        for i in 0..arity {
            let mut next = Vec::new();
            for c in &combos {
                // A shorter list for replace's third argument.
                for v in values.iter().filter(|v| i < 2 || short(v)) {
                    let mut c = c.clone();
                    c.push(v);
                    next.push(c);
                }
            }
            combos = next;
        }
        for args in combos {
            for cast in [false, true] {
                let operands: Vec<String> = (1..=arity)
                    .map(|i| match args[i - 1] {
                        V::Blob(_) if cast => format!("CAST(?{i} AS TEXT)"),
                        _ => format!("?{i}"),
                    })
                    .collect();
                let call = format!("{name}({})", operands.join(", "));
                let sql = format!("SELECT typeof({call}), hex({call})");
                let expected = plain_eval(&sql, &args);
                let params: Vec<V> = args.iter().map(|v| (*v).clone()).collect();
                let got = db.query(&sql, &params).ok().map(|r| {
                    match (&r.values[0][0], &r.values[0][1]) {
                        (V::Text(t), V::Text(h)) => (t.clone(), h.clone()),
                        other => panic!("{other:?}"),
                    }
                });
                assert_eq!(got, expected, "{sql} with {args:?}");
                checked += 1;
            }
        }
        db.end_event().unwrap();
    }
    assert!(checked > 1000, "{checked}");
}

/// The same functions on inputs that take SQLite's own versions minutes:
/// stopped at the event's SQL CPU budget.
#[test]
fn slow_string_functions_stop_at_the_sql_cpu_budget() {
    let dir = tempfile::tempdir().unwrap();
    let s = rooted(StorageConfig {
        max_sql_cpu_per_event: std::time::Duration::from_millis(100),
        ..StorageConfig::new(dir.path())
    });
    let mut db = s.open("c", "a").unwrap();
    for sql in [
        "SELECT instr(printf('%.*c', 4000000, 'a'), printf('%.*c', 2000000, 'a') || 'b')",
        "SELECT length(replace(printf('%.*c', 4000000, 'a'), printf('%.*c', 2000000, 'a') || 'b', 'x'))",
        "SELECT length(trim(printf('%.*c', 1000000, 'a'), replace(printf('%.*c', 500000, 'b'), 'b', 'é') || 'a'))",
        "SELECT length(ltrim(printf('%.*c', 1000000, 'a'), replace(printf('%.*c', 500000, 'b'), 'b', 'é') || 'a'))",
        "SELECT length(rtrim(printf('%.*c', 1000000, 'a'), replace(printf('%.*c', 500000, 'b'), 'b', 'é') || 'a'))",
    ] {
        let started = std::time::Instant::now();
        let err = db.query(sql, &[]).unwrap_err();
        assert_eq!(err.kind, SqlErrorKind::QuotaExceeded, "{sql}: {err:?}");
        assert!(err.message.contains("CPU"), "{sql}: {err:?}");
        assert!(
            started.elapsed() < std::time::Duration::from_secs(5),
            "{sql}: stopped after {:?}",
            started.elapsed()
        );
        db.end_event().unwrap();
    }
}

/// One statement that writes far more than the write-ahead log's cap is
/// stopped while it runs, not only refused once the log is already full.
#[test]
fn one_statement_cannot_write_far_past_the_wal_cap() {
    let dir = tempfile::tempdir().unwrap();
    let s = rooted(StorageConfig {
        max_wal_bytes: 1024 * 1024,
        ..StorageConfig::new(dir.path())
    });
    let mut db = s.open("c", "a").unwrap();
    db.start_capture().unwrap();
    db.exec("CREATE TABLE t (b BLOB)", &[]).unwrap();
    db.take_changes().unwrap();
    let err = db
        .exec(
            "INSERT INTO t WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c \
             WHERE x < 5000) SELECT randomblob(10000) FROM c",
            &[],
        )
        .unwrap_err();
    assert_eq!(err.kind, SqlErrorKind::QuotaExceeded, "{err:?}");
    assert!(err.message.contains("write-ahead log"), "{err:?}");
    let mut wal = db.path().as_os_str().to_owned();
    wal.push("-wal");
    let len = std::fs::metadata(&wal).unwrap().len();
    assert!(len < 16 << 20, "the WAL grew to {len} bytes");
    // Reads still run, and the event's rollback leaves the actor usable.
    db.query("SELECT count(*) FROM t", &[]).unwrap();
    db.end_event().unwrap();
    db.take_changes().unwrap();
    db.exec("INSERT INTO t VALUES (x'01')", &[]).unwrap();
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
