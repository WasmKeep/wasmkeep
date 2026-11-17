#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
use super::*;
use object_store::ObjectStoreExt;
use parquet::{
    file::reader::{FileReader, SerializedFileReader},
    record::Field,
};
use std::{collections::BTreeSet, path::Path};

async fn fixture() -> Arc<dyn ObjectStore> {
    let dir = tempfile::tempdir().unwrap();
    let storage = wasmkeep_storage::Storage::new(
        wasmkeep_storage::StorageConfig::new(dir.path()),
        wasmkeep_storage::StateDir {
            dir: "actors".into(),
            scratch: "scratch".into(),
            tenant: "t".into(),
        },
    );
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    for (class, name, value) in [("c", "a", 2), ("c", "b", 3), ("d", "z", 5)] {
        let mut db = storage.open(class, name).unwrap();
        for sql in [
            "CREATE TABLE items(value)",
            "CREATE TABLE empty(value)",
            "CREATE TABLE notes_data(value)",
            "CREATE VIRTUAL TABLE search USING fts5(text)",
            "CREATE VIRTUAL TABLE spatial USING rtree(id,min,max)",
        ] {
            db.exec(sql, &[]).unwrap();
        }
        db.exec("INSERT INTO items VALUES(?)", &[SqlValue::Integer(value)])
            .unwrap();
        db.exec(
            "INSERT INTO notes_data VALUES(?)",
            &[SqlValue::Integer(value)],
        )
        .unwrap();
        db.start_capture().unwrap();
        let bytes = db.snapshot().unwrap().to_ltx(1, 1, 100).unwrap();
        store
            .put(
                &Key::from(format!(
                    "state/t/a/actors/{class}/{name}/{}",
                    snapshot_key(SegmentId {
                        generation: 1,
                        txid: 1
                    })
                )),
                bytes.into(),
            )
            .await
            .unwrap();
    }
    store
}
fn args(out: &Path, state: &Path) -> Args {
    Args {
        app: ("t".into(), "a".into()),
        bucket: "s3://local".into(),
        to: Target::Parquet,
        out: out.into(),
        state: state.into(),
        classes: vec![],
        max_bytes: 2 << 20,
    }
}
fn files(path: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    let mut result = BTreeMap::new();
    if path.exists() {
        for entry in std::fs::read_dir(path).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                result.extend(files(&path));
            } else {
                result.insert(path.clone(), std::fs::read(path).unwrap());
            }
        }
    }
    result
}
fn rows(path: &Path) -> Vec<BTreeMap<String, Field>> {
    SerializedFileReader::new(std::fs::File::open(path).unwrap())
        .unwrap()
        .get_row_iter(None)
        .unwrap()
        .map(|r| {
            r.unwrap()
                .get_column_iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect()
        })
        .collect()
}
fn projection(out: &Path) {
    let mut latest = BTreeMap::new();
    let mut sequences = BTreeSet::new();
    for file in files(&out.join("_wk_states")).keys() {
        let mut actors = BTreeSet::new();
        for row in rows(file) {
            let (Field::Str(class), Field::Str(name), Field::ULong(seq)) =
                (&row["_wk_class"], &row["_wk_name"], &row["_wk_seq"])
            else {
                panic!()
            };
            assert!(actors.insert((class.clone(), name.clone())));
            assert_eq!(
                row["_wk_tables"],
                Field::Str("[\"empty\",\"items\",\"notes_data\"]".into())
            );
            latest
                .entry((class.clone(), name.clone()))
                .and_modify(|s: &mut u64| *s = (*s).max(*seq))
                .or_insert(*seq);
            sequences.insert(*seq);
        }
    }
    let mut actual = BTreeMap::new();
    for file in files(&out.join("tables")).keys() {
        assert_eq!(file.extension().unwrap(), "parquet");
        let seq: u64 = file.file_stem().unwrap().to_str().unwrap().parse().unwrap();
        assert!(sequences.contains(&seq), "orphan {file:?}");
        let table = file
            .parent()
            .unwrap()
            .file_name()
            .unwrap()
            .to_str()
            .unwrap();
        assert!(["empty", "items", "notes_data"].contains(&table));
        if table != "items" {
            continue;
        }
        let class = file
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .file_name()
            .unwrap()
            .to_str()
            .unwrap();
        for row in rows(file) {
            let (Field::Str(name), Field::ULong(seq), Field::Long(value)) =
                (&row["_wk_name"], &row["_wk_seq"], &row["value"])
            else {
                panic!()
            };
            let key = (class.to_string(), name.clone());
            if latest.get(&key) == Some(seq) {
                assert!(actual.insert(key, *value).is_none());
            }
        }
    }
    assert_eq!(
        actual,
        BTreeMap::from([
            (("c".into(), "a".into()), 2),
            (("c".into(), "b".into()), 3),
            (("d".into(), "z".into()), 5)
        ])
    );
}
#[tokio::test]
async fn resume_after_every_publication_step() {
    let store = fixture().await;
    for max_actors in [1, 10_000] {
        let batches = if max_actors == 1 { 3 } else { 1 };
        for (step, count) in [
            ("reserved", batches),
            ("listed", 3),
            ("data", if batches == 3 { 9 } else { 6 }),
            ("data_complete", batches),
            ("states", batches),
            ("before_state", batches),
            ("head", 3),
            ("advanced", batches),
        ] {
            for occurrence in 1..=count {
                let mut seen = 0;
                let out = tempfile::tempdir().unwrap();
                let state = tempfile::tempdir().unwrap();
                let args = args(out.path(), state.path());
                let error = poll_inner(
                    &args,
                    &store,
                    "",
                    &mut |s| {
                        if s == step {
                            seen += 1;
                        }
                        anyhow::ensure!(
                            s != step || seen != occurrence,
                            "injected {step}/{occurrence}"
                        );
                        Ok(())
                    },
                    max_actors,
                    1 << 30,
                )
                .await
                .unwrap_err();
                assert!(error.to_string().contains("injected"), "{error:#}");
                let committed = files(&out.path().join("_wk_states"));
                let before = files(out.path());
                let db = Connection::open(state.path().join("state.db")).unwrap();
                let reserved: i64 = db
                    .query_row("SELECT reserved FROM feed", [], |r| r.get(0))
                    .unwrap();
                let committed_seq: i64 = db
                    .query_row("SELECT seq FROM feed", [], |r| r.get(0))
                    .unwrap();
                let newest_head: i64 = db
                    .query_row("SELECT coalesce(max(seq),0) FROM heads", [], |r| r.get(0))
                    .unwrap();
                assert!(newest_head <= committed_seq);
                drop(db);
                // Model a process killed with an unfinished file and replay directory.
                std::fs::create_dir_all(state.path().join("scratch/stale")).unwrap();
                std::fs::write(state.path().join("scratch/stale/actor.db"), b"plaintext").unwrap();
                if step == "reserved" {
                    std::fs::create_dir_all(out.path().join("_wk_states")).unwrap();
                    std::fs::write(
                        out.path()
                            .join("_wk_states")
                            .join(format!("{reserved:020}.parquet.partial")),
                        b"partial states",
                    )
                    .unwrap();
                }
                if step == "data" {
                    let data = before
                        .keys()
                        .find(|p| {
                            p.file_name().unwrap() == format!("{reserved:020}.parquet").as_str()
                        })
                        .unwrap();
                    std::fs::write(data.with_extension("parquet.partial"), b"partial").unwrap();
                }
                assert_eq!(poll(&args, &store, "").await.unwrap(), 0);
                projection(out.path());
                for (path, bytes) in &before {
                    if path.extension().is_some_and(|e| e == "parquet")
                        && committed.contains_key(
                            &out.path()
                                .join("_wk_states")
                                .join(path.file_name().unwrap()),
                        )
                    {
                        assert_eq!(
                            &std::fs::read(path).unwrap(),
                            bytes,
                            "committed file changed"
                        );
                    }
                }
                assert!(files(&state.path().join("scratch")).is_empty());
                let after = files(out.path());
                for path in after.keys().filter(|p| {
                    p.extension().is_some_and(|e| e == "parquet") && !before.contains_key(*p)
                }) {
                    let seq: i64 = path.file_stem().unwrap().to_str().unwrap().parse().unwrap();
                    assert!(seq > reserved, "reserved sequence reused");
                }
                assert_eq!(poll(&args, &store, "").await.unwrap(), 0);
                assert_eq!(after, files(out.path()), "unchanged poll duplicated batch");
            }
        }
    }
}
#[tokio::test]
async fn batch_limits_resume_and_table_selection() {
    let store = fixture().await;
    for (max_actors, max_rows) in [(1, usize::MAX), (usize::MAX, 1)] {
        let out = tempfile::tempdir().unwrap();
        let state = tempfile::tempdir().unwrap();
        let args = args(out.path(), state.path());
        poll_inner(&args, &store, "", &mut |_| Ok(()), max_actors, max_rows)
            .await
            .unwrap();
        assert_eq!(files(&out.path().join("_wk_states")).len(), 3);
        projection(out.path());
    }
}

#[tokio::test]
async fn lock_identity_and_lost_state() {
    let store = fixture().await;
    let out = tempfile::tempdir().unwrap();
    let state = tempfile::tempdir().unwrap();
    let mut args = args(out.path(), state.path());
    assert_eq!(poll(&args, &store, "").await.unwrap(), 0);
    let before = files(out.path());
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .open(state.path().join("lock"))
        .unwrap();
    lock.lock().unwrap();
    assert!(
        poll(&args, &store, "")
            .await
            .unwrap_err()
            .to_string()
            .contains("locked")
    );
    drop(lock);
    args.bucket = "s3://another".into();
    assert!(
        poll(&args, &store, "")
            .await
            .unwrap_err()
            .to_string()
            .contains("belongs")
    );
    args.bucket = "s3://local".into();
    let fresh = tempfile::tempdir().unwrap();
    args.state = fresh.path().into();
    assert!(
        poll(&args, &store, "")
            .await
            .unwrap_err()
            .to_string()
            .contains("empty output")
    );
    assert_eq!(before, files(out.path()));
}

#[tokio::test]
async fn recorded_types_advance_with_heads() {
    let dir = tempfile::tempdir().unwrap();
    let storage = wasmkeep_storage::Storage::new(
        wasmkeep_storage::StorageConfig::new(dir.path()),
        wasmkeep_storage::StateDir {
            dir: "actors".into(),
            scratch: "scratch".into(),
            tenant: "t".into(),
        },
    );
    let mut actor = storage.open("c", "a").unwrap();
    actor.exec("CREATE TABLE amounts(value)", &[]).unwrap();
    actor.start_capture().unwrap();
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let out = tempfile::tempdir().unwrap();
    let state = tempfile::tempdir().unwrap();
    let args = args(out.path(), state.path());
    for (i, value) in [
        SqlValue::Real(2.5),
        SqlValue::Integer(2),
        SqlValue::Integer((1 << 53) + 1),
        SqlValue::Integer(3),
    ]
    .into_iter()
    .enumerate()
    {
        actor.exec("DELETE FROM amounts", &[]).unwrap();
        actor
            .exec("INSERT INTO amounts VALUES(?)", &[value])
            .unwrap();
        let changes = actor.take_changes().unwrap().unwrap();
        let txid = i as u64 + 1;
        if txid > 1 {
            let segment = wasmkeep_cluster::replication::Segment {
                id: SegmentId {
                    generation: 1,
                    txid,
                },
                parent_generation: 1,
            };
            store
                .put(
                    &Key::from(format!("state/t/a/actors/c/a/{}", segment_key(&segment))),
                    changes.to_ltx(txid, txid, 100).unwrap().into(),
                )
                .await
                .unwrap();
        }
        let bytes = actor.snapshot().unwrap().to_ltx(1, txid, 100).unwrap();
        store
            .put(
                &Key::from(format!(
                    "state/t/a/actors/c/a/{}",
                    snapshot_key(SegmentId {
                        generation: 1,
                        txid
                    })
                )),
                bytes.into(),
            )
            .await
            .unwrap();
        // A visible batch followed by a failed progress transaction must not
        // advance either heads or types. Recovery publishes the same head anew.
        if i == 0 {
            assert!(
                poll_inner(
                    &args,
                    &store,
                    "",
                    &mut |s| {
                        anyhow::ensure!(s != "head", "injected");
                        Ok(())
                    },
                    10_000,
                    1 << 30
                )
                .await
                .is_err()
            );
            let db = Connection::open(state.path().join("state.db")).unwrap();
            assert_eq!(
                db.query_row("SELECT count(*) FROM columns", [], |r| r.get::<_, i64>(0))
                    .unwrap(),
                0
            );
            assert_eq!(
                db.query_row("SELECT count(*) FROM heads", [], |r| r.get::<_, i64>(0))
                    .unwrap(),
                0
            );
        }
        assert_eq!(poll(&args, &store, "").await.unwrap(), 0);
        let path = files(&out.path().join("tables/c/amounts"))
            .last_key_value()
            .unwrap()
            .0
            .clone();
        let expected = match i {
            0 => Field::Double(2.5),
            1 => Field::Double(2.0),
            2 => Field::Str(((1i64 << 53) + 1).to_string()),
            _ => Field::Str("3".into()),
        };
        assert_eq!(rows(&path)[0]["value"], expected);
    }
}

async fn replace_fixture_actor(store: &Arc<dyn ObjectStore>, sql: &str, txid: u64) {
    let dir = tempfile::tempdir().unwrap();
    let storage = wasmkeep_storage::Storage::new(
        wasmkeep_storage::StorageConfig::new(dir.path()),
        wasmkeep_storage::StateDir {
            dir: "actors".into(),
            scratch: "scratch".into(),
            tenant: "t".into(),
        },
    );
    let mut actor = storage.open("c", "a").unwrap();
    for statement in sql.split(';').filter(|s| !s.trim().is_empty()) {
        actor.exec(statement, &[]).unwrap();
    }
    actor.start_capture().unwrap();
    store
        .put(
            &Key::from(format!(
                "state/t/a/actors/c/a/{}",
                snapshot_key(SegmentId {
                    generation: 1,
                    txid
                })
            )),
            actor
                .snapshot()
                .unwrap()
                .to_ltx(1, txid, 100)
                .unwrap()
                .into(),
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn review_visible_actor_removed_before_recovery() {
    visible_actor_removed_before_recovery(false).await;
}

#[tokio::test]
async fn review_resurrected_actor_removed_before_recovery() {
    visible_actor_removed_before_recovery(true).await;
}

async fn visible_actor_removed_before_recovery(previously_deleted: bool) {
    let store = fixture().await;
    let out = tempfile::tempdir().unwrap();
    let state = tempfile::tempdir().unwrap();
    let args = args(out.path(), state.path());
    if previously_deleted {
        assert_eq!(poll(&args, &store, "").await.unwrap(), 0);
        let empty: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        assert_eq!(poll(&args, &empty, "").await.unwrap(), 0);
    }
    assert!(
        poll_with_checkpoint(&args, &store, "", &mut |step| {
            anyhow::ensure!(step != "before_state", "injected crash");
            Ok(())
        })
        .await
        .unwrap_err()
        .to_string()
        .contains("injected crash")
    );
    let visible = files(&out.path().join("_wk_states"));
    let row = rows(visible.keys().next_back().unwrap()).remove(0);
    let (Field::Str(class), Field::Str(name)) = (&row["_wk_class"], &row["_wk_name"]) else {
        panic!()
    };
    let keys: Vec<_> = store
        .list(Some(&Key::from(format!(
            "state/t/a/actors/{class}/{name}/"
        ))))
        .try_collect()
        .await
        .unwrap();
    for key in keys {
        store.delete(&key.location).await.unwrap();
    }
    store
        .put(
            &Key::from("apps/t/a/restored"),
            b"new incarnation".to_vec().into(),
        )
        .await
        .unwrap();
    assert_eq!(poll(&args, &store, "").await.unwrap(), 0);
    let all: Vec<_> = files(&out.path().join("_wk_states"))
        .keys()
        .flat_map(|p| rows(p))
        .filter(|r| {
            r["_wk_class"] == Field::Str(class.clone()) && r["_wk_name"] == Field::Str(name.clone())
        })
        .collect();
    assert_eq!(
        all.last().unwrap()["_wk_kind"],
        Field::Str("deleted".into())
    );
    assert_eq!(
        all.last().unwrap()["_wk_reason"],
        Field::Str("absent".into())
    );
    for (path, bytes) in visible {
        assert_eq!(std::fs::read(path).unwrap(), bytes);
    }
}

#[tokio::test]
async fn review_literal_sqlite_prefix() {
    let store = fixture().await;
    replace_fixture_actor(
        &store,
        "CREATE TABLE sqliteXorders(value); INSERT INTO sqliteXorders VALUES(7)",
        1,
    )
    .await;
    let out = tempfile::tempdir().unwrap();
    let state = tempfile::tempdir().unwrap();
    assert_eq!(
        poll(&args(out.path(), state.path()), &store, "")
            .await
            .unwrap(),
        0
    );
    let data = files(
        &out.path()
            .join("tables/c")
            .join(encode_segment("sqliteXorders")),
    );
    assert_eq!(data.len(), 1);
    assert_eq!(
        rows(data.keys().next().unwrap())[0]["value"],
        Field::Long(7)
    );
    let states: Vec<_> = files(&out.path().join("_wk_states"))
        .keys()
        .flat_map(|p| rows(p))
        .collect();
    assert!(
        states
            .iter()
            .any(|r| r["_wk_tables"] == Field::Str("[\"sqliteXorders\"]".into()))
    );
}

#[tokio::test]
async fn review_large_actor_does_not_starve_later_actors() {
    large_actor_does_not_starve(vec![]).await;
}

#[tokio::test]
async fn review_excluded_large_actor_does_not_starve_later_actors() {
    large_actor_does_not_starve(vec!["c".into(), "d".into()]).await;
}

async fn large_actor_does_not_starve(classes: Vec<String>) {
    let store = fixture().await;
    for i in 0..8000 {
        store
            .put(
                &Key::from(format!("state/t/a/actors/b/large/history-{i:06}")),
                Vec::new().into(),
            )
            .await
            .unwrap();
    }
    {
        let out = tempfile::tempdir().unwrap();
        let state = tempfile::tempdir().unwrap();
        let mut args = args(out.path(), state.path());
        args.classes = classes;
        assert_eq!(poll(&args, &store, "").await.unwrap(), 2);
        let db = Connection::open(state.path().join("state.db")).unwrap();
        assert_eq!(
            db.query_row("SELECT count(*) FROM heads", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            3
        );
        assert_eq!(
            db.query_row("SELECT count(*) FROM heads WHERE class='b'", [], |r| r
                .get::<_, i64>(0))
                .unwrap(),
            0
        );
        projection(out.path());
    }
}

#[tokio::test]
async fn review_collision_does_not_poison_batch() {
    let store = fixture().await;
    replace_fixture_actor(
        &store,
        "CREATE TABLE items(_wk_name, _app_wk_name); INSERT INTO items VALUES('x','y')",
        1,
    )
    .await;
    let out = tempfile::tempdir().unwrap();
    let state = tempfile::tempdir().unwrap();
    assert_eq!(
        poll(&args(out.path(), state.path()), &store, "")
            .await
            .unwrap(),
        2
    );
    let db = Connection::open(state.path().join("state.db")).unwrap();
    assert_eq!(
        db.query_row("SELECT count(*) FROM heads", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        2
    );
    assert_eq!(
        db.query_row(
            "SELECT count(*) FROM heads WHERE class='c' AND name='a'",
            [],
            |r| r.get::<_, i64>(0)
        )
        .unwrap(),
        0
    );
}

#[tokio::test]
async fn review_visible_types_survive_bucket_change() {
    let store = fixture().await;
    replace_fixture_actor(
        &store,
        "CREATE TABLE amounts(value); INSERT INTO amounts VALUES(2.5)",
        1,
    )
    .await;
    let out = tempfile::tempdir().unwrap();
    let state = tempfile::tempdir().unwrap();
    let args = args(out.path(), state.path());
    assert!(
        poll_inner(
            &args,
            &store,
            "",
            &mut |step| {
                anyhow::ensure!(step != "before_state", "injected crash");
                Ok(())
            },
            10_000,
            1 << 30
        )
        .await
        .is_err()
    );
    replace_fixture_actor(
        &store,
        "CREATE TABLE amounts(value); INSERT INTO amounts VALUES(2)",
        1,
    )
    .await;
    assert_eq!(poll(&args, &store, "").await.unwrap(), 0);
    let data = files(&out.path().join("tables/c/amounts"));
    assert_eq!(
        rows(data.last_key_value().unwrap().0)[0]["value"],
        Field::Double(2.0)
    );
}

#[tokio::test]
async fn f5_schema_drift_views() {
    let out = tempfile::tempdir().unwrap();
    let state = tempfile::tempdir().unwrap();
    let store = fixture().await;
    let args = args(out.path(), state.path());
    assert_eq!(poll(&args, &store, "").await.unwrap(), 0);
    let sql = std::fs::read_to_string(out.path().join("views.duckdb.sql")).unwrap();
    assert!(sql.contains("CREATE OR REPLACE VIEW \"c_items_history\""));
    replace_fixture_actor(&store, "CREATE TABLE items(Value, added, _wk_name); INSERT INTO items VALUES(2.5, 'new', 'app-name')", 1).await;
    assert_eq!(poll(&args, &store, "").await.unwrap(), 0);
    let sql = std::fs::read_to_string(out.path().join("views.duckdb.sql")).unwrap();
    assert!(sql.contains("AS DOUBLE) AS \"value\""));
    assert!(sql.contains("CAST(NULL AS VARCHAR) AS \"added\""));
    assert!(sql.contains("\"_app_wk_name\""));
    assert!(sql.contains("_wk_kind <> 'deleted'"));
    assert!(sql.contains("max(_wk_seq)"));
    let duckdb = std::process::Command::new("duckdb")
        .arg("--version")
        .output()
        .is_ok();
    if !duckdb {
        eprintln!("DuckDB execution NOT RUN: offline duckdb binary unavailable; SQL shape checked");
    }
    fn query(sql: &str, query: &str) -> String {
        use std::io::Write;
        let mut child = std::process::Command::new("duckdb")
            .args(["-csv", "-noheader", ":memory:"])
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        child
            .stdin
            .take()
            .unwrap()
            .write_all(format!("{sql}\n{query}").as_bytes())
            .unwrap();
        let result = child.wait_with_output().unwrap();
        assert!(
            result.status.success(),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
        String::from_utf8(result.stdout).unwrap()
    }
    if duckdb {
        assert_eq!(
            query(
                &sql,
                "SELECT _wk_name, value, added, _app_wk_name FROM c_items ORDER BY _wk_name;"
            ),
            "a,2.5,new,app-name\nb,3.0,,\n"
        );
        assert_eq!(query(&sql, "SELECT count(*) FROM c_items_history;"), "3\n");
    }
    replace_fixture_actor(
        &store,
        "CREATE TABLE items(value); INSERT INTO items VALUES('text')",
        1,
    )
    .await;
    poll(&args, &store, "").await.unwrap();
    let sql = std::fs::read_to_string(out.path().join("views.duckdb.sql")).unwrap();
    assert!(sql.contains("AS VARCHAR) AS \"value\""));
    if duckdb {
        assert_eq!(
            query(&sql, "SELECT value FROM c_items WHERE _wk_name='a';"),
            "text\n"
        );
    }
    replace_fixture_actor(&store, "CREATE TABLE other(value)", 1).await;
    poll(&args, &store, "").await.unwrap();
    let sql = std::fs::read_to_string(out.path().join("views.duckdb.sql")).unwrap();
    assert!(sql.contains("CREATE OR REPLACE VIEW \"c_items\""));
    if duckdb {
        assert_eq!(query(&sql, "SELECT _wk_name FROM c_items;"), "b\n");
    }
    let objects = history::list_objects(&store, "state/t/a/actors/c/b/")
        .await
        .unwrap();
    for key in objects.keys() {
        store
            .delete(&Key::from(format!("state/t/a/actors/c/b/{key}")))
            .await
            .unwrap();
    }
    assert_eq!(poll(&args, &store, "").await.unwrap(), 0);
    let sql = std::fs::read_to_string(out.path().join("views.duckdb.sql")).unwrap();
    if duckdb {
        assert_eq!(query(&sql, "SELECT count(*) FROM c_items;"), "0\n");
        assert_eq!(query(&sql, "SELECT count(*) FROM c_items_history;"), "4\n");
    }
}

#[tokio::test]
async fn f5_views_quote_identifiers_cast_blobs_and_ignore_orphans() {
    let root = tempfile::Builder::new()
        .prefix("feed'quote")
        .tempdir()
        .unwrap();
    let out = root.path().join("out");
    let state = root.path().join("state");
    let store = fixture().await;
    let args = args(&out, &state);
    replace_fixture_actor(&store, "CREATE TABLE \"odd\"\"table\"(\"a\"\"b\", _WK_name); INSERT INTO \"odd\"\"table\" VALUES(X'00ff', 'app')", 1).await;
    poll(&args, &store, "").await.unwrap();
    let first = std::fs::read_to_string(out.join("views.duckdb.sql")).unwrap();
    assert!(first.contains("\"c_odd\"\"table_history\""));
    assert!(first.contains("AS BLOB) AS \"a\"\"b\""));
    assert!(first.contains("feed''quote"));
    replace_fixture_actor(
        &store,
        "CREATE TABLE \"odd\"\"table\"(\"A\"\"B\"); INSERT INTO \"odd\"\"table\" VALUES(7)",
        1,
    )
    .await;
    poll(&args, &store, "").await.unwrap();
    let sql = std::fs::read_to_string(out.join("views.duckdb.sql")).unwrap();
    assert!(sql.contains("lower(hex(d.\"a\"\"b\"))"));
    assert!(sql.contains("CAST(d.\"A\"\"B\" AS VARCHAR) AS \"a\"\"b\""));
    assert!(sql.contains("CAST(NULL AS VARCHAR) AS \"_app_WK_name\""));
    let dir = out.join("tables/c").join(encode_segment("odd\"table"));
    let file = files(&dir).into_keys().next().unwrap();
    std::fs::copy(file, dir.join("00000000000000000999.parquet")).unwrap();
    views::publish(&out).unwrap();
    assert_eq!(
        sql,
        std::fs::read_to_string(out.join("views.duckdb.sql")).unwrap()
    );
}

#[tokio::test]
async fn f5_empty_feed_has_loadable_script() {
    let out = tempfile::tempdir().unwrap();
    let state = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    assert_eq!(
        poll(&args(out.path(), state.path()), &store, "")
            .await
            .unwrap(),
        0
    );
    let sql = std::fs::read_to_string(out.path().join("views.duckdb.sql")).unwrap();
    assert!(sql.contains("BEGIN TRANSACTION;"));
    assert!(!sql.contains("read_parquet"));
}

#[tokio::test]
async fn f5_recovers_interrupted_view_publication() {
    let out = tempfile::tempdir().unwrap();
    let state = tempfile::tempdir().unwrap();
    let store = fixture().await;
    let args = args(out.path(), state.path());
    poll(&args, &store, "").await.unwrap();
    let expected = std::fs::read(out.path().join("views.duckdb.sql")).unwrap();
    std::fs::remove_file(out.path().join("views.duckdb.sql")).unwrap();
    std::fs::write(out.path().join("views.duckdb.sql.partial"), b"interrupted").unwrap();
    poll(&args, &store, "").await.unwrap();
    assert!(!out.path().join("views.duckdb.sql.partial").exists());
    assert_eq!(
        expected,
        std::fs::read(out.path().join("views.duckdb.sql")).unwrap()
    );
}

#[tokio::test]
async fn f7_compaction_recovers_and_poll_continues() {
    let mut stops = vec![None];
    let mut attempt = 0;
    while attempt < stops.len() {
        let stop = stops[attempt];
        attempt += 1;
        let out = tempfile::tempdir().unwrap();
        let state = tempfile::tempdir().unwrap();
        let store = fixture().await;
        let args = args(out.path(), state.path());
        poll(&args, &store, "").await.unwrap();
        replace_fixture_actor(
            &store,
            "CREATE TABLE items(value, added); INSERT INTO items VALUES(2.5, 'new')",
            1,
        )
        .await;
        poll(&args, &store, "").await.unwrap();
        replace_fixture_actor(
            &store,
            "CREATE TABLE items(value, added); INSERT INTO items VALUES('rebased', 'latest')",
            1,
        )
        .await;
        poll(&args, &store, "").await.unwrap();
        for key in history::list_objects(&store, "state/t/a/actors/c/b/")
            .await
            .unwrap()
            .keys()
        {
            store
                .delete(&Key::from(format!("state/t/a/actors/c/b/{key}")))
                .await
                .unwrap();
        }
        poll(&args, &store, "").await.unwrap();
        let expected_views = if stop.is_none() {
            f7_view_rows(out.path())
        } else {
            None
        };
        let before = files(&out.path().join("_wk_states"));
        let old_files = files(out.path());
        let mut trace = Vec::new();
        let result = compact::compact(out.path(), state.path(), &mut |step| {
            trace.push(step.to_owned());
            if stop == Some(trace.len() - 1) {
                anyhow::bail!("injected {step}");
            }
            Ok(())
        });
        for (path, bytes) in &old_files {
            if path.extension().is_some_and(|e| e == "parquet") && path.exists() {
                assert_eq!(
                    &std::fs::read(path).unwrap(),
                    bytes,
                    "committed file changed"
                );
            }
        }
        if stop.is_none() {
            result.unwrap();
            stops.extend((0..trace.len()).map(Some));
            assert_eq!(f7_view_rows(out.path()), expected_views);
        } else {
            assert_eq!(
                trace.len(),
                stop.unwrap() + 1,
                "checkpoint {stop:?} not reached: {result:?}"
            );
            assert!(result.is_err());
        }
        // A poll must finish or roll back compaction before examining the bucket.
        poll(&args, &store, "").await.unwrap();
        if trace.iter().any(|s| s == "states") {
            assert_eq!(
                files(&out.path().join("_wk_states")).len(),
                1,
                "poll did not complete interrupted compaction"
            );
        }
        compact::compact(out.path(), state.path(), &mut |_| Ok(())).unwrap();
        let states = files(&out.path().join("_wk_states"));
        assert_eq!(states.len(), 1);
        let latest = rows(states.keys().next().unwrap());
        assert_eq!(latest.len(), 3);
        assert!(
            latest
                .iter()
                .any(|r| r["_wk_name"] == Field::Str("b".into())
                    && r["_wk_kind"] == Field::Str("deleted".into()))
        );
        for path in files(&out.path().join("tables")).keys() {
            assert!(!path.to_string_lossy().ends_with(".partial"));
            if path.to_string_lossy().contains("/c/") {
                assert!(
                    rows(path)
                        .iter()
                        .all(|r| r["_wk_name"] != Field::Str("b".into()))
                );
            }
        }
        let data = files(&out.path().join("tables/c/items"));
        let data = rows(data.keys().next().unwrap());
        assert_eq!(data.len(), 1);
        assert_eq!(data[0]["value"], Field::Str("rebased".into()));
        let last = states
            .keys()
            .next()
            .unwrap()
            .file_stem()
            .unwrap()
            .to_str()
            .unwrap()
            .parse::<u64>()
            .unwrap();
        assert!(
            last > before
                .keys()
                .last()
                .unwrap()
                .file_stem()
                .unwrap()
                .to_str()
                .unwrap()
                .parse::<u64>()
                .unwrap()
        );
        let snapshot = files(out.path());
        poll(&args, &store, "").await.unwrap();
        assert_eq!(
            files(out.path()),
            snapshot,
            "unchanged poll duplicated compacted heads"
        );
        replace_fixture_actor(
            &store,
            "CREATE TABLE items(value); INSERT INTO items VALUES('next')",
            1,
        )
        .await;
        poll(&args, &store, "").await.unwrap();
        let states = files(&out.path().join("_wk_states"));
        assert_eq!(states.len(), 2);
        assert!(
            states
                .keys()
                .any(|p| p.file_name().unwrap() == format!("{:020}.parquet", last + 1).as_str())
        );
    }
}

// DuckDB remains optional, as in F5. The Python package is also a real DuckDB
// engine and lets a local test dependency live outside the workspace/toolchain.
fn f7_view_rows(out: &Path) -> Option<serde_json::Value> {
    use std::{
        io::Write,
        process::{Command, Stdio},
    };
    if !Command::new("python3")
        .args(["-c", "import duckdb"])
        .output()
        .is_ok_and(|r| r.status.success())
    {
        eprintln!("F7 DuckDB execution NOT RUN: Python duckdb unavailable");
        return None;
    }
    let mut child = Command::new("python3")
        .args([
            "-c",
            r#"
import duckdb, json, sys
c = duckdb.connect()
c.execute(sys.stdin.read())
result = {}
for (name,) in c.execute('SHOW TABLES').fetchall():
    if name.endswith('_history'): continue
    quoted = '"' + name.replace('"', '""') + '"'
    q = c.execute('SELECT * EXCLUDE (_wk_seq) FROM ' + quoted + ' ORDER BY ALL')
    result[name] = {'columns': [d[0:2] for d in q.description], 'rows': q.fetchall()}
print(json.dumps(result, default=str))
"#,
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(&std::fs::read(out.join("views.duckdb.sql")).unwrap())
        .unwrap();
    let result = child.wait_with_output().unwrap();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    Some(serde_json::from_slice(&result.stdout).unwrap())
}

#[tokio::test]
async fn f7_compaction_lock_binding_empty_and_all_deleted() {
    let out = tempfile::tempdir().unwrap();
    let state = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let args = args(out.path(), state.path());
    poll(&args, &store, "").await.unwrap();
    compact::compact(out.path(), state.path(), &mut |_| Ok(())).unwrap();
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .open(state.path().join("lock"))
        .unwrap();
    lock.try_lock().unwrap();
    assert!(
        compact::compact(out.path(), state.path(), &mut |_| Ok(()))
            .unwrap_err()
            .to_string()
            .contains("locked")
    );
    assert!(
        poll(&args, &store, "")
            .await
            .unwrap_err()
            .to_string()
            .contains("locked")
    );
    drop(lock);
    let other = tempfile::tempdir().unwrap();
    assert!(
        compact::compact(other.path(), state.path(), &mut |_| Ok(()))
            .unwrap_err()
            .to_string()
            .contains("another output")
    );
    let store = fixture().await;
    poll(&args, &store, "").await.unwrap();
    for object in store.list(None).try_collect::<Vec<_>>().await.unwrap() {
        store.delete(&object.location).await.unwrap();
    }
    poll(&args, &store, "").await.unwrap();
    let before = f7_view_rows(out.path());
    compact::compact(out.path(), state.path(), &mut |_| Ok(())).unwrap();
    assert_eq!(f7_view_rows(out.path()), before);
    let states = files(&out.path().join("_wk_states"));
    assert_eq!(states.len(), 1);
    assert!(
        rows(states.keys().next().unwrap())
            .iter()
            .all(|r| r["_wk_kind"] == Field::Str("deleted".into()))
    );
    assert!(
        files(&out.path().join("tables"))
            .keys()
            .all(|p| rows(p).is_empty())
    );
}

#[tokio::test]
async fn f7_compaction_preserves_cross_file_casts_and_column_spelling() {
    let out = tempfile::tempdir().unwrap();
    let state = tempfile::tempdir().unwrap();
    let store = fixture().await;
    poll(&args(out.path(), state.path()), &store, "")
        .await
        .unwrap();
    let mut db = Connection::open(state.path().join("state.db")).unwrap();
    for (name, values) in [
        (
            "b",
            vec![2.0, 1e-7, 1e16, -0.0]
                .into_iter()
                .map(SqlValue::Real)
                .collect::<Vec<_>>(),
        ),
        ("a", vec![SqlValue::Text("latest".into())]),
    ] {
        let actor = writer::ActorTable {
            tenant: "t".into(),
            app: "a".into(),
            name: name.into(),
            seq: 0,
            generation: 1,
            txid: 2,
            committed_at: 100,
            rows: Rows {
                columns: vec![],
                values: vec![],
            },
        };
        commit_batch(
            &mut db,
            out.path(),
            vec![Changed {
                class: "c".into(),
                encoded: name.into(),
                position: SegmentId {
                    generation: 1,
                    txid: 2,
                },
                identity: "test".into(),
                label: None,
                incarnation: "[null,null]".into(),
                state: writer::State {
                    actor,
                    class: "c".into(),
                    tables: vec!["items".into()],
                    emitted_at: 200,
                    kind: "state".into(),
                    reason: None,
                },
                tables: vec![(
                    "items".into(),
                    Rows {
                        columns: vec!["Value".into()],
                        values: values.into_iter().map(|v| vec![v]).collect(),
                    },
                )],
            }],
            &mut |_| Ok(()),
        )
        .unwrap();
    }
    let before = f7_view_rows(out.path());
    compact::compact(out.path(), state.path(), &mut |_| Ok(())).unwrap();
    assert_eq!(f7_view_rows(out.path()), before);
}

#[tokio::test]
async fn f7_compaction_adopts_visible_f3_batch_before_poll_retry() {
    let out = tempfile::tempdir().unwrap();
    let state = tempfile::tempdir().unwrap();
    let store = fixture().await;
    let args = args(out.path(), state.path());
    poll(&args, &store, "").await.unwrap();
    replace_fixture_actor(
        &store,
        "CREATE TABLE items(value); INSERT INTO items VALUES('visible')",
        1,
    )
    .await;
    assert!(
        poll_with_checkpoint(&args, &store, "", &mut |step| {
            anyhow::ensure!(step != "before_state", "injected F3 crash");
            Ok(())
        })
        .await
        .is_err()
    );
    // SQL still names the previous batches; regenerate for the visible state.
    views::publish(out.path()).unwrap();
    let before = f7_view_rows(out.path());
    compact::compact(out.path(), state.path(), &mut |_| Ok(())).unwrap();
    assert_eq!(f7_view_rows(out.path()), before);
    let states = files(&out.path().join("_wk_states"));
    assert_eq!(states.len(), 1);
    assert_eq!(rows(states.keys().next().unwrap()).len(), 3);
    poll(&args, &store, "").await.unwrap();
    let after = files(out.path());
    poll(&args, &store, "").await.unwrap();
    assert_eq!(files(out.path()), after);
}
