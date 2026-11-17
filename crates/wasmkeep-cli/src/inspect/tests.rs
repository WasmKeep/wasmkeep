use super::*;

#[test]
fn token_missing_wrong_and_constant_time() {
    let token = Token::new().unwrap();
    assert!(!token.accepts(None));
    assert!(!token.accepts(Some(&"0".repeat(64))));
    assert!(token.accepts(Some(&token.0)));
    assert!(include_str!("mod.rs").contains(".ct_eq("));
}

#[test]
fn host_and_bind_are_strict() {
    let address = "127.0.0.1:8765".parse().unwrap();
    assert!(valid_host(Some("127.0.0.1:8765"), address));
    for host in [
        None,
        Some("localhost:8765"),
        Some("evil.test:8765"),
        Some("127.0.0.1:80"),
    ] {
        assert!(!valid_host(host, address));
    }
    assert!(check_bind("127.0.0.1:0".parse().unwrap()).is_ok());
    assert!(check_bind("[::1]:0".parse().unwrap()).is_ok());
    assert!(check_bind("0.0.0.0:0".parse().unwrap()).is_err());
    assert!(check_bind("127.0.0.2:0".parse().unwrap()).is_err());
}

#[test]
fn listing_pages_have_no_gaps() {
    let names: Vec<_> = (0..123).map(|i| format!("actor{i:03}")).collect();
    let first = page(names.clone(), None);
    assert_eq!(first["items"].as_array().unwrap().len(), 50);
    let second = page(names.clone(), first["next"].as_str());
    assert_eq!(second["items"][0], "actor050");
    let third = page(names, second["next"].as_str());
    assert_eq!(third["items"].as_array().unwrap().len(), 23);
    assert!(third["next"].is_null());
}

#[test]
fn embedded_page_has_no_external_urls() {
    assert!(!PAGE.is_empty());
    for pattern in ["http:", "https:", "src=\"//", "href=\"//"] {
        assert!(!PAGE.contains(pattern));
    }
    assert!(PAGE.contains("Read-only SQL"));
}

#[test]
fn sql_denies_writes_and_bounds_results_and_work() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.db");
    let conn = rusqlite::Connection::open(&path).unwrap();
    conn.execute_batch("CREATE TABLE data (value); INSERT INTO data VALUES (1)")
        .unwrap();
    drop(conn);
    assert!(query(&path, "INSERT INTO data VALUES (2)").is_err());
    assert!(query(&path, "SELECT * FROM data").is_ok());
    assert!(query(&path, "WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<10001) SELECT x FROM n").is_err());
    assert!(query(&path, "WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<40) SELECT zeroblob(900000) FROM n").is_err());
    assert!(
        query(
            &path,
            "WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n) SELECT sum(x) FROM n"
        )
        .is_err()
    );
}

#[tokio::test]
async fn http_authentication_covers_page_api_and_assets() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let token = Token::new().unwrap();
            let secret = token.0.clone();
            let state = Arc::new(State {
                store: Arc::new(object_store::memory::InMemory::new()),
                prefix: String::new(),
                bucket: "memory".into(),
                app: ("t".into(), "a".into()),
                bootstrap: std::sync::Mutex::new(Some(Token(secret.clone()))),
                token: Token::new().unwrap(),
                address,
                work: Arc::new(tokio::sync::Semaphore::new(1)),
            });
            let server = tokio::task::spawn_local(async move {
                loop {
                    let (stream, _) = listener.accept().await.unwrap();
                    let state = state.clone();
                    tokio::task::spawn_local(async move {
                        let service = service_fn(move |r| handle(r, state.clone()));
                        let _ = hyper::server::conn::http1::Builder::new()
                            .serve_connection(hyper_util::rt::TokioIo::new(stream), service)
                            .await;
                    });
                }
            });
            let client = reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .unwrap();
            let base = format!("http://{address}");
            for path in ["/", "/api", "/favicon.ico"] {
                for authorization in ["", "Bearer incorrect"] {
                    let r = client
                        .get(format!("{base}{path}"))
                        .header("authorization", authorization)
                        .send()
                        .await
                        .unwrap();
                    assert_eq!(r.status(), StatusCode::UNAUTHORIZED);
                    assert_eq!(r.headers()["cache-control"], "no-store");
                    assert!(!r.headers().contains_key("access-control-allow-origin"));
                }
            }
            let cookie_only = client
                .get(&base)
                .header("cookie", format!("wasmkeep_inspect={secret}"))
                .send()
                .await
                .unwrap();
            assert_eq!(cookie_only.status(), StatusCode::UNAUTHORIZED);
            let r = client
                .get(format!("{base}/?token={secret}"))
                .send()
                .await
                .unwrap();
            assert_eq!(r.status(), StatusCode::OK);
            assert!(!r.headers().contains_key("set-cookie"));
            assert_eq!(r.text().await.unwrap(), PAGE);
            let exchange = client
                .post(format!("{base}/bootstrap"))
                .bearer_auth(&secret)
                .send()
                .await
                .unwrap();
            assert_eq!(exchange.status(), StatusCode::OK);
            assert!(!exchange.headers().contains_key("set-cookie"));
            let session: Value = serde_json::from_slice(&exchange.bytes().await.unwrap()).unwrap();
            let secret = session["bearer"].as_str().unwrap();
            let r = client
                .get(&base)
                .bearer_auth(secret)
                .header("Host", "rebind.example:80")
                .send()
                .await
                .unwrap();
            assert_eq!(r.status(), StatusCode::FORBIDDEN);
            let r = client
                .get(&base)
                .bearer_auth(secret)
                .header("Origin", "https://evil.example")
                .send()
                .await
                .unwrap();
            assert_eq!(r.status(), StatusCode::BAD_REQUEST);
            let r = client
                .post(format!("{base}/api"))
                .bearer_auth(secret)
                .header("Content-Type", "application/json")
                .body(r#"{"op":"classes"}"#)
                .send()
                .await
                .unwrap();
            assert_eq!(r.status(), StatusCode::OK);
            server.abort();
        })
        .await;
}

#[tokio::test]
async fn bucket_listing_pages_are_scoped_to_app_and_class() {
    use object_store::ObjectStoreExt;
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    for i in 0..123 {
        let key = Key::from(format!("state/t/a/actors/room/actor{i:03}/wal/1-1-0.ltx"));
        store.put(&key, Bytes::new().into()).await.unwrap();
    }
    store
        .put(
            &Key::from("state/t/other/actors/hidden/a/wal/1-1-0.ltx"),
            Bytes::new().into(),
        )
        .await
        .unwrap();
    let state = State {
        store,
        prefix: String::new(),
        bucket: "memory".into(),
        app: ("t".into(), "a".into()),
        token: Token::new().unwrap(),
        bootstrap: std::sync::Mutex::new(Some(Token::new().unwrap())),
        address: "127.0.0.1:1".parse().unwrap(),
        work: Arc::new(tokio::sync::Semaphore::new(1)),
    };
    let call = async |input| {
        api(
            &state,
            input,
            state.work.clone().acquire_owned().await.unwrap(),
        )
        .await
        .unwrap()
    };
    let classes = call(json!({"op":"classes"})).await;
    assert_eq!(classes["items"], json!(["room"]));
    let first = call(json!({"op":"actors","class":"room"})).await;
    let second = call(json!({"op":"actors","class":"room","after":first["next"]})).await;
    let third = call(json!({"op":"actors","class":"room","after":second["next"]})).await;
    assert_eq!(first["items"].as_array().unwrap().len(), 50);
    assert_eq!(second["items"][0], "actor050");
    assert_eq!(third["items"].as_array().unwrap().len(), 23);
    assert!(third["next"].is_null());
}

#[tokio::test]
async fn sql_and_history_use_verified_bucket_replay() {
    use object_store::ObjectStoreExt;
    use wasmkeep_storage::{StateDir, Storage, StorageConfig};
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::new(
        StorageConfig::new(dir.path()),
        StateDir {
            dir: "actors".into(),
            scratch: "scratch".into(),
            tenant: "t".into(),
        },
    );
    let mut db = storage.open("room", "a% b").unwrap();
    db.exec("CREATE TABLE items(n INTEGER)", &[]).unwrap();
    db.exec("INSERT INTO items VALUES(42)", &[]).unwrap();
    db.start_capture().unwrap();
    let snapshot = db.snapshot().unwrap().to_ltx(1, 1, 100).unwrap();
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let root = StateRoot::new("prefix", "t", "a");
    let snapshot_key =
        wasmkeep_cluster::replication::snapshot_key(wasmkeep_cluster::replication::SegmentId {
            generation: 1,
            txid: 1,
        });
    store
        .put(
            &Key::from(format!(
                "{}/{snapshot_key}",
                root.bucket_key(Family::History, "room/a%25%20b")
            )),
            snapshot.into(),
        )
        .await
        .unwrap();
    let state = State {
        store,
        prefix: "prefix".into(),
        bucket: "memory".into(),
        app: ("t".into(), "a".into()),
        token: Token::new().unwrap(),
        bootstrap: std::sync::Mutex::new(Some(Token::new().unwrap())),
        address: "127.0.0.1:1".parse().unwrap(),
        work: Arc::new(tokio::sync::Semaphore::new(1)),
    };
    let call = async |input| {
        api(
            &state,
            input,
            state.work.clone().acquire_owned().await.unwrap(),
        )
        .await
    };
    let actors = call(json!({"op":"actors","class":"room"})).await.unwrap();
    assert_eq!(actors["items"], json!(["a%25%20b"]));
    let history = call(json!({"op":"history","class":"room","actor":"a%25%20b"}))
        .await
        .unwrap();
    assert_eq!(history["head"]["txid"], 1);
    assert_eq!(history["retained_objects"], 1);
    let rows =
        call(json!({"op":"sql","class":"room","actor":"a%25%20b","sql":"SELECT n FROM items"}))
            .await
            .unwrap();
    assert_eq!(rows["rows"], json!([["42"]]));
    assert!(
        call(json!({"op":"sql","class":"room","actor":"a%25%20b","sql":"DELETE FROM items"}))
            .await
            .is_err()
    );
}

#[test]
fn csp_authenticates_embedded_script() {
    let r = response(StatusCode::OK, "text/html", PAGE);
    let csp = r.headers()["content-security-policy"].to_str().unwrap();
    assert!(!csp.contains("script-src 'unsafe-inline'"));
    use base64::Engine;
    use sha2::{Digest, Sha256};
    let script = PAGE
        .split_once("<script>")
        .unwrap()
        .1
        .split_once("</script>")
        .unwrap()
        .0;
    let hash = base64::engine::general_purpose::STANDARD.encode(Sha256::digest(script));
    assert!(csp.contains(&format!("script-src 'sha256-{hash}'")));
    assert!(PAGE.contains("history.replaceState"));
    assert!(!PAGE.contains("document.cookie"));
    assert!(!PAGE.contains("localStorage"));
    assert!(PAGE.contains("credentials:'omit'"));
}
#[tokio::test]
async fn bootstrap_cannot_be_reused() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let token = Token::new().unwrap();
            let secret = token.0.clone();
            let state = Arc::new(State {
                store: Arc::new(object_store::memory::InMemory::new()),
                prefix: String::new(),
                bucket: "memory".into(),
                app: ("t".into(), "a".into()),
                bootstrap: std::sync::Mutex::new(Some(Token(secret.clone()))),
                token: Token::new().unwrap(),
                address,
                work: Arc::new(tokio::sync::Semaphore::new(1)),
            });
            let server = tokio::task::spawn_local(async move {
                loop {
                    let (stream, _) = listener.accept().await.unwrap();
                    let state = state.clone();
                    tokio::task::spawn_local(async move {
                        let service = service_fn(move |r| handle(r, state.clone()));
                        let _ = hyper::server::conn::http1::Builder::new()
                            .serve_connection(hyper_util::rt::TokioIo::new(stream), service)
                            .await;
                    });
                }
            });
            let client = reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .unwrap();
            let base = format!("http://{address}");
            for path in ["/", "/api", "/favicon.ico"] {
                for authorization in ["", "Bearer incorrect"] {
                    let r = client
                        .get(format!("{base}{path}"))
                        .header("authorization", authorization)
                        .send()
                        .await
                        .unwrap();
                    assert_eq!(r.status(), StatusCode::UNAUTHORIZED);
                    assert_eq!(r.headers()["cache-control"], "no-store");
                    assert!(!r.headers().contains_key("access-control-allow-origin"));
                }
            }
            let r = client
                .get(format!("{base}/?token={secret}"))
                .send()
                .await
                .unwrap();
            assert!(
                !r.headers().contains_key("set-cookie"),
                "no cookie can be stolen across ports"
            );
            let exchange = client
                .post(format!("{base}/bootstrap"))
                .bearer_auth(&secret)
                .send()
                .await
                .unwrap();
            assert_eq!(exchange.status(), StatusCode::OK);
            assert!(!exchange.headers().contains_key("set-cookie"));
            let reused = client
                .post(format!("{base}/bootstrap"))
                .bearer_auth(&secret)
                .send()
                .await
                .unwrap();
            assert_eq!(reused.status(), StatusCode::UNAUTHORIZED);
            server.abort();
        })
        .await;
}

#[tokio::test]
async fn excess_listing_is_reported() {
    use object_store::ObjectStoreExt;
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    for i in 0..10_001 {
        store
            .put(
                &Key::from(format!("state/t/a/actors/room/a/{i}")),
                Bytes::new().into(),
            )
            .await
            .unwrap();
    }
    let state = State {
        store,
        prefix: String::new(),
        bucket: "memory".into(),
        app: ("t".into(), "a".into()),
        token: Token::new().unwrap(),
        bootstrap: std::sync::Mutex::new(None),
        address: "127.0.0.1:1".parse().unwrap(),
        work: Arc::new(tokio::sync::Semaphore::new(1)),
    };
    for op in ["classes", "actors", "history"] {
        let error = api(
            &state,
            json!({"op": op, "class": "room", "actor": "a"}),
            state.work.clone().acquire_owned().await.unwrap(),
        )
        .await
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("listing exceeds 10000 object limit"),
            "{op}: {error}"
        );
    }
}

#[tokio::test]
async fn hostile_checksum_valid_snapshot_fails_inspect_and_export() {
    use object_store::ObjectStoreExt;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("hostile.db");
    let conn = rusqlite::Connection::open(&path).unwrap();
    conn.execute_batch("CREATE VIEW _wasmkeep_meta AS WITH RECURSIVE n(x) AS (VALUES(0) UNION ALL SELECT x+1 FROM n) SELECT 'replaced' AS key, CAST(sum(x) AS TEXT) AS value FROM n").unwrap();
    let page_size: u32 = conn
        .query_row("PRAGMA page_size", [], |r| r.get(0))
        .unwrap();
    drop(conn);
    let capture = wasmkeep_storage::wal::WalCapture::start(&path, page_size).unwrap();
    let snapshot = capture.snapshot().unwrap().to_ltx(1, 1, 100).unwrap();
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let root = StateRoot::new("", "t", "a");
    let key =
        wasmkeep_cluster::replication::snapshot_key(wasmkeep_cluster::replication::SegmentId {
            generation: 1,
            txid: 1,
        });
    store
        .put(
            &Key::from(format!(
                "{}/{key}",
                root.bucket_key(Family::History, "room/a")
            )),
            snapshot.into(),
        )
        .await
        .unwrap();
    let state = State {
        store,
        prefix: String::new(),
        bucket: "memory".into(),
        app: ("t".into(), "a".into()),
        token: Token::new().unwrap(),
        bootstrap: std::sync::Mutex::new(None),
        address: "127.0.0.1:1".parse().unwrap(),
        work: Arc::new(tokio::sync::Semaphore::new(1)),
    };
    let started = std::time::Instant::now();
    let error = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        api(
            &state,
            json!({"op":"sql", "class":"room", "actor":"a", "sql":"SELECT 1"}),
            state.work.clone().acquire_owned().await.unwrap(),
        ),
    )
    .await
    .expect("inspect deadline")
    .unwrap_err();
    assert!(
        error.to_string().contains("host metadata schema"),
        "{error}"
    );
    // actor sql and actor export both use export_with_options before executing SQL.
    let actor = wasmkeep_runtime::ActorAddress::new("t", "room", "a").unwrap();
    for operation in ["actor-sql", "export"] {
        let output = dir.path().join(operation);
        let error = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            history::export_with_options(
                &state.store,
                &root,
                &actor,
                history::Point::Head,
                &output,
                history::Options {
                    max_bytes: 1 << 20,
                    tmpdir: None,
                    require_owner_match: false,
                },
            ),
        )
        .await
        .expect("export deadline")
        .unwrap_err();
        assert!(
            error.to_string().contains("host metadata schema"),
            "{operation}: {error}"
        );
        assert!(!output.exists());
    }
    assert!(started.elapsed() < std::time::Duration::from_secs(5));
}
