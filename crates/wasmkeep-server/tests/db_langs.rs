//! The database pattern (ADR 0029) across SDKs: every database class
//! (Rust `examples/db/database`, TypeScript `examples/ts-db` on QuickJS and
//! StarlingMonkey) against every app (Rust `examples/db/app` and the
//! TypeScript one, on both engines), with the same routes and the same
//! answers byte for byte. Covers writes with `RETURNING`, reads, a failed
//! statement and a failed batch leaving nothing behind, a write inside a
//! read rolled back, a commit that fails (a deferred foreign key),
//! transaction control refused, every SQL type round
//! trip (64-bit integers, a whole real, text with controls and an emoji,
//! blobs, null), errors, the answer and statement limits, the schema
//! migrations, and outside clients refused.
//!
//! The Rust classes are built here; the TypeScript ones are prebuilt
//! (`npm run build` and `npm run build:starlingmonkey` in
//! examples/ts-db): a missing one is skipped, or fails with
//! `WASMKEEP_REQUIRE_LANGS=1` (CI).

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::OnceLock;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use wasmkeep_runtime::{Runtime, RuntimeConfig};
use wasmkeep_server::{ActorConfig, ActorService};

#[path = "support/guests.rs"]
mod guests;

/// A class's component: Rust (built once per test binary) or prebuilt
/// TypeScript.
fn component(lang: &str, role: &str) -> Option<Vec<u8>> {
    static RUST: OnceLock<(Vec<u8>, Vec<u8>)> = OnceLock::new();
    if lang == "rust" {
        let (db, app) = RUST.get_or_init(|| {
            (
                guests::build_guest("../../examples/db/database", "db_database"),
                guests::build_guest("../../examples/db/app", "db_app"),
            )
        });
        return Some(if role == "database" { db } else { app }.clone());
    }
    let file = match lang {
        "quickjs" => format!("{role}.wasm"),
        _ => format!("{role}-starlingmonkey.wasm"),
    };
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../examples/ts-db")
        .join(&file);
    match std::fs::read(&path) {
        Ok(bytes) => Some(bytes),
        Err(_) if std::env::var_os("WASMKEEP_REQUIRE_LANGS").is_none() => {
            eprintln!(
                "ts-db: {} not built; skipping (see its README)",
                path.display()
            );
            None
        }
        Err(e) => panic!("ts-db: {}: {e}", path.display()),
    }
}

async fn start(data: &std::path::Path, db: &[u8], app: &[u8]) -> (SocketAddr, oneshot::Sender<()>) {
    let runtime = Runtime::new(&RuntimeConfig {
        max_instances: 32,
        ..RuntimeConfig::default()
    })
    .unwrap();
    let classes = vec![
        ("db".to_owned(), guests::compile(&runtime, db), db.to_vec()),
        (
            "app".to_owned(),
            guests::compile(&runtime, app),
            app.to_vec(),
        ),
    ];
    let svc = ActorService::new(runtime, ActorConfig::new(data), classes).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (stop, stopped) = oneshot::channel::<()>();
    tokio::spawn(wasmkeep_server::serve(listener, svc, async {
        let _ = stopped.await;
    }));
    (addr, stop)
}

async fn request(
    addr: SocketAddr,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    body: &str,
) -> (u16, String) {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let extra: String = headers
        .iter()
        .map(|(k, v)| format!("{k}: {v}\r\n"))
        .collect();
    let req = format!(
        "{method} {path} HTTP/1.1\r\nHost: t\r\n{extra}Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(req.as_bytes()).await.unwrap();
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).await.unwrap();
    let raw = String::from_utf8(raw).unwrap();
    let (head, body) = raw.split_once("\r\n\r\n").unwrap();
    let status = head.split_whitespace().nth(1).unwrap().parse().unwrap();
    let body = if head
        .to_ascii_lowercase()
        .contains("transfer-encoding: chunked")
    {
        dechunk(body)
    } else {
        body.to_owned()
    };
    (status, body)
}

fn dechunk(mut s: &str) -> String {
    let mut out = String::new();
    while let Some((size, rest)) = s.split_once("\r\n") {
        let n = usize::from_str_radix(size.trim(), 16).unwrap_or(0);
        if n == 0 {
            break;
        }
        out.push_str(&rest[..n]);
        s = &rest[n + 2..];
    }
    out
}

fn hex(s: &str) -> String {
    s.bytes().map(|b| format!("{b:02x}")).collect()
}

async fn scenario(db_lang: &str, app_lang: &str) {
    let (Some(db), Some(app)) = (component(db_lang, "database"), component(app_lang, "app")) else {
        return;
    };
    let pair = format!("{app_lang} app, {db_lang} database");
    let data = tempfile::tempdir().unwrap();
    let (addr, _stop) = start(data.path(), &db, &app).await;
    let get =
        async |path: &str| request(addr, "GET", &format!("/actor/app/main{path}"), &[], "").await;
    let post = async |path: &str, body: &str| {
        request(addr, "POST", &format!("/actor/app/main{path}"), &[], body).await
    };
    let ok = |s: &str| (200, s.to_owned());
    let failed = |s: &str| (400, s.to_owned());

    // The first call creates `db/main`, whose init applies the schema.
    assert_eq!(get("/count").await, ok("0\n"), "{pair}");
    assert!(
        data.path()
            .join("state/default/default/actors/db/main.db")
            .exists(),
        "{pair}: one actor holds the database"
    );

    // Writes with RETURNING, then a read.
    assert_eq!(post("/notes", "first").await, ok("1\n"), "{pair}");
    assert_eq!(post("/notes", "second").await, ok("2\n"), "{pair}");
    assert_eq!(get("/notes").await, ok("1 first\n2 second\n"), "{pair}");

    // A statement that fails keeps nothing; a batch is all or none.
    assert_eq!(
        post("/notes", "first").await,
        failed("sql constraint 0\n"),
        "{pair}"
    );
    assert_eq!(
        post("/notes/batch", "a\nb\na\n").await,
        failed("sql constraint 2\n"),
        "{pair}"
    );
    assert_eq!(
        get("/count").await,
        ok("2\n"),
        "{pair}: none of the batch was kept"
    );
    assert_eq!(post("/notes/batch", "a\nb\n").await, ok("2\n"), "{pair}");
    assert_eq!(get("/count").await, ok("4\n"), "{pair}");

    // A write inside a read is rolled back.
    assert_eq!(post("/notes/delete-in-read", "").await, ok("4\n"), "{pair}");
    assert_eq!(
        get("/count").await,
        ok("4\n"),
        "{pair}: the read changed nothing"
    );

    // Transaction control is refused before anything runs.
    assert_eq!(
        post("/sneaky", "").await,
        failed("not-allowed 1\n"),
        "{pair}"
    );
    assert_eq!(
        get("/count").await,
        ok("4\n"),
        "{pair}: the statement before it did not run"
    );

    // A commit that fails (a deferred foreign key) keeps nothing either.
    assert_eq!(
        post("/orphan", "").await,
        failed("sql constraint commit\n"),
        "{pair}"
    );
    assert_eq!(
        (get("/count").await, get("/tags").await),
        (ok("4\n"), ok("0\n")),
        "{pair}"
    );

    // Every SQL type, exactly.
    let expected = format!(
        "a-max integer 9223372036854775807\n\
         b-min integer -9223372036854775808\n\
         c-real real 2\n\
         d-tenth real 0.1\n\
         e-text text {}\n\
         f-blob blob 00ff10\n\
         g-null null null\n\
         h-empty blob \n",
        hex("tab\t\"é\" \u{1f600}\u{1}")
    );
    assert_eq!(get("/types").await, ok(&expected), "{pair}");

    // Errors, and the database's limits (answers of 64 KiB, 10 statements).
    assert_eq!(get("/error").await, failed("sql syntax 0\n"), "{pair}");
    assert_eq!(get("/big").await, failed("too-large\n"), "{pair}");
    assert_eq!(post("/many", "").await, failed("too-large\n"), "{pair}");

    // Outside clients never reach the handler, not even with the header.
    let body = r#"{"statements":[{"sql":"DELETE FROM notes","params":[],"rows":false}]}"#;
    let write = "/actor/db/main/wasmkeep-db/v1/write";
    let (status, answer) = request(addr, "POST", write, &[], body).await;
    assert_eq!(status, 403, "{pair}: {answer}");
    assert!(
        answer.starts_with(r#"{"error":"forbidden","#),
        "{pair}: {answer}"
    );
    let forged = [("x-wasmkeep-caller", "default/app/main")];
    assert_eq!(
        request(addr, "POST", write, &forged, body).await.0,
        403,
        "{pair}: forged"
    );
    assert_eq!(
        get("/count").await,
        ok("4\n"),
        "{pair}: nothing was deleted"
    );
}

macro_rules! pairs {
    ($($name:ident: $db:literal, $app:literal;)*) => {$(
        #[tokio::test(flavor = "multi_thread")]
        async fn $name() {
            scenario($db, $app).await;
        }
    )*};
}

pairs! {
    rust_app_rust_database: "rust", "rust";
    typescript_app_rust_database: "rust", "quickjs";
    rust_app_typescript_database: "quickjs", "rust";
    typescript_app_typescript_database: "quickjs", "quickjs";
    starlingmonkey_app_rust_database: "rust", "starlingmonkey";
    rust_app_starlingmonkey_database: "starlingmonkey", "rust";
    starlingmonkey_app_starlingmonkey_database: "starlingmonkey", "starlingmonkey";
    starlingmonkey_app_typescript_database: "quickjs", "starlingmonkey";
    typescript_app_starlingmonkey_database: "starlingmonkey", "quickjs";
}
