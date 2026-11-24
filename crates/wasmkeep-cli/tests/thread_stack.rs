//! The serving runtime's threads, its blocking pool included, have room for
//! SQLite's own recursion: a statement on a connection the host does not
//! sandbox (or a future SQLite function that recurses) must not abort the
//! node on a 2 MiB stack.

#![allow(clippy::unwrap_used, clippy::expect_used)] // test code

#[test]
fn the_serving_runtime_runs_sqlites_own_deep_like_in_its_blocking_pool() {
    let rt = wasmkeep_cli::serving_runtime().unwrap();
    let matched: i64 = rt.block_on(async {
        tokio::task::spawn_blocking(|| {
            let plain = rusqlite::Connection::open_in_memory().unwrap();
            plain
                .query_row(
                    "SELECT ?2 LIKE ?1",
                    ["%a".repeat(25_000), format!("{}b", "a".repeat(30_000))],
                    |r| r.get(0),
                )
                .unwrap()
        })
        .await
        .unwrap()
    });
    assert_eq!(matched, 0);
}
