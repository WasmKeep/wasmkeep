//! `LIKE` and `GLOB` on guest connections are the host's: the same answers
//! as SQLite's own, and stopped at the statement's budgets.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code

use wasmkeep_storage::{ActorDb, SqlErrorKind, SqlValue as V, StateDir, Storage, StorageConfig};

fn open(dir: &tempfile::TempDir, config: impl FnOnce(&mut StorageConfig)) -> ActorDb {
    let mut c = StorageConfig::new(dir.path());
    config(&mut c);
    let state = StateDir {
        dir: "actors/t".into(),
        scratch: ".deleting".into(),
        tenant: "t".into(),
    };
    Storage::new(c, state).open("c", "a").unwrap()
}

/// `sql`'s one row on a plain connection, as the type and hex of each
/// column; the error's message otherwise.
fn plain_answer(
    plain: &rusqlite::Connection,
    sql: &str,
    args: &[&V],
) -> Result<Vec<String>, String> {
    let params: Vec<rusqlite::types::Value> = args
        .iter()
        .map(|v| rusqlite::types::Value::from(*v))
        .collect();
    let mut stmt = plain.prepare(sql).map_err(|e| e.to_string())?;
    let n = stmt.column_count();
    stmt.query_row(rusqlite::params_from_iter(params), |r| {
        (0..n).map(|i| r.get::<_, String>(i)).collect()
    })
    .map_err(|e| e.to_string())
}

/// The same on the guest's connection.
fn guest_answer(db: &mut ActorDb, sql: &str, args: &[&V]) -> Result<Vec<String>, String> {
    let params: Vec<V> = args.iter().map(|v| (*v).clone()).collect();
    match db.query(sql, &params) {
        Ok(rows) => Ok(rows.values[0]
            .iter()
            .map(|v| match v {
                V::Text(t) => t.clone(),
                other => panic!("{sql}: {other:?}"),
            })
            .collect()),
        Err(e) => Err(e.message),
    }
}

/// Every form (the functions, the operators, `NOT`, `ESCAPE`) over
/// patterns, values and escapes, compared with a plain connection: the
/// result's type and bytes, or the error's message.
fn compare(
    db: &mut ActorDb,
    plain: &rusqlite::Connection,
    patterns: &[V],
    values: &[V],
    escapes: &[V],
) -> usize {
    let mut checked = 0;
    let two = [
        "like(?1, ?2)",
        "?2 LIKE ?1",
        "?2 NOT LIKE ?1",
        "glob(?1, ?2)",
        "?2 GLOB ?1",
        "?2 NOT GLOB ?1",
    ];
    let three = [
        "like(?1, ?2, ?3)",
        "?2 LIKE ?1 ESCAPE ?3",
        "?2 NOT LIKE ?1 ESCAPE ?3",
    ];
    for p in patterns {
        for v in values {
            for form in two {
                let sql = format!("SELECT typeof({form}), hex({form})");
                let expected = plain_answer(plain, &sql, &[p, v]);
                let got = guest_answer(db, &sql, &[p, v]);
                assert_eq!(
                    got.is_ok(),
                    expected.is_ok(),
                    "{sql} with {p:?}, {v:?}: {got:?} {expected:?}"
                );
                if let (Ok(g), Ok(e)) = (&got, &expected) {
                    assert_eq!(g, e, "{sql} with {p:?}, {v:?}");
                } else if let (Err(g), Err(e)) = (&got, &expected) {
                    assert!(g.contains(e.as_str()), "{sql} with {p:?}, {v:?}: {g} / {e}");
                }
                checked += 1;
            }
            for e in escapes {
                for form in three {
                    let sql = format!("SELECT typeof({form}), hex({form})");
                    let expected = plain_answer(plain, &sql, &[p, v, e]);
                    let got = guest_answer(db, &sql, &[p, v, e]);
                    assert_eq!(
                        got.is_ok(),
                        expected.is_ok(),
                        "{sql} with {p:?}, {v:?}, {e:?}: {got:?} {expected:?}"
                    );
                    if let (Ok(g), Ok(x)) = (&got, &expected) {
                        assert_eq!(g, x, "{sql} with {p:?}, {v:?}, {e:?}");
                    } else if let (Err(g), Err(x)) = (&got, &expected) {
                        assert!(
                            g.contains(x.as_str()),
                            "{sql} with {p:?}, {v:?}, {e:?}: {g} / {x}"
                        );
                    }
                    checked += 1;
                }
            }
        }
        db.end_event().unwrap();
    }
    checked
}

fn t(s: &str) -> V {
    V::Text(s.into())
}

/// Text made of raw bytes (invalid UTF-8 included), as a guest can store
/// with `CAST(blob AS TEXT)`.
fn raw(b: &[u8]) -> V {
    V::Blob(b.to_vec())
}

#[test]
fn like_and_glob_answer_as_sqlites_own() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = open(&dir, |_| {});
    let plain = rusqlite::Connection::open_in_memory().unwrap();
    let patterns = [
        V::Null,
        V::Integer(12),
        V::Real(1.5),
        t(""),
        t("%"),
        t("_"),
        t("%%"),
        t("%_"),
        t("_%"),
        t("%_%"),
        t("a%"),
        t("%a"),
        t("%a%"),
        t("A%B"),
        t("a_c"),
        t("abc"),
        t("ABC"),
        t("%b%c"),
        t("x%%_y"),
        t("é%"),
        t("%É"),
        t("%é_"),
        t("ÿ"),
        t("Ÿ"),
        t("[a]"),
        t("[a-c]*"),
        t("*[a-c]"),
        t("*[^a-c]"),
        t("[]]"),
        t("[]-]"),
        t("[^]]"),
        t("[a-]"),
        t("[-a]"),
        t("[z-a]"),
        t("[é-ê]"),
        t("*[é]*"),
        t("[*]"),
        t("[?]"),
        t("[abc"),
        t("*[abc"),
        t("a*b?c"),
        t("**?"),
        t("*?*"),
        t("?"),
        t("???"),
        t("\\%"),
        t("\\_%"),
        t("%\\"),
        t("a\\\\b"),
        t("%\\%%"),
        t("@%`{"),
        t("1%"),
        t("%.5"),
        raw(&[]),
        raw(&[b'a', 0, b'b']),
        raw(&[b'%', 0]),
        raw(&[0, b'%']),
        raw(&[0xc3]),
        raw(&[b'%', 0xc3]),
        raw(&[0x80, b'%']),
        raw(&[0xc0, 0x80]),
        raw(&[0xed, 0xa0, 0x80, b'%']),
        raw(&[0xf8, 0x88, 0x80, 0x80, 0x80]),
        raw(&[b'[', 0xc3, b']']),
        raw(&[b'*', b'[', 0xff, b'-', 0xc3, 0xa9, b']']),
    ];
    let values = [
        V::Null,
        V::Integer(12),
        V::Integer(-3),
        V::Real(1.5),
        V::Real(100.0),
        t(""),
        t("a"),
        t("A"),
        t("abc"),
        t("ABC"),
        t("aBc"),
        t("abcbc"),
        t("x__y"),
        t("x%y"),
        t("é"),
        t("É"),
        t("éa"),
        t("ÿ"),
        t("]"),
        t("-"),
        t("*"),
        t("?"),
        t("%"),
        t("_"),
        t("\\"),
        t("a\\b"),
        t("@`{"),
        t("ê"),
        raw(&[]),
        raw(&[b'a', 0, b'b']),
        raw(&[0, b'a']),
        raw(&[0xc3]),
        raw(&[0xc3, 0xa9, 0x80, 0x80]),
        raw(&[0x80, b'a']),
        raw(&[0xc0, 0x80]),
        raw(&[0xed, 0xa0, 0x80]),
        raw(&[0xff]),
        raw(&[0xf8, 0x88, 0x80, 0x80, 0x80]),
    ];
    let escapes = [
        V::Null,
        t(""),
        t("\\"),
        t("%"),
        t("_"),
        t("a"),
        t("A"),
        t("é"),
        t("ab"),
        V::Integer(1),
        raw(&[b'\\', 0, b'x']),
        raw(&[0xc3]),
        raw(&[0x80]),
    ];
    // The blobs as blobs, and as text with the same bytes.
    let as_text = |vs: &[V]| -> Vec<V> {
        vs.iter()
            .cloned()
            .chain(vs.iter().filter_map(|v| match v {
                V::Blob(b) => Some(V::Text(String::from_utf8_lossy(b).into_owned())),
                _ => None,
            }))
            .collect()
    };
    let checked = compare(
        &mut db,
        &plain,
        &as_text(&patterns),
        &as_text(&values),
        &escapes,
    );
    assert!(checked > 50_000, "{checked}");

    // Invalid UTF-8 as text (not as blobs): through CAST in the statement.
    for (p, v) in [
        (vec![b'%', 0xc3], vec![b'a', 0xc3]),
        (vec![0xc3, b'%'], vec![0xc3, 0xa9]),
        (vec![b'[', 0x80, b']'], vec![0x80]),
        (vec![b'*', 0xed, 0xa0, 0x80], vec![b'x', 0xef, 0xbf, 0xbd]),
    ] {
        for form in [
            "CAST(?2 AS TEXT) LIKE CAST(?1 AS TEXT)",
            "CAST(?2 AS TEXT) GLOB CAST(?1 AS TEXT)",
        ] {
            let sql = format!("SELECT typeof({form}), hex({form})");
            let (p, v) = (V::Blob(p.clone()), V::Blob(v.clone()));
            assert_eq!(
                guest_answer(&mut db, &sql, &[&p, &v]),
                plain_answer(&plain, &sql, &[&p, &v]),
                "{sql} with {p:?}, {v:?}"
            );
        }
    }

    // A seeded random pass over a small alphabet of wildcards, escapes,
    // letters in both cases and multi-byte characters.
    let alphabet: [&[u8]; 14] = [
        b"a",
        b"A",
        b"b",
        b"%",
        b"_",
        b"*",
        b"?",
        b"[",
        b"]",
        b"^",
        b"-",
        b"\\",
        "é".as_bytes(),
        "É".as_bytes(),
    ];
    let mut seed: u64 = 0x2545_f491_4f6c_dd1d;
    let mut next = |n: usize| {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        usize::try_from(seed % n as u64).unwrap()
    };
    let word = |max: usize, next: &mut dyn FnMut(usize) -> usize| {
        let len = next(max + 1);
        let mut out = Vec::new();
        for _ in 0..len {
            out.extend_from_slice(alphabet[next(alphabet.len())]);
        }
        V::Text(String::from_utf8(out).unwrap())
    };
    let mut rp = Vec::new();
    let mut rv = Vec::new();
    for _ in 0..120 {
        rp.push(word(7, &mut next));
        rv.push(word(9, &mut next));
    }
    let checked = compare(
        &mut db,
        &plain,
        &rp,
        &rv,
        &[t("\\"), t("%"), t("_"), t("a")],
    );
    assert!(checked > 100_000, "{checked}");
}

/// The pattern length limit (SQLite's 50,000 bytes) and the escape's
/// single character are checked as SQLite checks them, with its messages.
#[test]
fn like_and_glob_refuse_what_sqlite_refuses() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = open(&dir, |_| {});
    let at_limit = V::Text("a".repeat(50_000));
    let past = V::Text("a".repeat(50_001));
    let rows = db
        .query(
            "SELECT ?1 LIKE ?1, ?1 GLOB ?1",
            std::slice::from_ref(&at_limit),
        )
        .unwrap();
    assert_eq!(rows.values, vec![vec![V::Integer(1), V::Integer(1)]]);
    for sql in [
        "SELECT 'a' LIKE ?1",
        "SELECT 'a' GLOB ?1",
        "SELECT 'a' LIKE ?1 ESCAPE NULL",
    ] {
        let err = db.query(sql, std::slice::from_ref(&past)).unwrap_err();
        assert!(
            err.message.contains("LIKE or GLOB pattern too complex"),
            "{sql}: {err:?}"
        );
    }
    for esc in ["''", "'ab'"] {
        let err = db
            .query(&format!("SELECT 'a' LIKE 'a' ESCAPE {esc}"), &[])
            .unwrap_err();
        assert!(
            err.message
                .contains("ESCAPE expression must be a single character"),
            "{err:?}"
        );
    }
    // Case-sensitive LIKE stays off: the pragma that would switch it (and
    // put SQLite's own functions back) is refused.
    let err = db.exec("PRAGMA case_sensitive_like = ON", &[]).unwrap_err();
    assert_eq!(err.kind, SqlErrorKind::NotAllowed, "{err:?}");
    let rows = db.query("SELECT 'a' LIKE 'A'", &[]).unwrap();
    assert_eq!(rows.values, vec![vec![V::Integer(1)]]);
}

/// Patterns with as many wildcards as the length limit allows answer as
/// SQLite does on a thread with a small stack: the host's match does not
/// recurse.
#[test]
fn the_deepest_patterns_need_no_deep_stack() {
    let a = "a".repeat(30_000);
    let cases = [
        ("%a".repeat(25_000), format!("{a}b"), "LIKE"),
        ("%a".repeat(25_000), a.clone(), "LIKE"),
        ("*a".repeat(25_000), format!("{a}b"), "GLOB"),
        ("*[a]".repeat(12_500), format!("{a}b"), "GLOB"),
        ("*[a]".repeat(12_500), a.clone(), "GLOB"),
    ];
    let expected: Vec<V> = std::thread::Builder::new()
        .stack_size(256 << 20)
        .spawn({
            let cases = cases.clone();
            move || {
                let plain = rusqlite::Connection::open_in_memory().unwrap();
                cases
                    .iter()
                    .map(|(p, v, op)| {
                        V::Integer(
                            plain
                                .query_row(&format!("SELECT ?2 {op} ?1"), [p, v], |r| r.get(0))
                                .unwrap(),
                        )
                    })
                    .collect()
            }
        })
        .unwrap()
        .join()
        .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let got: Vec<V> = std::thread::Builder::new()
        .stack_size(256 << 10)
        .spawn({
            let path = dir.path().to_owned();
            move || {
                let state = StateDir {
                    dir: "actors/t".into(),
                    scratch: ".deleting".into(),
                    tenant: "t".into(),
                };
                let mut db = Storage::new(StorageConfig::new(path), state)
                    .open("c", "a")
                    .unwrap();
                cases
                    .iter()
                    .map(|(p, v, op)| {
                        let rows = db
                            .query(
                                &format!("SELECT ?2 {op} ?1"),
                                &[V::Text(p.clone()), V::Text(v.clone())],
                            )
                            .unwrap();
                        rows.values[0][0].clone()
                    })
                    .collect()
            }
        })
        .unwrap()
        .join()
        .unwrap();
    assert_eq!(got, expected);
}

/// A match that takes SQLite's own `LIKE` or `GLOB` minutes (a leading
/// wildcard, then a long literal that fails at its last character, over
/// a long value) stops at the event's SQL CPU budget.
#[test]
fn a_slow_like_or_glob_stops_at_the_sql_cpu_budget() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = open(&dir, |c| {
        c.max_sql_cpu_per_event = std::time::Duration::from_millis(100);
    });
    for sql in [
        "SELECT printf('%.*c', 4000000, 'a') LIKE '%' || printf('%.*c', 40000, 'a') || 'b'",
        "SELECT printf('%.*c', 4000000, 'a') LIKE '%' || printf('%.*c', 40000, 'a') || '!b' ESCAPE '!'",
        "SELECT printf('%.*c', 4000000, 'a') GLOB '*' || printf('%.*c', 40000, 'a') || 'b'",
        "SELECT printf('%.*c', 4000000, 'a') GLOB '*[a]' || printf('%.*c', 40000, 'a') || 'b'",
        "SELECT replace(printf('%.*c', 2000000, 'a'), 'a', 'é') LIKE '%' || replace(printf('%.*c', 20000, 'a'), 'a', 'é') || 'b'",
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
