#![allow(clippy::unwrap_used, clippy::panic)]
use super::*;
#[test]
fn numbering_cold_start_and_void() {
    let db = Db::new();
    let mut room = Core::new(db, false);
    room.init().unwrap();
    assert_eq!(
        room.publish(&[message("a", Some("id"))], 100).unwrap(),
        vec![Position { e: 1, n: 1 }]
    );
    assert_eq!(
        room.publish(&[message("retry", Some("id"))], 101).unwrap()[0].n,
        1
    );
    room.publish(&[message("lost", None)], 102).unwrap();
    room.db
        .exec("DELETE FROM wasmkeep_room_history WHERE n=2", &[], false)
        .unwrap();
    let mut room = Core::new(room.db, false);
    let h = room
        .history(Some(Position { e: 1, n: 2 }), 100, None)
        .unwrap();
    assert_eq!(h.lost, Some(Epoch { e: 2, base: 1 }));
    assert_eq!(
        room.publish(&[message("replacement", None)], 103).unwrap()[0],
        Position { e: 2, n: 2 }
    );
    let mut room = Core::new(room.db, false);
    assert_eq!(
        room.publish(&[message("clean", None)], 104).unwrap()[0],
        Position { e: 3, n: 3 }
    );
}
#[test]
fn window_gaps_reset_and_head_only() {
    let mut room = Core::new(Db::new(), false);
    room.init().unwrap();
    for _ in 0..2002 {
        room.publish(&[message("x", None)], 0).unwrap();
    }
    assert!(room.history(None, 100, None).unwrap().messages.is_empty());
    let h = room
        .history(Some(Position { e: 1, n: 1 }), 2, None)
        .unwrap();
    assert_eq!(h.reset, Some(Position { e: 1, n: 3 }));
    assert_eq!(h.messages.len(), 2);
    assert!(h.more);
    assert_eq!(
        room.history(Some(Position { e: 1, n: 2001 }), 100, None)
            .unwrap()
            .messages[0]
            .position
            .n,
        2002
    );
    assert_eq!(room.history(None, 100, Some(2)).unwrap().messages.len(), 2);
}
#[test]
fn confirm_a_pull_flushes_first_and_refuses_until_the_upload_is_in() {
    let mut room = Core::new(Db::new(), true);
    room.init().unwrap();
    room.publish(&[message("a", None)], 0).unwrap();
    room.db.flush_error = Some("not durable: still uploading".into());
    let at = Some(Position { e: 1, n: 0 });
    assert!(
        room.pull(at, 1 << 20, false, 20)
            .unwrap_err()
            .contains("not durable")
    );
    room.db.flush_error = None;
    assert_eq!(room.pull(at, 1 << 20, false, 70).unwrap().messages.len(), 1);
    assert_eq!(room.db.flushes, 2);
}
fn message(body: &str, id: Option<&str>) -> Publish {
    Publish {
        body: body.into(),
        id: id.map(str::to_owned),
    }
}
struct Db {
    conn: rusqlite::Connection,
    flushes: usize,
    flush_error: Option<String>,
    /// Queries run and rows they returned.
    read: std::cell::Cell<usize>,
    rows: std::cell::Cell<usize>,
}
impl Db {
    fn new() -> Self {
        Self {
            conn: rusqlite::Connection::open_in_memory().unwrap(),
            flushes: 0,
            flush_error: None,
            read: Default::default(),
            rows: Default::default(),
        }
    }
}
impl Store for Db {
    fn exec(&mut self, sql: &str, p: &[Value], _: bool) -> Result<(), String> {
        self.conn
            .execute(sql, rusqlite::params_from_iter(p.iter().map(param)))
            .map(|_| ())
            .map_err(|e| e.to_string())
    }
    fn query(&self, sql: &str, p: &[Value]) -> Result<Vec<Vec<Value>>, String> {
        self.read.set(self.read.get() + 1);
        let mut stmt = self.conn.prepare(sql).unwrap();
        let n = stmt.column_count();
        let rows: Vec<Vec<Value>> = stmt
            .query_map(rusqlite::params_from_iter(p.iter().map(param)), |r| {
                Ok((0..n)
                    .map(|i| match r.get_ref(i).unwrap() {
                        rusqlite::types::ValueRef::Integer(v) => Value::Integer(v),
                        rusqlite::types::ValueRef::Text(v) => {
                            Value::Text(String::from_utf8(v.to_vec()).unwrap())
                        }
                        rusqlite::types::ValueRef::Null => Value::Null,
                        rusqlite::types::ValueRef::Blob(v) => Value::Blob(v.to_vec()),
                        _ => panic!("unexpected value"),
                    })
                    .collect())
            })
            .unwrap()
            .map(Result::unwrap)
            .collect();
        self.rows.set(self.rows.get() + rows.len());
        Ok(rows)
    }
    fn flush(&mut self) -> Result<(), String> {
        self.flushes += 1;
        self.flush_error.clone().map_or(Ok(()), Err)
    }
}
fn param(v: &Value) -> rusqlite::types::Value {
    match v {
        Value::Integer(n) => (*n).into(),
        Value::Text(s) => s.clone().into(),
        Value::Blob(b) => b.clone().into(),
        _ => rusqlite::types::Value::Null,
    }
}
#[test]
fn shared_history_vectors_and_epoch_retention() {
    for line in include_str!("history-vectors.txt")
        .lines()
        .filter(|s| !s.starts_with('#'))
    {
        let v: Vec<u64> = line
            .split_whitespace()
            .map(|n| n.parse().unwrap())
            .collect();
        let mut r = Core::new(Db::new(), false);
        r.init().unwrap();
        for _ in 0..5 {
            r.publish(&[message("a", None)], 0).unwrap();
        }
        r.db.exec("DELETE FROM wasmkeep_room_history WHERE n>4", &[], false)
            .unwrap();
        r = Core::new(r.db, false);
        r.history(None, 100, None).unwrap();
        r.db.exec("DELETE FROM wasmkeep_room_history WHERE n>2", &[], false)
            .unwrap();
        r = Core::new(r.db, false);
        r.publish(&[message("new", None)], 0).unwrap();
        let h = r
            .history(Some(Position { e: v[0], n: v[1] }), 100, None)
            .unwrap();
        assert_eq!(
            h.lost.map(|e| (e.e, e.base)).unwrap_or((0, 0)),
            (v[2], v[3])
        );
        assert_eq!(h.messages.first().map_or(0, |m| m.position.n), v[4]);
        assert_eq!(u64::from(h.more), v[5]);
    }
    let mut r = Core::new(Db::new(), false);
    r.init().unwrap();
    r.publish(&[message("a", None)], 0).unwrap();
    for _ in 0..65 {
        r = Core::new(r.db, false);
        r.history(None, 100, None).unwrap();
    }
    assert_eq!(r.epochs().unwrap().len(), 64);
    assert!(
        r.history(Some(Position { e: 1, n: 0 }), 100, None)
            .unwrap()
            .reset
            .is_some()
    );
}
#[test]
fn byte_window_and_limits_are_atomic() {
    let mut r = Core::new(Db::new(), false);
    r.init().unwrap();
    let big = "é".repeat(32768);
    for _ in 0..129 {
        r.publish(&[message(&big, None)], 0).unwrap();
    }
    let h = r.history(None, 2000, Some(2000)).unwrap();
    assert_eq!(h.messages[0].position.n, 2);
    assert_eq!(h.messages.len(), 3);
    assert!(h.more);
    assert!(
        r.publish(
            &[message("valid", None), message(&format!("{big}x"), None)],
            0
        )
        .is_err()
    );
    assert_eq!(r.history(None, 100, None).unwrap().head.n, 129);
    r.db.flush_error = Some("fatal storage failure".into());
    r.confirm = true;
    assert!(r.pull(None, 0, false, 0).is_err());
}

#[test]
fn four_retained_messages_after_64_cold_wakes() {
    let mut r = Core::new(Db::new(), false);
    r.init().unwrap();
    r.publish(&vec![message(&"x".repeat(65536), None); 4], 0)
        .unwrap();
    for _ in 0..64 {
        r = Core::new(r.db, false);
        r.wake().unwrap();
    }
    let first = r
        .history(Some(Position { e: 1, n: 0 }), 2000, None)
        .unwrap();
    assert!(first.reset.is_some() && first.more);
    assert_eq!(first.messages.len(), 3);
    let second = r
        .history(Some(first.messages[2].position), 2000, None)
        .unwrap();
    assert!(second.reset.is_none());
    assert!(!second.more);
    assert_eq!(
        second
            .messages
            .iter()
            .map(|m| m.position.n)
            .collect::<Vec<_>>(),
        vec![4]
    );
}

#[cfg(feature = "actor")]
#[test]
fn unicode_room_wire_vectors_for_typescript_front() {
    let vectors: Vec<Vec<u8>> = include_str!("unicode-vectors.txt")
        .lines()
        .map(|s| {
            (0..s.len())
                .step_by(2)
                .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
                .collect()
        })
        .collect();
    let mut room = Core::new(Db::new(), false);
    room.init().unwrap();
    assert_eq!(
        super::super::protocol::serve(&mut room, "publish", &vectors[0], 0, "app/front", "")
            .unwrap(),
        vectors[1]
    );
    assert_eq!(
        super::super::protocol::serve(&mut room, "history", &vectors[2], 0, "app/front", "")
            .unwrap(),
        vectors[3]
    );
    // The browser's page, as the TypeScript front's JSON.stringify writes it.
    assert_eq!(
        super::super::protocol::serve(&mut room, "history-json", &vectors[4], 0, "app/front", "")
            .unwrap(),
        vectors[5]
    );
    let messages = room.messages().unwrap();
    assert_eq!(messages[0].id.as_deref(), Some("x"));
    assert_eq!(messages[1].id.as_deref(), Some("\u{feff}x"));
    assert_eq!(messages[1].body, "\u{feff}body");
}
/// `JSON.stringify` of a string, the reference `history-json` must match:
/// `"`, `\` and controls escaped, short forms for \b \t \n \f \r, the
/// other controls as lowercase `\u00xx`, everything else as it is.
fn stringify(s: &str) -> String {
    let mut out = String::from('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\u{8}' => out.push_str("\\b"),
            '\t' => out.push_str("\\t"),
            '\n' => out.push_str("\\n"),
            '\u{c}' => out.push_str("\\f"),
            '\r' => out.push_str("\\r"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}
/// The browser JSON of `h`, as the TypeScript front's
/// `JSON.stringify(historyJSON(h))` writes it.
fn browser(h: &History) -> String {
    let p = |p: Position| format!(r#"{{"e":{},"n":{}}}"#, p.e, p.n);
    let mut out = format!(r#"{{"head":{}"#, p(h.head));
    if let Some(l) = h.lost {
        out += &format!(r#","lost":{{"e":{},"base":{}}}"#, l.e, l.base);
    }
    if let Some(r) = h.reset {
        out += &format!(r#","reset":{}"#, p(r));
    }
    out += &format!(r#","more":{},"messages":["#, h.more);
    let messages: Vec<String> = h
        .messages
        .iter()
        .map(|m| {
            let id =
                m.id.as_deref()
                    .map_or(String::new(), |id| format!(r#","id":{}"#, stringify(id)));
            format!(
                r#"{{"position":{},"at":{}{id},"body":{}}}"#,
                p(m.position),
                m.at,
                stringify(&m.body)
            )
        })
        .collect();
    out + &messages.join(",") + "]}"
}
#[test]
fn history_json_is_the_browsers_json_byte_for_byte() {
    let mut room = Core::new(Db::new(), false);
    room.init().unwrap();
    let mut bodies: Vec<String> = (0..0x100u32)
        .step_by(16)
        .map(|c| (c..c + 16).filter_map(char::from_u32).collect())
        .collect();
    for s in [
        "\"\\/",
        "\u{2028}\u{2029}",
        "\u{feff}body",
        "\u{1f600}\u{10ffff}",
        &"\u{20ac}".repeat(100),
        "",
    ] {
        bodies.push(s.into());
    }
    let input: Vec<Publish> = bodies
        .iter()
        .enumerate()
        .map(|(i, b)| Publish {
            body: b.clone(),
            id: (i % 3 != 0 && !b.is_empty()).then(|| b.chars().take(16).collect()),
        })
        .collect();
    room.publish(&input, 1_700_000_000_000).unwrap();
    // Bodies and ids are bound as TEXT: `json_quote` refuses a BLOB, so a
    // BLOB row would make every page that holds it fail.
    let kinds = room
        .db
        .query(
            "SELECT DISTINCT typeof(body), coalesce(typeof(id),'') FROM wasmkeep_room_history",
            &[],
        )
        .unwrap();
    assert!(
        kinds.iter().all(
            |r| r[0].as_str() == Some("text") && matches!(r[1].as_str(), Some("text" | "null"))
        ),
        "{kinds:?}"
    );
    for after in [
        None,
        Some(Position { e: 1, n: 0 }),
        Some(Position { e: 1, n: 5 }),
    ] {
        let json = room.history_json(after, 256).unwrap();
        let want = browser(&room.history(after, 256, None).unwrap());
        assert_eq!(String::from_utf8(json).unwrap(), want);
    }
    // A crash voids numbers (lost), then the window moves past the cursor (reset).
    room.db
        .exec("DELETE FROM wasmkeep_room_history WHERE n > 20", &[], false)
        .unwrap();
    let mut room = Core::new(room.db, false);
    room.publish(&[message("after the crash", None)], 1)
        .unwrap();
    let at = Some(Position { e: 1, n: 22 });
    let lost = String::from_utf8(room.history_json(at, 256).unwrap()).unwrap();
    assert!(lost.contains(r#""lost":{"e":2,"base":20}"#), "{lost}");
    assert_eq!(lost, browser(&room.history(at, 256, None).unwrap()));
    for _ in 0..2000 {
        room.publish(&[message("x", None)], 2).unwrap();
    }
    let at = Some(Position { e: 1, n: 1 });
    let reset = String::from_utf8(room.history_json(at, 256).unwrap()).unwrap();
    assert!(reset.contains(r#""reset":"#));
    assert_eq!(reset, browser(&room.history(at, 256, None).unwrap()));
}
#[test]
fn history_json_pages_256_rows_64_kib_at_least_one_row() {
    let mut room = Core::new(Db::new(), false);
    room.init().unwrap();
    for _ in 0..300 {
        room.publish(&[message(&"y".repeat(193), None)], 0).unwrap();
    }
    let page = |room: &mut Core<Db>, after: u64, limit: usize| {
        let json = room
            .history_json(Some(Position { e: 1, n: after }), limit)
            .unwrap();
        let json = String::from_utf8(json).unwrap();
        (
            json.matches(r#"{"position""#).count(),
            json.contains(r#""more":true"#),
        )
    };
    assert_eq!(page(&mut room, 0, 256), (256, true));
    assert_eq!(
        page(&mut room, 0, 2000),
        (256, true),
        "at most 256 rows whatever the limit"
    );
    assert_eq!(page(&mut room, 0, 32), (32, true));
    assert_eq!(page(&mut room, 256, 256), (44, false));
    room.publish(
        &(0..40)
            .map(|_| message(&"z".repeat(4096), None))
            .collect::<Vec<_>>(),
        0,
    )
    .unwrap();
    assert_eq!(
        page(&mut room, 300, 256),
        (16, true),
        "16 bodies of 4 KiB in 64 KiB"
    );
    room.publish(&[message(&"w".repeat(65536), None)], 0)
        .unwrap();
    room.publish(&[message(&"v".repeat(65536), Some("big"))], 0)
        .unwrap();
    assert_eq!(page(&mut room, 340, 256), (1, true));
    assert_eq!(page(&mut room, 341, 256), (1, false));
    assert!(room.history_json(Some(Position { e: 1, n: 0 }), 0).is_err());
    room.publish(&[message("late", None)], 1 << 53).unwrap();
    assert!(
        room.history_json(Some(Position { e: 1, n: 342 }), 256)
            .unwrap_err()
            .contains("invalid browser integer")
    );
}
/// A full window and full history pages cost each event what it answers,
/// not the window (TypeScript has the same test).
#[test]
fn per_event_reads_are_bounded_by_the_answer() {
    let mut room = Core::new(Db::new(), false);
    room.init().unwrap();
    room.wake().unwrap();
    room.db.exec("BEGIN", &[], false).unwrap();
    for n in 1..=2000u64 {
        room.db
            .exec(
                "INSERT INTO wasmkeep_room_history(n,e,at,id,body) VALUES(?1,1,0,?2,?3)",
                &[
                    Value::Integer(n as i64),
                    format!("id{n}").into(),
                    "x".repeat(1024).into(),
                ],
                false,
            )
            .unwrap();
    }
    room.db.exec("COMMIT", &[], false).unwrap();
    room.db.rows.set(0);
    assert_eq!(
        room.publish(&[message("new", Some("fresh"))], 1).unwrap(),
        vec![Position { e: 1, n: 2001 }]
    );
    assert_eq!(
        room.publish(&[message("again", Some("id1500"))], 2)
            .unwrap(),
        vec![Position { e: 1, n: 1500 }]
    );
    assert!(
        room.db.rows.get() <= 8,
        "publish read {} rows",
        room.db.rows.get()
    );
    let count = room
        .db
        .query("SELECT count(*), min(n) FROM wasmkeep_room_history", &[])
        .unwrap();
    assert_eq!(count[0][0].as_i64(), Some(2000));
    assert_eq!(count[0][1].as_i64(), Some(2));
    room.db.rows.set(0);
    let h = room
        .history(Some(Position { e: 1, n: 0 }), 2000, None)
        .unwrap();
    assert!(h.more && h.reset.is_some() && h.messages.len() > 200 && h.messages.len() < 260);
    assert!(
        room.db.rows.get() <= h.messages.len() + 80,
        "history read {}",
        room.db.rows.get()
    );
    room.db.rows.set(0);
    let last = room.history(None, 2000, Some(3)).unwrap();
    assert_eq!(
        last.messages
            .iter()
            .map(|m| m.position.n)
            .collect::<Vec<_>>(),
        vec![1999, 2000, 2001]
    );
    assert!(room.db.rows.get() <= 80);
    room.db.rows.set(0);
    let slice = room
        .pull(Some(Position { e: 1, n: 999 }), 1 << 20, false, 0)
        .unwrap();
    assert!(
        slice.messages.len() > 490 && slice.messages.len() <= 513,
        "{}",
        slice.messages.len()
    );
    assert!(room.db.rows.get() <= 513 + 32 + 70);
    // The byte window: 8 MiB of bodies, oldest first.
    let big: Vec<_> = (0..4).map(|_| message(&"z".repeat(65536), None)).collect();
    for _ in 0..33 {
        room.publish(&big, 3).unwrap();
    }
    let bytes = room
        .db
        .query(
            "SELECT sum(length(CAST(body AS BLOB))) FROM wasmkeep_room_history",
            &[],
        )
        .unwrap()[0][0]
        .as_i64()
        .unwrap();
    assert!(
        bytes <= 8 * 1024 * 1024 && bytes > 8 * 1024 * 1024 - 65536,
        "{bytes}"
    );
}
/// The room's pull (ADR 0040, section 13), shared with TypeScript.
#[test]
fn shared_pull_vectors() {
    let text = include_str!("pull-vectors.txt");
    let scenarios: Vec<Vec<&str>> = text
        .split("\n\n")
        .map(|s| {
            s.lines()
                .filter(|l| !l.trim().is_empty() && !l.starts_with('#'))
                .collect()
        })
        .filter(|s: &Vec<&str>| !s.is_empty())
        .collect();
    assert!(scenarios.len() >= 5);
    for lines in scenarios {
        let mut core = Core::new(Db::new(), false);
        core.init().unwrap();
        let mut now = 0;
        for line in lines {
            let a: Vec<&str> = line.split(' ').collect();
            let int = |s: &str| s.parse::<u64>().unwrap();
            match a[0] {
                "publish" => {
                    for _ in 0..int(a[1]) {
                        let size = a.get(2).map_or(1, |s| int(s)) as usize;
                        core.publish(&[message(&"x".repeat(size), None)], now)
                            .unwrap();
                    }
                }
                "lose" | "trim" | "epochdrop" => {
                    let sql = match a[0] {
                        "lose" => "DELETE FROM wasmkeep_room_history WHERE n>?1",
                        "trim" => "DELETE FROM wasmkeep_room_history WHERE n<=?1",
                        _ => "DELETE FROM wasmkeep_room_epochs WHERE e=?1",
                    };
                    core.db
                        .exec(sql, &[Value::Integer(int(a[1]) as i64)], false)
                        .unwrap();
                }
                "restart" => core = Core::new(core.db, false),
                "clock" => now = int(a[1]),
                "poke" => assert_eq!(u64::from(core.poke(now)), int(a[1]), "{line}"),
                "pull" => {
                    let after = (a[1] != "-").then(|| Position {
                        e: int(a[1]),
                        n: int(a[2]),
                    });
                    let p = core
                        .pull(after, int(a[3]) as usize, a[4] == "1", now)
                        .unwrap();
                    let last = p.messages.last();
                    let got = [
                        p.e,
                        p.base,
                        p.first,
                        p.head,
                        u64::from(p.recover),
                        p.messages.len() as u64,
                        last.map_or(0, |(m, _)| m.position.e),
                        last.map_or(0, |(_, b)| *b),
                    ]
                    .map(|n| n.to_string())
                    .join(" ");
                    assert_eq!(got, a[6..].join(" "), "{line}");
                    for (i, (m, _)) in p.messages.iter().enumerate() {
                        assert_eq!(m.position.n, p.first + i as u64);
                    }
                }
                _ => panic!("unknown vector command {line}"),
            }
        }
    }
}
/// A pull reads what it answers, never the window (TypeScript has the same test).
#[test]
fn a_pull_reads_what_it_answers_whatever_the_position() {
    let mut room = Core::new(Db::new(), false);
    room.init().unwrap();
    room.wake().unwrap();
    room.db.exec("BEGIN", &[], false).unwrap();
    for n in 1..=2000u64 {
        let body = if n % 2 == 1 {
            String::new()
        } else {
            "x".repeat(4096)
        };
        room.db
            .exec(
                "INSERT INTO wasmkeep_room_history(n,e,at,id,body) VALUES(?1,1,0,NULL,?2)",
                &[Value::Integer(n as i64), body.into()],
                false,
            )
            .unwrap();
    }
    room.db.exec("COMMIT", &[], false).unwrap();
    for (after, budget) in [
        (0, 1 << 20),
        (999, 1 << 20),
        (1990, 1 << 20),
        (0, 0),
        (1500, 1),
        (2000, 1 << 20),
    ] {
        room.db.rows.set(0);
        let p = room
            .pull(Some(Position { e: 1, n: after }), budget, false, 0)
            .unwrap();
        assert!(p.messages.len() <= 513);
        assert!(
            room.db.rows.get() <= p.messages.len() + 32 + 70,
            "pull after {after} read {} rows for {}",
            room.db.rows.get(),
            p.messages.len()
        );
        let bytes: usize = p.messages.iter().map(|(m, _)| m.body.len()).sum();
        assert!(bytes <= 512 * 1024 + 65536, "{bytes} bytes");
    }
    assert!(
        room.pull(Some(Position { e: 1, n: 0 }), (1 << 20) + 1, false, 0)
            .is_err()
    );
    assert!(
        room.pull(Some(Position { e: 1, n: 2001 }), 0, false, 0)
            .is_err()
    );
    assert!(
        room.pull(Some(Position { e: 2, n: 0 }), 0, false, 0)
            .is_err()
    );
}
/// pull and adopt answer the room's pusher only; the publish answer carries
/// the idle flag a front pokes on.
#[cfg(feature = "actor")]
#[test]
fn pull_answers_the_pusher_only_and_publish_carries_the_poke_flag() {
    use crate::wire::{Reader, Writer};
    let mut room = Core::new(Db::new(), false);
    room.init().unwrap();
    let mut pull = Writer::default();
    pull.u64(0).u64(0).u64(0);
    let pull = pull.finish();
    for caller in ["t/front/main", "t/edge/r.e1", "t/pusher/other"] {
        assert!(
            super::super::protocol::serve(&mut room, "pull", &pull, 0, caller, "t/pusher/r")
                .unwrap_err()
                .starts_with("forbidden")
        );
    }
    let mut publish = Writer::default();
    publish.u64(1).u64(0).bytes(b"m");
    let publish = publish.finish();
    let flag = |answer: Vec<u8>| {
        let mut r = Reader::new(&answer);
        r.u64();
        r.u64();
        r.u64();
        r.u64()
    };
    let serve = |room: &mut Core<Db>, op: &str, body: &[u8]| {
        super::super::protocol::serve(room, op, body, 0, "t/pusher/r", "t/pusher/r").unwrap()
    };
    assert_eq!(flag(serve(&mut room, "publish", &publish)), Some(1), "cold");
    serve(&mut room, "pull", &pull);
    assert_eq!(flag(serve(&mut room, "publish", &publish)), Some(0), "busy");
    let mut idle = Writer::default();
    idle.u64(1).u64(1).u64(2).u64(0).u64(1);
    serve(&mut room, "pull", &idle.finish());
    assert_eq!(flag(serve(&mut room, "publish", &publish)), Some(1), "idle");
}
