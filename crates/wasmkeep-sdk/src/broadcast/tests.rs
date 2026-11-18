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
fn confirm_precedes_push_and_retries() {
    let mut room = Core::new(Db::new(), true);
    room.init().unwrap();
    room.publish(&[message("a", None)], 0).unwrap();
    room.db.flush_error = Some("not durable: still uploading".into());
    assert!(!room.before_push(20).unwrap());
    assert_eq!(room.db.alarm, Some(70));
    room.db.flush_error = None;
    assert!(room.before_push(70).unwrap());
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
    alarm: Option<u64>,
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
            alarm: None,
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
    fn alarm(&mut self, at: u64) -> Result<(), String> {
        self.alarm = Some(at);
        Ok(())
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
    assert!(r.before_push(0).is_err());
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
        super::super::protocol::serve(&mut room, "publish", &vectors[0], 0).unwrap(),
        vectors[1]
    );
    assert_eq!(
        super::super::protocol::serve(&mut room, "history", &vectors[2], 0).unwrap(),
        vectors[3]
    );
    let messages = room.messages().unwrap();
    assert_eq!(messages[0].id.as_deref(), Some("x"));
    assert_eq!(messages[1].id.as_deref(), Some("\u{feff}x"));
    assert_eq!(messages[1].body, "\u{feff}body");
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
    let slice = room.rows(1000, 513, 512 * 1024).unwrap();
    assert!(slice.len() > 500 && slice.len() <= 513, "{}", slice.len());
    assert!(room.db.rows.get() <= 513 + 32);
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
