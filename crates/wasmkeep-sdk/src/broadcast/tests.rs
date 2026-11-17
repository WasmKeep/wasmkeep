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
}
impl Db {
    fn new() -> Self {
        Self {
            conn: rusqlite::Connection::open_in_memory().unwrap(),
            alarm: None,
            flushes: 0,
            flush_error: None,
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
    fn query(&self, sql: &str) -> Result<Vec<Vec<Value>>, String> {
        let mut stmt = self.conn.prepare(sql).unwrap();
        let n = stmt.column_count();
        let rows = stmt
            .query_map([], |r| {
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
            .unwrap();
        Ok(rows.map(Result::unwrap).collect())
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
        r.history(Some(Position { e: 1, n: 1 }), 100, None)
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
