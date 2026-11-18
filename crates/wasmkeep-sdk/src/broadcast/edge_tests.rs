#![allow(clippy::unwrap_used, clippy::panic)]
use super::*;
#[derive(Default)]
struct Io {
    blocked: bool,
    frames: Vec<String>,
    closed: Vec<u16>,
}
impl SocketIo for Io {
    fn send(&mut self, _: u64, text: &str) -> SendResult {
        self.frames.push(text.into());
        if self.blocked {
            SendResult::Backpressure
        } else {
            SendResult::Ok
        }
    }
    fn close(&mut self, _: u64, code: u16) {
        self.closed.push(code);
    }
}
fn push(n: u64) -> Push {
    Push {
        recover: false,
        e: 1,
        base: 0,
        first: n,
        head: n,
        messages: vec![Item {
            epoch: None,
            at: 0,
            id: None,
            body: "hello".into(),
        }],
    }
}
#[test]
fn cursor_buffer_overflow_behind_then_close() {
    let mut edge = Edge::default();
    let mut io = Io::default();
    edge.push(
        &Push {
            messages: vec![],
            head: 0,
            ..push(1)
        },
        &mut io,
    );
    assert!(edge.open(1, None, &mut io));
    edge.push(&push(1), &mut io);
    let count = io.frames.len();
    edge.push(&push(1), &mut io);
    assert_eq!(count, io.frames.len());
    io.blocked = true;
    for n in 2..=514 {
        edge.push(&push(n), &mut io);
    }
    assert_eq!(edge.buffer.len(), 512);
    assert!(io.frames.last().unwrap().contains("behind"));
    assert_eq!(io.closed, vec![4802]);
}
#[test]
fn epoch_rollback_replaces_reused_number() {
    let mut edge = Edge::default();
    let mut io = Io::default();
    edge.push(
        &Push {
            messages: vec![],
            head: 0,
            ..push(1)
        },
        &mut io,
    );
    edge.open(1, None, &mut io);
    edge.push(&push(1), &mut io);
    edge.push(&push(2), &mut io);
    edge.push(
        &Push {
            e: 2,
            base: 1,
            ..push(2)
        },
        &mut io,
    );
    assert!(io.frames.last().unwrap().contains("\"e\":2"));
}
#[test]
fn retained_epoch_identity_and_invalid_payloads() {
    let mut edge = Edge::default();
    let mut io = Io::default();
    edge.push(
        &Push {
            messages: vec![],
            head: 0,
            ..push(1)
        },
        &mut io,
    );
    edge.open(1, None, &mut io);
    let p = Push::decode(r#"{"e":2,"b":1,"first":1,"head":2,"messages":[{"a":0,"d":"old","e":1,"b":0},{"a":0,"d":"new","e":2,"b":1}]}"#).unwrap();
    edge.push(&p, &mut io);
    edge.ping(1, &mut io);
    let frames: Vec<_> = io
        .frames
        .iter()
        .filter(|s| s.contains("\"t\":\"m\""))
        .collect();
    assert_eq!(frames.len(), 2);
    assert!(frames[0].contains("\"e\":1"));
    assert!(frames[1].contains("\"e\":2"));
    assert!(
        Push::decode(r#"{"e":1,"b":0,"first":1,"head":1,"messages":[{"a":0,"d":"x","i":42}]}"#)
            .is_err()
    );
}
#[test]
fn capacity_and_counters_reset_per_answer() {
    let mut e = Edge::default();
    let mut io = Io::default();
    let header = Push {
        messages: vec![],
        head: 0,
        ..push(1)
    };
    e.push(&header, &mut io);
    for id in 0..500 {
        assert!(e.open(id, None, &mut io));
    }
    assert!(!e.open(500, None, &mut io));
    let a = e.push(&header, &mut io).encode();
    assert!(a.contains("\"opens\":500"));
    assert!(a.contains("\"full\":1"));
    let a = e.push(&header, &mut io).encode();
    assert!(a.contains("\"opens\":0"));
    assert!(a.contains("\"full\":0"));
}
#[test]
fn shared_push_vectors() {
    for line in include_str!("push-vectors.txt")
        .lines()
        .filter(|s| !s.starts_with('#'))
    {
        let (ok, json) = line.split_once(' ').unwrap();
        assert_eq!(Push::decode(json).is_ok(), ok == "1", "{json}");
    }
}
#[test]
fn oversized_frame_recovers_over_http_and_resumes_live() {
    let mut e = Edge::default();
    let mut io = Io::default();
    e.push(
        &Push {
            messages: vec![],
            head: 0,
            ..push(1)
        },
        &mut io,
    );
    e.open(1, None, &mut io);
    let mut p = push(1);
    p.messages[0].body = "\"".repeat(65536);
    e.push(&p, &mut io);
    assert!(io.frames.iter().all(|f| f.len() <= 65536));
    assert!(io.frames.last().unwrap().contains("behind"));
    e.push(&push(2), &mut io);
    assert!(io.frames.last().unwrap().contains("\"n\":2"));
    assert!(io.closed.is_empty());
}
#[test]
fn empty_history_hole_sends_behind() {
    let mut e = Edge::default();
    let mut io = Io::default();
    e.push(
        &Push {
            messages: vec![],
            head: 0,
            ..push(1)
        },
        &mut io,
    );
    e.open(1, None, &mut io);
    e.push(
        &Push::decode(r#"{"e":70,"b":1,"first":2,"head":1,"messages":[],"recover":true}"#).unwrap(),
        &mut io,
    );
    assert!(io.frames.last().unwrap().contains("behind"));
}

#[test]
fn r4_split_closes_moved_points_and_resumes_by_number() {
    let mut edge = Edge::default();
    let mut io = Io::default();
    edge.push(
        &Push {
            messages: vec![],
            head: 0,
            ..push(1)
        },
        &mut io,
    );
    for id in 0..4 {
        edge.open(id, None, &mut io);
        edge.set_point(id, id << 62);
    }
    edge.push(&push(1), &mut io);
    edge.range(0, Some(1 << 63), false, &[0, 1, 2, 3], &mut io);
    assert_eq!(io.closed, vec![4801, 4801]);
    let mut other = Edge::default();
    other.push(&push(1), &mut io);
    other.push(&push(2), &mut io);
    other.open(9, Some(Position { e: 1, n: 1 }), &mut io);
    assert!(io.frames.last().unwrap().contains("\"n\":2"));
    other.range(0, None, true, &[9], &mut io);
    assert_eq!(io.closed, vec![4801, 4801, 4801]);
}
#[test]
fn r4_replayed_range_preserves_owned_sockets_and_closes_unknown_after_wake() {
    let mut e = Edge::default();
    let mut io = Io::default();
    e.push(&push(1), &mut io);
    e.open(1, None, &mut io);
    e.set_point(1, 0);
    e.range(0, Some(1 << 63), false, &[1, 2], &mut io);
    assert_eq!(io.closed, vec![4801]);
    e.range(0, Some(1 << 63), false, &[1], &mut io);
    assert_eq!(io.closed, vec![4801]);
    e.push(&push(2), &mut io);
    assert!(io.frames.last().unwrap().contains("\"n\":2"));
}
#[test]
fn distinct_cursor_budget_prioritizes_caught_up() {
    let mut edge = Edge::default();
    let mut io = Io::default();
    edge.push(
        &Push {
            head: 32,
            messages: (0..32)
                .map(|_| Item {
                    epoch: None,
                    at: 0,
                    id: None,
                    body: "é".repeat(1000),
                })
                .collect(),
            ..push(1)
        },
        &mut io,
    );
    io.blocked = true;
    for n in 0..20 {
        edge.open(n, Some(Position { e: 1, n }), &mut io);
    }
    edge.open(100, None, &mut io);
    edge.open(101, None, &mut io);
    io.blocked = false;
    io.frames.clear();
    let ack = edge.push(&push(33), &mut io);
    assert!(io.frames[0].contains("\"n\":33"));
    assert_eq!(io.frames.len(), 17);
    assert_eq!(ack.lagging, 5);
    io.frames.clear();
    let ack = edge.push(
        &Push {
            first: 34,
            head: 33,
            messages: vec![],
            ..push(1)
        },
        &mut io,
    );
    assert_eq!(ack.lagging, 0);
    assert_eq!(io.frames.len(), 5);
}
