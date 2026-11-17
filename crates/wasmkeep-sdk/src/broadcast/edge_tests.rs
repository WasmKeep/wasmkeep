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
        e: 1,
        base: 0,
        first: n,
        head: n,
        messages: vec![Item {
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
