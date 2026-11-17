use wasmkeep_sdk::broadcast::{Client, Position, Publish, handler};
use wasmkeep_sdk::broadcast::{EdgeActor, Round};
use wasmkeep_sdk::{Actor, http, storage};
thread_local! { static ROUND: std::cell::RefCell<Round> = std::cell::RefCell::new(Round::new(&["edge/e"]).unwrap()); }
fn edge() -> bool {
    wasmkeep_sdk::actors::self_address().class == "edge"
}
struct TestRoom;
impl Actor for TestRoom {
    fn on_init() -> Result<(), String> {
        if edge() {
            EdgeActor::on_init()
        } else {
            handler::init(false)
        }
    }
    fn on_websocket_open(id: u64, path: String) -> Result<(), String> {
        EdgeActor::on_websocket_open(id, path)
    }
    fn on_websocket_message(id: u64, msg: wasmkeep_sdk::Message) {
        EdgeActor::on_websocket_message(id, msg);
    }
    fn on_websocket_close(id: u64, code: u16, reason: String) {
        EdgeActor::on_websocket_close(id, code, reason);
    }
    fn on_alarm() -> Result<(), String> {
        if edge() {
            Ok(())
        } else {
            ROUND.with_borrow_mut(handler::round)
        }
    }
}
impl TestRoom {
    fn round() -> Result<String, String> {
        ROUND.with_borrow_mut(handler::round).map(|_| "ok".into())
    }
}
impl http::Guest for TestRoom {
    fn handle(req: http::IncomingRequest, out: http::ResponseOutparam) {
        if edge() {
            EdgeActor::handle(req, out);
            return;
        }
        let path = req.path_with_query().unwrap_or_default();
        let client = Client::new("t/room");
        let result: Result<String, String> = match path.as_str() {
            "/r3-round" => Self::round(),
            "/r3-start" => (|| {
                client.history(None, 100, None)?;
                for _ in 0..2 {
                    let r = wasmkeep_sdk::actors::call(
                        "t/room",
                        &wasmkeep_sdk::actors::Request {
                            method: "POST".into(),
                            path: "/r3-round".into(),
                            headers: vec![],
                            body: vec![],
                        },
                    )
                    .map_err(|e| format!("{e:?}"))?;
                    if r.status != 200 {
                        return Err(String::from_utf8_lossy(&r.body).into_owned());
                    }
                }
                Ok("ok".into())
            })(),
            "/r3-publish" => client
                .publish(&[Publish {
                    body: "x".repeat(32700),
                    id: None,
                }])
                .map(|p| format!("{}.{}", p[0].e, p[0].n)),
            "/publish" => client
                .publish(&[Publish {
                    body: "hello".into(),
                    id: None,
                }])
                .map(|p| format!("{}.{}", p[0].e, p[0].n)),
            "/warm" => client
                .history(None, 100, None)
                .map(|h| format!("{}.{}", h.head.e, h.head.n)),
            "/flush" => {
                let r = storage::flush().map_err(|e| e.to_string());
                r.map(|_| "ok".into())
            }
            _ if path.starts_with("/reconnect?") => {
                let raw = path.strip_prefix("/reconnect?after=").unwrap();
                let (e, n) = raw.split_once('.').unwrap();
                client
                    .history(
                        Some(Position {
                            e: e.parse().unwrap(),
                            n: n.parse().unwrap(),
                        }),
                        100,
                        None,
                    )
                    .map(|h| {
                        format!(
                            "head={}.{} lost={} reset={} messages={}",
                            h.head.e,
                            h.head.n,
                            h.lost
                                .map(|e| format!("{}.{}", e.e, e.base))
                                .unwrap_or_default(),
                            h.reset.is_some(),
                            h.messages.len()
                        )
                    })
            }
            _ => {
                handler::handle(req, out);
                return;
            }
        };
        match result {
            Ok(s) => http::respond(out, 200, "text/plain", s.as_bytes()),
            Err(s) => http::respond(out, 500, "text/plain", s.as_bytes()),
        }
    }
}
wasmkeep_sdk::export!(TestRoom);
