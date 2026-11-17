use wasmkeep_sdk::broadcast::{Client, Position, Publish, handler};
use wasmkeep_sdk::{Actor, http, storage};
struct TestRoom;
impl Actor for TestRoom {
    fn on_init() -> Result<(), String> {
        handler::init(false)
    }
    fn on_alarm() -> Result<(), String> {
        handler::before_push().map(|_| ())
    }
}
impl http::Guest for TestRoom {
    fn handle(req: http::IncomingRequest, out: http::ResponseOutparam) {
        let path = req.path_with_query().unwrap_or_default();
        let client = Client::new("t/room");
        let result: Result<String, String> = match path.as_str() {
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
