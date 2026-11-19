use wasmkeep_sdk::{
    Actor, actors,
    broadcast::{EdgeActor, Position, RelayActor, Round, ScaleHost, ScaleIo, Scaling, handler},
    directory::Directory,
    http,
};
thread_local! {static ROUND:std::cell::RefCell<Round>=std::cell::RefCell::new(Round::default());}
struct Guest;
impl Actor for Guest {
    fn on_init() -> Result<(), String> {
        match actors::self_address().class.as_str() {
            "directory" => Directory::on_init(),
            "edge" => EdgeActor::on_init(),
            "relay" => RelayActor::on_init(),
            _ => handler::init(false),
        }
    }
    fn on_alarm() -> Result<(), String> {
        let a = actors::self_address();
        if a.class != "room" {
            return Ok(());
        }
        let mut scale = Scaling::new(
            ScaleHost {
                directory: format!("directory/bg:{}", a.name),
                room: format!("{}/{}/{}", a.tenant, a.class, a.name),
                capacity: match a.name.as_str() {
                    "relays" => 2,
                    "big" => 150,
                    "small" => 15,
                    _ => 5,
                },
                head: Position { e: 0, n: 0 },
            },
            format!("edge/{}", a.name),
            match a.name.as_str() {
                "relays" => 2,
                "big" => 150,
                "small" => 15,
                _ => 5,
            },
        )?
        // R6 (room_cluster) prewarms its edges by name on the nodes it
        // names: e1, e3, e5, e7, the names four boots and splits get from
        // the room's counter. A merge also takes a number, so every later
        // edge gets a name nobody prewarmed and wakes on the room's node.
        // With 1 s, a slow runner merges a fresh split back while its
        // clients are still reconnecting (the new edge under 30 % of its
        // capacity a second later), and the sockets miss a node. R6 is about
        // spreading and failover, not merging: the default minute. R4/R5
        // (room_scaling) prove merges with 1 s.
        .with_hysteresis(if matches!(a.name.as_str(), "big" | "small") {
            60_000
        } else {
            1000
        });
        ROUND.with_borrow_mut(|r| handler::scaling(&mut scale, r))
    }
    fn on_websocket_open(id: u64, path: String) -> Result<(), String> {
        EdgeActor::on_websocket_open(id, path)
    }
    fn on_websocket_message(id: u64, m: wasmkeep_sdk::Message) {
        EdgeActor::on_websocket_message(id, m);
    }
    fn on_websocket_close(id: u64, c: u16, r: String) {
        EdgeActor::on_websocket_close(id, c, r);
    }
}
impl http::Guest for Guest {
    fn handle(req: http::IncomingRequest, out: http::ResponseOutparam) {
        if matches!(
            req.path_with_query().as_deref(),
            Some("/test-scale-state" | "/test-relay-state")
        ) {
            let host = ScaleHost {
                directory: String::new(),
                room: String::new(),
                capacity: 5,
                head: Position { e: 0, n: 0 },
            };
            let state = host.load().unwrap();
            http::respond(
                out,
                200,
                "text/plain",
                if req.path_with_query().as_deref() == Some("/test-relay-state") {
                    format!(
                        "{},{},{}",
                        state.children.len(),
                        state.relays.len(),
                        u8::from(state.op.is_some())
                    )
                } else {
                    format!("{},{}", state.children.len(), u8::from(state.op.is_some()))
                }
                .as_bytes(),
            );
            return;
        }
        match actors::self_address().class.as_str() {
            "directory" => Directory::handle(req, out),
            "edge" => EdgeActor::handle(req, out),
            "relay" => RelayActor::handle(req, out),
            _ => handler::handle(req, out),
        }
    }
}
wasmkeep_sdk::export!(Guest);
