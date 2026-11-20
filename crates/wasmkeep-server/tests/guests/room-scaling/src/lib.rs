use wasmkeep_sdk::{
    Actor, actors,
    broadcast::{
        EdgeActor, Position, RelayActor, Round, ScaleHost, ScaleIo, Scaling, handler, pusher,
    },
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
            "pusher" => Ok(()),
            _ => handler::init(false),
        }
    }
    /// The pusher runs the rounds and scaling (ADR 0040, section 13); the
    /// room's alarm, if a 0.6.0 one is left, does nothing.
    fn on_alarm() -> Result<(), String> {
        let a = actors::self_address();
        if a.class != "pusher" {
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
        ROUND.with_borrow_mut(|r| pusher::handler::scaling(&mut scale, r))
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
thread_local! {
    /// The pusher tests' slow edge: how long each push waits before it is applied.
    static SLOW: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}
impl http::Guest for Guest {
    fn handle(req: http::IncomingRequest, out: http::ResponseOutparam) {
        let path = req.path_with_query().unwrap_or_default();
        // The pusher tests (room_cluster): a room event that traps, a slow
        // edge, and the pusher's children by name.
        if path == "/test-trap" {
            panic!("a room event that traps");
        }
        if let Some(ms) = path.strip_prefix("/test-slow?ms=") {
            SLOW.set(ms.parse().unwrap_or(0));
            http::respond(out, 200, "text/plain", b"ok");
            return;
        }
        if path == "/wasmkeep-edge/v1/push" && SLOW.get() > 0 {
            std::thread::sleep(std::time::Duration::from_millis(SLOW.get()));
        }
        // A group as a 0.6.0 room left it (room_scaling): the room is the
        // controller, its own scale rows, the map's owner and the edges' room.
        if path == "/test-legacy-boot" {
            let a = actors::self_address();
            let result = (|| -> Result<(), String> {
                let mut core = wasmkeep_sdk::broadcast::Core::new(wasmkeep_sdk::broadcast::Host, false);
                core.init()?;
                let mut scale = Scaling::new(
                    ScaleHost {
                        directory: format!("directory/bg:{}", a.name),
                        room: format!("{}/{}/{}", a.tenant, a.class, a.name),
                        capacity: 5,
                        head: Position { e: 0, n: 0 },
                    },
                    format!("edge/{}", a.name),
                    5,
                )?;
                let mut round = Round::default();
                for _ in 0..3 {
                    scale.run(&mut round, &mut core, wasmkeep_sdk::now_ms())?;
                }
                Ok(())
            })();
            match result {
                Ok(()) => http::respond(out, 200, "text/plain", b"ok"),
                Err(e) => http::respond(out, 500, "text/plain", e.as_bytes()),
            }
            return;
        }
        if path == "/test-owner" || path == "/test-legacy-rows" {
            let sql = if path == "/test-owner" {
                "SELECT owner FROM wasmkeep_directory_map"
            } else {
                "SELECT group_concat(name) FROM sqlite_master WHERE name IN ('wasmkeep_room_scale','wasmkeep_room_ops')"
            };
            let rows = wasmkeep_sdk::storage::query(sql, &[]).unwrap();
            let v = rows
                .values
                .first()
                .and_then(|r| r[0].as_str().map(str::to_owned))
                .unwrap_or_default();
            http::respond(out, 200, "text/plain", v.as_bytes());
            return;
        }
        if path == "/test-children" {
            let host = ScaleHost {
                directory: String::new(),
                room: String::new(),
                capacity: 5,
                head: Position { e: 0, n: 0 },
            };
            let mut names: Vec<_> = host
                .load()
                .unwrap()
                .children
                .into_iter()
                .map(|c| c.name)
                .collect();
            names.sort();
            http::respond(out, 200, "text/plain", names.join(",").as_bytes());
            return;
        }
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
            "pusher" => pusher::handler::handle(req, out),
            _ => handler::handle(req, out),
        }
    }
}
wasmkeep_sdk::export!(Guest);
