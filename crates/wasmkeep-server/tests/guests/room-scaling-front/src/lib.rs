use wasmkeep_sdk::{
    broadcast::{Client, Position, Publish},
    directory, http,
};
fn query<'a>(path: &'a str, key: &str) -> Option<&'a str> {
    path.split_once('?')?
        .1
        .split('&')
        .filter_map(|p| p.split_once('='))
        .find(|(k, _)| *k == key)
        .map(|(_, v)| v)
}
struct Front;
impl http::Guest for Front {
    fn handle(req: http::IncomingRequest, out: http::ResponseOutparam) {
        let path = req.path_with_query().unwrap_or_default();
        let relays = path.starts_with("/r5/");
        let path = path.strip_prefix("/r5").unwrap_or(&path).to_owned();
        let room = Client::new(if relays { "room/relays" } else { "room/r" });
        let directory = if relays {
            "directory/bg:relays"
        } else {
            "directory/bg:r"
        };
        if path.starts_with("/join?") {
            let c = query(&path, "c").unwrap_or("client");
            let p = Position {
                e: query(&path, "e").unwrap_or("1").parse().unwrap(),
                n: query(&path, "n").unwrap_or("0").parse().unwrap(),
            };
            match room.join(directory, c, Some(p)) {
                Ok(r) => http::respond_with(out, &r),
                Err(e) => http::respond(out, 503, "text/plain", e.as_bytes()),
            }
            return;
        }
        if path == "/state" {
            let r = wasmkeep_sdk::actors::call(
                if relays { "room/relays" } else { "room/r" },
                &wasmkeep_sdk::actors::Request {
                    method: "POST".into(),
                    path: if relays {
                        "/test-relay-state"
                    } else {
                        "/test-scale-state"
                    }
                    .into(),
                    headers: vec![],
                    body: vec![],
                },
            )
            .unwrap();
            http::respond_with(out, &r);
            return;
        }
        let result: Result<String, String> = match path.as_str() {
            "/publish" => room
                .publish(&[Publish {
                    body: "message".into(),
                    id: None,
                }])
                .map(|p| p[0].n.to_string()),
            "/map" => directory::Client::new(directory)
                .entries()
                .map(|m| m.entries.len().to_string())
                .map_err(|e| e.to_string()),
            _ => room
                .history(
                    Some(Position {
                        e: 1,
                        n: query(&path, "n").unwrap_or("0").parse().unwrap(),
                    }),
                    2000,
                    None,
                )
                .map(|h| {
                    h.messages
                        .iter()
                        .map(|m| m.position.n.to_string())
                        .collect::<Vec<_>>()
                        .join(",")
                }),
        };
        match result {
            Ok(s) => http::respond(out, 200, "text/plain", s.as_bytes()),
            Err(e) => http::respond(out, 503, "text/plain", e.as_bytes()),
        }
    }
}
wasmkeep_sdk::export_front!(Front);
