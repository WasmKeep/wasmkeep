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
        if path.starts_with("/r6/") {
            r6(&path, out);
            return;
        }
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
                if relays { "pusher/relays" } else { "pusher/r" },
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

// R6-only test routes: production clients use the TypeScript example's front.
fn r6(path: &str, out: http::ResponseOutparam) {
    let name = query(path, "room").unwrap_or("big");
    assert!(matches!(name, "big" | "small" | "legacy" | "quiet"));
    let room = Client::new(format!("room/{name}"));
    let directory = format!("directory/bg:{name}");
    let endpoint = path.split('?').next().unwrap();
    if endpoint == "/r6/join" {
        let p = Position {
            e: 1,
            n: query(path, "n").unwrap_or("0").parse().unwrap(),
        };
        match room.join(&directory, query(path, "c").unwrap(), Some(p)) {
            Ok(r) => http::respond_with(out, &r),
            Err(e) => http::respond(out, 503, "text/plain", e.as_bytes()),
        }
        return;
    }
    // The pusher tests: wake the pusher where this front runs, trap the
    // room, slow an edge down, list the pusher's children.
    let call = |to: String, path: String| {
        wasmkeep_sdk::actors::call(
            &to,
            &wasmkeep_sdk::actors::Request {
                method: "POST".into(),
                path,
                headers: vec![],
                body: vec![],
            },
        )
        .map_err(|e| format!("{e:?}"))
        .and_then(|r| {
            let body = String::from_utf8_lossy(&r.body).into_owned();
            if r.status == 200 {
                Ok(body)
            } else {
                Err(format!("{}: {body}", r.status))
            }
        })
    };
    let result: Result<String, String> = match endpoint {
        "/r6/warm-pusher" => call(format!("pusher/{name}"), "/wasmkeep-pusher/v1/poke".into()),
        "/r6/trap" => call(format!("room/{name}"), "/test-trap".into()),
        "/r6/slow" => call(
            format!("edge/{name}.e{}", query(path, "edge").unwrap()),
            format!("/test-slow?ms={}", query(path, "ms").unwrap()),
        ),
        "/r6/children" => call(format!("pusher/{name}"), "/test-children".into()),
        "/r6/legacy-boot" => call(format!("room/{name}"), "/test-legacy-boot".into()),
        "/r6/legacy-rows" => call(format!("room/{name}"), "/test-legacy-rows".into()),
        "/r6/owner" => call(format!("directory/bg:{name}"), "/test-owner".into()),
        "/r6/warm" => wasmkeep_sdk::actors::call(
            &format!("edge/{name}.e{}", query(path, "edge").unwrap()),
            &wasmkeep_sdk::actors::Request {
                method: "GET".into(),
                path: "/warm".into(),
                headers: vec![],
                body: vec![],
            },
        )
        .map(|_| "ok".into())
        .map_err(|e| format!("{e:?}")),
        "/r6/publish" => room
            .publish(&[Publish {
                body: if query(path, "large").is_some() {
                    "x".repeat(16000)
                } else {
                    "message".into()
                },
                id: query(path, "id").map(str::to_owned),
            }])
            .map(|p| p[0].n.to_string()),
        "/r6/map" => directory::Client::new(&directory)
            .entries()
            .map(|m| {
                m.entries
                    .iter()
                    .map(|e| e.target.clone())
                    .collect::<Vec<_>>()
                    .join(",")
            })
            .map_err(|e| e.to_string()),
        "/r6/history" => room
            .history(
                Some(Position {
                    e: 1,
                    n: query(path, "n").unwrap().parse().unwrap(),
                }),
                2000,
                None,
            )
            .map(|h| {
                assert!(
                    h.reset.is_none() && h.lost.is_none(),
                    "R6 must retain every published number"
                );
                h.messages
                    .iter()
                    .map(|m| m.position.n.to_string())
                    .collect::<Vec<_>>()
                    .join(",")
            }),
        _ => Err("unknown R6 route".into()),
    };
    match result {
        Ok(s) => http::respond(out, 200, "text/plain", s.as_bytes()),
        Err(e) => http::respond(out, 503, "text/plain", e.as_bytes()),
    }
}
