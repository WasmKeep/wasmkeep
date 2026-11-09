use super::*;

#[test]
fn a_call_keeps_the_guests_headers_but_not_framing_or_upgrades() {
    let from = ActorAddress::new("default", "a", "x").unwrap();
    let to = ActorAddress::new("default", "b", "y").unwrap();
    let header = |n: &str, v: &str| (n.to_owned(), v.to_owned());
    let req = build_request(
        &from,
        &to,
        CallRequest {
            method: "POST".into(),
            path: "/pay".into(),
            headers: vec![
                header("content-type", "application/json"),
                header("Content-Length", "3"),
                header("transfer-encoding", "chunked"),
                header("upgrade", "websocket"),
                header("Connection", "upgrade"),
                header("x-wasmkeep-caller", "default/a/someone-else"),
            ],
            body: b"{\"amount\":10}".to_vec(),
        },
    )
    .unwrap();
    let h = req.headers();
    assert_eq!(h["content-type"], "application/json");
    for gone in [
        "content-length",
        "transfer-encoding",
        "upgrade",
        "connection",
    ] {
        assert!(!h.contains_key(gone), "{gone}");
    }
    // Who is calling is the host's to say.
    assert_eq!(h["x-wasmkeep-caller"], "default/a/x");
}
