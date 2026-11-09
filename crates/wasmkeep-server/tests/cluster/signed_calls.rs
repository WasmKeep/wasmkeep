//! Calls between actors on different nodes, and the signatures only nodes can make.

use super::*;

/// ADR 0017: only a node's signed call may ask for a take. A client sending
/// the marker gets an ordinary request.
#[tokio::test(flavor = "multi_thread")]
async fn a_client_cannot_ask_a_node_to_take_an_actor() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let da = tempfile::tempdir().unwrap();
    let a = node_with(
        "a",
        da.path(),
        &store,
        Duration::from_secs(30),
        "t",
        test_actor_wasm(),
    )
    .await;
    let body = "wasmkeep-take 1\ndefault\n";
    let r = exchange(
        a.addr,
        &format!(
            "POST /actor/t/someone/fired HTTP/1.1\r\nHost: t\r\nx-wasmkeep-take: 1\r\n\
             Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        ),
    )
    .await;
    // The actor itself answered: the marker meant nothing.
    assert_eq!((r.status, r.body.trim()), (200, "0"), "{}", r.body);
}

/// ADR 0014: only a node's signed call may hand an alarm off. A client
/// sending the marker gets an ordinary request, and no alarm is armed.
#[tokio::test(flavor = "multi_thread")]
async fn a_client_cannot_hand_an_alarm_off() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let da = tempfile::tempdir().unwrap();
    let a = node_with(
        "a",
        da.path(),
        &store,
        Duration::from_secs(30),
        "t",
        test_actor_wasm(),
    )
    .await;
    let body = format!("wasmkeep-alarm 1\ndefault\n{}\n", now_ms());
    let r = exchange(
        a.addr,
        &format!(
            "POST /actor/t/victim/fired HTTP/1.1\r\nHost: t\r\nx-wasmkeep-alarm-handoff: 1\r\n\
             Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        ),
    )
    .await;
    // The actor itself answered: the marker meant nothing.
    assert_eq!((r.status, r.body.trim()), (200, "0"), "{}", r.body);
    tokio::time::sleep(Duration::from_millis(300)).await;
    let r = send(a.addr, "GET", "/actor/t/victim/attempts", "").await;
    assert_eq!(r.body.trim(), "0", "no alarm ran");
}

#[tokio::test(flavor = "multi_thread")]
async fn an_actor_repeating_a_call_to_another_node_is_not_refused_as_a_replay() {
    // Through the real sender and receiver: an actor makes the same
    // call to an actor on another node many times in a row. Calls signed in
    // the same millisecond carry every signed field alike but the nonce;
    // without it, the receiver took the second for a replay of the first
    // (403, "signature already used"), and the guest saw that 403.
    let p = pair(Duration::from_secs(30), Duration::from_secs(30)).await;
    let r = send(p.a.addr, "GET", "/actor/t/callee/fired", "").await;
    assert_eq!(
        (r.status, r.node.as_deref()),
        (200, Some("a")),
        "{}",
        r.body
    );
    let r = send(p.b.addr, "GET", "/actor/t/caller/fired", "").await;
    assert_eq!(
        (r.status, r.node.as_deref()),
        (200, Some("b")),
        "{}",
        r.body
    );
    let path = "/actor/t/caller/call?to=t/callee&path=%2Fcaller";
    let started = std::time::Instant::now();
    while !send(p.b.addr, "POST", path, "")
        .await
        .body
        .starts_with("200:")
    {
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "b never reached a"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    // 50 identical calls in one event: on loopback a call takes well under
    // a millisecond, so many are signed in the same millisecond as the one
    // before.
    let r = send(p.b.addr, "POST", &format!("{path}&times=50"), "").await;
    assert_eq!(r.status, 200, "{}", r.body);
    let answers: Vec<&str> = r.body.trim().split(" | ").collect();
    assert_eq!(answers.len(), 50, "{}", r.body);
    for (i, answer) in answers.iter().enumerate() {
        assert!(
            answer.starts_with("200:") && answer.contains("caller"),
            "call {i} refused: {answer}"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn an_actor_can_call_an_actor_on_another_node() {
    let p = x_on_a_y_on_b().await;
    // x (on a) calls y (on b); y sees x as its caller.
    let r = send(
        p.a.addr,
        "POST",
        "/actor/t/x/call?to=t/y&path=%2Fcaller",
        "",
    )
    .await;
    assert_eq!((r.status, r.body.trim()), (200, "200:default/t/x"));
    // And the other way round, through b.
    let r = send(
        p.b.addr,
        "POST",
        "/actor/t/y/call?to=t/x&path=%2Fdurable",
        "",
    )
    .await;
    assert_eq!((r.status, r.body.trim()), (200, "200:2"));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_call_cycle_across_nodes_is_refused_not_deadlocked() {
    let p = x_on_a_y_on_b().await;
    // x calls y, which calls x back: x is waiting, so y's call is refused.
    let inner = "%2Fcall%3Fto%3Dt%2Fx%26path%3D%252Fdurable";
    let started = std::time::Instant::now();
    let r = send(
        p.a.addr,
        "POST",
        &format!("/actor/t/x/call?to=t/y&path={inner}"),
        "",
    )
    .await;
    assert_eq!((r.status, r.body.trim()), (200, "200:reentrant"));
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "refused, not timed out"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_client_cannot_pass_for_another_node() {
    let p = x_on_a_y_on_b().await;
    let r = send(
        p.b.addr,
        "POST",
        "/actor/t/y/caller",
        "x-wasmkeep-internal: guess\r\nx-wasmkeep-caller: default/t/x\r\n",
    )
    .await;
    assert_eq!((r.status, r.body.trim()), (200, "none"));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_signed_call_whose_body_was_changed_is_refused() {
    // Someone on the path between nodes keeps a call's signed headers and
    // swaps its body: the receiving node refuses it.
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = tempfile::tempdir().unwrap();
    let a = node_configured(
        "a",
        dir.path(),
        &store,
        Duration::from_secs(30),
        &["u"],
        test_actor_wasm(),
        Duration::from_secs(30),
        "",
        |config| {
            let mut rules = wasmkeep_server::AccessRules::default();
            rules.set("u", wasmkeep_server::Access::Internal);
            config.access = rules;
        },
    )
    .await;
    let key = object_store::path::Path::from("cluster/secret");
    let secret = loop {
        if let Ok(got) = object_store::ObjectStoreExt::get(store.as_ref(), &key).await {
            break String::from_utf8(got.bytes().await.unwrap().to_vec()).unwrap();
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    let secret = secret.trim().to_owned();
    // The node loads it at start; give it a moment.
    tokio::time::sleep(Duration::from_millis(300)).await;

    let path = "/actor/u/x/durable";
    let signed_with = |body: &[u8], ts: u128, nonce: &str| {
        let hash = sha256_hex(body);
        let msg = format!("wasmkeep-call v4\n{ts}\n{nonce}\na\nPOST\n{path}\n{hash}\n");
        let mac = hmac_hex(secret.as_bytes(), msg.as_bytes());
        format!("x-wasmkeep-internal: v4 {ts} {nonce} {hash} {mac}\r\nx-wasmkeep-forwarded: 1\r\n")
    };
    let signed = |body: &[u8], ts: u128| signed_with(body, ts, &format!("{ts:x}"));
    let post = |headers: String, body: &'static str| async move {
        let mut stream = TcpStream::connect(a.addr).await.unwrap();
        let req = format!(
            "POST {path} HTTP/1.1\r\nHost: t\r\nContent-Length: {}\r\n{headers}Connection: close\r\n\r\n{body}",
            body.len()
        );
        stream.write_all(req.as_bytes()).await.unwrap();
        let mut raw = String::new();
        stream.read_to_string(&mut raw).await.unwrap();
        let (head, body) = raw.split_once("\r\n\r\n").unwrap();
        let status: u16 = head.split_whitespace().nth(1).unwrap().parse().unwrap();
        let chunked = head
            .to_ascii_lowercase()
            .contains("transfer-encoding: chunked");
        (
            status,
            if chunked {
                dechunk(body)
            } else {
                body.to_owned()
            },
        )
    };
    let now = || {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
    };
    // Signed for its own body: a node's call, so the internal class answers.
    let (status, body) = post(signed(b"pay 10", now()), "pay 10").await;
    assert_eq!(
        (status, body.trim()),
        (200, "1"),
        "the test signs like a node"
    );
    // Someone on the path copies a real call's headers and gets there first
    // with another body: refused, and the actor did not run...
    let real = signed(b"pay 10", now() + 1);
    let (status, body) = post(real.clone(), "pay 9999").await;
    assert_eq!(status, 403, "{body}");
    assert!(body.contains("does not match"), "{body}");
    // ...and the real call, arriving second, still goes through once.
    let (status, body) = post(real.clone(), "pay 10").await;
    assert_eq!(
        (status, body.trim()),
        (200, "2"),
        "the swapped body did not run"
    );
    let (status, body) = post(real, "pay 10").await;
    assert_eq!(status, 403, "a replay: {body}");
    assert!(body.contains("already used"), "{body}");
    // Two identical calls signed in the same millisecond, as an actor
    // repeating a call quickly sends them: distinct nonces, both run.
    let ts = now() + 3;
    for nonce in ["aa", "bb"] {
        let (status, body) = post(signed_with(b"", ts, nonce), "").await;
        assert_eq!(status, 200, "nonce {nonce}: {body}");
    }
}
