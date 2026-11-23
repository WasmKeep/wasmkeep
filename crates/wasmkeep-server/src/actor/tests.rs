use super::*;

#[test]
fn a_call_keeps_the_guests_headers_but_not_framing_or_upgrades() {
    let from = ActorAddress::new("default", "a", "x").unwrap();
    let to = ActorAddress::new("default", "b", "y").unwrap();
    let header = |n: &str, v: &str| (n.to_owned(), v.to_owned());
    let req = build_request(
        &Caller::Actor(from),
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

/// ADR 0023: every `x-wasmkeep-` header a guest sets is dropped, whatever
/// its name, before the host sets the actor and caller.
#[test]
fn a_call_drops_every_wasmkeep_header_the_guest_set() {
    let from = ActorAddress::new("default", "a", "x").unwrap();
    let to = ActorAddress::new("default", "b", "y").unwrap();
    let header = |n: &str, v: &str| (n.to_owned(), v.to_owned());
    let req = build_request(
        &Caller::Actor(from),
        &to,
        CallRequest {
            method: "GET".into(),
            path: "/".into(),
            headers: vec![
                header("x-wasmkeep-forwarded", "1"),
                header("X-WasmKeep-Node", "elsewhere"),
                header("x-wasmkeep-chain", "default/c/z"),
                header("x-wasmkeep-internal", "v4 0 0 0 0"),
                header("x-wasmkeep-actor", "default/c/z"),
                header("x-wasmkeep-later", "anything"),
                header("x-wasmkeep-kv-other", "x"),
                header("x-wasmkeep-queue-other", "x"),
                header("x-other", "kept"),
                // The SDKs' key-value and queue protocol between actors.
                header("x-wasmkeep-kv-expect", "3"),
                header("X-WasmKeep-KV-TTL-MS", "1000"),
                header("x-wasmkeep-kv-durable", "1"),
                header("x-wasmkeep-queue-delay-ms", "50"),
            ],
            body: Vec::new(),
        },
    )
    .unwrap();
    let mut names: Vec<&str> = req
        .headers()
        .keys()
        .map(hyper::header::HeaderName::as_str)
        .filter(|n| n.starts_with("x-wasmkeep-"))
        .collect();
    names.sort_unstable();
    assert_eq!(
        names,
        [
            "x-wasmkeep-actor",
            "x-wasmkeep-caller",
            "x-wasmkeep-kv-durable",
            "x-wasmkeep-kv-expect",
            "x-wasmkeep-kv-ttl-ms",
            "x-wasmkeep-queue-delay-ms",
        ]
    );
    assert_eq!(req.headers()["x-wasmkeep-actor"], "default/b/y");
    assert_eq!(req.headers()["x-other"], "kept");
}

/// A call's headers are held to what a guest may build in one `fields`:
/// the callee gets them as its request's, outside that size check.
#[test]
fn a_call_with_too_many_or_too_large_headers_is_refused() {
    let from = ActorAddress::new("default", "a", "x").unwrap();
    let to = ActorAddress::new("default", "b", "y").unwrap();
    let call = |headers: Vec<(String, String)>| {
        build_request(
            &Caller::Actor(from.clone()),
            &to,
            CallRequest {
                method: "GET".into(),
                path: "/".into(),
                headers,
                body: Vec::new(),
            },
        )
    };
    let many = |n: usize| {
        (0..n)
            .map(|i| (format!("x-h{i}"), "v".to_owned()))
            .collect()
    };
    assert!(call(many(MAX_CALL_HEADERS)).is_ok());
    assert!(matches!(
        call(many(MAX_CALL_HEADERS + 1)),
        Err(CallError::TooLarge)
    ));
    let big = |bytes: usize| vec![("x-big".to_owned(), "v".repeat(bytes))];
    assert!(call(big(16 * 1024)).is_ok());
    assert!(matches!(
        call(big(crate::MAX_GUEST_FIELDS_BYTES)),
        Err(CallError::TooLarge)
    ));
}

#[tokio::test]
async fn socket_registration_restore_gate_is_not_capacity() {
    let dir = tempfile::tempdir().unwrap();
    let runtime =
        wasmkeep_runtime::Runtime::new(&wasmkeep_runtime::RuntimeConfig::default()).unwrap();
    let service = ActorService::new(runtime, ActorConfig::new(dir.path()), vec![]).unwrap();
    let address = ActorAddress::new("default", "t", "late").unwrap();
    service
        .inner
        .restoring
        .lock()
        .unwrap()
        .insert(address.clone(), 1);
    let result = service
        .inner
        .accept_socket(address.clone(), "/".into(), crate::websocket::Opener::Front)
        .await;
    let error = match result {
        Err(error) => error,
        Ok(_) => panic!("restore gate admitted a socket"),
    };
    assert!(matches!(error, crate::websocket::OpenError::RestorePending));
    let response = super::upgrade::open_error_response(error);
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(response.headers()[hyper::header::RETRY_AFTER], "1");
    use http_body_util::BodyExt as _;
    assert_eq!(
        response.into_body().collect().await.unwrap().to_bytes(),
        "actor restore pending\n"
    );
    assert_eq!(service.inner.sockets.reserved_by(&address), 0);
}

/// Review item 2 (ADR 0041, section 6): an event a stopping node never
/// ran (its mailboxes closed) is marked as not run, so a placed event's
/// receiver refuses it with the mark and its sender runs it.
#[tokio::test]
async fn an_event_a_stopping_node_never_ran_is_marked_not_run() {
    let dir = tempfile::tempdir().unwrap();
    let runtime =
        wasmkeep_runtime::Runtime::new(&wasmkeep_runtime::RuntimeConfig::default()).unwrap();
    let service = ActorService::new(runtime, ActorConfig::new(dir.path()), vec![]).unwrap();
    service.inner.mailboxes.close();
    let address = ActorAddress::new("default", "t", "late").unwrap();
    let req = Request::get("/").body(http_body_util::BodyExt::map_err(
        http_body_util::Empty::<bytes::Bytes>::new(),
        |never: std::convert::Infallible| -> wasmtime_wasi_http::Error { match never {} },
    ));
    let response = match service
        .dispatch(address, req.unwrap(), Arc::from(Vec::new()), true)
        .await
    {
        Ok(response) => response,
        Err(_) => panic!("handed back"),
    };
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert!(response.extensions().get::<requests::NotRun>().is_some());
}
