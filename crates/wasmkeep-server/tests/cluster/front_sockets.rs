//! Sockets through the front (ADR 0027) on a cluster: a front on one node
//! hands a WebSocket to an `internal` room owned by another node, over a
//! signed relay; the socket lives on the room's node and keeps its inside
//! mark while the room hibernates and wakes there (ADR 0024). A client's
//! upgrade, or a forged relay, still reaches the room as outside.

use super::*;

fn front_wasm() -> &'static [u8] {
    static WASM: OnceLock<Vec<u8>> = OnceLock::new();
    WASM.get_or_init(|| guests::build_guest("tests/guests/test-front", "test_front"))
}

type Client =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

/// A node serving the test front and class `r` (the test actor),
/// `internal`, hibernating after `idle`, in the cluster sharing `store`.
async fn room_node(id: &str, data: &Path, store: &Arc<dyn ObjectStore>, idle: Duration) -> Node {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let runtime = Runtime::new(&RuntimeConfig {
        max_instances: 16,
        ..RuntimeConfig::default()
    })
    .unwrap();
    let component = guests::compile(&runtime, test_actor_wasm());
    let mut config = ActorConfig::new(data);
    config.mailbox.idle_timeout = idle;
    let mut rules = wasmkeep_server::AccessRules::default();
    rules.set("r", wasmkeep_server::Access::Internal);
    config.access = rules;
    let mut cluster = ClusterConfig::new(id, advertised(addr), Arc::clone(store));
    cluster.lease = LeaseConfig::new(1_000, 200, 200, 100).unwrap();
    cluster.heartbeat = Duration::from_millis(100);
    cluster.alarm_sweep = Duration::from_millis(200);
    cluster.failover_wait = Some(Duration::ZERO);
    config.cluster = Some(cluster);
    let cluster_port = cluster_listener(&mut config).await;
    let service = ActorService::new(
        runtime.clone(),
        config,
        vec![("r".into(), component, test_actor_wasm().to_vec())],
    )
    .unwrap();
    let front = {
        let component = guests::compile(&runtime, front_wasm());
        wasmkeep_server::HttpComponent::new(runtime.clone(), &component)
            .unwrap()
            .with_max_requests(wasmkeep_server::fds::default_front_requests(16))
    };
    let app = wasmkeep_server::App {
        front: Some(front),
        actors: Some(service.clone()),
    };
    let (stop, stopped) = oneshot::channel::<()>();
    let stopped = async {
        let _ = stopped.await;
    };
    match cluster_port {
        Some(cluster) => tokio::spawn(wasmkeep_server::serve_with_cluster_listener(
            listener,
            cluster,
            app,
            stopped,
            wasmkeep_server::ServeLimits::default(),
        )),
        None => tokio::spawn(wasmkeep_server::serve(listener, app, stopped)),
    };
    let _ = service.node_id_checked().await;
    Node {
        addr,
        service,
        _stop: stop,
    }
}

/// The node `r/<name>`'s ownership record names, if any.
async fn room_holder(store: &Arc<dyn ObjectStore>, name: &str) -> Option<String> {
    use object_store::ObjectStoreExt as _;
    let key = object_store::path::Path::from(test_key(Family::Owners, &format!("r/{name}")));
    let body = match store.get(&key).await {
        Ok(r) => String::from_utf8(r.bytes().await.unwrap().to_vec()).unwrap(),
        Err(object_store::Error::NotFound { .. }) => return None,
        Err(e) => panic!("{e}"),
    };
    let node = body.lines().nth(1)?.split(' ').next()?.to_owned();
    (node != "-").then_some(node)
}

/// `/call` on the test front: `method path` on `r/<name>`, sent again
/// while the owner was briefly out of the caller's view.
async fn front_call(addr: SocketAddr, name: &str, method: &str, path: &str) -> String {
    let path = format!(
        "/call?to=r/{name}&method={method}&path={}",
        path.replace('%', "%25")
            .replace('?', "%3F")
            .replace('=', "%3D")
            .replace('&', "%26")
    );
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let body = send(addr, "GET", &path, "").await.body.trim().to_owned();
        let unreachable =
            body.starts_with("err:failed:node ") && body.ends_with(" is not reachable");
        if !unreachable || tokio::time::Instant::now() >= deadline {
            return body;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// A WebSocket through `addr`'s front, handed to `r/<name>` at `path`,
/// tried again while the owner is briefly out of this node's view.
async fn hand_off(addr: SocketAddr, name: &str, path: &str) -> Client {
    let url = format!(
        "ws://{addr}/handoff?class=r&name={name}&path={}",
        path.replace('?', "%3F")
            .replace('=', "%3D")
            .replace('&', "%26")
    );
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        match tokio_tungstenite::connect_async(&url).await {
            Ok((ws, _)) => return ws,
            Err(_) if tokio::time::Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(e) => panic!("hand-off through {addr}: {e}"),
        }
    }
}

async fn echo(ws: &mut Client, text: &str) -> String {
    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::Message;
    ws.send(Message::text(text)).await.unwrap();
    loop {
        match tokio::time::timeout(Duration::from_secs(5), ws.next()).await {
            Ok(Some(Ok(Message::Text(t)))) => return t.to_string(),
            Ok(Some(Ok(Message::Ping(_) | Message::Pong(_)))) => {}
            other => panic!("no echo: {other:?}"),
        }
    }
}

/// The status a raw upgrade to `path` on `addr` is answered with, and the
/// body when it is not 101.
async fn upgrade_status(addr: SocketAddr, path: &str, extra: &str) -> (u16, String) {
    let r = send(
        addr,
        "GET",
        path,
        &format!(
            "Upgrade: websocket\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\
             Sec-WebSocket-Version: 13\r\n{extra}"
        ),
    )
    .await;
    (r.status, r.body)
}

/// ADR 0027, the whole way: the client connects through b's front, the
/// room is `internal` and owned by a. The socket lives on a, its open saw
/// the front's path, and it keeps working after the room hibernated on a
/// (its record kept, ADR 0024) and woke there, twice: the inside mark is
/// the socket's, not the instance's. An upgrade at `/actor/` through
/// either node is refused all along.
#[tokio::test(flavor = "multi_thread")]
async fn a_socket_handed_off_on_one_node_lives_on_the_rooms_node() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (da, db) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let a = room_node("a", da.path(), &store, Duration::from_millis(300)).await;
    let b = room_node("b", db.path(), &store, Duration::from_secs(30)).await;
    settle().await;

    // a's front creates the room on a.
    assert_eq!(
        front_call(a.addr, "lobby", "GET", "/opened").await,
        "200:none"
    );
    assert_eq!(room_holder(&store, "lobby").await.as_deref(), Some("a"));

    let mut ws = hand_off(b.addr, "lobby", "/ws?user=bob").await;
    assert_eq!(echo(&mut ws, "hi").await, "hi");
    assert_eq!(a.service.websockets(), 1);
    assert_eq!(b.service.websockets(), 0, "the socket is the room's node's");
    assert_eq!(b.service.resident(), 0, "b ran the room");
    assert_eq!(
        front_call(b.addr, "lobby", "GET", "/opened").await,
        "200:/ws?user=bob"
    );

    for round in 0..2 {
        eventually("the lobby hibernated on a, its record kept", async || {
            a.service.resident() == 0 && room_holder(&store, "lobby").await.as_deref() == Some("a")
        })
        .await;
        // The next message wakes it on a, admitted as the front's.
        let text = format!("still here {round}");
        assert_eq!(echo(&mut ws, &text).await, text, "round {round}");
        assert_eq!(room_holder(&store, "lobby").await.as_deref(), Some("a"));
    }
    assert_eq!(b.service.resident(), 0, "b ran the room");

    // From outside, the room stays internal, through either node.
    for node in [&a, &b] {
        let (status, body) = upgrade_status(node.addr, "/actor/r/lobby/ws", "").await;
        assert_eq!(status, 403, "{body}");
    }
    assert_eq!(a.service.websockets(), 1);
}

/// What the owner believes of a relayed upgrade: only a valid signature,
/// spent once, with the front's caller and the upgrade mark among its
/// signed headers. A client forging the mark and the caller, or a node's
/// signed call without the mark, or a replay, is refused; the test signs
/// like a node, with the bucket's cluster secret.
#[tokio::test(flavor = "multi_thread")]
async fn only_a_signed_hand_off_opens_an_internal_room_from_another_node() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = tempfile::tempdir().unwrap();
    let a = room_node("a", dir.path(), &store, Duration::from_secs(30)).await;
    let key = object_store::path::Path::from("cluster/secret");
    let secret = loop {
        if let Ok(got) = object_store::ObjectStoreExt::get(store.as_ref(), &key).await {
            break String::from_utf8(got.bytes().await.unwrap().to_vec()).unwrap();
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    let secret = secret.trim().to_owned();
    tokio::time::sleep(Duration::from_millis(300)).await;
    let now = || {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
    };
    let path = "/actor/r/lobby/ws";
    // A node's relay, signed over `signed_path`, with `mark` signed.
    let relay = |signed_path: &str, mark: bool, ts: u128| {
        let hash = sha256_hex(b"");
        let mark_line = if mark {
            "x-wasmkeep-upgrade:websocket\n"
        } else {
            ""
        };
        let msg = format!(
            "wasmkeep-call v4\n{ts}\n{ts:x}\na\nGET\n{signed_path}\n{hash}\n\
             x-wasmkeep-caller:default/_front/main\nx-wasmkeep-app:default/default\n{mark_line}"
        );
        let mac = hmac_hex(secret.as_bytes(), msg.as_bytes());
        let mark_header = if mark {
            "x-wasmkeep-upgrade: websocket\r\n"
        } else {
            ""
        };
        format!(
            "x-wasmkeep-internal: v4 {ts} {ts:x} {hash} {mac}\r\nx-wasmkeep-forwarded: 1\r\n\
             x-wasmkeep-app: default/default\r\nx-wasmkeep-caller: default/_front/main\r\n{mark_header}"
        )
    };
    // A client's forgery: the mark and the caller, no valid signature.
    let forged = "x-wasmkeep-upgrade: websocket\r\nx-wasmkeep-caller: default/_front/main\r\n\
                  x-wasmkeep-internal: v4 1 2 3 4\r\n";
    assert_eq!(upgrade_status(a.addr, path, forged).await.0, 403);
    // A node's signature without the mark: not a hand-off.
    let (status, body) = upgrade_status(a.addr, path, &relay(path, false, now())).await;
    assert_eq!(status, 403, "{body}");
    // Signed for another path: not believed, so outside.
    let (status, _) = upgrade_status(a.addr, path, &relay("/actor/r/other/ws", true, now())).await;
    assert_eq!(status, 403);
    // The real thing opens it, once.
    let headers = relay(path, true, now() + 1);
    let mut stream = TcpStream::connect(a.addr).await.unwrap();
    let req = format!(
        "GET {path} HTTP/1.1\r\nHost: t\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\
         Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n{headers}\r\n"
    );
    stream.write_all(req.as_bytes()).await.unwrap();
    let mut head = vec![0u8; 12];
    stream.read_exact(&mut head).await.unwrap();
    assert_eq!(&head, b"HTTP/1.1 101", "{}", String::from_utf8_lossy(&head));
    eventually("the relayed socket is open on a", async || {
        a.service.websockets() == 1
    })
    .await;
    let (status, body) = upgrade_status(a.addr, path, &headers).await;
    assert_eq!(status, 403, "a replay: {body}");
    assert!(body.contains("already used"), "{body}");
    drop(stream);
}
