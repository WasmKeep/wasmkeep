//! Watch (examples/watch), the dogfood app, end to end on one node: a
//! monitor checks a target over outbound HTTP on its alarm, goes down and
//! up with it, and the hub delivers one alert per change to a webhook.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use wasmkeep_runtime::{Runtime, RuntimeConfig};
use wasmkeep_server::{ActorConfig, ActorEnv, ActorService};

#[path = "support/guests.rs"]
mod guests;

fn monitor_wasm() -> &'static [u8] {
    static WASM: OnceLock<Vec<u8>> = OnceLock::new();
    WASM.get_or_init(|| guests::build_guest("../../examples/watch/monitor", "watch_monitor"))
}

fn hub_wasm() -> &'static [u8] {
    static WASM: OnceLock<Vec<u8>> = OnceLock::new();
    WASM.get_or_init(|| guests::build_guest("../../examples/watch/hub", "watch_hub"))
}

/// A tiny HTTP server: answers every request with `status`, and records
/// request bodies.
async fn http_server(status: Arc<AtomicU16>) -> (SocketAddr, Arc<Mutex<Vec<String>>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let bodies = Arc::new(Mutex::new(Vec::new()));
    let seen = Arc::clone(&bodies);
    tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                return;
            };
            let (status, seen) = (Arc::clone(&status), Arc::clone(&seen));
            tokio::spawn(async move {
                let mut raw = Vec::new();
                let mut buf = [0u8; 4096];
                loop {
                    let Ok(n) = stream.read(&mut buf).await else {
                        return;
                    };
                    if n == 0 {
                        return;
                    }
                    raw.extend_from_slice(&buf[..n]);
                    let text = String::from_utf8_lossy(&raw).to_string();
                    if let Some((head, body)) = text.split_once("\r\n\r\n") {
                        let header = |name: &str| {
                            head.lines().find_map(|l| {
                                let (k, v) = l.split_once(':')?;
                                k.eq_ignore_ascii_case(name).then(|| v.trim().to_owned())
                            })
                        };
                        // Bodies come chunked (no length) or with a length.
                        let chunked = header("transfer-encoding")
                            .is_some_and(|v| v.eq_ignore_ascii_case("chunked"));
                        let complete = if chunked {
                            body.ends_with("0\r\n\r\n")
                        } else {
                            let len = header("content-length")
                                .and_then(|v| v.parse::<usize>().ok())
                                .unwrap_or(0);
                            body.len() >= len
                        };
                        if complete {
                            if !body.is_empty() {
                                seen.lock().unwrap().push(body.to_owned());
                            }
                            break;
                        }
                    }
                }
                let code = status.load(Ordering::SeqCst);
                let _ = stream
                    .write_all(
                        format!(
                            "HTTP/1.1 {code} X\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok"
                        )
                        .as_bytes(),
                    )
                    .await;
            });
        }
    });
    (addr, bodies)
}

async fn start(data: &std::path::Path) -> (SocketAddr, oneshot::Sender<()>) {
    // The test's target and webhook listen on 127.0.0.1, which the default
    // egress policy refuses.
    start_with(
        data,
        vec!["127.0.0.1".parse().unwrap()],
        ActorEnv::default(),
    )
    .await
}

async fn start_with(
    data: &std::path::Path,
    allow: Vec<wasmkeep_server::IpRange>,
    env: ActorEnv,
) -> (SocketAddr, oneshot::Sender<()>) {
    let runtime = Runtime::new(&RuntimeConfig {
        max_instances: 16,
        ..RuntimeConfig::default()
    })
    .unwrap();
    let classes = vec![
        (
            "monitor".to_owned(),
            runtime.compile(monitor_wasm()).unwrap(),
            monitor_wasm().to_vec(),
        ),
        (
            "hub".to_owned(),
            runtime.compile(hub_wasm()).unwrap(),
            hub_wasm().to_vec(),
        ),
    ];
    let mut config = ActorConfig::new(data);
    config.alarm_retry_initial = Duration::from_millis(100);
    config.egress = wasmkeep_server::EgressPolicy::allowing(allow);
    config.env = env;
    // As examples/watch/README.md recommends: monitors exist only once the
    // hub creates them.
    config
        .access
        .set("monitor", wasmkeep_server::Access::Existing);
    config.access.apply("hub", "existing:main").unwrap();
    let service = ActorService::new(runtime, config, classes).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (stop, stopped) = oneshot::channel::<()>();
    tokio::spawn(wasmkeep_server::serve(listener, service, async {
        let _ = stopped.await;
    }));
    (addr, stop)
}

async fn request(addr: SocketAddr, method: &str, path: &str, body: &str) -> (u16, String) {
    request_with(addr, method, path, "", body).await
}

/// `request` with extra header lines, each ending in `\r\n`.
async fn request_with(
    addr: SocketAddr,
    method: &str,
    path: &str,
    headers: &str,
    body: &str,
) -> (u16, String) {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let req = format!(
        "{method} {path} HTTP/1.1\r\nHost: t\r\n{headers}Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(req.as_bytes()).await.unwrap();
    let mut raw = String::new();
    stream.read_to_string(&mut raw).await.unwrap();
    let status = raw.split_whitespace().nth(1).unwrap().parse().unwrap();
    let body = raw.split_once("\r\n\r\n").map_or("", |(_, b)| b);
    // Chunked bodies: keep it simple, take the chunk contents.
    let mut text = String::new();
    let mut rest = body;
    while let Some((size, tail)) = rest.split_once("\r\n") {
        match usize::from_str_radix(size.trim(), 16) {
            Ok(0) => break,
            Ok(n) if tail.len() >= n => {
                text.push_str(&tail[..n]);
                rest = tail[n..].trim_start_matches("\r\n");
            }
            _ => {
                text = body.to_owned();
                break;
            }
        }
    }
    (status, text)
}

async fn wait_for(what: &str, limit: Duration, mut done: impl FnMut() -> bool) {
    let started = Instant::now();
    while !done() {
        assert!(started.elapsed() < limit, "{what} within {limit:?}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_monitor_alerts_once_per_change_through_the_webhook() {
    let data = tempfile::tempdir().unwrap();
    let target_status = Arc::new(AtomicU16::new(200));
    let (target, _) = http_server(Arc::clone(&target_status)).await;
    let (webhook, alerts) = http_server(Arc::new(AtomicU16::new(204))).await;
    let (addr, _stop) = start(data.path()).await;

    let (s, _) = request(
        addr,
        "PUT",
        "/actor/hub/main/webhook",
        &format!("http://{webhook}/hook"),
    )
    .await;
    assert_eq!(s, 200);
    let spec = format!(r#"{{"url":"http://{target}/health","every_s":1,"timeout_ms":1000}}"#);
    let (s, body) = request(addr, "PUT", "/actor/hub/main/monitors/api", &spec).await;
    assert_eq!(s, 200, "{body}");

    // Up: checks accumulate, no alert for the first "up".
    tokio::time::sleep(Duration::from_millis(2_500)).await;
    let (_, status) = request(addr, "GET", "/actor/monitor/api/", "").await;
    let v: serde_json::Value = serde_json::from_str(&status).unwrap();
    assert_eq!(v["state"], "up", "{status}");
    assert!(v["checks"].as_i64().unwrap() >= 2, "{status}");
    assert!(alerts.lock().unwrap().is_empty());

    // Down after two failures: one alert.
    target_status.store(500, Ordering::SeqCst);
    wait_for("the down alert", Duration::from_secs(10), || {
        !alerts.lock().unwrap().is_empty()
    })
    .await;
    // Model a monitor retry after the hub committed its report but the
    // monitor recovered its previous state. Its next alarm reports "down"
    // again with a new timestamp; the hub must keep just one transition.
    // The GET waits for the first monitor event to finish before the reset.
    let _ = request(addr, "GET", "/actor/monitor/api/", "").await;
    let storage = wasmkeep_storage::Storage::new(wasmkeep_storage::StorageConfig::new(data.path()));
    {
        let mut monitor = storage.open("default", "monitor", "api").unwrap();
        monitor
            .exec("UPDATE state SET v = 'up' WHERE k = 'state'", &[])
            .unwrap();
    }
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let (_, status) = request(addr, "GET", "/actor/monitor/api/", "").await;
        let status: serde_json::Value = serde_json::from_str(&status).unwrap();
        if status["state"] == "down" {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the repeated down report completes"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let (_, log) = request(addr, "GET", "/actor/hub/main/alerts", "").await;
    let log: serde_json::Value = serde_json::from_str(&log).unwrap();
    assert_eq!(
        log.as_array().unwrap().len(),
        1,
        "a repeated report is not a new change: {log}"
    );

    // Back up: one more alert.
    target_status.store(200, Ordering::SeqCst);
    wait_for("the up alert", Duration::from_secs(10), || {
        alerts.lock().unwrap().len() >= 2
    })
    .await;
    tokio::time::sleep(Duration::from_millis(1_500)).await;
    let sent = alerts.lock().unwrap().clone();
    assert_eq!(sent.len(), 2, "one alert per change: {sent:?}");
    assert!(
        sent[0].contains("DOWN") && sent[0].contains("HTTP 500"),
        "{}",
        sent[0]
    );
    assert!(sent[1].contains("up again"), "{}", sent[1]);

    // The hub's log agrees, and lists the monitor.
    let (_, log) = request(addr, "GET", "/actor/hub/main/alerts", "").await;
    let log: serde_json::Value = serde_json::from_str(&log).unwrap();
    assert_eq!(log.as_array().unwrap().len(), 2, "{log}");
    assert!(
        log.as_array()
            .unwrap()
            .iter()
            .all(|a| a["delivered"] == true),
        "{log}"
    );
    // The id is the calling monitor's own name, from the host.
    assert!(
        log.as_array().unwrap().iter().all(|a| a["id"] == "api"),
        "{log}"
    );
    let (_, list) = request(addr, "GET", "/actor/hub/main/monitors", "").await;
    let list: serde_json::Value = serde_json::from_str(&list).unwrap();
    assert_eq!(list[0]["id"], "api", "{list}");
    assert_eq!(list[0]["state"], "up", "{list}");

    // The dashboard links to the hub by its own address.
    let (s, page) = request(addr, "GET", "/actor/hub/main/", "").await;
    assert_eq!(s, 200);
    assert!(page.contains(r#"const SELF="/actor/hub/main";"#), "{page}");

    // A later down transition is new, and another monitor's down state
    // must not be suppressed by this monitor's history.
    target_status.store(500, Ordering::SeqCst);
    wait_for("a later down transition", Duration::from_secs(10), || {
        alerts.lock().unwrap().len() >= 3
    })
    .await;
    let (s, body) = request(addr, "PUT", "/actor/hub/main/monitors/other", &spec).await;
    assert_eq!(s, 200, "{body}");
    wait_for("another monitor's alert", Duration::from_secs(10), || {
        alerts.lock().unwrap().len() >= 4
    })
    .await;
    let (_, log) = request(addr, "GET", "/actor/hub/main/alerts", "").await;
    let log: serde_json::Value = serde_json::from_str(&log).unwrap();
    let log = log.as_array().unwrap();
    assert_eq!(log.len(), 4);
    assert_eq!(log.iter().filter(|a| a["id"] == "api").count(), 3);
    assert_eq!(log.iter().filter(|a| a["id"] == "other").count(), 1);

    // Only monitors may raise alerts.
    let (s, _) = request(addr, "POST", "/actor/hub/main/alert", "{}").await;
    assert_eq!(s, 403);
}

#[tokio::test(flavor = "multi_thread")]
async fn the_hub_needs_its_token_and_monitors_take_changes_from_the_hub_only() {
    let data = tempfile::tempdir().unwrap();
    let mut env = ActorEnv::default();
    env.extend(
        "hub",
        wasmkeep_server::env::parse("ADMIN_TOKEN=tok-123\nWEBHOOK_URL=http://127.0.0.1:9/hook")
            .unwrap(),
    );
    let (addr, _stop) = start_with(data.path(), Vec::new(), env).await;
    let spec = r#"{"url":"http://127.0.0.1:9/","every_s":60}"#;
    let ok = "Authorization: Bearer tok-123\r\n";

    // Changes need the token; a wrong one, or a prefix of it, is refused.
    for headers in [
        "",
        "Authorization: Bearer tok-124\r\n",
        "Authorization: Bearer tok-12\r\n",
        "Authorization: tok-123\r\n",
    ] {
        let (s, _) = request_with(addr, "PUT", "/actor/hub/main/monitors/api", headers, spec).await;
        assert_eq!(s, 401, "{headers:?}");
    }
    let (s, body) = request_with(addr, "PUT", "/actor/hub/main/monitors/api", ok, spec).await;
    assert_eq!(s, 200, "{body}");

    // The webhook URL holds a secret: reading it needs the token too, and
    // the operator's value cannot be replaced.
    let (s, _) = request(addr, "GET", "/actor/hub/main/webhook", "").await;
    assert_eq!(s, 401);
    let (s, url) = request_with(addr, "GET", "/actor/hub/main/webhook", ok, "").await;
    assert_eq!((s, url.as_str()), (200, "http://127.0.0.1:9/hook"));
    let (s, _) = request_with(addr, "PUT", "/actor/hub/main/webhook", ok, "https://x.y/").await;
    assert_eq!(s, 409);

    // Status reads stay open.
    let (s, list) = request(addr, "GET", "/actor/hub/main/monitors", "").await;
    assert_eq!(s, 200, "{list}");

    // Going around the hub, straight to the monitor, is refused.
    let evil = r#"{"url":"https://attacker.example/","every_s":1}"#;
    let (s, _) = request(addr, "PUT", "/actor/monitor/api/config", evil).await;
    assert_eq!(s, 403);
    let (s, _) = request(addr, "DELETE", "/actor/monitor/api/", "").await;
    assert_eq!(s, 403);
    let (_, status) = request(addr, "GET", "/actor/monitor/api/", "").await;
    assert!(status.contains("127.0.0.1:9"), "{status}");
    // And no monitor the hub did not create can be conjured up.
    let (s, _) = request(addr, "GET", "/actor/monitor/made-up/", "").await;
    assert_eq!(s, 404);
    // Nor a second hub: only hub/main may be created from outside.
    let (s, _) = request(addr, "GET", "/actor/hub/other/monitors", "").await;
    assert_eq!(s, 404);

    let (s, _) = request(addr, "DELETE", "/actor/hub/main/monitors/api", "").await;
    assert_eq!(s, 401);
    let (s, body) = request_with(addr, "DELETE", "/actor/hub/main/monitors/api", ok, "").await;
    assert_eq!(s, 200, "{body}");
}

/// A check says who is asking, and the header the monitor sets is the one
/// on the wire: some sites answer a client they cannot name with an error
/// although they are up.
#[tokio::test(flavor = "multi_thread")]
async fn a_check_names_watch_in_its_user_agent() {
    let data = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let target = listener.local_addr().unwrap();
    let heads = Arc::new(Mutex::new(Vec::<String>::new()));
    let seen = Arc::clone(&heads);
    tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                return;
            };
            let seen = Arc::clone(&seen);
            tokio::spawn(async move {
                let mut raw = Vec::new();
                let mut buf = [0u8; 4096];
                while !raw.windows(4).any(|w| w == b"\r\n\r\n") {
                    match stream.read(&mut buf).await {
                        Ok(n) if n > 0 => raw.extend_from_slice(&buf[..n]),
                        _ => return,
                    }
                }
                seen.lock()
                    .unwrap()
                    .push(String::from_utf8_lossy(&raw).into_owned());
                let _ = stream
                    .write_all(
                        b"HTTP/1.1 200 X\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok",
                    )
                    .await;
            });
        }
    });
    let (addr, _stop) = start(data.path()).await;

    let spec = format!(r#"{{"url":"http://{target}/health","every_s":1,"timeout_ms":1000}}"#);
    let (s, body) = request(addr, "PUT", "/actor/hub/main/monitors/api", &spec).await;
    assert_eq!(s, 200, "{body}");
    wait_for("the first check", Duration::from_secs(10), || {
        !heads.lock().unwrap().is_empty()
    })
    .await;

    let head = heads.lock().unwrap()[0].clone();
    assert!(head.starts_with("GET /health HTTP/1.1\r\n"), "{head}");
    let agents: Vec<&str> = head
        .lines()
        .filter_map(|l| {
            let (name, value) = l.split_once(':')?;
            name.eq_ignore_ascii_case("user-agent")
                .then(|| value.trim())
        })
        .collect();
    // The monitor crate's version follows the name.
    let [agent] = agents[..] else {
        panic!("one User-Agent expected: {head}");
    };
    let version = agent.strip_prefix("wasmkeep-watch/").unwrap_or_default();
    assert!(
        version.split('.').count() == 3
            && version
                .split('.')
                .all(|part| !part.is_empty() && part.bytes().all(|b| b.is_ascii_digit())),
        "{head}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn actors_cannot_reach_private_addresses_by_default() {
    // Server-side request forgery: a monitor (any actor) asking for
    // loopback, a name that resolves to it, or the cloud metadata address is
    // refused before any connection is made.
    let data = tempfile::tempdir().unwrap();
    let hit = Arc::new(AtomicU16::new(200));
    let (target, seen) = http_server(Arc::clone(&hit)).await;
    let (addr, _stop) = start_with(data.path(), Vec::new(), ActorEnv::default()).await;

    let urls = [
        ("loopback", format!("http://{target}/")),
        ("name", format!("http://localhost:{}/", target.port())),
        (
            "metadata",
            "http://169.254.169.254/latest/meta-data/".to_owned(),
        ),
    ];
    for (id, url) in &urls {
        let spec = format!(r#"{{"url":"{url}","every_s":60,"timeout_ms":1000}}"#);
        let (s, body) = request(
            addr,
            "PUT",
            &format!("/actor/hub/main/monitors/{id}"),
            &spec,
        )
        .await;
        assert_eq!(s, 200, "{body}");
    }
    tokio::time::sleep(Duration::from_millis(1_500)).await;
    for (id, _) in &urls {
        let (_, status) = request(addr, "GET", &format!("/actor/monitor/{id}/"), "").await;
        let v: serde_json::Value = serde_json::from_str(&status).unwrap();
        let error = v["last"]["error"].as_str().unwrap_or_default().to_owned();
        // A literal address is refused as such; a name that resolves only
        // to refused addresses looks like one that does not resolve, so
        // refusals cannot be used to map internal names.
        let expected = if *id == "name" {
            "DnsError"
        } else {
            "DestinationIpProhibited"
        };
        assert!(
            error.contains(expected),
            "{id}: expected {expected}, got {status}"
        );
    }
    assert!(seen.lock().unwrap().is_empty());
}

fn test_actor_wasm() -> &'static [u8] {
    static WASM: OnceLock<Vec<u8>> = OnceLock::new();
    WASM.get_or_init(|| guests::build_guest("tests/guests/test-actor", "test_actor"))
}

#[tokio::test(flavor = "multi_thread")]
async fn guests_have_no_way_out_but_outbound_http() {
    // wasi:sockets and wasi:filesystem are linked (toolchains import them
    // whether or not code uses them) but must reach nothing.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let connected = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let flag = Arc::clone(&connected);
    tokio::spawn(async move {
        if listener.accept().await.is_ok() {
            flag.store(true, Ordering::SeqCst);
        }
    });
    let data = tempfile::tempdir().unwrap();
    let runtime = Runtime::new(&RuntimeConfig {
        max_instances: 4,
        ..RuntimeConfig::default()
    })
    .unwrap();
    let component = runtime.compile(test_actor_wasm()).unwrap();
    let mut config = ActorConfig::new(data.path());
    // Even with loopback allowed for HTTP, raw sockets stay closed.
    config.egress = wasmkeep_server::EgressPolicy::allowing(vec!["127.0.0.1".parse().unwrap()]);
    let service = ActorService::new(
        runtime,
        config,
        vec![("t".into(), component, test_actor_wasm().to_vec())],
    )
    .unwrap();
    let server = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = server.local_addr().unwrap();
    let (_stop, stopped) = oneshot::channel::<()>();
    tokio::spawn(wasmkeep_server::serve(server, service, async {
        let _ = stopped.await;
    }));
    let (status, body) = request(addr, "GET", &format!("/actor/t/x/escape?port={port}"), "").await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body.trim(), "tcp=closed dns=closed dir=closed file=closed");
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        !connected.load(Ordering::SeqCst),
        "the guest reached a raw socket"
    );
}
