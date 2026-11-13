//! Deploys landing while an actor is starting (ADR 0008: no old code on
//! newer data). Uses storage fault injection to hold an actor's start-up at
//! the point where its database is being opened.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code

use std::net::SocketAddr;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use wasmkeep_runtime::{Runtime, RuntimeConfig};
use wasmkeep_server::{ActorConfig, ActorService};

#[path = "support/guests.rs"]
mod guests;

/// (status, de-chunked body).
async fn send(addr: SocketAddr, method: &str, path: &str) -> (u16, String) {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let req = format!(
        "{method} {path} HTTP/1.1\r\nHost: t\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
    );
    stream.write_all(req.as_bytes()).await.unwrap();
    let mut raw = String::new();
    stream.read_to_string(&mut raw).await.unwrap();
    let status = raw.split_whitespace().nth(1).unwrap().parse().unwrap();
    let body = raw.split_once("\r\n\r\n").map_or("", |(_, b)| b);
    let body = match body.split_once("\r\n") {
        Some((size, rest)) if usize::from_str_radix(size.trim(), 16).is_ok() => {
            rest.split("\r\n").next().unwrap_or("")
        }
        _ => body,
    };
    (status, body.trim_end().to_owned())
}

/// The deploy sequence the actor's database records (read while the
/// actor is hibernated).
fn recorded_seq(data: &std::path::Path, name: &str) -> Option<String> {
    let storage = wasmkeep_storage::Storage::new(
        wasmkeep_storage::StorageConfig::new(data),
        wasmkeep_server::state_dir(&wasmkeep_server::StateRoot::new("", "default", "default")),
    );
    storage.open("c", name).unwrap().meta("deploy_seq").unwrap()
}

async fn hibernated(service: &ActorService) {
    let started = std::time::Instant::now();
    while service.resident() > 0 {
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "never hibernated"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

struct Setup {
    addr: SocketAddr,
    service: ActorService,
    runtime: Runtime,
    v2: Vec<u8>,
    dir: tempfile::TempDir,
    _stop: tokio::sync::oneshot::Sender<()>,
}

/// Class `c` is the counter, deployed as sequence 1.
async fn setup() -> Setup {
    setup_idle(Duration::from_millis(200)).await
}

async fn setup_idle(idle: Duration) -> Setup {
    setup_with(idle, wasmkeep_server::ActorEnv::default()).await
}

/// With these `--env` values on the node.
async fn setup_with(idle: Duration, env: wasmkeep_server::ActorEnv) -> Setup {
    let v1 = guests::build_guest("../../examples/counter", "counter");
    let v2 = guests::build_guest("tests/guests/test-actor", "test_actor");
    let runtime = Runtime::new(&RuntimeConfig {
        max_instances: 8,
        ..RuntimeConfig::default()
    })
    .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let component = runtime.compile(&v1).unwrap();
    let mut config = ActorConfig::new(dir.path());
    config.mailbox.idle_timeout = idle;
    config.deploy_seq = 1;
    config.env = env;
    let service =
        ActorService::new(runtime.clone(), config, vec![("c".into(), component, v1)]).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    tokio::spawn(wasmkeep_server::serve(listener, service.clone(), async {
        let _ = stopped.await;
    }));
    Setup {
        addr,
        service,
        runtime,
        v2,
        dir,
        _stop: stop,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn code_and_recorded_sequence_always_agree_when_a_deploy_lands_mid_start() {
    let t = setup().await;
    let v2 = t.runtime.compile(&t.v2).unwrap();

    // The request takes deploy 1 (the counter) and is held while its
    // database opens; deploy 2 (the test actor) lands meanwhile.
    wasmkeep_storage::faults::delay_next("open", Duration::from_millis(800));
    let request = tokio::spawn(send(t.addr, "GET", "/actor/c/x/inits"));
    let started = std::time::Instant::now();
    while !wasmkeep_storage::faults::held("open") {
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "the start-up never reached the open"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    t.service
        .apply_deploy(2, &[("c".into(), v2, t.v2.clone())])
        .unwrap();
    assert_eq!(t.service.deploy_seq(), 2);

    // The event already under way finishes on the code it took (the
    // counter answers any GET with its value), and records *that* code's
    // sequence, not the node's.
    assert_eq!(request.await.unwrap(), (200, "0".into()));
    hibernated(&t.service).await;
    assert_eq!(
        recorded_seq(t.dir.path(), "x").as_deref(),
        Some("1"),
        "old code recorded the new deploy"
    );

    // The next event runs the new code and records its sequence.
    assert_eq!(
        send(t.addr, "GET", "/actor/c/x/inits").await,
        (200, "1".into())
    );
    hibernated(&t.service).await;
    assert_eq!(recorded_seq(t.dir.path(), "x").as_deref(), Some("2"));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_deploy_with_an_invalid_class_changes_nothing() {
    let t = setup().await;
    let v2 = t.runtime.compile(&t.v2).unwrap();
    // A stateless component is not an actor: the deploy is refused whole.
    let hello = guests::build_guest("../../examples/hello", "hello");
    let not_an_actor = t.runtime.compile(&hello).unwrap();
    let refused = t.service.apply_deploy(
        2,
        &[
            ("c".into(), v2, t.v2.clone()),
            ("d".into(), not_an_actor, hello),
        ],
    );
    assert_eq!(refused.unwrap_err().0, "d");
    assert_eq!(t.service.deploy_seq(), 1);
    // c is still the counter, d does not exist.
    assert_eq!(
        send(t.addr, "GET", "/actor/c/y/inits").await,
        (200, "0".into())
    );
    assert_eq!(send(t.addr, "GET", "/actor/d/y/").await.0, 404);
}

#[tokio::test(flavor = "multi_thread")]
async fn the_same_code_redeployed_keeps_its_instances_and_records_the_new_sequence() {
    // Resident throughout (no hibernation would hide a missed record).
    let t = setup_idle(Duration::from_secs(60)).await;
    let deploy = |seq: u64| {
        let component = t.runtime.compile(&t.v2).unwrap();
        t.service
            .apply_deploy(seq, &[("c".into(), component, t.v2.clone())])
            .unwrap();
    };
    deploy(2);
    assert_eq!(
        send(t.addr, "POST", "/actor/c/z/memory").await,
        (200, "1".into())
    );
    // Deploy 3 with the same bytes: the instance stays (its memory too),
    // and records the new sequence before its next event runs.
    deploy(3);
    assert_eq!(
        send(t.addr, "POST", "/actor/c/z/memory").await,
        (200, "2".into()),
        "the instance was rebuilt for unchanged code"
    );
    // Read through a second connection (WAL: it sees what was committed).
    assert_eq!(recorded_seq(t.dir.path(), "z").as_deref(), Some("3"));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_rollback_reruns_on_init_under_its_higher_sequence() {
    let t = setup_idle(Duration::from_secs(60)).await;
    let v1 = guests::build_guest("../../examples/counter", "counter");
    let apply = |seq: u64, bytes: &Vec<u8>| {
        let component = t.runtime.compile(bytes).unwrap();
        t.service
            .apply_deploy(seq, &[("c".into(), component, bytes.clone())])
            .unwrap();
    };
    apply(2, &t.v2);
    assert_eq!(
        send(t.addr, "GET", "/actor/c/r/inits").await,
        (200, "1".into())
    );
    // Rolled back to the counter (older code, higher sequence), then
    // forward again: on-init runs for each version change.
    apply(3, &v1);
    assert_eq!(
        send(t.addr, "GET", "/actor/c/r/inits").await,
        (200, "0".into())
    );
    assert_eq!(recorded_seq(t.dir.path(), "r").as_deref(), Some("3"));
    apply(4, &t.v2);
    assert_eq!(
        send(t.addr, "GET", "/actor/c/r/inits").await,
        (200, "2".into())
    );
    assert_eq!(recorded_seq(t.dir.path(), "r").as_deref(), Some("4"));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_removed_class_closes_its_resident_actors_sockets() {
    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::Message;
    let t = setup_idle(Duration::from_secs(60)).await;
    let component = t.runtime.compile(&t.v2).unwrap();
    t.service
        .apply_deploy(2, &[("c".into(), component, t.v2.clone())])
        .unwrap();
    let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{}/actor/c/s", t.addr))
        .await
        .unwrap();
    // A deploy without class c, then a message: the socket is closed so
    // the client knows, rather than its messages vanishing.
    let other = t.runtime.compile(&t.v2).unwrap();
    t.service
        .apply_deploy(3, &[("other".into(), other, t.v2.clone())])
        .unwrap();
    ws.send(Message::text("hello?")).await.unwrap();
    let closed = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match ws.next().await {
                Some(Ok(Message::Close(frame))) => return frame.map(|f| u16::from(f.code)),
                Some(Ok(_)) => {}
                other => panic!("expected a close frame, got {other:?}"),
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(closed, Some(1012));
    assert_eq!(send(t.addr, "GET", "/actor/c/s/inits").await.0, 404);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_queued_request_is_decided_by_the_rules_of_the_class_it_runs() {
    // ADR 0013: request B is admitted at the front door under `open`, then
    // waits behind A while a deploy makes the class `internal`. B must be
    // refused, not run on the new version from outside.
    let t = setup().await;
    let v1 = guests::build_guest("../../examples/counter", "counter");
    wasmkeep_storage::faults::delay_next("open", Duration::from_millis(800));
    let a = tokio::spawn(send(t.addr, "GET", "/actor/c/x"));
    let started = std::time::Instant::now();
    while !wasmkeep_storage::faults::held("open") {
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "A never reached the open"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let b = tokio::spawn(send(t.addr, "GET", "/actor/c/x"));
    // B is past the front door and queued behind A.
    tokio::time::sleep(Duration::from_millis(200)).await;
    let mut rules = wasmkeep_server::AccessRules::default();
    rules.set("c", wasmkeep_server::Access::Internal);
    let component = t.runtime.compile(&v1).unwrap();
    t.service
        .apply_version(
            2,
            &[("c".into(), component, v1)],
            &rules,
            &std::collections::BTreeMap::new(),
        )
        .unwrap();
    assert_eq!(
        a.await.unwrap(),
        (200, "0".into()),
        "A runs on what it took"
    );
    assert_eq!(
        b.await.unwrap().0,
        403,
        "B ran under a rule its class does not have"
    );
    assert_eq!(send(t.addr, "GET", "/actor/c/x").await.0, 403);
}

#[tokio::test(flavor = "multi_thread")]
async fn configuration_changes_with_the_version_and_node_values_win() {
    // Resident throughout: the same instance must see the new values.
    let mut node = wasmkeep_server::ActorEnv::default();
    node.extend(
        "c",
        wasmkeep_server::env::parse("URL=from-node\nSECRET=s").unwrap(),
    );
    let t = setup_with(Duration::from_secs(60), node).await;
    let apply = |seq: u64, config: &str| {
        let component = t.runtime.compile(&t.v2).unwrap();
        let config = std::collections::BTreeMap::from([(
            "c".to_owned(),
            wasmkeep_server::env::parse(config).unwrap(),
        )]);
        t.service
            .apply_version(
                seq,
                &[("c".into(), component, t.v2.clone())],
                &wasmkeep_server::AccessRules::default(),
                &config,
            )
            .unwrap();
    };
    let addr = t.addr;
    let env = move |name: &'static str| async move {
        send(addr, "GET", &format!("/actor/c/e/env?name={name}")).await
    };
    apply(2, "URL=deployed\nMODE=one");
    assert_eq!(env("MODE").await, (200, "one wasi=unset".into()));
    assert_eq!(
        env("URL").await,
        (200, "from-node wasi=unset".into()),
        "--env wins by name"
    );
    assert_eq!(env("SECRET").await, (200, "s wasi=unset".into()));
    assert_eq!(
        send(t.addr, "POST", "/actor/c/e/memory").await,
        (200, "1".into())
    );

    // Only the configuration changes: a new instance (a guest may have
    // cached a value), the database kept, on-init not rerun.
    apply(3, "URL=deployed\nMODE=two");
    assert_eq!(env("MODE").await, (200, "two wasi=unset".into()));
    assert_eq!(
        send(t.addr, "POST", "/actor/c/e/memory").await,
        (200, "1".into()),
        "an instance kept its old values"
    );
    assert_eq!(
        send(t.addr, "GET", "/actor/c/e/inits").await,
        (200, "1".into())
    );
    assert_eq!(recorded_seq(t.dir.path(), "e").as_deref(), Some("3"));
    // Rules only (values unchanged): the instance stays.
    apply(4, "URL=deployed\nMODE=two");
    assert_eq!(
        send(t.addr, "POST", "/actor/c/e/memory").await,
        (200, "2".into())
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_class_made_internal_closes_its_sockets_at_their_next_message() {
    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::Message;
    let t = setup_idle(Duration::from_secs(60)).await;
    let component = t.runtime.compile(&t.v2).unwrap();
    t.service
        .apply_deploy(2, &[("c".into(), component, t.v2.clone())])
        .unwrap();
    let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{}/actor/c/s", t.addr))
        .await
        .unwrap();
    let mut rules = wasmkeep_server::AccessRules::default();
    rules.set("c", wasmkeep_server::Access::Internal);
    let component = t.runtime.compile(&t.v2).unwrap();
    t.service
        .apply_version(
            3,
            &[("c".into(), component, t.v2.clone())],
            &rules,
            &std::collections::BTreeMap::new(),
        )
        .unwrap();
    ws.send(Message::text("still here?")).await.unwrap();
    let closed = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match ws.next().await {
                Some(Ok(Message::Close(frame))) => return frame.map(|f| u16::from(f.code)),
                Some(Ok(_)) => {}
                other => panic!("expected a close frame, got {other:?}"),
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(closed, Some(1008));
}
