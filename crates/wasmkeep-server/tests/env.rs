//! `env`: configuration and secrets per class, with tests/guests/test-actor.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code

use std::net::SocketAddr;
use std::path::Path;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use wasmkeep_runtime::{Runtime, RuntimeConfig};
use wasmkeep_server::{ActorConfig, ActorEnv, ActorService, env};

#[path = "support/guests.rs"]
mod guests;

/// Classes `t` (with values) and `u` (without), both the test actor.
fn service(data: &Path, actor_env: ActorEnv) -> ActorService {
    let wasm = guests::build_guest("tests/guests/test-actor", "test_actor");
    let runtime = Runtime::new(&RuntimeConfig {
        max_instances: 8,
        ..RuntimeConfig::default()
    })
    .unwrap();
    let component = runtime.compile(&wasm).unwrap();
    let mut config = ActorConfig::new(data);
    config.env = actor_env;
    ActorService::new(
        runtime,
        config,
        vec![
            ("t".into(), component.clone(), wasm.clone()),
            ("u".into(), component, wasm),
        ],
    )
    .unwrap()
}

/// The de-chunked body of a GET.
async fn get(addr: SocketAddr, path: &str) -> String {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let req = format!("GET {path} HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n");
    stream.write_all(req.as_bytes()).await.unwrap();
    let mut raw = String::new();
    stream.read_to_string(&mut raw).await.unwrap();
    assert!(raw.starts_with("HTTP/1.1 200"), "{raw}");
    let body = raw.split_once("\r\n\r\n").map_or("", |(_, b)| b);
    let body = match body.split_once("\r\n") {
        Some((size, rest)) if usize::from_str_radix(size.trim(), 16).is_ok() => {
            rest.split("\r\n").next().unwrap_or("")
        }
        _ => body,
    };
    body.trim_end().to_owned()
}

#[tokio::test(flavor = "multi_thread")]
async fn each_class_reads_its_own_values_and_nothing_reaches_wasi() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("t.env");
    std::fs::write(
        &file,
        "# the t class\nexport TOKEN=\"s3cret value\"\nURL=https://example.com/?a=b\n",
    )
    .unwrap();
    let (vars, _) = env::read_file(&file).unwrap();
    let mut actor_env = ActorEnv::default();
    actor_env.extend("t", vars);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let svc = service(&dir.path().join("data"), actor_env);
    let (_stop, stopped) = tokio::sync::oneshot::channel::<()>();
    tokio::spawn(wasmkeep_server::serve(listener, svc, async {
        let _ = stopped.await;
    }));

    assert_eq!(
        get(addr, "/actor/t/a/env?name=TOKEN").await,
        "s3cret value wasi=unset"
    );
    assert_eq!(
        get(addr, "/actor/t/other/env?name=URL").await,
        "https://example.com/?a=b wasi=unset",
        "every actor of the class"
    );
    assert_eq!(
        get(addr, "/actor/t/a/env?name=MISSING").await,
        "(none) wasi=unset"
    );
    assert_eq!(
        get(addr, "/actor/u/a/env?name=TOKEN").await,
        "(none) wasi=unset",
        "another class does not see t's values"
    );
    // The guest probes WASI with the host's own variables too: none leak.
    assert_eq!(
        get(addr, "/actor/u/a/env?name=PATH").await,
        "(none) wasi=unset"
    );
}

/// The status line's code of a GET.
async fn status(addr: SocketAddr, path: &str) -> u16 {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let req = format!("GET {path} HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n");
    stream.write_all(req.as_bytes()).await.unwrap();
    let mut raw = String::new();
    stream.read_to_string(&mut raw).await.unwrap();
    raw.split_whitespace().nth(1).unwrap().parse().unwrap()
}

/// Serves `svc` on a fresh port until the returned sender is dropped.
async fn serve(svc: ActorService) -> (SocketAddr, tokio::sync::oneshot::Sender<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    tokio::spawn(wasmkeep_server::serve(listener, svc, async {
        let _ = stopped.await;
    }));
    (addr, stop)
}

fn vars(pairs: &[(&str, &str)]) -> env::Vars {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
        .collect()
}

/// ADR 0016: a class's values are the deployed configuration, then the
/// app's secrets, then the node's `--env`, by name. A secrets change
/// reaches a resident actor at its next event, and a deploy after it keeps
/// the secrets.
#[tokio::test(flavor = "multi_thread")]
async fn secrets_sit_between_the_deployed_configuration_and_env() {
    use wasmkeep_server::secrets::SecretSet;
    let dir = tempfile::tempdir().unwrap();
    let mut actor_env = ActorEnv::default();
    actor_env.extend("t", vars(&[("ENV_WINS", "from env")]));
    let svc = service(&dir.path().join("data"), actor_env);
    let mut config = std::collections::BTreeMap::new();
    config.insert(
        "t".to_owned(),
        vars(&[
            ("PLAIN", "from config"),
            ("SECRET_WINS", "from config"),
            ("ENV_WINS", "from config"),
        ]),
    );
    let wasm = guests::build_guest("tests/guests/test-actor", "test_actor");
    let component = svc.runtime().compile(&wasm).unwrap();
    let classes = vec![("t".to_owned(), component, wasm)];
    svc.apply_version(
        1,
        &classes,
        &wasmkeep_server::access::AccessRules::default(),
        &config,
    )
    .unwrap();
    let (addr, _stop) = serve(svc.clone()).await;
    let read =
        |name: &'static str| async move { get(addr, &format!("/actor/t/a/env?name={name}")).await };
    assert_eq!(read("PLAIN").await, "from config wasi=unset");

    let mut secrets = SecretSet::default();
    secrets.set("t", "SECRET_WINS", "from secrets").unwrap();
    secrets.set("t", "ENV_WINS", "from secrets").unwrap();
    secrets.set("t", "PEM", "line one\nline two").unwrap();
    svc.apply_secrets(secrets);
    // The same resident actor, at its next event.
    assert_eq!(read("SECRET_WINS").await, "from secrets wasi=unset");
    assert_eq!(read("ENV_WINS").await, "from env wasi=unset");
    assert_eq!(read("PLAIN").await, "from config wasi=unset");

    // A deploy rebuilds values from all three layers.
    config
        .get_mut("t")
        .unwrap()
        .insert("PLAIN".into(), "deployed again".into());
    svc.apply_version(
        2,
        &classes,
        &wasmkeep_server::access::AccessRules::default(),
        &config,
    )
    .unwrap();
    assert_eq!(read("PLAIN").await, "deployed again wasi=unset");
    assert_eq!(read("SECRET_WINS").await, "from secrets wasi=unset");
}

/// ADR 0016: a node that cannot open the app's secrets refuses events
/// (503) and is not ready, rather than run actors with an older set.
#[tokio::test(flavor = "multi_thread")]
async fn a_node_that_cannot_open_the_secrets_refuses_events() {
    let dir = tempfile::tempdir().unwrap();
    let svc = service(&dir.path().join("data"), ActorEnv::default());
    let (addr, _stop) = serve(svc.clone()).await;
    assert_eq!(status(addr, "/actor/t/a/env?name=X").await, 200);
    svc.set_secrets_blocked(true);
    assert_eq!(
        status(addr, "/actor/t/a/env?name=X").await,
        503,
        "a resident actor"
    );
    assert_eq!(
        status(addr, "/actor/t/new/env?name=X").await,
        503,
        "a new actor"
    );
    assert_eq!(svc.not_ready(), Some("cannot open the app's secrets"));
    svc.set_secrets_blocked(false);
    assert_eq!(status(addr, "/actor/t/a/env?name=X").await, 200);
}

/// ADR 0016: a node following the bucket picks up a change within a poll,
/// and blocks on a set sealed under a key it does not hold.
#[tokio::test(flavor = "multi_thread")]
async fn a_node_follows_the_secrets_in_the_bucket() {
    use std::sync::Arc;
    use std::time::Duration;
    use wasmkeep_server::secret_store::SecretStore;
    use wasmkeep_server::secrets::{SecretSet, SecretsKey};
    let dir = tempfile::tempdir().unwrap();
    let svc = service(&dir.path().join("data"), ActorEnv::default());
    let (addr, _stop) = serve(svc.clone()).await;
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let bucket = SecretStore::new(Arc::clone(&store), "", "default", "shop");
    let (key, other) = (
        SecretsKey::generate().unwrap(),
        SecretsKey::generate().unwrap(),
    );
    tokio::spawn(bucket.clone().follow(
        svc.clone(),
        vec![key.clone()],
        None,
        Duration::from_millis(50),
        Duration::from_millis(300),
    ));

    let set = |value: &'static str| move |s: &mut SecretSet| s.set("t", "TOKEN", value);
    bucket
        .change(std::slice::from_ref(&key), &key, set("one"))
        .await
        .unwrap()
        .unwrap();
    let wait_for = |want: &'static str| async move {
        let started = std::time::Instant::now();
        loop {
            if get(addr, "/actor/t/a/env?name=TOKEN").await == want {
                return;
            }
            assert!(
                started.elapsed() < Duration::from_secs(5),
                "never read {want:?}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    };
    wait_for("one wasi=unset").await;
    bucket
        .change(std::slice::from_ref(&key), &key, set("two"))
        .await
        .unwrap()
        .unwrap();
    wait_for("two wasi=unset").await;

    // Re-sealed under a key this node does not hold: it blocks at once.
    bucket
        .change(std::slice::from_ref(&key), &other, |_| {
            Ok::<(), std::convert::Infallible>(())
        })
        .await
        .unwrap()
        .unwrap();
    let started = std::time::Instant::now();
    while svc.not_ready() != Some("cannot open the app's secrets") {
        assert!(started.elapsed() < Duration::from_secs(5), "never blocked");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(status(addr, "/actor/t/a/env?name=TOKEN").await, 503);
}

/// ADR 0016: a deploy and a secrets change landing at the same moment both
/// take effect: neither layer is lost to the other (review finding).
#[tokio::test(flavor = "multi_thread")]
async fn a_deploy_and_a_secrets_change_at_once_keep_both_layers() {
    use wasmkeep_server::secrets::SecretSet;
    let dir = tempfile::tempdir().unwrap();
    let svc = service(&dir.path().join("data"), ActorEnv::default());
    let wasm = guests::build_guest("tests/guests/test-actor", "test_actor");
    let component = svc.runtime().compile(&wasm).unwrap();
    let classes = std::sync::Arc::new(vec![("t".to_owned(), component, wasm)]);
    let (addr, _stop) = serve(svc.clone()).await;
    for i in 0..300_u64 {
        let mut config = std::collections::BTreeMap::new();
        config.insert("t".to_owned(), vars(&[("PLAIN", &format!("c{i}"))]));
        let mut secrets = SecretSet::default();
        secrets.set("t", "SECRET", &format!("s{i}")).unwrap();
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
        let deploy = {
            let (svc, classes, barrier) = (
                svc.clone(),
                std::sync::Arc::clone(&classes),
                std::sync::Arc::clone(&barrier),
            );
            std::thread::spawn(move || {
                barrier.wait();
                svc.apply_version(
                    i + 1,
                    &classes,
                    &wasmkeep_server::access::AccessRules::default(),
                    &config,
                )
                .unwrap();
            })
        };
        let change = {
            let (svc, barrier) = (svc.clone(), std::sync::Arc::clone(&barrier));
            std::thread::spawn(move || {
                barrier.wait();
                svc.apply_secrets(secrets);
            })
        };
        deploy.join().unwrap();
        change.join().unwrap();
        if i % 30 == 0 || i == 299 {
            assert_eq!(
                get(addr, "/actor/t/a/env?name=PLAIN").await,
                format!("c{i} wasi=unset"),
                "round {i}"
            );
            assert_eq!(
                get(addr, "/actor/t/a/env?name=SECRET").await,
                format!("s{i} wasi=unset"),
                "round {i}"
            );
        }
    }
}

/// ADR 0016: a node with no key blocks as soon as the app has secrets, and
/// one whose `current` names an object it cannot reach blocks after the
/// stale window, never running on the older set meanwhile past it.
#[tokio::test(flavor = "multi_thread")]
async fn a_node_blocks_without_the_key_or_when_the_current_set_is_unreachable() {
    use object_store::ObjectStoreExt as _;
    use std::sync::Arc;
    use std::time::Duration;
    use wasmkeep_server::secret_store::SecretStore;
    use wasmkeep_server::secrets::{SecretSet, SecretsKey};
    let key = SecretsKey::generate().unwrap();
    let blocked = |svc: &ActorService| svc.not_ready() == Some("cannot open the app's secrets");
    let wait_blocked = |svc: ActorService| async move {
        let started = std::time::Instant::now();
        while !blocked(&svc) {
            assert!(started.elapsed() < Duration::from_secs(5), "never blocked");
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    };
    let one = |s: &mut SecretSet| s.set("t", "TOKEN", "v");

    // No key: blocks once the app has secrets.
    let dir = tempfile::tempdir().unwrap();
    let svc = service(&dir.path().join("data"), ActorEnv::default());
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let bucket = SecretStore::new(Arc::clone(&store), "", "default", "shop");
    tokio::spawn(bucket.clone().follow(
        svc.clone(),
        Vec::new(),
        None,
        Duration::from_millis(25),
        Duration::from_millis(200),
    ));
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!blocked(&svc), "no secrets yet: nothing to open");
    bucket
        .change(std::slice::from_ref(&key), &key, one)
        .await
        .unwrap()
        .unwrap();
    wait_blocked(svc).await;

    // `current` names an object that is not there: blocks after the window.
    let dir = tempfile::tempdir().unwrap();
    let svc = service(&dir.path().join("data"), ActorEnv::default());
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let bucket = SecretStore::new(Arc::clone(&store), "", "default", "shop");
    let seq = bucket
        .change(std::slice::from_ref(&key), &key, one)
        .await
        .unwrap()
        .unwrap();
    tokio::spawn(bucket.clone().follow(
        svc.clone(),
        vec![key.clone()],
        Some(seq),
        Duration::from_millis(25),
        Duration::from_millis(200),
    ));
    store
        .put(
            &object_store::path::Path::from("apps/default/shop/secrets/current"),
            "00000000000000ff\n".into(),
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!blocked(&svc), "within the window");
    wait_blocked(svc).await;
}

/// ADR 0016: secrets opened at start reach the very first event.
#[tokio::test(flavor = "multi_thread")]
async fn secrets_opened_at_start_reach_the_first_event() {
    let dir = tempfile::tempdir().unwrap();
    let wasm = guests::build_guest("tests/guests/test-actor", "test_actor");
    let runtime = Runtime::new(&RuntimeConfig {
        max_instances: 8,
        ..RuntimeConfig::default()
    })
    .unwrap();
    let component = runtime.compile(&wasm).unwrap();
    let mut config = ActorConfig::new(dir.path().join("data"));
    config.secrets.set("t", "TOKEN", "at start").unwrap();
    let svc = ActorService::new(runtime, config, vec![("t".into(), component, wasm)]).unwrap();
    let (addr, _stop) = serve(svc).await;
    assert_eq!(
        get(addr, "/actor/t/a/env?name=TOKEN").await,
        "at start wasi=unset"
    );
}

/// ADR 0016: a class that requires a value refuses its events (503) while
/// the value is not set, and runs once a secret sets it.
#[tokio::test(flavor = "multi_thread")]
async fn a_class_missing_a_required_value_refuses_its_events() {
    use wasmkeep_server::secrets::SecretSet;
    let dir = tempfile::tempdir().unwrap();
    let wasm = guests::build_guest("tests/guests/test-actor", "test_actor");
    let runtime = Runtime::new(&RuntimeConfig {
        max_instances: 8,
        ..RuntimeConfig::default()
    })
    .unwrap();
    let component = runtime.compile(&wasm).unwrap();
    let mut config = ActorConfig::new(dir.path().join("data"));
    config.requires.insert(
        "t".into(),
        std::collections::BTreeSet::from(["TOKEN".to_owned()]),
    );
    let svc = ActorService::new(
        runtime,
        config,
        vec![
            ("t".into(), component.clone(), wasm.clone()),
            ("u".into(), component, wasm),
        ],
    )
    .unwrap();
    let (addr, _stop) = serve(svc.clone()).await;
    assert_eq!(status(addr, "/actor/t/a/env?name=TOKEN").await, 503);
    assert_eq!(
        status(addr, "/actor/u/a/env?name=TOKEN").await,
        200,
        "only that class"
    );
    let mut secrets = SecretSet::default();
    secrets.set("t", "TOKEN", "set now").unwrap();
    svc.apply_secrets(secrets);
    assert_eq!(
        get(addr, "/actor/t/a/env?name=TOKEN").await,
        "set now wasi=unset"
    );
}

/// ADR 0016: a running node following its app takes required names with
/// each deploy, and with a rollback.
#[tokio::test(flavor = "multi_thread")]
async fn required_names_reach_a_following_node_with_deploys_and_rollbacks() {
    use std::collections::BTreeSet;
    use std::sync::Arc;
    use wasmkeep_server::apps::{AppRef, Changes};
    let dir = tempfile::tempdir().unwrap();
    let svc = service(&dir.path().join("data"), ActorEnv::default());
    let (addr, _stop) = serve(svc.clone()).await;
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let app = AppRef::new(store, "", "default", "shop");
    let wasm = guests::build_guest("tests/guests/test-actor", "test_actor");
    let deploy = |requires: Vec<(String, BTreeSet<String>)>, classes: bool| {
        let (app, svc, wasm) = (app.clone(), svc.clone(), wasm.clone());
        async move {
            let changes = Changes {
                classes: classes.then(|| vec![("t".to_owned(), wasm)]),
                requires,
                ..Changes::default()
            };
            app.deploy_changes(svc.runtime(), changes).await.unwrap();
            assert!(app.update(&svc).await.unwrap());
        }
    };
    let token = || BTreeSet::from(["TOKEN".to_owned()]);
    deploy(vec![("t".into(), token())], true).await;
    assert_eq!(status(addr, "/actor/t/a/env?name=TOKEN").await, 503);
    deploy(vec![("t".into(), BTreeSet::new())], false).await;
    assert_eq!(status(addr, "/actor/t/a/env?name=TOKEN").await, 200);
    app.rollback(false).await.unwrap();
    assert!(app.update(&svc).await.unwrap());
    assert_eq!(
        status(addr, "/actor/t/a/env?name=TOKEN").await,
        503,
        "the rollback brings the requirement back"
    );
}
