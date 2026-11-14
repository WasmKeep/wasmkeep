//! Database lifetime and admission after a guest future is cancelled.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code

use std::sync::{Arc, mpsc};
use std::time::Duration;

use tokio::sync::oneshot;
use wasmkeep_runtime::{ActorAddress, BoxError, MailboxConfig, MailboxError, Mailboxes};
use wasmkeep_storage::{Storage, StorageConfig};

use super::{ActorConfig, ActorHost, ActorService, Sockets};

struct Fixture {
    _directory: tempfile::TempDir,
    storage: Storage,
    mailboxes: Mailboxes<ActorHost>,
}

fn address(name: &str) -> ActorAddress {
    ActorAddress::new("tenant", "class", name).unwrap()
}

fn fixture(idle_timeout: Duration) -> Fixture {
    let directory = tempfile::tempdir().unwrap();
    let storage = Storage::new(
        StorageConfig::new(directory.path()),
        crate::state_dir(&crate::StateRoot::new("", "tenant", "default")),
    );
    let factory_storage = storage.clone();
    let mailboxes = Mailboxes::new_with_residency(
        MailboxConfig {
            idle_timeout,
            max_resident: 1,
            ..MailboxConfig::default()
        },
        move |address: ActorAddress, residency| {
            let storage = factory_storage.clone();
            async move {
                let db = storage.open(address.class(), address.name())?;
                Ok::<_, BoxError>(ActorHost::new(
                    residency.retain(db),
                    Arc::<Sockets>::default(),
                ))
            }
        },
    );
    Fixture {
        _directory: directory,
        storage,
        mailboxes,
    }
}

async fn within<F: std::future::Future>(future: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(5), future)
        .await
        .expect("operation did not finish")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_db_work_and_cancelled_drain_preserve_the_connection() {
    let f = fixture(Duration::from_secs(60));
    let actor = address("recover");
    let (started, ready) = oneshot::channel();
    // A dropped sender also releases the worker if an assertion fails.
    let (release, held) = mpsc::channel();
    let interrupted = f.mailboxes.submit(&actor, move |host| {
        Box::pin(async move {
            tokio::time::timeout(
                Duration::from_millis(30),
                host.with_db(move |db| {
                    started.send(()).unwrap();
                    held.recv().unwrap();
                    db.set_meta("after-cancellation", "committed").unwrap();
                }),
            )
            .await
            .is_err()
        })
    });
    within(ready).await.unwrap();
    assert!(within(interrupted).await.unwrap());

    // A second cancellation must not take the saved JoinHandle out of the
    // host. Otherwise the worker's connection is lost again at this await.
    let interrupted_drain = f.mailboxes.submit(&actor, |host| {
        Box::pin(async move {
            tokio::time::timeout(Duration::from_millis(30), host.drain_db())
                .await
                .is_err()
        })
    });
    assert!(within(interrupted_drain).await.unwrap());
    release.send(()).unwrap();

    let recovered = f.mailboxes.submit(&actor, |host| {
        Box::pin(async move {
            host.drain_db().await.unwrap();
            host.with_db(|db| db.meta("after-cancellation"))
                .await
                .unwrap()
                .unwrap()
        })
    });
    assert_eq!(
        within(recovered).await.unwrap().as_deref(),
        Some("committed")
    );
    f.mailboxes.close();
    assert!(f.mailboxes.drained(Duration::from_secs(5)).await);
    assert!(!f.storage.is_open(&f.storage.db_path("class", "recover")));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_detached_db_worker_keeps_capacity_until_the_connection_closes() {
    let f = fixture(Duration::from_millis(10));
    let actor = address("retiring");
    let path = f.storage.db_path("class", "retiring");
    let (started, ready) = oneshot::channel();
    let (release, held) = mpsc::channel();
    let interrupted = f.mailboxes.submit(&actor, move |host| {
        Box::pin(async move {
            tokio::time::timeout(
                Duration::from_millis(30),
                host.with_db(move |db| {
                    started.send(()).unwrap();
                    held.recv().unwrap();
                    db.alarm().unwrap();
                }),
            )
            .await
            .is_err()
        })
    });
    within(ready).await.unwrap();
    assert!(within(interrupted).await.unwrap());
    within(async {
        while f.mailboxes.resident() != 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;

    // The mailbox and ActorHost are gone, but their detached worker still
    // owns a connection. A new actor must not consume that occupied slot.
    assert!(f.storage.is_open(&path));
    let refused = f
        .mailboxes
        .submit(&address("new"), |_host| Box::pin(async {}))
        .await;
    assert!(matches!(refused, Err(MailboxError::Capacity(_))));
    assert!(!f.storage.db_path("class", "new").exists());
    f.mailboxes.close();
    assert!(!f.mailboxes.drained(Duration::from_millis(30)).await);

    release.send(()).unwrap();
    assert!(f.mailboxes.drained(Duration::from_secs(5)).await);
    // This verifies the actual detached connection eventually closes.
    // Retained<T>'s runtime test separately observes the live reservation
    // count inside the resource destructor, proving the strict drop order.
    assert!(
        !f.storage.is_open(&path),
        "detached connection remained open"
    );
}

#[path = "../../tests/support/guests.rs"]
mod guests;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_alarm_refused_for_capacity_retries_after_a_gated_blocker() {
    use std::net::SocketAddr;
    use std::sync::OnceLock;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};
    use wasmkeep_runtime::{Runtime, RuntimeConfig};

    async fn request(addr: SocketAddr, method: &str, actor_path: &str) -> (u16, String) {
        let mut stream = TcpStream::connect(addr).await.unwrap();
        let req = format!(
            "{method} /actor/test/{actor_path} HTTP/1.1\r\nHost: t\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
        );
        stream.write_all(req.as_bytes()).await.unwrap();
        let mut response = String::new();
        within(stream.read_to_string(&mut response)).await.unwrap();
        let status = response.split_whitespace().nth(1).unwrap().parse().unwrap();
        (status, response)
    }

    fn stored_count(db: &mut wasmkeep_storage::ActorDb, table: &str) -> i64 {
        let rows = db
            .query(&format!("SELECT count(*) FROM {table}"), &[])
            .unwrap();
        match &rows.values[0][0] {
            wasmkeep_storage::SqlValue::Integer(value) => *value,
            _ => panic!("expected an integer count"),
        }
    }

    static WASM: OnceLock<Vec<u8>> = OnceLock::new();
    let wasm = WASM.get_or_init(|| guests::build_guest("tests/guests/test-actor", "test_actor"));
    let data = tempfile::tempdir().unwrap();
    let runtime = Runtime::new(&RuntimeConfig::default()).unwrap();
    let component = runtime.compile(wasm).unwrap();
    let mut config = ActorConfig::new(data.path());
    config.mailbox.max_resident = 1;
    config.mailbox.idle_timeout = Duration::from_secs(600);
    config.egress.allow.push("127.0.0.1/32".parse().unwrap());
    let service = ActorService::new(
        runtime,
        config,
        vec![("test".into(), component, wasm.clone())],
    )
    .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (stop, stopped) = oneshot::channel();
    let server = tokio::spawn(crate::serve(listener, service.clone(), async {
        let _ = stopped.await;
    }));

    // Initialize the actor's real schema and persisted alarm, safely outside
    // the test deadline. Only after the blocker is admitted do we make it due.
    let (status, response) = request(addr, "POST", "alarm/alarm?in=86400000").await;
    assert_eq!(status, 200, "{response}");
    let alarm_address = ActorAddress::new("default", "test", "alarm").unwrap();
    let actor_storage = service
        .inner
        .mailboxes
        .submit(&alarm_address, |state| {
            Box::pin(async move { state.storage.clone() })
        })
        .await
        .unwrap();
    let outbound = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let outbound_port = outbound.local_addr().unwrap().port();
    let (entered, admitted) = oneshot::channel();
    let (release, held) = oneshot::channel();
    let gate = tokio::spawn(async move {
        let (mut stream, _) = outbound.accept().await.unwrap();
        let mut headers = Vec::new();
        while !headers.windows(4).any(|part| part == b"\r\n\r\n") {
            let mut part = [0; 1024];
            let count = stream.read(&mut part).await.unwrap();
            assert_ne!(count, 0, "outbound request closed before its headers");
            headers.extend_from_slice(&part[..count]);
        }
        entered.send(()).unwrap();
        let _ = held.await;
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok")
            .await
            .unwrap();
    });
    let blocker = tokio::spawn(async move {
        let path = format!("blocker/fetch?url=http%3A%2F%2F127.0.0.1%3A{outbound_port}%2F");
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let (status, response) = request(addr, "GET", &path).await;
            if status == 200 {
                assert!(response.contains("200:ok"), "{response}");
                return;
            }
            assert_eq!(status, 503, "{response}");
            assert!(
                response
                    .lines()
                    .any(|line| line.eq_ignore_ascii_case("retry-after: 1")),
                "{response}"
            );
            assert!(
                tokio::time::Instant::now() < deadline,
                "blocker never admitted"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    });
    within(admitted).await.unwrap();

    // Receiving the outbound request proves the blocker is running in the
    // sole slot. Keep it there with the channel, not a guessed sleep interval.
    let storage = Storage::new(
        StorageConfig::new(data.path()),
        crate::state_dir(&crate::StateRoot::new("", "default", "default")),
    );
    let path = actor_storage.db_path("test", "alarm");
    assert!(!actor_storage.is_open(&path));
    let mut observed = storage.open("test", "alarm").unwrap();
    assert_eq!(stored_count(&mut observed, "attempts"), 0);
    assert_eq!(stored_count(&mut observed, "fired"), 0);
    let due = crate::alarm::now_ms();
    observed.set_alarm(due).unwrap();
    let retries = service.inner.alarms.admission_retries();
    service.inner.alarms.set(alarm_address, due);
    tokio::time::timeout(Duration::from_secs(2), async {
        while service.inner.alarms.admission_retries() == retries {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("timer did not reschedule the refused alarm");
    assert!(!blocker.is_finished());
    assert_eq!(stored_count(&mut observed, "attempts"), 0);
    assert_eq!(stored_count(&mut observed, "fired"), 0);

    release.send(()).unwrap();
    within(gate).await.unwrap();
    within(blocker).await.unwrap();
    // No subsequent HTTP request or mailbox submission wakes this actor.
    within(async {
        while stored_count(&mut observed, "fired") == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert_eq!(stored_count(&mut observed, "fired"), 1);
    assert_eq!(stored_count(&mut observed, "attempts"), 1);
    let _ = stop.send(());
    within(server).await.unwrap().unwrap();
}

#[tokio::test]
async fn an_actor_of_another_tenant_never_starts() {
    use wasmkeep_runtime::{Runtime, RuntimeConfig};
    let wasm = guests::build_guest("tests/guests/test-actor", "test_actor");
    let data = tempfile::tempdir().unwrap();
    let runtime = Runtime::new(&RuntimeConfig::default()).unwrap();
    let component = runtime.compile(&wasm).unwrap();
    let service = ActorService::new(
        runtime,
        ActorConfig::new(data.path()),
        vec![("test".into(), component, wasm)],
    )
    .unwrap();
    // Its database would be the root's tenant's (ADR 0022): refused,
    // though its class is served (the same name of this tenant starts
    // below).
    let other = ActorAddress::new("other", "test", "x").unwrap();
    let started = service
        .inner
        .mailboxes
        .submit(&other, |state| {
            Box::pin(async move { state.storage.clone() })
        })
        .await;
    match started {
        Err(MailboxError::Init { source, .. }) => {
            assert!(
                source.to_string().contains("not of this node's tenant"),
                "{source}"
            );
        }
        other => panic!("started: {other:?}"),
    }
    let mine = ActorAddress::new("default", "test", "x").unwrap();
    let storage = service
        .inner
        .mailboxes
        .submit(&mine, |state| {
            Box::pin(async move { state.storage.clone() })
        })
        .await
        .unwrap();
    assert_eq!(storage.state().tenant, "default");
}

/// A service running the test actor as class `test`, each actor's queue
/// holding `queue_capacity` events, the class requiring `requires`.
fn call_service(data: &std::path::Path, queue_capacity: usize, requires: &[&str]) -> ActorService {
    use std::sync::OnceLock;
    use wasmkeep_runtime::{Runtime, RuntimeConfig};
    static WASM: OnceLock<Vec<u8>> = OnceLock::new();
    let wasm = WASM.get_or_init(|| guests::build_guest("tests/guests/test-actor", "test_actor"));
    let runtime = Runtime::new(&RuntimeConfig::default()).unwrap();
    let component = runtime.compile(wasm).unwrap();
    let mut config = ActorConfig::new(data);
    config.mailbox.queue_capacity = queue_capacity;
    if !requires.is_empty() {
        config.requires.insert(
            "test".into(),
            requires.iter().map(|r| (*r).to_owned()).collect(),
        );
    }
    ActorService::new(
        runtime,
        config,
        vec![("test".into(), component, wasm.clone())],
    )
    .unwrap()
}

/// `actors.call` from `<tenant>/test/caller` to `test/<callee>`, `GET
/// /fired`.
async fn call_from(
    service: &ActorService,
    tenant: &str,
    callee: &str,
) -> Result<super::CallResponse, super::CallError> {
    let ctx = super::CallContext {
        service: None,
        me: ActorAddress::new(tenant, "test", "caller").unwrap(),
        chain: Arc::from(Vec::new()),
    };
    let request = super::CallRequest {
        method: "GET".into(),
        path: "/fired".into(),
        headers: Vec::new(),
        body: Vec::new(),
    };
    let target = service
        .inner
        .call_target(ctx.me.tenant(), &format!("test/{callee}"))?;
    let wall = service.inner.wall;
    within(Arc::clone(&service.inner).call(
        &super::Caller::Actor(ctx.me.clone()),
        &ctx.chain,
        target,
        request,
        wall,
    ))
    .await
}

/// `wasmkeep_admission_refused_total{reason}` (this process's: nextest
/// runs each test alone).
fn refusals(reason: &str) -> u64 {
    let page = crate::metrics::render(crate::metrics::Gauges::default());
    let series = format!("wasmkeep_admission_refused_total{{reason=\"{reason}\"}} ");
    page.lines()
        .find_map(|l| l.strip_prefix(&series))
        .unwrap_or_else(|| panic!("no {series}in:\n{page}"))
        .parse()
        .unwrap()
}

/// A call to an actor whose queue is full fails as `callee overloaded`,
/// counted as such, and the callee serves the next call once its queue
/// drains.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_call_to_a_full_queue_fails_as_overloaded() {
    let data = tempfile::tempdir().unwrap();
    let service = call_service(data.path(), 2, &[]);
    let callee = ActorAddress::new("default", "test", "callee").unwrap();
    // One event holds the callee; two more fill its queue.
    let (started, running) = oneshot::channel();
    let (release, released) = oneshot::channel::<()>();
    let blocker = tokio::spawn(service.inner.mailboxes.submit(&callee, move |_state| {
        Box::pin(async move {
            let _ = started.send(());
            let _ = released.await;
        })
    }));
    within(running).await.unwrap();
    let queued: Vec<_> = (0..2)
        .map(|_| {
            tokio::spawn(
                service
                    .inner
                    .mailboxes
                    .submit(&callee, |_state| Box::pin(async {})),
            )
        })
        .collect();
    let before = refusals("overloaded");
    match call_from(&service, "default", "callee").await {
        Err(super::CallError::Failed(e)) => assert_eq!(e, "callee overloaded"),
        other => panic!("{:?}", other.map(|r| r.status)),
    }
    assert_eq!(refusals("overloaded") - before, 1);
    assert_eq!(refusals("unavailable"), 0);
    release.send(()).unwrap();
    within(blocker).await.unwrap().unwrap();
    for q in queued {
        within(q).await.unwrap().unwrap();
    }
    let served = call_from(&service, "default", "callee").await.unwrap();
    assert_eq!((served.status, served.body.as_slice()), (200, &b"0\n"[..]));
}

/// A call to an actor that cannot run now (here its class requires a
/// value not set, ADR 0016) fails with why, counted as unavailable; the
/// callee runs once the value is set.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_call_to_an_actor_that_cannot_run_now_fails_as_unavailable() {
    let data = tempfile::tempdir().unwrap();
    let service = call_service(data.path(), 16, &["TOKEN"]);
    let before = refusals("unavailable");
    match call_from(&service, "default", "callee").await {
        Err(super::CallError::Failed(e)) => assert_eq!(e, "TOKEN is not set"),
        other => panic!("{:?}", other.map(|r| r.status)),
    }
    assert_eq!(refusals("unavailable") - before, 1);
    assert_eq!(refusals("overloaded"), 0);
    let mut secrets = crate::secrets::SecretSet::default();
    secrets.set("test", "TOKEN", "set now").unwrap();
    service.apply_secrets(secrets);
    let served = call_from(&service, "default", "callee").await.unwrap();
    assert_eq!((served.status, served.body.as_slice()), (200, &b"0\n"[..]));
}

/// A call whose callee's mailbox cannot start it fails with why, counted
/// as unavailable. A factory refusal reaches that: here the callee would be
/// another tenant's (ADR 0022), as the caller is.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_call_to_an_actor_whose_state_cannot_start_fails_as_unavailable() {
    let data = tempfile::tempdir().unwrap();
    let service = call_service(data.path(), 16, &[]);
    let before = refusals("unavailable");
    match call_from(&service, "other", "callee").await {
        Err(super::CallError::Failed(e)) => {
            assert_eq!(e, "actor other/test/callee failed to start");
        }
        other => panic!("{:?}", other.map(|r| r.status)),
    }
    assert_eq!(refusals("unavailable") - before, 1);
    assert_eq!(refusals("overloaded"), 0);
}
