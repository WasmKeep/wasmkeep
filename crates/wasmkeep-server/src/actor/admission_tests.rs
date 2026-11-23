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
pub(super) mod guests;

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
        deadline: tokio::time::Instant::now() + Duration::from_secs(30),
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
        &super::routing::BodyBudget::one(),
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

/// The watch is checked at the host boundary, even before `limit` gets
/// another poll. No guest SQL or outward effect runs after a batched stop.
#[tokio::test]
async fn batched_failure_refuses_storage_and_outward_calls() {
    use crate::replica::{Stop, UploadFailure};
    use wasmkeep_wit::wasmkeep::actor::{actors, alarms, calls, storage, websockets};
    let f = fixture(Duration::from_secs(60));
    f.mailboxes
        .submit(&address("failed"), |host| {
            Box::pin(async move {
                let (sender, flag) = tokio::sync::watch::channel(Some(Stop {
                    reason: UploadFailure::Late,
                    batched: true,
                }));
                host.replica_stop = Some(flag);
                let sql = "CREATE TABLE forbidden(x)".to_owned();
                for error in [
                    storage::Host::exec(host, sql.clone(), vec![])
                        .await
                        .unwrap()
                        .unwrap_err(),
                    storage::Host::exec_durable(host, sql, vec![])
                        .await
                        .unwrap()
                        .unwrap_err(),
                    storage::Host::query(host, "SELECT 1".into(), vec![])
                        .await
                        .unwrap()
                        .unwrap_err(),
                    storage::Host::flush(host).await.unwrap().unwrap_err(),
                    storage::Host::delete_self_durable(host)
                        .await
                        .unwrap()
                        .unwrap_err(),
                ] {
                    assert!(matches!(
                        error.kind,
                        wasmkeep_wit::wasmkeep::actor::types::SqlErrorKind::Internal
                    ));
                    assert!(error.message.starts_with("replication stopped"));
                }
                assert!(storage::Host::delete_self(host).await.is_err());
                assert!(!host.delete_requested);
                assert!(alarms::Host::set_alarm(host, 10).await.is_err());
                assert!(alarms::Host::clear_alarm(host).await.is_err());
                assert!(alarms::Host::get_alarm(host).await.is_err());
                let request = actors::Request {
                    method: "GET".into(),
                    path: "/".into(),
                    headers: vec![],
                    body: vec![],
                };
                assert!(
                    actors::Host::call(host, "class/other".into(), request.clone())
                        .await
                        .is_err()
                );
                assert!(
                    calls::Host::call_many(host, vec![("class/other".into(), request)])
                        .await
                        .is_err()
                );
                assert!(
                    websockets::Host::send(host, 1, websockets::Message::Text("x".into()))
                        .await
                        .is_err()
                );
                assert!(
                    websockets::Host::close(host, 1, 1000, String::new())
                        .await
                        .is_err()
                );
                // An inline strict failure does not close any of these new gates.
                sender.send_replace(Some(Stop {
                    reason: UploadFailure::Late,
                    batched: false,
                }));
                assert!(
                    storage::Host::exec(host, "CREATE TABLE allowed(x)".into(), vec![])
                        .await
                        .unwrap()
                        .is_ok()
                );
                assert!(
                    storage::Host::query(host, "SELECT * FROM allowed".into(), vec![])
                        .await
                        .unwrap()
                        .is_ok()
                );
                assert!(alarms::Host::set_alarm(host, 10).await.is_ok());
                assert!(
                    websockets::Host::send(host, 1, websockets::Message::Text("x".into()))
                        .await
                        .is_ok()
                );
                assert!(
                    host.with_db(|db| db.query(
                        "SELECT name FROM sqlite_master WHERE name = 'forbidden'",
                        &[]
                    ))
                    .await
                    .unwrap()
                    .unwrap()
                    .values
                    .is_empty()
                );
            })
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn response_outparam_checks_failure_when_set_and_does_not_recall_an_answer() {
    use crate::replica::{Stop, UploadFailure};
    use wasmtime_wasi_http::p2::bindings::http::types::ErrorCode;
    let f = fixture(Duration::from_secs(60));
    f.mailboxes.submit(&address("response"), |host| Box::pin(async move {
        let (sender, flag) = tokio::sync::watch::channel(None);
        host.replica_stop = Some(flag);
        let (tx, rx) = oneshot::channel();
        let out = host.response_outparam(tx).unwrap();
        sender.send_replace(Some(Stop { reason: UploadFailure::Late, batched: true }));
        let callback = host.table.delete(out).unwrap();
        (callback.send)(Err(ErrorCode::HttpRequestDenied));
        assert!(matches!(rx.await.unwrap(), Err(ErrorCode::InternalError(Some(message))) if message == "replication stopped"));
        sender.send_replace(Some(Stop { reason: UploadFailure::Late, batched: false }));
        let (tx, rx) = oneshot::channel();
        let out = host.response_outparam(tx).unwrap();
        let callback = host.table.delete(out).unwrap();
        (callback.send)(Err(ErrorCode::HttpRequestDenied));
        sender.send_replace(Some(Stop { reason: UploadFailure::Late, batched: true }));
        assert!(matches!(rx.await.unwrap(), Err(ErrorCode::HttpRequestDenied)));
    })).await.unwrap();
}

/// A step before a drop cut while its database work runs leaves the
/// connection with that work (`pending_db`), not in the host: the next
/// event's capture check recovers it rather than finding the database
/// "unavailable" and giving a healthy hold up (ADR 0034, section 6).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batched_capture_is_enabled_after_a_cancelled_database_step() {
    let f = fixture(Duration::from_secs(60));
    let (release, held) = mpsc::channel::<()>();
    let enabled = f
        .mailboxes
        .submit(&address("step"), move |host| {
            Box::pin(async move {
                host.with_db(|db| {
                    db.exec("CREATE TABLE t(v)", &[]).unwrap();
                    db.start_capture().unwrap();
                })
                .await
                .unwrap();
                let cut = tokio::time::timeout(
                    Duration::from_millis(30),
                    host.with_db(move |_db| {
                        let _ = held.recv();
                    }),
                )
                .await;
                assert!(cut.is_err(), "the step was cut");
                assert!(host.db.is_none(), "the connection is with the cut work");
                release.send(()).unwrap();
                let token = wasmkeep_storage::CaptureHold::default();
                let enabled = host.ensure_batched_capture(&token).await.map(|_| ());
                let on = host
                    .with_db(|db| db.batched_capture_enabled())
                    .await
                    .unwrap();
                // The token goes before the database it guards.
                drop(token);
                (enabled, on)
            })
        })
        .await
        .unwrap();
    assert_eq!(enabled, (Ok(()), true));
}

async fn activation_deadline(lease_lapse: bool) {
    use super::*;
    use wasmkeep_runtime::{Runtime, RuntimeConfig};
    let data = tempfile::tempdir().unwrap();
    let runtime = Runtime::new(&RuntimeConfig::default()).unwrap();
    let wasm = wat::parse_str(include_str!("../../tests/support/activation.wat")).unwrap();
    let component = runtime.compile(&wasm).unwrap();
    let pre = actor_linker(&runtime)
        .unwrap()
        .instantiate_pre(&component)
        .unwrap();
    let mut config = ActorConfig::new(data.path());
    config.mailbox.max_resident = 1;
    config.mailbox.idle_timeout = Duration::from_secs(600);
    config.wall_per_event = if lease_lapse {
        Duration::from_secs(30)
    } else {
        Duration::from_millis(50)
    };
    let service = ActorService::new(runtime, config, vec![]).unwrap();
    // The start runs before export lookup; no lifecycle exports are needed.
    service.inner.classes.write().unwrap().insert(
        "test".into(),
        Arc::new(ActorClass {
            pre,
            version: "timer".into(),
            seq: 0,
            rules: Arc::default(),
            env: Arc::default(),
            requires: Arc::default(),
            warned: Arc::default(),
            writes: crate::apps::Writes::Strict,
            apart: None,
        }),
    );
    let address = ActorAddress::new("default", "test", "timer").unwrap();
    let cluster = if lease_lapse {
        let store = Arc::new(object_store::memory::InMemory::new());
        let mut config =
            crate::cluster::ClusterConfig::new("activation", "http://activation", store);
        config.heartbeat = Duration::from_millis(20);
        config.lease = wasmkeep_cluster::lease::LeaseConfig::new(1_000, 200, 200, 100).unwrap();
        let cluster =
            Cluster::new(&config, &crate::StateRoot::new("", "default", "default")).unwrap();
        cluster.identity().await.unwrap();
        Some(cluster)
    } else {
        None
    };
    let hold = match &cluster {
        Some(cluster) => Some(Held::new(
            LeaseHold::acquire(cluster, &address).await.unwrap(),
            crate::apps::Writes::Strict,
        )),
        None => None,
    };
    let (tx, rx) = oneshot::channel();
    let (entered, ready) = oneshot::channel();
    let job = service.inner.mailboxes.submit(&address, move |state| {
        Box::pin(async move {
            state.held = hold;
            // With the database already open, activation first yields inside
            // the guest's timer, so the lease ends during instantiation.
            state.db = Some(
                state.residency.retain(
                    state
                        .storage
                        .open(state.address.class(), state.address.name())
                        .unwrap(),
                ),
            );
            entered.send(()).unwrap();
            state
                .request(
                    hyper::Request::new(http_body_util::Empty::<bytes::Bytes>::new()),
                    tx,
                    Arc::from(Vec::new()),
                )
                .await;
            assert!(state.live.is_none());
            assert!(state.db.is_none());
            assert!(state.held.is_none());
        })
    });
    within(ready).await.unwrap();
    if let Some(cluster) = &cluster {
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(cluster.stop_node_lease(Duration::from_secs(1)).await);
    }
    let outcome = within(rx).await.unwrap();
    match outcome {
        Outcome::Refused(why) => {
            assert!(why.contains("activation"), "{why}");
            assert!(
                why.contains(if lease_lapse { "lease" } else { "wall-clock" }),
                "{why}"
            );
        }
        _ => panic!("activation must return a retryable refusal"),
    }
    within(job).await.unwrap();
    within(async {
        while service.inner.mailboxes.contains(&address) {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
    assert_eq!(service.inner.mailboxes.resident(), 0);
    let next = ActorAddress::new("default", "test", "next").unwrap();
    within(
        service
            .inner
            .mailboxes
            .submit(&next, |_| Box::pin(async {})),
    )
    .await
    .unwrap();
    if let Some(cluster) = cluster {
        assert!(cluster.releases_done(Duration::from_secs(2)).await);
        assert!(!cluster.holds_here(&address));
    }
}

#[tokio::test]
async fn activation_deadline_frees_mailbox_and_slot() {
    activation_deadline(false).await;
}

#[tokio::test]
async fn activation_deadline_lease_lapse_frees_mailbox_and_slot() {
    activation_deadline(true).await;
}

/// A start function's blocking SQL worker can outlive its wall deadline.
/// Keep ownership until that worker can no longer commit on the old node.
#[tokio::test]
async fn activation_deadline_drains_write_before_releasing_hold() {
    use super::*;
    use wasmkeep_runtime::{Runtime, RuntimeConfig};
    let data = tempfile::tempdir().unwrap();
    let runtime = Runtime::new(&RuntimeConfig::default()).unwrap();
    let component = runtime
        .compile(
            &wat::parse_str(
                r#"
        (component
          (import "write" (func $write))
          (core func $write (canon lower (func $write)))
          (core module $m
            (import "" "write" (func $write))
            (func $start call $write)
            (start $start))
          (core instance (instantiate $m
            (with "" (instance (export "write" (func $write)))))))
    "#,
            )
            .unwrap(),
        )
        .unwrap();
    let (entered, ready) = oneshot::channel();
    let (release, released) = mpsc::channel();
    let gate = std::sync::Mutex::new(Some((entered, released)));
    let mut linker = actor_linker(&runtime).unwrap();
    linker
        .root()
        .func_wrap_async("write", move |mut store, (): ()| {
            let (entered, released) = gate.lock().unwrap().take().unwrap();
            Box::new(async move {
                // Reach the database near the wall, then hold an INSERT's
                // transaction open without burning CPU or depending on SQL speed.
                tokio::time::sleep(Duration::from_millis(150)).await;
                store
                    .data_mut()
                    .with_db(move |db| {
                        db.exec("CREATE TABLE writes(v)", &[]).unwrap();
                        db.exec("BEGIN IMMEDIATE", &[]).unwrap();
                        db.exec("INSERT INTO writes VALUES (1)", &[]).unwrap();
                        entered.send(()).unwrap();
                        let _ = released.recv();
                        db.exec("COMMIT", &[]).unwrap();
                    })
                    .await?;
                Ok(())
            })
        })
        .unwrap();
    let pre = linker.instantiate_pre(&component).unwrap();
    let mut config = ActorConfig::new(data.path());
    config.mailbox.max_resident = 1;
    config.wall_per_event = Duration::from_millis(200);
    let service = ActorService::new(runtime, config, vec![]).unwrap();
    service.inner.classes.write().unwrap().insert(
        "test".into(),
        Arc::new(ActorClass {
            pre,
            version: "write".into(),
            seq: 0,
            rules: Arc::default(),
            env: Arc::default(),
            requires: Arc::default(),
            warned: Arc::default(),
            writes: crate::apps::Writes::Strict,
            apart: None,
        }),
    );
    let address = ActorAddress::new("default", "test", "write").unwrap();
    let config = crate::cluster::ClusterConfig::new(
        "activation-write",
        "http://activation-write",
        Arc::new(object_store::memory::InMemory::new()),
    );
    let cluster = Cluster::new(&config, &crate::StateRoot::new("", "default", "default")).unwrap();
    cluster.identity().await.unwrap();
    let hold = Held::new(
        LeaseHold::acquire(&cluster, &address).await.unwrap(),
        crate::apps::Writes::Strict,
    );
    let job = service.inner.mailboxes.submit(&address, move |state| {
        Box::pin(async move {
            state.held = Some(hold);
            let error = state.ensure_ready().await.unwrap_err();
            assert!(
                error.contains("activation") && error.contains("wall-clock"),
                "{error}"
            );
            assert!(state.held.is_none());
            state.storage.clone()
        })
    });
    within(ready).await.unwrap();
    tokio::time::sleep(Duration::from_millis(250)).await;
    assert!(
        cluster.holds_here(&address),
        "ownership released while the INSERT worker can still commit"
    );
    release.send(()).unwrap();
    let storage = within(job).await.unwrap();
    assert!(cluster.releases_done(Duration::from_secs(2)).await);
    assert!(!cluster.holds_here(&address));
    within(async {
        while service.inner.mailboxes.contains(&address) {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
    assert_eq!(service.inner.mailboxes.resident(), 0);
    assert!(!storage.is_open(&storage.db_path("test", "write")));
    within(service.inner.mailboxes.submit(&address, |state| {
        Box::pin(async move {
            let mut db = state.storage.open("test", "write").unwrap();
            assert_eq!(
                db.query("SELECT v FROM writes", &[]).unwrap().values.len(),
                1
            );
        })
    }))
    .await
    .unwrap();
}

/// A call queued behind a held job exposes its queue delay, without actor names.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn timing_mailbox_wait_behind_slow_event() {
    let data = tempfile::tempdir().unwrap();
    let service = call_service(data.path(), 16, &[]);
    let callee = ActorAddress::new("default", "test", "callee").unwrap();
    let (started, running) = oneshot::channel();
    let (release, released) = oneshot::channel::<()>();
    let blocker = service.inner.mailboxes.submit(&callee, move |_| {
        Box::pin(async move {
            started.send(()).unwrap();
            released.await.unwrap();
        })
    });
    within(running).await.unwrap();
    let call = call_from(&service, "default", "callee");
    tokio::pin!(call);
    assert!(
        tokio::time::timeout(Duration::from_millis(150), &mut call)
            .await
            .is_err()
    );
    release.send(()).unwrap();
    assert_eq!(within(call).await.unwrap().status, 200);
    within(blocker).await.unwrap();
    let page = crate::metrics::render(crate::metrics::Gauges::default());
    let series =
        "wasmkeep_mailbox_wait_seconds_sum{tenant=\"default\",app=\"default\",class=\"test\"} ";
    let sum: f64 = page
        .lines()
        .find_map(|line| line.strip_prefix(series))
        .unwrap_or_else(|| panic!("missing mailbox timing: {page}"))
        .parse()
        .unwrap();
    assert!(sum >= 0.1, "queue delay: {sum}");
    assert!(!page.contains("callee"), "actor names must not be labels");
    assert!(
        !page.contains("wasmkeep_event_start_rule_wait_seconds_count{"),
        "strict actors must not record start-rule waits"
    );
}

/// The clock starts again on the accepted attempt, not at capacity admission.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn timing_mailbox_wait_excludes_capacity_admission() {
    let mailboxes = Mailboxes::new(
        MailboxConfig {
            max_resident: 1,
            ..MailboxConfig::default()
        },
        |_| async { Ok::<_, BoxError>(()) },
    );
    let (release, released) = oneshot::channel::<()>();
    let (entered, running) = oneshot::channel();
    let blocker = mailboxes.submit(&address("busy"), move |_| {
        Box::pin(async move {
            entered.send(()).unwrap();
            released.await.unwrap();
        })
    });
    within(running).await.unwrap();
    let started = std::time::Instant::now();
    let next =
        mailboxes.submit_wait_timed(&address("next"), Duration::from_secs(5), |_, waited| {
            Box::pin(async move { waited })
        });
    tokio::pin!(next);
    assert!(
        tokio::time::timeout(Duration::from_millis(150), &mut next)
            .await
            .is_err()
    );
    release.send(()).unwrap();
    let waited = within(next).await.unwrap();
    assert!(
        started.elapsed().saturating_sub(waited) >= Duration::from_millis(100),
        "capacity wait leaked into mailbox time: {waited:?}"
    );
    within(blocker).await.unwrap();
    mailboxes.close();
    assert!(mailboxes.drained(Duration::from_secs(5)).await);
}

fn host_work_sum(work: &str) -> f64 {
    let page = crate::metrics::render(Default::default());
    let prefix = format!(
        "wasmkeep_mailbox_host_work_seconds_sum{{tenant=\"default\",app=\"default\",class=\"test\",work=\"{work}\"}} "
    );
    page.lines()
        .find_map(|l| l.strip_prefix(&prefix))
        .unwrap_or("0")
        .parse()
        .unwrap()
}

#[tokio::test]
async fn timing_host_work_without_guest() {
    let data = tempfile::tempdir().unwrap();
    let service = call_service(data.path(), 16, &[]);
    let a = ActorAddress::new("default", "test", "host-only").unwrap();
    service
        .inner
        .mailboxes
        .submit(&a, |_| {
            Box::pin(async {
                tokio::time::sleep(Duration::from_millis(30)).await;
            })
        })
        .await
        .unwrap();
    // A following job starts only after the observer finished the previous one.
    service
        .inner
        .mailboxes
        .submit(&a, |_| Box::pin(async {}))
        .await
        .unwrap();
    assert!(host_work_sum("other") >= 0.025);
}

#[tokio::test]
async fn timing_host_work_fetch_sums_to_job() {
    let data = tempfile::tempdir().unwrap();
    let service = call_service(data.path(), 16, &[]);
    let a = ActorAddress::new("default", "test", "fetch-timing").unwrap();
    let (tx, rx) = oneshot::channel();
    let measured = service.inner.mailboxes.submit(&a, |state| {
        Box::pin(async move {
            let started = std::time::Instant::now();
            let req = hyper::Request::builder()
                .uri("/fired")
                .header(hyper::header::HOST, "actor.wasmkeep.internal")
                .body(http_body_util::Full::new(bytes::Bytes::new()))
                .unwrap();
            state
                .owned_request(req, tx, Arc::from(Vec::new()), false, false)
                .await;
            started.elapsed().as_secs_f64()
        })
    });
    let super::Outcome::Served(response) = rx.await.unwrap() else {
        panic!("fetch was not served");
    };
    let response =
        super::routing::collect_response(response, &super::routing::BodyBudget::one(), false)
            .await
            .unwrap();
    assert_eq!(response.status, 200);
    assert_eq!(response.body, b"0\n");
    let elapsed = measured.await.unwrap();
    service
        .inner
        .mailboxes
        .submit(&a, |_| Box::pin(async {}))
        .await
        .unwrap();
    let host: f64 = [
        "stale_alarm_hint",
        "complete_uploads",
        "snapshot_copy",
        "flush_to_barrier",
        "hibernate",
        "restore",
        "other",
    ]
    .iter()
    .map(|w| host_work_sum(w))
    .sum();
    assert!(host > 0.0, "host time must not disappear");
    let page = crate::metrics::render(Default::default());
    let guest: f64 = page
        .lines()
        .filter(|l| l.starts_with("wasmkeep_event_duration_seconds_sum{"))
        .map(|l| l.rsplit_once(' ').unwrap().1.parse::<f64>().unwrap())
        .sum();
    assert!(
        (guest + host - elapsed).abs() < 0.005,
        "guest={guest} host={host} job={elapsed}"
    );
}
