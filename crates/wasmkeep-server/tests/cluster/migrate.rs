//! `wasmkeep cluster migrate` (ADR 0022) against real nodes: a bucket in
//! the layout before 0.4.0 (made by this version's nodes, then moved back
//! to the bare keys, which have the same tails), refused at start,
//! migrated, then served by a node of this version.

use super::*;
use std::collections::BTreeMap;
use wasmkeep_cluster::lease::Clock;
use wasmkeep_cluster::migrate::{
    self, AppInfo, LOCK_KEY, MigrateError, Migrator, Options, Pause, StartRefusal, Store as _,
};
use wasmkeep_cluster::object_store::ObjectStoreBucket;

struct Now;

impl Clock for Now {
    fn now_ms(&self) -> u64 {
        u64::try_from(epoch_ms()).unwrap()
    }
}

impl Pause for Now {
    async fn pause_ms(&self, ms: u64) {
        tokio::time::sleep(Duration::from_millis(ms)).await;
    }
}

/// The windows of these nodes' short leases (ttl 1 s, grace 200 ms).
fn options(heirs: &[(&str, &str)]) -> Options {
    Options {
        heirs: heirs
            .iter()
            .map(|(t, a)| ((*t).to_owned(), (*a).to_owned()))
            .collect(),
        concurrency: 8,
        lease_window_ms: 1_500,
        old_window_ms: 5_000,
        prefix_bytes: 0,
    }
}

fn keys(store: &Arc<dyn ObjectStore>) -> ObjectStoreBucket {
    ObjectStoreBucket::new(Arc::clone(store), "")
}

/// Moves every key under `state/` back to the layout before 0.4.0
/// (`<family>/<tenant>/<tail>`), as a bucket written by 0.2 or 0.3 holds
/// it, and forgets the commits.
async fn to_bare(store: &Arc<dyn ObjectStore>) {
    let b = keys(store);
    for meta in b.list("state/").await.unwrap() {
        let rest = meta.key.strip_prefix("state/").unwrap();
        let mut parts = rest.splitn(4, '/');
        let (tenant, _app, folder, tail) = (
            parts.next().unwrap(),
            parts.next().unwrap(),
            parts.next().unwrap(),
            parts.next().unwrap(),
        );
        let family = Family::ALL
            .into_iter()
            .find(|f| f.folder() == folder)
            .unwrap();
        b.copy(&meta.key, &format!("{}/{tenant}/{tail}", family.bare()))
            .await
            .unwrap();
        migrate::Store::delete(&b, &meta.key).await.unwrap();
    }
    for meta in b.list("cluster/migrated/").await.unwrap() {
        migrate::Store::delete(&b, &meta.key).await.unwrap();
    }
}

/// A node's local databases moved back to `<data>/actors/<tenant>`, as a
/// node before 0.4.0 kept them.
fn local_to_bare(data: &Path) {
    let new = data.join("state/default/default/actors");
    if new.exists() {
        std::fs::create_dir_all(data.join("actors")).unwrap();
        std::fs::rename(new, data.join("actors/default")).unwrap();
        std::fs::remove_dir_all(data.join("state")).unwrap();
    }
}

/// Waits until every node lease on the bucket is past the migration's
/// window (the nodes were dropped, not released).
async fn nodes_quiet() {
    tokio::time::sleep(Duration::from_millis(1_800)).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_bucket_of_the_old_layout_is_refused_then_migrated_and_served() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let data = tempfile::tempdir().unwrap();
    let long = "x".repeat(250);
    let names = ["x", "Room 1 é", long.as_str()];
    {
        let a = node_with(
            "a",
            data.path(),
            &store,
            Duration::from_secs(60),
            "t",
            test_actor_wasm(),
        )
        .await;
        settle().await;
        for name in names {
            let path = format!("/actor/t/{}/durable", url_encode(name));
            for want in ["1", "2"] {
                let r = send_runnable(a.addr, "POST", &path, "").await;
                assert_eq!((r.status, r.body.trim()), (200, want), "{name}");
            }
        }
        let r = send(a.addr, "POST", "/actor/t/y/alarm?in=5000", "").await;
        assert_eq!(r.status, 200, "{}", r.body);
        // Every write uploaded before the node goes.
        tokio::time::sleep(Duration::from_millis(500)).await;
        a.service.shutdown(Duration::from_secs(5)).await;
        stop_listening(a).await;
    }
    let alarm_set = epoch_ms();
    // A move request, made while the old layout was live.
    request_move(&store, "x", "node-z").await;
    to_bare(&store).await;
    local_to_bare(data.path());
    let b = keys(&store);
    assert!(b.list("state/").await.unwrap().is_empty());
    assert!(!b.list("owners/default/t/").await.unwrap().is_empty());

    // A node of this version refuses, naming the command.
    match migrate::start_check(&b, "default").await {
        Err(StartRefusal::Unmigrated(tenant, found)) => {
            assert_eq!(tenant, "default");
            assert!(found.starts_with("actors/default/"), "{found}");
        }
        other => panic!("{other:?}"),
    }

    // The dry run: the implicit app inherits everything; nothing written.
    nodes_quiet().await;
    let before = bucket_keys(&store).await;
    let migrator = Migrator::new(&b, &Now, Vec::new(), options(&[]));
    let plan = migrator.plan().await.unwrap();
    assert!(plan.refusals.is_empty(), "{:?}", plan.refusals);
    assert_eq!(
        plan.tenants["default"].heirs,
        BTreeMap::from([("t".to_owned(), "default".to_owned())])
    );
    assert_eq!(bucket_keys(&store).await, before, "the dry run wrote");

    let outcome = migrator.apply().await.unwrap();
    assert_eq!(outcome.committed, ["default"]);
    migrate::start_check(&b, "default").await.unwrap();
    // The move is not copied (a copy would renew its lifetime): it stays
    // in the old layout, which no node of this version reads.
    let all = bucket_keys(&store).await;
    assert!(
        !all.iter()
            .any(|k| k.starts_with(&test_key(Family::Moves, "t/"))),
        "the move was copied"
    );
    assert!(all.iter().any(|k| k.starts_with("control/move/default/t/")));
    // Every bare record is a tombstone naming its new key.
    for meta in b.list("owners/default/t/").await.unwrap() {
        let (body, _) = wasmkeep_cluster::lease::Bucket::get(&b, &meta.key)
            .await
            .unwrap()
            .unwrap();
        let to = migrate::tombstone_target(&body).unwrap();
        assert_eq!(
            to,
            meta.key
                .replacen("owners/default/", "state/default/default/owners/", 1)
        );
    }

    // A node of this version on the same data directory: the old local
    // databases go aside (the bucket is the truth), and every actor
    // restores from its new root.
    // As `serve` sets it from its start check: the tenant was migrated.
    let c = node_configured(
        "c",
        data.path(),
        &store,
        Duration::from_secs(60),
        &["t"],
        test_actor_wasm(),
        Duration::from_secs(30),
        "",
        |config| config.cluster.as_mut().unwrap().migrated = true,
    )
    .await;
    settle().await;
    assert!(data.path().join("actors.pre-0.4/default/t").is_dir());
    assert!(data.path().join("actors/default").is_file());
    for name in names {
        let path = format!("/actor/t/{}/durable", url_encode(name));
        let r = send_runnable(c.addr, "GET", &path, "").await;
        assert_eq!((r.status, r.body.trim()), (200, "2"), "{name}");
        let r = send_runnable(c.addr, "POST", &path, "").await;
        assert_eq!((r.status, r.body.trim()), (200, "3"), "{name}");
    }
    // The alarm set before the migration fires, from the copied index.
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    loop {
        let r = send_runnable(c.addr, "GET", "/actor/t/y/fired", "").await;
        if r.body.trim() == "1" {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the alarm never fired"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        epoch_ms() >= alarm_set + 3_000,
        "the alarm fired before it was due"
    );

    // Run again while that node is live: nothing to do, not a refusal.
    let again = migrator.apply().await.unwrap();
    assert_eq!(
        (
            again.committed.len(),
            again.already.as_slice(),
            again.exit_code()
        ),
        (0, ["default".to_owned()].as_slice(), 0)
    );
    // A node ran: the rollback is refused, and changes nothing.
    let keys_now = bucket_keys(&store).await;
    match migrator.rollback().await {
        Err(MigrateError::Refused(why)) => assert!(why.concat().contains("one-way"), "{why:?}"),
        other => panic!("{other:?}"),
    }
    let after: Vec<String> = bucket_keys(&store)
        .await
        .into_iter()
        .filter(|k| !k.starts_with("nodes/") && !k.starts_with(&test_key(Family::Alarms, "")))
        .collect();
    let kept: Vec<String> = keys_now
        .into_iter()
        .filter(|k| !k.starts_with("nodes/") && !k.starts_with(&test_key(Family::Alarms, "")))
        .collect();
    assert_eq!(after, kept);
    drop(c);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_rollback_before_any_start_puts_the_old_layout_back() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let data = tempfile::tempdir().unwrap();
    {
        let a = node("a", data.path(), &store, Duration::from_secs(60)).await;
        settle().await;
        let r = send_runnable(a.addr, "POST", "/actor/counter/x", "").await;
        assert_eq!(r.status, 200, "{}", r.body);
        tokio::time::sleep(Duration::from_millis(500)).await;
        a.service.shutdown(Duration::from_secs(5)).await;
        stop_listening(a).await;
    }
    to_bare(&store).await;
    nodes_quiet().await;
    let b = keys(&store);
    let read_all = async || {
        let mut out = BTreeMap::new();
        for k in bucket_keys(&store).await {
            if k.starts_with("nodes/") {
                continue;
            }
            let body = wasmkeep_cluster::lease::Bucket::get(&b, &k)
                .await
                .unwrap()
                .unwrap()
                .0;
            out.insert(k, body);
        }
        out
    };
    let before = read_all().await;
    let migrator = Migrator::new(&b, &Now, Vec::new(), options(&[]));
    migrator.apply().await.unwrap();
    assert_ne!(read_all().await, before);
    assert_eq!(migrator.rollback().await.unwrap(), ["default"]);
    assert_eq!(read_all().await, before, "not the old layout byte for byte");
    // It migrates again.
    migrator.apply().await.unwrap();
    migrate::start_check(&b, "default").await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn each_class_goes_to_the_app_that_deploys_it_and_a_shared_one_needs_an_heir() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let rt = deploy_runtime();
    let wasm = || counter_wasm().to_vec();
    AppRef::new(Arc::clone(&store), "", "acme", "shop")
        .deploy(&rt, vec![("chat".into(), wasm()), ("both".into(), wasm())])
        .await
        .unwrap();
    AppRef::new(Arc::clone(&store), "", "acme", "blog")
        .deploy(&rt, vec![("post".into(), wasm()), ("both".into(), wasm())])
        .await
        .unwrap();
    // As 0.3 left them: unrooted currents, bare keys.
    let b = keys(&store);
    for app in ["shop", "blog"] {
        let key = format!("apps/acme/{app}/current");
        let (body, _) = wasmkeep_cluster::lease::Bucket::get(&b, &key)
            .await
            .unwrap()
            .unwrap();
        let text = String::from_utf8(body).unwrap();
        let old = text.replace(" 3\n", "\n");
        migrate::Store::put(&b, &key, old.into_bytes())
            .await
            .unwrap();
    }
    let record = wasmkeep_cluster::node_lease::OwnerRecord {
        holder: None,
        generation: 4,
    }
    .encode();
    for class in ["chat", "post", "both"] {
        migrate::Store::put(&b, &format!("owners/acme/{class}/n"), record.clone())
            .await
            .unwrap();
    }
    let apps = vec![
        AppInfo {
            tenant: "acme".into(),
            app: "shop".into(),
            classes: ["chat", "both"].map(str::to_owned).into(),
        },
        AppInfo {
            tenant: "acme".into(),
            app: "blog".into(),
            classes: ["post", "both"].map(str::to_owned).into(),
        },
    ];
    // A class both apps deploy: refused without --heir, nothing written.
    let before = bucket_keys(&store).await;
    match Migrator::new(&b, &Now, apps.clone(), options(&[]))
        .apply()
        .await
    {
        Err(MigrateError::Refused(why)) => {
            assert!(why.concat().contains("--heir acme/APP"), "{why:?}");
        }
        other => panic!("{other:?}"),
    }
    assert_eq!(bucket_keys(&store).await, before);
    // An heir that is not an app of the tenant (a typo) is refused too.
    match Migrator::new(&b, &Now, apps.clone(), options(&[("acme", "blgo")]))
        .plan()
        .await
    {
        Ok(plan) => assert!(
            plan.refusals
                .concat()
                .contains("tenant acme has no app blgo"),
            "{:?}",
            plan.refusals
        ),
        other => panic!("{other:?}"),
    }
    // A deploy refuses the tenant meanwhile.
    let e = AppRef::new(Arc::clone(&store), "", "acme", "shop")
        .deploy(&rt, vec![("chat".into(), wasm())])
        .await
        .unwrap_err()
        .to_string();
    assert!(e.contains("cluster migrate"), "{e}");

    Migrator::new(&b, &Now, apps, options(&[("acme", "blog")]))
        .apply()
        .await
        .unwrap();
    let all = bucket_keys(&store).await;
    for want in [
        "state/acme/shop/owners/chat/n",
        "state/acme/blog/owners/post/n",
        "state/acme/blog/owners/both/n",
    ] {
        assert!(all.contains(&want.to_owned()), "{want} missing: {all:?}");
    }
    assert!(!all.contains(&"state/acme/shop/owners/both/n".to_owned()));
    // Every current rooted: older binaries cannot read them.
    for app in ["shop", "blog"] {
        let current = AppRef::new(Arc::clone(&store), "", "acme", app)
            .current()
            .await
            .unwrap()
            .unwrap();
        assert!(current.rooted, "{app}");
    }
    // Deploys work again.
    AppRef::new(Arc::clone(&store), "", "acme", "shop")
        .deploy(&rt, vec![("chat".into(), wasm())])
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn deploys_and_starts_are_refused_while_a_migration_holds_the_prefix() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let b = keys(&store);
    migrate::Store::put(&b, LOCK_KEY, b"wasmkeep-migration 1\nmode apply\n".to_vec())
        .await
        .unwrap();
    assert_eq!(
        migrate::start_check(&b, "default").await,
        Err(StartRefusal::InProgress)
    );
    let e = AppRef::new(Arc::clone(&store), "", "acme", "shop")
        .deploy(
            &deploy_runtime(),
            vec![("c".into(), counter_wasm().to_vec())],
        )
        .await
        .unwrap_err()
        .to_string();
    assert!(e.contains("in progress"), "{e}");
    assert!(
        !bucket_keys(&store)
            .await
            .iter()
            .any(|k| k.starts_with("apps/")),
        "the refused deploy wrote"
    );
}

/// A node joining a bucket with no state of its tenant, with databases of
/// the layout before 0.4.0 from a single-node run: they are the only copy,
/// so they move into its app's root (never aside), and it serves them.
#[tokio::test(flavor = "multi_thread")]
async fn a_node_joining_with_single_node_data_keeps_it() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let data = tempfile::tempdir().unwrap();
    {
        // Written alone, then laid out as a node before 0.4.0 kept it.
        let alone = tempfile::tempdir().unwrap();
        let runtime = Runtime::new(&RuntimeConfig::default()).unwrap();
        let component = runtime.compile(test_actor_wasm()).unwrap();
        let service = ActorService::new(
            runtime,
            ActorConfig::new(alone.path()),
            vec![("t".into(), component, test_actor_wasm().to_vec())],
        )
        .unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (stop, stopped) = oneshot::channel::<()>();
        spawn_serve(listener, None, service.clone(), async {
            let _ = stopped.await;
        });
        for want in ["1", "2"] {
            let r = send(addr, "POST", "/actor/t/kept/durable", "").await;
            assert_eq!((r.status, r.body.trim()), (200, want));
        }
        service.shutdown(Duration::from_secs(5)).await;
        drop((stop, service));
        local_to_bare(alone.path());
        std::fs::rename(alone.path().join("actors"), data.path().join("actors")).unwrap();
    }
    let b = keys(&store);
    assert!(!migrate::start_check(&b, "default").await.unwrap());
    let a = node_with(
        "a",
        data.path(),
        &store,
        Duration::from_secs(60),
        "t",
        test_actor_wasm(),
    )
    .await;
    settle().await;
    assert!(!data.path().join("actors.pre-0.4").exists());
    assert!(data.path().join(test_dir("t")).is_dir());
    let r = send_runnable(a.addr, "GET", "/actor/t/kept/durable", "").await;
    assert_eq!((r.status, r.body.trim()), (200, "2"), "{}", r.body);
}
