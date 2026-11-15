//! A real `ActorState` of a batched hold (ADR 0034): dropped without a
//! step before the drop, as the mailbox drops a state after a panic, and
//! its uploader's deadlines.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code

use std::sync::Arc;
use std::time::Duration;

use object_store::ObjectStore;
use wasmkeep_cluster::lease::LeaseConfig;
use wasmkeep_cluster::state_root::StateRoot;
use wasmkeep_runtime::{BoxError, MailboxConfig, Mailboxes, Runtime, RuntimeConfig};
use wasmkeep_storage::{Storage, StorageConfig};

use super::*;
use crate::cluster::ClusterConfig;

async fn node(store: &Arc<dyn ObjectStore>) -> Arc<Cluster> {
    let mut config = ClusterConfig::new("a", "http://a-drop", Arc::clone(store));
    config.heartbeat = Duration::from_millis(100);
    config.lease = LeaseConfig::new(1_000, 200, 200, 100).unwrap();
    let node = Cluster::new(&config, &StateRoot::new("", "t", "default")).unwrap();
    node.identity().await.unwrap();
    let started = std::time::Instant::now();
    while !node.node_lease_valid() {
        assert!(started.elapsed() < Duration::from_secs(3), "never valid");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    node
}

/// A state for `address`, with nothing loaded: a class whose component
/// exports nothing (no event runs here) and this node's cluster.
fn state(
    address: ActorAddress,
    residency: wasmkeep_runtime::Residency,
    storage: Storage,
    cluster: Arc<Cluster>,
) -> ActorState {
    let runtime = Runtime::new(&RuntimeConfig::default()).unwrap();
    let component = runtime
        .compile(&wat::parse_str("(component)").unwrap())
        .unwrap();
    let pre = Linker::<ActorHost>::new(runtime.engine())
        .instantiate_pre(&component)
        .unwrap();
    let class = Arc::new(ActorClass {
        pre,
        version: "v".into(),
        seq: 0,
        rules: Arc::default(),
        env: Arc::default(),
        requires: Arc::default(),
        warned: Arc::default(),
        writes: crate::apps::Writes::Batched,
    });
    ActorState {
        moves: Arc::default(),
        secrets_blocked: Arc::default(),
        address,
        class,
        runtime,
        storage,
        limits: EventLimits {
            wall: Duration::from_secs(30),
            retry_initial: Duration::from_secs(1),
            retry_max: Duration::from_secs(10),
            max_unsent_age: Duration::from_millis(500),
        },
        alarms: crate::alarm::spawn(|_, _| {}),
        alarm_failures: 0,
        service: Arc::default(),
        sockets: Arc::default(),
        chain: Arc::from(Vec::new()),
        db: None,
        live: None,
        cluster: Some(cluster),
        held: None,
        owner_hint: None,
        behind_deploy: false,
        egress: Arc::default(),
        guest_log: crate::guest_log::GuestLog::default(),
        residency,
        collecting: Arc::default(),
        stepped: false,
    }
}

/// Restores `s` under a batched hold of this node: its first snapshot
/// uploaded, its database capturing in batched mode, nothing loaded.
async fn restored(s: &mut ActorState) {
    let cluster = s.cluster.clone().unwrap();
    let hold = LeaseHold::acquire(&cluster, &s.address).await.unwrap();
    let (view, generation) = (hold.view(), hold.generation().unwrap());
    let path = s.storage.db_path(s.address.class(), s.address.name());
    let mut replica = Replica::restore(&cluster, &s.address, view, generation, path, 1 << 30)
        .await
        .unwrap();
    let mut db = s
        .residency
        .retain(s.storage.open(s.address.class(), s.address.name()).unwrap());
    db.exec("CREATE TABLE t(v)", &[]).unwrap();
    db.start_capture().unwrap();
    let all = db.snapshot().unwrap();
    assert!(
        replica
            .ship_snapshot(&all, crate::replica::SnapshotKind::First)
            .await
            .unwrap()
    );
    let mut held = Held::new(hold, crate::apps::Writes::Batched);
    held.restored(Arc::new(tokio::sync::Mutex::new(replica)));
    s.held = Some(held);
    s.db = Some(db);
    s.ensure_batched_capture().await.unwrap();
}

/// This node's cluster and a mailbox of batched states without a step
/// before the drop.
struct Fixture {
    cluster: Arc<Cluster>,
    mailboxes: Mailboxes<ActorState>,
    _dir: tempfile::TempDir,
}

async fn fixture() -> Fixture {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let cluster = node(&store).await;
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::new(
        StorageConfig::new(dir.path()),
        crate::state_dir(&StateRoot::new("", "t", "default")),
    );
    let (factory_storage, factory_cluster) = (storage, Arc::clone(&cluster));
    let mailboxes = Mailboxes::new_with_residency(
        MailboxConfig::default(),
        move |address: ActorAddress, residency| {
            let (storage, cluster) = (factory_storage.clone(), Arc::clone(&factory_cluster));
            async move { Ok::<_, BoxError>(state(address, residency, storage, cluster)) }
        },
    );
    Fixture {
        cluster,
        mailboxes,
        _dir: dir,
    }
}

/// A capture asked for past its deadline starts nothing, even with room in
/// the node budget: what follows the reservation (the replica's lock, the
/// hand-off) is not waited for past it (ADR 0034, section 4). The boundary
/// stays for the next capture.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_capture_past_its_deadline_starts_nothing() {
    let f = fixture().await;
    let address = ActorAddress::new("t", "c", "late").unwrap();
    let got = f
        .mailboxes
        .submit(&address, |s: &mut ActorState| {
            Box::pin(async move {
                restored(s).await;
                let db = s.db.as_mut().unwrap();
                db.exec("INSERT INTO t VALUES (1)", &[]).unwrap();
                db.mark_boundary().unwrap().unwrap();
                let slot = s.batched_replica().unwrap();
                let past = tokio::time::Instant::now();
                tokio::time::sleep(Duration::from_millis(5)).await;
                let db = s.db.as_mut().unwrap();
                let started = super::uploads::pump(
                    &slot,
                    &mut super::uploads::DbAt::Resident(db),
                    None,
                    Some(past),
                )
                .await
                .unwrap();
                let idle = slot.lock().await.uploader_idle();
                (
                    started,
                    idle,
                    s.db.as_ref().unwrap().has_pending_boundaries(),
                )
            })
        })
        .await
        .unwrap();
    assert_eq!(got, (false, true, true));
    f.mailboxes.hibernate(&address);
    drop(f.cluster);
}

/// A batched hold whose database committed after its last boundary is
/// given up when its state drops without a step, never parked or released
/// as flushed, and that commit counts as dropped.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_state_dropped_without_a_step_counts_its_unmarked_commits() {
    let Fixture {
        cluster,
        mailboxes,
        _dir,
    } = fixture().await;
    let address = ActorAddress::new("t", "c", "x").unwrap();
    let before = crate::metrics::METRICS
        .plain_events_dropped
        .get(&[("reason", "panic")])
        .load(std::sync::atomic::Ordering::Relaxed);
    mailboxes
        .submit(&address, |s: &mut ActorState| {
            Box::pin(async move {
                restored(s).await;
                // Committed, never marked: the event never reached its end.
                s.db.as_mut()
                    .unwrap()
                    .exec("INSERT INTO t VALUES (1)", &[])
                    .unwrap();
            })
        })
        .await
        .unwrap();
    // No step is registered: the state drops as after a panic.
    mailboxes.hibernate(&address);
    let started = std::time::Instant::now();
    while cluster.holds_here(&address) {
        assert!(started.elapsed() < Duration::from_secs(5), "never dropped");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let after = crate::metrics::METRICS
        .plain_events_dropped
        .get(&[("reason", "panic")])
        .load(std::sync::atomic::Ordering::Relaxed);
    assert_eq!(after - before, 1, "the unmarked commit is counted");
    assert!(!cluster.is_parked(&address));
}
