#![allow(clippy::unwrap_used)] // test code

use super::*;
use object_store::memory::InMemory;
use object_store::{ObjectStoreExt as _, PutPayload};
use wasmkeep_cluster::node_lease::{NodeState, OwnerRecord};
use wasmkeep_cluster::recorded_head::{Completeness, RecordedHead};
use wasmkeep_storage::ltx;

const PAGE: usize = 512;

/// The bucket prefix every test's cluster is under.
const PREFIX: &str = "p/q";

fn root() -> StateRoot {
    StateRoot::new(PREFIX, "acme", "shop")
}

async fn put(store: &Store, key: &str, body: impl Into<PutPayload>) {
    store.put(&Key::from(key), body.into()).await.unwrap();
}

fn page(fill: u8) -> Vec<u8> {
    vec![fill; PAGE]
}

fn sum_of(fill: u8) -> ltx::DbChecksum {
    let mut sum = ltx::DbChecksum::default();
    sum.toggle(1, &page(fill), PAGE as u32);
    sum
}

/// A one-page database at `txid`, whole: a snapshot.
fn snapshot(txid: u64, fill: u8) -> Vec<u8> {
    let mut encoder = ltx::Encoder::new(ltx::Header {
        page_size: PAGE as u32,
        commit: 1,
        min_txid: 1,
        max_txid: txid,
        ..ltx::Header::default()
    })
    .unwrap();
    encoder.add_page(1, &page(fill)).unwrap();
    encoder.finish(sum_of(fill)).unwrap()
}

/// The transaction `txid`, which rewrites the page from `before` to `fill`.
fn segment(txid: u64, before: u8, fill: u8) -> Vec<u8> {
    let mut encoder = ltx::Encoder::new(ltx::Header {
        page_size: PAGE as u32,
        commit: 1,
        min_txid: txid,
        max_txid: txid,
        pre_apply_checksum: sum_of(before).value(),
        ..ltx::Header::default()
    })
    .unwrap();
    encoder.add_page(1, &page(fill)).unwrap();
    encoder.finish(sum_of(fill)).unwrap()
}

fn wal(txid: u64) -> String {
    format!(
        "wal/{txid:016x}-{:016x}-{:016x}.ltx",
        1,
        u64::from(txid > 1)
    )
}

fn snap(txid: u64) -> String {
    format!("snapshot/{txid:016x}-{:016x}.ltx", 1)
}

fn released(recorded: Option<RecordedHead>) -> Vec<u8> {
    OwnerRecord {
        holder: None,
        generation: 1,
        recorded,
    }
    .encode()
}

fn complete(txid: u64, fill: u8) -> RecordedHead {
    RecordedHead {
        head: Position::Known {
            generation: 1,
            txid,
            checksum: sum_of(fill).value(),
        },
        set_at: 1,
        completeness: Completeness::Complete,
        idle_since_ms: None,
    }
}

/// An app that was never deployed, with one actor `c/a`: a snapshot at
/// transaction 1, segments 1 to 3 (the page filled with the transaction's
/// number), a released version 1 record, one alarm entry and a lease
/// marker; one node lease; one file, its count and a sweep record.
async fn whole_bucket() -> Store {
    let store: Store = Arc::new(InMemory::new());
    let root = root();
    let actor = root.bucket_key(Family::History, "c/a");
    put(&store, &format!("{actor}/{}", snap(1)), snapshot(1, 1)).await;
    put(&store, &format!("{actor}/{}", wal(1)), snapshot(1, 1)).await;
    put(&store, &format!("{actor}/{}", wal(2)), segment(2, 1, 2)).await;
    put(&store, &format!("{actor}/{}", wal(3)), segment(3, 2, 3)).await;
    put(
        &store,
        &root.bucket_key(Family::Owners, "c/a"),
        released(None),
    )
    .await;
    put(
        &store,
        &root.bucket_key(Family::Leases, "c/a"),
        wasmkeep_cluster::node_lease::encode_marker(0),
    )
    .await;
    let entry = alarm_index::Entry::owned(1_000, 1, "ab").unwrap();
    put(
        &store,
        &root.bucket_key(Family::Alarms, &alarm_index::key_tail("c", "a", &entry)),
        Vec::new(),
    )
    .await;
    let lease = NodeLeaseObject {
        state: NodeState::Released,
        instance: Some("i1".into()),
        written_at_ms: 5,
    };
    put(&store, "p/q/nodes/n1", lease.encode()).await;
    let state = format!("p/q/{}", root.bucket());
    let epoch = wasmkeep_server::blobs::epoch(None, None);
    let id = sha256(b"a");
    put(
        &store,
        &format!("{state}blobs/{epoch}/c/{id}/0/photos/1.txt"),
        b"hello".to_vec(),
    )
    .await;
    put(
        &store,
        &format!("{state}blob-usage"),
        format!("wasmkeep-blob-usage 1\n{epoch}\n5\n1\n7\n").into_bytes(),
    )
    .await;
    let record = blob_sweep::Record {
        state: blob_sweep::State::Swept,
        epoch,
        name: "gone".into(),
    };
    put(
        &store,
        &format!("{state}blob-sweeps/c/{}/0", sha256(b"gone")),
        record.encode().into_bytes(),
    )
    .await;
    store
}

async fn check(store: &Store, deep: bool) -> Report {
    let options = Options {
        deep,
        ..Options::default()
    };
    let report = verify(Arc::clone(store), PREFIX, &options).await;
    assert_eq!(report.writes_refused, 0, "verify tried to write");
    report
}

/// The only problem of `report`: its key and its check.
fn only(report: &Report) -> (&str, &str) {
    assert_eq!(report.problems.len(), 1, "{}", report.to_text());
    assert_eq!(report.exit_code(), 1);
    (report.problems[0].key.as_str(), report.problems[0].check)
}

#[tokio::test]
async fn a_whole_bucket_under_a_prefix_passes_and_is_counted() {
    let store = whole_bucket().await;
    let shallow = check(&store, false).await;
    assert!(shallow.whole(), "{}", shallow.to_text());
    let a = &shallow.apps[0];
    assert_eq!((a.name.as_str(), a.state), ("acme/shop", "not deployed"));
    assert_eq!(
        (a.actors, a.restorable, a.records, a.records_read),
        (1, 1, 1, 1)
    );
    assert_eq!((a.history_objects, a.alarms, a.lease_markers), (4, 1, 1));
    assert_eq!((a.files, a.file_bytes, a.sweep_records), (1, 5, 1));
    assert_eq!(a.usage.as_ref().map(|u| (u.1, u.2)), Some((5, 1)));
    assert_eq!((shallow.nodes, shallow.nodes_old_format), (1, 0));
    // The record, the node lease, the count and the sweep record.
    assert_eq!(shallow.objects_read, 4, "{}", shallow.to_text());
    let deep = check(&store, true).await;
    assert!(deep.whole(), "{}", deep.to_text());
    // And the chain: the snapshot and the two segments after it.
    assert_eq!(deep.objects_read, 7, "{}", deep.to_text());
    assert_eq!(deep.apps[0].deep_checked, 1);
    assert!(deep.requests > deep.objects_read && deep.keys_listed >= 10);
    assert!(deep.to_text().ends_with("The bucket is whole.\n"));
    assert_eq!(deep.to_json()["apps"][0]["restorable"], 1);
    assert_eq!(deep.to_json()["writes_refused"], 0);
    assert_eq!(deep.to_json()["exit_code"], 0);
    // Another prefix holds nothing of it: no verdict, not a whole bucket.
    let other = verify(Arc::clone(&store), "p", &Options::default()).await;
    assert!(
        !other.whole() && other.exit_code() == 2,
        "an empty prefix passed"
    );
    assert!(
        other.stopped[0].contains("nothing of a cluster"),
        "{other:?}"
    );
    assert!(other.to_text().contains("stopped: nothing of a cluster"));
}

#[tokio::test]
async fn a_segment_that_does_not_follow_is_found_by_reading() {
    let store = whole_bucket().await;
    let key = format!("{}/{}", root().bucket_key(Family::History, "c/a"), wal(3));
    // Built on a database transaction 2 did not leave.
    put(&store, &key, segment(3, 9, 3)).await;
    assert!(check(&store, false).await.whole());
    assert_eq!(only(&check(&store, true).await), (key.as_str(), "history"));
    // A valid file of another transaction under that key.
    put(&store, &key, segment(4, 2, 3)).await;
    assert_eq!(only(&check(&store, true).await), (key.as_str(), "history"));
}

#[tokio::test]
async fn a_gap_and_two_segments_at_one_position_are_named() {
    let store = whole_bucket().await;
    let actor = root().bucket_key(Family::History, "c/a");
    put(
        &store,
        &format!("{actor}/wal/{:016x}-{:016x}-{:016x}.ltx", 2, 1, 7),
        segment(2, 1, 2),
    )
    .await;
    let report = check(&store, false).await;
    assert_eq!(
        only(&report),
        (
            format!("{actor}/wal/{:016x}-{:016x}-*.ltx", 2, 1).as_str(),
            "history"
        )
    );
    assert_eq!(report.apps[0].restorable, 0);

    let store = whole_bucket().await;
    store
        .delete(&Key::from(format!("{actor}/{}", wal(2))))
        .await
        .unwrap();
    let report = check(&store, false).await;
    assert_eq!(
        only(&report),
        (
            format!("{actor}/wal/{:016x}-{:016x}-*.ltx", 2, 1).as_str(),
            "history"
        )
    );
    assert!(
        report.problems[0]
            .detail
            .contains(&format!("{actor}/{} is built on it", wal(3))),
        "{:?}",
        report.problems[0]
    );
}

#[tokio::test]
async fn each_record_that_does_not_read_is_named() {
    let root = root();
    let state = format!("p/q/{}", root.bucket());
    let garbage = b"not what this key holds\n".to_vec();
    let stray = alarm_index::Entry::owned(2_000, 1, "cd").unwrap();
    let faults: Vec<(String, Vec<u8>, &str)> = vec![
        (
            root.bucket_key(Family::Owners, "c/a"),
            garbage.clone(),
            "ownership",
        ),
        // A record of an actor with no history.
        (
            root.bucket_key(Family::Owners, "c/never-wrote"),
            garbage.clone(),
            "ownership",
        ),
        // Version 2, in an app that writes version 1.
        (
            root.bucket_key(Family::Owners, "c/a"),
            released(Some(complete(3, 3))),
            "ownership",
        ),
        (app_format::key_of(&root), garbage.clone(), "format"),
        // A version 2 record is then not a second problem.
        (
            app_format::key_of(&root),
            b"wasmkeep-recorded-head 2\n".to_vec(),
            "format",
        ),
        (
            app_format::floor::key_of(&root),
            b"wasmkeep-floor 1\n0.09\n".to_vec(),
            "format",
        ),
        (format!("{state}blob-usage"), garbage.clone(), "files"),
        (
            format!("{state}blob-sweeps/c/{}/0", sha256(b"gone")),
            garbage.clone(),
            "files",
        ),
        // A sweep record under another actor's key.
        (
            format!("{state}blob-sweeps/c/{}/0", sha256(b"other")),
            blob_sweep::Record {
                state: blob_sweep::State::Pending,
                epoch: wasmkeep_server::blobs::epoch(None, None),
                name: "gone".into(),
            }
            .encode()
            .into_bytes(),
            "files",
        ),
        ("p/q/nodes/n2".to_owned(), garbage.clone(), "node"),
        (
            root.bucket_key(Family::Alarms, &alarm_index::key_tail("c", "ghost", &stray)),
            Vec::new(),
            "alarm",
        ),
        (
            "p/q/apps/acme/shop/current".to_owned(),
            garbage.clone(),
            "deploy",
        ),
        // Deployed in the layout from before 0.4.0.
        (
            "p/q/apps/acme/shop/current".to_owned(),
            b"00ff 3 2".to_vec(),
            "deploy",
        ),
        // Its version is not there.
        (
            "p/q/apps/acme/shop/current".to_owned(),
            b"00ff 3 3".to_vec(),
            "deploy",
        ),
    ];
    for (key, body, check_name) in faults {
        let store = whole_bucket().await;
        if body == b"wasmkeep-recorded-head 2\n" {
            let owner = root.bucket_key(Family::Owners, "c/a");
            put(&store, &owner, released(Some(complete(3, 3)))).await;
        }
        put(&store, &key, body).await;
        let report = check(&store, false).await;
        assert_eq!(report.problems.len(), 1, "{key}: {}", report.to_text());
        assert!(report.unread.is_empty(), "{key}: {}", report.to_text());
        let found = &report.problems[0];
        if check_name == "deploy" && key.ends_with("current") && found.key != key {
            assert_eq!(found.key, "p/q/apps/acme/shop/00ff/manifest");
        } else {
            assert_eq!(found.key, key, "{}", report.to_text());
        }
        assert_eq!(found.check, check_name, "{key}: {}", report.to_text());
        assert_eq!(report.exit_code(), 1);
    }
}

#[tokio::test]
async fn what_lags_or_is_left_alone_is_not_a_problem() {
    let store = whole_bucket().await;
    let root = root();
    let state = format!("p/q/{}", root.bucket());
    // A count that lags, a file under another epoch, keys nothing of
    // WasmKeep wrote, a heartbeat from before node leases, an alarm of an
    // actor that has only its record yet, a tombstone at an ownership key.
    put(
        &store,
        &format!("{state}blob-usage"),
        b"wasmkeep-blob-usage 1\n0123456789abcdef\n900\n40\n7\n".to_vec(),
    )
    .await;
    put(
        &store,
        &format!("{state}blobs/0123456789abcdef/_front/main/0/x"),
        vec![1],
    )
    .await;
    put(&store, &format!("{state}blobs/readme"), vec![1]).await;
    put(&store, &format!("{state}blob-sweeps/readme"), vec![1]).await;
    put(&store, &format!("{state}alarms/readme"), vec![1]).await;
    put(
        &store,
        "p/q/nodes/old",
        b"wasmkeep-node 2\nold\nhttp://10.0.0.1:1\n0.3.0\n5\nacme/shop\n".to_vec(),
    )
    .await;
    put(
        &store,
        &root.bucket_key(Family::Owners, "c/new"),
        released(None),
    )
    .await;
    let entry = alarm_index::Entry::owned(3_000, 1, "ef").unwrap();
    put(
        &store,
        &root.bucket_key(Family::Alarms, &alarm_index::key_tail("c", "new", &entry)),
        Vec::new(),
    )
    .await;
    put(
        &store,
        &root.bucket_key(Family::Owners, "c/moved"),
        b"wasmkeep-owner moved\nstate/acme/shop/\n".to_vec(),
    )
    .await;
    let report = check(&store, true).await;
    assert!(report.whole(), "{}", report.to_text());
    let a = &report.apps[0];
    assert_eq!((a.files, a.files_elsewhere), (1, 2), "{a:?}");
    assert_eq!((a.alarms, a.alarms_skipped, a.records), (2, 1, 3), "{a:?}");
    assert_eq!(a.usage.as_ref().map(|u| u.2), Some(40));
    assert_eq!((report.nodes, report.nodes_old_format), (1, 1));
}

#[tokio::test]
async fn a_deleted_app_is_not_checked() {
    let store = whole_bucket().await;
    put(&store, &root().bucket_key(Family::Owners, "c/a"), vec![0]).await;
    put(&store, "p/q/apps/acme/shop/deleted", b"who when".to_vec()).await;
    let report = check(&store, true).await;
    assert!(report.whole(), "{}", report.to_text());
    assert_eq!(
        (report.apps[0].state, report.apps[0].actors),
        ("deleted", 0)
    );
}

#[tokio::test]
async fn a_recorded_head_is_proven_as_a_restore_proves_it() {
    let root = root();
    let owner = root.bucket_key(Family::Owners, "c/a");
    let actor = root.bucket_key(Family::History, "c/a");
    let format = app_format::key_of(&root);
    let enabled = |store: Store| async {
        put(&store, &format, app_format::ENABLED_BODY.to_vec()).await;
        store
    };
    // The head itself: on the chain, and with --deep its checksum too.
    let store = enabled(whole_bucket().await).await;
    put(&store, &owner, released(Some(complete(3, 3)))).await;
    for deep in [false, true] {
        let report = check(&store, deep).await;
        assert!(report.whole(), "{}", report.to_text());
        assert!(report.apps[0].recorded_head);
    }
    // Another checksum than the history's: only reading tells.
    put(&store, &owner, released(Some(complete(3, 8)))).await;
    assert!(check(&store, false).await.whole());
    assert_eq!(
        only(&check(&store, true).await),
        (owner.as_str(), "recorded-head")
    );
    // A head the chain does not reach.
    put(&store, &owner, released(Some(complete(4, 4)))).await;
    for deep in [false, true] {
        assert_eq!(
            only(&check(&store, deep).await),
            (owner.as_str(), "recorded-head")
        );
    }
    // A head below the snapshot the plan starts from: proven by the links
    // from that snapshot back to it, as a restore proves it.
    put(&store, &owner, released(Some(complete(2, 2)))).await;
    put(&store, &format!("{actor}/{}", snap(3)), snapshot(3, 3)).await;
    for deep in [false, true] {
        let report = check(&store, deep).await;
        assert!(report.whole(), "{}", report.to_text());
        assert_eq!(report.apps[0].past_recorded_head, 1);
    }
    // Those links cut: not proven.
    store
        .delete(&Key::from(format!("{actor}/{}", wal(2))))
        .await
        .unwrap();
    assert_eq!(
        only(&check(&store, false).await),
        (owner.as_str(), "recorded-head")
    );
    // A record with a head and no history at all.
    let store = enabled(whole_bucket().await).await;
    let lost = root.bucket_key(Family::Owners, "c/lost");
    put(&store, &lost, released(Some(complete(3, 3)))).await;
    assert_eq!(
        only(&check(&store, false).await),
        (lost.as_str(), "recorded-head")
    );
}

/// A bucket whose first listing of anything leaves out one key, as a
/// listing that raced with a write does.
#[derive(Debug)]
struct HidesOnce {
    inner: Store,
    key: String,
    hidden: std::sync::atomic::AtomicBool,
}

impl std::fmt::Display for HidesOnce {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("HidesOnce")
    }
}

#[async_trait::async_trait]
impl ObjectStore for HidesOnce {
    async fn put_opts(
        &self,
        _: &Key,
        _: PutPayload,
        _: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        panic!("a PUT reached the bucket")
    }
    async fn put_multipart_opts(
        &self,
        _: &Key,
        _: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        panic!("a multipart upload reached the bucket")
    }
    async fn get_opts(
        &self,
        location: &Key,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        self.inner.get_opts(location, options).await
    }
    fn delete_stream(
        &self,
        _: futures_util::stream::BoxStream<'static, object_store::Result<Key>>,
    ) -> futures_util::stream::BoxStream<'static, object_store::Result<Key>> {
        panic!("a DELETE reached the bucket")
    }
    fn list(
        &self,
        prefix: Option<&Key>,
    ) -> futures_util::stream::BoxStream<'static, object_store::Result<ObjectMeta>> {
        let covers = prefix.is_none_or(|p| self.key.starts_with(p.as_ref()));
        let hide = covers && !self.hidden.swap(true, std::sync::atomic::Ordering::SeqCst);
        let key = self.key.clone();
        self.inner
            .list(prefix)
            .filter(move |m| {
                let hidden = hide && m.as_ref().is_ok_and(|m| m.location.as_ref() == key);
                std::future::ready(!hidden)
            })
            .boxed()
    }
    async fn list_with_delimiter(
        &self,
        prefix: Option<&Key>,
    ) -> object_store::Result<object_store::ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy_opts(
        &self,
        _: &Key,
        _: &Key,
        _: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        panic!("a COPY reached the bucket")
    }
}

#[tokio::test]
async fn what_one_listing_missed_is_listed_again_before_it_is_a_problem() {
    let key = format!("{}/{}", root().bucket_key(Family::History, "c/a"), wal(2));
    let store: Store = Arc::new(HidesOnce {
        inner: whole_bucket().await,
        key,
        hidden: std::sync::atomic::AtomicBool::new(false),
    });
    let report = check(&store, true).await;
    assert!(report.whole(), "{}", report.to_text());
    assert_eq!(report.apps[0].restorable, 1);
}

#[tokio::test]
async fn the_store_refuses_and_counts_every_write() {
    let inner: Store = Arc::new(InMemory::new());
    put(&inner, "k", vec![1, 2, 3]).await;
    let (store, counts) = store::ReadOnly::wrap(Arc::clone(&inner));
    assert!(store.put(&Key::from("new"), vec![1].into()).await.is_err());
    assert!(store.delete(&Key::from("k")).await.is_err());
    assert!(store.copy(&Key::from("k"), &Key::from("k2")).await.is_err());
    assert!(store.put_multipart(&Key::from("m")).await.is_err());
    assert_eq!(counts.writes(), 4);
    assert_eq!(
        store
            .get(&Key::from("k"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
            .len(),
        3
    );
    assert!(store.get(&Key::from("absent")).await.is_err());
    assert_eq!((counts.gets(), counts.objects(), counts.bytes()), (2, 1, 3));
    let listed: Vec<_> = inner.list(None).try_collect().await.unwrap();
    assert_eq!(listed.len(), 1, "a refused write changed the bucket");
    // One request per listing, one more per 1,000 keys.
    for i in 0..1_000 {
        put(&inner, &format!("many/{i}"), Vec::new()).await;
    }
    let listed: Vec<_> = store
        .list(Some(&Key::from("many")))
        .try_collect()
        .await
        .unwrap();
    assert_eq!(
        (listed.len(), counts.listed(), counts.lists()),
        (1_000, 1_000, 2)
    );
}

#[tokio::test]
async fn problems_are_kept_once_and_up_to_the_cap() {
    let (store, _) = store::ReadOnly::wrap(Arc::new(InMemory::new()));
    let run = Run {
        store,
        prefix: String::new(),
        deep: false,
        sample: None,
        budget: Arc::new(tokio::sync::Semaphore::new(1)),
        found: Mutex::default(),
    };
    run.add((0..MAX_PROBLEMS + 500).map(|i| problem(format!("k{i}"), "history", "gone")));
    run.add([problem("k1", "history", "gone")]);
    let found = run.found.into_inner().unwrap();
    assert_eq!(found.problems.len(), MAX_PROBLEMS);
    assert_eq!(found.total, MAX_PROBLEMS as u64 + 500);
}

#[test]
fn a_sample_keeps_its_size_and_every_slot_is_in_range() {
    let mut sample = Reservoir::new(Some(3));
    let mut kept = [0u32; 3];
    for i in 0..1_000 {
        if let Some(slot) = sample.offer().unwrap() {
            assert!(slot < 3 && (i >= 3 || slot == i));
            kept[slot] += 1;
        }
    }
    assert!(kept.iter().all(|n| *n >= 1));
    let mut none = Reservoir::new(None);
    assert!((0..10).all(|_| none.offer().unwrap().is_none()));
}

#[tokio::test]
async fn a_sample_reads_the_records_of_that_many_actors() {
    let store = whole_bucket().await;
    let root = root();
    for name in ["b", "c", "d", "e"] {
        let actor = root.bucket_key(Family::History, &format!("c/{name}"));
        put(&store, &format!("{actor}/{}", snap(1)), snapshot(1, 1)).await;
        put(
            &store,
            &root.bucket_key(Family::Owners, &format!("c/{name}")),
            released(None),
        )
        .await;
    }
    // One chain is broken: every chain is still checked by its names.
    let actor = root.bucket_key(Family::History, "c/a");
    store
        .delete(&Key::from(format!("{actor}/{}", wal(2))))
        .await
        .unwrap();
    let options = Options {
        sample: Some(2),
        deep: true,
        ..Options::default()
    };
    let report = verify(Arc::clone(&store), PREFIX, &options).await;
    assert_eq!(report.problems.len(), 1, "{}", report.to_text());
    let a = &report.apps[0];
    assert_eq!(
        (a.actors, a.restorable, a.records, a.records_read),
        (5, 4, 5, 2)
    );
    assert!(report.to_text().contains("2 actors of each app sampled"));
}

#[tokio::test]
async fn one_actor_is_checked_alone() {
    let store = whole_bucket().await;
    // Damage elsewhere is not this actor's.
    put(&store, "p/q/nodes/n2", vec![0]).await;
    let one = |name: &str| Options {
        app: Some(("acme".into(), "shop".into())),
        actor: Some(("c".into(), name.into())),
        deep: true,
        ..Options::default()
    };
    let report = verify(Arc::clone(&store), PREFIX, &one("a")).await;
    assert!(report.whole(), "{}", report.to_text());
    let text = report.to_text();
    assert!(
        text.contains("actor c/a of acme/shop: restorable (4 objects, ")
            && text.contains("ownership record read; history read whole")
            && text.ends_with("The actor is whole.\n")
            && !text.contains("nodes:")
            && !text.contains("alarms:"),
        "{text}"
    );
    assert_eq!((report.apps[0].actors, report.apps[0].restorable), (1, 1));
    let report = verify(Arc::clone(&store), PREFIX, &one("nobody")).await;
    assert_eq!(
        only(&report),
        ("p/q/state/acme/shop/actors/c/nobody", "history")
    );
    // An app the prefix holds nothing of is an error, not a whole bucket.
    let absent = Options {
        app: Some(("acme".into(), "typo".into())),
        ..Options::default()
    };
    let refused = verify(Arc::clone(&store), PREFIX, &absent).await;
    assert_eq!(refused.exit_code(), 2, "an app that is not there passed");
    assert!(refused.stopped[0].contains("acme/typo"), "{refused:?}");
    // A deleted app's actor is not "whole": nothing of it was checked.
    put(&store, "p/q/apps/acme/shop/deleted", b"who when".to_vec()).await;
    let report = verify(Arc::clone(&store), PREFIX, &one("a")).await;
    assert_eq!(report.exit_code(), 2, "{}", report.to_text());
    let text = report.to_text();
    assert!(
        text.contains("app acme/shop: deleted, not checked")
            && text.contains("stopped: app acme/shop: the app is deleted")
            && !text.contains("is whole")
            && !text.contains("restorable"),
        "{text}"
    );
}

/// What a [`Faulty`] bucket does with its key.
#[derive(Debug)]
enum Fault {
    /// Refuses that many reads of it, then gives it.
    Refuses(usize),
    /// Says once that it is not there.
    AbsentOnce,
    /// Refuses every listing whose prefix it is under.
    NoListing,
    /// Lists every key under it with an ETag of a megabyte.
    HugeTags,
}

/// A bucket with one fault at one key.
#[derive(Debug)]
struct Faulty {
    inner: Store,
    key: String,
    fault: Fault,
    gets: std::sync::atomic::AtomicUsize,
}

fn faulty(inner: Store, key: &str, fault: Fault) -> Store {
    Arc::new(Faulty {
        inner,
        key: key.to_owned(),
        fault,
        gets: std::sync::atomic::AtomicUsize::new(0),
    })
}

impl std::fmt::Display for Faulty {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Faulty")
    }
}

#[async_trait::async_trait]
impl ObjectStore for Faulty {
    async fn put_opts(
        &self,
        _: &Key,
        _: PutPayload,
        _: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        panic!("a PUT reached the bucket")
    }
    async fn put_multipart_opts(
        &self,
        _: &Key,
        _: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        panic!("a multipart upload reached the bucket")
    }
    async fn get_opts(
        &self,
        location: &Key,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        if location.as_ref() == self.key {
            let nth = self.gets.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            match self.fault {
                Fault::Refuses(times) if nth < times => {
                    return Err(object_store::Error::Generic {
                        store: "Faulty",
                        source: "access denied".into(),
                    });
                }
                Fault::AbsentOnce if nth == 0 => {
                    return Err(object_store::Error::NotFound {
                        path: location.to_string(),
                        source: "not yet".into(),
                    });
                }
                _ => {}
            }
        }
        self.inner.get_opts(location, options).await
    }
    fn delete_stream(
        &self,
        _: futures_util::stream::BoxStream<'static, object_store::Result<Key>>,
    ) -> futures_util::stream::BoxStream<'static, object_store::Result<Key>> {
        panic!("a DELETE reached the bucket")
    }
    fn list(
        &self,
        prefix: Option<&Key>,
    ) -> futures_util::stream::BoxStream<'static, object_store::Result<ObjectMeta>> {
        let under = prefix.is_some_and(|p| self.key.starts_with(p.as_ref()));
        match self.fault {
            Fault::NoListing if under => futures_util::stream::once(async {
                Err(object_store::Error::Generic {
                    store: "Faulty",
                    source: "listing denied".into(),
                })
            })
            .boxed(),
            Fault::HugeTags => {
                let key = self.key.clone();
                self.inner
                    .list(prefix)
                    .map_ok(move |mut meta| {
                        if meta.location.as_ref().starts_with(&key) {
                            meta.e_tag = Some("e".repeat(1 << 20));
                        }
                        meta
                    })
                    .boxed()
            }
            _ => self.inner.list(prefix),
        }
    }
    async fn list_with_delimiter(
        &self,
        prefix: Option<&Key>,
    ) -> object_store::Result<object_store::ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy_opts(
        &self,
        _: &Key,
        _: &Key,
        _: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        panic!("a COPY reached the bucket")
    }
}

/// What could not be read, alone in `report`: its key and its check. Not
/// a problem, and no verdict.
fn only_unread(report: &Report) -> (&str, &str) {
    assert!(report.problems.is_empty(), "{}", report.to_text());
    assert_eq!(report.unread.len(), 1, "{}", report.to_text());
    assert!(!report.whole() && report.exit_code() == 2);
    (report.unread[0].key.as_str(), report.unread[0].check)
}

#[tokio::test]
async fn an_object_the_bucket_refuses_to_give_is_not_read_not_broken() {
    let root = root();
    let actor = root.bucket_key(Family::History, "c/a");
    for (key, check_name, deep) in [
        (root.bucket_key(Family::Owners, "c/a"), "ownership", false),
        (format!("{actor}/{}", wal(2)), "history", true),
        ("p/q/nodes/n1".to_owned(), "node", false),
        (app_format::key_of(&root), "format", false),
        ("p/q/apps/acme/shop/current".to_owned(), "deploy", false),
        (format!("p/q/{}blob-usage", root.bucket()), "files", false),
    ] {
        let store = faulty(whole_bucket().await, &key, Fault::Refuses(usize::MAX));
        let report = check(&store, deep).await;
        assert_eq!(only_unread(&report), (key.as_str(), check_name));
        assert!(
            report.unread[0].detail.contains("access denied"),
            "{:?}",
            report.unread[0]
        );
        let text = report.to_text();
        assert!(
            text.contains(&format!("not read: {key}: "))
                && text.contains("No problem found, but the run was not complete")
                && !text.contains("is whole"),
            "{text}"
        );
        let json = report.to_json();
        assert_eq!(
            (&json["whole"], &json["exit_code"], &json["unread_total"]),
            (&json!(false), &json!(2), &json!(1))
        );
        assert_eq!(json["unread"][0]["key"], key.as_str());
    }
    // Refused more often than the reader asks at once, then given: asked
    // again after a pause, and whole.
    let key = root.bucket_key(Family::Owners, "c/a");
    let store = faulty(whole_bucket().await, &key, Fault::Refuses(4));
    let report = check(&store, false).await;
    assert!(report.whole(), "{}", report.to_text());
    // With a problem elsewhere, the verdict is the problem's.
    let store = whole_bucket().await;
    put(&store, "p/q/nodes/n2", vec![0]).await;
    let store = faulty(store, &key, Fault::Refuses(usize::MAX));
    let report = check(&store, false).await;
    assert_eq!((report.exit_code(), report.unread.len()), (1, 1));
    assert!(
        report
            .to_text()
            .contains("1 problems found, and the run was not complete.")
    );
}

#[tokio::test]
async fn a_listing_that_fails_leaves_what_was_found_until_then() {
    let root = root();
    let store = whole_bucket().await;
    // A problem found before the listing that fails.
    put(&store, &root.bucket_key(Family::Owners, "c/a"), vec![0]).await;
    let alarms = root.bucket_base(Family::Alarms);
    let store = faulty(store, &format!("{alarms}/x"), Fault::NoListing);
    let report = check(&store, false).await;
    let a = &report.apps[0];
    assert_eq!((a.actors, a.records, a.files), (1, 1, 0), "{a:?}");
    assert!(
        a.stopped
            .as_ref()
            .is_some_and(|why| why.contains("listing denied")),
        "{a:?}"
    );
    // The other families go on: the nodes are still read.
    assert_eq!(report.nodes, 1);
    assert_eq!(report.problems.len(), 1);
    assert_eq!(report.exit_code(), 1);
    let text = report.to_text();
    assert!(
        text.contains("stopped: app acme/shop: ") && text.contains("problem: "),
        "{text}"
    );
    assert!(report.to_json()["apps"][0]["stopped"].is_string());

    // Without that problem: no verdict, and the report is still there.
    let store = faulty(
        whole_bucket().await,
        &format!("{alarms}/x"),
        Fault::NoListing,
    );
    let report = check(&store, false).await;
    assert_eq!(report.exit_code(), 2, "{}", report.to_text());
    assert_eq!((report.apps[0].restorable, report.nodes), (1, 1));
    // The listing of the nodes: the apps were checked, and stay.
    let store = faulty(whole_bucket().await, "p/q/nodes/x", Fault::NoListing);
    let report = check(&store, false).await;
    assert_eq!((report.exit_code(), report.apps[0].restorable), (2, 1));
    assert!(report.stopped[0].contains("listing denied"), "{report:?}");
}

#[tokio::test]
async fn a_key_that_is_no_actors_is_named_and_the_rest_is_checked() {
    let root = root();
    let store = whole_bucket().await;
    let actors = root.bucket_base(Family::History);
    let strays = [format!("{actors}/c/marker"), format!("{actors}/readme")];
    for key in &strays {
        put(&store, key, Vec::new()).await;
    }
    // Under `owners/`, such a key is left alone and counted.
    put(&store, &root.bucket_key(Family::Owners, "readme"), vec![1]).await;
    put(&store, &root.bucket_key(Family::Owners, "c/a/b"), vec![1]).await;
    let report = check(&store, true).await;
    assert_eq!(report.exit_code(), 1, "{}", report.to_text());
    let mut named: Vec<_> = report
        .problems
        .iter()
        .map(|p| (p.key.clone(), p.check))
        .collect();
    named.sort();
    assert_eq!(
        named,
        strays
            .iter()
            .map(|k| (k.clone(), "history"))
            .collect::<Vec<_>>(),
        "{}",
        report.to_text()
    );
    let a = &report.apps[0];
    assert_eq!((a.actors, a.restorable, a.deep_checked), (1, 1, 1), "{a:?}");
    assert_eq!((a.records, a.records_skipped), (1, 2), "{a:?}");
}

#[tokio::test]
async fn an_actor_with_more_keys_than_the_listing_budget_is_not_checked() {
    let root = root();
    let store = whole_bucket().await;
    let busy = root.bucket_key(Family::History, "c/busy");
    for i in 0..40 {
        put(&store, &format!("{busy}/{}", wal(i + 1)), Vec::new()).await;
    }
    let store = faulty(store, &busy, Fault::HugeTags);
    let report = check(&store, false).await;
    assert_eq!(
        only_unread(&report),
        (format!("{busy}/").as_str(), "history")
    );
    assert!(
        report.unread[0].detail.contains("memory budget exceeded"),
        "{:?}",
        report.unread[0]
    );
    let a = &report.apps[0];
    assert_eq!((a.actors, a.restorable), (2, 1), "{a:?}");
}

#[tokio::test]
async fn what_changed_while_it_was_read_is_read_again_before_it_is_a_problem() {
    let root = root();
    // The format setting written after it was first read (`app format
    // --enable`): a version 2 record is then no problem.
    let store = whole_bucket().await;
    let format = app_format::key_of(&root);
    put(&store, &format, app_format::ENABLED_BODY.to_vec()).await;
    let owner = root.bucket_key(Family::Owners, "c/a");
    put(&store, &owner, released(Some(complete(3, 3)))).await;
    let store = faulty(store, &format, Fault::AbsentOnce);
    let report = check(&store, true).await;
    assert!(report.whole(), "{}", report.to_text());
    // An app deleted while it was read: what was found of it says nothing.
    let store = whole_bucket().await;
    put(&store, &owner, vec![0]).await;
    put(&store, "p/q/apps/acme/shop/deleted", b"who when".to_vec()).await;
    let store = faulty(store, "p/q/apps/acme/shop/deleted", Fault::AbsentOnce);
    let report = check(&store, false).await;
    assert!(report.whole(), "{}", report.to_text());
    assert_eq!(
        (report.apps[0].state, report.apps[0].actors),
        ("deleted", 0)
    );
    // A restore that is under way: not checked, and no verdict.
    let store = whole_bucket().await;
    put(&store, &owner, vec![0]).await;
    put(&store, "p/q/apps/acme/shop/restoring", b"{}".to_vec()).await;
    let report = check(&store, false).await;
    assert_eq!(
        only_unread(&report),
        ("p/q/apps/acme/shop/restoring", "deploy")
    );
    assert_eq!(report.apps[0].state, "restoring");
}

#[test]
fn a_full_set_says_so() {
    let mut seen = Seen::default();
    seen.insert("c", "a");
    assert!(seen.has("c", "a") && !seen.has("c", "b") && !seen.full);
}

/// The mark `cluster forget` leaves at `nodes/<id>` (ADR 0043) is not a
/// node and not a problem. A fresh one says nothing; one its command left
/// behind is a note, in `cluster status`'s words, that changes neither the
/// verdict nor the exit code. Anything else that does not decode is still
/// a problem.
#[tokio::test]
async fn a_forget_mark_is_not_a_node_and_an_old_one_is_a_note() {
    use wasmkeep_cluster::node_lease::encode_forgotten;
    let now = u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap();
    let store = whole_bucket().await;
    let before = check(&store, false).await;
    assert!(before.whole(), "{}", before.to_text());

    put(&store, "p/q/nodes/gone", encode_forgotten(now)).await;
    let report = check(&store, false).await;
    assert!(report.whole(), "{}", report.to_text());
    assert_eq!(
        (report.nodes, report.nodes_old_format),
        (before.nodes, before.nodes_old_format),
        "not counted as a node"
    );
    assert!(report.notes.is_empty(), "{:?}", report.notes);

    let old = now - FORGET_UNFINISHED_MS - 1_000;
    put(&store, "p/q/nodes/gone", encode_forgotten(old)).await;
    let report = check(&store, false).await;
    assert!(report.whole(), "{}", report.to_text());
    assert_eq!(report.exit_code(), 0);
    assert_eq!(
        report.notes,
        [
            "p/q/nodes/gone: forget did not finish; run `wasmkeep cluster forget gone` (no node \
          starts under that id until then)"
        ]
    );
    assert!(
        report
            .to_text()
            .contains("note: p/q/nodes/gone: forget did not finish"),
        "{}",
        report.to_text()
    );
    assert_eq!(report.to_json()["notes"][0], report.notes[0].as_str());
    assert_eq!(report.to_json()["exit_code"], 0);

    // A later mark number is not a mark.
    put(
        &store,
        "p/q/nodes/gone",
        b"wasmkeep-node-forgotten 2\n1\n".to_vec(),
    )
    .await;
    let report = check(&store, false).await;
    assert_eq!(report.exit_code(), 1);
    assert!(report.notes.is_empty());
    assert!(
        report.problems[0]
            .detail
            .contains("does not decode as a node lease or a heartbeat"),
        "{}",
        report.to_text()
    );
}

/// ADR 0045, section 4: the generation floor is read in its exact form;
/// markers left under `markers none` are redundant, not a problem.
#[tokio::test]
async fn a_generation_floor_is_read_and_markers_under_none_are_redundant() {
    use wasmkeep_cluster::generation_floor::{self as gf, GenerationFloor, Markers};
    let store = whole_bucket().await;
    let key = format!("{PREFIX}/{}", gf::key(&root()));
    let kept = GenerationFloor {
        generation: 9,
        markers: Markers::Kept,
    };
    put(&store, &key, kept.encode()).await;
    let report = check(&store, false).await;
    assert!(report.whole(), "{}", report.to_text());
    assert_eq!(report.apps[0].generation_floor, Some((9, false)));
    assert!(
        report
            .to_text()
            .contains("generation floor: 9, markers kept")
    );
    let none = GenerationFloor {
        markers: Markers::None,
        ..kept
    };
    put(&store, &key, none.encode()).await;
    let report = check(&store, false).await;
    assert!(report.whole(), "{}", report.to_text());
    assert_eq!(report.apps[0].redundant_markers, 1);
    assert!(
        report.to_text().contains("1 left are redundant"),
        "{}",
        report.to_text()
    );
    put(&store, &key, b"junk".to_vec()).await;
    let report = check(&store, false).await;
    assert_eq!(only(&report), (key.as_str(), "ownership"));
}
