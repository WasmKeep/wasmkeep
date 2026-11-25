//! `wasmkeep cluster verify` on a bucket real nodes wrote: three apps (two
//! record heads, one of them deployed; one keeps files), with history,
//! alarms, and ownership records held and released. It exits with 0 on
//! that bucket and never writes; with a segment deleted, a snapshot cut
//! short or an ownership record overwritten, it exits with 1 and names
//! the key.

use super::*;

use object_store::path::Path as StorePath;
use object_store::{ObjectStoreExt as _, PutPayload};
use wasmkeep_cli::verify::{Options, Report, verify};
use wasmkeep_cluster::replication::{Listing, plan_restore, segment_key, snapshot_key};

/// A bucket that fails every write, and counts them.
#[derive(Debug)]
struct NoWrites {
    inner: Arc<dyn ObjectStore>,
    writes: AtomicUsize,
}

impl std::fmt::Display for NoWrites {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "NoWrites")
    }
}

impl NoWrites {
    fn refused<T>(&self, what: &str) -> object_store::Result<T> {
        self.writes.fetch_add(1, Ordering::SeqCst);
        Err(object_store::Error::Generic {
            store: "NoWrites",
            source: format!("a {what} reached the bucket").into(),
        })
    }
}

#[async_trait::async_trait]
impl ObjectStore for NoWrites {
    async fn put_opts(
        &self,
        _: &StorePath,
        _: PutPayload,
        _: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        self.refused("PUT")
    }

    async fn put_multipart_opts(
        &self,
        _: &StorePath,
        _: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.refused("multipart upload")
    }

    async fn get_opts(
        &self,
        location: &StorePath,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        _: futures_util::stream::BoxStream<'static, object_store::Result<StorePath>>,
    ) -> futures_util::stream::BoxStream<'static, object_store::Result<StorePath>> {
        use futures_util::StreamExt as _;
        let refused = self.refused("DELETE");
        futures_util::stream::once(async move { refused }).boxed()
    }

    fn list(
        &self,
        prefix: Option<&StorePath>,
    ) -> futures_util::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
    {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&StorePath>,
    ) -> object_store::Result<object_store::ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        _: &StorePath,
        _: &StorePath,
        _: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        self.refused("COPY")
    }
}

/// Verifies `store` through [`NoWrites`]: no write was tried.
async fn run(store: &Arc<dyn ObjectStore>, options: Options) -> Report {
    let guarded = Arc::new(NoWrites {
        inner: Arc::clone(store),
        writes: AtomicUsize::new(0),
    });
    let report = verify(Arc::clone(&guarded) as Arc<dyn ObjectStore>, "", &options)
        .await
        .unwrap();
    assert_eq!(guarded.writes.load(Ordering::SeqCst), 0, "verify wrote");
    assert_eq!(report.writes_refused, 0, "verify tried to write");
    report
}

fn deep() -> Options {
    Options {
        deep: true,
        ..Options::default()
    }
}

/// A copy of everything `store` holds, to damage.
async fn copy_of(store: &Arc<dyn ObjectStore>) -> Arc<dyn ObjectStore> {
    use futures_util::TryStreamExt as _;
    let copy: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let listed: Vec<_> = store.list(None).try_collect().await.unwrap();
    for meta in listed {
        let body = store
            .get(&meta.location)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        copy.put(&meta.location, body.into()).await.unwrap();
    }
    copy
}

/// A node of app `tenant/app` serving `class` from `wasm`, its actors
/// hibernating (and releasing their records) once idle for `idle`.
async fn app_node(
    id: &str,
    data: &Path,
    store: &Arc<dyn ObjectStore>,
    (tenant, app): (&'static str, &'static str),
    class: &str,
    wasm: &[u8],
    idle: Duration,
) -> Node {
    node_configured(
        id,
        data,
        store,
        idle,
        &[class],
        wasm,
        Duration::from_secs(30),
        "",
        move |config| {
            config.tenant = tenant.into();
            config.app = app.into();
            config.cluster.as_mut().unwrap().app = format!("{tenant}/{app}");
        },
    )
    .await
}

async fn ok(addr: SocketAddr, method: &str, path: &str) {
    let r = send_runnable(addr, method, path, "").await;
    assert_eq!(r.status, 200, "{method} {path}: {}", r.body);
}

/// Whether every ownership record under `root` is released.
async fn all_released(store: &Arc<dyn ObjectStore>, root: &StateRoot) -> bool {
    use futures_util::TryStreamExt as _;
    let owners = StorePath::from(root.base(Family::Owners));
    let listed: Vec<_> = store.list(Some(&owners)).try_collect().await.unwrap();
    for meta in &listed {
        let body = store
            .get(&meta.location)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let record = wasmkeep_cluster::node_lease::OwnerRecord::decode(&body).unwrap();
        if record.holder.is_some() {
            return false;
        }
    }
    !listed.is_empty()
}

/// The keys under an actor's history folder, relative to it.
async fn history(store: &Arc<dyn ObjectStore>, folder: &str) -> Vec<String> {
    use futures_util::TryStreamExt as _;
    let base = StorePath::from(folder.trim_end_matches('/'));
    let listed: Vec<_> = store.list(Some(&base)).try_collect().await.unwrap();
    let prefix = format!("{}/", base.as_ref());
    listed
        .iter()
        .map(|m| {
            m.location
                .as_ref()
                .strip_prefix(&prefix)
                .unwrap()
                .to_owned()
        })
        .collect()
}

fn app<'a>(report: &'a Report, name: &str) -> &'a wasmkeep_cli::verify::AppReport {
    report
        .apps
        .iter()
        .find(|a| a.name == name)
        .unwrap_or_else(|| panic!("{name} is not in the report: {}", report.to_text()))
}

#[tokio::test(flavor = "multi_thread")]
async fn verify_passes_a_whole_bucket_and_names_the_key_of_each_fault() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let records = StateRoot::new("", "default", "default");
    let shop = StateRoot::new("", "acme", "shop");
    let files = StateRoot::new("", "acme", "files");
    // default/default and acme/shop record heads (ADR 0035); acme/files
    // writes version 1 records.
    for root in [&records, &shop] {
        store
            .put(
                &StorePath::from(wasmkeep_cluster::app_format::key_of(root)),
                wasmkeep_cluster::app_format::ENABLED_BODY.to_vec().into(),
            )
            .await
            .unwrap();
    }
    // acme/shop is deployed, twice: the second version names the first.
    let deployed = AppRef::new(Arc::clone(&store), "", "acme", "shop");
    let rt = deploy_runtime();
    deployed
        .deploy(&rt, vec![("t".into(), counter_wasm().to_vec())])
        .await
        .unwrap();
    deployed
        .deploy(&rt, vec![("t".into(), test_actor_wasm().to_vec())])
        .await
        .unwrap();

    let dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    // a's actors stay awake: held records, and segments after a snapshot.
    // b's and c's hibernate after a second: released records (in acme/shop
    // with the head they froze) and a snapshot at the head.
    let a = node_with(
        "a",
        dirs[0].path(),
        &store,
        Duration::from_secs(60),
        "t",
        test_actor_wasm(),
    )
    .await;
    let b = app_node(
        "b",
        dirs[1].path(),
        &store,
        ("acme", "shop"),
        "t",
        test_actor_wasm(),
        Duration::from_secs(1),
    )
    .await;
    let c = app_node(
        "c",
        dirs[2].path(),
        &store,
        ("acme", "files"),
        "blob",
        super::blobs::blob_actor(),
        Duration::from_secs(1),
    )
    .await;
    settle().await;

    // History: a first snapshot and segments after it. A name the store
    // escapes, and one long enough to be hashed in its keys. An alarm an
    // hour away stays in the index.
    let awkward = awkward_names();
    let names: Vec<&str> = ["x", "50%", "%41", awkward.last().unwrap().as_str()].to_vec();
    for node in [&a, &b] {
        for name in &names {
            let path = format!("/actor/t/{}/durable", url_encode(name));
            for _ in 0..6 {
                ok(node.addr, "POST", &path).await;
            }
        }
        ok(node.addr, "POST", "/actor/t/x/alarm?in=3600000").await;
        ok(node.addr, "POST", "/actor/t/50%25/alarm?in=3600000").await;
    }
    ok(c.addr, "POST", "/actor/blob/alice/touch").await;
    let put = |path: &str, body: &str| {
        format!(
            "PUT {path} HTTP/1.1\r\nHost: t\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        )
    };
    let r = exchange(c.addr, &put("/actor/blob/alice/b/photos/1.txt", "hello")).await;
    assert_eq!(r.status, 200, "{}", r.body);

    // Deleted actors: one by the operator, in an app that records heads;
    // one by itself, with a file, whose sweep record stays.
    for _ in 0..2 {
        ok(a.addr, "POST", "/actor/t/doomed/durable").await;
    }
    let asked = request_delete_of(&store, "t", "doomed").await;
    eventually("the operator's delete is done", async || {
        store.head(&asked).await.is_err()
    })
    .await;
    let r = exchange(c.addr, &put("/actor/blob/doomed/b/k", "old")).await;
    assert_eq!(r.status, 200, "{}", r.body);
    let r = send_runnable(c.addr, "POST", "/actor/blob/doomed/delete", "").await;
    assert_eq!((r.status, r.body.as_str()), (200, "deleted"));
    eventually("the deleted actor's files are swept", async || {
        use futures_util::TryStreamExt as _;
        let sweeps = StorePath::from(format!("{}blob-sweeps", files.bucket()));
        let listed: Vec<_> = store.list(Some(&sweeps)).try_collect().await.unwrap();
        let mut swept = !listed.is_empty();
        for meta in &listed {
            let body = store
                .get(&meta.location)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap();
            swept &= String::from_utf8_lossy(&body).contains("\nswept\n");
        }
        swept
    })
    .await;

    // Quiet: b's and c's actors hibernated and those nodes stopped. a
    // runs on, its actors awake and idle: the bucket of a live cluster.
    for root in [&shop, &files] {
        eventually("every record is released", async || {
            all_released(&store, root).await
        })
        .await;
    }
    for node in [b, c] {
        stop_listening(node).await;
    }
    tokio::time::sleep(Duration::from_millis(300)).await;

    // The whole bucket: by key names, then with every body read.
    let shallow = run(&store, Options::default()).await;
    println!("{}", shallow.to_text());
    assert!(shallow.whole(), "{}", shallow.to_text());
    assert_eq!(shallow.exit_code(), 0);
    let whole = run(&store, deep()).await;
    println!("{}", whole.to_text());
    assert!(whole.whole(), "{}", whole.to_text());
    assert_eq!(
        whole
            .apps
            .iter()
            .map(|a| a.name.as_str())
            .collect::<Vec<_>>(),
        ["acme/files", "acme/shop", "default/default"]
    );
    let d = app(&whole, "default/default");
    assert!(d.recorded_head && d.state == "not deployed", "{d:?}");
    assert_eq!((d.actors, d.restorable, d.deep_checked), (5, 5, 5), "{d:?}");
    assert_eq!((d.records, d.records_read, d.alarms), (5, 5, 2), "{d:?}");
    let s = app(&whole, "acme/shop");
    assert_eq!((s.state, s.versions_checked), ("deployed", 2), "{s:?}");
    assert_eq!((s.actors, s.restorable, s.alarms), (4, 4, 2), "{s:?}");
    assert!(s.recorded_head && s.floor.is_some(), "{s:?}");
    let f = app(&whole, "acme/files");
    assert_eq!(
        (f.actors, f.restorable, f.files, f.file_bytes),
        (2, 2, 1, 5),
        "{f:?}"
    );
    assert!(!f.recorded_head && f.sweep_records == 1, "{f:?}");
    assert_eq!(whole.nodes, 3, "{}", whole.to_text());
    assert!(whole.objects_read > shallow.objects_read && whole.requests > 0);
    let json = whole.to_json();
    assert_eq!(json["whole"], true);
    assert_eq!(json["apps"][2]["actors"], 5);
    assert_eq!(json["problems"].as_array().unwrap().len(), 0);

    // One app, one actor, a sample.
    let mut one = run(
        &store,
        Options {
            app: Some(("acme".into(), "shop".into())),
            ..deep()
        },
    )
    .await;
    one.bucket = "s3://wasmkeep-demo".into();
    println!("{}", one.to_text());
    assert!(one.whole() && one.apps.len() == 1, "{}", one.to_text());
    let actor = run(
        &store,
        Options {
            app: Some(("default".into(), "default".into())),
            actor: Some(("t".into(), "50%".into())),
            ..deep()
        },
    )
    .await;
    assert!(actor.whole(), "{}", actor.to_text());
    assert_eq!(
        (actor.apps[0].restorable, actor.apps[0].deep_checked),
        (1, 1)
    );
    let absent = run(
        &store,
        Options {
            app: Some(("default".into(), "default".into())),
            actor: Some(("t".into(), "nobody".into())),
            ..Options::default()
        },
    )
    .await;
    assert_eq!(absent.exit_code(), 1, "{}", absent.to_text());
    let sampled = run(
        &store,
        Options {
            sample: Some(2),
            ..deep()
        },
    )
    .await;
    assert!(sampled.whole(), "{}", sampled.to_text());
    let d = app(&sampled, "default/default");
    assert_eq!((d.actors, d.restorable, d.deep_checked), (5, 5, 2), "{d:?}");

    // A component that is not the one its version names; a kept previous
    // version is read only with every body.
    let current = deployed.current().await.unwrap().unwrap();
    let manifest_key = StorePath::from(format!("apps/acme/shop/{}/manifest", current.version));
    let manifest = store
        .get(&manifest_key)
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let manifest =
        wasmkeep_server::apps::Manifest::parse(std::str::from_utf8(&manifest).unwrap()).unwrap();
    let previous = manifest.previous.unwrap();
    for (version, options) in [(&current.version, Options::default()), (&previous, deep())] {
        let broken = copy_of(&store).await;
        let component = format!("apps/acme/shop/{version}/t.wasm");
        broken
            .put(
                &StorePath::from(component.as_str()),
                b"other".to_vec().into(),
            )
            .await
            .unwrap();
        let report = run(&broken, options).await;
        assert_eq!(report.exit_code(), 1, "{}", report.to_text());
        assert_eq!(report.problems.len(), 1, "{}", report.to_text());
        assert_eq!(
            (report.problems[0].key.as_str(), report.problems[0].check),
            (component.as_str(), "deploy"),
            "{}",
            report.to_text()
        );
    }

    // The chain of default/default's t/x.
    let folder = wasmkeep_server::history_folder(&records, "t", "x");
    let keys = history(&store, &folder).await;
    let listing = Listing::from_keys(keys.iter().map(String::as_str));
    let plan = plan_restore(&listing).unwrap();
    assert!(plan.segments.len() >= 3, "{plan:?}");
    let snapshot = format!("{folder}{}", snapshot_key(plan.snapshot.unwrap()));

    // Fault 1: a segment between the snapshot and the head is deleted.
    let middle = plan.segments[plan.segments.len() / 2];
    let segment = listing.segments.iter().find(|s| s.id == middle).unwrap();
    let deleted = format!("{folder}{}", segment_key(segment));
    let broken = copy_of(&store).await;
    broken
        .delete(&StorePath::from(deleted.as_str()))
        .await
        .unwrap();
    for options in [Options::default(), deep()] {
        let report = run(&broken, options).await;
        assert_eq!(report.exit_code(), 1, "{}", report.to_text());
        assert_eq!(report.problems.len(), 1, "{}", report.to_text());
        let p = &report.problems[0];
        // The key, but for the generation of the segment before it, which
        // only the deleted key itself told.
        let (stem, _) = p.key.split_once('*').unwrap();
        assert!(
            deleted.starts_with(stem) && stem.len() == deleted.len() - 20,
            "{p:?}"
        );
        assert_eq!(p.check, "history");
        assert!(report.to_text().contains(&format!("problem: {}", p.key)));
        assert_eq!(app(&report, "default/default").restorable, 4);
    }

    // Fault 2: the snapshot is cut short. Its name and its place in the
    // chain are whole: only reading it tells.
    let broken = copy_of(&store).await;
    let path = StorePath::from(snapshot.as_str());
    let body = broken.get(&path).await.unwrap().bytes().await.unwrap();
    assert!(body.len() > 400, "{}", body.len());
    broken
        .put(&path, body.slice(..body.len() / 2).into())
        .await
        .unwrap();
    let report = run(&broken, deep()).await;
    assert_eq!(report.exit_code(), 1, "{}", report.to_text());
    assert_eq!(report.problems.len(), 1, "{}", report.to_text());
    assert_eq!(
        (report.problems[0].key.as_str(), report.problems[0].check),
        (snapshot.as_str(), "history")
    );
    assert_eq!(report.to_json()["problems"][0]["key"], snapshot.as_str());
    // Cut to less than a history object's header and trailer, its size
    // in the listing tells too.
    broken.put(&path, body.slice(..64).into()).await.unwrap();
    let report = run(&broken, Options::default()).await;
    assert_eq!(report.exit_code(), 1, "{}", report.to_text());
    assert_eq!(report.problems[0].key, snapshot);

    // Fault 3: an ownership record that does not decode, in each app
    // format, one under a name the store escapes.
    let broken = copy_of(&store).await;
    let mut unreadable = Vec::new();
    for (root, tail) in [(&records, "t/%2541"), (&files, "blob/alice")] {
        let key = StorePath::from(root.key(Family::Owners, tail));
        broken.get(&key).await.expect("the record");
        broken
            .put(&key, b"wasmkeep-owner 9\nnot a record\n".to_vec().into())
            .await
            .unwrap();
        unreadable.push(key.as_ref().to_owned());
    }
    let report = run(&broken, Options::default()).await;
    assert_eq!(report.exit_code(), 1, "{}", report.to_text());
    let mut found: Vec<_> = report
        .problems
        .iter()
        .map(|p| (p.key.clone(), p.check))
        .collect();
    found.sort();
    unreadable.sort();
    assert_eq!(
        found,
        unreadable
            .iter()
            .map(|k| (k.clone(), "ownership"))
            .collect::<Vec<_>>(),
        "{}",
        report.to_text()
    );

    // A recorded head the bucket's chain does not reach: acme/shop's t/x
    // was released at its head; with the objects at that position gone,
    // the chain is whole but ends one transaction early.
    let folder = wasmkeep_server::history_folder(&shop, "t", "x");
    let keys = history(&store, &folder).await;
    let listing = Listing::from_keys(keys.iter().map(String::as_str));
    let head = plan_restore(&listing).unwrap().head();
    let broken = copy_of(&store).await;
    let at_head = format!("{:016x}-", head.txid);
    let mut gone = 0;
    for key in keys.iter().filter(|k| k.contains(&at_head)) {
        broken
            .delete(&StorePath::from(format!("{folder}{key}")))
            .await
            .unwrap();
        gone += 1;
    }
    assert!(gone >= 1 && gone < keys.len(), "{keys:?}");
    let report = run(&broken, Options::default()).await;
    assert_eq!(report.exit_code(), 1, "{}", report.to_text());
    assert_eq!(report.problems.len(), 1, "{}", report.to_text());
    assert_eq!(
        (report.problems[0].key.as_str(), report.problems[0].check),
        (shop.key(Family::Owners, "t/x").as_str(), "recorded-head"),
        "{}",
        report.to_text()
    );

    // An alarm entry for an actor the bucket does not know.
    let broken = copy_of(&store).await;
    let entry = wasmkeep_cluster::alarm_index::Entry::owned(9_999_999_999_999, 3, "ab").unwrap();
    drop(a);
    let stray = shop.key(
        Family::Alarms,
        &wasmkeep_cluster::alarm_index::key_tail("t", "ghost", &entry),
    );
    broken
        .put(&StorePath::from(stray.as_str()), Vec::new().into())
        .await
        .unwrap();
    let report = run(&broken, Options::default()).await;
    assert_eq!(report.exit_code(), 1, "{}", report.to_text());
    assert_eq!(
        (report.problems[0].key.as_str(), report.problems[0].check),
        (stray.as_str(), "alarm"),
        "{}",
        report.to_text()
    );
}
