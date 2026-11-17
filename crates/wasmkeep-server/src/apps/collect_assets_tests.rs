#![allow(clippy::unwrap_used)]
use super::*;
use crate::apps::{Changes, Manifest};
use crate::assets::{Bundle, Change, Content, File, Index, digest};
use futures_util::{StreamExt, stream::BoxStream};
use object_store::{memory::InMemory, path::Path, *};
use std::collections::BTreeMap;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};

// The bucket clock is two hours behind the workstation. Per-key ages let
// tests cross retention boundaries without sleeping or changing the host clock.
#[derive(Debug, Default, Clone)]
struct Bucket {
    inner: Arc<InMemory>,
    ages: Arc<Mutex<BTreeMap<Path, Duration>>>,
    pause: Arc<AtomicBool>,
    listed: Arc<tokio::sync::Notify>,
    resume: Arc<tokio::sync::Notify>,
}
impl std::fmt::Display for Bucket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "test bucket")
    }
}
impl Bucket {
    fn meta(&self, mut m: ObjectMeta) -> ObjectMeta {
        m.last_modified -= Duration::from_secs(7200)
            + self
                .ages
                .lock()
                .unwrap()
                .get(&m.location)
                .copied()
                .unwrap_or_default();
        m
    }
    fn age(&self, key: Path, seconds: u64) {
        self.ages
            .lock()
            .unwrap()
            .insert(key, Duration::from_secs(seconds));
    }
}
#[async_trait::async_trait]
impl ObjectStore for Bucket {
    async fn put_opts(&self, p: &Path, b: PutPayload, o: PutOptions) -> Result<PutResult> {
        self.ages.lock().unwrap().remove(p);
        self.inner.put_opts(p, b, o).await
    }
    async fn put_multipart_opts(
        &self,
        p: &Path,
        o: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(p, o).await
    }
    async fn get_opts(&self, p: &Path, o: GetOptions) -> Result<GetResult> {
        let mut r = self.inner.get_opts(p, o).await?;
        r.meta = self.meta(r.meta);
        Ok(r)
    }
    fn delete_stream(
        &self,
        p: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        self.inner.delete_stream(p)
    }
    fn list(&self, p: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        let this = self.clone();
        let pause = p.is_some_and(|p| p.as_ref().ends_with("_assets/content"))
            && self.pause.swap(false, Ordering::SeqCst);
        let stream = self.inner.list(p).map(move |r| r.map(|m| this.meta(m)));
        let this = self.clone();
        stream
            .chain(
                futures_util::stream::once(async move {
                    if pause {
                        this.listed.notify_one();
                        this.resume.notified().await;
                    }
                    None
                })
                .filter_map(std::future::ready),
            )
            .boxed()
    }
    async fn list_with_delimiter(&self, p: Option<&Path>) -> Result<ListResult> {
        self.inner.list_with_delimiter(p).await
    }
    async fn copy_opts(&self, a: &Path, b: &Path, o: CopyOptions) -> Result<()> {
        self.inner.copy_opts(a, b, o).await
    }
}
fn setup() -> (Arc<Bucket>, AppRef) {
    let store = Arc::new(Bucket::default());
    let app = AppRef::new(store.clone(), "", "acme", "shop");
    (store, app)
}
fn bundle() -> Bundle {
    let hash = digest(b"abc");
    Bundle {
        index: Index {
            files: BTreeMap::from([(
                "a.bin".into(),
                File {
                    content_type: "application/octet-stream".into(),
                    identity: Content {
                        digest: hash.clone(),
                        size: 3,
                    },
                    br: None,
                    gzip: None,
                },
            )]),
            ..Index::default()
        },
        contents: BTreeMap::from([(hash, b"abc".to_vec())]),
    }
}
async fn content(store: &Arc<Bucket>, app: &AppRef, age: u64) -> Path {
    let key = app.path(&format!("_assets/content/{}", digest(b"abc")));
    store.put(&key, b"abc".to_vec().into()).await.unwrap();
    store.age(key.clone(), age);
    key
}
async fn reference(store: &Arc<Bucket>, app: &AppRef, version: &str, current: bool) -> Path {
    let b = bundle();
    let key = app.path(&format!("_assets/index/{}", digest(&b.index.bytes())));
    store.put(&key, b.index.bytes().into()).await.unwrap();
    store.age(key.clone(), 7200);
    let mut m = Manifest::parse("wasmkeep-manifest 1\n").unwrap();
    m.assets = Some(digest(&b.index.bytes()));
    store
        .put(&app.manifest_path(version), m.format().into())
        .await
        .unwrap();
    if current {
        store
            .put(&app.path("current"), format!("{version}\n1\n").into())
            .await
            .unwrap();
    }
    key
}
#[tokio::test]
async fn unused_old_deleted_young_and_unknown_keys_kept() {
    let (s, a) = setup();
    let old = content(&s, &a, 7200).await;
    let unknown = a.path("_assets/content/not-a-digest");
    s.put(&unknown, vec![].into()).await.unwrap();
    s.age(unknown.clone(), 7200);
    assert_eq!(
        a.collect_assets(Duration::from_secs(3600)).await.unwrap(),
        1
    );
    assert!(s.head(&old).await.is_err());
    assert!(s.head(&unknown).await.is_ok());
    let young = content(&s, &a, 0).await;
    assert_eq!(
        a.collect_assets(Duration::from_secs(3600)).await.unwrap(),
        0
    );
    assert!(s.head(&young).await.is_ok());
}
#[tokio::test]
async fn every_version_including_orphan_keeps_index_and_content() {
    let (s, a) = setup();
    let c = content(&s, &a, 7200).await;
    let i = reference(&s, &a, "orphan", false).await;
    assert_eq!(a.collect_assets(Duration::ZERO).await.unwrap(), 0);
    assert!(s.head(&c).await.is_ok());
    assert!(s.head(&i).await.is_ok());
}
#[tokio::test]
async fn fresh_deploy_blocks_stale_killed_deploy_does_not() {
    let (s, a) = setup();
    let c = content(&s, &a, 7200).await;
    s.pause.store(true, Ordering::SeqCst);
    let ad = a.clone();
    let deploy = tokio::spawn(async move {
        let runtime =
            wasmkeep_runtime::Runtime::new(&wasmkeep_runtime::RuntimeConfig::default()).unwrap();
        ad.deploy_changes(
            &runtime,
            Changes {
                assets: Change::Set(bundle()),
                ..Changes::default()
            },
        )
        .await
    });
    tokio::time::timeout(Duration::from_secs(5), s.listed.notified())
        .await
        .unwrap();
    let marker = s
        .list(Some(&a.path("_assets/deploying")))
        .try_next()
        .await
        .unwrap()
        .unwrap()
        .location;
    assert_eq!(a.collect_assets(Duration::ZERO).await.unwrap(), 0);
    assert!(s.head(&c).await.is_ok());
    deploy.abort();
    assert!(deploy.await.unwrap_err().is_cancelled());
    assert!(s.head(&marker).await.is_ok());
    s.age(marker.clone(), 3601);
    assert_eq!(a.collect_assets(Duration::ZERO).await.unwrap(), 1);
    assert!(s.head(&marker).await.is_err());
}
#[tokio::test]
async fn missing_current_index_stops_other_missing_index_names_nothing() {
    let (s, a) = setup();
    let c = content(&s, &a, 7200).await;
    let i = reference(&s, &a, "v1", true).await;
    s.delete(&i).await.unwrap();
    assert!(a.collect_assets(Duration::ZERO).await.is_err());
    assert!(s.head(&c).await.is_ok());
    s.delete(&a.path("current")).await.unwrap();
    assert_eq!(a.collect_assets(Duration::ZERO).await.unwrap(), 1);
}
#[tokio::test]
async fn unknown_manifest_stops_before_deleting_assets() {
    let (s, a) = setup();
    let c = content(&s, &a, 7200).await;
    s.put(&a.manifest_path("orphan"), "wasmkeep-manifest 99\n".into())
        .await
        .unwrap();
    assert!(a.collect_assets(Duration::ZERO).await.is_err());
    assert!(s.head(&c).await.is_ok());
}
#[tokio::test]
async fn version_collection_uses_bucket_clock_not_two_hours_fast_local_clock() {
    let (s, a) = setup();
    content(&s, &a, 0).await;
    for (v, prev) in [("v0", None), ("v1", Some("v0"))] {
        let mut m = Manifest::parse("wasmkeep-manifest 1\n").unwrap();
        m.previous = prev.map(str::to_owned);
        s.put(&a.manifest_path(v), m.format().into()).await.unwrap();
    }
    s.put(
        &a.path("current"),
        super::super::Current {
            version: "v1".into(),
            seq: 2,
            marked: false,
            rooted: false,
        }
        .format()
        .into(),
    )
    .await
    .unwrap();
    assert_eq!(
        a.collect_versions(0, Duration::from_secs(3600))
            .await
            .unwrap(),
        0
    );
    assert!(s.head(&a.manifest_path("v0")).await.is_ok());
}
#[tokio::test]
async fn collection_first_forces_deploy_to_wait_then_reupload() {
    let (s, a) = setup();
    let c = content(&s, &a, 7200).await;
    s.pause.store(true, Ordering::SeqCst);
    let ac = a.clone();
    let collect = tokio::spawn(async move { ac.collect_assets(Duration::ZERO).await });
    tokio::time::timeout(Duration::from_secs(5), s.listed.notified())
        .await
        .unwrap();
    let ad = a.clone();
    let deploy = tokio::spawn(async move {
        let runtime =
            wasmkeep_runtime::Runtime::new(&wasmkeep_runtime::RuntimeConfig::default()).unwrap();
        ad.deploy_changes(
            &runtime,
            Changes {
                assets: Change::Set(bundle()),
                ..Changes::default()
            },
        )
        .await
    });
    loop {
        if s.list(Some(&a.path("_assets/deploying")))
            .try_next()
            .await
            .unwrap()
            .is_some()
        {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert!(!deploy.is_finished());
    s.resume.notify_one();
    assert_eq!(collect.await.unwrap().unwrap(), 1);
    let deployed = deploy.await.unwrap().unwrap();
    assert!(s.head(&c).await.is_ok());
    let hash = a
        .manifest(&deployed.current.version)
        .await
        .unwrap()
        .assets
        .unwrap();
    assert!(a.read_assets_index(&hash).await.is_ok());
}
#[tokio::test]
async fn deploy_first_collector_deletes_nothing() {
    let (s, a) = setup();
    let c = content(&s, &a, 7200).await;
    s.pause.store(true, Ordering::SeqCst);
    let ad = a.clone();
    let deploy = tokio::spawn(async move {
        let runtime =
            wasmkeep_runtime::Runtime::new(&wasmkeep_runtime::RuntimeConfig::default()).unwrap();
        ad.deploy_changes(
            &runtime,
            Changes {
                assets: Change::Set(bundle()),
                ..Changes::default()
            },
        )
        .await
    });
    tokio::time::timeout(Duration::from_secs(5), s.listed.notified())
        .await
        .unwrap();
    assert_eq!(a.collect_assets(Duration::ZERO).await.unwrap(), 0);
    s.resume.notify_one();
    deploy.await.unwrap().unwrap();
    assert!(s.head(&c).await.is_ok());
}

#[tokio::test(start_paused = true)]
async fn five_minute_deadline_keeps_candidates_and_cleans_marker() {
    let (s, a) = setup();
    let c = content(&s, &a, 7200).await;
    s.pause.store(true, Ordering::SeqCst);
    let ac = a.clone();
    let task = tokio::spawn(async move { ac.collect_assets(Duration::ZERO).await });
    tokio::time::timeout(Duration::from_secs(5), s.listed.notified())
        .await
        .unwrap();
    tokio::time::advance(Duration::from_secs(300)).await;
    s.resume.notify_one();
    assert_eq!(task.await.unwrap().unwrap(), 0);
    assert!(s.head(&c).await.is_ok());
    assert!(
        s.list(Some(&a.path("_assets/collecting")))
            .try_next()
            .await
            .unwrap()
            .is_none()
    );
}
#[tokio::test]
async fn unused_index_deleted_but_invalid_referenced_index_stops_collection() {
    let (s, a) = setup();
    let c = content(&s, &a, 7200).await;
    let i = a.path(&format!("_assets/index/{}", digest(b"unused")));
    s.put(&i, b"unused".to_vec().into()).await.unwrap();
    s.age(i.clone(), 7200);
    assert_eq!(a.collect_assets(Duration::ZERO).await.unwrap(), 2);
    assert!(s.head(&i).await.is_err());
    assert!(s.head(&c).await.is_err());
    let c = content(&s, &a, 7200).await;
    let i = reference(&s, &a, "v1", true).await;
    s.put(&i, b"corrupt".to_vec().into()).await.unwrap();
    assert!(a.collect_assets(Duration::ZERO).await.is_err());
    assert!(s.head(&c).await.is_ok());
}
#[tokio::test]
async fn rollback_missing_index_refused_without_switching_current() {
    let (s, a) = setup();
    let runtime =
        wasmkeep_runtime::Runtime::new(&wasmkeep_runtime::RuntimeConfig::default()).unwrap();
    let first = a
        .deploy_changes(
            &runtime,
            Changes {
                assets: Change::Set(bundle()),
                ..Changes::default()
            },
        )
        .await
        .unwrap();
    a.deploy_changes(
        &runtime,
        Changes {
            assets: Change::Remove,
            ..Changes::default()
        },
    )
    .await
    .unwrap();
    let before = a.current().await.unwrap();
    let hash = a
        .manifest(&first.current.version)
        .await
        .unwrap()
        .assets
        .unwrap();
    s.delete(&a.path(&format!("_assets/index/{hash}")))
        .await
        .unwrap();
    assert!(a.rollback(false).await.is_err());
    assert_eq!(a.current().await.unwrap(), before);
}

#[tokio::test]
async fn assets_are_collected_after_their_last_version_is_collected() {
    let (s, a) = setup();
    let runtime =
        wasmkeep_runtime::Runtime::new(&wasmkeep_runtime::RuntimeConfig::default()).unwrap();
    let first = a
        .deploy_changes(
            &runtime,
            Changes {
                assets: Change::Set(bundle()),
                ..Changes::default()
            },
        )
        .await
        .unwrap();
    a.deploy_changes(
        &runtime,
        Changes {
            assets: Change::Remove,
            ..Changes::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(a.collect(0, Duration::ZERO).await.unwrap(), (1, 2));
    assert!(
        s.head(&a.manifest_path(&first.current.version))
            .await
            .is_err()
    );
    assert!(
        s.list(Some(&a.path("_assets")))
            .try_next()
            .await
            .unwrap()
            .is_none()
    );
}
