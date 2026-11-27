//! The generation floor of an app (ADR 0045): a node of 0.10 seeds an
//! actor's first ownership record as 0.9 does until the app's upgrade
//! floor is 0.10, and from then on writes no per-actor marker; after
//! `cluster markers clean` it skips the marker's read. Each row of section
//! 2's table is checked by the requests the node sends for the marker key,
//! the upgrade floor and the generation floor, on a node that serves
//! through the finalize without a restart.
//!
//! The workspace version may still be 0.9: the node is configured to
//! announce 0.10.0, as the upgrade tests configure theirs.

use std::ops::Range;
use std::sync::Mutex;

use futures_util::stream::BoxStream;
use object_store::ObjectStoreExt as _;
use object_store::path::Path as StorePath;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions,
};
use wasmkeep_cluster::app_format::floor::{self, Minor};
use wasmkeep_cluster::generation_floor::{self as gf, GenerationFloor, Markers};
use wasmkeep_cluster::object_store::ObjectStoreBucket;

use super::*;

/// The requests a node sends for the keys this test watches, as
/// `(method, key)`.
#[derive(Debug, Default)]
struct Counted(Mutex<Vec<(&'static str, String)>>);

impl Counted {
    fn note(&self, method: &'static str, key: &str) {
        let watched = key.contains("/leases/")
            || key.ends_with("/generation-floor")
            || (key.starts_with("formats/") && key.ends_with("/floor"));
        if watched {
            self.0.lock().unwrap().push((method, key.to_owned()));
        }
    }

    /// Clears the log and gives what it held, as counts per method and
    /// family: `GET leases`, `PUT leases`, `GET upgrade-floor`,
    /// `GET generation-floor`.
    fn take(&self) -> Vec<String> {
        let mut out: Vec<String> = std::mem::take(&mut *self.0.lock().unwrap())
            .into_iter()
            .map(|(method, key)| {
                let family = if key.contains("/leases/") {
                    "leases"
                } else if key.ends_with("/generation-floor") {
                    "generation-floor"
                } else {
                    "upgrade-floor"
                };
                format!("{method} {family}")
            })
            .collect();
        out.sort();
        out
    }
}

#[derive(Debug)]
struct Counting {
    inner: Arc<dyn ObjectStore>,
    log: Arc<Counted>,
}

impl std::fmt::Display for Counting {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Counting({})", self.inner)
    }
}

#[async_trait::async_trait]
impl ObjectStore for Counting {
    async fn put_opts(
        &self,
        location: &StorePath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.log.note("PUT", location.as_ref());
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &StorePath,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &StorePath,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        if !options.head {
            self.log.note("GET", location.as_ref());
        }
        self.inner.get_opts(location, options).await
    }

    async fn get_ranges(
        &self,
        location: &StorePath,
        ranges: &[Range<u64>],
    ) -> object_store::Result<Vec<bytes::Bytes>> {
        self.inner.get_ranges(location, ranges).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<StorePath>>,
    ) -> BoxStream<'static, object_store::Result<StorePath>> {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&StorePath>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&StorePath>,
        offset: &StorePath,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&StorePath>,
    ) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &StorePath,
        to: &StorePath,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(
        &self,
        from: &StorePath,
        to: &StorePath,
        options: RenameOptions,
    ) -> object_store::Result<()> {
        self.inner.rename_opts(from, to, options).await
    }
}

/// The wall clock, for the cleanup's old-lease rule.
struct Now;
impl wasmkeep_cluster::lease::Clock for Now {
    fn now_ms(&self) -> u64 {
        now_ms()
    }
}

const NINE: Minor = Minor { major: 0, minor: 9 };

async fn body(store: &Arc<dyn ObjectStore>, key: &str) -> Option<String> {
    match store.get(&StorePath::from(key)).await {
        Ok(got) => Some(String::from_utf8(got.bytes().await.unwrap().to_vec()).unwrap()),
        Err(object_store::Error::NotFound { .. }) => None,
        Err(e) => panic!("{e}"),
    }
}

fn marker_key(name: &str) -> String {
    test_key(Family::Leases, &format!("t/{name}"))
}

fn generation_floor_key() -> String {
    gf::key(&StateRoot::new("", "default", "default"))
}

/// ADR 0045, sections 2 and 5, on one node of 0.10 that serves through the
/// finalize and the cleanup:
///
/// - before the finalize it writes exactly what 0.9 does (the marker, from
///   the keys), reading the upgrade floor once more per first acquisition;
/// - after it, no marker and no read of the upgrade floor again: the
///   marker's GET and the generation floor's;
/// - a restore's marker that is the only carrier of a seed is moved into
///   the generation floor by the cleanup, and the actor's first record
///   starts above it with the marker gone;
/// - after the cleanup, the generation floor's GET alone.
#[tokio::test(flavor = "multi_thread")]
async fn a_node_serving_through_the_finalize_stops_writing_markers() {
    let shared: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let log = Arc::new(Counted::default());
    let counting: Arc<dyn ObjectStore> = Arc::new(Counting {
        inner: Arc::clone(&shared),
        log: Arc::clone(&log),
    });
    // The app as an upgrade to 0.9 left it.
    shared
        .put(
            &"formats/default/default/floor".into(),
            floor::encode(NINE).into(),
        )
        .await
        .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let a = node_configured(
        "a",
        dir.path(),
        &counting,
        Duration::from_secs(30),
        &["t"],
        test_actor_wasm(),
        Duration::from_secs(30),
        "",
        |config| config.cluster.as_mut().unwrap().version = Some("0.10.0".into()),
    )
    .await;
    a.service.node_id_checked().await.unwrap();
    settle().await;
    log.take();

    // Before the finalize: as 0.9, plus one read of the upgrade floor.
    let r = send(a.addr, "POST", "/actor/t/before/durable", "").await;
    assert_eq!(r.status, 200, "{}", r.body);
    assert_eq!(
        log.take(),
        ["GET leases", "GET upgrade-floor", "PUT leases"],
        "an actor created before the finalize"
    );
    assert_eq!(
        body(&shared, &marker_key("before")).await.as_deref(),
        Some("wasmkeep-lease 2\n0\n"),
        "byte for byte 0.9's marker"
    );
    assert_eq!(body(&shared, &generation_floor_key()).await, None);

    // The finalize, while the node serves.
    floor::raise(
        &ObjectStoreBucket::new(Arc::clone(&shared), ""),
        "formats/default/default/floor",
        gf::SINCE,
    )
    .await
    .unwrap();
    let r = send(a.addr, "POST", "/actor/t/after/durable", "").await;
    assert_eq!(r.status, 200, "{}", r.body);
    assert_eq!(
        log.take(),
        ["GET generation-floor", "GET leases", "GET upgrade-floor"],
        "the first creation after the finalize reads both floors once"
    );
    assert_eq!(body(&shared, &marker_key("after")).await, None, "no marker");
    let r = send(a.addr, "POST", "/actor/t/later/durable", "").await;
    assert_eq!(r.status, 200, "{}", r.body);
    assert_eq!(
        log.take(),
        ["GET generation-floor", "GET leases"],
        "the upgrade floor is not read again"
    );

    // A marker left by a restore (no history, no record): the only
    // carrier of its actor's seed. The cleanup folds it, then deletes it.
    shared
        .put(
            &StorePath::from(marker_key("restored")),
            b"wasmkeep-lease 2\n41\n".to_vec().into(),
        )
        .await
        .unwrap();
    let bucket = ObjectStoreBucket::new(Arc::clone(&shared), "");
    let root = StateRoot::new("", "default", "default");
    let clock = Now;
    let plan = gf::plan_clean(&bucket, &root, &clock, &LeaseConfig::default_safe())
        .await
        .unwrap();
    // `before`'s marker is carried by its record; `restored`'s is folded.
    assert_eq!(plan.counts(), (1, 1));
    let done = gf::clean(&bucket, &root, &plan).await.unwrap();
    assert_eq!(
        done.floor,
        GenerationFloor {
            generation: 41,
            markers: Markers::None
        }
    );
    assert_eq!(body(&shared, &marker_key("restored")).await, None);
    assert_eq!(body(&shared, &marker_key("before")).await, None);
    log.take();

    // The first creation after the cleanup learns `markers none`; the
    // actor starts above the removed marker's number.
    let r = send(a.addr, "POST", "/actor/t/restored/durable", "").await;
    assert_eq!(r.status, 200, "{}", r.body);
    assert_eq!(log.take(), ["GET generation-floor", "GET leases"]);
    let (_, generation) = owner_record(&shared, "restored").await.unwrap();
    assert_eq!(generation, 42, "above the removed marker's 41");
    // From then on the generation floor's GET alone.
    let r = send(a.addr, "POST", "/actor/t/last/durable", "").await;
    assert_eq!(r.status, 200, "{}", r.body);
    assert_eq!(log.take(), ["GET generation-floor"]);
    let (_, generation) = owner_record(&shared, "last").await.unwrap();
    assert_eq!(generation, 42, "every first record after a restore floor");
}

/// ADR 0045, section 1: an unreadable generation floor fails the first
/// acquisition that needs it, and nothing is written; once rewritten, the
/// actor is acquired.
#[tokio::test(flavor = "multi_thread")]
async fn an_unreadable_generation_floor_fails_a_first_acquisition() {
    let shared: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    shared
        .put(
            &"formats/default/default/floor".into(),
            floor::encode(gf::SINCE).into(),
        )
        .await
        .unwrap();
    shared
        .put(
            &StorePath::from(generation_floor_key()),
            b"junk".to_vec().into(),
        )
        .await
        .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let a = node_configured(
        "a",
        dir.path(),
        &shared,
        Duration::from_secs(30),
        &["t"],
        test_actor_wasm(),
        Duration::from_secs(30),
        "",
        |config| config.cluster.as_mut().unwrap().version = Some("0.10.0".into()),
    )
    .await;
    a.service.node_id_checked().await.unwrap();
    settle().await;
    let r = send(a.addr, "POST", "/actor/t/x/durable", "").await;
    assert_ne!(r.status, 200, "{}", r.body);
    assert_eq!(owner_record(&shared, "x").await, None);
    assert_eq!(body(&shared, &marker_key("x")).await, None);
    shared
        .put(
            &StorePath::from(generation_floor_key()),
            GenerationFloor {
                generation: 5,
                markers: Markers::Kept,
            }
            .encode()
            .into(),
        )
        .await
        .unwrap();
    let r = send(a.addr, "POST", "/actor/t/x/durable", "").await;
    assert_eq!(r.status, 200, "{}", r.body);
    assert_eq!(owner_record(&shared, "x").await.unwrap().1, 6);
}
