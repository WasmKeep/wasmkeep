//! One reader and one lister for the bucket (`replica/bucket.rs`), under
//! the rules of each of their callers: a restore's reads and an upload's
//! read-back (`bounded_get`), the lease-free history reads
//! (`history::get`), a restore's listing and the listing that seeds a
//! first generation (`list_with` with an idle limit), a collection pass's
//! (without one), and the history listings (`history::list_objects`,
//! `history::list_objects_bounded`).
//!
//! Each test puts the same fault before both callers' rules and checks
//! what each makes of it: how many requests, how long (the clock is
//! paused, so times are exact), and which error.

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering::SeqCst;
use std::time::Duration;

use bytes::Bytes;
use futures_util::StreamExt;
use futures_util::stream::{self, BoxStream};
use object_store::memory::InMemory;
use object_store::path::Path as StorePath;
use object_store::{
    GetOptions, GetResult, GetResultPayload, ObjectMeta, ObjectStore, ObjectStoreExt,
};

use crate::replica::history::{self, Error};
use crate::replica::{
    READ_ATTEMPTS, READ_IDLE, READ_PAUSE, ReadTime, ReplicaError, bounded_get, list_with,
};

const MIB: usize = 1 << 20;

/// What the bucket makes of the next GET.
enum Get {
    /// Refused.
    Fail(object_store::Error),
    /// Never answered.
    Hang,
    /// Answered, with this body in place of the object's.
    Body(Vec<Piece>),
    /// Answered, declaring this size, with the object's own body.
    Declares(u64),
}

/// A step of a scripted body. A body whose steps run out ends there.
#[derive(Clone, Copy)]
enum Piece {
    /// The object's next bytes.
    Bytes(usize),
    /// Bytes the object does not have.
    Extra(usize),
    /// Nothing for this long.
    Wait(Duration),
    /// The connection breaks.
    Break,
    /// Nothing, ever.
    Hang,
}

/// What the bucket makes of the next LIST.
enum List {
    /// No key ever comes.
    Hang,
    /// Refused.
    Fail,
    /// This many keys, then nothing.
    HangAfter(usize),
    /// An object outside the prefix first, then the listing.
    Foreign(Box<ObjectMeta>),
}

/// An in-memory bucket whose next GETs and LISTs follow a script; past
/// the script, it answers as the bucket does.
#[derive(Default)]
struct Scripted {
    inner: InMemory,
    gets: Mutex<VecDeque<Get>>,
    lists: Mutex<VecDeque<List>>,
    /// The conditions of each GET received: `If-Match` and the version.
    asked: Mutex<Vec<(Option<String>, Option<String>)>>,
    listed: AtomicU32,
    /// With a node on the bucket: only requests whose key holds this are
    /// scripted and counted (an actor's history, not the node's own
    /// heartbeats and sweeps).
    only: Mutex<Option<&'static str>>,
}

impl Scripted {
    fn new() -> Arc<Self> {
        Arc::default()
    }

    fn store(self: &Arc<Self>) -> Arc<dyn ObjectStore> {
        Arc::clone(self) as Arc<dyn ObjectStore>
    }

    fn next_gets(&self, gets: impl IntoIterator<Item = Get>) {
        self.asked.lock().unwrap().clear();
        *self.gets.lock().unwrap() = gets.into_iter().collect();
    }

    fn next_lists(&self, lists: impl IntoIterator<Item = List>) {
        self.listed.store(0, SeqCst);
        *self.lists.lock().unwrap() = lists.into_iter().collect();
    }

    /// GETs received since the last script.
    fn gets(&self) -> usize {
        self.asked.lock().unwrap().len()
    }

    /// LISTs received since the last script.
    fn lists(&self) -> u32 {
        self.listed.load(SeqCst)
    }

    /// Whether a request for `key` is scripted and counted.
    fn scripted(&self, key: Option<&StorePath>) -> bool {
        let only = *self.only.lock().unwrap();
        only.is_none_or(|only| key.is_some_and(|key| key.as_ref().contains(only)))
    }
}

impl std::fmt::Debug for Scripted {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Scripted")
    }
}

impl std::fmt::Display for Scripted {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Scripted")
    }
}

fn reset() -> object_store::Error {
    object_store::Error::Generic {
        store: "Scripted",
        source: "connection reset".into(),
    }
}

fn not_found() -> object_store::Error {
    object_store::Error::NotFound {
        path: "gone".into(),
        source: "404".into(),
    }
}

fn denied() -> object_store::Error {
    object_store::Error::PermissionDenied {
        path: "kept".into(),
        source: "403".into(),
    }
}

fn precondition() -> object_store::Error {
    object_store::Error::Precondition {
        path: "changed".into(),
        source: "412".into(),
    }
}

#[async_trait::async_trait]
impl ObjectStore for Scripted {
    async fn put_opts(
        &self,
        location: &StorePath,
        payload: object_store::PutPayload,
        opts: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &StorePath,
        opts: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &StorePath,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        if !self.scripted(Some(location)) {
            return self.inner.get_opts(location, options).await;
        }
        self.asked
            .lock()
            .unwrap()
            .push((options.if_match.clone(), options.version.clone()));
        let next = self.gets.lock().unwrap().pop_front();
        let (pieces, declared) = match next {
            None => return self.inner.get_opts(location, options).await,
            Some(Get::Fail(e)) => return Err(e),
            Some(Get::Hang) => return std::future::pending().await,
            Some(Get::Body(pieces)) => (pieces, None),
            Some(Get::Declares(size)) => (vec![Piece::Bytes(usize::MAX)], Some(size)),
        };
        let got = self.inner.get_opts(location, options).await?;
        let mut meta = got.meta.clone();
        let (range, attributes, extensions) = (
            got.range.clone(),
            got.attributes.clone(),
            got.extensions.clone(),
        );
        let all = got.bytes().await?;
        if let Some(size) = declared {
            meta.size = size;
        }
        let body = stream::unfold(
            (pieces.into_iter(), all, 0usize),
            |(mut pieces, all, at)| async move {
                loop {
                    match pieces.next()? {
                        Piece::Bytes(n) => {
                            let end = at.saturating_add(n).min(all.len());
                            return Some((Ok(all.slice(at..end)), (pieces, all, end)));
                        }
                        Piece::Extra(n) => {
                            return Some((Ok(Bytes::from(vec![0u8; n])), (pieces, all, at)));
                        }
                        Piece::Wait(time) => tokio::time::sleep(time).await,
                        Piece::Break => return Some((Err(reset()), (pieces, all, at))),
                        Piece::Hang => std::future::pending::<()>().await,
                    }
                }
            },
        );
        Ok(GetResult {
            payload: GetResultPayload::Stream(body.boxed()),
            meta,
            range,
            attributes,
            extensions,
        })
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
        if !self.scripted(prefix) {
            return self.inner.list(prefix);
        }
        self.listed.fetch_add(1, SeqCst);
        let next = self.lists.lock().unwrap().pop_front();
        match next {
            None => self.inner.list(prefix),
            Some(List::Hang) => stream::pending().boxed(),
            Some(List::Fail) => stream::once(async { Err(reset()) }).boxed(),
            Some(List::HangAfter(n)) => self
                .inner
                .list(prefix)
                .take(n)
                .chain(stream::pending())
                .boxed(),
            Some(List::Foreign(meta)) => stream::once(async move { Ok(*meta) })
                .chain(self.inner.list(prefix))
                .boxed(),
        }
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&StorePath>,
    ) -> object_store::Result<object_store::ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &StorePath,
        to: &StorePath,
        options: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

/// A bucket with one object of `len` bytes at the returned location.
async fn one_object(len: usize) -> (Arc<Scripted>, Arc<dyn ObjectStore>, StorePath, Vec<u8>) {
    let scripted = Scripted::new();
    let at = StorePath::from("actors/t/c/a/wal/object");
    let body: Vec<u8> = (0..len).map(|i| (i % 251) as u8).collect();
    scripted.inner.put(&at, body.clone().into()).await.unwrap();
    let store = scripted.store();
    (scripted, store, at, body)
}

/// A read under a restore's rules, as a read-back makes it: the object's
/// listed size known, no place under the node's limits to give up.
async fn as_restore(
    store: &Arc<dyn ObjectStore>,
    at: &StorePath,
    listed: u64,
) -> Result<Bytes, ReplicaError> {
    bounded_get(
        store,
        at,
        listed,
        listed,
        Some(listed),
        &ReadTime::new(listed, None),
        None,
    )
    .await
}

/// A read under the history's rules, nothing listed: `cap` bounds it.
async fn as_history(
    store: &Arc<dyn ObjectStore>,
    at: &StorePath,
    cap: u64,
) -> Result<Bytes, Error> {
    history::get(store, at, None, cap).await
}

fn bucket_error<T: std::fmt::Debug>(result: Result<T, ReplicaError>) -> String {
    match result {
        Err(ReplicaError::Bucket(why)) => why,
        other => panic!("not a bucket error: {other:?}"),
    }
}

fn history_bucket_error<T: std::fmt::Debug>(result: Result<T, Error>) -> String {
    match result {
        Err(Error::Bucket(why)) => why,
        other => panic!("not a bucket error: {other:?}"),
    }
}

/// With no fault, both read the object whole, in one request; a restore's
/// is unconditional, whatever the listing knew.
#[tokio::test(start_paused = true)]
async fn both_read_an_object_in_one_request() {
    let (scripted, store, at, body) = one_object(3 * MIB).await;
    let size = body.len() as u64;
    let start = tokio::time::Instant::now();
    scripted.next_gets([]);
    assert_eq!(as_restore(&store, &at, size).await.unwrap(), body);
    assert_eq!(*scripted.asked.lock().unwrap(), [(None, None)]);
    scripted.next_gets([]);
    assert_eq!(as_history(&store, &at, size).await.unwrap(), body);
    assert_eq!(*scripted.asked.lock().unwrap(), [(None, None)]);
    assert_eq!(start.elapsed(), Duration::ZERO);
}

/// The history's read of a listed object is conditional on the listed
/// ETag; a restore's never is (its listing keeps sizes only).
#[tokio::test(start_paused = true)]
async fn only_a_history_read_is_conditional_on_the_listing() {
    let (scripted, store, at, body) = one_object(64).await;
    let listed = history::list_objects(&store, "actors/t/c/a/")
        .await
        .unwrap();
    let meta = &listed["wal/object"];
    assert!(meta.e_tag.is_some());
    scripted.next_gets([]);
    let read = history::get(&store, &at, Some(meta), 1 << 20).await;
    assert_eq!(read.unwrap(), body);
    assert_eq!(
        *scripted.asked.lock().unwrap(),
        [(meta.e_tag.clone(), meta.version.clone())]
    );
    // Replaced since the listing, by other bytes of the same length: the
    // history's read is stale; a restore's read takes what is there.
    store.put(&at, vec![9u8; 64].into()).await.unwrap();
    scripted.next_gets([]);
    let read = history::get(&store, &at, Some(meta), 1 << 20).await;
    assert!(matches!(read, Err(Error::Stale(_))), "{read:?}");
    scripted.next_gets([]);
    assert_eq!(as_restore(&store, &at, 64).await.unwrap(), vec![9u8; 64]);
    assert_eq!(*scripted.asked.lock().unwrap(), [(None, None)]);
}

/// An object that is gone ends both reads at once: a bucket error for a
/// restore (its next try lists again), the typed `Missing` the history's
/// callers plan again on.
#[tokio::test(start_paused = true)]
async fn a_missing_object_ends_both_reads_at_once() {
    let (scripted, store, at, _) = one_object(64).await;
    let start = tokio::time::Instant::now();
    scripted.next_gets([Get::Fail(not_found())]);
    let why = bucket_error(as_restore(&store, &at, 64).await);
    assert!(why.contains("gone"), "{why}");
    assert_eq!(scripted.gets(), 1);
    scripted.next_gets([Get::Fail(not_found())]);
    let read = as_history(&store, &at, 64).await;
    assert!(
        matches!(&read, Err(Error::Missing(path)) if *path == at.to_string()),
        "{read:?}"
    );
    assert_eq!(scripted.gets(), 1);
    assert_eq!(start.elapsed(), Duration::ZERO);
}

/// A failed precondition ends both too: a bucket error for a restore,
/// `Stale` for the history.
#[tokio::test(start_paused = true)]
async fn a_failed_precondition_ends_both_reads_at_once() {
    let (scripted, store, at, _) = one_object(64).await;
    scripted.next_gets([Get::Fail(precondition())]);
    bucket_error(as_restore(&store, &at, 64).await);
    assert_eq!(scripted.gets(), 1);
    scripted.next_gets([Get::Fail(precondition())]);
    let read = as_history(&store, &at, 64).await;
    assert!(matches!(read, Err(Error::Stale(_))), "{read:?}");
    assert_eq!(scripted.gets(), 1);
}

/// A refusal no resend changes (a 403) ends a restore's read at once; the
/// history sends it again, and reads the object if the bucket relents.
#[tokio::test(start_paused = true)]
async fn only_a_restore_stops_at_a_refusal_no_resend_changes() {
    let (scripted, store, at, body) = one_object(64).await;
    let start = tokio::time::Instant::now();
    scripted.next_gets([Get::Fail(denied())]);
    bucket_error(as_restore(&store, &at, 64).await);
    assert_eq!(scripted.gets(), 1);
    scripted.next_gets([Get::Fail(denied())]);
    assert_eq!(as_history(&store, &at, 64).await.unwrap(), body);
    assert_eq!(scripted.gets(), 2);
    scripted.next_gets((0..READ_ATTEMPTS).map(|_| Get::Fail(denied())));
    let why = history_bucket_error(as_history(&store, &at, 64).await);
    assert!(why.starts_with(&format!("{at}: ")), "{why}");
    assert_eq!(scripted.gets(), READ_ATTEMPTS as usize);
    assert_eq!(start.elapsed(), Duration::ZERO);
}

/// A refusal a resend may get past is sent again by both: a restore
/// pauses first, twice as long each time; the history does not pause.
/// Neither pauses after its last attempt.
#[tokio::test(start_paused = true)]
async fn only_a_restore_pauses_before_it_sends_a_refused_read_again() {
    let (scripted, store, at, body) = one_object(64).await;
    let resets = |n: u32| (0..n).map(|_| Get::Fail(reset()));

    let start = tokio::time::Instant::now();
    scripted.next_gets(resets(1));
    assert_eq!(as_restore(&store, &at, 64).await.unwrap(), body);
    assert_eq!((scripted.gets(), start.elapsed()), (2, READ_PAUSE));

    let start = tokio::time::Instant::now();
    scripted.next_gets(resets(2));
    assert_eq!(as_restore(&store, &at, 64).await.unwrap(), body);
    assert_eq!((scripted.gets(), start.elapsed()), (3, READ_PAUSE * 3));

    let start = tokio::time::Instant::now();
    scripted.next_gets(resets(READ_ATTEMPTS));
    let why = bucket_error(as_restore(&store, &at, 64).await);
    assert!(
        why.starts_with(&format!("{at}: ")) && why.ends_with(", 3 times"),
        "{why}"
    );
    assert!(why.contains("connection reset"), "{why}");
    assert_eq!((scripted.gets(), start.elapsed()), (3, READ_PAUSE * 3));

    let start = tokio::time::Instant::now();
    scripted.next_gets(resets(2));
    assert_eq!(as_history(&store, &at, 64).await.unwrap(), body);
    assert_eq!((scripted.gets(), start.elapsed()), (3, Duration::ZERO));

    scripted.next_gets(resets(READ_ATTEMPTS));
    let why = history_bucket_error(as_history(&store, &at, 64).await);
    assert!(
        why.starts_with(&format!("{at}: ")) && why.contains("connection reset"),
        "{why}"
    );
    assert_eq!((scripted.gets(), start.elapsed()), (3, Duration::ZERO));
}

/// A body that breaks half-way is a refusal like another: all of the
/// object is read again, after a pause for a restore only.
#[tokio::test(start_paused = true)]
async fn a_body_that_breaks_is_read_again_from_its_start() {
    let (scripted, store, at, body) = one_object(64).await;
    let cut = || Get::Body(vec![Piece::Bytes(32), Piece::Break]);
    let start = tokio::time::Instant::now();
    scripted.next_gets([cut()]);
    assert_eq!(as_restore(&store, &at, 64).await.unwrap(), body);
    assert_eq!((scripted.gets(), start.elapsed()), (2, READ_PAUSE));
    let start = tokio::time::Instant::now();
    scripted.next_gets([cut()]);
    assert_eq!(as_history(&store, &at, 64).await.unwrap(), body);
    assert_eq!((scripted.gets(), start.elapsed()), (2, Duration::ZERO));
}

/// A request the bucket does not answer is given up after the idle
/// limit by both, and sent again without a pause.
#[tokio::test(start_paused = true)]
async fn an_unanswered_read_is_given_up_after_the_idle_limit() {
    let (scripted, store, at, body) = one_object(64).await;
    let hangs = |n: u32| (0..n).map(|_| Get::Hang);

    let start = tokio::time::Instant::now();
    scripted.next_gets(hangs(1));
    assert_eq!(as_restore(&store, &at, 64).await.unwrap(), body);
    assert_eq!((scripted.gets(), start.elapsed()), (2, READ_IDLE));

    let start = tokio::time::Instant::now();
    scripted.next_gets(hangs(READ_ATTEMPTS));
    let why = bucket_error(as_restore(&store, &at, 64).await);
    assert_eq!(
        why,
        format!("{at}: no answer within {READ_IDLE:?}, 3 times")
    );
    assert_eq!((scripted.gets(), start.elapsed()), (3, READ_IDLE * 3));

    let start = tokio::time::Instant::now();
    scripted.next_gets(hangs(1));
    assert_eq!(as_history(&store, &at, 64).await.unwrap(), body);
    assert_eq!((scripted.gets(), start.elapsed()), (2, READ_IDLE));

    let start = tokio::time::Instant::now();
    scripted.next_gets(hangs(READ_ATTEMPTS));
    let why = history_bucket_error(as_history(&store, &at, 64).await);
    assert_eq!(why, format!("{at}: object headers stalled"));
    assert_eq!((scripted.gets(), start.elapsed()), (3, READ_IDLE * 3));
}

/// A body that goes silent is given up after the idle limit, short of the
/// attempt's whole time (the idle limit and a second per megabyte).
#[tokio::test(start_paused = true)]
async fn a_silent_body_is_given_up_after_the_idle_limit() {
    let (scripted, store, at, _) = one_object(2 * MIB).await;
    let size = 2 * MIB as u64;
    let stalls = || (0..READ_ATTEMPTS).map(|_| Get::Body(vec![Piece::Bytes(MIB), Piece::Hang]));

    let start = tokio::time::Instant::now();
    scripted.next_gets(stalls());
    let why = bucket_error(as_restore(&store, &at, size).await);
    assert_eq!(
        why,
        format!("{at}: body stalled after {MIB} of {size} bytes, 3 times")
    );
    assert_eq!((scripted.gets(), start.elapsed()), (3, READ_IDLE * 3));

    let start = tokio::time::Instant::now();
    scripted.next_gets(stalls());
    let why = history_bucket_error(as_history(&store, &at, size).await);
    assert_eq!(why, format!("{at}: object body stalled"));
    assert_eq!((scripted.gets(), start.elapsed()), (3, READ_IDLE * 3));
}

/// A body that keeps coming, but too slowly, is given up at the attempt's
/// whole time: the idle limit and a second per started megabyte, of the
/// bytes read alongside for a restore, of the listed size (or the cap,
/// with nothing listed) for the history.
#[tokio::test(start_paused = true)]
async fn a_slow_body_is_given_up_at_the_attempts_whole_time() {
    let (scripted, store, at, _) = one_object(2 * MIB).await;
    let size = 2 * MIB as u64;
    let whole = READ_IDLE + Duration::from_secs(2);
    let drip = || {
        (0..READ_ATTEMPTS).map(|_| {
            let mut pieces = Vec::new();
            for _ in 0..8 {
                pieces.push(Piece::Bytes(1));
                pieces.push(Piece::Wait(Duration::from_secs(4)));
            }
            Get::Body(pieces)
        })
    };

    let start = tokio::time::Instant::now();
    scripted.next_gets(drip());
    let why = bucket_error(as_restore(&store, &at, size).await);
    assert_eq!(
        why,
        format!("{at}: read too slowly (2 of {size} bytes in {whole:?}), 3 times")
    );
    assert_eq!((scripted.gets(), start.elapsed()), (3, whole * 3));

    let start = tokio::time::Instant::now();
    scripted.next_gets(drip());
    let why = history_bucket_error(as_history(&store, &at, size).await);
    assert_eq!(why, format!("{at}: object read deadline exceeded"));
    assert_eq!((scripted.gets(), start.elapsed()), (3, whole * 3));

    // With nothing listed, the history's whole time counts the cap.
    let start = tokio::time::Instant::now();
    scripted.next_gets(drip());
    let read = as_history(&store, &at, 8 * size).await;
    assert!(matches!(read, Err(Error::Bucket(_))), "{read:?}");
    assert_eq!(
        start.elapsed(),
        (READ_IDLE + Duration::from_secs(16)) * 3,
        "the cap, not the object, with nothing listed"
    );
}

/// Where the idle limit and the attempt's whole time end together (an
/// empty object), each keeps its own word for it.
#[tokio::test(start_paused = true)]
async fn the_two_limits_ending_together_keep_each_callers_wording() {
    let (scripted, store, at, _) = one_object(0).await;
    let stalls = || (0..READ_ATTEMPTS).map(|_| Get::Body(vec![Piece::Hang]));
    scripted.next_gets(stalls());
    let why = bucket_error(as_restore(&store, &at, 0).await);
    assert_eq!(
        why,
        format!("{at}: read too slowly (0 of 0 bytes in {READ_IDLE:?}), 3 times")
    );
    scripted.next_gets(stalls());
    let why = history_bucket_error(as_history(&store, &at, 0).await);
    assert_eq!(why, format!("{at}: object body stalled"));
}

/// An object of another size than the listing said, or larger than the
/// caller allows, is refused before its body is read, and not sent again.
#[tokio::test(start_paused = true)]
async fn sizes_are_checked_before_the_body_is_read() {
    let (scripted, store, at, _) = one_object(64).await;
    let unread = || Get::Body(vec![Piece::Hang]);

    scripted.next_gets([unread()]);
    let read = as_restore(&store, &at, 63).await;
    assert!(
        matches!(&read, Err(ReplicaError::Restore(why)) if why.ends_with("64 bytes, where the listing said 63")),
        "{read:?}"
    );
    assert_eq!(scripted.gets(), 1);

    scripted.next_gets([unread()]);
    let time = ReadTime::new(64, None);
    let read = bounded_get(&store, &at, 63, 60, None, &time, None).await;
    assert!(
        matches!(&read, Err(ReplicaError::Restore(why)) if why.contains("more than a database under the 60-byte cap can need")),
        "{read:?}"
    );
    assert_eq!(scripted.gets(), 1);

    scripted.next_gets([unread()]);
    let read = as_history(&store, &at, 63).await;
    assert!(
        matches!(&read, Err(Error::Corrupt(why)) if why.ends_with("object exceeds 63 byte cap")),
        "{read:?}"
    );
    assert_eq!(scripted.gets(), 1);

    // Listed with another size: stale for the history.
    let listed = history::list_objects(&store, "actors/t/c/a/")
        .await
        .unwrap();
    let mut meta = listed["wal/object"].clone();
    meta.e_tag = None;
    meta.size = 63;
    scripted.next_gets([unread()]);
    let read = history::get(&store, &at, Some(&meta), 1 << 20).await;
    assert!(matches!(read, Err(Error::Stale(_))), "{read:?}");
    assert_eq!(scripted.gets(), 1);
}

/// A body longer than the size its answer declared is refused by both
/// as it passes that size: no more of it is read, and it is not sent
/// again.
#[tokio::test(start_paused = true)]
async fn a_body_longer_than_it_declared_is_refused() {
    let (scripted, store, at, _) = one_object(64).await;
    let long = || Get::Body(vec![Piece::Bytes(64), Piece::Extra(1), Piece::Hang]);

    scripted.next_gets([long()]);
    let read = as_history(&store, &at, 64).await;
    assert!(
        matches!(&read, Err(Error::Corrupt(why)) if why.ends_with("body exceeds declared size")),
        "{read:?}"
    );
    assert_eq!(scripted.gets(), 1);

    scripted.next_gets([long()]);
    let read = as_restore(&store, &at, 64).await;
    assert!(
        matches!(&read, Err(ReplicaError::Restore(why)) if why.ends_with("a body longer than the 64 bytes it declared")),
        "{read:?}"
    );
    assert_eq!(scripted.gets(), 1);
}

/// A body that ends short of the size its answer declared is a failed
/// attempt for both, sent again without a pause.
#[tokio::test(start_paused = true)]
async fn a_body_shorter_than_it_declared_is_read_again() {
    let (scripted, store, at, body) = one_object(64).await;
    let short = |n: u32| (0..n).map(|_| Get::Body(vec![Piece::Bytes(40)]));
    let start = tokio::time::Instant::now();

    scripted.next_gets(short(1));
    assert_eq!(as_history(&store, &at, 64).await.unwrap(), body);
    assert_eq!(scripted.gets(), 2);
    scripted.next_gets(short(READ_ATTEMPTS));
    let why = history_bucket_error(as_history(&store, &at, 64).await);
    assert_eq!(why, format!("{at}: {at}: truncated object body"));

    scripted.next_gets(short(1));
    assert_eq!(as_restore(&store, &at, 64).await.unwrap(), body);
    assert_eq!(scripted.gets(), 2);
    scripted.next_gets(short(READ_ATTEMPTS));
    let why = bucket_error(as_restore(&store, &at, 64).await);
    assert_eq!(
        why,
        format!("{at}: body ended after 40 of 64 bytes, 3 times")
    );
    assert_eq!(start.elapsed(), Duration::ZERO);

    // An answer that declares less than the listing said is still
    // refused by its size, before its body.
    scripted.next_gets([Get::Declares(40)]);
    let read = as_restore(&store, &at, 64).await;
    assert!(matches!(read, Err(ReplicaError::Restore(_))), "{read:?}");
}

/// A bucket with three keys under a prefix that needs escaping, and one
/// under another prefix.
async fn listed_bucket() -> (Arc<Scripted>, Arc<dyn ObjectStore>, &'static str) {
    let scripted = Scripted::new();
    let prefix = "actors/t/c/a%2Fb/";
    for key in ["wal/1", "wal/2", "snapshots/1"] {
        let at = StorePath::from(format!("{prefix}{key}"));
        scripted.inner.put(&at, vec![1u8; 3].into()).await.unwrap();
    }
    let other = StorePath::from("actors/t/c/other/wal/1");
    scripted
        .inner
        .put(&other, vec![1u8; 3].into())
        .await
        .unwrap();
    let store = scripted.store();
    (scripted, store, prefix)
}

fn sorted(mut keys: Vec<String>) -> Vec<String> {
    keys.sort();
    keys
}

const KEYS: [&str; 3] = ["snapshots/1", "wal/1", "wal/2"];

/// Every caller's listing gives the keys under the prefix, relative to
/// it, compared in the store's encoding; the history's keeps each
/// object's listed identity.
#[tokio::test(start_paused = true)]
async fn every_listing_gives_the_keys_under_the_prefix() {
    let (scripted, store, prefix) = listed_bucket().await;
    scripted.next_lists([]);
    assert_eq!(sorted(list_with(&store, prefix, None).await.unwrap()), KEYS);
    let idle = Some(READ_IDLE);
    assert_eq!(sorted(list_with(&store, prefix, idle).await.unwrap()), KEYS);
    let objects = history::list_objects(&store, prefix).await.unwrap();
    assert_eq!(objects.keys().collect::<Vec<_>>(), KEYS);
    for (key, meta) in &objects {
        assert_eq!(meta.location, StorePath::from(format!("{prefix}{key}")));
        assert!(meta.e_tag.is_some() && meta.size == 3);
    }
    let bounded = history::list_objects_bounded(&store, prefix, 3).await;
    assert_eq!(bounded.unwrap().len(), 3);
    assert_eq!(scripted.lists(), 4, "one request each");
    // The history lists a whole bucket with an empty prefix; a restore's
    // and a collection pass's lister, nothing.
    assert!(list_with(&store, "", None).await.unwrap().is_empty());
    assert!(list_with(&store, "/", idle).await.unwrap().is_empty());
    let all = history::list_objects(&store, "").await.unwrap();
    assert_eq!(all.len(), 4);
    assert!(all.contains_key("actors/t/c/other/wal/1"));
}

/// A listing with an idle limit that goes silent is given up and made
/// again, from its start, up to three times: nothing of a listing given
/// up is kept.
#[tokio::test(start_paused = true)]
async fn a_silent_listing_is_made_again_from_its_start() {
    let (scripted, store, prefix) = listed_bucket().await;
    let idle = Some(READ_IDLE);

    let start = tokio::time::Instant::now();
    scripted.next_lists([List::Hang, List::HangAfter(2)]);
    assert_eq!(sorted(list_with(&store, prefix, idle).await.unwrap()), KEYS);
    assert_eq!((scripted.lists(), start.elapsed()), (3, READ_IDLE * 2));

    let start = tokio::time::Instant::now();
    scripted.next_lists([List::Hang, List::HangAfter(2)]);
    let objects = history::list_objects(&store, prefix).await.unwrap();
    assert_eq!(objects.keys().collect::<Vec<_>>(), KEYS);
    assert_eq!((scripted.lists(), start.elapsed()), (3, READ_IDLE * 2));

    let start = tokio::time::Instant::now();
    scripted.next_lists((0..READ_ATTEMPTS).map(|_| List::Hang));
    let why = bucket_error(list_with(&store, prefix, idle).await);
    assert_eq!(
        why,
        format!("{prefix}: the listing stalled ({READ_IDLE:?} without a key), 3 times")
    );
    assert_eq!((scripted.lists(), start.elapsed()), (3, READ_IDLE * 3));

    let start = tokio::time::Instant::now();
    scripted.next_lists((0..READ_ATTEMPTS).map(|_| List::Hang));
    let why = history_bucket_error(history::list_objects(&store, prefix).await);
    assert_eq!(why, "history listing kept stalling");
    assert_eq!((scripted.lists(), start.elapsed()), (3, READ_IDLE * 3));
}

/// A listing without an idle limit (a collection pass's) waits as long as
/// the bucket takes.
#[tokio::test(start_paused = true)]
async fn a_listing_without_an_idle_limit_waits() {
    let (scripted, store, prefix) = listed_bucket().await;
    scripted.next_lists([List::Hang]);
    let waited = tokio::time::timeout(READ_IDLE * 100, list_with(&store, prefix, None)).await;
    assert!(waited.is_err(), "{waited:?}");
    assert_eq!(scripted.lists(), 1);
}

/// A listing the bucket refuses ends at once for every caller: it is not
/// made again.
#[tokio::test(start_paused = true)]
async fn a_refused_listing_is_not_made_again() {
    let (scripted, store, prefix) = listed_bucket().await;
    let start = tokio::time::Instant::now();
    for idle in [None, Some(READ_IDLE)] {
        scripted.next_lists([List::Fail]);
        let why = bucket_error(list_with(&store, prefix, idle).await);
        assert!(why.contains("connection reset"), "{why}");
        assert_eq!(scripted.lists(), 1);
    }
    scripted.next_lists([List::Fail]);
    let why = history_bucket_error(history::list_objects(&store, prefix).await);
    assert!(why.contains("connection reset"), "{why}");
    assert_eq!(scripted.lists(), 1);
    assert_eq!(start.elapsed(), Duration::ZERO);
}

/// A bounded listing refuses more objects than its bound instead of
/// returning a part, and counts every object the bucket lists, before
/// the prefix is compared. No listing returns an object outside its
/// prefix.
#[tokio::test(start_paused = true)]
async fn a_bounded_listing_counts_what_the_bucket_lists() {
    let (scripted, store, prefix) = listed_bucket().await;
    let read = history::list_objects_bounded(&store, prefix, 2).await;
    assert_eq!(history_bucket_error(read), "listing exceeds 2 object limit");

    // An object the bucket lists though it is not under the prefix.
    let mut elsewhere = history::list_objects(&store, prefix).await.unwrap()["wal/1"].clone();
    elsewhere.location = StorePath::from("elsewhere/wal/9");
    let foreign = || List::Foreign(Box::new(elsewhere.clone()));
    scripted.next_lists([foreign()]);
    assert_eq!(sorted(list_with(&store, prefix, None).await.unwrap()), KEYS);
    scripted.next_lists([foreign()]);
    let objects = history::list_objects(&store, prefix).await.unwrap();
    assert_eq!(objects.keys().collect::<Vec<_>>(), KEYS);
    scripted.next_lists([foreign()]);
    let read = history::list_objects_bounded(&store, prefix, 3).await;
    assert_eq!(history_bucket_error(read), "listing exceeds 3 object limit");
}

/// Node `a` on `store`, with 100 ms heartbeats.
fn node(store: &Arc<dyn ObjectStore>) -> Arc<crate::cluster::Cluster> {
    use wasmkeep_cluster::lease::LeaseConfig;
    let mut config =
        crate::cluster::ClusterConfig::new("a", "http://a-one-reader", Arc::clone(store));
    config.heartbeat = Duration::from_millis(100);
    config.lease = LeaseConfig::new(1_000, 200, 200, 100).unwrap();
    let root = wasmkeep_cluster::state_root::StateRoot::new("", "t", "default");
    crate::cluster::Cluster::new(&config, &root).unwrap()
}

/// The listing that seeds an actor's first generation
/// (`highest_generation_in_bucket`) is a restore's: given up when the
/// bucket is silent for the idle limit, made again, and refused after
/// three such attempts.
#[tokio::test(start_paused = true)]
async fn the_listing_that_seeds_a_first_generation_is_made_again_when_silent() {
    use wasmkeep_cluster::replication::{SegmentId, snapshot_key};
    let scripted = Scripted::new();
    *scripted.only.lock().unwrap() = Some("/actors/c/x");
    let store = scripted.store();
    let node = node(&store);
    let x = wasmkeep_runtime::ActorAddress::new("t", "c", "x").unwrap();
    let prefix = node.history_key(&x);
    let id = SegmentId {
        generation: 7,
        txid: 3,
    };
    let at = StorePath::from(format!("{prefix}/{}", snapshot_key(id)));
    store.put(&at, vec![0u8; 8].into()).await.unwrap();

    let start = tokio::time::Instant::now();
    scripted.next_lists([List::Hang]);
    let highest = crate::replica::highest_generation_in_bucket(&node, &x).await;
    assert_eq!(highest.unwrap(), 7);
    assert_eq!((scripted.lists(), start.elapsed()), (2, READ_IDLE));

    scripted.next_lists((0..READ_ATTEMPTS).map(|_| List::Hang));
    let why = bucket_error(crate::replica::highest_generation_in_bucket(&node, &x).await);
    assert!(why.contains("the listing stalled"), "{why}");
    assert_eq!(scripted.lists(), 3);
}

/// A whole restore through the one lister and the one reader, on a real
/// hold: its listing goes silent once, one of its reads is refused once
/// and one's body ends short once. It lists again, reads both objects
/// again, and verifies the same head as the replica that wrote them.
#[tokio::test]
async fn a_restore_gets_past_a_silent_listing_a_refused_read_and_a_short_body() {
    use crate::cluster::LeaseHold;
    use crate::replica::{Replica, SnapshotKind};
    use wasmkeep_storage::{Storage, StorageConfig};
    let scripted = Scripted::new();
    *scripted.only.lock().unwrap() = Some("/actors/c/x");
    let store = scripted.store();
    let node = node(&store);
    node.identity().await.unwrap();
    let started = std::time::Instant::now();
    while !node.node_lease_valid() {
        assert!(started.elapsed() < Duration::from_secs(3), "never valid");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let x = wasmkeep_runtime::ActorAddress::new("t", "c", "x").unwrap();
    let storage_in = |dir: &std::path::Path| {
        let root = wasmkeep_cluster::state_root::StateRoot::new("", "t", "default");
        Storage::new(StorageConfig::new(dir), crate::state_dir(&root))
    };
    let first = tempfile::tempdir().unwrap();
    let storage = storage_in(first.path());
    let hold = LeaseHold::acquire(&node, &x).await.unwrap();
    let generation = hold.generation().unwrap();
    let path = storage.db_path("c", "x");
    let mut replica = Replica::restore(&node, &x, hold.view(), generation, path, 1 << 30)
        .await
        .unwrap();
    let mut db = storage.open("c", "x").unwrap();
    db.exec("CREATE TABLE t(v)", &[]).unwrap();
    db.start_capture().unwrap();
    let all = db.snapshot().unwrap();
    assert!(
        replica
            .ship_snapshot(&all, SnapshotKind::First)
            .await
            .unwrap()
    );
    db.exec("INSERT INTO t VALUES (1)", &[]).unwrap();
    let changes = db.take_changes().unwrap().unwrap();
    replica.ship(&changes).await.unwrap();
    let head = replica.verified_head();
    drop((db, replica));

    // Into an empty directory: the snapshot and the segment are read.
    let second = tempfile::tempdir().unwrap();
    let path = storage_in(second.path()).db_path("c", "x");
    scripted.next_lists([List::Hang]);
    scripted.next_gets([Get::Fail(reset()), Get::Body(vec![Piece::Bytes(8)])]);
    let restored = Replica::restore(&node, &x, hold.view(), generation, path, 1 << 30)
        .await
        .unwrap();
    assert_eq!(restored.verified_head(), head);
    assert_eq!(scripted.lists(), 2, "listed again after the silent one");
    let asked = scripted.asked.lock().unwrap().clone();
    assert_eq!(asked.len(), 4, "two objects, each read twice");
    assert!(
        asked.iter().all(|conditions| *conditions == (None, None)),
        "a restore's reads are unconditional: {asked:?}"
    );
}
