//! A simulated S3-like bucket on its own madsim node, and the client nodes
//! use to reach it over the simulated network. Shared by the simulations
//! with `#[path]`.
//!
//! Conditional create and replace behave like S3's `If-None-Match: *` and
//! `If-Match`. Requests and replies cross the simulated network, so they
//! can be delayed, held and delivered late, or lost, like the real thing.

#![allow(dead_code)] // each simulation uses part of it

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use madsim::net::Endpoint;
use madsim::time::timeout;
use wasmkeep_cluster::lease::{Bucket, BucketError, Version};

/// The bucket's address.
pub(crate) const BUCKET: &str = "10.0.0.1:9000";
const REQUEST_TAG: u64 = 1;

/// The bucket's contents: key -> (body, version). Ordered, so listings are.
pub(crate) type Store = Arc<Mutex<BTreeMap<String, (Vec<u8>, u64)>>>;

/// Message: `tag\ncmd\nkey\nversion\n` then the body. Reply: `OK\nversion\n`
/// then the body, `NONE\n` or `CONFLICT\n`. `LIST` replies `OK\n0\n` then
/// one `key\tbody\n` line per object under the prefix.
fn encode(tag: u64, cmd: &str, key: &str, version: &str, body: &[u8]) -> Vec<u8> {
    let mut m = format!("{tag}\n{cmd}\n{key}\n{version}\n").into_bytes();
    m.extend_from_slice(body);
    m
}

fn split_lines(buf: &[u8], n: usize) -> (Vec<String>, Vec<u8>) {
    let mut lines = Vec::new();
    let mut rest = buf;
    for _ in 0..n {
        let i = rest.iter().position(|&b| b == b'\n').unwrap_or(rest.len());
        lines.push(String::from_utf8_lossy(&rest[..i]).to_string());
        rest = rest.get(i + 1..).unwrap_or(&[]);
    }
    (lines, rest.to_vec())
}

/// How the simulated bucket misbehaves.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct Faults {
    /// One request in `slow_one_in` is applied only after a random delay of
    /// 300..2_500 ms (0: never), like a slow S3 PUT that still succeeds.
    pub(crate) slow_one_in: u64,
    /// One request in `late_reply_one_in` is applied at once and answered
    /// only after 300..1_300 ms (0: never), past the client's timeout: a
    /// write that landed with its reply lost.
    pub(crate) late_reply_one_in: u64,
}

thread_local! {
    /// Keys whose next request the bucket applies late, by how much (ms):
    /// a fault a simulation aims at one request (a simulation runs on one
    /// thread, the bucket's node included). See [`delay_next`].
    static DELAYED: std::cell::RefCell<Vec<(String, u64)>> =
        const { std::cell::RefCell::new(Vec::new()) };
}

thread_local! {
    /// When each key was last written, by the simulation's clock (ms since
    /// [`reset_modified`]): what a listing's last-modified time says.
    static MODIFIED: std::cell::RefCell<(Option<madsim::time::Instant>, BTreeMap<String, u64>)> =
        const { std::cell::RefCell::new((None, BTreeMap::new())) };
}

/// Starts the last-modified clock at 0 and forgets every time recorded: a
/// simulation that lists with times holds the returned guard for its run.
#[allow(dead_code, reason = "not every simulation lists with times")]
pub(crate) fn reset_modified() -> ModifiedGuard {
    MODIFIED.with(|m| *m.borrow_mut() = (Some(madsim::time::Instant::now()), BTreeMap::new()));
    ModifiedGuard
}

/// Timestamp history belongs to one seed, not the test thread. Simulations
/// that do not list timestamps never enable it or retain their object keys.
#[must_use]
pub(crate) struct ModifiedGuard;

impl Drop for ModifiedGuard {
    fn drop(&mut self) {
        MODIFIED.with(|m| *m.borrow_mut() = (None, BTreeMap::new()));
    }
}

fn note_modified(key: &str) {
    MODIFIED.with(|m| {
        let mut m = m.borrow_mut();
        let Some(start) = m.0 else {
            return;
        };
        let at = u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX);
        m.1.insert(key.to_owned(), at);
    });
}

fn modified(key: &str) -> u64 {
    MODIFIED.with(|m| m.borrow().1.get(key).copied().unwrap_or(0))
}

/// The bucket applies the next request for `key` only `ms` after it
/// arrives (once).
#[allow(dead_code, reason = "not every simulation aims a delay")]
pub(crate) fn delay_next(key: &str, ms: u64) {
    DELAYED.with(|d| d.borrow_mut().push((key.to_owned(), ms)));
}

thread_local! {
    /// Requests (command, key) whose next one the bucket applies at once
    /// and answers only that many ms later: a fault a simulation aims at
    /// one request. See [`late_reply_next`].
    static LATE_REPLIES: std::cell::RefCell<Vec<(String, String, u64)>> =
        const { std::cell::RefCell::new(Vec::new()) };
}

/// The bucket applies the next `cmd` request for `key` (`REPLACE`,
/// `CREATE`, ...) at once, and answers it only `ms` later (once): a write
/// that landed while its sender still waits for the answer.
#[allow(dead_code, reason = "not every simulation aims a late reply")]
pub(crate) fn late_reply_next(cmd: &str, key: &str, ms: u64) {
    LATE_REPLIES.with(|d| d.borrow_mut().push((cmd.to_owned(), key.to_owned(), ms)));
}

/// Forgets every aimed delay and late reply not taken yet: a simulation
/// calls it first, so one whose request was never sent cannot reach the
/// next seed.
#[allow(dead_code, reason = "not every simulation aims a delay")]
pub(crate) fn clear_delays() {
    DELAYED.with(|d| d.borrow_mut().clear());
    LATE_REPLIES.with(|d| d.borrow_mut().clear());
}

/// An aimed late reply for this request's command and key, taken.
fn aimed_late_reply(request: &[u8]) -> Option<u64> {
    let (h, _) = split_lines(request, 4);
    let (cmd, key) = (h.get(1)?, h.get(2)?);
    LATE_REPLIES.with(|d| {
        let mut d = d.borrow_mut();
        let i = d.iter().position(|(c, k, _)| c == cmd && k == key)?;
        Some(d.remove(i).2)
    })
}

/// An aimed delay for this request's key, taken.
fn aimed_delay(request: &[u8]) -> Option<u64> {
    let (h, _) = split_lines(request, 4);
    let key = h.get(2)?;
    DELAYED.with(|d| {
        let mut d = d.borrow_mut();
        let i = d.iter().position(|(k, _)| k == key)?;
        Some(d.remove(i).1)
    })
}

/// Serves the bucket forever, keeping its objects in `store`.
pub(crate) async fn bucket_node(store: Store) {
    bucket_node_with(store, Faults::default()).await;
}

/// [`bucket_node`] with faults.
pub(crate) async fn bucket_node_with(store: Store, faults: Faults) {
    let ep = Arc::new(Endpoint::bind(BUCKET).await.unwrap());
    let next = Arc::new(AtomicU64::new(0));
    let mut buf = vec![0; 1 << 16];
    loop {
        let Ok((n, from)) = ep.recv_from(REQUEST_TAG, &mut buf).await else {
            continue;
        };
        let request = buf[..n].to_vec();
        let delay = aimed_delay(&request).or_else(|| {
            (faults.slow_one_in > 0
                && madsim::rand::random::<u64>().is_multiple_of(faults.slow_one_in))
            .then(|| 300 + madsim::rand::random::<u64>() % 2_200)
        });
        let random_late = (faults.late_reply_one_in > 0
            && madsim::rand::random::<u64>().is_multiple_of(faults.late_reply_one_in))
        .then(|| 300 + madsim::rand::random::<u64>() % 1_000);
        // After the random draws, so a simulation that aims none keeps its
        // fault schedule.
        let late = aimed_late_reply(&request).or(random_late);
        let (ep, store, next) = (Arc::clone(&ep), Arc::clone(&store), Arc::clone(&next));
        let serve = async move {
            if let Some(ms) = delay {
                madsim::time::sleep(Duration::from_millis(ms)).await;
            }
            let (tag, reply) = apply(&store, &next, &request);
            if let Some(ms) = late {
                madsim::time::sleep(Duration::from_millis(ms)).await;
            }
            let _ = ep.send_to(from, tag, &reply).await;
        };
        if delay.is_some() || late.is_some() {
            madsim::task::spawn(serve);
        } else {
            serve.await;
        }
    }
}

/// Applies one request to the store; returns the reply tag and the reply.
fn apply(store: &Store, next: &AtomicU64, request: &[u8]) -> (u64, Vec<u8>) {
    let (h, body) = split_lines(request, 4);
    let (tag, cmd, key, version) = (h[0].parse::<u64>().unwrap(), &h[1], &h[2], &h[3]);
    let mut store = store.lock().unwrap();
    let current = store.get(key.as_str()).map(|(_, v)| v.to_string());
    let bump = || next.fetch_add(1, Ordering::Relaxed) + 1;
    let reply = match cmd.as_str() {
        "GET" => match store.get(key.as_str()) {
            Some((b, v)) => {
                let mut r = format!("OK\n{v}\n").into_bytes();
                r.extend_from_slice(b);
                r
            }
            None => b"NONE\n".to_vec(),
        },
        "LIST" => {
            let mut r = b"OK\n0\n".to_vec();
            for (k, (b, _)) in store.range(key.clone()..) {
                if !k.starts_with(key.as_str()) {
                    break;
                }
                r.extend_from_slice(k.as_bytes());
                r.push(b'\t');
                r.extend_from_slice(b);
                r.push(b'\n');
            }
            r
        }
        // Keys, versions and last-modified times, no bodies: what an S3
        // listing gives (its ETag and LastModified).
        "LISTV" => {
            let mut r = b"OK\n0\n".to_vec();
            for (k, (_, v)) in store.range(key.clone()..) {
                if !k.starts_with(key.as_str()) {
                    break;
                }
                r.extend_from_slice(format!("{k}\t{v}\t{}\n", modified(k)).as_bytes());
            }
            r
        }
        // Removes exactly this key; an absent key is fine (as on S3).
        "DELETE" => {
            store.remove(key.as_str());
            b"OK\n0\n".to_vec()
        }
        // Unconditional overwrite: only a deliberately broken variant uses
        // it (a fence written without its condition).
        "PUT" => {
            note_modified(key);
            let v = bump();
            store.insert(key.clone(), (body, v));
            format!("OK\n{v}\n").into_bytes()
        }
        "CREATE" if current.is_none() => {
            note_modified(key);
            let v = bump();
            store.insert(key.clone(), (body, v));
            format!("OK\n{v}\n").into_bytes()
        }
        "REPLACE" if current.as_deref() == Some(version.as_str()) => {
            note_modified(key);
            let v = bump();
            store.insert(key.clone(), (body, v));
            format!("OK\n{v}\n").into_bytes()
        }
        _ => b"CONFLICT\n".to_vec(),
    };
    (tag, reply)
}

/// A node's connection to the bucket. A request without a reply within the
/// timeout is `Unavailable`, though it may still be applied later.
pub(crate) struct EpBucket {
    ep: Endpoint,
    next_tag: AtomicU64,
    timeout: Duration,
}

impl EpBucket {
    /// Binds a client endpoint on the current node.
    pub(crate) async fn connect() -> Self {
        Self::connect_with_timeout(Duration::from_millis(300)).await
    }

    /// Binds a client that waits up to `timeout` for each reply.
    pub(crate) async fn connect_with_timeout(timeout: Duration) -> Self {
        // Tags start at a random offset: a restarted node can get the same
        // port back, and a late reply to its previous incarnation must not
        // match a new request (a real connection never outlives its process).
        let first = u64::from(madsim::rand::random::<u32>()) << 20;
        Self {
            ep: Endpoint::bind("0.0.0.0:0").await.unwrap(),
            next_tag: AtomicU64::new(first),
            timeout,
        }
    }

    async fn call(
        &self,
        cmd: &str,
        key: &str,
        version: &str,
        body: &[u8],
    ) -> Result<(String, Option<String>, Vec<u8>), BucketError> {
        let tag = 1_000 + self.next_tag.fetch_add(1, Ordering::Relaxed);
        let addr: SocketAddr = BUCKET.parse().unwrap();
        let exchange = async {
            self.ep
                .send_to(addr, REQUEST_TAG, &encode(tag, cmd, key, version, body))
                .await
                .ok()?;
            let mut buf = vec![0; 1 << 16];
            let (n, _) = self.ep.recv_from(tag, &mut buf).await.ok()?;
            Some(buf[..n].to_vec())
        };
        let Ok(Some(reply)) = timeout(self.timeout, exchange).await else {
            return Err(BucketError::Unavailable("timeout".into()));
        };
        let (status, _) = split_lines(&reply, 1);
        match status[0].as_str() {
            "OK" => {
                let (h, body) = split_lines(&reply, 2);
                Ok(("OK".into(), Some(h[1].clone()), body))
            }
            "NONE" => Ok(("NONE".into(), None, Vec::new())),
            _ => Err(BucketError::Conflict),
        }
    }

    /// Writes `key` unconditionally (for broken variants only).
    pub(crate) async fn put(&self, key: &str, body: Vec<u8>) -> Result<(), BucketError> {
        self.call("PUT", key, "", &body).await.map(drop)
    }

    /// Removes exactly `key` (absent is fine).
    pub(crate) async fn delete(&self, key: &str) -> Result<(), BucketError> {
        self.call("DELETE", key, "", &[]).await.map(drop)
    }

    /// Every object under `prefix`, as a listing shows it: (key, version,
    /// last-modified ms since [`reset_modified`]).
    #[allow(dead_code, reason = "not every simulation lists with times")]
    pub(crate) async fn list_versions(
        &self,
        prefix: &str,
    ) -> Result<Vec<(String, String, u64)>, BucketError> {
        let (_, _, body) = self.call("LISTV", prefix, "", &[]).await?;
        Ok(String::from_utf8_lossy(&body)
            .lines()
            .filter_map(|l| {
                let mut parts = l.split('\t');
                let (k, v, m) = (parts.next()?, parts.next()?, parts.next()?);
                Some((k.to_owned(), v.to_owned(), m.parse().ok()?))
            })
            .collect())
    }

    /// Every object under `prefix`: (key, body). Bodies must not contain
    /// tabs or newlines.
    pub(crate) async fn list(&self, prefix: &str) -> Result<Vec<(String, String)>, BucketError> {
        let (_, _, body) = self.call("LIST", prefix, "", &[]).await?;
        Ok(String::from_utf8_lossy(&body)
            .lines()
            .filter_map(|l| l.split_once('\t'))
            .map(|(k, v)| (k.to_owned(), v.to_owned()))
            .collect())
    }
}

impl Bucket for EpBucket {
    async fn get(&self, key: &str) -> Result<Option<(Vec<u8>, Version)>, BucketError> {
        let (status, version, body) = self.call("GET", key, "", &[]).await?;
        Ok((status == "OK").then(|| (body, Version(version.unwrap_or_default()))))
    }

    async fn create(&self, key: &str, body: Vec<u8>) -> Result<Version, BucketError> {
        let (_, version, _) = self.call("CREATE", key, "", &body).await?;
        Ok(Version(version.unwrap_or_default()))
    }

    async fn replace(
        &self,
        key: &str,
        body: Vec<u8>,
        expected: &Version,
    ) -> Result<Version, BucketError> {
        let (_, version, _) = self.call("REPLACE", key, &expected.0, &body).await?;
        Ok(Version(version.unwrap_or_default()))
    }
}

/// A client whose writes are refused while `cut` is set (reads still
/// answer): a node's lease client, cut while its other clients' writes
/// still land (a slow or throttled bucket timing the lease write out, an
/// error on that one key).
pub(crate) struct CutWrites {
    pub(crate) inner: EpBucket,
    pub(crate) cut: Arc<AtomicBool>,
}

impl CutWrites {
    /// `inner`, never cut.
    pub(crate) fn uncut(inner: EpBucket) -> Self {
        Self {
            inner,
            cut: Arc::default(),
        }
    }

    fn refused(&self) -> Option<BucketError> {
        self.cut
            .load(Ordering::SeqCst)
            .then(|| BucketError::Unavailable("lease write refused".into()))
    }
}

impl Bucket for CutWrites {
    async fn get(&self, key: &str) -> Result<Option<(Vec<u8>, Version)>, BucketError> {
        self.inner.get(key).await
    }

    async fn create(&self, key: &str, body: Vec<u8>) -> Result<Version, BucketError> {
        if let Some(e) = self.refused() {
            return Err(e);
        }
        self.inner.create(key, body).await
    }

    async fn replace(
        &self,
        key: &str,
        body: Vec<u8>,
        expected: &Version,
    ) -> Result<Version, BucketError> {
        if let Some(e) = self.refused() {
            return Err(e);
        }
        self.inner.replace(key, body, expected).await
    }
}

#[test]
fn modification_history_is_scoped_to_one_simulation() {
    for seed in 0..2 {
        madsim::runtime::Runtime::with_seed_and_config(seed, Default::default()).block_on(async {
            note_modified("untracked");
            MODIFIED.with(|m| assert!(m.borrow().1.is_empty()));
            {
                let _timestamps = reset_modified();
                madsim::time::sleep(Duration::from_millis(10)).await;
                note_modified("tracked");
                assert!(modified("tracked") >= 10);
                MODIFIED.with(|m| assert_eq!(m.borrow().1.len(), 1));
            }
            MODIFIED.with(|m| {
                assert!(m.borrow().0.is_none());
                assert!(m.borrow().1.is_empty());
            });
        });
    }
}
