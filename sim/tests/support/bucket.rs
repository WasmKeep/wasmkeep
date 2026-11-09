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
use std::sync::atomic::{AtomicU64, Ordering};
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
}

thread_local! {
    /// Keys whose next request the bucket applies late, by how much (ms):
    /// a fault a simulation aims at one request (a simulation runs on one
    /// thread, the bucket's node included). See [`delay_next`].
    static DELAYED: std::cell::RefCell<Vec<(String, u64)>> =
        const { std::cell::RefCell::new(Vec::new()) };
}

/// The bucket applies the next request for `key` only `ms` after it
/// arrives (once).
#[allow(dead_code, reason = "not every simulation aims a delay")]
pub(crate) fn delay_next(key: &str, ms: u64) {
    DELAYED.with(|d| d.borrow_mut().push((key.to_owned(), ms)));
}

/// Forgets every aimed delay not taken yet: a simulation calls it first,
/// so a delay whose request was never sent cannot reach the next seed.
#[allow(dead_code, reason = "not every simulation aims a delay")]
pub(crate) fn clear_delays() {
    DELAYED.with(|d| d.borrow_mut().clear());
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
        let (ep, store, next) = (Arc::clone(&ep), Arc::clone(&store), Arc::clone(&next));
        let serve = async move {
            if let Some(ms) = delay {
                madsim::time::sleep(Duration::from_millis(ms)).await;
            }
            let (tag, reply) = apply(&store, &next, &request);
            let _ = ep.send_to(from, tag, &reply).await;
        };
        if delay.is_some() {
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
        // Removes exactly this key; an absent key is fine (as on S3).
        "DELETE" => {
            store.remove(key.as_str());
            b"OK\n0\n".to_vec()
        }
        // Unconditional overwrite: only a deliberately broken variant uses
        // it (a fence written without its condition).
        "PUT" => {
            let v = bump();
            store.insert(key.clone(), (body, v));
            format!("OK\n{v}\n").into_bytes()
        }
        "CREATE" if current.is_none() => {
            let v = bump();
            store.insert(key.clone(), (body, v));
            format!("OK\n{v}\n").into_bytes()
        }
        "REPLACE" if current.as_deref() == Some(version.as_str()) => {
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
