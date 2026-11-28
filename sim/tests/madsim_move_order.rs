//! The order of two move requests for one actor (ADR 0017, amendment of
//! 2026-11-28) under madsim: two writers (`wasmkeep actor move`) ask for
//! actor X to move to two different targets, one after the other or at
//! the same time, most often within one second. The bucket keeps
//! last-modified times in whole seconds, as S3 does, and applies each
//! request after a random delay; its replies are late too. Three nodes
//! list the requests every few hundred milliseconds, keep one per actor
//! (`control::newest_moves`, the rule of `moves_pass`) and delete the
//! others, as they do on a tie between two requests of one second.
//!
//! Checked: the later of two requests written one after the other is the
//! one left at the end, and no request is ever dropped in silence: a
//! request a node deleted as superseded was either reported to its writer
//! as written beside another (`MoveWrite::Concurrent`), or replaced by a
//! request written after it in the bucket's order of writes (not their
//! seconds): of two concurrent writers, the one whose key landed first
//! may be told it stands alone and be replaced, as if it had run first.
//! Exactly one request is left at the end; if both writers were told they
//! stand alone, it is the one written last; and a writer told of the
//! other request predicts it by the nodes' rule over its listing (what
//! `actor move` reports when the nodes have not decided within its wait).
//! Over 50 seeds or more, the runs must see writes within one second, a
//! request deleted by a node, and concurrent writes reported. Broken on
//! purpose, each is caught on some seed: the writer before 0.10.0 (the
//! key written alone, the earlier request left beside it), and a writer
//! that writes its key before deleting the others (a node listing in
//! between keeps the earlier on a tie).
//!
//! Not modelled: the move itself (`madsim_drain` does), expiry, a writer
//! running again after a concurrent write (a node's delete from an older
//! listing can remove the new request), and the nodes' copy of the
//! requests between listings.
//!
//! `RUSTFLAGS="--cfg madsim" cargo test -p wasmkeep-sim --test madsim_move_order --target-dir target/madsim`

#![cfg(madsim)]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use madsim::time::{Instant, sleep};
use wasmkeep_cluster::control::{self, Keys, MoveIntent, MoveWrite};
use wasmkeep_cluster::lease::BucketError;
use wasmkeep_cluster::state_root::StateRoot;

#[path = "support/runtime.rs"]
mod runtime;
use runtime::Runtime;

const EPOCH_MS: u64 = 1_700_000_000_000;
/// A bucket request is applied up to this long after it is sent, and its
/// reply comes up to this long after that.
const LATENCY_MS: u64 = 120;
const RUN_MS: u64 = 6_000;
const NODES: usize = 3;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Writer {
    /// `control::write_move`: the others deleted, the key written, listed
    /// again.
    Production,
    /// Unsafe: the writer before 0.10.0, the key written alone.
    KeyOnly,
    /// Unsafe: the key written first, then the others deleted.
    PutFirst,
}

#[derive(Default)]
struct World {
    start: Option<Instant>,
    keys: BTreeMap<String, u64>,
    /// When each key was last written (ms since the start).
    put_at: BTreeMap<String, u64>,
    /// Keys a node deleted as superseded.
    superseded: Vec<String>,
}

#[derive(Clone)]
struct Bucket(Arc<Mutex<World>>);

impl Bucket {
    fn elapsed(&self) -> u64 {
        let start = self.0.lock().unwrap().start.unwrap();
        u64::try_from(start.elapsed().as_millis()).unwrap()
    }
    async fn delay() {
        sleep(Duration::from_millis(
            madsim::rand::random::<u64>() % LATENCY_MS,
        ))
        .await;
    }
}

impl Keys for Bucket {
    async fn list_keys(&self, prefix: &str) -> Result<Vec<(String, u64)>, BucketError> {
        Self::delay().await;
        let listed = self
            .0
            .lock()
            .unwrap()
            .keys
            .iter()
            .filter(|(k, _)| k.starts_with(prefix))
            .map(|(k, m)| (k.clone(), *m))
            .collect();
        Self::delay().await;
        Ok(listed)
    }
    async fn put_empty(&self, key: &str) -> Result<(), BucketError> {
        Self::delay().await;
        let now = self.elapsed();
        {
            let mut w = self.0.lock().unwrap();
            // Whole seconds, as S3 reports them.
            w.keys
                .insert(key.to_owned(), (EPOCH_MS + now) / 1_000 * 1_000);
            w.put_at.insert(key.to_owned(), now);
        }
        Self::delay().await;
        Ok(())
    }
    async fn put_body(&self, key: &str, _body: Vec<u8>) -> Result<(), BucketError> {
        self.put_empty(key).await
    }
    async fn get_body(&self, _key: &str) -> Result<Option<Vec<u8>>, BucketError> {
        Ok(None)
    }
    async fn delete(&self, key: &str) -> Result<(), BucketError> {
        Self::delay().await;
        self.0.lock().unwrap().keys.remove(key);
        Self::delay().await;
        Ok(())
    }
}

/// Writes `key` by `rule`.
async fn write(bucket: &Bucket, key: &str, rule: Writer) -> MoveWrite {
    let actor = format!("{}/", key.rsplit_once('/').unwrap().0);
    let others = |listed: Vec<(String, u64)>| -> Vec<String> {
        listed
            .into_iter()
            .map(|(k, _)| k)
            .filter(|k| k != key)
            .collect()
    };
    match rule {
        Writer::Production => control::write_move(bucket, key).await.unwrap(),
        Writer::KeyOnly => {
            bucket.put_empty(key).await.unwrap();
            MoveWrite::Alone
        }
        Writer::PutFirst => {
            bucket.put_empty(key).await.unwrap();
            for other in others(bucket.list_keys(&actor).await.unwrap()) {
                bucket.delete(&other).await.unwrap();
            }
            let listed = control::actor_moves(bucket, key).await.unwrap();
            if listed.iter().all(|m| m.key() == key) {
                MoveWrite::Alone
            } else {
                MoveWrite::Concurrent(listed)
            }
        }
    }
}

/// A node's pass: list, keep one request per actor, delete the others.
async fn node(bucket: Bucket) {
    loop {
        sleep(Duration::from_millis(
            20 + madsim::rand::random::<u64>() % 300,
        ))
        .await;
        if bucket.elapsed() > RUN_MS {
            return;
        }
        let listed = bucket.list_keys("state/").await.unwrap();
        let (requests, _) = control::parse(&listed, EPOCH_MS + bucket.elapsed());
        let (_, superseded) = control::newest_moves(requests.moves);
        for intent in superseded {
            let key = intent.key();
            bucket.delete(&key).await.unwrap();
            bucket.0.lock().unwrap().superseded.push(key);
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Outcome {
    concurrent: bool,
    keys: [String; 2],
    /// What each write returned, and when (ms since the start).
    written: [(MoveWrite, u64); 2],
    put_at: BTreeMap<String, u64>,
    superseded: Vec<String>,
    left: Vec<String>,
}

impl Outcome {
    /// Both keys written within one second of the bucket's clock.
    fn same_second(&self) -> bool {
        let second = |k: &String| (EPOCH_MS + self.put_at[k]) / 1_000;
        second(&self.keys[0]) == second(&self.keys[1])
    }
}

fn simulate(seed: u64, rule: Writer) -> Outcome {
    let rt = Runtime::with_seed_and_config(seed, madsim::Config::default());
    rt.block_on(async move {
        let bucket = Bucket(Arc::new(Mutex::new(World {
            start: Some(Instant::now()),
            ..World::default()
        })));
        let root = StateRoot::new("", "acme", "shop");
        // Which target the first writer names: the later one's key sorts
        // first or second.
        let targets = if madsim::rand::random::<bool>() {
            ["node-a", "node-b"]
        } else {
            ["node-b", "node-a"]
        };
        let keys = targets.map(|t| control::checked_move_key(&root, "room", "x", t).unwrap());
        let concurrent = madsim::rand::random::<u64>().is_multiple_of(3);
        let nodes: Vec<_> = (0..NODES)
            .map(|_| madsim::task::spawn(node(bucket.clone())))
            .collect();
        // Started at any point of a second.
        sleep(Duration::from_millis(
            1_000 + madsim::rand::random::<u64>() % 1_000,
        ))
        .await;
        let written = if concurrent {
            let (b0, k0) = (bucket.clone(), keys[0].clone());
            let first = madsim::task::spawn(async move {
                let w = write(&b0, &k0, rule).await;
                (w, b0.elapsed())
            });
            sleep(Duration::from_millis(madsim::rand::random::<u64>() % 150)).await;
            let second = write(&bucket, &keys[1], rule).await;
            let second = (second, bucket.elapsed());
            [first.await.unwrap(), second]
        } else {
            let first = write(&bucket, &keys[0], rule).await;
            let first = (first, bucket.elapsed());
            sleep(Duration::from_millis(madsim::rand::random::<u64>() % 200)).await;
            let second = write(&bucket, &keys[1], rule).await;
            [first, (second, bucket.elapsed())]
        };
        for n in nodes {
            n.await.unwrap();
        }
        let w = bucket.0.lock().unwrap();
        Outcome {
            concurrent,
            keys,
            written,
            put_at: w.put_at.clone(),
            superseded: w.superseded.clone(),
            left: w.keys.keys().cloned().collect(),
        }
    })
}

/// Why a run broke the rules, if it did.
fn broken(o: &Outcome) -> Option<String> {
    // A request deleted by a node in favour of one written before it (by
    // the bucket's order of writes, not its whole seconds), whose writer
    // was told it stands alone. One written after it may replace it: the
    // later write counts.
    for (i, key) in o.keys.iter().enumerate() {
        let (answer, _) = &o.written[i];
        let other = &o.keys[1 - i];
        let written_later = o.put_at.get(other) < o.put_at.get(key);
        if o.superseded.contains(key) && *answer == MoveWrite::Alone && written_later {
            return Some(format!(
                "the request to {key} was dropped in silence (concurrent: {})",
                o.concurrent
            ));
        }
    }
    if !o.concurrent && o.left != [o.keys[1].clone()] {
        return Some(format!(
            "the later request is not the one left: {:?}, wanted {}",
            o.left, o.keys[1]
        ));
    }
    // The nodes keep exactly one, the same one.
    if o.left.len() != 1 {
        return Some(format!("{} requests left: {:?}", o.left.len(), o.left));
    }
    // Both writers told they stand alone: the one written last stands.
    let last = o.keys.iter().max_by_key(|k| o.put_at.get(*k)).unwrap();
    if o.written.iter().all(|(w, _)| *w == MoveWrite::Alone) && o.left[0] != *last {
        return Some(format!(
            "both writers told alone, yet {} stands, not {last}",
            o.left[0]
        ));
    }
    // A writer told of the other request predicts, by the nodes' rule over
    // its listing, the one that stands (what `actor move` reports past its
    // wait).
    for (w, _) in &o.written {
        if let MoveWrite::Concurrent(listed) = w {
            let (kept, _) = control::newest_moves(listed.clone());
            let predicted = kept.first().map(MoveIntent::key);
            if predicted.as_ref() != o.left.first() {
                return Some(format!(
                    "a writer predicted {predicted:?}, yet {} stands",
                    o.left[0]
                ));
            }
        }
    }
    None
}

fn seeds() -> u64 {
    std::env::var("WASMKEEP_SIM_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(100)
}

#[test]
fn the_later_move_counts_and_none_is_dropped_in_silence() {
    let seeds = seeds();
    assert!(
        seeds > 0,
        "the move-order simulation needs at least one seed"
    );
    let (mut same_second, mut ties_deleted, mut reported) = (0, 0, 0);
    for seed in 0..seeds {
        let o = simulate(seed, Writer::Production);
        if let Some(why) = broken(&o) {
            panic!("seed {seed}: {why}: {o:?}");
        }
        same_second += u64::from(o.same_second());
        ties_deleted += u64::from(!o.superseded.is_empty());
        reported += o
            .written
            .iter()
            .filter(|(w, _)| matches!(w, MoveWrite::Concurrent(_)))
            .count();
    }
    // About three seeds in five write both within one second, one in
    // five has a node delete a request, one in three reports a concurrent
    // write: checked only over enough seeds to be stable.
    if seeds >= 50 {
        assert!(
            same_second * 5 > seeds * 2,
            "few writes within one second ({same_second} of {seeds})"
        );
        assert!(ties_deleted > 0, "no request deleted by a node");
        assert!(reported > 0, "no concurrent write reported");
    }
    eprintln!(
        "move order: {seeds} seeds: {same_second} with both writes in one second, \
         {ties_deleted} with a request deleted by a node, {reported} concurrent writes reported"
    );
}

#[test]
fn the_unsafe_writers_are_caught() {
    let seeds = seeds().max(200);
    for rule in [Writer::KeyOnly, Writer::PutFirst] {
        let found = (0..seeds).find_map(|seed| broken(&simulate(seed, rule)).map(|w| (seed, w)));
        let (seed, why) = found.unwrap_or_else(|| panic!("{rule:?} never caught in {seeds} seeds"));
        assert_eq!(
            broken(&simulate(seed, Writer::Production)),
            None,
            "seed {seed} breaks the correct writer too"
        );
        eprintln!("move order: {rule:?} caught at seed {seed}: {why}");
    }
}

#[test]
fn a_run_replays_with_the_same_seed() {
    assert_eq!(
        simulate(7, Writer::Production),
        simulate(7, Writer::Production)
    );
}
