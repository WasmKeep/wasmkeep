//! The alarm index of ADR 0009 under madsim: three nodes take turns
//! owning one actor through node leases and its ownership record, as the
//! server runs them since ADR 0020 (`wasmkeep_sim::node`, whose holds act by
//! the server's own rule, `wasmkeep_cluster::holds`), and keep its alarm index
//! entries with the real rules (`wasmkeep_cluster::alarm_index`): a fresh
//! key for every PUT (R1), deletes only of exact keys the durable state no
//! longer names (R2), never of an entry from a newer generation (R3).
//!
//! The actor's durable state follows replication's rule (ADR 0007): each
//! write is an upload keyed by the writer's generation and a sequence, and
//! the state is the newest upload of the highest generation. A stale
//! owner's upload that lands after a new owner restored, before the new
//! owner uploaded anything, is the state after all (a late commit); so a
//! new owner fences (uploads once) before it removes entries as stale. The
//! bucket applies one request in 10 late (up to 2.5 s), links are cut for
//! a while (requests lost), nodes crash: deletes and uploads land long
//! after they were sent, which is the late-delete race.
//!
//! The invariant, checked every 50 ms of the run and after every late
//! request has landed: if the durable state holds an alarm, the index entry
//! it names exists (at any moment the owner could die, and only that entry
//! lets another node fire the alarm). Some node incarnations join with an
//! alarm in their own data (single-node use): the first owner that finds
//! nothing in the bucket uploads it, listed first. Five unsafe variants
//! (the same key reused for a time, as before the fix; no generation check;
//! no fence before clearing; a joining node's first upload before its
//! entry; clearing entries while the bucket has nothing for the actor)
//! must each be caught by some seed.
//!
//! `RUSTFLAGS="--cfg madsim" cargo test -p wasmkeep-sim --test madsim_alarm_index --target-dir target/madsim`

#![cfg(madsim)]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code

use std::sync::Arc;
use std::time::Duration;

use madsim::net::NetSim;
use madsim::runtime::{Handle, Runtime};
use madsim::time::{Instant, sleep};
use wasmkeep_cluster::alarm_index::{self, Entry};
use wasmkeep_cluster::lease::{Bucket, LeaseConfig};
use wasmkeep_sim::driver::Env;
use wasmkeep_sim::node::{Breaks, Held, LeaseNode, NodeBuckets, NodeSetup};
use wasmkeep_sim::rng::Rng;

#[path = "support/bucket.rs"]
mod bucket;
use bucket::{EpBucket, Faults, Store, bucket_node_with, clear_delays, delay_next};

/// The actor's ownership record (ADR 0020).
const RECORD: &str = "owners/t/c/a";
/// Node lease objects.
const NODES: &str = "nodes/";
/// Uploads of the actor's state: `state/<generation, 20>/<seq, 20>`.
const STATE: &str = "state/t/c/a/";
const INDEX: &str = "alarms/t/";
const CLASS: &str = "c";
const NAME: &str = "a";
/// The due times alarms move between: two, so an alarm often returns to a
/// time it just left (and a key, or a stale time, is met again).
const TIMES: [u64; 2] = [1_000, 2_000];
const EPOCH_MS: u64 = 1_000_000;
/// How long the lease client waits for the bucket.
const WAIT: Duration = Duration::from_secs(3);
/// How long deletes wait: shorter than the bucket's slowest requests, so a
/// delete can time out and still land later (an S3 client timing out and
/// retrying), after the owner that sent it has moved on.
const DELETE_WAIT: Duration = Duration::from_secs(1);

/// Which rules the owners follow (all, except to prove a rule matters).
#[derive(Debug, Clone, Copy)]
struct Rules {
    /// R1: a fresh key (this owner's generation and a new nonce) per PUT.
    /// Off: the keys before the fix, one per due time and no generation (so
    /// R3 cannot tell owners apart either).
    fresh_keys: bool,
    /// R3: never delete an entry with a generation above one's own.
    generation_fence: bool,
    /// Upload once under one's own generation before clearing entries.
    fence_before_clearing: bool,
    /// A node joining with an alarm in its own data (single-node use) lists
    /// it before the actor's first upload carries it into the bucket. Off:
    /// the first upload names no entry, as before the fix.
    index_before_first_upload: bool,
    /// An owner that found nothing in the bucket, and has uploaded
    /// nothing, clears no entries (a joining node's first upload may still
    /// land and name them; fencing by upload would create the actor). Off:
    /// it clears them without a fence, as the server did before the fix.
    refuse_clearing_when_empty: bool,
    /// A fault aimed at one moment, not a rule: a joining node's first
    /// state upload is applied by the bucket 4 to 6 s after it arrives, and
    /// the node is cut off right after sending it, for 2.5 to 6 s, so it is
    /// fenced and the actor taken while that upload is still pending. On node leases that, or
    /// a crash right after sending, is how it happens (the lease keeps
    /// renewing while an upload is
    /// merely slow), and random faults hit that moment once in thousands of
    /// seeds. With it, an owner hibernates half the time right after
    /// delivering a stale alarm (5% otherwise).
    cut_at_first_upload: bool,
}

const SAFE: Rules = Rules {
    fresh_keys: true,
    generation_fence: true,
    fence_before_clearing: true,
    index_before_first_upload: true,
    refuse_clearing_when_empty: true,
    cut_at_first_upload: false,
};

/// The actor's durable state: its committed alarm and the entry it names.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
struct State {
    alarm: Option<u64>,
    indexed: Option<Entry>,
}

impl State {
    fn encode(&self) -> String {
        format!(
            "{}|{}",
            self.alarm.map(|a| a.to_string()).unwrap_or_default(),
            alarm_index::to_meta(self.indexed.as_ref()),
        )
    }

    fn decode(text: &str) -> Self {
        let (alarm, meta) = text.split_once('|').unwrap();
        Self {
            alarm: (!alarm.is_empty()).then(|| alarm.parse().unwrap()),
            indexed: alarm_index::parse_meta(meta),
        }
    }
}

/// The upload the state is: the newest of the highest generation, with
/// its position (generation, sequence).
fn head<'a>(objects: impl IntoIterator<Item = (&'a str, &'a str)>) -> Option<((u64, u64), State)> {
    objects
        .into_iter()
        .filter_map(|(k, body)| {
            let (generation, seq) = k.strip_prefix(STATE)?.split_once('/')?;
            Some(((generation.parse().ok()?, seq.parse().ok()?), body))
        })
        .max_by_key(|(position, _)| *position)
        .map(|(position, body)| (position, State::decode(body)))
}

/// The state's head in a store snapshot.
fn stored_head(store: &std::collections::BTreeMap<String, (Vec<u8>, u64)>) -> Option<State> {
    let uploads: Vec<(String, String)> = store
        .range(STATE.to_owned()..)
        .take_while(|(k, _)| k.starts_with(STATE))
        .map(|(k, (b, _))| (k.clone(), String::from_utf8_lossy(b).into_owned()))
        .collect();
    head(uploads.iter().map(|(k, b)| (k.as_str(), b.as_str()))).map(|(_, s)| s)
}

fn key(entry: &Entry) -> String {
    format!("{INDEX}{}", alarm_index::key_tail(CLASS, NAME, entry))
}

/// The actor's entries listed in the index.
fn listed(objects: &[(String, String)]) -> Vec<Entry> {
    objects
        .iter()
        .filter_map(|(k, _)| k.strip_prefix(INDEX))
        .filter_map(alarm_index::parse_listed)
        .filter(|l| l.class == CLASS && l.name == NAME)
        .map(|l| l.entry)
        .collect()
}

#[derive(Default)]
struct Counts {
    /// Durable state writes holding an alarm.
    alarms: u64,
    /// Tagged entries removed as stale (the legacy candidate not counted).
    clears: u64,
    /// Fences uploaded before clearing.
    fences: u64,
    /// Deletes sent (retirements and clears).
    deletes: u64,
    /// Times an owner took the actor over.
    takeovers: u64,
    /// First uploads that carried an alarm from a joining node's own data.
    joined: u64,
    /// Faults aimed at a joining node's first upload.
    aimed: u64,
    /// Clears refused because the bucket had nothing and this owner had
    /// uploaded nothing (`refuse_clearing_when_empty`).
    refused: u64,
}

thread_local! {
    static COUNTS: std::cell::RefCell<Counts> = std::cell::RefCell::default();
}

fn count(f: impl FnOnce(&mut Counts)) {
    COUNTS.with(|c| f(&mut c.borrow_mut()));
}

fn trace(msg: &str) {
    if std::env::var_os("WASMKEEP_SIM_TRACE").is_some() {
        eprintln!("[{:?}] {msg}", madsim::time::Instant::now());
    }
}

struct Owner {
    held: Held,
    state: State,
    /// The position of the upload this owner's state is (restored, or its
    /// own last one).
    head: (u64, u64),
}

/// A node's clock: global time shifted by its skew (`Copy`, as the node
/// driver needs).
#[derive(Clone, Copy)]
struct NodeClock {
    time: GlobalTime,
    skew_ms: i64,
}

impl wasmkeep_cluster::lease::Clock for NodeClock {
    fn now_ms(&self) -> u64 {
        self.time.now().saturating_add_signed(self.skew_ms)
    }
}

/// The simulation's services for a node.
#[derive(Clone, Copy)]
struct SimEnv(GlobalTime);

impl Env for SimEnv {
    fn global_ms(&self) -> u64 {
        self.0.now()
    }

    async fn sleep_ms(&self, ms: u64) {
        sleep(Duration::from_millis(ms)).await;
    }
}

struct Node<'a> {
    node: &'a LeaseNode<EpBucket, NodeClock>,
    bucket: &'a EpBucket,
    /// For deletes only (see `DELETE_WAIT`).
    deletes: &'a EpBucket,
    rules: Rules,
    rng: Rng,
    /// Unique to this incarnation of the node, so nonces never repeat
    /// across restarts (as the process seed does in production).
    incarnation: u32,
    nonces: u64,
    /// An alarm in this node's own data, from before it joined the
    /// cluster: uploaded by the first owner that finds nothing in the
    /// bucket here (then dropped, once uploaded).
    local: Option<u64>,
    /// This node's index among the nodes, and where it asks the fault loop
    /// to cut it off (`cut_at_first_upload`).
    index: usize,
    cut: Arc<std::sync::Mutex<Option<usize>>>,
}

impl Node<'_> {
    fn valid(&self, o: &Owner) -> bool {
        self.node.may_act(&o.held)
    }

    fn fresh_entry(&mut self, at: u64, generation: u64) -> Entry {
        if !self.rules.fresh_keys {
            // Before the fix: one key per due time, whoever writes it.
            return Entry::owned(at, 0, "0").unwrap();
        }
        self.nonces += 1;
        let nonce = format!("{:x}{:x}", self.incarnation, self.nonces);
        Entry::owned(at, generation, nonce).unwrap()
    }

    fn may_delete(&self, entry: &Entry, generation: u64) -> bool {
        !self.rules.generation_fence || alarm_index::may_delete(entry, generation)
    }

    /// Uploads `next` as the durable state, at the next position under
    /// this owner's generation, only while the hold may act. `false`:
    /// unknown whether it landed, or it landed too late to count; the
    /// owner gives the actor up (the upload may still land, later).
    async fn write(&self, o: &mut Owner, next: State) -> bool {
        if !self.valid(o) {
            return false;
        }
        let position = (o.held.generation, o.head.1 + 1);
        let key = format!("{STATE}{:020}/{:020}", position.0, position.1);
        match self.bucket.create(&key, next.encode().into_bytes()).await {
            Ok(_) if self.valid(o) => {
                if next.alarm.is_some() {
                    count(|c| c.alarms += 1);
                }
                o.state = next;
                o.head = position;
                true
            }
            _ => false,
        }
    }

    /// Takes the actor over: acquire, then restore the state (the newest
    /// upload of the highest generation). Nothing is written: as in
    /// production, a late upload of an earlier owner can still become the
    /// state until this owner uploads.
    async fn take_over(&mut self) -> Option<Owner> {
        let o = self.take_over_inner().await;
        if o.is_some() {
            count(|c| c.takeovers += 1);
        }
        o
    }

    async fn take_over_inner(&mut self) -> Option<Owner> {
        let held = self.node.acquire(RECORD, 1).await.ok()?;
        let Ok(objects) = self.bucket.list(STATE).await else {
            // Production gives the actor up.
            let _ = self.node.release(&held).await;
            return None;
        };
        let (head, state) = head(objects.iter().map(|(k, b)| (k.as_str(), b.as_str())))
            .unwrap_or(((0, 0), State::default()));
        trace(&format!(
            "n{} took over gen {} at {head:?}, local {:?}",
            self.index + 1,
            held.generation,
            self.local
        ));
        Some(Owner { held, state, head })
    }

    /// A joining node's first upload, as `start_replication` does when
    /// the bucket has nothing for the actor and its own data holds an
    /// alarm: the entry first (`index_first_alarm`), then the snapshot
    /// naming it.
    async fn first_upload(&mut self, o: &mut Owner) -> bool {
        let Some(at) = self.local else {
            return true;
        };
        if o.head != (0, 0) {
            return true;
        }
        let indexed = if self.rules.index_before_first_upload {
            if !self.valid(o) {
                return false;
            }
            let entry = self.fresh_entry(at, o.held.generation);
            if self.bucket.create(&key(&entry), Vec::new()).await.is_err()
                && !matches!(self.bucket.get(&key(&entry)).await, Ok(Some(_)))
            {
                return false;
            }
            Some(entry)
        } else {
            None
        };
        if self.rules.cut_at_first_upload {
            // The first upload reaches the bucket and is applied 4 to 6 s
            // later; meanwhile the node is cut off (the fault loop's next
            // step, within 50 ms) and fenced, and another node takes over
            // with nothing in the bucket yet.
            let key = format!("{STATE}{:020}/{:020}", o.held.generation, o.head.1 + 1);
            delay_next(&key, 4_000 + self.rng.below(2_000));
            *self.cut.lock().unwrap() = Some(self.index);
            count(|c| c.aimed += 1);
            trace(&format!(
                "n{} entry {indexed:?} landed; its first upload {key} is delayed, the node cut off",
                self.index + 1
            ));
        }
        let next = State {
            alarm: Some(at),
            indexed,
        };
        if !self.write(o, next).await {
            return false;
        }
        count(|c| c.joined += 1);
        self.local = None;
        true
    }

    /// An event that sets (or clears) the alarm, as `finish` does: a new
    /// entry before the state is written, the replaced one removed after.
    async fn set_alarm(&mut self, o: &mut Owner, at: Option<u64>) -> bool {
        let previous = o.state.indexed.clone();
        if !alarm_index::needs_publish(previous.as_ref(), at) {
            return true;
        }
        let generation = o.held.generation;
        let entry = match at {
            Some(at) => {
                if !self.valid(o) {
                    return false;
                }
                let entry = self.fresh_entry(at, generation);
                if self.bucket.create(&key(&entry), Vec::new()).await.is_err()
                    && !matches!(self.bucket.get(&key(&entry)).await, Ok(Some(_)))
                {
                    return false;
                }
                Some(entry)
            }
            None => None,
        };
        let next = State {
            alarm: at,
            indexed: entry,
        };
        if !self.write(o, next).await {
            return false;
        }
        // R2: the replaced entry, now that the state no longer names it.
        if let Some(previous) = previous
            && self.may_delete(&previous, generation)
            && self.valid(o)
        {
            count(|c| c.deletes += 1);
            let _ = self.deletes.delete(&key(&previous)).await;
        }
        true
    }

    /// An alarm delivered for `hint` that the committed alarm no longer
    /// matches: removes the entries there the state does not name, as
    /// `clear_stale_hint` does.
    async fn clear_stale(&mut self, o: &mut Owner, hint: u64) -> bool {
        if o.state.alarm == Some(hint) || !self.valid(o) {
            return true;
        }
        // Found nothing in the bucket, uploaded nothing since: a joining
        // node's first upload may still land and name these entries, and
        // fencing would create the actor. The server leaves them.
        if o.head == (0, 0) {
            if self.rules.refuse_clearing_when_empty {
                count(|c| c.refused += 1);
                return true;
            }
        } else if self.rules.fence_before_clearing && o.head.0 != o.held.generation {
            // The state is this owner's only once its own upload ends the
            // history: until then a late upload of an earlier owner can
            // become it, and name an entry removed here.
            let same = o.state.clone();
            if !self.write(o, same).await {
                return false;
            }
            count(|c| c.fences += 1);
        }
        let Ok(objects) = self.bucket.list(INDEX).await else {
            return true;
        };
        let generation = if self.rules.generation_fence {
            o.held.generation
        } else {
            u64::MAX
        };
        let clear = alarm_index::entries_to_clear(
            hint,
            &listed(&objects),
            o.state.indexed.as_ref(),
            generation,
        );
        let tagged = clear.iter().filter(|e| !e.is_legacy()).count();
        count(|c| c.clears += u64::try_from(tagged).unwrap());
        // Like the server: the lease was checked once, before listing; a
        // slow listing can return entries written after it ran out, which
        // only R3 keeps this owner from deleting.
        for entry in clear {
            trace(&format!(
                "n{} clears {entry:?} (head {:?})",
                self.index + 1,
                o.head
            ));
            count(|c| c.deletes += 1);
            let _ = self.deletes.delete(&key(&entry)).await;
        }
        true
    }

    async fn run(&mut self) {
        let mut owner: Option<Owner> = None;
        loop {
            sleep(Duration::from_millis(50)).await;
            // The node lease was lost: this process exits (a new one starts
            // when the node restarts).
            if self.node.lost() {
                return;
            }
            let Some(mut o) = owner.take() else {
                owner = self.take_over().await;
                if let Some(mut o) = owner.take() {
                    if self.first_upload(&mut o).await {
                        owner = Some(o);
                    } else {
                        // An abandoned hold: released, as the server's is.
                        let _ = self.node.release(&o.held).await;
                    }
                }
                continue;
            };
            if !self.valid(&o) {
                continue;
            }
            let mut cleared = false;
            let kept = match self.rng.below(10) {
                0..=4 => {
                    let at = TIMES[usize::try_from(self.rng.below(2)).unwrap()];
                    self.set_alarm(&mut o, Some(at)).await
                }
                5..=6 => self.set_alarm(&mut o, None).await,
                _ => {
                    cleared = true;
                    let hint = TIMES[usize::try_from(self.rng.below(2)).unwrap()];
                    self.clear_stale(&mut o, hint).await
                }
            };
            // Now and then the owner gives the actor up (hibernation). Under
            // the aimed profile, half the time after a stale alarm's
            // delivery, which usually leaves an actor with nothing to do:
            // the owner that cleared then writes nothing that would
            // supersede an earlier owner's late upload.
            let hibernates = if cleared && self.rules.cut_at_first_upload {
                self.rng.chance(50)
            } else {
                self.rng.chance(5)
            };
            if kept && hibernates {
                let _ = self.node.release(&o.held).await;
                sleep(Duration::from_secs(1)).await;
                continue;
            }
            if kept {
                owner = Some(o);
            } else {
                // An abandoned hold (a failed or late upload): released, as
                // the server's is (ADR 0020, section 3).
                let _ = self.node.release(&o.held).await;
            }
        }
    }
}

#[derive(Clone, Copy)]
struct GlobalTime(Instant);

impl GlobalTime {
    fn now(&self) -> u64 {
        EPOCH_MS + u64::try_from(self.0.elapsed().as_millis()).unwrap()
    }
}

/// The final store, what the run did, and the first violation seen during
/// the run (time in ms, description).
fn simulate(seed: u64, rules: Rules, duration: Duration) -> (Store, Counts, Option<(u64, String)>) {
    COUNTS.with(|c| *c.borrow_mut() = Counts::default());
    clear_delays();
    let config = LeaseConfig::new(1_000, 500, 200, 300).unwrap();
    let max_skew_ms = 300;
    let rt = Runtime::with_seed_and_config(seed, madsim::Config::default());
    let store = Store::default();
    let store_out = Arc::clone(&store);
    let watched = Arc::clone(&store);
    let first = Arc::new(std::sync::Mutex::new(None));
    // A node asking to be cut off now (`cut_at_first_upload`).
    let cut: Arc<std::sync::Mutex<Option<usize>>> = Arc::default();
    let first_out = Arc::clone(&first);
    rt.block_on(async move {
        let handle = Handle::current();
        let time = GlobalTime(Instant::now());
        let mut rng = Rng::new(seed ^ 0x0A1A_2B0C);
        let bucket = handle
            .create_node()
            .name("bucket")
            .ip("10.0.0.1".parse().unwrap())
            .init(move || bucket_node_with(Arc::clone(&store), Faults { slow_one_in: 10 }))
            .build();
        let mut nodes = Vec::new();
        for i in 1..=3u32 {
            let span = max_skew_ms * 2 + 1;
            let skew_ms =
                i64::try_from(rng.below(span)).unwrap() - i64::try_from(max_skew_ms).unwrap();
            let node_seed = seed ^ (u64::from(i) << 40) ^ 0x00A1_A2A3;
            let node = handle
                .create_node()
                .name(format!("n{i}"))
                .ip(format!("10.0.1.{i}").parse().unwrap())
                .init({
                    let cut = Arc::clone(&cut);
                    move || {
                        let cut = Arc::clone(&cut);
                        async move {
                            // A new process each start (and restart): its own
                            // instance, through the start check, its lease on a
                            // task of its own (bounded as the server bounds a lease
                            // write), dying with the node.
                            let clock = NodeClock { time, skew_ms };
                            let instance = format!("{:08x}", madsim::rand::random::<u32>());
                            let attempt =
                                Duration::from_millis((config.ttl_ms() - config.safety_ms()) / 3);
                            let setup = NodeSetup {
                                id: format!("n{i}"),
                                peers: (1..=3u32)
                                    .filter(|&p| p != i)
                                    .map(|p| format!("n{p}"))
                                    .collect(),
                                nodes_prefix: NODES.to_owned(),
                                config,
                                attempt_ms: u64::try_from(attempt.as_millis()).unwrap(),
                                breaks: Breaks::default(),
                            };
                            let buckets = NodeBuckets {
                                lease: EpBucket::connect_with_timeout(attempt).await,
                                task_records: EpBucket::connect_with_timeout(WAIT).await,
                                peers: EpBucket::connect_with_timeout(attempt).await,
                                records: EpBucket::connect_with_timeout(WAIT).await,
                            };
                            let (lease_node, lease_task) = LeaseNode::start(
                                setup,
                                &instance,
                                clock,
                                buckets,
                                format!("n{i}\n").into_bytes(),
                                &SimEnv(time),
                            )
                            .await;
                            madsim::task::spawn_local(lease_task.run(SimEnv(time)));
                            let bucket = EpBucket::connect_with_timeout(WAIT).await;
                            let deletes = EpBucket::connect_with_timeout(DELETE_WAIT).await;
                            let mut node = Node {
                                node: &lease_node,
                                bucket: &bucket,
                                deletes: &deletes,
                                rules,
                                rng: Rng::new(node_seed),
                                incarnation: madsim::rand::random::<u32>(),
                                nonces: 0,
                                // Two nodes in three join with an alarm of their own.
                                local: (madsim::rand::random::<u32>() % 3 != 0)
                                    .then(|| TIMES[usize::from(madsim::rand::random::<bool>())]),
                                index: usize::try_from(i - 1).unwrap(),
                                cut,
                            };
                            node.run().await;
                        }
                    }
                })
                .build();
            nodes.push(node.id());
        }

        let net = NetSim::current();
        let mut held: Vec<(usize, u64)> = Vec::new();
        let mut crashed: Vec<(usize, u64)> = Vec::new();
        let steps = u64::try_from(duration.as_millis()).unwrap() / 50;
        for step in 1..=steps {
            sleep(Duration::from_millis(50)).await;
            let at = step * 50;
            if let Err(v) = check(&watched) {
                first.lock().unwrap().get_or_insert((at, v));
            }
            let i = usize::try_from(rng.below(3)).unwrap();
            // A node asked to be cut off now (`cut_at_first_upload`).
            if let Some(n) = cut.lock().unwrap().take()
                && !held.iter().any(|(h, _)| *h == n)
            {
                net.clog_link(nodes[n], bucket.id());
                net.clog_link(bucket.id(), nodes[n]);
                held.push((n, at + 2_500 + rng.below(3_500)));
            }
            match rng.below(100) {
                0..=1 => {
                    net.clog_link(nodes[i], bucket.id());
                    net.clog_link(bucket.id(), nodes[i]);
                    held.push((i, at + 200 + rng.below(2_300)));
                }
                2 if !crashed.iter().any(|(n, _)| *n == i) => {
                    handle.kill(nodes[i]);
                    crashed.push((i, at + 300 + rng.below(1_500)));
                }
                _ => {}
            }
            held.retain(|(n, t)| {
                if *t <= at {
                    net.unclog_link(nodes[*n], bucket.id());
                    net.unclog_link(bucket.id(), nodes[*n]);
                    false
                } else {
                    true
                }
            });
            crashed.retain(|(n, t)| {
                if *t <= at {
                    handle.restart(nodes[*n]);
                    false
                } else {
                    true
                }
            });
        }
        // Stop every node, then let held traffic land: late deletes are the
        // point, and nothing may repair the index after them.
        for node in &nodes {
            handle.kill(*node);
        }
        for (n, _) in held {
            net.unclog_link(nodes[n], bucket.id());
            net.unclog_link(bucket.id(), nodes[n]);
        }
        sleep(Duration::from_secs(5)).await;
        if let Err(v) = check(&watched) {
            first.lock().unwrap().get_or_insert((u64::MAX, v));
        }
    });
    let first = first_out.lock().unwrap().take();
    (store_out, COUNTS.with(std::cell::RefCell::take), first)
}

/// The invariant on the final bucket: a committed alarm is listed under
/// the exact entry the state names.
fn check(store: &Store) -> Result<(), String> {
    let store = store.lock().unwrap();
    let Some(state) = stored_head(&store) else {
        return Ok(());
    };
    let Some(at) = state.alarm else {
        return Ok(());
    };
    let Some(entry) = &state.indexed else {
        return Err(format!("alarm {at} committed with no entry named"));
    };
    if entry.at != at {
        return Err(format!("alarm {at} names an entry due at {}", entry.at));
    }
    if !store.contains_key(&key(entry)) {
        return Err(format!(
            "alarm {at} committed, but its entry {entry:?} is gone: no other node would fire it"
        ));
    }
    Ok(())
}

fn seeds() -> u64 {
    std::env::var("WASMKEEP_SIM_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(200)
}

#[test]
fn a_committed_alarm_always_keeps_its_index_entry() {
    let only: Option<u64> = std::env::var("WASMKEEP_SIM_SEED")
        .ok()
        .and_then(|s| s.parse().ok());
    let seeds_run = if only.is_some() { 1 } else { seeds() };
    let (mut alarms, mut clears, mut deletes, mut takeovers, mut ending_with_alarm) =
        (0, 0, 0, 0, 0);
    let mut joined = 0;
    for seed in only.map_or(0..seeds(), |s| s..s + 1) {
        let (store, counts, violation) = simulate(seed, SAFE, Duration::from_secs(30));
        if let Some((at, v)) = violation {
            panic!("seed {seed} at {at} ms: {v}");
        }
        alarms += counts.alarms;
        clears += counts.clears;
        deletes += counts.deletes;
        takeovers += counts.takeovers;
        joined += counts.joined;
        let store = store.lock().unwrap();
        if stored_head(&store).is_some_and(|s| s.alarm.is_some()) {
            ending_with_alarm += 1;
        }
    }
    // Guard against a vacuous pass: alarms set, entries cleared and
    // deleted, and runs that end with an alarm to check.
    assert!(alarms > seeds_run * 10, "too few alarms set ({alarms})");
    assert!(
        clears > seeds_run,
        "stale entries rarely cleared ({clears})"
    );
    assert!(deletes > seeds_run * 10, "too few deletes ({deletes})");
    assert!(
        takeovers > seeds_run * 5,
        "ownership rarely changed ({takeovers})"
    );
    assert!(
        only.is_some() || joined > seeds_run / 2,
        "joining nodes rarely uploaded their own alarm ({joined})"
    );
    assert!(
        only.is_some() || ending_with_alarm > seeds_run / 4,
        "too few runs end with an alarm to check ({ending_with_alarm})"
    );
    eprintln!(
        "alarm index: {seeds_run} seeds x 30 s: {takeovers} takeovers, {alarms} alarms set, \
         {clears} stale clears, {deletes} deletes, {joined} first uploads from joining \
         nodes; invariant held throughout; {ending_with_alarm} runs end with an alarm, \
         each still listed"
    );
}

/// Each rule is necessary: drop it and some seed loses an alarm's entry.
fn caught(rules: Rules) -> u64 {
    caught_within(rules, seeds()).0
}

/// Like `caught`, searching `budget` seeds; also returns the violation.
fn caught_within(rules: Rules, budget: u64) -> (u64, String) {
    let seed = (0..budget)
        .find(|&s| simulate(s, rules, Duration::from_secs(30)).2.is_some())
        .unwrap_or_else(|| panic!("{rules:?}: no violation within the seed budget"));
    let a = simulate(seed, rules, Duration::from_secs(30)).2;
    let b = simulate(seed, rules, Duration::from_secs(30)).2;
    assert_eq!(a, b, "seed {seed} did not reproduce");
    (seed, a.map(|(_, v)| v).unwrap_or_default())
}

#[test]
fn the_keys_before_er04_are_caught() {
    let seed = caught(Rules {
        fresh_keys: false,
        ..SAFE
    });
    eprintln!("alarm index: the keys before the fix caught at seed {seed}");
}

#[test]
fn deleting_a_newer_owners_entry_is_caught() {
    // Needs an older owner's late delete to land after a newer owner wrote
    // its entry: on node leases a takeover waits for a release or a fence,
    // so this can take tens of seeds (20 here) or more than 200 (104 and
    // 277 under other fault schedules tried).
    let (seed, _) = caught_within(
        Rules {
            generation_fence: false,
            ..SAFE
        },
        seeds().max(2_000),
    );
    eprintln!("alarm index: no generation check caught at seed {seed}");
}

#[test]
fn clearing_before_ones_own_upload_is_caught() {
    // A rare violation: about one seed in 200 shows it (2 of 400, with and
    // without the acquire's read-back), so the gate's 200 seeds found it
    // only by luck (seed 6, then seed 269 once the seed streams moved).
    // At least 2 000 seeds: some ten catches expected.
    let (seed, _) = caught_within(
        Rules {
            fence_before_clearing: false,
            ..SAFE
        },
        seeds().max(2_000),
    );
    eprintln!("alarm index: clearing without a fence caught at seed {seed}");
}

#[test]
fn uploading_a_joining_nodes_alarm_before_listing_it_is_caught() {
    let seed = caught(Rules {
        index_before_first_upload: false,
        ..SAFE
    });
    eprintln!("alarm index: a first upload before its entry caught at seed {seed}");
}

#[test]
fn clearing_when_the_bucket_had_nothing_is_caught() {
    // The bucket has nothing for the actor only at the start of a run, so
    // this needs a joining node's first upload held up right then, while
    // another node takes over and clears. On node leases that takeover
    // needs the joining node cut off and fenced (its lease keeps renewing
    // while an upload is only slow): the fault aimed at that upload.
    let (seed, violation) = caught_within(
        Rules {
            refuse_clearing_when_empty: false,
            cut_at_first_upload: true,
            ..SAFE
        },
        seeds().max(2_000),
    );
    eprintln!(
        "alarm index: clearing with nothing in the bucket caught at seed {seed}: {violation}"
    );
}

/// Under the fault aimed at a joining node's first upload, the rules keep
/// every committed alarm's entry: the case `refuse_clearing_when_empty`
/// exists for, now with the rule in place.
#[test]
fn the_aimed_first_upload_fault_loses_no_entry_with_the_rules() {
    let aimed = Rules {
        cut_at_first_upload: true,
        ..SAFE
    };
    let (mut hit, mut takeovers, mut refused) = (0, 0, 0);
    for seed in 0..seeds() {
        let (_, counts, violation) = simulate(seed, aimed, Duration::from_secs(30));
        if let Some((at, v)) = violation {
            panic!("seed {seed} at {at} ms: {v}");
        }
        hit += counts.aimed;
        takeovers += counts.takeovers;
        refused += counts.refused;
    }
    assert!(hit > seeds() / 2, "the fault was rarely aimed ({hit})");
    assert!(
        takeovers > seeds() * 5,
        "ownership rarely changed ({takeovers})"
    );
    // The rule the aimed fault is about was reached: owners that found the
    // bucket empty refused to clear.
    assert!(
        refused > seeds() / 10,
        "the empty-bucket rule was rarely reached ({refused})"
    );
    eprintln!(
        "alarm index, aimed at first uploads: {} seeds, invariant held; {hit} first uploads \
         delayed with their node cut off, {takeovers} takeovers, {refused} clears refused \
         on an empty bucket",
        seeds()
    );
}
