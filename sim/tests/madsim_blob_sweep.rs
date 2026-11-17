//! Deleted actors' files ([ADR 0030], section 5), as a seeded model of the
//! server's rules: one actor address, holders that come and go (a hold
//! lapses at any step, the next holder restores the bucket's head and
//! fences), events that write files and the rows naming them (rows durable
//! only when uploaded), deletes (the sweep record before the reset, the
//! reset uploaded, acknowledged, lost, or landing late from a deposed
//! holder until the next holder's fence), browsers uploading through links
//! long after they were issued, `actor restore` of an earlier state, and
//! the sweep pass (`wasmkeep_cluster::blob_sweep`'s ages, judgement and
//! folders) judging `pending` records from a fenced restore.
//!
//! Checked at every step:
//! - **never reach the next incarnation**: every file the actor can see
//!   (under the incarnation its state names) was written while the actor
//!   was at that incarnation;
//! - **no live file deleted**: every file a row of the bucket's state
//!   names, under the incarnation that state names, is still there.
//!
//! The correct run must delete, judge both ways and sweep. Three broken
//! variants must be caught on some seed: no incarnation (every delete
//! keeps `0`), `swept` written on a reset sent but not acknowledged, and
//! a restore that brings back the staged database's incarnation instead
//! of keeping the current one.
//!
//! [ADR 0030]: ../../docs/adr/0030-files-and-static-assets.md
//!
//! `RUSTFLAGS="--cfg madsim" cargo test -p wasmkeep-sim --target-dir target/madsim`

#![cfg(madsim)]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code

use std::collections::{BTreeMap, BTreeSet};

use wasmkeep_cluster::blob_sweep::{self, Action, Ages, Judged, State};
use wasmkeep_sim::rng::Rng;

/// What is broken, for the variants that must be caught.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Broken {
    None,
    NoIncarnation,
    SweptOnSent,
    StagedIncarnation,
    /// Relist and Finish remove the namespace without judging again, and a
    /// database a node without `blobs` reset runs at `0` (both, as before
    /// the review: either alone is covered by the other rule).
    NoRejudge,
    /// A database a node without `blobs` reset runs at `0`.
    ZeroAfterOldDelete,
}

/// An actor's database state: its incarnation (`0` also when absent), the
/// files its rows name, whether a delete reset it, and which life of the
/// address it is (each delete starts the next).
#[derive(Debug, Clone, PartialEq, Eq)]
struct Db {
    incarnation: String,
    rows: BTreeSet<u64>,
    deleted: bool,
    life: u64,
}

/// A file in the bucket: its incarnation folder and id, and the life of
/// the actor that wrote it (or issued the link).
type Files = BTreeMap<(String, u64), u64>;

struct World {
    rng: Rng,
    broken: Broken,
    now: u64,
    /// The bucket's history: acknowledged states, last is the head.
    history: Vec<Db>,
    /// A reset sent by a deposed holder that may still land.
    late_reset: Option<Db>,
    files: Files,
    records: BTreeMap<String, (State, u64)>,
    /// The current holder: its restored state, its unsent rows.
    holder: Option<(Db, BTreeSet<u64>)>,
    /// Links issued: the incarnation folder each writes to, and its life.
    links: Vec<(String, u64)>,
    /// Every file ever written: its folder and life.
    written: BTreeMap<u64, (String, u64)>,
    next_file: u64,
    next_inc: u64,
    ages: Ages,
    // What happened, for the correct run's own checks.
    deletes: u32,
    judged_deleted: u32,
    judged_kept: u32,
    swept: u32,
}

impl World {
    fn new(seed: u64, broken: Broken) -> Self {
        Self {
            rng: Rng::new(seed),
            broken,
            now: 0,
            history: vec![Db {
                incarnation: blob_sweep::FIRST.into(),
                rows: BTreeSet::new(),
                deleted: false,
                life: 0,
            }],
            late_reset: None,
            files: Files::new(),
            records: BTreeMap::new(),
            holder: None,
            links: Vec::new(),
            written: BTreeMap::new(),
            next_file: 1,
            next_inc: 1,
            ages: Ages {
                judge_after: 50,
                relist_after: 100,
                last_after: 200,
                age_out: 400,
            },
            deletes: 0,
            judged_deleted: 0,
            judged_kept: 0,
            swept: 0,
        }
    }

    fn head(&self) -> &Db {
        self.history.last().unwrap()
    }

    /// The incarnation an instance of `db` uses (`None`: it draws a fresh
    /// one), by the server's rule.
    fn effective(&self, db: &Db) -> Option<String> {
        if self.broken == Broken::NoIncarnation {
            return Some(blob_sweep::FIRST.into());
        }
        let replaced = db.deleted.then_some("delete 1 1 1");
        let absent = db.incarnation == blob_sweep::FIRST;
        match blob_sweep::start((!absent).then_some(db.incarnation.as_str()), replaced).unwrap() {
            blob_sweep::Start::Use(i) => Some(i),
            blob_sweep::Start::Draw
                if matches!(self.broken, Broken::ZeroAfterOldDelete | Broken::NoRejudge) =>
            {
                Some(blob_sweep::FIRST.into())
            }
            blob_sweep::Start::Draw => None,
        }
    }

    fn fresh_incarnation(&mut self) -> String {
        if self.broken == Broken::NoIncarnation {
            return blob_sweep::FIRST.into();
        }
        self.next_inc += 1;
        format!("{:016x}", self.next_inc)
    }

    /// A new hold: what a late reset can still do lands first or never
    /// (the fence), then the head is restored.
    fn acquire(&mut self) {
        if let Some(late) = self.late_reset.take()
            && self.rng.chance(50)
        {
            self.history.push(late);
        }
        let mut db = self.head().clone();
        // The instance's start: a fresh incarnation, written into the
        // database (uploaded with its next change), if it must draw one.
        match self.effective(&db) {
            Some(i) => db.incarnation = i,
            None => db.incarnation = self.fresh_incarnation(),
        }
        self.holder = Some((db, BTreeSet::new()));
    }

    fn lapse(&mut self) {
        // Unsent rows are lost with the hold.
        self.holder = None;
    }

    fn upload_rows(&mut self) {
        if let Some((db, unsent)) = self.holder.as_mut()
            && !unsent.is_empty()
        {
            db.rows.extend(unsent.iter().copied());
            unsent.clear();
            let db = db.clone();
            self.history.push(db);
        }
    }

    fn write_file(&mut self) {
        let Some((db, unsent)) = self.holder.as_mut() else {
            return;
        };
        let id = self.next_file;
        self.next_file += 1;
        // The file first, durable at once; its row after.
        self.files.insert((db.incarnation.clone(), id), db.life);
        self.written.insert(id, (db.incarnation.clone(), db.life));
        unsent.insert(id);
        if self.rng.chance(30) {
            let link = (db.incarnation.clone(), db.life);
            self.links.push(link);
        }
    }

    /// A delete by a node without `blobs` (mid-upgrade, or downgraded): its
    /// reset carries no incarnation and it writes no record.
    fn old_delete(&mut self) {
        if self.holder.is_none() {
            return;
        }
        self.upload_rows();
        let life = self.holder.as_ref().unwrap().0.life + 1;
        self.history.push(Db {
            incarnation: blob_sweep::FIRST.into(),
            rows: BTreeSet::new(),
            deleted: true,
            life,
        });
        self.holder = None;
    }

    fn link_upload(&mut self) {
        if self.links.is_empty() {
            return;
        }
        let i = self.rng.below(self.links.len() as u64) as usize;
        let (inc, life) = self.links[i].clone();
        let id = self.next_file;
        self.next_file += 1;
        self.written.insert(id, (inc.clone(), life));
        self.files.insert((inc, id), life);
    }

    fn delete(&mut self) {
        if self.holder.is_none() {
            return;
        }
        // A batched holder flushes first; failing that, no record.
        if self.rng.chance(10) {
            self.lapse();
            return;
        }
        self.upload_rows();
        let old = self.holder.as_ref().unwrap().0.incarnation.clone();
        self.records.insert(old.clone(), (State::Pending, self.now));
        let life = self.holder.as_ref().unwrap().0.life + 1;
        let reset = Db {
            incarnation: self.fresh_incarnation(),
            rows: BTreeSet::new(),
            deleted: true,
            life,
        };
        self.deletes += 1;
        match self.rng.below(4) {
            // Acknowledged under a valid hold: swept.
            0 | 1 => self.acknowledged(&old, reset),
            // Sent, the hold lapsed before the answer: it may land later.
            2 => {
                if self.broken == Broken::SweptOnSent {
                    self.records.insert(old.clone(), (State::Swept, self.now));
                    self.remove(&old);
                }
                self.late_reset = Some(reset);
                self.holder = None;
            }
            // Failed: nothing landed.
            _ => {
                if self.broken == Broken::SweptOnSent {
                    self.records.insert(old.clone(), (State::Swept, self.now));
                    self.remove(&old);
                }
                self.holder = None;
            }
        }
    }

    /// A delete's reset acknowledged under a valid hold: the record
    /// `swept`, the old namespace removed.
    fn acknowledged(&mut self, old: &str, reset: Db) {
        self.history.push(reset);
        self.records
            .insert(old.to_owned(), (State::Swept, self.now));
        self.remove(old);
        self.holder = None;
    }

    /// A delete by a node with `blobs` that the bucket acknowledged.
    fn delete_now(&mut self) {
        self.upload_rows();
        let (old, life) = {
            let db = &self.holder.as_ref().unwrap().0;
            (db.incarnation.clone(), db.life + 1)
        };
        self.records.insert(old.clone(), (State::Pending, self.now));
        let reset = Db {
            incarnation: self.fresh_incarnation(),
            rows: BTreeSet::new(),
            deleted: true,
            life,
        };
        self.deletes += 1;
        self.acknowledged(&old, reset);
    }

    /// `actor restore` to an earlier state of the history, as a new
    /// segment: the replacement keeps the current incarnation.
    fn restore(&mut self) {
        if self.history.len() < 2 {
            return;
        }
        let i = self.rng.below(self.history.len() as u64 - 1) as usize;
        let mut staged = self.history[i].clone();
        // The current actor, rewound: its rows, its life and its marks.
        staged.life = self.head().life;
        staged.deleted = self.head().deleted;
        if self.broken != Broken::StagedIncarnation {
            staged.incarnation = self.head().incarnation.clone();
        }
        // Rows of another incarnation name files that were swept: the
        // restored rows are kept as data, as the docs say (checked with
        // `head`), and only the incarnation decides what is reachable.
        self.history.push(staged);
        self.holder = None;
    }

    fn remove(&mut self, incarnation: &str) {
        self.files.retain(|(inc, _), _| inc != incarnation);
        self.swept += 1;
    }

    /// The judging hold: restore, fence, read the incarnation an instance
    /// would use (`None`: a fresh one).
    fn judge(&mut self, recorded: &str) -> Judged {
        self.lapse();
        if let Some(late) = self.late_reset.take()
            && self.rng.chance(50)
        {
            self.history.push(late);
        }
        let head = self.head().clone();
        blob_sweep::judge(recorded, self.effective(&head).as_deref())
    }

    fn sweep_pass(&mut self) {
        let records: Vec<(String, (State, u64))> =
            self.records.iter().map(|(k, v)| (k.clone(), *v)).collect();
        for (inc, (state, at)) in records {
            let age = self.now - at;
            let action = blob_sweep::action_at(state, age, self.ages);
            if action == Action::Wait {
                continue;
            }
            let judged = if self.broken == Broken::NoRejudge && action != Action::Judge {
                Judged::Deleted
            } else {
                self.judge(&inc)
            };
            match blob_sweep::outcome(action, Some(judged), age, self.ages) {
                blob_sweep::Outcome::Sweep(then) => {
                    if action == Action::Judge {
                        self.judged_deleted += 1;
                    }
                    self.remove(&inc);
                    match then {
                        Some(state) => {
                            self.records.insert(inc, (state, self.now));
                        }
                        None => {
                            self.records.remove(&inc);
                        }
                    }
                }
                blob_sweep::Outcome::Drop => {
                    self.judged_kept += 1;
                    self.records.remove(&inc);
                }
                blob_sweep::Outcome::Skip | blob_sweep::Outcome::AgeOut => {}
            }
        }
    }
}

fn simulate(seed: u64, broken: Broken) -> Result<World, String> {
    let mut w = World::new(seed, broken);
    for _ in 0..400 {
        w.now += 1 + w.rng.below(10);
        match w.rng.below(20) {
            0..=1 => w.acquire(),
            2 => w.lapse(),
            3..=7 => {
                if w.holder.is_none() {
                    w.acquire();
                }
                w.write_file();
            }
            8..=9 => w.upload_rows(),
            10 => w.delete(),
            11..=13 => w.link_upload(),
            14 => w.restore(),
            15 => w.old_delete(),
            _ => w.sweep_pass(),
        }
        check(&w)?;
    }
    Ok(w)
}

/// Every file a row file-id once named, with the folder and life it was
/// written under: kept across sweeps (`files` loses entries).
fn check(w: &World) -> Result<(), String> {
    let head = w.head();
    let Some(current) = w.effective(head) else {
        // The next instance draws a fresh incarnation: nothing is reachable.
        return Ok(());
    };
    // A: what the actor reaches is its own life's.
    for ((folder, id), life) in &w.files {
        if *folder == current && *life != head.life {
            return Err(format!(
                "file {id} of life {life} is in the namespace of life {} ({folder})",
                head.life
            ));
        }
    }
    // B: what its rows name, written in its life under its incarnation, is there.
    for id in &head.rows {
        if let Some(life) = w.written.get(id)
            && life.0 == current
            && life.1 == head.life
            && !w.files.contains_key(&(current.clone(), *id))
        {
            return Err(format!(
                "file {id}, named by a live row of {current}, was deleted"
            ));
        }
    }
    Ok(())
}

fn seeds() -> u64 {
    std::env::var("WASMKEEP_SIM_SEEDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(500)
}

#[test]
fn deleted_actors_files_never_reach_the_next_incarnation() {
    let (mut deletes, mut deleted, mut kept, mut swept) = (0, 0, 0, 0);
    for seed in 0..seeds() {
        let w = simulate(seed, Broken::None).unwrap_or_else(|e| panic!("seed {seed}: {e}"));
        deletes += w.deletes;
        deleted += w.judged_deleted;
        kept += w.judged_kept;
        swept += w.swept;
    }
    // The run exercised what it checks.
    assert!(
        deletes > 0 && swept > 0,
        "deletes {deletes}, sweeps {swept}"
    );
    assert!(
        seeds() < 50 || (deleted > 0 && kept > 0),
        "judged deleted {deleted}, kept {kept}"
    );
}

#[test]
fn each_broken_variant_is_caught() {
    for broken in [
        Broken::NoIncarnation,
        Broken::SweptOnSent,
        Broken::StagedIncarnation,
        Broken::NoRejudge,
        Broken::ZeroAfterOldDelete,
    ] {
        let caught = (0..seeds()).any(|seed| simulate(seed, broken).is_err());
        assert!(caught, "{broken:?} was never caught");
    }
}

#[test]
fn a_seed_replays() {
    let (a, b) = (
        simulate(7, Broken::None).unwrap(),
        simulate(7, Broken::None).unwrap(),
    );
    assert_eq!(
        (a.deletes, a.swept, a.files.len()),
        (b.deletes, b.swept, b.files.len())
    );
}

/// The review's scenario (2026-11-17), step by step: a node with `blobs`
/// deletes X (`0` becomes I1, the record for `0` is `swept`); a node
/// without `blobs` deletes X again (its reset carries no incarnation); X
/// then writes files and rows; the pass relists and finishes the record
/// for `0`. No live file goes, and none of an earlier life is reachable.
/// The code as it was (`NoRejudge`) removes the live files, and running
/// at `0` after the old node's delete (`ZeroAfterOldDelete`, the
/// downgrade case) reaches an earlier life's late upload.
#[test]
fn an_incarnation_coming_back_never_loses_a_live_file() {
    let scenario = |broken| -> Result<World, String> {
        let mut w = World::new(1, broken);
        w.acquire();
        w.write_file();
        w.links.clear();
        w.links.push((blob_sweep::FIRST.into(), 0));
        w.upload_rows();
        w.delete_now();
        w.now += 1;
        w.acquire();
        w.old_delete();
        // A late upload through the first life's link lands under `0`.
        w.link_upload();
        w.acquire();
        w.write_file();
        w.upload_rows();
        check(&w)?;
        for _ in 0..2 {
            w.now += w.ages.last_after + 1;
            w.sweep_pass();
            check(&w)?;
        }
        Ok(w)
    };
    scenario(Broken::None).unwrap();
    let e = scenario(Broken::NoRejudge).err().expect("NoRejudge caught");
    assert!(e.contains("was deleted") || e.contains("of life"), "{e}");
    let e = scenario(Broken::ZeroAfterOldDelete)
        .err()
        .expect("ZeroAfterOldDelete caught");
    assert!(e.contains("of life 0"), "{e}");
}
