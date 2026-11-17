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
}

/// An actor's database state: its incarnation and the files its rows name.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Db {
    incarnation: String,
    rows: BTreeSet<u64>,
}

/// A file in the bucket: its incarnation folder and id, and the
/// incarnation of the actor that wrote it.
type Files = BTreeMap<(String, u64), String>;

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
    /// Links issued: the incarnation folder each writes to.
    links: Vec<String>,
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
            }],
            late_reset: None,
            files: Files::new(),
            records: BTreeMap::new(),
            holder: None,
            links: Vec::new(),
            next_file: 1,
            next_inc: 1,
            ages: Ages {
                judge_after: 50,
                relist_after: 100,
                last_after: 200,
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
        let db = self.head().clone();
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
        self.files
            .insert((db.incarnation.clone(), id), db.incarnation.clone());
        unsent.insert(id);
        if self.rng.chance(30) {
            let inc = db.incarnation.clone();
            self.links.push(inc);
        }
    }

    fn link_upload(&mut self) {
        if self.links.is_empty() {
            return;
        }
        let i = self.rng.below(self.links.len() as u64) as usize;
        let inc = self.links[i].clone();
        let id = self.next_file;
        self.next_file += 1;
        self.files.insert((inc.clone(), id), inc);
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
        let reset = Db {
            incarnation: self.fresh_incarnation(),
            rows: BTreeSet::new(),
        };
        self.deletes += 1;
        match self.rng.below(4) {
            // Acknowledged under a valid hold: swept.
            0 | 1 => {
                self.history.push(reset);
                self.records.insert(old.clone(), (State::Swept, self.now));
                self.remove(&old);
                self.holder = None;
            }
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

    /// `actor restore` to an earlier state of the history, as a new
    /// segment: the replacement keeps the current incarnation.
    fn restore(&mut self) {
        if self.history.len() < 2 {
            return;
        }
        let i = self.rng.below(self.history.len() as u64 - 1) as usize;
        let mut staged = self.history[i].clone();
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

    fn sweep_pass(&mut self) {
        let records: Vec<(String, (State, u64))> =
            self.records.iter().map(|(k, v)| (k.clone(), *v)).collect();
        for (inc, (state, at)) in records {
            match blob_sweep::action_at(state, self.now - at, self.ages) {
                Action::Wait => {}
                Action::Judge => {
                    // The judging hold: restore, fence, read.
                    self.lapse();
                    self.acquire();
                    let current = self.holder.as_ref().unwrap().0.incarnation.clone();
                    self.lapse();
                    match blob_sweep::judge(&inc, &current) {
                        Judged::Deleted => {
                            self.judged_deleted += 1;
                            self.records.insert(inc.clone(), (State::Swept, self.now));
                            self.remove(&inc);
                        }
                        Judged::NotDeleted => {
                            self.judged_kept += 1;
                            self.records.remove(&inc);
                        }
                    }
                }
                Action::Relist => {
                    self.remove(&inc);
                    self.records.insert(inc, (State::Listed, self.now));
                }
                Action::Finish => {
                    self.remove(&inc);
                    self.records.remove(&inc);
                }
            }
        }
    }
}

fn simulate(seed: u64, broken: Broken) -> Result<World, String> {
    let mut w = World::new(seed, broken);
    let mut written: BTreeMap<u64, String> = BTreeMap::new();
    for _ in 0..400 {
        w.now += 1 + w.rng.below(10);
        let before = w.next_file;
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
            _ => w.sweep_pass(),
        }
        for id in before..w.next_file {
            if let Some(((_, _), writer)) = w.files.iter().find(|((_, i), _)| *i == id) {
                written.insert(id, writer.clone());
            }
        }
        check(&w, &written)?;
    }
    Ok(w)
}

fn check(w: &World, written: &BTreeMap<u64, String>) -> Result<(), String> {
    let head = w.head();
    for ((folder, id), writer) in &w.files {
        if *folder == head.incarnation && *writer != head.incarnation {
            return Err(format!(
                "file {id} of incarnation {writer} is in {folder}'s namespace"
            ));
        }
    }
    for id in &head.rows {
        let named_here = written.get(id).is_some_and(|inc| *inc == head.incarnation);
        let there = w.files.contains_key(&(head.incarnation.clone(), *id));
        if named_here && !there {
            return Err(format!(
                "file {id}, named by a live row of {}, was deleted",
                head.incarnation
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
