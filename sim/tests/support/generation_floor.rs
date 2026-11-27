//! A bounded interleaving model of ADR 0045 (the generation floor of an
//! app): every interleaving, from a small start, of first acquisitions on
//! nodes that serve through the finalize, an actor's delete and recreate,
//! the finalize itself, `cluster markers clean`, and whole-app restores in
//! place (at a quiet point, nodes started again after it). The decisions
//! are the crate's own (`wasmkeep_cluster::generation_floor`: the floor's
//! merge, the seed at the floor, the cleanup's judgement and its raise);
//! the steps between them are the reads and writes the server and the CLI
//! make, one bucket request each, in their order.
//!
//! Checked at every ownership record written: its generation is above
//! every generation the actor's history ever carried, deleted history
//! included (ADR 0045, Context). Each mutation must be caught.
#![allow(dead_code)] // each including suite runs its own scenarios

use std::collections::HashSet;

use wasmkeep_cluster::generation_floor::{
    self as gf, CleanPlan, GenerationFloor, Leftover, MarkerRead, Markers,
};
use wasmkeep_cluster::lease::{Clock, LeaseConfig};

/// A rule broken on purpose; the model must catch each.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum Mutation {
    None,
    /// The cleanup deletes the markers before it raises the floor.
    DeleteBeforeRaise,
    /// A node reads the upgrade floor before the marker.
    UpgradeFloorFirst,
    /// A node at the floor reads the generation floor before the marker.
    GenerationFloorFirst,
    /// A node keeps the upgrade floor it read at its start.
    StartFloor,
    /// A restore in place does not read the generation floor it clears.
    RestoreForgetsFloor,
}

/// One actor's objects.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
struct Actor {
    marker: Option<u64>,
    record: Option<u64>,
    /// The highest generation its history keys carry now (0: none).
    keys: u64,
    /// The highest generation its history ever carried.
    ever: u64,
    /// The head generation of the backup a restore copies (0: none).
    plan: u64,
}

/// One step of a node's program.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum Task {
    /// An acquisition of actor `n`.
    Acquire(usize),
    /// Actor `n` deletes itself: its history goes, its record stays (ADR
    /// 0028). Only once this node holds it.
    Delete(usize),
}

/// Where an acquisition is, and what it read.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
struct Acq {
    pc: u8,
    keys: u64,
    marker: Option<u64>,
    marker_read: bool,
    at: Option<bool>,
    floor: Option<(u64, bool)>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct Node {
    tasks: Vec<Task>,
    next: usize,
    acq: Option<Acq>,
    /// The upgrade floor this node knows is 0.10.
    known: bool,
    /// It read `markers none`.
    markers_none: bool,
    /// The actors it holds.
    holds: Vec<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct Clean {
    pc: u8,
    floor: (u64, bool),
    listed: Vec<usize>,
    judged: Vec<(usize, u64, Option<u64>)>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct State {
    finalized: bool,
    floor: (u64, bool),
    actors: Vec<Actor>,
    nodes: Vec<Node>,
    clean: Option<Clean>,
    cleaned: bool,
    restores: u8,
}

/// An actor at the start: marker, record, keys, ever, plan.
pub(crate) type ActorStart = (Option<u64>, Option<u64>, u64, u64, u64);

/// What a scenario starts from.
#[derive(Debug, Clone)]
pub(crate) struct Start {
    pub(crate) finalized: bool,
    /// (generation, markers none).
    pub(crate) floor: (u64, bool),
    /// Per actor: marker, record, keys, ever, plan.
    pub(crate) actors: Vec<ActorStart>,
    pub(crate) nodes: Vec<Vec<Task>>,
    /// Restores in place allowed.
    pub(crate) restores: u8,
    /// A cleanup is run once the floor is 0.10.
    pub(crate) clean: bool,
}

/// What every interleaving of a scenario reached.
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct Reached {
    pub(crate) states: usize,
    pub(crate) records: usize,
    /// First records seeded from the generation floor alone (no marker,
    /// the floor above the keys).
    pub(crate) from_floor: usize,
    /// Records written after the actor deleted itself.
    pub(crate) recreated: usize,
    /// Markers deleted by a cleanup that were the only carrier.
    pub(crate) folded: usize,
    pub(crate) restored: usize,
}

/// Broken: a record at `generation` for an actor whose history carried
/// `ever`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Reused {
    pub(crate) generation: u64,
    pub(crate) ever: u64,
}

struct Never;
impl Clock for Never {
    fn now_ms(&self) -> u64 {
        0
    }
}

fn to_floor((generation, none): (u64, bool)) -> GenerationFloor {
    GenerationFloor {
        generation,
        markers: if none { Markers::None } else { Markers::Kept },
    }
}

fn of_floor(f: GenerationFloor) -> (u64, bool) {
    (f.generation, f.markers == Markers::None)
}

fn marker_read(m: Option<u64>) -> MarkerRead {
    m.map_or(MarkerRead::Absent, MarkerRead::Marker)
}

fn config() -> LeaseConfig {
    LeaseConfig::new(10_000, 5_000, 2_000, 1_000).unwrap()
}

/// Explores every interleaving of `start` under `mutation`.
///
/// # Errors
/// The first reused generation found.
pub(crate) fn explore(start: &Start, mutation: Mutation) -> Result<Reached, Reused> {
    let state = State {
        finalized: start.finalized,
        floor: start.floor,
        actors: start
            .actors
            .iter()
            .map(|&(marker, record, keys, ever, plan)| Actor {
                marker,
                record,
                keys,
                ever,
                plan,
            })
            .collect(),
        nodes: start
            .nodes
            .iter()
            .map(|tasks| Node {
                tasks: tasks.clone(),
                next: 0,
                acq: None,
                known: start.finalized,
                markers_none: false,
                holds: Vec::new(),
            })
            .collect(),
        clean: None,
        cleaned: !start.clean,
        restores: start.restores,
    };
    let mut seen = HashSet::new();
    let mut reached = Reached::default();
    let mut stack = vec![state];
    while let Some(s) = stack.pop() {
        if !seen.insert(s.clone()) {
            continue;
        }
        reached.states += 1;
        for next in successors(&s, mutation, &mut reached)? {
            stack.push(next);
        }
    }
    Ok(reached)
}

fn successors(s: &State, m: Mutation, r: &mut Reached) -> Result<Vec<State>, Reused> {
    let mut out = Vec::new();
    for i in 0..s.nodes.len() {
        if let Some(next) = node_step(s, i, m, r)? {
            out.push(next);
        }
    }
    if !s.finalized {
        let mut n = s.clone();
        n.finalized = true;
        out.push(n);
    }
    if let Some(next) = clean_step(s, m, r) {
        out.push(next);
    }
    let quiet = s.nodes.iter().all(|n| n.acq.is_none()) && s.clean.is_none();
    if s.restores > 0 && quiet {
        out.push(restore(s, m, r));
    }
    Ok(out)
}

/// The order of an acquisition's reads after the record and the keys:
/// `M` marker, `U` upgrade floor, `G` generation floor (only at the
/// floor), then the decision.
fn order(m: Mutation) -> &'static [u8] {
    match m {
        Mutation::UpgradeFloorFirst => b"UMGD",
        Mutation::GenerationFloorFirst => b"UGMD",
        _ => b"MUGD",
    }
}

/// Writes actor `x`'s record at `generation`, checked.
fn record(n: &mut State, x: usize, generation: u64, r: &mut Reached) -> Result<(), Reused> {
    let a = &mut n.actors[x];
    if generation <= a.ever {
        return Err(Reused {
            generation,
            ever: a.ever,
        });
    }
    a.record = Some(generation);
    // The holder writes its history at its generation.
    a.keys = generation;
    a.ever = generation;
    r.records += 1;
    Ok(())
}

fn node_step(s: &State, i: usize, m: Mutation, r: &mut Reached) -> Result<Option<State>, Reused> {
    let node = &s.nodes[i];
    let Some(&task) = node.tasks.get(node.next) else {
        return Ok(None);
    };
    let mut n = s.clone();
    match task {
        Task::Delete(x) => {
            if !node.holds.contains(&x) {
                // Never acquired here: nothing to delete.
                n.nodes[i].next += 1;
                return Ok(Some(n));
            }
            n.actors[x].keys = 0;
            n.nodes[i].next += 1;
            Ok(Some(n))
        }
        Task::Acquire(x) => {
            let mut acq = node.acq.unwrap_or_default();
            let finish = |n: &mut State| {
                n.nodes[i].acq = None;
                n.nodes[i].next += 1;
            };
            match acq.pc {
                // The record: one there is acquired from (generation + 1).
                0 => {
                    if let Some(rec) = s.actors[x].record {
                        let deleted = s.actors[x].keys == 0;
                        record(&mut n, x, rec + 1, r)?;
                        if deleted {
                            r.recreated += 1;
                        }
                        if !n.nodes[i].holds.contains(&x) {
                            n.nodes[i].holds.push(x);
                        }
                        finish(&mut n);
                        return Ok(Some(n));
                    }
                    acq.pc = 1;
                }
                // The listing of its keys.
                1 => {
                    acq.keys = s.actors[x].keys;
                    acq.pc = 2;
                }
                _ => {
                    let step = order(m)[usize::from(acq.pc - 2)];
                    acq.pc += 1;
                    match step {
                        b'M' => {
                            if !node.markers_none {
                                acq.marker = s.actors[x].marker;
                            }
                            acq.marker_read = true;
                        }
                        b'U' => {
                            let at = if m == Mutation::StartFloor || node.known {
                                node.known
                            } else {
                                s.finalized
                            };
                            n.nodes[i].known |= at && m != Mutation::StartFloor;
                            acq.at = Some(at);
                        }
                        b'G' => {
                            if acq.at == Some(true) {
                                acq.floor = Some(s.floor);
                            }
                        }
                        _ => {
                            // The decision, and the record's create.
                            let read = marker_read(acq.marker);
                            let seed = if acq.at == Some(true) {
                                let floor = to_floor(acq.floor.unwrap_or((0, false)));
                                if floor.markers == Markers::None {
                                    n.nodes[i].markers_none = true;
                                }
                                let seed =
                                    gf::seed_above(&Never, &config(), &read, acq.keys, floor)
                                        .unwrap();
                                if acq.marker.is_none() && floor.generation > acq.keys {
                                    r.from_floor += 1;
                                }
                                seed
                            } else {
                                // 0.9's path: create the marker if it was
                                // absent (a conflict ends the acquisition).
                                match acq.marker {
                                    None if s.actors[x].marker.is_some() => {
                                        finish(&mut n);
                                        return Ok(Some(n));
                                    }
                                    None => n.actors[x].marker = Some(acq.keys),
                                    Some(_) => {}
                                }
                                read.generation().max(acq.keys)
                            };
                            if s.actors[x].record.is_some() {
                                // Another node created it meanwhile.
                                finish(&mut n);
                                return Ok(Some(n));
                            }
                            record(&mut n, x, seed + 1, r)?;
                            n.nodes[i].holds.push(x);
                            finish(&mut n);
                            return Ok(Some(n));
                        }
                    }
                }
            }
            n.nodes[i].acq = Some(acq);
            Ok(Some(n))
        }
    }
}

fn clean_step(s: &State, m: Mutation, r: &mut Reached) -> Option<State> {
    if s.cleaned || !s.finalized {
        return None;
    }
    let mut n = s.clone();
    let Some(c) = &s.clean else {
        // The run starts: the generation floor's read.
        n.clean = Some(Clean {
            pc: 0,
            floor: s.floor,
            listed: Vec::new(),
            judged: Vec::new(),
        });
        return Some(n);
    };
    let mut c = c.clone();
    let plan = |c: &Clean| CleanPlan {
        floor_key: String::new(),
        floor: to_floor(c.floor),
        leftovers: c
            .judged
            .iter()
            .map(|&(x, g, rec)| gf::judge(x.to_string(), g, rec))
            .collect(),
    };
    let raise = |n: &mut State, c: &Clean| {
        if let Some(to) = plan(c).raise_to() {
            n.floor = of_floor(to_floor(n.floor).max(GenerationFloor {
                generation: to,
                markers: Markers::Kept,
            }));
        }
    };
    let delete = |n: &mut State, c: &Clean, r: &mut Reached| {
        for l in plan(c).leftovers {
            let (Leftover::Carried { key, .. } | Leftover::Folded { key, .. }) = &l;
            let x: usize = key.parse().unwrap();
            n.actors[x].marker = None;
            if matches!(l, Leftover::Folded { .. }) {
                r.folded += 1;
            }
        }
    };
    match c.pc {
        // The listing of `leases/`.
        0 => {
            c.listed = (0..s.actors.len())
                .filter(|&x| s.actors[x].marker.is_some())
                .collect();
            c.pc = 1;
        }
        // Each marker, then its record, one read each (as one step: the
        // record is read after the marker).
        1 => {
            if let Some(x) = c.listed.pop() {
                if let Some(g) = s.actors[x].marker {
                    c.judged.push((x, g, s.actors[x].record));
                }
            } else {
                c.pc = 2;
            }
        }
        2 => {
            if m == Mutation::DeleteBeforeRaise {
                delete(&mut n, &c, r);
            } else {
                raise(&mut n, &c);
            }
            c.pc = 3;
        }
        3 => {
            if m == Mutation::DeleteBeforeRaise {
                raise(&mut n, &c);
            } else {
                delete(&mut n, &c, r);
            }
            c.pc = 4;
        }
        // The listing after the deletes: empty, `markers none`.
        _ => {
            if n.actors.iter().all(|a| a.marker.is_none()) {
                n.floor = of_floor(to_floor(n.floor).max(GenerationFloor {
                    generation: 0,
                    markers: Markers::None,
                }));
            }
            n.clean = None;
            n.cleaned = true;
            return Some(n);
        }
    }
    n.clean = Some(c);
    Some(n)
}

/// A whole-app restore in place, at a quiet point: collects every actor's
/// highest generation (keys, record, marker) and the generation floor,
/// clears the state, copies the backup's plan, and writes the generation
/// floor at 0.10 or one marker per actor below. The nodes start again
/// after it (their floors read anew), except one that keeps its start
/// floor.
fn restore(s: &State, m: Mutation, r: &mut Reached) -> State {
    let mut n = s.clone();
    n.restores -= 1;
    r.restored += 1;
    let kept = if m == Mutation::RestoreForgetsFloor {
        0
    } else {
        s.floor.0
    };
    let seeds: Vec<u64> = s
        .actors
        .iter()
        .map(|a| {
            a.keys
                .max(a.record.unwrap_or(0))
                .max(a.marker.unwrap_or(0))
                .max(a.plan)
                + 1
        })
        .collect();
    for a in &mut n.actors {
        a.marker = None;
        a.record = None;
        a.keys = a.plan;
    }
    n.floor = (0, false);
    if s.finalized {
        let mut generation = seeds.iter().copied().max().unwrap_or(0);
        if s.floor != (0, false) {
            generation = generation.max(kept + 1);
        }
        n.floor = (generation, true);
    } else {
        for (a, seed) in n.actors.iter_mut().zip(seeds) {
            a.marker = Some(seed);
        }
    }
    for node in &mut n.nodes {
        if m != Mutation::StartFloor {
            node.known = s.finalized;
        }
        node.markers_none = false;
        node.holds.clear();
    }
    n
}
