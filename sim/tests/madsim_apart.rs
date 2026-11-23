//! ADR 0041, amendment of 2026-11-22: a room and its pusher kept apart at
//! wake-up. The real pure policy (`placement::apart`, `peer_apart`,
//! `companions`), modelled nodes, wake-ups, drains, hibernations and
//! restarts. Wake-ups run one at a time (two of a pair waking at the same
//! moment on one node is outside this model, and may share a node).
//! RUSTFLAGS='--cfg madsim' cargo test -j 4 -p wasmkeep-sim
//!   --test madsim_apart --target-dir target/madsim
#![cfg(madsim)]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use madsim::runtime::Runtime;
use std::collections::BTreeMap;
use wasmkeep_cluster::placement::{APART_NONE, apart, companions, peer_apart};
use wasmkeep_sim::rng::Rng;

const NODES: [&str; 4] = ["a", "b", "c", "d"];
const CLASSES: [&str; 2] = ["pusher", "room"];
const NAMES: [&str; 3] = ["lobby", "hall", "deck"];

fn seeds() -> u64 {
    std::env::var("WASMKEEP_SIM_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(200)
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum State {
    Serving,
    Draining,
    Down,
}

/// The cluster: each node's state, and who holds each actor.
struct World {
    nodes: Vec<(&'static str, State)>,
    held: BTreeMap<(&'static str, &'static str), &'static str>,
    declared: BTreeMap<String, String>,
    /// The policy removed: the mutation that must fail property 3.
    mutated: bool,
}

/// What one wake-up did, for the oracles.
#[derive(Debug)]
struct Wake {
    hops: u8,
    /// Where it was acquired; `None`: refused.
    at: Option<&'static str>,
    /// When it was acquired beside a companion: whether the node that
    /// decided had a peer to send it to that held no companion.
    beside_with_a_way_out: bool,
}

impl World {
    fn state(&self, node: &str) -> State {
        self.nodes.iter().find(|(n, _)| *n == node).unwrap().1
    }

    /// A drain's eligible peers seen from `me`: serving, not `me`.
    fn eligible(&self, me: &str) -> Vec<&'static str> {
        self.nodes
            .iter()
            .filter(|(n, s)| *n != me && *s == State::Serving)
            .map(|(n, _)| *n)
            .collect()
    }

    fn companions(&self, class: &str) -> Vec<String> {
        if self.mutated {
            return Vec::new();
        }
        companions(&self.declared, &CLASSES, class)
    }

    /// The nodes holding `class/name`'s companions.
    fn companion_nodes(&self, class: &str, name: &'static str) -> Vec<&'static str> {
        self.companions(class)
            .iter()
            .filter_map(|c| {
                CLASSES
                    .iter()
                    .find(|k| **k == c.as_str())
                    .and_then(|k| self.held.get(&(*k, name)).copied())
            })
            .collect()
    }

    /// A wake-up of `class/name` reaching `node`, as the server routes it:
    /// to its holder; else, on a draining node, to the drain's choice that
    /// avoids the companions' nodes; else, beside a companion held here,
    /// to `apart`'s choice; else here. The receiver of a hop acquires it,
    /// never sends it on.
    fn wake(&mut self, class: &'static str, name: &'static str, node: &'static str) -> Wake {
        if let Some(holder) = self.held.get(&(class, name)) {
            return Wake {
                hops: u8::from(*holder != node),
                at: Some(holder),
                beside_with_a_way_out: false,
            };
        }
        let key = format!("default/{class}/{name}");
        let near = self.companion_nodes(class, name);
        let eligible = self.eligible(node);
        let way_out = eligible.iter().any(|p| !near.contains(p));
        let target = match self.state(node) {
            State::Draining => peer_apart(&eligible, &key, &near),
            State::Serving => apart(near.contains(&node), &eligible, &key),
            State::Down => unreachable!(),
        };
        let (at, hops, deciding_way_out) = match target {
            Some(p) => (p, 1, eligible.iter().any(|q| !near.contains(q))),
            None => (node, 0, way_out),
        };
        self.held.insert((class, name), at);
        let beside = self.companion_nodes(class, name).contains(&at);
        Wake {
            hops,
            at: Some(at),
            beside_with_a_way_out: beside && deciding_way_out,
        }
    }
}

/// Runs one schedule. The oracles, for every wake-up:
/// 1. One hop: a wake-up is sent at most once.
/// 2. Never refused: it is acquired somewhere.
/// 3. Apart: it lands beside its companion only when the node that
///    decided had no peer without one (one node alive, every other peer
///    draining or down, or holding a companion).
fn schedule(seed: u64, declared: &[(&str, &str)], mutated: bool) -> Vec<(u8, String)> {
    let mut rng = Rng::new(seed);
    let count = 1 + rng.below(NODES.len() as u64) as usize;
    let mut w = World {
        nodes: NODES[..count]
            .iter()
            .map(|n| (*n, State::Serving))
            .collect(),
        held: BTreeMap::new(),
        declared: declared
            .iter()
            .map(|(c, o)| ((*c).to_owned(), (*o).to_owned()))
            .collect(),
        mutated,
    };
    let mut violations = Vec::new();
    for _ in 0..200 {
        match rng.below(10) {
            // A node drains (its residents released), comes back, goes
            // down (its actors taken over at their next wake-up) or starts.
            0 => {
                let i = rng.below(count as u64) as usize;
                let (node, state) = w.nodes[i];
                w.nodes[i].1 = match state {
                    State::Serving if rng.chance(50) => State::Draining,
                    State::Serving => State::Down,
                    State::Draining | State::Down => State::Serving,
                };
                if w.nodes[i].1 != State::Serving || state == State::Down {
                    w.held.retain(|_, n| *n != node);
                }
            }
            // An actor hibernates.
            1 => {
                let class = CLASSES[rng.below(2) as usize];
                let name = NAMES[rng.below(NAMES.len() as u64) as usize];
                w.held.remove(&(class, name));
            }
            // A wake-up reaches a node that is up.
            _ => {
                let up: Vec<_> = w
                    .nodes
                    .iter()
                    .filter(|(_, s)| *s != State::Down)
                    .map(|(n, _)| *n)
                    .collect();
                if up.is_empty() {
                    continue;
                }
                let node = up[rng.below(up.len() as u64) as usize];
                let class = CLASSES[rng.below(2) as usize];
                let name = NAMES[rng.below(NAMES.len() as u64) as usize];
                let wake = w.wake(class, name, node);
                if wake.hops > 1 {
                    violations.push((1, format!("{class}/{name}: {wake:?}")));
                }
                if wake.at.is_none() {
                    violations.push((2, format!("{class}/{name}: {wake:?}")));
                }
                if wake.beside_with_a_way_out {
                    violations.push((3, format!("{class}/{name} at {node}: {wake:?}")));
                }
            }
        }
    }
    violations
}

#[test]
fn the_default_pair_never_shares_a_node_it_could_avoid() {
    for seed in 0..seeds() {
        Runtime::with_seed_and_config(seed, Default::default()).block_on(async {
            let v = schedule(seed, &[], false);
            assert!(v.is_empty(), "seed {seed}: {v:?}");
        });
    }
}

#[test]
fn a_declared_pair_holds_and_turned_off_it_is_not_applied() {
    for seed in 0..seeds() {
        Runtime::with_seed_and_config(seed, Default::default()).block_on(async {
            let v = schedule(seed, &[("room", "pusher")], false);
            assert!(v.is_empty(), "seed {seed}: {v:?}");
            // Off: the pair is nobody's companion, so oracle 3 never applies.
            let v = schedule(seed, &[("pusher", APART_NONE)], false);
            assert!(v.is_empty(), "seed {seed}: {v:?}");
        });
    }
}

#[test]
fn without_the_rule_the_pair_shares_a_node_it_could_avoid() {
    // The mutation: the oracle counts companions by the real rule, the
    // routing ignores it. It must fail property 3 on some seed, and only it.
    let mut failed = 0;
    for seed in 0..seeds() {
        Runtime::with_seed_and_config(seed, Default::default()).block_on(async {
            let v = schedule_mutated(seed);
            assert!(v.iter().all(|(p, _)| *p == 3), "seed {seed}: {v:?}");
            failed += usize::from(!v.is_empty());
        });
    }
    assert!(failed > 0, "the mutation never failed property 3");
}

/// [`schedule`] with routing blind to companions but the oracle not.
fn schedule_mutated(seed: u64) -> Vec<(u8, String)> {
    let mut rng = Rng::new(seed);
    let count = 2 + rng.below(3) as usize;
    let mut w = World {
        nodes: NODES[..count]
            .iter()
            .map(|n| (*n, State::Serving))
            .collect(),
        held: BTreeMap::new(),
        declared: BTreeMap::new(),
        mutated: true,
    };
    let mut violations = Vec::new();
    for _ in 0..200 {
        let node = NODES[rng.below(count as u64) as usize];
        let class = CLASSES[rng.below(2) as usize];
        let name = NAMES[rng.below(NAMES.len() as u64) as usize];
        if rng.chance(20) {
            w.held.remove(&(class, name));
            continue;
        }
        let wake = w.wake(class, name, node);
        // The real rule's view of the result.
        w.mutated = false;
        let near = w.companion_nodes(class, name);
        let others = w.eligible(node).iter().any(|p| !near.contains(p));
        w.mutated = true;
        if wake.at.is_some_and(|at| near.contains(&at)) && others && wake.hops == 0 {
            violations.push((3, format!("{class}/{name} at {node}")));
        }
    }
    violations
}
