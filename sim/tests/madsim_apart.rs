//! ADR 0041, amendment of 2026-11-22: a room and its pusher kept apart at
//! wake-up. A model of the server's decision around the real pure policy
//! (`placement::companion`, `apart`, `peer_apart`): nodes that drain, die
//! and come back, actors that hibernate, wake-ups whose route read answers
//! "nobody holds it", "another node holds it" or "try here" (a record
//! naming a dead node), and alarms handed to a peer while the companion
//! may move before they fire. It checks the two things the rule promises
//! beyond what it is built from, each with a mutation that must fail it:
//!
//! 1. Bounded: an alarm is handed on at most once; the receiver acquires.
//!    Mutation: the receiver judges a handed alarm again (it bounces when
//!    the companion moved to it in flight).
//! 2. Apart: a wake-up lands beside its companion only when the rule did
//!    not apply (the route read was not "nobody", or the alarm was handed)
//!    or the deciding node had no peer without the companion. Mutation:
//!    routing blind to the rule.
//!
//! What it does not check: wake-ups of a pair at the same moment on one
//! node (sequential here), leases (the real ones are the node-lease
//! simulation's), and the hop's transport.
//! RUSTFLAGS='--cfg madsim' cargo test -j 4 -p wasmkeep-sim
//!   --test madsim_apart --target-dir target/madsim
#![cfg(madsim)]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use madsim::runtime::Runtime;
use std::collections::BTreeMap;
use wasmkeep_cluster::placement::{apart, companion, peer_apart};
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

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Mutation {
    None,
    /// A handed alarm is judged again on its receiver.
    Rejudge,
    /// Routing ignores the rule (the oracle does not).
    Blind,
}

type Actor = (&'static str, &'static str);

struct World {
    nodes: Vec<(&'static str, State)>,
    /// Who the record names: a live holder, or a dead one (until taken).
    held: BTreeMap<Actor, &'static str>,
    mutation: Mutation,
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

    /// The live node holding `actor`'s companion, if any.
    fn companion_node(&self, (class, name): Actor) -> Option<&'static str> {
        let other = companion(&CLASSES, class)?;
        let other = CLASSES.into_iter().find(|c| *c == other)?;
        self.held
            .get(&(other, name))
            .copied()
            .filter(|n| self.state(n) != State::Down)
    }

    /// Where the rule sends a wake-up of `actor` deciding on `node`, the
    /// route read having answered "nobody holds it".
    fn decide(&self, actor: Actor, node: &'static str) -> Option<&'static str> {
        if self.mutation == Mutation::Blind {
            return None;
        }
        let key = format!("default/{}/{}", actor.0, actor.1);
        let eligible = self.eligible(node);
        let near: Vec<&str> = self.companion_node(actor).into_iter().collect();
        match self.state(node) {
            State::Draining => peer_apart(&eligible, &key, &near),
            State::Serving => apart(near.contains(&node), &eligible, &key),
            State::Down => unreachable!(),
        }
    }

    /// Whether `node` deciding for `actor` had a peer without the
    /// companion.
    fn way_out(&self, actor: Actor, node: &str) -> bool {
        let near = self.companion_node(actor);
        self.eligible(node).iter().any(|p| Some(*p) != near)
    }
}

/// One schedule; the violations of oracles 1 and 2, by number.
fn schedule(seed: u64, mutation: Mutation) -> Vec<(u8, String)> {
    let mut rng = Rng::new(seed);
    let count = 2 + rng.below(3) as usize;
    let mut w = World {
        nodes: NODES[..count]
            .iter()
            .map(|n| (*n, State::Serving))
            .collect(),
        held: BTreeMap::new(),
        mutation,
    };
    let mut violations = Vec::new();
    let pick = |rng: &mut Rng| -> Actor {
        (
            CLASSES[rng.below(2) as usize],
            NAMES[rng.below(NAMES.len() as u64) as usize],
        )
    };
    for _ in 0..300 {
        let up: Vec<&'static str> = w
            .nodes
            .iter()
            .filter(|(_, s)| *s != State::Down)
            .map(|(n, _)| *n)
            .collect();
        match rng.below(12) {
            // A node drains (its residents released), dies (its records
            // left naming it), or comes back.
            0 => {
                let i = rng.below(count as u64) as usize;
                let (node, state) = w.nodes[i];
                w.nodes[i].1 = match state {
                    State::Serving if rng.chance(50) => State::Draining,
                    State::Serving => State::Down,
                    State::Draining | State::Down => State::Serving,
                };
                if w.nodes[i].1 == State::Draining {
                    w.held.retain(|_, n| *n != node);
                }
            }
            // An actor hibernates: its record names nobody.
            1 => {
                let actor = pick(&mut rng);
                if w.held
                    .get(&actor)
                    .is_some_and(|n| w.state(n) != State::Down)
                {
                    w.held.remove(&actor);
                }
            }
            // An alarm of an actor nobody holds comes due on a node, which
            // may hand it to a peer; before it fires there, the companion
            // may hibernate and wake on that peer.
            2 | 3 => {
                let Some(&node) = up.get(rng.below(up.len().max(1) as u64) as usize) else {
                    continue;
                };
                let actor = pick(&mut rng);
                if w.held.contains_key(&actor) {
                    continue;
                }
                let mut at = node;
                let mut hops = 0u8;
                let mut handed = false;
                loop {
                    let to = if handed && mutation != Mutation::Rejudge {
                        None
                    } else {
                        w.decide(actor, at)
                    };
                    let Some(to) = to else { break };
                    hops += 1;
                    if !handed && rng.chance(30) {
                        // In flight: the companion moves to the receiver.
                        let other = companion(&CLASSES, actor.0)
                            .and_then(|o| CLASSES.into_iter().find(|c| *c == o));
                        if let Some(other) = other {
                            w.held.insert((other, actor.1), to);
                        }
                    }
                    (at, handed) = (to, true);
                    if hops > 4 {
                        break;
                    }
                }
                if hops > 1 {
                    violations.push((1, format!("{actor:?} handed {hops} times")));
                }
                w.held.insert(actor, at);
                if w.companion_node(actor) == Some(at) && !handed && w.way_out(actor, node) {
                    violations.push((2, format!("{actor:?} alarm beside on {at}")));
                }
            }
            // A request or call reaches a node that is up.
            _ => {
                let Some(&node) = up.get(rng.below(up.len().max(1) as u64) as usize) else {
                    continue;
                };
                let actor = pick(&mut rng);
                let at = match w.held.get(&actor).copied() {
                    // Another node holds it: sent there, the rule unasked.
                    Some(holder) if w.state(holder) != State::Down => continue,
                    // The record names a dead node ("try here"): taken over
                    // here, the rule unasked.
                    Some(_) => node,
                    // Nobody: the rule decides; its peer acquires.
                    None => w.decide(actor, node).unwrap_or(node),
                };
                let nobody = !w.held.contains_key(&actor);
                w.held.insert(actor, at);
                if nobody && w.companion_node(actor) == Some(at) && w.way_out(actor, node) {
                    violations.push((2, format!("{actor:?} beside on {at} from {node}")));
                }
            }
        }
    }
    violations
}

#[test]
fn a_pair_shares_a_node_only_where_the_rule_cannot_help_and_alarms_hop_once() {
    for seed in 0..seeds() {
        Runtime::with_seed_and_config(seed, Default::default()).block_on(async {
            let v = schedule(seed, Mutation::None);
            assert!(v.is_empty(), "seed {seed}: {v:?}");
        });
    }
}

#[test]
fn judging_a_handed_alarm_again_fails_the_bound() {
    let mut failed = 0;
    for seed in 0..seeds() {
        Runtime::with_seed_and_config(seed, Default::default()).block_on(async {
            let v = schedule(seed, Mutation::Rejudge);
            assert!(v.iter().all(|(p, _)| *p == 1), "seed {seed}: {v:?}");
            failed += usize::from(!v.is_empty());
        });
    }
    assert!(failed > 0, "the mutation never failed oracle 1");
}

#[test]
fn routing_blind_to_the_rule_fails_apart() {
    let mut failed = 0;
    for seed in 0..seeds() {
        Runtime::with_seed_and_config(seed, Default::default()).block_on(async {
            let v = schedule(seed, Mutation::Blind);
            assert!(v.iter().all(|(p, _)| *p == 2), "seed {seed}: {v:?}");
            failed += usize::from(!v.is_empty());
        });
    }
    assert!(failed > 0, "the mutation never failed oracle 2");
}
