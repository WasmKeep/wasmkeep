//! ADR 0040, section 14 (amendment of 2026-11-26): spread actors return
//! home after a node rejoins. A model of the server's pass around the real
//! pure rule (`rebalance::decide`, `rebalance::home`): three or four nodes
//! that drain and stop, die, restart as new processes (now and then of
//! another minor version), and flap in and out of reach; views that lag
//! each other by up to two seconds (heartbeats late); actors whose sockets
//! reconnect at random nodes after every close; moves that take one or two
//! seconds, leave a gap between the owner's release and the target's take,
//! and fail one time in ten. It checks, each with a mutation that must
//! fail it:
//!
//! 1. Converges: once the live set is stable and quiet, every held spread
//!    actor is on its home within a bound from the settle delay, the stay
//!    and the pacing. Mutation: the settle delay never met.
//! 2. Monotone: every rebalance move raises the weight of the actor's node
//!    for that actor, and no actor moves within the stay of a take that
//!    armed it: a take on a node that is not the actor's home in its own
//!    view (an operator's move, which the model makes now and then, or a
//!    move or cold socket whose sender's view differed). Mutation: no stay
//!    (rule 7 removed).
//! 3. Paced: never more than two moves in flight per node, never more
//!    sockets closed in a period than the budget plus one actor's.
//!    Mutation: the budget removed.
//! 4. Quiet: no move starts while, in the mover's view, a node drains or
//!    is not serving, a node stopped or died less than the away bound ago
//!    (its drain request gone with it), or two minor versions are live.
//!    Mutations: the pass shown only the serving peers of its own version;
//!    the pass blind to stopped nodes (amendment of 2026-11-28). Now and
//!    then a node stops for good when the faults end: convergence over the
//!    nodes left, in a bound that includes the away bound, shows that the
//!    pause ends.
//! 5. Spread classes only: no actor of another class is moved. Mutation:
//!    the pass fed every held actor.
//! 7. A cold socket goes home: an actor nobody holds that a socket wakes is
//!    acquired by its home in the waking node's view, or by the waking
//!    node when the take fails, never by a third node, and the socket is
//!    never refused. Mutation: the take skipped.
//!
//! Not checked here: property 6 of the design (one holder, which the
//! node-lease simulation's invariant covers for the move sequence itself),
//! the bucket, and the hop's transport.
//! RUSTFLAGS='--cfg madsim' cargo test -j 4 -p wasmkeep-sim
//!   --test madsim_rebalance --target-dir target/madsim
#![cfg(madsim)]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use madsim::runtime::Runtime;
use std::collections::HashMap;
use std::time::Duration;
use wasmkeep_cluster::membership::{DrainState, Load};
use wasmkeep_cluster::placement::RoomLoad;
use wasmkeep_cluster::rebalance::{self, Budget, Held, Local, Peer, Settings};
use wasmkeep_sim::rng::Rng;

const NODES: [&str; 4] = ["a", "b", "c", "d"];
/// Seconds per heartbeat period.
const PERIOD: u64 = 3;
const SETTLE: u64 = 9;
const STAY: u64 = 60;
/// A failed target is left out this long.
const HORIZON: u64 = 10;
/// How long a stopped or dead node still pauses every pass (the away
/// bound, shortened): longer than most of the model's restarts.
const AWAY: u64 = 30;
/// Faults stop here; the cluster is then stable and quiet.
const STABLE_AT: u64 = 300;
const ACTORS: usize = 40;
const BASE: &str = "0.10.0";
const OTHER: &str = "0.11.0";

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
    NeverSettled,
    NoStay,
    NoBudget,
    NoQuiet,
    AllClasses,
    NoTake,
    NoAway,
}

/// One node as the others see it at one moment.
#[derive(Clone, Copy, Debug)]
struct Seen {
    state: State,
    /// Since when it serves, or since when it is down.
    since: u64,
    minor: &'static str,
    reachable: bool,
}

struct Node {
    seen: Seen,
    /// When a drained node stops, or a stopped one starts again.
    next_at: u64,
    budget: i64,
    closed: u64,
    failed: HashMap<usize, u64>,
}

struct Actor {
    key: String,
    spread: bool,
    holder: Option<usize>,
    /// The holder died: its record says "try here".
    dead: bool,
    sockets: u64,
    /// When a take that armed its stay brought it to its holder: a take
    /// on a node that is not its home in that node's own view (an
    /// operator's move, or a move or cold socket whose sender saw another
    /// home). `None` once it is acquired any other way.
    taken_at: Option<u64>,
}

struct Flight {
    actor: usize,
    from: usize,
    to: usize,
    done_at: u64,
}

struct World {
    now: u64,
    nodes: Vec<Node>,
    /// The last three moments' views of every node, newest last.
    history: Vec<Vec<Seen>>,
    actors: Vec<Actor>,
    flights: Vec<Flight>,
    mutation: Mutation,
    violations: Vec<(u8, String)>,
    /// A node stopped at the end of the faults that never comes back.
    gone: Option<usize>,
    /// Up to two seconds of lag, per observer and node.
    lag: Vec<Vec<usize>>,
}

impl World {
    fn id(i: usize) -> &'static str {
        NODES[i]
    }

    /// What `o` sees of `n` now.
    fn view(&self, o: usize, n: usize) -> Seen {
        if o == n {
            return self.nodes[n].seen;
        }
        let back = self.lag[o][n].min(self.history.len().saturating_sub(1));
        self.history[self.history.len() - 1 - back][n]
    }

    /// The home set `o` sees: serving, reachable peers.
    fn set(&self, o: usize) -> Vec<&'static str> {
        (0..self.nodes.len())
            .filter(|&n| n != o)
            .filter(|&n| {
                let s = self.view(o, n);
                s.state == State::Serving && s.reachable
            })
            .map(Self::id)
            .collect()
    }

    fn home_from(&self, o: usize, key: &str) -> usize {
        let set = self.set(o);
        match rebalance::home(Self::id(o), &set, key) {
            Some(p) => NODES.iter().position(|n| *n == p).unwrap(),
            None => o,
        }
    }

    fn up(&self) -> Vec<usize> {
        (0..self.nodes.len())
            .filter(|&n| self.nodes[n].seen.state == State::Serving)
            .collect()
    }

    /// An event or a socket for actor `i` lands on a serving node.
    fn wake(&mut self, i: usize, rng: &mut Rng) {
        let up = self.up();
        if up.is_empty() {
            return;
        }
        let x = up[rng.below(up.len() as u64) as usize];
        let a = &self.actors[i];
        if a.holder.is_some() && !a.dead {
            return; // relayed to its holder
        }
        if a.dead || !a.spread {
            // "Try here", or a class that does not spread: taken here.
            let a = &mut self.actors[i];
            (a.holder, a.dead, a.taken_at) = (Some(x), false, None);
            return;
        }
        // Nobody holds it: its home takes it first (section 3.3).
        let home = self.home_from(x, &a.key);
        let failed = !rng.chance(85) || self.nodes[home].seen.state != State::Serving;
        let (at, taken) = if home == x || self.mutation == Mutation::NoTake || failed {
            (x, false)
        } else {
            (home, true)
        };
        if at != home && !(failed && at == x) {
            self.violations.push((
                7,
                format!("{} woken on {} from {x}, home {home}", a.key, at),
            ));
        }
        let now = self.now;
        let armed = taken && self.home_from(at, &a.key) != at;
        let a = &mut self.actors[i];
        a.holder = Some(at);
        a.dead = false;
        a.taken_at = armed.then_some(now);
    }

    /// Node `o`'s pass.
    fn pass(&mut self, o: usize) {
        let me = self.nodes[o].seen;
        if me.state == State::Down {
            return;
        }
        let now = self.now;
        let all: Vec<(usize, Seen)> = (0..self.nodes.len())
            .filter(|&n| n != o)
            .map(|n| (n, self.view(o, n)))
            .collect();
        // Stopped or dead: listed, not live, each with how long ago.
        let away: Vec<(&str, Duration)> = all
            .iter()
            .filter(|(_, s)| s.state == State::Down && self.mutation != Mutation::NoAway)
            .map(|(n, s)| {
                (
                    Self::id(*n),
                    Duration::from_secs(now.saturating_sub(s.since)),
                )
            })
            .collect();
        let views: Vec<(usize, Seen)> = all
            .iter()
            .copied()
            .filter(|(_, s)| s.state != State::Down)
            .collect();
        let shown: Vec<&(usize, Seen)> = views
            .iter()
            .filter(|(_, s)| {
                self.mutation != Mutation::NoQuiet
                    || (s.state == State::Serving && s.minor == me.minor)
            })
            .collect();
        let room = RoomLoad {
            load: Load::default(),
            actor_room: Some(100),
            socket_room: Some(100_000),
        };
        let peers: Vec<Peer<'_>> = shown
            .iter()
            .map(|(n, s)| Peer {
                node: Self::id(*n),
                version: s.minor,
                drain: Some(match s.state {
                    State::Serving => DrainState::Serving,
                    _ => DrainState::Draining,
                }),
                drain_requested: s.state == State::Draining,
                reachable: s.reachable,
                serving_for: (s.state == State::Serving)
                    .then(|| Duration::from_secs(now.saturating_sub(s.since))),
                can_take: true,
                load: Some(room),
                failed: self.nodes[o].failed.get(n).is_some_and(|u| *u > now),
            })
            .collect();
        let held: Vec<(usize, Held<'_>)> = self
            .actors
            .iter()
            .enumerate()
            .filter(|(_, a)| a.holder == Some(o) && !a.dead)
            .filter(|(_, a)| a.spread || self.mutation == Mutation::AllClasses)
            .map(|(i, a)| {
                (
                    i,
                    Held {
                        key: &a.key,
                        taken: a.taken_at.map(|t| Duration::from_secs(now - t)),
                        busy: false,
                        sockets: a.sockets,
                    },
                )
            })
            .collect();
        let in_flight = self.flights.iter().filter(|f| f.from == o).count();
        let settings = Settings {
            settle: Duration::from_secs(if self.mutation == Mutation::NeverSettled {
                1_000_000
            } else {
                SETTLE
            }),
            stay: Duration::from_secs(if self.mutation == Mutation::NoStay {
                0
            } else {
                STAY
            }),
            in_flight: if self.mutation == Mutation::NoBudget {
                usize::MAX
            } else {
                2
            },
            away: Duration::from_secs(AWAY),
        };
        let budget = Budget {
            in_flight,
            sockets_left: if self.mutation == Mutation::NoBudget {
                i64::MAX
            } else {
                self.nodes[o].budget
            },
        };
        let local = Local {
            node: Self::id(o),
            version: me.minor,
            on: true,
            draining: me.state == State::Draining,
            lease_valid: true,
            away: &away,
        };
        let only: Vec<Held<'_>> = held.iter().map(|(_, h)| *h).collect();
        let verdict = rebalance::decide(local, &peers, &only, budget, settings);
        let starts: Vec<(usize, usize)> = verdict
            .moves
            .iter()
            .map(|m| {
                let i = held.iter().find(|(_, h)| h.key == m.key).unwrap().0;
                (i, NODES.iter().position(|n| *n == m.target).unwrap())
            })
            .collect();
        drop(held);
        let quiet = me.state == State::Serving
            && views
                .iter()
                .all(|(_, s)| s.state == State::Serving && s.minor == me.minor)
            && all
                .iter()
                .all(|(_, s)| s.state != State::Down || now - s.since >= AWAY);
        for (i, to) in starts {
            let a = &self.actors[i];
            if !quiet {
                self.violations
                    .push((4, format!("{} moved by {o} while not quiet", a.key)));
            }
            if !a.spread {
                self.violations
                    .push((5, format!("{} of another class moved", a.key)));
            }
            if rebalance::home(Self::id(o), &[Self::id(to)], &a.key) != Some(Self::id(to)) {
                self.violations
                    .push((2, format!("{} moved down from {o} to {to}", a.key)));
            }
            if a.taken_at.is_some_and(|t| now - t < STAY) {
                self.violations
                    .push((2, format!("{} moved within the stay of its take", a.key)));
            }
            let sockets = a.sockets;
            let node = &mut self.nodes[o];
            node.budget -= i64::try_from(sockets).unwrap();
            node.closed += sockets;
            let in_flight = self.flights.iter().filter(|f| f.from == o).count() + 1;
            let max = self.actors.iter().map(|a| a.sockets).max().unwrap_or(0);
            if in_flight > 2 {
                self.violations
                    .push((3, format!("{in_flight} moves in flight on {o}")));
            }
            if node.closed > u64::try_from(rebalance::SOCKETS_PER_PERIOD).unwrap() + max {
                self.violations.push((
                    3,
                    format!("{} sockets closed on {o} in a period", node.closed),
                ));
            }
            let a = &mut self.actors[i];
            // Released: the gap between the owner's release and the take.
            a.holder = None;
            self.flights.push(Flight {
                actor: i,
                from: o,
                to,
                done_at: now + 1 + (now + i as u64) % 2,
            });
        }
    }

    /// Moves whose take is answered now.
    fn land(&mut self, rng: &mut Rng) {
        let now = self.now;
        let (done, rest): (Vec<Flight>, Vec<Flight>) =
            self.flights.drain(..).partition(|f| f.done_at <= now);
        self.flights = rest;
        for f in done {
            let target_ok = self.nodes[f.to].seen.state == State::Serving;
            // The stay is armed only where the receiver is not the
            // actor's home in its own view (rule 7).
            let armed = self.home_from(f.to, &self.actors[f.actor].key) != f.to;
            let a = &mut self.actors[f.actor];
            if a.holder.is_some() {
                continue; // taken elsewhere in the gap
            }
            if target_ok && rng.chance(90) {
                a.holder = Some(f.to);
                a.dead = false;
                a.taken_at = armed.then_some(now);
            } else {
                self.nodes[f.from].failed.insert(f.to, now + HORIZON);
            }
        }
    }

    fn snapshot(&mut self) {
        let seen: Vec<Seen> = self.nodes.iter().map(|n| n.seen).collect();
        self.history.push(seen);
        if self.history.len() > 3 {
            self.history.remove(0);
        }
    }

    /// Faults, before the cluster is left stable.
    fn faults(&mut self, rng: &mut Rng) {
        let now = self.now;
        let count = self.nodes.len();
        for n in 0..count {
            let node = &mut self.nodes[n];
            match node.seen.state {
                State::Draining if now >= node.next_at => {
                    node.seen.state = State::Down;
                    node.seen.since = now;
                    node.next_at = now + 5 + rng.below(15);
                }
                State::Down if now >= node.next_at => {
                    // A new process; now and then of the next minor
                    // version, and back to the base one after.
                    node.seen.state = State::Serving;
                    node.seen.since = now;
                    node.seen.minor = if node.seen.minor == BASE && rng.chance(10) {
                        OTHER
                    } else {
                        BASE
                    };
                }
                _ => {}
            }
        }
        let up = self.up();
        if up.len() > 1 && rng.chance(3) {
            let n = up[rng.below(up.len() as u64) as usize];
            if rng.chance(70) {
                // A drain: its actors released; their sockets reconnect.
                self.nodes[n].seen.state = State::Draining;
                self.nodes[n].next_at = now + 2 + rng.below(6);
                for a in &mut self.actors {
                    if a.holder == Some(n) && !a.dead {
                        a.holder = None;
                    }
                }
            } else {
                // A death: its records left naming it.
                self.nodes[n].seen.state = State::Down;
                self.nodes[n].seen.since = now;
                self.nodes[n].next_at = now + 10 + rng.below(20);
                for a in &mut self.actors {
                    if a.holder == Some(n) {
                        a.dead = true;
                    }
                }
            }
        }
        if rng.chance(4) {
            let n = rng.below(count as u64) as usize;
            self.nodes[n].seen.reachable = !self.nodes[n].seen.reachable;
        }
        // An operator's move of a held spread actor to another serving
        // node: off its home, usually, so its stay is armed.
        let up = self.up();
        if up.len() > 1 && rng.chance(3) {
            let i = rng.below(self.actors.len() as u64) as usize;
            let to = up[rng.below(up.len() as u64) as usize];
            let a = &self.actors[i];
            let in_flight = self.flights.iter().any(|f| f.actor == i);
            if a.spread && !a.dead && !in_flight && a.holder.is_some_and(|h| h != to) {
                let armed = self.home_from(to, &a.key) != to;
                let a = &mut self.actors[i];
                a.holder = Some(to);
                a.taken_at = armed.then_some(now);
            }
        }
    }

    /// Faults end: every node serving, reachable, on the base version.
    fn stabilize(&mut self) {
        let now = self.now;
        for (i, n) in self.nodes.iter_mut().enumerate() {
            if self.gone == Some(i) {
                if n.seen.state != State::Down {
                    n.seen.since = now;
                }
                n.seen.state = State::Down;
                continue;
            }
            if n.seen.state != State::Serving || n.seen.minor != BASE {
                n.seen.since = now;
            }
            n.seen.state = State::Serving;
            n.seen.minor = BASE;
            n.seen.reachable = true;
        }
        for a in &mut self.actors {
            // A dead node's actors are free once it is fenced; the node
            // stopped for good released its own.
            if a.dead || (self.gone.is_some() && a.holder == self.gone) {
                a.holder = None;
                a.dead = false;
            }
        }
        let gone = self.gone;
        self.flights.retain(|f| Some(f.to) != gone);
    }

    /// Held spread actors away from their home over every node.
    fn away(&self) -> Vec<String> {
        let all: Vec<&str> = (0..self.nodes.len())
            .filter(|n| self.gone != Some(*n))
            .map(Self::id)
            .collect();
        self.actors
            .iter()
            .filter(|a| a.spread && !a.dead)
            .filter_map(|a| {
                let h = a.holder?;
                let home = rebalance::home(
                    Self::id(h),
                    &all.iter()
                        .copied()
                        .filter(|n| *n != Self::id(h))
                        .collect::<Vec<_>>(),
                    &a.key,
                );
                home.map(|p| format!("{} on {} (home {p})", a.key, Self::id(h)))
            })
            .collect()
    }
}

/// The bound of property 1, in seconds after the faults end: the settle
/// delay, the stay an operator's move just before armed (a take home in
/// its receiver's view arms none), and every misplaced
/// actor moved two at a time per node, with a failure's horizon twice.
fn bound() -> u64 {
    AWAY + SETTLE + STAY + (ACTORS as u64).div_ceil(2) * 2 + 2 * HORIZON + 2 * PERIOD
}

fn schedule(seed: u64, mutation: Mutation) -> Vec<(u8, String)> {
    let mut rng = Rng::new(seed);
    let count = 3 + rng.below(2) as usize;
    let nodes = (0..count)
        .map(|_| Node {
            seen: Seen {
                state: State::Serving,
                since: 0,
                minor: BASE,
                reachable: true,
            },
            next_at: 0,
            budget: rebalance::SOCKETS_PER_PERIOD,
            closed: 0,
            failed: HashMap::new(),
        })
        .collect();
    let actors = (0..ACTORS)
        .map(|k| Actor {
            key: if k % 8 == 7 {
                format!("t/app/room/r{k}")
            } else {
                format!("t/app/edge/lobby.e{k}")
            },
            spread: k % 8 != 7,
            holder: None,
            dead: false,
            sockets: 20 + rng.below(300),
            taken_at: None,
        })
        .collect();
    let mut w = World {
        now: 0,
        nodes,
        history: Vec::new(),
        actors,
        flights: Vec::new(),
        mutation,
        violations: Vec::new(),
        gone: rng.chance(30).then_some(count - 1),
        lag: vec![vec![0; count]; count],
    };
    w.snapshot();
    let end = STABLE_AT + bound();
    while w.now <= end {
        w.now += 1;
        if w.now < STABLE_AT {
            w.faults(&mut rng);
        } else if w.now == STABLE_AT {
            w.stabilize();
        }
        for o in 0..count {
            for n in 0..count {
                w.lag[o][n] = rng.below(3) as usize;
            }
        }
        w.snapshot();
        if w.now.is_multiple_of(PERIOD) {
            for n in &mut w.nodes {
                n.budget = Budget {
                    in_flight: 0,
                    sockets_left: n.budget,
                }
                .refilled(rebalance::SOCKETS_PER_PERIOD)
                .sockets_left;
                n.closed = 0;
            }
            let now = w.now;
            for n in &mut w.nodes {
                n.failed.retain(|_, u| *u > now);
            }
        }
        w.land(&mut rng);
        // Released actors' sockets reconnect, and a few events arrive.
        for i in 0..ACTORS {
            let a = &w.actors[i];
            if (a.holder.is_none() || a.dead) && rng.chance(60) {
                w.wake(i, &mut rng);
            }
        }
        for o in 0..count {
            w.pass(o);
        }
    }
    let away = w.away();
    if !away.is_empty() {
        w.violations
            .push((1, format!("{} s after the faults ended: {away:?}", bound())));
    }
    w.violations
}

/// Runs every seed with `mutation`: each one's violations are of the
/// properties `allowed` only, and with a mutation, property `must` fails
/// on at least one seed.
fn run(mutation: Mutation, allowed: &[u8], must: u8) {
    let mut failed = 0;
    for seed in 0..seeds() {
        Runtime::with_seed_and_config(seed, Default::default()).block_on(async {
            let v = schedule(seed, mutation);
            if mutation == Mutation::None {
                assert!(v.is_empty(), "seed {seed}: {v:?}");
            }
            assert!(
                v.iter().all(|(p, _)| allowed.contains(p)),
                "seed {seed}: {v:?}"
            );
            failed += usize::from(v.iter().any(|(p, _)| *p == must));
        });
    }
    if mutation != Mutation::None {
        assert!(failed > 0, "{mutation:?} never failed property {must}");
    }
}

#[test]
fn spread_actors_converge_home_monotone_paced_and_quiet() {
    run(Mutation::None, &[], 0);
}

#[test]
fn a_settle_delay_never_met_fails_convergence() {
    run(Mutation::NeverSettled, &[1], 1);
}

#[test]
fn no_stay_fails_monotone() {
    run(Mutation::NoStay, &[1, 2, 3], 2);
}

#[test]
fn no_budget_fails_pacing() {
    run(Mutation::NoBudget, &[3], 3);
}

#[test]
fn a_pass_blind_to_drains_and_versions_fails_quiet() {
    run(Mutation::NoQuiet, &[1, 2, 3, 4], 4);
}

#[test]
fn feeding_every_class_fails_spread_only() {
    run(Mutation::AllClasses, &[1, 2, 3, 5], 5);
}

#[test]
fn a_pass_blind_to_stopped_nodes_fails_quiet() {
    run(Mutation::NoAway, &[1, 2, 3, 4], 4);
}

#[test]
fn skipping_the_take_fails_a_cold_socket_goes_home() {
    run(Mutation::NoTake, &[7], 7);
}
