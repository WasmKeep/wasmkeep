//! Spread actors return home (ADR 0040, section 14, and ADR 0041,
//! amendment of 2026-11-26): the pure rule. No I/O, no clock read.
//!
//! A spread actor's **home** is the rendezvous choice for its address over
//! this node and the peers that serve its app ([`home`]). A node that holds
//! one whose home is a peer moves it there, a few at a time, once the
//! cluster is quiet (no node drains, every live node `serving`, no node
//! of the app stopped a moment ago), runs one
//! minor version, and the peer has been serving for a while and has room
//! ([`decide`]). Every move raises the weight of the actor's node for that
//! actor, so moves cannot cycle; an actor taken by a move stays a minute.

use crate::membership::DrainState;
use crate::placement::{RoomLoad, best, fill};
use std::time::Duration;

/// How long a peer is seen `serving` without a break before it is a
/// target: three heartbeat periods, the freshness horizon of ADR 0041.
pub const SETTLE_PERIODS: u32 = 3;
/// How long an actor taken by a take call (a rebalance move or an
/// operator's) stays before a rebalance moves it again.
pub const STAY: Duration = Duration::from_secs(60);
/// Rebalance moves in flight at once per node.
pub const IN_FLIGHT: usize = 2;
/// Sockets a node's rebalance closes per heartbeat period: one full edge.
pub const SOCKETS_PER_PERIOD: i64 = 500;
/// How long a node of the app that stopped, or whose heartbeat went
/// stale, still pauses every pass (ADR 0040, section 14, amendment of
/// 2026-11-28): counted from the last write of its node lease (its
/// release, or its last heartbeat). Longer than a restart's stop, install
/// and start check; a node gone for good leaves its actors home over the
/// nodes left, so the wait costs no reconnect.
pub const AWAY: Duration = Duration::from_secs(600);

/// The constants of the rule; tests shorten them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Settings {
    /// [`SETTLE_PERIODS`] heartbeat periods by default.
    pub settle: Duration,
    /// [`STAY`] by default.
    pub stay: Duration,
    /// [`IN_FLIGHT`] by default.
    pub in_flight: usize,
    /// [`AWAY`] by default.
    pub away: Duration,
}

impl Settings {
    /// The defaults for a heartbeat period of `period`.
    pub fn for_period(period: Duration) -> Self {
        Self {
            settle: period.saturating_mul(SETTLE_PERIODS),
            stay: STAY,
            in_flight: IN_FLIGHT,
            away: AWAY,
        }
    }
}

/// The actor's home over this node (`here`) and the peers of `set`: `None`
/// means this node. The same set gives the same home from every node; a
/// node leaving the set changes the home of its own actors only, and
/// returning gives exactly those back.
pub fn home<'a>(here: &str, set: &[&'a str], key: &str) -> Option<&'a str> {
    let chosen = best(key, set.iter().copied().chain([here]))?;
    if chosen == here {
        return None;
    }
    set.iter().copied().find(|p| *p == chosen)
}

/// A version's minor line, `(major, minor)`: `0.10.0+abc` and
/// `0.10.1-rc.1` are both `(0, 10)`. `None` if it does not start with two
/// numbers.
pub fn minor(version: &str) -> Option<(u32, u32)> {
    crate::app_format::floor::Minor::of_version(version).map(|m| (m.major, m.minor))
}

/// This node, as the pass sees it.
#[derive(Clone, Copy, Debug)]
pub struct Local<'a> {
    pub node: &'a str,
    pub version: &'a str,
    /// Placement and rebalancing both on.
    pub on: bool,
    /// Asked to drain, or cannot open the app's secrets.
    pub draining: bool,
    /// Its node lease is valid now.
    pub lease_valid: bool,
    /// The nodes of its app listed but not live (stopped, fenced, or a
    /// stale heartbeat), each with how long since the last write of its
    /// node lease. One written less than [`Settings::away`] ago pauses the
    /// pass, drain request or not: it may be restarting.
    pub away: &'a [(&'a str, Duration)],
}

/// A live peer of the same app, as this node's view shows it.
#[derive(Clone, Copy, Debug)]
pub struct Peer<'a> {
    pub node: &'a str,
    pub version: &'a str,
    /// Heartbeat line 9; `None` from a node that predates it.
    pub drain: Option<DrainState>,
    /// A drain intent names it.
    pub drain_requested: bool,
    /// This node can reach it the way node calls do.
    pub reachable: bool,
    /// How long this node has seen it `serving` without a break, on its
    /// own monotonic clock; `None` if it is not serving now.
    pub serving_for: Option<Duration>,
    /// It answers take calls.
    pub can_take: bool,
    /// Its load, if fresh.
    pub load: Option<RoomLoad>,
    /// A rebalance move to it failed within a heartbeat horizon, or it is
    /// suspected: not a target for now (still in the home set).
    pub failed: bool,
}

/// A spread actor this node holds (resident, or parked with sockets).
#[derive(Clone, Copy, Debug)]
pub struct Held<'a> {
    /// The encoded address, as rendezvous takes it.
    pub key: &'a str,
    /// Since this node acquired it through a take call, if it did.
    pub taken: Option<Duration>,
    /// A move of it is under way, an operator's move request names it, or
    /// a restore holds it: left alone.
    pub busy: bool,
    /// Its open sockets, which a move closes.
    pub sockets: u64,
}

/// What the pass may still start.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Budget {
    /// Rebalance moves of this node under way.
    pub in_flight: usize,
    /// Sockets left to close this period; below 0 after a move that went
    /// over (the next period starts in deficit).
    pub sockets_left: i64,
}

impl Budget {
    /// A period's refill: `cap` more, never above `cap`.
    #[must_use]
    pub fn refilled(self, cap: i64) -> Self {
        Self {
            sockets_left: self.sockets_left.saturating_add(cap).min(cap),
            ..self
        }
    }
}

/// Why nothing moves at all.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Pause {
    /// Placement or rebalancing is off.
    Off,
    /// This node or a peer is asked to drain.
    Draining,
    /// A node of the app is not `serving`: a live peer that shows another
    /// state (draining, stuck, or predates line 9), or one stopped or gone
    /// quiet less than [`Settings::away`] ago.
    NotServing,
    /// A live peer runs another minor version, or a version unreadable.
    Versions,
    /// This node's lease is not valid.
    Lease,
}

impl Pause {
    /// The word for metrics and `cluster status`.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::Draining => "draining",
            Self::NotServing => "not_serving",
            Self::Versions => "versions",
            Self::Lease => "lease",
        }
    }

    /// Every reason, for metrics.
    pub const ALL: [Self; 5] = [
        Self::Off,
        Self::Draining,
        Self::NotServing,
        Self::Versions,
        Self::Lease,
    ];
}

/// Why one misplaced actor does not move now.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Skip {
    /// Its move, an operator's request or a restore is under way.
    Busy,
    /// Taken by a take call less than [`Settings::stay`] ago.
    Stay,
    /// Its home has not been serving for [`Settings::settle`], or does
    /// not answer take calls.
    Settling,
    /// Its home has no fresh load, no resident room, or sockets at 800
    /// thousandths or more.
    NoRoom,
    /// A move to its home failed a moment ago.
    Failed,
    /// The moves in flight or the period's sockets are used up.
    Budget,
}

/// One move to start: the actor `key` to `target`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Move<'a> {
    pub key: &'a str,
    pub target: &'a str,
}

/// The pass's answer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Verdict<'a> {
    /// The held spread actors whose home is a peer, paused or not.
    pub misplaced: u64,
    /// Why nothing moves, if so.
    pub paused: Option<Pause>,
    /// What to start, in the order of the actors' addresses.
    pub moves: Vec<Move<'a>>,
    /// The misplaced actors left for later, and why.
    pub skipped: Vec<(&'a str, Skip)>,
}

/// The rebalance pass's decision: which held spread actors to move to their
/// home, and why the others wait. `peers` are the live peers of this
/// node's app. No I/O, no clock; the caller charges the budget for each
/// move it starts ([`Budget`]) and asks again when one ends.
pub fn decide<'a>(
    local: Local<'_>,
    peers: &[Peer<'a>],
    held: &[Held<'a>],
    budget: Budget,
    settings: Settings,
) -> Verdict<'a> {
    // The home set: the peers a wake-up could be sent to, suspected or
    // backed off included (that says a hop failed a moment ago, not that
    // the peer should lose its edges).
    let set: Vec<&'a str> = peers
        .iter()
        .filter(|p| p.drain == Some(DrainState::Serving) && !p.drain_requested && p.reachable)
        .map(|p| p.node)
        .collect();
    let mut held: Vec<&Held<'a>> = held.iter().collect();
    held.sort_unstable_by_key(|h| h.key);
    let away: Vec<(&Held<'a>, &'a str)> = held
        .into_iter()
        .filter_map(|h| home(local.node, &set, h.key).map(|p| (h, p)))
        .collect();
    let mut verdict = Verdict {
        misplaced: away.len() as u64,
        paused: None,
        moves: Vec::new(),
        skipped: Vec::new(),
    };
    let mine = minor(local.version);
    verdict.paused = if !local.on {
        Some(Pause::Off)
    } else if local.draining || peers.iter().any(|p| p.drain_requested) {
        Some(Pause::Draining)
    } else if peers.iter().any(|p| p.drain != Some(DrainState::Serving))
        || local.away.iter().any(|(_, ago)| *ago < settings.away)
    {
        Some(Pause::NotServing)
    } else if mine.is_none() || peers.iter().any(|p| minor(p.version) != mine) {
        Some(Pause::Versions)
    } else if !local.lease_valid {
        Some(Pause::Lease)
    } else {
        None
    };
    if verdict.paused.is_some() {
        return verdict;
    }
    let (mut in_flight, mut left) = (budget.in_flight, budget.sockets_left);
    for (h, target) in away {
        let Some(p) = peers.iter().find(|p| p.node == target) else {
            continue; // in the set, so always found
        };
        let skip = if h.busy {
            Some(Skip::Busy)
        } else if h.taken.is_some_and(|t| t < settings.stay) {
            Some(Skip::Stay)
        } else if !p.can_take || p.serving_for.is_none_or(|s| s < settings.settle) {
            Some(Skip::Settling)
        } else if !has_room(p.load) {
            Some(Skip::NoRoom)
        } else if p.failed {
            Some(Skip::Failed)
        } else if in_flight >= settings.in_flight || left <= 0 {
            Some(Skip::Budget)
        } else {
            None
        };
        match skip {
            Some(why) => verdict.skipped.push((h.key, why)),
            None => {
                in_flight += 1;
                left = left.saturating_sub(i64::try_from(h.sockets).unwrap_or(i64::MAX));
                verdict.moves.push(Move { key: h.key, target });
            }
        }
    }
    verdict
}

/// ADR 0041, section 4's candidate conditions: a fresh load with both room
/// fields, resident room above 0, socket fill below 800.
pub fn has_room(load: Option<RoomLoad>) -> bool {
    let Some(l) = load else {
        return false;
    };
    let (Some(actors), Some(sockets)) = (l.actor_room, l.socket_room) else {
        return false;
    };
    actors > 0 && fill(l.load.sockets, sockets) < 800
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;
    use crate::membership::Load;

    const S: Duration = Duration::from_secs(1);

    fn settings() -> Settings {
        Settings::for_period(Duration::from_secs(3))
    }
    fn local() -> Local<'static> {
        Local {
            node: "a",
            version: "0.10.0",
            on: true,
            draining: false,
            lease_valid: true,
            away: &[],
        }
    }
    fn room(sockets: u64, socket_room: u64, actor_room: u64) -> RoomLoad {
        RoomLoad {
            load: Load {
                actors: 10,
                sockets,
                ..Load::default()
            },
            actor_room: Some(actor_room),
            socket_room: Some(socket_room),
        }
    }
    fn peer(node: &'static str) -> Peer<'static> {
        Peer {
            node,
            version: "0.10.0",
            drain: Some(DrainState::Serving),
            drain_requested: false,
            reachable: true,
            serving_for: Some(S * 60),
            can_take: true,
            load: Some(room(0, 1000, 100)),
            failed: false,
        }
    }
    fn held(key: &'static str) -> Held<'static> {
        Held {
            key,
            taken: None,
            busy: false,
            sockets: 10,
        }
    }
    fn budget() -> Budget {
        Budget {
            in_flight: 0,
            sockets_left: SOCKETS_PER_PERIOD,
        }
    }
    /// The first edge name whose home over a, b and c is `node`.
    fn edge_of(node: &str) -> &'static str {
        (1..)
            .map(|k| -> &'static str { Box::leak(format!("t/a/edge/r.e{k}").into_boxed_str()) })
            .find(|k| home("a", &["b", "c"], k).unwrap_or("a") == node)
            .unwrap()
    }
    fn run(peers: &[Peer<'static>], held: &[Held<'static>]) -> Verdict<'static> {
        decide(local(), peers, held, budget(), settings())
    }
    fn one(peers: &[Peer<'static>], h: Held<'static>) -> Result<&'static str, Skip> {
        let v = run(peers, &[h]);
        assert_eq!(v.paused, None);
        assert_eq!(v.misplaced, 1);
        match (v.moves.first(), v.skipped.first()) {
            (Some(m), None) => Ok(m.target),
            (None, Some((_, why))) => Err(*why),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn home_is_the_rendezvous_choice_and_only_a_leavers_actors_change() {
        let keys: Vec<String> = (0..300).map(|k| format!("t/a/edge/r.e{k}")).collect();
        let all = ["b", "c", "d"];
        for k in &keys {
            let with = home("a", &all, k);
            let without = home("a", &["b", "d"], k);
            if with != Some("c") {
                assert_eq!(with, without, "{k}: not c's, so unchanged");
            } else {
                assert_ne!(without, Some("c"));
            }
        }
        // The same answer as the wake-up's spread choice.
        for k in &keys {
            assert_eq!(home("a", &all, k), crate::placement::spread("a", &all, k));
        }
        assert_eq!(home("a", &[], "t/a/edge/x"), None);
    }

    #[test]
    fn minor_reads_release_build_and_prerelease_versions() {
        assert_eq!(minor("0.10.0"), Some((0, 10)));
        assert_eq!(minor("0.10.3+0123456789ab"), Some((0, 10)));
        assert_eq!(minor("0.10.0-rc.1"), Some((0, 10)));
        assert_eq!(minor("1.2"), Some((1, 2)));
        assert_eq!(minor("dev"), None);
        assert_eq!(minor("0"), None);
    }

    #[test]
    fn an_actor_whose_home_is_a_settled_peer_with_room_moves() {
        let e = edge_of("b");
        assert_eq!(one(&[peer("b"), peer("c")], held(e)), Ok("b"));
        // At home: nothing to do, nothing counted.
        let v = run(&[peer("b"), peer("c")], &[held(edge_of("a"))]);
        assert_eq!((v.misplaced, v.moves.len(), v.skipped.len()), (0, 0, 0));
    }

    #[test]
    fn the_settle_delay_counts_to_the_edge() {
        let e = edge_of("b");
        let mut b = peer("b");
        b.serving_for = Some(Duration::from_millis(8_900));
        assert_eq!(one(&[b, peer("c")], held(e)), Err(Skip::Settling));
        b.serving_for = Some(S * 9);
        assert_eq!(one(&[b, peer("c")], held(e)), Ok("b"));
        // A peer that does not answer take calls is never a target.
        b.can_take = false;
        assert_eq!(one(&[b, peer("c")], held(e)), Err(Skip::Settling));
    }

    #[test]
    fn a_patch_apart_moves_and_a_minor_apart_pauses() {
        let e = edge_of("b");
        let mut b = peer("b");
        b.version = "0.10.1+abcdef012345";
        assert_eq!(one(&[b, peer("c")], held(e)), Ok("b"));
        b.version = "0.9.0";
        let v = run(&[b, peer("c")], &[held(e)]);
        assert_eq!(v.paused, Some(Pause::Versions));
        assert_eq!((v.misplaced, v.moves.len()), (1, 0));
        b.version = "unknown";
        assert_eq!(
            run(&[b, peer("c")], &[held(e)]).paused,
            Some(Pause::Versions)
        );
    }

    #[test]
    fn a_node_stopped_within_the_away_bound_pauses_every_node() {
        let e = edge_of("b");
        let away = settings().away;
        let mut l = local();
        // Stopped (released) a moment ago, its drain request withdrawn:
        // it is not a live peer, yet it may be restarting.
        let stopped = [("c", Duration::from_secs(2))];
        l.away = &stopped;
        let v = decide(l, &[peer("b")], &[held(e)], budget(), settings());
        assert_eq!(v.paused, Some(Pause::NotServing));
        assert_eq!(v.misplaced, 1, "counted while paused");
        let just = [("c", away - Duration::from_millis(1))];
        l.away = &just;
        let v = decide(l, &[peer("b")], &[held(e)], budget(), settings());
        assert_eq!(v.paused, Some(Pause::NotServing));
        // Past the bound it no longer counts: a node gone for good does
        // not stall the pass.
        let gone = [("c", away)];
        l.away = &gone;
        let v = decide(l, &[peer("b")], &[held(e)], budget(), settings());
        assert_eq!((v.paused, v.moves.len()), (None, 1));
        assert!(AWAY >= Duration::from_secs(300), "longer than a restart");
    }

    #[test]
    fn a_drain_or_a_node_not_serving_pauses_every_node() {
        let e = edge_of("b");
        let mut c = peer("c");
        c.drain_requested = true;
        let v = run(&[peer("b"), c], &[held(e)]);
        assert_eq!(v.paused, Some(Pause::Draining));
        assert_eq!(v.misplaced, 1, "counted while paused");
        for state in [DrainState::Draining, DrainState::Drained, DrainState::Stuck] {
            let mut c = peer("c");
            c.drain = Some(state);
            c.serving_for = None;
            assert_eq!(
                run(&[peer("b"), c], &[held(e)]).paused,
                Some(Pause::NotServing)
            );
        }
        let mut c = peer("c");
        c.drain = None;
        assert_eq!(
            run(&[peer("b"), c], &[held(e)]).paused,
            Some(Pause::NotServing)
        );
        let mut l = local();
        l.draining = true;
        let v = decide(l, &[peer("b"), peer("c")], &[held(e)], budget(), settings());
        assert_eq!(v.paused, Some(Pause::Draining));
        let mut l = local();
        l.on = false;
        let v = decide(l, &[peer("b"), peer("c")], &[held(e)], budget(), settings());
        assert_eq!(v.paused, Some(Pause::Off));
        let mut l = local();
        l.lease_valid = false;
        let v = decide(l, &[peer("b"), peer("c")], &[held(e)], budget(), settings());
        assert_eq!(v.paused, Some(Pause::Lease));
    }

    #[test]
    fn a_home_without_room_is_skipped_not_replaced() {
        let e = edge_of("b");
        let mut b = peer("b");
        b.load = Some(room(0, 1000, 0));
        assert_eq!(one(&[b, peer("c")], held(e)), Err(Skip::NoRoom));
        // Socket fill 799 moves, 800 does not.
        b.load = Some(room(799, 201, 100));
        assert_eq!(one(&[b, peer("c")], held(e)), Ok("b"));
        b.load = Some(room(800, 200, 100));
        assert_eq!(one(&[b, peer("c")], held(e)), Err(Skip::NoRoom));
        // No fresh load, or a load without room fields.
        b.load = None;
        assert_eq!(one(&[b, peer("c")], held(e)), Err(Skip::NoRoom));
        b.load = Some(RoomLoad {
            actor_room: None,
            ..room(0, 1000, 100)
        });
        assert_eq!(one(&[b, peer("c")], held(e)), Err(Skip::NoRoom));
        // A failed move to it: skipped, still its home.
        let mut b = peer("b");
        b.failed = true;
        assert_eq!(one(&[b, peer("c")], held(e)), Err(Skip::Failed));
    }

    #[test]
    fn an_actor_taken_by_a_take_stays_a_minute_and_a_wake_up_has_no_wait() {
        let e = edge_of("b");
        let mut h = held(e);
        h.taken = Some(S * 59);
        assert_eq!(one(&[peer("b"), peer("c")], h), Err(Skip::Stay));
        h.taken = Some(S * 60);
        assert_eq!(one(&[peer("b"), peer("c")], h), Ok("b"));
        // Acquired by a wake-up, a takeover or a drain's hand-over.
        h.taken = None;
        assert_eq!(one(&[peer("b"), peer("c")], h), Ok("b"));
        h.busy = true;
        assert_eq!(one(&[peer("b"), peer("c")], h), Err(Skip::Busy));
    }

    #[test]
    fn the_budget_bounds_moves_in_flight_and_sockets() {
        let keys: Vec<&'static str> = (1..)
            .map(|k| -> &'static str { Box::leak(format!("t/a/edge/r.e{k}").into_boxed_str()) })
            .filter(|k| home("a", &["b", "c"], k).is_some())
            .take(5)
            .collect();
        let hs: Vec<Held<'static>> = keys.iter().map(|k| held(k)).collect();
        let peers = [peer("b"), peer("c")];
        let v = decide(local(), &peers, &hs, budget(), settings());
        assert_eq!(v.moves.len(), 2, "two in flight");
        assert_eq!(v.skipped.len(), 3);
        assert!(v.skipped.iter().all(|(_, s)| *s == Skip::Budget));
        // In the order of the addresses.
        let mut sorted = keys.clone();
        sorted.sort_unstable();
        assert_eq!(v.moves[0].key, sorted[0]);
        // One in flight already: one more.
        let one_left = Budget {
            in_flight: 1,
            ..budget()
        };
        assert_eq!(
            decide(local(), &peers, &hs, one_left, settings())
                .moves
                .len(),
            1
        );
        // The period's sockets used up, or in deficit: none.
        for left in [0, -200] {
            let b = Budget {
                in_flight: 0,
                sockets_left: left,
            };
            assert!(decide(local(), &peers, &hs, b, settings()).moves.is_empty());
        }
        // A move may go over: the first actor holds more than is left.
        let mut big = hs.clone();
        for h in &mut big {
            h.sockets = 400;
        }
        let b = Budget {
            in_flight: 0,
            sockets_left: 300,
        };
        assert_eq!(decide(local(), &peers, &big, b, settings()).moves.len(), 1);
        // A deficit carries into the next period's refill.
        let deficit = Budget {
            in_flight: 0,
            sockets_left: -100,
        };
        assert_eq!(deficit.refilled(500).sockets_left, 400);
        assert_eq!(budget().refilled(500).sockets_left, 500);
    }

    #[test]
    fn moves_always_raise_the_actors_weight() {
        // From any holder, the move goes to the highest weight of the set,
        // the holder included: no cycle.
        let nodes = ["a", "b", "c", "d"];
        for k in 0..200 {
            let key = format!("t/a/edge/r.e{k}");
            for here in nodes {
                let set: Vec<&str> = nodes.iter().copied().filter(|n| *n != here).collect();
                if let Some(to) = home(here, &set, &key) {
                    assert!(
                        crate::placement::weight(&key, to) > crate::placement::weight(&key, here)
                    );
                }
            }
        }
    }

    #[test]
    fn a_suspected_or_failed_peer_keeps_its_actors_home() {
        // The home set keeps a peer a move to failed: its actors wait,
        // they are not sent to a second choice.
        let e = edge_of("b");
        let mut b = peer("b");
        b.failed = true;
        let v = run(&[b, peer("c")], &[held(e)]);
        assert!(v.moves.is_empty());
        // An unreachable peer is out of the set, as for a wake-up.
        let mut b = peer("b");
        b.reachable = false;
        let v = run(&[b, peer("c")], &[held(e)]);
        assert!(v.moves.iter().all(|m| m.target != "b"));
    }
}
