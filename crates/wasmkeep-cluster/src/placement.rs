//! Pure ADR 0041 placement policy. Not connected to routing or membership.

use crate::membership::{DrainState, Load};
use std::{collections::BTreeMap, time::Duration};

/// P1's room fields alongside the existing heartbeat load. Rooms are per
/// app, already bounded by the process; this module never recomputes them.
#[derive(Clone, Copy, Debug)]
pub struct RoomLoad {
    pub load: Load,
    pub actor_room: Option<u64>,
    pub socket_room: Option<u64>,
}

#[derive(Clone, Copy, Debug)]
pub struct Local<'a> {
    pub node: &'a str,
    pub app: &'a str,
    pub place: bool,
    /// Read from memory for this decision, not from a heartbeat.
    pub load: RoomLoad,
}

#[derive(Clone, Copy, Debug)]
pub struct Peer<'a> {
    pub node: &'a str,
    pub app: &'a str,
    pub live: bool,
    pub drain: Option<DrainState>,
    pub drain_requested: bool,
    pub suspected: bool,
    pub reachable: bool,
    pub place: bool,
    pub load: Option<RoomLoad>,
    /// Identity of the heartbeat (not its wall-clock timestamp).
    pub heartbeat: &'a str,
    /// Elapsed on the sender's monotonic clock since FIRST observation.
    pub age: Duration,
    pub backoff_remaining: Duration,
    /// Elapsed locally since accepting a placement from this peer.
    pub accepted_age: Option<Duration>,
}

/// One sender's credit ledger, independent of every other sender.
/// The view supplies the current heartbeat identity; a repeated read does
/// not reset credit. A new identity starts at zero, even with identical load.
#[derive(Clone, Debug, Default)]
pub struct Budget(BTreeMap<String, (String, u64)>);

impl Budget {
    pub fn sent(&self, node: &str, heartbeat: &str) -> u64 {
        self.0
            .get(node)
            .filter(|(h, _)| h == heartbeat)
            .map_or(0, |(_, k)| *k)
    }

    /// Charge a chosen attempt, including refused/unsent/ambiguous attempts.
    /// Callers serialize decision + charge and supply only their current
    /// observed heartbeat, never a historical heartbeat replay.
    pub fn charge(&mut self, node: &str, heartbeat: &str) {
        let k = self.sent(node, heartbeat).saturating_add(1);
        self.0.insert(node.into(), (heartbeat.into(), k));
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Reason {
    Floor,
    Band,
    Budget,
    NoCandidate,
    Disabled,
    Lighter,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Decision<'a> {
    Keep(Reason),
    Forward { node: &'a str, reason: Reason },
}

/// The caller has established an unforwarded, placeable wake of an unowned
/// actor and can sign its hop (ADR sections 1 and 8). `key` is the encoded
/// actor address. `peers` contains one entry per node id in the app's view.
/// No I/O, clock read or credit mutation; charge a Forward before sending.
pub fn decide<'a>(
    local: Local<'_>,
    peers: &[Peer<'a>],
    key: &str,
    period: Duration,
    budget: &Budget,
) -> Decision<'a> {
    if !local.place {
        return Decision::Keep(Reason::Disabled);
    }
    let Some(room) = local.load.actor_room else {
        return Decision::Keep(Reason::NoCandidate);
    };
    let s = ratio(
        u128::from(local.load.load.actors),
        capacity(local.load.load.actors, room),
    );
    if s < 200 {
        return Decision::Keep(Reason::Floor);
    }

    // N includes every same-app node announcing place, even one currently
    // ineligible as a destination. Count this sender exactly once.
    let others: std::collections::BTreeSet<_> = peers
        .iter()
        .filter(|p| p.node != local.node && p.app == local.app && p.place)
        .map(|p| p.node)
        .collect();
    let mut candidates: Vec<_> = peers
        .iter()
        .filter_map(|p| {
            if p.node == local.node
                || p.app != local.app
                || !p.live
                || p.drain != Some(DrainState::Serving)
                || p.drain_requested
                || p.suspected
                || !p.reachable
                || !p.place
                || p.age >= period.saturating_mul(3)
                || !p.backoff_remaining.is_zero()
                || p.accepted_age
                    .is_some_and(|age| age < period.saturating_mul(3))
            {
                return None;
            }
            let l = p.load?;
            let cap = capacity(l.load.actors, l.actor_room?);
            if ratio(
                u128::from(l.load.sockets),
                capacity(l.load.sockets, l.socket_room?),
            ) >= 800
            {
                return None;
            }
            let k = u128::from(budget.sent(p.node, p.heartbeat));
            let estimate = ratio(u128::from(l.load.actors) + k, cap);
            Some((
                p,
                weight(key, p.node),
                estimate,
                cap,
                k,
                u128::from(l.load.actors),
            ))
        })
        .collect();
    // Same total ordering as server cluster.rs::rendezvous, including the
    // node-id fallback for hash collisions. The third peer is never tried.
    candidates.sort_unstable_by(|a, b| (b.1, b.0.node).cmp(&(a.1, a.0.node)));
    candidates.truncate(2);
    let Some(&(p, _, c, cap, k, actors)) = candidates.iter().min_by(|a, b| {
        a.2.cmp(&b.2)
            .then_with(|| (b.1, b.0.node).cmp(&(a.1, a.0.node)))
    }) else {
        return Decision::Keep(Reason::NoCandidate);
    };
    if s * 4 <= c * 5 {
        return Decision::Keep(Reason::Band);
    }
    let t = s * 4 * cap / 5000;
    // A candidate implies N-1 >= 1. max(1) also makes malformed caller
    // input harmless; all arithmetic uses u128 to preserve u64 load counts.
    let limit = t.saturating_sub(actors) / (others.len() as u128).max(1);
    if k >= limit {
        return Decision::Keep(Reason::Budget);
    }
    Decision::Forward {
        node: p.node,
        reason: Reason::Lighter,
    }
}

/// The pure receiver policy; other admission gates remain the caller's job.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Arrival {
    Accept,
    InvalidMark,
    NoReturn,
    Capacity,
}

/// `sender_fill` is the parsed whole-number placed mark. `verified` means
/// the hop signature checked AND covered that value and sender identity.
/// No signature verification or request-path wiring happens here (P4).
/// Read `load` live and serialize this check with reserving the actor slot.
/// `sent_age` is elapsed on this receiver's monotonic clock since its last
/// send to this sender, regardless of that send's answer.
pub fn receive(
    load: RoomLoad,
    sender_fill: u64,
    verified: bool,
    sent_age: Option<Duration>,
    period: Duration,
) -> Arrival {
    if !verified || !(200..=1000).contains(&sender_fill) {
        return Arrival::InvalidMark;
    }
    if sent_age.is_some_and(|age| age < period.saturating_mul(3)) {
        return Arrival::NoReturn;
    }
    let Some(room) = load.actor_room else {
        return Arrival::Capacity;
    };
    let t = u128::from(sender_fill) * 4 * capacity(load.load.actors, room) / 5000;
    if u128::from(load.load.actors) + 1 > t {
        Arrival::Capacity
    } else {
        Arrival::Accept
    }
}

fn capacity(count: u64, room: u64) -> u128 {
    u128::from(count) + u128::from(room)
}
fn ratio(count: u128, capacity: u128) -> u128 {
    (count * 1000).checked_div(capacity).unwrap_or(1000)
}

/// Byte-for-byte algorithm of server cluster.rs::rendezvous. Kept local so
/// P2 does not refactor/wire that caller; golden unit vectors guard parity.
fn weight(key: &str, node: &str) -> u64 {
    let mut h = key
        .bytes()
        .chain([0])
        .chain(node.bytes())
        .fold(0xcbf2_9ce4_8422_2325_u64, |h, b| {
            (h ^ u64::from(b)).wrapping_mul(0x0000_0100_0000_01b3)
        });
    h = (h ^ (h >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    h = (h ^ (h >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    h ^ (h >> 31)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;
    const PERIOD: Duration = Duration::from_secs(3);
    fn load(actors: u64, room: u64) -> RoomLoad {
        RoomLoad {
            load: Load {
                actors,
                ..Load::default()
            },
            actor_room: Some(room),
            socket_room: Some(100),
        }
    }
    fn local(actors: u64) -> Local<'static> {
        Local {
            node: "self",
            app: "app",
            place: true,
            load: load(actors, 1000 - actors),
        }
    }
    fn peer(node: &'static str, actors: u64) -> Peer<'static> {
        Peer {
            node,
            app: "app",
            live: true,
            drain: Some(DrainState::Serving),
            drain_requested: false,
            suspected: false,
            reachable: true,
            place: true,
            load: Some(load(actors, 1000 - actors)),
            heartbeat: "h1",
            age: Duration::ZERO,
            backoff_remaining: Duration::ZERO,
            accepted_age: None,
        }
    }
    fn run(actors: u64, peers: &[Peer<'static>]) -> Decision<'static> {
        decide(
            local(actors),
            peers,
            "tenant/class/actor",
            PERIOD,
            &Budget::default(),
        )
    }
    fn forward(node: &'static str) -> Decision<'static> {
        Decision::Forward {
            node,
            reason: Reason::Lighter,
        }
    }

    #[test]
    fn floor_and_strict_integer_band() {
        assert_eq!(run(199, &[peer("a", 0)]), Decision::Keep(Reason::Floor));
        assert_eq!(run(200, &[peer("a", 0)]), forward("a"));
        assert_eq!(run(500, &[peer("a", 400)]), Decision::Keep(Reason::Band));
        assert_eq!(run(501, &[peer("a", 400)]), Decision::Keep(Reason::Budget));
        assert_eq!(run(500, &[peer("a", 399)]), forward("a"));
        assert_eq!(run(500, &[peer("a", 401)]), Decision::Keep(Reason::Band));
    }
    #[test]
    fn arrival_uses_live_capacity_verified_fill_and_wide_arithmetic() {
        assert_eq!(
            receive(load(39, 61), 500, true, None, PERIOD),
            Arrival::Accept
        );
        assert_eq!(
            receive(load(40, 60), 500, true, None, PERIOD),
            Arrival::Capacity
        );
        assert_eq!(
            receive(load(0, 1), 200, true, None, PERIOD),
            Arrival::Capacity
        );
        assert_eq!(
            receive(load(0, 0), 1000, true, None, PERIOD),
            Arrival::Capacity
        );
        assert_eq!(
            receive(load(u64::MAX, u64::MAX), 1000, true, None, PERIOD),
            Arrival::Accept
        );
        for s in [0, 199, 1001, u64::MAX] {
            assert_eq!(
                receive(load(0, 100), s, true, None, PERIOD),
                Arrival::InvalidMark
            );
        }
        assert_eq!(
            receive(load(0, 100), 500, false, None, PERIOD),
            Arrival::InvalidMark
        );
        let mut unknown = load(0, 100);
        unknown.actor_room = None;
        assert_eq!(receive(unknown, 500, true, None, PERIOD), Arrival::Capacity);
    }

    #[test]
    fn no_return_filters_before_rendezvous_and_expires_at_three_periods() {
        let mut peers = [peer("a", 0), peer("b", 0), peer("c", 0), peer("d", 0)];
        let before = PERIOD * 3 - Duration::from_nanos(1);
        peers[2].accepted_age = Some(before);
        peers[3].accepted_age = Some(before);
        assert_eq!(
            decide(local(900), &peers, "k", PERIOD, &Budget::default()),
            forward("b")
        );
        peers[2].accepted_age = Some(PERIOD * 3);
        assert_eq!(
            decide(local(900), &peers, "k", PERIOD, &Budget::default()),
            forward("c")
        );
        assert_eq!(
            receive(load(0, 100), 500, true, Some(before), PERIOD),
            Arrival::NoReturn
        );
        assert_eq!(
            receive(load(0, 100), 500, true, Some(PERIOD * 3), PERIOD),
            Arrival::Accept
        );
    }

    #[test]
    fn empty_self_and_disabled() {
        assert_eq!(run(900, &[]), Decision::Keep(Reason::NoCandidate));
        assert_eq!(
            run(900, &[peer("self", 0)]),
            Decision::Keep(Reason::NoCandidate)
        );
        let mut me = local(900);
        me.place = false;
        assert_eq!(
            decide(me, &[peer("a", 0)], "k", PERIOD, &Budget::default()),
            Decision::Keep(Reason::Disabled)
        );
    }
    #[test]
    fn every_candidate_gate_and_missing_room_field() {
        for gate in 0..12 {
            let mut p = peer("a", 0);
            match gate {
                0 => p.app = "other",
                1 => p.live = false,
                2 => p.drain = None,
                3 => p.drain = Some(DrainState::Draining),
                4 => p.drain_requested = true,
                5 => p.suspected = true,
                6 => p.reachable = false,
                7 => p.place = false,
                8 => p.load = None,
                9 => p.load.as_mut().unwrap().actor_room = None,
                10 => p.load.as_mut().unwrap().socket_room = None,
                _ => p.backoff_remaining = Duration::from_nanos(1),
            }
            assert_eq!(
                run(900, &[p]),
                Decision::Keep(Reason::NoCandidate),
                "gate {gate}"
            );
        }
    }
    #[test]
    fn freshness_is_three_periods_since_first_local_observation() {
        let mut p = peer("a", 0);
        p.age = Duration::from_secs(9) - Duration::from_nanos(1);
        assert_eq!(run(900, &[p]), forward("a"));
        p.age = Duration::from_secs(9);
        assert_eq!(run(900, &[p]), Decision::Keep(Reason::NoCandidate));
        p.age = Duration::from_secs(100);
        assert_eq!(run(900, &[p]), Decision::Keep(Reason::NoCandidate));
    }
    #[test]
    fn sockets_skip_at_800_and_zero_capacity_is_full() {
        let mut p = peer("a", 0);
        p.load.as_mut().unwrap().load.sockets = 799;
        p.load.as_mut().unwrap().socket_room = Some(201);
        assert_eq!(run(900, &[p]), forward("a"));
        p.load.as_mut().unwrap().load.sockets = 800;
        p.load.as_mut().unwrap().socket_room = Some(200);
        assert_eq!(run(900, &[p]), Decision::Keep(Reason::NoCandidate));
        p.load.as_mut().unwrap().load.sockets = 0;
        p.load.as_mut().unwrap().socket_room = Some(0);
        assert_eq!(run(900, &[p]), Decision::Keep(Reason::NoCandidate));
        assert_eq!(
            run(
                900,
                &[Peer {
                    load: Some(load(0, 0)),
                    ..peer("a", 0)
                }]
            ),
            Decision::Keep(Reason::Band)
        );
    }
    #[test]
    fn budget_is_per_sender_per_peer_heartbeat_and_counts_all_place_nodes() {
        let mut budget = Budget::default();
        let peers = [
            peer("a", 0),
            Peer {
                reachable: false,
                ..peer("b", 0)
            },
            Peer {
                place: false,
                ..peer("old", 0)
            },
        ];
        // s=500, t=400, N=3 (unreachable still announces), budget=200.
        for _ in 0..200 {
            assert_eq!(
                decide(local(500), &peers, "k", PERIOD, &budget),
                forward("a")
            );
            budget.charge("a", "h1");
        }
        assert_eq!(
            decide(local(500), &peers, "k", PERIOD, &budget),
            Decision::Keep(Reason::Budget)
        );
        assert_eq!(
            decide(local(500), &peers, "k", PERIOD, &Budget::default()),
            forward("a")
        );
        let mut new = peers;
        new[0].heartbeat = "h2";
        assert_eq!(decide(local(500), &new, "k", PERIOD, &budget), forward("a"));
        budget.charge("a", "h2");
        assert_eq!(budget.sent("a", "h2"), 1);
        assert_eq!(budget.sent("b", "h1"), 0);
    }
    #[test]
    fn estimated_fill_includes_credit_and_zero_share_keeps() {
        let mut budget = Budget::default();
        let peers = [peer("a", 399)];
        budget.charge("a", "h1");
        assert_eq!(
            decide(local(500), &peers, "k", PERIOD, &budget),
            Decision::Keep(Reason::Band)
        );
        let tiny = [Peer {
            load: Some(load(0, 1)),
            ..peer("a", 0)
        }];
        assert_eq!(
            decide(local(200), &tiny, "k", PERIOD, &Budget::default()),
            Decision::Keep(Reason::Budget)
        );
    }
    #[test]
    fn top_two_rendezvous_then_lighter_then_weight_tie_break() {
        // Golden ordering for key k from cluster.rs's FNV-1a/SplitMix64:
        // c=17456704261063364620, d=11920790254621522076,
        // b=11900083111828364613, a=2473509447625721475.
        assert_eq!(weight("k", "a"), 2_473_509_447_625_721_475);
        assert_eq!(weight("k", "b"), 11_900_083_111_828_364_613);
        assert_eq!(weight("k", "c"), 17_456_704_261_063_364_620);
        assert_eq!(weight("k", "d"), 11_920_790_254_621_522_076);
        let mut peers = [peer("a", 0), peer("b", 0), peer("c", 300), peer("d", 300)];
        let choose = |p: &[Peer<'static>]| decide(local(900), p, "k", PERIOD, &Budget::default());
        assert_eq!(choose(&peers), forward("c"));
        peers.reverse();
        assert_eq!(choose(&peers), forward("c"));
        peers[0].load = Some(load(299, 701)); // d is lighter
        assert_eq!(choose(&peers), forward("d"));
        peers[0].place = false;
        peers[1].place = false; // filter BEFORE selecting the top two
        assert_eq!(choose(&peers), forward("b"));
        // Same eligible set gives same choice regardless of sender identity.
        let mut me = local(900);
        me.node = "another-sender";
        assert_eq!(
            decide(me, &peers, "k", PERIOD, &Budget::default()),
            choose(&peers)
        );
    }
    #[test]
    fn exhausted_chosen_candidate_does_not_try_second() {
        let peers = [peer("a", 0), peer("b", 300)];
        let mut budget = Budget::default();
        for _ in 0..200 {
            budget.charge("a", "h1");
        }
        assert_eq!(
            decide(local(500), &peers, "k", PERIOD, &budget),
            Decision::Keep(Reason::Budget)
        );
    }
    #[test]
    fn rates_and_sender_sockets_are_not_resident_pressure() {
        let peers = [peer("a", 0)];
        let mut me = local(199);
        me.load.load.sockets = u64::MAX;
        me.load.load.events_per_min = Some(u64::MAX);
        me.load.load.front_per_min = Some(u64::MAX);
        assert_eq!(
            decide(me, &peers, "k", PERIOD, &Budget::default()),
            Decision::Keep(Reason::Floor)
        );
        me.load = load(u64::MAX, u64::MAX);
        assert_eq!(
            decide(me, &peers, "k", PERIOD, &Budget::default()),
            forward("a")
        );
        // Wide intermediates: actors+room and actors*1000 both exceed u64.
        let huge = [Peer {
            load: Some(load(u64::MAX, u64::MAX)),
            ..peer("a", 0)
        }];
        assert_eq!(
            decide(local(900), &huge, "k", PERIOD, &Budget::default()),
            forward("a")
        );
    }
}
