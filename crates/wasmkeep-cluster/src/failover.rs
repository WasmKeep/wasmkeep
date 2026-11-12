//! Whether a request for an actor another node holds is worth keeping
//! until that node's actors are free, and when to try again meanwhile.
//!
//! A node answers a request for an actor whose holder it cannot reach with
//! a 503 at once. When the holder is dead, the actor is free a lease
//! period plus grace after its last write ([`crate::node_lease::frees`]),
//! and a caller that had been kept until then would simply be served.
//! [`OwnerWatch`] says, from what this node already knows, whether that is
//! the case: the node leases in the heartbeat listing it makes anyway,
//! connections to a peer that failed here, and this node's own fences.
//! [`Waiter`] says when a kept request asks again.
//!
//! Nothing here grants anything. A verdict only chooses between answering
//! now and asking later; every takeover is still decided by
//! [`crate::node_lease::Owners::acquire`] on its own fresh reads, so a
//! wrong verdict costs a refused acquisition or a late answer, never a
//! second owner. Times are this node's clock, in ms, the same clock and
//! skew allowance as [`crate::node_lease::frees`] and
//! [`crate::node_lease::FenceWatch`].

use std::collections::HashMap;

use crate::lease::LeaseConfig;
use crate::membership::NodeInfo;
use crate::node_lease::NodeState;

/// A node's lease object as a listing showed it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Seen<'a> {
    /// The node's id.
    pub id: &'a str,
    /// The lease's state; `None` in a format from before node leases.
    pub state: Option<NodeState>,
    /// The process that wrote it.
    pub instance: Option<&'a str>,
    /// When, on its writer's clock (ms).
    pub written_at_ms: u64,
}

impl<'a> From<&'a NodeInfo> for Seen<'a> {
    fn from(node: &'a NodeInfo) -> Self {
        Self {
            id: &node.id,
            state: node.lease,
            instance: node.instance.as_deref(),
            written_at_ms: node.written_at_ms,
        }
    }
}

/// What this node knows about the holder of an actor it cannot serve.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    /// Nothing says the holder is going away (it renews its lease, or its
    /// object is absent, in an old format or dated ahead of this clock, none
    /// of which ever frees its actors by waiting): answer as without the
    /// wait.
    Forward,
    /// The holder is still live by its heartbeat, but a connection to it
    /// failed here and no write of its lease since proves it alive.
    Suspect,
    /// The holder's lease is `live` but too old to count as live, and the
    /// latest listing showed it unchanged: it stopped renewing. Its actors
    /// are free once a node fences it, not before `free_at_ms`.
    Stale {
        /// Its `written_at` plus ttl and grace.
        free_at_ms: u64,
    },
    /// The holder is `fenced` or `released`: its actors are free once
    /// `free_at_ms` has passed.
    Stopping {
        /// Its `written_at` plus ttl and grace.
        free_at_ms: u64,
        /// `released` (a clean stop), not `fenced`.
        released: bool,
    },
    /// The holder's actors look free now: fenced or released long enough
    /// ago, or just fenced by this node. A hint: the acquisition decides.
    Free,
    /// Another process of the holder's node is live since the request was
    /// kept: what the older process held is free, and what the new one
    /// took since is held by a live node.
    Restarted,
}

#[derive(Debug, Clone)]
struct Node {
    state: Option<NodeState>,
    instance: Option<String>,
    written_at_ms: u64,
    /// Whether the latest listing counted it live: written within the
    /// horizon of that listing, as the membership view decides. Not judged
    /// again as time passes: a node whose listings stall keeps its view,
    /// and takes no peer for dead on its own clock alone.
    in_view: bool,
    /// The latest listing showed a later `written_at` than the one before,
    /// under the same instance: the node writes.
    moved: bool,
    /// When (this clock) a listing first showed another instance than the
    /// one before.
    restarted_at_ms: Option<u64>,
}

/// A connection to a node failed here.
#[derive(Debug, Clone)]
struct Mark {
    instance: String,
    failed_at_ms: u64,
}

/// What this node knows about the other nodes' leases, for [`Verdict`]s.
#[derive(Debug, Clone)]
pub struct OwnerWatch {
    me: String,
    /// A lease's ttl plus grace.
    stale_ms: u64,
    max_skew_ms: u64,
    /// How long after its `written_at` a node still counts as live
    /// ([`crate::membership::is_live`]).
    horizon_ms: u64,
    nodes: HashMap<String, Node>,
    marks: HashMap<String, Mark>,
    /// Processes a listing proved alive after a failed connection, and
    /// until when they are not marked again.
    alive: HashMap<String, (String, u64)>,
    /// Nodes this node's own fence attempt found fenced or not live since
    /// the last listing.
    nudged: Vec<String>,
}

impl OwnerWatch {
    /// For the node `me`, with the lease timing and the heartbeat horizon
    /// of its membership view.
    pub fn new(me: impl Into<String>, config: &LeaseConfig, horizon_ms: u64) -> Self {
        Self {
            me: me.into(),
            stale_ms: config.ttl_ms().saturating_add(config.grace_ms()),
            max_skew_ms: config.max_skew_ms(),
            horizon_ms,
            nodes: HashMap::new(),
            marks: HashMap::new(),
            alive: HashMap::new(),
            nudged: Vec::new(),
        }
    }

    /// One listing of the node leases, at `now_ms`: every object it
    /// showed. A node it does not show is forgotten.
    pub fn observe<'a>(&mut self, listed: impl IntoIterator<Item = Seen<'a>>, now_ms: u64) {
        let mut nodes = HashMap::new();
        for seen in listed {
            if seen.id == self.me {
                continue;
            }
            let before = self.nodes.get(seen.id);
            let same_process = before.is_some_and(|b| b.instance.as_deref() == seen.instance);
            let restarted_at_ms = match before {
                Some(b) if same_process => b.restarted_at_ms,
                Some(_) => Some(now_ms),
                None => None,
            };
            nodes.insert(
                seen.id.to_owned(),
                Node {
                    state: seen.state,
                    instance: seen.instance.map(str::to_owned),
                    written_at_ms: seen.written_at_ms,
                    in_view: now_ms.saturating_sub(seen.written_at_ms) <= self.horizon_ms,
                    moved: same_process
                        && before.is_some_and(|b| seen.written_at_ms > b.written_at_ms),
                    restarted_at_ms,
                },
            );
        }
        self.nodes = nodes;
        // A listing replaces what a fence attempt said.
        self.nudged.clear();
        let (nodes, alive) = (&self.nodes, &mut self.alive);
        let (skew, horizon) = (self.max_skew_ms, self.horizon_ms);
        self.marks.retain(|id, mark| {
            let Some(node) = nodes.get(id) else {
                return false;
            };
            if node.state != Some(NodeState::Live)
                || node.instance.as_deref() != Some(mark.instance.as_str())
            {
                return false;
            }
            // Written after the failure, whatever the two clocks' skew
            // (each within the allowance of true time, so up to twice it
            // apart): the process is alive, only not reachable from here.
            // A write that landed before the failure and was first listed
            // after it does not pass.
            if node.written_at_ms > mark.failed_at_ms.saturating_add(skew.saturating_mul(2)) {
                alive.insert(
                    id.clone(),
                    (mark.instance.clone(), now_ms.saturating_add(horizon)),
                );
                return false;
            }
            true
        });
        self.alive.retain(|_, (_, until)| now_ms < *until);
    }

    /// A connection to `node` could not be opened at `now_ms`. Kept as a
    /// mark against the process last listed for it, until a listing shows
    /// a lease write later than this failure; nothing for a process such a
    /// listing proved alive within the last horizon, so a node that is
    /// alive but out of reach from here is not suspected over and over.
    pub fn connect_failed(&mut self, node: &str, now_ms: u64) {
        let Some(n) = self.nodes.get(node) else {
            return;
        };
        let (Some(NodeState::Live), Some(instance)) = (n.state, &n.instance) else {
            return;
        };
        if self
            .alive
            .get(node)
            .is_some_and(|(i, until)| i == instance && now_ms < *until)
        {
            return;
        }
        // The first failure counts: later ones must not push back the
        // write that would prove the process alive.
        if self
            .marks
            .get(node)
            .is_some_and(|m| &m.instance == instance)
        {
            return;
        }
        self.marks.insert(
            node.to_owned(),
            Mark {
                instance: instance.clone(),
                failed_at_ms: now_ms,
            },
        );
    }

    /// This node's fence attempt found `node` fenced, or not live: its
    /// actors may be free before the next listing says so.
    pub fn nudge(&mut self, node: &str) {
        if !self.nudged.iter().any(|n| n == node) {
            self.nudged.push(node.to_owned());
        }
    }

    /// What is known about `node` at `now_ms`. `held_since_ms`: since when
    /// the asking request has been kept (`None` for one just arrived); a
    /// restart of the node since then frees what its older process held.
    pub fn verdict(&self, node: &str, now_ms: u64, held_since_ms: Option<u64>) -> Verdict {
        if node == self.me {
            return Verdict::Forward;
        }
        if self.nudged.iter().any(|n| n == node) {
            return Verdict::Free;
        }
        let Some(n) = self.nodes.get(node) else {
            return Verdict::Forward;
        };
        let Some(state) = n.state else {
            return Verdict::Forward;
        };
        // Dated ahead of this clock: it never goes stale here, and waiting
        // for that date serves nobody.
        if n.written_at_ms > now_ms.saturating_add(self.max_skew_ms) {
            return Verdict::Forward;
        }
        let free_at_ms = n.written_at_ms.saturating_add(self.stale_ms);
        match state {
            NodeState::Fenced | NodeState::Released if now_ms > free_at_ms => Verdict::Free,
            NodeState::Fenced | NodeState::Released => Verdict::Stopping {
                free_at_ms,
                released: state == NodeState::Released,
            },
            NodeState::Live => {
                if let (Some(since), Some(restarted)) = (held_since_ms, n.restarted_at_ms)
                    && restarted >= since
                {
                    return Verdict::Restarted;
                }
                if n.in_view {
                    let marked = self
                        .marks
                        .get(node)
                        .is_some_and(|m| Some(m.instance.as_str()) == n.instance.as_deref());
                    if marked {
                        Verdict::Suspect
                    } else {
                        Verdict::Forward
                    }
                } else if n.moved {
                    // Late or skewed here, but writing.
                    Verdict::Forward
                } else {
                    Verdict::Stale { free_at_ms }
                }
            }
        }
    }
}

/// What a kept request does next.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Step {
    /// Ask for the actor again now.
    Try,
    /// The holder is alive: answer as without the wait.
    Leave,
    /// The deadline passed.
    Expire,
    /// Nothing until `until_ms`, or until the evidence changes (a listing,
    /// a fence), whichever comes first.
    Wait {
        /// When to look again at the latest.
        until_ms: u64,
    },
}

/// One kept request: when it asks again, and when it gives up.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Waiter {
    held_since_ms: u64,
    deadline_ms: u64,
    last_try_ms: u64,
    /// Since when the verdict has been [`Verdict::Free`] or
    /// [`Verdict::Restarted`] without a break.
    free_since_ms: Option<u64>,
    /// A try was made since the holder's node restarted.
    tried_restarted: bool,
    retry_ms: u64,
    jitter_ms: u64,
}

impl Waiter {
    /// Keeps a request that was just refused because another node holds
    /// its actor, if waiting can serve it: the holder is suspected, stale
    /// or stopping, and `deadline_ms` reaches the moment its actors can be
    /// free. `retry_ms`: the least time between two tries. `jitter_ms`: how
    /// long after the actor looks free the first try waits, so that the
    /// requests kept for one node's actors do not all ask at one instant.
    pub fn admit(
        verdict: Verdict,
        now_ms: u64,
        deadline_ms: u64,
        retry_ms: u64,
        jitter_ms: u64,
    ) -> Option<Self> {
        let reachable = match verdict {
            Verdict::Suspect => now_ms < deadline_ms,
            Verdict::Stale { free_at_ms } | Verdict::Stopping { free_at_ms, .. } => {
                free_at_ms < deadline_ms
            }
            Verdict::Forward | Verdict::Free | Verdict::Restarted => false,
        };
        reachable.then_some(Self {
            held_since_ms: now_ms,
            deadline_ms,
            last_try_ms: now_ms,
            free_since_ms: None,
            tried_restarted: false,
            retry_ms,
            jitter_ms,
        })
    }

    /// Since when the request has been kept.
    pub fn held_since_ms(&self) -> u64 {
        self.held_since_ms
    }

    /// A try ended at `now_ms` with the same holder still named.
    pub fn refused(&mut self, now_ms: u64) {
        self.last_try_ms = now_ms;
    }

    /// What to do at `now_ms`, given the holder's verdict now.
    ///
    /// A suspected holder, or one that stopped cleanly, is asked about
    /// every `retry_ms`: its record may be released any moment. A stale
    /// one is not asked about at all: nothing can free its actors before a
    /// fence, which a listing or this node's own attempt reports. A fenced
    /// one is asked about once its date has passed. After a restart of the
    /// holder's node the actor is asked for once: still refused, the new
    /// process holds it, and it is alive.
    pub fn step(&mut self, verdict: Verdict, now_ms: u64) -> Step {
        if now_ms >= self.deadline_ms {
            return Step::Expire;
        }
        if !matches!(verdict, Verdict::Free | Verdict::Restarted) {
            self.free_since_ms = None;
        }
        let due_ms = match verdict {
            Verdict::Forward => return Step::Leave,
            Verdict::Stale { .. } => {
                return Step::Wait {
                    until_ms: self.deadline_ms,
                };
            }
            Verdict::Stopping {
                free_at_ms,
                released: false,
            } => {
                return Step::Wait {
                    until_ms: free_at_ms.saturating_add(1).min(self.deadline_ms),
                };
            }
            Verdict::Suspect | Verdict::Stopping { released: true, .. } => {
                self.last_try_ms.saturating_add(self.retry_ms)
            }
            Verdict::Restarted if self.tried_restarted => return Step::Leave,
            Verdict::Free | Verdict::Restarted => {
                let since = *self.free_since_ms.get_or_insert(now_ms);
                self.last_try_ms
                    .saturating_add(self.retry_ms)
                    .max(since.saturating_add(self.jitter_ms))
            }
        };
        if now_ms >= due_ms {
            self.last_try_ms = now_ms;
            self.tried_restarted = verdict == Verdict::Restarted;
            Step::Try
        } else {
            Step::Wait {
                until_ms: due_ms.min(self.deadline_ms),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const HORIZON: u64 = 10_000;
    const T: u64 = 1_000_000;

    fn watch() -> OwnerWatch {
        // ttl 10 s, grace 5 s, skew 1 s.
        OwnerWatch::new("me", &LeaseConfig::default_safe(), HORIZON)
    }

    fn seen(
        id: &'static str,
        state: Option<NodeState>,
        instance: &'static str,
        at: u64,
    ) -> Seen<'static> {
        Seen {
            id,
            state,
            instance: Some(instance),
            written_at_ms: at,
        }
    }

    fn live(instance: &'static str, at: u64) -> Seen<'static> {
        seen("a", Some(NodeState::Live), instance, at)
    }

    #[test]
    fn a_node_that_renews_is_forwarded_to() {
        let mut w = watch();
        w.observe([live("i1", T)], T + 100);
        assert_eq!(w.verdict("a", T + 100, None), Verdict::Forward);
        // Up to the horizon.
        w.observe([live("i1", T)], T + HORIZON);
        assert_eq!(w.verdict("a", T + HORIZON, None), Verdict::Forward);
    }

    #[test]
    fn a_node_is_stale_by_a_listing_not_by_time_passing_without_one() {
        let mut w = watch();
        w.observe([live("i1", T)], T + 100);
        // This node's listings stalled: the peer stays in its view, and
        // requests go to it as they do without the wait.
        assert_eq!(w.verdict("a", T + 60_000, None), Verdict::Forward);
        assert_eq!(w.verdict("a", T + 60_000, Some(T + 200)), Verdict::Forward);
        w.connect_failed("a", T + 200);
        assert_eq!(w.verdict("a", T + 60_000, None), Verdict::Suspect);
    }

    #[test]
    fn nodes_whose_actors_waiting_never_frees_are_forwarded_to() {
        let mut w = watch();
        w.observe(
            [
                // A format from before node leases.
                seen("old", None, "i", T - 60_000),
                // Dated ahead of this clock by more than the skew.
                seen("ahead", Some(NodeState::Live), "i", T + 1_001),
                seen("ahead-fenced", Some(NodeState::Fenced), "i", T + 1_001),
                // This node itself.
                seen("me", Some(NodeState::Live), "i", T - 60_000),
            ],
            T,
        );
        for node in ["absent", "old", "ahead", "ahead-fenced", "me"] {
            assert_eq!(w.verdict(node, T, None), Verdict::Forward, "{node}");
            assert_eq!(
                w.verdict(node, T, Some(T - 5_000)),
                Verdict::Forward,
                "{node}"
            );
        }
        // Within the skew, a date ahead is an ordinary one.
        w.observe([seen("ahead", Some(NodeState::Fenced), "i", T + 1_000)], T);
        assert_eq!(
            w.verdict("ahead", T, None),
            Verdict::Stopping {
                free_at_ms: T + 16_000,
                released: false
            }
        );
    }

    #[test]
    fn a_node_that_stopped_writing_is_stale_once_it_left_the_horizon() {
        let mut w = watch();
        w.observe([live("i1", T)], T);
        w.observe([live("i1", T)], T + HORIZON + 1);
        assert_eq!(
            w.verdict("a", T + HORIZON + 1, None),
            Verdict::Stale {
                free_at_ms: T + 15_000
            }
        );
        // First listed already stale: the same.
        let mut w = watch();
        w.observe([live("i1", T)], T + 12_000);
        assert_eq!(
            w.verdict("a", T + 12_000, None),
            Verdict::Stale {
                free_at_ms: T + 15_000
            }
        );
    }

    #[test]
    fn a_node_out_of_the_horizon_that_still_writes_is_alive() {
        let mut w = watch();
        w.observe([live("i1", T)], T + 11_000);
        // Late, or its clock behind this one: but it wrote.
        w.observe([live("i1", T + 3_000)], T + 14_000);
        assert_eq!(w.verdict("a", T + 14_000, None), Verdict::Forward);
        assert_eq!(
            w.verdict("a", T + 14_000, Some(T + 11_000)),
            Verdict::Forward
        );
        // And stale again once a listing shows it unchanged.
        w.observe([live("i1", T + 3_000)], T + 17_000);
        assert_eq!(
            w.verdict("a", T + 17_000, None),
            Verdict::Stale {
                free_at_ms: T + 18_000
            }
        );
    }

    #[test]
    fn a_fenced_or_released_node_is_stopping_then_free() {
        for (state, released) in [(NodeState::Fenced, false), (NodeState::Released, true)] {
            let mut w = watch();
            w.observe([seen("a", Some(state), "i1", T)], T + 1_000);
            let stopping = Verdict::Stopping {
                free_at_ms: T + 15_000,
                released,
            };
            assert_eq!(w.verdict("a", T + 1_000, None), stopping);
            // As `frees`: not at the instant itself.
            assert_eq!(w.verdict("a", T + 15_000, None), stopping);
            assert_eq!(w.verdict("a", T + 15_001, None), Verdict::Free);
        }
    }

    #[test]
    fn a_failed_connect_makes_a_live_node_suspect() {
        let mut w = watch();
        w.observe([live("i1", T)], T + 100);
        w.connect_failed("a", T + 200);
        assert_eq!(w.verdict("a", T + 200, None), Verdict::Suspect);
        // Unchanged listings keep it, until the node leaves the horizon.
        w.observe([live("i1", T)], T + 3_000);
        assert_eq!(w.verdict("a", T + 3_000, None), Verdict::Suspect);
        w.observe([live("i1", T)], T + HORIZON + 1);
        assert_eq!(
            w.verdict("a", T + HORIZON + 1, None),
            Verdict::Stale {
                free_at_ms: T + 15_000
            }
        );
    }

    #[test]
    fn a_failed_connect_marks_only_a_listed_live_process() {
        let mut w = watch();
        w.connect_failed("a", T);
        assert_eq!(w.verdict("a", T, None), Verdict::Forward, "never listed");
        w.observe([seen("a", None, "i1", T)], T);
        w.connect_failed("a", T);
        assert_eq!(w.verdict("a", T, None), Verdict::Forward, "an old format");
        // Another process of the node since: the mark was the older one's.
        w.observe([live("i1", T)], T);
        w.connect_failed("a", T + 100);
        w.observe([live("i2", T + 200)], T + 300);
        assert_eq!(w.verdict("a", T + 300, None), Verdict::Forward);
    }

    #[test]
    fn a_write_listed_after_the_failure_but_made_before_it_does_not_clear_the_mark() {
        let mut w = watch();
        w.observe([live("i1", T)], T);
        // The node wrote at T + 2 900, this node last listed before that,
        // and the node died: the connect fails at T + 3 000.
        w.connect_failed("a", T + 3_000);
        w.observe([live("i1", T + 2_900)], T + 3_100);
        assert_eq!(w.verdict("a", T + 3_100, None), Verdict::Suspect);
        // The writer's clock ahead and this one behind, each by the skew
        // allowed: still not proof.
        w.observe([live("i1", T + 5_000)], T + 5_100);
        assert_eq!(w.verdict("a", T + 5_100, None), Verdict::Suspect);
    }

    #[test]
    fn a_write_after_the_failure_proves_the_node_alive_for_one_horizon() {
        let mut w = watch();
        w.observe([live("i1", T)], T);
        w.connect_failed("a", T + 3_000);
        // One ms past the failure plus twice the skew.
        w.observe([live("i1", T + 5_001)], T + 6_000);
        assert_eq!(w.verdict("a", T + 6_000, None), Verdict::Forward);
        // Not marked again while that is remembered,
        w.connect_failed("a", T + 7_000);
        assert_eq!(w.verdict("a", T + 7_000, None), Verdict::Forward);
        w.connect_failed("a", T + 15_999);
        w.observe([live("i1", T + 13_000)], T + 15_999);
        assert_eq!(w.verdict("a", T + 15_999, None), Verdict::Forward);
        // and again after one horizon: a node that was out of reach and
        // then dies is still noticed.
        w.observe([live("i1", T + 13_000)], T + 16_000);
        w.connect_failed("a", T + 16_100);
        assert_eq!(w.verdict("a", T + 16_100, None), Verdict::Suspect);
    }

    #[test]
    fn later_failures_do_not_push_back_the_proof() {
        let mut w = watch();
        w.observe([live("i1", T)], T);
        w.connect_failed("a", T + 1_000);
        w.connect_failed("a", T + 5_000);
        w.observe([live("i1", T + 3_001)], T + 5_100);
        assert_eq!(w.verdict("a", T + 5_100, None), Verdict::Forward);
    }

    #[test]
    fn a_restart_frees_what_the_older_process_held_for_a_kept_request() {
        let mut w = watch();
        w.observe([live("i1", T)], T + 11_000);
        let held_since = T + 11_500;
        assert!(matches!(
            w.verdict("a", held_since, Some(held_since)),
            Verdict::Stale { .. }
        ));
        w.observe([live("i2", T + 12_000)], T + 12_100);
        assert_eq!(
            w.verdict("a", T + 12_100, Some(held_since)),
            Verdict::Restarted
        );
        // A request that arrives now sees a live node like any other.
        assert_eq!(w.verdict("a", T + 12_200, None), Verdict::Forward);
        assert_eq!(
            w.verdict("a", T + 12_200, Some(T + 12_200)),
            Verdict::Forward
        );
    }

    #[test]
    fn this_nodes_own_fence_is_a_hint_until_the_next_listing() {
        let mut w = watch();
        w.observe([live("i1", T)], T + 15_500);
        w.nudge("a");
        assert_eq!(w.verdict("a", T + 15_600, Some(T + 12_000)), Verdict::Free);
        // The listing shows the fence: free by its date.
        w.observe([seen("a", Some(NodeState::Fenced), "i1", T)], T + 16_000);
        assert_eq!(w.verdict("a", T + 16_000, Some(T + 12_000)), Verdict::Free);
        // A nudge for an object that is gone lasts one listing.
        w.nudge("gone");
        assert_eq!(w.verdict("gone", T + 16_000, None), Verdict::Free);
        w.observe([], T + 19_000);
        assert_eq!(w.verdict("gone", T + 19_000, None), Verdict::Forward);
        assert_eq!(
            w.verdict("a", T + 19_000, Some(T + 12_000)),
            Verdict::Forward
        );
    }

    const RETRY: u64 = 1_100;
    const JITTER: u64 = 400;

    #[test]
    fn only_a_suspect_stale_or_stopping_holder_within_the_deadline_is_waited_for() {
        let admit = |v, deadline| Waiter::admit(v, T, deadline, RETRY, JITTER).is_some();
        assert!(!admit(Verdict::Forward, T + 60_000));
        assert!(!admit(Verdict::Free, T + 60_000));
        assert!(!admit(Verdict::Restarted, T + 60_000));
        assert!(admit(Verdict::Suspect, T + 1));
        assert!(!admit(Verdict::Suspect, T));
        for v in [
            Verdict::Stale {
                free_at_ms: T + 5_000,
            },
            Verdict::Stopping {
                free_at_ms: T + 5_000,
                released: false,
            },
            Verdict::Stopping {
                free_at_ms: T + 5_000,
                released: true,
            },
        ] {
            assert!(admit(v, T + 5_001), "{v:?}");
            // The deadline comes before the actor can be free.
            assert!(!admit(v, T + 5_000), "{v:?}");
        }
    }

    #[test]
    fn a_waiter_leaves_for_a_live_holder_and_expires_at_its_deadline() {
        let mut w = Waiter::admit(Verdict::Suspect, T, T + 20_000, RETRY, JITTER).unwrap();
        assert_eq!(w.held_since_ms(), T);
        assert_eq!(w.step(Verdict::Forward, T + 10), Step::Leave);
        assert_eq!(w.step(Verdict::Suspect, T + 20_000), Step::Expire);
        assert_eq!(w.step(Verdict::Free, T + 20_000), Step::Expire);
    }

    #[test]
    fn a_suspect_or_released_holder_is_asked_about_once_per_retry() {
        for v in [
            Verdict::Suspect,
            Verdict::Stopping {
                free_at_ms: T + 9_000,
                released: true,
            },
        ] {
            let mut w = Waiter::admit(v, T, T + 20_000, RETRY, JITTER).unwrap();
            assert_eq!(
                w.step(v, T + 10),
                Step::Wait {
                    until_ms: T + RETRY
                }
            );
            assert_eq!(w.step(v, T + RETRY), Step::Try);
            assert_eq!(
                w.step(v, T + RETRY + 1),
                Step::Wait {
                    until_ms: T + 2 * RETRY
                }
            );
            // Counted from when the try came back.
            w.refused(T + RETRY + 300);
            assert_eq!(
                w.step(v, T + 2 * RETRY),
                Step::Wait {
                    until_ms: T + 2 * RETRY + 300
                }
            );
        }
    }

    #[test]
    fn a_stale_holder_is_not_asked_about_and_a_fenced_one_only_at_its_date() {
        let stale = Verdict::Stale {
            free_at_ms: T + 9_000,
        };
        let mut w = Waiter::admit(stale, T, T + 20_000, RETRY, JITTER).unwrap();
        for at in [T + 10, T + 9_500, T + 19_999] {
            assert_eq!(
                w.step(stale, at),
                Step::Wait {
                    until_ms: T + 20_000
                }
            );
        }
        let fenced = Verdict::Stopping {
            free_at_ms: T + 9_000,
            released: false,
        };
        assert_eq!(
            w.step(fenced, T + 5_000),
            Step::Wait {
                until_ms: T + 9_001
            }
        );
    }

    #[test]
    fn a_free_actor_is_asked_for_after_the_jitter_then_once_per_retry() {
        let stale = Verdict::Stale {
            free_at_ms: T + 9_000,
        };
        let mut w = Waiter::admit(stale, T, T + 20_000, RETRY, JITTER).unwrap();
        assert_eq!(
            w.step(Verdict::Free, T + 9_500),
            Step::Wait {
                until_ms: T + 9_500 + JITTER
            }
        );
        assert_eq!(w.step(Verdict::Free, T + 9_500 + JITTER), Step::Try);
        // Still held (another node's clock is behind): once per retry.
        w.refused(T + 10_000);
        assert_eq!(
            w.step(Verdict::Free, T + 10_050),
            Step::Wait {
                until_ms: T + 10_000 + RETRY
            }
        );
        assert_eq!(w.step(Verdict::Free, T + 10_000 + RETRY), Step::Try);
        // Never sooner than a retry after the request was first refused.
        let mut w = Waiter::admit(stale, T, T + 20_000, RETRY, 0).unwrap();
        assert_eq!(
            w.step(Verdict::Free, T + 100),
            Step::Wait {
                until_ms: T + RETRY
            }
        );
        // The jitter counts from each time the actor starts to look free.
        let mut w = Waiter::admit(stale, T, T + 20_000, RETRY, JITTER).unwrap();
        assert!(matches!(
            w.step(Verdict::Free, T + 5_000),
            Step::Wait { .. }
        ));
        assert!(matches!(w.step(stale, T + 5_100), Step::Wait { .. }));
        assert_eq!(
            w.step(Verdict::Free, T + 6_000),
            Step::Wait {
                until_ms: T + 6_000 + JITTER
            }
        );
    }

    #[test]
    fn after_a_restart_the_actor_is_asked_for_once_then_left_to_the_new_process() {
        let stale = Verdict::Stale {
            free_at_ms: T + 9_000,
        };
        let mut w = Waiter::admit(stale, T, T + 20_000, RETRY, JITTER).unwrap();
        assert_eq!(
            w.step(Verdict::Restarted, T + 3_000),
            Step::Wait {
                until_ms: T + 3_000 + JITTER
            }
        );
        assert_eq!(w.step(Verdict::Restarted, T + 3_000 + JITTER), Step::Try);
        // Refused: the new process took the actor itself.
        w.refused(T + 3_500);
        assert_eq!(w.step(Verdict::Restarted, T + 3_600), Step::Leave);
    }
}
