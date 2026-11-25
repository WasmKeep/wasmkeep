//! Rolling upgrades of WasmKeep itself
//! ([ADR 0043](../../../docs/adr/0043-rolling-upgrades.md)): which versions
//! run beside each other, which formats a writer may write now, and the
//! state of an app's upgrade.
//!
//! Everything here is a rule over what was read (the app's floor, its
//! nodes' heartbeats) with the version that asks given as a value, so a
//! test can be any version. Only [`this_minor`] reads the build's own.
//!
//! - [`skew`]: a node or a CLI of minor version N runs beside N-1 to N+1
//!   and nothing else (section 5).
//! - [`FORMATS`] and [`gate`]: the gate table as data, and the one answer
//!   to "may this writer write this format now" (section 2).
//! - [`assess`], [`finalize_write`] and [`finalize_confirm`]: what
//!   `wasmkeep cluster upgrade status|finalize` do (section 4).
//! - `born_finalized`: the floor of an app that holds nothing (section 3).

use crate::app_format::floor::{self, Minor};
use crate::lease::{Bucket, BucketError};
use crate::membership::{Directory, NODES_PREFIX, NodeInfo, is_live, serves};

/// The minor version of this build ([`crate::VERSION`]).
///
/// # Errors
/// The version has no minor version to compare: nothing is judged by a
/// guess, so a node does not start and a command does not write.
pub fn this_minor() -> Result<Minor, String> {
    Minor::of_version(crate::VERSION).ok_or_else(|| {
        format!(
            "this build's version {} has no minor version to check the app's upgrade floor \
             and the other nodes' versions against",
            crate::VERSION
        )
    })
}

/// Whether a binary of minor version `this` runs beside one of `other`
/// (section 5): the same major version, and at most one minor apart.
pub fn beside(this: Minor, other: Minor) -> bool {
    this.major == other.major && this.minor.abs_diff(other.minor) <= 1
}

/// The range [`beside`] allows, as its messages name it: `0.8 to 0.10`.
fn range(this: Minor) -> String {
    format!("{} to {}", this.previous().unwrap_or(this), this.next())
}

/// The live nodes among `live` that a binary of minor version `this`
/// must not run beside (section 5), each as one sentence naming the node:
/// one on a minor version outside N-1 to N+1 or on another major version,
/// and one whose version line does not parse. Empty when it may run.
/// `live` holds the live nodes of one app, the asking node left out.
pub fn skew<'a>(this: Minor, live: impl IntoIterator<Item = &'a NodeInfo>) -> Vec<String> {
    live.into_iter()
        .filter_map(|n| match Minor::of_node_version(&n.version) {
            Some(other) if beside(this, other) => None,
            Some(_) => Some(format!(
                "node `{}` runs `{}`; this version ({this}) runs only beside {}. Upgrade one \
                 minor version at a time",
                n.id,
                n.version,
                range(this)
            )),
            None => Some(format!(
                "node `{}` names a version this one cannot read (`{}`); this version ({this}) \
                 runs only beside {}",
                n.id,
                n.version,
                range(this)
            )),
        })
        .collect()
}

/// Whether a node of minor version `this` may start over `floor` from
/// below (section 3): refused when the app has registered nodes and its
/// floor is below N-1, since the upgrade before this one was never
/// finalized. An app with no registered node passes: nothing ran it.
pub fn floor_from_below(floor: Option<Minor>, this: Minor, registered: bool) -> Result<(), String> {
    let floor = floor::in_force(floor);
    if !registered || floor >= this || this.previous() == Some(floor) {
        return Ok(());
    }
    let step = this.previous().map_or_else(
        || "the version before this one".to_owned(),
        |p| p.to_string(),
    );
    Err(format!(
        "this app's upgrade floor is {floor}: its upgrade to {step} was never finalized, and \
         this binary is {this}. Upgrade one minor version at a time: bring every node to \
         {step}, run `wasmkeep cluster upgrade finalize` with that version's `wasmkeep`, then \
         install {this}"
    ))
}

/// What the gate table says of one format (section 2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Rule {
    /// Never changed: it carries the gates, is signed between nodes, or
    /// is silently skipped in any other form. Written as it is, without
    /// a gate.
    Frozen,
    /// Extended only by what the previous minor version reads past
    /// (appended lines, new words). A test, and no gate.
    Extend,
    /// Written only when the app's floor is at or above the version that
    /// introduced it and every live node of the app announces these
    /// words. No word: every version since the first floor reads it.
    Gate(&'static [&'static str]),
    /// A form from before this version's writers, still decoded.
    ReadOnly,
    /// Not a cluster format: only the CLI reads it.
    CliOnly,
}

/// One row of the gate table: a header constant or a key family.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Format {
    /// The header as the sources spell it (`wasmkeep-manifest 5`), or a
    /// name for a key family or a form with no such header.
    pub name: &'static str,
    /// The minor version that introduced it.
    pub since: Minor,
    /// Its rule.
    pub rule: Rule,
    /// The test of what its reader does with a form it does not know;
    /// for a frozen form, the test that pins the bytes or the key shape
    /// both versions must agree on.
    pub unknown: &'static str,
}

const fn m(major: u32, minor: u32) -> Minor {
    Minor { major, minor }
}

const fn row(name: &'static str, since: Minor, rule: Rule, unknown: &'static str) -> Format {
    Format {
        name,
        since,
        rule,
        unknown,
    }
}

/// The row of the mark `wasmkeep cluster forget` leaves while it removes
/// a released node lease ([`crate::node_lease::forgotten`]): new in 0.9,
/// so behind the floor, with no word of its own. The node it matters to
/// is one that is starting, which no live-node gate sees; the floor is
/// what keeps a node that does not know the mark from starting.
const FORGOTTEN: Format = row(
    crate::node_lease::FORGOTTEN_HEADER,
    m(0, 9),
    Rule::Gate(&[]),
    "a_forgotten_mark_is_exact_or_not_one",
);

/// The gate table (ADR 0043, section 2), as data. A test fails when a
/// `wasmkeep-<name> <n>` literal exists in the crates without a row here.
/// `since` is the first release whose tree holds the form.
///
/// A manifest number inherits the words of those below it; they are
/// listed highest first, the order a refusal names them in.
pub const FORMATS: &[Format] = &[
    // The heartbeat and the node lease: they carry the gates.
    row(
        "wasmkeep-node 1",
        m(0, 1),
        Rule::ReadOnly,
        "a_later_header_is_never_read_as_a_known_one",
    ),
    row(
        "wasmkeep-node 2",
        m(0, 1),
        Rule::ReadOnly,
        "a_later_header_is_never_read_as_a_known_one",
    ),
    row(
        "wasmkeep-node 3",
        m(0, 1),
        Rule::Frozen,
        "a_later_header_is_never_read_as_a_known_one",
    ),
    FORGOTTEN,
    row(
        "wasmkeep-owner 1",
        m(0, 1),
        Rule::Gate(&[]),
        "a_later_header_is_never_read_as_a_known_one",
    ),
    row(
        "wasmkeep-owner 2",
        m(0, 5),
        Rule::Gate(&["recorded-head"]),
        "a_later_header_is_never_read_as_a_known_one",
    ),
    row(
        "wasmkeep-recorded-head 1",
        m(0, 5),
        Rule::Gate(&["recorded-head"]),
        "the_setting_is_absent_exact_or_refused",
    ),
    row(
        "wasmkeep-floor 1",
        m(0, 8),
        Rule::Frozen,
        "the_floor_is_absent_exact_or_refused",
    ),
    row(
        "wasmkeep-lease 2",
        m(0, 1),
        Rule::Frozen,
        "a_later_header_is_never_read_as_a_known_one",
    ),
    // The per-actor lease from before node leases (ADR 0020, section 6):
    // read once, to seed an actor's first record, and never written.
    row(
        "wasmkeep-lease 1",
        m(0, 1),
        Rule::ReadOnly,
        "a_later_header_is_never_read_as_a_known_one",
    ),
    // Deploys.
    row(
        "wasmkeep-manifest 1",
        m(0, 1),
        Rule::Gate(&[]),
        "manifests_with_unknown_or_inconsistent_lines_are_refused",
    ),
    row(
        "wasmkeep-manifest 2",
        m(0, 1),
        Rule::Gate(&[]),
        "manifests_with_unknown_or_inconsistent_lines_are_refused",
    ),
    row(
        "wasmkeep-manifest 3",
        m(0, 1),
        Rule::Gate(&["front"]),
        "manifests_with_unknown_or_inconsistent_lines_are_refused",
    ),
    row(
        "wasmkeep-manifest 4",
        m(0, 4),
        Rule::Gate(&["ends", "front"]),
        "manifests_with_unknown_or_inconsistent_lines_are_refused",
    ),
    row(
        "wasmkeep-manifest 5",
        m(0, 4),
        Rule::Gate(&["flush", "ends", "front"]),
        "manifests_with_unknown_or_inconsistent_lines_are_refused",
    ),
    row(
        "wasmkeep-manifest 6",
        m(0, 5),
        Rule::Gate(&["rate_limits", "flush", "ends", "front"]),
        "manifests_with_unknown_or_inconsistent_lines_are_refused",
    ),
    row(
        "wasmkeep-manifest 7",
        m(0, 5),
        Rule::Gate(&["assets", "rate_limits", "flush", "ends", "front"]),
        "manifests_with_unknown_or_inconsistent_lines_are_refused",
    ),
    // The first format behind the floor: the per-class placement keys
    // `apart` and `spread`, for classes with the app's own names.
    row(
        "wasmkeep-manifest 8",
        m(0, 9),
        Rule::Gate(&["apart", "assets", "rate_limits", "flush", "ends", "front"]),
        "manifests_with_unknown_or_inconsistent_lines_are_refused",
    ),
    row(
        "current",
        m(0, 4),
        Rule::Frozen,
        "current_reads_the_marker_and_old_readers_would_refuse_it",
    ),
    row(
        "wasmkeep-assets 1",
        m(0, 6),
        Rule::Gate(&["assets"]),
        "index_parser_requires_canonical_bytes",
    ),
    row(
        "wasmkeep-secrets 1",
        m(0, 1),
        Rule::Gate(&[]),
        "an_object_under_another_header_is_refused",
    ),
    row(
        "wasmkeep-app-deleted 1",
        m(0, 4),
        Rule::Gate(&["ends"]),
        "a_tombstone_is_read_by_its_id_line_alone",
    ),
    // Actors' files and restores.
    row(
        "wasmkeep-blob-sweep 1",
        m(0, 6),
        Rule::Gate(&["blobs"]),
        "a_later_header_is_never_read_as_a_known_one",
    ),
    row(
        "wasmkeep-blob-usage 1",
        m(0, 6),
        Rule::Extend,
        "a_count_under_another_header_is_no_count",
    ),
    row(
        "wasmkeep-restore 1",
        m(0, 5),
        Rule::Gate(&["actor-restore", "blobs"]),
        "a_later_header_is_never_read_as_a_known_one",
    ),
    // Between nodes (section 7): a 0.9 node signs and sends as 0.8.1.
    row(
        "wasmkeep-call v4",
        m(0, 1),
        Rule::Frozen,
        "a_v4_signature_signs_these_bytes",
    ),
    row(
        "wasmkeep-alarm 1",
        m(0, 1),
        Rule::Frozen,
        "an_alarm_hand_off_body_reads_back_and_junk_does_not",
    ),
    row(
        "wasmkeep-take 1",
        m(0, 1),
        Rule::Frozen,
        "a_take_body_reads_back_and_the_signature_covers_its_marker",
    ),
    row(
        "wasmkeep-sender v1",
        m(0, 6),
        Rule::Frozen,
        "a_call_sends_these_bytes",
    ),
    row(
        "wasmkeep-ingress-identity-v2",
        m(0, 5),
        Rule::Frozen,
        "a_buffered_forward_sends_these_bytes",
    ),
    // Key families: any other shape is skipped without an error.
    row(
        "keys: history",
        m(0, 1),
        Rule::Frozen,
        "history_keys_carry_the_prefix",
    ),
    row(
        "keys: alarm index",
        m(0, 1),
        Rule::Frozen,
        "alarm_keys_carry_the_prefix",
    ),
    row(
        "keys: control intents",
        m(0, 1),
        Rule::Frozen,
        "junk_keys_are_ignored",
    ),
    // The CLI's own.
    row(
        "backup run format 1",
        m(0, 6),
        Rule::CliOnly,
        "a_run_of_a_later_format_is_refused",
    ),
];

/// The row of [`FORMATS`] named `name`.
pub fn format(name: &str) -> Option<&'static Format> {
    FORMATS.iter().find(|f| f.name == name)
}

/// The deploy manifest's row for format `number` (1 to 8 today).
pub fn manifest_format(number: u8) -> Option<&'static Format> {
    format(&format!("wasmkeep-manifest {number}"))
}

/// Why a writer may not write a format now ([`gate`]).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Blocked {
    /// The app's floor is below the version that introduced the format:
    /// its upgrade is not finalized.
    #[error(
        "{format} needs every node at {needs} or later, and this app's upgrade floor is {floor}: \
         once every node runs {needs}, run `wasmkeep cluster upgrade finalize`"
    )]
    Floor {
        /// The format.
        format: &'static str,
        /// The version that introduced it.
        needs: Minor,
        /// The floor in force.
        floor: Minor,
    },
    /// Live nodes of the app run a version from before the format: one
    /// that started while the floor was written and read it before the
    /// write.
    #[error(
        "{format} needs every live node at {needs} or later; older, or naming a version that \
         does not parse: {}. Stop or upgrade each",
        nodes.join(", ")
    )]
    OlderNodes {
        /// The format.
        format: &'static str,
        /// The version that introduced it.
        needs: Minor,
        /// The live nodes below it, each with its version.
        nodes: Vec<String>,
    },
    /// Live nodes of the app do not announce a word the format needs.
    #[error("{format} needs `{word}` on every live node; without it: {}", nodes.join(", "))]
    Nodes {
        /// The format.
        format: &'static str,
        /// The first word some live node lacks.
        word: &'static str,
        /// The live nodes without it, by id.
        nodes: Vec<String>,
    },
    /// The floor record is unreadable: nothing is written (fail closed).
    #[error("the app's upgrade floor is unreadable ({len} bytes); refusing to write")]
    FloorUnreadable {
        /// The record's length.
        len: usize,
    },
}

/// The ids of the nodes among `live` that do not announce every word of
/// `words`: the one test behind every "all live nodes announce W".
pub fn without<'a>(words: &[&str], live: impl IntoIterator<Item = &'a NodeInfo>) -> Vec<String> {
    live.into_iter()
        .filter(|n| !words.iter().all(|w| n.announces(w)))
        .map(|n| n.id.clone())
        .collect()
}

/// Whether a writer may write `format` now (ADR 0043, section 2), from
/// the app's floor as read and the live nodes of the app. A gated format
/// needs both halves: the floor at or above the version that introduced
/// it (the node that was stopped when the gate opened), and every live
/// node able to read it (a node that started while the floor was
/// written, and a capability a node's configuration withholds). The
/// second half is the format's words and, for a format newer than the
/// first floor, the live nodes' versions too: a format gated on the floor
/// alone has no word to keep it from a live older node. A format without
/// a gate is written as it is.
///
/// # Errors
/// What blocks, naming the nodes.
pub fn gate<'a>(
    format: &Format,
    floor: Result<Option<Minor>, floor::Unknown>,
    live: impl IntoIterator<Item = &'a NodeInfo> + Clone,
) -> Result<(), Blocked> {
    let Rule::Gate(words) = format.rule else {
        return Ok(());
    };
    let floor = floor::in_force(floor.map_err(|e| Blocked::FloorUnreadable { len: e.len })?);
    if floor < format.since {
        return Err(Blocked::Floor {
            format: format.name,
            needs: format.since,
            floor,
        });
    }
    // Formats that exist in 0.8.1 keep their live words alone.
    if format.since > floor::ABSENT {
        let nodes: Vec<String> = live
            .clone()
            .into_iter()
            .filter(|n| Minor::of_node_version(&n.version).is_none_or(|v| v < format.since))
            .map(|n| format!("{} ({})", n.id, n.version))
            .collect();
        if !nodes.is_empty() {
            return Err(Blocked::OlderNodes {
                format: format.name,
                needs: format.since,
                nodes,
            });
        }
    }
    for word in words {
        let nodes = without(&[word], live.clone());
        if !nodes.is_empty() {
            return Err(Blocked::Nodes {
                format: format.name,
                word,
                nodes,
            });
        }
    }
    Ok(())
}

/// Whether `wasmkeep cluster forget` may leave its mark while it removes
/// a released lease of `app` (`tenant/app`): [`gate`] on the mark's row,
/// from the app's floor and its live nodes, read here at `now_ms`. Until
/// the app's upgrade to 0.9 is finalized a node of 0.8.1 may start under
/// that id, and it would write its lease over a mark it does not know.
///
/// The gate is per app, `app` being the one the released lease names,
/// while a node id is a key of the whole prefix (`nodes/<id>`). It says
/// nothing of the other apps under the prefix. Their nodes of 0.8.1 list
/// the mark and skip it (an unknown first line is no heartbeat to them).
/// But a node of 0.8.1 started under this very id for another app, one
/// that is not finalized, passes that app's floor and writes its lease
/// over the mark, which the command's delete then removes: the gap from
/// before the mark, for a node id moved between apps during the removal.
///
/// # Errors
/// The bucket did not answer. Inside: what blocks the mark.
pub async fn forget_gate<B: Bucket + Directory>(
    bucket: &B,
    app: &str,
    now_ms: u64,
    horizon_ms: u64,
) -> Result<Result<(), Blocked>, BucketError> {
    let (tenant, name) = app.split_once('/').unwrap_or((app, ""));
    let floor = bucket.get(&floor::relative_key(tenant, name)).await?;
    let floor = floor::decode(floor.as_ref().map(|(body, _)| body.as_slice()));
    // The floor first, alone: before the finalize the nodes are not
    // listed (a prefix that holds many released leases is why `forget`
    // is run).
    if let Err(blocked) = gate(&FORGOTTEN, floor.clone(), std::iter::empty()) {
        return Ok(Err(blocked));
    }
    let nodes = crate::membership::all_heartbeats(bucket).await?;
    let live = nodes
        .iter()
        .filter(|n| serves(n, app) && is_live(n, now_ms, horizon_ms));
    Ok(gate(&FORGOTTEN, floor, live))
}

/// Whether a writer of minor version `this` may write to an app at all
/// (ADR 0043, sections 3 and 5): not under a floor above its own version
/// ("this `wasmkeep` is older than the cluster"), not with an unreadable
/// floor, and not beside a live node outside its range.
///
/// # Errors
/// Why not, and which `wasmkeep` may write instead.
pub fn writer<'a>(
    this: Minor,
    floor: Result<Option<Minor>, floor::Unknown>,
    live: impl IntoIterator<Item = &'a NodeInfo>,
) -> Result<(), String> {
    match floor {
        Err(e) => {
            return Err(format!(
                "the app's upgrade floor is unreadable ({} bytes); refusing to write",
                e.len
            ));
        }
        Ok(Some(floor)) if floor > this => {
            return Err(format!(
                "this `wasmkeep` is older than the cluster: this app's upgrade was finalized at \
                 {floor} and this is {this}. Use a `wasmkeep` of {floor} or later, within one \
                 minor version of every live node"
            ));
        }
        Ok(_) => {}
    }
    let outside = skew(this, live);
    if outside.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "{}. Use a `wasmkeep` within one minor version of every live node, and not older \
             than the app's upgrade floor",
            outside.join("; ")
        ))
    }
}

/// The state of an app's upgrade (section 4), one per app.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum State {
    /// Every live node on the floor's minor version.
    OneVersion,
    /// Live nodes on two minor versions, next to each other.
    Mixed,
    /// Every live node on one minor version, the floor below it.
    FinalizePending,
    /// No node of the app is live: nothing to compare.
    NoLiveNode,
    /// A live node runs below the floor. Either it passed its floor reads
    /// before the floor was written and serves, or its read after its
    /// first lease write has had no answer and it keeps checking without
    /// serving: the bucket does not say which, the node's readiness does.
    BelowFloor,
    /// Something the skew check refuses, or an unreadable floor.
    Refused,
}

impl State {
    /// The state as `upgrade status` and `cluster status` print it.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::OneVersion => "one version",
            Self::Mixed => "mixed",
            Self::FinalizePending => "finalize pending",
            Self::NoLiveNode => "no live node",
            Self::BelowFloor => "node below the floor",
            Self::Refused => "refused",
        }
    }

    /// The exit code of `upgrade status` for this state: 0 when there is
    /// nothing to do, 3 while an upgrade is under way (or nothing is live
    /// to judge), 1 when something is wrong.
    pub fn exit_code(self) -> u8 {
        match self {
            Self::OneVersion => 0,
            Self::Mixed | Self::FinalizePending | Self::NoLiveNode => 3,
            Self::BelowFloor | Self::Refused => 1,
        }
    }
}

/// One registered node of an app, as `upgrade status` shows it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeVersion {
    /// The node's id.
    pub id: String,
    /// Its version line, as written.
    pub version: String,
    /// Its minor version; `None` if the line does not parse.
    pub minor: Option<Minor>,
    /// Whether its heartbeat is live.
    pub live: bool,
    /// Its node lease's state; `None` for a heartbeat from before node
    /// leases.
    pub lease: Option<crate::node_lease::NodeState>,
    /// Whether its lease proves its process stopped ([`Self::stopped`]).
    pub proven_stopped: bool,
}

impl NodeVersion {
    /// Whether its lease says its process stopped for sure: `released`
    /// (a clean stop), or `fenced` for longer than a lease's ttl plus
    /// grace after its last write (a fenced process stops at its next
    /// write, and acts no later than that). A lease still `live` but
    /// stale proves nothing: its node may be cut off or slow, and still
    /// act.
    pub fn stopped(&self) -> bool {
        self.proven_stopped
    }

    /// Whether it runs below `target`, or names a version that does not
    /// parse.
    fn older_than(&self, target: Minor) -> bool {
        self.minor.is_none_or(|v| v < target)
    }
}

/// One app's upgrade, as read ([`assess`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppUpgrade {
    /// The app, `tenant/app`.
    pub app: String,
    /// Its floor record: `Ok(None)` without one, `Err` with the length of
    /// a body this version cannot read.
    pub floor: Result<Option<Minor>, usize>,
    /// Its registered nodes, by id.
    pub nodes: Vec<NodeVersion>,
    /// Its state.
    pub state: State,
    /// The state in one sentence, naming nodes and versions.
    pub detail: String,
    /// The live nodes that block a finalize at the newest live version,
    /// by id.
    pub blocks: Vec<String>,
}

impl AppUpgrade {
    /// The floor in force (`0.8` without a record); `None` if unreadable.
    pub fn floor_in_force(&self) -> Option<Minor> {
        self.floor.ok().map(floor::in_force)
    }

    /// The floor as one word: `0.9`, `none (0.8)`, or `unreadable`.
    pub fn floor_text(&self) -> String {
        match self.floor {
            Ok(Some(floor)) => floor.to_string(),
            Ok(None) => format!("none ({})", floor::ABSENT),
            Err(len) => format!("unreadable ({len} bytes)"),
        }
    }

    /// The app's state and floor in one line, for `cluster status`.
    pub fn line(&self) -> String {
        format!(
            "{}: floor {}, {}: {}",
            self.app,
            self.floor_text(),
            self.state.as_str(),
            self.detail
        )
    }
}

fn named(nodes: &[&NodeVersion]) -> String {
    nodes
        .iter()
        .map(|n| format!("{} ({})", n.id, n.version))
        .collect::<Vec<_>>()
        .join(", ")
}

/// The state of `app`'s upgrade (section 4) from its floor as read and
/// every heartbeat under the prefix (`nodes`; those of other apps are
/// ignored), with `now_ms` and `horizon_ms` deciding which are live.
pub fn assess(
    app: &str,
    floor: Result<Option<Minor>, floor::Unknown>,
    nodes: &[NodeInfo],
    now_ms: u64,
    horizon_ms: u64,
) -> AppUpgrade {
    let nodes: Vec<NodeVersion> = nodes
        .iter()
        .filter(|n| serves(n, app))
        .map(|n| NodeVersion {
            id: n.id.clone(),
            version: n.version.clone(),
            minor: Minor::of_node_version(&n.version),
            live: is_live(n, now_ms, horizon_ms),
            lease: n.lease,
            proven_stopped: match n.lease {
                Some(crate::node_lease::NodeState::Released) => true,
                // The default timings: one read cannot know a cluster's.
                Some(crate::node_lease::NodeState::Fenced) => {
                    let lease = crate::lease::LeaseConfig::default_safe();
                    now_ms.saturating_sub(n.written_at_ms)
                        >= lease.ttl_ms().saturating_add(lease.grace_ms())
                }
                _ => false,
            },
        })
        .collect();
    let floor = floor.map_err(|e| e.len);
    let live: Vec<&NodeVersion> = nodes.iter().filter(|n| n.live).collect();
    let unparsed: Vec<&NodeVersion> = live.iter().copied().filter(|n| n.minor.is_none()).collect();
    let mut minors: Vec<Minor> = live.iter().filter_map(|n| n.minor).collect();
    minors.sort();
    minors.dedup();
    let newest = minors.last().copied();
    let on = |minor: Minor| -> Vec<&NodeVersion> {
        live.iter()
            .copied()
            .filter(|n| n.minor == Some(minor))
            .collect()
    };
    let below = |minor: Minor| -> Vec<&NodeVersion> {
        live.iter()
            .copied()
            .filter(|n| n.minor.is_some_and(|v| v < minor))
            .collect()
    };
    let mut blocks: Vec<String> = unparsed.iter().map(|n| n.id.clone()).collect();
    if let Some(newest) = newest {
        blocks.extend(below(newest).iter().map(|n| n.id.clone()));
        // An older lease that is stale but neither released nor fenced
        // blocks too: it is not proof of a stop.
        blocks.extend(
            nodes
                .iter()
                .filter(|n| !n.live && !n.stopped() && n.older_than(newest))
                .map(|n| n.id.clone()),
        );
    }
    blocks.sort();
    let (state, detail) = match (floor, minors.as_slice()) {
        (Err(len), _) => (
            State::Refused,
            format!(
                "the floor record is unreadable ({len} bytes): no node starts and nothing is \
                 written until it is repaired"
            ),
        ),
        (Ok(_), _) if !unparsed.is_empty() => (
            State::Refused,
            format!(
                "live nodes name a version that does not parse: {}",
                named(&unparsed)
            ),
        ),
        (Ok(_), []) => (State::NoLiveNode, "no node of this app is live".to_owned()),
        (Ok(found), [oldest, ..]) if *oldest < floor::in_force(found) => {
            let floor = floor::in_force(found);
            (
                State::BelowFloor,
                format!(
                    "{} live below the floor {floor}. Such a node either passed its floor reads \
                     before the floor was written and serves, or got no answer on its floor \
                     read after its first lease write and keeps checking without serving (its \
                     readiness answer says which). Stop it; formats that need {floor} stay \
                     refused while it is live",
                    named(&below(floor))
                ),
            )
        }
        (Ok(_), [oldest, .., newest]) if !beside(*oldest, *newest) || minors.len() > 2 => (
            State::Refused,
            format!(
                "live nodes are more than one minor version apart ({}): each version runs only \
                 beside the one before and the one after",
                minors
                    .iter()
                    .map(|v| format!("{v} on {}", named(&on(*v))))
                    .collect::<Vec<_>>()
                    .join("; ")
            ),
        ),
        (Ok(_), [older, newer]) => (
            State::Mixed,
            format!(
                "{} on {older}, {} on {newer}",
                named(&on(*older)),
                named(&on(*newer))
            ),
        ),
        (Ok(found), [only]) if floor::in_force(found) < *only => (
            State::FinalizePending,
            format!(
                "every live node runs {only} and the floor is {}: run `wasmkeep cluster upgrade \
                 finalize` to end the upgrade (no node of an older version can start after it)",
                floor::in_force(found)
            ),
        ),
        (Ok(_), [only]) => (State::OneVersion, format!("every live node runs {only}")),
        // More than two versions: refused above.
        (Ok(_), _) => (State::Refused, "live nodes run several versions".to_owned()),
    };
    AppUpgrade {
        app: app.to_owned(),
        floor,
        nodes,
        state,
        detail,
        blocks,
    }
}

/// Why `finalize` wrote nothing (section 4, steps 1 to 3).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum FinalizeRefusal {
    /// The floor is above the version that finalizes.
    #[error(
        "this `wasmkeep` is older than the cluster: the floor is already {floor} and this is \
         {target}"
    )]
    FloorAbove {
        /// The floor found.
        floor: Minor,
        /// The version that asked.
        target: Minor,
    },
    /// A live node runs a newer minor version.
    #[error(
        "live nodes run a newer version than this `wasmkeep` ({target}): {nodes}. Finalize with \
         that version's `wasmkeep`"
    )]
    NewerLive {
        /// The version that asked.
        target: Minor,
        /// The nodes, each with its version.
        nodes: String,
    },
    /// A live node runs an older minor version, or one that does not
    /// parse.
    #[error(
        "live nodes are older than {target}: {nodes}. Upgrade or stop each of them, then \
         finalize"
    )]
    OlderLive {
        /// The version that asked.
        target: Minor,
        /// The nodes, each with its version.
        nodes: String,
    },
    /// An older node is not live, but its lease was neither released
    /// nor fenced: stale is not stopped.
    #[error(
        "nodes older than {target} are not live, but their lease was never released, nor \
         fenced for longer than a lease's ttl plus grace: {nodes}. A stale lease is not proof \
         of a stop (the node may be cut off or slow, and \
         still act): run `wasmkeep cluster fence <id>` for each ({ids}), then finalize. For a \
         heartbeat from before node leases, stop the node and use `wasmkeep cluster forget <id>`"
    )]
    OlderNotStopped {
        /// The version that asked.
        target: Minor,
        /// The nodes, each with its version.
        nodes: String,
        /// Their ids, to fence.
        ids: String,
    },
    /// No node of the app is live.
    #[error("no node of this app is live: there is nothing to finalize")]
    NoLiveNode,
    /// The floor record is unreadable.
    #[error("the app's upgrade floor is unreadable ({len} bytes); nothing written")]
    Unreadable {
        /// The record's length.
        len: usize,
    },
    /// The bucket did not answer.
    #[error("bucket: {0}")]
    Bucket(String),
}

/// What `finalize` would do, or did, to an app's floor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Finalize {
    /// The floor is already at the target: nothing to write.
    Already(Minor),
    /// The floor goes from this (`None`: no record) to the target.
    Raise(Option<Minor>, Minor),
}

/// Steps 1 to 3 of `finalize` (section 4), from what was read: the floor
/// already at `target` passes at once (the command is idempotent);
/// otherwise every live node of the app must run `target`, and every
/// older node must have stopped for sure: its lease `released` or
/// `fenced`, not merely stale.
///
/// # Errors
/// The precondition that fails, naming the nodes.
pub fn finalize_check(target: Minor, status: &AppUpgrade) -> Result<Finalize, FinalizeRefusal> {
    let found = status
        .floor
        .map_err(|len| FinalizeRefusal::Unreadable { len })?;
    match found {
        Some(floor) if floor == target => return Ok(Finalize::Already(floor)),
        Some(floor) if floor > target => {
            return Err(FinalizeRefusal::FloorAbove { floor, target });
        }
        _ => {}
    }
    let live: Vec<&NodeVersion> = status.nodes.iter().filter(|n| n.live).collect();
    let newer: Vec<&NodeVersion> = live
        .iter()
        .copied()
        .filter(|n| n.minor.is_some_and(|v| v > target))
        .collect();
    if !newer.is_empty() {
        return Err(FinalizeRefusal::NewerLive {
            target,
            nodes: named(&newer),
        });
    }
    let older: Vec<&NodeVersion> = live
        .iter()
        .copied()
        .filter(|n| n.minor.is_none_or(|v| v < target))
        .collect();
    if !older.is_empty() {
        return Err(FinalizeRefusal::OlderLive {
            target,
            nodes: named(&older),
        });
    }
    let stale: Vec<&NodeVersion> = status
        .nodes
        .iter()
        .filter(|n| !n.live && !n.stopped() && n.older_than(target))
        .collect();
    if !stale.is_empty() {
        return Err(FinalizeRefusal::OlderNotStopped {
            target,
            nodes: named(&stale),
            ids: stale
                .iter()
                .map(|n| n.id.as_str())
                .collect::<Vec<_>>()
                .join(", "),
        });
    }
    if live.is_empty() {
        return Err(FinalizeRefusal::NoLiveNode);
    }
    Ok(Finalize::Raise(found, target))
}

/// Reads `tenant/app`'s floor and every heartbeat, and assesses them.
///
/// # Errors
/// The bucket did not answer.
pub async fn read_status<B: Bucket + Directory>(
    bucket: &B,
    tenant: &str,
    app: &str,
    now_ms: u64,
    horizon_ms: u64,
) -> Result<AppUpgrade, BucketError> {
    let nodes = crate::membership::all_heartbeats(bucket).await?;
    status_of(
        bucket,
        &format!("{tenant}/{app}"),
        &nodes,
        now_ms,
        horizon_ms,
    )
    .await
}

/// `app`'s floor, read, and its state among `nodes` (every heartbeat
/// under the prefix, read already).
///
/// # Errors
/// The bucket did not answer.
pub async fn status_of<B: Bucket>(
    bucket: &B,
    app: &str,
    nodes: &[NodeInfo],
    now_ms: u64,
    horizon_ms: u64,
) -> Result<AppUpgrade, BucketError> {
    let (tenant, name) = app.split_once('/').unwrap_or((app, ""));
    let floor = bucket.get(&floor::relative_key(tenant, name)).await?;
    let floor = floor::decode(floor.as_ref().map(|(body, _)| body.as_slice()));
    Ok(assess(app, floor, nodes, now_ms, horizon_ms))
}

/// The apps `nodes` name, sorted: those with registered nodes.
pub fn apps_of(nodes: &[NodeInfo]) -> Vec<String> {
    let mut apps: Vec<String> = nodes
        .iter()
        .map(|n| n.app.clone())
        .filter(|app| {
            app.split_once('/')
                .is_some_and(|(t, a)| !t.is_empty() && !a.is_empty())
        })
        .collect();
    apps.sort();
    apps.dedup();
    apps
}

/// The state of every app that has registered nodes under the prefix
/// (or of `only`, `tenant/app`, whether it has any), from one listing of
/// the heartbeats and one read of each app's floor.
///
/// # Errors
/// The bucket did not answer.
pub async fn read_statuses<B: Bucket + Directory>(
    bucket: &B,
    only: Option<&str>,
    now_ms: u64,
    horizon_ms: u64,
) -> Result<Vec<AppUpgrade>, BucketError> {
    let nodes = crate::membership::all_heartbeats(bucket).await?;
    let apps = match only {
        Some(app) => vec![app.to_owned()],
        None => apps_of(&nodes),
    };
    let mut out = Vec::with_capacity(apps.len());
    for app in apps {
        out.push(status_of(bucket, &app, &nodes, now_ms, horizon_ms).await?);
    }
    Ok(out)
}

/// The apps that have registered nodes under the prefix, sorted: what
/// `finalize` and `upgrade status` cover when no app is named.
///
/// # Errors
/// The bucket did not answer.
pub async fn apps_with_nodes<B: Directory>(bucket: &B) -> Result<Vec<String>, BucketError> {
    Ok(apps_of(&crate::membership::all_heartbeats(bucket).await?))
}

/// Steps 1 to 4 of `finalize` for one app (section 4): checks the floor
/// and the live nodes as read at `now_ms`, then raises the floor to
/// `target` by conditional write. The caller then waits one start check
/// and calls [`finalize_confirm`]: the second listing is not optional.
///
/// # Errors
/// A precondition failed (nothing written), or the bucket did not answer.
pub async fn finalize_write<B: Bucket + Directory>(
    bucket: &B,
    tenant: &str,
    app: &str,
    target: Minor,
    now_ms: u64,
    horizon_ms: u64,
) -> Result<Finalize, FinalizeRefusal> {
    let bucket_failed = |e: BucketError| FinalizeRefusal::Bucket(e.to_string());
    let status = read_status(bucket, tenant, app, now_ms, horizon_ms)
        .await
        .map_err(bucket_failed)?;
    let plan = finalize_check(target, &status)?;
    if let Finalize::Raise(..) = plan {
        match floor::raise(bucket, &floor::relative_key(tenant, app), target).await {
            // A lost race was read again: another finalize got there.
            Ok(floor::Raised::Kept(floor)) if floor == target => {
                return Ok(Finalize::Already(floor));
            }
            Ok(floor::Raised::Kept(floor)) => {
                return Err(FinalizeRefusal::FloorAbove { floor, target });
            }
            Ok(_) => {}
            Err(floor::RaiseError::Unreadable { len, .. }) => {
                return Err(FinalizeRefusal::Unreadable { len });
            }
            Err(floor::RaiseError::Bucket(e)) => return Err(FinalizeRefusal::Bucket(e)),
        }
    }
    Ok(plan)
}

/// What the listing after a finalize found (section 4, steps 5 and 6).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Confirmed {
    /// Nodes below the floor that are live, or whose lease is stale but
    /// neither released nor fenced: each passed its floor reads before
    /// the floor was written, or has had no answer on its second read and
    /// keeps checking. An error: the floor stays, formats that need it
    /// stay refused while such a node is live, and it must be stopped (or
    /// fenced).
    pub late: Vec<NodeVersion>,
    /// Registered nodes of an older version whose lease is released or
    /// fenced. Not an error: such a node stops at its next start.
    pub stopped_older: Vec<NodeVersion>,
}

/// Step 5 of `finalize` (section 4): the listing after the floor is
/// written and one start check has passed. It closes the race the floor's
/// readers leave open: a node whose first lease write landed after the
/// first listing and whose second floor read came before the write.
///
/// # Errors
/// The bucket did not answer.
pub async fn finalize_confirm<B: Bucket + Directory>(
    bucket: &B,
    tenant: &str,
    app: &str,
    target: Minor,
    now_ms: u64,
    horizon_ms: u64,
) -> Result<Confirmed, BucketError> {
    let status = read_status(bucket, tenant, app, now_ms, horizon_ms).await?;
    let older = |n: &&NodeVersion| n.older_than(target);
    Ok(Confirmed {
        late: status
            .nodes
            .iter()
            .filter(|n| n.live || !n.stopped())
            .filter(older)
            .cloned()
            .collect(),
        stopped_older: status
            .nodes
            .iter()
            .filter(|n| !n.live && n.stopped())
            .filter(older)
            .cloned()
            .collect(),
    })
}

/// How long `finalize` waits before its second listing (section 4, step
/// 5): one start check, a lease's ttl plus grace, plus a heartbeat
/// horizon. A node that was starting when the floor was written has by
/// then stopped itself at its second read, or shows as live.
pub fn finalize_wait_ms(lease: &crate::lease::LeaseConfig, horizon_ms: u64) -> u64 {
    lease
        .ttl_ms()
        .saturating_add(lease.grace_ms())
        .saturating_add(horizon_ms)
}

/// What one listing of [`NODES_PREFIX`] says about `app` for a starting
/// node: the heartbeats that decode, and the bodies that do not.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Listed {
    /// Every heartbeat that decodes, of any app.
    pub nodes: Vec<NodeInfo>,
    /// The objects that do not decode as a heartbeat, key and body: a
    /// version this one cannot read may write them.
    pub unread: Vec<(String, Vec<u8>)>,
}

impl Listed {
    /// Whether any node is registered for `app`, live or not, the node
    /// `but` left out. An object that does not decode counts: it cannot
    /// be shown to belong to another app.
    pub fn registered(&self, app: &str, but: &str) -> bool {
        self.nodes.iter().any(|n| serves(n, app) && n.id != but)
            || self
                .unread
                .iter()
                .any(|(key, _)| key != &format!("{NODES_PREFIX}{but}"))
    }

    /// [`Self::registered`] for the node `id` of minor version `this`
    /// that is starting: its own earlier lease is left out only when it
    /// names `this` version or a later one. One that names an older
    /// version (or none that parses) is the app's own past: a node
    /// restarted in place on a newer binary is not the first node of a
    /// new app.
    pub fn registered_at_start(&self, app: &str, id: &str, this: Minor) -> bool {
        self.registered(app, id)
            || self.nodes.iter().any(|n| {
                n.id == id
                    && serves(n, app)
                    && Minor::of_node_version(&n.version).is_none_or(|v| v < this)
            })
    }

    /// The objects of `self` that do not decode and that `earlier` held
    /// with another body, or not at all: rewritten between two listings,
    /// so a node this version cannot read is running (section 5).
    pub fn rewritten<'a>(&'a self, earlier: &Self) -> Vec<&'a str> {
        self.unread
            .iter()
            .filter(|(key, body)| {
                earlier
                    .unread
                    .iter()
                    .find(|(k, _)| k == key)
                    .is_none_or(|(_, was)| was != body)
            })
            .map(|(key, _)| key.as_str())
            .collect()
    }
}

/// One listing of the heartbeats, undecodable objects kept ([`Listed`]).
///
/// # Errors
/// The bucket did not answer.
pub async fn list_nodes<D: Directory>(directory: &D) -> Result<Listed, BucketError> {
    let mut listed = Listed::default();
    for (key, body) in directory.list(NODES_PREFIX).await? {
        // As `all_heartbeats`: a heartbeat speaks for the node its key
        // names, with a valid id.
        match NodeInfo::decode(&body).filter(|n| {
            key == format!("{NODES_PREFIX}{}", n.id)
                && crate::membership::validate_node_id(&n.id).is_ok()
        }) {
            Some(node) => listed.nodes.push(node),
            // An operator's fence over no object names no node and is
            // written once: not a heartbeat, and not a running node.
            None if crate::node_lease::NodeLeaseObject::decode(&body)
                .is_some_and(|o| crate::node_lease::created_fence(&o)) => {}
            // The mark of a `cluster forget`: not a node, and no version
            // this one cannot read wrote it.
            None if crate::node_lease::forgotten(&body).is_some() => {}
            None => listed.unread.push((key, body)),
        }
    }
    listed.nodes.sort_by(|a, b| a.id.cmp(&b.id));
    Ok(listed)
}

/// Who creates a fresh app's floor ([`born_finalized`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FirstWriter<'a> {
    /// A starting node, by its id: an app served without a deploy, or
    /// one whose first deploy wrote no floor.
    Node(&'a str),
    /// The app's first deploy.
    Deploy,
}

/// What [`born_finalized`] found.
#[cfg(feature = "object-store")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Born {
    /// The app held nothing: its floor was created at this version.
    Created(Minor),
    /// The floor was created at this version, and the listing after the
    /// create shows these nodes of the app below it, live or not stopped
    /// for sure: each started between the listing and the create. As
    /// after a finalize, the floor stays and each must be stopped.
    Late {
        /// The floor created.
        floor: Minor,
        /// The nodes, each with its version.
        nodes: String,
    },
    /// The app has a floor already (or another writer created it first).
    HasFloor,
    /// Nothing written, for this reason: the app is not fresh, or no
    /// floor may be chosen for it yet.
    NotFresh(&'static str),
}

/// The floor the first writer of a fresh app creates (ADR 0043, section
/// 3): its first deploy, or its first node. From the writer's minor
/// version (`cli`: the `wasmkeep` that deploys, or the node that starts)
/// and the other live nodes under the bucket prefix, of every app (a new
/// app has no node of its own yet, and the nodes that will serve it are
/// most likely those that serve the others). `None`: no floor is
/// written, and the app stays on the floor that no record means until a
/// `finalize`.
///
/// - No other live node under the prefix: the writer's own version.
/// - A live node whose version does not parse: none.
/// - Otherwise the lower of the CLI's version and the lowest live
///   version, so no live node is shut out of the new app; none when that
///   is not above what no record already means.
pub fn deploy_floor<'a>(
    cli: Minor,
    live_on_prefix: impl IntoIterator<Item = &'a NodeInfo>,
) -> Option<Minor> {
    let mut lowest = None;
    for node in live_on_prefix {
        let minor = Minor::of_node_version(&node.version)?;
        lowest = Some(lowest.map_or(minor, |l: Minor| l.min(minor)));
    }
    match lowest {
        None => Some(cli),
        Some(lowest) => Some(cli.min(lowest)).filter(|floor| *floor > floor::ABSENT),
    }
}

/// A fresh app is born finalized (ADR 0043, section 3): the first writer
/// into an app that never ran creates its floor, by conditional create,
/// so a new app needs no `finalize` before its first gated deploy. `this`
/// is the writer's minor version; a heartbeat is live for `horizon_ms`
/// at `now_ms`.
///
/// An app never ran when it has no floor record, no registered node, no
/// tombstone and nothing under `state/<tenant>/<app>/`.
///
/// - **No registered node.** An object under `nodes/` that does not
///   decode counts as one. A starting node leaves its own earlier lease
///   out only if that lease names its own version or a later one
///   ([`Listed::registered_at_start`]).
/// - **A deploy** (`apps/<tenant>/<app>/current`) makes the app one that
///   ran for the deploy that writes it, which is then not the first. A
///   starting node ignores it: with `serve --app` a node always starts
///   after a deploy, and the deploy may have written no floor
///   ([`deploy_floor`]). An app with a deploy, no state and no other
///   registered node has nothing a node of the version before could
///   lose.
/// - "No node registered" alone is not proof: an app whose node leases
///   were all removed with `cluster forget` still has its state, and
///   creating its floor would end the way back without a `finalize`.
/// - Secrets set before the first deploy, or the uploads of a deploy
///   that never finished, do not make an app one that ran.
///
/// Both writers create the floor [`deploy_floor`] gives for their own
/// version and the other live nodes under the prefix: their own version
/// when none is live, never above the lowest live one, and none when
/// that is not above what no record means. After the create the nodes
/// are listed again ([`Born::Late`]).
///
/// # Errors
/// The bucket did not answer: nothing is concluded, and the caller goes
/// on without a floor.
#[cfg(feature = "object-store")]
pub async fn born_finalized(
    bucket: &crate::object_store::ObjectStoreBucket,
    tenant: &str,
    app: &str,
    by: FirstWriter<'_>,
    this: Minor,
    now_ms: u64,
    horizon_ms: u64,
) -> Result<Born, BucketError> {
    let key = floor::relative_key(tenant, app);
    if bucket.get(&key).await?.is_some() {
        return Ok(Born::HasFloor);
    }
    let served = format!("{tenant}/{app}");
    let listed = list_nodes(bucket).await?;
    let registered = match by {
        FirstWriter::Node(id) => listed.registered_at_start(&served, id, this),
        FirstWriter::Deploy => listed.registered(&served, ""),
    };
    if registered {
        return Ok(Born::NotFresh("a node is registered for it"));
    }
    if bucket
        .get(&format!("apps/{served}/deleted"))
        .await?
        .is_some()
    {
        return Ok(Born::NotFresh("it was deleted"));
    }
    if by == FirstWriter::Deploy
        && bucket
            .get(&format!("apps/{served}/current"))
            .await?
            .is_some()
    {
        return Ok(Born::NotFresh("it has a deploy"));
    }
    if bucket.any_key(&format!("state/{served}")).await? {
        return Ok(Born::NotFresh("it has state"));
    }
    // One rule for both writers: never above the lowest version live
    // under the prefix, the writer itself left out. A node that is the
    // first of its app beside older nodes of other apps would otherwise
    // shut them out of it, as a deploy would.
    let own = match by {
        FirstWriter::Node(id) => id,
        FirstWriter::Deploy => "",
    };
    let live = listed
        .nodes
        .iter()
        .filter(|n| n.id != own && is_live(n, now_ms, horizon_ms));
    let Some(target) = deploy_floor(this, live) else {
        return Ok(Born::NotFresh(
            "the live nodes under the prefix run a version at or below the floor that no \
             record means, or one that does not parse",
        ));
    };
    match bucket.create(&key, floor::encode(target)).await {
        Ok(_) => {}
        Err(BucketError::Conflict) => return Ok(Born::HasFloor),
        Err(e) => return Err(e),
    }
    // As `finalize` lists again after its write: a node of the app that
    // started between the listing above and the create never saw the
    // floor.
    let again = assess(
        &served,
        Ok(Some(target)),
        &list_nodes(bucket).await?.nodes,
        now_ms,
        horizon_ms,
    );
    let late: Vec<&NodeVersion> = again
        .nodes
        .iter()
        .filter(|n| n.id != own && (n.live || !n.stopped()) && n.older_than(target))
        .collect();
    if late.is_empty() {
        Ok(Born::Created(target))
    } else {
        Ok(Born::Late {
            floor: target,
            nodes: named(&late),
        })
    }
}

#[cfg(test)]
mod tests;
