//! Cluster-wide intents written by an operator: drain a node, move an actor
//! ([ADR 0014](../../../docs/adr/0014-draining-a-node-and-moving-an-actor.md)),
//! delete an actor ([ADR 0028](../../../docs/adr/0028-actors-that-end.md)).
//!
//! An intent is an object *key*; bodies are empty and never read. Nodes
//! list the intents' prefixes reading keys and their last-modified time
//! (the bucket's clock) only.
//!
//! ```text
//! control/drain/<node-id>
//! state/<tenant>/<app>/moves/<class>/<name hex>/<target node-id>
//! state/<tenant>/<app>/deletes/<class>/<name hex>/<request id>
//! control/changed
//! ```
//!
//! Every writer of an intent (the CLI, a node deleting a move or a delete
//! request it acted on) rewrites `control/changed` with a new body right
//! after ([`announce`]). Nodes read that one object every heartbeat period
//! and list the intents only when its body changed, or every
//! [`LISTING_FALLBACK_EVERY`] periods whatever it says, which covers
//! writers that do not announce (CLIs of 0.4.1 and before) and an announce
//! that failed ([`ControlWatch`]; ADR 0014, amendment of 2026-11-14).
//!
//! A drain names a node, shared by the bucket prefix; a move or a delete
//! is an app's, under its state root (ADR 0022). Each delete request has
//! an id of its own, so a node that carried one out deletes exactly that
//! key and never a later request for the same actor; its last-modified
//! time orders it among them. Moves written under the layout before
//! 0.4.0 (`control/move/<tenant>/...`) are not read.

use std::collections::BTreeSet;

use crate::lease::BucketError;
use crate::membership::validate_node_id;
use crate::state_root::{Family, STATE_PREFIX, StateRoot};

/// Key prefix of every intent.
pub const CONTROL_PREFIX: &str = "control/";
const DRAIN: &str = "control/drain/";

/// Rewritten with a new body after every intent written or deleted: nodes
/// list the intents when it changes.
pub const CHANGED: &str = "control/changed";

/// Heartbeat periods after which a node lists the intents even though
/// [`CHANGED`] did not change: 60 s at the default 3 s period.
pub const LISTING_FALLBACK_EVERY: u32 = 20;

/// A move key older than this, by the bucket's clock, is ignored and
/// deleted by the next node that lists it.
pub const MOVE_TTL_MS: u64 = 5 * 60 * 1000;

/// A delete request older than this, by the bucket's clock, is ignored
/// and deleted by the next node that lists it (ADR 0028).
pub const DELETE_TTL_MS: u64 = 60 * 60 * 1000;

/// Keys and their last-modified time, and deletes: what intents need.
#[allow(async_fn_in_trait)]
pub trait Keys {
    /// Every key under `prefix`, with its last-modified time (ms since the
    /// epoch, the bucket's clock). Bodies are not read.
    async fn list_keys(&self, prefix: &str) -> Result<Vec<(String, u64)>, BucketError>;
    /// Writes `key` with an empty body.
    async fn put_empty(&self, key: &str) -> Result<(), BucketError>;
    /// Writes `key` with `body`, unconditionally.
    async fn put_body(&self, key: &str, body: Vec<u8>) -> Result<(), BucketError>;
    /// The body of `key`, `None` if it does not exist.
    async fn get_body(&self, key: &str) -> Result<Option<Vec<u8>>, BucketError>;
    /// Deletes `key`; deleting a missing key succeeds.
    async fn delete(&self, key: &str) -> Result<(), BucketError>;
}

/// An operator's request to move an actor to a node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MoveIntent {
    /// The actor's tenant.
    pub tenant: String,
    /// The actor's app, within the tenant.
    pub app: String,
    /// The actor's class.
    pub class: String,
    /// The actor's name, as given.
    pub name: String,
    /// The node it should move to (a preference, not a pin).
    pub target: String,
    /// When the key was written, by the bucket's clock (ms).
    pub written_at_ms: u64,
}

impl MoveIntent {
    /// Its key: the key it was listed under, under the root of the app
    /// the listing gave (the prefix plays no part in a key relative to
    /// it).
    pub fn key(&self) -> String {
        move_key(
            &StateRoot::new("", self.tenant.as_str(), self.app.as_str()),
            &self.class,
            &self.name,
            &self.target,
        )
    }
}

/// An operator's request to delete an actor (ADR 0028).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteIntent {
    /// The actor's tenant.
    pub tenant: String,
    /// The actor's app, within the tenant.
    pub app: String,
    /// The actor's class.
    pub class: String,
    /// The actor's name, as given.
    pub name: String,
    /// The request's own id ([`new_request_id`]).
    pub id: String,
    /// When the key was written, by the bucket's clock (ms): what the
    /// actor records once the request is carried out.
    pub written_at_ms: u64,
}

impl DeleteIntent {
    /// Its key, relative to the bucket prefix.
    pub fn key(&self) -> String {
        delete_key(
            &StateRoot::new("", self.tenant.as_str(), self.app.as_str()),
            &self.class,
            &self.name,
            &self.id,
        )
    }
}

/// What became of a delete request: written by the node that carried it
/// out, beside the request, before it deletes the request's key, and read
/// (then deleted) by the command waiting for it. The key going away alone
/// proves nothing (it also expires).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeleteOutcome {
    /// The delete is recorded in the bucket.
    Done,
    /// No such actor: nothing was deleted or created.
    Absent,
}

impl DeleteOutcome {
    const ALL: [Self; 2] = [Self::Done, Self::Absent];

    fn suffix(self) -> &'static str {
        match self {
            Self::Done => ".done",
            Self::Absent => ".absent",
        }
    }

    /// The outcome's key for the request at `request_key`.
    pub fn key(self, request_key: &str) -> String {
        format!("{request_key}{}", self.suffix())
    }

    /// The outcome `key` records for the request at `request_key`, if any.
    pub fn of(key: &str, request_key: &str) -> Option<Self> {
        let suffix = key.strip_prefix(request_key)?;
        Self::ALL.into_iter().find(|o| o.suffix() == suffix)
    }
}

/// `<tenant>/<app>/deletes/<class>/<name hex>/<id>.<outcome>`, after
/// `state/`.
fn parse_outcome(rest: &str) -> bool {
    let parts: Vec<&str> = rest.split('/').collect();
    let [_, _, family, _, _, last] = parts.as_slice() else {
        return false;
    };
    *family == Family::Deletes.folder()
        && DeleteOutcome::ALL
            .into_iter()
            .any(|o| last.strip_suffix(o.suffix()).is_some_and(request_id))
}

/// A fresh id for a delete request: 16 hex digits, from the standard
/// library's randomly seeded hasher and the clock.
pub fn new_request_id() -> String {
    use std::hash::{BuildHasher as _, Hasher as _};
    let mut h = std::collections::hash_map::RandomState::new().build_hasher();
    if let Ok(t) = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
        h.write_u128(t.as_nanos());
    }
    h.write_u32(std::process::id());
    format!("{:016x}", h.finish())
}

fn request_id(id: &str) -> bool {
    (1..=32).contains(&id.len()) && id.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
}

/// What the intents say, as of one listing.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Control {
    /// Nodes asked to drain.
    pub draining: BTreeSet<String>,
    /// Moves not yet expired.
    pub moves: Vec<MoveIntent>,
    /// Deletes not yet expired.
    pub deletes: Vec<DeleteIntent>,
}

/// An intent that cannot be written: a bad node id, tenant or class.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct InvalidIntent(pub String);

/// The key that asks node `node` to drain.
pub fn drain_key(node: &str) -> Result<String, InvalidIntent> {
    validate_node_id(node).map_err(|e| InvalidIntent(e.to_string()))?;
    Ok(format!("{DRAIN}{node}"))
}

/// The key that asks for actor `class/name` of `root` to move to
/// `target`. The root's tenant and app and the class must be single key
/// segments;
/// the name is hex-encoded, so any name fits.
pub fn checked_move_key(
    root: &StateRoot,
    class: &str,
    name: &str,
    target: &str,
) -> Result<String, InvalidIntent> {
    validate_node_id(target).map_err(|e| InvalidIntent(e.to_string()))?;
    check_actor(root, class, name)?;
    Ok(move_key(root, class, name, target))
}

/// The key of request `id` ([`new_request_id`]) to delete actor
/// `class/name` of `root` (ADR 0028), with the same checks as
/// [`checked_move_key`].
pub fn checked_delete_key(
    root: &StateRoot,
    class: &str,
    name: &str,
    id: &str,
) -> Result<String, InvalidIntent> {
    check_actor(root, class, name)?;
    if !request_id(id) {
        return Err(InvalidIntent(format!(
            "invalid request id {id:?}: 1 to 32 lowercase hex digits"
        )));
    }
    Ok(delete_key(root, class, name, id))
}

fn check_actor(root: &StateRoot, class: &str, name: &str) -> Result<(), InvalidIntent> {
    for (what, part) in [
        ("tenant", root.tenant()),
        ("app", root.app()),
        ("class", class),
    ] {
        if !segment(part) {
            return Err(InvalidIntent(format!(
                "invalid {what} {part:?}: one non-empty path segment"
            )));
        }
    }
    if name.is_empty() {
        return Err(InvalidIntent("empty actor name".into()));
    }
    Ok(())
}

fn delete_key(root: &StateRoot, class: &str, name: &str, id: &str) -> String {
    root.key(
        Family::Deletes,
        &format!("{class}/{}/{id}", hex(name.as_bytes())),
    )
}

fn move_key(root: &StateRoot, class: &str, name: &str, target: &str) -> String {
    root.key(
        Family::Moves,
        &format!("{class}/{}/{target}", hex(name.as_bytes())),
    )
}

fn segment(s: &str) -> bool {
    !s.is_empty()
        && s != "."
        && s != ".."
        && !s.contains('/')
        && !s.chars().any(|c| c.is_whitespace() || c.is_control())
}

/// Tells the nodes that an intent was written or deleted: rewrites
/// [`CHANGED`] with `token`, which must differ from any body written
/// before (a time with sub-millisecond digits and the writer's process
/// id do). Called after the intent's own write or delete has been
/// acknowledged, so a node that reads the new body and then lists sees
/// the intent.
pub async fn announce<K: Keys>(keys: &K, token: &str) -> Result<(), BucketError> {
    keys.put_body(CHANGED, format!("{token}\n").into_bytes())
        .await
}

/// When a node lists the intents (ADR 0014, amendment of 2026-11-14): at
/// its first period, whenever [`CHANGED`] reads differently than at the
/// last successful listing, and every [`LISTING_FALLBACK_EVERY`] periods
/// anyway. A failed read of [`CHANGED`] lists nothing (the bucket is not
/// answering) unless the fallback is due; a failed listing is tried again
/// next period, the last intents kept meanwhile.
#[derive(Debug, Clone, Default)]
pub struct ControlWatch {
    /// What [`CHANGED`] read before the last successful listing.
    listed_with: Listed,
    /// Periods since the last successful listing.
    since_listing: u32,
}

/// [`ControlWatch::listed_with`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
enum Listed {
    /// No successful listing yet.
    #[default]
    Never,
    /// Listed after a read of [`CHANGED`] that failed: the next good read
    /// lists again.
    Unread,
    /// Listed after reading this (`None`: absent).
    Marker(Option<Vec<u8>>),
}

impl ControlWatch {
    /// No listing yet: the first period lists.
    pub fn new() -> Self {
        Self::default()
    }

    /// Whether to list this period, given this period's read of
    /// [`CHANGED`] (`Err(())`: the read failed). Counts the period.
    pub fn should_list(&mut self, marker: Result<Option<&[u8]>, ()>) -> bool {
        self.since_listing = self.since_listing.saturating_add(1);
        let fallback = self.since_listing >= LISTING_FALLBACK_EVERY;
        match (&self.listed_with, marker) {
            (Listed::Never, _) => true,
            (_, Err(())) => fallback,
            (Listed::Unread, Ok(_)) => true,
            (Listed::Marker(then), Ok(now)) => fallback || then.as_deref() != now,
        }
    }

    /// A listing succeeded after this period's read of [`CHANGED`]
    /// (`None`: that read failed).
    pub fn listed(&mut self, marker: Option<Option<Vec<u8>>>) {
        self.since_listing = 0;
        self.listed_with = marker.map_or(Listed::Unread, Listed::Marker);
    }
}

/// Reads the intents: drain markers, and the moves and deletes of
/// `moves_of`'s app (none without it) written within [`MOVE_TTL_MS`]
/// (moves) or [`DELETE_TTL_MS`] (deletes) of `now_ms`. Older ones are
/// deleted, best effort. Keys that do not parse are
/// ignored. A failed listing is an error: callers keep what they read
/// last (ADR 0014).
pub async fn read<K: Keys>(
    keys: &K,
    moves_of: Option<&StateRoot>,
    now_ms: u64,
) -> Result<Control, BucketError> {
    let mut listed = keys.list_keys(DRAIN).await?;
    if let Some(root) = moves_of {
        listed.extend(
            keys.list_keys(&format!("{}/", root.base(Family::Moves)))
                .await?,
        );
        listed.extend(
            keys.list_keys(&format!("{}/", root.base(Family::Deletes)))
                .await?,
        );
    }
    let (control, expired) = parse(&listed, now_ms);
    for key in expired {
        // Best effort: a key left behind is still expired, and the next
        // listing tries again.
        let _ = keys.delete(&key).await;
    }
    Ok(control)
}

/// The intents in `listed`, and the expired move and delete keys to delete.
pub fn parse(listed: &[(String, u64)], now_ms: u64) -> (Control, Vec<String>) {
    let mut control = Control::default();
    let mut expired = Vec::new();
    for (key, modified) in listed {
        if let Some(node) = key.strip_prefix(DRAIN) {
            if validate_node_id(node).is_ok() {
                control.draining.insert(node.to_owned());
            }
        } else if let Some(rest) = key.strip_prefix(STATE_PREFIX) {
            if let Some(intent) = parse_delete(rest, *modified) {
                if now_ms.saturating_sub(*modified) > DELETE_TTL_MS {
                    expired.push(key.clone());
                } else {
                    control.deletes.push(intent);
                }
                continue;
            }
            // An outcome nobody read: the command that waited for it has
            // gone. Deleted once as old as a request may be.
            if parse_outcome(rest) {
                if now_ms.saturating_sub(*modified) > DELETE_TTL_MS {
                    expired.push(key.clone());
                }
                continue;
            }
            let Some(intent) = parse_move(rest, *modified) else {
                continue;
            };
            if now_ms.saturating_sub(*modified) > MOVE_TTL_MS {
                expired.push(key.clone());
            } else {
                control.moves.push(intent);
            }
        }
    }
    (control, expired)
}

/// `<tenant>/<app>/moves/<class>/<name hex>/<target>`, after `state/`.
fn parse_move(rest: &str, written_at_ms: u64) -> Option<MoveIntent> {
    let parts: Vec<&str> = rest.split('/').collect();
    let [tenant, app, family, class, name, target] = parts.as_slice() else {
        return None;
    };
    if *family != Family::Moves.folder()
        || !segment(tenant)
        || !segment(app)
        || !segment(class)
        || validate_node_id(target).is_err()
    {
        return None;
    }
    let name = String::from_utf8(unhex(name)?).ok()?;
    (!name.is_empty()).then(|| MoveIntent {
        tenant: (*tenant).to_owned(),
        app: (*app).to_owned(),
        class: (*class).to_owned(),
        name,
        target: (*target).to_owned(),
        written_at_ms,
    })
}

/// `<tenant>/<app>/deletes/<class>/<name hex>/<id>`, after `state/`.
fn parse_delete(rest: &str, written_at_ms: u64) -> Option<DeleteIntent> {
    let parts: Vec<&str> = rest.split('/').collect();
    let [tenant, app, family, class, name, id] = parts.as_slice() else {
        return None;
    };
    if *family != Family::Deletes.folder()
        || !segment(tenant)
        || !segment(app)
        || !segment(class)
        || !request_id(id)
    {
        return None;
    }
    let name = String::from_utf8(unhex(name)?).ok()?;
    (!name.is_empty()).then(|| DeleteIntent {
        tenant: (*tenant).to_owned(),
        app: (*app).to_owned(),
        class: (*class).to_owned(),
        name,
        id: (*id).to_owned(),
        written_at_ms,
    })
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(out, "{b:02x}");
    }
    out
}

fn unhex(s: &str) -> Option<Vec<u8>> {
    if !s.len().is_multiple_of(2) || !s.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f')) {
        return None;
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).ok())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn drain_and_move_keys_read_back() {
        let drain = drain_key("node-b").unwrap();
        let mv = checked_move_key(
            &StateRoot::new("", "acme", "blog"),
            "room",
            "lobby/1 ünï",
            "node-c",
        )
        .unwrap();
        assert_eq!(drain, "control/drain/node-b");
        assert!(mv.starts_with("state/acme/blog/moves/room/"), "{mv}");
        let (control, expired) = parse(&[(drain, 10), (mv.clone(), 1_000)], 2_000);
        assert!(expired.is_empty());
        assert_eq!(control.draining, BTreeSet::from(["node-b".to_owned()]));
        assert_eq!(
            control.moves,
            [MoveIntent {
                tenant: "acme".into(),
                app: "blog".into(),
                class: "room".into(),
                name: "lobby/1 ünï".into(),
                target: "node-c".into(),
                written_at_ms: 1_000,
            }]
        );
        assert_eq!(control.moves[0].key(), mv);
    }

    #[test]
    fn moves_expire_by_the_buckets_clock() {
        let mv =
            checked_move_key(&StateRoot::new("", "acme", "blog"), "room", "x", "node-c").unwrap();
        let (control, expired) = parse(&[(mv.clone(), 1_000)], 1_000 + MOVE_TTL_MS);
        assert_eq!(control.moves.len(), 1);
        assert!(expired.is_empty());
        let (control, expired) = parse(&[(mv.clone(), 1_000)], 1_001 + MOVE_TTL_MS);
        assert!(control.moves.is_empty());
        assert_eq!(expired, [mv]);
    }

    #[test]
    fn delete_keys_read_back_and_expire_after_an_hour() {
        let root = StateRoot::new("", "acme", "blog");
        let key = checked_delete_key(&root, "cart", "u/1 ünï", "0a1b").unwrap();
        assert!(key.starts_with("state/acme/blog/deletes/cart/"), "{key}");
        assert!(key.ends_with("/0a1b"), "{key}");
        let (control, expired) = parse(&[(key.clone(), 1_000)], 1_000 + DELETE_TTL_MS);
        assert!(expired.is_empty() && control.moves.is_empty());
        assert_eq!(
            control.deletes,
            [DeleteIntent {
                tenant: "acme".into(),
                app: "blog".into(),
                class: "cart".into(),
                name: "u/1 ünï".into(),
                id: "0a1b".into(),
                written_at_ms: 1_000,
            }]
        );
        assert_eq!(control.deletes[0].key(), key);
        let (control, expired) = parse(&[(key.clone(), 1_000)], 1_001 + DELETE_TTL_MS);
        assert!(control.deletes.is_empty());
        assert_eq!(expired, std::slice::from_ref(&key));
        for junk in [
            "state/acme/blog/deletes/cart",
            "state/acme/blog/deletes/cart/78",
            "state/acme/blog/deletes/cart/zz/0a",
            "state/acme/blog/deletes/cart//0a",
            "state/acme/blog/deletes/cart/78/",
            "state/acme/blog/deletes/cart/78/XY",
            "state/acme/blog/deletes/cart/78/0a/extra",
            "state/../blog/deletes/cart/78/0a",
            "state/acme/blog/delete/cart/78/0a",
        ] {
            assert_eq!(
                parse(&[(junk.into(), 0)], 0),
                (Control::default(), Vec::new()),
                "{junk}"
            );
        }
        assert!(checked_delete_key(&root, "", "x", "0a").is_err());
        assert!(checked_delete_key(&root, "cart", "", "0a").is_err());
        assert!(checked_delete_key(&root, "cart", "x", "").is_err());
        assert!(checked_delete_key(&root, "cart", "x", "0A").is_err());
        assert!(checked_delete_key(&root, "cart", "x", &"a".repeat(33)).is_err());
        assert!(
            checked_delete_key(&StateRoot::new("", "ac/me", "blog"), "cart", "x", "0a").is_err()
        );
        // Outcomes are not requests, and expire with them.
        let done = DeleteOutcome::Done.key(&key);
        assert_eq!(DeleteOutcome::of(&done, &key), Some(DeleteOutcome::Done));
        assert_eq!(DeleteOutcome::of(&key, &key), None);
        let (control, expired) = parse(&[(done.clone(), 1_000)], 1_000 + DELETE_TTL_MS);
        assert_eq!((control, expired), (Control::default(), Vec::new()));
        let (_, expired) = parse(&[(done.clone(), 1_000)], 1_001 + DELETE_TTL_MS);
        assert_eq!(expired, [done]);
        let (a, b) = (new_request_id(), new_request_id());
        assert!(request_id(&a) && a.len() == 16 && a != b, "{a} {b}");
    }

    #[test]
    fn junk_keys_are_ignored() {
        let listed = [
            "control/drain/Not Valid",
            "control/drain/",
            "state/acme/blog/moves/room/78",
            "state/acme/blog/moves/room/zz/node-c",
            "state/acme/blog/moves/room/787/node-c",
            "state/acme/blog/moves/room//node-c",
            "state/acme/blog/moves/room/78/node-c/extra",
            "state/../blog/moves/room/78/node-c",
            "state/acme/../moves/room/78/node-c",
            "state/acme/blog/owners/room/78/node-c",
            // The layout before 0.4.0: no longer read.
            "control/move/acme/room/78/node-c",
            "control/other/thing",
        ]
        .map(|k| (k.to_owned(), 0));
        assert_eq!(parse(&listed, 0), (Control::default(), Vec::new()));
    }

    #[test]
    fn the_watch_lists_first_on_a_change_and_on_the_fallback() {
        let mut w = ControlWatch::new();
        // The first period lists, whatever the marker says.
        assert!(w.should_list(Ok(None)));
        w.listed(Some(None));
        // Unchanged: not listed until the fallback.
        for _ in 1..LISTING_FALLBACK_EVERY {
            assert!(!w.should_list(Ok(None)));
        }
        assert!(w.should_list(Ok(None)), "the fallback lists");
        w.listed(Some(None));
        // A change lists at once, and once.
        assert!(w.should_list(Ok(Some(b"1\n"))));
        w.listed(Some(Some(b"1\n".to_vec())));
        assert!(!w.should_list(Ok(Some(b"1\n"))));
        assert!(w.should_list(Ok(Some(b"2\n"))));
        // A listing that failed is not recorded: the next period lists.
        assert!(w.should_list(Ok(Some(b"2\n"))));
        w.listed(Some(Some(b"2\n".to_vec())));
        // A read that fails lists nothing before the fallback.
        for _ in 1..LISTING_FALLBACK_EVERY {
            assert!(!w.should_list(Err(())));
        }
        assert!(w.should_list(Err(())));
        // Listed after a failed read: the next good read lists again.
        w.listed(None);
        assert!(w.should_list(Ok(Some(b"2\n"))));
    }

    #[test]
    fn bad_intents_are_refused_before_writing() {
        assert!(drain_key("Node B").is_err());
        assert!(
            checked_move_key(&StateRoot::new("", "acme", "blog"), "room", "x", "Node C").is_err()
        );
        assert!(checked_move_key(&StateRoot::new("", "ac/me", "blog"), "room", "x", "c").is_err());
        assert!(checked_move_key(&StateRoot::new("", "acme", "b/g"), "room", "x", "c").is_err());
        assert!(checked_move_key(&StateRoot::new("", "acme", "blog"), "", "x", "c").is_err());
        assert!(checked_move_key(&StateRoot::new("", "acme", "blog"), "..", "x", "c").is_err());
        assert!(checked_move_key(&StateRoot::new("", "acme", "blog"), "room", "", "c").is_err());
    }
}
