//! Cluster-wide intents written by an operator: drain a node, move an actor
//! ([ADR 0014](../../../docs/adr/0014-draining-a-node-and-moving-an-actor.md)).
//!
//! An intent is an object *key* under `control/`; bodies are empty and never
//! read. Nodes list the prefix once per heartbeat period, reading keys and
//! their last-modified time (the bucket's clock) only.
//!
//! ```text
//! control/drain/<node-id>
//! state/<tenant>/<app>/moves/<class>/<name hex>/<target node-id>
//! ```
//!
//! A drain names a node, shared by the bucket prefix; a move is an app's,
//! under its state root (ADR 0022). Moves written under the layout before
//! 0.4.0 (`control/move/<tenant>/...`) are not read.

use std::collections::BTreeSet;

use crate::lease::BucketError;
use crate::membership::validate_node_id;
use crate::state_root::{Family, STATE_PREFIX, StateRoot};

/// Key prefix of every intent.
pub const CONTROL_PREFIX: &str = "control/";
const DRAIN: &str = "control/drain/";

/// A move key older than this, by the bucket's clock, is ignored and
/// deleted by the next node that lists it.
pub const MOVE_TTL_MS: u64 = 5 * 60 * 1000;

/// Keys and their last-modified time, and deletes: what intents need.
#[allow(async_fn_in_trait)]
pub trait Keys {
    /// Every key under `prefix`, with its last-modified time (ms since the
    /// epoch, the bucket's clock). Bodies are not read.
    async fn list_keys(&self, prefix: &str) -> Result<Vec<(String, u64)>, BucketError>;
    /// Writes `key` with an empty body.
    async fn put_empty(&self, key: &str) -> Result<(), BucketError>;
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

/// What the intents say, as of one listing.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Control {
    /// Nodes asked to drain.
    pub draining: BTreeSet<String>,
    /// Moves not yet expired.
    pub moves: Vec<MoveIntent>,
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
    Ok(move_key(root, class, name, target))
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

/// Reads the intents: drain markers, and the moves of `moves_of`'s app
/// (none without it) written within [`MOVE_TTL_MS`] of `now_ms`. Moves
/// older than that are deleted, best effort. Keys that do not parse are
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
    }
    let (control, expired) = parse(&listed, now_ms);
    for key in expired {
        // Best effort: a key left behind is still expired, and the next
        // listing tries again.
        let _ = keys.delete(&key).await;
    }
    Ok(control)
}

/// The intents in `listed`, and the expired move keys to delete.
pub fn parse(listed: &[(String, u64)], now_ms: u64) -> (Control, Vec<String>) {
    let mut control = Control::default();
    let mut expired = Vec::new();
    for (key, modified) in listed {
        if let Some(node) = key.strip_prefix(DRAIN) {
            if validate_node_id(node).is_ok() {
                control.draining.insert(node.to_owned());
            }
        } else if let Some(rest) = key.strip_prefix(STATE_PREFIX) {
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
