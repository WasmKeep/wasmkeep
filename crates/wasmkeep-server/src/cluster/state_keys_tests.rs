//! Today's spelling of every actor-state key and local path, written out.
//! The layout is part of the bucket format (ADRs 0007, 0009, 0014, 0020
//! and 0022): a change to any line here is a change to that format and
//! must be one on purpose.

use super::*;
use wasmkeep_cluster::state_root::StateRoot;
use wasmkeep_storage::{Storage, StorageConfig};

/// Names that cover the encodings: plain, escaped, and one long enough to
/// be hashed (`encode_segment` keeps 200 bytes).
const PLAIN: &str = "room-1";
const ESCAPED: &str = "Room 1 é";
fn long() -> String {
    "x".repeat(256)
}

/// A node on a memory store under `prefix`, serving tenant `acme`.
fn node_on(prefix: &str) -> Arc<Cluster> {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let mut config = ClusterConfig::new("n1", "http://n1", store);
    config.prefix = prefix.into();
    Cluster::new(&config, "acme/", "acme").unwrap()
}

fn actor(name: &str) -> ActorAddress {
    ActorAddress::new("acme", "chat", name).unwrap()
}

#[tokio::test]
async fn owner_and_lease_keys_are_relative_to_the_prefix() {
    for prefix in ["", "p", "p/"] {
        let c = node_on(prefix);
        assert_eq!(c.owner_key(&actor(PLAIN)), "owners/acme/chat/room-1");
        assert_eq!(
            c.owner_key(&actor(ESCAPED)),
            "owners/acme/chat/%52oom%201%20%C3%A9"
        );
        assert_eq!(
            c.owner_key(&actor(&long())),
            format!("owners/acme/chat/{}", long_encoded())
        );
        assert_eq!(c.lease_key(&actor(PLAIN)), "leases/acme/chat/room-1");
        assert_eq!(
            c.lease_key(&actor(ESCAPED)),
            "leases/acme/chat/%52oom%201%20%C3%A9"
        );
        assert_eq!(
            c.lease_key(&actor(&long())),
            format!("leases/acme/chat/{}", long_encoded())
        );
    }
}

/// `encode_segment` of [`long`]: cut to 167 bytes, then `~` and a hash.
fn long_encoded() -> String {
    format!("{}~85e62acd750c4eb56b7b6a1d66dca5bf", "x".repeat(167))
}

#[tokio::test]
async fn alarm_keys_carry_the_prefix() {
    for (prefix, p) in [("", ""), ("p", "p/"), ("p/", "p/")] {
        let c = node_on(prefix);
        assert_eq!(c.alarm_base().as_ref(), format!("{p}alarms/acme"));
        let tagged = Entry::owned(1_700_000_000_000, 7, "ab12").unwrap();
        assert_eq!(
            c.alarm_key(&actor(PLAIN), &tagged).as_ref(),
            format!("{p}alarms/acme/001700000000000/chat.t/726f6f6d2d31/00000000000000000007-ab12")
        );
        assert_eq!(
            c.alarm_entries_base(&actor(PLAIN), 1_700_000_000_000)
                .as_ref(),
            format!("{p}alarms/acme/001700000000000/chat.t/726f6f6d2d31")
        );
        assert_eq!(
            c.alarm_key(&actor(ESCAPED), &Entry::legacy(5)).as_ref(),
            format!("{p}alarms/acme/000000000000005/chat/526f6f6d203120c3a9")
        );
        assert_eq!(
            c.alarm_key(&actor(&long()), &Entry::legacy(5)).as_ref(),
            format!("{p}alarms/acme/000000000000005/chat/{}", "78".repeat(256))
        );
    }
}

#[test]
fn move_keys_are_relative_to_the_prefix() {
    for prefix in ["", "p", "p/"] {
        let root = StateRoot::new(prefix, "acme");
        assert_eq!(
            wasmkeep_cluster::control::checked_move_key(&root, "chat", ESCAPED, "node-b").unwrap(),
            "control/move/acme/chat/526f6f6d203120c3a9/node-b"
        );
        assert_eq!(
            wasmkeep_cluster::control::checked_move_key(&root, "chat", &long(), "node-b").unwrap(),
            format!("control/move/acme/chat/{}/node-b", "78".repeat(256))
        );
    }
}

/// A key is built from the node's tenant, never the address's: an address
/// of another tenant is refused in every build, not given this tenant's
/// keys.
#[tokio::test]
#[should_panic(expected = "not of this node's tenant")]
async fn a_key_for_another_tenant_is_refused() {
    let other = ActorAddress::new("other", "chat", PLAIN).unwrap();
    let _ = node_on("").owner_key(&other);
}

#[test]
fn database_files_are_per_tenant_and_class() {
    let at = |tenant: &str| {
        Storage::new(
            StorageConfig::new("/data"),
            crate::state_dir(&StateRoot::new("p/", tenant)),
        )
    };
    let storage = at("acme");
    assert_eq!(
        storage.db_path("chat", PLAIN),
        std::path::Path::new("/data/actors/acme/chat/room-1.db")
    );
    assert_eq!(
        at("Acme").db_path("chat", ESCAPED),
        std::path::Path::new("/data/actors/%41cme/chat/%52oom%201%20%C3%A9.db")
    );
    assert_eq!(
        storage.db_path("chat", &long()),
        std::path::Path::new(&format!("/data/actors/acme/chat/{}.db", long_encoded()))
    );
    assert_eq!(
        storage.scratch_dir(),
        std::path::Path::new("/data/.deleting")
    );
}

#[tokio::test]
async fn history_keys_carry_the_prefix() {
    use wasmkeep_cluster::replication::{SegmentId, snapshot_key};
    for (prefix, p) in [("", ""), ("p", "p/"), ("p/", "p/")] {
        let c = node_on(prefix);
        assert_eq!(
            c.history_key(&actor(PLAIN)),
            format!("{p}actors/acme/chat/room-1")
        );
        assert_eq!(
            c.history_key(&actor(ESCAPED)),
            format!("{p}actors/acme/chat/%52oom%201%20%C3%A9")
        );
        assert_eq!(
            c.history_key(&actor(&long())),
            format!("{p}actors/acme/chat/{}", long_encoded())
        );
        let snapshot = snapshot_key(SegmentId {
            txid: 9,
            generation: 2,
        });
        assert_eq!(
            format!("{}/{snapshot}", c.history_key(&actor(PLAIN))),
            format!("{p}actors/acme/chat/room-1/snapshot/0000000000000009-0000000000000002.ltx")
        );
    }
}

#[test]
fn a_listed_move_is_deleted_under_the_key_it_was_listed_at() {
    let listed = "control/move/other/chat/526f6f6d203120c3a9/node-b".to_owned();
    let (control, _) = wasmkeep_cluster::control::parse(&[(listed.clone(), 5)], 5);
    assert_eq!(control.moves[0].key(), listed);
}
