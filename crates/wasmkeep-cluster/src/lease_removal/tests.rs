use super::*;
use crate::app_format::floor::{self, Minor};
use crate::node_lease::{OwnerRecord, encode_forgotten};
use crate::object_store::ObjectStoreBucket;

const DAY: u64 = 24 * 60 * 60 * 1000;

fn bucket() -> ObjectStoreBucket {
    ObjectStoreBucket::new(
        std::sync::Arc::new(object_store::memory::InMemory::new()),
        "wk",
    )
}

/// Later than the in-memory store's clock by a minute: what was just
/// written lists as old enough to be read.
fn now() -> u64 {
    let wall = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis();
    u64::try_from(wall).unwrap() + 60_000
}

fn settings() -> Settings {
    Settings {
        after_ms: DAY,
        horizon_ms: 10_000,
        config: LeaseConfig::default_safe(),
    }
}

fn body(app: &str, id: &str, version: &str, state: &str, at: u64) -> Vec<u8> {
    format!(
        "wasmkeep-node 3\n{state} i-{id}\n{at}\n{id}\nhttp://{id}\n{version}\n{app}\n\n\nserving\n\n\n"
    )
    .into_bytes()
}

async fn put(b: &ObjectStoreBucket, key: &str, body: Vec<u8>) {
    b.put_body(key, body).await.unwrap();
}

async fn finalized(b: &ObjectStoreBucket) {
    floor::raise(b, "formats/acme/shop/floor", Minor { major: 0, minor: 9 })
        .await
        .unwrap();
}

async fn round(r: &mut Remover, b: &ObjectStoreBucket, now: u64) -> Round {
    r.round(b, now, "acme/shop", &settings(), std::future::pending)
        .await
        .unwrap()
}

async fn present(b: &ObjectStoreBucket, key: &str) -> bool {
    b.get(key).await.unwrap().is_some()
}

/// A released lease of the app older than the period is removed once the
/// app is finalized at 0.9; a younger one, another app's, a fenced one and
/// a live one stay.
#[tokio::test]
async fn an_old_released_lease_is_removed_after_the_finalize() {
    let b = bucket();
    let now = now();
    put(&b, "nodes/a", body("acme/shop", "a", "0.10.0", "live", now)).await;
    put(
        &b,
        "nodes/old",
        body("acme/shop", "old", "0.9.0", "released", now - 2 * DAY),
    )
    .await;
    put(
        &b,
        "nodes/young",
        body("acme/shop", "young", "0.9.0", "released", now - DAY / 2),
    )
    .await;
    put(
        &b,
        "nodes/other",
        body("acme/blog", "other", "0.9.0", "released", now - 2 * DAY),
    )
    .await;
    put(
        &b,
        "nodes/fenced",
        body("acme/shop", "fenced", "0.9.0", "fenced", now - 2 * DAY),
    )
    .await;
    let mut r = Remover::new();
    // Before the finalize: nothing written, and what blocks is said.
    let blocked = round(&mut r, &b, now).await;
    assert!(blocked.is_idle(), "{blocked:?}");
    assert!(
        blocked.blocked.as_deref().unwrap().contains("finalize"),
        "{blocked:?}"
    );
    assert!(present(&b, "nodes/old").await);
    finalized(&b).await;
    let done = round(&mut r, &b, now).await;
    assert_eq!(done.removed, ["old"], "{done:?}");
    assert_eq!(done.blocked, None);
    for kept in ["nodes/a", "nodes/young", "nodes/other", "nodes/fenced"] {
        assert!(present(&b, kept).await, "{kept}");
    }
    assert!(!present(&b, "nodes/old").await);
    // Nothing more to do; the young one goes once it is old enough.
    assert!(round(&mut r, &b, now).await.is_idle());
    let later = round(&mut r, &b, now + DAY).await;
    assert_eq!(later.removed, ["young"], "{later:?}");
}

/// A live node of the app older than 0.9 blocks the mark, as for the
/// command: nothing is removed while it is live.
#[tokio::test]
async fn a_live_older_node_blocks_the_removal() {
    let b = bucket();
    let now = now();
    finalized(&b).await;
    put(&b, "nodes/z", body("acme/shop", "z", "0.8.1", "live", now)).await;
    put(
        &b,
        "nodes/old",
        body("acme/shop", "old", "0.9.0", "released", now - 2 * DAY),
    )
    .await;
    let done = round(&mut Remover::new(), &b, now).await;
    assert!(
        done.blocked.as_deref().unwrap().contains("z (0.8.1)"),
        "{done:?}"
    );
    assert!(present(&b, "nodes/old").await);
}

/// A record that names the node, any instance, keeps its lease: the lease
/// is what frees it. One that cannot be read keeps every lease; a
/// migration tombstone names nobody.
#[tokio::test]
async fn a_record_naming_the_node_keeps_its_lease() {
    let b = bucket();
    let now = now();
    finalized(&b).await;
    for id in ["held", "free"] {
        put(
            &b,
            &format!("nodes/{id}"),
            body("acme/shop", id, "0.9.0", "released", now - 2 * DAY),
        )
        .await;
    }
    let record = OwnerRecord {
        holder: Some(("held".into(), "i-old".into())),
        generation: 3,
        recorded: None,
    };
    put(&b, "state/acme/blog/owners/c/x", record.encode()).await;
    put(
        &b,
        "state/acme/shop/owners/c/moved",
        format!("{}\nacme/new\n", crate::legacy_layout::TOMBSTONE_HEADER).into_bytes(),
    )
    .await;
    let mut r = Remover::new();
    let done = round(&mut r, &b, now).await;
    assert_eq!(done.removed, ["free"], "{done:?}");
    assert_eq!(done.named, ["held"]);
    assert!(present(&b, "nodes/held").await);
    // An unreadable record: kept, whatever it may name.
    put(
        &b,
        "nodes/free",
        body("acme/shop", "free", "0.9.0", "released", now - 2 * DAY),
    )
    .await;
    put(&b, "state/acme/shop/owners/c/junk", b"junk".to_vec()).await;
    let done = round(&mut r, &b, now).await;
    assert!(done.removed.is_empty(), "{done:?}");
    assert_eq!(done.named, ["free", "held"]);
}

/// A mark older than the bound is a removal that did not finish: ended. A
/// younger one may still have its delete in flight: left.
#[tokio::test]
async fn a_mark_that_stayed_is_ended_and_a_young_one_is_left() {
    let cfg = LeaseConfig::default_safe();
    assert!(stale_mark_ms(&cfg) > forget_delete_bound_ms(&cfg));
    let b = bucket();
    let now = now();
    put(
        &b,
        "nodes/stale",
        encode_forgotten(now - stale_mark_ms(&cfg) - 1),
    )
    .await;
    put(&b, "nodes/young", encode_forgotten(now - 1_000)).await;
    let mut r = Remover::new();
    let done = round(&mut r, &b, now).await;
    assert_eq!(done.ended, ["stale"], "{done:?}");
    assert!(!present(&b, "nodes/stale").await);
    assert!(present(&b, "nodes/young").await);
    // Its time comes.
    let later = round(&mut r, &b, now + stale_mark_ms(&cfg)).await;
    assert_eq!(later.ended, ["young"], "{later:?}");
    // Broken on purpose: kept for ever.
    put(&b, "nodes/kept", encode_forgotten(now - DAY)).await;
    let kept = round(&mut Remover::with_fault(Fault::MarksKept), &b, now).await;
    assert!(kept.ended.is_empty() && present(&b, "nodes/kept").await);
}

/// At most [`MAX_PER_ROUND`] per round, oldest first; the rest next round.
#[tokio::test]
async fn a_round_is_bounded() {
    let b = bucket();
    let now = now();
    finalized(&b).await;
    let n = MAX_PER_ROUND + 3;
    for i in 0..n {
        let id = format!("n{i:02}");
        let at = now - 2 * DAY - u64::try_from(i).unwrap();
        put(
            &b,
            &format!("nodes/{id}"),
            body("acme/shop", &id, "0.9.0", "released", at),
        )
        .await;
    }
    let mut r = Remover::new();
    let first = round(&mut r, &b, now).await;
    assert_eq!(first.removed.len(), MAX_PER_ROUND);
    assert_eq!(first.left, 3);
    // Oldest first: the highest numbers are the oldest.
    assert_eq!(first.removed[0], format!("n{:02}", n - 1));
    let second = round(&mut r, &b, now).await;
    assert_eq!(second.removed, ["n02", "n01", "n00"], "{second:?}");
}

/// A key read once is not read again until it is written again: a round
/// over many released leases reads [`READS_PER_ROUND`] at most, then only
/// what changed.
#[tokio::test]
async fn keys_are_read_once_and_a_bounded_number_per_round() {
    let b = bucket();
    let now = now();
    let n = READS_PER_ROUND + 5;
    for i in 0..n {
        let id = format!("n{i:03}");
        put(
            &b,
            &format!("nodes/{id}"),
            body("acme/shop", &id, "0.9.0", "released", now),
        )
        .await;
    }
    let mut r = Remover::new();
    let _ = round(&mut r, &b, now).await;
    assert_eq!(r.seen.len(), READS_PER_ROUND);
    let _ = round(&mut r, &b, now).await;
    assert_eq!(r.seen.len(), n);
    // A key gone from the listing is forgotten.
    crate::control::Keys::delete(&b, "nodes/n000")
        .await
        .unwrap();
    let _ = round(&mut r, &b, now).await;
    assert_eq!(r.seen.len(), n - 1);
}

/// Never younger than the command's own window, whatever the period.
#[tokio::test]
async fn never_younger_than_the_window_of_cluster_forget() {
    let b = bucket();
    let now = now();
    finalized(&b).await;
    let cfg = LeaseConfig::default_safe();
    let window = least_age_ms(&cfg, 10_000);
    assert_eq!(window, 25_000);
    put(
        &b,
        "nodes/r",
        body("acme/shop", "r", "0.9.0", "released", now - window),
    )
    .await;
    let s = Settings {
        after_ms: 0,
        ..settings()
    };
    let mut r = Remover::new();
    let kept = r
        .round(&b, now, "acme/shop", &s, std::future::pending)
        .await
        .unwrap();
    assert!(kept.removed.is_empty(), "{kept:?}");
    let gone = r
        .round(&b, now + 1, "acme/shop", &s, std::future::pending)
        .await
        .unwrap();
    assert_eq!(gone.removed, ["r"]);
}
