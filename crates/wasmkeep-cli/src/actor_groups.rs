//! Ordered, bounded history groups shared by backup and feed.
use anyhow::{Context, ensure};
use futures_util::{StreamExt, TryStreamExt};
use object_store::{ObjectMeta, ObjectStore, path::Path as Key};
use std::{collections::BTreeMap, sync::Arc};
type Store = Arc<dyn ObjectStore>;
pub(crate) type ActorGroup = (String, BTreeMap<String, ObjectMeta>);
pub(crate) type FallibleActorGroup = (String, anyhow::Result<BTreeMap<String, ObjectMeta>>);
// Requested allocations, not process RSS: allocator bookkeeping, the provider's
// current page, and the caller's BTreeMap/restore data are separate.
const LISTING_MAX: usize = 32 << 20;

// Path hides its String capacity. Move that String out (without cloning) while
// collecting, keeping the remaining ObjectMeta fields intact for the caller.
struct ListedObject {
    key: String,
    meta: ObjectMeta,
}
impl ListedObject {
    fn new(mut meta: ObjectMeta) -> Self {
        let key = std::mem::take(&mut meta.location).into();
        Self { key, meta }
    }

    fn heap_bytes(&self) -> usize {
        self.key
            .capacity()
            .saturating_add(self.meta.e_tag.as_ref().map_or(0, String::capacity))
            .saturating_add(self.meta.version.as_ref().map_or(0, String::capacity))
    }
}

pub(crate) fn actor_groups(
    source: &Store,
    prefix: &str,
) -> futures_util::stream::BoxStream<'static, anyhow::Result<ActorGroup>> {
    actor_groups_with_errors(source, prefix)
        .and_then(|(prefix, group)| async move { Ok((prefix, group?)) })
        .boxed()
}

/// Keeps the actor boundary even when its metadata budget is exceeded.
pub(crate) fn actor_groups_with_errors(
    source: &Store,
    prefix: &str,
) -> futures_util::stream::BoxStream<'static, anyhow::Result<FallibleActorGroup>> {
    // The last group is yielded after observing EOF. The next group poll
    // must not poll S3's non-fused pagination stream a second time at EOF.
    let list = source
        .list(Some(&Key::from(prefix.trim_end_matches('/'))))
        .fuse();
    group_listing(list.boxed(), prefix.to_owned())
}

pub(crate) fn group_listing(
    list: futures_util::stream::BoxStream<'static, object_store::Result<ObjectMeta>>,
    prefix: String,
) -> futures_util::stream::BoxStream<'static, anyhow::Result<FallibleActorGroup>> {
    futures_util::stream::try_unfold(
        (list, None, prefix),
        |(mut list, mut pending, prefix)| async move {
            let mut group = Vec::<ListedObject>::new();
            let mut actor_prefix = String::new();
            let mut heap_bytes = 0usize;
            let mut count = 0u64;
            let mut exceeded = false;
            loop {
                let meta = match pending.take() {
                    Some(meta) => meta,
                    None => {
                        match tokio::time::timeout(std::time::Duration::from_secs(30), list.next())
                            .await
                            .context("app listing stalled")?
                        {
                            Some(meta) => meta?,
                            None => break,
                        }
                    }
                };
                let Some(relative) = meta.location.as_ref().strip_prefix(&prefix) else {
                    continue;
                };
                let parts: Vec<_> = relative.splitn(3, '/').collect();
                ensure!(parts.len() == 3, "invalid actor history key");
                let next = format!("{}{}/{}/", prefix, parts[0], parts[1]);
                if !actor_prefix.is_empty() && next != actor_prefix {
                    ensure!(
                        next > actor_prefix,
                        "source listing is not ordered by actor"
                    );
                    pending = Some(meta);
                    break;
                }
                actor_prefix = next;
                count = count.saturating_add(1);
                if exceeded {
                    continue;
                }
                let object = ListedObject::new(meta);
                heap_bytes = heap_bytes.saturating_add(object.heap_bytes());
                // Reserve in small fixed chunks, counting spare slots too. Check
                // before reserving so even a giant key/ETag cannot grow the group.
                let capacity = if group.len() == group.capacity() {
                    group.capacity().saturating_add(256)
                } else {
                    group.capacity()
                };
                let bytes =
                    heap_bytes.saturating_add(capacity.saturating_mul(size_of::<ListedObject>()));
                if bytes > LISTING_MAX {
                    exceeded = true;
                    group = Vec::new();
                    continue;
                }
                if capacity > group.capacity() {
                    group.reserve_exact(capacity - group.len());
                }
                if heap_bytes
                    .saturating_add(group.capacity().saturating_mul(size_of::<ListedObject>()))
                    > LISTING_MAX
                {
                    exceeded = true;
                    group = Vec::new();
                } else {
                    group.push(object);
                }
            }
            if actor_prefix.is_empty() {
                Ok(None)
            } else {
                let group = if exceeded {
                    Err(anyhow::anyhow!(
                        "{actor_prefix}: actor listing memory budget exceeded \
                         ({count} objects; 32 MiB limit); reduce the actor write rate or \
                         configured history retention, let collection run, then retry"
                    ))
                } else {
                    let mut objects = BTreeMap::new();
                    for object in group {
                        let mut meta = object.meta;
                        let tail = object.key[actor_prefix.len()..].to_owned();
                        meta.location = Key::parse(&object.key)?;
                        objects.insert(tail, meta);
                    }
                    Ok(objects)
                };
                Ok(Some(((actor_prefix, group), (list, pending, prefix))))
            }
        },
    )
    .boxed()
}

#[cfg(test)]
mod tests {
    use super::*;

    const PREFIX: &str = "state/acme/shop/actors/";

    fn meta(actor: &str, index: usize) -> ObjectMeta {
        ObjectMeta {
            location: Key::parse(format!(
                "{PREFIX}counter/{actor}/segments/{:016x}/{index:016x}.ltx",
                1
            ))
            .unwrap(),
            last_modified: chrono::DateTime::UNIX_EPOCH,
            size: 226,
            e_tag: Some("e".repeat(32)),
            version: None,
        }
    }

    #[test]
    fn per_object_memory_counts_fields_capacities_and_vec_slot() {
        let mut metadata = meta("busy", 1);
        let mut etag = String::with_capacity(128);
        etag.push_str("etag");
        let mut version = String::with_capacity(64);
        version.push_str("v1");
        metadata.e_tag = Some(etag);
        metadata.version = Some(version);
        let key_capacity = String::from(metadata.location.clone()).capacity();
        let expected_heap = key_capacity
            + metadata.e_tag.as_ref().unwrap().capacity()
            + metadata.version.as_ref().unwrap().capacity();
        let object = ListedObject::new(metadata);
        assert_eq!(object.heap_bytes(), expected_heap);
        assert!(object.meta.location.as_ref().is_empty());
        let objects = vec![object];
        let measured = objects.capacity() * size_of::<ListedObject>() + expected_heap;
        // The Vec slot includes the String/Option headers, timestamp, size and
        // struct padding. Payload bytes (meta.size) are not retained here.
        if cfg!(target_pointer_width = "64") {
            assert_eq!(size_of::<ObjectMeta>(), 96);
            assert_eq!(size_of::<ListedObject>(), 120);
            assert_eq!(measured, 120 + expected_heap);
        }
        let ordinary = ListedObject::new(meta("busy", 1));
        assert_eq!(ordinary.heap_bytes(), ordinary.key.capacity() + 32);
        if cfg!(target_pointer_width = "64") {
            // 82-byte full key + 32-byte ETag + 120-byte vector slot.
            assert_eq!(size_of::<ListedObject>() + ordinary.heap_bytes(), 234);
        }
        assert!(73_126 * (size_of::<ListedObject>() + ordinary.heap_bytes()) < LISTING_MAX);
    }

    #[tokio::test]
    async fn busy_actors_fit_and_metadata_survives() {
        // 20 writes/s for one hour, plus snapshots every 64 segments and
        // an initial snapshot; also cover the measured-bucket regression.
        for count in [15_000, 72_000 + 72_000 / 64 + 1] {
            let listing = futures_util::stream::iter((0..count).map(|i| Ok(meta("busy", i))));
            let mut groups = group_listing(listing.boxed(), PREFIX.to_owned());
            let (actor, group) = groups.try_next().await.unwrap().unwrap();
            assert_eq!(actor, format!("{PREFIX}counter/busy/"));
            let group = group.unwrap();
            assert_eq!(group.len(), count);
            let expected = meta("busy", 0);
            assert_eq!(group.values().next().unwrap(), &expected);
            assert!(groups.try_next().await.unwrap().is_none());
        }
    }

    #[tokio::test]
    async fn over_budget_reports_actor_total_and_recovery_then_continues() {
        // Large provider metadata reaches the real bound without thousands of
        // writes or a synthetic workload. Count the entire rejected actor.
        let listing = futures_util::stream::iter((0..34).map(|i| {
            let mut object = meta("busy", i);
            object.e_tag = Some("e".repeat(1 << 20));
            Ok(object)
        }))
        .chain(futures_util::stream::iter([Ok(meta("next", 0))]));
        let mut groups = group_listing(listing.boxed(), PREFIX.to_owned());
        let (actor, group) = groups.try_next().await.unwrap().unwrap();
        assert_eq!(actor, format!("{PREFIX}counter/busy/"));
        let message = group.unwrap_err().to_string();
        assert_eq!(
            message,
            format!(
                "{actor}: actor listing memory budget exceeded (34 objects; 32 MiB limit); \
             reduce the actor write rate or configured history retention, let collection run, then retry"
            )
        );
        let (actor, group) = groups.try_next().await.unwrap().unwrap();
        assert_eq!(actor, format!("{PREFIX}counter/next/"));
        assert_eq!(group.unwrap().len(), 1);
        assert!(groups.try_next().await.unwrap().is_none());
    }
}
