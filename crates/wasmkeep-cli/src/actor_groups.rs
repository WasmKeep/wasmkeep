//! Ordered, bounded history groups shared by backup and feed.
use anyhow::{Context, ensure};
use futures_util::{StreamExt, TryStreamExt};
use object_store::{ObjectMeta, ObjectStore, path::Path as Key};
use std::{collections::BTreeMap, sync::Arc};
type Store = Arc<dyn ObjectStore>;
pub(crate) type ActorGroup = (String, BTreeMap<String, ObjectMeta>);
type FallibleActorGroup = (String, anyhow::Result<BTreeMap<String, ObjectMeta>>);
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
    let prefix = prefix.to_owned();
    futures_util::stream::try_unfold(
        (list, None, prefix),
        |(mut list, mut pending, prefix)| async move {
            let mut group = BTreeMap::new();
            let mut actor_prefix = String::new();
            let mut group_bytes = 0u64;
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
                let tail = parts[2].to_owned();
                if !actor_prefix.is_empty() && next != actor_prefix {
                    ensure!(
                        next > actor_prefix,
                        "source listing is not ordered by actor"
                    );
                    pending = Some(meta);
                    break;
                }
                actor_prefix = next;
                group_bytes = group_bytes
                    .saturating_add(tail.len() as u64 + meta.location.as_ref().len() as u64 + 1024);
                if group_bytes > 8 << 20 {
                    exceeded = true;
                    group.clear();
                } else {
                    group.insert(tail, meta);
                }
            }
            if actor_prefix.is_empty() {
                Ok(None)
            } else {
                let group = if exceeded {
                    Err(anyhow::anyhow!(
                        "{actor_prefix}: actor listing memory budget exceeded"
                    ))
                } else {
                    Ok(group)
                };
                Ok(Some(((actor_prefix, group), (list, pending, prefix))))
            }
        },
    )
    .boxed()
}
