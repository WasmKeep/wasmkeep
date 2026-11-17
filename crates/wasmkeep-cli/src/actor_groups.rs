//! Ordered, bounded history groups shared by backup and feed.
use anyhow::{Context, ensure};
use futures_util::StreamExt;
use object_store::{ObjectMeta, ObjectStore, path::Path as Key};
use std::{collections::BTreeMap, sync::Arc};
type Store = Arc<dyn ObjectStore>;
pub(crate) type ActorGroup = (String, BTreeMap<String, ObjectMeta>);
pub(crate) fn actor_groups(
    source: &Store,
    prefix: &str,
) -> futures_util::stream::BoxStream<'static, anyhow::Result<ActorGroup>> {
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
            let mut group_bytes = 0;
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
                group_bytes += tail.len() as u64 + meta.location.as_ref().len() as u64 + 1024;
                ensure!(
                    group_bytes <= 8 << 20,
                    "actor listing memory budget exceeded"
                );
                group.insert(tail, meta);
            }
            if group.is_empty() {
                Ok(None)
            } else {
                Ok(Some(((actor_prefix, group), (list, pending, prefix))))
            }
        },
    )
    .boxed()
}
