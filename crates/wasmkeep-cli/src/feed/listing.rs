//! Ordered parallel ranges selected from the previous poll's heads.
//!
//! S3 pushes list_with_offset down as StartAfter. Range ends are enforced
//! locally; the last page can include keys from the following range. Spooling
//! allows every range to make progress without retaining the app in memory.
use super::*;
use std::{
    io::{BufRead, BufReader, BufWriter, Write},
    path::Path,
};

type Groups = futures_util::stream::BoxStream<
    'static,
    anyhow::Result<crate::actor_groups::FallibleActorGroup>,
>;

pub(super) fn boundaries(
    db: &Connection,
    prefix: &str,
    concurrency: usize,
) -> anyhow::Result<Vec<String>> {
    ensure!(
        (1..=64).contains(&concurrency),
        "list concurrency must be 1..=64"
    );
    let count: i64 = db.query_row("SELECT count(*) FROM heads", [], |r| r.get(0))?;
    // Avoid multiplying single-page listings for small apps. At the measured
    // two objects per actor, each range should have at least one S3 page.
    let ranges = concurrency.min(count as usize / 500);
    let mut boundaries = Vec::with_capacity(ranges.saturating_sub(1));
    // Keep exactly the same byte ordering as the merge, including the slash.
    if ranges > 1 {
        let mut query = db.prepare(
            "SELECT class || '/' || name || '/' FROM heads ORDER BY class || '/' || name || '/'",
        )?;
        let mut rows = query.query([])?;
        let mut index = 0i64;
        while let Some(row) = rows.next()? {
            if index == count * (boundaries.len() + 1) as i64 / ranges as i64 {
                let key: String = row.get(0)?;
                boundaries.push(format!("{prefix}{key}"));
                if boundaries.len() == ranges - 1 {
                    break;
                }
            }
            index += 1;
        }
    }
    Ok(boundaries)
}

pub(super) async fn groups(
    store: &Arc<dyn ObjectStore>,
    prefix: &str,
    boundaries: Vec<String>,
    concurrency: usize,
    scratch: &Path,
) -> anyhow::Result<Groups> {
    ensure!(
        (1..=64).contains(&concurrency),
        "list concurrency must be 1..=64"
    );
    if boundaries.is_empty() || concurrency == 1 {
        return Ok(crate::actor_groups::actor_groups_with_errors(store, prefix));
    }
    ensure!(
        boundaries.windows(2).all(|w| w[0] < w[1])
            && boundaries
                .iter()
                .all(|s| s.starts_with(prefix) && s.ends_with('/')),
        "invalid listing range boundaries"
    );
    let key = Key::from(prefix.trim_end_matches('/'));
    let files = futures_util::stream::iter(0..=boundaries.len())
        .map(|i| {
            let start = i.checked_sub(1).map(|j| &boundaries[j]);
            let end = boundaries.get(i);
            let key = &key;
            async move {
                let mut list = match start {
                    Some(start) => store.list_with_offset(Some(key), &Key::parse(start)?),
                    None => store.list(Some(key)),
                };
                let file = tempfile::NamedTempFile::new_in(scratch)?;
                {
                    let mut writer = BufWriter::new(file.as_file());
                    while let Some(meta) =
                        tokio::time::timeout(std::time::Duration::from_secs(30), list.next())
                            .await
                            .context("app listing stalled")?
                    {
                        let meta = meta?;
                        // Path::parse strips the trailing slash from StartAfter.
                        // E.g. an offset of "a" also returns "a-/..." before
                        // "a/...". Enforce the original inclusive lower bound.
                        if start.is_some_and(|start| meta.location.as_ref() < start.as_str()) {
                            continue;
                        }
                        if end.is_some_and(|end| meta.location.as_ref() >= end.as_str()) {
                            break;
                        }
                        // Do not decode/re-encode the key: the listing may contain
                        // double-escaped actor names. Preserve all metadata fields.
                        serde_json::to_writer(
                            &mut writer,
                            &(
                                meta.location.as_ref(),
                                meta.last_modified.timestamp(),
                                meta.last_modified.timestamp_subsec_nanos(),
                                meta.size,
                                meta.e_tag,
                                meta.version,
                            ),
                        )?;
                        writer.write_all(b"\n")?;
                    }
                    writer.flush()?;
                }
                Ok::<_, anyhow::Error>(file)
            }
        })
        .buffered(concurrency)
        .try_collect::<Vec<_>>()
        .await?;
    // Temp files are owned by the stream: success, error and cancellation all
    // unlink them. A process crash is cleaned by the poll's scratch recovery.
    let objects = futures_util::stream::iter(files)
        .map(|file| {
            let reader = file.reopen().map(BufReader::new);
            futures_util::stream::try_unfold((file, reader), |(file, reader)| async move {
                let mut reader = reader?;
                let mut line = String::new();
                if reader.read_line(&mut line)? == 0 {
                    return Ok(None);
                }
                let (key, secs, nanos, size, e_tag, version): (
                    String,
                    i64,
                    u32,
                    u64,
                    Option<String>,
                    Option<String>,
                ) = serde_json::from_str(&line)?;
                let meta = ObjectMeta {
                    location: Key::parse(key)?,
                    last_modified: chrono::DateTime::from_timestamp(secs, nanos)
                        .context("invalid listing timestamp")?,
                    size,
                    e_tag,
                    version,
                };
                Ok::<_, anyhow::Error>(Some((meta, (file, Ok(reader)))))
            })
        })
        .flatten()
        .map_err(|source| object_store::Error::Generic {
            store: "feed listing spool",
            source: source.into(),
        })
        .fuse()
        .boxed();
    Ok(crate::actor_groups::group_listing(
        objects,
        prefix.to_owned(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::ObjectStoreExt;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Debug)]
    struct Observed {
        inner: Arc<dyn ObjectStore>,
        active: Arc<AtomicUsize>,
        peak: Arc<AtomicUsize>,
        fail: bool,
    }
    impl std::fmt::Display for Observed {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("observed memory store")
        }
    }
    struct Active(Arc<AtomicUsize>);
    impl Drop for Active {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Ordering::SeqCst);
        }
    }
    #[async_trait::async_trait]
    impl ObjectStore for Observed {
        async fn put_opts(
            &self,
            p: &Key,
            v: object_store::PutPayload,
            o: object_store::PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            self.inner.put_opts(p, v, o).await
        }
        async fn put_multipart_opts(
            &self,
            _: &Key,
            _: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            unreachable!()
        }
        async fn get_opts(
            &self,
            p: &Key,
            o: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            self.inner.get_opts(p, o).await
        }
        fn delete_stream(
            &self,
            _: futures_util::stream::BoxStream<'static, object_store::Result<Key>>,
        ) -> futures_util::stream::BoxStream<'static, object_store::Result<Key>> {
            unreachable!()
        }
        async fn copy_opts(
            &self,
            _: &Key,
            _: &Key,
            _: object_store::CopyOptions,
        ) -> object_store::Result<()> {
            unreachable!()
        }
        async fn list_with_delimiter(
            &self,
            _: Option<&Key>,
        ) -> object_store::Result<object_store::ListResult> {
            unreachable!()
        }
        fn list(
            &self,
            p: Option<&Key>,
        ) -> futures_util::stream::BoxStream<'static, object_store::Result<ObjectMeta>> {
            let stream = self.inner.list(p);
            let active = self.active.clone();
            let peak = self.peak.clone();
            let fail = self.fail;
            futures_util::stream::once(async move {
                let n = active.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(n, Ordering::SeqCst);
                let guard = Active(active);
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                stream.map(move |item| {
                    if fail {
                        return Err(object_store::Error::Generic {
                            store: "test",
                            source: "injected LIST failure".into(),
                        });
                    }
                    let _ = &guard;
                    item.map(|mut meta| {
                        meta.version = Some("version\n\"%".into());
                        meta.last_modified =
                            chrono::DateTime::from_timestamp(-1, 123_456_789).unwrap();
                        meta
                    })
                })
            })
            .flatten()
            .boxed()
        }
    }

    async fn fixture() -> (Arc<dyn ObjectStore>, Vec<String>) {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let mut actors = Vec::new();
        for class in ["c", "c-", "c%253Aescaped", "z"] {
            for name in (0..120)
                .map(|n| format!("actor-{n:06}"))
                .chain(["bg%253Alobby", "a", "a-", "a%252Fb", "é"].map(str::to_owned))
            {
                let actor = format!("state/t/a/actors/{class}/{name}/");
                for tail in ["snapshots/0001.ltx", "segments/0002.ltx"] {
                    store
                        .put(
                            &Key::parse(format!("{actor}{tail}")).unwrap(),
                            bytes::Bytes::from_static(b"history").into(),
                        )
                        .await
                        .unwrap();
                }
                actors.push(actor);
            }
        }
        actors.sort();
        (store, actors)
    }

    #[tokio::test]
    async fn parallel_listing_matches_sequential_exactly() {
        let (inner, actors) = fixture().await;
        let store: Arc<dyn ObjectStore> = Arc::new(Observed {
            inner,
            active: Arc::new(AtomicUsize::new(0)),
            peak: Arc::new(AtomicUsize::new(0)),
            fail: false,
        });
        let expected: Vec<_> = crate::actor_groups::actor_groups(&store, "state/t/a/actors/")
            .try_collect()
            .await
            .unwrap();
        let scratch = tempfile::tempdir().unwrap();
        // Boundaries include absent actors and escaped names; ranges may be empty.
        let mut boundaries: Vec<_> = actors.iter().step_by(19).cloned().collect();
        boundaries.extend([
            "state/t/a/actors/c/absent/".into(),
            "state/t/a/actors/c/a/".into(),
            "state/t/a/actors/c/a-/".into(),
            "state/t/a/actors/c/bg%253Alobby/".into(),
        ]);
        boundaries.sort();
        boundaries.dedup();
        for concurrency in [1, 3, 16] {
            let actual: Vec<_> = groups(
                &store,
                "state/t/a/actors/",
                boundaries.clone(),
                concurrency,
                scratch.path(),
            )
            .await
            .unwrap()
            .and_then(|(p, g)| async move { Ok((p, g?)) })
            .try_collect()
            .await
            .unwrap();
            assert_eq!(actual, expected);
        }
    }

    #[tokio::test]
    async fn parallel_listing_concurrency_is_bounded_and_used() {
        let (inner, actors) = fixture().await;
        for limit in [1, 3, 16] {
            let active = Arc::new(AtomicUsize::new(0));
            let peak = Arc::new(AtomicUsize::new(0));
            let store: Arc<dyn ObjectStore> = Arc::new(Observed {
                inner: inner.clone(),
                active: active.clone(),
                peak: peak.clone(),
                fail: false,
            });
            let scratch = tempfile::tempdir().unwrap();
            let _: Vec<_> = groups(
                &store,
                "state/t/a/actors/",
                actors.iter().step_by(10).cloned().collect(),
                limit,
                scratch.path(),
            )
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
            assert_eq!(active.load(Ordering::SeqCst), 0);
            assert_eq!(peak.load(Ordering::SeqCst), limit);
        }
    }
    #[test]
    fn saved_heads_partition_in_bucket_order() {
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch("CREATE TABLE heads(class TEXT, name TEXT)")
            .unwrap();
        assert!(boundaries(&db, "p/", 16).unwrap().is_empty());
        db.execute("INSERT INTO heads VALUES ('c', 'a')", [])
            .unwrap();
        assert!(boundaries(&db, "p/", 16).unwrap().is_empty());
        db.execute("DELETE FROM heads", []).unwrap();
        for class in ["c", "c-", "z"] {
            for name in (0..497)
                .map(|n| format!("actor-{n:06}"))
                .chain(["a", "a-", "bg%253Alobby"].map(str::to_owned))
            {
                db.execute("INSERT INTO heads VALUES (?,?)", [class, &name])
                    .unwrap();
            }
        }
        assert!(boundaries(&db, "p/", 1).unwrap().is_empty());
        assert_eq!(boundaries(&db, "p/", 3).unwrap(), ["p/c/a-/", "p/z/a-/"]);
        assert_eq!(boundaries(&db, "p/", 16).unwrap().len(), 2);
        assert!(boundaries(&db, "p/", 0).is_err());
    }

    #[tokio::test]
    async fn parallel_listing_cleans_spools_on_drop_and_error() {
        let (inner, actors) = fixture().await;
        let scratch = tempfile::tempdir().unwrap();
        let stream = groups(
            &inner,
            "state/t/a/actors/",
            vec![actors[100].clone()],
            2,
            scratch.path(),
        )
        .await
        .unwrap();
        assert_eq!(std::fs::read_dir(scratch.path()).unwrap().count(), 2);
        drop(stream);
        assert_eq!(std::fs::read_dir(scratch.path()).unwrap().count(), 0);
        let active = Arc::new(AtomicUsize::new(0));
        let store: Arc<dyn ObjectStore> = Arc::new(Observed {
            inner,
            active: active.clone(),
            peak: Arc::new(AtomicUsize::new(0)),
            fail: true,
        });
        let result = groups(
            &store,
            "state/t/a/actors/",
            vec![actors[100].clone()],
            2,
            scratch.path(),
        )
        .await;
        assert!(result.is_err());
        assert_eq!(active.load(Ordering::SeqCst), 0);
        assert_eq!(std::fs::read_dir(scratch.path()).unwrap().count(), 0);
    }
}
