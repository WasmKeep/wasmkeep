//! Marker-protected asset publication (ADR 0030). Collection is A3.
use super::*;
use crate::assets::{Bundle, Index, digest};
use futures_util::{StreamExt, TryStreamExt};
use tokio::time::Instant;

#[derive(Clone)]
pub(super) struct Marker {
    last: tokio::sync::watch::Receiver<Instant>,
    start: Instant,
    bucket_ms: i64,
    /// The version and `seq` the deploy is about to write to `current`, set
    /// right before it does: what a failed renewal compares `current` with.
    switching: Arc<std::sync::Mutex<Option<(String, u64)>>>,
}
impl Marker {
    /// The deploy is about to point `current` at `version` with `seq`.
    pub(super) fn switching(&self, version: &str, seq: u64) {
        *self
            .switching
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some((version.to_owned(), seq));
    }
    pub(super) fn check(&self) -> Result<(), DeployError> {
        if self.last.borrow().elapsed() > Duration::from_secs(30 * 60) {
            return Err(DeployError::Refused(
                "assets deploy marker deadline exceeded; retry".into(),
            ));
        }
        Ok(())
    }
}
impl AppRef {
    pub(super) async fn with_assets_marker<
        T,
        F: std::future::Future<Output = Result<T, DeployError>>,
    >(
        &self,
        run: impl FnOnce(Marker) -> F,
    ) -> Result<T, DeployError> {
        let key = self.path(&format!(
            "_assets/deploying/{}",
            wasmkeep_cluster::control::new_request_id()
        ));
        let start = Instant::now();
        // On an id collision, never remove another deploy's marker.
        self.store
            .put_opts(
                &key,
                Vec::new().into(),
                PutOptions {
                    mode: PutMode::Create,
                    ..Default::default()
                },
            )
            .await
            .map_err(bucket)?;
        let switching = Arc::new(std::sync::Mutex::new(None));
        // `Err`: the renewal failed, and the operation was stopped.
        let result = async {
            let meta = match self.store.head(&key).await {
                Ok(meta) => meta,
                Err(e) => return Ok(Err(bucket(e))),
            };
            let (last, receiver) = tokio::sync::watch::channel(start);
            let marker = Marker { last: receiver, start: Instant::now(), bucket_ms: meta.last_modified.timestamp_millis(), switching: Arc::clone(&switching) };
            let operation = run(marker);
            tokio::pin!(operation);
            let mut timer = tokio::time::interval_at(Instant::now() + Duration::from_secs(600), Duration::from_secs(600));
            timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    result = &mut operation => break Ok(result),
                    _ = timer.tick() => {
                        let sent = Instant::now();
                        // A changing body makes the acknowledged renewal observable in tests.
                        if let Err(e) = self.store.put(&key, wasmkeep_cluster::control::new_request_id().into_bytes().into()).await {
                            break Err(e);
                        }
                        last.send_replace(sent);
                    }
                }
            }
        }.await;
        // The operation is dropped by now: what `current` names is what it
        // did, unless a switch it sent is still in flight.
        let result = match result {
            Ok(result) => result,
            Err(error) => Err(self.renewal_failed(&switching, &error).await),
        };
        // No detached task: cancelling/killing this future leaves the marker.
        // Ordinary errors, including a failed renewal, always pass this cleanup.
        let cleanup = self.store.delete(&key).await.map_err(bucket);
        match result {
            Err(e) => Err(e),
            Ok(value) => {
                if let Err(error) = cleanup {
                    eprintln!(
                        "warning: deploy is done, but its assets marker could not be removed: {error}; collection may wait until the marker expires"
                    );
                }
                Ok(value)
            }
        }
    }

    /// The renewal of an assets marker failed and the deploy was stopped:
    /// the error, and whether `current` was switched.
    async fn renewal_failed(
        &self,
        switching: &std::sync::Mutex<Option<(String, u64)>>,
        error: &object_store::Error,
    ) -> DeployError {
        let target = switching
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let Some((version, seq)) = target else {
            return DeployError::Bucket(format!(
                "renewing the assets deploy marker failed: {error}; the deploy stopped before \
                 switching `current`: nothing was switched"
            ));
        };
        let outcome = match self.read_current().await {
            Err(e) => format!(
                "the deploy was switching `current` to {version}, and reading it back failed \
                 ({e}): check what `current` names before deploying again"
            ),
            Ok(Some((now, _))) if now.version == version && now.seq == seq => {
                format!(
                    "`current` already names the new version {version} (seq {seq}): the deploy took effect"
                )
            }
            Ok(now) => format!(
                "the deploy was switching `current` to {version}, which it does not name yet \
                 ({}): the switch may still land; check what `current` names before deploying again",
                now.map_or_else(
                    || "nothing is deployed".to_owned(),
                    |(now, _)| format!("it names {} (seq {})", now.version, now.seq)
                )
            ),
        };
        DeployError::Bucket(format!(
            "renewing the assets deploy marker failed: {error}; {outcome}"
        ))
    }

    pub(super) async fn read_assets_index(&self, hash: &str) -> Result<Index, DeployError> {
        self.read_assets_index_if_present(hash)
            .await?
            .ok_or_else(|| {
                DeployError::Refused(format!(
                    "_assets/index/{hash} missing; deploy again with --assets"
                ))
            })
    }

    pub(super) async fn read_assets_index_if_present(
        &self,
        hash: &str,
    ) -> Result<Option<Index>, DeployError> {
        let key = format!("_assets/index/{hash}");
        let object = match self.store.get(&self.path(&key)).await {
            Ok(object) => object,
            Err(object_store::Error::NotFound { .. }) => {
                return Ok(None);
            }
            Err(e) => return Err(bucket(e)),
        };
        if object.meta.size > crate::assets::MAX_INDEX as u64 {
            return Err(DeployError::Corrupt(format!("{key} exceeds 8 MiB")));
        }
        let bytes = object.bytes().await.map_err(bucket)?;
        if digest(&bytes) != hash {
            return Err(DeployError::Refused(format!(
                "{key} digest mismatch; deploy again with --assets"
            )));
        }
        Index::parse(&bytes)
            .map(Some)
            .map_err(|e| DeployError::Corrupt(format!("{key}: {e}")))
    }

    pub(super) async fn prepare_assets(
        &self,
        hash: &str,
        supplied: Option<&Bundle>,
        verify: bool,
        marker: &Marker,
    ) -> Result<usize, DeployError> {
        // The timestamp is the bucket's clock plus monotonic elapsed time,
        // never the deploy workstation's wall clock.
        loop {
            let now = i128::from(marker.bucket_ms) + marker.start.elapsed().as_millis() as i128;
            let collecting = self.path("_assets/collecting/");
            let mut objects = self.store.list(Some(&collecting));
            let mut fresh = false;
            while let Some(meta) = objects.try_next().await.map_err(bucket)? {
                let tail = meta.location.filename().unwrap_or("");
                if tail.len() == 16
                    && tail
                        .bytes()
                        .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
                    && now - i128::from(meta.last_modified.timestamp_millis()) < 15 * 60 * 1000
                {
                    fresh = true;
                }
            }
            if !fresh {
                break;
            }
            eprintln!("waiting for a collection of assets to finish");
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
        let index = match supplied {
            Some(b) => b.index.clone(),
            None => self.read_assets_index(hash).await?,
        };
        let index_bytes = index.bytes();
        let mut expected = BTreeMap::new();
        for file in index.files.values() {
            for c in file.contents() {
                expected.insert(
                    format!("_assets/content/{}", c.digest),
                    (c.digest.clone(), c.size),
                );
            }
        }
        let prefix = self.path("_assets/content/");
        let existing: std::collections::BTreeSet<_> = self
            .store
            .list(Some(&prefix))
            .map_ok(|m| m.location)
            .try_collect()
            .await
            .map_err(bucket)?;
        let mut repaired = 0;
        // At most eight requests in flight; index publication follows contents.
        if let Some(bundle) = supplied {
            let uploads: Vec<(String, String)> = expected
                .iter()
                .filter(|(key, _)| !existing.contains(&self.path(key)))
                .map(|(key, (hash, _))| (key.clone(), hash.clone()))
                .collect();
            let results: Vec<_> = futures_util::stream::iter(uploads)
                .map(|(key, hash)| async move {
                    let bytes = bundle.contents.get(&hash).ok_or_else(|| {
                        DeployError::Refused(format!("missing local asset bytes {hash}"))
                    })?;
                    self.asset_put(&key, bytes).await
                })
                .buffer_unordered(8)
                .try_collect()
                .await?;
            repaired += results.into_iter().filter(|v| *v).count();
            let key = format!("_assets/index/{hash}");
            match self.store.head(&self.path(&key)).await {
                Ok(_) => {}
                Err(object_store::Error::NotFound { .. }) => {
                    repaired += usize::from(self.asset_put(&key, &index_bytes).await?)
                }
                Err(e) => return Err(bucket(e)),
            }
        }
        expected.insert(
            format!("_assets/index/{hash}"),
            (hash.into(), index_bytes.len() as u64),
        );
        for (key, (hash, size)) in expected {
            let path = self.path(&key);
            let meta = match self.store.head(&path).await {
                Ok(meta) => meta,
                Err(object_store::Error::NotFound { .. }) => {
                    return Err(DeployError::Refused(format!(
                        "{key} missing after upload; deploy again with --assets"
                    )));
                }
                Err(e) => return Err(bucket(e)),
            };
            let wrong = meta.size != size
                || (verify && {
                    let bytes = self
                        .store
                        .get(&path)
                        .await
                        .map_err(bucket)?
                        .bytes()
                        .await
                        .map_err(bucket)?;
                    digest(&bytes) != hash
                });
            if wrong {
                let Some(bundle) = supplied else {
                    return Err(DeployError::Refused(format!(
                        "{key} has wrong bytes; deploy again with --assets"
                    )));
                };
                let bytes = if key.starts_with("_assets/index/") {
                    &index_bytes
                } else {
                    bundle.contents.get(&hash).ok_or_else(|| {
                        DeployError::Refused(format!("missing local asset bytes {hash}"))
                    })?
                };
                self.store.delete(&path).await.map_err(bucket)?;
                repaired += usize::from(self.asset_put(&key, bytes).await?);
                let meta = self.store.head(&path).await.map_err(bucket)?;
                if meta.size != size {
                    return Err(DeployError::Corrupt(format!("{key} (size after repair)")));
                }
                if verify {
                    let bytes = self
                        .store
                        .get(&path)
                        .await
                        .map_err(bucket)?
                        .bytes()
                        .await
                        .map_err(bucket)?;
                    if digest(&bytes) != hash {
                        return Err(DeployError::Corrupt(format!("{key} (digest after repair)")));
                    }
                }
            }
        }
        Ok(repaired)
    }
    async fn asset_put(&self, key: &str, bytes: &[u8]) -> Result<bool, DeployError> {
        match self
            .store
            .put_opts(
                &self.path(key),
                bytes.to_vec().into(),
                PutOptions {
                    mode: PutMode::Create,
                    ..Default::default()
                },
            )
            .await
        {
            Ok(_) => Ok(true),
            Err(
                object_store::Error::AlreadyExists { .. }
                | object_store::Error::Precondition { .. },
            ) => Ok(false),
            Err(e) => Err(bucket(e)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test(start_paused = true)]
    async fn publication_deadline_uses_the_last_acknowledged_write_start() {
        let (last, receiver) = tokio::sync::watch::channel(Instant::now());
        let marker = Marker {
            last: receiver,
            start: Instant::now(),
            bucket_ms: 0,
            switching: Arc::default(),
        };
        tokio::time::advance(Duration::from_secs(1801)).await;
        assert!(marker.check().is_err());
        last.send_replace(Instant::now());
        assert!(marker.check().is_ok());
    }

    /// Overwrites of the deploy marker (its renewals) fail; everything
    /// else, its creation included, reaches the bucket.
    #[derive(Debug)]
    struct NoRenewal(object_store::memory::InMemory);
    impl std::fmt::Display for NoRenewal {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("no-renewal")
        }
    }
    #[async_trait::async_trait]
    impl ObjectStore for NoRenewal {
        async fn put_opts(
            &self,
            p: &object_store::path::Path,
            b: PutPayload,
            o: PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            if p.as_ref().contains("/_assets/deploying/") && matches!(o.mode, PutMode::Overwrite) {
                return Err(object_store::Error::Generic {
                    store: "test",
                    source: "renewal refused".into(),
                });
            }
            self.0.put_opts(p, b, o).await
        }
        async fn put_multipart_opts(
            &self,
            p: &object_store::path::Path,
            o: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.0.put_multipart_opts(p, o).await
        }
        async fn get_opts(
            &self,
            p: &object_store::path::Path,
            o: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            self.0.get_opts(p, o).await
        }
        fn delete_stream(
            &self,
            p: futures_util::stream::BoxStream<
                'static,
                object_store::Result<object_store::path::Path>,
            >,
        ) -> futures_util::stream::BoxStream<'static, object_store::Result<object_store::path::Path>>
        {
            self.0.delete_stream(p)
        }
        fn list(
            &self,
            p: Option<&object_store::path::Path>,
        ) -> futures_util::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
        {
            self.0.list(p)
        }
        async fn list_with_delimiter(
            &self,
            p: Option<&object_store::path::Path>,
        ) -> object_store::Result<object_store::ListResult> {
            self.0.list_with_delimiter(p).await
        }
        async fn copy_opts(
            &self,
            a: &object_store::path::Path,
            b: &object_store::path::Path,
            o: object_store::CopyOptions,
        ) -> object_store::Result<()> {
            self.0.copy_opts(a, b, o).await
        }
    }

    /// A failed renewal still stops the deploy, and says whether `current`
    /// was switched: not started, done, or sent and not seen yet. The
    /// marker is removed in every case.
    #[tokio::test(start_paused = true)]
    #[allow(clippy::unwrap_used)] // test code
    async fn a_failed_marker_renewal_says_whether_current_was_switched() {
        let store: Arc<dyn ObjectStore> =
            Arc::new(NoRenewal(object_store::memory::InMemory::new()));
        let app = AppRef::new(Arc::clone(&store), "", "default", "shop");
        let past_renewal = || tokio::time::sleep(Duration::from_secs(700));
        let markers = || async {
            store
                .list(Some(&app.path("_assets/deploying/")))
                .try_collect::<Vec<_>>()
                .await
                .unwrap()
                .len()
        };

        let before = app
            .with_assets_marker(|_marker| async move {
                past_renewal().await;
                Ok(())
            })
            .await;
        let e = before.unwrap_err().to_string();
        assert!(e.contains("renewal refused"), "{e}");
        assert!(e.contains("nothing was switched"), "{e}");
        assert_eq!(markers().await, 0);

        let done = app
            .with_assets_marker(|marker| {
                let app = &app;
                async move {
                    marker.switching("v1", 1);
                    app.swap_current(None, "v1", 1).await?;
                    past_renewal().await;
                    Ok(())
                }
            })
            .await;
        let e = done.unwrap_err().to_string();
        assert!(
            e.contains("already names the new version v1 (seq 1)"),
            "{e}"
        );
        assert_eq!(markers().await, 0);

        let in_flight = app
            .with_assets_marker(|marker| async move {
                marker.switching("v2", 2);
                past_renewal().await;
                Ok(())
            })
            .await;
        let e = in_flight.unwrap_err().to_string();
        assert!(e.contains("may still land"), "{e}");
        assert!(e.contains("it names v1 (seq 1)"), "{e}");
        assert_eq!(markers().await, 0);
    }
}
