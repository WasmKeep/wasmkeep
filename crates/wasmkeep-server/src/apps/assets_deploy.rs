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
}
impl Marker {
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
        let result = async {
            let meta = self.store.head(&key).await.map_err(bucket)?;
            let (last, receiver) = tokio::sync::watch::channel(start);
            let marker = Marker { last: receiver, start: Instant::now(), bucket_ms: meta.last_modified.timestamp_millis() };
            let operation = run(marker);
            tokio::pin!(operation);
            let mut timer = tokio::time::interval_at(Instant::now() + Duration::from_secs(600), Duration::from_secs(600));
            timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    result = &mut operation => break result,
                    _ = timer.tick() => {
                        let sent = Instant::now();
                        // A changing body makes the acknowledged renewal observable in tests.
                        self.store.put(&key, wasmkeep_cluster::control::new_request_id().into_bytes().into()).await.map_err(bucket)?;
                        last.send_replace(sent);
                    }
                }
            }
        }.await;
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
        };
        tokio::time::advance(Duration::from_secs(1801)).await;
        assert!(marker.check().is_err());
        last.send_replace(Instant::now());
        assert!(marker.check().is_ok());
    }
}
