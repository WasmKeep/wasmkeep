//! An app's sealed secrets in the bucket ([ADR 0016](../../../docs/adr/0016-secrets-in-the-bucket.md)):
//!
//! ```text
//! apps/<tenant>/<app>/secrets/<seq:016x>   # sealed, immutable, conditional create
//! apps/<tenant>/<app>/secrets/current      # "<seq:016x>\n"
//! ```
//!
//! A change lists the objects and reads `current`, takes the next `seq`
//! after both (an object left by a command that died before moving
//! `current` is skipped, never reused), applies the change to the current
//! set, creates the new object, then moves `current` conditionally. A race
//! lost at either write starts over, so a concurrent change is never lost.

use std::sync::Arc;
use std::time::Duration;

use futures_util::TryStreamExt as _;
use object_store::path::Path as StorePath;
use object_store::{
    ObjectStore, ObjectStoreExt as _, PutMode, PutOptions, PutPayload, UpdateVersion,
};

use crate::secrets::{self, Place, SecretSet, SecretsError, SecretsKey};

/// How long a sealed object is kept after it stopped being current (the
/// one before `current` is always kept, for a node that read `current` just
/// before it moved).
pub const RETENTION: Duration = Duration::from_secs(3600);
/// Attempts at a change before giving up on a busy app.
const ATTEMPTS: usize = 16;

/// Why reading or changing an app's secrets failed. Never holds a value.
#[derive(Debug, thiserror::Error)]
pub enum SecretStoreError {
    /// The bucket refused or did not answer.
    #[error("bucket: {0}")]
    Bucket(String),
    /// `current` or an object is not what this format writes.
    #[error("corrupt {0}")]
    Corrupt(String),
    /// `current` names an object that is not there.
    #[error("secrets/{0} is missing")]
    Missing(String),
    /// Sealing or opening failed.
    #[error("secrets/{0}: {1}")]
    Sealed(String, SecretsError),
    /// Other changes kept winning the race.
    #[error("the app's secrets kept changing; try again")]
    Busy,
}

fn bucket(e: object_store::Error) -> SecretStoreError {
    SecretStoreError::Bucket(e.to_string())
}

/// One sealed object, as listed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Listed {
    /// Its `seq`.
    pub seq: u64,
    /// When it was written, by the bucket's clock (ms since the epoch).
    pub written_ms: u64,
}

/// An app's secrets in the bucket.
#[derive(Debug, Clone)]
pub struct SecretStore {
    store: Arc<dyn ObjectStore>,
    prefix: String,
    tenant: String,
    app: String,
}

impl SecretStore {
    /// App `tenant/app` under the bucket key prefix `prefix` (may be empty;
    /// slashes around it are ignored).
    pub fn new(store: Arc<dyn ObjectStore>, prefix: &str, tenant: &str, app: &str) -> Self {
        Self {
            store,
            prefix: prefix.trim_matches('/').to_owned(),
            tenant: tenant.to_owned(),
            app: app.to_owned(),
        }
    }

    fn place(&self) -> Place<'_> {
        Place::new(&self.prefix, &self.tenant, &self.app)
    }

    fn base(&self) -> String {
        let prefix = if self.prefix.is_empty() {
            String::new()
        } else {
            format!("{}/", self.prefix)
        };
        format!("{prefix}apps/{}/{}/secrets", self.tenant, self.app)
    }

    fn path(&self, name: &str) -> StorePath {
        StorePath::from(format!("{}/{name}", self.base()))
    }

    /// The `seq` `current` names, with its version for a conditional
    /// replace; `None` if the app has no secrets yet.
    pub async fn current(&self) -> Result<Option<(u64, UpdateVersion)>, SecretStoreError> {
        match self.store.get(&self.path("current")).await {
            Ok(r) => {
                let version = UpdateVersion {
                    e_tag: r.meta.e_tag.clone(),
                    version: r.meta.version.clone(),
                };
                let bytes = r.bytes().await.map_err(bucket)?;
                let seq = std::str::from_utf8(&bytes)
                    .ok()
                    .and_then(|t| t.strip_suffix('\n'))
                    .and_then(secrets::parse_object_name)
                    .ok_or_else(|| SecretStoreError::Corrupt("secrets/current".into()))?;
                Ok(Some((seq, version)))
            }
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(bucket(e)),
        }
    }

    /// Every sealed object, in `seq` order. Other keys are ignored.
    pub async fn list(&self) -> Result<Vec<Listed>, SecretStoreError> {
        let base = StorePath::from(self.base());
        let metas: Vec<_> = self
            .store
            .list(Some(&base))
            .try_collect()
            .await
            .map_err(bucket)?;
        let mut out: Vec<Listed> = metas
            .iter()
            .filter_map(|m| {
                let seq = secrets::parse_object_name(m.location.filename()?)?;
                let written_ms = u64::try_from(m.last_modified.timestamp_millis()).unwrap_or(0);
                Some(Listed { seq, written_ms })
            })
            .collect();
        out.sort_by_key(|l| l.seq);
        Ok(out)
    }

    /// Opens object `seq` with whichever of `keys` sealed it. An object
    /// larger than [`secrets::MAX_OBJECT`] is refused before it is read.
    pub async fn read(&self, keys: &[SecretsKey], seq: u64) -> Result<SecretSet, SecretStoreError> {
        let name = secrets::object_name(seq);
        let path = self.path(&name);
        let meta = match self.store.head(&path).await {
            Ok(meta) => meta,
            Err(object_store::Error::NotFound { .. }) => {
                return Err(SecretStoreError::Missing(name));
            }
            Err(e) => return Err(bucket(e)),
        };
        if meta.size > secrets::MAX_OBJECT as u64 {
            return Err(SecretStoreError::Sealed(name, SecretsError::TooLarge));
        }
        let bytes = match self.store.get(&path).await {
            Ok(r) => r.bytes().await.map_err(bucket)?,
            Err(object_store::Error::NotFound { .. }) => {
                return Err(SecretStoreError::Missing(name));
            }
            Err(e) => return Err(bucket(e)),
        };
        secrets::open(keys, &self.place(), seq, &bytes)
            .map_err(|e| SecretStoreError::Sealed(name, e))
    }

    /// The set `current` names (empty if none), and its `seq`.
    pub async fn read_current(
        &self,
        keys: &[SecretsKey],
    ) -> Result<(Option<u64>, SecretSet), SecretStoreError> {
        match self.current().await? {
            Some((seq, _)) => Ok((Some(seq), self.read(keys, seq).await?)),
            None => Ok((None, SecretSet::default())),
        }
    }

    /// Applies `change` to the current set (opened with `keys`), seals the
    /// result with `seal_with` as the next `seq` and makes it current.
    /// Returns the new `seq`. `change` may run more than once (a lost race
    /// starts over on the newer set).
    pub async fn change<F, E>(
        &self,
        keys: &[SecretsKey],
        seal_with: &SecretsKey,
        mut change: F,
    ) -> Result<Result<u64, E>, SecretStoreError>
    where
        F: FnMut(&mut SecretSet) -> Result<(), E>,
    {
        for _ in 0..ATTEMPTS {
            let read = self.current().await?;
            let mut set = match &read {
                Some((seq, _)) => self.read(keys, *seq).await?,
                None => SecretSet::default(),
            };
            if let Err(e) = change(&mut set) {
                return Ok(Err(e));
            }
            if let Some(seq) = self.write(read, seal_with, &set).await? {
                return Ok(Ok(seq));
            }
        }
        Err(SecretStoreError::Busy)
    }

    /// Writes `set` as the next `seq` without opening the current one (a
    /// lost key: ADR 0016, `reset`).
    pub async fn reset(
        &self,
        seal_with: &SecretsKey,
        set: &SecretSet,
    ) -> Result<u64, SecretStoreError> {
        for _ in 0..ATTEMPTS {
            let read = self.current().await?;
            if let Some(seq) = self.write(read, seal_with, set).await? {
                return Ok(seq);
            }
        }
        Err(SecretStoreError::Busy)
    }

    /// One attempt: the next `seq` after every listed object and `current`,
    /// a conditional create, a conditional move of `current`. `None`: a race
    /// was lost, start over.
    async fn write(
        &self,
        read: Option<(u64, UpdateVersion)>,
        seal_with: &SecretsKey,
        set: &SecretSet,
    ) -> Result<Option<u64>, SecretStoreError> {
        let highest = self.list().await?.last().map_or(0, |l| l.seq);
        let current = read.as_ref().map_or(0, |(s, _)| *s);
        let next = highest.max(current) + 1;
        let name = secrets::object_name(next);
        let sealed = secrets::seal(seal_with, &self.place(), next, set)
            .map_err(|e| SecretStoreError::Sealed(name.clone(), e))?;
        let create = PutOptions {
            mode: PutMode::Create,
            ..PutOptions::default()
        };
        match self
            .store
            .put_opts(&self.path(&name), PutPayload::from(sealed), create)
            .await
        {
            Ok(_) => {}
            Err(
                object_store::Error::AlreadyExists { .. }
                | object_store::Error::Precondition { .. },
            ) => {
                return Ok(None);
            }
            Err(e) => return Err(bucket(e)),
        }
        let mode = match read {
            None => PutMode::Create,
            Some((_, version)) => PutMode::Update(version),
        };
        let swap = PutOptions {
            mode,
            ..PutOptions::default()
        };
        let body = PutPayload::from(format!("{name}\n").into_bytes());
        match self.store.put_opts(&self.path("current"), body, swap).await {
            Ok(_) => Ok(Some(next)),
            // The new object stays as an orphan, skipped by the next `seq`
            // and deleted after the retention window.
            Err(
                object_store::Error::AlreadyExists { .. }
                | object_store::Error::Precondition { .. },
            ) => Ok(None),
            Err(e) => Err(bucket(e)),
        }
    }

    /// Deletes sealed objects no node needs any more. Kept: `current`, the
    /// one before it, and any that stopped being current (timed by its
    /// successor's write) or was written above `current` (by a command that
    /// may still be running) less than `retention` ago. Returns how many
    /// were deleted.
    pub async fn delete_old(
        &self,
        now_ms: u64,
        retention: Duration,
    ) -> Result<usize, SecretStoreError> {
        let Some((current, _)) = self.current().await? else {
            return Ok(0);
        };
        let listed = self.list().await?;
        let retention = u64::try_from(retention.as_millis()).unwrap_or(u64::MAX);
        let old = |written: u64| now_ms.saturating_sub(written) > retention;
        let previous = listed.iter().rev().find(|l| l.seq < current).map(|l| l.seq);
        let mut doomed = Vec::new();
        for (i, l) in listed.iter().enumerate() {
            let delete = if l.seq == current || Some(l.seq) == previous {
                false
            } else if l.seq < current {
                // Its successor's write is when it stopped being current.
                listed.get(i + 1).is_some_and(|next| old(next.written_ms))
            } else {
                old(l.written_ms)
            };
            if delete {
                doomed.push(l.seq);
            }
        }
        for seq in &doomed {
            match self
                .store
                .delete(&self.path(&secrets::object_name(*seq)))
                .await
            {
                Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
                Err(e) => return Err(bucket(e)),
            }
        }
        Ok(doomed.len())
    }
}

/// How long a node may go without opening the set `current` names before
/// it stops running the app's actors (ADR 0016).
pub const STALE_AFTER: Duration = Duration::from_secs(30);

impl SecretStore {
    /// Follows `current` every `period` and swaps each newer set into
    /// `service`. `opened` is the `seq` already applied at start;
    /// `stale_after` is [`STALE_AFTER`] outside tests.
    ///
    /// - An object this node cannot open (no key, another key, altered,
    ///   too large) blocks it at once: it refuses the app's events and hands
    ///   its actors over (`ActorService::set_secrets_blocked`).
    /// - A newer set it cannot reach (the named object missing, or `current`
    ///   naming a lower `seq` than one already opened, or an unreadable
    ///   `current` or object while the bucket answers) blocks it after
    ///   `stale_after`.
    /// - A bucket outage changes nothing: every node shares it, and leases
    ///   stop anyway.
    pub async fn follow(
        self,
        service: crate::ActorService,
        keys: Vec<SecretsKey>,
        mut opened: Option<u64>,
        period: Duration,
        stale_after: Duration,
    ) {
        let mut stale_since: Option<tokio::time::Instant> = None;
        let mut warned_lower: Option<u64> = None;
        loop {
            tokio::time::sleep(period).await;
            let current = match self.current().await {
                Ok(current) => current,
                // The bucket did not answer: every node shares that, and
                // leases stop anyway. Keep the last set.
                Err(SecretStoreError::Bucket(e)) => {
                    tracing::debug!(error = %e, "reading the app's secrets failed; keeping the last set");
                    continue;
                }
                // `current` answered with something unreadable.
                Err(e) => {
                    tracing::warn!(error = %e, "secrets/current is unreadable");
                    if stale(&mut stale_since, stale_after) {
                        service.set_secrets_blocked(true);
                    }
                    continue;
                }
            };
            let Some((seq, _)) = current else {
                // Deleted: keep the set this node has.
                stale_since = None;
                service.set_secrets_blocked(false);
                continue;
            };
            if opened == Some(seq) {
                stale_since = None;
                service.set_secrets_blocked(false);
                continue;
            }
            if opened.is_some_and(|o| seq < o) {
                if warned_lower != Some(seq) {
                    warned_lower = Some(seq);
                    tracing::warn!(seq, opened = ?opened, "secrets/current names an older seq than this node opened; refused (after restoring the bucket from a backup, restart the nodes)");
                }
            } else {
                match self.read(&keys, seq).await {
                    Ok(set) => {
                        service.apply_secrets(set);
                        opened = Some(seq);
                        stale_since = None;
                        service.set_secrets_blocked(false);
                        tracing::info!(seq, "secrets updated");
                        continue;
                    }
                    // Deleted after a newer `current` was written, or not
                    // readable although `current` just answered: read
                    // `current` again next round, and block if it lasts.
                    Err(SecretStoreError::Missing(name)) => {
                        tracing::debug!(%name, "the named secrets object is gone; reading current again");
                    }
                    Err(SecretStoreError::Bucket(e)) => {
                        tracing::warn!(error = %e, "the current secrets object cannot be read");
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "cannot open the app's current secrets");
                        service.set_secrets_blocked(true);
                        continue;
                    }
                }
            }
            if stale(&mut stale_since, stale_after) {
                service.set_secrets_blocked(true);
            }
        }
    }
}

/// Starts or reads the stale timer: whether it has run past `after`.
fn stale(since: &mut Option<tokio::time::Instant>, after: Duration) -> bool {
    since
        .get_or_insert_with(tokio::time::Instant::now)
        .elapsed()
        >= after
}

/// Every app under `prefix` that has secrets (`tenant/app`), for changing
/// the key of all of them.
pub async fn apps_with_secrets(
    store: &Arc<dyn ObjectStore>,
    prefix: &str,
) -> Result<Vec<(String, String)>, SecretStoreError> {
    let prefix = prefix.trim_matches('/');
    let root = if prefix.is_empty() {
        StorePath::from("apps")
    } else {
        StorePath::from(format!("{prefix}/apps"))
    };
    let mut out = Vec::new();
    let tenants = store
        .list_with_delimiter(Some(&root))
        .await
        .map_err(bucket)?;
    for tenant in tenants.common_prefixes {
        let apps = store
            .list_with_delimiter(Some(&tenant))
            .await
            .map_err(bucket)?;
        for app in apps.common_prefixes {
            let (Some(t), Some(a)) = (tenant.filename(), app.filename()) else {
                continue;
            };
            let found = SecretStore::new(Arc::clone(store), prefix, t, a);
            if found.current().await?.is_some() {
                out.push((t.to_owned(), a.to_owned()));
            }
        }
    }
    out.sort();
    Ok(out)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)] // test code

    use object_store::memory::InMemory;

    use super::*;

    fn app(store: &Arc<dyn ObjectStore>) -> SecretStore {
        SecretStore::new(Arc::clone(store), "prod", "acme", "shop")
    }

    fn block_on<F: std::future::Future>(f: F) -> F::Output {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(f)
    }

    fn set_one(s: &mut SecretSet, name: &str, value: &str) -> Result<(), SecretsError> {
        s.set("api", name, value)
    }

    #[test]
    fn changes_build_on_each_other_and_move_current() {
        block_on(async {
            let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let key = SecretsKey::generate().unwrap();
            let keys = std::slice::from_ref(&key);
            let app = app(&store);
            assert_eq!(app.read_current(keys).await.unwrap().0, None);
            let one = app
                .change(keys, &key, |s| set_one(s, "A", "1"))
                .await
                .unwrap()
                .unwrap();
            let two = app
                .change(keys, &key, |s| set_one(s, "B", "2"))
                .await
                .unwrap()
                .unwrap();
            assert_eq!((one, two), (1, 2));
            let (seq, set) = app.read_current(keys).await.unwrap();
            assert_eq!(seq, Some(2));
            assert_eq!(set.class("api").unwrap().len(), 2);
            // A refused change writes nothing.
            let refused = app
                .change(keys, &key, |_| Err::<(), _>("no"))
                .await
                .unwrap();
            assert_eq!(refused, Err("no"));
            assert_eq!(app.list().await.unwrap().len(), 2);
        });
    }

    #[test]
    fn an_object_left_by_a_dead_command_is_skipped() {
        block_on(async {
            let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let key = SecretsKey::generate().unwrap();
            let keys = std::slice::from_ref(&key);
            let app = app(&store);
            app.change(keys, &key, |s| set_one(s, "A", "1"))
                .await
                .unwrap()
                .unwrap();
            // A command that created seq 2 and died before moving current.
            let orphan = secrets::seal(&key, &app.place(), 2, &SecretSet::default()).unwrap();
            store
                .put(&app.path(&secrets::object_name(2)), orphan.into())
                .await
                .unwrap();
            let seq = app
                .change(keys, &key, |s| set_one(s, "B", "2"))
                .await
                .unwrap()
                .unwrap();
            assert_eq!(seq, 3, "the orphan's seq is never reused");
            assert_eq!(
                app.read_current(keys)
                    .await
                    .unwrap()
                    .1
                    .class("api")
                    .unwrap()
                    .len(),
                2
            );
        });
    }

    #[test]
    fn concurrent_changes_all_land() {
        block_on(async {
            let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let key = SecretsKey::generate().unwrap();
            let changes = (0..8).map(|i| {
                let (app, key) = (app(&store), key.clone());
                tokio::spawn(async move {
                    app.change(std::slice::from_ref(&key), &key, |s| {
                        set_one(s, &format!("N{i}"), "v")
                    })
                    .await
                    .unwrap()
                    .unwrap()
                })
            });
            for c in changes.collect::<Vec<_>>() {
                c.await.unwrap();
            }
            let set = app(&store)
                .read_current(std::slice::from_ref(&key))
                .await
                .unwrap()
                .1;
            assert_eq!(set.class("api").unwrap().len(), 8);
        });
    }

    #[test]
    fn a_reset_needs_no_old_key_and_a_new_key_is_needed_after() {
        block_on(async {
            let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let (lost, new) = (
                SecretsKey::generate().unwrap(),
                SecretsKey::generate().unwrap(),
            );
            let app = app(&store);
            app.change(std::slice::from_ref(&lost), &lost, |s| set_one(s, "A", "1"))
                .await
                .unwrap()
                .unwrap();
            let mut fresh = SecretSet::default();
            fresh.set("api", "A", "2").unwrap();
            assert_eq!(app.reset(&new, &fresh).await.unwrap(), 2);
            assert!(app.read_current(std::slice::from_ref(&lost)).await.is_err());
            assert_eq!(
                app.read_current(std::slice::from_ref(&new))
                    .await
                    .unwrap()
                    .1,
                fresh
            );
        });
    }

    #[test]
    fn old_objects_go_after_the_window_and_the_one_before_current_stays() {
        block_on(async {
            let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let key = SecretsKey::generate().unwrap();
            let keys = std::slice::from_ref(&key);
            let app = app(&store);
            for i in 0..4 {
                app.change(keys, &key, |s| set_one(s, &format!("N{i}"), "v"))
                    .await
                    .unwrap()
                    .unwrap();
            }
            let now = app.list().await.unwrap().last().unwrap().written_ms;
            // Within the window: nothing goes.
            assert_eq!(app.delete_old(now, RETENTION).await.unwrap(), 0);
            // Past it: 1 and 2 go; 3 (before current) and 4 (current) stay.
            assert_eq!(app.delete_old(now + 7_200_000, RETENTION).await.unwrap(), 2);
            let left: Vec<u64> = app.list().await.unwrap().iter().map(|l| l.seq).collect();
            assert_eq!(left, [3, 4]);
            assert_eq!(app.read_current(keys).await.unwrap().0, Some(4));
        });
    }

    #[test]
    fn an_oversized_or_missing_object_is_refused_unread() {
        block_on(async {
            let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let key = SecretsKey::generate().unwrap();
            let app = app(&store);
            store
                .put(
                    &app.path(&secrets::object_name(1)),
                    vec![b'a'; secrets::MAX_OBJECT + 1].into(),
                )
                .await
                .unwrap();
            assert!(matches!(
                app.read(std::slice::from_ref(&key), 1).await,
                Err(SecretStoreError::Sealed(_, SecretsError::TooLarge))
            ));
            assert!(matches!(
                app.read(std::slice::from_ref(&key), 9).await,
                Err(SecretStoreError::Missing(_))
            ));
        });
    }

    #[test]
    fn apps_with_secrets_are_found_under_the_prefix() {
        block_on(async {
            let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let key = SecretsKey::generate().unwrap();
            for (t, a) in [("acme", "shop"), ("acme", "blog")] {
                SecretStore::new(Arc::clone(&store), "prod", t, a)
                    .change(std::slice::from_ref(&key), &key, |s| set_one(s, "A", "1"))
                    .await
                    .unwrap()
                    .unwrap();
            }
            store
                .put(&StorePath::from("prod/apps/acme/plain/current"), "x".into())
                .await
                .unwrap();
            assert_eq!(
                apps_with_secrets(&store, "prod/").await.unwrap(),
                [
                    ("acme".to_owned(), "blog".to_owned()),
                    ("acme".to_owned(), "shop".to_owned())
                ]
            );
        });
    }
}
