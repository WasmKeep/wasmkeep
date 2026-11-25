//! Deploying actor classes through the bucket, and nodes following the
//! deployed version (ADR 0008).
//!
//! ```text
//! apps/<tenant>/<app>/<version>/<class>.wasm   as uploaded, immutable
//! apps/<tenant>/<app>/<version>/<class>.config plain configuration, immutable
//! apps/<tenant>/<app>/<version>/_front.wasm    the front (ADR 0023), immutable
//! apps/<tenant>/<app>/<version>/_front.config  its plain configuration, immutable
//! apps/<tenant>/<app>/<version>/manifest       previous, classes, rules, config digests
//! apps/<tenant>/<app>/current                  "<version> <seq>"; If-Match only
//! ```
//!
//! A version is its classes with their access rules and plain
//! configuration (ADR 0013): they change together, and every version's
//! directory holds all of it.
//!
//! `seq` increases with every change of `current`, rollbacks included. Each
//! actor's database records the highest `seq` that ran an event on it, and a
//! node behind that refuses the actor instead of running older code.

pub mod limits;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use wasmkeep_cluster::restore_fence::{marker_matches, start_allowed};

use object_store::path::Path as StorePath;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload, UpdateVersion};
use sha2::{Digest, Sha256};
use wasmkeep_runtime::Runtime;

mod assets_deploy;
#[cfg(test)]
mod assets_tests;
mod collect;
mod delete;
mod deploy;
mod diff;
mod follow;
mod manifest;
mod readers;
#[cfg(test)]
mod tests;
use crate::access::AccessRules;
use crate::actor::FRONT_CLASS as FRONT;
use crate::env::Vars;
pub use collect::KEEP_PREVIOUS;
pub use delete::fence_app_nodes;
pub use delete::{DeleteApp, LATE_WRITES, delete_app};
pub use diff::Diff;
pub use follow::slow_follower_compiles;
pub use manifest::{
    Changes, Current, FrontChange, Manifest, Part, Version, VersionFront, format_config,
};
pub use readers::{Readers, live_nodes};

use diff::Settings;
use follow::read_local_marker;
use manifest::parse_config;

/// Why a deploy, rollback or update failed.
#[derive(Debug, thiserror::Error)]
pub enum DeployError {
    /// A live node cannot read or serve static assets.
    #[error(
        "live nodes of {app} cannot read a version with assets (manifest format 7) ({nodes}): upgrade them first"
    )]
    NodesWithoutAssets { app: String, nodes: String },
    /// A component does not implement the actor world (nothing was uploaded).
    #[error("class {0:?} is not a valid actor: {1}")]
    Invalid(String, String),
    /// The bucket did not answer, or refused a write.
    #[error("bucket: {0}")]
    Bucket(String),
    /// This node could not read its own local copy of an app marker
    /// (`.recreated`, `.restored`): not the bucket's, and not a sign the
    /// app was deleted.
    #[error("local files: {0}")]
    Local(String),
    /// Someone else changed `current` at the same moment; nothing changed.
    #[error("another deploy changed the app at the same time; retry")]
    Conflict,
    /// The bucket prefix is being migrated, or holds the tenant's actor
    /// state in the layout before 0.4.0 (ADR 0022): migrate with 0.8.x.
    #[error("{0}")]
    Layout(String),
    /// Nothing is deployed yet, or there is nothing to roll back to.
    #[error("{0}")]
    Nothing(&'static str),
    /// A stored object is not what this version of WasmKeep writes.
    #[error("unreadable {0}")]
    Corrupt(String),
    /// The front given is not one (nothing was uploaded), or front values
    /// were given for a version without a front (ADR 0023).
    #[error("front: {0}")]
    Front(String),
    /// Live nodes of the app do not announce the capability word `front`
    /// (ADR 0023): they could not read a version with a front. There is no
    /// `--force`.
    #[error(
        "live nodes of {app} cannot read a version with a front or a rule written `open` \
         (manifest format 3) ({nodes}): upgrade them first (this check has no --force: such a \
         node could not read the version at all)"
    )]
    NodesWithoutFront {
        /// `tenant/app`.
        app: String,
        /// Their ids, comma separated.
        nodes: String,
    },
    /// Live nodes of the app do not announce the capability word `ends`
    /// (ADR 0028): they could not read a version with an idle limit
    /// (manifest format 4).
    #[error(
        "live nodes of {app} cannot read a version with an idle limit (manifest format 4) \
         ({nodes}): upgrade them first"
    )]
    NodesWithoutEnds {
        /// `tenant/app`.
        app: String,
        /// Their ids, comma separated.
        nodes: String,
    },
    /// Live nodes of the app do not announce the capability word `flush`
    /// (ADR 0034, section 15): they cannot link a component that imports
    /// `storage.flush`, nor read a version with a batched class (manifest
    /// format 5). There is no `--force`.
    #[error(
        "live nodes of {app} do not have storage.flush ({nodes}), which {what}: upgrade \
         every node of the app first, then deploy (this check has no --force: such a node \
         would stay not ready on the version)"
    )]
    NodesWithoutFlush {
        /// `tenant/app`.
        app: String,
        /// Their ids, comma separated.
        nodes: String,
        /// What needs it: the classes importing it, or the format.
        what: String,
    },
    /// Live nodes of the app do not announce the capability word `blobs`
    /// (ADR 0030, section 11) while a class or the front imports
    /// `wasmkeep:actor/blobs`: they cannot link it, nor end an actor's
    /// files with it. There is no `--force`.
    #[error(
        "live nodes of {app} do not have files ({nodes}), which {what} wasmkeep:actor/blobs: \
         upgrade every node of the app first, then deploy (this check has no --force)"
    )]
    NodesWithoutBlobs {
        /// `tenant/app`.
        app: String,
        /// Their ids, comma separated.
        nodes: String,
        /// The classes (or `_front`) importing it.
        what: String,
    },
    /// Live nodes of the app do not announce the capability word `ends`
    /// (ADR 0028) while a component imports `storage.delete-self-durable`
    /// (ADR 0034, section 1, exception 2): they cannot link it.
    #[error(
        "live nodes of {app} do not have storage.delete-self-durable ({nodes}), which {what}: \
         upgrade every node of the app first, then deploy (this check has no --force: such a \
         node would stay not ready on the version)"
    )]
    NodesWithoutDeleteDurable {
        /// `tenant/app`.
        app: String,
        /// Their ids, comma separated.
        nodes: String,
        /// The classes importing it.
        what: String,
    },
    /// The app was deleted (`wasmkeep app delete`, ADR 0028): its
    /// tombstone is in the bucket.
    #[error(
        "{0} was deleted (wasmkeep app delete): nothing is written for it; to use the name \
         again, deploy with --recreate"
    )]
    Deleted(String),
    /// `wasmkeep app delete` refused, or stopped before the data went
    /// (ADR 0028): why, and what was written.
    #[error("{0}")]
    Refused(String),
    /// A rollback would make these classes more open (ADR 0013).
    #[error("rolling back would reopen {0}; pass --allow-reopen to do it anyway")]
    Reopens(String),
    /// The caller stopped the deploy after seeing its plan
    /// ([`AppRef::deploy_changes_with`]): nothing was written.
    #[error("stopped before the switch: nothing was written")]
    Stopped,
    /// A new app's tenant or app name is outside the grammar.
    #[error(
        "{what} {name:?} must match [a-z][a-z0-9-]{{0,63}} (lowercase letters, digits and \
         '-', starting with a letter) for a new app; apps deployed before keep their names"
    )]
    Name {
        /// `tenant` or `app`.
        what: &'static str,
        /// The name given.
        name: String,
    },
}

/// How a class's plain writes reach the bucket (ADR 0034, section 1): part
/// of the class's version, like its access rule.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub enum Writes {
    /// Every event's writes are uploaded before the next event starts:
    /// the behaviour of every release before, and the default.
    #[default]
    Strict,
    /// Opt in: plain writes are uploaded behind the events, one segment in
    /// flight per actor.
    Batched,
}

impl Writes {
    /// As the project file writes it: `strict` or `batched`.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Strict => "strict",
            Self::Batched => "batched",
        }
    }

    /// `strict` or `batched`.
    ///
    /// # Errors
    /// Anything else, with what is accepted.
    pub fn parse(s: &str) -> Result<Self, String> {
        match s {
            "strict" => Ok(Self::Strict),
            "batched" => Ok(Self::Batched),
            _ => Err(format!("{s:?} is not a mode of writes (strict, batched)")),
        }
    }
}

fn bucket(e: impl std::fmt::Display) -> DeployError {
    DeployError::Bucket(e.to_string())
}

/// Where an app lives in the bucket.
#[derive(Debug, Clone)]
pub struct AppRef {
    store: Arc<dyn ObjectStore>,
    /// `<prefix>apps/<tenant>/<app>/`
    base: String,
    /// The bucket key prefix, empty or ending in `/`.
    prefix: String,
    tenant: String,
    app: String,
}

/// The app's tombstone, under `apps/<t>/<a>/` (ADR 0028, section 5).
pub const TOMBSTONE: &str = "deleted";

/// Left by `deploy --recreate` (the tombstone's body): a node whose local
/// files of the app hold another value removes them before it serves.
const RECREATED: &str = "recreated";

/// The local copy of [`RECREATED`], in the app's folder under the data
/// directory.
const RECREATED_LOCAL: &str = ".recreated";

impl AppRef {
    /// App `app` of `tenant`, under the bucket key prefix `prefix` (may be
    /// empty).
    pub fn new(store: Arc<dyn ObjectStore>, prefix: &str, tenant: &str, app: &str) -> Self {
        let prefix = if prefix.is_empty() || prefix.ends_with('/') {
            prefix.to_owned()
        } else {
            format!("{prefix}/")
        };
        Self {
            // Counted for the metrics page while `serve` follows the app.
            store: crate::counted_store::counted(store),
            base: format!("{prefix}apps/{tenant}/{app}/"),
            prefix,
            tenant: tenant.to_owned(),
            app: app.to_owned(),
        }
    }

    fn path(&self, key: &str) -> StorePath {
        StorePath::from(format!("{}{key}", self.base))
    }

    /// `tenant/app`.
    #[must_use]
    pub fn name(&self) -> String {
        format!("{}/{}", self.tenant, self.app)
    }

    /// The app's tombstone (`apps/<t>/<a>/deleted`, ADR 0028), if it was
    /// deleted: its body (who and when).
    ///
    /// # Errors
    /// The bucket did not answer.
    pub async fn tombstone(&self) -> Result<Option<String>, DeployError> {
        Ok(self
            .get(TOMBSTONE)
            .await?
            .map(|(bytes, _)| String::from_utf8_lossy(&bytes).into_owned()))
    }

    /// Refuse destructive app operations while a restore holds its fence.
    pub(super) async fn check_not_restoring(&self) -> Result<(), DeployError> {
        if !start_allowed(false, self.get("restoring").await?.is_some()) {
            return Err(DeployError::Refused(format!(
                "{} is restoring",
                self.name()
            )));
        }
        Ok(())
    }

    /// [`DeployError::Deleted`] if the app has a tombstone.
    ///
    /// # Errors
    /// Deleted, or the bucket did not answer.
    pub async fn check_not_deleted(&self) -> Result<(), DeployError> {
        match self.tombstone().await? {
            Some(_) => Err(DeployError::Deleted(self.name())),
            None => {
                if !start_allowed(false, self.get("restoring").await?.is_some()) {
                    return Err(DeployError::Deleted(format!("{} (restoring)", self.name())));
                }
                Ok(())
            }
        }
    }

    /// Writes the app's tombstone, `If-None-Match: *`, holding `body` (who
    /// and when): `true` if this wrote it, `false` if it was already there
    /// (an `app delete` resumed).
    ///
    /// # Errors
    /// The bucket did not answer.
    pub async fn write_tombstone(&self, body: &str) -> Result<bool, DeployError> {
        self.check_not_restoring().await?;
        let opts = PutOptions {
            mode: PutMode::Create,
            ..PutOptions::default()
        };
        match self
            .store
            .put_opts(
                &self.path(TOMBSTONE),
                PutPayload::from(body.as_bytes().to_vec()),
                opts,
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

    /// Every key of the app but its tombstone, as listed (full locations in
    /// the store's encoding, for [`Self::delete_keys`]): under `state/<t>/<a>/` (histories, ownership records, the
    /// alarm index, requests) and `apps/<t>/<a>/` (every version, the
    /// deploy history, the sealed secrets).
    ///
    /// # Errors
    /// The bucket did not answer.
    pub async fn app_keys(&self) -> Result<Vec<String>, DeployError> {
        self.check_not_restoring().await?;
        use futures_util::TryStreamExt as _;
        let tombstone = self.path(TOMBSTONE);
        let mut out = Vec::new();
        for base in [
            format!("{}state/{}/{}/", self.prefix, self.tenant, self.app),
            self.base.clone(),
        ] {
            let listed: Vec<_> = self
                .store
                .list(Some(&StorePath::from(base.trim_end_matches('/'))))
                .try_collect()
                .await
                .map_err(bucket)?;
            out.extend(
                listed
                    .into_iter()
                    .filter(|m| m.location != tombstone)
                    .map(|m| m.location.to_string()),
            );
        }
        out.sort();
        Ok(out)
    }

    /// Whether the app has any key but its tombstone, under either of the
    /// prefixes [`Self::app_keys`] lists: the first page of each listing
    /// at most, never the whole app (an app with a long history has
    /// hundreds of thousands of keys, minutes of listing).
    ///
    /// # Errors
    /// The bucket did not answer.
    pub async fn has_keys(&self) -> Result<bool, DeployError> {
        use futures_util::TryStreamExt as _;
        let tombstone = self.path(TOMBSTONE);
        for base in [
            format!("{}state/{}/{}/", self.prefix, self.tenant, self.app),
            self.base.clone(),
        ] {
            let mut listed = self
                .store
                .list(Some(&StorePath::from(base.trim_end_matches('/'))));
            while let Some(meta) = listed.try_next().await.map_err(bucket)? {
                if meta.location != tombstone {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    /// Deletes `keys` (from [`Self::app_keys`]), at most 16 at a time.
    ///
    /// The keys are listed locations, already in the store's encoding: an
    /// escaped actor name (`bg%3Alobby`) is listed as `bg%253Alobby`. Each
    /// is parsed as is, never through `StorePath::from`, which would escape
    /// its `%` once more and address a key that does not exist (a delete
    /// of it succeeds, and the key stays).
    ///
    /// # Errors
    /// A key is not a listed location, or the bucket did not answer for
    /// one of them.
    pub async fn delete_keys(&self, keys: &[String]) -> Result<(), DeployError> {
        self.check_not_restoring().await?;
        use futures_util::StreamExt as _;
        let results: Vec<Result<(), DeployError>> = futures_util::stream::iter(keys)
            .map(|k| async move {
                let location = StorePath::parse(k).map_err(bucket)?;
                match self.store.delete(&location).await {
                    Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
                    Err(e) => Err(bucket(e)),
                }
            })
            .buffer_unordered(16)
            .collect()
            .await;
        results.into_iter().collect()
    }

    /// `deploy --recreate` (ADR 0028): removes the tombstone, only once
    /// nothing else is left under the app's two prefixes, and leaves the
    /// marker nodes compare their local files with
    /// (`apps/<t>/<a>/recreated`, the tombstone's body). `false` if the
    /// app has no tombstone (nothing to do). Refused until `late` has
    /// passed since the tombstone and since any of the app's nodes last
    /// wrote its heartbeat ([`LATE_WRITES`]): a write sent before then may
    /// still land.
    ///
    /// # Errors
    /// Something is left (run `app delete` again), too soon, or the bucket.
    pub async fn recreate(&self, late: Duration) -> Result<bool, DeployError> {
        self.check_not_restoring().await?;
        let Some(body) = self.tombstone().await? else {
            return Ok(false);
        };
        let written = body
            .lines()
            .find_map(|l| l.strip_prefix("at ")?.parse::<u64>().ok())
            .unwrap_or(0);
        let heartbeats = wasmkeep_cluster::membership::all_heartbeats(
            &wasmkeep_cluster::object_store::ObjectStoreBucket::new(
                Arc::clone(&self.store),
                self.prefix.clone(),
            ),
        )
        .await
        .map_err(bucket)?;
        let served = self.name();
        let last = heartbeats
            .iter()
            .filter(|n| n.app == served)
            .map(|n| n.written_at_ms)
            .fold(written, u64::max);
        let now = u64::try_from(crate::alarm::now_ms_i64()).unwrap_or(0);
        let ready = last.saturating_add(u64::try_from(late.as_millis()).unwrap_or(u64::MAX));
        if now < ready {
            return Err(DeployError::Refused(format!(
                "{served} was deleted moments ago: a write its nodes sent may still land; \
                 --recreate in {}s",
                (ready - now).div_ceil(1000)
            )));
        }
        let left = self.app_keys().await?;
        if !left.is_empty() {
            return Err(DeployError::Nothing(
                "the deleted app still has data in the bucket: run `wasmkeep app delete` again to \
                 finish removing it, then --recreate",
            ));
        }
        self.store
            .put(&self.path(RECREATED), PutPayload::from(body.into_bytes()))
            .await
            .map_err(bucket)?;
        match self.store.delete(&self.path(TOMBSTONE)).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(true),
            Err(e) => Err(bucket(e)),
        }
    }

    /// Before a node serves the app (ADR 0028): refuses an app with a
    /// tombstone, removing this node's local files of it; and, after a
    /// `--recreate`, removes local files kept from before the delete
    /// (`local` is the app's folder under the data directory), so no
    /// deleted actor comes back from a disk.
    ///
    /// Returns the app's epoch (ADR 0030, section 2), from the two
    /// markers it read: the segment every file key of this run carries
    /// ([`crate::ActorConfig::blob_epoch`]).
    ///
    /// # Errors
    /// [`DeployError::Deleted`], the bucket, or the local folder.
    pub async fn prepare_local(&self, local: &std::path::Path) -> Result<String, DeployError> {
        let remove = |why: &str| {
            if local.exists() {
                tracing::warn!(dir = %local.display(), "{why}: removing this node's local files of the app");
                std::fs::remove_dir_all(local)
                    .map_err(|e| DeployError::Bucket(format!("removing {}: {e}", local.display())))
            } else {
                Ok(())
            }
        };
        if self.tombstone().await?.is_some() {
            remove("the app was deleted")?;
            return Err(DeployError::Deleted(self.name()));
        }
        self.check_not_deleted().await?;
        // Read both before removing the directory; writing one marker and then
        // removing it for the other would invalidate the next start forever.
        let markers = [
            (RECREATED_LOCAL, self.get(RECREATED).await?),
            (".restored", self.get("restored").await?),
        ];
        let epoch = crate::blobs::epoch(
            markers[0].1.as_ref().map(|(body, _)| body.as_slice()),
            markers[1].1.as_ref().map(|(body, _)| body.as_slice()),
        );
        // A local marker this node cannot read is an error, never a reason
        // to remove the app's files.
        let mut stale = false;
        for (file, value) in &markers {
            if let Some((body, _)) = value {
                let ours = read_local_marker(&local.join(file))?;
                stale |= !marker_matches(Some(body.as_slice()), ours.as_deref());
            }
        }
        if stale {
            remove("the app was recreated or restored since this node last ran it")?;
            std::fs::create_dir_all(local).map_err(bucket)?;
            for (file, value) in markers {
                if let Some((body, _)) = value {
                    std::fs::write(local.join(file), body).map_err(bucket)?;
                }
            }
        }
        Ok(epoch)
    }

    async fn get(&self, key: &str) -> Result<Option<(Vec<u8>, UpdateVersion)>, DeployError> {
        match self.store.get(&self.path(key)).await {
            Ok(r) => {
                let version = UpdateVersion {
                    e_tag: r.meta.e_tag.clone(),
                    version: r.meta.version.clone(),
                };
                let bytes = r.bytes().await.map_err(bucket)?;
                Ok(Some((bytes.to_vec(), version)))
            }
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(bucket(e)),
        }
    }

    /// Creates an immutable object; one that already exists is the same
    /// content (keys are content-addressed).
    async fn put_once(&self, key: &str, bytes: Vec<u8>) -> Result<(), DeployError> {
        let opts = PutOptions {
            mode: PutMode::Create,
            ..PutOptions::default()
        };
        match self
            .store
            .put_opts(&self.path(key), PutPayload::from(bytes), opts)
            .await
        {
            Ok(_) | Err(object_store::Error::AlreadyExists { .. }) => Ok(()),
            Err(e) => Err(bucket(e)),
        }
    }

    /// The deployed version and its `seq`, if any.
    pub async fn current(&self) -> Result<Option<Current>, DeployError> {
        Ok(self.read_current().await?.map(|(c, _)| c))
    }

    async fn read_current(&self) -> Result<Option<(Current, UpdateVersion)>, DeployError> {
        let Some((bytes, version)) = self.get("current").await? else {
            return Ok(None);
        };
        let current =
            Current::parse(&bytes).ok_or_else(|| DeployError::Corrupt("current".into()))?;
        Ok(Some((current, version)))
    }

    /// A version's manifest.
    pub async fn manifest(&self, version: &str) -> Result<Manifest, DeployError> {
        let (bytes, _) =
            self.get(&format!("{version}/manifest"))
                .await?
                .ok_or(DeployError::Corrupt(format!(
                    "{version}/manifest (missing)"
                )))?;
        Manifest::parse(&String::from_utf8_lossy(&bytes))
            .ok_or_else(|| DeployError::Corrupt(format!("{version}/manifest")))
    }

    /// The component bytes of every class of a version, checked against the
    /// manifest's digests.
    pub async fn classes(&self, version: &str) -> Result<Vec<(String, Vec<u8>)>, DeployError> {
        Ok(self.version(version).await?.classes)
    }

    /// Everything a node needs to run `version`: its components, rules and
    /// configuration, each checked against the manifest's digests. Errors
    /// name objects and lines, never a configuration value.
    pub async fn version(&self, version: &str) -> Result<Version, DeployError> {
        let manifest = self.manifest(version).await?;
        self.version_from_manifest(version, &manifest).await
    }

    async fn version_from_manifest(
        &self,
        version: &str,
        manifest: &Manifest,
    ) -> Result<Version, DeployError> {
        let assets = match &manifest.assets {
            Some(hash) => {
                let index = self.node_assets_index(hash).await?;
                let previous = if let Some(previous) = &manifest.previous {
                    match self.manifest(previous).await {
                        Ok(m) => match m.assets {
                            Some(hash) => self.node_assets_index(&hash).await.ok(),
                            None => None,
                        },
                        Err(_) => None,
                    }
                } else {
                    None
                };
                Some(crate::assets::Deployed {
                    index,
                    previous,
                    app: self.clone(),
                })
            }
            None => None,
        };
        let mut classes = Vec::new();
        for (class, digest) in &manifest.classes {
            let key = format!("{version}/{class}.wasm");
            let (bytes, _) = self
                .get(&key)
                .await?
                .ok_or_else(|| DeployError::Corrupt(format!("{key} (missing)")))?;
            if hex(&Sha256::digest(&bytes)) != *digest {
                return Err(DeployError::Corrupt(format!("{key} (digest)")));
            }
            classes.push((class.clone(), bytes));
        }
        let (access, mut config) = self.settings(version, manifest).await?;
        let mut requires = manifest.requires.clone();
        let front = match &manifest.front {
            Some(digest) => {
                let key = format!("{version}/{FRONT}.wasm");
                let (bytes, _) = self
                    .get(&key)
                    .await?
                    .ok_or_else(|| DeployError::Corrupt(format!("{key} (missing)")))?;
                if hex(&Sha256::digest(&bytes)) != *digest {
                    return Err(DeployError::Corrupt(format!("{key} (digest)")));
                }
                Some(VersionFront {
                    wasm: bytes,
                    config: config.remove(FRONT).unwrap_or_default(),
                    requires: requires.remove(FRONT).unwrap_or_default(),
                })
            }
            None => None,
        };
        Ok(Version {
            assets,
            classes,
            access,
            config,
            requires,
            expire: manifest.expire.clone(),
            batched: manifest.batched.clone(),
            limits: manifest.limits.clone(),
            front,
        })
    }

    async fn node_assets_index(
        &self,
        hash: &str,
    ) -> Result<Arc<crate::assets::Index>, DeployError> {
        use object_store::ObjectStoreExt;
        let key = format!("_assets/index/{hash}");
        let object = self
            .store
            .get(&self.path(&key))
            .await
            .map_err(|e| match e {
                object_store::Error::NotFound { .. } => {
                    DeployError::Corrupt(format!("{key} (missing)"))
                }
                e => bucket(e),
            })?;
        if object.meta.size > crate::assets::MAX_INDEX as u64 {
            return Err(DeployError::Corrupt(format!("{key} (size)")));
        }
        use futures_util::TryStreamExt;
        let mut bytes = Vec::new();
        let mut stream = object.into_stream();
        while let Some(chunk) = stream.try_next().await.map_err(bucket)? {
            if bytes.len().saturating_add(chunk.len()) > crate::assets::MAX_INDEX {
                return Err(DeployError::Corrupt(format!("{key} (size)")));
            }
            bytes.extend_from_slice(&chunk);
        }
        if crate::assets::digest(&bytes) != hash {
            return Err(DeployError::Corrupt(format!("{key} (digest)")));
        }
        crate::assets::Index::parse(&bytes)
            .map(Arc::new)
            .map_err(|_| DeployError::Corrupt(key))
    }

    pub(crate) async fn asset_content(
        &self,
        hash: &str,
    ) -> object_store::Result<object_store::GetResult> {
        self.store
            .get(&self.path(&format!("_assets/content/{hash}")))
            .await
    }

    /// A version's access rules and configuration (digests checked),
    /// without its components.
    async fn settings(
        &self,
        version: &str,
        manifest: &Manifest,
    ) -> Result<(AccessRules, BTreeMap<String, Vars>), DeployError> {
        let mut access = AccessRules::default();
        for (class, spec) in &manifest.access {
            access.replace(class, spec).map_err(|_| {
                DeployError::Corrupt(format!("{version}/manifest (access {class})"))
            })?;
        }
        let mut config = BTreeMap::new();
        for (class, digest) in &manifest.config {
            let key = format!("{version}/{class}.config");
            let (bytes, _) = self
                .get(&key)
                .await?
                .ok_or_else(|| DeployError::Corrupt(format!("{key} (missing)")))?;
            if hex(&Sha256::digest(&bytes)) != *digest {
                return Err(DeployError::Corrupt(format!("{key} (digest)")));
            }
            let vars =
                parse_config(&bytes).map_err(|e| DeployError::Corrupt(format!("{key}:{e}")))?;
            config.insert(class.clone(), vars);
        }
        Ok((access, config))
    }
}

/// The third token of `current` once the app's state is under its root
/// (ADR 0022); a 0.8.x `cluster migrate` wrote it too.
pub const ROOTED_TOKEN: &str = wasmkeep_cluster::legacy_layout::ROOTED_TOKEN;

/// What a deploy or rollback made current, and what changed.
#[derive(Debug, Clone)]
pub struct Deployed {
    /// Content and index objects uploaded or repaired, excluding the marker.
    pub repaired: usize,
    /// What `current` now points at.
    pub current: Current,
    /// The version `current` pointed at before (rolled back from, for a
    /// rollback); `None` for an app's first deploy.
    pub previous: Option<String>,
    /// Nothing changed: the version asked for is the current one, so no
    /// version was written and `current` (its `seq` too) was left as it
    /// was.
    pub unchanged: bool,
    /// Against the version before.
    pub diff: Diff,
    /// This change set the marker on an app deployed before it existed:
    /// its nodes take their rules from deploys from now on, and ignore
    /// `--access`. Never for a new app's first deploy (nothing changes
    /// for it).
    pub newly_marked: bool,
}

/// The `storage` functions a component imports that a node of 0.4.1 or
/// 0.4.2 may lack (ADR 0034, section 15). TypeScript and Python components
/// import the whole interface, called or not; Rust, Go, C and Zig ones
/// only what they call.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StorageImports {
    /// `storage.flush`: a node announcing `flush`.
    pub flush: bool,
    /// `storage.delete-self-durable`: a node announcing `ends`.
    pub delete_self_durable: bool,
    /// `wasmkeep:actor/blobs`, any function of it: a node announcing
    /// `blobs` (ADR 0030, section 11).
    pub blobs: bool,
}

impl StorageImports {
    /// Read from the component's type, as the front's link check reads its
    /// exports: the functions of its `wasmkeep:actor/storage` import, at
    /// any version of the package.
    #[must_use]
    pub fn of(runtime: &Runtime, component: &wasmtime::component::Component) -> Self {
        use wasmtime::component::types::ComponentItem;
        let engine = runtime.engine();
        let mut found = Self::default();
        for (name, item) in component.component_type().imports(engine) {
            let ComponentItem::ComponentInstance(instance) = item.ty else {
                continue;
            };
            if name.starts_with("wasmkeep:actor/blobs@") {
                found.blobs = true;
                continue;
            }
            if !name.starts_with("wasmkeep:actor/storage@") {
                continue;
            }
            for (function, _) in instance.exports(engine) {
                match function {
                    "flush" => found.flush = true,
                    "delete-self-durable" => found.delete_self_durable = true,
                    _ => {}
                }
            }
        }
        found
    }
}

/// An error with its causes, for a message.
fn error_chain(e: &dyn std::error::Error) -> String {
    let mut out = e.to_string();
    let mut source = e.source();
    while let Some(cause) = source {
        out.push_str(": ");
        out.push_str(&cause.to_string());
        source = cause.source();
    }
    out
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
