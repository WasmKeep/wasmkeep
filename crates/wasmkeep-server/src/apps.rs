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

use crate::ActorService;

mod assets_deploy;
#[cfg(test)]
mod assets_tests;
mod collect;
mod delete;
use crate::access::AccessRules;
use crate::actor::FRONT_CLASS as FRONT;
use crate::env::Vars;
pub use collect::KEEP_PREVIOUS;
pub use delete::fence_app_nodes;
pub use delete::{DeleteApp, LATE_WRITES, delete_app};

/// A local copy of an app marker (`.recreated`, `.restored`): `None` only
/// when the file is missing. Any other error (EMFILE under socket load,
/// EIO, EACCES) says nothing about the app: an error, so the caller retries
/// or fails, never stops the app or removes its local files as if the
/// marker did not match.
fn read_local_marker(path: &std::path::Path) -> Result<Option<Vec<u8>>, DeployError> {
    match std::fs::read(path) {
        Ok(bytes) => Ok(Some(bytes)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(DeployError::Local(format!(
            "reading {}: {e}",
            path.display()
        ))),
    }
}

/// Deploy checks in a row that cannot read this node's local marker files
/// before the node reports itself not ready.
const LOCAL_FAILURES_BEHIND: u32 = 3;

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
    /// state in the layout before 0.4.0 (ADR 0022): run `cluster migrate`.
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

/// Longest wait between attempts after a failed update.
const FOLLOW_BACKOFF_MAX: Duration = Duration::from_secs(300);

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

    /// What a first deploy checks before it creates the app: the names'
    /// grammar (apps that exist keep theirs). Other apps of the tenant on
    /// the prefix are fine: each app's actors live under its own state
    /// root (ADR 0022).
    async fn check_new(&self) -> Result<(), DeployError> {
        for (what, name) in [("tenant", &self.tenant), ("app", &self.app)] {
            if !wasmkeep_runtime::is_identifier(name) {
                return Err(DeployError::Name {
                    what,
                    name: name.clone(),
                });
            }
        }
        Ok(())
    }

    /// What a deploy checks before it writes (ADR 0022): no migration in
    /// progress, and the tenant's actor state not left in the layout
    /// before 0.4.0 (a `current` written now is rooted, token 3).
    async fn check_layout(&self) -> Result<(), DeployError> {
        let keys = wasmkeep_cluster::object_store::ObjectStoreBucket::new(
            Arc::clone(&self.store),
            self.prefix.clone(),
        );
        wasmkeep_cluster::migrate::start_check(&keys, &self.tenant)
            .await
            .map(drop)
            .map_err(|e| match e {
                wasmkeep_cluster::migrate::StartRefusal::Bucket(e) => bucket(e),
                e => DeployError::Layout(e.to_string()),
            })
    }

    /// Live nodes of this app whose heartbeat does not announce the
    /// capability word `front` (ADR 0023): they cannot read a manifest of
    /// format 3, nor a secrets set with a `_front` entry. Live as `cluster
    /// status` counts it (the default horizon, this machine's clock).
    ///
    /// # Errors
    /// The bucket did not answer.
    pub async fn nodes_without_front(&self) -> Result<Vec<String>, DeployError> {
        nodes_without_front(
            Arc::clone(&self.store),
            &self.prefix,
            &self.tenant,
            &self.app,
        )
        .await
    }

    /// Refuses `manifest` while a live node could not read it: format 5
    /// needs `flush` (ADR 0034), and every format inherits the checks of
    /// those below it (4 needs `ends`, 3 needs `front`).
    async fn check_readers(&self, manifest: &Manifest) -> Result<(), DeployError> {
        if manifest.assets.is_some() {
            let nodes = live_nodes(
                Arc::clone(&self.store),
                &self.prefix,
                &self.tenant,
                &self.app,
            )
            .await?;
            let lacking: Vec<_> = nodes
                .iter()
                .filter(|n| !n.can_assets)
                .map(|n| n.id.as_str())
                .collect();
            if !lacking.is_empty() {
                return Err(DeployError::NodesWithoutAssets {
                    app: self.name(),
                    nodes: lacking.join(", "),
                });
            }
        }
        if manifest.format_number() >= 6 {
            let nodes = live_nodes(
                Arc::clone(&self.store),
                &self.prefix,
                &self.tenant,
                &self.app,
            )
            .await?;
            let lacking: Vec<_> = nodes
                .iter()
                .filter(|n| !n.can_rate_limits)
                .map(|n| n.id.as_str())
                .collect();
            if !lacking.is_empty() {
                return Err(DeployError::Refused(format!(
                    "manifest format 6 needs rate_limits on all live readers: {}",
                    lacking.join(", ")
                )));
            }
        }
        if manifest.format_number() >= 5 {
            let nodes = nodes_without_flush(
                Arc::clone(&self.store),
                &self.prefix,
                &self.tenant,
                &self.app,
            )
            .await?;
            if !nodes.is_empty() {
                return Err(DeployError::NodesWithoutFlush {
                    app: format!("{}/{}", self.tenant, self.app),
                    nodes: nodes.join(", "),
                    what: "a version with a batched class (manifest format 5) needs".into(),
                });
            }
        }
        if manifest.format_number() >= 4 {
            let nodes = nodes_without_ends(
                Arc::clone(&self.store),
                &self.prefix,
                &self.tenant,
                &self.app,
            )
            .await?;
            if !nodes.is_empty() {
                return Err(DeployError::NodesWithoutEnds {
                    app: format!("{}/{}", self.tenant, self.app),
                    nodes: nodes.join(", "),
                });
            }
        }
        if manifest.format_number() < 3 {
            return Ok(());
        }
        let nodes = self.nodes_without_front().await?;
        if nodes.is_empty() {
            Ok(())
        } else {
            Err(DeployError::NodesWithoutFront {
                app: format!("{}/{}", self.tenant, self.app),
                nodes: nodes.join(", "),
            })
        }
    }

    /// Refuses a version whose components import a `storage` function a
    /// live node of the app lacks (ADR 0034, section 15): `flush` needs the
    /// word `flush`, `delete-self-durable` the word `ends` (for strict
    /// classes too: section 1, exception 2). Such a node could not link the
    /// component and would stay not ready on the version. `given` are the
    /// components a deploy was given, already read; `kept`, `(version,
    /// class)`, those it keeps or a rollback brings back, read from the
    /// bucket only when a live node lacks a word.
    async fn check_imports(
        &self,
        runtime: Option<&Runtime>,
        given: &[(String, StorageImports)],
        kept: &[(String, String)],
    ) -> Result<(), DeployError> {
        let needs_any = given
            .iter()
            .any(|(_, i)| i.flush || i.delete_self_durable || i.blobs);
        if !needs_any && kept.is_empty() {
            return Ok(());
        }
        let nodes = live_nodes(
            Arc::clone(&self.store),
            &self.prefix,
            &self.tenant,
            &self.app,
        )
        .await?;
        if nodes
            .iter()
            .all(|n| n.can_flush && n.can_end && n.can_blobs)
        {
            return Ok(());
        }
        let mut imports = given.to_vec();
        if !kept.is_empty() {
            let own;
            let runtime = match runtime {
                Some(r) => r,
                None => {
                    own = Runtime::new(&wasmkeep_runtime::RuntimeConfig::default())
                        .map_err(|e| DeployError::Bucket(format!("checking imports: {e:#}")))?;
                    &own
                }
            };
            for (version, class) in kept {
                let key = format!("{version}/{class}.wasm");
                let (bytes, _) = self
                    .get(&key)
                    .await?
                    .ok_or_else(|| DeployError::Corrupt(format!("{key} (missing)")))?;
                let component = runtime
                    .compile(&bytes)
                    .map_err(|e| DeployError::Invalid(class.clone(), format!("{e:#}")))?;
                imports.push((class.clone(), StorageImports::of(runtime, &component)));
            }
        }
        let classes = |pick: fn(&StorageImports) -> bool| -> Vec<&str> {
            imports
                .iter()
                .filter(|(_, i)| pick(i))
                .map(|(c, _)| c.as_str())
                .collect()
        };
        let (flush, ends) = (classes(|i| i.flush), classes(|i| i.delete_self_durable));
        let blobs = classes(|i| i.blobs);
        let app = format!("{}/{}", self.tenant, self.app);
        let lacking = |has: fn(&wasmkeep_cluster::membership::NodeInfo) -> bool| {
            nodes
                .iter()
                .filter(|n| !has(n))
                .map(|n| n.id.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        };
        let what = |classes: &[&str]| {
            if classes == [FRONT] {
                return "the front imports".to_owned();
            }
            let plural = if classes.len() == 1 { "" } else { "es" };
            format!(
                "class{plural} {} import{}",
                classes.join(", "),
                if classes.len() == 1 { "s" } else { "" }
            )
        };
        if !flush.is_empty() {
            let without = lacking(|n| n.can_flush);
            if !without.is_empty() {
                return Err(DeployError::NodesWithoutFlush {
                    app,
                    nodes: without,
                    what: what(&flush),
                });
            }
        }
        if !blobs.is_empty() {
            let without = lacking(|n| n.can_blobs);
            if !without.is_empty() {
                return Err(DeployError::NodesWithoutBlobs {
                    app,
                    nodes: without,
                    what: what(&blobs),
                });
            }
        }
        if !ends.is_empty() {
            let without = lacking(|n| n.can_end);
            if !without.is_empty() {
                return Err(DeployError::NodesWithoutDeleteDurable {
                    app,
                    nodes: without,
                    what: what(&ends),
                });
            }
        }
        Ok(())
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

    /// Points `current` at `version`, conditionally on what was read.
    async fn swap_current(
        &self,
        read: Option<UpdateVersion>,
        version: &str,
        seq: u64,
    ) -> Result<(), DeployError> {
        let mode = match read {
            None => PutMode::Create,
            Some(v) => PutMode::Update(v),
        };
        let opts = PutOptions {
            mode,
            ..PutOptions::default()
        };
        // Rooted (ADR 0022), which implies marked.
        let body = Current {
            version: version.to_owned(),
            seq,
            marked: true,
            rooted: true,
        }
        .format();
        let body = PutPayload::from(body.into_bytes());
        match self.store.put_opts(&self.path("current"), body, opts).await {
            Ok(_) => Ok(()),
            Err(
                object_store::Error::AlreadyExists { .. }
                | object_store::Error::Precondition { .. },
            ) => Err(DeployError::Conflict),
            Err(e) => Err(bucket(e)),
        }
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

    /// Validates the components, uploads them and makes them current, with
    /// no rules or configuration. Deploying what is current changes nothing.
    pub async fn deploy(
        &self,
        runtime: &Runtime,
        classes: Vec<(String, Vec<u8>)>,
    ) -> Result<Current, DeployError> {
        self.deploy_version(runtime, classes, &AccessRules::default(), BTreeMap::new())
            .await
    }

    /// Deploys exactly this version: these classes, rules and
    /// configuration (stored in the clear: no secrets), and no required
    /// names (`requires`: use [`Self::deploy_changes`]). Rules and
    /// configuration must name classes of the version; a class with no
    /// values has no configuration. [`Self::deploy_changes`] is what
    /// `wasmkeep deploy` uses.
    pub async fn deploy_version(
        &self,
        runtime: &Runtime,
        classes: Vec<(String, Vec<u8>)>,
        access: &AccessRules,
        config: BTreeMap<String, Vars>,
    ) -> Result<Current, DeployError> {
        // Exactly this version: no front either.
        let changes = Changes {
            classes: Some(classes),
            front: FrontChange::Remove,
            ..Changes::default()
        };
        let set = Settings {
            access: access.clone(),
            config,
        };
        Ok(self
            .write(runtime, changes, Some(set), &mut |_| true)
            .await?
            .current)
    }

    /// `wasmkeep deploy` (ADR 0013): starting from the current version,
    /// replaces the classes if given (else keeps them, copied inside the
    /// bucket), sets the rules and configuration named, and keeps the rest.
    /// Reports what changed. Deploying what is current changes nothing.
    pub async fn deploy_changes(
        &self,
        runtime: &Runtime,
        changes: Changes,
    ) -> Result<Deployed, DeployError> {
        self.write(runtime, changes, None, &mut |_| true).await
    }

    /// [`Self::deploy_changes`], showing `plan` what the deploy changes
    /// before anything is uploaded or switched (ADR 0026: the deploy says
    /// what it removes before it switches). `plan` returning `false` stops
    /// the deploy there with [`DeployError::Stopped`]. A deploy that
    /// changes nothing does not call it.
    pub async fn deploy_changes_with(
        &self,
        runtime: &Runtime,
        changes: Changes,
        mut plan: impl FnMut(&Diff) -> bool + Send,
    ) -> Result<Deployed, DeployError> {
        self.write(runtime, changes, None, &mut plan).await
    }

    /// Builds the new version on the current one and makes it current.
    /// `exact`: the version's whole rules and configuration (else the
    /// current ones, with `changes` applied).
    async fn write(
        &self,
        runtime: &Runtime,
        changes: Changes,
        exact: Option<Settings>,
        plan: &mut (dyn FnMut(&Diff) -> bool + Send),
    ) -> Result<Deployed, DeployError> {
        self.check_not_deleted().await?;
        self.check_layout().await?;
        let every_rule = changes.every_rule;
        // What the components given import of `storage` beyond 0.4.1's
        // (ADR 0034, section 15): checked against the live nodes below.
        let mut imports = Vec::new();
        // The classes kept from the current version: read only if a live
        // node lacks a word.
        let mut kept = Vec::new();
        if let Some(classes) = &changes.classes {
            let mut compiled = Vec::new();
            for (name, bytes) in classes {
                let component = runtime
                    .compile(bytes)
                    .map_err(|e| DeployError::Invalid(name.clone(), format!("{e:#}")))?;
                compiled.push((name.clone(), component));
            }
            ActorService::validate(runtime, &compiled)
                .map_err(|(name, e)| DeployError::Invalid(name, e))?;
            for (name, component) in &compiled {
                imports.push((name.clone(), StorageImports::of(runtime, component)));
            }
        }
        // The front is checked before anything is written too (ADR 0023).
        if let FrontChange::Set(bytes) = &changes.front {
            let component = runtime
                .compile(bytes)
                .map_err(|e| DeployError::Front(format!("{e:#}")))?;
            crate::front::validate(runtime, &component)
                .await
                .map_err(|e| DeployError::Front(error_chain(&e)))?;
            // A front that keeps files needs `blobs` as a class does.
            imports.push((FRONT.to_owned(), StorageImports::of(runtime, &component)));
        }

        let read = self.read_current().await?;
        if read.is_none() {
            self.check_new().await?;
        }
        let base = match &read {
            Some((current, _)) => {
                let manifest = self.manifest(&current.version).await?;
                // Kept from it unless the whole set is given, so it must be
                // readable then: a deploy never drops rules it could not read.
                let settings = match self.settings(&current.version, &manifest).await {
                    Ok((access, config)) => Settings { access, config },
                    Err(_) if exact.is_some() => Settings::default(),
                    Err(e) => return Err(e),
                };
                Some((current.clone(), manifest, settings))
            }
            None => None,
        };

        let assets = match &changes.assets {
            crate::assets::Change::Keep => base.as_ref().and_then(|(_, m, _)| m.assets.clone()),
            crate::assets::Change::Remove => None,
            crate::assets::Change::Set(bundle) => {
                bundle.index.validate().map_err(DeployError::Refused)?;
                for file in bundle.index.files.values() {
                    for c in file.contents() {
                        let bytes = bundle.contents.get(&c.digest).ok_or_else(|| {
                            DeployError::Refused(format!("missing local asset {}", c.digest))
                        })?;
                        if bytes.len() as u64 != c.size || crate::assets::digest(bytes) != c.digest
                        {
                            return Err(DeployError::Refused(format!(
                                "invalid local asset {}",
                                c.digest
                            )));
                        }
                    }
                }
                let hash = crate::assets::digest(&bundle.index.bytes());
                // An unchanged index can repair itself even when its stored copy
                // is lost. Compare immutable names when the old index survives.
                if let Some(old_hash) = base.as_ref().and_then(|(_, m, _)| m.assets.as_ref())
                    && old_hash != &hash
                    && !bundle.index.immutable.is_empty()
                {
                    let old = self.read_assets_index_if_present(old_hash).await?;
                    if old.is_none() {
                        eprintln!(
                            "warning: old assets index {old_hash} is missing; immutable names cannot be compared; deploying supplied --assets"
                        );
                    }
                    for (name, file) in &bundle.index.files {
                        if bundle.index.immutable.iter().any(|p| name.starts_with(p))
                            && old
                                .as_ref()
                                .and_then(|old| old.files.get(name))
                                .is_some_and(|f| f.identity.digest != file.identity.digest)
                        {
                            return Err(DeployError::Refused(format!(
                                "immutable asset {name:?} changed contents"
                            )));
                        }
                    }
                }
                Some(hash)
            }
        };
        // The front, and what to write for it: given, kept (copied inside
        // the bucket, as classes are), or removed.
        let mut uploads = Vec::new();
        let mut copies = Vec::new();
        let front: Option<String> = match changes.front {
            FrontChange::Set(bytes) => {
                let digest = hex(&Sha256::digest(&bytes));
                uploads.push((format!("{FRONT}.wasm"), bytes));
                Some(digest)
            }
            FrontChange::Keep => match &base {
                Some((current, manifest, _)) => manifest.front.clone().inspect(|_| {
                    copies.push((current.version.clone(), format!("{FRONT}.wasm")));
                    kept.push((current.version.clone(), FRONT.to_owned()));
                }),
                None => None,
            },
            FrontChange::Remove => None,
        };
        let has_front = front.is_some();
        // Front values for a version without a front: a mistake, not a no-op.
        let gives_values = changes
            .front_config
            .as_ref()
            .is_some_and(|c| c.as_ref().is_some_and(|v| !v.is_empty()))
            || changes
                .front_requires
                .as_ref()
                .is_some_and(|n| !n.is_empty());
        if gives_values && !has_front {
            return Err(DeployError::Front(
                "front configuration or required names for a version without a front: \
                 give the front (--front PATH)"
                    .into(),
            ));
        }
        // Removing them without a front: nothing to remove.
        let (front_config, front_requires) = if has_front {
            (changes.front_config, changes.front_requires)
        } else {
            (None, None)
        };

        // The class table, and what to write for it.
        let table: BTreeMap<String, String> = match changes.classes {
            Some(classes) => classes
                .into_iter()
                .map(|(name, bytes)| {
                    let digest = hex(&Sha256::digest(&bytes));
                    uploads.push((format!("{name}.wasm"), bytes));
                    (name, digest)
                })
                .collect(),
            None => match &base {
                Some((current, manifest, _)) => {
                    for class in manifest.classes.keys() {
                        copies.push((current.version.clone(), format!("{class}.wasm")));
                        kept.push((current.version.clone(), class.clone()));
                    }
                    manifest.classes.clone()
                }
                // A first deploy may give a front alone (ADR 0023).
                None if has_front || assets.is_some() => BTreeMap::new(),
                None => {
                    return Err(DeployError::Nothing(
                        "nothing is deployed yet: give the classes (--actor) or a front (--front)",
                    ));
                }
            },
        };
        if table.is_empty()
            && !has_front
            && assets.is_none()
            && base.as_ref().is_some_and(|(_, m, _)| m.front.is_some())
        {
            return Err(DeployError::Nothing(
                "removing the front would leave a version with no class and no front",
            ));
        }
        // What may hold values: the classes, and the front's scope while
        // the version has a front.
        let scoped = |c: &str| table.contains_key(c) || (has_front && c == FRONT);

        // Rules and values: the whole set given, or the current ones kept
        // for the classes still there (and the front's while it stays),
        // with the changes applied.
        let keep_requires = exact.is_none();
        let mut set = match exact {
            Some(set) => set,
            None => {
                let mut set = base.as_ref().map(|(_, _, s)| s.clone()).unwrap_or_default();
                set.access.retain(|c| table.contains_key(c));
                set.config.retain(|c, _| scoped(c));
                set
            }
        };
        for (class, spec) in &changes.access {
            if !table.contains_key(class) {
                return Err(DeployError::Invalid(
                    class.clone(),
                    "an access rule for a class this version does not have".into(),
                ));
            }
            set.access
                .replace(class, spec)
                .map_err(|e| DeployError::Invalid(class.clone(), e))?;
        }
        let front_config = front_config.map(|v| (FRONT.to_owned(), v));
        for (class, vars) in changes.config.into_iter().chain(front_config) {
            if !scoped(&class) {
                return Err(DeployError::Invalid(
                    class,
                    "configuration for a class this version does not have".into(),
                ));
            }
            match vars {
                Some(vars) if !vars.is_empty() => {
                    set.config.insert(class, vars);
                }
                _ => {
                    set.config.remove(&class);
                }
            }
        }

        // Required names: the current ones kept for the classes still
        // there and the front while it stays (unless the whole set is
        // given), with the changes applied.
        let mut requires: BTreeMap<String, std::collections::BTreeSet<String>> =
            match (keep_requires, &base) {
                (true, Some((_, m, _))) => m
                    .requires
                    .iter()
                    .filter(|(c, _)| scoped(c))
                    .map(|(c, n)| (c.clone(), n.clone()))
                    .collect(),
                _ => BTreeMap::new(),
            };
        let front_requires = front_requires.map(|n| (FRONT.to_owned(), n));
        for (class, names) in changes.requires.into_iter().chain(front_requires) {
            if !scoped(&class) {
                return Err(DeployError::Invalid(
                    class,
                    "required names for a class this version does not have".into(),
                ));
            }
            if let Some(bad) = names.iter().find(|n| !crate::env::valid_name(n)) {
                return Err(DeployError::Invalid(class, format!("invalid name {bad:?}")));
            }
            if names.is_empty() {
                requires.remove(&class);
            } else {
                requires.insert(class, names);
            }
        }
        // Idle limits (ADR 0028): kept for the classes still there unless
        // the whole set is given, with the changes applied.
        let mut expire: BTreeMap<String, u64> = match (keep_requires, &base) {
            (true, Some((_, m, _))) => m
                .expire
                .iter()
                .filter(|(c, _)| table.contains_key(*c))
                .map(|(c, s)| (c.clone(), *s))
                .collect(),
            _ => BTreeMap::new(),
        };
        for (class, secs) in changes.expire {
            if !table.contains_key(&class) {
                return Err(DeployError::Invalid(
                    class,
                    "an idle limit for a class this version does not have".into(),
                ));
            }
            match secs {
                Some(secs)
                    if !(wasmkeep_cluster::expiry::MIN_IDLE_SECS
                        ..=wasmkeep_cluster::expiry::MAX_IDLE_SECS)
                        .contains(&secs) =>
                {
                    return Err(DeployError::Invalid(
                        class,
                        "an idle limit must be between 1h and 36500d".into(),
                    ));
                }
                Some(secs) => {
                    expire.insert(class, secs);
                }
                None => {
                    expire.remove(&class);
                }
            }
        }
        // How plain writes reach the bucket (ADR 0034): kept for the
        // classes still there unless the whole set is given, with the
        // changes applied. `strict` is the absence of a mode.
        let mut batched: std::collections::BTreeSet<String> = match (keep_requires, &base) {
            (true, Some((_, m, _))) => m
                .batched
                .iter()
                .filter(|c| table.contains_key(*c))
                .cloned()
                .collect(),
            _ => std::collections::BTreeSet::new(),
        };
        for (class, writes) in changes.writes {
            if !table.contains_key(&class) {
                return Err(DeployError::Invalid(
                    class,
                    "a mode of writes for a class this version does not have".into(),
                ));
            }
            match writes {
                Writes::Strict => {
                    batched.remove(&class);
                }
                Writes::Batched => {
                    batched.insert(class);
                }
            }
        }
        let mut manifest = Manifest {
            assets,
            previous: None,
            classes: table,
            front,
            access: BTreeMap::new(),
            config: BTreeMap::new(),
            requires,
            expire,
            batched,
            limits: changes.limits.clone().unwrap_or_else(|| {
                base.as_ref()
                    .map(|(_, m, _)| m.limits.clone())
                    .unwrap_or_default()
            }),
        };
        limits::validate(&manifest.limits).map_err(DeployError::Refused)?;
        for (class, _) in set.access.restricted() {
            if !manifest.classes.contains_key(&class) {
                return Err(DeployError::Invalid(
                    class,
                    "an access rule for a class this version does not have".into(),
                ));
            }
            if let Some(spec) = set.access.spec(&class) {
                manifest.access.insert(class, spec);
            }
        }
        // Every class's rule written, `open` included (ADR 0026): format 3.
        if every_rule {
            for class in manifest.classes.keys() {
                if !manifest.access.contains_key(class) {
                    manifest.access.insert(class.clone(), "open".to_owned());
                }
            }
        }
        // Stored as parsed, not as given: no comment (or commented-out
        // secret) is uploaded, and reformatting makes no new version.
        for (class, vars) in &set.config {
            if !manifest.classes.contains_key(class) && !(has_front && class == FRONT) {
                return Err(DeployError::Invalid(
                    class.clone(),
                    "configuration for a class this version does not have".into(),
                ));
            }
            if vars.is_empty() {
                continue;
            }
            let bytes = format_config(vars).into_bytes();
            if bytes.len() as u64 > crate::env::MAX_FILE {
                return Err(DeployError::Invalid(
                    class.clone(),
                    format!("configuration larger than {} bytes", crate::env::MAX_FILE),
                ));
            }
            manifest
                .config
                .insert(class.clone(), hex(&Sha256::digest(&bytes)));
            uploads.push((format!("{class}.config"), bytes));
        }

        let diff = Diff::between(base.as_ref().map(|(_, m, s)| (m, s)), (&manifest, &set));
        let unchanged = base
            .as_ref()
            .is_some_and(|(_, m, _)| m.identity() == manifest.identity());
        if let Some((current, _, _)) = &base {
            manifest.previous = Some(current.version.clone());
        }
        if !unchanged {
            self.check_readers(&manifest).await?;
            self.check_imports(Some(runtime), &imports, &kept).await?;
        }
        if !unchanged && !plan(&diff) {
            return Err(DeployError::Stopped);
        }
        let supplied = match &changes.assets {
            crate::assets::Change::Set(b) => Some(b),
            _ => None,
        };
        let has_assets = manifest.assets.is_some();
        let publish = |marker: Option<assets_deploy::Marker>| async move {
            let repaired = if let (Some(hash), Some(marker)) = (&manifest.assets, &marker) {
                self.prepare_assets(hash, supplied, changes.assets_verify, marker)
                    .await?
            } else {
                0
            };
            if unchanged {
                let current = base
                    .as_ref()
                    .map(|(c, _, _)| c.clone())
                    .ok_or(DeployError::Nothing("no current version"))?;
                return Ok(Deployed {
                    previous: Some(current.version.clone()),
                    current,
                    unchanged: true,
                    diff,
                    newly_marked: false,
                    repaired,
                });
            }
            let version = manifest.version_id();
            // News only for an app deployed before rules came with deploys (a
            // `current` without the marker): a new app has had them from its
            // first deploy.
            let from_before = read.as_ref().is_some_and(|(c, _)| !c.marked);
            // Every deploy of this version writes token 3 (ADR 0022), which
            // marks the app (ADR 0013): its nodes take their rules from
            // deploys, and refuse `--access`.
            let marked = true;
            for (key, bytes) in uploads {
                self.put_once(&format!("{version}/{key}"), bytes).await?;
            }
            // Server-side copies: unconditional, but the destination key is
            // content-addressed, so an overwrite writes the same bytes. Nodes
            // check every digest when they load the version.
            for (from, key) in copies {
                self.store
                    .copy(
                        &self.path(&format!("{from}/{key}")),
                        &self.path(&format!("{version}/{key}")),
                    )
                    .await
                    .map_err(bucket)?;
            }
            if let Some(marker) = &marker {
                marker.check()?;
            }
            let text = manifest.format();
            self.put_once(&format!("{version}/manifest"), text.clone().into_bytes())
                .await?;
            // A directory of this id may be half deleted by garbage collection
            // (ADR 0012) while this wrote it: never point `current` at one
            // whose manifest is not there, as written.
            match self.get(&format!("{version}/manifest")).await? {
                Some((bytes, _)) if bytes == text.as_bytes() => {}
                _ => {
                    return Err(DeployError::Bucket(format!(
                        "{version}/manifest changed while deploying; retry"
                    )));
                }
            }
            let seq = read.as_ref().map_or(1, |(c, _)| c.seq + 1);
            let previous = read.as_ref().map(|(c, _)| c.version.clone());
            if let Some(marker) = &marker {
                marker.check()?;
                marker.switching(&version, seq);
            }
            self.swap_current(read.map(|(_, v)| v), &version, seq)
                .await?;
            // Deleted while this deployed (ADR 0028): what it wrote is left to
            // `app delete`, which removes everything but the tombstone.
            if self.tombstone().await?.is_some() {
                return Err(DeployError::Refused(format!(
                    "{} was deleted while this deployed: what the deploy wrote stays in the \
                 bucket until `wasmkeep app delete` is run again",
                    self.name()
                )));
            }
            Ok(Deployed {
                repaired,
                current: Current {
                    version,
                    seq,
                    marked,
                    rooted: true,
                },
                previous,
                unchanged: false,
                diff,
                newly_marked: marked && from_before,
            })
        };
        if has_assets {
            self.with_assets_marker(|marker| publish(Some(marker)))
                .await
        } else {
            publish(None).await
        }
    }

    /// Makes the version deployed before the current one current again, with
    /// a new, higher `seq`, and its rules and configuration. Refused if that
    /// makes a class more open, unless `allow_reopen` (ADR 0013).
    pub async fn rollback(&self, allow_reopen: bool) -> Result<Deployed, DeployError> {
        self.check_not_deleted().await?;
        self.check_layout().await?;
        let (current, read) = self
            .read_current()
            .await?
            .ok_or(DeployError::Nothing("nothing is deployed"))?;
        let manifest = self.manifest(&current.version).await?;
        let previous = manifest
            .previous
            .clone()
            .ok_or(DeployError::Nothing("no previous version to roll back to"))?;
        let back = self.manifest(&previous).await?;
        if let Some(hash) = &back.assets {
            self.read_assets_index(hash).await?;
        }
        let (access, config) = self.settings(&current.version, &manifest).await?;
        let now = Settings { access, config };
        let (access, config) = self.settings(&previous, &back).await?;
        let then = Settings { access, config };
        let diff = Diff::between(Some((&manifest, &now)), (&back, &then));
        if !allow_reopen && !diff.reopened.is_empty() {
            return Err(DeployError::Reopens(diff.reopened.join(", ")));
        }
        // Back to a version with a front: its readers are checked as a
        // deploy writing it checks them, and its components' imports too.
        self.check_readers(&back).await?;
        let mut kept: Vec<(String, String)> = back
            .classes
            .keys()
            .map(|c| (previous.clone(), c.clone()))
            .collect();
        if back.front.is_some() {
            kept.push((previous.clone(), FRONT.to_owned()));
        }
        self.check_imports(None, &[], &kept).await?;
        // Rooted, so marked (ADR 0022).
        let marked = true;
        let seq = current.seq + 1;
        self.swap_current(Some(read), &previous, seq).await?;
        // Deleted while this rolled back (ADR 0028), as a deploy checks. A
        // rollback is not idempotent: run again after a failed check, it
        // would go one more version back, so the check is retried, and a
        // failure says the switch happened.
        let mut deleted = self.tombstone().await;
        for wait in [250, 500] {
            if deleted.is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(wait)).await;
            deleted = self.tombstone().await;
        }
        match deleted {
            Ok(None) => {}
            Ok(Some(_)) => {
                return Err(DeployError::Refused(format!(
                    "{} was deleted while this rolled back: the `current` it wrote stays in the \
                     bucket until `wasmkeep app delete` is run again",
                    self.name()
                )));
            }
            Err(e) => {
                return Err(DeployError::Bucket(format!(
                    "{}: `current` was switched to {previous} (seq {seq}); only the check for a \
                     delete made meanwhile failed ({e}): do not run the rollback again (it would \
                     go one more version back): check whether the app was deleted",
                    self.name()
                )));
            }
        }
        Ok(Deployed {
            repaired: 0,
            current: Current {
                version: previous,
                seq,
                marked,
                rooted: true,
            },
            previous: Some(current.version),
            unchanged: false,
            diff,
            newly_marked: marked && !current.marked,
        })
    }

    /// Keeps `service` on the deployed version: checks `current` every
    /// `every` (or at once when the service asks, see
    /// [`ActorService::deploy_seq`]), and swaps classes in at event
    /// boundaries when it changes. Runs until the service is dropped.
    pub async fn follow(self, service: ActorService, every: Duration) {
        // No actor is acquired until a check for the app's tombstone made
        // while this node's lease was valid finds none (ADR 0028, section
        // 5): a node whose lease is written after `app delete` read the
        // live nodes sees the tombstone, one written before is seen live.
        // The same after every lapse of the lease (a node paused or cut off
        // while the app was deleted, and maybe created again).
        service.expect_app_check();
        if !self.app_check(&service).await {
            return;
        }
        let wake = service.deploy_wakeup();
        // After a failure (a version this node cannot build, a bucket
        // outage), wait longer each time, wake-ups included: refused
        // actors would otherwise have it download and recompile in a loop.
        // The tombstone is still checked every `every` meanwhile (a few
        // small reads): a deleted app stops being served within a period
        // or two, not at the end of a backoff that reaches minutes.
        let mut backoff: Option<Duration> = None;
        // Checks in a row that could not read this node's own marker files.
        let mut local_failures = 0_u32;
        loop {
            match backoff {
                Some(wait) => {
                    if !self.wait_watching_tombstone(&service, wait, every).await {
                        return;
                    }
                }
                None => tokio::select! {
                    () = tokio::time::sleep(every) => {}
                    () = wake.notified() => {}
                },
            }
            if service.app_unchecked() && !self.app_check(&service).await {
                return;
            }
            match self.update(&service).await {
                Ok(_) => {
                    backoff = None;
                    local_failures = 0;
                    service.set_deploy_behind(false);
                }
                Err(DeployError::Deleted(_)) => {
                    service.stop_for_deleted_app().await;
                    return;
                }
                Err(e) => {
                    // Read but not applicable: this node would decide under
                    // rules the app no longer has. A bucket outage is every
                    // node's, and keeps them ready.
                    if matches!(
                        e,
                        DeployError::Invalid(..) | DeployError::Front(_) | DeployError::Corrupt(_)
                    ) {
                        service.set_deploy_behind(true);
                    }
                    // This node's own files, unlike a bucket outage: once it
                    // lasts, the node no longer follows deploys, so it is not
                    // ready (and ready again at the next good check).
                    if matches!(e, DeployError::Local(_)) {
                        local_failures += 1;
                        if local_failures >= LOCAL_FAILURES_BEHIND {
                            service.set_deploy_behind(true);
                        }
                    } else {
                        local_failures = 0;
                    }
                    let wait = backoff.map_or(every, |w| (w * 2).min(FOLLOW_BACKOFF_MAX));
                    tracing::warn!(error = %e, retry_in_s = wait.as_secs(), "checking for a new deploy failed");
                    backoff = Some(wait);
                }
            }
        }
    }

    /// Waits `wait` (a deploy check's backoff), checking every `every`
    /// whether the app is still this node's ([`Self::still_ours`]):
    /// `false` once it is not, and this node stopped serving it. A check
    /// that fails (the bucket unreachable) is left to the next one.
    async fn wait_watching_tombstone(
        &self,
        service: &ActorService,
        wait: Duration,
        every: Duration,
    ) -> bool {
        let until = tokio::time::Instant::now() + wait;
        loop {
            let left = until.saturating_duration_since(tokio::time::Instant::now());
            if left.is_zero() {
                return true;
            }
            tokio::time::sleep(left.min(every)).await;
            if left <= every {
                return true;
            }
            if matches!(self.still_ours(service.local_app_dir()).await, Ok(false)) {
                service.stop_for_deleted_app().await;
                return false;
            }
        }
    }

    /// Until a check sent while the lease is valid settles it: `true` once
    /// the app has no tombstone and was not created again since this node
    /// prepared its local files (acquisitions may go on); `false` once this
    /// node stopped serving it.
    async fn app_check(&self, service: &ActorService) -> bool {
        loop {
            if !service.lease_valid_now() {
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
            match self.still_ours(service.local_app_dir()).await {
                Ok(true) => {
                    service.app_checked();
                    return true;
                }
                Ok(false) => {
                    service.stop_for_deleted_app().await;
                    return false;
                }
                Err(e) => {
                    tracing::warn!(error = %e, "checking whether the app was deleted failed; retrying");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }
    }

    /// Whether this node may go on serving the app with the local files in
    /// `local`: no tombstone, and no `--recreate` since it prepared them
    /// ([`Self::prepare_local`] wrote the marker it compares).
    async fn still_ours(&self, local: &std::path::Path) -> Result<bool, DeployError> {
        if !start_allowed(
            self.tombstone().await?.is_some(),
            self.get("restoring").await?.is_some(),
        ) {
            return Ok(false);
        }
        for (remote, file) in [(RECREATED, RECREATED_LOCAL), ("restored", ".restored")] {
            let Some((marker, _)) = self.get(remote).await? else {
                continue;
            };
            let ours = read_local_marker(&local.join(file))?;
            if !marker_matches(Some(marker.as_slice()), ours.as_deref()) {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// One check of `current`; swaps classes in if it changed.
    pub async fn update(&self, service: &ActorService) -> Result<bool, DeployError> {
        // The app deleted (ADR 0028, section 5): checked at every version
        // check, before `current`.
        if !self.still_ours(service.local_app_dir()).await? {
            return Err(DeployError::Deleted(format!("{} (restored)", self.name())));
        }
        let Some(current) = self.current().await? else {
            return Ok(false);
        };
        if current.seq <= service.deploy_seq() {
            return Ok(false);
        }
        let manifest = match self.manifest(&current.version).await {
            Ok(manifest) => manifest,
            Err(error) => {
                if matches!(error, DeployError::Corrupt(_)) {
                    service.set_rate_limits_pending(true);
                }
                return Err(error);
            }
        };
        // Remember decoded policies before fetching any components. A bucket
        // outage alone is not evidence of a policy; a decoded policy is.
        service.set_rate_limits_pending(!manifest.limits.is_empty());
        let Version {
            assets,
            classes,
            access,
            config,
            requires,
            expire,
            batched,
            limits,
            front,
        } = self
            .version_from_manifest(&current.version, &manifest)
            .await?;
        // Everything compiled first, then swapped in at once with the
        // sequence, the rules and the values: a node that cannot build a
        // version keeps the previous one whole, and no event sees part of
        // a deploy. Compiled on a blocking thread: a component embedding
        // a JavaScript engine takes seconds, which on a runtime worker
        // would hold back events and lease renewals queued behind it. This
        // follower waits for it, so the swap still comes after it.
        let runtime = service.runtime().clone();
        let app = self.name();
        let (compiled, front) =
            tokio::task::spawn_blocking(move || compile_version(&runtime, &app, classes, front))
                .await
                .map_err(|e| DeployError::Bucket(format!("compiling the version: {e}")))??;
        service
            .apply_deployed_assets(
                assets,
                current.seq,
                &compiled,
                &service.effective_rules(current.marked, &access),
                &config,
                &requires,
                &batched,
                front.as_ref(),
                &limits,
            )
            .map_err(|(name, e)| {
                if name == FRONT {
                    DeployError::Front(error_chain(&e))
                } else {
                    DeployError::Invalid(name, e.to_string())
                }
            })?;
        service.set_expiry(&expire);
        tracing::info!(version = %current.version, seq = current.seq, "now running the deployed version");
        Ok(true)
    }
}

/// A followed version's classes and front, compiled (on a blocking
/// thread: [`AppRef::update`]).
#[allow(
    clippy::type_complexity,
    reason = "the classes as `apply_deployed` takes them"
)]
fn compile_version(
    runtime: &Runtime,
    app: &str,
    classes: Vec<(String, Vec<u8>)>,
    front: Option<VersionFront>,
) -> Result<
    (
        Vec<(String, wasmtime::component::Component, Vec<u8>)>,
        Option<crate::front::DeployedFront>,
    ),
    DeployError,
> {
    slow_compile::wait(app);
    let mut compiled = Vec::with_capacity(classes.len());
    for (name, bytes) in classes {
        let component = runtime
            .compile(&bytes)
            .map_err(|e| DeployError::Invalid(name.clone(), format!("{e:#}")))?;
        compiled.push((name, component, bytes));
    }
    let front = match front {
        Some(f) => Some(crate::front::DeployedFront {
            component: runtime
                .compile(&f.wasm)
                .map_err(|e| DeployError::Front(format!("{e:#}")))?,
            config: f.config,
            requires: f.requires,
        }),
        None => None,
    };
    Ok((compiled, front))
}

/// A test's slow compile: the deploy follower of an app named here takes
/// this much longer to compile each version, on the thread that compiles,
/// as a component embedding a JavaScript engine does.
mod slow_compile {
    use std::collections::BTreeMap;
    use std::sync::Mutex;
    use std::time::Duration;

    static SLOW: Mutex<BTreeMap<String, Duration>> = Mutex::new(BTreeMap::new());

    pub(super) fn wait(app: &str) {
        let by = SLOW
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(app)
            .copied();
        if let Some(by) = by {
            std::thread::sleep(by);
        }
    }

    pub(super) struct Slow(pub(super) String);

    impl Drop for Slow {
        fn drop(&mut self) {
            SLOW.lock()
                .unwrap_or_else(|p| p.into_inner())
                .remove(&self.0);
        }
    }

    pub(super) fn set(app: String, by: Duration) -> Slow {
        SLOW.lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(app.clone(), by);
        Slow(app)
    }
}

/// Test hook: until the guard drops, the deploy follower of `app`
/// (`tenant/app`) takes `by` longer to compile each version it follows.
#[doc(hidden)]
pub fn slow_follower_compiles(app: &str, by: std::time::Duration) -> impl Drop + Send {
    slow_compile::set(app.to_owned(), by)
}

/// The third token of `current` once the app's state is under its root
/// (ADR 0022); `wasmkeep_cluster::migrate` writes it too.
pub const ROOTED_TOKEN: &str = wasmkeep_cluster::migrate::ROOTED_TOKEN;

/// What `current` points at.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Current {
    /// Content-derived version id.
    pub version: String,
    /// Deploy sequence number; increases with every change, rollbacks too.
    pub seq: u64,
    /// A version with access rules or configuration has been current
    /// (ADR 0013): written as a third field, which tools and nodes from
    /// before refuse, and kept by every later deploy and rollback.
    pub marked: bool,
    /// The app's actor state lives under its own root (ADR 0022): token
    /// `3`, which implies `marked` and which versions before 0.4.0 cannot
    /// read, so their `serve --app` and `deploy` stop. Written by every
    /// deploy and rollback of this version, and by `cluster migrate`.
    pub rooted: bool,
}

impl Current {
    /// Reads the body of `apps/<tenant>/<app>/current`:
    /// `"<version> <seq>"`, `"<version> <seq> 2"` once marked, or
    /// `"<version> <seq> 3"` once rooted (and so marked). `None` if it is
    /// anything else.
    pub fn parse(body: &[u8]) -> Option<Self> {
        let text = String::from_utf8_lossy(body);
        let mut parts = text.split_whitespace();
        let (Some(version), Some(seq), marked, None) = (
            parts.next(),
            parts.next().and_then(|s| s.parse().ok()),
            parts.next(),
            parts.next(),
        ) else {
            return None;
        };
        let (marked, rooted) = match marked {
            None => (false, false),
            Some("2") => (true, false),
            Some(ROOTED_TOKEN) => (true, true),
            Some(_) => return None,
        };
        Some(Self {
            version: version.to_owned(),
            seq,
            marked,
            rooted,
        })
    }

    /// The stored form.
    pub fn format(&self) -> String {
        let Self {
            version,
            seq,
            marked,
            rooted,
        } = self;
        if *rooted {
            format!("{version} {seq} {ROOTED_TOKEN}\n")
        } else if *marked {
            format!("{version} {seq} 2\n")
        } else {
            format!("{version} {seq}\n")
        }
    }
}

/// What `wasmkeep deploy` changes (ADR 0013).
#[derive(Default)]
pub struct Changes {
    /// Keep, replace, or remove the static assets index.
    pub assets: crate::assets::Change,
    /// Read contents back as well as checking their sizes.
    pub assets_verify: bool,
    /// The new class table; `None` keeps the current classes.
    pub classes: Option<Vec<(String, Vec<u8>)>>,
    /// `(class, spec)`: sets the class's rule (`open` removes it).
    pub access: Vec<(String, String)>,
    /// `(class, values)`: replaces the class's configuration (`None`, or
    /// no values, removes it).
    pub config: Vec<(String, Option<Vars>)>,
    /// `(class, names)`: replaces the names the class requires among its
    /// values (ADR 0016); empty removes them.
    pub requires: Vec<(String, std::collections::BTreeSet<String>)>,
    /// `(class, limit)`: sets how long the class's actors may stay idle
    /// before they are deleted, in seconds (ADR 0028); `None` removes it.
    pub expire: Vec<(String, Option<u64>)>,
    /// `(class, mode)`: sets how the class's plain writes reach the bucket
    /// (ADR 0034). Classes not named keep theirs.
    pub writes: Vec<(String, Writes)>,
    /// None preserves policies; Some replaces the complete policy set.
    pub limits: Option<limits::Policies>,
    /// The front (ADR 0023): kept by default.
    pub front: FrontChange,
    /// The front's configuration (`--front-config`): `None` keeps it,
    /// `Some(None)` (or no values) removes it.
    pub front_config: Option<Option<Vars>>,
    /// The names the front requires (`--front-requires`): `None` keeps
    /// them, an empty set removes them.
    pub front_requires: Option<std::collections::BTreeSet<String>>,
    /// Write every class's rule in the manifest, `open` included (a deploy
    /// from the project file, ADR 0026); else `open` is written as
    /// absence, as flag deploys write it.
    pub every_rule: bool,
}

/// A part of a whole version (a deploy from the project file, ADR 0026):
/// its component, configuration and required names.
pub struct Part {
    pub wasm: Vec<u8>,
    /// `None` removes the part's configuration.
    pub config: Option<Vars>,
    /// Empty removes them.
    pub requires: std::collections::BTreeSet<String>,
    /// A class's idle limit in seconds (ADR 0028); `None`: never. Ignored
    /// for the front.
    pub expire: Option<u64>,
    /// How a class's plain writes reach the bucket (ADR 0034). Ignored for
    /// the front.
    pub writes: Writes,
}

impl std::fmt::Debug for Part {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Part")
            .field("wasm", &format_args!("{} bytes", self.wasm.len()))
            .field(
                "config",
                &self.config.as_ref().map(|v| v.keys().collect::<Vec<_>>()),
            )
            .field("requires", &self.requires)
            .field("expire", &self.expire)
            .field("writes", &self.writes)
            .finish()
    }
}

impl Changes {
    /// The whole version, as a project file gives it (ADR 0026): exactly
    /// these classes (`(name, rule, part)`, every rule written, `open`
    /// included), values and required names removed where a part has
    /// none, the front set or removed.
    #[must_use]
    pub fn whole(front: Option<Part>, classes: Vec<(String, String, Part)>) -> Self {
        let mut changes = Self {
            classes: Some(Vec::new()),
            every_rule: true,
            assets: crate::assets::Change::Remove,
            limits: Some(Default::default()),
            ..Self::default()
        };
        for (name, access, part) in classes {
            changes.access.push((name.clone(), access));
            changes.config.push((name.clone(), part.config));
            changes.requires.push((name.clone(), part.requires));
            changes.expire.push((name.clone(), part.expire));
            changes.writes.push((name.clone(), part.writes));
            if let Some(table) = changes.classes.as_mut() {
                table.push((name, part.wasm));
            }
        }
        match front {
            Some(part) => {
                changes.front = FrontChange::Set(part.wasm);
                changes.front_config = Some(part.config);
                changes.front_requires = Some(part.requires);
            }
            None => changes.front = FrontChange::Remove,
        }
        changes
    }
}

/// What a deploy does with the front (ADR 0023).
#[derive(Default)]
pub enum FrontChange {
    /// The current front, if any, copied inside the bucket.
    #[default]
    Keep,
    /// This component (`--front PATH`).
    Set(Vec<u8>),
    /// None (`--no-front`): its configuration and required names go too.
    Remove,
}

impl std::fmt::Debug for FrontChange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Keep => f.write_str("Keep"),
            Self::Set(bytes) => write!(f, "Set({} bytes)", bytes.len()),
            Self::Remove => f.write_str("Remove"),
        }
    }
}

impl std::fmt::Debug for Changes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let classes: Option<Vec<&str>> = self
            .classes
            .as_ref()
            .map(|c| c.iter().map(|(n, _)| n.as_str()).collect());
        let config: Vec<(&str, Option<Vec<&str>>)> = self
            .config
            .iter()
            .map(|(c, v)| {
                (
                    c.as_str(),
                    v.as_ref().map(|v| v.keys().map(String::as_str).collect()),
                )
            })
            .collect();
        f.debug_struct("Changes")
            .field("classes", &classes)
            .field("access", &self.access)
            .field("config", &config)
            .field("front", &self.front)
            .field("every_rule", &self.every_rule)
            .finish_non_exhaustive()
    }
}

/// A version's rules and configuration.
#[derive(Clone, Default)]
struct Settings {
    access: AccessRules,
    config: BTreeMap<String, Vars>,
}

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

/// What changes between two versions: classes, rules, and the names (never
/// the values) of configuration.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Diff {
    /// Assets added, changed, removed, or kept.
    pub assets: Option<&'static str>,
    /// Classes added.
    pub added: Vec<String>,
    /// Each added class's rule (`open` when it has none): an added class
    /// has a rule, never a change of rule.
    pub added_rules: BTreeMap<String, String>,
    /// Classes removed, with their rule if it was not `open`.
    pub removed: Vec<(String, Option<String>)>,
    /// Classes whose code changed.
    pub changed: Vec<String>,
    /// `(class, before, after)` for each kept class whose rule changes
    /// (`open` when it has none). An added class's rule is in
    /// `added_rules`.
    pub rules: Vec<(String, String, String)>,
    /// `"class: NAME added"` and the like, names only.
    pub config: Vec<String>,
    /// Kept classes the change makes more open, as `class (before -> after)`.
    pub reopened: Vec<String>,
    /// `"class: NAME now required"` and the like (ADR 0016).
    pub requires: Vec<String>,
    /// What happens to the front (ADR 0023): `added`, `changed`,
    /// `removed` or `unchanged`; `None` when neither version has one.
    pub front: Option<&'static str>,
    /// With a front after the change: the classes whose rule is `open`,
    /// reachable from outside past it.
    pub open_past_front: Vec<String>,
    /// `(class, written)`: kept classes that stay `open` but whose rule is
    /// now written in the version (`true`, a deploy from the project file,
    /// ADR 0026) or no longer written (`false`, a flag deploy after one).
    /// The rule does not change; the version does.
    pub open_written: Vec<(String, bool)>,
    /// `"class: expire after idle never -> 7d"` and the like (ADR 0028).
    pub expire: Vec<String>,
    /// `"class: strict -> batched"` and the like: a class's mode of writes
    /// changed, or an added class's when it is not strict (ADR 0034,
    /// section 15).
    pub writes: Vec<String>,
    /// Policy additions, changes and removals, without client values.
    pub limits: Vec<String>,
}

impl Diff {
    /// The rule of `class`, added by this change: `open` when it has none.
    #[must_use]
    pub fn added_rule(&self, class: &str) -> &str {
        self.added_rules.get(class).map_or("open", String::as_str)
    }

    fn between(before: Option<(&Manifest, &Settings)>, after: (&Manifest, &Settings)) -> Self {
        let empty_manifest = Manifest {
            assets: None,
            previous: None,
            classes: BTreeMap::new(),
            front: None,
            access: BTreeMap::new(),
            config: BTreeMap::new(),
            requires: BTreeMap::new(),
            expire: BTreeMap::new(),
            batched: std::collections::BTreeSet::new(),
            limits: Default::default(),
        };
        let empty = Settings::default();
        let (bm, bs) = before.unwrap_or((&empty_manifest, &empty));
        let (am, as_) = after;
        let mut diff = Self::default();
        for name in bm
            .limits
            .keys()
            .chain(am.limits.keys())
            .collect::<std::collections::BTreeSet<_>>()
        {
            let change = match (bm.limits.get(name), am.limits.get(name)) {
                (None, Some(_)) => "added",
                (Some(_), None) => "removed",
                (Some(a), Some(b)) if a != b => "changed",
                _ => continue,
            };
            diff.limits.push(format!("{name}: {change}"));
        }
        for (class, digest) in &am.classes {
            match bm.classes.get(class) {
                None => diff.added.push(class.clone()),
                Some(d) if d != digest => diff.changed.push(class.clone()),
                Some(_) => {}
            }
        }
        for class in bm.classes.keys() {
            if !am.classes.contains_key(class) {
                diff.removed.push((class.clone(), bs.access.spec(class)));
                for name in bm.requires.get(class).into_iter().flatten() {
                    diff.requires.push(format!(
                        "{class}: {name} no longer required (class removed)"
                    ));
                }
            }
        }
        diff.assets = match (&bm.assets, &am.assets) {
            (None, None) => None,
            (None, Some(_)) => Some("added"),
            (Some(_), None) => Some("removed"),
            (Some(b), Some(a)) if b != a => Some("changed"),
            _ => Some("unchanged"),
        };
        diff.front = match (&bm.front, &am.front) {
            (None, None) => None,
            (None, Some(_)) => Some("added"),
            (Some(_), None) => Some("removed"),
            (Some(b), Some(a)) if a != b => Some("changed"),
            (Some(_), Some(_)) => Some("unchanged"),
        };
        if am.front.is_some() {
            diff.open_past_front = am
                .classes
                .keys()
                .filter(|c| as_.access.of(c) == crate::access::Access::Open)
                .cloned()
                .collect();
        }
        for class in am.classes.keys().filter(|c| bm.classes.contains_key(*c)) {
            let (b, a) = (bm.access.get(class), am.access.get(class));
            match (b.map(String::as_str), a.map(String::as_str)) {
                (None, Some("open")) => diff.open_written.push((class.clone(), true)),
                (Some("open"), None) => diff.open_written.push((class.clone(), false)),
                _ => {}
            }
        }
        let open = || "open".to_owned();
        // The classes, and the front's scope when either version has a
        // front: it has values and required names, never a rule.
        let front_scope = (am.front.is_some() || bm.front.is_some()).then(|| FRONT.to_owned());
        for class in am.classes.keys().chain(front_scope.as_ref()) {
            let (b, a) = (bs.access.spec(class), as_.access.spec(class));
            if !bm.classes.contains_key(class) && class != FRONT {
                diff.added_rules
                    .insert(class.clone(), a.clone().unwrap_or_else(open));
            } else if b != a && class != FRONT {
                let (b, a) = (b.unwrap_or_else(open), a.unwrap_or_else(open));
                if more_open(&bs.access, &as_.access, class) {
                    diff.reopened.push(format!("{class} ({b} -> {a})"));
                }
                diff.rules.push((class.clone(), b, a));
            }
            let none = Vars::new();
            let (b, a) = (
                bs.config.get(class).unwrap_or(&none),
                as_.config.get(class).unwrap_or(&none),
            );
            for (name, value) in a {
                match b.get(name) {
                    None => diff.config.push(format!("{class}: {name} added")),
                    Some(v) if v != value => diff.config.push(format!("{class}: {name} changed")),
                    Some(_) => {}
                }
            }
            for name in b.keys() {
                if !a.contains_key(name) {
                    diff.config.push(format!("{class}: {name} removed"));
                }
            }
            let none = std::collections::BTreeSet::new();
            let (b, a) = (
                bm.requires.get(class).unwrap_or(&none),
                am.requires.get(class).unwrap_or(&none),
            );
            for name in a.difference(b) {
                diff.requires.push(format!("{class}: {name} now required"));
            }
            for name in b.difference(a) {
                diff.requires
                    .push(format!("{class}: {name} no longer required"));
            }
            let idle = |s: Option<&u64>| {
                s.map_or_else(
                    || "never".to_owned(),
                    |s| wasmkeep_cluster::expiry::format_idle(*s),
                )
            };
            let mode = |m: &Manifest| {
                if m.batched.contains(class) {
                    Writes::Batched
                } else {
                    Writes::Strict
                }
            };
            let (wb, wa) = (mode(bm), mode(am));
            if class != FRONT
                && (wb != wa || (wa == Writes::Batched && !bm.classes.contains_key(class)))
            {
                diff.writes.push(if bm.classes.contains_key(class) {
                    format!("{class}: {} -> {}", wb.as_str(), wa.as_str())
                } else {
                    format!("{class}: {}", wa.as_str())
                });
            }
            let (b, a) = (bm.expire.get(class), am.expire.get(class));
            if class != FRONT && a != b && (a.is_some() || bm.classes.contains_key(class)) {
                diff.expire.push(format!(
                    "{class}: expire after idle {} -> {}",
                    idle(b),
                    idle(a)
                ));
            }
        }
        diff
    }

    /// One line per change, for `wasmkeep deploy` to print.
    #[must_use]
    pub fn lines(&self) -> Vec<String> {
        let mut out = Vec::new();
        out.extend(self.front.map(|f| format!("front: {f}")));
        out.extend(
            self.added
                .iter()
                .map(|c| format!("class {c}: added ({})", self.added_rule(c))),
        );
        out.extend(self.changed.iter().map(|c| format!("class {c}: new code")));
        out.extend(self.removed.iter().map(|(c, rule)| match rule {
            Some(rule) => format!("class {c}: removed (it was {rule})"),
            None => format!("class {c}: removed"),
        }));
        out.extend(
            self.rules
                .iter()
                .map(|(c, b, a)| format!("access {c}: {b} -> {a}")),
        );
        out.extend(self.open_written.iter().map(|(c, written)| {
            if *written {
                format!("access {c}: open, now written in the version")
            } else {
                format!("access {c}: open, no longer written in the version (the same rule)")
            }
        }));
        out.extend(self.limits.iter().map(|l| format!("limit {l}")));
        out.extend(self.config.iter().map(|l| format!("config {l}")));
        out.extend(self.requires.iter().map(|l| format!("requires {l}")));
        out.extend(self.expire.iter().cloned());
        out.extend(self.writes.iter().map(|l| format!("writes {l}")));
        out.extend(
            self.open_past_front.iter().map(|c| {
                format!("class {c}: open, reachable at /actor/{c}/<name>, past the front")
            }),
        );
        out
    }
}

/// Whether `class` is more open under `after` than under `before`: a lower
/// mode (`internal` > `existing` > `open`), or more names an `existing`
/// class lets outside requests create.
fn more_open(before: &AccessRules, after: &AccessRules, class: &str) -> bool {
    use crate::access::Access;
    let rank = |a: Access| match a {
        Access::Open => 0,
        Access::Existing => 1,
        Access::Internal => 2,
    };
    let (b, a) = (before.of(class), after.of(class));
    if rank(a) != rank(b) {
        return rank(a) < rank(b);
    }
    if a == Access::Existing {
        let names = |r: &AccessRules| -> Vec<String> {
            r.spec(class)
                .and_then(|s| {
                    s.split_once(':')
                        .map(|(_, n)| n.split(',').map(str::to_owned).collect())
                })
                .unwrap_or_default()
        };
        let (bn, an) = (names(before), names(after));
        return an.iter().any(|n| !bn.contains(n));
    }
    false
}

/// What a node runs for one version (ADR 0013). Its `Debug` shows names
/// and sizes, never a configuration value.
#[derive(Clone)]
pub struct Version {
    /// Verified static assets of this version.
    pub assets: Option<crate::assets::Deployed>,
    /// Each class's component bytes.
    pub classes: Vec<(String, Vec<u8>)>,
    /// The version's access rules (classes not listed are open).
    pub access: AccessRules,
    /// Each class's plain configuration (classes not listed have none).
    pub config: BTreeMap<String, Vars>,
    /// Each class's required names (ADR 0016; classes not listed require
    /// none).
    pub requires: BTreeMap<String, std::collections::BTreeSet<String>>,
    /// Each class's idle limit in seconds (ADR 0028; classes not listed
    /// never expire).
    pub expire: BTreeMap<String, u64>,
    /// The classes whose writes are batched (ADR 0034; every other class
    /// is strict).
    pub batched: std::collections::BTreeSet<String>,
    /// Node-local ingress policies (ADR 0036).
    pub limits: limits::Policies,
    /// The version's front (ADR 0023), if it has one. Its values are not
    /// in `config` and `requires`, which hold classes only.
    pub front: Option<VersionFront>,
}

/// A version's front as the bucket holds it. Its `Debug` shows sizes and
/// names, never a value.
#[derive(Clone)]
pub struct VersionFront {
    /// The component bytes, checked against the manifest's digest.
    pub wasm: Vec<u8>,
    /// Its plain configuration (`_front.config`).
    pub config: Vars,
    /// The names it requires among its values.
    pub requires: std::collections::BTreeSet<String>,
}

impl std::fmt::Debug for Version {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let classes: Vec<(&str, usize)> = self
            .classes
            .iter()
            .map(|(n, b)| (n.as_str(), b.len()))
            .collect();
        let config: Vec<(&str, Vec<&str>)> = self
            .config
            .iter()
            .map(|(c, v)| (c.as_str(), v.keys().map(String::as_str).collect()))
            .collect();
        f.debug_struct("Version")
            .field("classes", &classes)
            .field("access", &self.access)
            .field("config", &config)
            .field(
                "front",
                &self.front.as_ref().map(|f| {
                    (
                        f.wasm.len(),
                        f.config.keys().map(String::as_str).collect::<Vec<_>>(),
                    )
                }),
            )
            .finish()
    }
}

impl std::fmt::Debug for VersionFront {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VersionFront")
            .field("wasm", &self.wasm.len())
            .field("config", &self.config.keys().collect::<Vec<_>>())
            .field("requires", &self.requires)
            .finish()
    }
}

/// A `.config` object's text: one `NAME='value'` line per name, sorted.
/// [`crate::env::parse`] reads it back unchanged: it removes one pair of
/// quotes and interprets nothing inside, and values have no line breaks.
#[must_use]
pub fn format_config(vars: &Vars) -> String {
    vars.iter().map(|(n, v)| format!("{n}='{v}'\n")).collect()
}

/// Parses a `.config` object: an `--env` file's format, at most
/// [`crate::env::MAX_FILE`] bytes. The error is `<line>: <problem>`, never
/// a value.
fn parse_config(bytes: &[u8]) -> Result<Vars, String> {
    if bytes.len() as u64 > crate::env::MAX_FILE {
        return Err(format!(" larger than {} bytes", crate::env::MAX_FILE));
    }
    let text = std::str::from_utf8(bytes).map_err(|_| " not UTF-8".to_owned())?;
    crate::env::parse(text)
}

/// A version's manifest: its classes, their access rules and
/// configuration digests, and the version before it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Manifest {
    /// SHA-256 of the canonical static assets index (format 7).
    pub assets: Option<String>,
    /// The version that was current when this one was deployed.
    pub previous: Option<String>,
    /// Class name -> SHA-256 of its component.
    pub classes: BTreeMap<String, String>,
    /// SHA-256 of the front's component (`front main <sha256>`, ADR 0023):
    /// written in format 3 only, so a version without a front keeps its
    /// bytes and id.
    pub front: Option<String>,
    /// Class name -> its access rule in canonical form
    /// ([`AccessRules::spec`]); open classes are not listed.
    pub access: BTreeMap<String, String>,
    /// Class name (or `_front`, with a front) -> SHA-256 of its `.config`
    /// object.
    pub config: BTreeMap<String, String>,
    /// Class name (or `_front`, with a front) -> the names it requires among its values (ADR 0016):
    /// a class missing one refuses its events.
    pub requires: BTreeMap<String, std::collections::BTreeSet<String>>,
    /// Class name -> how long its actors may stay idle before they are
    /// deleted, in seconds (`expire <class> <secs>`, ADR 0028): written in
    /// format 4 only, so a version without one keeps its bytes and id.
    pub expire: BTreeMap<String, u64>,
    /// Classes whose writes are batched (`writes <class> batched`, ADR
    /// 0034, section 15): written in format 5 only. `strict`, the default,
    /// is written as absence, said or not, so a version of strict classes
    /// keeps its format, bytes and id.
    pub batched: std::collections::BTreeSet<String>,
    /// Node-local ingress policies (ADR 0036).
    pub limits: limits::Policies,
}

impl Manifest {
    fn table(classes: &BTreeMap<String, String>) -> String {
        classes
            .iter()
            .map(|(n, d)| format!("class {n} {d}\n"))
            .collect()
    }

    /// The lines that make the version: classes, then rules, then
    /// configuration digests. Without rules or configuration, exactly the
    /// class table, so versions deployed before ADR 0013 keep their id.
    fn identity(&self) -> String {
        let mut out = Self::table(&self.classes);
        if let Some(digest) = &self.front {
            out.push_str(&format!("front main {digest}\n"));
        }
        if let Some(digest) = &self.assets {
            out.push_str(&format!("assets {digest}\n"));
        }
        for (class, spec) in &self.access {
            out.push_str(&format!("access {class} {spec}\n"));
        }
        for (class, digest) in &self.config {
            out.push_str(&format!("config {class} {digest}\n"));
        }
        for (class, names) in &self.requires {
            for name in names {
                out.push_str(&format!("requires {class} {name}\n"));
            }
        }
        for (class, secs) in &self.expire {
            out.push_str(&format!("expire {class} {secs}\n"));
        }
        for class in &self.batched {
            out.push_str(&format!("writes {class} batched\n"));
        }
        for (name, policy) in &self.limits {
            out.push_str(&policy.line(name));
        }
        out
    }

    /// The version id: the identity and the version it replaced, so a
    /// directory is never reused under another lineage (ADR 0013).
    #[must_use]
    pub fn version_id(&self) -> String {
        let mut text = self.identity();
        if let Some(p) = &self.previous {
            text.push_str(&format!("previous {p}\n"));
        }
        hex(&Sha256::digest(text.as_bytes()))[..16].to_owned()
    }

    /// 6 with ingress policies (ADR 0036); 5 with a batched class (ADR
    /// 0034); 4 with an idle limit (ADR
    /// 0028); 3 with a front, or an explicit `open` rule (ADR 0023); else 1
    /// when there are neither rules nor configuration (the form from before
    /// ADR 0013, kept as the minimal one), else 2.
    #[must_use]
    pub fn format_number(&self) -> u8 {
        if self.assets.is_some() {
            7
        } else if !self.limits.is_empty() {
            6
        } else if !self.batched.is_empty() {
            5
        } else if !self.expire.is_empty() {
            4
        } else if self.front.is_some() || self.access.values().any(|s| s == "open") {
            3
        } else if self.access.is_empty() && self.config.is_empty() && self.requires.is_empty() {
            1
        } else {
            2
        }
    }

    fn format(&self) -> String {
        let mut out = format!("wasmkeep-manifest {}\n", self.format_number());
        if let Some(p) = &self.previous {
            out.push_str(&format!("previous {p}\n"));
        }
        out.push_str(&self.identity());
        out
    }

    /// Decode a deploy manifest without contacting or mutating the app.
    pub fn parse(text: &str) -> Option<Self> {
        let mut lines = text.lines();
        let format = match lines.next()? {
            "wasmkeep-manifest 1" => 1,
            "wasmkeep-manifest 2" => 2,
            "wasmkeep-manifest 3" => 3,
            "wasmkeep-manifest 4" => 4,
            "wasmkeep-manifest 5" => 5,
            "wasmkeep-manifest 6" => 6,
            "wasmkeep-manifest 7" => 7,
            _ => return None,
        };
        let mut manifest = Self {
            assets: None,
            previous: None,
            classes: BTreeMap::new(),
            front: None,
            access: BTreeMap::new(),
            config: BTreeMap::new(),
            requires: BTreeMap::new(),
            expire: BTreeMap::new(),
            batched: std::collections::BTreeSet::new(),
            limits: Default::default(),
        };
        for line in lines {
            if line.starts_with("limit ") {
                if format < 6 {
                    return None;
                }
                let (name, policy) = limits::Policy::from_line(line)?;
                if manifest.limits.insert(name, policy).is_some() {
                    return None;
                }
                limits::validate(&manifest.limits).ok()?;
                continue;
            }
            // `access` keeps the rest of the line: names may hold spaces.
            if format >= 2
                && let Some(rest) = line.strip_prefix("access ")
            {
                let (class, spec) = rest.split_once(' ')?;
                // Format 3 also says `open` (ADR 0023), which earlier
                // formats write as absence.
                if format >= 3 && spec == "open" && !class.is_empty() {
                    manifest.access.insert(class.to_owned(), spec.to_owned());
                    continue;
                }
                let mut check = AccessRules::default();
                check.replace(class, spec).ok()?;
                if class.is_empty() || check.spec(class).as_deref() != Some(spec) {
                    return None; // open, or not canonical
                }
                manifest.access.insert(class.to_owned(), spec.to_owned());
                continue;
            }
            let mut parts = line.split_whitespace();
            match (parts.next(), parts.next(), parts.next(), parts.next()) {
                (Some("assets"), Some(d), None, None)
                    if format == 7
                        && manifest.assets.is_none()
                        && crate::assets::valid_digest(d) =>
                {
                    manifest.assets = Some(d.into());
                }
                (Some("previous"), Some(v), None, None) => manifest.previous = Some(v.to_owned()),
                // `_front` is never a class (no class of that name ever
                // linked, in any format): it names the front.
                (Some("class"), Some(n), Some(d), None) if n != FRONT => {
                    manifest.classes.insert(n.to_owned(), d.to_owned());
                }
                // At most one front, named `main`.
                (Some("front"), Some("main"), Some(d), None)
                    if format >= 3 && manifest.front.is_none() =>
                {
                    manifest.front = Some(d.to_owned());
                }
                (Some("config"), Some(n), Some(d), None) if format >= 2 => {
                    manifest.config.insert(n.to_owned(), d.to_owned());
                }
                // A class's idle limit (ADR 0028), once per class.
                (Some("expire"), Some(n), Some(secs), None)
                    if format >= 4 && !manifest.expire.contains_key(n) =>
                {
                    let secs: u64 = secs.parse().ok()?;
                    if !(wasmkeep_cluster::expiry::MIN_IDLE_SECS
                        ..=wasmkeep_cluster::expiry::MAX_IDLE_SECS)
                        .contains(&secs)
                    {
                        return None;
                    }
                    manifest.expire.insert(n.to_owned(), secs);
                }
                // A batched class (ADR 0034), once per class. `strict` is
                // never written: it is the absence of the line.
                (Some("writes"), Some(n), Some("batched"), None)
                    if format >= 5 && !manifest.batched.contains(n) =>
                {
                    manifest.batched.insert(n.to_owned());
                }
                (Some("requires"), Some(n), Some(name), None)
                    if format >= 2 && crate::env::valid_name(name) =>
                {
                    manifest
                        .requires
                        .entry(n.to_owned())
                        .or_default()
                        .insert(name.to_owned());
                }
                _ => return None,
            }
        }
        if format == 7 && manifest.assets.is_none() {
            return None;
        }
        let known = |c: &String| manifest.classes.contains_key(c);
        // The front's scope holds values and required names, never a rule,
        // and only beside a `front` line.
        let scoped = |c: &String| known(c) || (manifest.front.is_some() && c == FRONT);
        if !manifest.access.keys().all(known)
            || !manifest.expire.keys().all(known)
            || !manifest.batched.iter().all(known)
            || !manifest.config.keys().all(scoped)
            || !manifest.requires.keys().all(scoped)
        {
            return None;
        }
        Some(manifest)
    }
}

/// Live nodes of `tenant/app` whose heartbeat does not announce `front`
/// (ADR 0023), under the bucket key `prefix`: what a deploy writing format
/// 3, and a secrets write leaving a `_front` entry, are refused for.
///
/// # Errors
/// The bucket did not answer.
pub async fn nodes_without_front(
    store: Arc<dyn ObjectStore>,
    prefix: &str,
    tenant: &str,
    app: &str,
) -> Result<Vec<String>, DeployError> {
    without_front(store, prefix, tenant, app, false).await
}

/// [`nodes_without_front`], among the nodes that read the app's secrets
/// from the bucket (heartbeat line 10): a node run with `--no-secrets`
/// never opens the set.
///
/// # Errors
/// The bucket did not answer.
pub async fn secrets_readers_without_front(
    store: Arc<dyn ObjectStore>,
    prefix: &str,
    tenant: &str,
    app: &str,
) -> Result<Vec<String>, DeployError> {
    without_front(store, prefix, tenant, app, true).await
}

/// Live nodes of `tenant/app` whose heartbeat does not announce `ends`
/// (ADR 0028), under the bucket key `prefix`: they cannot read a version
/// with an idle limit, and would keep serving an app deleted under them.
///
/// # Errors
/// The bucket did not answer.
pub async fn nodes_without_ends(
    store: Arc<dyn ObjectStore>,
    prefix: &str,
    tenant: &str,
    app: &str,
) -> Result<Vec<String>, DeployError> {
    Ok(live_nodes(store, prefix, tenant, app)
        .await?
        .into_iter()
        .filter(|n| !n.can_end)
        .map(|n| n.id)
        .collect())
}

/// Live nodes of `tenant/app` whose heartbeat does not announce `flush`
/// (ADR 0034, section 15), under the bucket key `prefix`: they cannot link
/// a component that imports `storage.flush`, nor read manifest format 5.
///
/// # Errors
/// The bucket did not answer.
pub async fn nodes_without_flush(
    store: Arc<dyn ObjectStore>,
    prefix: &str,
    tenant: &str,
    app: &str,
) -> Result<Vec<String>, DeployError> {
    Ok(live_nodes(store, prefix, tenant, app)
        .await?
        .into_iter()
        .filter(|n| !n.can_flush)
        .map(|n| n.id)
        .collect())
}

/// The live nodes of `tenant/app` under the bucket key `prefix`, live as
/// `cluster status` counts them (the default horizon, this machine's
/// clock).
///
/// # Errors
/// The bucket did not answer.
pub async fn live_nodes(
    store: Arc<dyn ObjectStore>,
    prefix: &str,
    tenant: &str,
    app: &str,
) -> Result<Vec<wasmkeep_cluster::membership::NodeInfo>, DeployError> {
    let prefix = if prefix.is_empty() || prefix.ends_with('/') {
        prefix.to_owned()
    } else {
        format!("{prefix}/")
    };
    let heartbeats = wasmkeep_cluster::object_store::ObjectStoreBucket::new(store, prefix);
    let horizon = crate::heartbeat_horizon_ms(
        crate::DEFAULT_HEARTBEAT,
        &crate::LeaseConfig::default_safe(),
    );
    let now = u64::try_from(crate::alarm::now_ms_i64()).unwrap_or(0);
    let served = format!("{tenant}/{app}");
    let nodes = wasmkeep_cluster::membership::all_heartbeats(&heartbeats)
        .await
        .map_err(bucket)?;
    Ok(nodes
        .into_iter()
        .filter(|n| wasmkeep_cluster::membership::is_live(n, now, horizon) && n.app == served)
        .collect())
}

async fn without_front(
    store: Arc<dyn ObjectStore>,
    prefix: &str,
    tenant: &str,
    app: &str,
    reading_secrets: bool,
) -> Result<Vec<String>, DeployError> {
    let prefix = if prefix.is_empty() || prefix.ends_with('/') {
        prefix.to_owned()
    } else {
        format!("{prefix}/")
    };
    let heartbeats = wasmkeep_cluster::object_store::ObjectStoreBucket::new(store, prefix);
    let horizon = crate::heartbeat_horizon_ms(
        crate::DEFAULT_HEARTBEAT,
        &crate::LeaseConfig::default_safe(),
    );
    let now = u64::try_from(crate::alarm::now_ms_i64()).unwrap_or(0);
    let served = format!("{tenant}/{app}");
    let nodes = wasmkeep_cluster::membership::all_heartbeats(&heartbeats)
        .await
        .map_err(bucket)?;
    Ok(nodes
        .into_iter()
        .filter(|n| wasmkeep_cluster::membership::is_live(n, now, horizon))
        .filter(|n| n.app == served && !n.can_front && (n.reads_secrets || !reading_secrets))
        .map(|n| n.id)
        .collect())
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

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)] // test code

    use super::*;

    fn two_classes() -> Manifest {
        Manifest {
            assets: None,
            previous: Some("abc".into()),
            classes: BTreeMap::from([
                ("chat".into(), "11".into()),
                ("counter".into(), "22".into()),
            ]),
            front: None,
            access: BTreeMap::new(),
            config: BTreeMap::new(),
            requires: BTreeMap::new(),
            expire: BTreeMap::new(),
            batched: std::collections::BTreeSet::new(),
            limits: Default::default(),
        }
    }

    /// An added class says its rule, and never a change from a rule it
    /// never had (`access x: open -> internal` for a class just added).
    #[test]
    fn an_added_class_says_its_rule() {
        let before = two_classes();
        let mut after = two_classes();
        for class in ["pub", "priv", "some"] {
            after.classes.insert(class.into(), "33".into());
        }
        after.access.insert("pub".into(), "open".into());
        let mut rules = AccessRules::default();
        rules.replace("priv", "internal").unwrap();
        rules.replace("some", "existing:lobby").unwrap();
        rules.replace("chat", "existing").unwrap();
        let settings = Settings {
            access: rules,
            config: BTreeMap::new(),
        };
        let diff = Diff::between(Some((&before, &Settings::default())), (&after, &settings));
        let lines = diff.lines();
        for want in [
            "class priv: added (internal)",
            "class pub: added (open)",
            "class some: added (existing:lobby)",
            "access chat: open -> existing",
        ] {
            assert!(lines.contains(&want.to_owned()), "{want}: {lines:?}");
        }
        assert!(
            !lines.iter().any(|l| l.starts_with("access priv")
                || l.starts_with("access some")
                || l.starts_with("access pub")),
            "{lines:?}"
        );
        assert_eq!(diff.added_rule("priv"), "internal");
        assert_eq!(diff.added_rule("pub"), "open");
        // A first deploy: every class is added, with its rule.
        let diff = Diff::between(None, (&after, &settings));
        assert!(
            diff.lines()
                .contains(&"class chat: added (existing)".to_owned()),
            "{:?}",
            diff.lines()
        );
        assert!(diff.rules.is_empty(), "{:?}", diff.rules);
    }

    #[test]
    fn required_names_are_part_of_the_version_and_strict() {
        let mut m = two_classes();
        m.requires.insert(
            "chat".into(),
            std::collections::BTreeSet::from(["TOKEN".into(), "KEY".into()]),
        );
        let text = m.format();
        assert!(text.starts_with("wasmkeep-manifest 2\n"), "{text}");
        assert!(
            text.contains("requires chat KEY\nrequires chat TOKEN\n"),
            "{text}"
        );
        assert_eq!(Manifest::parse(&text), Some(m.clone()));
        assert_ne!(m.version_id(), two_classes().version_id());
        for bad in ["requires chat 1BAD", "requires nope TOKEN", "requires chat"] {
            let text = format!("wasmkeep-manifest 2\nclass chat 11\n{bad}\n");
            assert_eq!(Manifest::parse(&text), None, "{bad}");
        }
        // Format 1 never carries them.
        assert_eq!(
            Manifest::parse("wasmkeep-manifest 1\nclass chat 11\nrequires chat TOKEN\n"),
            None
        );
    }

    #[test]
    fn manifests_round_trip() {
        let m = two_classes();
        assert_eq!(Manifest::parse(&m.format()), Some(m));
        assert_eq!(Manifest::parse("something else"), None);
    }

    #[test]
    fn a_manifest_without_rules_or_config_is_written_as_before() {
        let m = two_classes();
        assert_eq!(
            m.format(),
            "wasmkeep-manifest 1\nprevious abc\nclass chat 11\nclass counter 22\n"
        );
        // A first deploy without rules gets the id it had before ADR 0013
        // (computed independently: `printf ... | sha256sum`); later ones
        // add the version they replaced.
        let first = Manifest {
            assets: None,
            previous: None,
            ..m.clone()
        };
        assert_eq!(first.version_id(), "a6d0b3dd3883b630");
        assert_eq!(
            m.version_id(),
            hex(&Sha256::digest(
                b"class chat 11\nclass counter 22\nprevious abc\n"
            ))[..16]
        );
    }

    #[test]
    fn the_version_it_replaced_is_part_of_the_id() {
        let after_a = Manifest {
            assets: None,
            previous: Some("aaaa".into()),
            ..two_classes()
        };
        let after_b = Manifest {
            assets: None,
            previous: Some("bbbb".into()),
            ..two_classes()
        };
        assert_eq!(after_a.identity(), after_b.identity());
        assert_ne!(after_a.version_id(), after_b.version_id());
    }

    #[test]
    fn current_reads_the_marker_and_old_readers_would_refuse_it() {
        let plain = Current::parse(b"abc 7\n").unwrap();
        assert!(!plain.marked);
        assert_eq!(plain.format(), "abc 7\n");
        let marked = Current::parse(b"abc 8 2\n").unwrap();
        assert!(marked.marked);
        assert_eq!(marked.format(), "abc 8 2\n");
        assert!(!marked.rooted && !plain.rooted);
        // Token 3 (ADR 0022): marked, and the app's state under its root.
        let rooted = Current::parse(b"abc 9 3\n").unwrap();
        assert!(rooted.marked && rooted.rooted);
        assert_eq!(rooted.format(), "abc 9 3\n");
        assert_eq!(Current::parse(b"abc 8 4"), None);
        assert_eq!(Current::parse(b"abc 8 2 x"), None);
        assert_eq!(Current::parse(b"abc 8 3 x"), None);
        // What `current` was read with before ADR 0013: exactly two fields.
        let old = |t: &str| t.split_whitespace().count() == 2;
        assert!(old(&plain.format()) && !old(&marked.format()));
        // And before 0.4.0: two fields, or a third that is `2` only.
        let before_0_4 = |t: &str| {
            let parts: Vec<&str> = t.split_whitespace().collect();
            parts.len() == 2 || (parts.len() == 3 && parts[2] == "2")
        };
        assert!(before_0_4(&marked.format()) && !before_0_4(&rooted.format()));
    }

    #[test]
    fn config_objects_are_canonical_and_read_back_unchanged() {
        let vars = crate::env::parse(
            "# a comment\n# TOKEN=commented-out\nZ = 'last'\nA=it's\nQ=\"quoted\"\nE=\nS=' spaced '\n",
        )
        .unwrap();
        let text = format_config(&vars);
        assert_eq!(text, "A='it's'\nE=''\nQ='quoted'\nS=' spaced '\nZ='last'\n");
        assert!(!text.contains("TOKEN"));
        assert_eq!(parse_config(text.as_bytes()).unwrap(), vars);
        let odd = Vars::from([
            ("D".to_owned(), "\"both\"".to_owned()),
            ("T".to_owned(), "a\tb".to_owned()),
            ("O".to_owned(), "'".to_owned()),
        ]);
        assert_eq!(parse_config(format_config(&odd).as_bytes()).unwrap(), odd);
    }

    #[test]
    fn version_debug_shows_no_value() {
        let v = Version {
            assets: None,
            classes: vec![("c".into(), vec![0; 3])],
            access: AccessRules::default(),
            config: BTreeMap::from([(
                "c".to_owned(),
                Vars::from([("URL".to_owned(), "https://hidden.test".to_owned())]),
            )]),
            requires: BTreeMap::new(),
            expire: BTreeMap::new(),
            batched: std::collections::BTreeSet::new(),
            limits: Default::default(),
            front: None,
        };
        let shown = format!("{v:?}");
        assert!(
            shown.contains("URL") && !shown.contains("hidden"),
            "{shown}"
        );
    }

    #[test]
    fn assets_format_7_round_trip_and_legacy_identity() {
        let old = two_classes();
        let text = format!(
            "wasmkeep-manifest 7\nclass chat 11\nclass counter 22\nassets {}\n",
            "a".repeat(64)
        );
        let m = Manifest::parse(&text).expect("format 7 with assets must parse");
        assert_eq!(m.format(), text);
        assert_ne!(m.version_id(), old.version_id());
        assert!(Manifest::parse(&text.replace("manifest 7", "manifest 6")).is_none());
        assert!(
            Manifest::parse(&(text.clone() + &format!("assets {}\n", "a".repeat(64)))).is_none()
        );
        assert!(Manifest::parse("wasmkeep-manifest 7\nclass chat 11\n").is_none());
        assert_eq!(Manifest::parse(&old.format()), Some(old));
    }

    #[test]
    fn rules_and_config_make_format_2_and_a_new_version() {
        let mut m = two_classes();
        m.access
            .insert("chat".into(), "existing:main,room 1".into());
        m.config.insert("counter".into(), "33".into());
        let text = m.format();
        assert!(text.starts_with("wasmkeep-manifest 2\n"), "{text}");
        assert!(
            text.contains("\naccess chat existing:main,room 1\n"),
            "{text}"
        );
        assert!(text.contains("\nconfig counter 33\n"), "{text}");
        assert_eq!(Manifest::parse(&text), Some(m.clone()));
        assert_ne!(m.version_id(), two_classes().version_id());
        let mut config_only = two_classes();
        config_only.config = m.config.clone();
        assert_ne!(config_only.version_id(), two_classes().version_id());
        assert_ne!(config_only.version_id(), m.version_id());
    }

    #[test]
    fn manifests_with_unknown_or_inconsistent_lines_are_refused() {
        let base = "wasmkeep-manifest 2\nclass chat 11\n";
        for bad in [
            // A format after this version's (7, ADR 0030).
            "wasmkeep-manifest 8\nclass chat 11\n".to_owned(),
            format!("{base}secret chat 11\n"),
            // Idle limits (ADR 0028): format 4 only, a known class, once,
            // between an hour and the longest.
            format!("{base}expire chat 3600\n"),
            "wasmkeep-manifest 3\nclass chat 11\nexpire chat 3600\n".to_owned(),
            "wasmkeep-manifest 4\nclass chat 11\nexpire other 3600\n".to_owned(),
            "wasmkeep-manifest 4\nclass chat 11\nexpire chat 3599\n".to_owned(),
            "wasmkeep-manifest 4\nclass chat 11\nexpire chat 7d\n".to_owned(),
            "wasmkeep-manifest 4\nclass chat 11\nexpire chat 3600\nexpire chat 7200\n".to_owned(),
            format!("{base}access other existing\n"),
            format!("{base}config other 33\n"),
            format!("{base}access chat closed\n"),
            format!("{base}access chat open\n"),
            format!("{base}access chat existing:b,a\n"),
            format!("{base}access chat internal:main\n"),
            format!("{base}config chat 33 extra\n"),
            // Format 1 never has rules or configuration.
            "wasmkeep-manifest 1\nclass chat 11\naccess chat existing\n".to_owned(),
            "wasmkeep-manifest 1\nclass chat 11\nconfig chat 33\n".to_owned(),
        ] {
            assert_eq!(Manifest::parse(&bad), None, "{bad}");
        }
    }

    #[test]
    fn a_batched_class_makes_format_5_and_reads_back() {
        // ADR 0034, section 15: without a batched class, the version keeps
        // its format, bytes and id (strict is written as absence).
        let mut m = two_classes();
        let before = (m.format(), m.version_id());
        m.batched.insert("chat".into());
        assert_eq!(m.format_number(), 5);
        let text = m.format();
        assert!(text.starts_with("wasmkeep-manifest 5\n"), "{text}");
        assert!(text.ends_with("writes chat batched\n"), "{text}");
        assert_ne!(m.version_id(), before.1);
        assert_eq!(Manifest::parse(&text), Some(m.clone()));
        // With everything formats 2 to 4 carry: all of it reads back.
        m.front = Some("ff".into());
        m.access.insert("counter".into(), "open".into());
        m.config.insert("chat".into(), "cc".into());
        m.requires.insert(
            "chat".into(),
            std::collections::BTreeSet::from(["KEY".into()]),
        );
        m.expire.insert("chat".into(), 7 * 86_400);
        m.batched.insert("counter".into());
        let text = m.format();
        assert!(text.starts_with("wasmkeep-manifest 5\n"), "{text}");
        assert_eq!(Manifest::parse(&text), Some(m.clone()));
        let mut m = two_classes();
        m.batched.insert("chat".into());
        m.batched.clear();
        assert_eq!((m.format(), m.version_id()), before);
    }

    #[test]
    fn writes_lines_are_refused_where_they_do_not_belong() {
        let ok = "wasmkeep-manifest 5\nclass chat 11\nwrites chat batched\n";
        assert!(Manifest::parse(ok).is_some());
        for bad in [
            // Strict is never written: absence says it.
            "wasmkeep-manifest 5\nclass chat 11\nwrites chat strict\n",
            "wasmkeep-manifest 5\nclass chat 11\nwrites chat gated\n",
            "wasmkeep-manifest 5\nclass chat 11\nwrites chat\n",
            "wasmkeep-manifest 5\nclass chat 11\nwrites chat batched extra\n",
            // A known class, once.
            "wasmkeep-manifest 5\nclass chat 11\nwrites nope batched\n",
            "wasmkeep-manifest 5\nclass chat 11\nwrites chat batched\nwrites chat batched\n",
            "wasmkeep-manifest 5\nclass chat 11\nwrites _front batched\n",
            // Not before format 5: a reader of format 4 stays as it was.
            "wasmkeep-manifest 4\nclass chat 11\nwrites chat batched\n",
            "wasmkeep-manifest 1\nclass chat 11\nwrites chat batched\n",
            // A format this version does not know.
            "wasmkeep-manifest 8\nclass chat 11\n",
        ] {
            assert_eq!(Manifest::parse(bad), None, "{bad}");
        }
        // Format 5 keeps the rules of the formats below it.
        for bad in [
            "wasmkeep-manifest 5\nclass chat 11\nexpire chat 1\nwrites chat batched\n",
            "wasmkeep-manifest 5\nclass chat 11\naccess nope open\nwrites chat batched\n",
        ] {
            assert_eq!(Manifest::parse(bad), None, "{bad}");
        }
    }

    /// A live heartbeat of a node serving `default/shop` with these words.
    pub(super) async fn heartbeat(store: &Arc<dyn ObjectStore>, id: &str, words: &str) {
        let body = format!(
            "wasmkeep-node 2\n{id}\nhttp://10.0.0.9:8080\n0.4.2\n{}\ndefault/shop\n\n\nserving\nsecrets\n{words}\n",
            crate::alarm::now_ms_i64()
        );
        store
            .put(
                &format!("nodes/{id}").as_str().into(),
                body.into_bytes().into(),
            )
            .await
            .unwrap();
    }

    /// Format 5 needs `flush` and inherits the reader checks of formats 4
    /// (`ends`) and 3 (`front`), even without an idle limit or a front.
    #[tokio::test]
    async fn format_5_needs_flush_and_every_word_below_it() {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let app = AppRef::new(Arc::clone(&store), "", "default", "shop");
        let mut m = two_classes();
        m.batched.insert("chat".into());
        app.check_readers(&m).await.unwrap();
        heartbeat(&store, "n", "take front handoff ends flush").await;
        app.check_readers(&m).await.unwrap();
        heartbeat(&store, "n", "take front handoff ends").await;
        let e = app.check_readers(&m).await.unwrap_err();
        assert!(
            matches!(&e, DeployError::NodesWithoutFlush { nodes, .. } if nodes == "n"),
            "{e}"
        );
        assert!(e.to_string().contains("manifest format 5"), "{e}");
        heartbeat(&store, "n", "take front handoff flush").await;
        let e = app.check_readers(&m).await.unwrap_err();
        assert!(matches!(e, DeployError::NodesWithoutEnds { .. }), "{e}");
        heartbeat(&store, "n", "take handoff ends flush").await;
        let e = app.check_readers(&m).await.unwrap_err();
        assert!(matches!(e, DeployError::NodesWithoutFront { .. }), "{e}");
        // Strict classes only: format 1, read by every node as before.
        heartbeat(&store, "n", "take").await;
        app.check_readers(&two_classes()).await.unwrap();
    }

    #[test]
    fn rate_limits_manifest_identity_and_legacy_bytes() {
        let mut m = two_classes();
        let old = (m.format(), m.version_id());
        m.limits.insert(
            "login".into(),
            limits::Policy::parse("POST /login", "ip", None, "5/s", 10).unwrap(),
        );
        assert_eq!(m.format_number(), 6);
        assert_eq!(Manifest::parse(&m.format()), Some(m.clone()));
        assert_ne!(m.version_id(), old.1);
        for bad in [
            m.format().replace("manifest 6", "manifest 5"),
            format!("{}limit login POST /login ip 5/s 10\n", m.format()),
            m.format().replace("5/s", "0/s"),
        ] {
            assert!(Manifest::parse(&bad).is_none(), "{bad}");
        }
        m.limits.clear();
        assert_eq!((m.format(), m.version_id()), old);
    }

    #[tokio::test]
    async fn rate_limits_require_capable_readers() {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let app = AppRef::new(Arc::clone(&store), "", "default", "shop");
        let mut m = two_classes();
        m.limits.insert(
            "login".into(),
            limits::Policy::parse("/login", "ip", None, "1/s", 1).unwrap(),
        );
        heartbeat(&store, "old", "take front ends flush").await;
        assert!(
            app.check_readers(&m)
                .await
                .unwrap_err()
                .to_string()
                .contains("rate_limits")
        );
        heartbeat(&store, "old", "take front ends flush rate_limits").await;
        app.check_readers(&m).await.unwrap();
    }

    #[test]
    fn modes_of_writes_parse_and_print() {
        for w in [Writes::Strict, Writes::Batched] {
            assert_eq!(Writes::parse(w.as_str()), Ok(w));
        }
        assert_eq!(Writes::default(), Writes::Strict);
        assert!(
            Writes::parse("gated")
                .unwrap_err()
                .contains("strict, batched")
        );
    }

    #[test]
    fn an_idle_limit_makes_format_4_and_reads_back() {
        // ADR 0028: without a limit, the version keeps its bytes and id.
        let mut m = two_classes();
        let before = (m.format(), m.version_id());
        m.expire.insert("chat".into(), 7 * 86_400);
        assert_eq!(m.format_number(), 4);
        let text = m.format();
        assert!(text.starts_with("wasmkeep-manifest 4\n"), "{text}");
        assert!(text.ends_with("expire chat 604800\n"), "{text}");
        assert_ne!(m.version_id(), before.1);
        assert_eq!(Manifest::parse(&text), Some(m.clone()));
        m.expire.clear();
        assert_eq!((m.format(), m.version_id()), before);
    }

    #[test]
    fn a_version_with_rules_but_no_front_is_written_as_before() {
        // Format 2, byte for byte and id for id, as before ADR 0023
        // (the id computed independently: `printf ... | sha256sum`).
        let mut m = two_classes();
        m.access
            .insert("chat".into(), "existing:main,room 1".into());
        m.config.insert("counter".into(), "33".into());
        m.requires.insert(
            "chat".into(),
            std::collections::BTreeSet::from(["TOKEN".into()]),
        );
        assert_eq!(
            m.format(),
            "wasmkeep-manifest 2\nprevious abc\nclass chat 11\nclass counter 22\n\
             access chat existing:main,room 1\nconfig counter 33\nrequires chat TOKEN\n"
        );
        assert_eq!(m.version_id(), "f6d159f826d96085");
    }

    fn with_front() -> Manifest {
        let mut m = two_classes();
        m.front = Some("44".into());
        m.access.insert("chat".into(), "existing".into());
        m.config.insert(FRONT.into(), "55".into());
        m.config.insert("counter".into(), "33".into());
        m.requires.insert(
            FRONT.into(),
            std::collections::BTreeSet::from(["SESSION_KEY".into()]),
        );
        m
    }

    #[test]
    fn a_front_makes_format_3_and_a_new_version() {
        let m = with_front();
        let text = m.format();
        assert_eq!(
            text,
            "wasmkeep-manifest 3\nprevious abc\nclass chat 11\nclass counter 22\n\
             front main 44\naccess chat existing\nconfig _front 55\nconfig counter 33\n\
             requires _front SESSION_KEY\n"
        );
        assert_eq!(Manifest::parse(&text), Some(m.clone()));
        // The front line is part of the identity: a deploy changing only
        // the front gets a new id.
        let other = Manifest {
            front: Some("45".into()),
            ..m.clone()
        };
        assert_ne!(other.version_id(), m.version_id());
        let mut none = m.clone();
        none.front = None;
        none.config.remove(FRONT);
        none.requires.clear();
        assert_eq!(none.format_number(), 2);
        assert_ne!(none.version_id(), m.version_id());
        // A front alone, no class.
        let alone = Manifest {
            assets: None,
            previous: None,
            classes: BTreeMap::new(),
            front: Some("44".into()),
            access: BTreeMap::new(),
            config: BTreeMap::new(),
            requires: BTreeMap::new(),
            expire: BTreeMap::new(),
            batched: std::collections::BTreeSet::new(),
            limits: Default::default(),
        };
        assert_eq!(alone.format(), "wasmkeep-manifest 3\nfront main 44\n");
        assert_eq!(Manifest::parse(&alone.format()), Some(alone));
    }

    #[test]
    fn format_3_says_open_and_earlier_formats_do_not() {
        // The project file writes every class's rule (ADR 0021).
        let text = "wasmkeep-manifest 3\nclass chat 11\naccess chat open\n";
        let m = Manifest::parse(text).unwrap();
        assert_eq!(m.access.get("chat").map(String::as_str), Some("open"));
        assert_eq!(m.format(), text, "read back as written");
        for format in [1, 2] {
            let old = format!("wasmkeep-manifest {format}\nclass chat 11\naccess chat open\n");
            assert_eq!(Manifest::parse(&old), None, "{old}");
        }
    }

    #[test]
    fn front_lines_are_refused_where_they_do_not_belong() {
        let v3 = "wasmkeep-manifest 3\nclass chat 11\n";
        let front = format!("{v3}front main 44\n");
        for bad in [
            // Not before format 3.
            "wasmkeep-manifest 2\nclass chat 11\nfront main 44\n".to_owned(),
            "wasmkeep-manifest 1\nclass chat 11\nfront main 44\n".to_owned(),
            // One front, named main.
            format!("{front}front main 45\n"),
            format!("{v3}front other 44\n"),
            format!("{v3}front main\n"),
            format!("{v3}front main 44 extra\n"),
            // The front's scope only beside a front line.
            format!("{v3}config _front 55\n"),
            format!("{v3}requires _front KEY\n"),
            "wasmkeep-manifest 2\nclass chat 11\nconfig _front 55\n".to_owned(),
            // No rule for the front, ever.
            format!("{front}access _front existing\n"),
            format!("{front}access _front open\n"),
            // `_front` is never a class.
            format!("{v3}class _front 66\n"),
            "wasmkeep-manifest 1\nclass _front 66\n".to_owned(),
            "wasmkeep-manifest 2\nclass _front 66\nconfig _front 55\n".to_owned(),
            // Open for a class the version does not have.
            format!("{v3}access other open\n"),
        ] {
            assert_eq!(Manifest::parse(&bad), None, "{bad}");
        }
        assert!(
            Manifest::parse(&format!("{front}config _front 55\nrequires _front KEY\n")).is_some()
        );
    }

    #[test]
    fn config_errors_never_show_a_value() {
        let e = parse_config(b"OK=1\nnot a line secret-value\n").unwrap_err();
        assert!(e.starts_with("2:"), "{e}");
        assert!(!e.contains("secret"), "{e}");
        let big = vec![b'#'; usize::try_from(crate::env::MAX_FILE).unwrap() + 1];
        assert!(parse_config(&big).is_err());
        assert!(parse_config(&[0xff, 0xfe]).is_err());
    }

    /// A local marker this node cannot read (here a directory where the
    /// file should be; in production EMFILE, EIO, EACCES) is not a
    /// mismatch: `still_ours` fails, and the follow loop retries, rather
    /// than stopping the app and removing its local files. Only a missing
    /// marker, or one with other bytes, says the app is no longer ours.
    #[tokio::test]
    async fn an_unreadable_local_marker_is_an_error_not_a_mismatch() {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let app = AppRef::new(Arc::clone(&store), "", "default", "shop");
        let local = tempfile::tempdir().unwrap();
        let marker = local.path().join(RECREATED_LOCAL);
        // No remote marker: the legacy app's files are kept.
        assert!(app.still_ours(local.path()).await.unwrap());
        app.put_once(RECREATED, b"one".to_vec()).await.unwrap();
        // Missing locally: recreated since this node prepared its files.
        assert!(!app.still_ours(local.path()).await.unwrap());
        std::fs::write(&marker, b"two").unwrap();
        assert!(!app.still_ours(local.path()).await.unwrap());
        std::fs::write(&marker, b"one").unwrap();
        assert!(app.still_ours(local.path()).await.unwrap());
        std::fs::remove_file(&marker).unwrap();
        std::fs::create_dir(&marker).unwrap();
        let read = app.still_ours(local.path()).await;
        assert!(
            matches!(&read, Err(DeployError::Local(why)) if why.contains(RECREATED_LOCAL)),
            "{read:?}"
        );
    }

    /// At start, too, a local marker this node cannot read is an error,
    /// never a reason to remove the app's local files: only a missing
    /// marker, or one with other bytes, is.
    #[tokio::test]
    async fn an_unreadable_local_marker_never_removes_local_files_at_start() {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let app = AppRef::new(Arc::clone(&store), "", "default", "shop");
        let data = tempfile::tempdir().unwrap();
        let local = data.path().join("app");
        app.put_once(RECREATED, b"one".to_vec()).await.unwrap();
        // Missing locally: the directory is replaced, the marker written.
        std::fs::create_dir_all(&local).unwrap();
        std::fs::write(local.join("state"), b"old").unwrap();
        app.prepare_local(&local).await.unwrap();
        assert!(!local.join("state").exists());
        assert_eq!(std::fs::read(local.join(RECREATED_LOCAL)).unwrap(), b"one");
        // Same bytes: kept.
        std::fs::write(local.join("state"), b"kept").unwrap();
        app.prepare_local(&local).await.unwrap();
        assert_eq!(std::fs::read(local.join("state")).unwrap(), b"kept");
        // Unreadable: an error, and nothing is removed.
        std::fs::remove_file(local.join(RECREATED_LOCAL)).unwrap();
        std::fs::create_dir(local.join(RECREATED_LOCAL)).unwrap();
        let prepared = app.prepare_local(&local).await;
        assert!(
            matches!(&prepared, Err(DeployError::Local(why)) if why.contains(RECREATED_LOCAL)),
            "{prepared:?}"
        );
        assert_eq!(std::fs::read(local.join("state")).unwrap(), b"kept");
        assert!(local.join(RECREATED_LOCAL).is_dir());
    }
}
