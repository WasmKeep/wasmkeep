//! Deploying actor classes through the bucket, and nodes following the
//! deployed version (ADR 0008).
//!
//! ```text
//! apps/<tenant>/<app>/<version>/<class>.wasm   as uploaded, immutable
//! apps/<tenant>/<app>/<version>/<class>.config plain configuration, immutable
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

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use object_store::path::Path as StorePath;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload, UpdateVersion};
use sha2::{Digest, Sha256};
use wasmkeep_runtime::Runtime;

use crate::ActorService;

mod collect;
use crate::access::AccessRules;
use crate::env::Vars;
pub use collect::KEEP_PREVIOUS;

/// Why a deploy, rollback or update failed.
#[derive(Debug, thiserror::Error)]
pub enum DeployError {
    /// A component does not implement the actor world (nothing was uploaded).
    #[error("class {0:?} is not a valid actor: {1}")]
    Invalid(String, String),
    /// The bucket did not answer, or refused a write.
    #[error("bucket: {0}")]
    Bucket(String),
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
    /// A rollback would make these classes more open (ADR 0013).
    #[error("rolling back would reopen {0}; pass --allow-reopen to do it anyway")]
    Reopens(String),
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

    fn path(&self, key: &str) -> StorePath {
        StorePath::from(format!("{}{key}", self.base))
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
        marked: bool,
    ) -> Result<(), DeployError> {
        let mode = match read {
            None => PutMode::Create,
            Some(v) => PutMode::Update(v),
        };
        let opts = PutOptions {
            mode,
            ..PutOptions::default()
        };
        // Every `current` this version writes is rooted (ADR 0022), which
        // implies marked.
        debug_assert!(marked, "a rooted current is marked");
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
        let (access, config) = self.settings(version, &manifest).await?;
        Ok(Version {
            classes,
            access,
            config,
            requires: manifest.requires.clone(),
        })
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
        let changes = Changes {
            classes: Some(classes),
            access: Vec::new(),
            config: Vec::new(),
            requires: Vec::new(),
        };
        let set = Settings {
            access: access.clone(),
            config,
        };
        Ok(self.write(runtime, changes, Some(set)).await?.current)
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
        self.write(runtime, changes, None).await
    }

    /// Builds the new version on the current one and makes it current.
    /// `exact`: the version's whole rules and configuration (else the
    /// current ones, with `changes` applied).
    async fn write(
        &self,
        runtime: &Runtime,
        changes: Changes,
        exact: Option<Settings>,
    ) -> Result<Deployed, DeployError> {
        self.check_layout().await?;
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

        // The class table, and what to write for it.
        let mut uploads = Vec::new();
        let mut copies = Vec::new();
        let table: BTreeMap<String, String> = match changes.classes {
            Some(classes) => classes
                .into_iter()
                .map(|(name, bytes)| {
                    let digest = hex(&Sha256::digest(&bytes));
                    uploads.push((format!("{name}.wasm"), bytes));
                    (name, digest)
                })
                .collect(),
            None => {
                let Some((current, manifest, _)) = &base else {
                    return Err(DeployError::Nothing(
                        "nothing is deployed yet: give the classes (--actor)",
                    ));
                };
                for class in manifest.classes.keys() {
                    copies.push((current.version.clone(), format!("{class}.wasm")));
                }
                manifest.classes.clone()
            }
        };

        // Rules and values: the whole set given, or the current ones kept
        // for the classes still there, with the changes applied.
        let keep_requires = exact.is_none();
        let mut set = match exact {
            Some(set) => set,
            None => {
                let mut set = base.as_ref().map(|(_, _, s)| s.clone()).unwrap_or_default();
                set.access.retain(|c| table.contains_key(c));
                set.config.retain(|c, _| table.contains_key(c));
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
        for (class, vars) in changes.config {
            if !table.contains_key(&class) {
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
        // there (unless the whole set is given), with the changes applied.
        let mut requires: BTreeMap<String, std::collections::BTreeSet<String>> =
            match (keep_requires, &base) {
                (true, Some((_, m, _))) => m
                    .requires
                    .iter()
                    .filter(|(c, _)| table.contains_key(*c))
                    .map(|(c, n)| (c.clone(), n.clone()))
                    .collect(),
                _ => BTreeMap::new(),
            };
        for (class, names) in changes.requires {
            if !table.contains_key(&class) {
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
        let mut manifest = Manifest {
            previous: None,
            classes: table,
            access: BTreeMap::new(),
            config: BTreeMap::new(),
            requires,
        };
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
        // Stored as parsed, not as given: no comment (or commented-out
        // secret) is uploaded, and reformatting makes no new version.
        for (class, vars) in &set.config {
            if !manifest.classes.contains_key(class) {
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
        if let Some((current, base_manifest, _)) = &base {
            // A no-op when nothing but the lineage would differ.
            if base_manifest.identity() == manifest.identity() {
                return Ok(Deployed {
                    current: current.clone(),
                    diff,
                    newly_marked: false,
                });
            }
            manifest.previous = Some(current.version.clone());
        }
        let version = manifest.version_id();
        let was_marked = read.as_ref().is_some_and(|(c, _)| c.marked);
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
        self.swap_current(read.map(|(_, v)| v), &version, seq, marked)
            .await?;
        Ok(Deployed {
            current: Current {
                version,
                seq,
                marked,
                rooted: true,
            },
            diff,
            newly_marked: marked && !was_marked,
        })
    }

    /// Makes the version deployed before the current one current again, with
    /// a new, higher `seq`, and its rules and configuration. Refused if that
    /// makes a class more open, unless `allow_reopen` (ADR 0013).
    pub async fn rollback(&self, allow_reopen: bool) -> Result<Deployed, DeployError> {
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
        let (access, config) = self.settings(&current.version, &manifest).await?;
        let now = Settings { access, config };
        let (access, config) = self.settings(&previous, &back).await?;
        let then = Settings { access, config };
        let diff = Diff::between(Some((&manifest, &now)), (&back, &then));
        if !allow_reopen && !diff.reopened.is_empty() {
            return Err(DeployError::Reopens(diff.reopened.join(", ")));
        }
        // Rooted, so marked (ADR 0022).
        let marked = true;
        let seq = current.seq + 1;
        self.swap_current(Some(read), &previous, seq, marked)
            .await?;
        Ok(Deployed {
            current: Current {
                version: previous,
                seq,
                marked,
                rooted: true,
            },
            diff,
            newly_marked: marked && !current.marked,
        })
    }

    /// Keeps `service` on the deployed version: checks `current` every
    /// `every` (or at once when the service asks, see
    /// [`ActorService::deploy_seq`]), and swaps classes in at event
    /// boundaries when it changes. Runs until the service is dropped.
    pub async fn follow(self, service: ActorService, every: Duration) {
        let wake = service.deploy_wakeup();
        // After a failure (a version this node cannot build, a bucket
        // outage), wait longer each time, wake-ups included: refused
        // actors would otherwise have it download and recompile in a loop.
        let mut backoff: Option<Duration> = None;
        loop {
            match backoff {
                Some(wait) => tokio::time::sleep(wait).await,
                None => tokio::select! {
                    () = tokio::time::sleep(every) => {}
                    () = wake.notified() => {}
                },
            }
            match self.update(&service).await {
                Ok(_) => {
                    backoff = None;
                    service.set_deploy_behind(false);
                }
                Err(e) => {
                    // Read but not applicable: this node would decide under
                    // rules the app no longer has. A bucket outage is every
                    // node's, and keeps them ready.
                    if matches!(e, DeployError::Invalid(..) | DeployError::Corrupt(_)) {
                        service.set_deploy_behind(true);
                    }
                    let wait = backoff.map_or(every, |w| (w * 2).min(FOLLOW_BACKOFF_MAX));
                    tracing::warn!(error = %e, retry_in_s = wait.as_secs(), "checking for a new deploy failed");
                    backoff = Some(wait);
                }
            }
        }
    }

    /// One check of `current`; swaps classes in if it changed.
    pub async fn update(&self, service: &ActorService) -> Result<bool, DeployError> {
        let Some(current) = self.current().await? else {
            return Ok(false);
        };
        if current.seq <= service.deploy_seq() {
            return Ok(false);
        }
        let Version {
            classes,
            access,
            config,
            requires,
        } = self.version(&current.version).await?;
        // Everything compiled first, then swapped in at once with the
        // sequence, the rules and the values: a node that cannot build a
        // version keeps the previous one whole, and no event sees part of
        // a deploy.
        let runtime = service.runtime().clone();
        let mut compiled = Vec::with_capacity(classes.len());
        for (name, bytes) in classes {
            let component = runtime
                .compile(&bytes)
                .map_err(|e| DeployError::Invalid(name.clone(), format!("{e:#}")))?;
            compiled.push((name, component, bytes));
        }
        service
            .apply_version_with(
                current.seq,
                &compiled,
                &service.effective_rules(current.marked, &access),
                &config,
                &requires,
            )
            .map_err(|(name, e)| DeployError::Invalid(name, e.to_string()))?;
        tracing::info!(version = %current.version, seq = current.seq, "now running the deployed version");
        Ok(true)
    }
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
            .finish()
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
    /// What `current` now points at.
    pub current: Current,
    /// Against the version before.
    pub diff: Diff,
    /// This change set the marker: nodes take their rules from deploys
    /// from now on, and ignore `--access`.
    pub newly_marked: bool,
}

/// What changes between two versions: classes, rules, and the names (never
/// the values) of configuration.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Diff {
    /// Classes added.
    pub added: Vec<String>,
    /// Classes removed, with their rule if it was not `open`.
    pub removed: Vec<(String, Option<String>)>,
    /// Classes whose code changed.
    pub changed: Vec<String>,
    /// `(class, before, after)` for each kept or added class whose rule
    /// changes (`open` when it has none).
    pub rules: Vec<(String, String, String)>,
    /// `"class: NAME added"` and the like, names only.
    pub config: Vec<String>,
    /// Kept classes the change makes more open, as `class (before -> after)`.
    pub reopened: Vec<String>,
    /// `"class: NAME now required"` and the like (ADR 0016).
    pub requires: Vec<String>,
}

impl Diff {
    fn between(before: Option<(&Manifest, &Settings)>, after: (&Manifest, &Settings)) -> Self {
        let empty_manifest = Manifest {
            previous: None,
            classes: BTreeMap::new(),
            access: BTreeMap::new(),
            config: BTreeMap::new(),
            requires: BTreeMap::new(),
        };
        let empty = Settings::default();
        let (bm, bs) = before.unwrap_or((&empty_manifest, &empty));
        let (am, as_) = after;
        let mut diff = Self::default();
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
        let open = || "open".to_owned();
        for class in am.classes.keys() {
            let (b, a) = (bs.access.spec(class), as_.access.spec(class));
            if b != a {
                let (b, a) = (b.unwrap_or_else(open), a.unwrap_or_else(open));
                if bm.classes.contains_key(class) && more_open(&bs.access, &as_.access, class) {
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
        }
        diff
    }

    /// One line per change, for `wasmkeep deploy` to print.
    #[must_use]
    pub fn lines(&self) -> Vec<String> {
        let mut out = Vec::new();
        out.extend(self.added.iter().map(|c| format!("class {c}: added")));
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
        out.extend(self.config.iter().map(|l| format!("config {l}")));
        out.extend(self.requires.iter().map(|l| format!("requires {l}")));
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
    /// Each class's component bytes.
    pub classes: Vec<(String, Vec<u8>)>,
    /// The version's access rules (classes not listed are open).
    pub access: AccessRules,
    /// Each class's plain configuration (classes not listed have none).
    pub config: BTreeMap<String, Vars>,
    /// Each class's required names (ADR 0016; classes not listed require
    /// none).
    pub requires: BTreeMap<String, std::collections::BTreeSet<String>>,
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
    /// The version that was current when this one was deployed.
    pub previous: Option<String>,
    /// Class name -> SHA-256 of its component.
    pub classes: BTreeMap<String, String>,
    /// Class name -> its access rule in canonical form
    /// ([`AccessRules::spec`]); open classes are not listed.
    pub access: BTreeMap<String, String>,
    /// Class name -> SHA-256 of its `.config` object.
    pub config: BTreeMap<String, String>,
    /// Class name -> the names it requires among its values (ADR 0016):
    /// a class missing one refuses its events.
    pub requires: BTreeMap<String, std::collections::BTreeSet<String>>,
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

    /// 1 when there are neither rules nor configuration (the form from
    /// before ADR 0013, kept as the minimal one), else 2.
    #[must_use]
    pub fn format_number(&self) -> u8 {
        if self.access.is_empty() && self.config.is_empty() && self.requires.is_empty() {
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

    fn parse(text: &str) -> Option<Self> {
        let mut lines = text.lines();
        let format = match lines.next()? {
            "wasmkeep-manifest 1" => 1,
            "wasmkeep-manifest 2" => 2,
            _ => return None,
        };
        let mut manifest = Self {
            previous: None,
            classes: BTreeMap::new(),
            access: BTreeMap::new(),
            config: BTreeMap::new(),
            requires: BTreeMap::new(),
        };
        for line in lines {
            // `access` keeps the rest of the line: names may hold spaces.
            if format >= 2
                && let Some(rest) = line.strip_prefix("access ")
            {
                let (class, spec) = rest.split_once(' ')?;
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
                (Some("previous"), Some(v), None, None) => manifest.previous = Some(v.to_owned()),
                (Some("class"), Some(n), Some(d), None) => {
                    manifest.classes.insert(n.to_owned(), d.to_owned());
                }
                (Some("config"), Some(n), Some(d), None) if format >= 2 => {
                    manifest.config.insert(n.to_owned(), d.to_owned());
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
        let known = |c: &String| manifest.classes.contains_key(c);
        if !manifest.access.keys().all(known)
            || !manifest.config.keys().all(known)
            || !manifest.requires.keys().all(known)
        {
            return None;
        }
        Some(manifest)
    }
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
            previous: Some("abc".into()),
            classes: BTreeMap::from([
                ("chat".into(), "11".into()),
                ("counter".into(), "22".into()),
            ]),
            access: BTreeMap::new(),
            config: BTreeMap::new(),
            requires: BTreeMap::new(),
        }
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
            previous: Some("aaaa".into()),
            ..two_classes()
        };
        let after_b = Manifest {
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
            classes: vec![("c".into(), vec![0; 3])],
            access: AccessRules::default(),
            config: BTreeMap::from([(
                "c".to_owned(),
                Vars::from([("URL".to_owned(), "https://hidden.test".to_owned())]),
            )]),
            requires: BTreeMap::new(),
        };
        let shown = format!("{v:?}");
        assert!(
            shown.contains("URL") && !shown.contains("hidden"),
            "{shown}"
        );
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
            "wasmkeep-manifest 3\nclass chat 11\n".to_owned(),
            format!("{base}secret chat 11\n"),
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
    fn config_errors_never_show_a_value() {
        let e = parse_config(b"OK=1\nnot a line secret-value\n").unwrap_err();
        assert!(e.starts_with("2:"), "{e}");
        assert!(!e.contains("secret"), "{e}");
        let big = vec![b'#'; usize::try_from(crate::env::MAX_FILE).unwrap() + 1];
        assert!(parse_config(&big).is_err());
        assert!(parse_config(&[0xff, 0xfe]).is_err());
    }
}
