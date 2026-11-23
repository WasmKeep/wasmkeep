use std::collections::BTreeMap;
use std::time::Duration;

use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload, UpdateVersion};
use sha2::{Digest, Sha256};
use wasmkeep_runtime::Runtime;

use super::{
    AppRef, Changes, Current, DeployError, Deployed, Diff, FrontChange, Manifest, Settings,
    StorageImports, Writes, assets_deploy, bucket, error_chain, format_config, hex, limits,
};
use crate::ActorService;
use crate::access::AccessRules;
use crate::actor::FRONT_CLASS as FRONT;
use crate::env::Vars;

impl AppRef {
    /// Points `current` at `version`, conditionally on what was read.
    pub(super) async fn swap_current(
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
}
