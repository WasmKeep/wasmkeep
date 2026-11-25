use std::sync::Arc;

use object_store::ObjectStore;
use wasmkeep_cluster::app_format::floor::{self, Minor};
use wasmkeep_cluster::membership::NodeInfo;
use wasmkeep_cluster::upgrade::{self, Blocked, Format};
use wasmkeep_runtime::Runtime;

use super::{AppRef, DeployError, Manifest, StorageImports, bucket};
use crate::actor::FRONT_CLASS as FRONT;

/// Who reads what a writer is about to write (ADR 0043, section 2): an
/// app's upgrade floor and the live nodes under its bucket prefix, read
/// once. Every writer, the CLI included, asks this one helper whether
/// every live node announces a word ([`Readers::without`]) and whether a
/// format may be written now ([`Readers::gate`]).
///
/// Live as `cluster status` counts it: the default horizon, on this
/// machine's clock.
#[derive(Debug, Clone)]
pub struct Readers {
    /// `tenant/app`.
    served: String,
    floor: Result<Option<Minor>, floor::Unknown>,
    /// Every live node under the prefix, of any app.
    nodes: Vec<NodeInfo>,
}

impl Readers {
    /// Reads `tenant/app`'s floor and the live heartbeats under the
    /// bucket key `prefix`.
    ///
    /// # Errors
    /// The bucket did not answer.
    pub async fn read(
        store: Arc<dyn ObjectStore>,
        prefix: &str,
        tenant: &str,
        app: &str,
    ) -> Result<Self, DeployError> {
        use wasmkeep_cluster::lease::Bucket as _;
        let keys = wasmkeep_cluster::object_store::ObjectStoreBucket::new(store, prefix.to_owned());
        let record = keys
            .get(&floor::relative_key(tenant, app))
            .await
            .map_err(bucket)?;
        let floor = floor::decode(record.as_ref().map(|(body, _)| body.as_slice()));
        let nodes = live_on(&keys).await?;
        Ok(Self::of(format!("{tenant}/{app}"), floor, nodes))
    }

    /// The live nodes under the bucket key `prefix`, of every app, with
    /// no app's floor: for what a whole process needs (`multi-app`).
    ///
    /// # Errors
    /// The bucket did not answer.
    pub async fn read_prefix(
        store: Arc<dyn ObjectStore>,
        prefix: &str,
    ) -> Result<Self, DeployError> {
        let keys = wasmkeep_cluster::object_store::ObjectStoreBucket::new(store, prefix.to_owned());
        Ok(Self::of(String::new(), Ok(None), live_on(&keys).await?))
    }

    /// From what was read already: `served` (`tenant/app`), its floor,
    /// and the live nodes under the prefix.
    pub fn of(
        served: String,
        floor: Result<Option<Minor>, floor::Unknown>,
        live: Vec<NodeInfo>,
    ) -> Self {
        Self {
            served,
            floor,
            nodes: live,
        }
    }

    /// The live nodes of the app.
    pub fn live(&self) -> impl Iterator<Item = &NodeInfo> + Clone {
        self.nodes.iter().filter(|n| n.app == self.served)
    }

    /// The live nodes under the prefix, of every app.
    pub fn live_on_prefix(&self) -> &[NodeInfo] {
        &self.nodes
    }

    /// The app's floor record as read: `Ok(None)` without one.
    pub fn floor(&self) -> &Result<Option<Minor>, floor::Unknown> {
        &self.floor
    }

    /// The live nodes of the app that do not announce every word of
    /// `words`, by id. Empty: every live node announces them.
    pub fn without(&self, words: &[&str]) -> Vec<String> {
        upgrade::without(words, self.live())
    }

    /// [`Self::without`], among the live nodes of the app that `pick`
    /// keeps (those that read the app's secrets, for one).
    pub fn without_among(&self, words: &[&str], pick: impl Fn(&NodeInfo) -> bool) -> Vec<String> {
        upgrade::without(words, self.live().filter(|n| pick(n)))
    }

    /// [`Self::without`], among the live nodes of every app under the
    /// prefix.
    pub fn without_on_prefix(&self, words: &[&str]) -> Vec<String> {
        upgrade::without(words, &self.nodes)
    }

    /// Whether `format` may be written to the app now: its floor at or
    /// above the version that introduced the format, and every live node
    /// of the app announcing its words.
    ///
    /// # Errors
    /// What blocks, naming the nodes.
    pub fn gate(&self, format: &Format) -> Result<(), Blocked> {
        upgrade::gate(format, self.floor.clone(), self.live())
    }

    /// Whether a writer of minor version `this` may write to the app at
    /// all (ADR 0043, sections 3 and 5): not under a floor above its own
    /// version, nor beside a live node outside its range.
    ///
    /// # Errors
    /// Why not.
    pub fn writer(&self, this: Minor) -> Result<(), String> {
        upgrade::writer(this, self.floor.clone(), self.live())
    }
}

/// The live heartbeats under `keys`' prefix.
async fn live_on(
    keys: &wasmkeep_cluster::object_store::ObjectStoreBucket,
) -> Result<Vec<NodeInfo>, DeployError> {
    let horizon = crate::heartbeat_horizon_ms(
        crate::DEFAULT_HEARTBEAT,
        &crate::LeaseConfig::default_safe(),
    );
    let now = u64::try_from(crate::alarm::now_ms_i64()).unwrap_or(0);
    let nodes = wasmkeep_cluster::membership::all_heartbeats(keys)
        .await
        .map_err(bucket)?;
    Ok(nodes
        .into_iter()
        .filter(|n| wasmkeep_cluster::membership::is_live(n, now, horizon))
        .collect())
}

impl AppRef {
    /// What a first deploy checks before it creates the app: the names'
    /// grammar (apps that exist keep theirs). Other apps of the tenant on
    /// the prefix are fine: each app's actors live under its own state
    /// root (ADR 0022).
    pub(super) async fn check_new(&self) -> Result<(), DeployError> {
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

    /// A fresh app is born finalized (ADR 0043, section 3): the first
    /// deploy of an app that never ran (no floor, no registered node, no
    /// deploy, no state) creates its upgrade floor, so a new app needs no
    /// `finalize` before its first gated deploy. The floor is the lower
    /// of this build's minor version and the lowest version live under
    /// the bucket prefix, so no live node is shut out of the new app, and
    /// none is written when that is not above what no record means
    /// ([`upgrade::deploy_floor`]). Called once the deploy is accepted (a
    /// refused or cancelled one writes nothing) and before it writes
    /// anything of the app.
    ///
    /// # Errors
    /// The bucket did not answer, or the listing after the create shows
    /// an older node of the app that started meanwhile: the floor stays,
    /// and the deploy writes nothing else.
    pub(super) async fn born_finalized(&self) -> Result<(), DeployError> {
        let keys = wasmkeep_cluster::object_store::ObjectStoreBucket::new(
            Arc::clone(&self.store),
            self.prefix.clone(),
        );
        let this = upgrade::this_minor().map_err(DeployError::Refused)?;
        let horizon = crate::heartbeat_horizon_ms(
            crate::DEFAULT_HEARTBEAT,
            &crate::LeaseConfig::default_safe(),
        );
        let now = u64::try_from(crate::alarm::now_ms_i64()).unwrap_or(0);
        let born = upgrade::born_finalized(
            &keys,
            &self.tenant,
            &self.app,
            upgrade::FirstWriter::Deploy,
            this,
            now,
            horizon,
        )
        .await
        .map_err(bucket)?;
        match born {
            upgrade::Born::Late { floor, nodes } => Err(DeployError::Refused(format!(
                "{}: this deploy created the app's upgrade floor at {floor}, and the listing \
                 after it shows older nodes of the app that started meanwhile: {nodes}. The \
                 floor stays and nothing else was written. Such a node cannot come back on its old \
                 version: upgrade each of them (or stop it, or `wasmkeep cluster fence` it), \
                 then deploy again",
                self.name()
            ))),
            _ => Ok(()),
        }
    }

    /// What a deploy checks before it writes (ADR 0022): no migration in
    /// progress, and the tenant's actor state not left in the layout
    /// before 0.4.0 (a `current` written now is rooted, token 3).
    pub(super) async fn check_layout(&self) -> Result<(), DeployError> {
        let keys = wasmkeep_cluster::object_store::ObjectStoreBucket::new(
            Arc::clone(&self.store),
            self.prefix.clone(),
        );
        wasmkeep_cluster::legacy_layout::start_check(&keys, &self.tenant)
            .await
            .map(drop)
            .map_err(|e| match e {
                wasmkeep_cluster::legacy_layout::StartRefusal::Bucket(e) => bucket(e),
                e => DeployError::Layout(e.to_string()),
            })
    }

    /// The app's floor and live nodes ([`Readers`]): what a deploy, a
    /// delete and every other writer ask before they write.
    ///
    /// # Errors
    /// The bucket did not answer.
    pub async fn readers(&self) -> Result<Readers, DeployError> {
        Readers::read(
            Arc::clone(&self.store),
            &self.prefix,
            &self.tenant,
            &self.app,
        )
        .await
    }

    /// Refuses `manifest` while it may not be written (ADR 0043, section
    /// 2): its format's row of the gate table, through [`Readers::gate`].
    /// Format 5 needs `flush` (ADR 0034), and every format inherits the
    /// words of those below it (4 needs `ends`, 3 needs `front`). Then
    /// the writer itself: refused under a floor above this build's
    /// version, or beside a live node outside its range.
    pub(super) async fn check_readers(&self, manifest: &Manifest) -> Result<(), DeployError> {
        let readers = self.readers().await?;
        let number = manifest.format_number();
        let format = upgrade::manifest_format(number)
            .ok_or_else(|| DeployError::Refused(format!("manifest format {number} has no gate")))?;
        let app = self.name();
        match readers.gate(format) {
            Ok(()) => {}
            Err(Blocked::Nodes { word, nodes, .. }) => {
                let nodes = nodes.join(", ");
                return Err(match word {
                    "assets" => DeployError::NodesWithoutAssets { app, nodes },
                    "flush" => DeployError::NodesWithoutFlush {
                        app,
                        nodes,
                        what: "a version with a batched class (manifest format 5) needs".into(),
                    },
                    "ends" => DeployError::NodesWithoutEnds { app, nodes },
                    "front" => DeployError::NodesWithoutFront { app, nodes },
                    // `rate_limits`, the word of format 6.
                    word => DeployError::Refused(format!(
                        "manifest format 6 needs {word} on all live readers: {nodes}"
                    )),
                });
            }
            Err(blocked) => return Err(DeployError::Refused(format!("{app}: {blocked}"))),
        }
        upgrade::this_minor()
            .and_then(|this| readers.writer(this))
            .map_err(|why| DeployError::Refused(format!("{app}: {why}")))
    }

    /// Refuses a version whose components import a `storage` function a
    /// live node of the app lacks (ADR 0034, section 15): `flush` needs the
    /// word `flush`, `delete-self-durable` the word `ends` (for strict
    /// classes too: section 1, exception 2). Such a node could not link the
    /// component and would stay not ready on the version. `given` are the
    /// components a deploy was given, already read; `kept`, `(version,
    /// class)`, those it keeps or a rollback brings back, read from the
    /// bucket only when a live node lacks a word.
    pub(super) async fn check_imports(
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
        let readers = self.readers().await?;
        if readers.without(&["flush", "ends", "blobs"]).is_empty() {
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
        let lacking = |word: &str| readers.without(&[word]).join(", ");
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
            let without = lacking("flush");
            if !without.is_empty() {
                return Err(DeployError::NodesWithoutFlush {
                    app,
                    nodes: without,
                    what: what(&flush),
                });
            }
        }
        if !blobs.is_empty() {
            let without = lacking("blobs");
            if !without.is_empty() {
                return Err(DeployError::NodesWithoutBlobs {
                    app,
                    nodes: without,
                    what: what(&blobs),
                });
            }
        }
        if !ends.is_empty() {
            let without = lacking("ends");
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
) -> Result<Vec<NodeInfo>, DeployError> {
    let keys = wasmkeep_cluster::object_store::ObjectStoreBucket::new(store, prefix.to_owned());
    let served = format!("{tenant}/{app}");
    Ok(live_on(&keys)
        .await?
        .into_iter()
        .filter(|n| n.app == served)
        .collect())
}
