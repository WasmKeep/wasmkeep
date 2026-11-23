use std::sync::Arc;

use object_store::ObjectStore;
use wasmkeep_runtime::Runtime;

use super::{AppRef, DeployError, Manifest, StorageImports, bucket};
use crate::actor::FRONT_CLASS as FRONT;

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

    /// What a deploy checks before it writes (ADR 0022): no migration in
    /// progress, and the tenant's actor state not left in the layout
    /// before 0.4.0 (a `current` written now is rooted, token 3).
    pub(super) async fn check_layout(&self) -> Result<(), DeployError> {
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
    pub(super) async fn check_readers(&self, manifest: &Manifest) -> Result<(), DeployError> {
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
