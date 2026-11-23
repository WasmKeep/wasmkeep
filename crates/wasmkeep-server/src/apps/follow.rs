use std::time::Duration;

use wasmkeep_cluster::restore_fence::{marker_matches, start_allowed};
use wasmkeep_runtime::Runtime;

use super::{AppRef, DeployError, RECREATED, RECREATED_LOCAL, Version, VersionFront, error_chain};
use crate::ActorService;
use crate::actor::FRONT_CLASS as FRONT;

/// A local copy of an app marker (`.recreated`, `.restored`): `None` only
/// when the file is missing. Any other error (EMFILE under socket load,
/// EIO, EACCES) says nothing about the app: an error, so the caller retries
/// or fails, never stops the app or removes its local files as if the
/// marker did not match.
pub(super) fn read_local_marker(path: &std::path::Path) -> Result<Option<Vec<u8>>, DeployError> {
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

/// Longest wait between attempts after a failed update.
const FOLLOW_BACKOFF_MAX: Duration = Duration::from_secs(300);

impl AppRef {
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
    pub(super) async fn still_ours(&self, local: &std::path::Path) -> Result<bool, DeployError> {
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
