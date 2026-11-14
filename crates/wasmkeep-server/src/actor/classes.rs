//! Actor classes: a compiled component with its access rules,
//! configuration and required values.

use super::*;

/// One actor class: a component implementing the `wasmkeep:actor/actor` world.
#[derive(Clone)]
pub(super) struct ActorClass {
    pub(super) pre: InstancePre<ActorHost>,
    /// SHA-256 of the component bytes; `on-init` reruns when it changes.
    pub(super) version: String,
    /// The deploy sequence this class was swapped in under (0: not from
    /// `wasmkeep deploy`). An event takes it with the class, so the code it
    /// runs and the sequence it records always agree (ADR 0008).
    pub(super) seq: u64,
    /// The access rules it was deployed with (ADR 0013). An event decides
    /// access with the rules of the class it took, never the node's.
    pub(super) rules: Arc<crate::access::AccessRules>,
    /// Its values for `env.get`: the deployed configuration with this
    /// node's `--env` values over it.
    pub(super) env: Arc<crate::env::Vars>,
    /// Names its values must hold (ADR 0016, `requires`): without one, its
    /// events are refused.
    pub(super) requires: Arc<std::collections::BTreeSet<String>>,
    /// Whether a missing required name was already logged for this class
    /// version (once, not per event).
    pub(super) warned: Arc<std::sync::atomic::AtomicBool>,
}

impl ActorClass {
    /// The first required name its values lack, if any.
    /// A value set to the empty string counts as set.
    pub(super) fn missing_required(&self, class: &str) -> Option<&str> {
        let mut missing = self
            .requires
            .iter()
            .filter(|n| !self.env.contains_key(*n))
            .map(String::as_str);
        let first = missing.next()?;
        if !self.warned.swap(true, std::sync::atomic::Ordering::Relaxed) {
            let all: Vec<&str> = std::iter::once(first).chain(missing).collect();
            tracing::error!(%class, variables = %all.join(", "), "required values are not set: the class refuses its events (set them with `wasmkeep secrets set`, or remove them with `wasmkeep deploy --requires`)");
        }
        Some(first)
    }
}

/// The linker every actor class is linked against.
pub(super) fn actor_linker(runtime: &Runtime) -> Result<Linker<ActorHost>, Error> {
    let mut linker = Linker::<ActorHost>::new(runtime.engine());
    wasmtime_wasi::p2::add_to_linker_async(&mut linker).map_err(Error::linker)?;
    wasmtime_wasi_http::p2::add_only_http_to_linker_async(&mut linker).map_err(Error::linker)?;
    ActorCore::add_to_linker::<ActorHost, HasSelf<ActorHost>>(&mut linker, |h| h)
        .map_err(Error::linker)?;
    Ok(linker)
}

pub(super) type Classes = Arc<std::sync::RwLock<HashMap<String, Arc<ActorClass>>>>;

pub(super) fn read_lock<T>(lock: &std::sync::RwLock<T>) -> std::sync::RwLockReadGuard<'_, T> {
    lock.read().unwrap_or_else(|p| p.into_inner())
}

pub(super) fn read(
    classes: &Classes,
) -> std::sync::RwLockReadGuard<'_, HashMap<String, Arc<ActorClass>>> {
    classes.read().unwrap_or_else(|p| p.into_inner())
}

/// Links and validates one class: the component must implement the
/// `wasmkeep:actor/actor` exports (lifecycle hooks and the HTTP handler).
pub(super) fn build_class(
    runtime: &Runtime,
    linker: &Linker<ActorHost>,
    tenant: &str,
    name: &str,
    component: &Component,
    bytes: &[u8],
) -> Result<ActorClass, Error> {
    ActorAddress::new(tenant, name, "x")
        .map_err(|e| Error::InvalidClass(name.to_owned(), e.to_string()))?;
    let pre = runtime
        .instantiate_pre(linker, component)
        .map_err(|e| Error::NotAnActor(name.to_owned(), Box::new(e)))?;
    ActorCorePre::new(pre.clone())
        .map_err(|e| Error::NotAnActor(name.to_owned(), e.into_boxed_dyn_error()))?;
    ProxyPre::new(pre.clone())
        .map_err(|e| Error::NotAnActor(name.to_owned(), e.into_boxed_dyn_error()))?;
    Ok(ActorClass {
        pre,
        version: hex(&Sha256::digest(bytes)),
        seq: 0,
        rules: Arc::default(),
        env: Arc::default(),
        requires: Arc::default(),
        warned: Arc::default(),
    })
}

/// `class`'s values, by name, last wins: the deployed configuration, the
/// app's secrets (ADR 0016), the node's `--env` values. A name set in more
/// than one is logged (never its value).
pub(super) fn class_env(
    node: &crate::env::ActorEnv,
    class: &str,
    deployed: Option<&crate::env::Vars>,
    secrets: Option<&crate::env::Vars>,
) -> Arc<crate::env::Vars> {
    merge_values(class, node.for_class(class), deployed, secrets)
}

/// The front's values (ADR 0023), from the same three layers as a class's:
/// its deployed configuration, the app's `_front` secrets, the node's
/// `--front-env`.
pub(super) fn front_env(
    node: &Arc<crate::env::Vars>,
    deployed: &crate::env::Vars,
    secrets: &crate::secrets::SecretSet,
) -> Arc<crate::env::Vars> {
    merge_values(
        FRONT_CLASS,
        Arc::clone(node),
        (!deployed.is_empty()).then_some(deployed),
        secrets.class(FRONT_CLASS),
    )
}

/// Links a deployed version's front (no lock held) with its values.
pub(super) fn front_version(
    runtime: &Runtime,
    deployed: &crate::front::DeployedFront,
    node: &Arc<crate::env::Vars>,
    secrets: &crate::secrets::SecretSet,
) -> Result<crate::front::FrontVersion, Error> {
    let linked = crate::front::FrontVersion::link(runtime, &deployed.component, Arc::default())?;
    Ok(front_values(&linked, deployed, node, secrets))
}

/// `linked` with the values `deployed` and the node give it.
pub(super) fn front_values(
    linked: &crate::front::FrontVersion,
    deployed: &crate::front::DeployedFront,
    node: &Arc<crate::env::Vars>,
    secrets: &crate::secrets::SecretSet,
) -> crate::front::FrontVersion {
    linked.with_values(
        front_env(node, &deployed.config, secrets),
        Arc::new(deployed.config.clone()),
        Arc::new(deployed.requires.clone()),
    )
}

/// `scope`'s values by name, last wins: `deployed`, `secrets`, `local`.
fn merge_values(
    class: &str,
    local: Arc<crate::env::Vars>,
    deployed: Option<&crate::env::Vars>,
    secrets: Option<&crate::env::Vars>,
) -> Arc<crate::env::Vars> {
    if deployed.is_none() && secrets.is_none() {
        return local;
    }
    let mut out = deployed.cloned().unwrap_or_default();
    for (name, value) in secrets.into_iter().flatten() {
        if out.contains_key(name) {
            tracing::warn!(%class, variable = %name, "a secret overrides the deployed configuration");
        }
        out.insert(name.clone(), value.clone());
    }
    for (name, value) in local.iter() {
        if secrets.is_some_and(|s| s.contains_key(name)) {
            tracing::warn!(%class, variable = %name, "this node's --env value overrides the app's secret");
        } else if out.contains_key(name) {
            tracing::warn!(%class, variable = %name, "this node's --env value overrides the deployed configuration");
        }
        out.insert(name.clone(), value.clone());
    }
    Arc::new(out)
}
