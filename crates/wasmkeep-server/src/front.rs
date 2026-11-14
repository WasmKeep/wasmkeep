//! The front (ADR 0023): an app's stateless `wasi:http` component, run in
//! a fresh instance per request, for every path outside `/actor/` and
//! `/_wasmkeep/`. It is linked with all of WASI 0.2 (inert), `wasi:http`
//! under the node's egress policy, and `actors` and `env`: it calls the
//! node's actors as an inside caller, `<tenant>/_front/main`, and reads its
//! own values.
//!
//! What is per node (the slots, the calls waiting on each actor, the
//! limits, the egress policy, the log, the node's values) is kept apart
//! from what is per version (the linked component and the values it reads,
//! [`FrontVersion`]), so that a version can be swapped under it without
//! resetting the slots.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use bytes::Bytes;
use http_body_util::BodyExt;
use hyper::body::Incoming;
use hyper::{Request, Response, StatusCode};
use wasmkeep_runtime::{ActorAddress, CpuBudgetExceeded, Runtime};
use wasmkeep_wit::FrontCore;
use wasmkeep_wit::wasmkeep::actor::actors::{
    self, CallError, Request as CallRequest, Response as CallResponse,
};
use wasmkeep_wit::wasmkeep::actor::{calls, env};
use wasmtime::component::{Component, HasSelf, Linker, ResourceTable};
use wasmtime_wasi::{WasiCtx, WasiCtxView, WasiView};
use wasmtime_wasi_http::p2::bindings::ProxyPre;
use wasmtime_wasi_http::p2::bindings::http::types::Scheme;
use wasmtime_wasi_http::p2::body::HyperOutgoingBody;
use wasmtime_wasi_http::{WasiHttpCtx, WasiHttpCtxView, WasiHttpView};

use crate::actor::{BodyBudget, FRONT_CLASS, FRONT_NAME, refuse_batch};
use crate::http::{CappedBody, text_response};
use crate::metrics::{METRICS, inc};
use crate::{ActorService, Error};

/// What the front keeps of its wall for itself: a call gives up while this
/// much of the request's wall remains, so the front can still answer.
pub(crate) const CALL_MARGIN: Duration = Duration::from_secs(5);

/// The front's CPU budget per request by default (`--front-cpu-budget-ms`).
pub const DEFAULT_CPU_BUDGET: Duration = Duration::from_millis(50);

/// The failure a call from the front gets past the per-actor bound.
const BUSY: &str = "front busy: too many front requests waiting on this actor";

/// The failure a call from the front gets past the node's bound (ADR 0025).
const BUSY_NODE: &str = "front busy: too many front calls waiting on this node";

/// Per-request host state stored in the Wasmtime `Store`.
///
/// Guests get no stdio, environment, arguments or preopened directories:
/// capabilities are granted explicitly, never inherited from the host.
struct FrontHost {
    wasi: WasiCtx,
    http: WasiHttpCtx,
    table: ResourceTable,
    /// Outbound HTTP under the egress policy.
    egress: crate::egress::EgressHooks,
    /// The node's actors, which the front's calls reach; `None` on a node
    /// without actors (every call fails `invalid-address`).
    actors: Option<ActorService>,
    /// The tenant of the front's caller value and `self-address`.
    tenant: String,
    /// The front's values (`env.get`).
    env: Arc<crate::env::Vars>,
    /// When the request's wall runs out.
    deadline: tokio::time::Instant,
    /// Calls from the node's fronts waiting on each actor.
    waits: Arc<Waits>,
    /// How many of them one actor, and the node, may have.
    bounds: Bounds,
}

impl WasiView for FrontHost {
    fn ctx(&mut self) -> WasiCtxView<'_> {
        WasiCtxView {
            ctx: &mut self.wasi,
            table: &mut self.table,
        }
    }
}

impl WasiHttpView for FrontHost {
    fn http(&mut self) -> WasiHttpCtxView<'_> {
        WasiHttpCtxView {
            ctx: &mut self.http,
            table: &mut self.table,
            hooks: &mut self.egress,
        }
    }
}

impl FrontHost {
    /// When a call starting now must give up (ADR 0023): while the margin
    /// of the request's wall remains, or after the actors' call limit,
    /// whichever comes first. `None`: less than the margin is left, so a
    /// call fails at once with `timeout`.
    fn call_deadline(&self, service: &ActorService) -> Option<tokio::time::Instant> {
        let now = tokio::time::Instant::now();
        let left = self.deadline.saturating_duration_since(now);
        let limit = left.checked_sub(CALL_MARGIN).filter(|l| !l.is_zero())?;
        Some(now + limit.min(service.call_wall()))
    }

    /// One call, as `actors.call` makes it and as each entry of a
    /// `calls.call-many` does: counted in the per-actor bound while it
    /// waits, given up at `deadline`, its response taken from `budget`.
    async fn entry(
        service: &ActorService,
        waits: &Arc<Waits>,
        bounds: Bounds,
        address: String,
        request: CallRequest,
        deadline: Option<tokio::time::Instant>,
        budget: &BodyBudget,
    ) -> Result<CallResponse, CallError> {
        let target = service.front_target(&address)?;
        let Some(deadline) = deadline else {
            return Err(CallError::Timeout);
        };
        let limit = deadline.saturating_duration_since(tokio::time::Instant::now());
        if limit.is_zero() {
            return Err(CallError::Timeout);
        }
        let _waiting =
            Waits::enter(waits, &target, bounds).map_err(|busy| CallError::Failed(busy.into()))?;
        service.front_call(target, request, limit, budget).await
    }
}

/// Counts a call's outcome in `wasmkeep_front_calls_total`. Every other
/// failure (invalid-address, reentrant, too-large) is `failed`.
fn count(result: &Result<CallResponse, CallError>) {
    let outcome = match result {
        Ok(_) => "ok",
        Err(CallError::Timeout) => "timeout",
        Err(CallError::Failed(m)) if m == BUSY || m == BUSY_NODE => "busy",
        Err(_) => "failed",
    };
    inc(&METRICS.front_calls.get(&[("outcome", outcome)]));
}

impl actors::Host for FrontHost {
    async fn call(
        &mut self,
        address: String,
        request: CallRequest,
    ) -> wasmtime::Result<Result<CallResponse, CallError>> {
        let result = match self.actors.clone() {
            None => Err(CallError::InvalidAddress("no such actor class".into())),
            Some(service) => {
                let deadline = self.call_deadline(&service);
                let budget = BodyBudget::one();
                Self::entry(
                    &service,
                    &self.waits,
                    self.bounds,
                    address,
                    request,
                    deadline,
                    &budget,
                )
                .await
            }
        };
        count(&result);
        Ok(result)
    }

    async fn self_address(&mut self) -> wasmtime::Result<actors::ActorAddress> {
        Ok(actors::ActorAddress {
            tenant: self.tenant.clone(),
            class: FRONT_CLASS.to_owned(),
            name: FRONT_NAME.to_owned(),
        })
    }
}

impl calls::Host for FrontHost {
    /// ADR 0025: every entry is an `actors.call` from the front, all
    /// started at once, under the deadline a call starting now would have
    /// and one response budget; each counts in the per-actor bound and in
    /// the metrics as a call.
    async fn call_many(
        &mut self,
        calls: Vec<(String, CallRequest)>,
    ) -> wasmtime::Result<Vec<Result<CallResponse, CallError>>> {
        if let Some(refused) = refuse_batch(&calls) {
            // Refused as a whole, before any call: counted once, however
            // long the list a guest sent.
            if let Some(first) = refused.first() {
                count(first);
            }
            return Ok(refused);
        }
        let results = if let Some(service) = self.actors.clone() {
            let deadline = self.call_deadline(&service);
            let budget = BodyBudget::one();
            let (waits, bounds) = (&self.waits, self.bounds);
            let entries = calls.into_iter().map(|(address, request)| {
                Self::entry(&service, waits, bounds, address, request, deadline, &budget)
            });
            futures_util::future::join_all(entries).await
        } else {
            calls
                .iter()
                .map(|_| Err(CallError::InvalidAddress("no such actor class".into())))
                .collect()
        };
        results.iter().for_each(count);
        Ok(results)
    }
}

impl env::Host for FrontHost {
    async fn get(&mut self, name: String) -> wasmtime::Result<Option<String>> {
        Ok(self.env.get(&name).cloned())
    }
}

/// Calls from the fronts of a node waiting on each actor, for the
/// per-actor bound, and in all, for the node's bound (ADR 0025). An entry
/// goes at zero: names come from URLs.
#[derive(Debug, Default)]
struct Waits(Mutex<WaitCounts>);

#[derive(Debug, Default)]
struct WaitCounts {
    per_actor: HashMap<ActorAddress, usize>,
    total: usize,
}

/// The bounds a front's call waits under.
#[derive(Debug, Clone, Copy)]
struct Bounds {
    /// Calls waiting on one actor (a quarter of the slots, ADR 0023).
    per_actor: usize,
    /// Calls waiting from the node's fronts in all: the slots, as many as
    /// one call per request allows, so that batches (ADR 0025) hold no
    /// more of the actors' instances than single calls could.
    per_node: usize,
}

impl Waits {
    /// One more call waiting on `target`, unless a bound is reached: then
    /// the failure the call gets.
    fn enter(
        waits: &Arc<Self>,
        target: &ActorAddress,
        bounds: Bounds,
    ) -> Result<Waiting, &'static str> {
        let mut counts = waits.0.lock().unwrap_or_else(|p| p.into_inner());
        if counts.total >= bounds.per_node {
            return Err(BUSY_NODE);
        }
        let n = counts.per_actor.entry(target.clone()).or_default();
        if *n >= bounds.per_actor {
            if *n == 0 {
                counts.per_actor.remove(target);
            }
            return Err(BUSY);
        }
        *n += 1;
        counts.total += 1;
        Ok(Waiting {
            waits: Arc::clone(waits),
            target: target.clone(),
        })
    }
}

/// A call counted in [`Waits`] until dropped.
struct Waiting {
    waits: Arc<Waits>,
    target: ActorAddress,
}

impl Drop for Waiting {
    fn drop(&mut self) {
        let mut counts = self.waits.0.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(n) = counts.per_actor.get_mut(&self.target) {
            *n = n.saturating_sub(1);
            if *n == 0 {
                counts.per_actor.remove(&self.target);
            }
            counts.total = counts.total.saturating_sub(1);
        }
    }
}

/// A slot held while one front instance lives; the gauge counts them.
struct Slot(#[allow(dead_code, reason = "held for its drop")] tokio::sync::OwnedSemaphorePermit);

impl Slot {
    fn new(permit: tokio::sync::OwnedSemaphorePermit) -> Self {
        METRICS
            .front_slots_in_use
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Self(permit)
    }
}

impl Drop for Slot {
    fn drop(&mut self) {
        METRICS
            .front_slots_in_use
            .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
    }
}

/// One version of a front: its component, linked and pre-instantiated, and
/// the values it reads. Built with no lock held ([`FrontVersion::link`]),
/// then put in place with [`HttpComponent::set_version`], or by the actor
/// service with the classes of a deployed version.
pub(crate) struct FrontVersion {
    pre: ProxyPre<FrontHost>,
    /// What `env.get` reads: the three layers merged.
    env: Arc<crate::env::Vars>,
    /// The version's deployed configuration, kept so a secrets change can
    /// rebuild `env` without a deploy (ADR 0016).
    pub(crate) config: Arc<crate::env::Vars>,
    /// Names `env` must hold (`--front-requires`): while one is missing,
    /// the front is not instantiated.
    pub(crate) requires: Arc<std::collections::BTreeSet<String>>,
    /// Whether a missing required name was already logged for these
    /// values (once, not per request).
    warned: Arc<std::sync::atomic::AtomicBool>,
}

/// The actor world's exports, which a front never has: a component with
/// one is an actor given as a front by mistake (ADR 0023).
const ACTOR_EXPORTS: [&str; 5] = [
    "on-init",
    "on-alarm",
    "on-websocket-open",
    "on-websocket-message",
    "on-websocket-close",
];

impl FrontVersion {
    /// Links `component` against the front linker: WASI 0.2 (inert),
    /// `wasi:http`, `actors` and `env`; checks that it exports the incoming
    /// handler and none of the actor world's `on-*` functions. A component
    /// importing anything else (an actor's `storage`, say) does not link.
    pub(crate) fn link(
        runtime: &Runtime,
        component: &Component,
        env: Arc<crate::env::Vars>,
    ) -> Result<Self, Error> {
        let ty = component.component_type();
        if let Some((name, _)) = ty
            .exports(runtime.engine())
            .find(|(name, _)| ACTOR_EXPORTS.contains(name))
        {
            return Err(Error::NotAFront(format!(
                "it exports {name}, an actor's function: give actors with --actor CLASS=PATH"
            )));
        }
        let mut linker = Linker::<FrontHost>::new(runtime.engine());
        wasmtime_wasi::p2::add_to_linker_async(&mut linker).map_err(Error::linker)?;
        wasmtime_wasi_http::p2::add_only_http_to_linker_async(&mut linker)
            .map_err(Error::linker)?;
        FrontCore::add_to_linker::<FrontHost, HasSelf<FrontHost>>(&mut linker, |h| h)
            .map_err(Error::linker)?;
        let instance_pre = runtime
            .instantiate_pre(&linker, component)
            .map_err(Error::not_http)?;
        let pre =
            ProxyPre::new(instance_pre).map_err(|e| Error::not_http(e.into_boxed_dyn_error()))?;
        Ok(Self {
            pre,
            env,
            config: Arc::default(),
            requires: Arc::default(),
            warned: Arc::default(),
        })
    }

    /// This version with these values: `env` (the three layers merged),
    /// the deployed `config` it was built from, and the names it requires.
    /// A missing name is logged again.
    pub(crate) fn with_values(
        &self,
        env: Arc<crate::env::Vars>,
        config: Arc<crate::env::Vars>,
        requires: Arc<std::collections::BTreeSet<String>>,
    ) -> Self {
        Self {
            pre: self.pre.clone(),
            env,
            config,
            requires,
            warned: Arc::default(),
        }
    }

    /// What `env.get` reads.
    pub(crate) fn env(&self) -> &Arc<crate::env::Vars> {
        &self.env
    }

    /// Whether a required name is not set; the first time, logs which
    /// (names only, never to the client: the port faces the public).
    fn unconfigured(&self) -> bool {
        let missing: Vec<&str> = self
            .requires
            .iter()
            .filter(|n| !self.env.contains_key(*n))
            .map(String::as_str)
            .collect();
        if missing.is_empty() {
            return false;
        }
        if !self.warned.swap(true, std::sync::atomic::Ordering::Relaxed) {
            tracing::error!(actor = FRONT_CLASS, variables = %missing.join(", "), "required values are not set: the front answers 503 (set them with `wasmkeep secrets set --front`, or remove them with `wasmkeep deploy --front-requires ''`)");
        }
        true
    }
}

/// Checks that `component` is a valid front, without serving it: that it
/// links (see [`FrontVersion::link`]) and instantiates once against the
/// front linker, its start functions run (ADR 0023). What `wasmkeep
/// deploy --front` checks before it writes anything. It runs without
/// values: required names do not hold it back, and a front must
/// instantiate before they are set.
///
/// # Errors
/// Why it is not one.
pub(crate) async fn validate(runtime: &Runtime, component: &Component) -> Result<(), Error> {
    let front = HttpComponent::new(runtime.clone(), component)?;
    let Some(version) = front.current() else {
        return Ok(());
    };
    front.instantiate(&version).await.map_err(|e| {
        // The cause, without the backtrace lines around it.
        Error::NotAFront(format!(
            "it does not instantiate against the front linker (a start function \
                 trapped, or it needs more than a node's limits): {}",
            e.root_cause()
        ))
    })
}

/// Where a node keeps the front its deployed version brings: `None`
/// while the version has none (requests answer 404). Shared by the actor
/// service, which swaps it with the classes, and the [`HttpComponent`]
/// serving it.
pub(crate) type FrontSlot = Arc<RwLock<Option<Arc<FrontVersion>>>>;

/// A deployed version's front, as a node loads it (ADR 0023): the
/// compiled component, its deployed configuration and the names it
/// requires. Its `Debug` shows names, never a value.
#[derive(Clone)]
pub struct DeployedFront {
    /// The compiled component.
    pub component: Component,
    /// Its deployed plain configuration (`--front-config`).
    pub config: crate::env::Vars,
    /// The names it requires among its values (`--front-requires`).
    pub requires: std::collections::BTreeSet<String>,
}

impl std::fmt::Debug for DeployedFront {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeployedFront")
            .field("config", &self.config.keys().collect::<Vec<_>>())
            .field("requires", &self.requires)
            .finish_non_exhaustive()
    }
}

/// The front: a `wasi:http/proxy` component (any plain one is a valid
/// front), with the node's limits. Each request gets a fresh instance.
/// Cheap to clone; clones share the slots and the version.
#[derive(Clone)]
pub struct HttpComponent {
    runtime: Runtime,
    /// The version requests start on; one in flight keeps its own.
    /// `None` while the deployed version has no front: 404.
    version: FrontSlot,
    egress: Arc<crate::egress::EgressPolicy>,
    /// Where instances' stdout and stderr go.
    guest_log: crate::guest_log::GuestLog,
    /// Wall-clock limit per request, including streaming the response
    /// and waiting on I/O (a slow client must not hold an instance).
    pub(crate) wall: Duration,
    /// CPU time per request (`--front-cpu-budget-ms`).
    cpu_budget: Duration,
    /// Requests running at once (`--max-front-requests`).
    slots: Arc<tokio::sync::Semaphore>,
    max_requests: usize,
    /// Calls waiting on each actor, for the per-actor bound.
    waits: Arc<Waits>,
}

impl std::fmt::Debug for HttpComponent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpComponent")
            .field("max_requests", &self.max_requests)
            .finish_non_exhaustive()
    }
}

impl HttpComponent {
    /// Links `component` as a front (see [`FrontVersion::link`]): WASI
    /// 0.2 and `wasi:http`, plus `actors` and `env`.
    pub fn new(runtime: Runtime, component: &Component) -> Result<Self, Error> {
        let version = FrontVersion::link(&runtime, component, Arc::default())?;
        let pool = usize::try_from(runtime.max_instances()).unwrap_or(usize::MAX);
        // Alone on its node, the front has the whole pool, as before it had
        // its own slots; beside actors, size it (`with_max_requests`).
        let max_requests = pool.max(1);
        Ok(Self {
            runtime,
            version: Arc::new(RwLock::new(Some(Arc::new(version)))),
            egress: Arc::default(),
            guest_log: crate::guest_log::GuestLog::default(),
            wall: Duration::from_secs(30),
            cpu_budget: DEFAULT_CPU_BUDGET,
            slots: Arc::new(tokio::sync::Semaphore::new(max_requests)),
            max_requests,
            waits: Arc::default(),
        })
    }

    /// The front of the version `actors` follows (`serve --app`, ADR
    /// 0023): none until a deployed version brings one, then each version's
    /// as the service swaps it in with the classes. The slots and the
    /// counters stay across versions.
    pub fn deployed(actors: &ActorService) -> Self {
        let runtime = actors.runtime().clone();
        let pool = usize::try_from(runtime.max_instances()).unwrap_or(usize::MAX);
        let max_requests = crate::fds::default_front_requests(pool).max(1);
        Self {
            runtime,
            version: actors.front_slot(),
            egress: Arc::default(),
            guest_log: crate::guest_log::GuestLog::default(),
            wall: Duration::from_secs(30),
            cpu_budget: DEFAULT_CPU_BUDGET,
            slots: Arc::new(tokio::sync::Semaphore::new(max_requests)),
            max_requests,
            waits: Arc::default(),
        }
    }

    /// Lets the component's outbound HTTP reach what `policy` allows (by
    /// default: public addresses only).
    #[must_use]
    pub fn with_egress(mut self, policy: crate::egress::EgressPolicy) -> Self {
        self.egress = Arc::new(policy);
        self
    }

    /// Logs what the component prints within `log`'s limits (by default
    /// [`crate::guest_log::DEFAULT_RATE`] lines a second per instance).
    #[must_use]
    pub fn with_guest_log(mut self, log: crate::guest_log::GuestLog) -> Self {
        self.guest_log = log;
        self
    }

    /// The front's CPU time per request (by default
    /// [`DEFAULT_CPU_BUDGET`]), apart from the actors' budget.
    #[must_use]
    pub fn with_cpu_budget(mut self, budget: Duration) -> Self {
        self.cpu_budget = budget;
        self
    }

    /// Front requests running at once on this node (at least 1; by default
    /// the whole instance pool, for a node without actors). Past it a
    /// request gets 503 with `Retry-After: 1`, without an instance. Beside
    /// actors, give this and the actor service the same number
    /// ([`crate::ActorConfig::max_front_requests`], by default
    /// [`crate::fds::default_front_requests`]), so the slots are always
    /// free in the pool.
    #[must_use]
    pub fn with_max_requests(mut self, n: usize) -> Self {
        let n = n.max(1);
        self.max_requests = n;
        self.slots = Arc::new(tokio::sync::Semaphore::new(n));
        self
    }

    /// The front's values from this node (`--front-env`), read with
    /// `env.get`, for a front given on the command line. A deployed front
    /// takes them from [`crate::ActorConfig::front_env`] instead, merged
    /// over its deployed configuration and secrets.
    #[must_use]
    pub fn with_env(self, vars: crate::env::Vars) -> Self {
        {
            let mut version = self.version.write().unwrap_or_else(|p| p.into_inner());
            if let Some(v) = version.as_ref() {
                *version = Some(Arc::new(v.with_values(
                    Arc::new(vars),
                    Arc::clone(&v.config),
                    Arc::clone(&v.requires),
                )));
            }
        }
        self
    }

    /// Whether a front is in place (a deployed version may have none).
    pub fn has_version(&self) -> bool {
        self.version
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .is_some()
    }

    /// Front requests running at once at most.
    pub fn max_requests(&self) -> usize {
        self.max_requests
    }

    fn current(&self) -> Option<Arc<FrontVersion>> {
        self.version
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
    }

    fn host(&self, actors: Option<&ActorService>, env: Arc<crate::env::Vars>) -> FrontHost {
        FrontHost {
            wasi: self.guest_log.wasi(
                actors.map_or_else(
                    || "default/default".to_owned(),
                    |a| {
                        let id = a.metrics_identity();
                        format!("{}/{}", id.tenant, id.app)
                    },
                ),
                FRONT_CLASS.to_owned(),
            ),
            http: WasiHttpCtx::new(),
            table: ResourceTable::new(),
            egress: crate::egress::EgressHooks::new(Arc::clone(&self.egress)),
            tenant: actors.map_or_else(|| "default".to_owned(), |a| a.tenant().to_owned()),
            actors: actors.cloned(),
            env,
            deadline: tokio::time::Instant::now() + self.wall,
            waits: Arc::clone(&self.waits),
            bounds: Bounds {
                per_actor: (self.max_requests / 4).max(1),
                per_node: self.max_requests,
            },
        }
    }

    /// Creates and drops one instance without calling it: proves the
    /// component instantiates (start functions, pool capacity) and is the
    /// per-request instantiation cost, minus the request itself.
    pub async fn instantiate_once(&self) -> Result<(), Error> {
        let Some(version) = self.current() else {
            return Ok(());
        };
        self.instantiate(&version)
            .await
            .map_err(|e| Error::not_http(e.into_boxed_dyn_error()))
    }

    /// The check `serve` makes at start: [`Self::instantiate_once`], unless
    /// there is nothing to check or it would not say what requests see.
    /// `Ok(false)`: not checked, because no version brings a front, or
    /// because the front is unconfigured (a required value not set, or the
    /// app's secrets, from `actors`, not open): requests do not run it
    /// then (503), and instantiating it without its values would test
    /// something else (ADR 0023).
    pub async fn check_instantiates(&self, actors: Option<&ActorService>) -> Result<bool, Error> {
        let Some(version) = self.current() else {
            return Ok(false);
        };
        if version.unconfigured() || actors.is_some_and(ActorService::secrets_blocked) {
            return Ok(false);
        }
        self.instantiate(&version)
            .await
            .map(|()| true)
            .map_err(|e| Error::not_http(e.into_boxed_dyn_error()))
    }

    async fn instantiate(&self, version: &FrontVersion) -> wasmtime::Result<()> {
        let mut store = self.runtime.new_async_store_with_budget(
            self.host(None, Arc::clone(&version.env)),
            self.cpu_budget,
        );
        version.pre.instantiate_async(&mut store).await.map(drop)
    }

    /// Handles one request in a fresh instance, without the node's actors.
    /// Guest failures (traps, no response set) become a 500 response,
    /// never an error for the caller.
    pub async fn handle(&self, req: Request<Incoming>) -> Response<HyperOutgoingBody> {
        self.serve(req, None).await
    }

    /// Handles one request, its calls reaching `actors`.
    pub(crate) async fn serve(
        &self,
        mut req: Request<Incoming>,
        actors: Option<&ActorService>,
    ) -> Response<HyperOutgoingBody> {
        let started = Instant::now();
        // The version this request runs on, whatever is swapped in after.
        let Some(version) = self.current() else {
            return text_response(StatusCode::NOT_FOUND, "not found\n");
        };
        // A required value not set, or the app's secrets not open: not
        // instantiated, and nothing said to the client of why (ADR 0023).
        if version.unconfigured() || actors.is_some_and(ActorService::secrets_blocked) {
            outcome("unconfigured");
            return unavailable("this node cannot serve the app now; retry\n");
        }
        // Past its slots, the front is not instantiated.
        let Ok(permit) = Arc::clone(&self.slots).try_acquire_owned() else {
            outcome("busy");
            return busy();
        };
        let slot = Slot::new(permit);
        strip_wasmkeep_headers(req.headers_mut());
        let head = req.method() == hyper::Method::HEAD;
        let observe = move || METRICS.front_request_seconds.observe(started.elapsed());
        match self.try_handle(req, actors, slot, version).await {
            Ok((mut response, running)) => {
                // wasmtime-wasi-http lets a guest set any status: never an
                // upgrade, nor any other informational answer. Dropping
                // `running` stops the instance.
                if response.status().is_informational() {
                    tracing::warn!(
                        status = response.status().as_u16(),
                        "front answered with an informational status"
                    );
                    running.abort.silence();
                    drop(running);
                    outcome("guest_error");
                    observe();
                    return internal_error();
                }
                strip_wasmkeep_headers(response.headers_mut());
                // From here the instance's own end says how the request
                // went: a cut while the body streams is not `ok`.
                let Running { mut abort, sent } = running;
                let _ = sent.send(());
                // The client has its whole answer once the declared length
                // went out, or at once when the answer has no body: hyper
                // may then drop the body without reading it to its end.
                let status = response.status();
                let left = if head
                    || status == StatusCode::NO_CONTENT
                    || status == StatusCode::NOT_MODIFIED
                {
                    Some(0)
                } else {
                    response
                        .headers()
                        .get(hyper::header::CONTENT_LENGTH)
                        .and_then(|v| v.to_str().ok()?.parse::<u64>().ok())
                };
                if left == Some(0) {
                    abort.disarm();
                }
                response.map(|body| {
                    Timed {
                        inner: body,
                        observe: Some(Box::new(observe)),
                        abort,
                        left,
                    }
                    .boxed_unsync()
                })
            }
            Err(e) => {
                // The pool had no instance (it keeps the front's slots, so
                // this is brief): as busy as no slot.
                if e.downcast_ref::<wasmtime::PoolConcurrencyLimitError>()
                    .is_some()
                {
                    outcome("busy");
                    observe();
                    tracing::warn!(error = %format!("{e:#}"), "front request: no instance free");
                    return busy();
                }
                let reason = reason(&e);
                outcome(reason);
                observe();
                tracing::warn!(reason, error = %format!("{e:#}"), "front request failed");
                internal_error()
            }
        }
    }

    async fn try_handle(
        &self,
        req: Request<Incoming>,
        actors: Option<&ActorService>,
        slot: Slot,
        version: Arc<FrontVersion>,
    ) -> wasmtime::Result<(Response<HyperOutgoingBody>, Running)> {
        let mut store = self.runtime.new_async_store_with_budget(
            self.host(actors, Arc::clone(&version.env)),
            self.cpu_budget,
        );
        let (sender, receiver) = tokio::sync::oneshot::channel();
        // The client's trailers lose the host's range as its headers did.
        let req = req.map(|body| body.map_frame(strip_wasmkeep_trailers as fn(_) -> _));
        let req = store
            .data_mut()
            .http()
            .new_incoming_request(Scheme::Http, CappedBody::of(req))?;
        let out = store.data_mut().http().new_response_outparam(sender)?;
        let pre = version.pre.clone();
        // Said once the response went to the client: the instance's end is
        // then the request's outcome. Dropped otherwise.
        let (sent, was_sent) = tokio::sync::oneshot::channel::<()>();
        // Whoever counts the request first: the instance's end, or the
        // client leaving before the end of its answer.
        let counted = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let counts = Arc::clone(&counted);

        // The guest may keep streaming the body after it has set the
        // response, so it runs in its own task, which holds the slot for as
        // long as the instance lives.
        let wall = self.wall;
        let task = tokio::task::spawn(async move {
            let _slot = slot;
            let run = async move {
                let proxy = pre.instantiate_async(&mut store).await?;
                proxy
                    .wasi_http_incoming_handler()
                    .call_handle(store, req, out)
                    .await
            };
            // Past the wall the instance is dropped, which also ends a
            // response still streaming.
            let result = tokio::time::timeout(wall, run)
                .await
                .unwrap_or_else(|_| Err(WallExceeded(wall).into()));
            if was_sent.await.is_ok() && !counts.swap(true, std::sync::atomic::Ordering::AcqRel) {
                match &result {
                    Ok(()) => outcome("ok"),
                    Err(e) => {
                        let reason = reason(e);
                        outcome(reason);
                        tracing::warn!(
                            reason,
                            error = %format!("{e:#}"),
                            "front request failed after its response began"
                        );
                    }
                }
            }
            result
        });
        // A client that leaves before the answer stops the instance, and
        // frees its slot.
        let abort = AbortOnDrop {
            task: Some(task.abort_handle()),
            counted,
        };

        let Ok(received) = tokio::time::timeout(wall, receiver).await else {
            abort.silence();
            return Err(WallExceeded(wall).into());
        };
        // Every failure below is counted by the caller.
        match received {
            Ok(Ok(response)) => Ok((response, Running { abort, sent })),
            Ok(Err(code)) => {
                abort.silence();
                Err(code.into())
            }
            // The sender was dropped with the store: the guest returned or
            // trapped without setting a response.
            Err(_) => {
                abort.silence();
                drop(sent);
                let cause = match task.await {
                    Ok(Ok(())) => wasmtime::format_err!("guest returned without a response"),
                    Ok(Err(e)) => e,
                    Err(join) => join.into(),
                };
                Err(cause.context("guest did not set a response"))
            }
        }
    }
}

/// Counts one front request ([`crate::metrics::Metrics::front_requests`]).
fn outcome(o: &str) {
    inc(&METRICS.front_requests.get(&[("outcome", o)]));
}

/// Why an instance failed, as an outcome.
fn reason(e: &wasmtime::Error) -> &'static str {
    if e.downcast_ref::<CpuBudgetExceeded>().is_some() {
        "cpu_budget"
    } else if e.downcast_ref::<WallExceeded>().is_some() {
        "wall"
    } else {
        "guest_error"
    }
}

/// 503, `Retry-After: 1`: no slot or no instance for the front.
fn busy() -> Response<HyperOutgoingBody> {
    unavailable("this node is busy; retry\n")
}

/// 503 with `Retry-After: 1` and `text`.
fn unavailable(text: &str) -> Response<HyperOutgoingBody> {
    let mut r = text_response(StatusCode::SERVICE_UNAVAILABLE, text);
    r.headers_mut().insert(
        hyper::header::RETRY_AFTER,
        hyper::header::HeaderValue::from_static("1"),
    );
    r
}

/// A front instance whose response is on its way: stopped when this is
/// dropped before the response ends.
struct Running {
    abort: AbortOnDrop,
    sent: tokio::sync::oneshot::Sender<()>,
}

/// Aborts a front instance's task when dropped, unless disarmed, and
/// counts the request as `client_gone` unless it was counted already.
struct AbortOnDrop {
    task: Option<tokio::task::AbortHandle>,
    /// Set by whoever counts the request first.
    counted: Arc<std::sync::atomic::AtomicBool>,
}

impl AbortOnDrop {
    /// The client has its whole answer: the instance may finish on its
    /// own, up to its wall, and counts the request when it ends.
    fn disarm(&mut self) {
        self.task = None;
    }

    /// The caller counts the request: still aborted when dropped.
    fn silence(&self) {
        self.counted
            .store(true, std::sync::atomic::Ordering::Release);
    }
}

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
            if !self.counted.swap(true, std::sync::atomic::Ordering::AcqRel) {
                outcome("client_gone");
            }
        }
    }
}

/// A front request ran past its wall.
#[derive(Debug)]
struct WallExceeded(Duration);

impl std::fmt::Display for WallExceeded {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "request ran past {:?}", self.0)
    }
}

impl std::error::Error for WallExceeded {}

/// A body frame with the host's `x-wasmkeep-` range removed from its
/// trailers (ADR 0023).
fn strip_wasmkeep_trailers(mut frame: hyper::body::Frame<Bytes>) -> hyper::body::Frame<Bytes> {
    if let Some(trailers) = frame.trailers_mut() {
        strip_wasmkeep_headers(trailers);
    }
    frame
}

/// Removes every header of the host's `x-wasmkeep-` range (ADR 0023).
fn strip_wasmkeep_headers(headers: &mut hyper::HeaderMap) {
    let names: Vec<hyper::header::HeaderName> = headers
        .keys()
        .filter(|n| crate::actor::is_wasmkeep_header(n.as_str()))
        .cloned()
        .collect();
    for name in names {
        headers.remove(name);
    }
}

fn internal_error() -> Response<HyperOutgoingBody> {
    text_response(StatusCode::INTERNAL_SERVER_ERROR, "internal server error\n")
}

/// A response body that records the request's duration once its last
/// byte went out (or the client left), removes the host's range from the
/// guest's trailers, and stops the instance if the client leaves before
/// the end. The end is the body's last frame, its trailers, or its
/// declared length sent (wasmtime's body never says it ended on its own).
struct Timed {
    inner: HyperOutgoingBody,
    observe: Option<Box<dyn FnOnce() + Send>>,
    abort: AbortOnDrop,
    /// Bytes of the declared `Content-Length` not sent yet.
    left: Option<u64>,
}

impl Timed {
    /// The client has its whole answer.
    fn ended(&mut self) {
        self.abort.disarm();
        if let Some(observe) = self.observe.take() {
            observe();
        }
    }
}

impl Drop for Timed {
    fn drop(&mut self) {
        if let Some(observe) = self.observe.take() {
            observe();
        }
    }
}

impl hyper::body::Body for Timed {
    type Data = Bytes;
    type Error = <HyperOutgoingBody as hyper::body::Body>::Error;

    fn poll_frame(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<hyper::body::Frame<Bytes>, Self::Error>>> {
        let polled = std::pin::Pin::new(&mut self.inner).poll_frame(cx);
        match polled {
            std::task::Poll::Ready(None) => self.ended(),
            std::task::Poll::Ready(Some(Ok(frame))) => {
                if frame.is_trailers() {
                    let frame = strip_wasmkeep_trailers(frame);
                    self.ended();
                    return std::task::Poll::Ready(Some(Ok(frame)));
                }
                if let (Some(left), Some(data)) = (self.left, frame.data_ref()) {
                    let left = left.saturating_sub(data.len() as u64);
                    self.left = Some(left);
                    if left == 0 {
                        self.ended();
                    }
                }
                return std::task::Poll::Ready(Some(Ok(frame)));
            }
            _ => {}
        }
        polled
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> hyper::body::SizeHint {
        self.inner.size_hint()
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)] // test code

    use super::*;

    /// Links as a front, traps in its start function.
    const TRAPPING_FRONT: &str = r#"(component
        (import "wasi:http/types@0.2.0" (instance $types
            (export "incoming-request" (type (sub resource)))
            (export "response-outparam" (type (sub resource)))))
        (alias export $types "incoming-request" (type $req))
        (alias export $types "response-outparam" (type $out))
        (core module $m
            (func $start unreachable)
            (start $start)
            (func (export "handle") (param i32 i32)))
        (core instance $i (instantiate $m))
        (func $handle (param "request" (own $req)) (param "response-out" (own $out))
            (canon lift (core func $i "handle")))
        (instance $h (export "handle" (func $handle)))
        (export "wasi:http/incoming-handler@0.2.0" (instance $h)))"#;

    /// At start, a front whose required values are not set is not
    /// instantiated to test it: it would run without them, while requests
    /// would not run it at all (503).
    #[tokio::test]
    async fn the_start_check_waits_for_required_values() {
        let runtime = Runtime::new(&wasmkeep_runtime::RuntimeConfig::default()).unwrap();
        let wasm = wat::parse_str(TRAPPING_FRONT).unwrap();
        let component = runtime.compile(&wasm).unwrap();
        let front = HttpComponent::new(runtime, &component).unwrap();
        // Configured (nothing required): checked, and it fails.
        assert!(front.check_instantiates(None).await.is_err());
        // A required name not set: not checked.
        {
            let mut slot = front.version.write().unwrap();
            let v = slot.as_ref().unwrap();
            *slot = Some(Arc::new(v.with_values(
                Arc::default(),
                Arc::default(),
                Arc::new(["SESSION_KEY".to_owned()].into()),
            )));
        }
        assert!(!front.check_instantiates(None).await.unwrap());
        // Set: checked again.
        let front = front.with_env(crate::env::parse("SESSION_KEY=k").unwrap());
        assert!(front.check_instantiates(None).await.is_err());
        // No version: nothing to check.
        let none = HttpComponent {
            version: Arc::default(),
            ..front
        };
        assert!(!none.check_instantiates(None).await.unwrap());
    }

    /// Every name of the range goes, in any case, every value of it;
    /// names that only look alike stay.
    #[test]
    fn strip_wasmkeep_headers_removes_the_whole_range() {
        let mut headers = hyper::HeaderMap::new();
        for (k, v) in [
            ("x-wasmkeep-caller", "default/admin/root"),
            ("X-WasmKeep-Internal", "v4 0 0 0 0"),
            ("x-wasmkeep-future", "a"),
            ("x-wasmkeep-future", "b"),
            ("x-wasmkeep-kv-expect", "none"),
            ("x-wasmkeepers", "kept"),
            ("x-other", "kept"),
            ("wasmkeep-caller", "kept"),
        ] {
            headers.append(
                hyper::header::HeaderName::from_bytes(k.as_bytes()).unwrap(),
                v.parse().unwrap(),
            );
        }
        strip_wasmkeep_headers(&mut headers);
        let mut left: Vec<&str> = headers
            .keys()
            .map(hyper::header::HeaderName::as_str)
            .collect();
        left.sort_unstable();
        assert_eq!(left, ["wasmkeep-caller", "x-other", "x-wasmkeepers"]);
    }

    /// Trailers lose the range; data frames pass as they are.
    #[test]
    fn trailers_lose_the_range() {
        let mut trailers = hyper::HeaderMap::new();
        trailers.insert("x-wasmkeep-node", "forged".parse().unwrap());
        trailers.insert("x-other", "kept".parse().unwrap());
        let frame = strip_wasmkeep_trailers(hyper::body::Frame::trailers(trailers));
        let trailers = frame.into_trailers().unwrap();
        assert_eq!(trailers.len(), 1);
        assert_eq!(trailers["x-other"], "kept");
        let data = strip_wasmkeep_trailers(hyper::body::Frame::data(Bytes::from_static(b"x")));
        assert_eq!(data.into_data().unwrap(), Bytes::from_static(b"x"));
    }
}
