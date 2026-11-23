//! Deploys on a running service: validating and swapping classes, the
//! deploy sequence, secrets, and the access rules in effect.

use super::*;

impl ActorService {
    /// Checks that each `(class, component)` is a valid actor class (the
    /// check `new` and `replace_class` make), without serving anything.
    /// `Err` names the first invalid class.
    pub fn validate(
        runtime: &Runtime,
        classes: &[(String, Component)],
    ) -> Result<(), (String, String)> {
        let linker = actor_linker(runtime).map_err(|e| (String::new(), e.to_string()))?;
        for (name, component) in classes {
            build_class(runtime, &linker, "default", name, component, &[])
                .map_err(|e| (name.clone(), format!("{e:#}")))?;
        }
        Ok(())
    }

    /// The deploy sequence this node runs (0 if its classes were not
    /// deployed with `wasmkeep deploy`).
    pub fn deploy_seq(&self) -> u64 {
        self.inner
            .deploy_seq
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Records that this node now runs deploy `seq` with the classes it
    /// has (at startup, after loading a deployed version). Actors then
    /// refuse to run on nodes behind them.
    pub fn set_deploy_seq(&self, seq: u64) {
        let mut classes = self
            .inner
            .classes
            .write()
            .unwrap_or_else(|p| p.into_inner());
        for class in classes.values_mut() {
            *class = Arc::new(ActorClass {
                seq,
                ..(**class).clone()
            });
        }
        self.inner
            .deploy_seq
            .store(seq, std::sync::atomic::Ordering::SeqCst);
    }

    /// Swaps in deploy `seq`: exactly `classes` (others are removed), all at
    /// once with the sequence, so no event sees part of a deploy or the new
    /// code under the old sequence. Everything is built first: if one class
    /// is invalid, nothing changes. A class whose bytes did not change keeps
    /// its resident instances.
    ///
    /// # Errors
    /// The first class that is not a valid actor, with why.
    pub fn apply_deploy(
        &self,
        seq: u64,
        classes: &[(String, Component, Vec<u8>)],
    ) -> Result<(), (String, Error)> {
        self.apply_version(
            seq,
            classes,
            &crate::access::AccessRules::default(),
            &std::collections::BTreeMap::new(),
        )
    }

    /// [`Self::apply_deploy`] with the version's access rules and plain
    /// configuration (ADR 0013), swapped in with its classes: an event
    /// takes a class with the rules and values it was deployed with.
    ///
    /// # Errors
    /// The first class that is not a valid actor, with why.
    pub fn apply_version(
        &self,
        seq: u64,
        classes: &[(String, Component, Vec<u8>)],
        rules: &crate::access::AccessRules,
        config: &std::collections::BTreeMap<String, crate::env::Vars>,
    ) -> Result<(), (String, Error)> {
        self.apply_version_with(
            seq,
            classes,
            rules,
            config,
            &std::collections::BTreeMap::new(),
        )
    }

    /// [`Self::apply_version`] with each class's required names (ADR
    /// 0016), swapped in with the code: a class missing one refuses its
    /// events.
    ///
    /// # Errors
    /// The first class that is not a valid actor, with why.
    pub fn apply_version_with(
        &self,
        seq: u64,
        classes: &[(String, Component, Vec<u8>)],
        rules: &crate::access::AccessRules,
        config: &std::collections::BTreeMap<String, crate::env::Vars>,
        requires: &std::collections::BTreeMap<String, std::collections::BTreeSet<String>>,
    ) -> Result<(), (String, Error)> {
        self.apply_deployed(
            seq,
            classes,
            rules,
            config,
            requires,
            &std::collections::BTreeSet::new(),
            None,
            &std::collections::BTreeMap::new(),
        )
    }

    /// [`Self::apply_version_with`] with the classes whose writes are
    /// batched (ADR 0034, section 1; every other class is strict) and the
    /// version's front (ADR 0023), swapped in with its classes under the
    /// same lock; `None` removes the node's front (requests outside
    /// `/actor/` answer 404). A front that does not link is refused like a
    /// class that does not link (as `_front`): nothing changes.
    ///
    /// # Errors
    /// The first class (or `_front`) that does not link, with why.
    #[allow(
        clippy::too_many_arguments,
        reason = "a version's parts, swapped in together under one lock"
    )]
    pub fn apply_deployed(
        &self,
        seq: u64,
        classes: &[(String, Component, Vec<u8>)],
        rules: &crate::access::AccessRules,
        config: &std::collections::BTreeMap<String, crate::env::Vars>,
        requires: &std::collections::BTreeMap<String, std::collections::BTreeSet<String>>,
        batched: &std::collections::BTreeSet<String>,
        front: Option<&crate::front::DeployedFront>,
        limits: &crate::apps::limits::Policies,
    ) -> Result<(), (String, Error)> {
        self.apply_deployed_assets(
            None, seq, classes, rules, config, requires, batched, front, limits,
        )
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "all parts of a version publish together"
    )]
    pub(crate) fn apply_deployed_assets(
        &self,
        assets: Option<crate::assets::Deployed>,
        seq: u64,
        classes: &[(String, Component, Vec<u8>)],
        rules: &crate::access::AccessRules,
        config: &std::collections::BTreeMap<String, crate::env::Vars>,
        requires: &std::collections::BTreeMap<String, std::collections::BTreeSet<String>>,
        batched: &std::collections::BTreeSet<String>,
        front: Option<&crate::front::DeployedFront>,
        limits: &crate::apps::limits::Policies,
    ) -> Result<(), (String, Error)> {
        crate::apps::limits::validate(limits)
            .map_err(|e| (FRONT_CLASS.to_owned(), Error::NotAFront(e)))?;
        // Linking is slow: done before taking the lock, the front's too.
        let front = match front {
            Some(deployed) => Some((
                crate::front::FrontVersion::link(
                    &self.inner.runtime,
                    &deployed.component,
                    Arc::default(),
                )
                .map_err(|e| (FRONT_CLASS.to_owned(), e))?,
                deployed,
            )),
            None => None,
        };
        let rules = Arc::new(rules.clone());
        // Linking is slow: done before taking the lock.
        let mut linked = Vec::with_capacity(classes.len());
        for (name, component, bytes) in classes {
            let class = build_class(
                &self.inner.runtime,
                &self.inner.linker,
                &self.inner.tenant,
                name,
                component,
                bytes,
            )
            .map_err(|e| (name.clone(), e))?;
            linked.push((name.clone(), class));
        }
        let names: Vec<&str> = classes.iter().map(|(name, _, _)| name.as_str()).collect();
        tracing::info!(seq, "access rules (deployed): {}", rules.describe(&names));
        // Values are built, and the configuration stored, under the class
        // lock, as `apply_secrets` does: a deploy and a secrets change
        // landing together each see the other's layer (ADR 0016).
        let mut map = self
            .inner
            .classes
            .write()
            .unwrap_or_else(|p| p.into_inner());
        let secrets = read_lock(&self.inner.secrets).clone();
        let mut built = HashMap::new();
        for (name, class) in linked {
            let env = class_env(
                &self.inner.node_env,
                &name,
                config.get(&name),
                secrets.class(&name),
            );
            let required = Arc::new(requires.get(&name).cloned().unwrap_or_default());
            let writes = writes_of(batched, &name);
            let apart = wasmkeep_cluster::placement::companion(&names, &name);
            let spread = wasmkeep_cluster::placement::spreads(&names, &name);
            built.insert(
                name,
                Arc::new(ActorClass {
                    seq,
                    rules: Arc::clone(&rules),
                    env,
                    requires: required,
                    writes,
                    apart,
                    spread,
                    ..class
                }),
            );
        }
        let assets = Arc::new(
            read_lock(&self.inner.assets)
                .switched(assets)
                .map_err(|e| ("_assets".into(), Error::NotAFront(e.to_string())))?,
        );
        *self.inner.assets.write().unwrap_or_else(|p| p.into_inner()) = assets.clone();
        assets.prefetch();
        *map = built;
        self.inner
            .rate_limiter
            .reconcile(limits, std::time::Instant::now());
        *self.inner.limits.write().unwrap_or_else(|p| p.into_inner()) = limits.clone();
        // The front's values from the same layers, under the same lock.
        let front = front.map(|(linked, deployed)| {
            Arc::new(front_values(
                &linked,
                deployed,
                &self.inner.front_env,
                &secrets,
            ))
        });
        *self.inner.front.write().unwrap_or_else(|p| p.into_inner()) = front;
        *self
            .inner
            .deployed_config
            .write()
            .unwrap_or_else(|p| p.into_inner()) = config.clone();
        self.inner
            .deploy_seq
            .store(seq, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }

    pub(crate) fn asset_snapshot<B>(
        &self,
        req: &mut hyper::Request<B>,
    ) -> Arc<crate::assets::State> {
        if let Some(assets) = req.extensions().get::<Arc<crate::assets::State>>() {
            return assets.clone();
        }
        let _classes = read_lock(&self.inner.classes);
        // Capture front and assets together even when the app has no ingress policies.
        req.extensions_mut().insert(crate::front::IngressFront(
            read_lock(&self.inner.front).clone(),
        ));
        read_lock(&self.inner.assets).clone()
    }

    /// Swaps in the app's secrets (ADR 0016): every class's values are
    /// rebuilt from the deployed configuration, these secrets and the
    /// node's `--env`. A class whose values changed drops its resident
    /// instances at their next event (the database is kept, `on-init` does
    /// not rerun); an event under way keeps the values it started with.
    pub fn apply_secrets(&self, secrets: crate::secrets::SecretSet) {
        let secrets = Arc::new(secrets);
        // Under the class lock, as `apply_version` does (see there).
        let mut map = self
            .inner
            .classes
            .write()
            .unwrap_or_else(|p| p.into_inner());
        *self
            .inner
            .secrets
            .write()
            .unwrap_or_else(|p| p.into_inner()) = Arc::clone(&secrets);
        let config = read_lock(&self.inner.deployed_config).clone();
        let mut changed = 0;
        for (name, class) in map.iter_mut() {
            let env = class_env(
                &self.inner.node_env,
                name,
                config.get(name),
                secrets.class(name),
            );
            if env != class.env {
                changed += 1;
                *class = Arc::new(ActorClass {
                    env,
                    // A value missing again is logged again.
                    warned: Arc::default(),
                    ..(**class).clone()
                });
            }
        }
        // The front's too, from its deployed configuration.
        let mut front = self.inner.front.write().unwrap_or_else(|p| p.into_inner());
        let mut front_changed = false;
        if let Some(version) = front.as_ref() {
            let env = front_env(&self.inner.front_env, &version.config, &secrets);
            if env != *version.env() {
                front_changed = true;
                *front = Some(Arc::new(version.with_values(
                    env,
                    Arc::clone(&version.config),
                    Arc::clone(&version.requires),
                )));
            }
        }
        drop(front);
        tracing::info!(classes_changed = changed, front_changed, "secrets applied");
    }

    /// Preserve ingress metadata through read-only routing. Admission must
    /// finish before a local mailbox is created, woken or reserved.
    pub(crate) fn prepare_ingress<B>(
        &self,
        req: &mut hyper::Request<B>,
    ) -> Option<hyper::Response<wasmtime_wasi_http::p2::body::HyperOutgoingBody>> {
        {
            let _classes = read_lock(&self.inner.classes);
            let limits = read_lock(&self.inner.limits);
            let pending = self
                .inner
                .rate_limits_pending
                .load(std::sync::atomic::Ordering::Acquire);
            if limits.is_empty() && !pending {
                if !req.uri().path().starts_with("/actor/") {
                    req.extensions_mut().insert(crate::front::IngressFront(
                        read_lock(&self.inner.front).clone(),
                    ));
                    req.extensions_mut()
                        .insert(read_lock(&self.inner.assets).clone());
                }
                return None;
            }
            if self
                .inner
                .deploy_behind
                .load(std::sync::atomic::Ordering::Acquire)
                || self
                    .inner
                    .shutting_down
                    .load(std::sync::atomic::Ordering::Acquire)
                || *self.inner.app_deleted.borrow()
            {
                return Some(crate::cluster::unavailable("app unavailable; retry\n"));
            }
        }
        if let Some(cluster) = &self.inner.cluster
            && cluster
                .is_internal(
                    req.method(),
                    req.uri().path_and_query().map_or("/", |p| p.as_str()),
                    req.headers(),
                    req.extensions()
                        .get::<crate::http::ArrivedOnClusterPort>()
                        .is_some(),
                )
                .is_some()
        {
            return self.admit_ingress(req);
        }
        if !read_lock(&self.inner.limits).is_empty()
            && !req.headers().contains_key(crate::cluster::APP)
            && !req.headers().contains_key(crate::cluster::ADMITTED)
        {
            req.headers_mut().remove(crate::cluster::FORWARDED);
        }
        if self.inner.cluster.is_some() && req.uri().path().starts_with("/actor/") {
            let mut original = hyper::Request::new(());
            *original.method_mut() = req.method().clone();
            *original.uri_mut() = req.uri().clone();
            *original.headers_mut() = req.headers().clone();
            *original.extensions_mut() = req.extensions().clone();
            req.extensions_mut()
                .insert(DeferredIngress(self.clone(), original));
            None
        } else {
            self.admit_ingress(req)
        }
    }

    /// Capture the front and admit under the same publication lock as classes.
    /// No lock is held while guest work or network I/O runs.
    pub(crate) fn admit_ingress<B>(
        &self,
        req: &mut hyper::Request<B>,
    ) -> Option<hyper::Response<wasmtime_wasi_http::p2::body::HyperOutgoingBody>> {
        use crate::http::text_response;
        use hyper::StatusCode;
        let _classes = read_lock(&self.inner.classes);
        let limits = read_lock(&self.inner.limits);
        if limits.is_empty()
            && !self
                .inner
                .rate_limits_pending
                .load(std::sync::atomic::Ordering::Acquire)
        {
            return None;
        }
        let policy_behind = self
            .inner
            .deploy_behind
            .load(std::sync::atomic::Ordering::Acquire)
            && (!limits.is_empty()
                || self
                    .inner
                    .rate_limits_pending
                    .load(std::sync::atomic::Ordering::Acquire));
        if self
            .inner
            .shutting_down
            .load(std::sync::atomic::Ordering::Acquire)
            || policy_behind
            || *self.inner.app_deleted.borrow()
        {
            return Some(crate::cluster::unavailable("app unavailable; retry\n"));
        }
        // A secrets-blocked node may still forward direct actor requests.
        // The front's established response and outcome are unchanged.
        if !req.uri().path().starts_with("/actor/") && self.secrets_blocked() {
            crate::metrics::front_request(&self.inner.app_labels, "unconfigured");
            return Some(crate::cluster::unavailable(
                "this node cannot serve the app now; retry\n",
            ));
        }
        let mut authenticated = false;
        let mut client_peer = None;
        let mut charged = false;
        if let Some(cluster) = &self.inner.cluster {
            if let Some(signed) = cluster.is_internal(
                req.method(),
                req.uri().path_and_query().map_or("/", |p| p.as_str()),
                req.headers(),
                req.extensions()
                    .get::<crate::http::ArrivedOnClusterPort>()
                    .is_some(),
            ) {
                let control = req.headers().contains_key(crate::cluster::TAKE)
                    || req.headers().contains_key(crate::cluster::ALARM_HANDOFF);
                if let Some(why) = cluster.admission_replayed(&signed, control) {
                    return Some(why.answer(req.headers()));
                }
                authenticated = true;
            }
            if !authenticated {
                if let Some((ip, already_charged)) = cluster.admission_received(req) {
                    client_peer = Some(ip);
                    charged = already_charged;
                }
                // Do not collapse forwarded clients into the forwarder's IP.
                if client_peer.is_none()
                    && (req.headers().contains_key(crate::cluster::ADMITTED)
                        || (req.headers().contains_key(crate::cluster::FORWARDED)
                            && req.headers().contains_key(crate::cluster::APP)))
                {
                    // A placed forward refused so: it never ran here.
                    return Some(crate::cluster::refuse_if_placed(
                        req.headers(),
                        crate::cluster::unavailable(
                            "cannot authenticate ingress admission; retry\n",
                        ),
                    ));
                }
            }
        }
        if !limits.is_empty() && !authenticated && !charged {
            let result = self.inner.rate_limiter.admit(
                &limits,
                req.method().as_str(),
                req.uri().path(),
                req.headers(),
                client_peer.or_else(|| {
                    req.extensions()
                        .get::<crate::http::PeerAddress>()
                        .map(|p| p.0.ip())
                }),
                std::time::Instant::now(),
            );
            crate::metrics::ingress_admission(result.as_ref().err().copied());
            if let Err(rejection) = result {
                let retry = match rejection {
                    crate::rate_limit::Rejection::InvalidKey => {
                        return Some(text_response(
                            StatusCode::BAD_REQUEST,
                            "invalid rate limit key\n",
                        ));
                    }
                    crate::rate_limit::Rejection::Quota(seconds) => seconds,
                    crate::rate_limit::Rejection::Capacity => 1,
                };
                let mut response =
                    text_response(StatusCode::TOO_MANY_REQUESTS, "rate limit exceeded\n");
                response.headers_mut().insert(
                    hyper::header::RETRY_AFTER,
                    hyper::header::HeaderValue::from(retry),
                );
                return Some(response);
            }
        }
        if !authenticated
            && (charged
                || limits
                    .values()
                    .any(|p| p.matches(req.method().as_str(), req.uri().path())))
        {
            req.extensions_mut().insert(crate::cluster::IngressCharged);
        }
        req.extensions_mut().insert(crate::front::IngressFront(
            read_lock(&self.inner.front).clone(),
        ));
        req.extensions_mut()
            .insert(read_lock(&self.inner.assets).clone());
        None
    }

    /// Whether a pending version needs ingress enforcement. Unknown manifests
    /// are treated conservatively until decoded; ordinary legacy front failures
    /// keep their previous behavior when no policy is involved.
    pub(crate) fn set_rate_limits_pending(&self, required: bool) {
        self.inner
            .rate_limits_pending
            .store(required, std::sync::atomic::Ordering::Release);
    }

    /// Whether this node cannot open the app's current secrets (ADR
    /// 0016): its front answers 503 meanwhile (ADR 0023).
    pub(crate) fn secrets_blocked(&self) -> bool {
        self.inner
            .secrets_blocked
            .load(std::sync::atomic::Ordering::Acquire)
    }

    /// Where the deployed version's front is kept, for the
    /// [`crate::HttpComponent`] that serves it.
    pub(crate) fn front_slot(&self) -> crate::front::FrontSlot {
        Arc::clone(&self.inner.front)
    }

    /// Records whether this node can open the app's current secrets (ADR
    /// 0016). While it cannot, it is not ready, refuses events for the
    /// app's classes, and in a cluster drains: it hands its actors over
    /// and takes none.
    pub fn set_secrets_blocked(&self, blocked: bool) {
        let was = self
            .inner
            .secrets_blocked
            .swap(blocked, std::sync::atomic::Ordering::AcqRel);
        if was != blocked {
            if blocked {
                tracing::error!(
                    "cannot open the app's current secrets: refusing events and handing actors over"
                );
            } else {
                tracing::info!("the app's secrets are open again");
            }
        }
        if let Some(cluster) = &self.inner.cluster {
            cluster.set_secrets_blocked(blocked);
        }
    }

    /// Notified when an actor needs a newer deploy than this node runs.
    pub fn deploy_wakeup(&self) -> Arc<tokio::sync::Notify> {
        Arc::clone(&self.inner.deploy_wakeup)
    }

    /// Adds a class, or replaces it with a new component version.
    ///
    /// Resident actors switch at their next event boundary: an event already
    /// running finishes on the old version; the next one instantiates the new
    /// component, and `on-init` runs for it. Databases, pending alarms and
    /// open WebSockets are kept. An invalid component is rejected and the
    /// current version keeps serving.
    ///
    /// For nodes that do not follow deploys (`wasmkeep dev` reloads): the
    /// new code takes the node's current deploy sequence. A node that
    /// follows deploys swaps versions with [`Self::apply_deploy`], which
    /// moves code and sequence together.
    pub fn replace_class(
        &self,
        name: &str,
        component: &Component,
        bytes: &[u8],
    ) -> Result<(), Error> {
        let class = build_class(
            &self.inner.runtime,
            &self.inner.linker,
            &self.inner.tenant,
            name,
            component,
            bytes,
        )?;
        let mut map = self
            .inner
            .classes
            .write()
            .unwrap_or_else(|p| p.into_inner());
        let seq = self
            .inner
            .deploy_seq
            .load(std::sync::atomic::Ordering::SeqCst);
        // New code, same rules and values (a reload changes only code).
        let (rules, env, requires, writes) = match map.get(name) {
            Some(old) => (
                Arc::clone(&old.rules),
                Arc::clone(&old.env),
                Arc::clone(&old.requires),
                old.writes,
            ),
            None => (
                Arc::clone(&self.inner.start_rules),
                self.inner.node_env.for_class(name),
                Arc::default(),
                crate::apps::Writes::Strict,
            ),
        };
        map.insert(
            name.to_owned(),
            Arc::new(ActorClass {
                seq,
                rules,
                env,
                requires,
                writes,
                ..class
            }),
        );
        Ok(())
    }

    /// Records whether this node is behind the deployed version because it
    /// cannot apply it (not because the bucket is unreachable): readiness
    /// then fails, so it stops deciding requests under rules the app no
    /// longer has (ADR 0013).
    pub fn set_deploy_behind(&self, behind: bool) {
        self.inner
            .deploy_behind
            .store(behind, std::sync::atomic::Ordering::Release);
    }

    /// The rules a deployed version is run with: its own once the app is
    /// marked (`marked`), else this node's `--access` if given (ADR 0013's
    /// migration), else the version's (none, for a format-1 version).
    #[must_use]
    pub fn effective_rules(
        &self,
        marked: bool,
        deployed: &crate::access::AccessRules,
    ) -> crate::access::AccessRules {
        match (&self.inner.flag_access, marked) {
            (Some(flags), false) => flags.clone(),
            (Some(_), true) => {
                tracing::warn!(
                    "access rules now come with deploys: this node's --access is ignored; \
                     remove it (it is refused at the next start)"
                );
                deployed.clone()
            }
            (None, _) => deployed.clone(),
        }
    }
}

#[cfg(test)]
mod rate_review_tests {
    use super::*;
    #[tokio::test]
    async fn assets_snapshot_precedes_a_policy_free_version_switch() {
        let dir = tempfile::tempdir().unwrap();
        let service = ActorService::new(
            Runtime::new(&Default::default()).unwrap(),
            ActorConfig::new(dir.path()),
            vec![],
        )
        .unwrap();
        let mut req = hyper::Request::get("/missing").body(()).unwrap();
        assert!(service.prepare_ingress(&mut req).is_none());
        let captured = req
            .extensions()
            .get::<Arc<crate::assets::State>>()
            .unwrap()
            .clone();
        let index = crate::assets::Index {
            not_found: "404".into(),
            ..Default::default()
        };
        let assets = crate::assets::Deployed {
            index: Arc::new(index),
            previous: None,
            app: crate::apps::AppRef::new(
                Arc::new(object_store::memory::InMemory::new()),
                "",
                "default",
                "default",
            ),
        };
        service
            .apply_deployed_assets(
                Some(assets),
                1,
                &[],
                &Default::default(),
                &Default::default(),
                &Default::default(),
                &Default::default(),
                None,
                &Default::default(),
            )
            .unwrap();
        assert!(Arc::ptr_eq(&captured, &service.asset_snapshot(&mut req)));
        assert!(captured.answer(&req).await.is_none());
        let mut next = hyper::Request::get("/missing").body(()).unwrap();
        assert_eq!(
            service
                .asset_snapshot(&mut next)
                .answer(&next)
                .await
                .unwrap()
                .status(),
            404
        );
    }

    #[tokio::test]
    async fn review_no_policy_skips_admission_and_front_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let service = ActorService::new(
            Runtime::new(&Default::default()).unwrap(),
            ActorConfig::new(dir.path()),
            vec![],
        )
        .unwrap();
        service.set_secrets_blocked(true);
        service.set_deploy_behind(true);
        let mut req = hyper::Request::get("/missing").body(()).unwrap();
        assert!(service.admit_ingress(&mut req).is_none());
        assert!(
            req.extensions()
                .get::<crate::front::IngressFront>()
                .is_none()
        );
        service
            .inner
            .shutting_down
            .store(true, std::sync::atomic::Ordering::Release);
        assert!(service.admit_ingress(&mut req).is_none());
    }
}

#[derive(Clone)]
struct DeferredIngress(ActorService, hyper::Request<()>);

pub(super) fn finish_ingress<B>(
    req: &mut hyper::Request<B>,
) -> Option<hyper::Response<wasmtime_wasi_http::p2::body::HyperOutgoingBody>> {
    let DeferredIngress(service, mut original) =
        req.extensions_mut().remove::<DeferredIngress>()?;
    let response = service.admit_ingress(&mut original);
    if response.is_none()
        && original
            .extensions()
            .get::<crate::cluster::IngressCharged>()
            .is_some()
    {
        req.extensions_mut().insert(crate::cluster::IngressCharged);
    }
    response
}
