//! Starting the service: classes, storage, the cluster, the alarm timer
//! and what fires it.

use super::*;

impl ActorService {
    /// Links each `(class, component bytes)` against the actor world and
    /// prepares the mailboxes. Components are compiled by the caller (from
    /// the cache) and passed with their original bytes for versioning.
    pub fn new(
        runtime: Runtime,
        config: ActorConfig,
        classes: Vec<(String, Component, Vec<u8>)>,
    ) -> Result<Self, Error> {
        let linker = actor_linker(&runtime)?;

        let mut map = HashMap::new();
        let rules = Arc::new(config.access.clone());
        for (name, component, bytes) in classes {
            let class = build_class(&runtime, &linker, &config.tenant, &name, &component, &bytes)?;
            let seq = config.deploy_seq;
            let env = class_env(
                &config.env,
                &name,
                config.config.get(&name),
                config.secrets.class(&name),
            );
            let rules = Arc::clone(&rules);
            let requires = Arc::new(config.requires.get(&name).cloned().unwrap_or_default());
            map.insert(
                name,
                Arc::new(ActorClass {
                    seq,
                    rules,
                    env,
                    requires,
                    ..class
                }),
            );
        }
        let classes: Classes = Arc::new(std::sync::RwLock::new(map));

        let cluster = match &config.cluster {
            Some(c) => Some(Cluster::new(
                c,
                // Classes given on the command line run as the implicit
                // app (ADR 0022): `default/default` for `serve --actor`.
                &if c.app.is_empty() {
                    format!("{}/{}", config.tenant, crate::cluster::IMPLICIT_APP_NAME)
                } else {
                    c.app.clone()
                },
                &config.tenant,
            )?),
            None => None,
        };
        // The root of this node's actor state (ADR 0022): every key and
        // database path of an actor starts from it. Built once, by the
        // cluster when there is one.
        let root = cluster.as_ref().map_or_else(
            || wasmkeep_cluster::state_root::StateRoot::new("", config.tenant.as_str()),
            |c| c.state_root().clone(),
        );
        let factory_cluster = cluster.clone();
        // A guest statement runs on a blocking thread, which does not stop
        // when its event is cut: stop the statement no later than that.
        let mut storage_config = config.storage.clone();
        storage_config.max_statement_time =
            storage_config.max_statement_time.min(config.wall_per_event);
        let data_dir = storage_config.data_dir.clone();
        let storage = Storage::new(storage_config, crate::state_dir(&root));
        let factory_classes = Arc::clone(&classes);
        let factory_runtime = runtime.clone();
        let limits = EventLimits {
            wall: config.wall_per_event,
            retry_initial: config.alarm_retry_initial,
            retry_max: config.alarm_retry_max,
        };

        // The timer fires through a weak reference, so it does not keep the
        // service alive.
        let (alarms, fire_to) = {
            let slot: ServiceSlot = Arc::new(std::sync::OnceLock::new());
            let target = Arc::clone(&slot);
            let handle = alarm::spawn(move |address, at| {
                if let Some(inner) = target.get().and_then(std::sync::Weak::upgrade) {
                    ActorService { inner }.fire_alarm(address, at);
                }
            });
            (handle, slot)
        };

        let factory_storage = storage.clone();
        let factory_alarms = alarms.clone();
        let factory_service = Arc::clone(&fire_to);
        let sockets = Arc::new(Sockets::default());
        let factory_sockets = Arc::clone(&sockets);
        let secrets_blocked = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let factory_blocked = Arc::clone(&secrets_blocked);
        let moves: drain::Moves = Arc::default();
        let factory_moves_outer = Arc::clone(&moves);
        let factory_egress = Arc::new(config.egress.clone());
        let factory_guest_log = config.guest_log.clone();
        // Resident actors stay under the instance pool, with room for the
        // stateless component's short-lived instances: past it, the least
        // recently active idle actor hibernates to make room.
        let mut mailbox = config.mailbox;
        let pool = usize::try_from(runtime.max_instances()).unwrap_or(usize::MAX);
        // Subtract the rounded-up tenth: dividing first would collapse a
        // pool smaller than ten to one actor, preventing local actor calls.
        mailbox.max_resident = mailbox
            .max_resident
            .min(crate::fds::resident_pool_limit(pool));
        let mailboxes =
            Mailboxes::new_with_residency(mailbox, move |address: ActorAddress, residency| {
                let class = read(&factory_classes).get(address.class()).cloned();
                let (runtime, storage, alarms) = (
                    runtime.clone(),
                    factory_storage.clone(),
                    factory_alarms.clone(),
                );
                let service = Arc::clone(&factory_service);
                let sockets = Arc::clone(&factory_sockets);
                let cluster = factory_cluster.clone();
                let egress = Arc::clone(&factory_egress);
                let guest_log = factory_guest_log.clone();
                let secrets_blocked = Arc::clone(&factory_blocked);
                let factory_moves = Arc::clone(&factory_moves_outer);
                async move {
                    let class = class.ok_or_else(|| -> BoxError {
                        format!("unknown class {}", address.class()).into()
                    })?;
                    // The database path and the address recorded in it come
                    // from the root's tenant, not the address's (ADR 0022):
                    // an actor of another tenant never starts here.
                    if address.tenant() != storage.state().tenant {
                        return Err(format!("{address} is not of this node's tenant").into());
                    }
                    Ok(ActorState {
                        address,
                        class,
                        runtime,
                        storage,
                        limits,
                        alarms,
                        alarm_failures: 0,
                        service,
                        sockets,
                        chain: Arc::from(Vec::new()),
                        db: None,
                        live: None,
                        cluster,
                        lease: None,
                        owner_hint: None,
                        behind_deploy: false,
                        replica: None,
                        egress: Arc::clone(&egress),
                        guest_log,
                        residency,
                        collecting: Arc::default(),
                        secrets_blocked,
                        moves: Arc::clone(&factory_moves),
                    })
                }
            });

        let service = Self {
            inner: Arc::new(ServiceInner {
                tenant: config.tenant,
                wall: config.wall_per_event,
                sockets: Arc::clone(&sockets),
                ping_after: config.websocket_ping_after,
                close_wait: config.websocket_close_wait,
                classes,
                mailboxes,
                alarms: alarms.clone(),
                linker,
                runtime: factory_runtime,
                cluster,
                deploy_seq: std::sync::atomic::AtomicU64::new(config.deploy_seq),
                deploy_wakeup: Arc::new(tokio::sync::Notify::new()),
                node_env: config.env,
                start_rules: rules,
                flag_access: config.flag_access,
                deploy_behind: std::sync::atomic::AtomicBool::new(false),
                deployed_config: std::sync::RwLock::new(config.config.clone()),
                secrets: std::sync::RwLock::new(Arc::new(config.secrets.clone())),
                secrets_blocked,
                moves,
                shutting_down: std::sync::atomic::AtomicBool::new(false),
                disk_low: std::sync::atomic::AtomicBool::new(false),
                disk_sampled_ms: std::sync::atomic::AtomicU64::new(0),
                owner_hints: std::sync::Mutex::default(),
                handed: std::sync::Mutex::default(),
            }),
        };
        let _ = fire_to.set(Arc::downgrade(&service.inner));
        tokio::spawn(sample_disk(Arc::downgrade(&service.inner), data_dir));
        // In a cluster, deliver alarms whose owner never fired them.
        if let Some(cluster) = service.inner.cluster.clone() {
            tokio::spawn(sweep_alarms(
                Arc::downgrade(&service.inner),
                Arc::clone(&cluster),
                alarms.clone(),
                limits.retry_max,
            ));
            // While asked to drain, hand resident actors over (ADR 0014).
            tokio::spawn(drain::drain_loop(
                Arc::downgrade(&service.inner),
                Arc::clone(&cluster),
            ));
            // Close sockets whose actor this node no longer owns.
            tokio::spawn(socket_owners::close_sockets_at_lapse(
                Arc::downgrade(&sockets),
                cluster.validity.subscribe(),
            ));
            let lapsed = Arc::downgrade(&sockets);
            cluster.on_lease_lapse(Box::new(move || {
                if let Some(sockets) = lapsed.upgrade() {
                    socket_owners::close_for_lapse(&sockets);
                }
            }));
            tokio::spawn(socket_owners::sweep_sockets(
                Arc::downgrade(&service.inner),
                cluster.socket_sweep,
            ));
        }

        // Re-arm alarms persisted before a restart or crash.
        let (tenant, known) = (
            service.inner.tenant.clone(),
            Arc::clone(&service.inner.classes),
        );
        tokio::spawn(async move {
            match tokio::task::spawn_blocking(move || storage.pending_alarms()).await {
                Ok(Ok(pending)) => {
                    for e in &pending.skipped {
                        tracing::warn!(error = %e, "an actor database could not be read; its alarm, if any, is not armed");
                    }
                    for (t, class, name, at) in pending.found {
                        if t != tenant || !read(&known).contains_key(&class) {
                            continue;
                        }
                        if let Ok(address) = ActorAddress::new(t, class, name) {
                            alarms.set(address, at);
                        }
                    }
                }
                Ok(Err(e)) => tracing::warn!(error = %e, "scanning for pending alarms failed"),
                Err(e) => tracing::warn!(error = %e, "alarm scan task failed"),
            }
        });

        Ok(service)
    }

    /// Delivers an alarm as an event in the actor's mailbox.
    ///
    /// An actor another node owns is left to it (it has the alarm, committed
    /// and indexed). At capacity the delivery waits for a slot like a
    /// request does, so the slot an eviction frees is not always taken by
    /// someone else first.
    pub(super) fn fire_alarm(&self, address: ActorAddress, delivery: Delivery) {
        let inner = Arc::clone(&self.inner);
        tokio::spawn(async move {
            if let Some(owner) = inner.remote_owner(&address).await {
                inner.recheck_if_handing_over(&address, &owner);
                inner.alarms.complete(address, delivery);
                return;
            }
            let event = inner.mailboxes.submit_wait(
                &address,
                ADMISSION_WAIT,
                move |state: &mut ActorState| {
                    Box::pin(async move {
                        state.alarm(delivery.hint).await;
                        // Set when another node turned out to own it.
                        state.owner_hint.as_ref().map(|(owner, _)| owner.clone())
                    })
                },
            );
            match event.await {
                // A rejected event never reaches ActorState::retry_alarm. Keep
                // its original deadline so delivery still checks the same hint.
                Err(MailboxError::Capacity(_) | MailboxError::Full(_)) => {
                    inner.alarms.retry(address, delivery);
                }
                Ok(Some(owner)) => {
                    // A stub that only learned the owner: no slot for it.
                    inner.mailboxes.hibernate(&address);
                    inner.remember_owner(&address, &owner);
                    inner.recheck_if_handing_over(&address, &owner);
                    inner.alarms.complete(address, delivery);
                }
                _ => {
                    // A draining node handed the alarm off: nothing of the
                    // actor stays here (ADR 0014).
                    if inner.cluster.as_ref().is_some_and(|c| c.draining()) {
                        inner.mailboxes.hibernate(&address);
                    }
                    inner.alarms.complete(address, delivery);
                }
            }
        });
    }
}
