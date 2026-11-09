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
        for (class, mode) in rules.restricted() {
            tracing::info!(%class, access = %mode, seq, "access restricted");
        }
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
            built.insert(
                name,
                Arc::new(ActorClass {
                    seq,
                    rules: Arc::clone(&rules),
                    env,
                    requires: required,
                    ..class
                }),
            );
        }
        *map = built;
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
        tracing::info!(classes_changed = changed, "secrets applied");
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
        let (rules, env, requires) = match map.get(name) {
            Some(old) => (
                Arc::clone(&old.rules),
                Arc::clone(&old.env),
                Arc::clone(&old.requires),
            ),
            None => (
                Arc::clone(&self.inner.start_rules),
                self.inner.node_env.for_class(name),
                Arc::default(),
            ),
        };
        map.insert(
            name.to_owned(),
            Arc::new(ActorClass {
                seq,
                rules,
                env,
                requires,
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
