//! How log lines name an actor: as its app sees it, `class/name`, with the
//! app as a field of its own (`app=demo/shop actor=counter/alice`), never
//! the internal `tenant/class/name`.
//!
//! A line from an app's runtime names that runtime's app ([`named`]). A
//! line that knows only the actor's address finds the app from its tenant
//! ([`app`]): exact while the process serves one app of that tenant, and
//! listing them (`tenant/{blog,shop}`) when it serves several, so it never
//! names one app for another (ADR 0038, section 9).

use std::collections::{BTreeSet, HashMap};
use std::fmt;
use std::sync::{OnceLock, RwLock};

use wasmkeep_runtime::ActorAddress;

/// Each tenant's apps served by this process.
fn apps() -> &'static RwLock<HashMap<String, BTreeSet<String>>> {
    static APPS: OnceLock<RwLock<HashMap<String, BTreeSet<String>>>> = OnceLock::new();
    APPS.get_or_init(RwLock::default)
}

/// Names `app` as the app of `tenant`'s actors in the log.
pub(crate) fn register(tenant: &str, app: &str) {
    apps()
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .entry(tenant.to_owned())
        .or_default()
        .insert(app.to_owned());
}

/// The `app` field for an actor: `tenant/app` (`tenant/default` for a
/// tenant no service registered).
pub(crate) struct App<'a>(pub(crate) &'a ActorAddress);

impl fmt::Display for App<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let tenant = self.0.tenant();
        let apps = apps()
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match apps
            .get(tenant)
            .map(|a| a.iter().collect::<Vec<_>>())
            .as_deref()
        {
            None | Some([]) => write!(f, "{tenant}/default"),
            Some([one]) => write!(f, "{tenant}/{one}"),
            Some(several) => {
                let names: Vec<&str> = several.iter().map(|s| s.as_str()).collect();
                write!(f, "{tenant}/{{{}}}", names.join(","))
            }
        }
    }
}

/// The `app` field of a line from an app's runtime, which knows its app
/// (`tenant/app`).
pub(crate) fn named(app: &str) -> &str {
    app
}

/// The `app` field for `address` (see [`App`]).
pub(crate) fn app(address: &ActorAddress) -> App<'_> {
    App(address)
}

/// The `actor` field for `address` (see [`Actor`]).
pub(crate) fn actor(address: &ActorAddress) -> Actor<'_> {
    Actor(address)
}

/// The `actor` field: `class/name`, as the app names it.
pub(crate) struct Actor<'a>(pub(crate) &'a ActorAddress);

impl fmt::Display for Actor<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.0.class(), self.0.name())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_actor_is_named_as_its_app_sees_it() {
        let address = ActorAddress::new("logging-demo", "counter", "alice").unwrap();
        assert_eq!(Actor(&address).to_string(), "counter/alice");
        assert_eq!(App(&address).to_string(), "logging-demo/default");
        register("logging-demo", "shop");
        assert_eq!(App(&address).to_string(), "logging-demo/shop");
    }

    /// ADR 0038, section 9: two apps of one tenant in one process keep
    /// their own labels; a line that knows only the address never names
    /// one of them for the other.
    #[test]
    fn two_apps_of_one_tenant_keep_their_labels() {
        let address = ActorAddress::new("logging-two", "counter", "alice").unwrap();
        register("logging-two", "shop");
        register("logging-two", "blog");
        let shown = App(&address).to_string();
        assert_ne!(shown, "logging-two/shop");
        assert_ne!(shown, "logging-two/blog");
        assert_eq!(shown, "logging-two/{blog,shop}");
        assert_eq!(named("logging-two/blog").to_string(), "logging-two/blog");
    }
}
