//! How log lines name an actor: as its app sees it, `class/name`, with the
//! app as a field of its own (`app=demo/shop actor=counter/alice`), never
//! the internal `tenant/class/name`.
//!
//! A process serves one app per tenant (an [`crate::ActorService`]
//! registers it when created), so the app is found from the actor's
//! tenant: a log site needs only the address.

use std::collections::HashMap;
use std::fmt;
use std::sync::{OnceLock, RwLock};

use wasmkeep_runtime::ActorAddress;

/// Each tenant's app, as `tenant/app`.
fn apps() -> &'static RwLock<HashMap<String, String>> {
    static APPS: OnceLock<RwLock<HashMap<String, String>>> = OnceLock::new();
    APPS.get_or_init(RwLock::default)
}

/// Names `app` as the app of `tenant`'s actors in the log.
pub(crate) fn register(tenant: &str, app: &str) {
    apps()
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .insert(tenant.to_owned(), format!("{tenant}/{app}"));
}

/// The `app` field for an actor: `tenant/app` (`tenant/default` for a
/// tenant no service registered).
pub(crate) struct App<'a>(pub(crate) &'a ActorAddress);

impl fmt::Display for App<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let tenant = self.0.tenant();
        match apps()
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(tenant)
        {
            Some(app) => f.write_str(app),
            None => write!(f, "{tenant}/default"),
        }
    }
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
}
