//! How log lines name an actor: as its app sees it, `class/name`, with the
//! app as a field of its own (`app=demo/shop actor=counter/alice`), never
//! the internal `tenant/class/name`.
//!
//! A line from an app's runtime names that runtime's app ([`named`]). A
//! line that knows only the actor's address finds the app from its tenant
//! ([`app`]): exact while the process serves one app of that tenant, and
//! listing them (`tenant/{blog,shop}`) when it serves several, so it never
//! names one app for another (ADR 0038, section 9).
//!
//! Text from outside the node (actor names, what guests print) is escaped
//! ([`push_escaped`]): Unicode controls (Cc), format characters such as the
//! bidi controls and zero-width ones (Cf), and the line and paragraph
//! separators (Zl, Zp) appear as `\u{202e}`, so such text cannot start a
//! line of its own, reorder what a terminal shows or colour it. A name with
//! a space, `=`, `"` or `\` is quoted, so it cannot add fields to its line.

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

/// The `actor` field: `class/name`, as the app names it; escaped and
/// quoted when the name needs it (see the module docs).
pub(crate) struct Actor<'a>(pub(crate) &'a ActorAddress);

impl fmt::Display for Actor<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = self.0.name();
        let plain = !name
            .chars()
            .any(|c| matches!(c, ' ' | '=' | '"' | '\\') || escaped(c));
        if plain {
            return write!(f, "{}/{name}", self.0.class());
        }
        let mut out = format!("\"{}/", self.0.class());
        for c in name.chars() {
            push_escaped(&mut out, c);
        }
        out.push('"');
        f.write_str(&out)
    }
}

/// Unicode's format characters (category Cf), as of Unicode 16.0.
const FORMAT: [(char, char); 21] = [
    ('\u{ad}', '\u{ad}'),
    ('\u{600}', '\u{605}'),
    ('\u{61c}', '\u{61c}'),
    ('\u{6dd}', '\u{6dd}'),
    ('\u{70f}', '\u{70f}'),
    ('\u{890}', '\u{891}'),
    ('\u{8e2}', '\u{8e2}'),
    ('\u{180e}', '\u{180e}'),
    ('\u{200b}', '\u{200f}'),
    ('\u{202a}', '\u{202e}'),
    ('\u{2060}', '\u{2064}'),
    ('\u{2066}', '\u{206f}'),
    ('\u{feff}', '\u{feff}'),
    ('\u{fff9}', '\u{fffb}'),
    ('\u{110bd}', '\u{110bd}'),
    ('\u{110cd}', '\u{110cd}'),
    ('\u{13430}', '\u{1343f}'),
    ('\u{1bca0}', '\u{1bca3}'),
    ('\u{1d173}', '\u{1d17a}'),
    ('\u{e0001}', '\u{e0001}'),
    ('\u{e0020}', '\u{e007f}'),
];

/// Whether the log shows `c` escaped: a control (Cc) other than tab, a
/// format character (Cf), or the line or paragraph separator (Zl, Zp).
pub(crate) fn escaped(c: char) -> bool {
    (c.is_control() && c != '\t')
        || c == '\u{2028}'
        || c == '\u{2029}'
        || (c >= '\u{ad}' && FORMAT.iter().any(|&(lo, hi)| (lo..=hi).contains(&c)))
}

/// Appends `c` as a quoted log value shows it: escaped (`\u{1b}`, `\r`) if
/// [`escaped`], `"` and `\` after a backslash, anything else as it is.
pub(crate) fn push_escaped(out: &mut String, c: char) {
    if escaped(c) || c == '"' || c == '\\' {
        out.extend(c.escape_default());
    } else {
        out.push(c);
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

    #[test]
    fn names_cannot_reorder_or_add_fields() {
        let plain = ActorAddress::new("logging-esc", "chat", "room-1").unwrap();
        assert_eq!(Actor(&plain).to_string(), "chat/room-1");
        let bidi = ActorAddress::new("logging-esc", "chat", "a\u{202e}b\u{2028}c").unwrap();
        assert_eq!(Actor(&bidi).to_string(), r#""chat/a\u{202e}b\u{2028}c""#);
        let spoof = ActorAddress::new("logging-esc", "chat", "x level=error \"q\"").unwrap();
        assert_eq!(Actor(&spoof).to_string(), r#""chat/x level=error \"q\"""#);
    }

    #[test]
    fn the_format_table_holds_what_it_names() {
        for c in [
            '\u{ad}',
            '\u{200b}',
            '\u{202a}',
            '\u{2066}',
            '\u{feff}',
            '\u{e0041}',
        ] {
            assert!(escaped(c), "{c:?}");
        }
        for c in ['a', 'é', '\t', ' ', '\u{2027}', '\u{2070}', '\u{1f600}'] {
            assert!(!escaped(c), "{c:?}");
        }
        assert!(FORMAT.windows(2).all(|w| w[0].1 < w[1].0));
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
