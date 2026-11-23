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
//! Text from outside the node (actor names, what guests print, errors they
//! cause) is escaped ([`push_escaped`]) with logfmt's escapes: Unicode
//! controls (Cc, tab included), format characters such as the bidi
//! controls and zero-width ones (Cf), and the line and paragraph
//! separators (Zl, Zp) appear as `\t`, `\n` or four hex digits after `\u`,
//! so such text cannot start a line of its own, reorder what a terminal
//! shows or colour it. A value with whitespace, `=`, `"` or `\` is quoted
//! ([`text`]), so it cannot add fields to its line.

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
        if plain(name) {
            return write!(f, "{}/{name}", self.0.class());
        }
        text(format_args!("{}/{name}", self.0.class())).fmt(f)
    }
}

/// A log field's value that may carry text from outside the node (an
/// error a guest caused, a key naming an actor): shown as it is when that
/// is safe, else quoted and escaped (see the module docs).
pub(crate) fn text<T: fmt::Display>(value: T) -> Text<T> {
    Text(value)
}

/// See [`text`].
pub(crate) struct Text<T>(T);

impl<T: fmt::Display> fmt::Display for Text<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let value = self.0.to_string();
        if plain(&value) {
            return f.write_str(&value);
        }
        let mut out = String::with_capacity(value.len() + 2);
        out.push('"');
        for c in value.chars() {
            push_escaped(&mut out, c);
        }
        out.push('"');
        f.write_str(&out)
    }
}

/// Whether `value` may stand unquoted: not empty, and no whitespace, `=`,
/// `"`, `\` or character the log escapes.
fn plain(value: &str) -> bool {
    !value.is_empty()
        && !value
            .chars()
            .any(|c| c.is_whitespace() || matches!(c, '=' | '"' | '\\') || escaped(c))
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

/// Whether the log shows `c` escaped: a control (Cc, tab included), a
/// format character (Cf), or the line or paragraph separator (Zl, Zp).
pub(crate) fn escaped(c: char) -> bool {
    c.is_control()
        || c == '\u{2028}'
        || c == '\u{2029}'
        || (c >= '\u{ad}' && FORMAT.iter().any(|&(lo, hi)| (lo..=hi).contains(&c)))
}

/// Appends `c` as a quoted log value shows it, with the escapes logfmt and
/// JSON read: `\t`, `\n`, `\r`, `\"` and `\\`, and `\u001b` (four hex
/// digits, a surrogate pair above U+FFFF) for any other character
/// [`escaped`] names; anything else as it is.
pub(crate) fn push_escaped(out: &mut String, c: char) {
    match c {
        '"' => out.push_str("\\\""),
        '\\' => out.push_str("\\\\"),
        '\t' => out.push_str("\\t"),
        '\n' => out.push_str("\\n"),
        '\r' => out.push_str("\\r"),
        c if escaped(c) => {
            let mut units = [0; 2];
            for unit in c.encode_utf16(&mut units) {
                out.push_str(&format!("\\u{unit:04x}"));
            }
        }
        c => out.push(c),
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
        assert_eq!(Actor(&bidi).to_string(), r#""chat/a\u202eb\u2028c""#);
        let spoof = ActorAddress::new("logging-esc", "chat", "x level=error \"q\"").unwrap();
        assert_eq!(Actor(&spoof).to_string(), r#""chat/x level=error \"q\"""#);
        // Any whitespace, not only the ASCII space, separates fields to
        // some readers.
        for space in ['\u{a0}', '\u{3000}', '\u{2003}'] {
            let name = format!("x{space}level=error");
            let address = ActorAddress::new("logging-esc", "chat", name).unwrap();
            let shown = Actor(&address).to_string();
            assert!(shown.starts_with('"') && shown.ends_with('"'), "{shown}");
        }
    }

    /// Escapes as logfmt and JSON read them: `\uXXXX`, with a surrogate
    /// pair above the BMP, and `\t`, `\n`, `\r`, `\"`, `\\`.
    #[test]
    fn escapes_are_logfmt_escapes() {
        let mut out = String::new();
        for c in [
            '\u{1b}',
            '\t',
            '\n',
            '\r',
            '"',
            '\\',
            '\u{202e}',
            '\u{e0001}',
            'é',
        ] {
            push_escaped(&mut out, c);
        }
        assert_eq!(out, r#"\u001b\t\n\r\"\\\u202e\udb40\udc01é"#);
    }

    /// Other values that may carry outside text (an error from a guest, a
    /// key naming an actor) go through the same rules.
    #[test]
    fn text_values_are_quoted_when_they_need_it() {
        assert_eq!(text("plain-key/a").to_string(), "plain-key/a");
        assert_eq!(
            text("trap: x\nlevel=error \u{202e}").to_string(),
            r#""trap: x\nlevel=error \u202e""#
        );
        assert_eq!(text("").to_string(), r#""""#);
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
            '\t',
        ] {
            assert!(escaped(c), "{c:?}");
        }
        for c in ['a', 'é', ' ', '\u{2027}', '\u{2070}', '\u{1f600}'] {
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
