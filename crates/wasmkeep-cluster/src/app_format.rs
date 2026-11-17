//! An app's durable ownership-format setting ([ADR 0035], section 5):
//! `formats/<tenant>/<app>/recorded-head` under the bucket prefix, outside
//! both `state/` and `apps/`, so state replacement, app deletion,
//! `--recreate`, deploy restoration and component rollback never touch it.
//!
//! Absent: the app writes ownership records in version 1. The exact body
//! [`ENABLED_BODY`]: it has enabled the recorded-head format
//! (`wasmkeep-owner 2`), one way. Anything else refuses service.
//!
//! Servers and the CLI enforce it: an enabled app writes version 2
//! records and is served only by nodes that announce `recorded-head`;
//! `wasmkeep app format --enable` writes the setting.
//!
//! [ADR 0035]: ../../../docs/adr/0035-a-recorded-head-on-hand-over.md

/// The setting's body once enabled, exactly.
pub const ENABLED_BODY: &[u8] = b"wasmkeep-recorded-head 1\n";

/// The setting's key under the bucket prefix (`prefix` ends in `/` or is
/// empty).
pub fn key(prefix: &str, tenant: &str, app: &str) -> String {
    format!("{prefix}formats/{tenant}/{app}/recorded-head")
}

/// The setting's key for the app `root` names, under its bucket prefix:
/// what servers and the CLI both read.
pub fn key_of(root: &crate::state_root::StateRoot) -> String {
    key(root.prefix(), root.tenant(), root.app())
}

/// An app's ownership format.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AppFormat {
    /// No setting: version 1 records.
    Version1,
    /// Enabled: version 2 records, with the recorded head.
    RecordedHead,
}

/// The setting holds something this version cannot read.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("the app's ownership-format setting is unreadable ({len} bytes); refusing service")]
pub struct Unknown {
    /// Its length.
    pub len: usize,
}

/// The format a read of the setting gives (`None`: absent).
pub fn decode(body: Option<&[u8]>) -> Result<AppFormat, Unknown> {
    match body {
        None => Ok(AppFormat::Version1),
        Some(ENABLED_BODY) => Ok(AppFormat::RecordedHead),
        Some(other) => Err(Unknown { len: other.len() }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_setting_is_absent_exact_or_refused() {
        assert_eq!(decode(None), Ok(AppFormat::Version1));
        assert_eq!(decode(Some(ENABLED_BODY)), Ok(AppFormat::RecordedHead));
        for bad in [
            &b""[..],
            b"wasmkeep-recorded-head 1",
            b"wasmkeep-recorded-head 1\n\n",
            b"wasmkeep-recorded-head 2\n",
            b"wasmkeep-recorded-head 1\r\n",
            b" wasmkeep-recorded-head 1\n",
        ] {
            assert_eq!(
                decode(Some(bad)),
                Err(Unknown { len: bad.len() }),
                "{bad:?}"
            );
        }
    }

    /// A prefix given with or without its slash (the CLI trims it, a
    /// server's configuration may not) names the same key.
    #[test]
    fn a_prefix_names_one_key_however_it_is_written() {
        use crate::state_root::StateRoot;
        let want = "wk/formats/acme/shop/recorded-head";
        for prefix in ["wk", "wk/"] {
            assert_eq!(key_of(&StateRoot::new(prefix, "acme", "shop")), want);
        }
        assert_eq!(
            key_of(&StateRoot::new("", "acme", "shop")),
            "formats/acme/shop/recorded-head"
        );
    }

    #[test]
    fn the_key_is_outside_state_and_apps() {
        let k = key("p/", "acme", "shop");
        assert_eq!(k, "p/formats/acme/shop/recorded-head");
        let root = crate::state_root::StateRoot::new("p/", "acme", "shop");
        for family in crate::state_root::Family::ALL {
            assert!(!k.starts_with(&root.key(family, "")), "{family:?}");
        }
        assert!(!k.starts_with("p/apps/") && !k.starts_with("p/state/"));
    }
}
