//! Actor addresses: `tenant/class/name`.

use std::fmt;

/// Longest allowed name, in bytes.
pub const MAX_NAME_LEN: usize = 256;
/// Longest allowed tenant or class, in bytes.
pub const MAX_IDENT_LEN: usize = 64;

/// Where an actor lives: `tenant/class/name`.
///
/// `tenant` and `class` are identifiers (`[a-z0-9-]`, starting with a
/// letter). `name` is chosen by tenants at runtime (room ids, user ids), so
/// it accepts any UTF-8 except `/`, control characters, and the path
/// segments `.` and `..`, up to 256 bytes.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ActorAddress {
    tenant: String,
    class: String,
    name: String,
}

/// Why an address was rejected.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid actor address: {0}")]
pub struct AddressError(&'static str);

impl ActorAddress {
    /// Builds an address from its parts, validating each.
    pub fn new(
        tenant: impl Into<String>,
        class: impl Into<String>,
        name: impl Into<String>,
    ) -> Result<Self, AddressError> {
        let (tenant, class, name) = (tenant.into(), class.into(), name.into());
        validate_ident(&tenant, "tenant")?;
        validate_ident(&class, "class")?;
        validate_name(&name)?;
        Ok(Self {
            tenant,
            class,
            name,
        })
    }

    /// Parses `tenant/class/name`.
    pub fn parse(s: &str) -> Result<Self, AddressError> {
        let mut parts = s.splitn(3, '/');
        match (parts.next(), parts.next(), parts.next()) {
            (Some(t), Some(c), Some(n)) => Self::new(t, c, n),
            _ => Err(AddressError("expected tenant/class/name")),
        }
    }

    /// The tenant.
    pub fn tenant(&self) -> &str {
        &self.tenant
    }

    /// The class (which component handles this actor).
    pub fn class(&self) -> &str {
        &self.class
    }

    /// The instance name.
    pub fn name(&self) -> &str {
        &self.name
    }
}

impl fmt::Display for ActorAddress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}/{}", self.tenant, self.class, self.name)
    }
}

fn validate_ident(s: &str, what: &'static str) -> Result<(), AddressError> {
    let ok = !s.is_empty()
        && s.len() <= MAX_IDENT_LEN
        && s.starts_with(|c: char| c.is_ascii_lowercase())
        && s.chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-');
    if ok {
        Ok(())
    } else {
        Err(AddressError(match what {
            "tenant" => "tenant must match [a-z][a-z0-9-]{0,63}",
            _ => "class must match [a-z][a-z0-9-]{0,63}",
        }))
    }
}

fn validate_name(s: &str) -> Result<(), AddressError> {
    if s.is_empty() || s.len() > MAX_NAME_LEN {
        return Err(AddressError("name must be 1 to 256 bytes"));
    }
    if s == "." || s == ".." {
        return Err(AddressError("name must not be . or .."));
    }
    if s.chars().any(|c| c == '/' || c.is_control()) {
        return Err(AddressError(
            "name must not contain / or control characters",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_and_displays() {
        let a = ActorAddress::parse("acme/chat/room-1").unwrap();
        assert_eq!(
            (a.tenant(), a.class(), a.name()),
            ("acme", "chat", "room-1")
        );
        assert_eq!(a.to_string(), "acme/chat/room-1");
    }

    #[test]
    fn names_are_permissive_but_safe() {
        for ok in [
            "user@example.com",
            "Ünïcode room",
            "a.b",
            "...",
            "x".repeat(256).as_str(),
        ] {
            assert!(ActorAddress::new("t", "c", ok).is_ok(), "{ok:?}");
        }
        for bad in ["", ".", "..", "a/b", "tab\there", "nul\0", &"x".repeat(257)] {
            assert!(ActorAddress::new("t", "c", bad).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn tenant_and_class_are_identifiers() {
        for bad in ["", "Acme", "1acme", "ac_me", "a/b", &"a".repeat(65)] {
            assert!(ActorAddress::new(bad, "c", "n").is_err(), "{bad:?}");
            assert!(ActorAddress::new("t", bad, "n").is_err(), "{bad:?}");
        }
        assert!(ActorAddress::new("acme-2", "chat-room", "n").is_ok());
    }

    #[test]
    fn parse_requires_three_parts() {
        assert!(ActorAddress::parse("acme/chat").is_err());
        // The name may not contain '/', so a fourth segment is rejected.
        assert!(ActorAddress::parse("acme/chat/a/b").is_err());
    }
}
