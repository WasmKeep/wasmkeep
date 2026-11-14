//! Who may reach an actor class from outside (ADR 0010).
//!
//! "Outside" is a request arriving at this node's front door: not a call
//! from another actor (`actors.call`), not an alarm, and not a request
//! another node of the cluster forwarded (it was checked where it arrived).

use std::collections::{HashMap, HashSet};

/// A class's access mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Access {
    /// Anyone may reach any actor of the class; a request to a new name
    /// creates it. The default.
    #[default]
    Open,
    /// Outside requests reach only actors that already exist; a new name
    /// answers 404 with nothing created (no database, no lease, nothing in
    /// the bucket). Other actors create them, with `actors.call`.
    Existing,
    /// Outside requests are refused (403); only actors and alarms reach
    /// the class.
    Internal,
}

impl std::str::FromStr for Access {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "open" => Ok(Self::Open),
            "existing" => Ok(Self::Existing),
            "internal" => Ok(Self::Internal),
            _ => Err(format!("{s:?}: expected open, existing or internal")),
        }
    }
}

impl std::fmt::Display for Access {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Open => "open",
            Self::Existing => "existing",
            Self::Internal => "internal",
        })
    }
}

/// Each class's mode; classes not listed are [`Access::Open`].
#[derive(Debug, Clone, Default)]
pub struct AccessRules {
    modes: HashMap<String, Access>,
    /// Names outside requests may create in an `existing` class: its
    /// well-known actors (`hub/main`), which nothing else would create.
    creatable: HashMap<String, HashSet<String>>,
}

impl AccessRules {
    /// Sets `class`'s mode.
    pub fn set(&mut self, class: &str, access: Access) {
        self.modes.insert(class.to_owned(), access);
    }

    /// Lets outside requests create these actors of an `existing` class
    /// (names decoded, as actors see them).
    pub fn allow_creating(&mut self, class: &str, names: impl IntoIterator<Item = String>) {
        self.creatable
            .entry(class.to_owned())
            .or_default()
            .extend(names);
    }

    /// `class`'s mode.
    #[must_use]
    pub fn of(&self, class: &str) -> Access {
        self.modes.get(class).copied().unwrap_or_default()
    }

    /// The mode for one actor: its class's, except that the names
    /// [`Self::allow_creating`] lists are open.
    #[must_use]
    pub fn for_actor(&self, class: &str, name: &str) -> Access {
        let mode = self.of(class);
        if mode == Access::Existing && self.creatable.get(class).is_some_and(|n| n.contains(name)) {
            return Access::Open;
        }
        mode
    }

    /// Parses `MODE` or `existing:NAME,NAME...` for `class` and applies it.
    ///
    /// # Errors
    /// An unknown mode, names on a mode other than `existing`, or an empty
    /// name.
    pub fn apply(&mut self, class: &str, spec: &str) -> Result<(), String> {
        let (mode, names) = match spec.split_once(':') {
            Some((mode, names)) => (mode, Some(names)),
            None => (spec, None),
        };
        let mode: Access = mode.parse()?;
        if let Some(names) = names {
            if mode != Access::Existing {
                return Err(format!(
                    "{spec:?}: only existing takes names (existing:NAME,...)"
                ));
            }
            let names: Vec<String> = names.split(',').map(str::to_owned).collect();
            if names.iter().any(String::is_empty) {
                return Err(format!("{spec:?}: an empty name"));
            }
            if names.iter().any(|n| n.chars().any(char::is_control)) {
                return Err(format!("{spec:?}: control characters in a name"));
            }
            self.allow_creating(class, names);
        }
        self.set(class, mode);
        Ok(())
    }

    /// `class`'s rule as `apply` reads it, in one canonical form (names
    /// sorted), or `None` when it is open: what a deploy manifest records.
    #[must_use]
    pub fn spec(&self, class: &str) -> Option<String> {
        let mode = self.of(class);
        if mode == Access::Open {
            return None;
        }
        let mut names: Vec<&String> = match (mode, self.creatable.get(class)) {
            (Access::Existing, Some(names)) => names.iter().collect(),
            _ => Vec::new(),
        };
        if names.is_empty() {
            return Some(mode.to_string());
        }
        names.sort();
        let names: Vec<&str> = names.into_iter().map(String::as_str).collect();
        Some(format!("{mode}:{}", names.join(",")))
    }

    /// Like [`Self::apply`], but replaces whatever `class` had (its names
    /// too) instead of adding to it.
    ///
    /// # Errors
    /// As [`Self::apply`]; `class` is then unchanged.
    pub fn replace(&mut self, class: &str, spec: &str) -> Result<(), String> {
        let mut one = Self::default();
        one.apply(class, spec)?;
        self.creatable.remove(class);
        self.modes.remove(class);
        self.modes.extend(one.modes);
        self.creatable.extend(one.creatable);
        Ok(())
    }

    /// Drops every rule of classes `keep` rejects.
    pub fn retain(&mut self, mut keep: impl FnMut(&str) -> bool) {
        self.modes.retain(|c, _| keep(c));
        self.creatable.retain(|c, _| keep(c));
    }

    /// The classes with a mode other than open, sorted (for the log).
    #[must_use]
    pub fn restricted(&self) -> Vec<(String, Access)> {
        let mut out: Vec<_> = self
            .modes
            .iter()
            .filter(|(_, a)| **a != Access::Open)
            .map(|(c, a)| (c.clone(), *a))
            .collect();
        out.sort_by(|a, b| a.0.cmp(&b.0));
        out
    }

    /// One line for the log, as a fact: which of `classes` outside
    /// requests reach, and how far (`existing` and `internal` listed after
    /// the open ones; rules of classes not in `classes` left out).
    #[must_use]
    pub fn describe(&self, classes: &[&str]) -> String {
        let mut classes = classes.to_vec();
        classes.sort_unstable();
        classes.dedup();
        let of = |mode: Access| -> Vec<&str> {
            classes
                .iter()
                .copied()
                .filter(|c| self.of(c) == mode)
                .collect()
        };
        let open = of(Access::Open);
        let mut out = format!(
            "classes open to outside requests: {}",
            if open.is_empty() {
                "none".to_owned()
            } else {
                open.join(", ")
            }
        );
        let existing: Vec<String> = of(Access::Existing)
            .into_iter()
            .map(|c| {
                let mut names: Vec<&str> = self
                    .creatable
                    .get(c)
                    .map(|n| n.iter().map(String::as_str).collect())
                    .unwrap_or_default();
                names.sort_unstable();
                if names.is_empty() {
                    c.to_owned()
                } else {
                    format!("{c} ({} created on request)", names.join(", "))
                }
            })
            .collect();
        if !existing.is_empty() {
            out.push_str("; existing actors only: ");
            out.push_str(&existing.join(", "));
        }
        let internal = of(Access::Internal);
        if !internal.is_empty() {
            out.push_str("; internal: ");
            out.push_str(&internal.join(", "));
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn modes_parse_and_default_to_open() {
        assert_eq!("existing".parse::<Access>().unwrap(), Access::Existing);
        assert_eq!("internal".parse::<Access>().unwrap(), Access::Internal);
        assert!("Open".parse::<Access>().is_err());
        let mut rules = AccessRules::default();
        rules.set("monitor", Access::Existing);
        assert_eq!(rules.of("monitor"), Access::Existing);
        assert_eq!(rules.of("hub"), Access::Open);
        assert_eq!(
            rules.restricted(),
            vec![("monitor".into(), Access::Existing)]
        );
    }

    #[test]
    fn existing_classes_may_list_names_outside_requests_create() {
        let mut rules = AccessRules::default();
        rules.apply("hub", "existing:main,room 1").unwrap();
        assert_eq!(rules.of("hub"), Access::Existing);
        assert_eq!(rules.for_actor("hub", "main"), Access::Open);
        assert_eq!(rules.for_actor("hub", "room 1"), Access::Open);
        assert_eq!(rules.for_actor("hub", "other"), Access::Existing);
        assert_eq!(rules.for_actor("monitor", "main"), Access::Open);
        assert!(rules.apply("x", "internal:main").is_err());
        assert!(rules.apply("x", "existing:a,,b").is_err());
        assert!(rules.apply("x", "existing:").is_err());
        assert!(rules.apply("x", "closed").is_err());
        assert!(rules.apply("x", "existing:a\nb").is_err());
    }

    #[test]
    fn specs_are_canonical() {
        let mut rules = AccessRules::default();
        rules.apply("hub", "existing:room 1,main,main").unwrap();
        rules.apply("monitor", "existing").unwrap();
        rules.apply("admin", "internal").unwrap();
        rules.apply("open", "open").unwrap();
        assert_eq!(rules.spec("hub").as_deref(), Some("existing:main,room 1"));
        assert_eq!(rules.spec("monitor").as_deref(), Some("existing"));
        assert_eq!(rules.spec("admin").as_deref(), Some("internal"));
        assert_eq!(rules.spec("open"), None);
        assert_eq!(rules.spec("unnamed"), None);
        let mut again = AccessRules::default();
        again.apply("hub", &rules.spec("hub").unwrap()).unwrap();
        assert_eq!(again.spec("hub"), rules.spec("hub"));
    }

    /// One line per start or deploy, as a fact: which classes outside
    /// requests reach, and how far.
    #[test]
    fn rules_are_described_in_one_line() {
        let mut rules = AccessRules::default();
        assert_eq!(
            rules.describe(&["counter"]),
            "classes open to outside requests: counter"
        );
        rules.apply("hub", "existing:main").unwrap();
        rules.apply("monitor", "existing").unwrap();
        rules.apply("admin", "internal").unwrap();
        // A rule for a class not served is not mentioned.
        rules.apply("gone", "internal").unwrap();
        assert_eq!(
            rules.describe(&["monitor", "counter", "hub", "admin", "board"]),
            "classes open to outside requests: board, counter; existing actors only: \
             hub (main created on request), monitor; internal: admin"
        );
        assert_eq!(
            rules.describe(&["admin"]),
            "classes open to outside requests: none; internal: admin"
        );
        assert_eq!(
            AccessRules::default().describe(&[]),
            "classes open to outside requests: none"
        );
    }
}
