use std::collections::BTreeMap;

use super::{Manifest, Writes};
use crate::access::AccessRules;
use crate::actor::FRONT_CLASS as FRONT;
use crate::env::Vars;

/// A version's rules and configuration.
#[derive(Clone, Default)]
pub(super) struct Settings {
    pub(super) access: AccessRules,
    pub(super) config: BTreeMap<String, Vars>,
}

/// What changes between two versions: classes, rules, and the names (never
/// the values) of configuration.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Diff {
    /// Assets added, changed, removed, or kept.
    pub assets: Option<&'static str>,
    /// Classes added.
    pub added: Vec<String>,
    /// Each added class's rule (`open` when it has none): an added class
    /// has a rule, never a change of rule.
    pub added_rules: BTreeMap<String, String>,
    /// Classes removed, with their rule if it was not `open`.
    pub removed: Vec<(String, Option<String>)>,
    /// Classes whose code changed.
    pub changed: Vec<String>,
    /// `(class, before, after)` for each kept class whose rule changes
    /// (`open` when it has none). An added class's rule is in
    /// `added_rules`.
    pub rules: Vec<(String, String, String)>,
    /// `"class: NAME added"` and the like, names only.
    pub config: Vec<String>,
    /// Kept classes the change makes more open, as `class (before -> after)`.
    pub reopened: Vec<String>,
    /// `"class: NAME now required"` and the like (ADR 0016).
    pub requires: Vec<String>,
    /// What happens to the front (ADR 0023): `added`, `changed`,
    /// `removed` or `unchanged`; `None` when neither version has one.
    pub front: Option<&'static str>,
    /// With a front after the change: the classes whose rule is `open`,
    /// reachable from outside past it.
    pub open_past_front: Vec<String>,
    /// `(class, written)`: kept classes that stay `open` but whose rule is
    /// now written in the version (`true`, a deploy from the project file,
    /// ADR 0026) or no longer written (`false`, a flag deploy after one).
    /// The rule does not change; the version does.
    pub open_written: Vec<(String, bool)>,
    /// `"class: expire after idle never -> 7d"` and the like (ADR 0028).
    pub expire: Vec<String>,
    /// `"class: strict -> batched"` and the like: a class's mode of writes
    /// changed, or an added class's when it is not strict (ADR 0034,
    /// section 15).
    pub writes: Vec<String>,
    /// Policy additions, changes and removals, without client values.
    pub limits: Vec<String>,
}

impl Diff {
    /// The rule of `class`, added by this change: `open` when it has none.
    #[must_use]
    pub fn added_rule(&self, class: &str) -> &str {
        self.added_rules.get(class).map_or("open", String::as_str)
    }

    pub(super) fn between(
        before: Option<(&Manifest, &Settings)>,
        after: (&Manifest, &Settings),
    ) -> Self {
        let empty_manifest = Manifest {
            assets: None,
            previous: None,
            classes: BTreeMap::new(),
            front: None,
            access: BTreeMap::new(),
            config: BTreeMap::new(),
            requires: BTreeMap::new(),
            expire: BTreeMap::new(),
            batched: std::collections::BTreeSet::new(),
            limits: Default::default(),
        };
        let empty = Settings::default();
        let (bm, bs) = before.unwrap_or((&empty_manifest, &empty));
        let (am, as_) = after;
        let mut diff = Self::default();
        for name in bm
            .limits
            .keys()
            .chain(am.limits.keys())
            .collect::<std::collections::BTreeSet<_>>()
        {
            let change = match (bm.limits.get(name), am.limits.get(name)) {
                (None, Some(_)) => "added",
                (Some(_), None) => "removed",
                (Some(a), Some(b)) if a != b => "changed",
                _ => continue,
            };
            diff.limits.push(format!("{name}: {change}"));
        }
        for (class, digest) in &am.classes {
            match bm.classes.get(class) {
                None => diff.added.push(class.clone()),
                Some(d) if d != digest => diff.changed.push(class.clone()),
                Some(_) => {}
            }
        }
        for class in bm.classes.keys() {
            if !am.classes.contains_key(class) {
                diff.removed.push((class.clone(), bs.access.spec(class)));
                for name in bm.requires.get(class).into_iter().flatten() {
                    diff.requires.push(format!(
                        "{class}: {name} no longer required (class removed)"
                    ));
                }
            }
        }
        diff.assets = match (&bm.assets, &am.assets) {
            (None, None) => None,
            (None, Some(_)) => Some("added"),
            (Some(_), None) => Some("removed"),
            (Some(b), Some(a)) if b != a => Some("changed"),
            _ => Some("unchanged"),
        };
        diff.front = match (&bm.front, &am.front) {
            (None, None) => None,
            (None, Some(_)) => Some("added"),
            (Some(_), None) => Some("removed"),
            (Some(b), Some(a)) if a != b => Some("changed"),
            (Some(_), Some(_)) => Some("unchanged"),
        };
        if am.front.is_some() {
            diff.open_past_front = am
                .classes
                .keys()
                .filter(|c| as_.access.of(c) == crate::access::Access::Open)
                .cloned()
                .collect();
        }
        for class in am.classes.keys().filter(|c| bm.classes.contains_key(*c)) {
            let (b, a) = (bm.access.get(class), am.access.get(class));
            match (b.map(String::as_str), a.map(String::as_str)) {
                (None, Some("open")) => diff.open_written.push((class.clone(), true)),
                (Some("open"), None) => diff.open_written.push((class.clone(), false)),
                _ => {}
            }
        }
        let open = || "open".to_owned();
        // The classes, and the front's scope when either version has a
        // front: it has values and required names, never a rule.
        let front_scope = (am.front.is_some() || bm.front.is_some()).then(|| FRONT.to_owned());
        for class in am.classes.keys().chain(front_scope.as_ref()) {
            let (b, a) = (bs.access.spec(class), as_.access.spec(class));
            if !bm.classes.contains_key(class) && class != FRONT {
                diff.added_rules
                    .insert(class.clone(), a.clone().unwrap_or_else(open));
            } else if b != a && class != FRONT {
                let (b, a) = (b.unwrap_or_else(open), a.unwrap_or_else(open));
                if more_open(&bs.access, &as_.access, class) {
                    diff.reopened.push(format!("{class} ({b} -> {a})"));
                }
                diff.rules.push((class.clone(), b, a));
            }
            let none = Vars::new();
            let (b, a) = (
                bs.config.get(class).unwrap_or(&none),
                as_.config.get(class).unwrap_or(&none),
            );
            for (name, value) in a {
                match b.get(name) {
                    None => diff.config.push(format!("{class}: {name} added")),
                    Some(v) if v != value => diff.config.push(format!("{class}: {name} changed")),
                    Some(_) => {}
                }
            }
            for name in b.keys() {
                if !a.contains_key(name) {
                    diff.config.push(format!("{class}: {name} removed"));
                }
            }
            let none = std::collections::BTreeSet::new();
            let (b, a) = (
                bm.requires.get(class).unwrap_or(&none),
                am.requires.get(class).unwrap_or(&none),
            );
            for name in a.difference(b) {
                diff.requires.push(format!("{class}: {name} now required"));
            }
            for name in b.difference(a) {
                diff.requires
                    .push(format!("{class}: {name} no longer required"));
            }
            let idle = |s: Option<&u64>| {
                s.map_or_else(
                    || "never".to_owned(),
                    |s| wasmkeep_cluster::expiry::format_idle(*s),
                )
            };
            let mode = |m: &Manifest| {
                if m.batched.contains(class) {
                    Writes::Batched
                } else {
                    Writes::Strict
                }
            };
            let (wb, wa) = (mode(bm), mode(am));
            if class != FRONT
                && (wb != wa || (wa == Writes::Batched && !bm.classes.contains_key(class)))
            {
                diff.writes.push(if bm.classes.contains_key(class) {
                    format!("{class}: {} -> {}", wb.as_str(), wa.as_str())
                } else {
                    format!("{class}: {}", wa.as_str())
                });
            }
            let (b, a) = (bm.expire.get(class), am.expire.get(class));
            if class != FRONT && a != b && (a.is_some() || bm.classes.contains_key(class)) {
                diff.expire.push(format!(
                    "{class}: expire after idle {} -> {}",
                    idle(b),
                    idle(a)
                ));
            }
        }
        diff
    }

    /// One line per change, for `wasmkeep deploy` to print.
    #[must_use]
    pub fn lines(&self) -> Vec<String> {
        let mut out = Vec::new();
        out.extend(self.front.map(|f| format!("front: {f}")));
        out.extend(
            self.added
                .iter()
                .map(|c| format!("class {c}: added ({})", self.added_rule(c))),
        );
        out.extend(self.changed.iter().map(|c| format!("class {c}: new code")));
        out.extend(self.removed.iter().map(|(c, rule)| match rule {
            Some(rule) => format!("class {c}: removed (it was {rule})"),
            None => format!("class {c}: removed"),
        }));
        out.extend(
            self.rules
                .iter()
                .map(|(c, b, a)| format!("access {c}: {b} -> {a}")),
        );
        out.extend(self.open_written.iter().map(|(c, written)| {
            if *written {
                format!("access {c}: open, now written in the version")
            } else {
                format!("access {c}: open, no longer written in the version (the same rule)")
            }
        }));
        out.extend(self.limits.iter().map(|l| format!("limit {l}")));
        out.extend(self.config.iter().map(|l| format!("config {l}")));
        out.extend(self.requires.iter().map(|l| format!("requires {l}")));
        out.extend(self.expire.iter().cloned());
        out.extend(self.writes.iter().map(|l| format!("writes {l}")));
        out.extend(
            self.open_past_front.iter().map(|c| {
                format!("class {c}: open, reachable at /actor/{c}/<name>, past the front")
            }),
        );
        out
    }
}

/// Whether `class` is more open under `after` than under `before`: a lower
/// mode (`internal` > `existing` > `open`), or more names an `existing`
/// class lets outside requests create.
fn more_open(before: &AccessRules, after: &AccessRules, class: &str) -> bool {
    use crate::access::Access;
    let rank = |a: Access| match a {
        Access::Open => 0,
        Access::Existing => 1,
        Access::Internal => 2,
    };
    let (b, a) = (before.of(class), after.of(class));
    if rank(a) != rank(b) {
        return rank(a) < rank(b);
    }
    if a == Access::Existing {
        let names = |r: &AccessRules| -> Vec<String> {
            r.spec(class)
                .and_then(|s| {
                    s.split_once(':')
                        .map(|(_, n)| n.split(',').map(str::to_owned).collect())
                })
                .unwrap_or_default()
        };
        let (bn, an) = (names(before), names(after));
        return an.iter().any(|n| !bn.contains(n));
    }
    false
}
