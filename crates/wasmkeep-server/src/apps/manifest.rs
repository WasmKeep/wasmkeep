use std::collections::BTreeMap;

use sha2::{Digest, Sha256};

use super::{ROOTED_TOKEN, Writes, hex, limits};
use crate::access::AccessRules;
use crate::actor::FRONT_CLASS as FRONT;
use crate::env::Vars;

/// What `current` points at.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Current {
    /// Content-derived version id.
    pub version: String,
    /// Deploy sequence number; increases with every change, rollbacks too.
    pub seq: u64,
    /// A version with access rules or configuration has been current
    /// (ADR 0013): written as a third field, which tools and nodes from
    /// before refuse, and kept by every later deploy and rollback.
    pub marked: bool,
    /// The app's actor state lives under its own root (ADR 0022): token
    /// `3`, which implies `marked` and which versions before 0.4.0 cannot
    /// read, so their `serve --app` and `deploy` stop. Written by every
    /// deploy and rollback of this version, and by `cluster migrate`.
    pub rooted: bool,
}

impl Current {
    /// Reads the body of `apps/<tenant>/<app>/current`:
    /// `"<version> <seq>"`, `"<version> <seq> 2"` once marked, or
    /// `"<version> <seq> 3"` once rooted (and so marked). `None` if it is
    /// anything else.
    pub fn parse(body: &[u8]) -> Option<Self> {
        let text = String::from_utf8_lossy(body);
        let mut parts = text.split_whitespace();
        let (Some(version), Some(seq), marked, None) = (
            parts.next(),
            parts.next().and_then(|s| s.parse().ok()),
            parts.next(),
            parts.next(),
        ) else {
            return None;
        };
        let (marked, rooted) = match marked {
            None => (false, false),
            Some("2") => (true, false),
            Some(ROOTED_TOKEN) => (true, true),
            Some(_) => return None,
        };
        Some(Self {
            version: version.to_owned(),
            seq,
            marked,
            rooted,
        })
    }

    /// The stored form.
    pub fn format(&self) -> String {
        let Self {
            version,
            seq,
            marked,
            rooted,
        } = self;
        if *rooted {
            format!("{version} {seq} {ROOTED_TOKEN}\n")
        } else if *marked {
            format!("{version} {seq} 2\n")
        } else {
            format!("{version} {seq}\n")
        }
    }
}

/// What `wasmkeep deploy` changes (ADR 0013).
#[derive(Default)]
pub struct Changes {
    /// Keep, replace, or remove the static assets index.
    pub assets: crate::assets::Change,
    /// Read contents back as well as checking their sizes.
    pub assets_verify: bool,
    /// The new class table; `None` keeps the current classes.
    pub classes: Option<Vec<(String, Vec<u8>)>>,
    /// `(class, spec)`: sets the class's rule (`open` removes it).
    pub access: Vec<(String, String)>,
    /// `(class, values)`: replaces the class's configuration (`None`, or
    /// no values, removes it).
    pub config: Vec<(String, Option<Vars>)>,
    /// `(class, names)`: replaces the names the class requires among its
    /// values (ADR 0016); empty removes them.
    pub requires: Vec<(String, std::collections::BTreeSet<String>)>,
    /// `(class, limit)`: sets how long the class's actors may stay idle
    /// before they are deleted, in seconds (ADR 0028); `None` removes it.
    pub expire: Vec<(String, Option<u64>)>,
    /// `(class, mode)`: sets how the class's plain writes reach the bucket
    /// (ADR 0034). Classes not named keep theirs.
    pub writes: Vec<(String, Writes)>,
    /// None preserves policies; Some replaces the complete policy set.
    pub limits: Option<limits::Policies>,
    /// The front (ADR 0023): kept by default.
    pub front: FrontChange,
    /// The front's configuration (`--front-config`): `None` keeps it,
    /// `Some(None)` (or no values) removes it.
    pub front_config: Option<Option<Vars>>,
    /// The names the front requires (`--front-requires`): `None` keeps
    /// them, an empty set removes them.
    pub front_requires: Option<std::collections::BTreeSet<String>>,
    /// Write every class's rule in the manifest, `open` included (a deploy
    /// from the project file, ADR 0026); else `open` is written as
    /// absence, as flag deploys write it.
    pub every_rule: bool,
}

/// A part of a whole version (a deploy from the project file, ADR 0026):
/// its component, configuration and required names.
pub struct Part {
    pub wasm: Vec<u8>,
    /// `None` removes the part's configuration.
    pub config: Option<Vars>,
    /// Empty removes them.
    pub requires: std::collections::BTreeSet<String>,
    /// A class's idle limit in seconds (ADR 0028); `None`: never. Ignored
    /// for the front.
    pub expire: Option<u64>,
    /// How a class's plain writes reach the bucket (ADR 0034). Ignored for
    /// the front.
    pub writes: Writes,
}

impl std::fmt::Debug for Part {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Part")
            .field("wasm", &format_args!("{} bytes", self.wasm.len()))
            .field(
                "config",
                &self.config.as_ref().map(|v| v.keys().collect::<Vec<_>>()),
            )
            .field("requires", &self.requires)
            .field("expire", &self.expire)
            .field("writes", &self.writes)
            .finish()
    }
}

impl Changes {
    /// The whole version, as a project file gives it (ADR 0026): exactly
    /// these classes (`(name, rule, part)`, every rule written, `open`
    /// included), values and required names removed where a part has
    /// none, the front set or removed.
    #[must_use]
    pub fn whole(front: Option<Part>, classes: Vec<(String, String, Part)>) -> Self {
        let mut changes = Self {
            classes: Some(Vec::new()),
            every_rule: true,
            assets: crate::assets::Change::Remove,
            limits: Some(Default::default()),
            ..Self::default()
        };
        for (name, access, part) in classes {
            changes.access.push((name.clone(), access));
            changes.config.push((name.clone(), part.config));
            changes.requires.push((name.clone(), part.requires));
            changes.expire.push((name.clone(), part.expire));
            changes.writes.push((name.clone(), part.writes));
            if let Some(table) = changes.classes.as_mut() {
                table.push((name, part.wasm));
            }
        }
        match front {
            Some(part) => {
                changes.front = FrontChange::Set(part.wasm);
                changes.front_config = Some(part.config);
                changes.front_requires = Some(part.requires);
            }
            None => changes.front = FrontChange::Remove,
        }
        changes
    }
}

/// What a deploy does with the front (ADR 0023).
#[derive(Default)]
pub enum FrontChange {
    /// The current front, if any, copied inside the bucket.
    #[default]
    Keep,
    /// This component (`--front PATH`).
    Set(Vec<u8>),
    /// None (`--no-front`): its configuration and required names go too.
    Remove,
}

impl std::fmt::Debug for FrontChange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Keep => f.write_str("Keep"),
            Self::Set(bytes) => write!(f, "Set({} bytes)", bytes.len()),
            Self::Remove => f.write_str("Remove"),
        }
    }
}

impl std::fmt::Debug for Changes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let classes: Option<Vec<&str>> = self
            .classes
            .as_ref()
            .map(|c| c.iter().map(|(n, _)| n.as_str()).collect());
        let config: Vec<(&str, Option<Vec<&str>>)> = self
            .config
            .iter()
            .map(|(c, v)| {
                (
                    c.as_str(),
                    v.as_ref().map(|v| v.keys().map(String::as_str).collect()),
                )
            })
            .collect();
        f.debug_struct("Changes")
            .field("classes", &classes)
            .field("access", &self.access)
            .field("config", &config)
            .field("front", &self.front)
            .field("every_rule", &self.every_rule)
            .finish_non_exhaustive()
    }
}

/// What a node runs for one version (ADR 0013). Its `Debug` shows names
/// and sizes, never a configuration value.
#[derive(Clone)]
pub struct Version {
    /// Verified static assets of this version.
    pub assets: Option<crate::assets::Deployed>,
    /// Each class's component bytes.
    pub classes: Vec<(String, Vec<u8>)>,
    /// The version's access rules (classes not listed are open).
    pub access: AccessRules,
    /// Each class's plain configuration (classes not listed have none).
    pub config: BTreeMap<String, Vars>,
    /// Each class's required names (ADR 0016; classes not listed require
    /// none).
    pub requires: BTreeMap<String, std::collections::BTreeSet<String>>,
    /// Each class's idle limit in seconds (ADR 0028; classes not listed
    /// never expire).
    pub expire: BTreeMap<String, u64>,
    /// The classes whose writes are batched (ADR 0034; every other class
    /// is strict).
    pub batched: std::collections::BTreeSet<String>,
    /// Node-local ingress policies (ADR 0036).
    pub limits: limits::Policies,
    /// The version's front (ADR 0023), if it has one. Its values are not
    /// in `config` and `requires`, which hold classes only.
    pub front: Option<VersionFront>,
}

/// A version's front as the bucket holds it. Its `Debug` shows sizes and
/// names, never a value.
#[derive(Clone)]
pub struct VersionFront {
    /// The component bytes, checked against the manifest's digest.
    pub wasm: Vec<u8>,
    /// Its plain configuration (`_front.config`).
    pub config: Vars,
    /// The names it requires among its values.
    pub requires: std::collections::BTreeSet<String>,
}

impl std::fmt::Debug for Version {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let classes: Vec<(&str, usize)> = self
            .classes
            .iter()
            .map(|(n, b)| (n.as_str(), b.len()))
            .collect();
        let config: Vec<(&str, Vec<&str>)> = self
            .config
            .iter()
            .map(|(c, v)| (c.as_str(), v.keys().map(String::as_str).collect()))
            .collect();
        f.debug_struct("Version")
            .field("classes", &classes)
            .field("access", &self.access)
            .field("config", &config)
            .field(
                "front",
                &self.front.as_ref().map(|f| {
                    (
                        f.wasm.len(),
                        f.config.keys().map(String::as_str).collect::<Vec<_>>(),
                    )
                }),
            )
            .finish()
    }
}

impl std::fmt::Debug for VersionFront {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VersionFront")
            .field("wasm", &self.wasm.len())
            .field("config", &self.config.keys().collect::<Vec<_>>())
            .field("requires", &self.requires)
            .finish()
    }
}

/// A `.config` object's text: one `NAME='value'` line per name, sorted.
/// [`crate::env::parse`] reads it back unchanged: it removes one pair of
/// quotes and interprets nothing inside, and values have no line breaks.
#[must_use]
pub fn format_config(vars: &Vars) -> String {
    vars.iter().map(|(n, v)| format!("{n}='{v}'\n")).collect()
}

/// Parses a `.config` object: an `--env` file's format, at most
/// [`crate::env::MAX_FILE`] bytes. The error is `<line>: <problem>`, never
/// a value.
pub(super) fn parse_config(bytes: &[u8]) -> Result<Vars, String> {
    if bytes.len() as u64 > crate::env::MAX_FILE {
        return Err(format!(" larger than {} bytes", crate::env::MAX_FILE));
    }
    let text = std::str::from_utf8(bytes).map_err(|_| " not UTF-8".to_owned())?;
    crate::env::parse(text)
}

/// A version's manifest: its classes, their access rules and
/// configuration digests, and the version before it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Manifest {
    /// SHA-256 of the canonical static assets index (format 7).
    pub assets: Option<String>,
    /// The version that was current when this one was deployed.
    pub previous: Option<String>,
    /// Class name -> SHA-256 of its component.
    pub classes: BTreeMap<String, String>,
    /// SHA-256 of the front's component (`front main <sha256>`, ADR 0023):
    /// written in format 3 only, so a version without a front keeps its
    /// bytes and id.
    pub front: Option<String>,
    /// Class name -> its access rule in canonical form
    /// ([`AccessRules::spec`]); open classes are not listed.
    pub access: BTreeMap<String, String>,
    /// Class name (or `_front`, with a front) -> SHA-256 of its `.config`
    /// object.
    pub config: BTreeMap<String, String>,
    /// Class name (or `_front`, with a front) -> the names it requires among its values (ADR 0016):
    /// a class missing one refuses its events.
    pub requires: BTreeMap<String, std::collections::BTreeSet<String>>,
    /// Class name -> how long its actors may stay idle before they are
    /// deleted, in seconds (`expire <class> <secs>`, ADR 0028): written in
    /// format 4 only, so a version without one keeps its bytes and id.
    pub expire: BTreeMap<String, u64>,
    /// Classes whose writes are batched (`writes <class> batched`, ADR
    /// 0034, section 15): written in format 5 only. `strict`, the default,
    /// is written as absence, said or not, so a version of strict classes
    /// keeps its format, bytes and id.
    pub batched: std::collections::BTreeSet<String>,
    /// Node-local ingress policies (ADR 0036).
    pub limits: limits::Policies,
}

impl Manifest {
    fn table(classes: &BTreeMap<String, String>) -> String {
        classes
            .iter()
            .map(|(n, d)| format!("class {n} {d}\n"))
            .collect()
    }

    /// The lines that make the version: classes, then rules, then
    /// configuration digests. Without rules or configuration, exactly the
    /// class table, so versions deployed before ADR 0013 keep their id.
    pub(super) fn identity(&self) -> String {
        let mut out = Self::table(&self.classes);
        if let Some(digest) = &self.front {
            out.push_str(&format!("front main {digest}\n"));
        }
        if let Some(digest) = &self.assets {
            out.push_str(&format!("assets {digest}\n"));
        }
        for (class, spec) in &self.access {
            out.push_str(&format!("access {class} {spec}\n"));
        }
        for (class, digest) in &self.config {
            out.push_str(&format!("config {class} {digest}\n"));
        }
        for (class, names) in &self.requires {
            for name in names {
                out.push_str(&format!("requires {class} {name}\n"));
            }
        }
        for (class, secs) in &self.expire {
            out.push_str(&format!("expire {class} {secs}\n"));
        }
        for class in &self.batched {
            out.push_str(&format!("writes {class} batched\n"));
        }
        for (name, policy) in &self.limits {
            out.push_str(&policy.line(name));
        }
        out
    }

    /// The version id: the identity and the version it replaced, so a
    /// directory is never reused under another lineage (ADR 0013).
    #[must_use]
    pub fn version_id(&self) -> String {
        let mut text = self.identity();
        if let Some(p) = &self.previous {
            text.push_str(&format!("previous {p}\n"));
        }
        hex(&Sha256::digest(text.as_bytes()))[..16].to_owned()
    }

    /// 6 with ingress policies (ADR 0036); 5 with a batched class (ADR
    /// 0034); 4 with an idle limit (ADR
    /// 0028); 3 with a front, or an explicit `open` rule (ADR 0023); else 1
    /// when there are neither rules nor configuration (the form from before
    /// ADR 0013, kept as the minimal one), else 2.
    #[must_use]
    pub fn format_number(&self) -> u8 {
        if self.assets.is_some() {
            7
        } else if !self.limits.is_empty() {
            6
        } else if !self.batched.is_empty() {
            5
        } else if !self.expire.is_empty() {
            4
        } else if self.front.is_some() || self.access.values().any(|s| s == "open") {
            3
        } else if self.access.is_empty() && self.config.is_empty() && self.requires.is_empty() {
            1
        } else {
            2
        }
    }

    pub(super) fn format(&self) -> String {
        let mut out = format!("wasmkeep-manifest {}\n", self.format_number());
        if let Some(p) = &self.previous {
            out.push_str(&format!("previous {p}\n"));
        }
        out.push_str(&self.identity());
        out
    }

    /// Decode a deploy manifest without contacting or mutating the app.
    pub fn parse(text: &str) -> Option<Self> {
        let mut lines = text.lines();
        let format = match lines.next()? {
            "wasmkeep-manifest 1" => 1,
            "wasmkeep-manifest 2" => 2,
            "wasmkeep-manifest 3" => 3,
            "wasmkeep-manifest 4" => 4,
            "wasmkeep-manifest 5" => 5,
            "wasmkeep-manifest 6" => 6,
            "wasmkeep-manifest 7" => 7,
            _ => return None,
        };
        let mut manifest = Self {
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
        for line in lines {
            if line.starts_with("limit ") {
                if format < 6 {
                    return None;
                }
                let (name, policy) = limits::Policy::from_line(line)?;
                if manifest.limits.insert(name, policy).is_some() {
                    return None;
                }
                limits::validate(&manifest.limits).ok()?;
                continue;
            }
            // `access` keeps the rest of the line: names may hold spaces.
            if format >= 2
                && let Some(rest) = line.strip_prefix("access ")
            {
                let (class, spec) = rest.split_once(' ')?;
                // Format 3 also says `open` (ADR 0023), which earlier
                // formats write as absence.
                if format >= 3 && spec == "open" && !class.is_empty() {
                    manifest.access.insert(class.to_owned(), spec.to_owned());
                    continue;
                }
                let mut check = AccessRules::default();
                check.replace(class, spec).ok()?;
                if class.is_empty() || check.spec(class).as_deref() != Some(spec) {
                    return None; // open, or not canonical
                }
                manifest.access.insert(class.to_owned(), spec.to_owned());
                continue;
            }
            let mut parts = line.split_whitespace();
            match (parts.next(), parts.next(), parts.next(), parts.next()) {
                (Some("assets"), Some(d), None, None)
                    if format == 7
                        && manifest.assets.is_none()
                        && crate::assets::valid_digest(d) =>
                {
                    manifest.assets = Some(d.into());
                }
                (Some("previous"), Some(v), None, None) => manifest.previous = Some(v.to_owned()),
                // `_front` is never a class (no class of that name ever
                // linked, in any format): it names the front.
                (Some("class"), Some(n), Some(d), None) if n != FRONT => {
                    manifest.classes.insert(n.to_owned(), d.to_owned());
                }
                // At most one front, named `main`.
                (Some("front"), Some("main"), Some(d), None)
                    if format >= 3 && manifest.front.is_none() =>
                {
                    manifest.front = Some(d.to_owned());
                }
                (Some("config"), Some(n), Some(d), None) if format >= 2 => {
                    manifest.config.insert(n.to_owned(), d.to_owned());
                }
                // A class's idle limit (ADR 0028), once per class.
                (Some("expire"), Some(n), Some(secs), None)
                    if format >= 4 && !manifest.expire.contains_key(n) =>
                {
                    let secs: u64 = secs.parse().ok()?;
                    if !(wasmkeep_cluster::expiry::MIN_IDLE_SECS
                        ..=wasmkeep_cluster::expiry::MAX_IDLE_SECS)
                        .contains(&secs)
                    {
                        return None;
                    }
                    manifest.expire.insert(n.to_owned(), secs);
                }
                // A batched class (ADR 0034), once per class. `strict` is
                // never written: it is the absence of the line.
                (Some("writes"), Some(n), Some("batched"), None)
                    if format >= 5 && !manifest.batched.contains(n) =>
                {
                    manifest.batched.insert(n.to_owned());
                }
                (Some("requires"), Some(n), Some(name), None)
                    if format >= 2 && crate::env::valid_name(name) =>
                {
                    manifest
                        .requires
                        .entry(n.to_owned())
                        .or_default()
                        .insert(name.to_owned());
                }
                _ => return None,
            }
        }
        if format == 7 && manifest.assets.is_none() {
            return None;
        }
        let known = |c: &String| manifest.classes.contains_key(c);
        // The front's scope holds values and required names, never a rule,
        // and only beside a `front` line.
        let scoped = |c: &String| known(c) || (manifest.front.is_some() && c == FRONT);
        if !manifest.access.keys().all(known)
            || !manifest.expire.keys().all(known)
            || !manifest.batched.iter().all(known)
            || !manifest.config.keys().all(scoped)
            || !manifest.requires.keys().all(scoped)
        {
            return None;
        }
        Some(manifest)
    }
}
