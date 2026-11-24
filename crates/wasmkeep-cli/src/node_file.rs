//! The operator's node file, `wasmkeep-node.toml` (ADR 0038, section 4):
//! which apps this process serves and how. `serve --node-file FILE` reads
//! it; `serve --app` is translated into a one-entry file with the same
//! meaning ([`NodeFile::from_serve`]).
//!
//! Until a process can serve several apps, a file lists exactly one
//! `[[app]]`: a second is refused, after every other check of the file.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};

/// The parsed file.
#[derive(Debug, Clone, Default)]
pub(crate) struct NodeFile {
    /// Where it was read from (for errors), or `the command line`.
    pub(crate) path: PathBuf,
    pub(crate) process: Process,
    pub(crate) listen: Listen,
    pub(crate) limits: Limits,
    pub(crate) apps: Vec<AppEntry>,
}

/// `[process]`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct Process {
    /// The process identity, `[a-z0-9-]` (published from a later version).
    pub(crate) id: Option<String>,
    /// Every app's data directory unless its entry names its own.
    pub(crate) data_dir: Option<PathBuf>,
    /// `placement` (ADR 0041, section 9): `Some(false)` turns placement at
    /// wake-up off for every app of the process; `None`: on.
    pub(crate) placement: Option<bool>,
    /// `idle-compaction` (ADR 0042): `Some(false)` turns idle compaction
    /// off for every app of the process, as `serve --no-idle-compaction`
    /// does; `None`: on.
    pub(crate) idle_compaction: Option<bool>,
}

/// `[listen]`: the three listeners, and the addresses peers use.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct Listen {
    pub(crate) public: Option<SocketAddr>,
    pub(crate) public_advertise: Option<String>,
    pub(crate) cluster: Option<SocketAddr>,
    pub(crate) cluster_advertise: Option<String>,
    pub(crate) metrics: Option<SocketAddr>,
    /// `metrics-hosts`: names the metrics page answers to besides IP
    /// addresses and `localhost`, normalized.
    pub(crate) metrics_hosts: Vec<String>,
}

/// `[limits]`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct Limits {
    pub(crate) max_sockets_per_node: Option<usize>,
    /// `sqlite-heap-bytes`: SQLite's heap for the whole process, as
    /// `--sqlite-heap-bytes`.
    pub(crate) sqlite_heap_bytes: Option<u64>,
}

/// One `[[app]]`.
#[derive(Debug, Clone, Default)]
pub(crate) struct AppEntry {
    /// Where the entry starts (`path:line:column`), for errors that
    /// compare entries.
    pub(crate) at: String,
    pub(crate) tenant: String,
    pub(crate) name: String,
    pub(crate) node_id: Option<String>,
    /// Normalized: lowercase, no trailing dot.
    pub(crate) hostnames: Vec<String>,
    pub(crate) secrets_keys: Vec<PathBuf>,
    pub(crate) no_secrets: bool,
    pub(crate) env: Vec<(String, PathBuf)>,
    pub(crate) front_env: Vec<PathBuf>,
    pub(crate) access: Vec<(String, String)>,
    pub(crate) allow_outbound: Vec<wasmkeep_server::IpRange>,
    pub(crate) allow_outbound_port: Vec<u16>,
    pub(crate) outbound_ca: Vec<PathBuf>,
    pub(crate) max_sockets: Option<usize>,
    pub(crate) min_resident: Option<usize>,
    pub(crate) max_resident: Option<usize>,
    pub(crate) max_front_requests: Option<usize>,
    pub(crate) front_cpu_budget_ms: Option<u64>,
    pub(crate) data_dir: Option<PathBuf>,
    pub(crate) no_alarms: bool,
    /// Where its node links point (ADR 0030, section 4).
    pub(crate) blob_link_base: Option<String>,
    /// Its stored-bytes limit (ADR 0030, section 7).
    pub(crate) max_app_blob_bytes: Option<u64>,
}

/// Reads and checks the file at `path`.
pub(crate) fn read(path: &Path) -> anyhow::Result<NodeFile> {
    let text = std::fs::read_to_string(path)
        .map_err(|e| anyhow::anyhow!("reading the node file {}: {e}", path.display()))?;
    parse(&text, path).map_err(|e| anyhow::anyhow!("{e}"))
}

/// Parses and checks `text`, read from `path`: [`parse_any`], then a
/// second `[[app]]` refused.
pub(crate) fn parse(text: &str, path: &Path) -> Result<NodeFile, String> {
    parse_any(text, path)
}

/// The top-level tables, and the keys of each.
const TOP: &[&str] = &["process", "listen", "limits", "app"];
const PROCESS: &[&str] = &["id", "data-dir", "placement", "idle-compaction"];
const LISTEN: &[&str] = &[
    "public",
    "public-advertise",
    "cluster",
    "cluster-advertise",
    "metrics",
    "metrics-hosts",
];
const LIMITS: &[&str] = &["max-sockets-per-node", "sqlite-heap-bytes"];
const APP: &[&str] = &[
    "name",
    "node-id",
    "hostnames",
    "secrets-key",
    "no-secrets",
    "env",
    "front-env",
    "access",
    "allow-outbound",
    "allow-outbound-port",
    "outbound-ca",
    "max-sockets",
    "min-resident",
    "max-resident",
    "max-front-requests",
    "front-cpu-budget-ms",
    "data-dir",
    "no-alarms",
    "blob-link-base",
    "max-app-blob-bytes",
];

/// The node socket ceiling when `[limits]` names none (as
/// `--max-sockets-per-node`).
const DEFAULT_NODE_SOCKETS: usize = 50_000;

/// Parses `text` and checks every rule of the file, however many apps it
/// lists: unknown keys, invalid values, and between entries duplicates,
/// overlapping local roots and a tenant's layout from before 0.4.0.
pub(crate) fn parse_any(text: &str, path: &Path) -> Result<NodeFile, String> {
    use crate::project::{Source, get, known_keys, table};
    use toml::de::{DeTable, DeValue};
    let src = Source { text, path };
    let root = DeTable::parse(text).map_err(|e| match e.span() {
        Some(span) => format!("{}: {}", src.at(span), e.message().trim()),
        None => format!("{}: {}", path.display(), e.message().trim()),
    })?;
    let root = root.get_ref();
    let dir = match path.parent() {
        Some(d) if !d.as_os_str().is_empty() => d.to_path_buf(),
        _ => PathBuf::from("."),
    };
    known_keys(&src, root, "the file", TOP)?;
    let mut file = NodeFile {
        path: path.to_path_buf(),
        ..NodeFile::default()
    };

    if let Some(t) = table(&src, root, "process")? {
        known_keys(&src, t, "[process]", PROCESS)?;
        if let Some((id, span)) = string_at(&src, t, "id", "[process]")? {
            if wasmkeep_cluster::membership::validate_node_id(&id).is_err() {
                return src.err(
                    span,
                    format!("[process] id {id:?}: use 1 to 64 of [a-z0-9-]"),
                );
            }
            file.process.id = Some(id);
        }
        file.process.data_dir =
            string_at(&src, t, "data-dir", "[process]")?.map(|(p, _)| dir.join(p));
        if crate::project::get(t, "placement").is_some() {
            file.process.placement = Some(boolean(&src, t, "placement", "[process]")?);
        }
        if crate::project::get(t, "idle-compaction").is_some() {
            file.process.idle_compaction = Some(boolean(&src, t, "idle-compaction", "[process]")?);
        }
    }

    let listen_at = get(root, "listen").map_or(0..0, |(k, _)| k.span());
    if let Some(t) = table(&src, root, "listen")? {
        known_keys(&src, t, "[listen]", LISTEN)?;
        let addr = |key: &str| -> Result<Option<SocketAddr>, String> {
            match string_at(&src, t, key, "[listen]")? {
                None => Ok(None),
                Some((a, span)) => a.parse().map(Some).or_else(|_| {
                    src.err(
                        span,
                        format!("[listen] {key} {a:?}: expected an address, e.g. 0.0.0.0:8080"),
                    )
                }),
            }
        };
        file.listen.public = addr("public")?;
        file.listen.cluster = addr("cluster")?;
        file.listen.metrics = addr("metrics")?;
        for (host, span) in crate::project::strings(&src, t, "metrics-hosts", "[listen]")? {
            let Some(normal) = hostname(&host) else {
                return src.err(
                    span,
                    format!(
                        "[listen] metrics-hosts: {host:?} is not a hostname (letters, digits \
                         and '-' in dot-separated labels; no port, no scheme)"
                    ),
                );
            };
            if !file.listen.metrics_hosts.contains(&normal) {
                file.listen.metrics_hosts.push(normal);
            }
        }
        if let Some((a, span)) = string_at(&src, t, "public-advertise", "[listen]")? {
            if !a.starts_with("http://") || a.len() == "http://".len() {
                return src.err(
                    span,
                    format!(
                        "[listen] public-advertise must be an http:// URL other nodes reach \
                         this one at (http://10.0.0.11:8080), got {a:?}"
                    ),
                );
            }
            file.listen.public_advertise = Some(a);
        }
        if let Some((a, span)) = string_at(&src, t, "cluster-advertise", "[listen]")? {
            if let Err(e) = crate::parse_cluster_advertise(&a) {
                return src.err(span, format!("[listen] cluster-advertise: {e}"));
            }
            file.listen.cluster_advertise = Some(a);
        }
    }
    if file.listen.cluster.is_some() != file.listen.cluster_advertise.is_some() {
        return src.err(
            listen_at.clone(),
            "[listen] cluster and cluster-advertise go together: the cluster listener and \
             the https:// URL other nodes reach it at",
        );
    }

    if let Some(t) = table(&src, root, "limits")? {
        known_keys(&src, t, "[limits]", LIMITS)?;
        file.limits.max_sockets_per_node = count(&src, t, "max-sockets-per-node", "[limits]")?;
        let heap = count(&src, t, "sqlite-heap-bytes", "[limits]")?;
        let min = wasmkeep_storage::MIN_SQLITE_HEAP_BYTES;
        if let Some(bytes) = heap
            && (bytes as u64) < min
        {
            let span = get(t, "sqlite-heap-bytes").map_or(0..0, |(_, v)| v.span());
            return src.err(
                span,
                format!("[limits] sqlite-heap-bytes: at least {min} (64 MiB), not {bytes}"),
            );
        }
        file.limits.sqlite_heap_bytes = heap.map(|b| b as u64);
    }
    let node_sockets = file
        .limits
        .max_sockets_per_node
        .unwrap_or(DEFAULT_NODE_SOCKETS);

    let Some((app_key, apps)) = get(root, "app") else {
        return src.err(
            0..0,
            "no [[app]]: the file lists the apps this process serves, each as \
             [[app]] with name = \"TENANT/APP\"",
        );
    };
    let DeValue::Array(apps) = apps.get_ref() else {
        return src.err(app_key.span(), "app: expected [[app]] entries, one per app");
    };
    for entry in apps {
        let DeValue::Table(t) = entry.get_ref() else {
            return src.err(entry.span(), "app: expected [[app]] entries, one per app");
        };
        let header = header_at(text, entry.span().start);
        let what = "[[app]]";
        known_keys(&src, t, what, APP)?;
        let mut a = AppEntry {
            at: src.at(header..header),
            ..AppEntry::default()
        };
        let Some((name, span)) = string_at(&src, t, "name", what)? else {
            return src.err(header..header, "[[app]] name is required (TENANT/APP)");
        };
        match crate::parse_app(&name) {
            Ok((tenant, app))
                if wasmkeep_runtime::is_identifier(&tenant)
                    && wasmkeep_runtime::is_identifier(&app) =>
            {
                (a.tenant, a.name) = (tenant, app);
            }
            _ => {
                return src.err(
                    span,
                    format!(
                        "[[app]] name {name:?}: expected TENANT/APP, each matching \
                         [a-z][a-z0-9-]{{0,63}}"
                    ),
                );
            }
        }
        let what = format!("[[app]] {name}");
        let what = what.as_str();
        if let Some((id, span)) = string_at(&src, t, "node-id", what)? {
            if wasmkeep_cluster::membership::validate_node_id(&id).is_err() {
                return src.err(
                    span,
                    format!("{what} node-id {id:?}: use 1 to 64 of [a-z0-9-]"),
                );
            }
            if file.listen.public_advertise.is_none() {
                return src.err(
                    span,
                    format!(
                        "{what} node-id: a cluster node needs [listen] public-advertise, the \
                         http:// URL other nodes reach it at"
                    ),
                );
            }
            a.node_id = Some(id);
        }
        for (host, span) in crate::project::strings(&src, t, "hostnames", what)? {
            let Some(normal) = hostname(&host) else {
                return src.err(
                    span,
                    format!(
                        "{what} hostnames: {host:?} is not a hostname (letters, digits and '-' \
                         in dot-separated labels; no port, no scheme)"
                    ),
                );
            };
            if a.hostnames.contains(&normal) {
                return src.err(span, format!("{what} hostnames: {normal} is named twice"));
            }
            a.hostnames.push(normal);
        }
        let keys = crate::project::strings(&src, t, "secrets-key", what)?;
        if keys.len() > 2 {
            return src.err(
                keys[2].1.clone(),
                format!("{what} secrets-key: one file, or two during a key change"),
            );
        }
        a.secrets_keys = keys.into_iter().map(|(p, _)| dir.join(p)).collect();
        a.no_secrets = boolean(&src, t, "no-secrets", what)?;
        if a.no_secrets && !a.secrets_keys.is_empty() {
            let span = get(t, "no-secrets").map_or(0..0, |(k, _)| k.span());
            return src.err(
                span,
                format!("{what}: no-secrets and secrets-key cannot be used together"),
            );
        }
        if let Some(env) = table(&src, t, "env")? {
            for (class, files) in env {
                let class_str = class.get_ref().as_ref();
                if class_str == crate::FRONT_SCOPE {
                    return src.err(
                        class.span(),
                        format!(
                            "{what} env: env takes classes; give the front's files as front-env"
                        ),
                    );
                }
                if !wasmkeep_runtime::is_identifier(class_str) {
                    return src.err(
                        class.span(),
                        format!("{what} env: {class_str:?} is not a class name"),
                    );
                }
                let DeValue::Array(items) = files.get_ref() else {
                    return src.err(
                        files.span(),
                        format!("{what} env {class_str}: expected a list of files ([\"...\"])"),
                    );
                };
                for item in items {
                    match item.get_ref() {
                        DeValue::String(f) if !f.is_empty() => {
                            a.env.push((class_str.to_owned(), dir.join(f.as_ref())));
                        }
                        _ => {
                            return src.err(
                                item.span(),
                                format!("{what} env {class_str}: expected a file name"),
                            );
                        }
                    }
                }
            }
        }
        a.front_env = crate::project::strings(&src, t, "front-env", what)?
            .into_iter()
            .map(|(p, _)| dir.join(p))
            .collect();
        if let Some(access) = table(&src, t, "access")? {
            for (class, mode) in access {
                let class_str = class.get_ref().as_ref();
                let DeValue::String(spec) = mode.get_ref() else {
                    return src.err(
                        mode.span(),
                        format!("{what} access {class_str}: expected a mode (open, existing, existing:NAME,..., internal)"),
                    );
                };
                if let Err(e) = crate::parse_access(&format!("{class_str}={spec}")) {
                    return src.err(mode.span(), format!("{what} access {class_str}: {e}"));
                }
                a.access.push((class_str.to_owned(), spec.to_string()));
            }
        }
        for (range, span) in crate::project::strings(&src, t, "allow-outbound", what)? {
            match range.parse() {
                Ok(r) => a.allow_outbound.push(r),
                Err(e) => {
                    return src.err(span, format!("{what} allow-outbound {range:?}: {e}"));
                }
            }
        }
        if let Some((k, v)) = get(t, "allow-outbound-port") {
            let bad = |span| {
                src.err(
                    span,
                    format!("{what} allow-outbound-port: expected a list of ports (1 to 65535)"),
                )
            };
            let DeValue::Array(items) = v.get_ref() else {
                return bad(k.span().start..v.span().end);
            };
            for item in items {
                match integer(item.get_ref()).and_then(|n| u16::try_from(n).ok()) {
                    Some(p) if p > 0 => a.allow_outbound_port.push(p),
                    _ => return bad(item.span()),
                }
            }
        }
        a.outbound_ca = crate::project::strings(&src, t, "outbound-ca", what)?
            .into_iter()
            .map(|(p, _)| dir.join(p))
            .collect();
        a.max_sockets = count(&src, t, "max-sockets", what)?;
        if let Some(n) = a.max_sockets
            && n > node_sockets
        {
            let span = get(t, "max-sockets").map_or(0..0, |(_, v)| v.span());
            return src.err(
                span,
                format!(
                    "{what} max-sockets {n} is above the node's ceiling, [limits] \
                     max-sockets-per-node {node_sockets}"
                ),
            );
        }
        a.min_resident = count(&src, t, "min-resident", what)?;
        a.max_resident = count(&src, t, "max-resident", what)?;
        if let (Some(min), Some(max)) = (a.min_resident, a.max_resident)
            && min > max
        {
            let span = get(t, "min-resident").map_or(0..0, |(_, v)| v.span());
            return src.err(
                span,
                format!("{what} min-resident {min} is above its max-resident {max}"),
            );
        }
        a.max_front_requests = positive(&src, t, "max-front-requests", what)?;
        a.front_cpu_budget_ms = positive(&src, t, "front-cpu-budget-ms", what)?
            .map(|n| u64::try_from(n).unwrap_or(u64::MAX));
        a.data_dir = string_at(&src, t, "data-dir", what)?.map(|(p, _)| dir.join(p));
        a.no_alarms = boolean(&src, t, "no-alarms", what)?;
        a.max_app_blob_bytes = positive(&src, t, "max-app-blob-bytes", what)?
            .map(|n| u64::try_from(n).unwrap_or(u64::MAX));
        if let Some((base, span)) = string_at(&src, t, "blob-link-base", what)? {
            match wasmkeep_server::blobs::links::check_base(&base) {
                Ok(b) => a.blob_link_base = Some(b),
                Err(e) => return src.err(span, format!("{what} blob-link-base: {e}")),
            }
        }
        file.apps.push(a);
    }
    if file.apps.is_empty() {
        return src.err(
            app_key.span(),
            "no [[app]]: the file lists at least one app",
        );
    }
    // Node hops use the cluster port only with several apps (ADR 0038,
    // section 5).
    if let Some(second) = file.apps.get(1)
        && file.listen.cluster.is_none()
    {
        return Err(format!(
            "{}: a second [[app]] ({}/{}) needs a cluster listener ([listen] cluster and \
             cluster-advertise): several apps in one process talk to other nodes on the \
             cluster port only",
            second.at, second.tenant, second.name
        ));
    }
    file.check_entries()?;
    Ok(file)
}

/// The start of the `[[app]]` header of the entry whose table starts at
/// `start`.
fn header_at(text: &str, start: usize) -> usize {
    let upto = start.saturating_add(2).min(text.len());
    text.get(..upto)
        .and_then(|t| t.rfind("[["))
        .unwrap_or(start)
}

/// `key` as a non-empty string, with the span of its value.
fn string_at(
    src: &crate::project::Source<'_>,
    t: &crate::project::Table<'_>,
    key: &str,
    what: &str,
) -> Result<Option<(String, std::ops::Range<usize>)>, String> {
    let span = crate::project::get(t, key).map(|(_, v)| v.span());
    Ok(crate::project::string(src, t, key, what)?.map(|s| (s, span.unwrap_or(0..0))))
}

/// A TOML integer's value, if it is one.
fn integer(v: &toml::de::DeValue<'_>) -> Option<i64> {
    match v {
        toml::de::DeValue::Integer(i) => {
            let digits = i.as_str().replace('_', "");
            i64::from_str_radix(digits.trim_start_matches('+'), i.radix()).ok()
        }
        _ => None,
    }
}

/// `key` as a count (0 or more).
fn count(
    src: &crate::project::Source<'_>,
    t: &crate::project::Table<'_>,
    key: &str,
    what: &str,
) -> Result<Option<usize>, String> {
    let Some((k, v)) = crate::project::get(t, key) else {
        return Ok(None);
    };
    match integer(v.get_ref()).and_then(|n| usize::try_from(n).ok()) {
        Some(n) => Ok(Some(n)),
        None => src.err(
            k.span().start..v.span().end,
            format!("{what} {key}: expected a whole number, 0 or more"),
        ),
    }
}

/// `key` as a count of at least 1.
fn positive(
    src: &crate::project::Source<'_>,
    t: &crate::project::Table<'_>,
    key: &str,
    what: &str,
) -> Result<Option<usize>, String> {
    match count(src, t, key, what)? {
        Some(0) => {
            let span = crate::project::get(t, key).map_or(0..0, |(_, v)| v.span());
            src.err(span, format!("{what} {key}: must be at least 1"))
        }
        n => Ok(n),
    }
}

fn boolean(
    src: &crate::project::Source<'_>,
    t: &crate::project::Table<'_>,
    key: &str,
    what: &str,
) -> Result<bool, String> {
    match crate::project::get(t, key) {
        None => Ok(false),
        Some((_, v)) => match v.get_ref() {
            toml::de::DeValue::Boolean(b) => Ok(*b),
            other => src.err(
                v.span(),
                format!(
                    "{what} {key}: expected true or false, got {}",
                    other.type_str()
                ),
            ),
        },
    }
}

/// `host` normalized (lowercase, a trailing dot removed), if it is a
/// hostname: dot-separated labels of 1 to 63 letters, digits and '-', not
/// starting or ending with '-', 253 characters at most.
pub(crate) fn hostname(host: &str) -> Option<String> {
    let host = host.strip_suffix('.').unwrap_or(host).to_ascii_lowercase();
    let ok = !host.is_empty()
        && host.len() <= 253
        && host.split('.').all(|label| {
            !label.is_empty()
                && label.len() <= 63
                && !label.starts_with('-')
                && !label.ends_with('-')
                && label
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        });
    ok.then_some(host)
}

impl NodeFile {
    /// Whether the file is served as several apps (ADR 0038): more than
    /// one entry, or a hostname table.
    pub(crate) fn several(&self) -> bool {
        self.apps.len() > 1 || self.apps.iter().any(|a| !a.hostnames.is_empty())
    }

    /// Sets on `args` what entry `i` says, as the matching flags would,
    /// with the file's listeners and limits: one app runtime of several.
    /// Its hostnames route to it; they are not a flag.
    pub(crate) fn apply_entry(&self, i: usize, args: &mut crate::ServeArgs) -> anyhow::Result<()> {
        let a = self
            .apps
            .get(i)
            .ok_or_else(|| anyhow::anyhow!("{}: no [[app]] {i}", self.path.display()))?;
        self.apply_app(a, args)
    }

    /// The data directory `app` uses.
    fn data_dir_of<'a>(&'a self, app: &'a AppEntry) -> Option<&'a Path> {
        app.data_dir.as_deref().or(self.process.data_dir.as_deref())
    }

    /// The rules between entries (ADR 0038, section 4).
    fn check_entries(&self) -> Result<(), String> {
        let default_dir = Path::new(".wasmkeep/data");
        for (i, b) in self.apps.iter().enumerate() {
            for a in &self.apps[..i] {
                let refuse = |why: String| Err(format!("{}: {why} (first at {})", b.at, a.at));
                if (a.tenant.as_str(), a.name.as_str()) == (b.tenant.as_str(), b.name.as_str()) {
                    return refuse(format!("{}/{} is listed twice", b.tenant, b.name));
                }
                if let (Some(x), Some(y)) = (&a.node_id, &b.node_id)
                    && x == y
                {
                    return refuse(format!("node id {y} is listed twice"));
                }
                if let Some(h) = b.hostnames.iter().find(|h| a.hostnames.contains(h)) {
                    return refuse(format!("hostname {h} is listed twice"));
                }
                let root = |e: &AppEntry| {
                    crate::project::lexical(&wasmkeep_server::local_app_dir(
                        self.data_dir_of(e).unwrap_or(default_dir),
                        &e.tenant,
                        &e.name,
                    ))
                };
                let dir = |e: &AppEntry| {
                    crate::project::lexical(self.data_dir_of(e).unwrap_or(default_dir))
                };
                let (ra, rb) = (root(a), root(b));
                // One app's files inside the other's root, or its data
                // directory (where a delete or the layout move works) there.
                if ra.starts_with(&rb)
                    || rb.starts_with(&ra)
                    || dir(a).starts_with(&rb)
                    || dir(b).starts_with(&ra)
                {
                    return refuse(format!(
                        "the local roots of {}/{} ({}) and {}/{} ({}) overlap",
                        a.tenant,
                        a.name,
                        ra.display(),
                        b.tenant,
                        b.name,
                        rb.display()
                    ));
                }
                if a.tenant == b.tenant {
                    for e in [a, b] {
                        let old = self
                            .data_dir_of(e)
                            .unwrap_or(default_dir)
                            .join("actors")
                            .join(&e.tenant);
                        if old.is_dir() {
                            return refuse(format!(
                                "{} still holds {}'s databases in the layout from before \
                                 0.4.0 ({}): with two of its apps, the one-time move would \
                                 give them to whichever starts first; run one app first",
                                self.data_dir_of(e).unwrap_or(default_dir).display(),
                                e.tenant,
                                old.display()
                            ));
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Refuses `--cluster-plaintext accept` (`accept`) with several apps
    /// (ADR 0038, section 5); one app keeps today's behaviour.
    pub(crate) fn check_plaintext(&self, accept: bool) -> Result<(), String> {
        if accept && self.several() {
            return Err(format!(
                "{}: several apps in one process talk to other nodes on the cluster port only: \
                 --cluster-plaintext accept is refused with them (finish moving the cluster to \
                 TLS first)",
                self.path.display()
            ));
        }
        Ok(())
    }

    /// What start logs as warnings about the file.
    pub(crate) fn warnings(&self) -> Vec<String> {
        let mut out = Vec::new();
        if self.apps.len() > 1 && self.apps.iter().all(|a| a.max_sockets.is_none()) {
            out.push(format!(
                "{}: several apps and none sets max-sockets: one app can take every socket \
                 session of the node",
                self.path.display()
            ));
        }
        out
    }

    /// Refuses resident floors whose sum exceeds the process's resident
    /// limit (known once the instance pool and descriptors are).
    pub(crate) fn check_resident_floors(&self, resident_limit: usize) -> Result<(), String> {
        let sum: usize = self.apps.iter().filter_map(|a| a.min_resident).sum();
        if sum > resident_limit {
            return Err(format!(
                "{}: the apps' min-resident add up to {sum}, above the process's resident \
                 limit {resident_limit}",
                self.path.display()
            ));
        }
        Ok(())
    }

    /// `serve --app` and its per-app flags as a one-entry file with the
    /// same meaning; `None` without `--app`.
    pub(crate) fn from_serve(args: &crate::ServeArgs) -> Option<Self> {
        let (tenant, name) = args.app.clone()?;
        Some(Self {
            path: PathBuf::from("the command line"),
            process: Process {
                id: None,
                data_dir: Some(args.data_dir.clone()),
                placement: args.no_placement.then_some(false),
                idle_compaction: args.no_idle_compaction.then_some(false),
            },
            listen: Listen {
                public: Some(args.listen),
                public_advertise: args.advertise.clone(),
                cluster: args.cluster_listen,
                cluster_advertise: args.cluster_advertise.clone(),
                metrics: args.metrics_listen,
                metrics_hosts: args.metrics_hosts.clone(),
            },
            limits: Limits {
                max_sockets_per_node: args.max_sockets_per_node,
                sqlite_heap_bytes: args.sqlite_heap_bytes,
            },
            apps: vec![AppEntry {
                at: "the command line".to_owned(),
                tenant,
                name,
                node_id: args.node_id.clone(),
                hostnames: Vec::new(),
                secrets_keys: args.secrets_keys.clone(),
                no_secrets: args.no_secrets,
                env: args.env.clone(),
                front_env: args.front_env.clone(),
                access: args.access.clone(),
                allow_outbound: args.allow_outbound.clone(),
                allow_outbound_port: args.allow_outbound_port.clone(),
                outbound_ca: args.outbound_ca.clone(),
                max_sockets: args.max_sockets_per_app,
                min_resident: None,
                max_resident: None,
                max_front_requests: args.max_front_requests,
                front_cpu_budget_ms: Some(args.front_cpu_budget_ms),
                data_dir: None,
                no_alarms: args.no_alarms,
                blob_link_base: args.blob_link_base.clone(),
                max_app_blob_bytes: args.max_app_blob_bytes,
            }],
        })
    }

    /// Sets on `args` what the file says, as the matching flags would.
    /// Refuses a field this version cannot honour yet, rather than ignore
    /// it.
    pub(crate) fn apply_to(&self, args: &mut crate::ServeArgs) -> anyhow::Result<()> {
        let [a] = self.apps.as_slice() else {
            anyhow::bail!(
                "{}: this version serves exactly one [[app]] per process",
                self.path.display()
            );
        };
        let not_yet = |field: &str, why: &str| {
            anyhow::anyhow!(
                "{}: {field} is not available in this version ({why}); remove it",
                a.at
            )
        };
        if !a.hostnames.is_empty() {
            return Err(not_yet(
                "hostnames",
                "a file with hostnames is served by hostname, as several apps are",
            ));
        }
        if a.min_resident.is_some() {
            return Err(not_yet(
                "min-resident",
                "resident floors apply between the apps of one process",
            ));
        }
        if a.max_resident.is_some() {
            return Err(not_yet(
                "max-resident",
                "resident caps apply between the apps of one process",
            ));
        }
        self.apply_app(a, args)
    }

    /// [`Self::apply_to`] for the entry `a`.
    fn apply_app(&self, a: &AppEntry, args: &mut crate::ServeArgs) -> anyhow::Result<()> {
        args.app = Some((a.tenant.clone(), a.name.clone()));
        args.node_id.clone_from(&a.node_id);
        // The process's switch, for each of its apps (ADR 0041).
        if self.process.placement == Some(false) {
            args.no_placement = true;
        }
        // ADR 0042's switch, the same way.
        if self.process.idle_compaction == Some(false) {
            args.no_idle_compaction = true;
        }
        args.advertise.clone_from(&self.listen.public_advertise);
        if let Some(public) = self.listen.public {
            args.listen = public;
        }
        args.cluster_listen = self.listen.cluster;
        args.cluster_advertise
            .clone_from(&self.listen.cluster_advertise);
        args.metrics_listen = self.listen.metrics;
        args.metrics_hosts.clone_from(&self.listen.metrics_hosts);
        args.max_sockets_per_node = self.limits.max_sockets_per_node;
        args.sqlite_heap_bytes = self.limits.sqlite_heap_bytes;
        if let Some(dir) = self.data_dir_of(a) {
            args.data_dir = dir.to_path_buf();
        }
        args.secrets_keys.clone_from(&a.secrets_keys);
        args.no_secrets = a.no_secrets;
        args.env.clone_from(&a.env);
        args.front_env.clone_from(&a.front_env);
        args.access.clone_from(&a.access);
        args.allow_outbound.clone_from(&a.allow_outbound);
        args.allow_outbound_port.clone_from(&a.allow_outbound_port);
        args.outbound_ca.clone_from(&a.outbound_ca);
        args.max_sockets_per_app = a.max_sockets;
        args.max_front_requests = a.max_front_requests;
        args.no_alarms = a.no_alarms;
        args.blob_link_base.clone_from(&a.blob_link_base);
        args.max_app_blob_bytes = a.max_app_blob_bytes;
        if let Some(ms) = a.front_cpu_budget_ms {
            args.front_cpu_budget_ms = ms;
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "node_file_tests.rs"]
mod tests;
