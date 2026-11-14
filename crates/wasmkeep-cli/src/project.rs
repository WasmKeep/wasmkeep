//! The project file, `wasmkeep.toml` (ADR 0026): what an app is (its
//! front, its classes, each class's rule, configuration file and required
//! names), how each part is built, and how `wasmkeep dev` runs it. Read by
//! `wasmkeep deploy` and `wasmkeep dev`; the deployed app is what the file
//! says.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use anyhow::Context as _;
use toml::Spanned;
use toml::de::{DeTable, DeValue};

/// The file `deploy` and `dev` look for in the current directory.
pub(crate) const FILE_NAME: &str = "wasmkeep.toml";

/// A parsed project file. Paths are resolved against the file's directory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Project {
    /// The file itself.
    pub(crate) path: PathBuf,
    /// Its directory: where builds run and relative paths start.
    pub(crate) dir: PathBuf,
    pub(crate) tenant: String,
    pub(crate) name: String,
    /// `[front]`, if any.
    pub(crate) front: Option<Item>,
    /// `[classes.<name>]`, by name.
    pub(crate) classes: BTreeMap<String, Class>,
    pub(crate) dev: Dev,
}

/// What the front and a class have in common.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Item {
    pub(crate) component: PathBuf,
    /// A command line for the system shell, run in the file's directory.
    pub(crate) build: Option<String>,
    /// A file of `NAME=value` lines (plain configuration, never secrets).
    pub(crate) config: Option<PathBuf>,
    pub(crate) requires: BTreeSet<String>,
    /// Paths `wasmkeep dev` watches to run `build` again.
    pub(crate) watch: Vec<PathBuf>,
}

/// A class: its item and its rule.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Class {
    pub(crate) item: Item,
    /// As `--access` takes it; `internal` when the file says nothing.
    pub(crate) access: String,
}

/// `[dev]`: for `wasmkeep dev` only.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct Dev {
    pub(crate) port: Option<u16>,
    pub(crate) data_dir: Option<PathBuf>,
    /// The local values file, and whether the file named it (else the
    /// default, `.env.local`, read only if it exists).
    pub(crate) env: PathBuf,
    pub(crate) env_named: bool,
}

const TOP: &[&str] = &["app", "front", "classes", "dev"];
const APP: &[&str] = &["tenant", "name"];
const FRONT: &[&str] = &["component", "build", "config", "requires", "watch"];
const CLASS: &[&str] = &[
    "component",
    "access",
    "build",
    "config",
    "requires",
    "watch",
];
const DEV: &[&str] = &["port", "data-dir", "env"];

/// A position in the file, for errors: `path:line:column`.
struct Source<'a> {
    text: &'a str,
    path: &'a Path,
}

impl Source<'_> {
    fn at(&self, span: std::ops::Range<usize>) -> String {
        let start = span.start.min(self.text.len());
        let before = &self.text[..start];
        let line = before.matches('\n').count() + 1;
        let column = before
            .rsplit_once('\n')
            .map_or(before, |(_, l)| l)
            .chars()
            .count()
            + 1;
        format!("{}:{line}:{column}", self.path.display())
    }

    fn err<T>(
        &self,
        span: std::ops::Range<usize>,
        msg: impl std::fmt::Display,
    ) -> Result<T, String> {
        Err(format!("{}: {msg}", self.at(span)))
    }
}

type Table<'i> = DeTable<'i>;

/// Checks that `table` (`what`, e.g. `[front]`) has only `allowed` keys.
fn known_keys(
    src: &Source<'_>,
    table: &Table<'_>,
    what: &str,
    allowed: &[&str],
) -> Result<(), String> {
    for key in table.keys() {
        if !allowed.contains(&key.get_ref().as_ref()) {
            return src.err(
                key.span(),
                format!(
                    "unknown key {:?} in {what} (it takes: {})",
                    key.get_ref(),
                    allowed.join(", ")
                ),
            );
        }
    }
    Ok(())
}

fn get<'a, 'i>(
    table: &'a Table<'i>,
    key: &str,
) -> Option<(
    &'a Spanned<std::borrow::Cow<'i, str>>,
    &'a Spanned<DeValue<'i>>,
)> {
    table.get_key_value(key)
}

fn string(
    src: &Source<'_>,
    table: &Table<'_>,
    key: &str,
    what: &str,
) -> Result<Option<String>, String> {
    let Some((k, v)) = get(table, key) else {
        return Ok(None);
    };
    match v.get_ref() {
        DeValue::String(s) if !s.is_empty() => Ok(Some(s.to_string())),
        DeValue::String(_) => src.err(v.span(), format!("{what} {key}: must not be empty")),
        other => src.err(
            k.span().start..v.span().end,
            format!("{what} {key}: expected a string, got {}", other.type_str()),
        ),
    }
}

fn strings(
    src: &Source<'_>,
    table: &Table<'_>,
    key: &str,
    what: &str,
) -> Result<Vec<(String, std::ops::Range<usize>)>, String> {
    let Some((k, v)) = get(table, key) else {
        return Ok(Vec::new());
    };
    let DeValue::Array(items) = v.get_ref() else {
        return src.err(
            k.span().start..v.span().end,
            format!(
                "{what} {key}: expected a list of strings ({key} = [\"...\"]), got {}",
                v.get_ref().type_str()
            ),
        );
    };
    let mut out = Vec::new();
    for item in items {
        match item.get_ref() {
            DeValue::String(s) if !s.is_empty() => out.push((s.to_string(), item.span())),
            DeValue::String(_) => {
                return src.err(item.span(), format!("{what} {key}: an empty string"));
            }
            other => {
                return src.err(
                    item.span(),
                    format!("{what} {key}: expected a string, got {}", other.type_str()),
                );
            }
        }
    }
    Ok(out)
}

fn table<'a, 'i>(
    src: &Source<'_>,
    parent: &'a Table<'i>,
    key: &str,
) -> Result<Option<&'a Table<'i>>, String> {
    match get(parent, key) {
        None => Ok(None),
        Some((_, v)) => match v.get_ref() {
            DeValue::Table(t) => Ok(Some(t)),
            other => src.err(
                v.span(),
                format!(
                    "{key}: expected a table ([{key}]), got {}",
                    other.type_str()
                ),
            ),
        },
    }
}

fn resolve(dir: &Path, path: &str) -> PathBuf {
    dir.join(path)
}

/// `path` made absolute (against the current directory) with `.` and `..`
/// resolved lexically: two spellings of one path compare equal without
/// touching the disk.
fn lexical(path: &Path) -> PathBuf {
    use std::path::Component;
    let path = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir().unwrap_or_default().join(path)
    };
    let mut out = PathBuf::new();
    for part in path.components() {
        match part {
            Component::CurDir => {}
            Component::ParentDir => {
                out.pop();
            }
            other => out.push(other),
        }
    }
    out
}

/// Whether `a` and `b` are one file: the same device and inode (links
/// included) or canonical path when both exist, else the same path
/// lexically.
fn same_file(a: &Path, b: &Path) -> bool {
    if let (Ok(ma), Ok(mb)) = (std::fs::metadata(a), std::fs::metadata(b)) {
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt as _;
            return ma.dev() == mb.dev() && ma.ino() == mb.ino();
        }
        #[cfg(not(unix))]
        {
            let _ = (ma, mb);
            if let (Ok(ca), Ok(cb)) = (a.canonicalize(), b.canonicalize()) {
                return ca == cb;
            }
        }
    }
    lexical(a) == lexical(b)
}

/// Refuses a configuration file that is the `[dev] env` file of local
/// values: configuration is uploaded in the clear.
fn not_local_values(project: &Project, what: &str, config: &Path) -> anyhow::Result<()> {
    anyhow::ensure!(
        !same_file(config, &project.dev.env),
        "{}: {what} config is {}, the [dev] env file of local values ({}): configuration is \
         stored in the bucket in the clear; give it a file of its own",
        project.path.display(),
        config.display(),
        project.dev.env.display()
    );
    Ok(())
}

fn item(
    src: &Source<'_>,
    dir: &Path,
    t: &Table<'_>,
    what: &str,
    span: std::ops::Range<usize>,
) -> Result<Item, String> {
    let component = string(src, t, "component", what)?.ok_or_else(|| {
        format!(
            "{}: {what}: component is required (the .wasm file)",
            src.at(span)
        )
    })?;
    let mut requires = BTreeSet::new();
    for (name, span) in strings(src, t, "requires", what)? {
        if !wasmkeep_server::env::valid_name(&name) {
            return src.err(
                span,
                format!(
                    "{what} requires: invalid name {name:?} (letters, digits and _, not starting with a digit)"
                ),
            );
        }
        if !requires.insert(name.clone()) {
            return src.err(span, format!("{what} requires: {name} is named twice"));
        }
    }
    Ok(Item {
        component: resolve(dir, &component),
        build: string(src, t, "build", what)?,
        config: string(src, t, "config", what)?.map(|p| resolve(dir, &p)),
        requires,
        watch: strings(src, t, "watch", what)?
            .into_iter()
            .map(|(p, _)| resolve(dir, &p))
            .collect(),
    })
}

/// Parses the file at `path` (its `text`): every key checked, errors with
/// the file, line, column and key.
#[cfg(test)]
pub(crate) fn parse(text: &str, path: &Path) -> Result<Project, String> {
    parse_with(text, path, false)
}

/// [`parse`], or with `app_only` only `[app]` (a rollback, ADR 0026: the
/// rest of the file may describe a version not deployed yet, typos
/// included); the rest is then left empty.
fn parse_with(text: &str, path: &Path, app_only: bool) -> Result<Project, String> {
    parse_inner(text, path, app_only).map_err(|e| {
        if app_only {
            format!(
                "{e}; a rollback reads only [app], from a file that is still TOML: fix the \
                 file, or pass --no-project --app TENANT/APP --rollback"
            )
        } else {
            e
        }
    })
}

fn parse_inner(text: &str, path: &Path, app_only: bool) -> Result<Project, String> {
    let src = Source { text, path };
    let root = DeTable::parse(text).map_err(|e| match e.span() {
        Some(span) => format!("{}: {}", src.at(span), e.message().trim()),
        None => format!("{}: {}", path.display(), e.message().trim()),
    })?;
    let root = root.get_ref();
    let dir = path
        .parent()
        .map_or_else(|| PathBuf::from("."), Path::to_path_buf);
    let dir = if dir.as_os_str().is_empty() {
        PathBuf::from(".")
    } else {
        dir
    };
    if !app_only {
        known_keys(&src, root, "the file", TOP)?;
    }

    let app = table(&src, root, "app")?
        .ok_or_else(|| format!("{}: [app] is required, with tenant and name", src.at(0..0)))?;
    let app_at = get(root, "app").map_or(0..0, |(k, _)| k.span());
    known_keys(&src, app, "[app]", APP)?;
    let mut names = Vec::new();
    for key in APP {
        let value = string(&src, app, key, "[app]")?
            .ok_or_else(|| format!("{}: [app] {key} is required", src.at(app_at.clone())))?;
        if !wasmkeep_runtime::is_identifier(&value) {
            let span = get(app, key).map(|(_, v)| v.span()).unwrap_or(0..0);
            return src.err(
                span,
                format!(
                    "[app] {key} {value:?} must match [a-z][a-z0-9-]{{0,63}} (lowercase letters, digits and '-', starting with a letter)"
                ),
            );
        }
        names.push(value);
    }
    if app_only {
        let mut names = names.into_iter();
        return Ok(Project {
            path: path.to_path_buf(),
            dir: dir.clone(),
            tenant: names.next().unwrap_or_default(),
            name: names.next().unwrap_or_default(),
            front: None,
            classes: BTreeMap::new(),
            dev: Dev {
                env: dir.join(".env.local"),
                ..Dev::default()
            },
        });
    }

    let front = match get(root, "front") {
        None => None,
        Some((k, _)) => {
            let t = table(&src, root, "front")?.unwrap_or_else(|| unreachable!());
            known_keys(&src, t, "[front]", FRONT)?;
            Some(item(&src, &dir, t, "[front]", k.span())?)
        }
    };

    let mut classes = BTreeMap::new();
    if let Some(t) = table(&src, root, "classes")? {
        for (name, value) in t {
            let name_str = name.get_ref().as_ref();
            let what = format!("[classes.{name_str}]");
            if name_str == crate::FRONT_SCOPE {
                return src.err(
                    name.span(),
                    format!(
                        "{what}: {} is the front's scope, not a class: give the front as [front]",
                        crate::FRONT_SCOPE
                    ),
                );
            }
            if !wasmkeep_runtime::is_identifier(name_str) {
                return src.err(
                    name.span(),
                    format!(
                        "{what}: a class name must match [a-z][a-z0-9-]{{0,63}} (lowercase letters, digits and '-', starting with a letter)"
                    ),
                );
            }
            let DeValue::Table(ct) = value.get_ref() else {
                return src.err(
                    value.span(),
                    format!(
                        "classes.{name_str}: expected a table ([classes.{name_str}] with component = \"...\"), got {}",
                        value.get_ref().type_str()
                    ),
                );
            };
            known_keys(&src, ct, &what, CLASS)?;
            let item = item(&src, &dir, ct, &what, name.span())?;
            let access = match string(&src, ct, "access", &what)? {
                None => "internal".to_owned(),
                Some(spec) => {
                    let mut check = wasmkeep_server::AccessRules::default();
                    if let Err(e) = check.replace(name_str, &spec) {
                        let span = get(ct, "access").map(|(_, v)| v.span()).unwrap_or(0..0);
                        return src.err(
                            span,
                            format!(
                                "{what} access {spec:?}: {e} (open, existing, existing:NAME,..., internal)"
                            ),
                        );
                    }
                    check.spec(name_str).unwrap_or_else(|| "open".to_owned())
                }
            };
            classes.insert(name_str.to_owned(), Class { item, access });
        }
    }
    if front.is_none() && classes.is_empty() {
        return Err(format!(
            "{}: nothing to deploy: give [front] or at least one [classes.<name>]",
            src.at(0..0)
        ));
    }

    let mut dev = Dev {
        env: dir.join(".env.local"),
        ..Dev::default()
    };
    if let Some(t) = table(&src, root, "dev")? {
        known_keys(&src, t, "[dev]", DEV)?;
        if let Some((k, v)) = get(t, "port") {
            let port = match v.get_ref() {
                DeValue::Integer(i) => {
                    let digits = i.as_str().replace('_', "");
                    u16::from_str_radix(digits.trim_start_matches('+'), i.radix()).ok()
                }
                _ => None,
            };
            match port {
                Some(p) => dev.port = Some(p),
                None => {
                    return src.err(
                        k.span().start..v.span().end,
                        "[dev] port: expected a port number (0 to 65535)",
                    );
                }
            }
        }
        dev.data_dir = string(&src, t, "data-dir", "[dev]")?.map(|p| resolve(&dir, &p));
        if let Some(env) = string(&src, t, "env", "[dev]")? {
            dev.env = resolve(&dir, &env);
            dev.env_named = true;
        }
    }

    // The local values file is never configuration: configuration is
    // uploaded in the clear.
    for (what, item) in front.iter().map(|f| ("[front]".to_owned(), f)).chain(
        classes
            .iter()
            .map(|(n, c)| (format!("[classes.{n}]"), &c.item)),
    ) {
        if item.config.as_deref().map(lexical) == Some(lexical(&dev.env)) {
            return Err(format!(
                "{}: {what} config is {}, the [dev] env file of local values: configuration is \
                 stored in the bucket in the clear; give it a file of its own",
                path.display(),
                dev.env.display()
            ));
        }
    }

    let mut names = names.into_iter();
    Ok(Project {
        path: path.to_path_buf(),
        dir,
        tenant: names.next().unwrap_or_default(),
        name: names.next().unwrap_or_default(),
        front,
        classes,
        dev,
    })
}

/// Reads and parses the project file at `path`.
fn load_with(path: &Path, app_only: bool) -> anyhow::Result<Project> {
    let text =
        std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    parse_with(&text, path, app_only).map_err(anyhow::Error::msg)
}

/// The project file for a command: `--project PATH` (a file, or a
/// directory holding `wasmkeep.toml`), else `./wasmkeep.toml` if there is
/// one, unless `--no-project`. With `app_only`, only `[app]` is read
/// (`deploy --rollback`).
pub(crate) fn discover(
    project: Option<&Path>,
    no_project: bool,
    app_only: bool,
) -> anyhow::Result<Option<Project>> {
    if no_project {
        return Ok(None);
    }
    match project {
        Some(p) => {
            let path = if p.is_dir() {
                p.join(FILE_NAME)
            } else {
                p.to_path_buf()
            };
            load_with(&path, app_only).map(Some)
        }
        None => {
            let path = PathBuf::from(FILE_NAME);
            if path.exists() {
                load_with(&path, app_only).map(Some)
            } else {
                Ok(None)
            }
        }
    }
}

/// Refuses flags that say what the version is, beside a project file
/// (ADR 0026): `given` lists them, as typed.
pub(crate) fn refuse_flags(project: &Project, given: &[&str]) -> anyhow::Result<()> {
    if given.is_empty() {
        return Ok(());
    }
    anyhow::bail!(
        "{} is used with {}: with a project file the app is what the file says. Edit {}, or \
         pass --no-project to deploy with flags only (classes added then are open unless \
         given --access)",
        given.join(", "),
        project.path.display(),
        project.path.display(),
    )
}

/// The app: the file's, and `--app` if given must name it.
pub(crate) fn app(
    project: &Project,
    flag: Option<&(String, String)>,
) -> anyhow::Result<(String, String)> {
    if let Some((t, a)) = flag {
        anyhow::ensure!(
            *t == project.tenant && *a == project.name,
            "--app {t}/{a} is not the app of {} ({}/{}): drop --app, or use --no-project",
            project.path.display(),
            project.tenant,
            project.name
        );
    }
    Ok((project.tenant.clone(), project.name.clone()))
}

/// The deploy's bucket credentials, which a build never sees: it runs
/// commands from the file, and needs no bucket.
const HIDDEN_FROM_BUILDS: &[&str] = &[
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_SESSION_TOKEN",
];

/// Runs `command` with the system shell in `dir`, its output on stderr
/// (stdout is the deploy's plan), without the bucket credentials.
pub(crate) fn run_build(dir: &Path, what: &str, command: &str) -> anyhow::Result<()> {
    eprintln!("building {what}: {command}");
    #[cfg(unix)]
    let mut cmd = {
        let mut cmd = std::process::Command::new("sh");
        cmd.arg("-c").arg(command);
        cmd
    };
    #[cfg(windows)]
    let mut cmd = {
        use std::os::windows::process::CommandExt as _;
        // As typed: `cmd.exe` parses its own command line, so Rust's
        // quoting of an argument would reach the command.
        let mut cmd = std::process::Command::new("cmd");
        cmd.arg("/C").raw_arg(command);
        cmd
    };
    #[cfg(not(any(unix, windows)))]
    let mut cmd = {
        let mut cmd = std::process::Command::new("sh");
        cmd.arg("-c").arg(command);
        cmd
    };
    for name in HIDDEN_FROM_BUILDS {
        cmd.env_remove(name);
    }
    let status = cmd.current_dir(dir).stdout(std::io::stderr()).status();
    let status = status.with_context(|| format!("running the build of {what}"))?;
    anyhow::ensure!(
        status.success(),
        "the build of {what} failed ({status}): {command}"
    );
    Ok(())
}

/// The items with their names for messages: the front first, then the
/// classes in name order.
pub(crate) fn items(project: &Project) -> Vec<(String, &Item)> {
    let mut out = Vec::new();
    if let Some(f) = &project.front {
        out.push(("the front".to_owned(), f));
    }
    for (name, class) in &project.classes {
        out.push((format!("class {name}"), &class.item));
    }
    out
}

/// Runs every `build`, the front's first, once every configuration file
/// the project names is there (a missing one fails before any build).
pub(crate) fn run_builds(project: &Project) -> anyhow::Result<()> {
    for (what, item) in items(project) {
        if let Some(config) = &item.config {
            anyhow::ensure!(
                config.is_file(),
                "{what}'s config: {} is not there (named in {})",
                config.display(),
                project.path.display()
            );
        }
    }
    for (what, item) in items(project) {
        if let Some(cmd) = &item.build {
            run_build(&project.dir, &what, cmd)?;
        }
    }
    Ok(())
}

/// What a deploy or `dev` reads from disk: components and configuration.
pub(crate) struct Parts {
    pub(crate) front: Option<Vec<u8>>,
    pub(crate) classes: Vec<(String, Vec<u8>)>,
    /// By class name, or `_front`: each scope's configuration, if its item
    /// names a file.
    pub(crate) config: BTreeMap<String, wasmkeep_server::env::Vars>,
}

/// Reads every component and configuration file the project names.
/// `warn` gets each configuration name that looks like a secret.
pub(crate) fn read_parts(project: &Project, warn: &mut dyn FnMut(String)) -> anyhow::Result<Parts> {
    let read = |path: &Path, what: &str| {
        std::fs::read(path)
            .with_context(|| format!("reading {what}'s component {}", path.display()))
    };
    let mut config = BTreeMap::new();
    let mut read_config = |scope: &str, item: &Item, what: &str| -> anyhow::Result<()> {
        if let Some(path) = &item.config {
            not_local_values(project, what, path)?;
            let (vars, _) = wasmkeep_server::env::read_file(path)
                .map_err(|e| anyhow::anyhow!("{what}'s config: {e}"))?;
            for w in crate::secret_looking(scope, &vars) {
                warn(w.replacen(
                    &format!("--config {scope}:"),
                    &format!("{what}'s config:"),
                    1,
                ));
            }
            config.insert(scope.to_owned(), vars);
        }
        Ok(())
    };
    let front = match &project.front {
        Some(f) => {
            read_config(crate::FRONT_SCOPE, f, "the front")?;
            Some(read(&f.component, "the front")?)
        }
        None => None,
    };
    let mut classes = Vec::new();
    for (name, class) in &project.classes {
        let what = format!("class {name}");
        read_config(name, &class.item, &what)?;
        classes.push((name.clone(), read(&class.item.component, &what)?));
    }
    Ok(Parts {
        front,
        classes,
        config,
    })
}

/// What a deploy from the file asks of the bucket: exactly the file, every
/// rule written, values and required names removed where the file has
/// none, the front set or removed (ADR 0026).
pub(crate) fn to_changes(project: &Project, parts: Parts) -> wasmkeep_server::apps::Changes {
    use wasmkeep_server::apps::{Changes, Part};
    let Parts {
        front,
        classes,
        mut config,
    } = parts;
    let classes = classes
        .into_iter()
        .filter_map(|(name, wasm)| {
            let class = project.classes.get(&name)?;
            let part = Part {
                wasm,
                config: config.remove(&name),
                requires: class.item.requires.clone(),
            };
            Some((name, class.access.clone(), part))
        })
        .collect();
    let front = match (front, &project.front) {
        (Some(wasm), Some(item)) => Some(Part {
            wasm,
            config: config.remove(crate::FRONT_SCOPE),
            requires: item.requires.clone(),
        }),
        _ => None,
    };
    Changes::whole(front, classes)
}

/// What a deploy from the file changes, printed before it switches
/// (ADR 0026): one line per change, removals saying what goes.
pub(crate) fn print_plan(file: &str, tenant: &str, app: &str, diff: &wasmkeep_server::apps::Diff) {
    for line in plan_lines(file, tenant, app, diff) {
        println!("{line}");
    }
}

fn plan_lines(
    file: &str,
    tenant: &str,
    app: &str,
    diff: &wasmkeep_server::apps::Diff,
) -> Vec<String> {
    let mut out = vec![format!("{tenant}/{app} from {file}, before switching:")];
    for line in diff.lines() {
        out.push(format!("  {line}"));
    }
    for (class, _) in &diff.removed {
        out.push(format!(
            "  class {class} is not in {file}: it is removed; its actors' state stays in the bucket"
        ));
    }
    if diff.front == Some("removed") {
        out.push(format!("  the front is not in {file}: it is removed"));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_ok(text: &str) -> Project {
        parse(text, Path::new("app/wasmkeep.toml")).unwrap()
    }

    fn parse_err(text: &str) -> String {
        parse(text, Path::new("wasmkeep.toml")).unwrap_err()
    }

    const FULL: &str = r#"
[app]
tenant = "acme"
name = "todo"

[front]
component = "front/front.wasm"
build = "cd front && make"
config = "front.config"
requires = ["TOKENS"]
watch = ["front/src"]

[classes.todos]
component = "../shared/todos.wasm"

[classes.rooms]
component = "/abs/rooms.wasm"
access = "existing:lobby"
requires = ["API_KEY", "OTHER"]

[classes.pub]
component = "pub.wasm"
access = "open"

[dev]
port = 9090
data-dir = "state"
env = ".env.dev"
"#;

    #[test]
    fn a_full_file_parses_with_paths_from_its_directory() {
        let p = parse_ok(FULL);
        assert_eq!((p.tenant.as_str(), p.name.as_str()), ("acme", "todo"));
        let front = p.front.as_ref().unwrap();
        assert_eq!(front.component, PathBuf::from("app/front/front.wasm"));
        assert_eq!(front.config, Some(PathBuf::from("app/front.config")));
        assert_eq!(front.build.as_deref(), Some("cd front && make"));
        assert_eq!(front.watch, vec![PathBuf::from("app/front/src")]);
        assert_eq!(front.requires, ["TOKENS".to_owned()].into());
        // The default rule is internal; `..` and absolute paths are kept.
        let todos = &p.classes["todos"];
        assert_eq!(todos.access, "internal");
        assert_eq!(
            todos.item.component,
            PathBuf::from("app/../shared/todos.wasm")
        );
        assert_eq!(p.classes["rooms"].access, "existing:lobby");
        assert_eq!(
            p.classes["rooms"].item.component,
            PathBuf::from("/abs/rooms.wasm")
        );
        assert_eq!(p.classes["pub"].access, "open");
        assert_eq!(p.dev.port, Some(9090));
        assert_eq!(p.dev.data_dir, Some(PathBuf::from("app/state")));
        assert_eq!(p.dev.env, PathBuf::from("app/.env.dev"));
        assert!(p.dev.env_named);
    }

    #[test]
    fn the_dev_env_defaults_to_env_local() {
        let p = parse_ok("[app]\ntenant='a'\nname='b'\n[classes.c]\ncomponent='c.wasm'\n");
        assert_eq!(p.dev.env, PathBuf::from("app/.env.local"));
        assert!(!p.dev.env_named);
        assert!(p.front.is_none());
        let p = parse(
            "[app]\ntenant='a'\nname='b'\n[front]\ncomponent='f.wasm'\n",
            Path::new("wasmkeep.toml"),
        )
        .unwrap();
        assert_eq!(p.dir, PathBuf::from("."));
        assert!(p.classes.is_empty());
    }

    #[test]
    fn typos_are_refused_with_the_line_column_and_key() {
        let base = "[app]\ntenant = \"acme\"\nname = \"todo\"\n";
        let cases = [
            (
                "[classes.todos]\ncomponent = \"t.wasm\"\nacess = \"open\"\n",
                "wasmkeep.toml:6:1",
                "unknown key \"acess\" in [classes.todos]",
            ),
            (
                "[clases.todos]\ncomponent = \"t.wasm\"\n",
                "wasmkeep.toml:4:2",
                "unknown key \"clases\" in the file",
            ),
            (
                "[front]\ncomponent = \"f.wasm\"\naccess = \"open\"\n",
                "wasmkeep.toml:6:1",
                "unknown key \"access\" in [front]",
            ),
            (
                "[dev]\nprot = 1\n[front]\ncomponent='f'\n",
                "wasmkeep.toml:5:1",
                "unknown key \"prot\" in [dev]",
            ),
            (
                "[classes.todos]\ncomponent = \"t.wasm\"\nrequires = \"A\"\n",
                "wasmkeep.toml:6:1",
                "expected a list of strings",
            ),
            (
                "[classes.todos]\ncomponent = \"t.wasm\"\naccess = \"opne\"\n",
                "wasmkeep.toml:6:10",
                "access \"opne\"",
            ),
            (
                "[classes.todos]\naccess = \"open\"\n",
                "wasmkeep.toml:4:10",
                "component is required",
            ),
            (
                "[classes.Todos]\ncomponent = \"t.wasm\"\n",
                "wasmkeep.toml:4:10",
                "class name must match",
            ),
            (
                "[classes._front]\ncomponent = \"t.wasm\"\n",
                "wasmkeep.toml:4:10",
                "the front's scope",
            ),
            (
                "[classes.\"../x\"]\ncomponent = \"t.wasm\"\n",
                "wasmkeep.toml:4:10",
                "class name must match",
            ),
            (
                "[classes.t]\ncomponent = \"t.wasm\"\nrequires = [\"A\", \"A\"]\n",
                "wasmkeep.toml:6:18",
                "A is named twice",
            ),
            (
                "[classes.t]\ncomponent = \"t.wasm\"\nrequires = [\"1A\"]\n",
                "wasmkeep.toml:6:13",
                "invalid name \"1A\"",
            ),
            (
                "[classes.t]\ncomponent = \"\"\n",
                "wasmkeep.toml:5:13",
                "must not be empty",
            ),
            (
                "[classes.t]\ncomponent = 3\n",
                "wasmkeep.toml:5:1",
                "expected a string, got integer",
            ),
            (
                "[classes.t]\ncomponent = \"t\"\n[classes.t]\ncomponent = \"u\"\n",
                "wasmkeep.toml:6:",
                "duplicate",
            ),
            (
                "[dev]\nport = \"80\"\n[front]\ncomponent='f'\n",
                "wasmkeep.toml:5:1",
                "[dev] port",
            ),
            (
                "[dev]\nport = 70000\n[front]\ncomponent='f'\n",
                "wasmkeep.toml:5:1",
                "[dev] port",
            ),
            ("", "wasmkeep.toml", "nothing to deploy"),
            (
                "[classes.t]\ncomponent = 't'\nsecret = 'hunter2'\n",
                "wasmkeep.toml:6:1",
                "unknown key \"secret\"",
            ),
        ];
        for (rest, at, want) in cases {
            let err = parse_err(&format!("{base}{rest}"));
            assert!(err.starts_with(at), "{rest:?}: {err}");
            assert!(err.contains(want), "{rest:?}: {err}");
            assert!(!err.contains("hunter2"), "a value leaked: {err}");
        }
        // [app] itself: at the file's start, or at the table.
        let err = parse_err("[classes.t]\ncomponent='t'\n");
        assert!(err.starts_with("wasmkeep.toml:1:1: "), "{err}");
        assert!(err.contains("[app] is required"), "{err}");
        let err = parse_err("[app]\ntenant='acme'\n[classes.t]\ncomponent='t'\n");
        assert!(err.starts_with("wasmkeep.toml:1:2: "), "{err}");
        assert!(err.contains("[app] name is required"), "{err}");
        let err = parse_err("[app]\ntenant='acme'\nname='x'\n");
        assert!(err.starts_with("wasmkeep.toml:1:1: "), "{err}");
        assert!(err.contains("nothing to deploy"), "{err}");
        let err = parse_err("[app]\ntenant='Acme'\nname='x'\n[classes.t]\ncomponent='t'\n");
        assert!(err.starts_with("wasmkeep.toml:2:8"), "{err}");
        let err =
            parse_err("[app]\ntenant='a'\nname='x'\nbucket='s3://b'\n[classes.t]\ncomponent='t'\n");
        assert!(err.contains("unknown key \"bucket\" in [app]"), "{err}");
        // Not TOML.
        let err = parse_err("[app\n");
        assert!(err.starts_with("wasmkeep.toml:1:"), "{err}");
    }

    fn parts(front: bool, config: &[(&str, &str)]) -> Parts {
        Parts {
            front: front.then(|| b"front".to_vec()),
            classes: vec![
                ("pub".to_owned(), b"p".to_vec()),
                ("rooms".to_owned(), b"r".to_vec()),
                ("todos".to_owned(), b"t".to_vec()),
            ],
            config: config
                .iter()
                .map(|(s, text)| ((*s).to_owned(), wasmkeep_server::env::parse(text).unwrap()))
                .collect(),
        }
    }

    #[test]
    fn the_changes_are_the_whole_file() {
        use wasmkeep_server::apps::FrontChange;
        let p = parse_ok(FULL);
        let c = to_changes(&p, parts(true, &[("_front", "A=1"), ("rooms", "B=2")]));
        assert!(c.every_rule);
        let names: Vec<&str> = c
            .classes
            .as_ref()
            .unwrap()
            .iter()
            .map(|(n, _)| n.as_str())
            .collect();
        assert_eq!(names, ["pub", "rooms", "todos"]);
        // Every class's rule, open included.
        assert_eq!(
            c.access,
            [
                ("pub".to_owned(), "open".to_owned()),
                ("rooms".to_owned(), "existing:lobby".to_owned()),
                ("todos".to_owned(), "internal".to_owned()),
            ]
        );
        // Every class's configuration: removed where the file has none.
        let config: Vec<(&str, bool)> = c
            .config
            .iter()
            .map(|(n, v)| (n.as_str(), v.is_some()))
            .collect();
        assert_eq!(config, [("pub", false), ("rooms", true), ("todos", false)]);
        // Every class's required names, empty removing them.
        let requires: Vec<(&str, usize)> = c
            .requires
            .iter()
            .map(|(n, r)| (n.as_str(), r.len()))
            .collect();
        assert_eq!(requires, [("pub", 0), ("rooms", 2), ("todos", 0)]);
        assert!(matches!(c.front, FrontChange::Set(ref b) if b == b"front"));
        assert!(c.front_config.as_ref().unwrap().is_some());
        assert_eq!(c.front_requires, Some(["TOKENS".to_owned()].into()));

        // No [front]: the front is removed, with nothing of its own.
        let mut p = p;
        p.front = None;
        let c = to_changes(&p, parts(false, &[]));
        assert!(matches!(c.front, FrontChange::Remove));
        assert!(c.front_config.is_none() && c.front_requires.is_none());
        // A front without config removes the front's configuration.
        let mut p = parse_ok(FULL);
        if let Some(f) = p.front.as_mut() {
            f.config = None;
            f.requires.clear();
        }
        let c = to_changes(&p, parts(true, &[]));
        assert_eq!(c.front_config, Some(None));
        assert_eq!(c.front_requires, Some(BTreeSet::new()));
    }

    #[test]
    fn the_examples_files_parse() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../examples");
        for example in ["todo-front", "ts-todo-front"] {
            let p = load_with(&root.join(example).join(FILE_NAME), false).unwrap();
            assert_eq!(p.classes["todos"].access, "internal", "{example}");
            assert!(
                p.front.as_ref().unwrap().requires.contains("TOKENS"),
                "{example}"
            );
            assert!(p.front.as_ref().unwrap().build.is_some(), "{example}");
        }
    }

    #[test]
    fn a_rollback_reads_only_the_app() {
        let text = "[app]\ntenant='acme'\nname='todo'\n[classes.a]\ncomponent='a'\nacess='open'\n[nonsense]\n";
        assert!(parse(text, Path::new("wasmkeep.toml")).is_err());
        let p = parse_with(text, Path::new("wasmkeep.toml"), true).unwrap();
        assert_eq!((p.tenant.as_str(), p.name.as_str()), ("acme", "todo"));
        assert!(p.classes.is_empty() && p.front.is_none());
        // [app] alone is enough, and still checked.
        assert!(parse_with("[app]\ntenant='acme'\nname='todo'\n", Path::new("w"), true).is_ok());
        let err =
            parse_with("[app]\ntenant='acme'\nnmae='todo'\n", Path::new("w"), true).unwrap_err();
        assert!(err.contains("unknown key \"nmae\""), "{err}");
        // Only the rest's schema is not read: the file must still be
        // TOML, and the error says how to roll back without it.
        let err = parse_with(
            "[app]\ntenant='acme'\nname='todo'\n[classes.a\n",
            Path::new("w"),
            true,
        )
        .unwrap_err();
        assert!(err.starts_with("w:4:"), "{err}");
        assert!(
            err.ends_with("or pass --no-project --app TENANT/APP --rollback"),
            "{err}"
        );
        // A normal deploy has no such hint.
        let err = parse(
            "[app]\ntenant='acme'\nname='todo'\n[classes.a\n",
            Path::new("w"),
        )
        .unwrap_err();
        assert!(!err.contains("--rollback"), "{err}");
    }

    #[test]
    fn the_local_values_file_is_not_configuration() {
        let base = "[app]\ntenant='a'\nname='b'\n";
        let err = parse_err(&format!(
            "{base}[classes.c]\ncomponent='c'\nconfig='.env.local'\n"
        ));
        assert!(err.contains("[classes.c] config is"), "{err}");
        let err = parse_err(&format!(
            "{base}[front]\ncomponent='f'\nconfig='local.env'\n[dev]\nenv='local.env'\n"
        ));
        assert!(err.contains("[front] config is"), "{err}");
        // The same file however it is spelled.
        let abs = std::env::current_dir().unwrap().join("app/.env.local");
        for spelled in [
            "x/../.env.local",
            "./.env.local",
            "sub/./../.env.local",
            abs.to_str().unwrap(),
        ] {
            let err = parse(
                &format!("{base}[classes.c]\ncomponent='c'\nconfig='{spelled}'\n"),
                Path::new("app/wasmkeep.toml"),
            )
            .unwrap_err();
            assert!(err.contains("[classes.c] config is"), "{spelled}: {err}");
        }
        // Another file of the same name elsewhere is not it.
        assert!(
            parse(
                &format!("{base}[classes.c]\ncomponent='c'\nconfig='sub/.env.local'\n"),
                Path::new("app/wasmkeep.toml"),
            )
            .is_ok()
        );
    }

    /// Where the configuration is read, a link to the local values file
    /// (or another spelling the file check cannot see) is refused too.
    #[cfg(unix)]
    #[test]
    fn a_link_to_the_local_values_file_is_not_configuration() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("c.wasm"), b"c").unwrap();
        std::fs::write(dir.path().join(".env.local"), "TOKEN=hunter2\n").unwrap();
        std::os::unix::fs::symlink(".env.local", dir.path().join("sym.env")).unwrap();
        std::fs::hard_link(dir.path().join(".env.local"), dir.path().join("hard.env")).unwrap();
        for config in ["sym.env", "hard.env"] {
            let text = format!(
                "[app]\ntenant='a'\nname='b'\n[classes.c]\ncomponent='c.wasm'\nconfig='{config}'\n"
            );
            let p = parse(&text, &dir.path().join(FILE_NAME)).unwrap();
            let err = read_parts(&p, &mut |_| {}).err().unwrap().to_string();
            assert!(err.contains("the [dev] env file"), "{config}: {err}");
            assert!(!err.contains("hunter2"), "{err}");
        }
        // A file of its own is read.
        std::fs::write(dir.path().join("own.env"), "MODE=x\n").unwrap();
        let text =
            "[app]\ntenant='a'\nname='b'\n[classes.c]\ncomponent='c.wasm'\nconfig='own.env'\n";
        let p = parse(text, &dir.path().join(FILE_NAME)).unwrap();
        assert!(read_parts(&p, &mut |_| {}).is_ok());
    }

    #[test]
    fn app_flag_must_name_the_file_app() {
        let p = parse_ok(FULL);
        assert!(app(&p, Some(&("acme".into(), "todo".into()))).is_ok());
        let err = app(&p, Some(&("acme".into(), "shop".into())))
            .unwrap_err()
            .to_string();
        assert!(err.contains("acme/todo"), "{err}");
        let err = refuse_flags(&p, &["--actor", "--access"])
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("--actor, --access") && err.contains("--no-project"),
            "{err}"
        );
    }

    #[test]
    fn the_plan_names_what_the_file_removes() {
        let diff = wasmkeep_server::apps::Diff {
            removed: vec![("rooms".into(), Some("internal".into()))],
            front: Some("removed"),
            ..Default::default()
        };
        let lines = plan_lines("wasmkeep.toml", "acme", "todo", &diff);
        assert_eq!(lines[0], "acme/todo from wasmkeep.toml, before switching:");
        for want in [
            "  class rooms: removed (it was internal)",
            "  front: removed",
            "  class rooms is not in wasmkeep.toml: it is removed; its actors' state stays in the bucket",
            "  the front is not in wasmkeep.toml: it is removed",
        ] {
            assert!(lines.contains(&want.to_owned()), "{want}: {lines:?}");
        }
    }

    #[cfg(unix)]
    #[test]
    fn builds_run_in_the_file_directory_and_failures_stop() {
        let dir = tempfile::tempdir().unwrap();
        run_build(dir.path(), "class t", "echo built > out.txt").unwrap();
        assert_eq!(
            std::fs::read_to_string(dir.path().join("out.txt"))
                .unwrap()
                .trim(),
            "built"
        );
        let err = run_build(dir.path(), "class t", "exit 3")
            .unwrap_err()
            .to_string();
        assert!(err.contains("the build of class t failed"), "{err}");
    }
}
