//! Configuration and secrets for actor classes: the `env` interface.
//!
//! The operator gives each class values in files on the node (`wasmkeep
//! serve --env CLASS=FILE`), read once at start. They sit over the deployed
//! configuration and the app's secrets from the bucket (ADR 0013, ADR 0016),
//! by name; they never come from the component, and are the same for every
//! actor of the class. Errors name the file and line, never a value: these
//! files hold secrets.

use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::sync::Arc;

/// One class's values.
pub type Vars = BTreeMap<String, String>;

/// Largest file accepted, in bytes.
pub const MAX_FILE: u64 = 1024 * 1024;
/// Longest name.
pub const MAX_NAME: usize = 128;

/// Every class's values. Classes without any get an empty set. Its `Debug`
/// shows how many values each class has, never a name or value.
#[derive(Clone, Default)]
pub struct ActorEnv {
    classes: HashMap<String, Arc<Vars>>,
    empty: Arc<Vars>,
}

impl std::fmt::Debug for ActorEnv {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_map().entries(self.summary()).finish()
    }
}

impl ActorEnv {
    /// Adds `vars` to `class`; a name set twice keeps the later value.
    pub fn extend(&mut self, class: &str, vars: Vars) {
        let entry = self.classes.entry(class.to_owned()).or_default();
        Arc::make_mut(entry).extend(vars);
    }

    /// The values of `class`.
    #[must_use]
    pub fn for_class(&self, class: &str) -> Arc<Vars> {
        self.classes
            .get(class)
            .map_or_else(|| Arc::clone(&self.empty), Arc::clone)
    }

    /// Classes with values, and how many each has (for the start-up log;
    /// names and values stay out of it).
    #[must_use]
    pub fn summary(&self) -> Vec<(String, usize)> {
        let mut out: Vec<_> = self
            .classes
            .iter()
            .map(|(c, v)| (c.clone(), v.len()))
            .collect();
        out.sort();
        out
    }
}

/// Reads an env file of configuration and secrets (`--env`,
/// `--front-env`). On Unix it is refused, as the secrets key is, if other
/// users can open it, if its group can write it, or if its group can read
/// it while it is the server's user's own file; see [`private_enough`].
/// So `0600` or `0400` the server's user's, or `0640` or `0440` root's
/// with the server's group (the service then cannot rewrite its own
/// secrets). Symlinks are followed (the target's mode counts, as for a
/// mounted secret), a fifo or other non-file is refused before it is
/// opened, and the file is checked and read through one handle, so the
/// path cannot be swapped in between.
///
/// # Errors
/// The file cannot be read, is too large, is open to others, or has a
/// malformed line.
pub fn read_file(path: &Path) -> Result<Vars, String> {
    read(path, Check::Private).map(|(vars, _)| vars)
}

/// Reads `wasmkeep dev`'s local values (`[dev] env`, `.env.local`) as
/// [`read_file`] does, except on a filesystem that keeps no file modes
/// (`chmod` has no effect there): the file is read, with the warning
/// returned.
///
/// # Errors
/// As [`read_file`].
pub fn read_local_file(path: &Path) -> Result<(Vars, Option<String>), String> {
    read(path, Check::Local)
}

/// Reads a file of plain deployed configuration (`wasmkeep deploy
/// --config`, a project's `config`): the same format as [`read_file`], but
/// its mode is not checked, since it is stored in the clear by design.
///
/// # Errors
/// The file cannot be read, is too large, or has a malformed line.
pub fn read_config_file(path: &Path) -> Result<Vars, String> {
    read(path, Check::None).map(|(vars, _)| vars)
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Check {
    None,
    Private,
    Local,
}

/// Whether a file of secrets with `mode`, owned by `owner`, may be read by
/// a server running as `euid`: `Err` says why not and how to fix it.
#[cfg_attr(not(unix), allow(dead_code))]
fn private_enough(mode: u32, owner: u32, euid: u32) -> Result<(), String> {
    let shown = mode & 0o777;
    if mode & 0o007 != 0 {
        return Err(format!(
            "other users can open it (mode {shown:o}); it may hold secrets (chmod o= FILE)"
        ));
    }
    if mode & 0o020 != 0 {
        return Err(format!(
            "its group can write it (mode {shown:o}); it may hold secrets (chmod g-w FILE)"
        ));
    }
    if mode & 0o040 != 0 && owner == euid && owner != 0 {
        return Err(format!(
            "its group can read it (mode {shown:o}) and it is the server's own file; it may \
             hold secrets (chmod 600 FILE, or make root its owner and the server's group \
             its group)"
        ));
    }
    Ok(())
}

/// `refused` for local values: a warning instead when the filesystem keeps
/// no modes (`modes_kept` false).
#[cfg_attr(not(unix), allow(dead_code))]
fn local_verdict(refused: Result<(), String>, modes_kept: bool) -> Result<Option<String>, String> {
    match refused {
        Ok(()) => Ok(None),
        Err(why) if !modes_kept => Ok(Some(format!(
            "{why}; read anyway, since its filesystem keeps no file modes"
        ))),
        Err(why) => Err(why),
    }
}

/// Whether the filesystem holding `dir` keeps the modes files are created
/// with: a probe file made `0600` there, checked, then removed.
#[cfg(unix)]
fn modes_kept(dir: &Path) -> bool {
    use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
    let probe = dir.join(format!(".wasmkeep-mode-probe-{}", std::process::id()));
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true).mode(0o600);
    let Ok(file) = options.open(&probe) else {
        // Cannot tell: keep the rule.
        return true;
    };
    let kept = file
        .metadata()
        .map(|m| m.permissions().mode() & 0o777 == 0o600)
        .unwrap_or(true);
    drop(file);
    let _ = std::fs::remove_file(&probe);
    kept
}

fn read(path: &Path, check: Check) -> Result<(Vars, Option<String>), String> {
    use std::io::Read as _;
    let shown = path.display();
    // Before opening: opening a fifo would block.
    let meta = std::fs::metadata(path).map_err(|e| format!("{shown}: {e}"))?;
    if !meta.is_file() {
        return Err(format!("{shown}: not a file"));
    }
    let file = std::fs::File::open(path).map_err(|e| format!("{shown}: {e}"))?;
    let meta = file.metadata().map_err(|e| format!("{shown}: {e}"))?;
    if !meta.is_file() {
        return Err(format!("{shown}: not a file"));
    }
    if meta.len() > MAX_FILE {
        return Err(format!("{shown}: larger than {MAX_FILE} bytes"));
    }
    let mut warning = None;
    #[cfg(unix)]
    if check != Check::None {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        let euid = rustix::process::geteuid().as_raw();
        let verdict = private_enough(meta.permissions().mode(), meta.uid(), euid);
        let verdict = if check == Check::Local && verdict.is_err() {
            let dir = path.parent().filter(|d| !d.as_os_str().is_empty());
            local_verdict(verdict, modes_kept(dir.unwrap_or(Path::new("."))))
        } else {
            verdict.map(|()| None)
        };
        warning = verdict
            .map_err(|why| format!("{shown}: {why}"))?
            .map(|w| format!("{shown}: {w}"));
    }
    #[cfg(not(unix))]
    let _ = check;
    let mut text = String::new();
    file.take(MAX_FILE + 1)
        .read_to_string(&mut text)
        .map_err(|e| format!("{shown}: {e}"))?;
    if text.len() as u64 > MAX_FILE {
        return Err(format!("{shown}: larger than {MAX_FILE} bytes"));
    }
    let vars = parse(&text).map_err(|e| format!("{shown}:{e}"))?;
    Ok((vars, warning))
}

/// Parses `NAME=value` lines, as systemd's `EnvironmentFile=` and most
/// dotenv tools read them:
///
/// - blank lines and lines starting with `#` are skipped;
/// - an `export ` prefix is allowed;
/// - the name is `[A-Za-z_][A-Za-z0-9_]*`;
/// - the value runs to the end of the line, without surrounding spaces; one
///   pair of matching quotes around it (`"..."` or `'...'`) is removed, and
///   nothing inside is interpreted (no escapes, no `$VAR`);
/// - a name set twice keeps the later value.
///
/// # Errors
/// `"<line>: <problem>"` for the first malformed line. The message never
/// contains the value.
pub fn parse(text: &str) -> Result<Vars, String> {
    let mut vars = Vars::new();
    for (i, line) in text.lines().enumerate() {
        let n = i + 1;
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let line = line.strip_prefix("export ").unwrap_or(line);
        let Some((name, value)) = line.split_once('=') else {
            return Err(format!("{n}: expected NAME=value"));
        };
        let name = name.trim();
        if !valid_name(name) {
            return Err(format!(
                "{n}: the name must be letters, digits and _, not starting with a digit, \
                 at most {MAX_NAME} characters"
            ));
        }
        let value = value.trim();
        let value = ['"', '\'']
            .iter()
            .find_map(|q| {
                value
                    .strip_prefix(*q)
                    .and_then(|v| v.strip_suffix(*q))
                    .filter(|_| value.len() >= 2)
            })
            .unwrap_or(value);
        if value.chars().any(|c| c.is_control() && c != '\t') {
            return Err(format!("{n}: {name}: control characters in the value"));
        }
        vars.insert(name.to_owned(), value.to_owned());
    }
    Ok(vars)
}

/// Whether `name` may name a value: letters, digits and `_`, not
/// starting with a digit, at most [`MAX_NAME`] bytes.
pub fn valid_name(name: &str) -> bool {
    let mut bytes = name.bytes();
    name.len() <= MAX_NAME
        && bytes
            .next()
            .is_some_and(|b| b.is_ascii_alphabetic() || b == b'_')
        && bytes.all(|b| b.is_ascii_alphanumeric() || b == b'_')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lines_parse_as_systemd_and_dotenv_read_them() {
        let vars = parse(
            "# a comment\n\
             \n\
             PLAIN=value\n\
             export EXPORTED = spaced value \n\
             DOUBLE=\"quoted # not a comment\"\n\
             SINGLE='single'\n\
             EMPTY=\n\
             EQUALS=a=b=c\n\
             LONE=\"\n\
             PLAIN=later wins\n",
        )
        .unwrap();
        let get = |k: &str| vars.get(k).map(String::as_str);
        assert_eq!(get("PLAIN"), Some("later wins"));
        assert_eq!(get("EXPORTED"), Some("spaced value"));
        assert_eq!(get("DOUBLE"), Some("quoted # not a comment"));
        assert_eq!(get("SINGLE"), Some("single"));
        assert_eq!(get("EMPTY"), Some(""));
        assert_eq!(get("EQUALS"), Some("a=b=c"));
        assert_eq!(get("LONE"), Some("\""), "a lone quote is kept");
        assert_eq!(vars.len(), 7);
    }

    #[test]
    fn malformed_lines_are_refused_without_showing_the_value() {
        for (text, line) in [
            ("OK=1\nno equals sign here\n", "2:"),
            ("1ABC=secret-value\n", "1:"),
            ("A-B=secret-value\n", "1:"),
            ("=secret-value\n", "1:"),
            ("A=secret\u{7}value\n", "1:"),
        ] {
            let e = parse(text).unwrap_err();
            assert!(e.starts_with(line), "{e}");
            assert!(!e.contains("secret"), "the value leaked: {e}");
        }
        assert!(parse(&format!("{}=1", "A".repeat(MAX_NAME + 1))).is_err());
        assert!(parse(&format!("{}=1", "A".repeat(MAX_NAME))).is_ok());
    }

    #[test]
    fn classes_see_only_their_own_values() {
        let mut env = ActorEnv::default();
        env.extend("hub", parse("TOKEN=a\nURL=u").unwrap());
        env.extend("hub", parse("TOKEN=b").unwrap());
        assert_eq!(env.for_class("hub").get("TOKEN").unwrap(), "b");
        assert_eq!(env.for_class("hub").get("URL").unwrap(), "u");
        assert!(env.for_class("monitor").is_empty());
        assert_eq!(env.summary(), vec![("hub".to_owned(), 2)]);
        let shown = format!("{env:?}");
        assert!(
            !shown.contains("TOKEN") && !shown.contains("URL"),
            "{shown}"
        );
    }

    /// Who may open a file of secrets: never other users, never its group
    /// for writing, its group for reading only when the file is root's (or
    /// another user's: a mounted secret with a group) rather than the
    /// server's own.
    #[test]
    fn the_mode_rule() {
        const ROOT: u32 = 0;
        const ME: u32 = 1000;
        const OTHER: u32 = 2000;
        for (mode, owner, euid) in [
            (0o600, ME, ME),
            (0o400, ME, ME),
            (0o640, ROOT, ME),
            (0o440, ROOT, ME),
            (0o640, OTHER, ME),
            (0o440, OTHER, ME),
            (0o640, ROOT, ROOT),
            (0o600, ROOT, ROOT),
        ] {
            assert!(
                private_enough(mode, owner, euid).is_ok(),
                "{mode:o} {owner} {euid}"
            );
        }
        for (mode, owner, euid) in [
            (0o640, ME, ME),
            (0o440, ME, ME),
            (0o604, ME, ME),
            (0o644, ROOT, ME),
            (0o601, ROOT, ME),
            (0o660, ROOT, ME),
            (0o620, ME, ME),
            (0o620, ROOT, ME),
            (0o666, OTHER, ME),
        ] {
            let e = private_enough(mode, owner, euid).unwrap_err();
            assert!(e.contains("chmod"), "{mode:o} {owner} {euid}: {e}");
        }
    }

    #[cfg(unix)]
    #[test]
    fn files_others_can_open_are_refused() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("hub.env");
        std::fs::write(&path, "TOKEN=secret-value\n").unwrap();
        for mode in [0o600, 0o400] {
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(mode)).unwrap();
            let vars = read_file(&path).unwrap();
            assert_eq!(vars.get("TOKEN").unwrap(), "secret-value");
        }
        let mut refused = vec![0o604, 0o644, 0o620];
        // This test's files are its own user's: group read is refused
        // unless that user is root.
        if rustix::process::geteuid().as_raw() != 0 {
            refused.push(0o640);
        }
        for mode in refused {
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(mode)).unwrap();
            let e = read_file(&path).unwrap_err();
            assert!(e.contains("chmod"), "{mode:o}: {e}");
            assert!(!e.contains("secret-value"), "{e}");
        }
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        std::fs::write(&path, "BAD LINE\n").unwrap();
        let e = read_file(&path).unwrap_err();
        assert!(e.ends_with(":1: expected NAME=value"), "{e}");
    }

    /// A fifo (or any other non-file) is refused before it is opened, so
    /// the start does not block on it.
    #[cfg(unix)]
    #[test]
    fn a_fifo_is_refused_without_blocking() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("hub.env");
        rustix::fs::mknodat(
            rustix::fs::CWD,
            &path,
            rustix::fs::FileType::Fifo,
            rustix::fs::Mode::from_raw_mode(0o600),
            0,
        )
        .unwrap();
        let (done, result) = std::sync::mpsc::channel();
        let read = path.clone();
        std::thread::spawn(move || {
            let _ = done.send(read_file(&read));
        });
        let e = result
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("reading a fifo blocked")
            .unwrap_err();
        assert!(e.ends_with("not a file"), "{e}");
    }

    /// `wasmkeep dev`'s local values on a filesystem that keeps no modes
    /// (a mounted Windows drive, say) are read with a warning.
    #[test]
    fn local_values_where_modes_are_not_kept_warn() {
        let refused = Err("its group can read it".to_owned());
        assert!(local_verdict(refused.clone(), true).is_err());
        let warning = local_verdict(refused, false).unwrap().unwrap();
        assert!(warning.contains("keeps no file modes"), "{warning}");
        assert_eq!(local_verdict(Ok(()), true).unwrap(), None);
    }

    /// Deployed configuration is plain by design (it goes to the bucket in
    /// the clear): its files are not checked.
    #[cfg(unix)]
    #[test]
    fn config_files_may_be_shared() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("c.conf");
        std::fs::write(&path, "MODEL=small\n").unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert_eq!(
            read_config_file(&path).unwrap().get("MODEL").unwrap(),
            "small"
        );
    }
}
