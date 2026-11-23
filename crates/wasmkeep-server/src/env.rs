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
/// `--front-env`, `wasmkeep dev`'s local values). On Unix, a file its group
/// or others can read or write is refused, as the secrets key is: it must
/// be the server's user's alone (`chmod 600`). Symlinks are followed (the
/// target's mode counts), and the file is checked and read through one
/// handle, so the path cannot be swapped in between.
///
/// # Errors
/// The file cannot be read, is too large, is open to others, or has a
/// malformed line.
pub fn read_file(path: &Path) -> Result<Vars, String> {
    read(path, true)
}

/// Reads a file of plain deployed configuration (`wasmkeep deploy
/// --config`, a project's `config`): the same format as [`read_file`], but
/// its mode is not checked, since it is stored in the clear by design.
///
/// # Errors
/// The file cannot be read, is too large, or has a malformed line.
pub fn read_config_file(path: &Path) -> Result<Vars, String> {
    read(path, false)
}

fn read(path: &Path, private: bool) -> Result<Vars, String> {
    use std::io::Read as _;
    let shown = path.display();
    let file = std::fs::File::open(path).map_err(|e| format!("{shown}: {e}"))?;
    let meta = file.metadata().map_err(|e| format!("{shown}: {e}"))?;
    if !meta.is_file() {
        return Err(format!("{shown}: not a file"));
    }
    if meta.len() > MAX_FILE {
        return Err(format!("{shown}: larger than {MAX_FILE} bytes"));
    }
    #[cfg(unix)]
    if private {
        use std::os::unix::fs::PermissionsExt;
        let mode = meta.permissions().mode();
        if mode & 0o077 != 0 {
            return Err(format!(
                "{shown}: its group or other users can open it (mode {:o}); it may hold \
                 secrets, so it must be the server's user's alone (chmod 600)",
                mode & 0o777
            ));
        }
    }
    #[cfg(not(unix))]
    let _ = private;
    let mut text = String::new();
    file.take(MAX_FILE + 1)
        .read_to_string(&mut text)
        .map_err(|e| format!("{shown}: {e}"))?;
    if text.len() as u64 > MAX_FILE {
        return Err(format!("{shown}: larger than {MAX_FILE} bytes"));
    }
    parse(&text).map_err(|e| format!("{shown}:{e}"))
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

    #[cfg(unix)]
    #[test]
    fn files_others_can_read_are_refused() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("hub.env");
        std::fs::write(&path, "TOKEN=secret-value\n").unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        let vars = read_file(&path).unwrap();
        assert_eq!(vars.get("TOKEN").unwrap(), "secret-value");
        for mode in [0o640, 0o604, 0o644, 0o620] {
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(mode)).unwrap();
            let e = read_file(&path).unwrap_err();
            assert!(e.contains("chmod 600"), "{mode:o}: {e}");
            assert!(!e.contains("secret-value"), "{e}");
        }
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        std::fs::write(&path, "BAD LINE\n").unwrap();
        let e = read_file(&path).unwrap_err();
        assert!(e.ends_with(":1: expected NAME=value"), "{e}");
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
