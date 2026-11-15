//! Secrets sealed in the bucket ([ADR 0016](../../../docs/adr/0016-secrets-in-the-bucket.md)).
//!
//! An app's secrets are one sealed object per change, with its own `seq`:
//! ChaCha20-Poly1305 under a key derived for that app (HKDF-SHA256 from the
//! secrets key, the bucket prefix, tenant and app), with the prefix, tenant,
//! app, `seq` and key id as associated data, so an object opens only at its
//! own place. Nothing here logs or prints a value or a name; errors say what
//! is wrong, never what a secret is.

use std::collections::BTreeMap;
use std::path::Path;

use ring::aead::{Aad, CHACHA20_POLY1305, LessSafeKey, NONCE_LEN, Nonce, UnboundKey};
use ring::hkdf;
use ring::rand::SystemRandom;
use sha2::{Digest, Sha256};

/// The first line of a sealed object, and the start of the associated
/// data and of the key derivation's info.
const FORMAT: &str = "wasmkeep-secrets 1";
/// Bytes in a secrets key.
pub const KEY_LEN: usize = 32;
/// Largest plaintext accepted (the same limit as an `--env` file).
pub const MAX_PLAINTEXT: usize = crate::env::MAX_FILE as usize;
/// Largest sealed object a node reads: the plaintext in hex, its tag and
/// the header.
pub const MAX_OBJECT: usize = MAX_PLAINTEXT * 2 + 256;

/// Why secrets could not be loaded, sealed or opened. Never holds a value.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SecretsError {
    /// A key file that is not 64 hex characters (and a newline).
    #[error("{0}: not a secrets key (64 hex characters expected)")]
    BadKey(String),
    /// A key file others can read.
    #[error("{0} is readable by its group or others; it holds the secrets key (chmod 600)")]
    KeyFileOpen(String),
    /// Reading or writing a key file failed.
    #[error("{0}")]
    Io(String),
    /// Sealed with a key this node does not hold.
    #[error("sealed with key {0}, which this node does not hold")]
    UnknownKey(String),
    /// Larger than [`MAX_OBJECT`] (or the plaintext than [`MAX_PLAINTEXT`]).
    #[error("too large")]
    TooLarge,
    /// Not a sealed object.
    #[error("not a sealed secrets object")]
    Malformed,
    /// It does not open: altered, or not sealed for this place and `seq`.
    #[error("does not open: altered, or sealed for another app, prefix or seq")]
    Tampered,
    /// The opened plaintext is not a valid set.
    #[error("the opened set is invalid: {0}")]
    BadSet(String),
    /// The system's random generator failed.
    #[error("no randomness available")]
    Random,
}

/// A secrets key and its id. `Debug` shows the id only.
#[derive(Clone)]
pub struct SecretsKey {
    bytes: [u8; KEY_LEN],
    id: String,
}

impl std::fmt::Debug for SecretsKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SecretsKey").field("id", &self.id).finish()
    }
}

impl SecretsKey {
    fn from_bytes(bytes: [u8; KEY_LEN]) -> Self {
        let mut digest = Sha256::new();
        digest.update(b"wasmkeep-secrets-key-id");
        digest.update(bytes);
        let id = hex(&digest.finalize()[..8]);
        Self { bytes, id }
    }

    /// A new key from the system's random generator.
    pub fn generate() -> Result<Self, SecretsError> {
        let bytes = ring::rand::generate::<[u8; KEY_LEN]>(&SystemRandom::new())
            .map_err(|_| SecretsError::Random)?
            .expose();
        Ok(Self::from_bytes(bytes))
    }

    /// A key as a key file holds it: 64 hex characters, then at most one
    /// newline. `shown` names the file in errors.
    pub fn from_text(text: &str, shown: &str) -> Result<Self, SecretsError> {
        let body = text
            .strip_suffix("\r\n")
            .or_else(|| text.strip_suffix('\n'))
            .unwrap_or(text);
        let bytes = unhex(body)
            .and_then(|v| <[u8; KEY_LEN]>::try_from(v).ok())
            .ok_or_else(|| SecretsError::BadKey(shown.to_owned()))?;
        Ok(Self::from_bytes(bytes))
    }

    /// What a key file holds (never logged: only `write_new` uses it).
    fn to_text(&self) -> String {
        format!("{}\n", hex(&self.bytes))
    }

    /// Its id: the first 8 bytes of SHA-256("wasmkeep-secrets-key-id" ||
    /// key), in hex. Safe to log.
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Reads a key file. One its group or others can read is refused.
    pub fn load(path: &Path) -> Result<Self, SecretsError> {
        use std::io::Read as _;
        let shown = path.display().to_string();
        let io = |e: std::io::Error| SecretsError::Io(format!("{shown}: {e}"));
        // Checked on the open file, then read from it: the path cannot be
        // swapped in between. Symlinks are followed: the target's mode
        // counts (a Kubernetes secret mounted 0644 is refused).
        let file = std::fs::File::open(path).map_err(io)?;
        let meta = file.metadata().map_err(io)?;
        if !meta.is_file() || meta.len() > 256 {
            return Err(SecretsError::BadKey(shown));
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if meta.permissions().mode() & 0o077 != 0 {
                return Err(SecretsError::KeyFileOpen(shown));
            }
        }
        let mut text = String::new();
        file.take(257).read_to_string(&mut text).map_err(io)?;
        Self::from_text(&text, &shown)
    }

    /// Writes a new key file: created exclusively (an existing file is
    /// refused), readable by its owner only.
    pub fn write_new(&self, path: &Path) -> Result<(), SecretsError> {
        use std::io::Write as _;
        let shown = path.display().to_string();
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options
            .open(path)
            .map_err(|e| SecretsError::Io(format!("{shown}: {e}")))?;
        let written = file
            .write_all(self.to_text().as_bytes())
            .and_then(|()| file.sync_all());
        if let Err(e) = written {
            // A partial file would make every retry fail (`create_new`).
            drop(file);
            let _ = std::fs::remove_file(path);
            return Err(SecretsError::Io(format!("{shown}: {e}")));
        }
        Ok(())
    }

    /// The key for one app's objects.
    fn app_key(&self, place: &Place<'_>) -> Result<LessSafeKey, SecretsError> {
        let mut info = Vec::new();
        for field in [FORMAT, place.prefix, place.tenant, place.app] {
            push_field(&mut info, field.as_bytes());
        }
        let prk = hkdf::Salt::new(hkdf::HKDF_SHA256, b"wasmkeep-secrets").extract(&self.bytes);
        let info = [info.as_slice()];
        let okm = prk
            .expand(&info, &CHACHA20_POLY1305)
            .map_err(|_| SecretsError::Malformed)?; // 32 bytes: within HKDF's limit
        Ok(LessSafeKey::new(UnboundKey::from(okm)))
    }
}

/// Where an app's sealed objects live: part of their key and associated
/// data, so an object opens nowhere else.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Place<'a> {
    prefix: &'a str,
    tenant: &'a str,
    app: &'a str,
}

impl<'a> Place<'a> {
    /// The app `tenant/app` under bucket prefix `prefix` (may be empty).
    /// Slashes around the prefix are ignored, as `AppRef` ignores them:
    /// `prod` and `prod/` are one place.
    pub fn new(prefix: &'a str, tenant: &'a str, app: &'a str) -> Self {
        Self {
            prefix: prefix.trim_matches('/'),
            tenant,
            app,
        }
    }
}

/// An object's name under `secrets/`: zero-padded hex, so a listing is in
/// `seq` order.
pub fn object_name(seq: u64) -> String {
    format!("{seq:016x}")
}

/// The `seq` an object name stands for.
pub fn parse_object_name(name: &str) -> Option<u64> {
    (name.len() == 16 && name.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f')))
        .then(|| u64::from_str_radix(name, 16).ok())
        .flatten()
}

fn associated_data(place: &Place<'_>, seq: u64, key_id: &str) -> Vec<u8> {
    let mut ad = Vec::new();
    for field in [
        FORMAT,
        place.prefix,
        place.tenant,
        place.app,
        &object_name(seq),
        key_id,
    ] {
        push_field(&mut ad, field.as_bytes());
    }
    ad
}

/// Seals `set` as object `seq` of the app at `place`.
pub fn seal(
    key: &SecretsKey,
    place: &Place<'_>,
    seq: u64,
    set: &SecretSet,
) -> Result<Vec<u8>, SecretsError> {
    let nonce = ring::rand::generate::<[u8; NONCE_LEN]>(&SystemRandom::new())
        .map_err(|_| SecretsError::Random)?
        .expose();
    seal_with_nonce(key, place, seq, set, nonce)
}

fn seal_with_nonce(
    key: &SecretsKey,
    place: &Place<'_>,
    seq: u64,
    set: &SecretSet,
    nonce: [u8; NONCE_LEN],
) -> Result<Vec<u8>, SecretsError> {
    let mut data = set.encode();
    if data.len() > MAX_PLAINTEXT {
        return Err(SecretsError::TooLarge);
    }
    let ad = associated_data(place, seq, key.id());
    key.app_key(place)?
        .seal_in_place_append_tag(
            Nonce::assume_unique_for_key(nonce),
            Aad::from(ad),
            &mut data,
        )
        .map_err(|_| SecretsError::TooLarge)?;
    Ok(format!("{FORMAT}\n{}\n{}\n{}\n", key.id(), hex(&nonce), hex(&data)).into_bytes())
}

/// The key id an object was sealed with, without opening it.
pub fn sealed_with(object: &[u8]) -> Result<String, SecretsError> {
    Ok(parse_object(object)?.0.to_owned())
}

fn parse_object(object: &[u8]) -> Result<(&str, [u8; NONCE_LEN], Vec<u8>), SecretsError> {
    if object.len() > MAX_OBJECT {
        return Err(SecretsError::TooLarge);
    }
    let text = std::str::from_utf8(object).map_err(|_| SecretsError::Malformed)?;
    let body = text.strip_suffix('\n').ok_or(SecretsError::Malformed)?;
    let mut lines = body.split('\n');
    let (Some(format), Some(key_id), Some(nonce), Some(sealed), None) = (
        lines.next(),
        lines.next(),
        lines.next(),
        lines.next(),
        lines.next(),
    ) else {
        return Err(SecretsError::Malformed);
    };
    if format != FORMAT || key_id.len() != 16 || unhex(key_id).is_none() {
        return Err(SecretsError::Malformed);
    }
    let nonce = unhex(nonce)
        .and_then(|v| <[u8; NONCE_LEN]>::try_from(v).ok())
        .ok_or(SecretsError::Malformed)?;
    let sealed = unhex(sealed).ok_or(SecretsError::Malformed)?;
    if sealed.len() < CHACHA20_POLY1305.tag_len() {
        return Err(SecretsError::Malformed);
    }
    Ok((key_id, nonce, sealed))
}

/// Opens object `seq` of the app at `place` with whichever of `keys` it
/// was sealed with.
pub fn open(
    keys: &[SecretsKey],
    place: &Place<'_>,
    seq: u64,
    object: &[u8],
) -> Result<SecretSet, SecretsError> {
    let (key_id, nonce, mut sealed) = parse_object(object)?;
    let key = keys
        .iter()
        .find(|k| k.id() == key_id)
        .ok_or_else(|| SecretsError::UnknownKey(key_id.to_owned()))?;
    let ad = associated_data(place, seq, key_id);
    let plain = key
        .app_key(place)?
        .open_in_place(
            Nonce::assume_unique_for_key(nonce),
            Aad::from(ad),
            &mut sealed,
        )
        .map_err(|_| SecretsError::Tampered)?;
    SecretSet::decode(plain)
}

/// An app's secrets: each class's names and values. `Debug` shows how many
/// each class has, never a name or a value.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct SecretSet {
    classes: BTreeMap<String, crate::env::Vars>,
}

impl std::fmt::Debug for SecretSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_map()
            .entries(self.classes.iter().map(|(c, v)| (c, v.len())))
            .finish()
    }
}

impl SecretSet {
    /// Sets `name` for `class`.
    pub fn set(&mut self, class: &str, name: &str, value: &str) -> Result<(), SecretsError> {
        check_entry(class, name, value)?;
        self.classes
            .entry(class.to_owned())
            .or_default()
            .insert(name.to_owned(), value.to_owned());
        Ok(())
    }

    /// Removes `name` from `class`; whether it was set.
    pub fn unset(&mut self, class: &str, name: &str) -> bool {
        let Some(vars) = self.classes.get_mut(class) else {
            return false;
        };
        let removed = vars.remove(name).is_some();
        if vars.is_empty() {
            self.classes.remove(class);
        }
        removed
    }

    /// `class`'s secrets, if it has any. A `Vars` prints its values in
    /// `Debug`: never log it.
    pub fn class(&self, class: &str) -> Option<&crate::env::Vars> {
        self.classes.get(class)
    }

    /// Whether the set has an entry for the front (scope `_front`, ADR
    /// 0023), which nodes from before it cannot read: such a node fails to
    /// open the whole set.
    pub fn has_front(&self) -> bool {
        self.classes.contains_key(FRONT_SCOPE)
    }

    /// Every class with secrets, and its names (no values), for listing.
    pub fn names(&self) -> Vec<(String, Vec<String>)> {
        self.classes
            .iter()
            .map(|(c, v)| (c.clone(), v.keys().cloned().collect()))
            .collect()
    }

    /// The plaintext: `<class> <name> <length>\n<value>\n` per secret, in
    /// order. Values may hold any UTF-8, newlines included.
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        for (class, vars) in &self.classes {
            for (name, value) in vars {
                out.extend_from_slice(format!("{class} {name} {}\n", value.len()).as_bytes());
                out.extend_from_slice(value.as_bytes());
                out.push(b'\n');
            }
        }
        out
    }

    /// Reads [`encode`](Self::encode)'s format (also `--file` input for the
    /// CLI). Errors give the entry's position, never a value.
    pub fn decode(data: &[u8]) -> Result<Self, SecretsError> {
        if data.len() > MAX_PLAINTEXT {
            return Err(SecretsError::TooLarge);
        }
        let mut set = Self::default();
        let mut rest = data;
        let mut entry = 0;
        while !rest.is_empty() {
            entry += 1;
            let bad = |what: &str| SecretsError::BadSet(format!("entry {entry}: {what}"));
            let end = rest
                .iter()
                .position(|b| *b == b'\n')
                .ok_or_else(|| bad("no header line"))?;
            let header = std::str::from_utf8(&rest[..end]).map_err(|_| bad("header not UTF-8"))?;
            let mut parts = header.split(' ');
            let (Some(class), Some(name), Some(len), None) =
                (parts.next(), parts.next(), parts.next(), parts.next())
            else {
                return Err(bad("expected `<class> <name> <length>`"));
            };
            // Plain digits, no sign, no leading zero: one encoding per set.
            let canonical = !len.is_empty()
                && len.bytes().all(|b| b.is_ascii_digit())
                && (len == "0" || !len.starts_with('0'));
            let len: usize = canonical
                .then(|| len.parse().ok())
                .flatten()
                .ok_or_else(|| bad("bad length"))?;
            let value_start = end + 1;
            let value_end = value_start
                .checked_add(len)
                .ok_or_else(|| bad("bad length"))?;
            if rest.get(value_end) != Some(&b'\n') {
                return Err(bad(
                    "value shorter than its length, or not ended by a newline",
                ));
            }
            let value = std::str::from_utf8(&rest[value_start..value_end])
                .map_err(|_| bad("value not UTF-8"))?;
            if set.class(class).is_some_and(|v| v.contains_key(name)) {
                return Err(bad("name set twice"));
            }
            // The class or name may be part of a value whose length was
            // wrong: the error says which entry, never the text.
            set.set(class, name, value).map_err(|e| match e {
                SecretsError::BadSet(_) => bad("invalid class or name"),
                other => other,
            })?;
            rest = &rest[value_end + 1..];
        }
        Ok(set)
    }
}

fn check_entry(class: &str, name: &str, value: &str) -> Result<(), SecretsError> {
    // The same rule as actor addresses' classes, or the front's scope
    // (ADR 0023), which no class can be.
    if class != FRONT_SCOPE && wasmkeep_runtime::ActorAddress::new("t", class, "x").is_err() {
        return Err(SecretsError::BadSet(format!(
            "invalid class {class:?}: use [a-z][a-z0-9-]{{0,63}}, or {FRONT_SCOPE} for the front"
        )));
    }
    if !crate::env::valid_name(name) {
        return Err(SecretsError::BadSet(format!(
            "invalid name {name:?}: use letters, digits and _, not starting with a digit, at most {} bytes",
            crate::env::MAX_NAME
        )));
    }
    if value.len() > MAX_PLAINTEXT {
        return Err(SecretsError::TooLarge);
    }
    Ok(())
}

/// The front's scope (ADR 0023): `_front <name> <length>` entries.
pub const FRONT_SCOPE: &str = "_front";

fn push_field(out: &mut Vec<u8>, field: &[u8]) {
    // Every field is far below 4 GiB (names, ids, a prefix).
    let len = u32::try_from(field.len()).unwrap_or(u32::MAX);
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(field);
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(out, "{b:02x}");
    }
    out
}

fn unhex(s: &str) -> Option<Vec<u8>> {
    if !s.len().is_multiple_of(2) || !s.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f')) {
        return None;
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).ok())
        .collect()
}

#[cfg(test)]
mod tests;
