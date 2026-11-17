//! What a guest may name (ADR 0030, sections 2 and 7): keys, listing
//! prefixes and content types, checked before anything reaches the bucket
//! or the disk; the namespace a caller's files live in; and the app's
//! epoch.

use sha2::{Digest, Sha256};

/// A key's longest spelling, in bytes.
pub(crate) const MAX_KEY: usize = 512;
/// A key segment's longest spelling, in bytes.
pub(crate) const MAX_SEGMENT: usize = 255;
/// A content type's longest spelling, in bytes.
pub(crate) const MAX_CONTENT_TYPE: usize = 256;
/// The type of a file written without one.
pub(crate) const DEFAULT_CONTENT_TYPE: &str = "application/octet-stream";

fn alphabet(b: u8) -> bool {
    b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'~' | b'-')
}

fn segment(s: &str) -> Result<(), String> {
    if s.is_empty() {
        return Err("an empty segment".into());
    }
    if s == "." || s == ".." {
        return Err(format!("a segment `{s}`"));
    }
    if s.len() > MAX_SEGMENT {
        return Err(format!("a segment longer than {MAX_SEGMENT} bytes"));
    }
    if let Some(b) = s.bytes().find(|b| !alphabet(*b)) {
        return Err(format!(
            "the byte {b:#04x}: a key takes only A-Z, a-z, 0-9, `.`, `_`, `~`, `-` and `/`"
        ));
    }
    Ok(())
}

/// Checks a key: 1 to 512 bytes of `[A-Za-z0-9._~-]` and `/`, in segments of
/// at most 255 bytes separated by single `/`, none empty, `.` or `..`. The
/// error says why, for `invalid-key`.
pub(crate) fn check_key(key: &str) -> Result<(), String> {
    if key.is_empty() {
        return Err("a key is 1 to 512 bytes; this one is empty".into());
    }
    if key.len() > MAX_KEY {
        return Err(format!(
            "a key is 1 to {MAX_KEY} bytes; this one is {}",
            key.len()
        ));
    }
    if key.starts_with('/') || key.ends_with('/') {
        return Err("a key does not start or end with `/`".into());
    }
    key.split('/').try_for_each(segment)
}

/// Checks a listing prefix: empty, or what a key may start with. Every
/// segment before the last `/` is a whole key segment; what follows it is
/// over the alphabet and may be empty (a prefix may end with `/`).
pub(crate) fn check_prefix(prefix: &str) -> Result<(), String> {
    if prefix.is_empty() {
        return Ok(());
    }
    if prefix.len() > MAX_KEY {
        return Err(format!("a prefix is at most {MAX_KEY} bytes"));
    }
    if prefix.starts_with('/') {
        return Err("a prefix does not start with `/`".into());
    }
    let (whole, partial) = match prefix.rfind('/') {
        Some(i) => (&prefix[..i], &prefix[i + 1..]),
        None => ("", prefix),
    };
    if !whole.is_empty() {
        whole.split('/').try_for_each(segment)?;
    }
    if partial.len() > MAX_SEGMENT {
        return Err(format!("a segment longer than {MAX_SEGMENT} bytes"));
    }
    if let Some(b) = partial.bytes().find(|b| !alphabet(*b)) {
        return Err(format!("the byte {b:#04x} in a prefix"));
    }
    Ok(())
}

fn token(b: u8) -> bool {
    // RFC 9110 `tchar`.
    b.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&b)
}

fn is_token(s: &str) -> bool {
    !s.is_empty() && s.bytes().all(token)
}

fn is_quoted(s: &str) -> bool {
    let Some(inner) = s.strip_prefix('"').and_then(|s| s.strip_suffix('"')) else {
        return false;
    };
    let mut escaped = false;
    for b in inner.bytes() {
        if escaped {
            if !(b == b'\t' || (0x20..0x7f).contains(&b)) {
                return false;
            }
            escaped = false;
        } else if b == b'\\' {
            escaped = true;
        } else if b == b'"' || !(b == b'\t' || (0x20..0x7f).contains(&b)) {
            return false;
        }
    }
    !escaped
}

/// The content type a write stores: the one given, checked (at most 256
/// bytes, `type/subtype` and `;`-parameters of RFC 9110 tokens and quoted
/// strings, no control characters), or `application/octet-stream`.
pub(crate) fn content_type(given: Option<&str>) -> Result<String, String> {
    let Some(given) = given else {
        return Ok(DEFAULT_CONTENT_TYPE.to_owned());
    };
    let bad = || {
        format!(
            "the content type `{}` is not `type/subtype` with optional parameters",
            given.escape_debug()
        )
    };
    if given.len() > MAX_CONTENT_TYPE {
        return Err(format!(
            "a content type is at most {MAX_CONTENT_TYPE} bytes"
        ));
    }
    // Quoted strings may hold `;`: split outside quotes only.
    let mut parts = Vec::new();
    let (mut start, mut quoted, mut escaped) = (0, false, false);
    for (i, b) in given.bytes().enumerate() {
        if escaped {
            escaped = false;
        } else if quoted && b == b'\\' {
            escaped = true;
        } else if b == b'"' {
            quoted = !quoted;
        } else if b == b';' && !quoted {
            parts.push(&given[start..i]);
            start = i + 1;
        }
    }
    parts.push(&given[start..]);
    let mut parts = parts.into_iter();
    let essence = parts.next().unwrap_or_default();
    let Some((ty, sub)) = essence.split_once('/') else {
        return Err(bad());
    };
    if !is_token(ty) || !is_token(sub) {
        return Err(bad());
    }
    for p in parts {
        let p = p.trim_matches([' ', '\t']);
        let Some((name, value)) = p.split_once('=') else {
            return Err(bad());
        };
        if !is_token(name) || !(is_token(value) || is_quoted(value)) {
            return Err(bad());
        }
    }
    Ok(given.to_owned())
}

/// Whose files: an actor's (`<class>/<name id>/<incarnation>`) or the
/// front's (`_front/main/0`). Built by the host from where the code runs,
/// never from a guest's argument.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct Namespace {
    rel: String,
}

impl Namespace {
    /// An actor's namespace at `incarnation` (`0` for an actor never
    /// deleted since this release): its name is hashed, so its length and
    /// bytes never matter.
    pub(crate) fn actor(class: &str, name: &str, incarnation: &str) -> Self {
        Self {
            rel: format!("{class}/{}/{incarnation}", name_id(name)),
        }
    }

    /// The front's namespace.
    pub(crate) fn front() -> Self {
        Self {
            rel: format!(
                "{}/{}/0",
                crate::actor::FRONT_CLASS,
                crate::actor::FRONT_NAME
            ),
        }
    }

    /// `<class>/<name id>/<incarnation>`, relative to the epoch's folder.
    pub(crate) fn rel(&self) -> &str {
        &self.rel
    }
}

/// An actor name's id in keys: its SHA-256, 64 lowercase hex digits.
pub(crate) fn name_id(name: &str) -> String {
    hex(&Sha256::digest(name.as_bytes()))
}

/// The app's epoch (ADR 0030, section 2, decision 1): the first 16 hex
/// digits of the SHA-256 of the `recreated` marker's body, one zero byte,
/// and the `restored` marker's body, an absent marker counting as empty.
pub fn epoch(recreated: Option<&[u8]>, restored: Option<&[u8]>) -> String {
    let mut h = Sha256::new();
    h.update(recreated.unwrap_or_default());
    h.update([0u8]);
    h.update(restored.unwrap_or_default());
    hex(&h.finalize())[..16].to_owned()
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_over_the_alphabet_in_whole_segments_are_taken() {
        for key in [
            "a",
            "photo.jpg",
            "uploads/2026/11/x_y-z~1.png",
            "..a/.b/c..",
            &"k".repeat(MAX_SEGMENT),
            &format!("{}/{}/b", "a".repeat(255), "b".repeat(254)),
        ] {
            assert_eq!(check_key(key), Ok(()), "{key}");
        }
    }

    #[test]
    fn every_other_key_is_refused() {
        let long = "a/".repeat(256) + "a";
        for key in [
            "",
            "/a",
            "a/",
            "a//b",
            ".",
            "..",
            "a/./b",
            "a/../b",
            "../x",
            "a\\b",
            "a%2Fb",
            "a b",
            "é",
            "a\nb",
            "a\0b",
            "a?b",
            "a#b",
            "a*b",
            "a:b",
            &"k".repeat(MAX_SEGMENT + 1),
            &long,
            &"a".repeat(MAX_KEY + 1),
        ] {
            assert!(check_key(key).is_err(), "{key:?} was taken");
        }
    }

    #[test]
    fn prefixes_are_keys_cut_anywhere() {
        for p in [
            "",
            "up",
            "uploads/",
            "uploads/a",
            "a/b/",
            ".",
            "a/.",
            "a/..x",
        ] {
            assert_eq!(check_prefix(p), Ok(()), "{p}");
        }
        for p in [
            "/",
            "/a",
            "a//",
            "./",
            "../a",
            "a b",
            "a/%",
            &"a".repeat(MAX_KEY + 1),
        ] {
            assert!(check_prefix(p).is_err(), "{p:?} was taken");
        }
    }

    #[test]
    fn a_key_has_one_spelling_in_the_bucket() {
        // `object_store`'s `Path::from` would escape `~`; `Path::parse`
        // keeps every byte of the alphabet as it is.
        let all: String = (0u8..128)
            .filter(|b| alphabet(*b))
            .map(char::from)
            .collect();
        assert_eq!(all.len(), 66);
        let key = format!("p/{all}/x");
        let path = object_store::path::Path::parse(&key).unwrap();
        assert_eq!(path.as_ref(), key);
    }

    #[test]
    fn content_types_are_checked() {
        assert_eq!(content_type(None).unwrap(), DEFAULT_CONTENT_TYPE);
        for t in [
            "image/png",
            "text/plain; charset=utf-8",
            "text/plain;charset=\"utf-8\"",
            "multipart/form-data; boundary=\"a;b\"",
            "application/vnd.api+json",
        ] {
            assert_eq!(content_type(Some(t)).as_deref(), Ok(t), "{t}");
        }
        let long = format!("a/{}", "b".repeat(MAX_CONTENT_TYPE));
        for t in [
            "",
            "text",
            "text/",
            "/plain",
            "text/plain\r\nx-amz-acl: public-read",
            "text/plain; charset",
            "text/pl ain",
            "text/plain; a=\"b",
            "text/plain\0",
            &long,
        ] {
            assert!(content_type(Some(t)).is_err(), "{t:?} was taken");
        }
    }

    #[test]
    fn namespaces_hash_the_name_and_never_meet() {
        let a = Namespace::actor("photos", "alice", "0");
        assert_eq!(a.rel(), format!("photos/{}/0", name_id("alice")));
        assert_eq!(name_id("alice").len(), 64);
        assert_eq!(
            Namespace::actor("photos", &"n".repeat(256), "0")
                .rel()
                .len(),
            6 + 1 + 64 + 2
        );
        assert_ne!(a, Namespace::actor("photos", "bob", "0"));
        assert_ne!(a, Namespace::actor("photo", "alice", "0"));
        assert_eq!(Namespace::front().rel(), "_front/main/0");
        // `..` as a name is one more hash, never a parent.
        assert!(!Namespace::actor("c", "..", "0").rel().contains(".."));
    }

    #[test]
    fn the_epoch_hashes_both_markers_in_order() {
        // `printf '\0' | sha256sum`: no marker at all.
        assert_eq!(epoch(None, None), "6e340b9cffb37a98");
        assert_eq!(epoch(Some(b""), Some(b"")), epoch(None, None));
        let (a, b) = (epoch(Some(b"x"), None), epoch(None, Some(b"x")));
        assert_ne!(a, b, "the two markers are told apart");
        assert_ne!(a, epoch(None, None));
        assert_ne!(epoch(Some(b"x"), Some(b"y")), epoch(Some(b"y"), Some(b"x")));
        assert_eq!(a.len(), 16);
        assert!(
            a.bytes()
                .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
        );
    }
}
