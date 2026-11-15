#![allow(clippy::unwrap_used)] // test code

use super::*;

fn place() -> Place<'static> {
    Place::new("prod", "acme", "shop")
}

fn set() -> SecretSet {
    let mut s = SecretSet::default();
    s.set("api", "STRIPE_KEY", "sk_live_123").unwrap();
    s.set(
        "api",
        "TLS_KEY",
        "-----BEGIN KEY-----\nAAAA\n-----END KEY-----\n",
    )
    .unwrap();
    s.set("web", "EMPTY", "").unwrap();
    s
}

/// The sealed bytes of an object (its fourth line, unhexed).
fn sealed_bytes(object: &[u8]) -> Vec<u8> {
    let text = std::str::from_utf8(object).unwrap();
    unhex(text.lines().nth(3).unwrap()).unwrap()
}

#[test]
fn a_sealed_set_opens_at_its_place() {
    let key = SecretsKey::generate().unwrap();
    let object = seal(&key, &place(), 7, &set()).unwrap();
    assert_eq!(
        open(std::slice::from_ref(&key), &place(), 7, &object).unwrap(),
        set()
    );
    assert_eq!(sealed_with(&object).unwrap(), key.id());
    // Two seals of the same set differ (random nonce).
    assert_ne!(object, seal(&key, &place(), 7, &set()).unwrap());
}

#[test]
fn neither_names_nor_values_appear_in_the_sealed_bytes() {
    let key = SecretsKey::generate().unwrap();
    let object = seal(&key, &place(), 1, &set()).unwrap();
    let sealed = sealed_bytes(&object);
    let plain = set().encode();
    assert_ne!(&sealed[..plain.len()], plain.as_slice(), "not encrypted");
    for needle in [
        "STRIPE_KEY",
        "sk_live_123",
        "TLS_KEY",
        "BEGIN",
        "api ",
        "web ",
    ] {
        assert!(
            !sealed.windows(needle.len()).any(|w| w == needle.as_bytes()),
            "{needle} in the sealed bytes"
        );
    }
}

/// Pins the format: objects already in buckets must keep opening.
/// Fixed key, place, seq and nonce give exactly these bytes.
#[test]
fn a_known_object_still_opens_and_seals_the_same() {
    let key = SecretsKey::from_text(&"11".repeat(32), "k").unwrap();
    let mut s = SecretSet::default();
    s.set("api", "TOKEN", "t0k3n\n").unwrap();
    let object = seal_with_nonce(&key, &place(), 3, &s, [7; NONCE_LEN]).unwrap();
    let expected = include_str!("../secrets_known_object.txt");
    assert_eq!(String::from_utf8(object).unwrap(), expected);
    assert_eq!(open(&[key], &place(), 3, expected.as_bytes()).unwrap(), s);
}

#[test]
fn each_place_has_its_own_key() {
    // Same key, nonce, seq and set: another place gives other bytes,
    // even though the associated data alone would already differ.
    let key = SecretsKey::generate().unwrap();
    let a = seal_with_nonce(&key, &place(), 1, &set(), [1; NONCE_LEN]).unwrap();
    let b = seal_with_nonce(
        &key,
        &Place::new("prod", "acme", "shop2"),
        1,
        &set(),
        [1; NONCE_LEN],
    )
    .unwrap();
    let (a, b) = (sealed_bytes(&a), sealed_bytes(&b));
    let tag = CHACHA20_POLY1305.tag_len();
    assert_ne!(a[..a.len() - tag], b[..b.len() - tag], "same keystream");
}

#[test]
fn an_object_opens_nowhere_else() {
    let key = SecretsKey::generate().unwrap();
    let keys = std::slice::from_ref(&key);
    let object = seal(&key, &place(), 7, &set()).unwrap();
    let elsewhere = [
        (Place::new("staging", "acme", "shop"), 7),
        (Place::new("prod", "acme2", "shop"), 7),
        (Place::new("prod", "acme", "shop2"), 7),
        (place(), 8),
        (place(), 6),
    ];
    for (place, seq) in elsewhere {
        assert_eq!(
            open(keys, &place, seq, &object),
            Err(SecretsError::Tampered),
            "{place:?} {seq}"
        );
    }
    // Fields are length-prefixed: moving a boundary is another place.
    let object = seal(&key, &Place::new("", "ab", "c"), 1, &set()).unwrap();
    assert_eq!(
        open(keys, &Place::new("", "a", "bc"), 1, &object),
        Err(SecretsError::Tampered)
    );
}

#[test]
fn slashes_around_the_prefix_do_not_make_another_place() {
    let key = SecretsKey::generate().unwrap();
    let object = seal(&key, &Place::new("prod/", "acme", "shop"), 1, &set()).unwrap();
    for prefix in ["prod", "/prod", "prod/"] {
        assert!(
            open(
                std::slice::from_ref(&key),
                &Place::new(prefix, "acme", "shop"),
                1,
                &object
            )
            .is_ok()
        );
    }
}

#[test]
fn a_changed_or_cut_object_does_not_open() {
    let key = SecretsKey::generate().unwrap();
    let keys = std::slice::from_ref(&key);
    let object = String::from_utf8(seal(&key, &place(), 1, &set()).unwrap()).unwrap();
    let lines: Vec<&str> = object.lines().collect();
    let rebuild = |sealed: &[u8]| {
        format!(
            "{}\n{}\n{}\n{}\n",
            lines[0],
            lines[1],
            lines[2],
            hex(sealed)
        )
    };
    let sealed = unhex(lines[3]).unwrap();
    // One bit of the ciphertext body (not the tag).
    let mut body = sealed.clone();
    body[0] ^= 1;
    assert_eq!(
        open(keys, &place(), 1, rebuild(&body).as_bytes()),
        Err(SecretsError::Tampered)
    );
    // Cut short by one byte.
    assert_eq!(
        open(
            keys,
            &place(),
            1,
            rebuild(&sealed[..sealed.len() - 1]).as_bytes()
        ),
        Err(SecretsError::Tampered)
    );
    // Shorter than a tag.
    assert_eq!(
        open(keys, &place(), 1, rebuild(&sealed[..4]).as_bytes()),
        Err(SecretsError::Malformed)
    );
    assert_eq!(
        open(keys, &place(), 1, b"not an object\n"),
        Err(SecretsError::Malformed)
    );
    let crlf = object.replace('\n', "\r\n");
    assert_eq!(
        open(keys, &place(), 1, crlf.as_bytes()),
        Err(SecretsError::Malformed)
    );
    assert_eq!(
        open(keys, &place(), 1, &vec![b'a'; MAX_OBJECT + 1]),
        Err(SecretsError::TooLarge)
    );
}

#[test]
fn the_wrong_key_says_which_key_it_needs_and_two_keys_open_either() {
    let (old, new) = (
        SecretsKey::generate().unwrap(),
        SecretsKey::generate().unwrap(),
    );
    let object = seal(&old, &place(), 1, &set()).unwrap();
    assert_eq!(
        open(std::slice::from_ref(&new), &place(), 1, &object),
        Err(SecretsError::UnknownKey(old.id().to_owned()))
    );
    let both = [new.clone(), old.clone()];
    assert_eq!(open(&both, &place(), 1, &object).unwrap(), set());
    let forged = String::from_utf8(object)
        .unwrap()
        .replacen(old.id(), new.id(), 1);
    assert_eq!(
        open(std::slice::from_ref(&new), &place(), 1, forged.as_bytes()),
        Err(SecretsError::Tampered)
    );
}

#[test]
fn the_plaintext_holds_any_value() {
    let mut s = SecretSet::default();
    s.set("api", "MULTI", "line one\nline two\n\n").unwrap();
    s.set("api", "SPACES", "  a b  ").unwrap();
    s.set("api", "UNICODE", "é = ü").unwrap();
    s.set("api", "HEADERLIKE", "api X 1\ny").unwrap();
    s.set("api", "CONTROL", "a\r\nb\0c\td").unwrap();
    let every: String = (1..=0x7f_u8).map(char::from).collect();
    s.set("web", "EVERY", &every).unwrap();
    assert_eq!(SecretSet::decode(&s.encode()).unwrap(), s);
}

#[test]
fn the_front_has_its_own_scope_and_nothing_else_is_one() {
    // ADR 0023: `_front <name> <length>`, beside the classes.
    let mut s = set();
    assert!(!s.has_front());
    s.set("_front", "SESSION_KEY", "k").unwrap();
    assert!(s.has_front());
    let back = SecretSet::decode(&s.encode()).unwrap();
    assert_eq!(back, s);
    assert_eq!(back.class("_front").unwrap()["SESSION_KEY"], "k");
    assert!(
        back.names()
            .iter()
            .any(|(c, n)| c == "_front" && n == &["SESSION_KEY"])
    );
    assert!(
        SecretSet::decode(b"_front TOKEN 1\nx\n")
            .unwrap()
            .has_front()
    );
    // The last `_front` entry removed: no scope left.
    assert!(s.unset("_front", "SESSION_KEY"));
    assert!(!s.has_front());
    // Only that name: no other underscore scope, and not a class.
    for bad in ["_frontx", "_other", "_", "_Front", "front_"] {
        let mut t = SecretSet::default();
        assert!(t.set(bad, "NAME", "v").is_err(), "{bad}");
    }
    assert!(SecretSet::decode(b"_admin TOKEN 1\nx\n").is_err());
    // Sealing does not change: the scope seals and opens as a class.
    let key = SecretsKey::generate().unwrap();
    let mut f = SecretSet::default();
    f.set("_front", "SESSION_KEY", "k").unwrap();
    let object = seal(&key, &place(), 3, &f).unwrap();
    assert_eq!(open(&[key], &place(), 3, &object).unwrap(), f);
}

#[test]
fn junk_plaintext_is_refused_without_quoting_it() {
    for junk in [
        &b"api NAME 3\nab\n"[..],
        b"api NAME 1\nab\n",
        b"api NAME\nx\n",
        b"Api NAME 1\nx\n",
        b"api 1NAME 1\nx\n",
        b"api NAME 1\nx\napi NAME 1\ny\n",
        b"api NAME 1\nx",
        b"api NAME +1\nx\n",
        b"api NAME 01\nx\n",
        b"api NAME 1\n\xff\n",
    ] {
        assert!(
            SecretSet::decode(junk).is_err(),
            "{:?}",
            String::from_utf8_lossy(junk)
        );
    }
    // A value cut short by a wrong length: its rest reads as a header,
    // and the error must not show it.
    let err = SecretSet::decode(b"api PW 3\nabc\nHunter2 x 1\ny\n").unwrap_err();
    assert!(!err.to_string().contains("Hunter2"), "{err}");
    assert_eq!(SecretSet::decode(b"").unwrap(), SecretSet::default());
}

#[test]
fn debug_and_errors_never_show_a_name_value_or_key() {
    let key = SecretsKey::generate().unwrap();
    let object = seal(&key, &place(), 1, &set()).unwrap();
    let errors = [
        open(&[SecretsKey::generate().unwrap()], &place(), 1, &object).unwrap_err(),
        open(std::slice::from_ref(&key), &place(), 2, &object).unwrap_err(),
    ];
    let shown = format!("{:?} {key:?} {errors:?} {}", set(), errors[0]);
    for needle in ["STRIPE_KEY", "sk_live", "BEGIN", key.to_text().trim()] {
        assert!(!shown.contains(needle), "{shown}");
    }
}

#[test]
fn key_files_are_strict_and_private() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("secrets.key");
    let key = SecretsKey::generate().unwrap();
    key.write_new(&path).unwrap();
    assert_eq!(SecretsKey::load(&path).unwrap().id(), key.id());
    assert!(
        key.write_new(&path).is_err(),
        "an existing file is never overwritten"
    );
    assert!(
        SecretsKey::load(dir.path()).is_err(),
        "a directory is not a key file"
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
        for mode in [0o640, 0o604, 0o620] {
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(mode)).unwrap();
            assert!(matches!(
                SecretsKey::load(&path),
                Err(SecretsError::KeyFileOpen(_))
            ));
        }
    }
    let text = key.to_text();
    assert!(
        SecretsKey::from_text(text.trim_end(), "k").is_ok(),
        "no newline"
    );
    for bad in [
        "",
        "zz",
        &text[..62],
        &format!("{text}x"),
        &text.to_uppercase(),
        &format!(" {text}"),
    ] {
        assert!(SecretsKey::from_text(bad, "k").is_err(), "{bad:?}");
    }
}

#[test]
fn object_names_sort_by_seq() {
    assert_eq!(object_name(10), "000000000000000a");
    assert_eq!(parse_object_name("000000000000000a"), Some(10));
    for bad in ["current", "a", "000000000000000A", "0000000000000000a"] {
        assert_eq!(parse_object_name(bad), None, "{bad}");
    }
}
