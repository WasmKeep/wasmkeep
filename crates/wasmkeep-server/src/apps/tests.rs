#![allow(clippy::unwrap_used)] // test code

use super::*;

fn two_classes() -> Manifest {
    Manifest {
        assets: None,
        previous: Some("abc".into()),
        classes: BTreeMap::from([
            ("chat".into(), "11".into()),
            ("counter".into(), "22".into()),
        ]),
        front: None,
        access: BTreeMap::new(),
        config: BTreeMap::new(),
        requires: BTreeMap::new(),
        expire: BTreeMap::new(),
        batched: std::collections::BTreeSet::new(),
        limits: Default::default(),
    }
}

/// An added class says its rule, and never a change from a rule it
/// never had (`access x: open -> internal` for a class just added).
#[test]
fn an_added_class_says_its_rule() {
    let before = two_classes();
    let mut after = two_classes();
    for class in ["pub", "priv", "some"] {
        after.classes.insert(class.into(), "33".into());
    }
    after.access.insert("pub".into(), "open".into());
    let mut rules = AccessRules::default();
    rules.replace("priv", "internal").unwrap();
    rules.replace("some", "existing:lobby").unwrap();
    rules.replace("chat", "existing").unwrap();
    let settings = Settings {
        access: rules,
        config: BTreeMap::new(),
    };
    let diff = Diff::between(Some((&before, &Settings::default())), (&after, &settings));
    let lines = diff.lines();
    for want in [
        "class priv: added (internal)",
        "class pub: added (open)",
        "class some: added (existing:lobby)",
        "access chat: open -> existing",
    ] {
        assert!(lines.contains(&want.to_owned()), "{want}: {lines:?}");
    }
    assert!(
        !lines.iter().any(|l| l.starts_with("access priv")
            || l.starts_with("access some")
            || l.starts_with("access pub")),
        "{lines:?}"
    );
    assert_eq!(diff.added_rule("priv"), "internal");
    assert_eq!(diff.added_rule("pub"), "open");
    // A first deploy: every class is added, with its rule.
    let diff = Diff::between(None, (&after, &settings));
    assert!(
        diff.lines()
            .contains(&"class chat: added (existing)".to_owned()),
        "{:?}",
        diff.lines()
    );
    assert!(diff.rules.is_empty(), "{:?}", diff.rules);
}

#[test]
fn required_names_are_part_of_the_version_and_strict() {
    let mut m = two_classes();
    m.requires.insert(
        "chat".into(),
        std::collections::BTreeSet::from(["TOKEN".into(), "KEY".into()]),
    );
    let text = m.format();
    assert!(text.starts_with("wasmkeep-manifest 2\n"), "{text}");
    assert!(
        text.contains("requires chat KEY\nrequires chat TOKEN\n"),
        "{text}"
    );
    assert_eq!(Manifest::parse(&text), Some(m.clone()));
    assert_ne!(m.version_id(), two_classes().version_id());
    for bad in ["requires chat 1BAD", "requires nope TOKEN", "requires chat"] {
        let text = format!("wasmkeep-manifest 2\nclass chat 11\n{bad}\n");
        assert_eq!(Manifest::parse(&text), None, "{bad}");
    }
    // Format 1 never carries them.
    assert_eq!(
        Manifest::parse("wasmkeep-manifest 1\nclass chat 11\nrequires chat TOKEN\n"),
        None
    );
}

#[test]
fn manifests_round_trip() {
    let m = two_classes();
    assert_eq!(Manifest::parse(&m.format()), Some(m));
    assert_eq!(Manifest::parse("something else"), None);
}

#[test]
fn a_manifest_without_rules_or_config_is_written_as_before() {
    let m = two_classes();
    assert_eq!(
        m.format(),
        "wasmkeep-manifest 1\nprevious abc\nclass chat 11\nclass counter 22\n"
    );
    // A first deploy without rules gets the id it had before ADR 0013
    // (computed independently: `printf ... | sha256sum`); later ones
    // add the version they replaced.
    let first = Manifest {
        assets: None,
        previous: None,
        ..m.clone()
    };
    assert_eq!(first.version_id(), "a6d0b3dd3883b630");
    assert_eq!(
        m.version_id(),
        hex(&Sha256::digest(
            b"class chat 11\nclass counter 22\nprevious abc\n"
        ))[..16]
    );
}

#[test]
fn the_version_it_replaced_is_part_of_the_id() {
    let after_a = Manifest {
        assets: None,
        previous: Some("aaaa".into()),
        ..two_classes()
    };
    let after_b = Manifest {
        assets: None,
        previous: Some("bbbb".into()),
        ..two_classes()
    };
    assert_eq!(after_a.identity(), after_b.identity());
    assert_ne!(after_a.version_id(), after_b.version_id());
}

#[test]
fn current_reads_the_marker_and_old_readers_would_refuse_it() {
    let plain = Current::parse(b"abc 7\n").unwrap();
    assert!(!plain.marked);
    assert_eq!(plain.format(), "abc 7\n");
    let marked = Current::parse(b"abc 8 2\n").unwrap();
    assert!(marked.marked);
    assert_eq!(marked.format(), "abc 8 2\n");
    assert!(!marked.rooted && !plain.rooted);
    // Token 3 (ADR 0022): marked, and the app's state under its root.
    let rooted = Current::parse(b"abc 9 3\n").unwrap();
    assert!(rooted.marked && rooted.rooted);
    assert_eq!(rooted.format(), "abc 9 3\n");
    assert_eq!(Current::parse(b"abc 8 4"), None);
    assert_eq!(Current::parse(b"abc 8 2 x"), None);
    assert_eq!(Current::parse(b"abc 8 3 x"), None);
    // What `current` was read with before ADR 0013: exactly two fields.
    let old = |t: &str| t.split_whitespace().count() == 2;
    assert!(old(&plain.format()) && !old(&marked.format()));
    // And before 0.4.0: two fields, or a third that is `2` only.
    let before_0_4 = |t: &str| {
        let parts: Vec<&str> = t.split_whitespace().collect();
        parts.len() == 2 || (parts.len() == 3 && parts[2] == "2")
    };
    assert!(before_0_4(&marked.format()) && !before_0_4(&rooted.format()));
}

#[test]
fn config_objects_are_canonical_and_read_back_unchanged() {
    let vars = crate::env::parse(
        "# a comment\n# TOKEN=commented-out\nZ = 'last'\nA=it's\nQ=\"quoted\"\nE=\nS=' spaced '\n",
    )
    .unwrap();
    let text = format_config(&vars);
    assert_eq!(text, "A='it's'\nE=''\nQ='quoted'\nS=' spaced '\nZ='last'\n");
    assert!(!text.contains("TOKEN"));
    assert_eq!(parse_config(text.as_bytes()).unwrap(), vars);
    let odd = Vars::from([
        ("D".to_owned(), "\"both\"".to_owned()),
        ("T".to_owned(), "a\tb".to_owned()),
        ("O".to_owned(), "'".to_owned()),
    ]);
    assert_eq!(parse_config(format_config(&odd).as_bytes()).unwrap(), odd);
}

#[test]
fn version_debug_shows_no_value() {
    let v = Version {
        assets: None,
        classes: vec![("c".into(), vec![0; 3])],
        access: AccessRules::default(),
        config: BTreeMap::from([(
            "c".to_owned(),
            Vars::from([("URL".to_owned(), "https://hidden.test".to_owned())]),
        )]),
        requires: BTreeMap::new(),
        expire: BTreeMap::new(),
        batched: std::collections::BTreeSet::new(),
        limits: Default::default(),
        front: None,
    };
    let shown = format!("{v:?}");
    assert!(
        shown.contains("URL") && !shown.contains("hidden"),
        "{shown}"
    );
}

#[test]
fn assets_format_7_round_trip_and_legacy_identity() {
    let old = two_classes();
    let text = format!(
        "wasmkeep-manifest 7\nclass chat 11\nclass counter 22\nassets {}\n",
        "a".repeat(64)
    );
    let m = Manifest::parse(&text).expect("format 7 with assets must parse");
    assert_eq!(m.format(), text);
    assert_ne!(m.version_id(), old.version_id());
    assert!(Manifest::parse(&text.replace("manifest 7", "manifest 6")).is_none());
    assert!(Manifest::parse(&(text.clone() + &format!("assets {}\n", "a".repeat(64)))).is_none());
    assert!(Manifest::parse("wasmkeep-manifest 7\nclass chat 11\n").is_none());
    assert_eq!(Manifest::parse(&old.format()), Some(old));
}

#[test]
fn rules_and_config_make_format_2_and_a_new_version() {
    let mut m = two_classes();
    m.access
        .insert("chat".into(), "existing:main,room 1".into());
    m.config.insert("counter".into(), "33".into());
    let text = m.format();
    assert!(text.starts_with("wasmkeep-manifest 2\n"), "{text}");
    assert!(
        text.contains("\naccess chat existing:main,room 1\n"),
        "{text}"
    );
    assert!(text.contains("\nconfig counter 33\n"), "{text}");
    assert_eq!(Manifest::parse(&text), Some(m.clone()));
    assert_ne!(m.version_id(), two_classes().version_id());
    let mut config_only = two_classes();
    config_only.config = m.config.clone();
    assert_ne!(config_only.version_id(), two_classes().version_id());
    assert_ne!(config_only.version_id(), m.version_id());
}

#[test]
fn manifests_with_unknown_or_inconsistent_lines_are_refused() {
    let base = "wasmkeep-manifest 2\nclass chat 11\n";
    for bad in [
        // A format after this version's (7, ADR 0030).
        "wasmkeep-manifest 8\nclass chat 11\n".to_owned(),
        format!("{base}secret chat 11\n"),
        // Idle limits (ADR 0028): format 4 only, a known class, once,
        // between an hour and the longest.
        format!("{base}expire chat 3600\n"),
        "wasmkeep-manifest 3\nclass chat 11\nexpire chat 3600\n".to_owned(),
        "wasmkeep-manifest 4\nclass chat 11\nexpire other 3600\n".to_owned(),
        "wasmkeep-manifest 4\nclass chat 11\nexpire chat 3599\n".to_owned(),
        "wasmkeep-manifest 4\nclass chat 11\nexpire chat 7d\n".to_owned(),
        "wasmkeep-manifest 4\nclass chat 11\nexpire chat 3600\nexpire chat 7200\n".to_owned(),
        format!("{base}access other existing\n"),
        format!("{base}config other 33\n"),
        format!("{base}access chat closed\n"),
        format!("{base}access chat open\n"),
        format!("{base}access chat existing:b,a\n"),
        format!("{base}access chat internal:main\n"),
        format!("{base}config chat 33 extra\n"),
        // Format 1 never has rules or configuration.
        "wasmkeep-manifest 1\nclass chat 11\naccess chat existing\n".to_owned(),
        "wasmkeep-manifest 1\nclass chat 11\nconfig chat 33\n".to_owned(),
    ] {
        assert_eq!(Manifest::parse(&bad), None, "{bad}");
    }
}

#[test]
fn a_batched_class_makes_format_5_and_reads_back() {
    // ADR 0034, section 15: without a batched class, the version keeps
    // its format, bytes and id (strict is written as absence).
    let mut m = two_classes();
    let before = (m.format(), m.version_id());
    m.batched.insert("chat".into());
    assert_eq!(m.format_number(), 5);
    let text = m.format();
    assert!(text.starts_with("wasmkeep-manifest 5\n"), "{text}");
    assert!(text.ends_with("writes chat batched\n"), "{text}");
    assert_ne!(m.version_id(), before.1);
    assert_eq!(Manifest::parse(&text), Some(m.clone()));
    // With everything formats 2 to 4 carry: all of it reads back.
    m.front = Some("ff".into());
    m.access.insert("counter".into(), "open".into());
    m.config.insert("chat".into(), "cc".into());
    m.requires.insert(
        "chat".into(),
        std::collections::BTreeSet::from(["KEY".into()]),
    );
    m.expire.insert("chat".into(), 7 * 86_400);
    m.batched.insert("counter".into());
    let text = m.format();
    assert!(text.starts_with("wasmkeep-manifest 5\n"), "{text}");
    assert_eq!(Manifest::parse(&text), Some(m.clone()));
    let mut m = two_classes();
    m.batched.insert("chat".into());
    m.batched.clear();
    assert_eq!((m.format(), m.version_id()), before);
}

#[test]
fn writes_lines_are_refused_where_they_do_not_belong() {
    let ok = "wasmkeep-manifest 5\nclass chat 11\nwrites chat batched\n";
    assert!(Manifest::parse(ok).is_some());
    for bad in [
        // Strict is never written: absence says it.
        "wasmkeep-manifest 5\nclass chat 11\nwrites chat strict\n",
        "wasmkeep-manifest 5\nclass chat 11\nwrites chat gated\n",
        "wasmkeep-manifest 5\nclass chat 11\nwrites chat\n",
        "wasmkeep-manifest 5\nclass chat 11\nwrites chat batched extra\n",
        // A known class, once.
        "wasmkeep-manifest 5\nclass chat 11\nwrites nope batched\n",
        "wasmkeep-manifest 5\nclass chat 11\nwrites chat batched\nwrites chat batched\n",
        "wasmkeep-manifest 5\nclass chat 11\nwrites _front batched\n",
        // Not before format 5: a reader of format 4 stays as it was.
        "wasmkeep-manifest 4\nclass chat 11\nwrites chat batched\n",
        "wasmkeep-manifest 1\nclass chat 11\nwrites chat batched\n",
        // A format this version does not know.
        "wasmkeep-manifest 8\nclass chat 11\n",
    ] {
        assert_eq!(Manifest::parse(bad), None, "{bad}");
    }
    // Format 5 keeps the rules of the formats below it.
    for bad in [
        "wasmkeep-manifest 5\nclass chat 11\nexpire chat 1\nwrites chat batched\n",
        "wasmkeep-manifest 5\nclass chat 11\naccess nope open\nwrites chat batched\n",
    ] {
        assert_eq!(Manifest::parse(bad), None, "{bad}");
    }
}

/// A live heartbeat of a node serving `default/shop` with these words,
/// at this build's version (a writer refuses beside a live node more than
/// one minor version away, ADR 0043).
pub(super) async fn heartbeat(store: &Arc<dyn ObjectStore>, id: &str, words: &str) {
    let body = format!(
        "wasmkeep-node 2\n{id}\nhttp://10.0.0.9:8080\n{}\n{}\ndefault/shop\n\n\nserving\nsecrets\n{words}\n",
        crate::VERSION,
        crate::alarm::now_ms_i64()
    );
    store
        .put(
            &format!("nodes/{id}").as_str().into(),
            body.into_bytes().into(),
        )
        .await
        .unwrap();
}

/// Format 5 needs `flush` and inherits the reader checks of formats 4
/// (`ends`) and 3 (`front`), even without an idle limit or a front.
#[tokio::test]
async fn format_5_needs_flush_and_every_word_below_it() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let app = AppRef::new(Arc::clone(&store), "", "default", "shop");
    let mut m = two_classes();
    m.batched.insert("chat".into());
    app.check_readers(&m).await.unwrap();
    heartbeat(&store, "n", "take front handoff ends flush").await;
    app.check_readers(&m).await.unwrap();
    heartbeat(&store, "n", "take front handoff ends").await;
    let e = app.check_readers(&m).await.unwrap_err();
    assert!(
        matches!(&e, DeployError::NodesWithoutFlush { nodes, .. } if nodes == "n"),
        "{e}"
    );
    assert!(e.to_string().contains("manifest format 5"), "{e}");
    heartbeat(&store, "n", "take front handoff flush").await;
    let e = app.check_readers(&m).await.unwrap_err();
    assert!(matches!(e, DeployError::NodesWithoutEnds { .. }), "{e}");
    heartbeat(&store, "n", "take handoff ends flush").await;
    let e = app.check_readers(&m).await.unwrap_err();
    assert!(matches!(e, DeployError::NodesWithoutFront { .. }), "{e}");
    // Strict classes only: format 1, read by every node as before.
    heartbeat(&store, "n", "take").await;
    app.check_readers(&two_classes()).await.unwrap();
}

#[test]
fn rate_limits_manifest_identity_and_legacy_bytes() {
    let mut m = two_classes();
    let old = (m.format(), m.version_id());
    m.limits.insert(
        "login".into(),
        limits::Policy::parse("POST /login", "ip", None, "5/s", 10).unwrap(),
    );
    assert_eq!(m.format_number(), 6);
    assert_eq!(Manifest::parse(&m.format()), Some(m.clone()));
    assert_ne!(m.version_id(), old.1);
    for bad in [
        m.format().replace("manifest 6", "manifest 5"),
        format!("{}limit login POST /login ip 5/s 10\n", m.format()),
        m.format().replace("5/s", "0/s"),
    ] {
        assert!(Manifest::parse(&bad).is_none(), "{bad}");
    }
    m.limits.clear();
    assert_eq!((m.format(), m.version_id()), old);
}

#[tokio::test]
async fn rate_limits_require_capable_readers() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let app = AppRef::new(Arc::clone(&store), "", "default", "shop");
    let mut m = two_classes();
    m.limits.insert(
        "login".into(),
        limits::Policy::parse("/login", "ip", None, "1/s", 1).unwrap(),
    );
    heartbeat(&store, "old", "take front ends flush").await;
    assert!(
        app.check_readers(&m)
            .await
            .unwrap_err()
            .to_string()
            .contains("rate_limits")
    );
    heartbeat(&store, "old", "take front ends flush rate_limits").await;
    app.check_readers(&m).await.unwrap();
}

#[test]
fn modes_of_writes_parse_and_print() {
    for w in [Writes::Strict, Writes::Batched] {
        assert_eq!(Writes::parse(w.as_str()), Ok(w));
    }
    assert_eq!(Writes::default(), Writes::Strict);
    assert!(
        Writes::parse("gated")
            .unwrap_err()
            .contains("strict, batched")
    );
}

#[test]
fn an_idle_limit_makes_format_4_and_reads_back() {
    // ADR 0028: without a limit, the version keeps its bytes and id.
    let mut m = two_classes();
    let before = (m.format(), m.version_id());
    m.expire.insert("chat".into(), 7 * 86_400);
    assert_eq!(m.format_number(), 4);
    let text = m.format();
    assert!(text.starts_with("wasmkeep-manifest 4\n"), "{text}");
    assert!(text.ends_with("expire chat 604800\n"), "{text}");
    assert_ne!(m.version_id(), before.1);
    assert_eq!(Manifest::parse(&text), Some(m.clone()));
    m.expire.clear();
    assert_eq!((m.format(), m.version_id()), before);
}

#[test]
fn a_version_with_rules_but_no_front_is_written_as_before() {
    // Format 2, byte for byte and id for id, as before ADR 0023
    // (the id computed independently: `printf ... | sha256sum`).
    let mut m = two_classes();
    m.access
        .insert("chat".into(), "existing:main,room 1".into());
    m.config.insert("counter".into(), "33".into());
    m.requires.insert(
        "chat".into(),
        std::collections::BTreeSet::from(["TOKEN".into()]),
    );
    assert_eq!(
        m.format(),
        "wasmkeep-manifest 2\nprevious abc\nclass chat 11\nclass counter 22\n\
         access chat existing:main,room 1\nconfig counter 33\nrequires chat TOKEN\n"
    );
    assert_eq!(m.version_id(), "f6d159f826d96085");
}

fn with_front() -> Manifest {
    let mut m = two_classes();
    m.front = Some("44".into());
    m.access.insert("chat".into(), "existing".into());
    m.config.insert(FRONT.into(), "55".into());
    m.config.insert("counter".into(), "33".into());
    m.requires.insert(
        FRONT.into(),
        std::collections::BTreeSet::from(["SESSION_KEY".into()]),
    );
    m
}

#[test]
fn a_front_makes_format_3_and_a_new_version() {
    let m = with_front();
    let text = m.format();
    assert_eq!(
        text,
        "wasmkeep-manifest 3\nprevious abc\nclass chat 11\nclass counter 22\n\
         front main 44\naccess chat existing\nconfig _front 55\nconfig counter 33\n\
         requires _front SESSION_KEY\n"
    );
    assert_eq!(Manifest::parse(&text), Some(m.clone()));
    // The front line is part of the identity: a deploy changing only
    // the front gets a new id.
    let other = Manifest {
        front: Some("45".into()),
        ..m.clone()
    };
    assert_ne!(other.version_id(), m.version_id());
    let mut none = m.clone();
    none.front = None;
    none.config.remove(FRONT);
    none.requires.clear();
    assert_eq!(none.format_number(), 2);
    assert_ne!(none.version_id(), m.version_id());
    // A front alone, no class.
    let alone = Manifest {
        assets: None,
        previous: None,
        classes: BTreeMap::new(),
        front: Some("44".into()),
        access: BTreeMap::new(),
        config: BTreeMap::new(),
        requires: BTreeMap::new(),
        expire: BTreeMap::new(),
        batched: std::collections::BTreeSet::new(),
        limits: Default::default(),
    };
    assert_eq!(alone.format(), "wasmkeep-manifest 3\nfront main 44\n");
    assert_eq!(Manifest::parse(&alone.format()), Some(alone));
}

#[test]
fn format_3_says_open_and_earlier_formats_do_not() {
    // The project file writes every class's rule (ADR 0021).
    let text = "wasmkeep-manifest 3\nclass chat 11\naccess chat open\n";
    let m = Manifest::parse(text).unwrap();
    assert_eq!(m.access.get("chat").map(String::as_str), Some("open"));
    assert_eq!(m.format(), text, "read back as written");
    for format in [1, 2] {
        let old = format!("wasmkeep-manifest {format}\nclass chat 11\naccess chat open\n");
        assert_eq!(Manifest::parse(&old), None, "{old}");
    }
}

#[test]
fn front_lines_are_refused_where_they_do_not_belong() {
    let v3 = "wasmkeep-manifest 3\nclass chat 11\n";
    let front = format!("{v3}front main 44\n");
    for bad in [
        // Not before format 3.
        "wasmkeep-manifest 2\nclass chat 11\nfront main 44\n".to_owned(),
        "wasmkeep-manifest 1\nclass chat 11\nfront main 44\n".to_owned(),
        // One front, named main.
        format!("{front}front main 45\n"),
        format!("{v3}front other 44\n"),
        format!("{v3}front main\n"),
        format!("{v3}front main 44 extra\n"),
        // The front's scope only beside a front line.
        format!("{v3}config _front 55\n"),
        format!("{v3}requires _front KEY\n"),
        "wasmkeep-manifest 2\nclass chat 11\nconfig _front 55\n".to_owned(),
        // No rule for the front, ever.
        format!("{front}access _front existing\n"),
        format!("{front}access _front open\n"),
        // `_front` is never a class.
        format!("{v3}class _front 66\n"),
        "wasmkeep-manifest 1\nclass _front 66\n".to_owned(),
        "wasmkeep-manifest 2\nclass _front 66\nconfig _front 55\n".to_owned(),
        // Open for a class the version does not have.
        format!("{v3}access other open\n"),
    ] {
        assert_eq!(Manifest::parse(&bad), None, "{bad}");
    }
    assert!(Manifest::parse(&format!("{front}config _front 55\nrequires _front KEY\n")).is_some());
}

#[test]
fn config_errors_never_show_a_value() {
    let e = parse_config(b"OK=1\nnot a line secret-value\n").unwrap_err();
    assert!(e.starts_with("2:"), "{e}");
    assert!(!e.contains("secret"), "{e}");
    let big = vec![b'#'; usize::try_from(crate::env::MAX_FILE).unwrap() + 1];
    assert!(parse_config(&big).is_err());
    assert!(parse_config(&[0xff, 0xfe]).is_err());
}

/// A local marker this node cannot read (here a directory where the
/// file should be; in production EMFILE, EIO, EACCES) is not a
/// mismatch: `still_ours` fails, and the follow loop retries, rather
/// than stopping the app and removing its local files. Only a missing
/// marker, or one with other bytes, says the app is no longer ours.
#[tokio::test]
async fn an_unreadable_local_marker_is_an_error_not_a_mismatch() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let app = AppRef::new(Arc::clone(&store), "", "default", "shop");
    let local = tempfile::tempdir().unwrap();
    let marker = local.path().join(RECREATED_LOCAL);
    // No remote marker: the legacy app's files are kept.
    assert!(app.still_ours(local.path()).await.unwrap());
    app.put_once(RECREATED, b"one".to_vec()).await.unwrap();
    // Missing locally: recreated since this node prepared its files.
    assert!(!app.still_ours(local.path()).await.unwrap());
    std::fs::write(&marker, b"two").unwrap();
    assert!(!app.still_ours(local.path()).await.unwrap());
    std::fs::write(&marker, b"one").unwrap();
    assert!(app.still_ours(local.path()).await.unwrap());
    std::fs::remove_file(&marker).unwrap();
    std::fs::create_dir(&marker).unwrap();
    let read = app.still_ours(local.path()).await;
    assert!(
        matches!(&read, Err(DeployError::Local(why)) if why.contains(RECREATED_LOCAL)),
        "{read:?}"
    );
}

/// At start, too, a local marker this node cannot read is an error,
/// never a reason to remove the app's local files: only a missing
/// marker, or one with other bytes, is.
#[tokio::test]
async fn an_unreadable_local_marker_never_removes_local_files_at_start() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let app = AppRef::new(Arc::clone(&store), "", "default", "shop");
    let data = tempfile::tempdir().unwrap();
    let local = data.path().join("app");
    app.put_once(RECREATED, b"one".to_vec()).await.unwrap();
    // Missing locally: the directory is replaced, the marker written.
    std::fs::create_dir_all(&local).unwrap();
    std::fs::write(local.join("state"), b"old").unwrap();
    app.prepare_local(&local).await.unwrap();
    assert!(!local.join("state").exists());
    assert_eq!(std::fs::read(local.join(RECREATED_LOCAL)).unwrap(), b"one");
    // Same bytes: kept.
    std::fs::write(local.join("state"), b"kept").unwrap();
    app.prepare_local(&local).await.unwrap();
    assert_eq!(std::fs::read(local.join("state")).unwrap(), b"kept");
    // Unreadable: an error, and nothing is removed.
    std::fs::remove_file(local.join(RECREATED_LOCAL)).unwrap();
    std::fs::create_dir(local.join(RECREATED_LOCAL)).unwrap();
    let prepared = app.prepare_local(&local).await;
    assert!(
        matches!(&prepared, Err(DeployError::Local(why)) if why.contains(RECREATED_LOCAL)),
        "{prepared:?}"
    );
    assert_eq!(std::fs::read(local.join("state")).unwrap(), b"kept");
    assert!(local.join(RECREATED_LOCAL).is_dir());
}

/// ADR 0043, section 2: a tombstone is one by its presence, and its
/// readers take its `id` line alone, so a later version may add lines (or
/// number its header) without an older node misreading it.
#[test]
fn a_tombstone_is_read_by_its_id_line_alone() {
    use super::delete::{TOMBSTONE_HEADER, tombstone_id};
    assert_eq!(TOMBSTONE_HEADER, "wasmkeep-app-deleted 1");
    let body = format!("{TOMBSTONE_HEADER}\nby ops\nat 5\nid 0a1b\n");
    assert_eq!(tombstone_id(&body), Some("0a1b"));
    for later in [
        "wasmkeep-app-deleted 2\nby ops\nat 5\nid 0a1b\n",
        "wasmkeep-app-deleted 1\nby ops\nat 5\nreason moved\nid 0a1b\nnext line\n",
    ] {
        assert_eq!(tombstone_id(later), Some("0a1b"), "{later:?}");
    }
    assert_eq!(tombstone_id("wasmkeep-app-deleted 1\nby ops\nat 5\n"), None);
}
