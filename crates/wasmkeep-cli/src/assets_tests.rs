use super::*;

fn put(dir: &Path, name: &str, body: &[u8]) {
    let path = dir.join(name);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, body).unwrap();
}

#[test]
fn canonical_bytes_and_walk_order() {
    let a = tempfile::tempdir().unwrap();
    let b = tempfile::tempdir().unwrap();
    for (dir, names) in [
        (a.path(), ["z.bin", "é %.bin"]),
        (b.path(), ["é %.bin", "z.bin"]),
    ] {
        for name in names {
            put(dir, name, b"abc");
        }
    }
    let rules = Index::default();
    let a = scan(a.path(), rules.clone()).unwrap();
    let b = scan(b.path(), rules).unwrap();
    let hash = digest(b"abc");
    assert_eq!(a.bundle.index.bytes(), format!("wasmkeep-assets 1\nnot-found front\nfile z.bin application/octet-stream identity:{hash}:3\nfile %C3%A9%20%25.bin application/octet-stream identity:{hash}:3\n").into_bytes());
    assert_eq!(a.bundle.index, b.bundle.index);
    assert_eq!(a.bundle.contents.len(), 1);
}

#[test]
fn scan_refusals_and_skips() {
    for name in ["actor/x", "_wasmkeep/x", "a\\b", "bad\nname"] {
        let dir = tempfile::tempdir().unwrap();
        put(dir.path(), name, b"x");
        assert!(scan(dir.path(), Index::default()).is_err(), "{name:?}");
    }
    for name in [
        "../x",
        "a/./b",
        "a/../b",
        "/x",
        "a//b",
        "a/",
        "actor",
        "_wasmkeep",
    ] {
        assert!(!valid_name(name), "{name}");
    }
    let dir = tempfile::tempdir().unwrap();
    put(dir.path(), ".hidden/key", b"secret");
    put(dir.path(), "a/.env", b"secret");
    put(dir.path(), ".well-known/.proof", b"public");
    let scan = scan(dir.path(), Index::default()).unwrap();
    assert_eq!(scan.bundle.index.files.len(), 1);
    assert!(scan.bundle.index.files.contains_key(".well-known/.proof"));
    assert_eq!(scan.skipped.len(), 2);
}

#[cfg(unix)]
#[test]
fn refuses_symlinks_non_utf8_and_non_regular_files() {
    use std::os::unix::{ffi::OsStringExt, fs::symlink, net::UnixListener};
    for directory in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let target = tempfile::tempdir().unwrap();
        put(target.path(), "secret", b"secret");
        symlink(
            if directory {
                target.path().to_owned()
            } else {
                target.path().join("secret")
            },
            root.path().join("link"),
        )
        .unwrap();
        assert!(
            scan(root.path(), Index::default())
                .unwrap_err()
                .to_string()
                .contains("link")
        );
        // DIR itself may be a link.
        assert!(scan(&root.path().join("link"), Index::default()).is_ok() == directory);
    }
    let root = tempfile::tempdir().unwrap();
    let _socket = UnixListener::bind(root.path().join("socket")).unwrap();
    assert!(scan(root.path(), Index::default()).is_err());
    let root = tempfile::tempdir().unwrap();
    std::fs::write(
        root.path().join(std::ffi::OsString::from_vec(vec![255])),
        b"x",
    )
    .unwrap();
    assert!(scan(root.path(), Index::default()).is_err());
}

#[test]
fn variants_rules_and_limits() {
    let dir = tempfile::tempdir().unwrap();
    put(dir.path(), "index.html", &vec![b'a'; 4096]);
    let built = scan(dir.path(), Index::default()).unwrap();
    let file = &built.bundle.index.files["index.html"];
    assert!(file.br.is_some() && file.gzip.is_some());
    put(dir.path(), "index.html.gz", b"not gzip");
    assert!(scan(dir.path(), Index::default()).is_err());
    let dir = tempfile::tempdir().unwrap();
    let mut rules = Index {
        not_found: "spa".into(),
        ..Index::default()
    };
    assert!(scan(dir.path(), rules.clone()).is_err());
    rules.not_found = "front".into();
    rules.front_first.insert("api".into());
    assert!(scan(dir.path(), rules).is_err());
    let f = std::fs::File::create(dir.path().join("large")).unwrap();
    f.set_len(25 * 1024 * 1024 + 1).unwrap();
    assert!(scan(dir.path(), Index::default()).is_err());
}

#[test]
fn aggregate_name_and_index_limits_and_nfc_warning() {
    assert!(!valid_name(&"a".repeat(1025)));
    let dir = tempfile::tempdir().unwrap();
    put(dir.path(), "e\u{301}.bin", b"x");
    assert_eq!(
        scan(dir.path(), Index::default()).unwrap().warnings.len(),
        1
    );
    // The same pre-upload validator enforces the limits without writing
    // half a gigabyte to disk or manufacturing CPU load.
    use wasmkeep_server::assets::{Content, File, MAX_FILE, MAX_FILES};
    let file = File {
        content_type: "application/octet-stream".into(),
        identity: Content {
            digest: digest(b"x"),
            size: MAX_FILE,
        },
        br: None,
        gzip: None,
    };
    let mut index = Index::default();
    for n in 0..21 {
        index.files.insert(format!("{n}.bin"), file.clone());
    }
    assert!(index.validate().unwrap_err().contains("512 MiB"));
    let mut file = file;
    file.identity.size = 0;
    index.files = (0..=MAX_FILES)
        .map(|n| (format!("{n}.bin"), file.clone()))
        .collect();
    assert!(index.validate().unwrap_err().contains("20,000"));
    index.files = (0..9000)
        .map(|n| (format!("{n}/{}", "a".repeat(900)), file.clone()))
        .collect();
    assert!(index.validate().unwrap_err().contains("8 MiB"));
}

#[test]
fn supplied_variants_match_exactly_and_orphans_are_files() {
    let dir = tempfile::tempdir().unwrap();
    put(dir.path(), "x.txt", b"expected");
    let mut writer = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    writer.write_all(b"different").unwrap();
    put(dir.path(), "x.txt.gz", &writer.finish().unwrap());
    assert!(
        scan(dir.path(), Index::default())
            .unwrap_err()
            .to_string()
            .contains("does not decompress")
    );
    std::fs::remove_file(dir.path().join("x.txt")).unwrap();
    assert!(
        scan(dir.path(), Index::default())
            .unwrap()
            .bundle
            .index
            .files
            .contains_key("x.txt.gz")
    );
}

#[test]
#[cfg(unix)]
fn dev_assets_refuses_only_the_bad_name_and_never_compresses() {
    let d = tempfile::tempdir().unwrap();
    put(d.path(), "index.html", &vec![b'x'; 4096]);
    put(d.path(), "index.html.gz", b"unchecked in dev");
    put(d.path(), ".secret", b"secret");
    std::os::unix::fs::symlink("/etc/passwd", d.path().join("link")).unwrap();
    let result = scan_dev(d.path(), Index::default()).unwrap();
    assert_eq!(result.bundle.index.files.len(), 1);
    let file = &result.bundle.index.files["index.html"];
    assert!(file.br.is_none() && file.gzip.is_none());
    assert!(result.warnings.iter().any(|s| s.contains("link")));
}
