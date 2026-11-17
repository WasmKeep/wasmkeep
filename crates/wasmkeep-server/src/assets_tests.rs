use super::*;

#[test]
fn index_parser_requires_canonical_bytes() {
    let hash = digest(b"abc");
    let text = format!(
        "wasmkeep-assets 1\nnot-found front\nfront-first api/\nimmutable static/\nfile a.bin application/octet-stream identity:{hash}:3\n"
    );
    let index = Index::parse(text.as_bytes()).unwrap();
    assert_eq!(index.bytes(), text.as_bytes());
    for bad in [
        text.replace("assets 1", "assets 2"),
        text.replace("api/", "api"),
        text.replace(":3", ":03"),
        text.replace("a.bin", "../a.bin"),
        text.replace("a.bin", "%61.bin"),
        text.replace("identity:", "gzip:"),
        text.replace("not-found front\n", "not-found front\nnot-found front\n"),
        text.replace("file a.bin", "unknown a.bin"),
        text.trim_end().to_owned(),
    ] {
        assert!(Index::parse(bad.as_bytes()).is_err(), "{bad}");
    }
}

#[test]
fn index_order_and_duplicate_files_are_refused() {
    let h = digest(b"x");
    let header = "wasmkeep-assets 1\nnot-found front\n";
    let a = format!("file a.bin application/octet-stream identity:{h}:1\n");
    let b = format!("file b.bin application/octet-stream identity:{h}:1\n");
    assert!(Index::parse(format!("{header}{a}{b}").as_bytes()).is_ok());
    for text in [
        format!("{header}{b}{a}"),
        format!("{header}{a}{a}"),
        format!("{header}immutable z/\nfront-first a/\n{a}"),
        format!("{header}front-first z/\nfront-first a/\n{a}"),
    ] {
        assert!(Index::parse(text.as_bytes()).is_err());
    }
}
