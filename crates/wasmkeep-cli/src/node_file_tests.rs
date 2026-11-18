use std::path::{Path, PathBuf};

use super::{NodeFile, parse, parse_any};

const SHOP: &str = r#"
[process]
id = "edge-1"
data-dir = ".wasmkeep/data"

[listen]
public = "0.0.0.0:8080"
public-advertise = "http://10.0.0.11:8080"
cluster = "0.0.0.0:7443"
cluster-advertise = "https://10.0.0.11:7443"
metrics = "127.0.0.1:9100"

[limits]
max-sockets-per-node = 50000

[[app]]
name = "acme/shop"
node-id = "shop-1"
hostnames = ["Shop.Example.com."]
secrets-key = ["keys/shop.key"]
max-sockets = 20000
min-resident = 200
max-resident = 800
max-front-requests = 32
front-cpu-budget-ms = 50
max-app-blob-bytes = 1073741824
allow-outbound = ["127.0.0.1/32"]
allow-outbound-port = [587]
outbound-ca = ["ca/internal.pem"]
front-env = ["env/shop-front.env"]

[app.env]
cart = ["env/cart.env", "env/cart-2.env"]

[app.access]
cart = "existing"
"#;

const BLOG: &str = r#"
[[app]]
name = "acme/blog"
node-id = "blog-1"
hostnames = ["blog.example.com", "www.blog.example.com"]
data-dir = "data/blog"
no-secrets = true
"#;

/// A cluster node's `[listen]`, for entries with a node id.
const LISTEN: &str = "[listen]\npublic-advertise = \"http://10.0.0.1:8080\"\ncluster = \"0.0.0.0:7443\"\ncluster-advertise = \"https://10.0.0.1:7443\"\n";
/// The same without a cluster listener.
const PLAIN: &str = "[listen]\npublic-advertise = \"http://10.0.0.1:8080\"\n";

fn at() -> &'static Path {
    Path::new("/etc/wasmkeep/wasmkeep-node.toml")
}

fn err_any(text: &str) -> String {
    match parse_any(text, at()) {
        Ok(f) => panic!("expected a refusal, parsed {} apps", f.apps.len()),
        Err(e) => e,
    }
}

fn err(text: &str) -> String {
    match parse(text, at()) {
        Ok(f) => panic!("expected a refusal, parsed {} apps", f.apps.len()),
        Err(e) => e,
    }
}

/// One entry of the app `name` with `node-id` and `rest`.
fn app(name: &str, node: &str, rest: &str) -> String {
    format!("[[app]]\nname = \"{name}\"\nnode-id = \"{node}\"\nno-secrets = true\n{rest}\n")
}

#[test]
fn the_adr_example_parses_every_field_with_paths_from_the_file() {
    let f = parse(SHOP, at()).unwrap();
    let dir = Path::new("/etc/wasmkeep");
    assert_eq!(f.process.id.as_deref(), Some("edge-1"));
    assert_eq!(f.process.data_dir, Some(dir.join(".wasmkeep/data")));
    assert_eq!(f.listen.public, Some("0.0.0.0:8080".parse().unwrap()));
    assert_eq!(
        f.listen.public_advertise.as_deref(),
        Some("http://10.0.0.11:8080")
    );
    assert_eq!(f.listen.cluster, Some("0.0.0.0:7443".parse().unwrap()));
    assert_eq!(
        f.listen.cluster_advertise.as_deref(),
        Some("https://10.0.0.11:7443")
    );
    assert_eq!(f.listen.metrics, Some("127.0.0.1:9100".parse().unwrap()));
    assert_eq!(f.limits.max_sockets_per_node, Some(50_000));
    assert_eq!(f.apps.len(), 1);
    let a = &f.apps[0];
    assert_eq!((a.tenant.as_str(), a.name.as_str()), ("acme", "shop"));
    assert_eq!(a.node_id.as_deref(), Some("shop-1"));
    assert_eq!(a.hostnames, ["shop.example.com"]);
    assert_eq!(a.secrets_keys, [dir.join("keys/shop.key")]);
    assert!(!a.no_secrets);
    assert_eq!(a.max_sockets, Some(20_000));
    assert_eq!((a.min_resident, a.max_resident), (Some(200), Some(800)));
    assert_eq!(a.max_front_requests, Some(32));
    assert_eq!(a.front_cpu_budget_ms, Some(50));
    assert_eq!(a.max_app_blob_bytes, Some(1 << 30));
    assert_eq!(a.allow_outbound, ["127.0.0.1/32".parse().unwrap()]);
    assert_eq!(a.allow_outbound_port, [587]);
    assert_eq!(a.outbound_ca, [dir.join("ca/internal.pem")]);
    assert_eq!(a.front_env, [dir.join("env/shop-front.env")]);
    assert_eq!(
        a.env,
        [
            ("cart".to_owned(), dir.join("env/cart.env")),
            ("cart".to_owned(), dir.join("env/cart-2.env")),
        ]
    );
    assert_eq!(a.access, [("cart".to_owned(), "existing".to_owned())]);
    assert!(
        a.at.starts_with("/etc/wasmkeep/wasmkeep-node.toml:"),
        "{}",
        a.at
    );
}

#[test]
fn a_second_app_is_served_and_each_entry_maps_as_its_flags() {
    // SHOP has a cluster listener; BLOG names its own data directory.
    let two = format!("{SHOP}{BLOG}");
    let file = parse(&two, at()).unwrap();
    assert_eq!(file.apps.len(), 2);
    assert!(file.several());
    let mut base = serve(&["--bucket", "s3://b", "--node-file", "unused.toml"]).unwrap();
    file.apply_entry(1, &mut base).unwrap();
    assert_eq!(base.app, Some(("acme".to_owned(), "blog".to_owned())));
    assert_eq!(base.node_id.as_deref(), Some("blog-1"));
    assert_eq!(base.data_dir, PathBuf::from("/etc/wasmkeep/data/blog"));
    assert!(base.no_secrets);
    assert_eq!(base.cluster_listen, Some("0.0.0.0:7443".parse().unwrap()));
    // An entry's resident floor and cap are the pool's business, not a
    // flag: the entry still maps.
    let mut base = serve(&["--bucket", "s3://b", "--node-file", "unused.toml"]).unwrap();
    file.apply_entry(0, &mut base).unwrap();
    assert_eq!(base.app, Some(("acme".to_owned(), "shop".to_owned())));
    // One app with no hostname table is served as one app.
    let one = parse(
        &app("acme/shop", "x", "").replace("node-id = \"x\"\n", ""),
        at(),
    )
    .unwrap();
    assert!(!one.several());
    // A mistake in the second entry is reported as that mistake.
    let e = err(&format!(
        "{SHOP}{}",
        BLOG.replace("no-secrets", "no-secret")
    ));
    assert!(e.contains("unknown key \"no-secret\""), "{e}");
}

#[test]
fn no_app_is_refused() {
    let e = err("[process]\nid = \"edge-1\"\n");
    assert!(e.contains("[[app]]"), "{e}");
}

#[test]
fn unknown_keys_are_refused_with_the_file_line_and_key() {
    for (text, key, line) in [
        (
            "colour = 1\n".to_owned() + &app("acme/shop", "shop-1", ""),
            "colour",
            1,
        ),
        (
            "[process]\nids = \"a\"\n".to_owned() + &app("acme/shop", "shop-1", ""),
            "ids",
            2,
        ),
        (
            "[listen]\npublik = \"0.0.0.0:1\"\n".to_owned() + &app("acme/shop", "shop-1", ""),
            "publik",
            2,
        ),
        (
            "[limits]\nmax-sockets = 1\n".to_owned() + &app("acme/shop", "shop-1", ""),
            "max-sockets",
            2,
        ),
        (
            app("acme/shop", "shop-1", "hostname = \"a.b\""),
            "hostname",
            5,
        ),
    ] {
        let e = err(&text);
        assert!(e.contains(&format!("unknown key {key:?}")), "{e}");
        assert!(
            e.starts_with(&format!("/etc/wasmkeep/wasmkeep-node.toml:{line}:")),
            "{e}"
        );
    }
}

#[test]
fn invalid_values_are_refused_naming_the_field() {
    for (rest, field) in [
        ("hostnames = [\"shop.example.com:443\"]", "hostnames"),
        ("hostnames = [\"-shop.example.com\"]", "hostnames"),
        ("hostnames = [\"shop..example.com\"]", "hostnames"),
        ("hostnames = [\"a.com\", \"A.com.\"]", "hostnames"),
        ("max-sockets = -1", "max-sockets"),
        ("max-sockets = 60000", "max-sockets"),
        ("min-resident = 900\nmax-resident = 800", "min-resident"),
        ("front-cpu-budget-ms = 0", "front-cpu-budget-ms"),
        ("max-front-requests = 0", "max-front-requests"),
        ("max-app-blob-bytes = 0", "max-app-blob-bytes"),
        ("max-app-blob-bytes = \"1G\"", "max-app-blob-bytes"),
        ("allow-outbound = [\"not-a-range\"]", "allow-outbound"),
        ("allow-outbound-port = [0]", "allow-outbound-port"),
        ("allow-outbound-port = [70000]", "allow-outbound-port"),
        ("no-alarms = \"yes\"", "no-alarms"),
        ("blob-link-base = \"shop.example.com\"", "blob-link-base"),
        (
            "blob-link-base = \"https://shop.example.com/files\"",
            "blob-link-base",
        ),
        (
            "blob-link-base = \"https://user@shop.example.com\"",
            "blob-link-base",
        ),
        ("[app.env]\n_front = [\"f.env\"]", "env"),
        ("[app.env]\ncart = \"f.env\"", "env"),
        ("[app.access]\ncart = \"everyone\"", "access"),
    ] {
        let e = err(&format!("{LISTEN}{}", app("acme/shop", "shop-1", rest)));
        assert!(e.contains(field), "{rest}: {e}");
        assert!(e.starts_with("/etc/wasmkeep/wasmkeep-node.toml:"), "{e}");
    }
    for (text, field) in [
        (app("acme", "shop-1", ""), "name"),
        (app("acme/Shop", "shop-1", ""), "name"),
        (app("acme/shop", "Shop_1", ""), "node-id"),
        (
            "[[app]]\nname = \"acme/shop\"\nsecrets-key = [\"k\"]\nno-secrets = true\n".into(),
            "no-secrets",
        ),
        (
            "[[app]]\nname = \"acme/shop\"\nsecrets-key = [\"a\", \"b\", \"c\"]\n".into(),
            "secrets-key",
        ),
        (
            "[process]\nid = \"Edge 1\"\n".to_owned() + &app("acme/shop", "shop-1", ""),
            "id",
        ),
        (
            "[listen]\npublic = \"localhost\"\n".to_owned() + &app("acme/shop", "shop-1", ""),
            "public",
        ),
        (
            "[listen]\npublic-advertise = \"10.0.0.1:8080\"\n".to_owned()
                + &app("acme/shop", "shop-1", ""),
            "public-advertise",
        ),
        (
            "[listen]\ncluster = \"0.0.0.0:7443\"\n".to_owned() + &app("acme/shop", "shop-1", ""),
            "cluster-advertise",
        ),
        (
            "[listen]\ncluster = \"0.0.0.0:7443\"\ncluster-advertise = \"http://10.0.0.1:7443\"\n"
                .to_owned()
                + &app("acme/shop", "shop-1", ""),
            "cluster-advertise",
        ),
        (
            "[[app]]\nname = \"acme/shop\"\nnode-id = \"a\"\n".into(),
            "public-advertise",
        ),
        (
            format!("[limits]\nmax-sockets-per-node = 10\n{LISTEN}")
                + &app("acme/shop", "shop-1", "max-sockets = 11"),
            "max-sockets",
        ),
        ("app = 1\n".into(), "[[app]]"),
    ] {
        let e = err(&text);
        assert!(e.contains(field), "{text}: {e}");
        assert!(e.starts_with("/etc/wasmkeep/wasmkeep-node.toml:"), "{e}");
    }
}

#[test]
fn hostnames_are_normalized() {
    let f = parse(
        &format!(
            "{LISTEN}{}",
            app(
                "acme/shop",
                "shop-1",
                "hostnames = [\"WWW.Shop.Example.COM.\", \"xn--bcher-kva.example\"]",
            )
        ),
        at(),
    )
    .unwrap();
    assert_eq!(
        f.apps[0].hostnames,
        ["www.shop.example.com", "xn--bcher-kva.example"]
    );
}

#[test]
fn entries_are_compared_whatever_the_number_of_apps() {
    let listen = LISTEN;
    for (apps, why) in [
        (
            app("acme/shop", "shop-1", "") + &app("acme/shop", "shop-2", ""),
            "acme/shop is listed twice",
        ),
        (
            app("acme/shop", "n-1", "") + &app("acme/blog", "n-1", ""),
            "node id n-1 is listed twice",
        ),
        (
            app("acme/shop", "shop-1", "hostnames = [\"a.example\"]")
                + &app("acme/blog", "blog-1", "hostnames = [\"A.example.\"]"),
            "hostname a.example is listed twice",
        ),
        (
            app("acme/shop", "shop-1", "data-dir = \"/srv/d\"")
                + &app(
                    "acme/blog",
                    "blog-1",
                    "data-dir = \"/srv/d/state/acme/shop\"",
                ),
            "overlap",
        ),
        (
            "[process]\ndata-dir = \"/srv/d\"\n".to_owned()
                + &app("acme/shop", "shop-1", "")
                + &app(
                    "acme/blog",
                    "blog-1",
                    "data-dir = \"/srv/d/state/acme/shop/x\"",
                ),
            "overlap",
        ),
    ] {
        let e = err_any(&format!("{listen}{apps}"));
        assert!(e.contains(why), "{why}: {e}");
        assert!(e.starts_with("/etc/wasmkeep/wasmkeep-node.toml:"), "{e}");
    }
    // Two node ids of different apps, different roots: fine.
    let ok = format!(
        "{listen}{}{}",
        app("acme/shop", "shop-1", ""),
        app("acme/blog", "blog-1", "")
    );
    assert_eq!(parse_any(&ok, at()).unwrap().apps.len(), 2);
}

#[test]
fn two_apps_of_a_tenant_are_refused_over_the_layout_before_0_4_0() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("wasmkeep-node.toml");
    let listen = format!("[process]\ndata-dir = \".\"\n{LISTEN}");
    let two_of_acme = format!(
        "{listen}{}{}",
        app("acme/shop", "shop-1", ""),
        app("acme/blog", "blog-1", "")
    );
    let acme_and_beta = format!(
        "{listen}{}{}",
        app("acme/shop", "shop-1", ""),
        app("beta/blog", "blog-1", "")
    );
    assert!(parse_any(&two_of_acme, &file).is_ok());
    std::fs::create_dir_all(dir.path().join("actors/acme/cart")).unwrap();
    let e = parse_any(&two_of_acme, &file).unwrap_err();
    assert!(e.contains("actors/acme"), "{e}");
    assert!(e.contains("one app first"), "{e}");
    // Another tenant's apps, or one app of the tenant: the move is not
    // ambiguous.
    assert!(parse_any(&acme_and_beta, &file).is_ok());
    assert!(
        parse(
            &app("acme/shop", "shop-1", "").replace("node-id = \"shop-1\"\n", ""),
            &file
        )
        .is_ok()
    );
}

#[test]
fn resident_floors_must_fit_the_process_limit() {
    let listen = LISTEN;
    let f = parse_any(
        &format!(
            "{listen}{}{}",
            app("acme/shop", "shop-1", "min-resident = 600"),
            app("acme/blog", "blog-1", "min-resident = 500")
        ),
        at(),
    )
    .unwrap();
    assert!(f.check_resident_floors(1_100).is_ok());
    let e = f.check_resident_floors(1_000).unwrap_err();
    assert!(e.contains("min-resident"), "{e}");
    assert!(e.contains("1100") && e.contains("1000"), "{e}");
}

/// `wasmkeep serve` with `argv`.
fn serve(argv: &[&str]) -> Result<crate::ServeArgs, String> {
    use clap::Parser as _;
    let mut all = vec!["wasmkeep", "serve"];
    all.extend_from_slice(argv);
    match crate::Cli::try_parse_from(all).map_err(|e| e.to_string())? {
        crate::Cli {
            command: crate::Command::Serve(args),
        } => Ok(*args),
        _ => unreachable!(),
    }
}

/// `args` through a node file, set on a command line that has only the
/// process flags and `--node-file`.
fn through_file(file: &NodeFile) -> crate::ServeArgs {
    let mut base = serve(&["--bucket", "s3://b", "--node-file", "unused.toml"]).unwrap();
    file.apply_to(&mut base).unwrap();
    base.node_file = None;
    base
}

const EVERY_APP_FLAG: &[&str] = &[
    "--bucket",
    "s3://b",
    "--app",
    "acme/shop",
    "--node-id",
    "shop-1",
    "--advertise",
    "http://10.0.0.11:8080",
    "--listen",
    "0.0.0.0:8080",
    "--cluster-listen",
    "0.0.0.0:7443",
    "--cluster-advertise",
    "https://10.0.0.11:7443",
    "--metrics-listen",
    "127.0.0.1:9100",
    "--max-sockets-per-node",
    "40000",
    "--max-sockets-per-app",
    "20000",
    "--data-dir",
    "/srv/data",
    "--secrets-key",
    "/k/shop.key",
    "--secrets-key",
    "/k/shop-next.key",
    "--env",
    "cart=/e/cart.env",
    "--env",
    "cart=/e/cart-2.env",
    "--front-env",
    "/e/front.env",
    "--access",
    "cart=existing",
    "--allow-outbound",
    "127.0.0.1/32",
    "--allow-outbound-port",
    "587",
    "--outbound-ca",
    "/ca.pem",
    "--max-front-requests",
    "32",
    "--front-cpu-budget-ms",
    "70",
    "--max-app-blob-bytes",
    "4096",
    "--no-alarms",
];

#[test]
fn serve_app_is_a_one_entry_file_with_the_same_meaning() {
    for argv in [
        EVERY_APP_FLAG.to_vec(),
        vec!["--bucket", "s3://b", "--app", "acme/shop"],
        vec!["--bucket", "s3://b", "--app", "acme/shop", "--no-secrets"],
        // Flags the file does not take stay flags.
        vec![
            "--bucket",
            "s3://b/p",
            "--app",
            "acme/shop",
            "--max-instances",
            "64",
            "--idle-timeout-secs",
            "5",
        ],
    ] {
        let flags = serve(&argv).unwrap();
        let file = NodeFile::from_serve(&flags).expect("--app is a one-entry file");
        assert_eq!(file.apps.len(), 1);
        assert!(file.apps[0].hostnames.is_empty(), "no hostname table");
        let mut base = serve(&{
            let mut v = vec!["--node-file", "unused.toml"];
            // The process flags of this command line.
            let mut i = 0;
            while i < argv.len() {
                if matches!(
                    argv[i],
                    "--bucket" | "--max-instances" | "--idle-timeout-secs"
                ) {
                    v.push(argv[i]);
                    v.push(argv[i + 1]);
                }
                i += 1;
            }
            v
        })
        .unwrap();
        file.apply_to(&mut base).unwrap();
        base.node_file = None;
        assert_eq!(format!("{base:?}"), format!("{flags:?}"), "{argv:?}");
    }
    // Without --app there is no app to describe.
    let actors = serve(&["--actor", "counter=c.wasm"]).unwrap();
    assert!(NodeFile::from_serve(&actors).is_none());
}

#[test]
fn every_field_of_a_file_maps_as_its_flag() {
    let dir = tempfile::tempdir().unwrap();
    let d = dir.path().display().to_string();
    let text = r#"
[process]
data-dir = "data"

[listen]
public = "0.0.0.0:8080"
public-advertise = "http://10.0.0.11:8080"
cluster = "0.0.0.0:7443"
cluster-advertise = "https://10.0.0.11:7443"
metrics = "127.0.0.1:9100"

[limits]
max-sockets-per-node = 40000

[[app]]
name = "acme/shop"
node-id = "shop-1"
secrets-key = ["k/shop.key", "k/shop-next.key"]
max-sockets = 20000
max-front-requests = 32
front-cpu-budget-ms = 70
max-app-blob-bytes = 4096
no-alarms = true
allow-outbound = ["127.0.0.1/32"]
allow-outbound-port = [587]
outbound-ca = ["ca.pem"]
front-env = ["e/front.env"]

[app.env]
cart = ["e/cart.env", "e/cart-2.env"]

[app.access]
cart = "existing"
"#;
    let path = dir.path().join("wasmkeep-node.toml");
    std::fs::write(&path, text).unwrap();
    let file = super::read(&path).unwrap();
    let got = through_file(&file);
    let p = |rel: &str| format!("{d}/{rel}");
    let (k1, k2, e1, e2, fe, ca, data) = (
        p("k/shop.key"),
        p("k/shop-next.key"),
        format!("cart={}", p("e/cart.env")),
        format!("cart={}", p("e/cart-2.env")),
        p("e/front.env"),
        p("ca.pem"),
        p("data"),
    );
    let mut argv: Vec<&str> = EVERY_APP_FLAG.to_vec();
    for (flag, value) in [
        ("/k/shop.key", k1.as_str()),
        ("/k/shop-next.key", k2.as_str()),
        ("cart=/e/cart.env", e1.as_str()),
        ("cart=/e/cart-2.env", e2.as_str()),
        ("/e/front.env", fe.as_str()),
        ("/ca.pem", ca.as_str()),
        ("/srv/data", data.as_str()),
    ] {
        let at = argv.iter().position(|a| *a == flag).unwrap();
        argv[at] = value;
    }
    let want = serve(&argv).unwrap();
    assert_eq!(format!("{got:?}"), format!("{want:?}"));
    // An app's own data-dir wins over the process's.
    let own = parse(
        &format!(
            "[process]\ndata-dir = \"data\"\n{}",
            app("acme/shop", "a", "data-dir = \"/srv/shop\"").replace("node-id = \"a\"\n", "")
        ),
        &path,
    )
    .unwrap();
    assert_eq!(through_file(&own).data_dir, PathBuf::from("/srv/shop"));
}

#[test]
fn fields_this_version_cannot_honour_are_refused_when_serving() {
    for rest in [
        "hostnames = [\"shop.example.com\"]",
        "min-resident = 1",
        "max-resident = 10",
    ] {
        let text = app("acme/shop", "x", rest).replace("node-id = \"x\"\n", "");
        let file = parse(&text, at()).expect(rest);
        let mut base = serve(&["--bucket", "s3://b", "--node-file", "f"]).unwrap();
        let e = file.apply_to(&mut base).unwrap_err().to_string();
        let field = rest.split(' ').next().unwrap();
        assert!(e.contains(field), "{rest}: {e}");
        assert!(e.contains("not available"), "{rest}: {e}");
    }
}

#[test]
fn the_node_file_flag_refuses_the_flags_it_replaces() {
    assert!(
        serve(&["--node-file", "f"])
            .unwrap_err()
            .contains("--bucket")
    );
    for extra in [
        &["--app", "acme/shop"][..],
        &["--node-id", "a"],
        &["--listen", "127.0.0.1:1"],
        &["--data-dir", "d"],
        &["--secrets-key", "k"],
        &["--env", "c=f"],
        &["--front-cpu-budget-ms", "5"],
        &["--max-sockets-per-app", "5"],
        &["--no-alarms"],
        &["--actor", "c=c.wasm"],
    ] {
        let mut argv = vec!["--bucket", "s3://b", "--node-file", "f"];
        argv.extend_from_slice(extra);
        let e = serve(&argv).unwrap_err();
        assert!(e.contains("cannot be used with"), "{extra:?}: {e}");
    }
    // Process flags stay.
    assert!(
        serve(&[
            "--bucket",
            "s3://b",
            "--node-file",
            "f",
            "--max-instances",
            "8"
        ])
        .is_ok()
    );
}

/// Several apps in one process (ADR 0038, section 5): node hops use the
/// cluster port only, so a file of several apps needs a cluster listener,
/// and `--cluster-plaintext accept` is refused with it.
#[test]
fn several_apps_need_the_cluster_port_and_refuse_plain_http_between_nodes() {
    let two = format!(
        "{PLAIN}{}{}",
        app("acme/shop", "shop-1", ""),
        app("acme/blog", "blog-1", "")
    );
    let e = err_any(&two);
    assert!(e.contains("cluster listener"), "{e}");
    let tls = "[listen]\npublic-advertise = \"http://10.0.0.1:8080\"\ncluster = \"0.0.0.0:7443\"\ncluster-advertise = \"https://10.0.0.1:7443\"\n";
    let two = format!(
        "{tls}{}{}",
        app("acme/shop", "shop-1", ""),
        app("acme/blog", "blog-1", "")
    );
    let file = parse_any(&two, at()).unwrap();
    assert!(file.check_plaintext(false).is_ok());
    let e = file.check_plaintext(true).unwrap_err();
    assert!(e.contains("--cluster-plaintext accept"), "{e}");
    // One app routed by hostname is served as several are: refused too.
    let hosts = parse(
        &format!(
            "{LISTEN}{}",
            app("acme/shop", "shop-1", "hostnames = [\"a.test\"]")
        ),
        at(),
    )
    .unwrap();
    assert!(hosts.check_plaintext(true).is_err());
    // One app keeps today's behaviour in both modes.
    let one = parse(&format!("{LISTEN}{}", app("acme/shop", "shop-1", "")), at()).unwrap();
    assert!(one.check_plaintext(true).is_ok());
}

/// Several apps and none sets a socket ceiling: one app can take every
/// session, which start logs (ADR 0038, section 8).
#[test]
fn several_apps_without_a_socket_ceiling_are_warned_about() {
    let tls = "[listen]\npublic-advertise = \"http://10.0.0.1:8080\"\ncluster = \"0.0.0.0:7443\"\ncluster-advertise = \"https://10.0.0.1:7443\"\n";
    let none = parse_any(
        &format!(
            "{tls}{}{}",
            app("acme/shop", "shop-1", ""),
            app("acme/blog", "blog-1", "")
        ),
        at(),
    )
    .unwrap();
    let w = none.warnings();
    assert_eq!(w.len(), 1, "{w:?}");
    assert!(w[0].contains("max-sockets"), "{w:?}");
    let some = parse_any(
        &format!(
            "{tls}{}{}",
            app("acme/shop", "shop-1", "max-sockets = 100"),
            app("acme/blog", "blog-1", "")
        ),
        at(),
    )
    .unwrap();
    assert!(some.warnings().is_empty());
    let one = parse(&format!("{LISTEN}{}", app("acme/shop", "shop-1", "")), at()).unwrap();
    assert!(one.warnings().is_empty());
}

#[test]
fn blob_link_base_is_an_apps_field() {
    let file = parse(
        &format!(
            "{LISTEN}{}",
            app(
                "acme/shop",
                "shop-1",
                "blob-link-base = \"https://files.example.net/\""
            )
        ),
        at(),
    )
    .unwrap();
    assert_eq!(
        file.apps[0].blob_link_base.as_deref(),
        Some("https://files.example.net")
    );
}

/// ADR 0041, section 9: `[process] placement = false` turns placement off
/// for every app of the process, as `serve --no-placement` does; on by
/// default, and a value that is not a boolean is refused.
#[test]
fn placement_is_a_process_switch_on_by_default() {
    let on = parse(&format!("{LISTEN}{}", app("acme/shop", "shop-1", "")), at()).unwrap();
    assert_eq!(on.process.placement, None);
    assert!(!through_file(&on).no_placement);
    let off = parse(
        &format!(
            "[process]\nplacement = false\n{LISTEN}{}",
            app("acme/shop", "shop-1", "")
        ),
        at(),
    )
    .unwrap();
    assert_eq!(off.process.placement, Some(false));
    assert!(through_file(&off).no_placement);
    let e = err(&format!(
        "[process]\nplacement = \"no\"\n{LISTEN}{}",
        app("acme/shop", "shop-1", "")
    ));
    assert!(e.contains("placement"), "{e}");
    // The flag.
    assert!(serve(&["--no-placement"]).unwrap().no_placement);
    assert!(!serve(&[]).unwrap().no_placement);
}
