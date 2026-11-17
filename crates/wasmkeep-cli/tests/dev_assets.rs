//! Assets-only project through the real dev command, without guest builds.
#![cfg(unix)]
#![allow(clippy::unwrap_used, clippy::panic)] // test process helpers
mod support;
use std::time::{Duration, Instant};
#[test]
fn dev_assets_build_watch_live_disk_and_refused_names() {
    let temp = tempfile::tempdir().unwrap();
    let dir = temp.path().join("project");
    std::fs::create_dir_all(dir.join("src")).unwrap();
    std::fs::write(dir.join("src/index.html"), "first").unwrap();
    std::fs::write(dir.join("wasmkeep.toml"), "[app]\ntenant='acme'\nname='site'\n[assets]\ndir='public'\nbuild='rm -rf public; mkdir public; cp src/index.html public/index.html'\nwatch=['src']\nnot-found='spa'\nfront-first=['api/']\n").unwrap();
    let dev = support::start_dev(temp, dir, 50, Duration::from_secs(20));
    let get = |path: &str| support::request(dev.addr, "GET", path, "", "").unwrap();
    assert_eq!(get("/"), (200, "first".into()));
    let until = |path: &str, expected: (u16, &str)| {
        let start = Instant::now();
        loop {
            let r = get(path);
            if (r.0, r.1.as_str()) == expected {
                break;
            }
            assert!(
                start.elapsed() < Duration::from_secs(5),
                "{r:?}: {}",
                dev.log()
            );
            std::thread::sleep(Duration::from_millis(20));
        }
    };
    std::fs::write(dev.dir.join("src/index.html"), "rebuilt").unwrap();
    until("/", (200, "rebuilt"));
    std::fs::write(dev.dir.join("public/index.html"), "on disk").unwrap();
    until("/", (200, "on disk"));
    std::fs::write(dev.dir.join("public/new.txt"), "new").unwrap();
    until("/new.txt", (200, "new"));
    std::fs::write(dev.dir.join("public/.secret"), "secret").unwrap();
    std::os::unix::fs::symlink("/etc/passwd", dev.dir.join("public/link.txt")).unwrap();
    std::fs::write(dev.dir.join("public/index.html.gz"), "unchecked").unwrap();
    std::thread::sleep(Duration::from_millis(150));
    for path in ["/.secret", "/link.txt", "/index.html.gz", "/api/a"] {
        assert_eq!(get(path).0, 404);
    }
    assert!(dev.log().contains("link.txt"));
    assert_eq!(
        support::request(dev.addr, "GET", "/navigation", "Accept: text/html\r\n", "").unwrap(),
        (200, "on disk".into())
    );
    std::fs::remove_file(dev.dir.join("public/new.txt")).unwrap();
    until("/new.txt", (404, "not found\n"));
}
