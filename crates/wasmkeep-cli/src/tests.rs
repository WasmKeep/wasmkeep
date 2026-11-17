#![allow(clippy::unwrap_used)] // test code

use super::{
    check_layout, load_env, move_target, move_target_warning, move_warnings, parse_bucket,
};

/// The front's slots by default: the whole pool for a node serving
/// only a component, a tenth beside actors; an actor-only node keeps
/// a tenth out of its residents as before.
#[test]
fn front_slots_by_default_follow_what_the_node_serves() {
    assert_eq!(
        super::front_requests(None, 1000, true, false).unwrap(),
        1000
    );
    assert_eq!(super::front_requests(None, 1, true, false).unwrap(), 1);
    assert_eq!(super::front_requests(None, 1000, true, true).unwrap(), 100);
    assert_eq!(super::front_requests(None, 1000, false, true).unwrap(), 100);
    // `--app --max-instances 1`: no room for the front's slot and an
    // actor.
    assert!(super::front_requests(None, 1, true, true).is_err());
    assert_eq!(
        super::front_requests(Some(7), 1000, true, false).unwrap(),
        7
    );
    assert!(super::front_requests(Some(1000), 1000, true, true).is_err());
}

/// `serve --app` follows only a rooted `current` (token 3).
#[test]
fn a_current_from_before_0_4_is_refused_at_start() {
    use wasmkeep_server::apps::Current;
    let old = Current::parse(b"v 3 2\n").unwrap();
    let e = super::require_rooted(&old, "acme", "shop", "s3://b/p")
        .unwrap_err()
        .to_string();
    assert!(e.contains("cluster migrate --bucket s3://b/p"), "{e}");
    assert!(e.contains("acme/shop"), "{e}");
    let rooted = Current::parse(b"v 4 3\n").unwrap();
    super::require_rooted(&rooted, "acme", "shop", "s3://b/p").unwrap();
}

/// A command of this version refuses a prefix being migrated, and a
/// tenant whose actor state is still in the layout before 0.4.0;
/// a migrated tenant and a fresh one start (ADR 0022).
#[test]
fn the_layout_check_refuses_an_unmigrated_tenant_and_a_running_migration() {
    use object_store::{
        ObjectStore, ObjectStoreExt as _, PutPayload, memory::InMemory, path::Path,
    };
    let store: std::sync::Arc<dyn ObjectStore> = std::sync::Arc::new(InMemory::new());
    let put = |k: &str| {
        let store = std::sync::Arc::clone(&store);
        let k = k.to_owned();
        std::thread::spawn(move || {
            tokio::runtime::Runtime::new()
                .unwrap()
                .block_on(store.put(&Path::from(k.as_str()), PutPayload::from("x")))
                .unwrap();
        })
        .join()
        .unwrap();
    };
    let check = |tenant: &str| check_layout("s3://b/wk", &store, "wk", tenant);
    check("acme").unwrap();
    // Keys of another prefix, or a move, do not count.
    put("other/owners/acme/c/n");
    put("wk/control/move/acme/c/6e/node-b");
    check("acme").unwrap();
    put("wk/owners/acme/c/n");
    let e = check("acme").unwrap_err().to_string();
    assert!(e.contains("cluster migrate --bucket s3://b/wk"), "{e}");
    check("other").unwrap();
    put("wk/cluster/migrated/acme");
    check("acme").unwrap();
    put("wk/cluster/migration");
    let e = check("other").unwrap_err().to_string();
    assert!(e.contains("in progress"), "{e}");
}

/// `serve --app` before the app's first deploy waits for one, reading
/// `current` as nodes read it for new versions, and goes on with the
/// version that lands; a stop signal while it waits ends it cleanly.
#[test]
fn a_node_started_before_the_first_deploy_waits_for_it() {
    use object_store::{ObjectStore, ObjectStoreExt as _, PutPayload, memory::InMemory};
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let store: std::sync::Arc<dyn ObjectStore> = std::sync::Arc::new(InMemory::new());
    let app = super::AppRef::new(std::sync::Arc::clone(&store), "wk", "acme", "shop");
    let every = std::time::Duration::from_millis(20);
    rt.block_on(async {
        // Stopped while nothing is deployed: no version, no error.
        let stopped = super::first_deploy(
            &app,
            "acme/shop",
            "s3://b/wk",
            every,
            tokio::time::sleep(std::time::Duration::from_millis(100)),
        )
        .await
        .unwrap();
        assert!(stopped.is_none());
        // A deploy lands while it waits: that version.
        let deploy = async {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            store
                .put(
                    &"wk/apps/acme/shop/current".into(),
                    PutPayload::from("3313a78daa7324b0 1 3\n"),
                )
                .await
                .unwrap();
        };
        let (current, ()) = tokio::join!(
            super::first_deploy(
                &app,
                "acme/shop",
                "s3://b/wk",
                every,
                std::future::pending()
            ),
            deploy
        );
        let current = current.unwrap().unwrap();
        assert_eq!(
            (current.version.as_str(), current.seq),
            ("3313a78daa7324b0", 1)
        );
        // Already deployed: at once.
        let current = super::first_deploy(
            &app,
            "acme/shop",
            "s3://b/wk",
            every,
            std::future::pending(),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(current.seq, 1);
    });
}

/// `actor move` runs the start check `serve` runs (ADR 0022): it
/// refuses a tenant whose state is still in the layout before 0.4.0,
/// naming the migrate command, and writes nothing.
#[test]
fn actor_move_refuses_an_unmigrated_tenant() {
    use clap::Parser as _;
    use object_store::{
        ObjectStore, ObjectStoreExt as _, PutPayload, memory::InMemory, path::Path,
    };
    let store: std::sync::Arc<dyn ObjectStore> = std::sync::Arc::new(InMemory::new());
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(store.put(&Path::from("wk/owners/acme/room/x"), PutPayload::from("x")))
        .unwrap();
    let super::Cli {
        command: super::Command::Actor(super::ActorCommand::Move(args)),
    } = super::Cli::try_parse_from([
        "wasmkeep",
        "actor",
        "move",
        "--app",
        "acme/shop",
        "room/x",
        "--to",
        "node-b",
        "--force",
        "--bucket",
        "s3://b/wk",
    ])
    .unwrap()
    else {
        unreachable!()
    };
    let e = super::actor_move_on(&args, || Ok((std::sync::Arc::clone(&store), "wk".into())))
        .unwrap_err()
        .to_string();
    assert!(e.contains("cluster migrate --bucket s3://b/wk"), "{e}");
    let listed: Vec<_> = rt.block_on(async {
        use futures_util::TryStreamExt as _;
        store.list(None).try_collect::<Vec<_>>().await.unwrap()
    });
    assert_eq!(listed.len(), 1, "{listed:?}");
}

fn delete_args(extra: &[&str]) -> super::DeleteArgs {
    use clap::Parser as _;
    let mut argv = vec![
        "wasmkeep",
        "actor",
        "delete",
        "--app",
        "acme/shop",
        "room/x 1",
        "--bucket",
        "s3://b/wk",
    ];
    argv.extend_from_slice(extra);
    let super::Cli {
        command: super::Command::Actor(super::ActorCommand::Delete(args)),
    } = super::Cli::try_parse_from(argv).unwrap()
    else {
        unreachable!()
    };
    args
}

fn keys_of(store: &std::sync::Arc<dyn object_store::ObjectStore>) -> Vec<String> {
    use futures_util::TryStreamExt as _;
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut keys: Vec<String> = rt.block_on(async {
        store
            .list(None)
            .map_ok(|m| m.location.to_string())
            .try_collect()
            .await
            .unwrap()
    });
    keys.sort();
    keys
}

/// `actor delete` (ADR 0028) refuses an actor with nothing in the
/// bucket and writes nothing; otherwise it writes the request under the
/// app's root and waits for its key to be gone.
#[test]
fn actor_delete_writes_its_request_and_waits_for_it() {
    use clap::Parser as _;
    use object_store::{ObjectStore, ObjectStoreExt as _, PutPayload, memory::InMemory};
    let store: std::sync::Arc<dyn ObjectStore> = std::sync::Arc::new(InMemory::new());
    let open = || Ok((std::sync::Arc::clone(&store), "wk".to_owned()));
    let poll = std::time::Duration::from_millis(20);
    let e = super::actor_delete_on(&delete_args(&["--no-wait"]), open, poll)
        .unwrap_err()
        .to_string();
    assert!(e.contains("no such actor"), "{e}");
    assert!(keys_of(&store).is_empty());
    // Its history: one segment.
    let rt = tokio::runtime::Runtime::new().unwrap();
    let segment = object_store::path::Path::from(format!(
        "wk/{}wal/1-1-0.ltx",
        wasmkeep_server::history_folder(
            &wasmkeep_cluster::state_root::StateRoot::new("", "acme", "shop"),
            "room",
            "x 1"
        )
    ));
    rt.block_on(store.put(&segment, PutPayload::from("x")))
        .unwrap();
    super::actor_delete_on(&delete_args(&["--no-wait"]), open, poll).unwrap();
    // The request, and `control/changed` rewritten after it, so nodes
    // list it within a period (ADR 0014, amendment of 2026-11-14).
    let all = keys_of(&store);
    assert!(all.contains(&"wk/control/changed".to_owned()), "{all:?}");
    let requests = |store: &std::sync::Arc<dyn ObjectStore>| -> Vec<String> {
        keys_of(store)
            .into_iter()
            .filter(|k| k != "wk/control/changed")
            .collect()
    };
    let keys = requests(&store);
    assert_eq!(keys.len(), 2, "{keys:?}");
    assert_eq!(keys[0], segment.to_string());
    let request = keys[1].clone();
    let id = request
        .strip_prefix("wk/state/acme/shop/deletes/room/782031/")
        .unwrap_or_else(|| panic!("{request}"));
    assert_eq!(id.len(), 16, "{request}");
    // Nobody acts: it says so, and its request stays (a request of its
    // own, beside the first).
    let e = super::actor_delete_on(&delete_args(&["--wait", "0"]), open, poll)
        .unwrap_err()
        .to_string();
    assert!(e.contains("not deleted yet"), "{e}");
    assert_eq!(requests(&store).len(), 3);
    // Requests carried out as a node does: the outcome written, then
    // the request deleted.
    let carry_out = |store: &std::sync::Arc<dyn ObjectStore>, outcome: &str| {
        let rt = tokio::runtime::Runtime::new().unwrap();
        for k in keys_of(store)
            .into_iter()
            .filter(|k| k.contains("/deletes/") && !k.ends_with(".done") && !k.ends_with(".absent"))
        {
            if !outcome.is_empty() {
                rt.block_on(store.put(
                    &object_store::path::Path::from(format!("{k}{outcome}")),
                    PutPayload::new(),
                ))
                .unwrap();
            }
            rt.block_on(store.delete(&object_store::path::Path::from(k)))
                .unwrap();
        }
    };
    carry_out(&store, "");
    let run_with = |outcome: &'static str| {
        let remover = std::sync::Arc::clone(&store);
        let node = std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(200));
            carry_out(&remover, outcome);
        });
        let r = super::actor_delete_on(&delete_args(&["--wait", "10"]), open, poll)
            .map_err(|e| e.to_string());
        node.join().unwrap();
        r
    };
    // Dropped without an outcome (expired): not reported as deleted.
    let e = run_with("").unwrap_err();
    assert!(e.contains("dropped without an outcome"), "{e}");
    // No such actor, by the node's look.
    let e = run_with(".absent").unwrap_err();
    assert!(e.contains("no such actor"), "{e}");
    // Carried out: done; outcomes remain readable for resumed waiters.
    run_with(".done").unwrap();
    assert!(requests(&store).iter().any(|key| key.ends_with(".done")));
    assert!(
        super::Cli::try_parse_from([
            "wasmkeep",
            "actor",
            "delete",
            "room/x",
            "--app",
            "acme/shop",
            "--bucket",
            "s3://b",
            "--wait",
            "3601",
        ])
        .is_err(),
        "a wait past the request's expiry"
    );
    assert!(
        super::Cli::try_parse_from([
            "wasmkeep",
            "actor",
            "delete",
            "room/x",
            "--app",
            "acme/shop",
            "--bucket",
            "s3://b",
            "--no-wait",
            "--wait",
            "5",
        ])
        .is_err(),
        "--no-wait with --wait"
    );
}

/// `--remove-old` takes its own flags only, and never with
/// `--rollback` or `--heir`.
#[test]
fn remove_old_flags_come_with_it() {
    use clap::Parser as _;
    let parse = |extra: &[&str]| {
        let mut argv = vec!["wasmkeep", "cluster", "migrate", "--bucket", "s3://b"];
        argv.extend_from_slice(extra);
        super::Cli::try_parse_from(argv)
            .map(drop)
            .map_err(|e| e.to_string())
    };
    parse(&["--remove-old"]).unwrap();
    parse(&["--remove-old", "--apply", "--forgo-rollback"]).unwrap();
    parse(&["--remove-old", "--history-retention-secs", "0"]).unwrap();
    assert!(parse(&["--forgo-rollback"]).is_err());
    assert!(parse(&["--history-retention-secs", "0"]).is_err());
    assert!(parse(&["--remove-old", "--rollback"]).is_err());
    assert!(parse(&["--remove-old", "--heir", "a/b"]).is_err());
}

/// `actor move` names the actor's app with `--app`, or the implicit
/// app with `default/` in front (ADR 0022).
#[test]
fn a_moved_actor_is_named_within_its_app() {
    let shop = ("acme".to_owned(), "shop".to_owned());
    let owned = |t: &str, a: &str, c: &str, n: &str| {
        ((t.to_owned(), a.to_owned()), c.to_owned(), n.to_owned())
    };
    assert_eq!(
        move_target("room/lobby/1", Some(&shop)).unwrap(),
        owned("acme", "shop", "room", "lobby/1")
    );
    assert_eq!(
        move_target("default/room/x", None).unwrap(),
        owned("default", "default", "room", "x")
    );
    let e = move_target("acme/room/x", None).unwrap_err().to_string();
    assert!(e.contains("--app acme/APP room/x"), "{e}");
    assert!(move_target("room", Some(&shop)).is_err());
    assert!(move_target("room/", Some(&shop)).is_err());
    assert!(move_target("default/room", None).is_err());
}

/// The tenant kept in front with `--app` (the syntax before it) parses
/// as a class named like the tenant: allowed, with a warning.
#[test]
fn a_tenant_kept_in_front_with_app_gets_a_warning() {
    let shop = ("acme".to_owned(), "shop".to_owned());
    let w = move_target_warning("acme/room/lobby", Some(&shop)).expect("a warning");
    assert!(w.contains("room/lobby"), "{w}");
    assert!(move_target_warning("room/lobby", Some(&shop)).is_none());
    assert!(move_target_warning("default/room/x", None).is_none());
}

/// A move target that cannot take the actor gets a warning saying why.
#[test]
fn a_move_target_that_cannot_take_it_gets_a_warning() {
    use wasmkeep_cluster::membership::{DrainState, NodeInfo};
    let node = |can_take, drain| NodeInfo {
        id: "c".into(),
        address: "http://c:8080".into(),
        version: "0.0.0".into(),
        written_at_ms: 0,
        app: "acme/shop".into(),
        cluster_address: None,
        tls_pins: Vec::new(),
        drain,
        reads_secrets: true,
        can_take,
        can_front: true,
        can_handoff: true,
        can_end: true,
        can_flush: true,
        can_rate_limits: false,
        can_restore: true,
        can_actor_restore: false,
        instance: None,
        lease: None,
        load: None,
        can_recorded_head: false,
    };
    assert!(move_warnings(&node(true, Some(DrainState::Serving)), "acme/shop").is_empty());
    assert!(move_warnings(&node(true, None), "acme/shop").is_empty());
    // Another app's node, of the same tenant or not: it refuses the
    // hop (ADR 0022).
    for app in ["globex/shop", "acme/admin"] {
        let other = move_warnings(&node(true, None), app);
        assert!(
            other.len() == 1 && other[0].contains(&format!("not {app}")),
            "{other:?}"
        );
    }
    let old = move_warnings(&node(false, None), "acme/shop");
    assert!(
        old.len() == 1 && old[0].contains("does not take moved actors"),
        "{old:?}"
    );
    for state in [DrainState::Draining, DrainState::Drained] {
        let w = move_warnings(&node(true, Some(state)), "acme/shop");
        assert!(w.len() == 1 && w[0].contains("is draining"), "{w:?}");
    }
}

#[test]
fn bucket_urls_split_into_bucket_and_prefix() {
    assert_eq!(parse_bucket("s3://b").unwrap(), ("b", ""));
    assert_eq!(parse_bucket("s3://b/").unwrap(), ("b", ""));
    assert_eq!(parse_bucket("s3://b/prod/eu/").unwrap(), ("b", "prod/eu"));
    assert!(parse_bucket("b/prod").is_err());
    assert!(parse_bucket("s3:///prod").is_err());
}

#[test]
fn env_files_must_name_a_served_class() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("hub.env");
    std::fs::write(&file, "TOKEN=t\n").unwrap();
    let env = load_env(&[("hub".into(), file.clone())], &["hub", "monitor"], true).unwrap();
    assert_eq!(env.for_class("hub").get("TOKEN").unwrap(), "t");
    let e = load_env(&[("hbu".into(), file.clone())], &["hub", "monitor"], true).unwrap_err();
    assert!(e.to_string().contains("no class hbu"), "{e}");
    let e = load_env(
        &[("hub".into(), dir.path().join("missing"))],
        &["hub"],
        true,
    )
    .unwrap_err();
    assert!(e.to_string().contains("missing"), "{e}");
    // With --app (not strict) a class the version lacks is only warned
    // about, and its values are kept for a deploy that brings it.
    let env = load_env(&[("gone".into(), file)], &["hub"], false).unwrap();
    assert_eq!(env.summary().len(), 1);
}

#[test]
fn secret_looking_configuration_is_named_without_its_value() {
    let vars = wasmkeep_server::env::parse(
        "URL=https://api.example.test/v1\n\
         DATABASE_URL=postgres://app:hunter2@db.internal/app\n\
         API_KEY=abc123\n\
         auth_header=xyz\n\
         PLAIN=https://user@example.test/\n\
         MODE=fast\n",
    )
    .unwrap();
    let warnings = super::secret_looking("hub", &vars);
    let named: Vec<&str> = ["API_KEY", "DATABASE_URL", "auth_header"].to_vec();
    assert_eq!(warnings.len(), named.len(), "{warnings:?}");
    for (w, name) in warnings.iter().zip(&named) {
        assert!(w.contains(&format!("hub: {name} ")), "{w}");
    }
    for value in ["hunter2", "abc123", "xyz", "postgres://"] {
        assert!(
            !warnings.concat().contains(value),
            "a value leaked: {value}"
        );
    }
}

#[test]
fn a_front_removed_beside_secrets_is_warned_about() {
    let removed = wasmkeep_server::apps::Diff {
        front: Some("removed"),
        ..Default::default()
    };
    // The set is sealed: whether it has `_front` entries is not
    // known without the key, so any set (or one not known) warns.
    let bucket = "s3://shop-bucket/prod";
    for secrets in [Some(true), None] {
        let w = super::front_secrets_warning(&removed, secrets, bucket, "acme", "shop").unwrap();
        assert!(w.contains("`_front`"), "{w}");
        // Both commands complete, as the secrets CLI parses them, once
        // the key file and the names are filled in.
        let commands: Vec<&str> = w.split('`').skip(1).step_by(2).collect();
        let runnable: Vec<&&str> = commands
            .iter()
            .filter(|c| c.starts_with("wasmkeep "))
            .collect();
        assert_eq!(runnable.len(), 2, "{w}");
        for (command, sub) in runnable.iter().zip(["list", "unset"]) {
            assert!(command.contains(&format!("secrets {sub} ")), "{command}");
            assert!(command.contains(&format!("--bucket {bucket}")), "{command}");
            assert!(command.contains("--app acme/shop"), "{command}");
            assert!(command.contains("--key KEY_FILE"), "{command}");
            let filled = command
                .replace("KEY_FILE", "/etc/wasmkeep/secrets.key")
                .replace("NAME...", "SESSION_KEY OTHER");
            let parsed = <super::Cli as clap::Parser>::try_parse_from(filled.split(' '));
            assert!(parsed.is_ok(), "{command}: {}", parsed.unwrap_err());
        }
        assert!(runnable[1].contains("--front NAME..."), "{w}");
        assert!(!w.contains(" if "), "plainly worded: {w}");
    }
    assert!(super::front_secrets_warning(&removed, Some(false), bucket, "acme", "shop").is_none());
    for front in [None, Some("added"), Some("changed"), Some("unchanged")] {
        let diff = wasmkeep_server::apps::Diff {
            front,
            ..Default::default()
        };
        assert!(super::front_secrets_warning(&diff, Some(true), bucket, "acme", "shop").is_none());
    }
}

/// The line a deploy ends with: a switch, a rollback (from which
/// version to which), or nothing changed (the sequence not bumped).
#[test]
fn a_deploy_says_whether_it_switched_rolled_back_or_changed_nothing() {
    let current = |version: &str, seq| wasmkeep_server::apps::Current {
        version: version.into(),
        seq,
        marked: true,
        rooted: true,
    };
    let deployed =
        |version, seq, previous: Option<&str>, unchanged| wasmkeep_server::apps::Deployed {
            current: current(version, seq),
            previous: previous.map(Into::into),
            unchanged,
            diff: wasmkeep_server::apps::Diff::default(),
            newly_marked: false,
        };
    let (old, new) = ("3313a78daa7324b0", "8447609efdefe311");
    assert_eq!(
        super::deployed_line("acme", "shop", &deployed(new, 2, Some(old), false), false),
        "acme/shop: version 8447609efdefe311 is current (deploy 2)"
    );
    assert_eq!(
        super::deployed_line("acme", "shop", &deployed(new, 1, None, false), false),
        "acme/shop: version 8447609efdefe311 is current (deploy 1)"
    );
    assert_eq!(
        super::deployed_line("acme", "shop", &deployed(new, 3, Some(old), false), true),
        "rolled back acme/shop: 3313a78daa7324b0 -> 8447609efdefe311 (deploy 3)"
    );
    assert_eq!(
        super::deployed_line("acme", "shop", &deployed(new, 2, Some(new), true), false),
        "acme/shop: nothing changed: version 8447609efdefe311 is already current (deploy 2)"
    );
}

/// A rollback says which version it rolled back to above what changed,
/// and its note about the project file comes on the same stream, before
/// the line it ends with.
#[test]
fn a_rollback_names_its_version_above_its_changes() {
    let deployed = |unchanged| wasmkeep_server::apps::Deployed {
        current: wasmkeep_server::apps::Current {
            version: "8447609efdefe311".into(),
            seq: 3,
            marked: true,
            rooted: true,
        },
        previous: Some("3313a78daa7324b0".into()),
        unchanged,
        diff: wasmkeep_server::apps::Diff {
            front: Some("changed"),
            ..Default::default()
        },
        newly_marked: false,
    };
    let note = "note: wasmkeep.toml may now describe another version than the one rolled \
                back to; the next deploy from it deploys the file again";
    assert_eq!(
        super::rollback_lines("acme", "shop", &deployed(false), Some("wasmkeep.toml")),
        [
            "rolling back acme/shop to 8447609efdefe311:",
            "  front: changed",
            note,
        ]
    );
    assert_eq!(
        super::rollback_lines("acme", "shop", &deployed(false), None),
        [
            "rolling back acme/shop to 8447609efdefe311:",
            "  front: changed"
        ]
    );
    // Nothing changed: no header over nothing.
    assert!(super::rollback_lines("acme", "shop", &deployed(true), None).is_empty());
}

/// Log lines carry colour codes only on a terminal, and not even there
/// under NO_COLOR (set and not empty).
#[test]
fn logs_are_coloured_only_on_a_terminal_without_no_color() {
    use std::ffi::OsString;
    assert!(super::log_colours(true, None));
    assert!(super::log_colours(true, Some(OsString::new())));
    assert!(!super::log_colours(true, Some(OsString::from("1"))));
    assert!(!super::log_colours(false, None));
    assert!(!super::log_colours(false, Some(OsString::from("1"))));
}

#[test]
fn a_rename_that_drops_a_rule_is_warned_about() {
    let diff = wasmkeep_server::apps::Diff {
        added: vec!["monitors".into()],
        removed: vec![("monitor".into(), Some("existing".into()))],
        ..Default::default()
    };
    let w = super::deploy_warnings(&diff, None);
    assert_eq!(w.len(), 1);
    assert!(w[0].contains("--access monitors="), "{}", w[0]);
    let ruled = wasmkeep_server::apps::Diff {
        added_rules: [("monitors".into(), "existing".into())].into(),
        ..diff.clone()
    };
    assert!(super::deploy_warnings(&ruled, None).is_empty());
    let open_removed = wasmkeep_server::apps::Diff {
        removed: vec![("old".into(), None)],
        ..diff
    };
    assert!(super::deploy_warnings(&open_removed, None).is_empty());
}

#[test]
fn requires_flags_parse_and_renames_are_warned_about() {
    let (class, names) = super::parse_requires("hub=TOKEN,URL").unwrap();
    assert_eq!(class, "hub");
    assert_eq!(names.into_iter().collect::<Vec<_>>(), ["TOKEN", "URL"]);
    assert!(
        super::parse_requires("hub=").unwrap().1.is_empty(),
        "removes"
    );
    for bad in ["hub", "=TOKEN", "hub=1BAD", "hub=A B"] {
        assert!(super::parse_requires(bad).is_err(), "{bad}");
    }
    let diff = wasmkeep_server::apps::Diff {
        added: vec!["hub2".into()],
        requires: vec![
            "hub: TOKEN no longer required (class removed)".into(),
            "hub: URL no longer required (class removed)".into(),
        ],
        ..Default::default()
    };
    let w = super::deploy_warnings(&diff, None);
    assert_eq!(w.len(), 1, "{w:?}");
    assert!(w[0].starts_with("hub had required values"), "{}", w[0]);
    assert!(w[0].contains("--requires"), "{}", w[0]);
    // From the file: what to set in it, never a flag it refuses.
    let w = super::deploy_warnings(&diff, Some("wasmkeep.toml"));
    assert_eq!(w.len(), 1, "{w:?}");
    assert!(
        w[0].contains("set requires in [classes.hub2] of wasmkeep.toml"),
        "{}",
        w[0]
    );
    assert!(!w[0].contains("--"), "{}", w[0]);
    // The added class has required names of its own: no warning.
    let given = wasmkeep_server::apps::Diff {
        requires: [
            diff.requires.clone(),
            vec!["hub2: TOKEN now required".into()],
        ]
        .concat(),
        ..diff.clone()
    };
    assert!(super::deploy_warnings(&given, None).is_empty());
    assert!(super::deploy_warnings(&given, Some("wasmkeep.toml")).is_empty());
    // Two added, one with names: the warning names the other.
    let one = wasmkeep_server::apps::Diff {
        added: vec!["hub2".into(), "other".into()],
        ..given
    };
    let w = super::deploy_warnings(&one, None);
    assert_eq!(w.len(), 1, "{w:?}");
    assert!(w[0].contains("while other was added"), "{}", w[0]);
}

#[test]
fn a_rename_from_the_file_says_what_to_set_in_it() {
    let diff = wasmkeep_server::apps::Diff {
        added: vec!["monitors".into()],
        added_rules: [("monitors".into(), "open".into())].into(),
        removed: vec![("monitor".into(), Some("existing".into()))],
        ..Default::default()
    };
    let w = super::deploy_warnings(&diff, Some("app/wasmkeep.toml"));
    assert_eq!(w.len(), 1, "{w:?}");
    assert!(
        w[0].contains("set access in [classes.monitors] of app/wasmkeep.toml"),
        "{}",
        w[0]
    );
    assert!(!w[0].contains("--access"), "{}", w[0]);
}

/// A deploy that changes nothing prints no change lines, from the
/// file or with flags: its last line says nothing changed.
#[test]
fn a_no_op_deploy_prints_no_changes() {
    let diff = wasmkeep_server::apps::Diff {
        front: Some("unchanged"),
        open_past_front: vec!["c".into()],
        ..Default::default()
    };
    let deployed = |unchanged| wasmkeep_server::apps::Deployed {
        current: wasmkeep_server::apps::Current {
            version: "8447609efdefe311".into(),
            seq: 2,
            marked: true,
            rooted: true,
        },
        previous: Some("8447609efdefe311".into()),
        unchanged,
        diff: diff.clone(),
        newly_marked: false,
    };
    for from_file in [false, true] {
        assert!(super::change_lines(&deployed(true), from_file).is_empty());
    }
    // A change with flags prints its lines; from the file, the plan did.
    assert_eq!(
        super::change_lines(&deployed(false), false),
        [
            "  front: unchanged",
            "  class c: open, reachable at /actor/c/<name>, past the front"
        ]
    );
    assert!(super::change_lines(&deployed(false), true).is_empty());
}

#[test]
fn idle_limits_and_app_delete_parse() {
    use clap::Parser as _;
    // ADR 0028: --expire-after-idle CLASS=DURATION, CLASS= removes it.
    let deploy = |args: &[&str]| {
        super::Cli::try_parse_from(
            ["wasmkeep", "deploy", "--app", "t/a", "--bucket", "s3://b"]
                .iter()
                .chain(args),
        )
    };
    let limits = |args: &[&str]| match deploy(args).unwrap().command {
        super::Command::Deploy(d) => d.expire_after_idle,
        _ => unreachable!(),
    };
    assert_eq!(
        limits(&[
            "--expire-after-idle",
            "carts=7d",
            "--expire-after-idle",
            "rooms="
        ]),
        [
            ("carts".to_owned(), Some(7 * 86_400)),
            ("rooms".to_owned(), None)
        ]
    );
    for bad in [
        "carts=30m",
        "carts=7",
        "carts",
        "=7d",
        "_front=7d",
        "carts=7w",
    ] {
        assert!(deploy(&["--expire-after-idle", bad]).is_err(), "{bad}");
    }
    assert!(deploy(&["--rollback", "--expire-after-idle", "c=1h"]).is_err());
    assert!(deploy(&["--recreate", "--actor", "c=c.wasm"]).is_ok());
    assert!(deploy(&["--recreate", "--rollback"]).is_err());
    // `app delete --app T/A --bucket ...`, with its wait and --force.
    let delete = |args: &[&str]| {
        super::Cli::try_parse_from(["wasmkeep", "app", "delete"].iter().chain(args))
    };
    match delete(&["--app", "acme/legacy", "--bucket", "s3://b/p"])
        .unwrap()
        .command
    {
        super::Command::App(super::AppCommand::Delete(d)) => {
            assert_eq!(d.app, ("acme".to_owned(), "legacy".to_owned()));
            assert_eq!((d.wait, d.force), (60, false));
        }
        _ => unreachable!(),
    }
    assert!(
        delete(&[
            "--app",
            "acme/legacy",
            "--bucket",
            "s3://b",
            "--wait",
            "5",
            "--force"
        ])
        .is_ok()
    );
    assert!(
        delete(&["--bucket", "s3://b"]).is_err(),
        "--app is required"
    );
    assert!(delete(&["--app", "acme", "--bucket", "s3://b"]).is_err());
}

/// `app delete` against a bucket with no node: the tombstone, every key
/// of the app but it removed, other apps untouched; run again, it
/// finishes; the implicit app is refused.
#[test]
fn app_delete_removes_both_prefixes_and_keeps_the_tombstone() {
    use object_store::ObjectStoreExt as _;
    let store: std::sync::Arc<dyn object_store::ObjectStore> =
        std::sync::Arc::new(object_store::memory::InMemory::new());
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let keys = [
        "p/apps/acme/legacy/current",
        "p/apps/acme/legacy/0123/manifest",
        "p/apps/acme/legacy/secrets/0000000000000001",
        "p/state/acme/legacy/actors/c/n/wal/1",
        "p/state/acme/legacy/owners/c/n",
        "p/state/acme/other/owners/c/n",
        "p/apps/acme/legacy-two/current",
    ];
    rt.block_on(async {
        for k in keys {
            store
                .put(&object_store::path::Path::from(k), b"x".to_vec().into())
                .await
                .unwrap();
        }
    });
    let ask = wasmkeep_server::apps::DeleteApp {
        resume: None,
        wait: std::time::Duration::from_secs(1),
        force: false,
        who: "test".into(),
        late: std::time::Duration::from_millis(10),
    };
    let run = |tenant: &str, app: &str| {
        let mut said = Vec::new();
        let out = rt.block_on(wasmkeep_server::apps::delete_app(
            std::sync::Arc::clone(&store),
            "p",
            tenant,
            app,
            &ask,
            &mut |l| said.push(l),
            std::time::Duration::from_millis(10),
            std::time::Duration::from_millis(10),
        ));
        (out, said)
    };
    let (out, said) = run("acme", "legacy");
    assert!(
        out.as_ref().unwrap().contains("5 object(s) removed"),
        "{out:?}"
    );
    assert!(said[0].contains("tombstone written"), "{said:?}");
    let left: Vec<String> = rt.block_on(async {
        use futures_util::TryStreamExt as _;
        store
            .list(None)
            .map_ok(|m| m.location.to_string())
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
    });
    assert_eq!(
        left,
        [
            "p/apps/acme/legacy-two/current",
            "p/apps/acme/legacy/deleted",
            "p/state/acme/other/owners/c/n",
        ]
    );
    let (out, said) = run("acme", "legacy");
    assert!(out.unwrap().contains("0 object(s) removed"));
    assert!(said[0].contains("already deleted"), "{said:?}");
    assert!(run("acme", "none").0.is_err(), "no such app");
    assert!(run("default", "default").0.is_err(), "the implicit app");
}

#[test]
fn deploy_flags_parse() {
    use clap::Parser as _;
    let parse = |args: &[&str]| {
        super::Cli::try_parse_from(
            ["wasmkeep", "deploy", "--app", "t/a", "--bucket", "s3://b"]
                .iter()
                .chain(args),
        )
    };
    assert!(parse(&["--access", "monitor=existing"]).is_ok());
    assert!(parse(&["--config", "hub=hub.env", "--config", "old="]).is_ok());
    assert!(parse(&["--rollback", "--allow-reopen"]).is_ok());
    assert!(parse(&["--allow-reopen"]).is_err(), "only with --rollback");
    assert!(parse(&["--rollback", "--access", "c=open"]).is_err());
    assert!(parse(&["--rollback", "--config", "c=f"]).is_err());
    assert!(parse(&["--access", "c=closed"]).is_err());
    // The front's flags (ADR 0023).
    assert!(parse(&["--front", "front.wasm"]).is_ok());
    assert!(parse(&["--no-front"]).is_ok());
    assert!(parse(&["--front", "f.wasm", "--no-front"]).is_err());
    assert!(parse(&["--front-config", "front.env", "--front-requires", "A,B"]).is_ok());
    assert!(parse(&["--front-config", "", "--front-requires", ""]).is_ok());
    assert!(parse(&["--front-requires", "1BAD"]).is_err());
    assert!(parse(&["--front-requires", "A,A"]).is_err());
    for flag in [
        &["--front", "f.wasm"][..],
        &["--no-front"],
        &["--front-config", "f.env"],
        &["--front-requires", "A"],
    ] {
        let mut args = vec!["--rollback"];
        args.extend_from_slice(flag);
        assert!(parse(&args).is_err(), "{flag:?}");
    }
    let requires = |args: &[&str]| match parse(args).unwrap().command {
        super::Command::Deploy(d) => d.front_requires,
        _ => unreachable!(),
    };
    assert_eq!(
        requires(&["--front-requires", "B,A"]),
        Some(["A".to_owned(), "B".to_owned()].into())
    );
    assert_eq!(
        requires(&["--front-requires", ""]),
        Some(Default::default())
    );
    // The front's scope is not a class: its own flags give its values.
    let class_flags = |args: &[&str]| match parse(args).unwrap().command {
        super::Command::Deploy(d) => super::refuse_front_scope(&d).map_err(|e| e.to_string()),
        _ => unreachable!(),
    };
    assert!(class_flags(&["--config", "c=f.env", "--requires", "c=A"]).is_ok());
    for (args, flag, own) in [
        (
            &["--config", "_front=f.env"][..],
            "--config",
            "--front-config FILE",
        ),
        (&["--config", "_front="], "--config", "--front-config FILE"),
        (
            &["--requires", "_front=A"],
            "--requires",
            "--front-requires NAMES",
        ),
        (&["--access", "_front=open"], "--access", "no access rule"),
    ] {
        let e = class_flags(args).unwrap_err();
        assert!(
            e.starts_with(&format!("{flag} _front=...: {flag} takes classes")),
            "{e}"
        );
        assert!(e.contains(own), "{e}");
    }
    let retention = |args: &[&str]| match parse(args).unwrap().command {
        super::Command::Deploy(d) => d.version_retention_secs,
        _ => unreachable!(),
    };
    assert_eq!(retention(&["--rollback"]), 3600, "an hour by default");
    assert_eq!(
        retention(&["--rollback", "--version-retention-secs", "0"]),
        0
    );
}

#[test]
fn broken_pipes_are_told_from_other_failures() {
    assert!(super::is_broken_pipe_panic(
        "failed printing to stdout: Broken pipe (os error 32)"
    ));
    assert!(super::is_broken_pipe_panic(
        "failed printing to stderr: Broken pipe (os error 32)"
    ));
    assert!(!super::is_broken_pipe_panic(
        "failed printing to stdout: No space left on device (os error 28)"
    ));
    assert!(!super::is_broken_pipe_panic("Broken pipe somewhere else"));
}

#[test]
fn a_drain_request_names_both_states() {
    let said = super::drain_asked("a");
    assert!(said.starts_with("Asked a to drain; "), "{said}");
    assert!(
        said.contains("`draining`, then `drained` once its actors are handed over"),
        "{said}"
    );
    assert!(said.contains("straight to `drained`"), "{said}");
    assert!(!said.contains("within a few seconds"), "{said}");
}

#[test]
fn a_node_following_an_app_takes_its_rules_from_the_deploy() {
    let mut deployed = wasmkeep_server::AccessRules::default();
    deployed.apply("monitor", "existing").unwrap();
    let flags = vec![("monitor".to_owned(), "internal".to_owned())];
    let served = ["hub", "monitor"];
    // Rules deployed (marker): --access refused, with the command to run.
    let e = super::serve_app_rules(&flags, &served, true, deployed.clone(), "t", "a")
        .unwrap_err()
        .to_string();
    assert!(
        e.contains("wasmkeep deploy --app t/a --bucket ... --access monitor=internal"),
        "{e}"
    );
    let rules = super::serve_app_rules(&[], &served, true, deployed.clone(), "t", "a").unwrap();
    assert_eq!(rules.of("monitor"), wasmkeep_server::Access::Existing);
    // Never deployed: the flags, during the migration; else nothing.
    let rules =
        super::serve_app_rules(&flags, &served, false, Default::default(), "t", "a").unwrap();
    assert_eq!(rules.of("monitor"), wasmkeep_server::Access::Internal);
    let rules = super::serve_app_rules(&[], &served, false, Default::default(), "t", "a").unwrap();
    assert_eq!(rules.of("monitor"), wasmkeep_server::Access::Open);
    let typo = vec![("monitr".to_owned(), "existing".to_owned())];
    assert!(super::serve_app_rules(&typo, &served, false, Default::default(), "t", "a").is_err());
}

/// `wasmkeep serve` with the cluster flags and `extra`.
fn parse_serve(extra: &[&str]) -> Result<super::ServeArgs, String> {
    use clap::Parser as _;
    let mut argv = vec![
        "wasmkeep",
        "serve",
        "--actor",
        "counter=counter.wasm",
        "--node-id",
        "a",
        "--advertise",
        "http://10.0.0.11:8080",
        "--bucket",
        "s3://b",
    ];
    argv.extend_from_slice(extra);
    match super::Cli::try_parse_from(argv).map_err(|e| e.to_string())? {
        super::Cli {
            command: super::Command::Serve(args),
        } => Ok(*args),
        _ => unreachable!(),
    }
}

#[test]
fn cluster_listener_flags_come_together_and_need_a_cluster() {
    let args = parse_serve(&[
        "--cluster-listen",
        "0.0.0.0:7443",
        "--cluster-advertise",
        "https://10.0.0.11:7443",
    ])
    .unwrap();
    assert_eq!(args.cluster_listen, Some("0.0.0.0:7443".parse().unwrap()));
    assert_eq!(
        args.cluster_advertise.as_deref(),
        Some("https://10.0.0.11:7443")
    );
    assert!(parse_serve(&[]).unwrap().cluster_listen.is_none());

    let e = parse_serve(&["--cluster-advertise", "https://10.0.0.11:7443"]).unwrap_err();
    assert!(e.contains("--cluster-listen"), "{e}");
    let e = parse_serve(&["--cluster-listen", "0.0.0.0:7443"]).unwrap_err();
    assert!(e.contains("--cluster-advertise"), "{e}");
    for plain in [
        "http://10.0.0.11:7443",
        "10.0.0.11:7443",
        "https://",
        "https://:7443",
    ] {
        let e = parse_serve(&[
            "--cluster-listen",
            "0.0.0.0:7443",
            "--cluster-advertise",
            plain,
        ])
        .unwrap_err();
        assert!(e.contains("https://"), "{plain}: {e}");
    }

    // Without the cluster flags.
    use clap::Parser as _;
    let e = super::Cli::try_parse_from([
        "wasmkeep",
        "serve",
        "--actor",
        "counter=counter.wasm",
        "--cluster-listen",
        "0.0.0.0:7443",
        "--cluster-advertise",
        "https://10.0.0.11:7443",
    ])
    .unwrap_err()
    .to_string();
    assert!(e.contains("--node-id"), "{e}");
}

#[test]
fn plaintext_between_nodes_is_refused_by_default() {
    use super::ClusterPlaintext;
    let listen = [
        "--cluster-listen",
        "0.0.0.0:7443",
        "--cluster-advertise",
        "https://10.0.0.11:7443",
    ];
    assert_eq!(
        parse_serve(&[]).unwrap().cluster_plaintext,
        ClusterPlaintext::Refuse
    );
    assert_eq!(
        parse_serve(&listen).unwrap().cluster_plaintext,
        ClusterPlaintext::Refuse
    );
    let args = parse_serve(&[&listen[..], &["--cluster-plaintext", "refuse"]].concat()).unwrap();
    assert_eq!(args.cluster_plaintext, ClusterPlaintext::Refuse);
    let args = parse_serve(&["--cluster-plaintext", "accept"]).unwrap();
    assert_eq!(args.cluster_plaintext, ClusterPlaintext::Accept);

    // Whether a cluster node has a listener is checked when it starts
    // (tests/cluster_tls.rs): a single node has none and needs none.
    let e = parse_serve(&["--cluster-plaintext", "never"]).unwrap_err();
    assert!(e.contains("accept") && e.contains("refuse"), "{e}");
}

#[test]
fn access_rules_must_name_a_served_class() {
    let (class, mode) = super::parse_access("monitor=existing").unwrap();
    let rules = super::access_rules(&[(class, mode)], &["hub", "monitor"]).unwrap();
    assert_eq!(rules.of("monitor"), wasmkeep_server::Access::Existing);
    assert!(super::parse_access("monitor=closed").is_err());
    assert!(super::parse_access("monitor").is_err());
    assert!(super::parse_access("hub=internal:main").is_err());
    let (class, mode) = super::parse_access("hub=existing:main").unwrap();
    let rules = super::access_rules(&[(class, mode)], &["hub"]).unwrap();
    assert_eq!(
        rules.for_actor("hub", "main"),
        wasmkeep_server::Access::Open
    );
    let typo = vec![("monitr".to_owned(), "internal".to_owned())];
    assert!(super::access_rules(&typo, &["monitor"]).is_err());
}

/// `deploy --writes CLASS=MODE` (ADR 0034, section 15): `strict` or
/// `batched`, for a class; anything else is refused when parsed.
#[test]
fn writes_flag_parses_a_class_and_a_mode() {
    use super::{Cli, Command, parse_writes};
    use wasmkeep_server::apps::Writes;
    assert_eq!(
        parse_writes("rooms=batched").unwrap(),
        ("rooms".to_owned(), Writes::Batched)
    );
    assert_eq!(
        parse_writes("rooms=strict").unwrap(),
        ("rooms".to_owned(), Writes::Strict)
    );
    for bad in ["rooms", "rooms=gated", "=batched", "_front=batched"] {
        assert!(parse_writes(bad).is_err(), "{bad}");
    }
    let args = <Cli as clap::Parser>::try_parse_from([
        "wasmkeep",
        "deploy",
        "--bucket",
        "file:///tmp/b",
        "--writes",
        "rooms=batched",
        "--writes",
        "chat=strict",
    ])
    .unwrap();
    match args.command {
        Command::Deploy(d) => assert_eq!(d.writes.len(), 2),
        _ => panic!("not a deploy"),
    }
}

/// `serve --writes CLASS=MODE` (ADR 0034): the batched classes of the ones
/// given with `--actor`, refused for a class not served or with `--app`
/// (a deployed version names its own).
#[test]
fn serve_writes_names_served_classes_only() {
    use super::serve_batched;
    use wasmkeep_server::apps::Writes;
    let served = ["t", "rooms"];
    let given = vec![
        ("rooms".to_owned(), Writes::Batched),
        ("t".to_owned(), Writes::Strict),
    ];
    let batched = serve_batched(&given, &served, false).unwrap();
    assert_eq!(
        batched.into_iter().collect::<Vec<_>>(),
        vec!["rooms".to_owned()]
    );
    assert!(serve_batched(&[("other".to_owned(), Writes::Batched)], &served, false).is_err());
    assert!(serve_batched(&given, &served, true).is_err(), "with --app");
    assert!(serve_batched(&[], &served, true).unwrap().is_empty());
}

// An operation handle must be accepted without selecting a new operation.
fn resume_cli(command: &[&str]) {
    use clap::Parser as _;
    let mut argv = vec!["wasmkeep"];
    argv.extend_from_slice(command);
    argv.extend_from_slice(&[
        "--bucket",
        "s3://b",
        "--app",
        "acme/shop",
        "--resume",
        "0123456789abcdef",
    ]);
    assert!(super::Cli::try_parse_from(argv).is_ok());
}
#[test]
fn stable_id_actor_delete_resume() {
    resume_cli(&["actor", "delete", "room/x"]);
}
#[test]
fn stable_id_app_delete_resume() {
    resume_cli(&["app", "delete"]);
}
#[test]
fn stable_id_actor_restore_resume() {
    resume_cli(&["actor", "restore", "room/x"]);
}
#[test]
fn stable_id_backup_restore_resume() {
    resume_cli(&["backup", "restore", "--from", "s3://archive"]);
}

#[test]
fn stable_id_actor_delete_timeout_finish_recreate_resume() {
    use object_store::{ObjectStoreExt as _, path::Path};
    let store: std::sync::Arc<dyn object_store::ObjectStore> =
        std::sync::Arc::new(object_store::memory::InMemory::new());
    let rt = tokio::runtime::Runtime::new().unwrap();
    let history = Path::from(
        "wk/state/acme/shop/actors/room/x%201/snapshot/0000000000000001-0000000000000001.ltx",
    );
    rt.block_on(store.put(&history, b"history present".to_vec().into()))
        .unwrap();
    let open = || Ok((store.clone(), "wk".into()));
    let poll = std::time::Duration::ZERO;
    assert!(super::actor_delete_on(&delete_args(&["--wait", "0"]), open, poll).is_err());
    let request = keys_of(&store)
        .into_iter()
        .find(|k| k.contains("/deletes/"))
        .unwrap();
    let id = request.rsplit('/').next().unwrap();
    rt.block_on(async {
        store
            .put(
                &Path::from(format!("wk/{}.done", request.strip_prefix("wk/").unwrap())),
                Vec::new().into(),
            )
            .await
            .unwrap();
        store.delete(&Path::from(request.as_str())).await.unwrap();
        store
            .put(&history, b"new incarnation".to_vec().into())
            .await
            .unwrap();
    });
    let before = keys_of(&store);
    for unknown in ["deadbeef", "../../invalid"] {
        assert!(
            super::actor_delete_on(
                &delete_args(&["--resume", unknown, "--wait", "0"]),
                open,
                poll
            )
            .is_err()
        );
        assert_eq!(keys_of(&store), before);
    }

    for _ in 0..2 {
        super::actor_delete_on(&delete_args(&["--resume", id, "--wait", "0"]), open, poll).unwrap();
        assert_eq!(keys_of(&store), before);
    }
    rt.block_on(store.delete(&Path::from(format!("{request}.done"))))
        .unwrap();
    let expired = keys_of(&store);
    assert!(
        super::actor_delete_on(&delete_args(&["--resume", id, "--no-wait"]), open, poll).is_err()
    );
    assert_eq!(keys_of(&store), expired);
    assert_eq!(
        rt.block_on(async { store.get(&history).await.unwrap().bytes().await.unwrap() }),
        "new incarnation"
    );
}

#[test]
fn stable_id_app_delete_timeout_finish_recreate_resume() {
    use object_store::{ObjectStoreExt as _, path::Path};
    let store: std::sync::Arc<dyn object_store::ObjectStore> =
        std::sync::Arc::new(object_store::memory::InMemory::new());
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let current = Path::from("apps/acme/shop/current");
        store.put(&current, b"old".to_vec().into()).await.unwrap();
        let mut ask = wasmkeep_server::apps::DeleteApp {
            resume: None,
            wait: std::time::Duration::ZERO,
            force: false,
            who: "test".into(),
            late: std::time::Duration::from_secs(10),
        };
        let run = |ask: wasmkeep_server::apps::DeleteApp| {
            let store = store.clone();
            async move {
                wasmkeep_server::apps::delete_app(
                    store,
                    "",
                    "acme",
                    "shop",
                    &ask,
                    &mut |_| {},
                    std::time::Duration::ZERO,
                    std::time::Duration::ZERO,
                )
                .await
            }
        };
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), run(ask.clone()))
                .await
                .is_err()
        );
        let app = wasmkeep_server::apps::AppRef::new(store.clone(), "", "acme", "shop");
        let tombstone = app.tombstone().await.unwrap().unwrap();
        let id = tombstone
            .lines()
            .find_map(|s| s.strip_prefix("id "))
            .unwrap()
            .to_owned();
        ask.late = std::time::Duration::ZERO;
        run(ask.clone()).await.unwrap();
        ask.resume = Some(id.clone());
        for _ in 0..2 {
            run(ask.clone()).await.unwrap();
        }
        app.recreate(std::time::Duration::ZERO).await.unwrap();
        store
            .put(&current, b"new incarnation".to_vec().into())
            .await
            .unwrap();
        ask.resume = Some(id);
        for _ in 0..2 {
            assert!(run(ask.clone()).await.is_err());
            assert!(app.tombstone().await.unwrap().is_none());
            assert_eq!(
                store.get(&current).await.unwrap().bytes().await.unwrap(),
                "new incarnation"
            );
        }
    });
}
