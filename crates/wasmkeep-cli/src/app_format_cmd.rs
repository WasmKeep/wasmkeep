//! `wasmkeep app format`: an app's ownership-format setting ([ADR 0035],
//! section 5), as the nodes read it, and with `--enable` the one-way switch
//! to the recorded-head format: only while every registered node of the
//! app announces `recorded-head`, none is live (the app stopped and
//! drained), and the last of them stopped longer ago than the late-write
//! window. `cluster diagnose` probes the bucket's semantics, not this
//! setting; its result says nothing about either.
//!
//! [ADR 0035]: ../../../docs/adr/0035-a-recorded-head-on-hand-over.md

use object_store::ObjectStoreExt as _;
use wasmkeep_cluster::app_format::{self, AppFormat};

use crate::AppFormatArgs;

/// `wasmkeep app format`.
pub(crate) fn run(args: &AppFormatArgs) -> anyhow::Result<()> {
    let (store, prefix) = crate::open_bucket(&args.bucket)?;
    let (tenant, app) = &args.app;
    // The servers' own key: the prefix as their state root normalises it.
    let key = app_format::key_of(&wasmkeep_cluster::state_root::StateRoot::new(
        &prefix, tenant, app,
    ));
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let body = rt.block_on(async {
        match store
            .get(&object_store::path::Path::from(key.as_str()))
            .await
        {
            Ok(got) => got.bytes().await.map(|b| Some(b.to_vec())),
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(e),
        }
    })?;
    if !args.enable {
        println!("{}", describe(tenant, app, &key, body.as_deref()));
        return Ok(());
    }
    match app_format::decode(body.as_deref()) {
        Ok(AppFormat::RecordedHead) => {
            println!("{tenant}/{app}: the recorded-head format is already enabled");
            return Ok(());
        }
        Err(e) => anyhow::bail!("{tenant}/{app}: {e}; nothing written"),
        Ok(AppFormat::Version1) => {}
    }
    let prefixed = wasmkeep_cluster::state_root::StateRoot::new(&prefix, tenant, app)
        .prefix()
        .to_owned();
    let nodes = rt.block_on(wasmkeep_cluster::membership::all_heartbeats(
        &wasmkeep_cluster::object_store::ObjectStoreBucket::new(
            std::sync::Arc::clone(&store),
            prefixed,
        ),
    ))?;
    let horizon = wasmkeep_server::heartbeat_horizon_ms(
        wasmkeep_server::DEFAULT_HEARTBEAT,
        &wasmkeep_cluster::lease::LeaseConfig::default_safe(),
    );
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX));
    let late = u64::try_from(wasmkeep_server::apps::LATE_WRITES.as_millis()).unwrap_or(u64::MAX);
    enable_check(&nodes, &format!("{tenant}/{app}"), now, horizon, late)
        .map_err(|why| anyhow::anyhow!("{tenant}/{app}: {why}; nothing written"))?;
    // The writer itself (ADR 0043, sections 3 and 5): not a `wasmkeep`
    // older than the app's upgrade floor.
    rt.block_on(wasmkeep_server::apps::Readers::read(
        std::sync::Arc::clone(&store),
        &prefix,
        tenant,
        app,
    ))?
    .writer(wasmkeep_cluster::upgrade::this_minor())
    .map_err(|why| anyhow::anyhow!("{tenant}/{app}: {why}; nothing written"))?;
    rt.block_on(write_enabled(&store, &key))
        .map_err(|e| anyhow::anyhow!("{tenant}/{app}: {e}"))?;
    println!(
        "{tenant}/{app}: the recorded-head format is enabled, for good. Start only nodes of \
         this version or later; a server outside the bucket's registrations must be upgraded \
         too before it starts."
    );
    Ok(())
}

/// Writes the enabled setting at `key`, create-only; `Ok(false)` if it was
/// already enabled. Anything else there is an error, never overwritten.
pub(crate) async fn write_enabled(
    store: &std::sync::Arc<dyn object_store::ObjectStore>,
    key: &str,
) -> anyhow::Result<bool> {
    let path = object_store::path::Path::from(key);
    let put = store
        .put_opts(
            &path,
            app_format::ENABLED_BODY.to_vec().into(),
            object_store::PutOptions {
                mode: object_store::PutMode::Create,
                ..object_store::PutOptions::default()
            },
        )
        .await;
    let created = match put {
        Ok(_) => true,
        Err(object_store::Error::AlreadyExists { .. }) => false,
        Err(e) => return Err(e.into()),
    };
    let body = store.get(&path).await?.bytes().await?;
    anyhow::ensure!(
        app_format::decode(Some(&body)) == Ok(AppFormat::RecordedHead),
        "something else is at {key}; nothing overwritten"
    );
    Ok(created)
}

/// Whether the app may switch to the recorded-head format now (ADR 0035,
/// section 5), from its nodes' heartbeats.
pub(crate) fn enable_check(
    nodes: &[wasmkeep_cluster::membership::NodeInfo],
    served: &str,
    now_ms: u64,
    horizon_ms: u64,
    late_ms: u64,
) -> Result<(), String> {
    let mine: Vec<_> = nodes
        .iter()
        .filter(|n| wasmkeep_cluster::membership::serves(n, served))
        .collect();
    // Every registered node, offline ones included, through the one test
    // of "announces this word" (ADR 0043, section 2).
    let old = wasmkeep_cluster::upgrade::without(&["recorded-head"], mine.iter().copied());
    if !old.is_empty() {
        return Err(format!(
            "registered nodes do not announce `recorded-head` ({}): upgrade them, or once \
             stopped remove them with `wasmkeep cluster forget`",
            old.join(", ")
        ));
    }
    let live: Vec<&str> = mine
        .iter()
        .filter(|n| wasmkeep_cluster::membership::is_live(n, now_ms, horizon_ms))
        .map(|n| n.id.as_str())
        .collect();
    if !live.is_empty() {
        return Err(format!(
            "nodes still run the app ({}): stop and drain it first",
            live.join(", ")
        ));
    }
    if let Some(last) = mine.iter().map(|n| n.written_at_ms).max()
        && now_ms < last.saturating_add(late_ms)
    {
        return Err(format!(
            "its nodes stopped {} s ago: wait {} s more for late writes",
            now_ms.saturating_sub(last) / 1_000,
            (last.saturating_add(late_ms) - now_ms).div_ceil(1_000)
        ));
    }
    Ok(())
}

/// What the command prints for the setting at `key` (`None`: absent).
pub(crate) fn describe(tenant: &str, app: &str, key: &str, body: Option<&[u8]>) -> String {
    let setting = match app_format::decode(body) {
        Ok(AppFormat::Version1) => {
            format!("ownership records in version 1 (wasmkeep-owner 1): no setting at {key}")
        }
        Ok(AppFormat::RecordedHead) => format!(
            "the recorded-head format is enabled (wasmkeep-owner 2), at {key}: served only \
             by nodes announcing `recorded-head`"
        ),
        Err(e) => format!("{e}: servers refuse to serve the app (at {key})"),
    };
    format!(
        "{tenant}/{app}: {setting}\n\
         This CLI and servers of this version read ownership records of both versions \
         and announce `recorded-head`; `app format --enable` switches an app, one way. \
         `cluster diagnose` does not check this setting."
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(
        id: &str,
        capable: bool,
        written_at_ms: u64,
        stopped: bool,
    ) -> wasmkeep_cluster::membership::NodeInfo {
        let mut n = wasmkeep_cluster::membership::NodeInfo::decode(
            format!(
                "wasmkeep-node 3\n{} i1\n{written_at_ms}\n{id}\nhttp://{id}\n0.5.0\nt/a\n\n\nserving\n\n{}\n",
                if stopped { "released" } else { "live" },
                if capable { "take recorded-head" } else { "take" }
            )
            .as_bytes(),
        )
        .unwrap();
        n.written_at_ms = written_at_ms;
        n
    }

    /// ADR 0035, section 5: enabled only with every registration capable,
    /// none live, and the late-write window passed.
    #[test]
    fn enabling_waits_for_capable_stopped_nodes() {
        let (horizon, late) = (15_000, 40_000);
        let now = 1_000_000;
        let ok = [
            node("a", true, now - 60_000, true),
            node("b", true, now - 70_000, true),
        ];
        assert_eq!(enable_check(&ok, "t/a", now, horizon, late), Ok(()));
        let old = [
            node("a", true, now - 60_000, true),
            node("o", false, now - 90_000, true),
        ];
        let why = enable_check(&old, "t/a", now, horizon, late).unwrap_err();
        assert!(why.contains("(o)") && why.contains("forget"), "{why}");
        let live = [node("a", true, now - 1_000, false)];
        assert!(
            enable_check(&live, "t/a", now, horizon, late)
                .unwrap_err()
                .contains("stop")
        );
        let recent = [node("a", true, now - 20_000, true)];
        assert!(
            enable_check(&recent, "t/a", now, horizon, late)
                .unwrap_err()
                .contains("wait")
        );
        // Another app's nodes do not count.
        let mut other = node("x", false, now, false);
        other.app = "t/other".into();
        assert_eq!(enable_check(&[other], "t/a", now, horizon, late), Ok(()));
    }

    /// The write is create-only: enabled once, then already enabled; an
    /// unreadable setting is never overwritten.
    #[test]
    fn the_setting_is_written_once_and_never_over_something_else() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let store: std::sync::Arc<dyn object_store::ObjectStore> =
            std::sync::Arc::new(object_store::memory::InMemory::new());
        let key = "formats/t/a/recorded-head";
        assert!(rt.block_on(write_enabled(&store, key)).unwrap());
        assert!(!rt.block_on(write_enabled(&store, key)).unwrap());
        let other = "formats/t/b/recorded-head";
        rt.block_on(store.put(
            &object_store::path::Path::from(other),
            b"junk".to_vec().into(),
        ))
        .unwrap();
        assert!(rt.block_on(write_enabled(&store, other)).is_err());
        let body = rt
            .block_on(async {
                store
                    .get(&object_store::path::Path::from(other))
                    .await?
                    .bytes()
                    .await
            })
            .unwrap();
        assert_eq!(&body[..], b"junk");
    }

    #[test]
    fn each_setting_is_named() {
        let key = "formats/t/a/recorded-head";
        assert!(describe("t", "a", key, None).contains("version 1"));
        assert!(describe("t", "a", key, Some(app_format::ENABLED_BODY)).contains("is enabled"));
        let unknown = describe("t", "a", key, Some(b"junk"));
        assert!(
            unknown.contains("unreadable") && unknown.contains("refuse"),
            "{unknown}"
        );
    }
}
