//! `wasmkeep app delete` ([ADR 0028](../../../docs/adr/0028-actors-that-end.md),
//! section 5): the tombstone first, then, once no live node of the app is
//! left, every key under its two prefixes but the tombstone.

use std::time::Duration;

use wasmkeep_server::apps;

use crate::AppDeleteArgs;

/// How often the command looks at the app's live nodes while it waits.
const POLL: Duration = Duration::from_secs(1);

/// After a removal round, how long the command waits before it lists
/// again: a write sent before a node stopped may still land.
const SETTLE: Duration = Duration::from_secs(3);

/// `wasmkeep app delete`.
pub(crate) fn run(args: &AppDeleteArgs) -> anyhow::Result<()> {
    let (store, prefix) = crate::open_bucket(&args.bucket)?;
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let (tenant, name) = &args.app;
    let ask = apps::DeleteApp {
        wait: Duration::from_secs(args.wait),
        force: args.force,
        who: std::env::var("USER").unwrap_or_else(|_| "-".into()),
    };
    let done = rt
        .block_on(apps::delete_app(
            store,
            &prefix,
            tenant,
            name,
            &ask,
            &mut |line| println!("{line}"),
            POLL,
            SETTLE,
        ))
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    println!("{done}");
    Ok(())
}
