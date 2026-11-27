//! `wasmkeep cluster markers clean`: removes the per-actor lease markers
//! an app still has once its upgrade floor is 0.10
//! ([ADR 0045](../../../docs/adr/0045-the-generation-floor-of-a-restored-app.md),
//! section 4). A marker its actor's ownership record carries is deleted; one
//! that is the only carrier of a seed (a restore's, before 0.10) is first
//! folded into the app's generation floor, then deleted. Once `leases/` is
//! empty the floor says `markers none`, and nodes skip the marker's read.
//! It may run with nodes live, and again at any point.

use std::fmt::Write as _;
use std::sync::Arc;

use clap::{Args, Subcommand};
use wasmkeep_cluster::app_format::floor::Minor;
use wasmkeep_cluster::generation_floor as gf;
use wasmkeep_cluster::object_store::ObjectStoreBucket;
use wasmkeep_cluster::state_root::StateRoot;
use wasmkeep_cluster::upgrade::{self, Blocked};

/// `wasmkeep cluster markers`.
#[derive(Debug, Subcommand)]
pub enum MarkersCommand {
    /// Remove an app's per-actor lease markers (ADR 0045), once its upgrade
    /// to 0.10 is finalized: refused while the app's upgrade floor is
    /// below 0.10, or while a live node of the app does not announce
    /// `genfloor`. A marker the actor's ownership record carries is
    /// deleted; one that is the only carrier of a generation (left by a
    /// restore before 0.10) is first folded into the app's generation
    /// floor, then deleted. When none is left, the floor says so and nodes
    /// stop reading markers. Safe with nodes live, idempotent, and safe to
    /// interrupt; `wasmkeep cluster verify` counts what is left. Without
    /// --yes it prints the counts, writes nothing and exits with 2.
    Clean(CleanArgs),
}

/// `wasmkeep cluster markers clean`.
#[derive(Debug, Args)]
pub struct CleanArgs {
    /// Bucket shared by the cluster: s3://BUCKET[/PREFIX] (credentials from
    /// the AWS_* environment variables).
    #[arg(long)]
    pub bucket: String,

    /// The app, TENANT/APP.
    #[arg(long, value_parser = crate::parse_app)]
    pub app: (String, String),

    /// Delete; without it, print the counts and exit with 2.
    #[arg(long)]
    pub yes: bool,
}

/// What a run printed, and its exit code (0 done, 2 counted only).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Outcome {
    /// The lines to print.
    pub out: String,
    /// The exit code.
    pub code: u8,
}

/// The lease configuration a per-actor lease from before node leases is
/// judged live by: the nodes' defaults.
fn lease_config() -> wasmkeep_cluster::lease::LeaseConfig {
    wasmkeep_server::LeaseConfig::default_safe()
}

/// The machine's clock.
struct Now;
impl wasmkeep_cluster::lease::Clock for Now {
    fn now_ms(&self) -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
    }
}

/// Runs `markers clean` for `tenant/app` under `prefix` of `store`, as a
/// `wasmkeep` of minor version `writer` (this build's when `None`; tests
/// give another).
///
/// # Errors
/// Refused (the floor, a live node, this `wasmkeep`'s version), a live
/// old lease, something unreadable, or the bucket did not answer.
pub async fn clean(
    store: Arc<dyn object_store::ObjectStore>,
    prefix: &str,
    tenant: &str,
    app: &str,
    yes: bool,
    writer: Option<Minor>,
) -> anyhow::Result<Outcome> {
    let name = format!("{tenant}/{app}");
    let this = writer
        .map_or_else(upgrade::this_minor, Ok)
        .map_err(anyhow::Error::msg)?;
    let readers = wasmkeep_server::apps::AppRef::new(Arc::clone(&store), prefix, tenant, app)
        .readers()
        .await?;
    readers
        .writer(this)
        .map_err(|why| anyhow::anyhow!("{name}: {why}"))?;
    match readers.gate(&upgrade::GENERATION_FLOOR) {
        Ok(()) => {}
        Err(Blocked::Floor { floor, .. }) => anyhow::bail!(
            "{name}: its upgrade floor is {floor}, and the markers are removed only once it is \
             {}: nodes below it still write and read them. Run `wasmkeep cluster upgrade \
             finalize` once every node runs {} or later, then this command. Nothing was \
             written",
            gf::SINCE,
            gf::SINCE
        ),
        Err(blocked) => anyhow::bail!("{name}: {blocked}. Nothing was written"),
    }
    let bucket = ObjectStoreBucket::new(store, prefix.to_owned());
    let root = StateRoot::new("", tenant, app);
    let plan = gf::plan_clean(&bucket, &root, &Now, &lease_config()).await?;
    let (carried, folded) = plan.counts();
    let mut out = String::new();
    let _ = writeln!(
        out,
        "{name}: {} per-actor markers: {carried} carried by their ownership record, {folded} \
         the only carrier of their generation{}.",
        plan.leftovers.len(),
        plan.raise_to().map_or_else(String::new, |to| format!(
            " (the generation floor goes from {} to {to} first)",
            plan.floor.generation
        )),
    );
    if !yes {
        let _ = writeln!(out, "Nothing written: run again with --yes to delete them.");
        return Ok(Outcome { out, code: 2 });
    }
    let done = gf::clean(&bucket, &root, &plan).await?;
    let _ = writeln!(
        out,
        "Deleted {} markers; the generation floor is {}.",
        done.deleted, done.floor.generation
    );
    if done.left == 0 {
        let _ = writeln!(
            out,
            "No marker is left: nodes no longer read them for {name}."
        );
    } else {
        let _ = writeln!(
            out,
            "{} markers were written meanwhile, by a node that had not yet read the finalized \
             floor: run this command again.",
            done.left
        );
    }
    Ok(Outcome { out, code: 0 })
}

impl MarkersCommand {
    /// The bucket the command names.
    pub fn bucket(&self) -> &str {
        let Self::Clean(args) = self;
        &args.bucket
    }
}

/// `wasmkeep cluster markers` on the bucket `store` (opened by the caller,
/// with the checks every writing command makes), under `prefix`: runs the
/// command and exits with its code.
///
/// # Errors
/// What [`clean`] gives.
pub fn run(
    command: MarkersCommand,
    store: Arc<dyn object_store::ObjectStore>,
    prefix: String,
) -> anyhow::Result<()> {
    let MarkersCommand::Clean(args) = command;
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let outcome = rt.block_on(clean(
        store,
        &prefix,
        &args.app.0,
        &args.app.1,
        args.yes,
        None,
    ))?;
    print!("{}", outcome.out);
    if outcome.code != 0 {
        use std::io::Write as _;
        let _ = std::io::stdout().flush();
        std::process::exit(i32::from(outcome.code));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use object_store::ObjectStoreExt as _;
    use object_store::memory::InMemory;

    use super::*;
    use wasmkeep_cluster::lease::Clock as _;

    const NINE: Minor = Minor { major: 0, minor: 9 };

    async fn put(store: &Arc<dyn object_store::ObjectStore>, key: &str, body: &[u8]) {
        store.put(&key.into(), body.to_vec().into()).await.unwrap();
    }

    async fn exists(store: &Arc<dyn object_store::ObjectStore>, key: &str) -> bool {
        store.head(&key.into()).await.is_ok()
    }

    /// Refused below the floor 0.10 and beside a live node without
    /// `genfloor`; counted without --yes; then the markers go, the floor
    /// carries the only seed, and it says `markers none`.
    #[tokio::test]
    async fn markers_clean_is_gated_counts_then_deletes() {
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let floor_key = "formats/acme/shop/floor";
        put(
            &store,
            floor_key,
            &wasmkeep_cluster::app_format::floor::encode(NINE),
        )
        .await;
        put(
            &store,
            "state/acme/shop/leases/c/a",
            b"wasmkeep-lease 2\n3\n",
        )
        .await;
        put(
            &store,
            "state/acme/shop/owners/c/a",
            b"wasmkeep-owner 1\n- -\n4\n",
        )
        .await;
        put(
            &store,
            "state/acme/shop/leases/c/b",
            b"wasmkeep-lease 2\n17\n",
        )
        .await;
        let run = async |yes, writer| {
            clean(Arc::clone(&store), "", "acme", "shop", yes, Some(writer)).await
        };
        let e = run(true, gf::SINCE).await.unwrap_err();
        assert!(format!("{e:#}").contains("upgrade floor is 0.9"), "{e:#}");
        put(
            &store,
            floor_key,
            &wasmkeep_cluster::app_format::floor::encode(gf::SINCE),
        )
        .await;
        // A 0.9 `wasmkeep` may not write under the floor 0.10.
        assert!(run(true, NINE).await.is_err());
        // A live node of the app that does not announce `genfloor`.
        let at = Now.now_ms() + 60_000;
        let blind = format!(
            "wasmkeep-node 3\nlive beef\n{at}\nn1\nhttp://127.0.0.1:9\n0.10.0\nacme/shop\n\n\nserving\n\ntake\n"
        );
        put(&store, "nodes/n1", blind.as_bytes()).await;
        let e = run(true, gf::SINCE).await.unwrap_err();
        assert!(format!("{e:#}").contains("genfloor"), "{e:#}");
        store.delete(&"nodes/n1".into()).await.unwrap();

        let counted = run(false, gf::SINCE).await.unwrap();
        assert_eq!(counted.code, 2);
        assert!(counted.out.contains("1 carried"), "{}", counted.out);
        assert!(
            counted.out.contains("1 the only carrier"),
            "{}",
            counted.out
        );
        assert!(
            exists(&store, "state/acme/shop/leases/c/b").await,
            "nothing written"
        );

        let done = run(true, gf::SINCE).await.unwrap();
        assert_eq!(done.code, 0, "{}", done.out);
        assert!(!exists(&store, "state/acme/shop/leases/c/a").await);
        assert!(!exists(&store, "state/acme/shop/leases/c/b").await);
        let body = store
            .get(&"state/acme/shop/generation-floor".into())
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(
            &body[..],
            b"wasmkeep-generation-floor 1\n17\nmarkers none\n"
        );
        // Again: nothing to do, the same floor.
        let again = run(true, gf::SINCE).await.unwrap();
        assert!(again.out.contains("0 per-actor markers"), "{}", again.out);
    }
}
