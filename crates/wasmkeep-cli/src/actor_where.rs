//! `wasmkeep actor where`: which node owns each named actor (ADR 0017,
//! ADR 0020). Reads the actors' ownership records, the heartbeats and
//! the floor of each app named (for the warning of ADR 0043, section 5),
//! and nothing else: it writes no key, claims nothing and wakes no actor.

use std::sync::Arc;

use clap::Args;
use object_store::ObjectStore;
use serde_json::{Value, json};
use wasmkeep_cluster::control::Keys as _;
use wasmkeep_cluster::membership::{self, NodeInfo};
use wasmkeep_cluster::node_lease::OwnerRecord;
use wasmkeep_cluster::object_store::ObjectStoreBucket;
use wasmkeep_cluster::state_root::{Family, StateRoot};

use crate::status::{age, table};

#[derive(Debug, Args)]
pub(crate) struct WhereArgs {
    /// The actors: CLASS/NAME with `--app`, or `default/CLASS/NAME` for
    /// the implicit app `default/default` (the name may contain `/`).
    #[arg(required = true)]
    actors: Vec<String>,

    /// The actors' app, TENANT/APP; `default/default` when an actor is
    /// written `default/CLASS/NAME`.
    #[arg(long, value_parser = crate::parse_app)]
    app: Option<(String, String)>,

    /// Bucket shared by the cluster: s3://BUCKET[/PREFIX] (credentials from
    /// the AWS_* environment variables; read access is enough).
    #[arg(long)]
    bucket: String,

    /// Print JSON instead of a table, for scripts.
    #[arg(long)]
    json: bool,
}

/// One actor's placement, as read.
#[derive(Debug)]
pub(crate) struct Row {
    actor: String,
    app: String,
    /// The node id its record names; `None`: no record, or released.
    owner: Option<String>,
    /// Whether that node's heartbeat is live; `None` without an owner.
    live: Option<bool>,
    /// When the record was written (the bucket's clock, ms): the owner's
    /// acquisition, as records are written on acquire and release only.
    since_ms: Option<u64>,
}

#[derive(Debug)]
pub(crate) struct Report {
    rows: Vec<Row>,
    /// What a read-only command warns about and goes on (ADR 0043,
    /// section 5): an app named whose live nodes are outside this
    /// `wasmkeep`'s range, or whose floor is above it. Printed on standard
    /// error, so the table and the JSON are what they were.
    pub(crate) warnings: Vec<String>,
}

impl Report {
    /// Plain columns, as `cluster status`; `now_ms` dates "since".
    pub(crate) fn to_text(&self, now_ms: u64) -> String {
        let rows: Vec<[String; 4]> = self
            .rows
            .iter()
            .map(|r| {
                let dash = || "-".to_owned();
                [
                    r.actor.clone(),
                    r.owner.clone().unwrap_or_else(|| "not placed".to_owned()),
                    r.live
                        .map_or_else(dash, |l| if l { "yes" } else { "no" }.to_owned()),
                    r.since_ms
                        .map_or_else(dash, |at| format!("{} ago", age(now_ms.saturating_sub(at)))),
                ]
            })
            .collect();
        let mut out = String::new();
        table(&mut out, ["ACTOR", "OWNER", "LIVE", "SINCE"], &rows);
        out
    }

    pub(crate) fn to_json(&self) -> Value {
        Value::Array(
            self.rows
                .iter()
                .map(|r| {
                    json!({
                        "actor": r.actor,
                        "app": r.app,
                        "placed": r.owner.is_some(),
                        "owner": r.owner,
                        "live": r.live,
                        "since_ms": r.since_ms,
                    })
                })
                .collect(),
        )
    }
}

pub(crate) fn run(args: &WhereArgs) -> anyhow::Result<()> {
    // The warning is beside the answer, never in its way: a version or a
    // floor that cannot be read is said, and the rows are printed.
    let this = wasmkeep_cluster::upgrade::this_minor();
    if let Err(why) = &this {
        eprintln!("warning: {why}; the nodes' versions are not checked");
    }
    let report = report_on(args, this.ok(), || {
        wasmkeep_cli::open_bucket_unchecked(&args.bucket)
    })?;
    for warning in &report.warnings {
        eprintln!("warning: {warning}");
    }
    if args.json {
        println!("{:#}", report.to_json());
    } else {
        print!("{}", report.to_text(crate::status::now_ms()));
    }
    Ok(())
}

/// The report on the bucket `open` gives, read by a `wasmkeep` of minor
/// version `this` (`None`: the nodes' versions are not checked). Only
/// reads.
pub(crate) fn report_on(
    args: &WhereArgs,
    this: Option<wasmkeep_cluster::app_format::floor::Minor>,
    open: impl FnOnce() -> anyhow::Result<(Arc<dyn ObjectStore>, String)>,
) -> anyhow::Result<Report> {
    let mut targets = Vec::new();
    for actor in &args.actors {
        targets.push(crate::move_target(actor, args.app.as_ref())?);
    }
    let (store, prefix) = open()?;
    // A tenant left in the layout before 0.4.0 has its records elsewhere:
    // refused, as `actor move` does, rather than reported as not placed.
    let mut checked = std::collections::BTreeSet::new();
    for ((tenant, _), _, _) in &targets {
        if checked.insert(tenant.clone()) {
            crate::check_layout(&args.bucket, &store, &prefix, tenant)?;
        }
    }
    let keys = ObjectStoreBucket::new(store, prefix);
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let horizon_ms = wasmkeep_server::heartbeat_horizon_ms(
        wasmkeep_server::DEFAULT_HEARTBEAT,
        &wasmkeep_server::LeaseConfig::default_safe(),
    );
    let now = crate::status::now_ms();
    let nodes: Vec<NodeInfo> = rt
        .block_on(membership::all_heartbeats(&keys))
        .map_err(|e| anyhow::anyhow!("listing the heartbeats: {e}"))?;
    let apps: std::collections::BTreeSet<String> = targets
        .iter()
        .map(|((tenant, app), _, _)| format!("{tenant}/{app}"))
        .collect();
    let mut warnings = Vec::new();
    for app in this.map(|_| apps).unwrap_or_default() {
        let read = rt.block_on(wasmkeep_cluster::upgrade::status_of(
            &keys, &app, &nodes, now, horizon_ms,
        ));
        match (read, this) {
            (Ok(upgrade), Some(this)) => {
                warnings.extend(crate::upgrade_cmd::reader_warning(&upgrade, this));
            }
            (Err(e), _) => warnings.push(format!(
                "reading the upgrade floor of {app}: {e}; its nodes' versions are not checked"
            )),
            (Ok(_), None) => {}
        }
    }
    let mut rows = Vec::new();
    for ((tenant, app), class, name) in targets {
        let root = StateRoot::new("", tenant.as_str(), app.as_str());
        let key = root.key(
            Family::Owners,
            &format!("{class}/{}", wasmkeep_storage::encode_segment(&name)),
        );
        let body = rt
            .block_on(keys.get_body(&key))
            .map_err(|e| anyhow::anyhow!("reading {key}: {e}"))?;
        let holder = body
            .as_deref()
            .and_then(OwnerRecord::decode)
            .and_then(|r| r.holder)
            .map(|(node, _)| node);
        let since_ms = match holder {
            None => None,
            Some(_) => rt
                // Listed by its folder: a listing of a key itself shows only
                // what is under it.
                .block_on(
                    keys.list_keys(&format!("{}/", key.rsplit_once('/').map_or("", |(f, _)| f))),
                )
                .map_err(|e| anyhow::anyhow!("listing {key}: {e}"))?
                .into_iter()
                .find_map(|(k, at)| (k == key).then_some(at)),
        };
        let live = holder.as_ref().map(|id| {
            nodes
                .iter()
                .any(|n| n.id == *id && membership::is_live(n, now, horizon_ms))
        });
        rows.push(Row {
            actor: format!("{class}/{name}"),
            app: format!("{tenant}/{app}"),
            owner: holder,
            live,
            since_ms,
        });
    }
    Ok(Report { rows, warnings })
}
