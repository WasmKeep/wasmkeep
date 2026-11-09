//! `wasmkeep cluster status`: what the bucket says about a cluster, read
//! without asking any node. Heartbeats (`nodes/<id>`) give the nodes; each
//! app's `apps/<tenant>/<app>/current` gives its deploy. Nothing is written.

use std::fmt::Write as _;
use std::sync::Arc;

use object_store::path::Path as StorePath;
use object_store::{ObjectStore, ObjectStoreExt};
use serde_json::{Value, json};
use wasmkeep_cluster::membership::{self, DrainState, NodeInfo};
use wasmkeep_cluster::node_lease::NodeState;
use wasmkeep_cluster::object_store::ObjectStoreBucket;
use wasmkeep_server::apps::Current;

/// This machine's clock, in ms since the epoch.
pub(crate) fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

/// This machine's clock, for the lease library.
pub(crate) struct Wall;

impl wasmkeep_cluster::lease::Clock for Wall {
    fn now_ms(&self) -> u64 {
        now_ms()
    }
}

/// Whether `wasmkeep cluster forget` may delete a heartbeat object with this
/// body at `now_ms`: only one in the format from before node leases, older
/// than its horizon plus a lease's ttl and grace (the defaults, 25 s), so a
/// retired node's. A node lease is never deleted (records may name it, and
/// a fence is what revokes a node); a recent old one may still run.
pub(crate) fn may_forget(body: &[u8], now_ms: u64) -> Result<(), String> {
    let Some(info) = NodeInfo::decode(body) else {
        return Err("this heartbeat is in a format this version cannot read: left alone".into());
    };
    if info.lease.is_some() {
        return Err(format!(
            "{} has a node lease (ADR 0020), never deleted: ownership records may name it. To revoke the node, `wasmkeep cluster fence {}`",
            info.id, info.id
        ));
    }
    let lease = wasmkeep_cluster::lease::LeaseConfig::default_safe();
    let window = wasmkeep_server::heartbeat_horizon_ms(wasmkeep_server::DEFAULT_HEARTBEAT, &lease)
        .saturating_add(lease.ttl_ms())
        .saturating_add(lease.grace_ms());
    if info.written_at_ms > now_ms || now_ms - info.written_at_ms <= window {
        return Err(format!(
            "{}'s heartbeat is recent (or dated ahead of this clock): its node may still run; stop it and try again once it is {} s old",
            info.id,
            window / 1000
        ));
    }
    Ok(())
}

/// One heartbeat, as seen at the time of reading.
#[derive(Debug)]
pub(crate) struct Node {
    pub(crate) info: NodeInfo,
    /// How long ago it was written, by this machine's clock (0 if the node's
    /// clock is ahead).
    pub(crate) age_ms: u64,
    /// Within the horizon the nodes themselves use.
    pub(crate) live: bool,
    /// How far its `written_at` is ahead of this machine's clock (0 if not).
    pub(crate) ahead_ms: u64,
}

impl Node {
    /// It has a cluster listener and publishes the pin to trust there.
    fn tls(&self) -> bool {
        self.info.cluster_address.is_some() && !self.info.tls_pins.is_empty()
    }
}

/// One app's deploy: `Ok(None)` when it has uploads but no `current` yet.
#[derive(Debug)]
pub(crate) struct App {
    pub(crate) name: String,
    pub(crate) current: Result<Option<Current>, String>,
}

/// Everything `cluster status` prints.
#[derive(Debug)]
pub(crate) struct Report {
    pub(crate) bucket: String,
    pub(crate) read_at_ms: u64,
    pub(crate) horizon_ms: u64,
    pub(crate) nodes: Vec<Node>,
    pub(crate) apps: Vec<App>,
}

/// Reads the cluster under `prefix` of `store`, at `now_ms`, counting a
/// heartbeat live for `horizon_ms`.
pub(crate) async fn read(
    store: Arc<dyn ObjectStore>,
    prefix: &str,
    bucket: &str,
    now_ms: u64,
    horizon_ms: u64,
) -> anyhow::Result<Report> {
    let heartbeats = ObjectStoreBucket::new(Arc::clone(&store), prefix);
    let nodes = membership::all_heartbeats(&heartbeats)
        .await
        .map_err(|e| anyhow::anyhow!("listing the heartbeats in {bucket}: {e}"))?
        .into_iter()
        .map(|info| Node {
            age_ms: now_ms.saturating_sub(info.written_at_ms),
            ahead_ms: info.written_at_ms.saturating_sub(now_ms),
            live: membership::is_live(&info, now_ms, horizon_ms),
            info,
        })
        .collect();
    Ok(Report {
        bucket: bucket.to_owned(),
        read_at_ms: now_ms,
        horizon_ms,
        nodes,
        apps: read_apps(store.as_ref(), prefix, bucket).await?,
    })
}

/// `apps/<tenant>/<app>/current` for every app, listing one level at a time
/// (never the uploaded components).
async fn read_apps(
    store: &dyn ObjectStore,
    prefix: &str,
    bucket: &str,
) -> anyhow::Result<Vec<App>> {
    let root = if prefix.is_empty() {
        StorePath::from("apps")
    } else {
        StorePath::from(format!("{prefix}/apps"))
    };
    let listing_failed =
        |e: object_store::Error| anyhow::anyhow!("listing the apps in {bucket}: {e}");
    let mut apps = Vec::new();
    let tenants = store
        .list_with_delimiter(Some(&root))
        .await
        .map_err(listing_failed)?;
    for tenant in tenants.common_prefixes {
        let names = store
            .list_with_delimiter(Some(&tenant))
            .await
            .map_err(listing_failed)?;
        for app in names.common_prefixes {
            let name = format!(
                "{}/{}",
                tenant.filename().unwrap_or_default(),
                app.filename().unwrap_or_default()
            );
            let current = match store.get(&app.clone().join("current")).await {
                Ok(r) => match r.bytes().await {
                    Ok(body) => Current::parse(&body)
                        .map(Some)
                        .ok_or_else(|| "unreadable current".to_owned()),
                    Err(e) => Err(e.to_string()),
                },
                Err(object_store::Error::NotFound { .. }) => Ok(None),
                Err(e) => Err(e.to_string()),
            };
            apps.push(App { name, current });
        }
    }
    apps.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(apps)
}

impl Report {
    fn live(&self) -> impl Iterator<Item = &Node> {
        self.nodes.iter().filter(|n| n.live)
    }

    /// Whether any node is live: `cluster status` fails otherwise.
    pub(crate) fn any_live(&self) -> bool {
        self.live().next().is_some()
    }

    /// The WasmKeep versions live nodes run, each with its nodes.
    fn versions(&self) -> Vec<(&str, Vec<&str>)> {
        let mut out: Vec<(&str, Vec<&str>)> = Vec::new();
        for n in self.live() {
            match out.iter_mut().find(|(v, _)| *v == n.info.version) {
                Some((_, ids)) => ids.push(&n.info.id),
                None => out.push((&n.info.version, vec![&n.info.id])),
            }
        }
        out.sort();
        out
    }

    /// Live nodes with and without a cluster listener.
    fn tls_split(&self) -> (Vec<&str>, Vec<&str>) {
        let (tls, plain): (Vec<&Node>, Vec<&Node>) = self.live().partition(|n| n.tls());
        fn ids(v: Vec<&Node>) -> Vec<&str> {
            v.into_iter().map(|n| n.info.id.as_str()).collect()
        }
        (ids(tls), ids(plain))
    }

    /// Nodes whose heartbeat is in a format from before node leases and
    /// recent by the start guard's test (its horizon plus a lease's ttl and
    /// grace, the defaults: one read cannot know a cluster's own timings),
    /// or dated ahead of this clock (one read cannot tell how long it has
    /// been): an old version may still run there (ADR 0020).
    fn old_format_running(&self) -> Vec<&str> {
        let lease = wasmkeep_cluster::lease::LeaseConfig::default_safe();
        let window = self
            .horizon_ms
            .saturating_add(lease.ttl_ms())
            .saturating_add(lease.grace_ms());
        self.nodes
            .iter()
            .filter(|n| n.info.lease.is_none() && n.age_ms < window)
            .map(|n| n.info.id.as_str())
            .collect()
    }

    /// What an operator should look at, one line each.
    pub(crate) fn warnings(&self) -> Vec<String> {
        let mut out = Vec::new();
        if !self.any_live() {
            out.push(if self.nodes.is_empty() {
                "no node has a heartbeat under this prefix".to_owned()
            } else {
                "no node is live: every heartbeat is older than the horizon".to_owned()
            });
        }
        let (tls, plain) = self.tls_split();
        if !tls.is_empty() && !plain.is_empty() {
            out.push(format!(
                "mixed plain and TLS nodes: {} still plain while {} use the cluster listener; \
                 give every node a cluster listener, then drop --cluster-plaintext accept (ADR 0011)",
                plain.join(", "),
                tls.join(", ")
            ));
        }
        let stuck: Vec<&str> = self
            .live()
            .filter(|n| n.info.drain == Some(DrainState::Stuck))
            .map(|n| n.info.id.as_str())
            .collect();
        if !stuck.is_empty() {
            out.push(format!(
                "draining but stuck: {} still hold actors and have no node to hand them to; \
                 start or undrain a node that serves the same app (ADR 0014)",
                stuck.join(", ")
            ));
        }
        let old = self.old_format_running();
        if !old.is_empty() {
            out.push(format!(
                "an older WasmKeep version runs: {} still write heartbeats in the format from \
                 before node leases; stop them, new nodes refuse to run alongside (ADR 0020)",
                old.join(", ")
            ));
        }
        // Beyond the skew nodes allow each other (the default timings).
        let skew = wasmkeep_server::LeaseConfig::default_safe().max_skew_ms();
        let ahead: Vec<String> = self
            .live()
            .filter(|n| n.info.lease.is_some() && n.ahead_ms > skew)
            .map(|n| format!("{} ({})", n.info.id, rough(n.ahead_ms)))
            .collect();
        if !ahead.is_empty() {
            out.push(format!(
                "node leases dated ahead of this clock: {}. They never go stale, so no node \
                 fences them and their actors stay held until that date; fix the clock that is \
                 wrong (theirs, or this machine's if it is behind). `wasmkeep cluster fence` \
                 stops such a node, but its fence keeps the date: its actors free 15 s after it \
                 (ADR 0020)",
                ahead.join(", ")
            ));
        }
        // Fenced or released with the date still ahead: not live, and its
        // actors still held until that date plus ttl and grace.
        let lease = wasmkeep_server::LeaseConfig::default_safe();
        let hold = lease.ttl_ms().saturating_add(lease.grace_ms());
        let held: Vec<String> = self
            .nodes
            .iter()
            .filter(|n| {
                matches!(
                    n.info.lease,
                    Some(
                        wasmkeep_cluster::node_lease::NodeState::Fenced
                            | wasmkeep_cluster::node_lease::NodeState::Released
                    )
                ) && n.ahead_ms > skew
            })
            .map(|n| {
                format!(
                    "{} (free in {})",
                    n.info.id,
                    rough(n.ahead_ms.saturating_add(hold))
                )
            })
            .collect();
        if !held.is_empty() {
            out.push(format!(
                "fenced or released nodes dated ahead of this clock: {}. Their actors stay held \
                 until that date plus 15 s; fix the clock that is wrong",
                held.join(", ")
            ));
        }
        let versions = self.versions();
        if versions.len() > 1 {
            let list: Vec<String> = versions
                .iter()
                .map(|(v, ids)| format!("{v} on {}", ids.join(", ")))
                .collect();
            out.push(format!(
                "live nodes run different WasmKeep versions ({}): upgrade every node together",
                list.join("; ")
            ));
        }
        out
    }

    pub(crate) fn to_json(&self) -> Value {
        let (tls, plain) = self.tls_split();
        let nodes: Vec<Value> = self
            .nodes
            .iter()
            .map(|n| {
                json!({
                    "id": n.info.id,
                    "ahead_ms": n.ahead_ms,
                    "live": n.live,
                    "age_ms": n.age_ms,
                    "written_at_ms": n.info.written_at_ms,
                    "version": n.info.version,
                    "app": n.info.app,
                    "address": n.info.address,
                    "cluster_address": n.info.cluster_address,
                    "tls_pins": n.info.tls_pins.iter().map(membership::pin_hex).collect::<Vec<_>>(),
                    "tls": n.tls(),
                    "state": n.info.drain.map(DrainState::as_str),
                    // ADR 0020: live, released or fenced; null for a
                    // heartbeat in an older format.
                    "lease": n.info.lease.map(NodeState::as_str),
                })
            })
            .collect();
        let apps: Vec<Value> = self
            .apps
            .iter()
            .map(|a| match &a.current {
                Ok(Some(c)) => json!({"app": a.name, "version": c.version, "seq": c.seq}),
                Ok(None) => json!({"app": a.name, "version": null, "seq": null}),
                Err(e) => json!({"app": a.name, "version": null, "seq": null, "error": e}),
            })
            .collect();
        let versions: Vec<&str> = self.versions().into_iter().map(|(v, _)| v).collect();
        json!({
            "bucket": self.bucket,
            "read_at_ms": self.read_at_ms,
            "horizon_ms": self.horizon_ms,
            "nodes": nodes,
            "apps": apps,
            "summary": {
                "nodes": self.nodes.len(),
                "live": self.live().count(),
                "live_tls": tls.len(),
                "live_plain": plain.len(),
                "mixed_tls": !tls.is_empty() && !plain.is_empty(),
                "versions": versions,
                "mixed_versions": versions.len() > 1,
            },
            "warnings": self.warnings(),
        })
    }

    pub(crate) fn to_text(&self) -> String {
        let mut out = String::new();
        let _ = writeln!(
            out,
            "Cluster {} (live: heartbeat within {})\n",
            self.bucket,
            age(self.horizon_ms)
        );
        if self.nodes.is_empty() {
            out.push_str("No nodes.\n");
        } else {
            let rows: Vec<[String; 9]> = self
                .nodes
                .iter()
                .map(|n| {
                    [
                        n.info.id.clone(),
                        match (n.live, n.info.lease) {
                            // A heartbeat from before node leases (ADR 0020).
                            (true, None) => "yes (old format)".to_owned(),
                            (false, None) => "no (retired, old format)".to_owned(),
                            (true, _) => "yes".to_owned(),
                            // Not live whatever its age (ADR 0020).
                            (false, Some(s @ (NodeState::Fenced | NodeState::Released))) => {
                                format!("no ({})", s.as_str())
                            }
                            (false, Some(NodeState::Live)) => "no".to_owned(),
                        },
                        age(n.age_ms),
                        n.info.drain.map_or("-", DrainState::as_str).to_owned(),
                        n.info.version.clone(),
                        or_dash(&n.info.app),
                        n.info.address.clone(),
                        or_dash(n.info.cluster_address.as_deref().unwrap_or_default()),
                        match (n.info.cluster_address.is_some(), n.info.tls_pins.len()) {
                            (false, 0) => "plain".to_owned(),
                            // An address whose pins line was malformed.
                            (true, 0) => "no pin".to_owned(),
                            (true, 1) => "pinned".to_owned(),
                            (true, k) => format!("pinned ({k})"),
                            (false, _) => "no address".to_owned(),
                        },
                    ]
                })
                .collect();
            table(
                &mut out,
                [
                    "NODE",
                    "LIVE",
                    "AGE",
                    "STATE",
                    "VERSION",
                    "APP",
                    "ADDRESS",
                    "CLUSTER ADDRESS",
                    "TLS",
                ],
                &rows,
            );
        }
        out.push('\n');
        if self.apps.is_empty() {
            out.push_str("No deployed apps.\n");
        } else {
            let rows: Vec<[String; 3]> = self
                .apps
                .iter()
                .map(|a| match &a.current {
                    Ok(Some(c)) => [a.name.clone(), c.version.clone(), c.seq.to_string()],
                    Ok(None) => [a.name.clone(), "(none)".into(), "-".into()],
                    Err(e) => [a.name.clone(), format!("({e})"), "-".into()],
                })
                .collect();
            table(&mut out, ["APP", "DEPLOYED VERSION", "SEQ"], &rows);
        }
        let (tls, plain) = self.tls_split();
        let versions: Vec<&str> = self.versions().into_iter().map(|(v, _)| v).collect();
        let _ = writeln!(
            out,
            "\n{} of {} nodes live; TLS: {} with a cluster listener, {} plain; version{}: {}",
            self.live().count(),
            self.nodes.len(),
            tls.len(),
            plain.len(),
            if versions.len() == 1 { "" } else { "s" },
            if versions.is_empty() {
                "-".to_owned()
            } else {
                versions.join(", ")
            }
        );
        for w in self.warnings() {
            let _ = writeln!(out, "warning: {w}");
        }
        out
    }
}

fn or_dash(s: &str) -> String {
    if s.is_empty() {
        "-".into()
    } else {
        s.to_owned()
    }
}

/// `850ms`, `4.2s`, `12m`, `3h`, `5d`.
fn age(ms: u64) -> String {
    match ms {
        0..1_000 => format!("{ms}ms"),
        1_000..60_000 => format!("{:.1}s", ms as f64 / 1_000.0),
        60_000..3_600_000 => format!("{}m", ms / 60_000),
        3_600_000..172_800_000 => format!("{}h", ms / 3_600_000),
        _ => format!("{}d", ms / 86_400_000),
    }
}

/// Left-aligned columns, two spaces apart.
fn table<const N: usize>(out: &mut String, header: [&str; N], rows: &[[String; N]]) {
    let mut width = header.map(str::len);
    for row in rows {
        for (w, cell) in width.iter_mut().zip(row) {
            *w = (*w).max(cell.chars().count());
        }
    }
    let mut line = |cells: [&str; N]| {
        let mut s = String::new();
        for (i, (cell, w)) in cells.iter().zip(width).enumerate() {
            if i + 1 == N {
                s.push_str(cell);
            } else {
                let _ = write!(s, "{cell:<w$}  ");
            }
        }
        out.push_str(s.trim_end());
        out.push('\n');
    };
    line(header);
    for row in rows {
        line(std::array::from_fn(|i| row[i].as_str()));
    }
}

/// A duration for a warning: whole hours, minutes or seconds.
fn rough(ms: u64) -> String {
    match ms {
        3_600_000.. => format!("{} h", ms / 3_600_000),
        60_000.. => format!("{} min", ms / 60_000),
        _ => format!("{} s", ms / 1_000),
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)] // test code

    use std::sync::atomic::{AtomicU64, Ordering};

    use object_store::PutPayload;
    use object_store::memory::InMemory;
    use wasmkeep_cluster::lease::Clock;
    use wasmkeep_cluster::membership::Membership;

    use super::*;

    const HORIZON: u64 = 10_000;
    const NOW: u64 = 1_000_000;

    struct At(Arc<AtomicU64>);
    impl Clock for At {
        fn now_ms(&self) -> u64 {
            self.0.load(Ordering::Relaxed)
        }
    }

    fn block_on<F: std::future::Future>(f: F) -> F::Output {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(f)
    }

    /// Writes node `id`'s heartbeat as of `at_ms`, with a cluster listener
    /// when `tls`.
    async fn beat(store: &Arc<dyn ObjectStore>, id: &str, at_ms: u64, tls: bool, app: &str) {
        let bucket = ObjectStoreBucket::new(Arc::clone(store), "demo");
        let m = Membership::new(
            bucket,
            At(Arc::new(AtomicU64::new(at_ms))),
            id,
            format!("http://{id}:8080"),
            9_000,
            1_000,
        )
        .unwrap()
        .with_app(app);
        let m = if tls {
            m.with_cluster_tls(format!("https://{id}:7443"), vec![[0xab; 32]])
        } else {
            m
        };
        // As a node writes it: its node lease, format 3 (ADR 0020).
        let body = wasmkeep_cluster::node_lease::NodeLeaseObject {
            state: NodeState::Live,
            instance: Some(format!("i-{id}")),
            written_at_ms: at_ms,
        }
        .encode_with(&m.lease_payload());
        store
            .put(
                &StorePath::from(format!("demo/nodes/{id}")),
                PutPayload::from(body),
            )
            .await
            .unwrap();
    }

    async fn put(store: &Arc<dyn ObjectStore>, key: &str, body: &str) {
        store
            .put(&StorePath::from(key), PutPayload::from(body.to_owned()))
            .await
            .unwrap();
    }

    fn report(store: Arc<dyn ObjectStore>) -> Report {
        block_on(read(store, "demo", "s3://b/demo", NOW, HORIZON)).unwrap()
    }

    /// A live node lease dated ahead of this clock by more than the allowed
    /// skew never goes stale: flagged, with how far ahead, in the text and
    /// the JSON. One within the skew is not.
    #[test]
    fn a_node_lease_dated_ahead_is_flagged() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        block_on(async {
            beat(&store, "a", NOW, false, "acme/shop").await;
            beat(&store, "b", NOW + 600, false, "acme/shop").await;
            beat(&store, "c", NOW + 3_600_000, false, "acme/shop").await;
            // d fenced, its kept date an hour ahead: not live, actors held
            // (its heartbeat with the state a fence writes).
            beat(&store, "d", NOW + 3_600_000, false, "acme/shop").await;
            let key = StorePath::from("demo/nodes/d");
            let live = store.get(&key).await.unwrap().bytes().await.unwrap();
            let fenced =
                String::from_utf8(live.to_vec())
                    .unwrap()
                    .replacen("\nlive ", "\nfenced ", 1);
            store.put(&key, PutPayload::from(fenced)).await.unwrap();
        });
        let r = report(store);
        let held: Vec<String> = r
            .warnings()
            .into_iter()
            .filter(|w| w.starts_with("fenced or released nodes dated ahead"))
            .collect();
        assert!(
            held.len() == 1 && held[0].contains("d (free in 1 h)"),
            "{held:?}"
        );
        let warnings = r.warnings();
        let ahead: Vec<&String> = warnings
            .iter()
            .filter(|w| w.starts_with("node leases dated ahead"))
            .collect();
        assert_eq!(ahead.len(), 1, "{warnings:?}");
        assert!(
            ahead[0].contains("c (1 h)") && !ahead[0].contains(" b "),
            "{}",
            ahead[0]
        );
        let j = r.to_json();
        let by_id = |id: &str| {
            j["nodes"]
                .as_array()
                .unwrap()
                .iter()
                .find(|n| n["id"] == id)
                .unwrap()["ahead_ms"]
                .clone()
        };
        assert_eq!(by_id("c"), 3_600_000);
        assert_eq!(by_id("a"), 0);
    }

    /// ADR 0020, section 6: a heartbeat in the format from before node
    /// leases is an old version running while recent (flagged), a retired
    /// node's once stale (shown, not flagged).
    #[test]
    fn an_old_format_heartbeat_is_flagged_while_recent() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        block_on(async {
            beat(&store, "a", NOW, false, "acme/shop").await;
            let old =
                |at: u64| format!("wasmkeep-node 2\nb\nhttp://b:8080\n0.0.0\n{at}\nacme/shop\n");
            put(&store, "demo/nodes/b", &old(NOW - 1_000)).await;
            put(
                &store,
                "demo/nodes/c",
                &old(NOW - 60_000).replace("\nb\n", "\nc\n"),
            )
            .await;
        });
        let r = report(store);
        let text = r.to_text();
        assert!(text.contains("yes (old format)"), "{text}");
        assert!(text.contains("no (retired, old format)"), "{text}");
        let warnings = r.warnings();
        assert_eq!(warnings.len(), 1, "{warnings:?}");
        assert!(
            warnings[0].contains("an older WasmKeep version runs: b "),
            "{warnings:?}"
        );
    }

    #[test]
    fn only_a_stale_old_format_heartbeat_may_be_forgotten() {
        let old = |at: u64| format!("wasmkeep-node 2\nb\nhttp://b\n0.0.0\n{at}\nt/\n");
        assert_eq!(may_forget(old(NOW - 60_000).as_bytes(), NOW), Ok(()));
        let recent = may_forget(old(NOW - 1_000).as_bytes(), NOW).unwrap_err();
        assert!(recent.contains("may still run"), "{recent}");
        assert!(may_forget(old(NOW + 5_000).as_bytes(), NOW).is_err());
        let lease = format!(
            "wasmkeep-node 3\nfenced beef\n{}\nb\nhttp://b\n0.0.0\nt/\n",
            NOW - 600_000
        );
        let why = may_forget(lease.as_bytes(), NOW).unwrap_err();
        assert!(why.contains("never deleted"), "{why}");
        assert!(may_forget(b"junk", NOW).is_err());
    }

    #[test]
    fn a_fenced_or_released_node_lease_is_shown_as_such() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        block_on(async {
            for (id, state) in [("a", "live"), ("b", "fenced"), ("c", "released")] {
                let body = format!(
                    "wasmkeep-node 3\n{state} beef\n{NOW}\n{id}\nhttp://{id}\n1\nacme/shop\n\n\nserving\n\ntake\n"
                );
                put(&store, &format!("demo/nodes/{id}"), &body).await;
            }
        });
        let r = report(store);
        let text = r.to_text();
        assert!(text.contains("no (fenced)"), "{text}");
        assert!(text.contains("no (released)"), "{text}");
        assert!(text.contains("1 of 3 nodes live"), "{text}");
        let j = r.to_json();
        assert_eq!(j["nodes"][0]["lease"], "live");
        assert_eq!(j["nodes"][1]["lease"], "fenced");
        assert_eq!(j["nodes"][2]["lease"], "released");
    }

    #[test]
    fn a_stale_heartbeat_is_listed_but_not_live() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        block_on(async {
            beat(&store, "a", NOW - 1_200, false, "acme/shop").await;
            // Exactly at the horizon still counts, as on the nodes.
            beat(&store, "b", NOW - HORIZON, false, "acme/shop").await;
            beat(&store, "c", NOW - HORIZON - 1, false, "acme/shop").await;
            // A node whose clock runs ahead of ours.
            beat(&store, "d", NOW + 500, false, "acme/shop").await;
            // Another cluster's prefix is not read.
            put(
                &store,
                "other/nodes/z",
                "wasmkeep-node 2\nz\nhttp://z\n1\n0\n\n",
            )
            .await;
        });
        let r = report(store);
        let rows: Vec<_> = r
            .nodes
            .iter()
            .map(|n| (n.info.id.as_str(), n.live, n.age_ms))
            .collect();
        assert_eq!(
            rows,
            [
                ("a", true, 1_200),
                ("b", true, HORIZON),
                ("c", false, HORIZON + 1),
                ("d", true, 0)
            ]
        );
        assert!(r.any_live());
        assert!(r.warnings().is_empty(), "{:?}", r.warnings());
        let text = r.to_text();
        assert!(text.contains("3 of 4 nodes live"), "{text}");
        assert!(text.contains("10.0s"), "{text}");
        let j = r.to_json();
        assert_eq!(j["summary"]["live"], 3);
        assert_eq!(j["nodes"][2]["id"], "c");
        assert_eq!(j["nodes"][2]["live"], false);
        assert_eq!(j["nodes"][2]["age_ms"], HORIZON + 1);
    }

    #[test]
    fn a_mixed_plain_and_tls_cluster_is_flagged() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        block_on(async {
            beat(&store, "a", NOW, true, "acme/shop").await;
            beat(&store, "b", NOW, false, "acme/shop").await;
            // A stale plain node does not count: it is gone.
            beat(&store, "c", NOW - 60_000, false, "acme/shop").await;
        });
        let r = report(store);
        let w = r.warnings();
        assert_eq!(w.len(), 1, "{w:?}");
        assert!(w[0].contains("b still plain while a use"), "{}", w[0]);
        let j = r.to_json();
        assert_eq!(j["summary"]["mixed_tls"], true);
        assert_eq!(j["summary"]["live_tls"], 1);
        assert_eq!(j["summary"]["live_plain"], 1);
        assert_eq!(j["nodes"][0]["cluster_address"], "https://a:7443");
        assert_eq!(j["nodes"][0]["tls_pins"][0], "ab".repeat(32));
        assert_eq!(j["nodes"][1]["cluster_address"], Value::Null);
        assert_eq!(j["nodes"][1]["tls"], false);
        let text = r.to_text();
        assert!(text.contains("pinned"), "{text}");
        assert!(
            text.contains("warning: mixed plain and TLS nodes"),
            "{text}"
        );

        // Once every live node has a listener, no warning.
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        block_on(async {
            beat(&store, "a", NOW, true, "acme/shop").await;
            beat(&store, "b", NOW, true, "acme/shop").await;
            beat(&store, "c", NOW - 60_000, false, "acme/shop").await;
        });
        assert!(report(store).warnings().is_empty());
    }

    #[test]
    fn drain_states_are_shown_and_a_stuck_node_is_flagged() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        block_on(async {
            beat(&store, "a", NOW, false, "acme/shop").await;
            let base = |id: &str| {
                format!(
                    "wasmkeep-node 3\nlive i-{id}\n{NOW}\n{id}\nhttp://{id}:8080\n{}\nacme/shop\n",
                    env!("CARGO_PKG_VERSION")
                )
            };
            put(&store, "demo/nodes/b", &format!("{}\n\nstuck\n", base("b"))).await;
            put(
                &store,
                "demo/nodes/c",
                &format!("{}\n\ndrained\n", base("c")),
            )
            .await;
            // A node from before ADR 0014: no line 9.
            put(&store, "demo/nodes/d", &base("d")).await;
        });
        let r = report(store);
        let j = r.to_json();
        let states: Vec<&Value> = (0..4).map(|i| &j["nodes"][i]["state"]).collect();
        assert_eq!(
            states,
            [
                &json!("serving"),
                &json!("stuck"),
                &json!("drained"),
                &Value::Null
            ]
        );
        let w = r.warnings();
        assert_eq!(w.len(), 1, "{w:?}");
        assert!(w[0].starts_with("draining but stuck: b "), "{}", w[0]);
        let text = r.to_text();
        assert!(text.contains("STATE"), "{text}");
        assert!(text.contains("stuck"), "{text}");
    }

    #[test]
    fn mixed_versions_are_flagged() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        block_on(async {
            beat(&store, "a", NOW, false, "acme/shop").await;
            // A node of another version (format 3, no app).
            put(
                &store,
                "demo/nodes/b",
                &format!("wasmkeep-node 3\nlive i-b\n{NOW}\nb\nhttp://b:8080\n0.0.1\n\n"),
            )
            .await;
        });
        let r = report(store);
        let w = r.warnings();
        assert_eq!(w.len(), 1, "{w:?}");
        assert!(w[0].contains("upgrade every node together"), "{}", w[0]);
        assert!(w[0].contains("0.0.1 on b"), "{}", w[0]);
        assert_eq!(r.to_json()["summary"]["mixed_versions"], true);
        assert_eq!(r.to_json()["nodes"][1]["app"], "");
    }

    #[test]
    fn deployed_apps_are_read_from_current() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        block_on(async {
            beat(&store, "a", NOW, false, "acme/shop").await;
            put(
                &store,
                "demo/apps/acme/shop/current",
                "0123456789abcdef 4\n",
            )
            .await;
            put(&store, "demo/apps/acme/shop/0123456789abcdef/manifest", "m").await;
            put(
                &store,
                "demo/apps/acme/shop/0123456789abcdef/chat.wasm",
                "w",
            )
            .await;
            put(&store, "demo/apps/acme/admin/current", "garbage").await;
            put(&store, "demo/apps/beta/new/fedcba9876543210/manifest", "m").await;
            // Another prefix's app is not listed.
            put(&store, "other/apps/x/y/current", "v 1").await;
        });
        let r = report(store);
        let j = r.to_json();
        assert_eq!(
            j["apps"],
            json!([
                {"app": "acme/admin", "version": null, "seq": null, "error": "unreadable current"},
                {"app": "acme/shop", "version": "0123456789abcdef", "seq": 4},
                {"app": "beta/new", "version": null, "seq": null},
            ])
        );
        let text = r.to_text();
        assert!(
            text.contains("acme/shop   0123456789abcdef      4"),
            "{text}"
        );
    }

    #[test]
    fn no_live_node_is_an_error() {
        let empty = report(Arc::new(InMemory::new()));
        assert!(!empty.any_live());
        assert!(empty.warnings()[0].contains("no node has a heartbeat"));
        assert!(empty.to_text().contains("No nodes."));

        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        block_on(beat(&store, "a", NOW - HORIZON - 1, false, "acme/shop"));
        let r = report(store);
        assert!(!r.any_live());
        assert!(r.warnings()[0].contains("no node is live"));
        assert_eq!(r.to_json()["summary"]["live"], 0);
    }

    #[test]
    fn a_cluster_address_without_a_valid_pin_is_not_tls() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        block_on(async {
            beat(&store, "a", NOW, true, "acme/shop").await;
            put(
                &store,
                "demo/nodes/b",
                &format!("wasmkeep-node 3\nlive i-b\n{NOW}\nb\nhttp://b:8080\n0.0.0\nacme/shop\nhttps://b:7443\nnot-a-pin\n"),
            )
            .await;
        });
        let r = report(store);
        let j = r.to_json();
        assert_eq!(j["nodes"][1]["cluster_address"], "https://b:7443");
        assert_eq!(j["nodes"][1]["tls"], false);
        assert_eq!(j["summary"]["mixed_tls"], true);
        assert!(r.to_text().contains("no pin"), "{}", r.to_text());
    }

    #[test]
    fn ages_read_at_a_glance() {
        assert_eq!(age(850), "850ms");
        assert_eq!(age(4_200), "4.2s");
        assert_eq!(age(12 * 60_000 + 5), "12m");
        assert_eq!(age(3 * 3_600_000), "3h");
        assert_eq!(age(5 * 86_400_000), "5d");
    }
}
