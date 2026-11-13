//! `wasmkeep cluster migrate`: the one-time, offline move of a bucket's
//! actor state from the layout before 0.4.0 into each app's root
//! ([ADR 0022](../../../docs/adr/0022-app-owned-state-and-the-one-time-migration.md)).
//!
//! Before 0.4.0 every family was keyed `<family>/<tenant>/<tail>`
//! ([`Family::bare`]); from 0.4.0 it is `state/<tenant>/<app>/<family>/<tail>`
//! ([`StateRoot`]). Tails are byte-identical, so a key only changes at its
//! front and no name is ever decoded. Each tail names its class, and each
//! class goes to one app, its *heir* ([`heir_of`]).
//!
//! It runs while every node is stopped, and refuses a bucket where a node
//! may still run. Every step can be run again after a crash:
//!
//! 1. **Plan** ([`Migrator::plan`], all a dry run does: it writes nothing).
//!    Refuses an unresolved heir, a node lease that may still be live,
//!    an object too large for one server-side copy, state already under a
//!    root of a tenant being migrated without this run's lock, or a lock
//!    with another plan.
//! 2. **Lock**: `cluster/migration`, with the plan, each app's `current`
//!    token as found, and the `nodes/` listing the plan judged.
//! 3. **Copy** server-side. History objects are create-only: skipped when
//!    already there. Records, markers and alarms are copied every
//!    time (moves are not copied: see [`COPIED`]); a bare record already
//!    replaced by a tombstone is not (its copy
//!    was made and verified before the tombstone was written).
//! 4. **Verify**: list the source again until it is unchanged; compare
//!    each mutable object byte for byte; check that a restore plan on the
//!    root is the source's; delete keys of the root the source does not
//!    have.
//! 5. **Barriers**: token `3` in every app's `current` (older binaries
//!    cannot read it), then each bare ownership record replaced by a
//!    tombstone naming its new key, conditional on the version whose bytes
//!    were compared. A conflict means an old node took the actor: abort.
//! 6. **Final proof**: list `nodes/` again; any change aborts, nothing
//!    committed. A node acknowledges a write only while its node lease is
//!    valid (ADR 0020), and holding it means rewriting `nodes/<id>`.
//! 7. **Commit** `cluster/migrated/<tenant>`, then unlock.
//!
//! [`Migrator::rollback`] undoes it while no node of any version has run
//! since (the `nodes/` listing is the one recorded). [`Migrator::remove_old`]
//! later deletes the old layout of each committed tenant but its
//! tombstones, once the rollback is over (or given up), and makes it
//! refuse for good. [`start_check`] is what a node or a command of this
//! version checks before any write.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::future::Future;
use std::pin::Pin;
use std::task::Poll;

use crate::lease::{Bucket, BucketError, Clock, Version};
use crate::membership::NodeInfo;
use crate::node_lease::{NodeLeaseObject, NodeState};
use crate::replication::{Listing, plan_restore};
use crate::state_root::{Family, STATE_PREFIX, StateRoot};

/// The lock a run holds, relative to the bucket prefix. Reserved since
/// 0.3.0, whose binaries refuse a bucket that has it.
pub const LOCK_KEY: &str = "cluster/migration";
/// A tenant's commit, `cluster/migrated/<tenant>`: from it on, the roots
/// are the truth for the tenant. Reserved since 0.3.0 too.
pub const COMMIT_PREFIX: &str = "cluster/migrated/";
/// A tenant's removal mark, `cluster/removed/<tenant>`, created by
/// `--remove-old` before its first delete: from it on, `--rollback`
/// refuses for good (the old layout is no longer whole).
pub const REMOVED_PREFIX: &str = "cluster/removed/";
/// The third token of an app's `current` once its state is rooted.
pub const ROOTED_TOKEN: &str = "3";
/// The first line of a tombstone over a bare ownership record. Older
/// binaries read only `wasmkeep-owner 1`, so they take the actor as
/// corrupt and serve it nowhere.
pub const TOMBSTONE_HEADER: &str = "wasmkeep-owner moved";
/// The largest object one server-side copy takes (S3's limit).
pub const MAX_COPY_BYTES: u64 = 5 * 1024 * 1024 * 1024;
/// Node leases and heartbeats.
const NODES: &str = "nodes/";
/// Deploys.
const APPS: &str = "apps/";
/// The families a run copies. Moves (`control/move/`) are not: they are
/// transient intents with a 5-minute lifetime measured from their
/// last-modified time, which a copy would renew; they stay in the old
/// layout, where `--remove-old` deletes them once expired.
pub const COPIED: [Family; 4] = [
    Family::History,
    Family::Owners,
    Family::Leases,
    Family::Alarms,
];
/// The third line of a tombstone written where no ownership record
/// existed (an actor from before records, ADR 0020 §6): a rollback deletes
/// it rather than putting a record back.
const UNOWNED: &str = "none";
/// The longest key S3 takes, in bytes.
pub const MAX_KEY_BYTES: usize = 1024;
/// Rounds of copy and verify while the source keeps changing.
const MAX_ROUNDS: usize = 4;
/// Attempts at a `current` whose conditional write keeps conflicting.
const TOKEN_ATTEMPTS: usize = 8;

/// How `--remove-old` decides.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RemoveOptions {
    /// Delete even while `--rollback` still works (no node has started
    /// since the migration): the operator gives the rollback up.
    pub forgo_rollback: bool,
    /// Old-layout history younger than this (`--history-retention-secs`)
    /// is kept for a later run, as collection keeps it (ADR 0012).
    pub history_retention_ms: u64,
}

/// What `--remove-old` deletes (or would) for one migrated tenant.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TenantRemoval {
    /// The old-layout keys to delete.
    pub delete: Vec<String>,
    /// History objects kept: younger than the retention window.
    pub young_history: usize,
    /// Move requests kept: not expired yet.
    pub live_moves: usize,
    /// Tombstones over the old ownership records, kept for good.
    pub tombstones: usize,
    /// Whether `--rollback` would still work (no node ran since).
    pub rollback_open: bool,
}

/// What `--remove-old` does (or would), and what refuses it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Removal {
    /// Every migrated tenant with old-layout keys left.
    pub tenants: BTreeMap<String, TenantRemoval>,
    /// Tenants with old-layout keys and no commit: never touched.
    pub skipped: Vec<String>,
    /// The reasons to refuse: nothing is deleted while there is one.
    pub refusals: Vec<String>,
}

impl Removal {
    /// Keys to delete, in all.
    pub fn deletes(&self) -> usize {
        self.tenants.values().map(|t| t.delete.len()).sum()
    }
}

/// One listed object.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ObjectMeta {
    /// Its key relative to the bucket prefix, as written (not as the
    /// store escapes it).
    pub key: String,
    /// Its size in bytes.
    pub size: u64,
    /// When it was last written, by the bucket's clock (ms).
    pub modified_ms: u64,
    /// Its entity tag, if the store gives one.
    pub etag: Option<String>,
}

/// What the migration needs from a bucket, beyond conditional writes.
#[allow(async_fn_in_trait)]
pub trait Store: Bucket {
    /// Every object under `prefix`, in key order.
    async fn list(&self, prefix: &str) -> Result<Vec<ObjectMeta>, BucketError>;
    /// The names of the folders directly under `prefix` (which ends in
    /// `/`), as written.
    async fn list_dirs(&self, prefix: &str) -> Result<Vec<String>, BucketError>;
    /// Copies `from` to `to` inside the bucket, overwriting it.
    async fn copy(&self, from: &str, to: &str) -> Result<(), BucketError>;
    /// Writes `key`, whatever is there.
    async fn put(&self, key: &str, body: Vec<u8>) -> Result<(), BucketError>;
    /// Deletes `key` (absent is fine).
    async fn delete(&self, key: &str) -> Result<(), BucketError>;
}

/// Waiting, for a run: [`Clock`] for the time of day, and a pause that a
/// change of the wall clock cannot shorten.
#[allow(async_fn_in_trait)]
pub trait Pause: Clock {
    /// Waits at least `ms` milliseconds of elapsed (monotonic) time.
    async fn pause_ms(&self, ms: u64);
}

/// An app as its deploys describe it: the classes of its current
/// version. Apps without a `current` are left out: they deploy nothing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppInfo {
    /// Its tenant.
    pub tenant: String,
    /// Its name within the tenant.
    pub app: String,
    /// The classes its current manifest deploys, as named there.
    pub classes: BTreeSet<String>,
}

/// How a run decides.
#[derive(Debug, Clone)]
pub struct Options {
    /// `--heir TENANT/APP`: the app of each tenant that inherits the
    /// classes no single app deploys.
    pub heirs: BTreeMap<String, String>,
    /// Requests in flight at once.
    pub concurrency: usize,
    /// How long after its last `live` write a node lease (ADR 0020) can no
    /// longer be valid, on any clock: its ttl, grace and the tolerated
    /// skew.
    pub lease_window_ms: u64,
    /// The same for a heartbeat in a format from before node leases: its
    /// horizon, plus a lease's ttl and grace.
    pub old_window_ms: u64,
    /// Bytes of the bucket prefix in front of every key (a destination key
    /// longer than [`MAX_KEY_BYTES`] with it is refused).
    pub prefix_bytes: usize,
}

/// Why a run stopped.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum MigrateError {
    /// Refused before writing anything: each reason.
    #[error("refused:\n  {}", .0.join("\n  "))]
    Refused(Vec<String>),
    /// Stopped after writing, with nothing committed that was not before:
    /// run it again, or roll back.
    #[error("aborted: {0}")]
    Aborted(String),
    /// A bucket request failed, naming what it was for.
    #[error("bucket: {what}: {error}")]
    Bucket {
        /// The request.
        what: String,
        /// Its error.
        error: BucketError,
    },
}

fn bucket(what: impl Into<String>) -> impl FnOnce(BucketError) -> MigrateError {
    let what = what.into();
    move |error| MigrateError::Bucket { what, error }
}

/// A `nodes/` object as the plan judged it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeSeen {
    /// The object.
    pub meta: ObjectMeta,
    /// Whether it may still be live (a reason to refuse), and why.
    pub live: Option<String>,
    /// Its body as a read of it returned (`None`: gone since the
    /// listing). The final proof reads it again and compares: every
    /// renewal rewrites its time, and reads are consistent where listings
    /// may lag.
    pub body: Option<Vec<u8>>,
    /// A heartbeat in a format from before node leases.
    pub old: bool,
}

/// One tenant's part of a plan.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TenantPlan {
    /// The heir of each class found in its bare keys.
    pub heirs: BTreeMap<String, String>,
    /// Objects per family.
    pub counts: BTreeMap<Family, usize>,
    /// Its apps (from their deploys).
    pub apps: BTreeSet<String>,
    /// Committed already: nothing is done for it.
    pub committed: bool,
    /// Classes two or more apps deploy: the apps other than the heir start
    /// them empty. `class: app, app`.
    pub shared: BTreeMap<String, Vec<String>>,
    /// Apps whose `current` has no token 3 yet (written by an older
    /// version).
    pub unrooted: BTreeSet<String>,
    /// Named by the lock of an unfinished run: its original tokens are
    /// there, and its commit still to write.
    pub locked: bool,
}

impl TenantPlan {
    /// Whether a run has something to do for it: not committed, and actor
    /// state in the old layout, a `current` an older version wrote, or an
    /// unfinished run's lock naming it. A tenant only this version ever
    /// wrote has nothing to migrate.
    pub fn pending(&self) -> bool {
        !self.committed && (!self.counts.is_empty() || !self.unrooted.is_empty() || self.locked)
    }
}

/// What a run would do, and what refuses it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Plan {
    /// Every tenant with bare keys or apps.
    pub tenants: BTreeMap<String, TenantPlan>,
    /// The `nodes/` listing.
    pub nodes: Vec<NodeSeen>,
    /// The reasons to refuse.
    pub refusals: Vec<String>,
    /// A run's lock is there.
    pub locked: Option<LockMode>,
    /// The fingerprint of what decides the heirs.
    pub hash: String,
    /// Average time of one listing request, ms (for the estimate).
    pub request_ms: u64,
}

impl Plan {
    /// The tenants a run would still migrate.
    pub fn pending(&self) -> Vec<&str> {
        self.tenants
            .iter()
            .filter(|(_, t)| t.pending())
            .map(|(name, _)| name.as_str())
            .collect()
    }

    /// Objects to copy, at most.
    pub fn objects(&self) -> usize {
        self.tenants
            .values()
            .filter(|t| !t.committed)
            .flat_map(|t| t.counts.values())
            .sum()
    }

    /// Roughly how long a run takes, ms: the copies `concurrency` at a
    /// time, the second listing and the tombstones.
    pub fn estimate_ms(&self, concurrency: usize) -> u64 {
        let objects = u64::try_from(self.objects()).unwrap_or(u64::MAX);
        let owners: usize = self
            .tenants
            .values()
            .filter(|t| !t.committed)
            .filter_map(|t| t.counts.get(&Family::Owners))
            .sum();
        let owners = u64::try_from(owners).unwrap_or(u64::MAX);
        let rounds = objects
            .saturating_mul(2)
            .saturating_add(owners.saturating_mul(3))
            / u64::try_from(concurrency.max(1)).unwrap_or(1);
        rounds
            .saturating_add(1)
            .saturating_mul(self.request_ms.max(20))
    }
}

/// What a lock is held for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockMode {
    /// `--apply`.
    Apply,
    /// `--rollback`.
    Rollback,
}

/// The body of `cluster/migration`, or of a commit (whose `tenants` is
/// its one tenant).
#[derive(Debug, Clone, PartialEq, Eq)]
struct Record {
    mode: LockMode,
    hash: String,
    tenants: BTreeSet<String>,
    /// `tenant/app` → its token as found (`-`: none; `absent`: no
    /// `current`).
    originals: BTreeMap<String, String>,
    nodes: Vec<ObjectMeta>,
    /// `tenant/class` → its heir.
    heirs: BTreeMap<String, String>,
    /// Commits only: objects per family.
    counts: BTreeMap<String, usize>,
    /// Commits only: `tenant/app` → the `version seq` of its `current` as
    /// committed. A rollback refuses once a deploy changed one.
    deployed: BTreeMap<String, String>,
    at_ms: u64,
}

const LOCK_HEADER: &str = "wasmkeep-migration 1";
const COMMIT_HEADER: &str = "wasmkeep-migrated 1";

impl Record {
    fn encode(&self, header: &str) -> Vec<u8> {
        let mut out = format!("{header}\n");
        let mode = match self.mode {
            LockMode::Apply => "apply",
            LockMode::Rollback => "rollback",
        };
        let _ = writeln!(out, "mode {mode}");
        let _ = writeln!(out, "plan {}", self.hash);
        let _ = writeln!(out, "at {}", self.at_ms);
        for t in &self.tenants {
            let _ = writeln!(out, "tenant {t}");
        }
        for (app, token) in &self.originals {
            let _ = writeln!(out, "current {token} {app}");
        }
        for (class, heir) in &self.heirs {
            let _ = writeln!(out, "heir {class} {heir}");
        }
        for (family, n) in &self.counts {
            let _ = writeln!(out, "count {family} {n}");
        }
        for (app, current) in &self.deployed {
            let _ = writeln!(out, "deployed {app} {current}");
        }
        for n in &self.nodes {
            let _ = writeln!(
                out,
                "node {} {} {} {}",
                n.etag.as_deref().unwrap_or("-"),
                n.size,
                n.modified_ms,
                n.key
            );
        }
        out.into_bytes()
    }

    fn decode(body: &[u8], header: &str) -> Option<Self> {
        let text = std::str::from_utf8(body).ok()?;
        let mut lines = text.lines();
        if lines.next()? != header {
            return None;
        }
        let mut record = Self {
            mode: LockMode::Apply,
            hash: String::new(),
            tenants: BTreeSet::new(),
            originals: BTreeMap::new(),
            nodes: Vec::new(),
            heirs: BTreeMap::new(),
            counts: BTreeMap::new(),
            deployed: BTreeMap::new(),
            at_ms: 0,
        };
        for line in lines {
            let (word, rest) = line.split_once(' ')?;
            match word {
                "mode" => {
                    record.mode = match rest {
                        "apply" => LockMode::Apply,
                        "rollback" => LockMode::Rollback,
                        _ => return None,
                    }
                }
                "plan" => rest.clone_into(&mut record.hash),
                "at" => record.at_ms = rest.parse().ok()?,
                "tenant" => {
                    record.tenants.insert(rest.to_owned());
                }
                "current" => {
                    let (token, app) = rest.split_once(' ')?;
                    record.originals.insert(app.to_owned(), token.to_owned());
                }
                "heir" => {
                    let (class, heir) = rest.split_once(' ')?;
                    record.heirs.insert(class.to_owned(), heir.to_owned());
                }
                "count" => {
                    let (family, n) = rest.split_once(' ')?;
                    record.counts.insert(family.to_owned(), n.parse().ok()?);
                }
                "deployed" => {
                    let (app, current) = rest.split_once(' ')?;
                    record.deployed.insert(app.to_owned(), current.to_owned());
                }
                "node" => {
                    let mut parts = rest.splitn(4, ' ');
                    let (etag, size, modified, key) =
                        (parts.next()?, parts.next()?, parts.next()?, parts.next()?);
                    record.nodes.push(ObjectMeta {
                        key: key.to_owned(),
                        size: size.parse().ok()?,
                        modified_ms: modified.parse().ok()?,
                        etag: (etag != "-").then(|| etag.to_owned()),
                    });
                }
                _ => return None,
            }
        }
        Some(record)
    }
}

/// The body of a tombstone over a bare record whose actor now lives at
/// `moved_to`.
pub fn tombstone(moved_to: &str) -> Vec<u8> {
    format!("{TOMBSTONE_HEADER}\n{moved_to}\n").into_bytes()
}

/// The body of a tombstone where no ownership record existed: its actor
/// (history or an old-lease marker, no record) now lives at `moved_to`,
/// and an older binary must not create a record for it.
pub fn tombstone_unowned(moved_to: &str) -> Vec<u8> {
    format!("{TOMBSTONE_HEADER}\n{moved_to}\n{UNOWNED}\n").into_bytes()
}

/// Whether `body` is a tombstone written where no record existed.
fn is_unowned(body: &[u8]) -> bool {
    tombstone_target(body).is_some()
        && std::str::from_utf8(body).is_ok_and(|t| t.lines().nth(2) == Some(UNOWNED))
}

/// Where a tombstone says its actor went; `None` if `body` is not one.
pub fn tombstone_target(body: &[u8]) -> Option<&str> {
    let text = std::str::from_utf8(body).ok()?;
    let mut lines = text.lines();
    (lines.next()? == TOMBSTONE_HEADER).then_some(())?;
    lines.next().filter(|t| !t.is_empty())
}

/// The class a bare key's tail (after `<family>/<tenant>/`) names; `None`
/// if it names none this layout knows.
pub fn class_of(family: Family, tail: &str) -> Option<&str> {
    let parts: Vec<&str> = tail.split('/').collect();
    let class = match family {
        // `<at>/<class>.t/<hex>/<tag>` or the legacy `<at>/<class>/<hex>`.
        Family::Alarms => match parts.as_slice() {
            [_, tagged, _, _] => tagged.strip_suffix(".t")?,
            [_, class, _] => class,
            _ => return None,
        },
        _ => parts.first().copied()?,
    };
    (!class.is_empty()).then_some(class)
}

/// The heir of class `class` of `tenant`: the one app whose current
/// version deploys it; else, with no app deploying it, the tenant's heir
/// (`--heir`, else its one app, else `default` for tenant `default` with no
/// app); a class two apps deploy needs `--heir`. Tenant `default` with an
/// app other than `default` needs `--heir` for a class no app deploys: it
/// may be an `--actor` node's (served from `default/default`) as well as a
/// class the app removed. `Err` says what to give.
pub fn heir_of(
    tenant: &str,
    class: Option<&str>,
    apps: &[&AppInfo],
    heirs: &BTreeMap<String, String>,
) -> Result<String, String> {
    let deploying: Vec<&str> = class.map_or_else(Vec::new, |c| {
        apps.iter()
            .filter(|a| a.classes.contains(c))
            .map(|a| a.app.as_str())
            .collect()
    });
    let given = heirs.get(tenant);
    match (deploying.as_slice(), given) {
        ([one], _) => Ok((*one).to_owned()),
        ([], Some(heir)) | ([_, _, ..], Some(heir)) => Ok(heir.clone()),
        ([], None) => match apps {
            [] if tenant == "default" => Ok("default".to_owned()),
            [one] if tenant == "default" && one.app == "default" => Ok("default".to_owned()),
            [_, ..] if tenant == "default" => Err(format!(
                "class {} of tenant default is in no app's current version: it may be an \
                 `--actor` node's (served from default/default) or a class an app removed; \
                 give --heir default/default or --heir default/APP",
                class.unwrap_or("(unreadable keys)")
            )),
            [one] => Ok(one.app.clone()),
            [] => Err(format!(
                "tenant {tenant} has actor state and no app: give --heir {tenant}/APP"
            )),
            _ => Err(format!(
                "class {} of tenant {tenant} is in no app's current version and the tenant \
                 has several apps ({}): give --heir {tenant}/APP",
                class.unwrap_or("(unreadable keys)"),
                apps.iter()
                    .map(|a| a.app.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            )),
        },
        (several, None) => Err(format!(
            "class {} of tenant {tenant} is deployed by {}: give --heir {tenant}/APP (the \
             other apps start that class empty)",
            class.unwrap_or_default(),
            several.join(" and ")
        )),
    }
}

/// Why a node or command of this version must not start on a bucket.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum StartRefusal {
    /// A run of `cluster migrate` holds the lock.
    #[error(
        "a migration of this bucket prefix is in progress ({LOCK_KEY} exists): let it finish, \
         or run `wasmkeep cluster migrate --apply` (or `--rollback`) again"
    )]
    InProgress,
    /// The tenant has actor state in the layout before 0.4.0 and no commit.
    #[error(
        "tenant {0} has actor state in the layout of a version before 0.4.0 ({1} exists) and \
         this bucket prefix was not migrated: stop every node, then run `wasmkeep cluster \
         migrate --bucket <this bucket>` (a dry run) and `--apply` \
         (docs/deploy/upgrading-to-0.4.md)"
    )]
    Unmigrated(String, String),
    /// The bucket could not be read.
    #[error("reading the bucket: {0}")]
    Bucket(BucketError),
}

/// What a node or command of this version checks for `tenant` before it
/// writes anything to the bucket: no lock, and either a commit or no bare
/// key (moves aside: they are transient, and an old `actor move` may still
/// write one). `Ok(true)` when the tenant was migrated (its commit exists).
pub async fn start_check<S: Store>(store: &S, tenant: &str) -> Result<bool, StartRefusal> {
    if store
        .get(LOCK_KEY)
        .await
        .map_err(StartRefusal::Bucket)?
        .is_some()
    {
        return Err(StartRefusal::InProgress);
    }
    if store
        .get(&format!("{COMMIT_PREFIX}{tenant}"))
        .await
        .map_err(StartRefusal::Bucket)?
        .is_some()
    {
        return Ok(true);
    }
    for family in COPIED {
        let base = format!("{}/{tenant}/", family.bare());
        let dirs = store.list_dirs(&base).await.map_err(StartRefusal::Bucket)?;
        if let Some(first) = dirs.first() {
            return Err(StartRefusal::Unmigrated(
                tenant.to_owned(),
                format!("{base}{first}/"),
            ));
        }
    }
    Ok(false)
}

/// Polls `futures` together and gives their outputs in order.
async fn join_all<F: Future>(futures: Vec<F>) -> Vec<F::Output> {
    let mut futures: Vec<Pin<Box<F>>> = futures.into_iter().map(Box::pin).collect();
    let mut out: Vec<Option<F::Output>> = futures.iter().map(|_| None).collect();
    std::future::poll_fn(|cx| {
        let mut pending = false;
        for (f, slot) in futures.iter_mut().zip(out.iter_mut()) {
            if slot.is_none() {
                match f.as_mut().poll(cx) {
                    Poll::Ready(v) => *slot = Some(v),
                    Poll::Pending => pending = true,
                }
            }
        }
        if pending {
            Poll::Pending
        } else {
            Poll::Ready(())
        }
    })
    .await;
    out.into_iter().flatten().collect()
}

/// Runs `op` on every item, `n` at a time; the first error stops it.
async fn each<'i, T, F, Fut>(items: &'i [T], n: usize, op: F) -> Result<(), MigrateError>
where
    F: Fn(&'i T) -> Fut,
    Fut: Future<Output = Result<(), MigrateError>>,
{
    for chunk in items.chunks(n.max(1)) {
        for r in join_all(chunk.iter().map(&op).collect()).await {
            r?;
        }
    }
    Ok(())
}

/// One bare object and where it goes.
#[derive(Debug, Clone)]
struct Move {
    family: Family,
    from: ObjectMeta,
    to: String,
}

/// One tenant's source, as listed.
#[derive(Debug, Clone, Default)]
struct Source {
    moves: Vec<Move>,
}

impl Source {
    /// What a second listing compares: key, entity tag and size.
    fn fingerprint(&self) -> BTreeSet<(String, Option<String>, u64)> {
        self.moves
            .iter()
            .map(|m| (m.from.key.clone(), m.from.etag.clone(), m.from.size))
            .collect()
    }
}

/// How a run ended.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Outcome {
    /// Tenants committed by this run.
    pub committed: Vec<String>,
    /// Tenants committed before.
    pub already: Vec<String>,
    /// Tenants not committed, each with why.
    pub failed: Vec<(String, String)>,
}

impl Outcome {
    /// 0: done, or nothing to do; 2: some tenants committed and others
    /// not.
    pub fn exit_code(&self) -> i32 {
        if self.failed.is_empty() { 0 } else { 2 }
    }
}

/// The lock as this run wrote it, and the version its last write returned.
/// Every write that another run's lock would make wrong rewrites it first,
/// conditional on that version ([`Migrator::hold`]): a run whose lock was
/// taken over stops before its next such write.
struct Held {
    body: Vec<u8>,
    version: std::sync::Mutex<Version>,
}

/// A commit, its record, version and last-modified time (bucket clock).
type Commit = (Record, Version, u64);

/// A migration of one bucket prefix.
#[allow(missing_debug_implementations)] // holds the store and clock by reference
pub struct Migrator<'a, S, C> {
    store: &'a S,
    clock: &'a C,
    apps: Vec<AppInfo>,
    options: Options,
}

impl<'a, S: Store, C: Pause> Migrator<'a, S, C> {
    /// A run over `store` (relative to the bucket prefix), judging time by
    /// `clock`, with `apps` as their deploys describe them.
    pub fn new(store: &'a S, clock: &'a C, apps: Vec<AppInfo>, options: Options) -> Self {
        Self {
            store,
            clock,
            apps,
            options,
        }
    }

    fn apps_of(&self, tenant: &str) -> Vec<&AppInfo> {
        self.apps.iter().filter(|a| a.tenant == tenant).collect()
    }

    /// The fingerprint of what decides the heirs: the apps and their
    /// classes, and `--heir`.
    fn hash(&self) -> String {
        let mut text = String::new();
        let mut apps: Vec<&AppInfo> = self.apps.iter().collect();
        apps.sort_by(|a, b| (&a.tenant, &a.app).cmp(&(&b.tenant, &b.app)));
        for a in apps {
            let classes: Vec<&str> = a.classes.iter().map(String::as_str).collect();
            let _ = writeln!(text, "app {}/{} {}", a.tenant, a.app, classes.join(","));
        }
        for (t, a) in &self.options.heirs {
            let _ = writeln!(text, "heir {t}/{a}");
        }
        // FNV-1a, 64 bits: a fingerprint, not a defence.
        let mut h: u64 = 0xcbf2_9ce4_8422_2325;
        for b in text.bytes() {
            h ^= u64::from(b);
            h = h.wrapping_mul(0x0100_0000_01b3);
        }
        format!("{h:016x}")
    }

    async fn read_record(
        &self,
        key: &str,
        header: &str,
    ) -> Result<Option<(Record, Version)>, MigrateError> {
        match self
            .store
            .get(key)
            .await
            .map_err(bucket(format!("reading {key}")))?
        {
            None => Ok(None),
            Some((body, version)) => Record::decode(&body, header)
                .map(|r| Some((r, version)))
                .ok_or_else(|| {
                    MigrateError::Refused(vec![format!(
                        "{key} is not in a format this version reads: it was written by \
                         another version; use that one"
                    )])
                }),
        }
    }

    /// The commit of every tenant that has one.
    async fn commits(&self) -> Result<BTreeMap<String, Commit>, MigrateError> {
        let listed = self
            .store
            .list(COMMIT_PREFIX)
            .await
            .map_err(bucket("listing the commits"))?;
        let mut out = BTreeMap::new();
        for meta in listed {
            let Some(tenant) = meta.key.strip_prefix(COMMIT_PREFIX) else {
                continue;
            };
            if let Some((record, version)) = self.read_record(&meta.key, COMMIT_HEADER).await? {
                out.insert(tenant.to_owned(), (record, version, meta.modified_ms));
            }
        }
        Ok(out)
    }

    /// Rewrites the lock as this run holds it, conditional on the version
    /// its last write returned. A conflict means another run (an `--apply`
    /// or a `--rollback`) took it: stop, writing nothing more.
    async fn hold(&self, held: &Held) -> Result<(), MigrateError> {
        let version = held
            .version
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        match self
            .store
            .replace(LOCK_KEY, held.body.clone(), &version)
            .await
        {
            Ok(v) => {
                *held
                    .version
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) = v;
                Ok(())
            }
            Err(BucketError::Conflict) => Err(MigrateError::Aborted(format!(
                "{LOCK_KEY} was taken by another run of migrate (an --apply or a --rollback): \
                 this one stopped; let that one finish"
            ))),
            Err(e) => Err(MigrateError::Bucket {
                what: "rewriting the lock".into(),
                error: e,
            }),
        }
    }

    /// Writes the lock: created, or over `existing` (a run to resume).
    async fn take_lock(
        &self,
        body: Vec<u8>,
        existing: Option<&Version>,
    ) -> Result<Held, MigrateError> {
        let version = match existing {
            None => self.store.create(LOCK_KEY, body.clone()).await,
            Some(version) => self.store.replace(LOCK_KEY, body.clone(), version).await,
        }
        .map_err(|e| match e {
            BucketError::Conflict => MigrateError::Aborted(format!(
                "{LOCK_KEY} changed under this run: another run of migrate is active"
            )),
            e => MigrateError::Bucket {
                what: "writing the lock".into(),
                error: e,
            },
        })?;
        Ok(Held {
            body,
            version: std::sync::Mutex::new(version),
        })
    }

    /// Removes the lock this run holds: rewritten first (so a run that
    /// lost it removes nothing), then deleted.
    async fn release(&self, held: &Held) -> Result<(), MigrateError> {
        self.hold(held).await?;
        self.store
            .delete(LOCK_KEY)
            .await
            .map_err(bucket("removing the lock"))
    }

    /// Whether the object at `meta` may still be a live node's, and why.
    fn judge(&self, meta: &ObjectMeta, body: &[u8], now: u64) -> Option<String> {
        let id = meta.key.strip_prefix(NODES).unwrap_or(&meta.key);
        let past = |written: u64, window: u64| now > written.saturating_add(window);
        if let Some(o) = NodeLeaseObject::decode(body) {
            return match o.state {
                // A clean stop: that process acknowledges nothing more.
                NodeState::Released => None,
                // A fenced or crashed process acts until its lease runs out
                // on its own clock.
                NodeState::Fenced | NodeState::Live
                    if past(o.written_at_ms, self.options.lease_window_ms) =>
                {
                    None
                }
                NodeState::Fenced => Some(format!(
                    "node {id} was fenced less than its lease ago: wait {} s",
                    self.options.lease_window_ms / 1000
                )),
                NodeState::Live => Some(format!(
                    "node {id} may be live (its node lease was written {} s ago): stop the \
                     service units, not just the processes, and run this again",
                    now.saturating_sub(o.written_at_ms) / 1000
                )),
            };
        }
        match NodeInfo::decode(body) {
            Some(i) if past(i.written_at_ms, self.options.old_window_ms) => None,
            Some(_) => Some(format!(
                "node {id} has a recent heartbeat of a version before node leases: stop it \
                 and run this again"
            )),
            None => Some(format!(
                "{} is not a heartbeat this version reads: a node of another version may run; \
                 stop it, or remove the object if it is not a node's",
                meta.key
            )),
        }
    }

    /// The `nodes/` listing, each object read and judged.
    async fn nodes(&self) -> Result<Vec<NodeSeen>, MigrateError> {
        let listed = self
            .store
            .list(NODES)
            .await
            .map_err(bucket("listing nodes/"))?;
        let now = self.clock.now_ms();
        let mut out = Vec::with_capacity(listed.len());
        for meta in listed {
            let found = self
                .store
                .get(&meta.key)
                .await
                .map_err(bucket(format!("reading {}", meta.key)))?;
            let (live, old) = match &found {
                // Deleted since the listing: a forgotten node's.
                None => (None, false),
                Some((body, _)) => (
                    self.judge(&meta, body, now),
                    NodeLeaseObject::decode(body).is_none() && NodeInfo::decode(body).is_some(),
                ),
            };
            out.push(NodeSeen {
                meta,
                live,
                body: found.map(|(b, _)| b),
                old,
            });
        }
        Ok(out)
    }

    /// The final proof: `nodes/` as the plan judged it. The listing
    /// catches a node id never seen; a read of each object catches a
    /// rewrite a lagging listing would still hide. `Some(node)` names the
    /// first that changed.
    async fn nodes_changed(&self, plan: &[NodeSeen]) -> Result<Option<String>, MigrateError> {
        let listed = self
            .store
            .list(NODES)
            .await
            .map_err(bucket("listing nodes/"))?;
        let recorded: Vec<ObjectMeta> = plan.iter().map(|n| n.meta.clone()).collect();
        if let Some(changed) = changed_nodes(&recorded, &listed) {
            return Ok(Some(changed));
        }
        for n in plan {
            let now = self
                .store
                .get(&n.meta.key)
                .await
                .map_err(bucket(format!("reading {}", n.meta.key)))?
                .map(|(b, _)| b);
            if now != n.body {
                return Ok(Some(format!(
                    "node {}",
                    n.meta.key.strip_prefix(NODES).unwrap_or(&n.meta.key)
                )));
            }
        }
        Ok(None)
    }

    /// Every tenant with bare keys (from delimiter listings).
    async fn bare_tenants(&self) -> Result<BTreeSet<String>, MigrateError> {
        let mut tenants = BTreeSet::new();
        for family in COPIED {
            let base = format!("{}/", family.bare());
            tenants.extend(
                self.store
                    .list_dirs(&base)
                    .await
                    .map_err(bucket(format!("listing {base}")))?,
            );
        }
        Ok(tenants)
    }

    /// Tenant `tenant`'s bare keys and where each goes, or why one cannot
    /// go anywhere.
    async fn source(&self, tenant: &str) -> Result<(Source, Vec<String>), MigrateError> {
        let apps = self.apps_of(tenant);
        let mut source = Source::default();
        let mut refusals = BTreeSet::new();
        for family in COPIED {
            let base = format!("{}/{tenant}/", family.bare());
            let listed = self
                .store
                .list(&base)
                .await
                .map_err(bucket(format!("listing {base}")))?;
            for meta in listed {
                let Some(tail) = meta.key.strip_prefix(&base) else {
                    continue;
                };
                if meta.size > MAX_COPY_BYTES {
                    refusals.insert(format!(
                        "{} is {} bytes, more than one server-side copy takes",
                        meta.key, meta.size
                    ));
                    continue;
                }
                let Some(class) = class_of(family, tail) else {
                    refusals.insert(format!(
                        "{} names no class this version reads: move it out of this prefix \
                         and run this again",
                        meta.key
                    ));
                    continue;
                };
                match heir_of(tenant, Some(class), &apps, &self.options.heirs) {
                    Ok(heir) => {
                        let to = StateRoot::new("", tenant, heir).key(family, tail);
                        if self.options.prefix_bytes + to.len() > MAX_KEY_BYTES {
                            refusals.insert(format!(
                                "{} would become {to}, longer than the {MAX_KEY_BYTES} bytes a \
                                 key can have under this bucket prefix",
                                meta.key
                            ));
                            continue;
                        }
                        source.moves.push(Move {
                            family,
                            from: meta.clone(),
                            to,
                        });
                    }
                    Err(why) => {
                        refusals.insert(why);
                    }
                }
            }
        }
        Ok((source, refusals.into_iter().collect()))
    }

    /// What a run would do, and what refuses it. Reads only.
    pub async fn plan(&self) -> Result<Plan, MigrateError> {
        let started = self.clock.now_ms();
        let mut requests = 0u64;
        let mut plan = Plan {
            hash: self.hash(),
            ..Plan::default()
        };
        for (heir_tenant, heir) in &self.options.heirs {
            if !wasmkeep_name(heir_tenant) || !wasmkeep_name(heir) {
                plan.refusals.push(format!(
                    "--heir {heir_tenant}/{heir}: not a valid TENANT/APP"
                ));
                continue;
            }
            // An heir no node serves would hide the classes it inherits.
            let apps = self.apps_of(heir_tenant);
            let implicit = heir_tenant == "default" && heir == "default";
            if !apps.is_empty() && !implicit && !apps.iter().any(|a| &a.app == heir) {
                plan.refusals.push(format!(
                    "--heir {heir_tenant}/{heir}: tenant {heir_tenant} has no app {heir} (its \
                     apps: {})",
                    apps.iter()
                        .map(|a| a.app.as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                ));
            }
        }
        let lock = self.read_record(LOCK_KEY, LOCK_HEADER).await?;
        requests += 1;
        if let Some((lock, _)) = &lock {
            plan.locked = Some(lock.mode);
            if lock.mode == LockMode::Apply && lock.hash != plan.hash {
                plan.refusals.push(format!(
                    "{LOCK_KEY} holds a run with another plan (apps or --heir changed since): \
                     run it again as it was, or --rollback"
                ));
            }
        }
        let commits = self.commits().await?;
        requests += 1;
        let mut tenants = self.bare_tenants().await?;
        requests += u64::try_from(COPIED.len()).unwrap_or(4);
        tenants.extend(self.apps.iter().map(|a| a.tenant.clone()));
        let tokens = self.tokens().await?;
        requests += u64::try_from(self.apps.len()).unwrap_or(0);
        for tenant in tenants {
            if !wasmkeep_name(&tenant) {
                plan.refusals.push(format!(
                    "{tenant:?} has actor state but is not a tenant name this version takes: \
                     move its keys out of this prefix and run this again"
                ));
                continue;
            }
            let mut t = TenantPlan {
                apps: self
                    .apps_of(&tenant)
                    .iter()
                    .map(|a| a.app.clone())
                    .collect(),
                committed: commits.contains_key(&tenant),
                unrooted: self
                    .apps_of(&tenant)
                    .iter()
                    .filter(|a| {
                        let token = tokens
                            .get(&format!("{}/{}", a.tenant, a.app))
                            .map(String::as_str);
                        !matches!(token, Some(ROOTED_TOKEN | "absent"))
                    })
                    .map(|a| a.app.clone())
                    .collect(),
                locked: lock
                    .as_ref()
                    .is_some_and(|(l, _)| l.mode == LockMode::Apply && l.tenants.contains(&tenant)),
                ..TenantPlan::default()
            };
            if !t.committed {
                let (source, refusals) = self.source(&tenant).await?;
                requests += u64::try_from(COPIED.len()).unwrap_or(4);
                plan.refusals.extend(refusals);
                for m in &source.moves {
                    *t.counts.entry(m.family).or_default() += 1;
                    let tail = m
                        .from
                        .key
                        .strip_prefix(&format!("{}/{tenant}/", m.family.bare()))
                        .unwrap_or_default();
                    if let Some(class) = class_of(m.family, tail) {
                        let heir =
                            m.to.strip_prefix(&format!("{STATE_PREFIX}{tenant}/"))
                                .and_then(|r| r.split('/').next())
                                .unwrap_or_default();
                        t.heirs.insert(class.to_owned(), heir.to_owned());
                    }
                }
                for class in t.heirs.keys() {
                    let deploying: Vec<String> = self
                        .apps_of(&tenant)
                        .iter()
                        .filter(|a| a.classes.contains(class))
                        .map(|a| a.app.clone())
                        .collect();
                    if deploying.len() > 1 {
                        t.shared.insert(class.clone(), deploying);
                    }
                }
                // State under a root of this tenant that this run did not
                // write: a node of this version ran on it, and the copy
                // would overwrite it.
                if lock.is_none() && !source.moves.is_empty() {
                    let roots = self
                        .store
                        .list_dirs(&format!("{STATE_PREFIX}{tenant}/"))
                        .await
                        .map_err(bucket(format!("listing {STATE_PREFIX}{tenant}/")))?;
                    requests += 1;
                    if let Some(app) = roots.first() {
                        plan.refusals.push(format!(
                            "tenant {tenant} has actor state both in the layout before 0.4.0 \
                             and under {STATE_PREFIX}{tenant}/{app}/, with no migration in \
                             progress: a node of this version wrote the latter, and an older \
                             binary or a restored backup the former. Migrating would overwrite \
                             live state: keep one (move the other out of this prefix) and run \
                             this again"
                        ));
                    }
                }
            }
            plan.tenants.insert(tenant, t);
        }
        plan.nodes = self.nodes().await?;
        requests += 1;
        // Live nodes refuse only a run with something to do: on a prefix
        // already migrated (nodes of this version running), there is none.
        if !plan.pending().is_empty() || plan.locked.is_some() {
            for n in &plan.nodes {
                if let Some(why) = &n.live {
                    plan.refusals.push(why.clone());
                }
            }
        }
        plan.request_ms = self.clock.now_ms().saturating_sub(started) / requests.max(1);
        Ok(plan)
    }

    /// Each app's `current` token as found: `-` for none, `absent` with no
    /// `current`.
    async fn tokens(&self) -> Result<BTreeMap<String, String>, MigrateError> {
        let mut out = BTreeMap::new();
        for a in &self.apps {
            let key = format!("{APPS}{}/{}/current", a.tenant, a.app);
            let token = match self
                .store
                .get(&key)
                .await
                .map_err(bucket(format!("reading {key}")))?
            {
                None => "absent".to_owned(),
                Some((body, _)) => third_token(&body).unwrap_or("-").to_owned(),
            };
            out.insert(format!("{}/{}", a.tenant, a.app), token);
        }
        Ok(out)
    }

    /// Every `current` of `tenant` now, listed again (an app deployed
    /// during the run counts): each must carry token 3. Gives each one's
    /// `version seq`.
    async fn rooted_currents(
        &self,
        tenant: &str,
    ) -> Result<BTreeMap<String, String>, MigrateError> {
        let base = format!("{APPS}{tenant}/");
        let mut out = BTreeMap::new();
        for app in self
            .store
            .list_dirs(&base)
            .await
            .map_err(bucket(format!("listing {base}")))?
        {
            let key = format!("{base}{app}/current");
            let Some((body, _)) = self
                .store
                .get(&key)
                .await
                .map_err(bucket(format!("reading {key}")))?
            else {
                continue;
            };
            if third_token(&body) != Some(ROOTED_TOKEN) {
                return Err(MigrateError::Aborted(format!(
                    "{key} was written during the migration by a version before 0.4.0 (no \
                     token 3): freeze deploys and run this again; nothing was committed"
                )));
            }
            out.insert(format!("{tenant}/{app}"), deployed_of(&body));
        }
        Ok(out)
    }

    /// `--apply`: migrates every tenant not committed yet. `Err` only when
    /// nothing new was committed.
    pub async fn apply(&self) -> Result<Outcome, MigrateError> {
        let plan = self.plan().await?;
        if plan.locked == Some(LockMode::Rollback) {
            return Err(MigrateError::Refused(vec![format!(
                "a rollback is in progress ({LOCK_KEY}): run --rollback again to finish it"
            )]));
        }
        if !plan.refusals.is_empty() {
            return Err(MigrateError::Refused(plan.refusals));
        }
        let mut outcome = Outcome {
            already: plan
                .tenants
                .iter()
                .filter(|(_, t)| t.committed)
                .map(|(n, _)| n.clone())
                .collect(),
            ..Outcome::default()
        };
        let mut pending: Vec<String> = plan.pending().iter().map(|t| (*t).to_owned()).collect();
        let existing = self.read_record(LOCK_KEY, LOCK_HEADER).await?;
        if pending.is_empty() {
            if let Some((_, version)) = &existing {
                let lock = self.take_lock(
                    existing
                        .as_ref()
                        .map(|(l, _)| l.encode(LOCK_HEADER))
                        .unwrap_or_default(),
                    Some(version),
                );
                self.release(&lock.await?).await?;
            }
            return Ok(outcome);
        }

        // 2. Lock: the plan, the tokens as first found, and the `nodes/`
        // listing the plan judged.
        let mut originals = self.tokens().await?;
        if let Some((lock, _)) = &existing {
            for (app, token) in &lock.originals {
                originals.insert(app.clone(), token.clone());
            }
        }
        let lock = Record {
            mode: LockMode::Apply,
            hash: plan.hash.clone(),
            tenants: pending.iter().cloned().collect(),
            originals,
            nodes: plan.nodes.iter().map(|n| n.meta.clone()).collect(),
            heirs: pending
                .iter()
                .flat_map(|t| {
                    plan.tenants.get(t).into_iter().flat_map(move |p| {
                        p.heirs
                            .iter()
                            .map(move |(c, a)| (format!("{t}/{c}"), a.clone()))
                    })
                })
                .collect(),
            counts: BTreeMap::new(),
            deployed: BTreeMap::new(),
            at_ms: self.clock.now_ms(),
        };
        let held = self
            .take_lock(lock.encode(LOCK_HEADER), existing.as_ref().map(|(_, v)| v))
            .await?;
        // A tenant another run committed since the plan: its roots may
        // already be a node's live state (it could start once the commit
        // was there and no lock was), never touched again. From here on
        // the lock keeps nodes of this version out.
        let commits = self.commits().await?;
        pending.retain(|t| {
            let done = commits.contains_key(t);
            if done {
                outcome.already.push(t.clone());
            }
            !done
        });

        // The quiet window: a node misjudged dead (clocks apart) either
        // renews its node lease in it, which the final proof sees, or has
        // lost it, and acknowledges nothing after. Every source listing
        // comes after it.
        let window = if plan.nodes.iter().any(|n| n.old) {
            self.options.old_window_ms.max(self.options.lease_window_ms)
        } else {
            self.options.lease_window_ms
        };
        self.clock.pause_ms(window).await;

        // 3, 4. Copy and verify each tenant.
        let mut copied = BTreeMap::new();
        for tenant in &pending {
            // A tenant with apps and no bare key has nothing to copy: its
            // roots may hold this version's live state, never touched.
            let has_state = plan
                .tenants
                .get(tenant)
                .is_some_and(|t| !t.counts.is_empty());
            let done = if has_state {
                self.copy_and_verify(tenant, &held).await?
            } else {
                (Source::default(), BTreeMap::new())
            };
            copied.insert(tenant.clone(), done);
        }

        // 5. Barriers: token 3, then the tombstones.
        self.hold(&held).await?;
        for a in &self.apps {
            self.root_current(&a.tenant, &a.app).await?;
        }
        for (tenant, (source, owners)) in &copied {
            self.tombstones(tenant, source, owners, &held).await?;
        }

        // 6. The final proof: no node ran since the plan judged them, and
        // no deploy of an older version slipped in.
        if let Some(changed) = self.nodes_changed(&plan.nodes).await? {
            return Err(MigrateError::Aborted(format!(
                "{changed} ran during the migration: stop it (the service unit, not just the \
                 process) and run this again; nothing was committed"
            )));
        }
        let mut deployed = BTreeMap::new();
        for tenant in copied.keys() {
            deployed.insert(tenant.clone(), self.rooted_currents(tenant).await?);
        }

        // 7. Commit each tenant, then unlock.
        for (tenant, (source, _)) in &copied {
            let mut heirs = BTreeMap::new();
            let mut counts: BTreeMap<String, usize> = BTreeMap::new();
            for m in &source.moves {
                *counts.entry(m.family.folder().to_owned()).or_default() += 1;
                let tail = m
                    .from
                    .key
                    .strip_prefix(&format!("{}/{tenant}/", m.family.bare()))
                    .unwrap_or_default();
                if let Some(class) = class_of(m.family, tail) {
                    let heir =
                        m.to.strip_prefix(&format!("{STATE_PREFIX}{tenant}/"))
                            .and_then(|r| r.split('/').next())
                            .unwrap_or_default();
                    heirs.insert(format!("{tenant}/{class}"), heir.to_owned());
                }
            }
            let commit = Record {
                mode: LockMode::Apply,
                hash: plan.hash.clone(),
                tenants: BTreeSet::from([tenant.clone()]),
                originals: lock
                    .originals
                    .iter()
                    .filter(|(app, _)| app.split('/').next() == Some(tenant.as_str()))
                    .map(|(a, t)| (a.clone(), t.clone()))
                    .collect(),
                nodes: lock.nodes.clone(),
                heirs,
                counts,
                deployed: deployed.remove(tenant).unwrap_or_default(),
                at_ms: self.clock.now_ms(),
            };
            let key = format!("{COMMIT_PREFIX}{tenant}");
            if let Err(lost) = self.hold(&held).await {
                outcome.failed.push((tenant.clone(), lost.to_string()));
                break;
            }
            match self.store.create(&key, commit.encode(COMMIT_HEADER)).await {
                Ok(_) => outcome.committed.push(tenant.clone()),
                // A re-run after a crash between the write and its answer.
                Err(BucketError::Conflict) => match self.read_record(&key, COMMIT_HEADER).await {
                    Ok(Some((found, _))) if found.hash == plan.hash => {
                        outcome.committed.push(tenant.clone());
                    }
                    Ok(_) => outcome
                        .failed
                        .push((tenant.clone(), format!("{key} holds another plan"))),
                    Err(e) => outcome.failed.push((tenant.clone(), e.to_string())),
                },
                Err(e) => outcome
                    .failed
                    .push((tenant.clone(), format!("writing {key}: {e}"))),
            }
            // A rollback that took the lock between the check and the
            // commit is deleting what the commit names: take it back.
            if let Err(lost) = self.hold(&held).await {
                self.undo_commit(&key, &plan.hash).await?;
                if outcome.committed.last() == Some(tenant) {
                    outcome.committed.pop();
                }
                outcome.failed.push((tenant.clone(), lost.to_string()));
                break;
            }
        }
        if outcome.committed.is_empty() && !outcome.failed.is_empty() {
            return Err(MigrateError::Aborted(
                outcome
                    .failed
                    .iter()
                    .map(|(t, why)| format!("{t}: {why}"))
                    .collect::<Vec<_>>()
                    .join("; "),
            ));
        }
        if outcome.failed.is_empty() {
            self.release(&held).await?;
        }
        // Tenants never reached after the lock was lost.
        for tenant in copied.keys() {
            let seen = outcome.committed.contains(tenant)
                || outcome.failed.iter().any(|(t, _)| t == tenant);
            if !seen {
                outcome.failed.push((
                    tenant.clone(),
                    "not committed: this run lost the lock".into(),
                ));
            }
        }
        Ok(outcome)
    }

    /// After this run lost its lock right after writing commit `key`:
    /// keeps the commit only while a run of the same plan holds the lock
    /// (it commits the same); otherwise (a rollback, or no lock) deletes
    /// it.
    async fn undo_commit(&self, key: &str, hash: &str) -> Result<(), MigrateError> {
        match self.read_record(LOCK_KEY, LOCK_HEADER).await {
            Ok(Some((l, _))) if l.mode == LockMode::Apply && l.hash == hash => Ok(()),
            _ => self
                .store
                .delete(key)
                .await
                .map_err(bucket(format!("deleting {key}"))),
        }
    }

    /// Copies tenant `tenant`'s bare keys into its roots until a second
    /// listing matches, then verifies them. Gives the source and the
    /// version of each bare record whose bytes were compared (none for a
    /// tombstone already in place).
    async fn copy_and_verify(
        &self,
        tenant: &str,
        held: &Held,
    ) -> Result<(Source, BTreeMap<String, (Version, String)>), MigrateError> {
        let mut round = 0;
        let source = loop {
            round += 1;
            let (source, refusals) = self.source(tenant).await?;
            if !refusals.is_empty() {
                return Err(MigrateError::Aborted(refusals.join("; ")));
            }
            self.hold(held).await?;
            self.copy(tenant, &source).await?;
            let (again, _) = self.source(tenant).await?;
            if again.fingerprint() == source.fingerprint() {
                break again;
            }
            if round >= MAX_ROUNDS {
                return Err(MigrateError::Aborted(format!(
                    "tenant {tenant}'s keys kept changing over {MAX_ROUNDS} listings: a node \
                     or a listing is not settled; run this again"
                )));
            }
        };
        let owners = self.verify(tenant, &source, held).await?;
        Ok((source, owners))
    }

    /// Step 3 for one tenant.
    async fn copy(&self, tenant: &str, source: &Source) -> Result<(), MigrateError> {
        let there: BTreeSet<String> = self
            .store
            .list(&format!("{STATE_PREFIX}{tenant}/"))
            .await
            .map_err(bucket(format!("listing {STATE_PREFIX}{tenant}/")))?
            .into_iter()
            .map(|m| m.key)
            .collect();
        each(&source.moves, self.options.concurrency, |m| {
            let there = &there;
            async move {
                match m.family {
                    // Create-only, keyed by transaction and generation.
                    Family::History if there.contains(&m.to) => Ok(()),
                    Family::Owners => {
                        let found = self
                            .store
                            .get(&m.from.key)
                            .await
                            .map_err(bucket(format!("reading {}", m.from.key)))?;
                        match found {
                            None => Ok(()),
                            // Copied and verified before its tombstone.
                            Some((body, _)) if tombstone_target(&body).is_some() => Ok(()),
                            Some((body, _)) => self
                                .store
                                .put(&m.to, body)
                                .await
                                .map_err(bucket(format!("writing {}", m.to))),
                        }
                    }
                    _ => self
                        .store
                        .copy(&m.from.key, &m.to)
                        .await
                        .map_err(bucket(format!("copying {} to {}", m.from.key, m.to))),
                }
            }
        })
        .await
    }

    /// Step 4 for one tenant: mutable objects byte for byte, nothing in
    /// the heirs' roots the source lacks, then history by its restore
    /// plan.
    async fn verify(
        &self,
        tenant: &str,
        source: &Source,
        held: &Held,
    ) -> Result<BTreeMap<String, (Version, String)>, MigrateError> {
        let owners = std::sync::Mutex::new(BTreeMap::new());
        let mutable: Vec<&Move> = source
            .moves
            .iter()
            .filter(|m| m.family != Family::History)
            .collect();
        each(&mutable, self.options.concurrency, |m| {
            let owners = &owners;
            async move {
                let from = self
                    .store
                    .get(&m.from.key)
                    .await
                    .map_err(bucket(format!("reading {}", m.from.key)))?;
                let to = self
                    .store
                    .get(&m.to)
                    .await
                    .map_err(bucket(format!("reading {}", m.to)))?;
                match (from, to) {
                    (Some((body, _)), Some(_))
                        if m.family == Family::Owners
                            && tombstone_target(&body) == Some(m.to.as_str()) =>
                    {
                        Ok(())
                    }
                    // Written where no record existed: nothing to copy.
                    (Some((body, _)), None)
                        if m.family == Family::Owners
                            && is_unowned(&body)
                            && tombstone_target(&body) == Some(m.to.as_str()) =>
                    {
                        Ok(())
                    }
                    (Some((a, version)), Some((b, _))) if a == b => {
                        if m.family == Family::Owners {
                            owners
                                .lock()
                                .unwrap_or_else(std::sync::PoisonError::into_inner)
                                .insert(m.from.key.clone(), (version, m.to.clone()));
                        }
                        Ok(())
                    }
                    _ => Err(MigrateError::Aborted(format!(
                        "{} and its copy {} differ (or one is gone): run this again",
                        m.from.key, m.to
                    ))),
                }
            }
        })
        .await?;

        // Keys of the copied families in the heirs' roots that no source
        // key maps to (alarms the source deleted since an earlier run,
        // stale copies) go first, so the history check sees the root as
        // it is committed. Other apps' roots are never touched.
        let roots: BTreeSet<String> = source
            .moves
            .iter()
            .filter_map(|m| {
                let rest = m.to.strip_prefix(&format!("{STATE_PREFIX}{tenant}/"))?;
                let (app, _) = rest.split_once('/')?;
                Some(format!("{STATE_PREFIX}{tenant}/{app}/"))
            })
            .collect();
        let wanted: BTreeSet<&str> = source.moves.iter().map(|m| m.to.as_str()).collect();
        let mut there = Vec::new();
        for root in &roots {
            there.extend(
                self.store
                    .list(root)
                    .await
                    .map_err(bucket(format!("listing {root}")))?
                    .into_iter()
                    .filter(|m| {
                        let rest = m.key.strip_prefix(root.as_str()).unwrap_or_default();
                        COPIED
                            .iter()
                            .any(|f| rest.starts_with(&format!("{}/", f.folder())))
                    }),
            );
        }
        let (kept, extra): (Vec<ObjectMeta>, Vec<ObjectMeta>) = there
            .into_iter()
            .partition(|m| wanted.contains(m.key.as_str()));
        if !extra.is_empty() {
            self.hold(held).await?;
        }
        each(&extra, self.options.concurrency, |meta| async move {
            self.store
                .delete(&meta.key)
                .await
                .map_err(bucket(format!("deleting {}", meta.key)))
        })
        .await?;

        // History: per actor, the same restore plan on both sides.
        let mut sides: BTreeMap<String, (Vec<String>, Vec<String>)> = BTreeMap::new();
        let actor_of = |key: &str| -> Option<(String, String)> {
            // `<root>actors/<class>/<name>/` and the rest.
            let rest = key.strip_prefix(&format!("{STATE_PREFIX}{tenant}/"))?;
            let (app, rest) = rest.split_once('/')?;
            let rest = rest.strip_prefix("actors/")?;
            let mut parts = rest.splitn(3, '/');
            let (class, name, tail) = (parts.next()?, parts.next()?, parts.next()?);
            Some((format!("{app}/{class}/{name}"), tail.to_owned()))
        };
        for m in source.moves.iter().filter(|m| m.family == Family::History) {
            if let Some((actor, tail)) = actor_of(&m.to) {
                sides.entry(actor).or_default().0.push(tail);
            }
        }
        for meta in &kept {
            if let Some((actor, tail)) = actor_of(&meta.key) {
                sides.entry(actor).or_default().1.push(tail);
            }
        }
        for (actor, (from, to)) in &sides {
            let plan = |keys: &[String]| {
                plan_restore(&Listing::from_keys(keys.iter().map(String::as_str)))
                    .map_err(|e| e.to_string())
            };
            if plan(from) != plan(to) {
                return Err(MigrateError::Aborted(format!(
                    "the history of {tenant}/{actor} does not restore to the same head under \
                     its root: run this again"
                )));
            }
        }
        Ok(owners
            .into_inner()
            .unwrap_or_else(std::sync::PoisonError::into_inner))
    }

    /// Writes token 3 into `tenant/app`'s `current`, retried on conflict
    /// (a deploy from an older tool), and checks it.
    async fn root_current(&self, tenant: &str, app: &str) -> Result<(), MigrateError> {
        let key = format!("{APPS}{tenant}/{app}/current");
        for _ in 0..TOKEN_ATTEMPTS {
            let Some((body, version)) = self
                .store
                .get(&key)
                .await
                .map_err(bucket(format!("reading {key}")))?
            else {
                return Ok(());
            };
            let Some(rooted) = with_token(&body, ROOTED_TOKEN) else {
                return Err(MigrateError::Aborted(format!(
                    "{key} is not in a format this version reads"
                )));
            };
            if rooted == body {
                return Ok(());
            }
            match self.store.replace(&key, rooted, &version).await {
                Ok(_) => return Ok(()),
                Err(BucketError::Conflict) => {}
                Err(e) => {
                    return Err(MigrateError::Bucket {
                        what: format!("writing {key}"),
                        error: e,
                    });
                }
            }
        }
        Err(MigrateError::Aborted(format!(
            "{key} kept changing: freeze deploys during the migration and run this again"
        )))
    }

    /// The tombstones of one tenant: over each record on the version
    /// compared, and created where an actor has history or an old-lease
    /// marker but no record (an older binary would create one, at a
    /// generation of its own). The lock is rewritten before each batch.
    async fn tombstones(
        &self,
        tenant: &str,
        source: &Source,
        owners: &BTreeMap<String, (Version, String)>,
        held: &Held,
    ) -> Result<(), MigrateError> {
        let owners: Vec<(&String, &(Version, String))> = owners.iter().collect();
        for chunk in owners.chunks(self.options.concurrency.max(1)) {
            self.hold(held).await?;
            each(
                chunk,
                self.options.concurrency,
                |(key, (version, to))| async move {
                    match self.store.replace(key, tombstone(to), version).await {
                        Ok(_) => Ok(()),
                        Err(BucketError::Conflict) => Err(MigrateError::Aborted(format!(
                            "{key} changed after it was copied: a node of an older version \
                             took the actor of tenant {tenant}; stop it and run this again (it \
                             is copied again)"
                        ))),
                        Err(e) => Err(MigrateError::Bucket {
                            what: format!("writing the tombstone over {key}"),
                            error: e,
                        }),
                    }
                },
            )
            .await?;
        }
        let unowned = unowned_actors(tenant, source);
        for chunk in unowned.chunks(self.options.concurrency.max(1)) {
            self.hold(held).await?;
            each(chunk, self.options.concurrency, |(key, to)| async move {
                match self.store.create(key, tombstone_unowned(to)).await {
                    Ok(_) => Ok(()),
                    // Ours, from an answer this run did not get.
                    Err(BucketError::Conflict)
                        if self
                            .store
                            .get(key)
                            .await
                            .map_err(bucket(format!("reading {key}")))?
                            .is_some_and(|(b, _)| is_unowned(&b)) =>
                    {
                        Ok(())
                    }
                    Err(BucketError::Conflict) => Err(MigrateError::Aborted(format!(
                        "{key} was created after the copy: a node of an older version took the \
                         actor of tenant {tenant}; stop it and run this again (it is copied \
                         again)"
                    ))),
                    Err(e) => Err(MigrateError::Bucket {
                        what: format!("writing the tombstone at {key}"),
                        error: e,
                    }),
                }
            })
            .await?;
        }
        Ok(())
    }

    /// `--rollback`: puts every migrated tenant back in the layout before
    /// 0.4.0, while no node has run and no app was deployed since its
    /// migration. A rollback already started is finished whatever ran
    /// since: from its first write, the old layout is the truth again.
    pub async fn rollback(&self) -> Result<Vec<String>, MigrateError> {
        let lock = self.read_record(LOCK_KEY, LOCK_HEADER).await?;
        let commits = self.commits().await?;
        let recorded = match (&lock, commits.values().next()) {
            (Some((l, _)), _) => l.nodes.clone(),
            (None, Some((c, _, _))) => c.nodes.clone(),
            (None, None) => return Ok(Vec::new()),
        };
        let resuming = lock
            .as_ref()
            .is_some_and(|(l, _)| l.mode == LockMode::Rollback);
        if !resuming && let Some(why) = self.ran_since(&recorded, &commits).await? {
            return Err(MigrateError::Refused(vec![why]));
        }
        let mut tenants: BTreeSet<String> = commits.keys().cloned().collect();
        let mut originals = BTreeMap::new();
        for (c, _, _) in commits.values() {
            originals.extend(c.originals.clone());
        }
        if let Some((l, _)) = &lock {
            tenants.extend(l.tenants.iter().cloned());
            for (app, token) in &l.originals {
                originals
                    .entry(app.clone())
                    .or_insert_with(|| token.clone());
            }
        }
        let record_heirs = lock
            .as_ref()
            .map(|(l, _)| l.heirs.clone())
            .unwrap_or_default();
        let record = Record {
            mode: LockMode::Rollback,
            hash: lock
                .as_ref()
                .map(|(l, _)| l.hash.clone())
                .unwrap_or_default(),
            tenants: tenants.clone(),
            originals: originals.clone(),
            nodes: recorded.clone(),
            heirs: record_heirs.clone(),
            counts: BTreeMap::new(),
            deployed: BTreeMap::new(),
            at_ms: self.clock.now_ms(),
        };
        let held = self
            .take_lock(record.encode(LOCK_HEADER), lock.as_ref().map(|(_, v)| v))
            .await?;
        // Checked again under the lock: a node that started between the
        // check and the lock. The lock goes back as it was.
        if !resuming && let Some(why) = self.ran_since(&recorded, &commits).await? {
            let version = held
                .version
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone();
            match &lock {
                Some((l, _)) => {
                    self.store
                        .replace(LOCK_KEY, l.encode(LOCK_HEADER), &version)
                        .await
                        .map_err(bucket("putting the lock back"))?;
                }
                None => self
                    .store
                    .delete(LOCK_KEY)
                    .await
                    .map_err(bucket("removing the lock"))?,
            }
            return Err(MigrateError::Refused(vec![why]));
        }

        // Every record back before any root key goes; twice, so a
        // tombstone an `--apply` that lost the lock wrote meanwhile is
        // put back too.
        for _ in 0..2 {
            self.hold(&held).await?;
            for tenant in &tenants {
                self.records_back(tenant).await?;
            }
        }
        self.hold(&held).await?;
        for (app, token) in &originals {
            let key = format!("{APPS}{app}/current");
            let Some((body, version)) = self
                .store
                .get(&key)
                .await
                .map_err(bucket(format!("reading {key}")))?
            else {
                continue;
            };
            if third_token(&body) != Some(ROOTED_TOKEN)
                || token == ROOTED_TOKEN
                || token == "absent"
            {
                continue;
            }
            let back = if token == "-" {
                without_token(&body)
            } else {
                with_token(&body, token)
            };
            let Some(back) = back else { continue };
            self.store
                .replace(&key, back, &version)
                .await
                .map_err(bucket(format!("putting back {key}")))?;
        }
        // Then the copies: only the keys a bare key maps to (by the heirs
        // recorded), never anything else under the roots.
        let mut heirs = record_heirs.clone();
        for (c, _, _) in commits.values() {
            heirs.extend(c.heirs.clone());
        }
        for tenant in &tenants {
            let mut copies = Vec::new();
            for family in COPIED {
                let base = format!("{}/{tenant}/", family.bare());
                for meta in self
                    .store
                    .list(&base)
                    .await
                    .map_err(bucket(format!("listing {base}")))?
                {
                    let Some(tail) = meta.key.strip_prefix(&base) else {
                        continue;
                    };
                    let Some(class) = class_of(family, tail) else {
                        continue;
                    };
                    if let Some(heir) = heirs.get(&format!("{tenant}/{class}")) {
                        copies.push(
                            StateRoot::new("", tenant.as_str(), heir.as_str()).key(family, tail),
                        );
                    }
                }
            }
            self.hold(&held).await?;
            each(&copies, self.options.concurrency, |key| async move {
                self.store
                    .delete(key)
                    .await
                    .map_err(bucket(format!("deleting {key}")))
            })
            .await?;
            let key = format!("{COMMIT_PREFIX}{tenant}");
            self.store
                .delete(&key)
                .await
                .map_err(bucket(format!("deleting {key}")))?;
            // A mark only a `--remove-old` that saw this lock wrote: it
            // stopped before deleting anything.
            let mark = format!("{REMOVED_PREFIX}{tenant}");
            self.store
                .delete(&mark)
                .await
                .map_err(bucket(format!("deleting {mark}")))?;
        }
        self.release(&held).await?;
        Ok(tenants.into_iter().collect())
    }

    /// Why a rollback is now one-way, if it is: a `nodes/` change since
    /// the run (a node of any version ran), a deploy since a commit, or a
    /// key under a migrated root written after its commit.
    async fn ran_since(
        &self,
        recorded: &[ObjectMeta],
        commits: &BTreeMap<String, Commit>,
    ) -> Result<Option<String>, MigrateError> {
        let one_way = |what: String| {
            Some(format!(
                "{what} since the migration: it is now one-way (ADR 0022). The state stays \
                 where this version reads it"
            ))
        };
        let now = self
            .store
            .list(NODES)
            .await
            .map_err(bucket("listing nodes/"))?;
        if let Some(changed) = changed_nodes(recorded, &now) {
            return Ok(one_way(format!("{changed} has run")));
        }
        for (tenant, (c, _, committed_ms)) in commits {
            let mark = format!("{REMOVED_PREFIX}{tenant}");
            if self
                .store
                .get(&mark)
                .await
                .map_err(bucket(format!("reading {mark}")))?
                .is_some()
            {
                return Ok(one_way(format!(
                    "--remove-old started deleting tenant {tenant}'s old layout"
                )));
            }
            if c.nodes != recorded {
                return Ok(one_way(format!(
                    "a node has run (since tenant {tenant}'s commit)"
                )));
            }
            let base = format!("{APPS}{tenant}/");
            for app in self
                .store
                .list_dirs(&base)
                .await
                .map_err(bucket(format!("listing {base}")))?
            {
                let key = format!("{base}{app}/current");
                let now = self
                    .store
                    .get(&key)
                    .await
                    .map_err(bucket(format!("reading {key}")))?
                    .map(|(b, _)| deployed_of(&b));
                if now.as_ref() != c.deployed.get(&format!("{tenant}/{app}")) {
                    return Ok(one_way(format!("{tenant}/{app} was deployed")));
                }
            }
            let root = format!("{STATE_PREFIX}{tenant}/");
            if let Some(late) = self
                .store
                .list(&root)
                .await
                .map_err(bucket(format!("listing {root}")))?
                .into_iter()
                .find(|m| m.modified_ms > *committed_ms)
            {
                return Ok(one_way(format!("{} was written", late.key)));
            }
        }
        Ok(None)
    }

    /// Puts tenant `tenant`'s ownership records back over their
    /// tombstones (each conditional on the tombstone's version), and
    /// removes the tombstones written where no record was.
    async fn records_back(&self, tenant: &str) -> Result<(), MigrateError> {
        let base = format!("{}/{tenant}/", Family::Owners.bare());
        let listed = self
            .store
            .list(&base)
            .await
            .map_err(bucket(format!("listing {base}")))?;
        each(&listed, self.options.concurrency, |meta| async move {
            let Some((body, version)) = self
                .store
                .get(&meta.key)
                .await
                .map_err(bucket(format!("reading {}", meta.key)))?
            else {
                return Ok(());
            };
            let Some(to) = tombstone_target(&body) else {
                return Ok(());
            };
            if is_unowned(&body) {
                return self
                    .store
                    .delete(&meta.key)
                    .await
                    .map_err(bucket(format!("deleting {}", meta.key)));
            }
            let Some((record, _)) = self
                .store
                .get(to)
                .await
                .map_err(bucket(format!("reading {to}")))?
            else {
                return Err(MigrateError::Aborted(format!(
                    "{} names {to}, which is gone: nothing to put back",
                    meta.key
                )));
            };
            self.store
                .replace(&meta.key, record, &version)
                .await
                .map(drop)
                .map_err(bucket(format!("putting back {}", meta.key)))
        })
        .await
    }

    /// `--remove-old` as a dry run: what it would delete, and what refuses
    /// it. Reads only.
    pub async fn removal(&self, options: RemoveOptions) -> Result<Removal, MigrateError> {
        let mut out = Removal::default();
        if let Some((lock, _)) = self.read_record(LOCK_KEY, LOCK_HEADER).await? {
            out.refusals.push(format!(
                "{LOCK_KEY} exists: a run of migrate ({:?}) is unfinished; run it again to the \
                 end first",
                lock.mode
            ));
        }
        let commits = self.commits().await?;
        let mut tenants = self.bare_tenants().await?;
        tenants.extend(
            self.store
                .list_dirs(&format!("{}/", Family::Moves.bare()))
                .await
                .map_err(bucket(format!("listing {}/", Family::Moves.bare())))?,
        );
        let now = self.clock.now_ms();
        for tenant in tenants {
            let Some(commit) = commits.get(&tenant) else {
                out.skipped.push(tenant);
                continue;
            };
            let single = BTreeMap::from([(tenant.clone(), commit.clone())]);
            let (t, refusals) = self
                .tenant_removal(&tenant, commit, &single, now, options)
                .await?;
            out.refusals.extend(refusals);
            out.tenants.insert(tenant, t);
        }
        Ok(out)
    }

    /// [`Self::removal`] for one migrated tenant.
    async fn tenant_removal(
        &self,
        tenant: &str,
        (commit, _, committed_ms): &Commit,
        single: &BTreeMap<String, Commit>,
        now: u64,
        options: RemoveOptions,
    ) -> Result<(TenantRemoval, Vec<String>), MigrateError> {
        let mut t = TenantRemoval {
            rollback_open: self.ran_since(&commit.nodes, single).await?.is_none(),
            ..TenantRemoval::default()
        };
        let mut refusals = Vec::new();
        if t.rollback_open && !options.forgo_rollback {
            refusals.push(format!(
                "tenant {tenant} can still be rolled back (no node has started since its \
                 migration, nothing was deployed): start the nodes of this version and check \
                 the app first, or pass --forgo-rollback to give the rollback up"
            ));
        }
        let listed = |family: Family| {
            let base = format!("{}/{tenant}/", family.bare());
            async move {
                let found = self
                    .store
                    .list(&base)
                    .await
                    .map_err(bucket(format!("listing {base}")))?;
                Ok::<_, MigrateError>((base, found))
            }
        };
        // The tombstones, kept: each names the actor's new key. Anything
        // else there is a record an older binary wrote after the barrier.
        let (base, owners) = listed(Family::Owners).await?;
        // Read `concurrency` at a time: whether each is a tombstone
        // (`None`: gone since the listing).
        let kinds = std::sync::Mutex::new(BTreeMap::new());
        each(&owners, self.options.concurrency, |meta| {
            let kinds = &kinds;
            async move {
                let body = self
                    .store
                    .get(&meta.key)
                    .await
                    .map_err(bucket(format!("reading {}", meta.key)))?;
                let kind = body.map(|(b, _)| tombstone_target(&b).is_some());
                kinds
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .insert(meta.key.clone(), kind);
                Ok(())
            }
        })
        .await?;
        let kinds = kinds
            .into_inner()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut tombstoned = BTreeSet::new();
        for meta in &owners {
            let tail = meta.key.strip_prefix(&base).unwrap_or_default();
            match kinds.get(&meta.key).copied().flatten() {
                None => {}
                Some(true) => {
                    t.tombstones += 1;
                    tombstoned.insert(tail.to_owned());
                }
                Some(false) => refusals.push(format!(
                    "{} is an ownership record, not a tombstone: a binary of a version before \
                     0.4.0 created that actor after the migration, and 0.4.0 does not see it. \
                     Move its keys out of this prefix (or keep them elsewhere) and run this \
                     again",
                    meta.key
                )),
            }
        }
        // History, markers and alarms: each copied by the run that
        // committed (its class has an heir there), none written since.
        for family in [Family::History, Family::Leases, Family::Alarms] {
            let (base, found) = listed(family).await?;
            for meta in found {
                let tail = meta.key.strip_prefix(&base).unwrap_or_default();
                let class = class_of(family, tail);
                if !class.is_some_and(|c| commit.heirs.contains_key(&format!("{tenant}/{c}"))) {
                    refusals.push(format!(
                        "{} names a class the migration of tenant {tenant} did not copy: it \
                         was written since; move it out of this prefix and run this again",
                        meta.key
                    ));
                    continue;
                }
                // Equal times count: a store may keep whole seconds, and
                // nothing of these families was written in the run's last
                // node-lease window.
                if meta.modified_ms >= *committed_ms {
                    refusals.push(format!(
                        "{} was written after tenant {tenant}'s migration: a binary of a \
                         version before 0.4.0 ran on this prefix. Its write is not in the \
                         state 0.4.0 serves; move the key out of this prefix (or keep it \
                         elsewhere) and run this again",
                        meta.key
                    ));
                    continue;
                }
                if matches!(family, Family::History | Family::Leases) {
                    let mut parts = tail.splitn(3, '/');
                    let actor = format!(
                        "{}/{}",
                        parts.next().unwrap_or_default(),
                        parts.next().unwrap_or_default()
                    );
                    if !tombstoned.contains(&actor) {
                        refusals.push(format!(
                            "{} belongs to an actor with no tombstone at {}/{tenant}/{actor}: \
                             a binary of a version before 0.4.0 created it during the \
                             migration; move its keys out of this prefix and run this again",
                            meta.key,
                            Family::Owners.bare()
                        ));
                        continue;
                    }
                }
                if family == Family::History
                    && now
                        <= meta
                            .modified_ms
                            .saturating_add(options.history_retention_ms)
                {
                    t.young_history += 1;
                    continue;
                }
                t.delete.push(meta.key);
            }
        }
        // Moves: never read by this version, and an older `actor move`
        // may still write one. Each expires; only expired ones go.
        let (_, moves) = listed(Family::Moves).await?;
        for meta in moves {
            if now > meta.modified_ms.saturating_add(crate::control::MOVE_TTL_MS) {
                t.delete.push(meta.key);
            } else {
                t.live_moves += 1;
            }
        }
        Ok((t, refusals))
    }

    /// `--remove-old`: deletes the old-layout keys of every migrated
    /// tenant ([`Self::removal`]), keeping the tombstones. Refuses while a
    /// run of migrate is unfinished, while the rollback still works
    /// (unless given up), and on any key an older binary wrote since.
    /// Before its first delete it marks each tenant, after which
    /// `--rollback` refuses; then, if a run took the lock meanwhile, it
    /// stops. Run again after a crash, it goes on.
    pub async fn remove_old(&self, options: RemoveOptions) -> Result<Removal, MigrateError> {
        let removal = self.removal(options).await?;
        if !removal.refusals.is_empty() {
            return Err(MigrateError::Refused(removal.refusals));
        }
        for tenant in removal.tenants.keys() {
            let mark = format!("{REMOVED_PREFIX}{tenant}");
            match self.store.create(&mark, Vec::new()).await {
                Ok(_) | Err(BucketError::Conflict) => {}
                Err(e) => {
                    return Err(MigrateError::Bucket {
                        what: format!("writing {mark}"),
                        error: e,
                    });
                }
            }
        }
        // A rollback checks for the marks under its lock: one of the two
        // sees the other.
        if self
            .store
            .get(LOCK_KEY)
            .await
            .map_err(bucket(format!("reading {LOCK_KEY}")))?
            .is_some()
        {
            return Err(MigrateError::Aborted(format!(
                "{LOCK_KEY} was taken by a run of migrate meanwhile: nothing was deleted; let \
                 it finish and run this again"
            )));
        }
        for t in removal.tenants.values() {
            each(&t.delete, self.options.concurrency, |key| async move {
                self.store
                    .delete(key)
                    .await
                    .map_err(bucket(format!("deleting {key}")))
            })
            .await?;
        }
        Ok(removal)
    }
}

/// The bare ownership keys of tenant `tenant`'s actors that have history
/// or an old-lease marker in `source` but no record, each with the root
/// key its tombstone names.
fn unowned_actors(tenant: &str, source: &Source) -> Vec<(String, String)> {
    let tail_of = |m: &Move| -> Option<String> {
        let tail = m
            .from
            .key
            .strip_prefix(&format!("{}/{tenant}/", m.family.bare()))?;
        let mut parts = tail.splitn(3, '/');
        Some(format!("{}/{}", parts.next()?, parts.next()?))
    };
    let owned: BTreeSet<String> = source
        .moves
        .iter()
        .filter(|m| m.family == Family::Owners)
        .filter_map(tail_of)
        .collect();
    let mut out = BTreeMap::new();
    for m in source
        .moves
        .iter()
        .filter(|m| matches!(m.family, Family::History | Family::Leases))
    {
        let Some(tail) = tail_of(m) else { continue };
        if owned.contains(&tail) {
            continue;
        }
        // The heir is the move's own root.
        let Some(root) = m.to.strip_prefix(STATE_PREFIX).and_then(|r| {
            let mut p = r.splitn(3, '/');
            Some((p.next()?.to_owned(), p.next()?.to_owned()))
        }) else {
            continue;
        };
        let to = StateRoot::new("", root.0.as_str(), root.1.as_str()).key(Family::Owners, &tail);
        out.insert(format!("{}/{tenant}/{tail}", Family::Owners.bare()), to);
    }
    out.into_iter().collect()
}

/// The `version seq` of a `current` body (its deploy, token aside).
fn deployed_of(body: &[u8]) -> String {
    std::str::from_utf8(body)
        .unwrap_or_default()
        .split_whitespace()
        .take(2)
        .collect::<Vec<_>>()
        .join(" ")
}

/// The first `nodes/` object that differs between `recorded` and `now`
/// (new, gone, or rewritten), by key; `None` if none does. Listing against
/// listing: entity tags are compared only to entity tags listed the same
/// way, with size and time.
fn changed_nodes(recorded: &[ObjectMeta], now: &[ObjectMeta]) -> Option<String> {
    let index = |list: &[ObjectMeta]| -> BTreeMap<String, ObjectMeta> {
        list.iter().map(|m| (m.key.clone(), m.clone())).collect()
    };
    let (before, after) = (index(recorded), index(now));
    for (key, meta) in &after {
        if before.get(key) != Some(meta) {
            return Some(format!("node {}", key.strip_prefix(NODES).unwrap_or(key)));
        }
    }
    before.keys().find(|k| !after.contains_key(*k)).map(|k| {
        format!(
            "node {} (its object is gone)",
            k.strip_prefix(NODES).unwrap_or(k)
        )
    })
}

/// The third token of a `current` body, if any.
fn third_token(body: &[u8]) -> Option<&str> {
    std::str::from_utf8(body).ok()?.split_whitespace().nth(2)
}

/// `body` (a `current`) with its third token set to `token`.
fn with_token(body: &[u8], token: &str) -> Option<Vec<u8>> {
    let text = std::str::from_utf8(body).ok()?;
    let parts: Vec<&str> = text.split_whitespace().collect();
    match parts.as_slice() {
        [version, seq] | [version, seq, _] => {
            Some(format!("{version} {seq} {token}\n").into_bytes())
        }
        _ => None,
    }
}

/// `body` (a `current`) without a third token.
fn without_token(body: &[u8]) -> Option<Vec<u8>> {
    let text = std::str::from_utf8(body).ok()?;
    let parts: Vec<&str> = text.split_whitespace().collect();
    match parts.as_slice() {
        [version, seq] | [version, seq, _] => Some(format!("{version} {seq}\n").into_bytes()),
        _ => None,
    }
}

/// A tenant or app name as the bucket can carry it in one key segment.
fn wasmkeep_name(s: &str) -> bool {
    !s.is_empty()
        && s != "."
        && s != ".."
        && !s.contains('/')
        && !s.chars().any(|c| c.is_whitespace() || c.is_control())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn app(tenant: &str, name: &str, classes: &[&str]) -> AppInfo {
        AppInfo {
            tenant: tenant.into(),
            app: name.into(),
            classes: classes.iter().map(|c| (*c).to_owned()).collect(),
        }
    }

    #[test]
    fn each_tail_names_its_class() {
        assert_eq!(
            class_of(Family::History, "chat/room/wal/x.ltx"),
            Some("chat")
        );
        assert_eq!(class_of(Family::Owners, "chat/room"), Some("chat"));
        assert_eq!(class_of(Family::Moves, "chat/726f/node-b"), Some("chat"));
        assert_eq!(
            class_of(
                Family::Alarms,
                "001700000000000/chat.t/726f/00000000000000000007-ab"
            ),
            Some("chat")
        );
        assert_eq!(
            class_of(Family::Alarms, "000000000000005/chat/726f"),
            Some("chat")
        );
        assert_eq!(class_of(Family::Alarms, "000000000000005"), None);
        assert_eq!(class_of(Family::Alarms, "1/chat/2/3/4"), None);
    }

    #[test]
    fn heirs_follow_the_deploys_then_the_tenant() {
        let none = BTreeMap::new();
        let shop = app("acme", "shop", &["cart", "chat"]);
        let blog = app("acme", "blog", &["post", "chat"]);
        let both = [&shop, &blog];
        assert_eq!(heir_of("acme", Some("cart"), &both, &none).unwrap(), "shop");
        assert_eq!(heir_of("acme", Some("post"), &both, &none).unwrap(), "blog");
        // Deployed by both: --heir, no default.
        let e = heir_of("acme", Some("chat"), &both, &none).unwrap_err();
        assert!(
            e.contains("shop and blog") && e.contains("--heir acme/APP"),
            "{e}"
        );
        let heir = BTreeMap::from([("acme".to_owned(), "blog".to_owned())]);
        assert_eq!(heir_of("acme", Some("chat"), &both, &heir).unwrap(), "blog");
        // In no manifest: the tenant's heir.
        assert!(heir_of("acme", Some("gone"), &both, &none).is_err());
        assert_eq!(heir_of("acme", Some("gone"), &both, &heir).unwrap(), "blog");
        assert_eq!(
            heir_of("acme", Some("gone"), &[&shop], &none).unwrap(),
            "shop"
        );
        assert_eq!(
            heir_of("default", Some("c"), &[], &none).unwrap(),
            "default"
        );
        assert!(heir_of("other", Some("c"), &[], &none).is_err());
        // Tenant `default` with a deployed app: a class in no manifest may
        // be an `--actor` node's, so it needs --heir; its app `default` is
        // the implicit one.
        let foo = app("default", "foo", &["c"]);
        let e = heir_of("default", Some("t"), &[&foo], &none).unwrap_err();
        assert!(e.contains("--heir default/default"), "{e}");
        assert_eq!(
            heir_of("default", Some("c"), &[&foo], &none).unwrap(),
            "foo"
        );
        let implicit = app("default", "default", &[]);
        assert_eq!(
            heir_of("default", Some("t"), &[&implicit], &none).unwrap(),
            "default"
        );
        assert_eq!(heir_of("acme", None, &[&shop], &none).unwrap(), "shop");
    }

    #[test]
    fn records_read_back() {
        let r = Record {
            mode: LockMode::Rollback,
            hash: "00ff".into(),
            tenants: BTreeSet::from(["acme".into()]),
            originals: BTreeMap::from([("acme/shop".into(), "-".into())]),
            nodes: vec![ObjectMeta {
                key: "nodes/a b".into(),
                size: 3,
                modified_ms: 9,
                etag: None,
            }],
            heirs: BTreeMap::from([("acme/chat".into(), "shop".into())]),
            counts: BTreeMap::from([("owners".into(), 2)]),
            deployed: BTreeMap::from([("acme/shop".into(), "v1 4".into())]),
            at_ms: 5,
        };
        assert_eq!(
            Record::decode(&r.encode(LOCK_HEADER), LOCK_HEADER),
            Some(r.clone())
        );
        assert_eq!(Record::decode(&r.encode(LOCK_HEADER), COMMIT_HEADER), None);
    }

    #[test]
    fn tombstones_and_tokens() {
        let t = tombstone("state/acme/shop/owners/chat/room");
        assert_eq!(
            tombstone_target(&t),
            Some("state/acme/shop/owners/chat/room")
        );
        assert_eq!(tombstone_target(b"wasmkeep-owner 1\na i\n3\n"), None);
        let u = tombstone_unowned("state/acme/shop/owners/chat/room");
        assert_eq!(
            tombstone_target(&u),
            Some("state/acme/shop/owners/chat/room")
        );
        assert!(is_unowned(&u) && !is_unowned(&t));
        assert!(crate::node_lease::OwnerRecord::decode(&u).is_none());
        assert_eq!(deployed_of(b"v2 7 3\n"), "v2 7");
        // Older binaries read only `wasmkeep-owner 1`.
        assert!(crate::node_lease::OwnerRecord::decode(&t).is_none());
        assert_eq!(with_token(b"v 3\n", "3").unwrap(), b"v 3 3\n");
        assert_eq!(with_token(b"v 3 2\n", "3").unwrap(), b"v 3 3\n");
        assert_eq!(without_token(b"v 3 3\n").unwrap(), b"v 3\n");
        assert_eq!(with_token(b"v\n", "3"), None);
        assert_eq!(third_token(b"v 3 2\n"), Some("2"));
    }

    #[test]
    fn a_changed_node_is_named() {
        let m = |key: &str, size| ObjectMeta {
            key: key.into(),
            size,
            modified_ms: 1,
            etag: Some("e".into()),
        };
        let before = vec![m("nodes/a", 1)];
        assert_eq!(changed_nodes(&before, &before), None);
        assert_eq!(
            changed_nodes(&before, &[m("nodes/a", 2)]).as_deref(),
            Some("node a")
        );
        assert_eq!(
            changed_nodes(&before, &[m("nodes/a", 1), m("nodes/b", 1)]).as_deref(),
            Some("node b")
        );
        assert!(changed_nodes(&before, &[]).unwrap().contains("gone"));
    }
}
