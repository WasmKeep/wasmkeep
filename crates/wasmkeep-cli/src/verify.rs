//! `wasmkeep cluster verify`: checks that a cluster's bucket is whole,
//! reading it and nothing else.
//!
//! It answers one question: can every actor be restored from what the
//! bucket holds, and do the records around the actors read? It is not
//! `backup verify`, which checks a backup's own contents in the backup's
//! bucket, and not `cluster diagnose`, which checks what a bucket's API
//! does by writing scratch objects.
//!
//! Every GET goes through the replica's one reader
//! ([`history::get`]), every per-actor listing through its one lister
//! ([`history::list_objects`]), and a restore's own code decides what is
//! whole: [`plan_restore`] for the chain, [`prove`] and [`linked_back`]
//! for a recorded head (ADR 0035), the decoders of each record for the
//! rest. The store is wrapped so that a write is refused before it is
//! sent (`store::ReadOnly`).
//!
//! Bounds: an app's history is listed as one stream and held one actor at
//! a time (16 actors at once, each group at most 32 MiB of names and
//! ETags; an actor over that is not checked; one listed again alone has
//! no such limit); the other families are streamed key by key; with `--deep` the bodies in memory
//! stay under 256 MiB unless one object alone is larger; at most 1,000
//! problems are kept (all are counted). What grows with
//! the bucket is one 16-byte entry per actor with history, up to
//! 4 million of them (about 100 MiB), so that an alarm entry or an ownership
//! record of such an actor needs no read of its own.

mod store;

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::fmt::Write as _;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::Context as _;
use clap::Args;
use futures_util::{StreamExt as _, TryStreamExt as _};
use object_store::path::Path as Key;
use object_store::{ObjectMeta, ObjectStore};
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};
use wasmkeep_cluster::local_heads::{Proof, Requirement, linked_back, prove};
use wasmkeep_cluster::node_lease::{NodeLeaseObject, OwnerObject};
use wasmkeep_cluster::recorded_head::Position;
use wasmkeep_cluster::replication::{
    EMPTY, Listing, PlanError, SegmentId, plan_restore, segment_key, snapshot_key,
};
use wasmkeep_cluster::state_root::{Family, StateRoot};
use wasmkeep_cluster::{alarm_index, app_format, blob_sweep};
use wasmkeep_server::history;

use crate::actor_groups::group_listing;

type Store = Arc<dyn ObjectStore>;
type Objects = BTreeMap<String, ObjectMeta>;

/// Actors, records and entries checked at once.
const CONCURRENCY: usize = 16;
/// Bodies `--deep` holds at once, in MiB; one larger object is read alone.
const BODY_BUDGET: u32 = 256;
/// Problems kept and printed; the rest are counted.
const MAX_PROBLEMS: usize = 1_000;
/// Actors remembered as having history (16 bytes each); past it, what
/// would have been looked up is read from the bucket instead.
const MAX_SEEN: usize = 4_000_000;
/// The longest a history object is read (as an export's replay bound).
const MAX_OBJECT: u64 = (2 << 30) + (2 << 30) / 16 + (1 << 20);
/// The longest a component or a configuration is read.
const MAX_COMPONENT: u64 = 2 << 30;
/// The longest a small record is read.
const MAX_RECORD: u64 = 64 << 10;
/// The longest a deploy manifest is read.
const MAX_MANIFEST: u64 = 4 << 20;
/// A check that found a problem is made again on a fresh read, this many
/// times in all: a running cluster collects history and replaces records
/// while they are read. A read the bucket refuses is sent again as often.
const ATTEMPTS: usize = 3;
/// Between two of those attempts.
const AGAIN_AFTER: Duration = Duration::from_millis(500);
/// A streamed listing silent for this long is given up.
const LIST_IDLE: Duration = Duration::from_secs(30);
/// Versions before the current one that a deploy keeps for rollbacks.
const KEEP_PREVIOUS: usize = wasmkeep_server::apps::KEEP_PREVIOUS;

/// `wasmkeep cluster verify`.
#[derive(Debug, Args)]
pub struct VerifyArgs {
    /// Bucket shared by the cluster: s3://BUCKET[/PREFIX] (credentials from
    /// the AWS_* environment variables; read access is enough, and no
    /// write is ever sent).
    #[arg(long)]
    bucket: String,

    /// One app, TENANT/APP, instead of every app under the prefix.
    #[arg(long, value_parser = crate::parse_app)]
    app: Option<(String, String)>,

    /// One actor of --app, CLASS/NAME: its history and its ownership
    /// record, nothing else.
    #[arg(long, value_name = "CLASS/NAME", requires = "app")]
    actor: Option<String>,

    /// Read the ownership record (and with --deep the history) of N actors
    /// of each app, chosen at random, instead of every actor (the listing
    /// is still whole, so every actor's chain is still checked by its key
    /// names).
    #[arg(long, value_name = "N", value_parser = clap::value_parser!(u64).range(1..), conflicts_with = "actor")]
    sample: Option<u64>,

    /// Also read every history object a restore would read and check its
    /// checksums (without --deep, damage inside one is not found), the
    /// previous versions a deploy keeps, and each static asset. Each
    /// object must be the transaction its key names and follow the one
    /// before it by its database checksum; the pages are not replayed into
    /// a database.
    #[arg(long)]
    deep: bool,

    /// Print JSON instead of text, for scripts.
    #[arg(long)]
    json: bool,
}

/// What to check.
#[derive(Debug, Clone, Default)]
pub struct Options {
    /// One app, instead of every app under the prefix.
    pub app: Option<(String, String)>,
    /// One actor of `app`, as (class, name).
    pub actor: Option<(String, String)>,
    /// Read the record and the bodies of this many actors per app.
    pub sample: Option<u64>,
    /// Read history bodies, previous versions and assets.
    pub deep: bool,
}

/// Something that is not whole, with the key it is about.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Problem {
    /// The object's key in the bucket. For an object that is missing, the
    /// key it would have; `*` stands for a part its neighbours do not tell.
    pub key: String,
    /// The check that found it: `deploy`, `format`, `history`,
    /// `ownership`, `recorded-head`, `alarm`, `files`, `node`.
    pub check: &'static str,
    /// What is wrong.
    pub detail: String,
    /// Nothing is known of the object: the bucket did not give it, or it
    /// was not checked. Such a finding is in [`Report::unread`] and makes
    /// the run incomplete (exit code 2); it does not say the bucket is
    /// not whole.
    pub unread: bool,
}

/// One app's result.
#[derive(Debug, Clone, Default)]
pub struct AppReport {
    /// `TENANT/APP`.
    pub name: String,
    /// `deployed`, `not deployed`, `deleted`, `restoring` or `unreadable`.
    pub state: &'static str,
    /// The current version and its deploy sequence number.
    pub version: Option<(String, u64)>,
    /// Versions whose manifest and digests were checked.
    pub versions_checked: u64,
    /// Whether the app writes ownership records with a recorded head.
    pub recorded_head: bool,
    /// The upgrade floor, if the app has one.
    pub floor: Option<String>,
    /// Actors with history.
    pub actors: u64,
    /// Those with a restore plan and no problem.
    pub restorable: u64,
    /// Those whose ownership record was read.
    pub records_read: u64,
    /// Those of them with no ownership record.
    pub without_record: u64,
    /// Those whose history bodies were read (`--deep`).
    pub deep_checked: u64,
    /// Those whose listed head is past a complete recorded head: writes
    /// that reached the bucket and were not acknowledged before a release.
    pub past_recorded_head: u64,
    /// History objects listed, and their bytes.
    pub history_objects: u64,
    /// See [`Self::history_objects`].
    pub history_bytes: u64,
    /// Ownership records listed.
    pub records: u64,
    /// Keys under `owners/` that are not a record's (left alone).
    pub records_skipped: u64,
    /// Why this app's checks stopped before their end (a listing the
    /// bucket refused or that stalled; with `--actor`, a deleted app).
    /// What was counted before is kept.
    pub stopped: Option<String>,
    /// Alarm index entries.
    pub alarms: u64,
    /// Keys under `alarms/` that are not entries (readers skip them).
    pub alarms_skipped: u64,
    /// Per-actor lease markers left from before node leases (ADR 0020).
    /// Counted, never read or deleted (`wasmkeep cluster markers clean`
    /// removes them, ADR 0045).
    pub lease_markers: u64,
    /// The app's generation floor (ADR 0045), if it has one: its
    /// generation, and whether it says no marker is left.
    pub generation_floor: Option<(u64, bool)>,
    /// Markers left under a floor that says `markers none`: redundant
    /// (what an acquisition from just before the finalize may still
    /// write), ignored by nodes, not a problem.
    pub redundant_markers: u64,
    /// Files of the app's current epoch, and their bytes.
    pub files: u64,
    /// See [`Self::files`].
    pub file_bytes: u64,
    /// Files under another epoch (being removed after a recreation or a
    /// restore) or under a key that is not a file's.
    pub files_elsewhere: u64,
    /// The last count the cluster published: epoch, bytes, files.
    pub usage: Option<(String, u64, u64)>,
    /// Sweep records of deleted actors' files.
    pub sweep_records: u64,
}

/// How old the mark of a `cluster forget` is before `cluster status` and
/// `cluster verify` say its command did not finish.
pub const FORGET_UNFINISHED_MS: u64 = 5_000;

/// A run's result.
#[derive(Debug, Clone, Default)]
pub struct Report {
    /// The bucket, as given.
    pub bucket: String,
    /// Whether bodies were read.
    pub deep: bool,
    /// The sample size, if one was asked.
    pub sample: Option<u64>,
    /// The one actor checked, `CLASS/NAME`, if one was asked: nothing
    /// else of its app or of the cluster was.
    pub actor: Option<String>,
    /// Each app, by name.
    pub apps: Vec<AppReport>,
    /// Node leases that decode.
    pub nodes: u64,
    /// Heartbeats in a format from before node leases.
    pub nodes_old_format: u64,
    /// What an operator should know that is neither a problem with what
    /// the bucket holds nor something left unread: it changes neither
    /// the verdict nor the exit code. Today: a `cluster forget` that did
    /// not finish.
    pub notes: Vec<String>,
    /// The problems, at most 1,000.
    pub problems: Vec<Problem>,
    /// How many there are in all.
    pub problems_total: u64,
    /// What could not be read or was not checked, at most 1,000.
    pub unread: Vec<Problem>,
    /// How many there are in all.
    pub unread_total: u64,
    /// Why the run stopped before its end, if it did (each app has its
    /// own [`AppReport::stopped`]).
    pub stopped: Vec<String>,
    /// Objects read, and their bytes.
    pub objects_read: u64,
    /// See [`Self::objects_read`].
    pub bytes_read: u64,
    /// Requests made: one per GET, one per listing and one more per 1,000
    /// keys a listing returned.
    pub requests: u64,
    /// Keys listed.
    pub keys_listed: u64,
    /// Writes this run tried, each refused before it was sent: always 0.
    pub writes_refused: u64,
    /// How long it took.
    pub seconds: f64,
}

impl Report {
    /// Nothing wrong was found, and everything was read.
    pub fn whole(&self) -> bool {
        self.problems_total == 0 && self.complete()
    }

    /// Everything that was to be checked was read: no object the bucket
    /// refused to give, nothing left unchecked, no listing cut short.
    pub fn complete(&self) -> bool {
        self.unread_total == 0
            && self.stopped.is_empty()
            && self.apps.iter().all(|a| a.stopped.is_none())
    }

    /// The process's exit code: 0 whole; 1 a problem was found (whether or
    /// not the run was complete); 2 no problem found, but the run was not
    /// complete, so there is no verdict.
    pub fn exit_code(&self) -> i32 {
        if self.problems_total > 0 {
            1
        } else if self.complete() {
            0
        } else {
            2
        }
    }

    /// For scripts.
    pub fn to_json(&self) -> Value {
        let apps: Vec<Value> = self
            .apps
            .iter()
            .map(|a| {
                json!({
                    "app": a.name,
                    "state": a.state,
                    "version": a.version.as_ref().map(|(v, _)| v),
                    "deploy_seq": a.version.as_ref().map(|(_, s)| s),
                    "versions_checked": a.versions_checked,
                    "recorded_head": a.recorded_head,
                    "floor": a.floor,
                    "actors": a.actors,
                    "restorable": a.restorable,
                    "records_read": a.records_read,
                    "without_record": a.without_record,
                    "deep_checked": a.deep_checked,
                    "past_recorded_head": a.past_recorded_head,
                    "history_objects": a.history_objects,
                    "history_bytes": a.history_bytes,
                    "ownership_records": a.records,
                    "ownership_keys_skipped": a.records_skipped,
                    "stopped": a.stopped,
                    "alarm_entries": a.alarms,
                    "alarm_keys_skipped": a.alarms_skipped,
                    "lease_markers": a.lease_markers,
                    "redundant_markers": a.redundant_markers,
                    "generation_floor": a.generation_floor.map(|(generation, none)| {
                        json!({"generation": generation, "markers": if none { "none" } else { "kept" }})
                    }),
                    "files": a.files,
                    "file_bytes": a.file_bytes,
                    "files_elsewhere": a.files_elsewhere,
                    "files_last_count": a.usage.as_ref().map(|(epoch, bytes, files)| {
                        json!({"epoch": epoch, "bytes": bytes, "files": files})
                    }),
                    "sweep_records": a.sweep_records,
                })
            })
            .collect();
        let listed = |found: &[Problem]| -> Vec<Value> {
            found
                .iter()
                .map(|p| json!({"key": p.key, "check": p.check, "detail": p.detail}))
                .collect()
        };
        let (problems, unread) = (listed(&self.problems), listed(&self.unread));
        json!({
            "bucket": self.bucket,
            "whole": self.whole(),
            "complete": self.complete(),
            "exit_code": self.exit_code(),
            "deep": self.deep,
            "sample": self.sample,
            "actor": self.actor,
            "apps": apps,
            "nodes": {"leases": self.nodes, "old_format": self.nodes_old_format},
            "notes": self.notes,
            "problems": problems,
            "problems_total": self.problems_total,
            "unread": unread,
            "unread_total": self.unread_total,
            "stopped": self.stopped,
            "writes_refused": self.writes_refused,
            "objects_read": self.objects_read,
            "bytes_read": self.bytes_read,
            "requests": self.requests,
            "keys_listed": self.keys_listed,
            "seconds": self.seconds,
        })
    }

    /// For people.
    pub fn to_text(&self) -> String {
        let mut out = String::new();
        let _ = writeln!(
            out,
            "Verifying {}{}{}",
            self.bucket,
            if self.deep { ", bodies read" } else { "" },
            self.sample
                .map(|n| format!(", {n} actors of each app sampled"))
                .unwrap_or_default()
        );
        let unchecked = |a: &AppReport| matches!(a.state, "deleted" | "restoring" | "unreadable");
        if let Some(actor) = &self.actor {
            for a in &self.apps {
                if unchecked(a) {
                    let _ = writeln!(out, "app {}: {}, not checked", a.name, a.state);
                    continue;
                }
                let _ = writeln!(
                    out,
                    "actor {actor} of {}: {} ({} objects, {}); ownership record {}{}",
                    a.name,
                    if a.restorable == 1 {
                        "restorable"
                    } else {
                        "not restorable"
                    },
                    a.history_objects,
                    size(a.history_bytes),
                    if a.records_read == 1 {
                        "read"
                    } else {
                        "absent"
                    },
                    if a.deep_checked == 1 {
                        "; history read whole"
                    } else {
                        ""
                    }
                );
            }
        }
        for a in self.apps.iter().filter(|_| self.actor.is_none()) {
            let _ = write!(out, "app {}: {}", a.name, a.state);
            if let Some((version, seq)) = &a.version {
                let _ = write!(out, " {version} (deploy {seq})");
            }
            if a.recorded_head {
                out.push_str(", recorded-head format");
            }
            if let Some(floor) = &a.floor {
                let _ = write!(out, ", floor {floor}");
            }
            out.push('\n');
            if unchecked(a) {
                continue;
            }
            let _ = writeln!(
                out,
                "  actors: {} with history, {} restorable ({} objects, {}); {} ownership records{}, {} read{}",
                a.actors,
                a.restorable,
                a.history_objects,
                size(a.history_bytes),
                a.records,
                if a.records_skipped > 0 {
                    format!(" ({} other keys)", a.records_skipped)
                } else {
                    String::new()
                },
                a.records_read,
                if self.deep {
                    format!("; {} read whole", a.deep_checked)
                } else {
                    String::new()
                }
            );
            if a.without_record > 0 || a.past_recorded_head > 0 {
                let _ = writeln!(
                    out,
                    "  {} without an ownership record, {} with a head past the recorded one",
                    a.without_record, a.past_recorded_head
                );
            }
            let _ = writeln!(
                out,
                "  alarms: {} entries{}; lease markers left: {}",
                a.alarms,
                if a.alarms_skipped > 0 {
                    format!(" ({} other keys)", a.alarms_skipped)
                } else {
                    String::new()
                },
                a.lease_markers
            );
            if let Some((generation, none)) = a.generation_floor {
                let _ = writeln!(
                    out,
                    "  generation floor: {generation}, markers {}{}",
                    if none { "none" } else { "kept" },
                    if a.redundant_markers > 0 {
                        format!(
                            " ({} left are redundant: nodes ignore them; `wasmkeep cluster \
                             markers clean` removes them)",
                            a.redundant_markers
                        )
                    } else {
                        String::new()
                    }
                );
            }
            if a.files > 0 || a.files_elsewhere > 0 || a.usage.is_some() || a.sweep_records > 0 {
                let _ = writeln!(
                    out,
                    "  files: {} ({}){}{}; sweep records: {}",
                    a.files,
                    size(a.file_bytes),
                    if a.files_elsewhere > 0 {
                        format!(", {} under another epoch or key", a.files_elsewhere)
                    } else {
                        String::new()
                    },
                    a.usage
                        .as_ref()
                        .map(|(_, bytes, files)| format!(", last count {files} ({})", size(*bytes)))
                        .unwrap_or_default(),
                    a.sweep_records
                );
            }
        }
        if self.actor.is_none() {
            let _ = writeln!(
                out,
                "nodes: {} leases{}",
                self.nodes,
                if self.nodes_old_format > 0 {
                    format!(", {} heartbeats in an older format", self.nodes_old_format)
                } else {
                    String::new()
                }
            );
        }
        for note in &self.notes {
            let _ = writeln!(out, "note: {note}");
        }
        for p in &self.problems {
            let _ = writeln!(out, "problem: {}: {} ({})", p.key, p.detail, p.check);
        }
        if self.problems_total > self.problems.len() as u64 {
            let _ = writeln!(
                out,
                "... and {} more problems",
                self.problems_total - self.problems.len() as u64
            );
        }
        for p in &self.unread {
            let _ = writeln!(out, "not read: {}: {} ({})", p.key, p.detail, p.check);
        }
        if self.unread_total > self.unread.len() as u64 {
            let _ = writeln!(
                out,
                "... and {} more not read",
                self.unread_total - self.unread.len() as u64
            );
        }
        for a in &self.apps {
            if let Some(why) = &a.stopped {
                let _ = writeln!(out, "stopped: app {}: {why}", a.name);
            }
        }
        for why in &self.stopped {
            let _ = writeln!(out, "stopped: {why}");
        }
        let _ = writeln!(
            out,
            "Read {} objects ({}) and listed {} keys in {} requests, {:.1} s.",
            self.objects_read,
            size(self.bytes_read),
            self.keys_listed,
            self.requests,
            self.seconds
        );
        if self.whole() {
            let scope = if self.actor.is_some() {
                ""
            } else if self.sample.is_some() {
                " (records and bodies of the sample only)"
            } else if self.deep {
                ""
            } else {
                " (history bodies not read: --deep reads them)"
            };
            let what = if self.actor.is_some() {
                "The actor"
            } else {
                "The bucket"
            };
            let _ = writeln!(out, "{what} is whole{scope}.");
        } else if self.problems_total > 0 {
            let _ = writeln!(
                out,
                "{} problems found{}.",
                self.problems_total,
                if self.complete() {
                    ""
                } else {
                    ", and the run was not complete"
                }
            );
        } else {
            let _ = writeln!(
                out,
                "No problem found, but the run was not complete ({} not read or not \
                 checked): no verdict.",
                self.unread_total
            );
        }
        out
    }
}

fn size(bytes: u64) -> String {
    const MIB: u64 = 1 << 20;
    if bytes >= 10 * MIB {
        format!("{} MiB", bytes / MIB)
    } else if bytes >= 10 * 1024 {
        format!("{} KiB", bytes / 1024)
    } else {
        format!("{bytes} bytes")
    }
}

/// `wasmkeep cluster verify`: prints the report, whole or partial, and
/// returns the exit code: 0 whole, 1 problems found, 2 no verdict (the
/// run could not start, or something could not be read or listed).
pub fn command(args: &VerifyArgs) -> i32 {
    let run = || -> anyhow::Result<Report> {
        let actor = match (&args.actor, &args.app) {
            (Some(actor), Some(_)) => Some(
                actor
                    .split_once('/')
                    .filter(|(class, name)| !class.is_empty() && !name.is_empty())
                    .map(|(class, name)| (class.to_owned(), name.to_owned()))
                    .with_context(|| format!("expected CLASS/NAME, got {actor:?}"))?,
            ),
            _ => None,
        };
        let options = Options {
            app: args.app.clone(),
            actor,
            sample: args.sample,
            deep: args.deep,
        };
        let (store, prefix) = crate::open_bucket_unchecked(&args.bucket)?;
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let mut report = rt.block_on(verify(store, &prefix, &options));
        report.bucket.clone_from(&args.bucket);
        Ok(report)
    };
    match run() {
        Ok(report) => {
            if args.json {
                println!("{:#}", report.to_json());
            } else {
                print!("{}", report.to_text());
            }
            report.exit_code()
        }
        Err(e) => {
            eprintln!("error: the verification did not start: {e:#}");
            2
        }
    }
}

/// Checks the cluster under `prefix` of `store`. Never writes.
///
/// It always returns a report. A problem with what the bucket holds is in
/// [`Report::problems`]; what could not be read is in [`Report::unread`];
/// a listing the bucket refused, a prefix that holds nothing or an app
/// that is not there ends the run or that app's checks and is in
/// [`Report::stopped`] or [`AppReport::stopped`], with everything found
/// until then.
pub async fn verify(store: Store, prefix: &str, options: &Options) -> Report {
    let started = std::time::Instant::now();
    let (store, counts) = store::ReadOnly::wrap(store);
    let mut prefix = prefix.trim_matches('/').to_owned();
    if !prefix.is_empty() {
        prefix.push('/');
    }
    let run = Run {
        store,
        prefix,
        deep: options.deep,
        sample: options.sample,
        budget: Arc::new(tokio::sync::Semaphore::new(BODY_BUDGET as usize)),
        found: Mutex::default(),
    };
    let mut report = Report {
        deep: options.deep,
        sample: options.sample,
        actor: options
            .actor
            .as_ref()
            .map(|(class, name)| format!("{class}/{name}")),
        ..Report::default()
    };
    if let Err(e) = run.all(options, &mut report).await {
        report.stopped.push(format!("{e:#}"));
    }
    let found = run.found.into_inner().unwrap_or_else(|p| p.into_inner());
    report.problems = found.problems;
    report.problems_total = found.total;
    report.unread = found.unread;
    report.unread_total = found.unread_total;
    report.objects_read = counts.objects();
    report.bytes_read = counts.bytes();
    report.requests = counts.gets() + counts.lists();
    report.keys_listed = counts.listed();
    report.writes_refused = counts.writes();
    report.seconds = started.elapsed().as_secs_f64();
    report
}

#[derive(Debug, Default)]
struct Found {
    problems: Vec<Problem>,
    total: u64,
    unread: Vec<Problem>,
    unread_total: u64,
}

/// Where the findings stood: [`Run::mark`].
type Mark = (usize, u64, usize, u64);

struct Run {
    /// Read-only and counted.
    store: Store,
    /// The bucket prefix, ending in `/` or empty.
    prefix: String,
    deep: bool,
    sample: Option<u64>,
    /// MiB of history bodies in memory.
    budget: Arc<tokio::sync::Semaphore>,
    found: Mutex<Found>,
}

/// What a read gave.
enum Read {
    Body(bytes::Bytes),
    Absent,
    /// No body: why, and whether nothing is known of the object (the
    /// bucket did not give it) rather than something wrong (too large).
    Failed(String, bool),
}

/// Why a history object did not pass.
struct Fault {
    why: String,
    /// Nothing is known of it.
    unread: bool,
    /// It is not the object that was listed any more: list again.
    changed: bool,
}

/// One actor's result.
#[derive(Debug, Default)]
struct Actor {
    /// What was found, problems and what could not be read alike.
    problems: Vec<Problem>,
    /// Worth another look on a fresh listing: something looks wrong, or
    /// an object changed while it was read.
    again: bool,
    /// A restore plan exists (its head is not the empty database).
    planned: bool,
    record_read: bool,
    record_absent: bool,
    deep: bool,
    past_recorded_head: bool,
    objects: u64,
    bytes: u64,
}

/// An app, as its actors' checks need it.
struct AppView {
    root: StateRoot,
    /// Whether the app records heads; `None` when its setting did not
    /// read, and nothing is concluded from a record's version.
    recorded_head: Option<bool>,
    /// The setting, read again, says the app records heads now (`wasmkeep
    /// app format --enable` ran meanwhile).
    enabled_since: std::sync::atomic::AtomicBool,
}

fn problem(key: impl Into<String>, check: &'static str, detail: impl Into<String>) -> Problem {
    failed(key, check, detail, false)
}

/// Something that could not be read or was not checked.
fn unread(key: impl Into<String>, check: &'static str, detail: impl Into<String>) -> Problem {
    failed(key, check, detail, true)
}

fn failed(
    key: impl Into<String>,
    check: &'static str,
    detail: impl Into<String>,
    unread: bool,
) -> Problem {
    Problem {
        key: key.into(),
        check,
        detail: detail.into(),
        unread,
    }
}

/// Whether a read's error says nothing of the object (the bucket refused,
/// stalled or the object changed) rather than that it is wrong.
fn lost(e: &history::Error) -> bool {
    !matches!(e, history::Error::Corrupt(_))
}

/// The key as the bucket spells it (the store escapes a `%` once more).
fn spelled(raw: &str) -> String {
    Key::from(raw).as_ref().to_owned()
}

fn sha256(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

/// An actor in the set of those with history: `class/name` as its history
/// folder spells them, hashed.
fn seen_id(class: &str, encoded_name: &str) -> [u8; 16] {
    let hash = Sha256::digest(format!("{class}/{encoded_name}").as_bytes());
    let mut id = [0u8; 16];
    id.copy_from_slice(&hash[..16]);
    id
}

/// The keys under the folder `folder` (raw, ending in `/`), each relative
/// to it, as one stream: nothing is kept.
fn keys_under(
    store: &Store,
    folder: &str,
) -> futures_util::stream::BoxStream<'static, anyhow::Result<(String, ObjectMeta)>> {
    let base = Key::from(folder.trim_end_matches('/'));
    let encoded = format!("{}/", base.as_ref());
    // S3's paginated stream must not be polled again after its end.
    let listing = store.list(Some(&base)).fuse();
    futures_util::stream::try_unfold(
        (listing, encoded, folder.to_owned()),
        |(mut listing, encoded, folder)| async move {
            loop {
                let next = tokio::time::timeout(LIST_IDLE, listing.next())
                    .await
                    .with_context(|| format!("the listing of {folder} stalled"))?;
                let Some(meta) = next else {
                    return Ok(None);
                };
                let meta = meta.with_context(|| format!("listing {folder}"))?;
                if let Some(rel) = meta.location.as_ref().strip_prefix(encoded.as_str()) {
                    let rel = rel.to_owned();
                    return Ok(Some(((rel, meta), (listing, encoded, folder))));
                }
            }
        },
    )
    .boxed()
}

impl Run {
    fn add(&self, new: impl IntoIterator<Item = Problem>) {
        let mut found = self.found.lock().unwrap_or_else(|p| p.into_inner());
        let found = &mut *found;
        for p in new {
            let (kept, total) = if p.unread {
                (&mut found.unread, &mut found.unread_total)
            } else {
                (&mut found.problems, &mut found.total)
            };
            if kept.contains(&p) {
                continue;
            }
            *total += 1;
            if kept.len() < MAX_PROBLEMS {
                kept.push(p);
            }
        }
    }

    /// Where the findings stand, to [`Self::rewind`] to.
    fn mark(&self) -> Mark {
        let found = self.found.lock().unwrap_or_else(|p| p.into_inner());
        (
            found.problems.len(),
            found.total,
            found.unread.len(),
            found.unread_total,
        )
    }

    /// Forgets what was found since `mark`.
    fn rewind(&self, mark: Mark) {
        let mut found = self.found.lock().unwrap_or_else(|p| p.into_inner());
        found.problems.truncate(mark.0);
        found.total = mark.1;
        found.unread.truncate(mark.2);
        found.unread_total = mark.3;
    }

    /// One read through the replica's reader, sent again after a pause
    /// while the bucket refuses it (the reader's own attempts follow each
    /// other at once).
    async fn get(
        &self,
        location: &Key,
        listed: Option<&ObjectMeta>,
        cap: u64,
    ) -> Result<bytes::Bytes, history::Error> {
        let mut attempt = 1;
        loop {
            match history::get(&self.store, location, listed, cap).await {
                Err(history::Error::Bucket(_)) if attempt < ATTEMPTS => {
                    attempt += 1;
                    tokio::time::sleep(AGAIN_AFTER).await;
                }
                other => return other,
            }
        }
    }

    /// Reads the object at `raw` whole.
    async fn read(&self, raw: &str, cap: u64) -> Read {
        match self.get(&Key::from(raw), None, cap).await {
            Ok(body) => Read::Body(body),
            Err(history::Error::Missing(_)) => Read::Absent,
            Err(e) => Read::Failed(e.to_string(), lost(&e)),
        }
    }

    /// Whether anything is under `folder` (raw, ending in `/`).
    async fn any_under(&self, folder: &str) -> anyhow::Result<bool> {
        Ok(keys_under(&self.store, folder).try_next().await?.is_some())
    }

    /// The whole run: every app (or the one asked), then the nodes.
    async fn all(&self, options: &Options, report: &mut Report) -> anyhow::Result<()> {
        let found = self.apps().await?;
        let apps = match &options.app {
            Some(named) => {
                anyhow::ensure!(
                    found.contains(named),
                    "nothing of the app {}/{} is under this bucket prefix",
                    named.0,
                    named.1
                );
                BTreeSet::from([named.clone()])
            }
            None => found,
        };
        for (tenant, app) in apps {
            let mark = self.mark();
            let mut checked = AppReport {
                name: format!("{tenant}/{app}"),
                ..AppReport::default()
            };
            let done = match &options.actor {
                Some((class, name)) => {
                    self.one_actor(&tenant, &app, class, name, &mut checked)
                        .await
                }
                None => self.app(&tenant, &app, &mut checked).await,
            };
            if let Err(e) = done {
                checked.stopped = Some(format!("{e:#}"));
            }
            // An app deleted or put under restore while it was read: what
            // was found of it says nothing.
            if self.mark() != mark || checked.stopped.is_some() {
                let before = checked.state;
                self.app_marks(&tenant, &app, &mut checked).await;
                if checked.state != before && matches!(checked.state, "deleted" | "restoring") {
                    self.rewind(mark);
                    checked = AppReport {
                        name: checked.name,
                        state: checked.state,
                        ..AppReport::default()
                    };
                    self.app_marks(&tenant, &app, &mut checked).await;
                }
            }
            if options.actor.is_some() && checked.state == "deleted" {
                checked.stopped = Some("the app is deleted: nothing of it is checked".into());
            }
            report.apps.push(checked);
        }
        if options.actor.is_none() {
            self.nodes(report).await?;
            anyhow::ensure!(
                !report.apps.is_empty() || report.nodes + report.nodes_old_format > 0,
                "nothing of a cluster is under this bucket prefix: no app and no node"
            );
        }
        Ok(())
    }

    /// The app's `deleted` and `restoring` marks: sets the state and says
    /// whether the app's state is to be checked.
    async fn app_marks(&self, tenant: &str, app: &str, report: &mut AppReport) -> bool {
        let base = format!("{}apps/{tenant}/{app}", self.prefix);
        match self.read(&format!("{base}/deleted"), MAX_RECORD).await {
            Read::Body(_) => {
                report.state = "deleted";
                return false;
            }
            Read::Absent => {}
            Read::Failed(why, lost) => {
                let key = spelled(&format!("{base}/deleted"));
                self.add([failed(key, "deploy", why, lost)]);
            }
        }
        match self.read(&format!("{base}/restoring"), MAX_RECORD).await {
            Read::Body(_) => {
                report.state = "restoring";
                self.add([unread(
                    spelled(&format!("{base}/restoring")),
                    "deploy",
                    "a restore of this app is under way or did not finish: its state is not \
                     checked",
                )]);
                false
            }
            Read::Absent => true,
            Read::Failed(why, lost) => {
                let key = spelled(&format!("{base}/restoring"));
                self.add([failed(key, "deploy", why, lost)]);
                true
            }
        }
    }

    /// The app's recorded-head setting; `None` when it does not read (the
    /// finding says why).
    async fn format_setting(&self, root: &StateRoot) -> (Option<bool>, Option<Problem>) {
        let key = app_format::key_of(root);
        match self.read(&key, MAX_RECORD).await {
            Read::Body(body) => match app_format::decode(Some(&body)) {
                Ok(format) => (Some(format == app_format::AppFormat::RecordedHead), None),
                Err(e) => (None, Some(problem(spelled(&key), "format", e.to_string()))),
            },
            Read::Absent => (Some(false), None),
            Read::Failed(why, lost) => (None, Some(failed(spelled(&key), "format", why, lost))),
        }
    }

    /// The folders directly under `folder` (raw, ending in `/`).
    async fn folders(&self, folder: &str) -> anyhow::Result<Vec<String>> {
        let base = Key::from(folder.trim_end_matches('/'));
        let listed = tokio::time::timeout(LIST_IDLE, self.store.list_with_delimiter(Some(&base)))
            .await
            .with_context(|| format!("the listing of {folder} stalled"))?
            .with_context(|| format!("listing {folder}"))?;
        Ok(listed
            .common_prefixes
            .iter()
            .filter_map(|p| p.filename().map(str::to_owned))
            .collect())
    }

    /// Every app the prefix holds something of: a deploy, state, or a
    /// format setting.
    async fn apps(&self) -> anyhow::Result<BTreeSet<(String, String)>> {
        let mut apps = BTreeSet::new();
        for family in ["apps", "state", "formats"] {
            let base = format!("{}{family}/", self.prefix);
            for tenant in self.folders(&base).await? {
                for app in self.folders(&format!("{base}{tenant}/")).await? {
                    if crate::parse_app(&format!("{tenant}/{app}")).is_ok() {
                        apps.insert((tenant.clone(), app));
                    }
                }
            }
        }
        Ok(apps)
    }

    /// The app's deploy and settings; `None` when its state is not to be
    /// checked (deleted, restoring, or in a layout this version cannot read).
    async fn app_settings(
        &self,
        tenant: &str,
        app: &str,
        deploy: bool,
        report: &mut AppReport,
    ) -> Option<AppView> {
        let base = format!("{}apps/{tenant}/{app}", self.prefix);
        report.state = "not deployed";
        if !self.app_marks(tenant, app, report).await {
            return None;
        }
        if deploy {
            // A deploy may replace `current` while its version is read.
            let mut problems = Vec::new();
            for attempt in 0..ATTEMPTS {
                if attempt > 0 {
                    tokio::time::sleep(AGAIN_AFTER).await;
                }
                let mut again = AppReport::default();
                problems = self.deploy(&base, &mut again).await;
                (report.state, report.version, report.versions_checked) =
                    (again.state, again.version, again.versions_checked);
                // What the bucket refused was already asked again.
                if problems.iter().all(|p| p.unread) {
                    break;
                }
            }
            self.add(problems);
            if report.state == "unreadable" {
                return None;
            }
        }
        let root = StateRoot::new(&self.prefix, tenant, app);
        let (recorded_head, found) = self.format_setting(&root).await;
        self.add(found);
        report.recorded_head = recorded_head == Some(true);
        let key = app_format::floor::key_of(&root);
        match self.read(&key, MAX_RECORD).await {
            Read::Body(body) => match app_format::floor::decode(Some(&body)) {
                Ok(floor) => report.floor = floor.map(|f| f.to_string()),
                Err(e) => self.add([problem(spelled(&key), "format", e.to_string())]),
            },
            Read::Absent => {}
            Read::Failed(why, lost) => self.add([failed(spelled(&key), "format", why, lost)]),
        }
        Some(AppView {
            root,
            recorded_head,
            enabled_since: std::sync::atomic::AtomicBool::new(false),
        })
    }

    /// `current`, its version and, with `--deep`, the versions kept before
    /// it for rollbacks.
    async fn deploy(&self, base: &str, report: &mut AppReport) -> Vec<Problem> {
        use wasmkeep_server::apps::Current;
        let mut problems = Vec::new();
        let key = format!("{base}/current");
        let current = match self.read(&key, MAX_RECORD).await {
            Read::Body(body) => body,
            Read::Absent => {
                report.state = "not deployed";
                return problems;
            }
            Read::Failed(why, lost) => {
                report.state = "unreadable";
                problems.push(failed(spelled(&key), "deploy", why, lost));
                return problems;
            }
        };
        let Some(current) = Current::parse(&current) else {
            report.state = "unreadable";
            problems.push(problem(spelled(&key), "deploy", "`current` does not parse"));
            return problems;
        };
        report.version = Some((current.version.clone(), current.seq));
        if !current.rooted {
            report.state = "unreadable";
            problems.push(problem(
                spelled(&key),
                "deploy",
                "the app is in the layout from before 0.4.0, which this version does not read",
            ));
            return problems;
        }
        report.state = "deployed";
        let mut contents = None;
        let mut version = Some(current.version);
        // The current version must be whole. One kept for a rollback may
        // have been collected: a missing manifest ends the walk.
        for depth in 0..=KEEP_PREVIOUS {
            let Some(v) = version.take() else { break };
            let manifest = self
                .version(base, &v, depth == 0, &mut contents, &mut problems)
                .await;
            let Some(manifest) = manifest else { break };
            report.versions_checked += 1;
            if !self.deep {
                break;
            }
            version = manifest.previous;
        }
        problems
    }

    /// One version: its manifest parses and every object it names has the
    /// digest it names. `None` if the manifest is absent or unreadable.
    async fn version(
        &self,
        base: &str,
        version: &str,
        current: bool,
        contents: &mut Option<Objects>,
        problems: &mut Vec<Problem>,
    ) -> Option<wasmkeep_server::apps::Manifest> {
        use wasmkeep_server::apps::Manifest;
        let key = format!("{base}/{version}/manifest");
        let body = match self.read(&key, MAX_MANIFEST).await {
            Read::Body(body) => body,
            Read::Absent if !current => return None,
            Read::Absent => {
                problems.push(problem(
                    spelled(&key),
                    "deploy",
                    "the current version has no manifest",
                ));
                return None;
            }
            Read::Failed(why, lost) => {
                problems.push(failed(spelled(&key), "deploy", why, lost));
                return None;
            }
        };
        let Some(manifest) = std::str::from_utf8(&body).ok().and_then(Manifest::parse) else {
            problems.push(problem(
                spelled(&key),
                "deploy",
                "the manifest does not parse (a newer WasmKeep's, or damaged)",
            ));
            return None;
        };
        let mut named: Vec<(String, &str, u64)> = Vec::new();
        for (class, digest) in &manifest.classes {
            named.push((
                format!("{base}/{version}/{class}.wasm"),
                digest,
                MAX_COMPONENT,
            ));
        }
        if let Some(digest) = &manifest.front {
            named.push((
                format!("{base}/{version}/_front.wasm"),
                digest,
                MAX_COMPONENT,
            ));
        }
        for (class, digest) in &manifest.config {
            named.push((
                format!("{base}/{version}/{class}.config"),
                digest,
                MAX_COMPONENT,
            ));
        }
        for (key, digest, cap) in named {
            self.digest(&key, digest, cap, problems).await;
        }
        if let Some(digest) = &manifest.assets {
            let key = format!("{base}/_assets/index/{digest}");
            let cap = wasmkeep_server::assets::MAX_INDEX as u64;
            if let Some(index) = self.digest(&key, digest, cap, problems).await {
                self.assets(base, &key, &index, contents, problems).await;
            }
        }
        Some(manifest)
    }

    /// Reads `key` and compares its SHA-256 with `digest`.
    async fn digest(
        &self,
        key: &str,
        digest: &str,
        cap: u64,
        problems: &mut Vec<Problem>,
    ) -> Option<bytes::Bytes> {
        match self.read(key, cap).await {
            Read::Body(body) if sha256(&body) == digest => Some(body),
            Read::Body(_) => {
                problems.push(problem(
                    spelled(key),
                    "deploy",
                    format!("its SHA-256 is not the {digest} its version names"),
                ));
                None
            }
            Read::Absent => {
                problems.push(problem(
                    spelled(key),
                    "deploy",
                    "named by its version, missing",
                ));
                None
            }
            Read::Failed(why, lost) => {
                problems.push(failed(spelled(key), "deploy", why, lost));
                None
            }
        }
    }

    /// The contents an assets index names: each listed with its size and,
    /// with `--deep`, read and its digest compared.
    async fn assets(
        &self,
        base: &str,
        index_key: &str,
        index: &[u8],
        contents: &mut Option<Objects>,
        problems: &mut Vec<Problem>,
    ) {
        let index = match wasmkeep_server::assets::Index::parse(index) {
            Ok(index) => index,
            Err(e) => {
                problems.push(problem(spelled(index_key), "deploy", e));
                return;
            }
        };
        let folder = format!("{base}/_assets/content/");
        if contents.is_none() {
            match history::list_objects(&self.store, &folder).await {
                Ok(listed) => *contents = Some(listed),
                Err(e) => {
                    problems.push(unread(spelled(&folder), "deploy", e.to_string()));
                    return;
                }
            }
        }
        let Some(listed) = contents.as_ref() else {
            return;
        };
        let mut checked = BTreeSet::new();
        for content in index.files.values().flat_map(|f| f.contents()) {
            if !checked.insert(content.digest.as_str()) {
                continue;
            }
            let key = format!("{folder}{}", content.digest);
            match listed.get(&content.digest) {
                None => problems.push(problem(
                    spelled(&key),
                    "deploy",
                    "a static asset its index names, missing",
                )),
                Some(meta) if meta.size != content.size => problems.push(problem(
                    spelled(&key),
                    "deploy",
                    format!(
                        "a static asset of {} bytes, {} in its index",
                        meta.size, content.size
                    ),
                )),
                Some(_) if self.deep => {
                    let cap = wasmkeep_server::assets::MAX_FILE;
                    self.digest(&key, &content.digest, cap, problems).await;
                }
                Some(_) => {}
            }
        }
    }

    /// One app, whole. An error ends its checks: what `report` counted
    /// until then stays.
    async fn app(&self, tenant: &str, app: &str, report: &mut AppReport) -> anyhow::Result<()> {
        let Some(view) = self.app_settings(tenant, app, true, report).await else {
            return Ok(());
        };
        let seen = self.histories(&view, report).await?;
        if self.sample.is_none() {
            self.records(&view, &seen, report).await?;
        } else {
            let folder = format!("{}/", view.root.bucket_base(Family::Owners));
            let mut listing = keys_under(&self.store, &folder);
            while listing.try_next().await?.is_some() {
                report.records += 1;
            }
        }
        self.alarms(&view, &seen, report).await?;
        let folder = format!("{}/", view.root.bucket_base(Family::Leases));
        let mut listing = keys_under(&self.store, &folder);
        while listing.try_next().await?.is_some() {
            report.lease_markers += 1;
        }
        drop(listing);
        self.generation_floor(&view, report).await;
        self.files(tenant, app, &view, report).await
    }

    /// The app's generation floor (ADR 0045): exact, and saying `markers
    /// none` only while no marker is left.
    async fn generation_floor(&self, view: &AppView, report: &mut AppReport) {
        use wasmkeep_cluster::generation_floor as gf;
        let key = format!("{}{}", view.root.prefix(), gf::key(&view.root));
        match self.read(&key, MAX_RECORD).await {
            Read::Body(body) => match gf::decode(Some(&body)) {
                Ok(floor) => {
                    let none = floor.markers == gf::Markers::None;
                    report.generation_floor = Some((floor.generation, none));
                    if none {
                        report.redundant_markers = report.lease_markers;
                    }
                }
                Err(e) => self.add([problem(spelled(&key), "ownership", e.to_string())]),
            },
            Read::Absent => {}
            Read::Failed(why, lost) => self.add([failed(spelled(&key), "ownership", why, lost)]),
        }
    }

    /// One actor of an app: its history and its record.
    async fn one_actor(
        &self,
        tenant: &str,
        app: &str,
        class: &str,
        name: &str,
        report: &mut AppReport,
    ) -> anyhow::Result<()> {
        let Some(view) = self.app_settings(tenant, app, false, report).await else {
            return Ok(());
        };
        let encoded = wasmkeep_storage::encode_segment(name);
        let actor = self.actor(&view, class, &encoded, None, true).await;
        if !actor.planned && actor.record_absent && actor.problems.is_empty() {
            self.add([problem(
                spelled(&format!(
                    "{}{}",
                    self.prefix,
                    wasmkeep_server::history_folder(&view.root, class, name)
                )),
                "history",
                "no such actor: no history and no ownership record",
            )]);
        }
        report.actors = u64::from(actor.planned);
        report.records = u64::from(actor.record_read);
        tally(report, &actor);
        self.add(actor.problems);
        Ok(())
    }

    /// Every actor with history: its chain, and (unless sampling) its
    /// record and with `--deep` its bodies. Returns the set of them.
    async fn histories(&self, view: &AppView, report: &mut AppReport) -> anyhow::Result<Seen> {
        let base = Key::from(view.root.bucket_base(Family::History));
        // As the bucket spells it, which is how listed keys compare.
        let folder = format!("{}/", base.as_ref());
        let full = self.sample.is_none();
        let mut seen = Seen::default();
        let mut sample = Reservoir::new(self.sample);
        let mut names = Vec::new();
        // A key that is no actor's history object (directly under
        // `actors/` or under a class: a sync tool's directory marker, a
        // stray file) is named and left out; the grouping shared with
        // `backup create` and `feed` refuses the whole listing at one.
        let strays: Arc<Mutex<(Vec<String>, u64)>> = Arc::default();
        let listing = {
            let (strays, folder) = (Arc::clone(&strays), folder.clone());
            // S3's paginated stream must not be polled again after its end.
            self.store.list(Some(&base)).fuse().filter(move |item| {
                let stray = item.as_ref().is_ok_and(|meta| {
                    meta.location
                        .as_ref()
                        .strip_prefix(folder.as_str())
                        .is_some_and(|rel| rel.splitn(3, '/').count() < 3)
                });
                if let (true, Ok(meta)) = (stray, item) {
                    let mut strays = strays.lock().unwrap_or_else(|p| p.into_inner());
                    strays.1 += 1;
                    if strays.0.len() < MAX_PROBLEMS {
                        strays.0.push(meta.location.as_ref().to_owned());
                    }
                }
                std::future::ready(!stray)
            })
        };
        let mut checks = group_listing(listing.boxed(), folder.clone())
            .map_ok(|(actor_prefix, group)| {
                let folder = &folder;
                async move {
                    // `<class>/<name>/`, the name as the store spells it.
                    let (class, name) = actor_prefix
                        .strip_prefix(folder.as_str())
                        .and_then(|rest| rest.trim_end_matches('/').split_once('/'))
                        .with_context(|| format!("{actor_prefix}: not an actor's folder"))?;
                    let found = |p: Problem| Actor {
                        problems: vec![p],
                        ..Actor::default()
                    };
                    let (encoded, actor) = match (crate::backup::object_segment(name), group) {
                        (Ok(encoded), Ok(objects)) => {
                            let actor =
                                self.actor(view, class, &encoded, Some(objects), full).await;
                            (encoded, actor)
                        }
                        // More keys than the listing budget holds: not checked.
                        (Ok(encoded), Err(e)) => {
                            let p = unread(actor_prefix.as_str(), "history", e.to_string());
                            (encoded, found(p))
                        }
                        (Err(_), _) => (
                            name.to_owned(),
                            found(problem(
                                actor_prefix.as_str(),
                                "history",
                                "the folder's name is not one a node writes for an actor",
                            )),
                        ),
                    };
                    Ok::<_, anyhow::Error>((class.to_owned(), encoded, actor))
                }
            })
            .try_buffer_unordered(CONCURRENCY);
        while let Some((class, encoded, actor)) = checks.try_next().await? {
            report.actors += 1;
            seen.insert(&class, &encoded);
            tally(report, &actor);
            self.add(actor.problems);
            if let Some(kept) = sample.offer()? {
                if kept < names.len() {
                    names[kept] = (class, encoded);
                } else {
                    names.push((class, encoded));
                }
            }
        }
        drop(checks);
        let (strays, total) =
            std::mem::take(&mut *strays.lock().unwrap_or_else(|p| p.into_inner()));
        let kept = strays.len() as u64;
        self.add(strays.into_iter().map(|key| {
            problem(
                key,
                "history",
                "not an actor's history object (a directory marker or a stray file?): \
                 `backup create` and `feed` stop at a key here",
            )
        }));
        if total > kept {
            self.add([problem(
                folder.as_str(),
                "history",
                format!(
                    "{} more keys that are no actor's history objects",
                    total - kept
                ),
            )]);
        }
        // The sample: each listed again, its record and its bodies read.
        let mut sampled = futures_util::stream::iter(
            names
                .iter()
                .map(|(class, encoded)| self.actor(view, class, encoded, None, true)),
        )
        .buffer_unordered(CONCURRENCY);
        while let Some(actor) = sampled.next().await {
            report.records_read += u64::from(actor.record_read);
            report.without_record += u64::from(actor.record_absent);
            report.deep_checked += u64::from(actor.deep);
            report.past_recorded_head += u64::from(actor.past_recorded_head);
            self.add(actor.problems);
        }
        Ok(seen)
    }

    /// One actor, checked again on a fresh listing while a check finds a
    /// problem: what the last attempt found stands.
    async fn actor(
        &self,
        view: &AppView,
        class: &str,
        encoded: &str,
        listed: Option<Objects>,
        full: bool,
    ) -> Actor {
        let folder = format!(
            "{}/",
            view.root
                .bucket_key(Family::History, &format!("{class}/{encoded}"))
        );
        let mut listed = listed;
        let mut actor = Actor::default();
        for attempt in 0..ATTEMPTS {
            let objects = match listed.take() {
                Some(objects) => objects,
                None => match history::list_objects(&self.store, &folder).await {
                    Ok(objects) => objects,
                    Err(e) => {
                        return Actor {
                            problems: vec![unread(spelled(&folder), "history", e.to_string())],
                            ..Actor::default()
                        };
                    }
                },
            };
            actor = self
                .actor_once(view, class, encoded, &folder, &objects, full)
                .await;
            if !actor.again || attempt + 1 == ATTEMPTS {
                break;
            }
            tokio::time::sleep(AGAIN_AFTER).await;
        }
        actor
    }

    async fn actor_once(
        &self,
        view: &AppView,
        class: &str,
        encoded: &str,
        folder: &str,
        objects: &Objects,
        full: bool,
    ) -> Actor {
        let spelled_folder = spelled(folder);
        let at = |rel: &str| format!("{spelled_folder}/{rel}");
        let mut actor = Actor {
            objects: objects.len() as u64,
            bytes: objects.values().map(|m| m.size).sum(),
            ..Actor::default()
        };
        let listing = Listing::from_keys(objects.keys().map(String::as_str));
        let plan = match plan_restore(&listing) {
            Ok(plan) => Some(plan),
            Err(PlanError::Missing { missing }) => {
                let built_on_it = listing
                    .segments
                    .iter()
                    .find(|s| s.parent() == missing)
                    .map(|s| format!(": {} is built on it", at(&segment_key(s))))
                    .unwrap_or_default();
                actor.problems.push(problem(
                    at(&format!(
                        "wal/{:016x}-{:016x}-*.ltx",
                        missing.txid, missing.generation
                    )),
                    "history",
                    format!(
                        "the segment of transaction {} (generation {}) is missing from the \
                         chain to the head, and no snapshot stands in for it{built_on_it}",
                        missing.txid, missing.generation
                    ),
                ));
                None
            }
            Err(e @ PlanError::Duplicate { at: position }) => {
                actor.problems.push(problem(
                    at(&format!(
                        "wal/{:016x}-{:016x}-*.ltx",
                        position.txid, position.generation
                    )),
                    "history",
                    e.to_string(),
                ));
                None
            }
        };
        // The objects a restore reads, in its order.
        let mut chain: Vec<(String, SegmentId, bool)> = Vec::new();
        if let Some(plan) = &plan {
            actor.planned = plan.head() != EMPTY;
            if let Some(id) = plan.snapshot {
                chain.push((snapshot_key(id), id, true));
            }
            for id in &plan.segments {
                if let Some(segment) = listing.segments.iter().find(|s| s.id == *id) {
                    chain.push((segment_key(segment), *id, false));
                }
            }
            let least =
                (wasmkeep_storage::ltx::HEADER_SIZE + wasmkeep_storage::ltx::TRAILER_SIZE) as u64;
            for (rel, _, _) in &chain {
                if let Some(meta) = objects.get(rel).filter(|m| m.size < least) {
                    actor.problems.push(problem(
                        meta.location.as_ref(),
                        "history",
                        format!(
                            "{} bytes: shorter than the header and trailer of a history object",
                            meta.size
                        ),
                    ));
                }
            }
        }
        actor.again = !actor.problems.is_empty();
        if !full {
            return actor;
        }
        let mut actor = self
            .actor_full(view, class, encoded, objects, &listing, plan, &chain, actor)
            .await;
        actor.again |= actor.problems.iter().any(|p| !p.unread);
        actor
    }

    /// The part of an actor's check that reads: its record, with `--deep`
    /// its bodies, and its recorded head.
    #[allow(clippy::too_many_arguments)]
    async fn actor_full(
        &self,
        view: &AppView,
        class: &str,
        encoded: &str,
        objects: &Objects,
        listing: &Listing,
        plan: Option<wasmkeep_cluster::replication::Plan>,
        chain: &[(String, SegmentId, bool)],
        mut actor: Actor,
    ) -> Actor {
        let history_whole = actor.problems.is_empty();
        // The record, read after the listing: its generation only grows.
        let owner_key = view
            .root
            .bucket_key(Family::Owners, &format!("{class}/{encoded}"));
        let record = match self.read(&owner_key, MAX_RECORD).await {
            Read::Body(body) => {
                actor.record_read = true;
                match OwnerObject::decode(&body) {
                    Some(OwnerObject::Record(record)) => Some(record),
                    Some(OwnerObject::Tombstone { .. }) => None,
                    None => {
                        actor.problems.push(problem(
                            spelled(&owner_key),
                            "ownership",
                            "the ownership record does not decode",
                        ));
                        None
                    }
                }
            }
            Read::Absent => {
                actor.record_absent = true;
                None
            }
            Read::Failed(why, lost) => {
                actor
                    .problems
                    .push(failed(spelled(&owner_key), "ownership", why, lost));
                None
            }
        };
        let Some(plan) = plan else {
            return actor;
        };
        // With --deep: each body, and the database checksum after it.
        let mut sums: BTreeMap<SegmentId, u64> = BTreeMap::new();
        if self.deep && history_whole {
            actor.deep = true;
            let mut before = None;
            for (rel, id, snapshot) in chain {
                let Some(meta) = objects.get(rel) else {
                    continue;
                };
                match self.body(meta, *id, *snapshot).await {
                    Ok((pre, post)) => {
                        if before.is_some_and(|b| b != pre) {
                            actor.problems.push(problem(
                                meta.location.as_ref(),
                                "history",
                                "does not follow the object before it: its pre-apply checksum \
                                 is not the database checksum that object leaves",
                            ));
                            break;
                        }
                        before = Some(post);
                        sums.insert(*id, post);
                    }
                    Err(fault) => {
                        actor.again |= fault.changed;
                        actor.problems.push(failed(
                            meta.location.as_ref(),
                            "history",
                            fault.why,
                            fault.unread,
                        ));
                        break;
                    }
                }
            }
        }
        let Some(record) = record else {
            return actor;
        };
        use std::sync::atomic::Ordering;
        let mut records_heads = view.recorded_head;
        if view.enabled_since.load(Ordering::Relaxed) {
            records_heads = Some(true);
        }
        if record.recorded.is_some() && records_heads == Some(false) {
            // The setting may have been written since it was read
            // (`wasmkeep app format --enable`): read it again.
            let (now, found) = self.format_setting(&view.root).await;
            actor.problems.extend(found);
            records_heads = now;
            match now {
                Some(true) => view.enabled_since.store(true, Ordering::Relaxed),
                Some(false) => {
                    actor.problems.push(problem(
                        spelled(&owner_key),
                        "ownership",
                        "a version 2 ownership record in an app that has not enabled the \
                         recorded-head format: no node takes this actor",
                    ));
                    return actor;
                }
                None => return actor,
            }
        }
        if records_heads != Some(true) || !actor.problems.is_empty() {
            return actor;
        }
        // ADR 0035, section 3, as a restore proves it.
        let floor = Requirement::of_record(&record);
        let proving: Vec<(SegmentId, Option<u64>)> = chain
            .iter()
            .map(|(_, id, _)| (*id, sums.get(id).copied()))
            .collect();
        let Some(Requirement {
            head: Position::Known {
                generation, txid, ..
            },
            completeness,
            ..
        }) = floor
        else {
            return actor;
        };
        let recorded = SegmentId { generation, txid };
        match prove(floor.as_ref(), &proving) {
            Proof::Proven | Proof::NeedsReplay => {}
            Proof::Conflict => actor.problems.push(problem(
                spelled(&owner_key),
                "recorded-head",
                format!(
                    "the history at the recorded head (transaction {txid}, generation \
                     {generation}) has another checksum than the record's"
                ),
            )),
            Proof::Missing => {
                let linked = plan
                    .snapshot
                    .is_some_and(|base| linked_back(floor.as_ref(), base, listing));
                if !linked {
                    actor.problems.push(problem(
                        spelled(&owner_key),
                        "recorded-head",
                        format!(
                            "the recorded head (transaction {txid}, generation {generation}) \
                             is not on the chain the bucket holds, which ends at transaction \
                             {} (generation {})",
                            plan.head().txid,
                            plan.head().generation
                        ),
                    ));
                }
            }
        }
        actor.past_recorded_head = actor.problems.is_empty()
            && record.holder.is_none()
            && completeness == wasmkeep_cluster::recorded_head::Completeness::Complete
            && plan.head() != recorded;
        actor
    }

    /// Reads one history object as listed and checks it: the checksums
    /// before and after it, or why it did not pass.
    async fn body(
        &self,
        meta: &ObjectMeta,
        id: SegmentId,
        snapshot: bool,
    ) -> Result<(u64, u64), Fault> {
        let fault = |why: String, unread: bool, changed: bool| Fault {
            why,
            unread,
            changed,
        };
        let mib = u32::try_from(meta.size.div_ceil(1 << 20))
            .unwrap_or(BODY_BUDGET)
            .clamp(1, BODY_BUDGET);
        let _held = self
            .budget
            .acquire_many(mib)
            .await
            .map_err(|e| fault(e.to_string(), true, false))?;
        let bytes = self
            .get(&meta.location, Some(meta), MAX_OBJECT)
            .await
            .map_err(|e| match &e {
                history::Error::Missing(_) => fault(
                    "listed, but the bucket has no such object".into(),
                    false,
                    true,
                ),
                history::Error::Stale(_) => fault(
                    "it kept changing while it was read: not checked".into(),
                    true,
                    true,
                ),
                _ => fault(e.to_string(), lost(&e), false),
            })?;
        tokio::task::spawn_blocking(move || history::check_object(&bytes, id, snapshot))
            .await
            .map_err(|e| fault(e.to_string(), true, false))?
            .map_err(|e| fault(e.to_string(), false, false))
    }

    /// The ownership records of actors without history (those with one
    /// were read with it).
    async fn records(
        &self,
        view: &AppView,
        seen: &Seen,
        report: &mut AppReport,
    ) -> anyhow::Result<()> {
        let folder = format!("{}/", view.root.bucket_base(Family::Owners));
        let (mut listed, mut skipped) = (0u64, 0u64);
        let mut checks = keys_under(&self.store, &folder)
            .map_ok(|(rel, _)| {
                // `<class>/<name>`, the name as the store spells it; any
                // other key is nothing of WasmKeep's, and is left alone.
                let record = rel.split_once('/').and_then(|(class, name)| {
                    let encoded = crate::backup::object_segment(name).ok()?;
                    (!name.contains('/')).then(|| (class.to_owned(), encoded))
                });
                match record {
                    Some(_) => listed += 1,
                    None => skipped += 1,
                }
                async move {
                    let Some((class, encoded)) = record else {
                        return Ok(None);
                    };
                    let class = class.as_str();
                    if seen.has(class, &encoded) {
                        return Ok(None);
                    }
                    // The set had no room for every actor: one with
                    // history was checked with it, not here again.
                    if seen.full {
                        let history = view
                            .root
                            .bucket_key(Family::History, &format!("{class}/{encoded}"));
                        if self.any_under(&format!("{history}/")).await? {
                            return Ok(None);
                        }
                    }
                    // Its history, listed now: none, or one that started
                    // since (or that the set had no room for).
                    let mut actor = self.actor(view, class, &encoded, None, true).await;
                    if actor.planned {
                        // Counted with its history, or not there then.
                        actor.objects = 0;
                        actor.bytes = 0;
                    }
                    Ok::<_, anyhow::Error>(Some(actor))
                }
            })
            .try_buffer_unordered(CONCURRENCY);
        while let Some(actor) = checks.try_next().await? {
            let Some(actor) = actor else { continue };
            report.records_read += u64::from(actor.record_read);
            report.deep_checked += u64::from(actor.deep);
            self.add(actor.problems);
        }
        drop(checks);
        (report.records, report.records_skipped) = (listed, skipped);
        Ok(())
    }

    /// The alarm index: each entry names an actor the bucket knows.
    async fn alarms(
        &self,
        view: &AppView,
        seen: &Seen,
        report: &mut AppReport,
    ) -> anyhow::Result<()> {
        let folder = format!("{}/", view.root.bucket_base(Family::Alarms));
        let (mut entries, mut skipped) = (0u64, 0u64);
        let mut checks = keys_under(&self.store, &folder)
            .map_ok(|(rel, meta)| {
                let entry =
                    alarm_index::parse_listed(&rel).map(|l| (l.class.to_owned(), l.name.clone()));
                match entry {
                    Some(_) => entries += 1,
                    None => skipped += 1,
                }
                async move {
                    let Some((class, name)) = entry else {
                        return Ok(None);
                    };
                    let encoded = wasmkeep_storage::encode_segment(&name);
                    if seen.has(&class, &encoded) {
                        return Ok(None);
                    }
                    Ok::<_, anyhow::Error>(
                        self.orphan_alarm(view, &class, &name, &encoded, &meta)
                            .await,
                    )
                }
            })
            .try_buffer_unordered(CONCURRENCY);
        while let Some(found) = checks.try_next().await? {
            self.add(found);
        }
        drop(checks);
        (report.alarms, report.alarms_skipped) = (entries, skipped);
        Ok(())
    }

    /// An entry whose actor was not seen with history: a problem unless
    /// the actor has an ownership record or history now, or the entry is
    /// gone again.
    async fn orphan_alarm(
        &self,
        view: &AppView,
        class: &str,
        name: &str,
        encoded: &str,
        entry: &ObjectMeta,
    ) -> Option<Problem> {
        let tail = format!("{class}/{encoded}");
        let owner_key = view.root.bucket_key(Family::Owners, &tail);
        let folder = format!("{}/", view.root.bucket_key(Family::History, &tail));
        for attempt in 0..ATTEMPTS {
            if attempt > 0 {
                tokio::time::sleep(AGAIN_AFTER).await;
            }
            match self.read(&owner_key, MAX_RECORD).await {
                Read::Absent => {}
                Read::Body(_) => return None,
                Read::Failed(why, lost) => {
                    return Some(failed(spelled(&owner_key), "ownership", why, lost));
                }
            }
            match self.any_under(&folder).await {
                Ok(false) => {}
                Ok(true) => return None,
                Err(e) => return Some(unread(spelled(&folder), "history", format!("{e:#}"))),
            }
            match self.get(&entry.location, None, MAX_RECORD).await {
                Err(history::Error::Missing(_)) => return None,
                Err(e) if lost(&e) => {
                    return Some(unread(entry.location.as_ref(), "alarm", e.to_string()));
                }
                _ => {}
            }
        }
        Some(problem(
            entry.location.as_ref(),
            "alarm",
            format!(
                "the entry names the actor {class}/{name}, which has no history and no \
                 ownership record"
            ),
        ))
    }

    /// Files: the listing by epoch, the last count, the sweep records.
    async fn files(
        &self,
        tenant: &str,
        app: &str,
        view: &AppView,
        report: &mut AppReport,
    ) -> anyhow::Result<()> {
        let state = format!("{}{}", self.prefix, view.root.bucket());
        let base = format!("{}apps/{tenant}/{app}", self.prefix);
        let mut markers = Vec::new();
        for marker in ["recreated", "restored"] {
            markers.push(
                match self.read(&format!("{base}/{marker}"), MAX_RECORD).await {
                    Read::Body(body) => Some(body),
                    Read::Absent => None,
                    Read::Failed(why, lost) => {
                        let key = spelled(&format!("{base}/{marker}"));
                        self.add([failed(key, "files", why, lost)]);
                        None
                    }
                },
            );
        }
        let epoch = wasmkeep_server::blobs::epoch(markers[0].as_deref(), markers[1].as_deref());
        let mut listing = keys_under(&self.store, &format!("{state}blobs/"));
        while let Some((rel, meta)) = listing.try_next().await? {
            if file_epoch(&rel) == Some(epoch.as_str()) {
                report.files += 1;
                report.file_bytes += meta.size;
            } else {
                report.files_elsewhere += 1;
            }
        }
        drop(listing);
        let key = format!("{state}{}", wasmkeep_server::blobs::USAGE_KEY);
        match self.read(&key, MAX_RECORD).await {
            Read::Body(body) => match wasmkeep_server::blobs::decode_usage(&body) {
                Some(count) => report.usage = Some(count),
                None => self.add([problem(
                    spelled(&key),
                    "files",
                    "the count of stored files does not decode",
                )]),
            },
            Read::Absent => {}
            Read::Failed(why, lost) => self.add([failed(spelled(&key), "files", why, lost)]),
        }
        let folder = format!("{state}blob-sweeps/");
        let mut checks = keys_under(&self.store, &folder)
            .map_ok(|(rel, meta)| async move {
                Ok::<_, anyhow::Error>(self.sweep_record(&rel, &meta).await)
            })
            .try_buffer_unordered(CONCURRENCY);
        while let Some(found) = checks.try_next().await? {
            report.sweep_records += 1;
            self.add(found);
        }
        Ok(())
    }

    /// A sweep record: it decodes, and names the actor its key names.
    async fn sweep_record(&self, rel: &str, meta: &ObjectMeta) -> Option<Problem> {
        // A key nothing of WasmKeep wrote is left alone, as the sweep does.
        let (_, name_id, _) = blob_sweep::parse_key(rel)?;
        let body = match self.get(&meta.location, None, MAX_RECORD).await {
            Ok(body) => body,
            // Dropped since it was listed.
            Err(history::Error::Missing(_)) => return None,
            Err(e) => {
                let key = meta.location.as_ref();
                return Some(failed(key, "files", e.to_string(), lost(&e)));
            }
        };
        match blob_sweep::Record::decode(&body) {
            None => Some(problem(
                meta.location.as_ref(),
                "files",
                "the sweep record does not decode",
            )),
            Some(record) if sha256(record.name.as_bytes()) != name_id => Some(problem(
                meta.location.as_ref(),
                "files",
                format!(
                    "the sweep record names the actor {:?}, not the one its key names",
                    record.name
                ),
            )),
            Some(_) => None,
        }
    }

    /// The node leases: each decodes. Counts in `report` how many do, and
    /// how many heartbeats are in an older format. The mark a `cluster
    /// forget` leaves at `nodes/<id>` (ADR 0043) is not a node and not a
    /// problem; one older than `cluster status` allows is a note, in the
    /// same words: its command did not finish.
    async fn nodes(&self, report: &mut Report) -> anyhow::Result<()> {
        let folder = format!("{}nodes/", self.prefix);
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX));
        let mut checks = keys_under(&self.store, &folder)
            .map_ok(|(id, meta)| async move {
                let body = match self.get(&meta.location, None, MAX_RECORD).await {
                    Ok(body) => body,
                    // Forgotten since it was listed.
                    Err(history::Error::Missing(_)) => return Ok((0, 0, None, None)),
                    Err(e) => {
                        let key = meta.location.as_ref();
                        let p = failed(key, "node", e.to_string(), lost(&e));
                        return Ok((0, 0, Some(p), None));
                    }
                };
                if let Some(at_ms) = wasmkeep_cluster::node_lease::forgotten(&body) {
                    let unfinished = now_ms.saturating_sub(at_ms) > FORGET_UNFINISHED_MS;
                    let note = unfinished.then(|| {
                        format!(
                            "{}: forget did not finish; run `wasmkeep cluster forget {id}` \
                             (no node starts under that id until then)",
                            meta.location
                        )
                    });
                    return Ok((0, 0, None, note));
                }
                Ok::<_, anyhow::Error>(if NodeLeaseObject::decode(&body).is_some() {
                    (1, 0, None, None)
                } else if wasmkeep_cluster::membership::NodeInfo::decode(&body).is_some() {
                    (0, 1, None, None)
                } else {
                    let p = problem(
                        meta.location.as_ref(),
                        "node",
                        "does not decode as a node lease or a heartbeat",
                    );
                    (0, 0, Some(p), None)
                })
            })
            .try_buffer_unordered(CONCURRENCY);
        while let Some((lease, older, found, note)) = checks.try_next().await? {
            report.nodes += lease;
            report.nodes_old_format += older;
            self.add(found);
            report.notes.extend(note);
        }
        report.notes.sort();
        Ok(())
    }
}

/// The epoch of a file's key under `blobs/`,
/// `<epoch>/<class>/<name id>/<incarnation>/<key>` or
/// `<epoch>/_front/main/0/<key>`; `None` for any other key.
fn file_epoch(rel: &str) -> Option<&str> {
    let mut parts = rel.splitn(5, '/');
    let (epoch, class, id, incarnation, key) = (
        parts.next()?,
        parts.next()?,
        parts.next()?,
        parts.next()?,
        parts.next()?,
    );
    let front = (class, id, incarnation) == ("_front", "main", "0");
    let actor = blob_sweep::parse_key(&format!("{class}/{id}/{incarnation}")).is_some();
    (blob_sweep::is_epoch(epoch) && (front || actor) && !key.is_empty()).then_some(epoch)
}

fn tally(report: &mut AppReport, actor: &Actor) {
    // With nothing found, or with only its record unread, it restores.
    report.restorable += u64::from(
        actor.planned
            && actor
                .problems
                .iter()
                .all(|p| p.unread && p.check != "history"),
    );
    report.records_read += u64::from(actor.record_read);
    report.without_record += u64::from(actor.record_absent);
    report.deep_checked += u64::from(actor.deep);
    report.past_recorded_head += u64::from(actor.past_recorded_head);
    report.history_objects += actor.objects;
    report.history_bytes += actor.bytes;
}

/// The actors seen with history, up to [`MAX_SEEN`].
#[derive(Debug, Default)]
struct Seen {
    ids: HashSet<[u8; 16]>,
    /// An actor was left out: not being in the set says nothing.
    full: bool,
}

impl Seen {
    fn insert(&mut self, class: &str, encoded: &str) {
        if self.ids.len() < MAX_SEEN {
            self.ids.insert(seen_id(class, encoded));
        } else {
            self.full = true;
        }
    }

    fn has(&self, class: &str, encoded: &str) -> bool {
        self.ids.contains(&seen_id(class, encoded))
    }
}

/// A uniform sample of a stream of unknown length (algorithm R).
struct Reservoir {
    size: u64,
    offered: u64,
}

impl Reservoir {
    fn new(size: Option<u64>) -> Self {
        Self {
            size: size.unwrap_or(0),
            offered: 0,
        }
    }

    /// Whether the next item is kept, and in which slot.
    fn offer(&mut self) -> anyhow::Result<Option<usize>> {
        self.offered += 1;
        if self.size == 0 {
            return Ok(None);
        }
        if self.offered <= self.size {
            return Ok(Some(usize::try_from(self.offered - 1)?));
        }
        let mut bytes = [0u8; 8];
        getrandom::fill(&mut bytes).map_err(|e| anyhow::anyhow!("OS randomness: {e}"))?;
        let slot = u64::from_le_bytes(bytes) % self.offered;
        Ok(if slot < self.size {
            Some(usize::try_from(slot)?)
        } else {
            None
        })
    }
}

#[cfg(test)]
mod tests;
