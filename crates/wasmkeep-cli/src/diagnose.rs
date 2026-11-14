//! `wasmkeep cluster diagnose`: whether a bucket does what WasmKeep needs
//! from it, checked through the same bucket code the nodes run
//! ([`ObjectStoreBucket`]), under a scratch prefix it creates and always
//! empties afterwards.
//!
//! Leases and ownership records are safe only if the bucket honours
//! conditional writes; failover, the alarm index and the migration read
//! listings and last-modified times. A store that accepts a condition but
//! ignores it loses data without an error, so each guarantee is tried
//! here, on throwaway keys, and measured.

use std::cell::RefCell;
use std::collections::BTreeSet;
use std::time::{Duration, Instant};

use clap::Args;
use futures_util::future::join_all;
use wasmkeep_cluster::bucket_check::{CheckError, check_conditional_writes};
use wasmkeep_cluster::lease::{Bucket, BucketError, LeaseConfig, Version};
use wasmkeep_cluster::migrate::Store;
use wasmkeep_cluster::object_store::ObjectStoreBucket;

#[derive(Debug, Args)]
pub(crate) struct DiagnoseArgs {
    /// Bucket to check: s3://BUCKET[/PREFIX] (credentials from the AWS_*
    /// environment variables, as for `serve`). Nothing outside the scratch
    /// prefix is written, read or deleted.
    #[arg(long)]
    bucket: String,

    /// Where under PREFIX the checks write: each run writes in its own
    /// run-<ms>-<pid>/ folder below it, deleted afterwards, so two runs
    /// never share one. [default: diagnose]
    #[arg(long, value_name = "PREFIX")]
    prefix_for_test: Option<String>,

    /// Skip the listing of 1,001 keys (1,001 small writes and as many
    /// deletes).
    #[arg(long)]
    skip_paging: bool,

    /// Print JSON instead of lines, for scripts.
    #[arg(long)]
    json: bool,
}

/// How one check came out.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Status {
    /// The bucket does what WasmKeep needs.
    Pass,
    /// It does, with a measured value worth knowing (a listing that lags).
    Warn,
    /// It broke a guarantee WasmKeep relies on: not safe.
    Fail,
    /// The check could not finish (no answer, an unexpected error).
    Error,
    /// Not run (by request).
    Skipped,
}

impl Status {
    fn label(self) -> &'static str {
        match self {
            Self::Pass => "pass",
            Self::Warn => "warn",
            Self::Fail => "FAIL",
            Self::Error => "error",
            Self::Skipped => "skip",
        }
    }
}

/// One check's outcome: its status and what was measured.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Check {
    pub(crate) name: &'static str,
    pub(crate) status: Status,
    pub(crate) value: String,
}

/// What a run measures against, and how long it waits.
#[derive(Debug, Clone)]
pub(crate) struct Options {
    /// The longest a listing may take to show a write or a delete.
    pub(crate) list_wait: Duration,
    /// The size of the large object.
    pub(crate) large_bytes: usize,
    /// Keys written for the paging check (`None` skips it).
    pub(crate) paging_keys: Option<usize>,
    /// The pause between writes whose last-modified times must differ
    /// (more than a second: stores may keep whole seconds).
    pub(crate) modified_gap: Duration,
    /// The lease timings the clock is checked against.
    pub(crate) lease: LeaseConfig,
    /// The longest one check may take, and the cleanup.
    pub(crate) check_timeout: Duration,
    /// How long to wait, after the cleanup, for writes that a check cut
    /// by its timeout had in flight, before listing once more.
    pub(crate) settle: Duration,
}

impl Options {
    pub(crate) fn production(skip_paging: bool) -> Self {
        Self {
            list_wait: Duration::from_secs(10),
            large_bytes: 16 << 20,
            paging_keys: (!skip_paging).then_some(1_001),
            modified_gap: Duration::from_millis(1_100),
            lease: LeaseConfig::default_safe(),
            check_timeout: Duration::from_secs(120),
            settle: Duration::from_secs(5),
        }
    }
}

/// What the checks need from a bucket: the migration's view (listings
/// with last-modified times, plain writes and deletes), the lease view
/// (conditional writes) and a head's last-modified time, as `cluster
/// fence` reads it.
#[allow(async_fn_in_trait)]
pub(crate) trait Target: Store {
    /// The object's last-modified time from a head request (ms).
    async fn head_modified_ms(&self, key: &str) -> Result<Option<u64>, BucketError>;
}

impl Target for ObjectStoreBucket {
    async fn head_modified_ms(&self, key: &str) -> Result<Option<u64>, BucketError> {
        self.modified_ms(key).await
    }
}

/// The run's result: every check, then the cleanup.
#[derive(Debug, Clone)]
pub(crate) struct Report {
    pub(crate) checks: Vec<Check>,
    /// Objects found under the scratch prefix after cleanup (`None`: the
    /// listing failed).
    pub(crate) left: Option<usize>,
    /// Why the cleanup did not finish, if it did not.
    pub(crate) cleanup_error: Option<String>,
}

/// The verdict and its exit code.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Verdict {
    Safe,
    Unsafe(Vec<&'static str>),
    Incomplete(Vec<&'static str>),
}

impl Verdict {
    pub(crate) fn code(&self) -> i32 {
        match self {
            Self::Safe => 0,
            Self::Unsafe(_) => 1,
            Self::Incomplete(_) => 2,
        }
    }

    fn text(&self) -> String {
        match self {
            Self::Safe => "safe for WasmKeep".into(),
            Self::Unsafe(names) => format!("not safe: {} failed", names.join(", ")),
            Self::Incomplete(names) => format!(
                "could not finish: {} did not complete (no verdict)",
                names.join(", ")
            ),
        }
    }
}

impl Report {
    /// Whether the scratch prefix was left empty.
    fn cleaned(&self) -> bool {
        self.left == Some(0) && self.cleanup_error.is_none()
    }

    /// The exit code: the verdict's, but 2 for a safe verdict whose
    /// cleanup failed (objects are left to delete by hand).
    pub(crate) fn exit_code(&self) -> i32 {
        match self.verdict().code() {
            0 if !self.cleaned() => 2,
            code => code,
        }
    }

    pub(crate) fn verdict(&self) -> Verdict {
        let named = |s: Status| -> Vec<&'static str> {
            self.checks
                .iter()
                .filter(|c| c.status == s)
                .map(|c| c.name)
                .collect()
        };
        let failed = named(Status::Fail);
        if !failed.is_empty() {
            return Verdict::Unsafe(failed);
        }
        let errors = named(Status::Error);
        if !errors.is_empty() {
            return Verdict::Incomplete(errors);
        }
        Verdict::Safe
    }

    fn cleanup_text(&self, scratch_url: &str) -> String {
        match (self.left, &self.cleanup_error) {
            (Some(0), None) => format!("cleanup: {scratch_url} is empty again"),
            (left, why) => format!(
                "cleanup FAILED: {} left under {scratch_url}{}; delete them by hand",
                left.map_or_else(|| "objects may be".to_owned(), |n| format!("{n} object(s)")),
                why.as_ref().map(|w| format!(" ({w})")).unwrap_or_default()
            ),
        }
    }

    pub(crate) fn to_text(&self, bucket_url: &str, scratch_url: &str) -> String {
        let mut out = format!("Bucket {bucket_url}, writing only under {scratch_url}\n\n");
        let width = self.checks.iter().map(|c| c.name.len()).max().unwrap_or(0);
        for c in &self.checks {
            out.push_str(&format!(
                "{:<5}  {:<width$}  {}\n",
                c.status.label(),
                c.name,
                c.value
            ));
        }
        out.push('\n');
        out.push_str(&self.cleanup_text(scratch_url));
        out.push('\n');
        out.push_str(&self.verdict().text());
        out.push('\n');
        out
    }

    pub(crate) fn to_json(&self, bucket_url: &str, scratch_url: &str) -> serde_json::Value {
        let verdict = self.verdict();
        serde_json::json!({
            "bucket": bucket_url,
            "scratch": scratch_url,
            "checks": self.checks.iter().map(|c| serde_json::json!({
                "name": c.name,
                "status": match c.status {
                    Status::Pass => "pass",
                    Status::Warn => "warn",
                    Status::Fail => "fail",
                    Status::Error => "error",
                    Status::Skipped => "skipped",
                },
                "value": c.value,
            })).collect::<Vec<_>>(),
            "cleanup": {
                "ok": self.cleaned(),
                "left": self.left,
                "error": self.cleanup_error,
            },
            "verdict": match verdict {
                Verdict::Safe => "safe",
                Verdict::Unsafe(_) => "unsafe",
                Verdict::Incomplete(_) => "incomplete",
            },
            "summary": verdict.text(),
            "exit_code": self.exit_code(),
        })
    }
}

/// Why a run did not start.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum NotRun {
    /// The scratch prefix already holds objects: never written over.
    NotEmpty(usize),
    /// The bucket did not answer a listing of the scratch prefix.
    Unreachable(String),
}

/// A bucket that remembers every key a check tried to write, so the
/// cleanup deletes them even if a listing misses some.
struct Tracked<'a, T> {
    inner: &'a T,
    written: RefCell<BTreeSet<String>>,
}

impl<T> Tracked<'_, T> {
    fn note(&self, key: &str) {
        self.written.borrow_mut().insert(key.to_owned());
    }
}

impl<T: Target> Tracked<'_, T> {
    async fn get(&self, key: &str) -> Result<Option<(Vec<u8>, Version)>, BucketError> {
        Bucket::get(self.inner, key).await
    }
    async fn get_owned(&self, key: String) -> Result<Option<(Vec<u8>, Version)>, BucketError> {
        self.get(&key).await
    }
    async fn create(&self, key: &str, body: Vec<u8>) -> Result<Version, BucketError> {
        self.note(key);
        self.inner.create(key, body).await
    }
    async fn replace(
        &self,
        key: &str,
        body: Vec<u8>,
        expected: &Version,
    ) -> Result<Version, BucketError> {
        self.note(key);
        self.inner.replace(key, body, expected).await
    }
    async fn put(&self, key: &str, body: Vec<u8>) -> Result<(), BucketError> {
        self.note(key);
        Store::put(self.inner, key, body).await
    }
    async fn delete(&self, key: &str) -> Result<(), BucketError> {
        Store::delete(self.inner, key).await
    }
    /// Keys and last-modified times under `prefix`, with sizes.
    async fn list(&self, prefix: &str) -> Result<Vec<(String, u64, u64)>, BucketError> {
        Ok(Store::list(self.inner, prefix)
            .await?
            .into_iter()
            .map(|m| (m.key, m.modified_ms, m.size))
            .collect())
    }
}

impl<T: Target> Bucket for Tracked<'_, T> {
    async fn get(&self, key: &str) -> Result<Option<(Vec<u8>, Version)>, BucketError> {
        Tracked::get(self, key).await
    }
    async fn create(&self, key: &str, body: Vec<u8>) -> Result<Version, BucketError> {
        Tracked::create(self, key, body).await
    }
    async fn replace(
        &self,
        key: &str,
        body: Vec<u8>,
        expected: &Version,
    ) -> Result<Version, BucketError> {
        Tracked::replace(self, key, body, expected).await
    }
}

fn now_ms() -> u64 {
    crate::status::now_ms()
}

/// An error outcome for a check that could not finish.
fn error(name: &'static str, step: &str, e: &BucketError) -> Check {
    Check {
        name,
        status: Status::Error,
        value: format!("{step}: {e}"),
    }
}

fn check(name: &'static str, status: Status, value: impl Into<String>) -> Check {
    Check {
        name,
        status,
        value: value.into(),
    }
}

/// `?` for a check: an unavailable bucket ends it as an error.
macro_rules! try_or {
    ($name:expr, $step:expr, $e:expr) => {
        match $e {
            Ok(v) => v,
            Err(e) => return error($name, $step, &e),
        }
    };
}

/// Runs every check against `target`, whose keys are all under the
/// scratch prefix, then deletes everything written. `Err` if it did not
/// start (nothing written).
pub(crate) async fn run<T: Target>(target: &T, options: &Options) -> Result<Report, NotRun> {
    match tokio::time::timeout(options.check_timeout, Store::list(target, "")).await {
        Ok(Ok(found)) if !found.is_empty() => return Err(NotRun::NotEmpty(found.len())),
        Ok(Ok(_)) => {}
        Ok(Err(e)) => return Err(NotRun::Unreachable(e.to_string())),
        Err(_) => {
            return Err(NotRun::Unreachable(format!(
                "no answer within {} s",
                options.check_timeout.as_secs_f64()
            )));
        }
    }
    let t = Tracked {
        inner: target,
        written: RefCell::new(BTreeSet::new()),
    };
    let mut checks = Vec::new();
    // A check cut by its timeout may have writes in flight: they can land
    // after the cleanup.
    let mut cut = false;
    let late = || format!("no answer within {} s", options.check_timeout.as_secs_f64());
    // Each check is bounded and returns its outcome: none can skip the
    // cleanup below.
    macro_rules! bounded {
        ($name:expr, $fut:expr) => {{
            let outcome = match tokio::time::timeout(options.check_timeout, $fut).await {
                Ok(c) => c,
                Err(_) => {
                    cut = true;
                    check($name, Status::Error, late())
                }
            };
            checks.push(outcome);
        }};
    }
    bounded!("start-check", start_check(&t));
    bounded!("create-if-absent", create_if_absent(&t));
    bounded!("concurrent-creates", concurrent_creates(&t));
    bounded!("replace-if-match", replace_if_match(&t));
    bounded!("concurrent-replaces", concurrent_replaces(&t));
    bounded!("read-after-write", read_after_write(&t));
    bounded!("read-after-overwrite", read_after_overwrite(&t));
    let (seen, gone) =
        match tokio::time::timeout(options.check_timeout, list_lag(&t, options)).await {
            Ok(both) => both,
            Err(_) => {
                cut = true;
                (
                    check("list-new-key", Status::Error, late()),
                    check("list-deleted-key", Status::Error, late()),
                )
            }
        };
    checks.push(seen);
    checks.push(gone);
    bounded!("delete-absent", delete_absent(&t));
    bounded!("large-object", large_object(&t, options));
    bounded!("last-modified", last_modified(&t, options));
    bounded!("clock-skew", clock_skew(&t, &options.lease));
    match options.paging_keys {
        Some(n) => bounded!("list-paging", paging(&t, n, options)),
        None => checks.push(check(
            "list-paging",
            Status::Skipped,
            "not run (--skip-paging)",
        )),
    }
    let mut ended = bounded_cleanup(&t, options).await;
    // A write cut short above may land after its delete: list once more
    // when it had time to, and delete what this run wrote again.
    if cut && ended == (Some(0), None) {
        tokio::time::sleep(options.settle).await;
        ended = bounded_cleanup(&t, options).await;
    }
    let (left, cleanup_error) = ended;
    Ok(Report {
        checks,
        left,
        cleanup_error,
    })
}

/// [`cleanup`], given up after the check timeout.
async fn bounded_cleanup<T: Target>(
    t: &Tracked<'_, T>,
    options: &Options,
) -> (Option<usize>, Option<String>) {
    tokio::time::timeout(options.check_timeout, cleanup(t, options))
        .await
        .unwrap_or_else(|_| {
            (
                None,
                Some(format!(
                    "no answer within {} s",
                    options.check_timeout.as_secs_f64()
                )),
            )
        })
}

/// Deletes every key this run tried to write, then lists until the
/// scratch prefix shows empty or the listing wait runs out. Anything else
/// found there is counted as left, never deleted.
async fn cleanup<T: Target>(
    t: &Tracked<'_, T>,
    options: &Options,
) -> (Option<usize>, Option<String>) {
    // Only what this run wrote: anything else found under the prefix is
    // reported, never deleted.
    let keys: BTreeSet<String> = t.written.borrow().clone();
    let mut error = None;
    let keys: Vec<String> = keys.into_iter().collect();
    for chunk in keys.chunks(32) {
        for (key, result) in chunk
            .iter()
            .zip(join_all(chunk.iter().map(|k| t.delete(k))).await)
        {
            if let Err(e) = result {
                error.get_or_insert_with(|| format!("deleting {key}: {e}"));
            }
        }
    }
    let start = Instant::now();
    loop {
        match t.list("").await {
            Ok(left) if left.is_empty() => return (Some(0), error),
            Ok(left) if start.elapsed() >= options.list_wait => {
                return (Some(left.len()), error);
            }
            Ok(_) => tokio::time::sleep(Duration::from_millis(200)).await,
            Err(e) => return (None, Some(format!("listing after deletes: {e}"))),
        }
    }
}

/// The check every node runs at start ([`check_conditional_writes`]).
async fn start_check<T: Target>(t: &Tracked<'_, T>) -> Check {
    const NAME: &str = "start-check";
    match check_conditional_writes(t, "start/probe").await {
        Ok(()) => check(
            NAME,
            Status::Pass,
            "the check every node runs at start passes",
        ),
        Err(CheckError::Unsafe(why)) => check(NAME, Status::Fail, why),
        Err(CheckError::Unavailable(why)) => check(NAME, Status::Error, why),
    }
}

/// How a conditional write that should succeed came out.
enum Wrote {
    /// Accepted, with the new version.
    Done(Version),
    /// Answered 412, but the object holds the body just sent, with this
    /// version: the write landed, its answer was lost (a 5xx, a timeout)
    /// and the client's retry found its own write. Not a refusal.
    Landed(Version),
    /// Refused: the object holds this instead (`None`: absent).
    Refused(Option<Vec<u8>>),
}

/// After a 412: whether the object holds `body` (the write landed).
async fn read_back<T: Target>(
    t: &Tracked<'_, T>,
    key: &str,
    body: &[u8],
) -> Result<Wrote, BucketError> {
    Ok(match t.get(key).await? {
        Some((got, version)) if got == body => Wrote::Landed(version),
        other => Wrote::Refused(other.map(|(b, _)| b)),
    })
}

/// A create that should succeed: a 412 is read back ([`Wrote`]).
async fn create_checked<T: Target>(
    t: &Tracked<'_, T>,
    key: &str,
    body: &[u8],
) -> Result<Wrote, BucketError> {
    match t.create(key, body.to_vec()).await {
        Ok(v) => Ok(Wrote::Done(v)),
        Err(BucketError::Conflict) => read_back(t, key, body).await,
        Err(e) => Err(e),
    }
}

/// A replace that should succeed: a 412 is read back ([`Wrote`]).
async fn replace_checked<T: Target>(
    t: &Tracked<'_, T>,
    key: &str,
    body: &[u8],
    expected: &Version,
) -> Result<Wrote, BucketError> {
    match t.replace(key, body.to_vec(), expected).await {
        Ok(v) => Ok(Wrote::Done(v)),
        Err(BucketError::Conflict) => read_back(t, key, body).await,
        Err(e) => Err(e),
    }
}

/// What a check adds to its value when a write answered 412 yet landed.
fn landed_note(landed: usize) -> String {
    if landed == 0 {
        String::new()
    } else {
        format!(" ({landed} write(s) answered 412 after the client retried them, but had landed)")
    }
}

fn shown(body: Option<&[u8]>) -> String {
    body.map_or("nothing".into(), |b| {
        format!("{:?}", String::from_utf8_lossy(b))
    })
}

/// `If-None-Match: *`: a create over an existing object is refused, and
/// one after a delete succeeds (an ownership record removed and taken
/// again).
async fn create_if_absent<T: Target>(t: &Tracked<'_, T>) -> Check {
    const NAME: &str = "create-if-absent";
    let key = "create/k";
    let mut landed = 0;
    match try_or!(NAME, "creating", create_checked(t, key, b"1").await) {
        Wrote::Done(_) => {}
        Wrote::Landed(_) => landed += 1,
        Wrote::Refused(got) => {
            return check(
                NAME,
                Status::Error,
                format!(
                    "a create of a fresh key was refused (it holds {})",
                    shown(got.as_deref())
                ),
            );
        }
    }
    match t.create(key, b"2".to_vec()).await {
        Err(BucketError::Conflict) => {}
        Ok(_) => {
            return check(
                NAME,
                Status::Fail,
                "a create over an existing object was accepted (If-None-Match ignored)",
            );
        }
        Err(e) => return error(NAME, "creating over it", &e),
    }
    let body = try_or!(NAME, "reading", t.get(key).await).map(|(b, _)| b);
    if body.as_deref() != Some(b"1") {
        return check(
            NAME,
            Status::Fail,
            format!("the refused create changed the object (it holds {body:?})"),
        );
    }
    try_or!(NAME, "deleting", t.delete(key).await);
    match try_or!(
        NAME,
        "creating after the delete",
        create_checked(t, key, b"3").await
    ) {
        Wrote::Done(_) => {}
        Wrote::Landed(_) => landed += 1,
        // Refused, and the deleted object is back: the delete did not
        // take effect.
        Wrote::Refused(Some(got)) if got == b"1" => {
            return check(
                NAME,
                Status::Fail,
                "a create after a delete was refused (the delete did not take effect)",
            );
        }
        Wrote::Refused(got) => {
            return check(
                NAME,
                Status::Error,
                format!(
                    "a create after a delete was refused, and the key holds {}",
                    shown(got.as_deref())
                ),
            );
        }
    }
    check(
        NAME,
        Status::Pass,
        format!(
            "create over an existing object refused; create after a delete accepted{}",
            landed_note(landed)
        ),
    )
}

/// Contenders in each race, and the races run.
const CONTENDERS: usize = 8;
const RACES: usize = 5;

/// Opens as many connections to the bucket as a race has contenders (a
/// read each, at once), so that the race's writes leave together instead
/// of one per new connection.
async fn warm<T: Target>(t: &Tracked<'_, T>) -> Result<(), BucketError> {
    for r in join_all((0..CONTENDERS).map(|i| t.get_owned(format!("warm/{i}")))).await {
        r?;
    }
    Ok(())
}

/// One race's outcome: how many writes won, which, and how many were
/// refused.
struct Race {
    won: Vec<usize>,
    refused: usize,
    other: Option<String>,
}

fn race(results: Vec<Result<Version, BucketError>>) -> Race {
    let mut out = Race {
        won: Vec::new(),
        refused: 0,
        other: None,
    };
    for (i, r) in results.into_iter().enumerate() {
        match r {
            Ok(_) => out.won.push(i),
            Err(BucketError::Conflict) => out.refused += 1,
            Err(e) => {
                out.other.get_or_insert_with(|| e.to_string());
            }
        }
    }
    out
}

/// After a race on `key` among `bodies`: `Ok(())` if exactly one write
/// won and the object holds its body (or none won, every write answered
/// 412, and the object holds one contender's body: that write landed and
/// its retry was refused, counted in `landed`).
async fn judge_race<T: Target>(
    t: &Tracked<'_, T>,
    name: &'static str,
    what: &str,
    key: &str,
    bodies: &[Vec<u8>],
    race: Race,
    landed: &mut usize,
) -> Result<(), Check> {
    let n = bodies.len();
    if race.won.len() > 1 {
        return Err(check(
            name,
            Status::Fail,
            format!(
                "{} of {n} simultaneous {what} of one key succeeded",
                race.won.len()
            ),
        ));
    }
    if let Some(why) = race.other {
        return Err(check(
            name,
            Status::Error,
            format!(
                "{} won, {} refused, the rest failed: {why}",
                race.won.len(),
                race.refused
            ),
        ));
    }
    let stored = match t.get(key).await {
        Ok(got) => got.map(|(b, _)| b),
        Err(e) => return Err(error(name, "reading the winner", &e)),
    };
    match race.won.first() {
        Some(&w) if stored.as_deref() == Some(bodies[w].as_slice()) => Ok(()),
        Some(&w) => Err(check(
            name,
            Status::Fail,
            format!(
                "the {what} by contender {w} won, but the object holds {}",
                shown(stored.as_deref())
            ),
        )),
        None if stored.as_ref().is_some_and(|s| bodies.contains(s)) => {
            *landed += 1;
            Ok(())
        }
        None => Err(check(
            name,
            Status::Error,
            format!(
                "none of {n} simultaneous {what} won, and the object holds {}",
                shown(stored.as_deref())
            ),
        )),
    }
}

/// Eight creates of one key at once, five times on fresh keys over warm
/// connections: exactly one wins each time, and the object is its.
async fn concurrent_creates<T: Target>(t: &Tracked<'_, T>) -> Check {
    const NAME: &str = "concurrent-creates";
    try_or!(NAME, "opening connections", warm(t).await);
    let mut landed = 0;
    for r in 0..RACES {
        let key = format!("race/{r}");
        let bodies: Vec<Vec<u8>> = (0..CONTENDERS)
            .map(|i| format!("{r}-{i}").into_bytes())
            .collect();
        let results = join_all(bodies.iter().map(|b| t.create(&key, b.clone()))).await;
        if let Err(c) = judge_race(
            t,
            NAME,
            "creates",
            &key,
            &bodies,
            race(results),
            &mut landed,
        )
        .await
        {
            return c;
        }
    }
    check(
        NAME,
        Status::Pass,
        format!(
            "{RACES} races of {CONTENDERS} simultaneous creates: one won each{}",
            landed_note(landed)
        ),
    )
}

/// Eight replaces of one object at once, all with its current version,
/// five times: exactly one wins each time and the object holds its body
/// (two nodes taking over one lease at once).
async fn concurrent_replaces<T: Target>(t: &Tracked<'_, T>) -> Check {
    const NAME: &str = "concurrent-replaces";
    try_or!(NAME, "opening connections", warm(t).await);
    let mut landed = 0;
    for r in 0..RACES {
        let key = format!("swap/{r}");
        let base = format!("{r}-base").into_bytes();
        let version = match try_or!(NAME, "creating", create_checked(t, &key, &base).await) {
            Wrote::Done(v) | Wrote::Landed(v) => v,
            Wrote::Refused(got) => {
                return check(
                    NAME,
                    Status::Error,
                    format!(
                        "a create of a fresh key was refused (it holds {})",
                        shown(got.as_deref())
                    ),
                );
            }
        };
        let bodies: Vec<Vec<u8>> = (0..CONTENDERS)
            .map(|i| format!("{r}-{i}").into_bytes())
            .collect();
        let results = join_all(bodies.iter().map(|b| t.replace(&key, b.clone(), &version))).await;
        if let Err(c) = judge_race(
            t,
            NAME,
            "replaces",
            &key,
            &bodies,
            race(results),
            &mut landed,
        )
        .await
        {
            return c;
        }
    }
    check(
        NAME,
        Status::Pass,
        format!(
            "{RACES} races of {CONTENDERS} simultaneous replaces of one version: one won each{}",
            landed_note(landed)
        ),
    )
}

/// `If-Match`: a replace with the current version (the write's or a
/// read's) is accepted, one with a stale version refused, and one of a
/// deleted object refused (a lease deleted underneath its holder reads as
/// replaced).
async fn replace_if_match<T: Target>(t: &Tracked<'_, T>) -> Check {
    const NAME: &str = "replace-if-match";
    let key = "replace/k";
    let mut landed = 0;
    let refused = |what: &str, got: Option<Vec<u8>>, old: &[u8]| {
        // Still the old body: a real refusal. Anything else: unexplained.
        if got.as_deref() == Some(old) {
            check(NAME, Status::Fail, format!("{what} was refused"))
        } else {
            check(
                NAME,
                Status::Error,
                format!(
                    "{what} was refused, and the object holds {}",
                    shown(got.as_deref())
                ),
            )
        }
    };
    let v1 = match try_or!(NAME, "creating", create_checked(t, key, b"1").await) {
        Wrote::Done(v) => v,
        Wrote::Landed(v) => {
            landed += 1;
            v
        }
        Wrote::Refused(got) => {
            return check(
                NAME,
                Status::Error,
                format!(
                    "a create of a fresh key was refused (it holds {})",
                    shown(got.as_deref())
                ),
            );
        }
    };
    let v2 = match try_or!(
        NAME,
        "replacing with the current version",
        replace_checked(t, key, b"2", &v1).await
    ) {
        Wrote::Done(v) => v,
        Wrote::Landed(v) => {
            landed += 1;
            v
        }
        Wrote::Refused(got) => {
            return refused("a replace with the version a write returned", got, b"1");
        }
    };
    match t.replace(key, b"x".to_vec(), &v1).await {
        Err(BucketError::Conflict) => {}
        Ok(_) => {
            return check(
                NAME,
                Status::Fail,
                "a replace with a stale version was accepted (If-Match ignored)",
            );
        }
        Err(e) => return error(NAME, "replacing with a stale version", &e),
    }
    let Some((body, read)) = try_or!(NAME, "reading", t.get(key).await) else {
        return check(NAME, Status::Fail, "the object vanished after a replace");
    };
    if body != b"2" {
        return check(
            NAME,
            Status::Fail,
            format!(
                "after a refused replace the object holds {:?}, not \"2\"",
                String::from_utf8_lossy(&body)
            ),
        );
    }
    let v3 = match try_or!(
        NAME,
        "replacing with a read's version",
        replace_checked(t, key, b"3", &read).await
    ) {
        Wrote::Done(v) => v,
        Wrote::Landed(v) => {
            landed += 1;
            v
        }
        Wrote::Refused(got) => {
            return refused("a replace with the version a read returned", got, b"2");
        }
    };
    match t.replace(key, b"x".to_vec(), &v2).await {
        Err(BucketError::Conflict) => {}
        Ok(_) => {
            return check(
                NAME,
                Status::Fail,
                "a write's own version was still accepted after another write",
            );
        }
        Err(e) => return error(NAME, "replacing with an old version", &e),
    }
    try_or!(NAME, "deleting", t.delete(key).await);
    match t.replace(key, b"x".to_vec(), &v3).await {
        Err(BucketError::Conflict) => check(
            NAME,
            Status::Pass,
            format!(
                "current version (written or read) accepted; stale and deleted refused{}",
                landed_note(landed)
            ),
        ),
        Ok(_) => check(
            NAME,
            Status::Fail,
            "a replace of a deleted object was accepted (it recreated the object)",
        ),
        Err(e) => error(NAME, "replacing a deleted object", &e),
    }
}

const ROUNDS: usize = 5;

/// A read right after a create returns it.
async fn read_after_write<T: Target>(t: &Tracked<'_, T>) -> Check {
    const NAME: &str = "read-after-write";
    let started = Instant::now();
    for i in 0..ROUNDS {
        let key = format!("raw/{i}");
        let body = format!("new {i}").into_bytes();
        try_or!(NAME, "creating", t.create(&key, body.clone()).await);
        match try_or!(NAME, "reading", t.get(&key).await) {
            Some((got, _)) if got == body => {}
            got => {
                return check(
                    NAME,
                    Status::Fail,
                    format!(
                        "a read right after a create returned {}",
                        got.map_or("nothing".into(), |(b, _)| format!(
                            "{:?}",
                            String::from_utf8_lossy(&b)
                        ))
                    ),
                );
            }
        }
    }
    check(
        NAME,
        Status::Pass,
        format!(
            "{ROUNDS} of {ROUNDS} reads right after a create were current ({} ms per write and read)",
            started.elapsed().as_millis() / ROUNDS as u128
        ),
    )
}

/// A read right after a replace or an overwrite returns the new body,
/// and a plain overwrite makes the version before it stale (a lease
/// overwritten by hand is not renewed over).
async fn read_after_overwrite<T: Target>(t: &Tracked<'_, T>) -> Check {
    const NAME: &str = "read-after-overwrite";
    let key = "rao/k";
    let mut landed = 0;
    let mut version = match try_or!(NAME, "creating", create_checked(t, key, b"0").await) {
        Wrote::Done(v) => v,
        Wrote::Landed(v) => {
            landed += 1;
            v
        }
        Wrote::Refused(got) => {
            return check(
                NAME,
                Status::Error,
                format!(
                    "a create of a fresh key was refused (it holds {})",
                    shown(got.as_deref())
                ),
            );
        }
    };
    for i in 1..=ROUNDS {
        let body = format!("replaced {i}").into_bytes();
        let before = match try_or!(
            NAME,
            "replacing",
            replace_checked(t, key, &body, &version).await
        ) {
            Wrote::Done(v) => v,
            Wrote::Landed(v) => {
                landed += 1;
                v
            }
            Wrote::Refused(got) => {
                return check(
                    NAME,
                    Status::Error,
                    format!(
                        "a replace with the current version was refused (it holds {})",
                        shown(got.as_deref())
                    ),
                );
            }
        };
        let got = try_or!(NAME, "reading", t.get(key).await).map(|(b, _)| b);
        if got.as_deref() != Some(body.as_slice()) {
            return check(
                NAME,
                Status::Fail,
                format!("a read right after a replace returned the old body ({got:?})"),
            );
        }
        let body = format!("overwritten {i}").into_bytes();
        try_or!(NAME, "overwriting", t.put(key, body.clone()).await);
        let Some((got, read)) = try_or!(NAME, "reading", t.get(key).await) else {
            return check(NAME, Status::Fail, "the object vanished after an overwrite");
        };
        if got != body {
            return check(
                NAME,
                Status::Fail,
                "a read right after an overwrite returned the old body",
            );
        }
        match t.replace(key, b"x".to_vec(), &before).await {
            Err(BucketError::Conflict) => {}
            Ok(_) => {
                return check(
                    NAME,
                    Status::Fail,
                    "a replace with the version from before a plain overwrite was accepted",
                );
            }
            Err(e) => return error(NAME, "replacing with the version before the overwrite", &e),
        }
        version = read;
    }
    check(
        NAME,
        Status::Pass,
        format!(
            "{} of {} reads right after a replace or overwrite were current; \
             an overwrite made the version before it stale{}",
            2 * ROUNDS,
            2 * ROUNDS,
            landed_note(landed)
        ),
    )
}

/// A listing shows a new key, then stops showing it once deleted, within
/// the wait; the lag is reported.
async fn list_lag<T: Target>(t: &Tracked<'_, T>, options: &Options) -> (Check, Check) {
    const SEEN: &str = "list-new-key";
    const GONE: &str = "list-deleted-key";
    let key = "list/k";
    let not_run = |why: &str| check(GONE, Status::Error, format!("not run: {why}"));
    if let Err(e) = t.put(key, b"x".to_vec()).await {
        return (error(SEEN, "writing", &e), not_run("the write failed"));
    }
    let lag = |want: bool| async move {
        let start = Instant::now();
        loop {
            let listed = t.list("list/").await?;
            let present = listed.iter().any(|(k, _, _)| k == key);
            if present == want {
                return Ok::<_, BucketError>(Some(start.elapsed()));
            }
            if start.elapsed() >= options.list_wait {
                return Ok(None);
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    };
    let judge = |name: &'static str, what: &str, r: Result<Option<Duration>, BucketError>| match r {
        Ok(Some(d)) if d <= Duration::from_secs(1) => check(
            name,
            Status::Pass,
            format!("{what} after {} ms", d.as_millis()),
        ),
        Ok(Some(d)) => check(
            name,
            Status::Warn,
            format!(
                "{what} only after {} ms (nodes discover peers and alarms through listings)",
                d.as_millis()
            ),
        ),
        Ok(None) => check(
            name,
            Status::Fail,
            format!("not {what} within {} s", options.list_wait.as_secs_f64()),
        ),
        Err(e) => error(name, "listing", &e),
    };
    let seen = judge(SEEN, "listed", lag(true).await);
    if let Err(e) = t.delete(key).await {
        return (seen, error(GONE, "deleting", &e));
    }
    let gone = judge(GONE, "gone from the listing", lag(false).await);
    (seen, gone)
}

/// Deleting an absent key succeeds and removes nothing else; a deleted
/// key reads as absent at once, and deleting it again succeeds.
async fn delete_absent<T: Target>(t: &Tracked<'_, T>) -> Check {
    const NAME: &str = "delete-absent";
    try_or!(
        NAME,
        "writing a neighbour",
        t.put("del/keep", b"k".to_vec()).await
    );
    try_or!(NAME, "deleting an absent key", t.delete("del/never").await);
    try_or!(NAME, "deleting an absent key", t.delete("del/kee").await);
    if try_or!(NAME, "reading the neighbour", t.get("del/keep").await).is_none() {
        return check(
            NAME,
            Status::Fail,
            "deleting an absent key removed another object",
        );
    }
    try_or!(NAME, "writing", t.put("del/k", b"x".to_vec()).await);
    try_or!(NAME, "deleting", t.delete("del/k").await);
    if try_or!(NAME, "reading the deleted key", t.get("del/k").await).is_some() {
        return check(
            NAME,
            Status::Fail,
            "a read right after a delete still returned the object",
        );
    }
    try_or!(NAME, "deleting it again", t.delete("del/k").await);
    check(
        NAME,
        Status::Pass,
        "absent keys delete without error; a deleted key reads absent at once",
    )
}

/// Deterministic, incompressible-looking bytes.
fn pattern(len: usize) -> Vec<u8> {
    let mut x: u64 = 0x9e37_79b9_7f4a_7c15;
    let mut out = Vec::with_capacity(len);
    while out.len() < len {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        out.extend_from_slice(&x.to_le_bytes());
    }
    out.truncate(len);
    out
}

/// A listing of `prefix`, `(key, last-modified ms, size)` each.
type Listing = Vec<(String, u64, u64)>;

/// Lists `prefix` until `ready` holds or the listing wait runs out: the
/// last listing, and how long until it was ready (`None`: never).
async fn list_until<T: Target>(
    t: &Tracked<'_, T>,
    prefix: &str,
    options: &Options,
    ready: impl Fn(&Listing) -> bool,
) -> Result<(Listing, Option<Duration>), BucketError> {
    let start = Instant::now();
    loop {
        let listed = t.list(prefix).await?;
        if ready(&listed) {
            return Ok((listed, Some(start.elapsed())));
        }
        if start.elapsed() >= options.list_wait {
            return Ok((listed, None));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// A listing that took more than a second to show a write: a warning, as
/// in `list-new-key`.
fn lag_note(lag: Duration) -> Option<String> {
    (lag > Duration::from_secs(1)).then(|| {
        format!(
            "; the listing showed the writes only after {} ms",
            lag.as_millis()
        )
    })
}

/// A large object (a database snapshot is one PUT) comes back whole, and
/// is listed with its size.
async fn large_object<T: Target>(t: &Tracked<'_, T>, options: &Options) -> Check {
    const NAME: &str = "large-object";
    let len = options.large_bytes;
    let key = "large/k";
    let body = pattern(len);
    let up = Instant::now();
    try_or!(NAME, "writing", t.put(key, body.clone()).await);
    let up = up.elapsed();
    let down = Instant::now();
    let got = try_or!(NAME, "reading", t.get(key).await);
    let down = down.elapsed();
    let mib = len as f64 / f64::from(1u32 << 20);
    match got {
        Some((got, _)) if got == body => {}
        Some((got, _)) => {
            return check(
                NAME,
                Status::Fail,
                format!(
                    "{mib:.0} MiB written, {} bytes read back, not the same",
                    got.len()
                ),
            );
        }
        None => return check(NAME, Status::Fail, "a large object read back as absent"),
    }
    let (listed, lag) = try_or!(
        NAME,
        "listing",
        list_until(t, "large/", options, |l| l.iter().any(|(k, _, _)| k == key)).await
    );
    let Some(lag) = lag else {
        return check(
            NAME,
            Status::Fail,
            format!(
                "the large object was not listed within {} s",
                options.list_wait.as_secs_f64()
            ),
        );
    };
    let size = listed.iter().find(|(k, _, _)| k == key).map(|(_, _, s)| *s);
    if size != Some(len as u64) {
        return check(
            NAME,
            Status::Fail,
            format!("the listing gives the wrong size: {size:?}, not {len}"),
        );
    }
    let value = format!(
        "{mib:.0} MiB round trip intact (write {:.1} s, read {:.1} s)",
        up.as_secs_f64(),
        down.as_secs_f64()
    );
    match lag_note(lag) {
        Some(note) => check(NAME, Status::Warn, value + &note),
        None => check(NAME, Status::Pass, value),
    }
}

/// Last-modified times as the migration and `cluster fence` use them: a
/// write (or overwrite) more than a second after another lists later, a
/// head and a listing agree within a second, and two writes back to back
/// list in order, or out of order by less than the allowed clock skew (a
/// store's servers may stamp them; a warning).
async fn last_modified<T: Target>(t: &Tracked<'_, T>, options: &Options) -> Check {
    const NAME: &str = "last-modified";
    let time = |listed: &[(String, u64, u64)], key: &str| {
        listed.iter().find(|(k, _, _)| k == key).map(|(_, m, _)| *m)
    };
    try_or!(NAME, "writing", t.put("mod/a", b"1".to_vec()).await);
    try_or!(NAME, "writing", t.put("mod/b", b"1".to_vec()).await);
    let (first, lag1) = try_or!(
        NAME,
        "listing",
        list_until(t, "mod/", options, |l| {
            time(l, "mod/a").is_some() && time(l, "mod/b").is_some()
        })
        .await
    );
    let (Some(a1), Some(b1)) = (time(&first, "mod/a"), time(&first, "mod/b")) else {
        return check(
            NAME,
            Status::Fail,
            format!(
                "a listing missed one of two writes for {} s",
                options.list_wait.as_secs_f64()
            ),
        );
    };
    if a1 == 0 || b1 == 0 {
        return check(
            NAME,
            Status::Fail,
            "the listing gives no last-modified time",
        );
    }
    let mut notes = Vec::new();
    if b1 < a1 {
        let by = a1 - b1;
        if by > options.lease.max_skew_ms() {
            return check(
                NAME,
                Status::Fail,
                format!(
                    "a later write lists {by} ms earlier ({b1} < {a1} ms), past the allowed skew"
                ),
            );
        }
        notes.push(format!(
            "; two writes back to back list {by} ms out of order (within the allowed skew)"
        ));
    }
    tokio::time::sleep(options.modified_gap).await;
    try_or!(NAME, "writing", t.put("mod/c", b"1".to_vec()).await);
    try_or!(NAME, "overwriting", t.put("mod/a", b"2".to_vec()).await);
    let (second, lag2) = try_or!(
        NAME,
        "listing",
        list_until(t, "mod/", options, |l| {
            time(l, "mod/c").is_some() && time(l, "mod/a").is_some_and(|a| a != a1)
        })
        .await
    );
    let (Some(a2), Some(c)) = (time(&second, "mod/a"), time(&second, "mod/c")) else {
        return check(
            NAME,
            Status::Fail,
            format!(
                "a listing missed one of two writes for {} s",
                options.list_wait.as_secs_f64()
            ),
        );
    };
    let gap = options.modified_gap.as_millis();
    if c <= b1 {
        return check(
            NAME,
            Status::Fail,
            format!("a write {gap} ms after another lists no later ({c} <= {b1} ms)"),
        );
    }
    if a2 <= a1 {
        return check(
            NAME,
            Status::Fail,
            format!("an overwrite {gap} ms later keeps the old time ({a2} <= {a1} ms)"),
        );
    }
    let Some(head) = try_or!(NAME, "head", t.inner.head_modified_ms("mod/a").await) else {
        return check(
            NAME,
            Status::Fail,
            "a head of a listed object found nothing",
        );
    };
    let diff = head.abs_diff(a2);
    if diff >= 1_000 {
        return check(
            NAME,
            Status::Fail,
            format!("a head and a listing disagree by {diff} ms"),
        );
    }
    let lag = lag1.max(lag2).unwrap_or_default();
    notes.extend(lag_note(lag));
    let whole = [a1, b1, a2, c, head].iter().all(|m| m % 1_000 == 0);
    let value = format!(
        "{} precision, writes a second apart in order; head and listing agree within {diff} ms{}",
        if whole { "whole-second" } else { "millisecond" },
        notes.concat()
    );
    check(
        NAME,
        if notes.is_empty() {
            Status::Pass
        } else {
            Status::Warn
        },
        value,
    )
}

/// This machine's clock against the bucket's, as the last-modified time
/// of a write it brackets (the clock `cluster fence` checks against). A
/// time in whole seconds widens the bound by a second.
async fn clock_skew<T: Target>(t: &Tracked<'_, T>, lease: &LeaseConfig) -> Check {
    const NAME: &str = "clock-skew";
    // Several writes spread over more than a second: with whole-second
    // stamps, each bounds the skew to a window a second wide, shifted by
    // where the second boundary fell; together they narrow it.
    let mut window: Option<(i128, i128)> = None;
    for i in 0..SKEW_SAMPLES {
        if i > 0 {
            tokio::time::sleep(Duration::from_millis(350)).await;
        }
        let key = format!("clock/{i}");
        let before = now_ms();
        try_or!(NAME, "writing", t.put(&key, Vec::new()).await);
        let after = now_ms();
        let Some(modified) = try_or!(NAME, "head", t.inner.head_modified_ms(&key).await) else {
            return check(
                NAME,
                Status::Fail,
                "a head right after a write found nothing",
            );
        };
        let precision = if modified % 1_000 == 0 { 1_000 } else { 1 };
        let (lo, hi) = skew_window(modified, precision, before, after);
        window = Some(narrow(window, (lo, hi)));
    }
    let (lo, hi) = window.unwrap_or((0, 0));
    judge_skew(NAME, lo, hi, lease.max_skew_ms())
}

const SKEW_SAMPLES: usize = 4;

/// Both windows hold the skew: their overlap. Disjoint windows (the
/// bucket's clock moved, or two of its servers disagree) give the span of
/// both, never a false precision.
fn narrow(window: Option<(i128, i128)>, (lo, hi): (i128, i128)) -> (i128, i128) {
    match window {
        None => (lo, hi),
        Some((l, h)) if lo.max(l) > hi.min(h) => (lo.min(l), hi.max(h)),
        Some((l, h)) => (lo.max(l), hi.min(h)),
    }
}

/// The bucket's time `stamp` (to `precision` ms, rounded down) was taken
/// between this machine's `before` and `after`: the bucket's clock minus
/// this machine's is in `[stamp - after, stamp + precision - before]`.
fn skew_window(stamp: u64, precision: u64, before: u64, after: u64) -> (i128, i128) {
    (
        i128::from(stamp) - i128::from(after),
        i128::from(stamp) + i128::from(precision) - i128::from(before),
    )
}

/// The bucket's clock minus this machine's is in `[lo, hi]` (ms): fails
/// when all of it is beyond `max_skew_ms`, warns when part is.
fn judge_skew(name: &'static str, lo: i128, hi: i128, max_skew_ms: u64) -> Check {
    let max = i128::from(max_skew_ms);
    let secs = |ms: i128| format!("{:+.1} s", ms as f64 / 1000.0);
    let value = format!(
        "bucket minus this machine between {} and {} (nodes tolerate {:.1} s)",
        secs(lo),
        secs(hi),
        max_skew_ms as f64 / 1000.0
    );
    if lo > max || hi < -max {
        check(
            name,
            Status::Fail,
            format!("{value}: fix this machine's clock (or the bucket's)"),
        )
    } else if lo < -max || hi > max {
        check(name, Status::Warn, format!("{value}: possibly beyond"))
    } else {
        check(name, Status::Pass, value)
    }
}

/// A listing of more keys than one page (S3: 1,000) returns them all.
async fn paging<T: Target>(t: &Tracked<'_, T>, n: usize, options: &Options) -> Check {
    const NAME: &str = "list-paging";
    let keys: Vec<String> = (0..n).map(|i| format!("page/{i:05}")).collect();
    for chunk in keys.chunks(32) {
        for result in join_all(chunk.iter().map(|k| t.put(k, Vec::new()))).await {
            try_or!(NAME, "writing", result);
        }
    }
    let start = Instant::now();
    loop {
        let listed = try_or!(NAME, "listing", t.list("page/").await);
        let distinct: BTreeSet<&str> = listed.iter().map(|(k, _, _)| k.as_str()).collect();
        if distinct.len() == n {
            return check(NAME, Status::Pass, format!("{n} of {n} keys listed"));
        }
        if start.elapsed() >= options.list_wait {
            return check(
                NAME,
                Status::Fail,
                format!(
                    "{} of {n} keys listed after {} s",
                    distinct.len(),
                    options.list_wait.as_secs_f64()
                ),
            );
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// This run's folder name: unique to this process.
fn run_folder() -> String {
    format!("run-{}-{}", now_ms(), std::process::id())
}

/// Where a run writes, below the URL's prefix: its own `run` folder under
/// `--prefix-for-test` (or `diagnose`), so that two runs at once never
/// share a prefix. `None` if the given prefix names no folder.
fn scratch_for(given: Option<&str>, run: &str) -> Option<String> {
    let base = given.unwrap_or("diagnose").trim_matches('/');
    scratch_prefix("", base)?;
    Some(format!("{base}/{run}"))
}

/// Joins the URL's prefix and the scratch prefix; `None` if the scratch
/// prefix is empty (the checks never write at the prefix's root).
fn scratch_prefix(url_prefix: &str, scratch: &str) -> Option<String> {
    let scratch = scratch.trim_matches('/');
    if scratch.is_empty()
        || scratch
            .split('/')
            .any(|s| s.is_empty() || s == "." || s == "..")
    {
        return None;
    }
    Some(if url_prefix.is_empty() {
        scratch.to_owned()
    } else {
        format!("{url_prefix}/{scratch}")
    })
}

/// `wasmkeep cluster diagnose`: the exit code (0 safe, 1 not safe, 2 not
/// run, not finished, or safe but not cleaned up).
pub(crate) fn command(args: &DiagnoseArgs) -> i32 {
    let fail = |msg: String| {
        eprintln!("error: {msg}");
        2
    };
    let (store, url_prefix) = match crate::open_bucket_unchecked(&args.bucket) {
        Ok(v) => v,
        Err(e) => return fail(format!("{e:#}")),
    };
    let Some(prefix) = scratch_for(args.prefix_for_test.as_deref(), &run_folder())
        .and_then(|scratch| scratch_prefix(&url_prefix, &scratch))
    else {
        return fail(format!(
            "--prefix-for-test must name a folder, got {:?}",
            args.prefix_for_test.as_deref().unwrap_or_default()
        ));
    };
    let bucket_name = args
        .bucket
        .trim_start_matches("s3://")
        .split('/')
        .next()
        .unwrap_or_default();
    let scratch_url = format!("s3://{bucket_name}/{prefix}/");
    let rt = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => return fail(format!("starting a runtime: {e}")),
    };
    let target = ObjectStoreBucket::new(store, prefix.clone());
    let options = Options::production(args.skip_paging);
    let report = match rt.block_on(run(&target, &options)) {
        Ok(r) => r,
        Err(NotRun::NotEmpty(n)) => {
            return fail(format!(
                "{scratch_url} already holds {n} object(s): refusing to write there \
                 (run it again: the next run gets another folder)"
            ));
        }
        Err(NotRun::Unreachable(why)) => {
            return fail(format!("listing {scratch_url}: {why}"));
        }
    };
    if args.json {
        println!("{:#}", report.to_json(&args.bucket, &scratch_url));
    } else {
        print!("{}", report.to_text(&args.bucket, &scratch_url));
    }
    if !report.cleaned() {
        eprintln!("warning: {}", report.cleanup_text(&scratch_url));
    }
    report.exit_code()
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::panic)] // test code

    use std::collections::HashMap;
    use std::sync::Arc;

    use object_store::ObjectStore;
    use object_store::memory::InMemory;
    use wasmkeep_cluster::migrate::ObjectMeta;

    use super::*;

    fn quick() -> Options {
        Options {
            list_wait: Duration::from_millis(300),
            large_bytes: 1 << 20,
            paging_keys: Some(1_001),
            modified_gap: Duration::from_millis(20),
            lease: LeaseConfig::default_safe(),
            check_timeout: Duration::from_secs(30),
            settle: Duration::from_millis(10),
        }
    }

    fn memory(prefix: &str) -> (Arc<dyn ObjectStore>, ObjectStoreBucket) {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        (Arc::clone(&store), ObjectStoreBucket::new(store, prefix))
    }

    /// What a broken store gets wrong.
    #[derive(Default)]
    struct Faults {
        /// Creates ignore `If-None-Match` (plain writes).
        lax_create: bool,
        /// Replaces ignore `If-Match` (plain writes).
        lax_replace: bool,
        /// `If-Match` checked, then written, not atomically: simultaneous
        /// replaces of one version all win.
        racy_replace: bool,
        /// A plain overwrite does not change the version `If-Match` takes.
        keeps_version: bool,
        /// A create or replace of these keys that succeeds answers 412, as
        /// a client's retry of a write that landed but whose answer was
        /// lost does.
        landed: &'static [&'static str],
        /// ... and the object is then gone (not explained by a retry).
        wipe: bool,
        /// Listings show a key only this long after its last write.
        list_lag: Option<Duration>,
        /// Listings give `mod/b` a time this much earlier than it is.
        shift_b_ms: u64,
        /// A plain write of this key lands this long after it was sent,
        /// and its answer never comes.
        late_put: Option<(&'static str, Duration)>,
    }

    struct Fake {
        store: Arc<dyn ObjectStore>,
        prefix: &'static str,
        inner: ObjectStoreBucket,
        f: Faults,
        written: RefCell<HashMap<String, Instant>>,
        versions: RefCell<HashMap<String, Version>>,
        lock: tokio::sync::Mutex<()>,
    }

    fn fake(f: Faults) -> (Arc<dyn ObjectStore>, Fake) {
        let (store, inner) = memory("scratch");
        let fake = Fake {
            store: Arc::clone(&store),
            prefix: "scratch",
            inner,
            f,
            written: RefCell::default(),
            versions: RefCell::default(),
            lock: tokio::sync::Mutex::new(()),
        };
        (store, fake)
    }

    impl Fake {
        async fn overwrite(&self, key: &str, body: Vec<u8>) -> Result<Version, BucketError> {
            Store::put(&self.inner, key, body).await?;
            Ok(Bucket::get(&self.inner, key)
                .await?
                .map(|(_, v)| v)
                .unwrap_or(Version(String::new())))
        }

        fn wrote(
            &self,
            key: &str,
            r: Result<Version, BucketError>,
        ) -> Result<Version, BucketError> {
            let v = r?;
            self.written
                .borrow_mut()
                .insert(key.to_owned(), Instant::now());
            self.versions.borrow_mut().insert(key.to_owned(), v.clone());
            Ok(v)
        }

        async fn after(
            &self,
            key: &str,
            r: Result<Version, BucketError>,
        ) -> Result<Version, BucketError> {
            let v = self.wrote(key, r)?;
            if self.f.landed.contains(&key) {
                if self.f.wipe {
                    Store::delete(&self.inner, key).await?;
                }
                return Err(BucketError::Conflict);
            }
            Ok(v)
        }
    }

    impl Bucket for Fake {
        async fn get(&self, key: &str) -> Result<Option<(Vec<u8>, Version)>, BucketError> {
            Bucket::get(&self.inner, key).await
        }
        async fn create(&self, key: &str, body: Vec<u8>) -> Result<Version, BucketError> {
            let r = if self.f.lax_create {
                self.overwrite(key, body).await
            } else {
                self.inner.create(key, body).await
            };
            self.after(key, r).await
        }
        async fn replace(
            &self,
            key: &str,
            body: Vec<u8>,
            expected: &Version,
        ) -> Result<Version, BucketError> {
            let r = if self.f.lax_replace {
                self.overwrite(key, body).await
            } else if self.f.racy_replace {
                let current = Bucket::get(&self.inner, key).await?.map(|(_, v)| v);
                tokio::task::yield_now().await;
                if current.as_ref() == Some(expected) {
                    self.overwrite(key, body).await
                } else {
                    Err(BucketError::Conflict)
                }
            } else if self.f.keeps_version {
                let _one_at_a_time = self.lock.lock().await;
                let known = self.versions.borrow().get(key).cloned();
                if known.as_ref() == Some(expected) {
                    self.overwrite(key, body).await
                } else {
                    self.inner.replace(key, body, expected).await
                }
            } else {
                self.inner.replace(key, body, expected).await
            };
            self.after(key, r).await
        }
    }

    impl Store for Fake {
        async fn list(&self, prefix: &str) -> Result<Vec<ObjectMeta>, BucketError> {
            let mut listed = Store::list(&self.inner, prefix).await?;
            if let Some(lag) = self.f.list_lag {
                let written = self.written.borrow();
                listed.retain(|m| written.get(&m.key).is_none_or(|at| at.elapsed() >= lag));
            }
            for m in &mut listed {
                if m.key == "mod/b" {
                    m.modified_ms -= self.f.shift_b_ms;
                }
            }
            Ok(listed)
        }
        async fn list_dirs(&self, prefix: &str) -> Result<Vec<String>, BucketError> {
            self.inner.list_dirs(prefix).await
        }
        async fn copy(&self, from: &str, to: &str) -> Result<(), BucketError> {
            self.inner.copy(from, to).await
        }
        async fn put(&self, key: &str, body: Vec<u8>) -> Result<(), BucketError> {
            if let Some((late, after)) = self.f.late_put
                && late == key
            {
                let bucket = ObjectStoreBucket::new(Arc::clone(&self.store), self.prefix);
                let key = key.to_owned();
                tokio::spawn(async move {
                    tokio::time::sleep(after).await;
                    Store::put(&bucket, &key, body).await
                });
                return std::future::pending().await;
            }
            Store::put(&self.inner, key, body).await?;
            self.written
                .borrow_mut()
                .insert(key.to_owned(), Instant::now());
            // A plain write makes a new version, unless the store is broken.
            if !self.f.keeps_version {
                self.versions.borrow_mut().remove(key);
            }
            Ok(())
        }
        async fn delete(&self, key: &str) -> Result<(), BucketError> {
            self.versions.borrow_mut().remove(key);
            Store::delete(&self.inner, key).await
        }
    }

    impl Target for Fake {
        async fn head_modified_ms(&self, key: &str) -> Result<Option<u64>, BucketError> {
            self.inner.head_modified_ms(key).await
        }
    }

    fn of<'a>(report: &'a Report, name: &str) -> &'a Check {
        report
            .checks
            .iter()
            .find(|c| c.name == name)
            .unwrap_or_else(|| panic!("no check {name}: {report:?}"))
    }

    fn status_of(report: &Report, name: &str) -> Status {
        of(report, name).status
    }

    async fn everything_under(store: &Arc<dyn ObjectStore>) -> Vec<String> {
        use futures_util::TryStreamExt as _;
        store
            .list(None)
            .map_ok(|m| m.location.to_string())
            .try_collect()
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn the_in_memory_store_is_safe_and_left_empty() {
        let (store, bucket) = memory("wk/diagnose/run-1");
        let report = run(&bucket, &quick()).await.unwrap();
        assert_eq!(report.verdict(), Verdict::Safe, "{report:#?}");
        assert!(
            report.checks.iter().all(|c| c.status == Status::Pass),
            "{report:#?}"
        );
        assert!(
            report
                .checks
                .iter()
                .any(|c| c.name == "concurrent-replaces"),
            "{report:#?}"
        );
        assert_eq!(report.left, Some(0));
        assert_eq!(everything_under(&store).await, Vec::<String>::new());
        let text = report.to_text("s3://b/wk", "s3://b/wk/diagnose/run-1/");
        assert!(text.ends_with("safe for WasmKeep\n"), "{text}");
        assert_eq!(report.exit_code(), 0);
        let json = report.to_json("s3://b/wk", "s3://b/wk/diagnose/run-1/");
        assert_eq!(json["verdict"], "safe");
        assert_eq!(
            json["checks"].as_array().unwrap().len(),
            report.checks.len()
        );
    }

    /// Runs against a broken store: exactly `broken` fail, and it is left
    /// empty all the same.
    async fn unsafe_with(f: Faults, broken: &[&str]) -> Report {
        let (store, bucket) = fake(f);
        let report = run(&bucket, &quick()).await.unwrap();
        for name in broken {
            assert_eq!(
                status_of(&report, name),
                Status::Fail,
                "{name}: {report:#?}"
            );
        }
        let Verdict::Unsafe(failed) = report.verdict() else {
            panic!("{report:#?}");
        };
        assert_eq!(failed, broken, "{report:#?}");
        assert_eq!(report.exit_code(), 1);
        let text = report.to_text("s3://b", "s3://b/scratch/");
        assert!(text.contains(&format!("not safe: {}", broken[0])), "{text}");
        assert_eq!(
            report.to_json("s3://b", "s3://b/scratch/")["verdict"],
            "unsafe"
        );
        // Cleaned up even though it failed.
        assert_eq!(everything_under(&store).await, Vec::<String>::new());
        report
    }

    #[tokio::test]
    async fn a_store_that_ignores_conditions_is_not_safe() {
        let replaces = [
            "replace-if-match",
            "concurrent-replaces",
            "read-after-overwrite",
        ];
        let creates = ["start-check", "create-if-absent", "concurrent-creates"];
        let both: Vec<&str> = creates.iter().chain(&replaces).copied().collect();
        let f = Faults {
            lax_create: true,
            lax_replace: true,
            ..Faults::default()
        };
        unsafe_with(f, &both).await;
        let f = Faults {
            lax_create: true,
            ..Faults::default()
        };
        unsafe_with(f, &creates).await;
        let f = Faults {
            lax_replace: true,
            ..Faults::default()
        };
        let mut broken = vec!["start-check"];
        broken.extend(replaces);
        unsafe_with(f, &broken).await;
    }

    /// The lease takeover race: a store that checks `If-Match` and then
    /// writes, not atomically, passes every sequential check.
    #[tokio::test]
    async fn simultaneous_replaces_that_all_win_are_not_safe() {
        let f = Faults {
            racy_replace: true,
            ..Faults::default()
        };
        let report = unsafe_with(f, &["concurrent-replaces"]).await;
        let value = &of(&report, "concurrent-replaces").value;
        assert!(value.contains("8 of 8 simultaneous replaces"), "{value}");
    }

    #[tokio::test]
    async fn a_version_that_survives_an_overwrite_is_not_safe() {
        let f = Faults {
            keeps_version: true,
            ..Faults::default()
        };
        let report = unsafe_with(f, &["read-after-overwrite"]).await;
        let value = &of(&report, "read-after-overwrite").value;
        assert!(value.contains("before a plain overwrite"), "{value}");
    }

    /// A write that landed, lost its answer (a 503) and was retried by the
    /// client gets a 412 for the retry: read back, it is the write's own
    /// body, so it is no refusal.
    #[tokio::test]
    async fn a_write_that_landed_before_its_retry_got_412_is_no_failure() {
        let f = Faults {
            landed: &["create/k", "race/0", "replace/k", "swap/1", "rao/k"],
            ..Faults::default()
        };
        let (store, bucket) = fake(f);
        let report = run(&bucket, &quick()).await.unwrap();
        assert_eq!(report.verdict(), Verdict::Safe, "{report:#?}");
        for name in [
            "create-if-absent",
            "concurrent-creates",
            "replace-if-match",
            "concurrent-replaces",
            "read-after-overwrite",
        ] {
            let c = of(&report, name);
            assert_eq!(c.status, Status::Pass, "{c:?}");
            assert!(c.value.contains("but had landed"), "{c:?}");
        }
        assert_eq!(everything_under(&store).await, Vec::<String>::new());
    }

    /// A 412 the object does not explain (it holds neither the new body
    /// nor the old one) is no verdict, not a failure.
    #[tokio::test]
    async fn an_unexplained_412_is_an_error() {
        let f = Faults {
            landed: &["create/k", "replace/k"],
            wipe: true,
            ..Faults::default()
        };
        let (_, bucket) = fake(f);
        let report = run(&bucket, &quick()).await.unwrap();
        assert_eq!(
            report.verdict(),
            Verdict::Incomplete(vec!["create-if-absent", "replace-if-match"]),
            "{report:#?}"
        );
        assert_eq!(report.exit_code(), 2);
    }

    /// A listing that shows writes late warns in every check that lists,
    /// as `list-new-key` does; none fails.
    #[tokio::test]
    async fn a_lagging_listing_warns() {
        let f = Faults {
            list_lag: Some(Duration::from_millis(1_200)),
            ..Faults::default()
        };
        let (store, bucket) = fake(f);
        let options = Options {
            list_wait: Duration::from_secs(5),
            paging_keys: None,
            ..quick()
        };
        let report = run(&bucket, &options).await.unwrap();
        for name in ["list-new-key", "large-object", "last-modified"] {
            assert_eq!(
                status_of(&report, name),
                Status::Warn,
                "{name}: {report:#?}"
            );
        }
        assert_eq!(report.verdict(), Verdict::Safe, "{report:#?}");
        assert_eq!(everything_under(&store).await, Vec::<String>::new());
    }

    /// Two writes back to back may list out of order by less than the
    /// allowed skew (a warning); by more, the order is broken.
    #[tokio::test]
    async fn back_to_back_writes_may_list_slightly_out_of_order() {
        let (_, bucket) = fake(Faults {
            shift_b_ms: 300,
            ..Faults::default()
        });
        let c = last_modified(
            &Tracked {
                inner: &bucket,
                written: RefCell::default(),
            },
            &quick(),
        )
        .await;
        assert_eq!(c.status, Status::Warn, "{c:?}");
        assert!(c.value.contains("out of order"), "{c:?}");
        let (_, bucket) = fake(Faults {
            shift_b_ms: 1_500,
            ..Faults::default()
        });
        let c = last_modified(
            &Tracked {
                inner: &bucket,
                written: RefCell::default(),
            },
            &quick(),
        )
        .await;
        assert_eq!(c.status, Status::Fail, "{c:?}");
    }

    /// A write a check's timeout cut short may land after the cleanup:
    /// it is listed and deleted again.
    #[tokio::test]
    async fn a_write_landing_after_the_cleanup_is_deleted_too() {
        let f = Faults {
            // Lands after the checks that follow and the cleanup's listing.
            late_put: Some(("large/k", Duration::from_secs(4))),
            ..Faults::default()
        };
        let (store, bucket) = fake(f);
        let options = Options {
            check_timeout: Duration::from_secs(1),
            settle: Duration::from_secs(4),
            paging_keys: None,
            ..quick()
        };
        let report = run(&bucket, &options).await.unwrap();
        assert_eq!(status_of(&report, "large-object"), Status::Error);
        assert_eq!((report.left, &report.cleanup_error), (Some(0), &None));
        // Past the late write, whenever the run ended: nothing is left.
        tokio::time::sleep(Duration::from_secs(3)).await;
        assert_eq!(everything_under(&store).await, Vec::<String>::new());
    }

    /// A store that never answers a delete: the cleanup gives up, says so,
    /// and a run otherwise safe exits 2.
    #[tokio::test]
    async fn a_cleanup_that_hangs_is_given_up() {
        struct Hangs(ObjectStoreBucket);
        impl Bucket for Hangs {
            async fn get(&self, key: &str) -> Result<Option<(Vec<u8>, Version)>, BucketError> {
                Bucket::get(&self.0, key).await
            }
            async fn create(&self, key: &str, body: Vec<u8>) -> Result<Version, BucketError> {
                self.0.create(key, body).await
            }
            async fn replace(
                &self,
                key: &str,
                body: Vec<u8>,
                expected: &Version,
            ) -> Result<Version, BucketError> {
                self.0.replace(key, body, expected).await
            }
        }
        impl Store for Hangs {
            async fn list(&self, prefix: &str) -> Result<Vec<ObjectMeta>, BucketError> {
                Store::list(&self.0, prefix).await
            }
            async fn list_dirs(&self, prefix: &str) -> Result<Vec<String>, BucketError> {
                self.0.list_dirs(prefix).await
            }
            async fn copy(&self, from: &str, to: &str) -> Result<(), BucketError> {
                self.0.copy(from, to).await
            }
            async fn put(&self, key: &str, body: Vec<u8>) -> Result<(), BucketError> {
                Store::put(&self.0, key, body).await
            }
            async fn delete(&self, _: &str) -> Result<(), BucketError> {
                std::future::pending().await
            }
        }
        impl Target for Hangs {
            async fn head_modified_ms(&self, key: &str) -> Result<Option<u64>, BucketError> {
                self.0.head_modified_ms(key).await
            }
        }
        let (_, inner) = memory("scratch");
        let bucket = Hangs(inner);
        let t = Tracked {
            inner: &bucket,
            written: RefCell::default(),
        };
        t.put("mine", b"x".to_vec()).await.unwrap();
        let options = Options {
            check_timeout: Duration::from_millis(200),
            ..quick()
        };
        let (left, why) = bounded_cleanup(&t, &options).await;
        assert_eq!(left, None);
        assert!(why.unwrap().contains("no answer"));
    }

    #[tokio::test]
    async fn cleanup_deletes_only_what_the_run_wrote() {
        let (store, bucket) = memory("scratch");
        let t = Tracked {
            inner: &bucket,
            written: RefCell::new(BTreeSet::new()),
        };
        t.put("mine", b"x".to_vec()).await.unwrap();
        Store::put(&bucket, "theirs", b"y".to_vec()).await.unwrap();
        let (left, why) = cleanup(&t, &quick()).await;
        assert_eq!((left, why), (Some(1), None));
        assert_eq!(everything_under(&store).await, ["scratch/theirs"]);
    }

    #[tokio::test]
    async fn a_scratch_prefix_that_holds_anything_is_refused_untouched() {
        let (store, bucket) = memory("scratch");
        Store::put(&bucket, "theirs", b"keep".to_vec())
            .await
            .unwrap();
        assert_eq!(
            run(&bucket, &quick()).await.unwrap_err(),
            NotRun::NotEmpty(1)
        );
        assert_eq!(everything_under(&store).await, ["scratch/theirs"]);
    }

    #[tokio::test]
    async fn only_the_scratch_prefix_is_written() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let cluster = ObjectStoreBucket::new(Arc::clone(&store), "wk");
        Store::put(&cluster, "nodes/a", b"lease".to_vec())
            .await
            .unwrap();
        let scratch = ObjectStoreBucket::new(Arc::clone(&store), "wk/diagnose/run-1");
        let report = run(&scratch, &quick()).await.unwrap();
        assert_eq!(report.verdict(), Verdict::Safe, "{report:#?}");
        assert_eq!(everything_under(&store).await, ["wk/nodes/a"]);
    }

    /// Two runs given one `--prefix-for-test` at once: each in its own
    /// folder, both safe, and neither deletes the other's keys.
    #[tokio::test]
    async fn two_runs_at_once_do_not_meet() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        Store::put(
            &ObjectStoreBucket::new(Arc::clone(&store), "wk/shared"),
            "theirs",
            b"keep".to_vec(),
        )
        .await
        .unwrap();
        let bucket = |run: &str| {
            let scratch = scratch_for(Some("shared"), run).unwrap();
            ObjectStoreBucket::new(Arc::clone(&store), scratch_prefix("wk", &scratch).unwrap())
        };
        let (one, two) = (bucket("run-1-1"), bucket("run-1-2"));
        let options = Options {
            paging_keys: None,
            ..quick()
        };
        let (a, b) = tokio::join!(run(&one, &options), run(&two, &options));
        for report in [a.unwrap(), b.unwrap()] {
            assert_eq!(report.verdict(), Verdict::Safe, "{report:#?}");
            assert_eq!(report.left, Some(0), "{report:#?}");
        }
        assert_eq!(everything_under(&store).await, ["wk/shared/theirs"]);
    }

    #[tokio::test]
    async fn paging_can_be_skipped() {
        let (_, bucket) = memory("scratch");
        let options = Options {
            paging_keys: None,
            ..quick()
        };
        let report = run(&bucket, &options).await.unwrap();
        assert_eq!(status_of(&report, "list-paging"), Status::Skipped);
        assert_eq!(report.verdict(), Verdict::Safe);
    }

    #[test]
    fn an_error_without_a_failure_is_no_verdict() {
        let report = Report {
            checks: vec![
                check("a", Status::Pass, ""),
                check("b", Status::Error, "no answer"),
                check("c", Status::Warn, "slow"),
            ],
            left: Some(0),
            cleanup_error: None,
        };
        assert_eq!(report.verdict(), Verdict::Incomplete(vec!["b"]));
        assert_eq!(report.exit_code(), 2);
        let report = Report {
            checks: vec![check("b", Status::Error, ""), check("d", Status::Fail, "")],
            ..report
        };
        assert_eq!(report.verdict(), Verdict::Unsafe(vec!["d"]));
    }

    /// A failed cleanup keeps the verdict, but a safe run exits 2: objects
    /// are left to delete by hand. An unsafe one still exits 1.
    #[test]
    fn a_failed_cleanup_says_where_it_wrote_and_exits_2() {
        let report = Report {
            checks: vec![check("a", Status::Pass, "")],
            left: Some(3),
            cleanup_error: Some("deleting x: bucket unavailable: 503".into()),
        };
        let text = report.to_text("s3://b/p", "s3://b/p/diagnose/run-1/");
        assert!(
            text.contains("cleanup FAILED: 3 object(s) left under s3://b/p/diagnose/run-1/"),
            "{text}"
        );
        assert!(text.ends_with("safe for WasmKeep\n"), "{text}");
        assert_eq!(report.exit_code(), 2);
        let json = report.to_json("s3://b/p", "s");
        assert_eq!(json["cleanup"]["ok"], false);
        assert_eq!(json["exit_code"], 2);
        let left_only = Report {
            cleanup_error: None,
            left: Some(1),
            ..report.clone()
        };
        assert_eq!(left_only.exit_code(), 2);
        let failed = Report {
            checks: vec![check("a", Status::Fail, "")],
            ..report
        };
        assert_eq!(failed.exit_code(), 1);
    }

    #[test]
    fn skew_is_judged_on_the_whole_interval() {
        let max = 1_000;
        // Whole seconds, written within 200 ms: within tolerance.
        let judge = |stamp, precision, before, after| {
            let (lo, hi) = skew_window(stamp, precision, before, after);
            judge_skew("s", lo, hi, max).status
        };
        assert_eq!(judge(10_000, 1_000, 10_100, 10_300), Status::Pass);
        // The bucket 3 s ahead whatever the rounding.
        assert_eq!(judge(13_000, 1_000, 10_000, 10_200), Status::Fail);
        // 3 s behind.
        assert_eq!(judge(7_000, 1_000, 10_000, 10_200), Status::Fail);
        // Maybe beyond: part of the interval is out.
        assert_eq!(judge(11_000, 1_000, 10_000, 10_200), Status::Warn);
        // A second boundary between the readings widens one window past
        // the tolerance; intersected with the next, it is back within.
        assert_eq!(skew_window(10_000, 1_000, 9_900, 10_100), (-100, 1_100));
        assert_eq!(skew_window(10_000, 1_000, 10_250, 10_300), (-300, 750));
        let both = narrow(Some((-100, 1_100)), (-300, 750));
        assert_eq!(both, (-100, 750));
        assert_eq!(judge_skew("s", both.0, both.1, max).status, Status::Pass);
        assert_eq!(narrow(Some((0, 100)), (500, 600)), (0, 600));
    }

    #[test]
    fn the_scratch_prefix_is_never_the_root() {
        assert_eq!(
            scratch_prefix("wk", "diagnose/x").as_deref(),
            Some("wk/diagnose/x")
        );
        assert_eq!(scratch_prefix("", "/d/").as_deref(), Some("d"));
        for bad in ["", "/", "a//b", "../x", "a/./b"] {
            assert_eq!(scratch_prefix("wk", bad), None, "{bad:?}");
        }
    }

    /// Every run gets its own folder, under `--prefix-for-test` too.
    #[test]
    fn every_run_writes_in_its_own_folder() {
        assert_eq!(
            scratch_for(None, "run-1").as_deref(),
            Some("diagnose/run-1")
        );
        assert_eq!(
            scratch_for(Some("/diagnose/today/"), "run-1").as_deref(),
            Some("diagnose/today/run-1")
        );
        for bad in ["", "/", "a//b", "../x"] {
            assert_eq!(scratch_for(Some(bad), "run-1"), None, "{bad:?}");
        }
        assert!(run_folder().starts_with("run-"));
        assert_ne!(run_folder().split('-').nth(2), None, "run-<ms>-<pid>");
    }
}
