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

    /// Where under PREFIX the checks write (deleted afterwards). Refused if
    /// it already holds an object. [default: diagnose/run-<ms>-<pid>]
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
    /// The longest one check may take.
    pub(crate) check_timeout: Duration,
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
                "ok": self.left == Some(0) && self.cleanup_error.is_none(),
                "left": self.left,
                "error": self.cleanup_error,
            },
            "verdict": match verdict {
                Verdict::Safe => "safe",
                Verdict::Unsafe(_) => "unsafe",
                Verdict::Incomplete(_) => "incomplete",
            },
            "summary": verdict.text(),
            "exit_code": verdict.code(),
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
    match Store::list(target, "").await {
        Ok(found) if !found.is_empty() => return Err(NotRun::NotEmpty(found.len())),
        Ok(_) => {}
        Err(e) => return Err(NotRun::Unreachable(e.to_string())),
    }
    let t = Tracked {
        inner: target,
        written: RefCell::new(BTreeSet::new()),
    };
    let mut checks = Vec::new();
    // Each check is bounded and returns its outcome: none can skip the
    // cleanup below.
    macro_rules! bounded {
        ($name:expr, $fut:expr) => {{
            let outcome = match tokio::time::timeout(options.check_timeout, $fut).await {
                Ok(c) => c,
                Err(_) => check(
                    $name,
                    Status::Error,
                    format!("no answer within {} s", options.check_timeout.as_secs_f64()),
                ),
            };
            checks.push(outcome);
        }};
    }
    bounded!("start-check", start_check(&t));
    bounded!("create-if-absent", create_if_absent(&t));
    bounded!("concurrent-creates", concurrent_creates(&t));
    bounded!("replace-if-match", replace_if_match(&t));
    bounded!("read-after-write", read_after_write(&t));
    bounded!("read-after-overwrite", read_after_overwrite(&t));
    let (seen, gone) = list_lag(&t, options).await;
    checks.push(seen);
    checks.push(gone);
    bounded!("delete-absent", delete_absent(&t));
    bounded!("large-object", large_object(&t, options.large_bytes));
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
    let (left, cleanup_error) = cleanup(&t, options).await;
    Ok(Report {
        checks,
        left,
        cleanup_error,
    })
}

/// Deletes every key written (and any other under the scratch prefix),
/// then lists until the prefix shows empty or the listing wait runs out.
async fn cleanup<T: Target>(
    t: &Tracked<'_, T>,
    options: &Options,
) -> (Option<usize>, Option<String>) {
    let mut keys: BTreeSet<String> = t.written.borrow().clone();
    let mut error = None;
    match t.list("").await {
        Ok(listed) => keys.extend(listed.into_iter().map(|(k, _, _)| k)),
        Err(e) => error = Some(format!("listing: {e}")),
    }
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

/// `If-None-Match: *`: a create over an existing object is refused, and
/// one after a delete succeeds (an ownership record removed and taken
/// again).
async fn create_if_absent<T: Target>(t: &Tracked<'_, T>) -> Check {
    const NAME: &str = "create-if-absent";
    let key = "create/k";
    try_or!(NAME, "creating", t.create(key, b"1".to_vec()).await);
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
    match t.create(key, b"3".to_vec()).await {
        Ok(_) => check(
            NAME,
            Status::Pass,
            "create over an existing object refused; create after a delete accepted",
        ),
        Err(BucketError::Conflict) => check(
            NAME,
            Status::Fail,
            "a create after a delete was refused (the delete did not take effect)",
        ),
        Err(e) => error(NAME, "creating after the delete", &e),
    }
}

/// Eight creates of one key at once: exactly one wins.
async fn concurrent_creates<T: Target>(t: &Tracked<'_, T>) -> Check {
    const NAME: &str = "concurrent-creates";
    const N: usize = 8;
    let results = join_all((0..N).map(|i| t.create("race/k", format!("{i}").into_bytes()))).await;
    let won = results.iter().filter(|r| r.is_ok()).count();
    let lost = results
        .iter()
        .filter(|r| matches!(r, Err(BucketError::Conflict)))
        .count();
    if won > 1 {
        return check(
            NAME,
            Status::Fail,
            format!("{won} of {N} simultaneous creates of one key succeeded"),
        );
    }
    if won == 1 && lost == N - 1 {
        return check(
            NAME,
            Status::Pass,
            format!("1 of {N} simultaneous creates won, {lost} refused"),
        );
    }
    let why = results
        .iter()
        .find_map(|r| match r {
            Err(BucketError::Unavailable(e)) => Some(e.clone()),
            _ => None,
        })
        .unwrap_or_else(|| "no create won".into());
    check(
        NAME,
        Status::Error,
        format!("{won} won, {lost} refused, the rest failed: {why}"),
    )
}

/// `If-Match`: a replace with the current version (the write's or a
/// read's) is accepted, one with a stale version refused, and one of a
/// deleted object refused (a lease deleted underneath its holder reads as
/// replaced).
async fn replace_if_match<T: Target>(t: &Tracked<'_, T>) -> Check {
    const NAME: &str = "replace-if-match";
    let key = "replace/k";
    let v1 = try_or!(NAME, "creating", t.create(key, b"1".to_vec()).await);
    let v2 = try_or!(
        NAME,
        "replacing with the current version",
        t.replace(key, b"2".to_vec(), &v1).await
    );
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
    let v3 = match t.replace(key, b"3".to_vec(), &read).await {
        Ok(v) => v,
        Err(BucketError::Conflict) => {
            return check(
                NAME,
                Status::Fail,
                "a replace with the version a read returned was refused",
            );
        }
        Err(e) => return error(NAME, "replacing with a read's version", &e),
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
            "current version (written or read) accepted; stale and deleted refused",
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

/// A read right after a replace or an overwrite returns the new body.
async fn read_after_overwrite<T: Target>(t: &Tracked<'_, T>) -> Check {
    const NAME: &str = "read-after-overwrite";
    let key = "rao/k";
    let mut version = try_or!(NAME, "creating", t.create(key, b"0".to_vec()).await);
    for i in 1..=ROUNDS {
        let body = format!("replaced {i}").into_bytes();
        try_or!(
            NAME,
            "replacing",
            t.replace(key, body.clone(), &version).await
        );
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
        version = read;
    }
    check(
        NAME,
        Status::Pass,
        format!(
            "{} of {} reads right after a replace or overwrite were current",
            2 * ROUNDS,
            2 * ROUNDS
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

/// A large object (a database snapshot is one PUT) comes back whole.
async fn large_object<T: Target>(t: &Tracked<'_, T>, len: usize) -> Check {
    const NAME: &str = "large-object";
    let body = pattern(len);
    let up = Instant::now();
    try_or!(NAME, "writing", t.put("large/k", body.clone()).await);
    let up = up.elapsed();
    let down = Instant::now();
    let got = try_or!(NAME, "reading", t.get("large/k").await);
    let down = down.elapsed();
    let listed = try_or!(NAME, "listing", t.list("large/").await);
    let mib = len as f64 / f64::from(1u32 << 20);
    match got {
        Some((got, _)) if got == body => {
            if listed.first().map(|(_, _, size)| *size) != Some(len as u64) {
                return check(
                    NAME,
                    Status::Fail,
                    format!("the listing gives the wrong size: {listed:?}"),
                );
            }
            check(
                NAME,
                Status::Pass,
                format!(
                    "{mib:.0} MiB round trip intact (write {:.1} s, read {:.1} s)",
                    up.as_secs_f64(),
                    down.as_secs_f64()
                ),
            )
        }
        Some((got, _)) => check(
            NAME,
            Status::Fail,
            format!(
                "{mib:.0} MiB written, {} bytes read back, not the same",
                got.len()
            ),
        ),
        None => check(NAME, Status::Fail, "a large object read back as absent"),
    }
}

/// Last-modified times as the migration and `cluster fence` use them: a
/// later write (or overwrite) never lists earlier, one more than a second
/// later lists later, and a head and a listing agree within a second.
async fn last_modified<T: Target>(t: &Tracked<'_, T>, options: &Options) -> Check {
    const NAME: &str = "last-modified";
    let time = |listed: &[(String, u64, u64)], key: &str| {
        listed.iter().find(|(k, _, _)| k == key).map(|(_, m, _)| *m)
    };
    try_or!(NAME, "writing", t.put("mod/a", b"1".to_vec()).await);
    try_or!(NAME, "writing", t.put("mod/b", b"1".to_vec()).await);
    let first = try_or!(NAME, "listing", t.list("mod/").await);
    let (Some(a1), Some(b1)) = (time(&first, "mod/a"), time(&first, "mod/b")) else {
        return check(
            NAME,
            Status::Fail,
            "a listing right after two writes missed one",
        );
    };
    if a1 == 0 || b1 == 0 {
        return check(
            NAME,
            Status::Fail,
            "the listing gives no last-modified time",
        );
    }
    if b1 < a1 {
        return check(
            NAME,
            Status::Fail,
            format!("a later write lists earlier ({b1} < {a1} ms)"),
        );
    }
    tokio::time::sleep(options.modified_gap).await;
    try_or!(NAME, "writing", t.put("mod/c", b"1".to_vec()).await);
    try_or!(NAME, "overwriting", t.put("mod/a", b"2".to_vec()).await);
    let second = try_or!(NAME, "listing", t.list("mod/").await);
    let (Some(a2), Some(c)) = (time(&second, "mod/a"), time(&second, "mod/c")) else {
        return check(
            NAME,
            Status::Fail,
            "a listing right after two writes missed one",
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
    let whole = [a1, b1, a2, c, head].iter().all(|m| m % 1_000 == 0);
    check(
        NAME,
        Status::Pass,
        format!(
            "{} precision, in write order; head and listing agree within {diff} ms",
            if whole { "whole-second" } else { "millisecond" }
        ),
    )
}

/// This machine's clock against the bucket's, as the last-modified time
/// of a write it brackets (the clock `cluster fence` checks against). A
/// time in whole seconds widens the bound by a second.
async fn clock_skew<T: Target>(t: &Tracked<'_, T>, lease: &LeaseConfig) -> Check {
    const NAME: &str = "clock-skew";
    let before = now_ms();
    try_or!(NAME, "writing", t.put("clock/k", Vec::new()).await);
    let after = now_ms();
    let Some(modified) = try_or!(NAME, "head", t.inner.head_modified_ms("clock/k").await) else {
        return check(
            NAME,
            Status::Fail,
            "a head right after a write found nothing",
        );
    };
    let precision = if modified % 1_000 == 0 { 1_000 } else { 1 };
    judge_skew(
        NAME,
        modified,
        precision,
        before,
        after,
        lease.max_skew_ms(),
    )
}

/// The bucket's time `stamp` (to `precision` ms, rounded down) was taken
/// between this machine's `before` and `after`: the bucket's clock minus
/// this machine's is in `[stamp - after, stamp + precision - before]`.
/// Fails when all of it is beyond `max_skew_ms`, warns when part is.
fn judge_skew(
    name: &'static str,
    stamp: u64,
    precision: u64,
    before: u64,
    after: u64,
    max_skew_ms: u64,
) -> Check {
    let lo = i128::from(stamp) - i128::from(after);
    let hi = i128::from(stamp) + i128::from(precision) - i128::from(before);
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

/// The default scratch prefix: unique to this run.
fn default_scratch() -> String {
    format!("diagnose/run-{}-{}", now_ms(), std::process::id())
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
/// run or not finished).
pub(crate) fn command(args: &DiagnoseArgs) -> i32 {
    let fail = |msg: String| {
        eprintln!("error: {msg}");
        2
    };
    let (store, url_prefix) = match crate::open_bucket_unchecked(&args.bucket) {
        Ok(v) => v,
        Err(e) => return fail(format!("{e:#}")),
    };
    let scratch = args.prefix_for_test.clone().unwrap_or_else(default_scratch);
    let Some(prefix) = scratch_prefix(&url_prefix, &scratch) else {
        return fail(format!(
            "--prefix-for-test must name a folder, got {scratch:?}"
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
    let report = match rt.block_on(async {
        tokio::time::timeout(Duration::from_secs(60), run(&target, &options))
            .await
            .unwrap_or_else(|_| Err(NotRun::Unreachable("no answer within 60 s".into())))
    }) {
        Ok(r) => r,
        Err(NotRun::NotEmpty(n)) => {
            return fail(format!(
                "{scratch_url} already holds {n} object(s): refusing to write there \
                 (give an empty --prefix-for-test)"
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
    if report.left != Some(0) || report.cleanup_error.is_some() {
        eprintln!("warning: {}", report.cleanup_text(&scratch_url));
    }
    report.verdict().code()
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::panic)] // test code

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
        }
    }

    fn memory(prefix: &str) -> (Arc<dyn ObjectStore>, ObjectStoreBucket) {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        (Arc::clone(&store), ObjectStoreBucket::new(store, prefix))
    }

    /// A store that accepts every conditional write as a plain one: what a
    /// provider that ignores `If-None-Match` and `If-Match` does.
    struct Lax {
        inner: ObjectStoreBucket,
        create: bool,
        replace: bool,
    }

    impl Lax {
        async fn overwrite(&self, key: &str, body: Vec<u8>) -> Result<Version, BucketError> {
            Store::put(&self.inner, key, body).await?;
            Ok(Bucket::get(&self.inner, key)
                .await?
                .map(|(_, v)| v)
                .unwrap_or(Version(String::new())))
        }
    }

    impl Bucket for Lax {
        async fn get(&self, key: &str) -> Result<Option<(Vec<u8>, Version)>, BucketError> {
            Bucket::get(&self.inner, key).await
        }
        async fn create(&self, key: &str, body: Vec<u8>) -> Result<Version, BucketError> {
            if self.create {
                self.overwrite(key, body).await
            } else {
                self.inner.create(key, body).await
            }
        }
        async fn replace(
            &self,
            key: &str,
            body: Vec<u8>,
            expected: &Version,
        ) -> Result<Version, BucketError> {
            if self.replace {
                self.overwrite(key, body).await
            } else {
                self.inner.replace(key, body, expected).await
            }
        }
    }

    impl Store for Lax {
        async fn list(&self, prefix: &str) -> Result<Vec<ObjectMeta>, BucketError> {
            Store::list(&self.inner, prefix).await
        }
        async fn list_dirs(&self, prefix: &str) -> Result<Vec<String>, BucketError> {
            self.inner.list_dirs(prefix).await
        }
        async fn copy(&self, from: &str, to: &str) -> Result<(), BucketError> {
            self.inner.copy(from, to).await
        }
        async fn put(&self, key: &str, body: Vec<u8>) -> Result<(), BucketError> {
            Store::put(&self.inner, key, body).await
        }
        async fn delete(&self, key: &str) -> Result<(), BucketError> {
            Store::delete(&self.inner, key).await
        }
    }

    impl Target for Lax {
        async fn head_modified_ms(&self, key: &str) -> Result<Option<u64>, BucketError> {
            self.inner.head_modified_ms(key).await
        }
    }

    fn status_of(report: &Report, name: &str) -> Status {
        report
            .checks
            .iter()
            .find(|c| c.name == name)
            .unwrap_or_else(|| panic!("no check {name}: {report:?}"))
            .status
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
        assert_eq!(report.left, Some(0));
        assert_eq!(everything_under(&store).await, Vec::<String>::new());
        let text = report.to_text("s3://b/wk", "s3://b/wk/diagnose/run-1/");
        assert!(text.ends_with("safe for WasmKeep\n"), "{text}");
        assert_eq!(report.verdict().code(), 0);
        let json = report.to_json("s3://b/wk", "s3://b/wk/diagnose/run-1/");
        assert_eq!(json["verdict"], "safe");
        assert_eq!(
            json["checks"].as_array().unwrap().len(),
            report.checks.len()
        );
    }

    #[tokio::test]
    async fn a_store_that_ignores_conditions_is_not_safe() {
        for (create, replace, broken) in [
            (
                true,
                true,
                &[
                    "start-check",
                    "create-if-absent",
                    "concurrent-creates",
                    "replace-if-match",
                ][..],
            ),
            (
                true,
                false,
                &["start-check", "create-if-absent", "concurrent-creates"][..],
            ),
            (false, true, &["start-check", "replace-if-match"][..]),
        ] {
            let (store, inner) = memory("scratch");
            let lax = Lax {
                inner,
                create,
                replace,
            };
            let report = run(&lax, &quick()).await.unwrap();
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
            assert_eq!(report.verdict().code(), 1);
            let text = report.to_text("s3://b", "s3://b/scratch/");
            assert!(text.contains("not safe: start-check"), "{text}");
            assert_eq!(
                report.to_json("s3://b", "s3://b/scratch/")["verdict"],
                "unsafe"
            );
            // Cleaned up even though it failed.
            assert_eq!(everything_under(&store).await, Vec::<String>::new());
        }
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
        assert_eq!(report.verdict().code(), 2);
        let report = Report {
            checks: vec![check("b", Status::Error, ""), check("d", Status::Fail, "")],
            ..report
        };
        assert_eq!(report.verdict(), Verdict::Unsafe(vec!["d"]));
    }

    #[test]
    fn a_failed_cleanup_says_where_it_wrote() {
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
        assert_eq!(report.to_json("s3://b/p", "s")["cleanup"]["ok"], false);
    }

    #[test]
    fn skew_is_judged_on_the_whole_interval() {
        let max = 1_000;
        // Whole seconds, written within 200 ms: within tolerance.
        let c = judge_skew("s", 10_000, 1_000, 10_100, 10_300, max);
        assert_eq!(c.status, Status::Pass, "{c:?}");
        // The bucket 3 s ahead whatever the rounding.
        let c = judge_skew("s", 13_000, 1_000, 10_000, 10_200, max);
        assert_eq!(c.status, Status::Fail, "{c:?}");
        // 3 s behind.
        let c = judge_skew("s", 7_000, 1_000, 10_000, 10_200, max);
        assert_eq!(c.status, Status::Fail, "{c:?}");
        // Maybe beyond: part of the interval is out.
        let c = judge_skew("s", 11_000, 1_000, 10_000, 10_200, max);
        assert_eq!(c.status, Status::Warn, "{c:?}");
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
        assert!(default_scratch().starts_with("diagnose/run-"));
    }
}
