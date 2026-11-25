//! The one reader and the one lister of the replica: every GET of a whole
//! object and every listing of a prefix that `replica/` makes goes
//! through [`read`] and [`list`].
//!
//! Their callers do not agree on what a failure means, so the loops are
//! here and the rules are the caller's:
//!
//! - a restore's reads and an upload's read-back
//!   ([`bounded_get`](super::bounded_get)): unconditional, a refusal no
//!   resend changes ends the read, another is sent again after a pause
//!   that gives up the read's place under the node's limits, and the
//!   attempt's whole time grows with the bytes read alongside it;
//! - the lease-free history reads ([`history::get`](super::history::get):
//!   exports, backups, the change feed, the operator's views):
//!   conditional on the listed identity, a missing or changed object is a
//!   typed error the caller plans again on, every other refusal is sent
//!   again at once, and the attempt's whole time is fixed.
//!
//! What is the same for all of them is here: [`READ_ATTEMPTS`] attempts,
//! an attempt given up when the bucket is silent for [`READ_IDLE`] or
//! when its whole time is spent, nothing kept of an attempt given up,
//! and a body that is exactly as long as its answer declared, of which
//! no more than that is ever kept.

use std::time::Duration;

use futures_util::StreamExt;
use object_store::path::Path as StorePath;
use object_store::{GetOptions, ObjectMeta, ObjectStore};

use super::{READ_ATTEMPTS, READ_IDLE};

/// Where a refused request was in its answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Phase {
    /// The request itself: no answer yet.
    Headers,
    /// The body, some of it perhaps read.
    Body,
}

/// Why an attempt of a read ended without the object, another attempt
/// being allowed.
#[derive(Debug)]
pub(super) enum Fault {
    /// No answer within [`READ_IDLE`].
    NoAnswer,
    /// The bucket refused the request, or the body broke.
    Refused(object_store::Error),
    /// A wait for the body's next bytes ran out.
    Waited {
        /// Bytes of the body read so far.
        read: usize,
        /// The size the answer declared.
        size: u64,
        /// The attempt's whole time, as last looked at.
        limit: Duration,
        /// The attempt's whole time is spent.
        spent: bool,
        /// The body was silent for [`READ_IDLE`].
        silent: bool,
    },
    /// The body ended short of the size the answer declared.
    Short {
        /// Bytes of the body read.
        read: usize,
        /// The size the answer declared.
        size: u64,
    },
    /// The body went past the size the answer declared.
    Long {
        /// The size the answer declared.
        size: u64,
    },
}

/// The most reserved for a body before any of it is read: the buffer
/// grows with what comes, so an answer that declares gigabytes (within
/// its caller's cap) costs nothing until it sends them.
const RESERVED: u64 = 8 << 20;

/// A caller's rules for [`read`].
pub(super) trait ReadRules {
    /// The caller's error.
    type Error;

    /// The request's conditions.
    fn options(&self) -> GetOptions;

    /// An attempt after the first is about to start.
    fn next_try(&mut self) {}

    /// The whole time of the attempt in progress, from its request:
    /// looked at when the attempt starts and at every wait for its body,
    /// so it may grow meanwhile. Never under [`READ_IDLE`].
    fn limit(&self) -> Duration;

    /// A refusal that ends the read: `Some`, the error to return; `None`
    /// for one that is a failed attempt ([`Fault::Refused`]).
    fn ends(&self, refusal: &object_store::Error, phase: Phase) -> Option<Self::Error>;

    /// The answer's metadata, before any of its body is read: an object
    /// that is not the one wanted, or too large, ends the read.
    fn accept(&self, answered: &ObjectMeta) -> Result<(), Self::Error>;

    /// The body went past the `size` its answer declared, and no more of
    /// it is read: `Some`, the error that ends the read; `None` for a
    /// failed attempt ([`Fault::Long`]).
    fn too_long(&self, size: u64) -> Option<Self::Error>;

    /// An attempt failed, nothing of its answer kept; `more` if another
    /// follows. The caller notes why, and may pause here before the next
    /// attempt. An error ends the read.
    async fn failed(&mut self, fault: Fault, more: bool) -> Result<(), Self::Error>;

    /// Every attempt failed.
    fn gave_up(self) -> Self::Error;
}

/// Reads the object at `location` whole, under `rules`, in at most
/// [`READ_ATTEMPTS`] attempts.
///
/// An attempt is one GET. It is given up when the bucket is silent for
/// [`READ_IDLE`] (no answer, or no next bytes of the body), or when it
/// has taken its whole time ([`ReadRules::limit`]). An attempt given up
/// is dropped whole and the next reads the object again from its start.
/// The body returned is exactly as long as its answer declared: a longer
/// one is given up as it passes that size (so a lying body allocates no
/// more than the caller accepted in [`ReadRules::accept`]), and ends the
/// read or is a failed attempt as the caller says
/// ([`ReadRules::too_long`]); a shorter one is a failed attempt.
pub(super) async fn read<R: ReadRules>(
    store: &dyn ObjectStore,
    location: &StorePath,
    mut rules: R,
) -> Result<bytes::Bytes, R::Error> {
    'attempts: for attempt in 1..=READ_ATTEMPTS {
        let more = attempt < READ_ATTEMPTS;
        if attempt > 1 {
            rules.next_try();
        }
        // The attempt's whole time counts from its request.
        let start = tokio::time::Instant::now();
        let limit = rules.limit();
        let answer = tokio::time::timeout_at(
            (start + limit).min(start + READ_IDLE),
            store.get_opts(location, rules.options()),
        )
        .await;
        let got = match answer {
            Ok(Ok(got)) => got,
            Ok(Err(e)) => {
                if let Some(end) = rules.ends(&e, Phase::Headers) {
                    return Err(end);
                }
                rules.failed(Fault::Refused(e), more).await?;
                continue;
            }
            Err(_) => {
                rules.failed(Fault::NoAnswer, more).await?;
                continue;
            }
        };
        rules.accept(&got.meta)?;
        let size = got.meta.size;
        let mut body = got.into_stream();
        let mut bytes = Vec::with_capacity(usize::try_from(size.min(RESERVED)).unwrap_or(0));
        loop {
            let asked = tokio::time::Instant::now();
            let limit = rules.limit();
            let next =
                tokio::time::timeout_at((start + limit).min(asked + READ_IDLE), body.next()).await;
            let fault = match next {
                Ok(Some(Ok(chunk))) => {
                    if (bytes.len() as u64).saturating_add(chunk.len() as u64) <= size {
                        bytes.extend_from_slice(&chunk);
                        continue;
                    }
                    if let Some(end) = rules.too_long(size) {
                        return Err(end);
                    }
                    Fault::Long { size }
                }
                Ok(Some(Err(e))) => {
                    if let Some(end) = rules.ends(&e, Phase::Body) {
                        return Err(end);
                    }
                    Fault::Refused(e)
                }
                Ok(None) if bytes.len() as u64 == size => return Ok(bytes.into()),
                Ok(None) => Fault::Short {
                    read: bytes.len(),
                    size,
                },
                Err(_) => {
                    let now = tokio::time::Instant::now();
                    Fault::Waited {
                        read: bytes.len(),
                        size,
                        limit,
                        spent: now >= start + limit,
                        silent: now >= asked + READ_IDLE,
                    }
                }
            };
            // Nothing of this answer is kept while the caller pauses.
            drop((body, bytes));
            rules.failed(fault, more).await?;
            continue 'attempts;
        }
    }
    Err(rules.gave_up())
}

/// Why [`list`] returned no listing.
#[derive(Debug)]
pub(super) enum ListError {
    /// The bucket refused the listing: it is not made again.
    Refused(object_store::Error),
    /// The bucket listed more than this many objects.
    Over(usize),
    /// The listing went silent for `idle` in each of `attempts` attempts.
    Stalled {
        /// The idle limit.
        idle: Duration,
        /// The attempts made.
        attempts: u32,
    },
}

/// A caller's limits for [`list`].
#[derive(Debug, Clone, Copy, Default)]
pub(super) struct ListLimits {
    /// Give up an attempt when no key comes for this long (a page of up
    /// to 1,000 keys is one request), and list again from the start, up
    /// to [`READ_ATTEMPTS`] times. `None`: one attempt, which waits as
    /// long as the bucket takes.
    pub(super) idle: Option<Duration>,
    /// Refuse a listing of more objects than this ([`ListError::Over`]),
    /// counting every object the bucket lists, under the prefix or not.
    pub(super) max: Option<usize>,
    /// Log each attempt given up, as a restore's listing does.
    pub(super) log: bool,
}

/// The objects under `prefix`, each with its key relative to the prefix
/// and the metadata the listing gave (its exact location, size, ETag and
/// last-modified time), in the order the bucket listed them. An empty
/// prefix lists the whole bucket.
///
/// A long listing is slow but is never cut while keys keep coming; one
/// given up is dropped whole, and nothing of it is returned.
pub(super) async fn list(
    store: &dyn ObjectStore,
    prefix: &str,
    limits: ListLimits,
) -> Result<Vec<(String, ObjectMeta)>, ListError> {
    let base = StorePath::from(prefix.trim_end_matches('/'));
    // Listed locations come back in the store's encoding, where a `%` in a
    // segment (as in an escaped actor name) is escaped again: compare with
    // the prefix in that same encoding, not with the raw string.
    let encoded = if base.as_ref().is_empty() {
        String::new()
    } else {
        format!("{}/", base.as_ref())
    };
    let attempts = if limits.idle.is_some() {
        READ_ATTEMPTS
    } else {
        1
    };
    'attempts: for _ in 0..attempts {
        let mut listing = store.list(Some(&base));
        let mut objects = Vec::new();
        let mut count = 0usize;
        loop {
            let next = match limits.idle {
                Some(idle) => match tokio::time::timeout(idle, listing.next()).await {
                    Ok(next) => next,
                    Err(_) => {
                        if limits.log {
                            tracing::info!(prefix, "a restore's listing stalled; listing again");
                        }
                        continue 'attempts;
                    }
                },
                None => listing.next().await,
            };
            match next {
                Some(Ok(meta)) => {
                    count += 1;
                    if let Some(max) = limits.max
                        && count > max
                    {
                        return Err(ListError::Over(max));
                    }
                    if let Some(key) = meta.location.as_ref().strip_prefix(encoded.as_str()) {
                        objects.push((key.to_owned(), meta));
                    }
                }
                Some(Err(e)) => return Err(ListError::Refused(e)),
                None => return Ok(objects),
            }
        }
    }
    if limits.log {
        tracing::warn!(prefix, "a restore's listing kept stalling");
    }
    Err(ListError::Stalled {
        idle: limits.idle.unwrap_or_default(),
        attempts,
    })
}
