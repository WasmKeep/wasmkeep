//! One mailbox per actor: events run one at a time, in arrival order.
//!
//! Each resident actor is a tokio task that owns its state `S` (in practice
//! a Wasmtime store plus its database handle) and runs queued jobs strictly
//! sequentially. Different actors run in parallel. After `idle_timeout`
//! without events the task drops its state and exits (hibernation); the next
//! event creates it again through the factory.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::{mpsc, oneshot};

use crate::{ActorAddress, Residency};

/// A boxed, `Send` future borrowing for `'a`.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Error type a factory may return.
pub type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

type Factory<S> =
    dyn Fn(ActorAddress, Residency) -> BoxFuture<'static, Result<S, BoxError>> + Send + Sync;
/// A queued event. It receives the actor state, or the error that kept the
/// state from being created, and reports to its caller either way.
type Job<S> = Box<dyn for<'a> FnOnce(Result<&'a mut S, MailboxError>) -> BoxFuture<'a, ()> + Send>;

/// Mailbox settings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MailboxConfig {
    /// Events that may wait in one actor's queue; more are refused with
    /// [`MailboxError::Full`].
    pub queue_capacity: usize,
    /// How long an actor stays resident without events before its state is
    /// dropped.
    pub idle_timeout: Duration,
    /// Maximum actors whose tasks or retained resources may still be alive.
    /// At this limit, a new name is refused with [`MailboxError::Capacity`]
    /// and the least recently active idle actor is asked to hibernate.
    /// A retry can enter only after the old actor's resources have closed.
    pub max_resident: usize,
}

impl Default for MailboxConfig {
    fn default() -> Self {
        Self {
            queue_capacity: 1024,
            idle_timeout: Duration::from_secs(30),
            max_resident: usize::MAX,
        }
    }
}

/// Why an event did not run.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum MailboxError {
    /// The actor's queue is full; retry later (maps to 429).
    #[error("actor {0} is overloaded")]
    Full(ActorAddress),
    /// The event did not run because resident capacity is unavailable.
    /// Retry after retiring actors or a failed state's resources close.
    #[error("no resident capacity for actor {0}")]
    Capacity(ActorAddress),
    /// Creating the actor's state failed. The next event tries again.
    #[error("actor {address} failed to start")]
    Init {
        /// The actor.
        address: ActorAddress,
        /// What the factory returned.
        #[source]
        source: BoxError,
    },
    /// The event did not complete: it panicked (a host bug; the actor's
    /// in-memory state is discarded and later events run normally), or the
    /// runtime is shutting down. The event may have partly run.
    #[error("actor {0} stopped before answering")]
    Gone(ActorAddress),
    /// The node is shutting down: it takes no new events.
    #[error("not accepting events for {0}: shutting down")]
    Closed(ActorAddress),
}

/// The set of resident actors and their queues. Cheap to clone.
pub struct Mailboxes<S> {
    inner: Arc<Inner<S>>,
}

impl<S> Clone for Mailboxes<S> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<S> std::fmt::Debug for Mailboxes<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Mailboxes")
            .field("config", &self.inner.config)
            .field("resident", &self.resident())
            .finish()
    }
}

struct Inner<S> {
    config: MailboxConfig,
    factory: Box<Factory<S>>,
    actors: Mutex<Registry<S>>,
    /// Set by `close`: actors finish what is queued, then drop their state.
    closed: tokio::sync::watch::Sender<bool>,
    /// One count per admitted actor, retained until its task, state and all
    /// cloned residencies have dropped (deregistration happens earlier).
    tasks: Arc<std::sync::atomic::AtomicUsize>,
    /// Final resource release, an actor becoming idle, or shutdown may let
    /// a bounded admission wait make progress.
    capacity_changed: Arc<tokio::sync::Notify>,
    residency_released: Arc<dyn Fn() + Send + Sync>,
    /// `submit_wait` callers currently waiting for a slot: how many slots
    /// evictions should be freeing.
    waiting: std::sync::atomic::AtomicUsize,
}

impl<S: Send + 'static> Mailboxes<S> {
    /// Creates an empty set. `factory` builds an actor's state the first
    /// time it handles an event (and again after hibernation).
    pub fn new<F, Fut>(config: MailboxConfig, factory: F) -> Self
    where
        F: Fn(ActorAddress) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<S, BoxError>> + Send + 'static,
    {
        Self::new_with_residency(config, move |address, _residency| factory(address))
    }

    /// Creates mailboxes whose factory may retain its capacity reservation.
    /// Clone the [`Residency`] into resources or blocking work that may
    /// outlive the actor task, and release it after those resources close.
    /// The mailbox itself also holds the reservation until its task and state drop.
    pub fn new_with_residency<F, Fut>(config: MailboxConfig, factory: F) -> Self
    where
        F: Fn(ActorAddress, Residency) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<S, BoxError>> + Send + 'static,
    {
        let capacity_changed = Arc::new(tokio::sync::Notify::new());
        let changed = Arc::clone(&capacity_changed);
        let residency_released = Arc::new(move || changed.notify_waiters());
        Self {
            inner: Arc::new(Inner {
                config,
                factory: Box::new(move |address, residency| Box::pin(factory(address, residency))),
                actors: Mutex::new(HashMap::new()),
                closed: tokio::sync::watch::Sender::new(false),
                tasks: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
                capacity_changed,
                residency_released,
                waiting: std::sync::atomic::AtomicUsize::new(0),
            }),
        }
    }

    /// Queues `job` for `address` and returns a future for its result.
    ///
    /// Accepted jobs are queued before this function returns, so jobs submitted
    /// in sequence run in that order. It runs with exclusive access to the
    /// actor's state; no other job for this actor runs until it finishes.
    pub fn submit<T, F>(
        &self,
        address: &ActorAddress,
        job: F,
    ) -> impl Future<Output = Result<T, MailboxError>> + Send + 'static
    where
        T: Send + 'static,
        F: for<'a> FnOnce(&'a mut S) -> BoxFuture<'a, T> + Send + 'static,
    {
        let (reply, answer) = oneshot::channel::<Result<T, MailboxError>>();
        let job: Job<S> = Box::new(move |state| {
            Box::pin(async move {
                let result = match state {
                    Ok(state) => Ok(job(state).await),
                    Err(e) => Err(e),
                };
                let _ = reply.send(result);
            })
        });
        let queued = self
            .enqueue(address, job, false)
            .map_err(|(error, _)| error);
        let address = address.clone();
        async move {
            queued?;
            answer.await.map_err(|_| MailboxError::Gone(address))?
        }
    }

    /// Like [`Self::submit`], only to an actor already registered: never
    /// creates a mailbox (nor evicts another actor for one). `Gone` if the
    /// actor is not resident.
    pub fn submit_if_resident<T, F>(
        &self,
        address: &ActorAddress,
        job: F,
    ) -> impl Future<Output = Result<T, MailboxError>> + Send + 'static
    where
        T: Send + 'static,
        F: for<'a> FnOnce(&'a mut S) -> BoxFuture<'a, T> + Send + 'static,
    {
        let (reply, answer) = oneshot::channel::<Result<T, MailboxError>>();
        let job: Job<S> = Box::new(move |state| {
            Box::pin(async move {
                let result = match state {
                    Ok(state) => Ok(job(state).await),
                    Err(e) => Err(e),
                };
                let _ = reply.send(result);
            })
        });
        let queued = {
            let actors = lock(&self.inner);
            if *self.inner.closed.borrow() {
                Err(MailboxError::Closed(address.clone()))
            } else {
                match actors.get(address) {
                    Some(entry) => entry.tx.try_send(job).map_err(|e| match e {
                        mpsc::error::TrySendError::Full(_) => MailboxError::Full(address.clone()),
                        mpsc::error::TrySendError::Closed(_) => MailboxError::Gone(address.clone()),
                    }),
                    None => Err(MailboxError::Gone(address.clone())),
                }
            }
        };
        let address = address.clone();
        async move {
            queued?;
            answer.await.map_err(|_| MailboxError::Gone(address))?
        }
    }

    /// Queues immediately when possible, like [`Self::submit`]. A cold
    /// address may wait at most `limit` for capacity while an idle actor
    /// closes its resources. Only an unaccepted job is retried; accepted
    /// events keep their normal ordering and are never replayed.
    /// Dropping this future cancels an unaccepted job; an accepted job runs
    /// even when its caller stops waiting, as with `submit`.
    pub fn submit_wait<T, F>(
        &self,
        address: &ActorAddress,
        limit: Duration,
        job: F,
    ) -> impl Future<Output = Result<T, MailboxError>> + Send + 'static
    where
        T: Send + 'static,
        F: for<'a> FnOnce(&'a mut S) -> BoxFuture<'a, T> + Send + 'static,
    {
        let (reply, answer) = oneshot::channel();
        let job: Job<S> = Box::new(move |state| {
            Box::pin(async move {
                let result = match state {
                    Ok(state) => Ok(job(state).await),
                    Err(error) => Err(error),
                };
                let _ = reply.send(result);
            })
        });
        let deadline = tokio::time::Instant::now() + limit;
        let mailboxes = self.clone();
        let address = address.clone();
        // Register before checking capacity, including before the returned
        // future is polled, so even a quick retirement cannot lose its wake.
        let mut changed = Box::pin(Arc::clone(&self.inner.capacity_changed).notified_owned());
        changed.as_mut().enable();
        // Counted as demand while it waits, so the wake-ups it gets (after
        // any event on the node) evict only while fewer slots are on their
        // way back than callers are waiting for.
        let mut demand = Some(Waiting::new(Arc::clone(&self.inner)));
        let first = self.enqueue(&address, job, true);
        async move {
            let mut attempt = first;
            loop {
                match attempt {
                    Ok(()) => {
                        demand.take();
                        break;
                    }
                    Err((error @ MailboxError::Capacity(_), job)) => {
                        if tokio::time::Instant::now() >= deadline {
                            return Err(error);
                        }
                        tokio::select! {
                            biased;
                            () = tokio::time::sleep_until(deadline) => return Err(error),
                            () = &mut changed => {}
                        }
                        changed = Box::pin(
                            Arc::clone(&mailboxes.inner.capacity_changed).notified_owned(),
                        );
                        changed.as_mut().enable();
                        attempt = mailboxes.enqueue(&address, job, true);
                    }
                    Err((error, _)) => return Err(error),
                }
            }
            answer.await.map_err(|_| MailboxError::Gone(address))?
        }
    }

    /// `counted`: the caller is a `submit_wait` already counted in
    /// `waiting`.
    fn enqueue(
        &self,
        address: &ActorAddress,
        job: Job<S>,
        counted: bool,
    ) -> Result<(), (MailboxError, Job<S>)> {
        let mut actors = lock(&self.inner);
        // Checked under the registry lock that `close` takes too, so no job
        // slips in after an actor decided its queue was empty.
        if *self.inner.closed.borrow() {
            return Err((MailboxError::Closed(address.clone()), job));
        }
        let job = match actors.get(address) {
            Some(entry) => match entry.tx.try_send(job) {
                Ok(()) => return Ok(()),
                Err(mpsc::error::TrySendError::Full(job)) => {
                    return Err((MailboxError::Full(address.clone()), job));
                }
                // The actor's task ended without deregistering (it panicked).
                Err(mpsc::error::TrySendError::Closed(job)) => job,
            },
            None => job,
        };
        let Some(residency) = Residency::try_acquire(
            &self.inner.tasks,
            self.inner.config.max_resident,
            Some(Arc::clone(&self.inner.residency_released)),
        ) else {
            // Slots already on their way back: actors asked to go that are
            // still registered, and actors gone from the registry whose
            // state or resources are still closing. Evict only for demand
            // they do not cover, so waiters woken again and again (after
            // every event on the node) do not each empty another actor.
            let tasks = self.inner.tasks.load(std::sync::atomic::Ordering::SeqCst);
            let evicting = actors
                .values()
                .filter(|e| {
                    e.activity
                        .evicting
                        .load(std::sync::atomic::Ordering::SeqCst)
                })
                .count();
            let returning = tasks.saturating_sub(actors.len()) + evicting;
            let demand = self.inner.waiting.load(std::sync::atomic::Ordering::SeqCst)
                + usize::from(!counted);
            if returning < demand {
                evict_one(&actors);
            }
            return Err((MailboxError::Capacity(address.clone()), job));
        };
        let entry = self.spawn_actor(address.clone(), residency);
        // A fresh queue has room for at least one job.
        let sent = entry
            .tx
            .try_send(job)
            .map_err(|error| (MailboxError::Gone(address.clone()), error.into_inner()));
        actors.insert(address.clone(), entry);
        sent
    }

    fn spawn_actor(&self, address: ActorAddress, residency: Residency) -> Entry<S> {
        let (tx, rx) = mpsc::channel(self.inner.config.queue_capacity.max(1));
        let inner = Arc::clone(&self.inner);
        let entry = Entry {
            tx,
            activity: Arc::new(Activity::default()),
        };
        let own = entry.clone();
        // Reserved before the task runs, so admission and `drained` cannot
        // miss an actor whose factory has not started yet.
        tokio::spawn(run_actor(inner, address, own, rx, residency));
        entry
    }
}

impl<S> Mailboxes<S> {
    /// Asks an actor to hibernate once its accepted events have finished.
    pub fn hibernate(&self, address: &ActorAddress) {
        if let Some(entry) = lock(&self.inner).get(address) {
            entry.activity.evict.notify_one();
        }
    }

    /// Whether this address has a registered mailbox that still accepts events.
    pub fn contains(&self, address: &ActorAddress) -> bool {
        lock(&self.inner)
            .get(address)
            .is_some_and(|entry| !entry.tx.is_closed())
    }

    /// The registered actors' addresses (in no particular order).
    pub fn addresses(&self) -> Vec<ActorAddress> {
        lock(&self.inner).keys().cloned().collect()
    }

    /// Number of registered actors. Retiring actors and detached resources
    /// may still reserve capacity after leaving this registry.
    pub fn resident(&self) -> usize {
        lock(&self.inner).len()
    }

    /// Stops taking events (new ones fail with [`MailboxError::Closed`]),
    /// for good. Each resident actor finishes the events already queued,
    /// then drops its state as hibernation does, without waiting for the
    /// idle timeout.
    pub fn close(&self) {
        let _actors = lock(&self.inner);
        self.inner.closed.send_replace(true);
        self.inner.capacity_changed.notify_waiters();
    }

    /// Whether `close` was called.
    pub fn is_closed(&self) -> bool {
        *self.inner.closed.borrow()
    }

    /// Waits until every actor task, state and retained [`Residency`] has
    /// dropped (after `close`), or `limit` passes. Returns whether all did.
    pub async fn drained(&self, limit: Duration) -> bool {
        let deadline = tokio::time::Instant::now() + limit;
        while self.inner.tasks.load(std::sync::atomic::Ordering::SeqCst) > 0 {
            if tokio::time::Instant::now() >= deadline {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        true
    }
}

type Registry<S> = HashMap<ActorAddress, Entry<S>>;

/// A resident actor's queue, and what eviction needs to know about it.
struct Entry<S> {
    tx: mpsc::Sender<Job<S>>,
    activity: Arc<Activity>,
}

impl<S> Clone for Entry<S> {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
            activity: Arc::clone(&self.activity),
        }
    }
}

/// When an actor last finished an event (`u64::MAX` while one runs), and
/// how to ask it to hibernate.
struct Activity {
    last: std::sync::atomic::AtomicU64,
    evict: tokio::sync::Notify,
    /// Asked to hibernate to make room (it finishes queued events first).
    evicting: std::sync::atomic::AtomicBool,
}

impl Default for Activity {
    fn default() -> Self {
        Self {
            last: std::sync::atomic::AtomicU64::new(clock_ms()),
            evict: tokio::sync::Notify::new(),
            evicting: std::sync::atomic::AtomicBool::new(false),
        }
    }
}

/// One `submit_wait` caller waiting for a slot, for as long as it lives.
struct Waiting<S>(Arc<Inner<S>>);

impl<S> Waiting<S> {
    fn new(inner: Arc<Inner<S>>) -> Self {
        inner
            .waiting
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Self(inner)
    }
}

impl<S> Drop for Waiting<S> {
    fn drop(&mut self) {
        self.0
            .waiting
            .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
    }
}

/// Asks the least recently active idle actor to hibernate (one busy with
/// an event is never chosen; if all are busy, nothing is).
fn evict_one<S>(actors: &Registry<S>) {
    let oldest = actors
        .values()
        .map(|e| {
            (
                e.activity.last.load(std::sync::atomic::Ordering::Relaxed),
                e,
            )
        })
        .filter(|(last, e)| {
            *last != u64::MAX
                && !e
                    .activity
                    .evicting
                    .load(std::sync::atomic::Ordering::SeqCst)
        })
        .min_by_key(|(last, _)| *last);
    if let Some((_, entry)) = oldest {
        entry
            .activity
            .evicting
            .store(true, std::sync::atomic::Ordering::SeqCst);
        entry.activity.evict.notify_one();
    }
}

/// Milliseconds on a monotonic clock.
fn clock_ms() -> u64 {
    static START: std::sync::OnceLock<std::time::Instant> = std::sync::OnceLock::new();
    let start = START.get_or_init(std::time::Instant::now);
    u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX - 1)
}

fn lock<S>(inner: &Inner<S>) -> std::sync::MutexGuard<'_, Registry<S>> {
    inner.actors.lock().unwrap_or_else(|p| p.into_inner())
}

async fn run_actor<S: Send + 'static>(
    inner: Arc<Inner<S>>,
    address: ActorAddress,
    own: Entry<S>,
    mut rx: mpsc::Receiver<Job<S>>,
    residency: Residency,
) {
    // Bound first, so dropped last: after `state`, also on a panic.
    let task_residency = residency;
    let mut state: Option<S> = None;
    let mut closed = inner.closed.subscribe();
    loop {
        let next = tokio::select! {
            // Queued events first: closing finishes them.
            biased;
            job = rx.recv() => Ok(job),
            _ = tokio::time::sleep(inner.config.idle_timeout) => Err(false),
            _ = closed.wait_for(|c| *c) => Err(false),
            // Asked to make room: hibernate as if idle for long enough.
            () = own.activity.evict.notified() => Err(true),
        };
        let job = match next {
            Ok(Some(job)) => {
                // Busy: never chosen for eviction while an event runs.
                own.activity
                    .last
                    .store(u64::MAX, std::sync::atomic::Ordering::Relaxed);
                job
            }
            // Every sender is gone: nothing can reach this actor any more.
            Ok(None) => return,
            Err(evicted) => {
                // Deregister under the lock that `enqueue` sends under, and
                // only if nothing was queued meanwhile; otherwise keep going.
                let mut actors = lock(&inner);
                if rx.is_empty() {
                    // (A queued event keeps it resident; `last` was set to
                    // busy by the eviction and is refreshed below.)
                    if actors
                        .get(&address)
                        .is_some_and(|e| e.tx.same_channel(&own.tx))
                    {
                        actors.remove(&address);
                    }
                    return; // drops `state`: hibernation
                }
                // A job raced the hibernation before we acquired the
                // registry lock. An eviction stays pending while the biased
                // receive branch drains those accepted events; an idle
                // timeout simply starts over.
                if evicted {
                    own.activity.evict.notify_one();
                }
                continue;
            }
        };

        if state.is_none() {
            // A panicking event may have left work holding the old state's
            // resources. Never build a second state under that same slot.
            if !task_residency.is_exclusive() {
                job(Err(MailboxError::Capacity(address.clone()))).await;
                own.activity
                    .last
                    .store(clock_ms(), std::sync::atomic::Ordering::Relaxed);
                inner.capacity_changed.notify_waiters();
                continue;
            }
            match (inner.factory)(address.clone(), task_residency.clone()).await {
                Ok(s) => state = Some(s),
                Err(source) => {
                    // This event fails with the cause; the next one retries.
                    job(Err(MailboxError::Init {
                        address: address.clone(),
                        source,
                    }))
                    .await;
                    // A failed factory is idle too: another name may need
                    // to evict it rather than wait for the idle timeout.
                    own.activity
                        .last
                        .store(clock_ms(), std::sync::atomic::Ordering::Relaxed);
                    inner.capacity_changed.notify_waiters();
                    continue;
                }
            }
        }
        if let Some(s) = state.as_mut() {
            // A panic in one event must not take the queue down with it:
            // events already queued behind it would be lost. Contain it, drop
            // the possibly inconsistent state, and keep serving. The caller
            // of the panicking event sees `Gone` (its reply was dropped).
            if CatchUnwind(job(Ok(s))).await.is_err() {
                state = None;
            }
        }
        own.activity
            .last
            .store(clock_ms(), std::sync::atomic::Ordering::Relaxed);
        inner.capacity_changed.notify_waiters();
    }
}

/// Runs a boxed future, turning a panic during any poll into `Err(())`.
struct CatchUnwind<'a>(BoxFuture<'a, ()>);

impl Future for CatchUnwind<'_> {
    type Output = Result<(), ()>;

    fn poll(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let fut = &mut self.get_mut().0;
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| fut.as_mut().poll(cx))) {
            Ok(poll) => poll.map(Ok),
            Err(_panic) => std::task::Poll::Ready(Err(())),
        }
    }
}
