//! Drives `wasmkeep_cluster::lease` like a node would, and reports what the
//! node believes in *global* (real) time so beliefs of nodes with different
//! clocks can be compared.

use wasmkeep_cluster::lease::{Bucket, Clock, Grant, LeaseError, Leases};

/// What a node believed: "I own the actor from `from_ms` to `until_ms`",
/// both in global time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Claim {
    /// The node.
    pub node: u32,
    /// Its process (a restart, or a twin started with the same node id, is
    /// another); 0 where a simulation does not tell them apart.
    pub instance: u64,
    /// Lease generation it believes it holds.
    pub generation: u64,
    /// When it learned it owned the actor (global time).
    pub from_ms: u64,
    /// When it stops acting as owner (global time).
    pub until_ms: u64,
}

/// Simulation services a node needs besides its lease.
#[allow(async_fn_in_trait)]
pub trait Env {
    /// Global (real) time in ms, for reporting only; the protocol uses the
    /// node's own, possibly skewed, clock.
    fn global_ms(&self) -> u64;
    /// Sleeps for `ms` of simulated time.
    async fn sleep_ms(&self, ms: u64);
}

/// A node's clock: global time shifted by a fixed skew.
#[derive(Debug, Clone, Copy)]
pub struct SkewedClock<G> {
    /// Source of global time.
    pub global: G,
    /// Offset of this node's clock from global time, in ms.
    pub skew_ms: i64,
}

impl<G: Fn() -> u64> Clock for SkewedClock<G> {
    fn now_ms(&self) -> u64 {
        // Simulations start far enough from zero that skew never underflows.
        self.global().saturating_add_signed(self.skew_ms)
    }
}

impl<G: Fn() -> u64> SkewedClock<G> {
    fn global(&self) -> u64 {
        (self.global)()
    }
}

/// What a node holding the lease does on a tick.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Decision {
    /// Keep the lease: renew it.
    Keep,
    /// Give it up (an actor hibernating, a node shutting down): stop acting
    /// as owner, then release.
    Release,
    /// Like `Release`, with a renewal still in flight: production aborts the
    /// renewer task when it releases, but a request may already be on its
    /// way to the bucket.
    ReleaseWhileRenewing,
    /// Stop acting as owner and go away without releasing, as a crashed
    /// node does: the control for what releasing buys.
    Abandon,
}

/// How long a node that released a lease stays away before competing again.
pub const RELEASE_AWAY_MS: u64 = 3_000;

/// What the simulation decides for a node and learns from it.
pub trait Observer {
    /// What the node does on a tick while it holds a valid grant.
    fn decide(&mut self) -> Decision;
    /// The node believes it owns the actor (see [`Claim`]).
    fn claim(&mut self, claim: Claim);
    /// The node stopped acting as owner at global time `at_ms`, before
    /// releasing: its earlier claims end there.
    fn stop(&mut self, node: u32, at_ms: u64);
}

/// Runs a node forever: acquire the lease when possible, renew it every
/// `tick_ms`, stop acting as owner when the grant runs out or is lost, and
/// release it when the observer decides so.
pub async fn run_node<B, G, E>(
    node: u32,
    key: &str,
    leases: &Leases<B, SkewedClock<G>>,
    env: &E,
    tick_ms: u64,
    observer: &mut impl Observer,
) where
    B: Bucket,
    G: Fn() -> u64,
    E: Env,
{
    let skew = leases.clock().skew_ms;
    let mut grant: Option<Grant> = None;
    loop {
        let now = leases.clock().now_ms();
        grant = match grant.take() {
            Some(g) if !g.is_valid(now) => None,
            Some(g) => match observer.decide() {
                Decision::Keep => match leases.renew(&g).await {
                    Ok(g) => Some(g),
                    Err(LeaseError::Lost) => None,
                    // No answer: keep acting until the grant runs out.
                    Err(_) => Some(g),
                },
                decision => {
                    observer.stop(node, env.global_ms());
                    match decision {
                        // The renewal's outcome is ignored, as in production.
                        Decision::ReleaseWhileRenewing => {
                            let _ = join(leases.renew(&g), leases.release(&g)).await;
                        }
                        Decision::Release => {
                            let _ = leases.release(&g).await;
                        }
                        Decision::Keep | Decision::Abandon => {}
                    }
                    // Step away, as a node shutting down would: the next
                    // owner is another node.
                    env.sleep_ms(RELEASE_AWAY_MS).await;
                    None
                }
            },
            None => leases.acquire(key).await.ok(),
        };
        if let Some(g) = &grant {
            observer.claim(Claim {
                node,
                instance: 0,
                generation: g.generation,
                from_ms: env.global_ms(),
                until_ms: g.valid_until_ms.saturating_add_signed(-skew),
            });
        }
        env.sleep_ms(tick_ms).await;
    }
}

/// Polls two futures concurrently until both finish.
async fn join<A: std::future::Future, B: std::future::Future>(
    a: A,
    b: B,
) -> (A::Output, B::Output) {
    let mut a = std::pin::pin!(a);
    let mut b = std::pin::pin!(b);
    let (mut ra, mut rb) = (None, None);
    std::future::poll_fn(|cx| {
        if ra.is_none()
            && let std::task::Poll::Ready(v) = a.as_mut().poll(cx)
        {
            ra = Some(v);
        }
        if rb.is_none()
            && let std::task::Poll::Ready(v) = b.as_mut().poll(cx)
        {
            rb = Some(v);
        }
        if ra.is_some() && rb.is_some() {
            std::task::Poll::Ready(())
        } else {
            std::task::Poll::Pending
        }
    })
    .await;
    match (ra, rb) {
        (Some(a), Some(b)) => (a, b),
        _ => unreachable!("both finished"),
    }
}
