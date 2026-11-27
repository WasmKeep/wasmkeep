//! The removal of released node leases by the live nodes ([ADR 0020],
//! amendment of 2026-11-27): every round, while this node is its app's
//! remover ([`Cluster::removes_released_leases`]), one
//! [`Remover::round`](wasmkeep_cluster::lease_removal::Remover::round).
//!
//! [ADR 0020]: ../../../../docs/adr/0020-one-lease-per-node.md

use wasmkeep_cluster::lease_removal::{Remover, Settings};

use super::*;

/// Runs a round every [`ClusterConfig::lease_removal_every`] while the
/// removal is on, until the cluster is dropped or its node lease task
/// ends.
pub(super) async fn lease_removal_loop(cluster: std::sync::Weak<Cluster>) {
    let Some(c) = cluster.upgrade() else {
        return;
    };
    let Some(after) = c.lease_removal else {
        return;
    };
    let every = c.lease_removal_every;
    let settings = Settings {
        after_ms: u64::try_from(after.as_millis()).unwrap_or(u64::MAX),
        horizon_ms: c.heartbeat_horizon_ms,
        config: c.lease_config,
    };
    let bound = Duration::from_millis(wasmkeep_cluster::node_lease::forget_delete_bound_ms(
        &c.lease_config,
    ));
    let mut done = c.lease_done.subscribe();
    drop(c);
    let mut remover = Remover::new();
    loop {
        tokio::select! {
            () = tokio::time::sleep(every) => {}
            _ = done.wait_for(|d| *d) => return,
        }
        let Some(c) = cluster.upgrade() else {
            return;
        };
        if c.identity_taken() || !c.removes_released_leases() {
            continue;
        }
        let round = remover
            .round(&c.control_keys, now_ms(), &c.serves, &settings, || {
                tokio::time::sleep(bound)
            })
            .await;
        match round {
            Err(e) => {
                tracing::warn!(error = ?e, "removing released node leases: listing nodes/ failed");
            }
            Ok(round) => {
                for node in &round.removed {
                    tracing::info!(%node, "removed a released node lease older than the removal period");
                }
                for node in &round.ended {
                    tracing::info!(%node, "deleted the mark of a removal that did not finish");
                }
                for node in &round.unfinished {
                    tracing::warn!(%node, "removing a released node lease: the delete of its mark got no answer; a later round ends it");
                }
                if let Some(e) = &round.failed {
                    tracing::warn!(error = %e, "removing released node leases: the round stopped");
                }
                crate::metrics::lease_removal(&round);
            }
        }
    }
}
