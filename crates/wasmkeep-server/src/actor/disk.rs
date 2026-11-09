//! Sampling free disk space for readiness and metrics.

use super::*;

/// Free space below which a node reports not ready: 1 GiB, or 5% of a
/// smaller disk.
pub(super) const DISK_LOW_WATER: u64 = 1 << 30;

/// How often the disk is sampled, and how old a sample may be.
pub(super) const DISK_SAMPLE_EVERY: Duration = Duration::from_secs(5);

pub(super) const DISK_SAMPLE_STALE: Duration = Duration::from_secs(30);

/// Samples the data directory's free space every few seconds on a blocking
/// thread (one at a time, so a hung file system holds at most one), until
/// the service is gone.
pub(super) async fn sample_disk(service: std::sync::Weak<ServiceInner>, dir: std::path::PathBuf) {
    use std::sync::atomic::Ordering;
    loop {
        let probe = dir.clone();
        let space = tokio::task::spawn_blocking(move || disk_space(&probe))
            .await
            .ok()
            .flatten();
        let Some(inner) = service.upgrade() else {
            return;
        };
        // Unreadable (no such directory yet, say): not counted as full.
        let low = space.is_some_and(|(free, total)| free < DISK_LOW_WATER.min(total / 20));
        inner.disk_low.store(low, Ordering::Release);
        inner
            .disk_sampled_ms
            .store(now_ms().max(1), Ordering::Release);
        drop(inner);
        tokio::time::sleep(DISK_SAMPLE_EVERY).await;
    }
}

/// Free and total bytes of the file system holding `dir` (or the nearest
/// existing parent); `None` if it cannot be read.
pub(super) fn disk_space(dir: &std::path::Path) -> Option<(u64, u64)> {
    let existing = dir.ancestors().find(|d| d.exists())?;
    let st = rustix::fs::statvfs(existing).ok()?;
    let block = st.f_frsize.max(1);
    Some((
        st.f_bavail.saturating_mul(block),
        st.f_blocks.saturating_mul(block),
    ))
}
