//! System allocator policy and reclamation after activity drops.

use std::time::Duration;

/// Result of an actual allocator call. Skipped checks return `None` instead.
#[derive(Debug, Clone, Copy)]
pub struct AllocatorTrim {
    /// Whether glibc reported releasing pages (not a byte count).
    pub released: bool,
    /// Time spent inside malloc_trim, excluding lock acquisition.
    pub duration: Duration,
}

/// Samples process-wide resident actors plus sockets in use. Trims at zero
/// or at most half the sampled peak since the last trim. Calls are serialized
/// with a cooldown of at least 60 seconds from completion, across all apps.
/// Call on a blocking worker: glibc can acquire arena locks. Unsupported
/// platforms return `None`. Live allocations are never freed.
pub fn trim_system_allocator(activity: usize, cooldown: Duration) -> Option<AllocatorTrim> {
    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    {
        use std::sync::Mutex;
        use std::time::Instant;

        static POLICY: Mutex<TrimPolicy> = Mutex::new(TrimPolicy {
            peak: 0,
            last: None,
        });
        // Do not queue scans behind an attempt already running.
        let Ok(mut policy) = POLICY.try_lock() else {
            return None;
        };
        if !policy.observe(
            activity,
            Instant::now(),
            cooldown.max(Duration::from_secs(60)),
        ) {
            return None;
        }
        // SAFETY: Linux GNU targets provide this glibc ABI (size_t -> int).
        unsafe extern "C" {
            fn malloc_trim(pad: usize) -> std::ffi::c_int;
        }
        let started = Instant::now();
        // SAFETY: malloc_trim is thread-safe, accepts no Rust pointers and
        // only releases unused glibc chunks, never live allocations or
        // Wasmtime's separately mapped linear memories.
        let released = unsafe { malloc_trim(0) != 0 };
        let completed = Instant::now();
        policy.completed(completed);
        Some(AllocatorTrim {
            released,
            duration: completed.duration_since(started),
        })
    }
    #[cfg(not(all(target_os = "linux", target_env = "gnu")))]
    {
        let _ = (activity, cooldown);
        None
    }
}

/// Default arena cap: available parallelism clamped to 2..8, fallback 2.
pub fn default_malloc_arena_max() -> i32 {
    arena_cap(std::thread::available_parallelism().map_or(2, usize::from))
}

fn arena_cap(cores: usize) -> i32 {
    cores.clamp(2, 8) as i32
}

/// Set glibc's arena cap before starting any threads. Zero leaves glibc's
/// default/environment policy untouched. Returns false on rejection or on
/// unsupported platforms; zero is a successful no-op.
pub fn configure_malloc_arena_max(arenas: i32) -> bool {
    if arenas == 0 {
        return true;
    }
    if arenas < 0 {
        return false;
    }
    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    {
        const M_ARENA_MAX: std::ffi::c_int = -8;
        // SAFETY: Linux GNU targets provide glibc's mallopt(int, int) ABI.
        unsafe extern "C" {
            fn mallopt(param: std::ffi::c_int, value: std::ffi::c_int) -> std::ffi::c_int;
        }
        // SAFETY: The parameter is glibc's M_ARENA_MAX constant and the
        // value is positive. No pointers are passed. CLI calls before threads.
        unsafe { mallopt(M_ARENA_MAX, arenas) != 0 }
    }
    #[cfg(not(all(target_os = "linux", target_env = "gnu")))]
    false
}

#[cfg(any(test, all(target_os = "linux", target_env = "gnu")))]
#[derive(Default)]
struct TrimPolicy {
    peak: usize,
    last: Option<std::time::Instant>,
}

#[cfg(any(test, all(target_os = "linux", target_env = "gnu")))]
impl TrimPolicy {
    fn observe(&mut self, activity: usize, now: std::time::Instant, cooldown: Duration) -> bool {
        self.peak = self.peak.max(activity);
        (activity == 0 || activity <= self.peak / 2)
            && self
                .last
                .is_none_or(|last| now.duration_since(last) >= cooldown)
    }

    fn completed(&mut self, now: std::time::Instant) {
        self.peak = 0;
        self.last = Some(now);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;
    const COOLDOWN: Duration = Duration::from_secs(60);

    #[test]
    fn arena_default_is_bounded_and_zero_skips_configuration() {
        for (cores, cap) in [
            (0, 2),
            (1, 2),
            (2, 2),
            (4, 4),
            (8, 8),
            (32, 8),
            (usize::MAX, 8),
        ] {
            assert_eq!(arena_cap(cores), cap);
        }
        assert!(configure_malloc_arena_max(0));
        assert!(!configure_malloc_arena_max(-1));
    }

    #[test]
    fn half_peak_trims_and_resets_peak() {
        let mut policy = TrimPolicy::default();
        let now = Instant::now();
        assert!(!policy.observe(100, now, COOLDOWN));
        assert!(!policy.observe(51, now, COOLDOWN));
        assert!(policy.observe(50, now, COOLDOWN));
        policy.completed(now);
        assert!(!policy.observe(50, now + COOLDOWN, COOLDOWN));
        assert!(policy.observe(25, now + COOLDOWN, COOLDOWN));
    }

    #[test]
    fn steady_busy_node_never_trims_on_timer() {
        let mut policy = TrimPolicy::default();
        let now = Instant::now();
        for tick in 0..10 {
            assert!(!policy.observe(100, now + COOLDOWN * tick, COOLDOWN));
        }
    }

    #[test]
    fn zero_trims_but_respects_completion_cooldown() {
        let mut policy = TrimPolicy::default();
        let now = Instant::now();
        assert!(policy.observe(0, now, COOLDOWN));
        policy.completed(now);
        assert!(!policy.observe(0, now + Duration::from_secs(59), COOLDOWN));
        assert!(policy.observe(0, now + COOLDOWN, COOLDOWN));
    }

    #[test]
    fn peak_is_observed_during_cooldown() {
        let mut policy = TrimPolicy::default();
        let now = Instant::now();
        policy.completed(now);
        assert!(!policy.observe(101, now + Duration::from_secs(1), COOLDOWN));
        assert!(!policy.observe(50, now + Duration::from_secs(59), COOLDOWN));
        assert!(policy.observe(50, now + COOLDOWN, COOLDOWN));
    }
}
