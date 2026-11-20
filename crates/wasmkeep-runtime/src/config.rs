use std::time::Duration;

/// Engine-wide settings, the same for every actor on the node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeConfig {
    /// Component instances the pooling allocator pre-reserves slots for.
    /// This is the hard cap on concurrently live instances per node.
    pub max_instances: u32,
    /// Core instances per component instance, on average. A Rust component
    /// built with the WASI adapter has a handful; pools are sized as
    /// `max_instances * core_instances_per_instance`.
    pub core_instances_per_instance: u32,
    /// Maximum size of one linear memory, in bytes (default: 128 MiB).
    pub max_memory_bytes: usize,
    /// Guest CPU time allowed per store (one event), excluding time spent
    /// in host calls and time the OS runs other threads (default:
    /// 50 ms). A guest that exceeds it traps with
    /// [`CpuBudgetExceeded`](crate::CpuBudgetExceeded). Measured with the
    /// thread CPU clock on Linux and macOS, wall time elsewhere; see
    /// `cpu.rs` for how the wall clock stands in for the thread CPU clock on
    /// short stretches of guest code, and how preemption is kept out of
    /// them. Time a hypervisor steals from the VM may be counted: the thread
    /// CPU clock leaves it out only where the guest kernel accounts steal
    /// apart.
    pub cpu_budget: Duration,
    /// How often the epoch advances; the CPU budget is enforced with this
    /// granularity. Must be non-zero.
    pub epoch_tick: Duration,
    /// Meter Wasmtime fuel (work units, never an execution limit).
    /// Counts depend on the component and Wasmtime version, not CPU speed.
    pub measure_fuel: bool,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            max_instances: 1_000,
            core_instances_per_instance: 8,
            max_memory_bytes: 128 * 1024 * 1024,
            cpu_budget: Duration::from_millis(50),
            epoch_tick: Duration::from_millis(1),
            measure_fuel: true,
        }
    }
}
