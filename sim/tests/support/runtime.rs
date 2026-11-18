//! Per-seed teardown for simulations with uniquely named madsim nodes.
//!
//! In madsim 0.2.34 a pending task holds its own waker through TaskInfo.
//! Dropping Runtime alone leaves that cycle (and captured buckets/leases)
//! alive. Kill every node and poll cancellation before dropping the runtime.
//! This happens only after the simulation's observation window has ended.

use std::future::Future;

pub(crate) struct Runtime(madsim::runtime::Runtime);

impl Runtime {
    pub(crate) fn with_seed_and_config(seed: u64, config: madsim::Config) -> Self {
        Self(madsim::runtime::Runtime::with_seed_and_config(seed, config))
    }

    pub(crate) fn block_on<F: Future>(&self, future: F) -> F::Output {
        self.0.block_on(future)
    }
}

impl Drop for Runtime {
    fn drop(&mut self) {
        // Preserve the original diagnostic if a seed panicked inside madsim.
        if std::thread::panicking() {
            return;
        }
        let handle = self.0.handle();
        let metrics = handle.metrics();
        for name in metrics.num_tasks_by_node().keys() {
            handle.kill(name);
        }
        self.0.block_on(async {
            while metrics.num_tasks() != 0 {
                madsim::task::yield_now().await;
            }
        });
    }
}

#[test]
fn dropping_a_seed_releases_pending_and_paused_task_state() {
    use std::sync::Arc;
    use std::time::Duration;

    for seed in 0..2 {
        let state = Arc::new(vec![0u8; 65_536]);
        let weak = Arc::downgrade(&state);
        {
            let runtime = Runtime::with_seed_and_config(seed, Default::default());
            runtime.block_on(async move {
                let handle = madsim::runtime::Handle::current();
                for name in ["pending", "paused"] {
                    let node = handle.create_node().name(name).build();
                    let state = Arc::clone(&state);
                    node.spawn(async move {
                        let _state = state;
                        madsim::time::sleep(Duration::from_secs(3600)).await;
                    });
                }
                madsim::time::sleep(Duration::from_millis(1)).await;
                handle.pause("paused");
            });
            assert!(weak.upgrade().is_some());
        }
        assert!(weak.upgrade().is_none(), "seed {seed} retained task state");
    }
}
