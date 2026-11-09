use std::sync::Arc;
use std::time::Duration;

use wasmtime::component::{Component, InstancePre, Linker};
use wasmtime::{Config, Engine, InstanceAllocationStrategy, PoolingAllocationConfig, Store};

use crate::cpu::{self, CpuMeter, Execution, Ticker};
use crate::{Error, RuntimeConfig};

/// Linear memories one component may have (its memory budget is this times
/// `max_memory_bytes`).
pub(crate) const MAX_MEMORIES_PER_COMPONENT: u32 = 2;
/// Core instances (and tables) one component may have.
pub(crate) const MAX_CORE_INSTANCES_PER_COMPONENT: u32 = 32;

/// A configured Wasmtime engine: component model, pooling allocator and epoch
/// interruption, plus the thread that advances the epoch. Cheap to clone;
/// clones share the engine, and the ticker stops when the last clone drops.
#[derive(Debug, Clone)]
pub struct Runtime {
    engine: Engine,
    max_instances: u32,
    cpu_budget: Duration,
    epoch_tick: Duration,
    _ticker: Arc<Ticker>,
}

impl Runtime {
    /// Builds the engine.
    pub fn new(config: &RuntimeConfig) -> Result<Self, Error> {
        if config.epoch_tick.is_zero() {
            return Err(Error::InvalidConfig("epoch_tick must be non-zero"));
        }
        let instances = config.max_instances;
        let core_instances = instances.saturating_mul(config.core_instances_per_instance);

        let mut pool = PoolingAllocationConfig::default();
        // Every resident actor's store keeps an async stack for reuse, so
        // the stack pool is as large as the instance pool (Wasmtime's
        // default of 1000 capped residents at 1000 whatever max_instances).
        pool.total_component_instances(instances)
            .total_stacks(instances)
            .total_core_instances(core_instances)
            .total_memories(core_instances)
            .total_tables(core_instances)
            .max_memory_size(config.max_memory_bytes)
            // Per component: one component nesting many modules could
            // otherwise take many 128 MiB memories, and many pool slots.
            .max_memories_per_component(MAX_MEMORIES_PER_COMPONENT)
            .max_core_instances_per_component(MAX_CORE_INSTANCES_PER_COMPONENT)
            .max_tables_per_component(MAX_CORE_INSTANCES_PER_COMPONENT);

        let mut cfg = Config::new();
        cfg.wasm_component_model(true)
            .epoch_interruption(true)
            .allocation_strategy(InstanceAllocationStrategy::Pooling(pool));

        let engine = Engine::new(&cfg).map_err(Error::engine_config)?;
        let ticker = Ticker::start(engine.clone(), config.epoch_tick)
            .map_err(|e| Error::EngineConfig(Box::new(e)))?;
        Ok(Self {
            engine,
            max_instances: config.max_instances,
            cpu_budget: config.cpu_budget,
            epoch_tick: config.epoch_tick,
            _ticker: Arc::new(ticker),
        })
    }

    /// The hard cap on concurrently live instances (the pool's size).
    pub fn max_instances(&self) -> u32 {
        self.max_instances
    }

    /// The underlying engine, for building linkers and stores.
    pub fn engine(&self) -> &Engine {
        &self.engine
    }

    /// Compiles a component in memory, without the cache.
    pub fn compile(&self, wasm: &[u8]) -> Result<Component, Error> {
        Component::new(&self.engine, wasm).map_err(Error::compile)
    }

    /// Ahead-of-time compiles a component to bytes loadable by this engine.
    pub fn precompile(&self, wasm: &[u8]) -> Result<Vec<u8>, Error> {
        self.engine
            .precompile_component(wasm)
            .map_err(Error::compile)
    }

    /// Resolves a component's imports against `linker` once, so that each
    /// later instantiation only allocates and initialises.
    pub fn instantiate_pre<T: 'static>(
        &self,
        linker: &Linker<T>,
        component: &Component,
    ) -> Result<InstancePre<T>, Error> {
        linker
            .instantiate_pre(component)
            .map_err(Error::instantiate)
    }

    /// Creates a store for one event, driven with sync calls. Guest code
    /// running longer than the CPU budget traps with
    /// [`CpuBudgetExceeded`](crate::CpuBudgetExceeded).
    pub fn new_store<T: 'static>(&self, data: T) -> Store<T> {
        self.store(data, Execution::Sync)
    }

    /// Like [`new_store`](Self::new_store), for stores driven with
    /// `*_async` calls: a running guest also yields to the executor on every
    /// epoch tick.
    pub fn new_async_store<T: 'static>(&self, data: T) -> Store<T> {
        self.store(data, Execution::Async)
    }

    /// Like [`new_async_store`](Self::new_async_store), for a store reused
    /// across events (an actor's): call [`CpuMeter::reset`] at the start of
    /// each event so every event gets the full budget.
    pub fn new_metered_async_store<T: 'static>(&self, data: T) -> (Store<T>, CpuMeter) {
        let mut store = Store::new(&self.engine, data);
        let meter = cpu::install(
            &mut store,
            self.cpu_budget,
            self.epoch_tick,
            Execution::Async,
        );
        (store, meter)
    }

    fn store<T: 'static>(&self, data: T, execution: Execution) -> Store<T> {
        let mut store = Store::new(&self.engine, data);
        let _meter = cpu::install(&mut store, self.cpu_budget, self.epoch_tick, execution);
        store
    }
}
