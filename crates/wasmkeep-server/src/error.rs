/// Boxed source error, used to carry [`wasmtime::Error`], which does not
/// implement [`std::error::Error`] itself.
type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// Errors returned by the server.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// The component does not fit the `wasi:http/proxy` world (wrong
    /// exports, or imports this host does not provide).
    #[error("component is not a valid wasi:http proxy component")]
    NotAnHttpComponent(#[source] BoxError),

    /// A component given as a front is an actor (it exports one of the
    /// actor world's `on-*` functions), or does not fit the front world
    /// (ADR 0023).
    #[error("component is not a valid front: {0}")]
    NotAFront(String),

    /// An actor class name is not a valid identifier.
    #[error("invalid actor class {0:?}: {1}")]
    InvalidClass(String, String),

    /// A component registered as an actor class does not implement the
    /// `wasmkeep:actor/actor` world.
    #[error("component for actor class {0:?} is not a wasmkeep actor")]
    NotAnActor(String, #[source] BoxError),

    /// Setting up the host linker failed.
    #[error("failed to set up host interfaces")]
    Linker(#[source] BoxError),

    /// A runtime operation failed.
    #[error(transparent)]
    Runtime(#[from] wasmkeep_runtime::Error),

    /// The cluster node id is not `[a-z0-9-]{1,64}`.
    #[error("invalid node id {0:?}: {1}")]
    InvalidNodeId(String, String),

    /// The cluster listener (mutual TLS between nodes) is misconfigured,
    /// or its identity could not be made.
    #[error("cluster TLS: {0}")]
    ClusterTls(String),

    /// The heartbeat period and the lease timing do not fit together.
    #[error("lease timing: {0}")]
    LeaseTiming(String),

    /// The node's configuration does not hold together.
    #[error("configuration: {0}")]
    Config(String),

    /// The data directory holds databases of the layout before 0.4.0 that
    /// cannot be moved into this app's root (ADR 0022), or the move failed.
    #[error("data directory: {0}")]
    LocalLayout(String),

    /// Accepting connections failed.
    #[error("listener error")]
    Io(#[from] std::io::Error),
}

impl Error {
    pub(crate) fn linker(e: wasmtime::Error) -> Self {
        Self::Linker(e.into_boxed_dyn_error())
    }

    pub(crate) fn not_http(e: impl Into<BoxError>) -> Self {
        Self::NotAnHttpComponent(e.into())
    }
}
