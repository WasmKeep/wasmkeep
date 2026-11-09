use std::path::PathBuf;

/// Boxed source error, used to carry [`wasmtime::Error`], which does not
/// implement [`std::error::Error`] itself.
type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// Errors returned by the runtime.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// A `RuntimeConfig` value is out of range.
    #[error("invalid runtime configuration: {0}")]
    InvalidConfig(&'static str),

    /// The engine configuration was rejected by Wasmtime, or the epoch
    /// thread could not start.
    #[error("invalid engine configuration")]
    EngineConfig(#[source] BoxError),

    /// The bytes are not a valid component, or compilation failed.
    #[error("failed to compile component")]
    Compile(#[source] BoxError),

    /// Instantiation failed (missing imports, pool exhausted, start trap).
    #[error("failed to instantiate component")]
    Instantiate(#[source] BoxError),

    /// Reading or writing the component cache failed.
    #[error("component cache I/O error at {path}")]
    CacheIo {
        /// The file or directory involved.
        path: PathBuf,
        /// The underlying I/O error.
        #[source]
        source: std::io::Error,
    },
}

impl Error {
    pub(crate) fn engine_config(e: wasmtime::Error) -> Self {
        Self::EngineConfig(e.into_boxed_dyn_error())
    }

    pub(crate) fn compile(e: wasmtime::Error) -> Self {
        Self::Compile(e.into_boxed_dyn_error())
    }

    pub(crate) fn instantiate(e: wasmtime::Error) -> Self {
        Self::Instantiate(e.into_boxed_dyn_error())
    }

    pub(crate) fn cache_io(path: impl Into<PathBuf>, source: std::io::Error) -> Self {
        Self::CacheIo {
            path: path.into(),
            source,
        }
    }
}
