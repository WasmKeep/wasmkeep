use std::hash::Hasher;
use std::io::Write;
use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};
use wasmtime::component::Component;

use crate::{Error, Runtime};

/// On-disk cache of pre-compiled components, keyed by content.
///
/// The key is SHA-256 over the engine's compatibility hash (Wasmtime version,
/// target, compiler settings) and the component bytes, so a Wasmtime upgrade
/// or a config change never loads an incompatible artifact; it misses and
/// recompiles. The directory must be private to the node: anything in it is
/// loaded as native code.
#[derive(Debug, Clone)]
pub struct ComponentCache {
    runtime: Runtime,
    dir: PathBuf,
}

impl ComponentCache {
    /// Opens (and creates if needed) a cache directory.
    pub fn new(runtime: Runtime, dir: impl Into<PathBuf>) -> Result<Self, Error> {
        let dir = dir.into();
        std::fs::create_dir_all(&dir).map_err(|e| Error::cache_io(&dir, e))?;
        Ok(Self { runtime, dir })
    }

    /// The cache directory.
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// The hex cache key for `wasm` under this engine.
    pub fn key(&self, wasm: &[u8]) -> String {
        let mut hasher = Sha256Hasher(Sha256::new());
        std::hash::Hash::hash(
            &self.runtime.engine().precompile_compatibility_hash(),
            &mut hasher,
        );
        let mut digest = hasher.0;
        digest.update(wasm);
        let bytes = digest.finalize();
        let mut hex = String::with_capacity(bytes.len() * 2);
        for b in bytes {
            hex.push_str(&format!("{b:02x}"));
        }
        hex
    }

    /// Where the pre-compiled artifact for `wasm` lives.
    pub fn path_for(&self, wasm: &[u8]) -> PathBuf {
        self.dir.join(format!("{}.cwasm", self.key(wasm)))
    }

    /// Loads `wasm` from the cache, pre-compiling and storing it on a miss.
    /// An unreadable or incompatible cache entry is replaced, not returned
    /// as an error.
    pub fn get_or_compile(&self, wasm: &[u8]) -> Result<Component, Error> {
        let path = self.path_for(wasm);
        if path.exists()
            && let Ok(component) = self.load(&path)
        {
            return Ok(component);
        }
        let artifact = self.runtime.precompile(wasm)?;
        write_atomically(&self.dir, &path, &artifact)?;
        self.load(&path)
    }

    /// Loads a pre-compiled artifact from inside the cache directory.
    fn load(&self, path: &Path) -> Result<Component, Error> {
        // SAFETY: `deserialize_file` trusts the file to be an artifact that
        // Wasmtime produced, since it is mapped and run as native code. Paths
        // reaching here are built by `path_for` inside the node-private cache
        // directory and written only by `get_or_compile` from
        // `Engine::precompile_component`. Wasmtime still checks the header and
        // the engine compatibility, so a stale or truncated file is an error,
        // not undefined behaviour; a file planted by someone with write access
        // to the cache directory is outside the threat model (they could
        // replace the binary too).
        unsafe { Component::deserialize_file(self.runtime.engine(), path) }.map_err(Error::compile)
    }
}

/// Writes to a temp file in `dir`, then renames over `path`, so concurrent
/// readers never see a partial artifact.
fn write_atomically(dir: &Path, path: &Path, bytes: &[u8]) -> Result<(), Error> {
    let tmp = dir.join(format!(".tmp-{}-{}", std::process::id(), unique()));
    let result = (|| {
        let mut file = std::fs::File::create(&tmp)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        std::fs::rename(&tmp, path)
    })();
    if let Err(e) = result {
        let _ = std::fs::remove_file(&tmp);
        return Err(Error::cache_io(path, e));
    }
    Ok(())
}

fn unique() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(0);
    NEXT.fetch_add(1, Ordering::Relaxed)
}

/// Feeds `Hash` output into SHA-256 so the compatibility hash is stable
/// across processes (unlike `DefaultHasher`, which is not guaranteed stable).
struct Sha256Hasher(Sha256);

impl Hasher for Sha256Hasher {
    fn write(&mut self, bytes: &[u8]) {
        self.0.update(bytes);
    }

    fn finish(&self) -> u64 {
        0
    }
}
