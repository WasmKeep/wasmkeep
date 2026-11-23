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
/// loaded as native code. So, on Unix, an entry is loaded only while the
/// directory and the file belong to this process's user and neither its
/// group nor others can write to them; otherwise the component is compiled
/// again, with a warning (in a directory others can write to, in memory,
/// without the cache).
#[derive(Debug, Clone)]
pub struct ComponentCache {
    runtime: Runtime,
    dir: PathBuf,
}

impl ComponentCache {
    /// Opens a cache directory, creating it if needed (the owner's alone,
    /// mode 0700). A relative `dir` is taken from the current directory
    /// once, here.
    pub fn new(runtime: Runtime, dir: impl Into<PathBuf>) -> Result<Self, Error> {
        let dir = dir.into();
        let dir = std::path::absolute(&dir).map_err(|e| Error::cache_io(&dir, e))?;
        create_private_dir(&dir)?;
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
    /// as an error, and so is one others could have written. In a cache
    /// directory others could write to, `wasm` is compiled without the
    /// cache.
    pub fn get_or_compile(&self, wasm: &[u8]) -> Result<Component, Error> {
        if let Err(why) = private(&self.dir, true) {
            tracing::warn!(
                dir = %self.dir.display(),
                "component cache not used: the directory {why}; compiling without it"
            );
            return self.runtime.compile(wasm);
        }
        let path = self.path_for(wasm);
        if path.exists() {
            match private(&path, false) {
                Ok(()) => {
                    if let Ok(component) = self.load(&path) {
                        return Ok(component);
                    }
                }
                Err(why) => tracing::warn!(
                    path = %path.display(),
                    "cached component not loaded: the file {why}; compiling it again"
                ),
            }
        }
        let artifact = self.runtime.precompile(wasm)?;
        write_atomically(&self.dir, &path, &artifact)?;
        self.load(&path)
    }

    /// Loads a pre-compiled artifact from inside the cache directory, if
    /// the directory and the file are still private.
    fn load(&self, path: &Path) -> Result<Component, Error> {
        let refused = |why: String| {
            Error::cache_io(
                path,
                std::io::Error::new(std::io::ErrorKind::PermissionDenied, why),
            )
        };
        private(&self.dir, true).map_err(|why| refused(format!("the directory {why}")))?;
        private(path, false).map_err(|why| refused(format!("the file {why}")))?;
        // SAFETY: `deserialize_file` trusts the file to be an artifact that
        // Wasmtime produced, since it is mapped and run as native code. Paths
        // reaching here are built by `path_for` inside the cache directory
        // and written only by `get_or_compile` from
        // `Engine::precompile_component`; on Unix the directory and the file
        // were just checked to be this user's and writable by no one else.
        // Wasmtime still checks the header and the engine compatibility, so
        // a stale or truncated file is an error, not undefined behaviour; a
        // file planted by this user, or by root, is outside the threat model
        // (they could replace the binary too).
        unsafe { Component::deserialize_file(self.runtime.engine(), path) }.map_err(Error::compile)
    }
}

/// Creates `dir`, if missing, with mode 0700 on Unix (its parents as usual).
fn create_private_dir(dir: &Path) -> Result<(), Error> {
    if let Some(parent) = dir.parent() {
        std::fs::create_dir_all(parent).map_err(|e| Error::cache_io(parent, e))?;
    }
    let mut builder = std::fs::DirBuilder::new();
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    match builder.create(dir) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists && dir.is_dir() => Ok(()),
        Err(e) => Err(Error::cache_io(dir, e)),
    }
}

/// Whether native code may be loaded from `path` (the cache directory if
/// `dir`, else an entry in it): on Unix, owned by this process's
/// effective user and not writable by its group or others; an entry must
/// also be a regular file, not a symlink. `Err` says why not.
fn private(path: &Path, dir: bool) -> Result<(), String> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        // The directory may be a symlink (a mount, say): its target counts.
        let meta = if dir {
            std::fs::metadata(path)
        } else {
            std::fs::symlink_metadata(path)
        }
        .map_err(|e| format!("cannot be checked ({e})"))?;
        if !dir && !meta.file_type().is_file() {
            return Err("is not a regular file".into());
        }
        let mode = meta.permissions().mode();
        if mode & 0o022 != 0 {
            return Err(format!(
                "can be written by its group or others (mode {:o})",
                mode & 0o777
            ));
        }
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        {
            let me = rustix::process::geteuid().as_raw();
            if meta.uid() != me {
                return Err(format!(
                    "belongs to user {}, not this process's ({me})",
                    meta.uid()
                ));
            }
        }
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        let _ = meta.uid();
    }
    #[cfg(not(unix))]
    let _ = (path, dir);
    Ok(())
}

/// Writes to a temp file in `dir`, then renames over `path`, so concurrent
/// readers never see a partial artifact. The file is the owner's alone
/// (0600 on Unix).
fn write_atomically(dir: &Path, path: &Path, bytes: &[u8]) -> Result<(), Error> {
    let tmp = dir.join(format!(".tmp-{}-{}", std::process::id(), unique()));
    let result = (|| {
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(&tmp)?;
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
