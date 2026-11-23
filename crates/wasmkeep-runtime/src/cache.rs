use std::hash::Hasher;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

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
/// group nor others can write to them, checked on the handles it is read
/// through (the entry opened without following a symlink); otherwise the
/// component is compiled again, with a warning (in a directory others can
/// write to, in memory, without the cache).
#[derive(Debug, Clone)]
pub struct ComponentCache {
    runtime: Runtime,
    dir: PathBuf,
    /// Whether the directory's refusal has been logged.
    warned: Arc<AtomicBool>,
}

impl ComponentCache {
    /// Opens a cache directory, creating it if needed (the owner's alone,
    /// mode 0700). The path is resolved once, here (a relative one from the
    /// current directory, symlinks to the real directory). A directory the
    /// cache cannot use, or one inside a directory others can write to, is
    /// logged here, once.
    pub fn new(runtime: Runtime, dir: impl Into<PathBuf>) -> Result<Self, Error> {
        let dir = dir.into();
        let dir = std::path::absolute(&dir).map_err(|e| Error::cache_io(&dir, e))?;
        create_private_dir(&dir)?;
        let dir = std::fs::canonicalize(&dir).map_err(|e| Error::cache_io(&dir, e))?;
        let cache = Self {
            runtime,
            dir,
            warned: Arc::default(),
        };
        if let Err(why) = checked::open_dir(&cache.dir) {
            cache.warn_dir(&why);
        }
        checked::warn_ancestors(&cache.dir);
        Ok(cache)
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
        self.dir.join(self.name_for(wasm))
    }

    fn name_for(&self, wasm: &[u8]) -> String {
        format!("{}.cwasm", self.key(wasm))
    }

    /// Loads `wasm` from the cache, pre-compiling and storing it on a miss.
    /// An unreadable or incompatible cache entry is replaced, not returned
    /// as an error, and so is one others could have written or a symlink.
    /// In a cache directory others could write to, `wasm` is compiled
    /// without the cache.
    pub fn get_or_compile(&self, wasm: &[u8]) -> Result<Component, Error> {
        let dir = match checked::open_dir(&self.dir) {
            Ok(dir) => dir,
            Err(why) => {
                self.warn_dir(&why);
                return self.runtime.compile(wasm);
            }
        };
        let name = self.name_for(wasm);
        let path = self.dir.join(&name);
        match checked::read_entry(&dir, &path, &name) {
            checked::Entry::Bytes(bytes) => {
                if let Ok(component) = self.deserialize(&bytes) {
                    return Ok(component);
                }
            }
            checked::Entry::Missing => {}
            checked::Entry::Refused(why) => tracing::warn!(
                path = %path.display(),
                "cached component not loaded: the file {why}; compiling it again"
            ),
        }
        let artifact = self.runtime.precompile(wasm)?;
        write_atomically(&self.dir, &path, &artifact, &mut temp_names())?;
        // What was just compiled, not the file: nothing to check again.
        self.deserialize(&artifact)
    }

    /// A component from an artifact's bytes.
    fn deserialize(&self, bytes: &[u8]) -> Result<Component, Error> {
        // SAFETY: `deserialize` trusts the bytes to be an artifact that
        // Wasmtime produced, since they are run as native code. They are
        // either what `Engine::precompile_component` just returned, or read
        // by `checked::read_entry` through the very handle checked to be a
        // regular file of this user's, writable by no one else, inside a
        // directory checked the same way (on Unix). Wasmtime still checks
        // the header and the engine compatibility, so a stale or truncated
        // file is an error, not undefined behaviour; a file planted by this
        // user, or by root, is outside the threat model (they could replace
        // the binary too).
        unsafe { Component::deserialize(self.runtime.engine(), bytes) }.map_err(Error::compile)
    }

    /// Logs, once, why the directory is not used.
    fn warn_dir(&self, why: &str) {
        if !self.warned.swap(true, Ordering::Relaxed) {
            tracing::warn!(
                dir = %self.dir.display(),
                fix = %checked::fix(&self.dir),
                "component cache not used: the directory {why}; components are compiled \
                 at each start until it is fixed"
            );
        }
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

/// Opening the cache directory and its entries, checked on the handles
/// they are read through: on Linux and macOS, with `openat`.
#[cfg(any(target_os = "linux", target_os = "macos"))]
mod checked {
    use std::io::Read as _;
    use std::os::fd::OwnedFd;
    use std::path::Path;

    use rustix::fs::{FileType, Mode, OFlags, Stat};

    /// The open cache directory.
    pub(super) struct Dir(OwnedFd);

    /// What an entry holds.
    pub(super) enum Entry {
        Missing,
        Refused(String),
        Bytes(Vec<u8>),
    }

    fn euid() -> u32 {
        rustix::process::geteuid().as_raw()
    }

    #[allow(clippy::unnecessary_cast)] // u16 on macOS
    fn mode(stat: &Stat) -> u32 {
        stat.st_mode as u32
    }

    /// This user's, and writable by no one else.
    fn private(stat: &Stat) -> Result<(), String> {
        let mode = mode(stat);
        if mode & 0o022 != 0 {
            return Err(format!(
                "can be written by its group or others (mode {:o})",
                mode & 0o777
            ));
        }
        let me = euid();
        if stat.st_uid != me {
            return Err(format!(
                "belongs to user {}, not this process's ({me})",
                stat.st_uid
            ));
        }
        Ok(())
    }

    pub(super) fn open_dir(path: &Path) -> Result<Dir, String> {
        let flags = OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC;
        let fd = rustix::fs::open(path, flags, Mode::empty())
            .map_err(|e| format!("cannot be opened ({e})"))?;
        let stat = rustix::fs::fstat(&fd).map_err(|e| format!("cannot be checked ({e})"))?;
        private(&stat)?;
        Ok(Dir(fd))
    }

    pub(super) fn read_entry(dir: &Dir, _path: &Path, name: &str) -> Entry {
        // Never through a symlink; never blocking on a fifo.
        let flags = OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC | OFlags::NONBLOCK;
        let fd = match rustix::fs::openat(&dir.0, name, flags, Mode::empty()) {
            Ok(fd) => fd,
            Err(rustix::io::Errno::NOENT) => return Entry::Missing,
            Err(rustix::io::Errno::LOOP) => return Entry::Refused("is a symlink".into()),
            Err(e) => return Entry::Refused(format!("cannot be opened ({e})")),
        };
        let stat = match rustix::fs::fstat(&fd) {
            Ok(stat) => stat,
            Err(e) => return Entry::Refused(format!("cannot be checked ({e})")),
        };
        if FileType::from_raw_mode(stat.st_mode) != FileType::RegularFile {
            return Entry::Refused("is not a regular file".into());
        }
        if let Err(why) = private(&stat) {
            return Entry::Refused(why);
        }
        if mode(&stat) & 0o077 != 0 {
            // Readable by others, as older versions wrote it: the owner's
            // alone from now on. Loading does not depend on it.
            let _ = rustix::fs::fchmod(&fd, Mode::from_raw_mode(0o600));
        }
        let mut bytes = Vec::new();
        match std::fs::File::from(fd).read_to_end(&mut bytes) {
            Ok(_) => Entry::Bytes(bytes),
            Err(e) => Entry::Refused(format!("cannot be read ({e})")),
        }
    }

    /// The commands that make `dir` usable.
    pub(super) fn fix(dir: &Path) -> String {
        let dir = dir.display();
        format!("chown {} {dir} && chmod 700 {dir}", euid())
    }

    /// Logs the first directory above `dir` that someone other than root
    /// or this user could write to without the sticky bit: they could
    /// replace the cache directory.
    pub(super) fn warn_ancestors(dir: &Path) {
        use std::os::unix::fs::MetadataExt;
        let me = euid();
        for ancestor in dir.ancestors().skip(1) {
            let Ok(meta) = std::fs::metadata(ancestor) else {
                continue;
            };
            let (mode, owner) = (meta.mode(), meta.uid());
            let sticky = mode & 0o1000 != 0;
            let others_write = mode & 0o022 != 0 && !sticky;
            let owner_other = owner != 0 && owner != me && mode & 0o200 != 0;
            if others_write || owner_other {
                tracing::warn!(
                    dir = %dir.display(),
                    ancestor = %ancestor.display(),
                    mode = format!("{:o}", mode & 0o7777),
                    owner,
                    "the component cache is inside a directory others can write to: they \
                     could replace the cache directory"
                );
                return;
            }
        }
    }
}

/// Elsewhere: checked by path (on other Unix systems, the modes only).
#[cfg(not(any(target_os = "linux", target_os = "macos")))]
mod checked {
    use std::path::{Path, PathBuf};

    pub(super) struct Dir(#[allow(dead_code)] PathBuf);

    pub(super) enum Entry {
        Missing,
        Refused(String),
        Bytes(Vec<u8>),
    }

    #[cfg(unix)]
    fn writable_by_others(meta: &std::fs::Metadata) -> Result<(), String> {
        use std::os::unix::fs::PermissionsExt;
        let mode = meta.permissions().mode();
        if mode & 0o022 != 0 {
            return Err(format!(
                "can be written by its group or others (mode {:o})",
                mode & 0o777
            ));
        }
        Ok(())
    }

    #[cfg(not(unix))]
    fn writable_by_others(_: &std::fs::Metadata) -> Result<(), String> {
        Ok(())
    }

    pub(super) fn open_dir(path: &Path) -> Result<Dir, String> {
        let meta = std::fs::metadata(path).map_err(|e| format!("cannot be checked ({e})"))?;
        writable_by_others(&meta)?;
        Ok(Dir(path.to_owned()))
    }

    pub(super) fn read_entry(_: &Dir, path: &Path, _name: &str) -> Entry {
        let meta = match std::fs::symlink_metadata(path) {
            Ok(meta) => meta,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Entry::Missing,
            Err(e) => return Entry::Refused(format!("cannot be checked ({e})")),
        };
        if !meta.file_type().is_file() {
            return Entry::Refused("is not a regular file".into());
        }
        if let Err(why) = writable_by_others(&meta) {
            return Entry::Refused(why);
        }
        match std::fs::read(path) {
            Ok(bytes) => Entry::Bytes(bytes),
            Err(e) => Entry::Refused(format!("cannot be read ({e})")),
        }
    }

    pub(super) fn fix(dir: &Path) -> String {
        format!("chmod 700 {}", dir.display())
    }

    pub(super) fn warn_ancestors(_: &Path) {}
}

/// Writes to a temp file in `dir`, then renames over `path`, so concurrent
/// readers never see a partial artifact. The file is the owner's alone
/// (0600 on Unix). The temp name is the first of `names` not taken: a
/// file another process left there is neither reused nor removed.
fn write_atomically(
    dir: &Path,
    path: &Path,
    bytes: &[u8],
    names: &mut dyn Iterator<Item = String>,
) -> Result<(), Error> {
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    for name in names.take(16) {
        let tmp = dir.join(name);
        let mut file = match options.open(&tmp) {
            Ok(file) => file,
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(e) => return Err(Error::cache_io(path, e)),
        };
        // From here the temp file is this call's own: removed on failure.
        let written = file
            .write_all(bytes)
            .and_then(|()| file.sync_all())
            .and_then(|()| std::fs::rename(&tmp, path));
        if let Err(e) = written {
            let _ = std::fs::remove_file(&tmp);
            return Err(Error::cache_io(path, e));
        }
        return Ok(());
    }
    Err(Error::cache_io(
        path,
        std::io::Error::new(
            std::io::ErrorKind::AlreadyExists,
            "no free temporary file name",
        ),
    ))
}

/// Temp file names unlikely to be any other process's: random (the
/// standard library's per-process random keys), with the pid, the time and
/// a counter hashed in.
fn temp_names() -> impl Iterator<Item = String> {
    use std::hash::BuildHasher;
    let state = std::collections::hash_map::RandomState::new();
    (0u64..).map(move |n| {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos());
        let random = state.hash_one((std::process::id(), unique(), n, now));
        format!(".tmp-{random:016x}")
    })
}

fn unique() -> u64 {
    use std::sync::atomic::AtomicU64;
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

#[cfg(test)]
mod tests {
    use super::*;

    /// A temp file another process left (a killed start, a container
    /// whose pid is always 1) neither fails the write nor is touched.
    #[test]
    fn a_stale_temp_file_is_left_alone() {
        let dir = tempfile::tempdir().unwrap();
        let stale = dir.path().join(".tmp-taken");
        std::fs::write(&stale, b"someone else's").unwrap();
        let path = dir.path().join("x.cwasm");
        let mut names = [".tmp-taken", ".tmp-free"].into_iter().map(String::from);
        write_atomically(dir.path(), &path, b"artefact", &mut names).unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), b"artefact");
        assert_eq!(std::fs::read(&stale).unwrap(), b"someone else's");
        assert!(!dir.path().join(".tmp-free").exists());
    }

    #[test]
    fn temp_names_differ() {
        let names: std::collections::BTreeSet<_> = temp_names().take(100).collect();
        assert_eq!(names.len(), 100);
        assert!(names.iter().all(|n| n.starts_with(".tmp-")));
    }
}
