//! Development assets: a table rebuilt by the CLI, and descriptor-relative reads.
use super::Index;
use std::{
    fs::File,
    io,
    path::Path,
    sync::{Arc, RwLock},
};

#[derive(Debug)]
pub struct Local {
    parent: File,
    name: std::ffi::OsString,
    index: RwLock<Arc<Index>>,
}
impl Local {
    /// Resolves DIR once. Children are never followed through symbolic links.
    pub fn new(dir: &Path, index: Index) -> io::Result<Arc<Self>> {
        let root = dir.canonicalize()?;
        let local = Arc::new(Self {
            parent: File::open(root.parent().unwrap_or(&root))?,
            name: root
                .file_name()
                .unwrap_or_else(|| std::ffi::OsStr::new("."))
                .to_owned(),
            index: RwLock::new(Arc::new(index)),
        });
        local.directory()?;
        Ok(local)
    }
    /// Reopen the resolved DIR relative to its pinned parent. Builds may replace
    /// the directory; replacing it with a symlink never changes the root.
    #[cfg(unix)]
    pub fn directory(&self) -> io::Result<File> {
        use rustix::fs::{Mode, OFlags, openat};
        Ok(File::from(openat(
            &self.parent,
            &self.name,
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
        )?))
    }
    #[cfg(not(unix))]
    pub fn directory(&self) -> io::Result<File> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "safe dev asset scanning requires Unix",
        ))
    }
    pub fn replace(&self, index: Index) {
        *self.index.write().unwrap_or_else(|p| p.into_inner()) = Arc::new(index);
    }
    pub(super) fn index(&self) -> Arc<Index> {
        self.index.read().unwrap_or_else(|p| p.into_inner()).clone()
    }
    #[cfg(unix)]
    pub(super) fn open(&self, name: &str) -> io::Result<File> {
        use rustix::fs::{Mode, OFlags, openat};
        if !super::valid_name(name) {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        let mut file = self.directory()?;
        let mut parts = name.split('/').peekable();
        while let Some(part) = parts.next() {
            let mut flags = OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC;
            if parts.peek().is_some() {
                flags |= OFlags::DIRECTORY;
            }
            file = File::from(openat(&file, part, flags, Mode::empty())?);
        }
        if !file.metadata()?.is_file() {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        Ok(file)
    }
    #[cfg(not(unix))]
    pub(super) fn open(&self, _name: &str) -> io::Result<File> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "safe dev asset reads require Unix",
        ))
    }
}
