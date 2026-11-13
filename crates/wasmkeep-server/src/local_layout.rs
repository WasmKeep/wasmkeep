//! Moving a node's local databases out of the layout before 0.4.0, once,
//! at start (ADR 0022), before anything else touches the data directory.
//!
//! Before 0.4.0 a tenant's databases lived at `<data_dir>/actors/<t>/`,
//! shared by every app of the tenant; they now live under the app's root,
//! `<data_dir>/state/<t>/<a>/actors/`.
//!
//! - **In a cluster** the bucket is the truth (and `cluster migrate` moved
//!   it): the old folder is put aside, at `<data_dir>/actors.pre-0.4/<t>`,
//!   and each actor restores from its new root on its first wake.
//! - **Alone** the local copy is the only one: the old folder becomes this
//!   app's, if no app of the tenant has local state yet. Otherwise the
//!   start is refused: which app it belongs to is the operator's call.
//!
//! Either way it is one `rename(2)` (atomic, the sidecars move with the
//! databases; never a copy, so a rename across file systems is refused),
//! then a regular file at the old path saying where the data went: a
//! binary from before 0.4.0 then fails to create its folder instead of
//! starting empty. Both folders present (a crash before that file, then an
//! older binary ran) is refused, naming both: they are never merged.

use std::path::{Path, PathBuf};

use wasmkeep_cluster::state_root::StateRoot;
use wasmkeep_storage::encode_segment;

use crate::Error;

/// Where a cluster node puts the old folders aside, under the data
/// directory.
pub(crate) const PUT_ASIDE: &str = "actors.pre-0.4";

/// Moves `root`'s tenant's databases of the layout before 0.4.0, if any:
/// aside in a cluster (`cluster`), into `root` when alone. See the module
/// documentation.
pub(crate) fn prepare(data_dir: &Path, root: &StateRoot, cluster: bool) -> Result<(), Error> {
    let old = data_dir.join(root.bare_local(encode_segment));
    let new = if cluster {
        data_dir.join(PUT_ASIDE).join(encode_segment(root.tenant()))
    } else {
        data_dir.join(root.local(encode_segment))
    };
    let refuse = |why: String| Err(Error::LocalLayout(why));
    let io = |what: &str, path: &Path, e: std::io::Error| {
        Error::LocalLayout(format!("{what} {}: {e}", path.display()))
    };
    match std::fs::symlink_metadata(&old) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            // A crash between the rename and the file: write the file.
            // Only in a data directory of the old layout (its `actors/`
            // folder is left behind), so a fresh one stays clean.
            if new.is_dir() && data_dir.join("actors").is_dir() {
                write_note(&old, &new).map_err(|e| io("writing", &old, e))?;
            }
            Ok(())
        }
        Err(e) => Err(io("reading", &old, e)),
        // Moved already: the file is ours.
        Ok(m) if m.is_file() => Ok(()),
        Ok(m) if !m.is_dir() => refuse(format!(
            "{} is neither the databases of a version before 0.4.0 nor the note this \
             version leaves there: move it away and start again",
            old.display()
        )),
        Ok(_) => {
            // The folder that would receive it, and anything an app of the
            // tenant already has there.
            let taken = if cluster {
                new.exists().then(|| new.clone())
            } else {
                let tenant_dir = data_dir.join("state").join(encode_segment(root.tenant()));
                first_entry(&tenant_dir).map_err(|e| io("reading", &tenant_dir, e))?
            };
            if let Some(taken) = taken {
                return refuse(format!(
                    "both {} (databases of a version before 0.4.0) and {} exist: they are \
                     never merged. Keep the one to serve, move the other out of the data \
                     directory, and start again (docs/backup-and-restore.md, Migrating to \
                     0.4.0)",
                    old.display(),
                    taken.display()
                ));
            }
            if let Some(parent) = new.parent() {
                std::fs::create_dir_all(parent).map_err(|e| io("creating", parent, e))?;
            }
            // Never a copy: a failed rename (another file system, say)
            // leaves everything where it was.
            std::fs::rename(&old, &new).map_err(|e| {
                Error::LocalLayout(format!(
                    "moving {} to {}: {e}; nothing was moved (a copy is never made: \
                     move it by hand with mv on one file system)",
                    old.display(),
                    new.display()
                ))
            })?;
            write_note(&old, &new).map_err(|e| io("writing", &old, e))?;
            tracing::info!(
                from = %old.display(),
                to = %new.display(),
                "databases of a version before 0.4.0 moved (ADR 0022)"
            );
            Ok(())
        }
    }
}

/// The first entry of `dir`, if it has one (`None` if it does not exist).
fn first_entry(dir: &Path) -> std::io::Result<Option<PathBuf>> {
    match std::fs::read_dir(dir) {
        Ok(mut entries) => entries.next().transpose().map(|e| e.map(|e| e.path())),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}

/// The regular file left at `old`, saying where the databases went.
fn write_note(old: &Path, new: &Path) -> std::io::Result<()> {
    std::fs::write(
        old,
        format!(
            "WasmKeep 0.4.0 moved the databases that were here to\n{}\n\
             (ADR 0022). This file keeps an older WasmKeep from starting empty here.\n",
            new.display()
        ),
    )
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)] // test code

    use super::*;

    fn root() -> StateRoot {
        StateRoot::new("", "acme", "blog")
    }

    /// A data directory of the old layout: one database and its sidecar.
    fn old_layout(dir: &Path) -> PathBuf {
        let class = dir.join("actors/acme/chat");
        std::fs::create_dir_all(&class).unwrap();
        std::fs::write(class.join("room.db"), b"db").unwrap();
        std::fs::write(class.join("room.db-wal"), b"wal").unwrap();
        dir.join("actors/acme")
    }

    #[test]
    fn alone_the_old_folder_becomes_this_apps() {
        let dir = tempfile::tempdir().unwrap();
        let old = old_layout(dir.path());
        prepare(dir.path(), &root(), false).unwrap();
        let moved = dir.path().join("state/acme/blog/actors/chat");
        assert_eq!(std::fs::read(moved.join("room.db")).unwrap(), b"db");
        assert_eq!(std::fs::read(moved.join("room.db-wal")).unwrap(), b"wal");
        // A file where the folder was: an older binary cannot create it.
        assert!(old.is_file());
        assert!(std::fs::create_dir_all(old.join("chat")).is_err());
        // Once only.
        prepare(dir.path(), &root(), false).unwrap();
        assert!(moved.join("room.db").is_file());
    }

    #[test]
    fn in_a_cluster_the_old_folder_is_put_aside() {
        let dir = tempfile::tempdir().unwrap();
        let old = old_layout(dir.path());
        prepare(dir.path(), &root(), true).unwrap();
        assert!(
            dir.path()
                .join("actors.pre-0.4/acme/chat/room.db")
                .is_file()
        );
        assert!(!dir.path().join("state").exists());
        assert!(old.is_file());
    }

    #[test]
    fn a_crash_before_the_note_is_finished_by_the_next_start() {
        let dir = tempfile::tempdir().unwrap();
        let old = old_layout(dir.path());
        let new = dir.path().join("state/acme/blog/actors");
        std::fs::create_dir_all(new.parent().unwrap()).unwrap();
        std::fs::rename(&old, &new).unwrap();
        prepare(dir.path(), &root(), false).unwrap();
        assert!(old.is_file());
        assert!(new.join("chat/room.db").is_file());
    }

    #[test]
    fn a_fresh_data_directory_stays_clean() {
        let dir = tempfile::tempdir().unwrap();
        prepare(dir.path(), &root(), false).unwrap();
        std::fs::create_dir_all(dir.path().join("state/acme/blog/actors")).unwrap();
        prepare(dir.path(), &root(), false).unwrap();
        assert!(!dir.path().join("actors").exists());
    }

    #[test]
    fn both_folders_are_refused_and_never_merged() {
        let dir = tempfile::tempdir().unwrap();
        let old = old_layout(dir.path());
        // Another app of the tenant already has local state.
        std::fs::create_dir_all(dir.path().join("state/acme/shop/actors")).unwrap();
        let e = prepare(dir.path(), &root(), false).unwrap_err().to_string();
        assert!(e.contains("never merged"), "{e}");
        assert!(e.contains("state/acme/shop"), "{e}");
        assert!(old.join("chat/room.db").is_file());
        // In a cluster: a folder put aside earlier.
        std::fs::create_dir_all(dir.path().join("actors.pre-0.4/acme")).unwrap();
        let e = prepare(dir.path(), &root(), true).unwrap_err().to_string();
        assert!(e.contains("actors.pre-0.4"), "{e}");
        assert!(old.join("chat/room.db").is_file());
    }
}
