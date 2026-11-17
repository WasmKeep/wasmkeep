//! Files on a node without a bucket (ADR 0030, section 2). Each namespace
//! is a directory holding a small SQLite index (`index.db`: key, file id,
//! size, type, ETag, time) and the contents as `f/<random id>`, written to
//! `tmp/` beside them, fsynced, renamed into `f/`, then committed in the
//! index; conditions are checked in the index's transaction. No path is
//! built from a key. Files in `f/` or `tmp/` that no row names (a crash
//! between the rename and the commit, an abandoned write) are removed the
//! first time this process opens the namespace.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use rusqlite::{Connection, OptionalExtension as _, TransactionBehavior, params};
use wasmkeep_wit::wasmkeep::actor::blobs::{BlobError, BlobInfo, Condition};

use super::streams::{Source, Spooled};
use super::{MAX_BUFFERED, Namespace, Write, now_ms, random_id, unavailable};

const INDEX: &str = "index.db";
const FILES: &str = "f";
const TMP: &str = "tmp";

#[derive(Debug)]
pub(super) struct Disk {
    /// `<data_dir>/state/<t>/<a>/blobs/<epoch>`.
    base: PathBuf,
    /// Namespaces this process has opened (and cleaned) already.
    opened: Arc<Mutex<HashSet<PathBuf>>>,
}

/// One row of the index.
struct Row {
    file: String,
    size: u64,
    content_type: String,
    etag: String,
    modified: u64,
}

impl Row {
    fn info(&self, key: &str) -> BlobInfo {
        BlobInfo {
            key: key.to_owned(),
            size: self.size,
            content_type: self.content_type.clone(),
            etag: self.etag.clone(),
            modified_ms: self.modified,
        }
    }
}

fn io(e: impl std::fmt::Display) -> BlobError {
    unavailable(format!("the node's disk: {e}"))
}

fn fsync_dir(dir: &Path) -> std::io::Result<()> {
    std::fs::File::open(dir)?.sync_all()
}

fn row(conn: &Connection, key: &str) -> Result<Option<Row>, BlobError> {
    conn.query_row(
        "SELECT file, size, type, etag, modified FROM files WHERE key = ?1",
        [key],
        |r| {
            Ok(Row {
                file: r.get(0)?,
                size: r.get::<_, i64>(1)? as u64,
                content_type: r.get(2)?,
                etag: r.get(3)?,
                modified: r.get::<_, i64>(4)? as u64,
            })
        },
    )
    .optional()
    .map_err(io)
}

impl Disk {
    pub(super) fn new(base: PathBuf) -> Self {
        Self {
            base,
            opened: Arc::default(),
        }
    }

    fn dir(&self, ns: &Namespace) -> PathBuf {
        self.base.join(ns.rel())
    }

    /// Runs `f` on the namespace's index on a blocking thread, the
    /// namespace created and cleaned first if this process has not opened
    /// it yet.
    async fn with<T: Send + 'static>(
        &self,
        ns: &Namespace,
        f: impl FnOnce(&Path, &mut Connection) -> Result<T, BlobError> + Send + 'static,
    ) -> Result<T, BlobError> {
        let dir = self.dir(ns);
        let opened = Arc::clone(&self.opened);
        tokio::task::spawn_blocking(move || {
            let mut conn = open(&dir, &opened)?;
            f(&dir, &mut conn)
        })
        .await
        .map_err(unavailable)?
    }

    pub(super) async fn head(
        &self,
        ns: &Namespace,
        key: &str,
    ) -> Result<Option<BlobInfo>, BlobError> {
        let key = key.to_owned();
        self.with(
            ns,
            move |_, conn| Ok(row(conn, &key)?.map(|r| r.info(&key))),
        )
        .await
    }

    pub(super) async fn get(
        &self,
        ns: &Namespace,
        key: &str,
    ) -> Result<Option<(BlobInfo, Vec<u8>)>, BlobError> {
        let key = key.to_owned();
        self.with(ns, move |dir, conn| {
            // A write that replaced the file between the row and the read
            // removed the one the row named: read the row again, once.
            for _ in 0..2 {
                let Some(r) = row(conn, &key)? else {
                    return Ok(None);
                };
                if r.size > MAX_BUFFERED {
                    return Err(BlobError::TooLarge);
                }
                match std::fs::read(dir.join(FILES).join(&r.file)) {
                    Ok(bytes) => return Ok(Some((r.info(&key), bytes))),
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                    Err(e) => return Err(io(e)),
                }
            }
            Err(unavailable("the file changed while it was read"))
        })
        .await
    }

    pub(super) async fn open_read(
        &self,
        ns: &Namespace,
        key: &str,
        range: Option<(u64, u64)>,
    ) -> Result<Option<(BlobInfo, Source)>, BlobError> {
        let key = key.to_owned();
        self.with(ns, move |dir, conn| {
            for _ in 0..2 {
                let Some(r) = row(conn, &key)? else {
                    return Ok(None);
                };
                let (start, length) = super::bucket::clamp(range, r.size)?;
                match std::fs::File::open(dir.join(FILES).join(&r.file)) {
                    Ok(file) => {
                        let source = Source::disk(file, start, length).map_err(io)?;
                        return Ok(Some((r.info(&key), source)));
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                    Err(e) => return Err(io(e)),
                }
            }
            Err(unavailable("the file changed while it was opened"))
        })
        .await
    }

    pub(super) async fn put(
        &self,
        ns: &Namespace,
        key: &str,
        body: Vec<u8>,
        write: Write,
    ) -> Result<BlobInfo, BlobError> {
        let key = key.to_owned();
        self.with(ns, move |dir, conn| {
            let id = random_id()?;
            let tmp = dir.join(TMP).join(&id);
            let size = body.len() as u64;
            (|| {
                use std::io::Write as _;
                let mut f = std::fs::File::create_new(&tmp)?;
                f.write_all(&body)?;
                f.sync_all()
            })()
            .map_err(|e| {
                let _ = std::fs::remove_file(&tmp);
                io(e)
            })?;
            place(dir, &tmp, &id)?;
            commit(dir, conn, &key, &id, size, &write)
        })
        .await
    }

    /// A spool file for a streamed write, in the namespace's `tmp/`.
    pub(super) async fn spool_path(&self, ns: &Namespace) -> Result<PathBuf, BlobError> {
        let id = random_id()?;
        self.with(ns, move |dir, _| Ok(dir.join(TMP).join(id)))
            .await
    }

    pub(super) async fn commit(
        &self,
        ns: &Namespace,
        key: &str,
        spooled: Spooled,
        write: Write,
    ) -> Result<BlobInfo, BlobError> {
        let key = key.to_owned();
        self.with(ns, move |dir, conn| {
            let id = random_id()?;
            place(dir, &spooled.path, &id)?;
            let size = spooled.size;
            spooled.persist();
            commit(dir, conn, &key, &id, size, &write)
        })
        .await
    }

    pub(super) async fn delete(&self, ns: &Namespace, key: &str) -> Result<(), BlobError> {
        let key = key.to_owned();
        self.with(ns, move |dir, conn| {
            let tx = conn
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(io)?;
            let old = row(&tx, &key)?;
            tx.execute("DELETE FROM files WHERE key = ?1", [&key])
                .map_err(io)?;
            tx.commit().map_err(io)?;
            if let Some(old) = old {
                remove_content(dir, &old.file);
            }
            Ok(())
        })
        .await
    }

    pub(super) async fn list(
        &self,
        ns: &Namespace,
        prefix: &str,
        after: Option<&str>,
        limit: u32,
    ) -> Result<(Vec<BlobInfo>, Option<String>), BlobError> {
        let (prefix, after) = (prefix.to_owned(), after.unwrap_or_default().to_owned());
        self.with(ns, move |_, conn| {
            let mut stmt = conn
                .prepare(
                    "SELECT key, size, type, etag, modified FROM files \
                     WHERE key > ?1 AND key >= ?2 AND substr(key, 1, ?3) = ?2 \
                     ORDER BY key LIMIT ?4",
                )
                .map_err(io)?;
            let rows = stmt
                .query_map(
                    params![after, prefix, prefix.len() as i64, i64::from(limit) + 1],
                    |r| {
                        Ok(BlobInfo {
                            key: r.get(0)?,
                            size: r.get::<_, i64>(1)? as u64,
                            content_type: r.get(2)?,
                            etag: r.get(3)?,
                            modified_ms: r.get::<_, i64>(4)? as u64,
                        })
                    },
                )
                .map_err(io)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(io)?;
            Ok(super::bucket::page(rows, limit))
        })
        .await
    }
}

/// Opens the namespace's index at `dir`, creating it; the first time in
/// this process, removes every content and spool file no row names.
fn open(dir: &Path, opened: &Mutex<HashSet<PathBuf>>) -> Result<Connection, BlobError> {
    // Held while cleaning: no write of this namespace starts meanwhile.
    let mut opened = opened.lock().unwrap_or_else(|p| p.into_inner());
    let first = !opened.contains(dir);
    if first {
        std::fs::create_dir_all(dir.join(FILES)).map_err(io)?;
        std::fs::create_dir_all(dir.join(TMP)).map_err(io)?;
    }
    let conn = Connection::open(dir.join(INDEX)).map_err(io)?;
    conn.busy_timeout(std::time::Duration::from_secs(5))
        .map_err(io)?;
    conn.execute_batch(
        "PRAGMA journal_mode = WAL; PRAGMA synchronous = FULL;
         CREATE TABLE IF NOT EXISTS files (
             key TEXT PRIMARY KEY NOT NULL,
             file TEXT NOT NULL,
             size INTEGER NOT NULL,
             type TEXT NOT NULL,
             etag TEXT NOT NULL,
             modified INTEGER NOT NULL
         ) WITHOUT ROWID;",
    )
    .map_err(io)?;
    if first {
        clean(dir, &conn)?;
        opened.insert(dir.to_owned());
    }
    Ok(conn)
}

/// Removes what no row names: every spool file, and every content file
/// whose write did not commit.
fn clean(dir: &Path, conn: &Connection) -> Result<(), BlobError> {
    for entry in std::fs::read_dir(dir.join(TMP)).map_err(io)? {
        let _ = std::fs::remove_file(entry.map_err(io)?.path());
    }
    let mut named = HashSet::new();
    let mut stmt = conn.prepare("SELECT file FROM files").map_err(io)?;
    for f in stmt.query_map([], |r| r.get::<_, String>(0)).map_err(io)? {
        named.insert(f.map_err(io)?);
    }
    let mut removed = false;
    for entry in std::fs::read_dir(dir.join(FILES)).map_err(io)? {
        let entry = entry.map_err(io)?;
        if !named.contains(entry.file_name().to_string_lossy().as_ref()) {
            let _ = std::fs::remove_file(entry.path());
            removed = true;
        }
    }
    if removed {
        fsync_dir(&dir.join(FILES)).map_err(io)?;
    }
    Ok(())
}

/// Moves a complete, fsynced body into `f/<id>` and makes the rename
/// durable. Not visible until [`commit`] names it.
fn place(dir: &Path, from: &Path, id: &str) -> Result<(), BlobError> {
    let to = dir.join(FILES).join(id);
    std::fs::rename(from, &to).map_err(|e| {
        let _ = std::fs::remove_file(from);
        io(e)
    })?;
    fsync_dir(&dir.join(FILES)).map_err(io)
}

/// Names `f/<id>` as the file at `key` if `write`'s condition holds, and
/// removes the content it replaces; otherwise removes `f/<id>`.
fn commit(
    dir: &Path,
    conn: &mut Connection,
    key: &str,
    id: &str,
    size: u64,
    write: &Write,
) -> Result<BlobInfo, BlobError> {
    let result = (|| {
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(io)?;
        let old = row(&tx, key)?;
        let holds = match (&write.condition, &old) {
            (Condition::Any, _) | (Condition::Absent, None) => true,
            (Condition::Etag(e), Some(old)) => *e == old.etag,
            (Condition::Absent, Some(_)) | (Condition::Etag(_), None) => false,
        };
        if !holds {
            return Err(BlobError::Conflict);
        }
        let new = Row {
            file: id.to_owned(),
            size,
            content_type: write.content_type.clone(),
            etag: id.to_owned(),
            modified: now_ms(),
        };
        tx.execute(
            "INSERT OR REPLACE INTO files (key, file, size, type, etag, modified) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                key,
                new.file,
                new.size as i64,
                new.content_type,
                new.etag,
                new.modified as i64
            ],
        )
        .map_err(io)?;
        tx.commit().map_err(io)?;
        Ok((new.info(key), old))
    })();
    match result {
        Ok((info, old)) => {
            if let Some(old) = old {
                remove_content(dir, &old.file);
            }
            Ok(info)
        }
        Err(e) => {
            remove_content(dir, id);
            Err(e)
        }
    }
}

fn remove_content(dir: &Path, file: &str) {
    let _ = std::fs::remove_file(dir.join(FILES).join(file));
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)] // test code
    use super::*;

    fn write(condition: Condition) -> Write {
        Write {
            content_type: "text/plain".into(),
            condition,
        }
    }

    #[tokio::test]
    async fn a_crash_between_rename_and_commit_leaves_no_visible_file() {
        let tmp = tempfile::tempdir().unwrap();
        let ns = Namespace::actor("c", "n", "0");
        let disk = Disk::new(tmp.path().join("blobs/e"));
        disk.put(&ns, "kept", b"one".to_vec(), write(Condition::Any))
            .await
            .unwrap();
        // The write's first half: fsynced, renamed into `f/`, never
        // committed; and a spool file an abandoned write left.
        let dir = disk.dir(&ns);
        let staged = dir.join(TMP).join("staged");
        std::fs::write(&staged, b"two").unwrap();
        place(&dir, &staged, "0123456789abcdef").unwrap();
        std::fs::write(dir.join(TMP).join("spool"), b"three").unwrap();
        assert!(dir.join(FILES).join("0123456789abcdef").exists());

        // The node starts again.
        let disk = Disk::new(tmp.path().join("blobs/e"));
        let (listed, next) = disk.list(&ns, "", None, 10).await.unwrap();
        assert_eq!(
            listed.iter().map(|i| i.key.as_str()).collect::<Vec<_>>(),
            ["kept"]
        );
        assert_eq!(next, None);
        assert!(!dir.join(FILES).join("0123456789abcdef").exists());
        assert_eq!(std::fs::read_dir(dir.join(TMP)).unwrap().count(), 0);
        assert_eq!(std::fs::read_dir(dir.join(FILES)).unwrap().count(), 1);
        let (_, body) = disk.get(&ns, "kept").await.unwrap().unwrap();
        assert_eq!(body, b"one");
    }

    #[tokio::test]
    async fn conditions_are_checked_in_the_index() {
        let tmp = tempfile::tempdir().unwrap();
        let ns = Namespace::front();
        let disk = Disk::new(tmp.path().to_owned());
        let first = disk
            .put(&ns, "a", b"1".to_vec(), write(Condition::Absent))
            .await
            .unwrap();
        assert!(matches!(
            disk.put(&ns, "a", b"2".to_vec(), write(Condition::Absent))
                .await,
            Err(BlobError::Conflict)
        ));
        assert!(matches!(
            disk.put(&ns, "a", b"2".to_vec(), write(Condition::Etag("x".into())))
                .await,
            Err(BlobError::Conflict)
        ));
        assert!(matches!(
            disk.put(
                &ns,
                "b",
                b"2".to_vec(),
                write(Condition::Etag(first.etag.clone()))
            )
            .await,
            Err(BlobError::Conflict)
        ));
        let second = disk
            .put(&ns, "a", b"22".to_vec(), write(Condition::Etag(first.etag)))
            .await
            .unwrap();
        assert_eq!(second.size, 2);
        let (info, body) = disk.get(&ns, "a").await.unwrap().unwrap();
        assert_eq!((info.etag, body), (second.etag, b"22".to_vec()));
        // The replaced and the refused contents are gone: one file left.
        assert_eq!(
            std::fs::read_dir(disk.dir(&ns).join(FILES))
                .unwrap()
                .count(),
            1
        );
        disk.delete(&ns, "a").await.unwrap();
        disk.delete(&ns, "a").await.unwrap();
        assert!(disk.head(&ns, "a").await.unwrap().is_none());
        assert_eq!(
            std::fs::read_dir(disk.dir(&ns).join(FILES))
                .unwrap()
                .count(),
            0
        );
    }
}
