use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use crate::wal::{CaptureError, Changes, WalCapture};
use rusqlite::hooks::{AuthAction, AuthContext, Authorization, TransactionOperation};
use rusqlite::{Connection, OpenFlags, OptionalExtension};

use crate::path::encode_segment;
use crate::{Rows, SqlError, SqlValue, StorageError};

/// Storage settings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageConfig {
    /// Root data directory; databases live under `<data_dir>/actors/`.
    pub data_dir: PathBuf,
    /// Size cap per database, in bytes (default: 1 GiB).
    pub max_db_bytes: u64,
    /// Longest a guest statement may run before it is interrupted (the
    /// event's wall-clock limit is the natural value). A statement on a
    /// blocking thread does not stop when its event is cut, so this is what
    /// bounds it.
    pub max_statement_time: Duration,
    /// Largest text or blob value (SQLite's `SQLITE_LIMIT_LENGTH`).
    pub max_value_bytes: usize,
    /// Largest result a `query` may return, all rows together.
    pub max_result_bytes: usize,
    /// Largest the write-ahead log may grow between two captures (while
    /// replicating, committed pages stay in it until the host captures them
    /// after the event). Past it, statements that write are refused until
    /// the next capture; reads and transaction control still run.
    pub max_wal_bytes: u64,
    /// CPU time one event's guest statements may use together (SQLite runs
    /// in the host, outside the guest's own CPU budget). Past it, the
    /// running statement is stopped and later ones refused until
    /// [`ActorDb::end_event`].
    pub max_sql_cpu_per_event: Duration,
}

impl StorageConfig {
    /// Defaults with the given data directory.
    pub fn new(data_dir: impl Into<PathBuf>) -> Self {
        Self {
            data_dir: data_dir.into(),
            max_db_bytes: 1024 * 1024 * 1024,
            max_statement_time: Duration::from_secs(30),
            max_value_bytes: 16 * 1024 * 1024,
            max_result_bytes: 16 * 1024 * 1024,
            max_wal_bytes: 256 * 1024 * 1024,
            max_sql_cpu_per_event: Duration::from_secs(2),
        }
    }
}

/// What a scan for pending alarms found.
#[derive(Debug, Default)]
pub struct PendingAlarms {
    /// `(tenant, class, name, at_ms)` of every actor with a pending alarm.
    pub found: Vec<(String, String, String, u64)>,
    /// Databases that could not be read, each with why.
    pub skipped: Vec<StorageError>,
}

/// Opens and deletes actor databases.
#[derive(Debug, Clone)]
pub struct Storage {
    config: StorageConfig,
    /// Open connections per database file.
    open: Arc<std::sync::Mutex<std::collections::HashMap<PathBuf, usize>>>,
}

/// Counts one open connection to a database file until dropped.
#[derive(Debug)]
struct OpenGuard {
    path: PathBuf,
    open: Arc<std::sync::Mutex<std::collections::HashMap<PathBuf, usize>>>,
}

impl Drop for OpenGuard {
    fn drop(&mut self) {
        let mut open = self.open.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(n) = open.get_mut(&self.path) {
            *n -= 1;
            if *n == 0 {
                open.remove(&self.path);
            }
        }
    }
}

impl Storage {
    /// The settings.
    #[must_use]
    pub fn config(&self) -> &StorageConfig {
        &self.config
    }

    /// Uses `config.data_dir` (created on first open).
    pub fn new(config: StorageConfig) -> Self {
        Self {
            config,
            open: Arc::default(),
        }
    }

    /// Whether any connection to this database file is still open (a
    /// cancelled call may still be closing one on a blocking thread). The
    /// file must not be replaced until it is not.
    pub fn is_open(&self, path: &Path) -> bool {
        self.open
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .contains_key(path)
    }

    /// The database file for an actor.
    pub fn db_path(&self, tenant: &str, class: &str, name: &str) -> PathBuf {
        self.config
            .data_dir
            .join("actors")
            .join(encode_segment(tenant))
            .join(encode_segment(class))
            .join(format!("{}.db", encode_segment(name)))
    }

    /// Opens (creating if needed) an actor's database.
    pub fn open(&self, tenant: &str, class: &str, name: &str) -> Result<ActorDb, StorageError> {
        #[cfg(feature = "fault-injection")]
        crate::faults::pause("open");
        let path = self.db_path(tenant, class, name);
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir).map_err(|source| StorageError::Io {
                path: dir.to_owned(),
                source,
            })?;
        }
        let mut db = open_retrying(&path, &self.config)?;
        *self
            .open
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .entry(path.clone())
            .or_insert(0) += 1;
        db.open_guard = Some(OpenGuard {
            path: path.clone(),
            open: Arc::clone(&self.open),
        });
        // Record the address inside the database: file names of long names
        // are hashed, so scans need it to know which actor a file belongs to.
        let address = format!("{tenant}/{class}/{name}");
        let recorded = db.meta(ADDRESS_KEY).map_err(|e| StorageError::Meta {
            path: path.clone(),
            source: e,
        })?;
        if recorded.as_deref() != Some(address.as_str()) {
            db.set_meta(ADDRESS_KEY, &address)
                .map_err(|e| StorageError::Meta { path, source: e })?;
        }
        Ok(db)
    }

    /// Every actor with a pending alarm. Opens each database under the data
    /// directory; run at startup. A database that cannot be read is
    /// reported in [`PendingAlarms::skipped`], and the scan goes on: one
    /// broken file must not cost every other actor its alarm.
    pub fn pending_alarms(&self) -> Result<PendingAlarms, StorageError> {
        let mut found = Vec::new();
        let mut skipped = Vec::new();
        let root = self.config.data_dir.join("actors");
        for file in db_files(&root)? {
            match Self::alarm_of(&file, &self.config) {
                Ok(Some(alarm)) => found.push(alarm),
                Ok(None) => {}
                Err(e) => skipped.push(e),
            }
        }
        Ok(PendingAlarms { found, skipped })
    }

    /// One database's pending alarm, if it has one.
    fn alarm_of(
        file: &Path,
        config: &StorageConfig,
    ) -> Result<Option<(String, String, String, u64)>, StorageError> {
        let mut db = ActorDb::open(file, config)?;
        #[cfg(feature = "fault-injection")]
        crate::faults::pause("alarm_scan");
        let meta_err = |source| StorageError::Meta {
            path: file.to_owned(),
            source,
        };
        let (Some(address), Some(at)) = (
            db.meta(ADDRESS_KEY).map_err(meta_err)?,
            db.alarm().map_err(meta_err)?,
        ) else {
            return Ok(None);
        };
        let mut parts = address.splitn(3, '/');
        Ok(match (parts.next(), parts.next(), parts.next()) {
            (Some(t), Some(c), Some(n)) => Some((t.to_owned(), c.to_owned(), n.to_owned(), at)),
            _ => None,
        })
    }

    /// Removes an actor's database files. The database must not be open.
    pub fn delete(&self, tenant: &str, class: &str, name: &str) -> Result<(), StorageError> {
        let path = self.db_path(tenant, class, name);
        #[cfg(feature = "fault-injection")]
        if crate::faults::take("delete") {
            return Err(StorageError::Io {
                path,
                source: std::io::Error::other("injected failure"),
            });
        }
        for suffix in ["", "-wal", "-shm"] {
            let file = PathBuf::from(format!("{}{suffix}", path.display()));
            match std::fs::remove_file(&file) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(source) => return Err(StorageError::Io { path: file, source }),
            }
        }
        Ok(())
    }
}

/// Meta key holding `tenant/class/name`.
const ADDRESS_KEY: &str = "address";

/// `*.db` files at `<root>/<tenant>/<class>/`.
fn db_files(root: &Path) -> Result<Vec<PathBuf>, StorageError> {
    let read = |dir: &Path| -> Result<Vec<PathBuf>, StorageError> {
        match std::fs::read_dir(dir) {
            Ok(entries) => Ok(entries.filter_map(Result::ok).map(|e| e.path()).collect()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
            Err(source) => Err(StorageError::Io {
                path: dir.to_owned(),
                source,
            }),
        }
    };
    let mut files = Vec::new();
    for tenant in read(root)?.into_iter().filter(|p| p.is_dir()) {
        for class in read(&tenant)?.into_iter().filter(|p| p.is_dir()) {
            files.extend(
                read(&class)?
                    .into_iter()
                    .filter(|p| p.extension().is_some_and(|e| e == "db")),
            );
        }
    }
    files.sort();
    Ok(files)
}

/// One actor's open database. Not `Sync`: owned by the actor's task.
#[derive(Debug)]
pub struct ActorDb {
    conn: Connection,
    path: PathBuf,
    /// Set while the host runs its own statements, which bypass the sandbox.
    internal: Arc<AtomicBool>,
    /// Set by the authorizer when a guest statement it is preparing is a
    /// `COMMIT` (or `END`).
    commit_seen: Arc<AtomicBool>,
    /// Set by the authorizer when a guest statement it is preparing controls
    /// a transaction (`BEGIN`, `COMMIT`, `ROLLBACK`, `SAVEPOINT`,
    /// `RELEASE`).
    tx_seen: Arc<AtomicBool>,
    /// Set by the authorizer when a guest statement it is preparing is a
    /// `ROLLBACK` (of the whole transaction).
    rollback_seen: Arc<AtomicBool>,
    /// `synchronous` is FULL from an `exec-durable` whose restore to NORMAL
    /// failed: `end_event` restores it.
    sync_full: bool,
    /// A statement failed inside an explicit transaction and SQLite rolled
    /// the transaction back: every later statement of the event is refused,
    /// rather than each committing alone, until `end_event`.
    transaction_aborted: bool,
    /// When the running guest statement must stop (`clock_ms`; 0: none).
    deadline: Arc<AtomicU64>,
    max_statement_time: Duration,
    max_result_bytes: usize,
    max_wal_bytes: u64,
    /// SQL CPU time this event, and the budget for it.
    sql_cpu_used: Duration,
    max_sql_cpu: Duration,
    /// Thread CPU time (ns, `thread_cpu`) at which the running guest
    /// statement must stop; 0: none.
    cpu_deadline: Arc<AtomicU64>,
    /// Change capture for replication, once started.
    capture: Option<WalCapture>,
    /// Last, so it is released after the connection has closed.
    open_guard: Option<OpenGuard>,
}

/// Refuses a statement that may write while the WAL is full. Transaction
/// control counts as read-only, so a guest can still commit or roll back.
fn refuse_writes_if(
    wal_full: impl FnOnce() -> bool,
    stmt: rusqlite::Statement<'_>,
) -> Result<rusqlite::Statement<'_>, SqlError> {
    if !stmt.readonly() && wal_full() {
        return Err(SqlError::new(
            crate::SqlErrorKind::QuotaExceeded,
            "too many writes in one event: the write-ahead log is full until the \
             event ends; split the work across events",
        ));
    }
    Ok(stmt)
}

/// Whether the WAL file is over its cap (only checked for statements that
/// may write, so reads cost no system call).
fn wal_over(limit: Option<&(Arc<Path>, u64)>) -> bool {
    limit.is_some_and(|(path, max)| std::fs::metadata(path).is_ok_and(|m| m.len() > *max))
}

/// Checkpoint once this much WAL has been captured.
const CHECKPOINT_AFTER_BYTES: u64 = 4 << 20;

/// PRAGMAs guests may run: read-only introspection, `user_version` for
/// migrations, and `foreign_keys`.
const ALLOWED_PRAGMAS: &[&str] = &[
    // Read-only; FTS5 queries it internally.
    "data_version",
    "foreign_keys",
    "foreign_key_check",
    "foreign_key_list",
    "index_info",
    "index_list",
    "index_xinfo",
    "integrity_check",
    "page_count",
    "quick_check",
    "table_info",
    "table_list",
    "table_xinfo",
    "user_version",
];

/// [`ActorDb::open`], again after a short wait when SQLite answers "busy"
/// or "locked": in WAL mode some of those come without the busy handler
/// (the 5 s wait rusqlite sets), for example while another connection to
/// the file, such as the previous instance's closing after hibernation,
/// rebuilds the shared-memory index. Under load that failed an actor's
/// first event (a WebSocket closed with "unavailable"). Opening is
/// idempotent (pragmas, `CREATE TABLE IF NOT EXISTS`), so it is retried
/// up to five times over about a third of a second.
fn open_retrying(path: &Path, config: &StorageConfig) -> Result<ActorDb, StorageError> {
    let mut wait = std::time::Duration::from_millis(20);
    for attempt in 1..=5 {
        match open_once(path, config) {
            Err(StorageError::Open { source, .. }) if attempt < 5 && is_busy(&source) => {
                std::thread::sleep(wait);
                wait *= 2;
            }
            other => return other,
        }
    }
    unreachable!("the fifth attempt returns")
}

/// One [`ActorDb::open`], or a "busy" injected for tests (`open_busy`).
fn open_once(path: &Path, config: &StorageConfig) -> Result<ActorDb, StorageError> {
    #[cfg(feature = "fault-injection")]
    if crate::faults::take("open_busy") {
        return Err(StorageError::Open {
            path: path.to_owned(),
            source: rusqlite::Error::SqliteFailure(
                rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_BUSY),
                Some("database is locked (injected)".into()),
            ),
        });
    }
    ActorDb::open(path, config)
}

/// SQLite's "busy" and "locked" families.
fn is_busy(e: &rusqlite::Error) -> bool {
    matches!(
        e.sqlite_error_code(),
        Some(rusqlite::ErrorCode::DatabaseBusy | rusqlite::ErrorCode::DatabaseLocked)
    )
}

impl ActorDb {
    fn open(path: &Path, config: &StorageConfig) -> Result<Self, StorageError> {
        let max_db_bytes = config.max_db_bytes;
        let open_err = |source| StorageError::Open {
            path: path.to_owned(),
            source,
        };
        // No URI filenames, no shared cache; one connection per actor.
        let flags = OpenFlags::SQLITE_OPEN_READ_WRITE
            | OpenFlags::SQLITE_OPEN_CREATE
            | OpenFlags::SQLITE_OPEN_NO_MUTEX;
        let mut conn = Connection::open_with_flags(path, flags).map_err(open_err)?;

        conn.pragma_update(None, "journal_mode", "WAL")
            .map_err(open_err)?;
        conn.pragma_update(None, "synchronous", "NORMAL")
            .map_err(open_err)?;
        conn.pragma_update(None, "foreign_keys", "ON")
            .map_err(open_err)?;
        // Defensive mode: no writes to the schema tables, no corrupting
        // through shadow tables; and functions in the schema (triggers,
        // views) must be innocuous.
        conn.set_db_config(rusqlite::config::DbConfig::SQLITE_DBCONFIG_DEFENSIVE, true)
            .map_err(open_err)?;
        conn.pragma_update(None, "trusted_schema", "OFF")
            .map_err(open_err)?;
        let value_limit = i32::try_from(config.max_value_bytes).unwrap_or(i32::MAX);
        conn.set_limit(rusqlite::limits::Limit::SQLITE_LIMIT_LENGTH, value_limit)
            .map_err(open_err)?;
        let page_size: i64 = conn
            .pragma_query_value(None, "page_size", |r| r.get(0))
            .map_err(open_err)?;
        let max_pages = max_db_bytes / u64::try_from(page_size.max(512)).unwrap_or(4096);
        let max_pages = i64::try_from(max_pages.max(1)).unwrap_or(i64::MAX);
        conn.pragma_update(None, "max_page_count", max_pages)
            .map_err(open_err)?;

        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS main._wasmkeep_meta (
                 key TEXT PRIMARY KEY,
                 value TEXT NOT NULL
             );
             CREATE TABLE IF NOT EXISTS main._wasmkeep_alarm (
                 id INTEGER PRIMARY KEY CHECK (id = 1),
                 at INTEGER NOT NULL
             );",
        )
        .map_err(open_err)?;

        let internal = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&internal);
        let commit_seen = Arc::new(AtomicBool::new(false));
        let seen = Arc::clone(&commit_seen);
        let tx_seen = Arc::new(AtomicBool::new(false));
        let control = Arc::clone(&tx_seen);
        let rollback_seen = Arc::new(AtomicBool::new(false));
        let rollback = Arc::clone(&rollback_seen);
        conn.authorizer(Some(move |ctx: AuthContext<'_>| {
            if flag.load(Ordering::Relaxed) {
                return Authorization::Allow;
            }
            if matches!(
                ctx.action,
                AuthAction::Transaction { .. } | AuthAction::Savepoint { .. }
            ) {
                control.store(true, Ordering::Relaxed);
            }
            if let AuthAction::Transaction {
                operation: TransactionOperation::Rollback,
            } = ctx.action
            {
                rollback.store(true, Ordering::Relaxed);
            }
            // SQLite names BEGIN, COMMIT or ROLLBACK here; rusqlite has no
            // variant for COMMIT. Should one appear, COMMIT is refused by
            // `exec-durable`, not mistaken for something else.
            if let AuthAction::Transaction {
                operation: TransactionOperation::Unknown,
            } = ctx.action
            {
                seen.store(true, Ordering::Relaxed);
            }
            authorize(&ctx)
        }))
        .map_err(open_err)?;

        // Interrupts a guest statement past its deadline or the event's SQL
        // CPU budget (0: none set). The statement runs on this thread, so
        // the thread's CPU clock is its time.
        let deadline = Arc::new(AtomicU64::new(0));
        let cpu_deadline = Arc::new(AtomicU64::new(0));
        let due = Arc::clone(&deadline);
        let cpu_due = Arc::clone(&cpu_deadline);
        conn.progress_handler(
            PROGRESS_STEPS,
            Some(move || {
                let at = due.load(Ordering::Relaxed);
                let cpu_at = cpu_due.load(Ordering::Relaxed);
                (at != 0 && clock_ms() > at) || (cpu_at != 0 && thread_cpu_ns() > cpu_at)
            }),
        )
        .map_err(open_err)?;

        Ok(Self {
            conn,
            path: path.to_owned(),
            internal,
            commit_seen,
            tx_seen,
            rollback_seen,
            sync_full: false,
            transaction_aborted: false,
            deadline,
            max_statement_time: config.max_statement_time,
            max_result_bytes: config.max_result_bytes,
            max_wal_bytes: config.max_wal_bytes,
            sql_cpu_used: Duration::ZERO,
            max_sql_cpu: config.max_sql_cpu_per_event,
            cpu_deadline,
            capture: None,
            open_guard: None,
        })
    }

    /// Starts capturing committed changes for replication: turns automatic
    /// checkpoints off, so committed pages stay in the WAL until captured,
    /// and checkpoints once so capture starts from a complete main file.
    pub fn start_capture(&mut self) -> Result<(), CaptureError> {
        self.internal(|conn| conn.pragma_update(None, "wal_autocheckpoint", 0))
            .map_err(|e| CaptureError::Sql(e.to_string()))?;
        self.checkpoint()?;
        let page_size: i64 = self
            .internal(|conn| conn.pragma_query_value(None, "page_size", |r| r.get(0)))
            .map_err(|e| CaptureError::Sql(e.to_string()))?;
        let page_size = u32::try_from(page_size).unwrap_or(4096);
        self.capture = Some(WalCapture::start(&self.path, page_size)?);
        Ok(())
    }

    /// What was committed since the last call, for replication; `None` if
    /// nothing was (or capture is not started). Call between events, with no
    /// transaction open.
    pub fn take_changes(&mut self) -> Result<Option<Changes>, CaptureError> {
        let Some(capture) = self.capture.as_mut() else {
            return Ok(None);
        };
        let changes = capture.capture()?;
        // A checkpoint cannot run inside an open transaction; try later.
        let threshold = CHECKPOINT_AFTER_BYTES.min(self.max_wal_bytes);
        // The file, not just what was captured: a rolled-back transaction
        // that spilled leaves frames no capture consumes, and a WAL left
        // over the cap would refuse every later write.
        let size = capture.wal_bytes().max(capture.wal_len());
        if size >= threshold && self.conn.is_autocommit() {
            self.checkpoint()?;
            if let Some(c) = self.capture.as_mut() {
                c.wal_truncated();
            }
        }
        Ok(changes)
    }

    /// Every page of the database, for a snapshot upload. Captures pending
    /// changes first; if there were any, returns them in
    /// [`CaptureError::Pending`] (ship them, then snapshot again).
    pub fn snapshot(&mut self) -> Result<Changes, CaptureError> {
        let Some(capture) = self.capture.as_mut() else {
            return Err(CaptureError::Sql("capture not started".into()));
        };
        if let Some(pending) = capture.capture()? {
            // Hand them back: they must be shipped before the snapshot.
            return Err(CaptureError::Pending(Box::new(pending)));
        }
        self.checkpoint()?;
        let capture = self
            .capture
            .as_mut()
            .ok_or(CaptureError::Sql("capture not started".into()))?;
        capture.wal_truncated();
        capture.snapshot()
    }

    /// The database checksum as of the last capture, if capturing.
    pub fn captured_checksum(&self) -> Option<crate::ltx::DbChecksum> {
        self.capture.as_ref().map(WalCapture::checksum)
    }

    /// Moves everything into the main file and empties the WAL.
    fn checkpoint(&mut self) -> Result<(), CaptureError> {
        let (busy, _, _): (i64, i64, i64) = self
            .internal(|conn| {
                conn.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |r| {
                    Ok((r.get(0)?, r.get(1)?, r.get(2)?))
                })
            })
            .map_err(|e| CaptureError::Sql(e.to_string()))?;
        if busy != 0 {
            return Err(CaptureError::Sql("checkpoint blocked".into()));
        }
        Ok(())
    }

    /// Whether a transaction is open.
    pub fn in_transaction(&self) -> bool {
        !self.conn.is_autocommit()
    }

    /// The database file.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Runs one statement that returns no rows; returns rows changed.
    pub fn exec(&mut self, sql: &str, params: &[SqlValue]) -> Result<u64, SqlError> {
        // After a transaction an error rolled back, the guest's own
        // `ROLLBACK` (from an error handler, say) is what already happened:
        // a no-op, so the handler keeps the original error to report.
        if self.transaction_aborted && self.is_rollback(sql) {
            return Ok(0);
        }
        let wal = self.wal_limit();
        self.guest(|conn| {
            let stmt = conn.prepare(sql)?;
            let mut stmt = refuse_writes_if(|| wal_over(wal.as_ref()), stmt)?;
            let changed = stmt.execute(bind(params))?;
            Ok(u64::try_from(changed).unwrap_or(u64::MAX))
        })
    }

    /// Whether `sql` is a `ROLLBACK` of the whole transaction (prepared,
    /// not run).
    fn is_rollback(&self, sql: &str) -> bool {
        self.rollback_seen.store(false, Ordering::Relaxed);
        self.conn.prepare(sql).is_ok() && self.rollback_seen.load(Ordering::Relaxed)
    }

    /// The WAL file and its cap, while capturing (otherwise SQLite
    /// checkpoints on its own).
    fn wal_limit(&self) -> Option<(Arc<Path>, u64)> {
        self.capture
            .as_ref()
            .map(|c| (c.wal_path(), self.max_wal_bytes))
    }

    /// Runs a guest statement under its deadline, and names an interrupt
    /// for what it is.
    fn guest<T>(
        &mut self,
        f: impl FnOnce(&Connection) -> Result<T, SqlError>,
    ) -> Result<T, SqlError> {
        if self.transaction_aborted {
            return Err(SqlError::new(
                crate::SqlErrorKind::NotAllowed,
                "this event's transaction was rolled back when one of its statements failed \
                 (stopped at its deadline or CPU budget, say); nothing more runs in this event",
            ));
        }
        let in_transaction = !self.conn.is_autocommit();
        let cpu_left = self.max_sql_cpu.saturating_sub(self.sql_cpu_used);
        if cpu_left.is_zero() {
            return Err(self.sql_cpu_spent());
        }
        let limit = u64::try_from(self.max_statement_time.as_millis()).unwrap_or(u64::MAX);
        self.deadline
            .store(clock_ms().saturating_add(limit).max(1), Ordering::Relaxed);
        let cpu_start = thread_cpu_ns();
        let cpu_left = u64::try_from(cpu_left.as_nanos()).unwrap_or(u64::MAX);
        self.cpu_deadline
            .store(cpu_start.saturating_add(cpu_left).max(1), Ordering::Relaxed);
        let result = f(&self.conn);
        self.cpu_deadline.store(0, Ordering::Relaxed);
        self.deadline.store(0, Ordering::Relaxed);
        // SQLite rolls an explicit transaction back on some errors (an
        // interrupt, a full disk): the connection is back in autocommit
        // with the transaction gone, while the guest believes it open.
        if result.is_err() && in_transaction && self.conn.is_autocommit() {
            self.transaction_aborted = true;
        }
        self.sql_cpu_used += Duration::from_nanos(thread_cpu_ns().saturating_sub(cpu_start));
        result.map_err(|e| {
            if e.message.contains("interrupted") && self.sql_cpu_used >= self.max_sql_cpu {
                self.sql_cpu_spent()
            } else if e.message.contains("interrupted") {
                SqlError::new(
                    crate::SqlErrorKind::QuotaExceeded,
                    format!(
                        "statement ran longer than {} s and was stopped",
                        self.max_statement_time.as_secs()
                    ),
                )
            } else {
                e
            }
        })
    }

    fn sql_cpu_spent(&self) -> SqlError {
        SqlError::new(
            crate::SqlErrorKind::QuotaExceeded,
            format!(
                "this event's SQL used its {} ms of CPU; do less per event \
                 (add an index, or split the work across events)",
                self.max_sql_cpu.as_millis()
            ),
        )
    }

    /// Like [`exec`](Self::exec), but the commit is fsynced before
    /// returning (`synchronous = FULL` for this statement).
    ///
    /// Inside a transaction, only a statement that ends it (`COMMIT`) is
    /// accepted, and the whole transaction is fsynced; anything else is
    /// refused with `not-allowed` before it runs, since nothing in a
    /// transaction is durable before its commit.
    pub fn exec_durable(&mut self, sql: &str, params: &[SqlValue]) -> Result<u64, SqlError> {
        self.commit_point(sql, params, true)
    }

    /// [`exec_durable`](Self::exec_durable) without the local fsync, for a
    /// caller that makes the commit durable itself before it acknowledges
    /// it: a cluster node, whose replica uploads the commit to the bucket
    /// and trusts a local file again only after checking it against the
    /// bucket's head. The rules about transactions are the same; the commit
    /// is written as any `exec` writes it (`synchronous = NORMAL`).
    pub fn exec_commit(&mut self, sql: &str, params: &[SqlValue]) -> Result<u64, SqlError> {
        self.commit_point(sql, params, false)
    }

    /// One statement that must leave no transaction open: alone, or the
    /// `COMMIT` of the open one. With `sync`, fsynced before returning.
    fn commit_point(
        &mut self,
        sql: &str,
        params: &[SqlValue],
        sync: bool,
    ) -> Result<u64, SqlError> {
        if self.in_transaction() {
            return self.commit_durable(sql, params, sync);
        }
        // Refused before anything runs: a statement that opens or controls
        // a transaction makes nothing durable, and `synchronous` could not
        // be restored inside the transaction it opened.
        let tx_seen = Arc::clone(&self.tx_seen);
        let controls = self.guest(|conn| {
            tx_seen.store(false, Ordering::Relaxed);
            conn.prepare(sql)?;
            Ok(tx_seen.load(Ordering::Relaxed))
        })?;
        if controls {
            return Err(SqlError::new(
                crate::SqlErrorKind::NotAllowed,
                "exec-durable does not open or control a transaction: run BEGIN (or SAVEPOINT) \
                 and the transaction's statements with exec, then exec-durable(\"COMMIT\")",
            ));
        }
        if !sync {
            return self.exec(sql, params);
        }
        #[cfg(feature = "fault-injection")]
        crate::faults::note_local_sync();
        self.internal_pragma("synchronous", "FULL")?;
        self.sync_full = true;
        let result = self.exec(sql, params);
        let restore = self.internal_pragma("synchronous", "NORMAL");
        if restore.is_ok() {
            self.sync_full = false;
        }
        let changed = result?;
        restore?;
        Ok(changed)
    }

    /// `exec-durable` with a transaction open. SQLite refuses to change
    /// `synchronous` inside a transaction, so the statement runs as is and,
    /// once it has committed, with `sync` the WAL is fsynced (what
    /// `synchronous = FULL` does at commit). No checkpoint: while capturing, the WAL must keep
    /// its frames until [`take_changes`](Self::take_changes).
    fn commit_durable(
        &mut self,
        sql: &str,
        params: &[SqlValue],
        sync: bool,
    ) -> Result<u64, SqlError> {
        let refused = || {
            SqlError::new(
                crate::SqlErrorKind::NotAllowed,
                "inside a transaction, exec-durable only runs the COMMIT: run the \
                 transaction's statements with exec, then exec-durable(\"COMMIT\")",
            )
        };
        let commit_seen = Arc::clone(&self.commit_seen);
        let ended = self.guest(|conn| {
            commit_seen.store(false, Ordering::Relaxed);
            let mut stmt = conn.prepare(sql)?;
            // Decided before it runs: any other statement would land in the
            // open transaction, not durably.
            if !commit_seen.load(Ordering::Relaxed) {
                return Ok(None);
            }
            let changed = stmt.execute(bind(params))?;
            Ok(Some((changed, conn.is_autocommit())))
        })?;
        let Some((changed, true)) = ended else {
            return Err(refused());
        };
        if sync {
            #[cfg(feature = "fault-injection")]
            crate::faults::note_local_sync();
            self.sync_wal()?;
        }
        Ok(u64::try_from(changed).unwrap_or(u64::MAX))
    }

    /// Fsyncs the WAL file, so every commit written to it is on disk.
    fn sync_wal(&self) -> Result<(), SqlError> {
        let mut wal = self.path.clone().into_os_string();
        wal.push("-wal");
        let synced = std::fs::File::open(&wal).and_then(|f| f.sync_all());
        match synced {
            // No WAL: nothing written since the last checkpoint, which
            // synced the main file.
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(SqlError::new(
                crate::SqlErrorKind::Internal,
                format!("committed but not fsynced: {e}"),
            )),
            Ok(()) => Ok(()),
        }
    }

    /// Runs one statement and returns its rows.
    pub fn query(&mut self, sql: &str, params: &[SqlValue]) -> Result<Rows, SqlError> {
        let max = self.max_result_bytes;
        let wal = self.wal_limit();
        self.guest(|conn| {
            let stmt = conn.prepare(sql)?;
            let mut stmt = refuse_writes_if(|| wal_over(wal.as_ref()), stmt)?;
            let columns: Vec<String> = stmt.column_names().map(String::from).collect();
            let width = columns.len();
            let mut rows = stmt.query(bind(params))?;
            let mut values = Vec::new();
            // Counted as rows are read, so a huge result is refused before
            // it is held in memory.
            let mut size = 0usize;
            while let Some(row) = rows.next()? {
                let mut out = Vec::with_capacity(width);
                for i in 0..width {
                    let value = SqlValue::from(row.get_ref(i)?);
                    size = size.saturating_add(match &value {
                        SqlValue::Text(t) => t.len(),
                        SqlValue::Blob(b) => b.len(),
                        _ => 8,
                    });
                    if size > max {
                        return Err(SqlError::new(
                            crate::SqlErrorKind::QuotaExceeded,
                            format!(
                                "result larger than {max} bytes; select less or page through it"
                            ),
                        ));
                    }
                    out.push(value);
                }
                values.push(out);
            }
            Ok(Rows { columns, values })
        })
    }

    /// Ends an event: rolls back a transaction the guest left open.
    /// Returns whether a rollback happened.
    pub fn end_event(&mut self) -> Result<bool, SqlError> {
        // The next event gets the full SQL CPU budget, and statements again.
        self.sql_cpu_used = Duration::ZERO;
        self.transaction_aborted = false;
        let rolled_back = if self.conn.is_autocommit() {
            false
        } else {
            self.internal(|conn| conn.execute_batch("ROLLBACK"))?;
            true
        };
        // A safety net: never leave an event's FULL behind.
        if self.sync_full {
            self.internal_pragma("synchronous", "NORMAL")?;
            self.sync_full = false;
        }
        Ok(rolled_back)
    }

    /// A host metadata value (e.g. the component version `on-init` last ran
    /// for). Stored in `_wasmkeep_meta`, invisible to guests.
    pub fn meta(&mut self, key: &str) -> Result<Option<String>, SqlError> {
        #[cfg(feature = "fault-injection")]
        if crate::faults::take(&format!("meta:{key}")) {
            return Err(SqlError::new(
                crate::SqlErrorKind::Internal,
                "disk I/O error (injected)",
            ));
        }
        self.internal(|conn| {
            conn.query_row(
                "SELECT value FROM main._wasmkeep_meta WHERE key = ?1",
                [key],
                |r| r.get(0),
            )
            .optional()
        })
    }

    /// Sets a host metadata value.
    pub fn set_meta(&mut self, key: &str, value: &str) -> Result<(), SqlError> {
        #[cfg(feature = "fault-injection")]
        if crate::faults::take(&format!("set_meta:{key}")) {
            return Err(SqlError::new(
                crate::SqlErrorKind::Internal,
                "disk I/O error (injected)",
            ));
        }
        self.internal(|conn| {
            conn.execute(
                "INSERT INTO main._wasmkeep_meta (key, value) VALUES (?1, ?2)
                 ON CONFLICT (key) DO UPDATE SET value = excluded.value",
                [key, value],
            )
            .map(drop)
        })
    }

    /// The pending alarm (ms since the Unix epoch), if any.
    ///
    /// Alarm writes go through the same connection as the guest's
    /// statements, so inside a guest transaction they commit or roll back
    /// with it.
    pub fn alarm(&mut self) -> Result<Option<u64>, SqlError> {
        let at: Option<i64> = self.internal(|conn| {
            conn.query_row(
                "SELECT at FROM main._wasmkeep_alarm WHERE id = 1",
                [],
                |r| r.get(0),
            )
            .optional()
        })?;
        Ok(at.map(|v| u64::try_from(v).unwrap_or(0)))
    }

    /// Replaces the pending alarm.
    pub fn set_alarm(&mut self, at: u64) -> Result<(), SqlError> {
        let at = i64::try_from(at).unwrap_or(i64::MAX);
        self.internal(|conn| {
            conn.execute(
                "INSERT INTO main._wasmkeep_alarm (id, at) VALUES (1, ?1)
                 ON CONFLICT (id) DO UPDATE SET at = excluded.at",
                [at],
            )
            .map(drop)
        })
    }

    /// Cancels the pending alarm.
    pub fn clear_alarm(&mut self) -> Result<(), SqlError> {
        self.internal(|conn| {
            conn.execute("DELETE FROM main._wasmkeep_alarm", [])
                .map(drop)
        })
    }

    fn internal_pragma(&mut self, name: &str, value: &str) -> Result<(), SqlError> {
        self.internal(|conn| conn.pragma_update(None, name, value))
    }

    fn internal<T>(
        &mut self,
        f: impl FnOnce(&Connection) -> rusqlite::Result<T>,
    ) -> Result<T, SqlError> {
        self.internal.store(true, Ordering::Relaxed);
        let result = f(&self.conn);
        self.internal.store(false, Ordering::Relaxed);
        result.map_err(SqlError::from)
    }
}

fn bind(params: &[SqlValue]) -> rusqlite::ParamsFromIter<Vec<rusqlite::types::Value>> {
    rusqlite::params_from_iter(
        params
            .iter()
            .map(rusqlite::types::Value::from)
            .collect::<Vec<_>>(),
    )
}

/// Tables reserved for the host. Guests can neither read nor write them.
const RESERVED_PREFIX: &str = "_wasmkeep_";

/// SQLite virtual machine steps between deadline checks.
const PROGRESS_STEPS: std::ffi::c_int = 10_000;

/// Virtual table modules guests may use; others (and `fts3_tokenizer`,
/// which takes a raw function pointer) could reach into the host.
const ALLOWED_MODULES: &[&str] = &["fts3", "fts4", "fts5", "rtree", "rtree_i32"];

/// Functions guests may not call.
const DENIED_FUNCTIONS: &[&str] = &["load_extension", "fts3_tokenizer"];

/// Milliseconds on a monotonic clock, for statement deadlines.
/// CPU time the calling thread has used, in ns from an arbitrary origin.
/// Only readings on one thread are compared.
#[cfg(any(target_os = "linux", target_os = "macos"))]
fn thread_cpu_ns() -> u64 {
    let t = rustix::time::clock_gettime(rustix::time::ClockId::ThreadCPUTime);
    u64::try_from(t.tv_sec)
        .unwrap_or(0)
        .saturating_mul(1_000_000_000)
        .saturating_add(u64::try_from(t.tv_nsec).unwrap_or(0))
}

/// Elsewhere, wall time (a preempted statement is charged its wait too).
#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn thread_cpu_ns() -> u64 {
    static START: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();
    u64::try_from(START.get_or_init(Instant::now).elapsed().as_nanos()).unwrap_or(u64::MAX)
}

fn clock_ms() -> u64 {
    static START: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();
    let start = START.get_or_init(Instant::now);
    u64::try_from(start.elapsed().as_millis())
        .unwrap_or(u64::MAX)
        .saturating_add(1)
}

fn table_name<'a>(action: &AuthAction<'a>) -> Option<&'a str> {
    use AuthAction as A;
    match *action {
        A::CreateIndex { table_name, .. }
        | A::CreateTable { table_name }
        | A::CreateTempIndex { table_name, .. }
        | A::CreateTempTable { table_name }
        | A::CreateTempTrigger { table_name, .. }
        | A::CreateTrigger { table_name, .. }
        | A::Delete { table_name }
        | A::DropIndex { table_name, .. }
        | A::DropTable { table_name }
        | A::DropTempIndex { table_name, .. }
        | A::DropTempTable { table_name }
        | A::DropTempTrigger { table_name, .. }
        | A::DropTrigger { table_name, .. }
        | A::Insert { table_name }
        | A::Read { table_name, .. }
        | A::Update { table_name, .. }
        | A::AlterTable { table_name, .. }
        | A::Analyze { table_name }
        | A::CreateVtable { table_name, .. }
        | A::DropVtable { table_name, .. } => Some(table_name),
        _ => None,
    }
}

fn authorize(ctx: &AuthContext<'_>) -> Authorization {
    if table_name(&ctx.action).is_some_and(|t| {
        t.get(..RESERVED_PREFIX.len())
            .is_some_and(|p| p.eq_ignore_ascii_case(RESERVED_PREFIX))
    }) {
        return Authorization::Deny;
    }
    match ctx.action {
        AuthAction::Attach { .. } | AuthAction::Detach { .. } => Authorization::Deny,
        // No temp schema: temp objects are not under the size cap, and a
        // temp table renamed onto a host table's name would shadow it.
        AuthAction::CreateTempTable { .. }
        | AuthAction::CreateTempIndex { .. }
        | AuthAction::CreateTempTrigger { .. }
        | AuthAction::CreateTempView { .. } => Authorization::Deny,
        AuthAction::CreateVtable { module_name, .. }
        | AuthAction::DropVtable { module_name, .. } => {
            if ALLOWED_MODULES
                .iter()
                .any(|m| m.eq_ignore_ascii_case(module_name))
            {
                Authorization::Allow
            } else {
                Authorization::Deny
            }
        }
        // Read only: rtree asks for it when it creates its tables.
        AuthAction::Pragma {
            pragma_name,
            pragma_value: None,
        } if pragma_name.eq_ignore_ascii_case("page_size") => Authorization::Allow,
        AuthAction::Pragma { pragma_name, .. } => {
            if ALLOWED_PRAGMAS.contains(&pragma_name.to_ascii_lowercase().as_str()) {
                Authorization::Allow
            } else {
                Authorization::Deny
            }
        }
        AuthAction::Function { function_name } => {
            if DENIED_FUNCTIONS
                .iter()
                .any(|f| f.eq_ignore_ascii_case(function_name))
            {
                Authorization::Deny
            } else {
                Authorization::Allow
            }
        }
        _ => Authorization::Allow,
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)] // test code

    use super::*;

    /// A refused `exec-durable("BEGIN")` leaves the connection at
    /// `synchronous = NORMAL` (1; FULL is 2), as every statement outside
    /// `exec-durable` expects.
    #[test]
    fn a_refused_durable_begin_leaves_synchronous_normal() {
        let dir = tempfile::tempdir().unwrap();
        let mut db = crate::Storage::new(crate::StorageConfig::new(dir.path()))
            .open("t", "c", "a")
            .unwrap();
        // Read as the host does (the guest's sandbox refuses pragmas).
        let level = |db: &mut ActorDb| -> i64 {
            db.internal(|conn| conn.pragma_query_value(None, "synchronous", |r| r.get(0)))
                .unwrap()
        };
        assert_eq!(level(&mut db), 1);
        assert!(db.exec_durable("BEGIN", &[]).is_err());
        assert!(!db.in_transaction());
        assert_eq!(level(&mut db), 1, "left at FULL");
    }
}
