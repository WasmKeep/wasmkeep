use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use crate::wal::{BoundaryChanges, BoundaryPlan, CaptureError, Changes, WalBoundary, WalCapture};
use rusqlite::hooks::{AuthAction, AuthContext, Authorization, TransactionOperation};
use rusqlite::{Connection, OpenFlags, OptionalExtension};

use crate::path::encode_segment;
use crate::{Rows, SqlError, SqlValue, StorageError};

// Conservative retained-result accounting, not an allocator/RSS measurement.
// Allow two machine words for allocation metadata/alignment per allocation.
const RESULT_ALLOCATION_OVERHEAD: usize = 2 * size_of::<usize>();
// Rows owns the two outer vectors, each with a possible heap allocation.
const RESULT_BASE_OVERHEAD: usize = size_of::<Rows>() + 2 * RESULT_ALLOCATION_OVERHEAD;
// The row index grows explicitly from one slot by doubling (see query), so
// charge two Vec headers per row to cover spare capacity, plus its allocation.
const RESULT_ROW_OVERHEAD: usize = 2 * size_of::<Vec<SqlValue>>() + RESULT_ALLOCATION_OVERHEAD;
// Row/column vectors reserve their exact width. SqlValue already includes the
// discriminant and inline scalar or String/Vec header; payloads are additional.
const RESULT_CELL_OVERHEAD: usize = size_of::<SqlValue>() + RESULT_ALLOCATION_OVERHEAD;
const RESULT_COLUMN_OVERHEAD: usize = size_of::<String>() + RESULT_ALLOCATION_OVERHEAD;

/// Storage settings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageConfig {
    /// Root data directory; databases live under it, where the server's
    /// [`StateDir`] says (`<data_dir>/state/<tenant>/<app>/actors/`).
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
    /// Largest result a `query` may return, all rows together (default: 16 MiB).
    /// Counts text/blob payloads, column names once, and conservative overhead
    /// for the result, rows, cells, allocations and vector spare capacity.
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

/// Where one root's actor databases live under the data directory, and
/// the tenant every one records as part of its address. Given by the
/// server, which knows the layout.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateDir {
    /// The folder of the databases, each at `<class>/<name>.db` in it,
    /// relative to the data directory.
    pub dir: PathBuf,
    /// Scratch space, relative to the data directory.
    pub scratch: PathBuf,
    /// The tenant of every actor in it.
    pub tenant: String,
}

/// Opens and deletes actor databases.
#[derive(Debug, Clone)]
pub struct Storage {
    config: StorageConfig,
    /// Where this root's databases live.
    state: StateDir,
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

    /// Opens databases in `state.dir` under `config.data_dir` (created on
    /// first open).
    pub fn new(config: StorageConfig, state: StateDir) -> Self {
        Self {
            config,
            state,
            open: Arc::default(),
        }
    }

    /// Where this root's databases live.
    #[must_use]
    pub fn state(&self) -> &StateDir {
        &self.state
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

    /// Scratch space under the data directory.
    #[must_use]
    pub fn scratch_dir(&self) -> PathBuf {
        self.config.data_dir.join(&self.state.scratch)
    }

    /// The database file for an actor.
    pub fn db_path(&self, class: &str, name: &str) -> PathBuf {
        self.config
            .data_dir
            .join(&self.state.dir)
            .join(encode_segment(class))
            .join(format!("{}.db", encode_segment(name)))
    }

    /// Opens (creating if needed) an actor's database.
    pub fn open(&self, class: &str, name: &str) -> Result<ActorDb, StorageError> {
        #[cfg(feature = "fault-injection")]
        crate::faults::pause("open");
        let path = self.db_path(class, name);
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir).map_err(|source| StorageError::Io {
                path: dir.to_owned(),
                source,
            })?;
        }
        let mut db = open_retrying(&path, &self.config)?;
        match db.find_reserved() {
            Ok(found) => db.reserved = found,
            // Not a reason to keep the actor closed: the sandbox refuses
            // what it would refuse anyway.
            Err(e) => tracing::warn!(
                path = %path.display(),
                error = %e,
                "could not look for guest objects with the reserved _wasmkeep_ prefix"
            ),
        }
        warn_reserved(&path, &db.reserved);
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
        let address = format!("{}/{class}/{name}", self.state.tenant);
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

    /// Every actor with a pending alarm. Opens each database of this root
    /// ([`StateDir::dir`]); run at startup. A database that cannot be read is
    /// reported in [`PendingAlarms::skipped`], and the scan goes on: one
    /// broken file must not cost every other actor its alarm.
    pub fn pending_alarms(&self) -> Result<PendingAlarms, StorageError> {
        let mut found = Vec::new();
        let mut skipped = Vec::new();
        let root = self.config.data_dir.join(&self.state.dir);
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
    pub fn delete(&self, class: &str, name: &str) -> Result<(), StorageError> {
        self.remove_files(class, name, false)
    }

    /// Like [`Self::delete`], and durable: the directory is fsynced once
    /// the files are gone (`delete-self-durable` on a single node, ADR
    /// 0028).
    pub fn delete_durably(&self, class: &str, name: &str) -> Result<(), StorageError> {
        self.remove_files(class, name, true)
    }

    fn remove_files(&self, class: &str, name: &str, sync: bool) -> Result<(), StorageError> {
        let path = self.db_path(class, name);
        #[cfg(feature = "fault-injection")]
        if crate::faults::take("delete") {
            return Err(StorageError::Io {
                path,
                source: std::io::Error::other("injected failure"),
            });
        }
        // The WAL first: a crash part way must never leave a WAL without
        // its database, which SQLite would replay into a new one.
        for suffix in ["-wal", "-shm", ""] {
            let file = PathBuf::from(format!("{}{suffix}", path.display()));
            match std::fs::remove_file(&file) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(source) => return Err(StorageError::Io { path: file, source }),
            }
        }
        if sync && let Some(dir) = path.parent() {
            match std::fs::File::open(dir).and_then(|d| d.sync_all()) {
                Ok(()) => {}
                // No directory: nothing was there to remove.
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(source) => {
                    return Err(StorageError::Io {
                        path: dir.to_owned(),
                        source,
                    });
                }
            }
        }
        Ok(())
    }
}

/// Meta key holding `tenant/class/name`.
const ADDRESS_KEY: &str = "address";

/// Logs, once per database file and process, the guest objects with the
/// reserved prefix it holds: at most a few, each name shortened and
/// escaped (a guest chose them).
fn warn_reserved(path: &Path, found: &[ReservedObject]) {
    use std::collections::HashSet;
    use std::sync::{Mutex, OnceLock};
    /// Files warned about; past this many, no more warnings.
    const MAX_WARNED: usize = 10_000;
    static WARNED: OnceLock<Mutex<HashSet<PathBuf>>> = OnceLock::new();
    if found.is_empty() {
        return;
    }
    {
        let mut warned = WARNED
            .get_or_init(Default::default)
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        if warned.len() >= MAX_WARNED || !warned.insert(path.to_owned()) {
            return;
        }
    }
    let names: Vec<String> = found
        .iter()
        .take(8)
        .map(|o| {
            let name: String = o.name.chars().take(64).collect();
            let how = match o.uses {
                ReservedUse::Named => "named",
                ReservedUse::References => "naming a host table",
            };
            format!("{} {name:?} ({how})", o.kind)
        })
        .collect();
    tracing::warn!(
        path = %path.display(),
        count = found.len(),
        objects = %names.join(", "),
        "the database holds guest objects that use the reserved _wasmkeep_ prefix (made \
         before 0.8): kept as they are; the guest can change none of them, nor read a table \
         so named"
    );
}

/// `*.db` files at `<root>/<class>/`.
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
    for class in read(root)?.into_iter().filter(|p| p.is_dir()) {
        files.extend(
            read(&class)?
                .into_iter()
                .filter(|p| p.extension().is_some_and(|e| e == "db")),
        );
    }
    files.sort();
    Ok(files)
}

/// Lifetime token owned by the server's batched hold. Keep it alive until a
/// `Flushed` or `GaveUp` barrier, including while a captured segment is in
/// flight. Dropping it permits connection teardown, not reuse of lost changes.
/// Storage cannot determine acknowledgement; the server owns that decision.
/// In a containing struct, declare `ActorDb` BEFORE `CaptureHold`: Rust drops
/// fields in declaration order, so the database checks the still-live hold.
/// On give-up, explicitly drop the hold first; on success call
/// `finish_batched_capture` after the acknowledgement barrier.
#[derive(Debug, Default)]
pub struct CaptureHold(Arc<()>);

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
    /// Set by the authorizer when a guest statement it is preparing changes
    /// the schema (creates or alters something).
    ddl_seen: Arc<AtomicBool>,
    /// While a guest statement that writes runs during capture, the WAL's
    /// cap, checked by the progress handler (0: none).
    wal_cap: Arc<AtomicU64>,
    /// Set by the progress handler when it stopped a statement for the WAL.
    wal_stopped: Arc<AtomicBool>,
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
    /// A checkpoint was blocked (a reader still on the WAL) after moving
    /// every frame, all of them captured, into the main file. SQLite then
    /// restarts the WAL at the next write once the reader leaves, which
    /// loses nothing: the next capture starts over on the new WAL.
    wal_restart_ok: bool,
    /// The last checkpoint was blocked by a reader: later ones try without
    /// waiting out the busy timeout, until one succeeds.
    checkpoint_blocked: bool,
    /// Checkpoints after a capture that failed for another reason than a
    /// reader (an I/O error, say): the changes were returned anyway.
    failed_checkpoints: u64,
    batch_hold: Option<std::sync::Weak<()>>,
    segment_byte_cap: u64,
    /// Guest objects found at open that use the reserved prefix.
    reserved: Vec<ReservedObject>,
    /// Last, so it is released after the connection has closed.
    open_guard: Option<OpenGuard>,
}

impl Drop for ActorDb {
    fn drop(&mut self) {
        // Do not double-panic during unwinding. NO_CKPT_ON_CLOSE still keeps
        // the WAL in place; a failed hold must be restored from its head.
        if !std::thread::panicking() {
            debug_assert!(
                !self
                    .batch_hold
                    .as_ref()
                    .is_some_and(|h| h.strong_count() != 0)
                    || self
                        .capture
                        .as_ref()
                        .is_none_or(|c| matches!(c.has_uncaptured_commits(), Ok(false))),
                "ActorDb dropped with uncaptured commits while its hold lives"
            );
        }
    }
}

/// Refuses a statement that may write while the WAL is full. Transaction
/// control counts as read-only, so a guest can still commit or roll back.
fn refuse_writes_if(
    wal_full: impl FnOnce() -> bool,
    why: WalFull,
    stmt: rusqlite::Statement<'_>,
) -> Result<rusqlite::Statement<'_>, SqlError> {
    if !stmt.readonly() && wal_full() {
        return Err(wal_full_error(why));
    }
    Ok(stmt)
}

/// Why the write-ahead log would be full.
#[derive(Debug, Clone, Copy)]
enum WalFull {
    /// Batched capture has not drained it yet.
    Batched,
    /// Another connection reads the database, so it cannot be emptied.
    CheckpointBlocked,
    /// This event wrote too much.
    Writes,
}

fn wal_full_error(why: WalFull) -> SqlError {
    SqlError::new(
        crate::SqlErrorKind::QuotaExceeded,
        match why {
            WalFull::Batched => {
                "the write-ahead log is full; hold new events until batched capture drains and checkpoints"
            }
            WalFull::CheckpointBlocked => {
                "the write-ahead log is full: another connection is reading the database, \
                 so the log cannot be emptied; writes resume once it stops"
            }
            WalFull::Writes => {
                "too many writes in one event: the write-ahead log is full until the \
                 event ends; split the work across events"
            }
        },
    )
}

/// Prepares a guest statement. Schema changes that name the reserved prefix
/// anywhere (a foreign key to a host table, a rename onto a reserved name)
/// are refused: the authorizer sees neither foreign key parents nor rename
/// targets, so the statement's text is checked.
fn prepare_guest<'c>(
    conn: &'c Connection,
    sql: &str,
    ddl_seen: &AtomicBool,
) -> Result<rusqlite::Statement<'c>, SqlError> {
    ddl_seen.store(false, Ordering::Relaxed);
    let stmt = conn.prepare(sql)?;
    if ddl_seen.load(Ordering::Relaxed) && names_reserved(sql) {
        return Err(SqlError::new(
            crate::SqlErrorKind::NotAllowed,
            "names starting with _wasmkeep_ are reserved for the host; a schema change \
             may not name one",
        ));
    }
    Ok(stmt)
}

/// Whether an identifier in `sql` starts with the reserved prefix (in any
/// case; quoted or not). A name that only contains it is the guest's.
fn names_reserved(sql: &str) -> bool {
    let bytes = sql.as_bytes();
    let prefix = RESERVED_PREFIX.as_bytes();
    // ASCII identifier characters only. SQLite also counts every byte of a
    // non-ASCII character as one, but reads a byte-order mark as white
    // space: any non-ASCII byte is a boundary here, which fails closed
    // (`é_wasmkeep_x` is refused too).
    let ident = |b: u8| b.is_ascii_alphanumeric() || b == b'_' || b == b'$';
    bytes
        .windows(prefix.len())
        .enumerate()
        .any(|(i, w)| w.eq_ignore_ascii_case(prefix) && (i == 0 || !ident(bytes[i - 1])))
}

fn is_reserved(name: &str) -> bool {
    name.get(..RESERVED_PREFIX.len())
        .is_some_and(|p| p.eq_ignore_ascii_case(RESERVED_PREFIX))
}

/// Has the progress handler watch the WAL while `stmt` runs, if it may
/// write and the WAL is capped; [`ActorDb::guest`] clears it afterwards.
fn watch_wal(stmt: &rusqlite::Statement<'_>, limit: Option<&(Arc<Path>, u64)>, cap: &AtomicU64) {
    if let Some((_, max)) = limit
        && !stmt.readonly()
    {
        cap.store((*max).max(1), Ordering::Relaxed);
    }
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
        {
            use rusqlite::limits::Limit as L;
            for (limit, value) in [
                (L::SQLITE_LIMIT_LENGTH, value_limit),
                (L::SQLITE_LIMIT_COLUMN, GUEST_MAX_COLUMNS),
                (L::SQLITE_LIMIT_FUNCTION_ARG, GUEST_MAX_FUNCTION_ARGS),
                (L::SQLITE_LIMIT_EXPR_DEPTH, GUEST_MAX_EXPR_DEPTH),
                (L::SQLITE_LIMIT_COMPOUND_SELECT, GUEST_MAX_COMPOUND_SELECT),
                (L::SQLITE_LIMIT_VDBE_OP, GUEST_MAX_VDBE_OPS),
                (L::SQLITE_LIMIT_LIKE_PATTERN_LENGTH, GUEST_MAX_LIKE_PATTERN),
            ] {
                conn.set_limit(limit, value).map_err(open_err)?;
            }
        }
        // Sorter and materialisation spill stays in memory, under the heap
        // cap below, not in the system's temporary directory outside the
        // database's size cap.
        conn.pragma_update(None, "temp_store", "MEMORY")
            .map_err(open_err)?;
        // Process-wide, and it only ever lowers the cap: the lowest set wins.
        conn.pragma_update(None, "hard_heap_limit", SQLITE_HEAP_LIMIT_BYTES)
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
        let ddl_seen = Arc::new(AtomicBool::new(false));
        let ddl = Arc::clone(&ddl_seen);
        conn.authorizer(Some(move |ctx: AuthContext<'_>| {
            if flag.load(Ordering::Relaxed) {
                // Host statements run with triggers and foreign keys off
                // (`internal`): nothing of the guest's runs in one.
                return Authorization::Allow;
            }
            if matches!(
                ctx.action,
                AuthAction::CreateTable { .. }
                    | AuthAction::CreateIndex { .. }
                    | AuthAction::CreateTrigger { .. }
                    | AuthAction::CreateView { .. }
                    | AuthAction::CreateVtable { .. }
                    | AuthAction::AlterTable { .. }
            ) {
                ddl.store(true, Ordering::Relaxed);
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
        let budget = crate::guest_functions::Budget {
            deadline: Arc::clone(&deadline),
            cpu_deadline: Arc::clone(&cpu_deadline),
        };
        crate::guest_functions::register(
            &conn,
            &budget,
            config.max_value_bytes,
            GUEST_MAX_LIKE_PATTERN as usize,
        )
        .map_err(open_err)?;
        // While a writing guest statement runs during capture, the WAL is
        // checked too: one statement must not grow it far past its cap.
        let wal_cap = Arc::new(AtomicU64::new(0));
        let wal_stopped = Arc::new(AtomicBool::new(false));
        let cap = Arc::clone(&wal_cap);
        let stopped = Arc::clone(&wal_stopped);
        let mut wal_path = path.as_os_str().to_owned();
        wal_path.push("-wal");
        let wal_path = PathBuf::from(wal_path);
        conn.progress_handler(
            PROGRESS_STEPS,
            Some(move || {
                if budget.spent() {
                    return true;
                }
                let max = cap.load(Ordering::Relaxed);
                if max != 0 && std::fs::metadata(&wal_path).is_ok_and(|m| m.len() > max) {
                    stopped.store(true, Ordering::Relaxed);
                    return true;
                }
                false
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
            ddl_seen,
            wal_cap,
            wal_stopped,
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
            wal_restart_ok: false,
            checkpoint_blocked: false,
            failed_checkpoints: 0,
            batch_hold: None,
            segment_byte_cap: crate::wal::DEFAULT_SEGMENT_BYTES,
            reserved: Vec::new(),
            open_guard: None,
        })
    }

    /// Guest objects in the schema that use the reserved prefix: a guest
    /// could give one a reserved name before 0.8 (a view, an index, a
    /// trigger, or a table renamed onto one), or name a host table in its
    /// definition (a foreign key to one). Found once, at open.
    ///
    /// They are left as they are: dropping one could lose the guest's rows
    /// (a renamed table holds them) and renaming one changes the guest's
    /// schema behind its back, both replicated as ordinary changes, while
    /// refusing to open would take the actor down. The sandbox already
    /// keeps them out of the guest's reach: no schema change may name one
    /// (drop, alter, reindex, or a new object naming it), a reserved table
    /// or view can be neither read nor written, and host statements run
    /// none of its triggers or cascades. What still works for the guest is
    /// an index or a trigger so named on a table of its own: the index is
    /// used and the trigger fires. A table carrying such a trigger cannot
    /// be dropped; one carrying only such an index can, and the index goes
    /// with it.
    #[must_use]
    pub fn reserved_objects(&self) -> &[ReservedObject] {
        &self.reserved
    }

    /// Reads [`Self::reserved_objects`] from the schema.
    fn find_reserved(&mut self) -> Result<Vec<ReservedObject>, SqlError> {
        let rows = self.internal(|conn| {
            // Narrowed by SQLite, judged here as guest DDL is judged; no
            // LIKE, where `_` is a wildcard.
            let mut stmt = conn.prepare(
                "SELECT type, name, sql FROM main.sqlite_schema \
                 WHERE instr(lower(name), '_wasmkeep_') OR instr(lower(sql), '_wasmkeep_')",
            )?;
            stmt.query_map([], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, Option<String>>(2)?,
                ))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()
        })?;
        Ok(rows
            .into_iter()
            .filter(|(kind, name, _)| !(kind == "table" && HOST_TABLES.contains(&name.as_str())))
            .filter_map(|(kind, name, sql)| {
                let uses = if is_reserved(&name) {
                    ReservedUse::Named
                } else if sql.as_deref().is_some_and(names_reserved) {
                    ReservedUse::References
                } else {
                    return None;
                };
                Some(ReservedObject { kind, name, uses })
            })
            .collect())
    }

    /// Starts capturing committed changes for replication: turns automatic
    /// checkpoints off, so committed pages stay in the WAL until captured,
    /// and checkpoints once so capture starts from a complete main file.
    pub fn start_capture(&mut self) -> Result<(), CaptureError> {
        if self
            .batch_hold
            .as_ref()
            .is_some_and(|h| h.strong_count() != 0)
        {
            return Err(CaptureError::Sql(
                "batched capture restart requires a barrier".into(),
            ));
        }
        self.internal(|conn| conn.pragma_update(None, "wal_autocheckpoint", 0))
            .map_err(|e| CaptureError::Sql(e.to_string()))?;
        self.checkpoint()?;
        let page_size: i64 = self
            .internal(|conn| conn.pragma_query_value(None, "page_size", |r| r.get(0)))
            .map_err(|e| CaptureError::Sql(e.to_string()))?;
        let page_size = u32::try_from(page_size).unwrap_or(4096);
        self.capture = Some(WalCapture::start(&self.path, page_size)?);
        self.wal_restart_ok = false;
        if self.batch_hold.take().is_some() {
            self.conn
                .set_db_config(
                    rusqlite::config::DbConfig::SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE,
                    false,
                )
                .map_err(|e| CaptureError::Sql(e.to_string()))?;
        }
        Ok(())
    }

    /// Opt an existing capture into boundary mode. Establish the initial
    /// snapshot before this call. The server owns `hold` through its barrier;
    /// capture, encoding and uploader hand-off must be one uncancellable step.
    /// SQLite's last-connection checkpoint is disabled in this mode, including
    /// release builds and unwinding, so a mistaken close cannot consume WAL.
    pub fn enable_batched_capture(&mut self, hold: &CaptureHold) -> Result<(), CaptureError> {
        if self.capture.is_none() || self.batch_hold.is_some() {
            return Err(CaptureError::Sql(
                "batched capture needs an existing strict capture".into(),
            ));
        }
        self.conn
            .set_db_config(
                rusqlite::config::DbConfig::SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE,
                true,
            )
            .map_err(|e| CaptureError::Sql(e.to_string()))?;
        self.batch_hold = Some(Arc::downgrade(&hold.0));
        // Boundary capture does not keep `wal_restart_ok` up to date.
        self.wal_restart_ok = false;
        Ok(())
    }

    /// Whether batched capture is on, under a hold that still lives.
    pub fn batched_capture_enabled(&self) -> bool {
        self.batch_hold
            .as_ref()
            .is_some_and(|h| h.strong_count() != 0)
    }

    /// Whether the uncaptured part already fills a segment at this
    /// database's cap ([`Self::set_segment_byte_cap`]): ADR 0034's
    /// event-start rule 2, judged at the last boundary.
    pub fn next_segment_full(&self) -> bool {
        self.boundaries()
            .last()
            .is_some_and(|b| b.estimated_bytes >= self.segment_byte_cap)
    }

    /// Whether something was committed after the last boundary marked (or
    /// the captured position): what a drop cannot account for from the
    /// marks. `false` when not capturing.
    pub fn has_unmarked_commits(&self) -> Result<bool, CaptureError> {
        match &self.capture {
            Some(c) => c.has_unmarked_commits(),
            None => Ok(false),
        }
    }

    /// Configure the conservative encoded-byte cap (default 1 MiB). Even a
    /// zero cap makes progress: each writing event is then oversize alone.
    pub fn set_segment_byte_cap(&mut self, bytes: u64) {
        self.segment_byte_cap = bytes;
    }

    /// Mark the last commit at a mailbox gap or flush point, without capture.
    /// An open transaction is refused; the caller must finish/roll it back.
    pub fn mark_boundary(&mut self) -> Result<Option<WalBoundary>, CaptureError> {
        if self.in_transaction() {
            return Err(CaptureError::Sql("boundary inside a transaction".into()));
        }
        self.batched_capture()?.mark_boundary()
    }

    /// Pending boundary estimates, rebased after every capture. The final
    /// estimate supplies ADR 0034's event-start rule 2; the server also checks
    /// age, node budget and `wal_wants_checkpoint`. Use boundary sequences
    /// for alarm metadata: offsets restart after a checkpoint.
    pub fn boundaries(&self) -> impl Iterator<Item = WalBoundary> + '_ {
        self.capture.iter().flat_map(WalCapture::boundaries)
    }

    /// Exposed for the server's hold/barrier assertions.
    pub fn has_pending_boundaries(&self) -> bool {
        self.capture
            .as_ref()
            .is_some_and(WalCapture::has_pending_boundaries)
    }

    /// The next segment and byte reservation, without consuming it. Keep the
    /// mailbox serialized between reserving the budget and capture.
    pub fn next_bounded_capture(&self) -> Option<BoundaryPlan> {
        self.capture.as_ref()?.next_capture(self.segment_byte_cap)
    }

    /// Whether WAL pressure requires holding new events until fully drained
    /// and checkpointed. Rewrites can keep the page estimate small while the
    /// physical WAL grows. The threshold is at most 4 MiB.
    pub fn wal_wants_checkpoint(&self) -> bool {
        self.capture
            .as_ref()
            .is_some_and(|c| c.wal_len() >= CHECKPOINT_AFTER_BYTES.min(self.max_wal_bytes))
    }

    /// Capture exactly the reserved prefix. Later marks and cap changes do
    /// not enlarge it. A stale plan is refused without advancing capture.
    /// Once consumed, changes are returned even if checkpointing is blocked.
    pub fn take_bounded_changes(
        &mut self,
        plan: BoundaryPlan,
    ) -> Result<BoundaryChanges, CaptureError> {
        let changes = self.batched_capture()?.capture_plan(plan)?;
        // Checkpointing is maintenance, never a reason to lose a consumed
        // segment. A blocked checkpoint leaves pressure set for a later retry.
        if self.wal_wants_checkpoint() && self.conn.is_autocommit() {
            let _ = self.checkpoint_batched_capture();
        }
        Ok(changes)
    }

    /// Retry maintenance after draining, including when no boundary remains.
    pub fn checkpoint_batched_capture(&mut self) -> Result<(), CaptureError> {
        if self.batched_capture()?.has_uncaptured_commits()? {
            return Err(CaptureError::BoundariesPending);
        }
        self.checkpoint()?;
        if let Some(c) = self.capture.as_mut() {
            c.wal_truncated();
        }
        Ok(())
    }

    /// Complete a successful batched barrier after the caller has awaited
    /// acknowledgement. Refuses unmarked or marked commits, truncates the
    /// WAL and restores normal close behavior so the local file is reusable.
    pub fn finish_batched_capture(&mut self) -> Result<(), CaptureError> {
        self.checkpoint_batched_capture()?;
        self.conn
            .set_db_config(
                rusqlite::config::DbConfig::SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE,
                false,
            )
            .map_err(|e| CaptureError::Sql(e.to_string()))?;
        self.batch_hold = None;
        Ok(())
    }

    fn batched_capture(&mut self) -> Result<&mut WalCapture, CaptureError> {
        if !self
            .batch_hold
            .as_ref()
            .is_some_and(|h| h.strong_count() != 0)
        {
            return Err(CaptureError::Sql("no live batched capture hold".into()));
        }
        self.capture
            .as_mut()
            .ok_or(CaptureError::Sql("capture not started".into()))
    }

    fn check_batched_checkpoint(&self) -> Result<(), CaptureError> {
        if self.batch_hold.is_some()
            && let Some(capture) = &self.capture
            && capture.has_uncaptured_commits()?
        {
            return Err(CaptureError::BoundariesPending);
        }
        Ok(())
    }

    /// What was committed since the last call, for replication; `None` if
    /// nothing was (or capture is not started). Call between events, with no
    /// transaction open.
    pub fn take_changes(&mut self) -> Result<Option<Changes>, CaptureError> {
        if self.batch_hold.is_some() {
            return Err(CaptureError::BoundariesPending);
        }
        if self.capture.is_none() {
            return Ok(None);
        }
        let changes = self.capture_pending()?;
        let Some(capture) = self.capture.as_ref() else {
            return Ok(None);
        };
        // A checkpoint cannot run inside an open transaction; try later.
        let threshold = CHECKPOINT_AFTER_BYTES.min(self.max_wal_bytes);
        // The file, not just what was captured: a rolled-back transaction
        // that spilled leaves frames no capture consumes, and a WAL left
        // over the cap would refuse every later write.
        let size = capture.wal_bytes().max(capture.wal_len());
        // Checkpointing is maintenance: once consumed, the changes are
        // returned even if it fails (a reader still on the WAL, say). The
        // WAL stays over the threshold, so the next call tries again, without
        // waiting for the reader while it stays.
        if size >= threshold && self.conn.is_autocommit() {
            match self.try_checkpoint(!self.checkpoint_blocked) {
                Ok(true) => {
                    if let Some(c) = self.capture.as_mut() {
                        c.wal_truncated();
                    }
                }
                Ok(false) => {}
                Err(_) => self.failed_checkpoints += 1,
            }
        }
        Ok(changes)
    }

    /// Every page of the database, for a snapshot upload. Captures pending
    /// changes first; if there were any, returns them in
    /// [`CaptureError::Pending`] (ship them, then snapshot again).
    pub fn snapshot(&mut self) -> Result<Changes, CaptureError> {
        self.check_batched_checkpoint()?;
        if let Some(pending) = self.capture_pending()? {
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

    /// Every page of the database as of the captured position, for a
    /// batched hold's periodic snapshot (ADR 0034, section 7): commits not
    /// captured yet are left out, nothing is consumed and nothing is
    /// checkpointed. Only in batched mode, where the main file never holds
    /// an uncaptured commit ([`WalCapture::snapshot_at_captured`]).
    pub fn snapshot_at_captured(&self) -> Result<Changes, CaptureError> {
        if !self.batched_capture_enabled() {
            return Err(CaptureError::Sql("no live batched capture hold".into()));
        }
        self.capture
            .as_ref()
            .ok_or(CaptureError::Sql("capture not started".into()))?
            .snapshot_at_captured()
    }

    /// The database checksum as of the last capture, if capturing.
    pub fn captured_checksum(&self) -> Option<crate::ltx::DbChecksum> {
        self.capture.as_ref().map(WalCapture::checksum)
    }

    /// The database's size in bytes as of the last capture, if capturing:
    /// what a snapshot of it would hold, read without copying it.
    pub fn captured_bytes(&self) -> Option<u64> {
        self.capture
            .as_ref()
            .map(|c| u64::from(c.pages()).saturating_mul(u64::from(c.page_size())))
    }

    /// What was committed since the last capture (strict mode), across a
    /// WAL restart that `wal_restart_ok` allows.
    fn capture_pending(&mut self) -> Result<Option<Changes>, CaptureError> {
        let restart_ok = self.wal_restart_ok;
        let capture = self
            .capture
            .as_mut()
            .ok_or(CaptureError::Sql("capture not started".into()))?;
        let changes = match capture.capture() {
            // Exactly one restart (SQLite adds one to salt-1 at each). Any
            // other change is someone else's doing and stays an error.
            Err(CaptureError::WalRestarted {
                captured_salt,
                found_salt,
            }) if restart_ok && found_salt == captured_salt.wrapping_add(1) => {
                capture.wal_truncated();
                self.wal_restart_ok = false;
                capture.capture()?
            }
            other => other?,
        };
        // A commit after the blocked checkpoint went to the same WAL, which
        // SQLite cannot restart again before another checkpoint.
        if changes.is_some() {
            self.wal_restart_ok = false;
        }
        Ok(changes)
    }

    /// Moves everything into the main file and empties the WAL. Call with
    /// every commit captured.
    fn checkpoint(&mut self) -> Result<(), CaptureError> {
        if self.try_checkpoint(true)? {
            Ok(())
        } else {
            Err(CaptureError::Sql("checkpoint blocked".into()))
        }
    }

    /// [`Self::checkpoint`]: `false` when a reader blocked it. Without
    /// `wait`, a reader is not waited for (the busy timeout).
    fn try_checkpoint(&mut self, wait: bool) -> Result<bool, CaptureError> {
        self.check_batched_checkpoint()?;
        #[cfg(feature = "fault-injection")]
        if crate::faults::take("checkpoint") {
            return Err(CaptureError::Sql("disk I/O error (injected)".into()));
        }
        let (busy, log, moved): (i64, i64, i64) = self
            .internal(|conn| {
                if !wait {
                    conn.busy_timeout(Duration::ZERO)?;
                }
                let counts = conn.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |r| {
                    Ok((r.get(0)?, r.get(1)?, r.get(2)?))
                });
                if !wait {
                    conn.busy_timeout(BUSY_TIMEOUT)?;
                }
                counts
            })
            .map_err(|e| CaptureError::Sql(e.to_string()))?;
        if busy != 0 {
            self.checkpoint_blocked = true;
            // Every frame moved, and every one of them captured: SQLite may
            // restart the WAL at the next write, losing nothing. Strict
            // capture only: boundary capture holds events until a checkpoint
            // succeeds, and does not keep this flag up to date.
            let all_captured = match (&self.capture, u64::try_from(log)) {
                (Some(c), Ok(frames)) => {
                    c.wal_bytes()
                        == WAL_HEADER_BYTES
                            + frames * (WAL_FRAME_HEADER_BYTES + u64::from(c.page_size()))
                }
                _ => false,
            };
            self.wal_restart_ok = log == moved && all_captured && self.batch_hold.is_none();
            return Ok(false);
        }
        self.checkpoint_blocked = false;
        self.wal_restart_ok = false;
        Ok(true)
    }

    /// Whether capture would follow a WAL restart (after a blocked
    /// checkpoint that had moved every captured frame). For tests.
    #[doc(hidden)]
    pub fn wal_restart_expected(&self) -> bool {
        self.wal_restart_ok
    }

    /// Checkpoints after a capture that failed for another reason than a
    /// reader on the WAL; the captured changes were returned all the same.
    pub fn failed_checkpoints(&self) -> u64 {
        self.failed_checkpoints
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
        let why = self.wal_full_why();
        let ddl_seen = Arc::clone(&self.ddl_seen);
        let wal_cap = Arc::clone(&self.wal_cap);
        self.guest(|conn| {
            let stmt = prepare_guest(conn, sql, &ddl_seen)?;
            let mut stmt = refuse_writes_if(|| wal_over(wal.as_ref()), why, stmt)?;
            watch_wal(&stmt, wal.as_ref(), &wal_cap);
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

    /// Why the WAL would be full, for the error.
    fn wal_full_why(&self) -> WalFull {
        if self.batch_hold.is_some() {
            WalFull::Batched
        } else if self.checkpoint_blocked {
            WalFull::CheckpointBlocked
        } else {
            WalFull::Writes
        }
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
        self.wal_stopped.store(false, Ordering::Relaxed);
        let result = f(&self.conn);
        self.cpu_deadline.store(0, Ordering::Relaxed);
        self.deadline.store(0, Ordering::Relaxed);
        self.wal_cap.store(0, Ordering::Relaxed);
        // SQLite rolls an explicit transaction back on some errors (an
        // interrupt, a full disk): the connection is back in autocommit
        // with the transaction gone, while the guest believes it open.
        if result.is_err() && in_transaction && self.conn.is_autocommit() {
            self.transaction_aborted = true;
        }
        self.sql_cpu_used += Duration::from_nanos(thread_cpu_ns().saturating_sub(cpu_start));
        let wal_stopped = self.wal_stopped.swap(false, Ordering::Relaxed);
        result.map_err(|e| {
            if wal_stopped && e.message.contains("interrupted") {
                wal_full_error(self.wal_full_why())
            } else if e.message.contains("out of memory") {
                // Also what SQLite answers for a statement compiled to more
                // instructions than its cap.
                SqlError::new(
                    crate::SqlErrorKind::QuotaExceeded,
                    "the statement is too large, or needs more memory than SQLite may use \
                     on this node; work on less at once",
                )
            } else if e.message.contains("interrupted") && self.sql_cpu_used >= self.max_sql_cpu {
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

    /// `storage.flush` on a single node (ADR 0034, section 4): fsyncs every
    /// commit made so far, those of plain `exec` included, without running
    /// a statement. Refused with `not-allowed` inside an open transaction,
    /// changing nothing: nothing in it can be made durable.
    pub fn sync_committed(&mut self) -> Result<(), SqlError> {
        self.flushable()?;
        #[cfg(feature = "fault-injection")]
        crate::faults::note_local_sync();
        self.sync_wal()
    }

    /// Whether `storage.flush` may run now (ADR 0034, section 4): not inside
    /// an open transaction, nor after SQLite rolled the event's transaction
    /// back under the guest (which still believes it open, and whose every
    /// later statement is refused).
    ///
    /// # Errors
    /// `not-allowed`, saying which.
    pub fn flushable(&self) -> Result<(), SqlError> {
        if self.transaction_aborted {
            return Err(SqlError::new(
                crate::SqlErrorKind::NotAllowed,
                "this event's transaction was rolled back when one of its statements failed \
                 (stopped at its deadline or CPU budget, say); nothing more runs in this event",
            ));
        }
        if self.in_transaction() {
            return Err(SqlError::new(
                crate::SqlErrorKind::NotAllowed,
                "flush inside a transaction: commit or roll back first",
            ));
        }
        Ok(())
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
        let why = self.wal_full_why();
        let ddl_seen = Arc::clone(&self.ddl_seen);
        let wal_cap = Arc::clone(&self.wal_cap);
        self.guest(|conn| {
            let stmt = prepare_guest(conn, sql, &ddl_seen)?;
            let mut stmt = refuse_writes_if(|| wal_over(wal.as_ref()), why, stmt)?;
            watch_wal(&stmt, wal.as_ref(), &wal_cap);
            let mut remaining = max;
            let mut charge = |bytes: usize| {
                remaining = remaining.checked_sub(bytes).ok_or_else(|| {
                    SqlError::new(
                        crate::SqlErrorKind::QuotaExceeded,
                        format!("result larger than {max} bytes; select less or page through it"),
                    )
                })?;
                Ok::<_, SqlError>(())
            };
            // Check borrowed names before allocating either the vector or its
            // strings. Even an empty result retains its column names.
            charge(RESULT_BASE_OVERHEAD)?;
            for name in stmt.column_names() {
                charge(RESULT_COLUMN_OVERHEAD)?;
                charge(name.len())?;
            }
            let width = stmt.column_count();
            let mut columns = Vec::with_capacity(width);
            columns.extend(stmt.column_names().map(String::from));
            let mut rows = stmt.query(bind(params))?;
            let mut values = Vec::new();
            while let Some(row) = rows.next()? {
                charge(RESULT_ROW_OVERHEAD)?;
                for _ in 0..width {
                    charge(RESULT_CELL_OVERHEAD)?;
                }
                let mut out = Vec::with_capacity(width);
                for i in 0..width {
                    use rusqlite::types::ValueRef;
                    let value = row.get_ref(i)?;
                    let value = match value {
                        ValueRef::Text(text) => {
                            // Preserve lossy UTF-8 conversion, but charge its
                            // expanded length before copying and reserve exactly
                            // that length so conversion cannot leave spare space.
                            let mut len = 0;
                            for chunk in text.utf8_chunks() {
                                let bytes = chunk.valid().len()
                                    + if chunk.invalid().is_empty() { 0 } else { 3 };
                                charge(bytes)?;
                                len += bytes;
                            }
                            let mut owned = String::with_capacity(len);
                            for chunk in text.utf8_chunks() {
                                owned.push_str(chunk.valid());
                                if !chunk.invalid().is_empty() {
                                    owned.push('\u{fffd}');
                                }
                            }
                            SqlValue::Text(owned)
                        }
                        ValueRef::Blob(blob) => {
                            charge(blob.len())?;
                            SqlValue::Blob(blob.to_vec())
                        }
                        _ => SqlValue::from(value),
                    };
                    out.push(value);
                }
                // Avoid Vec's minimum initial capacity of four; explicit
                // doubling keeps spare row slots within RESULT_ROW_OVERHEAD.
                if values.len() == values.capacity() {
                    values.reserve_exact(values.len().max(1));
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

    /// Removes a host metadata value, if there is one.
    pub fn delete_meta(&mut self, key: &str) -> Result<(), SqlError> {
        #[cfg(feature = "fault-injection")]
        if crate::faults::take(&format!("delete_meta:{key}")) {
            return Err(SqlError::new(
                crate::SqlErrorKind::Internal,
                "disk I/O error (injected)",
            ));
        }
        self.internal(|conn| {
            conn.execute("DELETE FROM main._wasmkeep_meta WHERE key = ?1", [key])
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

    /// Runs a host statement: past the sandbox, but with triggers and
    /// foreign key enforcement off, so it runs nothing the guest wrote (a
    /// trigger, or a cascade from a host table into a guest's), and under
    /// the statement deadline.
    fn internal<T>(
        &mut self,
        f: impl FnOnce(&Connection) -> rusqlite::Result<T>,
    ) -> Result<T, SqlError> {
        use rusqlite::config::DbConfig;
        // `PRAGMA foreign_keys` cannot change inside a transaction (host
        // statements run in the guest's); the connection setting can.
        let fkeys = self.conn.db_config(DbConfig::SQLITE_DBCONFIG_ENABLE_FKEY)?;
        let triggers = self
            .conn
            .db_config(DbConfig::SQLITE_DBCONFIG_ENABLE_TRIGGER)?;
        let in_transaction = !self.conn.is_autocommit();
        let limit = u64::try_from(self.max_statement_time.as_millis()).unwrap_or(u64::MAX);
        let (result, restored) = {
            // Restores the settings, the flag and the deadline even if `f`
            // panics.
            let mut host = HostMode {
                conn: &self.conn,
                internal: &self.internal,
                deadline: &self.deadline,
                fkeys,
                triggers,
                restored: false,
            };
            self.internal.store(true, Ordering::Relaxed);
            self.deadline
                .store(clock_ms().saturating_add(limit).max(1), Ordering::Relaxed);
            let result = self
                .conn
                .set_db_config(DbConfig::SQLITE_DBCONFIG_ENABLE_FKEY, false)
                .and_then(|_| {
                    self.conn
                        .set_db_config(DbConfig::SQLITE_DBCONFIG_ENABLE_TRIGGER, false)
                })
                .and_then(|_| f(&self.conn));
            (result, host.restore())
        };
        // As for a guest statement: an interrupted host statement rolls the
        // guest's transaction back.
        if result.is_err() && in_transaction && self.conn.is_autocommit() {
            self.transaction_aborted = true;
        }
        let value = result?;
        restored?;
        Ok(value)
    }
}

/// What [`ActorDb::internal`] changes for a host statement, restored when
/// dropped (also on a panic) if not before.
struct HostMode<'a> {
    conn: &'a Connection,
    internal: &'a AtomicBool,
    deadline: &'a AtomicU64,
    fkeys: bool,
    triggers: bool,
    restored: bool,
}

impl HostMode<'_> {
    fn restore(&mut self) -> rusqlite::Result<()> {
        use rusqlite::config::DbConfig;
        self.restored = true;
        self.deadline.store(0, Ordering::Relaxed);
        self.internal.store(false, Ordering::Relaxed);
        self.conn
            .set_db_config(DbConfig::SQLITE_DBCONFIG_ENABLE_TRIGGER, self.triggers)?;
        self.conn
            .set_db_config(DbConfig::SQLITE_DBCONFIG_ENABLE_FKEY, self.fkeys)?;
        Ok(())
    }
}

impl Drop for HostMode<'_> {
    fn drop(&mut self) {
        if !self.restored {
            let _ = self.restore();
        }
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

/// The host's own tables, made at the first open of every database.
const HOST_TABLES: &[&str] = &["_wasmkeep_meta", "_wasmkeep_alarm"];

/// A guest object in the schema that uses the reserved prefix
/// ([`ActorDb::reserved_objects`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReservedObject {
    /// `table`, `view`, `index` or `trigger`, as `sqlite_schema` says.
    pub kind: String,
    /// Its name, as the guest wrote it.
    pub name: String,
    pub uses: ReservedUse,
}

/// How a [`ReservedObject`] uses the reserved prefix.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReservedUse {
    /// Its name has it.
    Named,
    /// Its definition names something that has it (a foreign key to a
    /// host table, say).
    References,
}

/// SQLite virtual machine steps between deadline checks.
const PROGRESS_STEPS: std::ffi::c_int = 10_000;

// SQLite limits on guest connections: each bounds what one statement can
// make SQLite allocate or compute. SQLite applies the column, argument and
// depth limits while it loads the schema, too: one lower than what a
// stored table or CHECK, DEFAULT, generated-column or index expression
// already uses makes the whole database unreadable. Those stay where an
// existing database cannot already be past them.
/// Columns in a table, index or result: SQLite's own maximum (2000), as
/// set explicitly; lower, a wider existing table would fail to load.
const GUEST_MAX_COLUMNS: i32 = 2000;
/// Arguments to one function call: SQLite's default (1000), as set
/// explicitly.
const GUEST_MAX_FUNCTION_ARGS: i32 = 1000;
/// Depth of an expression tree: SQLite's default (1000), as set explicitly.
const GUEST_MAX_EXPR_DEPTH: i32 = 1000;
/// Terms of a compound `SELECT`: SQLite's default (500), as set explicitly
/// (SQLite applies it to stored views and triggers as it loads them).
const GUEST_MAX_COMPOUND_SELECT: i32 = 500;
/// Instructions in one compiled statement (SQLite: 250,000,000). Past it
/// SQLite answers "out of memory".
const GUEST_MAX_VDBE_OPS: i32 = 2_000_000;
/// Bytes in a `LIKE` or `GLOB` pattern: SQLite's default (50,000), as set
/// explicitly. The host's `like` and `glob` check it as SQLite's do; a
/// match costs up to the value's length times the pattern's, under the
/// statement's budgets ([`crate::guest_functions`]).
const GUEST_MAX_LIKE_PATTERN: i32 = 50_000;
/// rusqlite's busy timeout, which a checkpoint that does not wait for
/// readers sets aside for a moment.
const BUSY_TIMEOUT: Duration = Duration::from_secs(5);
/// The WAL's header and each frame's header, in bytes.
const WAL_HEADER_BYTES: u64 = 32;
const WAL_FRAME_HEADER_BYTES: u64 = 24;
/// SQLite's heap, for the whole process (`PRAGMA hard_heap_limit`): past it
/// an allocation fails and the statement with it, instead of the process
/// running out of memory. Room for the default 1,000 resident actors' page
/// caches (about 2 MiB each) and for statements holding 16 MiB values and
/// sorting in memory (`temp_store = MEMORY`).
const SQLITE_HEAP_LIMIT_BYTES: i64 = 4 << 30;

/// Virtual table modules guests may use; others (and `fts3_tokenizer`,
/// which takes a raw function pointer) could reach into the host.
const ALLOWED_MODULES: &[&str] = &["fts3", "fts4", "fts5", "rtree", "rtree_i32"];

/// Functions guests may not call.
const DENIED_FUNCTIONS: &[&str] = &["load_extension", "fts3_tokenizer"];

/// Milliseconds on a monotonic clock, for statement deadlines.
/// CPU time the calling thread has used, in ns from an arbitrary origin.
/// Only readings on one thread are compared.
#[cfg(any(target_os = "linux", target_os = "macos"))]
pub(crate) fn thread_cpu_ns() -> u64 {
    let t = rustix::time::clock_gettime(rustix::time::ClockId::ThreadCPUTime);
    u64::try_from(t.tv_sec)
        .unwrap_or(0)
        .saturating_mul(1_000_000_000)
        .saturating_add(u64::try_from(t.tv_nsec).unwrap_or(0))
}

/// Elsewhere, wall time (a preempted statement is charged its wait too).
#[cfg(not(any(target_os = "linux", target_os = "macos")))]
pub(crate) fn thread_cpu_ns() -> u64 {
    static START: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();
    u64::try_from(START.get_or_init(Instant::now).elapsed().as_nanos()).unwrap_or(u64::MAX)
}

pub(crate) fn clock_ms() -> u64 {
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

/// The name of the index, trigger or view an action creates, drops or
/// rebuilds.
fn object_name<'a>(action: &AuthAction<'a>) -> Option<&'a str> {
    use AuthAction as A;
    match *action {
        A::CreateIndex { index_name, .. }
        | A::DropIndex { index_name, .. }
        | A::Reindex { index_name } => Some(index_name),
        A::CreateTrigger { trigger_name, .. } | A::DropTrigger { trigger_name, .. } => {
            Some(trigger_name)
        }
        A::CreateView { view_name } | A::DropView { view_name } => Some(view_name),
        _ => None,
    }
}

/// The guest's sandbox: what a guest statement may do. Anything not named
/// here is refused, including actions a later SQLite adds. Rename targets
/// and foreign key parents are not reported to an authorizer; see
/// [`prepare_guest`].
fn authorize(ctx: &AuthContext<'_>) -> Authorization {
    use AuthAction as A;
    if table_name(&ctx.action).is_some_and(is_reserved)
        || object_name(&ctx.action).is_some_and(is_reserved)
    {
        return Authorization::Deny;
    }
    match ctx.action {
        A::Select
        | A::Recursive
        | A::Read { .. }
        | A::Insert { .. }
        | A::Update { .. }
        | A::Delete { .. }
        | A::CreateTable { .. }
        | A::CreateIndex { .. }
        | A::CreateTrigger { .. }
        | A::CreateView { .. }
        | A::DropTable { .. }
        | A::DropIndex { .. }
        | A::DropTrigger { .. }
        | A::DropView { .. }
        | A::AlterTable { .. }
        | A::Reindex { .. }
        | A::Analyze { .. }
        | A::Transaction { .. }
        | A::Savepoint { .. } => Authorization::Allow,
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
        // `ATTACH`, `DETACH`; temporary tables, views, triggers and indexes
        // (the temp schema is not under the size cap, and a temp table
        // renamed onto a host table's name would shadow it); anything else.
        _ => Authorization::Deny,
    }
}

/// What [`prepare_data_root`] found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataRoot {
    /// It did not exist: created for this account alone (0700 on Unix).
    Created,
    /// It exists and other accounts cannot read it.
    Private,
    /// It exists and its group or other accounts may read it (its mode,
    /// left unchanged).
    Loose(u32),
}

/// Creates the data directory for this account alone if it does not exist
/// yet. An existing one is never changed: it is reported [`DataRoot::Loose`]
/// when its group or other accounts can read it. Only the directory itself
/// gets the mode; missing parents are created as usual.
///
/// # Errors
/// It cannot be created or read.
pub fn prepare_data_root(dir: &Path) -> std::io::Result<DataRoot> {
    if let Some(parent) = dir.parent().filter(|p| !p.as_os_str().is_empty()) {
        std::fs::create_dir_all(parent)?;
    }
    #[allow(unused_mut)] // only Unix sets a mode
    let mut builder = std::fs::DirBuilder::new();
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    match builder.create(dir) {
        Ok(()) => return Ok(DataRoot::Created),
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(e) => return Err(e),
    }
    let meta = std::fs::metadata(dir)?;
    if !meta.is_dir() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::AlreadyExists,
            format!("{} is not a directory", dir.display()),
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = meta.permissions().mode() & 0o777;
        if mode & 0o077 != 0 {
            return Ok(DataRoot::Loose(mode));
        }
    }
    Ok(DataRoot::Private)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)] // test code

    use super::*;

    #[cfg(unix)]
    #[test]
    fn a_new_data_root_is_owner_only_and_an_existing_one_is_left_alone() {
        use std::os::unix::fs::PermissionsExt;
        let mode = |p: &Path| std::fs::metadata(p).unwrap().permissions().mode() & 0o777;
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("parent/data");
        assert_eq!(prepare_data_root(&root).unwrap(), DataRoot::Created);
        assert_eq!(mode(&root), 0o700);
        assert_eq!(prepare_data_root(&root).unwrap(), DataRoot::Private);
        let loose = dir.path().join("loose");
        std::fs::create_dir(&loose).unwrap();
        std::fs::set_permissions(&loose, std::fs::Permissions::from_mode(0o755)).unwrap();
        assert_eq!(prepare_data_root(&loose).unwrap(), DataRoot::Loose(0o755));
        assert_eq!(mode(&loose), 0o755);
    }

    /// A refused `exec-durable("BEGIN")` leaves the connection at
    /// `synchronous = NORMAL` (1; FULL is 2), as every statement outside
    /// `exec-durable` expects.
    #[test]
    fn a_refused_durable_begin_leaves_synchronous_normal() {
        let dir = tempfile::tempdir().unwrap();
        let state = crate::StateDir {
            dir: "actors/t".into(),
            scratch: ".deleting".into(),
            tenant: "t".into(),
        };
        let mut db = crate::Storage::new(crate::StorageConfig::new(dir.path()), state)
            .open("c", "a")
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

    fn open(config: crate::StorageConfig) -> ActorDb {
        let state = crate::StateDir {
            dir: "actors/t".into(),
            scratch: ".deleting".into(),
            tenant: "t".into(),
        };
        crate::Storage::new(config, state).open("c", "a").unwrap()
    }

    /// Sorter and materialisation spill stays in memory (2: MEMORY), not
    /// in files of the system's temporary directory.
    #[test]
    fn temporary_data_stays_in_memory() {
        let dir = tempfile::tempdir().unwrap();
        let mut db = open(crate::StorageConfig::new(dir.path()));
        let store: i64 = db
            .internal(|conn| conn.pragma_query_value(None, "temp_store", |r| r.get(0)))
            .unwrap();
        assert_eq!(store, 2);
    }

    /// A host statement has a deadline too, and stopping one inside a
    /// guest transaction ends the event's statements like a guest's.
    #[test]
    fn host_statements_stop_at_the_statement_deadline() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = crate::StorageConfig::new(dir.path());
        config.max_statement_time = Duration::from_millis(200);
        let mut db = open(config);
        db.exec("CREATE TABLE t (x)", &[]).unwrap();
        db.exec("BEGIN", &[]).unwrap();
        db.exec("INSERT INTO t VALUES (1)", &[]).unwrap();
        let started = Instant::now();
        let err = db
            .internal(|conn| {
                // A write: SQLite rolls the transaction back when it stops one.
                conn.execute(
                    "INSERT INTO t WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL \
                     SELECT x + 1 FROM c) SELECT x FROM c",
                    [],
                )
            })
            .unwrap_err();
        assert!(err.message.contains("interrupted"), "{err:?}");
        assert!(started.elapsed() < Duration::from_secs(5));
        assert!(
            db.query("SELECT 1", &[]).is_err(),
            "the event's transaction is gone"
        );
        db.end_event().unwrap();
        db.set_meta("k", "v").unwrap();
        let rows = db.query("SELECT count(*) FROM t", &[]).unwrap();
        assert_eq!(rows.values, vec![vec![SqlValue::Integer(0)]]);
    }

    #[test]
    fn reserved_identifiers_are_found_in_statement_text() {
        for sql in [
            "_wasmkeep_x",
            "ALTER TABLE t RENAME TO _WASMKEEP_x",
            "CREATE TABLE g (a REFERENCES \"_wasmkeep_alarm\")",
            "CREATE TABLE g (a REFERENCES [_wasmkeep_alarm])",
            "CREATE TABLE g (a REFERENCES `_wasmkeep_alarm`)",
            "CREATE TABLE g (a REFERENCES main._wasmkeep_alarm)",
            "CREATE TABLE g (a REFERENCES\n_wasmkeep_alarm)",
            "CREATE TABLE g (a REFERENCES/**/_wasmkeep_alarm)",
            // A byte-order mark is white space to SQLite.
            "CREATE TABLE g (a REFERENCES \u{FEFF}_wasmkeep_alarm(id))",
            "ALTER TABLE t RENAME TO \u{FEFF}_wasmkeep_x",
            // Any non-ASCII byte before it counts as a boundary (fails closed).
            "é_wasmkeep_",
        ] {
            assert!(names_reserved(sql), "{sql}");
        }
        for sql in [
            "CREATE TABLE app_wasmkeep_state (x)",
            "x__wasmkeep_",
            "x$_wasmkeep_",
        ] {
            assert!(!names_reserved(sql), "{sql}");
        }
    }
}
