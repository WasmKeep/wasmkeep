use std::path::PathBuf;

/// Why a statement failed; mirrors `wasmkeep:actor/types.sql-error-kind`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlErrorKind {
    /// Bad SQL, or a missing table or column.
    Syntax,
    /// A constraint failed.
    Constraint,
    /// Parameter count or types do not match.
    Bind,
    /// More than one statement in one call.
    MultipleStatements,
    /// The database reached its size cap.
    QuotaExceeded,
    /// Forbidden by the sandbox (ATTACH, most PRAGMAs, ...).
    NotAllowed,
    /// Host failure (I/O, corruption).
    Internal,
}

/// A statement error returned to the guest.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{kind:?}: {message}")]
pub struct SqlError {
    /// What kind of failure.
    pub kind: SqlErrorKind,
    /// Detail, usually SQLite's message.
    pub message: String,
}

impl SqlError {
    pub(crate) fn new(kind: SqlErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }
}

impl From<rusqlite::Error> for SqlError {
    fn from(e: rusqlite::Error) -> Self {
        use rusqlite::Error as E;
        use rusqlite::ErrorCode as C;
        let kind = match &e {
            E::MultipleStatement => SqlErrorKind::MultipleStatements,
            E::InvalidParameterCount(..) | E::InvalidParameterName(_) => SqlErrorKind::Bind,
            E::ExecuteReturnedResults => {
                return Self::new(
                    SqlErrorKind::Syntax,
                    "statement returns rows; use query instead of exec",
                );
            }
            E::SqliteFailure(f, _) => match f.code {
                C::ConstraintViolation => SqlErrorKind::Constraint,
                C::DiskFull | C::TooBig => SqlErrorKind::QuotaExceeded,
                C::AuthorizationForStatementDenied => SqlErrorKind::NotAllowed,
                C::Unknown | C::TypeMismatch | C::ParameterOutOfRange => SqlErrorKind::Syntax,
                _ => SqlErrorKind::Internal,
            },
            E::SqlInputError { error, .. } => match error.code {
                C::AuthorizationForStatementDenied => SqlErrorKind::NotAllowed,
                _ => SqlErrorKind::Syntax,
            },
            _ => SqlErrorKind::Internal,
        };
        let message = e.to_string();
        // SQLite reports a denied function as a plain error ("not authorized
        // to use function: ..."), not as an authorization failure.
        let kind = if message.contains("not authorized") {
            SqlErrorKind::NotAllowed
        } else {
            kind
        };
        Self::new(kind, message)
    }
}

/// Errors opening or deleting a database (host-side, not per statement).
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum StorageError {
    /// Creating directories or removing files failed.
    #[error("storage I/O error at {path}: {source}")]
    Io {
        /// The path involved.
        path: PathBuf,
        /// The cause.
        #[source]
        source: std::io::Error,
    },
    /// Reading or writing host metadata failed.
    #[error("host metadata error in {path}: {source}")]
    Meta {
        /// The database file.
        path: PathBuf,
        /// The cause.
        #[source]
        source: SqlError,
    },
    /// SQLite refused to open or configure the database.
    #[error("cannot open database {path}: {source}{code}", code = extended_code(.source))]
    Open {
        /// The database file.
        path: PathBuf,
        /// The cause.
        #[source]
        source: rusqlite::Error,
    },
}

/// SQLite's extended result code, when there is one (" (SQLite code 517)"):
/// it tells apart the cases its message does not (517 is SQLITE_BUSY_SNAPSHOT,
/// 261 SQLITE_BUSY_RECOVERY, 5 a plain busy).
fn extended_code(e: &rusqlite::Error) -> String {
    match e {
        rusqlite::Error::SqliteFailure(f, _) => format!(" (SQLite code {})", f.extended_code),
        _ => String::new(),
    }
}
