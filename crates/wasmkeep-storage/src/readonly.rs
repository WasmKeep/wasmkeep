//! Local verification and constrained operator queries; never opens an ActorDb.

use crate::{Rows, SqlError, SqlErrorKind, SqlValue};
use rusqlite::hooks::{AuthAction, AuthContext, Authorization};
use rusqlite::{Connection, OpenFlags};
use std::path::Path;

/// Checks performed on the raw replay, before journal conversion.
#[derive(Debug)]
pub struct Verification {
    /// Foreign-key violations are reported, not treated as corrupt storage.
    pub foreign_key_violations: u64,
    /// Application table names (host tables remain in the exported file).
    pub tables: Vec<String>,
    /// Replacement label, if a writer recorded one.
    pub replaced: Option<String>,
}

fn immutable(path: &Path) -> Result<Connection, SqlError> {
    let path = path
        .canonicalize()
        .map_err(|e| SqlError::new(SqlErrorKind::Internal, e.to_string()))?;
    let path = path
        .to_str()
        .ok_or_else(|| SqlError::new(SqlErrorKind::Internal, "database path is not UTF-8"))?;
    let path = path
        .replace('%', "%25")
        .replace('?', "%3F")
        .replace('#', "%23");
    let conn = Connection::open_with_flags(
        format!("file:{path}?immutable=1"),
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_URI,
    )?;
    conn.pragma_update(None, "trusted_schema", false)?;
    conn.pragma_update(None, "query_only", true)?;
    Ok(conn)
}

/// Checks the raw file, then converts this private copy to a rollback journal.
/// The caller must verify its LTX checksum before calling this function.
pub fn verify_and_make_portable(path: &Path) -> Result<Verification, SqlError> {
    let verification = verify(path)?;
    make_portable(path)?;
    Ok(verification)
}

/// Verify a raw replay without modifying its bytes, suitable for backup caches.
pub fn verify(path: &Path) -> Result<Verification, SqlError> {
    let conn = immutable(path)?;
    let mut stmt = conn.prepare("PRAGMA integrity_check")?;
    let mut rows = stmt.query([])?;
    let mut ok = false;
    while let Some(row) = rows.next()? {
        let result: String = row.get(0)?;
        if result != "ok" {
            return Err(SqlError::new(
                SqlErrorKind::Internal,
                format!("integrity_check: {result}"),
            ));
        }
        ok = true;
    }
    if !ok {
        return Err(SqlError::new(
            SqlErrorKind::Internal,
            "integrity_check returned no result",
        ));
    }
    drop(rows);
    drop(stmt);
    let mut stmt = conn.prepare("PRAGMA foreign_key_check")?;
    let mut rows = stmt.query([])?;
    let mut foreign_key_violations = 0;
    while rows.next()?.is_some() {
        foreign_key_violations += 1;
    }
    drop(rows);
    drop(stmt);
    let tables = conn.prepare("SELECT name FROM sqlite_schema WHERE type='table' AND lower(substr(name,1,10)) != '_wasmkeep_' AND name NOT LIKE 'sqlite_%' ORDER BY name")?
        .query_map([], |row| row.get::<_, String>(0))?.collect::<Result<Vec<_>, _>>()?;
    let has_meta: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE name='_wasmkeep_meta')",
        [],
        |row| row.get(0),
    )?;
    let replaced = if has_meta {
        use rusqlite::OptionalExtension;
        conn.query_row(
            "SELECT value FROM _wasmkeep_meta WHERE key='replaced'",
            [],
            |row| row.get(0),
        )
        .optional()?
    } else {
        None
    };
    drop(conn);
    Ok(Verification {
        foreign_key_violations,
        tables,
        replaced,
    })
}

fn make_portable(path: &Path) -> Result<(), SqlError> {
    let conn = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_WRITE)?;
    conn.pragma_update(None, "trusted_schema", false)?;
    let mode: String = conn.query_row("PRAGMA journal_mode=DELETE", [], |row| row.get(0))?;
    if !mode.eq_ignore_ascii_case("delete") {
        return Err(SqlError::new(
            SqlErrorKind::Internal,
            "could not switch to a rollback journal",
        ));
    }
    conn.close().map_err(|(_, e)| SqlError::from(e))?;
    Ok(())
}

/// Executes exactly one read-only statement on a completed, private export.
/// Results are bounded to 10,000 rows / 16 MiB, values to 1 MiB and execution
/// to five seconds or ten million VM instructions. Host and schema tables,
/// extension loading, PRAGMAs and all non-read authorizer actions are denied.
pub fn query(path: &Path, sql: &str) -> Result<Rows, SqlError> {
    use rusqlite::limits::Limit;
    let mut conn = immutable(path)?;
    conn.set_limit(Limit::SQLITE_LIMIT_LENGTH, 1 << 20)?;
    conn.set_limit(Limit::SQLITE_LIMIT_SQL_LENGTH, 1 << 20)?;
    conn.set_limit(Limit::SQLITE_LIMIT_COLUMN, 256)?;
    conn.set_limit(Limit::SQLITE_LIMIT_ATTACHED, 0)?;
    let started = std::time::Instant::now();
    let mut steps = 0u32;
    conn.progress_handler(
        1000,
        Some(move || {
            steps += 1;
            steps > 10_000 || started.elapsed() > std::time::Duration::from_secs(5)
        }),
    )?;
    conn.authorizer(Some(|ctx: AuthContext<'_>| match ctx.action {
        AuthAction::Select | AuthAction::Recursive => Authorization::Allow,
        AuthAction::Read { table_name, .. } if !hidden(table_name) => Authorization::Allow,
        AuthAction::Function { function_name }
            if !matches!(
                function_name.to_ascii_lowercase().as_str(),
                "load_extension" | "fts3_tokenizer" | "readfile" | "writefile" | "eval"
            ) && !function_name.to_ascii_lowercase().starts_with("pragma_") =>
        {
            Authorization::Allow
        }
        _ => Authorization::Deny,
    }))?;
    let mut stmt = conn.prepare(sql)?;
    if !stmt.readonly() || stmt.column_count() == 0 {
        return Err(SqlError::new(
            SqlErrorKind::NotAllowed,
            "expected one read-only statement returning rows",
        ));
    }
    let mut remaining = 16usize << 20;
    let mut charge = |bytes: usize| -> Result<(), SqlError> {
        remaining = remaining.checked_sub(bytes).ok_or_else(|| {
            SqlError::new(
                SqlErrorKind::QuotaExceeded,
                "query exceeds 16 MiB result limit; select less",
            )
        })?;
        Ok(())
    };
    let width = stmt.column_count();
    let mut columns = Vec::with_capacity(width);
    for name in stmt.column_names() {
        charge(64 + name.len())?;
        columns.push(name.to_owned());
    }
    let mut values = Vec::new();
    let mut rows = stmt.query([])?;
    while let Some(row) = rows.next()? {
        if values.len() == 10_000 {
            return Err(SqlError::new(
                SqlErrorKind::QuotaExceeded,
                "query exceeds 10000 rows; add LIMIT",
            ));
        }
        charge(64 + width * 64)?;
        let mut cells = Vec::with_capacity(width);
        for i in 0..width {
            let value = row.get_ref(i)?;
            match value {
                rusqlite::types::ValueRef::Text(v) => charge(v.len().saturating_mul(3))?,
                rusqlite::types::ValueRef::Blob(v) => charge(v.len())?,
                _ => {}
            }
            cells.push(SqlValue::from(value));
        }
        values.push(cells);
    }
    Ok(Rows { columns, values })
}

fn hidden(name: &str) -> bool {
    let name = name.to_ascii_lowercase();
    name.starts_with("_wasmkeep_") || name.starts_with("sqlite_") || name.starts_with("pragma_")
}

/// Alarm state from a verified private database; no guest SQL or write access.
pub fn backup_alarm(path: &Path) -> Result<(Option<i64>, Option<String>), SqlError> {
    use rusqlite::OptionalExtension;
    let conn = immutable(path)?;
    let has = |table: &str| -> Result<bool, rusqlite::Error> {
        conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE name=?1)",
            [table],
            |r| r.get(0),
        )
    };
    let alarm = if has("_wasmkeep_alarm")? {
        conn.query_row("SELECT at FROM _wasmkeep_alarm WHERE id=1", [], |r| {
            r.get(0)
        })
        .optional()?
    } else {
        None
    };
    let indexed = if has("_wasmkeep_meta")? {
        conn.query_row(
            "SELECT value FROM _wasmkeep_meta WHERE key='alarm_indexed'",
            [],
            |r| r.get(0),
        )
        .optional()?
    } else {
        None
    };
    Ok((alarm, indexed))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn authorizer_and_single_statement_policy_leave_bytes_unchanged() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("a?#.db");
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch("CREATE TABLE items(n); INSERT INTO items VALUES(7); CREATE TABLE _wasmkeep_meta(key,value); CREATE VIEW leak AS SELECT * FROM _wasmkeep_meta").unwrap();
        drop(conn);
        let before = std::fs::read(&path).unwrap();
        for sql in [
            "INSERT INTO items VALUES(8)",
            "ATTACH ':memory:' AS x",
            "PRAGMA user_version=1",
            "PRAGMA writable_schema=ON",
            "VACUUM INTO 'leaked.db'",
            "SELECT 1; SELECT 2",
            "SELECT * FROM _wasmkeep_meta",
            "SELECT * FROM leak",
            "SELECT * FROM sqlite_master",
            "SELECT * FROM pragma_table_info('_wasmkeep_meta')",
            "SELECT load_extension('x')",
            "SELECT fts3_tokenizer('simple')",
            "BEGIN",
            "",
        ] {
            assert!(query(&path, sql).is_err(), "{sql}");
        }
        assert_eq!(
            query(&path, "SELECT n FROM items; -- comment")
                .unwrap()
                .values,
            vec![vec![SqlValue::Integer(7)]]
        );
        assert_eq!(std::fs::read(&path).unwrap(), before);
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);
    }

    #[test]
    fn expensive_reads_and_oversized_results_are_bounded() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("a.db");
        Connection::open(&path)
            .unwrap()
            .execute_batch("CREATE TABLE t(n)")
            .unwrap();
        for sql in [
            "WITH RECURSIVE n(x) AS (VALUES(0) UNION ALL SELECT x+1 FROM n) SELECT sum(x) FROM n",
            "WITH RECURSIVE n(x) AS (VALUES(0) UNION ALL SELECT x+1 FROM n WHERE x<10001) SELECT x FROM n",
            "SELECT zeroblob(2000000)",
        ] {
            assert!(query(&path, sql).is_err());
        }
    }
}
