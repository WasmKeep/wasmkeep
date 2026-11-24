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

// Install before any verification SQL, including schema and host metadata reads.
fn verification_connection(path: &Path) -> Result<Connection, SqlError> {
    let mut conn = immutable(path)?;
    limits(&mut conn)?;
    conn.authorizer(Some(|ctx: AuthContext<'_>| {
        verification_authorizer(ctx, &[])
    }))?;
    // A host table's name held by something else (a view squatting
    // `_wasmkeep_meta`, say) is refused; a guest's own object with the
    // prefix, possible before 0.8, is not the host's and is kept.
    let invalid: bool = conn.query_row("SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE lower(name) IN ('_wasmkeep_meta', '_wasmkeep_alarm') AND (type != 'table' OR upper(substr(sql,1,20)) = 'CREATE VIRTUAL TABLE'))", [], |r| r.get(0))?;
    if invalid {
        return Err(SqlError::new(
            SqlErrorKind::NotAllowed,
            "verification rejects non-table host metadata schema",
        ));
    }
    // R-tree xConnect prepares shadow-table writes even on a read-only
    // connection. Permit only those preparations; immutable + query_only still
    // reject their execution. User SQL never receives this exception.
    let virtuals = conn.prepare("SELECT name FROM sqlite_schema WHERE type='table' AND upper(substr(sql,1,20))='CREATE VIRTUAL TABLE'")?
        .query_map([], |r| r.get::<_, String>(0))?.collect::<Result<Vec<_>, _>>()?;
    let shadows: Vec<String> = virtuals
        .iter()
        .flat_map(|name| {
            [
                format!("{name}_node"),
                format!("{name}_parent"),
                format!("{name}_rowid"),
            ]
        })
        .collect();
    conn.authorizer(Some(move |ctx: AuthContext<'_>| {
        verification_authorizer(ctx, &shadows)
    }))?;
    Ok(conn)
}

fn verification_authorizer(ctx: AuthContext<'_>, shadows: &[String]) -> Authorization {
    if ctx.accessor.is_some() {
        return Authorization::Deny;
    }
    match ctx.action {
        AuthAction::Select | AuthAction::Read { .. } => Authorization::Allow,
        AuthAction::Pragma {
            pragma_name: "integrity_check" | "foreign_key_check" | "data_version",
            pragma_value: None,
        } => Authorization::Allow,
        AuthAction::Function { function_name } if safe_function(function_name) => {
            Authorization::Allow
        }
        AuthAction::Insert { table_name }
        | AuthAction::Delete { table_name }
        | AuthAction::Update { table_name, .. }
            if shadows.iter().any(|name| name == table_name) =>
        {
            Authorization::Allow
        }
        _ => Authorization::Deny,
    }
}

/// Seconds an offline connection runs before it is stopped.
const DEADLINE_SECS: u64 = 5;

fn limits(conn: &mut Connection) -> Result<(), SqlError> {
    use rusqlite::limits::Limit;
    conn.set_limit(Limit::SQLITE_LIMIT_LENGTH, 1 << 20)?;
    conn.set_limit(Limit::SQLITE_LIMIT_SQL_LENGTH, 1 << 20)?;
    conn.set_limit(Limit::SQLITE_LIMIT_COLUMN, 256)?;
    conn.set_limit(Limit::SQLITE_LIMIT_ATTACHED, 0)?;
    // The host's `LIKE`, `GLOB`, `instr`, `replace` and `trim`, as on guest
    // connections: the same answers (an index or a CHECK a guest built on
    // them is recomputed by `integrity_check`), no recursion on a deep
    // pattern, and stopped at this connection's deadline.
    let budget = crate::guest_functions::Budget {
        deadline: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(
            crate::db::clock_ms().saturating_add(DEADLINE_SECS * 1000),
        )),
        cpu_deadline: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
    };
    let max_len = usize::try_from(conn.limit(Limit::SQLITE_LIMIT_LENGTH)?).unwrap_or(0);
    let max_pattern =
        usize::try_from(conn.limit(Limit::SQLITE_LIMIT_LIKE_PATTERN_LENGTH)?).unwrap_or(0);
    crate::guest_functions::register(conn, &budget, max_len, max_pattern)?;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(DEADLINE_SECS);
    let mut steps = 0u32;
    conn.progress_handler(
        1000,
        Some(move || {
            steps += 1;
            steps > 10_000 || std::time::Instant::now() >= deadline
        }),
    )?;
    Ok(())
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
    let conn = verification_connection(path)?;
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
    let mut conn = immutable(path)?;
    limits(&mut conn)?;
    conn.authorizer(Some(|ctx: AuthContext<'_>| match ctx.action {
        AuthAction::Select | AuthAction::Recursive => Authorization::Allow,
        AuthAction::Read { table_name, .. } if !hidden(table_name) => Authorization::Allow,
        AuthAction::Function { function_name } if safe_function(function_name) => {
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

fn safe_function(name: &str) -> bool {
    let name = name.to_ascii_lowercase();
    !matches!(
        name.as_str(),
        "load_extension" | "fts3_tokenizer" | "readfile" | "writefile" | "eval"
    ) && !name.starts_with("pragma_")
}

fn hidden(name: &str) -> bool {
    let name = name.to_ascii_lowercase();
    name.starts_with("_wasmkeep_") || name.starts_with("sqlite_") || name.starts_with("pragma_")
}

/// Alarm state from a verified private database; no guest SQL or write access.
pub fn backup_alarm(path: &Path) -> Result<(Option<i64>, Option<String>), SqlError> {
    use rusqlite::OptionalExtension;
    let conn = verification_connection(path)?;
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

/// Original address recorded by the host, including names shortened in keys.
pub fn backup_address(path: &Path) -> Result<Option<String>, SqlError> {
    use rusqlite::OptionalExtension;
    let conn = verification_connection(path)?;
    Ok(conn
        .query_row(
            "SELECT value FROM _wasmkeep_meta WHERE key='address'",
            [],
            |r| r.get(0),
        )
        .optional()?)
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

#[cfg(test)]
#[test]
fn verification_rejects_host_views_before_execution() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("hostile.db");
    let conn = Connection::open(&path).unwrap();
    // Rejected from sqlite_schema before executing the recursive view.
    conn.execute_batch("CREATE VIEW _wasmkeep_meta AS WITH RECURSIVE n(x) AS (VALUES(0) UNION ALL SELECT x+1 FROM n) SELECT 'replaced' AS key, CAST(sum(x) AS TEXT) AS value FROM n").unwrap();
    drop(conn);
    assert!(
        verify(&path).is_err(),
        "host metadata views must be rejected"
    );
}

#[cfg(test)]
#[test]
fn verification_of_virtual_tables_cannot_write_shadow_tables() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("virtual.db");
    let conn = Connection::open(&path).unwrap();
    conn.execute_batch(
        "CREATE VIRTUAL TABLE spatial USING rtree(id,min,max); INSERT INTO spatial VALUES(1,2,3); CREATE VIRTUAL TABLE search USING fts5(text)",
    )
    .unwrap();
    drop(conn);
    let before = std::fs::read(&path).unwrap();
    verify(&path).unwrap();
    let conn = verification_connection(&path).unwrap();
    assert!(conn.execute("DELETE FROM spatial_node", []).is_err());
    assert!(
        conn.execute("INSERT INTO spatial_rowid VALUES(2,1)", [])
            .is_err()
    );
    assert_eq!(std::fs::read(&path).unwrap(), before);
}
