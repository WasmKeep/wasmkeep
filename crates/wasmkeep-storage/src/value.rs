/// A SQL value; one variant per SQLite storage class.
#[derive(Debug, Clone, PartialEq)]
pub enum SqlValue {
    /// SQL NULL.
    Null,
    /// 64-bit signed integer.
    Integer(i64),
    /// 64-bit float.
    Real(f64),
    /// UTF-8 text.
    Text(String),
    /// Bytes.
    Blob(Vec<u8>),
}

/// A query result: column names, then rows of values in column order.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Rows {
    /// Column names.
    pub columns: Vec<String>,
    /// One entry per row.
    pub values: Vec<Vec<SqlValue>>,
}

impl From<&SqlValue> for rusqlite::types::Value {
    fn from(v: &SqlValue) -> Self {
        use rusqlite::types::Value as V;
        match v {
            SqlValue::Null => V::Null,
            SqlValue::Integer(i) => V::Integer(*i),
            SqlValue::Real(f) => V::Real(*f),
            SqlValue::Text(s) => V::Text(s.clone()),
            SqlValue::Blob(b) => V::Blob(b.clone()),
        }
    }
}

impl From<rusqlite::types::ValueRef<'_>> for SqlValue {
    fn from(v: rusqlite::types::ValueRef<'_>) -> Self {
        use rusqlite::types::ValueRef as V;
        match v {
            V::Null => SqlValue::Null,
            V::Integer(i) => SqlValue::Integer(i),
            V::Real(f) => SqlValue::Real(f),
            V::Text(t) => SqlValue::Text(String::from_utf8_lossy(t).into_owned()),
            V::Blob(b) => SqlValue::Blob(b.to_vec()),
        }
    }
}
