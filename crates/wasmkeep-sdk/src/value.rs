//! Conversions between Rust values and SQL [`Value`]s, and helpers on
//! [`Rows`]. (`SqlError` gets `Display` and `Error` from wit-bindgen.)

use crate::{Rows, Value};

macro_rules! from_integer {
    ($($t:ty),*) => {$(
        impl From<$t> for Value {
            fn from(v: $t) -> Self {
                Value::Integer(i64::from(v))
            }
        }
    )*};
}

from_integer!(i8, i16, i32, i64, u8, u16, u32);

impl From<bool> for Value {
    fn from(v: bool) -> Self {
        Value::Integer(i64::from(v))
    }
}

impl From<f32> for Value {
    fn from(v: f32) -> Self {
        Value::Real(f64::from(v))
    }
}

impl From<f64> for Value {
    fn from(v: f64) -> Self {
        Value::Real(v)
    }
}

impl From<&str> for Value {
    fn from(v: &str) -> Self {
        Value::Text(v.to_owned())
    }
}

impl From<String> for Value {
    fn from(v: String) -> Self {
        Value::Text(v)
    }
}

impl From<&[u8]> for Value {
    fn from(v: &[u8]) -> Self {
        Value::Blob(v.to_vec())
    }
}

impl From<Vec<u8>> for Value {
    fn from(v: Vec<u8>) -> Self {
        Value::Blob(v)
    }
}

impl<T: Into<Value>> From<Option<T>> for Value {
    fn from(v: Option<T>) -> Self {
        v.map_or(Value::Null, Into::into)
    }
}

impl Value {
    /// The integer, if this is one.
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Value::Integer(v) => Some(*v),
            _ => None,
        }
    }

    /// The number as a float, if this is an integer or a real.
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Value::Real(v) => Some(*v),
            #[allow(clippy::cast_precision_loss)]
            Value::Integer(v) => Some(*v as f64),
            _ => None,
        }
    }

    /// The text, if this is text.
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::Text(v) => Some(v),
            _ => None,
        }
    }

    /// The bytes, if this is a blob.
    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Value::Blob(v) => Some(v),
            _ => None,
        }
    }

    /// Whether this is SQL NULL.
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }
}

impl Rows {
    /// The value at `row`, in the column named `column`.
    pub fn get(&self, row: usize, column: &str) -> Option<&Value> {
        let index = self.columns.iter().position(|c| c == column)?;
        self.values.get(row)?.get(index)
    }

    /// Number of rows.
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Whether there are no rows.
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn conversions() {
        assert_eq!(Value::from(42_i32), Value::Integer(42));
        assert_eq!(Value::from(true), Value::Integer(1));
        assert_eq!(Value::from("a"), Value::Text("a".into()));
        assert_eq!(Value::from(None::<i64>), Value::Null);
        assert_eq!(Value::from(Some(1.5)), Value::Real(1.5));
        assert_eq!(Value::from(vec![1_u8]), Value::Blob(vec![1]));
        assert_eq!(
            crate::params![1, "x"],
            vec![Value::Integer(1), Value::Text("x".into())]
        );
    }

    #[test]
    fn rows_get_by_column_name() {
        let rows = Rows {
            columns: vec!["id".into(), "name".into()],
            values: vec![vec![Value::Integer(1), Value::Text("a".into())]],
        };
        assert_eq!(rows.get(0, "name").and_then(Value::as_str), Some("a"));
        assert_eq!(rows.get(0, "missing"), None);
        assert_eq!(rows.get(1, "id"), None);
        assert_eq!(rows.len(), 1);
    }
}
