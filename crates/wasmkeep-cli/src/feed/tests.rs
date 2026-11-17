#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
use super::*;
use parquet::{
    file::reader::{FileReader, SerializedFileReader},
    record::Field,
};
use std::fs::File;
use wasmkeep_storage::SqlValue::{self, *};

fn actor(columns: &[&str], values: Vec<Vec<SqlValue>>) -> ActorTable {
    ActorTable {
        tenant: "acme".into(),
        app: "shop".into(),
        name: "élise".into(),
        seq: u64::MAX,
        generation: u64::MAX,
        txid: u64::MAX,
        committed_at: 1700000000123,
        rows: Rows {
            columns: columns.iter().map(|s| (*s).into()).collect(),
            values,
        },
    }
}
fn read(path: &Path) -> Vec<BTreeMap<String, Field>> {
    SerializedFileReader::new(File::open(path).unwrap())
        .unwrap()
        .get_row_iter(None)
        .unwrap()
        .map(|r| {
            r.unwrap()
                .get_column_iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect()
        })
        .collect()
}
#[test]
fn round_trip_values_and_metadata() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("1.parquet");
    let a = actor(
        &["int", "real", "text", "blob", "null"],
        vec![
            vec![
                Integer(i64::MIN),
                Real(-1.25),
                Text("Québec,\"雪\n🦀".into()),
                Blob(vec![0, 255, 128]),
                Null,
            ],
            vec![
                Integer(i64::MAX),
                Null,
                Text(String::new()),
                Blob(vec![]),
                Null,
            ],
            vec![Null, Real(f64::INFINITY), Null, Null, Null],
        ],
    );
    let written = write_table(&path, "carts", "items", &[a], &BTreeMap::new()).unwrap();
    assert_eq!(
        written.columns,
        BTreeMap::from([
            ("int".into(), ColumnType::Integer),
            ("real".into(), ColumnType::Real),
            ("text".into(), ColumnType::Text),
            ("blob".into(), ColumnType::Blob)
        ])
    );
    let rows = read(&path);
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0]["int"], Field::Long(i64::MIN));
    assert_eq!(rows[1]["int"], Field::Long(i64::MAX));
    assert_eq!(rows[2]["int"], Field::Null);
    assert_eq!(rows[0]["real"], Field::Double(-1.25));
    assert_eq!(rows[1]["real"], Field::Null);
    assert_eq!(rows[2]["real"], Field::Double(f64::INFINITY));
    assert_eq!(rows[0]["text"], Field::Str("Québec,\"雪\n🦀".into()));
    assert_eq!(rows[1]["text"], Field::Str(String::new()));
    assert_eq!(rows[2]["text"], Field::Null);
    assert_eq!(rows[0]["blob"], Field::Bytes(vec![0, 255, 128].into()));
    assert_eq!(rows[1]["blob"], Field::Bytes(vec![].into()));
    assert_eq!(rows[2]["blob"], Field::Null);
    assert!(!rows[0].contains_key("null"));
    for row in rows {
        assert_eq!(row["_wk_tenant"], Field::Str("acme".into()));
        assert_eq!(row["_wk_app"], Field::Str("shop".into()));
        assert_eq!(row["_wk_name"], Field::Str("élise".into()));
        for key in ["_wk_seq", "_wk_generation", "_wk_txid"] {
            assert_eq!(row[key], Field::ULong(u64::MAX));
        }
        assert_eq!(
            row["_wk_committed_at"],
            Field::TimestampMillis(1700000000123)
        );
    }
    assert!(!dir.path().join("1.parquet.partial").exists());
}
#[test]
fn union_mixed_sticky_null_and_reserved_names() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("1.parquet");
    let previous = BTreeMap::from([
        ("sticky".into(), ColumnType::Text),
        ("empty".into(), ColumnType::Blob),
        ("changed".into(), ColumnType::Integer),
    ]);
    let a = actor(
        &["Mixed", "sticky", "empty", "changed", "_wk_name", "Ä", "ä"],
        vec![vec![
            Integer(1),
            Blob(vec![0, 255]),
            Null,
            Real(2.5),
            Text("app".into()),
            Integer(1),
            Integer(2),
        ]],
    );
    let mut b = actor(
        &["MIXED", "new"],
        vec![
            vec![Blob(vec![0, 255]), Text("new".into())],
            vec![Real(1.5), Null],
            vec![Text("雪".into()), Null],
            vec![Null, Null],
        ],
    );
    b.name = "bob".into();
    b.txid = 0;
    b.committed_at = -1;
    let written = write_table(&path, "carts", "items", &[a, b], &previous).unwrap();
    let reader = SerializedFileReader::new(File::open(&path).unwrap()).unwrap();
    let schema = reader.metadata().file_metadata().schema_descr();
    let empty = schema
        .columns()
        .iter()
        .find(|c| c.name() == "empty")
        .unwrap();
    assert_eq!(empty.physical_type(), PhysicalType::BYTE_ARRAY);
    assert_eq!(empty.logical_type_ref(), None);
    assert_eq!(empty.max_def_level(), 1);
    assert_eq!(reader.num_row_groups(), 2);
    assert_eq!(written.columns["Mixed"], ColumnType::Text);
    assert_eq!(written.columns["sticky"], ColumnType::Text);
    assert_eq!(written.columns["empty"], ColumnType::Blob);
    assert_eq!(written.columns["changed"], ColumnType::Real);
    assert!(
        written
            .warnings
            .iter()
            .any(|s| s.contains("carts") && s.contains("items") && s.contains("Mixed"))
    );
    assert!(written.warnings.iter().any(|s| s.contains("_wk_name")));
    let r = read(&path);
    for (i, s) in ["1", "X'00ff'", "1.5", "雪"].iter().enumerate() {
        assert_eq!(r[i]["Mixed"], Field::Str((*s).into()));
    }
    assert_eq!(r[4]["Mixed"], Field::Null);
    assert_eq!(r[1]["_wk_name"], Field::Str("bob".into()));
    assert_eq!(r[1]["_wk_txid"], Field::ULong(0));
    assert_eq!(r[1]["_wk_committed_at"], Field::TimestampMillis(-1));
    assert_eq!(r[0]["sticky"], Field::Str("X'00ff'".into()));
    assert_eq!(r[1]["sticky"], Field::Null);
    assert_eq!(r[0]["empty"], Field::Null);
    assert_eq!(r[0]["_app_wk_name"], Field::Str("app".into()));
    assert_eq!(r[0]["new"], Field::Null);
    assert_eq!(r[0]["Ä"], Field::Long(1));
    assert_eq!(r[0]["ä"], Field::Long(2));
    let second = dir.path().join("2.parquet");
    write_table(
        &second,
        "carts",
        "items",
        &[actor(&["mixed"], vec![vec![Integer(7)]])],
        &written.columns,
    )
    .unwrap();
    assert_eq!(read(&second)[0]["mixed"], Field::Str("7".into()));
}
#[test]
fn immutable_files_and_invalid_input() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("1.parquet");
    let a = actor(&["x"], vec![vec![Integer(1)]]);
    write_table(&path, "c", "t", std::slice::from_ref(&a), &BTreeMap::new()).unwrap();
    let bytes = std::fs::read(&path).unwrap();
    assert!(write_table(&path, "c", "t", &[a], &BTreeMap::new()).is_err());
    assert_eq!(bytes, std::fs::read(&path).unwrap());
    for a in [
        actor(&["x", "X"], vec![]),
        actor(&["_wk_name", "_app_wk_name"], vec![]),
        actor(&["x"], vec![vec![]]),
    ] {
        let bad = dir.path().join("bad.parquet");
        assert!(write_table(&bad, "c", "t", &[a], &BTreeMap::new()).is_err());
        assert!(!bad.exists());
        assert!(!dir.path().join("bad.parquet.partial").exists());
    }
    let empty = dir.path().join("empty.parquet");
    write_table(&empty, "c", "t", &[], &BTreeMap::new()).unwrap();
    assert!(read(&empty).is_empty());
    let partial = dir.path().join("busy.parquet.partial");
    std::fs::write(&partial, b"other writer").unwrap();
    assert!(
        write_table(
            &dir.path().join("busy.parquet"),
            "c",
            "t",
            &[],
            &BTreeMap::new()
        )
        .is_err()
    );
    assert_eq!(std::fs::read(partial).unwrap(), b"other writer");
}

#[test]
fn numeric_mixture_and_sticky_real() {
    let dir = tempfile::tempdir().unwrap();
    for (i, integer) in [-(1i64 << 53), 1 << 53, -(1 << 53) - 1, (1 << 53) + 1]
        .into_iter()
        .enumerate()
    {
        for reverse in [false, true] {
            let path = dir.path().join(format!("{i}-{reverse}.parquet"));
            let mut values = vec![vec![Integer(integer)], vec![Real(2.5)], vec![Null]];
            if reverse {
                values.reverse();
            }
            let written = write_table(
                &path,
                "c",
                "t",
                &[actor(&["amount"], values)],
                &BTreeMap::new(),
            )
            .unwrap();
            let safe = integer.abs() <= 1 << 53;
            assert_eq!(
                written.columns["amount"],
                if safe {
                    ColumnType::Real
                } else {
                    ColumnType::Text
                }
            );
            assert_eq!(written.warnings.is_empty(), safe);
            let rows = read(&path);
            assert_eq!(
                rows[if reverse { 2 } else { 0 }]["amount"],
                if safe {
                    Field::Double(integer as f64)
                } else {
                    Field::Str(integer.to_string())
                }
            );
        }
    }
    for (i, value) in [Integer(7), Integer((1 << 53) + 1), Null]
        .into_iter()
        .enumerate()
    {
        let path = dir.path().join(format!("sticky-{i}.parquet"));
        let written = write_table(
            &path,
            "c",
            "t",
            &[actor(&["AMOUNT"], vec![vec![value]])],
            &BTreeMap::from([("amount".into(), ColumnType::Real)]),
        )
        .unwrap();
        assert_eq!(
            written.columns["AMOUNT"],
            if i == 1 {
                ColumnType::Text
            } else {
                ColumnType::Real
            }
        );
    }
}
