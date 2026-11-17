//! Local compaction: reserve and journal, publish an F3 batch, then reclaim.
use super::*;
use parquet::{
    basic::Type,
    file::reader::{FileReader, SerializedFileReader},
    record::{Field, Row, RowAccessor},
};
use std::{fs::File, path::Path};

pub(super) fn compact(
    out: &Path,
    state: &Path,
    checkpoint: &mut dyn FnMut(&str) -> anyhow::Result<()>,
) -> anyhow::Result<()> {
    let out = out.canonicalize()?;
    let state = state.canonicalize()?;
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .open(state.join("lock"))?;
    lock.try_lock().context("feed state is locked")?;
    let mut db = Connection::open_with_flags(
        state.join("state.db"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE,
    )?;
    db.execute_batch("PRAGMA synchronous=FULL;")?;
    let binding: String = db.query_row("SELECT binding FROM feed WHERE id=1", [], |r| r.get(0))?;
    let identity: serde_json::Value = serde_json::from_str(&binding)?;
    ensure!(
        identity["out"] == out.to_str().context("non-UTF8 output")?
            && identity["feed_id"] == state.to_str().context("non-UTF8 state")?,
        "state belongs to another output directory"
    );
    ensure!(
        std::fs::read_to_string(out.join("_wk_feed.json"))? == binding,
        "output identity differs from state"
    );
    recover_compaction(&mut db, &out, &mut |_| Ok(()))?;
    let committed = db.query_row("SELECT seq FROM feed", [], |r| {
        r.get::<_, i64>(0).map(|n| n as u64)
    })?;
    recover(&out, committed)?;
    recover_visible(&mut db, &out, committed)?;

    // Use reader-visible heads, including an F3 batch whose progress transaction
    // rolled back. Such an actor retains an unknown identity for the next poll.
    let mut latest = BTreeMap::new();
    for path in parquet_files(&out.join("_wk_states"))? {
        for row in reader(&path)?.get_row_iter(None)? {
            let row = row?;
            let key = (
                row.get_string(2)?.clone(),
                encode_segment(row.get_string(3)?),
            );
            latest.insert(key, row);
        }
    }
    if latest.is_empty() {
        views::publish(&out)?;
        return Ok(());
    }
    let mut changed = Vec::new();
    for ((class, encoded), row) in latest {
        let seq = row.get_ulong(4)?;
        let (old_seq, identity, label, incarnation): (i64, String, Option<String>, String) = db
            .query_row(
                "SELECT seq,identity,label,incarnation FROM heads WHERE class=?1 AND name=?2",
                params![class, encoded],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )?;
        let state = writer::State {
            actor: writer::ActorTable {
                tenant: row.get_string(0)?.clone(),
                app: row.get_string(1)?.clone(),
                name: row.get_string(3)?.clone(),
                seq,
                generation: row.get_ulong(7)?,
                txid: row.get_ulong(8)?,
                committed_at: timestamp(&row, 9)?,
                rows: Rows {
                    columns: vec![],
                    values: vec![],
                },
            },
            class: class.clone(),
            kind: row.get_string(5)?.clone(),
            reason: match row.get_column_iter().nth(6).context("missing reason")?.1 {
                Field::Null => None,
                Field::Str(s) => Some(s.clone()),
                _ => anyhow::bail!("invalid reason"),
            },
            tables: serde_json::from_str(row.get_string(10)?)?,
            emitted_at: timestamp(&row, 11)?,
        };
        changed.push(Changed {
            class,
            encoded,
            position: SegmentId {
                generation: state.actor.generation,
                txid: state.actor.txid,
            },
            identity: if old_seq as u64 == seq {
                identity
            } else {
                String::new()
            },
            label: if old_seq as u64 == seq { label } else { None },
            incarnation,
            state,
            tables: vec![],
        });
    }
    let head_count: i64 = db.query_row("SELECT count(*) FROM heads", [], |r| r.get(0))?;
    ensure!(
        head_count as usize == changed.len(),
        "a recorded head has no visible state"
    );
    let indices: BTreeMap<_, _> = changed
        .iter()
        .enumerate()
        .map(|(i, a)| ((a.class.clone(), a.state.actor.name.clone()), i))
        .collect();
    // Keep every historical table schema, even a dropped table with no remaining
    // rows, so reloading the generated SQL keeps the same set of latest views.
    let mut schemas: BTreeMap<(String, String), BTreeMap<String, (String, u8)>> = BTreeMap::new();
    for path in parquet_files(&out.join("tables"))? {
        let file_seq: u64 = path
            .file_stem()
            .and_then(|s| s.to_str())
            .context("invalid filename")?
            .parse()?;
        if !out
            .join("_wk_states")
            .join(format!("{file_seq:020}.parquet"))
            .exists()
        {
            continue;
        }
        let table_dir = path.parent().context("missing table")?;
        let class_dir = table_dir.parent().context("missing class")?;
        let decode = |p: &Path| {
            decode_segment(
                p.file_name()
                    .and_then(|s| s.to_str())
                    .context("invalid table path")?,
            )
            .context("invalid encoding")
        };
        let class = decode(class_dir)?;
        let table = decode(table_dir)?;
        let input = reader(&path)?;
        let schema = schemas.entry((class.clone(), table.clone())).or_default();
        let mut columns = Vec::new();
        for col in input.metadata().file_metadata().schema_descr().columns() {
            let name = col.name();
            if name.to_ascii_lowercase().starts_with("_wk_") {
                continue;
            }
            let name = if name.to_ascii_lowercase().starts_with("_app_wk_") {
                &name[4..]
            } else {
                name
            };
            let kind = match col.physical_type() {
                Type::INT64 => 1,
                Type::DOUBLE => 2,
                Type::BYTE_ARRAY if col.logical_type_ref().is_some() => 4,
                Type::BYTE_ARRAY => 8,
                other => anyhow::bail!("unsupported feed type {other:?}"),
            };
            schema
                .entry(name.to_ascii_lowercase())
                .and_modify(|c| c.1 |= kind)
                .or_insert((name.into(), kind));
            columns.push(name.to_owned());
        }
        let mut actors: BTreeMap<usize, Vec<Vec<SqlValue>>> = BTreeMap::new();
        for row in input.get_row_iter(None)? {
            let row = row?;
            let n = columns.len();
            let Some(&index) = indices.get(&(class.clone(), row.get_string(n + 2)?.clone())) else {
                anyhow::bail!("data actor missing from states");
            };
            let actor = &changed[index].state;
            if actor.kind == "deleted"
                || actor.actor.seq != file_seq
                || !actor.tables.contains(&table)
            {
                continue;
            }
            actors.entry(index).or_default().push(
                row.get_column_iter()
                    .take(n)
                    .map(|(_, v)| value(v))
                    .collect::<anyhow::Result<_>>()?,
            );
        }
        for (index, values) in actors {
            changed[index].tables.push((
                table.clone(),
                Rows {
                    columns: columns.clone(),
                    values,
                },
            ));
        }
    }
    // Persist equivalent visible bookkeeping before reserving. Recovery of a
    // published compaction then only needs to move every head to its new seq.
    let tx = db.transaction()?;
    for actor in &changed {
        tx.execute("UPDATE heads SET generation=?3,txid=?4,identity=?5,label=?6,kind=?7,seq=?8 WHERE class=?1 AND name=?2", params![actor.class,actor.encoded,actor.position.generation as i64,actor.position.txid as i64,actor.identity,actor.label,actor.state.kind,actor.state.actor.seq as i64])?;
    }
    for ((class, table), columns) in &schemas {
        let prior: BTreeMap<_, _> = columns
            .values()
            .map(|(name, mask)| {
                (
                    name.clone(),
                    match mask {
                        1 => writer::ColumnType::Integer,
                        2 | 3 => writer::ColumnType::Real,
                        8 => writer::ColumnType::Blob,
                        _ => writer::ColumnType::Text,
                    },
                )
            })
            .collect();
        for (name, kind) in &prior {
            tx.execute("INSERT INTO columns VALUES(?1,?2,?3,?4) ON CONFLICT(class,table_name,name) DO UPDATE SET kind=excluded.kind",params![class,table,name,*kind as u8])?;
        }
        for actor in changed.iter_mut().filter(|a| &a.class == class) {
            for (_, rows) in actor.tables.iter_mut().filter(|(t, _)| t == table) {
                for name in &mut rows.columns {
                    *name = columns[&name.to_ascii_lowercase()].0.clone();
                }
                for values in &mut rows.values {
                    for (name, value) in rows.columns.iter().zip(values) {
                        let kind = prior
                            .iter()
                            .find(|(n, _)| n.eq_ignore_ascii_case(name))
                            .context("missing column")?
                            .1;
                        if *kind == writer::ColumnType::Real
                            && let SqlValue::Integer(n) = value
                        {
                            *value = SqlValue::Real(*n as f64);
                        } else if *kind == writer::ColumnType::Text
                            && let SqlValue::Real(n) = value
                        {
                            *value = SqlValue::Text(view_double(*n)?);
                        }
                    }
                }
            }
        }
        // A zero-row table carries the schema without inventing an actor row.
        let actor = changed
            .iter_mut()
            .find(|a| &a.class == class)
            .context("table class missing from heads")?;
        actor.tables.push((
            table.clone(),
            Rows {
                columns: prior.keys().cloned().collect(),
                values: vec![],
            },
        ));
    }
    tx.commit()?;
    commit_batch_inner(&mut db, &out, changed, checkpoint, true)?;
    recover_compaction(&mut db, &out, checkpoint)
}

// Match the generated view's DOUBLE -> VARCHAR cast, rather than the SQLite
// writer's scalar text conversion (which would turn 2.0 into "2"). Both use
// shortest-roundtrip digits; DuckDB spells exponent signs and two-digit powers.
fn view_double(value: f64) -> anyhow::Result<String> {
    if value.is_nan() {
        return Ok("nan".into());
    }
    let text = format!("{value:?}");
    if let Some((mantissa, exponent)) = text.split_once('e') {
        let exponent: i32 = exponent.parse()?;
        Ok(format!("{mantissa}e{exponent:+03}"))
    } else {
        Ok(text)
    }
}

fn timestamp(row: &Row, i: usize) -> anyhow::Result<i64> {
    match row.get_column_iter().nth(i).context("missing timestamp")?.1 {
        Field::TimestampMillis(n) => Ok(*n),
        _ => anyhow::bail!("invalid timestamp"),
    }
}
fn value(field: &Field) -> anyhow::Result<SqlValue> {
    Ok(match field {
        Field::Null => SqlValue::Null,
        Field::Long(n) => SqlValue::Integer(*n),
        Field::Double(n) => SqlValue::Real(*n),
        Field::Str(s) => SqlValue::Text(s.clone()),
        Field::Bytes(b) => SqlValue::Blob(b.data().to_vec()),
        _ => anyhow::bail!("invalid app value {field:?}"),
    })
}
fn reader(path: &Path) -> anyhow::Result<SerializedFileReader<File>> {
    Ok(SerializedFileReader::new(File::open(path)?)?)
}
fn parquet_files(dir: &Path) -> anyhow::Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    if dir.exists() {
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            ensure!(!entry.file_type()?.is_symlink(), "symlink in feed output");
            let path = entry.path();
            if path.is_dir() {
                files.extend(parquet_files(&path)?);
            } else if path.extension().is_some_and(|e| e == "parquet") {
                files.push(path);
            }
        }
    }
    files.sort();
    Ok(files)
}

pub(super) fn recover_compaction(
    db: &mut Connection,
    out: &Path,
    checkpoint: &mut dyn FnMut(&str) -> anyhow::Result<()>,
) -> anyhow::Result<()> {
    let exists: bool = db.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name='compaction' AND type='table')",
        [],
        |r| r.get(0),
    )?;
    if !exists {
        return Ok(());
    }
    let seq: Option<u64> = db
        .query_row("SELECT seq FROM compaction", [], |r| {
            r.get::<_, i64>(0).map(|n| n as u64)
        })
        .optional()?;
    let Some(seq) = seq else {
        return Ok(());
    };
    if out
        .join("_wk_states")
        .join(format!("{seq:020}.parquet"))
        .exists()
    {
        // The states rename is the switch. Complete it, even if F3's SQLite
        // transaction did not commit. Identities and labels were saved earlier.
        recover_visible(db, out, seq - 1)?;
        let tx = db.transaction()?;
        tx.execute("UPDATE heads SET seq=?1", [seq as i64])?;
        tx.execute("UPDATE feed SET seq=?1,pending=0", [seq as i64])?;
        tx.commit()?;
        checkpoint("switched")?;
        views::publish_from(out, seq)?;
        checkpoint("views")?;
        for dir in ["tables", "_wk_states"] {
            for path in parquet_files(&out.join(dir))? {
                let number: u64 = path
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .context("invalid sequence")?
                    .parse()?;
                if number < seq {
                    std::fs::remove_file(&path)?;
                    File::open(path.parent().context("missing parent")?)?.sync_all()?;
                    checkpoint("removed")?;
                }
            }
        }
        checkpoint("cleaned")?;
    } else {
        let committed = db.query_row("SELECT seq FROM feed", [], |r| {
            r.get::<_, i64>(0).map(|n| n as u64)
        })?;
        recover(out, committed)?;
    }
    db.execute("DELETE FROM compaction", [])?;
    checkpoint("finished")?;
    Ok(())
}
