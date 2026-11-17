//! ADR 0039: one read-only poll with durable, resumable batches.
//! Kinds describe replacement labels, incarnations and retained ancestry.
mod views;
pub mod writer;

use anyhow::{Context, ensure};
use clap::ValueEnum;
use futures_util::{StreamExt, TryStreamExt};
use object_store::{ObjectMeta, ObjectStore, path::Path as Key};
use rusqlite::{Connection, OptionalExtension, params};
use std::{collections::BTreeMap, fs::OpenOptions, path::PathBuf, sync::Arc};
use wasmkeep_cluster::replication::{Listing, SegmentId, plan_restore, segment_key, snapshot_key};
use wasmkeep_server::history;
use wasmkeep_storage::{Rows, SqlValue, decode_segment, encode_segment};

#[derive(Debug, Clone, Copy, ValueEnum)]
enum Target {
    Parquet,
}

/// Export changed actor states; runs one poll and only reads the bucket.
#[derive(Debug, clap::Args)]
pub struct Args {
    #[arg(value_name = "TENANT/APP", value_parser = crate::parse_app)]
    app: (String, String),
    #[arg(long)]
    bucket: String,
    #[arg(long, value_enum)]
    to: Target,
    #[arg(long)]
    out: PathBuf,
    #[arg(long)]
    state: PathBuf,
    #[arg(long, value_delimiter = ',')]
    classes: Vec<String>,
    #[arg(long, default_value_t = 2 << 30)]
    max_bytes: u64,
}

pub fn run(args: &Args) -> anyhow::Result<()> {
    let mut code = 0;
    crate::run_async(async {
        let (store, prefix) = crate::open_bucket_unchecked(&args.bucket)?;
        code = poll(args, &store, &prefix).await?;
        Ok(())
    })?;
    if code != 0 {
        std::process::exit(code);
    }
    Ok(())
}

fn head(objects: &BTreeMap<String, ObjectMeta>) -> anyhow::Result<(SegmentId, String)> {
    let listing = Listing::from_keys(objects.keys().map(String::as_str));
    let position = plan_restore(&listing)?.head();
    ensure!(position.txid != 0, "actor has no retained history");
    let key = listing
        .segments
        .iter()
        .find(|s| s.id == position)
        .map(segment_key)
        .unwrap_or_else(|| snapshot_key(position));
    let meta = objects.get(&key).context("head metadata missing")?;
    let identity = match &meta.e_tag {
        Some(tag) => serde_json::json!(["etag", tag]),
        None => serde_json::json!(["size-time", meta.size, meta.last_modified.to_rfc3339()]),
    };
    Ok((position, identity.to_string()))
}

struct Changed {
    class: String,
    encoded: String,
    position: SegmentId,
    identity: String,
    label: Option<String>,
    incarnation: String,
    state: writer::State,
    tables: Vec<(String, Rows)>,
}

/// Inject a local test bucket, or the CLI's S3 store. Returns 2 for actors left
/// unadvanced. This entry point never obtains a node or writes to the store.
pub async fn poll(args: &Args, store: &Arc<dyn ObjectStore>, prefix: &str) -> anyhow::Result<i32> {
    poll_inner(args, store, prefix, &mut |_| Ok(()), 10_000, 1 << 30).await
}

/// Deterministic publication fault injection for integration tests.
#[doc(hidden)]
pub async fn poll_with_checkpoint(
    args: &Args,
    store: &Arc<dyn ObjectStore>,
    prefix: &str,
    checkpoint: &mut dyn FnMut(&str) -> anyhow::Result<()>,
) -> anyhow::Result<i32> {
    poll_inner(args, store, prefix, checkpoint, 1, 1 << 30).await
}

async fn poll_inner(
    args: &Args,
    store: &Arc<dyn ObjectStore>,
    prefix: &str,
    checkpoint: &mut dyn FnMut(&str) -> anyhow::Result<()>,
    max_actors: usize,
    max_rows: usize,
) -> anyhow::Result<i32> {
    durable_directory(&args.state)?;
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(args.state.join("lock"))?;
    lock.try_lock().context("feed state is locked")?;
    durable_directory(&args.out)?;
    let out = args.out.canonicalize()?;
    let state = args.state.canonicalize()?;
    ensure!(
        !out.starts_with(&state) && !state.starts_with(&out),
        "output and state directories must be separate"
    );
    let mut db = Connection::open(state.join("state.db"))?;
    db.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; CREATE TABLE IF NOT EXISTS feed (id INTEGER PRIMARY KEY CHECK(id=1), binding TEXT NOT NULL, incarnation TEXT NOT NULL, seq INTEGER NOT NULL, pending INTEGER NOT NULL); CREATE TABLE IF NOT EXISTS heads (class TEXT NOT NULL, name TEXT NOT NULL, generation INTEGER NOT NULL, txid INTEGER NOT NULL, identity TEXT NOT NULL, PRIMARY KEY(class,name)); CREATE INDEX IF NOT EXISTS heads_order ON heads((class || '/' || name || '/'));")?;
    // Upgrade F2 progress without changing its binding or committed sequence.
    let has_reserved: bool = db.query_row(
        "SELECT EXISTS(SELECT 1 FROM pragma_table_info('feed') WHERE name='reserved')",
        [],
        |r| r.get(0),
    )?;
    if !has_reserved {
        let tx = db.transaction()?;
        tx.execute_batch("ALTER TABLE feed ADD COLUMN reserved INTEGER NOT NULL DEFAULT 0; UPDATE feed SET reserved=seq+pending;")?;
        tx.commit()?;
    }
    let has_head_seq: bool = db.query_row(
        "SELECT EXISTS(SELECT 1 FROM pragma_table_info('heads') WHERE name='seq')",
        [],
        |r| r.get(0),
    )?;
    if !has_head_seq {
        db.execute_batch("ALTER TABLE heads ADD COLUMN seq INTEGER NOT NULL DEFAULT 0;")?;
    }
    for (column, definition) in [
        ("label", "TEXT"),
        ("kind", "TEXT NOT NULL DEFAULT 'state'"),
        ("incarnation", "TEXT NOT NULL DEFAULT ''"),
    ] {
        let exists: bool = db.query_row(
            "SELECT EXISTS(SELECT 1 FROM pragma_table_info('heads') WHERE name=?1)",
            [column],
            |r| r.get(0),
        )?;
        if !exists {
            db.execute_batch(&format!(
                "ALTER TABLE heads ADD COLUMN {column} {definition}"
            ))?;
            if column == "incarnation" {
                db.execute_batch("UPDATE heads SET incarnation=coalesce((SELECT incarnation FROM feed WHERE id=1),'')")?;
            }
        }
    }
    std::fs::File::open(&state)?.sync_all()?;
    db.execute_batch("CREATE TABLE IF NOT EXISTS columns (class TEXT NOT NULL, table_name TEXT NOT NULL, name TEXT NOT NULL COLLATE NOCASE, kind INTEGER NOT NULL, PRIMARY KEY(class,table_name,name));")?;
    let binding = serde_json::json!({"format":1,"bucket":args.bucket,"prefix":prefix,"tenant":args.app.0,"app":args.app.1,"out":out,"feed_id":state}).to_string();
    let recorded: Option<(String, String, u64, bool)> = db
        .query_row(
            "SELECT binding, incarnation, seq, pending FROM feed WHERE id=1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get::<_, i64>(2)? as u64, r.get(3)?)),
        )
        .optional()?;
    if let Some((old, _, _, _)) = &recorded {
        ensure!(
            old == &binding,
            "state belongs to another bucket, app or output"
        );
        if !out.join("_wk_feed.json").exists() {
            let reserved: i64 = db.query_row("SELECT reserved FROM feed", [], |r| r.get(0))?;
            ensure!(reserved == 0, "output identity missing after publication");
            publish_identity(&out, &binding)?;
        }
        ensure!(
            std::fs::read_to_string(out.join("_wk_feed.json"))? == binding,
            "output identity differs from state"
        );
    } else {
        ensure!(
            std::fs::read_dir(&out)?.next().is_none(),
            "new state requires an empty output directory"
        );
    }
    if let Some((_, _, committed, _)) = &recorded {
        recover(&out, *committed)?;
        recover_visible(&mut db, &out, *committed)?;
        views::publish(&out)?;
    }
    let scratch = state.join("scratch");
    if scratch.exists() {
        std::fs::remove_dir_all(&scratch)?;
    }
    std::fs::create_dir_all(&scratch)?;
    let (tenant, app) = &args.app;
    let base = format!(
        "{}apps/{tenant}/{app}",
        if prefix.is_empty() {
            String::new()
        } else {
            format!("{}/", prefix.trim_end_matches('/'))
        }
    );
    let mut markers = Vec::new();
    for marker in ["restoring", "deleted", "recreated", "restored"] {
        let bytes = match history::get(
            store,
            &Key::from(format!("{base}/{marker}")),
            None,
            64 << 10,
        )
        .await
        {
            Ok(bytes) => Some(bytes.to_vec()),
            Err(history::Error::Missing(_)) => None,
            Err(e) => return Err(e.into()),
        };
        markers.push(bytes);
    }
    if markers[0].is_some() || markers[1].is_some() {
        eprintln!("app is restoring or deleted; poll writes nothing");
        return Ok(2);
    }
    let incarnation = serde_json::to_string(&markers[2..])?;
    let app_changed = recorded
        .as_ref()
        .is_some_and(|(_, old, _, _)| old != &incarnation);
    if recorded.is_none() {
        db.execute(
            "INSERT INTO feed VALUES(1,?1,?2,0,0,0)",
            params![binding, incarnation],
        )?;
        publish_identity(&out, &binding)?;
    }
    let root = wasmkeep_cluster::state_root::StateRoot::new(prefix, tenant, app);
    let history_prefix = format!(
        "{}/",
        root.bucket_key(wasmkeep_cluster::state_root::Family::History, "")
    );
    let history_prefix = format!("{}/", history_prefix.trim_end_matches('/'));
    let mut failed = false;
    let mut changed = Vec::new();
    let mut row_bytes = 0usize;
    {
        // Both scans use the bucket's byte ordering, including the slash after
        // each class/name. Only one recorded head is held ahead of the listing.
        let scan = Connection::open(state.join("state.db"))?;
        let mut statement = scan.prepare("SELECT class || '/' || name || '/', class, generation, txid, identity, label, kind, incarnation FROM heads ORDER BY class || '/' || name || '/'")?;
        let mut rows = statement.query([])?;
        let saved = next_saved(&mut rows)?;
        let groups = crate::actor_groups::actor_groups_with_errors(store, &history_prefix);
        futures_util::pin_mut!(groups);
        {
            let history_prefix = history_prefix.as_str();
            // Merge one saved row and one listed group at a time, including
            // absent actors. The same bounded reader handles either case.
            let merged = futures_util::stream::try_unfold(
                (groups, rows, saved, None),
                |(mut groups, mut rows, mut saved, mut group)| async move {
                    if group.is_none() {
                        group = groups.try_next().await?;
                    }
                    let take_saved = saved.as_ref().is_some_and(|s| {
                        group.as_ref().is_none_or(|(prefix, _)| {
                            format!("{history_prefix}{}", s.key) < *prefix
                        })
                    });
                    let job = if take_saved {
                        let old = saved.take().context("saved head missing")?;
                        let prefix = format!("{history_prefix}{}", old.key);
                        saved = next_saved(&mut rows)?;
                        (prefix, Ok(BTreeMap::new()), Some(old), true)
                    } else if let Some((prefix, objects)) = group.take() {
                        let old = if saved
                            .as_ref()
                            .is_some_and(|s| format!("{history_prefix}{}", s.key) == prefix)
                        {
                            let old = saved.take();
                            saved = next_saved(&mut rows)?;
                            old
                        } else {
                            None
                        };
                        (prefix, objects, old, false)
                    } else {
                        return Ok::<_, anyhow::Error>(None);
                    };
                    Ok(Some((job, (groups, rows, saved, group))))
                },
            );
            let jobs = merged
                .map_ok(|(actor_prefix, objects, old, missing)| {
                    read_changed(
                        args,
                        store,
                        &scratch,
                        history_prefix,
                        actor_prefix,
                        objects,
                        old,
                        &incarnation,
                        app_changed,
                        missing,
                    )
                })
                .try_buffer_unordered(16);
            futures_util::pin_mut!(jobs);
            while let Some(result) = jobs.next().await {
                match result {
                    Ok(Some(actor)) => {
                        let actor_bytes = actor
                            .tables
                            .iter()
                            .flat_map(|(_, r)| &r.values)
                            .flatten()
                            .map(|v| match v {
                                SqlValue::Text(s) => s.len(),
                                SqlValue::Blob(b) => b.len(),
                                _ => 8,
                            })
                            .sum::<usize>();
                        if !changed.is_empty()
                            && (changed.len() >= max_actors
                                || row_bytes.saturating_add(actor_bytes) > max_rows)
                        {
                            commit_batch(&mut db, &out, std::mem::take(&mut changed), checkpoint)?;
                            row_bytes = 0;
                        }
                        row_bytes += actor_bytes;
                        changed.push(actor);
                        checkpoint("listed")?;
                    }
                    Ok(None) => (),
                    Err(e) => {
                        eprintln!("feed actor left for next poll: {e:#}");
                        failed = true;
                    }
                }
            }
        }
    }
    if !changed.is_empty() {
        commit_batch(&mut db, &out, changed, checkpoint)?;
    }
    if !failed {
        db.execute("UPDATE feed SET incarnation=?1 WHERE id=1", [&incarnation])?;
    }
    if !out.join("views.duckdb.sql").exists() {
        views::publish(&out)?;
    }
    Ok(if failed { 2 } else { 0 })
}

fn commit_batch(
    db: &mut Connection,
    out: &std::path::Path,
    mut changed: Vec<Changed>,
    checkpoint: &mut dyn FnMut(&str) -> anyhow::Result<()>,
) -> anyhow::Result<()> {
    let tx = db.transaction()?;
    let reserved: i64 = tx.query_row("SELECT reserved FROM feed WHERE id=1", [], |r| r.get(0))?;
    let seq = reserved.checked_add(1).context("sequence exhausted")?;
    tx.execute("UPDATE feed SET reserved=?1,pending=1 WHERE id=1", [seq])?;
    tx.commit()?;
    checkpoint("reserved")?;
    for actor in &mut changed {
        actor.state.actor.seq = seq as u64;
    }
    let mut tables: BTreeMap<(String, String), Vec<writer::ActorTable>> = BTreeMap::new();
    for actor in &mut changed {
        for (table, rows) in std::mem::take(&mut actor.tables) {
            let mut data = actor.state.actor.clone();
            data.rows = rows;
            tables
                .entry((actor.class.clone(), table))
                .or_default()
                .push(data);
        }
    }
    let mut column_updates = Vec::new();
    for ((class, table), actors) in tables {
        let dir = out
            .join("tables")
            .join(encode_segment(&class))
            .join(encode_segment(&table));
        durable_directory(&dir)?;
        let previous = db
            .prepare("SELECT name,kind FROM columns WHERE class=?1 AND table_name=?2")?
            .query_map(params![class, table], |r| {
                Ok((r.get::<_, String>(0)?, r.get::<_, u8>(1)?))
            })?
            .map(|r| {
                let (name, kind) = r?;
                Ok((name, decode_type(kind)?))
            })
            .collect::<anyhow::Result<BTreeMap<_, _>>>()?;
        let written = writer::write_table(
            &dir.join(format!("{seq:020}.parquet")),
            &class,
            &table,
            &actors,
            &previous,
        )?;
        column_updates.push((class, table, written.columns));
        checkpoint("data")?;
        for warning in written.warnings {
            eprintln!("{warning}");
        }
    }
    checkpoint("data_complete")?;
    let states_dir = out.join("_wk_states");
    durable_directory(&states_dir)?;
    let states = changed.iter().map(|c| &c.state).collect::<Vec<_>>();
    writer::write_states(&states_dir.join(format!("{seq:020}.parquet")), &states)?;
    checkpoint("states")?;
    checkpoint("before_state")?;
    let tx = db.transaction()?;
    for (class, table, columns) in column_updates {
        for (name, kind) in columns {
            tx.execute("INSERT INTO columns VALUES(?1,?2,?3,?4) ON CONFLICT(class,table_name,name) DO UPDATE SET kind=excluded.kind",params![class,table,name,kind as u8])?;
        }
    }
    for actor in &changed {
        tx.execute(
            "INSERT OR REPLACE INTO heads (class,name,generation,txid,identity,seq,label,kind,incarnation) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9)",
            params![
                actor.class,
                actor.encoded,
                actor.position.generation as i64,
                actor.position.txid as i64,
                actor.identity,
                seq, actor.label, actor.state.kind, actor.incarnation
            ],
        )?;
        checkpoint("head")?;
    }
    tx.execute("UPDATE feed SET seq=?1,pending=0 WHERE id=1", [seq])?;
    tx.commit()?;
    checkpoint("advanced")?;
    views::publish(out)?;
    Ok(())
}

// Sync each newly created directory's entry, not only the leaf containing files.
fn durable_directory(path: &std::path::Path) -> anyhow::Result<()> {
    if path.is_dir() {
        return Ok(());
    }
    if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
        durable_directory(parent)?;
        std::fs::create_dir(path)?;
        std::fs::File::open(parent)?.sync_all()?;
    } else {
        std::fs::create_dir(path)?;
        std::fs::File::open(".")?.sync_all()?;
    }
    Ok(())
}

fn publish_identity(out: &std::path::Path, binding: &str) -> anyhow::Result<()> {
    use std::io::Write;
    let partial = out.join("_wk_feed.json.partial");
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&partial)?;
    file.write_all(binding.as_bytes())?;
    file.sync_all()?;
    drop(file);
    tempfile::TempPath::try_from_path(partial)?.persist_noclobber(out.join("_wk_feed.json"))?;
    std::fs::File::open(out)?.sync_all()?;
    Ok(())
}

fn decode_type(kind: u8) -> anyhow::Result<writer::ColumnType> {
    use writer::ColumnType::*;
    match kind {
        0 => Ok(Integer),
        1 => Ok(Real),
        2 => Ok(Text),
        3 => Ok(Blob),
        _ => anyhow::bail!("invalid recorded column type {kind}"),
    }
}

fn recover(out: &std::path::Path, committed: u64) -> anyhow::Result<()> {
    fn sweep(dir: &std::path::Path, out: &std::path::Path, committed: u64) -> anyhow::Result<()> {
        if !dir.exists() {
            return Ok(());
        }
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            let kind = entry.file_type()?;
            ensure!(
                !kind.is_symlink(),
                "symlink in feed output: {}",
                path.display()
            );
            if kind.is_dir() {
                sweep(&path, out, committed)?;
                continue;
            }
            let name = entry.file_name();
            let name = name.to_str().context("invalid feed filename")?;
            let partial = name.ends_with(".partial");
            let stem = name
                .trim_end_matches(".partial")
                .strip_suffix(".parquet")
                .context("invalid feed file")?;
            let seq: u64 = stem.parse().context("invalid feed sequence")?;
            if partial
                || (seq > committed
                    && !out
                        .join("_wk_states")
                        .join(format!("{seq:020}.parquet"))
                        .exists())
            {
                std::fs::remove_file(&path)?;
                std::fs::File::open(dir)?.sync_all()?;
            }
        }
        Ok(())
    }
    for name in ["_wk_feed.json.partial", "views.duckdb.sql.partial"] {
        let partial = out.join(name);
        if partial.exists() {
            std::fs::remove_file(partial)?;
            std::fs::File::open(out)?.sync_all()?;
        }
    }
    sweep(&out.join("tables"), out, committed)?;
    sweep(&out.join("_wk_states"), out, committed)
}

/// Restore bookkeeping from reader-visible batches whose progress transaction
/// rolled back. Empty identities force verification against the current bucket.
fn recover_visible(
    db: &mut Connection,
    out: &std::path::Path,
    committed: u64,
) -> anyhow::Result<()> {
    use parquet::{
        basic::Type as PhysicalType,
        file::reader::{FileReader, SerializedFileReader},
        record::RowAccessor,
    };
    let dir = out.join("_wk_states");
    if !dir.exists() {
        return Ok(());
    }
    let mut batches = std::fs::read_dir(dir)?
        .map(|entry| Ok(entry?.path()))
        .collect::<anyhow::Result<Vec<_>>>()?;
    batches.sort();
    for path in batches {
        let seq: u64 = path
            .file_stem()
            .and_then(|s| s.to_str())
            .context("invalid state filename")?
            .parse()?;
        if seq <= committed {
            continue;
        }
        let tx = db.transaction()?;
        let reader = SerializedFileReader::new(std::fs::File::open(&path)?)?;
        // Process one state row and one table schema at a time, not all data rows.
        let mut tables = std::collections::BTreeSet::new();
        // Existing heads already guarantee rediscovery; retain their precise
        // labels/identities so retry classification uses the pre-crash position.
        // A visible resurrection must clear an old deletion to allow absence
        // detection if the actor disappears again before recovery.
        for row in reader.get_row_iter(None)? {
            let row = row?;
            let class = row.get_string(2)?;
            let name = encode_segment(row.get_string(3)?);
            tx.execute(
                "INSERT INTO heads (class,name,generation,txid,identity,seq,label,kind,incarnation) VALUES(?1,?2,?3,?4,'',?5,NULL,?6,(SELECT incarnation FROM feed WHERE id=1)) ON CONFLICT(class,name) DO UPDATE SET kind=excluded.kind WHERE heads.kind='deleted' AND excluded.kind!='deleted'",
                params![class, name, row.get_ulong(7)? as i64, row.get_ulong(8)? as i64, seq as i64, row.get_string(5)?],
            )?;
            for table in serde_json::from_str::<Vec<String>>(row.get_string(10)?)? {
                tables.insert((class.clone(), table));
            }
        }
        for (class, table) in tables {
            let file = out
                .join("tables")
                .join(encode_segment(&class))
                .join(encode_segment(&table))
                .join(format!("{seq:020}.parquet"));
            let reader = SerializedFileReader::new(std::fs::File::open(file)?)?;
            for column in reader.metadata().file_metadata().schema_descr().columns() {
                let output = column.name();
                let lower = output.to_ascii_lowercase();
                if lower.starts_with("_wk_") {
                    continue;
                }
                let name = if lower.starts_with("_app_wk_") {
                    &output[4..]
                } else {
                    output
                };
                let kind = match column.physical_type() {
                    PhysicalType::INT64 => writer::ColumnType::Integer,
                    PhysicalType::DOUBLE => writer::ColumnType::Real,
                    PhysicalType::BYTE_ARRAY if column.logical_type_ref().is_some() => {
                        writer::ColumnType::Text
                    }
                    PhysicalType::BYTE_ARRAY => writer::ColumnType::Blob,
                    other => anyhow::bail!("unsupported recovered feed type: {other:?}"),
                };
                tx.execute("INSERT INTO columns VALUES(?1,?2,?3,?4) ON CONFLICT(class,table_name,name) DO UPDATE SET kind=CASE WHEN columns.kind=2 THEN 2 WHEN columns.kind=1 AND excluded.kind=0 THEN 1 ELSE excluded.kind END", params![class,table,name,kind as u8])?;
            }
        }
        tx.commit()?;
    }
    Ok(())
}

struct Saved {
    key: String,
    position: SegmentId,
    identity: String,
    label: Option<String>,
    kind: String,
    incarnation: String,
}
fn next_saved(rows: &mut rusqlite::Rows<'_>) -> anyhow::Result<Option<Saved>> {
    rows.next()?
        .map(|r| {
            Ok(Saved {
                key: r.get(0)?,
                position: SegmentId {
                    generation: r.get::<_, i64>(2)? as u64,
                    txid: r.get::<_, i64>(3)? as u64,
                },
                identity: r.get(4)?,
                label: r.get(5)?,
                kind: r.get(6)?,
                incarnation: r.get(7)?,
            })
        })
        .transpose()
}
#[allow(clippy::too_many_arguments)]
async fn read_changed(
    args: &Args,
    store: &Arc<dyn ObjectStore>,
    scratch: &std::path::Path,
    history_prefix: &str,
    actor_prefix: String,
    objects: anyhow::Result<BTreeMap<String, ObjectMeta>>,
    old: Option<Saved>,
    incarnation: &str,
    app_changed: bool,
    missing: bool,
) -> anyhow::Result<Option<Changed>> {
    let mut objects = objects?;
    let (tenant, app) = &args.app;
    let relative = actor_prefix
        .strip_prefix(history_prefix)
        .context("actor outside app")?
        .trim_end_matches('/');
    let (class, encoded) = relative.split_once('/').context("invalid actor prefix")?;
    if !args.classes.is_empty() && !args.classes.iter().any(|c| c == class) {
        return Ok(None);
    }
    let new_incarnation = old
        .as_ref()
        .map_or(app_changed, |o| o.incarnation != incarnation);
    if missing {
        let saved = old.as_ref().context("absent actor without recorded head")?;
        if saved.kind == "deleted" {
            return Ok(None);
        }
        if !new_incarnation {
            objects = history::list_objects(store, &actor_prefix).await?;
        }
        if objects.is_empty() {
            if !new_incarnation {
                eprintln!("{relative}: absent from app and actor listings");
            }
            return Ok(Some(Changed {
                class: class.into(),
                encoded: encoded.into(),
                position: saved.position,
                identity: String::new(),
                label: saved.label.clone(),
                incarnation: incarnation.into(),
                state: writer::State {
                    actor: writer::ActorTable {
                        tenant: tenant.clone(),
                        app: app.clone(),
                        name: decode_segment(encoded).context("invalid encoded actor name")?,
                        seq: 0,
                        generation: saved.position.generation,
                        txid: saved.position.txid,
                        committed_at: 0,
                        rows: Rows {
                            columns: vec![],
                            values: vec![],
                        },
                    },
                    class: class.into(),
                    tables: vec![],
                    emitted_at: now_ms()?,
                    kind: "deleted".into(),
                    reason: Some("absent".into()),
                },
                tables: vec![],
            }));
        }
    }
    // An incomplete plan is retried by the reader below.
    if !new_incarnation
        && let Ok((position, identity)) = head(&objects)
        && old.as_ref().is_some_and(|o| {
            o.position == position && o.identity == identity && !(o.kind == "deleted" && missing)
        })
    {
        return Ok(None);
    }
    let temp = tempfile::tempdir_in(scratch)?;
    let path = temp.path().join("actor.db");
    let (export, objects) =
        history::export_from_listing(store, &actor_prefix, objects, &path, args.max_bytes).await?;
    let (position, identity) = head(&objects)?;
    let label = export.verification.replaced.clone();
    let (kind, reason) = classify(
        old.as_ref(),
        position,
        &identity,
        label.as_deref(),
        new_incarnation,
        &objects,
    );
    let name = decode_segment(encoded).context("invalid encoded actor name")?;
    let connection =
        Connection::open_with_flags(&path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    connection.pragma_update(None, "trusted_schema", false)?;
    let tables = connection.prepare("SELECT name FROM pragma_table_list WHERE schema='main' AND type='table' AND lower(substr(name,1,7)) != 'sqlite_' AND lower(substr(name,1,10)) != '_wasmkeep_' ORDER BY name")?.query_map([], |r| r.get::<_,String>(0))?.collect::<Result<Vec<_>,_>>()?;
    let mut data = Vec::new();
    for table in &tables {
        let mut stmt =
            connection.prepare(&format!("SELECT * FROM \"{}\"", table.replace('"', "\"\"")))?;
        let columns = stmt.column_names().map(str::to_owned).collect::<Vec<_>>();
        // Reserve escaped metadata names for the escaping rule, even across
        // actors in the same table batch, so publication cannot fail later.
        ensure!(
            !columns
                .iter()
                .any(|name| name.to_ascii_lowercase().starts_with("_app_wk_")),
            "{relative}: table {table} has reserved escaped column prefix _app_wk_"
        );
        let count = columns.len();
        let values = stmt
            .query_map([], |row| {
                (0..count)
                    .map(|i| {
                        use rusqlite::types::ValueRef;
                        Ok(match row.get_ref(i)? {
                            ValueRef::Null => SqlValue::Null,
                            ValueRef::Integer(v) => SqlValue::Integer(v),
                            ValueRef::Real(v) => SqlValue::Real(v),
                            ValueRef::Text(v) => {
                                SqlValue::Text(String::from_utf8_lossy(v).into_owned())
                            }
                            ValueRef::Blob(v) => SqlValue::Blob(v.to_vec()),
                        })
                    })
                    .collect::<Result<Vec<_>, rusqlite::Error>>()
            })?
            .collect::<Result<Vec<_>, _>>()?;
        data.push((table.clone(), Rows { columns, values }));
    }
    let actor = writer::ActorTable {
        tenant: tenant.clone(),
        app: app.clone(),
        name,
        seq: 0, // Assigned only after the batch reserves its number.
        generation: position.generation,
        txid: position.txid,
        committed_at: export.timestamp_ms,
        rows: Rows {
            columns: vec![],
            values: vec![],
        },
    };
    let emitted_at = now_ms()?;
    Ok::<_, anyhow::Error>(Some(Changed {
        class: class.into(),
        encoded: encoded.into(),
        position,
        identity,
        label,
        incarnation: incarnation.into(),
        state: writer::State {
            actor,
            class: class.into(),
            tables,
            emitted_at,
            kind: kind.into(),
            reason: reason.map(str::to_owned),
        },
        tables: data,
    }))
}

#[cfg(test)]
mod poll_tests;

fn now_ms() -> anyhow::Result<i64> {
    Ok(i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis(),
    )?)
}

fn classify(
    old: Option<&Saved>,
    head: SegmentId,
    identity: &str,
    label: Option<&str>,
    incarnation: bool,
    objects: &BTreeMap<String, ObjectMeta>,
) -> (&'static str, Option<&'static str>) {
    if incarnation {
        return ("rebase", Some("incarnation"));
    }
    if label != old.and_then(|o| o.label.as_deref()) {
        if let Some(label) = label {
            let mut parts = label.split_whitespace();
            let action = parts.next();
            let txid = parts.next().and_then(|n| n.parse::<u64>().ok());
            if action == Some("delete") && txid == Some(head.txid) {
                return ("deleted", Some("delete"));
            }
            return (
                "rebase",
                Some(if action == Some("delete") {
                    "delete"
                } else {
                    "restore"
                }),
            );
        }
        return ("rebase", Some("restore"));
    }
    let Some(old) = old else {
        return ("state", None);
    };
    let chain = history::ancestry(
        &Listing::from_keys(objects.keys().map(String::as_str)),
        head,
    );
    // Identity changes at an identical position cannot prove descent.
    if !(head == old.position && identity != old.identity)
        && chain.iter().any(|p| {
            *p == old.position
                || (p.generation != 0
                    && p.generation == old.position.generation
                    && p.txid >= old.position.txid)
        })
    {
        return ("state", None);
    }
    (
        "rebase",
        Some(
            if head.txid < old.position.txid || chain.iter().any(|p| p.txid == old.position.txid) {
                "fork"
            } else {
                "unproven"
            },
        ),
    )
}
