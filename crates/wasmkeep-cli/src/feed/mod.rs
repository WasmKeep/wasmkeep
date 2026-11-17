//! ADR 0039 F2: a single read-only poll. Interrupted publication requires new
//! directories until F3 adds recovery; replacement kinds remain gated on F4.
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
    state: writer::State,
    tables: Vec<(String, Rows)>,
}

/// Inject a local test bucket, or the CLI's S3 store. Returns 2 for actors left
/// unadvanced. This entry point never obtains a node or writes to the store.
pub async fn poll(args: &Args, store: &Arc<dyn ObjectStore>, prefix: &str) -> anyhow::Result<i32> {
    std::fs::create_dir_all(&args.state)?;
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(args.state.join("lock"))?;
    lock.try_lock().context("feed state is locked")?;
    std::fs::create_dir_all(&args.out)?;
    let out = args.out.canonicalize()?;
    let state = args.state.canonicalize()?;
    ensure!(
        !out.starts_with(&state) && !state.starts_with(&out),
        "output and state directories must be separate"
    );
    let mut db = Connection::open(state.join("state.db"))?;
    db.execute_batch("PRAGMA synchronous=FULL; CREATE TABLE IF NOT EXISTS feed (id INTEGER PRIMARY KEY CHECK(id=1), binding TEXT NOT NULL, incarnation TEXT NOT NULL, seq INTEGER NOT NULL, pending INTEGER NOT NULL); CREATE TABLE IF NOT EXISTS heads (class TEXT NOT NULL, name TEXT NOT NULL, generation INTEGER NOT NULL, txid INTEGER NOT NULL, identity TEXT NOT NULL, PRIMARY KEY(class,name)); CREATE INDEX IF NOT EXISTS heads_order ON heads((class || '/' || name || '/'));")?;
    let binding = serde_json::json!({"format":1,"bucket":args.bucket,"prefix":prefix,"tenant":args.app.0,"app":args.app.1,"out":out,"feed_id":state}).to_string();
    let recorded: Option<(String, String, u64, bool)> = db
        .query_row(
            "SELECT binding, incarnation, seq, pending FROM feed WHERE id=1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get::<_, i64>(2)? as u64, r.get(3)?)),
        )
        .optional()?;
    if let Some((old, _, _, pending)) = &recorded {
        ensure!(
            old == &binding,
            "state belongs to another bucket, app or output"
        );
        ensure!(
            !pending,
            "interrupted feed: F3 recovery is not implemented; use new state and output directories"
        );
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
    if let Some((_, old, _, _)) = &recorded {
        ensure!(
            old == &incarnation,
            "incarnation changed: requires F4 kinds"
        );
    }
    if recorded.is_none() {
        std::fs::write(out.join("_wk_feed.json"), &binding)?;
        db.execute(
            "INSERT INTO feed VALUES(1,?1,?2,0,0)",
            params![binding, incarnation],
        )?;
    }
    let seq = recorded
        .as_ref()
        .map_or(0, |r| r.2)
        .checked_add(1)
        .context("sequence exhausted")?;
    let scratch = state.join("scratch");
    std::fs::create_dir_all(&scratch)?;
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
        let mut statement = db.prepare("SELECT class || '/' || name || '/', class, generation, txid, identity FROM heads ORDER BY class || '/' || name || '/'")?;
        let mut rows = statement.query([])?;
        let mut saved = next_saved(&mut rows)?;
        let absent_found = std::cell::Cell::new(false);
        {
            let jobs = crate::actor_groups::actor_groups(store, &history_prefix)
                .map(|group| {
                    let (actor_prefix, objects) = group?;
                    let key = actor_prefix
                        .strip_prefix(&history_prefix)
                        .context("actor outside app")?;
                    while saved.as_ref().is_some_and(|s| s.key.as_str() < key) {
                        if let Some(s) = &saved {
                            absent_found.set(absent_found.get() | absent(s, args));
                        }
                        saved = next_saved(&mut rows)?;
                    }
                    let old = if saved.as_ref().is_some_and(|s| s.key == key) {
                        let old = saved.take();
                        saved = next_saved(&mut rows)?;
                        old
                    } else {
                        None
                    };
                    Ok::<_, anyhow::Error>(read_changed(
                        args,
                        store,
                        &scratch,
                        &history_prefix,
                        actor_prefix,
                        objects,
                        old,
                        seq,
                    ))
                })
                .try_buffer_unordered(16);
            futures_util::pin_mut!(jobs);
            while let Some(result) = jobs.next().await {
                match result {
                    Ok(Some(actor)) => {
                        row_bytes += actor
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
                        ensure!(
                            changed.len() < 10_000 && row_bytes <= 1 << 30,
                            "poll exceeds F2 batch limit; F3 batching is required"
                        );
                        changed.push(actor);
                    }
                    Ok(None) => (),
                    Err(e) => {
                        eprintln!("feed actor left for next poll: {e:#}");
                        failed = true;
                    }
                }
            }
        }
        failed |= absent_found.get();
        while let Some(s) = saved {
            failed |= absent(&s, args);
            saved = next_saved(&mut rows)?;
        }
    }
    if changed.is_empty() {
        return Ok(if failed { 2 } else { 0 });
    }
    // F2 stops safely on interrupted publication; F3 supplies recovery.
    db.execute("UPDATE feed SET pending=1 WHERE id=1", [])?;
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
    for ((class, table), actors) in tables {
        let dir = out
            .join("tables")
            .join(encode_segment(&class))
            .join(encode_segment(&table));
        std::fs::create_dir_all(&dir)?;
        let written = writer::write_table(
            &dir.join(format!("{seq:020}.parquet")),
            &class,
            &table,
            &actors,
            &BTreeMap::new(),
        )?;
        for warning in written.warnings {
            eprintln!("{warning}");
        }
    }
    let states_dir = out.join("_wk_states");
    std::fs::create_dir_all(&states_dir)?;
    let tx = db.transaction()?;
    for actor in &changed {
        tx.execute(
            "INSERT OR REPLACE INTO heads VALUES(?1,?2,?3,?4,?5)",
            params![
                actor.class,
                actor.encoded,
                actor.position.generation as i64,
                actor.position.txid as i64,
                actor.identity
            ],
        )?;
    }
    let states = changed.into_iter().map(|c| c.state).collect::<Vec<_>>();
    writer::write_states(&states_dir.join(format!("{seq:020}.parquet")), &states)?;
    tx.execute(
        "UPDATE feed SET seq=?1,pending=0 WHERE id=1",
        [i64::try_from(seq)?],
    )?;
    tx.commit()?;
    Ok(if failed { 2 } else { 0 })
}

struct Saved {
    key: String,
    class: String,
    position: SegmentId,
    identity: String,
}
fn next_saved(rows: &mut rusqlite::Rows<'_>) -> anyhow::Result<Option<Saved>> {
    rows.next()?
        .map(|r| {
            Ok(Saved {
                key: r.get(0)?,
                class: r.get(1)?,
                position: SegmentId {
                    generation: r.get::<_, i64>(2)? as u64,
                    txid: r.get::<_, i64>(3)? as u64,
                },
                identity: r.get(4)?,
            })
        })
        .transpose()
}
fn absent(saved: &Saved, args: &Args) -> bool {
    if args.classes.is_empty() || args.classes.contains(&saved.class) {
        eprintln!(
            "{}: absent actor requires F4 kinds; head not advanced",
            saved.key
        );
        true
    } else {
        false
    }
}

#[allow(clippy::too_many_arguments)]
async fn read_changed(
    args: &Args,
    store: &Arc<dyn ObjectStore>,
    scratch: &std::path::Path,
    history_prefix: &str,
    actor_prefix: String,
    objects: BTreeMap<String, ObjectMeta>,
    old: Option<Saved>,
    seq: u64,
) -> anyhow::Result<Option<Changed>> {
    let (tenant, app) = &args.app;
    let relative = actor_prefix
        .strip_prefix(history_prefix)
        .context("actor outside app")?
        .trim_end_matches('/');
    let (class, encoded) = relative.split_once('/').context("invalid actor prefix")?;
    if !args.classes.is_empty() && !args.classes.iter().any(|c| c == class) {
        return Ok(None);
    }
    // An incomplete plan is retried by the reader below.
    if let Ok((position, identity)) = head(&objects)
        && old
            .as_ref()
            .is_some_and(|o| o.position == position && o.identity == identity)
    {
        return Ok(None);
    }
    let temp = tempfile::tempdir_in(scratch)?;
    let path = temp.path().join("actor.db");
    let (export, objects) =
        history::export_from_listing(store, &actor_prefix, objects, &path, args.max_bytes).await?;
    ensure!(
        export.verification.replaced.is_none(),
        "{relative}: replacement label requires F4 kinds"
    );
    let (position, identity) = head(&objects)?;
    if let Some(saved) = old {
        let SegmentId { generation, txid } = saved.position;
        let old_identity = saved.identity;
        let chain = history::ancestry(
            &Listing::from_keys(objects.keys().map(String::as_str)),
            position,
        );
        ensure!(
            !(position.generation == generation
                && position.txid == txid
                && old_identity != identity)
                && chain
                    .iter()
                    .any(|p| p.generation == generation && p.txid >= txid),
            "{relative}: unproven descent requires F4 kinds"
        );
    }
    let name = decode_segment(encoded).context("invalid encoded actor name")?;
    let connection =
        Connection::open_with_flags(&path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    connection.pragma_update(None, "trusted_schema", false)?;
    let tables = connection.prepare("SELECT name FROM pragma_table_list WHERE schema='main' AND type='table' AND name NOT LIKE 'sqlite_%' AND lower(substr(name,1,10)) != '_wasmkeep_' ORDER BY name")?.query_map([], |r| r.get::<_,String>(0))?.collect::<Result<Vec<_>,_>>()?;
    let mut data = Vec::new();
    for table in &tables {
        let mut stmt =
            connection.prepare(&format!("SELECT * FROM \"{}\"", table.replace('"', "\"\"")))?;
        let columns = stmt.column_names().map(str::to_owned).collect::<Vec<_>>();
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
        seq,
        generation: position.generation,
        txid: position.txid,
        committed_at: export.timestamp_ms,
        rows: Rows {
            columns: vec![],
            values: vec![],
        },
    };
    let emitted_at = i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis(),
    )?;
    Ok::<_, anyhow::Error>(Some(Changed {
        class: class.into(),
        encoded: encoded.into(),
        position,
        identity,
        state: writer::State {
            actor,
            class: class.into(),
            tables,
            emitted_at,
        },
        tables: data,
    }))
}
