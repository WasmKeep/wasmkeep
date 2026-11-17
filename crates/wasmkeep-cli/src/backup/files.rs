//! Files in backups (ADR 0030, section 5, "Backup and restore of a whole
//! app"): `backup create` copies `state/<t>/<a>/blobs/<epoch>/` as listed
//! into the archive's `data/<sha-256>`, `backup restore` writes back the
//! files of the incarnation each restored database names under the
//! target's new epoch. Assets travel with the deploys (`app_metadata`).
use super::*;
use object_store::{
    Attribute, Attributes, GetOptions, ObjectStoreExt, PutMultipartOptions, PutPayload,
};

/// A file larger than this is spooled to disk while its digest is
/// computed, then uploaded in parts (decision 4).
const SPOOL_FROM: usize = 16 << 20;
/// File entries per manifest chunk (`runs/<id>/files/<t>/<a>/<n>`).
const PER_CHUNK: usize = 1000;
/// The front's namespace, which comes back whole.
const FRONT: &str = "_front/main/0";

/// The namespace of an actor's files that its database names
/// (`<class>/<name id>/<incarnation>`), or `null` when the database will
/// draw a fresh incarnation on its next start (a delete's mark without an
/// incarnation: no file belongs to it).
pub(super) fn actor_namespace(
    prefix: &str,
    db: &std::path::Path,
    replaced: Option<&str>,
) -> anyhow::Result<Value> {
    let mut parts = prefix.trim_end_matches('/').rsplitn(3, '/');
    let name = parts.next().context("invalid actor prefix")?;
    let class = parts.next().context("invalid actor prefix")?;
    ensure!(
        parts.next().is_some_and(|rest| rest.ends_with("/actors")),
        "invalid actor prefix"
    );
    let class = restore::object_segment(class)?;
    let name = match wasmkeep_storage::decode_segment(&restore::object_segment(name)?) {
        Some(name) => name,
        // A long name is shortened in history keys; the address row keeps it.
        None => wasmkeep_storage::readonly::backup_address(db)?
            .and_then(|a| a.splitn(3, '/').nth(2).map(str::to_owned))
            .context("hashed actor lacks its original address")?,
    };
    let incarnation = stored_incarnation(db)?;
    Ok(
        match wasmkeep_cluster::blob_sweep::start(incarnation.as_deref(), replaced)
            .map_err(anyhow::Error::msg)?
        {
            wasmkeep_cluster::blob_sweep::Start::Use(i) => {
                json!(format!("{class}/{}/{i}", digest(name.as_bytes())))
            }
            wasmkeep_cluster::blob_sweep::Start::Draw => Value::Null,
        },
    )
}

fn stored_incarnation(db: &std::path::Path) -> anyhow::Result<Option<String>> {
    use rusqlite::OptionalExtension;
    let conn =
        rusqlite::Connection::open_with_flags(db, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    let has_meta: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE name='_wasmkeep_meta')",
        [],
        |row| row.get(0),
    )?;
    if !has_meta {
        return Ok(None);
    }
    Ok(conn
        .query_row(
            "SELECT value FROM _wasmkeep_meta WHERE key='blob_incarnation'",
            [],
            |row| row.get(0),
        )
        .optional()?)
}

/// The epoch the app's two markers give now.
pub(super) async fn epoch_of(store: &Store, app_base: &str) -> anyhow::Result<String> {
    let recreated = optional(store, &key(app_base, "recreated")).await?;
    let restored = optional(store, &key(app_base, "restored")).await?;
    Ok(wasmkeep_server::blobs::epoch(
        recreated.as_deref(),
        restored.as_deref(),
    ))
}

/// The file entries of an app record, every chunk checked by its digest.
pub(super) async fn entries(store: &Store, dp: &str, app: &Value) -> anyhow::Result<Vec<Value>> {
    let Some(chunks) = app.get("files") else {
        return Ok(Vec::new());
    };
    let base = format!(
        "{}/",
        key(
            dp,
            &format!("runs/{}/files/{}", text(app, "run")?, text(app, "app")?)
        )
    );
    let mut out = Vec::new();
    let mut seen = BTreeSet::new();
    for chunk in chunks.as_array().context("invalid manifest files")? {
        let path = text(chunk, "key")?;
        ensure!(
            path.starts_with(&base) && seen.insert(path.to_owned()),
            "invalid/duplicate file chunk"
        );
        let value = checked_json(store, path, text(chunk, "sha256")?).await?;
        for entry in value.as_array().context("file chunk is not an array")? {
            check_rel(text(entry, "rel")?)?;
            text(entry, "sha256")?;
            number(entry, "size")?;
            out.push(entry.clone());
        }
    }
    ensure!(
        out.len() as u64 == number(app, "file_count")?,
        "file count mismatch"
    );
    Ok(out)
}

/// `<class>/<name id>/<incarnation>/<key>`, canonical, at least four
/// segments, none empty or a dot segment.
fn check_rel(rel: &str) -> anyhow::Result<()> {
    let parts: Vec<_> = rel.split('/').collect();
    ensure!(
        parts.len() >= 4
            && parts
                .iter()
                .all(|p| !p.is_empty() && *p != "." && *p != "..")
            && Key::parse(rel)?.as_ref() == rel,
        "invalid file key: {rel}"
    );
    Ok(())
}
fn namespace(rel: &str) -> String {
    rel.splitn(4, '/').take(3).collect::<Vec<_>>().join("/")
}

/// Copy every file of the app's current epoch into the archive. A file
/// whose size, `ETag` and version match `previous` (the last compatible
/// run's entries, by source key) is carried over without a read; one gone
/// between listing and read is left out. Returns the record's fields.
#[allow(clippy::too_many_arguments)]
pub(super) async fn copy_app(
    source: &Store,
    sp: &str,
    dest: &Store,
    dp: &str,
    run_base: &str,
    tenant: &str,
    app: &str,
    previous: &BTreeMap<String, Value>,
    cache: Option<&std::path::Path>,
    epoch: Option<&str>,
) -> anyhow::Result<Value> {
    // A restore's undo run copies the epoch from before that restore wrote
    // `restored` (its journal recorded it); every other run the current one.
    let epoch = match epoch {
        Some(epoch) => epoch.to_owned(),
        None => epoch_of(source, &key(sp, &format!("apps/{tenant}/{app}"))).await?,
    };
    let root = format!(
        "{}/",
        key(sp, &format!("state/{tenant}/{app}/blobs/{epoch}"))
    );
    let listed = list_bounded(source, &root).await?;
    let mut copying = futures_util::stream::iter(listed.into_iter().map(|(rel, meta)| {
        let old = previous.get(meta.location.as_ref()).cloned();
        async move {
            if check_rel(&rel).is_err() {
                return Ok::<_, anyhow::Error>(None);
            }
            if let Some(old) = old.filter(|old| same(&meta, old)) {
                return Ok(Some(old));
            }
            let Some(mut entry) = copy_in(source, &meta, dest, dp, cache).await? else {
                return Ok(None);
            };
            entry["rel"] = json!(rel);
            entry["source"] = json!(meta.location.as_ref());
            Ok(Some(entry))
        }
    }))
    .buffered(8);
    let mut chunk = Vec::new();
    let mut chunks = Vec::new();
    let mut count = 0u64;
    while let Some(entry) = copying.try_next().await? {
        let Some(entry) = entry else { continue };
        chunk.push(entry);
        count += 1;
        if chunk.len() == PER_CHUNK {
            chunks.push(flush(dest, run_base, tenant, app, chunks.len(), &mut chunk).await?);
        }
    }
    if !chunk.is_empty() {
        chunks.push(flush(dest, run_base, tenant, app, chunks.len(), &mut chunk).await?);
    }
    Ok(json!({"blob_epoch":epoch,"files":chunks,"file_count":count}))
}

async fn flush(
    dest: &Store,
    run_base: &str,
    tenant: &str,
    app: &str,
    n: usize,
    chunk: &mut Vec<Value>,
) -> anyhow::Result<Value> {
    let path = key(run_base, &format!("files/{tenant}/{app}/{n:08}"));
    let hash = json_put(dest, &path, &json!(chunk), false).await?;
    chunk.clear();
    Ok(json!({"key":path,"sha256":hash}))
}

/// Read one file into the archive. `None` if it is gone.
async fn copy_in(
    source: &Store,
    meta: &ObjectMeta,
    dest: &Store,
    dp: &str,
    cache: Option<&std::path::Path>,
) -> anyhow::Result<Option<Value>> {
    let got = match source.get_opts(&meta.location, GetOptions::default()).await {
        Ok(got) => got,
        Err(object_store::Error::NotFound { .. }) => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    let content_type = got
        .attributes
        .get(&Attribute::ContentType)
        .map(|v| v.as_ref().to_owned());
    let read_meta = got.meta.clone();
    let mut stream = got.into_stream();
    let mut hasher = Sha256::new();
    let mut size = 0u64;
    let mut memory = Vec::new();
    let mut spool: Option<tempfile::NamedTempFile> = None;
    while let Some(part) = stream.next().await {
        let part = match part {
            Ok(part) => part,
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        hasher.update(&part);
        size += part.len() as u64;
        ensure!(
            size <= wasmkeep_server::blobs::MAX_FILE_CEILING,
            "file larger than any file can be: {}",
            meta.location
        );
        if spool.is_none() && memory.len() + part.len() > SPOOL_FROM {
            let mut file = match cache {
                Some(dir) => {
                    std::fs::create_dir_all(dir)?;
                    tempfile::NamedTempFile::new_in(dir)?
                }
                None => tempfile::NamedTempFile::new()?,
            };
            std::io::Write::write_all(&mut file, &memory)?;
            memory = Vec::new();
            spool = Some(file);
        }
        match &mut spool {
            Some(file) => std::io::Write::write_all(file, &part)?,
            None => memory.extend_from_slice(&part),
        }
    }
    let hash = format!("{:x}", hasher.finalize());
    let path = key(dp, &format!("data/{hash}"));
    match spool {
        None => put(dest, &path, memory, false).await?,
        Some(file) => upload_spooled(dest, &path, file).await?,
    }
    Ok(Some(json!({
        "sha256": hash,
        "size": size,
        "etag": read_meta.e_tag,
        "version": read_meta.version,
        "content_type": content_type,
    })))
}

async fn upload_spooled(
    dest: &Store,
    path: &str,
    file: tempfile::NamedTempFile,
) -> anyhow::Result<()> {
    let location = Key::from(path);
    match dest.head(&location).await {
        Ok(_) => return Ok(()),
        Err(object_store::Error::NotFound { .. }) => {}
        Err(e) => return Err(e.into()),
    }
    let mut reader = std::io::BufReader::new(file.reopen()?);
    let mut upload = dest
        .put_multipart_opts(&location, PutMultipartOptions::default())
        .await?;
    let result = async {
        loop {
            let mut part = Vec::with_capacity(SPOOL_FROM);
            std::io::Read::read_to_end(
                &mut std::io::Read::take(&mut reader, SPOOL_FROM as u64),
                &mut part,
            )?;
            if part.is_empty() {
                break;
            }
            upload.put_part(PutPayload::from(part)).await?;
        }
        upload.complete().await?;
        Ok::<_, anyhow::Error>(())
    }
    .await;
    if result.is_err() {
        let _ = upload.abort().await;
    }
    result
}

/// Every file entry's object is in the archive with its size.
pub(super) async fn verify(store: &Store, dp: &str, app: &Value) -> anyhow::Result<()> {
    for entry in entries(store, dp, app).await? {
        let hash = text(&entry, "sha256")?;
        ensure!(
            hash.len() == 64 && hash.bytes().all(|b| b.is_ascii_hexdigit()),
            "invalid file digest"
        );
        let meta = store
            .head(&Key::from(key(dp, &format!("data/{hash}"))))
            .await
            .with_context(|| format!("missing file object {hash}"))?;
        ensure!(
            meta.size == number(&entry, "size")?,
            "corrupt file object {hash}"
        );
    }
    Ok(())
}

/// Write the files of the namespaces in `wanted` (each restored actor's,
/// and the front's) under the target's epoch, which its markers give now:
/// `restored` was written by this operation before any copy.
pub(super) async fn restore_app(
    archive: &Store,
    ap: &str,
    app: &Value,
    mut wanted: BTreeSet<String>,
    target: &Store,
    app_base: &str,
    state: &str,
) -> anyhow::Result<u64> {
    wanted.insert(FRONT.to_owned());
    let epoch = epoch_of(target, app_base).await?;
    let mut written = 0;
    for entry in entries(archive, ap, app).await? {
        let rel = text(&entry, "rel")?;
        if !wanted.contains(&namespace(rel)) {
            continue;
        }
        let mut attributes = Attributes::new();
        if let Some(ctype) = entry["content_type"].as_str() {
            attributes.insert(Attribute::ContentType, ctype.to_owned().into());
        }
        copy_out(
            archive,
            &key(ap, &format!("data/{}", text(&entry, "sha256")?)),
            text(&entry, "sha256")?,
            target,
            &Key::parse(key(state, &format!("blobs/{epoch}/{rel}")))?,
            attributes,
        )
        .await?;
        written += 1;
    }
    Ok(written)
}

async fn copy_out(
    archive: &Store,
    from: &str,
    hash: &str,
    target: &Store,
    to: &Key,
    attributes: Attributes,
) -> anyhow::Result<()> {
    let got = archive
        .get_opts(&Key::from(from), GetOptions::default())
        .await?;
    let mut hasher = Sha256::new();
    if got.meta.size as usize <= SPOOL_FROM {
        let bytes = got.bytes().await?;
        hasher.update(&bytes);
        ensure!(
            format!("{:x}", hasher.finalize()) == hash,
            "corrupt backup object {hash}"
        );
        target
            .put_opts(
                to,
                PutPayload::from(bytes),
                PutOptions {
                    attributes,
                    ..Default::default()
                },
            )
            .await?;
        return Ok(());
    }
    let mut stream = got.into_stream();
    let mut upload = target
        .put_multipart_opts(
            to,
            PutMultipartOptions {
                attributes,
                ..Default::default()
            },
        )
        .await?;
    let result = async {
        let mut part = Vec::with_capacity(SPOOL_FROM);
        while let Some(bytes) = stream.next().await {
            let bytes = bytes?;
            hasher.update(&bytes);
            part.extend_from_slice(&bytes);
            if part.len() >= SPOOL_FROM {
                upload
                    .put_part(PutPayload::from(std::mem::take(&mut part)))
                    .await?;
            }
        }
        if !part.is_empty() {
            upload.put_part(PutPayload::from(part)).await?;
        }
        ensure!(
            format!("{:x}", hasher.finalize()) == hash,
            "corrupt backup object {hash}"
        );
        upload.complete().await?;
        Ok::<_, anyhow::Error>(())
    }
    .await;
    if result.is_err() {
        let _ = upload.abort().await;
    }
    result
}
