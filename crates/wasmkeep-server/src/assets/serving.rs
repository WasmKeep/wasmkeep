//! Host-only serving. The request can select an index entry, never a disk or bucket key.
use super::*;
use bytes::Bytes;
use futures_util::TryStreamExt;
use http_body_util::{BodyExt, StreamBody};
use hyper::{Request, Response, StatusCode};
use std::{collections::HashMap, path::PathBuf, sync::Arc};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use wasmtime_wasi_http::p2::body::HyperOutgoingBody;

/// One disk budget and LRU for a process; keys include the app's directory.
#[derive(Debug)]
pub struct Cache {
    limit: u64,
    inner: std::sync::Mutex<Entries>,
    fetch: tokio::sync::Mutex<()>,
}
#[derive(Debug, Default)]
struct Entries {
    files: HashMap<PathBuf, Entry>,
    registered: std::collections::HashSet<PathBuf>,
    tick: u64,
    bytes: u64,
}
#[derive(Debug)]
struct Entry {
    size: u64,
    used: u64,
    labels: Arc<str>,
}
impl Cache {
    pub fn new(limit: u64) -> Arc<Self> {
        Arc::new(Self {
            limit,
            inner: Default::default(),
            fetch: Default::default(),
        })
    }
    fn evict(&self, entries: &mut Entries, needed: u64) -> std::io::Result<()> {
        while entries.bytes.saturating_add(needed) > self.limit {
            let Some(old) = entries
                .files
                .iter()
                .min_by_key(|(_, e)| e.used)
                .map(|(p, _)| p.clone())
            else {
                break;
            };
            match std::fs::remove_file(&old) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(e),
            }
            let Some(entry) = entries.files.remove(&old) else {
                continue;
            };
            entries.bytes -= entry.size;
            crate::metrics::asset_cache_remove(&entry.labels, entry.size);
        }
        Ok(())
    }
    fn register(&self, dir: &std::path::Path, labels: &Arc<str>) -> std::io::Result<()> {
        use std::io::Read;
        std::fs::create_dir_all(dir)?;
        let mut entries = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        let first_start = entries.registered.insert(dir.to_path_buf());
        for file in std::fs::read_dir(dir)? {
            let file = file?;
            let name = file.file_name();
            let name = name.to_string_lossy();
            if !valid_digest(&name) {
                if first_start
                    && (name.starts_with(".fetch-") || name.starts_with(".indexes-"))
                    && file.file_type()?.is_file()
                {
                    std::fs::remove_file(file.path())?;
                }
                continue;
            }
            let path = file.path();
            if let Some(entry) = entries.files.remove(&path) {
                entries.bytes -= entry.size;
                crate::metrics::asset_cache_remove(&entry.labels, entry.size);
            }
            let meta = match file.metadata() {
                Ok(meta) => meta,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => return Err(e),
            };
            if !file.file_type()?.is_file() || meta.len() > self.limit {
                std::fs::remove_file(path)?;
                continue;
            }
            let mut hash = sha2::Sha256::new();
            let mut input = std::fs::File::open(&path)?;
            let mut buf = [0u8; 64 * 1024];
            loop {
                let n = input.read(&mut buf)?;
                if n == 0 {
                    break;
                }
                hash.update(&buf[..n]);
            }
            if format!("{:x}", hash.finalize()) != name {
                std::fs::remove_file(path)?;
                continue;
            }
            self.evict(&mut entries, meta.len())?;
            entries.tick += 1;
            let used = entries.tick;
            entries.bytes += meta.len();
            entries.files.insert(
                path,
                Entry {
                    size: meta.len(),
                    used,
                    labels: labels.clone(),
                },
            );
            crate::metrics::asset_cache_add(labels, meta.len());
        }
        Ok(())
    }
    async fn open(
        &self,
        state: &State,
        source: &Deployed,
        content: &Content,
        prefetch: bool,
    ) -> Result<tokio::fs::File, String> {
        // Serializes cache misses across apps, including concurrent requests for a digest.
        let _fetch = self.fetch.lock().await;
        let path = state.dir.join(&content.digest);
        let cached = {
            let mut entries = self.inner.lock().unwrap_or_else(|p| p.into_inner());
            entries.tick += 1;
            let tick = entries.tick;
            if let Some(entry) = entries.files.get_mut(&path) {
                if entry.size != content.size {
                    return Err("cached content size differs from index".into());
                }
                entry.used = tick;
                Some(std::fs::File::open(&path).map_err(|e| e.to_string())?)
            } else {
                None
            }
        };
        if let Some(file) = cached {
            return Ok(tokio::fs::File::from_std(file));
        }
        tokio::fs::create_dir_all(&state.dir)
            .await
            .map_err(|e| e.to_string())?;
        // A restart never trusts a cached object until its size and digest are checked.
        let valid_disk = verify_disk(&path, content).await;
        let file = if valid_disk {
            tokio::fs::File::open(&path)
                .await
                .map_err(|e| e.to_string())?
        } else {
            let temporary = tempfile::Builder::new()
                .prefix(".fetch-")
                .tempfile_in(&state.dir)
                .map_err(|e| e.to_string())?;
            let mut file =
                tokio::fs::File::from_std(temporary.reopen().map_err(|e| e.to_string())?);
            let result = async {
                let object = source
                    .app
                    .asset_content(&content.digest)
                    .await
                    .map_err(|e| e.to_string())?;
                if object.meta.size != content.size {
                    return Err("digest_mismatch".to_owned());
                }
                let mut stream = object.into_stream();
                let mut hash = sha2::Sha256::new();
                let mut size = 0u64;
                while let Some(chunk) = stream.try_next().await.map_err(|e| e.to_string())? {
                    size += chunk.len() as u64;
                    if size > content.size {
                        return Err("digest_mismatch".into());
                    }
                    hash.update(&chunk);
                    file.write_all(&chunk).await.map_err(|e| e.to_string())?;
                }
                if size != content.size || format!("{:x}", hash.finalize()) != content.digest {
                    return Err("digest_mismatch".into());
                }
                file.flush().await.map_err(|e| e.to_string())?;
                Ok(())
            }
            .await;
            if let Err(error) = result {
                crate::metrics::asset_fetch(
                    &state.labels,
                    if error == "digest_mismatch" {
                        "digest_mismatch"
                    } else {
                        "unavailable"
                    },
                );
                return Err(error);
            }
            crate::metrics::asset_fetch(&state.labels, "ok");
            file.rewind().await.map_err(|e| e.to_string())?;
            if content.size > self.limit {
                // An oversized object is verified and streamed from an unlinked temporary file.
                // It never enters the retained cache or displaces its entries.
                return Ok(file);
            }
            {
                let mut entries = self.inner.lock().unwrap_or_else(|p| p.into_inner());
                if prefetch && entries.bytes.saturating_add(content.size) > self.limit {
                    return Ok(file);
                }
                self.evict(&mut entries, content.size)
                    .map_err(|e| e.to_string())?;
                temporary.persist(&path).map_err(|e| e.to_string())?;
            }
            file
        };
        if content.size <= self.limit {
            let mut entries = self.inner.lock().unwrap_or_else(|p| p.into_inner());
            // A runtime restarting beside this fetch may have registered the renamed file.
            if entries.files.contains_key(&path) {
                return Ok(file);
            }
            self.evict(&mut entries, content.size)
                .map_err(|e| e.to_string())?;
            entries.bytes += content.size;
            let tick = entries.tick;
            entries.files.insert(
                path,
                Entry {
                    size: content.size,
                    used: tick,
                    labels: state.labels.clone(),
                },
            );
            crate::metrics::asset_cache_add(&state.labels, content.size);
        }
        Ok(file)
    }
}
async fn verify_disk(path: &std::path::Path, content: &Content) -> bool {
    let Ok(mut file) = tokio::fs::File::open(path).await else {
        return false;
    };
    if !file
        .metadata()
        .await
        .is_ok_and(|m| m.is_file() && m.len() == content.size)
    {
        return false;
    }
    let mut hash = sha2::Sha256::new();
    let mut buffer = vec![0u8; 64 * 1024];
    loop {
        match file.read(&mut buffer).await {
            Ok(0) => break,
            Ok(n) => hash.update(&buffer[..n]),
            Err(_) => return false,
        }
    }
    format!("{:x}", hash.finalize()) == content.digest
}

/// A verified index and its app-scoped bucket source. Never exposed to a guest.
#[derive(Clone, Debug)]
pub struct Deployed {
    pub index: Arc<Index>,
    pub previous: Option<Arc<Index>>,
    pub app: crate::apps::AppRef,
}
#[derive(Clone, Debug)]
pub struct State {
    current: Option<Deployed>,
    local: Option<Arc<Local>>,
    previous: Option<Arc<Index>>,
    dir: PathBuf,
    cache: Arc<Cache>,
    labels: Arc<str>,
}
impl State {
    pub fn new(
        current: Option<Deployed>,
        dir: PathBuf,
        cache: Arc<Cache>,
        labels: Arc<str>,
    ) -> Self {
        let mut state = Self {
            local: None,
            previous: current.as_ref().and_then(|d| d.previous.clone()),
            current,
            dir,
            cache,
            labels,
        };
        if let Ok(bytes) = std::fs::read(state.record())
            && let Some((old, previous)) = read_record(&bytes)
        {
            let now = state.current.as_ref().map(|d| d.index.clone());
            state.previous = if old == now { previous } else { old };
        }
        state
    }
    pub fn with_local(mut self, local: Option<Arc<Local>>) -> Self {
        if local.is_some() {
            self.current = None;
            self.previous = None;
        }
        self.local = local;
        self
    }
    fn index(&self) -> Option<Arc<Index>> {
        self.local
            .as_ref()
            .map(|l| l.index())
            .or_else(|| self.current.as_ref().map(|d| d.index.clone()))
    }
    fn record(&self) -> PathBuf {
        self.dir.join("indexes")
    }
    pub(crate) fn switched(&self, current: Option<Deployed>) -> std::io::Result<Self> {
        let next = Self {
            previous: self.current.as_ref().map(|d| d.index.clone()),
            current,
            ..self.clone()
        };
        next.persist()?;
        Ok(next)
    }
    pub(crate) fn initialize(&self) -> std::io::Result<()> {
        if self.local.is_some() {
            return Ok(());
        }
        if self.dir.exists() {
            self.cache.register(&self.dir, &self.labels)?;
        }
        self.persist()
    }
    pub(crate) fn persist(&self) -> std::io::Result<()> {
        if self.current.is_none() && self.previous.is_none() && !self.record().exists() {
            return Ok(());
        }
        std::fs::create_dir_all(&self.dir)?;
        let mut bytes = Vec::new();
        for index in [
            self.current.as_ref().map(|d| &d.index),
            self.previous.as_ref(),
        ] {
            let data = index.map(|i| i.bytes()).unwrap_or_default();
            bytes.extend_from_slice(&(data.len() as u64).to_le_bytes());
            bytes.extend(data);
        }
        use std::io::Write;
        let mut file = tempfile::Builder::new()
            .prefix(".indexes-")
            .tempfile_in(&self.dir)?;
        file.write_all(&bytes)?;
        file.as_file().sync_all()?;
        file.persist(self.record()).map_err(|e| e.error)?;
        std::fs::File::open(&self.dir)?.sync_all()?;
        Ok(())
    }
    /// Best-effort prefetch never evicts an entry to make room for a cold one.
    pub(crate) fn prefetch(self: Arc<Self>) {
        if self.current.is_none() {
            return;
        }
        // apply_deployed also supports callers on ordinary threads. Prefetch is optional.
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return;
        };
        runtime.spawn(async move {
            let Some(source) = &self.current else {
                return;
            };
            for content in source.index.files.values().flat_map(File::contents) {
                let full = {
                    let entries = self.cache.inner.lock().unwrap_or_else(|p| p.into_inner());
                    entries.bytes.saturating_add(content.size) > self.cache.limit
                };
                if full {
                    continue;
                }
                let _ = self.cache.open(&self, source, content, true).await;
            }
        });
    }
    /// Headers the front must retain for an unmatched asset request.
    pub(crate) fn front_headers<B>(&self, req: &Request<B>) -> (bool, bool, bool) {
        let Some(index) = self.index() else {
            return (false, false, false);
        };
        let Some(path) = request_path(req.uri().path()) else {
            return (false, false, false);
        };
        if !eligible(req) || under(&index.front_first, &path) {
            return (false, false, false);
        }
        (
            index.not_found == "spa",
            under(&index.immutable, &path),
            true,
        )
    }
    pub async fn answer<B>(&self, req: &Request<B>) -> Option<Response<HyperOutgoingBody>> {
        let index = self.index()?;
        let raw = req.uri().path();
        // Check aliases even on non-GET requests, before sending anything to code.
        if let Some(decoded) = decode(raw.trim_start_matches('/'))
            && ["actor/", "_wasmkeep/"]
                .iter()
                .any(|p| decoded.starts_with(p) && !raw.starts_with(&format!("/{p}")))
        {
            return Some(crate::http::text_response(
                StatusCode::BAD_REQUEST,
                "reserved path spelling\n",
            ));
        }
        let path = request_path(raw)?;
        if under(&index.front_first, &path) || !eligible(req) {
            return None;
        }
        let mut name = path.clone();
        let mut chosen_index = index.as_ref();
        if path.is_empty() || path.ends_with('/') {
            name.push_str("index.html");
        }
        let mut file = index.files.get(&name);
        if file.is_none()
            && under(&index.immutable, &name)
            && let Some(previous) = &self.previous
            && under(&previous.immutable, &name)
        {
            file = previous.files.get(&name);
            chosen_index = previous;
        }
        if file.is_none()
            && !path.ends_with('/')
            && index.files.contains_key(&format!("{path}/index.html"))
        {
            let location = format!(
                "{}/{}",
                raw,
                req.uri()
                    .query()
                    .map(|q| format!("?{q}"))
                    .unwrap_or_default()
            );
            let mut response = empty(308);
            response
                .headers_mut()
                .insert("location", location.parse().ok()?);
            response.headers_mut().insert(
                "cache-control",
                hyper::header::HeaderValue::from_static("no-cache"),
            );
            crate::metrics::asset_request(&self.labels, "redirect");
            return Some(response);
        }
        let mut status = 200;
        let mut outcome = "ok";
        let mut cache_control = if under(&chosen_index.immutable, &name) {
            "public, max-age=31536000, immutable"
        } else {
            "public, max-age=0, must-revalidate"
        };
        let mut navigation_vary = false;
        if file.is_none() {
            match index.not_found.as_str() {
                "spa" if navigation(req, &path) => {
                    name = "index.html".into();
                    file = index.files.get(&name);
                    cache_control = "no-cache";
                    navigation_vary = true;
                    outcome = "spa";
                }
                "404" => {
                    name = "404.html".into();
                    file = index.files.get(&name);
                    status = 404;
                    cache_control = "no-store";
                    outcome = "not_found";
                }
                _ => {
                    crate::metrics::asset_request(&self.labels, "to_front");
                    return None;
                }
            }
        }
        let Some(file) = file else {
            let mut response = crate::http::text_response(
                StatusCode::NOT_FOUND,
                if req.method() == hyper::Method::HEAD {
                    ""
                } else {
                    "not found\n"
                },
            );
            response.headers_mut().insert(
                "cache-control",
                hyper::header::HeaderValue::from_static("no-store"),
            );
            response.headers_mut().insert(
                "content-length",
                hyper::header::HeaderValue::from_static("10"),
            );
            crate::metrics::asset_request(&self.labels, "not_found");
            return Some(response);
        };
        let plain_tag = etag(&file.identity);
        let range = if status == 200 && header(req, "if-range").is_none_or(|h| h == plain_tag) {
            header(req, "range").and_then(|h| byte_range(h, file.identity.size))
        } else {
            None
        };
        let (encoding, content) = if range.is_some() {
            ("identity", &file.identity)
        } else {
            match negotiate(req, file) {
                Some(selected) => selected,
                None => {
                    let mut res = empty(406);
                    res.headers_mut().insert(
                        "vary",
                        hyper::header::HeaderValue::from_static("Accept-Encoding"),
                    );
                    res.headers_mut().insert(
                        "cache-control",
                        hyper::header::HeaderValue::from_static("no-store"),
                    );
                    return Some(res);
                }
            }
        };
        let tag = etag(content);
        let vary = match (file.br.is_some() || file.gzip.is_some(), navigation_vary) {
            (true, true) => Some("Accept-Encoding, Sec-Fetch-Mode, Accept"),
            (true, false) => Some("Accept-Encoding"),
            (false, true) => Some("Sec-Fetch-Mode, Accept"),
            _ => None,
        };
        let mut response = empty(status);
        let headers = response.headers_mut();
        headers.insert("etag", tag.parse().ok()?);
        headers.insert("cache-control", cache_control.parse().ok()?);
        headers.insert("content-type", file.content_type.parse().ok()?);
        headers.insert(
            "x-content-type-options",
            hyper::header::HeaderValue::from_static("nosniff"),
        );
        headers.insert(
            "accept-ranges",
            hyper::header::HeaderValue::from_static("bytes"),
        );
        if let Some(vary) = vary {
            headers.insert("vary", vary.parse().ok()?);
        }
        if status == 200
            && req
                .headers()
                .get_all("if-none-match")
                .iter()
                .filter_map(|v| v.to_str().ok())
                .flat_map(|v| v.split(','))
                .any(|v| {
                    let v = v.trim();
                    v == "*" || v.strip_prefix("W/").unwrap_or(v) == tag
                })
        {
            *response.status_mut() = StatusCode::NOT_MODIFIED;
            crate::metrics::asset_request(&self.labels, "not_modified");
            return Some(response);
        }
        let (start, length) = match range {
            Some(Err(())) => {
                *response.status_mut() = StatusCode::RANGE_NOT_SATISFIABLE;
                response.headers_mut().insert(
                    "content-range",
                    format!("bytes */{}", file.identity.size).parse().ok()?,
                );
                return Some(response);
            }
            Some(Ok((start, end))) => {
                *response.status_mut() = StatusCode::PARTIAL_CONTENT;
                response.headers_mut().insert(
                    "content-range",
                    format!("bytes {start}-{end}/{}", file.identity.size)
                        .parse()
                        .ok()?,
                );
                outcome = "partial";
                (start, end - start + 1)
            }
            None => {
                if encoding != "identity" {
                    response
                        .headers_mut()
                        .insert("content-encoding", encoding.parse().ok()?);
                }
                (0, content.size)
            }
        };
        response
            .headers_mut()
            .insert("content-length", length.into());
        {
            let opened = if let Some(local) = &self.local {
                local
                    .open(&name)
                    .map(tokio::fs::File::from_std)
                    .map_err(|e| e.to_string())
            } else {
                self.cache
                    .open(self, self.current.as_ref()?, content, false)
                    .await
            };
            match opened {
                Ok(mut file) => {
                    if req.method() == hyper::Method::HEAD {
                        crate::metrics::asset_request(&self.labels, outcome);
                        return Some(response);
                    }
                    if file.seek(std::io::SeekFrom::Start(start)).await.is_err() {
                        return Some(unavailable(&self.labels));
                    }
                    let stream = futures_util::stream::try_unfold(
                        (file, length),
                        |(mut file, left)| async move {
                            if left == 0 {
                                return Ok(None);
                            }
                            let mut buffer = vec![0u8; left.min(64 * 1024) as usize];
                            let n = file.read(&mut buffer).await.map_err(|_| wasmtime_wasi_http::p2::bindings::http::types::ErrorCode::InternalError(None))?;
                            if n == 0 {
                                return Err(wasmtime_wasi_http::p2::bindings::http::types::ErrorCode::InternalError(None));
                            }
                            buffer.truncate(n);
                            Ok(Some((
                                hyper::body::Frame::data(Bytes::from(buffer)),
                                (file, left - n as u64),
                            )))
                        },
                    );
                    *response.body_mut() =
                        BodyExt::map_err(StreamBody::new(stream), wasmtime_wasi_http::Error::from)
                            .boxed_unsync();
                    crate::metrics::asset_bytes(&self.labels, encoding, length);
                }
                Err(_) => {
                    let mut res = unavailable(&self.labels);
                    if let Some(vary) = vary {
                        res.headers_mut().insert("vary", vary.parse().ok()?);
                    }
                    if req.method() == hyper::Method::HEAD {
                        *res.body_mut() = empty(503).into_body();
                    }
                    return Some(res);
                }
            }
        }
        crate::metrics::asset_request(&self.labels, outcome);
        Some(response)
    }
}
type SavedIndexes = (Option<Arc<Index>>, Option<Arc<Index>>);
fn read_record(bytes: &[u8]) -> Option<SavedIndexes> {
    let mut rest = bytes;
    let mut read = || {
        let len = u64::from_le_bytes(rest.get(..8)?.try_into().ok()?) as usize;
        if len > MAX_INDEX {
            return None;
        }
        let data = rest.get(8..8 + len)?;
        rest = rest.get(8 + len..)?;
        Some(if len == 0 {
            None
        } else {
            Some(Arc::new(Index::parse(data).ok()?))
        })
    };
    let current = read()?;
    let previous = read()?;
    if !rest.is_empty() {
        return None;
    }
    Some((current, previous))
}
fn unavailable(labels: &str) -> Response<HyperOutgoingBody> {
    crate::metrics::asset_request(labels, "unavailable");
    let mut res = crate::http::text_response(
        StatusCode::SERVICE_UNAVAILABLE,
        "asset unavailable; retry\n",
    );
    res.headers_mut()
        .insert("retry-after", hyper::header::HeaderValue::from_static("1"));
    res.headers_mut().insert(
        "cache-control",
        hyper::header::HeaderValue::from_static("no-store"),
    );
    res
}
fn empty(status: u16) -> Response<HyperOutgoingBody> {
    crate::http::text_response(
        StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
        "",
    )
}
fn under(prefixes: &BTreeSet<String>, path: &str) -> bool {
    prefixes.iter().any(|p| path.starts_with(p))
}
fn request_path(raw: &str) -> Option<String> {
    if raw.to_ascii_lowercase().contains("%2f") {
        return None;
    }
    let name = decode(raw.strip_prefix('/')?)?;
    let trimmed = name.strip_suffix('/').unwrap_or(&name);
    if name.is_empty() || (valid_name(trimmed) && !name.starts_with('/')) {
        Some(name)
    } else {
        None
    }
}
fn eligible<B>(req: &Request<B>) -> bool {
    matches!(*req.method(), hyper::Method::GET | hyper::Method::HEAD)
        && !req.headers().contains_key("upgrade")
}
fn header<'a, B>(req: &'a Request<B>, name: &str) -> Option<&'a str> {
    req.headers().get(name)?.to_str().ok()
}
fn navigation<B>(req: &Request<B>, path: &str) -> bool {
    match header(req, "sec-fetch-mode") {
        Some(mode) => mode == "navigate",
        None => {
            !path.rsplit('/').next().unwrap_or("").contains('.')
                && header(req, "accept").is_some_and(|h| {
                    h.split(',')
                        .any(|s| s.trim().split(';').next() == Some("text/html"))
                })
        }
    }
}
fn etag(content: &Content) -> String {
    format!("\"{}\"", &content.digest[..32])
}
fn negotiate<'a, B>(req: &Request<B>, file: &'a File) -> Option<(&'static str, &'a Content)> {
    let mut qualities = HashMap::new();
    for value in req
        .headers()
        .get_all("accept-encoding")
        .iter()
        .filter_map(|v| v.to_str().ok())
    {
        for item in value.split(',') {
            let mut parts = item.trim().split(';');
            let name = parts.next()?.trim().to_ascii_lowercase();
            let mut q = 1f32;
            for parameter in parts {
                if let Some(value) = parameter.trim().strip_prefix("q=") {
                    q = value
                        .parse::<f32>()
                        .ok()
                        .filter(|q| (0.0..=1.0).contains(q))
                        .unwrap_or(0.0);
                }
            }
            qualities.insert(name, q);
        }
    }
    let quality = |name: &str| {
        qualities.get(name).copied().unwrap_or_else(|| {
            if name == "identity" {
                if qualities.get("*") == Some(&0.0) {
                    0.0
                } else {
                    1.0
                }
            } else {
                qualities.get("*").copied().unwrap_or(0.0)
            }
        })
    };
    // Preference order among available encodings, while respecting explicit exclusions.
    [
        ("br", file.br.as_ref()),
        ("gzip", file.gzip.as_ref()),
        ("identity", Some(&file.identity)),
    ]
    .into_iter()
    .find_map(|(name, c)| c.filter(|_| quality(name) > 0.0).map(|c| (name, c)))
}
fn byte_range(value: &str, size: u64) -> Option<Result<(u64, u64), ()>> {
    let value = value.strip_prefix("bytes=")?;
    if value.contains(',') {
        return None;
    }
    let (first, last) = value.split_once('-')?;
    if first.is_empty() {
        let suffix = last.parse::<u64>().ok()?;
        return Some(if size == 0 || suffix == 0 {
            Err(())
        } else {
            Ok((size.saturating_sub(suffix), size - 1))
        });
    }
    let start = first.parse::<u64>().ok()?;
    let end = if last.is_empty() {
        size.saturating_sub(1)
    } else {
        last.parse::<u64>().ok()?
    };
    Some(if start >= size || end < start {
        Err(())
    } else {
        Ok((start, end.min(size - 1)))
    })
}
#[cfg(test)]
mod tests;
