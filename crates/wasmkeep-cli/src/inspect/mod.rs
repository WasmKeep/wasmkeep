//! Local bucket-only inspector. No node RPCs and no bucket mutations.
use anyhow::{Context, ensure};
use bytes::Bytes;
use http_body_util::{BodyExt, Full, Limited};
use hyper::{Request, Response, StatusCode, body::Incoming, service::service_fn};
use object_store::{ObjectStore, path::Path as Key};
use serde_json::{Value, json};
use std::{
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    sync::Arc,
};
use subtle::ConstantTimeEq;
use wasmkeep_cluster::{
    replication::{Listing, plan_restore},
    state_root::{Family, StateRoot},
};
use wasmkeep_server::history;

const PAGE: &str = include_str!("page.html");
const PAGE_SIZE: usize = 50;

#[derive(Debug, clap::Args)]
pub(crate) struct Args {
    /// App to inspect, TENANT/APP.
    #[arg(long, value_parser = crate::parse_app)]
    app: (String, String),
    /// s3://BUCKET[/PREFIX]; defaults to WASMKEEP_BUCKET.
    #[arg(long)]
    bucket: Option<String>,
    /// Bind only to 127.0.0.1 or ::1.
    #[arg(long, default_value = "127.0.0.1")]
    bind: IpAddr,
    /// Local port; zero chooses a free port.
    #[arg(long, default_value_t = 0)]
    port: u16,
}

struct Token(String);
impl Token {
    fn new() -> anyhow::Result<Self> {
        let mut bytes = [0u8; 32];
        getrandom::fill(&mut bytes).map_err(|e| anyhow::anyhow!("OS randomness: {e}"))?;
        Ok(Self(bytes.iter().map(|b| format!("{b:02x}")).collect()))
    }
    fn accepts(&self, supplied: Option<&str>) -> bool {
        // Fixed-size comparison; neither mismatching position nor secret length
        // influences comparison work. The public input length is not a secret.
        let supplied = supplied.unwrap_or_default().as_bytes();
        let mut candidate = [0u8; 64];
        if supplied.len() == candidate.len() {
            candidate.copy_from_slice(supplied);
        }
        bool::from(self.0.as_bytes().ct_eq(&candidate))
    }
}
fn check_bind(address: SocketAddr) -> anyhow::Result<()> {
    ensure!(
        address.ip() == IpAddr::V4(Ipv4Addr::LOCALHOST)
            || address.ip() == IpAddr::V6(Ipv6Addr::LOCALHOST),
        "inspect binds only to 127.0.0.1 or ::1"
    );
    Ok(())
}
fn valid_host(host: Option<&str>, address: SocketAddr) -> bool {
    host == Some(address.to_string().as_str())
}
struct State {
    store: Arc<dyn ObjectStore>,
    prefix: String,
    bucket: String,
    app: (String, String),
    token: Token,
    bootstrap: std::sync::Mutex<Option<Token>>,
    address: SocketAddr,
    work: Arc<tokio::sync::Semaphore>,
}

pub(crate) fn run(args: Args) -> anyhow::Result<()> {
    let address = SocketAddr::new(args.bind, args.port);
    check_bind(address)?;
    wasmkeep_runtime::ActorAddress::new(&args.app.0, &args.app.1, "validation")?;
    let bucket = args
        .bucket
        .or_else(|| std::env::var("WASMKEEP_BUCKET").ok())
        .context("provide --bucket or WASMKEEP_BUCKET")?;
    let (store, prefix) = crate::open_bucket_unchecked(&bucket)?;
    wasmkeep_cli::run_async(tokio::task::LocalSet::new().run_until(async move {
        let listener = tokio::net::TcpListener::bind(address).await?;
        let address = listener.local_addr()?;
        let bootstrap = Token::new()?;
        let secret = bootstrap.0.clone();
        let state = Arc::new(State {
            store,
            prefix,
            bucket,
            app: args.app,
            token: Token::new()?,
            bootstrap: std::sync::Mutex::new(Some(bootstrap)),
            address,
            work: Arc::new(tokio::sync::Semaphore::new(1)),
        });
        println!("Inspector (read-only): http://{address}/?token={}", secret);
        let connections = Arc::new(tokio::sync::Semaphore::new(16));
        loop {
            let permit = connections.clone().acquire_owned().await?;
            let (stream, _) = listener.accept().await?;
            let state = state.clone();
            tokio::task::spawn_local(async move {
                let _permit = permit;
                let service = service_fn(move |request| handle(request, state.clone()));
                let connection = hyper::server::conn::http1::Builder::new()
                    .max_buf_size(16384)
                    .serve_connection(hyper_util::rt::TokioIo::new(stream), service);
                let _ = tokio::time::timeout(std::time::Duration::from_secs(120), connection).await;
            });
        }
    }))
}
fn response(
    status: StatusCode,
    content_type: &'static str,
    body: impl Into<Bytes>,
) -> Response<Full<Bytes>> {
    let mut reply = Response::new(Full::new(body.into()));
    *reply.status_mut() = status;
    for (name, value) in [
        ("content-type", content_type),
        ("cache-control", "no-store"),
        ("referrer-policy", "no-referrer"),
        ("x-content-type-options", "nosniff"),
        ("x-frame-options", "DENY"),
    ] {
        reply
            .headers_mut()
            .insert(name, hyper::header::HeaderValue::from_static(value));
    }
    use base64::Engine;
    use sha2::{Digest, Sha256};
    // A malformed embedded page fails closed: its scripts cannot match this hash.
    let script = PAGE
        .split_once("<script>")
        .and_then(|(_, rest)| rest.split_once("</script>"))
        .map_or("", |(script, _)| script);
    let hash = base64::engine::general_purpose::STANDARD.encode(Sha256::digest(script.as_bytes()));
    reply.headers_mut().insert("content-security-policy", format!("default-src 'none'; script-src 'sha256-{hash}'; style-src 'unsafe-inline'; connect-src 'self'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'").parse().unwrap_or_else(|_| hyper::header::HeaderValue::from_static("default-src 'none'")));
    reply
}
async fn handle(
    req: Request<Incoming>,
    state: Arc<State>,
) -> Result<Response<Full<Bytes>>, std::convert::Infallible> {
    Ok(match serve(req, state).await {
        Ok(response) => response,
        Err(error) => response(
            StatusCode::BAD_REQUEST,
            "text/plain; charset=utf-8",
            error.to_string(),
        ),
    })
}
async fn serve(req: Request<Incoming>, state: Arc<State>) -> anyhow::Result<Response<Full<Bytes>>> {
    let headers = req.headers();
    if headers.get_all("host").iter().count() != 1
        || !valid_host(
            headers.get("host").and_then(|v| v.to_str().ok()),
            state.address,
        )
    {
        return Ok(response(
            StatusCode::FORBIDDEN,
            "text/plain",
            "Invalid Host",
        ));
    }
    // Reject cross-origin requests even if a browser attaches credentials.
    if let Some(origin) = headers.get("origin") {
        ensure!(
            origin.to_str()? == format!("http://{}", state.address),
            "Invalid origin"
        );
    }
    if let Some(site) = headers.get("sec-fetch-site") {
        ensure!(site != "cross-site", "Cross-site request refused");
    }
    let url = reqwest::Url::parse(&format!("http://{}{}", state.address, req.uri()))?;
    let boot = url
        .query_pairs()
        .find(|(k, _)| k == "token")
        .map(|(_, v)| v.into_owned());
    let bearer = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "));
    if req.method() == hyper::Method::POST && url.path() == "/bootstrap" {
        let mut bootstrap = state
            .bootstrap
            .lock()
            .map_err(|_| anyhow::anyhow!("Bootstrap lock poisoned"))?;
        if bootstrap
            .as_ref()
            .is_some_and(|token| token.accepts(bearer))
        {
            bootstrap.take();
            return Ok(response(
                StatusCode::OK,
                "application/json",
                serde_json::to_vec(&json!({"bearer":state.token.0}))?,
            ));
        }
    } else if req.method() == hyper::Method::GET
        && url.path() == "/"
        && (state
            .bootstrap
            .lock()
            .map_err(|_| anyhow::anyhow!("Bootstrap lock poisoned"))?
            .as_ref()
            .is_some_and(|token| token.accepts(boot.as_deref()))
            || state.token.accepts(bearer))
    {
        return Ok(response(StatusCode::OK, "text/html; charset=utf-8", PAGE));
    }
    if !state.token.accepts(bearer) || boot.is_some() || url.path() == "/bootstrap" {
        return Ok(response(
            StatusCode::UNAUTHORIZED,
            "text/plain",
            "Inspector bearer required (bootstrap is single-use)",
        ));
    }
    ensure!(
        req.method() == hyper::Method::POST && url.path() == "/api",
        "Unknown route or method"
    );
    ensure!(
        headers.get("content-type").and_then(|v| v.to_str().ok()) == Some("application/json"),
        "Expected application/json"
    );
    let bytes = Limited::new(req.into_body(), 65536)
        .collect()
        .await
        .map_err(|e| anyhow::anyhow!("Request body: {e}"))?
        .to_bytes();
    let input: Value = serde_json::from_slice(&bytes)?;
    let permit = state
        .work
        .clone()
        .try_acquire_owned()
        .context("Inspector busy; retry after the current read")?;
    let value = api(&state, input, permit).await?;
    Ok(response(
        StatusCode::OK,
        "application/json",
        serde_json::to_vec(&value)?,
    ))
}
fn page(mut names: Vec<String>, after: Option<&str>) -> Value {
    names.sort();
    names.dedup();
    let mut items: Vec<_> = names
        .into_iter()
        .filter(|s| after.is_none_or(|a| s.as_str() > a))
        .take(PAGE_SIZE + 1)
        .collect();
    let more = items.len() > PAGE_SIZE;
    items.truncate(PAGE_SIZE);
    json!({"next": if more {items.last()} else {None}, "items":items})
}
fn query(path: &std::path::Path, sql: &str) -> anyhow::Result<Value> {
    use base64::Engine;
    use wasmkeep_storage::SqlValue;
    let rows = wasmkeep_storage::readonly::query(path, sql)?;
    let values: Vec<Vec<Value>> = rows
        .values
        .into_iter()
        .map(|row| {
            row.into_iter()
                .map(|v| match v {
                    SqlValue::Null => Value::Null,
                    SqlValue::Integer(v) => json!(v.to_string()),
                    SqlValue::Real(v) => json!(v),
                    SqlValue::Text(v) => json!(v),
                    SqlValue::Blob(v) => {
                        json!({"base64":base64::engine::general_purpose::STANDARD.encode(v)})
                    }
                })
                .collect()
        })
        .collect();
    Ok(json!({"columns":rows.columns,"rows":values}))
}
const MAX_LIST_OBJECTS: usize = 10_000;

async fn api(
    state: &State,
    input: Value,
    permit: tokio::sync::OwnedSemaphorePermit,
) -> anyhow::Result<Value> {
    let root = StateRoot::new(&state.prefix, &state.app.0, &state.app.1);
    let op = input["op"].as_str().context("Missing operation")?;
    if op == "status" {
        let horizon = wasmkeep_server::heartbeat_horizon_ms(
            wasmkeep_server::DEFAULT_HEARTBEAT,
            &wasmkeep_cluster::lease::LeaseConfig::default_safe(),
        );
        return Ok(crate::status::read_bounded(
            state.store.clone(),
            &state.prefix,
            &state.bucket,
            crate::status::now_ms(),
            horizon,
            MAX_LIST_OBJECTS,
        )
        .await?
        .to_json());
    }
    if op == "classes" || op == "actors" {
        let class = input["class"].as_str().unwrap_or_default();
        if op == "actors" {
            wasmkeep_runtime::ActorAddress::new(&state.app.0, class, "validation")?;
        }
        let prefix = root.bucket_key(Family::History, if op == "actors" { class } else { "" });
        let objects =
            history::list_objects_bounded(&state.store, &prefix, MAX_LIST_OBJECTS).await?;
        let mut names = Vec::new();
        for key in objects.keys() {
            if let Some((name, _)) = key.split_once('/') {
                let decoded = Key::from_url_path(name)?;
                names.push(decoded.as_ref().to_owned());
            }
        }
        if op == "classes" {
            let app = Key::from(
                format!("{}/apps/{}/{}", state.prefix, state.app.0, state.app.1)
                    .trim_start_matches('/'),
            );
            if let Some(current) = crate::status::current(&state.store, &app).await? {
                names.extend(
                    crate::status::manifest(&state.store, &app, &current.version)
                        .await?
                        .classes
                        .into_keys(),
                );
            }
        }
        return Ok(page(names, input["after"].as_str()));
    }
    let class = input["class"].as_str().context("Missing class")?;
    let encoded = input["actor"].as_str().context("Missing actor")?;
    ensure!(
        !encoded.contains('/') && !encoded.is_empty() && !encoded.starts_with('.'),
        "Invalid actor key"
    );
    wasmkeep_runtime::ActorAddress::new(&state.app.0, class, "validation")?;
    if op == "history" {
        let prefix = format!(
            "{}/",
            root.bucket_key(Family::History, &format!("{class}/{encoded}"))
        );
        let objects =
            history::list_objects_bounded(&state.store, &prefix, MAX_LIST_OBJECTS).await?;
        let listing = Listing::from_keys(objects.keys().map(String::as_str));
        let plan = plan_restore(&listing)?;
        let chain = history::ancestry(&listing, plan.head());
        let segments: Vec<_> = chain
            .iter()
            .rev()
            .take(50)
            .map(|id| json!({"generation":id.generation,"txid":id.txid}))
            .collect();
        let mut ordered: Vec<_> = objects.iter().collect();
        ordered.sort_by_key(|(_, meta)| std::cmp::Reverse(meta.last_modified));
        let recent: Vec<_> = ordered.into_iter().take(50).map(|(key, meta)| json!({"key":key,"bytes":meta.size,"uploaded":meta.last_modified.to_rfc3339()})).collect();
        return Ok(
            json!({"head":{"generation":plan.head().generation,"txid":plan.head().txid},"history":segments,"recent_objects":recent,"retained_objects":objects.len()}),
        );
    }
    ensure!(op == "sql", "Unknown operation");
    let name = wasmkeep_storage::decode_segment(encoded)
        .context("Hashed actor name cannot be decoded; use actor sql with its original name")?;
    let actor = wasmkeep_runtime::ActorAddress::new(&state.app.0, class, &name)?;
    let sql = input["sql"].as_str().context("Missing SQL")?.to_owned();
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("actor.db");
    let export = history::export(
        &state.store,
        &root,
        &actor,
        history::Point::Head,
        &path,
        2 << 30,
    )
    .await?;
    tokio::task::spawn_blocking(move || {
        // Keep both scratch ownership and concurrency permit through cancellation.
        let (_dir, _permit) = (dir, permit);
        let mut result = query(&path, &sql)?;
        result["head"] = json!({"generation":export.position.generation,"txid":export.position.txid,"owner_generation":export.owner_generation});
        Ok(result)
    }).await?
}
#[cfg(test)]
mod tests;
