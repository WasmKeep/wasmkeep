//! Presigned links (ADR 0030, section 4): the signer-only client for the
//! link endpoint, and the start proof that lets a process trust a bucket's
//! signatures at all. Run once per process; its result holds for every app
//! runtime, a restarted one included.
//!
//! Only downloads are presigned: `object_store` refuses to sign
//! `content-length`, so a presigned upload could not bind its size, and
//! uploads are node links. The proof therefore checks what a download link
//! relies on: the bytes with the signed `response-content-type` and
//! `response-content-disposition`, a link for one key reading no other, and
//! a signed parameter changed being refused.

use std::sync::Arc;
use std::time::Duration;

use anyhow::Context as _;
use object_store::path::Path;
use object_store::signer::{Method, SignedUrlOptions, Signer};
use object_store::{ObjectStore, ObjectStoreExt as _, PutPayload};

/// `--blob-links`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, clap::ValueEnum)]
pub enum Mode {
    /// Presign downloads on a bucket whose start proof passed, node links
    /// otherwise.
    #[default]
    Auto,
    /// Presign downloads; refuse to start if the proof fails.
    Presigned,
    /// Node links only (no proof).
    Node,
}

/// The type and disposition the proof's link asks for, over what the probe
/// was stored with.
const SIGNED_TYPE: &str = "application/octet-stream";
const SIGNED_DISPOSITION: &str = "attachment; filename=\"probe.bin\"";
const STORED_TYPE: &str = "text/html";

/// Builds the signer-only client: the node's bucket and credentials, from
/// the environment like its store, for `endpoint` (browsers' way to the
/// bucket) when given.
///
/// # Errors
/// The client cannot be built.
pub fn signer(bucket_url: &str, endpoint: Option<&str>) -> anyhow::Result<Arc<dyn Signer>> {
    let (bucket, _) = crate::parse_bucket(bucket_url)?;
    let mut b = object_store::aws::AmazonS3Builder::from_env().with_bucket_name(bucket);
    if let Some(e) = endpoint {
        b = b.with_endpoint(e).with_allow_http(e.starts_with("http://"));
    }
    Ok(Arc::new(b.build().context("building the link signer")?))
}

/// One case of the proof, for a message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Failed(pub String);

impl std::fmt::Display for Failed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// A probe folder no other run uses.
fn random_hex() -> String {
    let t = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    format!("{:016x}{:08x}", t.as_nanos() as u64, std::process::id())
}

/// The start proof: signs download links for the link endpoint and sends
/// them, with `http`, checking every case a download link relies on.
/// Writes and removes two objects under `<prefix>probe/blob-links/`.
///
/// # Errors
/// The first case that failed.
pub async fn prove(
    signer: &dyn Signer,
    store: &Arc<dyn ObjectStore>,
    prefix: &str,
    http: &reqwest::Client,
) -> Result<(), Failed> {
    let base = format!("{}probe/blob-links/{}", with_slash(prefix), random_hex());
    let (a, b) = (
        Path::from(format!("{base}/a")),
        Path::from(format!("{base}/b")),
    );
    let body_a = b"<p>wasmkeep link probe a</p>".to_vec();
    let mut attributes = object_store::Attributes::new();
    attributes.insert(object_store::Attribute::ContentType, STORED_TYPE.into());
    for (p, body) in [(&a, body_a.clone()), (&b, b"b".to_vec())] {
        let options = object_store::PutOptions {
            attributes: attributes.clone(),
            ..Default::default()
        };
        store
            .put_opts(p, PutPayload::from(body), options)
            .await
            .map_err(|e| Failed(format!("writing the probe: {e}")))?;
    }
    let outcome = cases(signer, &a, &body_a, http).await;
    for p in [&a, &b] {
        let _ = store.delete(p).await;
    }
    outcome
}

fn with_slash(prefix: &str) -> String {
    if prefix.is_empty() || prefix.ends_with('/') {
        prefix.to_owned()
    } else {
        format!("{prefix}/")
    }
}

/// A download link of `path` for `secs`, with the overrides a node signs.
pub async fn download_link(
    signer: &dyn Signer,
    path: &Path,
    secs: u64,
) -> Result<reqwest::Url, Failed> {
    let options = SignedUrlOptions::new().with_query([
        ("response-content-disposition", SIGNED_DISPOSITION),
        ("response-content-type", SIGNED_TYPE),
        ("response-cache-control", "private, max-age=60"),
        ("response-content-encoding", "identity"),
    ]);
    let url = signer
        .signed_url_opts(Method::GET, path, Duration::from_secs(secs), &options)
        .await
        .map_err(|e| Failed(format!("signing: {e}")))?;
    reqwest::Url::parse(url.as_str()).map_err(|e| Failed(e.to_string()))
}

async fn get(http: &reqwest::Client, url: reqwest::Url) -> Result<reqwest::Response, Failed> {
    http.get(url)
        .send()
        .await
        .map_err(|e| Failed(format!("the link endpoint did not answer: {e}")))
}

async fn cases(
    signer: &dyn Signer,
    a: &Path,
    body_a: &[u8],
    http: &reqwest::Client,
) -> Result<(), Failed> {
    // 1. The bytes, with the signed type and disposition over the stored.
    let link = download_link(signer, a, 60).await?;
    let r = get(http, link.clone()).await?;
    if !r.status().is_success() {
        return Err(Failed(format!(
            "a download link was refused: {}",
            r.status()
        )));
    }
    let header = |name: &str| {
        r.headers()
            .get(name)
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned)
    };
    if header("content-type").as_deref() != Some(SIGNED_TYPE) {
        return Err(Failed(format!(
            "a download link's response-content-type was not applied: {:?}",
            header("content-type")
        )));
    }
    if header("content-disposition").as_deref() != Some(SIGNED_DISPOSITION) {
        return Err(Failed(format!(
            "a download link's response-content-disposition was not applied: {:?}",
            header("content-disposition")
        )));
    }
    let got = r
        .bytes()
        .await
        .map_err(|e| Failed(format!("reading a download: {e}")))?;
    if got.as_ref() != body_a {
        return Err(Failed("a download link returned other bytes".into()));
    }
    // 2. A link signed for one key reads no other.
    let mut other = link.clone();
    let path = other.path().to_owned();
    let Some(cut) = path.rfind('/') else {
        return Err(Failed("a link without a path".into()));
    };
    other.set_path(&format!("{}/b", &path[..cut]));
    let r = get(http, other).await?;
    if r.status().is_success() {
        return Err(Failed("a link signed for one key read another".into()));
    }
    // 3. A signed parameter changed is refused.
    let mut changed = link;
    let pairs: Vec<(String, String)> = changed
        .query_pairs()
        .map(|(k, v)| {
            let v = if k == "response-content-type" {
                STORED_TYPE.to_owned()
            } else {
                v.into_owned()
            };
            (k.into_owned(), v)
        })
        .collect();
    changed.query_pairs_mut().clear().extend_pairs(pairs);
    let r = get(http, changed).await?;
    if r.status().is_success() {
        return Err(Failed(
            "a link whose signed response-content-type was changed was taken".into(),
        ));
    }
    Ok(())
}

/// The `blob-links` checks of `cluster diagnose` (ADR 0030, section 10),
/// by name: the start proof's cases, and a link signed for 2 s refused
/// after 4 s. `Err` is a warning: nodes would use node links.
pub async fn checks(
    signer: &dyn Signer,
    store: &Arc<dyn ObjectStore>,
    prefix: &str,
    http: &reqwest::Client,
    expiry: (Duration, Duration),
) -> Vec<(&'static str, Result<String, Failed>)> {
    let mut out = vec![(
        "presigned-download",
        prove(signer, store, prefix, http)
            .await
            .map(|()| "the signed type and name, one key only, no changed parameter".into()),
    )];
    let path = Path::from(format!(
        "{}probe/blob-links/{}/e",
        with_slash(prefix),
        random_hex()
    ));
    let expired = async {
        store
            .put(&path, PutPayload::from_static(b"e"))
            .await
            .map_err(|e| Failed(format!("writing the probe: {e}")))?;
        let link = download_link(signer, &path, expiry.0.as_secs().max(1)).await?;
        tokio::time::sleep(expiry.1).await;
        let r = get(http, link).await?;
        if r.status().is_success() {
            return Err(Failed(format!(
                "a link signed for {} s was taken after {} s",
                expiry.0.as_secs(),
                expiry.1.as_secs()
            )));
        }
        Ok(format!("refused after {} s", expiry.1.as_secs()))
    }
    .await;
    let _ = store.delete(&path).await;
    out.push(("presigned-expiry", expired));
    out
}

/// The presigner a process starts with under `mode`: `None` for node
/// links. With `auto`, a failed proof is said once, and nodes use node
/// links.
///
/// # Errors
/// `presigned` and the proof failed, or the signer cannot be built.
pub async fn presigner(
    mode: Mode,
    bucket_url: &str,
    endpoint: Option<&str>,
    store: &Arc<dyn ObjectStore>,
    prefix: &str,
) -> anyhow::Result<Option<Arc<dyn Signer>>> {
    if mode == Mode::Node {
        tracing::info!("blob links: node links (--blob-links node)");
        return Ok(None);
    }
    presigner_with(mode, signer(bucket_url, endpoint)?, store, prefix).await
}

/// [`presigner`] with the signer built.
///
/// # Errors
/// `presigned` and the proof failed.
pub async fn presigner_with(
    mode: Mode,
    signer: Arc<dyn Signer>,
    store: &Arc<dyn ObjectStore>,
    prefix: &str,
) -> anyhow::Result<Option<Arc<dyn Signer>>> {
    if mode == Mode::Node {
        return Ok(None);
    }
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .context("building the link proof's client")?;
    match prove(signer.as_ref(), store, prefix, &http).await {
        Ok(()) => {
            tracing::info!("blob links: downloads presigned (the start proof passed)");
            Ok(Some(signer))
        }
        Err(e) if mode == Mode::Auto => {
            tracing::warn!(reason = %e, "presigned links: not safe on this bucket; nodes use node links (--blob-links node)");
            Ok(None)
        }
        Err(e) => anyhow::bail!("--blob-links presigned: the start proof failed: {e}"),
    }
}

#[cfg(test)]
mod tests;
