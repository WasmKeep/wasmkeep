//! Serving node links (ADR 0030, sections 4 and 8): `GET` and `PUT` of
//! `/_wasmkeep/blobs/v1/<token>` on the public port, checked by the app
//! runtime its hostname picked, under that runtime's key. Nothing of the
//! token is used before its MAC is checked, and a token is refused before
//! any body is read.

use std::time::Duration;

use bytes::Bytes;
use futures_util::StreamExt as _;
use http_body_util::{BodyExt, StreamBody};
use hyper::body::Frame;
use hyper::{Request, Response, StatusCode};
use tokio::time::Instant;
use wasmkeep_wit::wasmkeep::actor::blobs::{BlobError, Condition};
use wasmtime_wasi_http::p2::bindings::http::types::ErrorCode;
use wasmtime_wasi_http::p2::body::HyperOutgoingBody;

use super::links::{NODE_LINK_PATH, disposition};
use super::streams::Source;
use super::token::{Grant, Token};
use super::{Blobs, Namespace, Write, keys, now_ms};
use crate::http::text_response;

/// How long past its expiry an upload that started in time may run.
const UPLOAD_GRACE_MS: u64 = 60 * 60 * 1_000;
/// How long an upload's body may send nothing before it is aborted.
const UPLOAD_IDLE: Duration = Duration::from_secs(60);
/// The slowest average an upload's body may arrive at, in bytes a second,
/// once [`UPLOAD_RATE_GRACE`] has passed: 16 KiB/s (128 kbit/s), below a
/// poor mobile link, so its whole size is not held for a client that
/// trickles.
const UPLOAD_MIN_RATE: u64 = 16 * 1024;
/// How long an upload's body may take before its average rate counts:
/// for a connection's slow start and a phone between cells.
const UPLOAD_RATE_GRACE: Duration = Duration::from_secs(30);

fn refuse(status: StatusCode, text: &str) -> Response<HyperOutgoingBody> {
    let mut r = text_response(status, text);
    secure(&mut r);
    r
}

/// The headers of section 8 on every answer from the app's origin: never
/// sniffed, sandboxed, not readable from another origin, never shared.
fn secure(r: &mut Response<HyperOutgoingBody>) {
    let h = r.headers_mut();
    for (name, value) in [
        ("x-content-type-options", "nosniff"),
        ("content-security-policy", "sandbox; default-src 'none'"),
        ("cross-origin-resource-policy", "same-origin"),
        ("referrer-policy", "no-referrer"),
    ] {
        h.insert(name, hyper::header::HeaderValue::from_static(value));
    }
    h.entry(hyper::header::CACHE_CONTROL)
        .or_insert(hyper::header::HeaderValue::from_static("private, no-store"));
}

/// The app is over its stored-bytes limit (`--max-app-blob-bytes`).
fn over_quota() -> Response<HyperOutgoingBody> {
    refuse(
        StatusCode::INSUFFICIENT_STORAGE,
        "the app is over its storage limit\n",
    )
}

/// Answers a node link. `deleted`: the app's tombstone was seen, and every
/// token is refused.
pub(crate) async fn serve<B>(
    blobs: &Blobs,
    deleted: bool,
    req: Request<B>,
) -> Response<HyperOutgoingBody>
where
    B: hyper::body::Body<Data = Bytes> + Unpin,
{
    let Some(links) = blobs.links() else {
        return refuse(StatusCode::NOT_FOUND, "not found\n");
    };
    if deleted {
        return refuse(StatusCode::FORBIDDEN, "this link is not valid\n");
    }
    let Some(spelled) = req.uri().path().strip_prefix(NODE_LINK_PATH) else {
        return refuse(StatusCode::NOT_FOUND, "not found\n");
    };
    if req.uri().query().is_some() {
        return refuse(StatusCode::FORBIDDEN, "this link is not valid\n");
    }
    let key = match links.key() {
        Ok(k) => k,
        Err(_) => {
            return refuse(
                StatusCode::SERVICE_UNAVAILABLE,
                "links are not served yet; retry\n",
            );
        }
    };
    let bad = || {
        crate::metrics::blob_node_link(blobs.labels(), "bad_token");
        refuse(StatusCode::FORBIDDEN, "this link is not valid\n")
    };
    let Ok(token) = Token::open(spelled, key) else {
        return bad();
    };
    // Under this app's key, so its own; the fields are checked anyway.
    if token.tenant != links.tenant || token.app != links.app || token.epoch != links.epoch {
        crate::metrics::blob_node_link(blobs.labels(), "wrong_app");
        return refuse(StatusCode::FORBIDDEN, "this link is not valid\n");
    }
    let Some(ns) = Namespace::from_token(&token.namespace) else {
        return bad();
    };
    if keys::check_key(&token.key).is_err() {
        return bad();
    }
    let now = now_ms();
    if now >= token.expires_at_ms {
        crate::metrics::blob_node_link(blobs.labels(), "expired");
        return refuse(StatusCode::FORBIDDEN, "this link has expired\n");
    }
    match &token.grant {
        Grant::Download { inline, filename } => {
            if req.method() != hyper::Method::GET {
                return refuse(
                    StatusCode::METHOD_NOT_ALLOWED,
                    "a download link takes GET only\n",
                );
            }
            download(blobs, &ns, &token, *inline, filename.as_deref(), now).await
        }
        Grant::Upload { size, content_type } => {
            if req.method() != hyper::Method::PUT {
                return refuse(
                    StatusCode::METHOD_NOT_ALLOWED,
                    "an upload link takes PUT only\n",
                );
            }
            upload(blobs, &ns, &token, *size, content_type, req).await
        }
    }
}

async fn download(
    blobs: &Blobs,
    ns: &Namespace,
    token: &Token,
    inline: bool,
    filename: Option<&str>,
    now: u64,
) -> Response<HyperOutgoingBody> {
    let (info, source) = match blobs.open_read(ns, &token.key, None).await {
        Ok(Some(found)) => found,
        Ok(None) => return refuse(StatusCode::NOT_FOUND, "no such file\n"),
        Err(_) => {
            return refuse(
                StatusCode::SERVICE_UNAVAILABLE,
                "the file cannot be read now; retry\n",
            );
        }
    };
    // An attachment is always bytes, which `nosniff` keeps from running as
    // script or style; inline only a type no browser runs, checked again
    // on the file as it is now.
    let content_type = if inline {
        if !super::links::inline_safe(&info.content_type) {
            return refuse(StatusCode::FORBIDDEN, "this file is not shown inline\n");
        }
        info.content_type.clone()
    } else {
        keys::DEFAULT_CONTENT_TYPE.to_owned()
    };
    let name = super::links::filename(filename, &token.key);
    let left = token.expires_at_ms.saturating_sub(now) / 1_000;
    let stream = body_of(source);
    let mut r = Response::new(
        BodyExt::map_err(StreamBody::new(stream), wasmtime_wasi_http::Error::from).boxed_unsync(),
    );
    let h = r.headers_mut();
    let mut set = |name: hyper::header::HeaderName, value: String| {
        if let Ok(v) = hyper::header::HeaderValue::from_str(&value) {
            h.insert(name, v);
        }
    };
    set(hyper::header::CONTENT_TYPE, content_type);
    set(hyper::header::CONTENT_LENGTH, info.size.to_string());
    set(
        hyper::header::CONTENT_DISPOSITION,
        disposition(inline, &name),
    );
    set(
        hyper::header::CACHE_CONTROL,
        format!("private, max-age={left}"),
    );
    secure(&mut r);
    crate::metrics::blob_node_link(blobs.labels(), "ok");
    r
}

/// A file's bytes as response frames.
fn body_of(
    source: Source,
) -> futures_util::stream::BoxStream<'static, Result<Frame<Bytes>, ErrorCode>> {
    match source {
        Source::Empty => futures_util::stream::empty().boxed(),
        Source::Bucket(s) => s
            .map(|r| {
                r.map(Frame::data)
                    .map_err(|_| ErrorCode::InternalError(None))
            })
            .boxed(),
        Source::Disk { file, left } => {
            futures_util::stream::try_unfold((Some(file), left), |(file, left)| async move {
                let Some(mut file) = file.filter(|_| left > 0) else {
                    return Ok(None);
                };
                let want = left.min(64 * 1024) as usize;
                let (file, buf) = tokio::task::spawn_blocking(move || {
                    use std::io::Read as _;
                    let mut buf = vec![0u8; want];
                    let n = file.read(&mut buf)?;
                    buf.truncate(n);
                    Ok::<_, std::io::Error>((file, buf))
                })
                .await
                .map_err(|_| ErrorCode::InternalError(None))?
                .map_err(|_| ErrorCode::InternalError(None))?;
                if buf.is_empty() {
                    return Err(ErrorCode::InternalError(None));
                }
                let left = left - buf.len() as u64;
                Ok(Some((Frame::data(Bytes::from(buf)), (Some(file), left))))
            })
            .boxed()
        }
    }
}

async fn upload<B>(
    blobs: &Blobs,
    ns: &Namespace,
    token: &Token,
    size: u64,
    content_type: &str,
    req: Request<B>,
) -> Response<HyperOutgoingBody>
where
    B: hyper::body::Body<Data = Bytes> + Unpin,
{
    let h = req.headers();
    // Refused before any byte of the body is read.
    if h.contains_key(hyper::header::TRANSFER_ENCODING) {
        return refuse(
            StatusCode::BAD_REQUEST,
            "an upload is sent with Content-Length, not chunked\n",
        );
    }
    let length = h
        .get(hyper::header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok());
    if length != Some(size) {
        crate::metrics::blob_node_link(blobs.labels(), "wrong_size");
        return refuse(
            StatusCode::BAD_REQUEST,
            "this link takes a body of another size\n",
        );
    }
    let types: Vec<&hyper::header::HeaderValue> =
        h.get_all(hyper::header::CONTENT_TYPE).iter().collect();
    if types.len() != 1 || types[0].as_bytes() != content_type.as_bytes() {
        crate::metrics::blob_node_link(blobs.labels(), "wrong_size");
        return refuse(
            StatusCode::BAD_REQUEST,
            "this link takes another Content-Type\n",
        );
    }
    // One body at a time for a key on this node: a link sent again while
    // its first body is still arriving is refused, not spooled twice.
    let Some(_receiving) = blobs.receive(ns, &token.key) else {
        crate::metrics::blob_node_link(blobs.labels(), "busy");
        return refuse(
            StatusCode::CONFLICT,
            "an upload to this link is already under way\n",
        );
    };
    // Everything from here, the commit included, ends by the link's expiry
    // plus the grace: dropped then, the spool and its budget go with it.
    let deadline_ms = token.expires_at_ms.saturating_add(UPLOAD_GRACE_MS);
    let left = Duration::from_millis(deadline_ms.saturating_sub(now_ms()));
    let req_body = req.into_body();
    match tokio::time::timeout(
        left,
        receive(blobs, ns, &token.key, size, content_type, req_body),
    )
    .await
    {
        Ok(answer) => answer,
        Err(_) => refuse(
            StatusCode::REQUEST_TIMEOUT,
            "the upload ran past its link's time\n",
        ),
    }
}

/// Why an upload's body was refused while it arrived.
enum Refused {
    /// Nothing for [`UPLOAD_IDLE`], or slower on average than
    /// [`UPLOAD_MIN_RATE`] once [`UPLOAD_RATE_GRACE`] has passed.
    Slow,
    WrongBody,
    Unavailable,
}

/// When an upload that started at `start` and has `got` bytes must have
/// sent its next: within [`UPLOAD_IDLE`] of now, and soon enough to keep
/// its average at [`UPLOAD_MIN_RATE`] after [`UPLOAD_RATE_GRACE`].
fn next_frame_by(start: Instant, got: u64) -> Instant {
    let credit = Duration::from_millis(got.saturating_mul(1_000) / UPLOAD_MIN_RATE);
    (Instant::now() + UPLOAD_IDLE).min(start + UPLOAD_RATE_GRACE + credit)
}

/// Reads exactly `size` bytes of `body` into a new spool, then commits it
/// as `key` if no file has that key.
async fn receive<B>(
    blobs: &Blobs,
    ns: &Namespace,
    key: &str,
    size: u64,
    content_type: &str,
    mut body: B,
) -> Response<HyperOutgoingBody>
where
    B: hyper::body::Body<Data = Bytes> + Unpin,
{
    match blobs.head(ns, key).await {
        Ok(None) => {}
        Ok(Some(_)) => {
            crate::metrics::blob_node_link(blobs.labels(), "exists");
            return refuse(StatusCode::PRECONDITION_FAILED, "the file exists\n");
        }
        Err(_) => {
            return refuse(
                StatusCode::SERVICE_UNAVAILABLE,
                "the file cannot be written now; retry\n",
            );
        }
    }
    // The whole body's size is held from the spool budgets before any of
    // it is read.
    let spool = match blobs.spool(ns, Some(size)).await {
        Ok(s) => s,
        Err(BlobError::QuotaExceeded) => return over_quota(),
        Err(_) => {
            return refuse(
                StatusCode::SERVICE_UNAVAILABLE,
                "the file cannot be written now; retry\n",
            );
        }
    };
    let start = Instant::now();
    let received = async {
        let mut got = 0u64;
        loop {
            let Ok(frame) = tokio::time::timeout_at(next_frame_by(start, got), body.frame()).await
            else {
                return Err(Refused::Slow);
            };
            let Some(frame) = frame else {
                break;
            };
            let Ok(frame) = frame else {
                return Err(Refused::WrongBody);
            };
            let Ok(data) = frame.into_data() else {
                // Trailers: an upload has none.
                return Err(Refused::WrongBody);
            };
            got += data.len() as u64;
            if got > size {
                return Err(Refused::WrongBody);
            }
            if spool.append(data).await.is_err() {
                return Err(Refused::Unavailable);
            }
        }
        if got == size {
            Ok(())
        } else {
            Err(Refused::WrongBody)
        }
    }
    .await;
    if let Err(why) = received {
        spool.abandon();
        return match why {
            Refused::Slow => {
                crate::metrics::blob_node_link(blobs.labels(), "slow");
                refuse(
                    StatusCode::REQUEST_TIMEOUT,
                    "the upload's body stopped arriving, or arrived too slowly\n",
                )
            }
            Refused::WrongBody => {
                crate::metrics::blob_node_link(blobs.labels(), "wrong_size");
                refuse(
                    StatusCode::BAD_REQUEST,
                    "the upload's body is not the one this link takes\n",
                )
            }
            Refused::Unavailable => refuse(
                StatusCode::SERVICE_UNAVAILABLE,
                "the file cannot be written now; retry\n",
            ),
        };
    }
    let spooled = match spool.finish().await {
        Ok(s) => s,
        Err(_) => {
            return refuse(
                StatusCode::SERVICE_UNAVAILABLE,
                "the file cannot be written now; retry\n",
            );
        }
    };
    let write = Write {
        content_type: content_type.to_owned(),
        condition: Condition::Absent,
    };
    match blobs.commit(ns, key, spooled, write).await {
        Ok(_) => {
            crate::metrics::blob_node_link(blobs.labels(), "ok");
            refuse(StatusCode::CREATED, "created\n")
        }
        Err(BlobError::Conflict) => {
            crate::metrics::blob_node_link(blobs.labels(), "exists");
            refuse(StatusCode::PRECONDITION_FAILED, "the file exists\n")
        }
        Err(BlobError::QuotaExceeded) => over_quota(),
        Err(BlobError::TooLarge) => refuse(StatusCode::PAYLOAD_TOO_LARGE, "too large\n"),
        Err(_) => refuse(
            StatusCode::SERVICE_UNAVAILABLE,
            "the file cannot be written now; retry\n",
        ),
    }
}
