//! An actor that stores and lists files with the Rust SDK
//! (`wasmkeep_sdk::blobs`, ADR 0030), behind the same routes as the
//! TypeScript one in `../files-ts` (crates/wasmkeep-server/tests/cluster/blobs.rs
//! runs both):
//! - `PUT /files/KEY[?type=T]` with the body (`If-None-Match: *` and
//!   `If-Match: E` as conditions): answers `ok ETAG SIZE TYPE`
//! - `GET /files/KEY`: the body; `GET /info/KEY`: `SIZE TYPE`
//! - `DELETE /files/KEY`: `ok`
//! - `GET /all?prefix=P&page=N`: every key under P, one a line, through
//!   the paged iterator, N a page
//! - `GET /page?prefix=P&limit=N[&after=A]`: one page, then `next=KEY|-`
//! - `GET /link/KEY`, `GET /upload-link/KEY`: a download or upload link
//! - Rust only: `POST /stream/KEY?n=N` writes N bytes (`i % 251`) through
//!   `open_write` (with `&leave=1`, returns `left` with the write still
//!   open); `GET /stream/KEY[?start=S&len=L]` reads through
//!   `open_read`: `read=N sum=SUM`
//!
//! A missing file answers 404 `none`; an error `err:KIND` with 400, 403,
//! 409, 413 or 503.

use std::io::{Read as _, Write as _};

use wasmkeep_sdk::Actor;
use wasmkeep_sdk::blobs::{self, BlobError, DownloadOptions, PutOptions};
use wasmkeep_sdk::http::{self, IncomingRequest, ResponseOutparam};

struct Files;

impl Actor for Files {}

fn query(q: &str, key: &str) -> Option<String> {
    q.split('&').find_map(|p| match p.split_once('=') {
        Some((k, v)) if k == key => Some(v.to_owned()),
        _ => None,
    })
}

fn error(e: &BlobError) -> (u16, String) {
    let (status, kind) = match e {
        BlobError::InvalidKey(_) => (400, "invalid-key"),
        BlobError::NotAllowed(_) => (403, "not-allowed"),
        BlobError::Conflict => (409, "conflict"),
        BlobError::TooLarge => (413, "too-large"),
        BlobError::QuotaExceeded => (507, "quota-exceeded"),
        BlobError::Unavailable(_) => (503, "unavailable"),
    };
    (status, format!("err:{kind}"))
}

fn route(
    method: &str,
    full: &str,
    if_match: Option<String>,
    if_none: bool,
    body: &[u8],
) -> (u16, Vec<u8>) {
    let (path, q) = full.split_once('?').unwrap_or((full, ""));
    let text = |r: Result<String, BlobError>| match r {
        Ok(t) => (200, t.into_bytes()),
        Err(e) => {
            let (s, t) = error(&e);
            (s, t.into_bytes())
        }
    };
    if let Some(key) = path.strip_prefix("/files/") {
        return match method {
            "PUT" => {
                let mut options = PutOptions::new();
                if let Some(t) = query(q, "type") {
                    options = options.content_type(t);
                }
                if let Some(e) = if_match {
                    options = options.if_match(e);
                } else if if_none {
                    options = options.if_absent();
                }
                text(
                    blobs::put(key, body, &options)
                        .map(|i| format!("ok {} {} {}", i.etag, i.size, i.content_type)),
                )
            }
            "DELETE" => text(blobs::delete(key).map(|()| "ok".into())),
            _ => match blobs::get(key) {
                Ok(Some((_, bytes))) => (200, bytes),
                Ok(None) => (404, b"none".to_vec()),
                Err(e) => text(Err(e)),
            },
        };
    }
    if let Some(key) = path.strip_prefix("/info/") {
        return match blobs::head(key) {
            Ok(Some(i)) => (200, format!("{} {}", i.size, i.content_type).into_bytes()),
            Ok(None) => (404, b"none".to_vec()),
            Err(e) => text(Err(e)),
        };
    }
    if path == "/all" {
        let prefix = query(q, "prefix").unwrap_or_default();
        let page = query(q, "page")
            .and_then(|p| p.parse().ok())
            .unwrap_or(1_000);
        return text(
            blobs::list_all_by(&prefix, page)
                .map(|i| i.map(|i| i.key))
                .collect::<Result<Vec<_>, _>>()
                .map(|keys| keys.join("\n")),
        );
    }
    if path == "/page" {
        let prefix = query(q, "prefix").unwrap_or_default();
        let limit = query(q, "limit")
            .and_then(|p| p.parse().ok())
            .unwrap_or(1_000);
        let after = query(q, "after");
        return text(blobs::list(&prefix, after.as_deref(), limit).map(|l| {
            let mut out: Vec<String> = l.entries.into_iter().map(|e| e.key).collect();
            out.push(format!("next={}", l.next.as_deref().unwrap_or("-")));
            out.join("\n")
        }));
    }
    if let Some(key) = path.strip_prefix("/link/") {
        return text(blobs::link_download(key, &DownloadOptions::default()));
    }
    if let Some(key) = path.strip_prefix("/upload-link/") {
        return text(blobs::link_upload(key, 10, "image/png", None).map(|l| l.url));
    }
    if let Some(key) = path.strip_prefix("/stream/") {
        if method == "POST" {
            let n: usize = query(q, "n").and_then(|v| v.parse().ok()).unwrap_or(0);
            // `i % 251`, made by copying one period: cheap enough for the
            // event's CPU budget on a slow machine (computing each byte of
            // 16 MiB is not).
            let period: Vec<u8> = (0..251u8).collect();
            let mut bytes = Vec::with_capacity(n);
            while bytes.len() < n {
                let take = (n - bytes.len()).min(period.len());
                bytes.extend_from_slice(&period[..take]);
            }
            let mut w = match blobs::open_write(key, &PutOptions::new()) {
                Ok(w) => w,
                Err(e) => return text(Err(e)),
            };
            if let Err(e) = w.write_all(&bytes) {
                return (500, format!("err:write:{e}").into_bytes());
            }
            if query(q, "leave").is_some() {
                // Returns with the write still open: neither finished nor
                // dropped.
                std::mem::forget(w);
                return (200, b"left".to_vec());
            }
            return text(w.finish().map(|i| format!("ok {}", i.size)));
        }
        let range = match (query(q, "start"), query(q, "len")) {
            (Some(s), Some(l)) => s.parse().ok().zip(l.parse().ok()),
            _ => None,
        };
        return match blobs::open_read(key, range) {
            Ok(Some((_, mut r))) => {
                // Read and added up a buffer at a time, never all of it
                // held: within the event's CPU budget on a slow machine.
                let mut buf = vec![0u8; 65_536];
                let (mut read, mut sum) = (0usize, 0u64);
                loop {
                    match r.read(&mut buf) {
                        Ok(0) => break,
                        Ok(k) => {
                            read += k;
                            sum += byte_sum(&buf[..k]);
                        }
                        Err(e) => return (500, format!("err:read:{e}").into_bytes()),
                    }
                }
                (200, format!("read={read} sum={sum}").into_bytes())
            }
            Ok(None) => (404, b"none".to_vec()),
            Err(e) => text(Err(e)),
        };
    }
    (404, b"no route".to_vec())
}

/// The bytes of `b` added up, eight at a time: cheap enough for the
/// event's CPU budget over 16 MiB on a slow machine.
fn byte_sum(b: &[u8]) -> u64 {
    const BYTES: u64 = 0x00ff_00ff_00ff_00ff;
    const PAIRS: u64 = 0x0000_ffff_0000_ffff;
    let mut words = b.chunks_exact(8);
    let mut sum = 0u64;
    for w in &mut words {
        let w = u64::from_le_bytes([w[0], w[1], w[2], w[3], w[4], w[5], w[6], w[7]]);
        let pairs = (w & BYTES) + ((w >> 8) & BYTES);
        let quads = (pairs & PAIRS) + ((pairs >> 16) & PAIRS);
        sum += (quads & 0xffff_ffff) + (quads >> 32);
    }
    sum + words.remainder().iter().map(|&x| u64::from(x)).sum::<u64>()
}

impl http::Guest for Files {
    fn handle(request: IncomingRequest, out: ResponseOutparam) {
        let full = request.path_with_query().unwrap_or_default();
        let method = http::method_name(&request.method());
        let headers = http::request_headers(&request);
        let if_match = headers.get("if-match").map(str::to_owned);
        let if_none = headers.get("if-none-match") == Some("*");
        let body = http::read_body(&request, 16 << 20).unwrap_or_default();
        let (status, answer) = route(&method, &full, if_match, if_none, &body);
        http::respond(out, status, "text/plain", &answer);
    }
}

wasmkeep_sdk::export!(Files);
