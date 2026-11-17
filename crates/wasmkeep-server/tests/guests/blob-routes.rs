// Routes over `wasmkeep:actor/blobs`, shared by the blob test actor and the
// test front (`include!`d, with `blobs` in scope). Query values are taken
// as they come (keys need no escaping):
// - `PUT /b/KEY[?type=T]` with the body: `put`, with `If-None-Match: *` as
//   `absent` and `If-Match: E` as `etag(E)`; answers `ok ETAG SIZE TYPE`
// - `GET /b/KEY`: `get`, the body; `GET /head/KEY`: `SIZE TYPE ETAG`
// - `DELETE /b/KEY`: `delete`, `ok`
// - `GET /list?prefix=P&after=A&limit=N`: one line `KEY SIZE TYPE` per
//   entry, then `next=KEY` or `next=-`
// - `POST /stream/KEY?n=N[&chunk=C]`: `open-write`, N bytes (`i % 251`)
//   written C at a time, `finish`; answers `ok SIZE`
// - `GET /stream/KEY[?start=S&len=L]`: `open-read`, answers
//   `size=SIZE read=N sum=SUM` (SUM: the bytes added up)
// Missing files answer 404 `none`; errors `err:KIND[:DETAIL]` with 400
// (invalid-key), 413 (too-large), 507 (quota), 409 (conflict), 403
// (not-allowed) or 503 (unavailable).

fn blob_query(query: &str, key: &str) -> Option<String> {
    query.split('&').find_map(|pair| match pair.split_once('=') {
        Some((k, v)) if k == key => Some(v.to_owned()),
        _ => None,
    })
}

fn blob_error(e: blobs::BlobError) -> (u16, Vec<u8>) {
    use blobs::BlobError as E;
    let (status, text) = match e {
        E::InvalidKey(d) => (400, format!("err:invalid-key:{d}")),
        E::TooLarge => (413, "err:too-large".to_owned()),
        E::QuotaExceeded => (507, "err:quota-exceeded".to_owned()),
        E::Conflict => (409, "err:conflict".to_owned()),
        E::NotAllowed(d) => (403, format!("err:not-allowed:{d}")),
        E::Unavailable(d) => (503, format!("err:unavailable:{d}")),
    };
    (status, text.into_bytes())
}

/// The answer to a blob route, or `None` for a path that is not one.
fn blob_route(
    method: &str,
    path_and_query: &str,
    if_match: Option<String>,
    if_none_match: bool,
    body: Vec<u8>,
) -> Option<(u16, Vec<u8>)> {
    let (path, query) = path_and_query.split_once('?').unwrap_or((path_and_query, ""));
    let ok = |s: String| (200, s.into_bytes());
    let answer = if let Some(key) = path.strip_prefix("/b/") {
        let key = key.to_owned();
        match method {
            "PUT" => {
                let condition = match (if_match, if_none_match) {
                    (Some(e), _) => blobs::Condition::Etag(e),
                    (None, true) => blobs::Condition::Absent,
                    (None, false) => blobs::Condition::Any,
                };
                let options = blobs::WriteOptions {
                    content_type: blob_query(query, "type"),
                    condition,
                };
                match blobs::put(&key, &body, &options) {
                    Ok(i) => ok(format!("ok {} {} {}", i.etag, i.size, i.content_type)),
                    Err(e) => blob_error(e),
                }
            }
            "GET" => match blobs::get(&key) {
                Ok(Some((_, bytes))) => (200, bytes),
                Ok(None) => (404, b"none".to_vec()),
                Err(e) => blob_error(e),
            },
            "DELETE" => match blobs::delete(&key) {
                Ok(()) => ok("ok".into()),
                Err(e) => blob_error(e),
            },
            _ => return None,
        }
    } else if let Some(key) = path.strip_prefix("/head/") {
        match blobs::head(key) {
            Ok(Some(i)) => ok(format!("{} {} {}", i.size, i.content_type, i.etag)),
            Ok(None) => (404, b"none".to_vec()),
            Err(e) => blob_error(e),
        }
    } else if path == "/list" {
        let prefix = blob_query(query, "prefix").unwrap_or_default();
        let after = blob_query(query, "after");
        let limit = blob_query(query, "limit")
            .and_then(|l| l.parse().ok())
            .unwrap_or(1_000);
        match blobs::list(&prefix, after.as_deref(), limit) {
            Ok(l) => {
                let mut out = String::new();
                for e in &l.entries {
                    out.push_str(&format!("{} {} {}\n", e.key, e.size, e.content_type));
                }
                out.push_str(&format!("next={}", l.next.as_deref().unwrap_or("-")));
                ok(out)
            }
            Err(e) => blob_error(e),
        }
    } else if let Some(key) = path.strip_prefix("/stream/") {
        match method {
            "POST" => {
                let n: usize = blob_query(query, "n").and_then(|v| v.parse().ok()).unwrap_or(0);
                let chunk: usize = blob_query(query, "chunk")
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(65_536);
                let upload = match blobs::open_write(key, &blobs::WriteOptions {
                    content_type: None,
                    condition: blobs::Condition::Any,
                }) {
                    Ok(u) => u,
                    Err(e) => return Some(blob_error(e)),
                };
                let stream = match upload.body() {
                    Ok(s) => s,
                    Err(e) => return Some(blob_error(e)),
                };
                let bytes: Vec<u8> = (0..n).map(|i| (i % 251) as u8).collect();
                // `check-write`, `write`, waiting when it takes nothing: the
                // stream's own protocol, at most C bytes a write.
                let mut rest = &bytes[..];
                while !rest.is_empty() {
                    let n = match stream.check_write() {
                        Ok(0) => {
                            stream.subscribe().block();
                            continue;
                        }
                        Ok(n) => (n as usize).min(chunk.max(1)).min(rest.len()),
                        Err(e) => return Some((500, format!("err:write:{e:?}").into_bytes())),
                    };
                    if let Err(e) = stream.write(&rest[..n]) {
                        return Some((500, format!("err:write:{e:?}").into_bytes()));
                    }
                    rest = &rest[n..];
                }
                if let Err(e) = stream.blocking_flush() {
                    return Some((500, format!("err:flush:{e:?}").into_bytes()));
                }
                drop(stream);
                match blobs::Upload::finish(upload) {
                    Ok(i) => ok(format!("ok {}", i.size)),
                    Err(e) => blob_error(e),
                }
            }
            "GET" => {
                let range = match (blob_query(query, "start"), blob_query(query, "len")) {
                    (Some(s), Some(l)) => match (s.parse(), l.parse()) {
                        (Ok(s), Ok(l)) => Some((s, l)),
                        _ => None,
                    },
                    _ => None,
                };
                match blobs::open_read(key, range) {
                    Ok(Some((info, stream))) => {
                        let (mut read, mut sum) = (0u64, 0u64);
                        while let Ok(b) = stream.blocking_read(65_536) {
                            read += b.len() as u64;
                            sum += b.iter().map(|&x| u64::from(x)).sum::<u64>();
                        }
                        ok(format!("size={} read={read} sum={sum}", info.size))
                    }
                    Ok(None) => (404, b"none".to_vec()),
                    Err(e) => blob_error(e),
                }
            }
            _ => return None,
        }
    } else {
        return None;
    };
    Some(answer)
}
