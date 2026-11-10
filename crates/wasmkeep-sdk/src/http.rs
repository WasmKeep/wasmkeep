//! The `wasi:http` request handler, plus small helpers for the common cases.

pub use wasip2::exports::http::incoming_handler::Guest;
pub use wasip2::http::types::{
    Fields, IncomingRequest, Method, OutgoingBody, OutgoingResponse, ResponseOutparam,
};

use std::io::{Read as _, Write as _};

/// Sends a complete response with one body. Errors writing the body are
/// ignored: the client has gone away and there is nobody left to tell.
pub fn respond(out: ResponseOutparam, status: u16, content_type: &str, body: &[u8]) {
    let headers = Fields::new();
    let _ = headers.set("content-type", &[content_type.as_bytes().to_vec()]);
    let response = OutgoingResponse::new(headers);
    let _ = response.set_status_code(status);
    let Ok(outgoing) = response.body() else {
        return;
    };
    ResponseOutparam::set(out, Ok(response));
    if let Ok(mut stream) = outgoing.write() {
        let _ = stream.write_all(body);
        let _ = stream.flush();
    }
    let _ = OutgoingBody::finish(outgoing, None);
}

/// Reads the whole request body, up to `limit` bytes. `None` if the body
/// was already consumed, could not be read, or is longer than `limit`.
pub fn read_body(request: &IncomingRequest, limit: usize) -> Option<Vec<u8>> {
    let body = request.consume().ok()?;
    let mut stream = body.stream().ok()?;
    let mut buf = Vec::new();
    let read = (&mut stream)
        .take(limit as u64 + 1)
        .read_to_end(&mut buf)
        .ok()?;
    drop(stream);
    (read <= limit).then_some(buf)
}

/// The request method as an uppercase string.
pub fn method_name(method: &Method) -> String {
    match method {
        Method::Get => "GET".into(),
        Method::Head => "HEAD".into(),
        Method::Post => "POST".into(),
        Method::Put => "PUT".into(),
        Method::Delete => "DELETE".into(),
        Method::Connect => "CONNECT".into(),
        Method::Options => "OPTIONS".into(),
        Method::Trace => "TRACE".into(),
        Method::Patch => "PATCH".into(),
        Method::Other(other) => other.clone(),
    }
}

/// A response from [`fetch`].
#[derive(Debug, Clone)]
pub struct Fetched {
    /// The HTTP status.
    pub status: u16,
    /// The whole body, or its first `limit` bytes when it is longer (see
    /// [`fetch`]).
    pub body: Vec<u8>,
}

/// Sends an outbound HTTP request and waits for the response: `url` is
/// `http://` or `https://`, `timeout` bounds connecting, the first byte of the
/// response, and each gap between bytes. The body is read up to `limit`
/// bytes (the rest is dropped unread, and the response still succeeds).
/// Errors are described as text (DNS, refused, timeout, ...), ready to log
/// or record. A body that breaks before its end, and before the limit (the
/// connection closes short of its Content-Length or inside a chunk, a
/// reset, a timeout between bytes), is an error starting
/// `<url>: incomplete body:`, not the status with the bytes that came.
///
/// The actor's event waits meanwhile (other events to it queue); waiting
/// does not count against its CPU budget, but does against the event's
/// wall-clock limit (30 s by default).
pub fn fetch(
    method: &str,
    url: &str,
    headers: &[(&str, &str)],
    body: &[u8],
    timeout: std::time::Duration,
    limit: usize,
) -> Result<Fetched, String> {
    use wasip2::http::outgoing_handler;
    use wasip2::http::types::{OutgoingRequest, RequestOptions, Scheme};

    let (scheme, rest) = url
        .split_once("://")
        .ok_or_else(|| format!("{url}: no scheme"))?;
    let scheme = match scheme {
        "http" => Scheme::Http,
        "https" => Scheme::Https,
        other => return Err(format!("{url}: unsupported scheme {other}")),
    };
    let (authority, path) = match rest.find(['/', '?']) {
        Some(i) => (&rest[..i], &rest[i..]),
        None => (rest, "/"),
    };
    let path = if path.starts_with('/') {
        path.to_owned()
    } else {
        format!("/{path}")
    };
    if authority.is_empty() {
        return Err(format!("{url}: no host"));
    }

    let fields = Fields::new();
    for (name, value) in headers {
        fields
            .append(name, value.as_bytes())
            .map_err(|e| format!("header {name}: {e:?}"))?;
    }
    // A known length rather than a chunked body: some servers refuse
    // requests without one.
    if !body.is_empty()
        && !headers
            .iter()
            .any(|(n, _)| n.eq_ignore_ascii_case("content-length"))
    {
        fields
            .append("content-length", body.len().to_string().as_bytes())
            .map_err(|e| format!("header content-length: {e:?}"))?;
    }
    let request = OutgoingRequest::new(fields);
    let method = match method.to_ascii_uppercase().as_str() {
        "GET" => Method::Get,
        "HEAD" => Method::Head,
        "POST" => Method::Post,
        "PUT" => Method::Put,
        "DELETE" => Method::Delete,
        "PATCH" => Method::Patch,
        "OPTIONS" => Method::Options,
        other => Method::Other(other.to_owned()),
    };
    request
        .set_method(&method)
        .and_then(|()| request.set_scheme(Some(&scheme)))
        .and_then(|()| request.set_authority(Some(authority)))
        .and_then(|()| request.set_path_with_query(Some(&path)))
        .map_err(|()| format!("{url}: invalid request"))?;
    let outgoing = request
        .body()
        .map_err(|()| "request body unavailable".to_owned())?;

    let options = RequestOptions::new();
    let nanos = u64::try_from(timeout.as_nanos()).unwrap_or(u64::MAX);
    let _ = options.set_connect_timeout(Some(nanos));
    let _ = options.set_first_byte_timeout(Some(nanos));
    let _ = options.set_between_bytes_timeout(Some(nanos));

    let future = outgoing_handler::handle(request, Some(options)).map_err(|e| format!("{e:?}"))?;
    let wait = || loop {
        if let Some(result) = future.get() {
            break result;
        }
        future.subscribe().block();
    };
    let mut sent = Ok(());
    if !body.is_empty() {
        let mut stream = outgoing
            .write()
            .map_err(|()| "request body unavailable".to_owned())?;
        sent = stream
            .write_all(body)
            .map_err(|e| e.to_string())
            .and_then(|()| stream.flush().map_err(|e| e.to_string()));
    }
    let response = match sent {
        Ok(()) => {
            OutgoingBody::finish(outgoing, None).map_err(|e| format!("{e:?}"))?;
            wait()
                .map_err(|()| "response already taken".to_owned())?
                .map_err(|e| format!("{e:?}"))?
        }
        // The body's stream closes when the request ends before the guest
        // is done with it: either it failed (refused, no connection), and
        // the response says why where it can, or the server already
        // answered and closed (it had the whole body, or answered early),
        // and that answer is the result.
        Err(e) => {
            drop(outgoing);
            match wait() {
                Ok(Ok(response)) => response,
                Ok(Err(code)) => return Err(format!("{code:?}")),
                Err(()) => return Err(format!("sending the body: {e}")),
            }
        }
    };
    let status = response.status();
    let incoming = response
        .consume()
        .map_err(|()| format!("{url}: response body already taken"))?;
    let body = read_response_body(incoming, limit).map_err(|e| format!("{url}: {e}"))?;
    Ok(Fetched { status, body })
}

/// Reads a response body up to `limit` bytes. Past the limit nothing more is
/// read (nor checked). Before it, the body must end normally: a stream that
/// breaks (`last-operation-failed`: a reset, a close short of the
/// Content-Length or inside a chunk, a timeout between bytes), or an error
/// reported with the trailers after a normal close, fails with
/// `incomplete body: ...` rather than passing the bytes that came for the
/// whole answer.
fn read_response_body(
    incoming: wasip2::http::types::IncomingBody,
    limit: usize,
) -> Result<Vec<u8>, String> {
    use wasip2::http::types::{IncomingBody, http_error_code};
    use wasip2::io::streams::StreamError;

    let stream = incoming
        .stream()
        .map_err(|()| "incomplete body: its stream was already taken".to_owned())?;
    let mut body = Vec::new();
    while body.len() < limit {
        let want = (limit - body.len()).min(65_536) as u64;
        match stream.blocking_read(want) {
            Ok(chunk) => body.extend_from_slice(&chunk),
            Err(StreamError::Closed) => break,
            Err(StreamError::LastOperationFailed(e)) => {
                let why =
                    http_error_code(&e).map_or_else(|| e.to_debug_string(), |c| format!("{c:?}"));
                return Err(format!("incomplete body: {why}"));
            }
        }
    }
    if body.len() >= limit {
        // The rest is dropped unread.
        return Ok(body);
    }
    // Closed: the body's own end, unless the trailers say otherwise.
    drop(stream);
    let trailers = IncomingBody::finish(incoming);
    // Declared after `trailers`, so dropped before it: trailers received are
    // the future's child.
    let result = loop {
        if let Some(result) = trailers.get() {
            break result;
        }
        trailers.subscribe().block();
    };
    match result {
        Ok(Ok(_)) => Ok(body),
        Ok(Err(code)) => Err(format!("incomplete body: {code:?}")),
        Err(()) => Err("incomplete body: its trailers were already taken".to_owned()),
    }
}
