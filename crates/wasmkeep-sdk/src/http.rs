//! The `wasi:http` request handler, plus small helpers for the common cases.

pub use wasip2::exports::http::incoming_handler::Guest;
pub use wasip2::http::types::{
    Fields, IncomingRequest, Method, OutgoingBody, OutgoingResponse, ResponseOutparam,
};

use std::io::{Read as _, Write as _};

/// Sends a complete response with one body. A caller that left (the
/// stream's `closed`) ends the write quietly; any other failure writing or
/// finishing the body is the actor's and is logged (there is no answer
/// left to carry it).
pub fn respond(out: ResponseOutparam, status: u16, content_type: &str, body: &[u8]) {
    let headers = Fields::new();
    let _ = headers.set("content-type", &[content_type.as_bytes().to_vec()]);
    let response = OutgoingResponse::new(headers);
    let _ = response.set_status_code(status);
    let Ok(outgoing) = response.body() else {
        return;
    };
    ResponseOutparam::set(out, Ok(response));
    write_body(outgoing, body);
}

/// Sends `response` (status, headers, body) as the answer: an actor's
/// response to a call, passed on, or [`crate::front::unavailable`]'s.
/// Headers the host would not accept are left out.
pub fn respond_with(out: ResponseOutparam, response: &crate::actors::Response) {
    let headers = Fields::new();
    for (name, value) in &response.headers {
        let _ = headers.append(name, value.as_bytes());
    }
    let outgoing_response = OutgoingResponse::new(headers);
    let _ = outgoing_response.set_status_code(response.status);
    let Ok(outgoing) = outgoing_response.body() else {
        return;
    };
    ResponseOutparam::set(out, Ok(outgoing_response));
    write_body(outgoing, &response.body);
}

/// Writes a handed-over response's body and finishes it. The stream's
/// `closed` is the caller leaving: nothing to tell anyone. Another failure
/// (a body that does not match its content-length, say) is logged, after
/// the stream is released, as the TypeScript SDK throws it.
fn write_body(outgoing: OutgoingBody, body: &[u8]) {
    let mut left = false;
    if let Ok(mut stream) = outgoing.write() {
        // wasip2's `Write` reports the stream's `closed` as a zero-length
        // write (or an unexpected end): the caller left.
        if let Err(e) = stream
            .write_all(body)
            .and_then(|()| std::io::Write::flush(&mut stream))
        {
            left = matches!(
                e.kind(),
                std::io::ErrorKind::WriteZero | std::io::ErrorKind::UnexpectedEof
            );
            drop(stream);
            if !left {
                eprintln!("response body not written: {e}");
                return;
            }
        }
    }
    if let Err(e) = OutgoingBody::finish(outgoing, None)
        && !left
    {
        eprintln!("response body not finished: {e:?}");
    }
}

/// Sends a complete response with the given headers, every pair as given
/// and in order (repeats of a name, `Set-Cookie` among them, stay separate
/// headers), and `content-type: text/plain; charset=utf-8` unless one is
/// set. Headers the host would not accept are left out.
pub fn respond_headers(
    out: ResponseOutparam,
    status: u16,
    headers: &crate::headers::Headers,
    body: &[u8],
) {
    let mut headers = headers.clone();
    if !headers.contains("content-type") {
        headers.set("content-type", "text/plain; charset=utf-8");
    }
    respond_with(
        out,
        &crate::actors::Response {
            status,
            headers: headers.into_pairs(),
            body: body.to_vec(),
        },
    );
}

/// The headers of an incoming request in the order received, repeats
/// included (a value that is not UTF-8 has its bad bytes replaced).
pub fn request_headers(request: &IncomingRequest) -> crate::headers::Headers {
    crate::headers::Headers::from_byte_pairs(request.headers().entries())
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

/// A response from [`fetch`] or [`fetch_all`].
#[derive(Debug, Clone)]
pub struct Fetched {
    /// The HTTP status.
    pub status: u16,
    /// Every response header in the order received, repeats included (each
    /// `Set-Cookie` is its own entry).
    pub headers: crate::headers::Headers,
    /// The whole body, or its first `limit` bytes when it is longer (see
    /// [`fetch`]).
    pub body: Vec<u8>,
}

/// One request of a [`fetch_all`] batch: a GET with no headers and no body
/// unless set.
#[derive(Debug, Clone, Copy)]
pub struct FetchRequest<'a> {
    /// The method (`GET`, `POST`, ...).
    pub method: &'a str,
    /// `http://` or `https://`.
    pub url: &'a str,
    /// Headers to send.
    pub headers: &'a [(&'a str, &'a str)],
    /// The body to send (none when empty).
    pub body: &'a [u8],
}

impl<'a> FetchRequest<'a> {
    /// A GET of `url`.
    #[must_use]
    pub fn get(url: &'a str) -> Self {
        Self::new("GET", url)
    }

    /// A `method` request to `url`.
    #[must_use]
    pub fn new(method: &'a str, url: &'a str) -> Self {
        Self {
            method,
            url,
            headers: &[],
            body: &[],
        }
    }

    /// With these headers.
    #[must_use]
    pub fn headers(mut self, headers: &'a [(&'a str, &'a str)]) -> Self {
        self.headers = headers;
        self
    }

    /// With this body.
    #[must_use]
    pub fn body(mut self, body: &'a [u8]) -> Self {
        self.body = body;
        self
    }
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
/// wall-clock limit (30 s by default). For several requests at once, see
/// [`fetch_all`].
pub fn fetch(
    method: &str,
    url: &str,
    headers: &[(&str, &str)],
    body: &[u8],
    timeout: std::time::Duration,
    limit: usize,
) -> Result<Fetched, String> {
    let request = FetchRequest {
        method,
        url,
        headers,
        body,
    };
    fetch_all(&[request], timeout, limit)
        .into_iter()
        .next()
        .unwrap_or_else(|| Err(format!("{url}: no answer")))
}

/// Requests a batch keeps open at once: the host's limit per actor
/// (`PER_ACTOR_REQUESTS` in the server's egress rules).
const MAX_OPEN: usize = 16;
/// Times a request the host refused for having too many open is tried
/// again, [`RETRY_WAIT`] apart: the host frees a finished request's place
/// a moment after its body ends.
const RETRIES: u32 = 100;
const RETRY_WAIT: u64 = 1_000_000; // 1 ms, in nanoseconds

/// Sends every request of `requests` at once and waits for all of them:
/// one result per request, in the same order, each as [`fetch`] gives it
/// (`timeout` and `limit` apply to each). One failing does not fail the
/// others.
///
/// The requests are open together, their responses and bodies read as
/// they come, so the batch takes about as long as its slowest request
/// rather than the sum. Request bodies are written one after the other,
/// once the requests that fit have all started. The host lets an actor
/// have 16 requests open at once: past 16, the rest start as earlier ones
/// finish. The host's egress rules apply to each.
///
/// The actor's event waits meanwhile, as for [`fetch`].
pub fn fetch_all(
    requests: &[FetchRequest<'_>],
    timeout: std::time::Duration,
    limit: usize,
) -> Vec<Result<Fetched, String>> {
    use wasip2::clocks::monotonic_clock;
    use wasip2::http::types::ErrorCode;
    use wasip2::io::poll::{Pollable, poll};

    let mut states: Vec<State> = requests.iter().map(|_| State::Queued(0)).collect();
    // A refused request is started again only once a retry wait has passed.
    let mut waited = false;
    loop {
        // Start what fits: every `handle` first, then the bodies.
        let mut open = states.iter().filter(|s| s.is_open()).count();
        let mut started = Vec::new();
        for (i, state) in states.iter_mut().enumerate() {
            if open >= MAX_OPEN {
                break;
            }
            let State::Queued(tries) = *state else {
                continue;
            };
            if tries > 0 && !waited {
                continue;
            }
            *state = match start(&requests[i], timeout) {
                Ok((future, outgoing)) => {
                    open += 1;
                    started.push((i, outgoing));
                    State::Head {
                        future,
                        sending: None,
                        tries,
                    }
                }
                Err(e) => State::Done(Err(e)),
            };
        }
        for (i, outgoing) in started {
            match send_body(outgoing, requests[i].body) {
                Sent::Yes => {}
                Sent::Body(e) => {
                    if let State::Head { sending, .. } = &mut states[i] {
                        *sending = Some(e);
                    }
                }
                Sent::Unfinished(e) => states[i] = State::Done(Err(e)),
            }
        }

        // Advance each as far as it goes without waiting.
        let mut retrying = false;
        for (i, state) in states.iter_mut().enumerate() {
            advance(state, requests[i].url, limit);
            match *state {
                State::Refused(tries) if tries < RETRIES => *state = State::Queued(tries + 1),
                State::Refused(_) => {
                    *state = State::Done(Err(format!("{:?}", ErrorCode::ConnectionLimitReached)));
                }
                _ => {}
            }
            retrying |= matches!(state, State::Queued(t) if *t > 0);
        }
        if states.iter().all(|s| matches!(s, State::Done(_))) {
            break;
        }
        // Wait for any of them, and for a moment when one is to be retried
        // (then something is always waited for: polling nothing traps).
        let mut pollables: Vec<Pollable> = states.iter().filter_map(State::pollable).collect();
        // Every request that was open is done, and others are still to
        // start: there is nothing to wait for, so start them.
        if pollables.is_empty() && !retrying {
            waited = false;
            continue;
        }
        let clock = retrying.then(|| {
            pollables.push(monotonic_clock::subscribe_duration(RETRY_WAIT));
            pollables.len() - 1
        });
        let refs: Vec<&Pollable> = pollables.iter().collect();
        let ready = poll(&refs);
        waited = clock.is_some_and(|c| ready.iter().any(|&r| r as usize == c));
        // Pollables are their resources' children: dropped here, before
        // any of those changes.
        drop(refs);
        drop(pollables);
    }
    states
        .into_iter()
        .map(|s| match s {
            State::Done(result) => result,
            _ => Err("unfinished".to_owned()),
        })
        .collect()
}

/// Where one request of a batch is. Fields drop in order: each resource
/// before the one it came from.
enum State {
    /// Not started; the number is how many times the host refused it for
    /// having too many open.
    Queued(u32),
    /// Refused that many times: to start again, or give up.
    Refused(u32),
    /// Waiting for the response head.
    Head {
        future: wasip2::http::types::FutureIncomingResponse,
        /// Why writing the request body failed, if it did.
        sending: Option<String>,
        tries: u32,
    },
    /// Reading the body.
    Body {
        stream: wasip2::io::streams::InputStream,
        incoming: wasip2::http::types::IncomingBody,
        status: u16,
        headers: crate::headers::Headers,
        body: Vec<u8>,
        response: wasip2::http::types::IncomingResponse,
        future: wasip2::http::types::FutureIncomingResponse,
    },
    /// The body ended: waiting for the trailers, which may report an error.
    Trailers {
        trailers: wasip2::http::types::FutureTrailers,
        status: u16,
        headers: crate::headers::Headers,
        body: Vec<u8>,
        // Held only to drop after the trailers.
        #[allow(dead_code)]
        response: wasip2::http::types::IncomingResponse,
        #[allow(dead_code)]
        future: wasip2::http::types::FutureIncomingResponse,
    },
    Done(Result<Fetched, String>),
}

impl State {
    /// Holding one of the host's places for open requests.
    fn is_open(&self) -> bool {
        matches!(
            self,
            Self::Head { .. } | Self::Body { .. } | Self::Trailers { .. }
        )
    }

    /// What it waits for.
    fn pollable(&self) -> Option<wasip2::io::poll::Pollable> {
        match self {
            Self::Head { future, .. } => Some(future.subscribe()),
            Self::Body { stream, .. } => Some(stream.subscribe()),
            Self::Trailers { trailers, .. } => Some(trailers.subscribe()),
            Self::Queued(_) | Self::Refused(_) | Self::Done(_) => None,
        }
    }
}

/// Builds the request and hands it to the host: its response's future, and
/// the body to write.
fn start(
    request: &FetchRequest<'_>,
    timeout: std::time::Duration,
) -> Result<
    (
        wasip2::http::types::FutureIncomingResponse,
        wasip2::http::types::OutgoingBody,
    ),
    String,
> {
    use wasip2::http::outgoing_handler;
    use wasip2::http::types::{OutgoingRequest, RequestOptions, Scheme};

    let FetchRequest {
        method,
        url,
        headers,
        body,
    } = *request;
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
    Ok((future, outgoing))
}

/// How writing a request body went.
enum Sent {
    Yes,
    /// The body's stream failed: the response, when one comes, says more.
    Body(String),
    /// Written, but not finished: the request fails.
    Unfinished(String),
}

/// Writes the request body and finishes it.
fn send_body(outgoing: OutgoingBody, body: &[u8]) -> Sent {
    let mut sent = Ok(());
    if !body.is_empty() {
        match outgoing.write() {
            Ok(mut stream) => {
                sent = stream
                    .write_all(body)
                    .map_err(|e| e.to_string())
                    .and_then(|()| stream.flush().map_err(|e| e.to_string()));
            }
            Err(()) => return Sent::Unfinished("request body unavailable".to_owned()),
        }
    }
    match sent {
        Ok(()) => match OutgoingBody::finish(outgoing, None) {
            Ok(()) => Sent::Yes,
            Err(e) => Sent::Unfinished(format!("{e:?}")),
        },
        // The body's stream closes when the request ends before the guest
        // is done with it: either it failed (refused, no connection), and
        // the response says why where it can, or the server already
        // answered and closed (it had the whole body, or answered early),
        // and that answer is the result.
        Err(e) => {
            drop(outgoing);
            Sent::Body(e)
        }
    }
}

/// Moves `state` on as far as it goes without waiting. A state is replaced
/// whole, never taken apart and dropped piecemeal, so its resources drop
/// in field order: each before the one it came from.
fn advance(state: &mut State, url: &str, limit: usize) {
    use wasip2::http::types::{ErrorCode, IncomingBody, http_error_code};
    use wasip2::io::streams::StreamError;

    loop {
        match state {
            State::Head {
                future,
                sending,
                tries,
            } => {
                let Some(got) = future.get() else { return };
                let response = match got {
                    Err(()) => {
                        let why = match sending.take() {
                            Some(e) => format!("sending the body: {e}"),
                            None => "response already taken".to_owned(),
                        };
                        *state = State::Done(Err(why));
                        continue;
                    }
                    Ok(Err(ErrorCode::ConnectionLimitReached)) => {
                        *state = State::Refused(*tries);
                        continue;
                    }
                    Ok(Err(code)) => {
                        *state = State::Done(Err(format!("{code:?}")));
                        continue;
                    }
                    Ok(Ok(response)) => response,
                };
                let status = response.status();
                let headers = {
                    let fields = response.headers();
                    crate::headers::Headers::from_byte_pairs(fields.entries())
                };
                let Ok(incoming) = response.consume() else {
                    drop(response);
                    *state = State::Done(Err(format!("{url}: response body already taken")));
                    continue;
                };
                let Ok(stream) = incoming.stream() else {
                    drop(incoming);
                    drop(response);
                    *state = State::Done(Err(format!(
                        "{url}: incomplete body: its stream was already taken"
                    )));
                    continue;
                };
                let State::Head { future, .. } =
                    std::mem::replace(state, State::Done(Err(String::new())))
                else {
                    unreachable!("matched above")
                };
                *state = State::Body {
                    stream,
                    incoming,
                    status,
                    headers,
                    body: Vec::new(),
                    response,
                    future,
                };
            }
            State::Body { stream, body, .. } => {
                let mut ended = false;
                while body.len() < limit {
                    let want = (limit - body.len()).min(65_536) as u64;
                    match wasip2::io::streams::InputStream::read(stream, want) {
                        Ok(chunk) if chunk.is_empty() => return,
                        Ok(chunk) => body.extend_from_slice(&chunk),
                        Err(StreamError::Closed) => {
                            ended = true;
                            break;
                        }
                        Err(StreamError::LastOperationFailed(e)) => {
                            let why = http_error_code(&e)
                                .map_or_else(|| e.to_debug_string(), |c| format!("{c:?}"));
                            drop(e);
                            *state = State::Done(Err(format!("{url}: incomplete body: {why}")));
                            break;
                        }
                    }
                }
                if let State::Body {
                    status,
                    headers,
                    body,
                    ..
                } = state
                    && !ended
                {
                    // At the limit: the rest is dropped unread.
                    let fetched = Fetched {
                        status: *status,
                        headers: std::mem::take(headers),
                        body: std::mem::take(body),
                    };
                    *state = State::Done(Ok(fetched));
                } else if ended {
                    // Closed: the body's own end, unless the trailers say
                    // otherwise.
                    let State::Body {
                        stream,
                        incoming,
                        status,
                        headers,
                        body,
                        response,
                        future,
                    } = std::mem::replace(state, State::Done(Err(String::new())))
                    else {
                        unreachable!("matched above")
                    };
                    drop(stream);
                    *state = State::Trailers {
                        trailers: IncomingBody::finish(incoming),
                        status,
                        headers,
                        body,
                        response,
                        future,
                    };
                }
            }
            State::Trailers {
                trailers,
                status,
                headers,
                body,
                ..
            } => {
                let Some(got) = trailers.get() else { return };
                let result = match &got {
                    Ok(Ok(_)) => Ok(Fetched {
                        status: *status,
                        headers: std::mem::take(headers),
                        body: std::mem::take(body),
                    }),
                    Ok(Err(code)) => Err(format!("{url}: incomplete body: {code:?}")),
                    Err(()) => Err(format!(
                        "{url}: incomplete body: its trailers were already taken"
                    )),
                };
                // Trailers received are the future's child: dropped first.
                drop(got);
                *state = State::Done(result);
            }
            State::Queued(_) | State::Refused(_) | State::Done(_) => return,
        }
    }
}
