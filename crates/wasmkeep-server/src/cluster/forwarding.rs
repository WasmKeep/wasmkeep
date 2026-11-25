//! Requests relayed to another node: addresses in headers, the headers
//! that are dropped, and what is never forwarded.

use super::*;

/// `tenant/class/name`, the name percent-encoded so any name fits a header.
pub(crate) fn encode_address(a: &ActorAddress) -> String {
    format!("{}/{}/{}", a.tenant(), a.class(), percent_encode(a.name()))
}

/// The inverse of [`encode_address`].
pub(crate) fn decode_address(s: &str) -> Option<ActorAddress> {
    let mut parts = s.splitn(3, '/');
    let (tenant, class, name) = (parts.next()?, parts.next()?, parts.next()?);
    ActorAddress::new(tenant, class, crate::http::percent_decode(name)?).ok()
}

/// Percent-encodes everything but unreserved URI characters.
pub(crate) fn percent_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_' | b'~') {
            out.push(char::from(b));
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

/// The header on a host's answer for a request that may have run although
/// its answer is not the actor's: `unknown`. The request was handed to the
/// owner or to the guest, and its answer was lost or withheld (a hop that
/// failed after the request was written, an answer from a node no longer
/// trusted, a peer whose pin lapsed while its request ran, a timeout after
/// the request was sent). A durable write in it may have landed: such an
/// answer has no `Retry-After`, and a client reads the outcome before it
/// repeats a write that is not idempotent. A host refusal made before
/// anything ran (`Retry-After: 1`) never carries it.
pub(crate) const OUTCOME: &str = "x-wasmkeep-outcome";

/// `response`, marked as an answer whose request may have run
/// ([`OUTCOME`]), with any `Retry-After` removed.
pub(crate) fn mark_unknown(
    mut response: Response<HyperOutgoingBody>,
) -> Response<HyperOutgoingBody> {
    response.headers_mut().remove(hyper::header::RETRY_AFTER);
    response.headers_mut().insert(
        hyper::header::HeaderName::from_static(OUTCOME),
        hyper::header::HeaderValue::from_static("unknown"),
    );
    response
}

/// 503 for a request that may have run ([`OUTCOME`]): `why`, then that the
/// request may have run.
pub(crate) fn outcome_unknown(why: &str) -> Response<HyperOutgoingBody> {
    mark_unknown(text_response(
        StatusCode::SERVICE_UNAVAILABLE,
        &format!("{why}; the request may have run\n"),
    ))
}

/// On a host's `503` for an event another node sent: the node that answers
/// reads the actor's record as naming another owner, the value (it read
/// the record again first, so not from a stale hint). It neither ran the
/// event nor sent it on a second time. Its sender takes it as "not run"
/// and does not suspect it: nothing is wrong with that node, ownership is
/// changing hands (ADR 0040, amendment of 2026-11-22).
pub(crate) const OWNER_SEEN: &str = "x-wasmkeep-owner-seen";

/// `503` "ownership is changing" for an event another node sent, marked
/// with the owner this node read ([`OWNER_SEEN`]).
pub(crate) fn owner_changing(owner: &str) -> Response<HyperOutgoingBody> {
    let mut r = unavailable("actor ownership is changing; retry\n");
    if let Ok(v) = hyper::header::HeaderValue::from_str(owner) {
        r.headers_mut()
            .insert(hyper::header::HeaderName::from_static(OWNER_SEEN), v);
    }
    r
}

pub(crate) fn unavailable(text: &str) -> Response<HyperOutgoingBody> {
    let mut r = text_response(StatusCode::SERVICE_UNAVAILABLE, text);
    r.headers_mut().insert(
        hyper::header::RETRY_AFTER,
        hyper::header::HeaderValue::from_static("1"),
    );
    r
}

/// Headers dropped from every request relayed or sent to another node:
/// framing and connection headers (the client sets them for its own hop),
/// as `actors.call` drops them from a guest's request.
pub(crate) const HOP_BY_HOP: &[&str] = &[
    "connection",
    "content-length",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "proxy-connection",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
];

/// The headers in [`HOP_BY_HOP`] that frame the body. `trailer` names the
/// trailer fields a chunked body carries; hyper's client sends only those.
pub(super) const FRAMING: &[&str] = &["content-length", "transfer-encoding", "trailer"];

/// Whether [`strip_hop_by_hop`] keeps the body's framing headers.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Framing {
    /// The body is passed on as it came, so its framing still describes it.
    Keep,
    /// The body is framed again for the next hop.
    Drop,
}

/// Removes the headers meant for one connection (RFC 9110 section 7.6.1):
/// [`HOP_BY_HOP`] and every header the `Connection` header names. With
/// [`Framing::Keep`], the [`FRAMING`] headers stay, even when `Connection`
/// names them.
pub(super) fn strip_hop_by_hop(headers: &mut hyper::HeaderMap, framing: Framing) {
    headers.remove(ADMITTED);
    let named: Vec<hyper::header::HeaderName> = headers
        .get_all(hyper::header::CONNECTION)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .filter_map(|token| hyper::header::HeaderName::from_bytes(token.trim().as_bytes()).ok())
        .collect();
    let listed = HOP_BY_HOP
        .iter()
        .filter_map(|h| hyper::header::HeaderName::from_bytes(h.as_bytes()).ok());
    for name in listed.chain(named) {
        if framing == Framing::Keep && FRAMING.contains(&name.as_str()) {
            continue;
        }
        headers.remove(&name);
    }
}

/// The headers of a WebSocket upgrade relayed to the actor's owner: the
/// client's, without its host, hop-by-hop headers, or any `x-wasmkeep-*`
/// header (the signature, caller, chain, forwarded mark and the markers
/// of node calls are set by nodes and the host only, and a name added to
/// that range later is covered too), then the upgrade itself. The relay
/// is an outside client's request.
pub(super) fn upgrade_headers(client: &hyper::HeaderMap) -> hyper::HeaderMap {
    let mut out = client.clone();
    strip_hop_by_hop(&mut out, Framing::Drop);
    out.remove(hyper::header::HOST);
    let reserved: Vec<hyper::header::HeaderName> = out
        .keys()
        .filter(|name| name.as_str().starts_with(RESERVED_PREFIX))
        .cloned()
        .collect();
    for name in reserved {
        out.remove(name);
    }
    out.insert(
        hyper::header::CONNECTION,
        hyper::header::HeaderValue::from_static("upgrade"),
    );
    out.insert(
        hyper::header::UPGRADE,
        hyper::header::HeaderValue::from_static("websocket"),
    );
    out
}

/// The range of header names nodes and the host set (header names are
/// lowercase in a `HeaderMap`).
const RESERVED_PREFIX: &str = "x-wasmkeep-";

impl Cluster {
    /// [`APP`] for the hops this node sends.
    pub(super) fn app_header(&self) -> Result<hyper::header::HeaderValue, String> {
        hyper::header::HeaderValue::from_str(&self.serves)
            .map_err(|_| format!("app {:?} cannot be sent in a header", self.serves))
    }

    /// The answer to a hop (a request another node sent: `signed`,
    /// anything on the cluster port, or a forward, which on the public
    /// port in refuse mode is a client's claim) that is not for this
    /// node's app, before anything of it is read (ADR 0022): 503 "not this
    /// app's node" when it names another app, and a refusal naming the
    /// cause when it names none (a node of an older version: upgrades stop
    /// every node). `None` for a hop naming this app, and for a request
    /// from outside, whose [`APP`] means nothing (the caller removes it).
    pub(crate) fn wrong_app<B>(
        &self,
        req: &Request<B>,
        signed: bool,
    ) -> Option<Response<HyperOutgoingBody>> {
        let on_cluster_port = req.extensions().get::<ArrivedOnClusterPort>().is_some();
        let forwarded = req.headers().contains_key(FORWARDED) && !self.refuse_plaintext;
        if !signed && !on_cluster_port && !forwarded {
            return None;
        }
        let mut named = req.headers().get_all(APP).iter();
        let (first, second) = (named.next(), named.next());
        if !matches!((first, second), (Some(app), None) if app.as_bytes() == self.serves.as_bytes())
        {
            crate::metrics::refused(crate::metrics::Refusal::WrongApp);
        }
        match (first, second) {
            (Some(app), None) if app.as_bytes() == self.serves.as_bytes() => None,
            (None, _) => {
                // On the public port, a forwarded mark alone may as well
                // be a client's (or a proxy's) as an older node's.
                self.note_refused(if signed || on_cluster_port {
                    "no x-wasmkeep-app: a node of an older version (upgrades stop every node)"
                } else {
                    "forwarded mark without x-wasmkeep-app on the public port: a node of an older version, or a client"
                });
                // Only an operator can fix it: no Retry-After.
                Some(text_response(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "refused: sent by a node of an older version (no x-wasmkeep-app); \
                     stop every node and upgrade them together\n",
                ))
            }
            (Some(first), second) => {
                let why = "a hop for another app";
                if self.refusal_log_due_now(why) {
                    // The sender's word: shown escaped and cut short.
                    let named: String = String::from_utf8_lossy(first.as_bytes())
                        .chars()
                        .take(64)
                        .collect();
                    tracing::warn!(
                        reason = why,
                        serves = %self.serves,
                        named = ?named,
                        several = second.is_some(),
                        "a call claiming to come from a node was refused"
                    );
                }
                Some(unavailable("not this app's node\n"))
            }
        }
    }
}

/// Whether a request must not be forwarded to another node: another node
/// already forwarded it, or it arrived on the cluster port (ADR 0011).
pub(crate) fn never_forward<B>(req: &Request<B>) -> bool {
    req.headers().contains_key(FORWARDED)
        || req.extensions().get::<ArrivedOnClusterPort>().is_some()
}

// Domain-separated client identity proof; uncharged replay consumes owner quota.
#[derive(Clone, Copy, Debug)]
pub(crate) struct IngressCharged;

pub(crate) const ADMITTED: &str = "x-wasmkeep-admitted";

fn identity_headers(headers: &hyper::HeaderMap) -> String {
    let mut values: Vec<_> = headers
        .iter()
        .filter(|(name, _)| {
            !HOP_BY_HOP.contains(&name.as_str())
                && name.as_str() != "host"
                && name.as_str() != "expect"
                && !name.as_str().starts_with(RESERVED_PREFIX)
        })
        .map(|(name, value)| (name.as_str(), value.as_bytes()))
        .collect();
    values.sort();
    let mut bytes = Vec::new();
    for (name, value) in values {
        bytes.extend_from_slice(name.as_bytes());
        bytes.push(0);
        bytes.extend_from_slice(value);
        bytes.push(0);
    }
    body_sha256(&bytes)
}
impl Cluster {
    pub(super) async fn sign_admission<B>(
        &self,
        owner: &str,
        path: &str,
        req: &mut Request<B>,
    ) -> Result<(), String> {
        // No-policy forwards must never fetch a secret or wait on storage.
        // With several apps a forward always carries its proof (ADR 0038,
        // section 5): without the secret it is refused, not sent unsigned.
        let Some(secret) = self.secret.get() else {
            if self.several_apps() {
                return Err("the cluster secret is not loaded: cannot sign the forward".into());
            }
            return Ok(());
        };
        let Some(peer) = req.extensions().get::<crate::http::PeerAddress>() else {
            return Ok(());
        };
        let ip = match peer.0.ip() {
            std::net::IpAddr::V4(ip) => ip.to_ipv6_mapped(),
            std::net::IpAddr::V6(ip) if ip.to_ipv4_mapped().is_some() => ip,
            std::net::IpAddr::V6(ip) => {
                std::net::Ipv6Addr::from(u128::from(ip) & (u128::MAX << 64))
            }
        };
        let key = format!("wasmkeep-ingress-identity-v2:{secret}");
        let charged = u8::from(req.extensions().get::<IngressCharged>().is_some());
        let mut nonce = format!("{charged}{:032x}", u128::from(ip));
        if charged != 0 {
            // Fit the verifier's 64-hex-digit bound while distinguishing even
            // identical hand-offs signed in the same millisecond. Hash the
            // process seed and send counter to retain 124 bits of uniqueness.
            nonce.push_str(&body_sha256(self.nonce().as_bytes())[..31]);
        }
        let body = identity_headers(req.headers());
        let method = req.method().clone();
        // The proof's MAC is under the derived key; its sender's, as every
        // sender's, under the cluster secret itself.
        let v4 = V4 {
            key: &key,
            nonce: &nonce,
            to: owner,
            method: &method,
            path,
            body: &body,
        };
        self.sign_hop(req.headers_mut(), HopKind::Admitted, secret, &v4)
    }

    /// A forward's proof ([`ADMITTED`]) that checks: its signature over
    /// the request (the placed mark included), the client's identity
    /// headers, and its sender. Spends nothing: the caller spends a
    /// charged proof. Its [`Signed::sender`] is the checked sender.
    pub(crate) fn admission_checked<B>(&self, req: &Request<B>) -> Option<(Signed, String)> {
        if self.refuse_plaintext && req.extensions().get::<ArrivedOnClusterPort>().is_none() {
            return None;
        }
        let mut values = req.headers().get_all(ADMITTED).iter();
        let given = values.next()?.to_str().ok()?;
        if values.next().is_some() {
            return None;
        }
        let secret = format!("wasmkeep-ingress-identity-v2:{}", self.secret.get()?);
        // As signed: the path the request arrived with, before routing
        // cut `/actor/<class>/<name>` off it.
        let path = req.extensions().get::<OriginalPath>().map_or_else(
            || req.uri().path_and_query().map_or("/", |p| p.as_str()),
            |p| p.0.as_str(),
        );
        let signed = verify(
            &secret,
            given,
            now_ms(),
            &self.node_id,
            req.method(),
            path,
            req.headers(),
        )?;
        if signed.body != identity_headers(req.headers()) {
            return None;
        }
        let on_cluster_port = req.extensions().get::<ArrivedOnClusterPort>().is_some();
        match self.sender_checked(
            req.headers(),
            self.secret.get()?,
            HopKind::Admitted,
            given,
            path,
            on_cluster_port,
        ) {
            Ok(sender) => Some((
                Signed {
                    sender: Some(sender),
                    ..signed
                },
                given.to_owned(),
            )),
            Err(why) => {
                self.note_refused(why);
                None
            }
        }
    }

    pub(crate) fn admission_received<B>(
        &self,
        req: &Request<B>,
    ) -> Option<(std::net::IpAddr, bool)> {
        let (signed, given) = self.admission_checked(req)?;
        let given = given.as_str();
        let encoded = given.split(' ').nth(2)?;
        let (encoded, charged) = match encoded.len() {
            33 if encoded.starts_with('0') || encoded.starts_with('1') => {
                (&encoded[1..], encoded.starts_with('1'))
            }
            64 if encoded.starts_with('1') => (&encoded[1..33], true),
            _ => return None,
        };
        let ip = std::net::Ipv6Addr::from(u128::from_str_radix(encoded, 16).ok()?);
        // Spend only authenticated, well-formed charged proofs, atomically.
        // None uses the forwarded-request 503 path; never fall back to an
        // unchecked exemption when this separate bounded record is full.
        if charged && !self.seen_ingress.first_use(signed.mac, signed.ts, now_ms()) {
            return None;
        }
        Some((
            ip.to_ipv4_mapped()
                .map_or(std::net::IpAddr::V6(ip), std::net::IpAddr::V4),
            charged,
        ))
    }
}

/// A body a forward sends ([`Cluster::forward`]), each on the peer
/// client built for it.
pub(crate) trait ForwardBody:
    hyper::body::Body<Data = bytes::Bytes, Error: Into<Box<dyn std::error::Error + Send + Sync>>>
    + Send
    + Unpin
    + 'static
    + Sized
{
    /// The client that sends it.
    fn client(clients: &Clients) -> Client<ClusterConnector, Self>;
}

/// A client's request, its body streaming through.
impl ForwardBody for Incoming {
    fn client(clients: &Clients) -> Client<ClusterConnector, Self> {
        clients.request.clone()
    }
}

/// A request whose body was read into memory.
impl ForwardBody for http_body_util::Full<bytes::Bytes> {
    fn client(clients: &Clients) -> Client<ClusterConnector, Self> {
        clients.call.clone()
    }
}

impl Cluster {
    /// A response from `base` (over TLS): `None` if the peer that answered
    /// is no longer trusted there (its pin lapsed while it worked on the
    /// request), else the response, its body cut if that happens while it
    /// streams.
    pub(super) fn trusted_response(
        &self,
        base: &str,
        node: &str,
        response: Response<Incoming>,
    ) -> Option<Response<HyperOutgoingBody>> {
        let pin = response
            .extensions()
            .get::<crate::cluster_tls::PeerPin>()
            .map(|p| p.0);
        let still = self.revocation(base, node).map(|r| r(pin));
        if still.as_ref().is_some_and(|s| !s()) {
            return None;
        }
        Some(response.map(|b| {
            let body = b.map_err(Into::into).boxed_unsync();
            match still {
                Some(s) => crate::cluster_tls::revocable_body(body, s),
                None => body,
            }
        }))
    }

    /// What every forward does before it is sent (to `owner`): never a
    /// second hop, the owner's address and the original path, the
    /// connection headers dropped, the forwarded mark, the app, the trace
    /// context (ADR 0044, section 3) and then the proof (ADR 0038),
    /// counted, and the target checked. The base it goes to and the
    /// forward's `hop` span (`kind`: what it is called there), or the
    /// answer that sends nothing.
    pub(crate) async fn prepare_forward<B>(
        &self,
        owner: &str,
        kind: &'static str,
        req: &mut Request<B>,
    ) -> Result<(String, crate::traces::Span), Box<Response<HyperOutgoingBody>>> {
        if never_forward(req) {
            // Two nodes disagree about who owns the actor (a lease is
            // changing hands). Do not bounce; let the client retry.
            return Err(Box::new(unavailable(
                "actor ownership is changing; retry\n",
            )));
        }
        let Some(base) = self.address_of(owner) else {
            return Err(Box::new(unavailable(
                "the actor's node is not reachable; retry\n",
            )));
        };
        // Routing cut `/actor/<class>/<name>` off the path; send the original.
        let path = match req.extensions().get::<OriginalPath>() {
            Some(original) => original.0.clone(),
            None => return Err(Box::new(unavailable("cannot forward this request\n"))),
        };
        let Ok(uri) = format!("{}{path}", base.trim_end_matches('/')).parse::<hyper::Uri>() else {
            return Err(Box::new(unavailable("bad advertised address\n")));
        };
        *req.uri_mut() = uri;
        // The client's connection headers are for its hop to this node: a
        // `Connection: close` passed on would close the pooled connection to
        // the owner (over TLS, a handshake per request). The body streams
        // through unchanged, so its framing headers stay. HTTP/1.0 would
        // close the connection after each response; the hop to the owner
        // is HTTP/1.1 whatever the client speaks (a 1.0 body is never
        // chunked, so its framing still holds).
        strip_hop_by_hop(req.headers_mut(), Framing::Keep);
        *req.version_mut() = hyper::Version::HTTP_11;
        // This node answers a client's `Expect: 100-continue` itself, as
        // soon as the body starts streaming to the owner, which is at
        // once. Passed on, it would tell the owner that the sender waits
        // before sending, and to leave an unread body unread (see
        // `CappedBody`).
        req.headers_mut().remove(hyper::header::EXPECT);
        req.headers_mut()
            .insert(FORWARDED, hyper::header::HeaderValue::from_static("1"));
        // This node's app, never the client's (ADR 0022).
        match self.app_header() {
            Ok(app) => req.headers_mut().insert(APP, app),
            Err(_) => return Err(Box::new(unavailable("cannot forward this request\n"))),
        };
        // This node's `hop` span as the forward's `traceparent`, replacing
        // the client's or added if the head has room (ADR 0044, section
        // 3): set here, on the request as it is sent, because the proof
        // below hashes every header of the client's, this one included.
        // Changed after, the proof would fail at the owner.
        let hop = crate::traces::hop(kind, &self.node_id, owner, req);
        if self.sign_admission(owner, &path, req).await.is_err() {
            return Err(Box::new(unavailable(
                "cannot authenticate ingress admission; retry\n",
            )));
        }
        crate::metrics::inc(&crate::metrics::METRICS.forwards.get(&[("kind", "request")]));
        // Bound the wait for the owner's response headers (a hung node or a
        // half-open connection); the body then streams as the owner sends it.
        if let Err(e) = self.check_tls_target(&base) {
            tracing::warn!(owner, error = %e, "forwarding refused");
            return Err(Box::new(unavailable(
                "the actor's node is not reachable; retry\n",
            )));
        }
        Ok((base, hop))
    }

    /// Forwards a request for an actor to the node that owns it: a
    /// client's, its body streaming through ([`Incoming`]), or one whose
    /// body was read into memory (a placed request that ran here after
    /// all, ADR 0041, section 6, whose actor another node took meanwhile).
    pub(crate) async fn forward<B: ForwardBody>(
        &self,
        owner: &str,
        mut req: Request<B>,
    ) -> Response<HyperOutgoingBody> {
        let (base, mut hop) = match self.prepare_forward(owner, "forward", &mut req).await {
            Ok(prepared) => prepared,
            Err(response) => return *response,
        };
        let client = B::client(&self.clients());
        match tokio::time::timeout(FORWARD_TIMEOUT, client.request(req)).await {
            // The owner answered, so it read the request: it may have run.
            Ok(Ok(response)) => {
                // The hop ends at the owner's answer; its body streams on.
                hop.answered(Some(response.status()));
                drop(hop);
                self.trusted_response(&base, owner, response)
                    .unwrap_or_else(|| {
                        tracing::warn!(owner, "forwarding: the node is no longer trusted");
                        outcome_unknown("the actor's node is no longer trusted")
                    })
            }
            // Every failure before an answer, none sent again from here:
            // a connection that could not be opened, a request the client
            // handed back unsent on a new connection, and a request
            // written into a connection that then failed, which the owner
            // may have read. (hyper-util itself sends an unsent request
            // again only on a reused pooled connection, and drops one it
            // finds closed when it takes it from the pool.) The body
            // streams from the client, so the first two cannot be sent
            // again here either. Of the three users of the peer clients,
            // `actors.call` (`Cluster::call`) and a WebSocket upgrade
            // (`Cluster::forward_upgrade`, which has no body) send again,
            // once, a request the owner provably never read
            // (`never_read_by_peer`: refused at the TLS handshake, or
            // handed back unsent on a new TLS connection), and nothing
            // else; this path answers 503 for all of them, saying whether
            // the owner may have run it ([`failed_forward`]).
            Ok(Err(e)) => {
                // No connection could be opened: requests that arrive
                // later for this node's actors may be kept (`keep`). This
                // one is answered as ever.
                self.failover.connect_failed(owner, &e);
                tracing::warn!(owner, error = %e, "forwarding failed");
                failed_forward(&base, &e)
            }
            Err(_) => {
                tracing::warn!(owner, "forwarding timed out");
                mark_unknown(text_response(
                    StatusCode::GATEWAY_TIMEOUT,
                    "the actor's node did not answer in time\n",
                ))
            }
        }
    }
}

/// The answer for a forward to `base` that failed before an answer: the
/// plain retryable 503 when the owner provably never got the request (no
/// connection, or [`never_read_by_peer`]), else 503 with the outcome
/// unknown ([`OUTCOME`]): it was written to the owner, which may have run
/// it and lost the connection.
fn failed_forward(
    base: &str,
    error: &hyper_util::client::legacy::Error,
) -> Response<HyperOutgoingBody> {
    if error.is_connect() || never_read_by_peer(base, error) {
        unavailable("the actor's node did not answer; retry\n")
    } else {
        outcome_unknown("the actor's node did not answer")
    }
}

/// Whether a request to the peer at `base` failed in a way that proves the
/// peer never read it, so that sending it again cannot run it twice. Two
/// cases:
///
/// - The peer refused the TLS handshake
///   ([`crate::cluster_tls::handshake_refused`]: one of a fixed list of
///   fatal alerts arrived). The request may have been written, TLS 1.3
///   letting this side write before the refusal arrives, but the peer did
///   not read it: a rustls server sends those alerts only while it handles
///   the ClientHello, the client's certificate or its CertificateVerify,
///   states in which it passes no application data on. (It reads
///   application data before its handshake is done only as early data,
///   which the cluster listener leaves off, with resumption.) Once its
///   handshake is done, it sends none of them. Checked in the source of
///   rustls 0.23.46, the server's side of TLS 1.3, the only version
///   between nodes; to check again when that dependency changes.
/// - Over TLS, the HTTP client handed the request back before writing any
///   of it ([`crate::cluster_tls::request_unsent`]) on a new connection
///   (a request it gets back on a connection reused from the pool, it
///   sends on another connection itself). A refusal that closes the
///   connection before the write looks like this; so does any other close
///   of a new connection before the write, which is as safe to send
///   again.
pub(super) fn never_read_by_peer(base: &str, error: &hyper_util::client::legacy::Error) -> bool {
    crate::cluster_tls::handshake_refused(error)
        || (base.starts_with("https://") && crate::cluster_tls::request_unsent(error))
}

#[cfg(test)]
mod admission_tests {
    use super::*;
    #[test]
    fn review_forward_headers_strip_client_receipt() {
        let mut h = hyper::HeaderMap::new();
        h.insert(ADMITTED, "forged".parse().unwrap());
        strip_hop_by_hop(&mut h, Framing::Keep);
        assert!(!h.contains_key(ADMITTED));
    }
    #[tokio::test]
    async fn fix4_charged_proof_is_single_use_even_with_another_body() {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let config = ClusterConfig::new("owner", "http://owner", store);
        let cluster = Cluster::new(&config, &StateRoot::new("", "t", "default")).unwrap();
        cluster.load_secret().await;
        let mut req = Request::post("/actor/c/a?q=1").body("first").unwrap();
        req.extensions_mut()
            .insert(crate::http::PeerAddress("192.0.2.1:1234".parse().unwrap()));
        req.extensions_mut().insert(IngressCharged);
        cluster
            .sign_admission("owner", "/actor/c/a?q=1", &mut req)
            .await
            .unwrap();
        assert!(cluster.admission_received(&req).is_some());
        *req.body_mut() = "other";
        assert!(
            cluster.admission_received(&req).is_none(),
            "charged replay accepted with another body"
        );
        assert_eq!(cluster.seen.len(), 0);
        assert_eq!(cluster.seen_control.len(), 0);
    }

    #[tokio::test]
    async fn fix4_charged_proof_capacity_fails_closed_without_blocking_uncharged() {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let config = ClusterConfig::new("owner", "http://owner", store);
        let cluster = Cluster::new(&config, &StateRoot::new("", "t", "default")).unwrap();
        cluster.load_secret().await;
        for i in 0..=4096 {
            let path = format!("/actor/c/a?q={i}");
            let mut req = Request::get(&path).body(()).unwrap();
            req.extensions_mut()
                .insert(crate::http::PeerAddress("192.0.2.1:1234".parse().unwrap()));
            req.extensions_mut().insert(IngressCharged);
            cluster
                .sign_admission("owner", &path, &mut req)
                .await
                .unwrap();
            assert_eq!(
                cluster.admission_received(&req).is_some(),
                i < 4096,
                "entry {i}"
            );
            if i == 4096 {
                req.extensions_mut().remove::<IngressCharged>();
                cluster
                    .sign_admission("owner", &path, &mut req)
                    .await
                    .unwrap();
                assert!(cluster.admission_received(&req).is_some());
                assert!(cluster.admission_received(&req).is_some());
            }
        }
        assert_eq!(cluster.seen.len(), 0);
        assert_eq!(cluster.seen_control.len(), 0);
    }

    #[tokio::test]
    async fn fix4_distinct_charged_sends_and_atomic_replay() {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let config = ClusterConfig::new("owner", "http://owner", store);
        let cluster = Cluster::new(&config, &StateRoot::new("", "t", "default")).unwrap();
        cluster.load_secret().await;
        let mut req = Request::get("/actor/c/a").body(()).unwrap();
        req.extensions_mut()
            .insert(crate::http::PeerAddress("192.0.2.1:1234".parse().unwrap()));
        req.extensions_mut().insert(IngressCharged);
        let mut nonces = std::collections::HashSet::new();
        for _ in 0..32 {
            cluster
                .sign_admission("owner", "/actor/c/a", &mut req)
                .await
                .unwrap();
            let nonce = req.headers()[ADMITTED]
                .to_str()
                .unwrap()
                .split(' ')
                .nth(2)
                .unwrap();
            assert_eq!(nonce.len(), 64);
            assert!(
                nonces.insert(nonce.to_owned()),
                "distinct sends reused a nonce"
            );
            let winners = std::thread::scope(|scope| {
                let handles: Vec<_> = (0..8)
                    .map(|_| {
                        scope.spawn(|| usize::from(cluster.admission_received(&req).is_some()))
                    })
                    .collect();
                handles
                    .into_iter()
                    .map(|h| h.join().unwrap())
                    .sum::<usize>()
            });
            assert_eq!(winners, 1);
        }
        assert_eq!(cluster.seen_ingress.len(), 32);
    }

    #[tokio::test]
    async fn fix3_charged_identity_is_signed_and_tamper_evident() {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let config = ClusterConfig::new("owner", "http://owner", store);
        let cluster = Cluster::new(&config, &StateRoot::new("", "t", "default")).unwrap();
        cluster.load_secret().await;
        let mut req = Request::get("/actor/c/a").body(()).unwrap();
        req.extensions_mut()
            .insert(crate::http::PeerAddress("192.0.2.1:1234".parse().unwrap()));
        req.extensions_mut().insert(IngressCharged);
        cluster
            .sign_admission("owner", "/actor/c/a", &mut req)
            .await
            .unwrap();
        assert_eq!(
            cluster.admission_received(&req),
            Some(("192.0.2.1".parse().unwrap(), true))
        );
        assert!(
            cluster
                .is_internal(req.method(), "/actor/c/a", req.headers(), false)
                .is_none()
        );
        let proof = req.headers()[ADMITTED].to_str().unwrap();
        let mut fields: Vec<_> = proof.split(' ').map(str::to_owned).collect();
        fields[2].replace_range(..1, "0");
        req.headers_mut()
            .insert(ADMITTED, fields.join(" ").parse().unwrap());
        assert!(cluster.admission_received(&req).is_none());
    }
    #[tokio::test]
    async fn fix2_cold_secret_forward_does_not_fetch_or_sign() {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let config = ClusterConfig::new("owner", "http://owner", store.clone());
        let cluster = Cluster::new(&config, &StateRoot::new("", "t", "default")).unwrap();
        let mut req = Request::get("/actor/c/a").body(()).unwrap();
        req.extensions_mut()
            .insert(crate::http::PeerAddress("192.0.2.1:1234".parse().unwrap()));
        cluster
            .sign_admission("owner", "/actor/c/a", &mut req)
            .await
            .unwrap();
        assert!(cluster.secret.get().is_none());
        assert!(!req.headers().contains_key(ADMITTED));
        assert!(store.get(&"cluster/secret".into()).await.is_err());
    }

    #[tokio::test]
    async fn fix2_proof_binds_identity_owner_method_query_and_headers() {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let config = ClusterConfig::new("owner", "http://owner", store);
        let cluster = Cluster::new(&config, &StateRoot::new("", "t", "default")).unwrap();
        cluster.load_secret().await;
        let mut req = Request::get("/actor/c/a?q=1")
            .header("x-client-id", "alice")
            .body(())
            .unwrap();
        req.extensions_mut().insert(crate::http::PeerAddress(
            "[2001:db8:1:2::abcd]:1234".parse().unwrap(),
        ));
        cluster
            .sign_admission("owner", "/actor/c/a?q=1", &mut req)
            .await
            .unwrap();
        assert_eq!(
            cluster.admission_received(&req),
            Some(("2001:db8:1:2::".parse().unwrap(), false))
        );
        let policies = [(
            "test".into(),
            crate::apps::limits::Policy::parse("/*", "ip", None, "1/h", 1).unwrap(),
        )]
        .into();
        let limiter = crate::rate_limit::Limiter::default();
        let now = std::time::Instant::now();
        limiter.reconcile(&policies, now);
        for expected in [Ok(()), Err(crate::rate_limit::Rejection::Quota(3600))] {
            assert_eq!(
                limiter.admit(
                    &policies,
                    "GET",
                    "/actor/c/a",
                    req.headers(),
                    cluster.admission_received(&req).map(|(ip, _)| ip),
                    now
                ),
                expected
            );
        }
        for change in 0..5 {
            let mut changed = req.clone();
            match change {
                0 => *changed.method_mut() = hyper::Method::POST,
                1 => *changed.uri_mut() = "/actor/c/a?q=2".parse().unwrap(),
                2 => {
                    changed
                        .headers_mut()
                        .insert("x-client-id", "bob".parse().unwrap());
                }
                3 => {
                    cluster
                        .sign_admission("other", "/actor/c/a?q=1", &mut changed)
                        .await
                        .unwrap();
                }
                _ => {
                    let proof = changed.headers()[ADMITTED].to_str().unwrap().replace(
                        "20010db8000100020000000000000000",
                        "20010db8000100030000000000000000",
                    );
                    changed
                        .headers_mut()
                        .insert(ADMITTED, proof.parse().unwrap());
                }
            }
            assert!(
                cluster.admission_received(&changed).is_none(),
                "change {change}"
            );
        }
        assert_eq!(cluster.seen.len(), 0);
    }

    #[tokio::test]
    async fn review_receipts_do_not_fill_call_replay_store() {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let config = ClusterConfig::new("owner", "http://owner", store);
        let cluster = Cluster::new(&config, &StateRoot::new("", "t", "default")).unwrap();
        cluster.load_secret().await;
        let mut req = Request::get("/actor/c/a").body(()).unwrap();
        req.extensions_mut()
            .insert(crate::http::PeerAddress("192.0.2.1:1234".parse().unwrap()));
        cluster
            .sign_admission("owner", "/actor/c/a", &mut req)
            .await
            .unwrap();
        assert!(cluster.admission_received(&req).is_some());
        assert_eq!(cluster.seen.len(), 0);
    }
    #[tokio::test]
    async fn fix2_identity_proofs_are_reusable_and_never_actor_credentials() {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let mut config = ClusterConfig::new("owner", "http://owner", store);
        config.heartbeat = Duration::from_secs(3600);
        config.lease = LeaseConfig::new(10_800_000, 5000, 2000, 1000).unwrap();
        let cluster = Cluster::new(&config, &StateRoot::new("", "t", "default")).unwrap();
        cluster.load_secret().await;
        let mut req = Request::get("/actor/c/a").body(()).unwrap();
        req.extensions_mut()
            .insert(crate::http::PeerAddress("192.0.2.1:1234".parse().unwrap()));
        cluster
            .sign_admission("owner", "/actor/c/a", &mut req)
            .await
            .unwrap();
        assert!(
            cluster
                .is_internal(req.method(), "/actor/c/a", req.headers(), false)
                .is_none()
        );
        let mut changed = req.clone();
        *changed.uri_mut() = "/actor/c/b".parse().unwrap();
        assert!(cluster.admission_received(&changed).is_none());
        assert!(cluster.admission_received(&req).is_some());
        assert!(cluster.admission_received(&req).is_some());
        let proof = req.headers()[ADMITTED].clone();
        req.headers_mut().remove(ADMITTED);
        req.headers_mut().insert(INTERNAL, proof);
        assert!(
            cluster
                .is_internal(req.method(), "/actor/c/a", req.headers(), false)
                .is_none()
        );
    }
}
