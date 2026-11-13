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
pub(super) fn percent_encode(s: &str) -> String {
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
        match (named.next(), named.next()) {
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
