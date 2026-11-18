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
        let secret = format!("wasmkeep-ingress-identity-v2:{secret}");
        let ts = now_ms();
        let charged = u8::from(req.extensions().get::<IngressCharged>().is_some());
        let mut nonce = format!("{charged}{:032x}", u128::from(ip));
        if charged != 0 {
            // Fit the verifier's 64-hex-digit bound while distinguishing even
            // identical hand-offs signed in the same millisecond. Hash the
            // process seed and send counter to retain 124 bits of uniqueness.
            nonce.push_str(&body_sha256(self.nonce().as_bytes())[..31]);
        }
        let body = identity_headers(req.headers());
        let mac = signature(
            &secret,
            ts,
            &nonce,
            owner,
            req.method(),
            path,
            &body,
            req.headers(),
        );
        req.headers_mut().insert(
            ADMITTED,
            hyper::header::HeaderValue::from_str(&format!("v4 {ts} {nonce} {body} {mac}"))
                .map_err(|e| e.to_string())?,
        );
        let cluster_secret = self.secret.get().map(String::as_str).unwrap_or_default();
        self.add_sender(req.headers_mut(), cluster_secret, "admitted", &mac);
        Ok(())
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
            "admitted",
            given,
            path,
            on_cluster_port,
        ) {
            Ok(sender) => Some((Signed { sender, ..signed }, given.to_owned())),
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
            32 => (encoded, false), // Previous uncharged identity format.
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
