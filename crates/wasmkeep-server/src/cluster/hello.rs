//! A node that just started says hello to the peers it found, so they list
//! `nodes/` at once and see it within a period, although they list only
//! every [`wasmkeep_cluster::membership::FULL_LISTING_EVERY`] periods
//! (ADR 0011 and 0020, amendments of 2026-11-14).
//!
//! The hello is a signed node call to [`HELLO_PATH`], with an empty body.
//! It asks for nothing but a listing, which the heartbeat loop makes at
//! once if it made no listing yet that period (one early listing a
//! period at most, the rest at the next period): a node lists at most twice
//! a period, as before over TLS. Unsigned,
//! badly signed or replayed, it is refused and changes nothing. Nodes of
//! 0.4.1 and before answer 404 and list every period anyway. With TLS, the
//! handshake of a node whose pin a peer has not listed yet already makes
//! that peer list (an unknown pin); the hello covers plain HTTP, and a
//! peer that saw the pin knock before its own listing.

use super::*;
use signing::body_sha256;

/// Where a hello is sent, on the cluster port with TLS, else the plain
/// address.
pub(crate) const HELLO_PATH: &str = "/_wasmkeep/hello";

/// How long one hello may take: it is sent once, at start, on a task of
/// its own.
const HELLO_TIMEOUT: Duration = Duration::from_secs(5);

impl Cluster {
    /// Says hello to every live peer of the last refresh, each on a task
    /// of its own; failures are logged at debug level (the peer lists by
    /// its own schedule anyway).
    /// `failed` is set if a peer did not take it (no answer, or an answer
    /// other than 200 and the 404 of a node of 0.4.1): the heartbeat loop
    /// sends another round.
    pub(super) fn say_hello(self: &Arc<Self>, failed: Arc<std::sync::atomic::AtomicBool>) {
        let peers: Vec<String> = self
            .peers
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .keys()
            .filter(|id| **id != self.node_id)
            .cloned()
            .collect();
        for peer in peers {
            let cluster = Arc::downgrade(self);
            let failed = Arc::clone(&failed);
            tokio::spawn(async move {
                let Some(c) = cluster.upgrade() else {
                    return;
                };
                let taken = match tokio::time::timeout(HELLO_TIMEOUT, c.hello(&peer)).await {
                    Ok(Ok(StatusCode::OK)) => {
                        tracing::debug!(%peer, "said hello");
                        true
                    }
                    // A node of 0.4.1: it lists every period anyway.
                    Ok(Ok(StatusCode::NOT_FOUND)) => {
                        tracing::debug!(%peer, "hello not known there (an older node)");
                        true
                    }
                    Ok(Ok(status)) => {
                        tracing::debug!(%peer, %status, "hello refused");
                        false
                    }
                    // Over TLS, a peer that has not listed this node's pin
                    // refuses the handshake, and lists because of it.
                    Ok(Err(e)) => {
                        tracing::debug!(%peer, error = %e, "hello failed");
                        false
                    }
                    Err(_) => {
                        tracing::debug!(%peer, "hello timed out");
                        false
                    }
                };
                if !taken {
                    failed.store(true, std::sync::atomic::Ordering::Release);
                }
            });
        }
    }

    /// One hello to `peer`: the status it answered.
    async fn hello(&self, peer: &str) -> Result<StatusCode, String> {
        let base = self
            .address_of(peer)
            .ok_or_else(|| format!("node {peer} is not reachable"))?;
        let mut req = Request::builder()
            .method(hyper::Method::POST)
            .uri(format!("{}{HELLO_PATH}", base.trim_end_matches('/')))
            .header(hyper::header::HOST, "node.wasmkeep.internal")
            .header(hyper::header::CONNECTION, "close")
            .body(http_body_util::Full::new(bytes::Bytes::new()))
            .map_err(|e| e.to_string())?;
        let secret = self.secret().await?.to_owned();
        let headers = req.headers_mut();
        headers.insert(APP, self.app_header()?);
        let ts = now_ms();
        let nonce = self.nonce();
        let body_hash = body_sha256(&[]);
        let mac = signing::signature(
            &secret,
            ts,
            &nonce,
            peer,
            &hyper::Method::POST,
            HELLO_PATH,
            &body_hash,
            headers,
        );
        headers.insert(
            INTERNAL,
            hyper::header::HeaderValue::from_str(&format!("v4 {ts} {nonce} {body_hash} {mac}"))
                .map_err(|e| e.to_string())?,
        );
        self.add_sender(headers, &secret, "internal", &mac);
        self.check_tls_target(&base)
            .map_err(|e| format!("node {peer}: {e}"))?;
        // A connection of its own, closed after the answer: a hello's must
        // not wait in a pool for the calls, which then would start on a
        // connection opened before a lapse of trust rather than on a new
        // handshake.
        let client: hyper_util::client::legacy::Client<_, http_body_util::Full<bytes::Bytes>> =
            peer_client_builder(PEER_POOL_IDLE)
                .pool_max_idle_per_host(0)
                .build(self.transport.connector.clone());
        let response = client
            .request(req)
            .await
            .map_err(|e| format!("node {peer}: {e}"))?;
        Ok(response.status())
    }

    /// A hello received: a listing at once (at most one early listing a
    /// period) if it is a node's signed call addressed to this node, with
    /// an empty body, never seen before. Answers 200, or 403 for anything
    /// else.
    pub(crate) fn hello_received(
        &self,
        method: &hyper::Method,
        headers: &hyper::HeaderMap,
        body: &[u8],
        on_cluster_port: bool,
    ) -> StatusCode {
        let Some(signed) = self.is_internal(method, HELLO_PATH, headers, on_cluster_port) else {
            return StatusCode::FORBIDDEN;
        };
        if *method != hyper::Method::POST
            || signed.body != body_sha256(body)
            || !self.first_use(&signed, false)
        {
            return StatusCode::FORBIDDEN;
        }
        crate::metrics::inc(&crate::metrics::METRICS.hellos_received);
        // Its heartbeat by key at once (a slow LIST must not delay a
        // join), and a listing as before.
        if let Some(sender) = &signed.sender {
            self.read_soon(sender);
        }
        self.want_listing();
        StatusCode::OK
    }
}
