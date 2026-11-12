//! How a node connects to other nodes ([ADR 0011](../../../docs/adr/0011-node-to-node-mtls.md)).
//!
//! [`ClusterConnector`] is the connector behind the three clients a node
//! uses to reach its peers (forwarded requests, relayed WebSockets,
//! `actors.call`). `http://` addresses are reached in plaintext. `https://`
//! addresses (a peer's cluster listener) are reached with mutual TLS, and
//! only by a node that has an identity of its own: the peer's certificate
//! must match a pin it published in its heartbeat. Never plaintext to an
//! address published as TLS.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, RwLock};
use std::task::{Context, Poll};
use std::time::Duration;

use hyper::Uri;
use hyper::rt::{Read, ReadBufCursor, Write};
use hyper_util::client::legacy::connect::{Connected, Connection, HttpConnector};
use hyper_util::rt::TokioIo;
use tokio::net::TcpStream;
use wasmtime_wasi_http::p2::body::HyperOutgoingBody;

use pinning::{Identity, Pin32, PinSet};
use wasmkeep_cluster::membership::NodeInfo;

/// Error type of a connection attempt.
pub(crate) type ConnectError = Box<dyn std::error::Error + Send + Sync>;

/// Longest a TCP connect to another node may take.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// Longest a TLS handshake with another node may take, once connected.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);

/// Longest a connection waits for a listing when the claim on its address
/// has lapsed: one default heartbeat period, by which the node lists again
/// (early if it has not listed early this period). A node whose own
/// listing is late would otherwise refuse a live peer.
const LAPSED_WAIT: Duration = Duration::from_secs(3);

/// This node's mutual TLS with its peers (ADR 0011): its identity, made in
/// memory at start, and what it learned from live heartbeats (the pins of
/// every live node, for the cluster listener, and each peer's cluster
/// address with its pins, for connecting). Refreshed with every listing of
/// the heartbeats.
#[derive(Debug)]
pub(crate) struct ClusterTls {
    identity: Identity,
    /// The cluster listener's address as published (`https://host:port`).
    advertise: String,
    /// Every live node's pins, this one's included (the listener's trust).
    pins: Arc<PinSet>,
    /// `host:port` of each live peer's cluster address -> what the node
    /// with the newest heartbeat naming it published.
    by_address: RwLock<HashMap<String, Claim>>,
    /// Counts listings applied by [`ClusterTls::refresh`]: a connection
    /// whose claim lapsed waits for the next one.
    listed: tokio::sync::watch::Sender<u64>,
}

/// A cluster address as the heartbeats name it.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Claim {
    pins: Vec<Pin32>,
    /// When the heartbeat naming it was written (ms, its node's clock).
    written_at_ms: u64,
    /// When that heartbeat stops being live (ms): trusted until then even if
    /// no listing succeeds in between, never after.
    expires_at_ms: u64,
    /// Two live heartbeats name it, written at the same millisecond:
    /// neither is trusted there.
    tied: bool,
}

impl ClusterTls {
    /// A fresh identity for node `node_id`, whose cluster listener is
    /// published as `advertise` (`https://host:port`).
    pub(crate) fn generate(node_id: &str, advertise: &str) -> Result<Self, String> {
        let uri: Uri = advertise
            .parse()
            .map_err(|e| format!("cluster address {advertise:?}: {e}"))?;
        let path_ok = uri.path_and_query().is_none_or(|p| p.as_str() == "/");
        if uri.scheme_str() != Some("https")
            || address_key(&uri).is_none()
            || !path_ok
            || advertise.contains(char::is_whitespace)
        {
            return Err(format!(
                "cluster address {advertise:?}: expected https://HOST:PORT"
            ));
        }
        let host = uri.host().unwrap_or_default();
        let host = host.trim_start_matches('[').trim_end_matches(']');
        Ok(Self {
            identity: Identity::generate(node_id, host)?,
            advertise: advertise.trim_end_matches('/').to_owned(),
            pins: Arc::new(PinSet::default()),
            by_address: RwLock::new(HashMap::new()),
            listed: tokio::sync::watch::Sender::new(0),
        })
    }

    /// The cluster address this node publishes.
    pub(crate) fn advertise(&self) -> &str {
        &self.advertise
    }

    /// The pin this node publishes.
    pub(crate) fn pin(&self) -> Pin32 {
        self.identity.pin()
    }

    /// The cluster listener's TLS: a live node's certificate required.
    pub(crate) fn server_config(&self) -> Result<Arc<rustls::ServerConfig>, String> {
        pinning::server_config(&self.identity, Arc::clone(&self.pins))
    }

    /// The live nodes' pins, as the cluster listener trusts them.
    pub(crate) fn pins(&self) -> &Arc<PinSet> {
        &self.pins
    }

    /// Replaces what this node knows from the heartbeats (the live nodes,
    /// as listed): each node's pins, and each cluster address with the pins
    /// of the node whose heartbeat naming it is newest (a node restarted
    /// under another id at the same address is trusted there at once). Every
    /// pin is trusted until its heartbeat's `written_at_ms + horizon_ms`,
    /// however long listings then fail.
    ///
    /// Returns whether an address this node knew was dropped or now has
    /// other pins: connections pooled to it must not be used again. This
    /// node's own address does not count (it never connects there), so its
    /// own heartbeat lapsing does not drop its connections to its peers.
    pub(crate) fn refresh(&self, nodes: &[NodeInfo], horizon_ms: u64) -> bool {
        let expiry = |n: &NodeInfo| n.written_at_ms.saturating_add(horizon_ms);
        self.pins.replace(
            nodes
                .iter()
                .map(|n| (n.id.as_str(), n.tls_pins.as_slice(), expiry(n))),
        );
        let mut by_address: HashMap<String, Claim> = HashMap::new();
        for n in nodes {
            let Some(key) = n
                .cluster_address
                .as_deref()
                .and_then(|a| a.parse::<Uri>().ok())
                .as_ref()
                .and_then(address_key)
            else {
                continue;
            };
            let claim = Claim {
                pins: n.tls_pins.clone(),
                written_at_ms: n.written_at_ms,
                expires_at_ms: expiry(n),
                tied: false,
            };
            match by_address.entry(key) {
                std::collections::hash_map::Entry::Vacant(v) => {
                    v.insert(claim);
                }
                std::collections::hash_map::Entry::Occupied(mut o) => {
                    let held = o.get_mut();
                    if claim.written_at_ms > held.written_at_ms {
                        *held = claim;
                    } else if claim.written_at_ms == held.written_at_ms && claim.pins != held.pins {
                        held.tied = true;
                    }
                }
            }
        }
        let own = self
            .advertise
            .parse::<Uri>()
            .ok()
            .as_ref()
            .and_then(address_key);
        let mut known = self.by_address.write().unwrap_or_else(|p| p.into_inner());
        // This node's own address changing (its own heartbeat lapsing) is
        // no reason to drop its pools; but only while that address names
        // nothing but this node's own pin: anything else there is a peer.
        let mine = [self.identity.pin()];
        let only_mine = |c: &Claim| c.pins == mine && !c.tied;
        let changed = known.iter().any(|(key, old)| {
            let new = by_address.get(key);
            let differs = new.is_none_or(|new| new.pins != old.pins || new.tied != old.tied);
            let own_only = Some(key) == own.as_ref() && only_mine(old) && new.is_none_or(only_mine);
            differs && !own_only
        });
        *known = by_address;
        drop(known);
        self.listed.send_modify(|n| *n = n.wrapping_add(1));
        changed
    }

    /// Whether the claim on `uri`'s address is refused only because its
    /// heartbeat, as last listed, expired: a newer listing may renew it.
    fn lapsed(&self, uri: &Uri) -> bool {
        address_key(uri).is_some_and(|key| {
            self.by_address
                .read()
                .unwrap_or_else(|p| p.into_inner())
                .get(&key)
                .is_some_and(|c| !c.tied && now_ms() > c.expires_at_ms)
        })
    }

    /// The pins the node at `uri` published, while its heartbeat is live;
    /// an error if no live node published that address, if it published no
    /// pin, or if two published it at the same time.
    pub(crate) fn expected(&self, uri: &Uri) -> Result<Vec<Pin32>, String> {
        let key = address_key(uri).ok_or("no host in the address")?;
        match self
            .by_address
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .get(&key)
        {
            Some(c) if c.tied => Err(format!(
                "{key}: claimed by two live nodes at once; trusting neither"
            )),
            Some(c) if now_ms() > c.expires_at_ms => Err(format!(
                "{key}: the heartbeat naming it expired (the bucket was not listed since)"
            )),
            Some(c) if c.pins.is_empty() => {
                Err(format!("{key}: the node published no certificate pin"))
            }
            Some(c) => Ok(c.pins.clone()),
            None => Err(format!(
                "{key}: no live node publishes this cluster address"
            )),
        }
    }

    /// Whether a connection to `uri` whose server presented `pin` may still
    /// be used: the pin is among those currently published there, live.
    pub(crate) fn still_trusted(&self, uri: &Uri, pin: Option<Pin32>) -> bool {
        pin.is_some_and(|pin| self.expected(uri).is_ok_and(|p| p.contains(&pin)))
    }
}

/// Wall clock in milliseconds since the Unix epoch.
pub(crate) fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

/// How often a connection that outlives its request (a relayed WebSocket)
/// checks that its peer is still trusted.
pub(crate) const REVOCATION_CHECK: Duration = Duration::from_millis(500);

/// A future that resolves once `still_trusted` turns false, checked every
/// [`REVOCATION_CHECK`].
pub(crate) type Revocation = Pin<Box<dyn Future<Output = ()> + Send>>;

/// [`Revocation`] for `still_trusted`.
pub(crate) fn until_untrusted(still_trusted: impl Fn() -> bool + Send + 'static) -> Revocation {
    Box::pin(async move {
        while still_trusted() {
            tokio::time::sleep(REVOCATION_CHECK).await;
        }
    })
}

/// Whether a peer node is still trusted (checked again and again).
pub(crate) type StillTrusted = Arc<dyn Fn() -> bool + Send + Sync>;

/// A response body between nodes, cut once its peer stops being trusted:
/// checked before every frame, and every [`REVOCATION_CHECK`] while
/// waiting for one, so a response the peer answers (or streams) after its
/// pin lapsed is not delivered. Cut, it fails like a dropped connection.
pub(crate) fn revocable_body(
    body: HyperOutgoingBody,
    still_trusted: StillTrusted,
) -> HyperOutgoingBody {
    http_body_util::BodyExt::boxed_unsync(RevocableBody {
        inner: body,
        still_trusted,
        timer: None,
        gone: false,
    })
}

struct RevocableBody {
    inner: HyperOutgoingBody,
    still_trusted: StillTrusted,
    /// The next check while no frame comes (armed on the first wait).
    timer: Option<Pin<Box<tokio::time::Sleep>>>,
    gone: bool,
}

impl http_body::Body for RevocableBody {
    type Data = bytes::Bytes;
    type Error = wasmtime_wasi_http::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        let this = self.get_mut();
        loop {
            if this.gone || !(this.still_trusted)() {
                this.gone = true;
                return Poll::Ready(Some(Err(wasmtime_wasi_http::Error::InternalError(Some(
                    "the peer node is no longer trusted".into(),
                )))));
            }
            if let Poll::Ready(frame) = Pin::new(&mut this.inner).poll_frame(cx) {
                return Poll::Ready(frame);
            }
            let timer = this
                .timer
                .get_or_insert_with(|| Box::pin(tokio::time::sleep(REVOCATION_CHECK)));
            if timer.as_mut().poll(cx).is_pending() {
                return Poll::Pending;
            }
            timer
                .as_mut()
                .reset(tokio::time::Instant::now() + REVOCATION_CHECK);
        }
    }

    fn is_end_stream(&self) -> bool {
        !self.gone && self.inner.is_end_stream()
    }

    fn size_hint(&self) -> http_body::SizeHint {
        self.inner.size_hint()
    }
}

/// A connection between nodes that outlives its request (an upgraded
/// WebSocket): once its peer stops being trusted (its heartbeat expired,
/// or its pin changed), every read and write fails, so whatever uses it
/// sees the connection drop.
pub(crate) struct Revocable<T> {
    inner: T,
    revoked: Option<Revocation>,
    gone: bool,
}

impl<T> Revocable<T> {
    /// `inner`, cut once `revoked` resolves (never if `None`).
    pub(crate) fn new(inner: T, revoked: Option<Revocation>) -> Self {
        Self {
            inner,
            revoked,
            gone: false,
        }
    }

    fn check(&mut self, cx: &mut Context<'_>) -> std::io::Result<()> {
        if !self.gone
            && let Some(r) = self.revoked.as_mut()
            && r.as_mut().poll(cx).is_ready()
        {
            self.gone = true;
        }
        if self.gone {
            return Err(std::io::Error::new(
                std::io::ErrorKind::ConnectionAborted,
                "the peer node is no longer trusted",
            ));
        }
        Ok(())
    }
}

impl<T: tokio::io::AsyncRead + Unpin> tokio::io::AsyncRead for Revocable<T> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        this.check(cx)?;
        Pin::new(&mut this.inner).poll_read(cx, buf)
    }
}

impl<T: tokio::io::AsyncWrite + Unpin> tokio::io::AsyncWrite for Revocable<T> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let this = self.get_mut();
        this.check(cx)?;
        Pin::new(&mut this.inner).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        this.check(cx)?;
        Pin::new(&mut this.inner).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}

/// The pin of the certificate a node presented on a TLS connection: on
/// every response that came over one ([`Connected::extra`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PeerPin(pub(crate) Pin32);

/// Whether the peer refused the TLS handshake (a fatal alert it sends only
/// while its handshake runs, as when it does not trust this node's
/// certificate): then it never read a request sent on that connection,
/// even though TLS 1.3 let this side write one first. Why these alerts
/// mean the handshake, and for which version of rustls that was checked:
/// `never_read_by_peer` in `cluster.rs`.
pub(crate) fn handshake_refused(error: &(dyn std::error::Error + 'static)) -> bool {
    use rustls::AlertDescription as A;
    let mut source = Some(error);
    while let Some(e) = source {
        let tls = e
            .downcast_ref::<std::io::Error>()
            .and_then(|io| io.get_ref())
            .and_then(|inner| inner.downcast_ref::<rustls::Error>())
            .or_else(|| e.downcast_ref::<rustls::Error>());
        if let Some(rustls::Error::AlertReceived(alert)) = tls {
            return matches!(
                alert,
                A::HandshakeFailure
                    | A::BadCertificate
                    | A::UnsupportedCertificate
                    | A::CertificateRevoked
                    | A::CertificateExpired
                    | A::CertificateUnknown
                    | A::UnknownCA
                    | A::AccessDenied
                    | A::CertificateRequired
            );
        }
        source = e.source();
    }
    false
}

/// Whether hyper-util gave the request back unsent: the connection closed
/// before the request was written (it reports `Canceled` exactly when it
/// got the message back, and retries it itself only on a reused pooled
/// connection). On a fresh connection to a peer that refuses this node's
/// certificate, the refusal can close it first, before any alert reaches
/// this side: then this, not [`handshake_refused`], is what arrives. Its
/// `is_canceled` is private, so the kind is read from the message, which
/// the pinned version writes as `client error (Canceled)`.
pub(crate) fn request_unsent(error: &hyper_util::client::legacy::Error) -> bool {
    error.to_string() == "client error (Canceled)"
}

/// `host:port` of an `https://` address, lowercased, the port 443 if not
/// given: how cluster addresses are matched.
fn address_key(uri: &Uri) -> Option<String> {
    let host = uri.host().filter(|h| !h.is_empty())?;
    let port = uri.port_u16().unwrap_or(443);
    Some(format!("{}:{port}", host.to_ascii_lowercase()))
}

/// Connects to other nodes. Cheap to clone.
#[derive(Debug, Clone)]
pub(crate) struct ClusterConnector {
    http: HttpConnector,
    /// This node's identity and its peers' pins; without it `https://` is
    /// refused (this node could not present a certificate).
    tls: Option<Arc<ClusterTls>>,
    /// Refuse mode (ADR 0011, phase B): `http://` is never dialled.
    refuse_plaintext: bool,
}

impl ClusterConnector {
    /// Plain HTTP only.
    #[cfg(test)]
    pub(crate) fn plain() -> Self {
        Self::new(None, false)
    }

    /// Plain HTTP (unless `refuse_plaintext`), and mutual TLS to
    /// `https://` addresses if `tls` is given.
    pub(crate) fn new(tls: Option<Arc<ClusterTls>>, refuse_plaintext: bool) -> Self {
        let mut http = HttpConnector::new();
        // The scheme is checked here, not by the HTTP connector: `https://`
        // gets TLS on top of the TCP connection it makes.
        http.enforce_http(false);
        http.set_nodelay(true);
        http.set_connect_timeout(Some(CONNECT_TIMEOUT));
        Self {
            http,
            tls,
            refuse_plaintext,
        }
    }
}

impl tower_service::Service<Uri> for ClusterConnector {
    type Response = ClusterConn;
    type Error = ConnectError;
    type Future = Pin<Box<dyn Future<Output = Result<ClusterConn, ConnectError>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        tower_service::Service::<Uri>::poll_ready(&mut self.http, cx).map_err(Into::into)
    }

    fn call(&mut self, uri: Uri) -> Self::Future {
        match uri.scheme_str() {
            Some("http") if self.refuse_plaintext => Box::pin(async {
                Err("plain HTTP between nodes is refused (--cluster-plaintext refuse)".into())
            }),
            Some("http") => {
                let connecting = tower_service::Service::call(&mut self.http, uri);
                Box::pin(async move { Ok(ClusterConn::Plain(connecting.await?)) })
            }
            // Never plaintext to an address published as TLS.
            Some("https") => match self.tls.clone() {
                Some(tls) => Box::pin(self.connect_tls(tls, uri)),
                None => Box::pin(async {
                    Err("TLS between nodes is not configured on this node".into())
                }),
            },
            other => {
                let other = other.unwrap_or("none").to_owned();
                Box::pin(async move { Err(format!("unsupported scheme {other}").into()) })
            }
        }
    }
}

impl ClusterConnector {
    /// TCP, then a TLS handshake presenting this node's certificate and
    /// requiring one of the pins the peer at `uri` published (looked up
    /// now, at connect time).
    fn connect_tls(
        &mut self,
        tls: Arc<ClusterTls>,
        uri: Uri,
    ) -> impl Future<Output = Result<ClusterConn, ConnectError>> + Send + 'static {
        // Lazy: nothing is dialled unless the pins were found.
        let connecting = tower_service::Service::call(&mut self.http, uri.clone());
        async move {
            let mut prepared = tls_config(&tls, &uri);
            if prepared.is_err() && tls.lapsed(&uri) {
                // Lapsed as last listed: ask for an early listing and wait
                // for one that renews the claim, at most `LAPSED_WAIT`. A
                // listing that does not renew it changes nothing.
                let mut listed = tls.listed.subscribe();
                tls.pins.note_unknown();
                let deadline = tokio::time::Instant::now() + LAPSED_WAIT;
                while tls.lapsed(&uri)
                    && tokio::time::timeout_at(deadline, listed.changed())
                        .await
                        .is_ok_and(|r| r.is_ok())
                {}
                prepared = tls_config(&tls, &uri);
            }
            let (config, name) = prepared.map_err(ConnectError::from)?;
            let tcp = connecting.await?.into_inner();
            let connector = tokio_rustls::TlsConnector::from(config);
            let stream = tokio::time::timeout(HANDSHAKE_TIMEOUT, connector.connect(name, tcp))
                .await
                .map_err(|_| "TLS handshake with the node timed out")??;
            Ok(ClusterConn::Tls(Box::new(TokioIo::new(stream))))
        }
    }
}

/// The client TLS for `uri`: this node's certificate, and one of the pins
/// the peer there published required (looked up now).
fn tls_config(
    tls: &ClusterTls,
    uri: &Uri,
) -> Result<
    (
        Arc<rustls::ClientConfig>,
        rustls::pki_types::ServerName<'static>,
    ),
    String,
> {
    let expected = tls.expected(uri)?;
    // A certificate that matches no pin published there makes this node
    // list the heartbeats sooner (the peer may have restarted); a refused
    // or timed-out connection does not.
    let config =
        pinning::client_config_reporting(&tls.identity, expected, Some(Arc::clone(&tls.pins)))?;
    // Names are not checked (the pin is the identity), but a valid one is
    // needed for the handshake (SNI for a DNS name).
    let host = uri.host().unwrap_or_default();
    let host = host.trim_start_matches('[').trim_end_matches(']');
    let name = rustls::pki_types::ServerName::try_from(host.to_owned())
        .map_err(|e| format!("{host}: {e}"))?;
    Ok((config, name))
}

/// A connection to another node.
#[derive(Debug)]
pub(crate) enum ClusterConn {
    /// Plain HTTP.
    Plain(TokioIo<TcpStream>),
    /// Mutual TLS to a peer's cluster listener.
    Tls(Box<TokioIo<tokio_rustls::client::TlsStream<TcpStream>>>),
}

impl Connection for ClusterConn {
    fn connected(&self) -> Connected {
        match self {
            Self::Plain(s) => s.connected(),
            Self::Tls(s) => {
                let (tcp, session) = s.inner().get_ref();
                let connected = tcp.connected();
                match session.peer_certificates().and_then(|c| c.first()) {
                    Some(cert) => connected.extra(PeerPin(pinning::fingerprint(cert))),
                    None => connected,
                }
            }
        }
    }
}

impl Read for ClusterConn {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: ReadBufCursor<'_>,
    ) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            Self::Plain(s) => Pin::new(s).poll_read(cx, buf),
            Self::Tls(s) => Pin::new(s.as_mut()).poll_read(cx, buf),
        }
    }
}

impl Write for ClusterConn {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        match self.get_mut() {
            Self::Plain(s) => Pin::new(s).poll_write(cx, buf),
            Self::Tls(s) => Pin::new(s.as_mut()).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            Self::Plain(s) => Pin::new(s).poll_flush(cx),
            Self::Tls(s) => Pin::new(s.as_mut()).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            Self::Plain(s) => Pin::new(s).poll_shutdown(cx),
            Self::Tls(s) => Pin::new(s.as_mut()).poll_shutdown(cx),
        }
    }

    fn is_write_vectored(&self) -> bool {
        match self {
            Self::Plain(s) => s.is_write_vectored(),
            Self::Tls(s) => s.is_write_vectored(),
        }
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[std::io::IoSlice<'_>],
    ) -> Poll<std::io::Result<usize>> {
        match self.get_mut() {
            Self::Plain(s) => Pin::new(s).poll_write_vectored(cx, bufs),
            Self::Tls(s) => Pin::new(s.as_mut()).poll_write_vectored(cx, bufs),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tower_service::Service;

    #[tokio::test]
    async fn plain_connects_and_https_is_refused_without_tls() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let mut c = ClusterConnector::plain();
        let uri: Uri = format!("http://{addr}/").parse().unwrap();
        let conn = c.call(uri).await.unwrap();
        assert!(matches!(conn, ClusterConn::Plain(_)));
        // The same address published as TLS: never reached in plaintext.
        let uri: Uri = format!("https://{addr}/").parse().unwrap();
        let err = c.call(uri).await.unwrap_err();
        assert!(err.to_string().contains("not configured"), "{err}");
        let uri: Uri = format!("ftp://{addr}/").parse().unwrap();
        assert!(c.call(uri).await.is_err());
    }

    #[tokio::test]
    async fn refuse_mode_never_dials_plain_http() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let mut c = ClusterConnector::new(None, true);
        let uri: Uri = format!("http://{addr}/").parse().unwrap();
        let err = c.call(uri).await.unwrap_err();
        assert!(err.to_string().contains("refused"), "{err}");
        // Nothing was dialled.
        let accepted =
            tokio::time::timeout(std::time::Duration::from_millis(200), listener.accept()).await;
        assert!(accepted.is_err(), "a plain connection was made");
    }

    type Listed = Vec<(String, Option<String>, Vec<Pin32>)>;

    /// A heartbeat as listed, written at `written_at_ms`.
    fn info(id: &str, address: Option<&str>, pins: Vec<Pin32>, written_at_ms: u64) -> NodeInfo {
        NodeInfo {
            id: id.into(),
            address: "http://127.0.0.1:9".into(),
            version: "0".into(),
            written_at_ms,
            app: String::new(),
            cluster_address: address.map(Into::into),
            tls_pins: pins,
            drain: None,
            reads_secrets: false,
            can_take: false,
            instance: None,
            lease: None,
        }
    }

    /// Refreshes `tls` with `nodes`, all heartbeats written now and live
    /// for a minute.
    fn refresh(tls: &ClusterTls, nodes: &Listed) -> bool {
        let now = now_ms();
        let listed: Vec<NodeInfo> = nodes
            .iter()
            .map(|(id, addr, pins)| info(id, addr.as_deref(), pins.clone(), now))
            .collect();
        tls.refresh(&listed, 60_000)
    }

    /// A port for node `id`'s cluster listener, and its identity.
    async fn listener(id: &str) -> (ClusterTls, tokio::net::TcpListener, std::net::SocketAddr) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let tls = ClusterTls::generate(id, &format!("https://{addr}")).unwrap();
        (tls, listener, addr)
    }

    /// Accepts one TLS connection as `server`: the handshake's outcome
    /// comes back on the handle.
    fn accept_one(
        server: &ClusterTls,
        listener: tokio::net::TcpListener,
    ) -> tokio::task::JoinHandle<Result<(), String>> {
        let acceptor = tokio_rustls::TlsAcceptor::from(server.server_config().unwrap());
        tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.map_err(|e| e.to_string())?;
            acceptor.accept(tcp).await.map_err(|e| e.to_string())?;
            Ok(())
        })
    }

    #[tokio::test]
    async fn https_peers_are_reached_with_mutual_tls_and_their_published_pin() {
        let (b, listener, addr) = listener("b").await;
        let a = Arc::new(ClusterTls::generate("a", "https://a.test:7443").unwrap());
        let nodes: Listed = vec![
            ("a".into(), Some(a.advertise().into()), vec![a.pin()]),
            ("b".into(), Some(b.advertise().into()), vec![b.pin()]),
        ];
        refresh(&a, &nodes);
        refresh(&b, &nodes);
        let accepted = accept_one(&b, listener);
        let mut c = ClusterConnector::new(Some(Arc::clone(&a)), false);
        let conn = c.call(format!("https://{addr}/").parse().unwrap()).await;
        assert!(matches!(conn, Ok(ClusterConn::Tls(_))), "{conn:?}");
        // b accepted a's certificate: a's pin is live.
        assert_eq!(accepted.await.unwrap(), Ok(()));
        // Plain addresses are still reached in plaintext.
        let plain = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let conn = c
            .call(
                format!("http://{}/", plain.local_addr().unwrap())
                    .parse()
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(matches!(conn, ClusterConn::Plain(_)));
    }

    #[tokio::test]
    async fn a_peer_must_present_the_pin_it_published() {
        let (b, listener, addr) = listener("b").await;
        let a = Arc::new(ClusterTls::generate("a", "https://a.test:7443").unwrap());
        // The heartbeat names b's address with another key's pin (b
        // restarted, or someone else answers there).
        let other = ClusterTls::generate("b", "https://b.test:7443").unwrap();
        let nodes: Listed = vec![
            ("a".into(), Some(a.advertise().into()), vec![a.pin()]),
            ("b".into(), Some(b.advertise().into()), vec![other.pin()]),
        ];
        refresh(&a, &nodes);
        refresh(&b, &nodes);
        let accepted = accept_one(&b, listener);
        let mut c = ClusterConnector::new(Some(Arc::clone(&a)), false);
        let err = c
            .call(format!("https://{addr}/").parse().unwrap())
            .await
            .unwrap_err();
        assert!(err.to_string().contains("published pin"), "{err}");
        let _ = accepted.await;
        // A pin mismatch makes a list the heartbeats again sooner.
        let early = tokio::time::timeout(Duration::from_millis(100), a.pins().unknown_seen());
        assert!(early.await.is_ok(), "mismatch not reported");
    }

    #[tokio::test]
    async fn a_claim_lapsed_because_the_listing_is_late_waits_for_the_next_listing() {
        let (b, listener, addr) = listener("b").await;
        let a = Arc::new(ClusterTls::generate("a", "https://a.test:7443").unwrap());
        let nodes: Listed = vec![
            ("a".into(), Some(a.advertise().into()), vec![a.pin()]),
            ("b".into(), Some(b.advertise().into()), vec![b.pin()]),
        ];
        refresh(&b, &nodes);
        // a last listed b's heartbeat long ago: its claim has lapsed,
        // though b is live (a's own listing is late).
        let old = now_ms() - 120_000;
        let stale: Vec<NodeInfo> = nodes
            .iter()
            .map(|(id, addr, pins)| info(id, addr.as_deref(), pins.clone(), old))
            .collect();
        a.refresh(&stale, 60_000);
        let accepted = accept_one(&b, listener);
        let mut c = ClusterConnector::new(Some(Arc::clone(&a)), false);
        let call = tokio::spawn(c.call(format!("https://{addr}/").parse().unwrap()));
        // The lapse asks for an early listing; it lists b live.
        let early = tokio::time::timeout(Duration::from_secs(1), a.pins().unknown_seen());
        assert!(early.await.is_ok(), "lapse not reported");
        refresh(&a, &nodes);
        let conn = call.await.unwrap();
        assert!(matches!(conn, Ok(ClusterConn::Tls(_))), "{conn:?}");
        assert_eq!(accepted.await.unwrap(), Ok(()));
    }

    #[tokio::test]
    async fn a_lapsed_claim_no_listing_renews_is_still_refused() {
        let a = Arc::new(ClusterTls::generate("a", "https://a.test:7443").unwrap());
        let old = now_ms() - 120_000;
        a.refresh(
            &[info("b", Some("https://127.0.0.1:9"), vec![[7; 32]], old)],
            60_000,
        );
        let mut c = ClusterConnector::new(Some(Arc::clone(&a)), false);
        let started = std::time::Instant::now();
        // A listing that still shows the old heartbeat does not help.
        let relist = Arc::clone(&a);
        tokio::spawn(async move {
            relist.pins().unknown_seen().await;
            relist.refresh(
                &[info("b", Some("https://127.0.0.1:9"), vec![[7; 32]], old)],
                60_000,
            );
        });
        let err = c
            .call("https://127.0.0.1:9/".parse().unwrap())
            .await
            .unwrap_err();
        assert!(err.to_string().contains("expired"), "{err}");
        assert!(started.elapsed() <= LAPSED_WAIT + Duration::from_secs(1));
    }

    #[tokio::test]
    async fn an_unreachable_peer_does_not_make_the_node_list_sooner() {
        // A port nothing listens on, published with a pin: the connection is
        // refused before any certificate is seen.
        let closed = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = closed.local_addr().unwrap();
        drop(closed);
        let a = Arc::new(ClusterTls::generate("a", "https://a.test:7443").unwrap());
        refresh(
            &a,
            &vec![("b".into(), Some(format!("https://{addr}")), vec![[7; 32]])],
        );
        let mut c = ClusterConnector::new(Some(Arc::clone(&a)), false);
        assert!(
            c.call(format!("https://{addr}/").parse().unwrap())
                .await
                .is_err()
        );
        let early = tokio::time::timeout(Duration::from_millis(100), a.pins().unknown_seen());
        assert!(early.await.is_err(), "a refused connection was reported");
    }

    #[tokio::test]
    async fn a_revocable_connection_fails_once_its_peer_is_untrusted() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let trusted = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let (near, mut far) = tokio::io::duplex(1024);
        let flag = Arc::clone(&trusted);
        let mut conn = Revocable::new(
            near,
            Some(until_untrusted(move || {
                flag.load(std::sync::atomic::Ordering::SeqCst)
            })),
        );
        far.write_all(b"x").await.unwrap();
        let mut b = [0u8; 1];
        conn.read_exact(&mut b).await.unwrap();
        // A read waiting for data is cut within a check period.
        trusted.store(false, std::sync::atomic::Ordering::SeqCst);
        let cut = tokio::time::timeout(REVOCATION_CHECK * 3, conn.read(&mut b)).await;
        assert!(matches!(cut, Ok(Err(_))), "{cut:?}");
        assert!(conn.write_all(b"y").await.is_err());
    }

    #[tokio::test]
    async fn a_revocable_body_fails_once_its_peer_is_untrusted() {
        use http_body_util::BodyExt;
        let trusted = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let flag = Arc::clone(&trusted);
        let still: StillTrusted = Arc::new(move || flag.load(std::sync::atomic::Ordering::SeqCst));
        // Trusted: the body comes through.
        let full = |text: &'static str| {
            http_body_util::Full::new(bytes::Bytes::from_static(text.as_bytes()))
                .map_err(|never| match never {})
                .boxed_unsync()
        };
        let body = revocable_body(full("whole"), Arc::clone(&still));
        assert_eq!(body.collect().await.unwrap().to_bytes(), "whole");
        // A body still streaming: cut within a check period once the peer
        // is untrusted, though no frame comes.
        let (tx, rx) = tokio::sync::mpsc::channel::<
            Result<http_body::Frame<bytes::Bytes>, wasmtime_wasi_http::Error>,
        >(1);
        let stream = http_body_util::StreamBody::new(tokio_stream_from(rx)).boxed_unsync();
        let mut body = revocable_body(stream, Arc::clone(&still));
        tx.send(Ok(http_body::Frame::data(bytes::Bytes::from_static(b"x"))))
            .await
            .unwrap();
        assert!(body.frame().await.unwrap().is_ok());
        trusted.store(false, std::sync::atomic::Ordering::SeqCst);
        let cut = tokio::time::timeout(REVOCATION_CHECK * 3, body.frame()).await;
        assert!(matches!(cut, Ok(Some(Err(_)))), "not cut while waiting");
        // A complete answer that arrives after the peer was untrusted is
        // not delivered either.
        let body = revocable_body(full("late"), still);
        assert!(body.collect().await.is_err());
        drop(tx);
    }

    /// An mpsc receiver as a stream.
    fn tokio_stream_from<T: Send + 'static>(
        mut rx: tokio::sync::mpsc::Receiver<T>,
    ) -> impl futures_util::Stream<Item = T> + Send {
        futures_util::stream::poll_fn(move |cx| rx.poll_recv(cx))
    }

    #[tokio::test]
    async fn no_connection_is_made_to_an_address_without_a_live_pin() {
        let (b, listener, addr) = listener("b").await;
        let a = Arc::new(ClusterTls::generate("a", "https://a.test:7443").unwrap());
        let mut c = ClusterConnector::new(Some(Arc::clone(&a)), false);
        let uri: Uri = format!("https://{addr}/").parse().unwrap();
        // Nobody published it.
        let err = c.call(uri.clone()).await.unwrap_err();
        assert!(err.to_string().contains("no live node"), "{err}");
        // Published without a pin.
        refresh(&a, &vec![("b".into(), Some(b.advertise().into()), vec![])]);
        let err = c.call(uri.clone()).await.unwrap_err();
        assert!(err.to_string().contains("no certificate pin"), "{err}");
        // Claimed by two live nodes at the same time: neither is trusted.
        refresh(
            &a,
            &vec![
                ("b".into(), Some(b.advertise().into()), vec![b.pin()]),
                ("c".into(), Some(b.advertise().into()), vec![a.pin()]),
            ],
        );
        let err = c.call(uri.clone()).await.unwrap_err();
        assert!(err.to_string().contains("two live nodes at once"), "{err}");
        // A heartbeat that expired: gone from the next refresh.
        refresh(
            &a,
            &vec![("b".into(), Some(b.advertise().into()), vec![b.pin()])],
        );
        refresh(&a, &Vec::new());
        assert!(c.call(uri).await.is_err());
        // None of these dialled b.
        let dialled = tokio::time::timeout(Duration::from_millis(100), listener.accept()).await;
        assert!(dialled.is_err(), "a connection was made");
    }

    #[tokio::test]
    async fn pins_expire_with_their_heartbeat_even_if_listing_stops() {
        // One listing, then none (the bucket cannot be listed): trust lasts
        // until the heartbeat's own horizon, not until the next listing.
        let a = ClusterTls::generate("a", "https://a.test:7443").unwrap();
        let b = Identity::generate("b", "b.test").unwrap();
        let uri: Uri = "https://b.test:7443".parse().unwrap();
        let now = now_ms();
        a.refresh(
            &[info("b", Some("https://b.test:7443"), vec![b.pin()], now)],
            300,
        );
        assert_eq!(a.pins().node_of(b.cert_der()).as_deref(), Some("b"));
        assert_eq!(a.expected(&uri), Ok(vec![b.pin()]));
        assert!(a.still_trusted(&uri, Some(b.pin())));
        tokio::time::sleep(Duration::from_millis(400)).await;
        assert_eq!(a.pins().node_of(b.cert_der()), None);
        let err = a.expected(&uri).unwrap_err();
        assert!(err.contains("expired"), "{err}");
        assert!(!a.still_trusted(&uri, Some(b.pin())));
        // A heartbeat written long ago is not trusted at all.
        a.refresh(
            &[info(
                "b",
                Some("https://b.test:7443"),
                vec![b.pin()],
                now - 10_000,
            )],
            300,
        );
        assert_eq!(a.pins().node_of(b.cert_der()), None);
        assert!(a.expected(&uri).is_err());
    }

    #[test]
    fn the_newest_heartbeat_naming_an_address_is_trusted_there() {
        // b restarted as b2 on the same address: its old heartbeat is still
        // live for a while, but the newer one wins.
        let a = ClusterTls::generate("a", "https://a.test:7443").unwrap();
        let (old, new) = ([1; 32], [2; 32]);
        let uri: Uri = "https://b.test:7443".parse().unwrap();
        let now = now_ms();
        for order in [[0, 1], [1, 0]] {
            let nodes = [
                info("b", Some("https://b.test:7443"), vec![old], now - 1_000),
                info("b2", Some("https://B.test:7443/"), vec![new], now),
            ];
            let listed: Vec<NodeInfo> = order.iter().map(|&i| nodes[i].clone()).collect();
            a.refresh(&listed, 60_000);
            assert_eq!(a.expected(&uri), Ok(vec![new]));
            assert!(!a.still_trusted(&uri, Some(old)));
        }
    }

    #[test]
    fn a_refresh_reports_addresses_whose_pins_changed_or_went_away() {
        let a = ClusterTls::generate("a", "https://a.test:7443").unwrap();
        let addr = Some("https://b.test:7443".to_owned());
        let b = |pin: Pin32| vec![("b".to_owned(), addr.clone(), vec![pin])];
        assert!(!refresh(&a, &b([1; 32])), "nothing was known before");
        assert!(!refresh(&a, &b([1; 32])), "same pins");
        let mut two = b([1; 32]);
        two.push((
            "c".into(),
            Some("https://c.test:7443".into()),
            vec![[3; 32]],
        ));
        assert!(!refresh(&a, &two), "a new address only");
        assert!(refresh(&a, &b([2; 32])), "b's pin changed (and c left)");
        assert!(refresh(&a, &Vec::new()), "b left");
        assert!(!refresh(&a, &b([1; 32])), "back, nothing pooled since");
        // a's own address naming only a's own pin: its own heartbeat
        // appearing or lapsing leaves its pools to b alone.
        let own = |pin: Pin32| {
            let mut nodes = b([1; 32]);
            nodes.push(("a".into(), Some(a.advertise().into()), vec![pin]));
            nodes
        };
        let mine = a.identity.pin();
        assert!(!refresh(&a, &own(mine)));
        assert!(!refresh(&a, &b([1; 32])), "a's own heartbeat lapsed");
        assert!(!refresh(&a, &own(mine)), "and came back");
        // Another pin at a's address is a peer there (a misconfigured
        // node, or an old id of this one): a change counts, so a pooled
        // connection to that address is not reused.
        assert!(
            refresh(&a, &own([4; 32])),
            "another pin appeared at a's address"
        );
        assert!(
            refresh(&a, &own([5; 32])),
            "another pin at a's address changed"
        );
    }

    #[test]
    fn only_an_https_host_and_port_is_a_cluster_address() {
        for ok in [
            "https://10.0.0.1:7443",
            "https://node-1.internal:7443/",
            "https://[::1]:7443",
        ] {
            assert!(ClusterTls::generate("a", ok).is_ok(), "{ok}");
        }
        for bad in [
            "http://10.0.0.1:7443",
            "10.0.0.1:7443",
            "https://",
            "https://10.0.0.1:7443/x",
            "https://10.0.0.1:7443?q",
            "",
        ] {
            assert!(ClusterTls::generate("a", bad).is_err(), "{bad}");
        }
        let a = |s: &str| address_key(&s.parse().unwrap());
        assert_eq!(a("https://Node.Test"), Some("node.test:443".into()));
        assert_eq!(a("https://node.test:443/"), a("https://NODE.test"));
        assert_ne!(a("https://node.test:7443"), a("https://node.test"));
    }
}

/// Identity and pins (ADR 0011).
pub(crate) mod pinning {
    use std::sync::Arc;

    /// A node's TLS identity: a key and a self-signed certificate made in memory
    /// at start (never written anywhere), and the certificate's pin.
    #[derive(Debug)]
    pub(crate) struct Identity {
        cert: rustls::pki_types::CertificateDer<'static>,
        key: rustls::pki_types::PrivatePkcs8KeyDer<'static>,
        pin: Pin32,
    }

    /// A SHA-256 fingerprint of a certificate (DER).
    pub(crate) type Pin32 = [u8; 32];

    /// The pin of a certificate.
    pub(crate) fn fingerprint(cert: &[u8]) -> Pin32 {
        use sha2::{Digest, Sha256};
        Sha256::digest(cert).into()
    }

    impl Identity {
        /// A fresh ECDSA P-256 key and a self-signed certificate naming node
        /// `node_id` (common name) and, as its subject alternative name, `host`
        /// (the host of its cluster address). Peers never check names: they
        /// check the pin.
        pub(crate) fn generate(node_id: &str, host: &str) -> Result<Self, String> {
            let key = rcgen::KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256)
                .map_err(|e| format!("generating the node key: {e}"))?;
            let mut params = rcgen::CertificateParams::new(vec![host.to_owned()])
                .map_err(|e| format!("certificate for {host}: {e}"))?;
            params.distinguished_name = rcgen::DistinguishedName::new();
            params
                .distinguished_name
                .push(rcgen::DnType::CommonName, node_id);
            let cert = params
                .self_signed(&key)
                .map_err(|e| format!("signing the node certificate: {e}"))?;
            let cert = cert.der().clone();
            let pin = fingerprint(&cert);
            Ok(Self {
                cert,
                key: rustls::pki_types::PrivatePkcs8KeyDer::from(key.serialize_der()),
                pin,
            })
        }

        /// The pin peers must know to trust this node.
        pub(crate) fn pin(&self) -> Pin32 {
            self.pin
        }

        /// The certificate (DER).
        #[cfg(test)]
        pub(crate) fn cert_der(&self) -> &[u8] {
            &self.cert
        }

        fn chain(&self) -> Vec<rustls::pki_types::CertificateDer<'static>> {
            vec![self.cert.clone()]
        }

        fn key(&self) -> rustls::pki_types::PrivateKeyDer<'static> {
            rustls::pki_types::PrivateKeyDer::Pkcs8(self.key.clone_key())
        }
    }

    /// The pins of the live nodes (from their heartbeats), shared by this
    /// node's server and clients. Pins are public (they are published in the
    /// bucket), so looking them up need not be constant-time.
    #[derive(Debug, Default)]
    pub(crate) struct PinSet {
        /// Each live pin's node, and when its heartbeat expires (ms).
        by_pin: std::sync::RwLock<std::collections::HashMap<Pin32, (String, u64)>>,
        /// Woken when a pin was not known (a peer refused, or one refusing
        /// us): the heartbeats are listed again sooner (rate-limited by the
        /// waiter, so strangers cannot make this node hammer the bucket).
        unknown: tokio::sync::Notify,
    }

    impl PinSet {
        /// Replaces the set: each live node's id with its pins, trusted
        /// until the given time (ms since the epoch: when its heartbeat
        /// stops being live).
        pub(crate) fn replace<'a>(
            &self,
            nodes: impl IntoIterator<Item = (&'a str, &'a [Pin32], u64)>,
        ) {
            let mut by_pin = std::collections::HashMap::new();
            for (node, pins, until) in nodes {
                for pin in pins {
                    by_pin.insert(*pin, (node.to_owned(), until));
                }
            }
            *self.by_pin.write().unwrap_or_else(|p| p.into_inner()) = by_pin;
        }

        /// Records that a pin was not known: see [`PinSet::unknown_seen`].
        pub(crate) fn note_unknown(&self) {
            self.unknown.notify_one();
        }

        /// Resolves once a pin was not known since the last wait.
        pub(crate) async fn unknown_seen(&self) {
            self.unknown.notified().await;
        }

        /// The node a certificate belongs to, if its pin is live: listed,
        /// and its heartbeat not expired (even if no listing succeeded
        /// since).
        pub(crate) fn node_of(&self, cert: &[u8]) -> Option<String> {
            self.node_of_pin(&fingerprint(cert))
        }

        /// Like `node_of`, for a pin already computed (once per
        /// connection, rather than hashing the certificate on every
        /// check).
        pub(crate) fn node_of_pin(&self, pin: &Pin32) -> Option<String> {
            let now = super::now_ms();
            self.by_pin
                .read()
                .unwrap_or_else(|p| p.into_inner())
                .get(pin)
                .filter(|(_, until)| now <= *until)
                .map(|(node, _)| node.clone())
        }
    }

    fn provider() -> Arc<rustls::crypto::CryptoProvider> {
        Arc::new(rustls::crypto::ring::default_provider())
    }

    /// Checks the handshake signature with the certificate's key (TLS 1.3
    /// only: the configs offer nothing else).
    fn verify_signature(
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &rustls::crypto::ring::default_provider().signature_verification_algorithms,
        )
    }

    fn schemes() -> Vec<rustls::SignatureScheme> {
        rustls::crypto::ring::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }

    /// On the cluster listener: a client must present a certificate whose pin
    /// belongs to a live node.
    #[derive(Debug)]
    pub(crate) struct PinnedClientVerifier {
        pins: Arc<PinSet>,
    }

    impl rustls::server::danger::ClientCertVerifier for PinnedClientVerifier {
        fn offer_client_auth(&self) -> bool {
            true
        }

        fn client_auth_mandatory(&self) -> bool {
            true
        }

        fn root_hint_subjects(&self) -> &[rustls::DistinguishedName] {
            &[]
        }

        fn verify_client_cert(
            &self,
            end_entity: &rustls::pki_types::CertificateDer<'_>,
            _intermediates: &[rustls::pki_types::CertificateDer<'_>],
            _now: rustls::pki_types::UnixTime,
        ) -> Result<rustls::server::danger::ClientCertVerified, rustls::Error> {
            match self.pins.node_of(end_entity) {
                Some(_) => Ok(rustls::server::danger::ClientCertVerified::assertion()),
                None => {
                    self.pins.note_unknown();
                    Err(rustls::Error::General(
                        "not a live node's certificate".into(),
                    ))
                }
            }
        }

        fn verify_tls12_signature(
            &self,
            _message: &[u8],
            _cert: &rustls::pki_types::CertificateDer<'_>,
            _dss: &rustls::DigitallySignedStruct,
        ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
            Err(rustls::Error::General(
                "TLS 1.2 is not used between nodes".into(),
            ))
        }

        fn verify_tls13_signature(
            &self,
            message: &[u8],
            cert: &rustls::pki_types::CertificateDer<'_>,
            dss: &rustls::DigitallySignedStruct,
        ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
            verify_signature(message, cert, dss)
        }

        fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
            schemes()
        }
    }

    /// Connecting to one node: its certificate's pin must be one that node
    /// published. Names are not checked; the pin is the identity.
    #[derive(Debug)]
    pub(crate) struct PinnedServerVerifier {
        expected: Vec<Pin32>,
        /// Told when the certificate matches none of `expected`.
        report: Option<Arc<PinSet>>,
    }

    impl rustls::client::danger::ServerCertVerifier for PinnedServerVerifier {
        fn verify_server_cert(
            &self,
            end_entity: &rustls::pki_types::CertificateDer<'_>,
            _intermediates: &[rustls::pki_types::CertificateDer<'_>],
            _server_name: &rustls::pki_types::ServerName<'_>,
            _ocsp_response: &[u8],
            _now: rustls::pki_types::UnixTime,
        ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
            let pin = fingerprint(end_entity);
            if self.expected.contains(&pin) {
                Ok(rustls::client::danger::ServerCertVerified::assertion())
            } else {
                if let Some(report) = &self.report {
                    report.note_unknown();
                }
                Err(rustls::Error::General(
                    "the node's certificate does not match its published pin".into(),
                ))
            }
        }

        fn verify_tls12_signature(
            &self,
            _message: &[u8],
            _cert: &rustls::pki_types::CertificateDer<'_>,
            _dss: &rustls::DigitallySignedStruct,
        ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
            Err(rustls::Error::General(
                "TLS 1.2 is not used between nodes".into(),
            ))
        }

        fn verify_tls13_signature(
            &self,
            message: &[u8],
            cert: &rustls::pki_types::CertificateDer<'_>,
            dss: &rustls::DigitallySignedStruct,
        ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
            verify_signature(message, cert, dss)
        }

        fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
            schemes()
        }
    }

    /// The cluster listener's TLS: 1.3 only, ALPN `http/1.1`, a live node's
    /// certificate required, and no session resumption (a resumed session
    /// would skip the client-certificate check: ADR 0011).
    pub(crate) fn server_config(
        identity: &Identity,
        pins: Arc<PinSet>,
    ) -> Result<Arc<rustls::ServerConfig>, String> {
        let mut config = rustls::ServerConfig::builder_with_provider(provider())
            .with_protocol_versions(&[&rustls::version::TLS13])
            .map_err(|e| e.to_string())?
            .with_client_cert_verifier(Arc::new(PinnedClientVerifier { pins }))
            .with_single_cert(identity.chain(), identity.key())
            .map_err(|e| e.to_string())?;
        config.alpn_protocols = vec![b"http/1.1".to_vec()];
        config.send_tls13_tickets = 0;
        config.session_storage = Arc::new(rustls::server::NoServerSessionStorage {});
        Ok(Arc::new(config))
    }

    /// TLS for connecting to one node, whose certificate must match one of
    /// `expected` (the pins it published). TLS 1.3 only, ALPN `http/1.1`, no
    /// resumption.
    #[cfg(test)]
    pub(crate) fn client_config(
        identity: &Identity,
        expected: Vec<Pin32>,
    ) -> Result<Arc<rustls::ClientConfig>, String> {
        client_config_reporting(identity, expected, None)
    }

    /// [`client_config`], telling `report` when the server's certificate
    /// matches none of `expected`.
    pub(crate) fn client_config_reporting(
        identity: &Identity,
        expected: Vec<Pin32>,
        report: Option<Arc<PinSet>>,
    ) -> Result<Arc<rustls::ClientConfig>, String> {
        let mut config = rustls::ClientConfig::builder_with_provider(provider())
            .with_protocol_versions(&[&rustls::version::TLS13])
            .map_err(|e| e.to_string())?
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(PinnedServerVerifier { expected, report }))
            .with_client_auth_cert(identity.chain(), identity.key())
            .map_err(|e| e.to_string())?;
        config.alpn_protocols = vec![b"http/1.1".to_vec()];
        config.resumption = rustls::client::Resumption::disabled();
        Ok(Arc::new(config))
    }

    #[cfg(test)]
    mod tls_tests {
        use super::*;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        /// One handshake over an in-memory pipe, then a byte each way. `Ok`
        /// with the negotiated ALPN if both sides accepted.
        async fn handshake(
            client: Arc<rustls::ClientConfig>,
            server: Arc<rustls::ServerConfig>,
        ) -> Result<Option<Vec<u8>>, String> {
            let (c, s) = tokio::io::duplex(64 * 1024);
            let acceptor = tokio_rustls::TlsAcceptor::from(server);
            let connector = tokio_rustls::TlsConnector::from(client);
            let name = rustls::pki_types::ServerName::try_from("node.test").unwrap();
            let server = tokio::spawn(async move {
                let mut tls = acceptor.accept(s).await.map_err(|e| e.to_string())?;
                let mut b = [0u8; 1];
                tls.read_exact(&mut b).await.map_err(|e| e.to_string())?;
                tls.write_all(&b).await.map_err(|e| e.to_string())?;
                tls.flush().await.map_err(|e| e.to_string())?;
                Ok::<_, String>(())
            });
            let client = async {
                let mut tls = connector
                    .connect(name, c)
                    .await
                    .map_err(|e| e.to_string())?;
                tls.write_all(b"x").await.map_err(|e| e.to_string())?;
                tls.flush().await.map_err(|e| e.to_string())?;
                let mut b = [0u8; 1];
                tls.read_exact(&mut b).await.map_err(|e| e.to_string())?;
                Ok::<_, String>(tls.get_ref().1.alpn_protocol().map(<[u8]>::to_vec))
            };
            let client = client.await;
            let server = server.await.map_err(|e| e.to_string())?;
            let alpn = client?;
            server?;
            Ok(alpn)
        }

        fn pins_of(nodes: &[(&str, &Identity)]) -> Arc<PinSet> {
            let set = Arc::new(PinSet::default());
            let owned: Vec<(String, Vec<Pin32>)> = nodes
                .iter()
                .map(|(id, i)| ((*id).to_owned(), vec![i.pin()]))
                .collect();
            set.replace(
                owned
                    .iter()
                    .map(|(id, p)| (id.as_str(), p.as_slice(), u64::MAX)),
            );
            set
        }

        #[tokio::test]
        async fn live_nodes_reach_each_other() {
            let a = Identity::generate("a", "a.test").unwrap();
            let b = Identity::generate("b", "b.test").unwrap();
            let server = server_config(&b, pins_of(&[("a", &a), ("b", &b)])).unwrap();
            let client = client_config(&a, vec![b.pin()]).unwrap();
            assert_eq!(
                handshake(client, server).await,
                Ok(Some(b"http/1.1".to_vec()))
            );
            assert_eq!(
                server_config(&b, Arc::default())
                    .unwrap()
                    .send_tls13_tickets,
                0
            );
        }

        /// Over TLS 1.3 a client's side of the handshake is done before
        /// the server checks its certificate: a refused client may write
        /// its request before it reads the server's alert (over TCP, it
        /// usually does). hyper then keeps the request, and the error is a
        /// send error it does not retry; if the alert came first, hyper
        /// hands the request back. Either way
        /// [`super::super::handshake_refused`] tells it was never read.
        /// (Which order this run takes depends on scheduling.)
        #[tokio::test]
        async fn a_refused_handshake_is_recognised_in_the_error() {
            let a = Identity::generate("a", "a.test").unwrap();
            let b = Identity::generate("b", "b.test").unwrap();
            // b trusts nobody.
            let server = server_config(&b, Arc::default()).unwrap();
            let client = client_config(&a, vec![b.pin()]).unwrap();
            let (c, s) = tokio::io::duplex(64 * 1024);
            let acceptor = tokio_rustls::TlsAcceptor::from(server);
            let server = tokio::spawn(async move { acceptor.accept(s).await.map(|_| ()) });
            let name = rustls::pki_types::ServerName::try_from("node.test").unwrap();
            let tls = tokio_rustls::TlsConnector::from(client)
                .connect(name, c)
                .await
                .expect("the client's side of the handshake completes");
            let (mut send, conn) = hyper::client::conn::http1::handshake::<
                _,
                http_body_util::Empty<bytes::Bytes>,
            >(hyper_util::rt::TokioIo::new(tls))
            .await
            .unwrap();
            tokio::spawn(conn);
            let req = hyper::Request::post("/")
                .header(hyper::header::HOST, "node.test")
                .body(http_body_util::Empty::new())
                .unwrap();
            let err = send.try_send_request(req).await.unwrap_err();
            assert!(super::super::handshake_refused(&err.into_error()));
            assert!(server.await.unwrap().is_err());
        }

        #[test]
        fn only_a_refused_handshake_counts_as_refused() {
            let alert = |a| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    rustls::Error::AlertReceived(a),
                )
            };
            assert!(super::super::handshake_refused(&alert(
                rustls::AlertDescription::HandshakeFailure
            )));
            assert!(super::super::handshake_refused(&alert(
                rustls::AlertDescription::CertificateUnknown
            )));
            // A record that failed to decrypt may have come after a request
            // the server read; an ended connection says nothing either.
            assert!(!super::super::handshake_refused(&alert(
                rustls::AlertDescription::BadRecordMac
            )));
            assert!(!super::super::handshake_refused(&std::io::Error::from(
                std::io::ErrorKind::UnexpectedEof
            )));
        }

        #[tokio::test]
        async fn an_unknown_client_is_refused() {
            let a = Identity::generate("a", "a.test").unwrap();
            let b = Identity::generate("b", "b.test").unwrap();
            let stranger = Identity::generate("a", "a.test").unwrap();
            // b knows a's pin, not the stranger's (same name, other key).
            let server = server_config(&b, pins_of(&[("a", &a)])).unwrap();
            let client = client_config(&stranger, vec![b.pin()]).unwrap();
            assert!(handshake(client, server).await.is_err());
            // No pins at all: nobody gets in.
            let server = server_config(&b, Arc::default()).unwrap();
            let client = client_config(&a, vec![b.pin()]).unwrap();
            assert!(handshake(client, server).await.is_err());
        }

        #[tokio::test]
        async fn a_server_must_match_the_pin_its_node_published() {
            let a = Identity::generate("a", "a.test").unwrap();
            let b = Identity::generate("b", "b.test").unwrap();
            let c = Identity::generate("c", "c.test").unwrap();
            // a dials b's address but c answers (with a valid, live identity of
            // its own): refused.
            let server = server_config(&c, pins_of(&[("a", &a)])).unwrap();
            let client = client_config(&a, vec![b.pin()]).unwrap();
            assert!(handshake(client, server).await.is_err());
            // Expecting nothing: refused too.
            let server = server_config(&b, pins_of(&[("a", &a)])).unwrap();
            let client = client_config(&a, Vec::new()).unwrap();
            assert!(handshake(client, server).await.is_err());
        }

        /// Presents a fixed certificate with a fixed key, as a TLS stack of
        /// its own would: rustls' check that they match is skipped.
        #[derive(Debug)]
        struct Forged(Arc<rustls::sign::CertifiedKey>);

        impl rustls::client::ResolvesClientCert for Forged {
            fn resolve(
                &self,
                _hints: &[&[u8]],
                _schemes: &[rustls::SignatureScheme],
            ) -> Option<Arc<rustls::sign::CertifiedKey>> {
                Some(Arc::clone(&self.0))
            }

            fn has_certs(&self) -> bool {
                true
            }
        }

        impl rustls::server::ResolvesServerCert for Forged {
            fn resolve(
                &self,
                _hello: rustls::server::ClientHello<'_>,
            ) -> Option<Arc<rustls::sign::CertifiedKey>> {
                Some(Arc::clone(&self.0))
            }
        }

        /// `owner`'s public certificate with `thief`'s private key.
        fn forged(owner: &Identity, thief: &Identity) -> Arc<Forged> {
            let key = provider()
                .key_provider
                .load_private_key(thief.key())
                .unwrap();
            Arc::new(Forged(Arc::new(rustls::sign::CertifiedKey::new(
                owner.chain(),
                key,
            ))))
        }

        #[tokio::test]
        async fn a_copied_certificate_without_its_key_is_refused() {
            let a = Identity::generate("a", "a.test").unwrap();
            let b = Identity::generate("b", "b.test").unwrap();
            let thief = Identity::generate("x", "x.test").unwrap();
            // As a client: a's certificate (a live pin), the thief's key.
            let server = server_config(&b, pins_of(&[("a", &a)])).unwrap();
            let mut client = rustls::ClientConfig::builder_with_provider(provider())
                .with_protocol_versions(&[&rustls::version::TLS13])
                .unwrap()
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(PinnedServerVerifier {
                    expected: vec![b.pin()],
                    report: None,
                }))
                .with_client_cert_resolver(forged(&a, &thief));
            client.alpn_protocols = vec![b"http/1.1".to_vec()];
            let err = handshake(Arc::new(client), server).await.unwrap_err();
            assert!(!err.is_empty());
            // As a server: b's certificate (the pin a expects), the thief's key.
            let mut server = rustls::ServerConfig::builder_with_provider(provider())
                .with_protocol_versions(&[&rustls::version::TLS13])
                .unwrap()
                .with_client_cert_verifier(Arc::new(PinnedClientVerifier {
                    pins: pins_of(&[("a", &a)]),
                }))
                .with_cert_resolver(forged(&b, &thief));
            server.alpn_protocols = vec![b"http/1.1".to_vec()];
            let client = client_config(&a, vec![b.pin()]).unwrap();
            assert!(handshake(client, Arc::new(server)).await.is_err());
            // The same server with b's own key is accepted.
            let mut server = rustls::ServerConfig::builder_with_provider(provider())
                .with_protocol_versions(&[&rustls::version::TLS13])
                .unwrap()
                .with_client_cert_verifier(Arc::new(PinnedClientVerifier {
                    pins: pins_of(&[("a", &a)]),
                }))
                .with_cert_resolver(forged(&b, &b));
            server.alpn_protocols = vec![b"http/1.1".to_vec()];
            let client = client_config(&a, vec![b.pin()]).unwrap();
            assert!(handshake(client, Arc::new(server)).await.is_ok());
            // The same setups with the real keys work: the key was the
            // only thing wrong.
            let honest = forged(&a, &a);
            let mut client = rustls::ClientConfig::builder_with_provider(provider())
                .with_protocol_versions(&[&rustls::version::TLS13])
                .unwrap()
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(PinnedServerVerifier {
                    expected: vec![b.pin()],
                    report: None,
                }))
                .with_client_cert_resolver(honest);
            client.alpn_protocols = vec![b"http/1.1".to_vec()];
            let server = server_config(&b, pins_of(&[("a", &a)])).unwrap();
            assert!(handshake(Arc::new(client), server).await.is_ok());
        }

        #[tokio::test]
        async fn only_http1_is_offered() {
            let a = Identity::generate("a", "a.test").unwrap();
            let b = Identity::generate("b", "b.test").unwrap();
            let server = server_config(&b, pins_of(&[("a", &a)])).unwrap();
            let mut client = (*client_config(&a, vec![b.pin()]).unwrap()).clone();
            client.alpn_protocols = vec![b"h2".to_vec()];
            assert!(handshake(Arc::new(client), server).await.is_err());
        }

        #[tokio::test]
        async fn every_connection_is_verified_again() {
            // A client that would resume if it could: the server sends no
            // tickets and keeps no sessions, so the second connection is a full
            // handshake, and a pin removed in between is refused.
            let a = Identity::generate("a", "a.test").unwrap();
            let b = Identity::generate("b", "b.test").unwrap();
            let pins = pins_of(&[("a", &a)]);
            let server = server_config(&b, Arc::clone(&pins)).unwrap();
            let mut resuming = (*client_config(&a, vec![b.pin()]).unwrap()).clone();
            resuming.resumption = rustls::client::Resumption::in_memory_sessions(16);
            let resuming = Arc::new(resuming);
            assert!(
                handshake(Arc::clone(&resuming), Arc::clone(&server))
                    .await
                    .is_ok()
            );
            pins.replace(std::iter::empty());
            assert!(handshake(resuming, server).await.is_err());
        }
    }
}
