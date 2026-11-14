//! Outbound HTTP from guests, restricted to public addresses.
//!
//! Actors run code the operator may not have written (other teams, other
//! tenants). Without a policy, `wasi:http/outgoing-handler` reaches any
//! address the node can: `localhost`, the private network, other nodes'
//! plain-HTTP ports, and cloud metadata endpoints (`169.254.169.254`) that
//! hand out credentials. That is server-side request forgery.
//!
//! So the host resolves the name itself, keeps only the addresses the
//! [`EgressPolicy`] allows (public ones, plus ranges the operator allows
//! explicitly), and connects to *that* address, with TLS verified against
//! the original host name. Checking the name and then letting the connector
//! resolve it again would let a name that changes its answer in between
//! ("DNS rebinding") through.
//!
//! The sender follows `wasmtime_wasi_http::default_send_request` (Apache-2.0)
//! for everything after the connection.

use std::future::{Future, poll_fn};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::pin::{Pin, pin};
use std::sync::Arc;
use std::task::{Context, Poll, ready};
use std::time::Duration;

use ::http::uri::Scheme;
use bytes::Bytes;
use http_body_util::BodyExt;
use hyper_util::rt::TokioIo;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use wasmtime_wasi_http::{Error, RequestOptions, WasiBody, WasiHttpHooks};

/// Wait for a step the guest set no timeout for.
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);
/// Longest wait a guest may ask for, per step.
const MAX_TIMEOUT: Duration = Duration::from_secs(60);
/// Outbound requests one actor (one store) may have open at once.
pub const PER_ACTOR_REQUESTS: usize = 16;
/// Outbound requests the whole node may have open at once.
pub const PER_NODE_REQUESTS: usize = 256;
/// Name lookups in flight on the node. Lookups run on the blocking thread
/// pool and cannot be cancelled; bounding them keeps a slow name server
/// from starving the pool other work needs (opening databases).
const PER_NODE_LOOKUPS: usize = 32;
/// Addresses tried per request.
const MAX_ADDRESSES: usize = 4;

/// Ports no outbound request may use unless the operator allows them: the
/// Fetch standard's "bad ports" (fetch.spec.whatwg.org, "port blocking"),
/// which browsers and Cloudflare Workers refuse too. They belong to
/// protocols other than HTTP (mail, FTP, SSH, IRC, name and file
/// services); a crafted HTTP request can drive some of those servers, as
/// in sending mail through an SMTP relay.
pub const BAD_PORTS: &[u16] = &[
    0, 1, 7, 9, 11, 13, 15, 17, 19, 20, 21, 22, 23, 25, 37, 42, 43, 53, 69, 77, 79, 87, 95, 101,
    102, 103, 104, 109, 110, 111, 113, 115, 117, 119, 123, 135, 137, 139, 143, 161, 179, 389, 427,
    465, 512, 513, 514, 515, 526, 530, 531, 532, 540, 548, 554, 556, 563, 587, 601, 636, 989, 990,
    993, 995, 1719, 1720, 1723, 2049, 3659, 4045, 4190, 5060, 5061, 6000, 6566, 6665, 6666, 6667,
    6668, 6669, 6679, 6697, 10080,
];

/// An IP range, `address/prefix`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IpRange {
    addr: IpAddr,
    prefix: u8,
}

impl IpRange {
    fn contains(&self, ip: IpAddr) -> bool {
        match (self.addr, normalize(ip)) {
            (IpAddr::V4(net), IpAddr::V4(ip)) => {
                let bits = u32::from(self.prefix.min(32));
                let mask = if bits == 0 {
                    0
                } else {
                    u32::MAX << (32 - bits)
                };
                u32::from(net) & mask == u32::from(ip) & mask
            }
            (IpAddr::V6(net), IpAddr::V6(ip)) => {
                let bits = u32::from(self.prefix.min(128));
                let mask = if bits == 0 {
                    0
                } else {
                    u128::MAX << (128 - bits)
                };
                u128::from(net) & mask == u128::from(ip) & mask
            }
            _ => false,
        }
    }
}

impl std::str::FromStr for IpRange {
    type Err = String;

    /// `10.0.0.0/8`, `fd00::/8`, or a single address (`127.0.0.1`).
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (addr, prefix) = match s.split_once('/') {
            Some((a, p)) => (a, Some(p)),
            None => (s, None),
        };
        let addr: IpAddr = addr
            .parse()
            .map_err(|_| format!("{s}: not an IP address or range"))?;
        let max = if addr.is_ipv4() { 32 } else { 128 };
        let prefix = match prefix {
            Some(p) => p
                .parse::<u8>()
                .ok()
                .filter(|p| *p <= max)
                .ok_or_else(|| format!("{s}: bad prefix length"))?,
            None => max,
        };
        Ok(Self { addr, prefix })
    }
}

impl std::fmt::Display for IpRange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.addr, self.prefix)
    }
}

/// Which addresses guests' outbound HTTP may connect to: every public
/// address, plus the `allow` ranges. Everything else (loopback, private,
/// link-local, shared, documentation, multicast, reserved; IPv6 included,
/// and IPv4 addresses embedded in IPv6) is refused.
#[derive(Debug, Clone)]
pub struct EgressPolicy {
    /// Non-public ranges guests may reach anyway (a private API, a local
    /// test server).
    pub allow: Vec<IpRange>,
    /// Ports in [`BAD_PORTS`] guests may reach anyway (a mail relay).
    allow_ports: Vec<u16>,
    /// Node-wide budgets, shared by every actor using this policy.
    limits: Arc<NodeLimits>,
    /// Certificate authorities trusted besides the public ones (a private
    /// CA for an internal API).
    extra_roots: Vec<rustls::pki_types::CertificateDer<'static>>,
    /// The TLS client configuration, built once on first use: the root
    /// store alone is about 150 certificates.
    tls: Arc<std::sync::OnceLock<Result<Arc<rustls::ClientConfig>, String>>>,
}

impl Default for EgressPolicy {
    fn default() -> Self {
        Self::allowing(Vec::new())
    }
}

#[derive(Debug)]
struct NodeLimits {
    requests: Arc<tokio::sync::Semaphore>,
    lookups: Arc<tokio::sync::Semaphore>,
}

impl EgressPolicy {
    /// Public addresses plus `allow`.
    pub fn allowing(allow: Vec<IpRange>) -> Self {
        Self {
            allow,
            allow_ports: Vec::new(),
            limits: Arc::new(NodeLimits {
                requests: Arc::new(tokio::sync::Semaphore::new(PER_NODE_REQUESTS)),
                lookups: Arc::new(tokio::sync::Semaphore::new(PER_NODE_LOOKUPS)),
            }),
            extra_roots: Vec::new(),
            tls: Arc::default(),
        }
    }

    /// Also trusts these certificate authorities for HTTPS (DER), besides
    /// the public ones: for an internal API behind a private CA.
    #[must_use]
    pub fn trusting(mut self, roots: Vec<rustls::pki_types::CertificateDer<'static>>) -> Self {
        self.extra_roots.extend(roots);
        self.tls = Arc::default();
        self
    }

    /// The TLS client configuration: public roots plus `extra_roots`.
    fn tls_config(&self) -> Result<Arc<rustls::ClientConfig>, Error> {
        self.tls
            .get_or_init(|| {
                let mut roots = rustls::RootCertStore {
                    roots: webpki_roots::TLS_SERVER_ROOTS.into(),
                };
                for cert in &self.extra_roots {
                    roots.add(cert.clone()).map_err(|e| e.to_string())?;
                }
                let config = rustls::ClientConfig::builder_with_provider(Arc::new(
                    rustls::crypto::ring::default_provider(),
                ))
                .with_safe_default_protocol_versions()
                .map_err(|e| e.to_string())?
                .with_root_certificates(roots)
                .with_no_client_auth();
                Ok(Arc::new(config))
            })
            .clone()
            .map_err(|e| Error::Tls(std::io::Error::other(e)))
    }

    /// Also allows these ports, which [`BAD_PORTS`] would refuse.
    #[must_use]
    pub fn allowing_ports(mut self, ports: Vec<u16>) -> Self {
        self.allow_ports.extend(ports);
        self
    }

    /// Whether a request to `port` is allowed: any port but the
    /// [`BAD_PORTS`], unless the operator allowed it.
    pub fn permits_port(&self, port: u16) -> bool {
        !BAD_PORTS.contains(&port) || self.allow_ports.contains(&port)
    }

    /// Whether a connection to `ip` is allowed.
    pub fn permits(&self, ip: IpAddr) -> bool {
        is_public(ip) || self.allow.iter().any(|r| r.contains(ip))
    }
}

/// Reads certificate authorities (PEM, one or more `CERTIFICATE` blocks)
/// for [`EgressPolicy::trusting`].
///
/// # Errors
/// The file cannot be read, or holds no certificate.
pub fn read_ca_file(
    path: &std::path::Path,
) -> Result<Vec<rustls::pki_types::CertificateDer<'static>>, String> {
    use rustls::pki_types::pem::PemObject as _;
    let shown = path.display();
    let certs = rustls::pki_types::CertificateDer::pem_file_iter(path)
        .map_err(|e| format!("{shown}: {e}"))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("{shown}: {e}"))?;
    if certs.is_empty() {
        return Err(format!("{shown}: no PEM CERTIFICATE in it"));
    }
    Ok(certs)
}

/// An IPv4 address carried in an IPv6 one (mapped, `::ffff:a.b.c.d`, or
/// NAT64, `64:ff9b::a.b.c.d`) is judged as that IPv4 address.
fn normalize(ip: IpAddr) -> IpAddr {
    match ip {
        IpAddr::V6(v6) => {
            if let Some(v4) = v6.to_ipv4_mapped() {
                return IpAddr::V4(v4);
            }
            let s = v6.segments();
            if s[..6] == [0x64, 0xff9b, 0, 0, 0, 0] {
                let [a, b] = s[6].to_be_bytes();
                let [c, d] = s[7].to_be_bytes();
                return IpAddr::V4(Ipv4Addr::new(a, b, c, d));
            }
            IpAddr::V6(v6)
        }
        v4 => v4,
    }
}

/// Whether `ip` is a globally routable unicast address (IANA special-purpose
/// registries, RFC 6890 and successors).
pub fn is_public(ip: IpAddr) -> bool {
    match normalize(ip) {
        IpAddr::V4(ip) => is_public_v4(ip),
        IpAddr::V6(ip) => is_public_v6(ip),
    }
}

fn is_public_v4(ip: Ipv4Addr) -> bool {
    let [a, b, c, _] = ip.octets();
    !(a == 0                                        // "this network"
        || a == 10                                  // private
        || a == 127                                 // loopback
        || (a == 100 && (64..=127).contains(&b))    // shared (carrier-grade NAT)
        || (a == 169 && b == 254)                   // link-local, cloud metadata
        || (a == 172 && (16..=31).contains(&b))     // private
        || (a == 192 && b == 0 && c == 0)           // IETF protocol assignments
        || (a == 192 && b == 0 && c == 2)           // documentation
        || (a == 192 && b == 88 && c == 99)         // 6to4 relay anycast
        || (a == 192 && b == 168)                   // private
        || (a == 198 && (b == 18 || b == 19))       // benchmarking
        || (a == 198 && b == 51 && c == 100)        // documentation
        || (a == 203 && b == 0 && c == 113)         // documentation
        || a >= 224) // multicast, reserved, broadcast
}

/// An allow-list: only global unicast (`2000::/3`), minus the special
/// ranges inside it. Everything else (loopback, unique-local, link-local,
/// multicast, IPv4-embedding forms such as SIIT `::ffff:0:0/96` or local
/// NAT64 `64:ff9b:1::/48`, and space not yet allocated) is refused.
fn is_public_v6(ip: Ipv6Addr) -> bool {
    let s = ip.segments();
    (s[0] & 0xe000) == 0x2000                       // global unicast
        && !(s[0] == 0x2001 && s[1] < 0x0200        // IETF protocol assignments (Teredo, ...)
            || (s[0] == 0x2001 && s[1] == 0x0db8)   // documentation
            || s[0] == 0x2002                       // 6to4 (embeds any IPv4)
            || (s[0] & 0xfff0) == 0x3ff0) // documentation (3fff::/20)
}

/// The WASI context every guest gets: no arguments, environment, stdio or
/// preopened directories (so `wasi:filesystem` reaches nothing), and every
/// `wasi:sockets` use off with all addresses refused. Toolchains import
/// these interfaces whether or not the code uses them (the WASI 0.1
/// adapter, ComponentizeJS, componentize-py), so they are linked, and inert.
/// Outbound HTTP is the only way out, under the egress policy.
/// Their stdout and stderr are discarded here; [`crate::GuestLog`] logs
/// them.
pub(crate) fn guest_wasi() -> wasmtime_wasi::WasiCtx {
    guest_wasi_builder().build()
}

/// [`guest_wasi`] before it is built.
pub(crate) fn guest_wasi_builder() -> wasmtime_wasi::WasiCtxBuilder {
    let mut builder = wasmtime_wasi::WasiCtx::builder();
    builder
        .allow_tcp(false)
        .allow_udp(false)
        .allow_ip_name_lookup(false)
        .socket_addr_check(|_, _| Box::pin(async { false }));
    builder
}

/// `wasi:http` hooks that send guests' outbound requests under a policy.
/// One per store (actor instance): it carries that store's request budget.
#[derive(Debug, Clone)]
pub struct EgressHooks {
    policy: Arc<EgressPolicy>,
    requests: Arc<tokio::sync::Semaphore>,
    /// Also forbid the host's `x-wasmkeep-` range at the guest boundary,
    /// headers and trailers both ways (the front, ADR 0023).
    forbid_wasmkeep: bool,
}

impl Default for EgressHooks {
    fn default() -> Self {
        Self::new(Arc::new(EgressPolicy::default()))
    }
}

impl EgressHooks {
    /// Hooks enforcing `policy`, with a fresh per-store budget.
    pub fn new(policy: Arc<EgressPolicy>) -> Self {
        Self {
            policy,
            requests: Arc::new(tokio::sync::Semaphore::new(PER_ACTOR_REQUESTS)),
            forbid_wasmkeep: false,
        }
    }

    /// These hooks, with every header and trailer of the `x-wasmkeep-`
    /// range forbidden where fields cross the guest boundary, both ways:
    /// what the client sends never reaches the guest, and what the guest
    /// sets never leaves it (wasmtime-wasi-http applies the filter to
    /// headers and trailers alike).
    #[must_use]
    pub(crate) fn forbidding_wasmkeep_headers(mut self) -> Self {
        self.forbid_wasmkeep = true;
        self
    }
}

type SendResult = wasmtime_wasi_http::Result<(
    ::http::Response<WasiBody>,
    Box<dyn Future<Output = Result<(), Error>> + Send>,
)>;

impl WasiHttpHooks for EgressHooks {
    fn forbidden_header_filter(&mut self) -> wasmtime_wasi_http::ForbiddenHeaderFilter {
        static DEFAULT: std::sync::LazyLock<wasmtime_wasi_http::ForbiddenHeaderFilter> =
            std::sync::LazyLock::new(|| {
                Arc::new(|name| wasmtime_wasi_http::DEFAULT_FORBIDDEN_HEADERS.contains(name))
            });
        static WITH_RANGE: std::sync::LazyLock<wasmtime_wasi_http::ForbiddenHeaderFilter> =
            std::sync::LazyLock::new(|| {
                Arc::new(|name| {
                    wasmtime_wasi_http::DEFAULT_FORBIDDEN_HEADERS.contains(name)
                        || crate::actor::is_wasmkeep_header(name.as_str())
                })
            });
        if self.forbid_wasmkeep {
            WITH_RANGE.clone()
        } else {
            DEFAULT.clone()
        }
    }

    fn send_request(
        &mut self,
        request: ::http::Request<WasiBody>,
        options: Option<RequestOptions>,
        fut: Box<dyn Future<Output = Result<(), Error>> + Send>,
    ) -> Box<dyn Future<Output = SendResult> + Send> {
        drop(fut);
        let policy = Arc::clone(&self.policy);
        // Both budgets are taken now, or the request fails at once; they are
        // held until the connection is done (the response body read or
        // dropped), not just until the response head arrives.
        let permits = Arc::clone(&self.requests)
            .try_acquire_owned()
            .and_then(|mine| {
                let node = Arc::clone(&policy.limits.requests).try_acquire_owned()?;
                Ok((mine, node))
            });
        Box::new(async move {
            let Ok(permits) = permits else {
                tracing::debug!("outbound request refused: too many in flight");
                return Err(Error::ConnectionLimitReached);
            };
            let (response, io) = send(&policy, request, options).await?;
            let io = async move {
                let _permits = permits;
                io.await
            };
            Ok((
                response.map(BodyExt::boxed_unsync),
                Box::new(io) as Box<dyn Future<Output = Result<(), Error>> + Send>,
            ))
        })
    }
}

trait Stream: AsyncRead + AsyncWrite + Send + Sync + Unpin + 'static {}
impl<T: AsyncRead + AsyncWrite + Send + Sync + Unpin + 'static> Stream for T {}

fn dns_error() -> Error {
    Error::DnsError {
        rcode: None,
        info_code: None,
    }
}

/// Resolves `host` (bounded by the node's lookup budget), keeps the
/// addresses the policy allows, and connects to the first that answers,
/// all within `timeout`.
async fn connect(
    policy: &EgressPolicy,
    host: &str,
    port: u16,
    timeout: Duration,
) -> Result<TcpStream, Error> {
    let deadline = tokio::time::Instant::now() + timeout;
    let bare = host
        .trim_start_matches('[')
        .trim_end_matches(']')
        .to_owned();
    if let Ok(ip) = bare.parse::<IpAddr>() {
        // A literal address reveals nothing: say plainly why it fails.
        if !policy.permits(ip) {
            tracing::debug!(host, "outbound request refused: address not allowed");
            return Err(Error::DestinationIpProhibited);
        }
        return tokio::time::timeout_at(deadline, TcpStream::connect(SocketAddr::new(ip, port)))
            .await
            .map_err(|_| Error::ConnectionTimeout)?
            .map_err(Error::Connect);
    }
    // The lookup holds a node permit until its blocking thread finishes,
    // even if this request gives up waiting for it.
    let permit =
        tokio::time::timeout_at(deadline, Arc::clone(&policy.limits.lookups).acquire_owned())
            .await
            .map_err(|_| Error::ConnectionTimeout)?
            .map_err(|_| dns_error())?;
    let lookup = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        std::net::ToSocketAddrs::to_socket_addrs(&(bare.as_str(), port))
            .map(Iterator::collect::<Vec<_>>)
    });
    let addrs: Vec<SocketAddr> = tokio::time::timeout_at(deadline, lookup)
        .await
        .map_err(|_| Error::ConnectionTimeout)?
        .map_err(|_| dns_error())?
        .map_err(|_| dns_error())?;
    let allowed: Vec<SocketAddr> = addrs
        .iter()
        .copied()
        .filter(|a| policy.permits(a.ip()))
        .take(MAX_ADDRESSES)
        .collect();
    if allowed.is_empty() {
        // The same error as a name that does not resolve, so a guest
        // cannot use refusals to map internal names.
        tracing::debug!(host, addresses = ?addrs, "outbound request refused: no allowed address");
        return Err(dns_error());
    }
    let mut last = Error::ConnectionRefused;
    for addr in allowed {
        match tokio::time::timeout_at(deadline, TcpStream::connect(addr)).await {
            Ok(Ok(stream)) => return Ok(stream),
            Ok(Err(e)) => last = Error::Connect(e),
            Err(_) => return Err(Error::ConnectionTimeout),
        }
    }
    Err(last)
}

async fn send(
    policy: &EgressPolicy,
    mut req: ::http::Request<WasiBody>,
    options: Option<RequestOptions>,
) -> Result<
    (
        ::http::Response<ResponseBody>,
        impl Future<Output = Result<(), Error>> + Send + 'static,
    ),
    Error,
> {
    let uri = req.uri();
    let authority = uri.authority().ok_or(Error::HttpRequestUriInvalid)?;
    let use_tls = uri.scheme() == Some(&Scheme::HTTPS);
    let host = authority.host().to_owned();
    let port = authority
        .port_u16()
        .unwrap_or(if use_tls { 443 } else { 80 });
    // Guests choose their timeouts, up to a cap.
    let pick = |t: Option<Duration>| t.unwrap_or(DEFAULT_TIMEOUT).min(MAX_TIMEOUT);
    let connect_timeout = pick(options.and_then(|o| o.connect_timeout));
    let first_byte_timeout = pick(options.and_then(|o| o.first_byte_timeout));
    let between_bytes_timeout =
        pick(options.and_then(|o| o.between_bytes_timeout)).max(Duration::from_nanos(1));

    if !policy.permits_port(port) {
        tracing::debug!(host, port, "outbound request refused: port blocked");
        return Err(Error::HttpRequestDenied);
    }
    let tcp = connect(policy, &host, port, connect_timeout).await?;
    let stream: Box<dyn Stream> = if use_tls {
        let connector = tokio_rustls::TlsConnector::from(policy.tls_config()?);
        let bare = host.trim_start_matches('[').trim_end_matches(']');
        let name = match bare.parse::<IpAddr>() {
            Ok(ip) => rustls::pki_types::ServerName::from(ip),
            Err(_) => rustls::pki_types::ServerName::try_from(bare.to_owned())?,
        };
        let tls = tokio::time::timeout(connect_timeout, connector.connect(name, tcp))
            .await
            .map_err(|_| Error::ConnectionTimeout)?
            .map_err(Error::Tls)?;
        Box::new(tls)
    } else {
        Box::new(tcp)
    };
    let (mut sender, conn) = tokio::time::timeout(
        connect_timeout,
        hyper::client::conn::http1::Builder::new().handshake(TokioIo::new(stream)),
    )
    .await
    .map_err(|_| Error::ConnectionTimeout)??;

    // Origin form on the wire: the scheme and authority only go to proxies.
    *req.uri_mut() = ::http::Uri::builder()
        .path_and_query(req.uri().path_and_query().map_or("/", |p| p.as_str()))
        .build()
        .map_err(|_| Error::HttpRequestUriInvalid)?;

    let send = async move {
        let res = tokio::time::timeout(first_byte_timeout, sender.send_request(req))
            .await
            .map_err(|_| Error::ConnectionReadTimeout)?
            .map_err(Error::from)?;
        let mut timeout = tokio::time::interval(between_bytes_timeout);
        timeout.reset();
        Ok::<_, Error>(res.map(|incoming| ResponseBody { incoming, timeout }))
    };
    let mut send = pin!(send);
    let mut conn = Some(conn);
    // Wait for the response while driving the connection's I/O.
    let response = poll_fn(|cx| match send.as_mut().poll(cx) {
        Poll::Ready(result) => Poll::Ready(result),
        Poll::Pending => {
            let Some(fut) = conn.as_mut() else {
                return Poll::Pending;
            };
            let result = ready!(Pin::new(fut).poll(cx));
            conn = None;
            match result {
                Ok(()) => send.as_mut().poll(cx),
                Err(e) => Poll::Ready(Err(Error::from(e))),
            }
        }
    })
    .await?;
    Ok((response, async move {
        let Some(conn) = conn.take() else {
            return Ok(());
        };
        conn.await.map_err(|e| {
            if e.is_timeout() {
                Error::HttpResponseTimeout
            } else {
                Error::from(e)
            }
        })
    }))
}

/// A response body that fails when the server pauses longer than the
/// between-bytes timeout.
struct ResponseBody {
    incoming: hyper::body::Incoming,
    timeout: tokio::time::Interval,
}

impl http_body::Body for ResponseBody {
    type Data = Bytes;
    type Error = Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Bytes>, Error>>> {
        match Pin::new(&mut self.incoming).poll_frame(cx) {
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(if e.is_timeout() {
                Error::HttpResponseTimeout
            } else {
                Error::from(e)
            }))),
            Poll::Ready(Some(Ok(frame))) => {
                self.timeout.reset();
                Poll::Ready(Some(Ok(frame)))
            }
            Poll::Pending => {
                ready!(self.timeout.poll_tick(cx));
                Poll::Ready(Some(Err(Error::ConnectionReadTimeout)))
            }
        }
    }

    fn is_end_stream(&self) -> bool {
        self.incoming.is_end_stream()
    }

    fn size_hint(&self) -> http_body::SizeHint {
        self.incoming.size_hint()
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)] // test code

    use super::*;

    fn ip(s: &str) -> IpAddr {
        s.parse().unwrap()
    }

    #[test]
    fn private_and_special_addresses_are_refused() {
        for s in [
            "0.0.0.0",
            "10.1.2.3",
            "127.0.0.1",
            "127.255.255.254",
            "100.64.0.1",
            "169.254.169.254",
            "172.16.0.1",
            "172.31.255.255",
            "192.0.0.8",
            "192.0.2.1",
            "192.168.1.1",
            "198.18.0.1",
            "198.51.100.7",
            "203.0.113.9",
            "224.0.0.1",
            "240.0.0.1",
            "255.255.255.255",
            "::",
            "::1",
            "fc00::1",
            "fd12:3456::1",
            "fe80::1",
            "fec0::1",
            "ff02::1",
            "2001:db8::1",
            "100::1",
            "2002:a00:1::",
            // IPv4 hidden in IPv6 is judged as IPv4.
            "::ffff:127.0.0.1",
            "::ffff:169.254.169.254",
            "64:ff9b::a9fe:a9fe",
            "::127.0.0.1",
            // IPv6 outside global unicast, or forms carrying IPv4 that are
            // not decoded: refused by construction.
            "64:ff9b:1::a9fe:a9fe",
            "::ffff:0:a9fe:a9fe",
            "::ffff:0:7f00:1",
            "::1:0:0:1",
            "4000::1",
            "5f00::1",
            "3fff::1",
            "2001:0:4136:e378::1",
        ] {
            assert!(!is_public(ip(s)), "{s} must be refused");
        }
    }

    #[test]
    fn public_addresses_are_allowed() {
        for s in [
            "1.1.1.1",
            "8.8.8.8",
            "93.184.216.34",
            "100.63.255.255",
            "100.128.0.1",
            "172.15.255.255",
            "172.32.0.1",
            "192.169.0.1",
            "2606:4700::1111",
            "2a00:1450:4001::200e",
            "::ffff:8.8.8.8",
            "64:ff9b::808:808",
        ] {
            assert!(is_public(ip(s)), "{s} must be allowed");
        }
    }

    #[test]
    fn allowed_ranges_open_specific_private_addresses() {
        let policy = EgressPolicy::allowing(vec![
            "10.0.0.0/8".parse().unwrap(),
            "127.0.0.1".parse().unwrap(),
            "fd00::/8".parse().unwrap(),
        ]);
        assert!(policy.permits(ip("10.9.8.7")));
        assert!(policy.permits(ip("127.0.0.1")));
        assert!(policy.permits(ip("::ffff:127.0.0.1")));
        assert!(!policy.permits(ip("127.0.0.2")));
        assert!(policy.permits(ip("fd12::1")));
        assert!(!policy.permits(ip("169.254.169.254")));
        assert!(policy.permits(ip("8.8.8.8")));
    }

    /// A request to `url` through hooks for `policy`; its error, if any
    /// (none listens on 127.0.0.1:25, :6667 or :9999 here, so a request let
    /// through fails to connect instead).
    async fn send_to(policy: EgressPolicy, url: &str) -> Option<Error> {
        let mut hooks = EgressHooks::new(Arc::new(policy));
        let req = ::http::Request::builder()
            .uri(url)
            .body(
                http_body_util::Empty::<Bytes>::new()
                    .map_err(|never| match never {})
                    .boxed_unsync(),
            )
            .unwrap();
        Pin::from(hooks.send_request(req, None, Box::new(async { Ok(()) })))
            .await
            .err()
    }

    #[tokio::test]
    async fn requests_to_ports_browsers_block_are_denied() {
        let loopback = || EgressPolicy::allowing(vec!["127.0.0.1".parse().unwrap()]);
        // SMTP and IRC: a crafted HTTP request can drive such servers.
        for url in ["http://127.0.0.1:25/", "https://127.0.0.1:6667/"] {
            assert!(
                matches!(
                    send_to(loopback(), url).await,
                    Some(Error::HttpRequestDenied)
                ),
                "{url}"
            );
        }
        // An ordinary port is only refused by the (absent) server.
        assert!(!matches!(
            send_to(loopback(), "http://127.0.0.1:9999/").await,
            Some(Error::HttpRequestDenied)
        ));
        // The operator can open one.
        let open = loopback().allowing_ports(vec![25]);
        assert!(!matches!(
            send_to(open, "http://127.0.0.1:25/").await,
            Some(Error::HttpRequestDenied)
        ));
    }

    #[test]
    fn the_bad_port_list_is_the_fetch_standards() {
        for port in [25, 465, 587, 21, 22, 23, 110, 143, 6667, 10080] {
            assert!(!EgressPolicy::default().permits_port(port), "{port}");
        }
        for port in [80, 443, 8080, 8443, 3000, 5432] {
            assert!(EgressPolicy::default().permits_port(port), "{port}");
        }
    }

    #[tokio::test]
    async fn one_actor_cannot_hold_more_than_its_share_of_requests() {
        // Budgets are taken when a request starts, so requests a guest
        // starts and never awaits count too.
        let mut hooks = EgressHooks::new(Arc::new(EgressPolicy::allowing(vec![
            "127.0.0.1".parse().unwrap(),
        ])));
        let request = || {
            ::http::Request::builder()
                .uri("http://127.0.0.1:9999/")
                .body(
                    http_body_util::Empty::<Bytes>::new()
                        .map_err(|never| match never {})
                        .boxed_unsync(),
                )
                .unwrap()
        };
        let held: Vec<_> = (0..PER_ACTOR_REQUESTS)
            .map(|_| hooks.send_request(request(), None, Box::new(async { Ok(()) })))
            .collect();
        let extra = Pin::from(hooks.send_request(request(), None, Box::new(async { Ok(()) })));
        assert!(matches!(extra.await, Err(Error::ConnectionLimitReached)));
        // Another actor (another store) has its own budget.
        let mut other = EgressHooks::new(Arc::clone(&hooks.policy));
        let fresh = Pin::from(other.send_request(request(), None, Box::new(async { Ok(()) })));
        assert!(!matches!(fresh.await, Err(Error::ConnectionLimitReached)));
        drop(held);
    }

    #[test]
    fn ranges_parse_and_reject_garbage() {
        assert_eq!(
            "10.0.0.0/8".parse::<IpRange>().unwrap().to_string(),
            "10.0.0.0/8"
        );
        assert_eq!("::1".parse::<IpRange>().unwrap().to_string(), "::1/128");
        for bad in ["10.0.0.0/33", "fd00::/129", "nope", "10.0.0.0/x", ""] {
            assert!(bad.parse::<IpRange>().is_err(), "{bad}");
        }
    }
}
