//! Signed calls between nodes: the signature, its check, and replay
//! protection.

use super::*;

/// Whether `given` (an [`INTERNAL`] header value) signs this request,
/// addressed to node `me`, with `secret`, within the window around `now`.
/// Returns the signature (for the replay check) and the body hash it signs,
/// which the caller must check the body against.
pub(super) fn verify(
    secret: &str,
    given: &str,
    now: u64,
    me: &str,
    method: &hyper::Method,
    path: &str,
    headers: &hyper::HeaderMap,
) -> Option<Signed> {
    #[cfg(test)]
    VERIFIED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let mut parts = given.split(' ');
    let (Some("v4"), Some(ts), Some(nonce), Some(body), Some(mac), None) = (
        parts.next(),
        parts.next(),
        parts.next(),
        parts.next(),
        parts.next(),
        parts.next(),
    ) else {
        return None;
    };
    let lower_hex = |s: &str| s.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'));
    if body.len() != 64 || !lower_hex(body) {
        return None;
    }
    if nonce.is_empty() || nonce.len() > 64 || !lower_hex(nonce) {
        return None;
    }
    let ts = ts.parse::<u64>().ok()?;
    if now.abs_diff(ts) > SIGNATURE_WINDOW_MS {
        return None;
    }
    let expected = signature(secret, ts, nonce, me, method, path, body, headers);
    if !constant_time_eq(mac.as_bytes(), expected.as_bytes()) {
        return None;
    }
    Some(Signed {
        mac: unhex32(mac)?,
        ts,
        body: body.to_owned(),
        sender: None,
    })
}

/// How many signatures [`verify`] checked, in this test process.
#[cfg(test)]
pub(super) static VERIFIED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// 64 lowercase hex characters as 32 bytes.
pub(super) fn unhex32(s: &str) -> Option<[u8; 32]> {
    let b = s.as_bytes();
    if b.len() != 64 {
        return None;
    }
    let digit = |c: u8| match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        _ => None,
    };
    let mut out = [0u8; 32];
    for (i, pair) in b.as_chunks::<2>().0.iter().enumerate() {
        out[i] = digit(pair[0])? << 4 | digit(pair[1])?;
    }
    Some(out)
}

/// A valid node signature on a request, not yet checked against its body
/// nor recorded as used.
#[derive(Debug, Clone)]
pub(crate) struct Signed {
    pub(super) mac: [u8; 32],
    /// The signed timestamp (ms): the signature is replayable only within
    /// the window around it, so it is remembered until then.
    pub(super) ts: u64,
    /// The body's SHA-256 it signs, lowercase hex.
    pub(crate) body: String,
    /// The node its [`SENDER`] names, bound to this signature and checked
    /// a live node of this app; `None` without one (an older node).
    pub(crate) sender: Option<String>,
}

/// What a hop's [`INTERNAL`] signature makes of a request
/// ([`Cluster::check_hop`]).
#[derive(Debug, Clone)]
pub(crate) enum HopCheck {
    /// A client's request: no signature, or one not taken here (junk or a
    /// burnt copy on the public port; off the cluster port, one that does
    /// not hold or that no secret can check yet).
    Outside,
    /// Refused with 503, never served as a client's request: why.
    Refused(&'static str),
    /// A node's call whose signature and sender hold.
    Signed(Signed),
}

/// The router's verdict on a request's signature, made once when it
/// arrived ([`Cluster::judge_hop`]) by the runtime of node `node`, for
/// the handlers after it ([`Cluster::signed_hop`]). Set by the host only:
/// a request's extensions never come from the wire.
#[derive(Debug, Clone)]
pub(crate) struct CheckedHop {
    pub(super) node: String,
    pub(super) hop: HopCheck,
}

/// The path a hop's signature covers: as the request arrived, query
/// included, before routing cut `/actor/<class>/<name>` off it.
pub(crate) fn hop_path<B>(req: &Request<B>) -> &str {
    req.extensions().get::<OriginalPath>().map_or_else(
        || req.uri().path_and_query().map_or("/", |p| p.as_str()),
        |p| p.0.as_str(),
    )
}

/// Why a hop is refused when no secret is loaded yet.
pub(super) const SECRET_NOT_LOADED: &str = "cluster secret not loaded yet";
/// Why a hop on the cluster port is refused when its signature does not
/// hold: the router's answer and log.
pub(super) const BAD_SIGNATURE: &str = "bad, expired or misaddressed signature";
/// The same, logged with its likely causes where no router logs it.
pub(super) const BAD_SIGNATURE_WHY: &str = "bad, expired or misaddressed signature (clock skew? another cluster's secret? a node of an older version?)";

/// SHA-256 of a call's body, lowercase hex, as signed in [`INTERNAL`].
pub(crate) fn body_sha256(body: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    use std::fmt::Write as _;
    let mut hex = String::with_capacity(64);
    for b in Sha256::digest(body) {
        let _ = write!(hex, "{b:02x}");
    }
    hex
}

/// Signatures accepted within the window, each once: a signature seen on
/// the wire cannot be sent again. Each is remembered until its own
/// timestamp leaves the window (after which [`verify`] refuses it anyway),
/// and forgotten in expiry order, so the cost per call stays logarithmic.
#[derive(Debug)]
pub(crate) struct SeenSignatures {
    seen: Mutex<Seen>,
    /// Which record this is, for the metric of refusals when full.
    name: &'static str,
    /// Most signatures it holds at once.
    cap: usize,
    /// The public port's burns ([`SeenSignatures::public`]): full, a new
    /// one is dropped and counted apart, refusing nothing.
    public: bool,
}

/// What [`SeenSignatures::spend`] found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Spent {
    /// First use: recorded.
    Fresh,
    /// Used already, or too old to tell: a replay.
    Used,
    /// The record is full: refused, though it may be fresh.
    Full,
    /// Signed before this record was started ([`SeenSignatures::started_now`]):
    /// a record from before (another process, or runtime, under this node
    /// id) may have seen it, so it is refused.
    BeforeStart,
}

/// Why a node's validly signed call is refused by the replay check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SignatureRefused {
    /// Used already: a replay, answered 403.
    Used,
    /// Signed before this node started: the sender may sign it again (a
    /// clock behind) or it is a replay; answered 503 so a node retries.
    BeforeStart,
    /// The record of used signatures is full: 503, retried.
    Full,
}

impl SignatureRefused {
    /// The status a sender gets.
    pub(crate) fn status(self) -> hyper::StatusCode {
        match self {
            Self::Used => hyper::StatusCode::FORBIDDEN,
            Self::BeforeStart | Self::Full => hyper::StatusCode::SERVICE_UNAVAILABLE,
        }
    }

    /// The answer to the request with `headers`: 403 for a replay; 503
    /// otherwise, marked refused for a placed event (the sender runs it,
    /// ADR 0041) and never an answer of the actor's.
    pub(crate) fn answer(
        self,
        headers: &hyper::HeaderMap,
    ) -> hyper::Response<wasmtime_wasi_http::p2::body::HyperOutgoingBody> {
        match self {
            Self::Used => text_response(hyper::StatusCode::FORBIDDEN, "signature already used\n"),
            Self::BeforeStart => refuse_if_placed(
                headers,
                unavailable("call signed before this node started; sign it again\n"),
            ),
            Self::Full => refuse_if_placed(
                headers,
                unavailable("too many node calls to record their signatures; retry\n"),
            ),
        }
    }
}

impl Default for SeenSignatures {
    fn default() -> Self {
        Self::named("calls")
    }
}

#[derive(Debug, Default)]
pub(crate) struct Seen {
    macs: std::collections::HashSet<[u8; 32]>,
    by_expiry: std::collections::BTreeSet<(u64, [u8; 32])>,
    /// The latest clock reading seen; never moves back. Records are dropped
    /// only once it passes their window, and a signature whose window ended
    /// before it is refused (its record may be gone): so neither a body
    /// arriving late nor a clock stepping back lets one through twice.
    horizon: u64,
    /// When the record was started, on the monotonic clock, and the clock
    /// skew allowed between nodes (ms): see [`Seen::floor`].
    started: Option<(std::time::Instant, u64)>,
}

impl Seen {
    /// The earliest signature time accepted, as this clock reads `now`:
    /// the start (now less the time elapsed since, so the floor follows
    /// this clock when it is set back or forward) plus the allowed skew (a
    /// sender whose clock is ahead may have dated a call to the previous
    /// process up to that much past this start). A signature made earlier
    /// is refused: only a record that died with its process or runtime
    /// could tell whether it was used.
    fn floor(&self, now: u64) -> u64 {
        let Some((started, skew)) = self.started else {
            return 0;
        };
        // A millisecond more: the two clocks tick apart, and a call signed
        // in the very millisecond of the start must not be refused for it.
        let elapsed = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
        now.saturating_sub(elapsed.saturating_add(1))
            .saturating_add(skew)
    }

    /// What spending `mac`, signed at `ts`, would find at `now`, recording
    /// nothing.
    fn judge(&self, mac: &[u8; 32], ts: u64, now: u64, cap: usize) -> Spent {
        // verify() checked the window earlier (the body arrived since), and
        // against a clock that may have stepped back: check it again against
        // the horizon, past which records are gone.
        if ts.saturating_add(SIGNATURE_WINDOW_MS) < self.horizon.max(now) {
            return Spent::Used;
        }
        if ts < self.floor(now) {
            return Spent::BeforeStart;
        }
        if self.macs.contains(mac) {
            return Spent::Used;
        }
        if self.macs.len() >= cap {
            return Spent::Full;
        }
        Spent::Fresh
    }
}

/// Most signatures remembered at once (about 80 bytes each); past it, calls
/// are refused until old ones expire. Each lives at most two windows, so
/// this allows several thousand calls a second into one node.
pub(super) const MAX_SEEN: usize = 500_000;
/// The same for take and alarm hand-off calls, which are rare: a drain or
/// a move sends one per actor.
pub(super) const MAX_SEEN_CONTROL: usize = 50_000;
/// Signed calls that arrived on the public port in refuse mode, spent so
/// that the same copy is never honoured on the cluster port. Never fails
/// closed: once full, new ones are dropped (and counted), and only the
/// cluster port's checks read it.
pub(super) const MAX_SEEN_PUBLIC: usize = 50_000;
/// Charged ingress forwarded after an ownership race: rare, fail closed when full.
pub(super) const MAX_SEEN_INGRESS: usize = 4_096;

impl SeenSignatures {
    /// An empty record of up to [`MAX_SEEN`], `name` in the metric of
    /// refusals when full.
    pub(super) fn named(name: &'static str) -> Self {
        Self::with_cap(name, MAX_SEEN)
    }

    /// An empty record of up to `cap` signatures.
    pub(super) fn with_cap(name: &'static str, cap: usize) -> Self {
        Self {
            seen: Mutex::default(),
            name,
            cap,
            public: false,
        }
    }

    /// The record of the public port's burns, up to [`MAX_SEEN_PUBLIC`]:
    /// full, a new one is dropped and counted in
    /// `wasmkeep_signatures_public_dropped_total`, refusing nothing.
    pub(super) fn public() -> Self {
        Self {
            seen: Mutex::default(),
            name: "",
            cap: MAX_SEEN_PUBLIC,
            public: true,
        }
    }

    /// The same record, refusing every signature made before now plus
    /// `skew_ms` ([`Seen::floor`]): a new process or runtime cannot know
    /// which of those its predecessor had used.
    pub(super) fn started_now(self, skew_ms: u64) -> Self {
        self.seen.lock().unwrap_or_else(|p| p.into_inner()).started =
            Some((std::time::Instant::now(), skew_ms));
        self
    }

    /// Whether `mac`, signed at `ts`, may have been spent here: recorded,
    /// or so old that its record may be forgotten.
    pub(super) fn holds(&self, mac: &[u8; 32], ts: u64) -> bool {
        let seen = self.seen.lock().unwrap_or_else(|p| p.into_inner());
        seen.macs.contains(mac) || ts.saturating_add(SIGNATURE_WINDOW_MS) < seen.horizon
    }

    /// What spending `mac`, signed at `ts`, would find now, recording
    /// nothing: for an early check that must not use up the signature.
    pub(super) fn peek(&self, mac: [u8; 32], ts: u64, now: u64) -> Spent {
        self.seen
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .judge(&mac, ts, now, self.cap)
    }

    /// Whether `mac`, signed at `ts`, is a replay or from before the start.
    #[cfg(test)]
    pub(super) fn used(&self, mac: [u8; 32], ts: u64, now: u64) -> bool {
        matches!(self.peek(mac, ts, now), Spent::Used | Spent::BeforeStart)
    }

    /// Records `mac`, signed at `ts`; false if it was already used (or the
    /// record is full).
    pub(super) fn first_use(&self, mac: [u8; 32], ts: u64, now: u64) -> bool {
        self.spend(mac, ts, now) == Spent::Fresh
    }

    /// Records `mac`, signed at `ts`, and says whether it was fresh.
    pub(super) fn spend(&self, mac: [u8; 32], ts: u64, now: u64) -> Spent {
        let mut seen = self.seen.lock().unwrap_or_else(|p| p.into_inner());
        seen.horizon = seen.horizon.max(now);
        let horizon = seen.horizon;
        while let Some(&(expiry, old)) = seen.by_expiry.first() {
            if expiry >= horizon {
                break;
            }
            seen.by_expiry.pop_first();
            seen.macs.remove(&old);
        }
        let found = seen.judge(&mac, ts, now, self.cap);
        match found {
            Spent::Fresh => {
                seen.macs.insert(mac);
                seen.by_expiry
                    .insert((ts.saturating_add(SIGNATURE_WINDOW_MS), mac));
            }
            Spent::Full if self.public => {
                crate::metrics::inc(&crate::metrics::METRICS.signatures_public_dropped);
            }
            Spent::Full => crate::metrics::inc(
                &crate::metrics::METRICS
                    .signatures_refused_full
                    .get(&[("record", self.name)]),
            ),
            Spent::Used | Spent::BeforeStart => {}
        }
        found
    }

    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.seen
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .macs
            .len()
    }
}

/// What `random_hex(32)` writes: 64 hex characters.
pub(super) fn valid_secret(secret: &str) -> bool {
    secret.len() == 64 && secret.bytes().all(|b| b.is_ascii_hexdigit())
}

/// The signature of a call between nodes: HMAC-SHA256 with the cluster
/// secret over version, timestamp, nonce, target node, method, path, the
/// body's SHA-256, then caller, chain, the node-call markers, the app
/// (ADR 0022) and the upgrade mark (ADR 0027), one header per line. Header values are signed as their
/// bytes (they cannot hold a line break), so two different values never
/// sign alike; valid UTF-8 signs as its text did.
#[allow(clippy::too_many_arguments)]
pub(super) fn signature(
    secret: &str,
    ts: u64,
    nonce: &str,
    to: &str,
    method: &hyper::Method,
    path: &str,
    body_sha256: &str,
    headers: &hyper::HeaderMap,
) -> String {
    use std::fmt::Write as _;
    let mut msg =
        format!("wasmkeep-call v4\n{ts}\n{nonce}\n{to}\n{method}\n{path}\n{body_sha256}\n")
            .into_bytes();
    // The alarm hand-off marker is signed too, when present (ADR 0014):
    // calls without it keep the same signature as before. So is the app
    // (ADR 0022): a call without it signs as an older node's did, and the
    // receiver refuses it for the missing header, not the signature.
    // The upgrade mark of a front's hand-off (ADR 0027) likewise.
    // The placed mark (ADR 0041, section 8) likewise: a hop without it
    // signs as before, and one with it goes only to nodes announcing
    // `place`, which sign it too.
    for name in [
        CALLER,
        CHAIN,
        ALARM_HANDOFF,
        TAKE,
        APP,
        UPGRADE_MARK,
        PLACED,
    ] {
        for v in headers.get_all(name) {
            msg.extend_from_slice(name.as_bytes());
            msg.push(b':');
            msg.extend_from_slice(v.as_bytes());
            msg.push(b'\n');
        }
    }
    let mac = hmac_sha256(secret.as_bytes(), &msg);
    let mut hex = String::with_capacity(64);
    for b in mac {
        let _ = write!(hex, "{b:02x}");
    }
    hex
}

/// The two kinds of signed hop: a node's call ([`INTERNAL`]) and a
/// forward's proof of its client ([`ADMITTED`]). Both are v4 values; the
/// kind is also the word their [`SENDER`] binds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HopKind {
    Call,
    Admitted,
}

impl HopKind {
    /// The header the hop's signature goes in.
    pub(crate) fn header(self) -> &'static str {
        match self {
            Self::Call => INTERNAL,
            Self::Admitted => ADMITTED,
        }
    }

    /// The word its [`SENDER`] MAC binds.
    pub(crate) fn sender_kind(self) -> &'static str {
        match self {
            Self::Call => "internal",
            Self::Admitted => "admitted",
        }
    }
}

/// What a v4 signature signs, but the timestamp (taken when signed) and
/// the headers (the request's own).
pub(super) struct V4<'a> {
    /// The MAC's key: the cluster secret, or a key derived from it.
    pub(super) key: &'a str,
    pub(super) nonce: &'a str,
    /// The node the hop is for.
    pub(super) to: &'a str,
    pub(super) method: &'a hyper::Method,
    /// As sent, query included.
    pub(super) path: &'a str,
    /// The body's SHA-256 for a call; the client's identity headers' for a
    /// forward's proof.
    pub(super) body: &'a str,
}

impl Cluster {
    /// Signs a hop of `kind` in `headers`, over the headers as they are
    /// now (every signed one must be in place first): its v4 value
    /// (`v4 <ts> <nonce> <body> <mac>`), then this node named its sender,
    /// bound to that MAC under the cluster `secret` (ADR 0038, section 5).
    /// The only place a hop is signed.
    pub(super) fn sign_hop(
        &self,
        headers: &mut hyper::HeaderMap,
        kind: HopKind,
        secret: &str,
        v4: &V4<'_>,
    ) -> Result<(), String> {
        let ts = now_ms();
        let V4 {
            key,
            nonce,
            to,
            method,
            path,
            body,
        } = *v4;
        let mac = signature(key, ts, nonce, to, method, path, body, headers);
        headers.insert(
            kind.header(),
            hyper::header::HeaderValue::from_str(&format!("v4 {ts} {nonce} {body} {mac}"))
                .map_err(|e| e.to_string())?,
        );
        if let Ok(v) = hyper::header::HeaderValue::from_str(&sender_value(
            secret,
            kind.sender_kind(),
            &mac,
            &self.node_id,
        )) {
            headers.insert(SENDER, v);
        }
        Ok(())
    }
}

/// Names the node that sent a signed hop (ADR 0038, section 5): `NODE
/// MAC`, the MAC binding the node id to the hop's own signature (the last
/// field of its [`INTERNAL`] or [`ADMITTED`] value) under the cluster
/// secret. A header of its own rather than a line of the signature: a node
/// of an older version ignores it and still verifies the hop, and a node
/// of this version verifies both.
pub(crate) const SENDER: &str = "x-wasmkeep-sender";

/// The [`SENDER`] value for `sender`, of a hop of `kind` (`internal` or
/// `admitted`) whose own signature ends with `primary` (hex).
pub(crate) fn sender_value(secret: &str, kind: &str, primary: &str, sender: &str) -> String {
    format!("{sender} {}", sender_mac(secret, kind, primary, sender))
}

fn sender_mac(secret: &str, kind: &str, primary: &str, sender: &str) -> String {
    use std::fmt::Write as _;
    let msg = format!("wasmkeep-sender v1\n{kind}\n{primary}\n{sender}\n");
    let mut hex = String::with_capacity(64);
    for b in hmac_sha256(secret.as_bytes(), msg.as_bytes()) {
        let _ = write!(hex, "{b:02x}");
    }
    hex
}

/// The node id a [`SENDER`] value names, unauthenticated: for matching it
/// against a connection's pin before the hop's signature is checked.
pub(crate) fn sender_named(value: &str) -> Option<&str> {
    let (node, _) = value.split_once(' ')?;
    wasmkeep_cluster::membership::validate_node_id(node)
        .is_ok()
        .then_some(node)
}

/// The node a [`SENDER`] value names, if it binds that node to the hop
/// whose own signature ends with `primary`.
pub(crate) fn verify_sender(
    secret: &str,
    kind: &str,
    primary: &str,
    value: &str,
) -> Option<String> {
    let node = sender_named(value)?;
    let (_, mac) = value.split_once(' ')?;
    let expected = sender_mac(secret, kind, primary, node);
    constant_time_eq(mac.as_bytes(), expected.as_bytes()).then(|| node.to_owned())
}

/// Whether the connection a hop came on (`peer`, on the cluster port) is
/// the one its [`SENDER`] names (ADR 0038, section 5): that node's own
/// live heartbeat publishes the pin presented. A hop without a sender (a
/// node of an older version) or over plain HTTP (`None`) passes here; a
/// sender named twice does not.
pub(crate) fn sender_presented(
    headers: &hyper::HeaderMap,
    peer: Option<&crate::http::ClusterPeer>,
) -> bool {
    let Some(peer) = peer else {
        return true;
    };
    let mut values = headers.get_all(SENDER).iter();
    let Some(first) = values.next() else {
        return true;
    };
    if values.next().is_some() {
        return false;
    }
    first
        .to_str()
        .ok()
        .and_then(sender_named)
        .is_some_and(|node| peer.publishes(node))
}

/// HMAC-SHA256 (RFC 2104) over `sha2`.
pub(super) fn hmac_sha256(key: &[u8], msg: &[u8]) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    let mut block = [0u8; 64];
    if key.len() > 64 {
        block[..32].copy_from_slice(&Sha256::digest(key));
    } else {
        block[..key.len()].copy_from_slice(key);
    }
    let (mut inner, mut outer) = ([0x36u8; 64], [0x5cu8; 64]);
    for i in 0..64 {
        inner[i] ^= block[i];
        outer[i] ^= block[i];
    }
    let inner = Sha256::new()
        .chain_update(inner)
        .chain_update(msg)
        .finalize();
    Sha256::new()
        .chain_update(outer)
        .chain_update(inner)
        .finalize()
        .into()
}

pub(super) fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}
