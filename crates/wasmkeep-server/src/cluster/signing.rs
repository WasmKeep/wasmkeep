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
    })
}

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
#[derive(Debug)]
pub(crate) struct Signed {
    pub(super) mac: [u8; 32],
    /// The signed timestamp (ms): the signature is replayable only within
    /// the window around it, so it is remembered until then.
    pub(super) ts: u64,
    /// The body's SHA-256 it signs, lowercase hex.
    pub(crate) body: String,
}

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
}

/// Most signatures remembered at once (about 80 bytes each); past it, calls
/// are refused until old ones expire. Each lives at most two windows, so
/// this allows several thousand calls a second into one node.
pub(super) const MAX_SEEN: usize = 500_000;
/// The same for take and alarm hand-off calls, which are rare: a drain or
/// a move sends one per actor.
pub(super) const MAX_SEEN_CONTROL: usize = 50_000;

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
        }
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
        // verify() checked the window earlier (the body arrived since), and
        // against a clock that may have stepped back: check it again against
        // the horizon, past which records are gone.
        if ts.saturating_add(SIGNATURE_WINDOW_MS) < horizon {
            return Spent::Used;
        }
        if seen.macs.contains(&mac) {
            return Spent::Used;
        }
        if seen.macs.len() >= self.cap {
            crate::metrics::inc(
                &crate::metrics::METRICS
                    .signatures_refused_full
                    .get(&[("record", self.name)]),
            );
            return Spent::Full;
        }
        seen.macs.insert(mac);
        seen.by_expiry
            .insert((ts.saturating_add(SIGNATURE_WINDOW_MS), mac));
        Spent::Fresh
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
/// body's SHA-256, caller and chain, one per line.
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
        format!("wasmkeep-call v4\n{ts}\n{nonce}\n{to}\n{method}\n{path}\n{body_sha256}\n");
    // The alarm hand-off marker is signed too, when present (ADR 0014):
    // calls without it keep the same signature as before.
    for name in [CALLER, CHAIN, ALARM_HANDOFF, TAKE] {
        for v in headers.get_all(name) {
            msg.push_str(name);
            msg.push(':');
            msg.push_str(&String::from_utf8_lossy(v.as_bytes()));
            msg.push('\n');
        }
    }
    let mac = hmac_sha256(secret.as_bytes(), msg.as_bytes());
    let mut hex = String::with_capacity(64);
    for b in mac {
        let _ = write!(hex, "{b:02x}");
    }
    hex
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
