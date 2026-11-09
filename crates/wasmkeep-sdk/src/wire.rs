//! The small binary framing the [`kv`](crate::kv) and [`queue`](crate::queue)
//! helpers use between a caller and the actor that holds the data: unsigned
//! integers as 8 bytes big-endian, byte strings as a 4-byte big-endian length
//! then the bytes. Internal: only the helpers on both ends read it, so it may
//! change with the SDK (both ends are built from the same SDK version when a
//! class is deployed with its callers; see ADR 0015).

use crate::actors;

/// Appends to a buffer.
#[derive(Debug, Default)]
pub(crate) struct Writer(pub(crate) Vec<u8>);

impl Writer {
    pub(crate) fn u64(&mut self, v: u64) -> &mut Self {
        self.0.extend_from_slice(&v.to_be_bytes());
        self
    }

    pub(crate) fn bytes(&mut self, v: &[u8]) -> &mut Self {
        // Bodies are capped at 16 MB by `actors.call`, so a length always fits.
        let len = u32::try_from(v.len()).unwrap_or(u32::MAX);
        self.0.extend_from_slice(&len.to_be_bytes());
        self.0.extend_from_slice(v);
        self
    }

    pub(crate) fn finish(self) -> Vec<u8> {
        self.0
    }
}

/// Reads what [`Writer`] wrote. Every read fails (`None`) past the end.
#[derive(Debug)]
pub(crate) struct Reader<'a>(&'a [u8]);

impl<'a> Reader<'a> {
    pub(crate) fn new(buf: &'a [u8]) -> Self {
        Self(buf)
    }

    fn take(&mut self, n: usize) -> Option<&'a [u8]> {
        if self.0.len() < n {
            return None;
        }
        let (head, rest) = self.0.split_at(n);
        self.0 = rest;
        Some(head)
    }

    pub(crate) fn u64(&mut self) -> Option<u64> {
        Some(u64::from_be_bytes(self.take(8)?.try_into().ok()?))
    }

    pub(crate) fn bytes(&mut self) -> Option<&'a [u8]> {
        let len = u32::from_be_bytes(self.take(4)?.try_into().ok()?);
        self.take(usize::try_from(len).ok()?)
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// Lowercase hex, so any key fits in a path segment.
pub(crate) fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push(char::from(DIGITS[usize::from(b >> 4)]));
        s.push(char::from(DIGITS[usize::from(b & 0x0f)]));
    }
    s
}

pub(crate) fn unhex(s: &str) -> Option<Vec<u8>> {
    if !s.len().is_multiple_of(2) {
        return None;
    }
    let digit = |c: u8| match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    };
    s.as_bytes()
        .chunks(2)
        .map(|pair| Some(digit(pair[0])? << 4 | digit(pair[1])?))
        .collect()
}

/// The value of `name` in a path's query string (`/x?a=1&b=2`).
pub(crate) fn query_param<'a>(path: &'a str, name: &str) -> Option<&'a str> {
    path.split_once('?')?
        .1
        .split('&')
        .find_map(|pair| pair.strip_prefix(name)?.strip_prefix('='))
}

/// A header's value (first occurrence, case-insensitive) in a call response
/// or request header list.
pub(crate) fn header<'a>(headers: &'a [(String, String)], name: &str) -> Option<&'a str> {
    headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case(name))
        .map(|(_, v)| v.as_str())
}

/// Sends one request to another actor. Errors are text: the call failing,
/// not the callee's answer (that is in the status).
pub(crate) fn call(
    address: &str,
    method: &str,
    path: String,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
) -> Result<actors::Response, String> {
    actors::call(
        address,
        &actors::Request {
            method: method.to_owned(),
            path,
            headers,
            body,
        },
    )
    .map_err(|e| match e {
        actors::CallError::InvalidAddress(m) => format!("invalid address {address}: {m}"),
        actors::CallError::Reentrant => {
            format!("call to {address} would re-enter an actor already in this call chain")
        }
        actors::CallError::TooLarge => format!("call to {address}: body too large"),
        actors::CallError::Timeout => format!("call to {address} timed out"),
        actors::CallError::Failed(m) => format!("call to {address} failed: {m}"),
    })
}

/// The caller's address (`tenant/class/name`) when the request came from
/// another actor through `actors.call`. The host strips the header from
/// outside requests, so it can be trusted.
pub(crate) fn caller(request: &crate::http::IncomingRequest) -> Option<String> {
    request
        .headers()
        .get("x-wasmkeep-caller")
        .into_iter()
        .next()
        .and_then(|v| String::from_utf8(v).ok())
}

/// The headers of an incoming request, as pairs (non-UTF-8 values dropped).
pub(crate) fn request_headers(request: &crate::http::IncomingRequest) -> Vec<(String, String)> {
    request
        .headers()
        .entries()
        .into_iter()
        .filter_map(|(k, v)| Some((k, String::from_utf8(v).ok()?)))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn framing_round_trips_and_rejects_truncation() {
        let mut w = Writer::default();
        w.u64(7).bytes(b"abc").bytes(b"").u64(u64::MAX);
        let buf = w.finish();
        let mut r = Reader::new(&buf);
        assert_eq!(r.u64(), Some(7));
        assert_eq!(r.bytes(), Some(&b"abc"[..]));
        assert_eq!(r.bytes(), Some(&b""[..]));
        assert_eq!(r.u64(), Some(u64::MAX));
        assert!(r.is_empty());
        assert_eq!(r.u64(), None);

        // A length that runs past the end is refused, not truncated.
        let mut r = Reader::new(&buf[8..12]);
        assert_eq!(r.bytes(), None);
    }

    #[test]
    fn hex_round_trips() {
        let all: Vec<u8> = (0..=255).collect();
        assert_eq!(unhex(&hex(&all)), Some(all));
        assert_eq!(hex(b"\x00\xffA"), "00ff41");
        assert_eq!(unhex("0F"), Some(vec![15]));
        assert_eq!(unhex("abc"), None);
        assert_eq!(unhex("zz"), None);
        assert_eq!(unhex(""), Some(vec![]));
    }

    #[test]
    fn query_params() {
        assert_eq!(query_param("/x?a=1&bb=2", "bb"), Some("2"));
        assert_eq!(query_param("/x?a=1&bb=2", "b"), None);
        assert_eq!(query_param("/x", "a"), None);
        assert_eq!(query_param("/x?a=", "a"), Some(""));
    }
}
