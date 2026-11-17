//! Node-link tokens (ADR 0030, section 4): `/_wasmkeep/blobs/v1/<token>`,
//! the token the base64url of a record and an HMAC-SHA256 over it, under a
//! key derived per app. A token allows one method on one file of one
//! namespace, of one app and epoch, until it expires: every field is under
//! the MAC, and nothing in it is read before the MAC is checked.

use ring::hmac;

/// The record's version: a later form takes another.
const VERSION: u8 = 1;
/// What the MAC covers before the record, so that no other use of the key
/// could produce a valid token.
const DOMAIN: &[u8] = b"wasmkeep blob link v1\0";
const TAG: usize = 32;
/// The longest record (a 512-byte key, a 256-byte type or a 255-byte name,
/// and the rest), and the longest token's spelling: checked before decoding.
const MAX_RECORD: usize = 2_048;
pub(crate) const MAX_TOKEN: usize = (MAX_RECORD + TAG).div_ceil(3) * 4;

/// What a link allows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Grant {
    /// A `GET` of the file, served as an attachment under `filename` (or
    /// the key's last segment), or inline.
    Download {
        inline: bool,
        filename: Option<String>,
    },
    /// A `PUT` creating the file with exactly `size` bytes of `content_type`.
    Upload { size: u64, content_type: String },
}

/// A link's record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Token {
    pub(crate) tenant: String,
    pub(crate) app: String,
    pub(crate) epoch: String,
    /// `<class>/<name id>/<incarnation>` or `_front/main/0`.
    pub(crate) namespace: String,
    pub(crate) key: String,
    /// Milliseconds since the epoch, by the signing node's clock.
    pub(crate) expires_at_ms: u64,
    pub(crate) grant: Grant,
}

/// Why a token is refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Refused {
    /// Not a token: malformed, too long, or its MAC does not match this
    /// app runtime's key (another app's, a forgery, a rotated secret).
    BadToken,
}

/// The MAC key of app `tenant/app`'s node links, from the cluster secret
/// (or, without a bucket, the node's own key file): HKDF-SHA256 with the
/// label `wasmkeep blob links v1 <tenant>/<app>`.
pub(crate) fn mac_key(secret: &[u8], tenant: &str, app: &str) -> Option<hmac::Key> {
    let info = format!("wasmkeep blob links v1 {tenant}/{app}");
    let info = [info.as_bytes()];
    let prk = ring::hkdf::Salt::new(ring::hkdf::HKDF_SHA256, &[]).extract(secret);
    // 32 bytes is within HKDF-SHA256's output: neither step fails.
    let okm = prk.expand(&info, ring::hkdf::HKDF_SHA256).ok()?;
    let mut bytes = [0u8; 32];
    okm.fill(&mut bytes).ok()?;
    Some(hmac::Key::new(hmac::HMAC_SHA256, &bytes))
}

fn field(out: &mut Vec<u8>, s: &[u8]) {
    let n = u16::try_from(s.len()).unwrap_or(u16::MAX);
    out.extend_from_slice(&n.to_be_bytes());
    out.extend_from_slice(&s[..usize::from(n)]);
}

impl Token {
    fn record(&self) -> Vec<u8> {
        let mut out = vec![VERSION];
        match &self.grant {
            Grant::Download { .. } => out.push(0),
            Grant::Upload { .. } => out.push(1),
        }
        out.extend_from_slice(&self.expires_at_ms.to_be_bytes());
        for f in [
            &self.tenant,
            &self.app,
            &self.epoch,
            &self.namespace,
            &self.key,
        ] {
            field(&mut out, f.as_bytes());
        }
        match &self.grant {
            Grant::Download { inline, filename } => {
                out.push(u8::from(*inline));
                match filename {
                    Some(n) => {
                        out.push(1);
                        field(&mut out, n.as_bytes());
                    }
                    None => out.push(0),
                }
            }
            Grant::Upload { size, content_type } => {
                out.extend_from_slice(&size.to_be_bytes());
                field(&mut out, content_type.as_bytes());
            }
        }
        out
    }

    /// The token's spelling in a URL: base64url, no padding.
    pub(crate) fn seal(&self, key: &hmac::Key) -> String {
        let record = self.record();
        let tag = sign(key, &record);
        let mut all = record;
        all.extend_from_slice(tag.as_ref());
        base64url(&all)
    }

    /// The token spelled `s`, if its MAC is this key's. Nothing of the
    /// record is read before the MAC is checked, in constant time.
    pub(crate) fn open(s: &str, key: &hmac::Key) -> Result<Self, Refused> {
        if s.len() > MAX_TOKEN {
            return Err(Refused::BadToken);
        }
        let all = unbase64url(s).ok_or(Refused::BadToken)?;
        if all.len() <= TAG || all.len() > MAX_RECORD + TAG {
            return Err(Refused::BadToken);
        }
        let (record, tag) = all.split_at(all.len() - TAG);
        let mut msg = DOMAIN.to_vec();
        msg.extend_from_slice(record);
        hmac::verify(key, &msg, tag).map_err(|_| Refused::BadToken)?;
        parse(record).ok_or(Refused::BadToken)
    }
}

fn sign(key: &hmac::Key, record: &[u8]) -> hmac::Tag {
    let mut msg = DOMAIN.to_vec();
    msg.extend_from_slice(record);
    hmac::sign(key, &msg)
}

struct Reader<'a>(&'a [u8]);

impl Reader<'_> {
    fn bytes(&mut self, n: usize) -> Option<&[u8]> {
        if self.0.len() < n {
            return None;
        }
        let (a, b) = self.0.split_at(n);
        self.0 = b;
        Some(a)
    }
    fn u8(&mut self) -> Option<u8> {
        self.bytes(1).map(|b| b[0])
    }
    fn u64(&mut self) -> Option<u64> {
        self.bytes(8)
            .map(|b| u64::from_be_bytes(b.try_into().unwrap_or_default()))
    }
    fn string(&mut self) -> Option<String> {
        let n = self.bytes(2)?;
        let n = usize::from(u16::from_be_bytes([n[0], n[1]]));
        String::from_utf8(self.bytes(n)?.to_vec()).ok()
    }
}

fn parse(record: &[u8]) -> Option<Token> {
    let mut r = Reader(record);
    if r.u8()? != VERSION {
        return None;
    }
    let method = r.u8()?;
    let expires_at_ms = r.u64()?;
    let tenant = r.string()?;
    let app = r.string()?;
    let epoch = r.string()?;
    let namespace = r.string()?;
    let key = r.string()?;
    let grant = match method {
        0 => {
            let inline = match r.u8()? {
                0 => false,
                1 => true,
                _ => return None,
            };
            let filename = match r.u8()? {
                0 => None,
                1 => Some(r.string()?),
                _ => return None,
            };
            Grant::Download { inline, filename }
        }
        1 => {
            let size = r.u64()?;
            Grant::Upload {
                size,
                content_type: r.string()?,
            }
        }
        _ => return None,
    };
    // Nothing may follow.
    if !r.0.is_empty() {
        return None;
    }
    Some(Token {
        tenant,
        app,
        epoch,
        namespace,
        key,
        expires_at_ms,
        grant,
    })
}

const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

fn base64url(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let b = [
            chunk[0],
            chunk.get(1).copied().unwrap_or(0),
            chunk.get(2).copied().unwrap_or(0),
        ];
        let n = (u32::from(b[0]) << 16) | (u32::from(b[1]) << 8) | u32::from(b[2]);
        for i in 0..=chunk.len() {
            out.push(char::from(ALPHABET[((n >> (18 - 6 * i)) & 63) as usize]));
        }
    }
    out
}

fn unbase64url(s: &str) -> Option<Vec<u8>> {
    let value = |c: u8| ALPHABET.iter().position(|&a| a == c).map(|p| p as u32);
    let s = s.as_bytes();
    if s.len() % 4 == 1 {
        return None;
    }
    let mut out = Vec::with_capacity(s.len() * 3 / 4);
    for chunk in s.chunks(4) {
        let mut n = 0u32;
        for (i, &c) in chunk.iter().enumerate() {
            n |= value(c)? << (18 - 6 * i);
        }
        let bytes = n.to_be_bytes();
        out.extend_from_slice(&bytes[1..chunk.len()]);
    }
    // One spelling per token: bits past the last byte must be zero.
    if base64url(&out).as_bytes() != s {
        return None;
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)] // test code
    use super::*;

    fn key(app: &str) -> hmac::Key {
        mac_key(b"0123456789abcdef0123456789abcdef", "acme", app).unwrap()
    }

    fn download() -> Token {
        Token {
            tenant: "acme".into(),
            app: "shop".into(),
            epoch: "6e340b9cffb37a98".into(),
            namespace: "photos/2bd8/0".into(),
            key: "a/b~c.jpg".into(),
            expires_at_ms: 1_800_000_000_000,
            grant: Grant::Download {
                inline: false,
                filename: Some("Été photo.jpg".into()),
            },
        }
    }

    fn upload() -> Token {
        Token {
            grant: Grant::Upload {
                size: 1234,
                content_type: "image/png".into(),
            },
            ..download()
        }
    }

    #[test]
    fn a_token_opens_under_its_key_only() {
        for t in [download(), upload()] {
            let s = t.seal(&key("shop"));
            assert!(s.bytes().all(|b| ALPHABET.contains(&b)), "{s}");
            assert_eq!(Token::open(&s, &key("shop")), Ok(t.clone()));
            // Another app's key: in the same process or another, the same.
            assert_eq!(Token::open(&s, &key("blog")), Err(Refused::BadToken));
            assert_eq!(
                Token::open(
                    &s,
                    &mac_key(b"another secret, another cluster", "acme", "shop").unwrap()
                ),
                Err(Refused::BadToken)
            );
        }
    }

    #[test]
    fn every_byte_is_under_the_mac() {
        let k = key("shop");
        for t in [download(), upload()] {
            let all = unbase64url(&t.seal(&k)).unwrap();
            for i in 0..all.len() {
                for bit in [0x01u8, 0x80] {
                    let mut changed = all.clone();
                    changed[i] ^= bit;
                    assert_eq!(
                        Token::open(&base64url(&changed), &k),
                        Err(Refused::BadToken),
                        "byte {i}"
                    );
                }
            }
            // Cut anywhere, or with a byte more.
            for n in 0..all.len() {
                assert!(
                    Token::open(&base64url(&all[..n]), &k).is_err(),
                    "cut at {n}"
                );
            }
            let mut longer = all.clone();
            longer.push(0);
            assert!(Token::open(&base64url(&longer), &k).is_err());
        }
    }

    #[test]
    fn a_token_has_one_spelling() {
        let k = key("shop");
        let s = download().seal(&k);
        for bad in [
            format!("{s}="),
            format!("{s}A"),
            s.replace('-', "+"),
            s.replace('_', "/"),
            format!(" {s}"),
            String::new(),
            "A".repeat(MAX_TOKEN + 1),
        ] {
            if bad != s {
                assert!(Token::open(&bad, &k).is_err(), "{bad}");
            }
        }
    }

    #[test]
    fn the_key_is_per_app_and_per_secret() {
        let msg = b"m";
        let tags: Vec<Vec<u8>> = [
            mac_key(b"s1", "acme", "shop"),
            mac_key(b"s1", "acme", "blog"),
            mac_key(b"s1", "beta", "shop"),
            mac_key(b"s2", "acme", "shop"),
        ]
        .iter()
        .map(|k| hmac::sign(k.as_ref().unwrap(), msg).as_ref().to_vec())
        .collect();
        for (i, a) in tags.iter().enumerate() {
            for b in &tags[i + 1..] {
                assert_ne!(a, b);
            }
        }
    }

    #[test]
    fn base64url_round_trips() {
        for n in 0..70 {
            let bytes: Vec<u8> = (0..n).map(|i| (i * 37 + 11) as u8).collect();
            assert_eq!(unbase64url(&base64url(&bytes)).unwrap(), bytes);
        }
    }
}
