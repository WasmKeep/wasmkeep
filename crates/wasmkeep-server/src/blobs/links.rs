//! Links (ADR 0030, section 4): URLs a browser downloads a file from, or
//! uploads one new file to, without the app's code.
//!
//! A download is presigned on a bucket whose start proof passed (SigV4,
//! signed by the bucket for the link endpoint), else a node link. An upload
//! is always a node link: `object_store` refuses to sign `content-length`,
//! so a presigned PUT could not bind the size the app allowed. A node link
//! is `<base>/_wasmkeep/blobs/v1/<token>` ([`super::token`]); its base comes
//! from configuration only (`blob-link-base`, else the app's first
//! hostname, else `public-advertise`), never from a request.

use std::sync::{Arc, OnceLock};
use std::time::Duration;

use object_store::signer::{SignedUrlOptions, Signer};
use ring::hmac;
use wasmkeep_wit::wasmkeep::actor::blobs::BlobError;

use super::token::{Grant, Token};
use super::{Namespace, now_ms};

/// A download link's default life.
pub(crate) const DOWNLOAD_SECS: u32 = 300;
/// An upload link's default life.
pub(crate) const UPLOAD_SECS: u32 = 900;
/// Any link's longest life.
pub(crate) const MAX_SECS: u32 = 3_600;
/// Links one event may make.
pub(crate) const PER_EVENT: u32 = 1_000;
/// A download file name's longest, in bytes, once cleaned.
const MAX_FILENAME: usize = 255;
/// The path node links are served at, on the public port.
pub const NODE_LINK_PATH: &str = "/_wasmkeep/blobs/v1/";

/// How an app runtime makes links, given by the process: whether downloads
/// are presigned (the start proof passed), and the base of node links.
#[derive(Debug, Clone, Default)]
pub struct LinkSettings {
    /// Signs downloads for the link endpoint; `None`: node links only.
    pub presigner: Option<Arc<dyn Signer>>,
    /// `scheme://authority` of node links (`blob-link-base`, else
    /// `https://` and the app's first hostname, else `public-advertise`).
    /// `None`: no node links.
    pub base: Option<String>,
}

/// Checks a `blob-link-base`: `http://` or `https://`, an authority, and
/// nothing else (no user, path, query or fragment). Returns it without a
/// trailing `/`.
///
/// # Errors
/// Why it is not one.
pub fn check_base(base: &str) -> Result<String, String> {
    let bad = || {
        format!(
            "{base:?} is not a link base: give `https://HOST[:PORT]`, with no path, query or user"
        )
    };
    let rest = base
        .strip_prefix("https://")
        .or_else(|| base.strip_prefix("http://"))
        .ok_or_else(bad)?;
    let authority = rest.strip_suffix('/').unwrap_or(rest);
    if authority.is_empty()
        || !authority
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'-' | b':' | b'[' | b']'))
    {
        return Err(bad());
    }
    Ok(base.strip_suffix('/').unwrap_or(base).to_owned())
}

/// Where the MAC key of node links comes from.
pub(crate) enum MacSource {
    /// Derived from the cluster secret once it is loaded.
    Cluster(Arc<crate::cluster::Cluster>),
    /// The node's own key file (a node without a bucket).
    Key(hmac::Key),
    /// No key: node links are refused.
    None(String),
}

/// An app runtime's links.
pub(crate) struct Links {
    pub(crate) tenant: String,
    pub(crate) app: String,
    pub(crate) epoch: String,
    pub(crate) settings: LinkSettings,
    mac: MacSource,
    derived: OnceLock<hmac::Key>,
}

impl std::fmt::Debug for Links {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Links")
            .field("app", &format!("{}/{}", self.tenant, self.app))
            .field("presigned", &self.settings.presigner.is_some())
            .field("base", &self.settings.base)
            .finish_non_exhaustive()
    }
}

impl Links {
    pub(crate) fn new(
        tenant: &str,
        app: &str,
        epoch: &str,
        settings: LinkSettings,
        mac: MacSource,
    ) -> Self {
        crate::metrics::blob_links_mode(settings.presigner.is_some());
        Self {
            tenant: tenant.to_owned(),
            app: app.to_owned(),
            epoch: epoch.to_owned(),
            settings,
            mac,
            derived: OnceLock::new(),
        }
    }

    /// The MAC key of this app runtime's node links, if it has one: from
    /// the cluster secret this node already loaded (never fetched here),
    /// or the node's key file. A rotated secret takes effect at restart.
    pub(crate) fn key(&self) -> Result<&hmac::Key, String> {
        if let Some(k) = self.derived.get() {
            return Ok(k);
        }
        match &self.mac {
            MacSource::Key(k) => Ok(k),
            MacSource::None(why) => Err(why.clone()),
            MacSource::Cluster(c) => {
                let secret = c
                    .loaded_secret()
                    .ok_or("the cluster secret is not loaded yet")?;
                let key = super::token::mac_key(secret.as_bytes(), &self.tenant, &self.app)
                    .ok_or("the node-link key cannot be derived")?;
                Ok(self.derived.get_or_init(|| key))
            }
        }
    }

    fn base(&self) -> Result<&str, BlobError> {
        self.settings.base.as_deref().ok_or_else(|| {
            BlobError::Unavailable(
                "node links need a base: set blob-link-base (or a hostname, or --advertise)".into(),
            )
        })
    }

    /// A node link for `grant` on `ns`'s `key`, alive `secs`.
    pub(crate) fn node_link(
        &self,
        ns: &Namespace,
        key: &str,
        secs: u32,
        grant: Grant,
    ) -> Result<(String, u64), BlobError> {
        let base = self.base()?;
        let mac = self.key().map_err(BlobError::Unavailable)?;
        let expires_at_ms = now_ms() + u64::from(secs) * 1_000;
        let token = Token {
            tenant: self.tenant.clone(),
            app: self.app.clone(),
            epoch: self.epoch.clone(),
            namespace: ns.rel().to_owned(),
            key: key.to_owned(),
            expires_at_ms,
            grant,
        };
        Ok((
            format!("{base}{NODE_LINK_PATH}{}", token.seal(mac)),
            expires_at_ms,
        ))
    }

    /// A presigned download of `path`, overriding every header the object
    /// was stored with (section 4).
    pub(crate) async fn presigned_download(
        &self,
        signer: &Arc<dyn Signer>,
        path: &object_store::path::Path,
        secs: u32,
        disposition: &str,
        content_type: &str,
    ) -> Result<String, BlobError> {
        let options = SignedUrlOptions::new().with_query([
            ("response-content-disposition", disposition.to_owned()),
            ("response-content-type", content_type.to_owned()),
            ("response-cache-control", format!("private, max-age={secs}")),
            ("response-content-encoding", "identity".to_owned()),
        ]);
        signer
            .signed_url_opts(
                object_store::signer::Method::GET,
                path,
                Duration::from_secs(u64::from(secs)),
                &options,
            )
            .await
            .map(|u| u.to_string())
            .map_err(|e| BlobError::Unavailable(format!("signing the link: {e}")))
    }
}

/// The node's own node-link secret, without a bucket: 32 random bytes in
/// `<app dir>/.blob-link-key`, made once (mode 0600) and kept, so links
/// outlive a restart. A new key is written whole to a file of its own and
/// then linked into place, never replacing a file there: of two starts at
/// once, one key wins and both read it, checked alike.
///
/// # Errors
/// The file cannot be read or made, or is not 32 bytes.
pub(crate) fn local_key(app_dir: &std::path::Path) -> Result<Vec<u8>, String> {
    use ring::rand::SecureRandom as _;
    let path = app_dir.join(".blob-link-key");
    if let Some(k) = read_key(&path)? {
        return Ok(k);
    }
    let mut k = vec![0u8; 32];
    ring::rand::SystemRandom::new()
        .fill(&mut k)
        .map_err(|_| "no randomness".to_owned())?;
    std::fs::create_dir_all(app_dir).map_err(|e| e.to_string())?;
    let id = super::random_id().map_err(|_| "no randomness".to_owned())?;
    let tmp = app_dir.join(format!(".blob-link-key.{id}.tmp"));
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
    let write = |at: &std::path::Path| {
        options.open(at).and_then(|mut f| {
            use std::io::Write as _;
            f.write_all(&k).and_then(|()| f.sync_all())
        })
    };
    // A hard link, unlike a rename, never replaces a file already there.
    let linked = write(&tmp).and_then(|()| std::fs::hard_link(&tmp, &path));
    let _ = std::fs::remove_file(&tmp);
    // A file system without hard links: the key is written in place, and
    // a start that reads it half-written refuses it (32 bytes, checked).
    let linked = match linked {
        Err(e) if e.kind() != std::io::ErrorKind::AlreadyExists && !path.exists() => {
            tracing::debug!(error = %e, "no hard link for the node-link key: writing it in place");
            write(&path)
        }
        other => other,
    };
    match linked {
        Ok(()) => {
            if let Ok(dir) = std::fs::File::open(app_dir) {
                let _ = dir.sync_all();
            }
            Ok(k)
        }
        // Made meanwhile by another start: read that one.
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
            read_key(&path)?.ok_or_else(|| format!("{} vanished as it was made", path.display()))
        }
        Err(e) => Err(format!("{}: {e}", path.display())),
    }
}

/// The key file at `path`, if there is one, checked to be 32 bytes.
fn read_key(path: &std::path::Path) -> Result<Option<Vec<u8>>, String> {
    match std::fs::read(path) {
        Ok(k) if k.len() == 32 => Ok(Some(k)),
        Ok(_) => Err(format!("{} is not 32 bytes", path.display())),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(format!("{}: {e}", path.display())),
    }
}

/// A link's life in seconds: `given`, or `default`, within 1 to 3,600.
pub(crate) fn life(given: Option<u32>, default: u32) -> Result<u32, BlobError> {
    match given.unwrap_or(default) {
        s @ 1..=MAX_SECS => Ok(s),
        s => Err(BlobError::NotAllowed(format!(
            "a link lives 1 to {MAX_SECS} seconds, not {s}"
        ))),
    }
}

/// Whether a browser may be told to show a file of `content_type` rather
/// than download it (section 8): images it cannot run, audio, video and
/// plain text; never HTML, SVG, PDF or XML.
pub(crate) fn inline_safe(content_type: &str) -> bool {
    let essence = content_type
        .split(';')
        .next()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    matches!(
        essence.as_str(),
        "image/png" | "image/jpeg" | "image/gif" | "image/webp" | "image/avif" | "text/plain"
    ) || essence
        .split_once('/')
        .is_some_and(|(t, s)| matches!(t, "audio" | "video") && !s.is_empty())
}

/// A download's name: what the app gave, else the key's last segment,
/// without `"`, `\`, `/`, CR, LF or control characters, at most 255 bytes.
pub(crate) fn filename(given: Option<&str>, key: &str) -> String {
    let raw = given.unwrap_or_else(|| key.rsplit('/').next().unwrap_or(key));
    let mut out = String::new();
    for c in raw.chars() {
        if c.is_control() || matches!(c, '"' | '\\' | '/') {
            continue;
        }
        if out.len() + c.len_utf8() > MAX_FILENAME {
            break;
        }
        out.push(c);
    }
    if out.trim().is_empty() {
        key.rsplit('/').next().unwrap_or("file").to_owned()
    } else {
        out
    }
}

/// `Content-Disposition` for a download (RFC 6266): `inline`, or an
/// attachment with an ASCII fallback and the UTF-8 name.
pub(crate) fn disposition(inline: bool, name: &str) -> String {
    if inline {
        return "inline".into();
    }
    let ascii: String = name
        .chars()
        .map(|c| {
            if c.is_ascii() && !c.is_ascii_control() {
                c
            } else {
                '_'
            }
        })
        .collect();
    let mut encoded = String::new();
    for b in name.bytes() {
        if b.is_ascii_alphanumeric() || b"!#$&+-.^_`|~".contains(&b) {
            encoded.push(char::from(b));
        } else {
            encoded.push_str(&format!("%{b:02X}"));
        }
    }
    format!("attachment; filename=\"{ascii}\"; filename*=UTF-8''{encoded}")
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)] // test code
    use super::*;

    #[test]
    fn only_types_a_browser_cannot_run_are_shown_inline() {
        for t in [
            "image/png",
            "IMAGE/JPEG",
            "image/webp",
            "text/plain; charset=utf-8",
            "audio/ogg",
            "video/mp4",
        ] {
            assert!(inline_safe(t), "{t}");
        }
        for t in [
            "text/html",
            "image/svg+xml",
            "application/pdf",
            "text/xml",
            "application/xhtml+xml",
            "application/javascript",
            "text/plain2",
            "audio",
            "application/octet-stream",
        ] {
            assert!(!inline_safe(t), "{t}");
        }
    }

    #[test]
    fn download_names_are_cleaned_and_encoded() {
        assert_eq!(filename(None, "a/b/photo.jpg"), "photo.jpg");
        assert_eq!(filename(Some("a\"b\\c/d\r\ne\u{7}.txt"), "k"), "abcde.txt");
        assert_eq!(filename(Some("\"/\\"), "x/k"), "k");
        assert_eq!(filename(Some(&"é".repeat(200)), "k").len(), 254);
        assert_eq!(
            disposition(false, "Été 1.jpg"),
            "attachment; filename=\"_t_ 1.jpg\"; filename*=UTF-8''%C3%89t%C3%A9%201.jpg"
        );
        assert_eq!(disposition(true, "x"), "inline");
    }

    #[test]
    fn lives_are_bounded() {
        assert!(matches!(life(None, DOWNLOAD_SECS), Ok(300)));
        assert!(matches!(life(Some(3_600), DOWNLOAD_SECS), Ok(3_600)));
        assert!(life(Some(0), UPLOAD_SECS).is_err());
        assert!(life(Some(3_601), UPLOAD_SECS).is_err());
    }

    #[test]
    fn a_key_file_is_always_32_bytes() {
        let tmp = tempfile::tempdir().unwrap();
        let k = local_key(tmp.path()).unwrap();
        assert_eq!(k.len(), 32);
        assert_eq!(local_key(tmp.path()).unwrap(), k, "kept across starts");
        for short in [&b""[..], &[7u8; 31][..], &[7u8; 33][..]] {
            std::fs::write(tmp.path().join(".blob-link-key"), short).unwrap();
            assert!(local_key(tmp.path()).is_err(), "{} bytes", short.len());
        }
    }

    #[test]
    fn starts_at_once_agree_on_one_whole_key() {
        for _ in 0..200 {
            let tmp = tempfile::tempdir().unwrap();
            let dir = tmp.path().join("app");
            let barrier = std::sync::Arc::new(std::sync::Barrier::new(8));
            let threads: Vec<_> = (0..8)
                .map(|_| {
                    let (dir, barrier) = (dir.clone(), std::sync::Arc::clone(&barrier));
                    std::thread::spawn(move || {
                        barrier.wait();
                        local_key(&dir)
                    })
                })
                .collect();
            let keys: Vec<Vec<u8>> = threads
                .into_iter()
                .map(|t| t.join().unwrap().unwrap())
                .collect();
            assert!(
                keys.iter().all(|k| k.len() == 32 && *k == keys[0]),
                "every start reads the one whole key"
            );
            let names: Vec<_> = std::fs::read_dir(&dir)
                .unwrap()
                .map(|e| e.unwrap().file_name())
                .collect();
            assert_eq!(names, [".blob-link-key"], "no temporary file is left");
        }
    }

    #[test]
    fn a_base_is_a_scheme_and_an_authority() {
        assert_eq!(
            check_base("https://shop.example.com/").unwrap(),
            "https://shop.example.com"
        );
        assert_eq!(
            check_base("http://10.0.0.1:8080").unwrap(),
            "http://10.0.0.1:8080"
        );
        for bad in [
            "shop.example.com",
            "ftp://x",
            "https://",
            "https://x/p",
            "https://u@x",
            "https://x?q",
            "https://x#f",
        ] {
            assert!(check_base(bad).is_err(), "{bad}");
        }
    }
}
