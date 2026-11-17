//! Static asset index and host-only serving (ADR 0030).
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};

pub const MAX_FILE: u64 = 25 * 1024 * 1024;
pub const MAX_TOTAL: u64 = 512 * 1024 * 1024;
pub const MAX_FILES: usize = 20_000;
pub const MAX_INDEX: usize = 8 * 1024 * 1024;

pub fn digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
pub fn valid_digest(s: &str) -> bool {
    s.len() == 64
        && s.bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
pub fn valid_name(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 1024
        && !s.chars().any(|c| c.is_control() || c == '\\')
        && s.split('/').all(|p| !p.is_empty() && p != "." && p != "..")
        && !matches!(s.split('/').next(), Some("actor" | "_wasmkeep"))
}
pub fn valid_prefix(s: &str) -> bool {
    s.strip_suffix('/').is_some_and(valid_name)
}
fn encode(s: &str) -> String {
    let mut out = String::new();
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || b"._~/-".contains(&b) {
            out.push(char::from(b));
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}
fn decode(s: &str) -> Option<String> {
    let mut bytes = Vec::new();
    let mut rest = s.as_bytes();
    while let Some((&b, tail)) = rest.split_first() {
        if b == b'%' {
            bytes.push(u8::from_str_radix(std::str::from_utf8(rest.get(1..3)?).ok()?, 16).ok()?);
            rest = rest.get(3..)?;
        } else {
            bytes.push(b);
            rest = tail;
        }
    }
    String::from_utf8(bytes).ok()
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Content {
    pub digest: String,
    pub size: u64,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct File {
    pub content_type: String,
    pub identity: Content,
    pub br: Option<Content>,
    pub gzip: Option<Content>,
}
impl File {
    pub fn contents(&self) -> impl Iterator<Item = &Content> {
        std::iter::once(&self.identity)
            .chain(self.br.iter())
            .chain(self.gzip.iter())
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Index {
    pub not_found: String,
    pub front_first: BTreeSet<String>,
    pub immutable: BTreeSet<String>,
    pub files: BTreeMap<String, File>,
}
impl Default for Index {
    fn default() -> Self {
        Self {
            not_found: "front".into(),
            front_first: BTreeSet::new(),
            immutable: BTreeSet::new(),
            files: BTreeMap::new(),
        }
    }
}
impl Index {
    pub fn bytes(&self) -> Vec<u8> {
        let mut out = format!("wasmkeep-assets 1\nnot-found {}\n", self.not_found);
        for p in &self.front_first {
            out.push_str(&format!("front-first {}\n", encode(p)));
        }
        for p in &self.immutable {
            out.push_str(&format!("immutable {}\n", encode(p)));
        }
        for (name, file) in &self.files {
            out.push_str(&format!("file {} {}", encode(name), file.content_type));
            for (encoding, content) in [
                ("identity", Some(&file.identity)),
                ("br", file.br.as_ref()),
                ("gzip", file.gzip.as_ref()),
            ] {
                if let Some(c) = content {
                    out.push_str(&format!(" {encoding}:{}:{}", c.digest, c.size));
                }
            }
            out.push('\n');
        }
        out.into_bytes()
    }
    pub fn parse(bytes: &[u8]) -> Result<Self, String> {
        Self::parse_inner(bytes)
            .ok_or_else(|| "invalid or noncanonical wasmkeep-assets 1 index".into())
    }
    fn parse_inner(bytes: &[u8]) -> Option<Self> {
        if bytes.len() > MAX_INDEX {
            return None;
        }
        let mut lines = std::str::from_utf8(bytes).ok()?.lines();
        if lines.next()? != "wasmkeep-assets 1" {
            return None;
        }
        let mut index = Self {
            not_found: lines.next()?.strip_prefix("not-found ")?.into(),
            ..Self::default()
        };
        for line in lines {
            let parts: Vec<_> = line.split(' ').collect();
            match parts.as_slice() {
                ["front-first", p] => {
                    if !index.front_first.insert(decode(p)?) {
                        return None;
                    }
                }
                ["immutable", p] => {
                    if !index.immutable.insert(decode(p)?) {
                        return None;
                    }
                }
                ["file", name, mime, variants @ ..] => {
                    let mut cs = variants.iter();
                    let identity = parse_content(cs.next()?, "identity")?;
                    let mut file = File {
                        content_type: (*mime).into(),
                        identity,
                        br: None,
                        gzip: None,
                    };
                    for c in cs {
                        if c.starts_with("br:") && file.br.is_none() {
                            file.br = Some(parse_content(c, "br")?);
                        } else if c.starts_with("gzip:") && file.gzip.is_none() {
                            file.gzip = Some(parse_content(c, "gzip")?);
                        } else {
                            return None;
                        }
                    }
                    if index.files.insert(decode(name)?, file).is_some() {
                        return None;
                    }
                }
                _ => return None,
            }
        }
        index.validate().ok()?;
        (index.bytes() == bytes).then_some(index)
    }
    pub fn validate_rules(&self) -> Result<(), String> {
        if !matches!(self.not_found.as_str(), "front" | "spa" | "404") {
            return Err("not-found must be front, spa or 404".into());
        }
        for p in self.front_first.iter().chain(&self.immutable) {
            if !valid_prefix(p) {
                return Err(format!(
                    "invalid assets prefix {p:?}: must name whole segments and end in /"
                ));
            }
        }
        Ok(())
    }
    pub fn validate(&self) -> Result<(), String> {
        self.validate_rules()?;
        if self.files.len() > MAX_FILES {
            return Err("more than 20,000 asset files".into());
        }
        let mut total = 0_u64;
        let mut sizes = BTreeMap::new();
        for (name, file) in &self.files {
            if !valid_name(name) {
                return Err(format!("invalid asset name {name:?}"));
            }
            if file.content_type.is_empty()
                || !file.content_type.is_ascii()
                || file
                    .content_type
                    .bytes()
                    .any(|b| b.is_ascii_whitespace() || b.is_ascii_control())
                || !file.content_type.contains('/')
            {
                return Err(format!("invalid content type for {name}"));
            }
            for c in file.contents() {
                if !valid_digest(&c.digest) || c.size > MAX_FILE {
                    return Err(format!("invalid content digest or size for {name}"));
                }
                if sizes
                    .insert(&c.digest, c.size)
                    .is_some_and(|old| old != c.size)
                {
                    return Err("one digest has conflicting sizes".into());
                }
            }
            total = total
                .checked_add(file.identity.size)
                .ok_or("assets size overflow")?;
        }
        if total > MAX_TOTAL {
            return Err("assets exceed 512 MiB".into());
        }
        if self.not_found == "spa" && !self.files.contains_key("index.html") {
            return Err("spa requires root index.html".into());
        }
        if self.bytes().len() > MAX_INDEX {
            return Err("assets index exceeds 8 MiB".into());
        }
        Ok(())
    }
}
fn parse_content(s: &str, encoding: &str) -> Option<Content> {
    let (e, rest) = s.split_once(':')?;
    let (digest, size) = rest.split_once(':')?;
    (e == encoding).then_some(Content {
        digest: digest.into(),
        size: size.parse().ok()?,
    })
}
#[derive(Clone, Debug)]
pub struct Bundle {
    pub index: Index,
    pub contents: BTreeMap<String, Vec<u8>>,
}
#[derive(Default, Debug)]
pub enum Change {
    #[default]
    Keep,
    Set(Bundle),
    Remove,
}

#[cfg(test)]
#[path = "assets_tests.rs"]
mod tests;

mod serving;
pub use serving::{Cache, Deployed, State};

/// Default process budget for embedders using ActorConfig directly.
pub fn default_cache() -> std::sync::Arc<Cache> {
    static CACHE: std::sync::LazyLock<std::sync::Arc<Cache>> =
        std::sync::LazyLock::new(|| Cache::new(1024 * 1024 * 1024));
    CACHE.clone()
}

mod local;
pub use local::Local;
