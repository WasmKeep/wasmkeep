//! Shared, canonical ingress policy used by project files and manifests.
use std::collections::BTreeMap;

pub const MAX_POLICIES: usize = 32;
pub type Policies = BTreeMap<String, Policy>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Policy {
    pub(crate) method: Option<String>,
    pub(crate) path: String,
    pub(crate) prefix: bool,
    pub(crate) header: Option<String>,
    pub(crate) rate: u32,
    pub(crate) seconds: u32,
    pub(crate) burst: u32,
}
impl Policy {
    pub fn parse(
        matcher: &str,
        key: &str,
        header: Option<&str>,
        rate: &str,
        burst: u32,
    ) -> Result<Self, String> {
        let (method, path) = match matcher.split_once(' ') {
            Some((m, p))
                if matches!(
                    m,
                    "GET"
                        | "HEAD"
                        | "POST"
                        | "PUT"
                        | "DELETE"
                        | "CONNECT"
                        | "OPTIONS"
                        | "TRACE"
                        | "PATCH"
                ) =>
            {
                (Some(m.to_owned()), p)
            }
            Some(_) => {
                return Err("match needs an uppercase standard HTTP method and one space".into());
            }
            None => (None, matcher),
        };
        let prefix = path.ends_with("/*");
        let path = if prefix {
            &path[..path.len() - 1]
        } else {
            path
        };
        if matcher.len() > 1032
            || !path.starts_with('/')
            || path.len() + usize::from(prefix) > 1024
            || !path.is_ascii()
            || path
                .bytes()
                .any(|b| b <= 32 || b == 127 || b"*?#".contains(&b))
        {
            return Err("match needs an absolute encoded path, optionally ending in /* (at most 1024 bytes)".into());
        }
        let header = match (key, header) {
            ("ip", None) => None,
            ("header", Some(h))
                if h.len() <= 128
                    && http::header::HeaderName::from_bytes(h.as_bytes()).is_ok()
                    && h.bytes().all(|b| !b.is_ascii_uppercase())
                    && !h.starts_with("x-wasmkeep-")
                    && !matches!(
                        h,
                        "connection"
                            | "upgrade"
                            | "host"
                            | "forwarded"
                            | "x-forwarded-for"
                            | "content-length"
                            | "transfer-encoding"
                            | "trailer"
                            | "te"
                            | "proxy-authorization"
                            | "proxy-authenticate"
                            | "keep-alive"
                    ) =>
            {
                Some(h.to_owned())
            }
            _ => return Err(
                "key must be ip without header, or header with a lowercase end-to-end header name"
                    .into(),
            ),
        };
        let (count, unit) = rate
            .split_once('/')
            .ok_or("rate needs a positive integer followed by /s, /m or /h")?;
        if count.is_empty() || !count.bytes().all(|b| b.is_ascii_digit()) {
            return Err("invalid rate integer".into());
        }
        let rate: u32 = count.parse().map_err(|_| "invalid rate integer")?;
        let seconds = match unit {
            "s" => 1,
            "m" => 60,
            "h" => 3600,
            _ => return Err("rate unit must be s, m or h".into()),
        };
        if !(1..=1_000_000).contains(&rate) || !(1..=1_000_000).contains(&burst) {
            return Err("rate and burst must be between 1 and 1000000".into());
        }
        Ok(Self {
            method,
            path: path.into(),
            prefix,
            header,
            rate,
            seconds,
            burst,
        })
    }
    pub(crate) fn matches(&self, method: &str, path: &str) -> bool {
        let paths = [match_path(path, false), match_path(path, true)];
        let rules = [match_path(&self.path, false), match_path(&self.path, true)];
        self.method
            .as_deref()
            .is_none_or(|m| m == method || (m == "GET" && method == "HEAD"))
            && paths.iter().any(|path| {
                rules.iter().any(|rule| {
                    if self.prefix {
                        path.starts_with(rule)
                    } else {
                        path.trim_end_matches('/') == rule.trim_end_matches('/')
                    }
                })
            })
    }
    pub(crate) fn line(&self, name: &str) -> String {
        format!(
            "limit {name} {} {}{} {} {}/{} {}\n",
            self.method.as_deref().unwrap_or("*"),
            self.path,
            if self.prefix { "*" } else { "" },
            self.header
                .as_ref()
                .map_or_else(|| "ip".to_owned(), |h| format!("header:{h}")),
            self.rate,
            match self.seconds {
                1 => "s",
                60 => "m",
                _ => "h",
            },
            self.burst
        )
    }
    pub(crate) fn from_line(line: &str) -> Option<(String, Self)> {
        let p: Vec<_> = line.split(' ').collect();
        if p.len() != 7 || p[0] != "limit" || !wasmkeep_runtime::is_identifier(p[1]) {
            return None;
        }
        let matcher = if p[2] == "*" {
            p[3].to_owned()
        } else {
            format!("{} {}", p[2], p[3])
        };
        let h = if p[4] == "ip" {
            None
        } else {
            Some(p[4].strip_prefix("header:")?)
        };
        let policy = Self::parse(
            &matcher,
            if h.is_some() { "header" } else { "ip" },
            h,
            p[5],
            p[6].parse().ok()?,
        )
        .ok()?;
        if policy.line(p[1]).trim_end() != line {
            return None;
        }
        Some((p[1].into(), policy))
    }
}
/// Matching only: routing and the guest-visible URI are not rewritten.
fn match_path(path: &str, resolve: bool) -> String {
    // Split exactly as ActorTarget does. Class and name are routing identity,
    // never dot segments; only the unresolved rest can be normalized.
    if let Some(tail) = path.strip_prefix("/actor/")
        && let Some((class, tail)) = tail.split_once('/')
    {
        let (name, rest) = tail.split_once('/').map_or((tail, ""), |(n, r)| (n, r));
        let name = crate::http::percent_decode(name)
            .map(|n| crate::cluster::percent_encode(&n))
            .unwrap_or_else(|| name.to_owned());
        let suffix = if tail.contains('/') {
            normalize_path(&format!("/{rest}"), resolve)
        } else {
            String::new()
        };
        return format!("/actor/{class}/{name}{suffix}");
    }
    normalize_path(path, resolve)
}

fn normalize_path(path: &str, resolve: bool) -> String {
    let mut out = String::with_capacity(path.len());
    let mut trailing_dot = false;
    for segment in path.split('/').filter(|s| !s.is_empty()) {
        let mut normalized = String::with_capacity(segment.len());
        let bytes = segment.as_bytes();
        let mut i = 0;
        while i < bytes.len() {
            if bytes[i] == b'%'
                && let Some(hex) = segment.get(i + 1..i + 3)
                && let Ok(b) = u8::from_str_radix(hex, 16)
            {
                if b.is_ascii_alphanumeric() || b"-._~".contains(&b) {
                    normalized.push(char::from(b));
                } else {
                    normalized.push_str(&format!("%{b:02X}"));
                }
                i += 3;
            } else {
                // URI paths and policy paths are ASCII.
                normalized.push(char::from(bytes[i]));
                i += 1;
            }
        }
        trailing_dot = resolve && (normalized == "." || normalized == "..");
        if resolve && normalized == "." {
            continue;
        }
        if resolve && normalized == ".." {
            if let Some(index) = out.rfind('/') {
                out.truncate(index);
            }
            continue;
        }
        out.push('/');
        out.push_str(&normalized);
    }
    if path.ends_with('/') || trailing_dot || out.is_empty() {
        out.push('/');
    }
    out
}

pub fn validate(policies: &Policies) -> Result<(), String> {
    if policies.len() > MAX_POLICIES {
        return Err("at most 32 limit policies per app".into());
    }
    if policies.keys().any(|n| !wasmkeep_runtime::is_identifier(n)) {
        return Err("limit names must match [a-z][a-z0-9-]{0,63}".into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn fix3_actor_boundary_and_raw_front_paths() {
        let p = Policy::parse("POST /actor/chat/*", "ip", None, "1/h", 1).unwrap();
        for path in [
            "/actor/chat/abc/../../x",
            "/actor/chat/abc/%2e%2e/%2e%2e/x",
            "/actor/chat/../x",
        ] {
            assert!(p.matches("POST", path), "{path}");
        }
        let p = Policy::parse("/admin/*", "ip", None, "1/h", 1).unwrap();
        assert!(p.matches("GET", "/admin/../public"));
        let p = Policy::parse("/public", "ip", None, "1/h", 1).unwrap();
        assert!(p.matches("GET", "/admin/../public"));
    }
    #[test]
    fn fix2_dot_segments_match_in_both_directions() {
        for alias in ["/a/../login", "/./login", "/a/%2e%2E/login", "/%2e/login"] {
            let p = Policy::parse("/login", "ip", None, "1/h", 1).unwrap();
            assert!(p.matches("GET", alias), "{alias}");
            let p = Policy::parse(alias, "ip", None, "1/h", 1).unwrap();
            assert!(p.matches("GET", "/login"), "{alias}");
        }
        let p = Policy::parse("/admin/*", "ip", None, "1/h", 1).unwrap();
        assert!(p.matches("GET", "/admin/../public"));
        for path in ["/admin/.", "/admin/x/..", "/admin/%2e", "/admin/x/%2e."] {
            assert!(p.matches("GET", path), "{path}");
        }
    }
    #[test]
    fn review_matching_closes_route_aliases() {
        let p = Policy::parse("GET /login", "ip", None, "1/h", 1).unwrap();
        for path in ["/login/", "//login", "/%6Cogin"] {
            assert!(p.matches("HEAD", path), "{path}");
        }
        let p = Policy::parse("/actor/chat/abc", "ip", None, "1/h", 1).unwrap();
        assert!(p.matches("GET", "/actor/chat/a%62c"));
        let p = Policy::parse("/actor/chat/a%2Fb", "ip", None, "1/h", 1).unwrap();
        assert!(p.matches("GET", "/actor/chat/a%2fb"));
    }
    #[test]
    fn matching_is_encoded_and_method_sensitive() {
        let p = Policy::parse("POST /api/*", "ip", None, "1/m", 1).unwrap();
        assert!(p.matches("POST", "/api/"));
        assert!(p.matches("POST", "/api/a%2Fb"));
        assert!(!p.matches("POST", "/api"));
        assert!(!p.matches("post", "/api/a"));
        assert!(!p.matches("GET", "/api/a"));
        assert!(!p.matches("POST", "/api%2Fa"));
        let p = Policy::parse("/a%2Fb", "ip", None, "1/s", 1).unwrap();
        assert!(p.matches("GET", "/a%2Fb"));
        assert!(!p.matches("GET", "/a/b"));
    }
    #[test]
    fn header_names_cannot_alias_the_ip_wire_key() {
        for header in ["-", "ip", "x-client-id"] {
            let p = Policy::parse("/", "header", Some(header), "1/s", 1).unwrap();
            assert_eq!(
                Policy::from_line(p.line("key").trim_end()),
                Some(("key".into(), p))
            );
        }
    }
    #[test]
    fn malformed_policies_are_rejected() {
        for path in [
            "get /",
            "GET  /",
            " /",
            "https://x/",
            "/a*b",
            "/a?x",
            "/a#b",
            "/a b",
            "/a\nb",
        ] {
            assert!(Policy::parse(path, "ip", None, "1/s", 1).is_err(), "{path}");
        }
        for rate in [
            "0/s",
            "-1/s",
            "+1/s",
            "1/d",
            "1.5/s",
            "1000001/s",
            "4294967296/s",
        ] {
            assert!(Policy::parse("/", "ip", None, rate, 1).is_err(), "{rate}");
        }
        for name in [
            "X-Client",
            "x-wasmkeep-internal",
            "x-forwarded-for",
            "connection",
            "bad name",
            "",
        ] {
            assert!(
                Policy::parse("/", "header", Some(name), "1/s", 1).is_err(),
                "{name}"
            );
        }
        assert!(Policy::parse("/", "ip", Some("x-client"), "1/s", 1).is_err());
        assert!(Policy::parse("/", "header", None, "1/s", 1).is_err());
    }
}
