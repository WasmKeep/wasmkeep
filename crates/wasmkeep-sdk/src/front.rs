//! Writing a front (ADR 0023), and the helpers fronts and actors share.
//!
//! A front is an app's stateless `wasi:http` component: it receives every
//! request outside `/actor/` and `/_wasmkeep/`, holds no state, and calls
//! the app's actors with [`actors::call`] as an inside caller, so it
//! reaches `internal` classes. A front crate depends on the SDK without its
//! default `actor` feature, so that the component targets the
//! `wasmkeep:actor/front` world only:
//!
//! ```toml
//! wasmkeep-sdk = { version = "0.3", default-features = false }
//! ```
//!
//! ```ignore
//! use wasmkeep_sdk::front::{self, actors};
//! use wasmkeep_sdk::http::{self, IncomingRequest, ResponseOutparam};
//!
//! struct Front;
//!
//! impl http::Guest for Front {
//!     fn handle(request: IncomingRequest, out: ResponseOutparam) {
//!         let call = actors::Request {
//!             method: "GET".into(),
//!             path: "/".into(),
//!             headers: vec![],
//!             body: vec![],
//!         };
//!         match actors::call("todos/alice", &call) {
//!             Ok(response) => http::respond_with(out, &response),
//!             Err(e) => http::respond_with(out, &front::unavailable(&e)),
//!         }
//!     }
//! }
//!
//! wasmkeep_sdk::export_front!(Front);
//! ```
//!
//! [`caller`], [`host`] and [`scheme`] read the request; [`caller`] is what
//! an actor uses to accept requests from its app's front only.

#[doc(hidden)]
#[allow(
    unsafe_code,
    missing_docs,
    missing_debug_implementations,
    unreachable_pub,
    clippy::all
)]
pub mod bindings {
    wit_bindgen::generate!({
        path: "wit",
        world: "front-core",
        additional_derives: [PartialEq, Clone],
    });
}

/// Calls to the app's actors (`actors::call("class/name", &request)`).
/// From a front, the call is inside: it reaches `internal` classes, and
/// the callee reads `<tenant>/_front/main` as its caller.
pub use bindings::wasmkeep::actor::actors;
/// Configuration and secrets the operator gives the front (`--front-config`
/// and `secrets set --front` in a deploy, `--front-env` on `serve`), or the
/// class in an actor.
pub use bindings::wasmkeep::actor::env;

use crate::http::IncomingRequest;
use actors::{ActorAddress, CallError, Response};

/// The header in which the host tells an actor who called it.
pub const CALLER_HEADER: &str = "x-wasmkeep-caller";

/// Who sent a request, as the host says in `x-wasmkeep-caller`.
#[derive(Debug, Clone, PartialEq)]
pub enum Caller {
    /// The app's front (`<tenant>/_front/main`).
    Front,
    /// Another actor of the app, by its address (the name decoded).
    Actor(ActorAddress),
    /// A client, through the port: no caller header. Also any value the
    /// host never sends, so that nothing unexpected passes for the front
    /// or an actor.
    Outside,
}

impl Caller {
    /// Reads an `x-wasmkeep-caller` value (`None`: the header is absent).
    ///
    /// Only the host sets the header: it removes any a client sends, to
    /// actors and to the front, and any a guest sets on `actors.call`. A
    /// value is the front only as exactly `<tenant>/_front/main`.
    pub fn parse(value: Option<&str>) -> Self {
        let Some(value) = value else {
            return Self::Outside;
        };
        let mut parts = value.splitn(3, '/');
        let (Some(tenant), Some(class), Some(name)) = (parts.next(), parts.next(), parts.next())
        else {
            return Self::Outside;
        };
        if tenant.is_empty() || class.is_empty() || name.is_empty() {
            return Self::Outside;
        }
        if class == "_front" {
            return if name == "main" {
                Self::Front
            } else {
                Self::Outside
            };
        }
        // A class starts with a lowercase letter; anything else is not an
        // actor the host would name.
        if !class.starts_with(|c: char| c.is_ascii_lowercase()) {
            return Self::Outside;
        }
        match percent_decode(name) {
            Some(name) => Self::Actor(ActorAddress {
                tenant: tenant.to_owned(),
                class: class.to_owned(),
                name,
            }),
            None => Self::Outside,
        }
    }
}

/// Who sent `request`: the app's front, another actor, or a client. In an
/// actor: `if caller(&request) != Caller::Front { /* answer 403 */ }`.
pub fn caller(request: &IncomingRequest) -> Caller {
    let value = first_value(request, CALLER_HEADER);
    Caller::parse(value.as_deref())
}

/// The request's authority: the client's `Host`, or the authority of an
/// absolute URI. Behind a proxy, the client's only if the proxy passes it
/// (nginx: `proxy_set_header Host $host`). The client chooses it and the
/// host does not validate it: escape it where it goes (HTML, JSON), and
/// check it against the names you serve before trusting it.
pub fn host(request: &IncomingRequest) -> Option<String> {
    request.authority()
}

/// `"https"` when the first `X-Forwarded-Proto` value is `https`, else
/// `"http"`. A client can send that header too: the answer is only as good
/// as the proxy in front of the node, which must replace it (nginx:
/// `proxy_set_header X-Forwarded-Proto $scheme`). Use it to build links and
/// redirects, never to decide what a request may do.
pub fn scheme(request: &IncomingRequest) -> &'static str {
    scheme_of(first_value(request, "x-forwarded-proto").as_deref())
}

/// The scheme for a first `X-Forwarded-Proto` header value (see [`scheme`]).
pub fn scheme_of(forwarded_proto: Option<&str>) -> &'static str {
    let first = forwarded_proto
        .and_then(|v| v.split(',').next())
        .map(str::trim);
    match first {
        Some(proto) if proto.eq_ignore_ascii_case("https") => "https",
        _ => "http",
    }
}

/// The answer for a call that failed: `503` with `Retry-After: 1` for
/// `failed` and `timeout` (the callee or its node is busy, overloaded or
/// unreachable, or the call ran out of time: retrying may work), `500` for
/// the others (`invalid-address`, `reentrant`, `too-large`: the front's own
/// mistake). The body is generic: the error's detail is for the log, not
/// the client.
pub fn unavailable(err: &CallError) -> Response {
    match err {
        CallError::Failed(_) | CallError::Timeout => Response {
            status: 503,
            headers: vec![
                ("content-type".into(), "text/plain; charset=utf-8".into()),
                ("retry-after".into(), "1".into()),
            ],
            body: b"unavailable, retry\n".to_vec(),
        },
        CallError::InvalidAddress(_) | CallError::Reentrant | CallError::TooLarge => Response {
            status: 500,
            headers: vec![("content-type".into(), "text/plain; charset=utf-8".into())],
            body: b"internal error\n".to_vec(),
        },
    }
}

/// The first value of header `name`, if it is text.
fn first_value(request: &IncomingRequest, name: &str) -> Option<String> {
    let values = request.headers().get(name);
    let first = values.into_iter().next()?;
    String::from_utf8(first).ok()
}

/// Decodes `%XX` escapes; `None` for a broken escape or bytes that are not
/// UTF-8.
fn percent_decode(s: &str) -> Option<String> {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%' {
            let hex = s.get(i + 1..i + 3)?;
            out.push(u8::from_str_radix(hex, 16).ok()?);
            i += 3;
        } else {
            out.push(b[i]);
            i += 1;
        }
    }
    String::from_utf8(out).ok()
}

/// Exports `$ty`, an [`http::Guest`](crate::http::Guest), as a front: the
/// `wasi:http` request handler, and nothing else.
#[macro_export]
macro_rules! export_front {
    ($ty:ident) => {
        $crate::wasi::http::proxy::export! { $ty with_types_in $crate::wasi }
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    fn actor(tenant: &str, class: &str, name: &str) -> Caller {
        Caller::Actor(ActorAddress {
            tenant: tenant.into(),
            class: class.into(),
            name: name.into(),
        })
    }

    #[test]
    fn the_front_is_exactly_tenant_front_main() {
        assert_eq!(Caller::parse(Some("default/_front/main")), Caller::Front);
        assert_eq!(Caller::parse(Some("acme/_front/main")), Caller::Front);
        for odd in [
            "default/_front/main/x",
            "default/_front/other",
            "default/_front/",
            "default/_front",
            "/_front/main",
            "default/_Front/main",
            "default/_front/main%20",
            "_front/main",
        ] {
            assert_ne!(Caller::parse(Some(odd)), Caller::Front, "{odd}");
        }
    }

    #[test]
    fn an_actor_is_its_address_with_the_name_decoded() {
        assert_eq!(
            Caller::parse(Some("default/room/lobby")),
            actor("default", "room", "lobby")
        );
        assert_eq!(
            Caller::parse(Some("acme/kv/a%2Fb%20c")),
            actor("acme", "kv", "a/b c")
        );
        // The name keeps its own slashes, encoded or not.
        assert_eq!(
            Caller::parse(Some("acme/kv/a/b")),
            actor("acme", "kv", "a/b")
        );
    }

    #[test]
    fn absent_or_malformed_is_outside() {
        assert_eq!(Caller::parse(None), Caller::Outside);
        for odd in [
            "",
            "default",
            "default/room",
            "default/room/",
            "/room/lobby",
            "default//lobby",
            "default/Room/lobby",
            "default/9room/lobby",
            "default/room/bad%zz",
            "default/room/bad%2",
            "default/room/%ff",
        ] {
            assert_eq!(Caller::parse(Some(odd)), Caller::Outside, "{odd}");
        }
    }

    #[test]
    fn scheme_reads_the_first_forwarded_proto_value() {
        assert_eq!(scheme_of(None), "http");
        assert_eq!(scheme_of(Some("https")), "https");
        assert_eq!(scheme_of(Some("HTTPS")), "https");
        assert_eq!(scheme_of(Some(" https , http")), "https");
        assert_eq!(scheme_of(Some("http, https")), "http");
        assert_eq!(scheme_of(Some("")), "http");
        assert_eq!(scheme_of(Some("httpsx")), "http");
        assert_eq!(scheme_of(Some("wss")), "http");
    }

    #[test]
    fn unavailable_is_503_for_failed_and_timeout_without_the_detail() {
        for err in [
            CallError::Failed("front busy: too many front requests waiting on this actor".into()),
            CallError::Timeout,
        ] {
            let r = unavailable(&err);
            assert_eq!(r.status, 503);
            assert!(r.headers.contains(&("retry-after".into(), "1".into())));
            assert!(!String::from_utf8_lossy(&r.body).contains("busy"));
        }
        for err in [
            CallError::InvalidAddress("secret detail".into()),
            CallError::Reentrant,
            CallError::TooLarge,
        ] {
            let r = unavailable(&err);
            assert_eq!(r.status, 500);
            assert!(!r.headers.iter().any(|(n, _)| n == "retry-after"));
            assert!(!String::from_utf8_lossy(&r.body).contains("secret"));
        }
    }
}
