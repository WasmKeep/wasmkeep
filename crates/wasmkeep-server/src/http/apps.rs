//! Several apps behind one process's ports (ADR 0038, section 6): the
//! public port routes by an explicit hostname table, the cluster port by
//! the app a hop names (checked against its signature by that app's
//! runtime). A connection on the public port belongs to the app of its
//! first request.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};

use hyper::body::Incoming;
use hyper::{Request, Response, StatusCode};
use wasmtime_wasi_http::p2::body::HyperOutgoingBody;

use super::{App, HEALTH_PATH, text_response};

/// Where an app's runtime is.
#[derive(Clone)]
pub(crate) enum SlotState {
    /// Not started yet, or being started again after a verdict: its
    /// hostnames answer 503 with `Retry-After`.
    Starting,
    /// Serving.
    Running(App),
    /// Its app was deleted (ADR 0028): 503 without `Retry-After`, for good.
    Deleted,
}

/// One app of the table.
pub(crate) struct AppSlot {
    /// `tenant/app`.
    pub(crate) name: String,
    state: RwLock<SlotState>,
}

impl AppSlot {
    pub(crate) fn set(&self, state: SlotState) {
        *self.state.write().unwrap_or_else(|p| p.into_inner()) = state;
    }

    pub(crate) fn get(&self) -> SlotState {
        self.state.read().unwrap_or_else(|p| p.into_inner()).clone()
    }
}

/// The apps of a process, by hostname and by name.
pub(crate) struct AppTable {
    pub(crate) slots: Vec<Arc<AppSlot>>,
    by_host: HashMap<String, usize>,
    by_name: HashMap<String, usize>,
}

impl AppTable {
    /// `apps`: each app's `tenant/app` and normalized hostnames (checked
    /// unique by the node file).
    pub(crate) fn new(apps: &[(String, Vec<String>)]) -> Self {
        let mut by_host = HashMap::new();
        let mut by_name = HashMap::new();
        let mut slots = Vec::new();
        for (i, (name, hosts)) in apps.iter().enumerate() {
            for h in hosts {
                by_host.insert(h.clone(), i);
            }
            by_name.insert(name.clone(), i);
            slots.push(Arc::new(AppSlot {
                name: name.clone(),
                state: RwLock::new(SlotState::Starting),
            }));
        }
        Self {
            slots,
            by_host,
            by_name,
        }
    }

    /// Every app running now.
    pub(crate) fn running(&self) -> Vec<App> {
        self.slots
            .iter()
            .filter_map(|s| match s.get() {
                SlotState::Running(app) => Some(app),
                _ => None,
            })
            .collect()
    }

    /// A request on the public port: the authority rules, then the app its
    /// host names. Every `x-wasmkeep-*` header is removed first: on this
    /// port every request is a client's (ADR 0038, section 5).
    pub(crate) async fn route_public(
        &self,
        mut req: Request<Incoming>,
        connection: &Mutex<Option<usize>>,
    ) -> Response<HyperOutgoingBody> {
        strip_node_headers(req.headers_mut());
        let get = matches!(*req.method(), hyper::Method::GET | hyper::Method::HEAD);
        if req.uri().path() == HEALTH_PATH && get {
            return text_response(StatusCode::OK, "ok\n");
        }
        let host = match authority(&req) {
            Ok(host) => host,
            Err(status) => return text_response(status, refusal(status)),
        };
        let Some(&i) = self.by_host.get(&host) else {
            return text_response(StatusCode::MISDIRECTED_REQUEST, "unknown host\n");
        };
        {
            let mut first = connection.lock().unwrap_or_else(|p| p.into_inner());
            match *first {
                Some(app) if app != i => {
                    return text_response(
                        StatusCode::MISDIRECTED_REQUEST,
                        "this connection belongs to another app's host\n",
                    );
                }
                Some(_) => {}
                None => *first = Some(i),
            }
        }
        self.serve(i, req).await
    }

    /// A request on the cluster port: the app its hop names, once (its
    /// runtime checks the name against the hop's signature).
    pub(crate) async fn route_cluster(
        &self,
        req: Request<Incoming>,
    ) -> Response<HyperOutgoingBody> {
        let get = matches!(*req.method(), hyper::Method::GET | hyper::Method::HEAD);
        if req.uri().path() == HEALTH_PATH && get {
            return text_response(StatusCode::OK, "ok\n");
        }
        let mut names = req.headers().get_all(crate::cluster::APP).iter();
        let (Some(name), None) = (names.next(), names.next()) else {
            tracing::warn!("a hop naming no app, or two, refused");
            return crate::cluster::unavailable("a hop must name its app once\n");
        };
        let Some(&i) = name.to_str().ok().and_then(|n| self.by_name.get(n)) else {
            tracing::warn!(app = ?name, "a hop for an app this process does not serve refused");
            return crate::cluster::unavailable("this process does not serve that app\n");
        };
        self.serve(i, req).await
    }

    async fn serve(&self, i: usize, req: Request<Incoming>) -> Response<HyperOutgoingBody> {
        match self.slots[i].get() {
            SlotState::Running(app) => app.route(req).await,
            SlotState::Starting => crate::cluster::unavailable("this app is starting; retry\n"),
            SlotState::Deleted => {
                text_response(StatusCode::SERVICE_UNAVAILABLE, "this app was deleted\n")
            }
        }
    }
}

fn refusal(status: StatusCode) -> &'static str {
    if status == StatusCode::MISDIRECTED_REQUEST {
        "a request needs a Host\n"
    } else {
        "a request needs exactly one Host, matching its target\n"
    }
}

/// Removes every `x-wasmkeep-*` request header.
pub(crate) fn strip_node_headers(headers: &mut hyper::HeaderMap) {
    let names: Vec<hyper::header::HeaderName> = headers
        .keys()
        .filter(|n| n.as_str().starts_with("x-wasmkeep-"))
        .cloned()
        .collect();
    for n in names {
        headers.remove(n);
    }
}

/// The request's authority, normalized (ADR 0038, section 6): exactly one
/// `Host` on HTTP/1.1 (400 otherwise), 421 on HTTP/1.0 without one; a
/// target in absolute form must name the same authority (400). `Forwarded`
/// and `X-Forwarded-Host` are ignored.
pub(crate) fn authority<B>(req: &Request<B>) -> Result<String, StatusCode> {
    let mut hosts = req.headers().get_all(hyper::header::HOST).iter();
    let host = match (hosts.next(), hosts.next()) {
        (Some(h), None) => h.to_str().map_err(|_| StatusCode::BAD_REQUEST)?,
        (None, _) if req.version() == hyper::Version::HTTP_10 => {
            return Err(StatusCode::MISDIRECTED_REQUEST);
        }
        _ => return Err(StatusCode::BAD_REQUEST),
    };
    let host = normalize(host).ok_or(StatusCode::BAD_REQUEST)?;
    if let Some(target) = req.uri().authority() {
        let target = normalize(target.as_str()).ok_or(StatusCode::BAD_REQUEST)?;
        if target != host {
            return Err(StatusCode::BAD_REQUEST);
        }
    }
    Ok(host)
}

/// `host[:port]` lowercase, without its port and a trailing dot; `None`
/// if empty or with user information.
pub(crate) fn normalize(authority: &str) -> Option<String> {
    let a = authority.trim();
    if a.is_empty() || a.contains('@') {
        return None;
    }
    let host = if let Some(rest) = a.strip_prefix('[') {
        // An IPv6 literal: up to its bracket.
        let (inside, _) = rest.split_once(']')?;
        format!("[{inside}]")
    } else {
        match a.rsplit_once(':') {
            Some((h, port)) if port.bytes().all(|b| b.is_ascii_digit()) => h.to_owned(),
            Some(_) => return None,
            None => a.to_owned(),
        }
    };
    let host = host.strip_suffix('.').unwrap_or(&host).to_ascii_lowercase();
    (!host.is_empty()).then_some(host)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn req(version: hyper::Version, target: &str, hosts: &[&str]) -> Request<()> {
        let mut b = Request::builder().version(version).uri(target);
        for h in hosts {
            b = b.header(hyper::header::HOST, *h);
        }
        b.body(()).unwrap()
    }

    #[test]
    fn the_authority_is_taken_once_and_strictly() {
        const HTTP_10: hyper::Version = hyper::Version::HTTP_10;
        const HTTP_11: hyper::Version = hyper::Version::HTTP_11;
        assert_eq!(
            authority(&req(HTTP_11, "/", &["Shop.Test.:8080"])),
            Ok("shop.test".into())
        );
        assert_eq!(
            authority(&req(HTTP_11, "/", &[])),
            Err(StatusCode::BAD_REQUEST)
        );
        assert_eq!(
            authority(&req(HTTP_11, "/", &["a.test", "b.test"])),
            Err(StatusCode::BAD_REQUEST)
        );
        assert_eq!(
            authority(&req(HTTP_10, "/", &[])),
            Err(StatusCode::MISDIRECTED_REQUEST)
        );
        assert_eq!(
            authority(&req(HTTP_11, "http://b.test/x", &["a.test"])),
            Err(StatusCode::BAD_REQUEST)
        );
        assert_eq!(
            authority(&req(HTTP_11, "http://A.test:80/x", &["a.test"])),
            Ok("a.test".into())
        );
        assert_eq!(
            authority(&req(HTTP_11, "/", &["[::1]:8080"])),
            Ok("[::1]".into())
        );
        assert_eq!(
            authority(&req(HTTP_11, "/", &["u@a.test"])),
            Err(StatusCode::BAD_REQUEST)
        );
        let mut forwarded = req(HTTP_11, "/", &["a.test"]);
        forwarded
            .headers_mut()
            .insert("x-forwarded-host", "b.test".parse().unwrap());
        assert_eq!(authority(&forwarded), Ok("a.test".into()));
    }

    #[test]
    fn node_headers_are_stripped() {
        let mut h = hyper::HeaderMap::new();
        for n in [
            "x-wasmkeep-internal",
            "x-wasmkeep-app",
            "x-wasmkeep-sender",
            "x-other",
        ] {
            h.insert(
                hyper::header::HeaderName::from_static(n),
                "1".parse().unwrap(),
            );
        }
        strip_node_headers(&mut h);
        assert_eq!(h.len(), 1);
        assert!(h.contains_key("x-other"));
    }
}
