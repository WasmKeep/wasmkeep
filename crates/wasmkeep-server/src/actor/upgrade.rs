//! WebSocket upgrades: `GET /actor/<class>/<name>/...` with an upgrade
//! header, accepted by the actor's `on-websocket-open` before 101; and
//! sockets the front hands to an actor (ADR 0027).

use super::requests::Held;
use super::sockets::Refused;
use super::*;

/// Why a socket was not opened before its 101.
enum NotOpened {
    /// Over a cap: others raced in since `can_open`. Nothing registered.
    Full(crate::websocket::OpenError),
    /// The actor refused a hand-off, or could not run its open event.
    Refused(Refused),
}

impl ActorService {
    /// Accepts a WebSocket upgrade for `/actor/<class>/<name><path>`: answers
    /// 101 and runs the socket in the background; `on-websocket-open` decides
    /// whether it stays open. A client's upgrade, or another node's signed
    /// relay of a hand-off by the front (ADR 0027).
    pub async fn upgrade(
        &self,
        class: &str,
        name: &str,
        mut req: Request<Incoming>,
    ) -> Response<HyperOutgoingBody> {
        // A node's signature, if this is one: only the front's hand-off is
        // relayed signed. Anything else (a client's forged mark or caller,
        // a bad signature) is an outside upgrade.
        let signed = self.inner.cluster.as_ref().and_then(|cluster| {
            let on_cluster_port = req
                .extensions()
                .get::<crate::http::ArrivedOnClusterPort>()
                .is_some();
            let path = req
                .extensions()
                .get::<crate::http::OriginalPath>()
                .map_or_else(
                    || {
                        req.uri()
                            .path_and_query()
                            .map_or("/", |p| p.as_str())
                            .to_owned()
                    },
                    |p| p.0.clone(),
                );
            cluster.is_internal(req.method(), &path, req.headers(), on_cluster_port)
        });
        // A relayed upgrade from a node of another app (or of an older
        // version) is refused first (ADR 0022); the header never reaches
        // a guest.
        if let Some(refused) = self
            .inner
            .cluster
            .as_ref()
            .and_then(|cluster| cluster.wrong_app(&req, signed.is_some()))
        {
            return refused;
        }
        let opener = match signed {
            None => Opener::Outside,
            Some(signed) => {
                let one = |name: &str| {
                    let mut values = req.headers().get_all(name).iter();
                    match (values.next(), values.next()) {
                        (Some(v), None) => Some(v.as_bytes().to_vec()),
                        _ => None,
                    }
                };
                let front = Caller::Front.value(&self.inner.tenant);
                let handed = req.method() == hyper::Method::GET
                    && signed.body == crate::cluster::body_sha256(b"")
                    && one(CALLER).as_deref() == Some(front.as_bytes())
                    && one(crate::cluster::UPGRADE_MARK).as_deref()
                        == Some(b"websocket".as_slice());
                if !handed {
                    crate::metrics::refused(crate::metrics::Refusal::Access);
                    return text_response(
                        StatusCode::FORBIDDEN,
                        "a node's upgrade must be the front's hand-off\n",
                    );
                }
                // Spent once: a relay seen on the wire cannot open another.
                let fresh = self
                    .inner
                    .cluster
                    .as_ref()
                    .is_some_and(|cluster| cluster.first_use(&signed, false));
                if !fresh {
                    return text_response(StatusCode::FORBIDDEN, "signature already used\n");
                }
                Opener::Front
            }
        };
        for name in [INTERNAL, CALLER, CHAIN, crate::cluster::UPGRADE_MARK] {
            req.headers_mut().remove(name);
        }
        self.open_socket(class, name, req, opener).await
    }

    /// A WebSocket upgrade the app's front handed to `class/name`, to open
    /// at `path` (ADR 0027): opened here as an inside caller's, or relayed,
    /// signed, to the actor's owner. `req` carries the client's headers and
    /// the connection's upgrade; its body is never read.
    pub(crate) async fn hand_off<B: Send + 'static>(
        &self,
        class: &str,
        name: &str,
        mut req: Request<B>,
    ) -> Response<HyperOutgoingBody> {
        let path = req
            .uri()
            .path_and_query()
            .map_or("/", |p| p.as_str())
            .to_owned();
        // Where a relay to the owner goes, as for an `/actor/` upgrade.
        req.extensions_mut()
            .insert(crate::http::OriginalPath(format!(
                "/actor/{class}/{}{path}",
                crate::cluster::percent_encode(name)
            )));
        self.open_socket(class, name, req, Opener::Front).await
    }

    /// Opens a socket on `class/name` for `opener`, here or on its owner.
    async fn open_socket<B: Send + 'static>(
        &self,
        class: &str,
        name: &str,
        mut req: Request<B>,
        opener: Opener,
    ) -> Response<HyperOutgoingBody> {
        req.headers_mut().remove(crate::cluster::APP);
        if !read(&self.inner.classes).contains_key(class) {
            return text_response(StatusCode::NOT_FOUND, "no such actor class\n");
        }
        let address = match ActorAddress::new(&self.inner.tenant, class, name) {
            Ok(a) => a,
            Err(e) => return text_response(StatusCode::BAD_REQUEST, &format!("{e}\n")),
        };
        if self.inner.cluster.as_ref().is_some_and(|c| {
            c.restores()
                .iter()
                .any(|r| r.class == class && r.name == name)
        }) {
            let mut response =
                text_response(StatusCode::SERVICE_UNAVAILABLE, "actor restore pending\n");
            response.headers_mut().insert(
                hyper::header::RETRY_AFTER,
                hyper::header::HeaderValue::from_static("1"),
            );
            return response;
        }
        let header = |name: &str| {
            req.headers()
                .get(name)
                .and_then(|v| v.to_str().ok())
                .map(str::to_owned)
        };
        let is_upgrade = req.method() == hyper::Method::GET
            && header("upgrade").is_some_and(|v| v.eq_ignore_ascii_case("websocket"))
            && header("sec-websocket-version").as_deref() == Some("13");
        let Some(key) = header("sec-websocket-key").filter(|_| is_upgrade) else {
            return text_response(StatusCode::BAD_REQUEST, "bad websocket upgrade\n");
        };
        // ADR 0010. A client's socket is outside (actors cannot open one);
        // a relayed upgrade is checked again on the owner, where the actor
        // is resident, so that check is immediate. The front's hand-off is
        // inside (ADR 0027).
        let outside = opener == Opener::Outside;
        if outside && self.early_rule(class, address.name()) == crate::access::Access::Internal {
            crate::metrics::refused(crate::metrics::Refusal::Access);
            return text_response(StatusCode::FORBIDDEN, "not reachable from outside\n");
        }
        // An upgrade from outside can wait for a dead node's actor to be
        // taken over (`ActorService::hold`): nothing of it was sent on, and
        // nothing of a body is read or buffered while it is kept. Its
        // deadline counts from now.
        let deadline = self
            .inner
            .cluster
            .as_ref()
            .and_then(|cluster| cluster.failover.deadline_ms())
            .filter(|_| !crate::cluster::never_forward(&req));
        let kind = crate::cluster::Kept::WebSocket;
        let mut ticket = None;
        // Once, unless the upgrade is kept: then again from here each time
        // it asks for the actor.
        loop {
            if let Some(owner) = self.inner.remote_owner(&address).await
                && let Some(cluster) = &self.inner.cluster
            {
                match self
                    .hold(cluster, &mut ticket, &owner, &address, deadline, kind)
                    .await
                {
                    Held::Retry => continue,
                    Held::Answer(response) => return response,
                    Held::Forward => {}
                }
                let response = cluster.forward_upgrade(&owner, req, opener).await;
                if response.status().is_server_error() {
                    self.inner.forget_owner(&address);
                }
                return response;
            }
            // Check admission before sending 101, including open classes. A
            // later eviction race is reported as WebSocket try-again (1013).
            let probe = self
                .inner
                .mailboxes
                .submit_wait(&address, ADMISSION_WAIT, move |state: &mut ActorState| {
                    Box::pin(async move { state.admit(outside).await })
                })
                .await;
            match probe {
                Ok(Ok(Admitted::Yes)) => {}
                Ok(Ok(Admitted::Missing)) => {
                    crate::metrics::refused(crate::metrics::Refusal::Access);
                    return text_response(StatusCode::NOT_FOUND, "no such actor\n");
                }
                Ok(Ok(Admitted::Forbidden)) => {
                    crate::metrics::refused(crate::metrics::Refusal::Access);
                    return text_response(StatusCode::FORBIDDEN, "not reachable from outside\n");
                }
                Err(MailboxError::Capacity(_)) => {
                    crate::metrics::refused(crate::metrics::Refusal::Capacity);
                    return capacity_response();
                }
                Err(MailboxError::Full(_)) => {
                    crate::metrics::refused(crate::metrics::Refusal::Overloaded);
                    return crate::cluster::unavailable("actor unavailable\n");
                }
                _ => {
                    crate::metrics::refused(crate::metrics::Refusal::Unavailable);
                    return crate::cluster::unavailable("actor unavailable\n");
                }
            }
            // In a cluster the socket must live on the actor's owner: find out
            // (acquiring the lease if it is free), and relay to the owner if it
            // is another node.
            if let Some(cluster) = &self.inner.cluster {
                let probe = self
                    .inner
                    .mailboxes
                    .submit_wait(&address, ADMISSION_WAIT, |state: &mut ActorState| {
                        Box::pin(async move {
                            match state.ensure_owner().await {
                                Ok(()) => {
                                    // The socket may never open (the client
                                    // gone, the node full): no event would arm
                                    // the actor's alarm here.
                                    state.arm_committed_alarm().await;
                                    Ok(())
                                }
                                Err(OwnershipError::NotOwner { owner, .. }) => {
                                    Err(Some((owner, false)))
                                }
                                Err(OwnershipError::Elsewhere { peer }) => Err(Some((peer, true))),
                                Err(
                                    OwnershipError::Unavailable(_)
                                    | OwnershipError::Refused(_)
                                    | OwnershipError::Broken(_),
                                ) => Err(None),
                            }
                        })
                    })
                    .await;
                match probe {
                    Ok(Ok(())) => {
                        if let Some(ticket) = ticket.take() {
                            ticket.end("served");
                        }
                    }
                    Ok(Err(Some((owner, false)))) => {
                        self.inner.mailboxes.hibernate(&address);
                        self.inner.remember_owner(&address, &owner);
                        match self
                            .hold(cluster, &mut ticket, &owner, &address, deadline, kind)
                            .await
                        {
                            Held::Retry => continue,
                            Held::Answer(response) => return response,
                            Held::Forward => {}
                        }
                        return cluster.forward_upgrade(&owner, req, opener).await;
                    }
                    Ok(Err(Some((peer, true)))) => {
                        self.inner.mailboxes.hibernate(&address);
                        if let Some(ticket) = ticket.take() {
                            ticket.end("forwarded");
                        }
                        // A hand-off this node does not send (the peer is
                        // of an older version) says nothing of the peer.
                        let sent = !crate::cluster::never_forward(&req)
                            && (opener == Opener::Outside || cluster.can_handoff(&peer));
                        let response = cluster.forward_upgrade(&peer, req, opener).await;
                        if sent && response.status().is_server_error() {
                            cluster.suspect(&peer);
                        }
                        return response;
                    }
                    _ => {
                        crate::metrics::refused(crate::metrics::Refusal::Unavailable);
                        let mut r =
                            text_response(StatusCode::SERVICE_UNAVAILABLE, "actor unavailable\n");
                        r.headers_mut().insert(
                            hyper::header::RETRY_AFTER,
                            hyper::header::HeaderValue::from_static("1"),
                        );
                        return r;
                    }
                }
            }
            break;
        }
        if let Err(e) = self.inner.sockets.can_open(&address) {
            tracing::debug!(app = %crate::logging::app(&address), actor = %crate::logging::actor(&address), error = ?e, "websocket refused");
            crate::metrics::refused(crate::metrics::Refusal::Capacity);
            let mut r = text_response(StatusCode::SERVICE_UNAVAILABLE, "too many connections\n");
            r.headers_mut().insert(
                hyper::header::RETRY_AFTER,
                hyper::header::HeaderValue::from_static("5"),
            );
            return r;
        }
        let accept = tokio_tungstenite::tungstenite::handshake::derive_accept_key(key.as_bytes());
        let path = req
            .uri()
            .path_and_query()
            .map_or_else(|| "/".to_owned(), ToString::to_string);

        // Relayed by a node over the cluster port: cut once that node's pin
        // stops being live (ADR 0011).
        let revoked = req
            .extensions()
            .get::<crate::http::ArrivedOnClusterPort>()
            .map(|arrived| {
                let peer = arrived.0.clone();
                crate::cluster_tls::until_untrusted(move || peer.is_live())
            });
        // Resolves once the 101 is sent, or fails if this request ends any
        // other way (dropped by a client hanging up, or answered 503).
        let on_upgrade = hyper::upgrade::on(&mut req);
        // Open the socket before answering 101 (registered, `on-websocket-open`
        // run), so the actor's next event sees it. One task owns the socket
        // from here to its end: the client may hang up while the hook waits,
        // which drops this request, but a queued hook still runs, so only
        // that task can close the socket (1006) and free its slot. Only the
        // outcome comes back here.
        let (opened_tx, opened_rx) = tokio::sync::oneshot::channel();
        let inner = Arc::clone(&self.inner);
        let opening = address.clone();
        tokio::spawn(async move {
            let accepted = match inner.accept_socket(opening.clone(), path, opener).await {
                Ok(accepted) => accepted,
                Err(e) => {
                    let _ = opened_tx.send(Err(NotOpened::Full(e)));
                    return;
                }
            };
            // A hand-off the actor refused is answered with an HTTP error,
            // never a socket that opens and closes (ADR 0027).
            if opener == Opener::Front
                && let Some(refused) = accepted.refused()
            {
                inner.discard_refused(accepted);
                let _ = opened_tx.send(Err(NotOpened::Refused(refused)));
                return;
            }
            let _ = opened_tx.send(Ok(()));
            let upgraded = match on_upgrade.await {
                Ok(u) => u,
                Err(e) => {
                    tracing::debug!(app = %crate::logging::app(&opening), actor = %crate::logging::actor(&opening), error = %e, "websocket upgrade failed");
                    inner.abandon_socket(accepted).await;
                    return;
                }
            };
            let ws = tokio_tungstenite::WebSocketStream::from_raw_socket(
                crate::cluster_tls::Revocable::new(hyper_util::rt::TokioIo::new(upgraded), revoked),
                tokio_tungstenite::tungstenite::protocol::Role::Server,
                Some(crate::websocket::config()),
            )
            .await;
            inner.run_socket(accepted, ws).await;
        });
        match opened_rx.await {
            Ok(Ok(())) => {}
            Ok(Err(NotOpened::Refused(refused))) => {
                return match refused {
                    Refused::Guest(reason) => {
                        crate::metrics::refused(crate::metrics::Refusal::Access);
                        text_response(StatusCode::FORBIDDEN, &format!("{reason}\n"))
                    }
                    Refused::Busy => {
                        crate::metrics::refused(crate::metrics::Refusal::Capacity);
                        capacity_response()
                    }
                    Refused::Failed => {
                        crate::metrics::refused(crate::metrics::Refusal::Unavailable);
                        crate::cluster::unavailable("actor unavailable\n")
                    }
                };
            }
            Ok(Err(NotOpened::Full(e))) => {
                // Over a cap after all: others raced in since `can_open`.
                // Nothing was registered.
                tracing::debug!(app = %crate::logging::app(&address), actor = %crate::logging::actor(&address), error = ?e, "websocket refused");
                crate::metrics::refused(crate::metrics::Refusal::Capacity);
                let mut r =
                    text_response(StatusCode::SERVICE_UNAVAILABLE, "too many connections\n");
                r.headers_mut().insert(
                    hyper::header::RETRY_AFTER,
                    hyper::header::HeaderValue::from_static("5"),
                );
                return r;
            }
            Err(_) => {
                // The task panicked.
                return text_response(StatusCode::INTERNAL_SERVER_ERROR, "websocket open failed\n");
            }
        }

        let mut response = text_response(StatusCode::SWITCHING_PROTOCOLS, "");
        let headers = response.headers_mut();
        headers.remove(hyper::header::CONTENT_TYPE);
        headers.insert(
            hyper::header::UPGRADE,
            hyper::header::HeaderValue::from_static("websocket"),
        );
        headers.insert(
            hyper::header::CONNECTION,
            hyper::header::HeaderValue::from_static("upgrade"),
        );
        if let Ok(v) = hyper::header::HeaderValue::from_str(&accept) {
            headers.insert("sec-websocket-accept", v);
        }
        response
    }
}
