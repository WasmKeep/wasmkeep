//! WebSocket upgrades: `GET /actor/<class>/<name>/...` with an upgrade
//! header, accepted by the actor's `on-websocket-open` before 101.

use super::requests::Held;
use super::*;

impl ActorService {
    /// Accepts a WebSocket upgrade for `/actor/<class>/<name><path>`: answers
    /// 101 and runs the socket in the background; `on-websocket-open` decides
    /// whether it stays open.
    pub async fn upgrade(
        &self,
        class: &str,
        name: &str,
        mut req: Request<Incoming>,
    ) -> Response<HyperOutgoingBody> {
        if !read(&self.inner.classes).contains_key(class) {
            return text_response(StatusCode::NOT_FOUND, "no such actor class\n");
        }
        let address = match ActorAddress::new(&self.inner.tenant, class, name) {
            Ok(a) => a,
            Err(e) => return text_response(StatusCode::BAD_REQUEST, &format!("{e}\n")),
        };
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
        // ADR 0010. Sockets always come from outside (actors cannot open
        // one); a relayed upgrade is checked again on the owner, where the
        // actor is resident, so that check is immediate.
        if self.early_rule(class, address.name()) == crate::access::Access::Internal {
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
                let response = cluster.forward_upgrade(&owner, req).await;
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
                    Box::pin(async move { state.admit(true).await })
                })
                .await;
            match probe {
                Ok(Ok(Admitted::Yes)) => {}
                Ok(Ok(Admitted::Missing)) => {
                    return text_response(StatusCode::NOT_FOUND, "no such actor\n");
                }
                Ok(Ok(Admitted::Forbidden)) => {
                    return text_response(StatusCode::FORBIDDEN, "not reachable from outside\n");
                }
                Err(MailboxError::Capacity(_)) => return capacity_response(),
                _ => return text_response(StatusCode::SERVICE_UNAVAILABLE, "actor unavailable\n"),
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
                        return cluster.forward_upgrade(&owner, req).await;
                    }
                    Ok(Err(Some((peer, true)))) => {
                        self.inner.mailboxes.hibernate(&address);
                        if let Some(ticket) = ticket.take() {
                            ticket.end("forwarded");
                        }
                        let sent = !crate::cluster::never_forward(&req);
                        let response = cluster.forward_upgrade(&peer, req).await;
                        if sent && response.status().is_server_error() {
                            cluster.suspect(&peer);
                        }
                        return response;
                    }
                    _ => {
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
            tracing::debug!(actor = %address, error = ?e, "websocket refused");
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
            let accepted = match inner.accept_socket(opening.clone(), path).await {
                Ok(accepted) => accepted,
                Err(e) => {
                    let _ = opened_tx.send(Err(e));
                    return;
                }
            };
            let _ = opened_tx.send(Ok(()));
            let upgraded = match on_upgrade.await {
                Ok(u) => u,
                Err(e) => {
                    tracing::debug!(actor = %opening, error = %e, "websocket upgrade failed");
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
            Ok(Err(e)) => {
                // Over a cap after all: others raced in since `can_open`.
                // Nothing was registered.
                tracing::debug!(actor = %address, error = ?e, "websocket refused");
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
