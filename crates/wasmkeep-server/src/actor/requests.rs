//! Requests: routing `/actor/<class>/<name>/...` to the actor's owner and
//! running it through the actor's mailbox.

use super::*;

/// What to do with a request another node holds ([`ActorService::hold`]).
pub(super) enum Held {
    /// Kept, and time to ask for the actor again.
    Retry,
    /// Not kept, or no longer: sent to the node as without the wait.
    Forward,
    /// Kept in vain: this answer.
    Answer(Response<HyperOutgoingBody>),
}

impl ActorService {
    /// Handles `/actor/<class>/<name><rest>`: `rest` (with the query) is the
    /// path the guest sees. Never fails; errors become HTTP statuses.
    pub async fn handle(
        &self,
        class: &str,
        name: &str,
        mut req: Request<Incoming>,
    ) -> Response<HyperOutgoingBody> {
        // Only `actors.call` says which actor is calling: locally, or from
        // another node of the cluster proving it with the cluster secret
        // (in refuse mode, only on the cluster port: ADR 0011, phase B).
        // A client claiming either is not believed.
        let on_cluster_port = req
            .extensions()
            .get::<crate::http::ArrivedOnClusterPort>()
            .is_some();
        let signed_body = match &self.inner.cluster {
            Some(cluster) => {
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
            }
            None => None,
        };
        let internal = signed_body.is_some();
        // A hop from a node of another app (or of an older version) is
        // refused before anything else, its body unread (ADR 0022). The
        // header never reaches a guest.
        if let Some(refused) = self
            .inner
            .cluster
            .as_ref()
            .and_then(|cluster| cluster.wrong_app(&req, internal))
        {
            return refused;
        }
        req.headers_mut().remove(crate::cluster::APP);
        if !read(&self.inner.classes).contains_key(class) {
            return text_response(StatusCode::NOT_FOUND, "no such actor class\n");
        }
        let address = match ActorAddress::new(&self.inner.tenant, class, name) {
            Ok(a) => a,
            Err(e) => return text_response(StatusCode::BAD_REQUEST, &format!("{e}\n")),
        };
        // Set by the host, never by the client: the same encoding as calls
        // (the name percent-encoded), so it is plain ASCII and unambiguous.
        req.headers_mut().remove("x-wasmkeep-actor");
        if let Ok(value) =
            hyper::header::HeaderValue::from_str(&crate::cluster::encode_address(&address))
        {
            req.headers_mut().insert("x-wasmkeep-actor", value);
        }
        // ADR 0010: outside requests may be refused, or kept from creating
        // actors. A request another node forwarded is checked again here
        // (it does not carry the cluster secret); the actor is resident on
        // its owner, so that check is immediate. Refused early here; the
        // event decides with the rules of the class it takes (ADR 0013).
        if !internal && self.early_rule(class, address.name()) == crate::access::Access::Internal {
            crate::metrics::refused(crate::metrics::Refusal::Access);
            return text_response(StatusCode::FORBIDDEN, "not reachable from outside\n");
        }
        let outside = !internal;
        let chain: Chain = if internal {
            req.headers()
                .get_all(CHAIN)
                .iter()
                .filter_map(|v| v.to_str().ok().and_then(decode_address))
                .collect::<Vec<_>>()
                .into()
        } else {
            req.headers_mut().remove(CALLER);
            Arc::from(Vec::new())
        };
        // Only a node's signed call may hand an alarm off (ADR 0014); the
        // marker never reaches a guest.
        let handoff = internal && req.headers().contains_key(ALARM_HANDOFF);
        req.headers_mut().remove(ALARM_HANDOFF);
        // The same for a take (ADR 0017).
        let take = internal && req.headers().contains_key(crate::cluster::TAKE);
        req.headers_mut().remove(crate::cluster::TAKE);
        // The secret never reaches a guest.
        req.headers_mut().remove(INTERNAL);
        req.headers_mut().remove(CHAIN);

        let Some(signed) = signed_body else {
            // A request from outside that can wait for a dead node's actor
            // to be taken over: a GET or HEAD whose body is known to be
            // empty, so nothing is buffered and nothing of it was sent
            // anywhere. Its deadline counts from now.
            let deadline = self
                .inner
                .cluster
                .as_ref()
                .and_then(|cluster| cluster.failover.deadline_ms())
                .filter(|_| {
                    matches!(*req.method(), hyper::Method::GET | hyper::Method::HEAD)
                        && hyper::body::Body::is_end_stream(req.body())
                        && !crate::cluster::never_forward(&req)
                });
            let mut ticket = None;
            let mut req = req;
            loop {
                let handback = match self
                    .dispatch(address.clone(), req, Arc::clone(&chain), outside)
                    .await
                {
                    Ok(response) => {
                        if let Some(ticket) = ticket.take() {
                            Self::kept_request_answered(ticket, &response);
                        }
                        return response;
                    }
                    Err(handback) => handback,
                };
                return match &self.inner.cluster {
                    Some(cluster) => {
                        let Handback {
                            node,
                            req: back,
                            elsewhere,
                        } = *handback;
                        let held = if elsewhere {
                            Held::Forward
                        } else {
                            self.hold(
                                cluster,
                                &mut ticket,
                                &node,
                                &address,
                                deadline,
                                crate::cluster::Kept::Request,
                            )
                            .await
                        };
                        match held {
                            Held::Retry => {
                                req = back;
                                continue;
                            }
                            Held::Answer(response) => return response,
                            Held::Forward => {}
                        }
                        if let Some(ticket) = ticket.take() {
                            ticket.end("forwarded");
                        }
                        let req = back;
                        // Already forwarded: answered 503 here, the peer
                        // never asked, so no reason to suspect it.
                        let sent = !crate::cluster::never_forward(&req);
                        let response = cluster.forward(&node, req).await;
                        if response.status().is_server_error() {
                            if elsewhere && sent {
                                // Down, unreachable, or draining itself: not
                                // chosen again for a while (ADR 0014).
                                cluster.suspect(&node);
                            } else if !elsewhere {
                                self.inner.forget_owner(&address);
                            }
                        }
                        response
                    }
                    None => text_response(StatusCode::INTERNAL_SERVER_ERROR, "actor error\n"),
                };
            }
        };
        // A node's call: its body must be the one it signed. Only a valid
        // signature gets this far, so only nodes make us buffer a body.
        let (parts, body) = req.into_parts();
        let max = usize::try_from(crate::http::MAX_BODY_BYTES).unwrap_or(usize::MAX);
        // Bounded in time too: the whole body is read before the actor runs.
        let read = http_body_util::BodyExt::collect(http_body_util::Limited::new(body, max));
        let Ok(Ok(body)) = tokio::time::timeout(SIGNED_BODY_TIMEOUT, read).await else {
            return text_response(
                StatusCode::BAD_REQUEST,
                "call body unreadable or too large\n",
            );
        };
        let body = body.to_bytes();
        if crate::cluster::body_sha256(&body) != signed.body {
            return text_response(
                StatusCode::FORBIDDEN,
                "the body does not match the node's signature\n",
            );
        }
        // Spent only now: a copy sent ahead with another body was refused
        // above without using up the real call's signature.
        let replayed = self
            .inner
            .cluster
            .as_ref()
            .is_none_or(|cluster| !cluster.first_use(&signed, handoff || take));
        if replayed {
            return text_response(StatusCode::FORBIDDEN, "signature already used\n");
        }
        if handoff {
            return self.accept_alarm(&address, &body);
        }
        if take {
            return self.accept_take(&address, &body).await;
        }
        let body = http_body_util::BodyExt::map_err(
            http_body_util::Full::new(body),
            |never: std::convert::Infallible| -> wasmtime_wasi_http::Error { match never {} },
        );
        let req = Request::from_parts(parts, body);
        match self.dispatch(address, req, chain, outside).await {
            Ok(response) => response,
            // A call is never forwarded twice (the calling node sent it to
            // the owner it saw); the calling actor gets the 503.
            Err(_) => crate::cluster::unavailable("actor ownership is changing; retry\n"),
        }
    }

    /// A request (or an upgrade) for `address` came back naming `owner`,
    /// another node. Keeps it if waiting for a takeover can serve it
    /// (`Cluster::keep`), and says what to do once the wait ends. `ticket`
    /// is the request's place while it is kept: `None` at first, and set
    /// on [`Held::Retry`]. `deadline`: `None` for a request that is never
    /// kept.
    pub(super) async fn hold<'a>(
        &self,
        cluster: &'a crate::cluster::Cluster,
        ticket: &mut Option<crate::cluster::Ticket<'a>>,
        owner: &str,
        address: &ActorAddress,
        deadline: Option<u64>,
        kind: crate::cluster::Kept,
    ) -> Held {
        use crate::cluster::Waited;
        let Some(deadline) = deadline else {
            return Held::Forward;
        };
        let mut kept = match ticket.take() {
            Some(mut kept) if kept.owner() == owner => {
                kept.refused();
                kept
            }
            // Another node took the actor: sent there, once.
            Some(kept) => {
                kept.end("forwarded");
                return Held::Forward;
            }
            None => match cluster.keep(owner, address, deadline, kind) {
                Some(kept) => kept,
                None => return Held::Forward,
            },
        };
        match kept.wait().await {
            Waited::Try => {
                // Asked afresh: not answered from what this node remembers.
                self.inner.forget_owner(address);
                *ticket = Some(kept);
                Held::Retry
            }
            Waited::Leave => Held::Forward,
            // What a request for an owner out of this node's view gets.
            Waited::GiveUp => Held::Answer(crate::cluster::unavailable(
                "the actor's node is not reachable; retry\n",
            )),
        }
    }

    /// A kept request was answered here: by its actor, or otherwise (the
    /// node full, the actor refused, a restore that failed).
    fn kept_request_answered(
        ticket: crate::cluster::Ticket<'_>,
        response: &Response<HyperOutgoingBody>,
    ) {
        ticket.end(if response.headers().contains_key(SERVED_BY) {
            "served"
        } else {
            "answered_other"
        });
    }

    /// Runs a request on the actor's mailbox. Hands the request back with
    /// the owner's id if another node owns the actor.
    pub(super) async fn dispatch<B>(
        &self,
        address: ActorAddress,
        req: Request<B>,
        chain: Chain,
        outside: bool,
    ) -> Result<Response<HyperOutgoingBody>, Box<Handback<B>>>
    where
        B: hyper::body::Body<Data = bytes::Bytes> + Send + 'static,
        B::Error: Into<wasmtime_wasi_http::Error>,
    {
        if let Some(owner) = self.inner.remote_owner(&address).await {
            return Err(Box::new(Handback {
                node: owner,
                req,
                elsewhere: false,
            }));
        }
        let (response_tx, response_rx) = oneshot::channel();
        let event = self.inner.mailboxes.submit_wait(
            &address,
            ADMISSION_WAIT,
            move |state: &mut ActorState| {
                Box::pin(async move {
                    state.owned_request(req, response_tx, chain, outside).await;
                })
            },
        );

        // The response is available as soon as the guest sets it; the event
        // itself keeps running (streaming the body) inside the mailbox.
        let event = tokio::spawn(event);
        match response_rx.await {
            Ok(Outcome::Served(mut response)) => {
                if let Some(cluster) = &self.inner.cluster
                    && let Ok(v) = hyper::header::HeaderValue::from_str(&cluster.node_id)
                {
                    response.headers_mut().insert(SERVED_BY, v);
                }
                Ok(response)
            }
            Ok(Outcome::NotOwner(owner, req)) => {
                self.inner.mailboxes.hibernate(&address);
                self.inner.remember_owner(&address, &owner);
                Err(Box::new(Handback {
                    node: owner,
                    req,
                    elsewhere: false,
                }))
            }
            Ok(Outcome::Elsewhere(peer, req)) => {
                self.inner.mailboxes.hibernate(&address);
                Err(Box::new(Handback {
                    node: peer,
                    req,
                    elsewhere: true,
                }))
            }
            Ok(Outcome::Failed(e)) => {
                tracing::warn!(actor = %address, error = %e, "event failed");
                Ok(text_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "actor error\n",
                ))
            }
            Ok(Outcome::Refused(why)) => {
                tracing::debug!(actor = %address, %why, "event refused");
                crate::metrics::refused(crate::metrics::Refusal::Unavailable);
                Ok(crate::cluster::unavailable(
                    "this node cannot run the actor now; retry\n",
                ))
            }
            Ok(Outcome::Missing) => {
                crate::metrics::refused(crate::metrics::Refusal::Access);
                Ok(text_response(StatusCode::NOT_FOUND, "no such actor\n"))
            }
            Ok(Outcome::Forbidden) => {
                crate::metrics::refused(crate::metrics::Refusal::Access);
                Ok(text_response(
                    StatusCode::FORBIDDEN,
                    "not reachable from outside\n",
                ))
            }
            Err(_) => Ok(match event.await {
                Ok(Err(MailboxError::Capacity(_))) => {
                    crate::metrics::refused(crate::metrics::Refusal::Capacity);
                    capacity_response()
                }
                Ok(Err(MailboxError::Full(_))) => {
                    crate::metrics::refused(crate::metrics::Refusal::Overloaded);
                    text_response(StatusCode::TOO_MANY_REQUESTS, "actor overloaded\n")
                }
                Ok(Err(e)) => {
                    tracing::warn!(actor = %address, error = %e, "event not run");
                    crate::metrics::refused(crate::metrics::Refusal::Unavailable);
                    text_response(StatusCode::SERVICE_UNAVAILABLE, "actor unavailable\n")
                }
                _ => text_response(StatusCode::INTERNAL_SERVER_ERROR, "actor error\n"),
            }),
        }
    }
}
