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

/// On an answer given before the event reached the actor's guest, which
/// a placed event's receiver turns into a refusal (ADR 0041, section 6):
/// no slot, or the node refused to run it. Never sent.
#[derive(Debug, Clone, Copy)]
pub(super) struct NotRun;

fn not_run(mut response: Response<HyperOutgoingBody>) -> Response<HyperOutgoingBody> {
    response.extensions_mut().insert(NotRun);
    response
}

/// Whether an outside request may be placed (ADR 0041, section 1): it
/// did not come from another node, and its body is absent or declared at
/// [`crate::cluster::MAX_PLACED_BODY`] or less, so it can be read into
/// memory and run here after a refusal. A chunked body streams: never.
fn placeable_request<B: hyper::body::Body>(req: &Request<B>) -> bool {
    !crate::cluster::never_forward(req)
        && !req.headers().contains_key(hyper::header::TRANSFER_ENCODING)
        && (req.body().is_end_stream()
            || req
                .body()
                .size_hint()
                .exact()
                .is_some_and(|n| n <= crate::cluster::MAX_PLACED_BODY))
}

/// Removes every name of the host's `x-wasmkeep-` range from a request
/// from outside but the forwarded mark ([`crate::cluster::FORWARDED`]).
fn strip_outside_headers(headers: &mut hyper::HeaderMap) {
    let names: Vec<hyper::header::HeaderName> = headers
        .keys()
        .filter(|n| is_wasmkeep_header(n.as_str()) && n.as_str() != crate::cluster::FORWARDED)
        .cloned()
        .collect();
    for name in names {
        headers.remove(name);
    }
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
        // A node's signed request with the hand-off's upgrade mark is a
        // relayed upgrade whose `Upgrade` header went missing (ADR 0027):
        // never a call, whatever it signs.
        if signed_body.is_some() && req.headers().contains_key(crate::cluster::UPGRADE_MARK) {
            crate::metrics::refused(crate::metrics::Refusal::Access);
            return text_response(StatusCode::FORBIDDEN, "a hand-off's relay is not a call\n");
        }
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
            return crate::cluster::refuse_if_placed(req.headers(), refused);
        }
        // A placed wake-up (ADR 0041, section 8): its mark is believed only
        // under a signature that checked (a call's, or the forward's
        // proof, which signs the app read here, before it is removed).
        // Unchecked, it only says what the forwarded mark says: never
        // forwarded again, never placed.
        let placed = self
            .inner
            .cluster
            .as_ref()
            .and_then(|cluster| cluster.placed_mark(&req, signed_body.as_ref()));
        req.headers_mut().remove(crate::cluster::APP);
        if !read(&self.inner.classes).contains_key(class) {
            // A placed wake-up for a class this node does not serve yet (a
            // deploy adding it is still on its way here): the sender, which
            // serves it, runs it (review item 1).
            if placed.is_some() {
                crate::metrics::placement_received(&self.inner.app_labels, "refused");
                return crate::cluster::placed_refusal("no such actor class here yet\n");
            }
            return text_response(StatusCode::NOT_FOUND, "no such actor class\n");
        }
        let address = match ActorAddress::new(&self.inner.tenant, class, name) {
            Ok(a) => a,
            Err(e) => return text_response(StatusCode::BAD_REQUEST, &format!("{e}\n")),
        };
        // A request from outside (or forwarded by a node, unsigned) carries
        // nothing of the host's range to the guest, as at the front's edge
        // (ADR 0023): a client cannot set the SDKs' key-value or queue
        // names an open actor might relay, nor any other. The forwarded
        // mark stays: the host reads it below, so the request is never
        // forwarded twice.
        if !internal {
            strip_outside_headers(req.headers_mut());
        }
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
        // The secret never reaches a guest, nor a hand-off's mark (ADR
        // 0027), which only relayed upgrades carry.
        // The caller stays: the guest reads it (checked above).
        for name in crate::cluster::HOP_PROOFS {
            if name != CALLER {
                req.headers_mut().remove(name);
            }
        }

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
            if let (Some(mark), Some(cluster)) = (&placed, &self.inner.cluster) {
                return self
                    .run_placed(cluster, address, req, chain, outside, mark)
                    .await;
            }
            // An outside request for an actor nobody holds may be placed on
            // a lighter node (ADR 0041), from the same single read routing
            // makes anyway.
            let mut routed = None;
            if let Some(cluster) = &self.inner.cluster
                && cluster.placement_on()
                && placeable_request(&req)
            {
                let route = self.inner.cold_route(&address).await;
                if route == Cold::Nobody {
                    match self
                        .place_request(cluster, &address, req, &chain, outside)
                        .await
                    {
                        Ok(response) => return response,
                        Err(back) => req = *back,
                    }
                }
                routed = Some(route);
            }
            loop {
                let own = !crate::cluster::never_forward(&req);
                let handback = match self
                    .dispatch_routed(
                        address.clone(),
                        req,
                        Arc::clone(&chain),
                        outside,
                        routed.take(),
                        own,
                    )
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
                            // A 5xx its actor answered is the actor's, not
                            // the node's.
                            let served = response.headers().contains_key(SERVED_BY);
                            if elsewhere && sent && !served {
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
        if let (Some(mark), Some(cluster)) = (&placed, &self.inner.cluster) {
            return self
                .run_placed(cluster, address, req, chain, outside, mark)
                .await;
        }
        match self.dispatch(address, req, chain, outside).await {
            Ok(response) => response,
            // A call is never forwarded twice (the calling node sent it to
            // the owner it saw); the calling actor gets the 503.
            Err(_) => crate::cluster::unavailable("actor ownership is changing; retry\n"),
        }
    }

    /// An outside request for `address`, which nobody holds (ADR 0041):
    /// placed on a lighter node if the rule says so, and that node's
    /// answer returned, or run here after a refusal. `Err` hands the
    /// request back, untouched, to run here as usual.
    async fn place_request(
        &self,
        cluster: &crate::cluster::Cluster,
        address: &ActorAddress,
        req: Request<Incoming>,
        chain: &Chain,
        outside: bool,
    ) -> Result<Response<HyperOutgoingBody>, Box<Request<Incoming>>> {
        use crate::cluster::{Choice, Hop};
        let app = &self.inner.app_labels;
        // The forward's proof needs the client's address (ADR 0038).
        let signable = req.extensions().get::<crate::http::PeerAddress>().is_some();
        // Asked first, charging nothing: the body is read only for a
        // wake-up that would be placed, and the credit is charged once it
        // has arrived (review item 4). Kept, the request streams as ever.
        if !cluster.would_place(address, super::load::rooms(&self.inner), signable) {
            if let Choice::Keep(reason) =
                cluster.choose_placement(address, super::load::rooms(&self.inner), signable)
            {
                crate::metrics::placement_kept(app, reason);
            }
            return Err(Box::new(req));
        }
        // Read into memory, so that a refusal can run it here (section 6).
        // No deadline of its own: the server's limits apply, as to a body
        // streamed to the actor here.
        let (parts, body) = req.into_parts();
        let max = usize::try_from(crate::cluster::MAX_PLACED_BODY).unwrap_or(usize::MAX);
        let read = http_body_util::BodyExt::collect(http_body_util::Limited::new(body, max));
        let Ok(body) = read.await else {
            return Ok(text_response(
                StatusCode::BAD_REQUEST,
                "request body unreadable or too large\n",
            ));
        };
        let body = body.to_bytes();
        let (node, fill) =
            match cluster.choose_placement(address, super::load::rooms(&self.inner), signable) {
                Choice::Place { node, fill } => (node, fill),
                // Its numbers moved while the body arrived: run here.
                Choice::Keep(reason) => {
                    crate::metrics::placement_kept(app, reason);
                    return Ok(self
                        .run_buffered(cluster, address, parts, body, chain, outside)
                        .await);
                }
                Choice::NotPlaceable => {
                    return Ok(self
                        .run_buffered(cluster, address, parts, body, chain, outside)
                        .await);
                }
            };
        let hop = Request::from_parts(parts.clone(), http_body_util::Full::new(body.clone()));
        let sent = cluster.forward_placed(&node, hop, fill).await;
        crate::metrics::placement_sent(app, "request", sent.outcome());
        Ok(match sent {
            Hop::Answered(response) => {
                // It ran there: the next requests skip the record read. An
                // access answer names no node and sets no hint.
                if response
                    .headers()
                    .get(SERVED_BY)
                    .is_some_and(|v| v.as_bytes() == node.as_bytes())
                {
                    self.inner.remember_owner(address, &node);
                }
                response
            }
            Hop::Refused | Hop::Unsent => {
                cluster.placement_backoff(&node);
                self.run_buffered(cluster, address, parts, body, chain, outside)
                    .await
            }
            Hop::Failed(why) => {
                cluster.placement_backoff(&node);
                tracing::warn!(node, error = %why, "a placed request's hop failed");
                // It may have run there: never run again here, and the
                // client is told the outcome is unknown.
                crate::cluster::outcome_unknown("the actor's node did not answer")
            }
        })
    }

    /// A placed request that did not run on its chosen node, run here by
    /// the ordinary path (ADR 0041, section 6): the record read again,
    /// then acquired, or forwarded once to an owner that appeared.
    async fn run_buffered(
        &self,
        cluster: &crate::cluster::Cluster,
        address: &ActorAddress,
        parts: hyper::http::request::Parts,
        body: bytes::Bytes,
        chain: &Chain,
        outside: bool,
    ) -> Response<HyperOutgoingBody> {
        let full = http_body_util::BodyExt::map_err(
            http_body_util::Full::new(body.clone()),
            |never: std::convert::Infallible| -> wasmtime_wasi_http::Error { match never {} },
        );
        let req = Request::from_parts(parts, full);
        match self
            .dispatch_routed(address.clone(), req, Arc::clone(chain), outside, None, true)
            .await
        {
            Ok(response) => response,
            Err(handback) => {
                let Handback {
                    node,
                    req,
                    elsewhere,
                } = *handback;
                let (parts, _) = req.into_parts();
                let req = Request::from_parts(parts, http_body_util::Full::new(body));
                let response = cluster.forward_buffered(&node, req).await;
                if response.status().is_server_error() {
                    if elsewhere {
                        if !response.headers().contains_key(SERVED_BY) {
                            cluster.suspect(&node);
                        }
                    } else {
                        self.inner.forget_owner(address);
                    }
                }
                response
            }
        }
    }

    /// A placed event that arrived (ADR 0041, sections 5 and 6): run here
    /// if this node takes it, never forwarded or placed again; otherwise
    /// refused with the mark, and its sender runs it.
    async fn run_placed<B>(
        &self,
        cluster: &crate::cluster::Cluster,
        address: ActorAddress,
        req: Request<B>,
        chain: Chain,
        outside: bool,
        mark: &crate::cluster::Mark,
    ) -> Response<HyperOutgoingBody>
    where
        B: hyper::body::Body<Data = bytes::Bytes> + Send + 'static,
        B::Error: Into<wasmtime_wasi_http::Error>,
    {
        use wasmkeep_cluster::placement::Arrival;
        let app = &self.inner.app_labels;
        let refuse = |outcome: &str, text: &str| {
            crate::metrics::placement_received(app, outcome);
            crate::cluster::placed_refusal(text)
        };
        if cluster.draining() || cluster.secrets_blocked() || cluster.move_names(&address) {
            return refuse("refused", "this node does not take this actor now; retry\n");
        }
        // Kept apart from a companion this node holds (ADR 0041, amendment
        // of 2026-11-22): its sender runs it, or sends it apart.
        if self.inner.companion_here(cluster, &address) {
            return refuse("apart", "this node holds the actor's companion\n");
        }
        let reservation = match cluster.arrive(mark, || super::load::rooms(&self.inner)) {
            Ok(reservation) => reservation,
            Err(Arrival::NoReturn) => {
                return refuse("returned", "this node placed an actor there just now\n");
            }
            Err(Arrival::Capacity) => {
                return refuse("fuller", "this node is not the lighter one\n");
            }
            Err(Arrival::InvalidMark | Arrival::Accept) => {
                return refuse("refused", "the placed mark is not valid\n");
            }
        };
        match self.dispatch(address, req, chain, outside).await {
            // Another holder, or this node draining: it did not run.
            Err(_) => refuse("refused", "another node holds the actor; retry\n"),
            Ok(response) if response.extensions().get::<NotRun>().is_some() => {
                crate::metrics::placement_received(app, "refused");
                crate::cluster::mark_refused(response)
            }
            Ok(response) => {
                // Handed to the guest: acquired here. An access answer
                // (403, 404) is the client's and carries no mark.
                // An actor error (500) ran here too (review item 8).
                if response.headers().contains_key(SERVED_BY)
                    || response.status() == StatusCode::INTERNAL_SERVER_ERROR
                {
                    reservation.accepted();
                    crate::metrics::placement_received(app, "acquired");
                }
                response
            }
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
        self.dispatch_routed(address, req, chain, outside, None, false)
            .await
    }

    /// [`Self::dispatch`], with the route already read for this request
    /// (`routed`), so that a placement's decision costs no second read.
    /// `own`: a wake-up of this node's own, which an actor kept apart from
    /// a companion held here does not take (ADR 0041, amendment of
    /// 2026-11-22); `false` for an event another node sent.
    pub(super) async fn dispatch_routed<B>(
        &self,
        address: ActorAddress,
        mut req: Request<B>,
        chain: Chain,
        outside: bool,
        routed: Option<Cold>,
        own: bool,
    ) -> Result<Response<HyperOutgoingBody>, Box<Handback<B>>>
    where
        B: hyper::body::Body<Data = bytes::Bytes> + Send + 'static,
        B::Error: Into<wasmtime_wasi_http::Error>,
    {
        let route = match routed {
            Some(route) => route,
            None => self.inner.cold_route(&address).await,
        };
        if let Cold::Owner(owner) = route {
            return Err(Box::new(Handback {
                node: owner,
                req,
                elsewhere: false,
            }));
        }
        if let Some(response) = super::deploys::finish_ingress(&mut req) {
            return Ok(response);
        }
        let (response_tx, response_rx) = oneshot::channel();
        let event = self.inner.mailboxes.submit_wait_timed(
            &address,
            ADMISSION_WAIT,
            move |state: &mut ActorState, waited| {
                state.record_mailbox_wait(waited);
                Box::pin(async move {
                    state
                        .owned_request(req, response_tx, chain, outside, own)
                        .await;
                })
            },
        );

        // The response is available as soon as the guest sets it; the event
        // itself keeps running (streaming the body) inside the mailbox.
        let event = tokio::spawn(event);
        match response_rx.await {
            Ok(Outcome::Served(mut response)) => {
                // The host's mark only (ADR 0041): a guest cannot make a
                // placed event's sender run it a second time.
                response.headers_mut().remove(crate::cluster::PLACED);
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
                tracing::warn!(app = %crate::logging::named(&self.inner.app_name), actor = %crate::logging::actor(&address), error = %e, "event failed");
                Ok(text_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "actor error\n",
                ))
            }
            Ok(Outcome::Refused(why)) => {
                tracing::debug!(app = %crate::logging::named(&self.inner.app_name), actor = %crate::logging::actor(&address), %why, "event refused");
                crate::metrics::refused(crate::metrics::Refusal::Unavailable);
                Ok(not_run(crate::cluster::unavailable(
                    "this node cannot run the actor now; retry\n",
                )))
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
                    not_run(capacity_response())
                }
                Ok(Err(MailboxError::Full(_))) => {
                    crate::metrics::refused(crate::metrics::Refusal::Overloaded);
                    text_response(StatusCode::TOO_MANY_REQUESTS, "actor overloaded\n")
                }
                Ok(Err(e)) => {
                    tracing::warn!(app = %crate::logging::named(&self.inner.app_name), actor = %crate::logging::actor(&address), error = %e, "event not run");
                    crate::metrics::refused(crate::metrics::Refusal::Unavailable);
                    // Not run (the mailboxes closed: a stop): a placed
                    // event's sender runs it (review item 2).
                    not_run(text_response(
                        StatusCode::SERVICE_UNAVAILABLE,
                        "actor unavailable\n",
                    ))
                }
                // The event started and ended with no response: cut by
                // its wall or a lease lapse, or a trap. A durable write in
                // it may have landed: the outcome is unknown.
                _ => crate::cluster::mark_unknown(text_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "actor error\n",
                )),
            }),
        }
    }
}
