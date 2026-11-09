//! Finding an actor's owner and calls between actors (`actors.call`),
//! and turning an HTTP request into a call and back.

use super::*;

/// Largest request or response body for `actors.call`.
pub(super) const MAX_CALL_BODY: usize = 16 * 1024 * 1024;

impl ServiceInner {
    /// Route a cold address before it consumes a local actor slot. A live
    /// mailbox checks its own lease, preserving service during bucket
    /// outages. A missing/failed hint falls back to normal lease acquisition;
    /// the hint never authorizes execution or bypasses forwarding limits.
    ///
    /// A hint learned in the last `HINT_MS` is used without reading the
    /// bucket, as the per-actor hint was before relaying stopped keeping a
    /// local mailbox: a stale one costs one relay that the old owner sends
    /// back as "ownership is changing", which also drops the hint.
    pub(super) async fn remote_owner(&self, address: &ActorAddress) -> Option<String> {
        let cluster = self.cluster.as_ref()?;
        if self.mailboxes.contains(address) {
            return None;
        }
        if let Some(owner) = self.cached_owner(address) {
            return Some(owner);
        }
        let owner = tokio::time::timeout(ADMISSION_WAIT, cluster.other_owner(address))
            .await
            .ok()?
            .ok()??;
        self.remember_owner(address, &owner);
        Some(owner)
    }

    pub(super) fn owner_hints(
        &self,
    ) -> std::sync::MutexGuard<'_, HashMap<ActorAddress, (String, u64)>> {
        self.owner_hints.lock().unwrap_or_else(|p| p.into_inner())
    }

    pub(super) fn cached_owner(&self, address: &ActorAddress) -> Option<String> {
        let now = now_ms();
        let mut hints = self.owner_hints();
        match hints.get(address) {
            Some((owner, until)) if now < *until => Some(owner.clone()),
            Some(_) => {
                hints.remove(address);
                None
            }
            None => None,
        }
    }

    /// Records that `owner` (another node) holds `address`'s lease.
    pub(super) fn remember_owner(&self, address: &ActorAddress, owner: &str) {
        let now = now_ms();
        let mut hints = self.owner_hints();
        if hints.len() >= MAX_OWNER_HINTS {
            hints.retain(|_, (_, until)| now < *until);
            if hints.len() >= MAX_OWNER_HINTS {
                hints.clear();
            }
        }
        hints.insert(
            address.clone(),
            (owner.to_owned(), now.saturating_add(HINT_MS)),
        );
    }

    /// Drops the hint for `address` (a relay to it failed).
    pub(super) fn forget_owner(&self, address: &ActorAddress) {
        self.owner_hints().remove(address);
    }

    /// `actors.call`: delivers `request` to `class/name` in the caller's
    /// tenant and returns its buffered response.
    ///
    /// A call to an actor already waiting in this chain (including the
    /// caller) fails with `reentrant`. A cycle across two independent chains
    /// cannot be seen here and ends in `timeout` after the wall-clock limit.
    pub(super) async fn call(
        self: Arc<Self>,
        ctx: &CallContext,
        address: &str,
        request: CallRequest,
    ) -> Result<CallResponse, CallError> {
        let invalid = |m: &str| CallError::InvalidAddress(m.to_owned());
        let (class, name) = address
            .split_once('/')
            .ok_or_else(|| invalid("expected class/name"))?;
        if !read(&self.classes).contains_key(class) {
            return Err(invalid("no such actor class"));
        }
        let target = ActorAddress::new(ctx.me.tenant(), class, name)
            .map_err(|e| CallError::InvalidAddress(e.to_string()))?;
        if target == ctx.me || ctx.chain.contains(&target) {
            return Err(CallError::Reentrant);
        }
        if request.body.len() > MAX_CALL_BODY {
            return Err(CallError::TooLarge);
        }

        let req = build_request(&ctx.me, &target, request).map_err(CallError::Failed)?;
        let mut chain = ctx.chain.to_vec();
        chain.push(ctx.me.clone());
        let chain: Chain = Arc::from(chain);
        let waiting = Arc::clone(&chain);

        let call = async {
            if let Some(owner) = self.remote_owner(&target).await
                && let Some(cluster) = &self.cluster
            {
                return match cluster.call(&owner, &target, &waiting, req).await {
                    Ok(response) => collect_response(response).await,
                    Err(error) => {
                        self.forget_owner(&target);
                        Err(CallError::Failed(error))
                    }
                };
            }
            let (response_tx, response_rx) = oneshot::channel();
            let event = self.mailboxes.submit_wait(
                &target,
                ADMISSION_WAIT,
                move |state: &mut ActorState| {
                    Box::pin(
                        async move { state.owned_request(req, response_tx, chain, false).await },
                    )
                },
            );
            let event = tokio::spawn(event);

            match response_rx.await {
                Ok(Outcome::Served(response)) => collect_response(response).await,
                Ok(Outcome::Failed(e) | Outcome::Refused(e)) => Err(CallError::Failed(e)),
                Ok(Outcome::Missing) => Err(CallError::Failed("no such actor".into())),
                Ok(Outcome::Forbidden) => Err(CallError::Failed("refused".into())),
                Ok(Outcome::NotOwner(owner, req)) => {
                    self.mailboxes.hibernate(&target);
                    self.remember_owner(&target, &owner);
                    match &self.cluster {
                        Some(cluster) => match cluster.call(&owner, &target, &waiting, req).await {
                            Ok(response) => collect_response(response).await,
                            Err(e) => {
                                self.forget_owner(&target);
                                Err(CallError::Failed(e))
                            }
                        },
                        None => Err(CallError::Failed("no cluster".into())),
                    }
                }
                // Draining: the chosen peer claims (ADR 0014); not an owner
                // hint, and not chosen again for a while if it fails.
                Ok(Outcome::Elsewhere(peer, req)) => {
                    self.mailboxes.hibernate(&target);
                    match &self.cluster {
                        Some(cluster) => match cluster.call(&peer, &target, &waiting, req).await {
                            Ok(response) => collect_response(response).await,
                            Err(e) => {
                                cluster.suspect(&peer);
                                Err(CallError::Failed(e))
                            }
                        },
                        None => Err(CallError::Failed("no cluster".into())),
                    }
                }
                Err(_) => match event.await {
                    Ok(Err(MailboxError::Capacity(_))) => {
                        collect_response(capacity_response()).await
                    }
                    Ok(Err(MailboxError::Full(_))) => {
                        Err(CallError::Failed("callee overloaded".to_owned()))
                    }
                    Ok(Err(e)) => Err(CallError::Failed(e.to_string())),
                    _ => Err(CallError::Failed("callee unavailable".to_owned())),
                },
            }
        };
        tokio::time::timeout(self.wall, call)
            .await
            .unwrap_or(Err(CallError::Timeout))
    }
}

/// Headers `actors.call` drops from a guest's request.
pub(super) use crate::cluster::HOP_BY_HOP;

/// Longest a node's signed call may take to send its body.
pub(super) const SIGNED_BODY_TIMEOUT: Duration = Duration::from_secs(30);

pub(super) fn build_request(
    from: &ActorAddress,
    to: &ActorAddress,
    request: CallRequest,
) -> Result<Request<http_body_util::Full<bytes::Bytes>>, String> {
    if !request.path.starts_with('/') {
        return Err("path must start with /".into());
    }
    let mut builder = Request::builder()
        .method(request.method.as_str())
        .uri(request.path.as_str())
        .header(hyper::header::HOST, "actor.wasmkeep.internal");
    for (name, value) in &request.headers {
        // The host frames the body and the connection; a guest's framing
        // or upgrade headers would cut the body or reroute the call.
        if HOP_BY_HOP.iter().any(|h| name.eq_ignore_ascii_case(h)) {
            continue;
        }
        // Only the host hands alarms off or asks a node to take an actor
        // (ADR 0014, ADR 0017).
        if name.eq_ignore_ascii_case(crate::cluster::ALARM_HANDOFF)
            || name.eq_ignore_ascii_case(crate::cluster::TAKE)
        {
            continue;
        }
        builder = builder.header(name.as_str(), value.as_str());
    }
    let mut req = builder
        .body(http_body_util::Full::new(bytes::Bytes::from(request.body)))
        .map_err(|e| format!("invalid request: {e}"))?;
    // The same encoding as calls arriving from another node: the name
    // percent-encoded, so the value is unambiguous and plain ASCII.
    for (header, address) in [("x-wasmkeep-actor", to), ("x-wasmkeep-caller", from)] {
        if let Ok(value) = hyper::header::HeaderValue::from_str(&encode_address(address)) {
            req.headers_mut().insert(header, value);
        }
    }
    Ok(req)
}

pub(super) async fn collect_response(
    response: Response<HyperOutgoingBody>,
) -> Result<CallResponse, CallError> {
    use http_body_util::BodyExt;
    let (parts, body) = response.into_parts();
    let body = http_body_util::Limited::new(body, MAX_CALL_BODY)
        .collect()
        .await
        .map_err(|e| {
            if e.is::<http_body_util::LengthLimitError>() {
                CallError::TooLarge
            } else {
                CallError::Failed(format!("reading the response body: {e}"))
            }
        })?
        .to_bytes();
    let headers = parts
        .headers
        .iter()
        .filter_map(|(k, v)| Some((k.as_str().to_owned(), v.to_str().ok()?.to_owned())))
        .collect();
    Ok(CallResponse {
        status: parts.status.as_u16(),
        headers,
        body: body.to_vec(),
    })
}
