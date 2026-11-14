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

    /// The actor `address` (`class/name`) names in `tenant` (the caller's),
    /// for `actors.call`: `invalid-address` if it is malformed or its class
    /// is not served. `_front` is never a class (ADR 0023).
    pub(super) fn call_target(
        &self,
        tenant: &str,
        address: &str,
    ) -> Result<ActorAddress, CallError> {
        let invalid = |m: &str| CallError::InvalidAddress(m.to_owned());
        let (class, name) = address
            .split_once('/')
            .ok_or_else(|| invalid("expected class/name"))?;
        if !read(&self.classes).contains_key(class) {
            return Err(invalid("no such actor class"));
        }
        ActorAddress::new(tenant, class, name).map_err(|e| CallError::InvalidAddress(e.to_string()))
    }

    /// `actors.call`: delivers `request` to `target` (from
    /// [`call_target`](Self::call_target)) and returns its buffered response,
    /// or `timeout` after `limit`.
    ///
    /// A call to an actor already waiting in this chain (including the
    /// caller) fails with `reentrant`. A cycle across two independent chains
    /// cannot be seen here and ends in `timeout` after the wall-clock limit.
    /// The front is never in a chain: it cannot be called back.
    pub(super) async fn call(
        self: Arc<Self>,
        caller: &Caller,
        chain: &Chain,
        target: ActorAddress,
        request: CallRequest,
        limit: Duration,
    ) -> Result<CallResponse, CallError> {
        let me = match caller {
            Caller::Actor(me) => Some(me),
            Caller::Front => None,
        };
        if me == Some(&target) || chain.contains(&target) {
            return Err(CallError::Reentrant);
        }
        if request.body.len() > MAX_CALL_BODY {
            return Err(CallError::TooLarge);
        }

        let req = build_request(caller, &target, request).map_err(CallError::Failed)?;
        let mut chain = chain.to_vec();
        if let Some(me) = me {
            chain.push(me.clone());
        }
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
                Ok(Outcome::Failed(e)) => Err(CallError::Failed(e)),
                Ok(Outcome::Refused(e)) => {
                    crate::metrics::refused(crate::metrics::Refusal::Unavailable);
                    Err(CallError::Failed(e))
                }
                Ok(Outcome::Missing) => {
                    crate::metrics::refused(crate::metrics::Refusal::Access);
                    Err(CallError::Failed("no such actor".into()))
                }
                Ok(Outcome::Forbidden) => {
                    crate::metrics::refused(crate::metrics::Refusal::Access);
                    Err(CallError::Failed("refused".into()))
                }
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
                        crate::metrics::refused(crate::metrics::Refusal::Capacity);
                        collect_response(capacity_response()).await
                    }
                    Ok(Err(MailboxError::Full(_))) => {
                        crate::metrics::refused(crate::metrics::Refusal::Overloaded);
                        Err(CallError::Failed("callee overloaded".to_owned()))
                    }
                    Ok(Err(e)) => {
                        crate::metrics::refused(crate::metrics::Refusal::Unavailable);
                        Err(CallError::Failed(e.to_string()))
                    }
                    _ => Err(CallError::Failed("callee unavailable".to_owned())),
                },
            }
        };
        tokio::time::timeout(limit, call)
            .await
            .unwrap_or(Err(CallError::Timeout))
    }
}

/// Headers `actors.call` drops from a guest's request.
pub(super) use crate::cluster::HOP_BY_HOP;

/// Longest a node's signed call may take to send its body.
pub(super) const SIGNED_BODY_TIMEOUT: Duration = Duration::from_secs(30);

pub(super) fn build_request(
    from: &Caller,
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
        // The `x-wasmkeep-` range is the host's, for every caller (ADR
        // 0023): who is calling, the chain, the forwarded mark, the node
        // markers, hand-offs, takes, the app, and any name added later. A
        // local call reaches the callee without the stripping requests from
        // the port get, so nothing of it may come from a guest, but for
        // the SDKs' own protocol between actors.
        if is_wasmkeep_header(name) && !SDK_HEADERS.iter().any(|h| name.eq_ignore_ascii_case(h)) {
            continue;
        }
        builder = builder.header(name.as_str(), value.as_str());
    }
    let mut req = builder
        .body(http_body_util::Full::new(bytes::Bytes::from(request.body)))
        .map_err(|e| format!("invalid request: {e}"))?;
    // The same encoding as calls arriving from another node: the name
    // percent-encoded, so the value is unambiguous and plain ASCII.
    for (header, value) in [
        ("x-wasmkeep-actor", encode_address(to)),
        ("x-wasmkeep-caller", from.value(to.tenant())),
    ] {
        if let Ok(value) = hyper::header::HeaderValue::from_str(&value) {
            req.headers_mut().insert(header, value);
        }
    }
    Ok(req)
}

/// Whether `name` is in the host's `x-wasmkeep-` range (any case).
pub(crate) fn is_wasmkeep_header(name: &str) -> bool {
    name.get(..WASMKEEP_PREFIX.len())
        .is_some_and(|p| p.eq_ignore_ascii_case(WASMKEEP_PREFIX))
}

/// The names in the `x-wasmkeep-` range that guests send each other on
/// `actors.call`: the SDKs' key-value and queue protocol, in every SDK
/// since 0.1.0 (ADR 0023, a note under "Header hygiene"). The host gives
/// them no meaning. Exact names: any other name in the range is dropped.
const SDK_HEADERS: [&str; 4] = [
    "x-wasmkeep-kv-expect",
    "x-wasmkeep-kv-ttl-ms",
    "x-wasmkeep-kv-durable",
    "x-wasmkeep-queue-delay-ms",
];

/// The prefix of every header the host sets or reads itself.
const WASMKEEP_PREFIX: &str = "x-wasmkeep-";

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
