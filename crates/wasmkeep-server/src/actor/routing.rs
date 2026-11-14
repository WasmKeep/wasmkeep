//! Finding an actor's owner and calls between actors (`actors.call`),
//! and turning an HTTP request into a call and back.

use super::*;

/// Largest request or response body for `actors.call`, and for the
/// requests, and the responses, of one `calls.call-many` together.
pub(super) const MAX_CALL_BODY: usize = 16 * 1024 * 1024;

/// Most entries in one `calls.call-many` (ADR 0025).
pub(crate) const MAX_BATCH: usize = 16;

/// What every entry of a batch past [`MAX_BATCH`] gets.
pub(crate) const TOO_MANY: &str = "too many calls: at most 16 in one call-many";

/// The response bytes calls may still buffer: one call's own
/// ([`BodyBudget::one`]), or shared by the entries of a batch, so that a
/// batch buffers no more than one call (ADR 0025).
#[derive(Debug)]
pub(crate) struct BodyBudget(std::sync::atomic::AtomicUsize);

impl BodyBudget {
    /// [`MAX_CALL_BODY`], for one call or one batch.
    pub(crate) fn one() -> Self {
        Self(std::sync::atomic::AtomicUsize::new(MAX_CALL_BODY))
    }

    /// Takes `n` bytes, or nothing and `false` if fewer are left.
    fn take(&self, n: usize) -> bool {
        use std::sync::atomic::Ordering::{AcqRel, Acquire};
        let mut left = self.0.load(Acquire);
        loop {
            let Some(rest) = left.checked_sub(n) else {
                return false;
            };
            match self.0.compare_exchange_weak(left, rest, AcqRel, Acquire) {
                Ok(_) => return true,
                Err(now) => left = now,
            }
        }
    }

    /// The bytes left.
    fn left(&self) -> usize {
        self.0.load(std::sync::atomic::Ordering::Acquire)
    }

    /// Gives back `n` bytes taken by a response that was dropped.
    fn give(&self, n: usize) {
        self.0.fetch_add(n, std::sync::atomic::Ordering::AcqRel);
    }
}

/// The answer for every entry when a batch is refused as a whole, before
/// any call starts: more than [`MAX_BATCH`] entries, or request bodies
/// past [`MAX_CALL_BODY`] together (ADR 0025). `None`: the batch runs.
pub(crate) fn refuse_batch(
    calls: &[(String, CallRequest)],
) -> Option<Vec<Result<CallResponse, CallError>>> {
    let refusal = if calls.len() > MAX_BATCH {
        CallError::Failed(TOO_MANY.to_owned())
    } else if calls
        .iter()
        .try_fold(0usize, |sum, (_, r)| sum.checked_add(r.body.len()))
        .is_none_or(|sum| sum > MAX_CALL_BODY)
    {
        CallError::TooLarge
    } else {
        return None;
    };
    Some(calls.iter().map(|_| Err(refusal.clone())).collect())
}

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
    /// or `timeout` after `limit`. The response takes its bytes from
    /// `budget`: past it, `too-large`. One entry of a `calls.call-many` is
    /// this call, with the batch's shared budget.
    ///
    /// A call to an actor already waiting in this chain (including the
    /// caller) fails with `reentrant`. A cycle across two independent chains
    /// cannot be seen here and ends in `timeout` at the caller's deadline.
    /// The front is never in a chain: it cannot be called back.
    pub(super) async fn call(
        self: Arc<Self>,
        caller: &Caller,
        chain: &Chain,
        target: ActorAddress,
        request: CallRequest,
        limit: Duration,
        budget: &BodyBudget,
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

        let head = request.method.eq_ignore_ascii_case("HEAD");
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
                    Ok(response) => collect_response(response, budget, head).await,
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
                Ok(Outcome::Served(response)) => collect_response(response, budget, head).await,
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
                            Ok(response) => collect_response(response, budget, head).await,
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
                            Ok(response) => collect_response(response, budget, head).await,
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
                        collect_response(capacity_response(), budget, false).await
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

/// Reads a call's answer into memory, its bytes taken from `budget`.
/// `head`: the call was a HEAD, whose answer has no body.
pub(super) async fn collect_response(
    response: Response<HyperOutgoingBody>,
    budget: &BodyBudget,
    head: bool,
) -> Result<CallResponse, CallError> {
    use http_body_util::BodyExt;
    let (parts, mut body) = response.into_parts();
    // An answer that says it is larger than what is left fails before
    // taking anything, so it cannot push a sibling's answer out. An answer
    // known to have no body (a HEAD's, a 1xx, 204 or 304) may still give
    // the length a GET would get: only what arrives is counted there.
    let bodiless = head
        || parts.status.is_informational()
        || parts.status == StatusCode::NO_CONTENT
        || parts.status == StatusCode::NOT_MODIFIED;
    let declared = parts
        .headers
        .get(hyper::header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok()?.parse::<u64>().ok());
    if !bodiless && declared.is_some_and(|n| n > budget.left() as u64) {
        return Err(CallError::TooLarge);
    }
    // Counted as it arrives, so that the entries of a batch together
    // never hold more than the budget. Without a length, which entry of a
    // batch fails depends on arrival order (ADR 0025).
    let mut taken = 0usize;
    let mut bytes = Vec::new();
    let read = async {
        while let Some(frame) = body.frame().await {
            let frame =
                frame.map_err(|e| CallError::Failed(format!("reading the response body: {e}")))?;
            if let Ok(data) = frame.into_data() {
                if !budget.take(data.len()) {
                    return Err(CallError::TooLarge);
                }
                taken += data.len();
                bytes.extend_from_slice(&data);
            }
        }
        Ok(())
    };
    if let Err(e) = read.await {
        budget.give(taken);
        return Err(e);
    }
    let body = bytes;
    // The host's range never reaches the calling guest (ADR 0023): not
    // the node that served a hop (`x-wasmkeep-node`), nor any name the
    // callee set. No SDK reads one from an answer. Nor does the hop's
    // framing: the body is whole here (its `content-length` stays true),
    // and a guest passing the answer on could not set `transfer-encoding`
    // or `connection` itself.
    let headers = parts
        .headers
        .iter()
        .filter(|(k, _)| {
            let k = k.as_str();
            !is_wasmkeep_header(k) && (k == "content-length" || !HOP_BY_HOP.contains(&k))
        })
        .filter_map(|(k, v)| Some((k.as_str().to_owned(), v.to_str().ok()?.to_owned())))
        .collect();
    Ok(CallResponse {
        status: parts.status.as_u16(),
        headers,
        body,
    })
}

#[cfg(test)]
mod budget_tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code

    use super::*;

    fn answer(len: usize, declared: Option<usize>) -> Response<HyperOutgoingBody> {
        let text = "x".repeat(len);
        let mut response = text_response(StatusCode::OK, &text);
        if let Some(n) = declared {
            response
                .headers_mut()
                .insert(hyper::header::CONTENT_LENGTH, n.into());
        }
        response
    }

    #[tokio::test]
    async fn a_batch_shares_one_response_budget() {
        let budget = BodyBudget::one();
        let nine = 9 << 20;
        assert_eq!(
            collect_response(answer(nine, None), &budget, false)
                .await
                .unwrap()
                .body
                .len(),
            nine
        );
        // The second 9 MiB answer passes the batch's 16 MiB: refused, and
        // what it had taken is given back.
        assert!(matches!(
            collect_response(answer(nine, None), &budget, false).await,
            Err(CallError::TooLarge)
        ));
        assert_eq!(budget.left(), MAX_CALL_BODY - nine);
        assert!(
            collect_response(answer(1 << 20, None), &budget, false)
                .await
                .is_ok()
        );
    }

    /// A HEAD's answer, a 204 and a 304 have no body whatever their
    /// `content-length` says (it describes the body a GET would get), so
    /// the declared length is not held against the budget.
    #[tokio::test]
    async fn an_answer_without_a_body_is_not_judged_on_its_length() {
        let budget = BodyBudget::one();
        let declared = MAX_CALL_BODY + 1;
        let mut ok = answer(0, Some(declared));
        let r = collect_response(ok, &budget, true).await.unwrap();
        assert_eq!((r.status, r.body.len()), (200, 0));
        assert!(
            r.headers
                .iter()
                .any(|(k, v)| k == "content-length" && *v == declared.to_string())
        );
        for status in [StatusCode::NO_CONTENT, StatusCode::NOT_MODIFIED] {
            ok = answer(0, Some(declared));
            *ok.status_mut() = status;
            let r = collect_response(ok, &budget, false).await.unwrap();
            assert_eq!(r.status, status.as_u16());
        }
        assert_eq!(budget.left(), MAX_CALL_BODY);
        // A GET's answer is still refused on its declared length.
        assert!(matches!(
            collect_response(answer(0, Some(declared)), &budget, false).await,
            Err(CallError::TooLarge)
        ));
    }

    #[tokio::test]
    async fn an_answer_declared_too_large_takes_nothing() {
        let budget = BodyBudget::one();
        assert!(matches!(
            collect_response(answer(10, Some(MAX_CALL_BODY + 1)), &budget, false).await,
            Err(CallError::TooLarge)
        ));
        assert_eq!(budget.left(), MAX_CALL_BODY);
        // A single call still takes its whole 16 MiB.
        let r = collect_response(answer(MAX_CALL_BODY, Some(MAX_CALL_BODY)), &budget, false).await;
        assert_eq!(r.unwrap().body.len(), MAX_CALL_BODY);
    }
}
