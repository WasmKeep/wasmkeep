//! Placement at wake-up (ADR 0041): this node's side of the pure policy in
//! `wasmkeep_cluster::placement`. The sender's ledger (credits, backoff,
//! the two no-return times), the choice over this node's view, the placed
//! hop and how it ended, and the receiver's arrival check, serialized with
//! the slot it reserves.

use tokio::time::Instant;
use wasmkeep_cluster::membership::Load;
use wasmkeep_cluster::placement::{self, Arrival, Budget, Decision, Reason, RoomLoad};

use super::*;

/// On a placed hop (ADR 0041, section 8): the sender's resident fill in
/// thousandths, 200 to 1000, signed with the hop. On an answer: `refused`,
/// the event did not run there. Set by the host only: removed from a
/// client's request and from a guest's response.
pub(crate) const PLACED: &str = "x-wasmkeep-placed";

/// The answer mark: the placed event did not run on the receiver.
pub(crate) const PLACED_REFUSED: &str = "refused";

/// The largest declared body a placed request may have (ADR 0041, section
/// 1): it is read into memory before the hop, so a refusal can run it here.
pub(crate) const MAX_PLACED_BODY: u64 = 64 * 1024;

/// Placement's own state on this node.
#[derive(Debug, Default)]
pub(crate) struct Placing {
    state: Mutex<State>,
}

#[derive(Debug, Default)]
struct State {
    /// Placements sent per peer heartbeat (section 5, the budget).
    budget: Budget,
    /// Peers not chosen until then (section 6): placement's own table,
    /// never ADR 0014's suspects.
    backoff: HashMap<String, Instant>,
    /// When this node last accepted a placed event from each peer.
    accepted: HashMap<String, Instant>,
    /// When this node last sent each peer a placed event, any answer.
    sent: HashMap<String, Instant>,
    /// Placed events accepted here whose actor is not counted resident
    /// yet: counted in `actors`, taken from the room.
    in_flight: u64,
}

/// What the sender decided for a wake-up.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Choice {
    /// Not a placeable wake-up here (section 1): placement off, the node
    /// draining, its lease not valid, the secrets not open, a move naming
    /// the actor. Not counted.
    NotPlaceable,
    /// Placeable, kept here: the reason, as `wasmkeep_placement_kept_total`
    /// counts it.
    Keep(&'static str),
    /// Send it to `node`, the hop carrying this node's `fill`.
    Place { node: String, fill: u64 },
}

/// How a placed hop ended, for the sender (section 6's table).
#[derive(Debug)]
pub(crate) enum Hop<T> {
    /// An answer without the refusal mark: returned as it is.
    Answered(T),
    /// `503` with the refusal mark: it did not run there; run it here.
    Refused,
    /// Never read by the node: run it here.
    Unsent,
    /// Ambiguous (it may have run): answered as a failed forward is.
    Failed(String),
}

impl<T> Hop<T> {
    /// `outcome` of `wasmkeep_placements_total`.
    pub(crate) fn outcome(&self) -> &'static str {
        match self {
            Self::Answered(_) => "placed",
            Self::Refused => "refused",
            Self::Unsent => "unsent",
            Self::Failed(_) => "failed",
        }
    }
}

/// A placed mark under a signature that checked, with the node it binds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Mark {
    /// The sender's fill, if the value is a whole number at all; its range
    /// is the arrival check's.
    pub(crate) fill: Option<u64>,
    pub(crate) sender: String,
}

/// The slot a placed event took on arrival: counted in this node's
/// residents until dropped, by which time the actor is registered or the
/// event ended.
pub(crate) struct Reservation<'a> {
    placing: &'a Placing,
    sender: String,
}

impl Reservation<'_> {
    /// The event was handed to the guest here: no return from its sender
    /// for three periods (section 5).
    pub(crate) fn accepted(&self) {
        self.placing
            .lock()
            .accepted
            .insert(self.sender.clone(), Instant::now());
    }
}

impl Drop for Reservation<'_> {
    fn drop(&mut self) {
        let mut state = self.placing.lock();
        state.in_flight = state.in_flight.saturating_sub(1);
    }
}

impl Placing {
    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|p| p.into_inner())
    }
}

/// The value of a placed mark: exactly one, decimal digits only.
pub(crate) fn mark_value(headers: &hyper::HeaderMap) -> Option<u64> {
    let mut values = headers.get_all(PLACED).iter();
    let value = values.next()?.to_str().ok()?;
    if values.next().is_some()
        || value.is_empty()
        || value.len() > 4
        || !value.bytes().all(|b| b.is_ascii_digit())
    {
        return None;
    }
    value.parse().ok()
}

/// Marks `response` as a refusal of a placed event (section 6), if
/// `headers` (the request's) carry a placed mark: the sender then runs it.
/// Only for answers given before the event reached a guest.
pub(crate) fn refuse_if_placed(
    headers: &hyper::HeaderMap,
    response: Response<HyperOutgoingBody>,
) -> Response<HyperOutgoingBody> {
    if headers.contains_key(PLACED) {
        mark_refused(response)
    } else {
        response
    }
}

/// `response` with the refusal mark.
pub(crate) fn mark_refused(
    mut response: Response<HyperOutgoingBody>,
) -> Response<HyperOutgoingBody> {
    response.headers_mut().insert(
        PLACED,
        hyper::header::HeaderValue::from_static(PLACED_REFUSED),
    );
    response
}

/// A refusal of a placed event: `503`, `Retry-After`, the mark.
pub(crate) fn placed_refusal(text: &str) -> Response<HyperOutgoingBody> {
    mark_refused(unavailable(text))
}

/// Whether an answer is a placed event's refusal: `503` with the mark.
pub(crate) fn is_placed_refusal<B>(response: &Response<B>) -> bool {
    response.status() == StatusCode::SERVICE_UNAVAILABLE
        && response
            .headers()
            .get(PLACED)
            .is_some_and(|v| v.as_bytes() == PLACED_REFUSED.as_bytes())
}

impl Cluster {
    /// Placement is on for this node ([`ClusterConfig::placement`]).
    pub(crate) fn placement_on(&self) -> bool {
        self.place
    }

    /// The sender's decision for a wake-up of `address`, which nobody
    /// holds (ADR 0041, sections 1 to 5), `local` being this app's load
    /// read from memory now. `signable`: the hop can carry its proof (a
    /// request with a peer address to prove, or a call). On
    /// [`Choice::Place`] the credit is charged and the send time stamped
    /// before any answer.
    pub(crate) fn choose_placement(
        &self,
        address: &ActorAddress,
        local: Load,
        signable: bool,
    ) -> Choice {
        if !self.place
            || self.draining()
            || self.secrets_blocked()
            || !self.lease_valid_now()
            || self.moves().iter().any(|m| self.names(m, address))
        {
            return Choice::NotPlaceable;
        }
        if !signable || self.secret.get().is_none() {
            return Choice::Keep("unsigned");
        }
        let period = self.heartbeat_every;
        let draining = self
            .control
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .draining
            .clone();
        let suspects: Vec<String> = {
            let now = now_ms();
            let mut s = self.suspects.lock().unwrap_or_else(|p| p.into_inner());
            s.retain(|_, until| *until > now);
            s.keys().cloned().collect()
        };
        // Owned first, the view's lock released before `address_of` reads
        // it again.
        let view: Vec<(String, Peer)> = self
            .peers
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .iter()
            .map(|(id, p)| (id.clone(), p.clone()))
            .collect();
        let rows: Vec<(String, Peer, bool, String)> = view
            .into_iter()
            .map(|(id, peer)| {
                let reachable = id != self.node_id && self.address_of(&id).is_some();
                let beat = peer.seen.as_ref().map_or_else(String::new, |s| {
                    format!("{}/{}", s.beat.0.as_deref().unwrap_or("-"), s.beat.1)
                });
                (id, peer, reachable, beat)
            })
            .collect();
        let s = placement::fill(local.actors, local.actor_room.unwrap_or(0));
        let local = placement::Local {
            node: &self.node_id,
            app: &self.serves,
            place: true,
            load: RoomLoad::from(local),
        };
        let key = encode_address(address);
        let now = Instant::now();
        let mut state = self.placing.lock();
        state.backoff.retain(|_, until| *until > now);
        let peers: Vec<placement::Peer<'_>> = rows
            .iter()
            .map(|(id, peer, reachable, beat)| placement::Peer {
                node: id,
                app: &peer.app,
                live: true,
                drain: peer.drain,
                drain_requested: draining.contains(id),
                suspected: suspects.contains(id),
                reachable: *reachable,
                place: peer.can_place,
                // Stale means keep it (section 3); `decide` checks the age too.
                load: peer
                    .seen
                    .as_ref()
                    .and_then(|s| s.fresh(period))
                    .map(|l| RoomLoad::from(*l)),
                heartbeat: beat,
                age: peer
                    .seen
                    .as_ref()
                    .map_or(Duration::MAX, |s| now.saturating_duration_since(s.at)),
                backoff_remaining: state
                    .backoff
                    .get(id)
                    .map_or(Duration::ZERO, |until| until.saturating_duration_since(now)),
                accepted_age: state
                    .accepted
                    .get(id)
                    .map(|at| now.saturating_duration_since(*at)),
            })
            .collect();
        match placement::decide(local, &peers, &key, period, &state.budget) {
            Decision::Keep(reason) => Choice::Keep(match reason {
                Reason::Floor => "floor",
                Reason::Band => "band",
                Reason::Budget => "budget",
                Reason::NoCandidate | Reason::Disabled | Reason::Lighter => "no_candidate",
            }),
            Decision::Forward { node, .. } => {
                let node = node.to_owned();
                let beat = rows
                    .iter()
                    .find(|(id, ..)| *id == node)
                    .map(|(.., beat)| beat.clone())
                    .unwrap_or_default();
                state.budget.charge(&node, &beat);
                state.sent.insert(node.clone(), now);
                Choice::Place { node, fill: s }
            }
        }
    }

    /// Whether a move request names `address` (ADR 0017): never placed,
    /// and a placed hop for it is refused.
    pub(crate) fn move_names(&self, address: &ActorAddress) -> bool {
        self.moves().iter().any(|m| self.names(m, address))
    }

    /// Whether move request `m` names `address` of this app.
    fn names(&self, m: &control::MoveIntent, address: &ActorAddress) -> bool {
        self.serves == format!("{}/{}", m.tenant, m.app)
            && m.class == address.class()
            && m.name == address.name()
    }

    /// A placed hop to `node` was refused, never read or failed: it is no
    /// candidate for one heartbeat horizon (section 6).
    pub(crate) fn placement_backoff(&self, node: &str) {
        let until = Instant::now() + Duration::from_millis(self.heartbeat_horizon_ms.max(1));
        self.placing.lock().backoff.insert(node.to_owned(), until);
    }

    /// The placed mark on a hop that arrived, if it is under a signature
    /// that checked and names its sender (section 8): a call's (`signed`)
    /// or a forward's proof. `None` for no mark, or one that does not
    /// check: then it grants and takes nothing, and the request is handled
    /// as the forward it is.
    pub(crate) fn placed_mark<B>(&self, req: &Request<B>, signed: Option<&Signed>) -> Option<Mark> {
        if !req.headers().contains_key(PLACED) {
            return None;
        }
        let sender = match signed {
            Some(signed) => signed.sender.clone(),
            None => self.admission_checked(req).and_then(|(s, _)| s.sender),
        }?;
        Some(Mark {
            fill: mark_value(req.headers()),
            sender,
        })
    }

    /// The receiver's arrival check (section 5) on `live`, this app's load
    /// read from memory under the same lock that reserves the slot, and
    /// no return. `Ok`: run it here, the slot held by the reservation.
    pub(crate) fn arrive(
        &self,
        mark: &Mark,
        live: impl FnOnce() -> Load,
    ) -> Result<Reservation<'_>, Arrival> {
        let now = Instant::now();
        let mut state = self.placing.lock();
        let load = live();
        let in_flight = state.in_flight;
        let room = RoomLoad {
            load: Load {
                actors: load.actors.saturating_add(in_flight),
                ..load
            },
            actor_room: load.actor_room.map(|r| r.saturating_sub(in_flight)),
            socket_room: load.socket_room,
        };
        let sent_age = state
            .sent
            .get(&mark.sender)
            .map(|at| now.saturating_duration_since(*at));
        match placement::receive(
            room,
            mark.fill.unwrap_or(0),
            true,
            sent_age,
            self.heartbeat_every,
        ) {
            Arrival::Accept => {
                state.in_flight = state.in_flight.saturating_add(1);
                Ok(Reservation {
                    placing: &self.placing,
                    sender: mark.sender.clone(),
                })
            }
            other => Err(other),
        }
    }

    /// Sends a placed request to `node` (section 6): the drain's forward,
    /// its body read already, with the signed mark carrying `fill`.
    pub(crate) async fn forward_placed(
        &self,
        node: &str,
        mut req: Request<http_body_util::Full<bytes::Bytes>>,
        fill: u64,
    ) -> Hop<Response<HyperOutgoingBody>> {
        req.headers_mut().remove(PLACED);
        req.headers_mut()
            .insert(PLACED, hyper::header::HeaderValue::from(fill));
        let base = match self.prepare_forward(node, &mut req).await {
            Ok(base) => base,
            // Nothing sent: no address, not trusted, or no proof.
            Err(_) => return Hop::Unsent,
        };
        let client = self.clients().call.clone();
        match tokio::time::timeout(FORWARD_TIMEOUT, client.request(req)).await {
            Ok(Ok(response)) => match self.trusted_response(&base, node, response) {
                Some(r) if is_placed_refusal(&r) => Hop::Refused,
                Some(r) => Hop::Answered(r),
                None => Hop::Failed(format!("node {node} is no longer trusted")),
            },
            Ok(Err(e)) if never_read_by_peer(&base, &e) => Hop::Unsent,
            Ok(Err(e)) => {
                self.failover.connect_failed(node, &e);
                Hop::Failed(format!("node {node}: {e}"))
            }
            Err(_) => Hop::Failed(format!("node {node} did not answer in time")),
        }
    }

    /// Sends a placed `actors.call` to `node` (section 6): signed as every
    /// call, the mark inside the signature, sent once (never the retry of
    /// [`Self::call`] after a refused handshake: it runs here instead).
    pub(crate) async fn call_placed(
        &self,
        node: &str,
        target: &ActorAddress,
        chain: &[ActorAddress],
        mut req: Request<http_body_util::Full<bytes::Bytes>>,
        fill: u64,
    ) -> Hop<Response<HyperOutgoingBody>> {
        let Some(base) = self.address_of(node) else {
            return Hop::Unsent;
        };
        if self.check_tls_target(&base).is_err() || self.secret.get().is_none() {
            return Hop::Unsent;
        }
        req.headers_mut().remove(PLACED);
        req.headers_mut()
            .insert(PLACED, hyper::header::HeaderValue::from(fill));
        match self.call_once(node, target, chain, req).await {
            Ok(r) if is_placed_refusal(&r) => Hop::Refused,
            Ok(r) => Hop::Answered(r),
            Err(CallFailure::HandshakeRefused) => Hop::Unsent,
            Err(CallFailure::Other(e)) => Hop::Failed(e),
        }
    }
}
