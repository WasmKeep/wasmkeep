//! WebSocket connections owned by the host.
//!
//! Sockets live here, not in the actor's instance, so they survive the actor
//! hibernating: each incoming message is delivered as an event that wakes it.

use std::collections::{BTreeSet, HashMap};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::protocol::{CloseFrame, WebSocketConfig};
use wasmkeep_runtime::ActorAddress;

/// Messages queued per connection before `send` reports backpressure.
const SEND_QUEUE: usize = 256;
/// Bytes queued per connection before `send` reports backpressure: a client
/// that stops reading cannot make the host hold more than this for it.
pub(crate) const MAX_QUEUED_BYTES: usize = 4 * 1024 * 1024;
/// Largest message (and frame) a client may send.
pub(crate) const MAX_MESSAGE_BYTES: usize = 1024 * 1024;
/// Open sockets per actor, and per node.
pub(crate) const MAX_SOCKETS_PER_ACTOR: usize = 1_000;
pub(crate) const MAX_SOCKETS_PER_NODE: usize = 50_000;
/// A silent client is pinged after this long, and closed if still silent
/// after [`PONG_WAIT`] more (half-open connections otherwise never end).
/// Under a load balancer's usual idle timeout (60 s for nginx and AWS
/// ALB), so an idle socket carries a frame before the balancer cuts it.
pub(crate) const PING_AFTER: Duration = Duration::from_secs(25);
pub(crate) const PONG_WAIT: Duration = Duration::from_secs(30);

/// The protocol limits for every socket the host accepts.
pub(crate) fn config() -> WebSocketConfig {
    WebSocketConfig::default()
        .max_message_size(Some(MAX_MESSAGE_BYTES))
        .max_frame_size(Some(MAX_MESSAGE_BYTES))
        .max_write_buffer_size(2 * MAX_QUEUED_BYTES)
}

/// Something to write to a socket.
pub(crate) enum Outgoing {
    Message(WsMessage),
    Close(u16, String),
}

/// Bytes a message holds while queued.
pub(crate) fn queued_size(msg: &WsMessage) -> usize {
    msg.len()
}

struct Conn {
    owner: ActorAddress,
    // Taking the sender closes and hides the socket. Its registry slot is
    // retained until run_socket finishes delivering the guest close event.
    tx: Option<mpsc::Sender<Outgoing>>,
    /// Bytes queued and not yet written.
    queued: Arc<AtomicUsize>,
    /// `on-websocket-open` accepted it: until then it is invisible to the
    /// actor's other events (no broadcast reaches a socket it may refuse).
    accepted: bool,
}

/// Why `send` failed.
pub(crate) enum SendError {
    UnknownConnection,
    Backpressure,
}

/// Why `open` refused a socket.
#[derive(Debug)]
pub(crate) enum OpenError {
    ActorFull,
    NodeFull,
}

#[derive(Default)]
struct Registry {
    conns: HashMap<u64, Conn>,
    by_owner: HashMap<ActorAddress, BTreeSet<u64>>,
}

impl Registry {
    fn remove(&mut self, id: u64) -> Option<Conn> {
        let conn = self.conns.remove(&id)?;
        if let Some(ids) = self.by_owner.get_mut(&conn.owner) {
            ids.remove(&id);
            if ids.is_empty() {
                self.by_owner.remove(&conn.owner);
            }
        }
        Some(conn)
    }
}

/// All open connections on this node.
#[derive(Default)]
pub(crate) struct Sockets {
    next_id: AtomicU64,
    registry: Mutex<Registry>,
}

/// A connection's queue, for its writer.
pub(crate) struct Opened {
    pub(crate) id: u64,
    pub(crate) outgoing: mpsc::Receiver<Outgoing>,
    pub(crate) queued: Arc<AtomicUsize>,
}

impl Sockets {
    fn lock(&self) -> std::sync::MutexGuard<'_, Registry> {
        self.registry.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// Whether `owner` may open another socket now (checked again by
    /// [`Self::open`]): answered before the upgrade is accepted.
    pub(crate) fn can_open(&self, owner: &ActorAddress) -> Result<(), OpenError> {
        Self::room(&self.lock(), owner)
    }

    fn room(registry: &Registry, owner: &ActorAddress) -> Result<(), OpenError> {
        if registry.conns.len() >= MAX_SOCKETS_PER_NODE {
            return Err(OpenError::NodeFull);
        }
        if registry
            .by_owner
            .get(owner)
            .is_some_and(|ids| ids.len() >= MAX_SOCKETS_PER_ACTOR)
        {
            return Err(OpenError::ActorFull);
        }
        Ok(())
    }

    /// Registers a connection, pending until [`Self::accept`]; refused when
    /// the actor or the node has too many.
    pub(crate) fn open(&self, owner: ActorAddress) -> Result<Opened, OpenError> {
        let mut registry = self.lock();
        Self::room(&registry, &owner)?;
        let id = self.next_id.fetch_add(1, Ordering::Relaxed) + 1;
        let (tx, outgoing) = mpsc::channel(SEND_QUEUE);
        let queued = Arc::new(AtomicUsize::new(0));
        registry
            .by_owner
            .entry(owner.clone())
            .or_default()
            .insert(id);
        registry.conns.insert(
            id,
            Conn {
                owner,
                tx: Some(tx),
                queued: Arc::clone(&queued),
                accepted: false,
            },
        );
        Ok(Opened {
            id,
            outgoing,
            queued,
        })
    }

    /// `on-websocket-open` accepted the connection: the actor's events now
    /// see it.
    pub(crate) fn accept(&self, id: u64) {
        if let Some(conn) = self.lock().conns.get_mut(&id)
            && conn.tx.is_some()
        {
            conn.accepted = true;
        }
    }

    /// Forgets a connection (its writer then drains and stops).
    pub(crate) fn remove(&self, id: u64) {
        self.lock().remove(id);
    }

    /// Hides a closing connection until its close event has been delivered.
    pub(crate) fn begin_close(&self, id: u64) {
        if let Some(conn) = self.lock().conns.get_mut(&id) {
            conn.accepted = false;
            conn.tx.take();
        }
    }

    /// Queues a message, if `id` is one of `owner`'s accepted connections
    /// (or `opening`, the one being opened in this event).
    pub(crate) fn send(
        &self,
        owner: &ActorAddress,
        id: u64,
        msg: WsMessage,
        opening: Option<u64>,
    ) -> Result<(), SendError> {
        let registry = self.lock();
        let conn = registry
            .conns
            .get(&id)
            .filter(|c| &c.owner == owner && (c.accepted || opening == Some(id)))
            .ok_or(SendError::UnknownConnection)?;
        let tx = conn.tx.as_ref().ok_or(SendError::UnknownConnection)?;
        let size = queued_size(&msg);
        let before = conn.queued.fetch_add(size, Ordering::AcqRel);
        if before.saturating_add(size) > MAX_QUEUED_BYTES {
            conn.queued.fetch_sub(size, Ordering::AcqRel);
            return Err(SendError::Backpressure);
        }
        tx.try_send(Outgoing::Message(msg)).map_err(|e| {
            conn.queued.fetch_sub(size, Ordering::AcqRel);
            match e {
                mpsc::error::TrySendError::Full(_) => SendError::Backpressure,
                mpsc::error::TrySendError::Closed(_) => SendError::UnknownConnection,
            }
        })
    }

    /// Asks the writer to close `id` (if it is `owner`'s) and hides it.
    /// Its slot stays reserved until its close event finishes.
    pub(crate) fn close(&self, owner: &ActorAddress, id: u64, code: u16, reason: String) {
        let mut registry = self.lock();
        if let Some(conn) = registry.conns.get_mut(&id)
            && &conn.owner == owner
        {
            Self::close_conn(conn, code, &reason);
        }
    }

    /// Closes every connection on the node (shutdown, a lapsed node
    /// lease). Each task releases its own slot after finishing cleanup.
    /// Returns how many were open (not closing already).
    pub(crate) fn close_all(&self, code: u16, reason: &str) -> usize {
        let mut registry = self.lock();
        let mut closed = 0;
        for conn in registry.conns.values_mut() {
            closed += usize::from(Self::close_conn(conn, code, reason));
        }
        closed
    }

    /// Closes every connection of `owner`, pending ones included (this
    /// node no longer owns the actor). Returns how many were open.
    pub(crate) fn close_owner(&self, owner: &ActorAddress, code: u16, reason: &str) -> usize {
        let mut registry = self.lock();
        let Registry { conns, by_owner } = &mut *registry;
        let mut closed = 0;
        for id in by_owner.get(owner).into_iter().flatten() {
            if let Some(conn) = conns.get_mut(id) {
                closed += usize::from(Self::close_conn(conn, code, reason));
            }
        }
        closed
    }

    /// Asks the writer to close `conn` and hides it; false if it was
    /// closing already.
    fn close_conn(conn: &mut Conn, code: u16, reason: &str) -> bool {
        conn.accepted = false;
        // If the queue is full the socket is dropped instead: still closed.
        match conn.tx.take() {
            Some(tx) => {
                let _ = tx.try_send(Outgoing::Close(code, reason.to_owned()));
                true
            }
            None => false,
        }
    }

    /// `owner`'s accepted connections (and `opening`, if it is one of its
    /// own), oldest first.
    pub(crate) fn of(&self, owner: &ActorAddress, opening: Option<u64>) -> Vec<u64> {
        let registry = self.lock();
        registry
            .by_owner
            .get(owner)
            .map(|ids| {
                ids.iter()
                    .copied()
                    .filter(|id| {
                        registry
                            .conns
                            .get(id)
                            .is_some_and(|c| c.tx.is_some() && (c.accepted || opening == Some(*id)))
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    /// `owner`'s reserved slots, closing ones included: 0 once every close
    /// event has run.
    pub(crate) fn reserved_by(&self, owner: &ActorAddress) -> usize {
        self.lock().by_owner.get(owner).map_or(0, BTreeSet::len)
    }

    /// Number of reserved socket slots, including pending close delivery.
    pub(crate) fn len(&self) -> usize {
        self.lock().conns.len()
    }
}

/// Builds the close frame for an `Outgoing::Close`.
pub(crate) fn close_frame(code: u16, reason: String) -> WsMessage {
    WsMessage::Close(Some(CloseFrame {
        code: CloseCode::from(code),
        reason: reason.into(),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn actor(name: &str) -> ActorAddress {
        ActorAddress::new("t", "c", name).unwrap()
    }

    #[test]
    fn pending_sockets_are_invisible_until_accepted() {
        let sockets = Sockets::default();
        let a = actor("a");
        let first = sockets.open(a.clone()).unwrap();
        sockets.accept(first.id);
        let pending = sockets.open(a.clone()).unwrap();
        assert_eq!(sockets.of(&a, None), vec![first.id]);
        assert!(matches!(
            sockets.send(&a, pending.id, WsMessage::text("x"), None),
            Err(SendError::UnknownConnection)
        ));
        // Its own open event sees it and may send to it.
        assert_eq!(sockets.of(&a, Some(pending.id)), vec![first.id, pending.id]);
        assert!(
            sockets
                .send(&a, pending.id, WsMessage::text("x"), Some(pending.id))
                .is_ok()
        );
        sockets.accept(pending.id);
        assert_eq!(sockets.of(&a, None), vec![first.id, pending.id]);
    }

    #[test]
    fn queued_bytes_are_capped_per_connection() {
        let sockets = Sockets::default();
        let a = actor("a");
        let conn = sockets.open(a.clone()).unwrap();
        sockets.accept(conn.id);
        let big = vec![0u8; MAX_QUEUED_BYTES / 2];
        assert!(
            sockets
                .send(&a, conn.id, WsMessage::binary(big.clone()), None)
                .is_ok()
        );
        assert!(
            sockets
                .send(&a, conn.id, WsMessage::binary(big.clone()), None)
                .is_ok()
        );
        assert!(matches!(
            sockets.send(&a, conn.id, WsMessage::binary(big.clone()), None),
            Err(SendError::Backpressure)
        ));
        // The writer took one: room again.
        conn.queued.fetch_sub(big.len(), Ordering::AcqRel);
        assert!(
            sockets
                .send(&a, conn.id, WsMessage::binary(big), None)
                .is_ok()
        );
        // Another actor cannot reach it.
        assert!(matches!(
            sockets.send(&actor("b"), conn.id, WsMessage::text("x"), None),
            Err(SendError::UnknownConnection)
        ));
    }

    #[test]
    fn sockets_per_actor_are_capped() {
        let sockets = Sockets::default();
        let a = actor("a");
        for _ in 0..MAX_SOCKETS_PER_ACTOR {
            sockets.open(a.clone()).unwrap();
        }
        assert!(matches!(sockets.open(a.clone()), Err(OpenError::ActorFull)));
        assert!(
            sockets.open(actor("b")).is_ok(),
            "other actors are not affected"
        );
        assert_eq!(sockets.len(), MAX_SOCKETS_PER_ACTOR + 1);
    }

    #[test]
    fn closing_sockets_keep_their_capacity_until_the_close_event_finishes() {
        let sockets = Sockets::default();
        let a = actor("a");
        let first = sockets.open(a.clone()).unwrap();
        sockets.accept(first.id);
        for _ in 1..MAX_SOCKETS_PER_ACTOR {
            sockets.open(a.clone()).unwrap();
        }
        sockets.close(&a, first.id, 1000, "done".into());
        assert_eq!(sockets.len(), MAX_SOCKETS_PER_ACTOR);
        assert!(matches!(sockets.can_open(&a), Err(OpenError::ActorFull)));
        assert!(sockets.of(&a, Some(first.id)).is_empty());
        sockets.accept(first.id);
        assert!(sockets.of(&a, None).is_empty());
        assert!(matches!(
            sockets.send(&a, first.id, WsMessage::text("late"), Some(first.id)),
            Err(SendError::UnknownConnection)
        ));
        sockets.remove(first.id);
        assert!(sockets.can_open(&a).is_ok());
    }

    #[test]
    fn a_peer_close_hides_the_connection_without_releasing_its_slot() {
        let sockets = Sockets::default();
        let a = actor("a");
        let mut opened = sockets.open(a.clone()).unwrap();
        sockets.accept(opened.id);
        sockets.begin_close(opened.id);
        assert_eq!(sockets.len(), 1);
        assert!(sockets.of(&a, Some(opened.id)).is_empty());
        assert!(matches!(
            opened.outgoing.try_recv(),
            Err(mpsc::error::TryRecvError::Disconnected)
        ));
        sockets.remove(opened.id);
        assert_eq!(sockets.len(), 0);
    }

    #[test]
    fn closing_an_actors_sockets_leaves_other_actors_alone() {
        let sockets = Sockets::default();
        let (a, b) = (actor("a"), actor("b"));
        let mut first = sockets.open(a.clone()).unwrap();
        sockets.accept(first.id);
        // Pending: its open event has not run yet. Closed too.
        let mut pending = sockets.open(a.clone()).unwrap();
        let mut other = sockets.open(b.clone()).unwrap();
        sockets.accept(other.id);
        assert_eq!(sockets.close_owner(&a, 1012, "moved"), 2);
        assert_eq!(sockets.close_owner(&a, 1012, "moved"), 0, "closed once");
        for conn in [&mut first, &mut pending] {
            assert!(matches!(
                conn.outgoing.try_recv(),
                Ok(Outgoing::Close(1012, _))
            ));
        }
        assert!(matches!(
            other.outgoing.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
        assert_eq!(sockets.of(&b, None), vec![other.id]);
        // Slots stay until each close event ran.
        assert_eq!(sockets.reserved_by(&a), 2);
    }

    #[test]
    fn shutdown_closes_senders_but_keeps_slots_until_tasks_finish() {
        let sockets = Sockets::default();
        let a = actor("a");
        let mut opened = sockets.open(a.clone()).unwrap();
        sockets.accept(opened.id);
        sockets.close_all(1012, "shutdown");
        assert_eq!(sockets.len(), 1);
        assert!(sockets.of(&a, None).is_empty());
        assert!(matches!(
            opened.outgoing.try_recv(),
            Ok(Outgoing::Close(1012, _))
        ));
        assert!(matches!(
            opened.outgoing.try_recv(),
            Err(mpsc::error::TryRecvError::Disconnected)
        ));
        sockets.remove(opened.id);
        assert_eq!(sockets.len(), 0);
    }
}
