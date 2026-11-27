//! WebSocket connections owned by the host.
//!
//! Sockets live here, not in the actor's instance, so they survive the actor
//! hibernating: each incoming message is delivered as an event that wakes it.

use std::collections::{BTreeSet, HashMap};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::{mpsc, oneshot};
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
/// A silent client is pinged after this long, and closed if still silent
/// after [`PONG_WAIT`] more (half-open connections otherwise never end).
/// Under a load balancer's usual idle timeout (60 s for nginx and AWS
/// ALB), so an idle socket carries a frame before the balancer cuts it.
pub(crate) const PING_AFTER: Duration = Duration::from_secs(25);
pub(crate) const PONG_WAIT: Duration = Duration::from_secs(30);
/// How long a socket the server closed waits for the client's close frame
/// before it is dropped. A client that never answers (and keeps sending
/// pings, say) cannot keep it, and its slot, any longer.
pub(crate) const CLOSE_WAIT: Duration = Duration::from_secs(5);

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

/// The data queue and an independent, single-use close slot. A close takes
/// priority even when every data permit or queued byte is occupied.
pub(crate) struct OutgoingQueue {
    data: mpsc::Receiver<Outgoing>,
    close: oneshot::Receiver<(u16, String)>,
    close_done: bool,
    queued: Arc<AtomicUsize>,
}

impl OutgoingQueue {
    fn discard(&mut self) {
        self.data.close();
        while let Ok(Outgoing::Message(msg)) = self.data.try_recv() {
            self.queued.fetch_sub(queued_size(&msg), Ordering::AcqRel);
        }
    }

    fn closed(&mut self, code: u16, reason: String) -> Outgoing {
        // The actor asked to close: discard stale queued frames. The client
        // reconnects and catches up instead of waiting behind its backlog.
        self.discard();
        Outgoing::Close(code, reason)
    }

    pub(crate) async fn recv(&mut self) -> Option<Outgoing> {
        if !self.close_done {
            tokio::select! {
                biased;
                close = &mut self.close => {
                    self.close_done = true;
                    if let Ok((code, reason)) = close {
                        return Some(self.closed(code, reason));
                    }
                }
                msg = self.data.recv() => return msg,
            }
        }
        self.data.recv().await
    }

    #[cfg(test)]
    pub(crate) fn try_recv(&mut self) -> Result<Outgoing, mpsc::error::TryRecvError> {
        if !self.close_done {
            match self.close.try_recv() {
                Ok((code, reason)) => {
                    self.close_done = true;
                    return Ok(self.closed(code, reason));
                }
                Err(oneshot::error::TryRecvError::Closed) => self.close_done = true,
                Err(oneshot::error::TryRecvError::Empty) => {}
            }
        }
        self.data.try_recv()
    }
}

impl Drop for OutgoingQueue {
    fn drop(&mut self) {
        self.discard();
    }
}

/// Releases an in-flight frame's bytes even if the writer is aborted.
pub(crate) struct WritingBytes(pub(crate) Arc<AtomicUsize>, pub(crate) usize);

impl Drop for WritingBytes {
    fn drop(&mut self) {
        self.0.fetch_sub(self.1, Ordering::AcqRel);
    }
}

/// Bytes a message holds while queued.
pub(crate) fn queued_size(msg: &WsMessage) -> usize {
    msg.len()
}

/// Who opened a socket (ADR 0027): its events admit as that caller.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Opener {
    /// A client, at `/actor/<class>/<name>`: outside (ADR 0010).
    Outside,
    /// The app's front handed it to the actor: inside.
    Front,
}

struct Conn {
    owner: ActorAddress,
    /// Set once, at open: kept with the host's socket, so it survives the
    /// actor hibernating.
    opener: Opener,
    // Taking the sender closes and hides the socket. Its registry slot is
    // retained until run_socket finishes delivering the guest close event.
    tx: Option<mpsc::Sender<Outgoing>>,
    close: Option<oneshot::Sender<(u16, String)>>,
    /// Bytes queued and not yet written.
    queued: Arc<AtomicUsize>,
    /// `on-websocket-open` accepted it: until then it is invisible to the
    /// actor's other events (no broadcast reaches a socket it may refuse).
    accepted: bool,
    /// Notified when the server closes it, so its reader stops delivering
    /// frames and waits for the client's close for a bounded time only.
    closing: Arc<tokio::sync::Notify>,
    /// Its actor was deleted while it was accepted or closing: its close
    /// event is not delivered, so it cannot create the actor again.
    deleted: bool,
}

/// Why `send` failed.
pub(crate) enum SendError {
    UnknownConnection,
    Backpressure,
}

/// Why `open` refused a socket.
#[derive(Debug)]
pub(crate) enum OpenError {
    RestorePending,
    /// A move or a drain's hand-over is closing the actor's sockets here
    /// (ADR 0040, section 14).
    Moving,
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
pub(crate) struct Sockets {
    next_id: AtomicU64,
    pub(crate) limits: crate::SocketLimits,
    pub(crate) budget: Arc<crate::SocketBudget>,
    app_budget: Arc<crate::SocketBudget>,
    registry: Mutex<Registry>,
    /// Told the actor whose last slot was freed ([`Self::on_vacated`]).
    vacated: std::sync::OnceLock<mpsc::UnboundedSender<ActorAddress>>,
}

/// A connection's queue, for its writer.
pub(crate) struct Opened {
    pub(crate) id: u64,
    pub(crate) outgoing: OutgoingQueue,
    pub(crate) queued: Arc<AtomicUsize>,
    /// Notified once the server closes it ([`Self::close_all`] and the like).
    pub(crate) closing: Arc<tokio::sync::Notify>,
}

impl Default for Sockets {
    fn default() -> Self {
        Self::new(
            crate::SocketLimits::default(),
            crate::SocketBudget::new(50_000, 50_000),
        )
    }
}

impl Sockets {
    pub(crate) fn new(limits: crate::SocketLimits, budget: Arc<crate::SocketBudget>) -> Self {
        Self {
            next_id: AtomicU64::new(0),
            limits,
            budget,
            app_budget: crate::SocketBudget::new(limits.per_app, limits.per_app),
            registry: Mutex::default(),
            vacated: std::sync::OnceLock::new(),
        }
    }

    pub(crate) fn reserve(&self) -> Option<crate::socket_limits::SessionPermit> {
        let process = self.budget.reserve()?;
        let app = self.app_budget.reserve()?;
        Some(crate::socket_limits::SessionPermit {
            _process: process,
            _app: app,
        })
    }

    /// How many more sessions this app could open on this node now (ADR
    /// 0041, section 2): its ceiling and the process's, with what each has
    /// in use.
    pub(crate) fn session_room(&self) -> u64 {
        let n = |v: usize| u64::try_from(v).unwrap_or(u64::MAX);
        wasmkeep_cluster::placement::socket_room(
            n(self.app_budget.effective()),
            n(self.app_budget.in_use()),
            n(self.budget.effective()),
            n(self.budget.in_use()),
        )
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Registry> {
        self.registry.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// Whether `owner` may open another socket now (checked again by
    /// [`Self::open`]): answered before the upgrade is accepted.
    pub(crate) fn can_open(&self, owner: &ActorAddress) -> Result<(), OpenError> {
        self.room(&self.lock(), owner)
    }

    fn room(&self, registry: &Registry, owner: &ActorAddress) -> Result<(), OpenError> {
        if registry.conns.len() >= self.budget.effective() {
            return Err(OpenError::NodeFull);
        }
        if registry.by_owner.get(owner).map_or(0, BTreeSet::len) >= self.limits.per_actor {
            return Err(OpenError::ActorFull);
        }
        Ok(())
    }

    /// Registers a connection, pending until [`Self::accept`]; refused when
    /// the actor or the node has too many.
    #[cfg(test)]
    pub(crate) fn open(&self, owner: ActorAddress) -> Result<Opened, OpenError> {
        self.open_as(owner, Opener::Outside)
    }

    /// [`Self::open`], for a socket `opener` opened.
    pub(crate) fn open_as(&self, owner: ActorAddress, opener: Opener) -> Result<Opened, OpenError> {
        let mut registry = self.lock();
        self.room(&registry, &owner)?;
        let id = self.next_id.fetch_add(1, Ordering::Relaxed) + 1;
        let (tx, data) = mpsc::channel(SEND_QUEUE);
        let (close_tx, close) = oneshot::channel();
        let queued = Arc::new(AtomicUsize::new(0));
        let closing = Arc::new(tokio::sync::Notify::new());
        registry
            .by_owner
            .entry(owner.clone())
            .or_default()
            .insert(id);
        registry.conns.insert(
            id,
            Conn {
                owner,
                opener,
                tx: Some(tx),
                close: Some(close_tx),
                queued: Arc::clone(&queued),
                accepted: false,
                closing: Arc::clone(&closing),
                deleted: false,
            },
        );
        Ok(Opened {
            id,
            outgoing: OutgoingQueue {
                data,
                close,
                close_done: false,
                queued: Arc::clone(&queued),
            },
            queued,
            closing,
        })
    }

    /// Who opened `id`: [`Opener::Outside`] for a connection this registry
    /// does not know, so nothing unknown passes for the front.
    pub(crate) fn opener(&self, id: u64) -> Opener {
        self.lock()
            .conns
            .get(&id)
            .map_or(Opener::Outside, |c| c.opener)
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

    /// Forgets a connection (its writer then drains and stops). If it was
    /// its actor's last slot, the receiver of [`Self::on_vacated`] is told.
    pub(crate) fn remove(&self, id: u64) {
        let mut registry = self.lock();
        let Some(conn) = registry.remove(id) else {
            return;
        };
        let vacated = !registry.by_owner.contains_key(&conn.owner);
        drop(registry);
        if vacated && let Some(tx) = self.vacated.get() {
            let _ = tx.send(conn.owner);
        }
    }

    /// The actors whose last slot is freed from now on, one message each
    /// (ADR 0024: a parked record is released then). Once per registry;
    /// `None` if already taken.
    pub(crate) fn on_vacated(&self) -> Option<mpsc::UnboundedReceiver<ActorAddress>> {
        let (tx, rx) = mpsc::unbounded_channel();
        self.vacated.set(tx).ok().map(|()| rx)
    }

    /// Whether `owner` has a connection here that is not closing (accepted,
    /// or being opened): a hibernation then parks its record (ADR 0024).
    pub(crate) fn has_open(&self, owner: &ActorAddress) -> bool {
        let registry = self.lock();
        registry.by_owner.get(owner).is_some_and(|ids| {
            ids.iter()
                .any(|id| registry.conns.get(id).is_some_and(|c| c.tx.is_some()))
        })
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

    /// `owner` was deleted (`delete-self`): closes its accepted sockets
    /// with `code` and `reason`, and marks them, closing ones included, so
    /// that none of their close events runs. In a cluster the deleted
    /// actor's lease outlives the delete, so a close event would pass the
    /// existence check and run on a new, empty instance (ADR 0010, ADR
    /// 0027). A socket still being opened is left alone: its open runs
    /// after the delete, and decides on its own.
    pub(crate) fn close_deleted(&self, owner: &ActorAddress, code: u16, reason: &str) {
        let mut registry = self.lock();
        let Registry { conns, by_owner } = &mut *registry;
        for id in by_owner.get(owner).into_iter().flatten() {
            if let Some(conn) = conns.get_mut(id)
                && (conn.accepted || conn.tx.is_none())
            {
                Self::close_conn(conn, code, reason);
                conn.deleted = true;
            }
        }
    }

    /// Whether `id`'s actor was deleted under it ([`Self::close_deleted`]).
    pub(crate) fn deleted(&self, id: u64) -> bool {
        self.lock().conns.get(&id).is_some_and(|c| c.deleted)
    }

    /// Asks the writer to close `conn` and hides it; false if it was
    /// closing already.
    fn close_conn(conn: &mut Conn, code: u16, reason: &str) -> bool {
        conn.accepted = false;
        // Publish the close before dropping the data sender so the writer
        // cannot mistake the disconnected queue for an ordinary EOF.
        match conn.tx.take() {
            Some(tx) => {
                if let Some(close) = conn.close.take() {
                    let _ = close.send((code, reason.to_owned()));
                }
                drop(tx);
                // Kept as a permit if the reader is not waiting yet.
                conn.closing.notify_one();
                true
            }
            None => false,
        }
    }

    /// Whether `id` is one of `owner`'s accepted connections, not closing:
    /// a frame read from it may still be delivered.
    pub(crate) fn is_open(&self, owner: &ActorAddress, id: u64) -> bool {
        self.lock()
            .conns
            .get(&id)
            .is_some_and(|c| &c.owner == owner && c.accepted && c.tx.is_some())
    }

    /// The actors with at least one connection on this node that is not
    /// closing.
    pub(crate) fn open_owners(&self) -> Vec<ActorAddress> {
        let registry = self.lock();
        registry
            .by_owner
            .iter()
            .filter(|(_, ids)| {
                ids.iter()
                    .any(|id| registry.conns.get(id).is_some_and(|c| c.tx.is_some()))
            })
            .map(|(owner, _)| owner.clone())
            .collect()
    }

    /// The actors with at least one slot on this node, closing sockets
    /// included.
    pub(crate) fn owners(&self) -> Vec<ActorAddress> {
        self.lock().by_owner.keys().cloned().collect()
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
    fn configured_actor_zero_and_app_and_process_limits() {
        let shared = crate::SocketBudget::new(2, 2);
        let first = Sockets::new(
            crate::SocketLimits {
                per_actor: 0,
                per_app: 1,
            },
            Arc::clone(&shared),
        );
        let second = Sockets::new(
            crate::SocketLimits {
                per_actor: 1,
                per_app: 2,
            },
            Arc::clone(&shared),
        );
        assert!(matches!(first.open(actor("a")), Err(OpenError::ActorFull)));
        let a = first.reserve().unwrap();
        assert!(first.reserve().is_none(), "app cap below process cap");
        assert_eq!(
            shared.in_use(),
            1,
            "failed app reservation returns process permit"
        );
        let b = second.reserve().unwrap();
        assert!(second.reserve().is_none(), "shared process cap");
        drop(a);
        assert!(second.reserve().is_some());
        drop(b);
        assert_eq!(shared.in_use(), 0);
        let opened = second.open(actor("a")).unwrap();
        assert!(matches!(second.open(actor("a")), Err(OpenError::ActorFull)));
        second.begin_close(opened.id);
        assert!(second.open(actor("a")).is_err());
        second.remove(opened.id);
        assert!(second.open(actor("a")).is_ok());
    }

    #[test]
    fn each_socket_keeps_who_opened_it_until_removed() {
        let sockets = Sockets::default();
        let a = actor("a");
        let outside = sockets.open(a.clone()).unwrap();
        let front = sockets.open_as(a.clone(), Opener::Front).unwrap();
        sockets.accept(front.id);
        assert_eq!(sockets.opener(outside.id), Opener::Outside);
        assert_eq!(sockets.opener(front.id), Opener::Front);
        // Still the front's while its close is delivered.
        sockets.begin_close(front.id);
        assert_eq!(sockets.opener(front.id), Opener::Front);
        sockets.remove(front.id);
        assert_eq!(sockets.opener(front.id), Opener::Outside);
        assert_eq!(sockets.opener(12_345), Opener::Outside);
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

    // ADR 0040 R3 requires the slow close to survive a full data queue.
    #[test]
    fn room_slow_close_survives_full_frame_queue() {
        let sockets = Sockets::default();
        let owner = actor("room-edge");
        let mut conn = sockets.open(owner.clone()).unwrap();
        sockets.accept(conn.id);
        for _ in 0..SEND_QUEUE {
            assert!(
                sockets
                    .send(&owner, conn.id, WsMessage::text("data"), None)
                    .is_ok()
            );
        }
        assert!(matches!(
            sockets.send(&owner, conn.id, WsMessage::text("behind"), None),
            Err(SendError::Backpressure)
        ));
        sockets.close(&owner, conn.id, 4802, "slow".into());
        let mut received_close = false;
        while let Ok(outgoing) = conn.outgoing.try_recv() {
            if let Outgoing::Close(4802, _) = outgoing {
                received_close = true;
            }
        }
        assert!(
            received_close,
            "4802 is lost when the 256-frame data queue is full"
        );
    }

    #[tokio::test]
    async fn close_discards_a_full_byte_queue_and_releases_its_budget() {
        let sockets = Sockets::default();
        let owner = actor("slow-bytes");
        let mut conn = sockets.open(owner.clone()).unwrap();
        sockets.accept(conn.id);
        assert!(
            sockets
                .send(
                    &owner,
                    conn.id,
                    WsMessage::binary(vec![0; MAX_QUEUED_BYTES]),
                    None
                )
                .is_ok()
        );
        assert!(matches!(
            sockets.send(&owner, conn.id, WsMessage::text("x"), None),
            Err(SendError::Backpressure)
        ));
        sockets.close(&owner, conn.id, 4802, "slow".into());
        sockets.close_all(1012, "shutdown"); // The first close wins.
        assert!(matches!(conn.outgoing.recv().await,
            Some(Outgoing::Close(4802, reason)) if reason == "slow"));
        assert!(conn.outgoing.recv().await.is_none());
        assert_eq!(conn.queued.load(Ordering::Acquire), 0);
        sockets.remove(conn.id);
        assert_eq!(sockets.len(), 0);
    }

    #[test]
    fn dropped_writer_releases_queued_and_in_flight_bytes() {
        let sockets = Sockets::default();
        let owner = actor("aborted");
        let mut conn = sockets.open(owner.clone()).unwrap();
        sockets.accept(conn.id);
        for _ in 0..2 {
            assert!(
                sockets
                    .send(&owner, conn.id, WsMessage::text("data"), None)
                    .is_ok()
            );
        }
        let Outgoing::Message(msg) = conn.outgoing.try_recv().unwrap() else {
            panic!()
        };
        let writing = WritingBytes(Arc::clone(&conn.queued), queued_size(&msg));
        drop(conn.outgoing);
        assert_eq!(conn.queued.load(Ordering::Acquire), 4);
        drop(writing);
        assert_eq!(conn.queued.load(Ordering::Acquire), 0);
    }

    #[test]
    fn queued_bytes_are_capped_per_connection() {
        let sockets = Sockets::default();
        let a = actor("a");
        let mut conn = sockets.open(a.clone()).unwrap();
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
        // The writer took and wrote one: room again.
        assert!(matches!(conn.outgoing.try_recv(), Ok(Outgoing::Message(_))));
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
        const CAP: usize = 2;
        let sockets = Sockets::new(
            crate::SocketLimits {
                per_actor: CAP,
                ..Default::default()
            },
            crate::SocketBudget::new(3, 3),
        );
        let a = actor("a");
        for _ in 0..CAP {
            sockets.open(a.clone()).unwrap();
        }
        assert!(matches!(sockets.open(a.clone()), Err(OpenError::ActorFull)));
        assert!(
            sockets.open(actor("b")).is_ok(),
            "other actors are not affected"
        );
        assert_eq!(sockets.len(), CAP + 1);
    }

    #[test]
    fn closing_sockets_keep_their_capacity_until_the_close_event_finishes() {
        const CAP: usize = 2;
        let sockets = Sockets::new(
            crate::SocketLimits {
                per_actor: CAP,
                ..Default::default()
            },
            crate::SocketBudget::new(3, 3),
        );
        let a = actor("a");
        let first = sockets.open(a.clone()).unwrap();
        sockets.accept(first.id);
        for _ in 1..CAP {
            sockets.open(a.clone()).unwrap();
        }
        sockets.close(&a, first.id, 1000, "done".into());
        assert_eq!(sockets.len(), CAP);
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
    fn a_delete_marks_its_actors_sockets_but_not_one_being_opened() {
        let sockets = Sockets::default();
        let (a, b) = (actor("a"), actor("b"));
        let mut open = sockets.open(a.clone()).unwrap();
        sockets.accept(open.id);
        // The client closed it; its close event has not run yet.
        let closing = sockets.open(a.clone()).unwrap();
        sockets.accept(closing.id);
        sockets.begin_close(closing.id);
        let mut pending = sockets.open(a.clone()).unwrap();
        let other = sockets.open(b.clone()).unwrap();
        sockets.accept(other.id);
        sockets.close_deleted(&a, 1000, "actor deleted");
        assert!(matches!(
            open.outgoing.try_recv(),
            Ok(Outgoing::Close(1000, _))
        ));
        assert!(sockets.deleted(open.id));
        assert!(sockets.deleted(closing.id));
        assert!(!sockets.deleted(pending.id), "its open decides");
        assert!(matches!(
            pending.outgoing.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
        assert!(!sockets.deleted(other.id));
        // Gone from the registry: nothing to skip.
        sockets.remove(open.id);
        assert!(!sockets.deleted(open.id));
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
        assert_eq!(sockets.owners().len(), 2);
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
