//! WebSocket events: opening, messages and closing, and the task that
//! runs one connection.

use super::*;

impl ActorState {
    /// `on-websocket-open`: whether to keep connection `conn`.
    /// `Err` is a host failure (logged, not shown to the client); `Ok`
    /// carries the guest's own decision.
    pub(super) async fn ws_open(
        &mut self,
        conn: u64,
        path: String,
    ) -> Result<Result<(), String>, String> {
        self.chain = Arc::from(Vec::new());
        // Sockets come from outside: under `existing` the actor must exist,
        // checked in this event (it may have been deleted since the
        // upgrade's check), with the rules of the class it takes.
        match self.admit(true).await? {
            Admitted::Yes => {}
            Admitted::Missing => return Err("no such actor".into()),
            Admitted::Unavailable(why) => return Err(why),
            Admitted::Forbidden => return Err("not reachable from outside".into()),
            Admitted::Unconfigured(name) => return Err(format!("{name} is not set")),
        }
        match self.ensure_owner().await {
            Ok(()) => {}
            // Rare: ownership changed between the upgrade's check and now.
            Err(OwnershipError::NotOwner { owner, .. }) => {
                return Err(format!("actor runs on node {owner}"));
            }
            // This node started draining since: the client reconnects and
            // its upgrade goes to the peer.
            Err(OwnershipError::Elsewhere { peer }) => {
                return Err(format!("node draining; actor moves to node {peer}"));
            }
            Err(
                OwnershipError::Unavailable(e)
                | OwnershipError::Refused(e)
                | OwnershipError::Broken(e),
            ) => {
                return Err(format!("lease: {e}"));
            }
        }
        self.ensure_ready().await?;
        self.install_call_context();
        let live = self.live.as_mut().ok_or("actor not ready")?;
        live.start_event("websocket");
        // During its own open event the guest may send to the new socket
        // and sees it in `connections`; no other event does until accepted.
        live.store.data_mut().opening = Some(conn);
        let run = limit(
            self.limits.wall,
            self.lease.as_ref(),
            live.core
                .call_on_websocket_open(&mut live.store, conn, &path),
        )
        .await;
        if let Some(live) = self.live.as_mut() {
            live.store.data_mut().opening = None;
        }
        let trapped = !matches!(run, Ok(Ok(_)));
        // Accepted in this same event, so the very next event (a message
        // already queued behind it) sees the socket.
        if matches!(run, Ok(Ok(Ok(())))) {
            self.sockets.accept(conn);
        }
        self.finish_event(trapped).await;
        match run {
            Ok(Ok(accepted)) => Ok(accepted),
            Ok(Err(trap)) => Err(format!("on-websocket-open trapped: {trap:#}")),
            Err(cut) => Err(format!("on-websocket-open {cut}")),
        }
    }

    /// `on-websocket-message`.
    pub(super) async fn ws_message(&mut self, conn: u64, msg: WsMessageWit) {
        // Closed by the server since the frame was read: not delivered (nor
        // may it wake the actor or claim it).
        if !self.sockets.is_open(&self.address, conn) {
            return;
        }
        if !self.ws_prepare(conn).await {
            return;
        }
        let Some(live) = self.live.as_mut() else {
            return;
        };
        let run = limit(
            self.limits.wall,
            self.lease.as_ref(),
            live.core
                .call_on_websocket_message(&mut live.store, conn, &msg),
        )
        .await;
        self.ws_done(matches!(run, Ok(Ok(())))).await;
    }

    /// `on-websocket-close`.
    pub(super) async fn ws_close(&mut self, conn: u64, code: u16, reason: String) {
        if !self.ws_prepare(conn).await {
            return;
        }
        let Some(live) = self.live.as_mut() else {
            return;
        };
        let run = limit(
            self.limits.wall,
            self.lease.as_ref(),
            live.core
                .call_on_websocket_close(&mut live.store, conn, code, &reason),
        )
        .await;
        self.ws_done(matches!(run, Ok(Ok(())))).await;
    }

    /// Readies the instance for a WebSocket event; false if it cannot run.
    pub(super) async fn ws_prepare(&mut self, conn: u64) -> bool {
        self.chain = Arc::from(Vec::new());
        // A socket event never creates an actor of an `existing` class: one
        // deleted under an open socket stays deleted. A class a deploy made
        // internal closes its sockets (ADR 0013).
        match self.admit(true).await {
            Ok(Admitted::Yes) => {}
            Ok(Admitted::Unconfigured(_)) => {
                self.sockets.close(
                    &self.address,
                    conn,
                    1011,
                    "a required value is not set".into(),
                );
                return false;
            }
            Ok(Admitted::Forbidden) => {
                self.sockets.close(
                    &self.address,
                    conn,
                    1008,
                    "not reachable from outside".into(),
                );
                return false;
            }
            // Whether it exists could not be told now: reconnect later.
            Ok(Admitted::Unavailable(_)) => {
                self.sockets.close(
                    &self.address,
                    conn,
                    1012,
                    "actor unavailable; reconnect".into(),
                );
                return false;
            }
            Err(e) if e.starts_with(CLASS_GONE) => {
                self.sockets.close(
                    &self.address,
                    conn,
                    1012,
                    "actor unavailable; reconnect".into(),
                );
                return false;
            }
            Ok(Admitted::Missing) | Err(_) => {
                self.sockets
                    .close(&self.address, conn, 1000, "no such actor".into());
                return false;
            }
        }
        // A socket on an actor this node no longer holds in memory (it
        // hibernated with the socket open): on a draining node that closes
        // it, and the client's reconnect goes to the peer (ADR 0014).
        if let Err(e) = self.ensure_owner().await {
            tracing::warn!(app = %crate::logging::app(&self.address), actor = %crate::logging::actor(&self.address), error = ?e, "websocket event: no longer the owner");
            match e {
                // Another node owns the actor, or this draining node sends
                // it to a peer: every socket of the actor here is on the
                // wrong node, not only this one. Close them all, so a
                // client that only listens reconnects too instead of
                // missing every broadcast.
                OwnershipError::NotOwner { .. } | OwnershipError::Elsewhere { .. } => {
                    let closed =
                        self.sockets
                            .close_owner(&self.address, 1012, "actor moved; reconnect");
                    crate::metrics::owner_closes("event", closed);
                }
                // Not now (the bucket, a write without an answer): this
                // socket reconnects; the others try on their next frame.
                OwnershipError::Unavailable(_)
                | OwnershipError::Refused(_)
                | OwnershipError::Broken(_) => {
                    self.sockets
                        .close(&self.address, conn, 1012, "actor moved; reconnect".into());
                }
            }
            return false;
        }
        if let Err(e) = self.ensure_ready().await {
            // Close rather than drop the message silently: the client
            // reconnects, and is answered (or refused) like a new socket.
            tracing::warn!(app = %crate::logging::app(&self.address), actor = %crate::logging::actor(&self.address), error = %e, "websocket event: actor not ready");
            self.sockets.close(
                &self.address,
                conn,
                1012,
                "actor unavailable; reconnect".into(),
            );
            return false;
        }
        self.install_call_context();
        match self.live.as_mut() {
            Some(live) => {
                live.start_event("websocket");
                true
            }
            None => false,
        }
    }

    pub(super) async fn ws_done(&mut self, ok: bool) {
        if !ok {
            tracing::warn!(app = %crate::logging::app(&self.address), actor = %crate::logging::actor(&self.address), "websocket event trapped; instance discarded");
        }
        self.finish_event(!ok).await;
    }
}

/// Close reasons are limited to 123 bytes by the protocol.
pub(super) fn truncate_reason(mut reason: String) -> String {
    if reason.len() > 123 {
        let mut cut = 123;
        while !reason.is_char_boundary(cut) {
            cut -= 1;
        }
        reason.truncate(cut);
    }
    reason
}

/// A socket registered with its actor and past `on-websocket-open`, whose
/// stream does not exist yet (the upgrade answers 101 after this).
pub(super) struct Accepted {
    address: ActorAddress,
    id: u64,
    outgoing: tokio::sync::mpsc::Receiver<crate::websocket::Outgoing>,
    queued: Arc<std::sync::atomic::AtomicUsize>,
    closing: Arc<tokio::sync::Notify>,
    /// The close to send instead of serving it: the guest refused it, or
    /// the open event failed.
    refusal: Option<(u16, String)>,
}

impl ServiceInner {
    /// Registers a socket and runs `on-websocket-open`, before the upgrade
    /// answers 101, as a Durable Object accepts a socket before answering:
    /// an event the actor runs after the client sees 101 (a broadcast, say)
    /// sees the socket, and what it sends waits in the socket's queue for
    /// the stream. `Err` when the actor or the node has too many sockets.
    pub(super) async fn accept_socket(
        &self,
        address: ActorAddress,
        path: String,
    ) -> Result<Accepted, crate::websocket::OpenError> {
        let opened = self.sockets.open(address.clone())?;
        let id = opened.id;
        let result = self
            .mailboxes
            .submit_wait(&address, self.wall, move |s: &mut ActorState| {
                Box::pin(s.ws_open(id, path))
            })
            .await;
        // Only the guest's own reason reaches the client; host failures
        // (bucket errors, traps, node names) are logged, not shown.
        let refusal = match result {
            Ok(Ok(Ok(()))) => None,
            Ok(Ok(Err(reason))) => Some((1008, reason)),
            Err(MailboxError::Capacity(_)) => Some((1013, "try again later".to_owned())),
            Ok(Err(e)) => {
                tracing::warn!(app = %crate::logging::app(&address), actor = %crate::logging::actor(&address), error = %e, "websocket open failed");
                Some((1008, "unavailable".to_owned()))
            }
            Err(e) => {
                tracing::warn!(app = %crate::logging::app(&address), actor = %crate::logging::actor(&address), error = %e, "websocket open failed");
                Some((1008, "unavailable".to_owned()))
            }
        };
        Ok(Accepted {
            address,
            id,
            outgoing: opened.outgoing,
            queued: opened.queued,
            closing: opened.closing,
            refusal,
        })
    }

    /// The upgrade failed after [`Self::accept_socket`]: the client is gone
    /// before its stream existed, so the actor sees an abnormal close.
    pub(super) async fn abandon_socket(self: Arc<Self>, accepted: Accepted) {
        let Accepted {
            address,
            id,
            outgoing,
            refusal,
            ..
        } = accepted;
        drop(outgoing);
        if refusal.is_none() {
            self.sockets.begin_close(id);
            self.deliver_close(&address, id, 1006, String::new()).await;
        }
        self.sockets.remove(id);
    }

    /// Owns one upgraded socket, accepted by [`Self::accept_socket`]: sends
    /// what the actor queued for it, then turns each message into an
    /// event, in order.
    pub(super) async fn run_socket<S>(
        self: Arc<Self>,
        accepted: Accepted,
        ws: tokio_tungstenite::WebSocketStream<S>,
    ) where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
    {
        use futures_util::{SinkExt, StreamExt};

        let Accepted {
            address,
            id,
            mut outgoing,
            queued,
            closing,
            refusal,
        } = accepted;
        let (mut sink, mut stream) = ws.split();
        let mut writer = tokio::spawn(async move {
            while let Some(out) = outgoing.recv().await {
                let msg = match out {
                    Outgoing::Message(m) => m,
                    Outgoing::Close(code, reason) => {
                        let _ = sink.send(close_frame(code, reason)).await;
                        break;
                    }
                };
                let size = crate::websocket::queued_size(&msg);
                let sent = sink.send(msg).await;
                queued.fetch_sub(size, std::sync::atomic::Ordering::AcqRel);
                if sent.is_err() {
                    break;
                }
            }
            let _ = sink.close().await;
        });

        if let Some((code, reason)) = refusal {
            self.sockets
                .close(&address, id, code, truncate_reason(reason));
            if tokio::time::timeout(self.wall, &mut writer).await.is_err() {
                writer.abort();
                let _ = writer.await;
            }
            self.sockets.remove(id);
            return;
        }

        let (mut code, mut reason) = (1006_u16, String::new());
        let mut pinged = false;
        // Set once the server closed the socket: from then on no frame is
        // delivered, and the client's close is awaited until then only, so
        // a client that ignores the close (pinging on, say) cannot keep
        // the socket and its slot, or drive its actor.
        let mut closed_by_us: Option<tokio::time::Instant> = None;
        loop {
            // A silent client is pinged, then closed if it stays silent:
            // half-open connections would otherwise live forever.
            let wait = if pinged {
                crate::websocket::PONG_WAIT
            } else {
                self.ping_after
            };
            let next = match closed_by_us {
                Some(deadline) => match tokio::time::timeout_at(deadline, stream.next()).await {
                    Ok(next) => Ok(next),
                    Err(_) => break, // no close from the client: 1006
                },
                None => tokio::select! {
                    biased;
                    () = closing.notified() => {
                        closed_by_us = Some(tokio::time::Instant::now() + self.close_wait);
                        continue;
                    }
                    next = tokio::time::timeout(wait, stream.next()) => next,
                },
            };
            let frame = match next {
                Ok(Some(frame)) => frame,
                Ok(None) => break,
                Err(_) if pinged => {
                    self.sockets.close(&address, id, 1001, "idle".into());
                    code = 1001;
                    break;
                }
                Err(_) => {
                    pinged = true;
                    let _ =
                        self.sockets
                            .send(&address, id, WsMessage::Ping(bytes::Bytes::new()), None);
                    continue;
                }
            };
            if closed_by_us.is_some() {
                match frame {
                    Ok(WsMessage::Close(frame)) => {
                        (code, reason) = match frame {
                            Some(f) => (u16::from(f.code), f.reason.to_string()),
                            None => (1005, String::new()),
                        };
                        break;
                    }
                    Ok(_) => continue,
                    Err(_) => break,
                }
            }
            pinged = false;
            let msg = match frame {
                Ok(WsMessage::Text(t)) => WsMessageWit::Text(t.to_string()),
                Ok(WsMessage::Binary(b)) => WsMessageWit::Binary(b.to_vec()),
                Ok(WsMessage::Close(frame)) => {
                    if let Some(f) = frame {
                        code = u16::from(f.code);
                        reason = f.reason.to_string();
                    } else {
                        code = 1005;
                    }
                    break;
                }
                Ok(_) => continue, // ping, pong, raw frames
                Err(_) => break,
            };
            // One event per message, in order; waiting on it is the backpressure.
            let delivered = self
                .mailboxes
                .submit_wait(&address, self.wall, move |s: &mut ActorState| {
                    Box::pin(s.ws_message(id, msg))
                })
                .await;
            if let Err(error) = delivered {
                code = if matches!(error, MailboxError::Capacity(_)) {
                    1013
                } else {
                    1011
                };
                self.sockets
                    .close(&address, id, code, "actor unavailable".into());
                break;
            }
        }
        // Hide the closed socket but keep its existing capacity slot until
        // cleanup is delivered. Otherwise clients could create an unbounded
        // number of pending close tasks by reconnecting during pressure.
        self.sockets.begin_close(id);
        // The connection itself closes now: `begin_close` ended the
        // writer's queue, so it sends what is left and closes the sink;
        // dropping the read half releases the descriptor. Only the
        // registry slot waits for `on-close`, which may be held back by
        // capacity for a while.
        if tokio::time::timeout(self.wall, &mut writer).await.is_err() {
            writer.abort();
            let _ = writer.await;
        }
        drop(stream);
        self.deliver_close(&address, id, code, reason).await;
        self.sockets.remove(id);
    }

    /// Delivers `on-websocket-close`, waiting for capacity as long as it
    /// takes: a close the actor never sees would leave its state wrong.
    async fn deliver_close(&self, address: &ActorAddress, id: u64, code: u16, reason: String) {
        let retry_jitter = std::collections::hash_map::RandomState::new();
        let mut retry_ceiling_ms = 20_u64;
        let mut refusals = 0_u64;
        loop {
            let reason = reason.clone();
            match self
                .mailboxes
                .submit_wait(address, ADMISSION_WAIT, move |s: &mut ActorState| {
                    Box::pin(s.ws_close(id, code, reason))
                })
                .await
            {
                Ok(()) => break,
                // These refusals guarantee the event never ran. Keep it
                // pending until accepted or shutdown; never replay Gone.
                Err(MailboxError::Capacity(_) | MailboxError::Full(_)) => {
                    let floor = retry_ceiling_ms / 2;
                    let random = std::hash::BuildHasher::hash_one(&retry_jitter, (id, refusals));
                    let delay = floor + random % (retry_ceiling_ms - floor + 1);
                    tokio::time::sleep(Duration::from_millis(delay)).await;
                    retry_ceiling_ms = (retry_ceiling_ms * 2).min(1_000);
                    refusals = refusals.wrapping_add(1);
                }
                Err(error) => {
                    tracing::debug!(app = %crate::logging::app(address), actor = %crate::logging::actor(address), error = %error, "websocket close not delivered");
                    break;
                }
            }
        }
    }
}
