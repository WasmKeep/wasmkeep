//! Cuts a connection whose writes make no progress for too long: a client
//! that asks for a response and then stops reading would otherwise hold
//! its connection slot, and the response, for as long as it likes.
//!
//! Only a write that is waiting counts. A connection with nothing to
//! write (kept alive between requests, an idle WebSocket) is never cut by
//! this, nor is a slow reader that keeps taking bytes.

use std::future::Future;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

/// A transport that can be offered bytes outside its readiness. A TCP
/// socket takes bytes as soon as any room frees in its send buffer, but
/// reports room to write only once about a third of the buffer is free
/// (and the client's receive window can hold back longer still): a slow
/// reader would look stalled for longer than it is. Offered bytes once
/// the limit passes tell a stalled client from a slow one.
pub(crate) trait Offer {
    /// Writes what of `buf` the transport takes now: `None` when it takes
    /// nothing (or cannot be asked).
    fn offer(&self, buf: &[u8]) -> Option<io::Result<usize>>;
}

impl Offer for tokio::net::TcpStream {
    #[cfg(unix)]
    fn offer(&self, buf: &[u8]) -> Option<io::Result<usize>> {
        // The socket is non-blocking: this takes what fits, or fails with
        // EAGAIN. Nothing of `buf` was written before (its poll was
        // pending), so the stream's order holds.
        match rustix::io::write(self, buf) {
            Ok(0) => None,
            Ok(n) => Some(Ok(n)),
            Err(rustix::io::Errno::AGAIN | rustix::io::Errno::INTR) => None,
            Err(e) => Some(Err(e.into())),
        }
    }

    #[cfg(not(unix))]
    fn offer(&self, _buf: &[u8]) -> Option<io::Result<usize>> {
        None
    }
}

impl Offer for tokio::io::DuplexStream {
    fn offer(&self, _buf: &[u8]) -> Option<io::Result<usize>> {
        None
    }
}

/// `io`, whose writes fail with `TimedOut` once one has been waiting for
/// `limit` and `io` takes none of its bytes then.
pub(crate) struct WriteIdle<I> {
    io: I,
    limit: Duration,
    /// Set while a write waits: when it gives up.
    stalled: Option<Pin<Box<tokio::time::Sleep>>>,
}

impl<I> WriteIdle<I> {
    pub(crate) fn new(io: I, limit: Duration) -> Self {
        Self {
            io,
            limit,
            stalled: None,
        }
    }

    /// Whether the write that waits has waited `limit` (arming the
    /// deadline on the first wait).
    fn timed_out(&mut self, cx: &mut Context<'_>) -> bool {
        let limit = self.limit;
        let deadline = self
            .stalled
            .get_or_insert_with(|| Box::pin(tokio::time::sleep(limit)));
        deadline.as_mut().poll(cx).is_ready()
    }

    /// The inner call returned `polled`: progress clears the deadline; a
    /// wait arms it (once) and, when it passes, fails unless `offered`
    /// (the bytes `io` takes then) shows progress.
    fn after<T>(
        &mut self,
        cx: &mut Context<'_>,
        polled: Poll<io::Result<T>>,
        offered: impl FnOnce(&I) -> Option<io::Result<T>>,
    ) -> Poll<io::Result<T>> {
        if polled.is_ready() {
            self.stalled = None;
            return polled;
        }
        if !self.timed_out(cx) {
            return Poll::Pending;
        }
        if let Some(taken) = offered(&self.io) {
            self.stalled = None;
            return Poll::Ready(taken);
        }
        Poll::Ready(Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "the peer read nothing for the write-idle limit",
        )))
    }
}

impl<I: AsyncRead + Unpin> AsyncRead for WriteIdle<I> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_read(cx, buf)
    }
}

impl<I: AsyncWrite + Offer + Unpin> AsyncWrite for WriteIdle<I> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let polled = Pin::new(&mut self.io).poll_write(cx, buf);
        self.after(cx, polled, |io| io.offer(buf))
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let polled = Pin::new(&mut self.io).poll_write_vectored(cx, bufs);
        self.after(cx, polled, |io| {
            bufs.iter()
                .find(|b| !b.is_empty())
                .and_then(|b| io.offer(b))
        })
    }

    fn is_write_vectored(&self) -> bool {
        self.io.is_write_vectored()
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let polled = Pin::new(&mut self.io).poll_flush(cx);
        self.after(cx, polled, |_| None)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let polled = Pin::new(&mut self.io).poll_shutdown(cx);
        self.after(cx, polled, |_| None)
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)] // test code

    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[tokio::test]
    async fn a_write_nobody_reads_fails_after_the_limit() {
        let (_reader, io) = tokio::io::duplex(4);
        let mut io = WriteIdle::new(io, Duration::from_millis(100));
        let err = tokio::time::timeout(Duration::from_secs(5), io.write_all(b"0123456789"))
            .await
            .unwrap()
            .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::TimedOut);
    }

    #[tokio::test]
    async fn time_with_nothing_to_write_does_not_count() {
        let (mut reader, io) = tokio::io::duplex(4);
        let mut io = WriteIdle::new(io, Duration::from_millis(50));
        tokio::time::sleep(Duration::from_millis(200)).await;
        io.write_all(b"ab").await.unwrap();
        let mut buf = [0u8; 2];
        reader.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"ab");
    }

    #[tokio::test(start_paused = true)]
    async fn each_accepted_byte_restarts_the_limit() {
        let (mut reader, io) = tokio::io::duplex(1);
        let mut io = WriteIdle::new(io, Duration::from_millis(100));
        let read = tokio::spawn(async move {
            let mut got = Vec::new();
            let mut buf = [0u8; 1];
            while got.len() < 10 {
                tokio::time::sleep(Duration::from_millis(40)).await;
                reader.read_exact(&mut buf).await.unwrap();
                got.push(buf[0]);
            }
            got
        });
        // About 400 ms in all, well past the limit, yet never 100 ms idle.
        io.write_all(b"0123456789").await.unwrap();
        assert_eq!(read.await.unwrap(), b"0123456789");
    }

    /// A real socket whose client never reads is cut, though the offer at
    /// the limit is tried.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_socket_whose_client_never_reads_is_cut() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let _client = tokio::net::TcpStream::connect(addr).await.unwrap();
        let (server, _) = listener.accept().await.unwrap();
        let mut io = WriteIdle::new(server, Duration::from_millis(300));
        let chunk = vec![0u8; 1 << 20];
        let err = tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                if let Err(e) = io.write_all(&chunk).await {
                    return e;
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(err.kind(), io::ErrorKind::TimedOut);
    }

    /// A socket with a large send buffer reports room only once about a
    /// third of it is free, which a slow reader takes long to free; the
    /// offer at the limit finds the room it freed, so the writer goes on.
    #[cfg(target_os = "linux")]
    #[tokio::test(flavor = "multi_thread")]
    async fn a_slow_reader_behind_a_large_send_buffer_is_not_cut() {
        let socket = tokio::net::TcpSocket::new_v4().unwrap();
        socket.set_send_buffer_size(4 << 20).unwrap();
        socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let listener = socket.listen(1).unwrap();
        let addr = listener.local_addr().unwrap();
        let client = tokio::net::TcpSocket::new_v4().unwrap();
        client.set_recv_buffer_size(64 * 1024).unwrap();
        let mut client = client.connect(addr).await.unwrap();
        let (server, _) = listener.accept().await.unwrap();
        let mut io = WriteIdle::new(server, Duration::from_millis(500));
        let writer = tokio::spawn(async move {
            let chunk = vec![0u8; 64 * 1024];
            loop {
                if let Err(e) = io.write_all(&chunk).await {
                    return e;
                }
            }
        });
        // About 320 KiB/s: freeing a third of an 8 MiB buffer takes over
        // 8 s, sixteen times the limit.
        let started = std::time::Instant::now();
        let mut buf = vec![0u8; 32 * 1024];
        while started.elapsed() < Duration::from_secs(5) {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let mut chunk = 0;
            while chunk < buf.len() {
                match client.read(&mut buf[chunk..]).await {
                    Ok(0) | Err(_) => panic!("the slow reader was cut"),
                    Ok(n) => chunk += n,
                }
            }
            assert!(!writer.is_finished(), "the writer gave up");
        }
        writer.abort();
    }
}
