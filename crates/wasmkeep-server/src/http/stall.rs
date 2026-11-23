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
use std::task::{Context, Poll, ready};
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

/// `io`, whose writes fail with `TimedOut` once one has been waiting, with
/// no byte accepted, for `limit`.
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

    /// The inner write returned `polled`: progress clears the deadline; a
    /// wait arms it (once) and fails when it passes.
    fn after<T>(
        &mut self,
        cx: &mut Context<'_>,
        polled: Poll<io::Result<T>>,
    ) -> Poll<io::Result<T>> {
        if polled.is_ready() {
            self.stalled = None;
            return polled;
        }
        let limit = self.limit;
        let deadline = self
            .stalled
            .get_or_insert_with(|| Box::pin(tokio::time::sleep(limit)));
        ready!(deadline.as_mut().poll(cx));
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

impl<I: AsyncWrite + Unpin> AsyncWrite for WriteIdle<I> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let polled = Pin::new(&mut self.io).poll_write(cx, buf);
        self.after(cx, polled)
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let polled = Pin::new(&mut self.io).poll_write_vectored(cx, bufs);
        self.after(cx, polled)
    }

    fn is_write_vectored(&self) -> bool {
        self.io.is_write_vectored()
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let polled = Pin::new(&mut self.io).poll_flush(cx);
        self.after(cx, polled)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let polled = Pin::new(&mut self.io).poll_shutdown(cx);
        self.after(cx, polled)
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

    #[tokio::test]
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
}
