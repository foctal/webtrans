//! WebTransport send stream wrapper around `quion::SendStream`.

use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
};

use bytes::{Buf, Bytes};

use crate::WriteError;

/// A stream that can be used to send bytes. See [`quion::SendStream`].
///
/// This wrapper exists primarily to adapt error codes. WebTransport uses u32 error
/// codes that map into the reserved HTTP/3 error space.
pub struct SendStream {
    stream: quion::SendStream,
    driver: Option<std::sync::Arc<dyn Send + Sync>>,
    reliable_prefix: Option<quion::VarInt>,
    priority: u16,
}

impl SendStream {
    pub(crate) fn new(stream: quion::SendStream) -> Self {
        Self {
            stream,
            driver: None,
            reliable_prefix: None,
            priority: 0,
        }
    }

    pub(crate) fn with_reliable_prefix(
        stream: quion::SendStream,
        size: usize,
        enabled: bool,
    ) -> Self {
        Self {
            stream,
            driver: None,
            reliable_prefix: enabled.then(|| quion::VarInt::from_u32(size as u32)),
            priority: 0,
        }
    }

    pub(crate) fn with_driver(mut self, driver: Option<std::sync::Arc<dyn Send + Sync>>) -> Self {
        self.driver = driver;
        self
    }

    /// Abruptly reset the stream with the provided error code. See [`quion::SendStream::reset`].
    /// WebTransport uses a u32 because it shares the error space with HTTP/3.
    /// Negotiated reliable resets preserve the session preface, including when
    /// the stream is reset before any application bytes are written.
    pub fn reset(&mut self, code: u32) -> Result<(), WriteError> {
        let code = webtrans_proto::error_to_http3(code);
        let code = quion::VarInt::try_from(code).unwrap();
        match self.reliable_prefix {
            Some(size) => self.stream.reset_at(code, size),
            None => self.stream.reset(code),
        }
        .map_err(Into::into)
    }

    /// Wait for STOP_SENDING or acknowledged completion.
    pub async fn stopped(&mut self) -> Result<Option<u32>, WriteError> {
        self.stream
            .stopped()
            .await
            .map(|code| code.and_then(|code| webtrans_proto::error_from_http3(code.into_inner())))
            .map_err(Into::into)
    }

    /// Write some data to the stream, returning the size written. See [`quion::SendStream::write`].
    pub async fn write(&mut self, buf: &[u8]) -> Result<usize, WriteError> {
        self.stream.write(buf).await.map_err(Into::into)
    }

    /// Write all of the data to the stream. See [`quion::SendStream::write_all`].
    pub async fn write_all(&mut self, buf: &[u8]) -> Result<(), WriteError> {
        self.stream.write_all(buf).await.map_err(Into::into)
    }

    /// Write a complete byte chunk.
    pub async fn write_chunk(&mut self, buf: Bytes) -> Result<(), WriteError> {
        self.write_all(&buf).await
    }

    /// Write all chunks, advancing each buffer as bytes are accepted.
    pub async fn write_all_chunks(&mut self, bufs: &mut [Bytes]) -> Result<(), WriteError> {
        for buf in bufs {
            while !buf.is_empty() {
                let written = self.write(buf).await?;
                buf.advance(written);
            }
        }
        Ok(())
    }

    /// Mark the stream as finished so no more data can be written. See [`quion::SendStream::finish`].
    ///
    /// WARNING: Quion implicitly calls this on drop. Dropping futures can lead to
    /// incomplete writes, so prefer explicit shutdown when possible.
    pub fn finish(&mut self) -> Result<(), WriteError> {
        self.stream.finish().map_err(Into::into)
    }

    /// Set scheduling priority. Lower values are sent first.
    pub fn set_priority(&mut self, order: u16) {
        self.priority = order;
        self.stream.set_priority(quion::StreamPriority(order));
    }

    /// Return the current scheduling priority.
    pub fn priority(&self) -> u16 {
        self.priority
    }

    /// Return the underlying QUIC stream ID.
    ///
    /// The connection also carries HTTP/3 control streams and may carry other
    /// sessions, so consecutive WebTransport streams need not have consecutive
    /// QUIC stream indices.
    pub fn quic_id(&self) -> Option<quion::StreamId> {
        self.stream.id()
    }
}

impl tokio::io::AsyncWrite for SendStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        tokio::io::AsyncWrite::poll_write(Pin::new(&mut self.stream), cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_shutdown(cx)
    }
}

impl webtrans_trait::SendStream for SendStream {
    type Error = WriteError;

    fn set_priority(&mut self, order: u8) {
        Self::set_priority(self, order.into());
    }

    fn reset(&mut self, code: u32) {
        Self::reset(self, code).ok();
    }

    fn finish(&mut self) -> Result<(), Self::Error> {
        Self::finish(self)
    }

    async fn write(&mut self, buf: &[u8]) -> Result<usize, Self::Error> {
        Self::write(self, buf).await
    }

    async fn write_buf<B: Buf + Send>(&mut self, buf: &mut B) -> Result<usize, Self::Error> {
        let size = self.write(buf.chunk()).await?;
        buf.advance(size);
        Ok(size)
    }

    async fn write_chunk(&mut self, chunk: Bytes) -> Result<(), Self::Error> {
        self.write_chunk(chunk).await
    }

    async fn closed(&mut self) -> Result<(), Self::Error> {
        match self.stopped().await? {
            Some(code) => Err(WriteError::Stopped(code)),
            None => Ok(()),
        }
    }
}

impl std::fmt::Debug for SendStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SendStream")
            .field("id", &self.quic_id())
            .finish_non_exhaustive()
    }
}
