//! WebTransport receive stream wrapper around `quion::RecvStream`.

use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
};

use bytes::Bytes;

use crate::ReadError;

/// A stream that can be used to receive bytes. See [`quion::RecvStream`].
pub struct RecvStream {
    inner: quion::RecvStream,
    driver: Option<std::sync::Arc<dyn Send + Sync>>,
}

impl RecvStream {
    pub(crate) fn new(stream: quion::RecvStream) -> Self {
        Self {
            inner: stream,
            driver: None,
        }
    }

    pub(crate) fn with_driver(mut self, driver: Option<std::sync::Arc<dyn Send + Sync>>) -> Self {
        self.driver = driver;
        self
    }

    /// Tell the peer to stop sending data with the given error code. See [`quion::RecvStream::stop`].
    /// WebTransport uses a u32 because it shares the error space with HTTP/3.
    pub fn stop(&mut self, code: u32) -> Result<(), ReadError> {
        let code = webtrans_proto::error_to_http3(code);
        let code = quion::VarInt::try_from(code).unwrap();
        self.inner.stop(code).map_err(Into::into)
    }

    // Wrap Quion errors so they map into WebTransport error types.

    /// Read some data into the buffer and return the amount read. See [`quion::RecvStream::read`].
    pub async fn read(&mut self, buf: &mut [u8]) -> Result<Option<usize>, ReadError> {
        self.inner.read(buf).await.map_err(Into::into)
    }

    /// Fill the entire buffer with data. See [`quion::RecvStream::read_exact`].
    pub async fn read_exact(&mut self, buf: &mut [u8]) -> Result<(), ReadError> {
        self.inner.read_exact(buf).await.map_err(Into::into)
    }

    /// Read a chunk of data from the stream. See [`quion::RecvStream::read_chunk`].
    /// Quion does not allow switching from ordered to unordered reads after
    /// the session preface has been consumed; use `ordered = true`.
    pub async fn read_chunk(
        &mut self,
        max_length: usize,
        ordered: bool,
    ) -> Result<Option<quion::Chunk>, ReadError> {
        self.inner
            .read_chunk(max_length, ordered)
            .await
            .map_err(Into::into)
    }

    /// Read until the end of the stream or the limit is hit. See [`quion::RecvStream::read_to_end`].
    pub async fn read_to_end(&mut self, size_limit: usize) -> Result<Vec<u8>, ReadError> {
        self.inner.read_to_end(size_limit).await.map_err(Into::into)
    }

    /// Wait for reset or graceful completion after consuming any reliable prefix.
    pub async fn received_reset(&mut self) -> Result<Option<u32>, ReadError> {
        self.inner
            .received_reset()
            .await
            .map(|code| code.and_then(|code| webtrans_proto::error_from_http3(code.into_inner())))
            .map_err(Into::into)
    }

    /// Return the underlying QUIC stream ID.
    ///
    /// The connection also carries HTTP/3 control streams and may carry other
    /// sessions, so consecutive WebTransport streams need not have consecutive
    /// QUIC stream indices.
    pub fn quic_id(&self) -> Option<quion::StreamId> {
        self.inner.id()
    }

    // 0-RTT is intentionally not exposed because it is invalid for WebTransport.
}

impl tokio::io::AsyncRead for RecvStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl webtrans_trait::RecvStream for RecvStream {
    type Error = ReadError;

    fn stop(&mut self, code: u32) {
        Self::stop(self, code).ok();
    }

    async fn read(&mut self, dst: &mut [u8]) -> Result<Option<usize>, Self::Error> {
        self.read(dst).await
    }

    async fn read_chunk(&mut self, max: usize) -> Result<Option<Bytes>, Self::Error> {
        self.read_chunk(max, true)
            .await
            .map(|r| r.map(|chunk| chunk.bytes))
    }

    async fn closed(&mut self) -> Result<(), Self::Error> {
        match self.received_reset().await? {
            Some(code) => Err(ReadError::Reset(code)),
            None => Ok(()),
        }
    }
}

impl std::fmt::Debug for RecvStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RecvStream")
            .field("id", &self.quic_id())
            .finish_non_exhaustive()
    }
}
