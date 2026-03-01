//! Transport-agnostic traits for WebTransport sessions and streams.
//!
//! This crate defines the core trait contracts shared by native and WASM
//! backends, including error mapping, stream operations, and datagram support.

mod bounds;

use std::future::Future;

pub use crate::bounds::{MaybeSend, MaybeSync};
use bytes::{Buf, BufMut, Bytes, BytesMut};

/// Error trait for WebTransport operations.
///
/// Implementations must be Send + Sync + 'static to cross async boundaries.
pub trait Error: std::error::Error + MaybeSend + MaybeSync + 'static {
    /// Return the error code and reason if this was an application error.
    ///
    /// NOTE: Reasons are bytes on the wire, but are converted to `String` for convenience.
    fn session_error(&self) -> Option<(u32, String)>;

    /// Return the error code if this was a stream error.
    fn stream_error(&self) -> Option<u32> {
        None
    }
}

/// A WebTransport session that can accept/create streams and send/receive datagrams.
///
/// The session can be cloned to create multiple handles.
/// The session will be closed on drop.
pub trait Session: Clone + MaybeSend + MaybeSync + 'static {
    /// Outgoing stream type returned by `open_*` and `accept_bi`.
    type SendStream: SendStream;
    /// Incoming stream type returned by `accept_*` and `open_bi`.
    type RecvStream: RecvStream;
    /// Error type returned by session operations.
    type Error: Error;

    /// Block until the peer creates a new unidirectional stream.
    fn accept_uni(&self)
    -> impl Future<Output = Result<Self::RecvStream, Self::Error>> + MaybeSend;

    /// Block until the peer creates a new bidirectional stream.
    fn accept_bi(
        &self,
    ) -> impl Future<Output = Result<(Self::SendStream, Self::RecvStream), Self::Error>> + MaybeSend;

    /// Open a new bidirectional stream, which may block if too many streams are open.
    fn open_bi(
        &self,
    ) -> impl Future<Output = Result<(Self::SendStream, Self::RecvStream), Self::Error>> + MaybeSend;

    /// Open a new unidirectional stream, which may block if too many streams are open.
    fn open_uni(&self) -> impl Future<Output = Result<Self::SendStream, Self::Error>> + MaybeSend;

    /// Send a datagram over the network.
    ///
    /// QUIC datagrams may be dropped for any reason:
    /// - Network congestion.
    /// - Random packet loss.
    /// - Payload is larger than `max_datagram_size()`.
    /// - Peer is not receiving datagrams.
    /// - Peer has too many outstanding datagrams.
    /// - Implementation-specific limits.
    fn send_datagram(&self, payload: Bytes) -> Result<(), Self::Error>;

    /// Receive a datagram over the network.
    fn recv_datagram(&self) -> impl Future<Output = Result<Bytes, Self::Error>> + MaybeSend;

    /// Return the maximum size of a datagram that can be sent.
    fn max_datagram_size(&self) -> usize;

    /// Close the connection immediately with a code and reason.
    fn close(&self, code: u32, reason: &str);

    /// Block until the connection is closed by either side.
    fn closed(&self) -> impl Future<Output = Self::Error> + MaybeSend;
}

/// An outgoing stream of bytes to the peer.
///
/// QUIC streams have flow control, which means the send rate is limited by the peer's receive window.
/// The stream is closed with a graceful FIN when dropped.
pub trait SendStream: MaybeSend {
    /// Error type returned by send-side stream operations.
    type Error: Error;

    /// Write some of the buffer to the stream.
    fn write(&mut self, buf: &[u8])
    -> impl Future<Output = Result<usize, Self::Error>> + MaybeSend;

    /// Write the given buffer to the stream, advancing the internal position.
    fn write_buf<B: Buf + MaybeSend>(
        &mut self,
        buf: &mut B,
    ) -> impl Future<Output = Result<usize, Self::Error>> + MaybeSend {
        async move {
            let chunk = buf.chunk();
            let size = self.write(chunk).await?;
            buf.advance(size);
            Ok(size)
        }
    }

    /// Write the entire [Bytes] chunk to the stream, potentially avoiding a copy.
    fn write_chunk(
        &mut self,
        chunk: Bytes,
    ) -> impl Future<Output = Result<(), Self::Error>> + MaybeSend {
        async move {
            // Avoid a mutable binding for the argument.
            let mut c = chunk;
            self.write_buf(&mut c).await?;
            Ok(())
        }
    }

    /// Helper to write all data in the buffer.
    fn write_all(
        &mut self,
        buf: &[u8],
    ) -> impl Future<Output = Result<(), Self::Error>> + MaybeSend {
        async move {
            let mut pos = 0;
            while pos < buf.len() {
                pos += self.write(&buf[pos..]).await?;
            }
            Ok(())
        }
    }

    /// Helper to write all data in the buffer.
    fn write_all_buf<B: Buf + MaybeSend>(
        &mut self,
        buf: &mut B,
    ) -> impl Future<Output = Result<(), Self::Error>> + MaybeSend {
        async move {
            while buf.has_remaining() {
                self.write_buf(buf).await?;
            }
            Ok(())
        }
    }

    /// Set the stream's priority.
    ///
    /// Streams with lower values are sent first, but arrival order is not guaranteed.
    fn set_priority(&mut self, order: u8);

    /// Mark the stream as finished, erroring on any future writes.
    ///
    /// [SendStream::reset] can still be called to abandon queued data.
    /// [SendStream::closed] should return when the FIN is acknowledged by the peer.
    ///
    /// NOTE: Quinn implicitly calls this on drop, but it is a common footgun.
    /// Implementations should call [SendStream::reset] on drop instead.
    fn finish(&mut self) -> Result<(), Self::Error>;

    /// Immediately close the stream and discard any remaining data.
    ///
    /// This translates into a RESET_STREAM QUIC code.
    /// The peer may not receive the reset code if the stream is already closed.
    fn reset(&mut self, code: u32);

    /// Block until the stream is closed by either side.
    ///
    /// This includes:
    /// - We sent a RESET_STREAM via [SendStream::reset]
    /// - We received a STOP_SENDING via [RecvStream::stop]
    /// - A FIN is acknowledged by the peer via [SendStream::finish]
    ///
    /// Some implementations do not support FIN acknowledgement, in which case this blocks until the FIN is sent.
    ///
    /// NOTE: This takes `&mut` to match Quinn and simplify the implementation.
    fn closed(&mut self) -> impl Future<Output = Result<(), Self::Error>> + MaybeSend;
}

/// An incoming stream of bytes from the peer.
///
/// All bytes are flushed in order and the stream is flow controlled.
/// The stream is closed with STOP_SENDING code=0 when dropped.
pub trait RecvStream: MaybeSend {
    /// Error type returned by receive-side stream operations.
    type Error: Error;

    /// Read the next chunk of data, up to the max size.
    ///
    /// This returns a chunk of data instead of copying, which can be more efficient.
    fn read(
        &mut self,
        dst: &mut [u8],
    ) -> impl Future<Output = Result<Option<usize>, Self::Error>> + MaybeSend;

    /// Read some data into the provided buffer.
    ///
    /// The number of bytes read is returned, or `None` if the stream is closed.
    /// The buffer is advanced by the number of bytes read.
    fn read_buf<B: BufMut + MaybeSend>(
        &mut self,
        buf: &mut B,
    ) -> impl Future<Output = Result<Option<usize>, Self::Error>> + MaybeSend {
        async move {
            let dst = unsafe {
                std::mem::transmute::<&mut bytes::buf::UninitSlice, &mut [u8]>(buf.chunk_mut())
            };
            let size = match self.read(dst).await? {
                Some(size) => size,
                None => return Ok(None),
            };

            unsafe { buf.advance_mut(size) };

            Ok(Some(size))
        }
    }

    /// Read the next chunk of data, up to the max size.
    ///
    /// This returns a chunk of data instead of copying, which can be more efficient.
    fn read_chunk(
        &mut self,
        max: usize,
    ) -> impl Future<Output = Result<Option<Bytes>, Self::Error>> + MaybeSend {
        async move {
            // Avoid excessive allocation; provide your own buffer to increase this limit.
            let mut buf = BytesMut::with_capacity(max.min(8 * 1024));

            Ok(self.read_buf(&mut buf).await?.map(|_| buf.freeze()))
        }
    }

    /// Send a `STOP_SENDING` QUIC code, informing the peer that no more data will be read.
    ///
    /// Implementations must do this on drop to avoid leaking flow control.
    /// Call this method manually to specify a custom code.
    fn stop(&mut self, code: u32);

    /// Block until the stream has been closed by either side.
    ///
    /// This includes:
    /// - We received a RESET_STREAM via [SendStream::reset]
    /// - We sent a STOP_SENDING via [RecvStream::stop]
    /// - We received a FIN via [SendStream::finish] and read all data.
    fn closed(&mut self) -> impl Future<Output = Result<(), Self::Error>> + MaybeSend;

    /// Helper to keep reading until the stream is closed.
    fn read_all(&mut self) -> impl Future<Output = Result<Bytes, Self::Error>> + MaybeSend {
        async move {
            let mut buf = BytesMut::new();
            self.read_all_buf(&mut buf).await?;
            Ok(buf.freeze())
        }
    }

    /// Helper to keep reading until the buffer is full.
    fn read_all_buf<B: BufMut + MaybeSend>(
        &mut self,
        buf: &mut B,
    ) -> impl Future<Output = Result<usize, Self::Error>> + MaybeSend {
        async move {
            let mut size = 0;
            while buf.has_remaining_mut() {
                match self.read_buf(buf).await? {
                    Some(n) => size += n,
                    None => break,
                }
            }
            Ok(size)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Error, MaybeSend, RecvStream};
    use bytes::{Bytes, BytesMut};
    use futures::executor::block_on;
    use std::fmt;

    #[derive(Debug)]
    struct TestError;

    impl fmt::Display for TestError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "test error")
        }
    }

    impl std::error::Error for TestError {}

    impl Error for TestError {
        fn session_error(&self) -> Option<(u32, String)> {
            None
        }
    }

    struct TestRecvStream {
        data: Vec<u8>,
        pos: usize,
    }

    impl TestRecvStream {
        fn new(data: &[u8]) -> Self {
            Self {
                data: data.to_vec(),
                pos: 0,
            }
        }
    }

    impl RecvStream for TestRecvStream {
        type Error = TestError;

        fn read(
            &mut self,
            dst: &mut [u8],
        ) -> impl std::future::Future<Output = Result<Option<usize>, Self::Error>> + MaybeSend
        {
            async move {
                let available = self.data.len().saturating_sub(self.pos);
                if available == 0 {
                    return Ok(None);
                }

                let size = available.min(dst.len());
                let end = self.pos + size;
                dst[..size].copy_from_slice(&self.data[self.pos..end]);
                self.pos = end;

                Ok(Some(size))
            }
        }

        fn stop(&mut self, _code: u32) {}

        fn closed(
            &mut self,
        ) -> impl std::future::Future<Output = Result<(), Self::Error>> + MaybeSend {
            async { Ok(()) }
        }
    }

    #[test]
    fn read_chunk_respects_max_and_eof() {
        let mut stream = TestRecvStream::new(b"hello world");

        let first = block_on(stream.read_chunk(5)).unwrap().unwrap();
        assert_eq!(first, Bytes::from_static(b"hello"));

        let second = block_on(stream.read_chunk(1024)).unwrap().unwrap();
        assert_eq!(second, Bytes::from_static(b" world"));

        let end = block_on(stream.read_chunk(1)).unwrap();
        assert!(end.is_none());
    }

    #[test]
    fn read_buf_advances_buffer() {
        let mut stream = TestRecvStream::new(b"test");
        let mut buf = BytesMut::with_capacity(4);

        let size = block_on(stream.read_buf(&mut buf)).unwrap().unwrap();
        assert_eq!(size, 4);
        assert_eq!(&buf[..], b"test");
    }
}
