use std::cmp;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::{BufMut, Bytes};
use futures_io::AsyncRead;
use js_sys::Uint8Array;
use web_sys::WebTransportReceiveStream;

use crate::Error;
use web_streams::Reader;

type ReadFuture = Pin<Box<dyn Future<Output = (Reader<Uint8Array>, io::Result<Option<Bytes>>)>>>;

enum ReadState {
    Idle,
    Reading(ReadFuture),
}

/// A byte stream received from the remote peer.
///
/// Either side may close with an error code, or the peer may close with a FIN.
pub struct RecvStream {
    reader: Option<Reader<Uint8Array>>,
    buffer: Bytes,
    read_state: ReadState,
}

impl RecvStream {
    pub(super) fn new(stream: WebTransportReceiveStream) -> Result<Self, Error> {
        let reader = Reader::new(&stream)?;

        Ok(Self {
            reader: Some(reader),
            buffer: Bytes::new(),
            read_state: ReadState::Idle,
        })
    }

    /// Read the next chunk of data with the provided maximum size.
    ///
    /// This returns a chunk of data instead of copying, which can be more efficient.
    pub async fn read(&mut self, max: usize) -> Result<Option<Bytes>, Error> {
        if !self.buffer.is_empty() {
            let size = cmp::min(max, self.buffer.len());
            let data = self.buffer.split_to(size);
            return Ok(Some(data));
        }

        let reader = self
            .reader
            .as_mut()
            .ok_or_else(|| Error::Unknown("reader is unavailable".into()))?;

        let mut data: Bytes = match reader.read().await? {
            Some(data) => Bytes::from(data.to_vec()),
            None => return Ok(None),
        };

        if data.len() > max {
            // The chunk is too large; buffer the remainder for the next read.
            self.buffer = data.split_off(max);
        }

        Ok(Some(data))
    }

    /// Read some data into the provided buffer.
    ///
    /// Returns the (non-zero) number of bytes read, or `None` if the stream is closed.
    /// Advances the buffer by the number of bytes read.
    pub async fn read_buf<B: BufMut>(&mut self, buf: &mut B) -> Result<Option<usize>, Error> {
        let chunk = match self.read(buf.remaining_mut()).await? {
            Some(chunk) => chunk,
            None => return Ok(None),
        };

        let size = chunk.len();
        buf.put(chunk);

        Ok(Some(size))
    }

    /// Abort reading from the stream with the given reason.
    pub fn stop(&mut self, reason: &str) {
        if let Some(reader) = self.reader.as_mut() {
            reader.abort(reason);
        }
    }

    /// Block until the stream has closed and return the error code, if any.
    pub async fn closed(&self) -> Result<Option<u8>, Error> {
        let reader = match self.reader.as_ref() {
            Some(reader) => reader,
            None => return Err(Error::Unknown("reader is unavailable".into())),
        };

        let err = match reader.closed().await {
            Ok(()) => return Ok(None),
            Err(err) => Error::from(err),
        };

        // If this is a WebTransportError, extract the error code when available.
        if let Error::Stream(err) = &err {
            if let Some(code) = err.stream_error_code() {
                return Ok(Some(code));
            }
        }

        Err(err)
    }
}

impl Drop for RecvStream {
    fn drop(&mut self) {
        if let Some(reader) = self.reader.as_mut() {
            reader.abort("dropped");
        }
    }
}

impl RecvStream {
    fn poll_inflight_read(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<Option<Bytes>>> {
        match &mut self.read_state {
            ReadState::Idle => Poll::Ready(Ok(None)),
            ReadState::Reading(fut) => match fut.as_mut().poll(cx) {
                Poll::Pending => Poll::Pending,
                Poll::Ready((reader, result)) => {
                    self.reader = Some(reader);
                    self.read_state = ReadState::Idle;
                    Poll::Ready(result)
                }
            },
        }
    }

    fn error_unavailable() -> io::Error {
        io::Error::new(io::ErrorKind::Other, "reader is unavailable")
    }

    fn to_io_error(error: Error) -> io::Error {
        io::Error::new(io::ErrorKind::Other, error.to_string())
    }
}

impl AsyncRead for RecvStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }

        loop {
            if !self.buffer.is_empty() {
                let size = cmp::min(buf.len(), self.buffer.len());
                buf[..size].copy_from_slice(&self.buffer.split_to(size));
                return Poll::Ready(Ok(size));
            }

            if matches!(self.read_state, ReadState::Idle) {
                let mut reader = match self.reader.take() {
                    Some(reader) => reader,
                    None => return Poll::Ready(Err(Self::error_unavailable())),
                };

                let fut = Box::pin(async move {
                    let result = reader
                        .read()
                        .await
                        .map(|data| data.map(|value| Bytes::from(value.to_vec())))
                        .map_err(|err| Self::to_io_error(err.into()));
                    (reader, result)
                });
                self.read_state = ReadState::Reading(fut);
            }

            match self.poll_inflight_read(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
                Poll::Ready(Ok(None)) => return Poll::Ready(Ok(0)),
                Poll::Ready(Ok(Some(chunk))) => {
                    self.buffer = chunk;
                }
            }
        }
    }
}

#[cfg(target_family = "wasm")]
impl webtrans_trait::RecvStream for RecvStream {
    type Error = Error;

    async fn read(&mut self, dst: &mut [u8]) -> Result<Option<usize>, Self::Error> {
        let chunk = match Self::read(self, dst.len()).await? {
            Some(chunk) => chunk,
            None => return Ok(None),
        };

        let size = chunk.len();
        dst[..size].copy_from_slice(&chunk);

        Ok(Some(size))
    }

    fn stop(&mut self, code: u32) {
        Self::stop(self, &code.to_string());
    }

    async fn closed(&mut self) -> Result<(), Self::Error> {
        match Self::closed(self).await? {
            Some(code) => Err(Error::Unknown(
                format!("stream closed with code {code}").into(),
            )),
            None => Ok(()),
        }
    }
}
