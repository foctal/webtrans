use std::future::Future;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Buf;
use futures_io::AsyncWrite;
use js_sys::{Reflect, Uint8Array};
use web_sys::WebTransportSendStream;

use crate::Error;
use web_streams::Writer;

type WriteFuture = Pin<Box<dyn Future<Output = (Writer, io::Result<usize>)>>>;

enum WriteState {
    Idle,
    Writing(WriteFuture),
}

/// A byte stream sent to the remote peer.
pub struct SendStream {
    stream: WebTransportSendStream,
    writer: Option<Writer>,
    write_state: WriteState,
    is_closed: bool,
}

impl SendStream {
    pub(super) fn new(stream: WebTransportSendStream) -> Result<Self, Error> {
        let writer = Writer::new(&stream)?;
        Ok(Self {
            stream,
            writer: Some(writer),
            write_state: WriteState::Idle,
            is_closed: false,
        })
    }

    /// Write all of the provided bytes to the stream.
    pub async fn write(&mut self, buf: &[u8]) -> Result<(), Error> {
        let writer = self
            .writer
            .as_mut()
            .ok_or_else(|| Error::Unknown("writer is unavailable".into()))?;
        writer
            .write(&Uint8Array::from(buf))
            .await
            .map_err(Into::into)
    }

    /// Write some of the provided buffer to the stream.
    pub async fn write_buf<B: Buf>(&mut self, buf: &mut B) -> Result<usize, Error> {
        let chunk = buf.chunk();
        let size = chunk.len();
        let writer = self
            .writer
            .as_mut()
            .ok_or_else(|| Error::Unknown("writer is unavailable".into()))?;
        writer.write(&Uint8Array::from(chunk)).await?;
        buf.advance(size);
        Ok(size)
    }

    /// Send an immediate reset, closing the stream with an error.
    pub fn reset(&mut self, reason: &str) {
        if let Some(writer) = self.writer.as_mut() {
            writer.abort(reason);
        }
    }

    /// Mark the stream as finished.
    ///
    /// This is called on drop, but can also be invoked manually.
    pub fn finish(&mut self) -> Result<(), Error> {
        if let Some(writer) = self.writer.as_mut() {
            writer.close();
        }
        self.is_closed = true;
        Ok(())
    }

    /// Set the stream's priority.
    ///
    /// Streams with higher values are sent first, but delivery order is not guaranteed.
    pub fn set_priority(&mut self, priority: i32) {
        Reflect::set(&self.stream, &"sendOrder".into(), &priority.into())
            .expect("failed to set priority");
    }

    /// Block until the stream has closed and return the error code, if any.
    pub async fn closed(&self) -> Result<Option<u8>, Error> {
        let writer = match self.writer.as_ref() {
            Some(writer) => writer,
            None => return Err(Error::Unknown("writer is unavailable".into())),
        };

        let err = match writer.closed().await {
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

impl Drop for SendStream {
    /// Close the stream with a FIN.
    fn drop(&mut self) {
        if let Some(writer) = self.writer.as_mut() {
            writer.close();
        }
    }
}

impl SendStream {
    fn poll_inflight_write(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<usize>> {
        match &mut self.write_state {
            WriteState::Idle => Poll::Ready(Ok(0)),
            WriteState::Writing(fut) => match fut.as_mut().poll(cx) {
                Poll::Pending => Poll::Pending,
                Poll::Ready((writer, result)) => {
                    self.writer = Some(writer);
                    self.write_state = WriteState::Idle;
                    Poll::Ready(result)
                }
            },
        }
    }

    fn error_unavailable() -> io::Error {
        io::Error::new(io::ErrorKind::Other, "writer is unavailable")
    }

    fn to_io_error(error: Error) -> io::Error {
        io::Error::new(io::ErrorKind::Other, error.to_string())
    }
}

impl AsyncWrite for SendStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }

        if self.is_closed {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "stream is already closed",
            )));
        }

        if matches!(self.write_state, WriteState::Idle) {
            let mut writer = match self.writer.take() {
                Some(writer) => writer,
                None => return Poll::Ready(Err(Self::error_unavailable())),
            };

            let payload = Vec::from(buf);
            let size = payload.len();
            let fut = Box::pin(async move {
                let result = writer
                    .write(&Uint8Array::from(payload.as_slice()))
                    .await
                    .map(|_| size)
                    .map_err(|err| Self::to_io_error(err.into()));
                (writer, result)
            });
            self.write_state = WriteState::Writing(fut);
        }

        self.poll_inflight_write(cx)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.poll_inflight_write(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(_)) => Poll::Ready(Ok(())),
            Poll::Ready(Err(err)) => Poll::Ready(Err(err)),
        }
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.as_mut().poll_flush(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(err)) => Poll::Ready(Err(err)),
            Poll::Ready(Ok(())) => {
                if !self.is_closed {
                    let writer = match self.writer.as_mut() {
                        Some(writer) => writer,
                        None => return Poll::Ready(Err(Self::error_unavailable())),
                    };
                    writer.close();
                    self.is_closed = true;
                }
                Poll::Ready(Ok(()))
            }
        }
    }
}

#[cfg(target_family = "wasm")]
impl webtrans_trait::SendStream for SendStream {
    type Error = Error;

    async fn write(&mut self, buf: &[u8]) -> Result<usize, Self::Error> {
        Self::write(self, buf).await?;
        Ok(buf.len())
    }

    fn set_priority(&mut self, order: u8) {
        Self::set_priority(self, i32::from(order));
    }

    fn finish(&mut self) -> Result<(), Self::Error> {
        Self::finish(self)
    }

    fn reset(&mut self, code: u32) {
        Self::reset(self, &code.to_string());
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
