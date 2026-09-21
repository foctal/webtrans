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
        use futures::io::AsyncWriteExt;
        self.write_all(buf)
            .await
            .map_err(|error| Error::Unknown(error.to_string().into()))?;
        self.flush()
            .await
            .map_err(|error| Error::Unknown(error.to_string().into()))
    }

    /// Write some of the provided buffer to the stream.
    /// Cancelling preserves accepted bytes; flush before finishing the stream.
    pub async fn write_buf<B: Buf>(&mut self, buf: &mut B) -> Result<usize, Error> {
        let size = futures::io::AsyncWriteExt::write(self, buf.chunk())
            .await
            .map_err(|error| Error::Unknown(error.to_string().into()))?;
        buf.advance(size);
        Ok(size)
    }

    /// Send an immediate reset, closing the stream with an error.
    pub fn reset(&mut self, reason: &str) {
        self.is_closed = true;
        self.write_state = WriteState::Idle;
        if let Some(writer) = self.writer.as_mut() {
            writer.abort(reason);
        } else {
            let abort = self.stream.abort_with_reason(&reason.into());
            wasm_bindgen_futures::spawn_local(async move {
                let _ = wasm_bindgen_futures::JsFuture::from(abort).await;
            });
        }
    }

    /// Mark the stream as finished.
    ///
    /// Flush accepted writes first, or use `AsyncWriteExt::close` to drain them.
    /// A synchronous finish rejects a pending write instead of silently losing it.
    pub fn finish(&mut self) -> Result<(), Error> {
        if matches!(self.write_state, WriteState::Writing(_)) {
            return Err(Error::Unknown("flush pending writes before finish".into()));
        }
        if let Some(writer) = self.writer.as_mut() {
            writer.close();
        }
        self.is_closed = true;
        Ok(())
    }

    /// Set the stream's priority.
    ///
    /// Streams with higher values are sent first, but delivery order is not guaranteed.
    pub fn set_priority(&mut self, priority: i32) -> Result<(), Error> {
        Reflect::set(&self.stream, &"sendOrder".into(), &priority.into())
            .map(|_| ())
            .map_err(Into::into)
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
        if let Error::Stream(err) = &err
            && let Some(code) = err.stream_error_code()
        {
            return Ok(Some(code));
        }

        Err(err)
    }
}

impl Drop for SendStream {
    /// Close an idle stream, or abort a dropped buffered write.
    fn drop(&mut self) {
        if matches!(self.write_state, WriteState::Writing(_)) {
            self.reset("dropped with pending write");
            return;
        }
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
        io::Error::other("writer is unavailable")
    }

    fn to_io_error(error: Error) -> io::Error {
        io::Error::other(error.to_string())
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

        match self.poll_inflight_write(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Err(error)) => {
                self.is_closed = true;
                return Poll::Ready(Err(error));
            }
            Poll::Ready(Ok(_)) => {}
        }
        if matches!(self.write_state, WriteState::Idle) {
            let mut writer = match self.writer.take() {
                Some(writer) => writer,
                None => return Poll::Ready(Err(Self::error_unavailable())),
            };

            let payload = Uint8Array::from(&buf[..buf.len().min(64 * 1024)]);
            let size = payload.length() as usize;
            let fut = Box::pin(async move {
                let result = writer
                    .write(&payload)
                    .await
                    .map(|_| size)
                    .map_err(|err| Self::to_io_error(err.into()));
                (writer, result)
            });
            self.write_state = WriteState::Writing(fut);
            // The owned chunk is now accepted. A later call may use a different
            // buffer after cancellation, and must never receive this length.
            return Poll::Ready(Ok(size));
        }

        unreachable!("inflight write was drained")
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.poll_inflight_write(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(_)) => Poll::Ready(Ok(())),
            Poll::Ready(Err(err)) => {
                self.is_closed = true;
                Poll::Ready(Err(err))
            }
        }
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.as_mut().poll_flush(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(err)) => {
                self.is_closed = true;
                Poll::Ready(Err(err))
            }
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
        let _ = Self::set_priority(self, i32::from(order));
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

#[cfg(all(test, target_arch = "wasm32"))]
mod tests {
    use super::*;
    use futures::{io::AsyncWriteExt, poll};
    use wasm_bindgen::prelude::*;
    use wasm_bindgen_test::*;

    #[wasm_bindgen(inline_js = "
        export function controlledWriter() {
            let release;
            const gate = new Promise(resolve => { release = resolve; });
            const chunks = [];
            const stream = new WritableStream({write(chunk) {
                chunks.push(Array.from(chunk));
                return chunks.length === 1 ? gate : Promise.resolve();
            }});
            stream.release = release;
            stream.chunks = chunks;
            return stream;
        }
        export function releaseWriter(stream) { stream.release(); }
        export function writtenBytes(stream) { return new Uint8Array(stream.chunks.flat()); }
    ")]
    extern "C" {
        fn controlledWriter() -> web_sys::WritableStream;
        fn releaseWriter(stream: &web_sys::WritableStream);
        fn writtenBytes(stream: &web_sys::WritableStream) -> Uint8Array;
    }

    #[wasm_bindgen_test(async)]
    async fn buffered_write_reports_acceptance_before_waiting_for_js() {
        let raw = controlledWriter();
        let mut stream = SendStream::new(raw.clone().unchecked_into()).unwrap();
        let mut first = Box::pin(AsyncWriteExt::write(&mut stream, b"first"));
        assert!(matches!(poll!(first.as_mut()), Poll::Ready(Ok(5))));
        drop(first);
        {
            let mut next = Box::pin(AsyncWriteExt::write(&mut stream, b"discarded"));
            assert!(poll!(next.as_mut()).is_pending());
        }
        assert!(stream.finish().is_err());
        releaseWriter(&raw);
        assert_eq!(AsyncWriteExt::write(&mut stream, b"x").await.unwrap(), 1);
        stream.flush().await.unwrap();
        assert_eq!(writtenBytes(&raw).to_vec(), b"firstx");
        AsyncWriteExt::close(&mut stream).await.unwrap();
    }
}
