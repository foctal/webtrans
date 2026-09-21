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
    stream: WebTransportReceiveStream,
    eof: bool,
    reader: Option<Reader<Uint8Array>>,
    buffer: Bytes,
    read_state: ReadState,
}

impl RecvStream {
    pub(super) fn new(stream: WebTransportReceiveStream) -> Result<Self, Error> {
        let reader = Reader::new(&stream)?;

        Ok(Self {
            stream,
            eof: false,
            reader: Some(reader),
            buffer: Bytes::new(),
            read_state: ReadState::Idle,
        })
    }

    /// Read the next chunk of data with the provided maximum size.
    ///
    /// This returns a chunk of data instead of copying, which can be more efficient.
    /// Cancellation retains the pending JS read for the next caller. A zero
    /// maximum returns an empty chunk without consuming input.
    pub async fn read(&mut self, max: usize) -> Result<Option<Bytes>, Error> {
        std::future::poll_fn(|cx| self.poll_chunk(cx, max))
            .await
            .map_err(|error| Error::Unknown(error.to_string().into()))
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
        self.eof = true;
        self.buffer = Bytes::new();
        self.read_state = ReadState::Idle;
        if let Some(reader) = self.reader.as_mut() {
            reader.abort(reason);
        } else {
            let cancel = self.stream.cancel_with_reason(&reason.into());
            wasm_bindgen_futures::spawn_local(async move {
                let _ = wasm_bindgen_futures::JsFuture::from(cancel).await;
            });
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
        if let Error::Stream(err) = &err
            && let Some(code) = err.stream_error_code()
        {
            return Ok(Some(code));
        }

        Err(err)
    }
}

impl Drop for RecvStream {
    fn drop(&mut self) {
        self.stop("dropped");
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
        io::Error::other("reader is unavailable")
    }

    fn to_io_error(error: Error) -> io::Error {
        io::Error::other(error.to_string())
    }
}

impl RecvStream {
    fn poll_chunk(&mut self, cx: &mut Context<'_>, max: usize) -> Poll<io::Result<Option<Bytes>>> {
        if max == 0 {
            return Poll::Ready(Ok(Some(Bytes::new())));
        }
        // Bound work per poll if a JS source produces empty chunks repeatedly.
        for _ in 0..16 {
            if !self.buffer.is_empty() {
                let size = cmp::min(max, self.buffer.len());
                return Poll::Ready(Ok(Some(self.buffer.split_to(size))));
            }
            if self.eof {
                return Poll::Ready(Ok(None));
            }
            if matches!(self.read_state, ReadState::Idle) {
                let mut reader = match self.reader.take() {
                    Some(reader) => reader,
                    None => return Poll::Ready(Err(Self::error_unavailable())),
                };
                self.read_state = ReadState::Reading(Box::pin(async move {
                    let result = reader
                        .read()
                        .await
                        .map(|data| data.map(|value| Bytes::from(value.to_vec())))
                        .map_err(|err| Self::to_io_error(err.into()));
                    (reader, result)
                }));
            }
            match self.poll_inflight_read(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(err)) => {
                    self.eof = true;
                    return Poll::Ready(Err(err));
                }
                Poll::Ready(Ok(None)) => {
                    self.eof = true;
                    return Poll::Ready(Ok(None));
                }
                Poll::Ready(Ok(Some(chunk))) => self.buffer = chunk,
            }
        }
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}

impl AsyncRead for RecvStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        match self.poll_chunk(cx, buf.len()) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
            Poll::Ready(Ok(None)) => Poll::Ready(Ok(0)),
            Poll::Ready(Ok(Some(chunk))) => {
                buf[..chunk.len()].copy_from_slice(&chunk);
                Poll::Ready(Ok(chunk.len()))
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

#[cfg(all(test, target_arch = "wasm32"))]
mod tests {
    use super::*;
    use futures::{io::AsyncReadExt, poll};
    use wasm_bindgen::prelude::*;
    use wasm_bindgen_test::*;

    #[wasm_bindgen(inline_js = "
        export function controlledReader() {
            let controller;
            const stream = new ReadableStream({start(c) { controller = c; }});
            stream.controller = controller;
            return stream;
        }
        export function deliver(stream) {
            stream.controller.enqueue(new Uint8Array([1,2,3,4]));
            stream.controller.close();
        }
    ")]
    extern "C" {
        fn controlledReader() -> web_sys::ReadableStream;
        fn deliver(stream: &web_sys::ReadableStream);
    }

    #[wasm_bindgen_test(async)]
    async fn cancelled_read_can_resume_through_either_api() {
        let raw = controlledReader();
        let mut stream = RecvStream::new(raw.clone().unchecked_into()).unwrap();
        assert_eq!(stream.read(0).await.unwrap().unwrap().len(), 0);
        {
            let mut pending = Box::pin(stream.read(2));
            assert!(poll!(pending.as_mut()).is_pending());
        }
        deliver(&raw);
        let mut first = [0; 1];
        AsyncReadExt::read_exact(&mut stream, &mut first)
            .await
            .unwrap();
        assert_eq!(first, [1]);
        assert_eq!(stream.read(2).await.unwrap().unwrap().as_ref(), &[2, 3]);
        assert_eq!(stream.read(2).await.unwrap().unwrap().as_ref(), &[4]);
        assert!(stream.read(2).await.unwrap().is_none());
        assert!(stream.read(2).await.unwrap().is_none());
    }
}
