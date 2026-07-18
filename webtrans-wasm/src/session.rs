use std::rc::Rc;

use bytes::Bytes;
use futures::lock::Mutex;
use js_sys::Uint8Array;
use url::Url;
use wasm_bindgen::JsCast;
use wasm_bindgen_futures::JsFuture;
use web_sys::{
    WebTransport, WebTransportBidirectionalStream, WebTransportCloseInfo,
    WebTransportReceiveStream, WebTransportSendStream,
};

use crate::{Error, RecvStream, SendStream};
use web_streams::{Reader, Writer};

struct SharedReader<T: JsCast> {
    inner: Rc<Mutex<Reader<T>>>,
}

impl<T: JsCast> SharedReader<T> {
    fn new(stream: &web_sys::ReadableStream) -> Result<Self, web_streams::Error> {
        Ok(Self {
            inner: Rc::new(Mutex::new(Reader::new(stream)?)),
        })
    }

    async fn read(&self) -> Result<Option<T>, web_streams::Error> {
        self.inner.lock().await.read().await
    }
}

impl<T: JsCast> Clone for SharedReader<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

/// A session represents a client-to-server connection.
///
/// This is the main entry point for creating streams and sending datagrams.
/// Either endpoint may close the session with an error code and reason.
///
/// The session can be cloned to create multiple handles. Stream acceptance and
/// datagram I/O are serialized across all clones because the browser exposes
/// each incoming stream and datagram queue through a single Web Streams lock.
///
/// If an accept future is cancelled, the next accept call resumes the same
/// pending browser read instead of losing the stream.
#[derive(Clone)]
pub struct Session {
    inner: WebTransport,
    url: Url,
    incoming_uni: SharedReader<WebTransportReceiveStream>,
    incoming_bi: SharedReader<WebTransportBidirectionalStream>,
    datagram_reader: SharedReader<Uint8Array>,
    datagram_writer: Rc<Mutex<Writer>>,
}

impl Session {
    pub fn new(inner: WebTransport, url: Url) -> Result<Self, Error> {
        let incoming_uni = SharedReader::new(&inner.incoming_unidirectional_streams())?;
        let incoming_bi = SharedReader::new(&inner.incoming_bidirectional_streams())?;
        let datagrams = inner.datagrams();
        let datagram_reader = SharedReader::new(&datagrams.readable())?;
        let datagram_writer = Writer::new(&datagrams.writable())?;

        Ok(Self {
            inner,
            url,
            incoming_uni,
            incoming_bi,
            datagram_reader,
            datagram_writer: Rc::new(Mutex::new(datagram_writer)),
        })
    }

    /// Accept a new unidirectional stream from the peer.
    ///
    /// Concurrent calls across cloned sessions are serviced in lock order.
    /// Cancelling a call does not discard a stream that arrives later.
    pub async fn accept_uni(&self) -> Result<RecvStream, Error> {
        match self.incoming_uni.read().await? {
            Some(stream) => Ok(RecvStream::new(stream)?),
            None => Err(self.closed().await),
        }
    }

    /// Accept a new bidirectional stream from the peer.
    ///
    /// Concurrent calls across cloned sessions are serviced in lock order.
    /// Cancelling a call does not discard a stream that arrives later.
    pub async fn accept_bi(&self) -> Result<(SendStream, RecvStream), Error> {
        let stream: WebTransportBidirectionalStream = match self.incoming_bi.read().await? {
            Some(stream) => stream,
            None => return Err(self.closed().await),
        };

        let send = SendStream::new(stream.writable())?;
        let recv = RecvStream::new(stream.readable())?;

        Ok((send, recv))
    }

    /// Create a new bidirectional stream.
    pub async fn open_bi(&self) -> Result<(SendStream, RecvStream), Error> {
        let stream: WebTransportBidirectionalStream =
            JsFuture::from(self.inner.create_bidirectional_stream()).await?;

        let send = SendStream::new(stream.writable())?;
        let recv = RecvStream::new(stream.readable())?;

        Ok((send, recv))
    }

    /// Create a new unidirectional stream.
    pub async fn open_uni(&self) -> Result<SendStream, Error> {
        let stream: WebTransportSendStream =
            JsFuture::from(self.inner.create_unidirectional_stream()).await?;

        let send = SendStream::new(stream)?;
        Ok(send)
    }

    /// Send a datagram over the network.
    pub async fn send_datagram(&self, payload: Bytes) -> Result<(), Error> {
        let mut writer = self.datagram_writer.lock().await;
        writer.write(&Uint8Array::from(payload.as_ref())).await?;
        Ok(())
    }

    /// Receive a datagram over the network.
    pub async fn recv_datagram(&self) -> Result<Bytes, Error> {
        let data: Uint8Array = match self.datagram_reader.read().await? {
            Some(data) => data,
            None => return Err(self.closed().await),
        };
        Ok(data.to_vec().into())
    }

    /// Close the session with the given error code and reason.
    pub fn close(&self, code: u32, reason: &str) {
        let info = WebTransportCloseInfo::new();
        info.set_close_code(code);
        info.set_reason(reason);
        self.inner.close_with_close_info(&info);
    }

    /// Block until the session closes and return the error.
    pub async fn closed(&self) -> Error {
        match JsFuture::from(self.inner.closed()).await {
            Ok(info) => {
                let info: WebTransportCloseInfo = info;
                Error::SessionClosed {
                    code: info.get_close_code().unwrap_or_default(),
                    reason: info.get_reason().unwrap_or_default(),
                }
            }
            Err(error) => error.into(),
        }
    }

    /// Return the URL used to create the session.
    pub fn url(&self) -> &Url {
        &self.url
    }
}

impl PartialEq for Session {
    fn eq(&self, other: &Self) -> bool {
        self.inner == other.inner
    }
}

impl Eq for Session {}

#[cfg(target_family = "wasm")]
impl webtrans_trait::Session for Session {
    type SendStream = SendStream;
    type RecvStream = RecvStream;
    type Error = Error;

    async fn accept_uni(&self) -> Result<Self::RecvStream, Self::Error> {
        Self::accept_uni(self).await
    }

    async fn accept_bi(&self) -> Result<(Self::SendStream, Self::RecvStream), Self::Error> {
        Self::accept_bi(self).await
    }

    async fn open_bi(&self) -> Result<(Self::SendStream, Self::RecvStream), Self::Error> {
        Self::open_bi(self).await
    }

    async fn open_uni(&self) -> Result<Self::SendStream, Self::Error> {
        Self::open_uni(self).await
    }

    async fn send_datagram(&self, payload: Bytes) -> Result<(), Self::Error> {
        Self::send_datagram(self, payload).await
    }

    async fn recv_datagram(&self) -> Result<Bytes, Self::Error> {
        Self::recv_datagram(self).await
    }

    fn max_datagram_size(&self) -> usize {
        self.inner.datagrams().max_datagram_size() as usize
    }

    fn close(&self, code: u32, reason: &str) {
        Self::close(self, code, reason);
    }

    async fn closed(&self) -> Self::Error {
        Self::closed(self).await
    }
}

#[cfg(all(test, target_family = "wasm"))]
mod tests {
    use std::{cell::RefCell, rc::Rc};

    use futures::{join, pin_mut, poll};
    use js_sys::{Object, Reflect, Uint8Array};
    use wasm_bindgen::{JsValue, closure::Closure};
    use wasm_bindgen_test::*;
    use web_sys::{ReadableStream, ReadableStreamDefaultController};

    use super::SharedReader;

    wasm_bindgen_test_configure!(run_in_browser);

    fn controlled_stream() -> (ReadableStream, ReadableStreamDefaultController) {
        let controller = Rc::new(RefCell::new(None));
        let captured = controller.clone();
        let start = Closure::<dyn FnMut(ReadableStreamDefaultController)>::new(move |value| {
            *captured.borrow_mut() = Some(value);
        });
        let source = Object::new();
        Reflect::set(&source, &JsValue::from_str("start"), start.as_ref()).unwrap();
        let stream = ReadableStream::new_with_underlying_source(&source).unwrap();
        let controller = controller.borrow_mut().take().unwrap();
        (stream, controller)
    }

    #[wasm_bindgen_test(async)]
    async fn cancelled_read_is_resumed_by_the_next_caller() {
        let (stream, controller) = controlled_stream();
        let reader = SharedReader::<Uint8Array>::new(&stream).unwrap();

        {
            let pending = reader.read();
            pin_mut!(pending);
            assert!(poll!(pending.as_mut()).is_pending());
        }

        controller
            .enqueue_with_chunk(&Uint8Array::from(&b"resumed"[..]).into())
            .unwrap();
        let value = reader.read().await.unwrap().unwrap();
        assert_eq!(value.to_vec(), b"resumed");
    }

    #[wasm_bindgen_test(async)]
    async fn cloned_readers_serialize_without_losing_values() {
        let (stream, controller) = controlled_stream();
        let first = SharedReader::<Uint8Array>::new(&stream).unwrap();
        let second = first.clone();
        controller
            .enqueue_with_chunk(&Uint8Array::from(&b"one"[..]).into())
            .unwrap();
        controller
            .enqueue_with_chunk(&Uint8Array::from(&b"two"[..]).into())
            .unwrap();

        let (one, two) = join!(first.read(), second.read());
        assert_eq!(one.unwrap().unwrap().to_vec(), b"one");
        assert_eq!(two.unwrap().unwrap().to_vec(), b"two");
    }
}
