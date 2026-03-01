//! Encode and decode WebTransport CONNECT requests and responses.

use std::{str::FromStr, sync::Arc};

use bytes::{Buf, BufMut, BytesMut};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use url::Url;

use super::{Frame, VarInt, qpack};
use crate::io::read_incremental;

use thiserror::Error;

// Errors that can occur while processing a CONNECT request.
#[derive(Error, Debug, Clone)]
/// Errors returned while encoding or decoding WebTransport CONNECT messages.
pub enum ConnectError {
    #[error("unexpected end of input")]
    /// Input ended before the complete frame/header block was available.
    UnexpectedEnd,

    #[error("qpack error")]
    /// QPACK header block decoding failed.
    QpackError(#[from] qpack::DecodeError),

    #[error("unexpected frame {0:?}")]
    /// Received an unexpected HTTP/3 frame type.
    UnexpectedFrame(Frame),

    #[error("invalid method")]
    /// `:method` could not be parsed as an HTTP method.
    InvalidMethod,

    #[error("invalid url")]
    /// URL reconstruction from pseudo-headers failed.
    InvalidUrl(#[from] url::ParseError),

    #[error("invalid status")]
    /// `:status` could not be parsed as a valid HTTP status code.
    InvalidStatus,

    #[error("expected 200, got: {0:?}")]
    /// Response status was not successful.
    WrongStatus(Option<http::StatusCode>),

    #[error("expected connect, got: {0:?}")]
    /// Request method was not CONNECT.
    WrongMethod(Option<http::method::Method>),

    #[error("expected https, got: {0:?}")]
    /// Request scheme was not HTTPS.
    WrongScheme(Option<String>),

    #[error("expected authority header")]
    /// Required `:authority` pseudo-header was missing.
    WrongAuthority,

    #[error("expected webtransport, got: {0:?}")]
    /// CONNECT protocol was not `webtransport`.
    WrongProtocol(Option<String>),

    #[error("expected path header")]
    /// Required `:path` pseudo-header was missing.
    WrongPath,

    #[error("non-200 status: {0:?}")]
    /// Peer returned an explicit non-2xx HTTP response status.
    ErrorStatus(http::StatusCode),

    #[error("io error: {0}")]
    /// I/O error while reading or writing frames.
    Io(Arc<std::io::Error>),
}

impl From<std::io::Error> for ConnectError {
    fn from(err: std::io::Error) -> Self {
        ConnectError::Io(Arc::new(err))
    }
}

#[derive(Debug)]
/// Decoded WebTransport CONNECT request metadata.
pub struct ConnectRequest {
    /// Target URL reconstructed from pseudo-headers.
    pub url: Url,
}

impl ConnectRequest {
    /// Decode a CONNECT request from an in-memory frame buffer.
    pub fn decode<B: Buf>(buf: &mut B) -> Result<Self, ConnectError> {
        let (typ, mut data) = Frame::read(buf).map_err(|_| ConnectError::UnexpectedEnd)?;
        if typ != Frame::HEADERS {
            return Err(ConnectError::UnexpectedFrame(typ));
        }

        // The frame payload should be complete, so an additional UnexpectedEnd is unexpected here.

        let headers = qpack::Headers::decode(&mut data)?;

        let scheme = match headers.get(":scheme") {
            Some("https") => "https",
            Some(scheme) => Err(ConnectError::WrongScheme(Some(scheme.to_string())))?,
            None => return Err(ConnectError::WrongScheme(None)),
        };

        let authority = headers
            .get(":authority")
            .ok_or(ConnectError::WrongAuthority)?;

        let path_and_query = headers.get(":path").ok_or(ConnectError::WrongPath)?;

        let method = headers.get(":method");
        match method
            .map(|method| method.try_into().map_err(|_| ConnectError::InvalidMethod))
            .transpose()?
        {
            Some(http::Method::CONNECT) => (),
            o => return Err(ConnectError::WrongMethod(o)),
        };

        let protocol = headers.get(":protocol");
        if protocol != Some("webtransport") {
            return Err(ConnectError::WrongProtocol(protocol.map(|s| s.to_string())));
        }

        let url = Url::parse(&format!("{scheme}://{authority}{path_and_query}"))?;

        Ok(Self { url })
    }

    /// Read and decode a CONNECT request from an async stream.
    pub async fn read<S: AsyncRead + Unpin>(stream: &mut S) -> Result<Self, ConnectError> {
        read_incremental(
            stream,
            |cursor| Self::decode(cursor),
            |err| matches!(err, ConnectError::UnexpectedEnd),
            ConnectError::UnexpectedEnd,
        )
        .await
    }

    /// Encode this CONNECT request into a HEADERS frame.
    pub fn encode<B: BufMut>(&self, buf: &mut B) {
        let mut headers = qpack::Headers::default();
        headers.set(":method", "CONNECT");
        headers.set(":scheme", self.url.scheme());
        headers.set(":authority", self.url.authority());
        let path_and_query = match self.url.query() {
            Some(query) => format!("{}?{}", self.url.path(), query),
            None => self.url.path().to_string(),
        };
        headers.set(":path", &path_and_query);
        headers.set(":protocol", "webtransport");
        encode_headers_frame(buf, &headers);
    }

    /// Encode and write this CONNECT request to an async stream.
    pub async fn write<S: AsyncWrite + Unpin>(&self, stream: &mut S) -> Result<(), ConnectError> {
        let mut buf = BytesMut::new();
        self.encode(&mut buf);
        stream.write_all_buf(&mut buf).await?;
        Ok(())
    }
}

#[derive(Debug)]
/// Decoded WebTransport CONNECT response metadata.
pub struct ConnectResponse {
    /// HTTP status returned by the peer.
    pub status: http::status::StatusCode,
}

impl ConnectResponse {
    /// Decode a CONNECT response from an in-memory frame buffer.
    pub fn decode<B: Buf>(buf: &mut B) -> Result<Self, ConnectError> {
        let (typ, mut data) = Frame::read(buf).map_err(|_| ConnectError::UnexpectedEnd)?;
        if typ != Frame::HEADERS {
            return Err(ConnectError::UnexpectedFrame(typ));
        }

        let headers = qpack::Headers::decode(&mut data)?;

        let status = match headers
            .get(":status")
            .map(|status| {
                http::StatusCode::from_str(status).map_err(|_| ConnectError::InvalidStatus)
            })
            .transpose()?
        {
            Some(status) if status.is_success() => status,
            o => return Err(ConnectError::WrongStatus(o)),
        };

        Ok(Self { status })
    }

    /// Read and decode a CONNECT response from an async stream.
    pub async fn read<S: AsyncRead + Unpin>(stream: &mut S) -> Result<Self, ConnectError> {
        read_incremental(
            stream,
            |cursor| Self::decode(cursor),
            |err| matches!(err, ConnectError::UnexpectedEnd),
            ConnectError::UnexpectedEnd,
        )
        .await
    }

    /// Encode this CONNECT response into a HEADERS frame.
    pub fn encode<B: BufMut>(&self, buf: &mut B) {
        let mut headers = qpack::Headers::default();
        headers.set(":status", self.status.as_str());
        headers.set("sec-webtransport-http3-draft", "draft02");
        encode_headers_frame(buf, &headers);
    }

    /// Encode and write this CONNECT response to an async stream.
    pub async fn write<S: AsyncWrite + Unpin>(&self, stream: &mut S) -> Result<(), ConnectError> {
        let mut buf = BytesMut::new();
        self.encode(&mut buf);
        stream.write_all_buf(&mut buf).await?;
        Ok(())
    }
}

fn encode_headers_frame<B: BufMut>(buf: &mut B, headers: &qpack::Headers) {
    // Encode headers into a temporary payload so we can prefix it with the frame length.
    let mut payload = Vec::new();
    headers.encode(&mut payload);

    Frame::HEADERS.encode(buf);
    VarInt::from_u32(payload.len() as u32).encode(buf);
    buf.put_slice(&payload);
}
