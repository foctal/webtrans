use std::sync::Arc;

use thiserror::Error;

use crate::{ConnectError, SettingsError};

/// Error returned when connecting to a WebTransport endpoint.
#[derive(Error, Debug, Clone)]
pub enum ClientError {
    /// Incoming bytes ended before the handshake exchange completed.
    #[error("unexpected end of stream")]
    UnexpectedEnd,

    /// QUIC connection-level failure.
    #[error("connection error: {0}")]
    Connection(#[from] quion::ConnectionError),

    /// Failed to write handshake data.
    #[error("failed to write: {0}")]
    WriteError(#[from] quion::WriteError),

    /// Failed to read handshake data.
    #[error("failed to read: {0}")]
    ReadError(#[from] quion::ReadError),

    /// HTTP/3 SETTINGS negotiation failed.
    #[error("failed to exchange h3 settings: {0}")]
    SettingsError(#[from] SettingsError),

    /// HTTP/3 CONNECT negotiation failed.
    #[error("failed to exchange h3 connect: {0}")]
    HttpError(#[from] ConnectError),

    /// Local endpoint construction failed.
    #[error("endpoint error: {0}")]
    Endpoint(#[from] quion::EndpointError),

    /// URL host component could not be converted to a DNS name.
    #[error("invalid DNS name: {0}")]
    InvalidDnsName(String),

    /// URL was invalid for WebTransport usage.
    #[error("invalid url: {0}")]
    InvalidUrl(String),

    /// DNS resolution exceeded the configured timeout.
    #[error("DNS resolution timed out")]
    DnsTimeout,

    /// QUIC and HTTP/3 session establishment exceeded the configured timeout.
    #[error("connection handshake timed out")]
    HandshakeTimeout,

    /// Local UDP endpoint creation failed.
    #[error("io error: {0}")]
    Io(Arc<std::io::Error>),

    /// TLS configuration did not provide a QUIC-compatible initial cipher suite.
    #[error("TLS configuration has no QUIC-compatible initial cipher suite")]
    InvalidCryptoConfiguration,

    #[cfg(any(feature = "ring", feature = "aws-lc-rs"))]
    /// Rustls-level TLS configuration or handshake error.
    #[error("rustls error: {0}")]
    Rustls(#[from] rustls::Error),
}

/// Errors returned by [`crate::Session`], grouped by QUIC or WebTransport origin.
#[derive(Clone, Error, Debug)]
pub enum SessionError {
    /// Generic QUIC connection failure.
    #[error("connection error: {0}")]
    ConnectionError(quion::ConnectionError),

    /// WebTransport semantic error mapped from connection context.
    #[error("webtransport error: {0}")]
    WebTransportError(#[from] WebTransportError),

    /// Failed to send a datagram over the active connection.
    #[error("send datagram error: {0}")]
    SendDatagramError(#[from] quion::SendDatagramError),
}

impl From<quion::ConnectionError> for SessionError {
    fn from(e: quion::ConnectionError) -> Self {
        match &e {
            quion::ConnectionError::ApplicationClosed { code, reason } => {
                match webtrans_proto::error_from_http3(code.into_inner()) {
                    Some(code) => WebTransportError::Closed(code, reason.clone()).into(),
                    None => SessionError::ConnectionError(e),
                }
            }
            _ => SessionError::ConnectionError(e),
        }
    }
}

/// Error that can occur when reading or writing the WebTransport stream header.
#[derive(Clone, Error, Debug)]
pub enum WebTransportError {
    /// Session was closed with an application code and reason.
    #[error("closed: code={0} reason={1}")]
    Closed(u32, String),

    /// Stream/session header did not match any known session.
    #[error("unknown session")]
    UnknownSession,

    /// Failed to read stream/session preface data.
    #[error("read error: {0}")]
    ReadError(#[from] quion::ReadError),

    /// Failed to write stream/session preface data.
    #[error("write error: {0}")]
    WriteError(#[from] quion::WriteError),
}

/// Error when writing to [`crate::SendStream`], similar to [`quion::WriteError`].
#[derive(Clone, Error, Debug)]
pub enum WriteError {
    /// Transport-specific write failure.
    #[error("transport write error: {0}")]
    Transport(quion::WriteError),
    /// Peer sent STOP_SENDING with the provided WebTransport code.
    #[error("STOP_SENDING: {0}")]
    Stopped(u32),

    /// STOP_SENDING carried a non-WebTransport error code.
    #[error("invalid STOP_SENDING: {0}")]
    InvalidStopped(quion::VarInt),

    /// Stream write failed because the parent session failed.
    #[error("session error: {0}")]
    SessionError(#[from] SessionError),

    /// Stream was already closed.
    #[error("stream closed")]
    ClosedStream,
}

impl From<quion::WriteError> for WriteError {
    fn from(e: quion::WriteError) -> Self {
        match e {
            quion::WriteError::Stopped(code) => {
                match webtrans_proto::error_from_http3(code.into_inner()) {
                    Some(code) => WriteError::Stopped(code),
                    None => WriteError::InvalidStopped(code),
                }
            }
            quion::WriteError::ConnectionLost(e) => WriteError::SessionError(e.into()),
            other => WriteError::Transport(other),
        }
    }
}

/// Error when reading from [`crate::RecvStream`], similar to [`quion::ReadError`].
#[derive(Clone, Error, Debug)]
pub enum ReadError {
    /// Transport-specific read failure.
    #[error("transport read error: {0}")]
    Transport(quion::ReadError),
    /// Stream read failed because the parent session failed.
    #[error("session error: {0}")]
    SessionError(#[from] SessionError),

    /// Peer reset the stream with the provided WebTransport code.
    #[error("RESET_STREAM: {0}")]
    Reset(u32),

    /// RESET_STREAM carried a non-WebTransport error code.
    #[error("invalid RESET_STREAM: {0}")]
    InvalidReset(quion::VarInt),

    /// Stream was already closed.
    #[error("stream already closed")]
    ClosedStream,

    /// Ordered read API was used on an unordered stream.
    #[error("ordered read on unordered stream")]
    IllegalOrderedRead,
}

impl From<quion::ReadError> for ReadError {
    fn from(value: quion::ReadError) -> Self {
        match value {
            quion::ReadError::Reset(code) => {
                match webtrans_proto::error_from_http3(code.into_inner()) {
                    Some(code) => ReadError::Reset(code),
                    None => ReadError::InvalidReset(code),
                }
            }
            quion::ReadError::ConnectionLost(e) => ReadError::SessionError(e.into()),
            quion::ReadError::IllegalOrderedState => ReadError::IllegalOrderedRead,
            other => ReadError::Transport(other),
        }
    }
}

/// Error returned when receiving a new WebTransport session.
#[derive(Error, Debug, Clone)]
pub enum ServerError {
    /// Endpoint construction failed.
    #[error("endpoint error: {0}")]
    Endpoint(#[from] quion::EndpointError),

    /// Transport configuration failed.
    #[error("configuration error: {0}")]
    Configuration(#[from] quion::ConfigError),
    /// A request no longer owns the handshake state needed to complete it.
    #[error("WebTransport request was already completed")]
    RequestAlreadyCompleted,

    /// Incoming bytes ended before the handshake exchange completed.
    #[error("unexpected end of stream")]
    UnexpectedEnd,

    /// QUIC connection-level failure.
    #[error("connection error")]
    Connection(#[from] quion::ConnectionError),

    /// QUIC and HTTP/3 session establishment exceeded the configured timeout.
    #[error("connection handshake timed out")]
    HandshakeTimeout,

    /// Failed to write handshake data.
    #[error("failed to write")]
    WriteError(#[from] quion::WriteError),

    /// Failed to read handshake data.
    #[error("failed to read")]
    ReadError(#[from] quion::ReadError),

    /// HTTP/3 SETTINGS negotiation failed.
    #[error("failed to exchange h3 settings")]
    SettingsError(#[from] SettingsError),

    /// HTTP/3 CONNECT negotiation failed.
    #[error("failed to exchange h3 connect")]
    ConnectError(#[from] ConnectError),

    /// Generic I/O failure during server setup or handshake.
    #[error("io error: {0}")]
    IoError(Arc<std::io::Error>),

    /// TLS configuration did not provide a QUIC-compatible initial cipher suite.
    #[error("TLS configuration has no QUIC-compatible initial cipher suite")]
    InvalidCryptoConfiguration,

    #[cfg(any(feature = "ring", feature = "aws-lc-rs"))]
    /// Rustls-level TLS configuration or handshake error.
    #[error("rustls error: {0}")]
    Rustls(#[from] rustls::Error),
}

impl webtrans_trait::Error for SessionError {
    fn session_error(&self) -> Option<(u32, String)> {
        if let SessionError::WebTransportError(WebTransportError::Closed(code, reason)) = self {
            return Some((*code, reason.to_string()));
        }

        None
    }
}

impl webtrans_trait::Error for WriteError {
    fn session_error(&self) -> Option<(u32, String)> {
        if let WriteError::SessionError(e) = self {
            return e.session_error();
        }

        None
    }

    fn stream_error(&self) -> Option<u32> {
        match self {
            WriteError::Stopped(code) => Some(*code),
            _ => None,
        }
    }
}

impl webtrans_trait::Error for ReadError {
    fn session_error(&self) -> Option<(u32, String)> {
        if let ReadError::SessionError(e) = self {
            return e.session_error();
        }

        None
    }

    fn stream_error(&self) -> Option<u32> {
        match self {
            ReadError::Reset(code) => Some(*code),
            _ => None,
        }
    }
}
