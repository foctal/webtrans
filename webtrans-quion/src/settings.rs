//! HTTP/3 SETTINGS exchange for WebTransport over Quion.

use futures::try_join;

use thiserror::Error;

#[derive(Error, Debug, Clone)]
pub enum SettingsError {
    #[error("quic stream was closed early")]
    UnexpectedEnd,

    #[error("protocol error: {0}")]
    ProtoError(#[from] webtrans_proto::SettingsError),

    #[error("WebTransport is not supported")]
    WebTransportUnsupported,

    #[error("connection error")]
    ConnectionError(#[from] quion::ConnectionError),

    #[error("read error")]
    ReadError(#[from] quion::ReadError),

    #[error("write error")]
    WriteError(#[from] quion::WriteError),
}

pub struct Settings {
    // Keep references to send/recv streams so they remain open until drop.
    #[allow(dead_code)]
    send: quion::SendStream,

    #[allow(dead_code)]
    recv: quion::RecvStream,
}

impl Settings {
    // Establish the HTTP/3 SETTINGS exchange.
    pub async fn connect(
        conn: &quion::Connection,
        peer_is_server: bool,
    ) -> Result<Self, SettingsError> {
        let recv = Self::accept(conn, peer_is_server);
        let send = Self::open(conn);

        // Run both tasks concurrently until one errors or both complete.
        let (send, recv) = try_join!(send, recv)?;
        Ok(Self { send, recv })
    }

    async fn accept(
        conn: &quion::Connection,
        peer_is_server: bool,
    ) -> Result<quion::RecvStream, SettingsError> {
        let mut recv = conn.accept_uni().await?;
        let settings = webtrans_proto::Settings::read(&mut recv).await?;

        tracing::debug!("received SETTINGS frame: {settings:?}");

        let supported = if peer_is_server {
            settings.supports_webtransport_server()
        } else {
            settings.supports_webtransport_client()
        };
        if !supported {
            return Err(SettingsError::WebTransportUnsupported);
        }

        Ok(recv)
    }

    async fn open(conn: &quion::Connection) -> Result<quion::SendStream, SettingsError> {
        let mut settings = webtrans_proto::Settings::default();
        settings.enable_webtransport(1);

        tracing::debug!("sending SETTINGS frame: {settings:?}");

        let mut send = conn.open_uni().await?;
        settings.write(&mut send).await?;

        Ok(send)
    }
}
