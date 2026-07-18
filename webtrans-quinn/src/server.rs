//! Server-side helpers for WebTransport over Quinn.

#[cfg(any(feature = "ring", feature = "aws-lc-rs"))]
use std::sync::Arc;
use std::{num::NonZeroUsize, time::Duration};

use futures::{StreamExt, future::BoxFuture, stream::FuturesUnordered};
#[cfg(any(feature = "ring", feature = "aws-lc-rs"))]
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use url::Url;

#[cfg(any(feature = "ring", feature = "aws-lc-rs"))]
use crate::{CongestionControl, crypto};
use crate::{Connect, ServerError, Session, Settings};

#[cfg(any(feature = "ring", feature = "aws-lc-rs"))]
/// Construct a WebTransport [Server] using sensible defaults.
///
/// This is optional; advanced users may use [Server::new] directly.
pub struct ServerBuilder {
    provider: crypto::Provider,
    addr: std::net::SocketAddr,
    transport: quinn::TransportConfig,
    handshake_timeout: Option<Duration>,
    max_pending_handshakes: NonZeroUsize,
}

#[cfg(any(feature = "ring", feature = "aws-lc-rs"))]
impl Default for ServerBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(any(feature = "ring", feature = "aws-lc-rs"))]
impl ServerBuilder {
    /// Create a server builder with sensible defaults.
    pub fn new() -> Self {
        Self {
            provider: crypto::default_provider(),
            addr: std::net::SocketAddr::from(([0_u16; 8], 443)),
            transport: quinn::TransportConfig::default(),
            handshake_timeout: None,
            max_pending_handshakes: NonZeroUsize::MAX,
        }
    }

    /// Listen on the specified address.
    pub fn with_addr(self, addr: std::net::SocketAddr) -> Self {
        Self { addr, ..self }
    }

    /// Enable the specified congestion controller.
    pub fn with_congestion_control(mut self, algorithm: CongestionControl) -> Self {
        match algorithm {
            CongestionControl::LowLatency => self.transport.congestion_controller_factory(
                Arc::new(quinn::congestion::NewRenoConfig::default()),
            ),
            CongestionControl::Throughput => self
                .transport
                .congestion_controller_factory(Arc::new(quinn::congestion::BbrConfig::default())),
            CongestionControl::Default => self
                .transport
                .congestion_controller_factory(Arc::new(quinn::congestion::CubicConfig::default())),
        };

        self
    }

    /// Replace the QUIC transport configuration.
    ///
    /// Use this to bound receive windows, concurrent streams, datagram buffers,
    /// idle time, and other per-connection resources.
    pub fn with_transport_config(mut self, transport: quinn::TransportConfig) -> Self {
        self.transport = transport;
        self
    }

    /// Limit the combined QUIC, HTTP/3 SETTINGS, and CONNECT handshake duration.
    pub fn with_handshake_timeout(mut self, timeout: Duration) -> Self {
        self.handshake_timeout = Some(timeout);
        self
    }

    /// Limit the number of handshakes retained before accepting another connection.
    pub fn with_max_pending_handshakes(mut self, limit: NonZeroUsize) -> Self {
        self.max_pending_handshakes = limit;
        self
    }

    /// Supply a certificate used for TLS.
    pub fn with_certificate(
        self,
        chain: Vec<CertificateDer<'static>>,
        key: PrivateKeyDer<'static>,
    ) -> Result<Server, ServerError> {
        let mut config = rustls::ServerConfig::builder_with_provider(self.provider.clone())
            .with_protocol_versions(&[&rustls::version::TLS13])?
            .with_no_client_auth()
            .with_single_cert(chain, key)?;

        config.alpn_protocols = vec![crate::ALPN.as_bytes().to_vec()]; // Required ALPN.

        let config: quinn::crypto::rustls::QuicServerConfig = config
            .try_into()
            .map_err(|_| ServerError::InvalidCryptoConfiguration)?;
        let mut config = quinn::ServerConfig::with_crypto(Arc::new(config));
        config.transport_config(Arc::new(self.transport));

        let server = quinn::Endpoint::server(config, self.addr)
            .map_err(|e| ServerError::IoError(e.into()))?;

        Ok(Server::with_accept_limits(
            server,
            self.handshake_timeout,
            self.max_pending_handshakes,
        ))
    }

    /// Supply certificates for multiple hostnames using SNI.
    pub fn with_certificates(
        self,
        certificates: Vec<(String, Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)>,
    ) -> Result<Server, ServerError> {
        let mut resolver = rustls::server::ResolvesServerCertUsingSni::new();

        for (name, chain, key) in certificates {
            let certified = rustls::sign::CertifiedKey::from_der(chain, key, &self.provider)?;
            resolver.add(&name, certified)?;
        }

        let mut config = rustls::ServerConfig::builder_with_provider(self.provider.clone())
            .with_protocol_versions(&[&rustls::version::TLS13])?
            .with_no_client_auth()
            .with_cert_resolver(Arc::new(resolver));

        config.alpn_protocols = vec![crate::ALPN.as_bytes().to_vec()];

        let config: quinn::crypto::rustls::QuicServerConfig = config
            .try_into()
            .map_err(|_| ServerError::InvalidCryptoConfiguration)?;
        let mut config = quinn::ServerConfig::with_crypto(Arc::new(config));
        config.transport_config(Arc::new(self.transport));

        let server = quinn::Endpoint::server(config, self.addr)
            .map_err(|e| ServerError::IoError(e.into()))?;

        Ok(Server::with_accept_limits(
            server,
            self.handshake_timeout,
            self.max_pending_handshakes,
        ))
    }
}

/// A WebTransport server that accepts new sessions.
pub struct Server {
    endpoint: quinn::Endpoint,
    accept: FuturesUnordered<BoxFuture<'static, Result<Request, ServerError>>>,
    handshake_timeout: Option<Duration>,
    max_pending_handshakes: NonZeroUsize,
}

impl Server {
    /// Manually create a new server with a preconfigured endpoint.
    ///
    /// NOTE: The ALPN must be set to `crate::ALPN` for WebTransport to work.
    pub fn new(endpoint: quinn::Endpoint) -> Self {
        Self {
            endpoint,
            accept: Default::default(),
            handshake_timeout: None,
            max_pending_handshakes: NonZeroUsize::MAX,
        }
    }

    fn with_accept_limits(
        endpoint: quinn::Endpoint,
        handshake_timeout: Option<Duration>,
        max_pending_handshakes: NonZeroUsize,
    ) -> Self {
        Self {
            endpoint,
            accept: Default::default(),
            handshake_timeout,
            max_pending_handshakes,
        }
    }

    /// Accept a new WebTransport session request from a client.
    ///
    /// Returns `None` after the endpoint is closed. Handshake failures are
    /// returned to the caller instead of being silently discarded.
    pub async fn accept(&mut self) -> Option<Result<Request, ServerError>> {
        loop {
            tokio::select! {
                res = self.endpoint.accept(), if self.accept.len() < self.max_pending_handshakes.get() => {
                    let conn = res?;
                    let timeout = self.handshake_timeout;
                    self.accept.push(Box::pin(async move {
                        let handshake = async move {
                            let conn = conn.await?;
                            Request::accept(conn).await
                        };
                        match timeout {
                            Some(timeout) => tokio::time::timeout(timeout, handshake)
                                .await
                                .map_err(|_| ServerError::HandshakeTimeout)?,
                            None => handshake.await,
                        }
                    }));
                }
                Some(res) = self.accept.next() => {
                    return Some(res);
                }
            }
        }
    }
}

/// A mostly complete WebTransport handshake awaiting server accept/reject based on URL.
pub struct Request {
    conn: quinn::Connection,
    settings: Settings,
    connect: Connect,
}

impl Request {
    /// Accept a new WebTransport session from a client.
    pub async fn accept(conn: quinn::Connection) -> Result<Self, ServerError> {
        // Perform the HTTP/3 handshake by sending/receiving SETTINGS frames.
        let settings = Settings::connect(&conn, false).await?;

        // Accept the CONNECT request but defer the response.
        let connect = Connect::accept(&conn).await?;

        // Return the request while retaining settings/connect streams.
        Ok(Self {
            conn,
            settings,
            connect,
        })
    }

    /// Return the URL provided by the client.
    pub fn url(&self) -> &Url {
        self.connect.url()
    }

    /// Accept the session and return a 200 OK response.
    pub async fn ok(mut self) -> Result<Session, ServerError> {
        self.connect.respond(http::StatusCode::OK).await?;
        Ok(Session::new(self.conn, self.settings, self.connect))
    }

    /// Reject the session and return the provided HTTP status code.
    pub async fn close(mut self, status: http::StatusCode) -> Result<(), ServerError> {
        self.connect.respond(status).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builder_records_handshake_admission_limits() {
        let limit = NonZeroUsize::new(17).unwrap();
        let builder = ServerBuilder::new()
            .with_handshake_timeout(Duration::from_secs(9))
            .with_max_pending_handshakes(limit);
        assert_eq!(builder.handshake_timeout, Some(Duration::from_secs(9)));
        assert_eq!(builder.max_pending_handshakes, limit);
    }
}
