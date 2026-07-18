//! Server-side helpers for WebTransport over Quinn.

#[cfg(any(feature = "ring", feature = "aws-lc-rs"))]
use std::sync::Arc;

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
    congestion_controller:
        Option<Arc<dyn quinn::congestion::ControllerFactory + Send + Sync + 'static>>,
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
            congestion_controller: None,
        }
    }

    /// Listen on the specified address.
    pub fn with_addr(self, addr: std::net::SocketAddr) -> Self {
        Self { addr, ..self }
    }

    /// Enable the specified congestion controller.
    pub fn with_congestion_control(mut self, algorithm: CongestionControl) -> Self {
        self.congestion_controller = match algorithm {
            CongestionControl::LowLatency => {
                Some(Arc::new(quinn::congestion::NewRenoConfig::default()))
            }
            CongestionControl::Throughput => {
                Some(Arc::new(quinn::congestion::BbrConfig::default()))
            }
            CongestionControl::Default => None,
        };

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
        config.transport_config(self.transport_config());

        let server = quinn::Endpoint::server(config, self.addr)
            .map_err(|e| ServerError::IoError(e.into()))?;

        Ok(Server::new(server))
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
        config.transport_config(self.transport_config());

        let server = quinn::Endpoint::server(config, self.addr)
            .map_err(|e| ServerError::IoError(e.into()))?;

        Ok(Server::new(server))
    }

    fn transport_config(&self) -> Arc<quinn::TransportConfig> {
        let mut transport = quinn::TransportConfig::default();
        if let Some(controller) = &self.congestion_controller {
            transport.congestion_controller_factory(controller.clone());
        }
        Arc::new(transport)
    }
}

/// A WebTransport server that accepts new sessions.
pub struct Server {
    endpoint: quinn::Endpoint,
    accept: FuturesUnordered<BoxFuture<'static, Result<Request, ServerError>>>,
}

impl Server {
    /// Manually create a new server with a preconfigured endpoint.
    ///
    /// NOTE: The ALPN must be set to `crate::ALPN` for WebTransport to work.
    pub fn new(endpoint: quinn::Endpoint) -> Self {
        Self {
            endpoint,
            accept: Default::default(),
        }
    }

    /// Accept a new WebTransport session request from a client.
    ///
    /// Returns `None` after the endpoint is closed. Handshake failures are
    /// returned to the caller instead of being silently discarded.
    pub async fn accept(&mut self) -> Option<Result<Request, ServerError>> {
        loop {
            tokio::select! {
                res = self.endpoint.accept() => {
                    let conn = res?;
                    self.accept.push(Box::pin(async move {
                        let conn = conn.await?;
                        Request::accept(conn).await
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
