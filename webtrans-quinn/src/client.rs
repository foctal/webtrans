//! Client-side helpers for WebTransport over Quinn.

use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

#[cfg(any(feature = "ring", feature = "aws-lc-rs"))]
use quinn::crypto::rustls::QuicClientConfig;
use rustls::{client::danger::ServerCertVerifier, pki_types::CertificateDer};
use tokio::net::lookup_host;
use url::{Host, Url};

#[cfg(any(feature = "ring", feature = "aws-lc-rs"))]
use crate::ALPN;
use crate::crypto;
use crate::{ClientError, Session};

/// Congestion control algorithm to use for the connection.
///
/// Different algorithms make different tradeoffs between throughput and latency.
pub enum CongestionControl {
    /// Use the default congestion control algorithm (typically CUBIC).
    Default,
    /// Optimize for throughput (BBR).
    Throughput,
    /// Optimize for low latency (NewReno).
    LowLatency,
}

#[cfg(any(feature = "ring", feature = "aws-lc-rs"))]
/// Construct a WebTransport [Client] using sensible defaults.
///
/// This is optional; advanced users may use [Client::new] directly.
pub struct ClientBuilder {
    provider: crypto::Provider,
    congestion_controller:
        Option<Arc<dyn quinn::congestion::ControllerFactory + Send + Sync + 'static>>,
}

#[cfg(any(feature = "ring", feature = "aws-lc-rs"))]
impl ClientBuilder {
    /// Create a client builder, which can establish multiple [Session]s.
    pub fn new() -> Self {
        Self {
            provider: crypto::default_provider(),
            congestion_controller: None,
        }
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

    /// Accept certificates from servers chained to known root CAs.
    pub fn with_system_roots(self) -> Result<Client, ClientError> {
        let mut roots = rustls::RootCertStore::empty();

        let native = rustls_native_certs::load_native_certs();

        // Log any errors encountered while loading native root certificates.
        for err in native.errors {
            tracing::warn!("failed to load root cert: {err:?}");
        }

        // Add the platform's native root certificates.
        for cert in native.certs {
            if let Err(err) = roots.add(cert) {
                tracing::warn!("failed to add root cert: {err:?}");
            }
        }

        let crypto = self
            .builder()?
            .with_root_certificates(roots)
            .with_no_client_auth();

        self.build(crypto)
    }

    /// Supply certificates for accepted servers instead of using root CAs.
    pub fn with_server_certificates(
        self,
        certs: Vec<CertificateDer>,
    ) -> Result<Client, ClientError> {
        let hashes = certs.iter().map({
            let provider = self.provider.clone();
            move |cert| crypto::sha256(&provider, cert).as_ref().to_vec()
        });

        self.with_server_certificate_hashes(hashes.collect())
    }

    /// Supply SHA-256 hashes for accepted certificates instead of using root CAs.
    pub fn with_server_certificate_hashes(
        self,
        hashes: Vec<Vec<u8>>,
    ) -> Result<Client, ClientError> {
        // Use a custom fingerprint verifier.
        let fingerprints = Arc::new(ServerFingerprints {
            provider: self.provider.clone(),
            fingerprints: hashes,
        });

        // Configure the crypto client.
        let crypto = self
            .builder()?
            .dangerous()
            .with_custom_certificate_verifier(fingerprints.clone())
            .with_no_client_auth();

        self.build(crypto)
    }

    /// Access dangerous configuration options.
    ///
    /// This method returns a builder that provides access to potentially insecure
    /// TLS configurations. These options are opt-in and require explicit acknowledgment
    /// through the builder pattern, making the security implications clear at the call site.
    pub fn dangerous(self) -> DangerousClientBuilder {
        DangerousClientBuilder { inner: self }
    }

    fn builder(
        &self,
    ) -> Result<rustls::ConfigBuilder<rustls::ClientConfig, rustls::WantsVerifier>, ClientError>
    {
        rustls::ClientConfig::builder_with_provider(self.provider.clone())
            .with_protocol_versions(&[&rustls::version::TLS13])
            .map_err(Into::into)
    }

    fn build(self, mut crypto: rustls::ClientConfig) -> Result<Client, ClientError> {
        crypto.alpn_protocols = vec![ALPN.as_bytes().to_vec()];

        let client_config = QuicClientConfig::try_from(crypto)
            .map_err(|_| ClientError::InvalidCryptoConfiguration)?;
        let mut client_config = quinn::ClientConfig::new(Arc::new(client_config));

        let mut transport = quinn::TransportConfig::default();
        if let Some(cc) = &self.congestion_controller {
            transport.congestion_controller_factory(cc.clone());
        }

        client_config.transport_config(transport.into());

        let client = quinn::Endpoint::client(SocketAddr::from(([0_u16; 8], 0)))
            .map_err(|error| ClientError::Io(Arc::new(error)))?;
        Ok(Client {
            endpoint: client,
            config: client_config,
        })
    }
}

#[cfg(any(feature = "ring", feature = "aws-lc-rs"))]
impl Default for ClientBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(any(feature = "ring", feature = "aws-lc-rs"))]
/// Builder for dangerous TLS configuration options.
///
/// This builder exposes potentially insecure TLS settings. Use only when you
/// understand the security implications, such as in local development or over
/// a secure VPN connection.
pub struct DangerousClientBuilder {
    inner: ClientBuilder,
}

#[cfg(any(feature = "ring", feature = "aws-lc-rs"))]
impl DangerousClientBuilder {
    /// Disable certificate verification entirely.
    ///
    /// This makes the connection vulnerable to man-in-the-middle attacks.
    /// Only use this in secure environments, such as local development or over a VPN.
    ///
    /// This method is memory-safe, but dangerous from a security perspective, hence
    /// the explicit `dangerous()` builder requirement.
    pub fn with_no_certificate_verification(self) -> Result<Client, ClientError> {
        let noop = NoCertificateVerification(self.inner.provider.clone());

        let crypto = self
            .inner
            .builder()?
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(noop))
            .with_no_client_auth();

        self.inner.build(crypto)
    }
}

/// A client for connecting to a WebTransport server.
#[derive(Clone, Debug)]
pub struct Client {
    endpoint: quinn::Endpoint,
    config: quinn::ClientConfig,
}

impl Client {
    /// Manually create a client via a Quinn endpoint and config.
    ///
    /// The ALPN must be set to [ALPN].
    pub fn new(endpoint: quinn::Endpoint, config: quinn::ClientConfig) -> Self {
        Self { endpoint, config }
    }

    /// Connect to the server.
    pub async fn connect(&self, url: Url) -> Result<Session, ClientError> {
        validate_url(&url)?;
        let port = url.port().unwrap_or(443);

        let (host, remote) = match url
            .host()
            .ok_or_else(|| ClientError::InvalidDnsName("".to_string()))?
        {
            Host::Domain(domain) => {
                let domain = domain.to_string();
                // Look up the DNS entry.
                let mut remotes = match lookup_host((domain.clone(), port)).await {
                    Ok(remotes) => remotes,
                    Err(_) => return Err(ClientError::InvalidDnsName(domain)),
                };

                // Use the first resolved address.
                let remote = match remotes.next() {
                    Some(remote) => remote,
                    None => return Err(ClientError::InvalidDnsName(domain)),
                };

                (domain, remote)
            }
            Host::Ipv4(ipv4) => (ipv4.to_string(), SocketAddr::new(IpAddr::V4(ipv4), port)),
            Host::Ipv6(ipv6) => (ipv6.to_string(), SocketAddr::new(IpAddr::V6(ipv6), port)),
        };

        // Connect to the server using the resolved address.
        let conn = self
            .endpoint
            .connect_with(self.config.clone(), remote, &host)?;
        let conn = conn.await?;

        // Complete WebTransport connection establishment.
        Session::connect(conn, url).await
    }
}

fn validate_url(url: &Url) -> Result<(), ClientError> {
    if url.scheme() != "https" {
        return Err(ClientError::InvalidUrl(
            "WebTransport requires an https URL".to_string(),
        ));
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(ClientError::InvalidUrl(
            "userinfo is not supported in the authority".to_string(),
        ));
    }
    if url.fragment().is_some() {
        return Err(ClientError::InvalidUrl(
            "URL fragments are not sent in HTTP request targets".to_string(),
        ));
    }
    if url.cannot_be_a_base() || url.host().is_none() {
        return Err(ClientError::InvalidUrl(
            "URL must contain a valid authority and path".to_string(),
        ));
    }
    Ok(())
}

#[cfg_attr(not(any(feature = "ring", feature = "aws-lc-rs")), allow(dead_code))]
#[derive(Debug)]
struct ServerFingerprints {
    provider: crypto::Provider,
    fingerprints: Vec<Vec<u8>>,
}

impl ServerCertVerifier for ServerFingerprints {
    fn verify_server_cert(
        &self,
        end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        let cert_hash = crypto::sha256(&self.provider, end_entity);
        if self
            .fingerprints
            .iter()
            .any(|fingerprint| fingerprint == cert_hash.as_ref())
        {
            return Ok(rustls::client::danger::ServerCertVerified::assertion());
        }

        Err(rustls::Error::InvalidCertificate(
            rustls::CertificateError::UnknownIssuer,
        ))
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}

#[derive(Debug)]
/// Certificate verifier that disables all chain and hostname validation.
///
/// Use only in controlled environments such as local development.
pub struct NoCertificateVerification(Arc<rustls::crypto::CryptoProvider>);

impl rustls::client::danger::ServerCertVerifier for NoCertificateVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_webtransport_urls() {
        assert!(validate_url(&Url::parse("https://example.com/chat").unwrap()).is_ok());
        assert!(validate_url(&Url::parse("http://example.com/chat").unwrap()).is_err());
        assert!(validate_url(&Url::parse("https://user@example.com/chat").unwrap()).is_err());
        assert!(validate_url(&Url::parse("https://example.com/chat#fragment").unwrap()).is_err());
    }
}
