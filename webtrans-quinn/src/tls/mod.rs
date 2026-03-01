//! TLS helpers for managing certificates and private keys.

pub mod cert;
pub mod key;

use webtrans_proto::{Error, Result};

use cert::SkipServerVerification;
use rustls::client::{ClientConfig, WebPkiServerVerifier};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use rustls::server::ServerConfig;
use std::path::Path;
use std::sync::Arc;

/// Alias of [`rustls::server::ServerConfig`].
pub type TlsServerConfig = rustls::server::ServerConfig;

/// Alias of [`rustls::client::ClientConfig`].
pub type TlsClientConfig = rustls::client::ClientConfig;

/// Generate a self-signed certificate and private key (DER).
pub fn generate_self_signed_pair_der(
    subject_alt_names: Vec<String>,
) -> Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)> {
    let cert = rcgen::generate_simple_self_signed(subject_alt_names)
        .map_err(|e| Error::Tls(e.to_string()))?;

    let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der()));
    let cert_chain = vec![CertificateDer::from(cert.cert)];
    Ok((cert_chain, key))
}

/// Generate a self-signed certificate and private key (PEM).
pub fn generate_self_signed_pair_pem(
    subject_alt_names: Vec<String>,
) -> Result<(Vec<String>, String)> {
    let cert = rcgen::generate_simple_self_signed(subject_alt_names)
        .map_err(|e| Error::Tls(e.to_string()))?;

    let key = cert.signing_key.serialize_pem();
    let cert_chain = vec![cert.cert.pem()];
    Ok((cert_chain, key))
}

/// Load a certificate chain and private key from files.
pub fn load_cert(
    cert_path: &Path,
    key_path: &Path,
) -> Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)> {
    let cert_chain = cert::load_certs(cert_path)?;
    let key = key::load_key(key_path)?;
    Ok((cert_chain, key))
}

/// TLS configuration containing both client and server configurations.
#[derive(Debug, Clone)]
pub struct TlsConfig {
    /// TLS client configuration used by outbound WebTransport connections.
    pub client_config: ClientConfig,
    /// TLS server configuration used by inbound WebTransport listeners.
    pub server_config: ServerConfig,
}

impl TlsConfig {
    /// Create a new TLS configuration with the specified certificate and private key.
    pub fn with_cert(cert_path: &Path, key_path: &Path) -> Result<Self> {
        let client_config = TlsClientConfigBuilder::new_with_native_certs()?
            .with_alpn_protocols(vec![b"h3".to_vec()])
            .build();

        let server_config = TlsServerConfigBuilder::new_with_cert(cert_path, key_path)?
            .with_alpn_protocols(vec![b"h3".to_vec()])
            .build();

        Ok(Self {
            client_config,
            server_config,
        })
    }

    /// Create a new TLS configuration with self-signed certificates (localhost).
    pub fn with_self_signed_certs() -> Result<Self> {
        let client_config = TlsClientConfigBuilder::new_with_native_certs()?
            .with_alpn_protocols(vec![b"h3".to_vec()])
            .build();

        let server_config =
            TlsServerConfigBuilder::new_with_self_signed_certs(vec!["localhost".into()])?
                .with_alpn_protocols(vec![b"h3".to_vec()])
                .build();

        Ok(Self {
            client_config,
            server_config,
        })
    }

    /// Create a new TLS configuration with system certificates (server side uses self-signed localhost).
    pub fn new_native_config() -> Result<Self> {
        let client_config = TlsClientConfigBuilder::new_with_native_certs()?
            .with_alpn_protocols(vec![b"h3".to_vec()])
            .build();

        let server_config =
            TlsServerConfigBuilder::new_with_self_signed_certs(vec!["localhost".into()])?
                .with_alpn_protocols(vec![b"h3".to_vec()])
                .build();

        Ok(Self {
            client_config,
            server_config,
        })
    }

    /// Create a new TLS configuration with no certificate verification (testing only).
    pub fn new_insecure_config() -> Result<Self> {
        let client_config = TlsClientConfigBuilder::new_insecure()?
            .with_alpn_protocols(vec![b"h3".to_vec()])
            .build();

        let server_config =
            TlsServerConfigBuilder::new_with_self_signed_certs(vec!["localhost".into()])?
                .with_alpn_protocols(vec![b"h3".to_vec()])
                .build();

        Ok(Self {
            client_config,
            server_config,
        })
    }
}

/// Server config builder (owned builder).
#[derive(Debug, Clone)]
pub struct TlsServerConfigBuilder {
    inner: TlsServerConfig,
}

impl TlsServerConfigBuilder {
    /// Build a server TLS config using an ephemeral self-signed certificate.
    pub fn new_insecure(subject_alt_names: Vec<String>) -> Result<Self> {
        let (certs, key) = generate_self_signed_pair_der(subject_alt_names)?;
        let inner = ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .map_err(|e| Error::Tls(e.to_string()))?;
        Ok(Self { inner })
    }

    /// Build a server TLS config from certificate and private key files.
    pub fn new_with_cert(cert_path: &Path, key_path: &Path) -> Result<Self> {
        let (certs, key) = load_cert(cert_path, key_path)?;
        let inner = ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .map_err(|e| Error::Tls(e.to_string()))?;
        Ok(Self { inner })
    }

    /// Build a server TLS config using generated self-signed certificates.
    pub fn new_with_self_signed_certs(subject_alt_names: Vec<String>) -> Result<Self> {
        Self::new_insecure(subject_alt_names)
    }

    /// Override ALPN protocol list.
    pub fn with_alpn_protocols(mut self, protocols: Vec<Vec<u8>>) -> Self {
        self.inner.alpn_protocols = protocols;
        self
    }

    /// Finalize and return the server TLS config.
    pub fn build(self) -> TlsServerConfig {
        self.inner
    }
}

/// Client config builder (owned builder).
#[derive(Debug, Clone)]
pub struct TlsClientConfigBuilder {
    inner: TlsClientConfig,
}

impl TlsClientConfigBuilder {
    /// Build a client TLS config that disables certificate verification.
    pub fn new_insecure() -> Result<Self> {
        let inner = ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(SkipServerVerification::new())
            .with_no_client_auth();
        Ok(Self { inner })
    }

    /// Build a client TLS config from native system trust roots.
    pub fn new_with_native_certs() -> Result<Self> {
        let native_certs = cert::get_native_certs()?;
        let inner = ClientConfig::builder()
            .with_root_certificates(native_certs)
            .with_no_client_auth();
        Ok(Self { inner })
    }

    /// Build a client TLS config with a custom WebPKI verifier.
    pub fn new_with_webpki_verifier(verifier: Arc<WebPkiServerVerifier>) -> Result<Self> {
        let inner = ClientConfig::builder()
            .with_webpki_verifier(verifier)
            .with_no_client_auth();
        Ok(Self { inner })
    }

    /// Override ALPN protocol list.
    pub fn with_alpn_protocols(mut self, protocols: Vec<Vec<u8>>) -> Self {
        self.inner.alpn_protocols = protocols;
        self
    }

    /// Finalize and return the client TLS config.
    pub fn build(self) -> TlsClientConfig {
        self.inner
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_self_signed_pair_der() {
        let (cert_chain, key) = generate_self_signed_pair_der(vec!["localhost".into()]).unwrap();
        let rustls_server_config = ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(cert_chain, key);

        if let Err(e) = rustls_server_config {
            panic!("Failed to create ServerConfig: {e}");
        }
    }

    #[test]
    fn test_generate_self_signed_pair_pem() {
        let (cert_chain, key) = generate_self_signed_pair_pem(vec!["localhost".into()]).unwrap();

        let cert_path = Path::new("cert.pem");
        let key_path = Path::new("key.pem");
        std::fs::write(cert_path, cert_chain.join("\n")).unwrap();
        std::fs::write(key_path, key).unwrap();

        let (cert_chain, key) = load_cert(cert_path, key_path).unwrap();
        let rustls_server_config = ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(cert_chain, key);

        if let Err(e) = rustls_server_config {
            panic!("Failed to create ServerConfig: {e}");
        }

        std::fs::remove_file(cert_path).unwrap();
        std::fs::remove_file(key_path).unwrap();
    }
}
