//! Certificate handling utilities.

use rustls::client::danger::ServerCertVerifier;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use std::{fs, path::Path, sync::Arc};
use webtrans_proto::{Error, Result};

/// Get the native certificates from the system.
pub fn get_native_certs() -> Result<rustls::RootCertStore> {
    let mut root_store = rustls::RootCertStore::empty();

    let cert_result = rustls_native_certs::load_native_certs();

    for cert in cert_result.certs {
        let _ = root_store.add(cert);
    }

    Ok(root_store)
}

/// Load a certificate chain from a file (.der or .pem).
pub fn load_certs(cert_path: &Path) -> Result<Vec<CertificateDer<'static>>> {
    let cert_bytes = fs::read(cert_path).map_err(|e| Error::Io(e.to_string()))?;

    if cert_path.extension().is_some_and(|x| x == "der") {
        return Ok(vec![CertificateDer::from(cert_bytes)]);
    }

    CertificateDer::pem_slice_iter(&cert_bytes)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| Error::Tls(e.to_string()))
}

/// Dummy certificate verifier that treats any certificate as valid.
/// NOTE: This is vulnerable to MITM attacks; use only for testing.
#[derive(Debug)]
pub struct SkipServerVerification(Arc<rustls::crypto::CryptoProvider>);

impl SkipServerVerification {
    /// Use the crate's selected default crypto provider.
    pub fn new() -> Arc<Self> {
        Self::with_provider(crate::crypto::default_provider())
    }

    /// Create a verifier backed by the provided crypto provider.
    pub fn with_provider(provider: Arc<rustls::crypto::CryptoProvider>) -> Arc<Self> {
        Arc::new(Self(provider))
    }
}

impl ServerCertVerifier for SkipServerVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp: &[u8],
        _now: UnixTime,
    ) -> std::result::Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
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
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
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
