//! Private key handling utilities.

use rustls::pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer};
use std::{fs, path::Path};
use webtrans_proto::{Error, Result};

/// Load a private key from a file.
pub fn load_key(key_path: &Path) -> Result<PrivateKeyDer<'static>> {
    let key = fs::read(key_path).map_err(|e| Error::Io(e.to_string()))?;

    let key = if key_path.extension().map_or(false, |x| x == "der") {
        // Treat raw DER as PKCS#8.
        PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(key))
    } else {
        // Decode a PEM-encoded key.
        rustls_pemfile::private_key(&mut &*key)
            .map_err(|e| Error::Tls(e.to_string()))?
            .ok_or_else(|| Error::Io("no keys found".into()))?
    };

    Ok(key)
}
