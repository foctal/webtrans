//! Native WebTransport implementation built on top of QUIC using Quion.
//!
//! This crate provides a low-level, QUIC WebTransport API for native environments.
//!
//! The implementation is powered by [`quion`], and most transport-level
//! behavior (congestion control, flow control, crypto, etc.) is delegated
//! directly to Quion.

mod client;
mod error;
mod recv;
mod send;
mod server;
mod session;
pub mod tls;

pub use client::*;
pub use error::*;
pub use recv::*;
pub use send::*;
pub use server::*;
pub use session::*;

mod connect;
mod settings;

use connect::Connect;
pub use connect::ConnectError;
use settings::Settings;
pub use settings::SettingsError;

/// The HTTP/3 ALPN token used when negotiating a QUIC connection.
pub const ALPN: &str = "h3";

/// Whether Quion supports reliable stream resets.
///
/// Builders enable the extension. Each connection must also negotiate it with
/// its peer; streams fall back to ordinary resets when it is unavailable.
pub const RESET_STREAM_AT_SUPPORTED: bool = quion::RESET_STREAM_AT_SUPPORTED;

#[cfg(any(feature = "ring", feature = "aws-lc-rs"))]
fn default_transport_config() -> quion::TransportConfig {
    let mut transport = quion::TransportConfig::default();
    transport
        .set_reset_stream_at(true)
        .set_max_datagram_frame_size(Some(quion::VarInt::from_u32(65_535)));
    transport
}

// Export the simple crypto provider.
pub mod crypto;

// Re-export the underlying QUIC implementation.
pub use quion;

// Re-export the `rustls` crate because it is part of the public API.
pub use rustls;

// Re-export the `http` crate because it is part of the public API.
pub use http;

// Re-export the generic WebTransport traits.
pub use webtrans_trait as generic;

// Close established connections if their HTTP/3 handshake fails or is cancelled.
struct HandshakeGuard(Option<quion::Connection>);

impl HandshakeGuard {
    fn new(conn: &quion::Connection) -> Self {
        Self(Some(conn.clone()))
    }

    fn complete(mut self) {
        self.0.take();
    }
}

impl Drop for HandshakeGuard {
    fn drop(&mut self) {
        if let Some(conn) = self.0.take() {
            conn.close(
                quion::VarInt::from_u32(0x102),
                b"WebTransport handshake aborted",
            );
        }
    }
}
