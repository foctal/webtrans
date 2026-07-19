//! Native WebTransport implementation built on top of QUIC using Quinn.
//!
//! This crate provides a low-level, QUIC WebTransport API for native environments.
//!
//! The implementation is powered by [`quinn`], and most transport-level
//! behavior (congestion control, flow control, crypto, etc.) is delegated
//! directly to Quinn.

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

/// Whether the native transport can use the draft-required RESET_STREAM_AT
/// extension.
///
/// Quinn 0.11 does not expose this extension, so callers that require strict
/// draft-16 conformance should reject the native backend while this is `false`.
pub const RESET_STREAM_AT_SUPPORTED: bool = false;

// Export the simple crypto provider.
pub mod crypto;

// Re-export the underlying QUIC implementation.
pub use quinn;

// Re-export the `rustls` crate because it is part of the public API.
pub use rustls;

// Re-export the `http` crate because it is part of the public API.
pub use http;

// Re-export the generic WebTransport traits.
pub use webtrans_trait as generic;
