//! WebAssembly WebTransport implementation re-exports.
//!
//! This module exposes browser-based WebTransport bindings for `wasm32`
//! targets, backed by the WebTransport API.
//!
//! Server-side functionality is not available in this environment;
//! only client-side APIs are re-exported.

/// Re-export the underlying browser WebTransport implementation
pub use webtrans_wasm as wasm;

pub use webtrans_wasm::{
    Client, ClientBuilder, CongestionControl, RecvStream, SendStream, Session,
};
