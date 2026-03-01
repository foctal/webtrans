//! Native WebTransport implementation re-exports.
//!
//! This module exposes the Quinn-based WebTransport implementation
//! for non-WASM targets.

/// Re-export the underlying Quinn-based implementation
pub use webtrans_quinn as quinn;

pub use webtrans_quinn::{
    Client, ClientBuilder, CongestionControl, RecvStream, Request, SendStream, Server,
    ServerBuilder, Session,
};

pub use webtrans_quinn::{crypto, tls};
