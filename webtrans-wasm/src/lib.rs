//! WebTransport wrapper for WebAssembly.
//!
//! Provides ergonomic Rust bindings around the browser WebTransport API.

mod client;
mod error;
mod recv;
mod send;
mod session;

pub use client::*;
pub use error::*;
pub use recv::*;
pub use send::*;
pub use session::*;

pub use webtrans_trait as generic;
