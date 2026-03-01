//! WebTransport library for native and WebAssembly
//!
//! - **Native** (`non-wasm32`): Quinn-based HTTP/3 + QUIC implementation
//!   via webtrans-quinn
//! - **WebAssembly** (`wasm32`): Browser WebTransport API bindings
//!   via webtrans-wasm

pub use webtrans_proto::*;

#[cfg(any(not(target_arch = "wasm32"), target_os = "wasi"))]
#[path = "quinn.rs"]
mod transport;

#[cfg(all(target_arch = "wasm32", not(target_os = "wasi")))]
#[path = "wasm.rs"]
mod transport;

pub use transport::*;
