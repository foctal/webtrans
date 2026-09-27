//! WebTransport library for native and WebAssembly
//!
//! - **Native** (`non-wasm32`): Quinn by default, or Quion with the `quion` feature.
//!   When both features are enabled, Quinn remains the default and Quion is
//!   available through `webtrans::quion`.
//! - **WebAssembly** (`wasm32`): Browser WebTransport API bindings
//!   via webtrans-wasm

pub use webtrans_proto::*;

#[cfg(all(not(target_arch = "wasm32"), feature = "quinn"))]
#[path = "quinn.rs"]
mod transport;

#[cfg(all(not(target_arch = "wasm32"), feature = "quion"))]
pub use webtrans_quion as quion;

#[cfg(all(not(target_arch = "wasm32"), not(feature = "quinn"), feature = "quion"))]
#[path = "quion.rs"]
mod transport;

#[cfg(all(
    not(target_arch = "wasm32"),
    not(any(feature = "quinn", feature = "quion"))
))]
compile_error!("enable the quinn or quion feature for a native WebTransport backend");

#[cfg(target_arch = "wasm32")]
#[path = "wasm.rs"]
mod transport;

#[cfg(any(target_arch = "wasm32", feature = "quinn", feature = "quion"))]
pub use transport::*;
