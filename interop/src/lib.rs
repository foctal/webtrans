//! Cross-implementation WebTransport interoperability test support.

/// Native backend selected for independent and browser interoperability tests.
#[cfg(not(feature = "quion"))]
pub use webtrans_quinn as native;
/// Native backend selected for independent and browser interoperability tests.
#[cfg(feature = "quion")]
pub use webtrans_quion as native;
