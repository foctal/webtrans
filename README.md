[crates-badge]: https://img.shields.io/crates/v/webtrans.svg
[crates-url]: https://crates.io/crates/webtrans
[doc-url]: https://docs.rs/webtrans/latest/webtrans
[license-badge]: https://img.shields.io/crates/l/webtrans.svg
[examples-url]: https://github.com/foctal/webtrans/tree/main/webtrans/examples

# webtrans [![Crates.io][crates-badge]][crates-url] ![License][license-badge]

WebTransport for native Rust (QUIC/HTTP/3 through Quinn) and WebAssembly
(the browser WebTransport API).

## Compatibility

The native transport implements the WebTransport over HTTP/3 negotiation and
wire formats used by `draft-ietf-webtrans-http3-16`, with legacy upgrade-token
acceptance for older peers. Quinn does not yet expose the draft-16
RESET_STREAM_AT extension, so native support is draft-compatible rather than
fully draft-16 compliant.

The WASM transport requires a browser that provides the global `WebTransport`
API in a secure context. Browser certificate and network policy still apply.
Check the target browsers used by your application because WebTransport
availability can differ by browser and deployment environment.

## Installation

The workspace MSRV is Rust 1.85.

```toml
[dependencies]
webtrans = "0.4"
```

API documentation is available on [docs.rs][doc-url]. Depend directly on
`webtrans-quinn`, `webtrans-wasm`, `webtrans-proto`, or `webtrans-trait` when
transport-specific APIs are required.

## Native client

Use system roots for public servers. Pin a certificate or SHA-256 certificate
hash when connecting to a development server with a private certificate.
Disabling verification is intentionally behind `dangerous()` and should only
be used for controlled local development.

```rust,no_run
use std::time::Duration;
use url::Url;
use webtrans::ClientBuilder;

# async fn connect() -> Result<(), Box<dyn std::error::Error>> {
let client = ClientBuilder::new()
    .with_dns_timeout(Duration::from_secs(5))
    .with_handshake_timeout(Duration::from_secs(10))
    .with_system_roots()?;

let session = client
    .connect(Url::parse("https://example.com/webtransport")?)
    .await?;
let (mut send, _recv) = session.open_bi().await?;
send.write_all(b"hello").await?;
send.finish()?;
# Ok(())
# }
```

## Native server and resource limits

Quinn's `TransportConfig` controls per-connection idle time, stream limits,
flow-control windows, and datagram buffers. Pending handshakes have a separate
endpoint-wide limit.

```rust,no_run
use std::{num::NonZeroUsize, time::Duration};
use webtrans::{ServerBuilder, quinn};

# fn build(
#     chain: Vec<webtrans::quinn::rustls::pki_types::CertificateDer<'static>>,
#     key: webtrans::quinn::rustls::pki_types::PrivateKeyDer<'static>,
# ) -> Result<(), Box<dyn std::error::Error>> {
let mut transport = quinn::quinn::TransportConfig::default();
transport
    .max_idle_timeout(Some(Duration::from_secs(30).try_into()?))
    .max_concurrent_bidi_streams(128_u32.into())
    .max_concurrent_uni_streams(128_u32.into())
    .stream_receive_window((512_u32 * 1024).into())
    .receive_window((2_u32 * 1024 * 1024).into())
    .datagram_receive_buffer_size(Some(1024 * 1024));

let mut server = ServerBuilder::new()
    .with_transport_config(transport)
    .with_handshake_timeout(Duration::from_secs(10))
    .with_max_pending_handshakes(NonZeroUsize::new(256).unwrap())
    .with_certificate(chain, key)?;

# async move {
while let Some(result) = server.accept().await {
    let request = match result {
        Ok(request) => request,
        Err(error) => {
            eprintln!("handshake failed: {error}");
            continue;
        }
    };
    tokio::spawn(async move {
        if request.url().path() == "/webtransport" {
            let _session = request.ok().await;
        } else {
            let _ = request
                .close(webtrans::quinn::http::StatusCode::NOT_FOUND)
                .await;
        }
    });
}
# };
# Ok(())
# }
```

Choose limits from a memory budget and expected bandwidth-delay product. In
particular, worst-case receive memory grows with the number of connections,
concurrent streams, receive windows, and buffered datagrams. Applications
should also bound their own stream payloads and operation durations.

Complete runnable programs, including PEM certificate loading and a local
self-signed setup, are in the [examples directory][examples-url]:

```bash
cargo run -p webtrans --example echo-server
cargo run -p webtrans --example echo-client
```

## WebAssembly

The `webtrans-wasm-demo` crate contains a browser example. Build the facade and
WASM crates with:

```bash
rustup target add wasm32-unknown-unknown
cargo build --target wasm32-unknown-unknown -p webtrans -p webtrans-wasm
```

## Workspace crates

- `webtrans`: target-selecting facade.
- `webtrans-proto`: bounded protocol primitives and HTTP/3 field validation.
- `webtrans-quinn`: native client/server implementation.
- `webtrans-trait`: transport-agnostic session and stream traits.
- `webtrans-wasm`: browser bindings.
- `webtrans-wasm-demo`: browser demo.

## Versioning and support

The crates follow Semantic Versioning as a coordinated workspace. Before 1.0,
a minor release may contain public API changes; patch releases remain
backward-compatible unless a security or soundness issue requires otherwise.
The MSRV may increase in a minor release and is recorded in `Cargo.toml` and
the changelog. Supported versions and vulnerability reporting are documented
in [SECURITY.md](SECURITY.md).

## Benchmarking

Criterion benchmarks are available for `webtrans-proto`:

```bash
cargo bench -p webtrans-proto
```
