[crates-badge]: https://img.shields.io/crates/v/webtrans.svg
[crates-url]: https://crates.io/crates/webtrans
[doc-url]: https://docs.rs/webtrans/latest/webtrans
[license-badge]: https://img.shields.io/crates/l/webtrans.svg
[examples-url]: https://github.com/foctal/webtrans/tree/main/webtrans/examples

# webtrans [![Crates.io][crates-badge]][crates-url] ![License][license-badge]

WebTransport implementation for native and WebAssembly.

## Compatibility

The native transports implement the WebTransport over HTTP/3 negotiation and
wire formats used by `draft-ietf-webtrans-http3-16`, with legacy upgrade-token
acceptance for older peers. Quinn does not yet expose the draft-16
RESET_STREAM_AT extension, so the Quinn backend is draft-compatible rather than
fully draft-16 compliant. Native applications can inspect
`webtrans_quinn::RESET_STREAM_AT_SUPPORTED`; it remains `false` until Quinn
provides the required transport extension.

The Quion backend uses Quion 0.2.0 and enables RESET_STREAM_AT by default.
`webtrans_quion::RESET_STREAM_AT_SUPPORTED` reports transport capability;
`Session::reset_stream_at_negotiated()` reports the actual peer negotiation.
Without negotiation, resets fall back to RESET_STREAM for compatibility.

The WASM transport requires a browser that provides the global `WebTransport`
API in a secure context. Browser certificate and network policy still apply.
Check the target browsers used by your application because WebTransport
availability can differ by browser and deployment environment.

## Installation

```toml
[dependencies]
webtrans = "0.6"
```

API documentation is available on [docs.rs][doc-url]. Depend directly on
`webtrans-quinn`, `webtrans-quion`, `webtrans-wasm`, `webtrans-proto`, or `webtrans-trait` when
transport-specific APIs are required.

## Selecting Quion

Quinn remains the default native backend. Select Quion through the facade with:

```toml
[dependencies]
webtrans = { version = "0.6", default-features = false, features = ["quion"] }
```

The same `webtrans::{ClientBuilder, ServerBuilder, Session}` entry points work
with either backend. If both features are enabled, top-level types remain
Quinn-based and Quion is available through `webtrans::quion`. For direct access,
depend on `webtrans-quion` and use `webtrans_quion::quion` for transport types.

Both native backends implement `webtrans-trait` and Tokio stream I/O. Quion's
stream errors, priority (`u16`, lower first), stream IDs (`Option<StreamId>`),
and configuration types follow Quion's API rather than Quinn's. Its
`read_exact` and `read_to_end` methods return `webtrans_quion::ReadError`.
The `ring` crypto feature is enabled by default; direct users can select
`default-features = false, features = ["aws-lc-rs"]` instead.

Quion builders enable QUIC datagrams and reliable resets by default. Supplying
`with_transport_config` replaces those defaults, so explicitly enable the
extensions in a custom configuration:

```rust
use webtrans_quion::quion::{TransportConfig, VarInt};

let mut transport = TransportConfig::default();
transport
    .set_reset_stream_at(true)
    .set_max_datagram_frame_size(Some(VarInt::from_u32(65_535)))
    .set_initial_max_streams_bidi(VarInt::from_u32(128))
    .set_initial_max_streams_uni(VarInt::from_u32(128))
    .set_max_stream_metadata_entries(512)
    .set_max_recv_buffered_stream_data_per_connection(2 * 1024 * 1024)
    .set_max_send_buffered_stream_data(2 * 1024 * 1024)
    .set_max_endpoint_memory_bytes(64 * 1024 * 1024);
```

Quion replenishes stream admission credit when streams are accepted, so its
initial stream limits do not cap all active application streams. Use metadata,
connection-memory, and application concurrency limits together. The Quion
server builder owns the UDP driver and keeps it alive for accepted requests,
sessions, and streams. `Server::new` instead requires the caller to run and
retain the configured endpoint's driver.

Run the existing echo examples with Quion using:

```bash
cargo run -p webtrans --no-default-features --features quion --example echo-server
cargo run -p webtrans --no-default-features --features quion --example echo-client
```

## Native client

Use system roots for public servers. Pin a certificate or SHA-256 certificate
hash when connecting to a development server with a private certificate.
Use `dangerous()` to disable certificate verification for local development only.

```rust,no_run
use std::time::Duration;
use url::Url;
use webtrans::ClientBuilder;

async fn connect() -> Result<(), Box<dyn std::error::Error>> {
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
    Ok(())
}
```

## Native server and resource limits

Quinn's `TransportConfig` controls per-connection idle time, stream limits,
flow-control windows, and datagram buffers. Pending handshakes have a separate
endpoint-wide limit.

```rust,no_run
use std::{num::NonZeroUsize, time::Duration};
use webtrans::{ServerBuilder, quinn};

fn build(
    chain: Vec<webtrans::quinn::rustls::pki_types::CertificateDer<'static>>,
    key: webtrans::quinn::rustls::pki_types::PrivateKeyDer<'static>,
) -> Result<(), Box<dyn std::error::Error>> {
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

    async move {
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
    };
    Ok(())
}
```

Every accepted `Request` must be completed with `Request::ok` or
`Request::close`. Dropping an unanswered request automatically sends
`500 Internal Server Error` and emits a tracing event. If the request is
dropped after leaving its Tokio runtime, the response cannot be scheduled and
an error event is emitted instead.

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
- `webtrans-quinn`: native client/server implementation using Quinn.
- `webtrans-quion`: native client/server implementation using Quion.
- `webtrans-trait`: transport-agnostic session and stream traits.
- `webtrans-wasm`: browser bindings.
- `webtrans-wasm-demo`: browser demo.

## Benchmarking

Criterion benchmarks are available for `webtrans-proto`:

```bash
cargo bench -p webtrans-proto
```
