[crates-badge]: https://img.shields.io/crates/v/webtrans.svg
[crates-url]: https://crates.io/crates/webtrans
[doc-url]: https://docs.rs/webtrans/latest/webtrans
[license-badge]: https://img.shields.io/crates/l/webtrans.svg
[examples-url]: https://github.com/foctal/webtrans/tree/main/webtrans/examples

# webtrans [![Crates.io][crates-badge]][crates-url] ![License][license-badge]
Rust implementation of the WebTransport protocol 
for native (QUIC/HTTP3 via Quinn) and WebAssembly (browser WebTransport API) targets.

## Workspace crates

- `webtrans` - top-level facade that selects native (`webtrans-quinn`) or WASM (`webtrans-wasm`).
- `webtrans-proto` - protocol primitives.
- `webtrans-quinn` - native client/server implementation on top of `quinn`.
- `webtrans-trait` - shared traits for sessions and streams.
- `webtrans-wasm` - browser WebTransport bindings for WASM.
- `webtrans-wasm-demo` - simple WASM demo crate.

## Quick start

```toml
[dependencies]
webtrans = "0.4"
```

API documentation is available on [docs.rs][doc-url].

If you need transport-specific features, depend on one of the transport crates directly.

### Native
See [examples][examples-url]

### WebAssembly

The `webtrans-wasm-demo` crate contains a small browser demo that connects to an echo server.

## Benchmarking

Criterion benchmarks are available for webtrans-proto.

```bash
cargo bench -p webtrans-proto
```
