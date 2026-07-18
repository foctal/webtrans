# Production Readiness Review

Last reviewed: 2026-07-18

This review uses `draft-ietf-webtrans-http3-16` (2026-07-06), RFC 9114,
RFC 9297, and RFC 9221 as the current protocol baseline.

## Release decision

**Not yet ready for an unqualified production-support claim.**

The memory-safety, partial-write, input-bounding, error-observability, and
current negotiation issues found in this review have been fixed. The remaining
release blockers are current-draft transport support and real interoperability
coverage. The draft requires the RESET_STREAM_AT QUIC extension, which Quinn
0.11.9 does not currently expose.

## P0 - Release blockers

- [x] Prevent data loss in the default `SendStream::write_chunk`
  implementation when a transport performs a partial write.
- [x] Remove the unsafe uninitialized-buffer conversion from the generic
  `RecvStream::read_buf` implementation.
- [x] Bound CONNECT, SETTINGS, and capsule incremental decoders to prevent
  unbounded memory growth from attacker-controlled frame lengths.
- [x] Prevent incremental protocol reads from consuming and discarding bytes
  belonging to the next capsule or frame.
- [x] Return server handshake failures from `Server::accept` instead of
  silently dropping them.
- [x] Use the current `webtransport-h3` extended CONNECT token and
  `SETTINGS_WT_ENABLED` code point while retaining legacy token acceptance.
- [x] Reject duplicate SETTINGS and invalid boolean SETTINGS values.
- [x] Reject invalid native client URLs before DNS or network activity.
- [ ] Add RESET_STREAM_AT support after the Quinn transport exposes the
  extension required by draft-16. Until then, document native transport support
  as draft-compatible rather than fully draft-16 compliant.
- [ ] Add automated interoperability tests against at least current Chromium
  and one independent HTTP/3 WebTransport implementation. Cover streams,
  datagrams, close codes, rejected requests, malformed input, and reconnects.

## P1 - Required for a stable public release

- [x] Preserve full 32-bit browser session close codes and reasons.
- [x] Remove avoidable panic paths from endpoint creation, stream acceptance,
  datagram capability queries, and WASM priority updates.
- [x] Apply the configured congestion controller to server transports.
- [x] Expand CI to enforce formatting, warnings, Clippy, tests, documentation,
  the alternate native crypto backend, and the WASM target.
- [x] Declare the MSRV implied by Rust 2024 (`1.85`).
- [x] Upgrade vulnerable Quinn, rustls-webpki, aws-lc-sys, crossbeam,
  anyhow, and rand lockfile entries identified by `cargo audit`.
- [x] Remove the unmaintained `rustls-pemfile` dependency in favor of
  `rustls-pki-types` PEM support.
- [x] Replace the minimal QPACK implementation or complete its HTTP/3 field
  validation. The decoder remains intentionally static-table-only and rejects
  dynamic references while enforcing RFC 9114 field and pseudo-header rules.
- [ ] Add fuzz targets for VarInt, QPACK/Huffman, frame, SETTINGS, CONNECT, and
  capsule decoders. Seed the corpus with truncated, oversized, duplicate, and
  non-canonical inputs.
- [x] Add configurable handshake, idle, and DNS timeouts. Current async APIs
  rely on callers to wrap operations in a timeout.
- [ ] Add explicit server resource-limit configuration and tests for concurrent
  streams, receive windows, datagram buffers, pending handshakes, and connection
  admission.
  - [x] Expose Quinn transport limits and pending-handshake admission settings.
  - [ ] Add end-to-end saturation tests for each configured limit.
- [x] Redesign the transport-agnostic datagram send API to be asynchronous.
  The browser implementation now awaits the JavaScript write and reports its
  failure through the shared trait method.
- [x] Define and test cancellation and clone semantics for browser stream
  acceptors. Clones serialize access to shared Web Streams readers, and a
  cancelled accept preserves its pending read for the next caller.
- [ ] Decide whether dropping an unanswered `Request` should send an explicit
  rejection, and make that behavior observable and documented.

## P2 - Operational and maintenance work

- [x] Add `SECURITY.md` with supported versions, private vulnerability
  reporting instructions, and response expectations.
- [x] Add a changelog and a documented semver/MSRV policy.
- [x] Expand the README with native client/server examples, certificate
  verification guidance, timeout examples, limits, browser requirements, and
  protocol-draft compatibility.
- [x] Add a weekly and per-change `cargo audit` CI job.
- [ ] Add automated dependency license checks.
- [ ] Add code coverage reporting and set a meaningful floor for protocol and
  error-path coverage.
- [ ] Add benchmarks or load tests for connection setup, concurrent streams,
  datagrams, and adversarial decoder inputs, with recorded regression budgets.
- [ ] Test supported operating systems and architectures explicitly. At
  minimum, cover Linux, macOS, Windows, and `wasm32-unknown-unknown`.
- [ ] Review public API names and breaking changes before the next release.
  This review intentionally changes `Server::accept`, removes the panic-prone
  native `Client::default`, makes WASM `set_priority` return a `Result`, and
  adds a structured WASM session-close error, asynchronous generic datagram
  sends, and fallible WASM `Session::new`.

## Verification checklist

- [x] `cargo fmt --all -- --check`
- [x] `cargo clippy --workspace --all-targets -- -D warnings`
- [x] `cargo test --workspace --all-targets`
- [x] `cargo check -p webtrans-quinn --no-default-features --features aws-lc-rs`
- [x] `cargo check --target wasm32-unknown-unknown -p webtrans -p webtrans-wasm -p webtrans-wasm-demo`
- [x] `cargo doc --workspace --no-deps`
- [x] `cargo audit`
