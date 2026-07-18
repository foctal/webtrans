# Changelog

All notable changes to this workspace are documented here. The project follows
Semantic Versioning, with the pre-1.0 compatibility policy described in the
README.

## Unreleased

### Added

- Configurable DNS and full connection-handshake timeouts for native clients.
- Configurable handshake timeout and pending-handshake admission limit for
  native servers.
- Builder access to Quinn transport configuration for idle timeouts, stream
  limits, flow-control windows, and datagram buffers.
- Browser tests for cancellation and clone coordination of incoming stream
  readers.
- Security reporting and support policy.

### Changed

- QPACK decoding now enforces HTTP/3 field-name, field-value, pseudo-header
  ordering, duplicate pseudo-header, connection-specific field, and
  request/response pseudo-header rules.
- Non-zero QPACK dynamic-table prefixes are rejected explicitly because the
  decoder intentionally supports only static-table and literal fields.
- The transport-agnostic datagram send API is asynchronous so browser write
  failures are returned to callers.
- Browser session clones share and serialize incoming stream and datagram
  readers. Cancelling an accept future preserves its pending browser read for
  the next caller.
- Browser `Session::new` now returns a `Result` because acquiring the required
  Web Streams locks can fail.
