# Interoperability tests

This package is kept outside the main workspace so an independent WebTransport
implementation and browser tooling do not become release dependencies.

Run the native cross-implementation suite with:

```bash
cargo test --manifest-path interop/Cargo.toml
```

The suite starts both implementations locally and checks streams, datagrams,
close codes, request rejection, malformed input, and reconnect behavior.

The Chromium suite is run by the dedicated `interop` GitHub Actions workflow.
It requires the current stable Chrome/Chromium and Node.js dependencies listed
in `browser/package.json`.
