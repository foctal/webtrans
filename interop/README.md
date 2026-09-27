# Interoperability tests

This package is kept outside the main workspace so an independent WebTransport
implementation and browser tooling do not become release dependencies.

Run the native cross-implementation suite with:

```bash
cargo test --manifest-path interop/Cargo.toml --locked
```

The suite starts both implementations locally and checks streams, datagrams,
close codes, request rejection, malformed input, and reconnect behavior.

Run the Chromium suite with Node.js and Playwright:

```bash
cd interop/browser
npm ci
npx playwright install chromium
npm test
```

The Interoperability GitHub Actions workflow runs both suites for each backend
and can be started manually from the Actions tab.

## Quion backend

The same independent and Chromium scenarios can run against `webtrans-quion`:

```bash
cargo test --manifest-path interop/Cargo.toml --locked --features quion
cd interop/browser
WEBTRANS_BACKEND=quion npm test
```

Omitting the feature or environment variable keeps the Quinn backend. Both
backends are covered by the interoperability workflow matrix.
