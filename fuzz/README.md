# Fuzzing

Install `cargo-fuzz`, then run an individual decoder target:

```bash
cargo install cargo-fuzz
cargo fuzz run varint
cargo fuzz run qpack_huffman
cargo fuzz run frame
cargo fuzz run settings
cargo fuzz run connect
cargo fuzz run capsule
```

The checked-in corpus covers truncated input, oversized declared lengths,
duplicate SETTINGS, and non-canonical QUIC variable-length integers. Corpus
files beginning with `hex:` are decoded by the harness before use so binary
protocol seeds remain readable in reviews.
