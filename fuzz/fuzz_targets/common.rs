/// Corpus files may use `hex:` followed by hexadecimal bytes. This keeps
/// protocol edge-case seeds reviewable while arbitrary fuzzer input remains
/// raw bytes.
pub fn corpus_bytes(data: &[u8]) -> Vec<u8> {
    let Some(hex) = data.strip_prefix(b"hex:") else {
        return data.to_vec();
    };
    let Ok(hex) = std::str::from_utf8(hex) else {
        return data.to_vec();
    };
    let hex = hex.trim();
    if hex.len() % 2 != 0 {
        return data.to_vec();
    }

    let mut decoded = Vec::with_capacity(hex.len() / 2);
    for pair in hex.as_bytes().chunks_exact(2) {
        let Ok(pair) = std::str::from_utf8(pair) else {
            return data.to_vec();
        };
        let Ok(byte) = u8::from_str_radix(pair, 16) else {
            return data.to_vec();
        };
        decoded.push(byte);
    }
    decoded
}
