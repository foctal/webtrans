//! Shared incremental decode loop for async protocol reads.

use std::io::Cursor;

use tokio::io::{AsyncRead, AsyncReadExt};

/// Repeatedly read from the stream until `decode` succeeds or fails with a non-retryable error.
pub(crate) async fn read_incremental<S, T, E, D, R>(
    stream: &mut S,
    mut decode: D,
    mut retryable: R,
    unexpected_end: E,
) -> Result<T, E>
where
    S: AsyncRead + Unpin,
    E: From<std::io::Error>,
    D: for<'a> FnMut(&mut Cursor<&'a [u8]>) -> Result<T, E>,
    R: FnMut(&E) -> bool,
{
    let mut buf = Vec::new();

    loop {
        if stream.read_buf(&mut buf).await? == 0 {
            return Err(unexpected_end);
        }

        let mut cursor = Cursor::new(buf.as_slice());
        match decode(&mut cursor) {
            Ok(value) => return Ok(value),
            Err(err) if retryable(&err) => continue,
            Err(err) => return Err(err),
        }
    }
}
