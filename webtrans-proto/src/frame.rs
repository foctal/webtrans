//! HTTP/3 frame type utilities used by WebTransport.

use bytes::{Buf, BufMut};

use crate::grease::is_grease_value;
use crate::{VarInt, VarIntUnexpectedEnd};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
/// HTTP/3 frame type identifier.
pub struct Frame(pub VarInt);

impl Frame {
    /// Decode a frame type varint from the buffer.
    pub fn decode<B: Buf>(buf: &mut B) -> Result<Self, VarIntUnexpectedEnd> {
        let typ = VarInt::decode(buf)?;
        Ok(Frame(typ))
    }

    /// Encode this frame type varint to the buffer.
    pub fn encode<B: BufMut>(&self, buf: &mut B) {
        self.0.encode(buf)
    }

    /// Return `true` when this frame type uses RFC 9114 GREASE spacing.
    pub fn is_grease(&self) -> bool {
        is_grease_value(self.0.into_inner())
    }

    /// Read one full frame header and return its type plus a limited payload view.
    pub fn read<B: Buf>(
        buf: &mut B,
    ) -> Result<(Frame, bytes::buf::Take<&mut B>), VarIntUnexpectedEnd> {
        let typ = Frame::decode(buf)?;
        let size = VarInt::decode(buf)?;

        let mut limit = Buf::take(buf, size.into_inner() as usize);
        if limit.remaining() < limit.limit() {
            return Err(VarIntUnexpectedEnd);
        }

        // Retry if this is a GREASE frame that must be ignored.
        if typ.is_grease() {
            limit.advance(limit.limit());
            return Self::read(limit.into_inner());
        }

        Ok((typ, limit))
    }

    /// Build a frame type from a known `u32` value.
    pub const fn from_u32(value: u32) -> Self {
        Self(VarInt::from_u32(value))
    }

    // Frames sent at the start of a bidirectional stream.
    /// DATA frame type (`0x00`).
    pub const DATA: Frame = Frame::from_u32(0x00);
    /// HEADERS frame type (`0x01`).
    pub const HEADERS: Frame = Frame::from_u32(0x01);
    /// SETTINGS frame type (`0x04`).
    pub const SETTINGS: Frame = Frame::from_u32(0x04);
    /// WEBTRANSPORT stream frame type (`0x41`).
    pub const WEBTRANSPORT: Frame = Frame::from_u32(0x41);
}
