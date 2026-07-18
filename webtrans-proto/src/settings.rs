//! HTTP/3 SETTINGS frame helpers for WebTransport.

use std::{
    collections::HashMap,
    fmt::Debug,
    ops::{Deref, DerefMut},
    sync::Arc,
};

use bytes::{Buf, BufMut, BytesMut};

use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use super::{Frame, UniStream, VarInt, VarIntUnexpectedEnd};
use crate::grease::is_grease_value;
const MAX_SETTINGS_FRAME_SIZE: usize = 16 * 1024;

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
/// HTTP/3 SETTINGS identifier.
pub struct Setting(pub VarInt);

impl Setting {
    /// Decode a settings identifier.
    pub fn decode<B: Buf>(buf: &mut B) -> Result<Self, VarIntUnexpectedEnd> {
        Ok(Setting(VarInt::decode(buf)?))
    }

    /// Encode a settings identifier.
    pub fn encode<B: BufMut>(&self, buf: &mut B) {
        self.0.encode(buf)
    }

    /// Return the encoded size of this identifier.
    pub fn size(&self) -> usize {
        self.0.size()
    }

    // Reference: https://datatracker.ietf.org/doc/html/rfc9114#section-7.2.4.1
    /// Return `true` when the setting uses RFC 9114 GREASE spacing.
    pub fn is_grease(&self) -> bool {
        is_grease_value(self.0.into_inner())
    }
}

impl Debug for Setting {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {
            Setting::QPACK_MAX_TABLE_CAPACITY => write!(f, "QPACK_MAX_TABLE_CAPACITY"),
            Setting::MAX_FIELD_SECTION_SIZE => write!(f, "MAX_FIELD_SECTION_SIZE"),
            Setting::QPACK_BLOCKED_STREAMS => write!(f, "QPACK_BLOCKED_STREAMS"),
            Setting::ENABLE_CONNECT_PROTOCOL => write!(f, "ENABLE_CONNECT_PROTOCOL"),
            Setting::ENABLE_DATAGRAM => write!(f, "ENABLE_DATAGRAM"),
            Setting::ENABLE_DATAGRAM_DEPRECATED => write!(f, "ENABLE_DATAGRAM_DEPRECATED"),
            Setting::WEBTRANSPORT_ENABLE_DEPRECATED => write!(f, "WEBTRANSPORT_ENABLE_DEPRECATED"),
            Setting::WEBTRANSPORT_MAX_SESSIONS_DEPRECATED => {
                write!(f, "WEBTRANSPORT_MAX_SESSIONS_DEPRECATED")
            }
            Setting::WEBTRANSPORT_MAX_SESSIONS => write!(f, "WEBTRANSPORT_MAX_SESSIONS"),
            Setting::WT_ENABLED => write!(f, "WT_ENABLED"),
            x if x.is_grease() => write!(f, "GREASE SETTING [{:x?}]", x.0.into_inner()),
            x => write!(f, "UNKNOWN_SETTING [{:x?}]", x.0.into_inner()),
        }
    }
}

impl Setting {
    /// Build a settings identifier from a known `u32` value.
    pub const fn from_u32(value: u32) -> Self {
        Self(VarInt::from_u32(value))
    }

    // HTTP/3 settings that WebTransport ignores.
    /// HTTP/3 QPACK dynamic table capacity setting.
    pub const QPACK_MAX_TABLE_CAPACITY: Setting = Setting::from_u32(0x1); // Default is 0, which disables the dynamic table.
    /// HTTP/3 maximum header field section size setting.
    pub const MAX_FIELD_SECTION_SIZE: Setting = Setting::from_u32(0x6);
    /// HTTP/3 QPACK blocked streams setting.
    pub const QPACK_BLOCKED_STREAMS: Setting = Setting::from_u32(0x7);

    // Both values are required for WebTransport.
    /// HTTP/3 extended CONNECT enable flag.
    pub const ENABLE_CONNECT_PROTOCOL: Setting = Setting::from_u32(0x8);
    /// HTTP/3 datagram support flag (current).
    pub const ENABLE_DATAGRAM: Setting = Setting::from_u32(0x33);
    /// HTTP/3 datagram support flag (deprecated draft value).
    pub const ENABLE_DATAGRAM_DEPRECATED: Setting = Setting::from_u32(0xFFD277); // Still used by some Chrome versions.

    // Removed in draft-06.
    /// Draft WebTransport enable flag (deprecated).
    pub const WEBTRANSPORT_ENABLE_DEPRECATED: Setting = Setting::from_u32(0x2b603742);
    /// Draft maximum WebTransport sessions setting (deprecated).
    pub const WEBTRANSPORT_MAX_SESSIONS_DEPRECATED: Setting = Setting::from_u32(0x2b603743);

    // Current way to enable WebTransport.
    /// Current WebTransport maximum sessions setting.
    pub const WEBTRANSPORT_MAX_SESSIONS: Setting = Setting::from_u32(0xc671706a);
    /// WebTransport over HTTP/3 draft-16 enable flag.
    pub const WT_ENABLED: Setting = Setting::from_u32(0x2c7cf000);
}

#[derive(Error, Debug, Clone)]
/// Errors returned while encoding or decoding SETTINGS exchanges.
pub enum SettingsError {
    /// Input ended before a full SETTINGS payload was available.
    #[error("unexpected end of input")]
    UnexpectedEnd,

    /// First unidirectional stream type was not the expected control stream.
    #[error("unexpected stream type {0:?}")]
    UnexpectedStreamType(UniStream),

    /// Frame type was not SETTINGS.
    #[error("unexpected frame {0:?}")]
    UnexpectedFrame(Frame),

    /// Invalid varint or truncated settings pair inside a frame payload.
    #[error("invalid size")]
    InvalidSize,

    /// A SETTINGS identifier occurred more than once in the same frame.
    #[error("duplicate setting {0:?}")]
    DuplicateSetting(Setting),

    /// A boolean SETTINGS value was neither zero nor one.
    #[error("invalid value {1} for setting {0:?}")]
    InvalidValue(Setting, VarInt),

    /// SETTINGS frame exceeded the implementation's bounded decode limit.
    #[error("SETTINGS frame exceeds the 16 KiB implementation limit")]
    MessageTooLong,

    /// I/O error while reading or writing the SETTINGS exchange.
    #[error("io error: {0}")]
    Io(Arc<std::io::Error>),
}

impl From<std::io::Error> for SettingsError {
    fn from(err: std::io::Error) -> Self {
        SettingsError::Io(Arc::new(err))
    }
}

// A map of SETTINGS identifiers to values.
#[derive(Default, Debug)]
/// Parsed HTTP/3 settings map keyed by [`Setting`].
pub struct Settings(HashMap<Setting, VarInt>);

impl Settings {
    /// Decode a control stream prefix and SETTINGS frame from an in-memory buffer.
    pub fn decode<B: Buf>(buf: &mut B) -> Result<Self, SettingsError> {
        let typ = UniStream::decode(buf).map_err(|_| SettingsError::UnexpectedEnd)?;
        if typ != UniStream::CONTROL {
            return Err(SettingsError::UnexpectedStreamType(typ));
        }

        let (typ, mut data) = Frame::read(buf).map_err(|_| SettingsError::UnexpectedEnd)?;
        if typ != Frame::SETTINGS {
            return Err(SettingsError::UnexpectedFrame(typ));
        }

        let mut settings = Settings::default();
        while data.has_remaining() {
            // Use InvalidSize because retrying will not help.
            let id = Setting::decode(&mut data).map_err(|_| SettingsError::InvalidSize)?;
            let value = VarInt::decode(&mut data).map_err(|_| SettingsError::InvalidSize)?;
            // Only retain non-GREASE entries.
            if !id.is_grease() {
                if settings.0.contains_key(&id) {
                    return Err(SettingsError::DuplicateSetting(id));
                }
                if matches!(
                    id,
                    Setting::ENABLE_CONNECT_PROTOCOL
                        | Setting::ENABLE_DATAGRAM
                        | Setting::ENABLE_DATAGRAM_DEPRECATED
                        | Setting::WEBTRANSPORT_ENABLE_DEPRECATED
                        | Setting::WT_ENABLED
                ) && value.into_inner() > 1
                {
                    return Err(SettingsError::InvalidValue(id, value));
                }
                settings.0.insert(id, value);
            }
        }

        Ok(settings)
    }

    /// Read and decode one SETTINGS exchange from an async stream.
    pub async fn read<S: AsyncRead + Unpin>(stream: &mut S) -> Result<Self, SettingsError> {
        let stream_type = VarInt::read(stream)
            .await
            .map_err(|_| SettingsError::UnexpectedEnd)?;
        let stream_type = UniStream(stream_type);
        if stream_type != UniStream::CONTROL {
            return Err(SettingsError::UnexpectedStreamType(stream_type));
        }

        loop {
            let typ = VarInt::read(stream)
                .await
                .map_err(|_| SettingsError::UnexpectedEnd)?;
            let length = VarInt::read(stream)
                .await
                .map_err(|_| SettingsError::UnexpectedEnd)?;
            let length =
                usize::try_from(length.into_inner()).map_err(|_| SettingsError::MessageTooLong)?;
            if length > MAX_SETTINGS_FRAME_SIZE {
                return Err(SettingsError::MessageTooLong);
            }

            let mut payload = vec![0; length];
            stream.read_exact(&mut payload).await?;
            let typ = Frame(typ);
            if typ.is_grease() {
                continue;
            }

            let mut frame =
                Vec::with_capacity(stream_type.0.size() + typ.0.size() + VarInt::MAX_SIZE + length);
            stream_type.encode(&mut frame);
            typ.encode(&mut frame);
            VarInt::try_from(length)
                .map_err(|_| SettingsError::MessageTooLong)?
                .encode(&mut frame);
            frame.extend_from_slice(&payload);
            return Self::decode(&mut frame.as_slice());
        }
    }

    /// Encode this settings map as a control stream prefix followed by a SETTINGS frame.
    pub fn encode<B: BufMut>(&self, buf: &mut B) {
        UniStream::CONTROL.encode(buf);
        Frame::SETTINGS.encode(buf);

        let payload_len = self.payload_len();
        VarInt::try_from(payload_len as u64)
            .expect("settings payload length exceeds VarInt bounds")
            .encode(buf);

        for (id, value) in &self.0 {
            id.encode(buf);
            value.encode(buf);
        }
    }

    /// Encode and write this settings map to an async stream.
    pub async fn write<S: AsyncWrite + Unpin>(&self, stream: &mut S) -> Result<(), SettingsError> {
        let mut buf = BytesMut::with_capacity(self.encoded_len());
        self.encode(&mut buf);
        stream.write_all_buf(&mut buf).await?;
        Ok(())
    }

    /// Enable WebTransport settings, including deprecated parameters for compatibility.
    pub fn enable_webtransport(&mut self, max_sessions: u32) {
        self.enable_webtransport_internal(max_sessions, true);
    }

    /// Enable WebTransport settings without deprecated draft parameters.
    pub fn enable_webtransport_latest(&mut self, max_sessions: u32) {
        self.enable_webtransport_internal(max_sessions, false);
    }

    fn enable_webtransport_internal(&mut self, max_sessions: u32, include_deprecated: bool) {
        let max = VarInt::from_u32(max_sessions);

        self.insert(Setting::ENABLE_CONNECT_PROTOCOL, VarInt::from_u32(1));
        self.insert(Setting::ENABLE_DATAGRAM, VarInt::from_u32(1));
        self.insert(Setting::ENABLE_DATAGRAM_DEPRECATED, VarInt::from_u32(1));
        self.insert(Setting::WT_ENABLED, VarInt::from_u32(1));
        self.insert(Setting::WEBTRANSPORT_MAX_SESSIONS, max);

        if include_deprecated {
            self.insert(Setting::WEBTRANSPORT_MAX_SESSIONS_DEPRECATED, max);
            self.insert(Setting::WEBTRANSPORT_ENABLE_DEPRECATED, VarInt::from_u32(1));
        } else {
            self.0
                .remove(&Setting::WEBTRANSPORT_MAX_SESSIONS_DEPRECATED);
            self.0.remove(&Setting::WEBTRANSPORT_ENABLE_DEPRECATED);
        }
    }

    // Return the maximum number of sessions supported.
    /// Return the peer-advertised maximum number of WebTransport sessions, or `0` when unsupported.
    pub fn supports_webtransport(&self) -> u64 {
        let enabled = self
            .get(&Setting::WT_ENABLED)
            .map(|value| value.into_inner());
        if enabled == Some(1)
            && self.get(&Setting::ENABLE_DATAGRAM).map(|v| v.into_inner()) == Some(1)
        {
            return 1;
        }

        // Observed from Chrome 114.0.5735.198 (July 19, 2023).
        // Setting(1): 65536,              // qpack_max_table_capacity
        // Setting(6): 16384,              // max_field_section_size
        // Setting(7): 100,                // qpack_blocked_streams
        // Setting(51): 1,                 // enable_datagram
        // Setting(16765559): 1            // enable_datagram_deprecated
        // Setting(727725890): 1,          // webtransport_max_sessions_deprecated
        // Setting(4445614305): 454654587, // grease

        // NOTE: The presence of ENABLE_WEBTRANSPORT implies ENABLE_CONNECT is supported.

        let datagram = self
            .get(&Setting::ENABLE_DATAGRAM)
            .or(self.get(&Setting::ENABLE_DATAGRAM_DEPRECATED))
            .map(|v| v.into_inner());

        if datagram != Some(1) {
            return 0;
        }

        // Before draft-07, enabling WebTransport used two parameters: ENABLE=1 and MAX_SESSIONS=N.
        // The modern approach uses MAX_SESSIONS alone, where non-zero means enabled.

        if let Some(max) = self.get(&Setting::WEBTRANSPORT_MAX_SESSIONS) {
            return max.into_inner();
        }

        let enabled = self
            .get(&Setting::WEBTRANSPORT_ENABLE_DEPRECATED)
            .map(|v| v.into_inner());
        if enabled != Some(1) {
            return 0;
        }

        // Only the server may set this value; default to 1 if absent.
        self.get(&Setting::WEBTRANSPORT_MAX_SESSIONS_DEPRECATED)
            .map(|v| v.into_inner())
            .unwrap_or(1)
    }

    /// Return whether settings received from a server satisfy current
    /// WebTransport negotiation requirements.
    pub fn supports_webtransport_server(&self) -> bool {
        (self.get(&Setting::WT_ENABLED).map(|v| v.into_inner()) == Some(1)
            && self
                .get(&Setting::ENABLE_CONNECT_PROTOCOL)
                .map(|v| v.into_inner())
                == Some(1)
            && self.get(&Setting::ENABLE_DATAGRAM).map(|v| v.into_inner()) == Some(1))
            || (self.supports_webtransport() > 0 && self.get(&Setting::WT_ENABLED).is_none())
    }

    /// Return whether settings received from a client satisfy current
    /// WebTransport draft negotiation requirements.
    pub fn supports_webtransport_client(&self) -> bool {
        (self.get(&Setting::WT_ENABLED).map(|v| v.into_inner()) == Some(1)
            && self.get(&Setting::ENABLE_DATAGRAM).map(|v| v.into_inner()) == Some(1))
            || (self.supports_webtransport() > 0 && self.get(&Setting::WT_ENABLED).is_none())
    }

    fn payload_len(&self) -> usize {
        self.0
            .iter()
            .map(|(id, value)| id.size() + value.size())
            .sum()
    }

    fn encoded_len(&self) -> usize {
        let payload_len = self.payload_len();
        UniStream::CONTROL.0.size()
            + Frame::SETTINGS.0.size()
            + VarInt::try_from(payload_len as u64)
                .expect("settings payload length exceeds VarInt bounds")
                .size()
            + payload_len
    }
}

impl Deref for Settings {
    type Target = HashMap<Setting, VarInt>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Settings {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn latest_settings_enable_webtransport() {
        let mut settings = Settings::default();
        settings.enable_webtransport_latest(1);

        assert_eq!(settings.supports_webtransport(), 1);
        assert_eq!(
            settings.get(&Setting::WT_ENABLED),
            Some(&VarInt::from_u32(1))
        );
    }

    #[test]
    fn rejects_duplicate_settings() {
        let mut data = Vec::new();
        UniStream::CONTROL.encode(&mut data);
        Frame::SETTINGS.encode(&mut data);
        VarInt::from_u32(4).encode(&mut data);
        Setting::ENABLE_DATAGRAM.encode(&mut data);
        VarInt::from_u32(1).encode(&mut data);
        Setting::ENABLE_DATAGRAM.encode(&mut data);
        VarInt::from_u32(1).encode(&mut data);

        let error = Settings::decode(&mut data.as_slice()).unwrap_err();
        assert!(matches!(
            error,
            SettingsError::DuplicateSetting(Setting::ENABLE_DATAGRAM)
        ));
    }

    #[test]
    fn rejects_non_boolean_enabled_value() {
        let mut data = Vec::new();
        UniStream::CONTROL.encode(&mut data);
        Frame::SETTINGS.encode(&mut data);
        VarInt::from_u32(5).encode(&mut data);
        Setting::WT_ENABLED.encode(&mut data);
        VarInt::from_u32(2).encode(&mut data);

        let error = Settings::decode(&mut data.as_slice()).unwrap_err();
        assert!(matches!(
            error,
            SettingsError::InvalidValue(Setting::WT_ENABLED, _)
        ));
    }

    #[test]
    fn server_settings_require_extended_connect() {
        let mut settings = Settings::default();
        settings.insert(Setting::WT_ENABLED, VarInt::from_u32(1));
        settings.insert(Setting::ENABLE_DATAGRAM, VarInt::from_u32(1));

        assert!(settings.supports_webtransport_client());
        assert!(!settings.supports_webtransport_server());

        settings.insert(Setting::ENABLE_CONNECT_PROTOCOL, VarInt::from_u32(1));
        assert!(settings.supports_webtransport_server());
    }
}
