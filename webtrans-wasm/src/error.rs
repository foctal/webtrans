use wasm_bindgen::prelude::*;

/// A WebTransport error classified by source.
#[derive(Clone, Debug, thiserror::Error)]
pub enum Error {
    #[error("webtransport session closed: code={code} reason={reason}")]
    /// Session closed cleanly with application-provided details.
    SessionClosed {
        /// Application close code.
        code: u32,
        /// Application close reason.
        reason: String,
    },

    #[error("webtransport session error: {0:?}")]
    Session(web_sys::WebTransportError),

    #[error("webtransport stream error: {0:?}")]
    Stream(web_sys::WebTransportError),

    #[error("web streams error: {0:?}")]
    Streams(#[from] web_streams::Error),

    #[error("unknown error: {0:?}")]
    Unknown(JsValue),
}

impl Error {
    /// Return the error code used when closing the stream or session.
    pub fn code(&self) -> Option<u8> {
        match self {
            Error::SessionClosed { .. } => None,
            Error::Session(e) | Error::Stream(e) => e.stream_error_code(),
            _ => None,
        }
    }
}

impl From<JsValue> for Error {
    /// Convert a generic `JsValue` into a `WebTransportError` when possible, otherwise `Error::Unknown`.
    fn from(v: JsValue) -> Self {
        if let Some(e) = v.dyn_ref::<web_sys::WebTransportError>().cloned() {
            match e.source() {
                web_sys::WebTransportErrorSource::Stream => Error::Stream(e),
                web_sys::WebTransportErrorSource::Session => Error::Session(e),
                _ => Error::Unknown(v),
            }
        } else {
            Error::Unknown(v)
        }
    }
}

#[cfg(target_family = "wasm")]
impl webtrans_trait::Error for Error {
    fn session_error(&self) -> Option<(u32, String)> {
        match self {
            Error::SessionClosed { code, reason } => Some((*code, reason.clone())),
            Error::Session(err) => err
                .stream_error_code()
                .map(|code| (u32::from(code), format!("{err:?}"))),
            _ => None,
        }
    }

    fn stream_error(&self) -> Option<u32> {
        match self {
            Error::Stream(err) => err.stream_error_code().map(u32::from),
            _ => None,
        }
    }
}
