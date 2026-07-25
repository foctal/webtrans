use js_sys::Uint8Array;
use url::Url;
use wasm_bindgen_futures::JsFuture;
use web_sys::{WebTransport, WebTransportHash, WebTransportOptions};

use crate::{Error, Session};

pub use web_sys::WebTransportCongestionControl as CongestionControl;

/// Builder wrapper for [`WebTransportOptions`].
#[derive(Debug, Default)]
pub struct ClientBuilder {
    options: WebTransportOptions,
}

impl ClientBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Control whether the client/server is allowed to pool connections.
    /// In practice, keep this disabled unless you have a specific need.
    pub fn with_pooling(self, val: bool) -> Self {
        self.options.set_allow_pooling(val);
        self
    }

    /// `true` if QUIC is required, `false` if TCP is an acceptable fallback.
    pub fn with_unreliable(self, val: bool) -> Self {
        self.options.set_require_unreliable(val);
        self
    }

    /// Hint at the desired congestion control algorithm.
    pub fn with_congestion_control(self, control: CongestionControl) -> Self {
        self.options.set_congestion_control(control);
        self
    }

    /// Supply SHA-256 hashes for accepted certificates instead of using a root CA.
    pub fn with_server_certificate_hashes(self, hashes: Vec<Vec<u8>>) -> Client {
        // Expected format: [{ algorithm: "sha-256", value: hashValue }, ...].
        let hashes = hashes
            .into_iter()
            .map(|hash| {
                let hash_obj = WebTransportHash::new();
                hash_obj.set_algorithm("sha-256");
                hash_obj.set_value_u8_array(&Uint8Array::from(&hash[..]));
                hash_obj
            })
            .collect::<Vec<WebTransportHash>>();

        self.options.set_server_certificate_hashes(&hashes);
        Client {
            options: self.options,
        }
    }

    pub fn with_system_roots(self) -> Client {
        Client {
            options: self.options,
        }
    }
}

/// Build a client with the given URL and options.
///
/// See [`WebTransportOptions`].
#[derive(Clone, Debug, Default)]
pub struct Client {
    options: WebTransportOptions,
}

impl Client {
    /// Connect once the builder is configured.
    pub async fn connect(&self, url: Url) -> Result<Session, Error> {
        let inner = WebTransport::new_with_options(url.as_str(), &self.options)?;
        JsFuture::from(inner.ready()).await?;

        Session::new(inner, url)
    }
}
