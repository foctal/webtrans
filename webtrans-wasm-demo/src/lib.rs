//! WebTransport WASM demo entry point for the browser UI.

use bytes::Bytes;
use std::cell::RefCell;
use url::Url;
use wasm_bindgen::JsCast;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::spawn_local;
use web_sys::{Document, HtmlButtonElement, HtmlInputElement};

use webtrans_wasm::{ClientBuilder, Session};

thread_local! {
    static STATE: RefCell<Option<AppState>> = RefCell::new(None);
}

struct AppState {
    session: Session,
}

fn document() -> Document {
    web_sys::window().unwrap().document().unwrap()
}

fn by_id<T: JsCast>(id: &str) -> T {
    document()
        .get_element_by_id(id)
        .unwrap()
        .dyn_into()
        .unwrap()
}

fn log(msg: &str) {
    let doc = document();
    let el = doc.get_element_by_id("log").unwrap();
    let mut text = el.text_content().unwrap_or_default();
    text.push_str(msg);
    text.push('\n');
    el.set_text_content(Some(&text));
    web_sys::console::log_1(&msg.into());
}

fn set_session(session: Session) {
    STATE.with(|s| *s.borrow_mut() = Some(AppState { session }));
}

fn get_session() -> Result<Session, JsValue> {
    STATE.with(|s| {
        s.borrow()
            .as_ref()
            .map(|st| st.session.clone())
            .ok_or_else(|| JsValue::from_str("not connected"))
    })
}

fn parse_hex_bytes(input: &str) -> Result<Vec<u8>, JsValue> {
    let s: String = input.chars().filter(|c| c.is_ascii_hexdigit()).collect();

    if s.len() % 2 != 0 {
        return Err(JsValue::from_str("hex length must be even"));
    }

    let mut out = Vec::with_capacity(s.len() / 2);
    let bytes = s.as_bytes();

    for i in (0..bytes.len()).step_by(2) {
        let hi = (bytes[i] as char).to_digit(16).unwrap();
        let lo = (bytes[i + 1] as char).to_digit(16).unwrap();
        out.push(((hi << 4) | lo) as u8);
    }

    Ok(out)
}

#[wasm_bindgen(start)]
pub fn start() {
    console_error_panic_hook::set_once();

    let url_input: HtmlInputElement = by_id("url");
    let hash_input: HtmlInputElement = by_id("cert-hash");

    let btn_connect_system: HtmlButtonElement = by_id("btn-connect-system");
    let btn_connect_pin: HtmlButtonElement = by_id("btn-connect-pin");
    let btn_close: HtmlButtonElement = by_id("btn-close");
    let btn_bi: HtmlButtonElement = by_id("btn-bistream");
    let btn_dg: HtmlButtonElement = by_id("btn-dgram");

    // Wire up the "connect (system roots)" action.
    {
        let url_input = url_input.clone();
        let cb = Closure::<dyn FnMut()>::new(move || {
            let url = url_input.value();
            spawn_local(async move {
                match connect_system(&url).await {
                    Ok(()) => log(&format!("[connect system] ok: {url}")),
                    Err(e) => log(&format!("[connect system] err: {e:?}")),
                }
            });
        });
        btn_connect_system.set_onclick(Some(cb.as_ref().unchecked_ref()));
        cb.forget();
    }

    // Wire up the "connect (pinned cert)" action.
    {
        let url_input = url_input.clone();
        let hash_input = hash_input.clone();
        let cb = Closure::<dyn FnMut()>::new(move || {
            let url = url_input.value();
            let hash = hash_input.value();
            spawn_local(async move {
                match connect_pin(&url, &hash).await {
                    Ok(()) => log(&format!("[connect pin] ok: {url}")),
                    Err(e) => log(&format!("[connect pin] err: {e:?}")),
                }
            });
        });
        btn_connect_pin.set_onclick(Some(cb.as_ref().unchecked_ref()));
        cb.forget();
    }

    // Wire up the session close action.
    {
        let cb = Closure::<dyn FnMut()>::new(move || {
            spawn_local(async move {
                close();
                log("[close] requested");
            });
        });
        btn_close.set_onclick(Some(cb.as_ref().unchecked_ref()));
        cb.forget();
    }

    // Wire up the bidirectional echo action.
    {
        let cb = Closure::<dyn FnMut()>::new(move || {
            spawn_local(async move {
                match bi_echo().await {
                    Ok(()) => log("[bi] ok"),
                    Err(e) => log(&format!("[bi] err: {e:?}")),
                }
            });
        });
        btn_bi.set_onclick(Some(cb.as_ref().unchecked_ref()));
        cb.forget();
    }

    // Wire up the datagram echo action.
    {
        let cb = Closure::<dyn FnMut()>::new(move || {
            spawn_local(async move {
                match datagram_echo().await {
                    Ok(()) => log("[dgram] ok"),
                    Err(e) => log(&format!("[dgram] err: {e:?}")),
                }
            });
        });
        btn_dg.set_onclick(Some(cb.as_ref().unchecked_ref()));
        cb.forget();
    }

    log("ready");
}

async fn connect_system(url: &str) -> Result<(), JsValue> {
    let url = Url::parse(url).map_err(|e| JsValue::from_str(&format!("invalid url: {e}")))?;

    let client = ClientBuilder::new().with_system_roots();

    let session = client
        .connect(url)
        .await
        .map_err(|e| JsValue::from_str(&format!("{e}")))?;

    set_session(session);
    Ok(())
}

async fn connect_pin(url: &str, sha256_hex: &str) -> Result<(), JsValue> {
    let url = Url::parse(url).map_err(|e| JsValue::from_str(&format!("invalid url: {e}")))?;

    let fp = parse_hex_bytes(sha256_hex)?;
    if fp.len() != 32 {
        return Err(JsValue::from_str(
            "sha256 fingerprint must be 32 bytes (64 hex chars)",
        ));
    }

    let client = ClientBuilder::new().with_server_certificate_hashes(vec![fp]);

    let session = client
        .connect(url)
        .await
        .map_err(|e| JsValue::from_str(&format!("{e}")))?;

    set_session(session);
    Ok(())
}

fn close() {
    STATE.with(|s| {
        if let Some(st) = s.borrow().as_ref() {
            st.session.close(0, "bye");
        }
        *s.borrow_mut() = None;
    });
}

async fn bi_echo() -> Result<(), JsValue> {
    let session = get_session()?;

    let (mut send, mut recv) = session
        .open_bi()
        .await
        .map_err(|e| JsValue::from_str(&format!("{e}")))?;

    let msg = b"hello world";
    send.write(&Bytes::from_static(msg))
        .await
        .map_err(|e| JsValue::from_str(&format!("{e}")))?;
    send.finish()
        .map_err(|e| JsValue::from_str(&format!("{e}")))?;

    match recv.read(1024).await {
        Ok(data_opt) => {
            let data = data_opt.ok_or_else(|| JsValue::from_str("stream closed"))?;
            let text = String::from_utf8_lossy(&data);
            log(&format!("[bi] recv: {text}"));
        }
        Err(e) => return Err(JsValue::from_str(&format!("{e}"))),
    };

    Ok(())
}

async fn datagram_echo() -> Result<(), JsValue> {
    let session = get_session()?;

    let msg = Bytes::from_static(b"hello datagram");
    session
        .send_datagram(msg.clone())
        .await
        .map_err(|e| JsValue::from_str(&format!("{e}")))?;

    let data = session
        .recv_datagram()
        .await
        .map_err(|e| JsValue::from_str(&format!("{e}")))?;

    let text = String::from_utf8_lossy(&data);
    log(&format!("[dgram] recv: {text}"));
    Ok(())
}
