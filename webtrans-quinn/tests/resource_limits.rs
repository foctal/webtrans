use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use url::Url;
use webtrans_quinn::quinn::{self, VarInt};
use webtrans_quinn::{
    ClientBuilder, ClientError, ServerBuilder, Session, tls::generate_self_signed_pair_der,
};

const SHORT_WAIT: Duration = Duration::from_millis(150);
const TEST_TIMEOUT: Duration = Duration::from_secs(5);

async fn connect_pair(
    server_transport: quinn::TransportConfig,
) -> Result<(Session, Session), Box<dyn std::error::Error>> {
    let (chain, key) = generate_self_signed_pair_der(vec!["localhost".to_string()])?;
    let client_chain = chain.clone();
    let mut server = ServerBuilder::new()
        .with_addr("127.0.0.1:0".parse()?)
        .with_transport_config(server_transport)
        .with_handshake_timeout(TEST_TIMEOUT)
        .with_certificate(chain, key)?;
    let addr = server.local_addr()?;
    let client = ClientBuilder::new()
        .with_handshake_timeout(TEST_TIMEOUT)
        .with_server_certificates(client_chain)?;
    let url = Url::parse(&format!("https://127.0.0.1:{}/limits", addr.port()))?;

    let server_session = async {
        server
            .accept()
            .await
            .expect("the test endpoint remains open")?
            .ok()
            .await
    };
    let (client_session, server_session) = tokio::join!(client.connect(url), server_session);
    let client_session = client_session?;
    let server_session = server_session?;
    Ok((client_session, server_session))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_stream_limit_blocks_until_credit_is_released() {
    let mut transport = quinn::TransportConfig::default();
    // The CONNECT stream permanently consumes one peer-initiated
    // bidirectional stream slot, leaving one application stream slot.
    transport.max_concurrent_bidi_streams(VarInt::from_u32(2));
    let (client, server) = connect_pair(transport).await.unwrap();

    let (mut client_send, mut client_recv) = client.open_bi().await.unwrap();
    let (mut server_send, mut server_recv) = server.accept_bi().await.unwrap();

    assert!(
        tokio::time::timeout(SHORT_WAIT, client.open_bi())
            .await
            .is_err(),
        "a second stream opened while the one-stream limit was saturated"
    );

    client_send.finish().unwrap();
    client_recv.stop(0).unwrap();
    server_send.finish().unwrap();
    server_recv.stop(0).unwrap();

    tokio::time::timeout(TEST_TIMEOUT, client.open_bi())
        .await
        .expect("stream credit was not returned")
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn receive_windows_apply_backpressure_to_stream_writes() {
    let mut transport = quinn::TransportConfig::default();
    transport
        .stream_receive_window(VarInt::from_u32(1024))
        // Leave enough connection-level credit for the HTTP/3 control and
        // CONNECT streams while still bounding application buffering.
        .receive_window(VarInt::from_u32(16 * 1024));
    let (client, server) = connect_pair(transport).await.unwrap();

    let (mut client_send, _client_recv) = client.open_bi().await.unwrap();
    let (_server_send, mut server_recv) = server.accept_bi().await.unwrap();
    let payload = vec![0x5a; 8 * 1024 * 1024];

    assert!(
        tokio::time::timeout(SHORT_WAIT, client_send.write_all(&payload))
            .await
            .is_err(),
        "the complete payload was buffered despite the bounded receive windows"
    );
    client_send.finish().unwrap();

    let received = tokio::time::timeout(TEST_TIMEOUT, server_recv.read_to_end(8 * 1024 * 1024))
        .await
        .expect("partially written data was not readable")
        .unwrap();
    assert!(!received.is_empty());
    assert!(received.len() < payload.len());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn disabled_datagram_buffer_is_advertised_as_no_receive_support() {
    let mut transport = quinn::TransportConfig::default();
    transport.datagram_receive_buffer_size(None);
    let (client, _server) = connect_pair(transport).await.unwrap();

    assert_eq!(client.max_datagram_size(), 0);
    assert!(
        client
            .send_datagram(Bytes::from_static(b"blocked"))
            .is_err()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pending_handshake_limit_stops_connection_admission() {
    let (chain, key) = generate_self_signed_pair_der(vec!["localhost".to_string()]).unwrap();
    let client_chain = chain.clone();
    let mut server = ServerBuilder::new()
        .with_addr("127.0.0.1:0".parse().unwrap())
        .with_max_pending_handshakes(NonZeroUsize::new(1).unwrap())
        .with_certificate(chain, key)
        .unwrap();
    let addr = server.local_addr().unwrap();

    let accept_task = tokio::spawn(async move { server.accept().await });
    let stalled = make_raw_client(client_chain.clone());
    let stalled_connection = stalled.connect(addr, "localhost").unwrap().await.unwrap();

    tokio::time::sleep(SHORT_WAIT).await;

    let client = ClientBuilder::new()
        .with_handshake_timeout(SHORT_WAIT)
        .with_server_certificates(client_chain)
        .unwrap();
    let url = Url::parse(&format!("https://127.0.0.1:{}/admission", addr.port())).unwrap();
    assert!(matches!(
        client.connect(url).await,
        Err(ClientError::HandshakeTimeout)
    ));

    stalled_connection.close(VarInt::from_u32(0), b"test complete");
    accept_task.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropping_request_sends_an_observable_http_rejection() {
    let (chain, key) = generate_self_signed_pair_der(vec!["localhost".to_string()]).unwrap();
    let client_chain = chain.clone();
    let mut server = ServerBuilder::new()
        .with_addr("127.0.0.1:0".parse().unwrap())
        .with_certificate(chain, key)
        .unwrap();
    let addr = server.local_addr().unwrap();
    let client = ClientBuilder::new()
        .with_server_certificates(client_chain)
        .unwrap();
    let url = Url::parse(&format!("https://127.0.0.1:{}/drop", addr.port())).unwrap();

    let server_side = async {
        let request = server.accept().await.unwrap().unwrap();
        drop(request);
        tokio::time::sleep(SHORT_WAIT).await;
    };
    let (result, ()) = tokio::join!(client.connect(url), server_side);
    assert!(
        matches!(
            &result,
            Err(ClientError::HttpError(
                webtrans_quinn::ConnectError::ErrorStatus(
                    webtrans_quinn::http::StatusCode::INTERNAL_SERVER_ERROR
                )
            ))
        ),
        "unexpected dropped-request result: {result:?}"
    );
}

fn make_raw_client(
    certs: Vec<webtrans_quinn::rustls::pki_types::CertificateDer<'static>>,
) -> quinn::Endpoint {
    let mut roots = webtrans_quinn::rustls::RootCertStore::empty();
    for cert in certs {
        roots.add(cert).unwrap();
    }
    let mut tls = webtrans_quinn::rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    tls.alpn_protocols = vec![webtrans_quinn::ALPN.as_bytes().to_vec()];
    let crypto = quinn::crypto::rustls::QuicClientConfig::try_from(tls).unwrap();
    let mut config = quinn::ClientConfig::new(Arc::new(crypto));
    config.transport_config(Arc::new(quinn::TransportConfig::default()));

    let mut endpoint = quinn::Endpoint::client("0.0.0.0:0".parse().unwrap()).unwrap();
    endpoint.set_default_client_config(config);
    endpoint
}
