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
    make_raw_client_with_transport(certs, quinn::TransportConfig::default())
}

fn make_raw_client_with_transport(
    certs: Vec<webtrans_quinn::rustls::pki_types::CertificateDer<'static>>,
    transport: quinn::TransportConfig,
) -> quinn::Endpoint {
    let mut roots = webtrans_quinn::rustls::RootCertStore::empty();
    for cert in certs {
        roots.add(cert).unwrap();
    }
    let mut tls = webtrans_quinn::rustls::ClientConfig::builder_with_provider(std::sync::Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .expect("protocol versions")
    .with_root_certificates(roots)
    .with_no_client_auth();
    tls.alpn_protocols = vec![webtrans_quinn::ALPN.as_bytes().to_vec()];
    let crypto = quinn::crypto::rustls::QuicClientConfig::try_from(tls).unwrap();
    let mut config = quinn::ClientConfig::new(Arc::new(crypto));
    config.transport_config(Arc::new(transport));

    let mut endpoint = quinn::Endpoint::client("0.0.0.0:0".parse().unwrap()).unwrap();
    endpoint.set_default_client_config(config);
    endpoint
}

#[tokio::test]
async fn dropping_last_session_closes_the_peer_without_waiting_for_idle_timeout() {
    let (client, server) = connect_pair(quinn::TransportConfig::default())
        .await
        .unwrap();
    drop(client);
    tokio::time::timeout(TEST_TIMEOUT, server.closed())
        .await
        .expect("dropping the final session must release the CONNECT task");
}

#[tokio::test]
async fn repeated_close_requests_complete_with_bounded_queueing() {
    let (client, server) = connect_pair(quinn::TransportConfig::default())
        .await
        .unwrap();
    let reason = vec![b'x'; 16 * 1024];
    // No await: a current-thread runtime cannot drain the close queue here.
    for _ in 0..1024 {
        client.close(42, &reason);
    }
    tokio::time::timeout(TEST_TIMEOUT, server.closed())
        .await
        .expect("repeated close must still reach the peer");
}

#[tokio::test]
async fn close_deadline_includes_a_credit_blocked_capsule_write() {
    let (chain, key) = generate_self_signed_pair_der(vec!["localhost".to_owned()]).unwrap();
    let mut server = ServerBuilder::new()
        .with_addr("127.0.0.1:0".parse().unwrap())
        .with_certificate(chain.clone(), key)
        .unwrap();
    let addr = server.local_addr().unwrap();
    let mut transport = quinn::TransportConfig::default();
    transport.stream_receive_window(VarInt::from_u32(128));
    let endpoint = make_raw_client_with_transport(chain, transport);
    let server_task =
        tokio::spawn(async move { server.accept().await.unwrap().unwrap().ok().await.unwrap() });
    let conn = endpoint.connect(addr, "localhost").unwrap().await.unwrap();
    let mut control = conn.open_uni().await.unwrap();
    let mut settings = webtrans_proto::Settings::default();
    settings.enable_webtransport(1);
    settings.write(&mut control).await.unwrap();
    let mut peer_control = conn.accept_uni().await.unwrap();
    webtrans_proto::Settings::read(&mut peer_control)
        .await
        .unwrap();
    let (mut send, mut recv) = conn.open_bi().await.unwrap();
    webtrans_proto::ConnectRequest {
        url: Url::parse(&format!("https://localhost:{}/blocked-close", addr.port())).unwrap(),
    }
    .write(&mut send)
    .await
    .unwrap();
    webtrans_proto::ConnectResponse::read(&mut recv)
        .await
        .unwrap();
    let session = server_task.await.unwrap();
    // Keep the CONNECT receive stream alive without reading its close capsule.
    // The capsule exceeds the remaining stream credit and cannot finish writing.
    session.close(42, &[b'x'; 1024]);
    let error = tokio::time::timeout(Duration::from_secs(7), conn.closed())
        .await
        .expect("the close deadline must include capsule submission");
    match error {
        quinn::ConnectionError::ApplicationClosed(close) => assert_eq!(
            close.error_code.into_inner(),
            webtrans_proto::error_to_http3(42)
        ),
        other => panic!("unexpected shutdown: {other:?}"),
    }
}

#[tokio::test]
async fn capsule_stream_survives_grease_fragmentation_and_coalescing() {
    for chunk_size in [usize::MAX, 1, 5] {
        let (chain, key) = generate_self_signed_pair_der(vec!["localhost".to_owned()]).unwrap();
        let mut server = ServerBuilder::new()
            .with_addr("127.0.0.1:0".parse().unwrap())
            .with_certificate(chain.clone(), key)
            .unwrap();
        let addr = server.local_addr().unwrap();
        let endpoint = make_raw_client(chain);
        let server_task =
            tokio::spawn(
                async move { server.accept().await.unwrap().unwrap().ok().await.unwrap() },
            );
        let conn = endpoint.connect(addr, "localhost").unwrap().await.unwrap();
        let mut control = conn.open_uni().await.unwrap();
        let mut settings = webtrans_proto::Settings::default();
        settings.enable_webtransport(1);
        settings.write(&mut control).await.unwrap();
        let mut peer_control = conn.accept_uni().await.unwrap();
        webtrans_proto::Settings::read(&mut peer_control)
            .await
            .unwrap();
        let (mut send, mut recv) = conn.open_bi().await.unwrap();
        webtrans_proto::ConnectRequest {
            url: Url::parse(&format!("https://localhost:{}/capsules", addr.port())).unwrap(),
        }
        .write(&mut send)
        .await
        .unwrap();
        webtrans_proto::ConnectResponse::read(&mut recv)
            .await
            .unwrap();
        let _session = server_task.await.unwrap();
        let mut capsules = Vec::new();
        // 0x21 was incorrectly treated as HTTP/3 GREASE by the capsule decoder.
        for typ in [0x21, 0x17] {
            webtrans_proto::Capsule::Unknown {
                typ: webtrans_proto::VarInt::from_u32(typ),
                payload: Bytes::from_static(&[0xff, 0xff, 0xff]),
            }
            .encode(&mut capsules)
            .unwrap();
        }
        webtrans_proto::Capsule::CloseWebTransportSession {
            code: 42,
            reason: "complete".into(),
        }
        .encode(&mut capsules)
        .unwrap();
        for chunk in capsules.chunks(chunk_size) {
            let mut frame = Vec::new();
            // Empty DATA and HTTP/3 GREASE do not delimit or terminate capsules.
            webtrans_proto::Frame::DATA.encode(&mut frame);
            webtrans_proto::VarInt::from_u32(0).encode(&mut frame);
            webtrans_proto::Frame::from_u32(0x21).encode(&mut frame);
            webtrans_proto::VarInt::from_u32(3).encode(&mut frame);
            frame.extend_from_slice(&[0xff; 3]);
            webtrans_proto::Frame::DATA.encode(&mut frame);
            webtrans_proto::VarInt::try_from(chunk.len())
                .unwrap()
                .encode(&mut frame);
            frame.extend_from_slice(chunk);
            send.write_all(&frame).await.unwrap();
        }
        let error = tokio::time::timeout(TEST_TIMEOUT, conn.closed())
            .await
            .unwrap();
        match error {
            quinn::ConnectionError::ApplicationClosed(close) => assert_eq!(
                close.error_code.into_inner(),
                webtrans_proto::error_to_http3(42)
            ),
            other => panic!("unexpected capsule shutdown: {other:?}"),
        }
    }
}
