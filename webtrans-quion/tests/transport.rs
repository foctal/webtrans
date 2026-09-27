//! Real UDP coverage of both native backends and their shared trait contract.

use std::{future::Future, time::Duration};

use bytes::Bytes;
use url::Url;
use webtrans_quion::{ClientBuilder, ServerBuilder, Session, tls::generate_self_signed_pair_der};
use webtrans_trait::{Error, RecvStream, SendStream, Session as SessionTrait};

async fn bounded(future: impl Future<Output = ()>) {
    tokio::time::timeout(Duration::from_secs(20), future)
        .await
        .expect("transport test timed out");
}

async fn pair() -> (Session, Session) {
    let (chain, key) = generate_self_signed_pair_der(vec!["localhost".into()]).unwrap();
    let mut server = ServerBuilder::new()
        .with_addr("127.0.0.1:0".parse().unwrap())
        .with_certificate(chain.clone(), key)
        .unwrap();
    let client = ClientBuilder::new()
        .with_server_certificates(chain)
        .unwrap();
    let url = Url::parse(&format!(
        "https://127.0.0.1:{}/echo?q=1",
        server.local_addr().unwrap().port()
    ))
    .unwrap();
    let (client, server) = tokio::join!(client.connect(url.clone()), async {
        let request = server.accept().await.unwrap().unwrap();
        assert_eq!(request.url(), &url);
        request.ok().await.unwrap()
    });
    (client.unwrap(), server)
}

async fn exercise<A: SessionTrait, B: SessionTrait>(sender: &A, receiver: &B) {
    // Exercise partial writes and flow-control updates with a nontrivial payload.
    let payload = Bytes::from((0..256 * 1024).map(|i| (i % 251) as u8).collect::<Vec<_>>());
    let send_payload = payload.clone();
    let outbound = async {
        let (mut send, mut recv) = sender.open_bi().await.unwrap();
        send.set_priority(17);
        send.write_chunk(send_payload).await.unwrap();
        send.finish().unwrap();
        assert_eq!(recv.read_all().await.unwrap(), b"reply"[..]);
        send.closed().await.unwrap();
    };
    let inbound = async {
        let (mut send, mut recv) = receiver.accept_bi().await.unwrap();
        assert_eq!(recv.read_all().await.unwrap(), payload);
        send.write_all(b"reply").await.unwrap();
        send.finish().unwrap();
        send.closed().await.unwrap();
    };
    tokio::join!(outbound, inbound);

    let mut send = sender.open_uni().await.unwrap();
    send.write_all(b"unidirectional").await.unwrap();
    send.finish().unwrap();
    let mut recv = receiver.accept_uni().await.unwrap();
    assert_eq!(recv.read_all().await.unwrap(), b"unidirectional"[..]);

    assert!(sender.max_datagram_size() >= 32);
    sender
        .send_datagram(Bytes::from_static(b"datagram"))
        .await
        .unwrap();
    assert_eq!(receiver.recv_datagram().await.unwrap(), b"datagram"[..]);

    let mut send = sender.open_uni().await.unwrap();
    send.write_all(b"stop").await.unwrap();
    let mut recv = receiver.accept_uni().await.unwrap();
    recv.stop(12345);
    assert_eq!(send.closed().await.unwrap_err().stream_error(), Some(12345));
}

#[tokio::test]
async fn quion_loopback_and_trait_contract() {
    bounded(async {
        let (client, server) = pair().await;
        assert_eq!(client, client.clone());
        assert_ne!(client, server);
        assert!(client.reset_stream_at_negotiated());
        assert!(server.reset_stream_at_negotiated());
        exercise(&client, &server).await;
        exercise(&server, &client).await;
        client.close(71, b"finished");
        let error = server.closed().await;
        assert_eq!(error.session_error(), Some((71, "finished".into())));
    })
    .await;
}

#[tokio::test]
async fn quion_client_with_quinn_server() {
    bounded(async {
        let (chain, key) = generate_self_signed_pair_der(vec!["localhost".into()]).unwrap();
        let mut server = webtrans_quinn::ServerBuilder::new()
            .with_addr("127.0.0.1:0".parse().unwrap())
            .with_certificate(chain.clone(), key)
            .unwrap();
        let client = ClientBuilder::new()
            .with_server_certificates(chain)
            .unwrap();
        let url = Url::parse(&format!(
            "https://127.0.0.1:{}/interop",
            server.local_addr().unwrap().port()
        ))
        .unwrap();
        let (client, server) = tokio::join!(client.connect(url), async {
            server.accept().await.unwrap().unwrap().ok().await.unwrap()
        });
        let client = client.unwrap();
        assert!(!client.reset_stream_at_negotiated());
        exercise(&client, &server).await;
        exercise(&server, &client).await;
        client.close(72, b"interop");
        let error = server.closed().await;
        assert!(
            matches!(
                error,
                webtrans_quinn::SessionError::ConnectionError(
                    webtrans_quinn::quinn::ConnectionError::LocallyClosed
                )
            ) || error.session_error() == Some((72, "interop".into()))
        );
    })
    .await;
}

#[tokio::test]
async fn quinn_client_with_quion_server() {
    bounded(async {
        let (chain, key) = generate_self_signed_pair_der(vec!["localhost".into()]).unwrap();
        let mut server = ServerBuilder::new()
            .with_addr("127.0.0.1:0".parse().unwrap())
            .with_certificate(chain.clone(), key)
            .unwrap();
        let client = webtrans_quinn::ClientBuilder::new()
            .with_server_certificates(chain)
            .unwrap();
        let url = Url::parse(&format!(
            "https://127.0.0.1:{}/interop",
            server.local_addr().unwrap().port()
        ))
        .unwrap();
        let (client, server) = tokio::join!(client.connect(url), async {
            server.accept().await.unwrap().unwrap().ok().await.unwrap()
        });
        let client = client.unwrap();
        assert!(!server.reset_stream_at_negotiated());
        exercise(&client, &server).await;
        exercise(&server, &client).await;
        server.close(73, b"interop");
        let error = client.closed().await;
        assert!(
            matches!(
                error,
                webtrans_quinn::SessionError::ConnectionError(
                    webtrans_quinn::quinn::ConnectionError::LocallyClosed
                )
            ) || error.session_error() == Some((73, "interop".into()))
        );
    })
    .await;
}

#[tokio::test]
async fn reliable_reset_delivers_preface_before_application_reset() {
    bounded(async {
        let (client, server) = pair().await;
        for code in [0, 1, 255, u32::MAX] {
            // No yield between open and reset: a normal RESET_STREAM can discard
            // the preface before the receiver can associate it with the session.
            let mut send = client.open_uni().await.unwrap();
            send.write_all(b"discarded").await.unwrap();
            send.reset(code).unwrap();
            let mut recv = server.accept_uni().await.unwrap();
            assert_eq!(
                recv.read(&mut [0; 1]).await.unwrap_err().stream_error(),
                Some(code)
            );
            let (mut send, _recv) = server.open_bi().await.unwrap();
            send.write_all(b"discarded").await.unwrap();
            send.reset(code).unwrap();
            let (_send, mut recv) = client.accept_bi().await.unwrap();
            assert_eq!(recv.received_reset().await.unwrap(), Some(code));
        }
    })
    .await;
}

#[tokio::test]
async fn tokio_io_chunks_and_read_limits() {
    bounded(async {
        let (client, server) = pair().await;
        let mut send = client.open_uni().await.unwrap();
        send.write_all_chunks(&mut [Bytes::from_static(b"abc"), Bytes::from_static(b"def")])
            .await
            .unwrap();
        tokio::io::AsyncWriteExt::write_all(&mut send, b"ghi")
            .await
            .unwrap();
        tokio::io::AsyncWriteExt::shutdown(&mut send).await.unwrap();
        let mut recv = server.accept_uni().await.unwrap();
        let mut output = Vec::new();
        tokio::io::AsyncReadExt::read_to_end(&mut recv, &mut output)
            .await
            .unwrap();
        assert_eq!(output, b"abcdefghi");

        let mut send = client.open_uni().await.unwrap();
        send.write_all(b"too long").await.unwrap();
        send.finish().unwrap();
        let mut recv = server.accept_uni().await.unwrap();
        assert!(matches!(
            recv.read_to_end(2).await,
            Err(webtrans_quion::ReadError::Transport(
                webtrans_quion::quion::ReadError::TooLong(2)
            ))
        ));
    })
    .await;
}

#[tokio::test]
async fn ipv6_and_http_rejection() {
    bounded(async {
        let (chain, key) = generate_self_signed_pair_der(vec!["localhost".into()]).unwrap();
        let mut server = ServerBuilder::new()
            .with_addr("[::1]:0".parse().unwrap())
            .with_certificate(chain.clone(), key)
            .unwrap();
        let client = ClientBuilder::new()
            .with_server_certificates(chain)
            .unwrap();
        let url = Url::parse(&format!(
            "https://[::1]:{}/reject",
            server.local_addr().unwrap().port()
        ))
        .unwrap();
        let (result, _) = tokio::join!(client.connect(url), async {
            server
                .accept()
                .await
                .unwrap()
                .unwrap()
                .close(http::StatusCode::FORBIDDEN)
                .await
                .unwrap();
        });
        assert!(matches!(
            result,
            Err(webtrans_quion::ClientError::HttpError(
                webtrans_quion::ConnectError::ErrorStatus(http::StatusCode::FORBIDDEN)
            ))
        ));
    })
    .await;
}

#[tokio::test]
async fn certificate_pin_mismatch_is_rejected() {
    bounded(async {
        let (chain, key) = generate_self_signed_pair_der(vec!["localhost".into()]).unwrap();
        let mut server = ServerBuilder::new()
            .with_addr("127.0.0.1:0".parse().unwrap())
            .with_certificate(chain, key)
            .unwrap();
        let client = ClientBuilder::new()
            .with_handshake_timeout(Duration::from_secs(3))
            .with_server_certificate_hashes(vec![vec![0; 32]])
            .unwrap();
        let url = Url::parse(&format!(
            "https://127.0.0.1:{}/",
            server.local_addr().unwrap().port()
        ))
        .unwrap();
        let accept = tokio::spawn(async move { server.accept().await });
        assert!(matches!(
            client.connect(url).await,
            Err(webtrans_quion::ClientError::Connection(_))
        ));
        accept.abort();
    })
    .await;
}

#[tokio::test]
async fn close_notifies_all_cloned_session_waiters() {
    bounded(async {
        let (client, server) = pair().await;
        let first = server.clone();
        let second = server.clone();
        let first = tokio::spawn(async move { first.closed().await });
        let second = tokio::spawn(async move { second.closed().await });
        tokio::task::yield_now().await;
        assert!(server.close_reason().is_none());
        client.close(74, b"broadcast");
        for waiter in [first, second] {
            assert_eq!(
                waiter.await.unwrap().session_error(),
                Some((74, "broadcast".into()))
            );
        }
    })
    .await;
}

#[tokio::test]
async fn header_only_reliable_reset_preserves_the_error_code() {
    bounded(async {
        let (client, server) = pair().await;
        let mut send = client.open_uni().await.unwrap();
        send.reset(42).unwrap();
        let mut recv = server.accept_uni().await.unwrap();
        assert_eq!(
            recv.read(&mut [0; 1]).await.unwrap_err().stream_error(),
            Some(42)
        );
    })
    .await;
}

#[tokio::test]
async fn ipv4_client_reaches_dual_stack_server() {
    bounded(async {
        let (chain, key) = generate_self_signed_pair_der(vec!["localhost".into()]).unwrap();
        let mut server = ServerBuilder::new()
            .with_addr("[::]:0".parse().unwrap())
            .with_certificate(chain.clone(), key)
            .unwrap();
        let client = ClientBuilder::new()
            .with_server_certificates(chain)
            .unwrap();
        let url = Url::parse(&format!(
            "https://127.0.0.1:{}/",
            server.local_addr().unwrap().port()
        ))
        .unwrap();
        let (client, server) = tokio::join!(client.connect(url), async {
            server.accept().await.unwrap().unwrap().ok().await.unwrap()
        });
        exercise(&client.unwrap(), &server).await;
    })
    .await;
}

#[tokio::test]
async fn raw_quic_sessions_exchange_data_and_close_on_drop() {
    bounded(async {
        use webtrans_quion::quion;
        let (chain, key) = generate_self_signed_pair_der(vec!["localhost".into()]).unwrap();
        let mut roots = webtrans_quion::rustls::RootCertStore::empty();
        roots.add(chain[0].clone()).unwrap();
        let mut transport = quion::TransportConfig::default();
        transport.set_max_datagram_frame_size(Some(quion::VarInt::from_u32(65_535)));
        let config = quion::ServerConfig::builder()
            .with_single_cert(chain, key).unwrap()
            .with_alpn_protocols([b"raw-test".to_vec()])
            .with_transport_config(transport.clone()).build().unwrap();
        let endpoint = quion::Endpoint::server(config, "127.0.0.1:0".parse().unwrap()).unwrap();
        let _driver = endpoint.spawn_default_server_udp_driver(65_535).unwrap();
        let client_endpoint = quion::Endpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
        client_endpoint.set_default_client_config(quion::ClientConfig::builder()
            .with_root_certificates(roots).unwrap()
            .with_alpn_protocols([b"raw-test".to_vec()])
            .with_transport_config(transport).build());
        let (client, server) = tokio::join!(
            client_endpoint.connect(endpoint.local_addr(), "localhost").unwrap(),
            async { endpoint.accept().await.unwrap().await.unwrap() }
        );
        let url = Url::parse("https://localhost/raw").unwrap();
        let client = Session::raw(client.unwrap(), url.clone());
        let server = Session::raw(server, url);
        exercise(&client, &server).await;
        drop(client);
        assert!(matches!(server.closed().await,
            webtrans_quion::SessionError::ConnectionError(quion::ConnectionError::ApplicationClosed { code, .. })
                if code == quion::VarInt::from_u32(0)));
    }).await;
}
