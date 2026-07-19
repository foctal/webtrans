use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use webtrans_quinn::quinn::{self, VarInt as QuinnVarInt};
use webtrans_quinn::{ServerBuilder, tls::generate_self_signed_pair_der};
use wtransport::error::ConnectionError as IndependentConnectionError;
use wtransport::{ClientConfig, Endpoint, VarInt};

const TIMEOUT: Duration = Duration::from_secs(5);

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn interoperates_with_wtransport_across_protocol_scenarios() -> Result<()> {
    let (chain, key) =
        generate_self_signed_pair_der(vec!["localhost".to_string(), "127.0.0.1".to_string()])?;
    let raw_client_chain = chain.clone();
    let mut server = ServerBuilder::new()
        .with_addr("127.0.0.1:0".parse()?)
        .with_handshake_timeout(TIMEOUT)
        .with_certificate(chain, key)?;
    let addr = server.local_addr()?;

    let server_task = tokio::spawn(async move {
        serve_echo(&mut server).await?;
        serve_close(&mut server).await?;
        serve_rejection(&mut server).await?;
        serve_echo(&mut server).await?;

        let malformed = tokio::time::timeout(TIMEOUT, server.accept())
            .await
            .context("timed out waiting for malformed peer")?
            .context("server closed while waiting for malformed peer")?;
        match malformed {
            Err(webtrans_quinn::ServerError::SettingsError(
                webtrans_quinn::SettingsError::ProtoError(error),
            )) if error.to_string().contains("16 KiB") => {}
            Err(error) => bail!("unexpected malformed SETTINGS error: {error}"),
            Ok(_) => bail!("malformed SETTINGS unexpectedly produced a request"),
        }
        Result::<()>::Ok(())
    });

    let config = ClientConfig::builder()
        .with_bind_default()
        .with_no_cert_validation()
        .build();
    let independent = Endpoint::client(config)?;

    let base = format!("https://127.0.0.1:{}", addr.port());
    exercise_echo(&independent, &format!("{base}/echo")).await?;

    let connection = independent.connect(format!("{base}/server-close")).await?;
    let close = tokio::time::timeout(TIMEOUT, connection.accept_bi())
        .await
        .context("timed out waiting for the native close")?
        .expect_err("a closed session unexpectedly accepted a stream");
    match close {
        IndependentConnectionError::ApplicationClosed(close) => {
            assert_eq!(close.code().into_inner(), 0x00c0_ffee);
            assert_eq!(close.reason(), b"independent close");
        }
        error => bail!("unexpected independent close result: {error}"),
    }

    assert!(
        independent.connect(format!("{base}/reject")).await.is_err(),
        "the independent client accepted a rejected request"
    );

    exercise_echo(&independent, &format!("{base}/echo")).await?;
    send_malformed_settings(addr, raw_client_chain).await?;

    tokio::time::timeout(TIMEOUT, server_task)
        .await
        .context("native interoperability server timed out")???;
    Ok(())
}

async fn serve_echo(server: &mut webtrans_quinn::Server) -> Result<()> {
    let request = tokio::time::timeout(TIMEOUT, server.accept())
        .await
        .context("timed out waiting for echo request")?
        .context("server closed while waiting for echo request")??;
    assert_eq!(request.url().path(), "/echo");
    let session = request.ok().await?;

    let (mut send, mut recv) = tokio::time::timeout(TIMEOUT, session.accept_bi())
        .await
        .context("timed out waiting for independent stream")??;
    let payload = recv.read_to_end(1024).await?;
    send.write_all(&payload).await?;
    send.finish()?;

    let datagram = tokio::time::timeout(TIMEOUT, session.read_datagram())
        .await
        .context("timed out waiting for independent datagram")??;
    session.send_datagram(datagram)?;
    let _ = tokio::time::timeout(TIMEOUT, session.closed()).await;
    Ok(())
}

async fn serve_close(server: &mut webtrans_quinn::Server) -> Result<()> {
    let request = tokio::time::timeout(TIMEOUT, server.accept())
        .await
        .context("timed out waiting for close request")?
        .context("server closed while waiting for close request")??;
    assert_eq!(request.url().path(), "/server-close");
    let session = request.ok().await?;
    session.close(0x00c0_ffee, b"independent close");
    Ok(())
}

async fn serve_rejection(server: &mut webtrans_quinn::Server) -> Result<()> {
    let request = tokio::time::timeout(TIMEOUT, server.accept())
        .await
        .context("timed out waiting for rejected request")?
        .context("server closed while waiting for rejected request")??;
    assert_eq!(request.url().path(), "/reject");
    request
        .close(webtrans_quinn::http::StatusCode::FORBIDDEN)
        .await?;
    Ok(())
}

async fn exercise_echo(
    client: &Endpoint<wtransport::endpoint::endpoint_side::Client>,
    url: &str,
) -> Result<()> {
    let connection = tokio::time::timeout(TIMEOUT, client.connect(url))
        .await
        .with_context(|| format!("independent client connection to {url} timed out"))??;

    let opening = tokio::time::timeout(TIMEOUT, connection.open_bi())
        .await
        .context("independent stream credit timed out")??;
    let (mut send, mut recv) = tokio::time::timeout(TIMEOUT, opening)
        .await
        .context("independent stream initialization timed out")??;
    send.write_all(b"stream payload").await?;
    send.finish().await?;
    let mut echoed = [0; 14];
    recv.read_exact(&mut echoed).await?;
    assert_eq!(&echoed, b"stream payload");

    connection.send_datagram(b"datagram payload")?;
    let datagram = tokio::time::timeout(TIMEOUT, connection.receive_datagram())
        .await
        .context("independent datagram receive timed out")??;
    assert_eq!(datagram.as_ref(), b"datagram payload");

    connection.close(VarInt::from_u32(0), b"echo complete");
    Ok(())
}

async fn send_malformed_settings(
    addr: std::net::SocketAddr,
    certs: Vec<webtrans_quinn::rustls::pki_types::CertificateDer<'static>>,
) -> Result<()> {
    let mut roots = webtrans_quinn::rustls::RootCertStore::empty();
    for cert in certs {
        roots.add(cert)?;
    }
    let mut tls = webtrans_quinn::rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    tls.alpn_protocols = vec![webtrans_quinn::ALPN.as_bytes().to_vec()];
    let crypto = quinn::crypto::rustls::QuicClientConfig::try_from(tls)?;
    let config = quinn::ClientConfig::new(Arc::new(crypto));
    let mut endpoint = quinn::Endpoint::client("0.0.0.0:0".parse()?)?;
    endpoint.set_default_client_config(config);
    let connection = endpoint.connect(addr, "localhost")?.await?;

    let mut stream = connection.open_uni().await?;
    // Control stream, SETTINGS frame, and a declared 16 KiB + 1 payload with
    // no body. The native decoder must reject the length without allocating it.
    stream
        .write_all(&[0x00, 0x04, 0x80, 0x00, 0x40, 0x01])
        .await?;
    stream.finish()?;
    tokio::time::sleep(Duration::from_millis(100)).await;
    connection.close(QuinnVarInt::from_u32(0), b"malformed input complete");
    Ok(())
}
