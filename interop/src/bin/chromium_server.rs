use anyhow::{Context, Result, bail};
use webtrans_quinn::ServerBuilder;
use webtrans_quinn::rustls::pki_types::{CertificateDer, PrivateKeyDer};
use wtransport::Identity;
use wtransport::tls::Sha256DigestFmt;

#[tokio::main]
async fn main() -> Result<()> {
    let identity = Identity::self_signed(["localhost", "127.0.0.1"])?;
    let certificate = identity
        .certificate_chain()
        .as_slice()
        .first()
        .context("self-signed identity has no certificate")?;
    let fingerprint = certificate
        .hash()
        .fmt(Sha256DigestFmt::DottedHex)
        .replace(':', "");
    let chain = vec![CertificateDer::from(certificate.der().to_vec())];
    let key = PrivateKeyDer::try_from(identity.private_key().secret_der().to_vec())
        .map_err(anyhow::Error::msg)?;

    let mut server = ServerBuilder::new()
        .with_addr("127.0.0.1:0".parse()?)
        .with_certificate(chain, key)?;
    println!("READY {} {}", server.local_addr()?.port(), fingerprint);

    serve_echo(&mut server).await?;
    serve_close(&mut server).await?;
    serve_rejection(&mut server).await?;
    serve_echo(&mut server).await?;
    Ok(())
}

async fn next_request(server: &mut webtrans_quinn::Server) -> Result<webtrans_quinn::Request> {
    server
        .accept()
        .await
        .context("server endpoint closed")?
        .map_err(Into::into)
}

async fn serve_echo(server: &mut webtrans_quinn::Server) -> Result<()> {
    let request = next_request(server).await?;
    if request.url().path() != "/echo" {
        bail!("expected /echo, got {}", request.url());
    }
    let session = request.ok().await?;

    let (mut bi_send, mut bi_recv) = session.accept_bi().await?;
    let payload = bi_recv.read_to_end(1024).await?;
    bi_send.write_all(&payload).await?;
    bi_send.finish()?;

    let mut uni_recv = session.accept_uni().await?;
    let payload = uni_recv.read_to_end(1024).await?;
    let mut uni_send = session.open_uni().await?;
    uni_send.write_all(&payload).await?;
    uni_send.finish()?;

    let datagram = session.read_datagram().await?;
    session.send_datagram(datagram)?;

    let _ = session.closed().await;
    Ok(())
}

async fn serve_close(server: &mut webtrans_quinn::Server) -> Result<()> {
    let request = next_request(server).await?;
    if request.url().path() != "/server-close" {
        bail!("expected /server-close, got {}", request.url());
    }
    let session = request.ok().await?;
    session.close(0xfedc_ba98, b"native close");
    Ok(())
}

async fn serve_rejection(server: &mut webtrans_quinn::Server) -> Result<()> {
    let request = next_request(server).await?;
    if request.url().path() != "/reject" {
        bail!("expected /reject, got {}", request.url());
    }
    request
        .close(webtrans_quinn::http::StatusCode::FORBIDDEN)
        .await?;
    Ok(())
}
