use anyhow::Context;
use clap::Parser;
use rustls::pki_types::{CertificateDer, pem::PemObject};
use std::{fs, path};
use tracing::Level;
use tracing_subscriber::FmtSubscriber;
use url::Url;
use webtrans::{Client, ClientBuilder};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short, long, default_value = "https://localhost:4443")]
    url: Url,

    /// Accept certificates at this path, encoded as PEM.
    #[arg(long)]
    tls_cert: Option<path::PathBuf>,

    /// Dangerous: disable TLS certificate verification.
    #[arg(long, default_value = "false")]
    tls_disable_verify: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing subscriber for logging in this example.
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::DEBUG)
        .finish();

    tracing::subscriber::set_global_default(subscriber)
        .expect("failed to set global tracing subscriber");

    let args = Args::parse();

    let client: Client;

    let is_local = is_localhost_url(&args.url);

    if args.tls_disable_verify {
        tracing::warn!("disabling TLS certificate verification");
        client = ClientBuilder::new()
            .dangerous()
            .with_no_certificate_verification()?;
    } else if let Some(path) = &args.tls_cert {
        let chain = load_pem_certs(path)?;
        anyhow::ensure!(!chain.is_empty(), "could not find certificate");
        client = ClientBuilder::new().with_server_certificates(chain)?;
    } else if is_local {
        tracing::warn!(
            "no --tls-cert provided and target looks local ({}); \
             using insecure mode for quick testing (equivalent to --tls-disable-verify)",
            args.url
        );
        client = ClientBuilder::new()
            .dangerous()
            .with_no_certificate_verification()?;
    } else {
        client = ClientBuilder::new().with_system_roots()?;
    }

    tracing::info!("connecting to {}", args.url);

    let session = client.connect(args.url).await?;
    tracing::info!("connected");

    let (mut send, mut recv) = session.open_bi().await?;
    tracing::info!("created stream");

    let msg = "hello world".to_string();
    send.write_all(msg.as_bytes()).await?;
    tracing::info!("sent: {msg}");

    send.finish()?;

    let msg = recv.read_to_end(1024).await?;
    tracing::info!("recv: {}", String::from_utf8_lossy(&msg));

    session.close(42069, b"bye");
    session.closed().await;

    tracing::info!("session closed");
    tracing::info!("waiting a moment to ensure close");
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    Ok(())
}

fn load_pem_certs(path: &path::Path) -> anyhow::Result<Vec<CertificateDer<'static>>> {
    let pem = fs::read(path).context("failed to read cert file")?;
    let chain: Vec<CertificateDer<'static>> = CertificateDer::pem_slice_iter(&pem)
        .collect::<Result<_, _>>()
        .context("failed to load certs")?;
    Ok(chain)
}

fn is_localhost_url(url: &Url) -> bool {
    // Localhost patterns:
    // - Domain("localhost")
    // - Ipv4(127.0.0.1)
    // - Ipv6(::1)
    match url.host() {
        Some(url::Host::Domain(d)) => d.eq_ignore_ascii_case("localhost"),
        Some(url::Host::Ipv4(ip)) => ip.is_loopback(),
        Some(url::Host::Ipv6(ip)) => ip.is_loopback(),
        None => false,
    }
}
