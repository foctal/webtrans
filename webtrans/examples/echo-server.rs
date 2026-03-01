use anyhow::{Context, anyhow};
use clap::Parser;
use ring::digest::{SHA256, digest};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use std::{fs, io, path};
use tracing::Level;
use tracing_subscriber::FmtSubscriber;
use webtrans::{Request, Server, ServerBuilder, Session};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short, long, default_value = "[::]:4443")]
    addr: std::net::SocketAddr,

    /// Use certificates at this path, encoded as PEM.
    #[arg(long)]
    pub tls_cert: Option<path::PathBuf>,

    /// Use the private key at this path, encoded as PEM.
    #[arg(long)]
    pub tls_key: Option<path::PathBuf>,
}

fn fp(der: &[u8]) -> String {
    let d = digest(&SHA256, der);
    d.as_ref().iter().map(|b| format!("{:02x}", b)).collect()
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing subscriber for logging in this example.
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::DEBUG)
        .finish();

    tracing::subscriber::set_global_default(subscriber)
        .expect("failed to set global tracing subscriber");

    // Enable info-level logging.
    let args = Args::parse();

    let (chain, key) = load_or_generate_certs(&args)?;

    let mut server: Server = ServerBuilder::new()
        .with_addr(args.addr)
        .with_certificate(chain, key)?;

    tracing::info!("listening on {}", args.addr);

    // Accept new connections.
    while let Some(conn) = server.accept().await {
        tokio::spawn(async move {
            let err = run_conn(conn).await;
            if let Err(err) = err {
                tracing::error!("connection failed: {err}")
            }
        });
    }

    Ok(())
}

fn load_or_generate_certs(
    args: &Args,
) -> anyhow::Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)> {
    match (&args.tls_cert, &args.tls_key) {
        (Some(cert_path), Some(key_path)) => {
            // Read the PEM certificate chain.
            let chain_file = fs::File::open(cert_path).context("failed to open cert file")?;
            let mut chain_reader = io::BufReader::new(chain_file);

            let chain: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut chain_reader)
                .collect::<Result<_, _>>()
                .context("failed to load certs")?;

            anyhow::ensure!(!chain.is_empty(), "could not find certificate");

            // Read the PEM private key.
            let key_file = fs::File::open(key_path).context("failed to open key file")?;
            let mut key_reader = io::BufReader::new(key_file);

            let key = rustls_pemfile::private_key(&mut key_reader)
                .context("failed to load private key")?
                .context("missing private key")?;

            tracing::info!(
                "loaded certificate from {} and key from {}",
                cert_path.display(),
                key_path.display()
            );

            let leaf = chain
                .get(0)
                .ok_or_else(|| anyhow!("certificate chain is empty"))?;

            tracing::info!(
                "server cert fingerprint(sha256) leaf: {} (chain_len={})",
                fp(leaf.as_ref()),
                chain.len()
            );

            Ok((chain, key))
        }
        (None, None) => {
            // Generate self-signed certs on the fly.
            tracing::warn!(
                "tls_cert/tls_key not provided; generating self-signed certs for local testing"
            );

            let sans = vec![
                "localhost".to_string(),
                "127.0.0.1".to_string(),
                "::1".to_string(),
            ];

            let (chain, key) = webtrans::tls::generate_self_signed_pair_der(sans)
                .map_err(|e| anyhow!("failed to generate self-signed certs: {e}"))?;

            let leaf = chain
                .get(0)
                .ok_or_else(|| anyhow!("certificate chain is empty"))?;

            tracing::info!(
                "server cert fingerprint(sha256) leaf: {} (chain_len={})",
                fp(leaf.as_ref()),
                chain.len()
            );

            Ok((chain, key))
        }
        _ => Err(anyhow!(
            "both --tls-cert and --tls-key must be provided together, or neither"
        )),
    }
}

async fn run_conn(request: Request) -> anyhow::Result<()> {
    tracing::info!("received WebTransport request: {}", request.url());

    // Accept the session.
    let session = request.ok().await.context("failed to accept session")?;
    tracing::info!("accepted session");

    // Run the session.
    if let Err(err) = run_session(session).await {
        tracing::info!("closing session: {err}");
    }

    Ok(())
}

async fn run_session(session: Session) -> anyhow::Result<()> {
    loop {
        // Wait for a bidirectional stream or datagram.
        tokio::select! {
            res = session.accept_bi() => {
                let (mut send, mut recv) = res?;
                tracing::info!("accepted stream");

                // Read the message and echo it back.
                let msg = recv.read_to_end(1024).await?;
                tracing::info!("recv: {}", String::from_utf8_lossy(&msg));

                send.write_all(&msg).await?;
                tracing::info!("send: {}", String::from_utf8_lossy(&msg));
            },
            res = session.read_datagram() => {
                let msg = res?;
                tracing::info!("accepted datagram");
                tracing::info!("recv: {}", String::from_utf8_lossy(&msg));

                session.send_datagram(msg.clone())?;
                tracing::info!("send: {}", String::from_utf8_lossy(&msg));
            },
        };

        tracing::info!("echo successful!");
    }
}
