use std::net::IpAddr;

use clap::Parser;
use tokio::net::TcpListener;
use tracing::info;
use tracing_subscriber::{fmt::format::FmtSpan, EnvFilter};

use crate::error::RedisResult;

mod error;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct CLIArguments {
    /// The host to bind to.
    #[arg(long, default_value = "127.0.0.1")]
    host: IpAddr,

    /// The port to bind to.
    #[arg(long, default_value_t = 6379)]
    port: u16,
}

#[tokio::main]
async fn main() -> RedisResult<()> {
    let args = CLIArguments::parse();
    tracing_subscriber::fmt()
        .compact()
        .without_time()
        .with_target(true)
        .with_file(true)
        .with_line_number(true)
        .with_span_events(FmtSpan::NONE)
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let listener = TcpListener::bind((args.host, args.port)).await?;
    info!("server listening on {}:{}", args.host, args.port);

    loop {
        let (stream, client_address) = listener.accept().await?;
        let (_read_half, _write_half) = stream.into_split();
        info!("accepted connection from {client_address}");
        info!("dropped connection from {client_address}");
    }
}
