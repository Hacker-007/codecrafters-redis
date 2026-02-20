use std::net::IpAddr;

use clap::Parser;
use futures::SinkExt;
use tokio::net::TcpListener;
use tokio_stream::StreamExt;
use tokio_util::codec::{FramedRead, FramedWrite};
use tracing_subscriber::EnvFilter;

use crate::{
    error::RedisResult,
    resp::{
        codec::{RESPCodec, RedisCommandCodec},
        encoding,
    },
};

#[allow(unused)]
mod error;
#[allow(unused)]
mod resp;

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
        .with_file(true)
        .with_line_number(true)
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let listener = TcpListener::bind((args.host, args.port)).await?;
    tracing::info!("server listening on {}:{}", args.host, args.port);

    loop {
        let (stream, client_address) = listener.accept().await?;
        let (read_half, write_half) = stream.into_split();
        tracing::info!("accepted connection from {client_address}");
        let mut read_half = FramedRead::new(read_half, RedisCommandCodec);
        let mut write_half = FramedWrite::new(write_half, RESPCodec);
        tokio::spawn(async move {
            while let Some(command) = read_half.next().await {
                tracing::info!("{command:#?}");
                let response = encoding::simple_string(&b"OK"[..]);
                if let Err(err) = write_half.send(response).await {
                    tracing::error!("{err}");
                    break;
                }
            }

            // TODO:
            // We should continue to keep the connection alive, even if
            // there is an error. Currently, we drop the connection, forcing
            // the client to reconnect. But if we do not drop, then how
            // can we determine that the client dropped the connection?
            tracing::info!("dropped connection from {client_address}");
        });
    }
}
