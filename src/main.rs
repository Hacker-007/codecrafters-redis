use std::net::IpAddr;

use clap::Parser;
use futures::SinkExt;
use resp3::{
    codec::{RESPCodec, RedisCommandCodec},
    encoding, ClientMessage,
};
use tokio::net::TcpListener;
use tokio_stream::StreamExt;
use tokio_util::codec::{FramedRead, FramedWrite};
use tracing_subscriber::EnvFilter;

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
            while let Some(result) = read_half.next().await {
                let message = match result {
                    Ok(message) => message,
                    Err(err) => {
                        tracing::error!("{err}");
                        break;
                    }
                };

                let response = match message {
                    ClientMessage::Command(command) => {
                        tracing::info!("{command:#?}");
                        encoding::simple_string(&b"OK"[..])
                    }
                    ClientMessage::Error(err) => {
                        tracing::error!("{err}");
                        encoding::simple_error(format!("ERR {err}").into_bytes())
                    }
                };

                if let Err(err) = write_half.send(response).await {
                    tracing::error!("{err}");
                    break;
                }
            }

            tracing::info!("dropped connection from {client_address}");
        });
    }
}
