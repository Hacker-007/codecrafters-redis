use std::net::IpAddr;

use clap::{Parser, Subcommand};
use tokio::net::{TcpListener, UnixListener};
use tracing_subscriber::EnvFilter;

use crate::{error::RedisResult, server::RedisServer};

mod client;
mod error;
mod server;
mod store;
mod utils;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct CLIArguments {
    #[command(subcommand)]
    mode: ServerMode,
}

#[derive(Debug, Subcommand)]
enum ServerMode {
    Tcp {
        /// The host to bind to.
        #[arg(long, default_value = "127.0.0.1")]
        host: IpAddr,

        /// The port to bind to.
        #[arg(long, default_value_t = 6379)]
        port: u16,
    },
    Socket {
        /// The path of the Unix domain socket
        /// file descriptor.
        #[arg(long)]
        path: String,
    },
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

    match args.mode {
        ServerMode::Tcp { host, port } => {
            let listener = TcpListener::bind((host, port)).await?;
            tracing::info!("server listening on {host}:{port}");
            RedisServer::new(listener).start().await
        }
        ServerMode::Socket { path } => {
            if std::path::Path::new(&path).exists() {
                std::fs::remove_file(&path)?;
            }
            let listener = UnixListener::bind(&path)?;
            tracing::info!("server listening on {}", path);
            RedisServer::new(listener).start().await
        }
    }
}
