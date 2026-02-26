use std::net::IpAddr;

use clap::Parser;
use tracing_subscriber::EnvFilter;

use crate::{error::RedisResult, server::RedisServer};

mod client;
mod error;
mod server;
mod store;

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

    let mut server = RedisServer::new(args.host, args.port);
    server.start().await
}
