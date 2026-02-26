use std::net::IpAddr;

use tokio::{net::TcpListener, sync::mpsc};

use crate::{
    client::ClientConnection,
    error::RedisResult,
    store::{actor::RedisStoreActor, RedisStore},
};

/// The server process that accepts incoming
/// Redis commands and applies them to the
/// underlying Redis store.
#[derive(Debug)]
pub struct RedisServer {
    host: IpAddr,
    port: u16,
}

impl RedisServer {
    pub fn new(host: IpAddr, port: u16) -> Self {
        Self { host, port }
    }

    /// Starts a TCP server, handling client
    /// connections in separate Tokio tasks.
    pub async fn start(&mut self) -> RedisResult<()> {
        let listener = TcpListener::bind((self.host, self.port)).await?;
        tracing::info!("server listening on {}:{}", self.host, self.port);

        let (command_tx, command_rx) = mpsc::channel(32);
        tokio::spawn(async move {
            let store = RedisStore::default();
            RedisStoreActor::new(store).run_forever(command_rx).await;
        });

        loop {
            let (stream, client_address) = listener.accept().await?;
            let command_tx = command_tx.clone();
            tokio::spawn(async move {
                let client = ClientConnection::new(stream, client_address, command_tx);
                if let Err(error) = client.run_forever().await {
                    tracing::error!("{error}");
                }
            });
        }
    }
}
