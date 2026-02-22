use crate::error::RedisResult;
use futures::{SinkExt, StreamExt};
use resp3::{
    codec::{RESPCodec, RedisCommandCodec},
    encoding, ClientMessage,
};
use std::net::{IpAddr, SocketAddr};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::{FramedRead, FramedWrite};

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
        loop {
            let (stream, client_address) = listener.accept().await?;
            tokio::spawn(async move {
                if let Err(error) = Self::client_loop(stream, client_address).await {
                    tracing::error!("{error}");
                }

                tracing::info!("dropped connection from {client_address}")
            });
        }
    }

    /// Handles incoming client connections in a separate
    /// Tokio task, establishes command forwarding to
    /// store actor.
    async fn client_loop(stream: TcpStream, client_address: SocketAddr) -> RedisResult<()> {
        tracing::info!("accepted connection from {client_address}");
        let (read_half, write_half) = stream.into_split();
        let mut read_half = FramedRead::new(read_half, RedisCommandCodec);
        let mut write_half = FramedWrite::new(write_half, RESPCodec);
        while let Some(message) = read_half.next().await.transpose()? {
            let response = match message {
                ClientMessage::Command(_) => encoding::simple_string(&b"OK"[..]),
                ClientMessage::Error(error) => encoding::simple_error(format!("ERR {error}")),
            };

            write_half.send(response).await?;
        }

        Ok(())
    }
}
