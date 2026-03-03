use std::fmt;

use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::{TcpListener, UnixListener},
    sync::mpsc,
};

use crate::{
    client::ClientConnection,
    error::RedisResult,
    store::{actor::RedisStoreActor, RedisStore},
};

/// The server process that accepts incoming
/// Redis commands and applies them to the
/// underlying Redis store.
#[derive(Debug)]
pub struct RedisServer<L> {
    listener: L,
}

impl<L> RedisServer<L> {
    pub fn new(listener: L) -> Self {
        Self { listener }
    }
}

impl<L: Listener> RedisServer<L> {
    /// Starts the server, handling client
    /// connections in separate Tokio tasks.
    pub async fn start(&self) -> RedisResult<()> {
        let (command_tx, command_rx) = mpsc::channel(32);
        tokio::spawn(async move {
            let store = RedisStore::default();
            RedisStoreActor::new(store).run_forever(command_rx).await;
        });

        loop {
            let (addr, read, write) = self.listener.accept().await?;
            let command_tx = command_tx.clone();
            tokio::spawn(async move {
                let client = ClientConnection::new(addr, read, write, command_tx);
                if let Err(error) = client.run_forever().await {
                    tracing::error!("{error}");
                }
            });
        }
    }
}

/// An abstraction over connection-oriented listeners,
/// allowing the server to accept client connections
/// over different transports (e.g. TCP, Unix sockets).
pub(crate) trait Listener {
    type Addr: fmt::Display + Send + 'static;
    type Read: AsyncRead + Unpin + Send + 'static;
    type Write: AsyncWrite + Unpin + Send + 'static;

    /// Accepts the next incoming connection, returning
    /// the read and write halves of the stream along
    /// with the client address.
    async fn accept(&self) -> RedisResult<(Self::Addr, Self::Read, Self::Write)>;
}

impl Listener for TcpListener {
    type Addr = std::net::SocketAddr;
    type Read = tokio::net::tcp::OwnedReadHalf;
    type Write = tokio::net::tcp::OwnedWriteHalf;

    async fn accept(&self) -> RedisResult<(Self::Addr, Self::Read, Self::Write)> {
        let (stream, addr) = TcpListener::accept(self).await?;
        let (read, write) = stream.into_split();
        Ok((addr, read, write))
    }
}

impl Listener for UnixListener {
    type Addr = String;
    type Read = tokio::net::unix::OwnedReadHalf;
    type Write = tokio::net::unix::OwnedWriteHalf;

    async fn accept(&self) -> RedisResult<(Self::Addr, Self::Read, Self::Write)> {
        let (stream, addr) = UnixListener::accept(self).await?;
        let (read, write) = stream.into_split();
        let label = addr
            .as_pathname()
            .map(|p| p.display().to_string())
            .unwrap_or_else(|| "(unnamed)".to_string());

        Ok((label, read, write))
    }
}
