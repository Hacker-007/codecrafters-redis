use std::net::SocketAddr;

use futures::{SinkExt, StreamExt};
use resp3::{
    codec::{RESPCodec, RedisCommandCodec},
    encoding, ClientMessage,
};
use tokio::{
    net::{
        tcp::{OwnedReadHalf, OwnedWriteHalf},
        TcpStream,
    },
    sync::{mpsc, oneshot},
};
use tokio_util::codec::{FramedRead, FramedWrite};

use crate::{
    error::{RedisError, RedisResult},
    store::actor::StoreMessage,
};

/// A TCP connection to a client connected to
/// the server. Commands sent across this connection
/// are forwarded to the store actor across an MPSC
/// [channel](tokio::sync::mpsc::channel) to be
/// processed.
///
/// The connection is responsible for converting
/// requests from and requests into RESP format.
pub struct ClientConnection {
    address: SocketAddr,
    read_half: FramedRead<OwnedReadHalf, RedisCommandCodec>,
    write_half: FramedWrite<OwnedWriteHalf, RESPCodec>,
    command_tx: mpsc::Sender<StoreMessage>,
}

impl ClientConnection {
    pub fn new(
        stream: TcpStream,
        address: SocketAddr,
        command_tx: mpsc::Sender<StoreMessage>,
    ) -> Self {
        let (read_half, write_half) = stream.into_split();
        Self {
            address,
            read_half: FramedRead::new(read_half, RedisCommandCodec),
            write_half: FramedWrite::new(write_half, RESPCodec),
            command_tx,
        }
    }

    pub async fn run_forever(mut self) -> RedisResult<()> {
        tracing::info!("accepted connection from {}", self.address);
        while let Some(message) = self.read_half.next().await.transpose()? {
            let response = match message {
                ClientMessage::Command(command) => {
                    let (response_tx, response_rx) = oneshot::channel();
                    let message = StoreMessage::new(command, response_tx);
                    self.command_tx
                        .send(message)
                        .await
                        .map_err(|_| RedisError::Unknown)?;

                    response_rx.await.map_err(|_| RedisError::Unknown)?
                }
                ClientMessage::Error(error) => encoding::simple_error(format!("ERR {error}")),
            };

            self.write_half.send(response).await?;
        }

        Ok(())
    }
}

impl Drop for ClientConnection {
    fn drop(&mut self) {
        tracing::info!("dropped connection from {}", self.address);
    }
}
