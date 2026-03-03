use std::fmt;

use futures::{SinkExt, StreamExt};
use resp3::{
    codec::{RESPCodec, RedisCommandCodec},
    encoding, ClientMessage,
};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::{mpsc, oneshot},
};
use tokio_util::codec::{FramedRead, FramedWrite};

use crate::{
    error::{RedisError, RedisResult},
    store::actor::StoreMessage,
};

/// A connection to a client connected to the server.
/// Commands sent across this connection are forwarded
/// to the store actor across an MPSC
/// [channel](tokio::sync::mpsc::channel) to be
/// processed.
///
/// The connection is responsible for converting
/// requests from and requests into RESP format.
pub struct ClientConnection<R, W> {
    label: String,
    read_half: FramedRead<R, RedisCommandCodec>,
    write_half: FramedWrite<W, RESPCodec>,
    command_tx: mpsc::Sender<StoreMessage>,
}

impl<R: AsyncRead + Unpin, W: AsyncWrite + Unpin> ClientConnection<R, W> {
    pub fn new(
        address: impl fmt::Display,
        read: R,
        write: W,
        command_tx: mpsc::Sender<StoreMessage>,
    ) -> Self {
        Self {
            label: address.to_string(),
            read_half: FramedRead::new(read, RedisCommandCodec),
            write_half: FramedWrite::new(write, RESPCodec),
            command_tx,
        }
    }

    pub async fn run_forever(mut self) -> RedisResult<()> {
        tracing::info!("accepted connection from {}", self.label);
        while let Some(message) = self.read_half.next().await.transpose()? {
            let response = match message {
                ClientMessage::Command(command) => {
                    let (response_tx, response_rx) = oneshot::channel();
                    let message = StoreMessage::new(command, response_tx);
                    self.command_tx
                        .send(message)
                        .await
                        .map_err(|_| RedisError::Unknown)?;

                    response_rx
                        .await
                        .map_err(|_| RedisError::Unknown)?
                        .unwrap_or_else(|error| encoding::simple_error(format!("ERR {error}")))
                }
                ClientMessage::Error(error) => encoding::simple_error(format!("ERR {error}")),
            };

            self.write_half.send(response).await?;
        }

        Ok(())
    }
}

impl<R, W> Drop for ClientConnection<R, W> {
    fn drop(&mut self) {
        tracing::info!("dropped connection from {}", self.label);
    }
}
