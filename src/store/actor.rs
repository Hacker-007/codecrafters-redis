use bytes::Bytes;
use resp3::{encoding, ConnectionCommand, RESPValue, RedisCommand, StringCommand};
use tokio::sync::{mpsc, oneshot};

use crate::{
    error::{RedisError, RedisResult},
    store::RedisStore,
};

#[derive(Debug)]
pub struct StoreMessage {
    command: RedisCommand,
    response_tx: oneshot::Sender<RESPValue>,
}

impl StoreMessage {
    pub fn new(command: RedisCommand, response_tx: oneshot::Sender<RESPValue>) -> Self {
        Self {
            command,
            response_tx,
        }
    }
}

/// An actor that handles all client requests,
/// with full ownership of the key-value store.
/// This ensures serial execution of messages and
/// atomic operations.
#[derive(Debug)]
pub struct RedisStoreActor {
    store: RedisStore,
}

impl RedisStoreActor {
    pub fn new(store: RedisStore) -> Self {
        Self { store }
    }

    pub async fn run_forever(self, mut command_rx: mpsc::Receiver<StoreMessage>) {
        while let Some(message) = command_rx.recv().await {
            tracing::info!("got command `{}`", message.command);
            let response = match message.command {
                RedisCommand::Connection(ConnectionCommand::Ping) => self.handle_ping(),
                RedisCommand::String(StringCommand::Get { key }) => self.handle_get(key),
                command => Err(RedisError::UnsupportedCommand {
                    command: format!("{command}"),
                }),
            };

            // If the receiving end of this channel is closed, then
            // this actor is complete.
            let is_complete = message
                .response_tx
                .send(response.unwrap_or_else(Into::into))
                .is_err();

            if is_complete {
                break;
            }
        }
    }

    fn handle_ping(&self) -> RedisResult<RESPValue> {
        Ok(encoding::simple_string("PONG"))
    }

    fn handle_get(&self, key: Bytes) -> RedisResult<RESPValue> {
        self.store
            .get(&key)
            .map(Into::into)
            .ok_or_else(|| RedisError::KeyNotFound { key })
    }
}
