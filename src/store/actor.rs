use bytes::Bytes;
use resp3::{
    encoding, ConnectionCommand, RESPValue, RedisCommand, SetCondition, SetExpiration,
    StringCommand,
};
use tokio::sync::{mpsc, oneshot};

use crate::{
    store::{RedisStore, StoreValue},
    utils::digest,
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

    pub async fn run_forever(mut self, mut command_rx: mpsc::Receiver<StoreMessage>) {
        while let Some(message) = command_rx.recv().await {
            tracing::info!("got command `{}`", message.command);
            let response = match message.command {
                RedisCommand::Connection(ConnectionCommand::Ping) => self.handle_ping(),
                RedisCommand::String(StringCommand::Get { key }) => self.handle_get(key),
                RedisCommand::String(StringCommand::Set {
                    key,
                    value,
                    condition,
                    get,
                    expiration,
                }) => self.handle_set(key, value, condition, get, expiration),
            };

            // If the receiving end of this channel is closed, then
            // this actor is complete.
            if message.response_tx.send(response).is_err() {
                break;
            }
        }
    }

    fn handle_ping(&self) -> RESPValue {
        encoding::simple_string("PONG")
    }

    fn handle_get(&mut self, key: Bytes) -> RESPValue {
        self.store
            .get(&key)
            .map(Into::into)
            .unwrap_or_else(|| encoding::null())
    }

    fn handle_set(
        &mut self,
        key: Bytes,
        value: Bytes,
        condition: Option<SetCondition>,
        get: bool,
        expiration: Option<SetExpiration>,
    ) -> RESPValue {
        let previous = self.store.get(&key).map(StoreValue::as_string).flatten();
        let should_set = match condition {
            None => true,
            Some(SetCondition::Nx) => previous.is_none(),
            Some(SetCondition::Xx) => previous.is_some(),
            Some(SetCondition::IfEq(expected)) => previous == Some(expected),
            Some(SetCondition::IfNe(expected)) => previous != Some(expected),
            Some(SetCondition::IfDeq(expected)) => {
                previous.as_deref().map(digest) == Some(expected)
            }
            Some(SetCondition::IfDne(expected)) => {
                previous.as_deref().map(digest) != Some(expected)
            }
        };

        if should_set {
            self.store.insert(key.clone(), StoreValue::String(value));
            match expiration {
                None => self.store.remove_expiration(&key),
                Some(SetExpiration::KeepTtl) => {}
                Some(exp) => {
                    let timestamp = exp.resolve().expect("`KEEPTTL` is checked separately");
                    self.store.set_expiration(key, timestamp);
                }
            }
        }

        if get {
            previous
                .map(encoding::bulk_string)
                .unwrap_or(encoding::null())
        } else if should_set {
            encoding::simple_string("OK")
        } else {
            encoding::null()
        }
    }
}
