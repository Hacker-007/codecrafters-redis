use bytes::{BufMut, Bytes, BytesMut};
use resp3::{
    encoding, ConnectionCommand, RESPValue, RedisCommand, SetCondition, SetExpiration,
    StringCommand,
};
use tokio::sync::{mpsc, oneshot};

use crate::{
    error::{RedisError, RedisResult},
    store::{RedisStore, StoreValue},
    utils::digest,
};

#[derive(Debug)]
pub struct StoreMessage {
    command: RedisCommand,
    response_tx: oneshot::Sender<RedisResult<RESPValue>>,
}

impl StoreMessage {
    pub fn new(
        command: RedisCommand,
        response_tx: oneshot::Sender<RedisResult<RESPValue>>,
    ) -> Self {
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
            let response = match message.command {
                RedisCommand::Connection(ConnectionCommand::Ping) => Ok(self.handle_ping()),
                RedisCommand::String(StringCommand::Get { key }) => Ok(self.handle_get(key)),
                RedisCommand::String(StringCommand::Set {
                    key,
                    value,
                    condition,
                    get,
                    expiration,
                }) => Ok(self.handle_set(key, value, condition, get, expiration)),
                RedisCommand::String(StringCommand::Append { key, value }) => {
                    self.handle_append(key, value)
                }
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
            .unwrap_or_else(encoding::null)
    }

    fn handle_set(
        &mut self,
        key: Bytes,
        value: Bytes,
        condition: Option<SetCondition>,
        get: bool,
        expiration: Option<SetExpiration>,
    ) -> RESPValue {
        let previous = self.store.get(&key).and_then(StoreValue::as_string);
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
                    let timestamp = exp
                        .resolve()
                        .expect("`KEEPTTL` should be checked separately");

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

    fn handle_append(&mut self, key: Bytes, value: Bytes) -> RedisResult<RESPValue> {
        let appended = match self.store.get(&key).and_then(StoreValue::as_string) {
            Some(previous) => {
                let mut buffer = BytesMut::with_capacity(previous.len() + value.len());
                buffer.put(previous);
                buffer.put(value);
                buffer.freeze()
            }
            None => value,
        };

        let length = appended.len();
        self.store.insert(key, StoreValue::String(appended));
        i64::try_from(length)
            .map(encoding::integer)
            .map_err(|_| RedisError::LengthTooLarge)
    }
}
