use bytes::Bytes;
use resp3::{encoding, RESPValue};
use std::collections::HashMap;

pub mod actor;

/// A cheaply clonable key within the
/// [`Store`].
pub type StoreKey = Bytes;

#[derive(Debug)]
pub enum StoreValue {
    String(Bytes),
}

impl From<&StoreValue> for RESPValue {
    fn from(value: &StoreValue) -> Self {
        match value {
            StoreValue::String(bytes) => encoding::bulk_string(bytes.clone()),
        }
    }
}

/// The primary in-memory key-value store
/// with wait queues and expiration metadata.
#[derive(Debug, Default)]
pub struct RedisStore {
    items: HashMap<StoreKey, StoreValue>,
}

impl RedisStore {
    pub fn get(&self, key: &StoreKey) -> Option<&StoreValue> {
        self.items.get(key)
    }
}
