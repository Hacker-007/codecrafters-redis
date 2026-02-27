use bytes::Bytes;
use resp3::{encoding, RESPValue};
use std::{collections::HashMap, time::SystemTime};

pub mod actor;

/// A cheaply clonable key within the
/// [`Store`].
pub type StoreKey = Bytes;

#[derive(Debug)]
pub enum StoreValue {
    String(Bytes),
}

impl StoreValue {
    pub fn as_string(&self) -> Option<Bytes> {
        match self {
            StoreValue::String(bytes) => Some(bytes.clone()),
        }
    }
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
    expirations: HashMap<StoreKey, SystemTime>,
}

impl RedisStore {
    /// Gets the value associated with `key`. If the
    /// `key` does not exist or was set to expire at
    /// a previous time, then `None` is returned.
    pub fn get(&mut self, key: &StoreKey) -> Option<&StoreValue> {
        if let Some(&expiration) = self.expirations.get(key) {
            if expiration <= SystemTime::now() {
                self.expirations.remove(key);
                self.items.remove(key);
                return None;
            }
        }

        self.items.get(key)
    }

    pub fn insert(&mut self, key: StoreKey, value: StoreValue) {
        self.items.insert(key, value);
    }

    pub fn set_expiration(&mut self, key: StoreKey, expiration: SystemTime) {
        self.expirations.insert(key, expiration);
    }

    pub fn remove_expiration(&mut self, key: &StoreKey) {
        self.expirations.remove(key);
    }
}
