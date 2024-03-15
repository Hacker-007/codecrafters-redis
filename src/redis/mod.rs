use std::collections::HashMap;
use std::time::SystemTime;
use std::sync::{Mutex, MutexGuard};

use self::commands::{echo, get, info, ping, set, RedisCommand};
use self::value::RedisValue;

pub mod commands;
pub mod resp_reader;
pub mod value;

type StoreKey = String;
struct StoreValue {
    pub(self) value: String,
    pub(self) expiration: Option<SystemTime>,
}

pub struct Redis {
    store: Mutex<HashMap<StoreKey, StoreValue>>,
}

impl Redis {
    pub fn new() -> Self {
        Self {
            store: Mutex::new(HashMap::new()),
        }
    }

    pub fn handle_command(&self, command: RedisCommand) -> anyhow::Result<RedisValue> {
        match command {
            RedisCommand::Ping => ping::process(),
            RedisCommand::Echo { echo } => echo::process(echo),
            RedisCommand::Info { section } => info::process(section),
            RedisCommand::Get { key } => get::process(key, self),
            RedisCommand::Set { key, value, px } => set::process(key, value, px, self),
        }
    }

    pub(self) fn lock_store(&self) -> MutexGuard<'_, HashMap<StoreKey, StoreValue>> {
        self.store
            .lock()
            .unwrap()
    }
}
