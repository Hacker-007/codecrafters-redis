use std::collections::HashMap;
use std::sync::{Mutex, MutexGuard};
use std::time::SystemTime;

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

enum RedisMode {
    Master,
    Slave {
        master_host: String,
        master_port: String,
    },
}

pub struct Redis {
    mode: RedisMode,
    store: Mutex<HashMap<StoreKey, StoreValue>>,
}

impl Redis {
    fn new(mode: RedisMode) -> Self {
        Self {
            mode,
            store: Mutex::new(HashMap::new()),
        }
    }

    pub fn master() -> Self {
        Self::new(RedisMode::Master)
    }

    pub fn slave(master_host: String, master_port: String) -> Self {
        Self::new(RedisMode::Slave { master_host, master_port })
    }

    pub fn handle_command(&self, command: RedisCommand) -> anyhow::Result<RedisValue> {
        match command {
            RedisCommand::Ping => ping::process(),
            RedisCommand::Echo { echo } => echo::process(echo),
            RedisCommand::Info { section } => info::process(section, self),
            RedisCommand::Get { key } => get::process(key, self),
            RedisCommand::Set { key, value, px } => set::process(key, value, px, self),
        }
    }

    pub(self) fn lock_store(&self) -> MutexGuard<'_, HashMap<StoreKey, StoreValue>> {
        self.store.lock().unwrap()
    }
}
