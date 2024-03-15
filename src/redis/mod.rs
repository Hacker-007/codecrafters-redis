use std::collections::HashMap;
use std::net::TcpStream;
use std::sync::{Mutex, MutexGuard};
use std::time::SystemTime;

use self::commands::{echo, get, info, ping, set, RedisCommand};

pub mod commands;
pub mod resp_reader;
pub mod value;

type StoreKey = String;
struct StoreValue {
    pub(self) value: String,
    pub(self) expiration: Option<SystemTime>,
}

#[allow(dead_code)]
enum RedisMode {
    Master {
        replication_id: String,
        replication_offset: String,
    },
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

    pub fn master(replication_id: String, replication_offset: String) -> Self {
        Self::new(RedisMode::Master {
            replication_id,
            replication_offset,
        })
    }

    pub fn slave(master_host: String, master_port: String) -> Self {
        Self::new(RedisMode::Slave {
            master_host,
            master_port,
        })
    }

    pub fn handle_command(&self, command: RedisCommand, stream: &mut TcpStream) -> anyhow::Result<()> {
        match command {
            RedisCommand::Ping => ping::process(stream),
            RedisCommand::Echo { echo } => echo::process(echo, stream),
            RedisCommand::Info { section } => info::process(section, self, stream),
            RedisCommand::Get { key } => get::process(key, self, stream),
            RedisCommand::Set { key, value, px } => set::process(key, value, px, self, stream),
        }
    }

    pub(self) fn lock_store(&self) -> MutexGuard<'_, HashMap<StoreKey, StoreValue>> {
        self.store.lock().unwrap()
    }
}
