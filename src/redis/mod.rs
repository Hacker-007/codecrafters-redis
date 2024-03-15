use std::{
    collections::{HashMap, VecDeque},
    io::Write,
    net::TcpStream,
    sync::{Mutex, MutexGuard},
    time::SystemTime,
};

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

    pub fn is_slave(&self) -> bool {
        matches!(self.mode, RedisMode::Slave { .. })
    }

    pub fn connect_to_master(&self) -> anyhow::Result<()> {
        match &self.mode {
            RedisMode::Slave {
                master_host,
                master_port,
            } => {
                let mut stream = TcpStream::connect(format!("{master_host}:{master_port}"))?;
                write!(
                    stream,
                    "{}",
                    RedisValue::Array(
                        [RedisValue::BulkString("ping".to_string())]
                            .into_iter()
                            .collect::<VecDeque<_>>()
                    )
                )?;

                Ok(())
            }
            RedisMode::Master { .. } => Err(anyhow::anyhow!(
                "[redis - error] Redis must be running in slave mode"
            )),
        }
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
