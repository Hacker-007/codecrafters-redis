use std::{
    collections::{HashMap, VecDeque},
    io::Write,
    net::TcpStream,
    sync::{Mutex, MutexGuard},
    time::SystemTime,
};

use self::value::RedisValue;
use self::{
    commands::{echo, get, info, ping, set, RedisCommand},
    resp_reader::RESPReader,
};

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
    port: u64,
    mode: RedisMode,
    store: Mutex<HashMap<StoreKey, StoreValue>>,
}

impl Redis {
    fn new(port: u64, mode: RedisMode) -> Self {
        Self {
            port,
            mode,
            store: Mutex::new(HashMap::new()),
        }
    }

    pub fn master(port: u64, replication_id: String, replication_offset: String) -> Self {
        Self::new(
            port,
            RedisMode::Master {
                replication_id,
                replication_offset,
            },
        )
    }

    pub fn slave(port: u64, master_host: String, master_port: String) -> Self {
        Self::new(
            port,
            RedisMode::Slave {
                master_host,
                master_port,
            },
        )
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
                let stream = TcpStream::connect(format!("{master_host}:{master_port}"))?;
                let mut reader = RESPReader::new(stream.try_clone()?);
                self.send_ping_master(&stream, &mut reader)?;
                self.send_replconf_port_master(&stream, &mut reader)?;
                self.send_replconf_capa_master(&stream, &mut reader)?;

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

impl Redis {
    fn send_ping_master(
        &self,
        mut stream: &TcpStream,
        reader: &mut RESPReader,
    ) -> anyhow::Result<()> {
        write!(
            stream,
            "{}",
            RedisValue::Array(
                [RedisValue::BulkString("ping".to_string())]
                    .into_iter()
                    .collect::<VecDeque<_>>()
            )
        )?;

        let Ok(RedisValue::SimpleString(response)) = RedisValue::parse(reader) else {
            return Err(anyhow::anyhow!("[redis - error] expected a simple-string encoded response from the master"))
        };

        if response == "PONG" {
            Ok(())
        } else {
            Err(anyhow::anyhow!(
                "[redis - error] expected 'PONG' from master but got '{response}'"
            ))
        }
    }

    fn send_replconf_port_master(
        &self,
        mut stream: &TcpStream,
        reader: &mut RESPReader,
    ) -> anyhow::Result<()> {
        write!(
            stream,
            "{}",
            RedisValue::Array(
                [
                    RedisValue::BulkString("replconf".to_string()),
                    RedisValue::BulkString("listening-port".to_string()),
                    RedisValue::BulkString(self.port.to_string()),
                ]
                .into_iter()
                .collect::<VecDeque<_>>()
            )
        )?;

        let Ok(RedisValue::SimpleString(response)) = RedisValue::parse(reader) else {
            return Err(anyhow::anyhow!("[redis - error] expected a simple-string encoded response from the master"))
        };

        if response == "OK" {
            Ok(())
        } else {
            Err(anyhow::anyhow!(
                "[redis - error] expected 'OK' from master but got '{response}'"
            ))
        }
    }

    fn send_replconf_capa_master(
        &self,
        mut stream: &TcpStream,
        reader: &mut RESPReader,
    ) -> anyhow::Result<()> {
        write!(
            stream,
            "{}",
            RedisValue::Array(
                [
                    RedisValue::BulkString("replconf".to_string()),
                    RedisValue::BulkString("capa".to_string()),
                    RedisValue::BulkString("psync2".to_string()),
                ]
                .into_iter()
                .collect::<VecDeque<_>>()
            )
        )?;

        let Ok(RedisValue::SimpleString(response)) = RedisValue::parse(reader) else {
            return Err(anyhow::anyhow!("[redis - error] expected a simple-string encoded response from the master"))
        };

        if response == "OK" {
            Ok(())
        } else {
            Err(anyhow::anyhow!(
                "[redis - error] expected 'OK' from master but got '{response}'"
            ))
        }
    }
}
