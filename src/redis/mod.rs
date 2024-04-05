use bytes::Bytes;
use std::{fmt::Write, io::Write as IOWrite, ops::DerefMut, time::SystemTime};
use tokio::sync::oneshot;

use self::{
    command::{Command, InfoSection},
    resp::RESPValue,
    store::{Store, StoreValue},
};

pub mod command;
pub mod resp;
pub mod resp_builder;
pub mod server;
pub mod store;

#[derive(Debug, Clone)]
pub enum RedisMode {
    Slave {
        master_host: String,
        master_port: String,
    },
    Master {
        replication_id: String,
        replication_offset: usize,
    },
}

pub struct CommandPacket {
    pub command: Command,
    pub response_tx: oneshot::Sender<Vec<u8>>,
}

impl CommandPacket {
    pub fn new(command: Command, response_tx: oneshot::Sender<Vec<u8>>) -> Self {
        Self {
            command,
            response_tx,
        }
    }
}

const EMPTY_RDB_HEX: &'static str = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";

#[derive(Debug)]
pub struct Redis {
    mode: RedisMode,
    store: Store,
}

impl Redis {
    pub fn new(mode: RedisMode) -> Self {
        Self {
            mode,
            store: Store::new(),
        }
    }

    pub fn handle(&mut self, command: Command, output_sink: &mut Vec<u8>) -> anyhow::Result<()> {
        match command {
            Command::Ping => self.ping(output_sink),
            Command::Echo { echo } => self.echo(echo, output_sink),
            Command::Info { section } => self.info(section, output_sink),
            Command::Get { key } => self.get(key, output_sink),
            Command::Set { key, value, px } => self.set(key, value, px, output_sink),
            Command::ReplConfPort { .. } => self.repl_conf_port(output_sink),
            Command::ReplConfCapa { .. } => self.repl_conf_capa(output_sink),
            Command::PSync { .. } => self.psync(output_sink),
        }
    }

    fn ping(&mut self, output_sink: &mut Vec<u8>) -> anyhow::Result<()> {
        write!(
            output_sink,
            "{}",
            RESPValue::SimpleString("PONG".to_string())
        )?;

        Ok(())
    }

    fn echo(&mut self, echo: String, output_sink: &mut Vec<u8>) -> anyhow::Result<()> {
        write!(output_sink, "{}", RESPValue::BulkString(echo))?;

        Ok(())
    }

    fn info(&mut self, section: InfoSection, output_sink: &mut Vec<u8>) -> anyhow::Result<()> {
        if section == InfoSection::Replication {
            let info = match &self.mode {
                RedisMode::Master {
                    replication_id,
                    replication_offset,
                    ..
                } => format!(
                    "role:master\nmaster_replid:{replication_id}\nmaster_repl_offset:{replication_offset}",
                ),
                RedisMode::Slave { .. } => "role:slave".to_string(),
            };

            write!(output_sink, "{}", RESPValue::BulkString(info))?;
            return Ok(());
        }

        Err(anyhow::anyhow!(
            "[redis - error] command 'info' currently only supports section 'replication'"
        ))
    }

    fn get(&mut self, key: String, output_sink: &mut Vec<u8>) -> anyhow::Result<()> {
        let value = match self.store.get(&key) {
            Some(StoreValue {
                expiration: Some(expiration),
                ..
            }) if *expiration <= SystemTime::now() => {
                self.store.remove(&key);
                RESPValue::NullBulkString
            }
            Some(StoreValue { value, .. }) => RESPValue::BulkString(value.clone()),
            _ => RESPValue::NullBulkString,
        };

        write!(output_sink, "{value}")?;
        Ok(())
    }

    fn set(
        &mut self,
        key: String,
        value: String,
        px: Option<SystemTime>,
        output_sink: &mut Vec<u8>,
    ) -> anyhow::Result<()> {
        self.store.insert(
            key,
            StoreValue {
                value,
                expiration: px,
            },
        );
        write!(output_sink, "{}", RESPValue::SimpleString("OK".to_string()))?;
        Ok(())
    }

    fn repl_conf_port(&mut self, output_sink: &mut Vec<u8>) -> anyhow::Result<()> {
        write!(output_sink, "{}", RESPValue::SimpleString("OK".to_string()))?;
        Ok(())
    }

    fn repl_conf_capa(&mut self, output_sink: &mut Vec<u8>) -> anyhow::Result<()> {
        write!(output_sink, "{}", RESPValue::SimpleString("OK".to_string()))?;
        Ok(())
    }

    fn psync(&mut self, output_sink: &mut Vec<u8>) -> anyhow::Result<()> {
        if let RedisMode::Master {
            replication_id,
            replication_offset,
            ..
        } = &self.mode
        {
            write!(
                output_sink,
                "{}",
                RESPValue::SimpleString(format!(
                    "FULLRESYNC {} {}",
                    replication_id, *replication_offset
                ))
            )?;

            let rdb_file = (0..EMPTY_RDB_HEX.len())
                .step_by(2)
                .map(|i| u8::from_str_radix(&EMPTY_RDB_HEX[i..i + 2], 16))
                .collect::<Result<Vec<_>, _>>()?;

            write!(output_sink, "${}\r\n", rdb_file.len())?;
            output_sink.write_all(&rdb_file)?;
            return Ok(());
        } else {
            Err(anyhow::anyhow!(
                "[redis - error] Redis must be running in master mode to respond to 'PSYNC' command"
            ))
        }
    }
}
