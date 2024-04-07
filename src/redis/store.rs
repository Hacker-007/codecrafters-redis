use std::{collections::HashMap, time::SystemTime};

use anyhow::Context;
use tokio::{
    io::AsyncWriteExt,
    net::{
        tcp::{ReadHalf, WriteHalf},
        TcpStream,
    },
};

use super::{
    command::{InfoSection, RedisCommand},
    resp::RESPValue,
    resp_stream::RESPStream,
    server::RedisWriteStream,
};

const EMPTY_RDB_HEX: &str = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";

pub struct RedisCommandPacket {
    pub command: RedisCommand,
    pub write_stream: RedisWriteStream,
}

#[derive(Debug)]
pub enum RedisMode {
    Primary {
        replication_id: String,
        replication_offset: usize,
    },
    Replica {
        primary_host: String,
        primary_port: usize,
    },
}

type StoreKey = Vec<u8>;

#[derive(Debug)]
pub struct StoreValue {
    value: Vec<u8>,
    expiration: Option<SystemTime>,
}

#[derive(Debug)]
pub struct RedisStore {
    mode: RedisMode,
    values: HashMap<StoreKey, StoreValue>,
}

impl RedisStore {
    pub fn replica(primary_host: String, primary_port: usize) -> Self {
        Self {
            mode: RedisMode::Replica {
                primary_host,
                primary_port,
            },
            values: HashMap::default(),
        }
    }

    pub fn primary(replication_id: String, replication_offset: usize) -> Self {
        Self {
            mode: RedisMode::Primary {
                replication_id,
                replication_offset,
            },
            values: HashMap::default(),
        }
    }

    pub async fn start(&mut self, port: usize) -> anyhow::Result<()> {
        if let RedisMode::Replica {
            primary_host,
            primary_port,
        } = &self.mode
        {
            let mut stream = TcpStream::connect(format!("{primary_host}:{primary_port}")).await?;
            let (read_stream, mut write_stream) = stream.split();
            let mut read_stream = RESPStream::new(read_stream);
            self.send_ping(&mut read_stream, &mut write_stream).await?;
            self.send_replconf_port(&mut read_stream, &mut write_stream, port)
                .await?;
            self.send_replconf_capa(&mut read_stream, &mut write_stream)
                .await?;
            self.send_psync(&mut read_stream, &mut write_stream).await?;
        }

        Ok(())
    }

    pub async fn handle(
        &mut self,
        command: RedisCommand,
        write_stream: RedisWriteStream,
    ) -> anyhow::Result<()> {
        match command {
            RedisCommand::Ping => self.ping(write_stream).await,
            RedisCommand::Echo { echo } => self.echo(echo, write_stream).await,
            RedisCommand::Info { section } => self.info(section, write_stream).await,
            RedisCommand::Get { key } => self.get(key, write_stream).await,
            RedisCommand::Set { key, value, px } => self.set(key, value, px, write_stream).await,
            RedisCommand::ReplConfPort { .. } => self.repl_conf_port(write_stream).await,
            RedisCommand::ReplConfCapa { capabilities } => self.repl_conf_capa(write_stream).await,
            RedisCommand::PSync { .. } => self.psync(write_stream).await,
            _ => todo!(),
        }
    }

    async fn ping(&mut self, mut write_stream: RedisWriteStream) -> anyhow::Result<()> {
        write_stream
            .write(RESPValue::SimpleString(b"PONG".to_vec()))
            .await
    }

    async fn echo(
        &mut self,
        echo: Vec<u8>,
        mut write_stream: RedisWriteStream,
    ) -> anyhow::Result<()> {
        write_stream.write(RESPValue::BulkString(echo)).await
    }

    async fn info(
        &mut self,
        section: InfoSection,
        mut write_stream: RedisWriteStream,
    ) -> anyhow::Result<()> {
        match section {
            InfoSection::Replication => {
                let info = match &self.mode {
                    RedisMode::Primary { replication_id, replication_offset } => format!(
                        "role:master\nmaster_replid:{replication_id}\nmaster_repl_offset:{replication_offset}"
                    ),
                    RedisMode::Replica { .. } => "role:slave".to_string(),
                };

                write_stream
                    .write(RESPValue::BulkString(info.into_bytes()))
                    .await
            }
            InfoSection::Default => Err(anyhow::anyhow!(
                "[redis - error] command 'info' currently only supports section 'replication'"
            )),
        }
    }

    async fn get(
        &mut self,
        key: Vec<u8>,
        mut write_stream: RedisWriteStream,
    ) -> anyhow::Result<()> {
        let value = match self.values.get(&key) {
            Some(StoreValue {
                expiration: Some(expiration),
                ..
            }) if *expiration <= SystemTime::now() => {
                self.values.remove(&key);
                RESPValue::NullBulkString
            }
            Some(StoreValue { value, .. }) => RESPValue::BulkString(value.clone()),
            _ => RESPValue::NullBulkString,
        };

        write_stream.write(value).await
    }

    async fn set(
        &mut self,
        key: Vec<u8>,
        value: Vec<u8>,
        px: Option<SystemTime>,
        mut write_stream: RedisWriteStream,
    ) -> anyhow::Result<()> {
        self.values.insert(
            key,
            StoreValue {
                value,
                expiration: px,
            },
        );

        write_stream
            .write(RESPValue::SimpleString(b"OK".to_vec()))
            .await
    }

    async fn repl_conf_port(&mut self, mut write_stream: RedisWriteStream) -> anyhow::Result<()> {
        write_stream
            .write(RESPValue::SimpleString(b"OK".to_vec()))
            .await
    }

    async fn repl_conf_capa(&mut self, mut write_stream: RedisWriteStream) -> anyhow::Result<()> {
        write_stream
            .write(RESPValue::SimpleString(b"OK".to_vec()))
            .await
    }

    async fn psync(&mut self, mut write_stream: RedisWriteStream) -> anyhow::Result<()> {
        if let RedisMode::Primary { replication_id, replication_offset } = &self.mode {
            write_stream
                .write(RESPValue::SimpleString(format!(
                    "FULLRESYNC {} {}",
                    replication_id,
                    replication_offset,
                ).into_bytes()))
                .await?;

            let rdb_file = (0..EMPTY_RDB_HEX.len())
                .step_by(2)
                .map(|i| u8::from_str_radix(&EMPTY_RDB_HEX[i..i + 2], 16))
                .collect::<Result<Vec<_>, _>>()?;

            write_stream.write(format!("${}\r\n", rdb_file.len())).await?;
            write_stream.write(rdb_file).await?;
            Ok(())
        } else {
            Err(anyhow::anyhow!("[redis - error] Redis must be running in primary mode to respond to 'PSYNC' command"))
        }
    }
}

impl RedisStore {
    async fn send_ping(
        &mut self,
        read_stream: &mut RESPStream<ReadHalf<'_>>,
        write_stream: &mut WriteHalf<'_>,
    ) -> anyhow::Result<()> {
        let ping = RESPValue::Array(vec![RESPValue::BulkString(b"PING".to_vec())]);
        write_stream.write_all(&Vec::from(ping)).await?;
        match read_stream.read_value().await {
            Ok(RESPValue::SimpleString(ref s)) if s == b"PONG" => Ok(()),
            _ => Err(anyhow::anyhow!(
                "[redis - error] expected simple-string encoded 'PONG' from primary"
            )),
        }
    }

    async fn send_replconf_port(
        &mut self,
        read_stream: &mut RESPStream<ReadHalf<'_>>,
        write_stream: &mut WriteHalf<'_>,
        port: usize,
    ) -> anyhow::Result<()> {
        let replconf_port = RESPValue::Array(vec![
            RESPValue::BulkString(b"replconf".to_vec()),
            RESPValue::BulkString(b"listening-port".to_vec()),
            RESPValue::BulkString(port.to_string().into_bytes()),
        ]);

        write_stream.write_all(&Vec::from(replconf_port)).await?;
        match read_stream.read_value().await {
            Ok(RESPValue::SimpleString(ref s)) if s == b"OK" => Ok(()),
            _ => Err(anyhow::anyhow!(
                "[redis - error] expected simple-string encoded 'OK' from primary"
            )),
        }
    }

    async fn send_replconf_capa(
        &mut self,
        read_stream: &mut RESPStream<ReadHalf<'_>>,
        write_stream: &mut WriteHalf<'_>,
    ) -> anyhow::Result<()> {
        let replconf_port = RESPValue::Array(vec![
            RESPValue::BulkString(b"replconf".to_vec()),
            RESPValue::BulkString(b"capa".to_vec()),
            RESPValue::BulkString(b"psync2".to_vec()),
        ]);

        write_stream.write_all(&Vec::from(replconf_port)).await?;
        match read_stream.read_value().await {
            Ok(RESPValue::SimpleString(ref s)) if s == b"OK" => Ok(()),
            _ => Err(anyhow::anyhow!(
                "[redis - error] expected simple-string encoded 'OK' from primary"
            )),
        }
    }

    async fn send_psync(
        &mut self,
        read_stream: &mut RESPStream<ReadHalf<'_>>,
        write_stream: &mut WriteHalf<'_>,
    ) -> anyhow::Result<()> {
        let replconf_capa = RESPValue::Array(vec![
            RESPValue::BulkString(b"psync".to_vec()),
            RESPValue::BulkString(b"?".to_vec()),
            RESPValue::BulkString(b"-1".to_vec()),
        ]);

        write_stream.write_all(&Vec::from(replconf_capa)).await?;
        let response = read_stream.read_value().await?;
        let response = if let RESPValue::SimpleString(response) = response {
            String::from_utf8(response)?
        } else {
            return Err(anyhow::anyhow!(
                "[redis - error] expected a simple-string encoded response from the primary"
            ));
        };

        if let Some(primary_info) = response.strip_prefix("FULLRESYNC ") {
            let mut primary_info = primary_info.split_ascii_whitespace();
            let _replication_id = primary_info.next().unwrap();
            let _replication_offset = primary_info.next().unwrap().parse::<usize>()?;
            let _rdb_file = read_stream.read_rdb_file().await?;
            Ok(())
        } else {
            Err(anyhow::anyhow!(
                "[redis - error] expected 'FULLRESYNC' from primary but got '{response}'"
            ))
        }
    }
}
