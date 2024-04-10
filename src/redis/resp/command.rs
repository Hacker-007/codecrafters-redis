use bytes::Bytes;
use std::time::{Duration, SystemTime};

use super::RESPValue;

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum InfoSection {
    Replication,
    Default,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum ReplConfSection {
    Port { listening_port: u16 },
    Capa { capabilities: Vec<Bytes> },
    GetAck,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum RedisServerCommand {
    Ping,
    Echo {
        echo: Bytes,
    },
    Info {
        section: InfoSection,
    },
    ReplConf {
        section: ReplConfSection,
    },
    PSync {
        replication_id: String,
        replication_offset: i64,
    },
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum RedisStoreCommand {
    Get {
        key: Bytes,
    },
    Set {
        key: Bytes,
        value: Bytes,
        px: Option<SystemTime>,
    },
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum RedisCommand {
    Server(RedisServerCommand),
    Store(RedisStoreCommand),
}

impl RedisStoreCommand {
    pub fn is_write(&self) -> bool {
        matches!(self, Self::Set { .. })
    }
}

impl From<&RedisCommand> for RESPValue {
    fn from(command: &RedisCommand) -> Self {
        match command {
            RedisCommand::Server(command) => command.into(),
            RedisCommand::Store(command) => command.into(),
        }
    }
}

impl From<&RedisServerCommand> for RESPValue {
    fn from(command: &RedisServerCommand) -> Self {
        match command {
            RedisServerCommand::Ping => {
                RESPValue::Array(vec![RESPValue::BulkString(Bytes::from_static(b"PING"))])
            }
            RedisServerCommand::Echo { echo } => RESPValue::Array(vec![
                RESPValue::BulkString(Bytes::from_static(b"ECHO")),
                RESPValue::BulkString(echo.clone()),
            ]),
            RedisServerCommand::Info {
                section: InfoSection::Default,
            } => RESPValue::Array(vec![RESPValue::BulkString(Bytes::from_static(b"INFO"))]),
            RedisServerCommand::Info {
                section: InfoSection::Replication,
            } => RESPValue::Array(vec![
                RESPValue::BulkString(Bytes::from_static(b"INFO")),
                RESPValue::BulkString(Bytes::from_static(b"replication")),
            ]),
            RedisServerCommand::ReplConf {
                section: ReplConfSection::Port { listening_port },
            } => RESPValue::Array(vec![
                RESPValue::BulkString(Bytes::from_static(b"REPLCONF")),
                RESPValue::BulkString(Bytes::from_static(b"listening-port")),
                RESPValue::BulkString(Bytes::copy_from_slice(
                    listening_port.to_string().as_bytes(),
                )),
            ]),
            RedisServerCommand::ReplConf {
                section: ReplConfSection::Capa { capabilities },
            } => {
                let mut values = vec![
                    RESPValue::BulkString(Bytes::from_static(b"REPLCONF")),
                    RESPValue::BulkString(Bytes::from_static(b"capa")),
                ];

                for capability in capabilities {
                    values.push(RESPValue::BulkString(capability.clone()));
                }

                RESPValue::Array(values)
            }
            RedisServerCommand::ReplConf {
                section: ReplConfSection::GetAck,
            } => RESPValue::Array(vec![
                RESPValue::BulkString(Bytes::from_static(b"REPLCONF")),
                RESPValue::BulkString(Bytes::from_static(b"GETACK")),
                RESPValue::BulkString(Bytes::from_static(b"*")),
            ]),
            RedisServerCommand::PSync {
                replication_id,
                replication_offset,
            } => RESPValue::Array(vec![
                RESPValue::BulkString(Bytes::from_static(b"PSYNC")),
                RESPValue::BulkString(Bytes::copy_from_slice(replication_id.as_bytes())),
                RESPValue::BulkString(Bytes::copy_from_slice(
                    replication_offset.to_string().as_bytes(),
                )),
            ]),
        }
    }
}

impl From<&RedisStoreCommand> for RESPValue {
    fn from(command: &RedisStoreCommand) -> Self {
        match command {
            RedisStoreCommand::Get { key } => RESPValue::Array(vec![
                RESPValue::BulkString(Bytes::from_static(b"GET")),
                RESPValue::BulkString(key.clone()),
            ]),
            RedisStoreCommand::Set { key, value, px } => {
                let mut command = vec![
                    RESPValue::BulkString(Bytes::from_static(b"SET")),
                    RESPValue::BulkString(key.clone()),
                    RESPValue::BulkString(value.clone()),
                ];

                if let Some(px) = px {
                    let difference = match px.elapsed() {
                        Ok(duration) => duration,
                        Err(err) => err.duration(),
                    };

                    command.push(RESPValue::BulkString(Bytes::from_static(b"SET")));
                    command.push(RESPValue::BulkString(Bytes::copy_from_slice(
                        difference.as_millis().to_string().as_bytes(),
                    )));
                }

                RESPValue::Array(command)
            }
        }
    }
}

struct CommandParser {
    parts: Vec<Bytes>,
}

impl CommandParser {
    fn new(mut parts: Vec<Bytes>) -> Self {
        parts.reverse();
        Self { parts }
    }

    fn parse_next(&mut self) -> Option<Bytes> {
        self.parts.pop()
    }

    fn expect_arg(&mut self, command_name: &str, arg_name: &str) -> anyhow::Result<Bytes> {
        if let Some(arg) = self.parts.pop() {
            Ok(arg)
        } else {
            Err(anyhow::anyhow!(
                "[redis - error] command '{command_name}' requires an argument '{arg_name}' but was not provided one"
            ))
        }
    }

    fn attempt_named_arg(&mut self, command_name: &str, arg_name: &str) -> Option<Bytes> {
        match self.parts.last() {
            Some(arg) if arg == arg_name.as_bytes() => {
                self.parts.pop();
                self.expect_arg(command_name, arg_name).ok()
            }
            _ => None,
        }
    }

    fn attempt_flag<T>(&mut self, mapper: impl Fn(&[u8]) -> Option<T>) -> Option<T> {
        self.parts.last().and_then(|arg| mapper(arg))
    }
}

impl TryFrom<RESPValue> for RedisCommand {
    type Error = anyhow::Error;

    fn try_from(value: RESPValue) -> Result<Self, Self::Error> {
        let command_parts = value
            .into_array()
            .map(|values| {
                values
                    .into_iter()
                    .filter_map(|value| value.into_bulk_string())
                    .collect::<Vec<_>>()
            })
            .ok_or_else(|| {
                anyhow::anyhow!("[redis - error] expected command to be an array of bulk strings")
            })?;

        if command_parts.is_empty() {
            return Err(anyhow::anyhow!(
                "[redis - error] expected client input to be a non-empty array of bulk strings"
            ));
        }

        let mut parser = CommandParser::new(command_parts);
        let command_name = parser.parse_next().unwrap().to_ascii_lowercase();
        match &*command_name {
            b"ping" => Ok(RedisCommand::Server(RedisServerCommand::Ping)),
            b"echo" => parser
                .expect_arg("echo", "echo")
                .map(|echo| RedisCommand::Server(RedisServerCommand::Echo { echo })),
            b"info" => Ok(RedisCommand::Server(RedisServerCommand::Info {
                section: parser
                    .attempt_flag(|byte| match byte {
                        b"replication" => Some(InfoSection::Replication),
                        _ => Some(InfoSection::Default),
                    })
                    .unwrap(),
            })),
            b"get" => parser
                .expect_arg("get", "key")
                .map(|key| RedisCommand::Store(RedisStoreCommand::Get { key })),
            b"set" => {
                let key = parser.expect_arg("set", "key")?;
                let value = parser.expect_arg("set", "value")?;
                let px = parser
                    .attempt_named_arg("set", "px")
                    .and_then(|millis| String::from_utf8(millis.to_vec()).ok())
                    .and_then(|millis| millis.parse::<u64>().ok())
                    .map(Duration::from_millis)
                    .map(|duration| SystemTime::now() + duration);

                Ok(RedisCommand::Store(RedisStoreCommand::Set {
                    key,
                    value,
                    px,
                }))
            }
            b"replconf" => {
                let section = match parser
                    .parse_next()
                    .map(|section| section.to_ascii_lowercase())
                    .as_deref()
                {
                    Some(b"listening-port") => {
                        let port = parser.parse_next().ok_or_else(|| anyhow::anyhow!("[redis - error] expected value for argument 'listening-port' for command 'replconf'"))?;
                        let port = std::str::from_utf8(&port)?;
                        let port = port.parse::<u16>()?;
                        ReplConfSection::Port {
                            listening_port: port,
                        }
                    }
                    Some(b"capa") => {
                        let mut capabilities = vec![];
                        while let Some(capability) = parser.parse_next() {
                            capabilities.push(capability);
                        }

                        ReplConfSection::Capa { capabilities }
                    }
                    Some(b"getack") => {
                        if let Some(b"*") = parser.parse_next().as_deref() {
                            ReplConfSection::GetAck
                        } else {
                            return Err(anyhow::anyhow!("[redis - error] unexpected section for argument 'getack' for command 'replconf'"));
                        }
                    }
                    _ => {
                        return Err(anyhow::anyhow!(
                            "[redis - error] unknown argument found for command 'replconf'"
                        ))
                    }
                };

                Ok(RedisCommand::Server(RedisServerCommand::ReplConf {
                    section,
                }))
            }
            b"psync" => {
                let replication_id = parser.expect_arg("psync", "replication_id")?;
                let replication_id = String::from_utf8(replication_id.to_vec())?;
                let replication_offset = parser.expect_arg("psync", "replication_offset")?;
                let replication_offset = std::str::from_utf8(&replication_offset)?.parse()?;
                Ok(RedisCommand::Server(RedisServerCommand::PSync {
                    replication_id,
                    replication_offset,
                }))
            }
            bytes => Err(anyhow::anyhow!(
                "[redis - error] received an unprocessable command '{}'",
                std::str::from_utf8(bytes).unwrap_or("unknown")
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::redis::resp::{
        command::{RedisCommand, RedisServerCommand},
        resp_reader::RESPReader,
    };

    #[tokio::test]
    async fn parses_ping() {
        let mut stream = RESPReader::new("*1\r\n$4\r\nping\r\n".as_bytes());
        let value = stream.read_value().await.unwrap();
        let command: anyhow::Result<RedisCommand> = value.try_into();
        assert!(command.is_ok());
        assert_eq!(
            command.unwrap(),
            RedisCommand::Server(RedisServerCommand::Ping)
        )
    }
}
