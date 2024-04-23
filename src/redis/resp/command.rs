use bytes::Bytes;
use std::time::{Duration, SystemTime};

use crate::redis::replication::command::{InfoSection, RedisReplicationCommand, ReplConfSection};

use super::RESPValue;

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum ConfigSection {
    Get { keys: Vec<Bytes> },
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum RedisServerCommand {
    Ping,
    Echo { message: Bytes },
    Config { section: ConfigSection },
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
    Keys {
        key: Bytes,
    },
    Type {
        key: Bytes,
    },
    XAdd {
        key: Bytes,
        entry_id: Bytes,
        fields: Vec<(Bytes, Bytes)>,
    },
}

impl RedisStoreCommand {
    pub fn is_write(&self) -> bool {
        matches!(self, Self::Set { .. })
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum RedisCommand {
    Store(RedisStoreCommand),
    Server(RedisServerCommand),
    Replication(RedisReplicationCommand),
}

impl RedisCommand {
    pub fn is_getack(&self) -> bool {
        matches!(
            self,
            Self::Replication(RedisReplicationCommand::ReplConf {
                section: ReplConfSection::GetAck
            })
        )
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

    fn is_finished(&self) -> bool {
        self.parts.is_empty()
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
            b"keys" => {
                let key = parser.expect_arg("keys", "key")?;
                Ok(RedisCommand::Store(RedisStoreCommand::Keys { key }))
            }
            b"type" => {
                let key = parser.expect_arg("type", "key")?;
                Ok(RedisCommand::Store(RedisStoreCommand::Type { key }))
            }
            b"xadd" => {
                let key = parser.expect_arg("xadd", "key")?;
                let entry_id = parser.expect_arg("xadd", "id")?;
                let mut fields = vec![];
                while !parser.is_finished() {
                    let field = parser.expect_arg("xadd", "field")?;
                    let value = parser.expect_arg("xadd", "value")?;
                    fields.push((field, value));
                }

                Ok(RedisCommand::Store(RedisStoreCommand::XAdd {
                    key,
                    entry_id,
                    fields,
                }))
            }
            b"ping" => Ok(RedisCommand::Server(RedisServerCommand::Ping)),
            b"echo" => parser
                .expect_arg("echo", "message")
                .map(|message| RedisCommand::Server(RedisServerCommand::Echo { message })),
            b"config" => {
                let section = match parser
                    .parse_next()
                    .map(|section| section.to_ascii_lowercase())
                    .as_deref()
                {
                    Some(b"get") => {
                        let mut keys = vec![];
                        while let Some(key) = parser.parse_next() {
                            keys.push(key);
                        }

                        ConfigSection::Get { keys }
                    }
                    _ => {
                        return Err(anyhow::anyhow!(
                            "[redis - error] unknown argument found for command 'config'"
                        ))
                    }
                };

                Ok(RedisCommand::Server(RedisServerCommand::Config { section }))
            }
            b"info" => Ok(RedisCommand::Replication(RedisReplicationCommand::Info {
                section: parser
                    .attempt_flag(|byte| match byte {
                        b"replication" => Some(InfoSection::Replication),
                        _ => Some(InfoSection::Default),
                    })
                    .unwrap(),
            })),
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
                    Some(b"ack") => {
                        if let Some(processed_bytes) = parser.parse_next().as_deref() {
                            let processed_bytes = std::str::from_utf8(processed_bytes)?.parse()?;
                            ReplConfSection::Ack { processed_bytes }
                        } else {
                            return Err(anyhow::anyhow!("[redis - error] expected value for argument 'ack' for command 'replconf'"));
                        }
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

                Ok(RedisCommand::Replication(
                    RedisReplicationCommand::ReplConf { section },
                ))
            }
            b"psync" => {
                let replication_id = parser.expect_arg("psync", "replication_id")?;
                let replication_id = String::from_utf8(replication_id.to_vec())?;
                let replication_offset = parser.expect_arg("psync", "replication_offset")?;
                let replication_offset = std::str::from_utf8(&replication_offset)?.parse()?;
                Ok(RedisCommand::Replication(RedisReplicationCommand::PSync {
                    replication_id,
                    replication_offset,
                }))
            }
            b"wait" => {
                let num_replicas = parser.expect_arg("wait", "num_replicas")?;
                let num_replicas = std::str::from_utf8(&num_replicas)?.parse()?;
                let timeout = parser.expect_arg("wait", "timeout")?;
                let timeout = std::str::from_utf8(&timeout)?.parse()?;
                Ok(RedisCommand::Replication(RedisReplicationCommand::Wait {
                    num_replicas,
                    timeout,
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
