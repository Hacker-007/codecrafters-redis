use bytes::Bytes;
use std::time::{Duration, SystemTime};

use super::RESPValue;

#[derive(Debug, PartialEq, Eq)]
pub enum InfoSection {
    Replication,
    Default,
}

#[derive(Debug, PartialEq, Eq)]
pub enum RedisServerCommand {
    Ping,
    Echo {
        echo: Bytes,
    },
    Info {
        section: InfoSection,
    },
    ReplConfPort {
        listening_port: u64,
    },
    ReplConfCapa {
        capabilities: Vec<Bytes>,
    },
    PSync {
        replication_id: String,
        replication_offset: usize,
    },
}

#[derive(Debug, PartialEq, Eq)]
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

#[derive(Debug, PartialEq, Eq)]
pub enum RedisCommand {
    Server(RedisServerCommand),
    Store(RedisStoreCommand),
}

struct CommandParser {
    parts: Vec<Bytes>,
}

impl CommandParser {
    fn new(mut parts: Vec<Bytes>) -> Self {
        parts.reverse();
        Self { parts }
    }

    fn parse_next(&mut self) -> Bytes {
        self.parts.pop().unwrap()
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

    fn attempt_named_arg(
        &mut self,
        command_name: &str,
        arg_name: &str,
    ) -> anyhow::Result<Option<Bytes>> {
        match self.parts.last() {
            Some(arg) if arg == arg_name.as_bytes() => {
                self.parts.pop();
                self.expect_arg(command_name, arg_name).map(Some)
            }
            _ => Ok(None),
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
        let command_name = parser.parse_next().to_ascii_lowercase();
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
                    .attempt_named_arg("set", "px")?
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
                if let Some(port) = parser.attempt_named_arg("replconf", "listening-port")? {
                    let port = std::str::from_utf8(&port)?;
                    let port = port.parse::<u64>()?;
                    return Ok(RedisCommand::Server(RedisServerCommand::ReplConfPort {
                        listening_port: port,
                    }));
                }

                let mut capabilities = vec![];
                while let Some(capability) = parser.attempt_named_arg("replconf", "capa")? {
                    capabilities.push(capability);
                }

                Ok(RedisCommand::Server(RedisServerCommand::ReplConfCapa {
                    capabilities,
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
