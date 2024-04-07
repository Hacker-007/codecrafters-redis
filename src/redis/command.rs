use std::time::{Duration, SystemTime};

use super::resp::RESPValue;

#[derive(Debug, PartialEq, Eq)]
pub enum InfoSection {
    Replication,
    Default,
}

#[derive(Debug, PartialEq, Eq)]
pub enum RedisCommand {
    Ping,
    Echo {
        echo: Vec<u8>,
    },
    Info {
        section: InfoSection,
    },
    Get {
        key: Vec<u8>,
    },
    Set {
        key: Vec<u8>,
        value: Vec<u8>,
        px: Option<SystemTime>,
    },
    ReplConfPort {
        listening_port: u64,
    },
    ReplConfCapa {
        capabilities: Vec<Vec<u8>>,
    },
    PSync {
        replication_id: String,
        replication_offset: String,
    },
}

struct CommandParser {
    parts: Vec<Vec<u8>>,
}

impl CommandParser {
    fn new(mut parts: Vec<Vec<u8>>) -> Self {
        parts.reverse();
        Self {
            parts: parts
                .into_iter()
                .map(|mut part| {
                    part.make_ascii_lowercase();
                    part
                })
                .collect(),
        }
    }

    fn parse_next(&mut self) -> Vec<u8> {
        self.parts.pop().unwrap()
    }

    fn expect_arg(&mut self, command_name: &str, arg_name: &str) -> anyhow::Result<Vec<u8>> {
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
    ) -> anyhow::Result<Option<Vec<u8>>> {
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
        match &*parser.parse_next() {
            b"ping" => Ok(RedisCommand::Ping),
            b"echo" => parser
                .expect_arg("echo", "echo")
                .map(|echo| RedisCommand::Echo { echo }),
            b"info" => Ok(RedisCommand::Info {
                section: parser
                    .attempt_flag(|byte| match byte {
                        b"replication" => Some(InfoSection::Replication),
                        _ => Some(InfoSection::Default),
                    })
                    .unwrap(),
            }),
            b"get" => parser
                .expect_arg("get", "key")
                .map(|key| RedisCommand::Get { key }),
            b"set" => {
                let key = parser.expect_arg("set", "key")?;
                let value = parser.expect_arg("set", "value")?;
                let px = parser
                    .attempt_named_arg("set", "px")?
                    .and_then(|millis| String::from_utf8(millis).ok())
                    .and_then(|millis| millis.parse::<u64>().ok())
                    .map(Duration::from_millis)
                    .map(|duration| SystemTime::now() + duration);

                Ok(RedisCommand::Set { key, value, px })
            }
            b"replconf" => {
                if let Some(port) = parser.attempt_named_arg("replconf", "listening-port")? {
                    let port = String::from_utf8(port)?;
                    let port = port.parse::<u64>()?;
                    return Ok(RedisCommand::ReplConfPort {
                        listening_port: port,
                    });
                }

                let mut capabilities = vec![];
                while let Some(capability) = parser.attempt_named_arg("replconf", "capa")? {
                    capabilities.push(capability);
                }

                Ok(RedisCommand::ReplConfCapa { capabilities })
            }
            b"psync" => {
                let replication_id = parser.expect_arg("psync", "replication_id")?;
                let replication_id = String::from_utf8(replication_id)?;
                let replication_offset = parser.expect_arg("psync", "replication_offset")?;
                let replication_offset = String::from_utf8(replication_offset)?;
                Ok(RedisCommand::PSync {
                    replication_id,
                    replication_offset,
                })
            }
            _ => Err(anyhow::anyhow!(
                "[redis - error] received an unknown command"
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::redis::{command::RedisCommand, resp_stream::RESPStream};

    #[tokio::test]
    async fn parses_ping() {
        let mut stream = RESPStream::new("*1\r\n$4\r\nping\r\n".as_bytes());
        let value = stream.read_value().await.unwrap();
        let command: anyhow::Result<RedisCommand> = value.try_into();
        assert!(command.is_ok());
        assert_eq!(command.unwrap(), RedisCommand::Ping)
    }
}
