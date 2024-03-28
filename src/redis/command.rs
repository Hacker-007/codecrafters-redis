use std::{collections::VecDeque, str::FromStr, time::{Duration, SystemTime}};

use super::resp::RESPValue;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InfoSection {
    Replication,
    Default,
}

impl FromStr for InfoSection {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "replication" => Ok(Self::Replication),
            _ => Ok(Self::Default),
        }
    }
}

#[derive(Debug, Clone)]
pub enum Command {
    Ping,
    Echo {
        echo: String,
    },
    Info {
        section: InfoSection,
    },
    Get {
        key: String,
    },
    Set {
        key: String,
        value: String,
        px: Option<SystemTime>,
    },
    ReplConfPort {
        listening_port: u64,
    },
    ReplConfCapa {
        capabilities: Vec<String>,
    },
    PSync {
        replication_id: String,
        replication_offset: String,
    },
}

impl From<Command> for RESPValue {
    fn from(command: Command) -> Self {
        match command {
            Command::Ping => {
                let mut parts = VecDeque::new();
                parts.push_back(RESPValue::BulkString("ping".to_string()));
                RESPValue::Array(parts)
            }
            Command::Echo { echo } => {
                let mut parts = VecDeque::new();
                parts.push_back(RESPValue::BulkString("echo".to_string()));
                parts.push_back(RESPValue::BulkString(echo));
                RESPValue::Array(parts)
            }
            Command::Info { section } => {
                let mut parts = VecDeque::new();
                parts.push_back(RESPValue::BulkString("info".to_string()));
                match section {
                    InfoSection::Replication => {
                        parts.push_back(RESPValue::BulkString("replication".to_string()))
                    }
                    InfoSection::Default => {
                        parts.push_back(RESPValue::BulkString("default".to_string()))
                    }
                }

                RESPValue::Array(parts)
            }
            Command::Get { key } => {
                let mut parts = VecDeque::new();
                parts.push_back(RESPValue::BulkString("get".to_string()));
                parts.push_back(RESPValue::BulkString(key));
                RESPValue::Array(parts)
            }
            Command::Set { key, value, px } => {
                let mut parts = VecDeque::new();
                parts.push_back(RESPValue::BulkString("set".to_string()));
                parts.push_back(RESPValue::BulkString(key));
                parts.push_back(RESPValue::BulkString(value));
                if let Some(px) = px {
                    if px > SystemTime::now() {
                        let duration = px.duration_since(SystemTime::now()).unwrap();
                        parts.push_back(RESPValue::BulkString("px".to_string()));
                        parts.push_back(RESPValue::BulkString(duration.as_millis().to_string()));
                    }
                }

                RESPValue::Array(parts)
            }
            Command::ReplConfPort { listening_port } => {
                let mut parts = VecDeque::new();
                parts.push_back(RESPValue::BulkString("replconf".to_string()));
                parts.push_back(RESPValue::BulkString("listening-port".to_string()));
                parts.push_back(RESPValue::BulkString(listening_port.to_string()));
                RESPValue::Array(parts)
            }
            Command::ReplConfCapa { capabilities } => {
                let mut parts = VecDeque::new();
                parts.push_back(RESPValue::BulkString("replconf".to_string()));
                capabilities.into_iter().for_each(|capability| {
                    parts.push_back(RESPValue::BulkString("capa".to_string()));
                    parts.push_back(RESPValue::BulkString(capability));
                });

                RESPValue::Array(parts)
            }
            Command::PSync {
                replication_id,
                replication_offset,
            } => {
                let mut parts = VecDeque::new();
                parts.push_back(RESPValue::BulkString("psync".to_string()));
                parts.push_back(RESPValue::BulkString(replication_id));
                parts.push_back(RESPValue::BulkString(replication_offset));
                RESPValue::Array(parts)
            }
        }
    }
}

trait RedisCommandParser {
    fn parse_next(&mut self) -> String;
    fn expect_arg(&mut self, command_name: &str, arg_name: &str) -> anyhow::Result<String>;
    fn attempt_named_arg(
        &mut self,
        command_name: &str,
        arg_name: &str,
    ) -> anyhow::Result<Option<String>>;
    fn attempt_flag<T: FromStr>(&mut self) -> Option<T>;
}

impl RedisCommandParser for VecDeque<String> {
    fn parse_next(&mut self) -> String {
        self.pop_front().unwrap().to_lowercase()
    }

    fn expect_arg(&mut self, command_name: &str, arg_name: &str) -> anyhow::Result<String> {
        if let Some(arg) = self.pop_front() {
            Ok(arg)
        } else {
            Err(anyhow::anyhow!(
                "[redis - error] command '{command_name}' requires an argument '{arg_name}'"
            ))
        }
    }

    fn attempt_named_arg(
        &mut self,
        command_name: &str,
        arg_name: &str,
    ) -> anyhow::Result<Option<String>> {
        match self.front() {
            Some(arg) if arg == arg_name => {
                self.pop_front();
                if let Some(value) = self.pop_front() {
                    Ok(Some(value))
                } else {
                    Err(anyhow::anyhow!("[redis - error] command '{command_name}' requires a value for argument '{arg_name}'"))
                }
            }
            _ => Ok(None),
        }
    }

    fn attempt_flag<T: FromStr>(&mut self) -> Option<T> {
        if let Some(arg) = self.front() {
            if let Ok(flag) = arg.parse::<T>() {
                self.pop_front();
                return Some(flag);
            }
        }

        None
    }
}

impl TryFrom<RESPValue> for Command {
    type Error = anyhow::Error;

    fn try_from(value: RESPValue) -> Result<Self, Self::Error> {
        let values = if let RESPValue::Array(values) = value {
            values
        } else {
            return Err(anyhow::anyhow!(
                "[redis - error] expected command to be an array of bulk strings"
            ));
        };
        
        anyhow::ensure!(
            !values.is_empty(),
            "[redis - error] client input must be a non-empty array"
        );

        let mut values = values
            .into_iter()
            .filter_map(|value| {
                if let RESPValue::BulkString(s) = value {
                    Some(s)
                } else {
                    None
                }
            })
            .collect::<VecDeque<_>>();

        match values.parse_next().as_str() {
            "ping" => Ok(Command::Ping),
            "echo" => values
                .expect_arg("echo", "echo")
                .map(|echo| Command::Echo { echo }),
            "info" => Ok(Command::Info {
                section: values
                    .attempt_flag::<InfoSection>()
                    .unwrap_or(InfoSection::Default),
            }),
            "get" => values
                .expect_arg("get", "key")
                .map(|key| Command::Get { key }),
            "set" => {
                let key = values.expect_arg("set", "key")?;
                let value = values.expect_arg("set", "value")?;
                let px = values
                    .attempt_named_arg("set", "px")?
                    .and_then(|millis| millis.parse::<u64>().ok())
                    .map(Duration::from_millis)
                    .map(|duration| SystemTime::now() + duration);

                Ok(Command::Set { key, value, px })
            }
            "replconf" => {
                if let Some(port) = values.attempt_named_arg("replconf", "listening-port")? {
                    let port = port.parse::<u64>()?;
                    return Ok(Command::ReplConfPort {
                        listening_port: port,
                    });
                }

                let mut capabilities = vec![];
                while let Some(capability) = values.attempt_named_arg("replconf", "capa")? {
                    capabilities.push(capability);
                }

                Ok(Command::ReplConfCapa { capabilities })
            }
            "psync" => {
                let replication_id = values.expect_arg("psync", "replication_id")?;
                let replication_offset = values.expect_arg("psync", "replication_offset")?;
                Ok(Command::PSync {
                    replication_id,
                    replication_offset,
                })
            }
            command => Err(anyhow::anyhow!(
                "[redis - error] unknown command '{command}' received"
            )),
        }
    }
}
