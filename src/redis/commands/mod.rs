use std::{
    collections::VecDeque,
    str::FromStr,
    time::{Duration, SystemTime},
};

use super::value::RedisValue;

pub mod echo;
pub mod get;
pub mod info;
pub mod ping;
pub mod psync;
pub mod repl_conf_capa;
pub mod repl_conf_port;
pub mod set;

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
pub enum RedisCommand {
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

impl RedisCommand {
    pub fn is_write(&self) -> bool {
        matches!(self, RedisCommand::Set { .. })
    }
}

impl From<RedisCommand> for RedisValue {
    fn from(command: RedisCommand) -> Self {
        match command {
            RedisCommand::Ping => {
                let mut parts = VecDeque::new();
                parts.push_back(RedisValue::BulkString("ping".to_string()));
                RedisValue::Array(parts)
            }
            RedisCommand::Echo { echo } => {
                let mut parts = VecDeque::new();
                parts.push_back(RedisValue::BulkString("echo".to_string()));
                parts.push_back(RedisValue::BulkString(echo));
                RedisValue::Array(parts)
            }
            RedisCommand::Info { section } => {
                let mut parts = VecDeque::new();
                parts.push_back(RedisValue::BulkString("info".to_string()));
                match section {
                    InfoSection::Replication => {
                        parts.push_back(RedisValue::BulkString("replication".to_string()))
                    }
                    InfoSection::Default => {
                        parts.push_back(RedisValue::BulkString("default".to_string()))
                    }
                }
                
                RedisValue::Array(parts)
            }
            RedisCommand::Get { key } => {
                let mut parts = VecDeque::new();
                parts.push_back(RedisValue::BulkString("get".to_string()));
                parts.push_back(RedisValue::BulkString(key));
                RedisValue::Array(parts)
            }
            RedisCommand::Set { key, value, px } => {
                let mut parts = VecDeque::new();
                parts.push_back(RedisValue::BulkString("set".to_string()));
                parts.push_back(RedisValue::BulkString(key));
                parts.push_back(RedisValue::BulkString(value));
                if let Some(px) = px {
                    if px > SystemTime::now() {
                        let duration = px.duration_since(SystemTime::now()).unwrap();
                        parts.push_back(RedisValue::BulkString("px".to_string()));
                        parts.push_back(RedisValue::BulkString(duration.as_millis().to_string()));
                    }
                }

                RedisValue::Array(parts)
            }
            RedisCommand::ReplConfPort { listening_port } => {
                let mut parts = VecDeque::new();
                parts.push_back(RedisValue::BulkString("replconf".to_string()));
                parts.push_back(RedisValue::BulkString("listening-port".to_string()));
                parts.push_back(RedisValue::BulkString(listening_port.to_string()));
                RedisValue::Array(parts)
            }
            RedisCommand::ReplConfCapa { capabilities } => {
                let mut parts = VecDeque::new();
                parts.push_back(RedisValue::BulkString("replconf".to_string()));
                capabilities.into_iter().for_each(|capability| {
                    parts.push_back(RedisValue::BulkString("capa".to_string()));
                    parts.push_back(RedisValue::BulkString(capability));
                });

                RedisValue::Array(parts)
            }
            RedisCommand::PSync {
                replication_id,
                replication_offset,
            } => {
                let mut parts = VecDeque::new();
                parts.push_back(RedisValue::BulkString("psync".to_string()));
                parts.push_back(RedisValue::BulkString(replication_id));
                parts.push_back(RedisValue::BulkString(replication_offset));
                RedisValue::Array(parts)
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

impl TryFrom<VecDeque<RedisValue>> for RedisCommand {
    type Error = anyhow::Error;

    fn try_from(values: VecDeque<RedisValue>) -> Result<Self, Self::Error> {
        anyhow::ensure!(
            !values.is_empty(),
            "[redis - error] client input must be a non-empty array"
        );

        let mut values = values
            .into_iter()
            .filter_map(|value| {
                if let RedisValue::BulkString(s) = value {
                    Some(s)
                } else {
                    None
                }
            })
            .collect::<VecDeque<_>>();

        match values.parse_next().as_str() {
            "ping" => Ok(RedisCommand::Ping),
            "echo" => values
                .expect_arg("echo", "echo")
                .map(|echo| RedisCommand::Echo { echo }),
            "info" => Ok(RedisCommand::Info {
                section: values
                    .attempt_flag::<InfoSection>()
                    .unwrap_or(InfoSection::Default),
            }),
            "get" => values
                .expect_arg("get", "key")
                .map(|key| RedisCommand::Get { key }),
            "set" => {
                let key = values.expect_arg("set", "key")?;
                let value = values.expect_arg("set", "value")?;
                let px = values
                    .attempt_named_arg("set", "px")?
                    .and_then(|millis| millis.parse::<u64>().ok())
                    .map(Duration::from_millis)
                    .map(|duration| SystemTime::now() + duration);

                Ok(RedisCommand::Set { key, value, px })
            }
            "replconf" => {
                if let Some(port) = values.attempt_named_arg("replconf", "listening-port")? {
                    let port = port.parse::<u64>()?;
                    return Ok(RedisCommand::ReplConfPort {
                        listening_port: port,
                    });
                }

                let mut capabilities = vec![];
                while let Some(capability) = values.attempt_named_arg("replconf", "capa")? {
                    capabilities.push(capability);
                }

                Ok(RedisCommand::ReplConfCapa { capabilities })
            }
            "psync" => {
                let replication_id = values.expect_arg("psync", "replication_id")?;
                let replication_offset = values.expect_arg("psync", "replication_offset")?;
                Ok(RedisCommand::PSync {
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
