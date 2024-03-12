use std::{
    collections::VecDeque,
    time::{Duration, SystemTime},
};

use super::value::RedisValue;

pub mod echo;
pub mod get;
pub mod ping;
pub mod set;

#[derive(Debug)]
pub enum RedisCommand {
    Ping,
    Echo {
        echo: String,
    },
    Get {
        key: String,
    },
    Set {
        key: String,
        value: String,
        px: Option<SystemTime>,
    },
}

trait RedisCommandParser {
    fn parse_next(&mut self) -> String;
    fn expect_arg(&mut self, command_name: &str, arg_name: &str) -> anyhow::Result<String>;
    fn attempt_arg(&mut self, command_name: &str, arg_name: &str)
        -> anyhow::Result<Option<String>>;
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

    fn attempt_arg(
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
            "get" => values
                .expect_arg("get", "key")
                .map(|key| RedisCommand::Get { key }),
            "set" => {
                let key = values.expect_arg("set", "key")?;
                let value = values.expect_arg("set", "value")?;
                let px = values
                    .attempt_arg("set", "px")?
                    .and_then(|millis| millis.parse::<u64>().ok())
                    .map(Duration::from_millis)
                    .map(|duration| SystemTime::now() + duration);

                Ok(RedisCommand::Set { key, value, px })
            }
            command => Err(anyhow::anyhow!(
                "[redis - error] unknown command '{command}' received"
            )),
        }
    }
}
