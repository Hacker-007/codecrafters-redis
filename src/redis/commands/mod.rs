use std::collections::VecDeque;

use super::value::RedisValue;

pub mod echo;
pub mod get;
pub mod ping;
pub mod set;

#[derive(Debug)]
pub enum RedisCommand {
    Ping,
    Echo { echo: String },
    Get { key: String },
    Set { key: String, value: String },
}

impl TryFrom<VecDeque<RedisValue>> for RedisCommand {
    type Error = anyhow::Error;

    fn try_from(values: VecDeque<RedisValue>) -> Result<Self, Self::Error> {
        anyhow::ensure!(
            values.len() >= 1,
            "[redis - error] client input must be a non-empty array"
        );

        let mut values = values
            .into_iter()
            .filter_map(|value| value.to_bulk_string())
            .collect::<VecDeque<_>>();

        let command_name = values.pop_front().unwrap();
        match command_name.to_lowercase().as_str() {
            "ping" => Ok(RedisCommand::Ping),
            "echo" => {
                if let Some(echo) = values.pop_front() {
                    Ok(RedisCommand::Echo { echo })
                } else {
                    Err(anyhow::anyhow!(
                        "[redis - error] command 'echo' requires one argument"
                    ))
                }
            }
            "get" => {
                if let Some(key) = values.pop_front() {
                    Ok(RedisCommand::Get { key })
                } else {
                    Err(anyhow::anyhow!(
                        "[redis - error] command 'get' requires one argument"
                    ))
                }
            }
            "set" => {
                if let Some(key) = values.pop_front() {
                    if let Some(value) = values.pop_front() {
                        return Ok(RedisCommand::Set { key, value })
                    }
                }

                Err(anyhow::anyhow!(
                    "[redis - error] command 'set' requires two arguments"
                ))
            }
            command => Err(anyhow::anyhow!(
                "[redis - error] unknown command '{command}' received"
            )),
        }
    }
}
